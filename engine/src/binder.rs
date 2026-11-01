//! Resolves a parsed query against the catalog, clause by clause in SQL's logical evaluation
//! order: FROM, then WHERE, then SELECT, then ORDER BY, then LIMIT. Everything after binding works with resolved identifiers, never
//! with the names as written.

use std::collections::BTreeMap;
use std::fmt;

use crate::ast::{
    CallArg, CmpOp, ColumnRef, Direction, Expr, Literal, NullsOrder, Order, Projection, Query,
    SelectExpr,
};
use crate::error::{render_at, Span};
use crate::history::{DescribedColumn, HistoryError, HistorySource};
use crate::rules::{ColumnSource, KafkaField, NamingRules};
use crate::security::{Filters, RowFilter};
use crate::source::{EventSource, SourceError};
use crate::table_engine::{Key, TableEngine};
use crate::types::{kafka_accepts, Kind, LiteralError, Type, Value};

pub use chlive_core::bound::*;

/// What the binder can look things up in.
pub struct Catalog<'a> {
    pub rules: &'a dyn NamingRules,
    pub history: &'a dyn HistorySource,
    pub source: &'a dyn EventSource,
    /// The values of the query's parameters (`{name:Type}`), by name.
    pub params: &'a Params,
    /// The rows and columns of each table the subscriber may see.
    pub filters: &'a Filters,
}

/// Query parameter values by name, as JSON; each is read by its placeholder's declared type,

#[derive(Debug)]
pub enum BindError {
    /// A problem with the query itself, at a position in it.
    At { message: String, span: Span },
    /// The catalog could not be consulted (ClickHouse unreachable, …).
    History(HistoryError),
    /// The event source could not be consulted (Kafka unreachable, …).
    Source(SourceError),
    /// A problem with the query's parameters as a whole (one given that the query doesn't use).
    Params(String),
    /// The query names a column the subscriber may not see.
    Denied { message: String, span: Span },
}

impl BindError {
    fn at(message: impl Into<String>, span: Span) -> Self {
        Self::At {
            message: message.into(),
            span,
        }
    }

    /// With line and column for query errors.
    pub fn render(&self, src: &str) -> String {
        match self {
            BindError::At { message, span } | BindError::Denied { message, span } => {
                render_at(src, *span, message)
            }
            BindError::History(e) => e.to_string(),
            BindError::Source(e) => e.to_string(),
            BindError::Params(message) => message.clone(),
        }
    }
}

impl fmt::Display for BindError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BindError::At { message, span } | BindError::Denied { message, span } => {
                write!(f, "{message} at byte {}", span.start)
            }
            BindError::History(e) => write!(f, "{e}"),
            BindError::Source(e) => write!(f, "{e}"),
            BindError::Params(message) => write!(f, "{message}"),
        }
    }
}

impl std::error::Error for BindError {}

pub async fn bind(query: &Query, catalog: &Catalog<'_>) -> Result<BoundQuery, BindError> {
    let mut query = query.clone();
    resolve_params(&mut query, catalog.params)?;
    bind_resolved(&query, catalog).await
}

/// Replaces each parameter placeholder, in the query and its sub-queries, with its value read
/// by the declared type; every given value must be used.
fn resolve_params(query: &mut Query, params: &Params) -> Result<(), BindError> {
    let mut declared: BTreeMap<String, (Type, Value)> = BTreeMap::new();
    visit_query_literals(query, &mut |lit, span| {
        let Literal::Param { name, ty } = lit else {
            return Ok(());
        };
        let (t, value) = match declared.get(name.as_str()) {
            Some((t, _)) if t.clickhouse != *ty => {
                return Err(BindError::at(
                    format!("parameter {name} is declared as both {t} and {ty}"),
                    span,
                ))
            }
            Some(resolved) => resolved.clone(),
            None => {
                let resolved = param_value(name, ty, params, span)?;
                declared.insert(name.clone(), resolved.clone());
                resolved
            }
        };
        *lit = Literal::Bound {
            name: std::mem::take(name),
            ty: t,
            value,
        };
        Ok(())
    })?;
    match params.keys().find(|k| !declared.contains_key(k.as_str())) {
        Some(extra) => Err(BindError::Params(format!(
            "unknown parameter {extra} (not in the query)"
        ))),
        None => Ok(()),
    }
}

/// A parameter's declared type and its value.
fn param_value(
    name: &str,
    ty: &str,
    params: &Params,
    span: Span,
) -> Result<(Type, Value), BindError> {
    let t = Type::parse(ty);
    if !t.ops().filter {
        return Err(BindError::at(
            format!("parameter {name}: type {ty} is not supported"),
            span,
        ));
    }
    let Some(json) = params.get(name) else {
        return Err(BindError::at(format!("missing parameter {name}"), span));
    };
    let value = if json.is_null() {
        if !t.nullable {
            return Err(BindError::at(
                format!("parameter {name} is null, but {ty} isn't Nullable"),
                span,
            ));
        }
        Value::Null
    } else {
        t.kind.column_type().decode(json).ok_or_else(|| {
            BindError::at(
                format!("parameter {name}: cannot read {json} as {ty}"),
                span,
            )
        })?
    };
    Ok((t, value))
}

fn visit_query_literals(
    query: &mut Query,
    f: &mut dyn FnMut(&mut Literal, Span) -> Result<(), BindError>,
) -> Result<(), BindError> {
    match &mut query.selection {
        Some(expr) => visit_literals(expr, f),
        None => Ok(()),
    }
}

fn visit_literals(
    expr: &mut Expr,
    f: &mut dyn FnMut(&mut Literal, Span) -> Result<(), BindError>,
) -> Result<(), BindError> {
    match expr {
        Expr::Column(_) => Ok(()),
        Expr::Literal { value, span } => f(value, *span),
        Expr::Compare { left, right, .. } | Expr::And(left, right) | Expr::Or(left, right) => {
            visit_literals(left, f)?;
            visit_literals(right, f)
        }
        Expr::Not { expr, .. } | Expr::IsNull { expr, .. } => visit_literals(expr, f),
        Expr::InList { expr, list, .. } => {
            visit_literals(expr, f)?;
            list.iter_mut().try_for_each(|(lit, span)| f(lit, *span))
        }
        Expr::Exists { query, .. } => visit_query_literals(query, f),
        Expr::InSubquery { expr, query, .. } => {
            visit_literals(expr, f)?;
            visit_query_literals(query, f)
        }
        Expr::Now { .. } => Ok(()),
    }
}

async fn bind_resolved(query: &Query, catalog: &Catalog<'_>) -> Result<BoundQuery, BindError> {
    let (table, row_filter) = bind_from(query, catalog).await?;
    check_final_shape(query, &table)?;
    let (selection, sub) = match &query.selection {
        Some(expr) => split_subquery(expr)?,
        None => (None, None),
    };
    let mut windows = Vec::new();
    let filter = match &selection {
        Some(expr) => Some(bind_cond(expr, &table, &mut windows, true)?),
        None => None,
    };
    let filter = and(filter, row_filter);
    let semi_join = match sub {
        Some(Sub::In(key, sub, span)) => {
            Some(bind_semi_join(key, sub, span, &table, catalog).await?)
        }
        Some(Sub::Exists(sub, _)) => Some(bind_exists(sub, &table, catalog).await?),
        None => None,
    };
    if let Some(w) = windows.first() {
        if semi_join.is_some() {
            return Err(BindError::at(
                "now() with a sub-query isn't supported yet",
                w.span,
            ));
        }
    }
    if let Some(mut aggregation) = bind_aggregation(query, &table)? {
        // now(): native functions only (each row's values are taken back out at its deadline)
        if let Some(w) = windows.first() {
            for item in &aggregation.items {
                if let AggExpr::Call { func, .. } = &item.expr {
                    if crate::aggregate::native(*func).is_none() {
                        return Err(BindError::at(
                            format!(
                                "now() with {}() isn't supported yet \
                                 (with now(): count, sum, avg, min, max, groupArray)",
                                func.name()
                            ),
                            w.span,
                        ));
                    }
                }
            }
        }
        // FINAL: native functions only (each replaced row's values are taken back out)
        if let (Some(_), Some(span)) = (&table.final_key, query.from.final_span) {
            for item in &aggregation.items {
                if let AggExpr::Call { func, .. } = &item.expr {
                    if crate::aggregate::native(*func).is_none() {
                        return Err(BindError::at(
                            format!(
                                "FINAL on {} (ReplacingMergeTree) with {}() isn't supported yet \
                                 (on FINAL: count, sum, avg, min, max, groupArray)",
                                table.name,
                                func.name()
                            ),
                            span,
                        ));
                    }
                }
            }
        }
        if let Some(Sub::In(_, _, span) | Sub::Exists(_, span)) = sub {
            return Err(BindError::at(
                "IN (subquery) with GROUP BY or aggregates is not supported yet",
                span,
            ));
        }
        if let Some(order) = &query.order {
            bind_aggregate_order(order, &table, &mut aggregation)?;
        }
        aggregation.limit = query.limit.map(|l| l.value);
        aggregation.offset = offset_of(query);
        return Ok(BoundQuery {
            table,
            filter,
            projection: Vec::new(),
            order: Vec::new(),
            limit: None,
            offset: None,
            aggregation: Some(aggregation),
            semi_join: None,
            windows,
        });
    }
    let projection = bind_select(query, &table)?;
    let order = match &query.order {
        Some(order) => {
            let ids: Vec<ColumnId> = projection.iter().map(|p| p.column).collect();
            bind_order(order, &table, &ids)?
        }
        None => Vec::new(),
    };
    check_window_shape(query, &table, filter.as_ref(), &windows, &order)?;
    Ok(BoundQuery {
        table,
        filter,
        projection,
        order,
        limit: query.limit.map(|l| l.value),
        offset: offset_of(query),
        aggregation: None,
        semi_join,
        windows,
    })
}

/// `OFFSET m`, with `OFFSET 0` as none.
fn offset_of(query: &Query) -> Option<u64> {
    query.offset.map(|o| o.value).filter(|m| *m > 0)
}

/// The WHERE without its `IN (subquery)`, and that sub-query (key, query, the IN's span). It
/// must be a top-level AND condition (anywhere else, `bind_where` rejects it), and only one.
/// A query's one sub-query condition.
#[derive(Clone, Copy)]
enum Sub<'a> {
    /// `key IN (SELECT …)`
    In(&'a Expr, &'a Query, Span),
    /// `EXISTS (SELECT …)`
    Exists(&'a Query, Span),
}

fn split_subquery(expr: &Expr) -> Result<(Option<Expr>, Option<Sub<'_>>), BindError> {
    fn conjuncts<'a>(e: &'a Expr, out: &mut Vec<&'a Expr>) {
        match e {
            Expr::And(l, r) => {
                conjuncts(l, out);
                conjuncts(r, out);
            }
            e => out.push(e),
        }
    }
    let mut all = Vec::new();
    conjuncts(expr, &mut all);
    let mut rest: Option<Expr> = None;
    let mut sub = None;
    for c in all {
        match c {
            Expr::InSubquery {
                expr,
                query,
                negated,
                span,
            } => {
                if *negated {
                    return Err(BindError::at(
                        "NOT IN (subquery) is not supported yet",
                        *span,
                    ));
                }
                if sub.is_some() {
                    return Err(BindError::at(SUBQUERY_PLACE, *span));
                }
                sub = Some(Sub::In(expr.as_ref(), query.as_ref(), *span));
            }
            Expr::Exists {
                query,
                negated,
                span,
            } => {
                if *negated {
                    return Err(BindError::at("NOT EXISTS is not supported yet", *span));
                }
                if sub.is_some() {
                    return Err(BindError::at(SUBQUERY_PLACE, *span));
                }
                sub = Some(Sub::Exists(query.as_ref(), *span));
            }
            other => {
                rest = Some(match rest {
                    None => other.clone(),
                    Some(r) => Expr::And(Box::new(r), Box::new(other.clone())),
                })
            }
        }
    }
    Ok((rest, sub))
}

const NOW_PLACE: &str =
    "now() is supported only compared with a DateTime column (`ts > now() - INTERVAL 1 HOUR`), \
     in the WHERE of the main query";

/// `col op now() ± INTERVAL …` (either side; `<`, `<=`, `>`, `>=`): its window, and the
/// condition with the column on the left; `None` when it isn't a comparison with `now()`.
fn bind_now(
    op: CmpOp,
    left: &Expr,
    right: &Expr,
    table: &BoundTable,
    windows: &mut Vec<TimeWindow>,
) -> Result<Option<BoundExpr>, BindError> {
    // the column on the left: `now() … < col` is `col > now() …`
    let (other, now, op) = match (left, right) {
        (o, n @ Expr::Now { .. }) => (o, n, op),
        (n @ Expr::Now { .. }, o) => (o, n, op.flipped()),
        _ => return Ok(None),
    };
    let Expr::Now {
        tz,
        intervals,
        span,
    } = now
    else {
        unreachable!("matched above")
    };
    if matches!(op, CmpOp::Eq | CmpOp::NotEq) {
        return Err(BindError::at(
            format!(
                "`column {} now()` is true for an instant (or all but one); use <, <=, > or >=",
                op.symbol()
            ),
            *span,
        ));
    }
    let Expr::Column(col) = other else {
        return Err(BindError::at(
            "now() is compared with a DateTime or DateTime64 column",
            other.span(),
        ));
    };
    let c = column(table, col)?;
    if !matches!(c.ty.kind, Kind::DateTime(_)) {
        return Err(BindError::at(
            format!(
                "now() is compared with DateTime or DateTime64 columns; {} is {}",
                c.name, c.ty
            ),
            col.span,
        ));
    }
    windows.push(TimeWindow {
        column: c.id,
        tz: match tz {
            None => None,
            Some(name) => Some(
                name.parse::<chrono_tz::Tz>()
                    .map_err(|_| BindError::at(format!("unknown timezone '{name}'"), *span))?,
            ),
        },
        intervals: intervals.clone(),
        span: *span,
    });
    Ok(Some(BoundExpr::Now {
        window: windows.len() - 1,
        column: c.id,
        op,
    }))
}

/// The shapes `now()` conditions don't follow yet: plain FINAL rows or plain rows with OFFSET;
/// LIMIT except where rows only leave from the bottom (every `now()` a top-level AND `>`/`>=`
/// on the first ORDER BY column, DESC).
fn check_window_shape(
    query: &Query,
    table: &BoundTable,
    filter: Option<&BoundExpr>,
    windows: &[TimeWindow],
    order: &[OrderKey],
) -> Result<(), BindError> {
    let Some(first) = windows.first() else {
        return Ok(());
    };
    let unsupported =
        |what: &str| BindError::at(format!("now() with {what} isn't supported yet"), first.span);
    if table.final_key.is_some() && (query.limit.is_some() || offset_of(query).is_some()) {
        return Err(unsupported("FINAL and LIMIT or OFFSET"));
    }
    if offset_of(query).is_some() {
        return Err(unsupported("OFFSET"));
    }
    if query.limit.is_some() {
        // rows leave from the bottom only when the newest come first
        let by_window = order
            .first()
            .is_some_and(|k| k.desc && windows.iter().all(|w| w.column == k.column));
        if !by_window {
            return Err(BindError::at(
                format!(
                    "now() with LIMIT needs ORDER BY {} DESC first (rows leave from the bottom)",
                    table.column(first.column).name
                ),
                first.span,
            ));
        }
        // and only leave, once
        fn conjuncts<'a>(e: &'a BoundExpr, out: &mut Vec<&'a BoundExpr>) {
            match e {
                BoundExpr::And(l, r) => {
                    conjuncts(l, out);
                    conjuncts(r, out);
                }
                e => out.push(e),
            }
        }
        let mut top = Vec::new();
        conjuncts(filter.expect("windows are in the WHERE"), &mut top);
        let leaving = top
            .iter()
            .filter(|e| {
                matches!(
                    e,
                    BoundExpr::Now {
                        op: CmpOp::Gt | CmpOp::GtEq,
                        ..
                    }
                )
            })
            .count();
        if leaving != windows.len() {
            return Err(unsupported(
                "LIMIT where rows can come in or come back (only `column > now() …` as \
                 top-level AND conditions)",
            ));
        }
    }
    Ok(())
}

const SUBQUERY_PLACE: &str =
    "a sub-query (IN or EXISTS) is only supported as a top-level AND condition, once";

/// `key IN (SELECT col FROM t [WHERE …])`: one column of one table, optionally filtered.
async fn bind_semi_join(
    key: &Expr,
    sub: &Query,
    span: Span,
    outer: &BoundTable,
    catalog: &Catalog<'_>,
) -> Result<SemiJoin, BindError> {
    let Expr::Column(k) = key else {
        return Err(BindError::at(
            "IN (subquery) needs a column on its left",
            key.span(),
        ));
    };
    let outer_key = resolve_column(outer, k)?;
    let unsupported = |what: &str, at: Span| {
        BindError::at(
            format!("{what} in an IN sub-query is not supported yet; select one column"),
            at,
        )
    };
    check_subquery_clauses(sub, "an IN sub-query")?;
    let column = match &sub.projection {
        Projection::Columns(cols) if cols.len() == 1 => &cols[0],
        Projection::Items(items) if items.len() == 1 => match &items[0].expr {
            SelectExpr::Column(c) => c,
            SelectExpr::Call { span, .. } => return Err(unsupported("an aggregate", *span)),
            SelectExpr::Literal(_, at) => {
                return Err(BindError::at(
                    "an IN sub-query must select exactly one column",
                    *at,
                ))
            }
        },
        _ => {
            return Err(BindError::at(
                "an IN sub-query must select exactly one column",
                span,
            ))
        }
    };
    let (inner, row_filter) = bind_from(sub, catalog).await?;
    check_inner_final(sub, &inner)?;
    let inner_key = resolve_column(&inner, column)?;
    check_key_types(
        outer,
        outer_key,
        &inner,
        inner_key,
        &k.name,
        &column.name,
        span,
    )?;
    let inner_filter = match &sub.selection {
        Some(e) => {
            if let Some(at) = outer_ref(e, &inner, outer) {
                return Err(BindError::at(
                    "this refers to the outer query; only EXISTS sub-queries can, as `inner = outer`",
                    at,
                ));
            }
            Some(bind_where(e, &inner)?)
        }
        None => None,
    };
    let inner_filter = and(inner_filter, row_filter);
    Ok(SemiJoin {
        keys: Some((outer_key, inner_key)),
        inner,
        inner_filter,
    })
}

/// GROUP BY, ORDER BY and LIMIT aren't supported in sub-queries.
fn check_subquery_clauses(sub: &Query, what: &str) -> Result<(), BindError> {
    let unsupported = |clause: &str, at: Span| {
        BindError::at(format!("{clause} in {what} is not supported yet"), at)
    };
    if let Some((_, at)) = &sub.group_by {
        return Err(unsupported("GROUP BY", *at));
    }
    if let Some(l) = &sub.limit {
        return Err(unsupported("LIMIT", l.span));
    }
    if let Some(o) = &sub.order {
        let at = match o {
            Order::All { span, .. } => *span,
            Order::Items(items) => match &items[0].expr {
                SelectExpr::Column(c) => c.span,
                SelectExpr::Call { span, .. } | SelectExpr::Literal(_, span) => *span,
            },
        };
        return Err(unsupported("ORDER BY", at));
    }
    Ok(())
}

/// Which query a column of a sub-query refers to: its own table first, then the outer one.
#[derive(Debug, Clone, Copy, PartialEq)]
enum Side {
    Inner,
    Outer,
}

fn side(c: &ColumnRef, inner: &BoundTable, outer: &BoundTable) -> Option<Side> {
    // a column it may not see is still its own (and fails as such)
    let has = |t: &BoundTable| {
        t.columns.iter().any(|col| col.name == c.name) || t.denied.contains(&c.name)
    };
    match &c.qualifier {
        Some(q) if inner.names(q) => Some(Side::Inner),
        Some(q) if outer.names(q) => Some(Side::Outer),
        Some(_) => None,
        None if has(inner) => Some(Side::Inner),
        None if has(outer) => Some(Side::Outer),
        None => None,
    }
}

/// The first column of `expr` that refers to the outer query, if any.
fn outer_ref(expr: &Expr, inner: &BoundTable, outer: &BoundTable) -> Option<Span> {
    match expr {
        Expr::Column(c) => (side(c, inner, outer) == Some(Side::Outer)).then_some(c.span),
        Expr::Literal { .. } => None,
        Expr::Compare { left, right, .. } => {
            outer_ref(left, inner, outer).or_else(|| outer_ref(right, inner, outer))
        }
        Expr::And(l, r) | Expr::Or(l, r) => {
            outer_ref(l, inner, outer).or_else(|| outer_ref(r, inner, outer))
        }
        Expr::Not { expr, .. } | Expr::IsNull { expr, .. } | Expr::InList { expr, .. } => {
            outer_ref(expr, inner, outer)
        }
        Expr::InSubquery { expr, .. } => outer_ref(expr, inner, outer),
        Expr::Exists { .. } | Expr::Now { .. } => None,
    }
}

/// `EXISTS (SELECT … FROM inner WHERE inner.k = outer.k AND …)`: the same semi-join as
/// `outer.k IN (SELECT k FROM inner WHERE …)`. Its SELECT list doesn't matter. Without a
/// correlation, it's true for every row once the sub-query has a row.
async fn bind_exists(
    sub: &Query,
    outer: &BoundTable,
    catalog: &Catalog<'_>,
) -> Result<SemiJoin, BindError> {
    check_subquery_clauses(sub, "an EXISTS sub-query")?;
    let (inner, row_filter) = bind_from(sub, catalog).await?;
    check_inner_final(sub, &inner)?;
    let mut conjuncts = Vec::new();
    if let Some(e) = &sub.selection {
        fn split<'a>(e: &'a Expr, out: &mut Vec<&'a Expr>) {
            match e {
                Expr::And(l, r) => {
                    split(l, out);
                    split(r, out);
                }
                e => out.push(e),
            }
        }
        split(e, &mut conjuncts);
    }
    let mut keys: Option<(ColumnId, ColumnId)> = None;
    let mut filter: Option<Expr> = None;
    for c in conjuncts {
        // `inner = outer` (either way round): the correlation
        if let Expr::Compare {
            op: CmpOp::Eq,
            left,
            right,
            span: at,
        } = c
        {
            if let (Expr::Column(a), Expr::Column(b)) = (left.as_ref(), right.as_ref()) {
                let pair = match (side(a, &inner, outer), side(b, &inner, outer)) {
                    (Some(Side::Inner), Some(Side::Outer)) => Some((b, a)),
                    (Some(Side::Outer), Some(Side::Inner)) => Some((a, b)),
                    _ => None,
                };
                if let Some((o, i)) = pair {
                    if keys.is_some() {
                        return Err(BindError::at(
                            "EXISTS with more than one correlation is not supported yet",
                            *at,
                        ));
                    }
                    let (ok, ik) = (
                        resolve_column(outer, &unqualified(o))?,
                        resolve_column(&inner, &unqualified(i))?,
                    );
                    check_key_types(outer, ok, &inner, ik, &o.name, &i.name, *at)?;
                    keys = Some((ok, ik));
                    continue;
                }
            }
        }
        if let Some(at) = outer_ref(c, &inner, outer) {
            return Err(BindError::at(
                "only `inner = outer` equalities can refer to the outer query",
                at,
            ));
        }
        filter = Some(match filter {
            None => c.clone(),
            Some(f) => Expr::And(Box::new(f), Box::new(c.clone())),
        });
    }
    let inner_filter = match &filter {
        Some(e) => Some(bind_where(e, &inner)?),
        None => None,
    };
    let inner_filter = and(inner_filter, row_filter);
    Ok(SemiJoin {
        keys,
        inner,
        inner_filter,
    })
}

/// The column without its qualifier (already checked to name its table).
fn unqualified(c: &ColumnRef) -> ColumnRef {
    ColumnRef {
        qualifier: None,
        ..c.clone()
    }
}

/// Keys of an IN or EXISTS must be comparable: numbers with numbers, otherwise the same kind.
fn check_key_types(
    outer: &BoundTable,
    ok: ColumnId,
    inner: &BoundTable,
    ik: ColumnId,
    oname: &str,
    iname: &str,
    at: Span,
) -> Result<(), BindError> {
    let (ot, it) = (&outer.column(ok).ty, &inner.column(ik).ty);
    let comparable = ot.class().is_some() && ot.class() == it.class();
    if !comparable {
        return Err(BindError::at(
            format!("{oname} ({ot}) can't be compared with the sub-query's {iname} ({it})"),
            at,
        ));
    }
    Ok(())
}

/// GROUP BY and aggregate functions, if the query has either. Selected columns must be grouping
/// columns; aggregates take one column (`count` also none, or `*`).
fn bind_aggregation(query: &Query, table: &BoundTable) -> Result<Option<Aggregation>, BindError> {
    let has_calls = matches!(&query.projection, Projection::Items(items)
        if items.iter().any(|i| matches!(i.expr, SelectExpr::Call { .. })));
    let Some((group_refs, group_span)) = query
        .group_by
        .as_ref()
        .map(|(k, s)| (k.as_slice(), Some(*s)))
        .or(has_calls.then_some((&[][..], None)))
    else {
        return Ok(None);
    };
    let mut keys = Vec::new();
    for k in group_refs {
        let id = resolve_column(table, k)?;
        if !table.column(id).ty.ops().group {
            return Err(BindError::at(
                format!(
                    "grouping by column {} of type {} is not supported",
                    k.name,
                    table.column(id).ty
                ),
                k.span,
            ));
        }
        if !keys.contains(&id) {
            keys.push(id);
        }
    }
    let items: Vec<(SelectExpr, Option<String>)> = match &query.projection {
        Projection::Wildcard => {
            return Err(BindError::at(
                "SELECT * can't be used with GROUP BY; list the columns and aggregates",
                group_span.unwrap_or(query.from.span),
            ))
        }
        Projection::Columns(cols) => cols
            .iter()
            .map(|c| (SelectExpr::Column(c.clone()), None))
            .collect(),
        Projection::Items(items) => items
            .iter()
            .map(|i| (i.expr.clone(), i.alias.clone()))
            .collect(),
    };
    let mut out: Vec<AggItem> = Vec::new();
    for (expr, alias) in items {
        let (default_name, ty, expr, span) = match expr {
            SelectExpr::Literal(_, span) => {
                return Err(BindError::at(
                    "selecting constants is not supported yet",
                    span,
                ))
            }
            SelectExpr::Column(c) => {
                let id = resolve_column(table, &c)?;
                if !keys.contains(&id) {
                    return Err(BindError::at(
                        format!(
                            "column {} must be in GROUP BY or inside an aggregate function",
                            c.name
                        ),
                        c.span,
                    ));
                }
                let col = table.column(id);
                (col.name.clone(), col.ty.clone(), AggExpr::Key(id), c.span)
            }
            SelectExpr::Call { name, arg, span } => {
                let (default_name, ty, expr) = bind_call(&name, &arg, span, table)?;
                (default_name, ty, expr, span)
            }
        };
        if let Some(alias) = &alias {
            check_reserved(alias, span)?;
        }
        let name = alias.unwrap_or(default_name);
        if out.iter().any(|o| o.name == name) {
            return Err(BindError::at(
                format!("duplicate output column {name}; name it with AS"),
                span,
            ));
        }
        out.push(AggItem {
            name,
            ty,
            expr,
            hidden: false,
        });
    }
    Ok(Some(Aggregation {
        keys,
        items: out,
        order: Vec::new(),
        limit: None,
        offset: None,
    }))
}

/// An aggregate call: its default name (`count()`, `avg(latency_ms)`), type and expression.
fn bind_call(
    name: &str,
    arg: &CallArg,
    span: Span,
    table: &BoundTable,
) -> Result<(String, Type, AggExpr), BindError> {
    if name.eq_ignore_ascii_case("now") {
        return Err(BindError::at(NOW_PLACE, span));
    }
    let Some(func) = crate::aggregate::lookup(name) else {
        return Err(BindError::at(
            format!(
                "unknown function {name} (supported: {})",
                crate::aggregate::SUPPORTED
            ),
            span,
        ));
    };
    let arg = match (func, arg) {
        (AggFunc::Count, CallArg::None | CallArg::Star) => None,
        (_, CallArg::Column(c)) => Some((resolve_column(table, c)?, c)),
        (f, _) => {
            return Err(BindError::at(
                format!("{}() needs a column", f.name()),
                span,
            ))
        }
    };
    let ty = result_type(func, arg.as_ref().map(|(id, c)| (table.column(*id), *c)))?;
    // as ClickHouse names it: the function as written (`COUNT()`, `array_agg(x)`)
    let default_name = match &arg {
        None => format!("{name}()"),
        Some((id, _)) => format!("{name}({})", table.column(*id).name),
    };
    let expr = AggExpr::Call {
        func,
        arg: arg.map(|(id, _)| id),
    };
    Ok((default_name, ty, expr))
}

/// ORDER BY of an aggregate query. A name is an output column (its alias or default name) or
/// else a grouping column; a call is an aggregate, the selected one if it's the same, else
/// computed hidden. As in ClickHouse, a column that is neither grouped nor aggregated can't be
/// ordered by.
fn bind_aggregate_order(
    order: &Order,
    table: &BoundTable,
    agg: &mut Aggregation,
) -> Result<(), BindError> {
    let mut keys = Vec::new();
    let mut key = |agg: &mut Aggregation,
                   item: usize,
                   d: Option<Direction>,
                   n: Option<NullsOrder>,
                   span: Span|
     -> Result<(), BindError> {
        let it = &agg.items[item];
        if !it.ty.ops().order {
            return Err(BindError::at(
                format!("ordering by {} of type {} is not supported", it.name, it.ty),
                span,
            ));
        }
        keys.push(AggOrderKey {
            item,
            desc: d == Some(Direction::Desc),
            nulls_first: n == Some(NullsOrder::First),
        });
        Ok(())
    };
    // the item computing `expr`: a selected one, else a new hidden one
    let item_for = |agg: &mut Aggregation, expr: AggExpr, ty: Type| -> usize {
        if let Some(i) = agg.items.iter().position(|i| i.expr == expr) {
            return i;
        }
        let n = agg.items.iter().filter(|i| i.hidden).count();
        agg.items.push(AggItem {
            name: format!("__o{n}"),
            ty,
            expr,
            hidden: true,
        });
        agg.items.len() - 1
    };
    match order {
        Order::All {
            direction,
            nulls,
            span,
        } => {
            for i in 0..agg.items.len() {
                if !agg.items[i].hidden {
                    key(agg, i, *direction, *nulls, *span)?;
                }
            }
        }
        Order::Items(items) => {
            for o in items {
                let (item, span) = match &o.expr {
                    SelectExpr::Literal(_, span) => {
                        return Err(BindError::at(
                            "ordering by a constant is not supported",
                            *span,
                        ))
                    }
                    SelectExpr::Column(c) => {
                        let output = agg
                            .items
                            .iter()
                            .position(|i| !i.hidden && c.qualifier.is_none() && i.name == c.name);
                        let item = match output {
                            Some(i) => i,
                            None => {
                                let id = resolve_column(table, c)?;
                                if !agg.keys.contains(&id) {
                                    return Err(BindError::at(
                                        format!(
                                            "column {} must be in GROUP BY or inside an aggregate function",
                                            c.name
                                        ),
                                        c.span,
                                    ));
                                }
                                let ty = table.column(id).ty.clone();
                                item_for(agg, AggExpr::Key(id), ty)
                            }
                        };
                        (item, c.span)
                    }
                    SelectExpr::Call { name, arg, span } => {
                        let (_, ty, expr) = bind_call(name, arg, *span, table)?;
                        (item_for(agg, expr, ty), *span)
                    }
                };
                key(agg, item, o.direction, o.nulls, span)?;
            }
        }
    }
    agg.order = keys;
    Ok(())
}

/// An aggregate's result type (see [`crate::aggregate::result_type`]), with the error pointing
/// at the argument.
fn result_type(
    func: AggFunc,
    arg: Option<(&BoundColumn, &crate::ast::ColumnRef)>,
) -> Result<Type, BindError> {
    crate::aggregate::result_type(func, arg.map(|(col, _)| &col.ty)).map_err(|e| match arg {
        Some((col, c)) => BindError::at(format!("{e}; column {} is {}", col.name, col.ty), c.span),
        None => BindError::at(e, Span::default()),
    })
}

/// ORDER BY: columns of the table, selected or not; `ALL` is the SELECT list, in SELECT order
/// (as in ClickHouse). Without NULLS FIRST/LAST, NULLs go last in both directions (ClickHouse's
/// default).
fn bind_order(
    order: &Order,
    table: &BoundTable,
    projection: &[ColumnId],
) -> Result<Vec<OrderKey>, BindError> {
    let key = |id: ColumnId, d: Option<Direction>, n: Option<NullsOrder>, span: Span| {
        let col = table.column(id);
        if !col.ty.ops().order {
            return Err(BindError::at(
                format!(
                    "ordering by column {} of type {} is not supported",
                    col.name, col.ty
                ),
                span,
            ));
        }
        Ok(OrderKey {
            column: id,
            desc: d == Some(Direction::Desc),
            nulls_first: n == Some(NullsOrder::First),
        })
    };
    match order {
        Order::All {
            direction,
            nulls,
            span,
        } => projection
            .iter()
            .map(|id| key(*id, *direction, *nulls, *span))
            .collect(),
        Order::Items(items) => items
            .iter()
            .map(|item| match &item.expr {
                SelectExpr::Column(c) => {
                    let id = resolve_column(table, c)?;
                    key(id, item.direction, item.nulls, c.span)
                }
                SelectExpr::Call { span, .. } => Err(BindError::at(
                    "aggregate functions in ORDER BY need GROUP BY or aggregates in SELECT",
                    *span,
                )),
                SelectExpr::Literal(_, span) => Err(BindError::at(
                    "ordering by a constant is not supported",
                    *span,
                )),
            })
            .collect(),
    }
}

fn resolve_column(table: &BoundTable, c: &ColumnRef) -> Result<ColumnId, BindError> {
    check_qualifier(table, c)?;
    let (name, span) = (&c.name, c.span);
    match table.columns.iter().find(|c| &c.name == name) {
        Some(c) => Ok(c.id),
        None if table.denied.contains(name) => Err(BindError::Denied {
            message: format!("column {name} of {} isn't readable here", table.name),
            span,
        }),
        None => {
            let names: Vec<&str> = table.columns.iter().map(|c| c.name.as_str()).collect();
            Err(BindError::at(
                format!(
                    "unknown column {name} in table {} (columns: {})",
                    table.name,
                    names.join(", ")
                ),
                span,
            ))
        }
    }
}

/// `t.col`: `t` must be the table's name or alias.
fn check_qualifier(table: &BoundTable, c: &ColumnRef) -> Result<(), BindError> {
    match &c.qualifier {
        Some(q) if !table.names(q) => {
            Err(BindError::at(format!("unknown table or alias {q}"), c.span))
        }
        _ => Ok(()),
    }
}

/// FROM: the table must exist in ClickHouse, have an event stream (a Kafka topic), and record
/// each row's Kafka partition and offset (so history and stream can be lined up).
/// `a AND b`, either possibly absent.
fn and(a: Option<BoundExpr>, b: Option<BoundExpr>) -> Option<BoundExpr> {
    match (a, b) {
        (Some(a), Some(b)) => Some(BoundExpr::And(Box::new(a), Box::new(b))),
        (a, b) => a.or(b),
    }
}

/// The FROM table, and the condition its row filter puts on it (none: every row).
async fn bind_from(
    query: &Query,
    catalog: &Catalog<'_>,
) -> Result<(BoundTable, Option<BoundExpr>), BindError> {
    let from = &query.from;
    if from.name.starts_with(crate::snapshot::CLONE_PREFIX) {
        return Err(BindError::at(
            format!("unknown table {} (snapshots aren't tables)", from.name),
            from.span,
        ));
    }
    if !catalog.rules.exposes(&from.name) {
        return Err(BindError::at(
            format!("unknown table {} (not in the server's config)", from.name),
            from.span,
        ));
    }
    let (database, clickhouse_name) = catalog.rules.clickhouse_location(&from.name);
    let clickhouse_table = catalog.rules.clickhouse_table(&from.name);
    let described = match catalog.history.describe(&clickhouse_table).await {
        Ok(cols) => cols,
        Err(HistoryError::UnknownTable(_)) => {
            return Err(BindError::at(
                format!(
                    "unknown table {} in ClickHouse ({clickhouse_table})",
                    from.name
                ),
                from.span,
            ))
        }
        Err(e) => return Err(BindError::History(e)),
    };
    let engine = catalog
        .history
        .table_engine(&database, &clickhouse_name)
        .await
        .map_err(BindError::History)?;
    match (&engine, from.final_span) {
        (TableEngine::Other(name), _) => {
            return Err(BindError::at(
                format!(
                "table {} is a {name} table; chlive reads MergeTree and ReplacingMergeTree tables",
                from.name
            ),
                from.span,
            ))
        }
        (
            TableEngine::Replacing {
                key: Key::Unsupported(why),
                ..
            },
            Some(span),
        ) => {
            return Err(BindError::at(
                format!("FINAL on {}: {why}", from.name),
                span,
            ))
        }
        // FINAL on a MergeTree: nothing to merge
        _ => {}
    }
    let topic = catalog.rules.topic(&from.name);
    if catalog
        .source
        .partitions(&topic)
        .await
        .map_err(BindError::Source)?
        .is_none()
    {
        return Err(BindError::at(
            format!("no event stream for table {} (topic {topic})", from.name),
            from.span,
        ));
    }
    let bound = |i: usize, c: &DescribedColumn| {
        let m = catalog.rules.column(&from.name, &c.name);
        BoundColumn {
            id: ColumnId(i),
            name: m.name,
            clickhouse: c.name.clone(),
            source: m.source,
            ty: Type::parse(&c.ty),
        }
    };
    // the columns it may see: not hidden, and allowed by its filter
    let filter = catalog.filters.get(&from.name);
    let allowed = filter.and_then(|f| f.columns.as_ref());
    let mut denied = Vec::new();
    let columns: Vec<BoundColumn> = described
        .iter()
        .filter(|c| !catalog.rules.is_hidden(&from.name, &c.name))
        .filter(|c| {
            let name = catalog.rules.column(&from.name, &c.name).name;
            let ok = allowed.is_none_or(|a| a.contains(&name));
            if !ok {
                denied.push(name);
            }
            ok
        })
        .enumerate()
        .map(|(i, c)| bound(i, c))
        .collect();
    // FINAL: the key, version and is-deleted columns, hidden ones too
    let mut internal: Vec<BoundColumn> = Vec::new();
    let final_key = match (&engine, from.final_span) {
        (
            TableEngine::Replacing {
                ver,
                is_deleted,
                key: Key::Columns { sorting, .. },
            },
            Some(_),
        ) => {
            let mut id_of = |name: &str| -> Result<ColumnId, BindError> {
                if let Some(c) = columns
                    .iter()
                    .chain(&internal)
                    .find(|c| c.clickhouse == name)
                {
                    return Ok(c.id);
                }
                let c = described.iter().find(|c| c.name == name).ok_or_else(|| {
                    BindError::at(
                        format!("FINAL on {}: no column {name} in ClickHouse", from.name),
                        from.span,
                    )
                })?;
                let c = bound(columns.len() + internal.len(), c);
                internal.push(c);
                Ok(internal.last().unwrap().id)
            };
            Some(FinalKey {
                key: sorting.iter().map(|c| id_of(c)).collect::<Result<_, _>>()?,
                ver: ver.as_deref().map(&mut id_of).transpose()?,
                deleted: is_deleted.as_deref().map(&mut id_of).transpose()?,
            })
        }
        _ => None,
    };
    // a Kafka field's column must hold it exactly as Kafka Connect stores it
    for c in columns.iter().chain(&internal) {
        if let ColumnSource::Kafka(field) = c.source {
            if !kafka_accepts(field, &c.ty) {
                let fits = match field {
                    KafkaField::Offset | KafkaField::Partition => "an integer",
                    KafkaField::Timestamp => "DateTime64(3) or a 64-bit integer (milliseconds)",
                };
                return Err(BindError::at(
                    format!(
                        "column {} of {} holds the Kafka {field}, so it must be {fits}, not {}",
                        c.name, from.name, c.ty
                    ),
                    from.span,
                ));
            }
        }
    }
    // the row filter's column, hidden or not
    let row_filter = match filter.map(|f| &f.rows) {
        None | Some(RowFilter::All) => None,
        Some(RowFilter::None) => Some(BoundExpr::Literal(Value::Bool(false))),
        Some(RowFilter::In { column, values }) => {
            let c = match columns
                .iter()
                .chain(&internal)
                .find(|c| c.clickhouse == *column)
            {
                Some(c) => c.clone(),
                None => {
                    let c = described
                        .iter()
                        .find(|c| c.name == *column)
                        .ok_or_else(|| {
                            BindError::at(
                                format!(
                                "table {}: its row filter's column {column} isn't in ClickHouse",
                                from.name
                            ),
                                from.span,
                            )
                        })?;
                    internal.push(bound(columns.len() + internal.len(), c));
                    internal.last().unwrap().clone()
                }
            };
            let decode = c.ty.kind.column_type();
            let list = values
                .iter()
                .map(|v| {
                    decode
                        .decode(&serde_json::Value::String(v.clone()))
                        .ok_or_else(|| {
                            BindError::at(
                                format!(
                                    "table {}: its row filter's value {v:?} isn't a {}",
                                    from.name, c.ty
                                ),
                                from.span,
                            )
                        })
                })
                .collect::<Result<Vec<_>, _>>()?;
            Some(match list.is_empty() {
                true => BoundExpr::Literal(Value::Bool(false)),
                false => BoundExpr::InList {
                    expr: Box::new(BoundExpr::Column(c.id)),
                    list,
                    negated: false,
                },
            })
        }
    };
    // a rename can land on another column's name
    let mut names = std::collections::BTreeSet::new();
    if let Some(dup) = columns.iter().find(|c| !names.insert(c.name.as_str())) {
        return Err(BindError::at(
            format!(
                "table {} has two columns named {} (check its column mapping)",
                from.name, dup.name
            ),
            from.span,
        ));
    }
    let table = BoundTable {
        name: from.name.clone(),
        alias: from.alias.clone(),
        clickhouse_table,
        database,
        clickhouse_name,
        topic,
        columns,
        engine,
        internal,
        final_key,
        denied,
    };
    Ok((table, row_filter))
}

/// FINAL on a sub-query's ReplacingMergeTree.
fn check_inner_final(sub: &Query, inner: &BoundTable) -> Result<(), BindError> {
    match (&inner.final_key, sub.from.final_span) {
        (Some(_), Some(span)) => Err(BindError::at(
            format!(
                "FINAL on {} (ReplacingMergeTree) in a sub-query isn't supported yet",
                inner.name
            ),
            span,
        )),
        _ => Ok(()),
    }
}

/// `FROM t FINAL` on a ReplacingMergeTree: the shapes live FINAL doesn't follow yet.
fn check_final_shape(query: &Query, table: &BoundTable) -> Result<(), BindError> {
    let (Some(_), Some(span)) = (&table.final_key, query.from.final_span) else {
        return Ok(());
    };
    let what = if query.selection.as_ref().is_some_and(has_subquery) {
        "a sub-query"
    } else {
        return Ok(());
    };
    Err(final_unsupported(table, what, span))
}

fn final_unsupported(table: &BoundTable, what: &str, span: Span) -> BindError {
    BindError::at(
        format!(
            "FINAL on {} (ReplacingMergeTree) with {what} isn't supported yet",
            table.name
        ),
        span,
    )
}

/// Whether a condition has an IN or EXISTS sub-query anywhere.
fn has_subquery(e: &Expr) -> bool {
    match e {
        Expr::Exists { .. } | Expr::InSubquery { .. } => true,
        Expr::And(l, r) | Expr::Or(l, r) => has_subquery(l) || has_subquery(r),
        Expr::Not { expr, .. } => has_subquery(expr),
        _ => false,
    }
}

/// WHERE: a boolean expression over the table's columns. Comparisons need compatible types;
/// literals are coerced to the other side's type here, once, not per event.
fn bind_where(expr: &Expr, table: &BoundTable) -> Result<BoundExpr, BindError> {
    bind_cond(expr, table, &mut Vec::new(), false)
}

/// A condition; with `timed`, comparisons with `now()` anywhere in it (their windows collected
/// in `windows`).
fn bind_cond(
    expr: &Expr,
    table: &BoundTable,
    windows: &mut Vec<TimeWindow>,
    timed: bool,
) -> Result<BoundExpr, BindError> {
    match expr {
        Expr::InSubquery { span, .. } | Expr::Exists { span, .. } => {
            Err(BindError::at(SUBQUERY_PLACE, *span))
        }
        Expr::Now { span, .. } => Err(BindError::at(NOW_PLACE, *span)),
        Expr::And(l, r) => Ok(BoundExpr::And(
            Box::new(bind_cond(l, table, windows, timed)?),
            Box::new(bind_cond(r, table, windows, timed)?),
        )),
        Expr::Or(l, r) => Ok(BoundExpr::Or(
            Box::new(bind_cond(l, table, windows, timed)?),
            Box::new(bind_cond(r, table, windows, timed)?),
        )),
        Expr::Not { expr, .. } => Ok(BoundExpr::Not(Box::new(bind_cond(
            expr, table, windows, timed,
        )?))),
        Expr::Compare {
            op,
            left,
            right,
            span,
        } => {
            if timed {
                if let Some(now) = bind_now(*op, left, right, table, windows)? {
                    return Ok(now);
                }
            }
            let (l, r) = (operand(left, table)?, operand(right, table)?);
            if matches!(l, Operand::Literal(Literal::Null, _))
                || matches!(r, Operand::Literal(Literal::Null, _))
            {
                let what = if *op == CmpOp::NotEq {
                    "IS NOT NULL"
                } else {
                    "IS NULL"
                };
                return Err(BindError::at(
                    format!("comparing with NULL is never true; use {what}"),
                    *span,
                ));
            }
            let (left, right, ty) = match (l, r) {
                (Operand::Column(a, ta), Operand::Column(b, tb)) => {
                    check_comparable(&ta, &tb, *span)?;
                    (BoundExpr::Column(a), BoundExpr::Column(b), ta)
                }
                (Operand::Column(c, t), Operand::Literal(lit, lspan)) => (
                    BoundExpr::Column(c),
                    BoundExpr::Literal(coerce(&lit, lspan, &t)?),
                    t,
                ),
                (Operand::Literal(lit, lspan), Operand::Column(c, t)) => (
                    BoundExpr::Literal(coerce(&lit, lspan, &t)?),
                    BoundExpr::Column(c),
                    t,
                ),
                (Operand::Literal(a, aspan), Operand::Literal(b, bspan)) => {
                    let ta = natural_type(&a, aspan)?;
                    let tb = natural_type(&b, bspan)?;
                    check_comparable(&ta, &tb, *span)?;
                    (
                        BoundExpr::Literal(coerce(&a, aspan, &ta)?),
                        BoundExpr::Literal(coerce(&b, bspan, &tb)?),
                        ta,
                    )
                }
            };
            if matches!(ty.kind, Kind::Bool(_)) && !matches!(op, CmpOp::Eq | CmpOp::NotEq) {
                return Err(BindError::at(
                    "booleans can only be compared with = or !=",
                    *span,
                ));
            }
            Ok(BoundExpr::Compare {
                op: *op,
                left: Box::new(left),
                right: Box::new(right),
            })
        }
        Expr::IsNull { expr, negated, .. } => {
            let e = match operand(expr, table)? {
                Operand::Column(c, _) => BoundExpr::Column(c),
                Operand::Literal(lit, span) => {
                    BoundExpr::Literal(coerce(&lit, span, &natural_type(&lit, span)?)?)
                }
            };
            Ok(BoundExpr::IsNull {
                expr: Box::new(e),
                negated: *negated,
            })
        }
        Expr::InList {
            expr,
            list,
            negated,
            span,
        } => {
            let Operand::Column(c, ty) = operand(expr, table)? else {
                return Err(BindError::at("IN needs a column on its left", *span));
            };
            if matches!(ty.kind, Kind::Bool(_)) {
                return Err(BindError::at(
                    "IN is not supported for booleans; use =",
                    *span,
                ));
            }
            let values = list
                .iter()
                .map(|(lit, lspan)| match lit {
                    Literal::Bound {
                        name,
                        value: Value::Null,
                        ..
                    } => Err(BindError::at(
                        format!("parameter {name} is null, which never matches in IN; use IS NULL"),
                        *lspan,
                    )),
                    Literal::Null => Err(BindError::at(
                        "NULL in an IN list never matches; use IS NULL",
                        *lspan,
                    )),
                    _ => coerce(lit, *lspan, &ty),
                })
                .collect::<Result<_, _>>()?;
            Ok(BoundExpr::InList {
                expr: Box::new(BoundExpr::Column(c)),
                list: values,
                negated: *negated,
            })
        }
        Expr::Column(c) => {
            let col = column(table, c)?;
            if !matches!(col.ty.kind, Kind::Bool(_)) {
                return Err(BindError::at(
                    format!(
                        "column {} ({}) is not a condition; compare it with something",
                        col.name, col.ty
                    ),
                    c.span,
                ));
            }
            Ok(BoundExpr::Column(col.id))
        }
        Expr::Literal {
            value: Literal::Bool(b),
            ..
        } => Ok(BoundExpr::Literal(Value::Bool(*b))),
        Expr::Literal { span, .. } => Err(BindError::at("this value is not a condition", *span)),
    }
}

/// One side of a comparison, before type checking.
enum Operand {
    Column(ColumnId, Type),
    Literal(Literal, Span),
}

fn operand(expr: &Expr, table: &BoundTable) -> Result<Operand, BindError> {
    match expr {
        Expr::Column(c) => {
            let col = column(table, c)?;
            if !col.ty.ops().filter {
                return Err(BindError::at(
                    format!(
                        "filtering on column {} of type {} is not supported",
                        col.name, col.ty
                    ),
                    c.span,
                ));
            }
            Ok(Operand::Column(col.id, col.ty.clone()))
        }
        Expr::Literal { value, span } => Ok(Operand::Literal(value.clone(), *span)),
        Expr::Now { span, .. } => Err(BindError::at(NOW_PLACE, *span)),
        other => Err(BindError::at(
            "only columns and values can be compared",
            other.span(),
        )),
    }
}

fn column<'a>(table: &'a BoundTable, c: &ColumnRef) -> Result<&'a BoundColumn, BindError> {
    Ok(table.column(resolve_column(table, c)?))
}

fn check_comparable(a: &Type, b: &Type, span: Span) -> Result<(), BindError> {
    if a.class().is_some() && a.class() == b.class() {
        Ok(())
    } else {
        Err(BindError::at(format!("cannot compare {a} with {b}"), span))
    }
}

/// The type a literal has on its own (when compared with another literal).
fn natural_type(lit: &Literal, span: Span) -> Result<Type, BindError> {
    Ok(Type::parse(match lit {
        Literal::Number(n) if n.contains('.') => "Float64",
        Literal::Number(_) => "Int64",
        Literal::String(_) => "String",
        Literal::Bool(_) => "Bool",
        Literal::Null => return Err(BindError::at("NULL has no type here", span)),
        Literal::Bound { ty, .. } => return Ok(ty.clone()),
        Literal::Param { name, .. } => return Err(unresolved(name, span)),
    }))
}

fn unresolved(name: &str, span: Span) -> BindError {
    BindError::at(format!("parameter {name} has no value"), span)
}

/// A literal as a value of `ty`, or an error pointing at the literal.
fn coerce(lit: &Literal, span: Span, ty: &Type) -> Result<Value, BindError> {
    let mismatch = || {
        let what = match lit {
            Literal::Number(_) => "a number",
            Literal::String(_) => "a string",
            Literal::Bool(_) => "a boolean",
            Literal::Null => "NULL",
            Literal::Param { .. } | Literal::Bound { .. } => "a parameter",
        };
        BindError::at(format!("cannot compare {ty} with {what}"), span)
    };
    match lit {
        // typed as declared; compared with the column's values as two columns would be
        Literal::Bound {
            name,
            ty: declared,
            value,
        } => {
            return if declared.class().is_some() && declared.class() == ty.class() {
                Ok(value.clone())
            } else {
                Err(BindError::at(
                    format!("cannot compare {ty} with parameter {name} ({declared})"),
                    span,
                ))
            }
        }
        Literal::Param { name, .. } => return Err(unresolved(name, span)),
        _ => {}
    }
    ty.literal(lit).map_err(|e| match e {
        LiteralError::Mismatch => mismatch(),
        LiteralError::Invalid(message) => BindError::at(message, span),
    })
}

/// SELECT: `*` is every column of the table; a list must name existing columns, once each.
fn bind_select(query: &Query, table: &BoundTable) -> Result<Vec<Projected>, BindError> {
    let items: Vec<(&ColumnRef, Option<&String>)> = match &query.projection {
        Projection::Wildcard => {
            return Ok(table
                .columns
                .iter()
                .map(|c| Projected {
                    column: c.id,
                    name: c.name.clone(),
                })
                .collect())
        }
        Projection::Columns(refs) => refs.iter().map(|r| (r, None)).collect(),
        Projection::Items(items) => items
            .iter()
            .map(|i| match &i.expr {
                SelectExpr::Column(c) => Ok((c, i.alias.as_ref())),
                SelectExpr::Literal(_, span) => Err(BindError::at(
                    "selecting constants is not supported yet",
                    *span,
                )),
                SelectExpr::Call { .. } => unreachable!("lists with calls are aggregations"),
            })
            .collect::<Result<_, _>>()?,
    };
    let mut out: Vec<Projected> = Vec::with_capacity(items.len());
    for (r, alias) in items {
        let column = resolve_column(table, r)?;
        let name = match alias {
            None => r.name.clone(),
            Some(alias) => {
                check_reserved(alias, r.span)?;
                // In ClickHouse an alias shadows a column of the same name in WHERE and ORDER BY,
                // so the history query would read the aliased column where the stream reads the
                // table's: not allowed until aliases can be referred to.
                if let Some(other) = table.columns.iter().find(|c| &c.name == alias) {
                    if other.id != column {
                        return Err(BindError::at(
                            format!(
                                "alias {alias} is also a column of {}; pick another name",
                                table.name
                            ),
                            r.span,
                        ));
                    }
                }
                alias.clone()
            }
        };
        if out.iter().any(|o| o.name == name) {
            return Err(BindError::at(
                format!("duplicate output column {name}; name it with AS"),
                r.span,
            ));
        }
        out.push(Projected { column, name });
    }
    Ok(out)
}

/// Names starting with `_` are the Kafka columns' (`_partition`, `_offset`) and the engine's
/// hidden fields (`__order_0`, `__k0`, …).
fn check_reserved(alias: &str, span: Span) -> Result<(), BindError> {
    if alias.starts_with('_') {
        return Err(BindError::at(
            format!("alias {alias}: names starting with _ are reserved"),
            span,
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::calendar::Interval;
    use crate::diff::Row;
    use crate::history::DescribedColumn;
    use crate::parser::parse;
    use crate::rules::ConfiguredRules;
    use crate::rules::TopicId;
    use crate::security::TableFilter;
    use crate::source::MemorySource;
    use crate::types::{parse_datetime, DateTime};
    use async_trait::async_trait;

    struct FakeHistory;

    #[async_trait]
    impl HistorySource for FakeHistory {
        async fn describe(&self, table: &str) -> Result<Vec<DescribedColumn>, HistoryError> {
            let col = |n: &str, ty: &str| DescribedColumn {
                name: n.into(),
                ty: ty.into(),
            };
            match table {
                "default.events" => Ok(vec![
                    col("ts", "DateTime64(3)"),
                    col("user_id", "String"),
                    col("latency_ms", "UInt32"),
                    col("coupon", "Nullable(String)"),
                    col("paid", "Bool"),
                    col("tags", "Array(String)"),
                    col("_partition", "UInt64"),
                    col("_offset", "UInt64"),
                ]),
                "default.orders" => Ok(vec![col("order_id", "String")]),
                "default.plain" => Ok(vec![col("id", "String"), col("_partition", "UInt64")]),
                "default.sessions" => Ok(vec![
                    col("id", "UUID"),
                    col("parent", "Nullable(UUID)"),
                    col("user_id", "String"),
                ]),
                "default.products" => Ok(vec![
                    col("product_id", "String"),
                    col("version", "UInt64"),
                    col("deleted", "UInt8"),
                ]),
                "default.ledger" => Ok(vec![col("user_id", "String"), col("sign", "Int8")]),
                "default.items" => Ok(vec![
                    col("item_id", "String"),
                    col("name", "String"),
                    col("_version", "UInt64"),
                ]),
                "default.lowered" => Ok(vec![col("k", "String")]),
                _ => Err(HistoryError::UnknownTable(table.into())),
            }
        }
        async fn table_engine(&self, _: &str, table: &str) -> Result<TableEngine, HistoryError> {
            Ok(match table {
                "products" => TableEngine::Replacing {
                    ver: Some("version".into()),
                    is_deleted: Some("deleted".into()),
                    key: Key::from_system("product_id", ""),
                },
                // its version column hidden by the naming rules (`_*`)
                "items" => TableEngine::Replacing {
                    ver: Some("_version".into()),
                    is_deleted: None,
                    key: Key::from_system("item_id", ""),
                },
                "lowered" => TableEngine::Replacing {
                    ver: None,
                    is_deleted: None,
                    key: Key::from_system("lower(k)", ""),
                },
                "ledger" => TableEngine::Other("CollapsingMergeTree".into()),
                _ => TableEngine::MergeTree,
            })
        }
        async fn fetch(&self, _sql: &str) -> Result<Vec<Row>, HistoryError> {
            unreachable!("the binder never fetches")
        }
    }

    /// Only `events`, `plain`, `sessions`, `products` and `ledger` have a stream.
    fn topics() -> MemorySource {
        let mut source = MemorySource::new();
        source
            .partition("events", 0)
            .partition("plain", 0)
            .partition("sessions", 0)
            .partition("products", 0)
            .partition("ledger", 0)
            .partition("items", 0)
            .partition("lowered", 0);
        source
    }

    async fn bind_sql(sql: &str) -> Result<BoundQuery, String> {
        bind_params(sql, serde_json::json!({})).await
    }

    async fn bind_params(sql: &str, params: serde_json::Value) -> Result<BoundQuery, String> {
        bind_with(ConfiguredRules::default(), sql, params).await
    }

    async fn bind_with(
        rules: ConfiguredRules,
        sql: &str,
        params: serde_json::Value,
    ) -> Result<BoundQuery, String> {
        bind_filtered(rules, sql, params, &Filters::new()).await
    }

    async fn bind_filtered(
        rules: ConfiguredRules,
        sql: &str,
        params: serde_json::Value,
        filters: &Filters,
    ) -> Result<BoundQuery, String> {
        parse(sql).map_err(|e| e.render(sql))?;
        bind_raw(rules, sql, params, filters)
            .await
            .map_err(|e| e.render(sql))
    }

    async fn bind_raw(
        rules: ConfiguredRules,
        sql: &str,
        params: serde_json::Value,
        filters: &Filters,
    ) -> Result<BoundQuery, BindError> {
        let params: Params = serde_json::from_value(params).unwrap();
        let catalog = Catalog {
            rules: &rules,
            history: &FakeHistory,
            source: &topics(),
            params: &params,
            filters,
        };
        let q = parse(sql).expect("parses");
        bind(&q, &catalog).await
    }

    fn configured(tables: serde_json::Value) -> ConfiguredRules {
        ConfiguredRules::new(serde_json::from_value(tables).unwrap()).unwrap()
    }

    #[tokio::test]
    async fn replacing_merge_trees_are_read_without_final_and_other_engines_refused() {
        let q = bind_sql("select product_id, version from products")
            .await
            .unwrap();
        assert!(matches!(q.table.engine, TableEngine::Replacing { .. }));
        assert_eq!(q.table.final_key, None);
        // FINAL on a MergeTree changes nothing
        let plain = bind_sql("select ts from events where latency_ms > 5")
            .await
            .unwrap();
        let final_ = bind_sql("select ts from events final where latency_ms > 5")
            .await
            .unwrap();
        assert_eq!(final_, plain);
        assert_eq!(
            bind_sql("select * from ledger").await.unwrap_err(),
            "table ledger is a CollapsingMergeTree table; chlive reads MergeTree and \
             ReplacingMergeTree tables at 1:15"
        );
        assert_eq!(
            bind_sql("select ts from events where user_id in (select user_id from ledger)")
                .await
                .unwrap_err(),
            "table ledger is a CollapsingMergeTree table; chlive reads MergeTree and \
             ReplacingMergeTree tables at 1:61"
        );
        assert_eq!(
            bind_sql(
                "select ts from events where user_id in (select product_id from products final)"
            )
            .await
            .unwrap_err(),
            "FINAL on products (ReplacingMergeTree) in a sub-query isn't supported yet at 1:73"
        );
    }

    #[tokio::test]
    async fn final_on_a_replacing_merge_tree_binds_its_key_version_and_deleted_columns() {
        let q = bind_sql("select product_id from products final where version > 1")
            .await
            .unwrap();
        let col = |id: ColumnId| q.table.column(id).clickhouse.clone();
        let k = q.table.final_key.clone().expect("a FINAL key");
        assert_eq!(
            (
                k.key.iter().map(|c| col(*c)).collect::<Vec<_>>(),
                k.ver.map(col),
                k.deleted.map(col)
            ),
            (
                vec!["product_id".to_string()],
                Some("version".into()),
                Some("deleted".into())
            )
        );
        assert!(q.table.internal.is_empty());
        // a hidden version column is internal: the engine reads it, queries can't
        let q = bind_sql("select * from items final").await.unwrap();
        let k = q.table.final_key.clone().unwrap();
        let ver = q.table.column(k.ver.unwrap());
        assert_eq!((ver.clickhouse.as_str(), ver.id), ("_version", ColumnId(2)));
        assert_eq!(q.table.width(), 3);
        assert_eq!(
            q.output().map(|c| c.name).collect::<Vec<_>>(),
            ["item_id", "name"]
        );
        assert!(bind_sql("select _version from items final").await.is_err());
    }

    #[tokio::test]
    async fn final_refuses_the_shapes_it_does_not_follow_yet() {
        let err = |sql: &'static str| async move { bind_sql(sql).await.unwrap_err() };
        let unsupported = |what: &str, at: &str| {
            format!(
                "FINAL on products (ReplacingMergeTree) with {what} isn't supported yet at {at}"
            )
        };
        // ORDER BY is followed, LIMIT not yet
        assert!(
            bind_sql("select * from products final order by version desc")
                .await
                .is_ok()
        );
        assert!(
            bind_sql("select * from products final order by version limit 5")
                .await
                .is_ok()
        );
        // LIMIT alone is followed
        assert!(bind_sql("select * from products final limit 5")
            .await
            .is_ok());
        assert!(bind_sql("select * from products final limit 0")
            .await
            .is_ok());
        // OFFSET too, with or without LIMIT and ORDER BY
        for sql in [
            "select * from products final offset 5",
            "select * from products final limit 2 offset 5",
            "select * from products final order by version limit 2 offset 5",
            "select * from products final order by version offset 5",
        ] {
            assert!(bind_sql(sql).await.is_ok(), "{sql}");
        }
        // native aggregates, grouped or not
        for sql in [
            "select deleted, count(), sum(version), avg(version), min(version), max(version) \
             from products final group by deleted",
            "select count() from products final",
            "select deleted, count() as n from products final group by deleted \
             order by n desc limit 3",
        ] {
            assert!(bind_sql(sql).await.is_ok(), "{sql}");
        }
        assert_eq!(
            err("select deleted, uniq(version) from products final group by deleted").await,
            unsupported("uniq()", "1:45").replace(
                " at 1:45",
                " (on FINAL: count, sum, avg, min, max, groupArray) at 1:45"
            )
        );
        assert_eq!(
            err("select * from products final where product_id in (select item_id from items)")
                .await,
            unsupported("a sub-query", "1:24")
        );
        assert_eq!(
            err("select * from lowered final").await,
            "FINAL on lowered: the sorting key has an expression (lower(k)) at 1:23"
        );
        // without FINAL it reads like a MergeTree
        assert!(bind_sql("select * from lowered").await.is_ok());
    }

    /// The intervals' total, a day as 24 hours.
    fn shift_of(intervals: &[Interval]) -> i128 {
        crate::calendar::shift(0, intervals, chrono_tz::Tz::UTC)
    }

    #[tokio::test]
    async fn now_takes_a_timezone() {
        let q = bind_sql("select ts from events where ts > now('Europe/Warsaw') - interval 1 day")
            .await
            .unwrap();
        assert_eq!(q.windows[0].tz, Some(chrono_tz::Europe::Warsaw));
        assert_eq!(shift_of(&q.windows[0].intervals), -86_400_000_000_000);
        let q = bind_sql("select ts from events where ts > now()")
            .await
            .unwrap();
        assert_eq!(q.windows[0].tz, None, "the server's");
        assert_eq!(
            bind_sql("select ts from events where ts > now('Europe/Warsw')")
                .await
                .unwrap_err(),
            "unknown timezone 'Europe/Warsw' at 1:34"
        );
    }

    /// `now()` comparisons bind anywhere in the WHERE, the column on the left, each with its
    /// own window.
    #[tokio::test]
    async fn now_comparisons_bind_anywhere_in_the_where() {
        const H: i128 = 3_600_000_000_000;
        let q = bind_sql(
            "select user_id from events where latency_ms > 5 \
             and now() - interval 1 hour < ts and ts >= now() - interval 2 hour",
        )
        .await
        .unwrap();
        let ts = ColumnId(0);
        assert_eq!(
            q.windows
                .iter()
                .map(|w| (w.column, shift_of(&w.intervals)))
                .collect::<Vec<_>>(),
            [(ts, -H), (ts, -2 * H)]
        );
        let mut nows = Vec::new();
        q.filter
            .as_ref()
            .unwrap()
            .visit_now(&mut |w, op, c| nows.push((w, op, c)));
        // `now() - 1h < ts` is `ts > now() - 1h`
        assert_eq!(nows, [(0, CmpOp::Gt, ts), (1, CmpOp::GtEq, ts)]);
        for sql in [
            "select ts from events where ts > now() - interval 1 hour or latency_ms > 100",
            "select ts from events where ts <= now()",
            "select ts from events where not (ts > now() - interval 5 second)",
            "select ts from events where (ts < now() and latency_ms > 1) or ts > now() + interval 1 day",
            "select ts from events where ts > now() order by latency_ms",
            // the newest first, then LIMIT: rows leave from the bottom
            "select ts from events where ts > now() - interval 1 minute order by ts desc limit 5",
        ] {
            let q = bind_sql(sql).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
            assert!(!q.windows.is_empty(), "{sql}");
        }
    }

    #[tokio::test]
    async fn now_binds_with_native_aggregates() {
        for sql in [
            "select count() from events where ts > now() - interval 1 minute",
            "select user_id, count() as n, avg(latency_ms) from events \
             where ts > now() - interval 1 minute group by user_id order by n desc limit 3 offset 1",
        ] {
            let q = bind_sql(sql).await.unwrap_or_else(|e| panic!("{sql}: {e}"));
            assert_eq!(q.windows.len(), 1, "{sql}");
            assert!(q.aggregation.is_some());
        }
    }

    #[tokio::test]
    async fn now_is_refused_where_time_windows_dont_reach_yet() {
        let cases = [
            (
                "select ts from events where ts = now()",
                "`column = now()` is true for an instant (or all but one); use <, <=, > or >= at 1:34",
            ),
            (
                "select ts from events where now() > 5",
                "now() is compared with a DateTime or DateTime64 column at 1:37",
            ),
            (
                "select now() from events",
                "now() is supported only compared with a DateTime column (`ts > now() - INTERVAL 1 HOUR`), in the WHERE of the main query at 1:8",
            ),
            (
                "select ts from events where ts > now() - interval 1 hour or latency_ms > 1 \
                 order by ts desc limit 3",
                "now() with LIMIT where rows can come in or come back (only `column > now() …` as top-level AND conditions) isn't supported yet at 1:34",
            ),
            (
                "select ts from events where user_id > now()",
                "now() is compared with DateTime or DateTime64 columns; user_id is String at 1:29",
            ),
            (
                "select uniq(user_id) from events where ts > now()",
                "now() with uniq() isn't supported yet (with now(): count, sum, avg, min, max, groupArray) at 1:45",
            ),
            (
                "select ts from events where ts > now() limit 3 offset 1",
                "now() with OFFSET isn't supported yet at 1:34",
            ),
            (
                "select ts from events where ts > now() limit 3",
                "now() with LIMIT needs ORDER BY ts DESC first (rows leave from the bottom) at 1:34",
            ),
            (
                "select ts from events where ts > now() order by ts limit 3",
                "now() with LIMIT needs ORDER BY ts DESC first (rows leave from the bottom) at 1:34",
            ),
            (
                "select ts from events where ts > now() and user_id in (select user_id from sessions)",
                "now() with a sub-query isn't supported yet at 1:34",
            ),
        ];
        for (sql, want) in cases {
            assert_eq!(bind_sql(sql).await.unwrap_err(), want, "{sql}");
        }
    }

    #[tokio::test]
    async fn only_exposed_tables_can_be_queried() {
        let rules = || configured(serde_json::json!({ "events": {} }));
        assert!(
            bind_with(rules(), "select ts from events", serde_json::json!({}))
                .await
                .is_ok()
        );
        let e = bind_with(rules(), "select id from plain", serde_json::json!({}))
            .await
            .unwrap_err();
        assert!(
            e.contains("unknown table plain (not in the server's config)"),
            "{e}"
        );
        assert!(e.contains("1:16"), "at the table: {e}");
        // and in sub-queries too
        let e = bind_with(
            rules(),
            "select ts from events where user_id in (select id from plain)",
            serde_json::json!({}),
        )
        .await
        .unwrap_err();
        assert!(e.contains("unknown table plain"), "{e}");
    }

    #[tokio::test]
    async fn renamed_and_hidden_columns_follow_the_mapping() {
        let rules = configured(serde_json::json!({
            "events": {
                "hidden_columns": ["coupon"],
                "columns": {
                    "who": { "clickhouse": "user_id", "json": "uid" },
                    "latency": { "clickhouse": "latency_ms" },
                },
            },
        }));
        let b = bind_with(
            rules,
            "select who, latency from events where who = 'u1' and latency > 5",
            serde_json::json!({}),
        )
        .await
        .unwrap();
        let out: Vec<_> = b
            .output()
            .map(|c| (c.name, c.clickhouse, c.source.to_string()))
            .collect();
        assert_eq!(
            out,
            [
                ("who".into(), "user_id".into(), "uid".into()),
                ("latency".into(), "latency_ms".into(), "latency_ms".into()),
            ]
        );
        let all: Vec<_> = b.table.columns.iter().map(|c| c.name.as_str()).collect();
        assert_eq!(
            all,
            ["ts", "who", "latency", "paid", "tags"],
            "coupon and _* hidden"
        );
        // the old names are gone
        let rules = configured(serde_json::json!({
            "events": { "columns": { "who": { "clickhouse": "user_id" } } },
        }));
        let e = bind_with(rules, "select user_id from events", serde_json::json!({}))
            .await
            .unwrap_err();
        assert!(e.contains("user_id"), "{e}");
    }

    #[tokio::test]
    async fn kafka_columns_are_visible_and_typed() {
        let rules = configured(serde_json::json!({
            "events": { "columns": {
                "kafka_offset": { "clickhouse": "_offset", "kafka": "offset" },
                "kafka_partition": { "clickhouse": "_partition", "kafka": "partition" },
                "kafka_ts": { "clickhouse": "ts", "kafka": "timestamp" },
            }},
        }));
        let b = bind_with(
            rules,
            "select kafka_partition, kafka_offset, kafka_ts from events where kafka_offset > 5",
            serde_json::json!({}),
        )
        .await
        .unwrap();
        let out: Vec<_> = b.output().map(|c| (c.name, c.source.to_string())).collect();
        assert_eq!(
            out,
            [
                ("kafka_partition".into(), "kafka partition".into()),
                ("kafka_offset".into(), "kafka offset".into()),
                ("kafka_ts".into(), "kafka timestamp".into()),
            ],
            "configured from Kafka: shown, though `_*` is hidden"
        );
        // a column that can't hold the field as Kafka Connect stores it
        for (column, field, ty) in [
            ("latency_ms", "timestamp", "UInt32"),
            ("user_id", "offset", "String"),
        ] {
            let rules = configured(serde_json::json!({
                "events": { "columns": { "k": { "clickhouse": column, "kafka": field } } },
            }));
            let e = bind_with(rules, "select k from events", serde_json::json!({}))
                .await
                .unwrap_err();
            assert!(
                e.contains(&format!("column k of events holds the Kafka {field}"))
                    && e.contains(&format!("not {ty}")),
                "{e}"
            );
            assert!(e.contains("1:15"), "at the table: {e}");
        }
    }

    #[tokio::test]
    async fn a_rename_onto_another_column_is_an_error() {
        let rules = configured(serde_json::json!({
            "events": { "columns": { "ts": { "clickhouse": "user_id" } } },
        }));
        let e = bind_with(rules, "select ts from events", serde_json::json!({}))
            .await
            .unwrap_err();
        assert!(e.contains("two columns named ts"), "{e}");
    }

    fn names(b: &BoundQuery) -> Vec<String> {
        b.output().map(|c| c.name).collect()
    }

    #[tokio::test]
    async fn from_resolves_clickhouse_table_topic_and_visible_columns() {
        let b = bind_sql("select * from events").await.unwrap();
        assert_eq!(b.table.clickhouse_table, "default.events");
        assert_eq!(b.table.topic, TopicId("events".into()));
        assert_eq!(
            names(&b),
            ["ts", "user_id", "latency_ms", "coupon", "paid", "tags"]
        );
        assert_eq!(b.table.column(ColumnId(2)).ty, Type::parse("UInt32"));
    }

    #[tokio::test]
    async fn select_list_keeps_written_order() {
        let b = bind_sql("select latency_ms, ts from events").await.unwrap();
        assert_eq!(names(&b), ["latency_ms", "ts"]);
        let ids: Vec<ColumnId> = b.projection.iter().map(|p| p.column).collect();
        assert_eq!(ids, [ColumnId(2), ColumnId(0)]);
    }

    #[tokio::test]
    async fn aliases_name_output_columns() {
        let b = bind_sql("select user_id as u, latency_ms from events")
            .await
            .unwrap();
        assert_eq!(names(&b), ["u", "latency_ms"]);
        let out: Vec<BoundColumn> = b.output().collect();
        assert_eq!(
            (out[0].id, out[0].clickhouse.as_str()),
            (ColumnId(1), "user_id")
        );
        // a column twice, under two names; and an alias that is its own name
        let b = bind_sql("select ts, ts as t, latency_ms as latency_ms from events")
            .await
            .unwrap();
        assert_eq!(names(&b), ["ts", "t", "latency_ms"]);
    }

    #[tokio::test]
    async fn parameters_bind_as_typed_values() {
        use serde_json::json;
        let b = bind_params(
            "select * from sessions where id = {id:UUID} and user_id in ({u:String}, 'x')",
            json!({"id": "ABCDEF00-0000-0000-0000-000000000001", "u": "u1"}),
        )
        .await
        .unwrap();
        let uuid = Value::Uuid(crate::types::Uuid(0xabcdef00 << 96 | 1));
        let Some(BoundExpr::And(l, r)) = b.filter else {
            panic!("an AND: {:?}", b.filter)
        };
        assert!(
            matches!(*l, BoundExpr::Compare { ref right, .. } if **right == BoundExpr::Literal(uuid.clone()))
        );
        assert!(matches!(*r, BoundExpr::InList { ref list, .. }
            if list == &[Value::String("u1".into()), Value::String("x".into())]));
        // values read by the declared type: a number as a string, a DateTime compared with a
        // DateTime64 column, one placeholder used twice, and in a sub-query
        for (sql, params) in [
            ("select * from events where latency_ms > {l:UInt32}", json!({"l": "500"})),
            ("select * from events where ts >= {t:DateTime}", json!({"t": "2026-10-01 00:00:00"})),
            ("select * from events where latency_ms > {l:UInt32} or latency_ms = {l:UInt32}", json!({"l": 5})),
            ("select * from sessions where parent in (select id from sessions where user_id = {u:String})", json!({"u": "u1"})),
            ("select * from sessions where parent = {p:Nullable(UUID)}", json!({"p": null})),
        ] {
            assert!(bind_params(sql, params.clone()).await.is_ok(), "{sql} {params}: {:?}", bind_params(sql, params.clone()).await);
        }
    }

    #[tokio::test]
    async fn parameters_render_into_history_and_the_cursor_fingerprint() {
        use serde_json::json;
        let sql = "select id from sessions where id = {id:UUID}";
        let planned = |id: &'static str| async move {
            crate::plan::plan(&bind_params(sql, json!({ "id": id })).await.unwrap())
        };
        let a = planned("ABCDEF00-0000-0000-0000-000000000001").await;
        let b = planned("abcdef00-0000-0000-0000-000000000002").await;
        assert_eq!(
            a.history.sql("default.sessions"),
            "SELECT id FROM default.sessions WHERE id = toUUID('abcdef00-0000-0000-0000-000000000001')"
        );
        assert_ne!(
            crate::cursor::fingerprint(&a),
            crate::cursor::fingerprint(&b)
        );
    }

    fn rows(table: &str, rows: RowFilter) -> Filters {
        Filters::from([(
            table.to_string(),
            TableFilter {
                rows,
                columns: None,
            },
        )])
    }

    fn only(table: &str, column: &str, values: &[&str]) -> Filters {
        rows(
            table,
            RowFilter::In {
                column: column.into(),
                values: values.iter().map(|v| v.to_string()).collect(),
            },
        )
    }

    async fn history_of(rules: ConfiguredRules, sql: &str, filters: &Filters) -> String {
        let b = bind_filtered(rules, sql, serde_json::json!({}), filters)
            .await
            .unwrap();
        crate::plan::plan(&b).history.sql(&b.table.clickhouse_table)
    }

    #[tokio::test]
    async fn row_filters_are_anded_into_the_table() {
        let f = only("events", "user_id", &["u1", "u2"]);
        assert_eq!(
            history_of(
                ConfiguredRules::default(),
                "select ts from events where latency_ms > 5",
                &f
            )
            .await,
            "SELECT ts FROM default.events WHERE (latency_ms > 5) AND (user_id IN ('u1', 'u2'))"
        );
        // no rows: an empty list, or none at all
        for f in [
            only("events", "user_id", &[]),
            rows("events", RowFilter::None),
        ] {
            assert_eq!(
                history_of(ConfiguredRules::default(), "select ts from events", &f).await,
                "SELECT ts FROM default.events WHERE false"
            );
        }
        // every row, or another table's filter: nothing added
        for f in [
            rows("events", RowFilter::All),
            only("orders", "order_id", &["o1"]),
        ] {
            assert_eq!(
                history_of(ConfiguredRules::default(), "select ts from events", &f).await,
                "SELECT ts FROM default.events"
            );
        }
    }

    #[tokio::test]
    async fn row_filters_reach_hidden_columns_and_sub_queries() {
        // a hidden column: read for the filter, still not selectable
        let rules =
            || configured(serde_json::json!({ "events": { "hidden_columns": ["user_id"] } }));
        let f = only("events", "user_id", &["u1"]);
        assert_eq!(
            history_of(rules(), "select ts from events", &f).await,
            "SELECT ts FROM default.events WHERE user_id IN ('u1')"
        );
        assert!(bind_filtered(
            rules(),
            "select user_id from events",
            serde_json::json!({}),
            &f
        )
        .await
        .is_err());
        // the sub-query's table too, in IN and in EXISTS
        let f = only("sessions", "user_id", &["u1"]);
        for sql in [
            "select ts from events where user_id in (select user_id from sessions)",
            "select ts from events e where exists (select 1 from sessions l where l.user_id = e.user_id)",
        ] {
            let b = bind_filtered(ConfiguredRules::default(), sql, serde_json::json!({}), &f)
                .await
                .unwrap();
            let semi = b.semi_join.expect("a semi-join");
            assert!(
                matches!(semi.inner_filter, Some(BoundExpr::InList { ref list, .. }) if list.len() == 1),
                "{sql}: {:?}",
                semi.inner_filter
            );
            assert_eq!(b.filter, None);
        }
    }

    #[tokio::test]
    async fn row_filters_tell_plans_apart_and_check_their_column() {
        let id = |f: Filters| async move {
            let b = bind_filtered(
                ConfiguredRules::default(),
                "select ts from events",
                serde_json::json!({}),
                &f,
            )
            .await
            .unwrap();
            crate::cursor::identity(&crate::plan::plan(&b))
        };
        let a = id(only("events", "user_id", &["u1"])).await;
        assert_ne!(a, id(only("events", "user_id", &["u2"])).await);
        assert_eq!(a, id(only("events", "user_id", &["u1"])).await);
        let err = |f: Filters| async move {
            bind_filtered(
                ConfiguredRules::default(),
                "select ts from events",
                serde_json::json!({}),
                &f,
            )
            .await
            .unwrap_err()
        };
        assert!(err(only("events", "nope", &["x"]))
            .await
            .contains("column nope isn't in ClickHouse"));
        assert!(err(only("events", "latency_ms", &["x"]))
            .await
            .contains("isn't a UInt32"));
    }

    fn columns(table: &str, allowed: &[&str]) -> Filters {
        Filters::from([(
            table.to_string(),
            TableFilter {
                rows: RowFilter::All,
                columns: Some(allowed.iter().map(|c| c.to_string()).collect()),
            },
        )])
    }

    #[tokio::test]
    async fn denied_columns_cant_be_named_and_star_skips_them() {
        let f = columns("events", &["ts", "user_id"]);
        let star = bind_filtered(
            ConfiguredRules::default(),
            "select * from events",
            serde_json::json!({}),
            &f,
        )
        .await
        .unwrap();
        assert_eq!(names(&star), ["ts", "user_id"]);
        // anywhere: the select list, WHERE, ORDER BY, GROUP BY, an aggregate, sub-query keys
        for (sql, f) in [
            ("select latency_ms from events", &f),
            ("select ts from events where latency_ms > 5", &f),
            ("select ts from events order by latency_ms", &f),
            ("select latency_ms, count() from events group by latency_ms", &f),
            ("select sum(latency_ms) from events", &f),
            ("select id from sessions where user_id in (select coupon from events)", &f),
            (
                "select id from sessions s where exists (select 1 from events e where e.coupon = s.user_id)",
                &f,
            ),
            (
                "select ts from events where user_id in (select user_id from sessions where parent is null)",
                &columns("sessions", &["id", "user_id"]),
            ),
        ] {
            match bind_raw(ConfiguredRules::default(), sql, serde_json::json!({}), f).await {
                Err(e @ BindError::Denied { .. }) => {
                    assert!(e.render(sql).contains("isn't readable here at 1:"), "{sql}")
                }
                other => panic!("{sql}: {other:?}"),
            }
        }
        // an unknown column is still unknown
        let e = bind_filtered(
            ConfiguredRules::default(),
            "select nope from events",
            serde_json::json!({}),
            &f,
        )
        .await
        .unwrap_err();
        assert!(e.starts_with("unknown column nope"), "{e}");
    }

    #[tokio::test]
    async fn denied_columns_still_serve_row_filters_and_final() {
        // the row filter's column, not visible
        let mut f = columns("events", &["ts"]);
        f.get_mut("events").unwrap().rows = RowFilter::In {
            column: "user_id".into(),
            values: vec!["u1".into()],
        };
        assert_eq!(
            history_of(ConfiguredRules::default(), "select * from events", &f).await,
            "SELECT ts FROM default.events WHERE user_id IN ('u1')"
        );
        // FINAL's key, version and deleted columns, not visible
        let b = bind_filtered(
            ConfiguredRules::default(),
            "select * from products final",
            serde_json::json!({}),
            &columns("products", &["product_id"]),
        )
        .await
        .unwrap();
        assert_eq!(names(&b), ["product_id"]);
        assert!(b.table.final_key.is_some());
    }

    #[tokio::test]
    async fn column_sets_tell_star_plans_apart_only() {
        let id = |sql: &'static str, f: Filters| async move {
            let b = bind_filtered(ConfiguredRules::default(), sql, serde_json::json!({}), &f)
                .await
                .unwrap();
            crate::cursor::identity(&crate::plan::plan(&b))
        };
        let (a, b) = (
            columns("events", &["ts", "user_id"]),
            columns("events", &["ts", "user_id", "paid"]),
        );
        assert_ne!(
            id("select * from events", a.clone()).await,
            id("select * from events", b.clone()).await
        );
        assert_eq!(
            id("select ts from events", a).await,
            id("select ts from events", b).await
        );
    }

    #[tokio::test]
    async fn the_plan_identity_tells_queries_apart() {
        let id = |sql: &'static str| async move {
            crate::cursor::identity(&crate::plan::plan(&bind_sql(sql).await.unwrap()))
        };
        // aggregates over the same table and filter: their plain history is the same, their
        // identities aren't
        let by_user = id("select user_id, count() from events group by user_id").await;
        let global = id("select count(), sum(latency_ms) from events").await;
        let top =
            id("select user_id, count() as n from events group by user_id order by n desc limit 3")
                .await;
        assert_ne!(by_user, global);
        assert_ne!(by_user, top);
        assert_ne!(global, id("select count() from events").await);
        // the same query, written differently (keywords, spacing): the same identity
        assert_eq!(
            by_user,
            id("SELECT user_id,   count()  FROM events  GROUP BY user_id").await
        );
        assert_eq!(
            id("select ts from events where latency_ms > 5").await,
            id("select ts from events where latency_ms>5").await
        );
        // output names are part of it: `COUNT()` is another column than `count()`
        assert_ne!(
            by_user,
            id("select user_id, COUNT() from events group by user_id").await
        );
    }

    #[tokio::test]
    async fn parameter_errors() {
        use serde_json::json;
        let err = |sql: &'static str, params: serde_json::Value| async move {
            bind_params(sql, params).await.unwrap_err()
        };
        assert_eq!(
            err("select * from sessions where id = {id:UUID}", json!({})).await,
            "missing parameter id at 1:35"
        );
        assert_eq!(
            err(
                "select * from sessions where id = {id:UUID}",
                json!({"id": "nope"})
            )
            .await,
            r#"parameter id: cannot read "nope" as UUID at 1:35"#
        );
        assert_eq!(
            err(
                "select * from sessions where id = {id:UUID}",
                json!({"id": null})
            )
            .await,
            "parameter id is null, but UUID isn't Nullable at 1:35"
        );
        assert_eq!(
            err(
                "select * from sessions where id = {id:UUID}",
                json!({"id": "abcdef00000000000000000000000001", "x": 1})
            )
            .await,
            "unknown parameter x (not in the query)"
        );
        assert_eq!(
            err(
                "select * from events where latency_ms = {x:String}",
                json!({"x": "a"})
            )
            .await,
            "cannot compare UInt32 with parameter x (String) at 1:41"
        );
        assert_eq!(
            err(
                "select * from events where user_id = {t:Array(String)}",
                json!({"t": []})
            )
            .await,
            "parameter t: type Array(String) is not supported at 1:38"
        );
        assert_eq!(
            err(
                "select * from events where latency_ms = {l:UInt32} or latency_ms = {l:UInt64}",
                json!({"l": 1})
            )
            .await,
            "parameter l is declared as both UInt32 and UInt64 at 1:68"
        );
        assert_eq!(
            err(
                "select * from sessions where parent in ({p:Nullable(UUID)})",
                json!({"p": null})
            )
            .await,
            "parameter p is null, which never matches in IN; use IS NULL at 1:41"
        );
    }

    #[tokio::test]
    async fn uuid_columns_filter_order_group_and_join() {
        for sql in [
            "select * from sessions where id = 'ABCDEF00-0000-0000-0000-000000000001'",
            "select * from sessions where id in ('abcdef00000000000000000000000001') and parent is null",
            "select * from sessions where id > parent order by id desc limit 3",
            "select parent, count() from sessions group by parent order by parent",
            "select min(id), max(parent) from sessions",
            "select * from sessions where parent in (select id from sessions where user_id = 'u1')",
        ] {
            assert!(bind_sql(sql).await.is_ok(), "{sql}: {:?}", bind_sql(sql).await);
        }
        let b =
            bind_sql("select * from sessions where id = 'ABCDEF00-0000-0000-0000-000000000001'")
                .await
                .unwrap();
        assert!(b.filter.as_ref().is_some_and(|f| matches!(f,
            BoundExpr::Compare { right, .. }
                if right.as_ref() == &BoundExpr::Literal(Value::Uuid(crate::types::Uuid(0xabcdef00 << 96 | 1)))
        )));
        assert_eq!(
            bind_sql("select * from sessions where id = 'nope'")
                .await
                .unwrap_err(),
            "invalid UUID value 'nope' (expected 8-4-4-4-12 hex digits) at 1:35"
        );
        assert_eq!(
            bind_sql("select * from sessions where id < 5")
                .await
                .unwrap_err(),
            "cannot compare UUID with a number at 1:35"
        );
        assert_eq!(
            bind_sql("select * from sessions where id = user_id")
                .await
                .unwrap_err(),
            "cannot compare UUID with String at 1:33"
        );
        // other unsupported types keep their errors
        assert_eq!(
            bind_sql("select * from events where tags = 'a'")
                .await
                .unwrap_err(),
            "filtering on column tags of type Array(String) is not supported at 1:28"
        );
    }

    #[tokio::test]
    async fn errors_point_at_the_offending_name() {
        assert_eq!(
            bind_sql("select * from nope").await.unwrap_err(),
            "unknown table nope in ClickHouse (default.nope) at 1:15"
        );
        assert_eq!(
            bind_sql("select * from orders").await.unwrap_err(),
            "no event stream for table orders (topic orders) at 1:15"
        );
        // no Kafka bookkeeping columns needed: snapshots line history up with the topic
        assert!(bind_sql("select * from plain").await.is_ok());
        assert_eq!(
            bind_sql("select * from __chlive_snap_events_1_0")
                .await
                .unwrap_err(),
            "unknown table __chlive_snap_events_1_0 (snapshots aren't tables) at 1:15"
        );
        assert_eq!(
            bind_sql("select ts, nope from events").await.unwrap_err(),
            "unknown column nope in table events (columns: ts, user_id, latency_ms, coupon, paid, tags) at 1:12"
        );
        // hidden bookkeeping columns are not part of the dialect table
        assert!(bind_sql("select _offset from events")
            .await
            .unwrap_err()
            .starts_with("unknown column _offset"));
        assert_eq!(
            bind_sql("select ts, user_id, ts from events")
                .await
                .unwrap_err(),
            "duplicate output column ts; name it with AS at 1:21"
        );
        // column names are case-sensitive, like ClickHouse's
        assert!(bind_sql("select TS from events")
            .await
            .unwrap_err()
            .starts_with("unknown column TS"));
    }

    #[tokio::test]
    async fn where_literals_are_coerced_to_the_column_type() {
        let b =
            bind_sql("select user_id from events where ts >= '2026-10-01' and latency_ms > 99.5")
                .await
                .unwrap();
        let Some(BoundExpr::And(l, r)) = b.filter else {
            panic!("{:?}", b.filter)
        };
        assert_eq!(
            *l,
            BoundExpr::Compare {
                op: CmpOp::GtEq,
                left: Box::new(BoundExpr::Column(ColumnId(0))),
                right: Box::new(BoundExpr::Literal(Value::DateTime(DateTime {
                    ticks: parse_datetime("2026-10-01 00:00:00", 3).unwrap(),
                    precision: 3
                }))),
            }
        );
        assert!(
            matches!(*r, BoundExpr::Compare { right, .. } if *right == BoundExpr::Literal(Value::Float(99.5)))
        );
    }

    #[tokio::test]
    async fn where_type_errors_point_at_the_problem() {
        let e = |sql: &'static str| async move { bind_sql(sql).await.unwrap_err() };
        assert_eq!(
            e("select * from events where latency_ms > 'x'").await,
            "cannot compare UInt32 with a string at 1:41"
        );
        assert_eq!(
            e("select * from events where user_id = 1").await,
            "cannot compare String with a number at 1:38"
        );
        assert_eq!(
            e("select * from events where ts < '2026-13-01'").await,
            "invalid DateTime64(3) value '2026-13-01' (expected YYYY-MM-DD[ hh:mm:ss[.fff]]) at 1:33"
        );
        assert_eq!(
            e("select * from events where user_id = latency_ms").await,
            "cannot compare String with UInt32 at 1:36"
        );
        assert_eq!(
            e("select * from events where coupon = null").await,
            "comparing with NULL is never true; use IS NULL at 1:35"
        );
        assert_eq!(
            e("select * from events where coupon in ('a', null)").await,
            "NULL in an IN list never matches; use IS NULL at 1:44"
        );
        assert_eq!(
            e("select * from events where latency_ms").await,
            "column latency_ms (UInt32) is not a condition; compare it with something at 1:28"
        );
        assert_eq!(
            e("select * from events where 1").await,
            "this value is not a condition at 1:28"
        );
        assert_eq!(
            e("select * from events where paid > true").await,
            "booleans can only be compared with = or != at 1:33"
        );
        assert_eq!(
            e("select * from events where tags = 'x'").await,
            "filtering on column tags of type Array(String) is not supported at 1:28"
        );
        assert_eq!(
            e("select * from events where (user_id = 'a') = true").await,
            "only columns and values can be compared at 1:37"
        );
        // FROM, then WHERE, then SELECT: the WHERE error is reported first
        assert!(e("select nope from events where nope2 = 1")
            .await
            .starts_with("unknown column nope2"));
    }

    #[tokio::test]
    async fn bool_columns_and_literals_are_conditions() {
        let b = bind_sql("select * from events where paid and not false")
            .await
            .unwrap();
        assert_eq!(
            b.filter,
            Some(BoundExpr::And(
                Box::new(BoundExpr::Column(ColumnId(4))),
                Box::new(BoundExpr::Not(Box::new(BoundExpr::Literal(Value::Bool(
                    false
                )))))
            ))
        );
    }

    #[tokio::test]
    async fn order_by_resolves_keys_and_defaults() {
        let k = |c: usize, desc, nulls_first| OrderKey {
            column: ColumnId(c),
            desc,
            nulls_first,
        };
        // unselected columns are allowed; NULLS LAST is the default in both directions
        let b =
            bind_sql("select user_id from events order by latency_ms desc, coupon nulls first, ts")
                .await
                .unwrap();
        assert_eq!(
            b.order,
            [k(2, true, false), k(3, false, true), k(0, false, false)]
        );
        // ALL is the SELECT list, in SELECT order, not the table's columns
        let b = bind_sql("select latency_ms, ts from events order by all desc")
            .await
            .unwrap();
        assert_eq!(b.order, [k(2, true, false), k(0, true, false)]);
        assert!(bind_sql("select * from events")
            .await
            .unwrap()
            .order
            .is_empty());
    }

    #[tokio::test]
    async fn order_by_errors() {
        assert_eq!(
            bind_sql("select * from events order by nope").await.unwrap_err(),
            "unknown column nope in table events (columns: ts, user_id, latency_ms, coupon, paid, tags) at 1:31"
        );
        assert_eq!(
            bind_sql("select ts from events order by tags")
                .await
                .unwrap_err(),
            "ordering by column tags of type Array(String) is not supported at 1:32"
        );
        // ALL over a SELECT * that includes an unorderable column
        assert_eq!(
            bind_sql("select * from events order by all")
                .await
                .unwrap_err(),
            "ordering by column tags of type Array(String) is not supported at 1:31"
        );
    }

    #[tokio::test]
    async fn aggregation() {
        let b = bind_sql("select user_id, count() as n, avg(latency_ms), sum(latency_ms), min(ts), max(coupon), count(coupon) from events group by user_id")
            .await
            .unwrap();
        let a = b.aggregation.unwrap();
        assert_eq!(a.keys, [ColumnId(1)]);
        let names: Vec<(&str, &str)> = a
            .items
            .iter()
            .map(|i| (i.name.as_str(), i.ty.clickhouse.as_str()))
            .collect();
        assert_eq!(
            names,
            [
                ("user_id", "String"),
                ("n", "UInt64"),
                ("avg(latency_ms)", "Float64"),
                ("sum(latency_ms)", "UInt64"),
                ("min(ts)", "DateTime64(3)"),
                ("max(coupon)", "Nullable(String)"),
                ("count(coupon)", "UInt64"),
            ]
        );
        assert!(b.projection.is_empty());
        // a global aggregate; a key that isn't selected
        let a = bind_sql("select count() from events")
            .await
            .unwrap()
            .aggregation
            .unwrap();
        assert!(a.keys.is_empty());
        let a = bind_sql("select count() from events group by user_id")
            .await
            .unwrap()
            .aggregation
            .unwrap();
        assert_eq!(a.keys, [ColumnId(1)]);
        assert!(bind_sql("select ts from events")
            .await
            .unwrap()
            .aggregation
            .is_none());
    }

    #[tokio::test]
    async fn group_array_and_names_as_written() {
        let b = bind_sql(
            "select user_id, groupArray(latency_ms), array_agg(coupon), ARRAY_AGG(ts), COUNT(*), Sum(latency_ms) \
             from events group by user_id",
        )
        .await
        .unwrap();
        let items: Vec<(String, String)> = b
            .aggregation
            .unwrap()
            .items
            .iter()
            .map(|i| (i.name.clone(), i.ty.clickhouse.clone()))
            .collect();
        let want = [
            ("user_id", "String"),
            ("groupArray(latency_ms)", "Array(UInt32)"),
            // NULLs are skipped: the elements aren't Nullable
            ("array_agg(coupon)", "Array(String)"),
            ("ARRAY_AGG(ts)", "Array(DateTime64(3))"),
            // ClickHouse's names keep the function as written
            ("COUNT()", "UInt64"),
            ("Sum(latency_ms)", "UInt64"),
        ];
        assert_eq!(items, want.map(|(n, t)| (n.to_string(), t.to_string())));
        // arrays have no order to sort groups by
        let e =
            bind_sql("select user_id, groupArray(ts) as g from events group by user_id order by g")
                .await
                .unwrap_err();
        assert!(
            e.starts_with("ordering by g of type Array(DateTime64(3)) is not supported"),
            "{e}"
        );
        // and it's native
        assert!(crate::aggregate::native(AggFunc::GroupArray).is_some());
    }

    #[tokio::test]
    async fn aggregation_errors() {
        let e = |sql: &'static str| async move { bind_sql(sql).await.unwrap_err() };
        assert_eq!(
            e("select ts, count() from events group by user_id").await,
            "column ts must be in GROUP BY or inside an aggregate function at 1:8"
        );
        assert_eq!(
            e("select ts, count() from events").await,
            "column ts must be in GROUP BY or inside an aggregate function at 1:8"
        );
        assert_eq!(
            e("select * from events group by ts").await,
            "SELECT * can't be used with GROUP BY; list the columns and aggregates at 1:22"
        );
        assert_eq!(
            e("select stddev(ts) from events").await,
            "unknown function stddev (supported: count, sum, avg, min, max, uniq, uniqExact, median, any, anyLast, groupArray (array_agg)) at 1:8"
        );
        assert_eq!(
            e("select sum(user_id) from events").await,
            "sum() needs a number; column user_id is String at 1:12"
        );
        assert_eq!(
            e("select avg() from events").await,
            "avg() needs a column at 1:8"
        );
        assert_eq!(
            e("select max(tags) from events").await,
            "max() of this type is not supported; column tags is Array(String) at 1:12"
        );
        assert_eq!(
            e("select count(), count() from events").await,
            "duplicate output column count(); name it with AS at 1:17"
        );
        assert_eq!(
            e("select count() from events order by ts").await,
            "column ts must be in GROUP BY or inside an aggregate function at 1:37"
        );
        assert_eq!(
            e("select user_id from events order by count()").await,
            "aggregate functions in ORDER BY need GROUP BY or aggregates in SELECT at 1:37"
        );
        assert_eq!(
            e("select user_id, count() from events group by user_id order by nope").await,
            "unknown column nope in table events (columns: ts, user_id, latency_ms, coupon, paid, tags) at 1:63"
        );
        assert_eq!(
            e("select ts as t, user_id as t from events").await,
            "duplicate output column t; name it with AS at 1:17"
        );
        assert_eq!(
            e("select user_id as latency_ms from events").await,
            "alias latency_ms is also a column of events; pick another name at 1:8"
        );
        assert_eq!(
            e("select ts as _offset from events").await,
            "alias _offset: names starting with _ are reserved at 1:8"
        );
        assert_eq!(
            e("select count() as __k0 from events").await,
            "alias __k0: names starting with _ are reserved at 1:8"
        );
        assert_eq!(
            e("select count() from events group by tags").await,
            "grouping by column tags of type Array(String) is not supported at 1:37"
        );
    }

    #[tokio::test]
    async fn offset_binds_for_rows_and_groups() {
        let q = bind_sql("select user_id from events order by latency_ms limit 5 offset 10")
            .await
            .unwrap();
        assert_eq!((q.limit, q.offset), (Some(5), Some(10)));
        let q = bind_sql("select user_id from events limit 10, 5")
            .await
            .unwrap();
        assert_eq!((q.limit, q.offset), (Some(5), Some(10)));
        // OFFSET 0 is no offset
        let q = bind_sql("select user_id from events offset 0")
            .await
            .unwrap();
        assert_eq!((q.limit, q.offset), (None, None));
        let q = bind_sql(
            "select user_id, count() as n from events group by user_id \
             order by n desc limit 3 offset 2",
        )
        .await
        .unwrap();
        let a = q.aggregation.unwrap();
        assert_eq!((a.limit, a.offset), (Some(3), Some(2)));
        assert_eq!(q.offset, None);
    }

    #[tokio::test]
    async fn aggregate_order_by_and_limit() {
        let agg =
            |sql: &'static str| async move { bind_sql(sql).await.unwrap().aggregation.unwrap() };
        let keys = |a: &Aggregation| -> Vec<(String, bool, bool)> {
            a.order
                .iter()
                .map(|k| (a.items[k.item].name.clone(), k.desc, k.nulls_first))
                .collect()
        };
        // an alias, a default name, a selected grouping column; LIMIT
        let a = agg(
            "select user_id, count() as n, max(latency_ms) from events group by user_id \
                     order by n desc, max(latency_ms), user_id limit 5",
        )
        .await;
        assert_eq!(
            keys(&a),
            [
                ("n".into(), true, false),
                ("max(latency_ms)".into(), false, false),
                ("user_id".into(), false, false)
            ]
        );
        assert_eq!(a.limit, Some(5));
        assert!(a.items.iter().all(|i| !i.hidden));
        // a call matching a selected item uses it; others, and an unselected grouping column,
        // are computed hidden
        let a = agg("select count() as n from events group by user_id \
                     order by count() desc, sum(latency_ms), user_id nulls first")
        .await;
        assert_eq!(
            keys(&a),
            [
                ("n".into(), true, false),
                ("__o0".into(), false, false),
                ("__o1".into(), false, true)
            ]
        );
        let hidden: Vec<(&str, &AggExpr)> = a
            .items
            .iter()
            .filter(|i| i.hidden)
            .map(|i| (i.name.as_str(), &i.expr))
            .collect();
        assert_eq!(
            hidden,
            [
                (
                    "__o0",
                    &AggExpr::Call {
                        func: AggFunc::Sum,
                        arg: Some(ColumnId(2))
                    }
                ),
                ("__o1", &AggExpr::Key(ColumnId(1)))
            ]
        );
        // ALL: the output columns, in SELECT order
        let a = agg("select user_id, count() from events group by user_id order by all desc").await;
        assert_eq!(
            keys(&a),
            [
                ("user_id".into(), true, false),
                ("count()".into(), true, false)
            ]
        );
        // LIMIT alone
        let a = agg("select user_id from events group by user_id limit 3").await;
        assert!(a.order.is_empty());
        assert_eq!(a.limit, Some(3));
    }

    #[tokio::test]
    async fn in_subquery_is_a_semi_join() {
        let b = bind_sql(
            "select ts from events where latency_ms > 1 \
             and user_id in (select id from plain where id != 'x') and coupon is null",
        )
        .await
        .unwrap();
        let semi = b.semi_join.as_ref().unwrap();
        let (outer_key, inner_key) = semi.keys.unwrap();
        assert_eq!(outer_key, ColumnId(1));
        assert_eq!(semi.inner.name, "plain");
        assert_eq!(semi.inner.column(inner_key).name, "id");
        assert!(semi.inner_filter.is_some());
        // the rest of the WHERE stays the filter, without the IN
        let mut cols = Vec::new();
        b.filter.as_ref().unwrap().columns(&mut cols);
        assert_eq!(cols, [ColumnId(2), ColumnId(3)]);
        // the same table on both sides
        let b = bind_sql(
            "select ts from events where user_id in (select user_id from events where paid)",
        )
        .await
        .unwrap();
        assert!(b.filter.is_none());
        assert_eq!(b.semi_join.unwrap().inner.name, "events");
    }

    #[tokio::test]
    async fn in_subquery_errors() {
        let e = |sql: &'static str| async move { bind_sql(sql).await.unwrap_err() };
        assert_eq!(
            e("select ts from events where user_id not in (select id from plain)").await,
            "NOT IN (subquery) is not supported yet at 1:41"
        );
        assert_eq!(
            e("select ts from events where paid or user_id in (select id from plain)").await,
            "a sub-query (IN or EXISTS) is only supported as a top-level AND condition, once at 1:45"
        );
        assert_eq!(
            e(
                "select ts from events where user_id in (select id from plain) \
               and ts in (select id from plain)"
            )
            .await,
            "a sub-query (IN or EXISTS) is only supported as a top-level AND condition, once at 1:70"
        );
        assert_eq!(
            e("select count() from events where user_id in (select id from plain)").await,
            "IN (subquery) with GROUP BY or aggregates is not supported yet at 1:42"
        );
        assert_eq!(
            e("select ts from events where user_id in (select id from plain limit 5)").await,
            "LIMIT in an IN sub-query is not supported yet at 1:68"
        );
        assert_eq!(
            e("select ts from events where user_id in (select * from plain)").await,
            "an IN sub-query must select exactly one column at 1:37"
        );
        assert_eq!(
            e("select ts from events where latency_ms in (select id from plain)").await,
            "latency_ms (UInt32) can't be compared with the sub-query's id (String) at 1:40"
        );
        assert!(
            e("select ts from events where user_id in (select nope from plain)")
                .await
                .starts_with("unknown column nope in table plain")
        );
    }

    #[tokio::test]
    async fn aliases_and_qualified_names() {
        let b = bind_sql("select e.ts, events.user_id from events e where e.paid order by e.ts")
            .await
            .unwrap();
        assert_eq!(names(&b), ["ts", "user_id"]);
        assert_eq!(
            bind_sql("select x.ts from events e").await.unwrap_err(),
            "unknown table or alias x at 1:8"
        );
        assert_eq!(
            bind_sql("select ts from events where e.paid")
                .await
                .unwrap_err(),
            "unknown table or alias e at 1:29"
        );
        assert_eq!(
            bind_sql("select 1 from events").await.unwrap_err(),
            "selecting constants is not supported yet at 1:8"
        );
        assert_eq!(
            bind_sql("select ts from events order by 'x'")
                .await
                .unwrap_err(),
            "ordering by expressions is not supported yet; name a column at 1:32"
        );
    }

    #[tokio::test]
    async fn exists_is_the_same_semi_join_as_in() {
        let key = |b: &BoundQuery| {
            let s = b.semi_join.as_ref().unwrap();
            (
                s.keys,
                s.inner.name.clone(),
                s.inner_filter.clone(),
                b.filter.clone(),
            )
        };
        let want = key(
            &bind_sql("select ts from events where paid and user_id in (select id from plain where id != 'x')")
                .await
                .unwrap(),
        );
        for sql in [
            "select ts from events e where e.paid and exists (select 1 from plain p where p.id = e.user_id and p.id != 'x')",
            "select ts from events e where paid and exists (select * from plain where e.user_id = id and id != 'x')",
            "select ts from events where exists (select id from plain p where p.id != 'x' and user_id = p.id) and paid",
        ] {
            assert_eq!(key(&bind_sql(sql).await.unwrap()), want, "{sql}");
        }
        // the same table on both sides, told apart by aliases
        let b = bind_sql(
            "select ts from events a where exists (select 1 from events b where b.user_id = a.user_id and b.paid)",
        )
        .await
        .unwrap();
        assert_eq!(b.semi_join.unwrap().keys, Some((ColumnId(1), ColumnId(1))));
        // uncorrelated: no keys
        let b = bind_sql("select ts from events where exists (select 1 from plain where id = 'x')")
            .await
            .unwrap();
        let semi = b.semi_join.unwrap();
        assert_eq!(semi.keys, None);
        assert!(semi.inner_filter.is_some());
    }

    #[tokio::test]
    async fn exists_errors() {
        let e = |sql: &'static str| async move { bind_sql(sql).await.unwrap_err() };
        assert_eq!(
            e("select ts from events where not exists (select 1 from plain)").await,
            "NOT EXISTS is not supported yet at 1:33"
        );
        assert_eq!(
            e(
                "select ts from events a where exists (select 1 from events b \
               where b.user_id = a.user_id and b.ts = a.ts)"
            )
            .await,
            "EXISTS with more than one correlation is not supported yet at 1:99"
        );
        assert_eq!(
            e("select ts from events e where exists (select 1 from plain p where p.id = e.user_id or e.paid)")
                .await,
            "only `inner = outer` equalities can refer to the outer query at 1:74"
        );
        assert_eq!(
            e("select ts from events e where user_id in (select id from plain where id = e.user_id)")
                .await,
            "this refers to the outer query; only EXISTS sub-queries can, as `inner = outer` at 1:75"
        );
        assert_eq!(
            e("select count() from events where exists (select 1 from plain)").await,
            "IN (subquery) with GROUP BY or aggregates is not supported yet at 1:34"
        );
        assert_eq!(
            e("select ts from events where exists (select 1 from plain limit 1)").await,
            "LIMIT in an EXISTS sub-query is not supported yet at 1:63"
        );
    }
}
