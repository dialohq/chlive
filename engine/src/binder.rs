//! Resolves a parsed query against the catalog, clause by clause in SQL's logical evaluation
//! order: FROM, then WHERE, then SELECT, then ORDER BY, then LIMIT. Everything after binding works with resolved identifiers, never
//! with the names as written.

use std::fmt;

use crate::ast::{
    CallArg, CmpOp, ColumnRef, Direction, Expr, Literal, NullsOrder, Order, Projection, Query,
    SelectExpr,
};
use crate::error::{render_at, Span};
use crate::history::{HistoryError, HistorySource};
use crate::rules::{NamingRules, TopicId};
use crate::source::{EventSource, SourceError};
use crate::types::{Kind, LiteralError, Type, Value};

/// What the binder can look things up in.
pub struct Catalog<'a> {
    pub rules: &'a dyn NamingRules,
    pub history: &'a dyn HistorySource,
    pub source: &'a dyn EventSource,
}

/// Index of a column in [`BoundTable::columns`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ColumnId(pub usize);

/// A column of a dialect table, with where it lives on each side.
#[derive(Debug, Clone, PartialEq)]
pub struct BoundColumn {
    pub id: ColumnId,
    /// Name in the dialect and in output rows.
    pub name: String,
    /// Column in ClickHouse.
    pub clickhouse: String,
    /// Key in the topic's JSON objects.
    pub json_key: String,
    /// From the ClickHouse column type; decides how event JSON is decoded.
    pub ty: Type,
}

/// The FROM table: its ClickHouse table, its topic, and its (visible) columns.
#[derive(Debug, Clone, PartialEq)]
pub struct BoundTable {
    pub name: String,
    /// `FROM events e`: what qualified names may call it, besides `name`.
    pub alias: Option<String>,
    /// The ClickHouse table, quoted for SQL (`db.table`).
    pub clickhouse_table: String,
    /// The same, unquoted.
    pub database: String,
    pub clickhouse_name: String,
    pub topic: TopicId,
    pub columns: Vec<BoundColumn>,
}

impl BoundTable {
    /// Whether `q` (in `q.column`) names this table: its name or its alias.
    pub fn names(&self, q: &str) -> bool {
        q == self.name || self.alias.as_deref() == Some(q)
    }

    /// What to snapshot for this table's history.
    pub fn snapshot_table(&self) -> crate::snapshot::SnapshotTable {
        crate::snapshot::SnapshotTable::new(&self.database, &self.clickhouse_name, &self.topic.0)
    }

    pub fn column(&self, id: ColumnId) -> &BoundColumn {
        &self.columns[id.0]
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct BoundQuery {
    pub table: BoundTable,
    /// The WHERE condition, typed.
    pub filter: Option<BoundExpr>,
    /// Output columns, in output order.
    pub projection: Vec<Projected>,
    /// ORDER BY keys, most significant first; empty when unordered.
    pub order: Vec<OrderKey>,
    /// LIMIT: at most this many rows.
    pub limit: Option<u64>,
    /// OFFSET: the rows the result starts after (never 0).
    pub offset: Option<u64>,
    /// With GROUP BY or aggregate functions: the groups and what to compute per group. Then
    /// `projection` is empty and the output is `aggregation.items`.
    pub aggregation: Option<Aggregation>,
    /// `key IN (SELECT …)`, a top-level AND condition of the WHERE (not part of `filter`).
    pub semi_join: Option<SemiJoin>,
}

/// `outer_key IN (SELECT inner_key FROM inner [WHERE inner_filter])`: the rows whose key is
/// one of the sub-query's; also a correlated `EXISTS (… WHERE inner_key = outer_key …)`. Without
/// keys, an uncorrelated `EXISTS`: every row, once the sub-query has one.
#[derive(Debug, Clone, PartialEq)]
pub struct SemiJoin {
    /// (outer key, inner key); `None` for an uncorrelated EXISTS.
    pub keys: Option<(ColumnId, ColumnId)>,
    pub inner: BoundTable,
    pub inner_filter: Option<BoundExpr>,
}

/// A selected column and its output name (its alias, or its own name).
#[derive(Debug, Clone, PartialEq)]
pub struct Projected {
    pub column: ColumnId,
    pub name: String,
}

/// GROUP BY and aggregate functions.
#[derive(Debug, Clone, PartialEq)]
pub struct Aggregation {
    /// The grouping columns (none: one global group).
    pub keys: Vec<ColumnId>,
    /// The output columns, in SELECT order, then any hidden ones ORDER BY needs.
    pub items: Vec<AggItem>,
    /// ORDER BY, most significant first; empty when unordered.
    pub order: Vec<AggOrderKey>,
    /// LIMIT: the first groups by `order` (without one: the first groups seen).
    pub limit: Option<u64>,
    /// OFFSET: the groups the result starts after (never 0).
    pub offset: Option<u64>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AggItem {
    /// Output name: the alias, or ClickHouse's default (`count()`, `avg(latency_ms)`); hidden
    /// items are `__o0`, `__o1`, ….
    pub name: String,
    /// Its type, as ClickHouse gives it.
    pub ty: Type,
    pub expr: AggExpr,
    /// Computed for ORDER BY only, not output.
    pub hidden: bool,
}

/// One ORDER BY key of an aggregate query: an item, by index into [`Aggregation::items`].
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct AggOrderKey {
    pub item: usize,
    pub desc: bool,
    pub nulls_first: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub enum AggExpr {
    /// A grouping column.
    Key(ColumnId),
    /// `func(arg)`; `count()` has no argument.
    Call {
        func: AggFunc,
        arg: Option<ColumnId>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggFunc {
    Count,
    Sum,
    Avg,
    Min,
    Max,
    Uniq,
    UniqExact,
    Median,
    Any,
    AnyLast,
}

impl AggFunc {
    /// The ClickHouse name (also the start of its `-State`/`-Merge` names).
    pub fn name(self) -> &'static str {
        match self {
            AggFunc::Count => "count",
            AggFunc::Sum => "sum",
            AggFunc::Avg => "avg",
            AggFunc::Min => "min",
            AggFunc::Max => "max",
            AggFunc::Uniq => "uniq",
            AggFunc::UniqExact => "uniqExact",
            AggFunc::Median => "median",
            AggFunc::Any => "any",
            AggFunc::AnyLast => "anyLast",
        }
    }
}

/// One ORDER BY key, with ClickHouse's defaults resolved: ascending, NULLS LAST.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OrderKey {
    pub column: ColumnId,
    pub desc: bool,
    pub nulls_first: bool,
}

/// A typed expression. Literals are already coerced to the type they are compared with.
#[derive(Debug, Clone, PartialEq)]
pub enum BoundExpr {
    Column(ColumnId),
    Literal(Value),
    Compare {
        op: CmpOp,
        left: Box<BoundExpr>,
        right: Box<BoundExpr>,
    },
    And(Box<BoundExpr>, Box<BoundExpr>),
    Or(Box<BoundExpr>, Box<BoundExpr>),
    Not(Box<BoundExpr>),
    IsNull {
        expr: Box<BoundExpr>,
        negated: bool,
    },
    InList {
        expr: Box<BoundExpr>,
        list: Vec<Value>,
        negated: bool,
    },
}

impl BoundExpr {
    /// Columns the expression reads, in first-use order.
    pub fn columns(&self, out: &mut Vec<ColumnId>) {
        match self {
            BoundExpr::Column(id) => {
                if !out.contains(id) {
                    out.push(*id);
                }
            }
            BoundExpr::Literal(_) => {}
            BoundExpr::Compare { left, right, .. }
            | BoundExpr::And(left, right)
            | BoundExpr::Or(left, right) => {
                left.columns(out);
                right.columns(out);
            }
            BoundExpr::Not(e)
            | BoundExpr::IsNull { expr: e, .. }
            | BoundExpr::InList { expr: e, .. } => e.columns(out),
        }
    }
}

impl BoundQuery {
    /// The output columns, named as in the output.
    pub fn output(&self) -> impl Iterator<Item = BoundColumn> + '_ {
        self.projection.iter().map(|p| BoundColumn {
            name: p.name.clone(),
            ..self.table.column(p.column).clone()
        })
    }
}

#[derive(Debug)]
pub enum BindError {
    /// A problem with the query itself, at a position in it.
    At { message: String, span: Span },
    /// The catalog could not be consulted (ClickHouse unreachable, …).
    History(HistoryError),
    /// The event source could not be consulted (Kafka unreachable, …).
    Source(SourceError),
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
            BindError::At { message, span } => render_at(src, *span, message),
            BindError::History(e) => e.to_string(),
            BindError::Source(e) => e.to_string(),
        }
    }
}

impl fmt::Display for BindError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BindError::At { message, span } => write!(f, "{message} at byte {}", span.start),
            BindError::History(e) => write!(f, "{e}"),
            BindError::Source(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for BindError {}

pub async fn bind(query: &Query, catalog: &Catalog<'_>) -> Result<BoundQuery, BindError> {
    let table = bind_from(query, catalog).await?;
    let (selection, sub) = match &query.selection {
        Some(expr) => split_subquery(expr)?,
        None => (None, None),
    };
    let filter = match &selection {
        Some(expr) => Some(bind_where(expr, &table)?),
        None => None,
    };
    let semi_join = match sub {
        Some(Sub::In(key, sub, span)) => {
            Some(bind_semi_join(key, sub, span, &table, catalog).await?)
        }
        Some(Sub::Exists(sub, _)) => Some(bind_exists(sub, &table, catalog).await?),
        None => None,
    };
    if let Some(mut aggregation) = bind_aggregation(query, &table)? {
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
    Ok(BoundQuery {
        table,
        filter,
        projection,
        order,
        limit: query.limit.map(|l| l.value),
        offset: offset_of(query),
        aggregation: None,
        semi_join,
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
    let inner = bind_from(sub, catalog).await?;
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
    let has = |t: &BoundTable| t.columns.iter().any(|col| col.name == c.name);
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
        Expr::Exists { .. } => None,
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
    let inner = bind_from(sub, catalog).await?;
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
    let default_name = match &arg {
        None => format!("{}()", func.name()),
        Some((id, _)) => format!("{}({})", func.name(), table.column(*id).name),
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
async fn bind_from(query: &Query, catalog: &Catalog<'_>) -> Result<BoundTable, BindError> {
    let from = &query.from;
    if from.name.starts_with(crate::snapshot::CLONE_PREFIX) {
        return Err(BindError::at(
            format!("unknown table {} (snapshots aren't tables)", from.name),
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
    let columns = described
        .into_iter()
        .filter(|c| !catalog.rules.is_hidden(&c.name))
        .enumerate()
        .map(|(i, c)| {
            let m = catalog.rules.column(&c.name);
            BoundColumn {
                id: ColumnId(i),
                name: m.name,
                clickhouse: c.name,
                json_key: m.json_key,
                ty: Type::parse(&c.ty),
            }
        })
        .collect();
    Ok(BoundTable {
        name: from.name.clone(),
        alias: from.alias.clone(),
        clickhouse_table,
        database,
        clickhouse_name,
        topic,
        columns,
    })
}

/// WHERE: a boolean expression over the table's columns. Comparisons need compatible types;
/// literals are coerced to the other side's type here, once, not per event.
fn bind_where(expr: &Expr, table: &BoundTable) -> Result<BoundExpr, BindError> {
    match expr {
        Expr::InSubquery { span, .. } | Expr::Exists { span, .. } => {
            Err(BindError::at(SUBQUERY_PLACE, *span))
        }
        Expr::And(l, r) => Ok(BoundExpr::And(
            Box::new(bind_where(l, table)?),
            Box::new(bind_where(r, table)?),
        )),
        Expr::Or(l, r) => Ok(BoundExpr::Or(
            Box::new(bind_where(l, table)?),
            Box::new(bind_where(r, table)?),
        )),
        Expr::Not { expr, .. } => Ok(BoundExpr::Not(Box::new(bind_where(expr, table)?))),
        Expr::Compare {
            op,
            left,
            right,
            span,
        } => {
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
    }))
}

/// A literal as a value of `ty`, or an error pointing at the literal.
fn coerce(lit: &Literal, span: Span, ty: &Type) -> Result<Value, BindError> {
    let mismatch = || {
        let what = match lit {
            Literal::Number(_) => "a number",
            Literal::String(_) => "a string",
            Literal::Bool(_) => "a boolean",
            Literal::Null => "NULL",
        };
        BindError::at(format!("cannot compare {ty} with {what}"), span)
    };
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
    use crate::diff::Row;
    use crate::history::DescribedColumn;
    use crate::parser::parse;
    use crate::rules::HardcodedRules;
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
                _ => Err(HistoryError::UnknownTable(table.into())),
            }
        }
        async fn fetch(&self, _sql: &str) -> Result<Vec<Row>, HistoryError> {
            unreachable!("the binder never fetches")
        }
    }

    /// Only `events`, `plain` and `sessions` have a stream.
    fn topics() -> MemorySource {
        let mut source = MemorySource::new();
        source
            .partition("events", 0)
            .partition("plain", 0)
            .partition("sessions", 0);
        source
    }

    async fn bind_sql(sql: &str) -> Result<BoundQuery, String> {
        let rules = HardcodedRules::default();
        let catalog = Catalog {
            rules: &rules,
            history: &FakeHistory,
            source: &topics(),
        };
        let q = parse(sql).map_err(|e| e.render(sql))?;
        bind(&q, &catalog).await.map_err(|e| e.render(sql))
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
            "unknown function stddev (supported: count, sum, avg, min, max, uniq, uniqExact, median, any, anyLast) at 1:8"
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
