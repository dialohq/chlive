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
use crate::rules::{KafkaColumns, NamingRules, TopicId};
use crate::source::{EventSource, SourceError};
use crate::types::{parse_date, parse_datetime, Kind, Type, Value};

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
    pub clickhouse_table: String,
    pub topic: TopicId,
    /// ClickHouse columns with each row's Kafka partition and offset.
    pub kafka: KafkaColumns,
    pub columns: Vec<BoundColumn>,
}

impl BoundTable {
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
    /// With GROUP BY or aggregate functions: the groups and what to compute per group. Then
    /// `projection` is empty and the output is `aggregation.items`.
    pub aggregation: Option<Aggregation>,
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
    /// The output columns, in SELECT order.
    pub items: Vec<AggItem>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct AggItem {
    /// Output name: the alias, or ClickHouse's default (`count()`, `avg(latency_ms)`).
    pub name: String,
    /// Its type, as ClickHouse gives it.
    pub ty: Type,
    pub expr: AggExpr,
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
    let filter = match &query.selection {
        Some(expr) => Some(bind_where(expr, &table)?),
        None => None,
    };
    if let Some(aggregation) = bind_aggregation(query, &table)? {
        if let Some(order) = &query.order {
            let span = match order {
                Order::All { span, .. } => *span,
                Order::Items(items) => items[0].column.span,
            };
            return Err(BindError::at(
                "ORDER BY with GROUP BY or aggregates is not supported yet",
                span,
            ));
        }
        if let Some(limit) = &query.limit {
            return Err(BindError::at(
                "LIMIT with GROUP BY or aggregates is not supported yet",
                limit.span,
            ));
        }
        return Ok(BoundQuery {
            table,
            filter,
            projection: Vec::new(),
            order: Vec::new(),
            limit: None,
            aggregation: Some(aggregation),
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
        aggregation: None,
    })
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
        let id = resolve_column(table, &k.name, k.span)?;
        if table.column(id).ty.kind == Kind::Other {
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
            SelectExpr::Column(c) => {
                let id = resolve_column(table, &c.name, c.span)?;
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
                let Some(func) = crate::aggregate::lookup(&name) else {
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
                    (_, CallArg::Column(c)) => Some((resolve_column(table, &c.name, c.span)?, c)),
                    (f, _) => {
                        return Err(BindError::at(
                            format!("{}() needs a column", f.name()),
                            span,
                        ))
                    }
                };
                let ty = result_type(func, arg.as_ref().map(|(id, c)| (table.column(*id), c)))?;
                let default_name = match &arg {
                    None => format!("{}()", func.name()),
                    Some((id, _)) => format!("{}({})", func.name(), table.column(*id).name),
                };
                (
                    default_name,
                    ty,
                    AggExpr::Call {
                        func,
                        arg: arg.map(|(id, _)| id),
                    },
                    span,
                )
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
        out.push(AggItem { name, ty, expr });
    }
    Ok(Some(Aggregation { keys, items: out }))
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
        if col.ty.kind == Kind::Other {
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
            .map(|item| {
                let id = resolve_column(table, &item.column.name, item.column.span)?;
                key(id, item.direction, item.nulls, item.column.span)
            })
            .collect(),
    }
}

fn resolve_column(table: &BoundTable, name: &str, span: Span) -> Result<ColumnId, BindError> {
    match table.columns.iter().find(|c| c.name == name) {
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

/// FROM: the table must exist in ClickHouse, have an event stream (a Kafka topic), and record
/// each row's Kafka partition and offset (so history and stream can be lined up).
async fn bind_from(query: &Query, catalog: &Catalog<'_>) -> Result<BoundTable, BindError> {
    let from = &query.from;
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
    let kafka = catalog.rules.kafka_columns();
    for column in [&kafka.partition, &kafka.offset] {
        if !described.iter().any(|c| &c.name == column) {
            return Err(BindError::at(
                format!(
                    "table {} has no {column} column in ClickHouse ({clickhouse_table}); chlive \
                     needs each row's Kafka partition and offset to line history up with the topic",
                    from.name
                ),
                from.span,
            ));
        }
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
        clickhouse_table,
        topic,
        kafka,
        columns,
    })
}

/// WHERE: a boolean expression over the table's columns. Comparisons need compatible types;
/// literals are coerced to the other side's type here, once, not per event.
fn bind_where(expr: &Expr, table: &BoundTable) -> Result<BoundExpr, BindError> {
    match expr {
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
            if ty.kind == Kind::Bool && !matches!(op, CmpOp::Eq | CmpOp::NotEq) {
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
            if ty.kind == Kind::Bool {
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
            let col = column(table, &c.name, c.span)?;
            if col.ty.kind != Kind::Bool {
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
            let col = column(table, &c.name, c.span)?;
            if col.ty.kind == Kind::Other {
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

fn column<'a>(table: &'a BoundTable, name: &str, span: Span) -> Result<&'a BoundColumn, BindError> {
    table
        .columns
        .iter()
        .find(|c| c.name == name)
        .ok_or_else(|| {
            let names: Vec<&str> = table.columns.iter().map(|c| c.name.as_str()).collect();
            BindError::at(
                format!(
                    "unknown column {name} in table {} (columns: {})",
                    table.name,
                    names.join(", ")
                ),
                span,
            )
        })
}

/// What kind of values can be compared with each other.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Class {
    Number,
    String,
    DateTime,
    Date,
    Bool,
}

fn class(ty: &Type) -> Class {
    match ty.kind {
        Kind::Int { .. } | Kind::Float => Class::Number,
        Kind::String => Class::String,
        Kind::DateTime { .. } => Class::DateTime,
        Kind::Date => Class::Date,
        Kind::Bool => Class::Bool,
        Kind::Other => unreachable!("rejected in operand()"),
    }
}

fn check_comparable(a: &Type, b: &Type, span: Span) -> Result<(), BindError> {
    if class(a) == class(b) {
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
    match (&ty.kind, lit) {
        (_, Literal::Null) => Ok(Value::Null),
        // A value outside the column's range is fine: it simply never (or always) matches, as
        // in ClickHouse, which compares integers of different types exactly.
        (Kind::Int { .. }, Literal::Number(n)) => {
            if n.contains('.') {
                return n.parse::<f64>().map(Value::Float).map_err(|_| mismatch());
            }
            n.parse::<i128>()
                .map(Value::Int)
                .map_err(|_| BindError::at(format!("number {n} is too large"), span))
        }
        (Kind::Float, Literal::Number(n)) => {
            n.parse::<f64>().map(Value::Float).map_err(|_| mismatch())
        }
        (Kind::String, Literal::String(s)) => Ok(Value::String(s.clone())),
        (Kind::DateTime { precision }, Literal::String(s)) => parse_datetime(s, *precision)
            .map(|ticks| Value::DateTime {
                ticks,
                precision: *precision,
            })
            .ok_or_else(|| {
                BindError::at(
                    format!("invalid {ty} value '{s}' (expected YYYY-MM-DD[ hh:mm:ss[.fff]])"),
                    span,
                )
            }),
        (Kind::Date, Literal::String(s)) => parse_date(s).map(Value::Date).ok_or_else(|| {
            BindError::at(
                format!("invalid Date value '{s}' (expected YYYY-MM-DD)"),
                span,
            )
        }),
        (Kind::Bool, Literal::Bool(b)) => Ok(Value::Bool(*b)),
        _ => Err(mismatch()),
    }
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
                SelectExpr::Column(c) => (c, i.alias.as_ref()),
                SelectExpr::Call { .. } => unreachable!("lists with calls are aggregations"),
            })
            .collect(),
    };
    let mut out: Vec<Projected> = Vec::with_capacity(items.len());
    for (r, alias) in items {
        let column = resolve_column(table, &r.name, r.span)?;
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
                _ => Err(HistoryError::UnknownTable(table.into())),
            }
        }
        async fn fetch(&self, _sql: &str) -> Result<Vec<Row>, HistoryError> {
            unreachable!("the binder never fetches")
        }
    }

    /// Only `events` and `plain` have a stream.
    fn topics() -> MemorySource {
        let mut source = MemorySource::new();
        source.partition("events", 0).partition("plain", 0);
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
    async fn errors_point_at_the_offending_name() {
        assert_eq!(
            bind_sql("select * from nope").await.unwrap_err(),
            "unknown table nope in ClickHouse (default.nope) at 1:15"
        );
        assert_eq!(
            bind_sql("select * from orders").await.unwrap_err(),
            "no event stream for table orders (topic orders) at 1:15"
        );
        assert_eq!(
            bind_sql("select * from plain").await.unwrap_err(),
            "table plain has no _offset column in ClickHouse (default.plain); chlive needs each \
             row's Kafka partition and offset to line history up with the topic at 1:15"
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
                right: Box::new(BoundExpr::Literal(Value::DateTime {
                    ticks: parse_datetime("2026-10-01 00:00:00", 3).unwrap(),
                    precision: 3
                })),
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
            "ORDER BY with GROUP BY or aggregates is not supported yet at 1:37"
        );
        assert_eq!(
            e("select count() from events limit 5").await,
            "LIMIT with GROUP BY or aggregates is not supported yet at 1:34"
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
}
