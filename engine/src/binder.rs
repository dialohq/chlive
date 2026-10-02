//! Resolves a parsed query against the catalog, clause by clause in SQL's logical evaluation
//! order: FROM, then WHERE, then SELECT, then ORDER BY, then LIMIT. Everything after binding works with resolved identifiers, never
//! with the names as written.

use std::fmt;

use crate::ast::{CmpOp, Direction, Expr, Literal, NullsOrder, Order, Projection, Query};
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
    pub projection: Vec<ColumnId>,
    /// ORDER BY keys, most significant first; empty when unordered.
    pub order: Vec<OrderKey>,
    /// LIMIT: at most this many rows.
    pub limit: Option<u64>,
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
    pub fn output(&self) -> impl Iterator<Item = &BoundColumn> {
        self.projection.iter().map(|id| self.table.column(*id))
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
    let projection = bind_select(query, &table)?;
    let order = match &query.order {
        Some(order) => bind_order(order, &table, &projection)?,
        None => Vec::new(),
    };
    Ok(BoundQuery {
        table,
        filter,
        projection,
        order,
        limit: query.limit.map(|l| l.value),
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
fn bind_select(query: &Query, table: &BoundTable) -> Result<Vec<ColumnId>, BindError> {
    match &query.projection {
        Projection::Wildcard => Ok(table.columns.iter().map(|c| c.id).collect()),
        Projection::Columns(refs) => {
            let mut ids: Vec<ColumnId> = Vec::with_capacity(refs.len());
            for r in refs {
                let id = resolve_column(table, &r.name, r.span)?;
                if ids.contains(&id) {
                    return Err(BindError::at(
                        format!("column {} is selected twice", r.name),
                        r.span,
                    ));
                }
                ids.push(id);
            }
            Ok(ids)
        }
    }
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

    fn names(b: &BoundQuery) -> Vec<&str> {
        b.output().map(|c| c.name.as_str()).collect()
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
        assert_eq!(b.projection, [ColumnId(2), ColumnId(0)]);
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
            "column ts is selected twice at 1:21"
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
}
