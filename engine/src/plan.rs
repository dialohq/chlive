//! From a bound query to what to run: the ClickHouse query for history, and for the stream:
//! which topics to consume, which keys of their events to decode (and as which types), which
//! events are relevant, and how to turn them into result rows.

use std::cmp::Ordering;

use crate::ast::CmpOp;
use crate::binder::{BoundColumn, BoundExpr, BoundQuery, BoundTable, ColumnId};
use crate::diff::Row;
use crate::rules::{quote_ident, KafkaColumns, TopicId};
use crate::types::{decode, DecodeError, Type, Value};
use crate::watermark::Watermark;

#[derive(Debug, Clone, PartialEq)]
pub struct Plan {
    pub table: String,
    /// Output columns, in output order.
    pub columns: Vec<BoundColumn>,
    pub history: HistoryPlan,
    pub stream: StreamPlan,
}

/// The history query. Its upper bound, the [`Watermark`], is only known when subscribing.
#[derive(Debug, Clone, PartialEq)]
pub struct HistoryPlan {
    /// The fully qualified ClickHouse table.
    pub table: String,
    /// `SELECT … FROM …`
    pub select: String,
    /// The WHERE condition, rendered for ClickHouse.
    pub filter: Option<String>,
    pub kafka: KafkaColumns,
}

impl HistoryPlan {
    /// The query, bounded by `watermark` (rows up to it) or, without one, unbounded.
    pub fn sql(&self, watermark: Option<&Watermark>) -> String {
        let bound =
            watermark.map(|w| w.history_predicate(&self.kafka.partition, &self.kafka.offset));
        match (&self.filter, bound) {
            (None, None) => self.select.clone(),
            (Some(f), None) => format!("{} WHERE {f}", self.select),
            (None, Some(b)) => format!("{} WHERE {b}", self.select),
            (Some(f), Some(b)) => format!("{} WHERE ({f}) AND ({b})", self.select),
        }
    }
}

/// A column the stream needs: decoded from `json_key` as `ty`.
#[derive(Debug, Clone, PartialEq)]
pub struct StreamColumn {
    pub id: ColumnId,
    pub json_key: String,
    pub ty: Type,
}

#[derive(Debug, Clone, PartialEq)]
pub struct StreamPlan {
    /// The only topics the query needs; nothing else is consumed.
    pub topics: Vec<TopicId>,
    /// Columns to decode from each event (output and filter), with their types.
    pub columns: Vec<StreamColumn>,
    /// Number of columns of the table; decoded values are indexed by [`ColumnId`].
    pub width: usize,
    /// Output column name and the column it comes from, in output order.
    pub output: Vec<(String, ColumnId)>,
    /// Which events are relevant to the query.
    pub filter: Filter,
}

/// Decides whether an event is relevant to the query.
#[derive(Debug, Clone, PartialEq)]
pub enum Filter {
    /// Every event of the subscribed topics.
    All,
    /// Events for which the WHERE condition is true (not false, not unknown).
    Where {
        expr: BoundExpr,
        /// The condition in dialect syntax, for `explain`.
        text: String,
    },
}

/// An event's needed columns, decoded; indexed by [`ColumnId`], others are NULL.
pub type DecodedRow = Vec<Value>;

#[derive(Debug, Clone, PartialEq, thiserror::Error)]
#[error("column {column}: {error}")]
pub struct RowDecodeError {
    pub column: String,
    pub error: DecodeError,
}

impl StreamPlan {
    /// The JSON keys the stream must decode; every other key of an event is skipped.
    pub fn required_keys(&self) -> Vec<String> {
        self.columns.iter().map(|c| c.json_key.clone()).collect()
    }

    /// Decode an event's needed fields by their column types.
    pub fn decode(&self, event: &Row) -> Result<DecodedRow, RowDecodeError> {
        let mut row = vec![Value::Null; self.width];
        for c in &self.columns {
            row[c.id.0] =
                decode(event.get(&c.json_key), &c.ty).map_err(|error| RowDecodeError {
                    column: c.json_key.clone(),
                    error,
                })?;
        }
        Ok(row)
    }

    pub fn matches(&self, row: &DecodedRow) -> bool {
        match &self.filter {
            Filter::All => true,
            Filter::Where { expr, .. } => eval(expr, row) == Some(true),
        }
    }

    /// The result row, rendered the way ClickHouse renders it.
    pub fn project(&self, row: &DecodedRow) -> Row {
        self.output
            .iter()
            .map(|(name, id)| (name.clone(), row[id.0].to_json()))
            .collect()
    }
}

/// SQL three-valued logic: `None` is unknown (NULL).
fn eval(expr: &BoundExpr, row: &DecodedRow) -> Option<bool> {
    match expr {
        BoundExpr::Column(_) | BoundExpr::Literal(_) => match value(expr, row) {
            Value::Bool(b) => Some(*b),
            _ => None,
        },
        BoundExpr::Compare { op, left, right } => {
            let ord = value(left, row).compare(value(right, row))?;
            Some(match op {
                CmpOp::Eq => ord == Ordering::Equal,
                CmpOp::NotEq => ord != Ordering::Equal,
                CmpOp::Lt => ord == Ordering::Less,
                CmpOp::LtEq => ord != Ordering::Greater,
                CmpOp::Gt => ord == Ordering::Greater,
                CmpOp::GtEq => ord != Ordering::Less,
            })
        }
        BoundExpr::And(l, r) => match (eval(l, row), eval(r, row)) {
            (Some(false), _) | (_, Some(false)) => Some(false),
            (Some(true), Some(true)) => Some(true),
            _ => None,
        },
        BoundExpr::Or(l, r) => match (eval(l, row), eval(r, row)) {
            (Some(true), _) | (_, Some(true)) => Some(true),
            (Some(false), Some(false)) => Some(false),
            _ => None,
        },
        BoundExpr::Not(e) => eval(e, row).map(|b| !b),
        BoundExpr::IsNull { expr, negated } => Some((*value(expr, row) == Value::Null) != *negated),
        BoundExpr::InList {
            expr,
            list,
            negated,
        } => {
            let v = value(expr, row);
            if *v == Value::Null {
                return None;
            }
            let found = list
                .iter()
                .any(|item| v.compare(item) == Some(Ordering::Equal));
            Some(found != *negated)
        }
    }
}

fn value<'a>(expr: &'a BoundExpr, row: &'a DecodedRow) -> &'a Value {
    match expr {
        BoundExpr::Column(id) => &row[id.0],
        BoundExpr::Literal(v) => v,
        _ => unreachable!("the binder only allows columns and literals as operands"),
    }
}

/// The condition as ClickHouse SQL, from the typed tree (never from the query text).
fn to_clickhouse(expr: &BoundExpr, table: &BoundTable) -> String {
    match expr {
        BoundExpr::Column(id) => quote_ident(&table.column(*id).clickhouse),
        BoundExpr::Literal(v) => v.to_clickhouse_sql(),
        BoundExpr::Compare { op, left, right } => {
            format!(
                "{} {} {}",
                to_clickhouse(left, table),
                op.symbol(),
                to_clickhouse(right, table)
            )
        }
        BoundExpr::And(l, r) => format!(
            "({}) AND ({})",
            to_clickhouse(l, table),
            to_clickhouse(r, table)
        ),
        BoundExpr::Or(l, r) => format!(
            "({}) OR ({})",
            to_clickhouse(l, table),
            to_clickhouse(r, table)
        ),
        BoundExpr::Not(e) => format!("NOT ({})", to_clickhouse(e, table)),
        BoundExpr::IsNull { expr, negated } => {
            format!(
                "{} IS {}NULL",
                to_clickhouse(expr, table),
                if *negated { "NOT " } else { "" }
            )
        }
        BoundExpr::InList {
            expr,
            list,
            negated,
        } => format!(
            "{} {}IN ({})",
            to_clickhouse(expr, table),
            if *negated { "NOT " } else { "" },
            list.iter()
                .map(Value::to_clickhouse_sql)
                .collect::<Vec<_>>()
                .join(", ")
        ),
    }
}

/// The condition in dialect syntax, for `explain`.
fn to_dialect(expr: &BoundExpr, table: &BoundTable) -> String {
    let lit = |v: &Value| match v {
        Value::Null => "NULL".to_string(),
        other => other.to_json().to_string().replace('"', "'"),
    };
    match expr {
        BoundExpr::Column(id) => table.column(*id).name.clone(),
        BoundExpr::Literal(v) => lit(v),
        BoundExpr::Compare { op, left, right } => {
            format!(
                "{} {} {}",
                to_dialect(left, table),
                op.symbol(),
                to_dialect(right, table)
            )
        }
        BoundExpr::And(l, r) => format!("({} AND {})", to_dialect(l, table), to_dialect(r, table)),
        BoundExpr::Or(l, r) => format!("({} OR {})", to_dialect(l, table), to_dialect(r, table)),
        BoundExpr::Not(e) => format!("NOT {}", to_dialect(e, table)),
        BoundExpr::IsNull { expr, negated } => {
            format!(
                "{} IS {}NULL",
                to_dialect(expr, table),
                if *negated { "NOT " } else { "" }
            )
        }
        BoundExpr::InList {
            expr,
            list,
            negated,
        } => format!(
            "{} {}IN ({})",
            to_dialect(expr, table),
            if *negated { "NOT " } else { "" },
            list.iter().map(lit).collect::<Vec<_>>().join(", ")
        ),
    }
}

/// What `--explain` shows.
impl std::fmt::Display for Plan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let columns: Vec<&str> = self.columns.iter().map(|c| c.name.as_str()).collect();
        let topics: Vec<&str> = self.stream.topics.iter().map(|t| t.0.as_str()).collect();
        let keys: Vec<String> = self
            .stream
            .columns
            .iter()
            .map(|c| format!("{} {}", c.json_key, c.ty))
            .collect();
        writeln!(f, "table:   {}", self.table)?;
        writeln!(f, "columns: {}", columns.join(", "))?;
        writeln!(f, "history: {}", self.history.sql(None))?;
        writeln!(
            f,
            "         up to max({}) per {}, read when subscribing",
            self.history.kafka.offset, self.history.kafka.partition
        )?;
        writeln!(
            f,
            "topics:  {} (each partition from right after that)",
            topics.join(", ")
        )?;
        writeln!(f, "keys:    {}", keys.join(", "))?;
        match &self.stream.filter {
            Filter::All => write!(f, "filter:  all events"),
            Filter::Where { text, .. } => write!(f, "filter:  {text}"),
        }
    }
}

pub fn plan(bound: &BoundQuery) -> Plan {
    let table = &bound.table;
    let columns: Vec<BoundColumn> = bound.output().cloned().collect();
    let select_list: Vec<String> = columns
        .iter()
        .map(|c| {
            if c.clickhouse == c.name {
                quote_ident(&c.clickhouse)
            } else {
                format!("{} AS {}", quote_ident(&c.clickhouse), quote_ident(&c.name))
            }
        })
        .collect();
    let history = HistoryPlan {
        table: table.clickhouse_table.clone(),
        select: format!(
            "SELECT {} FROM {}",
            select_list.join(", "),
            table.clickhouse_table
        ),
        filter: bound.filter.as_ref().map(|f| to_clickhouse(f, table)),
        kafka: table.kafka.clone(),
    };

    // Columns to decode: the output's, then the filter's that aren't output.
    let mut needed: Vec<ColumnId> = bound.projection.clone();
    if let Some(filter) = &bound.filter {
        filter.columns(&mut needed);
    }
    let stream = StreamPlan {
        topics: vec![table.topic.clone()],
        columns: needed
            .iter()
            .map(|id| {
                let c = table.column(*id);
                StreamColumn {
                    id: *id,
                    json_key: c.json_key.clone(),
                    ty: c.ty.clone(),
                }
            })
            .collect(),
        width: table.columns.len(),
        output: columns.iter().map(|c| (c.name.clone(), c.id)).collect(),
        filter: match &bound.filter {
            None => Filter::All,
            Some(expr) => Filter::Where {
                expr: expr.clone(),
                text: to_dialect(expr, table),
            },
        },
    };
    Plan {
        table: table.name.clone(),
        columns,
        history,
        stream,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn table() -> BoundTable {
        let col = |i: usize, n: &str, ty: &str| BoundColumn {
            id: ColumnId(i),
            name: n.into(),
            clickhouse: n.into(),
            json_key: n.into(),
            ty: Type::parse(ty),
        };
        BoundTable {
            name: "events".into(),
            clickhouse_table: "default.events".into(),
            topic: TopicId("events".into()),
            kafka: KafkaColumns {
                partition: "_partition".into(),
                offset: "_offset".into(),
            },
            columns: vec![
                col(0, "ts", "DateTime64(3)"),
                col(1, "user_id", "String"),
                col(2, "latency_ms", "UInt32"),
                col(3, "coupon", "Nullable(String)"),
            ],
        }
    }

    fn bound(projection: &[usize], filter: Option<BoundExpr>) -> BoundQuery {
        BoundQuery {
            table: table(),
            filter,
            projection: projection.iter().map(|i| ColumnId(*i)).collect(),
        }
    }

    fn col(i: usize) -> Box<BoundExpr> {
        Box::new(BoundExpr::Column(ColumnId(i)))
    }

    fn lit(v: Value) -> Box<BoundExpr> {
        Box::new(BoundExpr::Literal(v))
    }

    fn row(v: serde_json::Value) -> Row {
        v.as_object().unwrap().clone()
    }

    #[test]
    fn select_star_plan() {
        let p = plan(&bound(&[0, 1, 2, 3], None));
        assert_eq!(
            p.history.sql(None),
            "SELECT ts, user_id, latency_ms, coupon FROM default.events"
        );
        assert_eq!(p.stream.topics, [TopicId("events".into())]);
        assert_eq!(
            p.stream.required_keys(),
            ["ts", "user_id", "latency_ms", "coupon"]
        );
        assert_eq!(p.stream.filter, Filter::All);
    }

    #[test]
    fn filter_columns_are_decoded_but_not_output() {
        let f = BoundExpr::Compare {
            op: CmpOp::Gt,
            left: col(2),
            right: lit(Value::Int(100)),
        };
        let p = plan(&bound(&[1], Some(f)));
        assert_eq!(
            p.history.sql(None),
            "SELECT user_id FROM default.events WHERE latency_ms > 100"
        );
        assert_eq!(p.stream.required_keys(), ["user_id", "latency_ms"]);
        let fast = p
            .stream
            .decode(&row(json!({"user_id": "a", "latency_ms": 50})))
            .unwrap();
        let slow = p
            .stream
            .decode(&row(json!({"user_id": "b", "latency_ms": 500})))
            .unwrap();
        assert!(!p.stream.matches(&fast));
        assert!(p.stream.matches(&slow));
        assert_eq!(
            serde_json::to_string(&p.stream.project(&slow)).unwrap(),
            r#"{"user_id":"b"}"#
        );
        assert_eq!(
            p.to_string().lines().last().unwrap(),
            "filter:  latency_ms > 100"
        );
    }

    #[test]
    fn history_sql_is_rendered_from_typed_values() {
        let f = BoundExpr::And(
            Box::new(BoundExpr::Or(
                Box::new(BoundExpr::Compare {
                    op: CmpOp::Eq,
                    left: col(1),
                    right: lit(Value::String("it's".into())),
                }),
                Box::new(BoundExpr::IsNull {
                    expr: col(3),
                    negated: true,
                }),
            )),
            Box::new(BoundExpr::InList {
                expr: col(2),
                list: vec![Value::Int(1), Value::Int(-2)],
                negated: true,
            }),
        );
        let p = plan(&bound(&[1], Some(f)));
        assert_eq!(
            p.history.sql(None),
            r"SELECT user_id FROM default.events WHERE ((user_id = 'it\'s') OR (coupon IS NOT NULL)) AND (latency_ms NOT IN (1, -2))"
        );
    }

    #[test]
    fn history_is_bounded_by_the_watermark() {
        let w = Watermark(std::collections::BTreeMap::from([(0, 7), (2, 3)]));
        let p = plan(&bound(&[1], None));
        assert_eq!(
            p.history.sql(Some(&w)),
            "SELECT user_id FROM default.events WHERE toInt64(_offset) <= multiIf(_partition = 0, 7, _partition = 2, 3, -1)"
        );
        let f = BoundExpr::Or(
            Box::new(BoundExpr::IsNull {
                expr: col(3),
                negated: false,
            }),
            lit(Value::Bool(false)),
        );
        let p = plan(&bound(&[1], Some(f)));
        // the filter is parenthesized as a whole, so its OR can't swallow the bound
        assert_eq!(
            p.history.sql(Some(&w)),
            "SELECT user_id FROM default.events WHERE ((coupon IS NULL) OR (false)) AND (toInt64(_offset) <= multiIf(_partition = 0, 7, _partition = 2, 3, -1))"
        );
    }

    #[test]
    fn three_valued_logic() {
        // coupon = 'X' is unknown for a NULL coupon: neither it nor its negation matches
        let eq = BoundExpr::Compare {
            op: CmpOp::Eq,
            left: col(3),
            right: lit(Value::String("X".into())),
        };
        let p = plan(&bound(&[1], Some(eq.clone())));
        let null_coupon = p.stream.decode(&row(json!({"user_id": "a"}))).unwrap();
        assert!(!p.stream.matches(&null_coupon));
        let p = plan(&bound(&[1], Some(BoundExpr::Not(Box::new(eq.clone())))));
        assert!(!p.stream.matches(&null_coupon));
        // ... but OR with something true is true
        let or_true = BoundExpr::Or(Box::new(eq), lit(Value::Bool(true)));
        let p = plan(&bound(&[1], Some(or_true)));
        assert!(p.stream.matches(&null_coupon));
    }

    #[test]
    fn missing_keys_follow_the_column_type() {
        let p = plan(&bound(&[0, 2, 3], None));
        let r = p.stream.decode(&row(json!({}))).unwrap();
        assert_eq!(
            serde_json::to_string(&p.stream.project(&r)).unwrap(),
            r#"{"ts":"1970-01-01 00:00:00.000","latency_ms":0,"coupon":null}"#
        );
        let err = p
            .stream
            .decode(&row(json!({"latency_ms": "slow"})))
            .unwrap_err();
        assert_eq!(
            err.to_string(),
            r#"column latency_ms: cannot read "slow" as UInt32"#
        );
    }
}
