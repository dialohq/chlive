//! A query as the binder resolves it: its tables, columns, expressions and clauses by resolved
//! identifiers (see the engine's binder, which makes them from a parsed query and the catalog).

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::ast::CmpOp;
use crate::calendar::Interval;
use crate::error::Span;
use crate::rules::{ColumnSource, TopicId};
use crate::table_engine::TableEngine;
use crate::types::{Type, Value};

/// with the rules event JSON is read by (`"42"` or `42` for a UInt32, …).
pub type Params = BTreeMap<String, serde_json::Value>;

/// Index of a column in [`BoundTable::columns`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ColumnId(pub usize);

/// A column of a dialect table, with where it lives on each side.
#[derive(Debug, Clone, PartialEq)]
pub struct BoundColumn {
    pub id: ColumnId,
    /// Name in the dialect and in output rows.
    pub name: String,
    /// Column in ClickHouse.
    pub clickhouse: String,
    /// Where events hold it: a key of their JSON objects, or a Kafka field.
    pub source: ColumnSource,
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
    /// Its ClickHouse engine: a MergeTree or a ReplacingMergeTree.
    pub engine: TableEngine,
    /// Columns queries can't name but the engine needs (a FINAL key, version or is-deleted
    /// column the naming rules hide); their ids come after `columns`'.
    pub internal: Vec<BoundColumn>,
    /// `FROM t FINAL` on a ReplacingMergeTree: what its rows are replaced by.
    pub final_key: Option<FinalKey>,
    /// Columns the subscriber may not see (left out of `columns`): naming one is
    /// [`BindError::Denied`].
    pub denied: Vec<String>,
}

/// A ReplacingMergeTree read with FINAL: one row per key, its latest version (the highest
/// `ver`, the last one on ties or without), gone when that version is deleted.
#[derive(Debug, Clone, PartialEq)]
pub struct FinalKey {
    /// The sorting key's columns.
    pub key: Vec<ColumnId>,
    pub ver: Option<ColumnId>,
    pub deleted: Option<ColumnId>,
}

impl BoundTable {
    /// Whether `q` (in `q.column`) names this table: its name or its alias.
    pub fn names(&self, q: &str) -> bool {
        q == self.name || self.alias.as_deref() == Some(q)
    }

    /// What to snapshot for this table's history.
    pub fn snapshot_table(&self) -> crate::table::SnapshotTable {
        crate::table::SnapshotTable::new(&self.database, &self.clickhouse_name, &self.topic.0)
    }

    pub fn column(&self, id: ColumnId) -> &BoundColumn {
        match id.0.checked_sub(self.columns.len()) {
            Some(i) => &self.internal[i],
            None => &self.columns[id.0],
        }
    }

    /// How many columns a decoded row has: the visible ones, then the internal ones.
    pub fn width(&self) -> usize {
        self.columns.len() + self.internal.len()
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
    /// The `now()` comparisons in `filter` (`BoundExpr::Now` refers to them by index): rows go
    /// in and out as time passes.
    pub windows: Vec<TimeWindow>,
}

/// The `now(['tz']) ± INTERVAL …` side of a comparison in the WHERE (`BoundExpr::Now` has the
/// column and the operator): the server's clock with the intervals applied (DAY and WEEK by the
/// calendar in `tz`, the server's timezone if `None`). `column` is the column it's compared
/// with.
#[derive(Debug, Clone, PartialEq)]
pub struct TimeWindow {
    pub column: ColumnId,
    pub tz: Option<chrono_tz::Tz>,
    pub intervals: Vec<Interval>,
    /// The `now`'s, for errors.
    pub span: Span,
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
    /// `groupArray` (also `array_agg`): every value, NULLs skipped.
    GroupArray,
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
            AggFunc::GroupArray => "groupArray",
        }
    }
}

/// One ORDER BY key, with ClickHouse's defaults resolved: ascending, NULLS LAST.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderKey {
    pub column: ColumnId,
    pub desc: bool,
    pub nulls_first: bool,
}

/// A typed expression. Literals are already coerced to the type they are compared with.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
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
    /// `column op now() ± INTERVAL …`, the column on the left (`<`, `<=`, `>`, `>=`); `window`
    /// is the query's [`TimeWindow`] for the `now()` side.
    Now {
        window: usize,
        column: ColumnId,
        op: CmpOp,
    },
}

impl BoundExpr {
    /// Each `now()` comparison in it: window, operator, column.
    pub fn visit_now(&self, f: &mut impl FnMut(usize, CmpOp, ColumnId)) {
        match self {
            BoundExpr::Now { window, column, op } => f(*window, *op, *column),
            BoundExpr::And(l, r) | BoundExpr::Or(l, r) => {
                l.visit_now(f);
                r.visit_now(f);
            }
            BoundExpr::Not(e) => e.visit_now(f),
            _ => {}
        }
    }

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
            BoundExpr::Now { column, .. } => {
                if !out.contains(column) {
                    out.push(*column);
                }
            }
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
