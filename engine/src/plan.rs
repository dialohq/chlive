//! From a bound query to what to run: the ClickHouse query for history, and for the stream:
//! which topics to consume, which keys of their events to decode (and as which types), which
//! events are relevant, how to turn them into result rows and, with ORDER BY, where they go.

use std::cmp::Ordering;

use crate::ast::CmpOp;
use crate::binder::{
    AggExpr, AggFunc, Aggregation, BoundColumn, BoundExpr, BoundQuery, BoundTable, ColumnId,
    OrderKey,
};
use crate::diff::Row;
use crate::order::{KeyOrder, SortKey};
use crate::rules::{quote_ident, TopicId};
use crate::types::quote_string;
use crate::types::{decode, DecodeError, Type, Value};

#[derive(Debug, Clone, PartialEq)]
pub struct Plan {
    pub table: String,
    /// What history snapshots are taken of: the ClickHouse table and its topic.
    pub source: crate::snapshot::SnapshotTable,
    /// Output columns, in output order.
    pub columns: Vec<BoundColumn>,
    pub history: HistoryPlan,
    pub stream: StreamPlan,
    /// With GROUP BY or aggregates: how chDB keeps and merges the per-group states.
    pub aggregate: Option<AggregatePlan>,
}

/// GROUP BY and aggregates, run in chDB: main ClickHouse computes the history's aggregate
/// states (`countState()`, …) into a chDB table; each batch of events adds its own states; the
/// `-Merge` functions finalize. Grouping keys are always kept, as `__k0…`, for row ids.
#[derive(Debug, Clone, PartialEq)]
pub struct AggregatePlan {
    /// Output columns: name and type, in SELECT order.
    pub columns: Vec<(String, Type)>,
    /// How many grouping keys (none: one global group).
    pub keys: usize,
    /// `country AS __k0, countState() AS s0, avgState(latency_ms) AS s1`
    state_select: String,
    /// `__k0, __k1`, empty for a global aggregate.
    group_by: String,
    /// `__k0, __k1, __k0 AS \`country\`, countMerge(s0) AS \`count()\``
    merge_select: String,
    /// `country, path`: the keys in a batch's rows.
    batch_keys: String,
    /// What a batch's rows carry: the keys and arguments, by ClickHouse name.
    pub batch_columns: Vec<(ColumnId, String)>,
    /// `'country LowCardinality(String), latency_ms UInt32'`, for `format()`.
    structure: String,
    filter: Option<String>,
    /// ORDER BY: the row columns (output or hidden) to order groups by.
    pub order: Vec<crate::aggregate::result::GroupOrderTerm>,
    /// LIMIT: how many groups the result has (by `order`, or the first seen).
    pub limit: Option<u64>,
    /// Columns computed only for ORDER BY (`__o0`, …), not output.
    pub hidden: Vec<String>,
    /// When every aggregate has a native implementation: how to run without chDB.
    pub native: Option<NativeAggregates>,
}

/// GROUP BY with native aggregates: their states come from main ClickHouse as plain SQL values
/// (`count()`, `sum(x)`, …) and live in the engine.
#[derive(Debug, Clone, PartialEq)]
pub struct NativeAggregates {
    /// The grouping keys' columns, in order (`__k0…` in the history query).
    pub keys: Vec<ColumnId>,
    pub aggs: Vec<NativeAgg>,
    /// The output columns, in SELECT order.
    pub items: Vec<NativeItem>,
    /// `country AS __k0, count() AS __a0_0, …`
    select: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct NativeAgg {
    pub func: AggFunc,
    pub arg: Option<ColumnId>,
    pub arg_ty: Option<Type>,
    pub result_ty: Type,
    /// The history query's columns for this aggregate's state: alias and type.
    pub history: Vec<(String, Type)>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum NativeItem {
    /// The i-th grouping key, named.
    Key(usize, String),
    /// The i-th aggregate, named.
    Agg(usize, String),
}

impl AggregatePlan {
    /// The result as the client gets it: ordered and limited.
    pub fn result(&self) -> crate::aggregate::result::GroupResult {
        crate::aggregate::result::GroupResult::new(
            self.order.clone(),
            self.limit,
            self.hidden.clone(),
        )
    }

    /// Native: the history's per-group states from main ClickHouse, read from `from` (a
    /// snapshot of the table).
    pub fn native_history_sql(&self, from: &str) -> Option<String> {
        let native = self.native.as_ref()?;
        Some(format!(
            "SELECT {} FROM {from}{}{}",
            native.select,
            self.where_clause(),
            self.group_by_clause()
        ))
    }

    /// Create the chDB table with the history's states, computed by main ClickHouse (`remote`:
    /// the `remote(…)` table function reading a snapshot of the table).
    pub fn create_sql(&self, table: &str, remote: &str) -> String {
        let order = if self.keys == 0 {
            "tuple()".to_string()
        } else {
            format!("({})", self.group_by)
        };
        format!(
            "CREATE TABLE {table} ENGINE = AggregatingMergeTree ORDER BY {order} \
             SETTINGS allow_nullable_key = 1 AS SELECT {} FROM {remote}{}{}",
            self.state_select,
            self.where_clause(),
            self.group_by_clause()
        )
    }

    /// The `remote()` reading `table` (`db.name`) from main ClickHouse.
    pub fn remote(table: &str, address: &str, user: &str, password: &str) -> String {
        format!(
            "remote({}, {table}, {}, {})",
            quote_string(address),
            quote_string(user),
            quote_string(password)
        )
    }

    fn where_clause(&self) -> String {
        match &self.filter {
            Some(f) => format!(" WHERE {f}"),
            None => String::new(),
        }
    }

    /// Every group's finalized values.
    pub fn snapshot_sql(&self, table: &str) -> String {
        format!(
            "SELECT {} FROM {table}{}",
            self.merge_select,
            self.group_by_clause()
        )
    }

    /// Add a batch's states (`rows`: JSONEachRow of `batch_columns`).
    pub fn insert_sql(&self, table: &str, rows: &str) -> String {
        format!(
            "INSERT INTO {table} SELECT {} FROM {}{}",
            self.state_select,
            self.format(rows),
            self.group_by_clause()
        )
    }

    /// The finalized values of the groups a batch touched.
    pub fn touched_sql(&self, table: &str, rows: &str) -> String {
        if self.keys == 0 {
            return self.snapshot_sql(table);
        }
        format!(
            "SELECT {} FROM {table} WHERE ({}) IN (SELECT {} FROM {}){}",
            self.merge_select,
            self.group_by,
            self.batch_keys,
            self.format(rows),
            self.group_by_clause()
        )
    }

    fn format(&self, rows: &str) -> String {
        format!(
            "format(JSONEachRow, {}, {})",
            quote_string(&self.structure),
            quote_string(rows)
        )
    }

    fn group_by_clause(&self) -> String {
        if self.keys == 0 {
            String::new()
        } else {
            format!(" GROUP BY {}", self.group_by)
        }
    }
}

/// The column of a batch's rows when the aggregates read none (e.g. a global `count()`).
pub const PLACEHOLDER: &str = "__one";

fn backquote(name: &str) -> String {
    format!("`{}`", name.replace('\\', "\\\\").replace('`', "\\`"))
}

/// The native plan, if every aggregate has a native implementation.
fn native_aggregates(agg: &Aggregation, table: &BoundTable) -> Option<NativeAggregates> {
    let col = |id: ColumnId| quote_ident(&table.column(id).clickhouse);
    let mut select: Vec<String> = agg
        .keys
        .iter()
        .enumerate()
        .map(|(i, k)| format!("{} AS __k{i}", col(*k)))
        .collect();
    let (mut aggs, mut items) = (Vec::new(), Vec::new());
    for item in &agg.items {
        match &item.expr {
            AggExpr::Key(id) => {
                let k = agg
                    .keys
                    .iter()
                    .position(|k| k == id)
                    .expect("a grouping key");
                items.push(NativeItem::Key(k, item.name.clone()));
            }
            AggExpr::Call { func, arg } => {
                let f = crate::aggregate::native(*func)?;
                let arg_ty = arg.map(|a| table.column(a).ty.clone());
                let arg_sql = arg.map(col);
                let j = aggs.len();
                let history: Vec<(String, Type)> = f
                    .history_sql(arg_sql.as_deref(), arg_ty.as_ref(), &item.ty)
                    .into_iter()
                    .enumerate()
                    .map(|(n, (sql, ty))| {
                        let alias = format!("__a{j}_{n}");
                        select.push(format!("{sql} AS {alias}"));
                        (alias, ty)
                    })
                    .collect();
                aggs.push(NativeAgg {
                    func: *func,
                    arg: *arg,
                    arg_ty,
                    result_ty: item.ty.clone(),
                    history,
                });
                items.push(NativeItem::Agg(j, item.name.clone()));
            }
        }
    }
    Some(NativeAggregates {
        keys: agg.keys.clone(),
        aggs,
        items,
        select: select.join(", "),
    })
}

fn aggregate_plan(agg: &Aggregation, table: &BoundTable, filter: Option<String>) -> AggregatePlan {
    let col = |id: ColumnId| quote_ident(&table.column(id).clickhouse);
    let mut state = Vec::new();
    let mut merge = Vec::new();
    for (i, k) in agg.keys.iter().enumerate() {
        state.push(format!("{} AS __k{i}", col(*k)));
        merge.push(format!("__k{i}"));
    }
    let mut batch_columns: Vec<(ColumnId, String)> = agg
        .keys
        .iter()
        .map(|k| (*k, table.column(*k).clickhouse.clone()))
        .collect();
    let mut states = 0;
    for item in &agg.items {
        match &item.expr {
            AggExpr::Key(id) => {
                let k = agg
                    .keys
                    .iter()
                    .position(|k| k == id)
                    .expect("a grouping key");
                merge.push(format!("__k{k} AS {}", backquote(&item.name)));
            }
            AggExpr::Call { func, arg } => {
                let f = func.name();
                let arg_sql = arg.map(col).unwrap_or_default();
                state.push(format!("{f}State({arg_sql}) AS s{states}"));
                merge.push(format!("{f}Merge(s{states}) AS {}", backquote(&item.name)));
                states += 1;
                if let Some(a) = arg {
                    if !batch_columns.iter().any(|(id, _)| id == a) {
                        batch_columns.push((*a, table.column(*a).clickhouse.clone()));
                    }
                }
            }
        }
    }
    let structure = if batch_columns.is_empty() {
        // e.g. a global count(): rows with no columns; `format()` needs one
        format!("{PLACEHOLDER} UInt8")
    } else {
        batch_columns
            .iter()
            .map(|(id, name)| format!("{} {}", quote_ident(name), table.column(*id).ty.clickhouse))
            .collect::<Vec<_>>()
            .join(", ")
    };
    let native = native_aggregates(agg, table);
    AggregatePlan {
        native,
        columns: agg
            .items
            .iter()
            .filter(|i| !i.hidden)
            .map(|i| (i.name.clone(), i.ty.clone()))
            .collect(),
        order: agg
            .order
            .iter()
            .map(|k| crate::aggregate::result::GroupOrderTerm {
                column: agg.items[k.item].name.clone(),
                ty: agg.items[k.item].ty.clone(),
                desc: k.desc,
                nulls_first: k.nulls_first,
            })
            .collect(),
        limit: agg.limit,
        hidden: agg
            .items
            .iter()
            .filter(|i| i.hidden)
            .map(|i| i.name.clone())
            .collect(),
        keys: agg.keys.len(),
        state_select: state.join(", "),
        group_by: (0..agg.keys.len())
            .map(|i| format!("__k{i}"))
            .collect::<Vec<_>>()
            .join(", "),
        merge_select: merge.join(", "),
        batch_keys: agg
            .keys
            .iter()
            .map(|k| col(*k))
            .collect::<Vec<_>>()
            .join(", "),
        batch_columns,
        structure,
        filter,
    }
}

/// The history query. It reads a snapshot of the table, which is only known when subscribing.
#[derive(Debug, Clone, PartialEq)]
pub struct HistoryPlan {
    /// The table snapshots are taken of (`db.table`).
    pub table: String,
    /// The SELECT list.
    pub select: String,
    /// The WHERE condition, rendered for ClickHouse.
    pub filter: Option<String>,
    /// `ORDER BY …`, with every key's direction and NULL placement explicit.
    pub order: Option<String>,
    pub limit: Option<u64>,
    /// With ORDER BY: the sort columns, the keys a resume rebuilds the order from.
    pub keys_select: Option<String>,
}

impl HistoryPlan {
    /// The query, reading `from` (a snapshot of [`Self::table`]).
    pub fn sql(&self, from: &str) -> String {
        self.build(&self.select, from, self.order.as_deref(), self.limit)
    }

    /// Resume: the sort keys of the result (with ORDER BY), in order.
    pub fn keys_sql(&self, from: &str) -> Option<String> {
        let select = self.keys_select.as_ref()?;
        Some(self.build(select, from, self.order.as_deref(), self.limit))
    }

    /// Resume: how many rows the result has, at most the LIMIT.
    pub fn count_sql(&self, from: &str) -> String {
        let inner = self.build("1", from, None, self.limit);
        format!("SELECT count() AS n FROM ({inner})")
    }

    fn build(&self, select: &str, from: &str, order: Option<&str>, limit: Option<u64>) -> String {
        let mut sql = format!("SELECT {select} FROM {from}");
        if let Some(f) = &self.filter {
            sql.push_str(&format!(" WHERE {f}"));
        }
        if let Some(order) = order {
            sql.push(' ');
            sql.push_str(order);
        }
        if let Some(limit) = limit {
            sql.push_str(&format!(" LIMIT {limit}"));
        }
        sql
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
    /// With ORDER BY: how to place rows.
    pub order: Option<OrderPlan>,
    /// LIMIT: with ORDER BY, the top N; without, a window of the latest N rows.
    pub limit: Option<u64>,
}

/// ORDER BY for the stream: the keys, and where each key's value is found in a history row.
#[derive(Debug, Clone, PartialEq)]
pub struct OrderPlan {
    pub keys: KeyOrder,
    pub columns: Vec<OrderColumn>,
    /// Hidden history fields (`__order_<i>`) that carry ORDER BY values of columns that aren't
    /// selected; removed from history rows before they are output.
    pub hidden: Vec<String>,
    /// The ORDER BY in dialect syntax, for `explain`.
    pub text: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct OrderColumn {
    pub id: ColumnId,
    /// Its output name when selected, otherwise the column's dialect name.
    pub name: String,
    /// The field holding this column in a history row: its output name, or a hidden alias.
    pub history_field: String,
    pub ty: Type,
}

impl OrderPlan {
    /// The sort key of a history row (as ClickHouse rendered it), removing hidden fields.
    pub fn history_key(&self, row: &mut Row) -> Result<SortKey, RowDecodeError> {
        let key = self
            .columns
            .iter()
            .map(|c| {
                decode(row.get(&c.history_field), &c.ty).map_err(|error| RowDecodeError {
                    column: c.history_field.clone(),
                    error,
                })
            })
            .collect();
        for h in &self.hidden {
            row.shift_remove(h);
        }
        key
    }

    /// The sort key of a decoded event.
    pub fn event_key(&self, row: &DecodedRow) -> SortKey {
        self.columns.iter().map(|c| row[c.id.0].clone()).collect()
    }
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
        if let Some(a) = &self.aggregate {
            let names: Vec<String> = a.columns.iter().map(|(n, t)| format!("{n} {t}")).collect();
            writeln!(f, "output:  {}", names.join(", "))?;
            writeln!(
                f,
                "history: {}",
                a.create_sql(
                    "<chdb table>",
                    &AggregatePlan::remote("<snapshot>", "<main>", "<user>", "<password>")
                )
            )?;
            writeln!(f, "merge:   {}", a.snapshot_sql("<chdb table>"))?;
            match a.native_history_sql("<snapshot>") {
                Some(sql) => writeln!(f, "native:  {sql} (states in the engine; chDB unused)")?,
                None => writeln!(
                    f,
                    "native:  no (an aggregate has no native implementation: chDB)"
                )?,
            }
            if !a.order.is_empty() {
                let terms: Vec<String> = a
                    .order
                    .iter()
                    .map(|t| {
                        format!(
                            "{} {} NULLS {}",
                            t.column,
                            if t.desc { "DESC" } else { "ASC" },
                            if t.nulls_first { "FIRST" } else { "LAST" }
                        )
                    })
                    .collect();
                writeln!(
                    f,
                    "order:   {} (groups move as their values change)",
                    terms.join(", ")
                )?;
            }
            if let Some(n) = a.limit {
                writeln!(
                    f,
                    "limit:   {n} groups ({}; the others are kept, to come in later)",
                    if a.order.is_empty() {
                        "the first seen"
                    } else {
                        "the first by order"
                    }
                )?;
            }
            let batch: Vec<&str> = a.batch_columns.iter().map(|(_, n)| n.as_str()).collect();
            writeln!(
                f,
                "batches: their {} into chDB as states; touched groups read back merged",
                batch.join(", ")
            )?;
        } else {
            writeln!(f, "history: {}", self.history.sql("<snapshot>"))?;
        }
        writeln!(
            f,
            "         <snapshot>: a clone of {} made while its ingestion was paused, with the \
             offsets it holds",
            self.history.table
        )?;
        writeln!(
            f,
            "topics:  {} (each partition from right after that)",
            topics.join(", ")
        )?;
        writeln!(f, "keys:    {}", keys.join(", "))?;
        match &self.stream.filter {
            Filter::All => write!(f, "filter:  all events")?,
            Filter::Where { text, .. } => write!(f, "filter:  {text}")?,
        }
        if let Some(order) = &self.stream.order {
            write!(
                f,
                "\norder:   {} (a new row goes after the last row it ties with)",
                order.text
            )?;
        }
        match (self.stream.limit, &self.stream.order) {
            (Some(n), Some(_)) => write!(
                f,
                "\nlimit:   {n} (the top {n}: a row inserted inside them pushes the last one out)"
            )?,
            (Some(n), None) => write!(
                f,
                "\nlimit:   {n} (a window of the latest {n} rows: each new one pushes out the first)"
            )?,
            (None, _) => {}
        }
        Ok(())
    }
}

/// `ASC NULLS LAST` etc.: always explicit, so ClickHouse and the stream can't disagree.
fn order_modifiers(k: &OrderKey) -> String {
    format!(
        "{} NULLS {}",
        if k.desc { "DESC" } else { "ASC" },
        if k.nulls_first { "FIRST" } else { "LAST" }
    )
}

pub fn plan(bound: &BoundQuery) -> Plan {
    let table = &bound.table;
    let columns: Vec<BoundColumn> = bound.output().collect();
    let mut select_list: Vec<String> = columns
        .iter()
        .map(|c| {
            if c.clickhouse == c.name {
                quote_ident(&c.clickhouse)
            } else {
                format!("{} AS {}", quote_ident(&c.clickhouse), quote_ident(&c.name))
            }
        })
        .collect();

    // ORDER BY: an ordered column that isn't selected is fetched under a hidden alias, so the
    // history rows carry every sort value.
    let mut order_columns = Vec::new();
    let mut hidden = Vec::new();
    for k in &bound.order {
        let c = table.column(k.column);
        // a selected column is in history rows under its output name (its first, if selected
        // more than once)
        let selected = bound.projection.iter().find(|p| p.column == k.column);
        let history_field = if let Some(p) = selected {
            p.name.clone()
        } else if let Some(existing) = order_columns
            .iter()
            .find(|o: &&OrderColumn| o.id == k.column)
        {
            existing.history_field.clone()
        } else {
            let alias = format!("__order_{}", hidden.len());
            select_list.push(format!("{} AS {alias}", quote_ident(&c.clickhouse)));
            hidden.push(alias.clone());
            alias
        };
        order_columns.push(OrderColumn {
            id: k.column,
            name: selected.map_or_else(|| c.name.clone(), |p| p.name.clone()),
            history_field,
            ty: c.ty.clone(),
        });
    }
    let order_sql = (!bound.order.is_empty()).then(|| {
        let keys: Vec<String> = bound
            .order
            .iter()
            .map(|k| {
                format!(
                    "{} {}",
                    quote_ident(&table.column(k.column).clickhouse),
                    order_modifiers(k)
                )
            })
            .collect();
        format!("ORDER BY {}", keys.join(", "))
    });
    let history = HistoryPlan {
        table: table.clickhouse_table.clone(),
        select: select_list.join(", "),
        filter: bound.filter.as_ref().map(|f| to_clickhouse(f, table)),
        order: order_sql,
        limit: bound.limit,
        keys_select: (!order_columns.is_empty()).then(|| {
            let mut fields: Vec<String> = Vec::new();
            for o in &order_columns {
                let c = table.column(o.id);
                let field = if o.history_field == c.clickhouse {
                    quote_ident(&c.clickhouse)
                } else {
                    format!(
                        "{} AS {}",
                        quote_ident(&c.clickhouse),
                        quote_ident(&o.history_field)
                    )
                };
                if !fields.contains(&field) {
                    fields.push(field);
                }
            }
            fields.join(", ")
        }),
    };

    // Columns to decode: the output's, then the filter's and ORDER BY's that aren't output.
    let mut needed: Vec<ColumnId> = Vec::new();
    for p in &bound.projection {
        if !needed.contains(&p.column) {
            needed.push(p.column);
        }
    }
    if let Some(filter) = &bound.filter {
        filter.columns(&mut needed);
    }
    for k in &bound.order {
        if !needed.contains(&k.column) {
            needed.push(k.column);
        }
    }
    let aggregate = bound.aggregation.as_ref().map(|a| {
        aggregate_plan(
            a,
            table,
            bound.filter.as_ref().map(|f| to_clickhouse(f, table)),
        )
    });
    if let Some(a) = &aggregate {
        for (id, _) in &a.batch_columns {
            if !needed.contains(id) {
                needed.push(*id);
            }
        }
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
        order: (!bound.order.is_empty()).then(|| OrderPlan {
            keys: KeyOrder(bound.order.clone()),
            text: bound
                .order
                .iter()
                .map(|k| format!("{} {}", table.column(k.column).name, order_modifiers(k)))
                .collect::<Vec<_>>()
                .join(", "),
            columns: order_columns,
            hidden,
        }),
        limit: bound.limit,
    };
    Plan {
        table: table.name.clone(),
        source: table.snapshot_table(),
        columns,
        history,
        stream,
        aggregate,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::binder::Projected;
    use crate::watermark::Watermark;
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
            database: "default".into(),
            clickhouse_name: "events".into(),
            topic: TopicId("events".into()),
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
            projection: projection
                .iter()
                .map(|i| Projected {
                    column: ColumnId(*i),
                    name: table().columns[*i].name.clone(),
                })
                .collect(),
            order: Vec::new(),
            limit: None,
            aggregation: None,
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
            p.history.sql("default.events"),
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
            p.history.sql("default.events"),
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
            p.history.sql("default.events"),
            r"SELECT user_id FROM default.events WHERE ((user_id = 'it\'s') OR (coupon IS NOT NULL)) AND (latency_ms NOT IN (1, -2))"
        );
    }

    #[test]
    fn history_reads_the_snapshot() {
        let p = plan(&bound(&[1], None));
        assert_eq!(
            p.history.sql("default.__chlive_snap_events_1_0"),
            "SELECT user_id FROM default.__chlive_snap_events_1_0"
        );
        let f = BoundExpr::Or(
            Box::new(BoundExpr::IsNull {
                expr: col(3),
                negated: false,
            }),
            lit(Value::Bool(false)),
        );
        let p = plan(&bound(&[1], Some(f)));
        assert_eq!(
            p.history.sql("s"),
            "SELECT user_id FROM s WHERE (coupon IS NULL) OR (false)"
        );
        // the logical table is what cursors are tied to
        assert_eq!(p.history.table, "default.events");
        assert_eq!(p.source.table, "events");
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

    fn ordered(
        projection: &[usize],
        filter: Option<BoundExpr>,
        order: &[(usize, bool, bool)],
    ) -> BoundQuery {
        let mut b = bound(projection, filter);
        b.order = order
            .iter()
            .map(|(c, desc, nulls_first)| OrderKey {
                column: ColumnId(*c),
                desc: *desc,
                nulls_first: *nulls_first,
            })
            .collect();
        b
    }

    #[test]
    fn order_by_in_history_and_stream() {
        // ORDER BY latency_ms DESC (not selected), user_id (selected), latency_ms again
        let f = BoundExpr::IsNull {
            expr: col(3),
            negated: true,
        };
        let p = plan(&ordered(
            &[1],
            Some(f),
            &[(2, true, false), (1, false, true), (2, false, false)],
        ));
        assert_eq!(
            p.history.sql("default.events"),
            "SELECT user_id, latency_ms AS __order_0 FROM default.events WHERE coupon IS NOT NULL \
             ORDER BY latency_ms DESC NULLS LAST, user_id ASC NULLS FIRST, latency_ms ASC NULLS LAST"
        );
        // order-only columns are decoded, but not output
        assert_eq!(
            p.stream.required_keys(),
            ["user_id", "coupon", "latency_ms"]
        );
        let order = p.stream.order.as_ref().unwrap();
        assert_eq!(order.hidden, ["__order_0"]);

        // a history row: its key comes from output and hidden fields; hidden ones are removed
        let mut h = row(json!({"user_id": "a", "__order_0": 120}));
        assert_eq!(
            order.history_key(&mut h).unwrap(),
            [Value::Int(120), Value::String("a".into()), Value::Int(120)]
        );
        assert_eq!(serde_json::to_string(&h).unwrap(), r#"{"user_id":"a"}"#);

        let e = p
            .stream
            .decode(&row(
                json!({"user_id": "b", "latency_ms": "75", "coupon": "X"}),
            ))
            .unwrap();
        assert_eq!(
            order.event_key(&e),
            [Value::Int(75), Value::String("b".into()), Value::Int(75)]
        );
        assert_eq!(
            p.to_string().lines().last().unwrap(),
            "order:   latency_ms DESC NULLS LAST, user_id ASC NULLS FIRST, latency_ms ASC NULLS LAST \
             (a new row goes after the last row it ties with)"
        );
        assert!(plan(&bound(&[1], None)).stream.order.is_none());
    }

    #[test]
    fn aliases_in_history_stream_and_order() {
        // SELECT user_id AS u, latency_ms AS l, latency_ms ORDER BY latency_ms DESC
        let mut b = ordered(&[1, 2, 2], None, &[(2, true, false)]);
        b.projection[0].name = "u".into();
        b.projection[1].name = "l".into();
        let p = plan(&b);
        assert_eq!(
            p.history.sql("default.events"),
            "SELECT user_id AS u, latency_ms AS l, latency_ms FROM default.events \
             ORDER BY latency_ms DESC NULLS LAST"
        );
        // a selected ORDER BY column is read from history rows under its (first) output name
        let order = p.stream.order.as_ref().unwrap();
        assert!(order.hidden.is_empty());
        assert_eq!(order.columns[0].history_field, "l");
        assert_eq!(order.columns[0].name, "l");
        assert!(p
            .history
            .keys_sql("s")
            .unwrap()
            .starts_with("SELECT latency_ms AS l FROM s"));
        // each column is decoded once; output under every name
        assert_eq!(p.stream.required_keys(), ["user_id", "latency_ms"]);
        let e = p
            .stream
            .decode(&row(json!({"user_id": "a", "latency_ms": 5})))
            .unwrap();
        assert_eq!(
            serde_json::to_string(&p.stream.project(&e)).unwrap(),
            r#"{"u":"a","l":5,"latency_ms":5}"#
        );
        let mut h = row(json!({"u": "a", "l": 9, "latency_ms": 9}));
        assert_eq!(order.history_key(&mut h).unwrap(), [Value::Int(9)]);
    }

    #[test]
    fn resume_queries() {
        // ordered + limited, ordered by an unselected column: the snapshot's sort keys
        let f = BoundExpr::IsNull {
            expr: col(3),
            negated: true,
        };
        let mut b = ordered(&[1], Some(f), &[(2, true, false), (1, false, false)]);
        b.limit = Some(10);
        let p = plan(&b);
        assert_eq!(
            p.history.keys_sql("s").unwrap(),
            "SELECT latency_ms AS __order_0, user_id FROM s WHERE coupon IS NOT NULL \
             ORDER BY latency_ms DESC NULLS LAST, user_id ASC NULLS LAST LIMIT 10"
        );
        // LIMIT alone: the snapshot's row count, at most the LIMIT
        let mut b = bound(&[1], None);
        b.limit = Some(3);
        let p = plan(&b);
        assert!(p.history.keys_sql("s").is_none());
        assert_eq!(
            p.history.count_sql("s"),
            "SELECT count() AS n FROM (SELECT 1 FROM s LIMIT 3)"
        );
    }

    #[test]
    fn cursor_tokens() {
        use crate::cursor;
        let p = plan(&bound(&[1], None));
        let at = Watermark(std::collections::BTreeMap::from([(0, 5), (2, -1)]));
        let token = cursor::encode(&p, &at);
        assert_eq!(cursor::decode(&p, &token), Some(at.clone()));
        // another query (another plan) can't use it
        let other = plan(&bound(&[0, 1], None));
        assert_ne!(cursor::fingerprint(&p), cursor::fingerprint(&other));
        assert_eq!(cursor::decode(&other, &token), None);
        assert_eq!(cursor::decode(&p, "garbage"), None);
        assert_eq!(
            cursor::decode(&p, &token.replace("\"v\":1", "\"v\":2")),
            None
        );
        // stable across runs: a hash of the plan, not of memory addresses
        assert_eq!(
            cursor::fingerprint(&p),
            cursor::fingerprint(&plan(&bound(&[1], None)))
        );
    }
}
