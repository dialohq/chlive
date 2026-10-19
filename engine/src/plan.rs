//! From a bound query to what to run: the ClickHouse query for history, and for the stream:
//! which topics to consume, which keys of their events to decode (and as which types), which
//! events are relevant, how to turn them into result rows and, with ORDER BY, where they go.

use std::cmp::Ordering;

use crate::ast::CmpOp;
use crate::binder::{
    AggExpr, AggFunc, Aggregation, BoundColumn, BoundExpr, BoundQuery, BoundTable, ColumnId,
    OrderKey, TimeWindow,
};
use crate::calendar::Interval;
use crate::diff::Row;
use crate::order::{KeyOrder, SortKey};
use crate::rules::{quote_ident, TopicId};
use crate::table_engine::TableEngine;
use crate::types::quote_string;
use crate::types::{decode, DecodeError, Type, Value};

#[derive(Debug, Clone, PartialEq)]
pub struct Plan {
    pub table: String,
    /// What history snapshots are taken of: the ClickHouse table and its topic.
    pub source: crate::snapshot::SnapshotTable,
    /// The table's engine; a ReplacingMergeTree is read without FINAL (every version).
    pub engine: TableEngine,
    /// Output columns, in output order.
    pub columns: Vec<BoundColumn>,
    pub history: HistoryPlan,
    pub stream: StreamPlan,
    /// With GROUP BY or aggregates: how chDB keeps and merges the per-group states.
    pub aggregate: Option<AggregatePlan>,
    /// With `key IN (SELECT …)`: the sub-query, and how its keys are found and followed.
    pub semi: Option<SemiPlan>,
}

/// `outer_key IN (SELECT inner_key FROM inner [WHERE …])`. The sub-query's keys (`S`) come from
/// a snapshot of the inner table, then from its topic; outer rows whose key isn't in `S` yet
/// are kept back until it is (see `engine::semi`).
#[derive(Debug, Clone, PartialEq)]
pub struct SemiPlan {
    /// The inner table's snapshots, and its topic.
    pub inner: crate::snapshot::SnapshotTable,
    pub inner_topic: TopicId,
    /// The inner table, quoted (`db.t`), for explain and the cursor.
    pub inner_table: String,
    pub inner_engine: TableEngine,
    /// Decodes the inner topic's events: the key (if any) and the filter's columns.
    pub inner_stream: StreamPlan,
    /// The inner key, in `inner_stream`'s decoded rows; `None`: an uncorrelated EXISTS, where
    /// every row has the same (constant) key.
    pub inner_key: Option<ColumnId>,
    /// The outer key, in the outer stream's decoded rows (`None` likewise).
    pub outer_key: Option<ColumnId>,
    /// The inner key's type, to read the key set from ClickHouse.
    pub inner_key_ty: Option<Type>,
    /// Keys compare as numbers when either side is a float (`5` = `5.0`).
    pub float_keys: bool,
    /// (outer, inner) key columns, quoted.
    keys_sql: Option<(String, String)>,
    inner_filter_sql: Option<String>,
}

impl SemiPlan {
    /// The outer rows' condition, over a snapshot of the inner table (`inner_from`).
    pub fn condition(&self, inner_from: &str) -> String {
        match &self.keys_sql {
            Some((outer, inner)) => format!(
                "{outer} IN (SELECT {inner} FROM {inner_from}{})",
                self.inner_where()
            ),
            None => format!("EXISTS (SELECT 1 FROM {inner_from}{})", self.inner_where()),
        }
    }

    /// The sub-query's keys in a snapshot of the inner table, as `k`; uncorrelated, a row if
    /// it has one.
    pub fn keys_sql(&self, inner_from: &str) -> String {
        match &self.keys_sql {
            Some((_, inner)) => format!(
                "SELECT DISTINCT {inner} AS k FROM {inner_from}{}",
                self.inner_where()
            ),
            None => format!(
                "SELECT 1 AS k FROM {inner_from}{} LIMIT 1",
                self.inner_where()
            ),
        }
    }

    /// Back-fill: the outer rows of these keys (rendered literals); uncorrelated, `None`: all
    /// of them.
    pub fn keys_condition(&self, literals: &[String]) -> Option<String> {
        let (outer, _) = self.keys_sql.as_ref()?;
        Some(format!("{outer} IN ({})", literals.join(", ")))
    }

    fn inner_where(&self) -> String {
        match &self.inner_filter_sql {
            Some(f) => format!(" WHERE {f}"),
            None => String::new(),
        }
    }
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
    /// OFFSET: how many groups (by `order`, or the first seen) come before the result.
    pub offset: Option<u64>,
    /// Columns computed only for ORDER BY (`__o0`, …), not output.
    pub hidden: Vec<String>,
    /// When every aggregate has a native implementation: how to run without chDB.
    pub native: Option<NativeAggregates>,
    /// `now()` in the WHERE: the comparisons' `now()` sides, and the WHERE of what's fetched
    /// (always native, per row; see [`HistoryPlan::possible`]).
    pub windows: Vec<HistoryWindow>,
    possible: Option<String>,
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
    /// FINAL: the history is every row, not states (see [`PerRow`]).
    pub per_row: Option<PerRow>,
}

/// FINAL's aggregates keep each row's values, to take them back out when a newer version
/// replaces the row (or deletes it, or it stops matching). The history reads every row: its
/// key and version (`__key_<i>`, `__ver`, as [`KeyedPlan`] reads them), its group (`__k<i>`)
/// and each aggregate's argument (`__arg_<j>`).
#[derive(Debug, Clone, PartialEq)]
pub struct PerRow {
    select: String,
    /// Read with FINAL (a ReplacingMergeTree's latest versions), or every row (time windows).
    pub final_: bool,
    /// Each aggregate's argument field in history rows, with its type (`count()`: none).
    pub args: Vec<Option<(String, Type)>>,
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
            self.offset,
            self.hidden.clone(),
        )
    }

    /// Native: the history's per-group states from main ClickHouse, read from `from` (a
    /// snapshot of the table); `now()` as itself (for the identity and explain).
    pub fn native_history_sql(&self, from: &str) -> Option<String> {
        self.native_history(from, None)
    }

    /// [`Self::native_history_sql`] at `now` (nanoseconds since the epoch): what's fetched.
    pub fn native_history_sql_at(&self, from: &str, now: i128) -> Option<String> {
        self.native_history(from, Some(now))
    }

    fn native_history(&self, from: &str, now: Option<i128>) -> Option<String> {
        let native = self.native.as_ref()?;
        if let Some(rows) = &native.per_row {
            let template = match now {
                Some(_) => self.possible.as_ref().or(self.filter.as_ref()),
                None => self.filter.as_ref(),
            };
            let where_ = template
                .map(|t| format!(" WHERE {}", render(t, &self.windows, now)))
                .unwrap_or_default();
            return Some(format!(
                "SELECT {} FROM {from}{}{where_}",
                rows.select,
                if rows.final_ { " FINAL" } else { "" },
            ));
        }
        Some(format!(
            "SELECT {} FROM {from}{}{}",
            native.select,
            self.where_clause(),
            self.group_by_clause()
        ))
    }

    /// The history's per-group states, as main ClickHouse computes them from `from` (a
    /// snapshot of the table): what chDB's table starts with.
    pub fn states_sql(&self, from: &str) -> String {
        format!(
            "SELECT {} FROM {from}{}{}",
            self.state_select,
            self.where_clause(),
            self.group_by_clause()
        )
    }

    /// Create the chDB table with the history's states, read from `source` (see
    /// [`AggregatePlan::http_source`]).
    pub fn create_sql(&self, table: &str, source: &str) -> String {
        let order = if self.keys == 0 {
            "tuple()".to_string()
        } else {
            format!("({})", self.group_by)
        };
        format!(
            "CREATE TABLE {table} ENGINE = AggregatingMergeTree ORDER BY {order} \
             SETTINGS allow_nullable_key = 1 AS SELECT * FROM {source}"
        )
    }

    /// chDB's `url()` running `sql` on main ClickHouse over HTTP. The result comes as
    /// RowBinaryWithNamesAndTypes, which carries aggregate states as they are and, unlike Native,
    /// names its columns and types even when there are no rows (no history), so chDB always
    /// knows the table's structure. The credentials go in headers (so they're in no URL or query
    /// log).
    pub fn http_source(http_url: &str, sql: &str, user: &str, password: &str) -> String {
        let mut url = reqwest::Url::parse(http_url)
            .unwrap_or_else(|_| reqwest::Url::parse("http://invalid/").expect("a URL"));
        url.query_pairs_mut()
            .append_pair("query", &format!("{sql} FORMAT RowBinaryWithNamesAndTypes"));
        format!(
            "url({}, 'RowBinaryWithNamesAndTypes', headers('X-ClickHouse-User' = {}, 'X-ClickHouse-Key' = {}))",
            quote_string(url.as_str()),
            quote_string(user),
            quote_string(password)
        )
    }

    fn where_clause(&self) -> String {
        match &self.filter {
            Some(f) => format!(" WHERE {}", render(f, &self.windows, None)),
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
fn native_aggregates(
    agg: &Aggregation,
    table: &BoundTable,
    windows: &[TimeWindow],
    where_columns: &[ColumnId],
) -> Option<NativeAggregates> {
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
    // FINAL or time windows: every row's values are kept, to take them back out
    let per_row = (table.final_key.is_some() || !windows.is_empty()).then(|| {
        let mut select: Vec<String> = Vec::new();
        if let Some(k) = &table.final_key {
            for (i, id) in k.key.iter().enumerate() {
                select.push(format!("{} AS __key_{i}", col(*id)));
            }
            if let Some(v) = k.ver {
                select.push(format!("{} AS __ver", col(v)));
            }
        }
        for (i, id) in agg.keys.iter().enumerate() {
            select.push(format!("{} AS __k{i}", col(*id)));
        }
        let args = aggs
            .iter()
            .enumerate()
            .map(|(j, a)| {
                a.arg.map(|id| {
                    select.push(format!("{} AS __arg_{j}", col(id)));
                    (format!("__arg_{j}"), table.column(id).ty.clone())
                })
            })
            .collect();
        // with now(): the WHERE's columns, for the row's schedule (as
        // `StreamPlan::history_fields`)
        if !windows.is_empty() {
            for id in where_columns {
                select.push(format!("{} AS {}", col(*id), hidden_column(*id)));
            }
        }
        PerRow {
            select: select.join(", "),
            final_: table.final_key.is_some(),
            args,
        }
    });
    Some(NativeAggregates {
        keys: agg.keys.clone(),
        aggs,
        items,
        select: select.join(", "),
        per_row,
    })
}

fn aggregate_plan(
    agg: &Aggregation,
    table: &BoundTable,
    where_: &Where,
    windows: &[TimeWindow],
    where_columns: &[ColumnId],
) -> AggregatePlan {
    let filter = where_.exact.clone();
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
    let native = native_aggregates(agg, table, windows, where_columns);
    AggregatePlan {
        native,
        windows: history_windows(windows, table),
        possible: where_.possible.clone(),
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
        offset: agg.offset,
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
    /// The WHERE condition, rendered for ClickHouse; its `now()` comparisons as markers (see
    /// [`render`]).
    pub filter: Option<String>,
    /// With `now()`: the WHERE of what's fetched, every row that's in now or can come in later
    /// (see [`possible`]).
    pub possible: Option<String>,
    /// `ORDER BY …`, with every key's direction and NULL placement explicit.
    pub order: Option<String>,
    pub limit: Option<u64>,
    /// With ORDER BY: the sort columns, the keys a resume rebuilds the order from.
    pub keys_select: Option<String>,
    /// `FROM … FINAL`: a ReplacingMergeTree's latest version of each row.
    pub final_: bool,
    /// The `now()` sides of the WHERE's comparisons.
    pub windows: Vec<HistoryWindow>,
}

/// The `now()` side of a comparison in the history query: `now(['tz']) ± INTERVAL …`.
#[derive(Debug, Clone, PartialEq)]
pub struct HistoryWindow {
    /// The column it's compared with, quoted.
    pub column: String,
    pub tz: Option<chrono_tz::Tz>,
    pub intervals: Vec<Interval>,
}

/// Marks a `now()` comparison in a rendered condition: the window and the operator, filled in
/// by [`render`].
const NOW_MARK: char = '\u{1}';

/// A condition with its `now()` comparisons filled in: at `now` (nanoseconds since the epoch)
/// as literals, otherwise as `now()` itself (for the plan's identity and explain, which
/// mustn't change as time passes).
fn render(template: &str, windows: &[HistoryWindow], now: Option<i128>) -> String {
    let mut out = String::new();
    let mut parts = template.split(NOW_MARK);
    out.push_str(parts.next().unwrap_or(""));
    while let (Some(mark), Some(rest)) = (parts.next(), parts.next()) {
        let (w, op) = mark.split_once(':').expect("a window mark");
        let w = &windows[w.parse::<usize>().expect("a window index")];
        out.push_str(&w.sql(op, now));
        out.push_str(rest);
    }
    out
}

impl HistoryWindow {
    /// `column op now(…) ± INTERVAL …`, at `now` a literal. The intervals stay as written, so
    /// ClickHouse adds DAY and WEEK by the calendar in the timezone.
    fn sql(&self, op: &str, now: Option<i128>) -> String {
        let tz = self.tz.map(|tz| quote_string(tz.name()));
        let now = match (now, tz) {
            (Some(n), Some(tz)) => format!("fromUnixTimestamp64Nano(toInt64({n}), {tz})"),
            (Some(n), None) => format!("fromUnixTimestamp64Nano(toInt64({n}))"),
            (None, Some(tz)) => format!("now64(9, {tz})"),
            (None, None) => "now64(9)".into(),
        };
        format!(
            "{} {op} {now}{}",
            self.column,
            intervals_sql(&self.intervals)
        )
    }
}

/// ` - INTERVAL 1 DAY + INTERVAL 2 HOUR`, as written.
fn intervals_sql(intervals: &[Interval]) -> String {
    intervals
        .iter()
        .map(|i| {
            format!(
                " {} INTERVAL {} {}",
                if i.amount < 0 { '-' } else { '+' },
                i.amount.unsigned_abs(),
                i.unit.name()
            )
        })
        .collect()
}

impl HistoryPlan {
    /// The query, reading `from` (a snapshot of [`Self::table`]), with `now()` as itself: for
    /// the plan's identity and explain (see [`Self::sql_at`] for what's fetched).
    pub fn sql(&self, from: &str) -> String {
        self.build(&self.select, from, self.order.as_deref(), self.limit, None)
    }

    /// The query at `now` (nanoseconds since the epoch): the time windows as literals.
    pub fn sql_at(&self, from: &str, now: i128) -> String {
        self.build(
            &self.select,
            from,
            self.order.as_deref(),
            self.limit,
            Some(now),
        )
    }

    /// The WHERE: as written (`now()` symbolic), or at `now` what's fetched.
    fn where_sql(&self, now: Option<i128>) -> Option<String> {
        let template = match now {
            Some(_) => self.possible.as_ref().or(self.filter.as_ref()),
            None => self.filter.as_ref(),
        };
        template.map(|t| render(t, &self.windows, now))
    }

    /// The query with one more condition (a semi-join's, or a back-fill's keys).
    pub fn sql_where(&self, from: &str, extra: &str) -> String {
        let mut sql = format!("SELECT {} FROM {from}{}", self.select, self.final_sql());
        match self.where_sql(None) {
            Some(f) => sql.push_str(&format!(" WHERE ({f}) AND ({extra})")),
            None => sql.push_str(&format!(" WHERE {extra}")),
        }
        if let Some(order) = &self.order {
            sql.push(' ');
            sql.push_str(order);
        }
        if let Some(limit) = self.limit {
            sql.push_str(&format!(" LIMIT {limit}"));
        }
        sql
    }

    /// The query with another LIMIT (a FINAL window's refill).
    pub fn sql_limit(&self, from: &str, limit: u64) -> String {
        self.build(&self.select, from, self.order.as_deref(), Some(limit), None)
    }

    /// Resume: the sort keys of the result (with ORDER BY), in order.
    pub fn keys_sql(&self, from: &str) -> Option<String> {
        let select = self.keys_select.as_ref()?;
        Some(self.build(select, from, self.order.as_deref(), self.limit, None))
    }

    /// Resume: how many rows the result has, at most the LIMIT.
    pub fn count_sql(&self, from: &str) -> String {
        let inner = self.build("1", from, None, self.limit, None);
        format!("SELECT count() AS n FROM ({inner})")
    }

    fn final_sql(&self) -> &'static str {
        if self.final_ {
            " FINAL"
        } else {
            ""
        }
    }

    fn build(
        &self,
        select: &str,
        from: &str,
        order: Option<&str>,
        limit: Option<u64>,
        now: Option<i128>,
    ) -> String {
        let mut sql = format!("SELECT {select} FROM {from}{}", self.final_sql());
        if let Some(f) = self.where_sql(now) {
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
    /// OFFSET: with ORDER BY, the result starts after the top M; without, the newest M rows
    /// are held back.
    pub offset: Option<u64>,
    /// FINAL on a ReplacingMergeTree: rows have ids (their keys), and events replace them.
    pub keyed: Option<KeyedPlan>,
    /// The `now()` sides of the WHERE's comparisons: rows go in and out as time passes.
    pub windows: Vec<WindowPlan>,
    /// With `now()`: the columns a history row carries to rebuild its decoded row (for its
    /// schedule, and to come in later): each one's field, and whether it's hidden
    /// (`__col_<id>`, removed before output).
    pub history_fields: Vec<(ColumnId, String, bool)>,
}

/// A row's state over time under a WHERE with `now()`: whether it's in now, then each instant
/// it goes in (`true`) or out, in order.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct Schedule {
    pub in_now: bool,
    pub transitions: Vec<(i128, bool)>,
}

/// The `now()` side of a comparison in the WHERE, for the stream: `column op now(['tz']) ±
/// INTERVAL …` flips once for a row, at its threshold ([`WindowPlan::threshold`]).
#[derive(Debug, Clone, PartialEq)]
pub struct WindowPlan {
    pub column: ColumnId,
    pub ty: Type,
    pub intervals: Vec<Interval>,
    /// The query's timezone (`now('…')`), and the one DAY and WEEK go by: the query's, or the
    /// server's (set by the engine; UTC until then).
    pub tz: Option<chrono_tz::Tz>,
    pub zone: chrono_tz::Tz,
    /// `now(…) ± INTERVAL …` in dialect syntax, for `explain`.
    pub text: String,
}

impl WindowPlan {
    /// When `value op now() …` flips (nanoseconds since the epoch), and how: `>`/`>=` are true
    /// until then (the row leaves), `<`/`<=` from then on (it comes in). `None` for NULL: never
    /// true.
    pub fn threshold(&self, op: CmpOp, value: &Value) -> Option<(i128, bool)> {
        let Value::DateTime(dt) = value else {
            return None;
        };
        let ns = i128::from(dt.ticks) * 10i128.pow(9 - u32::from(dt.precision.min(9)));
        let at = |inclusive| crate::calendar::deadline(ns, &self.intervals, self.zone, inclusive);
        Some(match op {
            CmpOp::Gt => (at(false), false),
            CmpOp::GtEq => (at(true), false),
            // `v < now…` once `now…` > v, the first instant `v >= now…` stops holding
            CmpOp::Lt => (at(true), true),
            CmpOp::LtEq => (at(false), true),
            CmpOp::Eq | CmpOp::NotEq => unreachable!("the binder refuses = and != with now()"),
        })
    }
}

/// `now(['tz']) ± INTERVAL …` in dialect syntax, as written.
fn now_text(tz: Option<chrono_tz::Tz>, intervals: &[Interval]) -> String {
    let tz = tz.map(|tz| quote_string(tz.name())).unwrap_or_default();
    format!("now({tz}){}", intervals_sql(intervals))
}

/// FINAL for the stream: what identifies a row, which version it is, and whether it's deleted;
/// and where history rows carry the key and version (hidden fields, removed before output).
#[derive(Debug, Clone, PartialEq)]
pub struct KeyedPlan {
    pub key: Vec<KeyedColumn>,
    pub ver: Option<KeyedColumn>,
    pub deleted: Option<ColumnId>,
    /// The key's column names, for `explain`.
    pub text: String,
    /// `LIMIT n` (no ORDER BY): the n most recently active rows, and a buffer of more.
    pub window: Option<KeyedWindowPlan>,
}

/// FINAL with `LIMIT n` and/or `OFFSET m`: rows `[m, m+n)` of a kept sequence (the newest first,
/// or ranked by ORDER BY), the first m held back and `buffer` more after them. The history
/// reads `m + n + buffer` rows (all of them without LIMIT). A refill reads `m + n + 2·buffer`
/// (candidates for the buffer), or with ORDER BY `m + n + buffer` (a fresh top, rebuilt).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct KeyedWindowPlan {
    /// `None`: no LIMIT, every row after the first m.
    pub limit: Option<u64>,
    pub offset: u64,
    pub buffer: u64,
    /// With ORDER BY: ranked, not newest first.
    pub ordered: bool,
}

impl KeyedWindowPlan {
    /// How many rows the history reads (`None`: all of them).
    pub fn history_limit(&self) -> Option<u64> {
        match self.limit {
            Some(0) => Some(0),
            Some(n) => Some(self.offset + n + self.buffer),
            None => None,
        }
    }

    /// How many rows a refill reads from a snapshot.
    pub fn refill_limit(&self) -> u64 {
        let n = self.offset + self.limit.unwrap_or(0);
        if self.ordered {
            n + self.buffer
        } else {
            n + 2 * self.buffer
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct KeyedColumn {
    pub id: ColumnId,
    pub ty: Type,
    /// Its hidden field in history rows (`__key_<i>`, `__ver`).
    pub history_field: String,
}

impl KeyedPlan {
    /// A history row's id and version (as ClickHouse rendered them), removing the hidden
    /// fields. FINAL already left out deleted rows.
    pub fn history_row(&self, row: &mut Row) -> Result<(String, Value), RowDecodeError> {
        let mut take = |c: &KeyedColumn| {
            let v = decode(row.get(&c.history_field), &c.ty).map_err(|error| RowDecodeError {
                column: c.history_field.clone(),
                error,
            });
            row.shift_remove(&c.history_field);
            v
        };
        let key = self
            .key
            .iter()
            .map(&mut take)
            .collect::<Result<Vec<_>, _>>()?;
        let ver = self.ver.as_ref().map(take).transpose()?;
        Ok((id_of(&key), ver.unwrap_or(Value::Null)))
    }

    /// A decoded event's id, version (`Null` without a version column) and whether it deletes
    /// its row.
    pub fn event(&self, row: &DecodedRow) -> (String, Value, bool) {
        let key: Vec<Value> = self.key.iter().map(|c| row[c.id.0].clone()).collect();
        let ver = self
            .ver
            .as_ref()
            .map_or(Value::Null, |c| row[c.id.0].clone());
        // is_deleted is a UInt8: 1 deletes
        let deleted = self
            .deleted
            .is_some_and(|d| matches!(row[d.0], Value::Int(n) if n != 0));
        (id_of(&key), ver, deleted)
    }
}

/// A row's id: its key values as a JSON array, as ClickHouse renders them (`["p1"]`).
fn id_of(key: &[Value]) -> String {
    crate::aggregate::result::group_id(&key.iter().map(Value::to_json).collect::<Vec<_>>())
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

    /// Whether a row is in the result (with `now()`: at the instant `now`).
    pub fn matches(&self, row: &DecodedRow) -> bool {
        match &self.filter {
            Filter::All => true,
            Filter::Where { expr, .. } => eval(expr, row, &|_, _| None) == Some(true),
        }
    }

    /// Whether the WHERE has `now()` in it: rows go in and out as time passes.
    pub fn timed(&self) -> bool {
        !self.windows.is_empty()
    }

    /// A row's state from `now` on: in or out now, and each instant it flips. Its `now()`
    /// comparisons flip once each (at their thresholds), so the WHERE can only change there:
    /// it's evaluated now and at each later threshold.
    pub fn schedule(&self, row: &DecodedRow, now: i128) -> Schedule {
        let Filter::Where { expr, .. } = &self.filter else {
            return Schedule {
                in_now: true,
                transitions: Vec::new(),
            };
        };
        let threshold =
            |w: usize, op: CmpOp, column: ColumnId| self.windows[w].threshold(op, &row[column.0]);
        let at = |t: i128| {
            let cmp = |w: usize, op: CmpOp, column: ColumnId| {
                threshold(w, op, column).map(|(d, comes_in)| (t >= d) == comes_in)
            };
            eval(expr, row, &|w, (op, column)| cmp(w, op, column)) == Some(true)
        };
        let mut instants = Vec::new();
        expr.visit_now(&mut |w, op, column| {
            if let Some((d, _)) = threshold(w, op, column) {
                if d > now {
                    instants.push(d);
                }
            }
        });
        instants.sort_unstable();
        instants.dedup();
        let in_now = at(now);
        let mut state = in_now;
        let mut transitions = Vec::new();
        for d in instants {
            let s = at(d);
            if s != state {
                transitions.push((d, s));
                state = s;
            }
        }
        Schedule {
            in_now,
            transitions,
        }
    }

    /// A history row decoded (with `now()`; see [`Self::history_fields`]), removing its hidden
    /// fields.
    pub fn history_decode(&self, row: &mut Row) -> Result<DecodedRow, RowDecodeError> {
        let mut decoded = vec![Value::Null; self.width];
        for (id, field, hidden) in &self.history_fields {
            let ty = &self
                .columns
                .iter()
                .find(|c| c.id == *id)
                .expect("a stream column")
                .ty;
            decoded[id.0] = decode(row.get(field), ty).map_err(|error| RowDecodeError {
                column: field.clone(),
                error,
            })?;
            if *hidden {
                row.shift_remove(field);
            }
        }
        Ok(decoded)
    }

    /// The result row, rendered the way ClickHouse renders it.
    pub fn project(&self, row: &DecodedRow) -> Row {
        self.output
            .iter()
            .map(|(name, id)| (name.clone(), row[id.0].to_json()))
            .collect()
    }
}

/// A WHERE for ClickHouse: as written, and with `now()`, what's fetched (see [`possible`]).
struct Where {
    exact: Option<String>,
    possible: Option<String>,
}

/// The rows that are in now or can come in later, a superset: the condition with its NOTs
/// pushed down to the comparisons, each `now()` comparison a row leaves by (`>`, `>=`) as it is
/// (at the fetch's now it's true if it can be later), and each one a row comes in by (`<`,
/// `<=`) true unless NULL (it will be). The rest is monotone, so whatever can be true later is
/// true of this now.
fn possible(expr: &BoundExpr, negate: bool) -> BoundExpr {
    let both = |l: &BoundExpr, r: &BoundExpr| {
        (Box::new(possible(l, negate)), Box::new(possible(r, negate)))
    };
    match expr {
        BoundExpr::And(l, r) => {
            let (l, r) = both(l, r);
            if negate {
                BoundExpr::Or(l, r)
            } else {
                BoundExpr::And(l, r)
            }
        }
        BoundExpr::Or(l, r) => {
            let (l, r) = both(l, r);
            if negate {
                BoundExpr::And(l, r)
            } else {
                BoundExpr::Or(l, r)
            }
        }
        BoundExpr::Not(e) => possible(e, !negate),
        BoundExpr::Now { window, column, op } => {
            let op = if negate {
                match op {
                    CmpOp::Gt => CmpOp::LtEq,
                    CmpOp::GtEq => CmpOp::Lt,
                    CmpOp::Lt => CmpOp::GtEq,
                    CmpOp::LtEq => CmpOp::Gt,
                    other => *other,
                }
            } else {
                *op
            };
            match op {
                CmpOp::Gt | CmpOp::GtEq => BoundExpr::Now {
                    window: *window,
                    column: *column,
                    op,
                },
                _ => BoundExpr::IsNull {
                    expr: Box::new(BoundExpr::Column(*column)),
                    negated: true,
                },
            }
        }
        other if negate => BoundExpr::Not(Box::new(other.clone())),
        other => other.clone(),
    }
}

/// The `now()` sides of the comparisons, for the history query.
fn history_windows(windows: &[TimeWindow], table: &BoundTable) -> Vec<HistoryWindow> {
    windows
        .iter()
        .map(|w| HistoryWindow {
            column: quote_ident(&table.column(w.column).clickhouse),
            tz: w.tz,
            intervals: w.intervals.clone(),
        })
        .collect()
}

/// The name a column is fetched under in a history row with `now()`, when it isn't output.
fn hidden_column(id: ColumnId) -> String {
    format!("__col_{}", id.0)
}

/// SQL three-valued logic: `None` is unknown (NULL). `now` gives each `now()` comparison's
/// value (by window, operator and column).
fn eval(
    expr: &BoundExpr,
    row: &DecodedRow,
    now: &dyn Fn(usize, (CmpOp, ColumnId)) -> Option<bool>,
) -> Option<bool> {
    match expr {
        BoundExpr::Now { window, column, op } => now(*window, (*op, *column)),
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
        BoundExpr::And(l, r) => match (eval(l, row, now), eval(r, row, now)) {
            (Some(false), _) | (_, Some(false)) => Some(false),
            (Some(true), Some(true)) => Some(true),
            _ => None,
        },
        BoundExpr::Or(l, r) => match (eval(l, row, now), eval(r, row, now)) {
            (Some(true), _) | (_, Some(true)) => Some(true),
            (Some(false), Some(false)) => Some(false),
            _ => None,
        },
        BoundExpr::Not(e) => eval(e, row, now).map(|b| !b),
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
        BoundExpr::Now { window, op, .. } => {
            format!("{NOW_MARK}{window}:{}{NOW_MARK}", op.symbol())
        }
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
fn to_dialect(expr: &BoundExpr, table: &BoundTable, windows: &[TimeWindow]) -> String {
    let lit = |v: &Value| match v {
        Value::Null => "NULL".to_string(),
        other => other.to_json().to_string().replace('"', "'"),
    };
    let to_dialect = |e: &BoundExpr, t: &BoundTable| to_dialect(e, t, windows);
    match expr {
        BoundExpr::Now { window, column, op } => format!(
            "{} {} {}",
            table.column(*column).name,
            op.symbol(),
            now_text(windows[*window].tz, &windows[*window].intervals)
        ),
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

/// What explain says about a table's engine: nothing for a MergeTree.
fn engine_note(engine: &TableEngine, keyed: Option<&KeyedPlan>) -> Option<String> {
    match (engine, keyed) {
        (TableEngine::Replacing { .. }, Some(k)) => {
            let mut note = format!(
                "{engine}, FINAL: one row per {}, its latest version; deleted rows leave",
                k.text
            );
            match k.window {
                Some(w) if w.offset > 0 && w.limit.is_none() => note.push_str(&format!(
                    "; OFFSET {}: every row but the first {} ({}), all of them kept",
                    w.offset,
                    w.offset,
                    if w.ordered {
                        "by ORDER BY"
                    } else {
                        "the newest"
                    }
                )),
                Some(w) => {
                    let n = w.limit.unwrap_or(0);
                    if w.ordered {
                        note.push_str(&format!(
                            "; LIMIT {n}: the top {n}, and the next {} kept to replace \
                             departures, refilled by a rebuild from a newer snapshot",
                            w.buffer
                        ));
                    } else {
                        note.push_str(&format!(
                            "; LIMIT {n}: the most recently active rows, newest first, with a \
                             buffer of {} more to refill departures",
                            w.buffer
                        ));
                    }
                    if w.offset > 0 {
                        note.push_str(&format!(
                            "; OFFSET {}: the first {} held back",
                            w.offset, w.offset
                        ));
                    }
                }
                None => {}
            }
            Some(note)
        }
        (TableEngine::Replacing { .. }, None) => Some(format!(
            "{engine}, read without FINAL: every version of a row"
        )),
        _ => None,
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
        if let Some(note) = engine_note(&self.engine, self.stream.keyed.as_ref()) {
            writeln!(f, "engine:  {note}")?;
        }
        writeln!(f, "columns: {}", columns.join(", "))?;
        if let Some(a) = &self.aggregate {
            let names: Vec<String> = a.columns.iter().map(|(n, t)| format!("{n} {t}")).collect();
            writeln!(f, "output:  {}", names.join(", "))?;
            writeln!(
                f,
                "history: {} (in ClickHouse; chDB reads the states over HTTP)",
                a.states_sql("<snapshot>")
            )?;
            writeln!(f, "merge:   {}", a.snapshot_sql("<chdb table>"))?;
            match a.native_history_sql("<snapshot>") {
                Some(sql) if a.native.as_ref().is_some_and(|n| n.per_row.is_some()) => writeln!(
                    f,
                    "native:  {sql} (every row's values in the engine, taken back out when \
                     a version replaces the row or, with now(), it goes out; chDB unused)"
                )?,
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
            if let Some(m) = a.offset {
                writeln!(f, "offset:  {m} groups come before the result")?;
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
        if let Some(s) = &self.semi {
            writeln!(
                f,
                "semi:    {} (history over a snapshot of {} too)",
                s.condition("<inner snapshot>"),
                s.inner_table
            )?;
            if let Some(note) = engine_note(&s.inner_engine, None) {
                writeln!(f, "         {}: {note}", s.inner_table)?;
            }
            match s.keys_condition(&["…".into()]) {
                Some(c) => writeln!(
                    f,
                    "         keys: {}, then from topic {}; a key's earlier rows come from the \
                     latest snapshot ({c}) and a tail of the events kept back since",
                    s.keys_sql("<inner snapshot>"),
                    s.inner_topic,
                )?,
                None => writeln!(
                    f,
                    "         uncorrelated: all rows once the sub-query has one ({}, then topic \
                     {}); until then rows are kept back",
                    s.keys_sql("<inner snapshot>"),
                    s.inner_topic,
                )?,
            }
        }
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
        if self.stream.timed() {
            write!(
                f,
                "\nnow():   rows go in and out as time passes, at their instants (within the \
                 server's now_coalescing)"
            )?;
        }
        if let Some(order) = &self.stream.order {
            let how = if self.stream.keyed.is_some() {
                "rows move as new versions change their values; a moved or new row goes after \
                 the rows it ties with"
            } else {
                "a new row goes after the last row it ties with"
            };
            write!(f, "\norder:   {} ({how})", order.text)?;
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
        match (self.stream.offset, &self.stream.order) {
            (Some(m), Some(_)) => write!(
                f,
                "\noffset:  {m} (the result starts after the top {m}, whose rows are held to \
                 slide in when a row lands above)"
            )?,
            (Some(m), None) => write!(
                f,
                "\noffset:  {m} (the newest {m} rows are held back; each new one moves the oldest \
                 of them into the window)"
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
    plan_with(bound, crate::keyed::BUFFER)
}

/// [`plan`], with a FINAL window's buffer of `final_buffer` rows.
pub fn plan_with(bound: &BoundQuery, final_buffer: u64) -> Plan {
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
    // FINAL: each row's key and version, under hidden aliases
    let keyed = table.final_key.as_ref().map(|k| {
        let mut hidden_field = |id: ColumnId, alias: String| {
            let c = table.column(id);
            select_list.push(format!("{} AS {alias}", quote_ident(&c.clickhouse)));
            KeyedColumn {
                id,
                ty: c.ty.clone(),
                history_field: alias,
            }
        };
        let key: Vec<KeyedColumn> = k
            .key
            .iter()
            .enumerate()
            .map(|(i, id)| hidden_field(*id, format!("__key_{i}")))
            .collect();
        let ver = k.ver.map(|id| hidden_field(id, "__ver".into()));
        KeyedPlan {
            window: (bound.limit.is_some() || bound.offset.is_some()).then(|| KeyedWindowPlan {
                limit: bound.limit,
                offset: bound.offset.unwrap_or(0),
                buffer: final_buffer,
                ordered: !bound.order.is_empty(),
            }),
            text: key
                .iter()
                .map(|c| table.column(c.id).name.clone())
                .collect::<Vec<_>>()
                .join(", "),
            key,
            ver,
            deleted: k.deleted,
        }
    });
    // now(): the WHERE's comparisons' `now()` sides
    let windows: Vec<WindowPlan> = bound
        .windows
        .iter()
        .map(|w| WindowPlan {
            column: w.column,
            ty: table.column(w.column).ty.clone(),
            intervals: w.intervals.clone(),
            tz: w.tz,
            zone: w.tz.unwrap_or(chrono_tz::Tz::UTC),
            text: now_text(w.tz, &w.intervals),
        })
        .collect();
    let where_ = Where {
        exact: bound.filter.as_ref().map(|f| to_clickhouse(f, table)),
        possible: bound
            .filter
            .as_ref()
            .filter(|_| !windows.is_empty())
            .map(|f| to_clickhouse(&possible(f, false), table)),
    };
    let mut where_columns = Vec::new();
    if let Some(f) = &bound.filter {
        f.columns(&mut where_columns);
    }
    // now(): history rows carry what rebuilds them (their schedules, and rows coming in later)
    let mut history_fields: Vec<(ColumnId, String, bool)> = Vec::new();
    if !windows.is_empty() {
        let mut cols: Vec<ColumnId> = bound.projection.iter().map(|p| p.column).collect();
        cols.extend(where_columns.iter().copied());
        cols.extend(bound.order.iter().map(|k| k.column));
        if let Some(k) = &table.final_key {
            cols.extend(k.key.iter().chain(&k.ver).chain(&k.deleted).copied());
        }
        for id in cols {
            if history_fields.iter().any(|(c, _, _)| *c == id) {
                continue;
            }
            let field = match bound.projection.iter().find(|p| p.column == id) {
                Some(p) if bound.aggregation.is_none() => (id, p.name.clone(), false),
                _ => {
                    let alias = hidden_column(id);
                    select_list.push(format!(
                        "{} AS {alias}",
                        quote_ident(&table.column(id).clickhouse)
                    ));
                    (id, alias, true)
                }
            };
            history_fields.push(field);
        }
        if bound.aggregation.is_some() {
            // aggregates read their own rows: the WHERE's columns, hidden
            history_fields = where_columns
                .iter()
                .map(|id| (*id, hidden_column(*id), true))
                .collect();
        }
    }
    let history = HistoryPlan {
        table: table.clickhouse_table.clone(),
        select: select_list.join(", "),
        filter: where_.exact.clone(),
        possible: where_.possible.clone(),
        order: order_sql,
        // with OFFSET the engine needs the rows above the window too: the first m + n
        limit: match keyed.as_ref().and_then(|k| k.window) {
            // FINAL: the held-back rows, the window and its buffer (LIMIT 0: nothing)
            Some(w) => w.history_limit(),
            None => bound.limit.map(|n| n + bound.offset.unwrap_or(0)),
        },
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
        final_: keyed.is_some(),
        windows: history_windows(&bound.windows, table),
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
    let aggregate = bound
        .aggregation
        .as_ref()
        .map(|a| aggregate_plan(a, table, &where_, &bound.windows, &where_columns));
    if let Some(a) = &aggregate {
        for (id, _) in &a.batch_columns {
            if !needed.contains(id) {
                needed.push(*id);
            }
        }
    }
    if let Some((outer_key, _)) = bound.semi_join.as_ref().and_then(|s| s.keys) {
        if !needed.contains(&outer_key) {
            needed.push(outer_key);
        }
    }
    for w in &bound.windows {
        if !needed.contains(&w.column) {
            needed.push(w.column);
        }
    }
    if let Some(k) = &table.final_key {
        for id in k.key.iter().chain(&k.ver).chain(&k.deleted) {
            if !needed.contains(id) {
                needed.push(*id);
            }
        }
    }
    let semi = bound.semi_join.as_ref().map(|s| semi_plan(s, table));
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
        width: table.width(),
        output: columns.iter().map(|c| (c.name.clone(), c.id)).collect(),
        filter: match &bound.filter {
            None => Filter::All,
            Some(expr) => Filter::Where {
                expr: expr.clone(),
                text: to_dialect(expr, table, &bound.windows),
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
        offset: bound.offset,
        keyed,
        windows,
        history_fields,
    };
    Plan {
        table: table.name.clone(),
        source: table.snapshot_table(),
        engine: table.engine.clone(),
        columns,
        history,
        stream,
        aggregate,
        semi,
    }
}

fn semi_plan(s: &crate::binder::SemiJoin, outer: &BoundTable) -> SemiPlan {
    let inner = &s.inner;
    let mut needed: Vec<ColumnId> = s.keys.iter().map(|(_, ik)| *ik).collect();
    if let Some(f) = &s.inner_filter {
        let mut cols = Vec::new();
        f.columns(&mut cols);
        for c in cols {
            if !needed.contains(&c) {
                needed.push(c);
            }
        }
    }
    let inner_stream = StreamPlan {
        topics: vec![inner.topic.clone()],
        columns: needed
            .iter()
            .map(|id| {
                let c = inner.column(*id);
                StreamColumn {
                    id: *id,
                    json_key: c.json_key.clone(),
                    ty: c.ty.clone(),
                }
            })
            .collect(),
        width: inner.width(),
        output: Vec::new(),
        filter: match &s.inner_filter {
            None => Filter::All,
            Some(expr) => Filter::Where {
                expr: expr.clone(),
                text: to_dialect(expr, inner, &[]),
            },
        },
        order: None,
        limit: None,
        offset: None,
        keyed: None,
        windows: Vec::new(),
        history_fields: Vec::new(),
    };
    let float = |t: &Type| matches!(t.kind, crate::types::Kind::Float(_));
    SemiPlan {
        inner: inner.snapshot_table(),
        inner_topic: inner.topic.clone(),
        inner_table: inner.clickhouse_table.clone(),
        inner_engine: inner.engine.clone(),
        inner_stream,
        inner_key: s.keys.map(|(_, ik)| ik),
        outer_key: s.keys.map(|(ok, _)| ok),
        inner_key_ty: s.keys.map(|(_, ik)| inner.column(ik).ty.clone()),
        float_keys: s
            .keys
            .is_some_and(|(ok, ik)| float(&inner.column(ik).ty) || float(&outer.column(ok).ty)),
        keys_sql: s.keys.map(|(ok, ik)| {
            (
                quote_ident(&outer.column(ok).clickhouse),
                quote_ident(&inner.column(ik).clickhouse),
            )
        }),
        inner_filter_sql: s.inner_filter.as_ref().map(|f| to_clickhouse(f, inner)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::binder::Projected;

    #[test]
    fn chdb_reads_states_over_http() {
        let sql = "SELECT country AS __k0, uniqExactState(user_id) AS __a0_0 \
                   FROM db.`__chlive_snap_events_1` WHERE path = 'a&b' GROUP BY __k0";
        let src = AggregatePlan::http_source("http://ch:8123", sql, "ro", "it's secret");
        // the query (with its FORMAT) is in the URL, encoded; the credentials are headers
        assert_eq!(
            src,
            "url('http://ch:8123/?query=SELECT+country+AS+__k0%2C+uniqExactState%28user_id%29+AS+__a0_0+\
             FROM+db.%60__chlive_snap_events_1%60+WHERE+path+%3D+%27a%26b%27+GROUP+BY+__k0+FORMAT+RowBinaryWithNamesAndTypes', \
             'RowBinaryWithNamesAndTypes', headers('X-ClickHouse-User' = 'ro', 'X-ClickHouse-Key' = 'it\\'s secret'))"
        );
        assert!(
            !src.contains("secret") || !src.split("headers").next().unwrap().contains("secret")
        );
        // HTTPS, a path, and parameters already in the URL are kept
        let src = AggregatePlan::http_source(
            "https://ch.example:8443/proxy?database=x",
            "SELECT 1",
            "u",
            "",
        );
        assert!(
            src.starts_with(
                "url('https://ch.example:8443/proxy?database=x&query=SELECT+1+FORMAT+RowBinaryWithNamesAndTypes'"
            ),
            "{src}"
        );
    }
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
            alias: None,
            clickhouse_table: "default.events".into(),
            database: "default".into(),
            clickhouse_name: "events".into(),
            topic: TopicId("events".into()),
            engine: TableEngine::MergeTree,
            internal: Vec::new(),
            final_key: None,
            columns: vec![
                col(0, "ts", "DateTime64(3)"),
                col(1, "user_id", "String"),
                col(2, "latency_ms", "UInt32"),
                col(3, "coupon", "Nullable(String)"),
                col(4, "session", "Nullable(UUID)"),
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
            offset: None,
            aggregation: None,
            semi_join: None,
            windows: Vec::new(),
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
    fn explain_says_a_replacing_merge_tree_is_read_without_final() {
        let explain = |engine| {
            let mut b = bound(&[0], None);
            b.table.engine = engine;
            plan(&b).to_string()
        };
        assert!(!explain(TableEngine::MergeTree).contains("engine:"));
        let replacing = TableEngine::Replacing {
            ver: Some("version".into()),
            is_deleted: None,
            key: crate::table_engine::Key::from_system("k", ""),
        };
        assert!(explain(replacing.clone()).contains(
            "engine:  ReplacingMergeTree(version), read without FINAL: every version of a row\n"
        ));
        // the same result either way
        let fp = |engine| {
            let mut b = bound(&[0], None);
            b.table.engine = engine;
            crate::cursor::fingerprint(&plan(&b))
        };
        assert_eq!(fp(replacing), fp(TableEngine::MergeTree));
    }

    #[test]
    fn final_reads_one_row_per_key() {
        let mut b = bound(&[0, 2], None);
        b.table.engine = TableEngine::Replacing {
            ver: Some("latency_ms".into()),
            is_deleted: None,
            key: crate::table_engine::Key::from_system("user_id", ""),
        };
        let without = crate::cursor::fingerprint(&plan(&b));
        b.table.final_key = Some(crate::binder::FinalKey {
            key: vec![ColumnId(1)],
            ver: Some(ColumnId(2)),
            deleted: None,
        });
        let p = plan(&b);
        assert_eq!(
            p.history.sql("db.snap"),
            "SELECT ts, latency_ms, user_id AS __key_0, latency_ms AS __ver FROM db.snap FINAL"
        );
        assert_eq!(p.stream.required_keys(), ["ts", "latency_ms", "user_id"]);
        assert!(p.to_string().contains(
            "engine:  ReplacingMergeTree(latency_ms), FINAL: one row per user_id, its latest \
             version; deleted rows leave\n"
        ));
        assert_ne!(crate::cursor::fingerprint(&p), without);
        let keyed = p.stream.keyed.as_ref().unwrap();
        let mut row =
            json!({"ts": "2026-10-17 00:00:00.000", "latency_ms": 5, "__key_0": "u1", "__ver": 5})
                .as_object()
                .unwrap()
                .clone();
        let (id, ver) = keyed.history_row(&mut row).unwrap();
        assert_eq!((id.as_str(), ver), ("[\"u1\"]", Value::Int(5)));
        assert_eq!(row.keys().collect::<Vec<_>>(), ["ts", "latency_ms"]);
        // an event's id is the same
        let mut decoded = vec![Value::Null; 5];
        decoded[1] = Value::String("u1".into());
        decoded[2] = Value::Int(6);
        assert_eq!(
            keyed.event(&decoded),
            ("[\"u1\"]".into(), Value::Int(6), false)
        );

        // ORDER BY an unselected column: its hidden field comes before the key's
        b.order = vec![OrderKey {
            column: ColumnId(3),
            desc: true,
            nulls_first: false,
        }];
        let p = plan(&b);
        assert_eq!(
            p.history.sql("db.snap"),
            "SELECT ts, latency_ms, coupon AS __order_0, user_id AS __key_0, latency_ms AS __ver \
             FROM db.snap FINAL ORDER BY coupon DESC NULLS LAST"
        );
        assert!(p.to_string().contains(
            "order:   coupon DESC NULLS LAST (rows move as new versions change their values"
        ));

        // LIMIT without ORDER BY: the window and its buffer; a refill reads twice the buffer
        b.order = Vec::new();
        b.limit = Some(3);
        let p = plan_with(&b, 100);
        assert_eq!(
            p.history.sql("db.snap"),
            "SELECT ts, latency_ms, user_id AS __key_0, latency_ms AS __ver FROM db.snap FINAL \
             LIMIT 103"
        );
        let w = p.stream.keyed.as_ref().unwrap().window.unwrap();
        assert_eq!(
            p.history.sql_limit("db.snap", w.refill_limit()),
            p.history.sql("db.snap").replace("LIMIT 103", "LIMIT 203")
        );
        assert!(p.to_string().contains(
            "LIMIT 3: the most recently active rows, newest first, with a buffer of 100 more"
        ));
        b.limit = Some(0);
        assert!(
            plan(&b).history.sql("x").ends_with(" LIMIT 0"),
            "LIMIT 0 reads nothing"
        );

        // ORDER BY and LIMIT: the top n and the next ranks; a rebuild reads the same top
        b.limit = Some(3);
        b.order = vec![OrderKey {
            column: ColumnId(0),
            desc: false,
            nulls_first: false,
        }];
        let p = plan_with(&b, 100);
        assert!(p
            .history
            .sql("db.snap")
            .ends_with("FINAL ORDER BY ts ASC NULLS LAST LIMIT 103"));
        let w = p.stream.keyed.as_ref().unwrap().window.unwrap();
        assert!(w.ordered);
        assert_eq!(w.refill_limit(), 103);
        assert!(p
            .to_string()
            .contains("LIMIT 3: the top 3, and the next 100 kept to replace departures"));

        // OFFSET: the held-back rows are read too; without LIMIT, the whole result
        b.offset = Some(2);
        let p = plan_with(&b, 100);
        assert!(p.history.sql("db.snap").ends_with(" LIMIT 105"));
        assert_eq!(
            p.stream
                .keyed
                .as_ref()
                .unwrap()
                .window
                .unwrap()
                .refill_limit(),
            105
        );
        assert!(p.to_string().contains("; OFFSET 2: the first 2 held back"));
        b.limit = None;
        let p = plan_with(&b, 100);
        assert!(p
            .history
            .sql("db.snap")
            .ends_with("FINAL ORDER BY ts ASC NULLS LAST"));
        assert!(p
            .to_string()
            .contains("OFFSET 2: every row but the first 2 (by ORDER BY), all of them kept"));
    }

    fn window(tz: Option<chrono_tz::Tz>, intervals: &[(i64, crate::calendar::Unit)]) -> TimeWindow {
        TimeWindow {
            column: ColumnId(0),
            tz,
            intervals: intervals
                .iter()
                .map(|(amount, unit)| crate::calendar::Interval {
                    amount: *amount,
                    unit: *unit,
                })
                .collect(),
            span: crate::error::Span::new(0, 0),
        }
    }

    fn now_cmp(window: usize, op: CmpOp) -> BoundExpr {
        BoundExpr::Now {
            window,
            column: ColumnId(0),
            op,
        }
    }

    fn history_row(v: serde_json::Value) -> Row {
        v.as_object().unwrap().clone()
    }

    #[test]
    fn now_is_symbolic_in_the_identity_and_literal_when_fetched() {
        use crate::calendar::Unit;
        const H: i128 = 3_600_000_000_000;
        let mut b = bound(&[1], Some(now_cmp(0, CmpOp::Gt)));
        b.windows = vec![window(None, &[(-1, Unit::Hour)])];
        let p = plan(&b);
        // ts isn't selected: fetched under a hidden alias, for the rows' schedules
        assert_eq!(
            p.history.sql("db.snap"),
            "SELECT user_id, ts AS __col_0 FROM db.snap WHERE ts > now64(9) - INTERVAL 1 HOUR"
        );
        let now: i128 = 1_800_000_000_000_000_000;
        assert_eq!(
            p.history.sql_at("db.snap", now),
            format!(
                "SELECT user_id, ts AS __col_0 FROM db.snap WHERE ts > \
                 fromUnixTimestamp64Nano(toInt64({now})) - INTERVAL 1 HOUR"
            )
        );
        assert!(p.stream.required_keys().contains(&"ts".to_string()));
        let text = p.to_string();
        assert!(
            text.contains("filter:  ts > now() - INTERVAL 1 HOUR"),
            "{text}"
        );
        assert!(
            text.contains("now():   rows go in and out as time passes"),
            "{text}"
        );
        // a row's schedule: in until ts + 1 hour; the hidden field is gone
        let mut row = history_row(json!({"user_id": "u", "__col_0": "2026-10-18 12:00:00.000"}));
        let decoded = p.stream.history_decode(&mut row).unwrap();
        assert_eq!(row.keys().collect::<Vec<_>>(), ["user_id"]);
        let ts = i128::from(crate::types::parse_datetime("2026-10-18 12:00:00.000", 3).unwrap())
            * 1_000_000;
        assert_eq!(
            p.stream.schedule(&decoded, ts),
            Schedule {
                in_now: true,
                transitions: vec![(ts + H, false)]
            }
        );

        // with a filter, and rows that come in: fetched if they're in now or can come in
        let f = BoundExpr::Or(
            Box::new(BoundExpr::And(
                Box::new(BoundExpr::Compare {
                    op: CmpOp::Gt,
                    left: col(2),
                    right: lit(Value::Int(5)),
                }),
                Box::new(now_cmp(0, CmpOp::GtEq)),
            )),
            Box::new(BoundExpr::Not(Box::new(BoundExpr::Or(
                Box::new(now_cmp(1, CmpOp::Lt)),
                Box::new(now_cmp(0, CmpOp::Gt)),
            )))),
        );
        let mut b2 = bound(&[0], Some(f));
        b2.windows = vec![
            window(None, &[(-1, Unit::Hour)]),
            window(None, &[(1, Unit::Day)]),
        ];
        let p2 = plan(&b2);
        assert!(p2.history.sql("x").ends_with(
            "WHERE ((latency_ms > 5) AND (ts >= now64(9) - INTERVAL 1 HOUR)) OR \
             (NOT ((ts < now64(9) + INTERVAL 1 DAY) OR (ts > now64(9) - INTERVAL 1 HOUR)))"
        ));
        // NOT pushed down: `ts >= now + 1 day` stays (a row can only leave it), `ts <= now -
        // 1 hour` will be true unless NULL
        assert!(p2.history.sql_at("x", now).ends_with(&format!(
            "WHERE ((latency_ms > 5) AND (ts >= fromUnixTimestamp64Nano(toInt64({now})) - \
             INTERVAL 1 HOUR)) OR ((ts >= fromUnixTimestamp64Nano(toInt64({now})) + INTERVAL 1 \
             DAY) AND (ts IS NOT NULL))"
        )));

        // a timezone: in the query (so in its identity), and ClickHouse adds DAYs by it
        let mut b3 = bound(&[1], Some(now_cmp(0, CmpOp::Gt)));
        b3.windows = vec![window(
            Some(chrono_tz::Europe::Warsaw),
            &[(-1, Unit::Day), (2, Unit::Hour)],
        )];
        let p3 = plan(&b3);
        assert!(p3
            .history
            .sql("x")
            .ends_with("WHERE ts > now64(9, 'Europe/Warsaw') - INTERVAL 1 DAY + INTERVAL 2 HOUR"));
        assert!(p3.history.sql_at("x", now).ends_with(&format!(
            "WHERE ts > fromUnixTimestamp64Nano(toInt64({now}), 'Europe/Warsaw') \
             - INTERVAL 1 DAY + INTERVAL 2 HOUR"
        )));
        assert!(p3
            .to_string()
            .contains("filter:  ts > now('Europe/Warsaw') - INTERVAL 1 DAY + INTERVAL 2 HOUR"));
        assert_ne!(p3.history.sql("x"), {
            let mut b4 = b3.clone();
            b4.windows[0].tz = Some(chrono_tz::Europe::London);
            plan(&b4).history.sql("x")
        });
        // its schedule by the Warsaw calendar: the day before the clocks go back is 25 h
        let mut p3 = p3;
        p3.stream.windows[0].zone = chrono_tz::Europe::Warsaw;
        let mut row = history_row(json!({"user_id": "u", "__col_0": "2026-10-24 10:00:00.000"}));
        let decoded = p3.stream.history_decode(&mut row).unwrap();
        let ts = i128::from(crate::types::parse_datetime("2026-10-24 10:00:00.000", 3).unwrap())
            * 1_000_000;
        assert_eq!(
            p3.stream.schedule(&decoded, ts).transitions,
            [(ts + 25 * H - 2 * H, false)]
        );
    }

    /// Schedules: each operator at its threshold, rows coming in and going out, and agreement
    /// with evaluating the WHERE at many instants.
    #[test]
    fn schedules_follow_the_where_through_time() {
        use crate::calendar::Unit;
        const S: i128 = 1_000_000_000;
        let ts = |sec: i64| {
            Value::DateTime(crate::types::DateTime {
                ticks: sec * 1000,
                precision: 3,
            })
        };
        let at = |sec: i64| i128::from(sec) * S;
        let stream = |f: BoundExpr, windows: Vec<TimeWindow>| {
            let mut b = bound(&[0], Some(f));
            b.windows = windows;
            plan(&b).stream
        };
        let w0 = window(None, &[]);
        let row = |sec: i64, latency: i128| {
            let mut r = vec![Value::Null; 5];
            r[0] = ts(sec);
            r[2] = Value::Int(latency);
            r
        };
        // `ts > now()`: in until ts; `>=`: a nanosecond longer
        let p = stream(now_cmp(0, CmpOp::Gt), vec![w0.clone()]);
        assert_eq!(
            p.schedule(&row(100, 0), at(0)).transitions,
            [(at(100), false)]
        );
        let p = stream(now_cmp(0, CmpOp::GtEq), vec![w0.clone()]);
        assert_eq!(
            p.schedule(&row(100, 0), at(0)).transitions,
            [(at(100) + 1, false)]
        );
        // `ts <= now()`: comes in at ts; `<`: a nanosecond later
        let p = stream(now_cmp(0, CmpOp::LtEq), vec![w0.clone()]);
        let s = p.schedule(&row(100, 0), at(0));
        assert!(!s.in_now);
        assert_eq!(s.transitions, [(at(100), true)]);
        let p = stream(now_cmp(0, CmpOp::Lt), vec![w0.clone()]);
        assert_eq!(
            p.schedule(&row(100, 0), at(0)).transitions,
            [(at(100) + 1, true)]
        );
        // `ts <= now() and ts > now() - 10 s`: in at ts, out 10 s later
        let p = stream(
            BoundExpr::And(
                Box::new(now_cmp(0, CmpOp::LtEq)),
                Box::new(now_cmp(1, CmpOp::Gt)),
            ),
            vec![w0.clone(), window(None, &[(-10, Unit::Second)])],
        );
        assert_eq!(
            p.schedule(&row(100, 0), at(0)).transitions,
            [(at(100), true), (at(110), false)]
        );
        // past both: never again
        assert_eq!(p.schedule(&row(100, 0), at(200)), Schedule::default());
        // OR an ordinary condition: a slow row never leaves
        let p = stream(
            BoundExpr::Or(
                Box::new(now_cmp(0, CmpOp::Gt)),
                Box::new(BoundExpr::Compare {
                    op: CmpOp::Gt,
                    left: col(2),
                    right: lit(Value::Int(100)),
                }),
            ),
            vec![w0.clone()],
        );
        assert_eq!(
            p.schedule(&row(100, 500), at(0)),
            Schedule {
                in_now: true,
                transitions: vec![]
            }
        );
        assert_eq!(
            p.schedule(&row(100, 5), at(0)).transitions,
            [(at(100), false)]
        );
        // a NULL column: never in
        let mut null = row(100, 0);
        null[0] = Value::Null;
        assert_eq!(p.schedule(&null, at(0)).transitions, []);

        // random expressions of comparisons: the schedule says what evaluating says, at every
        // instant
        let mut seed: u64 = 5;
        let mut rand = |n: u64| {
            seed = seed
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            (seed >> 33) % n
        };
        let windows: Vec<TimeWindow> = (0..3)
            .map(|i| window(None, &[(i as i64 * 7 - 7, Unit::Second)]))
            .collect();
        fn random(rand: &mut impl FnMut(u64) -> u64, depth: u32) -> BoundExpr {
            let ops = [CmpOp::Gt, CmpOp::GtEq, CmpOp::Lt, CmpOp::LtEq];
            match if depth == 0 { 0 } else { rand(4) } {
                0 => now_cmp(rand(3) as usize, ops[rand(4) as usize]),
                1 => BoundExpr::And(
                    Box::new(random(rand, depth - 1)),
                    Box::new(random(rand, depth - 1)),
                ),
                2 => BoundExpr::Or(
                    Box::new(random(rand, depth - 1)),
                    Box::new(random(rand, depth - 1)),
                ),
                _ => BoundExpr::Not(Box::new(random(rand, depth - 1))),
            }
        }
        for _ in 0..300 {
            let f = random(&mut rand, 3);
            let p = stream(f.clone(), windows.clone());
            let r = row(rand(40) as i64, 0);
            let now = at(rand(40) as i64);
            let s = p.schedule(&r, now);
            // the possible WHERE (as evaluated) is true if the row is in at any later instant
            let possible = possible(&f, false);
            let in_possible = eval(&possible, &r, &|w, (op, c)| {
                p.windows[w]
                    .threshold(op, &r[c.0])
                    .map(|(d, comes_in)| (now >= d) == comes_in)
            }) == Some(true);
            for k in 0..120 {
                let t = now + i128::from(k) * S / 2 - 3 * S;
                if t < now {
                    continue;
                }
                let want = eval(&f, &r, &|w, (op, c)| {
                    p.windows[w]
                        .threshold(op, &r[c.0])
                        .map(|(d, comes_in)| (t >= d) == comes_in)
                }) == Some(true);
                let got = s
                    .transitions
                    .iter()
                    .take_while(|(d, _)| *d <= t)
                    .last()
                    .map_or(s.in_now, |(_, b)| *b);
                assert_eq!(got, want, "{f:?} at {t}");
                if want {
                    assert!(in_possible, "{f:?}: in at {t}, but not possible at {now}");
                }
            }
        }
    }

    #[test]
    fn uuids_match_and_render_normalized() {
        let session = crate::types::Uuid::parse("abcdef00-0000-0000-0000-000000000001").unwrap();
        let f = BoundExpr::Compare {
            op: CmpOp::Eq,
            left: col(4),
            right: lit(Value::Uuid(session)),
        };
        let p = plan(&bound(&[1, 4], Some(f)));
        assert_eq!(
            p.history.sql("default.events"),
            "SELECT user_id, session FROM default.events \
             WHERE session = toUUID('abcdef00-0000-0000-0000-000000000001')"
        );
        let upper = p
            .stream
            .decode(&row(
                json!({"user_id": "a", "session": "ABCDEF00-0000-0000-0000-000000000001"}),
            ))
            .unwrap();
        assert!(p.stream.matches(&upper));
        assert_eq!(
            serde_json::to_string(&p.stream.project(&upper)).unwrap(),
            r#"{"user_id":"a","session":"abcdef00-0000-0000-0000-000000000001"}"#
        );
        let other = p
            .stream
            .decode(&row(json!({"user_id": "b", "session": null})))
            .unwrap();
        assert!(!p.stream.matches(&other));
        assert!(p
            .stream
            .decode(&row(json!({"user_id": "c", "session": "{abcdef00}"})))
            .is_err());
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
            cursor::decode(&p, &token.replace("\"v\":2", "\"v\":1")),
            None
        );
        // stable across runs: a hash of the plan, not of memory addresses
        assert_eq!(
            cursor::fingerprint(&p),
            cursor::fingerprint(&plan(&bound(&[1], None)))
        );
        assert_eq!(cursor::fingerprint(&p), cursor::hash(&cursor::identity(&p)));
        // with a lineage (a shared result's): both come back
        let token = cursor::encode_with(&p, Some("lin-1"), &at);
        assert_eq!(
            cursor::decode_cursor(&p, &token),
            Some(cursor::Cursor {
                at: at.clone(),
                lineage: Some("lin-1".into())
            })
        );
        assert_eq!(cursor::decode(&p, &token), Some(at.clone()));
        assert_eq!(
            cursor::decode_cursor(&p, &cursor::encode(&p, &at))
                .unwrap()
                .lineage,
            None
        );
    }
}
