//! The engine's entry point: [`Engine::explain`] plans a query, [`Engine::subscribe`] runs it.
//!
//! A subscription yields the history from ClickHouse first, then one [`Iteration`] per event of
//! the topics the query needs, each carrying the diffs it produced.

use std::fmt;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;

use futures::stream::{self, BoxStream, Stream, StreamExt};

use crate::aggregate::chdb_run::{AggregateRun, ChdbTable};
use crate::aggregate::native_run::NativeRun;
use crate::aggregate::GroupDiffer;
use crate::binder::{bind, BindError, Catalog};
use crate::chdb::{Chdb, ChdbError};
use crate::cursor;
use crate::diff::{Diff, Row};
use crate::history::{ClickHouseHttp, HistoryError, HistorySource};
use crate::order::{OrderedState, SortKey, WindowState};
use crate::parser::parse;
use crate::plan::{plan, AggregatePlan, Plan, StreamPlan};
use crate::rules::{HardcodedRules, NamingRules, TopicId};
use crate::source::{Event, EventSource, KafkaSource, SourceError, TopicStart};
use crate::watermark::Watermark;

#[derive(Debug, thiserror::Error)]
pub enum EngineError {
    /// Rendered with line and column.
    #[error("syntax error: {0}")]
    Syntax(String),
    /// The query doesn't fit the catalog (unknown table/column, …); rendered with position.
    #[error("{0}")]
    Bind(String),
    #[error(transparent)]
    History(#[from] HistoryError),
    #[error(transparent)]
    Source(#[from] SourceError),
    /// Aggregating (chDB) failed.
    #[error(transparent)]
    Aggregate(#[from] ChdbError),
}

/// Everything [`Engine::connect`] needs for the standard setup: ClickHouse over HTTP, Kafka,
/// and the hard-coded naming rules.
#[derive(Debug, Clone)]
pub struct Config {
    pub clickhouse_url: String,
    pub user: String,
    pub password: String,
    /// ClickHouse database that dialect tables map to.
    pub database: String,
    /// Kafka bootstrap servers, e.g. `127.0.0.1:9092`.
    pub kafka_brokers: String,
    /// ClickHouse's native protocol address, for chDB's `remote()` (aggregates).
    pub clickhouse_native: String,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            clickhouse_url: "http://127.0.0.1:18123".into(),
            user: "default".into(),
            password: String::new(),
            database: "default".into(),
            kafka_brokers: "127.0.0.1:9092".into(),
            clickhouse_native: "127.0.0.1:19000".into(),
        }
    }
}

pub struct Engine {
    rules: Arc<dyn NamingRules>,
    history: Arc<dyn HistorySource>,
    source: Arc<dyn EventSource>,
    native: Native,
    max_batch: usize,
    aggregates: Aggregates,
}

/// Which backend runs aggregate queries.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Aggregates {
    /// Native states in the engine when every aggregate has a native implementation; chDB
    /// otherwise.
    #[default]
    Auto,
    /// Always chDB (ClickHouse's `-State`/`-Merge`), e.g. to compare the two.
    ChdbOnly,
}

#[derive(Debug, Clone)]
struct Native {
    address: String,
    user: String,
    password: String,
}

impl Engine {
    /// The standard setup from a [`Config`].
    pub fn connect(config: Config) -> Self {
        Self::new(
            HardcodedRules {
                database: config.database,
            },
            ClickHouseHttp::new(&config.clickhouse_url, &config.user, &config.password),
            KafkaSource::new(&config.kafka_brokers),
        )
        .with_clickhouse_native(&config.clickhouse_native, &config.user, &config.password)
    }

    /// Where chDB reads aggregate states from main ClickHouse (`remote()`, native protocol);
    /// default `127.0.0.1:19000` as `default` without a password.
    pub fn with_clickhouse_native(mut self, address: &str, user: &str, password: &str) -> Self {
        self.native = Native {
            address: address.into(),
            user: user.into(),
            password: password.into(),
        };
        self
    }

    /// At most this many events are aggregated together (default 1000). Events that arrive
    /// together are batched up to it; 1 means one chDB round trip per event.
    pub fn with_max_batch(mut self, n: usize) -> Self {
        self.max_batch = n.max(1);
        self
    }

    /// Any naming rules, history and event source, e.g. fakes in tests.
    pub fn new(
        rules: impl NamingRules + 'static,
        history: impl HistorySource + 'static,
        source: impl EventSource + 'static,
    ) -> Self {
        Self {
            rules: Arc::new(rules),
            history: Arc::new(history),
            source: Arc::new(source),
            native: Native {
                address: "127.0.0.1:19000".into(),
                user: "default".into(),
                password: String::new(),
            },
            max_batch: 1000,
            aggregates: Aggregates::Auto,
        }
    }

    /// Which backend runs aggregate queries (default: native when possible).
    pub fn with_aggregates(mut self, aggregates: Aggregates) -> Self {
        self.aggregates = aggregates;
        self
    }

    /// Parse, bind and plan a query without running it.
    pub async fn explain(&self, sql: &str) -> Result<Plan, EngineError> {
        let query = parse(sql).map_err(|e| EngineError::Syntax(e.render(sql)))?;
        let catalog = Catalog {
            rules: self.rules.as_ref(),
            history: self.history.as_ref(),
            source: self.source.as_ref(),
        };
        let bound = bind(&query, &catalog).await.map_err(|e| match e {
            BindError::History(e) => EngineError::History(e),
            BindError::Source(e) => EngineError::Source(e),
            e => EngineError::Bind(e.render(sql)),
        })?;
        Ok(plan(&bound))
    }

    /// Plan a query and start it: the history first, then one iteration per event of the
    /// plan's topics.
    ///
    /// History and stream meet at the [`Watermark`], read now: history is every row up to it,
    /// and each partition is consumed from right after it. So every event is in exactly one of
    /// them, wherever ClickHouse's ingestion is at.
    pub async fn subscribe(&self, sql: &str) -> Result<Subscription, EngineError> {
        self.subscribe_from(sql, None).await
    }

    /// Like [`subscribe`](Self::subscribe), but for a client that already holds the result up to
    /// the cursor `since` (a token from an earlier [`Iteration::at`], see [`crate::cursor`]).
    /// The first iteration is then a [`Origin::CatchUp`] with only what the client is missing:
    /// - ClickHouse's rows after the cursor and up to the current watermark `W`;
    /// - with ORDER BY, placed by the sort keys of the client's result, read from ClickHouse up
    ///   to the cursor and, where the client is ahead of ClickHouse, from Kafka (its events
    ///   between `W` and the cursor, read first and not sent again); with LIMIT alone, by the
    ///   client's row count, likewise;
    /// - and the stream continues from `max(cursor, W)`, so nothing is missed or repeated.
    ///
    /// An invalid cursor (another plan, or past the end of the topic, e.g. it was recreated)
    /// gives a normal subscription from history ([`Subscription::resumed`] is false).
    pub async fn subscribe_from(
        &self,
        sql: &str,
        since: Option<&str>,
    ) -> Result<Subscription, EngineError> {
        let plan = self.explain(sql).await?;
        let kafka = &plan.history.kafka;
        let watermark = Watermark::fetch(
            self.history.as_ref(),
            &plan.history.table,
            &kafka.partition,
            &kafka.offset,
        )
        .await?;
        if let Some(aggregate) = plan.aggregate.clone() {
            // aggregates always start from history (no resume yet)
            return self.subscribe_aggregate(plan, aggregate, watermark).await;
        }
        // With ORDER BY or LIMIT the server rebuilds the client's state, which needs every row
        // the client holds; without, there's nothing to rebuild.
        let stateful = plan.stream.order.is_some() || plan.stream.limit.is_some();
        let resume_from = match since {
            Some(token) => self.resumable(&plan, token, &watermark, stateful).await?,
            None => None,
        };
        let (begin, at, first, history_sql) = match &resume_from {
            None => {
                let sql = plan.history.sql(Some(&watermark));
                (
                    watermark.clone(),
                    watermark.clone(),
                    First::History(sql.clone()),
                    sql,
                )
            }
            Some(cursor) => {
                let catch_up = plan.history.catch_up_sql(cursor, &watermark);
                let seed = match (&plan.stream.order, plan.stream.limit) {
                    (Some(_), _) => Seed::Keys(plan.history.keys_sql(cursor).expect("ordered")),
                    (None, Some(_)) => Seed::Count(plan.history.count_sql(cursor)),
                    (None, None) => Seed::Nothing,
                };
                // Where the client is ahead of ClickHouse (it streamed events ClickHouse hasn't
                // ingested yet), those events are re-read from Kafka to rebuild the state: the
                // stream starts right after ClickHouse there, and its events up to the cursor
                // are the client's own (`ahead`), never sent again.
                let ahead = Watermark(if stateful {
                    cursor
                        .0
                        .iter()
                        .filter(|(p, o)| **o > watermark.get(**p))
                        .map(|(p, o)| (*p, *o))
                        .collect()
                } else {
                    Default::default()
                });
                let mut begin = cursor.max(&watermark);
                for p in ahead.0.keys() {
                    begin.0.insert(*p, watermark.get(*p));
                }
                // nothing to fetch where ClickHouse isn't past the client
                let behind = watermark.0.iter().all(|(p, w)| *w <= cursor.get(*p));
                (
                    begin,
                    cursor.max(&watermark),
                    First::CatchUp {
                        seed,
                        rows: (!behind).then(|| catch_up.clone()),
                        ahead,
                    },
                    catch_up,
                )
            }
        };
        let mut starts = Vec::new();
        for topic in &plan.stream.topics {
            let partitions = self
                .source
                .partitions(topic)
                .await?
                .ok_or_else(|| SourceError(format!("topic {topic} disappeared")))?;
            starts.push(TopicStart {
                topic: topic.clone(),
                partitions: partitions
                    .into_iter()
                    .map(|p| (p, begin.start(p)))
                    .collect(),
            });
        }
        tracing::info!(
            history = %history_sql,
            watermark = %watermark.describe(),
            resumed_from = %resume_from.as_ref().map(Watermark::describe).unwrap_or_default(),
            "subscribed"
        );
        let events = self
            .source
            .subscribe(&starts, &plan.stream.required_keys())
            .await?;
        let iterations = self.iterations(&plan, first, at, events);
        Ok(Subscription {
            plan,
            watermark,
            history_sql,
            resumed: resume_from.is_some(),
            iterations,
        })
    }

    /// GROUP BY / aggregates: chDB keeps each group's aggregate states in a table of its own
    /// (dropped with the subscription). History: main ClickHouse computes the states up to the
    /// watermark (`remote()`). Then, per batch of events: their states are added, and the groups
    /// they touched are read back finalized (`-Merge`) and compared with what was sent.
    async fn subscribe_aggregate(
        &self,
        plan: Plan,
        aggregate: AggregatePlan,
        watermark: Watermark,
    ) -> Result<Subscription, EngineError> {
        let bound =
            watermark.history_predicate(&aggregate.kafka.partition, &aggregate.kafka.offset);
        let mut starts = Vec::new();
        for topic in &plan.stream.topics {
            let partitions = self
                .source
                .partitions(topic)
                .await?
                .ok_or_else(|| SourceError(format!("topic {topic} disappeared")))?;
            starts.push(TopicStart {
                topic: topic.clone(),
                partitions: partitions
                    .into_iter()
                    .map(|p| (p, watermark.start(p)))
                    .collect(),
            });
        }
        let events = self
            .source
            .subscribe(&starts, &plan.stream.required_keys())
            .await?
            .ready_chunks(self.max_batch)
            .boxed();
        let native_sql = match self.aggregates {
            Aggregates::Auto => aggregate.native_history_sql(&bound),
            Aggregates::ChdbOnly => None,
        };
        let (history_sql, iterations) = match (native_sql, aggregate.native.clone()) {
            (Some(sql), Some(native)) => {
                tracing::info!(history = %sql, "subscribed (aggregate, native)");
                let run = NativeRun::new(
                    self.history.clone(),
                    sql.clone(),
                    native,
                    plan.stream.clone(),
                    events,
                    watermark.clone(),
                );
                let iterations = stream::unfold(run, |mut run| async move {
                    let next = run.step().await?;
                    Some((next, run))
                })
                .boxed();
                (sql, iterations)
            }
            _ => {
                static NEXT: AtomicU64 = AtomicU64::new(1);
                let chdb = Chdb::shared()?;
                let table = format!(
                    "chlive_agg_{}_{}",
                    std::process::id(),
                    NEXT.fetch_add(1, AtomicOrdering::Relaxed)
                );
                let remote = aggregate.remote(
                    &self.native.address,
                    &self.native.user,
                    &self.native.password,
                );
                let create = aggregate.create_sql(&table, &remote, &bound);
                tracing::info!(table, create = %create.replace(&self.native.password, "***"), "subscribed (aggregate, chDB)");
                let run = AggregateRun {
                    table: Arc::new(ChdbTable {
                        chdb: chdb.clone(),
                        name: table,
                    }),
                    chdb,
                    create: Some(create.clone()),
                    aggregate,
                    stream: plan.stream.clone(),
                    events,
                    sent: GroupDiffer::default(),
                    at: watermark.clone(),
                    batches: 0,
                    failed: false,
                };
                let iterations = stream::unfold(run, |mut run| async move {
                    let next = run.step().await?;
                    Some((next, run))
                })
                .boxed();
                (create, iterations)
            }
        };
        Ok(Subscription {
            plan,
            watermark,
            history_sql,
            resumed: false,
            iterations,
        })
    }

    /// The cursor in `token`, if this plan can resume from it: one of ours for this plan, and
    /// every offset to be read again still in the topic. Past its end means the topic was
    /// recreated; before its start, retention deleted what's needed.
    async fn resumable(
        &self,
        plan: &Plan,
        token: &str,
        watermark: &Watermark,
        stateful: bool,
    ) -> Result<Option<Watermark>, EngineError> {
        let Some(cursor) = cursor::decode(plan, token) else {
            tracing::info!("resume cursor is for another plan or malformed; starting over");
            return Ok(None);
        };
        for topic in &plan.stream.topics {
            let Some(bounds) = self.source.offset_bounds(topic).await? else {
                continue;
            };
            for (p, c) in &cursor.0 {
                let (low, high) = bounds.get(p).copied().unwrap_or((0, 0));
                let w = watermark.get(*p);
                // the first offset the resume reads from Kafka in this partition
                let first = if stateful && *c > w {
                    w + 1
                } else {
                    (*c).max(w) + 1
                };
                if *c >= high || first < low {
                    tracing::info!(
                        cursor = %cursor.describe(),
                        partition = p,
                        low,
                        high,
                        "resume cursor doesn't fit {topic} (recreated, or deleted by retention); starting over"
                    );
                    return Ok(None);
                }
            }
        }
        Ok(Some(cursor))
    }

    fn iterations(
        &self,
        plan: &Plan,
        first: First,
        at: Watermark,
        events: BoxStream<'static, Event>,
    ) -> BoxStream<'static, Result<Iteration, EngineError>> {
        enum Item {
            History(Result<Vec<Row>, HistoryError>),
            CatchUp(
                Result<(Option<Vec<Row>>, Vec<Row>), HistoryError>,
                Vec<Event>,
            ),
            Event(Event),
            Failed(EngineError),
        }
        let history = self.history.clone();
        // the first items, then the rest of the stream
        let items = async move {
            match first {
                First::History(sql) => {
                    let rows = history.fetch(&sql).await;
                    stream::iter([Item::History(rows)])
                        .chain(events.map(Item::Event))
                        .boxed()
                }
                First::CatchUp { seed, rows, ahead } => {
                    let fetched = async {
                        let seed = match seed {
                            Seed::Keys(sql) | Seed::Count(sql) => Some(history.fetch(&sql).await?),
                            Seed::Nothing => None,
                        };
                        let rows = match rows {
                            Some(sql) => history.fetch(&sql).await?,
                            None => Vec::new(),
                        };
                        Ok((seed, rows))
                    }
                    .await;
                    let mut events = events;
                    match read_ahead(&mut events, &ahead).await {
                        Ok((own, held)) => stream::iter([Item::CatchUp(fetched, own)])
                            .chain(stream::iter(held).map(Item::Event))
                            .chain(events.map(Item::Event))
                            .boxed(),
                        Err(e) => stream::iter([Item::Failed(e)]).boxed(),
                    }
                }
            }
        };
        let stream_plan = plan.stream.clone();
        let state = (ResultState::for_plan(&stream_plan), at);
        stream::once(items)
            .flatten()
            .scan(state, move |(state, at), item| {
                let iteration = match item {
                    Item::History(rows) => history_iteration(&stream_plan, state, at, rows),
                    Item::CatchUp(fetched, own) => {
                        catch_up_iteration(&stream_plan, state, at, fetched, own)
                    }
                    Item::Event(event) => Ok(event_iteration(&stream_plan, state, at, event)),
                    Item::Failed(e) => Err(e),
                };
                futures::future::ready(Some(iteration))
            })
            .boxed()
    }
}

/// How long a resume may take to re-read the client's own events from Kafka.
const READ_AHEAD_TIMEOUT: Duration = Duration::from_secs(30);

/// Resume, for partitions where the client is ahead of ClickHouse: read the stream until each
/// reaches the client's cursor. Returns the client's own events (up to its cursor, to rebuild the
/// state from) and the later ones read meanwhile (to be sent after the catch-up, in order).
async fn read_ahead(
    events: &mut BoxStream<'static, Event>,
    ahead: &Watermark,
) -> Result<(Vec<Event>, Vec<Event>), EngineError> {
    let mut pending = ahead.0.clone();
    let (mut own, mut held) = (Vec::new(), Vec::new());
    let deadline = tokio::time::Instant::now() + READ_AHEAD_TIMEOUT;
    while !pending.is_empty() {
        let event = match tokio::time::timeout_at(deadline, events.next()).await {
            Ok(Some(e)) => e,
            Ok(None) => break, // a finite source (tests) ended
            Err(_) => {
                return Err(SourceError(format!(
                    "re-reading the client's events from Kafka timed out (still waiting for {})",
                    Watermark(pending).describe()
                ))
                .into())
            }
        };
        match ahead.0.get(&event.partition) {
            Some(upto) if event.offset <= *upto => {
                if event.offset == *upto {
                    pending.remove(&event.partition);
                }
                own.push(event);
            }
            Some(_) => {
                // past the cursor (offsets can skip): this partition is done
                pending.remove(&event.partition);
                held.push(event);
            }
            None => held.push(event),
        }
    }
    Ok((own, held))
}

/// What a subscription fetches first.
enum First {
    /// The whole result up to the watermark.
    History(String),
    /// Resume: what to seed the state with, and the rows the client is missing (`None` when
    /// ClickHouse has nothing past the client's cursor).
    CatchUp {
        seed: Seed,
        rows: Option<String>,
        /// Per partition, the client's cursor where it is ahead of ClickHouse.
        ahead: Watermark,
    },
}

enum Seed {
    /// ORDER BY: the sort keys of the client's result.
    Keys(String),
    /// LIMIT alone: the client's row count.
    Count(String),
    Nothing,
}

/// What a subscription keeps about its result to place new rows.
enum ResultState {
    /// Unordered, unlimited: every row is appended.
    Append,
    /// LIMIT without ORDER BY: the latest N rows.
    Window(WindowState),
    /// ORDER BY (and maybe LIMIT): the sort keys of the result, in tie groups.
    Ordered(OrderedState),
}

impl ResultState {
    fn for_plan(plan: &StreamPlan) -> Self {
        match (&plan.order, plan.limit) {
            (Some(o), limit) => Self::Ordered(OrderedState::with_limit(o.keys.clone(), limit)),
            (None, Some(n)) => Self::Window(WindowState::new(n)),
            (None, None) => Self::Append,
        }
    }

    /// Where a new row goes: an insert (at an index with ORDER BY) and the delete it causes, or
    /// nothing when it falls outside a LIMIT. `key` is its sort key with ORDER BY.
    fn place(&mut self, row: Row, key: Option<SortKey>) -> Vec<Diff> {
        match self {
            ResultState::Append => vec![Diff::insert(row)],
            ResultState::Window(w) => match w.push() {
                None => Vec::new(),
                Some(false) => vec![Diff::insert(row)],
                Some(true) => vec![Diff::insert(row), Diff::Delete { index: 0 }],
            },
            ResultState::Ordered(s) => match s.insert(key.expect("a sort key")) {
                None => Vec::new(),
                Some(p) => {
                    let mut diffs = vec![Diff::Insert {
                        row,
                        index: Some(p.index),
                        id: None,
                    }];
                    if let Some(index) = p.evicts {
                        diffs.push(Diff::Delete { index });
                    }
                    diffs
                }
            },
        }
    }
}

fn bad_history_row(e: impl std::fmt::Display) -> EngineError {
    HistoryError::ClickHouse(format!("history row: {e}")).into()
}

/// The history as one iteration. Seeds the state: with ORDER BY the sort keys, in ClickHouse's
/// order (dropping the hidden ORDER BY fields from the rows); with only a LIMIT the row count.
fn history_iteration(
    plan: &StreamPlan,
    state: &mut ResultState,
    at: &Watermark,
    rows: Result<Vec<Row>, HistoryError>,
) -> Result<Iteration, EngineError> {
    let mut rows = rows?;
    match (state, &plan.order) {
        (ResultState::Ordered(state), Some(order)) => {
            for row in &mut rows {
                state.push_history(order.history_key(row).map_err(bad_history_row)?);
            }
        }
        (ResultState::Window(w), _) => rows.iter().for_each(|_| w.push_history()),
        _ => {}
    }
    Ok(Iteration {
        origin: Origin::History,
        diffs: rows.into_iter().map(Diff::insert).collect(),
        at: at.clone(),
    })
}

/// Resume: seed the state as the client's result was at its cursor, then place the rows it is
/// missing, exactly as new events would be.
fn catch_up_iteration(
    plan: &StreamPlan,
    state: &mut ResultState,
    at: &Watermark,
    fetched: Result<(Option<Vec<Row>>, Vec<Row>), HistoryError>,
    own: Vec<Event>,
) -> Result<Iteration, EngineError> {
    let (seed, mut rows) = fetched?;
    match (&mut *state, &plan.order, seed) {
        (ResultState::Ordered(s), Some(order), Some(mut keys)) => {
            for k in &mut keys {
                s.push_history(order.history_key(k).map_err(bad_history_row)?);
            }
        }
        (ResultState::Window(_), _, Some(count)) => {
            let n = count
                .first()
                .and_then(|r| r.get("n"))
                .and_then(|n| n.as_u64())
                .ok_or_else(|| bad_history_row("no row count"))?;
            *state = ResultState::Window(WindowState::with_len(plan.limit.unwrap_or(0), n));
        }
        _ => {}
    }
    // the client's own events that ClickHouse doesn't have yet: into the state, not sent
    for event in own {
        if let Ok(row) = plan.decode(&event.value) {
            if plan.matches(&row) {
                let key = plan.order.as_ref().map(|o| o.event_key(&row));
                state.place(plan.project(&row), key);
            }
        }
    }
    if plan.order.is_none() && plan.limit.is_some() {
        rows.reverse(); // fetched latest first
    }
    let mut diffs = Vec::new();
    for mut row in rows {
        let key = match &plan.order {
            Some(order) => Some(order.history_key(&mut row).map_err(bad_history_row)?),
            None => None,
        };
        diffs.extend(state.place(row, key));
    }
    Ok(Iteration {
        origin: Origin::CatchUp,
        diffs,
        at: at.clone(),
    })
}

/// One event: decode by column types, filter, project, and place it. An insert and the delete
/// it causes come in the same iteration, insert first, so they always land in the same diff.
fn event_iteration(
    plan: &StreamPlan,
    state: &mut ResultState,
    at: &mut Watermark,
    event: Event,
) -> Iteration {
    let diffs = match plan.decode(&event.value) {
        Ok(row) if plan.matches(&row) => {
            let key = plan.order.as_ref().map(|o| o.event_key(&row));
            state.place(plan.project(&row), key)
        }
        Ok(_) => Vec::new(),
        Err(e) => {
            // ClickHouse's ingestion skips this message too
            tracing::warn!(
                topic = %event.topic,
                partition = event.partition,
                offset = event.offset,
                error = %e,
                "skipping event"
            );
            Vec::new()
        }
    };
    let position = at.0.entry(event.partition).or_insert(event.offset);
    *position = (*position).max(event.offset);
    Iteration {
        origin: Origin::Event {
            topic: event.topic,
            partition: event.partition,
            offset: event.offset,
        },
        diffs,
        at: at.clone(),
    }
}

/// A running query: a stream of [`Iteration`]s, plus the plan it runs.
pub struct Subscription {
    pub plan: Plan,
    /// Where history ends and the stream begins.
    pub watermark: Watermark,
    /// The history query as run: the plan's, bounded by the watermark (or, when resumed, the
    /// catch-up query).
    pub history_sql: String,
    /// Whether this resumed from a client's cursor (the first iteration is then a catch-up).
    pub resumed: bool,
    iterations: BoxStream<'static, Result<Iteration, EngineError>>,
}

impl Stream for Subscription {
    type Item = Result<Iteration, EngineError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.iterations.poll_next_unpin(cx)
    }
}

/// Where an iteration's diffs came from.
#[derive(Debug, Clone, PartialEq)]
pub enum Origin {
    /// The initial result, from ClickHouse.
    History,
    /// Resume: what the client is missing since its cursor, from ClickHouse.
    CatchUp,
    /// One event of a subscribed topic.
    Event {
        topic: TopicId,
        partition: i32,
        offset: i64,
    },
}

#[derive(Debug, Clone, PartialEq)]
pub struct Iteration {
    pub origin: Origin,
    /// Empty when the event was not relevant to the query.
    pub diffs: Vec<Diff>,
    /// The cursor after this iteration: per partition, the offset up to which the result is
    /// complete (whether events matched or not). Resume from it with [`cursor::encode`] and
    /// [`Engine::subscribe_from`].
    pub at: Watermark,
}

/// One line per diff: `+ {row}` for the history (followed by `history: N rows`), and
/// `<topic>/<partition>:<offset> + {row}` for events. Irrelevant events print nothing.
impl fmt::Display for Iteration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.origin {
            Origin::History => {
                for d in &self.diffs {
                    writeln!(f, "{d}")?;
                }
                write!(f, "history: {} rows", self.diffs.len())
            }
            Origin::CatchUp => {
                for d in &self.diffs {
                    writeln!(f, "{d}")?;
                }
                write!(f, "caught up: {} changes", self.diffs.len())
            }
            Origin::Event {
                topic,
                partition,
                offset,
            } => {
                let mut first = true;
                for d in &self.diffs {
                    if !first {
                        writeln!(f)?;
                    }
                    first = false;
                    write!(f, "{topic}/{partition}:{offset} {d}")?;
                }
                Ok(())
            }
        }
    }
}
