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
use crate::binder::{bind, BindError, Catalog};
use crate::chdb::{Chdb, ChdbError};
use crate::cursor;
use crate::diff::{Diff, Row};
use crate::history::{ClickHouseHttp, HistoryError, HistorySource};
use crate::order::{HeldWindow, OffsetState, OrderedState, SortKey, WindowState};
use crate::parser::parse;
use crate::plan::{plan, AggregatePlan, Plan, StreamPlan};
use crate::rules::{HardcodedRules, NamingRules, TopicId};
use crate::snapshot::{
    ClickHouseCloner, Lease, PauseServer, SnapshotConfig, SnapshotError, SnapshotManager, Snapshots,
};
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
    /// No snapshot of the table could be taken (pause server or ClickHouse).
    #[error("snapshot: {0}")]
    Snapshot(#[from] SnapshotError),
}

/// Everything [`Engine::connect`] needs for the standard setup: ClickHouse over HTTP, Kafka,
/// snapshots through the pause server, and the hard-coded naming rules.
///
/// `clickhouse_url` and `clickhouse_native` must be the same ClickHouse node: snapshots are
/// created there, and exist only there.
#[derive(Debug, Clone)]
pub struct Config {
    pub clickhouse_url: String,
    /// Reads history (needs SELECT on the tables and their snapshots).
    pub user: String,
    pub password: String,
    /// ClickHouse database that dialect tables map to.
    pub database: String,
    /// Kafka bootstrap servers, e.g. `127.0.0.1:9092`.
    pub kafka_brokers: String,
    /// ClickHouse's native protocol address, for chDB's `remote()` (aggregates).
    pub clickhouse_native: String,
    /// The pause server (continuous-backup's), e.g. `http://127.0.0.1:18091`.
    pub pause_server_url: String,
    /// Creates and drops snapshots: CREATE TABLE, ALTER (ATTACH PARTITION) and DROP TABLE in
    /// the tables' databases, and SYSTEM SYNC REPLICA for replicated tables.
    pub snapshot_user: String,
    pub snapshot_password: String,
    pub snapshots: SnapshotConfig,
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
            pause_server_url: "http://127.0.0.1:18091".into(),
            snapshot_user: "default".into(),
            snapshot_password: String::new(),
            snapshots: SnapshotConfig::default(),
        }
    }
}

pub struct Engine {
    rules: Arc<dyn NamingRules>,
    history: Arc<dyn HistorySource>,
    source: Arc<dyn EventSource>,
    snapshots: Arc<dyn Snapshots>,
    native: Native,
    max_batch: usize,
    /// How often a semi-join looks for a newer snapshot of its outer table.
    semi_refresh: Duration,
}

#[derive(Debug, Clone)]
struct Native {
    address: String,
    user: String,
    password: String,
}

impl Engine {
    /// The standard setup from a [`Config`]. Starts the snapshot task, so it must run inside a
    /// tokio runtime.
    pub fn connect(config: Config) -> Self {
        let snapshots = SnapshotManager::start(
            Arc::new(PauseServer::new(&config.pause_server_url)),
            Arc::new(ClickHouseCloner::new(
                &config.clickhouse_url,
                &config.snapshot_user,
                &config.snapshot_password,
            )),
            config.snapshots,
        );
        Self::new(
            HardcodedRules {
                database: config.database,
            },
            ClickHouseHttp::new(&config.clickhouse_url, &config.user, &config.password),
            KafkaSource::new(&config.kafka_brokers),
            snapshots,
        )
        .with_semi_refresh(config.snapshots.refresh)
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

    /// Any naming rules, history, event source and snapshots, e.g. fakes in tests.
    pub fn new(
        rules: impl NamingRules + 'static,
        history: impl HistorySource + 'static,
        source: impl EventSource + 'static,
        snapshots: impl Snapshots + 'static,
    ) -> Self {
        Self {
            rules: Arc::new(rules),
            history: Arc::new(history),
            source: Arc::new(source),
            snapshots: Arc::new(snapshots),
            native: Native {
                address: "127.0.0.1:19000".into(),
                user: "default".into(),
                password: String::new(),
            },
            max_batch: 1000,
            semi_refresh: Duration::from_secs(10),
        }
    }

    /// How often a semi-join (`IN (SELECT …)`) moves to a newer snapshot of its outer table,
    /// so the rows it keeps back stay few (default 10 s, the snapshots' refresh).
    pub fn with_semi_refresh(mut self, every: Duration) -> Self {
        self.semi_refresh = every;
        self
    }

    /// Stop taking snapshots and drop the ones no subscription is reading. Call before exiting.
    pub async fn shutdown(&self) {
        self.snapshots.shutdown().await;
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
    /// History is read from a snapshot of the table (see [`crate::snapshot`]): a clone holding
    /// every offset up to its [`Watermark`], and each partition is consumed from right after
    /// it. So every event is in exactly one of them, wherever ClickHouse's ingestion is at.
    pub async fn subscribe(&self, sql: &str) -> Result<Subscription, EngineError> {
        self.subscribe_from(sql, None).await
    }

    /// Like [`subscribe`](Self::subscribe), but for a client that already holds the result up to
    /// the cursor `since` (a token from an earlier [`Iteration::at`], see [`crate::cursor`]).
    /// The first iteration is then an [`Origin::CatchUp`], and the client gets only what it's
    /// missing:
    /// - unordered, unlimited: nothing to rebuild; the stream continues right after the cursor;
    /// - with ORDER BY or LIMIT, the client's state is rebuilt: the snapshot's sort keys (or row
    ///   count), then the client's own events between the snapshot and the cursor, re-read from
    ///   Kafka and not sent again; the stream continues after the cursor. This needs the cursor
    ///   at or past the snapshot in every partition.
    ///
    /// Otherwise (another plan, a cursor behind the snapshot, offsets gone from the topic by
    /// retention or recreation) it's a normal subscription from history
    /// ([`Subscription::resumed`] is false).
    pub async fn subscribe_from(
        &self,
        sql: &str,
        since: Option<&str>,
    ) -> Result<Subscription, EngineError> {
        let plan = self.explain(sql).await?;
        if plan.semi.is_some() {
            // a semi-join always starts from history (no resume yet)
            return self.subscribe_semi(plan).await;
        }
        if let Some(aggregate) = plan.aggregate.clone() {
            // aggregates always start from history (no resume yet)
            let lease = self.snapshot(&plan.source).await?;
            return self.subscribe_aggregate(plan, aggregate, lease).await;
        }
        // With ORDER BY or LIMIT the server rebuilds the client's state, which needs every row
        // the client holds; without, there's nothing to rebuild, nor any snapshot to read.
        let stateful = plan.stream.order.is_some()
            || plan.stream.limit.is_some()
            || plan.stream.offset.is_some();
        if let (Some(token), false) = (since, stateful) {
            if let Some(cursor) = self.resumable(&plan, token, None).await? {
                let first = First::CatchUp {
                    seed: Seed::Nothing,
                    ahead: Watermark::default(),
                };
                return self
                    .start(
                        plan,
                        None,
                        cursor.clone(),
                        cursor.clone(),
                        first,
                        String::new(),
                        true,
                    )
                    .await;
            }
        }
        let lease = self.snapshot(&plan.source).await?;
        let watermark = lease.watermark.clone();
        let from = lease.sql_table();
        let resume_from = match (since, stateful) {
            (Some(token), true) => self.resumable(&plan, token, Some(&watermark)).await?,
            _ => None,
        };
        let (begin, at, first, history_sql) = match resume_from {
            None => {
                let sql = plan.history.sql(&from);
                (
                    watermark.clone(),
                    watermark.clone(),
                    First::History(sql.clone()),
                    sql,
                )
            }
            Some(cursor) => {
                // the client's state as of the snapshot (it's at or past it everywhere) ...
                let seed_sql = match &plan.stream.order {
                    Some(_) => plan.history.keys_sql(&from).expect("ordered"),
                    None => plan.history.count_sql(&from),
                };
                let seed = match &plan.stream.order {
                    Some(_) => Seed::Keys(seed_sql.clone()),
                    None => Seed::Count(seed_sql.clone()),
                };
                // ... then its own events after the snapshot, re-read from Kafka and not sent
                let ahead = Watermark(
                    cursor
                        .0
                        .iter()
                        .filter(|(p, o)| **o > watermark.get(**p))
                        .map(|(p, o)| (*p, *o))
                        .collect(),
                );
                (
                    watermark.clone(),
                    cursor.max(&watermark),
                    First::CatchUp { seed, ahead },
                    seed_sql,
                )
            }
        };
        let resumed = matches!(first, First::CatchUp { .. });
        self.start(plan, Some(lease), begin, at, first, history_sql, resumed)
            .await
    }

    /// `key IN (SELECT …)`: history over snapshots of both tables, then both topics, the
    /// sub-query's keys growing and the outer rows kept back until their key comes (see
    /// [`crate::semi`]).
    async fn subscribe_semi(&self, plan: Plan) -> Result<Subscription, EngineError> {
        let semi = plan.semi.clone().expect("a semi-join plan");
        let outer = self.snapshot(&plan.source).await?;
        let same = semi.inner == plan.source;
        let inner = if same {
            outer.clone()
        } else {
            self.snapshot(&semi.inner).await?
        };
        let history_sql = plan
            .history
            .sql_where(&outer.sql_table(), &semi.condition(&inner.sql_table()));
        let keys_sql = semi.keys_sql(&inner.sql_table());
        let mut starts = Vec::new();
        let mut topics = vec![(plan.stream.topics[0].clone(), outer.watermark.clone())];
        if !same {
            topics.push((semi.inner_topic.clone(), inner.watermark.clone()));
        }
        for (topic, w) in topics {
            starts.push(self.topic_start(&topic, &w).await?);
        }
        let mut keys = plan.stream.required_keys();
        for k in semi.inner_stream.required_keys() {
            if !keys.contains(&k) {
                keys.push(k);
            }
        }
        let events = self
            .source
            .subscribe(&starts, &keys)
            .await?
            .ready_chunks(self.max_batch)
            .boxed();
        tracing::info!(history = %history_sql, keys = %keys_sql, "subscribed (semi-join)");
        let watermark = outer.watermark.clone();
        let run = crate::semi::SemiRun::new(
            self.history.clone(),
            self.snapshots.clone(),
            plan.clone(),
            crate::semi::SemiStart {
                history_sql: history_sql.clone(),
                keys_sql,
                inner_lease: inner.clone(),
            },
            outer,
            inner.watermark.clone(),
            events,
            self.semi_refresh,
        );
        let iterations = stream::unfold(run, |mut run| async move {
            let next = run.step().await?;
            Some((next, run))
        })
        .boxed();
        Ok(Subscription {
            plan,
            watermark,
            history_sql,
            resumed: false,
            iterations,
        })
    }

    async fn snapshot(&self, table: &crate::snapshot::SnapshotTable) -> Result<Lease, EngineError> {
        Ok(self.snapshots.acquire(table).await?)
    }

    /// Where to read `topic` from: every partition, right after `from`. The partitions usually
    /// come from the source's cache; if `from` knows one it doesn't (partitions were added
    /// since), they're read anew, so no partition is skipped.
    async fn topic_start(
        &self,
        topic: &TopicId,
        from: &Watermark,
    ) -> Result<TopicStart, EngineError> {
        let mut partitions = self.source.partitions(topic).await?;
        if let Some(known) = &partitions {
            if from.0.keys().any(|p| !known.contains(p)) {
                partitions = self.source.refresh_partitions(topic).await?;
            }
        }
        let partitions =
            partitions.ok_or_else(|| SourceError(format!("topic {topic} disappeared")))?;
        Ok(TopicStart {
            topic: topic.clone(),
            partitions: partitions.into_iter().map(|p| (p, from.start(p))).collect(),
        })
    }

    /// Subscribe to the topics from `begin` and run: `first`, then the events.
    #[allow(clippy::too_many_arguments)]
    async fn start(
        &self,
        plan: Plan,
        lease: Option<Lease>,
        begin: Watermark,
        at: Watermark,
        first: First,
        history_sql: String,
        resumed: bool,
    ) -> Result<Subscription, EngineError> {
        let mut starts = Vec::new();
        for topic in &plan.stream.topics {
            starts.push(self.topic_start(topic, &begin).await?);
        }
        tracing::info!(
            history = %history_sql,
            begin = %begin.describe(),
            resumed,
            "subscribed"
        );
        let events = self
            .source
            .subscribe(&starts, &plan.stream.required_keys())
            .await?;
        let iterations = self.iterations(&plan, lease, first, at, events);
        Ok(Subscription {
            plan,
            watermark: begin,
            history_sql,
            resumed,
            iterations,
        })
    }

    /// GROUP BY / aggregates, natively or in chDB (see [`crate::aggregate`]). History: the
    /// states of the snapshot's rows, computed by main ClickHouse. Then, per batch of events:
    /// the groups they touched are updated and compared with what was sent.
    async fn subscribe_aggregate(
        &self,
        plan: Plan,
        aggregate: AggregatePlan,
        lease: Lease,
    ) -> Result<Subscription, EngineError> {
        let watermark = lease.watermark.clone();
        let from = lease.sql_table();
        let mut starts = Vec::new();
        for topic in &plan.stream.topics {
            starts.push(self.topic_start(topic, &watermark).await?);
        }
        let events = self
            .source
            .subscribe(&starts, &plan.stream.required_keys())
            .await?
            .ready_chunks(self.max_batch)
            .boxed();
        let (history_sql, iterations) = match (
            aggregate.native_history_sql(&from),
            aggregate.native.clone(),
        ) {
            (Some(sql), Some(native)) => {
                tracing::info!(history = %sql, "subscribed (aggregate, native)");
                let run = NativeRun::new(
                    self.history.clone(),
                    sql.clone(),
                    lease,
                    native,
                    plan.stream.clone(),
                    events,
                    watermark.clone(),
                    aggregate.result(),
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
                let remote = AggregatePlan::remote(
                    &from,
                    &self.native.address,
                    &self.native.user,
                    &self.native.password,
                );
                let create = aggregate.create_sql(&table, &remote);
                tracing::info!(table, create = %create.replace(&self.native.password, "***"), "subscribed (aggregate, chDB)");
                let result = aggregate.result();
                let run = AggregateRun {
                    table: Arc::new(ChdbTable {
                        chdb: chdb.clone(),
                        name: table,
                    }),
                    chdb,
                    create: Some(create.clone()),
                    snapshot: Some(lease),
                    aggregate,
                    stream: plan.stream.clone(),
                    events,
                    result,
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

    /// The cursor in `token`, if this plan can resume from it: one of ours for this plan, at or
    /// past `snapshot` in every partition (stateful queries, which rebuild their state from it),
    /// and every offset to be read again still in the topic. Past its end means the topic was
    /// recreated; before its start, retention deleted what's needed.
    async fn resumable(
        &self,
        plan: &Plan,
        token: &str,
        snapshot: Option<&Watermark>,
    ) -> Result<Option<Watermark>, EngineError> {
        if plan.stream.offset.is_some() {
            tracing::info!("queries with OFFSET don't resume yet; starting over");
            return Ok(None);
        }
        let Some(cursor) = cursor::decode(plan, token) else {
            tracing::info!("resume cursor is for another plan or malformed; starting over");
            return Ok(None);
        };
        if let Some(w) = snapshot {
            if let Some((p, o)) = w.0.iter().find(|(p, o)| cursor.get(**p) < **o) {
                tracing::info!(
                    cursor = %cursor.describe(),
                    partition = p,
                    snapshot = o,
                    "resume cursor is behind the snapshot; starting over"
                );
                return Ok(None);
            }
        }
        for topic in &plan.stream.topics {
            let Some(bounds) = self.source.offset_bounds(topic).await? else {
                continue;
            };
            for (p, c) in &cursor.0 {
                let (low, high) = bounds.get(p).copied().unwrap_or((0, 0));
                // the first offset the resume reads from Kafka in this partition: right after
                // the snapshot when rebuilding a state, right after the cursor otherwise
                let first = match snapshot {
                    Some(w) => w.get(*p) + 1,
                    None => *c + 1,
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
        lease: Option<Lease>,
        first: First,
        at: Watermark,
        events: BoxStream<'static, Event>,
    ) -> BoxStream<'static, Result<Iteration, EngineError>> {
        enum Item {
            History(Result<Vec<Row>, HistoryError>),
            CatchUp(Result<Option<Vec<Row>>, HistoryError>, Vec<Event>),
            Event(Event),
            Failed(EngineError),
        }
        let history = self.history.clone();
        // the first items, then the rest of the stream
        let items = async move {
            match first {
                First::History(sql) => {
                    let rows = history.fetch(&sql).await;
                    drop(lease); // done with the snapshot
                    stream::iter([Item::History(rows)])
                        .chain(events.map(Item::Event))
                        .boxed()
                }
                First::CatchUp { seed, ahead } => {
                    let fetched = match seed {
                        Seed::Keys(sql) | Seed::Count(sql) => history.fetch(&sql).await.map(Some),
                        Seed::Nothing => Ok(None),
                    };
                    drop(lease);
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
    /// The whole result, from a snapshot.
    History(String),
    /// Resume: what to seed the state with (from a snapshot), and where the client's own events
    /// after the snapshot end.
    CatchUp {
        seed: Seed,
        /// Per partition, the client's cursor where it is past the snapshot.
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
pub(crate) enum ResultState {
    /// Unordered, unlimited: every row is appended.
    Append,
    /// LIMIT without ORDER BY: the latest N rows.
    Window(WindowState),
    /// ORDER BY (and maybe LIMIT): the sort keys of the result, in tie groups.
    Ordered(OrderedState),
    /// ORDER BY with OFFSET: the sort keys of the top `m + n`, and the rows above the result.
    Offset(OffsetState),
    /// OFFSET without ORDER BY: the newest `m` rows held back, the window before them.
    Held(HeldWindow),
}

impl ResultState {
    pub(crate) fn for_plan(plan: &StreamPlan) -> Self {
        match (&plan.order, plan.limit, plan.offset) {
            (Some(o), limit, Some(m)) => Self::Offset(OffsetState::new(o.keys.clone(), m, limit)),
            (None, limit, Some(m)) => Self::Held(HeldWindow::new(m, limit)),
            (Some(o), limit, None) => {
                Self::Ordered(OrderedState::with_limit(o.keys.clone(), limit))
            }
            (None, Some(n), None) => Self::Window(WindowState::new(n)),
            (None, None, None) => Self::Append,
        }
    }

    /// Where a new row goes: an insert (at an index with ORDER BY) and the delete it causes, or
    /// nothing when it falls outside a LIMIT. `key` is its sort key with ORDER BY.
    pub(crate) fn place(&mut self, row: Row, key: Option<SortKey>) -> Vec<Diff> {
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
            ResultState::Offset(s) => s.insert(key.expect("a sort key"), row),
            ResultState::Held(w) => w.push(row),
        }
    }
}

pub(crate) fn bad_history_row(e: impl std::fmt::Display) -> EngineError {
    HistoryError::ClickHouse(format!("history row: {e}")).into()
}

/// The history as one iteration. Seeds the state: with ORDER BY the sort keys, in ClickHouse's
/// order (dropping the hidden ORDER BY fields from the rows); with only a LIMIT the row count.
pub(crate) fn history_iteration(
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
        (ResultState::Offset(state), Some(order)) => {
            // the rows above the result are kept, not sent
            let mut shown = Vec::new();
            for mut row in rows {
                let key = order.history_key(&mut row).map_err(bad_history_row)?;
                shown.extend(state.push_history(key, row));
            }
            rows = shown;
        }
        (ResultState::Held(w), _) => rows = w.history(rows),
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
    fetched: Result<Option<Vec<Row>>, HistoryError>,
    own: Vec<Event>,
) -> Result<Iteration, EngineError> {
    let seed = fetched?;
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
    // the client's own events after the snapshot: into the state, not sent
    for event in own {
        if let Ok(row) = plan.decode(&event.value) {
            if plan.matches(&row) {
                let key = plan.order.as_ref().map(|o| o.event_key(&row));
                state.place(plan.project(&row), key);
            }
        }
    }
    // the client has everything up to its cursor: what it's missing comes from the stream
    Ok(Iteration {
        origin: Origin::CatchUp,
        diffs: Vec::new(),
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
    /// Where history ends and the stream begins: the snapshot's watermark (or, when resumed
    /// without one, the client's cursor).
    pub watermark: Watermark,
    /// The history query as run, against the snapshot (or, when resumed, its seed query; empty
    /// without one).
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
