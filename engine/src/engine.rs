//! The engine's entry point: [`Engine::explain`] plans a query, [`Engine::subscribe`] runs it.
//!
//! A subscription yields the history from ClickHouse first, then one [`Iteration`] per event of
//! the topics the query needs, each carrying the diffs it produced.

use std::collections::BTreeMap;
use std::fmt;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

use futures::stream::{self, BoxStream, Stream, StreamExt};

use crate::aggregate::chdb_run::{AggregateRun, ChdbTable};
use crate::aggregate::native_run::NativeRun;
use crate::binder::{bind, BindError, Catalog, Params};
use crate::chdb::{Chdb, ChdbError};
use crate::checkpoint::{Checkpoint, Checkpointer};
use crate::cursor;
use crate::diff::{Diff, Row};
use crate::history::{ClickHouseHttp, HistoryError, HistorySource};
use crate::keyed::{KeyedState, NewVersion, Refill};
use crate::order::{HeldWindow, OffsetState, OrderedState, SortKey, WindowState};
use crate::parser::parse;
use crate::plan::{plan_with, AggregatePlan, DecodedRow, Plan, StreamPlan};
use crate::rules::{ConfigError, ConfiguredRules, NamingRules, TablesConfig, TopicId};
use crate::snapshot::{
    ClickHouseCloner, InUse, Lease, PauseServer, SnapshotConfig, SnapshotError, SnapshotManager,
    Snapshots,
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
    /// A checkpoint that doesn't fit the query (another shape, a bad format).
    #[error("checkpoint: {0}")]
    Checkpoint(String),
}

/// Everything [`Engine::connect`] needs for the standard setup: ClickHouse over HTTP (history,
/// snapshots, and the states chDB merges aggregates from), Kafka, snapshots through the pause
/// server, and the table mapping.
///
/// `clickhouse_url` must reach the node snapshots are made on: they exist only there.
#[derive(Debug, Clone)]
pub struct Config {
    pub clickhouse_url: String,
    /// Reads history (needs SELECT on the tables and their snapshots).
    pub user: String,
    pub password: String,
    /// Which tables queries can use, and where they live (ClickHouse, topics, columns).
    pub tables: TablesConfig,
    /// Kafka bootstrap servers, e.g. `127.0.0.1:9092`.
    pub kafka_brokers: String,
    /// librdkafka properties for every Kafka client (TLS, SASL, …).
    pub kafka_properties: BTreeMap<String, String>,
    /// How often topics' partitions are reloaded.
    pub metadata_refresh: Duration,
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
            clickhouse_url: "http://127.0.0.1:8123".into(),
            user: "default".into(),
            password: String::new(),
            tables: TablesConfig::default(),
            kafka_brokers: "127.0.0.1:9092".into(),
            kafka_properties: BTreeMap::new(),
            metadata_refresh: crate::source::METADATA_REFRESH,
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
    /// Where chDB reads aggregate states from (main ClickHouse, over HTTP).
    states_from: HttpSource,
    max_batch: usize,
    /// How often a semi-join looks for a newer snapshot of its outer table.
    semi_refresh: Duration,
    /// FINAL with LIMIT: how many rows past the window are kept to replace departures.
    final_buffer: u64,
}

#[derive(Debug, Clone)]
struct HttpSource {
    url: String,
    user: String,
    password: String,
}

impl Engine {
    /// The standard setup from a [`Config`]; fails on an invalid table mapping. Starts the
    /// snapshot task, so it must run inside a tokio runtime.
    pub fn connect(config: Config) -> Result<Self, ConfigError> {
        let rules = ConfiguredRules::new(config.tables)?;
        let semi_refresh = config.snapshots.refresh;
        let snapshots = SnapshotManager::start(
            Arc::new(PauseServer::new(&config.pause_server_url)),
            Arc::new(ClickHouseCloner::new(
                &config.clickhouse_url,
                &config.snapshot_user,
                &config.snapshot_password,
            )),
            config.snapshots,
        );
        Ok(Self::new(
            rules,
            ClickHouseHttp::new(&config.clickhouse_url, &config.user, &config.password),
            KafkaSource::with_options(
                &config.kafka_brokers,
                config.kafka_properties,
                config.metadata_refresh,
            ),
            snapshots,
        )
        .with_semi_refresh(semi_refresh)
        .with_clickhouse(&config.clickhouse_url, &config.user, &config.password))
    }

    /// Main ClickHouse over HTTP, which chDB reads aggregate states from (the history's
    /// node); default `http://127.0.0.1:8123` as `default` without a password.
    pub fn with_clickhouse(mut self, url: &str, user: &str, password: &str) -> Self {
        self.states_from = HttpSource {
            url: url.into(),
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
            states_from: HttpSource {
                url: "http://127.0.0.1:8123".into(),
                user: "default".into(),
                password: String::new(),
            },
            max_batch: 1000,
            semi_refresh: Duration::from_secs(10),
            final_buffer: crate::keyed::BUFFER,
        }
    }

    /// FINAL with LIMIT: how many rows past the window are kept to replace departures (default
    /// 2000); a refill reads twice that from a newer snapshot when departures drain them.
    pub fn with_final_buffer(mut self, rows: u64) -> Self {
        self.final_buffer = rows.max(1);
        self
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

    /// The snapshots' state, per table (for the debug page).
    pub async fn snapshot_status(&self) -> Vec<crate::snapshot::SnapshotStatus> {
        self.snapshots.status().await
    }

    /// Parse, bind and plan a query without running it.
    pub async fn explain(&self, sql: &str) -> Result<Plan, EngineError> {
        self.explain_with(sql, &Params::new()).await
    }

    /// [`explain`](Self::explain) with the values of the query's parameters (`{name:Type}`).
    pub async fn explain_with(&self, sql: &str, params: &Params) -> Result<Plan, EngineError> {
        let query = parse(sql).map_err(|e| EngineError::Syntax(e.render(sql)))?;
        let catalog = Catalog {
            rules: self.rules.as_ref(),
            history: self.history.as_ref(),
            source: self.source.as_ref(),
            params,
        };
        let bound = bind(&query, &catalog).await.map_err(|e| match e {
            BindError::History(e) => EngineError::History(e),
            BindError::Source(e) => EngineError::Source(e),
            e => EngineError::Bind(e.render(sql)),
        })?;
        Ok(plan_with(&bound, self.final_buffer))
    }

    /// Plan a query and start it: the history first, then one iteration per event of the
    /// plan's topics.
    ///
    /// History is read from a snapshot of the table (see [`crate::snapshot`]): a clone holding
    /// every offset up to its [`Watermark`], and each partition is consumed from right after
    /// it. So every event is in exactly one of them, wherever ClickHouse's ingestion is at.
    pub async fn subscribe(&self, sql: &str) -> Result<Subscription, EngineError> {
        self.subscribe_with(sql, &Params::new(), None).await
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
        self.subscribe_with(sql, &Params::new(), since).await
    }

    /// [`subscribe_from`](Self::subscribe_from) with the values of the query's parameters
    /// (`{name:Type}`). They're bound as literals, so a cursor taken with other values doesn't
    /// resume (its plan differs).
    pub async fn subscribe_with(
        &self,
        sql: &str,
        params: &Params,
        since: Option<&str>,
    ) -> Result<Subscription, EngineError> {
        let plan = self.explain_with(sql, params).await?;
        // its tables are in use, their snapshots kept fresh, while it runs
        let mut in_use = vec![self.snapshots.in_use(&plan.source)];
        if let Some(semi) = &plan.semi {
            in_use.push(self.snapshots.in_use(&semi.inner));
        }
        let mut subscription = self.subscribe_plan(plan, since).await?;
        subscription.in_use = in_use;
        Ok(subscription)
    }

    /// Continue a subscription from its checkpoint: the plan re-made from the query, its state
    /// as saved, and Kafka from right after the checkpoint's cursor. No snapshot and no history:
    /// the first iteration is an empty catch-up at that cursor (whoever saved the checkpoint
    /// keeps the rows). Fails when the checkpoint doesn't fit the query.
    pub async fn restore(
        &self,
        sql: &str,
        params: &Params,
        checkpoint: &Checkpoint,
    ) -> Result<Subscription, EngineError> {
        let plan = self.explain_with(sql, params).await?;
        let bad = EngineError::Checkpoint;
        let in_use = {
            let mut g = vec![self.snapshots.in_use(&plan.source)];
            if let Some(semi) = &plan.semi {
                g.push(self.snapshots.in_use(&semi.inner));
            }
            g
        };
        let at = checkpoint.at.clone();
        let mut subscription = match (&plan.aggregate, &plan.semi) {
            (None, None) => {
                let mut state =
                    ResultState::restore(&plan.stream, &checkpoint.state).map_err(bad)?;
                if let ResultState::Keyed(k) = &mut state {
                    k.restored_at(&at);
                }
                self.start(
                    plan,
                    None,
                    at.clone(),
                    at,
                    First::Restored,
                    String::new(),
                    true,
                    Some(state),
                )
                .await?
            }
            (Some(aggregate), None) if aggregate.native.is_some() => {
                let aggregate = aggregate.clone();
                self.restore_native(plan, aggregate, checkpoint).await?
            }
            _ => return Err(bad("this query's results have no checkpoints".into())),
        };
        subscription.in_use = in_use;
        Ok(subscription)
    }

    /// A native aggregate from its checkpoint (see [`Engine::restore`]).
    async fn restore_native(
        &self,
        plan: Plan,
        aggregate: AggregatePlan,
        checkpoint: &Checkpoint,
    ) -> Result<Subscription, EngineError> {
        let at = checkpoint.at.clone();
        let mut starts = Vec::new();
        for topic in &plan.stream.topics {
            starts.push(self.topic_start(topic, &at).await?);
        }
        let events = self
            .source
            .subscribe(&starts, &plan.stream.required_keys())
            .await?
            .ready_chunks(self.max_batch)
            .boxed();
        let native = aggregate.native.clone().expect("a native aggregate");
        let run = NativeRun::restored(
            self.history.clone(),
            native,
            plan.stream.clone(),
            events,
            checkpoint,
            aggregate.result(),
        )
        .map_err(EngineError::Checkpoint)?;
        tracing::info!(at = %at.describe(), "restored (aggregate, native)");
        let checkpointer = run.checkpointer();
        let iterations = stream::unfold(run, |mut run| async move {
            let next = run.step().await?;
            Some((next, run))
        })
        .boxed();
        Ok(Subscription {
            plan,
            watermark: at,
            history_sql: String::new(),
            resumed: true,
            snapshot: None,
            iterations,
            in_use: Vec::new(),
            checkpointer: Some(checkpointer),
        })
    }

    /// How many events a restore from `at` would read before it's up to date, or `None` if it
    /// can't (an offset no longer in the topic, or past its end: the topic was recreated).
    /// `Some(0)` when the source doesn't know its bounds.
    pub async fn catch_up_events(
        &self,
        plan: &Plan,
        at: &Watermark,
    ) -> Result<Option<u64>, EngineError> {
        let mut total = 0u64;
        for topic in &plan.stream.topics {
            let Some(bounds) = self.source.offset_bounds(topic).await? else {
                continue;
            };
            for (p, (low, high)) in &bounds {
                let next = at.get(*p) + 1;
                if next < *low || next > *high {
                    return Ok(None);
                }
                total += (*high - next) as u64;
            }
        }
        Ok(Some(total))
    }

    async fn subscribe_plan(
        &self,
        plan: Plan,
        since: Option<&str>,
    ) -> Result<Subscription, EngineError> {
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
            || plan.stream.offset.is_some()
            || plan.stream.keyed.is_some();
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
                        None,
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
        self.start(
            plan,
            Some(lease),
            begin,
            at,
            first,
            history_sql,
            resumed,
            None,
        )
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
        let used = outer.used();
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
            snapshot: Some(used),
            iterations,
            in_use: Vec::new(),
            checkpointer: None,
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
        restored: Option<ResultState>,
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
        let state = match &first {
            First::Restored => restored.expect("a restored state"),
            _ => ResultState::for_plan(&plan.stream),
        };
        let snapshot = lease.as_ref().map(|l| l.used());
        let (iterations, checkpointer) = self.iterations(&plan, lease, first, at, events, state);
        Ok(Subscription {
            plan,
            watermark: begin,
            history_sql,
            resumed,
            snapshot,
            iterations,
            in_use: Vec::new(),
            checkpointer: Some(checkpointer),
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
        let used = lease.used();
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
        let (history_sql, iterations, checkpointer) = match (
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
                let checkpointer = run.checkpointer();
                let iterations = stream::unfold(run, |mut run| async move {
                    let next = run.step().await?;
                    Some((next, run))
                })
                .boxed();
                (sql, iterations, Some(checkpointer))
            }
            _ => {
                static NEXT: AtomicU64 = AtomicU64::new(1);
                let chdb = Chdb::shared()?;
                let table = format!(
                    "chlive_agg_{}_{}",
                    std::process::id(),
                    NEXT.fetch_add(1, AtomicOrdering::Relaxed)
                );
                // what ClickHouse runs on the snapshot; chDB reads its result over HTTP
                let states = aggregate.states_sql(&from);
                let source = AggregatePlan::http_source(
                    &self.states_from.url,
                    &states,
                    &self.states_from.user,
                    &self.states_from.password,
                );
                let create = aggregate.create_sql(&table, &source);
                let logged = match self.states_from.password.as_str() {
                    "" => create.clone(),
                    p => create.replace(p, "***"),
                };
                tracing::info!(table, create = %logged, "subscribed (aggregate, chDB)");
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
                (states, iterations, None)
            }
        };
        Ok(Subscription {
            plan,
            watermark,
            history_sql,
            resumed: false,
            snapshot: Some(used),
            iterations,
            in_use: Vec::new(),
            checkpointer,
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
        if plan.stream.keyed.is_some() {
            tracing::info!("FINAL queries don't resume yet; starting over");
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
        state: ResultState,
    ) -> (
        BoxStream<'static, Result<Iteration, EngineError>>,
        Checkpointer,
    ) {
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
                First::Restored => {
                    drop(lease);
                    stream::iter([Item::Restored])
                        .chain(events.map(Item::Event))
                        .boxed()
                }
            }
        };
        let stream_plan = plan.stream.clone();
        // shared with the checkpointer: each item updates it before it's yielded, so between
        // iterations it matches the last one's cursor
        let shared = Arc::new(Mutex::new((state, at)));
        let checkpointer: Checkpointer = {
            let shared = shared.clone();
            Arc::new(move || {
                let (state, at) = &*shared.lock().unwrap();
                Checkpoint {
                    at: at.clone(),
                    state: state.save(),
                }
            })
        };
        // FINAL with LIMIT: refills from a newer snapshot when departures drain the buffer,
        // fetched between two events
        let refill = plan
            .stream
            .keyed
            .as_ref()
            .and_then(|k| k.window.map(|w| (k.clone(), w)))
            .filter(|(_, w)| w.limit.is_some_and(|n| n > 0))
            .map(|(keyed, w)| RefillSource {
                snapshots: self.snapshots.clone(),
                history: self.history.clone(),
                source: self.source.clone(),
                table: plan.source.clone(),
                plan: plan.history.clone(),
                stream: plan.stream.clone(),
                keyed,
                limit: w.refill_limit(),
            });
        let items = stream::once(items).flatten().boxed();
        let iterations = stream::unfold((items, false), move |(items, wanted)| {
            let (shared, stream_plan, refill) = (shared.clone(), stream_plan.clone(), refill.clone());
            async move {
                if let (true, Some(r)) = (wanted, &refill) {
                    // with ORDER BY: rebuilt from the snapshot and the events since
                    let rebuild = match &*shared.lock().unwrap() {
                        (ResultState::Keyed(k), at) if k.rebuilds() => {
                            Some((k.last_refill(), at.clone(), k.fresh()))
                        }
                        _ => None,
                    };
                    if let Some((last, position, shadow)) = rebuild {
                        let rebuilt = r.rebuild(last.as_ref(), &position, shadow).await;
                        let it = {
                            let mut guard = shared.lock().unwrap();
                            let (state, at) = &mut *guard;
                            match state {
                                ResultState::Keyed(k) => {
                                    let diffs = match rebuilt {
                                        Ok(Some((shadow, from))) => k.rebuilt(shadow, from),
                                        Ok(_) => {
                                            k.refill_failed();
                                            Vec::new()
                                        }
                                        Err(e) => {
                                            tracing::warn!(error = %e, "rebuilding a FINAL window failed; trying again later");
                                            k.refill_failed();
                                            Vec::new()
                                        }
                                    };
                                    (!diffs.is_empty()).then(|| Iteration {
                                        origin: Origin::Refill,
                                        diffs,
                                        at: at.clone(),
                                    })
                                }
                                _ => None,
                            }
                        };
                        if let Some(it) = it {
                            return Some((Ok(it), (items, false)));
                        }
                        return next_item(items, &shared, &stream_plan, &refill).await;
                    }
                    let last = match &*shared.lock().unwrap() {
                        (ResultState::Keyed(k), _) => k.last_refill(),
                        _ => None,
                    };
                    let fetched = r.fetch(last.as_ref()).await;
                    let mut guard = shared.lock().unwrap();
                    let (state, at) = &mut *guard;
                    if let ResultState::Keyed(k) = state {
                        let diffs = match fetched {
                            Ok(Some(fetched)) => k.refill(fetched, at),
                            // the same snapshot as the last refill: wait for a newer one
                            Ok(None) => {
                                k.refill_failed();
                                Vec::new()
                            }
                            Err(e) => {
                                tracing::warn!(error = %e, "refilling a FINAL window failed; trying again later");
                                k.refill_failed();
                                Vec::new()
                            }
                        };
                        if !diffs.is_empty() {
                            let more = k.wants_refill();
                            let it = Iteration {
                                origin: Origin::Refill,
                                diffs,
                                at: at.clone(),
                            };
                            return Some((Ok(it), (items, more)));
                        }
                    }
                }
                next_item(items, &shared, &stream_plan, &refill).await
            }
        })
        .boxed();
        (iterations, checkpointer)
    }
}

/// What a subscription's stream is made of, before it becomes iterations.
enum Item {
    History(Result<Vec<Row>, HistoryError>),
    CatchUp(Result<Option<Vec<Row>>, HistoryError>, Vec<Event>),
    Restored,
    Event(Event),
    Failed(EngineError),
}

type Shared = Arc<Mutex<(ResultState, Watermark)>>;
type ItemStream = BoxStream<'static, Item>;

/// The next item as an iteration (`None` at the end), and whether the state now wants a refill.
async fn next_item(
    mut items: ItemStream,
    shared: &Shared,
    stream_plan: &StreamPlan,
    refill: &Option<RefillSource>,
) -> Option<(Result<Iteration, EngineError>, (ItemStream, bool))> {
    let item = items.next().await?;
    let mut guard = shared.lock().unwrap();
    let (state, at) = &mut *guard;
    let it = match item {
        Item::History(rows) => history_iteration(stream_plan, state, at, rows),
        Item::CatchUp(fetched, own) => catch_up_iteration(stream_plan, state, at, fetched, own),
        Item::Restored => Ok(Iteration {
            origin: Origin::CatchUp,
            diffs: Vec::new(),
            at: at.clone(),
        }),
        Item::Event(event) => Ok(event_iteration(stream_plan, state, at, event)),
        Item::Failed(e) => Err(e),
    };
    let wants = match state {
        ResultState::Keyed(k) => refill.is_some() && k.wants_refill(),
        _ => false,
    };
    Some((it, (items, wants)))
}

/// Where a FINAL window's refills come from: the table's current snapshot, read with a bigger
/// LIMIT.
#[derive(Clone)]
struct RefillSource {
    snapshots: Arc<dyn Snapshots>,
    history: Arc<dyn HistorySource>,
    source: Arc<dyn EventSource>,
    table: crate::snapshot::SnapshotTable,
    plan: crate::plan::HistoryPlan,
    stream: StreamPlan,
    keyed: crate::plan::KeyedPlan,
    limit: u64,
}

impl RefillSource {
    /// With ORDER BY: `shadow` (an empty window) rebuilt from the current snapshot's top and
    /// the events since it, up to `position`, re-read from Kafka; with the snapshot's watermark.
    /// `None` if the snapshot is still `last`'s, or past `position` (the stream gets there
    /// first).
    async fn rebuild(
        &self,
        last: Option<&Watermark>,
        position: &Watermark,
        empty: KeyedState,
    ) -> Result<Option<(KeyedState, Watermark)>, EngineError> {
        let lease = self.snapshots.acquire(&self.table).await?;
        let from = lease.watermark.clone();
        if last == Some(&from) || !position.covers(&from) {
            return Ok(None);
        }
        // the events since the snapshot, as the stream had them
        let ahead: std::collections::BTreeMap<i32, i64> = position
            .0
            .iter()
            .filter(|(p, o)| **o > from.get(**p))
            .map(|(p, o)| (*p, *o))
            .collect();
        let mut since = Vec::new();
        if !ahead.is_empty() {
            let starts = [TopicStart {
                topic: self.stream.topics[0].clone(),
                partitions: ahead.keys().map(|p| (*p, from.start(*p))).collect(),
            }];
            let mut events = self
                .source
                .subscribe(&starts, &self.stream.required_keys())
                .await?;
            since = read_ahead(&mut events, &Watermark(ahead)).await?.0;
        }
        // the snapshot's top, then those events; if departures among them leave too few rows,
        // a bigger top (until enough, or all of them)
        let mut limit = self.limit;
        loop {
            let rows = self
                .history
                .fetch(&self.plan.sql_limit(&lease.sql_table(), limit))
                .await?;
            let all = (rows.len() as u64) < limit;
            let mut empty = empty.clone();
            empty.history_limit(limit);
            let mut shadow = ResultState::Keyed(empty);
            let mut at = from.clone();
            history_iteration(&self.stream, &mut shadow, &at, Ok(rows))?;
            for event in &since {
                event_iteration(&self.stream, &mut shadow, &mut at, event.clone());
            }
            let ResultState::Keyed(mut shadow) = shadow else {
                unreachable!("a keyed shadow")
            };
            shadow.settle();
            // enough, or every row there is (a bigger top can't do better)
            if !shadow.short() || all {
                return Ok(Some((shadow, from)));
            }
            limit = limit.saturating_mul(4);
        }
    }

    /// From the table's current snapshot; `None` if it's still `last`'s.
    async fn fetch(&self, last: Option<&Watermark>) -> Result<Option<Refill>, EngineError> {
        let lease = self.snapshots.acquire(&self.table).await?;
        if last == Some(&lease.watermark) {
            return Ok(None);
        }
        let mut rows = self
            .history
            .fetch(&self.plan.sql_limit(&lease.sql_table(), self.limit))
            .await?;
        let complete = (rows.len() as u64) < self.limit;
        let rows = rows
            .iter_mut()
            .map(|row| {
                let (id, ver) = self.keyed.history_row(row).map_err(bad_history_row)?;
                Ok((id, ver, std::mem::take(row)))
            })
            .collect::<Result<_, EngineError>>()?;
        Ok(Some(Refill {
            rows,
            at: lease.watermark.clone(),
            complete,
        }))
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
    /// Restored from a checkpoint: the state is seeded, the stream continues after it.
    Restored,
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
    /// FINAL on a ReplacingMergeTree: the result's keys and their versions.
    Keyed(KeyedState),
}

impl ResultState {
    /// Its state, for a checkpoint: `{"shape": …, …}`.
    pub(crate) fn save(&self) -> serde_json::Value {
        let (shape, mut state) = match self {
            ResultState::Append => ("append", serde_json::json!({})),
            ResultState::Window(w) => ("window", w.save()),
            ResultState::Ordered(o) => ("ordered", o.save()),
            ResultState::Offset(o) => ("offset", o.save()),
            ResultState::Held(h) => ("held", h.save()),
            ResultState::Keyed(k) => ("keyed", k.save()),
        };
        state["shape"] = shape.into();
        state
    }

    /// The plan's state, as saved: the shape must be the plan's.
    pub(crate) fn restore(plan: &StreamPlan, saved: &serde_json::Value) -> Result<Self, String> {
        let mut state = Self::for_plan(plan);
        let want = state.save()["shape"]
            .as_str()
            .unwrap_or_default()
            .to_string();
        let got = saved["shape"].as_str().unwrap_or_default();
        if got != want {
            return Err(format!("checkpoint of a {got} result for a {want} query"));
        }
        match &mut state {
            ResultState::Append => {}
            ResultState::Window(w) => w.load(saved)?,
            ResultState::Ordered(o) => o.load(saved)?,
            ResultState::Offset(o) => o.load(saved)?,
            ResultState::Held(h) => h.load(saved)?,
            ResultState::Keyed(k) => k.load(plan.keyed.as_ref().expect("a keyed plan"), saved)?,
        }
        Ok(state)
    }

    pub(crate) fn for_plan(plan: &StreamPlan) -> Self {
        if let Some(keyed) = &plan.keyed {
            let order = plan.order.as_ref().map(|o| o.keys.clone());
            return Self::Keyed(match keyed.window {
                Some(w) => KeyedState::with_window(w, order),
                None => KeyedState::new(order),
            });
        }
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
            // newest first: in at the top; once full, the last row goes
            ResultState::Window(w) => match w.push() {
                None => Vec::new(),
                Some(full) => {
                    let mut diffs = vec![Diff::Insert {
                        row,
                        index: Some(0),
                        id: None,
                    }];
                    if full {
                        diffs.push(Diff::Delete {
                            index: w.limit() as usize,
                        });
                    }
                    diffs
                }
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
            ResultState::Keyed(_) => unreachable!("FINAL rows are replaced, not placed"),
        }
    }

    /// A decoded event's diffs: a new row placed (when it matches the WHERE), or, FINAL, a new
    /// version of its key's row.
    pub(crate) fn event(
        &mut self,
        plan: &StreamPlan,
        row: &DecodedRow,
        at: (i32, i64),
    ) -> Vec<Diff> {
        match (self, &plan.keyed) {
            (ResultState::Keyed(state), Some(keyed)) => {
                let (id, ver, deleted) = keyed.event(row);
                let key = plan
                    .order
                    .as_ref()
                    .map(|o| o.event_key(row))
                    .unwrap_or_default();
                let v = NewVersion {
                    id,
                    ver,
                    key,
                    deleted,
                    matches: plan.matches(row),
                    at,
                };
                state.event(v, || plan.project(row))
            }
            (state, _) if plan.matches(row) => {
                let key = plan.order.as_ref().map(|o| o.event_key(row));
                state.place(plan.project(row), key)
            }
            _ => Vec::new(),
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
        // newest first: the history's arrival order (ClickHouse's) reversed
        (ResultState::Window(w), _) => {
            rows.iter().for_each(|_| w.push_history());
            rows.reverse();
        }
        (ResultState::Offset(state), Some(order)) => {
            // the rows above the result are kept, not sent
            let mut shown = Vec::new();
            for mut row in rows {
                let key = order.history_key(&mut row).map_err(bad_history_row)?;
                shown.extend(state.push_history(key, row));
            }
            rows = shown;
        }
        (ResultState::Held(w), _) => {
            rows = w.history(rows);
            rows.reverse();
        }
        (ResultState::Keyed(state), order) => {
            let keyed = plan.keyed.as_ref().expect("a keyed plan");
            let rows = rows
                .into_iter()
                .map(|mut row| {
                    let key = match order {
                        Some(o) => o.history_key(&mut row).map_err(bad_history_row)?,
                        None => Vec::new(),
                    };
                    let (id, ver) = keyed.history_row(&mut row).map_err(bad_history_row)?;
                    Ok((id, ver, key, row))
                })
                .collect::<Result<_, EngineError>>()?;
            let diffs = state.history(rows);
            return Ok(Iteration {
                origin: Origin::History,
                diffs,
                at: at.clone(),
            });
        }
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
            state.event(plan, &row, (event.partition, event.offset));
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
    let mut diffs = match plan.decode(&event.value) {
        Ok(row) => state.event(plan, &row, (event.partition, event.offset)),
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
    // a FINAL window's refill, waiting for the stream to reach its snapshot
    if let ResultState::Keyed(k) = state {
        diffs.extend(k.advanced(at));
    }
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
    /// The snapshot its history was read from (none when resumed or restored).
    pub snapshot: Option<crate::snapshot::UsedSnapshot>,
    iterations: BoxStream<'static, Result<Iteration, EngineError>>,
    /// Its tables' [`InUse`] guards.
    in_use: Vec<InUse>,
    /// Reads its placement state, for the shapes that can be checkpointed.
    checkpointer: Option<Checkpointer>,
}

impl Subscription {
    /// Its placement state now, complete to the cursor of the last iteration it yielded (call it
    /// between iterations; `None` for shapes without checkpoints: chDB aggregates, semi-joins).
    /// [`Engine::restore`] continues from it.
    pub fn checkpoint(&self) -> Option<Checkpoint> {
        self.checkpointer.as_ref().map(|c| c())
    }

    /// Whether [`Subscription::checkpoint`] gives one.
    pub fn checkpointable(&self) -> bool {
        self.checkpointer.is_some()
    }
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
    /// FINAL with LIMIT: rows from a newer snapshot replacing departures.
    Refill,
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
            Origin::Refill => {
                for d in &self.diffs {
                    writeln!(f, "{d}")?;
                }
                write!(f, "refilled: {} changes", self.diffs.len())
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
