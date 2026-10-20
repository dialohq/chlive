//! Shared results: one entry per distinct query (and parameter values), shared by every
//! subscription to it, on any connection.
//!
//! An entry runs one engine subscription and keeps its result (the rows, and the cursor they're
//! complete to), so a new subscriber gets the rows at once, without ClickHouse. It also keeps a
//! log of its recent `diff` messages: a client returning with a cursor from this entry (the same
//! *lineage*) gets just the diffs it missed.
//!
//! Stages, with a disk store (`cache.dir`; see [`crate::store`]) for the shapes that have
//! checkpoints (plain queries, native aggregates):
//!
//! | stage | follows Kafka | rows and log | engine state |
//! |---|---|---|---|
//! | live (subscribed, then `keep_alive`) | yes | memory, mirrored to disk | memory |
//! | warm (`keep_warm`) | yes | disk only | memory |
//! | cold | no | disk | disk (checkpoint) |
//! | gone (evicted by `disk_budget`) | no | — | — |
//!
//! A subscriber brings a warm entry back to live (its rows loaded from disk), and a cold one
//! (restored from its checkpoint, continuing from Kafka; no ClickHouse). Without a store, or for
//! other shapes (chDB aggregates, semi-joins), an entry is live, then dropped.
//!
//! Lineage: an id made when an entry starts from a ClickHouse snapshot, carried in its cursors.
//! Two entries of one query can hold different rows at the same cursor (ties, arbitrary order,
//! OFFSET's hidden rows), so a log is only replayed for a cursor of the same lineage. A restored
//! entry is a new lineage that *forks* from the old one at its checkpoint: old cursors up to it
//! still resume.
//!
//! Each entry is a task owning all of its state; joins and leaves arrive as messages, between
//! iterations, so every snapshot it sends is consistent with its cursor.

use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use chlive_engine::cursor::{self, Cursor};
use chlive_engine::diff::{Diff, ResultList};
use chlive_engine::plan::Plan;
use chlive_engine::watermark::Watermark;
use chlive_engine::{
    Checkpoint, Engine, EngineError, Filters, Iteration, Origin, Params, Subscription,
};
use futures::stream::ReadyChunks;
use futures::StreamExt;
use serde::Serialize;
use serde_json::Value as Json;
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, info, trace, warn};

use crate::protocol::{change, engine_error, order_terms, Column, OrderTerm, ServerMsg};
use crate::store::{self, Meta, Store, Stored};

/// Iterations already waiting are sent together, in one `diff`, up to this many.
pub const MAX_ITERATIONS_PER_MESSAGE: usize = 256;

/// The `[cache]` settings.
#[derive(Debug, Clone, PartialEq)]
pub struct CacheSettings {
    /// The disk store; `None`: no warm or cold stages (live, then dropped).
    pub dir: Option<PathBuf>,
    /// After the last subscriber leaves, the entry stays live (in memory) this long.
    pub keep_alive: Duration,
    /// Then warm (following Kafka, rows on disk) this long, then cold.
    pub keep_warm: Duration,
    /// Entries warm at most (their Kafka consumers); the one warm the longest goes cold first.
    pub max_warm: usize,
    /// `diff` messages kept in memory per live entry, for resuming clients.
    pub log_length: usize,
    /// Cold entries are evicted, least recently used first, beyond this many bytes on disk.
    pub disk_budget: u64,
    /// A stored result whose snapshot is older than this is rebuilt from ClickHouse.
    pub max_age: Duration,
    /// A stored result more events than this behind Kafka is rebuilt from ClickHouse.
    pub max_catch_up: u64,
    /// How often running entries save their engine state.
    pub checkpoint_interval: Duration,
    /// An entry's log on disk past this many bytes is folded into its base.
    pub log_bytes: u64,
}

impl Default for CacheSettings {
    fn default() -> Self {
        Self {
            dir: None,
            keep_alive: Duration::from_secs(30),
            keep_warm: Duration::from_secs(3600),
            max_warm: 100,
            log_length: 1000,
            disk_budget: 10 << 30,
            max_age: Duration::from_secs(24 * 3600),
            max_catch_up: 1_000_000,
            checkpoint_interval: Duration::from_secs(60),
            log_bytes: 16 << 20,
        }
    }
}

/// What a connection's writer sends: its own messages, or an entry's, serialized once for every
/// subscriber, with the subscriber's id put in when written.
pub enum Outgoing {
    Msg(ServerMsg),
    Shared {
        id: String,
        body: Arc<str>,
    },
    /// From an entry to one subscription, sent only while its gate is open: a subscription
    /// moved to another entry (its row filters changed) gets nothing more from the old one,
    /// even what's already queued.
    Gated {
        open: Arc<AtomicBool>,
        msg: Box<Outgoing>,
    },
}

/// The id a shared body is serialized with; replaced by each subscriber's own.
const PLACEHOLDER: &str = "\u{1}chlive-subscription\u{1}";

impl Outgoing {
    /// The message's text; none when its gate has closed.
    pub fn render(&self) -> Option<String> {
        Some(match self {
            Outgoing::Msg(m) => serde_json::to_string(m).expect("server messages serialize"),
            Outgoing::Shared { id, body } => {
                let placeholder = serde_json::to_string(PLACEHOLDER).expect("a string");
                let id = serde_json::to_string(id).expect("a string");
                body.replacen(&placeholder, &id, 1)
            }
            Outgoing::Gated { open, msg } => match open.load(Ordering::SeqCst) {
                true => return msg.render(),
                false => return None,
            },
        })
    }
}

/// A message for every subscriber, serialized once (`msg` has [`PLACEHOLDER`] as its id).
fn shared(msg: &ServerMsg) -> Arc<str> {
    Arc::from(serde_json::to_string(msg).expect("server messages serialize"))
}

/// Why a joiner gets a snapshot rather than a resume.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Reason {
    NoCursor,
    BadToken,
    OtherLineage,
    BeforeLog,
    Ahead,
}

/// Where a subscriber's result came from.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(
    tag = "source",
    rename_all = "snake_case",
    rename_all_fields = "camelCase"
)]
pub enum Source {
    /// Already in memory: the entry was live (or in keep-alive).
    Memory,
    /// The entry was warm: its rows were loaded from disk.
    Warm { load_ms: u64 },
    /// The entry was cold (or the server restarted): restored from disk and caught up from
    /// Kafka, without ClickHouse.
    Restored { load_ms: u64, events_behind: u64 },
    /// A new result: its history queried from a ClickHouse snapshot.
    Clickhouse {
        load_ms: u64,
        /// The clone it was read from, and whether it was taken for this result (else an
        /// existing one was reused).
        snapshot: Option<String>,
        new_snapshot: Option<bool>,
        /// Why a stored result couldn't be used instead, if there was one.
        #[serde(skip_serializing_if = "Option::is_none")]
        rejected: Option<String>,
    },
}

/// How a subscriber was served: where its result came from, and what it got.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Served {
    /// Unix milliseconds.
    pub at: u64,
    pub conn: u64,
    pub id: String,
    pub key: String,
    pub sql: String,
    pub lineage: String,
    /// It sent a cursor (`since`).
    pub cursor: bool,
    #[serde(flatten)]
    pub source: Source,
    /// `snapshot` or `resume`.
    pub decision: &'static str,
    /// Why a snapshot rather than a resume.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<Reason>,
    /// Resumed from the `memory` or the `disk` log, with this many diffs.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub log: Option<&'static str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub diffs: Option<usize>,
    /// Rows in the result.
    pub rows: usize,
    /// It joined before the result was ready, and waited this long.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub waited_ms: Option<u64>,
}

/// Joins kept for `/debug/state`.
const RECENT_JOINS: usize = 200;

/// How a subscriber starts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Decision {
    /// The history isn't in yet: a snapshot once it is.
    Wait,
    Snapshot(Reason),
    /// `resume`, then the log from this index (`log.len()`: nothing to send).
    Resume {
        from: usize,
    },
}

/// How a subscriber with `since` starts, given the entry's lineage (and the earlier lineages it
/// forked from, each with where it ended), its cursor, and the cursor each log item ends at (the
/// log starts right after `log_start`).
pub fn decide(
    lineage: &str,
    forks: &[(String, Watermark)],
    ready: bool,
    at: &Watermark,
    log_start: &Watermark,
    log: &[&Watermark],
    since: Option<Result<Cursor, ()>>,
) -> Decision {
    if !ready {
        return Decision::Wait;
    }
    let cursor = match since {
        None => return Decision::Snapshot(Reason::NoCursor),
        Some(Err(())) => return Decision::Snapshot(Reason::BadToken),
        Some(Ok(c)) => c,
    };
    let ours = cursor.lineage.as_deref() == Some(lineage)
        || forks
            .iter()
            .any(|(l, end)| cursor.lineage.as_deref() == Some(l) && end.covers(&cursor.at));
    if !ours {
        return Decision::Snapshot(Reason::OtherLineage);
    }
    if !at.covers(&cursor.at) {
        return Decision::Snapshot(Reason::Ahead);
    }
    if !cursor.at.covers(log_start) {
        return Decision::Snapshot(Reason::BeforeLog);
    }
    let from = log
        .iter()
        .position(|end| !cursor.at.covers(end))
        .unwrap_or(log.len());
    Decision::Resume { from }
}

enum Cmd {
    Join {
        sub: u64,
        conn: u64,
        client_id: String,
        out: mpsc::Sender<Outgoing>,
        since: Option<String>,
        alive: Arc<AtomicBool>,
        gate: Arc<AtomicBool>,
    },
    Leave {
        sub: u64,
    },
    /// Too many warm entries: go cold, if still warm.
    Evict,
    Shutdown(oneshot::Sender<()>),
}

struct Handle {
    /// This entry instance (a key's entry can be replaced by a new one).
    id: u64,
    tx: mpsc::UnboundedSender<Cmd>,
    info: Arc<Mutex<Info>>,
    /// Since when it's been warm, if it is.
    warm_since: Option<Instant>,
}

/// A subscription as the connection keeps it: which entry, which subscriber there, and whether
/// it's still running (an entry that fails ends its subscribers).
pub struct Joined {
    pub key: String,
    pub sub: u64,
    pub alive: Arc<AtomicBool>,
    /// Open while the connection takes this entry's messages for it (see [`Outgoing::Gated`]).
    pub gate: Arc<AtomicBool>,
}

impl Joined {
    /// Take nothing more from this entry, queued or not (before joining another under the
    /// same id).
    pub fn close(&self) {
        self.gate.store(false, Ordering::SeqCst);
    }
}

pub struct Hub {
    engine: Arc<Engine>,
    cache: CacheSettings,
    progress_interval: Duration,
    store: Option<Arc<Store>>,
    entries: Mutex<HashMap<String, Handle>>,
    next: AtomicU64,
    /// The last joins, newest first.
    recent: Mutex<VecDeque<Served>>,
}

impl Hub {
    /// With `cache.dir`, results go to a disk store there (opened now; if it can't be, the
    /// server runs without one, and says so).
    pub fn new(
        engine: Arc<Engine>,
        cache: CacheSettings,
        progress_interval: Duration,
    ) -> Arc<Self> {
        let store = cache
            .dir
            .as_ref()
            .and_then(|dir| match Store::open(dir, cache.disk_budget) {
                Ok(s) => Some(s),
                Err(e) => {
                    warn!(dir = %dir.display(), error = %e, "the cache store can't be opened: results stay in memory only");
                    None
                }
            });
        Arc::new(Self {
            engine,
            cache,
            progress_interval,
            store,
            entries: Mutex::new(HashMap::new()),
            recent: Mutex::new(VecDeque::new()),
            next: AtomicU64::new(1),
        })
    }

    pub fn engine(&self) -> &Arc<Engine> {
        &self.engine
    }

    pub fn store(&self) -> Option<&Arc<Store>> {
        self.store.as_ref()
    }

    /// Subscribe to `plan`'s entry: the running one, or a new one (restored from the store if
    /// it's there and still good, else from a ClickHouse snapshot).
    #[allow(clippy::too_many_arguments)]
    pub fn join(
        self: &Arc<Self>,
        plan: &Plan,
        sql: &str,
        params: &Params,
        filters: &Filters,
        conn: u64,
        client_id: &str,
        out: mpsc::Sender<Outgoing>,
        since: Option<String>,
    ) -> Joined {
        let key = cursor::identity(plan);
        let sub = self.next.fetch_add(1, Ordering::Relaxed);
        let alive = Arc::new(AtomicBool::new(true));
        let gate = Arc::new(AtomicBool::new(true));
        let join = Cmd::Join {
            sub,
            conn,
            client_id: client_id.to_string(),
            out,
            since,
            alive: alive.clone(),
            gate: gate.clone(),
        };
        // joins are sent under the lock, so an entry deciding to go (also under it) never
        // misses one
        let mut entries = self.entries.lock().unwrap();
        let join = match entries.get(&key) {
            Some(h) => match h.tx.send(join) {
                Ok(()) => {
                    return Joined {
                        key,
                        sub,
                        alive,
                        gate,
                    }
                }
                // its task ended without removing itself (a panic): replace it
                Err(mpsc::error::SendError(join)) => join,
            },
            None => join,
        };
        let id = self.next.fetch_add(1, Ordering::Relaxed);
        let lineage = new_lineage(id);
        let fp = cursor::hash(&key);
        info!(key = %fp, lineage, sql, params = %params_text(params), "entry created");
        let info = Arc::new(Mutex::new(Info::new(&fp, sql, params, &lineage)));
        let (tx, rx) = mpsc::unbounded_channel();
        let _ = tx.send(join);
        entries.insert(
            key.clone(),
            Handle {
                id,
                tx,
                info: info.clone(),
                warm_since: None,
            },
        );
        drop(entries);
        let entry = Entry::new(
            self.clone(),
            key.clone(),
            fp,
            id,
            lineage,
            sql.to_string(),
            params.clone(),
            filters.clone(),
            info,
        );
        tokio::spawn(entry.run(rx));
        Joined {
            key,
            sub,
            alive,
            gate,
        }
    }

    pub fn leave(&self, key: &str, sub: u64) {
        if let Some(h) = self.entries.lock().unwrap().get(key) {
            let _ = h.tx.send(Cmd::Leave { sub });
        }
    }

    /// For `/debug/subscriptions`: the running entries, then the cold ones in the store (as
    /// of every write sent so far).
    pub async fn list(&self) -> Vec<Json> {
        self.entries_and_index().await.0
    }

    /// For `/debug/state`: the entries (as [`Hub::list`]), the store and the cache settings.
    pub async fn state(&self) -> Json {
        let (entries, index) = self.entries_and_index().await;
        let store = self.store.as_ref().map(|store| {
            let running: HashMap<String, &str> = entries
                .iter()
                .filter_map(|e| Some((e["key"].as_str()?.to_string(), e["stage"].as_str()?)))
                .collect();
            let mut by_stage: HashMap<&str, u64> = HashMap::new();
            let mut bytes = 0;
            for e in &index {
                bytes += e.bytes.max(0) as u64;
                // as listed: a running entry's stage as it is now (the store's lags a batch)
                let stage = match running.get(&store::short(&e.key)).copied() {
                    Some(s @ ("warm" | "cold")) => s,
                    Some(_) => "live",
                    None => e.stage.as_str(),
                };
                *by_stage.entry(stage).or_default() += 1;
            }
            let mut stats = serde_json::to_value(store.stats()).expect("stats serialize");
            stats["bytes"] = bytes.into();
            stats["budget"] = self.cache.disk_budget.into();
            stats["byStage"] = serde_json::to_value(by_stage).expect("counts serialize");
            stats
        });
        let c = &self.cache;
        serde_json::json!({
            "cache": {
                "dir": c.dir.as_ref().map(|d| d.display().to_string()),
                "keepAlive": c.keep_alive.as_secs_f64(),
                "keepWarm": c.keep_warm.as_secs_f64(),
                "maxWarm": c.max_warm,
                "logLength": c.log_length,
                "diskBudget": c.disk_budget,
                "maxAge": c.max_age.as_secs_f64(),
                "maxCatchUp": c.max_catch_up,
                "checkpointInterval": c.checkpoint_interval.as_secs_f64(),
                "logBytes": c.log_bytes,
            },
            "entries": entries,
            "store": store,
            "joins": *self.recent.lock().unwrap(),
        })
    }

    /// The running entries, then the cold ones in the store; and the store's index.
    async fn entries_and_index(&self) -> (Vec<Json>, Vec<store::IndexEntry>) {
        if let Some(store) = &self.store {
            store.flush().await;
        }
        let (mut running, keys): (Vec<Info>, Vec<String>) = {
            let entries = self.entries.lock().unwrap();
            (
                entries
                    .values()
                    .map(|h| h.info.lock().unwrap().clone())
                    .collect(),
                entries.keys().cloned().collect(),
            )
        };
        running.sort_by_key(|i| i.created);
        let now = store::now_ms() as u64;
        let mut out: Vec<Json> = running
            .iter_mut()
            .map(|i| {
                i.stage_ends_in = i
                    .stage_ends_at
                    .map(|at| at.saturating_sub(now) as f64 / 1000.0);
                serde_json::to_value(&*i).expect("info serializes")
            })
            .collect();
        let mut index = Vec::new();
        if let Some(store) = self.store.clone() {
            match tokio::task::spawn_blocking(move || store.index())
                .await
                .expect("the index read")
            {
                Ok(i) => index = i,
                Err(e) => warn!(error = %e, "reading the cache store's index failed"),
            }
        }
        out.extend(index.iter().filter(|e| !keys.contains(&e.key)).map(|e| {
            serde_json::json!({
                "key": store::short(&e.key),
                "sql": e.sql,
                "lineage": e.lineage,
                "stage": e.stage,
                "at": e.state_at,
                "bytes": e.bytes,
                "logRows": e.log_rows,
                "usedAt": e.used_at,
                "bornAt": e.born_at,
            })
        }));
        (out, index)
    }

    /// Stop every entry: those with a store are checkpointed and go cold.
    pub async fn shutdown(&self) {
        let handles: Vec<Handle> = self
            .entries
            .lock()
            .unwrap()
            .drain()
            .map(|(_, h)| h)
            .collect();
        for h in handles {
            let (done, wait) = oneshot::channel();
            if h.tx.send(Cmd::Shutdown(done)).is_ok() {
                let _ = wait.await;
            }
        }
        if let Some(store) = &self.store {
            store.flush().await;
        }
    }

    /// An entry going: removed if it's still this instance and nothing is waiting for it (a join
    /// that arrived meanwhile keeps it). Returns whether it was removed.
    fn remove_if_idle(&self, key: &str, id: u64, rx: &mpsc::UnboundedReceiver<Cmd>) -> bool {
        let mut entries = self.entries.lock().unwrap();
        if !rx.is_empty() {
            return false;
        }
        if entries.get(key).is_some_and(|h| h.id == id) {
            entries.remove(key);
        }
        true
    }

    /// An entry that failed: gone whatever is waiting (queued joins get nothing; their
    /// connections see their subscription ended).
    fn remove(&self, key: &str, id: u64) {
        let mut entries = self.entries.lock().unwrap();
        if entries.get(key).is_some_and(|h| h.id == id) {
            entries.remove(key);
        }
    }

    /// A subscriber served: kept among the recent joins.
    fn served(&self, served: Served) {
        let mut recent = self.recent.lock().unwrap();
        recent.push_front(served);
        recent.truncate(RECENT_JOINS);
    }

    /// An entry becoming warm (or not). Over `max_warm`, the one warm the longest goes cold.
    fn set_warm(&self, key: &str, id: u64, warm: bool) {
        let mut entries = self.entries.lock().unwrap();
        if let Some(h) = entries.get_mut(key).filter(|h| h.id == id) {
            h.warm_since = warm.then(Instant::now);
            h.info.lock().unwrap().warm_since = warm.then(|| store::now_ms() as u64);
        }
        if !warm {
            return;
        }
        let mut warm: Vec<(&String, Instant)> = entries
            .iter()
            .filter_map(|(k, h)| h.warm_since.map(|t| (k, t)))
            .collect();
        if warm.len() <= self.cache.max_warm {
            return;
        }
        warm.sort_by_key(|(_, t)| *t);
        for (k, _) in warm.iter().take(warm.len() - self.cache.max_warm) {
            if let Some(h) = entries.get(*k) {
                debug!(key = %cursor::hash(k), max_warm = self.cache.max_warm, "too many warm entries: the longest-warm goes cold");
                let _ = h.tx.send(Cmd::Evict);
            }
        }
    }
}

/// Unique per process and across restarts (time-based).
fn new_lineage(id: u64) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    format!("{:x}-{id:x}", nanos & 0xffff_ffff_ffff)
}

fn params_text(params: &Params) -> String {
    if params.is_empty() {
        String::new()
    } else {
        serde_json::to_string(params).unwrap_or_default()
    }
}

/// An entry, as `/debug/subscriptions` shows it.
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Info {
    pub key: String,
    pub sql: String,
    #[serde(skip_serializing_if = "Params::is_empty")]
    pub params: Params,
    pub lineage: String,
    /// Earlier lineages it forked from (restored from the store).
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub forks: Vec<String>,
    /// `starting`, `live`, `keep_alive` or `warm`.
    pub stage: &'static str,
    /// Seconds until the next stage (keep-alive, warm).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stage_ends_in: Option<f64>,
    /// When the next stage starts (Unix milliseconds).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stage_ends_at: Option<u64>,
    /// Since when it's been warm (Unix milliseconds): over `max_warm`, the earliest goes cold.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub warm_since: Option<u64>,
    pub subscribers: Vec<SubscriberInfo>,
    pub rows: usize,
    /// Whether the rows are in memory (live) or only on disk (warm).
    pub in_memory: bool,
    /// Mirrored to the disk store.
    pub persisted: bool,
    /// Restored from the store, rather than started from a snapshot.
    pub restored: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub at: Option<String>,
    pub log_length: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub log_start: Option<String>,
    /// Unix milliseconds.
    pub created: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub history_ms: Option<u64>,
    pub messages: u64,
    pub checkpoints: u64,
    pub joins_snapshot: u64,
    pub joins_resume: u64,
    pub joins_wait: u64,
    pub resyncs: u64,
    /// How it started: from ClickHouse, or restored from disk.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source: Option<Source>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SubscriberInfo {
    pub conn: u64,
    pub id: String,
    pub lagging: bool,
    /// How it was served (none while it waits for the result).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub served: Option<Served>,
}

impl Info {
    fn new(key: &str, sql: &str, params: &Params, lineage: &str) -> Self {
        Self {
            key: key.to_string(),
            sql: sql.to_string(),
            params: params.clone(),
            lineage: lineage.to_string(),
            forks: Vec::new(),
            stage: "starting",
            stage_ends_in: None,
            stage_ends_at: None,
            warm_since: None,
            subscribers: Vec::new(),
            rows: 0,
            in_memory: true,
            persisted: false,
            restored: false,
            at: None,
            log_length: 0,
            log_start: None,
            created: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
            history_ms: None,
            messages: 0,
            checkpoints: 0,
            joins_snapshot: 0,
            joins_resume: 0,
            joins_wait: 0,
            resyncs: 0,
            source: None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Stage {
    Live,
    KeepAlive,
    Warm,
}

impl Stage {
    fn name(self) -> &'static str {
        match self {
            Stage::Live => "live",
            Stage::KeepAlive => "keep_alive",
            Stage::Warm => "warm",
        }
    }
}

struct Sub {
    conn: u64,
    client_id: String,
    out: mpsc::Sender<Outgoing>,
    alive: Arc<AtomicBool>,
    gate: Arc<AtomicBool>,
    /// The cursor of the last message it got.
    delivered: Option<Watermark>,
    /// Joined before the result was ready: decided when it is (with its cursor).
    waiting: Option<Option<String>>,
    /// Its connection's queue was full: nothing is sent until it catches up.
    lagging: bool,
    joined_at: Instant,
    served: Option<Served>,
}

struct LogItem {
    /// The cursor the result is complete to after this message.
    at: Watermark,
    body: Arc<str>,
}

/// What the subscription's plan says about the result's form.
struct Form {
    plan: Plan,
    columns: Vec<Column>,
    order: Option<Vec<OrderTerm>>,
    limit: Option<u64>,
    offset: Option<u64>,
    /// Rows have ids (aggregated and FINAL results): snapshots send them.
    ids: bool,
}

enum Sent {
    Ok,
    Full,
    Closed,
}

/// How an entry started.
enum Start {
    /// From a ClickHouse snapshot (its history comes as the first iteration); with why a stored
    /// result couldn't be used, if there was one.
    Fresh {
        sub: Subscription,
        rejected: Option<String>,
    },
    /// From the store: the rows as of the checkpoint, the log messages up to it, and where the
    /// old lineage ended.
    Restored {
        sub: Subscription,
        stored: Box<Stored>,
        rows: Box<ResultList>,
        log: Vec<store::LogItem>,
        checkpoint: Checkpoint,
        lag: u64,
        ms: u64,
    },
}

struct Entry {
    hub: Arc<Hub>,
    key: String,
    /// The key's fingerprint, for logs.
    fp: String,
    id: u64,
    lineage: String,
    /// Earlier lineages (restores fork them) and where each ended.
    forks: Vec<(String, Watermark)>,
    /// When the snapshot the result grew from was taken (unix ms).
    born_at: i64,
    sql: String,
    params: Params,
    /// The subscribers' row filters: part of the plan, so every subscriber has the same.
    filters: Filters,
    info: Arc<Mutex<Info>>,
    sub: Option<ReadyChunks<Subscription>>,
    form: Option<Form>,
    rows: ResultList,
    /// The rows are in memory (not warm).
    in_memory: bool,
    /// How many rows the result has (also while warm).
    rows_count: usize,
    at: Option<Watermark>,
    ready: bool,
    log: VecDeque<LogItem>,
    log_start: Watermark,
    subs: HashMap<u64, Sub>,
    stage: Stage,
    deadline: Option<tokio::time::Instant>,
    created: Instant,
    /// The cursor last sent, and when a `progress` last went out.
    sent_at: Option<Watermark>,
    last_progress: Instant,
    served: u64,
    /// Mirrored to the store (there is one, and the shape has checkpoints).
    persisted: bool,
    /// Changed since the last checkpoint.
    dirty: bool,
    last_checkpoint: Instant,
    /// Bytes logged to disk since the last compaction.
    disk_log_bytes: u64,
    restored: bool,
    /// How it started (once its result is ready): what waiting subscribers are told.
    source: Option<Source>,
    /// A fresh start's snapshot and why a stored result wasn't used, until the history is in.
    fresh: Option<(
        Option<chlive_engine::snapshot::UsedSnapshot>,
        Option<String>,
    )>,
    started_at: SystemTime,
}

impl Entry {
    #[allow(clippy::too_many_arguments)]
    fn new(
        hub: Arc<Hub>,
        key: String,
        fp: String,
        id: u64,
        lineage: String,
        sql: String,
        params: Params,
        filters: Filters,
        info: Arc<Mutex<Info>>,
    ) -> Self {
        let progress_interval = hub.progress_interval;
        Self {
            hub,
            key,
            fp,
            id,
            lineage,
            forks: Vec::new(),
            born_at: store::now_ms(),
            sql,
            params,
            filters,
            info,
            sub: None,
            form: None,
            rows: ResultList::default(),
            in_memory: true,
            rows_count: 0,
            at: None,
            ready: false,
            log: VecDeque::new(),
            log_start: Watermark::default(),
            subs: HashMap::new(),
            stage: Stage::Live,
            deadline: None,
            created: Instant::now(),
            sent_at: None,
            last_progress: Instant::now() - progress_interval,
            served: 0,
            persisted: false,
            dirty: false,
            last_checkpoint: Instant::now(),
            disk_log_bytes: 0,
            restored: false,
            source: None,
            fresh: None,
            started_at: SystemTime::now(),
        }
    }

    fn store(&self) -> Option<&Arc<Store>> {
        self.persisted.then(|| self.hub.store.as_ref()).flatten()
    }

    async fn run(mut self, mut rx: mpsc::UnboundedReceiver<Cmd>) {
        let started = Instant::now();
        let mut starting = Box::pin(start(
            self.hub.clone(),
            self.key.clone(),
            self.fp.clone(),
            self.sql.clone(),
            self.params.clone(),
            self.filters.clone(),
        ));
        let mut subscribed = false;
        let mut tick = tokio::time::interval(Duration::from_millis(200));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            let deadline = self.deadline;
            tokio::select! {
                // joins and leaves first: a subscriber that joined before the history gets it
                // as its snapshot, and the changes after as diffs
                biased;
                cmd = rx.recv() => match cmd {
                    Some(Cmd::Shutdown(done)) => {
                        self.shutdown().await;
                        let _ = done.send(());
                        return;
                    }
                    Some(cmd) => if !self.command(cmd, &rx).await { return },
                    None => return,
                },
                r = &mut starting, if !subscribed => {
                    subscribed = true;
                    match r {
                        Ok(Start::Fresh { sub: s, rejected }) => {
                            self.subscribed(&s);
                            self.fresh = Some((s.snapshot.clone(), rejected));
                            self.sub = Some(s.ready_chunks(MAX_ITERATIONS_PER_MESSAGE));
                        }
                        Ok(Start::Restored { sub, stored, rows, log, checkpoint, lag, ms }) => {
                            self.subscribed(&sub);
                            self.restore(*stored, *rows, log, checkpoint, lag, ms);
                            self.sub = Some(sub.ready_chunks(MAX_ITERATIONS_PER_MESSAGE));
                            // subscribers that joined while it was being restored: their
                            // cursors may resume through the fork
                            self.serve_waiting().await;
                            self.publish();
                        }
                        Err(e) => {
                            self.fail(&e, started);
                            return;
                        }
                    }
                }
                batch = next_batch(&mut self.sub) => match batch {
                    Some(batch) => if !self.batch(batch, started) { return },
                    // a finite source (tests) ended: the result stays as it is
                    None => self.sub = None,
                },
                _ = sleep_until(deadline) => if !self.next_stage(&rx).await { return },
                _ = tick.tick() => {
                    if self.subs.values().any(|s| s.lagging) {
                        self.catch_up_lagging();
                    }
                    if self.dirty
                        && self.last_checkpoint.elapsed() >= self.hub.cache.checkpoint_interval
                    {
                        self.checkpoint("interval");
                    }
                }
            }
        }
    }

    fn subscribed(&mut self, s: &Subscription) {
        let plan = s.plan.clone();
        let columns: Vec<Column> = match &plan.aggregate {
            Some(a) => a
                .columns
                .iter()
                .map(|(name, ty)| Column {
                    name: name.clone(),
                    ty: ty.clickhouse.clone(),
                })
                .collect(),
            None => plan
                .columns
                .iter()
                .map(|c| Column {
                    name: c.name.clone(),
                    ty: c.ty.clickhouse.clone(),
                })
                .collect(),
        };
        let order = order_terms(&plan);
        let limit = plan
            .aggregate
            .as_ref()
            .map_or(plan.stream.limit, |a| a.limit);
        let offset = plan
            .aggregate
            .as_ref()
            .map_or(plan.stream.offset, |a| a.offset);
        self.persisted = self.hub.store.is_some() && s.checkpointable();
        debug!(key = %self.fp, lineage = %self.lineage, watermark = %s.watermark.describe(), history = %s.history_sql, persisted = self.persisted, "entry subscribed");
        self.form = Some(Form {
            ids: plan.aggregate.is_some() || plan.stream.keyed.is_some(),
            plan,
            columns,
            order,
            limit,
            offset,
        });
    }

    /// Restored from the store: the rows, the log up to the checkpoint, a new lineage forked
    /// from the stored one, and written back as this lineage's base.
    fn restore(
        &mut self,
        stored: Stored,
        rows: ResultList,
        log: Vec<store::LogItem>,
        checkpoint: Checkpoint,
        lag: u64,
        ms: u64,
    ) {
        let at = checkpoint.at.clone();
        self.forks = stored.meta.forks.clone();
        self.forks.push((stored.meta.lineage.clone(), at.clone()));
        self.born_at = stored.meta.born_at;
        self.rows = rows;
        self.rows_count = self.rows.rows.len();
        self.log = log
            .iter()
            .map(|i| LogItem {
                at: i.at.clone(),
                body: i.body.clone(),
            })
            .collect();
        self.log_start = stored.base_at.clone();
        self.trim_log();
        self.ready = true;
        self.restored = true;
        self.at = Some(at.clone());
        self.sent_at = Some(at.clone());
        info!(key = %self.fp, lineage = %self.lineage, from_lineage = %stored.meta.lineage, rows = self.rows_count, at = %at.describe(), log = self.log.len(), events_behind = lag, ms, "restored from the store (no ClickHouse)");
        if let Some(store) = self.store() {
            store.create(self.meta(), at, self.rows.clone(), checkpoint.to_json());
        }
        self.last_checkpoint = Instant::now();
        let source = Source::Restored {
            load_ms: ms,
            events_behind: lag,
        };
        self.source = Some(source.clone());
        let mut info = self.info.lock().unwrap();
        info.history_ms = Some(ms);
        info.forks = self.forks.iter().map(|(l, _)| l.clone()).collect();
        info.source = Some(source);
        drop(info);
    }

    fn meta(&self) -> Meta {
        Meta {
            key: self.key.clone(),
            fp: self.fp.clone(),
            sql: self.sql.clone(),
            params: params_text(&self.params),
            lineage: self.lineage.clone(),
            forks: self.forks.clone(),
            born_at: self.born_at,
        }
    }

    fn checkpoint_now(&self) -> Option<Checkpoint> {
        self.sub.as_ref().and_then(|s| s.get_ref().checkpoint())
    }

    /// Save the engine state (persisted entries).
    fn checkpoint(&mut self, why: &str) {
        let Some(store) = self.store().cloned() else {
            return;
        };
        let Some(cp) = self.checkpoint_now() else {
            return;
        };
        let json = cp.to_json();
        debug!(target: "chlive_server::store", key = %self.fp, lineage = %self.lineage, at = %cp.at.describe(), bytes = json.to_string().len(), why, "checkpoint");
        store.checkpoint(&self.key, cp.at, json);
        self.dirty = false;
        self.last_checkpoint = Instant::now();
        self.info.lock().unwrap().checkpoints += 1;
    }

    fn encode(&self, at: &Watermark) -> String {
        let form = self.form.as_ref().expect("subscribed");
        cursor::encode_with(&form.plan, Some(&self.lineage), at)
    }

    /// Subscribers that joined before the result was ready: decided now, with their cursors
    /// (a fresh result is a new lineage, so they get the snapshot; a restored one may resume
    /// them).
    async fn serve_waiting(&mut self) {
        if !self.ready {
            return;
        }
        let waiting: Vec<(u64, u64, String, Option<String>)> = self
            .subs
            .iter_mut()
            .filter_map(|(k, s)| {
                let since = s.waiting.take()?;
                Some((*k, s.conn, s.client_id.clone(), since))
            })
            .collect();
        let source = self.source.clone().unwrap_or(Source::Memory);
        for (sub, conn, client_id, since) in waiting {
            debug!(key = %self.fp, lineage = %self.lineage, sub, "a waiting subscriber, now the result is ready");
            self.join(sub, conn, client_id, since, source.clone()).await;
        }
    }

    /// The history is in: waiting subscribers get the snapshot (without waiting on anything).
    fn serve_waiting_snapshot(&mut self) {
        let waiting: Vec<(u64, bool)> = self
            .subs
            .iter_mut()
            .filter_map(|(k, s)| s.waiting.take().map(|since| (*k, since.is_some())))
            .collect();
        if waiting.is_empty() {
            return;
        }
        let body = self.snapshot_body();
        let at = self.at.clone().expect("ready");
        let source = self.source.clone().unwrap_or(Source::Memory);
        for (sub, cursor) in waiting {
            debug!(key = %self.fp, lineage = %self.lineage, sub, "waiting subscriber gets the snapshot");
            // a fresh result is a new lineage: no cursor resumes it
            let reason = match cursor {
                true => Reason::OtherLineage,
                false => Reason::NoCursor,
            };
            self.record(sub, cursor, source.clone(), Some(reason), None);
            self.deliver(sub, body.clone(), &at);
        }
    }

    /// Remember how a subscriber was served (on the subscriber, and among the hub's recent
    /// joins). A snapshot has a reason; a resume, its log and diffs.
    fn record(
        &mut self,
        sub: u64,
        cursor: bool,
        source: Source,
        reason: Option<Reason>,
        resumed: Option<(&'static str, usize)>,
    ) {
        let rows = self.rows_count;
        let Some(s) = self.subs.get_mut(&sub) else {
            return;
        };
        // only a subscriber that waited for the result gets it as it started
        let waited_ms = matches!(source, Source::Restored { .. } | Source::Clickhouse { .. })
            .then(|| s.joined_at.elapsed().as_millis() as u64);
        let served = Served {
            at: store::now_ms() as u64,
            conn: s.conn,
            id: s.client_id.clone(),
            key: self.fp.clone(),
            sql: self.sql.clone(),
            lineage: self.lineage.clone(),
            cursor,
            source,
            decision: if resumed.is_some() {
                "resume"
            } else {
                "snapshot"
            },
            reason,
            log: resumed.map(|r| r.0),
            diffs: resumed.map(|r| r.1),
            rows,
            waited_ms,
        };
        s.served = Some(served.clone());
        self.hub.served(served);
    }

    /// One batch of iterations: the history (once), then diffs and progress, sent to everyone,
    /// logged and mirrored. Returns false when the entry has failed.
    fn batch(&mut self, batch: Vec<Result<Iteration, EngineError>>, started: Instant) -> bool {
        let mut diffs: Vec<Diff> = Vec::new();
        let mut at: Option<Watermark> = None;
        let mut catch_up = false;
        let mut history = false;
        for iteration in batch {
            match iteration {
                Ok(it) if it.origin == Origin::History => {
                    let mut rows = ResultList::default();
                    for d in &it.diffs {
                        if let Err(e) = rows.apply(d) {
                            warn!(key = %self.fp, error = %e, "history didn't apply");
                        }
                    }
                    self.rows = rows;
                    self.rows_count = self.rows.rows.len();
                    self.ready = true;
                    self.log_start = it.at.clone();
                    self.at = Some(it.at.clone());
                    self.sent_at = Some(it.at);
                    history = true;
                    let ms = started.elapsed().as_millis() as u64;
                    let (snapshot, rejected) = self.fresh.take().unwrap_or_default();
                    let source = Source::Clickhouse {
                        load_ms: ms,
                        new_snapshot: snapshot.as_ref().map(|s| s.taken_at >= self.started_at),
                        snapshot: snapshot.map(|s| s.clone),
                        rejected,
                    };
                    info!(key = %self.fp, lineage = %self.lineage, rows = self.rows_count, ms, source = ?source, "history loaded");
                    let mut info = self.info.lock().unwrap();
                    info.history_ms = Some(ms);
                    info.source = Some(source.clone());
                    drop(info);
                    self.source = Some(source);
                    self.serve_waiting_snapshot();
                }
                Ok(it) => {
                    catch_up |= it.origin == Origin::CatchUp;
                    diffs.extend(it.diffs);
                    at = Some(it.at);
                }
                Err(e) => {
                    if let Some(a) = at.take() {
                        if !diffs.is_empty() {
                            self.diff(std::mem::take(&mut diffs), a);
                        }
                    }
                    self.fail(&e, started);
                    return false;
                }
            }
        }
        if let Some(a) = at {
            if !diffs.is_empty() {
                self.diff(diffs, a);
            } else if self.sent_at.as_ref() != Some(&a)
                && (catch_up || self.last_progress.elapsed() >= self.hub.progress_interval)
            {
                // only non-matching events: tell everyone how far the result is, now and then
                self.at = Some(a.clone());
                let body = shared(&ServerMsg::Progress {
                    id: PLACEHOLDER.into(),
                    at: self.encode(&a),
                });
                self.broadcast(&body, &a);
                self.last_progress = Instant::now();
                self.sent_at = Some(a);
                self.dirty = true;
            } else {
                if self.at.as_ref() != Some(&a) {
                    self.dirty = true;
                }
                self.at = Some(a);
            }
        }
        // a new result: its base and first checkpoint, at the end of this batch
        if history {
            if let (Some(store), Some(cp)) = (self.store().cloned(), self.checkpoint_now()) {
                store.create(self.meta(), cp.at.clone(), self.rows.clone(), cp.to_json());
                self.dirty = false;
                self.last_checkpoint = Instant::now();
                debug!(target: "chlive_server::store", key = %self.fp, lineage = %self.lineage, rows = self.rows_count, at = %cp.at.describe(), "mirrored to the store");
            }
        }
        self.publish();
        true
    }

    /// One `diff` message: applied to the rows (if in memory), logged, mirrored, sent.
    fn diff(&mut self, diffs: Vec<Diff>, at: Watermark) {
        for d in &diffs {
            match d {
                Diff::Insert { .. } => self.rows_count += 1,
                Diff::Delete { .. } | Diff::DeleteId { .. } => {
                    self.rows_count = self.rows_count.saturating_sub(1)
                }
                Diff::Update { .. } => {}
            }
            if self.in_memory {
                if let Err(e) = self.rows.apply(d) {
                    // the engine's own diffs: a bug if they don't apply
                    warn!(key = %self.fp, lineage = %self.lineage, error = %e, "a diff didn't apply to the shared rows");
                }
            }
        }
        let changes: Vec<_> = diffs.into_iter().map(change).collect();
        let n = changes.len();
        let body = shared(&ServerMsg::Diff {
            id: PLACEHOLDER.into(),
            changes,
            at: self.encode(&at),
        });
        trace!(key = %self.fp, changes = n, rows = self.rows_count, log = self.log.len(), at = %at.describe(), "diff");
        if self.in_memory {
            self.log.push_back(LogItem {
                at: at.clone(),
                body: body.clone(),
            });
            self.trim_log();
        }
        if let Some(store) = self.store().cloned() {
            store.append(&self.key, at.clone(), body.clone());
            self.disk_log_bytes += body.len() as u64;
            if self.disk_log_bytes > self.hub.cache.log_bytes {
                // fold what the checkpoint covers (all of it, after this one) into the base
                self.checkpoint("compaction");
                store.compact(&self.key);
                self.disk_log_bytes = 0;
            }
        }
        self.at = Some(at.clone());
        self.sent_at = Some(at.clone());
        self.dirty = true;
        self.info.lock().unwrap().messages += 1;
        self.broadcast(&body, &at);
    }

    fn trim_log(&mut self) {
        while self.log.len() > self.hub.cache.log_length {
            if let Some(gone) = self.log.pop_front() {
                self.log_start = gone.at;
            }
        }
    }

    fn snapshot_body(&self) -> Arc<str> {
        let form = self.form.as_ref().expect("subscribed");
        let at = self.at.as_ref().expect("ready");
        shared(&ServerMsg::Snapshot {
            id: PLACEHOLDER.into(),
            columns: form.columns.clone(),
            rows: self.rows.rows.clone(),
            order: form.order.clone(),
            limit: form.limit,
            offset: form.offset,
            at: self.encode(at),
            ids: form.ids.then(|| {
                self.rows
                    .ids
                    .iter()
                    .map(|i| i.clone().unwrap_or_default())
                    .collect()
            }),
        })
    }

    /// To every subscriber that's up to date (not waiting for the history, not lagging).
    fn broadcast(&mut self, body: &Arc<str>, at: &Watermark) {
        let subs: Vec<u64> = self
            .subs
            .iter()
            .filter(|(_, s)| s.waiting.is_none() && !s.lagging)
            .map(|(k, _)| *k)
            .collect();
        for sub in subs {
            self.deliver(sub, body.clone(), at);
        }
    }

    /// Without waiting: a full queue makes the subscriber lagging, a closed one removes it.
    fn deliver(&mut self, sub: u64, body: Arc<str>, at: &Watermark) -> Sent {
        let Some(s) = self.subs.get_mut(&sub) else {
            return Sent::Closed;
        };
        let msg = Outgoing::Gated {
            open: s.gate.clone(),
            msg: Box::new(Outgoing::Shared {
                id: s.client_id.clone(),
                body,
            }),
        };
        match s.out.try_send(msg) {
            Ok(()) => {
                s.delivered = Some(at.clone());
                Sent::Ok
            }
            Err(mpsc::error::TrySendError::Full(_)) => {
                if !s.lagging {
                    warn!(key = %self.fp, lineage = %self.lineage, sub, conn = s.conn, id = %s.client_id, delivered = %s.delivered.as_ref().map(Watermark::describe).unwrap_or_default(), "subscriber is lagging: its queue is full; it catches up later");
                }
                s.lagging = true;
                Sent::Full
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                debug!(key = %self.fp, sub, "subscriber's connection is gone");
                self.subs.remove(&sub);
                Sent::Closed
            }
        }
    }

    fn send_msg(&mut self, sub: u64, msg: ServerMsg) -> Sent {
        let Some(s) = self.subs.get_mut(&sub) else {
            return Sent::Closed;
        };
        let msg = Outgoing::Gated {
            open: s.gate.clone(),
            msg: Box::new(Outgoing::Msg(msg)),
        };
        match s.out.try_send(msg) {
            Ok(()) => Sent::Ok,
            Err(mpsc::error::TrySendError::Full(_)) => {
                s.lagging = true;
                Sent::Full
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                self.subs.remove(&sub);
                Sent::Closed
            }
        }
    }

    /// The rows back from disk (a warm entry someone subscribes to): the base and the whole
    /// log (a running entry's log is all its own). Also the log, as the memory log.
    async fn load_rows(&mut self) {
        let Some(store) = self.store().cloned() else {
            return;
        };
        let started = Instant::now();
        store.flush().await;
        let key = self.key.clone();
        let loaded = {
            let store = store.clone();
            tokio::task::spawn_blocking(move || store.load(&key)).await
        };
        match loaded {
            Ok(Ok(Some(stored))) => match store::replay(&stored, false) {
                Ok((rows, log)) => {
                    self.rows = rows;
                    self.rows_count = self.rows.rows.len();
                    self.log = log
                        .into_iter()
                        .map(|i| LogItem {
                            at: i.at,
                            body: i.body,
                        })
                        .collect();
                    self.log_start = stored.base_at;
                    self.trim_log();
                    self.in_memory = true;
                    debug!(target: "chlive_server::store", key = %self.fp, lineage = %self.lineage, rows = self.rows_count, ms = started.elapsed().as_millis() as u64, "warm rows loaded from disk");
                }
                Err(e) => warn!(key = %self.fp, error = %e, "the stored log didn't apply"),
            },
            other => {
                warn!(key = %self.fp, result = ?other.map(|r| r.map(|s| s.is_some())), "loading the warm rows failed")
            }
        }
    }

    async fn command(&mut self, cmd: Cmd, rx: &mpsc::UnboundedReceiver<Cmd>) -> bool {
        match cmd {
            Cmd::Join {
                sub,
                conn,
                client_id,
                out,
                since,
                alive,
                gate,
            } => {
                let mut source = Source::Memory;
                if self.stage != Stage::Live {
                    debug!(key = %self.fp, lineage = %self.lineage, from = self.stage.name(), "entry is live again");
                    if self.stage == Stage::Warm {
                        self.hub.set_warm(&self.key, self.id, false);
                        let started = Instant::now();
                        self.load_rows().await;
                        source = Source::Warm {
                            load_ms: started.elapsed().as_millis() as u64,
                        };
                        if let Some(store) = self.store() {
                            store.stage(&self.key, "live");
                        }
                    }
                    self.stage = Stage::Live;
                    self.deadline = None;
                }
                self.subs.insert(
                    sub,
                    Sub {
                        conn,
                        client_id: client_id.clone(),
                        out,
                        alive,
                        gate,
                        delivered: None,
                        waiting: None,
                        lagging: false,
                        joined_at: Instant::now(),
                        served: None,
                    },
                );
                self.served += 1;
                self.join(sub, conn, client_id, since, source).await;
            }
            Cmd::Leave { sub } => {
                if let Some(s) = self.subs.remove(&sub) {
                    debug!(key = %self.fp, lineage = %self.lineage, sub, conn = s.conn, id = %s.client_id, left = self.subs.len(), "leave");
                }
                if self.subs.is_empty() && self.stage == Stage::Live {
                    self.stage = Stage::KeepAlive;
                    self.deadline = Some(tokio::time::Instant::now() + self.hub.cache.keep_alive);
                    debug!(key = %self.fp, lineage = %self.lineage, secs = self.hub.cache.keep_alive.as_secs_f64(), "no subscribers: keep-alive");
                }
            }
            Cmd::Evict => {
                if self.subs.is_empty() && self.stage == Stage::Warm {
                    return !self.go_cold("max_warm", rx).await;
                }
            }
            Cmd::Shutdown(_) => unreachable!("handled in run"),
        }
        self.publish();
        true
    }

    /// How a new subscriber starts: a snapshot, or a resume from the memory log, or from the
    /// disk log when the memory one doesn't reach back far enough.
    async fn join(
        &mut self,
        sub: u64,
        conn: u64,
        client_id: String,
        since: Option<String>,
        source: Source,
    ) {
        let cursor = since.is_some();
        let since_text = since.clone().unwrap_or_default();
        let waiting_since = since.clone();
        let decoded = since.map(|t| match &self.form {
            Some(f) => cursor::decode_cursor(&f.plan, &t).ok_or(()),
            None => Err(()),
        });
        let client_at = decoded
            .as_ref()
            .and_then(|d| d.as_ref().ok())
            .map(|c| c.at.clone());
        let mut decision = match &self.at {
            Some(at) => {
                let ends: Vec<&Watermark> = self.log.iter().map(|i| &i.at).collect();
                decide(
                    &self.lineage,
                    &self.forks,
                    self.ready,
                    at,
                    &self.log_start,
                    &ends,
                    decoded.clone(),
                )
            }
            None => Decision::Wait,
        };
        // older than the memory log: the disk log may reach back further
        let mut from_disk: Option<Vec<store::LogItem>> = None;
        if decision == Decision::Snapshot(Reason::BeforeLog) {
            if let (Some(store), Some(at)) = (self.store().cloned(), self.at.clone()) {
                store.flush().await;
                let key = self.key.clone();
                let loaded = tokio::task::spawn_blocking(move || store.load(&key)).await;
                if let Ok(Ok(Some(stored))) = loaded {
                    let ends: Vec<&Watermark> = stored.log.iter().map(|i| &i.at).collect();
                    let d = decide(
                        &self.lineage,
                        &self.forks,
                        self.ready,
                        &at,
                        &stored.base_at,
                        &ends,
                        decoded,
                    );
                    if let Decision::Resume { .. } = d {
                        decision = d;
                        from_disk = Some(stored.log);
                    }
                }
            }
        }
        let mut info = self.info.lock().unwrap();
        match decision {
            Decision::Wait => info.joins_wait += 1,
            Decision::Snapshot(_) => info.joins_snapshot += 1,
            Decision::Resume { .. } => info.joins_resume += 1,
        }
        drop(info);
        match decision {
            Decision::Wait => {
                debug!(key = %self.fp, lineage = %self.lineage, sub, conn, id = %client_id, decision = "wait", "join: the result is still loading");
                if let Some(s) = self.subs.get_mut(&sub) {
                    s.waiting = Some(waiting_since);
                }
            }
            Decision::Snapshot(reason) => {
                debug!(key = %self.fp, lineage = %self.lineage, sub, conn, id = %client_id, decision = "snapshot", ?reason, since = %since_text, rows = self.rows_count, ?source, "join");
                self.record(sub, cursor, source, Some(reason), None);
                let body = self.snapshot_body();
                let at = self.at.clone().expect("ready");
                self.deliver(sub, body, &at);
            }
            Decision::Resume { from } => {
                let items: Vec<(Arc<str>, Watermark)> = match &from_disk {
                    Some(log) => log
                        .iter()
                        .skip(from)
                        .map(|i| (i.body.clone(), i.at.clone()))
                        .collect(),
                    None => self
                        .log
                        .iter()
                        .skip(from)
                        .map(|i| (i.body.clone(), i.at.clone()))
                        .collect(),
                };
                let log = if from_disk.is_some() {
                    "disk"
                } else {
                    "memory"
                };
                debug!(key = %self.fp, lineage = %self.lineage, sub, conn, id = %client_id, decision = "resume", since = %since_text, diffs = items.len(), log, ?source, "join");
                self.record(sub, cursor, source, None, Some((log, items.len())));
                let form = self.form.as_ref().expect("subscribed");
                let resume = ServerMsg::Resume {
                    id: client_id,
                    columns: form.columns.clone(),
                    order: form.order.clone(),
                    limit: form.limit,
                };
                // the client has its rows up to its cursor; the log brings it up to date
                if let Some(s) = self.subs.get_mut(&sub) {
                    s.delivered = client_at;
                }
                if let Sent::Ok = self.send_msg(sub, resume) {
                    self.replay(sub, items);
                }
            }
        }
    }

    /// Log messages to a subscriber; it stays lagging (from where it got to) if its queue fills
    /// up.
    fn replay(&mut self, sub: u64, items: Vec<(Arc<str>, Watermark)>) {
        for (body, at) in items {
            match self.deliver(sub, body, &at) {
                Sent::Ok => {}
                Sent::Full | Sent::Closed => return,
            }
        }
        if let Some(s) = self.subs.get_mut(&sub) {
            s.lagging = false;
        }
    }

    /// Lagging subscribers whose queue has room again: the log since what they got, or a
    /// snapshot if it no longer reaches back.
    fn catch_up_lagging(&mut self) {
        let lagging: Vec<u64> = self
            .subs
            .iter()
            .filter(|(_, s)| s.lagging && s.out.capacity() * 2 >= s.out.max_capacity())
            .map(|(k, _)| *k)
            .collect();
        for sub in lagging {
            let Some(at) = self.at.clone() else { return };
            let delivered = self.subs[&sub].delivered.clone();
            let ends: Vec<&Watermark> = self.log.iter().map(|i| &i.at).collect();
            let decision = decide(
                &self.lineage,
                &[],
                self.ready,
                &at,
                &self.log_start,
                &ends,
                delivered.map(|d| {
                    Ok(Cursor {
                        at: d,
                        lineage: Some(self.lineage.clone()),
                    })
                }),
            );
            self.info.lock().unwrap().resyncs += 1;
            match decision {
                Decision::Resume { from } => {
                    debug!(key = %self.fp, lineage = %self.lineage, sub, diffs = self.log.len() - from, "lagging subscriber catches up from the log");
                    let items = self
                        .log
                        .iter()
                        .skip(from)
                        .map(|i| (i.body.clone(), i.at.clone()))
                        .collect();
                    self.replay(sub, items);
                }
                _ => {
                    debug!(key = %self.fp, lineage = %self.lineage, sub, rows = self.rows_count, "lagging subscriber is past the log: a snapshot");
                    if let Some(s) = self.subs.get_mut(&sub) {
                        s.lagging = false;
                    }
                    let body = self.snapshot_body();
                    self.deliver(sub, body, &at);
                }
            }
        }
        self.publish();
    }

    /// Keep-alive over: warm (persisted), or gone. Warm over: cold. Returns false when the
    /// entry has ended.
    async fn next_stage(&mut self, rx: &mpsc::UnboundedReceiver<Cmd>) -> bool {
        let cache = self.hub.cache.clone();
        match self.stage {
            Stage::KeepAlive => {
                if !self.persisted {
                    if self.hub.remove_if_idle(&self.key, self.id, rx) {
                        self.dropped("not kept (no disk store, or no checkpoints for this query)");
                        return false;
                    }
                    self.deadline = None;
                } else if cache.keep_warm.is_zero() {
                    if self.go_cold("keep_warm = 0", rx).await {
                        return false;
                    }
                } else {
                    // warm: the rows go to disk only; still following Kafka
                    self.checkpoint("warm");
                    self.rows = ResultList::default();
                    self.log.clear();
                    self.in_memory = false;
                    self.stage = Stage::Warm;
                    self.deadline = Some(tokio::time::Instant::now() + cache.keep_warm);
                    if let Some(store) = self.store() {
                        store.stage(&self.key, "warm");
                    }
                    info!(key = %self.fp, lineage = %self.lineage, rows = self.rows_count, secs = cache.keep_warm.as_secs_f64(), "warm: rows on disk only, still following Kafka");
                    self.hub.set_warm(&self.key, self.id, true);
                }
            }
            Stage::Warm => {
                if self.go_cold("keep_warm over", rx).await {
                    return false;
                }
            }
            Stage::Live => self.deadline = None,
        }
        self.publish();
        true
    }

    /// Checkpoint, mark cold, and end (its Kafka consumer with it). Returns whether it went (a
    /// join that arrived meanwhile keeps it).
    async fn go_cold(&mut self, reason: &str, rx: &mpsc::UnboundedReceiver<Cmd>) -> bool {
        if !self.hub.remove_if_idle(&self.key, self.id, rx) {
            self.deadline = None;
            return false;
        }
        self.checkpoint("cold");
        if let Some(store) = self.store() {
            store.stage(&self.key, "cold");
        }
        info!(key = %self.fp, lineage = %self.lineage, reason, rows = self.rows_count, at = %self.at.as_ref().map(Watermark::describe).unwrap_or_default(), lived_secs = self.created.elapsed().as_secs_f64(), "cold: stopped, kept on disk");
        true
    }

    /// The server is stopping: persisted entries are checkpointed and left cold.
    async fn shutdown(&mut self) {
        if self.persisted && self.ready {
            self.checkpoint("shutdown");
            if let Some(store) = self.store() {
                store.stage(&self.key, "cold");
                store.flush().await;
            }
        }
        self.dropped("shutdown");
    }

    fn fail(&mut self, e: &EngineError, started: Instant) {
        warn!(key = %self.fp, lineage = %self.lineage, error = %e, secs = started.elapsed().as_secs_f64(), "entry failed");
        let subs: Vec<u64> = self.subs.keys().copied().collect();
        for sub in subs {
            let id = self.subs[&sub].client_id.clone();
            self.subs[&sub].alive.store(false, Ordering::Relaxed);
            self.send_msg(sub, engine_error(&id, e));
        }
        // what's stored stays (its last checkpoint is still a good start), cold
        if let Some(store) = self.store() {
            store.stage(&self.key, "cold");
        }
        self.hub.remove(&self.key, self.id);
        self.dropped("error");
    }

    fn dropped(&self, reason: &str) {
        for s in self.subs.values() {
            s.alive.store(false, Ordering::Relaxed);
        }
        info!(
            key = %self.fp,
            lineage = %self.lineage,
            reason,
            lived_secs = self.created.elapsed().as_secs_f64(),
            rows = self.rows_count,
            served = self.served,
            messages = self.info.lock().unwrap().messages,
            "entry ended"
        );
    }

    /// The debug view, updated after each step.
    fn publish(&self) {
        let mut info = self.info.lock().unwrap();
        info.stage = if self.form.is_none() || !self.ready {
            "starting"
        } else {
            self.stage.name()
        };
        // as a time, so listings count down between steps
        info.stage_ends_at = self.deadline.map(|d| {
            let left = d.saturating_duration_since(tokio::time::Instant::now());
            store::now_ms() as u64 + left.as_millis() as u64
        });
        info.subscribers = self
            .subs
            .values()
            .map(|s| SubscriberInfo {
                conn: s.conn,
                id: s.client_id.clone(),
                lagging: s.lagging,
                served: s.served.clone(),
            })
            .collect();
        info.rows = self.rows_count;
        info.in_memory = self.in_memory;
        info.persisted = self.persisted;
        info.restored = self.restored;
        info.at = self.at.as_ref().map(Watermark::describe);
        info.log_length = self.log.len();
        info.log_start = self.ready.then(|| self.log_start.describe());
    }
}

/// Start an entry: from the store if it has the result and it's still good (young enough,
/// still in Kafka, not too far behind), else from a ClickHouse snapshot. A stored result that
/// isn't good anymore is deleted.
async fn start(
    hub: Arc<Hub>,
    key: String,
    fp: String,
    sql: String,
    params: Params,
    filters: Filters,
) -> Result<Start, EngineError> {
    let mut rejected = None;
    if let Some(store) = hub.store.clone() {
        let started = Instant::now();
        let k = key.clone();
        let loaded = {
            let store = store.clone();
            tokio::task::spawn_blocking(move || store.load(&k)).await
        };
        match loaded {
            Ok(Ok(Some(stored))) => {
                match restorable(&hub, &store, &stored, &sql, &params, &filters).await {
                    Ok((sub, rows, log, checkpoint, lag)) => {
                        return Ok(Start::Restored {
                            sub,
                            stored: Box::new(stored),
                            rows: Box::new(rows),
                            log,
                            checkpoint,
                            lag,
                            ms: started.elapsed().as_millis() as u64,
                        })
                    }
                    Err(reason) => {
                        info!(key = %fp, lineage = %stored.meta.lineage, reason, "the stored result can't be used: starting from ClickHouse");
                        store.delete(&key, &reason);
                        rejected = Some(reason);
                    }
                }
            }
            Ok(Ok(None)) => {}
            other => {
                warn!(key = %fp, result = ?other.map(|r| r.map(|s| s.is_some())), "reading the cache store failed")
            }
        }
    }
    hub.engine
        .subscribe_with(&sql, &params, &filters, None)
        .await
        .map(|sub| Start::Fresh { sub, rejected })
}

type Restorable = (
    Subscription,
    ResultList,
    Vec<store::LogItem>,
    Checkpoint,
    u64,
);

/// Whether a stored result can continue: young enough, in Kafka, not too far behind, and its
/// checkpoint fits the query. The rows are the base plus the log up to the checkpoint; log
/// messages after it (an unclean stop) are dropped.
async fn restorable(
    hub: &Hub,
    store: &Store,
    stored: &Stored,
    sql: &str,
    params: &Params,
    filters: &Filters,
) -> Result<Restorable, String> {
    let cache = &hub.cache;
    let state = stored.state.as_ref().ok_or("no checkpoint")?;
    let age = Duration::from_millis((store::now_ms() - stored.meta.born_at).max(0) as u64);
    if age > cache.max_age {
        return Err(format!(
            "max_age: its snapshot is {}s old (max {}s)",
            age.as_secs(),
            cache.max_age.as_secs()
        ));
    }
    let checkpoint = Checkpoint::from_json(state)?;
    let plan = hub
        .engine
        .explain_with(sql, params, filters)
        .await
        .map_err(|e| format!("the query doesn't plan anymore: {e}"))?;
    if cursor::identity(&plan) != stored.meta.key {
        return Err("the query plans differently now (schema or mapping)".into());
    }
    let lag = hub
        .engine
        .catch_up_events(&plan, &checkpoint.at)
        .await
        .map_err(|e| format!("Kafka: {e}"))?
        .ok_or("retention: its cursor is no longer in Kafka (or the topic was recreated)")?;
    if lag > cache.max_catch_up {
        return Err(format!(
            "max_catch_up: {lag} events behind (max {})",
            cache.max_catch_up
        ));
    }
    let (rows, log) = store::replay(stored, true)?;
    if log.len() < stored.log.len() {
        store.truncate(&stored.meta.key);
    }
    let sub = hub
        .engine
        .restore(sql, params, filters, &checkpoint)
        .await
        .map_err(|e| format!("restoring: {e}"))?;
    Ok((sub, rows, log, checkpoint, lag))
}

async fn next_batch(
    stream: &mut Option<ReadyChunks<Subscription>>,
) -> Option<Vec<Result<Iteration, EngineError>>> {
    match stream {
        Some(s) => s.next().await,
        None => std::future::pending().await,
    }
}

async fn sleep_until(deadline: Option<tokio::time::Instant>) {
    match deadline {
        Some(d) => tokio::time::sleep_until(d).await,
        None => std::future::pending().await,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn w(pairs: &[(i32, i64)]) -> Watermark {
        Watermark(pairs.iter().copied().collect())
    }

    fn c(lineage: &str, pairs: &[(i32, i64)]) -> Option<Result<Cursor, ()>> {
        Some(Ok(Cursor {
            at: w(pairs),
            lineage: Some(lineage.into()),
        }))
    }

    #[test]
    fn deciding_how_a_subscriber_starts() {
        // history at {0: 10, 1: 5}; then three diffs; the log keeps the last two
        let start = w(&[(0, 12), (1, 5)]);
        let (d1, d2) = (w(&[(0, 14), (1, 5)]), w(&[(0, 14), (1, 9)]));
        let at = d2.clone();
        let log = [&d1, &d2];
        let d = |since| decide("L", &[], true, &at, &start, &log, since);
        assert_eq!(
            decide("L", &[], false, &at, &start, &log, None),
            Decision::Wait
        );
        assert_eq!(d(None), Decision::Snapshot(Reason::NoCursor));
        assert_eq!(d(Some(Err(()))), Decision::Snapshot(Reason::BadToken));
        assert_eq!(
            d(c("other", &[(0, 14), (1, 9)])),
            Decision::Snapshot(Reason::OtherLineage)
        );
        let no_lineage = Some(Ok(Cursor {
            at: at.clone(),
            lineage: None,
        }));
        assert_eq!(d(no_lineage), Decision::Snapshot(Reason::OtherLineage));
        // up to date: nothing to replay
        assert_eq!(d(c("L", &[(0, 14), (1, 9)])), Decision::Resume { from: 2 });
        // exactly at the log's start: all of it
        assert_eq!(d(c("L", &[(0, 12), (1, 5)])), Decision::Resume { from: 0 });
        // between two messages (a `progress` cursor): from the first it hasn't got
        assert_eq!(d(c("L", &[(0, 13), (1, 5)])), Decision::Resume { from: 0 });
        assert_eq!(d(c("L", &[(0, 14), (1, 5)])), Decision::Resume { from: 1 });
        assert_eq!(d(c("L", &[(0, 14), (1, 7)])), Decision::Resume { from: 1 });
        // older than the log
        assert_eq!(
            d(c("L", &[(0, 11), (1, 5)])),
            Decision::Snapshot(Reason::BeforeLog)
        );
        // past the entry (in any partition)
        assert_eq!(
            d(c("L", &[(0, 14), (1, 10)])),
            Decision::Snapshot(Reason::Ahead)
        );
    }

    #[test]
    fn cursors_of_earlier_lineages_resume_up_to_where_they_ended() {
        // restored from lineage "old" at {0: 13}: "old" cursors up to there are ours too
        let start = w(&[(0, 12)]);
        let (d1, d2) = (w(&[(0, 13)]), w(&[(0, 15)]));
        let at = d2.clone();
        let log = [&d1, &d2];
        let forks = [("old".to_string(), w(&[(0, 13)]))];
        let d = |since| decide("new", &forks, true, &at, &start, &log, since);
        assert_eq!(d(c("old", &[(0, 12)])), Decision::Resume { from: 0 });
        assert_eq!(d(c("old", &[(0, 13)])), Decision::Resume { from: 1 });
        // past where "old" ended: its rows may differ from ours
        assert_eq!(
            d(c("old", &[(0, 14)])),
            Decision::Snapshot(Reason::OtherLineage)
        );
        assert_eq!(d(c("new", &[(0, 15)])), Decision::Resume { from: 2 });
        assert_eq!(
            d(c("older", &[(0, 12)])),
            Decision::Snapshot(Reason::OtherLineage)
        );
    }

    #[test]
    fn shared_messages_get_each_subscribers_id() {
        let body = shared(&ServerMsg::Progress {
            id: PLACEHOLDER.into(),
            at: "c".into(),
        });
        let out = |id: &str| {
            Outgoing::Shared {
                id: id.into(),
                body: body.clone(),
            }
            .render()
            .unwrap()
        };
        assert_eq!(out("s1"), r#"{"type":"progress","id":"s1","at":"c"}"#);
        // ids are JSON-escaped
        assert_eq!(out("a\"b"), r#"{"type":"progress","id":"a\"b","at":"c"}"#);
        // a value that looks like the placeholder isn't touched (only the id is)
        let body = shared(&ServerMsg::Progress {
            id: PLACEHOLDER.into(),
            at: PLACEHOLDER.into(),
        });
        let s = Outgoing::Shared {
            id: "x".into(),
            body,
        }
        .render()
        .unwrap();
        assert!(s.starts_with(r#"{"type":"progress","id":"x""#), "{s}");
    }

    #[test]
    fn cache_defaults() {
        let c = CacheSettings::default();
        assert_eq!(c.dir, None);
        assert_eq!(c.keep_alive, Duration::from_secs(30));
        assert_eq!(c.keep_warm, Duration::from_secs(3600));
        assert_eq!(c.max_warm, 100);
        assert_eq!(c.log_length, 1000);
        assert_eq!(c.disk_budget, 10 << 30);
        assert_eq!(c.max_age, Duration::from_secs(86_400));
        assert_eq!(c.max_catch_up, 1_000_000);
        assert_eq!(c.checkpoint_interval, Duration::from_secs(60));
        assert_eq!(c.log_bytes, 16 << 20);
    }
}
