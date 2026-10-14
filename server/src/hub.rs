//! Shared live results: one entry per distinct query (and parameter values), shared by every
//! subscription to it, on any connection.
//!
//! An entry runs one engine subscription and keeps its result (the rows, and the cursor they're
//! complete to), so a new subscriber gets the rows at once, without ClickHouse. It also keeps a
//! bounded log of its recent `diff` messages: a client returning with a cursor from this entry
//! (the same *lineage*) gets just the diffs it missed. After its last subscriber leaves, an entry
//! stays live for `keep_alive`, then *warm* for `keep_warm`: still following Kafka, with nobody
//! connected, so later subscribers get an up-to-date result. Then it's dropped.
//!
//! Lineage: an id made when an entry starts from a ClickHouse snapshot, carried in its cursors.
//! Two entries of one query can hold different rows at the same cursor (ties, arbitrary order,
//! OFFSET's hidden rows), so a log is only ever replayed for a cursor of the same lineage.
//!
//! Each entry is a task owning all of its state; joins and leaves arrive as messages, between
//! iterations, so every snapshot it sends is consistent with its cursor.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use chlive_engine::cursor::{self, Cursor};
use chlive_engine::diff::{Diff, ResultList};
use chlive_engine::plan::Plan;
use chlive_engine::watermark::Watermark;
use chlive_engine::{Engine, EngineError, Iteration, Origin, Params, Subscription};
use futures::stream::ReadyChunks;
use futures::StreamExt;
use serde::Serialize;
use tokio::sync::{mpsc, oneshot};
use tracing::{debug, info, trace, warn};

use crate::protocol::{change, engine_error, order_terms, Column, OrderTerm, ServerMsg};

/// Iterations already waiting are sent together, in one `diff`, up to this many.
pub const MAX_ITERATIONS_PER_MESSAGE: usize = 256;

/// The `[cache]` settings.
#[derive(Debug, Clone, PartialEq)]
pub struct CacheSettings {
    /// After the last subscriber leaves, the entry stays fully live this long.
    pub keep_alive: Duration,
    /// Then it keeps following Kafka with nobody connected this long (0: not at all).
    pub keep_warm: Duration,
    /// Entries kept warm at most; the one warm the longest goes first.
    pub max_warm: usize,
    /// `diff` messages kept per entry, for resuming clients.
    pub log_length: usize,
    /// Results larger than this are shared while subscribed, but not kept warm.
    pub max_shared_rows: usize,
}

impl Default for CacheSettings {
    fn default() -> Self {
        Self {
            keep_alive: Duration::from_secs(30),
            keep_warm: Duration::from_secs(15 * 60),
            max_warm: 100,
            log_length: 1000,
            max_shared_rows: 100_000,
        }
    }
}

/// What a connection's writer sends: its own messages, or an entry's, serialized once for every
/// subscriber, with the subscriber's id put in when written.
pub enum Outgoing {
    Msg(ServerMsg),
    Shared { id: String, body: Arc<str> },
}

/// The id a shared body is serialized with; replaced by each subscriber's own.
const PLACEHOLDER: &str = "\u{1}chlive-subscription\u{1}";

impl Outgoing {
    pub fn render(&self) -> String {
        match self {
            Outgoing::Msg(m) => serde_json::to_string(m).expect("server messages serialize"),
            Outgoing::Shared { id, body } => {
                let placeholder = serde_json::to_string(PLACEHOLDER).expect("a string");
                let id = serde_json::to_string(id).expect("a string");
                body.replacen(&placeholder, &id, 1)
            }
        }
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

/// How a subscriber with `since` starts, given the entry's lineage, cursor, and the cursor each
/// log item ends at (the log starts right after `log_start`).
pub fn decide(
    lineage: &str,
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
    if cursor.lineage.as_deref() != Some(lineage) {
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
    },
    Leave {
        sub: u64,
    },
    /// Too many warm entries: go, if still unsubscribed.
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
}

pub struct Hub {
    engine: Arc<Engine>,
    cache: CacheSettings,
    progress_interval: Duration,
    entries: Mutex<HashMap<String, Handle>>,
    next: AtomicU64,
}

impl Hub {
    pub fn new(
        engine: Arc<Engine>,
        cache: CacheSettings,
        progress_interval: Duration,
    ) -> Arc<Self> {
        Arc::new(Self {
            engine,
            cache,
            progress_interval,
            entries: Mutex::new(HashMap::new()),
            next: AtomicU64::new(1),
        })
    }

    pub fn engine(&self) -> &Arc<Engine> {
        &self.engine
    }

    /// Subscribe to `plan`'s entry, creating it (from a ClickHouse snapshot) if there's none.
    #[allow(clippy::too_many_arguments)]
    pub fn join(
        self: &Arc<Self>,
        plan: &Plan,
        sql: &str,
        params: &Params,
        conn: u64,
        client_id: &str,
        out: mpsc::Sender<Outgoing>,
        since: Option<String>,
    ) -> Joined {
        let key = cursor::identity(plan);
        let sub = self.next.fetch_add(1, Ordering::Relaxed);
        let alive = Arc::new(AtomicBool::new(true));
        let join = Cmd::Join {
            sub,
            conn,
            client_id: client_id.to_string(),
            out,
            since,
            alive: alive.clone(),
        };
        // joins are sent under the lock, so an entry deciding to go (also under it) never
        // misses one
        let mut entries = self.entries.lock().unwrap();
        let join = match entries.get(&key) {
            Some(h) => match h.tx.send(join) {
                Ok(()) => return Joined { key, sub, alive },
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
            info,
        );
        tokio::spawn(entry.run(rx));
        Joined { key, sub, alive }
    }

    pub fn leave(&self, key: &str, sub: u64) {
        if let Some(h) = self.entries.lock().unwrap().get(key) {
            let _ = h.tx.send(Cmd::Leave { sub });
        }
    }

    /// For `/debug/subscriptions`.
    pub fn list(&self) -> Vec<Info> {
        let mut out: Vec<Info> = self
            .entries
            .lock()
            .unwrap()
            .values()
            .map(|h| h.info.lock().unwrap().clone())
            .collect();
        out.sort_by_key(|a| a.created);
        out
    }

    /// Stop every entry (their engine subscriptions with them).
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

    /// An entry becoming warm (or not). Over `max_warm`, the one warm the longest is evicted.
    fn set_warm(&self, key: &str, id: u64, warm: bool) {
        let mut entries = self.entries.lock().unwrap();
        if let Some(h) = entries.get_mut(key).filter(|h| h.id == id) {
            h.warm_since = warm.then(Instant::now);
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
                debug!(key = %cursor::hash(k), max_warm = self.cache.max_warm, "evicting the longest-warm entry");
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
    /// `starting`, `live`, `keep_alive` or `warm`.
    pub stage: &'static str,
    /// Seconds until the next stage (keep-alive, warm).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub stage_ends_in: Option<f64>,
    pub subscribers: Vec<SubscriberInfo>,
    pub rows: usize,
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
    pub joins_snapshot: u64,
    pub joins_resume: u64,
    pub joins_wait: u64,
    pub resyncs: u64,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SubscriberInfo {
    pub conn: u64,
    pub id: String,
    pub lagging: bool,
}

impl Info {
    fn new(key: &str, sql: &str, params: &Params, lineage: &str) -> Self {
        Self {
            key: key.to_string(),
            sql: sql.to_string(),
            params: params.clone(),
            lineage: lineage.to_string(),
            stage: "starting",
            stage_ends_in: None,
            subscribers: Vec::new(),
            rows: 0,
            at: None,
            log_length: 0,
            log_start: None,
            created: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
            history_ms: None,
            messages: 0,
            joins_snapshot: 0,
            joins_resume: 0,
            joins_wait: 0,
            resyncs: 0,
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
    /// The cursor of the last message it got.
    delivered: Option<Watermark>,
    /// Joined before the history was in: gets a snapshot when it is.
    waiting: bool,
    /// Its connection's queue was full: nothing is sent until it catches up.
    lagging: bool,
}

struct LogItem {
    /// The cursor the result is complete to after this message.
    at: Watermark,
    body: Arc<str>,
    changes: usize,
}

/// What the subscription's plan says about the result's form.
struct Form {
    plan: Plan,
    columns: Vec<Column>,
    order: Option<Vec<OrderTerm>>,
    limit: Option<u64>,
    offset: Option<u64>,
    aggregated: bool,
}

enum Sent {
    Ok,
    Full,
    Closed,
}

struct Entry {
    hub: Arc<Hub>,
    key: String,
    /// The key's fingerprint, for logs.
    fp: String,
    id: u64,
    lineage: String,
    sql: String,
    params: Params,
    info: Arc<Mutex<Info>>,
    form: Option<Form>,
    rows: ResultList,
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
        info: Arc<Mutex<Info>>,
    ) -> Self {
        let progress_interval = hub.progress_interval;
        Self {
            hub,
            key,
            fp,
            id,
            lineage,
            sql,
            params,
            info,
            form: None,
            rows: ResultList::default(),
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
        }
    }

    async fn run(mut self, mut rx: mpsc::UnboundedReceiver<Cmd>) {
        let engine = self.hub.engine.clone();
        let (sql, params) = (self.sql.clone(), self.params.clone());
        let started = Instant::now();
        let mut starting =
            Box::pin(async move { engine.subscribe_with(&sql, &params, None).await });
        let mut stream: Option<ReadyChunks<Subscription>> = None;
        let mut subscribed = false;
        let mut tick = tokio::time::interval(Duration::from_millis(200));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            let deadline = self.deadline;
            let lagging = self.subs.values().any(|s| s.lagging);
            tokio::select! {
                // joins and leaves first: a subscriber that joined before the history gets it
                // as its snapshot, and the changes after as diffs
                biased;
                cmd = rx.recv() => match cmd {
                    Some(Cmd::Shutdown(done)) => {
                        self.dropped("shutdown");
                        let _ = done.send(());
                        return;
                    }
                    Some(cmd) => if !self.command(cmd, &rx) { return },
                    None => return,
                },
                r = &mut starting, if !subscribed => {
                    subscribed = true;
                    match r {
                        Ok(s) => {
                            self.subscribed(&s);
                            stream = Some(s.ready_chunks(MAX_ITERATIONS_PER_MESSAGE));
                        }
                        Err(e) => {
                            self.fail(&e, started);
                            return;
                        }
                    }
                }
                batch = next_batch(&mut stream) => match batch {
                    Some(batch) => if !self.batch(batch, started) { return },
                    // a finite source (tests) ended: the result stays as it is
                    None => stream = None,
                },
                _ = sleep_until(deadline) => if !self.next_stage(&rx) { return },
                _ = tick.tick(), if lagging => self.catch_up_lagging(),
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
        debug!(key = %self.fp, lineage = %self.lineage, watermark = %s.watermark.describe(), history = %s.history_sql, "entry subscribed");
        self.form = Some(Form {
            aggregated: plan.aggregate.is_some(),
            plan,
            columns,
            order,
            limit,
            offset,
        });
    }

    fn encode(&self, at: &Watermark) -> String {
        let form = self.form.as_ref().expect("subscribed");
        cursor::encode_with(&form.plan, Some(&self.lineage), at)
    }

    /// One batch of iterations: the history (once), then diffs and progress, sent to everyone
    /// and logged. Returns false when the entry has failed.
    fn batch(&mut self, batch: Vec<Result<Iteration, EngineError>>, started: Instant) -> bool {
        let mut diffs: Vec<Diff> = Vec::new();
        let mut at: Option<Watermark> = None;
        let mut catch_up = false;
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
                    self.ready = true;
                    self.log_start = it.at.clone();
                    self.at = Some(it.at.clone());
                    self.sent_at = Some(it.at);
                    let ms = started.elapsed().as_millis() as u64;
                    info!(key = %self.fp, lineage = %self.lineage, rows = self.rows.rows.len(), ms, "history loaded");
                    self.info.lock().unwrap().history_ms = Some(ms);
                    let waiting: Vec<u64> = self
                        .subs
                        .iter()
                        .filter(|(_, s)| s.waiting)
                        .map(|(k, _)| *k)
                        .collect();
                    if !waiting.is_empty() {
                        let body = self.snapshot_body();
                        let at = self.at.clone().expect("ready");
                        for sub in waiting {
                            if let Some(s) = self.subs.get_mut(&sub) {
                                s.waiting = false;
                            }
                            debug!(key = %self.fp, lineage = %self.lineage, sub, "waiting subscriber gets the snapshot");
                            self.deliver(sub, body.clone(), &at);
                        }
                    }
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
            } else {
                self.at = Some(a);
            }
        }
        self.publish();
        true
    }

    /// One `diff` message: applied to the rows, logged, sent.
    fn diff(&mut self, diffs: Vec<Diff>, at: Watermark) {
        for d in &diffs {
            if let Err(e) = self.rows.apply(d) {
                // the engine's own diffs: a bug if they don't apply
                warn!(key = %self.fp, lineage = %self.lineage, error = %e, "a diff didn't apply to the shared rows");
            }
        }
        let changes: Vec<_> = diffs.into_iter().map(change).collect();
        let n = changes.len();
        let body = shared(&ServerMsg::Diff {
            id: PLACEHOLDER.into(),
            changes,
            at: self.encode(&at),
        });
        trace!(key = %self.fp, changes = n, rows = self.rows.rows.len(), log = self.log.len(), at = %at.describe(), "diff");
        self.log.push_back(LogItem {
            at: at.clone(),
            body: body.clone(),
            changes: n,
        });
        while self.log.len() > self.hub.cache.log_length {
            if let Some(gone) = self.log.pop_front() {
                self.log_start = gone.at;
            }
        }
        if self.log.len() == self.hub.cache.log_length {
            trace!(key = %self.fp, log_start = %self.log_start.describe(), "log trimmed");
        }
        self.at = Some(at.clone());
        self.sent_at = Some(at.clone());
        self.info.lock().unwrap().messages += 1;
        self.broadcast(&body, &at);
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
            ids: form.aggregated.then(|| {
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
            .filter(|(_, s)| !s.waiting && !s.lagging)
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
        let msg = Outgoing::Shared {
            id: s.client_id.clone(),
            body,
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
        match s.out.try_send(Outgoing::Msg(msg)) {
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

    fn command(&mut self, cmd: Cmd, rx: &mpsc::UnboundedReceiver<Cmd>) -> bool {
        match cmd {
            Cmd::Join {
                sub,
                conn,
                client_id,
                out,
                since,
                alive,
            } => {
                if self.stage != Stage::Live {
                    debug!(key = %self.fp, lineage = %self.lineage, from = self.stage.name(), "entry is live again");
                    if self.stage == Stage::Warm {
                        self.hub.set_warm(&self.key, self.id, false);
                    }
                    self.stage = Stage::Live;
                    self.deadline = None;
                }
                let since_text = since.clone().unwrap_or_default();
                let decoded = since.map(|t| match &self.form {
                    Some(f) => cursor::decode_cursor(&f.plan, &t).ok_or(()),
                    None => Err(()),
                });
                let client_at = decoded
                    .as_ref()
                    .and_then(|d| d.as_ref().ok())
                    .map(|c| c.at.clone());
                let decision = match &self.at {
                    Some(at) => {
                        let ends: Vec<&Watermark> = self.log.iter().map(|i| &i.at).collect();
                        decide(
                            &self.lineage,
                            self.ready,
                            at,
                            &self.log_start,
                            &ends,
                            decoded,
                        )
                    }
                    None => Decision::Wait,
                };
                self.subs.insert(
                    sub,
                    Sub {
                        conn,
                        client_id: client_id.clone(),
                        out,
                        alive,
                        delivered: None,
                        waiting: false,
                        lagging: false,
                    },
                );
                self.served += 1;
                let mut info = self.info.lock().unwrap();
                match decision {
                    Decision::Wait => info.joins_wait += 1,
                    Decision::Snapshot(_) => info.joins_snapshot += 1,
                    Decision::Resume { .. } => info.joins_resume += 1,
                }
                drop(info);
                match decision {
                    Decision::Wait => {
                        debug!(key = %self.fp, lineage = %self.lineage, sub, conn, id = %client_id, decision = "wait", "join: history still loading");
                        if let Some(s) = self.subs.get_mut(&sub) {
                            s.waiting = true;
                        }
                    }
                    Decision::Snapshot(reason) => {
                        debug!(key = %self.fp, lineage = %self.lineage, sub, conn, id = %client_id, decision = "snapshot", ?reason, since = %since_text, rows = self.rows.rows.len(), "join");
                        let body = self.snapshot_body();
                        let at = self.at.clone().expect("ready");
                        self.deliver(sub, body, &at);
                    }
                    Decision::Resume { from } => {
                        let replayed = self.log.len() - from;
                        let changes: usize = self.log.iter().skip(from).map(|i| i.changes).sum();
                        debug!(key = %self.fp, lineage = %self.lineage, sub, conn, id = %client_id, decision = "resume", since = %since_text, diffs = replayed, changes, "join");
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
                            self.replay(sub, from);
                        }
                    }
                }
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
                if self.subs.is_empty()
                    && self.stage == Stage::Warm
                    && self.hub.remove_if_idle(&self.key, self.id, rx)
                {
                    self.dropped("evicted (max_warm)");
                    return false;
                }
            }
            Cmd::Shutdown(_) => unreachable!("handled in run"),
        }
        self.publish();
        true
    }

    /// The log from `from` to a subscriber; it stays lagging (from where it got to) if its queue
    /// fills up.
    fn replay(&mut self, sub: u64, from: usize) {
        let items: Vec<(Arc<str>, Watermark)> = self
            .log
            .iter()
            .skip(from)
            .map(|i| (i.body.clone(), i.at.clone()))
            .collect();
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
                    self.replay(sub, from);
                }
                _ => {
                    debug!(key = %self.fp, lineage = %self.lineage, sub, rows = self.rows.rows.len(), "lagging subscriber is past the log: a snapshot");
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

    /// Keep-alive over: warm, or gone. Warm over: gone. Returns false when gone.
    fn next_stage(&mut self, rx: &mpsc::UnboundedReceiver<Cmd>) -> bool {
        let cache = &self.hub.cache;
        match self.stage {
            Stage::KeepAlive => {
                let too_big = self.rows.rows.len() > cache.max_shared_rows;
                if cache.keep_warm.is_zero() || too_big {
                    let reason = if too_big {
                        "not kept warm (max_shared_rows)"
                    } else {
                        "not kept warm (keep_warm = 0)"
                    };
                    if self.hub.remove_if_idle(&self.key, self.id, rx) {
                        self.dropped(reason);
                        return false;
                    }
                    self.deadline = None;
                } else {
                    self.stage = Stage::Warm;
                    self.deadline = Some(tokio::time::Instant::now() + cache.keep_warm);
                    debug!(key = %self.fp, lineage = %self.lineage, secs = cache.keep_warm.as_secs_f64(), "keep-alive over: warm (following Kafka with nobody connected)");
                    self.hub.set_warm(&self.key, self.id, true);
                }
            }
            Stage::Warm => {
                if self.hub.remove_if_idle(&self.key, self.id, rx) {
                    self.hub.set_warm(&self.key, self.id, false);
                    self.dropped("warm expired");
                    return false;
                }
                self.deadline = None;
            }
            Stage::Live => self.deadline = None,
        }
        self.publish();
        true
    }

    fn fail(&mut self, e: &EngineError, started: Instant) {
        warn!(key = %self.fp, lineage = %self.lineage, error = %e, secs = started.elapsed().as_secs_f64(), "entry failed");
        let subs: Vec<u64> = self.subs.keys().copied().collect();
        for sub in subs {
            let id = self.subs[&sub].client_id.clone();
            self.subs[&sub].alive.store(false, Ordering::Relaxed);
            self.send_msg(sub, engine_error(&id, e));
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
            rows = self.rows.rows.len(),
            served = self.served,
            messages = self.info.lock().unwrap().messages,
            "entry dropped"
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
        info.stage_ends_in = self.deadline.map(|d| {
            d.saturating_duration_since(tokio::time::Instant::now())
                .as_secs_f64()
        });
        info.subscribers = self
            .subs
            .values()
            .map(|s| SubscriberInfo {
                conn: s.conn,
                id: s.client_id.clone(),
                lagging: s.lagging,
            })
            .collect();
        info.rows = self.rows.rows.len();
        info.at = self.at.as_ref().map(Watermark::describe);
        info.log_length = self.log.len();
        info.log_start = self.ready.then(|| self.log_start.describe());
    }
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
        let d = |since| decide("L", true, &at, &start, &log, since);
        assert_eq!(decide("L", false, &at, &start, &log, None), Decision::Wait);
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
        .render();
        assert!(s.starts_with(r#"{"type":"progress","id":"x""#), "{s}");
    }

    #[test]
    fn cache_defaults() {
        let c = CacheSettings::default();
        assert_eq!(c.keep_alive, Duration::from_secs(30));
        assert_eq!(c.keep_warm, Duration::from_secs(900));
        assert_eq!(c.max_warm, 100);
        assert_eq!(c.log_length, 1000);
        assert_eq!(c.max_shared_rows, 100_000);
    }
}
