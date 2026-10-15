//! Snapshots of ClickHouse tables, lined up with Kafka offsets.
//!
//! Ingestion (Kafka Connect's ClickHouse sink) keeps no per-row offsets, so the engine can't ask
//! a table which offsets it holds. Instead, the pause server pauses a table's ingestion and
//! reports, per partition, the next offset it would ingest; while it's paused, the table is
//! cloned (`CREATE TABLE … AS` + `ATTACH PARTITION ALL FROM`, which only links existing parts),
//! then ingestion resumes. The clone holds exactly the offsets below those: history is the whole
//! clone, and the stream starts where it ends.
//!
//! Any snapshot is correct, however old: an older one only means more of the stream to replay.
//! So subscriptions don't each pause ingestion: a [`SnapshotManager`] task keeps a recent
//! snapshot of every table in use, refreshing it every `refresh` while subscriptions keep
//! coming, and stops after `idle` without any. A clone is dropped once it's been replaced and
//! the last subscription reading it has let go of its [`Lease`].

pub mod clone;
pub mod pause;

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use tokio::sync::{mpsc, oneshot};
use tokio::time::Instant;

use crate::rules::quote_ident;
use crate::watermark::Watermark;
pub use clone::{ClickHouseCloner, Cloner};
pub use pause::{PauseServer, Paused, Pauser};

/// Clones are named `__chlive_snap_<table>_<n>` next to their source table.
pub const CLONE_PREFIX: &str = "__chlive_snap_";

#[derive(Debug, Clone, PartialEq, thiserror::Error)]
#[error("{0}")]
pub struct SnapshotError(pub String);

/// A table to snapshot: where it is in ClickHouse, and the topic it's ingested from.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SnapshotTable {
    pub database: String,
    pub table: String,
    pub topic: String,
}

impl SnapshotTable {
    pub fn new(database: &str, table: &str, topic: &str) -> Self {
        Self {
            database: database.into(),
            table: table.into(),
            topic: topic.into(),
        }
    }
}

/// A clone of a table and the offsets it holds.
#[derive(Debug)]
pub struct Snapshot {
    /// The clone's database (its source's, or the configured snapshot database).
    pub database: String,
    /// The clone's name.
    pub name: String,
    /// Per partition, the last offset in the clone (`-1`: none).
    pub watermark: Watermark,
    /// When the table was paused for it.
    pub taken_at: SystemTime,
    /// Where to ask for the clone to be dropped when the last lease goes.
    on_drop: Option<mpsc::UnboundedSender<Cmd>>,
}

impl Snapshot {
    /// A snapshot nothing manages (tests, fakes): dropping it drops no table.
    pub fn fixed(database: &str, name: &str, watermark: Watermark) -> Self {
        Self {
            database: database.into(),
            name: name.into(),
            watermark,
            taken_at: SystemTime::now(),
            on_drop: None,
        }
    }
}

impl Snapshot {
    /// Which clone this is and when it was taken, for a subscription to say what it read.
    pub fn used(&self) -> UsedSnapshot {
        UsedSnapshot {
            clone: format!("{}.{}", self.database, self.name),
            taken_at: self.taken_at,
        }
    }

    /// The clone, quoted for SQL: what history queries read from.
    pub fn sql_table(&self) -> String {
        format!(
            "{}.{}",
            quote_ident(&self.database),
            quote_ident(&self.name)
        )
    }
}

impl Drop for Snapshot {
    fn drop(&mut self) {
        if let Some(tx) = &self.on_drop {
            let _ = tx.send(Cmd::DropClone(self.database.clone(), self.name.clone()));
        }
    }
}

/// The snapshot a subscription's history was read from.
#[derive(Debug, Clone, PartialEq)]
pub struct UsedSnapshot {
    /// `database.name`
    pub clone: String,
    /// When the table was paused for it: after the subscription started if it was taken for
    /// it, before if an existing one was reused.
    pub taken_at: SystemTime,
}

/// A snapshot in use. Hold it while reading the clone; the clone outlives no lease.
pub type Lease = Arc<Snapshot>;

/// Where subscriptions get their snapshots. Implemented by [`SnapshotManager`] in process; a
/// separate snapshot service can implement it too.
#[async_trait]
pub trait Snapshots: Send + Sync {
    async fn acquire(&self, table: &SnapshotTable) -> Result<Lease, SnapshotError>;
    /// The table is in use while the guard lives (a running subscription): its snapshots keep
    /// being refreshed, and it's idle only from when the last guard goes.
    fn in_use(&self, _table: &SnapshotTable) -> InUse {
        InUse(None)
    }
    /// Stop: drop the current clones (leases still held keep reading theirs; their tables are
    /// left for the next start's sweep).
    async fn shutdown(&self) {}
    /// What it's doing, per table (for the debug page); none if it doesn't say.
    async fn status(&self) -> Vec<SnapshotStatus> {
        Vec::new()
    }
}

/// A table's snapshots, as the debug page shows them.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SnapshotStatus {
    pub database: String,
    pub table: String,
    pub topic: String,
    /// Running subscriptions on it.
    pub users: usize,
    /// Since the last acquire, or since the last user went.
    pub idle_secs: f64,
    /// A snapshot is being taken.
    pub taking: bool,
    /// Acquires waiting for it.
    pub waiting: usize,
    pub current: Option<CurrentSnapshot>,
    pub taken: u64,
    pub failed: u64,
    pub last_error: Option<String>,
    /// How long the last take (pause, clone, resume) took.
    pub last_take_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct CurrentSnapshot {
    /// `database.name`
    pub clone: String,
    pub watermark: String,
    /// Unix milliseconds.
    pub taken_at: u64,
    /// Leases held on it (subscriptions reading its history).
    pub leases: usize,
}

#[derive(Debug, Clone, PartialEq)]
pub struct SnapshotConfig {
    /// How often a table in use gets a new snapshot.
    pub refresh: Duration,
    /// After this long without a subscription, a table's snapshots stop.
    pub idle: Duration,
    /// On start, drop every clone (`__chlive_snap_*`) left by an earlier run. Only for the one
    /// process that owns them (the server), not for tools running next to it.
    pub sweep: bool,
    /// Where clones are created; `None`: next to their table. Must exist, on the same storage
    /// policy as the tables (ATTACH PARTITION links their parts).
    pub database: Option<String>,
}

impl Default for SnapshotConfig {
    fn default() -> Self {
        Self {
            refresh: Duration::from_secs(10),
            idle: Duration::from_secs(3600),
            sweep: false,
            database: None,
        }
    }
}

/// Keeps a table in use until dropped (see [`Snapshots::in_use`]).
pub struct InUse(Option<(mpsc::UnboundedSender<Cmd>, SnapshotTable)>);

impl Drop for InUse {
    fn drop(&mut self) {
        if let Some((tx, table)) = self.0.take() {
            let _ = tx.send(Cmd::Unuse(table));
        }
    }
}

enum Cmd {
    Acquire(SnapshotTable, oneshot::Sender<Result<Lease, SnapshotError>>),
    Use(SnapshotTable),
    Unuse(SnapshotTable),
    Taken(SnapshotTable, Result<Snapshot, SnapshotError>, Duration),
    DropClone(String, String),
    Status(oneshot::Sender<Vec<SnapshotStatus>>),
    Shutdown(oneshot::Sender<()>),
}

/// A handle to the task that takes, refreshes and drops snapshots. Cheap to clone; the task
/// ends when the last handle goes (its clones are then left for the next start's sweep).
#[derive(Clone)]
pub struct SnapshotManager {
    tx: mpsc::UnboundedSender<Cmd>,
}

impl SnapshotManager {
    /// Starts the task. It first drops clones left behind by an earlier run.
    pub fn start(pauser: Arc<dyn Pauser>, cloner: Arc<dyn Cloner>, config: SnapshotConfig) -> Self {
        let (tx, rx) = mpsc::unbounded_channel();
        let task = Task {
            pauser,
            cloner,
            config,
            tx: tx.downgrade(),
            tables: HashMap::new(),
        };
        tokio::spawn(task.run(rx));
        Self { tx }
    }
}

#[async_trait]
impl Snapshots for SnapshotManager {
    async fn acquire(&self, table: &SnapshotTable) -> Result<Lease, SnapshotError> {
        let (reply, rx) = oneshot::channel();
        self.tx
            .send(Cmd::Acquire(table.clone(), reply))
            .map_err(|_| SnapshotError("the snapshot task stopped".into()))?;
        rx.await
            .map_err(|_| SnapshotError("the snapshot task stopped".into()))?
    }

    fn in_use(&self, table: &SnapshotTable) -> InUse {
        match self.tx.send(Cmd::Use(table.clone())) {
            Ok(()) => InUse(Some((self.tx.clone(), table.clone()))),
            Err(_) => InUse(None),
        }
    }

    async fn shutdown(&self) {
        let (done, rx) = oneshot::channel();
        if self.tx.send(Cmd::Shutdown(done)).is_ok() {
            let _ = rx.await;
        }
    }

    async fn status(&self) -> Vec<SnapshotStatus> {
        let (reply, rx) = oneshot::channel();
        if self.tx.send(Cmd::Status(reply)).is_err() {
            return Vec::new();
        }
        rx.await.unwrap_or_default()
    }
}

struct TableState {
    current: Option<Lease>,
    /// The last acquire, or when the last user went.
    last_used: Instant,
    /// Running subscriptions on the table ([`InUse`] guards).
    users: usize,
    taking: bool,
    waiting: Vec<oneshot::Sender<Result<Lease, SnapshotError>>>,
    taken: u64,
    failed: u64,
    last_error: Option<String>,
    last_take: Option<Duration>,
}

struct Task {
    pauser: Arc<dyn Pauser>,
    cloner: Arc<dyn Cloner>,
    config: SnapshotConfig,
    /// Weak, so the task ends when the handles are gone.
    tx: mpsc::WeakUnboundedSender<Cmd>,
    tables: HashMap<SnapshotTable, TableState>,
}

impl Task {
    async fn run(mut self, mut rx: mpsc::UnboundedReceiver<Cmd>) {
        // before any new clone exists, so only leftovers go
        let leftovers = match self.config.sweep {
            true => self.cloner.list(CLONE_PREFIX).await,
            false => Ok(Vec::new()),
        };
        match leftovers {
            Ok(leftovers) => {
                for (db, name) in leftovers {
                    tracing::info!(database = %db, clone = %name, "dropping a leftover snapshot");
                    if let Err(e) = self.cloner.drop_table(&db, &name).await {
                        tracing::warn!(clone = %name, error = %e, "dropping a leftover snapshot");
                    }
                }
            }
            Err(e) => tracing::warn!(error = %e, "listing leftover snapshots"),
        }
        let mut tick = tokio::time::interval(self.config.refresh);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                cmd = rx.recv() => match cmd {
                    Some(Cmd::Shutdown(done)) => {
                        // the clones no subscription is reading go now
                        for (_, state) in self.tables.drain() {
                            if let Some(mut s) = state.current.and_then(Arc::into_inner) {
                                s.on_drop = None;
                                let _ = self.cloner.drop_table(&s.database, &s.name).await;
                            }
                        }
                        let _ = done.send(());
                        return;
                    }
                    Some(cmd) => self.handle(cmd),
                    None => return,
                },
                _ = tick.tick() => self.refresh(),
            }
        }
    }

    fn handle(&mut self, cmd: Cmd) {
        match cmd {
            Cmd::Use(table) => {
                let state = self.state(&table);
                state.users += 1;
                state.last_used = Instant::now();
            }
            Cmd::Unuse(table) => {
                if let Some(state) = self.tables.get_mut(&table) {
                    state.users = state.users.saturating_sub(1);
                    // idle from now
                    state.last_used = Instant::now();
                }
            }
            Cmd::Acquire(table, reply) => {
                let state = self.state(&table);
                state.last_used = Instant::now();
                match &state.current {
                    Some(lease) => {
                        let _ = reply.send(Ok(lease.clone()));
                    }
                    None => {
                        state.waiting.push(reply);
                        if !state.taking {
                            self.take(table);
                        }
                    }
                }
            }
            Cmd::Taken(table, result, took) => {
                let Some(state) = self.tables.get_mut(&table) else {
                    return; // forgotten meanwhile (idle): the snapshot drops its clone
                };
                state.taking = false;
                state.last_take = Some(took);
                match result {
                    Ok(snapshot) => {
                        state.taken += 1;
                        let lease = Arc::new(snapshot);
                        for w in state.waiting.drain(..) {
                            let _ = w.send(Ok(lease.clone()));
                        }
                        // the previous one is dropped once its last reader lets go
                        state.current = Some(lease);
                    }
                    Err(e) => {
                        tracing::warn!(table = %table.table, error = %e, "snapshot failed");
                        state.failed += 1;
                        state.last_error = Some(e.0.clone());
                        // keep the current one: it's still a correct starting point
                        for w in state.waiting.drain(..) {
                            let _ = w.send(Err(e.clone()));
                        }
                    }
                }
            }
            Cmd::Shutdown(_) => unreachable!("handled in run"),
            Cmd::Status(reply) => {
                let mut out: Vec<SnapshotStatus> = self
                    .tables
                    .iter()
                    .map(|(t, s)| SnapshotStatus {
                        database: t.database.clone(),
                        table: t.table.clone(),
                        topic: t.topic.clone(),
                        users: s.users,
                        idle_secs: s.last_used.elapsed().as_secs_f64(),
                        taking: s.taking,
                        waiting: s.waiting.len(),
                        current: s.current.as_ref().map(|l| CurrentSnapshot {
                            clone: format!("{}.{}", l.database, l.name),
                            watermark: l.watermark.describe(),
                            taken_at: l
                                .taken_at
                                .duration_since(UNIX_EPOCH)
                                .unwrap_or_default()
                                .as_millis() as u64,
                            // the task's own reference isn't a lease
                            leases: Arc::strong_count(l) - 1,
                        }),
                        taken: s.taken,
                        failed: s.failed,
                        last_error: s.last_error.clone(),
                        last_take_ms: s.last_take.map(|d| d.as_millis() as u64),
                    })
                    .collect();
                out.sort_by(|a, b| (&a.database, &a.table).cmp(&(&b.database, &b.table)));
                let _ = reply.send(out);
            }
            Cmd::DropClone(db, clone) => {
                let cloner = self.cloner.clone();
                tokio::spawn(async move {
                    if let Err(e) = cloner.drop_table(&db, &clone).await {
                        tracing::warn!(clone = %clone, error = %e, "dropping a snapshot");
                    }
                });
            }
        }
    }

    fn state(&mut self, table: &SnapshotTable) -> &mut TableState {
        self.tables.entry(table.clone()).or_insert(TableState {
            current: None,
            last_used: Instant::now(),
            users: 0,
            taking: false,
            waiting: Vec::new(),
            taken: 0,
            failed: 0,
            last_error: None,
            last_take: None,
        })
    }

    /// Every `refresh`: a new snapshot of each table in use; tables idle for `idle` (no
    /// subscription running, none acquired) are let go.
    fn refresh(&mut self) {
        let idle = self.config.idle;
        self.tables.retain(|table, state| {
            let keep = state.users > 0 || state.last_used.elapsed() < idle || state.taking;
            if !keep {
                tracing::info!(table = %table.table, "no subscriptions lately: snapshots stop");
            }
            keep
        });
        let due: Vec<SnapshotTable> = self
            .tables
            .iter()
            .filter(|(_, s)| !s.taking && s.current.is_some())
            .map(|(t, _)| t.clone())
            .collect();
        for table in due {
            self.take(table);
        }
    }

    fn take(&mut self, table: SnapshotTable) {
        let Some(tx) = self.tx.upgrade() else { return };
        if let Some(state) = self.tables.get_mut(&table) {
            state.taking = true;
        }
        let (pauser, cloner) = (self.pauser.clone(), self.cloner.clone());
        let into = self.config.database.clone();
        tokio::spawn(async move {
            let started = Instant::now();
            let result = take(pauser.as_ref(), cloner.as_ref(), &table, into.as_deref())
                .await
                .map(|mut s| {
                    s.on_drop = Some(tx.clone());
                    s
                });
            let _ = tx.send(Cmd::Taken(table, result, started.elapsed()));
        });
    }
}

/// Pause the table's ingestion, clone it (into `database`, or next to it), resume. Resuming
/// always happens; a failed clone is dropped.
pub async fn take(
    pauser: &dyn Pauser,
    cloner: &dyn Cloner,
    table: &SnapshotTable,
    database: Option<&str>,
) -> Result<Snapshot, SnapshotError> {
    let clone = format!("{CLONE_PREFIX}{}_{}", table.table, unique_suffix());
    let clone_db = database.unwrap_or(&table.database);
    let started = std::time::Instant::now();
    let paused = pauser.pause(table).await?;
    let taken_at = SystemTime::now();
    // the pause server resumes on its own after the TTL unless renewed
    let renew = {
        let (token, every) = (&paused.token, (paused.ttl / 3).max(Duration::from_secs(1)));
        async move {
            loop {
                tokio::time::sleep(every).await;
                if let Err(e) = pauser.renew(token).await {
                    tracing::warn!(error = %e, "renewing a pause");
                }
            }
        }
    };
    let cloned = tokio::select! {
        r = cloner.clone_table(&table.database, &table.table, clone_db, &clone) => r,
        _ = renew => unreachable!("renewing never ends"),
    };
    let resumed = pauser.resume(&paused.token).await;
    if let Err(e) = &resumed {
        // the pause server resumes the connector by itself when the token expires
        tracing::error!(table = %table.table, error = %e, "resuming ingestion failed");
    }
    match cloned {
        Ok(()) => {
            tracing::debug!(
                table = %table.table, clone = %clone, watermark = %paused.watermark.describe(),
                ms = started.elapsed().as_millis() as u64, "snapshot taken"
            );
            Ok(Snapshot {
                database: clone_db.to_string(),
                name: clone,
                watermark: paused.watermark,
                taken_at,
                on_drop: None,
            })
        }
        Err(e) => {
            let _ = cloner.drop_table(clone_db, &clone).await;
            Err(e)
        }
    }
}

/// Unique per process: milliseconds since the epoch, then a counter.
fn unique_suffix() -> String {
    static N: AtomicU64 = AtomicU64::new(0);
    let ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis();
    format!("{ms}_{}", N.fetch_add(1, Ordering::Relaxed))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    /// Records calls; offsets grow by 10 per pause; clones can be made slow or to fail.
    #[derive(Default)]
    struct Fake {
        log: Mutex<Vec<String>>,
        pauses: AtomicU64,
        fail_clone: std::sync::atomic::AtomicBool,
        clone_delay: Mutex<Duration>,
        leftovers: Vec<(String, String)>,
    }

    impl Fake {
        fn log(&self) -> Vec<String> {
            self.log.lock().unwrap().clone()
        }
        fn push(&self, s: String) {
            self.log.lock().unwrap().push(s);
        }
        /// The log with clone names reduced to `clone`, for comparing sequences.
        fn steps(&self) -> Vec<String> {
            self.log()
                .into_iter()
                .map(|l| match l.split_once(' ') {
                    Some((op, _)) if op == "clone" || op == "drop" => op.to_string(),
                    _ => l,
                })
                .collect()
        }
    }

    #[async_trait]
    impl Pauser for Fake {
        async fn pause(&self, table: &SnapshotTable) -> Result<Paused, SnapshotError> {
            let n = self.pauses.fetch_add(1, Ordering::SeqCst) as i64 + 1;
            self.push(format!("pause {}", table.table));
            Ok(Paused {
                token: format!("t{n}"),
                ttl: Duration::from_secs(60),
                watermark: Watermark([(0, n * 10 - 1)].into()),
            })
        }
        async fn resume(&self, token: &str) -> Result<(), SnapshotError> {
            self.push(format!("resume {token}"));
            Ok(())
        }
        async fn renew(&self, token: &str) -> Result<(), SnapshotError> {
            self.push(format!("renew {token}"));
            Ok(())
        }
    }

    #[async_trait]
    impl Cloner for Fake {
        async fn clone_table(
            &self,
            db: &str,
            source: &str,
            into: &str,
            clone: &str,
        ) -> Result<(), SnapshotError> {
            let delay = *self.clone_delay.lock().unwrap();
            tokio::time::sleep(delay).await;
            self.push(format!("clone {db}.{source} {into}.{clone}"));
            if self.fail_clone.load(Ordering::SeqCst) {
                return Err(SnapshotError("no space".into()));
            }
            Ok(())
        }
        async fn drop_table(&self, db: &str, table: &str) -> Result<(), SnapshotError> {
            self.push(format!("drop {db}.{table}"));
            Ok(())
        }
        async fn list(&self, _: &str) -> Result<Vec<(String, String)>, SnapshotError> {
            Ok(self.leftovers.clone())
        }
    }

    fn events() -> SnapshotTable {
        SnapshotTable::new("default", "events", "events")
    }

    fn manager(fake: &Arc<Fake>) -> SnapshotManager {
        SnapshotManager::start(
            fake.clone(),
            fake.clone(),
            SnapshotConfig {
                refresh: Duration::from_secs(10),
                idle: Duration::from_secs(3600),
                sweep: true,
                database: None,
            },
        )
    }

    /// Let spawned tasks run (time is paused: this advances nothing).
    async fn settle() {
        for _ in 0..20 {
            tokio::task::yield_now().await;
        }
    }

    #[tokio::test(start_paused = true)]
    async fn clones_go_next_to_their_table_or_into_the_snapshot_database() {
        let fake = Fake::default();
        let s = take(&fake, &fake, &events(), None).await.unwrap();
        assert_eq!(s.database, "default");
        assert_eq!(s.sql_table(), format!("default.{}", s.name));
        assert!(fake
            .log()
            .contains(&format!("clone default.events default.{}", s.name)));

        let fake = Fake::default();
        let s = take(&fake, &fake, &events(), Some("snaps")).await.unwrap();
        assert_eq!(s.database, "snaps", "history reads the clone there");
        assert_eq!(s.sql_table(), format!("snaps.{}", s.name));
        assert!(fake
            .log()
            .contains(&format!("clone default.events snaps.{}", s.name)));

        // a failed clone is dropped where it was made
        let fake = Fake::default();
        fake.fail_clone.store(true, Ordering::SeqCst);
        assert!(take(&fake, &fake, &events(), Some("snaps")).await.is_err());
        assert!(fake
            .log()
            .iter()
            .any(|l| l.starts_with("drop snaps.__chlive_snap_events_")));

        // the manager uses its config's database, and drops replaced clones there
        let fake = Arc::new(Fake::default());
        let m = SnapshotManager::start(
            fake.clone(),
            fake.clone(),
            SnapshotConfig {
                database: Some("snaps".into()),
                ..SnapshotConfig::default()
            },
        );
        let a = m.acquire(&events()).await.unwrap();
        assert_eq!(a.database, "snaps");
        let name = a.name.clone();
        drop(a);
        m.shutdown().await;
        settle().await;
        assert!(
            fake.log().contains(&format!("drop snaps.{name}")),
            "{:?}",
            fake.log()
        );
    }

    #[test]
    fn snapshot_config_defaults() {
        let d = SnapshotConfig::default();
        assert_eq!(d.refresh, Duration::from_secs(10));
        assert_eq!(d.idle, Duration::from_secs(3600));
        assert!(!d.sweep);
        assert_eq!(d.database, None);
    }

    #[tokio::test(start_paused = true)]
    async fn sweeps_leftovers_then_shares_one_snapshot() {
        let fake = Arc::new(Fake {
            leftovers: vec![("default".into(), "__chlive_snap_events_1_0".into())],
            ..Default::default()
        });
        let m = manager(&fake);
        let t = events();
        let (a, b) = tokio::join!(m.acquire(&t), m.acquire(&t));
        let (a, b) = (a.unwrap(), b.unwrap());
        assert!(
            Arc::ptr_eq(&a, &b),
            "concurrent acquires share one snapshot"
        );
        assert_eq!(a.database, "default");
        assert!(a.name.starts_with("__chlive_snap_events_"));
        assert_eq!(a.watermark.describe(), "{0: 9}");
        assert_eq!(
            fake.log()[0],
            "drop default.__chlive_snap_events_1_0",
            "leftovers go first"
        );
        assert_eq!(fake.steps()[1..], ["pause events", "clone", "resume t1"]);
    }

    #[tokio::test(start_paused = true)]
    async fn refreshes_while_used_and_drops_replaced_clones_after_their_last_lease() {
        let fake = Arc::new(Fake::default());
        let m = manager(&fake);
        let first = m.acquire(&events()).await.unwrap();
        tokio::time::sleep(Duration::from_secs(10)).await;
        settle().await;
        let second = m.acquire(&events()).await.unwrap();
        assert_eq!(second.watermark.describe(), "{0: 19}", "refreshed");
        // the first is replaced, but still leased: not dropped yet
        assert!(!fake.log().contains(&format!("drop default.{}", first.name)));
        let name = format!("default.{}", first.name);
        drop(first);
        settle().await;
        assert!(fake.log().contains(&format!("drop {name}")));
    }

    #[tokio::test(start_paused = true)]
    async fn stops_after_idle_and_starts_cold_again() {
        let fake = Arc::new(Fake::default());
        let m = manager(&fake);
        let lease = m.acquire(&events()).await.unwrap();
        let name = format!("default.{}", lease.name);
        drop(lease);
        // refreshed every 10 s for an hour, then let go
        tokio::time::sleep(Duration::from_secs(3600 + 30)).await;
        settle().await;
        let pauses = fake.pauses.load(Ordering::SeqCst);
        assert!((360..=362).contains(&pauses), "{pauses} pauses in an hour");
        tokio::time::sleep(Duration::from_secs(600)).await;
        settle().await;
        assert_eq!(
            fake.pauses.load(Ordering::SeqCst),
            pauses,
            "no more after idle"
        );
        assert!(fake.log().contains(&format!("drop {name}")));
        let drops = fake.log().iter().filter(|l| l.starts_with("drop")).count();
        assert_eq!(drops as u64, pauses, "every clone is dropped");
        // a new subscription takes a fresh one
        m.acquire(&events()).await.unwrap();
        assert_eq!(fake.pauses.load(Ordering::SeqCst), pauses + 1);
    }

    #[tokio::test(start_paused = true)]
    async fn a_running_subscription_keeps_its_table_in_use_past_idle() {
        let fake = Arc::new(Fake::default());
        let m = manager(&fake); // refresh 10 s, idle 1 h
        let guard = m.in_use(&events());
        drop(m.acquire(&events()).await.unwrap());
        // two hours with no acquire, but in use: still refreshed
        tokio::time::sleep(Duration::from_secs(7200)).await;
        settle().await;
        let pauses = fake.pauses.load(Ordering::SeqCst);
        assert!(
            (720..=722).contains(&pauses),
            "{pauses} pauses in two hours"
        );
        // the subscription ends: idle counts from now
        drop(guard);
        settle().await;
        tokio::time::sleep(Duration::from_secs(1800)).await;
        settle().await;
        let half = fake.pauses.load(Ordering::SeqCst);
        assert!(half > pauses + 170, "still refreshed 30 min after: {half}");
        tokio::time::sleep(Duration::from_secs(1800 + 60)).await;
        settle().await;
        let after = fake.pauses.load(Ordering::SeqCst);
        tokio::time::sleep(Duration::from_secs(600)).await;
        settle().await;
        assert_eq!(
            fake.pauses.load(Ordering::SeqCst),
            after,
            "idle an hour after it ended"
        );
        // guards from a stopped manager (or the default) do nothing
        let none = FakeSnapshotsOnly.in_use(&events());
        drop(none);
    }

    /// Only `acquire`: `in_use` is the trait's no-op default.
    struct FakeSnapshotsOnly;

    #[async_trait]
    impl Snapshots for FakeSnapshotsOnly {
        async fn acquire(&self, _: &SnapshotTable) -> Result<Lease, SnapshotError> {
            Err(SnapshotError("none".into()))
        }
    }

    #[tokio::test(start_paused = true)]
    async fn a_failed_clone_resumes_and_a_failed_refresh_keeps_the_snapshot() {
        let fake = Arc::new(Fake::default());
        fake.fail_clone.store(true, Ordering::SeqCst);
        let m = manager(&fake);
        assert_eq!(m.acquire(&events()).await.unwrap_err().0, "no space");
        assert_eq!(fake.steps(), ["pause events", "clone", "resume t1", "drop"]);

        fake.fail_clone.store(false, Ordering::SeqCst);
        let good = m.acquire(&events()).await.unwrap();
        fake.fail_clone.store(true, Ordering::SeqCst);
        tokio::time::sleep(Duration::from_secs(10)).await;
        settle().await;
        let still = m.acquire(&events()).await.unwrap();
        assert!(Arc::ptr_eq(&good, &still), "the last good snapshot stays");
    }

    #[tokio::test(start_paused = true)]
    async fn status_shows_each_tables_snapshot_users_and_failures() {
        let fake = Arc::new(Fake::default());
        let m = manager(&fake);
        assert_eq!(m.status().await, []);
        let guard = m.in_use(&events());
        let lease = m.acquire(&events()).await.unwrap();
        let status = m.status().await;
        assert_eq!(status.len(), 1);
        let s = &status[0];
        assert_eq!((s.table.as_str(), s.topic.as_str()), ("events", "events"));
        assert_eq!((s.users, s.taking, s.waiting), (1, false, 0));
        assert_eq!((s.taken, s.failed, s.last_error.clone()), (1, 0, None));
        assert!(s.last_take_ms.is_some());
        let current = s.current.clone().unwrap();
        assert_eq!(current.clone, format!("default.{}", lease.name));
        assert_eq!(current.watermark, "{0: 9}");
        assert_eq!(current.leases, 1);
        drop(lease);
        assert_eq!(m.status().await[0].current.as_ref().unwrap().leases, 0);

        // a failed refresh: counted, and the last good snapshot stays current
        fake.fail_clone.store(true, Ordering::SeqCst);
        tokio::time::sleep(Duration::from_secs(10)).await;
        settle().await;
        let s = &m.status().await[0];
        assert_eq!((s.taken, s.failed), (1, 1));
        assert_eq!(s.last_error.as_deref(), Some("no space"));
        assert_eq!(s.current.as_ref().unwrap().watermark, "{0: 9}");
        drop(guard);
        // a Snapshots that doesn't say
        assert_eq!(FakeSnapshotsOnly.status().await, []);
    }

    #[tokio::test(start_paused = true)]
    async fn renews_the_pause_while_a_clone_is_slow() {
        let fake = Arc::new(Fake::default());
        *fake.clone_delay.lock().unwrap() = Duration::from_secs(45);
        let m = manager(&fake);
        m.acquire(&events()).await.unwrap();
        // TTL 60 s: renewed every 20 s, at 20 and 40
        assert_eq!(
            fake.steps(),
            ["pause events", "renew t1", "renew t1", "clone", "resume t1"]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn shutdown_drops_the_clones_nobody_reads() {
        let fake = Arc::new(Fake::default());
        let m = manager(&fake);
        let events = m.acquire(&events()).await.unwrap();
        let orders = SnapshotTable::new("default", "orders", "orders");
        drop(m.acquire(&orders).await.unwrap());
        m.shutdown().await;
        let drops: Vec<String> = fake
            .log()
            .into_iter()
            .filter(|l| l.starts_with("drop"))
            .collect();
        assert_eq!(drops.len(), 1, "{drops:?}");
        assert!(drops[0].contains("orders"));
        // still read by a subscription: left for the next start's sweep
        drop(events);
    }
}
