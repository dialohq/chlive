//! Shared results on disk (SQLite): for each entry its rows as of some cursor (the *base*), every
//! `diff` message after that (the *log*), and the engine's placement state at a cursor (the
//! *checkpoint*). A running entry keeps all three current, so it can drop its rows from memory
//! (warm), stop following Kafka (cold), or come back after a restart, without ClickHouse.
//!
//! Writes go through one thread that owns the write connection, batched per transaction; reads
//! use their own connection (WAL lets both work at once). Invariants: the base is at or before
//! the checkpoint, and the log holds the messages after the base, in order; a crash can leave log
//! messages past the checkpoint, which a restore drops ([`Store::truncate`]).

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{mpsc, Arc, Mutex};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use chlive_engine::diff::{Diff, ResultList, Row};
use chlive_engine::watermark::Watermark;
use rusqlite::{params, Connection, OptionalExtension};
use serde_json::{json, Value as Json};
use tokio::sync::oneshot;
use tracing::{debug, info, warn};

use crate::protocol::Change;

/// The tables' layout; a store with another one is emptied.
const LAYOUT: i64 = 1;
/// Rows per base chunk.
const CHUNK_ROWS: usize = 1000;
/// Ops per write transaction at most, and how long the writer gathers them.
const BATCH_OPS: usize = 64;
const BATCH_WAIT: Duration = Duration::from_millis(100);

pub fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

pub fn watermark_json(w: &Watermark) -> String {
    let m: serde_json::Map<String, Json> =
        w.0.iter().map(|(p, o)| (p.to_string(), json!(o))).collect();
    Json::Object(m).to_string()
}

pub fn watermark_from(text: &str) -> Watermark {
    let v: Json = serde_json::from_str(text).unwrap_or(Json::Null);
    Watermark(
        v.as_object()
            .map(|m| {
                m.iter()
                    .filter_map(|(p, o)| Some((p.parse().ok()?, o.as_i64()?)))
                    .collect()
            })
            .unwrap_or_default(),
    )
}

/// What an entry is, besides its rows, log and state.
#[derive(Debug, Clone, PartialEq)]
pub struct Meta {
    pub key: String,
    pub fp: String,
    pub sql: String,
    pub params: String,
    pub lineage: String,
    /// Earlier lineages of this result and the cursor each ended at (restores fork it).
    pub forks: Vec<(String, Watermark)>,
    /// When the snapshot it grew from was taken (unix ms).
    pub born_at: i64,
}

impl Meta {
    fn forks_json(&self) -> String {
        Json::Array(
            self.forks
                .iter()
                .map(|(l, at)| json!([l, watermark_json(at)]))
                .collect(),
        )
        .to_string()
    }

    fn forks_from(text: &str) -> Vec<(String, Watermark)> {
        let v: Json = serde_json::from_str(text).unwrap_or(Json::Null);
        v.as_array()
            .map(|a| {
                a.iter()
                    .filter_map(|f| {
                        Some((
                            f.get(0)?.as_str()?.to_string(),
                            watermark_from(f.get(1)?.as_str()?),
                        ))
                    })
                    .collect()
            })
            .unwrap_or_default()
    }
}

/// One `diff` message of the log, as sent.
#[derive(Debug, Clone, PartialEq)]
pub struct LogItem {
    pub seq: i64,
    pub at: Watermark,
    pub body: Arc<str>,
}

/// An entry as stored.
#[derive(Debug, Clone)]
pub struct Stored {
    pub meta: Meta,
    pub base_at: Watermark,
    pub base: ResultList,
    pub log: Vec<LogItem>,
    /// The engine's checkpoint (JSON) and its cursor.
    pub state: Option<Json>,
    pub state_at: Watermark,
    pub stage: String,
}

/// An entry in the index, for `/debug/subscriptions`.
#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct IndexEntry {
    pub key: String,
    pub sql: String,
    pub lineage: String,
    pub stage: String,
    pub state_at: String,
    pub bytes: i64,
    pub log_rows: i64,
    pub used_at: i64,
    pub born_at: i64,
}

enum Op {
    /// A new entry (or a restored one under a new lineage): its meta, base and checkpoint.
    Create {
        meta: Meta,
        at: Watermark,
        rows: ResultList,
        state: Json,
    },
    Append {
        key: String,
        at: Watermark,
        body: Arc<str>,
    },
    Checkpoint {
        key: String,
        at: Watermark,
        state: Json,
    },
    /// Fold the log messages the checkpoint covers into the base.
    Compact {
        key: String,
    },
    /// Drop log messages past the checkpoint (a crash's).
    Truncate {
        key: String,
    },
    Stage {
        key: String,
        stage: &'static str,
    },
    Delete {
        key: String,
        reason: String,
    },
    Flush(oneshot::Sender<()>),
}

/// What the writer has done, shared with it (for the debug page).
#[derive(Default)]
struct Counters {
    /// Ops sent and not yet taken by the writer.
    queued: AtomicU64,
    batches: AtomicU64,
    ops: AtomicU64,
    last_batch_ops: AtomicU64,
    last_batch_ms: AtomicU64,
    last_batch_at: AtomicU64,
    /// Batches whose transaction failed.
    errors: AtomicU64,
    evictions: AtomicU64,
    evicted_bytes: AtomicU64,
}

/// The store's files and its writer, as the debug page shows them.
#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct StoreStats {
    pub path: String,
    pub file_bytes: u64,
    pub wal_bytes: u64,
    pub queued: u64,
    pub batches: u64,
    pub ops: u64,
    pub last_batch_ops: u64,
    pub last_batch_ms: u64,
    /// Unix milliseconds; 0: none yet.
    pub last_batch_at: u64,
    pub errors: u64,
    pub evictions: u64,
    pub evicted_bytes: u64,
}

/// The store: a handle to the writer thread, and a connection for reads.
pub struct Store {
    path: PathBuf,
    tx: Mutex<mpsc::Sender<Op>>,
    read: Mutex<Connection>,
    counters: Arc<Counters>,
}

impl Store {
    /// Open (or create) the store in `dir`. A store left by an earlier run is kept; every entry
    /// in it is cold now (nothing runs yet).
    pub fn open(dir: &Path, budget: u64) -> rusqlite::Result<Arc<Self>> {
        std::fs::create_dir_all(dir)
            .map_err(|e| rusqlite::Error::InvalidPath(format!("{}: {e}", dir.display()).into()))?;
        let path = dir.join("cache.sqlite3");
        let write = Connection::open(&path)?;
        write.pragma_update(None, "journal_mode", "WAL")?;
        write.pragma_update(None, "synchronous", "NORMAL")?;
        let layout: i64 = write.query_row("PRAGMA user_version", [], |r| r.get(0))?;
        if layout != LAYOUT {
            if layout != 0 {
                info!(path = %path.display(), from = layout, to = LAYOUT, "cache store layout changed: emptied");
            }
            write.execute_batch(
                "DROP TABLE IF EXISTS entries; DROP TABLE IF EXISTS base; DROP TABLE IF EXISTS log;",
            )?;
            write.pragma_update(None, "user_version", LAYOUT)?;
        }
        write.execute_batch(
            "CREATE TABLE IF NOT EXISTS entries (
               key TEXT PRIMARY KEY, fp TEXT NOT NULL, sql TEXT NOT NULL, params TEXT NOT NULL,
               lineage TEXT NOT NULL, forks TEXT NOT NULL, born_at INTEGER NOT NULL,
               base_at TEXT NOT NULL, state TEXT, state_at TEXT NOT NULL, stage TEXT NOT NULL,
               base_bytes INTEGER NOT NULL DEFAULT 0, log_bytes INTEGER NOT NULL DEFAULT 0,
               state_bytes INTEGER NOT NULL DEFAULT 0, used_at INTEGER NOT NULL);
             CREATE TABLE IF NOT EXISTS base (
               key TEXT NOT NULL, seq INTEGER NOT NULL, data TEXT NOT NULL, PRIMARY KEY (key, seq));
             CREATE TABLE IF NOT EXISTS log (
               key TEXT NOT NULL, seq INTEGER NOT NULL, at TEXT NOT NULL, body TEXT NOT NULL,
               PRIMARY KEY (key, seq));
             CREATE INDEX IF NOT EXISTS entries_used ON entries (stage, used_at);",
        )?;
        // nothing runs yet: everything is cold until subscribed
        write.execute("UPDATE entries SET stage = 'cold'", [])?;
        let read = Connection::open(&path)?;
        let (tx, rx) = mpsc::channel();
        let writer_path = path.clone();
        let counters = Arc::new(Counters::default());
        let c = counters.clone();
        std::thread::Builder::new()
            .name("chlive-store".into())
            .spawn(move || writer(write, rx, budget, writer_path, c))
            .expect("a thread");
        info!(path = %path.display(), "cache store open");
        Ok(Arc::new(Self {
            path,
            tx: Mutex::new(tx),
            read: Mutex::new(read),
            counters,
        }))
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    fn send(&self, op: Op) {
        self.counters.queued.fetch_add(1, Ordering::Relaxed);
        if self.tx.lock().unwrap().send(op).is_err() {
            self.counters.queued.fetch_sub(1, Ordering::Relaxed);
        }
    }

    /// The files' sizes and what the writer has done.
    pub fn stats(&self) -> StoreStats {
        let size = |p: &Path| std::fs::metadata(p).map(|m| m.len()).unwrap_or(0);
        let mut wal = self.path.clone().into_os_string();
        wal.push("-wal");
        let c = &self.counters;
        let get = |a: &AtomicU64| a.load(Ordering::Relaxed);
        StoreStats {
            path: self.path.display().to_string(),
            file_bytes: size(&self.path),
            wal_bytes: size(Path::new(&wal)),
            queued: get(&c.queued),
            batches: get(&c.batches),
            ops: get(&c.ops),
            last_batch_ops: get(&c.last_batch_ops),
            last_batch_ms: get(&c.last_batch_ms),
            last_batch_at: get(&c.last_batch_at),
            errors: get(&c.errors),
            evictions: get(&c.evictions),
            evicted_bytes: get(&c.evicted_bytes),
        }
    }

    pub fn create(&self, meta: Meta, at: Watermark, rows: ResultList, state: Json) {
        self.send(Op::Create {
            meta,
            at,
            rows,
            state,
        });
    }

    pub fn append(&self, key: &str, at: Watermark, body: Arc<str>) {
        self.send(Op::Append {
            key: key.into(),
            at,
            body,
        });
    }

    pub fn checkpoint(&self, key: &str, at: Watermark, state: Json) {
        self.send(Op::Checkpoint {
            key: key.into(),
            at,
            state,
        });
    }

    pub fn compact(&self, key: &str) {
        self.send(Op::Compact { key: key.into() });
    }

    pub fn truncate(&self, key: &str) {
        self.send(Op::Truncate { key: key.into() });
    }

    pub fn stage(&self, key: &str, stage: &'static str) {
        self.send(Op::Stage {
            key: key.into(),
            stage,
        });
    }

    pub fn delete(&self, key: &str, reason: &str) {
        self.send(Op::Delete {
            key: key.into(),
            reason: reason.into(),
        });
    }

    /// Until every op sent so far is written.
    pub async fn flush(&self) {
        let (done, wait) = oneshot::channel();
        self.send(Op::Flush(done));
        let _ = wait.await;
    }

    /// The entry, if stored (flush first for a running one's latest).
    pub fn load(&self, key: &str) -> rusqlite::Result<Option<Stored>> {
        let conn = self.read.lock().unwrap();
        let row = conn
            .query_row(
                "SELECT fp, sql, params, lineage, forks, born_at, base_at, state, state_at, stage
                 FROM entries WHERE key = ?1",
                params![key],
                |r| {
                    Ok((
                        r.get::<_, String>(0)?,
                        r.get::<_, String>(1)?,
                        r.get::<_, String>(2)?,
                        r.get::<_, String>(3)?,
                        r.get::<_, String>(4)?,
                        r.get::<_, i64>(5)?,
                        r.get::<_, String>(6)?,
                        r.get::<_, Option<String>>(7)?,
                        r.get::<_, String>(8)?,
                        r.get::<_, String>(9)?,
                    ))
                },
            )
            .optional()?;
        let Some((fp, sql, params, lineage, forks, born_at, base_at, state, state_at, stage)) = row
        else {
            return Ok(None);
        };
        let mut base = ResultList::default();
        let mut stmt = conn.prepare("SELECT data FROM base WHERE key = ?1 ORDER BY seq")?;
        for chunk in stmt.query_map(params![key], |r| r.get::<_, String>(0))? {
            let chunk: Json = serde_json::from_str(&chunk?).unwrap_or(Json::Null);
            for item in chunk.as_array().into_iter().flatten() {
                let row: Row = item["row"].as_object().cloned().unwrap_or_default();
                base.rows.push(row);
                base.ids.push(item["id"].as_str().map(String::from));
            }
        }
        let mut stmt = conn.prepare("SELECT seq, at, body FROM log WHERE key = ?1 ORDER BY seq")?;
        let log = stmt
            .query_map(params![key], |r| {
                Ok(LogItem {
                    seq: r.get(0)?,
                    at: watermark_from(&r.get::<_, String>(1)?),
                    body: Arc::from(r.get::<_, String>(2)?),
                })
            })?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        Ok(Some(Stored {
            meta: Meta {
                key: key.into(),
                fp,
                sql,
                params,
                lineage,
                forks: Meta::forks_from(&forks),
                born_at,
            },
            base_at: watermark_from(&base_at),
            base,
            log,
            state: state.and_then(|s| serde_json::from_str(&s).ok()),
            state_at: watermark_from(&state_at),
            stage,
        }))
    }

    /// Every stored entry, for the debug listing.
    pub fn index(&self) -> rusqlite::Result<Vec<IndexEntry>> {
        let conn = self.read.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT e.key, e.sql, e.lineage, e.stage, e.state_at,
                    e.base_bytes + e.log_bytes + e.state_bytes, e.used_at, e.born_at,
                    (SELECT count(*) FROM log l WHERE l.key = e.key)
             FROM entries e ORDER BY e.used_at DESC",
        )?;
        let out = stmt
            .query_map([], |r| {
                Ok(IndexEntry {
                    key: r.get(0)?,
                    sql: r.get(1)?,
                    lineage: r.get(2)?,
                    stage: r.get(3)?,
                    state_at: watermark_from(&r.get::<_, String>(4)?).describe(),
                    bytes: r.get(5)?,
                    used_at: r.get(6)?,
                    born_at: r.get(7)?,
                    log_rows: r.get(8)?,
                })
            })?
            .collect();
        out
    }
}

/// A stored entry's rows: the base, then the log messages (all, or only those the checkpoint
/// covers).
pub fn replay(
    stored: &Stored,
    upto_checkpoint: bool,
) -> Result<(ResultList, Vec<LogItem>), String> {
    let mut rows = stored.base.clone();
    let mut kept = Vec::new();
    for item in &stored.log {
        if upto_checkpoint && !stored.state_at.covers(&item.at) {
            break;
        }
        apply_body(&mut rows, &item.body)?;
        kept.push(item.clone());
    }
    Ok((rows, kept))
}

/// Apply a logged `diff` message (as sent) to rows.
pub fn apply_body(rows: &mut ResultList, body: &str) -> Result<(), String> {
    let msg: Json = serde_json::from_str(body).map_err(|e| format!("a logged message: {e}"))?;
    let changes: Vec<Change> = serde_json::from_value(msg["changes"].clone())
        .map_err(|e| format!("a logged message's changes: {e}"))?;
    for c in changes {
        let d = match c {
            Change::Insert { row, index, id } => Diff::Insert { row, index, id },
            Change::Delete { index } => Diff::Delete { index },
            Change::Update { id, set, index } => Diff::Update { id, set, index },
        };
        rows.apply(&d)?;
    }
    Ok(())
}

fn write_base(tx: &rusqlite::Transaction, key: &str, rows: &ResultList) -> rusqlite::Result<i64> {
    tx.execute("DELETE FROM base WHERE key = ?1", params![key])?;
    let mut bytes = 0i64;
    for (seq, chunk) in rows
        .rows
        .chunks(CHUNK_ROWS)
        .zip(rows.ids.chunks(CHUNK_ROWS))
        .enumerate()
    {
        let items: Vec<Json> = chunk
            .0
            .iter()
            .zip(chunk.1)
            .map(|(row, id)| json!({ "row": row, "id": id }))
            .collect();
        let data = Json::Array(items).to_string();
        bytes += data.len() as i64;
        tx.execute(
            "INSERT INTO base (key, seq, data) VALUES (?1, ?2, ?3)",
            params![key, seq as i64, data],
        )?;
    }
    Ok(bytes)
}

fn writer(
    mut conn: Connection,
    rx: mpsc::Receiver<Op>,
    budget: u64,
    path: PathBuf,
    counters: Arc<Counters>,
) {
    while let Ok(first) = rx.recv() {
        counters.queued.fetch_sub(1, Ordering::Relaxed);
        let mut ops = vec![first];
        let started = Instant::now();
        while ops.len() < BATCH_OPS {
            match rx.recv_timeout(BATCH_WAIT.saturating_sub(started.elapsed())) {
                Ok(op) => {
                    counters.queued.fetch_sub(1, Ordering::Relaxed);
                    let flush = matches!(op, Op::Flush(_));
                    ops.push(op);
                    if flush {
                        break;
                    }
                }
                Err(_) => break,
            }
        }
        let n = ops.iter().filter(|op| !matches!(op, Op::Flush(_))).count() as u64;
        let mut flushes = Vec::new();
        let result = (|| -> rusqlite::Result<()> {
            let tx = conn.transaction()?;
            for op in ops {
                match op {
                    Op::Flush(done) => flushes.push(done),
                    op => apply(&tx, op)?,
                }
            }
            tx.commit()
        })();
        if let Err(e) = result {
            warn!(path = %path.display(), error = %e, "writing the cache store failed");
            counters.errors.fetch_add(1, Ordering::Relaxed);
        }
        match evict(&conn, budget) {
            Ok((entries, bytes)) => {
                counters.evictions.fetch_add(entries, Ordering::Relaxed);
                counters.evicted_bytes.fetch_add(bytes, Ordering::Relaxed);
            }
            Err(e) => warn!(error = %e, "evicting from the cache store failed"),
        }
        let ms = started.elapsed().as_millis() as u64;
        if n > 0 {
            counters.batches.fetch_add(1, Ordering::Relaxed);
            counters.ops.fetch_add(n, Ordering::Relaxed);
            counters.last_batch_ops.store(n, Ordering::Relaxed);
            counters.last_batch_ms.store(ms, Ordering::Relaxed);
            counters
                .last_batch_at
                .store(now_ms() as u64, Ordering::Relaxed);
            // flushes alone (the debug page polls) aren't counted or logged
            debug!(target: "chlive_server::store", ops = n, ms, "batch written");
        }
        for done in flushes {
            let _ = done.send(());
        }
    }
}

fn apply(tx: &rusqlite::Transaction, op: Op) -> rusqlite::Result<()> {
    match op {
        Op::Create {
            meta,
            at,
            rows,
            state,
        } => {
            let at_text = watermark_json(&at);
            let state_text = state.to_string();
            tx.execute("DELETE FROM log WHERE key = ?1", params![meta.key])?;
            let base_bytes = write_base(tx, &meta.key, &rows)?;
            tx.execute(
                "INSERT INTO entries (key, fp, sql, params, lineage, forks, born_at, base_at, state,
                   state_at, stage, base_bytes, log_bytes, state_bytes, used_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?8, 'live', ?10, 0, ?11, ?12)
                 ON CONFLICT (key) DO UPDATE SET fp = excluded.fp, sql = excluded.sql,
                   params = excluded.params, lineage = excluded.lineage, forks = excluded.forks,
                   born_at = excluded.born_at, base_at = excluded.base_at, state = excluded.state,
                   state_at = excluded.state_at, stage = 'live', base_bytes = excluded.base_bytes,
                   log_bytes = 0, state_bytes = excluded.state_bytes, used_at = excluded.used_at",
                params![
                    meta.key,
                    meta.fp,
                    meta.sql,
                    meta.params,
                    meta.lineage,
                    meta.forks_json(),
                    meta.born_at,
                    at_text,
                    state_text,
                    base_bytes,
                    state_text.len() as i64,
                    now_ms()
                ],
            )?;
            debug!(target: "chlive_server::store", key = %meta.fp, lineage = %meta.lineage, rows = rows.rows.len(), at = %at.describe(), "entry written (base and checkpoint)");
        }
        Op::Append { key, at, body } => {
            let seq: i64 = tx.query_row(
                "SELECT COALESCE(MAX(seq), -1) + 1 FROM log WHERE key = ?1",
                params![key],
                |r| r.get(0),
            )?;
            tx.execute(
                "INSERT INTO log (key, seq, at, body) VALUES (?1, ?2, ?3, ?4)",
                params![key, seq, watermark_json(&at), &*body],
            )?;
            tx.execute(
                "UPDATE entries SET log_bytes = log_bytes + ?2 WHERE key = ?1",
                params![key, body.len() as i64],
            )?;
        }
        Op::Checkpoint { key, at, state } => {
            let text = state.to_string();
            tx.execute(
                "UPDATE entries SET state = ?2, state_at = ?3, state_bytes = ?4, used_at = ?5
                 WHERE key = ?1",
                params![key, text, watermark_json(&at), text.len() as i64, now_ms()],
            )?;
        }
        Op::Compact { key } => compact(tx, &key)?,
        Op::Truncate { key } => {
            let state_at: Option<String> = tx
                .query_row(
                    "SELECT state_at FROM entries WHERE key = ?1",
                    params![key],
                    |r| r.get(0),
                )
                .optional()?;
            let Some(state_at) = state_at.map(|s| watermark_from(&s)) else {
                return Ok(());
            };
            let mut stmt = tx.prepare("SELECT seq, at, length(body) FROM log WHERE key = ?1")?;
            let past: Vec<(i64, i64)> = stmt
                .query_map(params![key], |r| {
                    Ok((
                        r.get::<_, i64>(0)?,
                        r.get::<_, String>(1)?,
                        r.get::<_, i64>(2)?,
                    ))
                })?
                .filter_map(Result::ok)
                .filter(|(_, at, _)| !state_at.covers(&watermark_from(at)))
                .map(|(seq, _, len)| (seq, len))
                .collect();
            for (seq, len) in &past {
                tx.execute(
                    "DELETE FROM log WHERE key = ?1 AND seq = ?2",
                    params![key, seq],
                )?;
                tx.execute(
                    "UPDATE entries SET log_bytes = log_bytes - ?2 WHERE key = ?1",
                    params![key, len],
                )?;
            }
            if !past.is_empty() {
                info!(target: "chlive_server::store", key = %crate::store::short(&key), dropped = past.len(), "log past the checkpoint dropped (an unclean stop)");
            }
        }
        Op::Stage { key, stage } => {
            tx.execute(
                "UPDATE entries SET stage = ?2, used_at = ?3 WHERE key = ?1",
                params![key, stage, now_ms()],
            )?;
        }
        Op::Delete { key, reason } => {
            tx.execute("DELETE FROM entries WHERE key = ?1", params![key])?;
            tx.execute("DELETE FROM base WHERE key = ?1", params![key])?;
            tx.execute("DELETE FROM log WHERE key = ?1", params![key])?;
            info!(target: "chlive_server::store", key = %short(&key), reason, "entry deleted from the store");
        }
        Op::Flush(_) => unreachable!("handled by the writer"),
    }
    Ok(())
}

/// The fingerprint of a key, for logs.
pub fn short(key: &str) -> String {
    chlive_engine::cursor::hash(key)
}

/// Fold the log messages the checkpoint covers into the base (at the last of them).
fn compact(tx: &rusqlite::Transaction, key: &str) -> rusqlite::Result<()> {
    let started = Instant::now();
    let row: Option<(String, String)> = tx
        .query_row(
            "SELECT state_at, base_at FROM entries WHERE key = ?1",
            params![key],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .optional()?;
    let Some((state_at, _)) = row else {
        return Ok(());
    };
    let state_at = watermark_from(&state_at);
    // the base as stored
    let mut rows = ResultList::default();
    {
        let mut stmt = tx.prepare("SELECT data FROM base WHERE key = ?1 ORDER BY seq")?;
        for chunk in stmt.query_map(params![key], |r| r.get::<_, String>(0))? {
            let chunk: Json = serde_json::from_str(&chunk?).unwrap_or(Json::Null);
            for item in chunk.as_array().into_iter().flatten() {
                rows.rows
                    .push(item["row"].as_object().cloned().unwrap_or_default());
                rows.ids.push(item["id"].as_str().map(String::from));
            }
        }
    }
    let items: Vec<(i64, String, String)> = {
        let mut stmt = tx.prepare("SELECT seq, at, body FROM log WHERE key = ?1 ORDER BY seq")?;
        let items = stmt
            .query_map(params![key], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        items
    };
    let mut folded = 0usize;
    let mut new_base_at: Option<Watermark> = None;
    let mut freed = 0i64;
    for (seq, at, body) in &items {
        let at = watermark_from(at);
        if !state_at.covers(&at) {
            break;
        }
        if let Err(e) = apply_body(&mut rows, body) {
            warn!(key = %short(key), error = %e, "compaction stopped: a logged message didn't apply");
            break;
        }
        tx.execute(
            "DELETE FROM log WHERE key = ?1 AND seq = ?2",
            params![key, seq],
        )?;
        freed += body.len() as i64;
        folded += 1;
        new_base_at = Some(at);
    }
    let Some(base_at) = new_base_at else {
        return Ok(());
    };
    let base_bytes = write_base(tx, key, &rows)?;
    tx.execute(
        "UPDATE entries SET base_at = ?2, base_bytes = ?3, log_bytes = MAX(0, log_bytes - ?4)
         WHERE key = ?1",
        params![key, watermark_json(&base_at), base_bytes, freed],
    )?;
    debug!(target: "chlive_server::store", key = %short(key), folded, rows = rows.rows.len(), ms = started.elapsed().as_millis() as u64, "compacted: the log folded into the base");
    Ok(())
}

/// Over the budget: cold entries go, least recently used first. Returns how many went, and
/// their bytes.
fn evict(conn: &Connection, budget: u64) -> rusqlite::Result<(u64, u64)> {
    let total: i64 = conn.query_row(
        "SELECT COALESCE(SUM(base_bytes + log_bytes + state_bytes), 0) FROM entries",
        [],
        |r| r.get(0),
    )?;
    if total as u64 <= budget {
        return Ok((0, 0));
    }
    let mut over = total - budget as i64;
    let victims: Vec<(String, i64)> = {
        let mut stmt = conn.prepare(
            "SELECT key, base_bytes + log_bytes + state_bytes FROM entries
             WHERE stage = 'cold' ORDER BY used_at ASC",
        )?;
        let v = stmt
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))?
            .collect::<rusqlite::Result<Vec<_>>>()?;
        v
    };
    let mut gone = (0, 0);
    for (key, bytes) in victims {
        if over <= 0 {
            break;
        }
        gone.0 += 1;
        gone.1 += bytes as u64;
        conn.execute("DELETE FROM entries WHERE key = ?1", params![key])?;
        conn.execute("DELETE FROM base WHERE key = ?1", params![key])?;
        conn.execute("DELETE FROM log WHERE key = ?1", params![key])?;
        over -= bytes;
        info!(target: "chlive_server::store", key = %short(&key), bytes, budget, "evicted: over the disk budget");
    }
    Ok(gone)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn row(v: Json) -> Row {
        v.as_object().unwrap().clone()
    }

    fn w(pairs: &[(i32, i64)]) -> Watermark {
        Watermark(pairs.iter().copied().collect())
    }

    fn meta(key: &str) -> Meta {
        Meta {
            key: key.into(),
            fp: short(key),
            sql: "select x from t".into(),
            params: String::new(),
            lineage: "L1".into(),
            forks: vec![("L0".into(), w(&[(0, 3)]))],
            born_at: 1,
        }
    }

    fn diff_body(changes: Json) -> Arc<str> {
        Arc::from(json!({"type": "diff", "id": "x", "changes": changes, "at": "c"}).to_string())
    }

    fn rows(list: &ResultList) -> Vec<String> {
        list.rows.iter().map(|r| r["x"].to_string()).collect()
    }

    #[tokio::test]
    async fn base_log_checkpoint_compact_and_truncate() {
        let dir = tempdir();
        let store = Store::open(&dir, u64::MAX).unwrap();
        let mut base = ResultList::default();
        base.rows.push(row(json!({"x": 1})));
        base.ids.push(None);
        store.create(meta("k"), w(&[(0, 5)]), base, json!({"shape": "append"}));
        store.append(
            "k",
            w(&[(0, 6)]),
            diff_body(json!([{"op": "insert", "row": {"x": 2}}])),
        );
        store.append(
            "k",
            w(&[(0, 7)]),
            diff_body(json!([{"op": "insert", "row": {"x": 3}}])),
        );
        store.checkpoint("k", w(&[(0, 6)]), json!({"shape": "append", "n": 1}));
        store.flush().await;

        let s = store.load("k").unwrap().unwrap();
        assert_eq!(s.meta, meta("k"));
        assert_eq!(s.stage, "live");
        assert_eq!(s.base_at, w(&[(0, 5)]));
        assert_eq!(s.state_at, w(&[(0, 6)]));
        assert_eq!(s.state.as_ref().unwrap()["n"], 1);
        assert_eq!(s.log.len(), 2);
        // everything, or only what the checkpoint covers (after a crash)
        assert_eq!(rows(&replay(&s, false).unwrap().0), ["1", "2", "3"]);
        let (upto, kept) = replay(&s, true).unwrap();
        assert_eq!(
            (rows(&upto), kept.len()),
            (vec!["1".to_string(), "2".into()], 1)
        );

        // compaction folds what the checkpoint covers into the base
        store.compact("k");
        store.flush().await;
        let s = store.load("k").unwrap().unwrap();
        assert_eq!(s.base_at, w(&[(0, 6)]));
        assert_eq!(rows(&s.base), ["1", "2"]);
        assert_eq!(s.log.len(), 1);
        assert_eq!(rows(&replay(&s, false).unwrap().0), ["1", "2", "3"]);

        // truncation drops the log past the checkpoint
        store.truncate("k");
        store.flush().await;
        let s = store.load("k").unwrap().unwrap();
        assert!(s.log.is_empty());

        // a new store on the same dir: the entry is cold
        drop(store);
        let store = Store::open(&dir, u64::MAX).unwrap();
        let s = store.load("k").unwrap().unwrap();
        assert_eq!(s.stage, "cold");
        assert_eq!(store.index().unwrap()[0].stage, "cold");
        store.delete("k", "test");
        store.flush().await;
        assert!(store.load("k").unwrap().is_none());
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[tokio::test]
    async fn over_the_budget_cold_entries_go_least_recently_used_first() {
        let dir = tempdir();
        let store = Store::open(&dir, 1_000_000).unwrap();
        assert_eq!(store.stats().batches, 0);
        let big = |n: usize| {
            let mut r = ResultList::default();
            for i in 0..n {
                r.rows.push(row(json!({"x": "y".repeat(100), "i": i})));
                r.ids.push(None);
            }
            r
        };
        for k in ["a", "b", "c"] {
            store.create(meta(k), w(&[]), big(10), json!({}));
            store.stage(k, "cold");
            store.flush().await;
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
        // a live one isn't evicted however old
        store.stage("a", "live");
        store.flush().await;
        let stats = store.stats();
        assert_eq!((stats.ops, stats.queued, stats.errors), (7, 0, 0));
        assert!(stats.batches >= 4 && stats.last_batch_at > 0, "{stats:?}");
        assert!(stats.file_bytes > 0 && stats.path.ends_with("cache.sqlite3"));
        assert_eq!(stats.evictions, 0);
        drop(store);
        // a much smaller budget: the oldest cold ones go until it fits
        let store = Store::open(&dir, 3_000).unwrap();
        store.stage("a", "live");
        store.flush().await;
        let left: Vec<String> = store.index().unwrap().into_iter().map(|e| e.key).collect();
        assert!(left.contains(&"a".to_string()), "{left:?}");
        assert!(
            !left.contains(&"b".to_string()),
            "the oldest cold one went: {left:?}"
        );
        let stats = store.stats();
        assert_eq!(stats.evictions as usize, 3 - left.len());
        assert!(stats.evicted_bytes > 0);
        std::fs::remove_dir_all(dir).unwrap();
    }

    #[test]
    fn a_new_layout_empties_the_store() {
        let dir = tempdir();
        std::fs::create_dir_all(&dir).unwrap();
        {
            let c = Connection::open(dir.join("cache.sqlite3")).unwrap();
            c.execute_batch("CREATE TABLE entries (x); PRAGMA user_version = 99;")
                .unwrap();
        }
        let store = Store::open(&dir, u64::MAX).unwrap();
        assert!(store.index().unwrap().is_empty());
        std::fs::remove_dir_all(dir).unwrap();
    }

    fn tempdir() -> PathBuf {
        static N: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let d = std::env::temp_dir().join(format!(
            "chlive-store-test-{}-{}",
            std::process::id(),
            N.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&d);
        d
    }
}
