//! Embedded chDB (ClickHouse as a library): where aggregate states live and get merged.
//!
//! chDB's engine must be the same ClickHouse version as the main server: states cross between
//! them in ClickHouse's binary format (see `nix/packages/libchdb.nix`).

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};

use chdb_rust::connection::Connection;
use chdb_rust::format::OutputFormat;
use serde_json::Value;

use crate::diff::Row;

#[derive(Debug, Clone, thiserror::Error)]
#[error("chDB: {0}")]
pub struct ChdbError(pub String);

/// Applied to every connection, so results render exactly like main's JSON (see
/// `history.rs`), in main's time zone.
const SESSION_SETTINGS: &[&str] = &[
    "SET output_format_json_quote_64bit_integers = 0",
    "SET output_format_json_quote_denormals = 1",
    "SET session_timezone = 'UTC'",
];

/// A pool of connections to the one in-process chDB engine; they share its tables.
#[derive(Clone)]
pub struct Chdb {
    inner: Arc<Pool>,
}

struct Pool {
    /// `None` once closed at process exit.
    conns: Vec<Mutex<Option<Connection>>>,
    next: AtomicUsize,
}

static POOL: OnceLock<Result<Chdb, ChdbError>> = OnceLock::new();

/// Close chDB before the process exits: with connections still open, chDB's own exit handlers
/// crash (SIGSEGV), and closing from an `atexit` handler is too late (some of chDB's internals
/// are already gone). Binaries call this at the end of `main`; queries fail afterwards.
pub fn shutdown() {
    if let Some(Ok(chdb)) = POOL.get() {
        for c in &chdb.inner.conns {
            let mut c = c.lock().unwrap_or_else(|e| e.into_inner());
            c.take();
        }
    }
}

impl Chdb {
    /// The process-wide pool (chDB allows one engine per process), opened on first use.
    pub fn shared() -> Result<Self, ChdbError> {
        POOL.get_or_init(|| Self::open(4)).clone()
    }

    fn open(size: usize) -> Result<Self, ChdbError> {
        // let tokio own SIGINT/SIGTERM
        chdb_rust::runtime::signal_handlers(false);
        // chDB's own server log would otherwise go to the console (and is noisy)
        let args: &[&str] = &["--logger.console=0"];
        let mut conns = Vec::with_capacity(size);
        for i in 0..size {
            let conn = Connection::open(args)
                .map_err(|e| ChdbError(format!("opening connection {i}: {e}")))?;
            for s in SESSION_SETTINGS {
                conn.query(s, OutputFormat::TabSeparated)
                    .map_err(|e| ChdbError(format!("{s}: {e}")))?;
            }
            conns.push(Mutex::new(Some(conn)));
        }
        tracing::info!(
            version = chdb_rust::version::clickhouse_version().unwrap_or_default(),
            "chDB ready"
        );
        Ok(Self {
            inner: Arc::new(Pool {
                conns,
                next: AtomicUsize::new(0),
            }),
        })
    }

    /// Run a statement.
    pub async fn exec(&self, sql: String) -> Result<(), ChdbError> {
        self.run(sql, OutputFormat::TabSeparated).await.map(|_| ())
    }

    /// Run a query; its rows as JSON objects.
    pub async fn rows(&self, sql: String) -> Result<Vec<Row>, ChdbError> {
        let text = self.run(sql, OutputFormat::JSONEachRow).await?;
        text.lines()
            .filter(|l| !l.is_empty())
            .map(|l| {
                serde_json::from_str::<Value>(l)
                    .ok()
                    .and_then(|v| v.as_object().cloned())
                    .ok_or_else(|| ChdbError(format!("bad JSON row: {l}")))
            })
            .collect()
    }

    /// Run `sql` on a free connection, off the async runtime.
    async fn run(&self, sql: String, format: OutputFormat) -> Result<String, ChdbError> {
        let pool = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let conn = pool.acquire();
            let conn = conn
                .as_ref()
                .ok_or_else(|| ChdbError("closed (shut down)".into()))?;
            conn.query(&sql, format)
                .map(|r| r.data_utf8_lossy().into_owned())
                .map_err(|e| ChdbError(format!("{e} (in `{}`)", abbreviate(&sql))))
        })
        .await
        .map_err(|e| ChdbError(format!("task failed: {e}")))?
    }
}

impl Pool {
    fn acquire(&self) -> MutexGuard<'_, Option<Connection>> {
        let start = self.next.fetch_add(1, Ordering::Relaxed);
        let n = self.conns.len();
        for i in 0..n {
            if let Ok(c) = self.conns[(start + i) % n].try_lock() {
                return c;
            }
        }
        self.conns[start % n]
            .lock()
            .unwrap_or_else(|e| e.into_inner())
    }
}

/// The start of a long statement (batches inline their rows), for error messages.
fn abbreviate(sql: &str) -> String {
    if sql.len() <= 300 {
        sql.to_string()
    } else {
        let mut end = 300;
        while !sql.is_char_boundary(end) {
            end -= 1;
        }
        format!("{}…", &sql[..end])
    }
}
