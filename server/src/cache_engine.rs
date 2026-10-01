//! The local cache the server merges in: embedded chDB (ClickHouse as a library), in-process.
//!
//! The engine version must match the main ClickHouse server (see flake.nix and
//! docker-compose.yml): history is materialized as AggregateFunction states read from main
//! through `remote()`, and those are only compatible within a version.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, OnceLock};

use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use chdb_rust::connection::Connection;
use chdb_rust::format::{InputFormat, OutputFormat};
use chdb_rust::query_param::{QueryParam, QueryParams};

use crate::ch_http::{Params, QueryResult};

#[async_trait]
pub trait CacheEngine: Send + Sync + 'static {
    async fn exec(&self, sql: &str, params: &Params) -> Result<()>;
    async fn query(&self, sql: &str, params: &Params) -> Result<QueryResult>;
    async fn insert_json_each_row(&self, table: &str, rows: String) -> Result<()>;
}

/// Settings applied to every connection so results look exactly like main's HTTP JSON.
const SESSION_SETTINGS: &[&str] = &[
    "SET output_format_json_quote_64bit_integers = 0",
    "SET output_format_json_quote_denormals = 1",
];

/// A pool of chDB connections to one in-memory engine.
///
/// All connections share the same tables. Each connection runs one statement at a time, so
/// the pool bounds cache-side parallelism. Connections are opened once and never closed:
/// chDB allows a single engine per process, and repeatedly closing and reopening is known to
/// corrupt the allocator on macOS. [`ChdbCache::shared`] therefore hands out one
/// process-wide pool.
#[derive(Clone)]
pub struct ChdbCache {
    inner: Arc<Pool>,
}

struct Pool {
    conns: Vec<Mutex<Connection>>,
    next: AtomicUsize,
}

impl ChdbCache {
    /// The process-wide pool, created on first use with `pool_size` connections (later calls
    /// reuse it and ignore `pool_size`).
    pub fn shared(pool_size: usize, console_log: bool) -> Result<Self> {
        static POOL: OnceLock<std::result::Result<ChdbCache, String>> = OnceLock::new();
        POOL.get_or_init(|| Self::open(pool_size.max(1), console_log).map_err(|e| format!("{e:#}")))
            .clone()
            .map_err(|e| anyhow!(e))
    }

    fn open(pool_size: usize, console_log: bool) -> Result<Self> {
        // Let tokio own SIGINT/SIGTERM; chDB would otherwise install its own handlers.
        chdb_rust::runtime::signal_handlers(false);
        // chDB writes its own ClickHouse server log to the console whenever stderr is a
        // terminal (e.g. a dnvr pane). Every tail insert made it log
        // `<Fatal> ThreadStatus: current_thread contains invalid address` (~85/s), an
        // internal bookkeeping diagnostic with no effect on results (see NOTES.md). Query
        // errors still come back through the API and are logged by the server.
        let args: &[&str] = if console_log {
            &[]
        } else {
            &["--logger.console=0"]
        };
        let mut conns = Vec::with_capacity(pool_size);
        for i in 0..pool_size {
            let conn =
                Connection::open(args).with_context(|| format!("opening chDB connection {i}"))?;
            for s in SESSION_SETTINGS {
                conn.query(s, OutputFormat::TabSeparated)
                    .with_context(|| format!("applying `{s}`"))?;
            }
            conns.push(Mutex::new(conn));
        }
        tracing::info!(
            version = chdb_rust::version::clickhouse_version().unwrap_or_default(),
            pool_size,
            "chDB cache ready"
        );
        Ok(Self {
            inner: Arc::new(Pool {
                conns,
                next: AtomicUsize::new(0),
            }),
        })
    }

    /// Run `sql` on every connection (session settings such as `session_timezone`).
    pub async fn exec_on_all(&self, sql: &str) -> Result<()> {
        let pool = self.inner.clone();
        let sql = sql.to_string();
        tokio::task::spawn_blocking(move || {
            for c in &pool.conns {
                let c = c.lock().unwrap_or_else(|e| e.into_inner());
                c.query(&sql, OutputFormat::TabSeparated)
                    .map_err(|e| anyhow!("chDB error in `{sql}`: {e}"))?;
            }
            Ok(())
        })
        .await
        .context("chDB task panicked")?
    }

    /// Run `f` on a free connection in the blocking pool.
    async fn with_conn<T: Send + 'static>(
        &self,
        f: impl FnOnce(&mut Connection) -> Result<T> + Send + 'static,
    ) -> Result<T> {
        let pool = self.inner.clone();
        tokio::task::spawn_blocking(move || {
            let mut conn = pool.acquire();
            f(&mut conn)
        })
        .await
        .context("chDB task panicked")?
    }
}

impl Pool {
    fn acquire(&self) -> MutexGuard<'_, Connection> {
        let start = self.next.fetch_add(1, Ordering::Relaxed);
        let n = self.conns.len();
        for i in 0..n {
            if let Ok(c) = self.conns[(start + i) % n].try_lock() {
                return c;
            }
        }
        // All busy: queue on one of them. A panicked query cannot leave a connection in a
        // state we'd need to discard, so a poisoned lock is simply taken over.
        self.conns[start % n]
            .lock()
            .unwrap_or_else(|e| e.into_inner())
    }
}

fn params(p: &Params) -> QueryParams {
    p.iter().fold(QueryParams::new(), |acc, (k, v)| {
        acc.bind(k, QueryParam::Text(v.clone()))
    })
}

#[async_trait]
impl CacheEngine for ChdbCache {
    async fn exec(&self, sql: &str, p: &Params) -> Result<()> {
        let (sql, p) = (sql.to_string(), params(p));
        self.with_conn(move |c| {
            c.query_with_params(&sql, OutputFormat::TabSeparated, p)
                .map(|_| ())
                .map_err(|e| anyhow!("chDB error: {e}"))
        })
        .await
    }

    async fn query(&self, sql: &str, p: &Params) -> Result<QueryResult> {
        let (sql, p) = (sql.to_string(), params(p));
        let text = self
            .with_conn(move |c| {
                c.query_with_params(&sql, OutputFormat::JSON, p)
                    .map(|r| r.data_utf8_lossy().into_owned())
                    .map_err(|e| anyhow!("chDB error: {e}"))
            })
            .await?;
        serde_json::from_str(&text).with_context(|| {
            format!(
                "decoding chDB JSON result: {}",
                text.chars().take(200).collect::<String>()
            )
        })
    }

    async fn insert_json_each_row(&self, table: &str, rows: String) -> Result<()> {
        let sql = format!("INSERT INTO {table}");
        self.with_conn(move |c| {
            let mut ins = c
                .insert_stream(&sql, InputFormat::JSONEachRow)
                .map_err(|e| anyhow!("chDB error: {e}"))?;
            ins.append(rows.as_bytes())
                .map_err(|e| anyhow!("chDB error: {e}"))?;
            ins.finish()
                .map(|_| ())
                .map_err(|e| anyhow!("chDB error: {e}"))
        })
        .await
    }
}
