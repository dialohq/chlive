//! Helpers for end-to-end tests against the full local stack (`dnvr up` in the default shell):
//! an in-process `chlive-server` over the real ClickHouse and Redpanda, spoken to over
//! WebSocket like a client would.
//!
//! Tests only run when `CHLIVE_E2E=1` (the `e2e` script sets it); they need `dnvr up` and no
//! other producers writing to the `events` topic while they run.

pub mod producer;

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use chlive_engine::history::{ClickHouseHttp, HistorySource};
use chlive_engine::{Config, Engine};
use chlive_server::Server;
use futures::{FutureExt, SinkExt, StreamExt};
use serde_json::{json, Map, Value};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;

pub type Row = Map<String, Value>;

pub fn enabled() -> bool {
    let on = std::env::var_os("CHLIVE_E2E").is_some();
    if !on {
        eprintln!("skipping: set CHLIVE_E2E=1 (requires `dnvr up`; the `e2e` script sets it)");
    }
    on
}

/// Tests share one Kafka topic and one ClickHouse table, so they must not overlap.
pub async fn exclusive() -> tokio::sync::MutexGuard<'static, ()> {
    static LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();
    LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
        .lock()
        .await
}

pub fn init_tracing() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            std::env::var("RUST_LOG").unwrap_or_else(|_| "warn,chlive_server=info".into()),
        )
        .with_test_writer()
        .try_init();
}

/// The stack at CHLIVE_CLICKHOUSE_URL / CHLIVE_KAFKA / CHLIVE_PAUSE_SERVER (the `e2e` script
/// sets them to the dev shell's), as the server's ClickHouse users.
pub fn config() -> Config {
    let env = |k: &str, default: &str| std::env::var(k).unwrap_or_else(|_| default.into());
    Config {
        pause_server_url: env("CHLIVE_PAUSE_SERVER", "http://127.0.0.1:18091"),
        snapshot_user: "chlive_snap".into(),
        snapshot_password: "chlive_snap".into(),
        // as the dev shell's server: snapshots in their own database
        snapshots: chlive_engine::snapshot::SnapshotConfig {
            database: Some("chlive_snapshots".into()),
            ..Default::default()
        },
        clickhouse_url: std::env::var("CHLIVE_CLICKHOUSE_URL")
            .unwrap_or_else(|_| "http://127.0.0.1:18123".into()),
        user: "chlive_ro".into(),
        password: "chlive_ro".into(),
        tables: chlive_engine::rules::TablesConfig::listed([
            "events",
            "orders",
            "customers",
            "products",
        ]),
        kafka_brokers: std::env::var("CHLIVE_KAFKA").unwrap_or_else(|_| "127.0.0.1:9092".into()),
        kafka_properties: Default::default(),
        metadata_refresh: chlive_engine::source::METADATA_REFRESH,
        now_coalescing: chlive_engine::engine::NOW_COALESCING,
    }
}

/// Run `test` against a server on `config`, then shut the server down (dropping its snapshots),
/// also when the test fails or panics.
pub async fn with_server<F>(config: Config, test: F) -> Result<()>
where
    F: for<'a> FnOnce(&'a Server) -> futures::future::BoxFuture<'a, Result<()>>,
{
    with_server_settings(config, chlive_server::ServerSettings::default(), test).await
}

/// [`with_server`] with these server settings (e.g. how long shared results are kept).
pub async fn with_server_settings<F>(
    config: Config,
    settings: chlive_server::ServerSettings,
    test: F,
) -> Result<()>
where
    F: for<'a> FnOnce(&'a Server) -> futures::future::BoxFuture<'a, Result<()>>,
{
    let server =
        chlive_server::start_with("127.0.0.1:0", settings, Engine::connect(config)?).await?;
    let result = std::panic::AssertUnwindSafe(test(&server))
        .catch_unwind()
        .await;
    server.shutdown().await;
    match result {
        Ok(r) => r,
        Err(panic) => std::panic::resume_unwind(panic),
    }
}

pub fn producer() -> Result<producer::Producer> {
    producer::Producer::new(&config().kafka_brokers, "events")
}

/// What one subscription has received, applied the way a client applies it.
#[derive(Debug, Clone, Default)]
pub struct LiveResult {
    pub snapshot: bool,
    pub rows: Vec<Row>,
    /// Row ids (aggregated results), parallel to `rows`.
    pub ids: Vec<Option<String>>,
    pub updated: usize,
    /// Rows deleted (pushed out by a LIMIT, or FINAL's deleted keys).
    pub deleted: usize,
    /// Rows that came as inserts after the snapshot.
    pub inserted: usize,
    pub diffs: usize,
    pub error: Option<(String, String)>,
    /// From the snapshot; checked after every diff.
    pub limit: Option<usize>,
    /// Whether the server resumed (instead of a snapshot).
    pub resumed: bool,
    /// The last cursor received.
    pub at: Option<String>,
    /// Anything after an error, or a second snapshot.
    pub protocol_violations: Vec<String>,
}

impl LiveResult {
    fn apply(&mut self, msg: &Value) {
        if self.error.is_some() {
            self.protocol_violations
                .push(format!("after an error: {msg}"));
        }
        match msg["type"].as_str() {
            Some("snapshot") => {
                if self.snapshot {
                    self.protocol_violations.push("second snapshot".into());
                }
                self.snapshot = true;
                self.rows = rows(&msg["rows"]);
                self.ids = match msg["ids"].as_array() {
                    Some(ids) => ids.iter().map(|i| i.as_str().map(str::to_string)).collect(),
                    None => vec![None; self.rows.len()],
                };
                self.limit = msg["limit"].as_u64().map(|n| n as usize);
                self.at = msg["at"].as_str().map(str::to_string);
            }
            Some("resume") => {
                // the rows we hold stay; diffs follow
                self.snapshot = true;
                self.resumed = true;
                self.limit = msg["limit"].as_u64().map(|n| n as usize);
            }
            Some("progress") => self.at = msg["at"].as_str().map(str::to_string),
            Some("diff") => {
                self.at = msg["at"].as_str().map(str::to_string);
                if !self.snapshot {
                    self.protocol_violations
                        .push("diff before the snapshot".into());
                }
                self.diffs += 1;
                for change in msg["changes"].as_array().expect("changes") {
                    match change["op"].as_str() {
                        Some("insert") => {
                            let row = change["row"].as_object().expect("row").clone();
                            match change["index"].as_u64().map(|i| i as usize) {
                                None => {
                                    self.rows.push(row);
                                    self.ids.push(change["id"].as_str().map(str::to_string));
                                }
                                Some(i) if i <= self.rows.len() => {
                                    self.rows.insert(i, row);
                                    self.ids
                                        .insert(i, change["id"].as_str().map(str::to_string));
                                }
                                Some(i) => self
                                    .protocol_violations
                                    .push(format!("insert at {i} of {} rows", self.rows.len())),
                            }
                            self.inserted += 1;
                        }
                        Some("update") => {
                            let id = change["id"].as_str();
                            match self.ids.iter().position(|x| x.as_deref() == id) {
                                Some(i) => {
                                    for (k, v) in change["set"].as_object().expect("set") {
                                        self.rows[i].insert(k.clone(), v.clone());
                                    }
                                    self.updated += 1;
                                    // ordered aggregates: the row moves to `index`
                                    match change["index"].as_u64().map(|t| t as usize) {
                                        None => {}
                                        Some(to) if to < self.rows.len() => {
                                            let (row, id) =
                                                (self.rows.remove(i), self.ids.remove(i));
                                            self.rows.insert(to, row);
                                            self.ids.insert(to, id);
                                        }
                                        Some(to) => self.protocol_violations.push(format!(
                                            "move to {to} of {} rows",
                                            self.rows.len()
                                        )),
                                    }
                                }
                                None => self
                                    .protocol_violations
                                    .push(format!("update of unknown row {id:?}")),
                            }
                        }
                        Some("delete") => {
                            // by index (LIMIT), or by id (FINAL)
                            let at = match (change["index"].as_u64(), change["id"].as_str()) {
                                (Some(i), None) => Some(i as usize),
                                (None, Some(id)) => {
                                    self.ids.iter().position(|x| x.as_deref() == Some(id))
                                }
                                _ => None,
                            };
                            match at {
                                Some(i) if i < self.rows.len() => {
                                    self.rows.remove(i);
                                    self.ids.remove(i);
                                    self.deleted += 1;
                                }
                                _ => self
                                    .protocol_violations
                                    .push(format!("delete {change} of {} rows", self.rows.len())),
                            }
                        }
                        other => self
                            .protocol_violations
                            .push(format!("unknown op {other:?}")),
                    }
                }
                if let Some(n) = self.limit.filter(|n| self.rows.len() > *n) {
                    self.protocol_violations
                        .push(format!("{} rows after a diff, LIMIT {n}", self.rows.len()));
                }
            }
            Some("error") => {
                self.error = Some((
                    msg["code"].as_str().unwrap_or_default().into(),
                    msg["message"].as_str().unwrap_or_default().into(),
                ))
            }
            _ => self
                .protocol_violations
                .push(format!("unknown message {msg}")),
        }
    }
}

fn rows(v: &Value) -> Vec<Row> {
    v.as_array()
        .expect("rows")
        .iter()
        .map(|r| r.as_object().expect("row").clone())
        .collect()
}

/// One WebSocket connection; a background task applies every message to its subscription's
/// [`LiveResult`].
pub struct LiveConn {
    tx: mpsc::UnboundedSender<String>,
    results: Arc<Mutex<HashMap<String, LiveResult>>>,
    last_msg: Arc<Mutex<Option<Instant>>>,
    _task: tokio::task::JoinHandle<()>,
}

impl LiveConn {
    pub async fn connect(server: &Server) -> Result<Self> {
        let url = format!("ws://{}/live", server.addr);
        // like the clients: no limit on message size (a snapshot is one message)
        let config = tokio_tungstenite::tungstenite::protocol::WebSocketConfig::default()
            .max_message_size(None)
            .max_frame_size(None);
        let (ws, _) = tokio_tungstenite::connect_async_with_config(&url, Some(config), false)
            .await
            .context("connecting websocket")?;
        let (mut sink, mut stream) = ws.split();
        let (tx, mut rx) = mpsc::unbounded_channel::<String>();
        let results: Arc<Mutex<HashMap<String, LiveResult>>> = Default::default();
        let last_msg: Arc<Mutex<Option<Instant>>> = Default::default();
        let (res, last) = (results.clone(), last_msg.clone());
        let task = tokio::spawn(async move {
            loop {
                tokio::select! {
                    out = rx.recv() => match out {
                        Some(text) => if sink.send(Message::Text(text.into())).await.is_err() { return },
                        None => { let _ = sink.close().await; return }
                    },
                    msg = stream.next() => match msg {
                        Some(Ok(Message::Text(t))) => {
                            let v: Value = serde_json::from_str(&t).expect("server sends JSON");
                            let id = v["id"].as_str().unwrap_or_default().to_string();
                            res.lock().unwrap().entry(id).or_default().apply(&v);
                            *last.lock().unwrap() = Some(Instant::now());
                        }
                        Some(Ok(_)) => {}
                        Some(Err(e)) => {
                            // the tests then wait in vain: say why
                            eprintln!("live connection failed: {e}");
                            return;
                        }
                        None => return,
                    }
                }
            }
        });
        Ok(Self {
            tx,
            results,
            last_msg,
            _task: task,
        })
    }

    pub fn subscribe(&self, id: &str, sql: &str) {
        let msg = json!({"type": "subscribe", "id": id, "sql": sql});
        self.tx.send(msg.to_string()).expect("socket open");
    }

    /// Subscribe with values for the query's parameters (`{name:Type}`).
    pub fn subscribe_with(&self, id: &str, sql: &str, params: serde_json::Value) {
        let msg = json!({"type": "subscribe", "id": id, "sql": sql, "params": params});
        self.tx.send(msg.to_string()).expect("socket open");
    }

    /// Resume a result we hold (`rows`, complete up to the cursor `since`).
    pub fn resume(&self, id: &str, sql: &str, since: &str, rows: Vec<Row>) {
        self.results.lock().unwrap().insert(
            id.to_string(),
            LiveResult {
                ids: vec![None; rows.len()],
                rows,
                ..Default::default()
            },
        );
        let msg = json!({"type": "subscribe", "id": id, "sql": sql, "since": since});
        self.tx.send(msg.to_string()).expect("socket open");
    }

    /// Resume with a whole result as kept: its rows, their ids (aggregated and FINAL results)
    /// and its cursor.
    pub fn resume_kept(&self, id: &str, sql: &str, kept: &LiveResult) {
        self.results.lock().unwrap().insert(
            id.to_string(),
            LiveResult {
                rows: kept.rows.clone(),
                ids: kept.ids.clone(),
                ..Default::default()
            },
        );
        let since = kept.at.as_deref().expect("a cursor");
        let msg = json!({"type": "subscribe", "id": id, "sql": sql, "since": since});
        self.tx.send(msg.to_string()).expect("socket open");
    }

    pub fn unsubscribe(&self, id: &str) {
        self.tx
            .send(json!({"type": "unsubscribe", "id": id}).to_string())
            .expect("socket open");
    }

    pub fn result(&self, id: &str) -> Option<LiveResult> {
        self.results.lock().unwrap().get(id).cloned()
    }

    /// Wait until every id has received a snapshot or an error.
    pub async fn wait_ready(&self, ids: &[&str], timeout: Duration) -> Result<()> {
        let deadline = Instant::now() + timeout;
        loop {
            let pending: Vec<&str> = ids
                .iter()
                .copied()
                .filter(|id| {
                    self.result(id)
                        .is_none_or(|r| !r.snapshot && r.error.is_none())
                })
                .collect();
            if pending.is_empty() {
                return Ok(());
            }
            if Instant::now() > deadline {
                bail!("no snapshot for {pending:?}");
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }

    /// Wait until no message has arrived for `quiet`.
    pub async fn wait_quiet(&self, quiet: Duration, timeout: Duration) -> Result<()> {
        let deadline = Instant::now() + timeout;
        loop {
            let last = *self.last_msg.lock().unwrap();
            if last.is_none_or(|l| l.elapsed() >= quiet) {
                return Ok(());
            }
            if Instant::now() > deadline {
                bail!("connection never went quiet");
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}

pub fn clickhouse() -> ClickHouseHttp {
    let c = config();
    ClickHouseHttp::new(&c.clickhouse_url, &c.user, &c.password)
}

/// Highest offset per partition that ClickHouse has ingested into `default.events`, from the
/// sink connector's exactly-once state (KeeperMap, keyed `events-<partition>`).
pub async fn ingested() -> Result<BTreeMap<i32, i64>> {
    ingested_from("events").await
}

/// The same for another topic's table.
pub async fn ingested_from(topic: &str) -> Result<BTreeMap<i32, i64>> {
    let rows = clickhouse()
        .fetch(&format!(
            "SELECT toInt32(splitByChar('-', key)[-1]) AS p, maxOffset AS o \
             FROM default.chlive_sink_{topic}_state WHERE state = 'AFTER_PROCESSING'"
        ))
        .await?;
    Ok(rows
        .iter()
        .map(|r| (r["p"].as_i64().unwrap() as i32, r["o"].as_i64().unwrap()))
        .collect())
}

/// Wait until ClickHouse has ingested everything up to `produced`, then until the connection
/// has been quiet for a while (the stream has delivered the rest).
pub async fn settle(conn: &LiveConn, produced: &BTreeMap<i32, i64>) -> Result<()> {
    wait_ingested(produced).await?;
    conn.wait_quiet(Duration::from_millis(1000), Duration::from_secs(60))
        .await
}

/// Wait until ClickHouse has ingested everything up to `produced`.
pub async fn wait_ingested(produced: &BTreeMap<i32, i64>) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(120);
    loop {
        let have = ingested().await?;
        if produced
            .iter()
            .all(|(p, o)| have.get(p).is_some_and(|h| h >= o))
        {
            break;
        }
        if Instant::now() > deadline {
            bail!("ClickHouse did not catch up: produced={produced:?} ingested={have:?}");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    Ok(())
}

/// The query run directly on ClickHouse, unbounded: the SQL the engine plans for its history.
pub async fn direct(sql: &str) -> Result<Vec<Row>> {
    let plan = Engine::connect(config())?
        .explain(sql)
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    let rows = clickhouse()
        .fetch(&plan.history.sql(&plan.history.table))
        .await?;
    // the history has the rows above an OFFSET too (the engine holds them): skip them, as
    // ClickHouse's OFFSET would
    let skip = plan.stream.offset.unwrap_or(0) as usize;
    Ok(rows.into_iter().skip(skip).collect())
}

/// Rows as a sorted list of their JSON, to compare results as multisets (duplicates count).
pub fn multiset(rows: &[Row]) -> Vec<String> {
    let mut v: Vec<String> = rows
        .iter()
        .map(|r| serde_json::to_string(r).unwrap())
        .collect();
    v.sort();
    v
}

/// An ordered result as tie groups: each run of equal `keys` values, in order, with its rows
/// sorted. Two results with the same groups agree on the order up to the order within ties.
pub fn tie_groups(rows: &[Row], keys: &[&str]) -> Vec<(String, Vec<String>)> {
    let mut out: Vec<(String, Vec<String>)> = Vec::new();
    for row in rows {
        let key: Vec<String> = keys.iter().map(|k| row[*k].to_string()).collect();
        let key = key.join(" | ");
        // floats to 9 significant digits: ClickHouse adds in parallel, in no particular order
        let text = row
            .iter()
            .map(|(k, v)| match v.as_f64() {
                Some(f) if !v.is_i64() && !v.is_u64() => format!("{k}={f:.9e}"),
                _ => format!("{k}={v}"),
            })
            .collect::<Vec<_>>()
            .join(",");
        match out.last_mut() {
            Some((last, rows)) if *last == key => rows.push(text),
            _ => out.push((key, vec![text])),
        }
    }
    for (_, rows) in &mut out {
        rows.sort();
    }
    out
}

/// `/debug/subscriptions`: the server's shared entries.
pub async fn entries(server: &Server) -> Result<Vec<serde_json::Value>> {
    let url = format!("http://{}/debug/subscriptions", server.addr);
    Ok(reqwest::get(url).await?.json().await?)
}
