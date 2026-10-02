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
use futures::{SinkExt, StreamExt};
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

/// The dev shell's stack (or CHLIVE_CLICKHOUSE_URL / CHLIVE_KAFKA), as the
/// server's read-only ClickHouse user.
pub fn config() -> Config {
    Config {
        clickhouse_url: std::env::var("CHLIVE_CLICKHOUSE_URL")
            .unwrap_or_else(|_| "http://127.0.0.1:18123".into()),
        user: "chlive_ro".into(),
        password: "chlive_ro".into(),
        database: "default".into(),
        kafka_brokers: std::env::var("CHLIVE_KAFKA").unwrap_or_else(|_| "127.0.0.1:9092".into()),
    }
}

pub async fn start_server() -> Result<Server> {
    Ok(chlive_server::start("127.0.0.1:0", Engine::connect(config())).await?)
}

pub fn producer() -> Result<producer::Producer> {
    producer::Producer::new(&config().kafka_brokers, "events")
}

/// What one subscription has received, applied the way a client applies it.
#[derive(Debug, Clone, Default)]
pub struct LiveResult {
    pub snapshot: bool,
    pub rows: Vec<Row>,
    /// Rows that came as inserts after the snapshot.
    pub inserted: usize,
    pub diffs: usize,
    pub error: Option<(String, String)>,
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
            }
            Some("diff") => {
                if !self.snapshot {
                    self.protocol_violations
                        .push("diff before the snapshot".into());
                }
                self.diffs += 1;
                for change in msg["changes"].as_array().expect("changes") {
                    match change["op"].as_str() {
                        Some("insert") => {
                            self.rows
                                .push(change["row"].as_object().expect("row").clone());
                            self.inserted += 1;
                        }
                        other => self
                            .protocol_violations
                            .push(format!("unknown op {other:?}")),
                    }
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
        let (ws, _) = tokio_tungstenite::connect_async(&url)
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
                        _ => return,
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

/// Highest offset per partition that ClickHouse has ingested into `default.events`.
pub async fn ingested() -> Result<BTreeMap<i32, i64>> {
    let rows = clickhouse()
        .fetch("SELECT _partition AS p, max(_offset) AS o FROM default.events GROUP BY p")
        .await?;
    Ok(rows
        .iter()
        .map(|r| (r["p"].as_i64().unwrap() as i32, r["o"].as_i64().unwrap()))
        .collect())
}

/// Wait until ClickHouse has ingested everything up to `produced`, then until the connection
/// has been quiet for a while (the stream has delivered the rest).
pub async fn settle(conn: &LiveConn, produced: &BTreeMap<i32, i64>) -> Result<()> {
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
    conn.wait_quiet(Duration::from_millis(1000), Duration::from_secs(60))
        .await
}

/// The query run directly on ClickHouse, unbounded: the SQL the engine plans for its history.
pub async fn direct(sql: &str) -> Result<Vec<Row>> {
    let plan = Engine::connect(config())
        .explain(sql)
        .await
        .map_err(|e| anyhow::anyhow!("{e}"))?;
    Ok(clickhouse().fetch(&plan.history.sql(None)).await?)
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
