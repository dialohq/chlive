//! Helpers for end-to-end tests against the local stack (`dnvr up`).
//!
//! Tests only run when `CHLIVE_E2E=1` (the `e2e` script sets it); they need `dnvr up` and no other
//! producers writing to the `events` topic while they run.

use std::collections::{BTreeMap, HashMap};
use std::path::Path;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use chlive_server::ch_http::{ChHttp, Params};
use chlive_server::config::Config;
use chlive_server::watermark::Watermark;
use chlive_server::Server;
use futures::{SinkExt, StreamExt};
use indexmap::IndexMap;
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

/// Tests share one Kafka topic and one main table, so they must not overlap.
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

pub fn config() -> Result<Config> {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../chlive.toml");
    let mut cfg = Config::load(&path)?;
    cfg.listen = "127.0.0.1:0".into();
    cfg.debounce_ms = 50;
    Ok(cfg)
}

/// Start a server. All servers in this test process share the one embedded chDB engine
/// (chDB allows a single engine per process); each start drops the previous tables.
pub async fn start_server() -> Result<(Server, Config)> {
    let cfg = config()?;
    let server = chlive_server::start(cfg.clone()).await?;
    Ok((server, cfg))
}

pub fn main_client(cfg: &Config) -> ChHttp {
    ChHttp::new(
        &cfg.main.url,
        &cfg.main.user,
        &cfg.main.password,
        &cfg.main.database,
    )
}

pub fn generator(cfg: &Config) -> Result<chlive_generator::Generator> {
    chlive_generator::Generator::new(&cfg.kafka.brokers, &cfg.tables[0].topic)
}

/// Client-side reconstruction of one live query, built exactly like `@chlive/client`.
#[derive(Debug, Default, Clone)]
pub struct LiveResult {
    pub rows: IndexMap<String, Row>,
    pub order: Option<Vec<String>>,
    pub snapshots: usize,
    pub patches: usize,
    pub error: Option<(String, String)>,
    pub last_msg: Option<Instant>,
}

impl LiveResult {
    fn apply(&mut self, msg: &Value) {
        self.last_msg = Some(Instant::now());
        let rows = |v: &Value| -> Vec<Row> {
            v.as_array()
                .into_iter()
                .flatten()
                .filter_map(|r| r.as_object().cloned())
                .collect()
        };
        let order = msg.get("order").and_then(|o| {
            o.as_array().map(|a| {
                a.iter()
                    .map(|k| k.as_str().unwrap_or_default().to_string())
                    .collect()
            })
        });
        match msg["type"].as_str() {
            Some("snapshot") => {
                self.snapshots += 1;
                self.rows.clear();
                for r in rows(&msg["rows"]) {
                    self.rows
                        .insert(r["__key"].as_str().unwrap().to_string(), r);
                }
                self.order = order;
            }
            Some("patch") => {
                self.patches += 1;
                for k in msg["deletes"].as_array().into_iter().flatten() {
                    self.rows.shift_remove(k.as_str().unwrap());
                }
                for r in rows(&msg["upserts"]) {
                    self.rows
                        .insert(r["__key"].as_str().unwrap().to_string(), r);
                }
                if order.is_some() {
                    self.order = order;
                }
            }
            Some("error") => {
                self.error = Some((
                    msg["code"].as_str().unwrap_or_default().into(),
                    msg["message"].as_str().unwrap_or_default().into(),
                ))
            }
            _ => {}
        }
    }

    /// Rows in result order, without `__key`.
    pub fn ordered_rows(&self) -> Vec<Row> {
        let keys: Vec<&String> = match &self.order {
            Some(o) => o.iter().collect(),
            None => self.rows.keys().collect(),
        };
        keys.into_iter()
            .filter_map(|k| self.rows.get(k))
            .map(|r| {
                let mut r = r.clone();
                r.shift_remove("__key");
                r
            })
            .collect()
    }
}

pub struct LiveConn {
    tx: mpsc::UnboundedSender<String>,
    results: Arc<Mutex<HashMap<String, LiveResult>>>,
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
        let res = results.clone();
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
            _task: task,
        })
    }

    pub fn subscribe(&self, id: &str, sql: &str, params: Value) {
        let msg = json!({"type": "subscribe", "id": id, "sql": sql, "params": params});
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
                        .is_none_or(|r| r.snapshots == 0 && r.error.is_none())
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
            let last = self
                .results
                .lock()
                .unwrap()
                .values()
                .filter_map(|r| r.last_msg)
                .max();
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

pub async fn main_max_offsets(main: &ChHttp, table: &str) -> Result<BTreeMap<u32, i64>> {
    let r = main
        .query(
            &chlive_server::watermark::max_offsets_sql(main.database(), table),
            &Params::new(),
        )
        .await?;
    Ok(r.data
        .iter()
        .map(|row| {
            (
                row["p"].as_u64().unwrap() as u32,
                row["o"].as_i64().unwrap(),
            )
        })
        .collect())
}

/// Wait until main and the server's tail have ingested everything up to `produced`, then
/// until the client has seen no patches for a while.
pub async fn settle(
    server: &Server,
    main: &ChHttp,
    conn: &LiveConn,
    produced: &BTreeMap<i32, i64>,
) -> Result<()> {
    let target = Watermark(produced.iter().map(|(p, o)| (*p as u32, *o)).collect());
    let deadline = Instant::now() + Duration::from_secs(120);
    loop {
        let main_w = Watermark(main_max_offsets(main, "events").await?);
        let tail_w = server.tables["events"].tail_max.read().unwrap().clone();
        if main_w.covers(&target) && tail_w.covers(&target) {
            break;
        }
        if Instant::now() > deadline {
            bail!("ingestion did not catch up: target={target:?} main={main_w:?} tail={tail_w:?}");
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    conn.wait_quiet(Duration::from_millis(750), Duration::from_secs(60))
        .await
}

#[derive(Debug, Clone, Copy)]
pub enum Tol {
    Exact,
    /// Relative tolerance for numbers (and element-wise for arrays).
    Rel(f64),
    /// Within `rel` relative OR `abs` absolute (for sampled quantiles of small values).
    Approx {
        rel: f64,
        abs: f64,
    },
}

fn close(a: &Value, b: &Value, tol: Tol) -> bool {
    match (a, b, tol) {
        (Value::Number(x), Value::Number(y), Tol::Rel(_) | Tol::Approx { .. }) => {
            let (x, y) = (x.as_f64().unwrap(), y.as_f64().unwrap());
            let (rel, abs) = match tol {
                Tol::Rel(r) => (r, 0.0),
                Tol::Approx { rel, abs } => (rel, abs),
                Tol::Exact => unreachable!(),
            };
            let d = (x - y).abs();
            d <= rel * x.abs().max(y.abs()).max(1e-12) || d <= abs
        }
        (Value::Array(x), Value::Array(y), _) => {
            x.len() == y.len() && x.iter().zip(y).all(|(a, b)| close(a, b, tol))
        }
        _ => a == b,
    }
}

/// Assert `live` equals `direct` row by row, using `tol(column)` per column.
pub fn assert_results_match(name: &str, live: &[Row], direct: &[Row], tol: impl Fn(&str) -> Tol) {
    let show = |rows: &[Row]| {
        rows.iter()
            .map(|r| Value::Object(r.clone()).to_string())
            .collect::<Vec<_>>()
            .join("\n  ")
    };
    assert_eq!(
        live.len(),
        direct.len(),
        "{name}: row count differs\nlive:\n  {}\ndirect:\n  {}",
        show(live),
        show(direct)
    );
    for (i, (l, d)) in live.iter().zip(direct).enumerate() {
        let lk: Vec<&String> = l.keys().collect();
        let dk: Vec<&String> = d.keys().collect();
        assert_eq!(lk, dk, "{name}: columns differ in row {i}");
        for (col, dv) in d {
            let lv = &l[col];
            assert!(
                close(lv, dv, tol(col)),
                "{name}: row {i} column {col}: live={lv} direct={dv} (tol {:?})\nlive:\n  {}\ndirect:\n  {}",
                tol(col),
                show(live),
                show(direct)
            );
        }
    }
}

/// Run a query on main with JSON params, the way a client would have sent them.
pub async fn direct(main: &ChHttp, sql: &str, params: &Value) -> Result<Vec<Row>> {
    let p: Params = params
        .as_object()
        .into_iter()
        .flatten()
        .map(|(k, v)| {
            (
                k.clone(),
                v.as_str()
                    .map(str::to_string)
                    .unwrap_or_else(|| v.to_string()),
            )
        })
        .collect();
    Ok(main.query(sql, &p).await?.data)
}
