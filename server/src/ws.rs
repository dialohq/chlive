//! `/live` (the WebSocket protocol, see [`crate::protocol`]), `/debug/subscriptions` and
//! `/healthz`.
//!
//! Each connection has a writer task fed by a bounded channel, and one task per subscription
//! that runs an engine [`Subscription`](chlive_engine::Subscription) and forwards its
//! iterations. Unsubscribing or disconnecting aborts the task, which drops the subscription and
//! with it its Kafka consumer. A slow client fills the channel, which pauses its subscriptions.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::State;
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use chlive_engine::cursor;
use chlive_engine::watermark::Watermark;
use chlive_engine::{Engine, Origin};
use futures::{SinkExt, StreamExt};
use serde::Serialize;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

use crate::protocol::{
    change, engine_error, order_terms, Change, ClientMsg, Column, ServerMsg, BAD_MESSAGE,
    DUPLICATE_ID, TOO_MANY_SUBSCRIPTIONS,
};

/// Iterations already waiting are sent together, in one `diff`, up to this many.
const MAX_ITERATIONS_PER_MESSAGE: usize = 256;
/// Messages buffered per connection before its subscriptions pause.
const OUTBOX: usize = 256;
/// The server's own settings (the `[server]` config section, besides `listen`).
#[derive(Debug, Clone, PartialEq)]
pub struct ServerSettings {
    /// Where the WebSocket is served.
    pub path: String,
    /// Browsers' `Origin`s allowed to connect: empty allows any, `"*"` too. Clients that send
    /// no `Origin` (services, not browsers) are always allowed.
    pub allowed_origins: Vec<String>,
    /// Open WebSockets at most; more are refused with 503.
    pub max_connections: usize,
    /// Subscriptions running on one connection at most; more get `TOO_MANY_SUBSCRIPTIONS`.
    pub max_subscriptions_per_connection: usize,
    /// At most this often, a `progress` tells a client how far its result is complete when only
    /// events that didn't match arrived.
    pub progress_interval: Duration,
}

impl Default for ServerSettings {
    fn default() -> Self {
        Self {
            path: "/live".into(),
            allowed_origins: Vec::new(),
            max_connections: 10_000,
            max_subscriptions_per_connection: 100,
            progress_interval: Duration::from_secs(1),
        }
    }
}

#[derive(Clone)]
struct AppState {
    engine: Arc<Engine>,
    settings: Arc<ServerSettings>,
    registry: Arc<Registry>,
    next_conn: Arc<AtomicU64>,
    connections: Arc<AtomicUsize>,
}

pub fn router(engine: Arc<Engine>, settings: ServerSettings) -> Router {
    let path = settings.path.clone();
    let state = AppState {
        engine,
        settings: Arc::new(settings),
        registry: Arc::default(),
        next_conn: Arc::new(AtomicU64::new(1)),
        connections: Arc::default(),
    };
    Router::new()
        .route(&path, get(live))
        .route("/debug/subscriptions", get(debug_subscriptions))
        .route("/healthz", get(|| async { "ok" }))
        .with_state(state)
}

/// Open subscriptions, for `/debug/subscriptions`.
#[derive(Default)]
struct Registry(Mutex<BTreeMap<(u64, String), Arc<Entry>>>);

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct Entry {
    sql: String,
    params: chlive_protocol::Params,
    /// Set once subscribed: where history ends and the stream begins.
    watermark: Mutex<Option<String>>,
    /// Whether it resumed from the client's cursor rather than starting with a snapshot.
    resumed: AtomicBool,
    rows_sent: AtomicU64,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct DebugEntry {
    conn: u64,
    id: String,
    sql: String,
    #[serde(skip_serializing_if = "chlive_protocol::Params::is_empty")]
    params: chlive_protocol::Params,
    watermark: Option<String>,
    resumed: bool,
    rows_sent: u64,
}

impl Registry {
    fn insert(&self, conn: u64, id: &str, entry: Arc<Entry>) {
        self.0.lock().unwrap().insert((conn, id.to_string()), entry);
    }

    fn remove(&self, conn: u64, id: &str) {
        self.0.lock().unwrap().remove(&(conn, id.to_string()));
    }

    fn remove_conn(&self, conn: u64) {
        self.0.lock().unwrap().retain(|(c, _), _| *c != conn);
    }

    fn list(&self) -> Vec<DebugEntry> {
        self.0
            .lock()
            .unwrap()
            .iter()
            .map(|((conn, id), e)| DebugEntry {
                conn: *conn,
                id: id.clone(),
                sql: e.sql.clone(),
                params: e.params.clone(),
                watermark: e.watermark.lock().unwrap().clone(),
                resumed: e.resumed.load(Ordering::Relaxed),
                rows_sent: e.rows_sent.load(Ordering::Relaxed),
            })
            .collect()
    }
}

async fn debug_subscriptions(State(s): State<AppState>) -> impl IntoResponse {
    Json(s.registry.list())
}

/// One open WebSocket, counted against `max_connections` until dropped.
struct Slot(Arc<AtomicUsize>);

impl Drop for Slot {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::Relaxed);
    }
}

fn origin_allowed(allowed: &[String], origin: Option<&str>) -> bool {
    match origin {
        None => true,
        Some(o) => allowed.is_empty() || allowed.iter().any(|a| a == "*" || a == o),
    }
}

async fn live(ws: WebSocketUpgrade, headers: HeaderMap, State(s): State<AppState>) -> Response {
    let origin = headers.get(header::ORIGIN).and_then(|v| v.to_str().ok());
    if !origin_allowed(&s.settings.allowed_origins, origin) {
        debug!(origin, "origin not allowed");
        return (StatusCode::FORBIDDEN, "origin not allowed").into_response();
    }
    if s.connections.fetch_add(1, Ordering::Relaxed) >= s.settings.max_connections {
        s.connections.fetch_sub(1, Ordering::Relaxed);
        warn!(max = s.settings.max_connections, "too many connections");
        return (StatusCode::SERVICE_UNAVAILABLE, "too many connections").into_response();
    }
    // freed when the connection ends, or if the upgrade fails (the callback is dropped)
    let slot = Slot(s.connections.clone());
    let conn = s.next_conn.fetch_add(1, Ordering::Relaxed);
    ws.on_upgrade(move |socket| async move {
        connection(socket, s, conn).await;
        drop(slot);
    })
}

async fn connection(socket: WebSocket, s: AppState, conn: u64) {
    info!(conn, "connected");
    let (mut sink, mut incoming) = socket.split();
    let (tx, mut rx) = mpsc::channel::<ServerMsg>(OUTBOX);
    let writer = tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            let text = serde_json::to_string(&msg).expect("server messages serialize");
            if sink.send(Message::Text(text.into())).await.is_err() {
                break;
            }
        }
    });

    let mut subscriptions: HashMap<String, JoinHandle<()>> = HashMap::new();
    while let Some(Ok(msg)) = incoming.next().await {
        let text = match msg {
            Message::Text(t) => t,
            Message::Close(_) => break,
            _ => continue,
        };
        let msg = match serde_json::from_str::<ClientMsg>(&text) {
            Ok(m) => m,
            Err(e) => {
                let _ = tx
                    .send(ServerMsg::error(
                        None,
                        BAD_MESSAGE,
                        format!("bad message: {e}"),
                    ))
                    .await;
                continue;
            }
        };
        match msg {
            ClientMsg::Subscribe {
                id,
                sql,
                since,
                params,
            } => {
                if subscriptions.get(&id).is_some_and(|t| !t.is_finished()) {
                    let _ = tx
                        .send(ServerMsg::error(
                            Some(&id),
                            DUPLICATE_ID,
                            format!("subscription {id} already exists on this connection"),
                        ))
                        .await;
                    continue;
                }
                subscriptions.retain(|_, t| !t.is_finished());
                if subscriptions.len() >= s.settings.max_subscriptions_per_connection {
                    let _ = tx
                        .send(ServerMsg::error(
                            Some(&id),
                            TOO_MANY_SUBSCRIPTIONS,
                            format!(
                                "at most {} subscriptions per connection",
                                s.settings.max_subscriptions_per_connection
                            ),
                        ))
                        .await;
                    continue;
                }
                debug!(conn, id, sql, "subscribe");
                let entry = Arc::new(Entry {
                    sql: sql.clone(),
                    params: params.clone(),
                    watermark: Mutex::new(None),
                    resumed: AtomicBool::new(false),
                    rows_sent: AtomicU64::new(0),
                });
                s.registry.insert(conn, &id, entry.clone());
                let task = tokio::spawn(run(
                    s.engine.clone(),
                    s.registry.clone(),
                    Request {
                        conn,
                        id: id.clone(),
                        sql,
                        since,
                        params,
                        progress_interval: s.settings.progress_interval,
                    },
                    entry,
                    tx.clone(),
                ));
                subscriptions.insert(id, task);
            }
            ClientMsg::Unsubscribe { id } => {
                debug!(conn, id, "unsubscribe");
                if let Some(task) = subscriptions.remove(&id) {
                    task.abort();
                }
                s.registry.remove(conn, &id);
            }
        }
    }

    for task in subscriptions.into_values() {
        task.abort();
    }
    s.registry.remove_conn(conn);
    drop(tx);
    let _ = writer.await;
    info!(conn, "disconnected");
}

/// One subscription: subscribe, send the snapshot, then a diff per batch of ready iterations.
/// What a client asked for.
struct Request {
    conn: u64,
    id: String,
    sql: String,
    since: Option<String>,
    params: chlive_protocol::Params,
    progress_interval: Duration,
}

async fn run(
    engine: Arc<Engine>,
    registry: Arc<Registry>,
    Request {
        conn,
        id,
        sql,
        since,
        params,
        progress_interval,
    }: Request,
    entry: Arc<Entry>,
    tx: mpsc::Sender<ServerMsg>,
) {
    let subscription = match engine.subscribe_with(&sql, &params, since.as_deref()).await {
        Ok(s) => s,
        Err(e) => {
            debug!(conn, id, error = %e, "subscribe failed");
            registry.remove(conn, &id);
            let _ = tx.send(engine_error(&id, &e)).await;
            return;
        }
    };
    *entry.watermark.lock().unwrap() = Some(subscription.watermark.describe());
    entry.resumed.store(subscription.resumed, Ordering::Relaxed);
    let plan = subscription.plan.clone();
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
    let mut columns = Some(columns);
    if subscription.resumed {
        let resume = ServerMsg::Resume {
            id: id.clone(),
            columns: columns.take().unwrap_or_default(),
            order: order.clone(),
            limit,
        };
        if tx.send(resume).await.is_err() {
            return;
        }
    }

    // The cursor last sent, and when a `progress` last went out.
    let mut sent_at: Option<Watermark> = None;
    let mut last_progress = Instant::now() - progress_interval;
    let mut batches = subscription.ready_chunks(MAX_ITERATIONS_PER_MESSAGE);
    while let Some(batch) = batches.next().await {
        let mut changes: Vec<Change> = Vec::new();
        let mut out = Vec::new();
        let mut at: Option<Watermark> = None;
        let mut catch_up = false;
        let mut failed = false;
        for iteration in batch {
            match iteration {
                Ok(it) if it.origin == Origin::History => {
                    let ids: Vec<String> = it
                        .diffs
                        .iter()
                        .filter_map(|d| match d {
                            chlive_engine::diff::Diff::Insert { id, .. } => id.clone(),
                            _ => None,
                        })
                        .collect();
                    out.push(ServerMsg::Snapshot {
                        id: id.clone(),
                        columns: columns.take().unwrap_or_default(),
                        // the history, already in order
                        rows: it
                            .diffs
                            .into_iter()
                            .filter_map(|d| match d {
                                chlive_engine::diff::Diff::Insert { row, .. } => Some(row),
                                _ => None,
                            })
                            .collect(),
                        order: order.clone(),
                        limit,
                        offset,
                        at: cursor::encode(&plan, &it.at),
                        ids: plan.aggregate.is_some().then_some(ids),
                    });
                    sent_at = Some(it.at);
                }
                Ok(it) => {
                    catch_up |= it.origin == Origin::CatchUp;
                    changes.extend(it.diffs.into_iter().map(change));
                    at = Some(it.at);
                }
                Err(e) => {
                    warn!(conn, id, error = %e, "subscription failed");
                    if let (false, Some(a)) = (changes.is_empty(), &at) {
                        out.push(ServerMsg::Diff {
                            id: id.clone(),
                            changes: std::mem::take(&mut changes),
                            at: cursor::encode(&plan, a),
                        });
                    }
                    out.push(engine_error(&id, &e));
                    failed = true;
                    break;
                }
            }
        }
        if let (false, Some(a)) = (failed, at) {
            if !changes.is_empty() {
                out.push(ServerMsg::Diff {
                    id: id.clone(),
                    changes,
                    at: cursor::encode(&plan, &a),
                });
                sent_at = Some(a);
            } else if sent_at.as_ref() != Some(&a)
                && (catch_up || last_progress.elapsed() >= progress_interval)
            {
                // only non-matching events: tell the client how far it is anyway, now and then
                out.push(ServerMsg::Progress {
                    id: id.clone(),
                    at: cursor::encode(&plan, &a),
                });
                last_progress = Instant::now();
                sent_at = Some(a);
            }
        }
        for msg in out {
            let rows = match &msg {
                ServerMsg::Snapshot { rows, .. } => rows.len(),
                ServerMsg::Diff { changes, .. } => changes.len(),
                _ => 0,
            };
            entry.rows_sent.fetch_add(rows as u64, Ordering::Relaxed);
            if tx.send(msg).await.is_err() {
                return; // the connection is gone
            }
        }
        if failed {
            break;
        }
    }
    registry.remove(conn, &id);
}
