//! `/live` (the WebSocket protocol, see [`crate::protocol`]), `/debug/subscriptions` and
//! `/healthz`.
//!
//! Each connection has a writer task fed by a bounded channel, and one task per subscription
//! that runs an engine [`Subscription`](chlive_engine::Subscription) and forwards its
//! iterations. Unsubscribing or disconnecting aborts the task, which drops the subscription and
//! with it its Kafka consumer. A slow client fills the channel, which pauses its subscriptions.

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::State;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Json, Router};
use chlive_engine::{Engine, Origin};
use futures::{SinkExt, StreamExt};
use serde::Serialize;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{debug, info, warn};

use crate::protocol::{Change, ClientMsg, Column, OrderTerm, ServerMsg, BAD_MESSAGE, DUPLICATE_ID};

/// Iterations already waiting are sent together, in one `diff`, up to this many.
const MAX_ITERATIONS_PER_MESSAGE: usize = 256;
/// Messages buffered per connection before its subscriptions pause.
const OUTBOX: usize = 256;

#[derive(Clone)]
struct AppState {
    engine: Arc<Engine>,
    registry: Arc<Registry>,
    next_conn: Arc<AtomicU64>,
}

pub fn router(engine: Arc<Engine>) -> Router {
    let state = AppState {
        engine,
        registry: Arc::default(),
        next_conn: Arc::new(AtomicU64::new(1)),
    };
    Router::new()
        .route("/live", get(live))
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
    /// Set once subscribed: where history ends and the stream begins.
    watermark: Mutex<Option<String>>,
    rows_sent: AtomicU64,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct DebugEntry {
    conn: u64,
    id: String,
    sql: String,
    watermark: Option<String>,
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
                watermark: e.watermark.lock().unwrap().clone(),
                rows_sent: e.rows_sent.load(Ordering::Relaxed),
            })
            .collect()
    }
}

async fn debug_subscriptions(State(s): State<AppState>) -> impl IntoResponse {
    Json(s.registry.list())
}

async fn live(ws: WebSocketUpgrade, State(s): State<AppState>) -> impl IntoResponse {
    let conn = s.next_conn.fetch_add(1, Ordering::Relaxed);
    ws.on_upgrade(move |socket| connection(socket, s, conn))
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
            ClientMsg::Subscribe { id, sql } => {
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
                debug!(conn, id, sql, "subscribe");
                let entry = Arc::new(Entry {
                    sql: sql.clone(),
                    watermark: Mutex::new(None),
                    rows_sent: AtomicU64::new(0),
                });
                s.registry.insert(conn, &id, entry.clone());
                let task = tokio::spawn(run(
                    s.engine.clone(),
                    s.registry.clone(),
                    conn,
                    id.clone(),
                    sql,
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
async fn run(
    engine: Arc<Engine>,
    registry: Arc<Registry>,
    conn: u64,
    id: String,
    sql: String,
    entry: Arc<Entry>,
    tx: mpsc::Sender<ServerMsg>,
) {
    let subscription = match engine.subscribe(&sql).await {
        Ok(s) => s,
        Err(e) => {
            debug!(conn, id, error = %e, "subscribe failed");
            registry.remove(conn, &id);
            let _ = tx.send(ServerMsg::engine_error(&id, &e)).await;
            return;
        }
    };
    *entry.watermark.lock().unwrap() = Some(subscription.watermark.describe());
    let columns: Vec<Column> = subscription
        .plan
        .columns
        .iter()
        .map(|c| Column {
            name: c.name.clone(),
            ty: c.ty.clickhouse.clone(),
        })
        .collect();
    let mut columns = Some(columns);
    let order = subscription.plan.stream.order.as_ref().map(OrderTerm::of);
    let subscription_limit = subscription.plan.stream.limit;

    let mut batches = subscription.ready_chunks(MAX_ITERATIONS_PER_MESSAGE);
    while let Some(batch) = batches.next().await {
        let mut changes: Vec<Change> = Vec::new();
        let mut out = Vec::new();
        let mut failed = false;
        for iteration in batch {
            match iteration {
                Ok(it) if it.origin == Origin::History => {
                    out.push(ServerMsg::Snapshot {
                        id: id.clone(),
                        columns: columns.take().unwrap_or_default(),
                        // the history, already in order
                        rows: it
                            .diffs
                            .into_iter()
                            .filter_map(|d| match d {
                                chlive_engine::diff::Diff::Insert { row, .. } => Some(row),
                                chlive_engine::diff::Diff::Delete { .. } => None,
                            })
                            .collect(),
                        order: order.clone(),
                        limit: subscription_limit,
                    });
                }
                Ok(it) => changes.extend(it.diffs.into_iter().map(Change::from)),
                Err(e) => {
                    warn!(conn, id, error = %e, "subscription failed");
                    if !changes.is_empty() {
                        out.push(ServerMsg::Diff {
                            id: id.clone(),
                            changes: std::mem::take(&mut changes),
                        });
                    }
                    out.push(ServerMsg::engine_error(&id, &e));
                    failed = true;
                    break;
                }
            }
        }
        if !changes.is_empty() {
            out.push(ServerMsg::Diff {
                id: id.clone(),
                changes,
            });
        }
        for msg in out {
            let rows = match &msg {
                ServerMsg::Snapshot { rows, .. } => rows.len(),
                ServerMsg::Diff { changes, .. } => changes.len(),
                ServerMsg::Error { .. } => 0,
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
