//! WebSocket protocol (`/live`) and the debug endpoint.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::State;
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Json, Router};
use futures::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tracing::{debug, info};

use crate::ch_http::Column;
use crate::diff::Row;
use crate::subscriptions::{ConnId, Manager};

#[derive(Debug, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum ClientMsg {
    Subscribe {
        id: String,
        sql: String,
        #[serde(default)]
        params: serde_json::Map<String, serde_json::Value>,
    },
    Unsubscribe {
        id: String,
    },
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum ServerMsg {
    #[serde(rename_all = "camelCase")]
    Snapshot {
        id: String,
        columns: Vec<Column>,
        key_columns: Vec<String>,
        rows: Vec<Row>,
        #[serde(skip_serializing_if = "Option::is_none")]
        order: Option<Vec<String>>,
    },
    Patch {
        id: String,
        upserts: Vec<Row>,
        deletes: Vec<String>,
        #[serde(skip_serializing_if = "Option::is_none")]
        order: Option<Vec<String>>,
    },
    Error {
        #[serde(skip_serializing_if = "Option::is_none")]
        id: Option<String>,
        code: String,
        message: String,
    },
}

#[derive(Clone)]
pub struct AppState {
    pub manager: Arc<Manager>,
    next_conn: Arc<AtomicU64>,
}

pub fn router(manager: Arc<Manager>) -> Router {
    let state = AppState { manager, next_conn: Arc::new(AtomicU64::new(1)) };
    Router::new()
        .route("/live", get(live))
        .route("/debug/subscriptions", get(debug_subs))
        .route("/healthz", get(|| async { "ok" }))
        .with_state(state)
}

async fn debug_subs(State(s): State<AppState>) -> impl IntoResponse {
    Json(s.manager.debug().await)
}

async fn live(ws: WebSocketUpgrade, State(s): State<AppState>) -> impl IntoResponse {
    let conn = s.next_conn.fetch_add(1, Ordering::Relaxed);
    ws.on_upgrade(move |socket| connection(socket, conn, s.manager))
}

async fn connection(socket: WebSocket, conn: ConnId, manager: Arc<Manager>) {
    info!(conn, "connected");
    let (mut sink, mut stream) = socket.split();
    let (tx, mut rx) = mpsc::unbounded_channel::<ServerMsg>();
    let writer = tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            let text = serde_json::to_string(&msg).expect("serializable");
            if sink.send(Message::Text(text.into())).await.is_err() {
                break;
            }
        }
    });

    // client subscription id → dedupe hash; `None` while the subscribe is in flight.
    let subs: Arc<tokio::sync::Mutex<HashMap<String, Option<String>>>> = Default::default();
    let mut tasks = tokio::task::JoinSet::new();
    while let Some(Ok(msg)) = stream.next().await {
        let text = match msg {
            Message::Text(t) => t,
            Message::Close(_) => break,
            _ => continue,
        };
        let parsed: ClientMsg = match serde_json::from_str(&text) {
            Ok(m) => m,
            Err(e) => {
                let _ = tx.send(ServerMsg::Error { id: None, code: "BAD_MESSAGE".into(), message: e.to_string() });
                continue;
            }
        };
        match parsed {
            ClientMsg::Subscribe { id, sql, params } => {
                {
                    let mut s = subs.lock().await;
                    if s.contains_key(&id) {
                        let _ = tx.send(ServerMsg::Error {
                            id: Some(id),
                            code: "DUPLICATE_ID".into(),
                            message: "subscription id already in use on this connection".into(),
                        });
                        continue;
                    }
                    s.insert(id.clone(), None);
                }
                debug!(conn, id, sql, "subscribe");
                // Snapshots can take a while; don't block other messages on this socket.
                let (manager, tx, subs) = (manager.clone(), tx.clone(), subs.clone());
                tasks.spawn(async move {
                    match manager.subscribe(conn, id.clone(), &sql, &params, tx.clone()).await {
                        Ok(hash) => {
                            let mut s = subs.lock().await;
                            match s.get_mut(&id) {
                                Some(slot) => *slot = Some(hash),
                                // unsubscribed while the snapshot was being built
                                None => {
                                    drop(s);
                                    manager.unsubscribe(conn, &id, &hash).await;
                                }
                            }
                        }
                        Err(e) => {
                            subs.lock().await.remove(&id);
                            let _ = tx.send(ServerMsg::Error { id: Some(id), code: e.code.into(), message: e.message });
                        }
                    }
                });
            }
            ClientMsg::Unsubscribe { id } => {
                let entry = subs.lock().await.remove(&id);
                if let Some(Some(hash)) = entry {
                    manager.unsubscribe(conn, &id, &hash).await;
                }
            }
        }
    }
    // Let in-flight subscribes settle so they can be released below.
    while tasks.join_next().await.is_some() {}
    let remaining: Vec<(String, Option<String>)> = subs.lock().await.drain().collect();
    for (id, hash) in remaining {
        if let Some(hash) = hash {
            manager.unsubscribe(conn, &id, &hash).await;
        }
    }
    writer.abort();
    info!(conn, "disconnected");
}
