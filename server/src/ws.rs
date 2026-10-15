//! `/live` (the WebSocket protocol, see [`crate::protocol`]), `/debug/subscriptions` and
//! `/healthz`.
//!
//! Each connection has a writer task fed by a bounded channel. A subscription joins the shared
//! entry of its query (see [`crate::hub`]), which sends it the result and its changes; a client
//! too slow to take them falls behind and catches up later, without holding anyone else up.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::State;
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::get;
use axum::{Json, Router};
use chlive_engine::Engine;
use futures::{SinkExt, StreamExt};
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

use crate::hub::{CacheSettings, Hub, Joined, Outgoing};
use crate::protocol::{
    engine_error, ClientMsg, ServerMsg, BAD_MESSAGE, DUPLICATE_ID, TOO_MANY_SUBSCRIPTIONS,
};

/// Messages buffered per connection; a subscription whose messages don't fit falls behind.
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
    /// Shared results: how long they're kept, and their change logs (the `[cache]` section).
    pub cache: CacheSettings,
}

impl Default for ServerSettings {
    fn default() -> Self {
        Self {
            path: "/live".into(),
            allowed_origins: Vec::new(),
            max_connections: 10_000,
            max_subscriptions_per_connection: 100,
            progress_interval: Duration::from_secs(1),
            cache: CacheSettings::default(),
        }
    }
}

#[derive(Clone)]
struct AppState {
    hub: Arc<Hub>,
    settings: Arc<ServerSettings>,
    next_conn: Arc<AtomicU64>,
    connections: Arc<AtomicUsize>,
}

/// The routes, over a new hub of shared results.
pub fn router(engine: Arc<Engine>, settings: ServerSettings) -> Router {
    let hub = Hub::new(engine, settings.cache.clone(), settings.progress_interval);
    router_with(hub, settings)
}

/// The routes, over this hub.
pub fn router_with(hub: Arc<Hub>, settings: ServerSettings) -> Router {
    let path = settings.path.clone();
    let state = AppState {
        hub,
        settings: Arc::new(settings),
        next_conn: Arc::new(AtomicU64::new(1)),
        connections: Arc::default(),
    };
    Router::new()
        .route(&path, get(live))
        .route("/debug/subscriptions", get(debug_subscriptions))
        .route("/healthz", get(|| async { "ok" }))
        .with_state(state)
}

/// The shared entries, with their subscribers.
async fn debug_subscriptions(State(s): State<AppState>) -> impl IntoResponse {
    Json(s.hub.list().await)
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
    let (tx, mut rx) = mpsc::channel::<Outgoing>(OUTBOX);
    let writer = tokio::spawn(async move {
        while let Some(out) = rx.recv().await {
            if sink.send(Message::Text(out.render().into())).await.is_err() {
                break;
            }
        }
    });
    let send = |msg: ServerMsg| {
        let tx = tx.clone();
        async move {
            let _ = tx.send(Outgoing::Msg(msg)).await;
        }
    };

    let mut subscriptions: HashMap<String, Joined> = HashMap::new();
    while let Some(Ok(msg)) = incoming.next().await {
        let text = match msg {
            Message::Text(t) => t,
            Message::Close(_) => break,
            _ => continue,
        };
        let msg = match serde_json::from_str::<ClientMsg>(&text) {
            Ok(m) => m,
            Err(e) => {
                send(ServerMsg::error(
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
                // a subscription whose entry failed is over: its id can be used again
                subscriptions.retain(|_, j| j.alive.load(Ordering::Relaxed));
                if subscriptions.contains_key(&id) {
                    send(ServerMsg::error(
                        Some(&id),
                        DUPLICATE_ID,
                        format!("subscription {id} already exists on this connection"),
                    ))
                    .await;
                    continue;
                }
                if subscriptions.len() >= s.settings.max_subscriptions_per_connection {
                    send(ServerMsg::error(
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
                let plan = match s.hub.engine().explain_with(&sql, &params).await {
                    Ok(plan) => plan,
                    Err(e) => {
                        debug!(conn, id, error = %e, "subscribe failed");
                        send(engine_error(&id, &e)).await;
                        continue;
                    }
                };
                let joined = s
                    .hub
                    .join(&plan, &sql, &params, conn, &id, tx.clone(), since);
                subscriptions.insert(id, joined);
            }
            ClientMsg::Unsubscribe { id } => {
                debug!(conn, id, "unsubscribe");
                if let Some(j) = subscriptions.remove(&id) {
                    s.hub.leave(&j.key, j.sub);
                }
            }
        }
    }

    for j in subscriptions.into_values() {
        s.hub.leave(&j.key, j.sub);
    }
    drop(tx);
    let _ = writer.await;
    info!(conn, "disconnected");
}
