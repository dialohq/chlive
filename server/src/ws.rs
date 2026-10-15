//! `/live` (the WebSocket protocol, see [`crate::protocol`]), `/healthz`, and for debugging
//! `/debug` (a page showing the server's state live), `/debug/state` (that state, as JSON) and
//! `/debug/subscriptions` (the shared entries).
//!
//! Each connection has a writer task fed by a bounded channel. A subscription joins the shared
//! entry of its query (see [`crate::hub`]), which sends it the result and its changes; a client
//! too slow to take them falls behind and catches up later, without holding anyone else up.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
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
    /// The open connections, for `/debug/state`.
    conns: Arc<Mutex<HashMap<u64, ConnInfo>>>,
    /// Unix milliseconds.
    started_at: u64,
}

/// An open connection, as `/debug/state` shows it.
struct ConnInfo {
    origin: Option<String>,
    connected_at: u64,
    /// Weak: the writer ends when the connection drops its sender.
    outbox: mpsc::WeakSender<Outgoing>,
    /// Its subscriptions: id, and the entry's key (as listed).
    subscriptions: Vec<(String, String)>,
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
        conns: Arc::default(),
        started_at: crate::store::now_ms() as u64,
    };
    Router::new()
        .route(&path, get(live))
        .route("/debug", get(debug_page))
        .route("/debug/state", get(debug_state))
        .route("/debug/subscriptions", get(debug_subscriptions))
        .route("/healthz", get(|| async { "ok" }))
        .with_state(state)
}

/// The shared entries, with their subscribers.
async fn debug_subscriptions(State(s): State<AppState>) -> impl IntoResponse {
    Json(s.hub.list().await)
}

/// A page showing `/debug/state`, refreshed every second.
async fn debug_page() -> impl IntoResponse {
    (
        [(header::CONTENT_TYPE, "text/html; charset=utf-8")],
        include_str!("debug.html"),
    )
}

/// The whole server: its entries, disk store, snapshots and connections.
async fn debug_state(State(s): State<AppState>) -> impl IntoResponse {
    let (mut state, snapshots) = tokio::join!(s.hub.state(), s.hub.engine().snapshot_status());
    let mut conns: Vec<serde_json::Value> = s
        .conns
        .lock()
        .unwrap()
        .iter()
        .map(|(conn, c)| {
            serde_json::json!({
                "conn": conn,
                "origin": c.origin,
                "connectedAt": c.connected_at,
                "outbox": c.outbox.upgrade().map(|tx| OUTBOX - tx.capacity()),
                "subscriptions": c.subscriptions.iter()
                    .map(|(id, key)| serde_json::json!({"id": id, "key": key}))
                    .collect::<Vec<_>>(),
            })
        })
        .collect();
    conns.sort_by_key(|c| c["conn"].as_u64());
    state["server"] = serde_json::json!({
        "version": env!("CARGO_PKG_VERSION"),
        "startedAt": s.started_at,
        "now": crate::store::now_ms(),
        "path": s.settings.path,
        "connections": s.connections.load(Ordering::Relaxed),
        "maxConnections": s.settings.max_connections,
        "outbox": OUTBOX,
    });
    state["snapshots"] = serde_json::to_value(snapshots).expect("statuses serialize");
    state["connections"] = conns.into();
    Json(state)
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
    let origin = origin.map(str::to_string);
    ws.on_upgrade(move |socket| async move {
        connection(socket, s, conn, origin).await;
        drop(slot);
    })
}

async fn connection(socket: WebSocket, s: AppState, conn: u64, origin: Option<String>) {
    info!(conn, "connected");
    let (mut sink, mut incoming) = socket.split();
    let (tx, mut rx) = mpsc::channel::<Outgoing>(OUTBOX);
    s.conns.lock().unwrap().insert(
        conn,
        ConnInfo {
            origin,
            connected_at: crate::store::now_ms() as u64,
            outbox: tx.downgrade(),
            subscriptions: Vec::new(),
        },
    );
    let listed = |subscriptions: &HashMap<String, Joined>| {
        if let Some(c) = s.conns.lock().unwrap().get_mut(&conn) {
            c.subscriptions = subscriptions
                .iter()
                .map(|(id, j)| (id.clone(), crate::store::short(&j.key)))
                .collect();
            c.subscriptions.sort();
        }
    };
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
                listed(&subscriptions);
            }
            ClientMsg::Unsubscribe { id } => {
                debug!(conn, id, "unsubscribe");
                if let Some(j) = subscriptions.remove(&id) {
                    s.hub.leave(&j.key, j.sub);
                }
                listed(&subscriptions);
            }
        }
    }

    s.conns.lock().unwrap().remove(&conn);
    for j in subscriptions.into_values() {
        s.hub.leave(&j.key, j.sub);
    }
    drop(tx);
    let _ = writer.await;
    info!(conn, "disconnected");
}
