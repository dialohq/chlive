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
use chlive_engine::watermark::Watermark;
use chlive_engine::{Engine, EngineError, Filters, Params, Plan};
use futures::{SinkExt, StreamExt};
use tokio::sync::mpsc;
use tokio::task::AbortHandle;
use tracing::{debug, info, warn};

use crate::hub::{CacheSettings, Hub, Joined, Outgoing};
use crate::protocol::{
    engine_error, ClientMsg, ServerMsg, BAD_MESSAGE, DUPLICATE_ID, PERMISSION_DENIED,
    TOO_MANY_SUBSCRIPTIONS,
};
use crate::security::{Decision, Security, TableAccess};

/// How long a subscribe waits for a permission provider that's still loading.
const PERMISSIONS_WAIT: Duration = Duration::from_secs(10);

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
    /// A `complete` subscription (its snapshot holds everything in Kafka when subscribing) waits
    /// at most this long for the result to get there; then it gets `CATCH_UP_TIMEOUT`.
    pub complete_wait: Duration,
    /// Shared results: how long they're kept, and their change logs (the `[cache]` section).
    pub cache: CacheSettings,
    /// Who may read which rows (the `[security]` section, its providers started).
    pub security: Arc<Security>,
}

impl Default for ServerSettings {
    fn default() -> Self {
        Self {
            path: "/live".into(),
            allowed_origins: Vec::new(),
            max_connections: 10_000,
            max_subscriptions_per_connection: 100,
            progress_interval: Duration::from_secs(1),
            complete_wait: Duration::from_secs(30),
            cache: CacheSettings::default(),
            security: Arc::new(Security::open()),
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
    /// Its user (`hello`).
    user: Option<String>,
    /// Its subscriptions: id, and the entry's key (as listed).
    subscriptions: Vec<(String, String)>,
}

/// The routes, over a new hub of shared results.
pub fn router(engine: Arc<Engine>, settings: ServerSettings) -> Router {
    let hub = Hub::new(
        engine,
        settings.cache.clone(),
        settings.progress_interval,
        settings.complete_wait,
    );
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
                "user": c.user,
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
            user: None,
            subscriptions: Vec::new(),
        },
    );
    let listed = |subscriptions: &HashMap<String, Live>| {
        if let Some(c) = s.conns.lock().unwrap().get_mut(&conn) {
            c.subscriptions = subscriptions
                .iter()
                .map(|(id, l)| (id.clone(), crate::store::short(&l.joined.key)))
                .collect();
            c.subscriptions.sort();
        }
    };
    let writer = tokio::spawn(async move {
        while let Some(out) = rx.recv().await {
            let Some(text) = out.render() else {
                continue;
            };
            if sink.send(Message::Text(text.into())).await.is_err() {
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

    let mut subscriptions: HashMap<String, Live> = HashMap::new();
    let mut user: Option<String> = None;
    // `hello` only comes first
    let mut greeted = false;
    // a subscription whose permissions changed, by id
    let (changed_tx, mut changed_rx) = mpsc::unbounded_channel::<String>();
    loop {
        let msg = tokio::select! {
            msg = incoming.next() => msg,
            Some(id) = changed_rx.recv() => {
                refilter(&s, conn, &id, &mut subscriptions, &tx).await;
                listed(&subscriptions);
                continue;
            }
        };
        let Some(Ok(msg)) = msg else {
            break;
        };
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
            ClientMsg::Hello { user: who } => {
                if greeted {
                    send(ServerMsg::error(
                        None,
                        PERMISSION_DENIED,
                        "hello comes first, once",
                    ))
                    .await;
                    continue;
                }
                greeted = true;
                debug!(conn, user = who, "hello");
                if let Some(c) = s.conns.lock().unwrap().get_mut(&conn) {
                    c.user = who.clone();
                }
                user = who;
            }
            ClientMsg::Subscribe {
                id,
                sql,
                since,
                params,
                complete,
            } => {
                greeted = true;
                // a subscription whose entry failed is over: its id can be used again
                subscriptions.retain(|_, l| l.joined.alive.load(Ordering::Relaxed));
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
                debug!(conn, id, sql, complete, "subscribe");
                let mut access = match permitted(&s.settings.security, user.as_deref(), &sql) {
                    Ok(a) => a,
                    Err(e) => {
                        debug!(conn, id, error = %e, "subscribe denied");
                        send(ServerMsg::error(Some(&id), PERMISSION_DENIED, e)).await;
                        continue;
                    }
                };
                let mut filters = Filters::new();
                let mut denied = None;
                for (table, a) in &mut access {
                    match tokio::time::timeout(PERMISSIONS_WAIT, a.decide()).await {
                        Ok(Decision::Filter(f)) => {
                            if !f.is_open() {
                                filters.insert(table.clone(), f);
                            }
                        }
                        Ok(Decision::Denied(e)) => {
                            denied = Some(e);
                            break;
                        }
                        Ok(Decision::Loading) | Err(_) => {
                            denied = Some(format!(
                                "the permissions for {table} aren't loaded yet; try again"
                            ));
                            break;
                        }
                    }
                }
                if let Some(e) = denied {
                    debug!(conn, id, error = %e, "subscribe denied");
                    send(ServerMsg::error(Some(&id), PERMISSION_DENIED, e)).await;
                    continue;
                }
                let plan = match s.hub.engine().explain_with(&sql, &params, &filters).await {
                    Ok(plan) => plan,
                    Err(e) => {
                        debug!(conn, id, error = %e, "subscribe failed");
                        send(engine_error(&id, &e)).await;
                        continue;
                    }
                };
                // complete: its snapshot covers Kafka as it is now
                let target = match target(&s, &plan, complete).await {
                    Ok(target) => target,
                    Err(e) => {
                        debug!(conn, id, error = %e, "subscribe failed");
                        send(engine_error(&id, &e)).await;
                        continue;
                    }
                };
                let joined = s.hub.join(
                    &plan,
                    &sql,
                    &params,
                    &filters,
                    conn,
                    &id,
                    tx.clone(),
                    since,
                    target,
                );
                let watchers = access
                    .iter()
                    .flat_map(|(_, a)| a.watches())
                    .map(|mut granted| {
                        let (changed, id) = (changed_tx.clone(), id.clone());
                        tokio::spawn(async move {
                            while granted.changed().await.is_ok() {
                                if changed.send(id.clone()).is_err() {
                                    break;
                                }
                            }
                        })
                        .abort_handle()
                    })
                    .collect();
                subscriptions.insert(
                    id,
                    Live {
                        joined,
                        sql,
                        params,
                        complete,
                        access,
                        filters,
                        watchers,
                    },
                );
                listed(&subscriptions);
            }
            ClientMsg::Unsubscribe { id } => {
                debug!(conn, id, "unsubscribe");
                if let Some(l) = subscriptions.remove(&id) {
                    s.hub.leave(&l.joined.key, l.joined.sub);
                }
                listed(&subscriptions);
            }
        }
    }

    s.conns.lock().unwrap().remove(&conn);
    for l in subscriptions.into_values() {
        s.hub.leave(&l.joined.key, l.joined.sub);
    }
    drop(tx);
    let _ = writer.await;
    info!(conn, "disconnected");
}

/// A running subscription: its entry, and what it's allowed to read.
struct Live {
    joined: Joined,
    sql: String,
    params: Params,
    /// Its snapshots cover Kafka as it was when they were asked for.
    complete: bool,
    /// Per table it reads, its access (watched while it changes).
    access: Vec<(String, TableAccess)>,
    /// The filters its entry was planned with.
    filters: Filters,
    /// The tasks telling the connection that a table's permissions changed.
    watchers: Vec<AbortHandle>,
}

impl Drop for Live {
    fn drop(&mut self) {
        for w in &self.watchers {
            w.abort();
        }
    }
}

/// A complete subscription's target: the latest offsets of the plan's topics now.
async fn target(
    s: &AppState,
    plan: &Plan,
    complete: bool,
) -> Result<Option<Watermark>, EngineError> {
    match complete {
        false => Ok(None),
        true => s.hub.engine().latest(plan).await.map(Some),
    }
}

/// What `user` may read of each table the query reads; an error names one it may not. A query
/// that doesn't parse reads nothing here (planning reports it).
fn permitted(
    security: &Security,
    user: Option<&str>,
    sql: &str,
) -> Result<Vec<(String, TableAccess)>, String> {
    let tables: Vec<String> = match chlive_engine::parser::parse(sql) {
        Ok(q) => q.tables().into_iter().map(str::to_string).collect(),
        Err(_) => Vec::new(),
    };
    tables
        .into_iter()
        .map(|t| security.access(user, &t).map(|a| (t, a)))
        .collect()
}

/// A subscription's permissions changed: when its filters did, it moves to the entry of its
/// query with the new ones (a new snapshot replaces its result), or ends with
/// `PERMISSION_DENIED` when it lost a table, or a column it names; nothing more comes from the
/// old entry either way.
async fn refilter(
    s: &AppState,
    conn: u64,
    id: &str,
    subscriptions: &mut HashMap<String, Live>,
    tx: &mpsc::Sender<Outgoing>,
) {
    let Some(live) = subscriptions.get_mut(id) else {
        return;
    };
    let mut filters = Filters::new();
    let mut denied = None;
    for (table, a) in &live.access {
        match a.current() {
            Decision::Filter(f) => {
                if !f.is_open() {
                    filters.insert(table.clone(), f);
                }
            }
            Decision::Denied(e) => {
                denied = Some(e);
                break;
            }
            // reloading: it says again once loaded
            Decision::Loading => return,
        }
    }
    if denied.is_none() && filters == live.filters {
        return;
    }
    let plan = match denied {
        Some(e) => Err(ServerMsg::error(Some(id), PERMISSION_DENIED, e)),
        None => s
            .hub
            .engine()
            .explain_with(&live.sql, &live.params, &filters)
            .await
            .map_err(|e| engine_error(id, &e)),
    };
    let plan = match plan {
        Ok(plan) => match target(s, &plan, live.complete).await {
            Ok(t) => Ok((plan, t)),
            Err(e) => Err(engine_error(id, &e)),
        },
        Err(e) => Err(e),
    };
    live.joined.close();
    s.hub.leave(&live.joined.key, live.joined.sub);
    let (plan, target) = match plan {
        Ok(planned) => planned,
        Err(msg) => {
            debug!(conn, id, "permissions changed: the subscription ends");
            let _ = tx.send(Outgoing::Msg(msg)).await;
            subscriptions.remove(id);
            return;
        }
    };
    debug!(conn, id, "permissions changed: new snapshot");
    live.joined = s.hub.join(
        &plan,
        &live.sql,
        &live.params,
        &filters,
        conn,
        id,
        tx.clone(),
        None,
        target,
    );
    live.filters = filters;
}
