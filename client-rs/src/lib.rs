//! A Rust client for chlive: live ClickHouse queries over WebSocket, for services.
//!
//! One [`LiveClient`] is one WebSocket shared by any number of subscriptions. Each
//! [`Subscription`] is a [`Stream`] of [`Update`]s: the server's snapshot of the result, then
//! every diff applied to it, with the rows after it and the changes it made. When the connection
//! drops, the client reconnects with exponential backoff and subscribes again; the new snapshot
//! replaces the rows.
//!
//! ```no_run
//! # async fn run() {
//! use chlive_client::{Change, ClientOptions, LiveClient, Status};
//! use futures::StreamExt;
//!
//! #[derive(serde::Deserialize)]
//! struct Slow { user_id: String, latency_ms: u32 }
//!
//! let client = LiveClient::new(ClientOptions::new("ws://127.0.0.1:8080/live"));
//! let mut slowest = client.subscribe::<Slow>(
//!     "select user_id, latency_ms from events order by latency_ms desc limit 5",
//! );
//! while let Some(update) = slowest.next().await {
//!     if let Status::Error(e) = &update.status {
//!         eprintln!("{}: {}", e.code, e.message);
//!         break;
//!     }
//!     for change in &update.changes {
//!         if let Change::Insert { row, index, .. } = change {
//!             println!("{} is #{} ({} ms)", row.user_id, index.unwrap_or(0) + 1, row.latency_ms);
//!         }
//!     }
//! }
//! # }
//! ```
//!
//! Rows are deserialized into `R` with serde; [`Row`] (a JSON object) takes them as they are.
//! The protocol itself is `chlive-protocol`.

mod conn;
mod store;

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant, SystemTime};

use futures::Stream;
use serde::de::DeserializeOwned;
use tokio::sync::mpsc;

pub use chlive_protocol::{Column, Params, Row, ServerMsg, CLIENT_ERROR};
pub use store::Rows;

use conn::{Cmd, Event};
use store::Store;

/// How many messages a subscription may have waiting before the connection waits for its reader.
const BUFFER: usize = 64;

#[derive(Debug, Clone)]
pub struct ClientOptions {
    /// e.g. `ws://127.0.0.1:8080/live`
    pub url: String,
    /// The first reconnect delay; doubles up to `max_backoff`.
    pub min_backoff: Duration,
    pub max_backoff: Duration,
}

impl ClientOptions {
    pub fn new(url: impl Into<String>) -> Self {
        ClientOptions {
            url: url.into(),
            min_backoff: Duration::from_millis(500),
            max_backoff: Duration::from_secs(10),
        }
    }
}

/// One WebSocket shared by any number of live queries. Cheap to clone (a handle to the
/// connection's task); the connection opens with the first subscription. Needs a tokio runtime.
#[derive(Clone)]
pub struct LiveClient {
    cmds: mpsc::UnboundedSender<Cmd>,
    next_id: Arc<std::sync::atomic::AtomicU64>,
}

impl LiveClient {
    pub fn new(opts: ClientOptions) -> Self {
        LiveClient {
            cmds: conn::spawn(opts),
            next_id: Arc::new(std::sync::atomic::AtomicU64::new(1)),
        }
    }

    /// Subscribe to a query in the chlive dialect. Rows are deserialized into `R`
    /// (`subscribe::<Row>` keeps them as JSON). Dropping the subscription unsubscribes.
    pub fn subscribe<R: DeserializeOwned>(&self, sql: impl Into<String>) -> Subscription<R> {
        self.subscribe_with(sql, Params::new())
    }

    /// [`subscribe`](Self::subscribe) with values for the query's parameters (`{name:Type}`),
    /// e.g. `Params::from([("id".into(), json!(session))])`. They're sent again on every
    /// reconnect.
    pub fn subscribe_with<R: DeserializeOwned>(
        &self,
        sql: impl Into<String>,
        params: Params,
    ) -> Subscription<R> {
        let n = self
            .next_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let id = format!("s{n}");
        let (tx, rx) = mpsc::channel(BUFFER);
        let open = self
            .cmds
            .send(Cmd::Subscribe {
                id: id.clone(),
                sql: sql.into(),
                params,
                events: tx,
            })
            .is_ok();
        Subscription {
            stats: SubscriptionStats {
                id: id.clone(),
                ..Default::default()
            },
            id,
            events: rx,
            cmds: self.cmds.clone(),
            store: Store::default(),
            columns: Arc::from(Vec::new()),
            at: None,
            status: Status::Loading,
            started: false,
            done: !open,
        }
    }

    /// Close the socket and stop reconnecting; every subscription's stream ends.
    pub async fn close(&self) {
        let (tx, rx) = tokio::sync::oneshot::channel();
        if self.cmds.send(Cmd::Close(tx)).is_ok() {
            let _ = rx.await;
        }
    }
}

/// - `Loading`: nothing yet, or disconnected (the rows are kept until the next snapshot);
/// - `Live`: up to date, and following changes;
/// - `Error`: the subscription failed; its stream ends after this.
#[derive(Debug, Clone, PartialEq)]
pub enum Status {
    Loading,
    Live,
    Error(LiveError),
}

/// Why a subscription failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveError {
    /// `SYNTAX_ERROR`, `INVALID_QUERY` (both with a line:column in the message),
    /// `HISTORY_FAILED`, `STREAM_FAILED`, `AGGREGATE_FAILED`, `SNAPSHOT_FAILED`,
    /// `TOO_MANY_SUBSCRIPTIONS` (the server's limit per connection), or `CLIENT_ERROR` (a
    /// message the client couldn't apply).
    pub code: String,
    pub message: String,
}

impl std::fmt::Display for LiveError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.code, self.message)
    }
}

impl std::error::Error for LiveError {}

/// What an update is about.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Kind {
    /// The first update: subscribed, nothing yet.
    Subscribed,
    /// The whole result: first, and again after every reconnect (it replaces the rows).
    Snapshot,
    /// Changes to the result.
    Diff,
    /// The connection dropped; the rows are kept until the snapshot after reconnecting.
    Disconnected,
    /// The subscription failed (see `status`); the last update.
    Error,
}

/// One change, as applied to the result. Indexes are into the rows as the previous change of
/// the same update left them.
#[derive(Debug)]
pub enum Change<R> {
    /// At `index` with ORDER BY, appended without. Aggregated rows have an `id`, their group's
    /// key (e.g. `["PL"]`).
    Insert {
        row: Arc<R>,
        index: Option<usize>,
        id: Option<String>,
    },
    /// The row at `index` left the result (pushed out by LIMIT); `row` is the row that left.
    Delete { index: usize, row: Arc<R> },
    /// An aggregated row changed; `row` is the row now. With an `index`, it also moved there.
    Update {
        id: String,
        row: Arc<R>,
        index: Option<usize>,
    },
}

impl<R> Clone for Change<R> {
    fn clone(&self) -> Self {
        match self {
            Change::Insert { row, index, id } => Change::Insert {
                row: row.clone(),
                index: *index,
                id: id.clone(),
            },
            Change::Delete { index, row } => Change::Delete {
                index: *index,
                row: row.clone(),
            },
            Change::Update { id, row, index } => Change::Update {
                id: id.clone(),
                row: row.clone(),
                index: *index,
            },
        }
    }
}

/// A subscription's state after a message, and what the message changed.
#[derive(Debug)]
pub struct Update<R> {
    pub kind: Kind,
    pub status: Status,
    /// The whole result after this update: exactly the selected columns, in ORDER BY order.
    pub rows: Rows<R>,
    /// What changed (diffs only); applying them in order to the previous rows gives `rows`.
    pub changes: Vec<Change<R>>,
    /// The output columns and their ClickHouse types (from the snapshot).
    pub columns: Arc<[Column]>,
    /// The server's cursor: how far the result is complete (opaque).
    pub at: Option<String>,
    pub stats: SubscriptionStats,
}

/// What a subscription has received so far; for logs and debugging.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct SubscriptionStats {
    /// The id on the wire (`s1`, `s2`, …).
    pub id: String,
    /// `subscribe` messages sent: 1, plus one per reconnect.
    pub subscribes: u64,
    pub snapshots: u64,
    pub diffs: u64,
    pub inserts: u64,
    pub deletes: u64,
    /// Bytes of JSON received for this subscription.
    pub bytes: u64,
    pub last_message_at: Option<SystemTime>,
    /// How long applying the last message took.
    pub last_apply: Option<Duration>,
    /// The cursor the rows are complete to.
    pub at: Option<String>,
}

/// A live query. Its stream yields an [`Update`] for the subscription itself, every snapshot
/// and diff, disconnects and an error (the last). Messages are applied when the stream is
/// polled; until then they wait (up to a small buffer, then the connection waits too, so
/// subscriptions sharing a connection should all be read).
pub struct Subscription<R> {
    id: String,
    events: mpsc::Receiver<Event>,
    cmds: mpsc::UnboundedSender<Cmd>,
    store: Store<R>,
    columns: Arc<[Column]>,
    at: Option<String>,
    status: Status,
    stats: SubscriptionStats,
    /// Whether the first (`Subscribed`) update went out.
    started: bool,
    done: bool,
}

impl<R: DeserializeOwned> Subscription<R> {
    /// The id on the wire (`s1`, `s2`, …).
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Stop the subscription (also what dropping it does).
    pub fn unsubscribe(self) {}

    fn update(&self, kind: Kind, changes: Vec<Change<R>>) -> Update<R> {
        Update {
            kind,
            status: self.status.clone(),
            rows: self.store.rows(),
            changes,
            columns: self.columns.clone(),
            at: self.at.clone(),
            stats: self.stats.clone(),
        }
    }

    fn fail(&mut self, code: &str, message: String) -> Update<R> {
        self.status = Status::Error(LiveError {
            code: code.to_string(),
            message,
        });
        self.done = true;
        // the server is done with it (or should be): no resubscribing after a reconnect
        let _ = self.cmds.send(Cmd::Unsubscribe(self.id.clone()));
        self.update(Kind::Error, Vec::new())
    }

    /// An event from the connection, as an update (`None`: nothing to tell, e.g. progress).
    fn apply(&mut self, event: Event) -> Option<Update<R>> {
        match event {
            Event::Subscribed => {
                self.stats.subscribes += 1;
                None
            }
            Event::Disconnected => (self.status == Status::Live).then(|| {
                self.status = Status::Loading;
                self.update(Kind::Disconnected, Vec::new())
            }),
            Event::Unreadable { error, bytes } => {
                self.stats.bytes += bytes as u64;
                Some(self.fail(CLIENT_ERROR, error))
            }
            Event::Message { msg, bytes } => {
                let started = Instant::now();
                self.stats.bytes += bytes as u64;
                self.stats.last_message_at = Some(SystemTime::now());
                let update = match msg {
                    ServerMsg::Snapshot {
                        columns,
                        rows,
                        at,
                        ids,
                        ..
                    } => {
                        if let Err(e) = self.store.snapshot(rows, ids) {
                            return Some(self.fail(CLIENT_ERROR, e));
                        }
                        self.columns = columns.into();
                        self.at = Some(at);
                        self.status = Status::Live;
                        self.stats.snapshots += 1;
                        Some(Kind::Snapshot)
                    }
                    ServerMsg::Diff { changes, at, .. } => match self.store.diff(changes) {
                        Ok(applied) => {
                            self.at = Some(at);
                            self.stats.diffs += 1;
                            for c in &applied {
                                match c {
                                    Change::Insert { .. } => self.stats.inserts += 1,
                                    Change::Delete { .. } => self.stats.deletes += 1,
                                    Change::Update { .. } => {}
                                }
                            }
                            self.stats.last_apply = Some(started.elapsed());
                            self.stats.at = self.at.clone();
                            return Some(self.update(Kind::Diff, applied));
                        }
                        Err(e) => return Some(self.fail(CLIENT_ERROR, e)),
                    },
                    ServerMsg::Progress { at, .. } => {
                        self.at = Some(at);
                        None
                    }
                    ServerMsg::Error { code, message, .. } => {
                        return Some(self.fail(&code, message));
                    }
                    // never asked for: this client doesn't resume
                    ServerMsg::Resume { .. } => {
                        return Some(self.fail(CLIENT_ERROR, "unexpected resume".into()));
                    }
                };
                self.stats.last_apply = Some(started.elapsed());
                self.stats.at = self.at.clone();
                update.map(|kind| self.update(kind, Vec::new()))
            }
        }
    }
}

impl<R: DeserializeOwned> Stream for Subscription<R> {
    type Item = Update<R>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Update<R>>> {
        let this = self.get_mut();
        if !this.started {
            this.started = true;
            return Poll::Ready(Some(this.update(Kind::Subscribed, Vec::new())));
        }
        loop {
            if this.done {
                return Poll::Ready(None);
            }
            match this.events.poll_recv(cx) {
                Poll::Pending => return Poll::Pending,
                // the client was closed
                Poll::Ready(None) => {
                    this.done = true;
                    return Poll::Ready(None);
                }
                Poll::Ready(Some(event)) => {
                    if let Some(update) = this.apply(event) {
                        return Poll::Ready(Some(update));
                    }
                }
            }
        }
    }
}

// Nothing in a subscription cares about being moved (rows are behind `Arc`s).
impl<R> Unpin for Subscription<R> {}

impl<R> Drop for Subscription<R> {
    fn drop(&mut self) {
        if !self.done {
            let _ = self.cmds.send(Cmd::Unsubscribe(self.id.clone()));
        }
    }
}
