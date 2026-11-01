//! A Rust client for chlive: live ClickHouse queries over WebSocket, for services.
//!
//! One [`LiveClient`] is one WebSocket shared by any number of live queries. A [`Query`] is
//! subscribed to through the view that fits what the service wants to know:
//!
//! | View | What it gives |
//! | --- | --- |
//! | [`raw`](Query::raw) | every [`Update`]: the snapshot, each diff with the rows after it and what changed |
//! | [`rows`](Query::rows) | the whole result after every change |
//! | [`first`](Query::first) / [`last`](Query::last) | one row, kept current ([`Live`]) |
//! | [`cell`](Query::cell) | the first column of the first row, kept current ([`Live`]) |
//! | [`exists`](Query::exists) | resolves once the result has a row |
//! | [`inserts`](Query::inserts) / [`deletes`](Query::deletes) | each row entering or leaving the result ([`Events`]) |
//! | [`history`](Query::history) | the result now, and each row inserted after it |
//! | [`fetch`](Query::fetch), [`fetch_one`](Query::fetch_one), [`fetch_cell`](Query::fetch_cell) | the result now, once |
//!
//! ```no_run
//! # async fn run() -> Result<(), chlive_client::LiveError> {
//! use chlive_client::{ClientOptions, LiveClient};
//! use futures::StreamExt;
//!
//! #[derive(serde::Deserialize)]
//! struct Event { ts: String, kind: String }
//!
//! let client = LiveClient::new(ClientOptions::new("ws://127.0.0.1:8080/live"));
//! let session = "4a7c…";
//! let events = client
//!     .query("select ts, kind from events where session = {id:String} order by ts")
//!     .param("id", session);
//!
//! // the latest event, whenever it changes (a slow reader skips the ones in between)
//! let mut latest = events.clone().last::<Event>();
//! // how many there are
//! let mut count = client.query("select count() from events").cell::<u64>();
//! count.wait_for(|n| n.is_some_and(|n| n >= 1000)).await?;
//!
//! // what happened so far, then every new event, each exactly once
//! let (history, mut new) = events.history::<Event>().await?;
//! for e in history.iter() { println!("was: {}", e.kind); }
//! while let Some(e) = new.next().await { println!("new: {}", e?.kind); }
//! # let _ = latest.changed().await;
//! # Ok(()) }
//! ```
//!
//! [`Live`] values run on a task of their own, so a slow reader only misses values in between.
//! [`raw`](Query::raw), [`rows`](Query::rows) and [`Events`] are read as they're polled: one
//! that isn't read holds up the connection it shares (a [`LiveClient`] of its own avoids that).
//!
//! When the connection drops, the client reconnects with exponential backoff and subscribes
//! again from each result's cursor: the server resumes it with the diffs since, or, when it
//! can't, sends a new snapshot.
//!
//! Rows are deserialized into `R` with serde; [`Row`] (a JSON object) takes them as they are.
//! The protocol itself is `chlive-protocol`.

mod conn;
mod events;
mod live;
mod query;
mod store;

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant, SystemTime};

use futures::Stream;
use serde::de::DeserializeOwned;
use tokio::sync::mpsc;

pub use chlive_protocol::{
    Column, Params, Row, ServerMsg, CATCH_UP_TIMEOUT, CLIENT_ERROR, DISCONNECTED, INVALID_INSERT,
    WRITE_FAILED,
};
pub use events::Events;
pub use live::Live;
pub use query::Query;
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
    /// Who the client is (`hello`, sent on every connect): what the server's security lets it
    /// read and insert. None: anonymous.
    pub user: Option<String>,
}

impl ClientOptions {
    pub fn new(url: impl Into<String>) -> Self {
        ClientOptions {
            url: url.into(),
            min_backoff: Duration::from_millis(500),
            max_backoff: Duration::from_secs(10),
            user: None,
        }
    }

    /// Connect as `user` (see [`ClientOptions::user`]).
    pub fn user(mut self, user: impl Into<String>) -> Self {
        self.user = Some(user.into());
        self
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
        self.subscribe_opts(sql, params, false)
    }

    /// [`subscribe_with`](Self::subscribe_with), and with `complete`, a snapshot holding everything
    /// in Kafka when subscribing ([`Query::complete`]).
    pub(crate) fn subscribe_opts<R: DeserializeOwned>(
        &self,
        sql: impl Into<String>,
        params: Params,
        complete: bool,
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
                complete,
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

    /// Insert rows into a table (each serialized as a JSON object by column name, as queries
    /// name them): each row's Kafka partition and offset once the server has produced it. The
    /// server checks them against the user's write permissions and the table's columns; an error
    /// is `PERMISSION_DENIED`, `INVALID_INSERT` (nothing went in), `WRITE_FAILED`, or
    /// `DISCONNECTED` (the connection dropped before the answer: the rows may or may not have
    /// gone in). Sent once connected; an update or delete is an insert by the table's own
    /// convention (e.g. a new version in a ReplacingMergeTree).
    pub async fn insert<T: serde::Serialize>(
        &self,
        table: impl Into<String>,
        rows: impl IntoIterator<Item = T>,
    ) -> Result<Ack, LiveError> {
        let rows = rows
            .into_iter()
            .map(|r| match serde_json::to_value(r) {
                Ok(serde_json::Value::Object(row)) => Ok(row),
                Ok(other) => Err(format!("a row is a JSON object, not {other}")),
                Err(e) => Err(format!("a row doesn't serialize: {e}")),
            })
            .collect::<Result<Vec<Row>, _>>()
            .map_err(|message| LiveError {
                code: CLIENT_ERROR.into(),
                message,
            })?;
        let n = self
            .next_id
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let (done, answer) = tokio::sync::oneshot::channel();
        let insert = Cmd::Insert {
            id: format!("w{n}"),
            table: table.into(),
            rows,
            done,
        };
        if self.cmds.send(insert).is_err() {
            return Err(query::closed());
        }
        answer.await.unwrap_or_else(|_| Err(query::closed()))
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

/// An insert went in: each row's Kafka `(partition, offset)`, in order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Ack {
    pub offsets: Vec<(i32, i64)>,
}

/// Why a subscription failed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveError {
    /// `SYNTAX_ERROR`, `INVALID_QUERY` (both with a line:column in the message),
    /// `HISTORY_FAILED`, `STREAM_FAILED`, `AGGREGATE_FAILED`, `SNAPSHOT_FAILED`,
    /// `TOO_MANY_SUBSCRIPTIONS` (the server's limit per connection), `CATCH_UP_TIMEOUT` (a
    /// [`complete`](Query::complete) snapshot couldn't be had in time), or `CLIENT_ERROR` (a
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
    /// The whole result: first, and again after a reconnect the server couldn't resume (it
    /// replaces the rows).
    Snapshot,
    /// Reconnected and resumed from the cursor: the rows are still valid, and the diffs that
    /// follow bring them up to date.
    Resumed,
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
    /// At `index` with ORDER BY, appended without. Aggregated and FINAL rows have an `id`, their
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
                    // inserts' answers go to them, never to a subscription
                    ServerMsg::Ack { .. } => None,
                    ServerMsg::Error { code, message, .. } => {
                        return Some(self.fail(&code, message));
                    }
                    ServerMsg::Resume { columns, .. } => {
                        self.columns = columns.into();
                        self.status = Status::Live;
                        Some(Kind::Resumed)
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
