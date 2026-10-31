//! A query to subscribe to, and the views of it that don't need a task of their own: the raw
//! updates, every state of the rows, a one-off fetch and the history.

use std::sync::Arc;

use futures::{Stream, StreamExt};
use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::events::{Events, Which};
use crate::live::{self, Live};
use crate::store::parse;
use crate::{Kind, LiveClient, LiveError, Params, Row, Rows, Status, Subscription, Update};

/// A query with its parameters, not subscribed yet: each view subscribes to it on its own. Cheap
/// to clone, so one query can feed several views.
#[derive(Clone)]
pub struct Query {
    pub(crate) client: LiveClient,
    pub(crate) sql: String,
    pub(crate) params: Params,
    pub(crate) complete: bool,
}

impl LiveClient {
    /// A query in the chlive dialect, to subscribe to through one of [`Query`]'s views.
    pub fn query(&self, sql: impl Into<String>) -> Query {
        Query {
            client: self.clone(),
            sql: sql.into(),
            params: Params::new(),
            complete: false,
        }
    }
}

pub(crate) fn closed() -> LiveError {
    LiveError {
        code: crate::CLIENT_ERROR.into(),
        message: "the client was closed".into(),
    }
}

pub(crate) fn error_of<R>(u: &Update<R>) -> LiveError {
    match &u.status {
        Status::Error(e) => e.clone(),
        _ => closed(),
    }
}

pub(crate) fn unreadable(e: String) -> LiveError {
    LiveError {
        code: crate::CLIENT_ERROR.into(),
        message: e,
    }
}

/// The first column of `row`, read as `T`; a string that isn't a `T` is read for what it holds,
/// so a `UInt64` (which ClickHouse may quote) reads as a `u64` either way.
pub(crate) fn read_cell<T: DeserializeOwned>(value: &serde_json::Value) -> Result<T, String> {
    serde_json::from_value(value.clone())
        .or_else(|e| match value {
            serde_json::Value::String(s) => serde_json::from_str(s).map_err(|_| e.to_string()),
            _ => Err(e.to_string()),
        })
        .map_err(|e| format!("a value doesn't fit: {e}"))
}

pub(crate) fn first_cell(row: &Row) -> Option<&serde_json::Value> {
    row.values().next()
}

impl Query {
    /// A value for the parameter `{name:Type}`.
    pub fn param(mut self, name: impl Into<String>, value: impl Serialize) -> Self {
        let value = serde_json::to_value(value).expect("a parameter is a JSON value");
        self.params.insert(name.into(), value);
        self
    }

    /// Values for several parameters.
    pub fn params(mut self, params: Params) -> Self {
        self.params.extend(params);
        self
    }

    /// The snapshot holds every event in Kafka when subscribing (up to the topics' latest
    /// offsets then), not just what the result has: the server holds it until the result gets
    /// there, or ends the subscription with `CATCH_UP_TIMEOUT` (its `complete_wait`). Also on
    /// every snapshot after a reconnect. [`history`](Self::history) always asks for it.
    pub fn complete(mut self) -> Self {
        self.complete = true;
        self
    }

    /// Every update: the snapshot, each diff with what it changed, disconnects and resumes.
    pub fn raw<R: DeserializeOwned>(self) -> Subscription<R> {
        self.client
            .subscribe_opts(self.sql, self.params, self.complete)
    }

    /// The whole result after each snapshot and diff. Ends after an error, its last item.
    pub fn rows<R: DeserializeOwned>(
        self,
    ) -> impl Stream<Item = Result<Rows<R>, LiveError>> + Unpin {
        self.raw::<R>().filter_map(|u| {
            futures::future::ready(match u.kind {
                Kind::Snapshot | Kind::Diff => Some(Ok(u.rows)),
                Kind::Error => Some(Err(error_of(&u))),
                Kind::Subscribed | Kind::Resumed | Kind::Disconnected => None,
            })
        })
    }

    /// The first row, kept current (`None` while the result is empty).
    pub fn first<R: DeserializeOwned + Send + Sync + 'static>(self) -> Live<Option<Arc<R>>> {
        live::spawn(
            self,
            |rows| rows.first().map(|r| r.as_ref().clone().into()),
            read_row,
        )
    }

    /// The last row, kept current (`None` while the result is empty): e.g. the latest event of
    /// a query ordered by time.
    pub fn last<R: DeserializeOwned + Send + Sync + 'static>(self) -> Live<Option<Arc<R>>> {
        live::spawn(
            self,
            |rows| rows.last().map(|r| r.as_ref().clone().into()),
            read_row,
        )
    }

    /// The first column of the first row, whatever it's called, kept current (`None` while the
    /// result is empty): e.g. a count.
    pub fn cell<T: DeserializeOwned + Clone + Send + Sync + 'static>(self) -> Live<Option<T>> {
        live::spawn(
            self,
            |rows| rows.first().and_then(|r| first_cell(r).cloned()),
            |v| v.map(read_cell).transpose(),
        )
    }

    /// Resolves once the result has a row, then unsubscribes.
    pub async fn exists(self) -> Result<(), LiveError> {
        let mut has = live::spawn(
            self,
            |rows| Some(serde_json::Value::Bool(!rows.is_empty())),
            |v| Ok(v == Some(&serde_json::Value::Bool(true))),
        );
        has.wait_for(|b| *b).await.map(|_| ())
    }

    /// Each row entering the result after the snapshot (the snapshot's rows aren't).
    pub fn inserts<R: DeserializeOwned>(self) -> Events<R> {
        Events::new(self.raw::<Row>(), Which::Inserts, None)
    }

    /// Each row leaving the result: pushed out by LIMIT, or a deleted FINAL row.
    pub fn deletes<R: DeserializeOwned>(self) -> Events<R> {
        Events::new(self.raw::<Row>(), Which::Deletes, None)
    }

    /// The result as it is now, then unsubscribes. Waits through reconnects.
    pub async fn fetch<R: DeserializeOwned>(self) -> Result<Rows<R>, LiveError> {
        let mut sub = self.raw::<R>();
        while let Some(u) = sub.next().await {
            match u.kind {
                Kind::Snapshot => return Ok(u.rows),
                Kind::Error => return Err(error_of(&u)),
                _ => {}
            }
        }
        Err(closed())
    }

    /// The first row now, if any.
    pub async fn fetch_one<R: DeserializeOwned>(self) -> Result<Option<Arc<R>>, LiveError> {
        Ok(self.fetch::<R>().await?.first().cloned())
    }

    /// The first column of the first row now, if any (see [`cell`](Self::cell)).
    pub async fn fetch_cell<T: DeserializeOwned>(self) -> Result<Option<T>, LiveError> {
        let rows = self.fetch::<Row>().await?;
        rows.first()
            .and_then(|r| first_cell(r))
            .map(read_cell)
            .transpose()
            .map_err(unreadable)
    }

    /// Every event in Kafka when subscribing, and every row inserted after: each row is in
    /// exactly one of the two, also over reconnects. The history is [`complete`](Self::complete):
    /// it waits for the result to get to Kafka's offsets at subscribe time.
    pub async fn history<R: DeserializeOwned>(self) -> Result<(Rows<R>, Events<R>), LiveError> {
        let mut sub = self.complete().raw::<Row>();
        while let Some(u) = sub.next().await {
            match u.kind {
                Kind::Snapshot => {
                    let history = u
                        .rows
                        .iter()
                        .map(|r| parse(r.as_ref().clone()).map(Arc::new))
                        .collect::<Result<Vec<_>, _>>()
                        .map_err(unreadable)?;
                    let events = Events::new(sub, Which::Inserts, Some(u.rows));
                    return Ok((Arc::new(history), events));
                }
                Kind::Error => return Err(error_of(&u)),
                _ => {}
            }
        }
        Err(closed())
    }
}

fn read_row<R: DeserializeOwned>(v: Option<&serde_json::Value>) -> Result<Option<Arc<R>>, String> {
    v.map(|v| {
        serde_json::from_value(v.clone())
            .map(Arc::new)
            .map_err(|e| format!("a row doesn't fit: {e}"))
    })
    .transpose()
}
