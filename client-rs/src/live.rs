//! A value kept current: one task per view follows the query and publishes the value whenever
//! it's different, so a slow reader only misses values in between, and never holds up the
//! connection the subscriptions share.

use futures::{Stream, StreamExt};
use serde_json::Value;
use tokio::sync::watch;

use crate::query::{closed, error_of, unreadable, Query};
use crate::{Kind, LiveError, Row, Rows, Status};

#[derive(Debug, Clone)]
struct State<T> {
    value: Option<T>,
    status: Status,
    /// Bumped on every new value.
    seq: u64,
}

/// A value of a live query, kept current by a task of its own; dropping every handle (clones
/// included) stops the task and unsubscribes.
#[derive(Debug, Clone)]
pub struct Live<T> {
    rx: watch::Receiver<State<T>>,
    /// The `seq` last returned.
    seen: u64,
}

/// Follows `query`: `select` picks the JSON the value is made of from the rows, `read` makes it
/// a `T` (only when it's different from the last).
pub(crate) fn spawn<T: Clone + Send + Sync + 'static>(
    query: Query,
    select: impl Fn(&Rows<Row>) -> Option<Value> + Send + 'static,
    read: impl Fn(Option<&Value>) -> Result<T, String> + Send + 'static,
) -> Live<T> {
    let (tx, rx) = watch::channel(State {
        value: None,
        status: Status::Loading,
        seq: 0,
    });
    let mut sub = query.raw::<Row>();
    tokio::spawn(async move {
        let mut last: Option<Option<Value>> = None;
        loop {
            let u = tokio::select! {
                u = sub.next() => u,
                _ = tx.closed() => return,
            };
            let Some(u) = u else {
                tx.send_modify(|s| s.status = Status::Error(closed()));
                return;
            };
            match u.kind {
                Kind::Snapshot | Kind::Diff | Kind::Resumed => {
                    let picked = select(&u.rows);
                    if last.as_ref() == Some(&picked) {
                        tx.send_if_modified(|s| {
                            let was = std::mem::replace(&mut s.status, Status::Live);
                            was != Status::Live
                        });
                        continue;
                    }
                    match read(picked.as_ref()) {
                        Ok(value) => tx.send_modify(|s| {
                            s.value = Some(value);
                            s.status = Status::Live;
                            s.seq += 1;
                        }),
                        Err(e) => {
                            tx.send_modify(|s| s.status = Status::Error(unreadable(e)));
                            return;
                        }
                    }
                    last = Some(picked);
                }
                Kind::Disconnected => tx.send_modify(|s| s.status = Status::Loading),
                Kind::Error => {
                    tx.send_modify(|s| s.status = Status::Error(error_of(&u)));
                    return;
                }
                Kind::Subscribed => {}
            }
        }
    });
    Live { rx, seen: 0 }
}

impl<T: Clone> Live<T> {
    /// The value now; `None` until the first snapshot.
    pub fn get(&self) -> Option<T> {
        self.rx.borrow().value.clone()
    }

    /// `Loading` (before the first snapshot, or reconnecting: the value is kept), `Live` or
    /// `Error`.
    pub fn status(&self) -> Status {
        self.rx.borrow().status.clone()
    }

    /// The value, once there is one.
    pub async fn current(&mut self) -> Result<T, LiveError> {
        self.wait_for(|_| true).await
    }

    /// The next value different from the last one this handle returned (the first one, if it
    /// returned none).
    pub async fn changed(&mut self) -> Result<T, LiveError> {
        let seen = self.seen;
        self.until(|s| s.seq > seen).await
    }

    /// The value once `f` holds for it, now or later. Values in between may be skipped: it's
    /// for states, like a count reaching 10, not for events.
    pub async fn wait_for(&mut self, f: impl Fn(&T) -> bool) -> Result<T, LiveError> {
        self.until(|s| s.value.as_ref().is_some_and(&f)).await
    }

    async fn until(&mut self, f: impl Fn(&State<T>) -> bool) -> Result<T, LiveError> {
        let state = self
            .rx
            .wait_for(|s| matches!(s.status, Status::Error(_)) || f(s))
            .await
            .map_err(|_| closed())?;
        if let Status::Error(e) = &state.status {
            return Err(e.clone());
        }
        self.seen = state.seq;
        Ok(state.value.clone().expect("a value, once one's seen"))
    }

    /// The values this handle hasn't returned yet: the value now (unless it returned it), then
    /// every different one. Ends after an error, its last item.
    pub fn into_stream(self) -> impl Stream<Item = Result<T, LiveError>> + Unpin
    where
        T: Send + Sync + 'static,
    {
        Box::pin(futures::stream::unfold(Some(self), |live| async move {
            let mut live = live?;
            let next = live.changed().await;
            let more = next.is_ok().then_some(live);
            Some((next, more))
        }))
    }
}
