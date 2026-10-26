//! Rows entering or leaving a result, one by one. Read on the caller's poll, like the raw
//! subscription: a reader that falls behind slows the connection rather than losing rows.

use std::collections::{HashMap, VecDeque};
use std::marker::PhantomData;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use futures::{Stream, StreamExt};
use serde::de::DeserializeOwned;

use crate::query::{error_of, unreadable};
use crate::store::parse;
use crate::{Change, Kind, LiveError, Row, Rows, Subscription};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Which {
    Inserts,
    Deletes,
}

/// A stream of the rows entering ([`inserts`](crate::Query::inserts)) or leaving
/// ([`deletes`](crate::Query::deletes)) a result, each once; ends after an error, its last item.
///
/// Changes to aggregated and FINAL rows by id (`update`s) are neither. A reconnect resumes where
/// it left off; when the server can't resume, the new snapshot is compared with the rows before
/// it, so rows that came or went meanwhile are told (one that did both isn't).
pub struct Events<R> {
    sub: Subscription<Row>,
    which: Which,
    /// The rows so far; `None` until the first snapshot (whose rows aren't events).
    rows: Option<Rows<Row>>,
    queue: VecDeque<Arc<Row>>,
    done: bool,
    _rows: PhantomData<fn() -> R>,
}

impl<R> Unpin for Events<R> {}

impl<R: DeserializeOwned> Events<R> {
    pub(crate) fn new(sub: Subscription<Row>, which: Which, rows: Option<Rows<Row>>) -> Self {
        Events {
            sub,
            which,
            rows,
            queue: VecDeque::new(),
            done: false,
            _rows: PhantomData,
        }
    }

    /// The id on the wire (`s1`, `s2`, …).
    pub fn id(&self) -> &str {
        self.sub.id()
    }
}

/// The rows of `after` that aren't in `before`, in order; equal rows are counted, not merged.
fn added(before: &[Arc<Row>], after: &[Arc<Row>]) -> Vec<Arc<Row>> {
    let key = |r: &Row| serde_json::to_string(r).expect("rows are JSON");
    let mut left: HashMap<String, usize> = HashMap::new();
    for r in before {
        *left.entry(key(r)).or_default() += 1;
    }
    after
        .iter()
        .filter(|r| match left.get_mut(&key(r)) {
            Some(n) if *n > 0 => {
                *n -= 1;
                false
            }
            _ => true,
        })
        .cloned()
        .collect()
}

impl<R: DeserializeOwned> Stream for Events<R> {
    type Item = Result<Arc<R>, LiveError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            if let Some(row) = this.queue.pop_front() {
                return Poll::Ready(Some(match parse(row.as_ref().clone()) {
                    Ok(r) => Ok(Arc::new(r)),
                    Err(e) => {
                        this.done = true;
                        this.queue.clear();
                        Err(unreadable(e))
                    }
                }));
            }
            if this.done {
                return Poll::Ready(None);
            }
            let Some(u) = std::task::ready!(this.sub.poll_next_unpin(cx)) else {
                this.done = true;
                return Poll::Ready(None);
            };
            match u.kind {
                Kind::Snapshot => {
                    if let Some(before) = &this.rows {
                        this.queue.extend(match this.which {
                            Which::Inserts => added(before, &u.rows),
                            Which::Deletes => added(&u.rows, before),
                        });
                    }
                    this.rows = Some(u.rows);
                }
                Kind::Diff => {
                    for c in u.changes {
                        match (c, this.which) {
                            (Change::Insert { row, .. }, Which::Inserts)
                            | (Change::Delete { row, .. }, Which::Deletes) => {
                                this.queue.push_back(row)
                            }
                            _ => {}
                        }
                    }
                    this.rows = Some(u.rows);
                }
                Kind::Error => {
                    this.done = true;
                    return Poll::Ready(Some(Err(error_of(&u))));
                }
                Kind::Subscribed | Kind::Resumed | Kind::Disconnected => {}
            }
        }
    }
}
