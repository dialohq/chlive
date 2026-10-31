//! Running `key IN (SELECT inner_key FROM inner [WHERE …])` live.
//!
//! The sub-query's keys `S` only grow (both tables are append-only), so outer rows only ever
//! enter the result. Each outer event whose key is in `S` is placed as usual; one whose key
//! isn't yet is kept back, in a **tail** keyed by its key. When a key enters `S` (an inner
//! event), the outer rows with that key that were passed over are sent:
//! - those up to the outer snapshot's watermark `W`, from that snapshot (one ClickHouse query
//!   per batch of entering keys);
//! - those after it, from the tail.
//!
//! The tail only ever holds rows after `W`: the run keeps switching to newer snapshots of the
//! outer table (once its stream is past them) and drops the tail rows they now hold. So the
//! state is `S` (keys) plus about one snapshot refresh interval of kept-back rows.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use futures::stream::{BoxStream, StreamExt};

use crate::diff::{Diff, Row};
use crate::engine::{
    bad_history_row, history_iteration, EngineError, Iteration, Origin, ResultState,
};
use crate::history::HistorySource;
use crate::order::SortKey;
use crate::plan::{Plan, SemiPlan};
use crate::snapshot::{Lease, Snapshots};
use crate::source::Event;
use crate::types::{decode, Value};
use crate::watermark::Watermark;

/// An outer row kept back: its key wasn't in `S` when it arrived.
struct Held {
    partition: i32,
    offset: i64,
    row: Row,
    key: Option<SortKey>,
}

/// What the first step fetches: the history, and `S`, over snapshots of both tables.
pub(crate) struct SemiStart {
    pub history_sql: String,
    pub keys_sql: String,
    /// Read by both queries; dropped once they've run.
    pub inner_lease: Lease,
}

pub(crate) struct SemiRun {
    history: Arc<dyn HistorySource>,
    snapshots: Arc<dyn Snapshots>,
    plan: Plan,
    start: Option<SemiStart>,
    /// `S`, by [`key_string`].
    keys: HashSet<String>,
    /// Kept-back outer rows after the snapshot `lease`, by key.
    tail: HashMap<String, Vec<Held>>,
    /// The outer table's snapshot back-fills read: every outer row up to its watermark.
    lease: Lease,
    /// How far the outer topic has been read (the cursor), and the inner topic.
    at: Watermark,
    inner_at: Watermark,
    state: ResultState,
    events: BoxStream<'static, Vec<Event>>,
    /// How often to look for a newer outer snapshot (also keeps its table in use).
    refresh: Duration,
    last_refresh: tokio::time::Instant,
    failed: bool,
}

impl SemiRun {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        history: Arc<dyn HistorySource>,
        snapshots: Arc<dyn Snapshots>,
        plan: Plan,
        start: SemiStart,
        lease: Lease,
        inner_at: Watermark,
        events: BoxStream<'static, Vec<Event>>,
        refresh: Duration,
    ) -> Self {
        let state = ResultState::for_plan(&plan.stream);
        Self {
            history,
            snapshots,
            at: lease.watermark.clone(),
            plan,
            start: Some(start),
            keys: HashSet::new(),
            tail: HashMap::new(),
            lease,
            inner_at,
            state,
            events,
            refresh,
            last_refresh: tokio::time::Instant::now(),
            failed: false,
        }
    }

    fn semi(&self) -> &SemiPlan {
        self.plan.semi.as_ref().expect("a semi-join plan")
    }

    pub(crate) async fn step(&mut self) -> Option<Result<Iteration, EngineError>> {
        if self.failed {
            return None;
        }
        let result = match self.start.take() {
            Some(start) => self.first(start).await,
            None => {
                let batch = self.events.next().await?;
                self.follow_snapshots().await;
                self.batch(batch).await
            }
        };
        self.failed = result.is_err();
        Some(result)
    }

    /// The history (over both snapshots) and `S`.
    async fn first(&mut self, start: SemiStart) -> Result<Iteration, EngineError> {
        let rows = self.history.fetch(&start.history_sql).await;
        let keys = self.history.fetch(&start.keys_sql).await?;
        drop(start.inner_lease);
        let semi = self.semi();
        let (key_ty, float) = (semi.inner_key_ty.clone(), semi.float_keys);
        for row in keys {
            let s = match &key_ty {
                Some(ty) => key_string(&decode(row.get("k"), ty).map_err(bad_history_row)?, float),
                None => String::new(), // uncorrelated: the one key, if the sub-query has a row
            };
            self.keys.insert(s);
        }
        history_iteration(&self.plan.stream, &mut self.state, &self.at, rows)
    }

    /// Switch to a newer outer snapshot once the stream is past it, dropping the tail rows it
    /// now holds. Not before: back-filling from a snapshot ahead of the stream would send rows
    /// the stream sends again.
    async fn follow_snapshots(&mut self) {
        if self.last_refresh.elapsed() < self.refresh {
            return;
        }
        self.last_refresh = tokio::time::Instant::now();
        let lease = match self.snapshots.acquire(&self.plan.source).await {
            Ok(l) => l,
            Err(e) => {
                tracing::warn!(error = %e, "semi-join: no newer snapshot; keeping the current one");
                return;
            }
        };
        let w = &lease.watermark;
        let newer = w.0.iter().any(|(p, o)| *o > self.lease.watermark.get(*p));
        let behind = w.0.iter().all(|(p, o)| *o <= self.at.get(*p));
        if newer && behind {
            for held in self.tail.values_mut() {
                held.retain(|h| h.offset > w.get(h.partition));
            }
            self.tail.retain(|_, v| !v.is_empty());
            self.lease = lease;
        }
    }

    async fn batch(&mut self, events: Vec<Event>) -> Result<Iteration, EngineError> {
        let last = events.last().expect("a non-empty batch");
        let origin = Origin::Event {
            topic: last.topic.clone(),
            partition: last.partition,
            offset: last.offset,
        };
        let semi = self.semi().clone();
        let mut entering: Vec<(String, Value)> = Vec::new();
        let mut diffs = Vec::new();
        for event in events {
            // the same table on both sides: the inner side first, so an event that brings its
            // key into `S` is itself in the result (as in ClickHouse)
            if event.topic == semi.inner_topic {
                advance(&mut self.inner_at, &event);
                match semi.inner_stream.decode(&event) {
                    Ok(row) if semi.inner_stream.matches(&row) => {
                        let (k, s) = match semi.inner_key {
                            Some(ik) => {
                                let k = row[ik.0].clone();
                                let s = key_string(&k, semi.float_keys);
                                (k, s)
                            }
                            // uncorrelated: every row has the one key
                            None => (Value::String(String::new()), String::new()),
                        };
                        if !matches!(k, Value::Null) && self.keys.insert(s.clone()) {
                            entering.push((s, k));
                        }
                    }
                    Ok(_) => {}
                    Err(e) => skip(&event, &e),
                }
            }
            if event.topic == self.plan.stream.topics[0] {
                advance(&mut self.at, &event);
                let stream = &self.plan.stream;
                match stream.decode(&event) {
                    Ok(row) if stream.matches(&row) => {
                        let s = match semi.outer_key {
                            Some(ok) => key_string(&row[ok.0], semi.float_keys),
                            None => String::new(),
                        };
                        let key = stream.order.as_ref().map(|o| o.event_key(&row));
                        let projected = stream.project(&row);
                        if self.keys.contains(&s) {
                            diffs.extend(self.state.place(projected, key));
                        } else {
                            self.tail.entry(s).or_default().push(Held {
                                partition: event.partition,
                                offset: event.offset,
                                row: projected,
                                key,
                            });
                        }
                    }
                    Ok(_) => {}
                    Err(e) => skip(&event, &e),
                }
            }
        }
        if !entering.is_empty() {
            diffs.extend(self.backfill(&semi, entering).await?);
        }
        Ok(Iteration {
            origin,
            diffs,
            at: self.at.clone(),
        })
    }

    /// The outer rows of keys that just entered `S`: up to the snapshot from it, after it from
    /// the tail. Each exactly once.
    async fn backfill(
        &mut self,
        semi: &SemiPlan,
        entering: Vec<(String, Value)>,
    ) -> Result<Vec<Diff>, EngineError> {
        let literals: Vec<String> = entering
            .iter()
            .map(|(_, v)| v.to_clickhouse_sql())
            .collect();
        let from = self.lease.sql_table();
        let sql = match semi.keys_condition(&literals) {
            Some(c) => self.plan.history.sql_where(&from, &c),
            // uncorrelated: the sub-query just got its first row; every row comes
            None => self.plan.history.sql(&from),
        };
        let rows = self.history.fetch(&sql).await?;
        let mut diffs = Vec::new();
        for mut row in rows {
            let key = match &self.plan.stream.order {
                Some(order) => Some(order.history_key(&mut row).map_err(bad_history_row)?),
                None => None,
            };
            diffs.extend(self.state.place(row, key));
        }
        for (s, _) in entering {
            for held in self.tail.remove(&s).unwrap_or_default() {
                diffs.extend(self.state.place(held.row, held.key));
            }
        }
        Ok(diffs)
    }
}

fn advance(at: &mut Watermark, event: &Event) {
    let position = at.0.entry(event.partition).or_insert(event.offset);
    *position = (*position).max(event.offset);
}

fn skip(event: &Event, e: &dyn std::fmt::Display) {
    tracing::warn!(topic = %event.topic, partition = event.partition, offset = event.offset,
        error = %e, "skipping event");
}

/// A key as `S` holds it: its JSON, with numbers as floats when either side's key is a float
/// (so `5` from an Int column matches `5.0` from a Float one).
pub(crate) fn key_string(v: &Value, float: bool) -> String {
    match (v, float) {
        (Value::Int(i), true) => format!("{:?}", *i as f64),
        (Value::Float(f), true) => format!("{f:?}"),
        _ => v.to_json().to_string(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn keys_match_across_number_types_only_when_needed() {
        assert_eq!(key_string(&Value::String("u1".into()), false), "\"u1\"");
        assert_eq!(key_string(&Value::Int(5), false), "5");
        // an Int column against a Float one: both as floats
        assert_eq!(
            key_string(&Value::Int(5), true),
            key_string(&Value::Float(5.0), true)
        );
    }
}
