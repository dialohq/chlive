//! The native backend: each group's aggregate states live in the engine. History seeds them
//! from main ClickHouse (plain SQL values); each event updates its group; per batch, the touched
//! groups are finalized and diffed.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use futures::stream::{BoxStream, StreamExt};
use serde_json::Value as Json;

use super::result::GroupResult;
use super::AggregateState;
use crate::diff::{Diff, Row};
use crate::engine::{EngineError, Iteration, Origin};
use crate::history::{HistoryError, HistorySource};
use crate::plan::{NativeAggregates, NativeItem, StreamPlan};
use crate::source::Event;
use crate::types::decode;
use crate::watermark::Watermark;

struct Group {
    /// The key values, as ClickHouse renders them (also the id, as a JSON array).
    key: Vec<Json>,
    states: Vec<Box<dyn AggregateState>>,
}

pub(crate) struct NativeRun {
    history: Arc<dyn HistorySource>,
    /// The history query, until it has run.
    history_sql: Option<String>,
    /// The snapshot it reads, until then.
    snapshot: Option<crate::snapshot::Lease>,
    plan: NativeAggregates,
    stream: StreamPlan,
    events: BoxStream<'static, Vec<Event>>,
    at: Watermark,
    groups: HashMap<String, Group>,
    result: GroupResult,
    failed: bool,
}

impl NativeRun {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        history: Arc<dyn HistorySource>,
        history_sql: String,
        snapshot: crate::snapshot::Lease,
        plan: NativeAggregates,
        stream: StreamPlan,
        events: BoxStream<'static, Vec<Event>>,
        at: Watermark,
        result: GroupResult,
    ) -> Self {
        Self {
            history,
            history_sql: Some(history_sql),
            snapshot: Some(snapshot),
            plan,
            stream,
            events,
            at,
            groups: HashMap::new(),
            result,
            failed: false,
        }
    }

    pub(crate) async fn step(&mut self) -> Option<Result<Iteration, EngineError>> {
        if self.failed {
            return None;
        }
        let result = match self.history_sql.take() {
            Some(sql) => self.history(&sql).await,
            None => {
                let batch = self.events.next().await?;
                Ok(self.batch(batch))
            }
        };
        self.failed = result.is_err();
        Some(result)
    }

    fn new_states(&self) -> Vec<Box<dyn AggregateState>> {
        self.plan
            .aggs
            .iter()
            .map(|a| {
                crate::aggregate::native(a.func)
                    .expect("a native plan")
                    .new_state(a.arg_ty.as_ref(), &a.result_ty)
            })
            .collect()
    }

    async fn history(&mut self, sql: &str) -> Result<Iteration, EngineError> {
        let rows = self.history.fetch(sql).await;
        self.snapshot = None;
        let rows = rows?;
        let mut ids = Vec::with_capacity(rows.len());
        for mut row in rows {
            let key: Vec<Json> = (0..self.plan.keys.len())
                .map(|i| row.shift_remove(&format!("__k{i}")).unwrap_or(Json::Null))
                .collect();
            let mut states = self.new_states();
            for (agg, state) in self.plan.aggs.iter().zip(&mut states) {
                let values = agg
                    .history
                    .iter()
                    .map(|(alias, ty)| decode(row.get(alias), ty))
                    .collect::<Result<Vec<_>, _>>()
                    .map_err(|e| HistoryError::ClickHouse(format!("aggregate state: {e}")))?;
                state.merge_history(&values);
            }
            let id = Json::Array(key.clone()).to_string();
            ids.push(id.clone());
            self.groups.insert(id, Group { key, states });
        }
        let rows = ids.into_iter().map(|id| self.row(id)).collect();
        let diffs = self.result.history(rows);
        Ok(Iteration {
            origin: Origin::History,
            diffs,
            at: self.at.clone(),
        })
    }

    fn batch(&mut self, events: Vec<Event>) -> Iteration {
        let last = events.last().expect("a non-empty batch");
        let origin = Origin::Event {
            topic: last.topic.clone(),
            partition: last.partition,
            offset: last.offset,
        };
        let mut touched: Vec<String> = Vec::new();
        let mut seen: HashSet<String> = HashSet::new();
        for event in &events {
            let position = self.at.0.entry(event.partition).or_insert(event.offset);
            *position = (*position).max(event.offset);
            let row = match self.stream.decode(&event.value) {
                Ok(row) if self.stream.matches(&row) => row,
                Ok(_) => continue,
                Err(e) => {
                    tracing::warn!(topic = %event.topic, partition = event.partition,
                        offset = event.offset, error = %e, "skipping event");
                    continue;
                }
            };
            let key: Vec<Json> = self.plan.keys.iter().map(|k| row[k.0].to_json()).collect();
            let id = Json::Array(key.clone()).to_string();
            if !self.groups.contains_key(&id) {
                let states = self.new_states();
                self.groups.insert(id.clone(), Group { key, states });
            }
            let group = self.groups.get_mut(&id).expect("just inserted");
            for (agg, state) in self.plan.aggs.iter().zip(&mut group.states) {
                state.update(agg.arg.map(|a| &row[a.0]));
            }
            if seen.insert(id.clone()) {
                touched.push(id);
            }
        }
        let rows = touched.into_iter().map(|id| self.row(id)).collect();
        let diffs: Vec<Diff> = self.result.batch(rows);
        Iteration {
            origin,
            diffs,
            at: self.at.clone(),
        }
    }

    /// The group's row now (hidden ORDER BY columns included).
    fn row(&self, id: String) -> (String, Row) {
        let group = &self.groups[&id];
        let row: Row = self
            .plan
            .items
            .iter()
            .map(|item| match item {
                NativeItem::Key(k, name) => (name.clone(), group.key[*k].clone()),
                NativeItem::Agg(j, name) => (name.clone(), group.states[*j].finalize().to_json()),
            })
            .collect();
        (id, row)
    }
}
