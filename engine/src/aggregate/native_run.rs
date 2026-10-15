//! The native backend: each group's aggregate states live in the engine. History seeds them
//! from main ClickHouse (plain SQL values); each event updates its group; per batch, the touched
//! groups are finalized and diffed.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use futures::stream::{BoxStream, StreamExt};
use serde_json::Value as Json;

use super::result::GroupResult;
use super::AggregateState;
use crate::checkpoint::{Checkpoint, Checkpointer};
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

/// What a checkpoint holds: every group's states, the ranking, and the cursor. Shared with the
/// checkpointer; each step updates it before its iteration is yielded.
struct Shared {
    at: Watermark,
    groups: HashMap<String, Group>,
    result: GroupResult,
}

pub(crate) struct NativeRun {
    history: Arc<dyn HistorySource>,
    /// The history query, until it has run.
    history_sql: Option<String>,
    /// The snapshot it reads, until then.
    snapshot: Option<crate::snapshot::Lease>,
    /// Restored from a checkpoint: the first step is an empty catch-up.
    restored: bool,
    plan: NativeAggregates,
    stream: StreamPlan,
    events: BoxStream<'static, Vec<Event>>,
    shared: Arc<Mutex<Shared>>,
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
            restored: false,
            plan,
            stream,
            events,
            shared: Arc::new(Mutex::new(Shared {
                at,
                groups: HashMap::new(),
                result,
            })),
            failed: false,
        }
    }

    /// From a checkpoint (its `state`): every group's states and the ranking, at `at`.
    pub(crate) fn restored(
        history: Arc<dyn HistorySource>,
        plan: NativeAggregates,
        stream: StreamPlan,
        events: BoxStream<'static, Vec<Event>>,
        checkpoint: &Checkpoint,
        mut result: GroupResult,
    ) -> Result<Self, String> {
        let saved = &checkpoint.state;
        if checkpoint.shape() != "native" {
            return Err(format!(
                "checkpoint of a {} result for an aggregate query",
                checkpoint.shape()
            ));
        }
        let mut run = Self {
            history,
            history_sql: None,
            snapshot: None,
            restored: true,
            plan,
            stream,
            events,
            shared: Arc::new(Mutex::new(Shared {
                at: checkpoint.at.clone(),
                groups: HashMap::new(),
                result: GroupResult::new(Vec::new(), None, None, Vec::new()),
            })),
            failed: false,
        };
        let mut groups = HashMap::new();
        for g in saved["groups"].as_array().ok_or("checkpoint: no groups")? {
            let id = g["id"].as_str().ok_or("checkpoint: group id")?.to_string();
            let key = g["key"].as_array().ok_or("checkpoint: group key")?.clone();
            let mut states = run.new_states();
            let saved_states = g["states"].as_array().ok_or("checkpoint: group states")?;
            if saved_states.len() != states.len() {
                return Err("checkpoint: another set of aggregates".into());
            }
            for (state, s) in states.iter_mut().zip(saved_states) {
                state.load(s)?;
            }
            groups.insert(id, Group { key, states });
        }
        result.load(&saved["result"])?;
        {
            let mut shared = run.shared.lock().unwrap();
            shared.groups = groups;
            shared.result = result;
        }
        run.history_sql = None;
        Ok(run)
    }

    /// Reads the groups' states and ranking, between steps.
    pub(crate) fn checkpointer(&self) -> Checkpointer {
        let shared = self.shared.clone();
        Arc::new(move || {
            let s = shared.lock().unwrap();
            let groups: Vec<Json> = s
                .groups
                .iter()
                .map(|(id, g)| {
                    serde_json::json!({
                        "id": id,
                        "key": g.key,
                        "states": g.states.iter().map(|st| st.save()).collect::<Vec<_>>(),
                    })
                })
                .collect();
            Checkpoint {
                at: s.at.clone(),
                state: serde_json::json!({
                    "shape": "native",
                    "groups": groups,
                    "result": s.result.save(),
                }),
            }
        })
    }

    pub(crate) async fn step(&mut self) -> Option<Result<Iteration, EngineError>> {
        if self.failed {
            return None;
        }
        if std::mem::take(&mut self.restored) {
            let at = self.shared.lock().unwrap().at.clone();
            return Some(Ok(Iteration {
                origin: Origin::CatchUp,
                diffs: Vec::new(),
                at,
            }));
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
        let mut groups = HashMap::new();
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
            groups.insert(id, Group { key, states });
        }
        let mut shared = self.shared.lock().unwrap();
        shared.groups = groups;
        let rows = ids
            .into_iter()
            .map(|id| row(&self.plan, &shared.groups, id))
            .collect();
        let diffs = shared.result.history(rows);
        Ok(Iteration {
            origin: Origin::History,
            diffs,
            at: shared.at.clone(),
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
        let new_states = || self.new_states();
        let mut guard = self.shared.lock().unwrap();
        let shared = &mut *guard;
        for event in &events {
            let position = shared.at.0.entry(event.partition).or_insert(event.offset);
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
            let group = shared.groups.entry(id.clone()).or_insert_with(|| Group {
                key,
                states: new_states(),
            });
            for (agg, state) in self.plan.aggs.iter().zip(&mut group.states) {
                state.update(agg.arg.map(|a| &row[a.0]));
            }
            if seen.insert(id.clone()) {
                touched.push(id);
            }
        }
        let rows = touched
            .into_iter()
            .map(|id| row(&self.plan, &shared.groups, id))
            .collect();
        let diffs: Vec<Diff> = shared.result.batch(rows);
        Iteration {
            origin,
            diffs,
            at: shared.at.clone(),
        }
    }
}

/// The group's row now (hidden ORDER BY columns included).
fn row(plan: &NativeAggregates, groups: &HashMap<String, Group>, id: String) -> (String, Row) {
    let group = &groups[&id];
    let row: Row = plan
        .items
        .iter()
        .map(|item| match item {
            NativeItem::Key(k, name) => (name.clone(), group.key[*k].clone()),
            NativeItem::Agg(j, name) => (name.clone(), group.states[*j].finalize().to_json()),
        })
        .collect();
    (id, row)
}
