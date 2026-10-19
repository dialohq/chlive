//! The native backend: each group's aggregate states live in the engine. History seeds them
//! from main ClickHouse (plain SQL values); each event updates its group; per batch, the touched
//! groups are finalized and diffed.
//!
//! FINAL (a ReplacingMergeTree): each key's row is kept with its version, group and argument
//! values, and an event replaces it: the old values are taken back out of their group, the new
//! ones added (if the version isn't deleted and matches the WHERE). A group whose last row left
//! is gone. The history reads every row, so this holds every matching row's values in memory.

use std::cmp::Ordering;
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
use crate::types::{decode, Value};
use crate::watermark::Watermark;

struct Group {
    /// The key values, as ClickHouse renders them (also the id, as a JSON array).
    key: Vec<Json>,
    states: Vec<Box<dyn AggregateState>>,
    /// FINAL: how many kept rows are in it (at 0 it's gone, unless it's the global group).
    rows: u64,
}

/// FINAL: a key's row, as its values went into its group.
struct Kept {
    ver: Value,
    group: String,
    /// Each aggregate's argument (`Null` for `count()`'s none).
    args: Vec<Value>,
}

/// What a checkpoint holds: every group's states, the ranking, and the cursor. Shared with the
/// checkpointer; each step updates it before its iteration is yielded.
struct Shared {
    at: Watermark,
    groups: HashMap<String, Group>,
    result: GroupResult,
    /// FINAL: every matching key's row.
    kept: HashMap<String, Kept>,
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
                kept: HashMap::new(),
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
        let shape = if plan.per_row.is_some() {
            "native_final"
        } else {
            "native"
        };
        if checkpoint.shape() != shape {
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
                kept: HashMap::new(),
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
            let rows = g["rows"].as_u64().unwrap_or(0);
            groups.insert(id, Group { key, states, rows });
        }
        let mut kept = HashMap::new();
        if run.plan.per_row.is_some() {
            let bad = || "checkpoint: bad kept row".to_string();
            for k in saved["kept"].as_array().ok_or_else(bad)? {
                let id = k["id"].as_str().ok_or_else(bad)?.to_string();
                let load = |v: &Json| Value::load(v).ok_or_else(bad);
                let args = k["args"]
                    .as_array()
                    .ok_or_else(bad)?
                    .iter()
                    .map(load)
                    .collect::<Result<Vec<_>, _>>()?;
                kept.insert(
                    id,
                    Kept {
                        ver: load(&k["ver"])?,
                        group: k["group"].as_str().ok_or_else(bad)?.to_string(),
                        args,
                    },
                );
            }
        }
        result.load(&saved["result"])?;
        {
            let mut shared = run.shared.lock().unwrap();
            shared.groups = groups;
            shared.result = result;
            shared.kept = kept;
        }
        run.history_sql = None;
        Ok(run)
    }

    /// Reads the groups' states and ranking, between steps.
    pub(crate) fn checkpointer(&self) -> Checkpointer {
        let shared = self.shared.clone();
        let per_row = self.plan.per_row.is_some();
        Arc::new(move || {
            let s = shared.lock().unwrap();
            let groups: Vec<Json> = s
                .groups
                .iter()
                .map(|(id, g)| {
                    let mut json = serde_json::json!({
                        "id": id,
                        "key": g.key,
                        "states": g.states.iter().map(|st| st.save()).collect::<Vec<_>>(),
                    });
                    if per_row {
                        json["rows"] = g.rows.into();
                    }
                    json
                })
                .collect();
            let mut state = serde_json::json!({
                "shape": if per_row { "native_final" } else { "native" },
                "groups": groups,
                "result": s.result.save(),
            });
            if per_row {
                state["kept"] = s
                    .kept
                    .iter()
                    .map(|(id, k)| {
                        serde_json::json!({
                            "id": id,
                            "ver": k.ver.save(),
                            "group": k.group,
                            "args": k.args.iter().map(Value::save).collect::<Vec<_>>(),
                        })
                    })
                    .collect();
            }
            Checkpoint {
                at: s.at.clone(),
                state,
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
        let retractable = self.plan.per_row.is_some();
        self.plan
            .aggs
            .iter()
            .map(|a| {
                let f = crate::aggregate::native(a.func).expect("a native plan");
                if retractable {
                    f.new_retractable_state(a.arg_ty.as_ref(), &a.result_ty)
                } else {
                    f.new_state(a.arg_ty.as_ref(), &a.result_ty)
                }
            })
            .collect()
    }

    async fn history(&mut self, sql: &str) -> Result<Iteration, EngineError> {
        let rows = self.history.fetch(sql).await;
        self.snapshot = None;
        let rows = rows?;
        if self.plan.per_row.is_some() {
            return self.history_rows(rows);
        }
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
            groups.insert(
                id,
                Group {
                    key,
                    states,
                    rows: 0,
                },
            );
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

    /// FINAL's history: every row, into its group and kept.
    fn history_rows(&mut self, rows: Vec<Row>) -> Result<Iteration, EngineError> {
        let keyed = self.stream.keyed.as_ref().expect("FINAL").clone();
        let per_row = self.plan.per_row.clone().expect("FINAL");
        let bad = |e: String| EngineError::History(HistoryError::ClickHouse(e));
        let mut guard = self.shared.lock().unwrap();
        let shared = &mut *guard;
        let mut ids: Vec<String> = Vec::new();
        if self.plan.keys.is_empty() {
            // a global aggregate has its one row, rows or not
            let id = Json::Array(Vec::new()).to_string();
            shared.groups.insert(
                id.clone(),
                Group {
                    key: Vec::new(),
                    states: self.new_states(),
                    rows: 0,
                },
            );
            ids.push(id);
        }
        for mut row in rows {
            let (id, ver) = keyed
                .history_row(&mut row)
                .map_err(|e| bad(format!("FINAL row: {e:?}")))?;
            let key: Vec<Json> = (0..self.plan.keys.len())
                .map(|i| row.shift_remove(&format!("__k{i}")).unwrap_or(Json::Null))
                .collect();
            let args = per_row
                .args
                .iter()
                .map(|a| match a {
                    Some((field, ty)) => decode(row.get(field), ty)
                        .map_err(|e| bad(format!("aggregate argument: {e}"))),
                    None => Ok(Value::Null),
                })
                .collect::<Result<Vec<_>, _>>()?;
            let group = Json::Array(key.clone()).to_string();
            if !shared.groups.contains_key(&group) {
                ids.push(group.clone());
                shared.groups.insert(
                    group.clone(),
                    Group {
                        key,
                        states: self.new_states(),
                        rows: 0,
                    },
                );
            }
            let g = shared.groups.get_mut(&group).expect("just made");
            add(&self.plan, g, &args);
            shared.kept.insert(id, Kept { ver, group, args });
        }
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

    /// FINAL's events: each replaces its key's row (see the module's doc).
    fn batch_rows(&mut self, events: Vec<Event>) -> Iteration {
        let keyed = self.stream.keyed.as_ref().expect("FINAL").clone();
        let last = events.last().expect("a non-empty batch");
        let origin = Origin::Event {
            topic: last.topic.clone(),
            partition: last.partition,
            offset: last.offset,
        };
        let mut touched: Vec<String> = Vec::new();
        let mut seen: HashSet<String> = HashSet::new();
        let mut touch = |id: &String| {
            if seen.insert(id.clone()) {
                touched.push(id.clone());
            }
        };
        let global = self.plan.keys.is_empty();
        let mut guard = self.shared.lock().unwrap();
        let shared = &mut *guard;
        for event in &events {
            let position = shared.at.0.entry(event.partition).or_insert(event.offset);
            *position = (*position).max(event.offset);
            let row = match self.stream.decode(&event.value) {
                Ok(row) => row,
                Err(e) => {
                    tracing::warn!(topic = %event.topic, partition = event.partition,
                        offset = event.offset, error = %e, "skipping event");
                    continue;
                }
            };
            let (id, ver, deleted) = keyed.event(&row);
            // an older version than the kept row's: ignored (ties go to the later)
            if let Some(k) = shared.kept.get(&id) {
                if ver.compare(&k.ver) == Some(Ordering::Less) {
                    continue;
                }
            }
            if let Some(old) = shared.kept.remove(&id) {
                let g = shared
                    .groups
                    .get_mut(&old.group)
                    .expect("a kept row's group");
                for (j, (agg, state)) in self.plan.aggs.iter().zip(&mut g.states).enumerate() {
                    state.retract(agg.arg.map(|_| &old.args[j]));
                }
                g.rows -= 1;
                if g.rows == 0 && !global {
                    shared.groups.remove(&old.group);
                }
                touch(&old.group);
            }
            if deleted || !self.stream.matches(&row) {
                continue;
            }
            let key: Vec<Json> = self.plan.keys.iter().map(|k| row[k.0].to_json()).collect();
            let args: Vec<Value> = self
                .plan
                .aggs
                .iter()
                .map(|a| a.arg.map_or(Value::Null, |c| row[c.0].clone()))
                .collect();
            let group = Json::Array(key.clone()).to_string();
            let new_states = || self.new_states();
            let g = shared.groups.entry(group.clone()).or_insert_with(|| Group {
                key,
                states: new_states(),
                rows: 0,
            });
            add(&self.plan, g, &args);
            touch(&group);
            shared.kept.insert(id, Kept { ver, group, args });
        }
        let changes = touched
            .into_iter()
            .map(|id| match shared.groups.contains_key(&id) {
                true => {
                    let (id, row) = row(&self.plan, &shared.groups, id);
                    (id, Some(row))
                }
                false => (id, None),
            })
            .collect();
        let diffs = shared.result.changes(changes);
        Iteration {
            origin,
            diffs,
            at: shared.at.clone(),
        }
    }

    fn batch(&mut self, events: Vec<Event>) -> Iteration {
        if self.plan.per_row.is_some() {
            return self.batch_rows(events);
        }
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
                rows: 0,
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

/// A row's values into its group.
fn add(plan: &NativeAggregates, group: &mut Group, args: &[Value]) {
    for (j, (agg, state)) in plan.aggs.iter().zip(&mut group.states).enumerate() {
        state.update(agg.arg.map(|_| &args[j]));
    }
    group.rows += 1;
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
