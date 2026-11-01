//! A native aggregate's state: each group's aggregate states, seeded from the history (plain
//! SQL values), updated per event, and per batch the touched groups finalized and diffed.
//!
//! FINAL (a ReplacingMergeTree): each key's row is kept with its version, group and argument
//! values, and an event replaces it: the old values are taken back out of their group, the new
//! ones added (if the version isn't deleted and matches the WHERE). A group whose last row left
//! is gone. The history reads every row, so this holds every matching row's values in memory.
//!
//! Time windows (`ts > now() - INTERVAL …`) keep every row too, with its deadline; [`expire`]
//! takes the rows due out of their groups, as one iteration. With FINAL, a key's new version
//! replaces its deadline. Time is the caller's (`now`, in nanoseconds).
//!
//! [`expire`]: NativeState::expire

use std::cmp::Ordering;
use std::collections::{BTreeSet, HashMap, HashSet, VecDeque};
use std::sync::{Arc, Mutex};

use serde_json::Value as Json;

use super::result::GroupResult;
use super::AggregateState;
use crate::checkpoint::{Checkpoint, Checkpointer};
use crate::diff::{Diff, Row};
use crate::event::Event;
use crate::plan::{NativeAggregates, NativeItem, StreamPlan};
use crate::state::{Iteration, Origin, StateError};
use crate::types::{decode, Value};
use crate::watermark::Watermark;

struct Group {
    /// The key values, as ClickHouse renders them (also the id, as a JSON array).
    key: Vec<Json>,
    states: Vec<Box<dyn AggregateState>>,
    /// Per row: how many kept rows are in it (at 0 it's gone, unless it's the global group).
    rows: u64,
}

/// Per row (FINAL, `now()`): a row, as its values went into its group.
#[derive(Clone)]
struct Kept {
    /// FINAL: its version (`Null` without a version column, or without FINAL).
    ver: Value,
    /// With `now()`: whether it's in its group now, and the instants it goes out or comes in.
    in_group: bool,
    transitions: VecDeque<(i128, bool)>,
    group: String,
    /// Each aggregate's argument (`Null` for `count()`'s none).
    args: Vec<Value>,
    /// Its identity in its group's states (in arrival order): which element it added to a
    /// `groupArray`.
    seq: u64,
}

/// What a checkpoint holds: every group's states, the ranking, and the cursor. Shared with the
/// checkpointer; each step updates it before its iteration is yielded.
struct Shared {
    at: Watermark,
    groups: HashMap<String, Group>,
    result: GroupResult,
    /// Per row: every row in the result, by id (FINAL: its key; otherwise `#<n>`).
    kept: HashMap<String, Kept>,
    /// The kept rows' next instants, earliest first.
    expiry: BTreeSet<(i128, String)>,
    /// The last kept row's `seq` (and without FINAL, its id: `#<seq>`).
    next_row: u64,
}

impl Shared {
    fn keep(&mut self, id: String, kept: Kept) {
        if let Some((t, _)) = kept.transitions.front() {
            self.expiry.insert((*t, id.clone()));
        }
        self.kept.insert(id, kept);
    }

    fn forget(&mut self, id: &str) -> Option<Kept> {
        let kept = self.kept.remove(id)?;
        if let Some((t, _)) = kept.transitions.front() {
            self.expiry.remove(&(*t, id.to_string()));
        }
        Some(kept)
    }

    /// A row's values into its group (made if new); its group touched.
    fn put_in(
        &mut self,
        plan: &NativeAggregates,
        kept: &Kept,
        new_states: impl FnOnce() -> Vec<Box<dyn AggregateState>>,
        touch: &mut impl FnMut(&String),
    ) {
        let g = self
            .groups
            .entry(kept.group.clone())
            .or_insert_with(|| Group {
                key: serde_json::from_str(&kept.group).expect("a group's id is its key"),
                states: new_states(),
                rows: 0,
            });
        add(plan, g, &kept.args, kept.seq);
        touch(&kept.group);
    }

    /// A new row (or key's version), with its schedule: in its group if it's in now; kept
    /// while it can still change, and (FINAL) while it's in, for a new version to take it out.
    fn admit(
        &mut self,
        plan: &NativeAggregates,
        id: String,
        kept: Kept,
        keyed: bool,
        new_states: impl FnOnce() -> Vec<Box<dyn AggregateState>>,
        touch: &mut impl FnMut(&String),
    ) {
        if kept.in_group {
            self.put_in(plan, &kept, new_states, touch);
        }
        if !kept.transitions.is_empty() || (keyed && kept.in_group) {
            self.keep(id, kept);
        }
    }

    /// A row's values out of its group (gone with its last row, unless global); its group
    /// touched. Nothing for a row that isn't in.
    fn take_out(&mut self, plan: &NativeAggregates, old: Kept, touch: &mut impl FnMut(&String)) {
        if !old.in_group {
            return;
        }
        let g = self.groups.get_mut(&old.group).expect("a kept row's group");
        for (j, (agg, state)) in plan.aggs.iter().zip(&mut g.states).enumerate() {
            state.retract_row(agg.arg.map(|_| &old.args[j]), old.seq);
        }
        g.rows -= 1;
        if g.rows == 0 && !plan.keys.is_empty() {
            self.groups.remove(&old.group);
        }
        touch(&old.group);
    }
}

pub struct NativeState {
    plan: NativeAggregates,
    stream: StreamPlan,
    shared: Arc<Mutex<Shared>>,
}

impl NativeState {
    pub fn new(
        plan: NativeAggregates,
        stream: StreamPlan,
        at: Watermark,
        result: GroupResult,
    ) -> Self {
        Self {
            plan,
            stream,
            shared: Arc::new(Mutex::new(Shared {
                at,
                groups: HashMap::new(),
                result,
                kept: HashMap::new(),
                expiry: BTreeSet::new(),
                next_row: 0,
            })),
        }
    }

    /// Where the stream is: the cursor of the last event (or the history).
    pub fn at(&self) -> Watermark {
        self.shared.lock().unwrap().at.clone()
    }

    /// The earliest deadline of a kept row (none: nothing changes by time).
    pub fn earliest(&self) -> Option<i128> {
        self.shared.lock().unwrap().expiry.first().map(|(d, _)| *d)
    }

    /// Whether it keeps every row (FINAL, or time windows) rather than only the groups.
    pub fn per_row(&self) -> bool {
        self.plan.per_row.is_some()
    }

    /// The checkpoint shape: per row with FINAL, per row with time windows, or per group.
    fn shape(plan: &NativeAggregates) -> &'static str {
        match &plan.per_row {
            Some(p) if p.final_ => "native_final",
            Some(_) => "native_rows",
            None => "native",
        }
    }

    /// From a checkpoint (its `state`): every group's states and the ranking, at `at`.
    pub fn restored(
        plan: NativeAggregates,
        stream: StreamPlan,
        checkpoint: &Checkpoint,
        mut result: GroupResult,
    ) -> Result<Self, String> {
        let saved = &checkpoint.state;
        if checkpoint.shape() != Self::shape(&plan) {
            return Err(format!(
                "checkpoint of a {} result for an aggregate query",
                checkpoint.shape()
            ));
        }
        let run = Self {
            plan,
            stream,
            shared: Arc::new(Mutex::new(Shared {
                at: checkpoint.at.clone(),
                groups: HashMap::new(),
                result: GroupResult::new(Vec::new(), None, None, Vec::new()),
                kept: HashMap::new(),
                expiry: BTreeSet::new(),
                next_row: 0,
            })),
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
        let mut kept = Vec::new();
        let mut next_row = 0;
        if run.plan.per_row.is_some() {
            let bad = || "checkpoint: bad kept row".to_string();
            next_row = saved["next_row"].as_u64().unwrap_or(0);
            for k in saved["kept"].as_array().ok_or_else(bad)? {
                let id = k["id"].as_str().ok_or_else(bad)?.to_string();
                let load = |v: &Json| Value::load(v).ok_or_else(bad);
                let args = k["args"]
                    .as_array()
                    .ok_or_else(bad)?
                    .iter()
                    .map(load)
                    .collect::<Result<Vec<_>, _>>()?;
                // older checkpoints: a deadline (in, until then)
                let transitions: VecDeque<(i128, bool)> = match (&k["t"], &k["deadline"]) {
                    (Json::Array(t), _) => t
                        .iter()
                        .map(|t| {
                            let at = t[0].as_str().and_then(|s| s.parse().ok()).ok_or_else(bad)?;
                            Ok((at, t[1].as_bool().ok_or_else(bad)?))
                        })
                        .collect::<Result<_, String>>()?,
                    (_, Json::String(d)) => {
                        VecDeque::from([(d.parse().map_err(|_| bad())?, false)])
                    }
                    _ => VecDeque::new(),
                };
                let in_group = k["in"].as_bool().unwrap_or(true);
                kept.push((
                    id,
                    Kept {
                        ver: load(&k["ver"])?,
                        in_group,
                        transitions,
                        group: k["group"].as_str().ok_or_else(bad)?.to_string(),
                        args,
                        seq: k["seq"].as_u64().unwrap_or(0),
                    },
                ));
            }
        }
        result.load(&saved["result"])?;
        {
            let mut shared = run.shared.lock().unwrap();
            shared.groups = groups;
            shared.result = result;
            shared.next_row = next_row;
            for (id, k) in kept {
                shared.keep(id, k);
            }
        }
        Ok(run)
    }

    /// Reads the groups' states and ranking, between steps.
    pub fn checkpointer(&self) -> Checkpointer {
        let shared = self.shared.clone();
        let per_row = self.plan.per_row.is_some();
        let shape = Self::shape(&self.plan);
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
                "shape": shape,
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
                            // instants as strings: they don't fit JSON's numbers
                            "in": k.in_group,
                            "t": k
                                .transitions
                                .iter()
                                .map(|(t, b)| serde_json::json!([t.to_string(), b]))
                                .collect::<Vec<_>>(),
                            "group": k.group,
                            "seq": k.seq,
                            "args": k.args.iter().map(Value::save).collect::<Vec<_>>(),
                        })
                    })
                    .collect();
                state["next_row"] = s.next_row.into();
            }
            Checkpoint {
                at: s.at.clone(),
                state,
            }
        })
    }

    /// The rows whose deadline is at or before `now`, out of their groups: one iteration, if
    /// the result changed.
    pub fn expire(&mut self, now: i128) -> Option<Iteration> {
        let mut touched: Vec<String> = Vec::new();
        let mut seen: HashSet<String> = HashSet::new();
        let mut touch = |id: &String| {
            if seen.insert(id.clone()) {
                touched.push(id.clone());
            }
        };
        let mut guard = self.shared.lock().unwrap();
        let shared = &mut *guard;
        let keyed = self.stream.keyed.is_some();
        while let Some((d, id)) = shared.expiry.first().cloned() {
            if d > now {
                break;
            }
            let mut kept = shared.forget(&id).expect("a due row is kept");
            let (_, comes_in) = kept.transitions.pop_front().expect("a transition");
            if comes_in {
                // in its group as it comes in: its place in arrival order (groupArray) is now
                kept.in_group = true;
                shared.next_row += 1;
                kept.seq = shared.next_row;
                shared.put_in(&self.plan, &kept, || self.new_states(), &mut touch);
            } else {
                shared.take_out(&self.plan, kept.clone(), &mut touch);
                kept.in_group = false;
            }
            if !kept.transitions.is_empty() || (keyed && kept.in_group) {
                shared.keep(id, kept);
            }
        }
        share_arrays(&self.plan, &shared.groups, &mut shared.result, &touched);
        let diffs = shared
            .result
            .changes(changes(&self.plan, &shared.groups, touched));
        (!diffs.is_empty()).then(|| Iteration {
            origin: Origin::Clock,
            diffs,
            at: shared.at.clone(),
        })
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

    /// The history: per group, its states as ClickHouse computed them (or, per row, every row
    /// into its group, scheduled at `now`).
    pub fn history(&mut self, rows: Vec<Row>, now: i128) -> Result<Iteration, StateError> {
        if self.plan.per_row.is_some() {
            return self.history_rows(rows, now);
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
                    .map_err(|e| StateError(format!("aggregate state: {e}")))?;
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
        let mut guard = self.shared.lock().unwrap();
        let shared = &mut *guard;
        shared.groups = groups;
        share_arrays(&self.plan, &shared.groups, &mut shared.result, &ids);
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

    /// The history per row: every row, into its group and kept.
    fn history_rows(&mut self, rows: Vec<Row>, now: i128) -> Result<Iteration, StateError> {
        let keyed = self.stream.keyed.clone();
        let per_row = self.plan.per_row.clone().expect("per row");
        let timed = self.stream.timed();
        let bad = StateError;
        let mut guard = self.shared.lock().unwrap();
        let shared = &mut *guard;
        let mut ids: Vec<String> = Vec::new();
        let mut seen: HashSet<String> = HashSet::new();
        let mut touch = |id: &String| {
            if seen.insert(id.clone()) {
                ids.push(id.clone());
            }
        };
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
            touch(&id);
        }
        for mut row in rows {
            shared.next_row += 1;
            let seq = shared.next_row;
            let (id, ver) = match &keyed {
                Some(keyed) => keyed
                    .history_row(&mut row)
                    .map_err(|e| bad(format!("FINAL row: {e:?}")))?,
                None => (format!("#{seq}"), Value::Null),
            };
            // with now(): in now, or later (fetched as possible: maybe never)
            let schedule = match timed {
                true => {
                    let decoded = self
                        .stream
                        .history_decode(&mut row)
                        .map_err(|e| bad(format!("now(): {e:?}")))?;
                    self.stream.schedule(&decoded, now)
                }
                false => crate::plan::Schedule {
                    in_now: true,
                    transitions: Vec::new(),
                },
            };
            if !schedule.in_now && schedule.transitions.is_empty() {
                continue;
            }
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
            let group = Json::Array(key).to_string();
            let kept = Kept {
                ver,
                in_group: schedule.in_now,
                transitions: schedule.transitions.into(),
                group,
                args,
                seq,
            };
            shared.admit(
                &self.plan,
                id,
                kept,
                keyed.is_some(),
                || self.new_states(),
                &mut touch,
            );
        }
        share_arrays(&self.plan, &shared.groups, &mut shared.result, &ids);
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

    /// Events per row: each a new row (FINAL: its key's new version, replacing its row), with
    /// its deadline; one already past it doesn't count.
    fn batch_rows(&mut self, events: Vec<Event>, now: i128) -> Iteration {
        let keyed = self.stream.keyed.clone();
        let timed = self.stream.timed();
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
        let mut guard = self.shared.lock().unwrap();
        let shared = &mut *guard;
        for event in &events {
            let position = shared.at.0.entry(event.partition).or_insert(event.offset);
            *position = (*position).max(event.offset);
            let row = match self.stream.decode(event) {
                Ok(row) => row,
                Err(e) => {
                    tracing::warn!(topic = %event.topic, partition = event.partition,
                        offset = event.offset, error = %e, "skipping event");
                    continue;
                }
            };
            let (id, ver, deleted) = match &keyed {
                Some(keyed) => keyed.event(&row),
                None => (String::new(), Value::Null, false),
            };
            if keyed.is_some() {
                // an older version than the kept row's: ignored (ties go to the later)
                if let Some(k) = shared.kept.get(&id) {
                    if ver.compare(&k.ver) == Some(Ordering::Less) {
                        continue;
                    }
                }
                if let Some(old) = shared.forget(&id) {
                    shared.take_out(&self.plan, old, &mut touch);
                }
            }
            if deleted {
                continue;
            }
            let schedule = match timed {
                true => self.stream.schedule(&row, now),
                false => crate::plan::Schedule {
                    in_now: self.stream.matches(&row),
                    transitions: Vec::new(),
                },
            };
            if !schedule.in_now && schedule.transitions.is_empty() {
                continue;
            }
            shared.next_row += 1;
            let seq = shared.next_row;
            let id = match keyed {
                Some(_) => id,
                None => format!("#{seq}"),
            };
            let key: Vec<Json> = self.plan.keys.iter().map(|k| row[k.0].to_json()).collect();
            let args: Vec<Value> = self
                .plan
                .aggs
                .iter()
                .map(|a| a.arg.map_or(Value::Null, |c| row[c.0].clone()))
                .collect();
            let group = Json::Array(key).to_string();
            let kept = Kept {
                ver,
                in_group: schedule.in_now,
                transitions: schedule.transitions.into(),
                group,
                args,
                seq,
            };
            shared.admit(
                &self.plan,
                id,
                kept,
                keyed.is_some(),
                || self.new_states(),
                &mut touch,
            );
        }
        share_arrays(&self.plan, &shared.groups, &mut shared.result, &touched);
        let diffs = shared
            .result
            .changes(changes(&self.plan, &shared.groups, touched));
        Iteration {
            origin,
            diffs,
            at: shared.at.clone(),
        }
    }

    /// A batch of events (not empty): the touched groups' changes, as one iteration; `now` for
    /// time windows.
    pub fn batch(&mut self, events: Vec<Event>, now: i128) -> Iteration {
        if self.plan.per_row.is_some() {
            return self.batch_rows(events, now);
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
            let row = match self.stream.decode(event) {
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
        share_arrays(&self.plan, &shared.groups, &mut shared.result, &touched);
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

/// The touched groups' rows now, or `None` for the ones that are gone.
fn changes(
    plan: &NativeAggregates,
    groups: &HashMap<String, Group>,
    touched: Vec<String>,
) -> Vec<(String, Option<Row>)> {
    touched
        .into_iter()
        .map(|id| match groups.contains_key(&id) {
            true => {
                let (id, row) = row(plan, groups, id);
                (id, Some(row))
            }
            false => (id, None),
        })
        .collect()
}

/// A row's values into its group, by its identity.
fn add(plan: &NativeAggregates, group: &mut Group, args: &[Value], seq: u64) {
    for (j, (agg, state)) in plan.aggs.iter().zip(&mut group.states).enumerate() {
        state.update_row(agg.arg.map(|_| &args[j]), seq);
    }
    group.rows += 1;
}

/// The groups' array columns' element identities, to the result (before their rows).
fn share_arrays(
    plan: &NativeAggregates,
    groups: &HashMap<String, Group>,
    result: &mut GroupResult,
    ids: &[String],
) {
    for id in ids {
        let Some(group) = groups.get(id) else {
            continue;
        };
        let arrays: crate::aggregate::result::Arrays = plan
            .items
            .iter()
            .filter_map(|item| match item {
                NativeItem::Agg(j, name) => {
                    group.states[*j].identities().map(|ids| (name.clone(), ids))
                }
                NativeItem::Key(..) => None,
            })
            .collect();
        result.set_arrays(id, arrays);
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
