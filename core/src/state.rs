//! A live result's state as events come, and the diffs each makes: placing rows by the plan's
//! shape (appended, ordered, in a LIMIT window, held back by OFFSET, by key for FINAL, or
//! scheduled by `now()`), seeded from the history. The engine drives it from ClickHouse and
//! Kafka; a client can run the same plan on its own rows.

use std::fmt;

use crate::diff::{Diff, Row};
use crate::event::Event;
use crate::keyed::{KeyedState, NewVersion};
use crate::order::{HeldWindow, OffsetState, OrderedState, SortKey, WindowState};
use crate::plan::{DecodedRow, StreamPlan};
use crate::rules::TopicId;
use crate::timeline::{Entry, KeyEntry, KeyTimeline, Timeline};
use crate::watermark::Watermark;

/// A row of the history (or a resume's seed) that doesn't fit the plan.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
#[error("{0}")]
pub struct StateError(pub String);

fn bad_history_row(e: impl fmt::Display) -> StateError {
    StateError(format!("history row: {e}"))
}

/// What a subscription keeps about its result to place new rows.
pub enum ResultState {
    /// Unordered, unlimited: every row is appended.
    Append,
    /// LIMIT without ORDER BY: the latest N rows.
    Window(WindowState),
    /// ORDER BY (and maybe LIMIT): the sort keys of the result, in tie groups.
    Ordered(OrderedState),
    /// ORDER BY with OFFSET: the sort keys of the top `m + n`, and the rows above the result.
    Offset(OffsetState),
    /// OFFSET without ORDER BY: the newest `m` rows held back, the window before them.
    Held(HeldWindow),
    /// FINAL on a ReplacingMergeTree: the result's keys and their versions.
    Keyed(KeyedState),
}

impl ResultState {
    /// Its state, for a checkpoint: `{"shape": …, …}`.
    pub fn save(&self) -> serde_json::Value {
        let (shape, mut state) = match self {
            ResultState::Append => ("append", serde_json::json!({})),
            ResultState::Window(w) => ("window", w.save()),
            ResultState::Ordered(o) => ("ordered", o.save()),
            ResultState::Offset(o) => ("offset", o.save()),
            ResultState::Held(h) => ("held", h.save()),
            ResultState::Keyed(k) => ("keyed", k.save()),
        };
        state["shape"] = shape.into();
        state
    }

    /// The plan's state, as saved: the shape must be the plan's.
    pub fn restore(plan: &StreamPlan, saved: &serde_json::Value) -> Result<Self, String> {
        let mut state = Self::for_plan(plan);
        let want = state.save()["shape"]
            .as_str()
            .unwrap_or_default()
            .to_string();
        let got = saved["shape"].as_str().unwrap_or_default();
        if got != want {
            return Err(format!("checkpoint of a {got} result for a {want} query"));
        }
        match &mut state {
            ResultState::Append => {}
            ResultState::Window(w) => w.load(saved)?,
            ResultState::Ordered(o) => o.load(saved)?,
            ResultState::Offset(o) => o.load(saved)?,
            ResultState::Held(h) => h.load(saved)?,
            ResultState::Keyed(k) => k.load(plan.keyed.as_ref().expect("a keyed plan"), saved)?,
        }
        Ok(state)
    }

    pub fn for_plan(plan: &StreamPlan) -> Self {
        if let Some(keyed) = &plan.keyed {
            let order = plan.order.as_ref().map(|o| o.keys.clone());
            return Self::Keyed(match keyed.window {
                Some(w) => KeyedState::with_window(w, order),
                None => KeyedState::new(order),
            });
        }
        match (&plan.order, plan.limit, plan.offset) {
            (Some(o), limit, Some(m)) => Self::Offset(OffsetState::new(o.keys.clone(), m, limit)),
            (None, limit, Some(m)) => Self::Held(HeldWindow::new(m, limit)),
            (Some(o), limit, None) => {
                Self::Ordered(OrderedState::with_limit(o.keys.clone(), limit))
            }
            (None, Some(n), None) => Self::Window(WindowState::new(n)),
            (None, None, None) => Self::Append,
        }
    }

    /// Where a new row goes: an insert (at an index with ORDER BY) and the delete it causes, or
    /// nothing when it falls outside a LIMIT. `key` is its sort key with ORDER BY.
    pub fn place(&mut self, row: Row, key: Option<SortKey>) -> Vec<Diff> {
        match self {
            ResultState::Append => vec![Diff::insert(row)],
            // newest first: in at the top; once full, the last row goes
            ResultState::Window(w) => match w.push() {
                None => Vec::new(),
                Some(full) => {
                    let mut diffs = vec![Diff::Insert {
                        row,
                        index: Some(0),
                        id: None,
                    }];
                    if full {
                        diffs.push(Diff::Delete {
                            index: w.limit() as usize,
                        });
                    }
                    diffs
                }
            },
            ResultState::Ordered(s) => match s.insert(key.expect("a sort key")) {
                None => Vec::new(),
                Some(p) => {
                    let mut diffs = vec![Diff::Insert {
                        row,
                        index: Some(p.index),
                        id: None,
                    }];
                    if let Some(index) = p.evicts {
                        diffs.push(Diff::Delete { index });
                    }
                    diffs
                }
            },
            ResultState::Offset(s) => s.insert(key.expect("a sort key"), row),
            ResultState::Held(w) => w.push(row),
            ResultState::Keyed(_) => unreachable!("FINAL rows are replaced, not placed"),
        }
    }

    /// The row at `index` left (its time window passed). Time windows run only on appended
    /// and ordered results.
    pub fn remove_at(&mut self, index: usize) {
        match self {
            ResultState::Append => {}
            ResultState::Ordered(o) => o.remove_at(index),
            _ => unreachable!("now() runs on appended and ordered results only"),
        }
    }

    /// A decoded event's diffs: a new row placed (when it matches the WHERE), or, FINAL, a new
    /// version of its key's row.
    pub fn event(&mut self, plan: &StreamPlan, row: &DecodedRow, at: (i32, i64)) -> Vec<Diff> {
        let matches = plan.matches(row);
        self.event_matching(plan, row, at, matches)
    }

    /// [`Self::event`], with whether the row is in the result decided by the caller (with
    /// `now()`: by its schedule).
    pub fn event_matching(
        &mut self,
        plan: &StreamPlan,
        row: &DecodedRow,
        at: (i32, i64),
        matches: bool,
    ) -> Vec<Diff> {
        match (self, &plan.keyed) {
            (ResultState::Keyed(state), Some(keyed)) => {
                let (id, ver, deleted) = keyed.event(row);
                let key = plan
                    .order
                    .as_ref()
                    .map(|o| o.event_key(row))
                    .unwrap_or_default();
                let v = NewVersion {
                    id,
                    ver,
                    key,
                    deleted,
                    matches,
                    at,
                };
                state.event(v, || plan.project(row))
            }
            (state, _) if matches => {
                let key = plan.order.as_ref().map(|o| o.event_key(row));
                state.place(plan.project(row), key)
            }
            _ => Vec::new(),
        }
    }
}

/// The history as one iteration. Seeds the state: with ORDER BY the sort keys, in ClickHouse's
/// order (dropping the hidden ORDER BY fields from the rows); with only a LIMIT the row count.
pub fn history_iteration(
    plan: &StreamPlan,
    state: &mut ResultState,
    at: &Watermark,
    mut rows: Vec<Row>,
) -> Result<Iteration, StateError> {
    match (state, &plan.order) {
        (ResultState::Ordered(state), Some(order)) => {
            for row in &mut rows {
                state.push_history(order.history_key(row).map_err(bad_history_row)?);
            }
        }
        // newest first: the history's arrival order (ClickHouse's) reversed
        (ResultState::Window(w), _) => {
            rows.iter().for_each(|_| w.push_history());
            rows.reverse();
        }
        (ResultState::Offset(state), Some(order)) => {
            // the rows above the result are kept, not sent
            let mut shown = Vec::new();
            for mut row in rows {
                let key = order.history_key(&mut row).map_err(bad_history_row)?;
                shown.extend(state.push_history(key, row));
            }
            rows = shown;
        }
        (ResultState::Held(w), _) => {
            rows = w.history(rows);
            rows.reverse();
        }
        (ResultState::Keyed(state), order) => {
            let keyed = plan.keyed.as_ref().expect("a keyed plan");
            let rows = rows
                .into_iter()
                .map(|mut row| {
                    let key = match order {
                        Some(o) => o.history_key(&mut row).map_err(bad_history_row)?,
                        None => Vec::new(),
                    };
                    let (id, ver) = keyed.history_row(&mut row).map_err(bad_history_row)?;
                    Ok((id, ver, key, row))
                })
                .collect::<Result<_, StateError>>()?;
            let diffs = state.history(rows);
            return Ok(Iteration {
                origin: Origin::History,
                diffs,
                at: at.clone(),
            });
        }
        _ => {}
    }
    Ok(Iteration {
        origin: Origin::History,
        diffs: rows.into_iter().map(Diff::insert).collect(),
        at: at.clone(),
    })
}

/// Resume: seed the state as the client's result was at its cursor, then place the rows it is
/// missing, exactly as new events would be.
pub fn catch_up_iteration(
    plan: &StreamPlan,
    state: &mut ResultState,
    at: &Watermark,
    seed: Option<Vec<Row>>,
    own: Vec<Event>,
) -> Result<Iteration, StateError> {
    match (&mut *state, &plan.order, seed) {
        (ResultState::Ordered(s), Some(order), Some(mut keys)) => {
            for k in &mut keys {
                s.push_history(order.history_key(k).map_err(bad_history_row)?);
            }
        }
        (ResultState::Window(_), _, Some(count)) => {
            let n = count
                .first()
                .and_then(|r| r.get("n"))
                .and_then(|n| n.as_u64())
                .ok_or_else(|| bad_history_row("no row count"))?;
            *state = ResultState::Window(WindowState::with_len(plan.limit.unwrap_or(0), n));
        }
        _ => {}
    }
    // the client's own events after the snapshot: into the state, not sent
    for event in own {
        if let Ok(row) = plan.decode(&event) {
            state.event(plan, &row, (event.partition, event.offset));
        }
    }
    // the client has everything up to its cursor: what it's missing comes from the stream
    Ok(Iteration {
        origin: Origin::CatchUp,
        diffs: Vec::new(),
        at: at.clone(),
    })
}

/// One event: decode by column types, filter, project, and place it. An insert and the delete
/// it causes come in the same iteration, insert first, so they always land in the same diff.
/// The rows of a result with `now()` in its WHERE, as time passes.
#[derive(Debug, Clone)]
pub enum Timed {
    /// Plain rows: by their place in the result.
    Rows(Timeline),
    /// FINAL: by key.
    Keys(KeyTimeline),
}

impl Timed {
    pub fn for_plan(plan: &StreamPlan) -> Self {
        match plan.keyed {
            Some(_) => Timed::Keys(KeyTimeline::default()),
            None => Timed::Rows(Timeline::default()),
        }
    }

    pub fn save(&self) -> serde_json::Value {
        match self {
            Timed::Rows(t) => t.save(),
            Timed::Keys(k) => k.save(),
        }
    }

    /// From a checkpoint's state (also its older `deadlines`).
    pub fn load(plan: &StreamPlan, state: &serde_json::Value) -> Result<Self, String> {
        let saved = match &state["timeline"] {
            serde_json::Value::Null => &state["deadlines"],
            t => t,
        };
        Ok(match plan.keyed {
            Some(_) => Timed::Keys(KeyTimeline::load(saved)?),
            None => Timed::Rows(Timeline::load(saved)?),
        })
    }

    pub fn earliest(&self) -> Option<i128> {
        match self {
            Timed::Rows(t) => t.earliest(),
            Timed::Keys(k) => k.earliest(),
        }
    }

    /// The rows due at `now` out of the result and into it: their diffs.
    pub fn due(&mut self, plan: &StreamPlan, state: &mut ResultState, now: i128) -> Vec<Diff> {
        let mut diffs = Vec::new();
        match self {
            Timed::Rows(t) => {
                let (out, coming) = t.due(now);
                for index in out {
                    state.remove_at(index);
                    diffs.push(Diff::Delete { index });
                }
                for e in coming {
                    let row = e.row.clone().expect("a row coming in has its values");
                    let d = state.event_matching(plan, &row, (0, -1), true);
                    t.follow(&d, (!e.transitions.is_empty()).then_some(e));
                    diffs.extend(d);
                }
            }
            Timed::Keys(k) => {
                for (_, e, comes_in) in k.due(now) {
                    diffs.extend(state.event_matching(plan, &e.row, e.at, comes_in));
                }
            }
        }
        diffs
    }
}

/// The history of a plan with `now()`: each row's schedule at `now`; the rows in now as usual,
/// the ones that come in later held, the others (fetched as possible) dropped.
pub fn timed_history_iteration(
    plan: &StreamPlan,
    state: &mut ResultState,
    at: &Watermark,
    rows: Vec<Row>,
    timed: &mut Timed,
    now: i128,
) -> Result<Iteration, StateError> {
    let mut shown = Vec::new();
    for mut row in rows {
        let decoded = plan.history_decode(&mut row).map_err(bad_history_row)?;
        let schedule = plan.schedule(&decoded, now);
        let in_now = schedule.in_now;
        match timed {
            Timed::Rows(t) => {
                let entry = Entry::of(decoded, schedule);
                match (in_now, entry) {
                    (true, entry) => t.show(entry),
                    (false, Some(entry)) => t.hold(entry),
                    (false, None) => {}
                }
            }
            Timed::Keys(k) => {
                let keyed = plan.keyed.as_ref().expect("a keyed plan");
                let (id, ver) = keyed
                    .history_row(&mut row.clone())
                    .map_err(bad_history_row)?;
                k.set(
                    &id,
                    Some(KeyEntry {
                        ver,
                        row: decoded,
                        at: (0, -1),
                        transitions: schedule.transitions.into(),
                    }),
                );
            }
        }
        if in_now {
            shown.push(row);
        }
    }
    history_iteration(plan, state, at, shown)
}

/// An event of a plan with `now()`: in the result if it's in now, held if it comes in later
/// (FINAL: a key's new version replaces its schedule).
pub fn timed_event_iteration(
    plan: &StreamPlan,
    state: &mut ResultState,
    at: &mut Watermark,
    event: Event,
    now: i128,
    timed: &mut Timed,
) -> Iteration {
    let place = (event.partition, event.offset);
    let diffs = match plan.decode(&event) {
        Ok(row) => match timed {
            Timed::Rows(t) => {
                let schedule = plan.schedule(&row, now);
                let in_now = schedule.in_now;
                let entry = Entry::of(row.clone(), schedule);
                match (in_now, entry) {
                    (true, entry) => {
                        let diffs = state.event_matching(plan, &row, place, true);
                        t.follow(&diffs, entry);
                        diffs
                    }
                    (false, Some(entry)) => {
                        t.hold(entry);
                        Vec::new()
                    }
                    (false, None) => Vec::new(),
                }
            }
            Timed::Keys(k) => {
                let keyed = plan.keyed.as_ref().expect("a keyed plan");
                let (id, ver, deleted) = keyed.event(&row);
                // an older version than the key's latest (in the result, or scheduled): ignored
                let latest = match (k.ver(&id), &*state) {
                    (Some(v), _) => Some(v.clone()),
                    (None, ResultState::Keyed(s)) => s.version(&id).cloned(),
                    _ => None,
                };
                if latest.is_some_and(|l| ver.compare(&l) == Some(std::cmp::Ordering::Less)) {
                    Vec::new()
                } else {
                    let schedule = match deleted {
                        true => Default::default(),
                        false => plan.schedule(&row, now),
                    };
                    let diffs = state.event_matching(plan, &row, place, schedule.in_now);
                    k.set(
                        &id,
                        Some(KeyEntry {
                            ver,
                            row,
                            at: place,
                            transitions: schedule.transitions.into(),
                        }),
                    );
                    diffs
                }
            }
        },
        Err(e) => {
            tracing::warn!(
                topic = %event.topic,
                partition = event.partition,
                offset = event.offset,
                error = %e,
                "skipping event"
            );
            Vec::new()
        }
    };
    let position = at.0.entry(event.partition).or_insert(event.offset);
    *position = (*position).max(event.offset);
    Iteration {
        origin: Origin::Event {
            topic: event.topic,
            partition: event.partition,
            offset: event.offset,
        },
        diffs,
        at: at.clone(),
    }
}

pub fn event_iteration(
    plan: &StreamPlan,
    state: &mut ResultState,
    at: &mut Watermark,
    event: Event,
) -> Iteration {
    let mut diffs = match plan.decode(&event) {
        Ok(row) => state.event(plan, &row, (event.partition, event.offset)),
        Err(e) => {
            // ClickHouse's ingestion skips this message too
            tracing::warn!(
                topic = %event.topic,
                partition = event.partition,
                offset = event.offset,
                error = %e,
                "skipping event"
            );
            Vec::new()
        }
    };
    let position = at.0.entry(event.partition).or_insert(event.offset);
    *position = (*position).max(event.offset);
    // a FINAL window's refill, waiting for the stream to reach its snapshot
    if let ResultState::Keyed(k) = state {
        diffs.extend(k.advanced(at));
    }
    Iteration {
        origin: Origin::Event {
            topic: event.topic,
            partition: event.partition,
            offset: event.offset,
        },
        diffs,
        at: at.clone(),
    }
}

/// Where an iteration's diffs came from.
#[derive(Debug, Clone, PartialEq)]
pub enum Origin {
    /// The initial result, from ClickHouse.
    History,
    /// Resume: what the client is missing since its cursor, from ClickHouse.
    CatchUp,
    /// One event of a subscribed topic.
    Event {
        topic: TopicId,
        partition: i32,
        offset: i64,
    },
    /// FINAL with LIMIT: rows from a newer snapshot replacing departures.
    Refill,
    /// `now()`: rows whose time window the clock passed, leaving.
    Clock,
}

#[derive(Debug, Clone, PartialEq)]
pub struct Iteration {
    pub origin: Origin,
    /// Empty when the event was not relevant to the query.
    pub diffs: Vec<Diff>,
    /// The cursor after this iteration: per partition, the offset up to which the result is
    /// complete (whether events matched or not). Resume from it with [`cursor::encode`] and
    /// [`Engine::subscribe_from`].
    pub at: Watermark,
}

/// One line per diff: `+ {row}` for the history (followed by `history: N rows`), and
/// `<topic>/<partition>:<offset> + {row}` for events. Irrelevant events print nothing.
impl fmt::Display for Iteration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.origin {
            Origin::History => {
                for d in &self.diffs {
                    writeln!(f, "{d}")?;
                }
                write!(f, "history: {} rows", self.diffs.len())
            }
            Origin::CatchUp => {
                for d in &self.diffs {
                    writeln!(f, "{d}")?;
                }
                write!(f, "caught up: {} changes", self.diffs.len())
            }
            Origin::Refill => {
                for d in &self.diffs {
                    writeln!(f, "{d}")?;
                }
                write!(f, "refilled: {} changes", self.diffs.len())
            }
            Origin::Clock => {
                let lines: Vec<String> = self.diffs.iter().map(|d| format!("clock: {d}")).collect();
                write!(f, "{}", lines.join("\n"))
            }
            Origin::Event {
                topic,
                partition,
                offset,
            } => {
                let mut first = true;
                for d in &self.diffs {
                    if !first {
                        writeln!(f)?;
                    }
                    first = false;
                    write!(f, "{topic}/{partition}:{offset} {d}")?;
                }
                Ok(())
            }
        }
    }
}
