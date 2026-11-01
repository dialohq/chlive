//! Rows under a WHERE with `now()`, as time passes. Each row has a schedule (see
//! [`crate::plan::StreamPlan::schedule`]): whether it's in now, then the instants it goes out
//! or comes in. The engine keeps every row whose state can still change, in the result or
//! pending, and wakes at the earliest instant (plus the server's coalescing window).
//!
//! [`Timeline`] is for plain results: the rows shown, in the client's order (kept in step
//! with the diffs, so a row leaving is deleted by its index), and the rows pending. A row
//! shown is next due to go out; a pending one, to come in. [`KeyTimeline`] is for FINAL
//! results, by key: a key's new version replaces its schedule.

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};

use serde_json::{json, Value as Json};

use crate::diff::Diff;
use crate::plan::{DecodedRow, Schedule};
use crate::types::Value;

/// A row's remaining transitions, and the row (to come in again, or to be re-evaluated).
/// Rows from older checkpoints have none: they only leave.
#[derive(Debug, Clone, PartialEq)]
pub struct Entry {
    pub row: Option<DecodedRow>,
    pub transitions: VecDeque<(i128, bool)>,
}

impl Entry {
    /// `None` when the row's state never changes again.
    pub fn of(row: DecodedRow, schedule: Schedule) -> Option<Self> {
        (!schedule.transitions.is_empty()).then(|| Entry {
            row: Some(row),
            transitions: schedule.transitions.into(),
        })
    }

    fn next(&self) -> Option<i128> {
        self.transitions.front().map(|(t, _)| *t)
    }

    fn save(&self) -> Json {
        json!({
            "row": self.row.as_ref().map(|r| r.iter().map(Value::save).collect::<Vec<_>>()),
            "t": self.transitions.iter().map(|(t, b)| json!([t.to_string(), b])).collect::<Vec<_>>(),
        })
    }

    fn load(json: &Json) -> Result<Self, String> {
        let bad = || format!("checkpoint: bad schedule {json}");
        let row = match &json["row"] {
            Json::Null => None,
            r => Some(
                r.as_array()
                    .ok_or_else(bad)?
                    .iter()
                    .map(|v| Value::load(v).ok_or_else(bad))
                    .collect::<Result<_, _>>()?,
            ),
        };
        let transitions = json["t"]
            .as_array()
            .ok_or_else(bad)?
            .iter()
            .map(|t| {
                let at = t[0].as_str().and_then(|s| s.parse().ok()).ok_or_else(bad)?;
                Ok((at, t[1].as_bool().ok_or_else(bad)?))
            })
            .collect::<Result<_, String>>()?;
        Ok(Entry { row, transitions })
    }
}

/// A plain result's rows as time passes.
#[derive(Debug, Clone, Default)]
pub struct Timeline {
    /// Parallel to the client's rows: each one's schedule (`None`: it never changes).
    shown: Vec<Option<Entry>>,
    /// The shown rows' next instants, counted.
    next_shown: BTreeMap<i128, usize>,
    /// Rows out now that come in later, by their next instant and arrival.
    pending: BTreeMap<(i128, u64), Entry>,
    arrivals: u64,
}

impl PartialEq for Timeline {
    fn eq(&self, other: &Self) -> bool {
        self.shown == other.shown
            && self.pending.values().collect::<Vec<_>>()
                == other.pending.values().collect::<Vec<_>>()
    }
}

impl Timeline {
    fn count(&mut self, entry: &Option<Entry>, by: isize) {
        if let Some(t) = entry.as_ref().and_then(Entry::next) {
            let n = self.next_shown.entry(t).or_default();
            *n = n.checked_add_signed(by).expect("counted");
            if *n == 0 {
                self.next_shown.remove(&t);
            }
        }
    }

    /// A history row in the result, appended.
    pub fn show(&mut self, entry: Option<Entry>) {
        self.count(&entry, 1);
        self.shown.push(entry);
    }

    /// A row out now that comes in later.
    pub fn hold(&mut self, entry: Entry) {
        let t = entry.next().expect("a pending row comes in");
        self.arrivals += 1;
        self.pending.insert((t, self.arrivals), entry);
    }

    /// A row's diffs: its insert (with its schedule), and the rows they deleted.
    pub fn follow(&mut self, diffs: &[Diff], entry: Option<Entry>) {
        let mut entry = Some(entry);
        for d in diffs {
            match d {
                Diff::Insert { index, .. } => {
                    let e = entry.take().expect("one insert per row");
                    self.count(&e, 1);
                    match index {
                        None => self.shown.push(e),
                        Some(i) => self.shown.insert(*i, e),
                    }
                }
                Diff::Delete { index } => {
                    let gone = self.shown.remove(*index);
                    self.count(&gone, -1);
                }
                Diff::Update { .. } | Diff::DeleteId { .. } => {}
            }
        }
    }

    /// The earliest instant a row goes out or comes in.
    pub fn earliest(&self) -> Option<i128> {
        let shown = self.next_shown.keys().next().copied();
        let pending = self.pending.keys().next().map(|(t, _)| *t);
        match (shown, pending) {
            (Some(a), Some(b)) => Some(a.min(b)),
            (a, b) => a.or(b),
        }
    }

    /// What's due at `now`: the shown rows going out, highest index first (to delete in that
    /// order; taken out, and pending if they come back), and the pending rows coming in, in
    /// order (to insert, then [`Self::follow`] with what's left of their schedules).
    pub fn due(&mut self, now: i128) -> (Vec<usize>, Vec<Entry>) {
        let mut out = Vec::new();
        if self.next_shown.keys().next().is_some_and(|t| *t <= now) {
            for i in (0..self.shown.len()).rev() {
                if self.shown[i]
                    .as_ref()
                    .and_then(Entry::next)
                    .is_some_and(|t| t <= now)
                {
                    out.push(i);
                }
            }
            for &i in &out {
                let mut e = self.shown.remove(i).expect("due");
                self.count(&Some(e.clone()), -1);
                e.transitions.pop_front();
                if e.next().is_some() && e.row.is_some() {
                    self.hold(e);
                }
            }
        }
        let mut coming = Vec::new();
        while let Some(entry) = self.pending.first_entry() {
            if entry.key().0 > now {
                break;
            }
            let mut e = entry.remove();
            e.transitions.pop_front();
            coming.push(e);
        }
        (out, coming)
    }

    /// How many rows are shown.
    pub fn len(&self) -> usize {
        self.shown.len()
    }

    pub fn is_empty(&self) -> bool {
        self.shown.is_empty()
    }

    /// For a checkpoint.
    pub fn save(&self) -> Json {
        json!({
            "shown": self.shown.iter().map(|e| e.as_ref().map(Entry::save)).collect::<Vec<_>>(),
            "pending": self.pending.values().map(Entry::save).collect::<Vec<_>>(),
        })
    }

    /// From a checkpoint; also the older format, the shown rows' deadlines as strings.
    pub fn load(json: &Json) -> Result<Self, String> {
        let mut t = Timeline::default();
        if let Some(deadlines) = json.as_array() {
            for d in deadlines {
                let d: i128 = d
                    .as_str()
                    .and_then(|d| d.parse().ok())
                    .ok_or("checkpoint: bad deadline")?;
                t.show(Some(Entry {
                    row: None,
                    transitions: VecDeque::from([(d, false)]),
                }));
            }
            return Ok(t);
        }
        let bad = || "checkpoint: bad timeline".to_string();
        for e in json["shown"].as_array().ok_or_else(bad)? {
            t.show(match e {
                Json::Null => None,
                e => Some(Entry::load(e)?),
            });
        }
        for e in json["pending"].as_array().ok_or_else(bad)? {
            t.hold(Entry::load(e)?);
        }
        Ok(t)
    }
}

/// A FINAL key's latest version under a WHERE with `now()`.
#[derive(Debug, Clone, PartialEq)]
pub struct KeyEntry {
    pub ver: Value,
    pub row: DecodedRow,
    /// The event it came from (partition, offset).
    pub at: (i32, i64),
    pub transitions: VecDeque<(i128, bool)>,
}

/// A FINAL result's keys as time passes: each key whose state can still change, by its next
/// instant.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct KeyTimeline {
    keys: HashMap<String, KeyEntry>,
    next: BTreeSet<(i128, String)>,
}

impl KeyTimeline {
    /// The version a key's schedule is of, if it has one.
    pub fn ver(&self, id: &str) -> Option<&Value> {
        self.keys.get(id).map(|e| &e.ver)
    }

    /// A key's new version: its schedule replaces the old one's (none: it never changes).
    pub fn set(&mut self, id: &str, entry: Option<KeyEntry>) {
        if let Some(old) = self.keys.remove(id) {
            if let Some((t, _)) = old.transitions.front() {
                self.next.remove(&(*t, id.to_string()));
            }
        }
        if let Some(e) = entry.filter(|e| !e.transitions.is_empty()) {
            self.next.insert((e.transitions[0].0, id.to_string()));
            self.keys.insert(id.to_string(), e);
        }
    }

    pub fn earliest(&self) -> Option<i128> {
        self.next.first().map(|(t, _)| *t)
    }

    /// The keys due at `now`, in order: each one's entry and whether it's now in (it stays
    /// scheduled while transitions are left).
    pub fn due(&mut self, now: i128) -> Vec<(String, KeyEntry, bool)> {
        let mut due = Vec::new();
        while let Some((t, id)) = self.next.first().cloned() {
            if t > now {
                break;
            }
            self.next.pop_first();
            let mut e = self.keys.remove(&id).expect("scheduled");
            let (_, comes_in) = e.transitions.pop_front().expect("a transition");
            due.push((id.clone(), e.clone(), comes_in));
            self.set(&id, Some(e));
        }
        due
    }

    pub fn save(&self) -> Json {
        json!(self
            .keys
            .iter()
            .map(|(id, e)| json!({
                "id": id,
                "ver": e.ver.save(),
                "row": e.row.iter().map(Value::save).collect::<Vec<_>>(),
                "at": [e.at.0, e.at.1],
                "t": e.transitions.iter().map(|(t, b)| json!([t.to_string(), b])).collect::<Vec<_>>(),
            }))
            .collect::<Vec<_>>())
    }

    pub fn load(json: &Json) -> Result<Self, String> {
        let bad = || format!("checkpoint: bad key schedule {json}");
        let mut k = KeyTimeline::default();
        for e in json.as_array().ok_or_else(bad)? {
            let id = e["id"].as_str().ok_or_else(bad)?;
            let entry = Entry::load(&json!({"row": e["row"], "t": e["t"]}))?;
            k.set(
                id,
                Some(KeyEntry {
                    ver: Value::load(&e["ver"]).ok_or_else(bad)?,
                    row: entry.row.ok_or_else(bad)?,
                    at: (
                        e["at"][0].as_i64().ok_or_else(bad)? as i32,
                        e["at"][1].as_i64().ok_or_else(bad)?,
                    ),
                    transitions: entry.transitions,
                }),
            );
        }
        Ok(k)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Map;

    fn entry(n: i128, transitions: &[(i128, bool)]) -> Option<Entry> {
        Some(Entry {
            row: Some(vec![Value::Int(n)]),
            transitions: transitions.iter().copied().collect(),
        })
    }

    fn insert(index: Option<usize>) -> Diff {
        Diff::Insert {
            row: Map::new(),
            index,
            id: None,
        }
    }

    #[test]
    fn rows_go_out_and_come_in_at_their_instants() {
        let mut t = Timeline::default();
        t.show(entry(1, &[(10, false)]));
        t.show(None); // never changes
        t.show(entry(3, &[(20, false), (30, true)])); // out, then back
        t.hold(entry(4, &[(15, true), (40, false)]).unwrap());
        assert_eq!(t.earliest(), Some(10));
        let (out, coming) = t.due(9);
        assert!(out.is_empty() && coming.is_empty());
        let (out, coming) = t.due(10);
        assert_eq!((out, coming.len()), (vec![0], 0));
        // row 4 comes in, at the top (ordered); its schedule goes on
        let (out, mut coming) = t.due(15);
        assert_eq!((out.len(), coming.len()), (0, 1));
        let e = coming.pop().unwrap();
        assert_eq!(e.transitions, [(40, false)]);
        t.follow(&[insert(Some(0))], Some(e));
        assert_eq!(t.len(), 3);
        // row 3 goes out (index 2), pending until 30
        assert_eq!(t.due(20), (vec![2], vec![]));
        assert_eq!(t.earliest(), Some(30));
        let (_, coming) = t.due(30);
        assert_eq!(coming[0].row, Some(vec![Value::Int(3)]));
        assert!(coming[0].transitions.is_empty());
        // round trip, and the older format
        let back = Timeline::load(&t.save()).unwrap();
        assert_eq!(back, t);
        let old = Timeline::load(&json!(["5", "7"])).unwrap();
        assert_eq!(old.earliest(), Some(5));
    }

    #[test]
    fn keys_follow_their_latest_version() {
        let mut k = KeyTimeline::default();
        let e = |ver: i128, t: &[(i128, bool)]| KeyEntry {
            ver: Value::Int(ver),
            row: vec![],
            at: (0, ver as i64),
            transitions: t.iter().copied().collect(),
        };
        k.set("a", Some(e(1, &[(10, true), (20, false)])));
        k.set("b", Some(e(1, &[(5, false)])));
        k.set("b", Some(e(2, &[(50, false)]))); // replaced
        assert_eq!(k.earliest(), Some(10));
        let due = k.due(10);
        assert_eq!(due.len(), 1);
        assert_eq!((due[0].0.as_str(), due[0].2), ("a", true));
        assert_eq!(k.earliest(), Some(20));
        assert_eq!(KeyTimeline::load(&k.save()).unwrap(), k);
        k.set("a", None);
        assert_eq!(k.earliest(), Some(50));
    }
}
