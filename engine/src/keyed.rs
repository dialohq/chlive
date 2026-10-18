//! FINAL results on a ReplacingMergeTree: one row per key, its latest version, as ClickHouse's
//! `SELECT … FINAL` has it. Rows have ids (their keys); each event is a new version of its key,
//! so it inserts, updates (the whole row) or deletes the key's row.
//!
//! Versions are trusted to arrive in order: an event replaces its key's row unless its version
//! is lower than the row's (ties, and tables without a version column, go to the later event,
//! as ClickHouse keeps the last inserted). That holds when each key's versions only grow and
//! all of them are in one Kafka partition (a keyed topic). Only the result's keys are kept,
//! with their versions.
//!
//! With `LIMIT n` (no ORDER BY, so any n rows will do) the result is the n most recently active
//! matching rows, newest first, and a buffer of more: a matching version of a key that isn't
//! kept comes in at the top (the last visible row slides into the buffer, whose last one is let
//! go), and a visible row that leaves is replaced by the buffer's first. When departures drain
//! the buffer, it's refilled from a newer snapshot (see [`KeyedState::refill`]).

use std::cmp::Ordering;
use std::collections::{HashMap, VecDeque};
use std::time::{Duration, Instant};

use serde_json::{json, Value as Json};

use crate::diff::{Diff, Row};
use crate::order::{KeyOrder, Ranking, SortKey};
use crate::plan::{KeyedPlan, KeyedWindowPlan};
use crate::types::{decode, Value};
use crate::watermark::Watermark;

/// FINAL with LIMIT: how many rows past the window are kept to replace departures.
pub const BUFFER: u64 = 2000;
/// Departures (deleted, or no longer matching) remembered to vouch for a refill's candidates.
const GONE_LOG: usize = 100_000;
/// After a refill that couldn't be used, how long until the next.
const REFILL_RETRY: Duration = Duration::from_secs(1);

/// A new version of a key, from an event at `at` (partition, offset).
pub struct NewVersion {
    pub id: String,
    pub ver: Value,
    /// Its ORDER BY values (empty without ORDER BY).
    pub key: SortKey,
    pub deleted: bool,
    /// Whether it matches the WHERE.
    pub matches: bool,
    pub at: (i32, i64),
}

#[derive(Debug, Clone, Default)]
pub struct KeyedState {
    /// The result's rows by id: their versions (`Null` without a version column).
    versions: HashMap<String, Value>,
    /// With ORDER BY: the result's rows in order.
    ranking: Option<Ranking>,
    /// With LIMIT: the window and its buffer.
    window: Option<Box<Window>>,
    /// Events older than their key's row, ignored.
    stale: u64,
}

/// FINAL with `LIMIT n`: the visible rows and the buffer, newest first, and the rows of both
/// (a visible row can slide into the buffer and come back). `versions` has every kept id.
#[derive(Debug, Clone)]
struct Window {
    limit: usize,
    capacity: usize,
    visible: VecDeque<String>,
    buffered: VecDeque<String>,
    rows: HashMap<String, Row>,
    /// The kept rows are every matching row (nothing past them to refill from).
    exhaustive: bool,
    gone: Gone,
    /// A refill from a snapshot the stream hasn't reached yet: applied once it has.
    pending: Option<Refill>,
    /// A refill was asked for and hasn't come.
    requested: bool,
    /// After a refill: not before this.
    retry_at: Option<Instant>,
    /// The snapshot the last refill came from (the next needs a newer one).
    last_refill: Option<Watermark>,
    /// Each kept row's latest event, and the newest event of a row let go: a refill from a
    /// snapshot before it can't have that row, so it can't say it has every row.
    seen: HashMap<String, (i32, i64)>,
    let_go: Watermark,
}

/// A refill: one row per key from a snapshot at `at` (id, version, row), and whether it had
/// every matching row there.
#[derive(Debug, Clone)]
pub struct Refill {
    pub rows: Vec<(String, Value, Row)>,
    pub at: Watermark,
    pub complete: bool,
}

/// The latest departure of each key, since `from`: a version deleted, or not matching, or a
/// kept row leaving. Bounded; what's let go is remembered per partition.
#[derive(Debug, Clone, Default)]
struct Gone {
    last: HashMap<String, (i32, i64)>,
    order: VecDeque<(String, i32, i64)>,
    /// Per partition, the newest offset let go.
    dropped: HashMap<i32, i64>,
    /// Where the log starts (a restore's position; before it, nothing is known).
    from: Watermark,
}

impl Gone {
    fn record(&mut self, id: &str, (partition, offset): (i32, i64)) {
        self.last.insert(id.to_string(), (partition, offset));
        self.order.push_back((id.to_string(), partition, offset));
        while self.order.len() > GONE_LOG {
            let (id, p, o) = self.order.pop_front().expect("not empty");
            if self.last.get(&id) == Some(&(p, o)) {
                self.last.remove(&id);
            }
            let d = self.dropped.entry(p).or_insert(o);
            *d = (*d).max(o);
        }
    }

    /// Whether the key left after `w`.
    fn since(&self, id: &str, w: &Watermark) -> bool {
        self.last.get(id).is_some_and(|(p, o)| *o > w.get(*p))
    }

    /// Whether the log knows every departure after `w`.
    fn covers(&self, w: &Watermark) -> bool {
        w.covers(&self.from) && self.dropped.iter().all(|(p, o)| *o <= w.get(*p))
    }
}

impl Window {
    fn new(plan: KeyedWindowPlan) -> Self {
        Self {
            limit: plan.limit as usize,
            capacity: plan.buffer as usize,
            visible: VecDeque::new(),
            buffered: VecDeque::new(),
            rows: HashMap::new(),
            exhaustive: true,
            gone: Gone::default(),
            pending: None,
            requested: false,
            retry_at: None,
            last_refill: None,
            seen: HashMap::new(),
            let_go: Watermark::default(),
        }
    }

    /// Fewer than this many buffered rows: time for a refill.
    fn low(&self) -> usize {
        self.capacity / 4
    }

    /// Fill the visible rows from the buffer (inserts at the end).
    fn promote(&mut self, diffs: &mut Vec<Diff>) {
        while self.visible.len() < self.limit {
            let Some(id) = self.buffered.pop_front() else {
                break;
            };
            self.visible.push_back(id.clone());
            diffs.push(Diff::Insert {
                row: self.rows[&id].clone(),
                index: Some(self.visible.len() - 1),
                id: Some(id),
            });
        }
    }
}

impl KeyedState {
    /// With ORDER BY, rows are kept in its order: each insert and move has an index.
    pub fn new(order: Option<KeyOrder>) -> Self {
        Self {
            ranking: order.map(Ranking::new),
            ..Self::default()
        }
    }

    /// With `LIMIT n` (no ORDER BY): the newest n rows, and a buffer.
    pub fn with_window(plan: KeyedWindowPlan) -> Self {
        Self {
            window: Some(Box::new(Window::new(plan))),
            ..Self::default()
        }
    }

    /// The history: one row per key, its latest version (FINAL left out deleted ones), as
    /// (id, version, sort key, row), in ClickHouse's order. The inserts come in result order.
    pub fn history(&mut self, rows: Vec<(String, Value, SortKey, Row)>) -> Vec<Diff> {
        let insert = |id: String, row: Row| Diff::Insert {
            row,
            index: None,
            id: Some(id),
        };
        if let Some(w) = &mut self.window {
            // the first n are the result, the rest the buffer
            w.exhaustive = rows.len() < w.limit + w.capacity;
            let mut diffs = Vec::new();
            for (id, ver, _, row) in rows {
                self.versions.insert(id.clone(), ver);
                if w.visible.len() < w.limit {
                    w.visible.push_back(id.clone());
                    diffs.push(insert(id.clone(), row.clone()));
                } else {
                    w.buffered.push_back(id.clone());
                }
                w.rows.insert(id, row);
            }
            return diffs;
        }
        let Some(ranking) = &mut self.ranking else {
            return rows
                .into_iter()
                .map(|(id, ver, _, row)| {
                    self.versions.insert(id.clone(), ver);
                    insert(id, row)
                })
                .collect();
        };
        let mut by_id: HashMap<String, Row> = HashMap::with_capacity(rows.len());
        let mut keys = Vec::with_capacity(rows.len());
        for (id, ver, key, row) in rows {
            self.versions.insert(id.clone(), ver);
            by_id.insert(id.clone(), row);
            keys.push((id, key));
        }
        ranking.history(keys);
        ranking
            .iter()
            .map(|e| insert(e.id.clone(), by_id.remove(&e.id).expect("a history row")))
            .collect()
    }

    /// A new version of a key; `row` renders it.
    pub fn event(&mut self, v: NewVersion, row: impl FnOnce() -> Row) -> Vec<Diff> {
        let NewVersion {
            id,
            ver,
            key,
            deleted,
            matches,
            at,
        } = v;
        let shown = !deleted && matches;
        if let Some(old) = self.versions.get(&id) {
            if older(&ver, old) {
                self.stale += 1;
                tracing::debug!(id, ver = ?ver, row_ver = ?old, stale = self.stale, "an older version than the row's; ignored");
                return Vec::new();
            }
        }
        if self.window.is_some() {
            return self.window_event(id, ver, shown, at, row);
        }
        match self.versions.get_mut(&id) {
            Some(_) if !shown => {
                self.versions.remove(&id);
                if let Some(r) = &mut self.ranking {
                    r.remove(&id);
                }
                vec![Diff::DeleteId { id }]
            }
            Some(old) => {
                *old = ver;
                // a new sort key moves the row: out, then after its new ties
                let index = self.ranking.as_mut().and_then(|r| {
                    let same = r
                        .key(&id)
                        .is_some_and(|k| r.order().cmp(k, &key) == Ordering::Equal);
                    if same {
                        return None;
                    }
                    let from = r.remove(&id);
                    let to = r.place(&id, key);
                    (from != Some(to)).then_some(to)
                });
                vec![Diff::Update {
                    id,
                    set: row(),
                    index,
                }]
            }
            None if shown => {
                self.versions.insert(id.clone(), ver);
                let index = self.ranking.as_mut().map(|r| r.place(&id, key));
                vec![Diff::Insert {
                    row: row(),
                    index,
                    id: Some(id),
                }]
            }
            None => Vec::new(),
        }
    }

    /// A new version, with LIMIT: kept rows are updated, or leave (a visible one replaced from
    /// the buffer); a matching key not kept comes in at the top.
    fn window_event(
        &mut self,
        id: String,
        ver: Value,
        shown: bool,
        at: (i32, i64),
        row: impl FnOnce() -> Row,
    ) -> Vec<Diff> {
        let w = self.window.as_mut().expect("a window");
        let mut diffs = Vec::new();
        let visible = w.visible.iter().position(|x| *x == id);
        let kept = visible.is_some() || self.versions.contains_key(&id);
        match (kept, shown) {
            (true, true) => {
                let row = row();
                self.versions.insert(id.clone(), ver);
                w.seen.insert(id.clone(), at);
                w.rows.insert(id.clone(), row.clone());
                if visible.is_some() {
                    diffs.push(Diff::Update {
                        id,
                        set: row,
                        index: None,
                    });
                }
            }
            (true, false) => {
                self.versions.remove(&id);
                w.rows.remove(&id);
                w.seen.remove(&id);
                w.gone.record(&id, at);
                match visible {
                    Some(i) => {
                        w.visible.remove(i);
                        diffs.push(Diff::DeleteId { id });
                        w.promote(&mut diffs);
                    }
                    None => w.buffered.retain(|x| *x != id),
                }
            }
            (false, true) if w.limit > 0 => {
                let row = row();
                self.versions.insert(id.clone(), ver);
                w.seen.insert(id.clone(), at);
                w.rows.insert(id.clone(), row.clone());
                w.visible.push_front(id.clone());
                diffs.push(Diff::Insert {
                    row,
                    index: Some(0),
                    id: Some(id),
                });
                if w.visible.len() > w.limit {
                    // the last visible row slides into the buffer
                    let last = w.visible.pop_back().expect("over the limit");
                    diffs.push(Diff::Delete { index: w.limit });
                    w.buffered.push_front(last);
                    if w.buffered.len() > w.capacity {
                        // let go: it still matches, but it isn't kept any more
                        let out = w.buffered.pop_back().expect("over capacity");
                        self.versions.remove(&out);
                        w.rows.remove(&out);
                        if let Some((p, o)) = w.seen.remove(&out) {
                            let l = w.let_go.0.entry(p).or_insert(o);
                            *l = (*l).max(o);
                        }
                        w.exhaustive = false;
                    }
                }
            }
            (false, true) => {}
            (false, false) => w.gone.record(&id, at),
        }
        diffs
    }

    /// With LIMIT: whether to refill the buffer now (asked once, until it comes).
    pub fn wants_refill(&mut self) -> bool {
        let Some(w) = &mut self.window else {
            return false;
        };
        let low = w.buffered.len() < w.low() || w.visible.len() < w.limit;
        let wanted = !w.exhaustive
            && low
            && !w.requested
            && w.pending.is_none()
            && w.retry_at.is_none_or(|t| Instant::now() >= t);
        if wanted {
            w.requested = true;
        }
        wanted
    }

    /// The snapshot the last refill came from: a refill needs a newer one.
    pub fn last_refill(&self) -> Option<Watermark> {
        self.window.as_ref().and_then(|w| w.last_refill.clone())
    }

    /// A refill couldn't be fetched, or there was no newer snapshot: try again later.
    pub fn refill_failed(&mut self) {
        if let Some(w) = &mut self.window {
            w.requested = false;
            w.retry_at = Some(Instant::now() + REFILL_RETRY);
        }
    }

    /// A refill from a snapshot, with the stream at `position`. Its rows were right at the
    /// snapshot; since then, a key's matching version made it kept, and a departure is in the
    /// log, so a candidate neither kept nor gone since is still a matching row as it was. Not
    /// before the stream reaches the snapshot (then it waits), nor when the log has let go of
    /// departures after it (then it's dropped, to be asked for again).
    pub fn refill(&mut self, refill: Refill, position: &Watermark) -> Vec<Diff> {
        let Some(w) = &mut self.window else {
            return Vec::new();
        };
        w.requested = false;
        if !position.covers(&refill.at) {
            w.pending = Some(refill);
            return Vec::new();
        }
        if !w.gone.covers(&refill.at) {
            tracing::info!(at = %refill.at.describe(), "a refill from a snapshot older than the departures known: asking again");
            w.retry_at = Some(Instant::now() + REFILL_RETRY);
            return Vec::new();
        }
        w.last_refill = Some(refill.at.clone());
        // every matching row: the snapshot had all of them, none was let go after it, and
        // they all fit
        let mut all = refill.complete && refill.at.covers(&w.let_go);
        for (id, ver, row) in refill.rows {
            if self.versions.contains_key(&id) || w.gone.since(&id, &refill.at) {
                continue;
            }
            if w.buffered.len() >= w.capacity {
                all = false;
                break;
            }
            w.buffered.push_back(id.clone());
            self.versions.insert(id.clone(), ver);
            w.rows.insert(id, row);
        }
        w.exhaustive = all;
        w.retry_at = Some(Instant::now() + REFILL_RETRY);
        let mut diffs = Vec::new();
        w.promote(&mut diffs);
        diffs
    }

    /// After an event: a pending refill, once the stream has reached its snapshot.
    pub fn advanced(&mut self, position: &Watermark) -> Vec<Diff> {
        let pending = match &mut self.window {
            Some(w) if w.pending.as_ref().is_some_and(|r| position.covers(&r.at)) => {
                w.pending.take()
            }
            _ => None,
        };
        match pending {
            Some(r) => self.refill(r, position),
            None => Vec::new(),
        }
    }

    /// Restored from a checkpoint at `at`: departures before it aren't known.
    pub fn restored_at(&mut self, at: &Watermark) {
        if let Some(w) = &mut self.window {
            w.gone.from = at.clone();
            // rows let go before it aren't known either
            w.let_go = at.clone();
        }
    }

    pub fn len(&self) -> usize {
        self.versions.len()
    }

    pub fn is_empty(&self) -> bool {
        self.versions.is_empty()
    }

    /// For a checkpoint: `{"versions": {id: version}}`, with ORDER BY and `"ranking"`.
    pub fn save(&self) -> Json {
        let versions: serde_json::Map<String, Json> = self
            .versions
            .iter()
            .map(|(id, v)| (id.clone(), v.to_json()))
            .collect();
        let mut json = json!({ "versions": versions });
        if let Some(r) = &self.ranking {
            json["ranking"] = r.save();
        }
        if let Some(w) = &self.window {
            json["window"] = json!({
                "visible": w.visible,
                "buffered": w.buffered,
                "rows": w.rows,
                "exhaustive": w.exhaustive,
            });
        }
        json
    }

    /// Back to a saved state, its versions read by the plan's version type.
    pub fn load(&mut self, plan: &KeyedPlan, json: &Json) -> Result<(), String> {
        let saved = json["versions"]
            .as_object()
            .ok_or("a keyed checkpoint without versions")?;
        self.versions = saved
            .iter()
            .map(|(id, v)| {
                let ver = match &plan.ver {
                    Some(c) => decode(Some(v), &c.ty).map_err(|e| e.to_string())?,
                    None => Value::Null,
                };
                Ok((id.clone(), ver))
            })
            .collect::<Result<_, String>>()?;
        if let Some(r) = &mut self.ranking {
            r.load(&json["ranking"])?;
        }
        if let Some(w) = &mut self.window {
            let bad = |what: &str| format!("checkpoint: bad keyed window ({what})");
            let saved = &json["window"];
            let ids = |name: &str| -> Result<VecDeque<String>, String> {
                saved[name]
                    .as_array()
                    .ok_or_else(|| bad(name))?
                    .iter()
                    .map(|id| id.as_str().map(String::from).ok_or_else(|| bad(name)))
                    .collect()
            };
            w.visible = ids("visible")?;
            w.buffered = ids("buffered")?;
            w.rows = saved["rows"]
                .as_object()
                .ok_or_else(|| bad("rows"))?
                .iter()
                .map(|(id, r)| {
                    Ok((
                        id.clone(),
                        r.as_object().ok_or_else(|| bad("rows"))?.clone(),
                    ))
                })
                .collect::<Result<_, String>>()?;
            w.exhaustive = saved["exhaustive"]
                .as_bool()
                .ok_or_else(|| bad("exhaustive"))?;
        }
        Ok(())
    }
}

/// Whether `ver` is lower than the row's `old` (never without a version column).
fn older(ver: &Value, old: &Value) -> bool {
    match (ver, old) {
        (Value::Null, _) | (_, Value::Null) => false,
        _ => ver.compare(old) == Some(Ordering::Less),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(v: i64) -> Row {
        json!({ "v": v }).as_object().unwrap().clone()
    }

    fn lines(diffs: impl IntoIterator<Item = Vec<Diff>>) -> Vec<String> {
        diffs.into_iter().flatten().map(|d| d.to_string()).collect()
    }

    /// A new version from an event at (0, 0).
    fn ev(
        s: &mut KeyedState,
        id: &str,
        ver: Value,
        key: SortKey,
        deleted: bool,
        matches: bool,
        row: Row,
    ) -> Vec<Diff> {
        let v = NewVersion {
            id: id.to_string(),
            ver,
            key,
            deleted,
            matches,
            at: (0, 0),
        };
        s.event(v, || row)
    }

    const K: &str = "[\"k\"]";

    #[test]
    fn new_versions_insert_update_and_delete_their_row() {
        let mut s = KeyedState::default();
        let v = |n| Value::Int(n);
        let out = lines([
            // not in the result: matching inserts, the rest does nothing
            ev(&mut s, K, v(1), vec![], false, false, row(1)),
            ev(&mut s, K, v(2), vec![], true, true, row(2)),
            ev(&mut s, K, v(3), vec![], false, true, row(3)),
            // in the result: a matching version updates, a tie too (the later wins)
            ev(&mut s, K, v(4), vec![], false, true, row(4)),
            ev(&mut s, K, v(4), vec![], false, true, row(5)),
            // an older one is ignored
            ev(&mut s, K, v(2), vec![], true, true, row(6)),
            // no longer matching: deleted; a deleted version likewise
            ev(&mut s, K, v(5), vec![], false, false, row(7)),
            ev(&mut s, K, v(6), vec![], false, true, row(8)),
            ev(&mut s, K, v(7), vec![], true, true, row(9)),
            // gone: a deleted key's later versions come back
            ev(&mut s, K, v(1), vec![], false, true, row(10)),
        ]);
        assert_eq!(
            out,
            [
                "+ [\"k\"] {\"v\":3}",
                "~ [\"k\"] {\"v\":4}",
                "~ [\"k\"] {\"v\":5}",
                "- [\"k\"]",
                "+ [\"k\"] {\"v\":8}",
                "- [\"k\"]",
                "+ [\"k\"] {\"v\":10}",
            ]
        );
        assert_eq!(s.stale, 1);
        assert_eq!(s.len(), 1);
    }

    #[test]
    fn without_a_version_column_the_later_event_wins() {
        let mut s = KeyedState::default();
        let out = lines([
            ev(&mut s, K, Value::Null, vec![], false, true, row(1)),
            ev(&mut s, K, Value::Null, vec![], false, true, row(2)),
            ev(&mut s, K, Value::Null, vec![], true, true, row(3)),
        ]);
        assert_eq!(
            out,
            ["+ [\"k\"] {\"v\":1}", "~ [\"k\"] {\"v\":2}", "- [\"k\"]"]
        );
        assert!(s.is_empty());
    }

    #[test]
    fn history_rows_and_checkpoints() {
        use crate::binder::ColumnId;
        use crate::plan::KeyedColumn;
        use crate::types::Type;
        let mut s = KeyedState::default();
        assert_eq!(
            s.history(vec![(K.into(), Value::Int(3), vec![], row(1))])[0].to_string(),
            "+ [\"k\"] {\"v\":1}"
        );
        let plan = KeyedPlan {
            key: Vec::new(),
            ver: Some(KeyedColumn {
                id: ColumnId(0),
                ty: Type::parse("UInt64"),
                history_field: "__ver".into(),
            }),
            deleted: None,
            text: "k".into(),
            window: None,
        };
        let saved = serde_json::from_str(&s.save().to_string()).unwrap();
        let mut restored = KeyedState::default();
        restored.load(&plan, &saved).unwrap();
        // the version came back: an older event is still ignored
        assert!(ev(&mut restored, K, Value::Int(2), vec![], false, true, row(2)).is_empty());
        assert_eq!(restored.stale, 1);
        assert!(restored.load(&plan, &json!({})).is_err());
    }

    #[test]
    fn ordered_rows_are_inserted_moved_and_deleted_at_their_places() {
        use crate::binder::{ColumnId, OrderKey};
        let order = KeyOrder(vec![OrderKey {
            column: ColumnId(0),
            desc: false,
            nulls_first: false,
        }]);
        let mut s = KeyedState::new(Some(order.clone()));
        let k = |n: i128| vec![Value::Int(n)];
        let id = |x: &str| format!("[\"{x}\"]");
        // history in ClickHouse's order (ties as it chose): inserts in result order
        let history: Vec<String> = s
            .history(vec![
                (id("a"), Value::Int(1), k(2), row(2)),
                (id("b"), Value::Int(1), k(1), row(1)),
                (id("c"), Value::Int(1), k(2), row(2)),
            ])
            .iter()
            .map(|d| d.to_string())
            .collect();
        assert_eq!(
            history,
            [
                "+ [\"b\"] {\"v\":1}",
                "+ [\"a\"] {\"v\":2}",
                "+ [\"c\"] {\"v\":2}"
            ]
        );
        let mut e = |x: &str, ver: i128, key: i128, deleted: bool| {
            ev(
                &mut s,
                &id(x),
                Value::Int(ver),
                k(key),
                deleted,
                true,
                row(key as i64),
            )
        };
        let out = lines([
            // new, tying with a and c: after them
            e("d", 1, 2, false),
            // a new version with the same sort key: no move
            e("a", 2, 2, false),
            // a new sort key: b moves after the 2s
            e("b", 2, 2, false),
            // and to the top
            e("c", 2, 0, false),
            // deleted
            e("a", 3, 2, true),
        ]);
        assert_eq!(
            out,
            [
                "+ @3 [\"d\"] {\"v\":2}",
                "~ [\"a\"] {\"v\":2}",
                "~ [\"b\"] @3 {\"v\":2}",
                "~ [\"c\"] @0 {\"v\":0}",
                "- [\"a\"]",
            ]
        );
        // a checkpoint keeps the order within ties: c, d, b
        let mut back = KeyedState::new(Some(order));
        let plan = KeyedPlan {
            key: Vec::new(),
            ver: None,
            deleted: None,
            text: "k".into(),
            window: None,
        };
        back.load(&plan, &serde_json::from_str(&s.save().to_string()).unwrap())
            .unwrap();
        let ids = |s: &KeyedState| -> Vec<String> {
            s.ranking
                .as_ref()
                .unwrap()
                .iter()
                .map(|e| e.id.clone())
                .collect()
        };
        assert_eq!(ids(&back), [id("c"), id("d"), id("b")]);
        assert_eq!(ids(&back), ids(&s));
    }

    fn window(limit: u64, buffer: u64) -> KeyedState {
        KeyedState::with_window(KeyedWindowPlan { limit, buffer })
    }

    fn history(s: &mut KeyedState, ids: &[&str]) -> Vec<String> {
        let rows = ids
            .iter()
            .map(|x| (x.to_string(), Value::Int(1), vec![], row(1)))
            .collect();
        s.history(rows).iter().map(|d| d.to_string()).collect()
    }

    /// A version of `id` at offset `o` of partition 0.
    fn at(s: &mut KeyedState, id: &str, o: i64, shown: bool) -> Vec<Diff> {
        let v = NewVersion {
            id: id.to_string(),
            ver: Value::Int(o as i128),
            key: vec![],
            deleted: !shown,
            matches: true,
            at: (0, o),
        };
        s.event(v, || row(o))
    }

    fn kept(s: &KeyedState) -> (Vec<String>, Vec<String>) {
        let w = s.window.as_ref().unwrap();
        (
            w.visible.iter().cloned().collect(),
            w.buffered.iter().cloned().collect(),
        )
    }

    fn wm(o: i64) -> Watermark {
        Watermark([(0, o)].into())
    }

    #[test]
    fn a_window_keeps_the_newest_rows_and_replaces_departures_from_its_buffer() {
        let mut s = window(2, 4);
        // the first 2 are the result, the rest the buffer; fewer than 2 + 4: every row
        assert_eq!(
            history(&mut s, &["a", "b", "c", "d"]),
            ["+ a {\"v\":1}", "+ b {\"v\":1}"]
        );
        assert!(s.window.as_ref().unwrap().exhaustive);
        let out = lines([
            // new keys come in at the top; the last visible row slides into the buffer
            at(&mut s, "e", 1, true),
            at(&mut s, "f", 2, true),
            // the buffer is over capacity: its last row (d) is let go
            at(&mut s, "g", 3, true),
            // a visible row's new version: updated; a buffered one's: kept, not sent
            at(&mut s, "g", 4, true),
            at(&mut s, "a", 5, true),
            // a visible row leaves: the buffer's first comes in at the end
            at(&mut s, "f", 6, false),
            // a buffered one leaves: nothing to send
            at(&mut s, "b", 7, false),
            // not kept, not shown: nothing
            at(&mut s, "x", 8, false),
        ]);
        assert_eq!(
            out,
            [
                "+ @0 e {\"v\":1}",
                "- @2",
                "+ @0 f {\"v\":2}",
                "- @2",
                "+ @0 g {\"v\":3}",
                "- @2",
                "~ g {\"v\":4}",
                "- f",
                "+ @1 e {\"v\":1}",
            ]
        );
        assert_eq!(
            kept(&s),
            (vec!["g".into(), "e".into()], vec!["a".into(), "c".into()])
        );
        assert!(!s.window.as_ref().unwrap().exhaustive, "d was let go");
        // the promoted row is the one kept, as last seen
        assert_eq!(s.window.as_ref().unwrap().rows["a"], row(5));
        // a checkpoint keeps both lists and the rows
        let mut back = window(2, 4);
        let plan = KeyedPlan {
            key: Vec::new(),
            ver: None,
            deleted: None,
            text: "k".into(),
            window: None,
        };
        back.load(&plan, &serde_json::from_str(&s.save().to_string()).unwrap())
            .unwrap();
        assert_eq!(kept(&back), kept(&s));
        assert_eq!(
            back.window.as_ref().unwrap().rows,
            s.window.as_ref().unwrap().rows
        );
        // LIMIT 0: never anything
        let mut none = window(0, 4);
        assert!(history(&mut none, &[]).is_empty());
        assert!(at(&mut none, "a", 1, true).is_empty());
    }

    #[test]
    fn departures_drain_the_buffer_and_a_refill_brings_rows_that_still_match() {
        let refill = |ids: &[&str], at: i64, complete: bool| Refill {
            rows: ids
                .iter()
                .map(|x| (x.to_string(), Value::Int(at as i128), row(at)))
                .collect(),
            at: wm(at),
            complete,
        };
        let mut s = window(2, 4);
        // 2 + 4 rows: maybe more past them
        history(&mut s, &["a", "b", "c", "d", "e", "f"]);
        assert!(!s.wants_refill(), "a full buffer");
        for (o, x) in [(10, "c"), (11, "d"), (12, "e"), (13, "f")] {
            assert!(at(&mut s, x, o, false).is_empty());
        }
        // a visible row leaves with an empty buffer: one short, and a refill is due
        assert_eq!(lines([at(&mut s, "b", 14, false)]), ["- b"]);
        assert!(s.wants_refill());
        assert!(!s.wants_refill(), "asked once");
        // z leaves after the snapshot (16 > 15): it isn't brought back; c left before it, but
        // the snapshot wouldn't have it anyway; a is kept already
        assert!(at(&mut s, "z", 16, false).is_empty());
        let diffs = s.refill(refill(&["a", "z", "y", "w"], 15, true), &wm(16));
        assert_eq!(lines([diffs]), ["+ @1 y {\"v\":15}"]);
        assert_eq!(kept(&s), (vec!["a".into(), "y".into()], vec!["w".into()]));
        assert!(
            s.window.as_ref().unwrap().exhaustive,
            "the snapshot had every row"
        );
        assert!(!s.wants_refill(), "nothing past them");

        // a snapshot past the stream: it waits until the stream gets there
        let mut s = window(1, 4);
        history(&mut s, &["a", "b", "c", "d", "e"]);
        for (o, x) in [(10, "b"), (11, "c"), (12, "d"), (13, "e")] {
            at(&mut s, x, o, false);
        }
        assert!(s.wants_refill());
        assert!(s.refill(refill(&["v"], 30, false), &wm(20)).is_empty());
        assert!(s.advanced(&wm(25)).is_empty());
        assert!(
            s.advanced(&wm(30)).is_empty(),
            "into the buffer, the result is full"
        );
        assert_eq!(kept(&s).1, ["v"]);

        // restored at 40: departures before it aren't known, so a snapshot at 35 can't be used
        let mut s = window(1, 4);
        history(&mut s, &["a", "b", "c", "d", "e"]);
        s.restored_at(&wm(40));
        for (o, x) in [(41, "b"), (42, "c"), (43, "d"), (44, "e")] {
            at(&mut s, x, o, false);
        }
        assert!(s.wants_refill());
        assert!(s.refill(refill(&["v"], 35, true), &wm(44)).is_empty());
        assert!(kept(&s).1.is_empty());
        assert!(!s.wants_refill(), "not right away");
    }

    /// A row let go (still matching) after a refill's snapshot isn't in it: the refill can't
    /// say it has every row, however complete the snapshot was.
    #[test]
    fn a_refill_isnt_complete_when_rows_were_let_go_after_its_snapshot() {
        let mut s = window(1, 1);
        history(&mut s, &["a", "b"]);
        for (x, o) in [("c", 5), ("d", 6), ("e", 7)] {
            at(&mut s, x, o, true);
        }
        // c (last seen at 5) was let go when e came in
        assert_eq!(kept(&s), (vec!["e".into()], vec!["d".into()]));
        at(&mut s, "e", 8, false);
        at(&mut s, "d", 9, false);
        assert!(kept(&s).0.is_empty() && s.wants_refill());
        let empty = |at: i64| Refill {
            rows: Vec::new(),
            at: wm(at),
            complete: true,
        };
        s.refill(empty(4), &wm(9));
        assert!(
            !s.window.as_ref().unwrap().exhaustive,
            "c came after the snapshot"
        );
        assert_eq!(s.last_refill(), Some(wm(4)));
        // one past it can: nothing matches now (c, d, e and the history's rows left or were
        // let go before it)
        let mut s2 = s.clone();
        s2.window.as_mut().unwrap().requested = false;
        s2.refill(empty(6), &wm(9));
        assert!(s2.window.as_ref().unwrap().exhaustive);
    }
}
