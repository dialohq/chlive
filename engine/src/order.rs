//! Managed ordering: where a new row goes in an ORDER BY result.
//!
//! The state holds only the ORDER BY values of the result, never its rows, with equal keys
//! merged into one tie group: `(key, how many rows have it)`, in result order. The history
//! seeds it in the order ClickHouse returned (ties included, in whatever order ClickHouse
//! chose); after that, a new row goes after the last row of its tie group. So the result always
//! has the same sequence of keys as a fresh query would, and only the order of rows within a
//! tie group can differ.

use std::cmp::Ordering;

use std::collections::{HashMap, VecDeque};

use serde_json::{json, Value as Json};

use crate::binder::OrderKey;
use crate::diff::{Diff, Row};
use crate::types::Value;

/// The ORDER BY values of one row, most significant first.
pub type SortKey = Vec<Value>;

/// A sort key as JSON (each value in [`Value::save`]'s form), for checkpoints.
pub fn save_key(key: &SortKey) -> Json {
    Json::Array(key.iter().map(Value::save).collect())
}

pub fn load_key(json: &Json) -> Result<SortKey, String> {
    json.as_array()
        .ok_or("a sort key isn't a list")?
        .iter()
        .map(|v| Value::load(v).ok_or_else(|| format!("not a saved value: {v}")))
        .collect()
}

fn bad(what: &str) -> String {
    format!("checkpoint: bad {what}")
}

/// Compares sort keys by ORDER BY's keys: each one's direction and NULL placement.
#[derive(Debug, Clone, PartialEq)]
pub struct KeyOrder(pub Vec<OrderKey>);

impl KeyOrder {
    pub fn cmp(&self, a: &SortKey, b: &SortKey) -> Ordering {
        for (k, (x, y)) in self.0.iter().zip(a.iter().zip(b)) {
            match x.sort_cmp(y, k.desc, k.nulls_first) {
                Ordering::Equal => continue,
                other => return other,
            }
        }
        Ordering::Equal
    }
}

/// One ranked row: its sort key, and `seq`, when it got that key (ties: later goes after).
#[derive(Debug, Clone, PartialEq)]
pub struct Entry {
    pub key: SortKey,
    pub seq: u64,
    pub id: String,
}

/// Rows with ids, in ORDER BY order: an id's index found by a binary search on its (key, seq),
/// which are unique. A row placed (new, or with a new key) goes after the rows it ties with.
/// Inserting, removing and moving shift the list (O(n)), as applying the diff does on a client.
#[derive(Debug, Clone)]
pub struct Ranking {
    order: KeyOrder,
    ranked: Vec<Entry>,
    keys: HashMap<String, (SortKey, u64)>,
    seq: u64,
}

impl Ranking {
    pub fn new(order: KeyOrder) -> Self {
        Self {
            order,
            ranked: Vec::new(),
            keys: HashMap::new(),
            seq: 0,
        }
    }

    pub fn order(&self) -> &KeyOrder {
        &self.order
    }

    /// The history: rows in arrival order, ranked by (key, arrival). Sorted once, rather than
    /// placed one at a time (which would shift the list on every row, O(n²)); when they
    /// arrive in order already, the order stays.
    pub fn history(&mut self, rows: impl IntoIterator<Item = (String, SortKey)>) {
        debug_assert!(self.ranked.is_empty(), "history comes first");
        let mut entries: Vec<Entry> = rows
            .into_iter()
            .map(|(id, key)| {
                self.seq += 1;
                Entry {
                    key,
                    seq: self.seq,
                    id,
                }
            })
            .collect();
        let order = &self.order;
        entries.sort_by(|a, b| order.cmp(&a.key, &b.key).then(a.seq.cmp(&b.seq)));
        for e in &entries {
            self.keys.insert(e.id.clone(), (e.key.clone(), e.seq));
        }
        self.ranked = entries;
    }

    /// Rank a row by `key`, after the rows it ties with; its index.
    pub fn place(&mut self, id: &str, key: SortKey) -> usize {
        self.seq += 1;
        let seq = self.seq;
        let at = self
            .ranked
            .partition_point(|e| self.cmp(&key, seq, e) == Ordering::Greater);
        self.keys.insert(id.to_string(), (key.clone(), seq));
        self.ranked.insert(
            at,
            Entry {
                key,
                seq,
                id: id.to_string(),
            },
        );
        at
    }

    /// Take a row out; where it was.
    pub fn remove(&mut self, id: &str) -> Option<usize> {
        let at = self.position(id)?;
        self.ranked.remove(at);
        self.keys.remove(id);
        Some(at)
    }

    /// Where a row is.
    pub fn position(&self, id: &str) -> Option<usize> {
        let (key, seq) = self.keys.get(id)?;
        let at = self
            .ranked
            .partition_point(|e| self.cmp(key, *seq, e) == Ordering::Greater);
        debug_assert_eq!(self.ranked[at].id, id);
        Some(at)
    }

    /// A row's sort key.
    pub fn key(&self, id: &str) -> Option<&SortKey> {
        self.keys.get(id).map(|(k, _)| k)
    }

    pub fn get(&self, index: usize) -> Option<&Entry> {
        self.ranked.get(index)
    }

    pub fn iter(&self) -> impl Iterator<Item = &Entry> {
        self.ranked.iter()
    }

    pub fn len(&self) -> usize {
        self.ranked.len()
    }

    pub fn is_empty(&self) -> bool {
        self.ranked.is_empty()
    }

    fn cmp(&self, a: &SortKey, a_seq: u64, b: &Entry) -> Ordering {
        self.order.cmp(a, &b.key).then(a_seq.cmp(&b.seq))
    }

    /// For a checkpoint: `{"ranked": [[id, key, seq], …], "seq": n}`.
    pub fn save(&self) -> Json {
        let ranked: Vec<Json> = self
            .ranked
            .iter()
            .map(|e| json!([e.id, save_key(&e.key), e.seq]))
            .collect();
        json!({ "ranked": ranked, "seq": self.seq })
    }

    /// Back to a saved ranking (of the same ORDER BY): the fields of `json` [`Self::save`] wrote.
    pub fn load(&mut self, json: &Json) -> Result<(), String> {
        let bad = |what: &str| format!("checkpoint: bad ranking ({what})");
        let mut ranked = Vec::new();
        let mut keys = HashMap::new();
        for e in json["ranked"].as_array().ok_or_else(|| bad("ranked"))? {
            let id = e
                .get(0)
                .and_then(Json::as_str)
                .ok_or_else(|| bad("id"))?
                .to_string();
            let key = load_key(e.get(1).ok_or_else(|| bad("key"))?)?;
            let seq = e.get(2).and_then(Json::as_u64).ok_or_else(|| bad("seq"))?;
            keys.insert(id.clone(), (key.clone(), seq));
            ranked.push(Entry { key, seq, id });
        }
        self.seq = json["seq"].as_u64().ok_or_else(|| bad("seq"))?;
        self.ranked = ranked;
        self.keys = keys;
        Ok(())
    }
}

/// The tie groups of an ordered result, at most `limit` rows with LIMIT (the top N).
#[derive(Debug, Clone)]
pub struct OrderedState {
    order: KeyOrder,
    groups: Vec<(SortKey, usize)>,
    len: usize,
    limit: Option<usize>,
}

/// Where a new row goes in an ordered result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Placement {
    /// Insert the row here.
    pub index: usize,
    /// Then delete the row at this index (the last one, pushed past the LIMIT).
    pub evicts: Option<usize>,
}

impl OrderedState {
    pub fn new(order: KeyOrder) -> Self {
        Self::with_limit(order, None)
    }

    pub fn with_limit(order: KeyOrder, limit: Option<u64>) -> Self {
        Self {
            order,
            groups: Vec::new(),
            len: 0,
            limit: limit.map(|n| usize::try_from(n).unwrap_or(usize::MAX)),
        }
    }

    /// Append a row of the (already ordered and limited) history. A key equal to the previous
    /// one joins its tie group; anything else starts a new group.
    pub fn push_history(&mut self, key: SortKey) {
        if self.is_full() {
            return; // ClickHouse already applied the LIMIT
        }
        match self.groups.last_mut() {
            Some((last, n)) if self.order.cmp(last, &key) == Ordering::Equal => *n += 1,
            _ => self.groups.push((key, 1)),
        }
        self.len += 1;
    }

    /// Place a new row after every row that sorts before it or ties with it. With a LIMIT and a
    /// full result, a row that would land past the end is dropped (`None`, nothing changes), and
    /// any other pushes the last row out.
    pub fn insert(&mut self, key: SortKey) -> Option<Placement> {
        let (at, existing, index) = self.locate(&key);
        if self.is_full() && Some(index) >= self.limit {
            return None;
        }
        if existing {
            self.groups[at].1 += 1;
        } else {
            self.groups.insert(at, (key, 1));
        }
        self.len += 1;
        let evicts = match self.limit {
            Some(n) if self.len > n => {
                let (_, count) = self.groups.last_mut().expect("not empty");
                *count -= 1;
                if *count == 0 {
                    self.groups.pop();
                }
                self.len -= 1;
                Some(n)
            }
            _ => None,
        };
        Some(Placement { index, evicts })
    }

    /// The group a key belongs at, whether it already exists, and the row index after the end
    /// of its tie group.
    fn locate(&self, key: &SortKey) -> (usize, bool, usize) {
        // fast path: at or after the last group (e.g. ORDER BY ts with rising timestamps)
        match self.groups.last() {
            None => return (0, false, 0),
            Some((last, _)) => match self.order.cmp(key, last) {
                Ordering::Equal => return (self.groups.len() - 1, true, self.len),
                Ordering::Greater => return (self.groups.len(), false, self.len),
                Ordering::Less => {}
            },
        }
        let at = self
            .groups
            .partition_point(|(g, _)| self.order.cmp(g, key) == Ordering::Less);
        let before: usize = self.groups[..at].iter().map(|(_, n)| n).sum();
        if self.order.cmp(&self.groups[at].0, key) == Ordering::Equal {
            (at, true, before + self.groups[at].1)
        } else {
            (at, false, before)
        }
    }

    fn is_full(&self) -> bool {
        self.limit.is_some_and(|n| self.len >= n)
    }

    /// Rows in the result.
    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Tie groups held (what the state costs).
    pub fn groups(&self) -> usize {
        self.groups.len()
    }

    /// The tie groups, for a checkpoint.
    pub fn save(&self) -> Json {
        let groups: Vec<Json> = self
            .groups
            .iter()
            .map(|(k, n)| json!([save_key(k), n]))
            .collect();
        json!({ "groups": groups })
    }

    /// Back to a saved state (this one made by [`OrderedState::with_limit`] for the same query).
    pub fn load(&mut self, json: &Json) -> Result<(), String> {
        let mut groups = Vec::new();
        for g in json["groups"]
            .as_array()
            .ok_or_else(|| bad("ordered state"))?
        {
            let n = g
                .get(1)
                .and_then(Json::as_u64)
                .ok_or_else(|| bad("tie group"))? as usize;
            groups.push((load_key(g.get(0).ok_or_else(|| bad("tie group"))?)?, n));
        }
        self.len = groups.iter().map(|(_, n)| n).sum();
        self.groups = groups;
        Ok(())
    }
}

/// LIMIT without ORDER BY: a window of the latest N rows, newest first (the history's order
/// reversed; each new row in at the top, the oldest out at the bottom once full). Only the row
/// count is kept.
#[derive(Debug, Clone)]
pub struct WindowState {
    len: u64,
    limit: u64,
}

impl WindowState {
    pub fn new(limit: u64) -> Self {
        Self { len: 0, limit }
    }

    pub fn push_history(&mut self) {
        self.len = (self.len + 1).min(self.limit);
    }

    /// Resume: a window already holding `len` rows.
    pub fn with_len(limit: u64, len: u64) -> Self {
        Self {
            len: len.min(limit),
            limit,
        }
    }

    pub fn save(&self) -> Json {
        json!({ "len": self.len })
    }

    pub fn load(&mut self, json: &Json) -> Result<(), String> {
        self.len = json["len"].as_u64().ok_or_else(|| bad("window state"))?;
        Ok(())
    }

    pub fn limit(&self) -> u64 {
        self.limit
    }

    /// A new row: `None` with `LIMIT 0` (it never shows), otherwise whether, after inserting it
    /// at the top, the last row must be deleted.
    pub fn push(&mut self) -> Option<bool> {
        if self.limit == 0 {
            return None;
        }
        if self.len < self.limit {
            self.len += 1;
            Some(false)
        } else {
            Some(true)
        }
    }
}

/// ORDER BY with OFFSET m (and maybe LIMIT n): the result is ranks `[m, m + n)`. The state
/// holds the sort keys of the top `m + n`, and the rows of the top `m`: a row landing above the
/// result shifts it, and the row at rank `m - 1` slides in at its top. Ties: a new row goes after
/// the rows it ties with, as in [`OrderedState`].
#[derive(Debug, Clone)]
pub struct OffsetState {
    order: KeyOrder,
    entries: Vec<OffsetEntry>,
    offset: usize,
    /// `m + n`; unbounded without LIMIT.
    cap: Option<usize>,
    limit: Option<usize>,
}

#[derive(Debug, Clone)]
struct OffsetEntry {
    key: SortKey,
    /// Only for ranks `< m`: the rows the client doesn't have, which may slide in.
    row: Option<Row>,
}

impl OffsetState {
    pub fn new(order: KeyOrder, offset: u64, limit: Option<u64>) -> Self {
        let offset = offset as usize;
        let limit = limit.map(|n| n as usize);
        Self {
            order,
            entries: Vec::new(),
            offset,
            cap: limit.map(|n| offset + n),
            limit,
        }
    }

    /// The next row of the (ordered, `LIMIT m + n`) history: `Some` if it's in the result, to
    /// send; `None` if it's above it (kept, to slide in later).
    pub fn push_history(&mut self, key: SortKey, row: Row) -> Option<Row> {
        if self.cap.is_some_and(|c| self.entries.len() >= c) {
            return None;
        }
        let above = self.entries.len() < self.offset;
        self.entries.push(OffsetEntry {
            key,
            row: above.then(|| row.clone()),
        });
        (!above).then_some(row)
    }

    /// The keys of the top `m + n`, and the rows above the result.
    pub fn save(&self) -> Json {
        let entries: Vec<Json> = self
            .entries
            .iter()
            .map(|e| json!({ "key": save_key(&e.key), "row": e.row }))
            .collect();
        json!({ "entries": entries })
    }

    pub fn load(&mut self, json: &Json) -> Result<(), String> {
        let mut entries = Vec::new();
        for e in json["entries"]
            .as_array()
            .ok_or_else(|| bad("offset state"))?
        {
            let row = match &e["row"] {
                Json::Null => None,
                Json::Object(r) => Some(r.clone()),
                _ => return Err(bad("offset row")),
            };
            entries.push(OffsetEntry {
                key: load_key(&e["key"])?,
                row,
            });
        }
        self.entries = entries;
        Ok(())
    }

    /// A new row: placed after every row that sorts before it or ties with it.
    pub fn insert(&mut self, key: SortKey, row: Row) -> Vec<Diff> {
        let p = self
            .entries
            .partition_point(|e| self.order.cmp(&e.key, &key) != Ordering::Greater);
        if self.cap.is_some_and(|c| p >= c) {
            return Vec::new(); // below the result
        }
        let m = self.offset;
        let mut diffs = Vec::new();
        if p < m {
            self.entries.insert(
                p,
                OffsetEntry {
                    key,
                    row: Some(row),
                },
            );
            // the row pushed from rank m - 1 to m comes into the result, at its top
            if let Some(e) = self.entries.get_mut(m) {
                let row = e.row.take().expect("rows are kept above the result");
                diffs.push(Diff::Insert {
                    row,
                    index: Some(0),
                    id: None,
                });
            }
        } else {
            self.entries.insert(p, OffsetEntry { key, row: None });
            diffs.push(Diff::Insert {
                row,
                index: Some(p - m),
                id: None,
            });
        }
        if let (Some(c), Some(n)) = (self.cap, self.limit) {
            if self.entries.len() > c {
                self.entries.pop();
                diffs.push(Diff::Delete { index: n });
            }
        }
        diffs
    }
}

/// No ORDER BY, with OFFSET m (and maybe LIMIT n): a sliding window over the arrival order,
/// shown newest first. The newest `m` rows are held back; the result is the `n` before them. A
/// new row is held back, the oldest held-back row comes into the result at its top, and, with a
/// full result, the oldest one leaves at the bottom.
#[derive(Debug, Clone)]
pub struct HeldWindow {
    held: VecDeque<Row>,
    offset: usize,
    limit: Option<u64>,
    visible: u64,
}

impl HeldWindow {
    pub fn new(offset: u64, limit: Option<u64>) -> Self {
        Self {
            held: VecDeque::new(),
            offset: offset as usize,
            limit,
            visible: 0,
        }
    }

    /// The history (`LIMIT m + n`, in ClickHouse's order, as the arrival order): its last `m`
    /// rows are held back; the rows before them are the result, returned to send.
    pub fn history(&mut self, rows: Vec<Row>) -> Vec<Row> {
        let mut rows = rows;
        let split = rows.len().saturating_sub(self.offset);
        self.held = rows.split_off(split).into();
        if let Some(n) = self.limit {
            // the n before the held-back ones
            let extra = rows.len().saturating_sub(n as usize);
            rows.drain(..extra);
        }
        self.visible = rows.len() as u64;
        rows
    }

    /// The held-back rows, oldest first, and how many rows the result has.
    pub fn save(&self) -> Json {
        json!({ "held": self.held, "visible": self.visible })
    }

    pub fn load(&mut self, json: &Json) -> Result<(), String> {
        let mut held = VecDeque::new();
        for r in json["held"].as_array().ok_or_else(|| bad("held rows"))? {
            held.push_back(r.as_object().ok_or_else(|| bad("held row"))?.clone());
        }
        self.held = held;
        self.visible = json["visible"].as_u64().ok_or_else(|| bad("held state"))?;
        Ok(())
    }

    pub fn push(&mut self, row: Row) -> Vec<Diff> {
        self.held.push_back(row);
        if self.held.len() <= self.offset {
            return Vec::new();
        }
        // newest first: the oldest held-back row enters at the top; once full, the last goes
        let row = self.held.pop_front().expect("more than offset");
        let insert = Diff::Insert {
            row,
            index: Some(0),
            id: None,
        };
        match self.limit {
            Some(0) => Vec::new(),
            Some(n) if self.visible >= n => vec![insert, Diff::Delete { index: n as usize }],
            _ => {
                self.visible += 1;
                vec![insert]
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::binder::ColumnId;

    fn order(keys: &[(bool, bool)]) -> KeyOrder {
        KeyOrder(
            keys.iter()
                .enumerate()
                .map(|(i, (desc, nulls_first))| OrderKey {
                    column: ColumnId(i),
                    desc: *desc,
                    nulls_first: *nulls_first,
                })
                .collect(),
        )
    }

    fn int(n: i128) -> SortKey {
        vec![Value::Int(n)]
    }

    /// Insert keys one by one into a list of labels, the way a client applies indexes.
    fn apply(state: &mut OrderedState, list: &mut Vec<String>, rows: &[(SortKey, &str)]) {
        for (key, label) in rows {
            if let Some(p) = state.insert(key.clone()) {
                list.insert(p.index, label.to_string());
                if let Some(e) = p.evicts {
                    list.remove(e);
                }
            }
        }
    }

    #[test]
    fn ties_go_after_their_group_and_groups_merge() {
        let mut s = OrderedState::new(order(&[(false, false)]));
        let mut list: Vec<String> = Vec::new();
        for (k, l) in [(1, "h1"), (3, "h3a"), (3, "h3b"), (5, "h5")] {
            s.push_history(int(k));
            list.push(l.into());
        }
        assert_eq!(s.groups(), 3);
        apply(
            &mut s,
            &mut list,
            &[
                (int(3), "e3"), // end of the 3s
                (int(0), "e0"), // first
                (int(9), "e9"), // last (fast path)
                (int(4), "e4"), // a new group in the middle
                (int(1), "e1"), // end of the 1s
                (int(9), "e9b"),
            ],
        );
        assert_eq!(
            list,
            ["e0", "h1", "e1", "h3a", "h3b", "e3", "e4", "h5", "e9", "e9b"]
        );
        assert_eq!((s.len(), s.groups()), (10, 6));
    }

    #[test]
    fn desc_and_nulls() {
        // ORDER BY a DESC NULLS FIRST
        let mut s = OrderedState::new(order(&[(true, true)]));
        let mut list = Vec::new();
        apply(
            &mut s,
            &mut list,
            &[
                (int(1), "1"),
                (int(5), "5"),
                (vec![Value::Null], "null"),
                (int(3), "3"),
                (vec![Value::Null], "null2"),
            ],
        );
        assert_eq!(list, ["null", "null2", "5", "3", "1"]);
    }

    #[test]
    fn multiple_keys() {
        // ORDER BY country, latency DESC
        let mut s = OrderedState::new(order(&[(false, false), (true, false)]));
        let k = |c: &str, l: i128| vec![Value::String(c.into()), Value::Int(l)];
        let mut list = Vec::new();
        apply(
            &mut s,
            &mut list,
            &[
                (k("PL", 10), "pl10"),
                (k("DE", 5), "de5"),
                (k("PL", 50), "pl50"),
                (k("DE", 5), "de5b"),
                (k("PL", 1), "pl1"),
            ],
        );
        assert_eq!(list, ["de5", "de5b", "pl50", "pl10", "pl1"]);
    }

    #[test]
    fn top_n() {
        // ORDER BY a DESC LIMIT 3, history 9 5 5
        let mut s = OrderedState::with_limit(order(&[(true, false)]), Some(3));
        let mut list: Vec<String> = Vec::new();
        for (k, l) in [(9, "h9"), (5, "h5a"), (5, "h5b")] {
            s.push_history(int(k));
            list.push(l.into());
        }
        // past the end, and a tie with the last row: dropped, nothing changes
        assert_eq!(s.insert(int(1)), None);
        assert_eq!(s.insert(int(5)), None);
        assert_eq!((s.len(), s.groups()), (3, 2));
        // inside: inserted, then the last row (now at index N) is deleted
        assert_eq!(
            s.clone().insert(int(7)),
            Some(Placement {
                index: 1,
                evicts: Some(3)
            })
        );
        apply(
            &mut s,
            &mut list,
            &[
                (int(7), "e7"),
                (int(10), "e10"),
                (int(6), "e6"),
                (int(8), "e8"),
            ],
        );
        assert_eq!(list, ["e10", "h9", "e8"]);
        // the state never holds more than N
        assert_eq!((s.len(), s.groups()), (3, 3));
    }

    #[test]
    fn limit_zero_and_short_history() {
        let mut s = OrderedState::with_limit(order(&[(false, false)]), Some(0));
        assert_eq!(s.insert(int(1)), None);
        // history shorter than the limit: no evictions until full
        let mut s = OrderedState::with_limit(order(&[(false, false)]), Some(2));
        s.push_history(int(5));
        assert_eq!(
            s.insert(int(1)),
            Some(Placement {
                index: 0,
                evicts: None
            })
        );
        assert_eq!(
            s.insert(int(3)),
            Some(Placement {
                index: 1,
                evicts: Some(2)
            })
        );
        assert_eq!(s.insert(int(9)), None);
    }

    #[test]
    fn window() {
        let mut w = WindowState::new(2);
        w.push_history();
        assert_eq!(w.push(), Some(false)); // now full
        assert_eq!(w.push(), Some(true)); // append, then delete the first
        assert_eq!(w.push(), Some(true));
        assert_eq!(WindowState::new(0).push(), None);
    }

    fn row(n: i64, tag: usize) -> Row {
        serde_json::json!({"n": n, "tag": tag})
            .as_object()
            .unwrap()
            .clone()
    }

    /// Random rows: the result built from the diffs alone always has the sort keys of ranks
    /// [m, m + n) of everything so far (ties in any order, as in ClickHouse).
    #[test]
    fn offset_state_matches_a_full_recompute() {
        let mut seed: u64 = 7;
        let mut rand = move |n: u64| {
            seed = seed
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            (seed >> 33) % n
        };
        for (desc, offset, limit) in [
            (true, 2, Some(3)),
            (false, 3, None),
            (true, 1, Some(1)),
            (false, 4, Some(2)),
        ] {
            let o = order(&[(desc, false)]);
            let mut s = OffsetState::new(o.clone(), offset, limit);
            let mut all: Vec<i64> = Vec::new();
            // history: the first 6, ordered, LIMIT m + n
            let mut history: Vec<i64> = (0..6).map(|_| rand(10) as i64).collect();
            history.sort_by(|a, b| if desc { b.cmp(a) } else { a.cmp(b) });
            all.extend(&history);
            let cap = limit.map(|n| (offset + n) as usize).unwrap_or(usize::MAX);
            let mut list: Vec<Row> = Vec::new();
            for (i, n) in history.iter().take(cap).enumerate() {
                if let Some(r) = s.push_history(vec![Value::Int(*n as i128)], row(*n, i)) {
                    list.push(r);
                }
            }
            for i in 0..200 {
                let n = rand(10) as i64;
                all.push(n);
                for d in s.insert(vec![Value::Int(n as i128)], row(n, 100 + i)) {
                    d.apply(&mut list).unwrap_or_else(|e| panic!("{d}: {e}"));
                }
                let mut want = all.clone();
                want.sort_by(|a, b| if desc { b.cmp(a) } else { a.cmp(b) });
                let want: Vec<i64> = want
                    .into_iter()
                    .skip(offset as usize)
                    .take(limit.map_or(usize::MAX, |n| n as usize))
                    .collect();
                let got: Vec<i64> = list.iter().map(|r| r["n"].as_i64().unwrap()).collect();
                assert_eq!(got, want, "desc {desc}, offset {offset}, limit {limit:?}");
            }
        }
    }

    /// Random arrivals: the result is the `n` rows before the newest `m`, in arrival order.
    #[test]
    fn held_window_slides_over_the_arrival_order() {
        for (offset, limit) in [
            (2, Some(3)),
            (3, None),
            (1, Some(1)),
            (0, Some(2)),
            (4, Some(0)),
        ] {
            let mut w = HeldWindow::new(offset, limit);
            let history: Vec<Row> = (0..5).map(|i| row(i, i as usize)).collect();
            let take = limit.map_or(5, |n| (offset + n).min(5) as usize);
            let mut arrived: Vec<i64> = (0..take as i64).collect();
            // the engine shows the history newest first too
            let mut list = w.history(history.into_iter().take(take).collect());
            list.reverse();
            for i in 5..60 {
                arrived.push(i);
                for d in w.push(row(i, i as usize)) {
                    d.apply(&mut list).unwrap();
                }
                let end = arrived.len().saturating_sub(offset as usize);
                let start = limit.map_or(0, |n| end.saturating_sub(n as usize));
                // the same rows, newest first
                let want: Vec<i64> = arrived[start..end].iter().rev().copied().collect();
                let got: Vec<i64> = list.iter().map(|r| r["n"].as_i64().unwrap()).collect();
                assert_eq!(got, want, "offset {offset}, limit {limit:?}");
            }
        }
    }

    fn ids(r: &Ranking) -> Vec<&str> {
        r.iter().map(|e| e.id.as_str()).collect()
    }

    #[test]
    fn a_ranking_places_rows_after_their_ties() {
        let mut r = Ranking::new(order(&[(false, false)]));
        // the history keeps arrival order within ties
        r.history([
            ("a".into(), int(2)),
            ("b".into(), int(1)),
            ("c".into(), int(2)),
        ]);
        assert_eq!(ids(&r), ["b", "a", "c"]);
        assert_eq!(r.place("d", int(2)), 3);
        assert_eq!(r.place("e", int(0)), 0);
        assert_eq!(ids(&r), ["e", "b", "a", "c", "d"]);
        // a move: out, then after its new ties
        assert_eq!(r.remove("b"), Some(1));
        assert_eq!(r.place("b", int(2)), 4);
        assert_eq!(ids(&r), ["e", "a", "c", "d", "b"]);
        assert_eq!((r.position("c"), r.key("c")), (Some(2), Some(&int(2))));
        assert_eq!(r.remove("nope"), None);
        // a checkpoint keeps the order within ties
        let mut back = Ranking::new(order(&[(false, false)]));
        back.load(&serde_json::from_str(&r.save().to_string()).unwrap())
            .unwrap();
        assert_eq!(ids(&back), ids(&r));
        assert_eq!(back.place("f", int(2)), r.place("f", int(2)));
    }

    /// Every row's position is where it is, through random places, moves and removes.
    #[test]
    fn a_ranking_finds_its_rows() {
        let mut seed: u64 = 3;
        let mut rand = |n: u64| {
            seed = seed
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            (seed >> 33) % n
        };
        let mut r = Ranking::new(order(&[(true, false)]));
        for i in 0..2000 {
            let id = format!("r{}", rand(60));
            match rand(3) {
                0 => {
                    r.remove(&id);
                }
                _ => {
                    r.remove(&id);
                    r.place(&id, int(rand(10) as i128));
                }
            }
            for (at, e) in r.iter().enumerate() {
                assert_eq!(r.position(&e.id), Some(at), "step {i}");
            }
            let keys: Vec<&SortKey> = r.iter().map(|e| &e.key).collect();
            assert!(
                keys.windows(2)
                    .all(|w| r.order().cmp(w[0], w[1]) != Ordering::Greater),
                "step {i}: out of order"
            );
        }
    }
}
