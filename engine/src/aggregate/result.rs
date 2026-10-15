//! What the client has of an aggregated result, kept in order: every group ranked by the
//! ORDER BY values of its finalized row, the first `limit` of them sent.
//!
//! A group's values change as events arrive, so with ORDER BY rows move, up or down. Each batch
//! re-ranks the groups it touched and says how the client's list changes, one change at a time
//! (each index is into the list as the previous change left it):
//! - a sent group that stays in the result: `update` of the changed columns, with an `index`
//!   when it moved;
//! - one that falls out (LIMIT): `delete`, then the group that takes the last place comes in;
//! - one that comes in (new, or climbing into the LIMIT): `insert` at its place, then the one
//!   pushed past the LIMIT goes.
//!
//! A group whose sort key changes goes after the groups it ties with, as new rows do in plain
//! ORDER BY results. Without ORDER BY groups are in the order they first appeared and never
//! move; with LIMIT alone, the first `limit` of them are the result.

use std::cmp::Ordering;
use std::collections::HashMap;

use serde_json::Value as Json;

use crate::binder::{ColumnId, OrderKey};
use crate::diff::{Diff, Row};
use crate::order::{KeyOrder, SortKey};
use crate::types::{decode, Type, Value};

/// An ORDER BY term of an aggregated result: the row column holding its value.
#[derive(Debug, Clone, PartialEq)]
pub struct GroupOrderTerm {
    pub column: String,
    pub ty: Type,
    pub desc: bool,
    pub nulls_first: bool,
}

struct Entry {
    key: SortKey,
    /// Ties are broken by when the group got its key: later goes after.
    seq: u64,
    id: String,
}

pub struct GroupResult {
    terms: Vec<GroupOrderTerm>,
    order: KeyOrder,
    limit: usize,
    /// The window starts after this many groups (OFFSET).
    offset: usize,
    /// Columns computed only to order by (`__o0`, …), never sent.
    hidden: Vec<String>,
    /// Every group, in result order.
    ranked: Vec<Entry>,
    /// Each group's place in `ranked`: its key and seq.
    keys: HashMap<String, (SortKey, u64)>,
    /// Each group's latest row (without hidden columns).
    rows: HashMap<String, Row>,
    /// What the client has: the rows of the groups in the result, as sent.
    sent: HashMap<String, Row>,
    seq: u64,
}

impl GroupResult {
    pub fn new(
        terms: Vec<GroupOrderTerm>,
        limit: Option<u64>,
        offset: Option<u64>,
        hidden: Vec<String>,
    ) -> Self {
        let order = KeyOrder(
            terms
                .iter()
                .enumerate()
                .map(|(i, t)| OrderKey {
                    column: ColumnId(i),
                    desc: t.desc,
                    nulls_first: t.nulls_first,
                })
                .collect(),
        );
        Self {
            terms,
            order,
            limit: limit.map_or(usize::MAX, |n| n as usize),
            offset: offset.map_or(0, |m| m as usize),
            hidden,
            ranked: Vec::new(),
            keys: HashMap::new(),
            rows: HashMap::new(),
            sent: HashMap::new(),
            seq: 0,
        }
    }

    fn ordered(&self) -> bool {
        !self.terms.is_empty()
    }

    /// The ranking, every group's row and what was sent, for a checkpoint.
    pub fn save(&self) -> Json {
        let ranked: Vec<Json> = self
            .ranked
            .iter()
            .map(|e| serde_json::json!([e.id, crate::order::save_key(&e.key), e.seq]))
            .collect();
        serde_json::json!({
            "ranked": ranked,
            "rows": self.rows,
            "sent": self.sent,
            "seq": self.seq,
        })
    }

    /// Back to a saved ranking (this one made by [`GroupResult::new`] for the same query).
    pub fn load(&mut self, json: &Json) -> Result<(), String> {
        let bad = |what: &str| format!("checkpoint: bad group result ({what})");
        let mut ranked = Vec::new();
        let mut keys = HashMap::new();
        for e in json["ranked"].as_array().ok_or_else(|| bad("ranked"))? {
            let id = e
                .get(0)
                .and_then(Json::as_str)
                .ok_or_else(|| bad("id"))?
                .to_string();
            let key = crate::order::load_key(e.get(1).ok_or_else(|| bad("key"))?)?;
            let seq = e.get(2).and_then(Json::as_u64).ok_or_else(|| bad("seq"))?;
            keys.insert(id.clone(), (key.clone(), seq));
            ranked.push(Entry { key, seq, id });
        }
        let rows = |name: &str| -> Result<HashMap<String, Row>, String> {
            json[name]
                .as_object()
                .ok_or_else(|| bad(name))?
                .iter()
                .map(|(id, r)| Ok((id.clone(), r.as_object().ok_or_else(|| bad(name))?.clone())))
                .collect()
        };
        self.rows = rows("rows")?;
        self.sent = rows("sent")?;
        self.seq = json["seq"].as_u64().ok_or_else(|| bad("seq"))?;
        self.ranked = ranked;
        self.keys = keys;
        Ok(())
    }

    /// The history: every group ranked; the window (`limit` after `offset`), in order, as
    /// appended inserts.
    ///
    /// The groups are sorted once by (key, arrival), the order [`Self::place`] keeps: placing
    /// them one at a time would shift the list on every insert, O(groups²).
    pub fn history(&mut self, groups: Vec<(String, Row)>) -> Vec<Diff> {
        debug_assert!(self.ranked.is_empty(), "history comes first");
        let mut entries = Vec::with_capacity(groups.len());
        for (id, row) in groups {
            let (key, row) = self.split(row);
            self.seq += 1;
            self.rows.insert(id.clone(), row);
            entries.push(Entry {
                key,
                seq: self.seq,
                id,
            });
        }
        let order = &self.order;
        entries.sort_by(|a, b| order.cmp(&a.key, &b.key).then(a.seq.cmp(&b.seq)));
        for e in &entries {
            self.keys.insert(e.id.clone(), (e.key.clone(), e.seq));
        }
        self.ranked = entries;
        let mut diffs = Vec::new();
        for e in self.ranked.iter().skip(self.offset).take(self.limit) {
            let row = self.rows[&e.id].clone();
            self.sent.insert(e.id.clone(), row.clone());
            diffs.push(Diff::Insert {
                row,
                index: None,
                id: Some(e.id.clone()),
            });
        }
        diffs
    }

    /// The groups a batch touched, with their new rows, in the order they were touched.
    pub fn batch(&mut self, groups: Vec<(String, Row)>) -> Vec<Diff> {
        let mut diffs = Vec::new();
        for (id, row) in groups {
            self.touch(id, row, &mut diffs);
        }
        diffs
    }

    fn touch(&mut self, id: String, row: Row, diffs: &mut Vec<Diff>) {
        if self.offset > 0 {
            return self.touch_window(id, row, diffs);
        }
        let (key, row) = self.split(row);
        let old = self.position(&id);
        let moved = match old {
            Some(p) => self.order.cmp(&self.ranked[p].key, &key) != Ordering::Equal,
            None => true,
        };
        if moved {
            if let Some(p) = old {
                self.ranked.remove(p);
            }
            self.place(&id, key);
        }
        let new = self.position(&id).expect("just placed");
        self.rows.insert(id.clone(), row.clone());
        let n = self.limit;
        let ordered = self.ordered();
        let index = move |i: usize| ordered.then_some(i);
        let was_in = self.sent.contains_key(&id);
        match (was_in, new < n) {
            (true, true) => {
                let sent = self.sent.get_mut(&id).expect("sent");
                let set: Row = row
                    .into_iter()
                    .filter(|(k, v)| sent.get(k) != Some(v))
                    .collect();
                for (k, v) in &set {
                    sent.insert(k.clone(), v.clone());
                }
                let to = (old != Some(new)).then_some(new);
                if !set.is_empty() || to.is_some() {
                    diffs.push(Diff::Update { id, set, index: to });
                }
            }
            (true, false) => {
                // out of the LIMIT: the group now last in it comes in
                diffs.push(Diff::Delete {
                    index: old.expect("a sent group is ranked"),
                });
                self.sent.remove(&id);
                if let Some(next) = self.ranked.get(n - 1).map(|e| e.id.clone()) {
                    let row = self.rows[&next].clone();
                    self.sent.insert(next.clone(), row.clone());
                    diffs.push(Diff::Insert {
                        row,
                        index: index(n - 1),
                        id: Some(next),
                    });
                }
            }
            (false, true) => {
                self.sent.insert(id.clone(), row.clone());
                diffs.push(Diff::Insert {
                    row,
                    index: index(new),
                    id: Some(id),
                });
                // the group pushed past the LIMIT goes
                if let Some(out) = self.ranked.get(n).map(|e| e.id.clone()) {
                    if self.sent.remove(&out).is_some() {
                        diffs.push(Diff::Delete { index: n });
                    }
                }
            }
            (false, false) => {}
        }
    }

    /// The ids in the window, in order.
    fn window(&self) -> Vec<String> {
        self.ranked
            .iter()
            .skip(self.offset)
            .take(self.limit)
            .map(|e| e.id.clone())
            .collect()
    }

    /// With an OFFSET, a group moving can bring groups into the window, or take them out, at
    /// either end. From the window before and after: delete the groups that left, move the
    /// touched group if it stayed, insert the groups that came in (each change into the list
    /// as the previous one left it).
    fn touch_window(&mut self, id: String, row: Row, diffs: &mut Vec<Diff>) {
        use std::collections::HashSet;
        let before = self.window();
        let (key, row) = self.split(row);
        let old = self.position(&id);
        let moved = match old {
            Some(p) => self.order.cmp(&self.ranked[p].key, &key) != Ordering::Equal,
            None => true,
        };
        if moved {
            if let Some(p) = old {
                self.ranked.remove(p);
            }
            self.place(&id, key);
        }
        self.rows.insert(id.clone(), row);
        let after = self.window();
        let staying: HashSet<&String> = after.iter().collect();
        let mut list = before;
        for i in (0..list.len()).rev() {
            if !staying.contains(&list[i]) {
                diffs.push(Diff::Delete { index: i });
                self.sent.remove(&list[i]);
                list.remove(i);
            }
        }
        if let Some(cur) = list.iter().position(|x| *x == id) {
            let kept: HashSet<&String> = list.iter().collect();
            let target = after
                .iter()
                .filter(|x| kept.contains(x))
                .position(|x| *x == id)
                .expect("stays");
            let sent = self.sent.get_mut(&id).expect("sent");
            let set: Row = self.rows[&id]
                .iter()
                .filter(|(k, v)| sent.get(*k) != Some(*v))
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect();
            for (k, v) in &set {
                sent.insert(k.clone(), v.clone());
            }
            let to = (target != cur).then_some(target);
            if let Some(t) = to {
                let x = list.remove(cur);
                list.insert(t, x);
            }
            if !set.is_empty() || to.is_some() {
                diffs.push(Diff::Update {
                    id: id.clone(),
                    set,
                    index: to,
                });
            }
        }
        let ordered = self.ordered();
        let present: HashSet<String> = list.iter().cloned().collect();
        for (j, x) in after.iter().enumerate() {
            if !present.contains(x) {
                let row = self.rows[x].clone();
                self.sent.insert(x.clone(), row.clone());
                diffs.push(Diff::Insert {
                    row,
                    index: ordered.then_some(j),
                    id: Some(x.clone()),
                });
            }
        }
    }

    /// The sort key of a finalized row, and the row as sent (without hidden columns).
    fn split(&self, mut row: Row) -> (SortKey, Row) {
        let key = self
            .terms
            .iter()
            .map(|t| decode(row.get(&t.column), &t.ty).unwrap_or(Value::Null))
            .collect();
        for h in &self.hidden {
            row.shift_remove(h);
        }
        (key, row)
    }

    fn cmp(&self, a: &SortKey, a_seq: u64, b: &Entry) -> Ordering {
        self.order.cmp(a, &b.key).then(a_seq.cmp(&b.seq))
    }

    /// Rank a group by `key`, after the groups it ties with.
    fn place(&mut self, id: &str, key: SortKey) {
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
    }

    /// Where a group is in `ranked`: a binary search by its (key, seq), which are unique.
    fn position(&self, id: &str) -> Option<usize> {
        let (key, seq) = self.keys.get(id)?;
        let at = self
            .ranked
            .partition_point(|e| self.cmp(key, *seq, e) == Ordering::Greater);
        debug_assert_eq!(self.ranked[at].id, id);
        Some(at)
    }
}

/// A group's id: its key values as a JSON array, e.g. `["PL"]`.
pub fn group_id(key: &[Json]) -> String {
    Json::Array(key.to_vec()).to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::diff::ResultList;
    use serde_json::json;

    fn int() -> Type {
        Type::parse("Int64")
    }

    fn term(column: &str, desc: bool) -> GroupOrderTerm {
        GroupOrderTerm {
            column: column.into(),
            ty: int(),
            desc,
            nulls_first: false,
        }
    }

    fn row(g: &str, n: i64) -> (String, Row) {
        let r = json!({"g": g, "n": n}).as_object().unwrap().clone();
        (format!("[\"{g}\"]"), r)
    }

    fn lines(diffs: &[Diff]) -> Vec<String> {
        diffs.iter().map(|d| d.to_string()).collect()
    }

    #[test]
    fn moves_within_an_ordered_result() {
        let mut r = GroupResult::new(vec![term("n", true)], None, None, vec![]);
        assert_eq!(
            lines(&r.history(vec![row("a", 1), row("b", 5), row("c", 3)])),
            [
                r#"+ ["b"] {"g":"b","n":5}"#,
                r#"+ ["c"] {"g":"c","n":3}"#,
                r#"+ ["a"] {"g":"a","n":1}"#
            ]
        );
        assert_eq!(
            lines(&r.batch(vec![
                row("a", 4), // up, past c
                row("b", 6), // stays first: no index
                row("d", 3), // new, after c (ties go after)
            ])),
            [
                r#"~ ["a"] @1 {"n":4}"#,
                r#"~ ["b"] {"n":6}"#,
                r#"+ @3 ["d"] {"g":"d","n":3}"#
            ]
        );
        // down, to the end; and a touch that changes nothing sends nothing
        assert_eq!(
            lines(&r.batch(vec![row("b", 0), row("c", 3)])),
            [r#"~ ["b"] @3 {"n":0}"#]
        );
    }

    #[test]
    fn a_limit_brings_groups_in_and_out() {
        let mut r = GroupResult::new(vec![term("n", true)], Some(2), None, vec![]);
        assert_eq!(
            lines(&r.history(vec![row("a", 1), row("b", 5), row("c", 3)])),
            [r#"+ ["b"] {"g":"b","n":5}"#, r#"+ ["c"] {"g":"c","n":3}"#]
        );
        assert_eq!(
            lines(&r.batch(vec![row("a", 9)])),
            // in at the top; the last one goes
            [r#"+ @0 ["a"] {"g":"a","n":9}"#, "- @2"]
        );
        assert_eq!(
            lines(&r.batch(vec![row("a", 0)])),
            // out; the next one comes back, with its full row
            ["- @0", r#"+ @1 ["c"] {"g":"c","n":3}"#]
        );
        // below the LIMIT: nothing to send, but remembered
        assert_eq!(
            lines(&r.batch(vec![row("d", 1), row("a", 2)])),
            Vec::<String>::new()
        );
        assert_eq!(
            lines(&r.batch(vec![row("b", 1)])),
            ["- @0", r#"+ @1 ["a"] {"g":"a","n":2}"#]
        );
    }

    #[test]
    fn unordered_results_append_and_a_limit_keeps_the_first_groups() {
        let mut r = GroupResult::new(vec![], None, None, vec![]);
        r.history(vec![row("a", 1)]);
        assert_eq!(
            lines(&r.batch(vec![row("b", 2), row("a", 7)])),
            [r#"+ ["b"] {"g":"b","n":2}"#, r#"~ ["a"] {"n":7}"#]
        );
        let mut r = GroupResult::new(vec![], Some(1), None, vec![]);
        assert_eq!(lines(&r.history(vec![row("a", 1), row("b", 1)])).len(), 1);
        assert_eq!(
            lines(&r.batch(vec![row("b", 5), row("c", 1), row("a", 2)])),
            [r#"~ ["a"] {"n":2}"#]
        );
        // OFFSET 1 LIMIT 2: the 2nd and 3rd groups seen; they never move
        let mut r = GroupResult::new(vec![], Some(2), Some(1), vec![]);
        assert_eq!(
            lines(&r.history(vec![row("a", 1), row("b", 1)])),
            [r#"+ ["b"] {"g":"b","n":1}"#]
        );
        assert_eq!(
            lines(&r.batch(vec![row("a", 3), row("c", 1), row("d", 1), row("b", 4)])),
            [r#"+ ["c"] {"g":"c","n":1}"#, r#"~ ["b"] {"n":4}"#]
        );
    }

    #[test]
    fn hidden_order_columns_are_not_sent() {
        let mut r = GroupResult::new(vec![term("__o0", false)], None, None, vec!["__o0".into()]);
        let with = |g: &str, o: i64| {
            let (id, mut r) = row(g, 1);
            r.insert("__o0".into(), json!(o));
            (id, r)
        };
        assert_eq!(
            lines(&r.history(vec![with("a", 2), with("b", 1)])),
            [r#"+ ["b"] {"g":"b","n":1}"#, r#"+ ["a"] {"g":"a","n":1}"#]
        );
        // only the hidden column changed: the row moves, with nothing to set
        assert_eq!(lines(&r.batch(vec![with("a", 0)])), [r#"~ ["a"] @0 {}"#]);
    }

    /// A seeded generator of numbers below `n`.
    fn lcg(mut seed: u64) -> impl FnMut(u64) -> u64 {
        move |n: u64| {
            seed = seed
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            (seed >> 33) % n
        }
    }

    /// Sorting the history once gives exactly what placing its groups one at a time gave: the
    /// same ranking (ties in arrival order, NULLs where the term puts them), seqs and window.
    #[test]
    fn history_ranks_like_placing_one_at_a_time() {
        let mut rand = lcg(7);
        for (desc, nulls_first, limit, offset) in [
            (true, false, None, None),
            (false, false, Some(10), None),
            (true, true, Some(25), Some(40)),
            (false, true, None, Some(3)),
        ] {
            let terms = vec![GroupOrderTerm {
                nulls_first,
                ..term("n", desc)
            }];
            let groups: Vec<(String, Row)> = (0..500)
                .map(|i| {
                    let g = format!("g{i}");
                    // few distinct keys: many ties; some NULLs
                    let n = match rand(12) {
                        0 => Json::Null,
                        k => json!(k),
                    };
                    let r = json!({"g": g, "n": n}).as_object().unwrap().clone();
                    (format!("[\"{g}\"]"), r)
                })
                .collect();
            let mut sorted = GroupResult::new(terms.clone(), limit, offset, vec![]);
            let diffs = sorted.history(groups.clone());
            let mut placed = GroupResult::new(terms, limit, offset, vec![]);
            for (id, row) in groups {
                let (key, row) = placed.split(row);
                placed.place(&id, key);
                placed.rows.insert(id, row);
            }
            let ranked = |r: &GroupResult| -> Vec<(String, u64)> {
                r.ranked.iter().map(|e| (e.id.clone(), e.seq)).collect()
            };
            assert_eq!(ranked(&sorted), ranked(&placed));
            assert_eq!(sorted.keys, placed.keys);
            assert_eq!(sorted.seq, placed.seq);
            let want: Vec<String> = placed
                .ranked
                .iter()
                .skip(offset.map_or(0, |m| m as usize))
                .take(limit.map_or(usize::MAX, |n| n as usize))
                .map(|e| e.id.clone())
                .collect();
            let got: Vec<String> = diffs
                .iter()
                .map(|d| match d {
                    Diff::Insert { id: Some(id), .. } => id.clone(),
                    other => panic!("{other}"),
                })
                .collect();
            assert_eq!(got, want);
        }
    }

    /// Random batches: the client's list, built from the diffs alone, is always the first
    /// `limit` groups in order, each with its latest row.
    #[test]
    fn diffs_rebuild_the_ordered_result() {
        let mut rand = lcg(42);
        for (desc, limit, offset) in [
            (true, None, None),
            (false, Some(3), None),
            (true, Some(5), None),
            (true, Some(1), None),
            (true, Some(3), Some(2)),
            (false, None, Some(3)),
            (true, Some(1), Some(4)),
        ] {
            let mut r = GroupResult::new(vec![term("n", desc)], limit, offset, vec![]);
            let mut latest: HashMap<String, i64> = HashMap::new();
            let mut list = ResultList::default();
            // a random history of most groups, with ties
            let history: Vec<(String, Row)> = (0..6)
                .map(|i| row(&format!("g{i}"), rand(4) as i64))
                .collect();
            for (id, row) in &history {
                latest.insert(id.clone(), row["n"].as_i64().unwrap());
            }
            for d in r.history(history) {
                list.apply(&d).unwrap();
            }
            for _ in 0..300 {
                let batch: Vec<(String, Row)> = (0..1 + rand(4))
                    .map(|_| {
                        let g = format!("g{}", rand(8));
                        let n = rand(10) as i64 - 3; // sums can go down
                        row(&g, n)
                    })
                    .collect();
                for (id, row) in &batch {
                    latest.insert(id.clone(), row["n"].as_i64().unwrap());
                }
                for d in r.batch(batch) {
                    list.apply(&d).unwrap_or_else(|e| panic!("{d}: {e}"));
                }
                let mut want: Vec<i64> = latest.values().copied().collect();
                want.sort_by(|a, b| if desc { b.cmp(a) } else { a.cmp(b) });
                let want: Vec<i64> = want
                    .into_iter()
                    .skip(offset.map_or(0, |m| m as usize))
                    .take(limit.map_or(usize::MAX, |n| n as usize))
                    .collect();
                let got: Vec<i64> = list.rows.iter().map(|r| r["n"].as_i64().unwrap()).collect();
                assert_eq!(got, want, "desc {desc}, limit {limit:?}, offset {offset:?}");
                for (id, r) in list.ids.iter().zip(&list.rows) {
                    assert_eq!(latest[id.as_ref().unwrap()], r["n"].as_i64().unwrap());
                }
                let mut ids = list.ids.clone();
                ids.sort();
                ids.dedup();
                assert_eq!(ids.len(), list.rows.len(), "ids unique");
            }
        }
    }
}
