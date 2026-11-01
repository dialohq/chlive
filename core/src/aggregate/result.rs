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

use crate::bound::{ColumnId, OrderKey};
use crate::diff::{ArrayEdit, Diff, Row};
use crate::order::{KeyOrder, Ranking, SortKey};
use crate::types::{decode, Type, Value};

/// An ORDER BY term of an aggregated result: the row column holding its value.
#[derive(Debug, Clone, PartialEq)]
pub struct GroupOrderTerm {
    pub column: String,
    pub ty: Type,
    pub desc: bool,
    pub nulls_first: bool,
}

pub struct GroupResult {
    terms: Vec<GroupOrderTerm>,
    limit: usize,
    /// The window starts after this many groups (OFFSET).
    offset: usize,
    /// Columns computed only to order by (`__o0`, …), never sent.
    hidden: Vec<String>,
    /// Every group, in result order.
    ranking: Ranking,
    /// Each group's latest row (without hidden columns).
    rows: HashMap<String, Row>,
    /// What the client has: the rows of the groups in the result, as sent.
    sent: HashMap<String, Row>,
    /// Array columns' elements (`groupArray`), by their identities: each group's latest, and
    /// as sent; an update then edits the client's array instead of sending it whole.
    arrays: HashMap<String, Arrays>,
    sent_arrays: HashMap<String, Arrays>,
}

/// A group's array columns: each one's elements' identities, in order.
pub type Arrays = Vec<(String, Vec<u64>)>;

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
            limit: limit.map_or(usize::MAX, |n| n as usize),
            offset: offset.map_or(0, |m| m as usize),
            hidden,
            ranking: Ranking::new(order),
            rows: HashMap::new(),
            sent: HashMap::new(),
            arrays: HashMap::new(),
            sent_arrays: HashMap::new(),
        }
    }

    /// The identities of a group's array elements, for its next row (before
    /// [`Self::history`] or [`Self::changes`]).
    pub fn set_arrays(&mut self, id: &str, arrays: Arrays) {
        if arrays.is_empty() {
            self.arrays.remove(id);
        } else {
            self.arrays.insert(id.to_string(), arrays);
        }
    }

    /// A group's row goes to the client (whole): remembered as sent.
    fn send(&mut self, id: &str) -> Row {
        let row = self.rows[id].clone();
        self.sent.insert(id.to_string(), row.clone());
        match self.arrays.get(id) {
            Some(a) => self.sent_arrays.insert(id.to_string(), a.clone()),
            None => self.sent_arrays.remove(id),
        };
        row
    }

    /// A group leaves the client: whether it had it.
    fn unsend(&mut self, id: &str) -> bool {
        self.sent_arrays.remove(id);
        self.sent.remove(id).is_some()
    }

    /// A sent group's update: the columns that changed, and the array columns among them as
    /// edits where that's smaller (see [`ArrayEdit::between`]).
    fn update_sent(&mut self, id: &str) -> (Row, Vec<(String, ArrayEdit)>) {
        let sent = self.sent.get_mut(id).expect("sent");
        let mut set: Row = self.rows[id]
            .iter()
            .filter(|(k, v)| sent.get(*k) != Some(*v))
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect();
        for (k, v) in &set {
            sent.insert(k.clone(), v.clone());
        }
        let mut edit = Vec::new();
        let new = self.arrays.get(id).cloned().unwrap_or_default();
        if let Some(old) = self.sent_arrays.get(id) {
            for (column, ids) in &new {
                let Some((_, old_ids)) = old.iter().find(|(c, _)| c == column) else {
                    continue;
                };
                let Some(Json::Array(values)) = set.get(column) else {
                    continue;
                };
                if let Some(e) = ArrayEdit::between(old_ids, ids, values) {
                    set.shift_remove(column);
                    edit.push((column.clone(), e));
                }
            }
        }
        if new.is_empty() {
            self.sent_arrays.remove(id);
        } else {
            self.sent_arrays.insert(id.to_string(), new);
        }
        (set, edit)
    }

    fn ordered(&self) -> bool {
        !self.terms.is_empty()
    }

    /// The ranking, every group's row and what was sent, for a checkpoint.
    pub fn save(&self) -> Json {
        let mut json = self.ranking.save();
        json["rows"] = serde_json::json!(self.rows);
        json["sent"] = serde_json::json!(self.sent);
        if !self.arrays.is_empty() || !self.sent_arrays.is_empty() {
            json["arrays"] = serde_json::json!(self.arrays);
            json["sent_arrays"] = serde_json::json!(self.sent_arrays);
        }
        json
    }

    /// Back to a saved ranking (this one made by [`GroupResult::new`] for the same query).
    pub fn load(&mut self, json: &Json) -> Result<(), String> {
        let bad = |what: &str| format!("checkpoint: bad group result ({what})");
        self.ranking.load(json)?;
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
        // older checkpoints have none: arrays then go whole once
        let arrays = |name: &str| -> Result<HashMap<String, Arrays>, String> {
            match &json[name] {
                Json::Null => Ok(HashMap::new()),
                v => serde_json::from_value(v.clone()).map_err(|_| bad(name)),
            }
        };
        self.arrays = arrays("arrays")?;
        self.sent_arrays = arrays("sent_arrays")?;
        Ok(())
    }

    /// The history: every group ranked; the window (`limit` after `offset`), in order, as
    /// appended inserts.
    ///
    /// The groups are sorted once by (key, arrival), the order [`Self::place`] keeps: placing
    /// them one at a time would shift the list on every insert, O(groups²).
    pub fn history(&mut self, groups: Vec<(String, Row)>) -> Vec<Diff> {
        let mut keys = Vec::with_capacity(groups.len());
        for (id, row) in groups {
            let (key, row) = self.split(row);
            self.rows.insert(id.clone(), row);
            keys.push((id, key));
        }
        self.ranking.history(keys);
        let mut diffs = Vec::new();
        let window: Vec<String> = self
            .ranking
            .iter()
            .skip(self.offset)
            .take(self.limit)
            .map(|e| e.id.clone())
            .collect();
        for id in window {
            let row = self.send(&id);
            diffs.push(Diff::Insert {
                row,
                index: None,
                id: Some(id),
            });
        }
        diffs
    }

    /// The groups a batch touched, with their new rows, in the order they were touched.
    pub fn batch(&mut self, groups: Vec<(String, Row)>) -> Vec<Diff> {
        self.changes(
            groups
                .into_iter()
                .map(|(id, row)| (id, Some(row)))
                .collect(),
        )
    }

    /// Like [`Self::batch`], with groups that are gone (`None`: their last row left).
    pub fn changes(&mut self, groups: Vec<(String, Option<Row>)>) -> Vec<Diff> {
        let mut diffs = Vec::new();
        for (id, row) in groups {
            match row {
                Some(row) => self.touch(id, row, &mut diffs),
                None => self.remove(id, &mut diffs),
            }
        }
        diffs
    }

    /// A group is gone: out of the result if it was in, and (LIMIT) the next one comes in.
    fn remove(&mut self, id: String, diffs: &mut Vec<Diff>) {
        self.arrays.remove(&id);
        let Some(old) = self.ranking.position(&id) else {
            self.rows.remove(&id);
            return;
        };
        if self.offset > 0 {
            // everything after it moves up a place: compare the windows
            let before = self.window();
            self.ranking.remove(&id);
            self.rows.remove(&id);
            let after = self.window();
            return self.rewindow(before, after, diffs);
        }
        self.ranking.remove(&id);
        self.rows.remove(&id);
        if !self.unsend(&id) {
            return;
        }
        diffs.push(Diff::Delete { index: old });
        // the group now last in the LIMIT comes in
        let n = self.limit;
        if n == usize::MAX {
            return;
        }
        if let Some(next) = self.ranking.get(n - 1).map(|e| e.id.clone()) {
            if !self.sent.contains_key(&next) {
                let row = self.send(&next);
                diffs.push(Diff::Insert {
                    row,
                    index: self.ordered().then_some(n - 1),
                    id: Some(next),
                });
            }
        }
    }

    /// From the window `before` to `after` (no group in both changed): deletes, then inserts.
    fn rewindow(&mut self, before: Vec<String>, after: Vec<String>, diffs: &mut Vec<Diff>) {
        use std::collections::HashSet;
        let staying: HashSet<&String> = after.iter().collect();
        let mut present: HashSet<String> = HashSet::new();
        for (i, x) in before.iter().enumerate().rev() {
            if staying.contains(x) {
                present.insert(x.clone());
            } else {
                diffs.push(Diff::Delete { index: i });
                self.unsend(x);
            }
        }
        let ordered = self.ordered();
        for (j, x) in after.iter().enumerate() {
            if !present.contains(x) {
                let row = self.send(x);
                diffs.push(Diff::Insert {
                    row,
                    index: ordered.then_some(j),
                    id: Some(x.clone()),
                });
            }
        }
    }

    fn touch(&mut self, id: String, row: Row, diffs: &mut Vec<Diff>) {
        if self.offset > 0 {
            return self.touch_window(id, row, diffs);
        }
        let (key, row) = self.split(row);
        let old = self.ranking.position(&id);
        let moved = match self.ranking.key(&id) {
            Some(k) => self.ranking.order().cmp(k, &key) != Ordering::Equal,
            None => true,
        };
        if moved {
            self.ranking.remove(&id);
            self.ranking.place(&id, key);
        }
        let new = self.ranking.position(&id).expect("just placed");
        self.rows.insert(id.clone(), row);
        let n = self.limit;
        let ordered = self.ordered();
        let index = move |i: usize| ordered.then_some(i);
        let was_in = self.sent.contains_key(&id);
        match (was_in, new < n) {
            (true, true) => {
                let (set, edit) = self.update_sent(&id);
                let to = (old != Some(new)).then_some(new);
                if !set.is_empty() || !edit.is_empty() || to.is_some() {
                    diffs.push(Diff::Update {
                        id,
                        set,
                        index: to,
                        edit,
                    });
                }
            }
            (true, false) => {
                // out of the LIMIT: the group now last in it comes in
                diffs.push(Diff::Delete {
                    index: old.expect("a sent group is ranked"),
                });
                self.unsend(&id);
                if let Some(next) = self.ranking.get(n - 1).map(|e| e.id.clone()) {
                    let row = self.send(&next);
                    diffs.push(Diff::Insert {
                        row,
                        index: index(n - 1),
                        id: Some(next),
                    });
                }
            }
            (false, true) => {
                let row = self.send(&id);
                diffs.push(Diff::Insert {
                    row,
                    index: index(new),
                    id: Some(id),
                });
                // the group pushed past the LIMIT goes
                if let Some(out) = self.ranking.get(n).map(|e| e.id.clone()) {
                    if self.unsend(&out) {
                        diffs.push(Diff::Delete { index: n });
                    }
                }
            }
            (false, false) => {}
        }
    }

    /// The ids in the window, in order.
    fn window(&self) -> Vec<String> {
        self.ranking
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
        let moved = match self.ranking.key(&id) {
            Some(k) => self.ranking.order().cmp(k, &key) != Ordering::Equal,
            None => true,
        };
        if moved {
            self.ranking.remove(&id);
            self.ranking.place(&id, key);
        }
        self.rows.insert(id.clone(), row);
        let after = self.window();
        let staying: HashSet<&String> = after.iter().collect();
        let mut list = before;
        for i in (0..list.len()).rev() {
            if !staying.contains(&list[i]) {
                diffs.push(Diff::Delete { index: i });
                self.unsend(&list[i]);
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
            let (set, edit) = self.update_sent(&id);
            let to = (target != cur).then_some(target);
            if let Some(t) = to {
                let x = list.remove(cur);
                list.insert(t, x);
            }
            if !set.is_empty() || !edit.is_empty() || to.is_some() {
                diffs.push(Diff::Update {
                    id: id.clone(),
                    set,
                    index: to,
                    edit,
                });
            }
        }
        let ordered = self.ordered();
        let present: HashSet<String> = list.iter().cloned().collect();
        for (j, x) in after.iter().enumerate() {
            if !present.contains(x) {
                let row = self.send(x);
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

    /// The ranking moved into `order::Ranking`; checkpoints keep their fields, so the ones
    /// already on disk still load.
    #[test]
    fn checkpoints_keep_their_format() {
        let mut r = GroupResult::new(vec![term("n", true)], Some(2), None, vec![]);
        r.history(vec![row("a", 1), row("b", 3), row("c", 2)]);
        let saved = r.save();
        let mut fields: Vec<&String> = saved.as_object().unwrap().keys().collect();
        fields.sort();
        assert_eq!(fields, ["ranked", "rows", "sent", "seq"]);
        let mut back = GroupResult::new(vec![term("n", true)], Some(2), None, vec![]);
        back.load(&saved).unwrap();
        assert_eq!(back.save(), saved);
        assert_eq!(
            lines(&back.batch(vec![row("a", 9)])),
            lines(&r.batch(vec![row("a", 9)]))
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
    fn groups_that_are_gone_leave_and_the_next_comes_in() {
        let gone = |g: &str| (format!("[\"{g}\"]"), None);
        let some = |g: &str, n: i64| {
            let (id, r) = row(g, n);
            (id, Some(r))
        };
        // unordered: out of its place
        let mut r = GroupResult::new(vec![], None, None, vec![]);
        r.history(vec![row("a", 1), row("b", 2), row("c", 3)]);
        assert_eq!(
            lines(&r.changes(vec![gone("b"), some("a", 5)])),
            ["- @1", r#"~ ["a"] {"n":5}"#]
        );
        // ordered, LIMIT 2: the next one comes in at the end
        let mut r = GroupResult::new(vec![term("n", true)], Some(2), None, vec![]);
        r.history(vec![row("a", 1), row("b", 5), row("c", 3)]);
        assert_eq!(
            lines(&r.changes(vec![gone("b")])),
            ["- @0", r#"+ @1 ["a"] {"g":"a","n":1}"#]
        );
        // one past the LIMIT, or never seen: nothing to send
        assert_eq!(lines(&r.changes(vec![gone("x")])), Vec::<String>::new());
        r.changes(vec![some("d", 0)]);
        assert_eq!(lines(&r.changes(vec![gone("d")])), Vec::<String>::new());
        // OFFSET 1 LIMIT 1: one above the window goes, everything moves up
        let mut r = GroupResult::new(vec![term("n", true)], Some(1), Some(1), vec![]);
        r.history(vec![row("a", 1), row("b", 5), row("c", 3)]);
        assert_eq!(
            lines(&r.changes(vec![gone("b")])),
            ["- @0", r#"+ @0 ["a"] {"g":"a","n":1}"#]
        );
    }

    /// A group with an array column `xs` of these elements (identity `i` holds `10 * i`).
    fn with_xs(r: &mut GroupResult, g: &str, n: i64, xs: &[u64]) -> (String, Row) {
        let (id, mut row) = row(g, n);
        row.insert(
            "xs".into(),
            json!(xs.iter().map(|i| i * 10).collect::<Vec<_>>()),
        );
        r.set_arrays(&id, vec![("xs".into(), xs.to_vec())]);
        (id, row)
    }

    #[test]
    fn array_columns_change_by_edits() {
        let mut r = GroupResult::new(vec![], None, None, vec![]);
        let h = with_xs(&mut r, "a", 1, &[1, 2, 3, 4]);
        r.history(vec![h]);
        // appended
        let t = with_xs(&mut r, "a", 2, &[1, 2, 3, 4, 5]);
        assert_eq!(lines(&r.batch(vec![t])), [r#"~ ["a"] {"n":2} xs[+[50]]"#]);
        // the oldest leave (time windows)
        let t = with_xs(&mut r, "a", 2, &[3, 4, 5]);
        assert_eq!(lines(&r.batch(vec![t])), [r#"~ ["a"] {} xs[-0+2 +[]]"#]);
        // one replaced (FINAL): out of its place, in at the end
        let t = with_xs(&mut r, "a", 2, &[3, 5, 6]);
        assert_eq!(lines(&r.batch(vec![t])), [r#"~ ["a"] {} xs[-1+1 +[60]]"#]);
        // mostly new: whole
        let t = with_xs(&mut r, "a", 2, &[7, 8]);
        assert_eq!(lines(&r.batch(vec![t])), [r#"~ ["a"] {"xs":[70,80]}"#]);
        // the checkpoint keeps what was sent
        let mut back = GroupResult::new(vec![], None, None, vec![]);
        back.load(&r.save()).unwrap();
        let t = with_xs(&mut back, "a", 2, &[7, 8, 9]);
        assert_eq!(lines(&back.batch(vec![t])), [r#"~ ["a"] {} xs[+[90]]"#]);
    }

    /// Random arrays (elements gone anywhere, new ones at the end) in random groups, with a
    /// LIMIT and ORDER BY: the client's list, built from the diffs alone, has every array.
    #[test]
    fn edits_rebuild_the_arrays() {
        let mut rand = lcg(9);
        for (limit, offset) in [(None, None), (Some(3), None), (Some(2), Some(1))] {
            let mut r = GroupResult::new(vec![term("n", true)], limit, offset, vec![]);
            let mut list = ResultList::default();
            let mut arrays: HashMap<String, (i64, Vec<u64>)> = HashMap::new();
            let mut next = 0;
            for d in r.history(vec![]) {
                list.apply(&d).unwrap();
            }
            for _ in 0..500 {
                let g = format!("g{}", rand(5));
                let id = format!("[\"{g}\"]");
                let (n, mut xs) = arrays.remove(&id).unwrap_or((0, Vec::new()));
                xs.retain(|_| rand(5) > 0);
                for _ in 0..rand(3) {
                    next += 1;
                    xs.push(next);
                }
                let n = if rand(3) == 0 { rand(4) as i64 } else { n };
                let change = if xs.is_empty() {
                    (id.clone(), None)
                } else {
                    let (id, row) = with_xs(&mut r, &g, n, &xs);
                    arrays.insert(id.clone(), (n, xs));
                    (id, Some(row))
                };
                for d in r.changes(vec![change]) {
                    list.apply(&d).unwrap_or_else(|e| panic!("{d}: {e}"));
                }
                for (id, row) in list.ids.iter().zip(&list.rows) {
                    let (_, xs) = &arrays[id.as_ref().unwrap()];
                    let want: Vec<u64> = xs.iter().map(|i| i * 10).collect();
                    assert_eq!(row["xs"], json!(want), "{limit:?} {offset:?}");
                }
            }
        }
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
                placed.ranking.place(&id, key);
                placed.rows.insert(id, row);
            }
            let ranked = |r: &GroupResult| -> Vec<(String, u64)> {
                r.ranking.iter().map(|e| (e.id.clone(), e.seq)).collect()
            };
            assert_eq!(ranked(&sorted), ranked(&placed));
            assert_eq!(sorted.ranking.save(), placed.ranking.save());
            let want: Vec<String> = placed
                .ranking
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
                let batch: Vec<(String, Option<Row>)> = (0..1 + rand(4))
                    .map(|_| {
                        let g = format!("g{}", rand(8));
                        let n = rand(10) as i64 - 3; // sums can go down
                        let (id, row) = row(&g, n);
                        // some groups are gone (their last row left)
                        (id, (rand(5) > 0).then_some(row))
                    })
                    .collect();
                for (id, row) in &batch {
                    match row {
                        Some(row) => latest.insert(id.clone(), row["n"].as_i64().unwrap()),
                        None => latest.remove(id),
                    };
                }
                for d in r.changes(batch) {
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
