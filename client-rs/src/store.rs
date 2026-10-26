//! A live result as the client keeps it: the snapshot's rows, then each diff applied in order
//! (a port of `ResultStore` in `client/src/state.ts`).
//!
//! Rows are deserialized into `R` once, when they arrive. Aggregated results also keep each
//! row's JSON, by id, so that an `update` (only the columns that changed) can be merged into it
//! and that one row deserialized again.
//!
//! A diff is applied in two steps: first checked and prepared against a model of the result
//! (lengths and ids), where anything can fail; then committed, which can't. So a diff that
//! doesn't fit leaves the result as it was.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use chlive_protocol::Row;
use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::Change;

/// The rows of a result, in order. Cheap to clone; a new one after every change (rows that
/// didn't change are shared).
pub type Rows<R> = Arc<Vec<Arc<R>>>;

/// Row ids (aggregated results), parallel to the rows, and where each one is.
#[derive(Debug, Clone, Default)]
struct Ids {
    list: Vec<Option<String>>,
    /// id → index; rebuilt when rows move.
    at: HashMap<String, usize>,
    stale: bool,
}

impl Ids {
    fn new(list: Vec<Option<String>>) -> Self {
        let mut ids = Ids {
            list,
            at: HashMap::new(),
            stale: true,
        };
        ids.reindex();
        ids
    }

    fn reindex(&mut self) {
        if self.stale {
            self.at = self
                .list
                .iter()
                .enumerate()
                .filter_map(|(i, id)| Some((id.clone()?, i)))
                .collect();
            self.stale = false;
        }
    }

    fn position(&mut self, id: &str) -> Option<usize> {
        self.reindex();
        self.at.get(id).copied()
    }

    fn insert(&mut self, index: Option<usize>, id: Option<String>) {
        match index {
            None => {
                if let (Some(id), false) = (&id, self.stale) {
                    self.at.insert(id.clone(), self.list.len());
                }
                self.list.push(id);
            }
            Some(i) => {
                self.list.insert(i, id);
                self.stale = true;
            }
        }
    }

    fn remove(&mut self, index: usize) -> Option<String> {
        self.stale = true;
        self.list.remove(index)
    }

    fn move_to(&mut self, from: usize, to: usize) {
        let id = self.list.remove(from);
        self.list.insert(to, id);
        self.stale = true;
    }
}

/// One step of a checked diff: indexes resolved, rows deserialized.
enum Op<R> {
    Insert {
        index: Option<usize>,
        row: Arc<R>,
        id: Option<String>,
    },
    Delete {
        index: usize,
    },
    Update {
        id: String,
        from: usize,
        to: Option<usize>,
        row: Arc<R>,
    },
}

pub(crate) struct Store<R> {
    rows: Rows<R>,
    ids: Ids,
    /// Aggregated results: each row's JSON by id, to merge updates into.
    json: HashMap<String, Row>,
}

impl<R> Default for Store<R> {
    fn default() -> Self {
        Store {
            rows: Arc::new(Vec::new()),
            ids: Ids::default(),
            json: HashMap::new(),
        }
    }
}

pub(crate) fn parse<R: DeserializeOwned>(row: Row) -> Result<R, String> {
    serde_json::from_value(Value::Object(row)).map_err(|e| format!("a row doesn't fit: {e}"))
}

impl<R: DeserializeOwned> Store<R> {
    pub(crate) fn rows(&self) -> Rows<R> {
        self.rows.clone()
    }

    /// Replaces everything (also after a reconnect: a snapshot is the whole result).
    pub(crate) fn snapshot(
        &mut self,
        rows: Vec<Row>,
        ids: Option<Vec<String>>,
    ) -> Result<(), String> {
        if let Some(ids) = &ids {
            if ids.len() != rows.len() {
                return Err(format!("{} ids for {} rows", ids.len(), rows.len()));
            }
        }
        let mut json = HashMap::new();
        let mut parsed = Vec::with_capacity(rows.len());
        for (i, row) in rows.into_iter().enumerate() {
            if let Some(ids) = &ids {
                json.insert(ids[i].clone(), row.clone());
            }
            parsed.push(Arc::new(parse(row)?));
        }
        let n = parsed.len();
        self.rows = Arc::new(parsed);
        self.ids = Ids::new(match ids {
            Some(ids) => ids.into_iter().map(Some).collect(),
            None => vec![None; n],
        });
        self.json = json;
        Ok(())
    }

    /// Applies a whole diff, in sequence (each index into the list as the previous change left
    /// it), and returns the changes as applied. On an error (an index outside the list, an
    /// unknown id, a row that doesn't fit `R`) nothing changes.
    pub(crate) fn diff(
        &mut self,
        changes: Vec<chlive_protocol::Change>,
    ) -> Result<Vec<Change<R>>, String> {
        // 1. check and prepare, against a model of the result
        let mut len = self.rows.len();
        let mut ids = self.ids.clone();
        let mut staged: HashMap<String, Row> = HashMap::new();
        let mut removed: HashSet<String> = HashSet::new();
        let mut ops: Vec<Op<R>> = Vec::with_capacity(changes.len());
        for change in changes {
            match change {
                chlive_protocol::Change::Insert { row, index, id } => {
                    if let Some(i) = index {
                        if i > len {
                            return Err(format!("insert at index {i} of a result with {len} rows"));
                        }
                    }
                    if let Some(id) = &id {
                        removed.remove(id);
                        staged.insert(id.clone(), row.clone());
                    }
                    ids.insert(index, id.clone());
                    len += 1;
                    ops.push(Op::Insert {
                        index,
                        row: Arc::new(parse(row)?),
                        id,
                    });
                }
                chlive_protocol::Change::Delete { index, id } => {
                    let index = match (index, id) {
                        (Some(i), None) => i,
                        (None, Some(id)) => ids
                            .position(&id)
                            .ok_or_else(|| format!("delete of unknown row {id}"))?,
                        _ => return Err("a delete needs exactly one of index and id".into()),
                    };
                    if index >= len {
                        return Err(format!(
                            "delete at index {index} of a result with {len} rows"
                        ));
                    }
                    if let Some(id) = ids.remove(index) {
                        staged.remove(&id);
                        removed.insert(id);
                    }
                    len -= 1;
                    ops.push(Op::Delete { index });
                }
                chlive_protocol::Change::Update {
                    id,
                    set,
                    index,
                    edit,
                } => {
                    let from = ids
                        .position(&id)
                        .ok_or_else(|| format!("update of unknown row {id}"))?;
                    if let Some(to) = index {
                        if to >= len {
                            return Err(format!("move to index {to} of a result with {len} rows"));
                        }
                    }
                    let mut json = match staged.remove(&id) {
                        Some(j) => j,
                        None => self.json.get(&id).cloned().unwrap_or_default(),
                    };
                    for (k, v) in set {
                        json.insert(k, v);
                    }
                    // array columns changed in place
                    for (column, e) in &edit {
                        let value = json
                            .get_mut(column)
                            .ok_or_else(|| format!("an edit of {id}'s unknown column {column}"))?;
                        chlive_protocol::ArrayEdit::from_json(e)?.apply(value)?;
                    }
                    let row = Arc::new(parse(json.clone())?);
                    staged.insert(id.clone(), json);
                    let to = index.filter(|to| *to != from);
                    if let Some(to) = to {
                        ids.move_to(from, to);
                    }
                    ops.push(Op::Update { id, from, to, row });
                }
            }
        }

        // 2. commit: in place if nobody holds the previous rows, on a copy otherwise
        let rows = Arc::make_mut(&mut self.rows);
        let mut applied = Vec::with_capacity(ops.len());
        for op in ops {
            match op {
                Op::Insert { index, row, id } => {
                    match index {
                        None => rows.push(row.clone()),
                        Some(i) => rows.insert(i, row.clone()),
                    }
                    applied.push(Change::Insert { row, index, id });
                }
                Op::Delete { index } => {
                    let row = rows.remove(index);
                    applied.push(Change::Delete { index, row });
                }
                Op::Update { id, from, to, row } => {
                    match to {
                        None => rows[from] = row.clone(),
                        Some(to) => {
                            rows.remove(from);
                            rows.insert(to, row.clone());
                        }
                    }
                    applied.push(Change::Update { id, row, index: to });
                }
            }
        }
        self.ids = ids;
        for id in removed {
            self.json.remove(&id);
        }
        self.json.extend(staged);
        Ok(applied)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::Deserialize;
    use serde_json::json;

    fn row(v: Value) -> Row {
        v.as_object().unwrap().clone()
    }

    fn change(v: Value) -> chlive_protocol::Change {
        serde_json::from_value(v).unwrap()
    }

    fn names(s: &Store<Row>) -> Vec<String> {
        s.rows()
            .iter()
            .map(|r| r["u"].as_str().unwrap().to_string())
            .collect()
    }

    #[test]
    fn snapshot_then_inserts_in_arrival_order() {
        let mut s = Store::<Row>::default();
        s.snapshot(vec![row(json!({"u": "h1"}))], None).unwrap();
        s.diff(vec![
            change(json!({"op": "insert", "row": {"u": "e1"}})),
            change(json!({"op": "insert", "row": {"u": "e2"}})),
        ])
        .unwrap();
        assert_eq!(names(&s), ["h1", "e1", "e2"]);
        // a snapshot replaces everything
        s.snapshot(vec![row(json!({"u": "x"}))], None).unwrap();
        assert_eq!(names(&s), ["x"]);
    }

    #[test]
    fn ordered_inserts_land_at_their_index_in_sequence() {
        let mut s = Store::<Row>::default();
        s.snapshot(vec![row(json!({"u": "a"})), row(json!({"u": "c"}))], None)
            .unwrap();
        // each index is into the list as the previous change left it
        s.diff(vec![
            change(json!({"op": "insert", "row": {"u": "b"}, "index": 1})),
            change(json!({"op": "insert", "row": {"u": "d"}, "index": 3})),
            change(json!({"op": "insert", "row": {"u": "_"}, "index": 0})),
        ])
        .unwrap();
        assert_eq!(names(&s), ["_", "a", "b", "c", "d"]);
    }

    #[test]
    fn a_change_that_does_not_fit_leaves_the_rows_untouched() {
        let mut s = Store::<Row>::default();
        s.snapshot(vec![row(json!({"u": "a"}))], None).unwrap();
        let before = s.rows();
        for bad in [
            json!({"op": "insert", "row": {"u": "b"}, "index": 5}),
            json!({"op": "delete", "index": 2}),
            json!({"op": "update", "id": "[\"x\"]", "set": {}}),
        ] {
            let err = s
                .diff(vec![
                    change(json!({"op": "insert", "row": {"u": "ok"}})),
                    change(bad.clone()),
                ])
                .unwrap_err();
            assert!(!err.is_empty(), "{bad}");
            assert!(Arc::ptr_eq(&s.rows(), &before), "{bad}");
        }
        assert_eq!(names(&s), ["a"]);
    }

    #[test]
    fn limit_inserts_and_deletes_apply_together() {
        let mut s = Store::<Row>::default();
        s.snapshot(vec![row(json!({"u": "a"})), row(json!({"u": "b"}))], None)
            .unwrap();
        let applied = s
            .diff(vec![
                change(json!({"op": "insert", "row": {"u": "new"}, "index": 0})),
                change(json!({"op": "delete", "index": 2})),
            ])
            .unwrap();
        assert_eq!(names(&s), ["new", "a"]);
        // the delete carries the row that left
        assert!(matches!(&applied[1], Change::Delete { index: 2, row } if row["u"] == "b"));
    }

    #[test]
    fn aggregated_rows_are_updated_by_id_and_moved_with_an_index() {
        let mut s = Store::<Row>::default();
        s.snapshot(
            vec![
                row(json!({"u": "PL", "n": 3, "avg": 1.5})),
                row(json!({"u": "DE", "n": 2, "avg": 2.0})),
            ],
            Some(vec!["[\"PL\"]".into(), "[\"DE\"]".into()]),
        )
        .unwrap();
        s.diff(vec![
            change(json!({"op": "update", "id": "[\"DE\"]", "set": {"n": 5}, "index": 0})),
            change(json!({"op": "insert", "row": {"u": "FR", "n": 1, "avg": 0}, "id": "[\"FR\"]"})),
            change(json!({"op": "update", "id": "[\"PL\"]", "set": {"avg": 9.5}})),
        ])
        .unwrap();
        assert_eq!(names(&s), ["DE", "PL", "FR"]);
        assert_eq!(s.rows()[0]["n"], 5);
        // columns not in `set` keep their values, in their order
        assert_eq!(s.rows()[1].keys().collect::<Vec<_>>(), ["u", "n", "avg"]);
        assert_eq!(s.rows()[1]["avg"], 9.5);
        // a group leaving and later updates of the ones left
        s.diff(vec![
            change(json!({"op": "delete", "index": 0})),
            change(json!({"op": "update", "id": "[\"FR\"]", "set": {"n": 7}, "index": 0})),
        ])
        .unwrap();
        assert_eq!(names(&s), ["FR", "PL"]);
        assert!(s
            .diff(vec![change(
                json!({"op": "update", "id": "[\"DE\"]", "set": {}})
            )])
            .is_err());
    }

    #[test]
    fn array_columns_are_edited_in_place() {
        let mut s = Store::<Row>::default();
        s.snapshot(
            vec![row(json!({"u": "PL", "users": ["a", "b", "c"]}))],
            Some(vec!["[\"PL\"]".into()]),
        )
        .unwrap();
        s.diff(vec![change(
            json!({"op": "update", "id": "[\"PL\"]", "set": {},
            "edit": {"users": {"remove": [[0, 2]], "append": ["d", "e"]}}}),
        )])
        .unwrap();
        assert_eq!(s.rows()[0]["users"], json!(["c", "d", "e"]));
        // a range outside the array: the diff fails, nothing changes
        assert!(s
            .diff(vec![change(
                json!({"op": "update", "id": "[\"PL\"]", "set": {},
                "edit": {"users": {"remove": [[2, 5]]}}})
            )])
            .is_err());
        assert_eq!(s.rows()[0]["users"], json!(["c", "d", "e"]));
    }

    #[derive(Debug, Deserialize, PartialEq)]
    struct Country {
        country: String,
        n: u64,
        avg: f64,
    }

    #[test]
    fn typed_rows_with_partial_updates() {
        let mut s = Store::<Country>::default();
        s.snapshot(
            vec![row(json!({"country": "PL", "n": 3, "avg": 1.5}))],
            Some(vec!["[\"PL\"]".into()]),
        )
        .unwrap();
        let applied = s
            .diff(vec![change(
                json!({"op": "update", "id": "[\"PL\"]", "set": {"n": 4}}),
            )])
            .unwrap();
        let want = Country {
            country: "PL".into(),
            n: 4,
            avg: 1.5,
        };
        assert_eq!(*s.rows()[0], want);
        assert!(matches!(&applied[0], Change::Update { row, index: None, .. } if **row == want));
        // a row that doesn't fit the type is an error, and changes nothing
        assert!(s
            .diff(vec![change(
                json!({"op": "insert", "row": {"country": "DE"}, "id": "[\"DE\"]"})
            )])
            .is_err());
        assert_eq!(s.rows().len(), 1);
    }

    #[test]
    fn rows_nobody_holds_are_changed_in_place() {
        let mut s = Store::<Row>::default();
        s.snapshot(vec![row(json!({"u": "a"}))], None).unwrap();
        let held = s.rows();
        s.diff(vec![change(json!({"op": "insert", "row": {"u": "b"}}))])
            .unwrap();
        // the reader's rows are untouched
        assert_eq!(held.len(), 1);
        assert_eq!(s.rows().len(), 2);
        // rows that didn't change are shared
        assert!(Arc::ptr_eq(&held[0], &s.rows()[0]));
    }
}
