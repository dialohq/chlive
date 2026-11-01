//! A live result as a client keeps it: the snapshot's rows, then each diff applied in order.
//! One implementation for every client (the Rust client natively, the TS client as
//! WebAssembly).
//!
//! Rows are read into `R` once, when they arrive (by the store's `parse`; for `R = Row`, as
//! they are). Aggregated results also keep each row's JSON, by id, so that an `update` (only the
//! columns that changed) can be merged into it and that one row read again.
//!
//! A diff is applied in two steps: first checked and prepared against a model of the result
//! (lengths and ids), where anything can fail; then committed, which can't. So a diff that
//! doesn't fit leaves the result as it was.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use chlive_protocol::{ArrayEdit, Change, Row};

/// The rows of a result, in order. Cheap to clone; a new one after every change (rows that
/// didn't change are shared).
pub type Rows<R> = Arc<Vec<Arc<R>>>;

/// One change, as applied to the result. Indexes are into the rows as the previous change of
/// the same diff left them.
#[derive(Debug)]
pub enum Applied<R> {
    /// At `index` with ORDER BY, appended without. Aggregated and FINAL rows have an `id`.
    Insert {
        row: Arc<R>,
        index: Option<usize>,
        id: Option<String>,
    },
    /// The row at `index` left the result; `row` is the row that left.
    Delete { index: usize, row: Arc<R> },
    /// A row with an id changed; `row` is the row now. With an `index`, it also moved there.
    Update {
        id: String,
        row: Arc<R>,
        index: Option<usize>,
    },
}

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

/// One step of a checked diff: indexes resolved, rows read.
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

pub struct ResultRows<R> {
    rows: Rows<R>,
    ids: Ids,
    /// Aggregated results: each row's JSON by id, to merge updates into.
    json: HashMap<String, Row>,
    parse: fn(Row) -> Result<R, String>,
}

impl ResultRows<Row> {
    /// Rows kept as JSON, as they come.
    pub fn json() -> Self {
        Self::new(Ok)
    }
}

impl<R> ResultRows<R> {
    /// Rows read into `R` by `parse` (an error fails the snapshot or diff).
    pub fn new(parse: fn(Row) -> Result<R, String>) -> Self {
        Self {
            rows: Arc::new(Vec::new()),
            ids: Ids::default(),
            json: HashMap::new(),
            parse,
        }
    }

    pub fn rows(&self) -> Rows<R> {
        self.rows.clone()
    }

    pub fn len(&self) -> usize {
        self.rows.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// Replaces everything (also after a reconnect: a snapshot is the whole result).
    pub fn snapshot(&mut self, rows: Vec<Row>, ids: Option<Vec<String>>) -> Result<(), String> {
        let parse = self.parse;
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
    pub fn diff(&mut self, changes: Vec<Change>) -> Result<Vec<Applied<R>>, String> {
        let parse = self.parse;
        // 1. check and prepare, against a model of the result
        let mut len = self.rows.len();
        let mut ids = self.ids.clone();
        let mut staged: HashMap<String, Row> = HashMap::new();
        let mut removed: HashSet<String> = HashSet::new();
        let mut ops: Vec<Op<R>> = Vec::with_capacity(changes.len());
        for change in changes {
            match change {
                Change::Insert { row, index, id } => {
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
                Change::Delete { index, id } => {
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
                Change::Update {
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
                        ArrayEdit::from_json(e)?.apply(value)?;
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
                    applied.push(Applied::Insert { row, index, id });
                }
                Op::Delete { index } => {
                    let row = rows.remove(index);
                    applied.push(Applied::Delete { index, row });
                }
                Op::Update { id, from, to, row } => {
                    match to {
                        None => rows[from] = row.clone(),
                        Some(to) => {
                            rows.remove(from);
                            rows.insert(to, row.clone());
                        }
                    }
                    applied.push(Applied::Update { id, row, index: to });
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
    use serde_json::json;

    fn change(v: serde_json::Value) -> Change {
        serde_json::from_value(v).unwrap()
    }

    fn rows(s: &ResultRows<Row>) -> Vec<serde_json::Value> {
        s.rows()
            .iter()
            .map(|r| serde_json::Value::Object((**r).clone()))
            .collect()
    }

    fn obj(v: serde_json::Value) -> Row {
        v.as_object().unwrap().clone()
    }

    #[test]
    fn inserts_deletes_and_moves_in_sequence() {
        let mut s = ResultRows::json();
        s.snapshot(vec![obj(json!({"a": 1})), obj(json!({"a": 2}))], None)
            .unwrap();
        let applied = s
            .diff(vec![
                change(json!({"op": "insert", "row": {"a": 0}, "index": 0})),
                change(json!({"op": "insert", "row": {"a": 3}})),
                change(json!({"op": "delete", "index": 1})),
            ])
            .unwrap();
        assert_eq!(
            rows(&s),
            [json!({"a": 0}), json!({"a": 2}), json!({"a": 3})]
        );
        assert!(matches!(&applied[2], Applied::Delete { index: 1, row } if row["a"] == 1));
    }

    #[test]
    fn updates_by_id_merge_move_and_edit_arrays() {
        let mut s = ResultRows::json();
        let ids = vec!["[\"PL\"]".to_string(), "[\"DE\"]".to_string()];
        s.snapshot(
            vec![
                obj(json!({"c": "PL", "n": 2, "xs": [1, 2]})),
                obj(json!({"c": "DE", "n": 1, "xs": []})),
            ],
            Some(ids),
        )
        .unwrap();
        s.diff(vec![change(
            json!({"op": "update", "id": "[\"DE\"]", "set": {"n": 5},
            "index": 0, "edit": {"xs": {"append": [9]}}}),
        )])
        .unwrap();
        assert_eq!(
            rows(&s),
            [
                json!({"c": "DE", "n": 5, "xs": [9]}),
                json!({"c": "PL", "n": 2, "xs": [1, 2]})
            ]
        );
        s.diff(vec![change(json!({"op": "delete", "id": "[\"PL\"]"}))])
            .unwrap();
        assert_eq!(rows(&s), [json!({"c": "DE", "n": 5, "xs": [9]})]);
    }

    #[test]
    fn a_diff_that_does_not_fit_changes_nothing() {
        let mut s = ResultRows::json();
        s.snapshot(vec![obj(json!({"a": 1}))], None).unwrap();
        let e = s
            .diff(vec![
                change(json!({"op": "insert", "row": {"a": 2}})),
                change(json!({"op": "delete", "index": 5})),
            ])
            .unwrap_err();
        assert!(e.contains("index 5"), "{e}");
        assert_eq!(rows(&s), [json!({"a": 1})]);
        assert!(s
            .diff(vec![change(json!({"op": "update", "id": "x", "set": {}}))])
            .is_err());
    }
}
