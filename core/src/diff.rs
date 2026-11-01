use std::collections::HashMap;

use serde_json::{Map, Value};

/// One result row, keyed by output column name.
pub type Row = Map<String, Value>;

/// A change to the query result.
#[derive(Debug, Clone, PartialEq)]
pub enum Diff {
    /// A row was added: at `index` in an ordered result (the row there and every row after it
    /// move one down), or at the end when the result is unordered (`None`). Rows of aggregated
    /// results carry an `id` (their group's key) that later updates refer to.
    Insert {
        row: Row,
        index: Option<usize>,
        id: Option<String>,
    },
    /// The row at `index` was removed (pushed out by LIMIT); the rows after it move one up.
    Delete { index: usize },
    /// The row with this `id` was removed (FINAL: deleted, or no longer matching); the rows
    /// after it move one up.
    DeleteId { id: String },
    /// The row with this `id` has new values in these columns (aggregated results), and, with
    /// an `index` (ordered results), it moves: it's taken out, and put back so that it is at
    /// `index`.
    ///
    /// `edit` changes array columns in place (a `groupArray` gaining and losing elements)
    /// instead of setting them whole; a column is in `set` or in `edit`, never both.
    Update {
        id: String,
        set: Row,
        index: Option<usize>,
        edit: Vec<(String, ArrayEdit)>,
    },
}

/// A change to an array column: the elements in `remove` (`(start, count)` ranges of positions
/// in the array before it, ascending and apart) go, then `append` is added at the end.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct ArrayEdit {
    pub remove: Vec<(usize, usize)>,
    pub append: Vec<Value>,
}

impl ArrayEdit {
    /// Apply to an array value.
    pub fn apply(&self, array: &mut Value) -> Result<(), String> {
        let Value::Array(items) = array else {
            return Err(format!("an array edit of a non-array value {array}"));
        };
        let mut end = 0;
        for &(start, count) in &self.remove {
            if start < end || start + count > items.len() {
                return Err(format!(
                    "removing [{start}, {count}] of an array of {} elements",
                    items.len()
                ));
            }
            end = start + count;
        }
        for &(start, count) in self.remove.iter().rev() {
            items.drain(start..start + count);
        }
        items.extend(self.append.iter().cloned());
        Ok(())
    }

    /// The edit from one array to the next, by the elements' identities (ascending in both):
    /// the ones gone, and the new ones, which must all come after the old ones. `None` when
    /// they don't, or when the edit is no smaller than the new array itself.
    pub fn between(old: &[u64], new: &[u64], values: &[Value]) -> Option<ArrayEdit> {
        let mut remove: Vec<(usize, usize)> = Vec::new();
        let mut j = 0;
        for (i, id) in old.iter().enumerate() {
            if new.get(j) == Some(id) {
                j += 1;
            } else {
                match remove.last_mut() {
                    Some((start, count)) if *start + *count == i => *count += 1,
                    _ => remove.push((i, 1)),
                }
            }
        }
        // what's left of the new ones: appended, after every old one
        if old
            .last()
            .is_some_and(|last| new[j..].first().is_some_and(|f| f <= last))
        {
            return None;
        }
        let removed: usize = remove.iter().map(|(_, c)| c).sum();
        let appended = new.len() - j;
        if removed + appended >= new.len() || old.len() - removed != j {
            return None;
        }
        Some(ArrayEdit {
            remove,
            append: values[j..].to_vec(),
        })
    }
}

impl Diff {
    /// An unordered insert (appended).
    pub fn insert(row: Row) -> Self {
        Diff::Insert {
            row,
            index: None,
            id: None,
        }
    }

    /// Apply to a result list without row ids, the way a client does. Fails on an index
    /// outside the list, and on updates (they need ids: see [`ResultList`]).
    pub fn apply(&self, rows: &mut Vec<Row>) -> Result<(), String> {
        match self {
            Diff::Insert {
                row, index: None, ..
            } => rows.push(row.clone()),
            Diff::Insert {
                row,
                index: Some(i),
                ..
            } if *i <= rows.len() => rows.insert(*i, row.clone()),
            Diff::Delete { index } if *index < rows.len() => {
                rows.remove(*index);
            }
            other => {
                return Err(format!(
                    "{other} doesn't fit a result of {} rows",
                    rows.len()
                ))
            }
        }
        Ok(())
    }
}

/// A result as a client keeps it: rows, and the ids of those that have one, found by id in
/// O(1). Build it with [`ResultList::push`] or by applying diffs.
#[derive(Debug, Clone, Default)]
pub struct ResultList {
    pub rows: Vec<Row>,
    pub ids: Vec<Option<String>>,
    /// Where each id is in `rows`.
    by_id: HashMap<String, usize>,
}

impl PartialEq for ResultList {
    fn eq(&self, other: &Self) -> bool {
        self.rows == other.rows && self.ids == other.ids
    }
}

impl ResultList {
    /// Append a row.
    pub fn push(&mut self, row: Row, id: Option<String>) {
        if let Some(id) = &id {
            self.by_id.insert(id.clone(), self.rows.len());
        }
        self.rows.push(row);
        self.ids.push(id);
    }

    pub fn apply(&mut self, diff: &Diff) -> Result<(), String> {
        match diff {
            Diff::Update {
                id,
                set,
                index,
                edit,
            } => {
                let i = self
                    .position(id)
                    .ok_or_else(|| format!("update of unknown row {id}"))?;
                for (k, v) in set {
                    self.rows[i].insert(k.clone(), v.clone());
                }
                for (column, e) in edit {
                    let value = self.rows[i]
                        .get_mut(column)
                        .ok_or_else(|| format!("an edit of {id}'s unknown column {column}"))?;
                    e.apply(value)?;
                }
                if let Some(to) = *index {
                    if to >= self.rows.len() {
                        return Err(format!(
                            "move to index {to} of a result with {} rows",
                            self.rows.len()
                        ));
                    }
                    let row = self.rows.remove(i);
                    let id = self.ids.remove(i);
                    self.rows.insert(to, row);
                    self.ids.insert(to, id);
                    self.reindex(i.min(to));
                }
            }
            Diff::Insert {
                row,
                index: None,
                id,
            } => self.push(row.clone(), id.clone()),
            Diff::Insert {
                index: Some(i), id, ..
            } => {
                diff.apply(&mut self.rows)?;
                self.ids.insert(*i, id.clone());
                self.reindex(*i);
            }
            Diff::Delete { index } => {
                diff.apply(&mut self.rows)?;
                if let Some(id) = self.ids.remove(*index) {
                    self.by_id.remove(&id);
                }
                self.reindex(*index);
            }
            Diff::DeleteId { id } => {
                let i = self
                    .position(id)
                    .ok_or_else(|| format!("delete of unknown row {id}"))?;
                self.rows.remove(i);
                self.ids.remove(i);
                self.by_id.remove(id);
                self.reindex(i);
            }
        }
        Ok(())
    }

    /// The row with this id.
    pub fn get(&self, id: &str) -> Option<&Row> {
        Some(&self.rows[self.position(id)?])
    }

    fn position(&self, id: &str) -> Option<usize> {
        self.by_id.get(id).copied()
    }

    /// After rows moved from `start` on: their ids' positions again.
    fn reindex(&mut self, start: usize) {
        for (i, id) in self.ids.iter().enumerate().skip(start) {
            if let Some(id) = id {
                self.by_id.insert(id.clone(), i);
            }
        }
    }
}

/// `+ {row}`, `+ @2 {row}` with an index, `+ ["PL"] {row}` with an id, `- @3` for a delete,
/// `- ["PL"]` for one by id, `~ ["PL"] {set}` for an update, `~ ["PL"] @1 {set}` if it moves.
impl std::fmt::Display for Diff {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Diff::Insert { row, index, id } => {
                write!(f, "+")?;
                if let Some(i) = index {
                    write!(f, " @{i}")?;
                }
                if let Some(id) = id {
                    write!(f, " {id}")?;
                }
                write!(f, " {}", Value::Object(row.clone()))
            }
            Diff::Delete { index } => write!(f, "- @{index}"),
            Diff::DeleteId { id } => write!(f, "- {id}"),
            Diff::Update {
                id,
                set,
                index,
                edit,
            } => {
                write!(f, "~ {id}")?;
                if let Some(i) = index {
                    write!(f, " @{i}")?;
                }
                write!(f, " {}", Value::Object(set.clone()))?;
                for (column, e) in edit {
                    write!(f, " {column}[")?;
                    for (start, count) in &e.remove {
                        write!(f, "-{start}+{count} ")?;
                    }
                    write!(f, "+{}]", Value::Array(e.append.clone()))?;
                }
                Ok(())
            }
        }
    }
}

impl Diff {
    /// Its wire form (the protocol's change).
    pub fn into_change(self) -> chlive_protocol::Change {
        match self {
            Diff::Insert { row, index, id } => chlive_protocol::Change::Insert { row, index, id },
            Diff::Delete { index } => chlive_protocol::Change::Delete {
                index: Some(index),
                id: None,
            },
            Diff::DeleteId { id } => chlive_protocol::Change::Delete {
                index: None,
                id: Some(id),
            },
            Diff::Update {
                id,
                set,
                index,
                edit,
            } => chlive_protocol::Change::Update {
                id,
                set,
                index,
                edit: edit
                    .into_iter()
                    .map(|(column, e)| {
                        let e = chlive_protocol::ArrayEdit {
                            remove: e.remove.into_iter().map(|(s, c)| [s, c]).collect(),
                            append: e.append,
                        };
                        (column, serde_json::to_value(e).expect("an edit is JSON"))
                    })
                    .collect(),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn lcg(mut seed: u64) -> impl FnMut(u64) -> u64 {
        move |n: u64| {
            seed = seed
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            (seed >> 33) % n
        }
    }

    /// Ids are found where they are, whatever moved: every id's indexed position is its
    /// position by a linear scan.
    #[test]
    fn ids_stay_found_through_inserts_moves_and_deletes() {
        let mut rand = lcg(7);
        let mut list = ResultList::default();
        let mut next = 0;
        for _ in 0..2000 {
            let len = list.rows.len() as u64;
            let id = |i: usize| list.ids[i].clone().unwrap();
            let diff = match rand(5) {
                0 | 1 => {
                    next += 1;
                    let index = (rand(2) == 0).then(|| rand(len + 1) as usize);
                    let row = Map::from_iter([("n".to_string(), Value::from(next))]);
                    Diff::Insert {
                        row,
                        index,
                        id: Some(format!("k{next}")),
                    }
                }
                2 if len > 0 => Diff::DeleteId {
                    id: id(rand(len) as usize),
                },
                3 if len > 0 => Diff::Delete {
                    index: rand(len) as usize,
                },
                4 if len > 0 => Diff::Update {
                    id: id(rand(len) as usize),
                    set: Map::new(),
                    index: (rand(2) == 0).then(|| rand(len) as usize),
                    edit: Vec::new(),
                },
                _ => continue,
            };
            list.apply(&diff).unwrap();
            assert_eq!(list.by_id.len(), list.rows.len());
            for (i, id) in list.ids.iter().enumerate() {
                assert_eq!(
                    list.position(id.as_deref().unwrap()),
                    Some(i),
                    "after {diff}"
                );
            }
        }
        assert!(list.apply(&Diff::DeleteId { id: "nope".into() }).is_err());
    }

    #[test]
    fn array_edits_remove_ranges_then_append() {
        let mut a = serde_json::json!([0, 1, 2, 3, 4, 5]);
        let e = ArrayEdit {
            remove: vec![(0, 2), (4, 1)],
            append: vec![Value::from(9)],
        };
        e.apply(&mut a).unwrap();
        assert_eq!(a, serde_json::json!([2, 3, 5, 9]));
        for bad in [vec![(5, 2)], vec![(2, 2), (3, 1)]] {
            let e = ArrayEdit {
                remove: bad,
                append: vec![],
            };
            assert!(e.apply(&mut serde_json::json!([0, 1, 2, 3, 4])).is_err());
        }
        assert!(ArrayEdit::default()
            .apply(&mut serde_json::json!(1))
            .is_err());
        // in a result list, by id
        let mut list = ResultList::default();
        list.push(
            Map::from_iter([("a".to_string(), serde_json::json!(["x", "y"]))]),
            Some("k".into()),
        );
        let d = Diff::Update {
            id: "k".into(),
            set: Map::new(),
            index: None,
            edit: vec![(
                "a".into(),
                ArrayEdit {
                    remove: vec![(0, 1)],
                    append: vec![Value::from("z")],
                },
            )],
        };
        assert_eq!(d.to_string(), r#"~ k {} a[-0+1 +["z"]]"#);
        list.apply(&d).unwrap();
        assert_eq!(list.rows[0]["a"], serde_json::json!(["y", "z"]));
    }

    /// Edits between random arrays (elements gone anywhere, new ones at the end) rebuild the
    /// new array; or there's none when the whole array is smaller.
    #[test]
    fn edits_between_arrays_rebuild_the_new_one() {
        let mut rand = lcg(3);
        let mut ids: Vec<u64> = (0..10).collect();
        let mut next = 10;
        let mut array: Value = Value::Array(ids.iter().map(|i| Value::from(*i)).collect());
        for _ in 0..2000 {
            let mut new: Vec<u64> = ids.iter().copied().filter(|_| rand(6) > 0).collect();
            for _ in 0..rand(4) {
                new.push(next);
                next += 1;
            }
            let values: Vec<Value> = new.iter().map(|i| Value::from(*i)).collect();
            match ArrayEdit::between(&ids, &new, &values) {
                Some(e) => {
                    let removed: usize = e.remove.iter().map(|(_, c)| c).sum();
                    assert!(removed + e.append.len() < new.len());
                    e.apply(&mut array).unwrap();
                }
                None => array = Value::Array(values.clone()),
            }
            assert_eq!(array, Value::Array(values));
            ids = new;
            if ids.is_empty() {
                ids.push(next);
                next += 1;
                array = serde_json::json!([ids[0]]);
            }
        }
        // new ones before an old one: no edit
        assert!(ArrayEdit::between(&[1, 5], &[1, 5, 3, 7, 8, 9], &vec![Value::Null; 6]).is_none());
    }
}
