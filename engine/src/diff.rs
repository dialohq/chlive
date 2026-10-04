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
    /// The row with this `id` has new values in these columns (aggregated results).
    Update { id: String, set: Row },
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

/// A result as a client keeps it: rows, and the ids of those that have one.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct ResultList {
    pub rows: Vec<Row>,
    pub ids: Vec<Option<String>>,
}

impl ResultList {
    pub fn apply(&mut self, diff: &Diff) -> Result<(), String> {
        match diff {
            Diff::Update { id, set } => {
                let i = self
                    .ids
                    .iter()
                    .position(|x| x.as_deref() == Some(id.as_str()))
                    .ok_or_else(|| format!("update of unknown row {id}"))?;
                for (k, v) in set {
                    self.rows[i].insert(k.clone(), v.clone());
                }
            }
            Diff::Insert { index, id, .. } => {
                diff.apply(&mut self.rows)?;
                match index {
                    Some(i) => self.ids.insert(*i, id.clone()),
                    None => self.ids.push(id.clone()),
                }
            }
            Diff::Delete { index } => {
                diff.apply(&mut self.rows)?;
                self.ids.remove(*index);
            }
        }
        Ok(())
    }

    /// The row with this id.
    pub fn get(&self, id: &str) -> Option<&Row> {
        let i = self.ids.iter().position(|x| x.as_deref() == Some(id))?;
        Some(&self.rows[i])
    }
}

/// `+ {row}`, `+ @2 {row}` with an index, `+ ["PL"] {row}` with an id, `- @3` for a delete,
/// `~ ["PL"] {set}` for an update.
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
            Diff::Update { id, set } => write!(f, "~ {id} {}", Value::Object(set.clone())),
        }
    }
}
