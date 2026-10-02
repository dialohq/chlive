use serde_json::{Map, Value};

/// One result row, keyed by output column name.
pub type Row = Map<String, Value>;

/// A change to the query result.
#[derive(Debug, Clone, PartialEq)]
pub enum Diff {
    /// A row was added: at `index` in an ordered result (the row there and every row after it
    /// move one down), or at the end when the result is unordered (`None`).
    Insert { row: Row, index: Option<usize> },
    /// The row at `index` was removed (pushed out by LIMIT); the rows after it move one up.
    Delete { index: usize },
}

impl Diff {
    /// An unordered insert (appended).
    pub fn insert(row: Row) -> Self {
        Diff::Insert { row, index: None }
    }

    /// Apply to a result list, the way a client does. Fails on an index outside the list.
    pub fn apply(&self, rows: &mut Vec<Row>) -> Result<(), String> {
        match self {
            Diff::Insert { row, index: None } => rows.push(row.clone()),
            Diff::Insert {
                row,
                index: Some(i),
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

/// `+ {row}`, `+ @2 {row}` with an index, `- @3` for a delete.
impl std::fmt::Display for Diff {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Diff::Insert { row, index: None } => write!(f, "+ {}", Value::Object(row.clone())),
            Diff::Insert {
                row,
                index: Some(i),
            } => write!(f, "+ @{i} {}", Value::Object(row.clone())),
            Diff::Delete { index } => write!(f, "- @{index}"),
        }
    }
}
