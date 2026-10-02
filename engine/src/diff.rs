use serde_json::{Map, Value};

/// One result row, keyed by output column name.
pub type Row = Map<String, Value>;

/// A change to the query result.
#[derive(Debug, Clone, PartialEq)]
pub enum Diff {
    /// A row was added: at `index` in an ordered result (the row there and every row after it
    /// move one down), or at the end when the result is unordered (`None`).
    Insert { row: Row, index: Option<usize> },
}

impl Diff {
    /// An unordered insert (appended).
    pub fn insert(row: Row) -> Self {
        Diff::Insert { row, index: None }
    }
}

/// `+ {row}`, or `+ @2 {row}` with an index.
impl std::fmt::Display for Diff {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Diff::Insert { row, index: None } => write!(f, "+ {}", Value::Object(row.clone())),
            Diff::Insert {
                row,
                index: Some(i),
            } => write!(f, "+ @{i} {}", Value::Object(row.clone())),
        }
    }
}
