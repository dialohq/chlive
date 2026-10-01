use serde_json::{Map, Value};

/// One result row, keyed by output column name.
pub type Row = Map<String, Value>;

/// A change to the query result.
#[derive(Debug, Clone, PartialEq)]
pub enum Diff {
    /// A row was added. `SELECT *` over an append-only stream only ever adds rows.
    Insert(Row),
}

impl std::fmt::Display for Diff {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Diff::Insert(row) => write!(f, "+ {}", Value::Object(row.clone())),
        }
    }
}
