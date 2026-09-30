//! Keyed results and the diff between two of them.

use indexmap::IndexMap;
use serde_json::{Map, Value};

use crate::rewriter::Plan;

pub type Row = Map<String, Value>;

/// Result rows keyed by `__key`, in result order.
pub type KeyedRows = IndexMap<String, Row>;

pub const KEY_FIELD: &str = "__key";

#[derive(Debug, Default, PartialEq)]
pub struct Diff {
    pub upserts: Vec<Row>,
    pub deletes: Vec<String>,
}

impl Diff {
    pub fn is_empty(&self) -> bool {
        self.upserts.is_empty() && self.deletes.is_empty()
    }
}

/// JSON-encoded array of the key column values, e.g. `["PL"]`.
pub fn row_key(key_columns: &[String], row: &Row) -> String {
    let vals: Vec<&Value> = key_columns.iter().map(|c| row.get(c).unwrap_or(&Value::Null)).collect();
    serde_json::to_string(&vals).expect("serializable")
}

/// Turn raw merge-query rows into client rows: `__key` plus the visible output columns.
pub fn key_rows(plan: &Plan, data: Vec<Row>) -> KeyedRows {
    let mut out = KeyedRows::with_capacity(data.len());
    for raw in data {
        let key = row_key(&plan.key_columns, &raw);
        let mut row = Row::new();
        row.insert(KEY_FIELD.into(), Value::String(key.clone()));
        for c in &plan.output_columns {
            row.insert(c.clone(), raw.get(c).cloned().unwrap_or(Value::Null));
        }
        if out.insert(key.clone(), row).is_some() {
            tracing::warn!(key, "duplicate key in merge result");
        }
    }
    out
}

pub fn diff(old: &KeyedRows, new: &KeyedRows) -> Diff {
    let upserts = new
        .iter()
        .filter(|(k, row)| old.get(*k) != Some(*row))
        .map(|(_, row)| row.clone())
        .collect();
    let deletes = old.keys().filter(|k| !new.contains_key(*k)).cloned().collect();
    Diff { upserts, deletes }
}

pub fn order(rows: &KeyedRows) -> Vec<String> {
    rows.keys().cloned().collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn rows(items: &[(&str, i64)]) -> KeyedRows {
        items
            .iter()
            .map(|(c, n)| {
                let key = row_key(&["country".into()], json!({"country": c}).as_object().unwrap());
                let row = json!({"__key": key, "country": c, "n": n}).as_object().unwrap().clone();
                (key, row)
            })
            .collect()
    }

    #[test]
    fn keys_are_json_arrays() {
        let r = json!({"a": "PL", "b": 3});
        assert_eq!(row_key(&["a".into(), "b".into()], r.as_object().unwrap()), r#"["PL",3]"#);
        assert_eq!(row_key(&[], r.as_object().unwrap()), "[]");
    }

    #[test]
    fn diff_upserts_changed_and_new_deletes_missing() {
        let old = rows(&[("PL", 1), ("DE", 2), ("FR", 3)]);
        let new = rows(&[("PL", 1), ("DE", 5), ("US", 1)]);
        let d = diff(&old, &new);
        let up: Vec<_> = d.upserts.iter().map(|r| r["country"].as_str().unwrap()).collect();
        assert_eq!(up, ["DE", "US"]);
        assert_eq!(d.deletes, [r#"["FR"]"#]);
        assert!(diff(&new, &new).is_empty());
    }
}
