//! FINAL results on a ReplacingMergeTree: one row per key, its latest version, as ClickHouse's
//! `SELECT … FINAL` has it. Rows have ids (their keys); each event is a new version of its key,
//! so it inserts, updates (the whole row) or deletes the key's row.
//!
//! Versions are trusted to arrive in order: an event replaces its key's row unless its version
//! is lower than the row's (ties, and tables without a version column, go to the later event,
//! as ClickHouse keeps the last inserted). That holds when each key's versions only grow and
//! all of them are in one Kafka partition (a keyed topic). Only the result's keys are kept,
//! with their versions.

use std::cmp::Ordering;
use std::collections::HashMap;

use serde_json::{json, Value as Json};

use crate::diff::{Diff, Row};
use crate::plan::KeyedPlan;
use crate::types::{decode, Value};

#[derive(Debug, Clone, Default)]
pub struct KeyedState {
    /// The result's rows by id: their versions (`Null` without a version column).
    versions: HashMap<String, Value>,
    /// Events older than their key's row, ignored.
    stale: u64,
}

impl KeyedState {
    /// A history row (one per key, the latest version; FINAL left out deleted ones).
    pub fn history(&mut self, id: String, ver: Value, row: Row) -> Diff {
        self.versions.insert(id.clone(), ver);
        Diff::Insert {
            row,
            index: None,
            id: Some(id),
        }
    }

    /// A new version of the key `id`: whether it's deleted, and whether it matches the WHERE;
    /// `row` renders it.
    pub fn event(
        &mut self,
        id: String,
        ver: Value,
        deleted: bool,
        matches: bool,
        row: impl FnOnce() -> Row,
    ) -> Option<Diff> {
        let shown = !deleted && matches;
        match self.versions.get_mut(&id) {
            Some(old) if older(&ver, old) => {
                self.stale += 1;
                tracing::debug!(id, ver = ?ver, row_ver = ?old, stale = self.stale, "an older version than the row's; ignored");
                None
            }
            Some(old) if shown => {
                *old = ver;
                Some(Diff::Update {
                    id,
                    set: row(),
                    index: None,
                })
            }
            Some(_) => {
                self.versions.remove(&id);
                Some(Diff::DeleteId { id })
            }
            None if shown => {
                self.versions.insert(id.clone(), ver);
                Some(Diff::Insert {
                    row: row(),
                    index: None,
                    id: Some(id),
                })
            }
            None => None,
        }
    }

    pub fn len(&self) -> usize {
        self.versions.len()
    }

    pub fn is_empty(&self) -> bool {
        self.versions.is_empty()
    }

    /// For a checkpoint: `{"versions": {id: version}}`.
    pub fn save(&self) -> Json {
        let versions: serde_json::Map<String, Json> = self
            .versions
            .iter()
            .map(|(id, v)| (id.clone(), v.to_json()))
            .collect();
        json!({ "versions": versions })
    }

    /// Back to a saved state, its versions read by the plan's version type.
    pub fn load(&mut self, plan: &KeyedPlan, json: &Json) -> Result<(), String> {
        let saved = json["versions"]
            .as_object()
            .ok_or("a keyed checkpoint without versions")?;
        self.versions = saved
            .iter()
            .map(|(id, v)| {
                let ver = match &plan.ver {
                    Some(c) => decode(Some(v), &c.ty).map_err(|e| e.to_string())?,
                    None => Value::Null,
                };
                Ok((id.clone(), ver))
            })
            .collect::<Result<_, String>>()?;
        Ok(())
    }
}

/// Whether `ver` is lower than the row's `old` (never without a version column).
fn older(ver: &Value, old: &Value) -> bool {
    match (ver, old) {
        (Value::Null, _) | (_, Value::Null) => false,
        _ => ver.compare(old) == Some(Ordering::Less),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn row(v: i64) -> Row {
        json!({ "v": v }).as_object().unwrap().clone()
    }

    fn lines(diffs: impl IntoIterator<Item = Option<Diff>>) -> Vec<String> {
        diffs.into_iter().flatten().map(|d| d.to_string()).collect()
    }

    const K: &str = "[\"k\"]";

    #[test]
    fn new_versions_insert_update_and_delete_their_row() {
        let mut s = KeyedState::default();
        let v = |n| Value::Int(n);
        let out = lines([
            // not in the result: matching inserts, the rest does nothing
            s.event(K.into(), v(1), false, false, || row(1)),
            s.event(K.into(), v(2), true, true, || row(2)),
            s.event(K.into(), v(3), false, true, || row(3)),
            // in the result: a matching version updates, a tie too (the later wins)
            s.event(K.into(), v(4), false, true, || row(4)),
            s.event(K.into(), v(4), false, true, || row(5)),
            // an older one is ignored
            s.event(K.into(), v(2), true, true, || row(6)),
            // no longer matching: deleted; a deleted version likewise
            s.event(K.into(), v(5), false, false, || row(7)),
            s.event(K.into(), v(6), false, true, || row(8)),
            s.event(K.into(), v(7), true, true, || row(9)),
            // gone: a deleted key's later versions come back
            s.event(K.into(), v(1), false, true, || row(10)),
        ]);
        assert_eq!(
            out,
            [
                "+ [\"k\"] {\"v\":3}",
                "~ [\"k\"] {\"v\":4}",
                "~ [\"k\"] {\"v\":5}",
                "- [\"k\"]",
                "+ [\"k\"] {\"v\":8}",
                "- [\"k\"]",
                "+ [\"k\"] {\"v\":10}",
            ]
        );
        assert_eq!(s.stale, 1);
        assert_eq!(s.len(), 1);
    }

    #[test]
    fn without_a_version_column_the_later_event_wins() {
        let mut s = KeyedState::default();
        let out = lines([
            s.event(K.into(), Value::Null, false, true, || row(1)),
            s.event(K.into(), Value::Null, false, true, || row(2)),
            s.event(K.into(), Value::Null, true, true, || row(3)),
        ]);
        assert_eq!(
            out,
            ["+ [\"k\"] {\"v\":1}", "~ [\"k\"] {\"v\":2}", "- [\"k\"]"]
        );
        assert!(s.is_empty());
    }

    #[test]
    fn history_rows_and_checkpoints() {
        use crate::binder::ColumnId;
        use crate::plan::KeyedColumn;
        use crate::types::Type;
        let mut s = KeyedState::default();
        assert_eq!(
            s.history(K.into(), Value::Int(3), row(1)).to_string(),
            "+ [\"k\"] {\"v\":1}"
        );
        let plan = KeyedPlan {
            key: Vec::new(),
            ver: Some(KeyedColumn {
                id: ColumnId(0),
                ty: Type::parse("UInt64"),
                history_field: "__ver".into(),
            }),
            deleted: None,
            text: "k".into(),
        };
        let saved = serde_json::from_str(&s.save().to_string()).unwrap();
        let mut restored = KeyedState::default();
        restored.load(&plan, &saved).unwrap();
        // the version came back: an older event is still ignored
        assert_eq!(
            restored.event(K.into(), Value::Int(2), false, true, || row(2)),
            None
        );
        assert_eq!(restored.stale, 1);
        assert!(restored.load(&plan, &json!({})).is_err());
    }
}
