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
use crate::order::{KeyOrder, Ranking, SortKey};
use crate::plan::KeyedPlan;
use crate::types::{decode, Value};

#[derive(Debug, Clone, Default)]
pub struct KeyedState {
    /// The result's rows by id: their versions (`Null` without a version column).
    versions: HashMap<String, Value>,
    /// With ORDER BY: the result's rows in order.
    ranking: Option<Ranking>,
    /// Events older than their key's row, ignored.
    stale: u64,
}

impl KeyedState {
    /// With ORDER BY, rows are kept in its order: each insert and move has an index.
    pub fn new(order: Option<KeyOrder>) -> Self {
        Self {
            ranking: order.map(Ranking::new),
            ..Self::default()
        }
    }

    /// The history: one row per key, its latest version (FINAL left out deleted ones), as
    /// (id, version, sort key, row), in ClickHouse's order. The inserts come in result order.
    pub fn history(&mut self, rows: Vec<(String, Value, SortKey, Row)>) -> Vec<Diff> {
        let insert = |id: String, row: Row| Diff::Insert {
            row,
            index: None,
            id: Some(id),
        };
        let Some(ranking) = &mut self.ranking else {
            return rows
                .into_iter()
                .map(|(id, ver, _, row)| {
                    self.versions.insert(id.clone(), ver);
                    insert(id, row)
                })
                .collect();
        };
        let mut by_id: HashMap<String, Row> = HashMap::with_capacity(rows.len());
        let mut keys = Vec::with_capacity(rows.len());
        for (id, ver, key, row) in rows {
            self.versions.insert(id.clone(), ver);
            by_id.insert(id.clone(), row);
            keys.push((id, key));
        }
        ranking.history(keys);
        ranking
            .iter()
            .map(|e| insert(e.id.clone(), by_id.remove(&e.id).expect("a history row")))
            .collect()
    }

    /// A new version of the key `id`: its sort key (empty without ORDER BY), whether it's
    /// deleted, and whether it matches the WHERE; `row` renders it.
    pub fn event(
        &mut self,
        id: String,
        ver: Value,
        key: SortKey,
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
                // a new sort key moves the row: out, then after its new ties
                let index = self.ranking.as_mut().and_then(|r| {
                    let same = r
                        .key(&id)
                        .is_some_and(|k| r.order().cmp(k, &key) == Ordering::Equal);
                    if same {
                        return None;
                    }
                    let from = r.remove(&id);
                    let to = r.place(&id, key);
                    (from != Some(to)).then_some(to)
                });
                Some(Diff::Update {
                    id,
                    set: row(),
                    index,
                })
            }
            Some(_) => {
                self.versions.remove(&id);
                if let Some(r) = &mut self.ranking {
                    r.remove(&id);
                }
                Some(Diff::DeleteId { id })
            }
            None if shown => {
                self.versions.insert(id.clone(), ver);
                let index = self.ranking.as_mut().map(|r| r.place(&id, key));
                Some(Diff::Insert {
                    row: row(),
                    index,
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

    /// For a checkpoint: `{"versions": {id: version}}`, with ORDER BY and `"ranking"`.
    pub fn save(&self) -> Json {
        let versions: serde_json::Map<String, Json> = self
            .versions
            .iter()
            .map(|(id, v)| (id.clone(), v.to_json()))
            .collect();
        let mut json = json!({ "versions": versions });
        if let Some(r) = &self.ranking {
            json["ranking"] = r.save();
        }
        json
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
        if let Some(r) = &mut self.ranking {
            r.load(&json["ranking"])?;
        }
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
            s.event(K.into(), v(1), vec![], false, false, || row(1)),
            s.event(K.into(), v(2), vec![], true, true, || row(2)),
            s.event(K.into(), v(3), vec![], false, true, || row(3)),
            // in the result: a matching version updates, a tie too (the later wins)
            s.event(K.into(), v(4), vec![], false, true, || row(4)),
            s.event(K.into(), v(4), vec![], false, true, || row(5)),
            // an older one is ignored
            s.event(K.into(), v(2), vec![], true, true, || row(6)),
            // no longer matching: deleted; a deleted version likewise
            s.event(K.into(), v(5), vec![], false, false, || row(7)),
            s.event(K.into(), v(6), vec![], false, true, || row(8)),
            s.event(K.into(), v(7), vec![], true, true, || row(9)),
            // gone: a deleted key's later versions come back
            s.event(K.into(), v(1), vec![], false, true, || row(10)),
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
            s.event(K.into(), Value::Null, vec![], false, true, || row(1)),
            s.event(K.into(), Value::Null, vec![], false, true, || row(2)),
            s.event(K.into(), Value::Null, vec![], true, true, || row(3)),
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
            s.history(vec![(K.into(), Value::Int(3), vec![], row(1))])[0].to_string(),
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
            restored.event(K.into(), Value::Int(2), vec![], false, true, || row(2)),
            None
        );
        assert_eq!(restored.stale, 1);
        assert!(restored.load(&plan, &json!({})).is_err());
    }

    #[test]
    fn ordered_rows_are_inserted_moved_and_deleted_at_their_places() {
        use crate::binder::{ColumnId, OrderKey};
        let order = KeyOrder(vec![OrderKey {
            column: ColumnId(0),
            desc: false,
            nulls_first: false,
        }]);
        let mut s = KeyedState::new(Some(order.clone()));
        let k = |n: i128| vec![Value::Int(n)];
        let id = |x: &str| format!("[\"{x}\"]");
        // history in ClickHouse's order (ties as it chose): inserts in result order
        let history: Vec<String> = s
            .history(vec![
                (id("a"), Value::Int(1), k(2), row(2)),
                (id("b"), Value::Int(1), k(1), row(1)),
                (id("c"), Value::Int(1), k(2), row(2)),
            ])
            .iter()
            .map(|d| d.to_string())
            .collect();
        assert_eq!(
            history,
            [
                "+ [\"b\"] {\"v\":1}",
                "+ [\"a\"] {\"v\":2}",
                "+ [\"c\"] {\"v\":2}"
            ]
        );
        let mut e = |x: &str, ver: i128, key: i128, deleted: bool| {
            s.event(id(x), Value::Int(ver), k(key), deleted, true, || {
                row(key as i64)
            })
        };
        let out = lines([
            // new, tying with a and c: after them
            e("d", 1, 2, false),
            // a new version with the same sort key: no move
            e("a", 2, 2, false),
            // a new sort key: b moves after the 2s
            e("b", 2, 2, false),
            // and to the top
            e("c", 2, 0, false),
            // deleted
            e("a", 3, 2, true),
        ]);
        assert_eq!(
            out,
            [
                "+ @3 [\"d\"] {\"v\":2}",
                "~ [\"a\"] {\"v\":2}",
                "~ [\"b\"] @3 {\"v\":2}",
                "~ [\"c\"] @0 {\"v\":0}",
                "- [\"a\"]",
            ]
        );
        // a checkpoint keeps the order within ties: c, d, b
        let mut back = KeyedState::new(Some(order));
        let plan = KeyedPlan {
            key: Vec::new(),
            ver: None,
            deleted: None,
            text: "k".into(),
        };
        back.load(&plan, &serde_json::from_str(&s.save().to_string()).unwrap())
            .unwrap();
        let ids = |s: &KeyedState| -> Vec<String> {
            s.ranking
                .as_ref()
                .unwrap()
                .iter()
                .map(|e| e.id.clone())
                .collect()
        };
        assert_eq!(ids(&back), [id("c"), id("d"), id("b")]);
        assert_eq!(ids(&back), ids(&s));
    }
}
