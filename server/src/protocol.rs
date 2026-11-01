//! The wire protocol (`chlive-protocol`, re-exported here) and its glue to the engine: changes
//! from the engine's diffs, ORDER BY terms from a plan, error codes from engine errors.

pub use chlive_protocol::*;

use chlive_engine::diff::Diff;
use chlive_engine::plan::{OrderPlan, Plan};
use chlive_engine::EngineError;

/// The wire form of an engine diff.
pub fn change(d: Diff) -> Change {
    match d {
        Diff::Insert { row, index, id } => Change::Insert { row, index, id },
        Diff::Delete { index } => Change::Delete {
            index: Some(index),
            id: None,
        },
        Diff::DeleteId { id } => Change::Delete {
            index: None,
            id: Some(id),
        },
        Diff::Update {
            id,
            set,
            index,
            edit,
        } => Change::Update {
            id,
            set,
            index,
            edit: edit
                .into_iter()
                .map(|(column, e)| {
                    let e = ArrayEdit {
                        remove: e.remove.into_iter().map(|(s, c)| [s, c]).collect(),
                        append: e.append,
                    };
                    (column, serde_json::to_value(e).expect("an edit is JSON"))
                })
                .collect(),
        },
    }
}

/// An engine diff from its wire form (the server's change log, as sent).
pub fn diff(c: Change) -> Result<Diff, String> {
    Ok(match c {
        Change::Insert { row, index, id } => Diff::Insert { row, index, id },
        Change::Delete {
            index: Some(index),
            id: None,
        } => Diff::Delete { index },
        Change::Delete {
            index: None,
            id: Some(id),
        } => Diff::DeleteId { id },
        Change::Delete { .. } => return Err("a delete needs exactly one of index and id".into()),
        Change::Update {
            id,
            set,
            index,
            edit,
        } => Diff::Update {
            id,
            set,
            index,
            edit: edit
                .iter()
                .map(|(column, e)| {
                    let e = ArrayEdit::from_json(e)?;
                    Ok((
                        column.clone(),
                        chlive_engine::diff::ArrayEdit {
                            remove: e.remove.into_iter().map(|[s, c]| (s, c)).collect(),
                            append: e.append,
                        },
                    ))
                })
                .collect::<Result<_, String>>()?,
        },
    })
}

fn term(column: &str, desc: bool, nulls_first: bool) -> OrderTerm {
    OrderTerm {
        column: column.to_string(),
        direction: if desc {
            Direction::Desc
        } else {
            Direction::Asc
        },
        nulls: if nulls_first {
            Nulls::First
        } else {
            Nulls::Last
        },
    }
}

/// The query's ORDER BY, plain or aggregated; `None` when unordered.
pub fn order_terms(plan: &Plan) -> Option<Vec<OrderTerm>> {
    match &plan.aggregate {
        Some(a) if a.order.is_empty() => None,
        Some(a) => Some(
            a.order
                .iter()
                .map(|t| term(&t.column, t.desc, t.nulls_first))
                .collect(),
        ),
        None => plan.stream.order.as_ref().map(stream_order_terms),
    }
}

/// A plain query's ORDER BY.
pub fn stream_order_terms(plan: &OrderPlan) -> Vec<OrderTerm> {
    plan.keys
        .0
        .iter()
        .zip(&plan.columns)
        .map(|(k, c)| term(&c.name, k.desc, k.nulls_first))
        .collect()
}

/// The error code for an engine error. `SYNTAX_ERROR` and `INVALID_QUERY` are problems with
/// the query (their message has a line:column); the others are with ClickHouse, Kafka, chDB or
/// the snapshots (the pause server).
pub fn error_code(e: &EngineError) -> &'static str {
    match e {
        EngineError::Syntax(_) => SYNTAX_ERROR,
        EngineError::Bind(_) => INVALID_QUERY,
        EngineError::Denied(_) => PERMISSION_DENIED,
        EngineError::History(_) => HISTORY_FAILED,
        EngineError::Source(_) => STREAM_FAILED,
        EngineError::Aggregate(_) => AGGREGATE_FAILED,
        EngineError::Snapshot(_) => SNAPSHOT_FAILED,
        EngineError::Checkpoint(_) => SNAPSHOT_FAILED,
        EngineError::Insert(_) => INVALID_INSERT,
        EngineError::Write(_) => WRITE_FAILED,
    }
}

/// An engine error, with the message the engine renders (e.g. `… at 1:15`).
pub fn engine_error(id: &str, e: &EngineError) -> ServerMsg {
    let message = match e {
        EngineError::Syntax(m) => format!("syntax error: {m}"),
        other => other.to_string(),
    };
    ServerMsg::error(Some(id), error_code(e), message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn client_messages() {
        let m: ClientMsg = serde_json::from_value(
            json!({"type": "subscribe", "id": "s1", "sql": "select * from events"}),
        )
        .unwrap();
        assert_eq!(
            m,
            ClientMsg::Subscribe {
                id: "s1".into(),
                sql: "select * from events".into(),
                since: None,
                params: Default::default(),
                complete: false,
            }
        );
        let m: ClientMsg = serde_json::from_value(
            json!({"type": "subscribe", "id": "s1", "sql": "x", "since": "{\"v\":1}"}),
        )
        .unwrap();
        assert!(matches!(m, ClientMsg::Subscribe { since: Some(t), .. } if t == "{\"v\":1}"));
        let m: ClientMsg =
            serde_json::from_value(json!({"type": "unsubscribe", "id": "s1"})).unwrap();
        assert_eq!(m, ClientMsg::Unsubscribe { id: "s1".into() });
        // unknown fields are rejected rather than silently ignored
        assert!(serde_json::from_value::<ClientMsg>(
            json!({"type": "subscribe", "id": "s1", "sql": "x", "nope": {}})
        )
        .is_err());
        assert!(serde_json::from_value::<ClientMsg>(json!({"type": "nope"})).is_err());
    }

    #[test]
    fn server_messages() {
        let row = json!({"user_id": "u1", "latency_ms": 10})
            .as_object()
            .unwrap()
            .clone();
        let snapshot = ServerMsg::Snapshot {
            id: "s1".into(),
            columns: vec![Column {
                name: "user_id".into(),
                ty: "String".into(),
            }],
            rows: vec![row.clone()],
            order: None,
            limit: None,
            offset: None,
            at: "c".into(),
            ids: None,
        };
        assert_eq!(
            serde_json::to_value(&snapshot).unwrap(),
            json!({"type": "snapshot", "id": "s1", "columns": [{"name": "user_id", "type": "String"}],
                   "rows": [{"user_id": "u1", "latency_ms": 10}], "at": "c"})
        );
        let diff = ServerMsg::Diff {
            id: "s1".into(),
            changes: vec![
                change(Diff::insert(row.clone())),
                change(Diff::Insert {
                    row: row.clone(),
                    index: Some(2),
                    id: None,
                }),
                change(Diff::Update {
                    id: "[\"PL\"]".into(),
                    set: row.clone(),
                    index: None,
                    edit: Vec::new(),
                }),
                change(Diff::Update {
                    id: "[\"DE\"]".into(),
                    set: Row::new(),
                    index: Some(0),
                    edit: vec![(
                        "users".into(),
                        chlive_engine::diff::ArrayEdit {
                            remove: vec![(0, 2)],
                            append: vec![json!("u7")],
                        },
                    )],
                }),
                change(Diff::Delete { index: 0 }),
                change(Diff::DeleteId {
                    id: "[\"PL\"]".into(),
                }),
            ],
            at: "c2".into(),
        };
        assert_eq!(
            serde_json::to_value(&diff).unwrap(),
            json!({"type": "diff", "id": "s1", "changes": [
                {"op": "insert", "row": {"user_id": "u1", "latency_ms": 10}},
                {"op": "insert", "row": {"user_id": "u1", "latency_ms": 10}, "index": 2},
                {"op": "update", "id": "[\"PL\"]", "set": {"user_id": "u1", "latency_ms": 10}},
                {"op": "update", "id": "[\"DE\"]", "set": {}, "index": 0,
                 "edit": {"users": {"remove": [[0, 2]], "append": ["u7"]}}},
                {"op": "delete", "index": 0},
                {"op": "delete", "id": "[\"PL\"]"}
            ], "at": "c2"})
        );
        // and back, as the change log reads it
        let ServerMsg::Diff { changes, .. } = diff.clone() else {
            unreachable!()
        };
        let back: Vec<String> = changes
            .into_iter()
            .map(|c| super::diff(c).unwrap().to_string())
            .collect();
        assert_eq!(back[3], r#"~ ["DE"] @0 {} users[-0+2 +["u7"]]"#);
        assert_eq!(
            serde_json::to_value(ServerMsg::Progress {
                id: "s1".into(),
                at: "c3".into()
            })
            .unwrap(),
            json!({"type": "progress", "id": "s1", "at": "c3"})
        );
        assert_eq!(
            serde_json::to_value(ServerMsg::Resume {
                id: "s1".into(),
                columns: vec![],
                order: None,
                limit: Some(5)
            })
            .unwrap(),
            json!({"type": "resume", "id": "s1", "columns": [], "limit": 5})
        );
        assert_eq!(
            serde_json::to_value(ServerMsg::error(None, BAD_MESSAGE, "nope")).unwrap(),
            json!({"type": "error", "code": "BAD_MESSAGE", "message": "nope"})
        );
    }
}
