//! The WebSocket protocol (v1): JSON text frames on `/live`. Mirrored by
//! `client/src/protocol.ts`.
//!
//! The client subscribes with an id of its choosing (scoped to the connection) and a dialect
//! query. The server answers with one `snapshot` (the history), then `diff`s as events arrive,
//! or an `error`, after which that subscription is over.
//!
//! With ORDER BY, the snapshot is in order and says so (`order`), and every insert carries the
//! `index` the row takes: the row at that index and all after it move one down. Without ORDER BY
//! there is no index and rows are appended.
//!
//! Aggregated results (GROUP BY or aggregates) have row ids, their group's key: new groups
//! are inserts with an `id`, changed ones `update`s of the changed columns by `id`. Ordered,
//! a group can move: its `update` then has an `index`, where the row is after the move.
//!
//! With LIMIT (`limit` in the snapshot), a row pushed out is a `delete` of its index, right
//! after the insert that pushed it; for aggregates, a group falling out is deleted and the one
//! taking the last place inserted. Each `diff` is atomic: its changes apply in sequence (each
//! index is into the list as the previous change left it), and only the state after the whole
//! diff is meaningful. In between, an insert and its delete can briefly make one row more than
//! the LIMIT, so clients must not expose results mid-diff.
//!
//! Resuming: every `snapshot`, `diff` and `progress` carries `at`, an opaque cursor: how far the
//! result is complete. A client that kept a result (e.g. in an offline cache) subscribes with
//! `since: <at>` and, if the server can resume from it, gets `resume` instead of a snapshot: its
//! rows are still valid, and the diffs that follow bring them up to date. Otherwise it gets a
//! normal `snapshot` that replaces them.

use chlive_engine::diff::{Diff, Row};
use chlive_engine::plan::{OrderPlan, Plan};
use chlive_engine::EngineError;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase", deny_unknown_fields)]
pub enum ClientMsg {
    Subscribe {
        id: String,
        sql: String,
        /// The `at` of a result the client already holds, to resume from it.
        #[serde(default)]
        since: Option<String>,
    },
    Unsubscribe {
        id: String,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum ServerMsg {
    /// The result as of subscribing; always first and exactly once per subscription.
    Snapshot {
        id: String,
        columns: Vec<Column>,
        rows: Vec<Row>,
        /// The ORDER BY the rows are kept in; absent when unordered.
        #[serde(skip_serializing_if = "Option::is_none")]
        order: Option<Vec<OrderTerm>>,
        /// The LIMIT: the result never has more rows (at the end of each diff).
        #[serde(skip_serializing_if = "Option::is_none")]
        limit: Option<u64>,
        /// The cursor of this result.
        at: String,
        /// Aggregated results: each row's id (its group's key), in row order.
        #[serde(skip_serializing_if = "Option::is_none")]
        ids: Option<Vec<String>>,
    },
    /// Instead of a snapshot, when resuming from `since`: the client's rows are still valid;
    /// the diffs that follow bring them up to date.
    Resume {
        id: String,
        columns: Vec<Column>,
        #[serde(skip_serializing_if = "Option::is_none")]
        order: Option<Vec<OrderTerm>>,
        #[serde(skip_serializing_if = "Option::is_none")]
        limit: Option<u64>,
    },
    /// Changes since the previous message, in order. Never empty.
    Diff {
        id: String,
        changes: Vec<Change>,
        /// The cursor after these changes.
        at: String,
    },
    /// The cursor moved without changes to the result (events that didn't match).
    Progress { id: String, at: String },
    Error {
        /// Absent when the message can't be attributed to a subscription.
        #[serde(skip_serializing_if = "Option::is_none")]
        id: Option<String>,
        code: String,
        message: String,
    },
}

/// An output column: its name and ClickHouse type.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Column {
    pub name: String,
    #[serde(rename = "type")]
    pub ty: String,
}

/// One ORDER BY key: `{"column": "ts", "direction": "desc", "nulls": "last"}`.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct OrderTerm {
    pub column: String,
    pub direction: &'static str,
    pub nulls: &'static str,
}

impl OrderTerm {
    /// The query's ORDER BY, plain or aggregated; `None` when unordered.
    pub fn for_plan(plan: &Plan) -> Option<Vec<OrderTerm>> {
        let term = |column: &str, desc: bool, nulls_first: bool| OrderTerm {
            column: column.to_string(),
            direction: if desc { "desc" } else { "asc" },
            nulls: if nulls_first { "first" } else { "last" },
        };
        match &plan.aggregate {
            Some(a) if a.order.is_empty() => None,
            Some(a) => Some(
                a.order
                    .iter()
                    .map(|t| term(&t.column, t.desc, t.nulls_first))
                    .collect(),
            ),
            None => plan.stream.order.as_ref().map(OrderTerm::of),
        }
    }

    pub fn of(plan: &OrderPlan) -> Vec<OrderTerm> {
        plan.keys
            .0
            .iter()
            .zip(&plan.columns)
            .map(|(k, c)| OrderTerm {
                column: c.name.clone(),
                direction: if k.desc { "desc" } else { "asc" },
                nulls: if k.nulls_first { "first" } else { "last" },
            })
            .collect()
    }
}

/// One change to the result; mirrors the engine's [`Diff`]. Clients must reject ops they don't
/// know.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "op", rename_all = "lowercase")]
pub enum Change {
    /// At `index` in an ordered result, appended otherwise. Rows of aggregated results have an
    /// `id` (their group's key, e.g. `["PL"]`) that updates refer to.
    Insert {
        row: Row,
        #[serde(skip_serializing_if = "Option::is_none")]
        index: Option<usize>,
        #[serde(skip_serializing_if = "Option::is_none")]
        id: Option<String>,
    },
    /// The row at `index` leaves the result (pushed out by LIMIT); the rows after it move up.
    Delete { index: usize },
    /// The row with this `id` has these new values (only the columns that changed), and, with
    /// an `index`, moves: it's taken out and put back so that it's at `index`.
    Update {
        id: String,
        set: Row,
        #[serde(skip_serializing_if = "Option::is_none")]
        index: Option<usize>,
    },
}

impl From<Diff> for Change {
    fn from(d: Diff) -> Self {
        match d {
            Diff::Insert { row, index, id } => Change::Insert { row, index, id },
            Diff::Delete { index } => Change::Delete { index },
            Diff::Update { id, set, index } => Change::Update { id, set, index },
        }
    }
}

pub const BAD_MESSAGE: &str = "BAD_MESSAGE";
pub const DUPLICATE_ID: &str = "DUPLICATE_ID";

/// The error code for an engine error. `SYNTAX_ERROR` and `INVALID_QUERY` are problems with
/// the query (their message has a line:column); the others are with ClickHouse, Kafka, chDB or
/// the snapshots (the pause server).
pub fn error_code(e: &EngineError) -> &'static str {
    match e {
        EngineError::Syntax(_) => "SYNTAX_ERROR",
        EngineError::Bind(_) => "INVALID_QUERY",
        EngineError::History(_) => "HISTORY_FAILED",
        EngineError::Source(_) => "STREAM_FAILED",
        EngineError::Aggregate(_) => "AGGREGATE_FAILED",
        EngineError::Snapshot(_) => "SNAPSHOT_FAILED",
    }
}

impl ServerMsg {
    pub fn error(id: Option<&str>, code: &str, message: impl Into<String>) -> Self {
        ServerMsg::Error {
            id: id.map(str::to_string),
            code: code.to_string(),
            message: message.into(),
        }
    }

    /// An engine error, with the message the engine renders (e.g. `… at 1:15`).
    pub fn engine_error(id: &str, e: &EngineError) -> Self {
        let message = match e {
            EngineError::Syntax(m) => format!("syntax error: {m}"),
            other => other.to_string(),
        };
        Self::error(Some(id), error_code(e), message)
    }
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
        // v0 fields (params) are rejected rather than silently ignored
        assert!(serde_json::from_value::<ClientMsg>(
            json!({"type": "subscribe", "id": "s1", "sql": "x", "params": {}})
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
                Diff::insert(row.clone()).into(),
                Diff::Insert {
                    row: row.clone(),
                    index: Some(2),
                    id: None,
                }
                .into(),
                Diff::Update {
                    id: "[\"PL\"]".into(),
                    set: row.clone(),
                    index: None,
                }
                .into(),
                Diff::Update {
                    id: "[\"DE\"]".into(),
                    set: Row::new(),
                    index: Some(0),
                }
                .into(),
                Diff::Delete { index: 0 }.into(),
            ],
            at: "c2".into(),
        };
        assert_eq!(
            serde_json::to_value(&diff).unwrap(),
            json!({"type": "diff", "id": "s1", "changes": [
                {"op": "insert", "row": {"user_id": "u1", "latency_ms": 10}},
                {"op": "insert", "row": {"user_id": "u1", "latency_ms": 10}, "index": 2},
                {"op": "update", "id": "[\"PL\"]", "set": {"user_id": "u1", "latency_ms": 10}},
                {"op": "update", "id": "[\"DE\"]", "set": {}, "index": 0},
                {"op": "delete", "index": 0}
            ], "at": "c2"})
        );
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
