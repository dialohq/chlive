//! The WebSocket protocol (v1): JSON text frames on `/live`. Mirrored by
//! `client/src/protocol.ts`.
//!
//! The client subscribes with an id of its choosing (scoped to the connection) and a dialect
//! query. The server answers with one `snapshot` (the history), then `diff`s as events arrive,
//! or an `error`, after which that subscription is over.

use chlive_engine::diff::{Diff, Row};
use chlive_engine::EngineError;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase", deny_unknown_fields)]
pub enum ClientMsg {
    Subscribe { id: String, sql: String },
    Unsubscribe { id: String },
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum ServerMsg {
    /// The result as of subscribing; always first and exactly once per subscription.
    Snapshot {
        id: String,
        columns: Vec<Column>,
        rows: Vec<Row>,
    },
    /// Changes since the previous message, in order. Never empty.
    Diff { id: String, changes: Vec<Change> },
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

/// One change to the result; mirrors the engine's [`Diff`]. Only inserts exist so far;
/// clients must reject ops they don't know.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(tag = "op", rename_all = "lowercase")]
pub enum Change {
    Insert { row: Row },
}

impl From<Diff> for Change {
    fn from(d: Diff) -> Self {
        match d {
            Diff::Insert(row) => Change::Insert { row },
        }
    }
}

pub const BAD_MESSAGE: &str = "BAD_MESSAGE";
pub const DUPLICATE_ID: &str = "DUPLICATE_ID";

/// The error code for an engine error. `SYNTAX_ERROR` and `INVALID_QUERY` are problems with
/// the query (their message has a line:column); the others are with ClickHouse or Kafka.
pub fn error_code(e: &EngineError) -> &'static str {
    match e {
        EngineError::Syntax(_) => "SYNTAX_ERROR",
        EngineError::Bind(_) => "INVALID_QUERY",
        EngineError::History(_) => "HISTORY_FAILED",
        EngineError::Source(_) => "STREAM_FAILED",
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
                sql: "select * from events".into()
            }
        );
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
        };
        assert_eq!(
            serde_json::to_value(&snapshot).unwrap(),
            json!({"type": "snapshot", "id": "s1", "columns": [{"name": "user_id", "type": "String"}],
                   "rows": [{"user_id": "u1", "latency_ms": 10}]})
        );
        let diff = ServerMsg::Diff {
            id: "s1".into(),
            changes: vec![Diff::Insert(row).into()],
        };
        assert_eq!(
            serde_json::to_value(&diff).unwrap(),
            json!({"type": "diff", "id": "s1", "changes": [{"op": "insert", "row": {"user_id": "u1", "latency_ms": 10}}]})
        );
        assert_eq!(
            serde_json::to_value(ServerMsg::error(None, BAD_MESSAGE, "nope")).unwrap(),
            json!({"type": "error", "code": "BAD_MESSAGE", "message": "nope"})
        );
    }
}
