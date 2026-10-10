//! The WebSocket protocol (v1): JSON text frames on `/live`. Mirrored by
//! `client/src/protocol.ts`;
//! used by `chlive-server` and the Rust client, `chlive-client`.
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

use serde::{Deserialize, Serialize};

/// A result row: exactly the selected columns, rendered the way ClickHouse renders JSON, in
/// column order.
pub type Row = serde_json::Map<String, serde_json::Value>;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase", deny_unknown_fields)]
pub enum ClientMsg {
    Subscribe {
        id: String,
        sql: String,
        /// The `at` of a result the client already holds, to resume from it.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        since: Option<String>,
    },
    Unsubscribe {
        id: String,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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
        /// The OFFSET: the rows (or groups) the result starts after, which the client never
        /// sees. Informational; the diffs already account for it.
        #[serde(skip_serializing_if = "Option::is_none")]
        offset: Option<u64>,
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
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Column {
    pub name: String,
    #[serde(rename = "type")]
    pub ty: String,
}

/// One ORDER BY key: `{"column": "ts", "direction": "desc", "nulls": "last"}`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Direction {
    Asc,
    Desc,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Nulls {
    First,
    Last,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderTerm {
    pub column: String,
    pub direction: Direction,
    pub nulls: Nulls,
}

/// One change to the result; mirrors the engine's `Diff`. Clients must reject ops they don't
/// know.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
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

/// Protocol misuse: a message the server couldn't read (no id).
pub const BAD_MESSAGE: &str = "BAD_MESSAGE";
/// A `subscribe` with an id already in use on the connection.
pub const DUPLICATE_ID: &str = "DUPLICATE_ID";
/// The query's syntax (the message ends with `at line:column`).
pub const SYNTAX_ERROR: &str = "SYNTAX_ERROR";
/// The query doesn't fit the tables (the message ends with `at line:column`).
pub const INVALID_QUERY: &str = "INVALID_QUERY";
/// ClickHouse.
pub const HISTORY_FAILED: &str = "HISTORY_FAILED";
/// Kafka.
pub const STREAM_FAILED: &str = "STREAM_FAILED";
/// chDB.
pub const AGGREGATE_FAILED: &str = "AGGREGATE_FAILED";
/// The snapshots (the pause server, or cloning).
pub const SNAPSHOT_FAILED: &str = "SNAPSHOT_FAILED";
/// Never sent: a client's code for a message it couldn't apply.
pub const CLIENT_ERROR: &str = "CLIENT_ERROR";

impl ServerMsg {
    pub fn error(id: Option<&str>, code: &str, message: impl Into<String>) -> Self {
        ServerMsg::Error {
            id: id.map(str::to_string),
            code: code.to_string(),
            message: message.into(),
        }
    }

    /// The subscription this message is for (`None` for an error about the connection).
    pub fn id(&self) -> Option<&str> {
        match self {
            ServerMsg::Snapshot { id, .. }
            | ServerMsg::Resume { id, .. }
            | ServerMsg::Diff { id, .. }
            | ServerMsg::Progress { id, .. } => Some(id),
            ServerMsg::Error { id, .. } => id.as_deref(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::{json, Value};

    /// Read as the other side would, and written back exactly as it was.
    fn round_trip<T: Serialize + for<'de> Deserialize<'de>>(v: Value) -> T {
        let parsed: T = serde_json::from_value(v.clone()).unwrap();
        assert_eq!(serde_json::to_value(&parsed).unwrap(), v);
        parsed
    }

    #[test]
    fn client_messages() {
        let m: ClientMsg =
            round_trip(json!({"type": "subscribe", "id": "s1", "sql": "select * from events"}));
        assert!(matches!(m, ClientMsg::Subscribe { since: None, .. }));
        let m: ClientMsg =
            round_trip(json!({"type": "subscribe", "id": "s1", "sql": "x", "since": "c"}));
        assert!(matches!(m, ClientMsg::Subscribe { since: Some(c), .. } if c == "c"));
        round_trip::<ClientMsg>(json!({"type": "unsubscribe", "id": "s1"}));
        assert!(serde_json::from_value::<ClientMsg>(
            json!({"type": "subscribe", "id": "s1", "sql": "x", "params": {}})
        )
        .is_err());
    }

    #[test]
    fn server_messages() {
        let m: ServerMsg = round_trip(json!({
            "type": "snapshot", "id": "s1",
            "columns": [{"name": "country", "type": "LowCardinality(String)"}, {"name": "n", "type": "UInt64"}],
            "rows": [{"country": "PL", "n": 3}],
            "order": [{"column": "n", "direction": "desc", "nulls": "last"}],
            "limit": 5, "offset": 2, "at": "c1", "ids": ["[\"PL\"]"]
        }));
        let ServerMsg::Snapshot { order, rows, .. } = &m else {
            panic!("{m:?}")
        };
        assert_eq!(
            order.as_deref(),
            Some(
                &[OrderTerm {
                    column: "n".into(),
                    direction: Direction::Desc,
                    nulls: Nulls::Last
                }][..]
            )
        );
        // columns keep their order
        assert_eq!(rows[0].keys().collect::<Vec<_>>(), ["country", "n"]);
        assert_eq!(m.id(), Some("s1"));
        round_trip::<ServerMsg>(
            json!({"type": "snapshot", "id": "s1", "columns": [], "rows": [], "at": "c"}),
        );
        round_trip::<ServerMsg>(json!({"type": "resume", "id": "s1", "columns": [], "limit": 5}));
        let m: ServerMsg = round_trip(json!({"type": "diff", "id": "s1", "changes": [
            {"op": "insert", "row": {"a": 1}},
            {"op": "insert", "row": {"a": 2}, "index": 0, "id": "[\"x\"]"},
            {"op": "update", "id": "[\"x\"]", "set": {"a": 3}},
            {"op": "update", "id": "[\"x\"]", "set": {}, "index": 1},
            {"op": "delete", "index": 0}
        ], "at": "c2"}));
        assert!(matches!(&m, ServerMsg::Diff { changes, .. } if changes.len() == 5));
        round_trip::<ServerMsg>(json!({"type": "progress", "id": "s1", "at": "c3"}));
        let m: ServerMsg =
            round_trip(json!({"type": "error", "code": "BAD_MESSAGE", "message": "nope"}));
        assert_eq!(m.id(), None);
        round_trip::<ServerMsg>(
            json!({"type": "error", "id": "s1", "code": SYNTAX_ERROR, "message": "x at 1:2"}),
        );
    }

    #[test]
    fn unknown_ops_and_types_are_rejected() {
        assert!(
            serde_json::from_value::<ServerMsg>(json!({"type": "diff", "id": "s1",
            "changes": [{"op": "upsert", "row": {}}], "at": "c"}))
            .is_err()
        );
        assert!(serde_json::from_value::<ServerMsg>(json!({"type": "nope", "id": "s1"})).is_err());
    }
}
