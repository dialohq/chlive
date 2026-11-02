//! The WebSocket protocol (v1): JSON text frames on `/live`. Mirrored by
//! `client/src/protocol.ts`;
//! used by `chlive-server` and the Rust client, `chlive-client`.
//!
//! The client may first say who it is (`hello`); a server that secures its tables shows each
//! user only the tables, rows and columns they may see (`*` is the columns they may see). It
//! subscribes with an id of its choosing (scoped to the connection) and a dialect query.
//!
//! Authentication: a server that verifies credentials (`[auth] mode = "jwt"`) takes the
//! `hello`'s `token` (a JWT its issuer signed), and the connection is the token's subject's.
//! Nothing comes before the `hello`, which the server answers with `authenticated` (the subject,
//! and when the credential expires). Before then, the client sends `renew` with a fresh token for
//! the same subject, answered with `authenticated` again: the connection, its subscriptions and
//! their cursors carry on, with no new snapshot. A refused `renew` is an `error` (with no id) and
//! the connection carries on until the credential expires. A connection without a valid
//! credential gets an `error` with no id (`AUTH_MISSING`, `AUTH_BAD_SIGNATURE`,
//! `AUTH_WRONG_AUDIENCE` or `AUTH_EXPIRED`, nothing more said), and is closed with code
//! [`AUTH_CLOSE`] (4401) and the code as the reason. A server in dev mode (`mode = "declared"`)
//! trusts the `hello`'s `user` instead, and one without authentication refuses any `hello`. The server answers with a `snapshot` (the history), then
//! `diff`s as events arrive, or an `error`, after which that subscription is over. When what
//! the user may see changes (their permissions), the subscription gets a new `snapshot` that
//! replaces its result, and the diffs continue from it; when they lose a table it reads, or a
//! column it names, it gets `PERMISSION_DENIED` instead, and is over.
//!
//! With ORDER BY, the snapshot is in order and says so (`order`), and every insert carries the
//! `index` the row takes: the row at that index and all after it move one down. Without ORDER BY
//! there is no index and rows are appended, except in LIMIT and OFFSET windows, which are
//! newest first: a new row comes in at index 0.
//!
//! Aggregated results (GROUP BY or aggregates) have row ids, their group's key: new groups
//! are inserts with an `id`, changed ones `update`s of the changed columns by `id`. Ordered,
//! a group can move: its `update` then has an `index`, where the row is after the move. An
//! array column (`groupArray`) changes in place, by an `edit` in the `update`: elements removed
//! by position ranges, then values appended at the end (see [`ArrayEdit`]).
//!
//! FINAL results (on a ReplacingMergeTree) have row ids too, the row's key: a new key is an
//! insert with an `id`, a new version an `update` by `id` (of the whole row), and a deleted
//! key, or one whose new version no longer matches the WHERE, a `delete` by `id`.
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
//! normal `snapshot` that replaces them, never older than the cursor: when the server's result
//! is behind it (e.g. restored from an older checkpoint after a restart), the snapshot waits
//! for the result to catch up (at most 10 seconds).
//!
//! A `subscribe` with `complete: true` asks for a snapshot holding every event in the query's
//! topics up to their latest offsets when the server got the `subscribe`: the snapshot waits
//! for the result to get there (from ClickHouse's snapshot, which trails Kafka, or a shared
//! result behind on a burst), and if it can't within the server's `complete_wait`, the
//! subscription ends with `CATCH_UP_TIMEOUT` instead. Without it, the snapshot is whatever the
//! result has, and what's in Kafka beyond it comes as diffs. A `resume` isn't affected.
//!
//! Writing: an `insert` of rows into a table (each a JSON object by column name, as queries
//! name them), with an id of the client's choosing (its own, apart from subscriptions'). The
//! server checks the user's write permissions and the rows, produces them to the table's topic,
//! and answers with an `ack` holding each row's Kafka partition and offset, in order; or an
//! `error`: `PERMISSION_DENIED` (the table takes no inserts, or not these from this user),
//! `INVALID_INSERT` (a row doesn't fit the table; nothing was produced) or `WRITE_FAILED`
//! (Kafka; the message says which rows went in). Like queries, an insert reaches subscribers as
//! a new event; an update or delete is an insert by the table's own convention.
//!
//! Optimistic results: a `subscribe` with `optimistic: true` gets a `plan` right before each
//! `snapshot` or `resume`: how the server places rows in that result (its WHERE with the user's
//! row filters, ORDER BY, LIMIT, OFFSET, FINAL, `now()` windows), so that the client can show
//! its own inserts where the server's diffs will put them, before they come. Queries a client
//! can't predict (aggregates, semi-joins, ordered or identified by columns the result doesn't
//! have) get a `plan` with a `reason` instead.

use serde::{Deserialize, Serialize};

/// A result row: exactly the selected columns, rendered the way ClickHouse renders JSON, in
/// column order.
pub type Row = serde_json::Map<String, serde_json::Value>;

/// Query parameter values by name, for the query's `{name:Type}` placeholders; each is read by
/// its declared type (`"42"` or `42` for a UInt32, a UUID in either case, …).
pub type Params = std::collections::BTreeMap<String, serde_json::Value>;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase", deny_unknown_fields)]
pub enum ClientMsg {
    /// Who the client is: first, at most once per connection. Without one, the connection is
    /// anonymous (on a server that allows it). A server verifying credentials takes a `token`
    /// and answers `authenticated`; one that trusts declared users (dev, tests) takes `user`.
    Hello {
        #[serde(default, skip_serializing_if = "Option::is_none")]
        user: Option<String>,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        token: Option<String>,
    },
    /// A fresh credential for the connection's subject, before the current one expires: the
    /// connection, its subscriptions and their cursors carry on (see the module docs).
    Renew {
        token: String,
    },
    Subscribe {
        id: String,
        sql: String,
        /// The `at` of a result the client already holds, to resume from it.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        since: Option<String>,
        /// Values for the query's parameters (`{name:Type}`).
        #[serde(default, skip_serializing_if = "Params::is_empty")]
        params: Params,
        /// The snapshot holds everything in Kafka when subscribing (see the module docs).
        #[serde(default, skip_serializing_if = "std::ops::Not::not")]
        complete: bool,
        /// A `plan` comes before each snapshot, to predict the client's own rows with (see the
        /// module docs).
        #[serde(default, skip_serializing_if = "std::ops::Not::not")]
        optimistic: bool,
    },
    Unsubscribe {
        id: String,
    },
    /// Rows to insert into `table` (see the module docs).
    Insert {
        id: String,
        table: String,
        rows: Vec<Row>,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum ServerMsg {
    /// The result as of subscribing; always first. Another one later replaces the result (the
    /// user's permissions changed).
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
    /// An `optimistic` subscription's plan, before each of its snapshots (and resumes): how the
    /// server places rows in this result, for the client to place its own pending rows the
    /// same way (chlive-core's `ClientPlan`). Without one, `reason` says why the query isn't
    /// predicted.
    Plan {
        id: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        plan: Option<serde_json::Value>,
        #[serde(skip_serializing_if = "Option::is_none")]
        reason: Option<String>,
    },
    /// The cursor moved without changes to the result (events that didn't match).
    Progress { id: String, at: String },
    /// An `insert` went in: each row's Kafka `[partition, offset]`, in order.
    Ack {
        id: String,
        offsets: Vec<(i32, i64)>,
    },
    /// The `hello`'s (or a `renew`'s) credential is verified: the connection is `subject`'s
    /// until `expires_at` (Unix milliseconds), when it's closed unless renewed.
    Authenticated { subject: String, expires_at: i64 },
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
    /// A row leaves the result: the one at `index` (pushed out by LIMIT), or the one with this
    /// `id` (FINAL: deleted, or no longer matching); exactly one of the two is set. The rows
    /// after it move up.
    Delete {
        #[serde(skip_serializing_if = "Option::is_none")]
        index: Option<usize>,
        #[serde(skip_serializing_if = "Option::is_none")]
        id: Option<String>,
    },
    /// The row with this `id` has these new values (the columns that changed, or all of them),
    /// and, with an `index`, moves: it's taken out and put back so that it's at `index`.
    ///
    /// `edit` changes array columns in place, by column; a column is in `set` or in `edit`, never
    /// both.
    Update {
        id: String,
        set: Row,
        #[serde(skip_serializing_if = "Option::is_none")]
        index: Option<usize>,
        #[serde(default, skip_serializing_if = "Edits::is_empty")]
        edit: Edits,
    },
}

/// Array columns' edits, by column name, in column order.
pub type Edits = serde_json::Map<String, serde_json::Value>;

/// One array column's edit: the elements at the `remove` ranges (`[start, count]` positions in
/// the array before the edit, ascending and apart) go, then `append` goes at the end.
#[derive(Debug, Clone, PartialEq, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArrayEdit {
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub remove: Vec<[usize; 2]>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub append: Vec<serde_json::Value>,
}

impl ArrayEdit {
    /// Read one from an `edit`'s value.
    pub fn from_json(json: &serde_json::Value) -> Result<Self, String> {
        serde_json::from_value(json.clone()).map_err(|e| format!("a bad array edit: {e}"))
    }

    /// Apply to an array value; fails on a range outside it, or on a value that isn't one.
    pub fn apply(&self, array: &mut serde_json::Value) -> Result<(), String> {
        let serde_json::Value::Array(items) = array else {
            return Err(format!("an array edit of a non-array value {array}"));
        };
        let mut end = 0;
        for &[start, count] in &self.remove {
            if start < end || start + count > items.len() {
                return Err(format!(
                    "removing [{start}, {count}] of an array of {} elements",
                    items.len()
                ));
            }
            end = start + count;
        }
        for &[start, count] in self.remove.iter().rev() {
            items.drain(start..start + count);
        }
        items.extend(self.append.iter().cloned());
        Ok(())
    }
}

/// Protocol misuse: a message the server couldn't read (no id).
pub const BAD_MESSAGE: &str = "BAD_MESSAGE";
/// A `subscribe` with an id already in use on the connection.
pub const DUPLICATE_ID: &str = "DUPLICATE_ID";
/// A `subscribe` beyond the server's `max_subscriptions_per_connection`.
pub const TOO_MANY_SUBSCRIPTIONS: &str = "TOO_MANY_SUBSCRIPTIONS";
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
/// A table the query reads, or a column it names, isn't one this connection's user may read
/// (anymore: a live subscription that loses it ends with this); or a misplaced `hello`.
pub const PERMISSION_DENIED: &str = "PERMISSION_DENIED";
/// An `insert` with rows the table can't take (or too many); nothing was produced.
pub const INVALID_INSERT: &str = "INVALID_INSERT";
/// Producing an `insert`'s rows to Kafka failed.
pub const WRITE_FAILED: &str = "WRITE_FAILED";
/// A `complete` subscription's result didn't get to Kafka's offsets at subscribe time within
/// the server's `complete_wait`.
pub const CATCH_UP_TIMEOUT: &str = "CATCH_UP_TIMEOUT";
/// No credential: none in the `hello`, a declared `user` instead, a message before the `hello`,
/// or no `hello` within the server's `hello_timeout`.
pub const AUTH_MISSING: &str = "AUTH_MISSING";
/// The credential can't be verified: malformed, signed by a key the server doesn't trust, or
/// with an algorithm it doesn't take.
pub const AUTH_BAD_SIGNATURE: &str = "AUTH_BAD_SIGNATURE";
/// The credential's issuer or audience isn't the server's.
pub const AUTH_WRONG_AUDIENCE: &str = "AUTH_WRONG_AUDIENCE";
/// The credential expired (or isn't valid yet).
pub const AUTH_EXPIRED: &str = "AUTH_EXPIRED";
/// The WebSocket close code of a connection closed for its credential (or its `hello`); the
/// close reason is the error's code, e.g. `AUTH_EXPIRED`.
pub const AUTH_CLOSE: u16 = 4401;

/// Whether `code` is one of the credential's (`AUTH_*`).
pub fn is_auth_error(code: &str) -> bool {
    [
        AUTH_MISSING,
        AUTH_BAD_SIGNATURE,
        AUTH_WRONG_AUDIENCE,
        AUTH_EXPIRED,
    ]
    .contains(&code)
}

/// Never sent: a client's code for a message it couldn't apply.
pub const CLIENT_ERROR: &str = "CLIENT_ERROR";
/// Never sent: a client's code for an insert whose connection dropped before its answer (it may
/// or may not have gone in).
pub const DISCONNECTED: &str = "DISCONNECTED";

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
            | ServerMsg::Progress { id, .. }
            | ServerMsg::Plan { id, .. }
            | ServerMsg::Ack { id, .. } => Some(id),
            ServerMsg::Error { id, .. } => id.as_deref(),
            ServerMsg::Authenticated { .. } => None,
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
        let m: ClientMsg = round_trip(json!({
            "type": "insert", "id": "w1", "table": "messages", "rows": [{"body": "hi"}, {"body": "yo"}]
        }));
        assert!(matches!(m, ClientMsg::Insert { rows, .. } if rows.len() == 2));
        let m: ClientMsg = round_trip(json!({
            "type": "subscribe", "id": "s1", "sql": "x", "params": {"id": "u1", "n": 5}
        }));
        assert!(matches!(m, ClientMsg::Subscribe { params, .. } if params["n"] == 5));
        let m: ClientMsg =
            round_trip(json!({"type": "subscribe", "id": "s1", "sql": "x", "complete": true}));
        assert!(matches!(m, ClientMsg::Subscribe { complete: true, .. }));
        let m: ClientMsg =
            round_trip(json!({"type": "subscribe", "id": "s1", "sql": "x", "optimistic": true}));
        assert!(matches!(
            m,
            ClientMsg::Subscribe {
                optimistic: true,
                ..
            }
        ));
        let m: ClientMsg = round_trip(json!({"type": "hello", "token": "eyJ…"}));
        assert!(matches!(
            m,
            ClientMsg::Hello {
                user: None,
                token: Some(_)
            }
        ));
        let m: ClientMsg = round_trip(json!({"type": "hello", "user": "alice"}));
        assert!(matches!(
            m,
            ClientMsg::Hello {
                user: Some(_),
                token: None
            }
        ));
        round_trip::<ClientMsg>(json!({"type": "renew", "token": "eyJ…"}));
        // no params, not complete: neither sent
        let m: ClientMsg = serde_json::from_value(
            json!({"type": "subscribe", "id": "s1", "sql": "x", "params": {}, "complete": false}),
        )
        .unwrap();
        assert_eq!(
            serde_json::to_value(&m).unwrap(),
            json!({"type": "subscribe", "id": "s1", "sql": "x"})
        );
        assert!(serde_json::from_value::<ClientMsg>(
            json!({"type": "subscribe", "id": "s1", "sql": "x", "nope": 1})
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
            {"op": "update", "id": "[\"x\"]", "set": {}, "edit": {"a": {"remove": [[0, 2]], "append": [7]}}},
            {"op": "delete", "index": 0},
            {"op": "delete", "id": "[\"x\"]"}
        ], "at": "c2"}));
        assert!(matches!(&m, ServerMsg::Diff { changes, .. } if changes.len() == 7));
        round_trip::<ServerMsg>(json!({"type": "progress", "id": "s1", "at": "c3"}));
        let m: ServerMsg = round_trip(
            json!({"type": "plan", "id": "s1", "plan": {"v": 1, "stream": {"filter": "all"}}}),
        );
        assert_eq!(m.id(), Some("s1"));
        round_trip::<ServerMsg>(
            json!({"type": "plan", "id": "s1", "reason": "GROUP BY and aggregates aren't predicted yet"}),
        );
        let m: ServerMsg =
            round_trip(json!({"type": "ack", "id": "w1", "offsets": [[0, 41], [2, 7]]}));
        assert!(matches!(&m, ServerMsg::Ack { offsets, .. } if offsets == &[(0, 41), (2, 7)]));
        assert_eq!(m.id(), Some("w1"));
        let m: ServerMsg =
            round_trip(json!({"type": "error", "code": "BAD_MESSAGE", "message": "nope"}));
        assert_eq!(m.id(), None);
        round_trip::<ServerMsg>(
            json!({"type": "error", "id": "s1", "code": SYNTAX_ERROR, "message": "x at 1:2"}),
        );
        let m: ServerMsg = round_trip(
            json!({"type": "authenticated", "subject": "user:alice", "expires_at": 1762000000000_i64}),
        );
        assert_eq!(m.id(), None);
        assert!(is_auth_error(AUTH_EXPIRED) && !is_auth_error(PERMISSION_DENIED));
    }

    #[test]
    fn array_edits_remove_then_append() {
        let e =
            ArrayEdit::from_json(&json!({"remove": [[0, 1], [2, 2]], "append": ["z"]})).unwrap();
        let mut a = json!(["a", "b", "c", "d", "e"]);
        e.apply(&mut a).unwrap();
        assert_eq!(a, json!(["b", "e", "z"]));
        assert!(e.apply(&mut json!(["a"])).is_err(), "outside the array");
        assert!(
            ArrayEdit::from_json(&json!({"remove": [[1, 1], [0, 1]]}))
                .unwrap()
                .apply(&mut json!([1, 2]))
                .is_err(),
            "not ascending"
        );
        assert!(ArrayEdit::from_json(&json!({"insert": [1]})).is_err());
        assert_eq!(
            ArrayEdit::from_json(&json!({})).unwrap(),
            ArrayEdit::default()
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
