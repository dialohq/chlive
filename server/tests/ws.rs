//! The protocol over a real WebSocket, without ClickHouse or Kafka: a fake history and
//! in-memory events behind an in-process server.

use std::time::Duration;

use async_trait::async_trait;
use chlive_engine::diff::Row;
use chlive_engine::history::{DescribedColumn, HistoryError, HistorySource};
use chlive_engine::rules::{HardcodedRules, TopicId};
use chlive_engine::snapshot::{Lease, Snapshot, SnapshotError, SnapshotTable, Snapshots};
use chlive_engine::source::{Event, EventSource, MemorySource, SourceError, TopicStart};
use chlive_engine::watermark::Watermark;
use chlive_engine::Engine;
use futures::stream::{self, BoxStream, StreamExt};
use futures::SinkExt;
use serde_json::{json, Value};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

/// History as read from a snapshot of `requests`: the row `h1`.
struct FakeHistory;

#[async_trait]
impl HistorySource for FakeHistory {
    async fn describe(&self, table: &str) -> Result<Vec<DescribedColumn>, HistoryError> {
        let col = |n: &str, ty: &str| DescribedColumn {
            name: n.into(),
            ty: ty.into(),
        };
        match table {
            "default.requests" => Ok(vec![col("user_id", "String"), col("latency_ms", "UInt32")]),
            _ => Err(HistoryError::UnknownTable(table.into())),
        }
    }

    async fn fetch(&self, sql: &str) -> Result<Vec<Row>, HistoryError> {
        if sql.contains(" GROUP BY ") {
            // the native aggregate test's history: its one group, as states
            assert_eq!(
                sql,
                "SELECT user_id AS __k0, count() AS __a0_0, max(latency_ms) AS __a1_0, \
                 count(latency_ms) AS __a1_1 FROM default.__chlive_snap_requests \
                 WHERE latency_ms > 100 GROUP BY __k0"
            );
            return Ok(vec![obj(
                json!({"__k0": "h1", "__a0_0": 1, "__a1_0": 900, "__a1_1": 1}),
            )]);
        }
        // the history row, projected to the selected columns (the WHERE is the test's job)
        let history = obj(json!({"user_id": "h1", "latency_ms": 900}));
        let columns = sql
            .strip_prefix("SELECT ")
            .and_then(|s| s.split(" FROM ").next())
            .ok_or_else(|| HistoryError::ClickHouse(format!("unexpected query {sql}")))?;
        Ok(vec![columns
            .split(", ")
            .map(|c| (c.to_string(), history[c].clone()))
            .collect()])
    }
}

/// The snapshot of `requests` holds offset 0 of partition 0 (the row `h1`).
struct FakeSnapshots;

#[async_trait]
impl Snapshots for FakeSnapshots {
    async fn acquire(&self, table: &SnapshotTable) -> Result<Lease, SnapshotError> {
        Ok(std::sync::Arc::new(Snapshot::fixed(
            &table.database,
            &format!("__chlive_snap_{}", table.table),
            Watermark([(0, 0)].into()),
        )))
    }
}

/// Like Kafka: after the events there are, the stream stays open.
struct Endless(MemorySource);

#[async_trait]
impl EventSource for Endless {
    async fn partitions(&self, topic: &TopicId) -> Result<Option<Vec<i32>>, SourceError> {
        self.0.partitions(topic).await
    }

    async fn subscribe(
        &self,
        starts: &[TopicStart],
        keys: &[String],
    ) -> Result<BoxStream<'static, Event>, SourceError> {
        Ok(self
            .0
            .subscribe(starts, keys)
            .await?
            .chain(stream::pending())
            .boxed())
    }
}

fn obj(v: Value) -> Row {
    v.as_object().unwrap().clone()
}

async fn server() -> chlive_server::Server {
    let mut events = MemorySource::new();
    events
        .push("requests", 0, r#"{"user_id": "h1", "latency_ms": 900}"#) // offset 0: history
        .push("requests", 0, r#"{"user_id": "fast", "latency_ms": 5}"#)
        .push("requests", 0, r#"{"user_id": "slow", "latency_ms": 500}"#)
        .push(
            "requests",
            0,
            r#"{"user_id": "slower", "latency_ms": "700"}"#,
        );
    let engine = Engine::new(
        HardcodedRules::default(),
        FakeHistory,
        Endless(events),
        FakeSnapshots,
    );
    chlive_server::start("127.0.0.1:0", engine).await.unwrap()
}

struct Client(WebSocketStream<MaybeTlsStream<TcpStream>>);

impl Client {
    async fn connect(server: &chlive_server::Server) -> Self {
        let (ws, _) = tokio_tungstenite::connect_async(format!("ws://{}/live", server.addr))
            .await
            .unwrap();
        Self(ws)
    }

    async fn send(&mut self, v: Value) {
        self.send_text(&v.to_string()).await;
    }

    async fn send_text(&mut self, text: &str) {
        self.0.send(Message::Text(text.into())).await.unwrap();
    }

    async fn subscribe(&mut self, id: &str, sql: &str) {
        self.send(json!({"type": "subscribe", "id": id, "sql": sql}))
            .await;
    }

    /// The next message, or `None` if none arrives in a while.
    async fn next(&mut self) -> Option<Value> {
        loop {
            let msg = tokio::time::timeout(Duration::from_millis(500), self.0.next())
                .await
                .ok()??
                .unwrap();
            if let Message::Text(t) = msg {
                return Some(serde_json::from_str(&t).unwrap());
            }
        }
    }

    async fn expect(&mut self) -> Value {
        self.next().await.expect("a message")
    }

    /// The inserted rows of `diff`s for `id` until `n` have arrived.
    async fn inserts(&mut self, id: &str, n: usize) -> Vec<Value> {
        let mut rows = Vec::new();
        while rows.len() < n {
            let m = self.expect().await;
            assert_eq!(
                (m["type"].as_str(), m["id"].as_str()),
                (Some("diff"), Some(id)),
                "{m}"
            );
            for c in m["changes"].as_array().unwrap() {
                assert_eq!(c["op"], "insert");
                rows.push(c["row"].clone());
            }
        }
        rows
    }
}

#[tokio::test]
async fn snapshot_then_inserts_of_matching_events() {
    let server = server().await;
    let mut c = Client::connect(&server).await;
    c.subscribe(
        "s1",
        "select user_id, latency_ms from requests where latency_ms > 100",
    )
    .await;
    let mut snapshot = c.expect().await;
    assert!(snapshot["at"].is_string(), "{snapshot}");
    snapshot.as_object_mut().unwrap().remove("at");
    assert_eq!(
        snapshot,
        json!({"type": "snapshot", "id": "s1",
               "columns": [{"name": "user_id", "type": "String"}, {"name": "latency_ms", "type": "UInt32"}],
               "rows": [{"user_id": "h1", "latency_ms": 900}]})
    );
    // offset 0 is history (not repeated); "fast" doesn't match; "700" is read as UInt32
    assert_eq!(
        c.inserts("s1", 2).await,
        [
            json!({"user_id": "slow", "latency_ms": 500}),
            json!({"user_id": "slower", "latency_ms": 700})
        ]
    );
    assert_eq!(c.next().await, None, "nothing for non-matching events");
}

#[tokio::test]
async fn subscriptions_on_one_connection_are_independent() {
    let server = server().await;
    let mut c = Client::connect(&server).await;
    c.subscribe("a", "select user_id from requests where latency_ms < 100")
        .await;
    c.subscribe("b", "select latency_ms from requests").await;
    let mut a = Vec::new();
    let mut b = Vec::new();
    while let Some(m) = c.next().await {
        let rows: Vec<Value> = match m["type"].as_str().unwrap() {
            "snapshot" => m["rows"].as_array().unwrap().clone(),
            "diff" => m["changes"]
                .as_array()
                .unwrap()
                .iter()
                .map(|c| c["row"].clone())
                .collect(),
            other => panic!("unexpected {other}: {m}"),
        };
        match m["id"].as_str().unwrap() {
            "a" => a.extend(rows),
            "b" => b.extend(rows),
            other => panic!("unknown id {other}"),
        }
    }
    // (the fake history ignores WHERE, so `a` gets h1 from history)
    assert_eq!(a, [json!({"user_id": "h1"}), json!({"user_id": "fast"})]);
    assert_eq!(
        b,
        [
            json!({"latency_ms": 900}),
            json!({"latency_ms": 5}),
            json!({"latency_ms": 500}),
            json!({"latency_ms": 700})
        ]
    );
}

#[tokio::test]
async fn errors() {
    let server = server().await;
    let mut c = Client::connect(&server).await;

    c.subscribe("s1", "select * form requests").await;
    assert_eq!(
        c.expect().await,
        json!({"type": "error", "id": "s1", "code": "SYNTAX_ERROR",
               "message": "syntax error: expected FROM, found identifier `form` at 1:10"})
    );
    c.subscribe("s2", "select nope from requests").await;
    assert_eq!(
        c.expect().await,
        json!({"type": "error", "id": "s2", "code": "INVALID_QUERY",
               "message": "unknown column nope in table requests (columns: user_id, latency_ms) at 1:8"})
    );
    c.subscribe("s3", "select * from requests where latency_ms > 'x'")
        .await;
    let m = c.expect().await;
    assert_eq!(
        (m["code"].as_str(), m["message"].as_str()),
        (
            Some("INVALID_QUERY"),
            Some("cannot compare UInt32 with a string at 1:43")
        )
    );

    c.send_text("not json").await;
    let m = c.expect().await;
    assert_eq!(m["code"], "BAD_MESSAGE");
    assert!(m.get("id").is_none());
    c.send(json!({"type": "subscribe", "id": "p", "sql": "select * from requests", "params": {}}))
        .await;
    assert_eq!(c.expect().await["code"], "BAD_MESSAGE");

    // a failed subscription's id can be reused; a live one's can't
    c.subscribe("s1", "select user_id from requests").await;
    assert_eq!(c.expect().await["type"], "snapshot");
    c.subscribe("s1", "select user_id from requests").await;
    let mut codes = Vec::new();
    while let Some(m) = c.next().await {
        codes
            .push(m["type"].as_str().unwrap().to_string() + "/" + m["code"].as_str().unwrap_or(""));
    }
    assert!(
        codes.contains(&"error/DUPLICATE_ID".to_string()),
        "{codes:?}"
    );
}

#[tokio::test]
async fn unsubscribe_stops_the_subscription() {
    let server = server().await;
    let debug = || async {
        let resp = http_get(&format!("http://{}/debug/subscriptions", server.addr)).await;
        serde_json::from_str::<Value>(&resp).unwrap()
    };
    let mut c = Client::connect(&server).await;
    c.subscribe("s1", "select user_id from requests").await;
    assert_eq!(c.expect().await["type"], "snapshot");
    assert_eq!(c.inserts("s1", 3).await.len(), 3);
    let listed = debug().await;
    assert_eq!(listed[0]["id"], "s1");
    assert_eq!(listed[0]["watermark"], "{0: 0}");
    assert_eq!(listed[0]["rowsSent"], 4);

    c.send(json!({"type": "unsubscribe", "id": "s1"})).await;
    assert_eq!(c.next().await, None);
    assert_eq!(debug().await, json!([]));

    // disconnecting releases everything too
    let mut c = Client::connect(&server).await;
    c.subscribe("s1", "select user_id from requests").await;
    assert_eq!(c.expect().await["type"], "snapshot");
    assert_eq!(debug().await.as_array().unwrap().len(), 1);
    drop(c);
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(debug().await, json!([]));
}

#[tokio::test]
async fn ordered_inserts_carry_their_index() {
    let server = server().await;
    let mut c = Client::connect(&server).await;
    c.subscribe(
        "s1",
        "select user_id, latency_ms from requests order by latency_ms desc",
    )
    .await;
    let snapshot = c.expect().await;
    assert_eq!(
        snapshot["order"],
        json!([{"column": "latency_ms", "direction": "desc", "nulls": "last"}])
    );
    assert_eq!(
        snapshot["rows"],
        json!([{"user_id": "h1", "latency_ms": 900}])
    );
    let mut changes = Vec::new();
    while changes.len() < 3 {
        let m = c.expect().await;
        changes.extend(m["changes"].as_array().unwrap().iter().cloned());
    }
    // 5 goes after 900; 500 between them; 700 after 900 again
    let placed: Vec<(String, u64)> = changes
        .iter()
        .map(|c| {
            (
                c["row"]["user_id"].as_str().unwrap().to_string(),
                c["index"].as_u64().expect("an index"),
            )
        })
        .collect();
    assert_eq!(
        placed,
        [("fast".into(), 1), ("slow".into(), 1), ("slower".into(), 1)]
    );
    // unordered: no index, no order
    c.subscribe("s2", "select user_id from requests").await;
    let snapshot = c.expect().await;
    assert!(snapshot.get("order").is_none());
    let m = c.expect().await;
    assert!(m["changes"][0].get("index").is_none(), "{m}");
}

#[tokio::test]
async fn limit_sends_deletes_in_the_same_diff() {
    let server = server().await;
    let mut c = Client::connect(&server).await;
    // without ORDER BY: a window of the latest row
    c.subscribe("w", "select user_id from requests limit 1")
        .await;
    let snapshot = c.expect().await;
    assert_eq!(snapshot["limit"], 1);
    assert_eq!(snapshot["rows"], json!([{"user_id": "h1"}]));
    let mut changes = Vec::new();
    while changes.len() < 6 {
        let m = c.expect().await;
        changes.extend(m["changes"].as_array().unwrap().iter().cloned());
    }
    // each event: appended, then the first row deleted
    for pair in changes.chunks(2) {
        assert_eq!(pair[0]["op"], "insert");
        assert!(pair[0].get("index").is_none());
        assert_eq!(pair[1], json!({"op": "delete", "index": 0}));
    }
    assert_eq!(changes[4]["row"], json!({"user_id": "slower"}));

    // with ORDER BY: the top 2
    c.subscribe(
        "t",
        "select user_id, latency_ms from requests order by latency_ms desc limit 2",
    )
    .await;
    assert_eq!(c.expect().await["limit"], 2);
    let mut rows: Vec<Value> = vec![json!({"user_id": "h1", "latency_ms": 900})];
    let mut seen = 0;
    while seen < 3 {
        let m = c.expect().await;
        for ch in m["changes"].as_array().unwrap() {
            match ch["op"].as_str().unwrap() {
                "insert" => {
                    rows.insert(ch["index"].as_u64().unwrap() as usize, ch["row"].clone());
                    seen += 1;
                }
                "delete" => {
                    rows.remove(ch["index"].as_u64().unwrap() as usize);
                }
                op => panic!("{op}"),
            }
        }
        // the limit holds at the end of every diff
        assert!(rows.len() <= 2, "{rows:?}");
    }
    assert_eq!(
        rows,
        [
            json!({"user_id": "h1", "latency_ms": 900}),
            json!({"user_id": "slower", "latency_ms": 700})
        ]
    );
}

#[tokio::test]
async fn resume_from_a_cursor_sends_only_what_is_missing() {
    let server = server().await;
    let sql = "select user_id from requests";
    let mut c = Client::connect(&server).await;
    c.subscribe("s1", sql).await;
    let snapshot = c.expect().await;
    let at_snapshot = snapshot["at"].as_str().unwrap().to_string();
    assert_eq!(c.inserts("s1", 3).await.len(), 3);
    drop(c);

    // a client holding just the history (h1) gets the three events, and no snapshot
    let mut c = Client::connect(&server).await;
    c.send(json!({"type": "subscribe", "id": "r1", "sql": sql, "since": at_snapshot}))
        .await;
    let resume = c.expect().await;
    assert_eq!(
        resume,
        json!({"type": "resume", "id": "r1", "columns": [{"name": "user_id", "type": "String"}]})
    );
    let mut users = Vec::new();
    let mut last_at = String::new();
    while users.len() < 3 {
        let m = c.expect().await;
        assert_eq!(m["type"], "diff", "{m}");
        last_at = m["at"].as_str().unwrap().to_string();
        users.extend(
            m["changes"]
                .as_array()
                .unwrap()
                .iter()
                .map(|ch| ch["row"]["user_id"].as_str().unwrap().to_string()),
        );
    }
    assert_eq!(users, ["fast", "slow", "slower"]);
    drop(c);

    // a client that is up to date: resume, then just a progress with its cursor
    let mut c = Client::connect(&server).await;
    c.send(json!({"type": "subscribe", "id": "r2", "sql": sql, "since": last_at}))
        .await;
    assert_eq!(c.expect().await["type"], "resume");
    let m = c.expect().await;
    assert_eq!(
        (m["type"].as_str(), m["at"].as_str()),
        (Some("progress"), Some(last_at.as_str()))
    );
    assert_eq!(c.next().await, None);

    // a cursor for another query, or garbage: a normal snapshot
    for since in [last_at.as_str(), "garbage"] {
        c.send(json!({"type": "subscribe", "id": since, "sql": "select user_id, latency_ms from requests", "since": since}))
            .await;
        assert_eq!(c.expect().await["type"], "snapshot");
        let _ = c.inserts(since, 3).await;
    }
}

/// A plain HTTP GET (no client library needed for one endpoint).
async fn http_get(url: &str) -> String {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let rest = url.strip_prefix("http://").unwrap();
    let (host, path) = rest.split_once('/').unwrap();
    let mut s = TcpStream::connect(host).await.unwrap();
    s.write_all(
        format!("GET /{path} HTTP/1.1\r\nHost: {host}\r\nConnection: close\r\n\r\n").as_bytes(),
    )
    .await
    .unwrap();
    let mut buf = String::new();
    s.read_to_string(&mut buf).await.unwrap();
    let body = buf.split_once("\r\n\r\n").unwrap().1;
    body.to_string()
}

#[tokio::test]
async fn aggregates_are_a_snapshot_with_ids_then_new_groups() {
    let server = server().await;
    let mut c = Client::connect(&server).await;
    c.subscribe(
        "g",
        "select user_id, count() as n, max(latency_ms) as top from requests \
         where latency_ms > 100 group by user_id",
    )
    .await;
    let mut snapshot = c.expect().await;
    snapshot.as_object_mut().unwrap().remove("at");
    assert_eq!(
        snapshot,
        json!({"type": "snapshot", "id": "g",
               "columns": [{"name": "user_id", "type": "String"}, {"name": "n", "type": "UInt64"},
                           {"name": "top", "type": "UInt32"}],
               "rows": [{"user_id": "h1", "n": 1, "top": 900}],
               "ids": ["[\"h1\"]"]})
    );
    let mut changes = Vec::new();
    while changes.len() < 2 {
        let m = c.expect().await;
        assert_eq!(
            (m["type"].as_str(), m["id"].as_str()),
            (Some("diff"), Some("g")),
            "{m}"
        );
        changes.extend(m["changes"].as_array().unwrap().iter().cloned());
    }
    assert_eq!(
        changes,
        [
            json!({"op": "insert", "id": "[\"slow\"]", "row": {"user_id": "slow", "n": 1, "top": 500}}),
            json!({"op": "insert", "id": "[\"slower\"]", "row": {"user_id": "slower", "n": 1, "top": 700}}),
        ]
    );
    assert_eq!(c.next().await, None);
}

#[tokio::test]
async fn ordered_aggregates_report_their_order_and_place_new_groups() {
    let server = server().await;
    let mut c = Client::connect(&server).await;
    c.subscribe(
        "g",
        "select user_id, count() as n, max(latency_ms) as top from requests \
         where latency_ms > 100 group by user_id order by top desc limit 2",
    )
    .await;
    let mut snapshot = c.expect().await;
    snapshot.as_object_mut().unwrap().remove("at");
    assert_eq!(
        snapshot,
        json!({"type": "snapshot", "id": "g",
               "columns": [{"name": "user_id", "type": "String"}, {"name": "n", "type": "UInt64"},
                           {"name": "top", "type": "UInt32"}],
               "rows": [{"user_id": "h1", "n": 1, "top": 900}],
               "order": [{"column": "top", "direction": "desc", "nulls": "last"}],
               "limit": 2,
               "ids": ["[\"h1\"]"]})
    );
    let mut changes = Vec::new();
    while changes.len() < 3 {
        let m = c.expect().await;
        assert_eq!(
            (m["type"].as_str(), m["id"].as_str()),
            (Some("diff"), Some("g")),
            "{m}"
        );
        changes.extend(m["changes"].as_array().unwrap().iter().cloned());
    }
    assert_eq!(
        changes,
        [
            json!({"op": "insert", "index": 1, "id": "[\"slow\"]", "row": {"user_id": "slow", "n": 1, "top": 500}}),
            // 700 goes between 900 and 500, and pushes 500 past the LIMIT
            json!({"op": "insert", "index": 1, "id": "[\"slower\"]", "row": {"user_id": "slower", "n": 1, "top": 700}}),
            json!({"op": "delete", "index": 2}),
        ]
    );
    assert_eq!(c.next().await, None);
}
