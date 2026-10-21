//! The protocol over a real WebSocket, without ClickHouse or Kafka: a fake history and
//! in-memory events behind an in-process server.

use std::time::Duration;

use async_trait::async_trait;
use chlive_engine::diff::Row;
use chlive_engine::history::{DescribedColumn, HistoryError, HistorySource};
use chlive_engine::rules::{ConfiguredRules, TopicId};
use chlive_engine::snapshot::{Lease, Snapshot, SnapshotError, SnapshotTable, Snapshots};
use chlive_engine::source::{Event, EventSource, MemorySource, SourceError, TopicStart};
use chlive_engine::watermark::Watermark;
use chlive_engine::Engine;
use chlive_server::security::{Default as SecurityDefault, MemoryProvider, Security, Source};
use chlive_server::{CacheSettings, ServerSettings};
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
        if sql.contains("latency_ms = 13") {
            return Err(HistoryError::ClickHouse("boom".into()));
        }
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
            .map(|c| match c.split_once(" AS ") {
                Some((col, alias)) => (alias.to_string(), history[col].clone()),
                None => (c.to_string(), history[c].clone()),
            })
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
    server_with(ServerSettings::default()).await
}

async fn server_with(settings: ServerSettings) -> chlive_server::Server {
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
        ConfiguredRules::default(),
        FakeHistory,
        Endless(events),
        FakeSnapshots,
    );
    chlive_server::start_with("127.0.0.1:0", settings, engine)
        .await
        .unwrap()
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
async fn parameters_are_bound_by_their_declared_types() {
    let server = server().await;
    let mut c = Client::connect(&server).await;
    // `l` comes as a string and is read as the UInt32 it's declared as; only "slower" (700) is
    // above it
    c.send(json!({"type": "subscribe", "id": "s1",
                  "sql": "select user_id, latency_ms from requests where latency_ms > {l:UInt32}",
                  "params": {"l": "600"}}))
        .await;
    assert_eq!(c.expect().await["type"], "snapshot");
    assert_eq!(
        c.inserts("s1", 1).await,
        [json!({"user_id": "slower", "latency_ms": 700})]
    );
    c.subscribe("s2", "select * from requests where latency_ms > {l:UInt32}")
        .await;
    assert_eq!(
        c.expect().await,
        json!({"type": "error", "id": "s2", "code": "INVALID_QUERY",
               "message": "missing parameter l at 1:43"})
    );
    let listed: Value = serde_json::from_str(
        &http_get(&format!("http://{}/debug/subscriptions", server.addr)).await,
    )
    .unwrap();
    assert_eq!(listed[0]["params"], json!({"l": "600"}));
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
    c.send(json!({"type": "subscribe", "id": "p", "sql": "select * from requests", "nope": {}}))
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
async fn unsubscribe_leaves_the_shared_entry() {
    let server = server_with(ServerSettings {
        cache: CacheSettings {
            keep_alive: Duration::from_millis(400),
            keep_warm: Duration::ZERO,
            ..Default::default()
        },
        ..Default::default()
    })
    .await;
    let debug = || debug(&server);
    let mut c = Client::connect(&server).await;
    c.subscribe("s1", "select user_id from requests").await;
    assert_eq!(c.expect().await["type"], "snapshot");
    assert_eq!(c.inserts("s1", 3).await.len(), 3);
    let listed = debug().await;
    let e = &listed[0];
    assert_eq!(e["sql"], "select user_id from requests");
    assert_eq!(e["stage"], "live");
    let sub = &e["subscribers"][0];
    assert_eq!(e["subscribers"].as_array().unwrap().len(), 1);
    assert_eq!(
        (&sub["conn"], &sub["id"], &sub["lagging"]),
        (&json!(1), &json!("s1"), &json!(false))
    );
    assert_eq!(sub["served"]["source"], "clickhouse", "{sub}");
    assert_eq!(e["rows"], 4);
    assert_eq!(e["at"], "{0: 3}");
    assert_eq!(e["logStart"], "{0: 0}");
    assert_eq!(e["joinsSnapshot"], 0);
    assert_eq!(e["joinsWait"], 1);
    assert!(e["lineage"].as_str().is_some_and(|l| !l.is_empty()));
    assert_eq!(e["key"].as_str().unwrap().len(), 16);

    c.send(json!({"type": "unsubscribe", "id": "s1"})).await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    // kept alive a moment, then (no warm) gone
    let listed = debug().await;
    assert_eq!(listed[0]["stage"], "keep_alive");
    assert_eq!(listed[0]["subscribers"], json!([]));
    assert_eq!(c.next().await, None, "nothing more for it");
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(debug().await, json!([]));

    // disconnecting leaves too
    let mut c = Client::connect(&server).await;
    c.subscribe("s1", "select user_id from requests").await;
    assert_eq!(c.expect().await["type"], "snapshot");
    assert_eq!(debug().await.as_array().unwrap().len(), 1);
    drop(c);
    tokio::time::sleep(Duration::from_millis(700)).await;
    assert_eq!(debug().await, json!([]));
}

async fn debug(server: &chlive_server::Server) -> Value {
    let resp = http_get(&format!("http://{}/debug/subscriptions", server.addr)).await;
    serde_json::from_str::<Value>(&resp).unwrap()
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
    // each event: in at the top (newest first), then the last row deleted
    for pair in changes.chunks(2) {
        assert_eq!(pair[0]["op"], "insert");
        assert_eq!(pair[0]["index"], 0);
        assert_eq!(pair[1], json!({"op": "delete", "index": 1}));
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
async fn offset_holds_back_rows_and_is_in_the_snapshot() {
    let server = server().await;
    let mut c = Client::connect(&server).await;
    // without ORDER BY: the newest row is held back; the history's only row is it
    c.subscribe("o", "select user_id from requests limit 1 offset 1")
        .await;
    let snapshot = c.expect().await;
    assert_eq!(snapshot["limit"], 1);
    assert_eq!(snapshot["offset"], 1);
    assert_eq!(snapshot["rows"], json!([]));
    // the next event is held back instead, and h1 comes into the result
    let m = c.expect().await;
    assert_eq!(
        m["changes"][0],
        json!({"op": "insert", "row": {"user_id": "h1"}, "index": 0})
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

    // a client that is up to date: a resume, and nothing more (it has everything)
    let mut c = Client::connect(&server).await;
    c.send(json!({"type": "subscribe", "id": "r2", "sql": sql, "since": last_at}))
        .await;
    assert_eq!(c.expect().await["type"], "resume");
    assert_eq!(c.next().await, None);

    // a cursor for another query: a snapshot (a new entry: the history, then the events)
    let other = "select user_id, latency_ms from requests";
    c.send(json!({"type": "subscribe", "id": "o1", "sql": other, "since": last_at}))
        .await;
    assert_eq!(c.expect().await["type"], "snapshot");
    let _ = c.inserts("o1", 3).await;
    // garbage: a snapshot of that (now shared) entry as it is, events included
    c.send(json!({"type": "subscribe", "id": "o2", "sql": other, "since": "garbage"}))
        .await;
    let m = c.expect().await;
    assert_eq!(m["type"], "snapshot");
    assert_eq!(m["rows"].as_array().unwrap().len(), 4);
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

// --- [server] settings ----------------------------------------------------------------------

/// Open a WebSocket at `path`, with an `Origin` header if given; the HTTP status if refused.
async fn open(
    server: &chlive_server::Server,
    path: &str,
    origin: Option<&str>,
) -> Result<Client, u16> {
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    use tokio_tungstenite::tungstenite::Error;
    let mut req = format!("ws://{}{path}", server.addr)
        .into_client_request()
        .unwrap();
    if let Some(o) = origin {
        req.headers_mut().insert("origin", o.parse().unwrap());
    }
    match tokio_tungstenite::connect_async(req).await {
        Ok((ws, _)) => Ok(Client(ws)),
        Err(Error::Http(resp)) => Err(resp.status().as_u16()),
        Err(e) => panic!("{e}"),
    }
}

#[tokio::test]
async fn settings_defaults() {
    let d = ServerSettings::default();
    assert_eq!(d.path, "/live");
    assert!(d.allowed_origins.is_empty());
    assert_eq!(d.max_connections, 10_000);
    assert_eq!(d.max_subscriptions_per_connection, 100);
    assert_eq!(d.progress_interval, Duration::from_secs(1));
    // any origin, by default
    let server = server().await;
    assert!(open(&server, "/live", Some("https://anywhere.example"))
        .await
        .is_ok());
}

#[tokio::test]
async fn the_websocket_path_is_configurable() {
    let server = server_with(ServerSettings {
        path: "/ws".into(),
        ..Default::default()
    })
    .await;
    let mut c = open(&server, "/ws", None).await.unwrap();
    c.subscribe("s1", "select user_id from requests").await;
    assert_eq!(c.expect().await["type"], "snapshot");
    assert_eq!(open(&server, "/live", None).await.err(), Some(404));
}

#[tokio::test]
async fn only_allowed_origins_connect() {
    let server = server_with(ServerSettings {
        allowed_origins: vec!["https://app.example".into()],
        ..Default::default()
    })
    .await;
    assert!(open(&server, "/live", Some("https://app.example"))
        .await
        .is_ok());
    assert_eq!(
        open(&server, "/live", Some("https://evil.example"))
            .await
            .err(),
        Some(403)
    );
    // services send no Origin
    assert!(open(&server, "/live", None).await.is_ok());

    let any = server_with(ServerSettings {
        allowed_origins: vec!["*".into()],
        ..Default::default()
    })
    .await;
    assert!(open(&any, "/live", Some("https://evil.example"))
        .await
        .is_ok());
}

#[tokio::test]
async fn connections_beyond_the_limit_are_refused_until_one_closes() {
    let server = server_with(ServerSettings {
        max_connections: 2,
        ..Default::default()
    })
    .await;
    let a = open(&server, "/live", None).await.unwrap();
    let mut b = open(&server, "/live", None).await.unwrap();
    assert_eq!(open(&server, "/live", None).await.err(), Some(503));
    // the others still work
    b.subscribe("s1", "select user_id from requests").await;
    assert_eq!(b.expect().await["type"], "snapshot");
    drop(a);
    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    loop {
        match open(&server, "/live", None).await {
            Ok(_) => break,
            Err(503) if std::time::Instant::now() < deadline => {
                tokio::time::sleep(Duration::from_millis(20)).await
            }
            Err(s) => panic!("{s}"),
        }
    }
}

#[tokio::test]
async fn subscriptions_beyond_the_limit_get_an_error() {
    let server = server_with(ServerSettings {
        max_subscriptions_per_connection: 2,
        ..Default::default()
    })
    .await;
    let mut c = open(&server, "/live", None).await.unwrap();
    let sql = "select user_id from requests where latency_ms > 100";
    c.subscribe("a", sql).await;
    c.subscribe("b", sql).await;
    c.subscribe("c", sql).await;
    let mut errors = Vec::new();
    while let Some(m) = c.next().await {
        if m["type"] == "error" {
            errors.push(m);
        }
    }
    assert_eq!(errors.len(), 1, "{errors:?}");
    assert_eq!(
        (errors[0]["id"].as_str(), errors[0]["code"].as_str()),
        (Some("c"), Some("TOO_MANY_SUBSCRIPTIONS"))
    );
    // unsubscribing makes room; a failed subscription doesn't hold a place
    c.send(json!({"type": "unsubscribe", "id": "a"})).await;
    c.subscribe("bad", "select nope from requests").await;
    assert_eq!(c.expect().await["code"], "INVALID_QUERY");
    c.subscribe("d", sql).await;
    assert_eq!(c.expect().await["type"], "snapshot");
    // and another connection has its own limit
    let mut other = open(&server, "/live", None).await.unwrap();
    other.subscribe("a", sql).await;
    assert_eq!(other.expect().await["type"], "snapshot");
}

/// Events that never match, one every 10 ms: only `progress` messages, as often as allowed.
struct Trickle;

#[async_trait]
impl EventSource for Trickle {
    async fn partitions(&self, _: &TopicId) -> Result<Option<Vec<i32>>, SourceError> {
        Ok(Some(vec![0]))
    }

    async fn subscribe(
        &self,
        starts: &[TopicStart],
        keys: &[String],
    ) -> Result<BoxStream<'static, Event>, SourceError> {
        let mut events = MemorySource::new();
        events.push("requests", 0, r#"{"user_id": "h1", "latency_ms": 900}"#);
        for _ in 0..60 {
            events.push("requests", 0, r#"{"user_id": "fast", "latency_ms": 5}"#);
        }
        Ok(events
            .subscribe(starts, keys)
            .await?
            .then(|e| async {
                tokio::time::sleep(Duration::from_millis(10)).await;
                e
            })
            .chain(stream::pending())
            .boxed())
    }
}

#[tokio::test]
async fn progress_comes_at_most_every_progress_interval() {
    async fn progresses(interval: Duration) -> usize {
        let engine = Engine::new(
            ConfiguredRules::default(),
            FakeHistory,
            Trickle,
            FakeSnapshots,
        );
        let server = chlive_server::start_with(
            "127.0.0.1:0",
            ServerSettings {
                progress_interval: interval,
                ..Default::default()
            },
            engine,
        )
        .await
        .unwrap();
        let mut c = open(&server, "/live", None).await.unwrap();
        c.subscribe("s1", "select user_id from requests where latency_ms > 100")
            .await;
        assert_eq!(c.expect().await["type"], "snapshot");
        let mut n = 0;
        while let Some(m) = c.next().await {
            assert_eq!(m["type"], "progress", "{m}");
            n += 1;
        }
        n
    }
    // 60 events over ~600 ms
    let often = progresses(Duration::from_millis(100)).await;
    assert!((4..=8).contains(&often), "every 100 ms: {often}");
    assert_eq!(
        progresses(Duration::from_secs(10)).await,
        1,
        "the first goes at once"
    );
}

// --- shared entries, the change log, warm results ----------------------------------------------

/// `requests`, partition 0: offset 0 is the history's `h1` (see [`FakeSnapshots`]); events pushed
/// later go to every engine subscription reading it. Counts the engine's subscriptions.
#[derive(Clone)]
struct Live(std::sync::Arc<LiveInner>);

struct LiveInner {
    messages: std::sync::Mutex<Vec<String>>,
    pushed: tokio::sync::broadcast::Sender<(i64, String)>,
    subscribes: std::sync::atomic::AtomicUsize,
    /// The first offset still in the topic (retention).
    low: std::sync::atomic::AtomicI64,
}

impl Live {
    fn new() -> Self {
        Self(std::sync::Arc::new(LiveInner {
            messages: std::sync::Mutex::new(vec![
                r#"{"user_id": "h1", "latency_ms": 900}"#.to_string()
            ]),
            pushed: tokio::sync::broadcast::channel(100_000).0,
            subscribes: Default::default(),
            low: Default::default(),
        }))
    }

    fn push(&self, user: &str, latency: u32) {
        self.push_raw(json!({"user_id": user, "latency_ms": latency}).to_string());
    }

    fn push_raw(&self, message: String) {
        let mut messages = self.0.messages.lock().unwrap();
        messages.push(message.clone());
        let _ = self.0.pushed.send(((messages.len() - 1) as i64, message));
    }

    fn subscribes(&self) -> usize {
        self.0.subscribes.load(std::sync::atomic::Ordering::SeqCst)
    }
}

#[async_trait]
impl EventSource for Live {
    async fn partitions(&self, topic: &TopicId) -> Result<Option<Vec<i32>>, SourceError> {
        Ok((topic.0 == "requests").then(|| vec![0]))
    }

    async fn offset_bounds(
        &self,
        topic: &TopicId,
    ) -> Result<Option<std::collections::BTreeMap<i32, (i64, i64)>>, SourceError> {
        let high = self.0.messages.lock().unwrap().len() as i64;
        let low = self.0.low.load(std::sync::atomic::Ordering::SeqCst);
        Ok((topic.0 == "requests").then(|| [(0, (low, high))].into()))
    }

    async fn subscribe(
        &self,
        starts: &[TopicStart],
        keys: &[String],
    ) -> Result<BoxStream<'static, Event>, SourceError> {
        self.0
            .subscribes
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let keys: std::collections::HashSet<String> = keys.iter().cloned().collect();
        let from = starts
            .iter()
            .flat_map(|s| s.partitions.iter())
            .map(|(_, o)| *o)
            .next()
            .unwrap_or(0);
        // what's there, and what's pushed after, with nothing lost in between
        let (existing, rx) = {
            let messages = self.0.messages.lock().unwrap();
            (messages.clone(), self.0.pushed.subscribe())
        };
        let event =
            move |offset: i64, text: &str, keys: &std::collections::HashSet<String>| Event {
                topic: TopicId("requests".into()),
                partition: 0,
                offset,
                value: chlive_engine::source::decode_pruned(text.as_bytes(), keys).unwrap(),
            };
        let first: Vec<Event> = existing
            .iter()
            .enumerate()
            .skip(from.max(0) as usize)
            .map(|(o, t)| event(o as i64, t, &keys))
            .collect();
        let later = stream::unfold((rx, keys), move |(mut rx, keys)| async move {
            loop {
                match rx.recv().await {
                    Ok((offset, text)) if offset >= from => {
                        let e = event(offset, &text, &keys);
                        return Some((e, (rx, keys)));
                    }
                    Ok(_) => continue,
                    Err(_) => return None,
                }
            }
        });
        Ok(stream::iter(first).chain(later).boxed())
    }
}

async fn live_server(live: &Live, settings: ServerSettings) -> chlive_server::Server {
    let engine = Engine::new(
        ConfiguredRules::default(),
        FakeHistory,
        live.clone(),
        FakeSnapshots,
    );
    chlive_server::start_with("127.0.0.1:0", settings, engine)
        .await
        .unwrap()
}

fn cache(cache: CacheSettings) -> ServerSettings {
    ServerSettings {
        cache,
        ..Default::default()
    }
}

/// Every `diff`'s inserted users, until `n` have come; and the last cursor.
async fn users(c: &mut Client, id: &str, n: usize) -> (Vec<String>, String) {
    let (mut out, mut at) = (Vec::new(), String::new());
    while out.len() < n {
        let m = c.expect().await;
        assert_eq!(
            (m["type"].as_str(), m["id"].as_str()),
            (Some("diff"), Some(id)),
            "{m}"
        );
        at = m["at"].as_str().unwrap().to_string();
        for ch in m["changes"].as_array().unwrap() {
            out.push(ch["row"]["user_id"].as_str().unwrap().to_string());
        }
    }
    (out, at)
}

#[tokio::test]
async fn subscribers_to_one_query_share_one_entry() {
    let live = Live::new();
    let server = live_server(&live, ServerSettings::default()).await;
    let sql = "select user_id from requests where latency_ms > {l:UInt32}";
    let mut a = Client::connect(&server).await;
    a.send(json!({"type": "subscribe", "id": "a", "sql": sql, "params": {"l": 100}}))
        .await;
    assert_eq!(a.expect().await["rows"], json!([{"user_id": "h1"}]));
    live.push("u1", 500);
    let (got, a_at) = users(&mut a, "a", 1).await;
    assert_eq!(got, ["u1"]);

    // another connection, the same query and value (spelled "100"): the same entry, which it
    // gets as a snapshot of the rows as they are now, at the same cursor
    let mut b = Client::connect(&server).await;
    b.send(json!({"type": "subscribe", "id": "b", "sql": sql, "params": {"l": "100"}}))
        .await;
    let snap = b.expect().await;
    assert_eq!(snap["type"], "snapshot");
    assert_eq!(snap["rows"], json!([{"user_id": "h1"}, {"user_id": "u1"}]));
    assert_eq!(snap["at"].as_str().unwrap(), a_at);
    assert_eq!(live.subscribes(), 1, "one engine subscription");
    // both get what comes next
    live.push("u2", 600);
    assert_eq!(users(&mut a, "a", 1).await.0, ["u2"]);
    assert_eq!(users(&mut b, "b", 1).await.0, ["u2"]);
    // another value: another entry
    // (a new entry: the history, then the events after it)
    let mut c = Client::connect(&server).await;
    c.send(json!({"type": "subscribe", "id": "c", "sql": sql, "params": {"l": 550}}))
        .await;
    assert_eq!(c.expect().await["rows"], json!([{"user_id": "h1"}]));
    assert_eq!(users(&mut c, "c", 1).await.0, ["u2"]);
    assert_eq!(live.subscribes(), 2);
    assert_eq!(debug(&server).await.as_array().unwrap().len(), 2);
}

#[tokio::test]
async fn a_returning_client_gets_what_it_missed_from_the_log() {
    let live = Live::new();
    let server = live_server(&live, ServerSettings::default()).await;
    let sql = "select user_id from requests";
    let mut c = Client::connect(&server).await;
    c.subscribe("s", sql).await;
    assert_eq!(c.expect().await["type"], "snapshot");
    live.push("u1", 1);
    let (_, at) = users(&mut c, "s", 1).await;
    drop(c);

    // while away (kept alive)
    live.push("u2", 1);
    live.push("u3", 1);
    tokio::time::sleep(Duration::from_millis(100)).await;

    let mut c = Client::connect(&server).await;
    c.send(json!({"type": "subscribe", "id": "s", "sql": sql, "since": at}))
        .await;
    assert_eq!(c.expect().await["type"], "resume");
    assert_eq!(users(&mut c, "s", 2).await.0, ["u2", "u3"]);
    assert_eq!(c.next().await, None, "nothing more");
    let e = &debug(&server).await[0];
    assert_eq!(
        (e["joinsResume"].as_u64(), e["joinsWait"].as_u64()),
        (Some(1), Some(1))
    );
    assert_eq!(live.subscribes(), 1);
}

/// A new directory for a disk store, removed when dropped.
struct TempDir(std::path::PathBuf);

impl TempDir {
    fn new() -> Self {
        static N: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let d = std::env::temp_dir().join(format!(
            "chlive-ws-test-{}-{}",
            std::process::id(),
            N.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
        ));
        let _ = std::fs::remove_dir_all(&d);
        Self(d)
    }

    fn cache(&self, c: CacheSettings) -> ServerSettings {
        cache(CacheSettings {
            dir: Some(self.0.clone()),
            ..c
        })
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

/// The debug listing's (sql, stage) pairs.
async fn stages(server: &chlive_server::Server) -> Vec<(String, String)> {
    debug(server)
        .await
        .as_array()
        .unwrap()
        .iter()
        .map(|e| {
            (
                e["sql"].as_str().unwrap().to_string(),
                e["stage"].as_str().unwrap().to_string(),
            )
        })
        .collect()
}

#[tokio::test]
async fn warm_entries_follow_kafka_with_their_rows_on_disk() {
    let live = Live::new();
    let dir = TempDir::new();
    let server = live_server(
        &live,
        dir.cache(CacheSettings {
            keep_alive: Duration::from_millis(50),
            keep_warm: Duration::from_secs(60),
            ..Default::default()
        }),
    )
    .await;
    let sql = "select user_id from requests";
    let mut c = Client::connect(&server).await;
    c.subscribe("s", sql).await;
    let at = c.expect().await["at"].as_str().unwrap().to_string();
    drop(c);
    tokio::time::sleep(Duration::from_millis(250)).await;
    let e = &debug(&server).await[0];
    assert_eq!(
        (&e["stage"], &e["inMemory"], &e["persisted"]),
        (&json!("warm"), &json!(false), &json!(true)),
        "{e}"
    );
    // events while warm go to disk
    live.push("w1", 1);
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert_eq!(debug(&server).await[0]["rows"], 2);
    // back while warm: the rows and the log from disk; a resume with what it missed
    let mut c = Client::connect(&server).await;
    c.send(json!({"type": "subscribe", "id": "s", "sql": sql, "since": at}))
        .await;
    assert_eq!(c.expect().await["type"], "resume");
    assert_eq!(users(&mut c, "s", 1).await.0, ["w1"]);
    let mut d = Client::connect(&server).await;
    d.subscribe("d", sql).await;
    let snap = d.expect().await;
    assert_eq!(snap["rows"], json!([{"user_id": "h1"}, {"user_id": "w1"}]));
    let e = &debug(&server).await[0];
    assert_eq!(
        (&e["stage"], &e["inMemory"]),
        (&json!("live"), &json!(true)),
        "{e}"
    );
    assert_eq!(live.subscribes(), 1, "it never stopped following Kafka");
}

#[tokio::test]
async fn the_debug_state_shows_the_whole_server() {
    let live = Live::new();
    let dir = TempDir::new();
    let settings = dir.cache(CacheSettings {
        keep_alive: Duration::from_millis(50),
        keep_warm: Duration::from_secs(60),
        ..Default::default()
    });
    let server = live_server(&live, settings.clone()).await;
    let state_of = |server: &chlive_server::Server| {
        let url = format!("http://{}/debug/state", server.addr);
        async move { serde_json::from_str::<Value>(&http_get(&url).await).unwrap() }
    };
    let state = || state_of(&server);
    let mut c = Client::connect(&server).await;
    c.subscribe("s", "select user_id from requests").await;
    c.expect().await;
    let s = state().await;
    assert_eq!(s["server"]["connections"], 1, "{s}");
    assert_eq!(s["server"]["path"], "/live");
    assert!(s["server"]["now"].as_i64().unwrap() >= s["server"]["startedAt"].as_i64().unwrap());
    assert_eq!(s["cache"]["keepAlive"], 0.05);
    assert_eq!(s["snapshots"], json!([]), "the fake snapshots don't say");
    let e = &s["entries"][0];
    assert_eq!(e["stage"], "live", "{e}");
    let conns = s["connections"].as_array().unwrap();
    assert_eq!(conns.len(), 1);
    assert_eq!(
        conns[0]["subscriptions"],
        json!([{"id": "s", "key": e["key"]}]),
        "a subscription links to its entry"
    );
    assert_eq!(conns[0]["outbox"], 0);
    let store = &s["store"];
    assert!(store["path"].as_str().unwrap().ends_with("cache.sqlite3"));
    assert!(store["bytes"].as_u64().unwrap() > 0, "{store}");
    assert!(store["batches"].as_u64().unwrap() >= 1);
    assert_eq!(store["byStage"], json!({"live": 1}));
    assert_eq!(store["budget"], 10u64 << 30);

    // gone: the connection goes, the entry goes warm with a countdown to cold
    drop(c);
    tokio::time::sleep(Duration::from_millis(250)).await;
    let s = state().await;
    assert_eq!(s["connections"], json!([]));
    let e = &s["entries"][0];
    assert_eq!(e["stage"], "warm", "{e}");
    let left = e["stageEndsIn"].as_f64().unwrap();
    assert!(left > 50.0 && left <= 60.0, "{e}");
    assert!(e["stageEndsAt"].as_u64().unwrap() > s["server"]["now"].as_u64().unwrap());
    assert!(e["warmSince"].is_u64());
    assert_eq!(s["store"]["byStage"], json!({"warm": 1}));

    // the page polls it
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut t = TcpStream::connect(server.addr).await.unwrap();
    t.write_all(b"GET /debug HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n")
        .await
        .unwrap();
    let mut page = String::new();
    t.read_to_string(&mut page).await.unwrap();
    assert!(page.starts_with("HTTP/1.1 200"), "{page}");
    assert!(page.to_lowercase().contains("content-type: text/html"));
    assert!(page.contains(r#"fetch("debug/state""#));

    // restarted: the result is cold, listed from the store
    server.shutdown().await;
    let server = live_server(&live, settings).await;
    let s = state_of(&server).await;
    assert_eq!(s["entries"][0]["stage"], "cold", "{s}");
    assert_eq!(s["store"]["byStage"], json!({"cold": 1}));
}

#[tokio::test]
async fn each_join_says_where_its_result_came_from_and_what_it_got() {
    let live = Live::new();
    let dir = TempDir::new();
    let server = live_server(
        &live,
        dir.cache(CacheSettings {
            keep_alive: Duration::from_millis(50),
            keep_warm: Duration::from_secs(60),
            ..Default::default()
        }),
    )
    .await;
    let sql = "select user_id from requests";
    let joins = || async {
        let body = http_get(&format!("http://{}/debug/state", server.addr)).await;
        serde_json::from_str::<Value>(&body).unwrap()["joins"].clone()
    };
    // the first: a new result, from ClickHouse, which it waited for
    let mut a = Client::connect(&server).await;
    a.subscribe("a", sql).await;
    let at = a.expect().await["at"].as_str().unwrap().to_string();
    let j = &joins().await[0];
    assert_eq!(
        (
            &j["id"],
            &j["source"],
            &j["decision"],
            &j["reason"],
            &j["cursor"]
        ),
        (
            &json!("a"),
            &json!("clickhouse"),
            &json!("snapshot"),
            &json!("no_cursor"),
            &json!(false)
        ),
        "{j}"
    );
    assert_eq!(j["snapshot"], "default.__chlive_snap_requests");
    assert_eq!(j["newSnapshot"], true, "taken for it");
    assert!(j["waitedMs"].is_u64() && j["loadMs"].is_u64(), "{j}");
    assert!(j.get("rejected").is_none(), "nothing stored: {j}");
    // the next: the result in memory, no wait
    let mut b = Client::connect(&server).await;
    b.subscribe("b", sql).await;
    b.expect().await;
    let j = &joins().await[0];
    assert_eq!(
        (&j["id"], &j["source"], &j["decision"], &j["rows"]),
        (&json!("b"), &json!("memory"), &json!("snapshot"), &json!(1)),
        "{j}"
    );
    assert!(j.get("waitedMs").is_none(), "{j}");
    // with a cursor: a resume from the memory log
    live.push("u1", 1);
    users(&mut a, "a", 1).await;
    let mut c = Client::connect(&server).await;
    c.send(json!({"type": "subscribe", "id": "c", "sql": sql, "since": at}))
        .await;
    assert_eq!(c.expect().await["type"], "resume");
    let j = &joins().await[0];
    assert_eq!(
        (
            &j["source"],
            &j["decision"],
            &j["log"],
            &j["diffs"],
            &j["cursor"]
        ),
        (
            &json!("memory"),
            &json!("resume"),
            &json!("memory"),
            &json!(1),
            &json!(true)
        ),
        "{j}"
    );
    // a cursor of another lineage: a snapshot, and why
    let mut d = Client::connect(&server).await;
    d.send(json!({"type": "subscribe", "id": "d", "sql": sql, "since": "nonsense"}))
        .await;
    d.expect().await;
    assert_eq!(joins().await[0]["reason"], "bad_token");
    // everyone gone, the result warm: the next one gets the rows from disk
    drop((a, b, c, d));
    tokio::time::sleep(Duration::from_millis(250)).await;
    let mut e = Client::connect(&server).await;
    e.subscribe("e", sql).await;
    e.expect().await;
    let all = joins().await;
    let j = &all[0];
    assert_eq!(
        (&j["source"], &j["decision"]),
        (&json!("warm"), &json!("snapshot")),
        "{j}"
    );
    assert!(j["loadMs"].is_u64());
    let ids: Vec<&str> = all
        .as_array()
        .unwrap()
        .iter()
        .map(|j| j["id"].as_str().unwrap())
        .collect();
    assert_eq!(ids, ["e", "d", "c", "b", "a"], "newest first");
    // each subscriber also carries how it was served
    let entry = &debug(&server).await[0];
    assert_eq!(entry["subscribers"][0]["served"]["source"], "warm");
    assert_eq!(
        entry["source"]["source"], "clickhouse",
        "how the entry started: {entry}"
    );
}

#[tokio::test]
async fn cold_entries_are_restored_from_disk_and_resume_through_the_fork() {
    let live = Live::new();
    let dir = TempDir::new();
    let server = live_server(
        &live,
        dir.cache(CacheSettings {
            keep_alive: Duration::from_millis(30),
            keep_warm: Duration::ZERO,
            ..Default::default()
        }),
    )
    .await;
    let sql = "select user_id from requests";
    let mut c = Client::connect(&server).await;
    c.subscribe("s", sql).await;
    let first = c.expect().await["at"].as_str().unwrap().to_string();
    live.push("u1", 1);
    let (_, at) = users(&mut c, "s", 1).await;
    drop(c);
    tokio::time::sleep(Duration::from_millis(250)).await;
    let listed = debug(&server).await;
    assert_eq!(listed.as_array().unwrap().len(), 1, "{listed}");
    assert_eq!(listed[0]["stage"], "cold");
    assert!(listed[0]["bytes"].as_i64().unwrap() > 0);
    // while cold, nothing follows Kafka
    live.push("u2", 1);
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(live.subscribes(), 1);

    // back: restored from its checkpoint (no snapshot, no history), caught up from Kafka;
    // its cursor, from the old lineage, resumes
    let mut c = Client::connect(&server).await;
    c.send(json!({"type": "subscribe", "id": "s", "sql": sql, "since": at}))
        .await;
    assert_eq!(c.expect().await["type"], "resume");
    assert_eq!(users(&mut c, "s", 1).await.0, ["u2"]);
    // and an older one, from the log on disk
    c.send(json!({"type": "subscribe", "id": "t", "sql": sql, "since": first}))
        .await;
    assert_eq!(c.expect().await["type"], "resume");
    assert_eq!(users(&mut c, "t", 2).await.0, ["u1", "u2"]);
    let e = &debug(&server).await[0];
    assert_eq!(
        (&e["restored"], &e["stage"]),
        (&json!(true), &json!("live"))
    );
    assert_eq!(e["forks"].as_array().unwrap().len(), 1, "{e}");
    assert_ne!(e["lineage"], e["forks"][0], "a new lineage");
    assert_eq!(live.subscribes(), 2);
    let mut d = Client::connect(&server).await;
    d.subscribe("d", sql).await;
    assert_eq!(
        d.expect().await["rows"],
        json!([{"user_id": "h1"}, {"user_id": "u1"}, {"user_id": "u2"}])
    );
}

#[tokio::test]
async fn a_restarted_server_restores_from_disk_when_subscribed() {
    let live = Live::new();
    let dir = TempDir::new();
    let settings = dir.cache(CacheSettings::default());
    let sql = "select user_id from requests order by latency_ms desc limit 2";
    let server = live_server(&live, settings.clone()).await;
    let mut c = Client::connect(&server).await;
    c.subscribe("s", sql).await;
    assert_eq!(c.expect().await["type"], "snapshot");
    live.push("u1", 1000);
    let m = c.expect().await;
    let at = m["at"].as_str().unwrap().to_string();
    drop(c);
    server.shutdown().await;
    drop(server);

    live.push("u2", 950);
    let server = live_server(&live, settings).await;
    assert_eq!(
        stages(&server).await,
        [(sql.to_string(), "cold".to_string())]
    );
    let mut c = Client::connect(&server).await;
    c.send(json!({"type": "subscribe", "id": "s", "sql": sql, "since": at}))
        .await;
    assert_eq!(c.expect().await["type"], "resume");
    let m = c.expect().await;
    assert_eq!(m["type"], "diff", "{m}");
    // u2 in, h1 (900) out of the top two
    let ops: Vec<&str> = m["changes"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| c["op"].as_str().unwrap())
        .collect();
    assert_eq!(ops, ["insert", "delete"], "{m}");
    let e = &debug(&server).await[0];
    assert_eq!(e["restored"], true);
    let served = &e["subscribers"][0]["served"];
    assert_eq!(
        (
            &served["source"],
            &served["decision"],
            &served["eventsBehind"]
        ),
        (&json!("restored"), &json!("resume"), &json!(1)),
        "a cold result restored, resumed through the fork: {served}"
    );
    assert!(served["waitedMs"].is_u64() && served["loadMs"].is_u64());
    let mut d = Client::connect(&server).await;
    d.subscribe("d", sql).await;
    assert_eq!(
        d.expect().await["rows"],
        json!([{"user_id": "u1"}, {"user_id": "u2"}])
    );
}

#[tokio::test]
async fn stored_results_that_cant_continue_start_from_clickhouse() {
    let sql = "select user_id from requests";
    // each case: a stored result (left by a server that stopped), then a server with `c`
    async fn case(name: &str, c: CacheSettings, before: impl FnOnce(&Live)) {
        let live = Live::new();
        let dir = TempDir::new();
        let server = live_server(&live, dir.cache(CacheSettings::default())).await;
        let mut cl = Client::connect(&server).await;
        cl.subscribe("s", "select user_id from requests").await;
        cl.expect().await;
        live.push("u1", 1);
        users(&mut cl, "s", 1).await;
        drop(cl);
        server.shutdown().await;
        drop(server);
        before(&live);
        let server = live_server(&live, dir.cache(c)).await;
        let mut cl = Client::connect(&server).await;
        cl.subscribe("s", "select user_id from requests").await;
        let snap = cl.expect().await;
        assert_eq!(snap["type"], "snapshot", "{name}");
        // the history (the fake's one row), then the events after the snapshot
        assert_eq!(snap["rows"], json!([{"user_id": "h1"}]), "{name}");
        let e = &debug(&server).await[0];
        assert_eq!(e["restored"], false, "{name}: {e}");
        // the subscriber hears why
        let served = &e["subscribers"][0]["served"];
        assert_eq!(served["source"], "clickhouse", "{name}: {served}");
        let rejected = served["rejected"].as_str().unwrap();
        assert!(rejected.starts_with(name), "{name}: {rejected}");
        assert_eq!(live.subscribes(), 2, "{name}");
    }
    let _ = sql;
    case(
        "max_age",
        CacheSettings {
            max_age: Duration::from_millis(1),
            ..Default::default()
        },
        |_| std::thread::sleep(Duration::from_millis(20)),
    )
    .await;
    case(
        "max_catch_up",
        CacheSettings {
            max_catch_up: 2,
            ..Default::default()
        },
        |live| {
            for i in 0..3 {
                live.push(&format!("x{i}"), 1);
            }
        },
    )
    .await;
    case("retention", CacheSettings::default(), |live| {
        live.push("x", 1);
        live.push("y", 1);
        live.0.low.store(3, std::sync::atomic::Ordering::SeqCst);
    })
    .await;
}

#[tokio::test]
async fn which_entries_are_kept() {
    let live = Live::new();
    // no disk store: gone after the keep-alive
    let server = live_server(
        &live,
        cache(CacheSettings {
            keep_alive: Duration::from_millis(30),
            ..Default::default()
        }),
    )
    .await;
    let mut c = Client::connect(&server).await;
    c.subscribe("s", "select user_id from requests").await;
    c.expect().await;
    let e = &debug(&server).await[0];
    assert_eq!(e["persisted"], false);
    drop(c);
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(debug(&server).await, json!([]));

    // max_warm = 1: the one warm the longest goes cold
    let dir = TempDir::new();
    let server = live_server(
        &live,
        dir.cache(CacheSettings {
            keep_alive: Duration::from_millis(30),
            max_warm: 1,
            ..Default::default()
        }),
    )
    .await;
    let mut c = Client::connect(&server).await;
    c.subscribe("a", "select user_id from requests").await;
    c.expect().await;
    c.send(json!({"type": "unsubscribe", "id": "a"})).await;
    tokio::time::sleep(Duration::from_millis(150)).await;
    c.subscribe("b", "select latency_ms from requests").await;
    c.expect().await;
    c.send(json!({"type": "unsubscribe", "id": "b"})).await;
    let want = [
        (
            "select latency_ms from requests".to_string(),
            "warm".to_string(),
        ),
        (
            "select user_id from requests".to_string(),
            "cold".to_string(),
        ),
    ];
    let started = std::time::Instant::now();
    loop {
        let got = stages(&server).await;
        if got == want {
            break;
        }
        assert!(started.elapsed() < Duration::from_secs(5), "{got:?}");
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

#[tokio::test]
async fn a_cursor_the_log_no_longer_covers_gets_a_snapshot() {
    let live = Live::new();
    let server = live_server(
        &live,
        cache(CacheSettings {
            log_length: 1,
            ..Default::default()
        }),
    )
    .await;
    let sql = "select user_id from requests";
    let mut c = Client::connect(&server).await;
    c.subscribe("s", sql).await;
    let at = c.expect().await["at"].as_str().unwrap().to_string();
    drop(c);
    // two messages while away; the log keeps one
    live.push("u1", 1);
    tokio::time::sleep(Duration::from_millis(50)).await;
    live.push("u2", 1);
    tokio::time::sleep(Duration::from_millis(50)).await;
    let mut c = Client::connect(&server).await;
    c.send(json!({"type": "subscribe", "id": "s", "sql": sql, "since": at.clone()}))
        .await;
    let snap = c.expect().await;
    assert_eq!(snap["type"], "snapshot", "{snap}");
    assert_eq!(snap["rows"].as_array().unwrap().len(), 3);
    // garbage, an old (v1) cursor: snapshots too
    let v1 = at.replace("\"v\":2", "\"v\":1");
    for (id, since) in [("g", "garbage".to_string()), ("v1", v1)] {
        c.send(json!({"type": "subscribe", "id": id, "sql": sql, "since": since}))
            .await;
        assert_eq!(c.expect().await["type"], "snapshot");
    }
}

#[tokio::test]
async fn a_slow_subscriber_catches_up_without_holding_others_up() {
    let live = Live::new();
    let server = live_server(&live, ServerSettings::default()).await;
    let sql = "select user_id from requests";
    // a client that doesn't read for a while, and one that does
    let mut slow = Client::connect(&server).await;
    slow.subscribe("s", sql).await;
    let mut fast = Client::connect(&server).await;
    fast.subscribe("f", sql).await;
    assert_eq!(fast.expect().await["type"], "snapshot");
    // many messages, big enough to fill the socket and the slow one's queue
    let big = "x".repeat(20_000);
    let mut fast_got = 0;
    for i in 0..600 {
        live.push(&format!("{big}{i}"), 1);
        tokio::time::sleep(Duration::from_millis(1)).await;
        while let Ok(Some(Ok(_))) =
            tokio::time::timeout(Duration::from_millis(0), fast.0.next()).await
        {
            fast_got += 1;
        }
    }
    // the fast one got everything meanwhile
    let (rest, _) = {
        let mut n = 0;
        while let Some(m) = fast.next().await {
            n += m["changes"].as_array().map_or(0, |c| c.len());
        }
        (n, ())
    };
    assert!(fast_got > 0 && rest < 600, "the fast client kept up");
    let e = &debug(&server).await[0];
    assert!(
        e["subscribers"]
            .as_array()
            .unwrap()
            .iter()
            .any(|s| s["lagging"] == true)
            || e["resyncs"].as_u64().unwrap() > 0,
        "the slow one fell behind: {e}"
    );
    // the slow one reads now: it ends up with the whole result (a snapshot replaces its rows,
    // or the log brings them up to date)
    let mut rows: Vec<String> = Vec::new();
    while let Some(m) = slow.next().await {
        match m["type"].as_str() {
            Some("snapshot") => {
                rows = m["rows"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|r| r["user_id"].as_str().unwrap().to_string())
                    .collect()
            }
            Some("diff") => rows.extend(
                m["changes"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|c| c["row"]["user_id"].as_str().unwrap().to_string()),
            ),
            _ => {}
        }
    }
    assert_eq!(rows.len(), 601, "h1 and the 600 events, each once");
    let mut sorted = rows.clone();
    sorted.sort();
    sorted.dedup();
    assert_eq!(sorted.len(), 601);
}

#[tokio::test]
async fn an_entry_that_fails_tells_every_subscriber_and_goes() {
    let live = Live::new();
    let server = live_server(&live, ServerSettings::default()).await;
    let sql = "select user_id from requests where latency_ms = 13";
    let mut a = Client::connect(&server).await;
    a.subscribe("a", sql).await;
    let mut b = Client::connect(&server).await;
    b.subscribe("b", sql).await;
    for (c, id) in [(&mut a, "a"), (&mut b, "b")] {
        let m = c.expect().await;
        assert_eq!(
            (m["type"].as_str(), m["id"].as_str(), m["code"].as_str()),
            (Some("error"), Some(id), Some("HISTORY_FAILED")),
            "{m}"
        );
    }
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(debug(&server).await, json!([]));
    // the same id can subscribe again: a new attempt
    a.subscribe("a", sql).await;
    assert_eq!(a.expect().await["code"], "HISTORY_FAILED");
}

/// `requests` secured by user_id, its grants in `provider` (grant 0); nothing else readable.
fn secured(provider: std::sync::Arc<MemoryProvider>) -> ServerSettings {
    ServerSettings {
        security: std::sync::Arc::new(Security::secured(SecurityDefault::Deny).rows(
            "requests",
            "user_id",
            Source { provider, grant: 0 },
        )),
        ..ServerSettings::default()
    }
}

async fn hello(c: &mut Client, user: &str) {
    c.send(json!({"type": "hello", "user": user})).await;
}

#[tokio::test]
async fn secured_tables_show_each_user_their_rows() {
    let p = MemoryProvider::loaded();
    p.set(0, "alice", &["slow", "fast"]);
    p.set(0, "bob", &["slower"]);
    let server = server_with(secured(p)).await;

    // anonymous, or another table: denied
    let mut anon = Client::connect(&server).await;
    anon.subscribe("s", "select user_id from requests").await;
    let m = anon.expect().await;
    assert_eq!(
        (&m["code"], &m["id"]),
        (&json!("PERMISSION_DENIED"), &json!("s")),
        "{m}"
    );
    let mut a = Client::connect(&server).await;
    hello(&mut a, "alice").await;
    a.subscribe(
        "x",
        "select * from requests where user_id in (select user_id from other)",
    )
    .await;
    let m = a.expect().await;
    assert_eq!(m["code"], "PERMISSION_DENIED", "{m}");
    assert!(
        m["message"].as_str().unwrap().contains("table other"),
        "{m}"
    );

    // each user: their rows (the fake history ignores the WHERE)
    a.subscribe("s", "select user_id from requests").await;
    assert_eq!(a.expect().await["type"], "snapshot");
    assert_eq!(
        a.inserts("s", 2).await,
        [json!({"user_id": "fast"}), json!({"user_id": "slow"})]
    );
    assert_eq!(a.next().await, None);
    let mut b = Client::connect(&server).await;
    hello(&mut b, "bob").await;
    b.subscribe("s", "select user_id from requests").await;
    assert_eq!(b.expect().await["type"], "snapshot");
    assert_eq!(b.inserts("s", 1).await, [json!({"user_id": "slower"})]);
    assert_eq!(b.next().await, None);
    // the filter is in the plan: two entries
    assert_eq!(debug(&server).await.as_array().unwrap().len(), 2);

    // hello only comes first
    hello(&mut a, "bob").await;
    let m = a.expect().await;
    assert_eq!(
        (&m["code"], &m["id"]),
        (&json!("PERMISSION_DENIED"), &Value::Null),
        "{m}"
    );
}

#[tokio::test]
async fn a_permission_change_replaces_the_result() {
    let p = MemoryProvider::loaded();
    p.set(0, "alice", &["fast"]);
    let server = server_with(secured(p.clone())).await;
    let mut a = Client::connect(&server).await;
    hello(&mut a, "alice").await;
    a.subscribe("s", "select user_id from requests").await;
    assert_eq!(a.expect().await["type"], "snapshot");
    assert_eq!(a.inserts("s", 1).await, [json!({"user_id": "fast"})]);
    assert_eq!(a.next().await, None);

    // granted more: a new snapshot for the same id, then what matches now
    p.set(0, "alice", &["fast", "slower"]);
    let m = a.expect().await;
    assert_eq!(
        (&m["type"], &m["id"]),
        (&json!("snapshot"), &json!("s")),
        "{m}"
    );
    assert_eq!(
        a.inserts("s", 2).await,
        [json!({"user_id": "fast"}), json!({"user_id": "slower"})]
    );
    assert_eq!(a.next().await, None);
    // the same values again: nothing
    p.set(0, "alice", &["slower", "fast"]);
    assert_eq!(a.next().await, None);
    // nothing left: a snapshot of no stream rows (the fake history still has h1)
    p.set(0, "alice", &[]);
    let m = a.expect().await;
    assert_eq!(m["type"], "snapshot", "{m}");
    while let Some(m) = a.next().await {
        assert_eq!(m["type"], "progress", "no rows: {m}");
    }
    // the old entries go (no subscribers), the current one stays
    tokio::time::sleep(Duration::from_millis(100)).await;
    let subscribed: Vec<Value> = debug(&server)
        .await
        .as_array()
        .unwrap()
        .iter()
        .filter(|e| !e["subscribers"].as_array().unwrap().is_empty())
        .cloned()
        .collect();
    assert_eq!(subscribed.len(), 1, "{subscribed:?}");
}

#[tokio::test]
async fn users_with_the_same_permissions_share_an_entry() {
    let p = MemoryProvider::loaded();
    p.set(0, "alice", &["slow"]);
    p.set(0, "bob", &["slow"]);
    p.set(0, "carol", &["fast"]);
    let server = server_with(secured(p)).await;
    for user in ["alice", "bob", "carol"] {
        let mut c = Client::connect(&server).await;
        hello(&mut c, user).await;
        c.subscribe("s", "select user_id from requests").await;
        assert_eq!(c.expect().await["type"], "snapshot");
        std::mem::forget(c);
    }
    let entries = debug(&server).await;
    let mut sizes: Vec<usize> = entries
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["subscribers"].as_array().unwrap().len())
        .collect();
    sizes.sort();
    assert_eq!(sizes, [1, 2]);
}

/// `requests` with all rows; grant 1 says which tables a user may read, grant 2 which columns
/// of `requests`.
fn tables_and_columns(provider: std::sync::Arc<MemoryProvider>) -> ServerSettings {
    let source = |grant| Source {
        provider: provider.clone(),
        grant,
    };
    ServerSettings {
        security: std::sync::Arc::new(
            Security::secured(SecurityDefault::Deny)
                .public("requests")
                .tables(source(1))
                .columns("requests", source(2)),
        ),
        ..ServerSettings::default()
    }
}

/// Every message until a quiet while, by subscription id.
async fn drain(c: &mut Client) -> std::collections::BTreeMap<String, Vec<Value>> {
    let mut by_id = std::collections::BTreeMap::<String, Vec<Value>>::new();
    while let Some(m) = c.next().await {
        let id = m["id"].as_str().unwrap_or_default().to_string();
        by_id.entry(id).or_default().push(m);
    }
    by_id
}

fn columns_of(snapshot: &Value) -> Vec<String> {
    snapshot["columns"]
        .as_array()
        .unwrap()
        .iter()
        .map(|c| c["name"].as_str().unwrap().to_string())
        .collect()
}

#[tokio::test]
async fn tables_must_be_granted_and_a_revoke_ends_the_subscription() {
    let p = MemoryProvider::loaded();
    for user in ["alice", "bob"] {
        p.set(2, user, &["user_id", "latency_ms"]);
    }
    p.set(1, "alice", &["requests"]);
    let server = server_with(tables_and_columns(p.clone())).await;

    let mut b = Client::connect(&server).await;
    hello(&mut b, "bob").await;
    b.subscribe("s", "select user_id from requests").await;
    let m = b.expect().await;
    assert_eq!(
        (&m["code"], &m["id"]),
        (&json!("PERMISSION_DENIED"), &json!("s")),
        "{m}"
    );
    assert!(
        m["message"].as_str().unwrap().contains("table requests"),
        "{m}"
    );

    let mut a = Client::connect(&server).await;
    hello(&mut a, "alice").await;
    a.subscribe("s", "select user_id from requests").await;
    assert_eq!(a.expect().await["type"], "snapshot");
    assert_eq!(a.inserts("s", 3).await.len(), 3);
    // revoked: the subscription ends
    p.set(1, "alice", &[]);
    let m = a.expect().await;
    assert_eq!(
        (&m["type"], &m["code"], &m["id"]),
        (&json!("error"), &json!("PERMISSION_DENIED"), &json!("s")),
        "{m}"
    );
    assert_eq!(a.next().await, None);
    // and is over: granted again, nothing comes until it subscribes again
    p.set(1, "alice", &["requests"]);
    assert_eq!(a.next().await, None);
    a.subscribe("s", "select user_id from requests").await;
    assert_eq!(a.expect().await["type"], "snapshot");
}

#[tokio::test]
async fn a_table_grant_allows_while_it_answers_anything() {
    let p = MemoryProvider::loaded();
    p.set(0, "alice", &["requests:read"]);
    let settings = ServerSettings {
        security: std::sync::Arc::new(Security::secured(SecurityDefault::Deny).table(
            "requests",
            Source {
                provider: p.clone(),
                grant: 0,
            },
        )),
        ..ServerSettings::default()
    };
    let server = server_with(settings).await;

    let mut b = Client::connect(&server).await;
    hello(&mut b, "bob").await;
    b.subscribe("s", "select user_id from requests").await;
    assert_eq!(b.expect().await["code"], "PERMISSION_DENIED");

    let mut a = Client::connect(&server).await;
    hello(&mut a, "alice").await;
    a.subscribe("s", "select user_id from requests").await;
    assert_eq!(a.expect().await["type"], "snapshot");
    assert_eq!(a.inserts("s", 3).await.len(), 3);
    // revoked: the subscription ends
    p.set(0, "alice", &[]);
    let m = a.expect().await;
    assert_eq!(
        (&m["type"], &m["code"], &m["id"]),
        (&json!("error"), &json!("PERMISSION_DENIED"), &json!("s")),
        "{m}"
    );
}

#[tokio::test]
async fn columns_must_be_granted_and_star_follows_them() {
    let p = MemoryProvider::loaded();
    for user in ["alice", "bob"] {
        p.set(1, user, &["requests"]);
    }
    p.set(2, "alice", &["user_id", "latency_ms"]);
    p.set(2, "bob", &["user_id"]);
    let server = server_with(tables_and_columns(p.clone())).await;

    // Bob: `*` is the columns he may read; naming another is denied, anywhere
    let mut b = Client::connect(&server).await;
    hello(&mut b, "bob").await;
    b.subscribe("s", "select * from requests").await;
    let m = b.expect().await;
    assert_eq!(columns_of(&m), ["user_id"], "{m}");
    drain(&mut b).await;
    b.subscribe("t", "select user_id from requests where latency_ms > 5")
        .await;
    let m = b.expect().await;
    assert_eq!(
        (&m["code"], &m["id"]),
        (&json!("PERMISSION_DENIED"), &json!("t")),
        "{m}"
    );
    assert!(
        m["message"]
            .as_str()
            .unwrap()
            .contains("column latency_ms of requests isn't readable here"),
        "{m}"
    );

    // Alice: both columns; her `*` isn't Bob's
    let mut a = Client::connect(&server).await;
    hello(&mut a, "alice").await;
    a.subscribe("star", "select * from requests").await;
    a.subscribe("named", "select user_id, latency_ms from requests")
        .await;
    let first = drain(&mut a).await;
    assert_eq!(columns_of(&first["star"][0]), ["user_id", "latency_ms"]);
    assert_eq!(first["named"][0]["type"], "snapshot");
    // her `*` is exactly her named query (one entry), Bob's `*` another
    assert_eq!(debug(&server).await.as_array().unwrap().len(), 2);

    // latency_ms revoked: the query naming it ends, `*` gets a snapshot without it
    p.set(2, "alice", &["user_id"]);
    let after = drain(&mut a).await;
    let named = &after["named"];
    assert_eq!(named.len(), 1, "{named:?}");
    assert_eq!(named[0]["code"], "PERMISSION_DENIED", "{named:?}");
    let star = &after["star"];
    assert_eq!(star[0]["type"], "snapshot", "{star:?}");
    assert_eq!(columns_of(&star[0]), ["user_id"]);
}
