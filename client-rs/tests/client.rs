//! The client against a fake server: a WebSocket listener the test drives by hand.

use std::sync::Arc;
use std::time::Duration;

use chlive_client::{
    Change, ClientOptions, Kind, LiveClient, LiveError, Params, Row, Status, Subscription, Update,
};
use futures::{SinkExt, StreamExt};
use serde_json::{json, Value};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;

const SQL: &str = "select user_id from events where latency_ms > 100";

/// Accepts connections; each one comes out of `next()`.
struct FakeServer {
    url: String,
    conns: mpsc::Receiver<FakeConn>,
}

struct FakeConn(WebSocketStream<TcpStream>);

impl FakeServer {
    async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}/live", listener.local_addr().unwrap());
        let (tx, conns) = mpsc::channel(8);
        tokio::spawn(async move {
            while let Ok((tcp, _)) = listener.accept().await {
                let ws = tokio_tungstenite::accept_async(tcp).await.unwrap();
                if tx.send(FakeConn(ws)).await.is_err() {
                    return;
                }
            }
        });
        FakeServer { url, conns }
    }

    async fn next(&mut self) -> FakeConn {
        within(self.conns.recv()).await.expect("a connection")
    }

    fn client(&self) -> LiveClient {
        let mut opts = ClientOptions::new(&self.url);
        opts.min_backoff = Duration::from_millis(5);
        LiveClient::new(opts)
    }
}

impl FakeConn {
    /// The next message from the client.
    async fn recv(&mut self) -> Value {
        loop {
            match within(self.0.next()).await {
                Some(Ok(Message::Text(t))) => return serde_json::from_str(&t).unwrap(),
                Some(Ok(_)) => continue,
                other => panic!("connection ended: {other:?}"),
            }
        }
    }

    async fn send(&mut self, v: Value) {
        self.0.send(Message::text(v.to_string())).await.unwrap();
    }

    async fn close(mut self) {
        let _ = self.0.close(None).await;
    }
}

async fn within<F: std::future::Future>(f: F) -> F::Output {
    tokio::time::timeout(Duration::from_secs(5), f)
        .await
        .expect("timed out")
}

async fn next<R: serde::de::DeserializeOwned>(sub: &mut Subscription<R>) -> Update<R> {
    within(sub.next()).await.expect("an update")
}

fn snapshot(id: &str, rows: Value) -> Value {
    json!({"type": "snapshot", "id": id, "columns": [{"name": "user_id", "type": "String"}],
           "rows": rows, "at": "c0"})
}

fn users(u: &Update<Row>) -> Vec<&str> {
    u.rows
        .iter()
        .map(|r| r["user_id"].as_str().unwrap())
        .collect()
}

#[tokio::test]
async fn subscribes_applies_messages_and_resubscribes_after_reconnect() {
    let mut server = FakeServer::start().await;
    let client = server.client();
    let mut sub = client.subscribe::<Row>(SQL);
    let first = next(&mut sub).await;
    assert_eq!(
        (first.kind, first.status),
        (Kind::Subscribed, Status::Loading)
    );

    let mut ws = server.next().await;
    assert_eq!(
        ws.recv().await,
        json!({"type": "subscribe", "id": "s1", "sql": SQL})
    );
    ws.send(snapshot("s1", json!([{"user_id": "h1"}]))).await;
    let u = next(&mut sub).await;
    assert_eq!((u.kind, &u.status), (Kind::Snapshot, &Status::Live));
    assert_eq!(users(&u), ["h1"]);
    assert_eq!(u.columns[0].name, "user_id");

    // messages for other ids, and progress, give no update
    ws.send(json!({"type": "diff", "id": "s9", "changes": [{"op": "insert", "row": {"user_id": "x"}}], "at": "c"}))
        .await;
    ws.send(json!({"type": "progress", "id": "s1", "at": "c1"}))
        .await;
    ws.send(json!({"type": "diff", "id": "s1", "changes": [
        {"op": "insert", "row": {"user_id": "e1"}}, {"op": "insert", "row": {"user_id": "e2"}}
    ], "at": "c2"}))
        .await;
    let u = next(&mut sub).await;
    assert_eq!(u.kind, Kind::Diff);
    assert_eq!(users(&u), ["h1", "e1", "e2"]);
    assert_eq!(u.changes.len(), 2);
    assert_eq!(u.at.as_deref(), Some("c2"));

    // the connection drops: rows are kept, it subscribes again from its cursor, and a snapshot
    // (the server couldn't resume) replaces them
    ws.close().await;
    let u = next(&mut sub).await;
    assert_eq!((u.kind, &u.status), (Kind::Disconnected, &Status::Loading));
    assert_eq!(users(&u), ["h1", "e1", "e2"]);
    let mut ws = server.next().await;
    assert_eq!(
        ws.recv().await,
        json!({"type": "subscribe", "id": "s1", "sql": SQL, "since": "c2"})
    );
    ws.send(snapshot(
        "s1",
        json!([{"user_id": "h1"}, {"user_id": "e1"}]),
    ))
    .await;
    let u = next(&mut sub).await;
    assert_eq!((u.kind, &u.status), (Kind::Snapshot, &Status::Live));
    assert_eq!(users(&u), ["h1", "e1"]);
    assert_eq!(u.stats.subscribes, 2);
    assert_eq!(u.stats.snapshots, 2);
}

#[tokio::test]
async fn parameters_are_sent_and_sent_again_after_a_reconnect() {
    let mut server = FakeServer::start().await;
    let client = server.client();
    let sql = "select user_id from events where session_id = {s:UUID}";
    let params = Params::from([(
        "s".to_string(),
        json!("ABCDEF00-0000-0000-0000-000000000001"),
    )]);
    let _sub = client.subscribe_with::<Row>(sql, params);
    let expected = json!({"type": "subscribe", "id": "s1", "sql": sql,
                          "params": {"s": "ABCDEF00-0000-0000-0000-000000000001"}});
    let mut conn = server.next().await;
    assert_eq!(conn.recv().await, expected);
    conn.close().await;
    let mut again = server.next().await;
    assert_eq!(again.recv().await, expected);
}

#[tokio::test]
async fn only_complete_queries_ask_for_a_complete_snapshot() {
    let mut server = FakeServer::start().await;
    let client = server.client();
    let _plain = client.query(SQL).raw::<Row>();
    let mut ws = server.next().await;
    assert_eq!(
        ws.recv().await,
        json!({"type": "subscribe", "id": "s1", "sql": SQL})
    );
    let _complete = client.query(SQL).complete().raw::<Row>();
    assert_eq!(
        ws.recv().await,
        json!({"type": "subscribe", "id": "s2", "sql": SQL, "complete": true})
    );
}

#[tokio::test]
async fn errors_are_per_subscription() {
    let mut server = FakeServer::start().await;
    let client = server.client();
    let mut bad = client.subscribe::<Row>("select nope from events");
    let mut good = client.subscribe::<Row>(SQL);
    next(&mut bad).await;
    next(&mut good).await;
    let mut ws = server.next().await;
    ws.recv().await;
    ws.recv().await;
    ws.send(json!({"type": "error", "id": "s1", "code": "INVALID_QUERY",
                   "message": "unknown column nope at 1:8"}))
        .await;
    ws.send(snapshot("s2", json!([{"user_id": "h1"}]))).await;
    let u = next(&mut bad).await;
    assert_eq!(u.kind, Kind::Error);
    let Status::Error(e) = u.status else {
        panic!("{:?}", u.status)
    };
    assert_eq!(e.code, "INVALID_QUERY");
    assert!(within(bad.next()).await.is_none(), "the stream ends");
    // the client tells the server it's done with it
    assert_eq!(ws.recv().await, json!({"type": "unsubscribe", "id": "s1"}));
    assert_eq!(users(&next(&mut good).await), ["h1"]);
}

#[tokio::test]
async fn an_unknown_change_op_fails_the_subscription() {
    let mut server = FakeServer::start().await;
    let client = server.client();
    let mut sub = client.subscribe::<Row>(SQL);
    next(&mut sub).await;
    let mut ws = server.next().await;
    ws.recv().await;
    ws.send(snapshot("s1", json!([{"user_id": "h1"}]))).await;
    let before = next(&mut sub).await;
    ws.send(
        json!({"type": "diff", "id": "s1", "changes": [{"op": "upsert", "row": {}}], "at": "c"}),
    )
    .await;
    let u = next(&mut sub).await;
    assert!(
        matches!(&u.status, Status::Error(e) if e.code == "CLIENT_ERROR"),
        "{:?}",
        u.status
    );
    // the rows are what they were before
    assert!(std::sync::Arc::ptr_eq(&u.rows, &before.rows));
    assert!(within(sub.next()).await.is_none());
}

#[tokio::test]
async fn updates_never_have_more_rows_than_the_limit() {
    let mut server = FakeServer::start().await;
    let client = server.client();
    let mut sub = client.subscribe::<Row>(SQL);
    next(&mut sub).await;
    let mut ws = server.next().await;
    ws.recv().await;
    let mut snap = snapshot("s1", json!([{"user_id": "a"}, {"user_id": "b"}]));
    snap["limit"] = json!(2);
    ws.send(snap).await;
    next(&mut sub).await;
    ws.send(json!({"type": "diff", "id": "s1", "changes": [
        {"op": "insert", "row": {"user_id": "new"}, "index": 0}, {"op": "delete", "index": 2}
    ], "at": "c"}))
        .await;
    let u = next(&mut sub).await;
    assert_eq!(users(&u), ["new", "a"]);
    assert!(matches!(&u.changes[1], Change::Delete { index: 2, row } if row["user_id"] == "b"));
    assert_eq!((u.stats.inserts, u.stats.deletes, u.stats.diffs), (1, 1, 1));
    assert!(u.stats.bytes > 0);
}

#[tokio::test]
async fn dropping_a_subscription_unsubscribes() {
    let mut server = FakeServer::start().await;
    let client = server.client();
    let sub = client.subscribe::<Row>(SQL);
    let mut ws = server.next().await;
    ws.recv().await;
    drop(sub);
    assert_eq!(ws.recv().await, json!({"type": "unsubscribe", "id": "s1"}));
    // and a later one is subscribed on the same connection
    let _other = client.subscribe::<Row>(SQL);
    assert_eq!(
        ws.recv().await,
        json!({"type": "subscribe", "id": "s2", "sql": SQL})
    );
}

#[tokio::test]
async fn close_ends_every_stream() {
    let mut server = FakeServer::start().await;
    let client = server.client();
    let mut a = client.subscribe::<Row>(SQL);
    let mut b = client.subscribe::<Row>(SQL);
    next(&mut a).await;
    next(&mut b).await;
    let mut ws = server.next().await;
    ws.recv().await;
    ws.recv().await;
    within(client.close()).await;
    assert!(within(a.next()).await.is_none());
    assert!(within(b.next()).await.is_none());
}

#[derive(Debug, serde::Deserialize, PartialEq)]
struct Country {
    country: String,
    n: u64,
}

#[tokio::test]
async fn typed_rows_of_an_aggregated_result() {
    let mut server = FakeServer::start().await;
    let client = server.client();
    let mut sub =
        client.subscribe::<Country>("select country, count() as n from events group by country");
    next(&mut sub).await;
    let mut ws = server.next().await;
    ws.recv().await;
    ws.send(
        json!({"type": "snapshot", "id": "s1", "columns": [], "at": "c",
        "rows": [{"country": "PL", "n": 2}, {"country": "DE", "n": 1}],
        "ids": ["[\"PL\"]", "[\"DE\"]"],
        "order": [{"column": "n", "direction": "desc", "nulls": "last"}]}),
    )
    .await;
    next(&mut sub).await;
    ws.send(json!({"type": "diff", "id": "s1", "at": "c2",
        "changes": [{"op": "update", "id": "[\"DE\"]", "set": {"n": 3}, "index": 0}]}))
        .await;
    let u = next(&mut sub).await;
    let want = Country {
        country: "DE".into(),
        n: 3,
    };
    assert_eq!(*u.rows[0], want);
    assert!(
        matches!(&u.changes[0], Change::Update { id, row, index: Some(0) } if id == "[\"DE\"]" && **row == want)
    );
}

#[derive(Debug, serde::Deserialize, PartialEq)]
struct Product {
    product_id: String,
    price: u64,
}

/// FINAL results: rows with ids, replaced by updates of the whole row and deleted by id.
#[tokio::test]
async fn final_rows_are_updated_and_deleted_by_id() {
    let mut server = FakeServer::start().await;
    let client = server.client();
    let mut sub = client.subscribe::<Product>("select product_id, price from products final");
    next(&mut sub).await;
    let mut ws = server.next().await;
    ws.recv().await;
    ws.send(
        json!({"type": "snapshot", "id": "s1", "columns": [], "at": "c",
        "rows": [{"product_id": "a", "price": 1}, {"product_id": "b", "price": 2}],
        "ids": ["[\"a\"]", "[\"b\"]"]}),
    )
    .await;
    next(&mut sub).await;
    ws.send(json!({"type": "diff", "id": "s1", "at": "c2", "changes": [
        {"op": "update", "id": "[\"b\"]", "set": {"product_id": "b", "price": 5}},
        {"op": "delete", "id": "[\"a\"]"},
        {"op": "insert", "row": {"product_id": "c", "price": 3}, "id": "[\"c\"]"}
    ]}))
    .await;
    let u = next(&mut sub).await;
    let prices: Vec<(String, u64)> = u
        .rows
        .iter()
        .map(|p| (p.product_id.clone(), p.price))
        .collect();
    assert_eq!(prices, [("b".to_string(), 5), ("c".to_string(), 3)]);
    assert!(
        matches!(&u.changes[1], Change::Delete { index: 0, row } if row.product_id == "a"),
        "{:?}",
        u.changes
    );
    // a delete of an unknown id is a protocol error
    ws.send(json!({"type": "diff", "id": "s1", "at": "c3",
        "changes": [{"op": "delete", "id": "[\"nope\"]"}]}))
        .await;
    let e = next(&mut sub).await;
    assert_eq!(e.kind, Kind::Error);
    let Status::Error(err) = e.status else {
        panic!("{:?}", e.status)
    };
    assert!(err.message.contains("unknown row"), "{}", err.message);
}

#[tokio::test]
async fn a_snapshot_bigger_than_16_mib_arrives() {
    let mut server = FakeServer::start().await;
    let client = server.client();
    let mut sub = client.subscribe::<Row>(SQL);
    assert_eq!(next(&mut sub).await.kind, Kind::Subscribed);
    let mut conn = server.next().await;
    conn.recv().await;
    // ~20 MB in one message
    let rows: Vec<Value> = (0..100_000)
        .map(|i| json!({"user_id": format!("{i:0>200}")}))
        .collect();
    conn.send(
        json!({"type": "snapshot", "id": "s1", "columns": [{"name": "user_id", "type": "String"}],
                     "rows": rows, "at": "c1"}),
    )
    .await;
    let u = next(&mut sub).await;
    assert_eq!(u.kind, Kind::Snapshot);
    assert_eq!(u.rows.len(), 100_000);
}

fn diff(id: &str, at: &str, changes: Value) -> Value {
    json!({"type": "diff", "id": id, "changes": changes, "at": at})
}

fn insert(user: &str) -> Value {
    json!({"op": "insert", "row": {"user_id": user}})
}

#[derive(Debug, serde::Deserialize, PartialEq)]
struct User {
    user_id: String,
}

fn user(u: &str) -> User {
    User { user_id: u.into() }
}

/// A connection with the subscription `id`'s `subscribe` read.
async fn subscribed(server: &mut FakeServer) -> FakeConn {
    let mut ws = server.next().await;
    ws.recv().await;
    ws
}

#[tokio::test]
async fn a_reconnect_resumes_from_the_cursor_and_keeps_the_rows() {
    let mut server = FakeServer::start().await;
    let client = server.client();
    let mut sub = client.subscribe::<Row>(SQL);
    next(&mut sub).await;
    let mut ws = subscribed(&mut server).await;
    ws.send(snapshot("s1", json!([{"user_id": "a"}]))).await;
    next(&mut sub).await;
    ws.send(diff("s1", "c1", json!([insert("b")]))).await;
    // progress moves the cursor too
    ws.send(json!({"type": "progress", "id": "s1", "at": "c2"}))
        .await;
    next(&mut sub).await;
    ws.close().await;
    assert_eq!(next(&mut sub).await.kind, Kind::Disconnected);

    let mut ws = server.next().await;
    assert_eq!(ws.recv().await["since"], "c2");
    ws.send(
        json!({"type": "resume", "id": "s1", "columns": [{"name": "user_id", "type": "String"}]}),
    )
    .await;
    let u = next(&mut sub).await;
    assert_eq!((u.kind, &u.status), (Kind::Resumed, &Status::Live));
    assert_eq!(users(&u), ["a", "b"]);
    ws.send(diff("s1", "c3", json!([insert("c")]))).await;
    assert_eq!(users(&next(&mut sub).await), ["a", "b", "c"]);
}

#[tokio::test]
async fn rows_are_every_state_and_end_with_an_error() {
    let mut server = FakeServer::start().await;
    let client = server.client();
    let mut rows = client.query(SQL).rows::<User>();
    let mut ws = subscribed(&mut server).await;
    ws.send(snapshot("s1", json!([{"user_id": "a"}]))).await;
    let r = within(rows.next()).await.unwrap().unwrap();
    assert_eq!(r.len(), 1);
    ws.send(diff("s1", "c1", json!([insert("b")]))).await;
    let r = within(rows.next()).await.unwrap().unwrap();
    assert_eq!(*r[1], user("b"));
    ws.send(json!({"type": "error", "id": "s1", "code": "STREAM_FAILED", "message": "boom"}))
        .await;
    let e = within(rows.next()).await.unwrap().unwrap_err();
    assert_eq!(e.code, "STREAM_FAILED");
    assert!(within(rows.next()).await.is_none());
}

#[tokio::test]
async fn first_and_last_rows_are_kept_current() {
    let mut server = FakeServer::start().await;
    let client = server.client();
    let q = client.query(SQL);
    let mut first = q.clone().first::<User>();
    let mut last = q.last::<User>();
    let mut ws = server.next().await;
    ws.recv().await;
    ws.recv().await;
    assert_eq!(first.get(), None);
    assert_eq!(first.status(), Status::Loading);

    ws.send(snapshot("s1", json!([]))).await;
    ws.send(snapshot("s2", json!([]))).await;
    assert_eq!(within(first.current()).await.unwrap(), None);
    assert_eq!(within(last.current()).await.unwrap(), None);

    ws.send(diff("s1", "c1", json!([insert("a")]))).await;
    ws.send(diff("s2", "c1", json!([insert("a")]))).await;
    assert_eq!(
        within(first.changed()).await.unwrap().as_deref(),
        Some(&user("a"))
    );
    assert_eq!(
        within(last.changed()).await.unwrap().as_deref(),
        Some(&user("a"))
    );

    // a row at the end changes the last, not the first
    ws.send(diff("s1", "c2", json!([insert("b")]))).await;
    ws.send(diff("s2", "c2", json!([insert("b")]))).await;
    assert_eq!(
        within(last.changed()).await.unwrap().as_deref(),
        Some(&user("b"))
    );
    // the same rows again (a snapshot after a reconnect): nothing changed
    ws.send(snapshot("s1", json!([{"user_id": "a"}, {"user_id": "b"}])))
        .await;
    ws.send(diff(
        "s1",
        "c3",
        json!([{"op": "insert", "row": {"user_id": "z"}, "index": 0}]),
    ))
    .await;
    assert_eq!(
        within(first.changed()).await.unwrap().as_deref(),
        Some(&user("z"))
    );
    assert_eq!(first.status(), Status::Live);
}

#[tokio::test]
async fn a_slow_reader_gets_only_the_latest_value() {
    let mut server = FakeServer::start().await;
    let client = server.client();
    let mut last = client.query(SQL).last::<User>();
    let mut ws = subscribed(&mut server).await;
    ws.send(snapshot("s1", json!([]))).await;
    for (i, u) in ["a", "b", "c", "d"].into_iter().enumerate() {
        ws.send(diff("s1", &format!("c{i}"), json!([insert(u)])))
            .await;
    }
    // once "d" is in, the values before it are gone
    let latest = within(last.wait_for(|r| r.as_deref() == Some(&user("d"))))
        .await
        .unwrap();
    assert_eq!(latest.as_deref(), Some(&user("d")));
    ws.send(diff("s1", "c9", json!([insert("e")]))).await;
    assert_eq!(
        within(last.changed()).await.unwrap().as_deref(),
        Some(&user("e"))
    );
}

#[tokio::test]
async fn a_cell_is_the_first_column_of_the_first_row() {
    let mut server = FakeServer::start().await;
    let client = server.client();
    let mut count = client.query("select count() from events").cell::<u64>();
    let mut flag = client.query("select paid from orders").cell::<bool>();
    let mut ws = server.next().await;
    ws.recv().await;
    ws.recv().await;
    let cols = json!([{"name": "count()", "type": "UInt64"}]);
    ws.send(json!({"type": "snapshot", "id": "s1", "columns": cols, "rows": [], "at": "c0"}))
        .await;
    assert_eq!(within(count.current()).await.unwrap(), None);
    // ClickHouse may quote a UInt64
    ws.send(json!({"type": "snapshot", "id": "s1", "columns": cols, "rows": [{"count()": "41"}], "at": "c1"}))
        .await;
    assert_eq!(within(count.changed()).await.unwrap(), Some(41));
    ws.send(json!({"type": "snapshot", "id": "s1", "columns": cols, "rows": [{"count()": 42}], "at": "c2"}))
        .await;
    assert_eq!(within(count.changed()).await.unwrap(), Some(42));

    ws.send(
        json!({"type": "snapshot", "id": "s2", "columns": [{"name": "paid", "type": "Bool"}],
                   "rows": [{"paid": false}, {"paid": true}], "at": "c0"}),
    )
    .await;
    let mut flags = flag.clone().into_stream();
    assert_eq!(within(flag.current()).await.unwrap(), Some(false));
    // the stream starts from what its own handle hasn't returned
    assert_eq!(within(flags.next()).await.unwrap().unwrap(), Some(false));
}

#[tokio::test]
async fn a_value_that_does_not_fit_fails_the_view() {
    let mut server = FakeServer::start().await;
    let client = server.client();
    let mut n = client.query(SQL).cell::<u64>();
    let mut ws = subscribed(&mut server).await;
    ws.send(snapshot("s1", json!([{"user_id": "not a number"}])))
        .await;
    let e = within(n.current()).await.unwrap_err();
    assert_eq!(e.code, "CLIENT_ERROR");
    assert!(matches!(n.status(), Status::Error(_)));
    // and it unsubscribed
    assert_eq!(ws.recv().await, json!({"type": "unsubscribe", "id": "s1"}));
}

#[tokio::test]
async fn exists_resolves_once_there_is_a_row_and_unsubscribes() {
    let mut server = FakeServer::start().await;
    let client = server.client();
    let waiting = tokio::spawn(client.query(SQL).exists());
    let mut ws = subscribed(&mut server).await;
    ws.send(snapshot("s1", json!([]))).await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(!waiting.is_finished());
    ws.send(diff("s1", "c1", json!([insert("a")]))).await;
    within(waiting).await.unwrap().unwrap();
    assert_eq!(ws.recv().await, json!({"type": "unsubscribe", "id": "s1"}));

    let failing = tokio::spawn(client.query(SQL).exists());
    ws.recv().await;
    ws.send(json!({"type": "error", "id": "s2", "code": "INVALID_QUERY", "message": "no"}))
        .await;
    let e: LiveError = within(failing).await.unwrap().unwrap_err();
    assert_eq!(e.code, "INVALID_QUERY");
}

#[tokio::test]
async fn inserts_and_deletes_are_each_row_once_after_the_snapshot() {
    let mut server = FakeServer::start().await;
    let client = server.client();
    let q = client.query(SQL);
    let mut inserts = q.clone().inserts::<User>();
    let mut deletes = q.deletes::<User>();
    let mut ws = server.next().await;
    ws.recv().await;
    ws.recv().await;
    for id in ["s1", "s2"] {
        let mut snap = snapshot(id, json!([{"user_id": "a"}, {"user_id": "b"}]));
        snap["limit"] = json!(2);
        ws.send(snap).await;
        ws.send(diff(
            id,
            "c1",
            json!([
                {"op": "insert", "row": {"user_id": "c"}, "index": 0}, {"op": "delete", "index": 2},
                {"op": "insert", "row": {"user_id": "d"}, "index": 0}, {"op": "delete", "index": 2},
            ]),
        ))
        .await;
    }
    // the snapshot's rows aren't inserts
    assert_eq!(*within(inserts.next()).await.unwrap().unwrap(), user("c"));
    assert_eq!(*within(inserts.next()).await.unwrap().unwrap(), user("d"));
    assert_eq!(*within(deletes.next()).await.unwrap().unwrap(), user("b"));
    assert_eq!(*within(deletes.next()).await.unwrap().unwrap(), user("a"));

    // resumed: nothing again, only what's new
    ws.close().await;
    let mut ws = server.next().await;
    assert_eq!(ws.recv().await["since"], "c1");
    assert_eq!(ws.recv().await["since"], "c1");
    for id in ["s1", "s2"] {
        ws.send(
            json!({"type": "resume", "id": id, "columns": [{"name": "user_id", "type": "String"}]}),
        )
        .await;
        ws.send(diff(
            id,
            "c2",
            json!([
                {"op": "insert", "row": {"user_id": "e"}, "index": 0}, {"op": "delete", "index": 2},
            ]),
        ))
        .await;
    }
    assert_eq!(*within(inserts.next()).await.unwrap().unwrap(), user("e"));
    assert_eq!(*within(deletes.next()).await.unwrap().unwrap(), user("c"));
}

#[tokio::test]
async fn a_snapshot_after_a_reconnect_tells_only_what_changed_meanwhile() {
    let mut server = FakeServer::start().await;
    let client = server.client();
    let mut inserts = client.query(SQL).inserts::<User>();
    let mut ws = subscribed(&mut server).await;
    // two equal rows: counted, not merged
    ws.send(snapshot(
        "s1",
        json!([{"user_id": "a"}, {"user_id": "a"}, {"user_id": "b"}]),
    ))
    .await;
    ws.close().await;
    let mut ws = subscribed(&mut server).await;
    ws.send(snapshot(
        "s1",
        json!([{"user_id": "a"}, {"user_id": "a"}, {"user_id": "a"}, {"user_id": "b"}, {"user_id": "c"}]),
    ))
    .await;
    ws.send(diff("s1", "c1", json!([insert("d")]))).await;
    let mut got = Vec::new();
    for _ in 0..3 {
        got.push(
            within(inserts.next())
                .await
                .unwrap()
                .unwrap()
                .user_id
                .clone(),
        );
    }
    assert_eq!(got, ["a", "c", "d"]);
}

#[tokio::test]
async fn fetch_takes_the_snapshot_and_unsubscribes() {
    let mut server = FakeServer::start().await;
    let client = server.client();
    let fetching = tokio::spawn(client.query(SQL).fetch::<User>());
    let mut ws = subscribed(&mut server).await;
    ws.send(snapshot("s1", json!([{"user_id": "a"}, {"user_id": "b"}])))
        .await;
    let rows = within(fetching).await.unwrap().unwrap();
    assert_eq!(
        rows.iter().map(|r| r.user_id.as_str()).collect::<Vec<_>>(),
        ["a", "b"]
    );
    assert_eq!(ws.recv().await, json!({"type": "unsubscribe", "id": "s1"}));

    let one = tokio::spawn(client.query(SQL).fetch_one::<User>());
    ws.recv().await;
    ws.send(snapshot("s2", json!([{"user_id": "x"}]))).await;
    assert_eq!(
        within(one).await.unwrap().unwrap().as_deref(),
        Some(&user("x"))
    );
    ws.recv().await;

    let cell = tokio::spawn(
        client
            .query("select count() from events")
            .fetch_cell::<u64>(),
    );
    ws.recv().await;
    ws.send(
        json!({"type": "snapshot", "id": "s3", "columns": [{"name": "n", "type": "UInt64"}],
                   "rows": [{"n": "7"}], "at": "c0"}),
    )
    .await;
    assert_eq!(within(cell).await.unwrap().unwrap(), Some(7));
    ws.recv().await;

    let failing = tokio::spawn(client.query(SQL).fetch::<User>());
    ws.recv().await;
    ws.send(json!({"type": "error", "id": "s4", "code": "SYNTAX_ERROR", "message": "1:1"}))
        .await;
    assert_eq!(
        within(failing).await.unwrap().unwrap_err().code,
        "SYNTAX_ERROR"
    );
}

#[tokio::test]
async fn history_then_each_new_row_with_none_in_both() {
    let mut server = FakeServer::start().await;
    let client = server.client();
    let started = tokio::spawn(client.query(SQL).history::<User>());
    // a complete snapshot: everything in Kafka when subscribing
    let mut ws = server.next().await;
    assert_eq!(
        ws.recv().await,
        json!({"type": "subscribe", "id": "s1", "sql": SQL, "complete": true})
    );
    ws.send(snapshot("s1", json!([{"user_id": "a"}, {"user_id": "b"}])))
        .await;
    ws.send(diff("s1", "c1", json!([insert("c")]))).await;
    let (history, mut new) = within(started).await.unwrap().unwrap();
    assert_eq!(*history, [Arc::new(user("a")), Arc::new(user("b"))]);
    assert_eq!(*within(new.next()).await.unwrap().unwrap(), user("c"));

    // a reconnect the server can't resume: the history isn't new again, what came meanwhile is
    ws.close().await;
    let mut ws = server.next().await;
    let again = ws.recv().await;
    assert_eq!(
        (&again["since"], &again["complete"]),
        (&json!("c1"), &json!(true))
    );
    ws.send(snapshot(
        "s1",
        json!([{"user_id": "a"}, {"user_id": "b"}, {"user_id": "c"}, {"user_id": "d"}]),
    ))
    .await;
    assert_eq!(*within(new.next()).await.unwrap().unwrap(), user("d"));
}

#[tokio::test]
async fn inserts_are_sent_once_connected_and_answered_by_id() {
    let mut server = FakeServer::start().await;
    let mut opts = ClientOptions::new(&server.url).user("alice");
    opts.min_backoff = Duration::from_millis(5);
    let client = LiveClient::new(opts);
    // no subscription: the insert opens the connection, after `hello`
    let first = tokio::spawn({
        let client = client.clone();
        async move { client.insert("messages", [json!({"body": "hi"})]).await }
    });
    let mut ws = server.next().await;
    assert_eq!(ws.recv().await, json!({"type": "hello", "user": "alice"}));
    let sent = ws.recv().await;
    assert_eq!(
        sent,
        json!({"type": "insert", "id": "w1", "table": "messages", "rows": [{"body": "hi"}]})
    );
    ws.send(json!({"type": "ack", "id": "w1", "offsets": [[2, 40]]}))
        .await;
    let ack = within(first).await.unwrap().unwrap();
    assert_eq!(ack.offsets, [(2, 40)]);

    // an error is that insert's
    #[derive(serde::Serialize)]
    struct Message {
        body: &'static str,
    }
    let second = tokio::spawn({
        let client = client.clone();
        async move { client.insert("messages", [Message { body: "x" }]).await }
    });
    let id = ws.recv().await["id"].clone();
    ws.send(json!({"type": "error", "id": id, "code": "INVALID_INSERT", "message": "no"}))
        .await;
    let e = within(second).await.unwrap().unwrap_err();
    assert_eq!(
        (e.code.as_str(), e.message.as_str()),
        ("INVALID_INSERT", "no")
    );

    // not a JSON object: refused before sending
    let e = client.insert("messages", [1]).await.unwrap_err();
    assert_eq!(e.code, "CLIENT_ERROR");
}

#[tokio::test]
async fn an_insert_unanswered_when_the_connection_drops_is_disconnected() {
    let mut server = FakeServer::start().await;
    let client = server.client();
    let pending = tokio::spawn({
        let client = client.clone();
        async move { client.insert("messages", [json!({"body": "hi"})]).await }
    });
    let mut ws = server.next().await;
    assert_eq!(ws.recv().await["type"], "insert", "no user: no hello");
    ws.close().await;
    let e = within(pending).await.unwrap().unwrap_err();
    assert_eq!(e.code, "DISCONNECTED");
    // closed: queued inserts fail
    client.close().await;
    let e = client.insert("messages", [json!({})]).await.unwrap_err();
    assert_eq!(e.code, "CLIENT_ERROR");
}

#[tokio::test]
async fn hello_comes_before_resubscribing() {
    let mut server = FakeServer::start().await;
    let mut opts = ClientOptions::new(&server.url).user("bob");
    opts.min_backoff = Duration::from_millis(5);
    let client = LiveClient::new(opts);
    let _sub = client.subscribe::<Row>(SQL);
    let mut ws = server.next().await;
    assert_eq!(ws.recv().await["type"], "hello");
    assert_eq!(ws.recv().await["type"], "subscribe");
    ws.close().await;
    let mut again = server.next().await;
    assert_eq!(again.recv().await, json!({"type": "hello", "user": "bob"}));
    assert_eq!(again.recv().await["type"], "subscribe");
}
