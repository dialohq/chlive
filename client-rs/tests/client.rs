//! The client against a fake server: a WebSocket listener the test drives by hand.

use std::time::Duration;

use chlive_client::{Change, ClientOptions, Kind, LiveClient, Row, Status, Subscription, Update};
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

    // the connection drops: rows are kept, then a fresh snapshot (no resume) replaces them
    ws.close().await;
    let u = next(&mut sub).await;
    assert_eq!((u.kind, &u.status), (Kind::Disconnected, &Status::Loading));
    assert_eq!(users(&u), ["h1", "e1", "e2"]);
    let mut ws = server.next().await;
    assert_eq!(
        ws.recv().await,
        json!({"type": "subscribe", "id": "s1", "sql": SQL})
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
