//! Against a real ClickHouse (either dev shell's, via CHLIVE_CLICKHOUSE_URL); runs only with
//! CHLIVE_E2E=1. Uses its own small table instead of the seeded `default.events`.

use chlive_engine::diff::Diff;
use chlive_engine::history::ClickHouseHttp;
use chlive_engine::rules::HardcodedRules;
use chlive_engine::stream::JsonlTopics;
use chlive_engine::{Engine, Origin};
use futures::StreamExt;
use serde_json::json;

fn url() -> String {
    std::env::var("CHLIVE_CLICKHOUSE_URL").unwrap_or_else(|_| "http://127.0.0.1:18123".into())
}

async fn exec(sql: &str) {
    let resp = reqwest::Client::new()
        .post(url())
        .header("X-ClickHouse-User", "default")
        .body(sql.to_string())
        .send()
        .await
        .expect("ClickHouse reachable (is `dnvr up` running?)");
    assert!(
        resp.status().is_success(),
        "{sql}: {}",
        resp.text().await.unwrap()
    );
}

#[tokio::test]
async fn select_star_from_events_against_clickhouse() {
    if std::env::var_os("CHLIVE_E2E").is_none() {
        eprintln!("skipping: set CHLIVE_E2E=1 (requires `dnvr up`)");
        return;
    }
    exec("CREATE DATABASE IF NOT EXISTS chlive_engine_test").await;
    exec("DROP TABLE IF EXISTS chlive_engine_test.events").await;
    exec("CREATE TABLE chlive_engine_test.events (ts DateTime64(3), user_id String, latency_ms UInt32, _partition UInt64, _offset UInt64) ENGINE = MergeTree ORDER BY ts").await;
    exec("INSERT INTO chlive_engine_test.events VALUES ('2026-10-01 12:00:00.000', 'u1', 10, 0, 0), ('2026-10-01 12:00:01.000', 'u2', 20, 1, 0)").await;

    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("events.jsonl"),
        "{\"ts\": \"2026-10-01 12:00:02.000\", \"user_id\": \"u3\", \"latency_ms\": 30}\n",
    )
    .unwrap();
    let engine = Engine::new(
        HardcodedRules {
            database: "chlive_engine_test".into(),
        },
        ClickHouseHttp::new(&url(), "default", ""),
        JsonlTopics::new(dir.path()),
    );
    let subscription = engine.subscribe("SELECT * FROM events").await.unwrap();
    assert_eq!(
        subscription.plan.history.sql,
        "SELECT ts, user_id, latency_ms FROM chlive_engine_test.events"
    );

    let its: Vec<_> = subscription.map(|i| i.unwrap()).collect().await;
    assert_eq!(its.len(), 2);
    assert_eq!(its[0].origin, Origin::History);
    let mut history: Vec<String> = its[0].diffs.iter().map(|d| d.to_string()).collect();
    history.sort();
    assert_eq!(
        history,
        [
            r#"+ {"ts":"2026-10-01 12:00:00.000","user_id":"u1","latency_ms":10}"#,
            r#"+ {"ts":"2026-10-01 12:00:01.000","user_id":"u2","latency_ms":20}"#,
        ]
    );
    assert_eq!(
        its[1].diffs,
        [Diff::Insert(
            json!({"ts": "2026-10-01 12:00:02.000", "user_id": "u3", "latency_ms": 30})
                .as_object()
                .unwrap()
                .clone()
        )]
    );

    // a column list: only those columns, in that order, on both sides
    let subscription = engine
        .subscribe("select latency_ms, user_id from events")
        .await
        .unwrap();
    assert_eq!(
        subscription.plan.history.sql,
        "SELECT latency_ms, user_id FROM chlive_engine_test.events"
    );
    let lines: Vec<String> = subscription.map(|i| i.unwrap().to_string()).collect().await;
    let mut history: Vec<&str> = lines[0].lines().collect();
    history.sort();
    assert_eq!(
        history,
        [
            r#"+ {"latency_ms":10,"user_id":"u1"}"#,
            r#"+ {"latency_ms":20,"user_id":"u2"}"#,
            "history: 2 rows",
        ]
    );
    assert_eq!(lines[1], r#"events:1 + {"latency_ms":30,"user_id":"u3"}"#);

    exec("DROP DATABASE chlive_engine_test").await;
}
