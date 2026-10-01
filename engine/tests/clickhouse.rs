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

/// The same events go into ClickHouse (which fills omitted keys with its defaults / NULL) and
/// into the stream (which decodes them by column type). For every predicate, history and
/// stream must return the same rows: our evaluator agrees with ClickHouse's.
#[tokio::test]
async fn history_and_stream_agree_on_where() {
    if std::env::var_os("CHLIVE_E2E").is_none() {
        eprintln!("skipping: set CHLIVE_E2E=1 (requires `dnvr up`)");
        return;
    }
    const EVENTS: &str = concat!(
        r#"{"ts":"2026-10-01 12:00:00.000","user_id":"u1","country":"PL","latency_ms":12,"amount":0,"coupon":null}"#,
        "\n",
        r#"{"ts":"2026-10-01 12:00:01.250","user_id":"u2","country":"DE","latency_ms":100,"amount":100.5,"coupon":"SPRING"}"#,
        "\n",
        r#"{"ts":"2026-10-01 12:00:02","user_id":"u3","country":"US","latency_ms":101,"amount":99.99}"#,
        "\n",
        r#"{"user_id":"u4","country":"US","latency_ms":0}"#,
        "\n",
        r#"{"ts":"2026-10-01 12:00:04.999","country":"PL","latency_ms":4294967295,"amount":1e3,"coupon":""}"#,
        "\n",
        r#"{"ts":"2026-10-01 12:00:05.000","user_id":"u6","country":"GB","latency_ms":"250","amount":"12.5","coupon":"X"}"#,
        "\n",
        r#"{"ts":"2026-10-01 12:00:06.000","user_id":"it's","country":"FR","latency_ms":99,"amount":-5,"coupon":"X"}"#,
        "\n",
        r#"{"ts":"2026-10-01 12:00:07.000","user_id":"u8","country":"","latency_ms":10,"amount":0.1}"#,
        "\n",
        r#"{"ts":"2026-09-30 23:59:59.999","user_id":"u9","country":"JP","latency_ms":1530,"amount":42.5,"coupon":"SPRING"}"#,
        "\n",
        r#"{"ts":"2026-10-01 12:00:09.000","user_id":"u10","country":"DE","latency_ms":100,"amount":100,"coupon":null}"#,
        "\n",
    );
    const PREDICATES: &[&str] = &[
        "latency_ms > 100",
        "latency_ms >= 100 and country != 'US'",
        "country in ('PL', 'DE') or amount > 50",
        "not (country = 'US')",
        "coupon is null",
        "coupon is not null and coupon != ''",
        "coupon = 'X'",
        "not (coupon = 'X')",
        "coupon != 'X' or latency_ms = 0",
        "ts >= '2026-10-01 12:00:02'",
        "ts < '2026-10-01 12:00:01.250'",
        "ts >= '2026-10-01'",
        "amount = 0",
        "amount > 99.995",
        "latency_ms > 99.5",
        "latency_ms = 4294967295",
        "latency_ms > -1",
        "latency_ms not in (0, 10, 100)",
        "user_id = ''",
        "user_id = 'it''s'",
        "country = ''",
        "amount < 0 or latency_ms in (12, 99)",
    ];

    exec("CREATE DATABASE IF NOT EXISTS chlive_engine_agree").await;
    exec("DROP TABLE IF EXISTS chlive_engine_agree.events").await;
    exec(
        "CREATE TABLE chlive_engine_agree.events (ts DateTime64(3), user_id String, \
         country LowCardinality(String), latency_ms UInt32, amount Float64, coupon Nullable(String), \
         _partition UInt64, _offset UInt64) ENGINE = MergeTree ORDER BY tuple()",
    )
    .await;
    exec(&format!(
        "INSERT INTO chlive_engine_agree.events FORMAT JSONEachRow\n{EVENTS}"
    ))
    .await;

    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("events.jsonl"), EVENTS).unwrap();
    let engine = Engine::new(
        HardcodedRules {
            database: "chlive_engine_agree".into(),
        },
        ClickHouseHttp::new(&url(), "default", ""),
        JsonlTopics::new(dir.path()),
    );

    let mut matched_any = 0;
    for predicate in PREDICATES {
        let sql = format!("select * from events where {predicate}");
        let iterations: Vec<_> = engine
            .subscribe(&sql)
            .await
            .unwrap()
            .map(|i| i.unwrap())
            .collect()
            .await;
        let rows = |history: bool| {
            let mut v: Vec<String> = iterations
                .iter()
                .filter(|i| (i.origin == Origin::History) == history)
                .flat_map(|i| i.diffs.iter().map(|d| d.to_string()))
                .collect();
            v.sort();
            v
        };
        let (history, stream) = (rows(true), rows(false));
        assert_eq!(
            history, stream,
            "history and stream disagree on: {predicate}"
        );
        if !history.is_empty() {
            matched_any += 1;
        }
        eprintln!("{:>2} rows  {predicate}", history.len());
    }
    assert!(
        matched_any > PREDICATES.len() / 2,
        "too many predicates matched nothing"
    );
    exec("DROP DATABASE chlive_engine_agree").await;
}
