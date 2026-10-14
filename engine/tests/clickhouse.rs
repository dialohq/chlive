//! Against a real ClickHouse (CHLIVE_CLICKHOUSE_URL, which the `e2e`
//! script sets); runs only with CHLIVE_E2E=1. Uses its own small table instead of the seeded
//! `default.events`.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use chlive_engine::diff::Diff;
use chlive_engine::history::ClickHouseHttp;
use chlive_engine::rules::ConfiguredRules;
use chlive_engine::snapshot::{
    ClickHouseCloner, Paused, Pauser, SnapshotConfig, SnapshotError, SnapshotManager, SnapshotTable,
};
use chlive_engine::source::MemorySource;
use chlive_engine::watermark::Watermark;
use chlive_engine::{Engine, Origin};
use futures::StreamExt;
use serde_json::json;

fn url() -> String {
    std::env::var("CHLIVE_CLICKHOUSE_URL").unwrap_or_else(|_| "http://127.0.0.1:18123".into())
}

/// Stands in for the pause server: ingestion is "paused" with the offsets the test put in
/// ClickHouse. The clone is real.
struct FixedPause(Watermark, std::collections::HashMap<String, Watermark>);

#[async_trait]
impl Pauser for FixedPause {
    async fn pause(&self, table: &SnapshotTable) -> Result<Paused, SnapshotError> {
        Ok(Paused {
            token: "t".into(),
            ttl: Duration::from_secs(60),
            watermark: self.1.get(&table.table).unwrap_or(&self.0).clone(),
        })
    }
    async fn resume(&self, _: &str) -> Result<(), SnapshotError> {
        Ok(())
    }
    async fn renew(&self, _: &str) -> Result<(), SnapshotError> {
        Ok(())
    }
}

/// An engine over `database`, whose tables hold the offsets in `watermark`.
fn engine_at(database: String, source: MemorySource, watermark: Watermark) -> Engine {
    engine_with(database, source, watermark, Default::default())
}

/// Like [`engine_at`], with other watermarks for some tables.
fn engine_with(
    database: String,
    source: MemorySource,
    watermark: Watermark,
    tables: std::collections::HashMap<String, Watermark>,
) -> Engine {
    engine_custom(
        ConfiguredRules::expose_all(&database),
        None,
        source,
        watermark,
        tables,
    )
}

/// Any table mapping, and snapshots in `snapshot_database` (or next to their tables).
fn engine_custom(
    rules: ConfiguredRules,
    snapshot_database: Option<&str>,
    source: MemorySource,
    watermark: Watermark,
    tables: std::collections::HashMap<String, Watermark>,
) -> Engine {
    let snapshots = SnapshotManager::start(
        Arc::new(FixedPause(watermark, tables)),
        Arc::new(ClickHouseCloner::new(&url(), "default", "")),
        SnapshotConfig {
            refresh: Duration::from_secs(3600),
            idle: Duration::from_secs(3600),
            sweep: false,
            database: snapshot_database.map(String::from),
        },
    );
    Engine::new(
        rules,
        ClickHouseHttp::new(&url(), "default", ""),
        source,
        snapshots,
    )
    .with_clickhouse(&url(), "default", "")
}

/// ClickHouse holds partition 0's first `n` events.
fn upto(n: usize) -> Watermark {
    Watermark([(0, n as i64 - 1)].into())
}

async fn query(sql: &str) -> String {
    let resp = reqwest::Client::new()
        .post(url())
        .header("X-ClickHouse-User", "default")
        .body(sql.to_string())
        .send()
        .await
        .expect("ClickHouse reachable (is `dnvr up` running?)");
    assert!(resp.status().is_success(), "{sql}");
    resp.text().await.unwrap()
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
    exec("CREATE TABLE chlive_engine_test.events (ts DateTime64(3), user_id String, latency_ms UInt32) ENGINE = MergeTree ORDER BY ts").await;
    exec("INSERT INTO chlive_engine_test.events VALUES ('2026-10-01 12:00:00.000', 'u1', 10), ('2026-10-01 12:00:01.000', 'u2', 20)").await;

    // the topic holds the two rows ClickHouse has (partitions 0 and 1, offset 0 each), then u3
    let mut source = MemorySource::new();
    source
        .push(
            "events",
            0,
            r#"{"ts": "2026-10-01 12:00:00.000", "user_id": "u1", "latency_ms": 10}"#,
        )
        .push(
            "events",
            1,
            r#"{"ts": "2026-10-01 12:00:01.000", "user_id": "u2", "latency_ms": 20}"#,
        )
        .push(
            "events",
            0,
            r#"{"ts": "2026-10-01 12:00:02.000", "user_id": "u3", "latency_ms": 30}"#,
        );
    let engine = engine_at(
        "chlive_engine_test".into(),
        source,
        Watermark([(0, 0), (1, 0)].into()),
    );
    let subscription = engine.subscribe("SELECT * FROM events").await.unwrap();
    assert_eq!(subscription.watermark.describe(), "{0: 0, 1: 0}");
    // history reads a clone of the table, made by CREATE … AS + ATTACH PARTITION ALL
    assert!(
        subscription.history_sql.starts_with(
            "SELECT ts, user_id, latency_ms FROM chlive_engine_test.__chlive_snap_events_"
        ),
        "{}",
        subscription.history_sql
    );
    let clones = "SELECT count() FROM system.tables WHERE database = 'chlive_engine_test' \
                  AND startsWith(name, '__chlive_snap_')";
    assert_eq!(query(clones).await.trim(), "1");

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
        [Diff::insert(
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
        subscription.plan.history.sql("chlive_engine_test.events"),
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
    assert_eq!(lines[1], r#"events/0:1 + {"latency_ms":30,"user_id":"u3"}"#);

    // the clone goes when the engine stops (no subscription reads it anymore)
    engine.shutdown().await;
    assert_eq!(query(clones).await.trim(), "0");

    exec("DROP DATABASE chlive_engine_test").await;
}

/// Snapshots in their own database, and a table mapped under other names (ClickHouse table,
/// topic, columns, JSON keys): history reads the clone there by the ClickHouse names, the
/// stream decodes by the JSON keys, and both come out under the dialect names.
#[tokio::test]
async fn snapshot_database_and_table_mapping_against_clickhouse() {
    if std::env::var_os("CHLIVE_E2E").is_none() {
        eprintln!("skipping: set CHLIVE_E2E=1 (requires `dnvr up`)");
        return;
    }
    // fresh, whatever an earlier run left
    for db in ["chlive_engine_map_test", "chlive_engine_snaps_test"] {
        exec(&format!("DROP DATABASE IF EXISTS {db} SYNC")).await;
        exec(&format!("CREATE DATABASE {db}")).await;
    }
    exec("CREATE TABLE chlive_engine_map_test.events_v2 (ts DateTime64(3), user_id String, latency_ms UInt32, note String) ENGINE = MergeTree ORDER BY ts").await;
    exec("INSERT INTO chlive_engine_map_test.events_v2 VALUES ('2026-10-01 12:00:00.000', 'u1', 10, 'x'), ('2026-10-01 12:00:01.000', 'u2', 20, 'y')").await;

    let mut source = MemorySource::new();
    source
        .push(
            "prod.events",
            0,
            r#"{"ts": "2026-10-01 12:00:00.000", "uid": "u1", "latency_ms": 10, "note": "x"}"#,
        )
        .push(
            "prod.events",
            0,
            r#"{"ts": "2026-10-01 12:00:01.000", "uid": "u2", "latency_ms": 20, "note": "y"}"#,
        )
        .push(
            "prod.events",
            0,
            r#"{"ts": "2026-10-01 12:00:02.000", "uid": "u3", "latency_ms": 30, "note": "z"}"#,
        );
    let rules = ConfiguredRules::new(
        serde_json::from_value(json!({
            "events": {
                "clickhouse": "chlive_engine_map_test.events_v2",
                "topic": "prod.events",
                "hidden_columns": ["note"],
                "columns": { "who": { "clickhouse": "user_id", "json": "uid" } },
            },
        }))
        .unwrap(),
    )
    .unwrap();
    let engine = engine_custom(
        rules,
        Some("chlive_engine_snaps_test"),
        source,
        Watermark([(0, 1)].into()),
        Default::default(),
    );
    let subscription = engine
        .subscribe("select who, latency_ms from events where latency_ms >= 20")
        .await
        .unwrap();
    assert!(
        subscription.history_sql.starts_with(
            "SELECT user_id AS who, latency_ms FROM chlive_engine_snaps_test.__chlive_snap_events_v2_"
        ),
        "{}",
        subscription.history_sql
    );
    let clones = |db: &str| {
        format!(
            "SELECT count() FROM system.tables WHERE database = '{db}' \
             AND startsWith(name, '__chlive_snap_')"
        )
    };
    assert_eq!(query(&clones("chlive_engine_snaps_test")).await.trim(), "1");
    assert_eq!(query(&clones("chlive_engine_map_test")).await.trim(), "0");

    let lines: Vec<String> = subscription.map(|i| i.unwrap().to_string()).collect().await;
    assert_eq!(
        lines,
        [
            "+ {\"who\":\"u2\",\"latency_ms\":20}\nhistory: 1 rows",
            r#"prod.events/0:2 + {"who":"u3","latency_ms":30}"#,
        ]
    );
    // unlisted tables are unknown
    let err = engine.subscribe("select * from other").await.err().unwrap();
    assert!(
        err.to_string().contains("not in the server's config"),
        "{err}"
    );

    engine.shutdown().await;
    assert_eq!(query(&clones("chlive_engine_snaps_test")).await.trim(), "0");
    exec("DROP DATABASE chlive_engine_map_test").await;
    exec("DROP DATABASE chlive_engine_snaps_test").await;
}

/// The same events go into ClickHouse (which fills omitted keys with its defaults / NULL) and
/// into the stream (which decodes them by column type). For every predicate, these give the same
/// rows: all events from history; all from the stream; and the first half from history, the
/// rest from the stream, split at the watermark. So our evaluator agrees with ClickHouse's, and
/// the boundary neither loses nor repeats an event.
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

    let events: Vec<&str> = EVENTS.lines().collect();
    // ClickHouse holds the first events of partition 0 (offsets 0, 1, …)
    let with_offsets =
        |events: &[&str]| -> String { events.iter().map(|e| format!("{e}\n")).collect() };
    let mut source = MemorySource::new();
    for e in &events {
        source.push("events", 0, *e);
    }
    let mut engines = Vec::new();
    for (database, in_clickhouse) in [
        ("chlive_engine_agree_history", events.len()),
        ("chlive_engine_agree_stream", 0),
        ("chlive_engine_agree_split", events.len() / 2),
    ] {
        exec(&format!("DROP DATABASE IF EXISTS {database}")).await;
        exec(&format!("CREATE DATABASE {database}")).await;
        exec(&format!(
            "CREATE TABLE {database}.events (ts DateTime64(3), user_id String, \
             country LowCardinality(String), latency_ms UInt32, amount Float64, \
             coupon Nullable(String)) \
             ENGINE = MergeTree ORDER BY tuple()"
        ))
        .await;
        if in_clickhouse > 0 {
            exec(&format!(
                "INSERT INTO {database}.events FORMAT JSONEachRow\n{}",
                with_offsets(&events[..in_clickhouse])
            ))
            .await;
        }
        let engine = engine_at(database.into(), source.clone(), upto(in_clickhouse));
        engines.push((database, in_clickhouse, engine));
    }

    let mut matched_any = 0;
    for predicate in PREDICATES {
        let sql = format!("select * from events where {predicate}");
        let mut results = Vec::new();
        for (database, in_clickhouse, engine) in &engines {
            let iterations: Vec<_> = engine
                .subscribe(&sql)
                .await
                .unwrap()
                .map(|i| i.unwrap())
                .collect()
                .await;
            let from = |history: bool| -> Vec<String> {
                iterations
                    .iter()
                    .filter(|i| (i.origin == Origin::History) == history)
                    .flat_map(|i| i.diffs.iter().map(|d| d.to_string()))
                    .collect()
            };
            let (history, stream) = (from(true), from(false));
            if *in_clickhouse == events.len() {
                assert!(
                    stream.is_empty(),
                    "{database}: events repeated after history"
                );
            }
            if *in_clickhouse == 0 {
                assert!(history.is_empty(), "{database}: history should be empty");
            }
            let mut rows = [history, stream].concat();
            rows.sort();
            results.push((database, rows));
        }
        let (_, expected) = &results[0];
        for (database, rows) in &results[1..] {
            assert_eq!(
                rows, expected,
                "{database} disagrees with history on: {predicate}"
            );
        }
        if !expected.is_empty() {
            matched_any += 1;
        }
        eprintln!("{:>2} rows  {predicate}", expected.len());
    }
    assert!(
        matched_any > PREDICATES.len() / 2,
        "too many predicates matched nothing"
    );
    for (database, _, _) in &engines {
        exec(&format!("DROP DATABASE {database}")).await;
    }
}

/// ORDER BY: the same events in ClickHouse and/or the stream, as in the WHERE test. A client's
/// list (the snapshot, then each insert at its index) must have the same sequence of sort keys
/// as ClickHouse's own ordering of all events, with the same rows in every tie group (only
/// their order within a group may differ). Covers NULL, NaN, ±inf, -0, byte-wise strings,
/// DateTime precision, Bool, duplicates, DESC, NULLS FIRST/LAST, several keys, ALL, an
/// unselected column and WHERE.
#[tokio::test]
async fn history_and_stream_agree_on_order_by() {
    if std::env::var_os("CHLIVE_E2E").is_none() {
        eprintln!("skipping: set CHLIVE_E2E=1 (requires `dnvr up`)");
        return;
    }
    // `name` is unique, so each row's values (and sort key) can be looked up by it.
    const EVENTS: &[&str] = &[
        r#"{"ts":"2026-10-01 12:00:00.000","name":"n1","score":1.5,"n":3,"flag":true,"label":"b"}"#,
        r#"{"ts":"2026-10-01 12:00:01.250","name":"n2","score":null,"n":-2,"flag":false,"label":null}"#,
        r#"{"ts":"2026-10-01 12:00:02","name":"n3","score":"nan","n":3,"flag":true,"label":""}"#,
        r#"{"ts":"2026-10-01 12:00:00.000","name":"n4","score":-0.0,"n":0,"flag":false,"label":"B"}"#,
        r#"{"ts":"2026-10-01 12:00:04.500","name":"n5","score":0,"n":3,"flag":false,"label":"a"}"#,
        r#"{"ts":"2026-10-01 12:00:05.000","name":"n6","score":"inf","n":10,"flag":true,"label":"é"}"#,
        r#"{"ts":"2026-10-01 12:00:06.000","name":"n7","score":-1e3,"n":-2,"flag":false,"label":"b"}"#,
        r#"{"ts":"2026-10-01 12:00:07.000","name":"n8","score":1.5,"n":7,"flag":true,"label":null}"#,
        r#"{"ts":"2026-10-01 12:00:08.000","name":"n9","n":3,"flag":true}"#,
        r#"{"ts":"2026-10-01 12:00:09.000","name":"n10","score":"-inf","n":0,"flag":false,"label":"Z"}"#,
        r#"{"ts":"2026-10-01 12:00:00.000","name":"n11","score":1.5,"n":3,"flag":true,"label":"b"}"#,
        r#"{"ts":"2026-09-30 23:59:59.999","name":"n12","score":2,"n":100,"flag":false,"label":"aa"}"#,
    ];
    // (query, its ORDER BY columns)
    const CASES: &[(&str, &[&str])] = &[
        ("select * from events order by score", &["score"]),
        ("select * from events order by score desc", &["score"]),
        (
            "select * from events order by score nulls first",
            &["score"],
        ),
        (
            "select * from events order by score desc nulls first",
            &["score"],
        ),
        (
            "select * from events order by n, score desc",
            &["n", "score"],
        ),
        ("select * from events order by label", &["label"]),
        (
            "select * from events order by label desc nulls first",
            &["label"],
        ),
        ("select * from events order by flag, n desc", &["flag", "n"]),
        ("select * from events order by ts", &["ts"]),
        // aliases: the ORDER BY column is read from history rows under its alias
        (
            "select name, score as s, n as count from events order by score desc, n limit 5",
            &["score", "n"],
        ),
        (
            "select * from events order by ts desc, label",
            &["ts", "label"],
        ),
        (
            "select * from events where n = 3 order by score",
            &["score"],
        ),
        (
            "select flag, n, name from events order by all desc",
            &["flag", "n", "name"],
        ),
        (
            "select name from events order by score desc, n",
            &["score", "n"],
        ),
        (
            "select name, ts from events where flag order by n nulls first, ts desc",
            &["n", "ts"],
        ),
        // LIMIT: the top N
        ("select * from events order by score limit 4", &["score"]),
        (
            "select * from events order by n desc, score limit 5",
            &["n", "score"],
        ),
        (
            "select name from events order by score desc nulls first limit 3",
            &["score"],
        ),
        (
            "select * from events where flag order by ts desc limit 2",
            &["ts"],
        ),
        ("select * from events order by label limit 6", &["label"]),
        ("select * from events order by score limit 0", &["score"]),
        // OFFSET: the rows after the top M (with ties at both ends)
        (
            "select * from events order by score limit 4 offset 2",
            &["score"],
        ),
        (
            "select * from events order by n desc, score limit 3 offset 4",
            &["n", "score"],
        ),
        (
            "select name from events order by score desc nulls first offset 5",
            &["score"],
        ),
        ("select * from events order by label limit 2, 3", &["label"]),
        (
            "select * from events where flag order by ts desc limit 2 offset 1",
            &["ts"],
        ),
        ("select * from events order by n offset 20", &["n"]),
    ];

    let mut source = MemorySource::new();
    for e in EVENTS {
        source.push("events", 0, *e);
    }
    let mut engines = Vec::new();
    for (database, in_clickhouse) in [
        ("chlive_engine_order_history", EVENTS.len()),
        ("chlive_engine_order_stream", 0),
        ("chlive_engine_order_split", EVENTS.len() / 2),
    ] {
        exec(&format!("DROP DATABASE IF EXISTS {database}")).await;
        exec(&format!("CREATE DATABASE {database}")).await;
        exec(&format!(
            "CREATE TABLE {database}.events (ts DateTime64(3), name String, \
             score Nullable(Float64), n Int32, flag Bool, label Nullable(String), \
            ) ENGINE = MergeTree ORDER BY tuple()"
        ))
        .await;
        if in_clickhouse > 0 {
            let rows: String = EVENTS[..in_clickhouse]
                .iter()
                .map(|e| format!("{e}\n"))
                .collect();
            exec(&format!(
                "INSERT INTO {database}.events FORMAT JSONEachRow\n{rows}"
            ))
            .await;
        }
        let engine = engine_at(database.into(), source.clone(), upto(in_clickhouse));
        engines.push((database, engine));
    }

    // Every row's values as rendered (by ClickHouse = by the engine), by name.
    let all: Vec<_> = engines[0]
        .1
        .subscribe("select * from events")
        .await
        .unwrap()
        .map(|i| i.unwrap())
        .collect()
        .await;
    let by_name: std::collections::HashMap<String, serde_json::Value> = all[0]
        .diffs
        .iter()
        .map(|d| {
            let Diff::Insert { row, .. } = d else {
                panic!("history is inserts")
            };
            (
                row["name"].as_str().unwrap().to_string(),
                serde_json::Value::Object(row.clone()),
            )
        })
        .collect();
    assert_eq!(by_name.len(), EVENTS.len());

    for (sql, keys) in CASES {
        // The client's list for each setup, as names; then its sort keys and tie groups.
        let mut lists = Vec::new();
        for (database, engine) in &engines {
            let sql = sql.to_string();
            let iterations: Vec<_> = engine
                .subscribe(&sql)
                .await
                .unwrap()
                .map(|i| i.unwrap())
                .collect()
                .await;
            let limit = limit_of(&sql);
            let mut rows = Vec::new();
            for it in &iterations {
                for d in &it.diffs {
                    if it.origin == Origin::History {
                        assert!(
                            matches!(d, Diff::Insert { index: None, .. }),
                            "{database}: {sql}: {d}"
                        );
                    }
                    d.apply(&mut rows)
                        .unwrap_or_else(|e| panic!("{database}: {sql}: {e}"));
                }
                if let Some(n) = limit {
                    assert!(
                        rows.len() <= n,
                        "{database}: {sql}: {} rows after {it}",
                        rows.len()
                    );
                }
            }
            let list: Vec<String> = rows
                .iter()
                .map(|r| r["name"].as_str().unwrap().to_string())
                .collect();
            lists.push((database, list));
        }
        let key = |name: &String| -> String {
            keys.iter()
                .map(|k| match &by_name[name][*k] {
                    // -0 ties with 0
                    serde_json::Value::Number(n) if n.as_f64() == Some(0.0) => "0".to_string(),
                    v => v.to_string(),
                })
                .collect::<Vec<_>>()
                .join(" | ")
        };
        let groups = |list: &Vec<String>| -> Vec<(String, Vec<String>)> {
            let mut out: Vec<(String, Vec<String>)> = Vec::new();
            for name in list {
                let k = key(name);
                match out.last_mut() {
                    Some((last, names)) if *last == k => names.push(name.clone()),
                    _ => out.push((k, vec![name.clone()])),
                }
            }
            for (_, names) in &mut out {
                names.sort();
            }
            out
        };
        // With LIMIT, which rows of the boundary tie group made it in is arbitrary (in ClickHouse
        // too): compare that group by key and size only. With OFFSET, the first one too.
        let comparable = |list: &Vec<String>| -> Vec<(String, Vec<String>)> {
            let mut g = groups(list);
            let (limit, offset) = window_of(sql);
            if limit.is_some() {
                if let Some((key, names)) = g.last_mut() {
                    *names = vec![format!("{key}: {} rows", names.len())];
                }
            }
            if offset.is_some() {
                if let Some((key, names)) = g.first_mut() {
                    *names = vec![format!("{key}: {} rows", names.len())];
                }
            }
            g
        };
        let (_, reference) = &lists[0];
        if let Some(n) = limit_of(sql) {
            assert!(reference.len() <= n);
        }
        for (database, list) in &lists[1..] {
            assert_eq!(list.len(), reference.len(), "{database}: {sql}: row count");
            assert_eq!(
                comparable(list),
                comparable(reference),
                "{database} disagrees with ClickHouse on: {sql}\n  got      {list:?}\n  expected {reference:?}"
            );
        }
        eprintln!("{sql}: {}", reference.join(" "));
    }
    // LIMIT without ORDER BY: a window of the latest rows. n = 3 matches n1 n3 n5 n9 n11.
    let sql = "select name, n from events where n = 3 limit 3";
    let mut lists = Vec::new();
    for (database, engine) in &engines {
        let iterations: Vec<_> = engine
            .subscribe(sql)
            .await
            .unwrap()
            .map(|i| i.unwrap())
            .collect()
            .await;
        let mut rows = Vec::new();
        for it in &iterations {
            for d in &it.diffs {
                d.apply(&mut rows).unwrap();
            }
            assert!(
                rows.len() <= 3,
                "{database}: {} rows after {it}",
                rows.len()
            );
        }
        assert!(rows.iter().all(|r| r["n"] == 3), "{database}: {rows:?}");
        let names: Vec<String> = rows
            .iter()
            .map(|r| r["name"].as_str().unwrap().to_string())
            .collect();
        eprintln!("{database}: {sql}: {}", names.join(" "));
        lists.push(names);
    }
    // history: any 3; stream-only: the latest 3; split: its 2 streamed ones last
    assert_eq!(lists[0].len(), 3);
    assert_eq!(lists[1], ["n5", "n9", "n11"]);
    assert_eq!(lists[2].len(), 3);
    assert_eq!(lists[2][1..], ["n9", "n11"]);

    // OFFSET without ORDER BY: the newest m rows are held back, the window is the n before them
    // (the arrival order: the history in ClickHouse's order, then the stream)
    for (sql, len, stream_only, split_tail) in [
        // n1 n3 n5 n9 n11 match; the split has n1 n3 n5 in ClickHouse
        (
            "select name, n from events where n = 3 limit 2 offset 1",
            2,
            &["n5", "n9"][..],
            &["n9"][..],
        ),
        (
            "select name, n from events where n = 3 offset 2",
            3,
            &["n1", "n3", "n5"][..],
            &[][..],
        ),
    ] {
        let mut lists = Vec::new();
        for (database, engine) in &engines {
            let iterations: Vec<_> = engine
                .subscribe(sql)
                .await
                .unwrap()
                .map(|i| i.unwrap())
                .collect()
                .await;
            let mut rows = Vec::new();
            for it in &iterations {
                for d in &it.diffs {
                    d.apply(&mut rows).unwrap();
                }
            }
            assert!(rows.iter().all(|r| r["n"] == 3), "{database}: {rows:?}");
            let names: Vec<String> = rows
                .iter()
                .map(|r| r["name"].as_str().unwrap().to_string())
                .collect();
            eprintln!("{database}: {sql}: {}", names.join(" "));
            assert_eq!(names.len(), len, "{database}: {sql}");
            lists.push(names);
        }
        assert_eq!(lists[1], stream_only, "{sql}");
        assert_eq!(
            lists[2][lists[2].len() - split_tail.len()..],
            *split_tail,
            "{sql}"
        );
        // the split's history rows (n1 n3 n5, in ClickHouse's order) come first, all of them
        // when nothing streamed is in the window
        if split_tail.is_empty() {
            let mut got = lists[2].clone();
            got.sort();
            assert_eq!(got, ["n1", "n3", "n5"], "{sql}");
        }
    }

    for (database, _) in &engines {
        exec(&format!("DROP DATABASE {database}")).await;
    }
}

fn limit_of(sql: &str) -> Option<usize> {
    window_of(sql).0
}

/// A case's LIMIT and OFFSET: `limit n [offset m]`, `limit m, n` or `offset m`.
fn window_of(sql: &str) -> (Option<usize>, Option<usize>) {
    let offset = sql
        .split_once(" offset ")
        .map(|(_, m)| m.trim().parse().unwrap());
    match sql.split_once(" limit ") {
        None => (None, offset),
        Some((_, rest)) => {
            let rest = rest.split(" offset ").next().unwrap().trim();
            match rest.split_once(", ") {
                Some((m, n)) => (Some(n.parse().unwrap()), Some(m.parse().unwrap())),
                None => (Some(rest.parse().unwrap()), offset),
            }
        }
    }
}

/// Resume: a client holds a result and its cursor; later it resumes with only what it missed.
/// Its list (cached rows + catch-up + stream) must equal a fresh subscription's, both when
/// ClickHouse has caught up past the client and when the client is ahead of ClickHouse.
#[tokio::test]
async fn resume_matches_a_fresh_subscription() {
    if std::env::var_os("CHLIVE_E2E").is_none() {
        eprintln!("skipping: set CHLIVE_E2E=1 (requires `dnvr up`)");
        return;
    }
    const EVENTS: &[&str] = &[
        r#"{"ts":"2026-10-01 12:00:00.000","name":"n1","score":1.5,"n":3,"flag":true}"#,
        r#"{"ts":"2026-10-01 12:00:01.250","name":"n2","score":null,"n":-2,"flag":false}"#,
        r#"{"ts":"2026-10-01 12:00:02","name":"n3","score":7,"n":3,"flag":true}"#,
        r#"{"ts":"2026-10-01 12:00:00.000","name":"n4","score":-0.0,"n":0,"flag":false}"#,
        r#"{"ts":"2026-10-01 12:00:04.500","name":"n5","score":0,"n":3,"flag":false}"#,
        r#"{"ts":"2026-10-01 12:00:05.000","name":"n6","score":9.5,"n":10,"flag":true}"#,
        r#"{"ts":"2026-10-01 12:00:06.000","name":"n7","score":-1e3,"n":-2,"flag":false}"#,
        r#"{"ts":"2026-10-01 12:00:07.000","name":"n8","score":1.5,"n":7,"flag":true}"#,
        r#"{"ts":"2026-10-01 12:00:08.000","name":"n9","n":3,"flag":true}"#,
        r#"{"ts":"2026-10-01 12:00:09.000","name":"n10","score":4,"n":0,"flag":false}"#,
        r#"{"ts":"2026-10-01 12:00:00.000","name":"n11","score":1.5,"n":3,"flag":true}"#,
        r#"{"ts":"2026-09-30 23:59:59.999","name":"n12","score":2,"n":100,"flag":false}"#,
    ];
    // (query, ORDER BY columns)
    const CASES: &[(&str, &[&str])] = &[
        ("select * from events", &[]),
        ("select name, n from events where n = 3 or flag", &[]),
        ("select * from events order by score desc", &["score"]),
        (
            "select name, score from events where n >= 0 order by n, score nulls first",
            &["n", "score"],
        ),
        ("select * from events order by score limit 3", &["score"]),
        ("select name from events order by n desc limit 4", &["n"]),
        ("select name, n from events where n = 3 limit 2", &[]),
    ];
    const DB: &str = "chlive_engine_resume";

    // ClickHouse holds the first `in_clickhouse` events, the topic the first `in_topic`
    async fn setup(in_clickhouse: usize, in_topic: usize) -> Engine {
        exec(&format!("DROP DATABASE IF EXISTS {DB}")).await;
        exec(&format!("CREATE DATABASE {DB}")).await;
        exec(&format!(
            "CREATE TABLE {DB}.events (ts DateTime64(3), name String, score Nullable(Float64), \
             n Int32, flag Bool) ENGINE = MergeTree ORDER BY tuple()"
        ))
        .await;
        if in_clickhouse > 0 {
            let rows: String = EVENTS[..in_clickhouse]
                .iter()
                .map(|e| format!("{e}\n"))
                .collect();
            exec(&format!(
                "INSERT INTO {DB}.events FORMAT JSONEachRow\n{rows}"
            ))
            .await;
        }
        let mut source = MemorySource::new();
        for e in &EVENTS[..in_topic] {
            source.push("events", 0, *e);
        }
        engine_at(DB.into(), source, upto(in_clickhouse))
    }

    /// Run a subscription to the end, applying it to `rows`; returns its last cursor token.
    async fn run(
        engine: &Engine,
        sql: &str,
        since: Option<&str>,
        rows: &mut Vec<chlive_engine::diff::Row>,
    ) -> (bool, String, usize) {
        let subscription = engine.subscribe_from(sql, since).await.unwrap();
        let resumed = subscription.resumed;
        let plan = subscription.plan.clone();
        let iterations: Vec<_> = subscription.map(|i| i.unwrap()).collect().await;
        if !resumed {
            rows.clear();
        }
        let mut changes = 0;
        for it in &iterations {
            if it.origin != Origin::History {
                changes += it.diffs.len();
            }
            for d in &it.diffs {
                d.apply(rows).unwrap();
            }
        }
        let at = iterations.last().unwrap().at.clone();
        (resumed, chlive_engine::cursor::encode(&plan, &at), changes)
    }

    fn names(rows: &[chlive_engine::diff::Row]) -> Vec<String> {
        rows.iter()
            .map(|r| r["name"].as_str().unwrap().to_string())
            .collect()
    }

    let full: std::collections::HashMap<String, serde_json::Value> = EVENTS
        .iter()
        .map(|e| {
            let v: serde_json::Value = serde_json::from_str(e).unwrap();
            (v["name"].as_str().unwrap().to_string(), v)
        })
        .collect();
    // tie groups by the ORDER BY columns (as written in the events; -0 ties with 0); the last
    // group only by key and size when there's a LIMIT
    let groups = |list: &[String], keys: &[&str], limited: bool| -> Vec<(String, Vec<String>)> {
        let mut out: Vec<(String, Vec<String>)> = Vec::new();
        for name in list {
            let k: Vec<String> = keys
                .iter()
                .map(|k| match &full[name][*k] {
                    serde_json::Value::Number(n) if n.as_f64() == Some(0.0) => "0".into(),
                    v => v.to_string(),
                })
                .collect();
            let k = k.join("|");
            match out.last_mut() {
                Some((last, ns)) if *last == k => ns.push(name.clone()),
                _ => out.push((k, vec![name.clone()])),
            }
        }
        for (_, ns) in &mut out {
            ns.sort();
        }
        if limited {
            if let Some((k, ns)) = out.last_mut() {
                *ns = vec![format!("{k}: {}", ns.len())];
            }
        }
        out
    };

    // (snapshot, topic) before; then the snapshot after, with the whole topic.
    // (4, 7, 4) and (0, 7, 0): the client is ahead of the snapshot, which (in the last case)
    // has nothing at all, so the whole state is rebuilt from Kafka. Otherwise the client is
    // behind the new snapshot: unordered queries still resume (from Kafka), stateful ones get a
    // full snapshot.
    for (k1, k2, later_in_clickhouse) in [(4, 7, 12), (4, 7, 4), (0, 5, 9), (6, 6, 12), (0, 7, 0)] {
        for (sql, keys) in CASES {
            let mut cached = Vec::new();
            let (_, token, _) = run(&setup(k1, k2).await, sql, None, &mut cached).await;
            let engine = setup(later_in_clickhouse, EVENTS.len()).await;
            let (resumed, _, changes) = run(&engine, sql, Some(&token), &mut cached).await;
            let stateful = !keys.is_empty() || sql.contains(" limit ");
            assert_eq!(
                resumed,
                !stateful || k2 >= later_in_clickhouse,
                "{sql}: resumed {resumed} [CH {k1}/topic {k2} -> CH {later_in_clickhouse}]"
            );
            let mut fresh = Vec::new();
            run(&engine, sql, None, &mut fresh).await;
            let (got, want) = (names(&cached), names(&fresh));
            let label = format!("{sql} [CH {k1}/topic {k2} -> CH {later_in_clickhouse}/topic 12]");
            let limit = sql
                .split_once(" limit ")
                .map(|(_, n)| n.parse::<usize>().unwrap());
            if keys.is_empty() && limit.is_some() {
                // a window: the latest rows, which a fresh history can't reproduce exactly
                assert_eq!(got.len(), want.len(), "{label}: {got:?} vs {want:?}");
            } else if keys.is_empty() {
                let (mut g, mut w) = (got.clone(), want.clone());
                g.sort();
                w.sort();
                assert_eq!(g, w, "{label}");
            } else {
                assert_eq!(
                    groups(&got, keys, limit.is_some()),
                    groups(&want, keys, limit.is_some()),
                    "{label}\n  resumed {got:?}\n  fresh   {want:?}"
                );
            }
            eprintln!(
                "{label}: {} changes on resume -> {}",
                changes,
                got.join(" ")
            );
        }
    }
    exec(&format!("DROP DATABASE {DB}")).await;
}

/// GROUP BY / aggregates (chDB): the result built from the snapshot plus inserts and updates
/// by id equals ClickHouse's own GROUP BY over all events, whether the events came through
/// history, the stream, or half and half.
#[tokio::test]
async fn aggregates_match_clickhouse() {
    if std::env::var_os("CHLIVE_E2E").is_none() {
        eprintln!("skipping: set CHLIVE_E2E=1 (requires `dnvr up`)");
        return;
    }
    let countries = ["PL", "DE", "US", "FR"];
    let events: Vec<String> = (0..40)
        .map(|i| {
            let score = if i % 5 == 0 {
                "null".to_string()
            } else {
                format!("{}", (i * 7 % 13) as f64 / 4.0)
            };
            format!(
                r#"{{"ts":"2026-10-0{} 12:00:{:02}.{:03}","name":"n{}","country":"{}","score":{score},"n":{},"amount":{},"flag":{}}}"#,
                1 + i % 3,
                i % 60,
                i * 37 % 1000,
                i,
                countries[i % 4],
                i as i64 - 15,
                (i * 13 % 50) as f64 * 1.25,
                i % 3 == 0
            )
        })
        .collect();
    const CASES: &[&str] = &[
        "select count() from events",
        "select country, count() as n from events group by country",
        "select country, flag, count(), sum(n), avg(amount) from events group by country, flag",
        "select country, min(ts), max(ts), min(name), max(name), min(n), max(amount) from events where n > -10 group by country",
        "select country, count(score), sum(score), avg(score), min(score), max(score) from events group by country",
        "select count() as c, sum(amount) from events group by country",
        "select sum(amount), avg(n), min(flag), max(country) from events where flag",
        "select flag from events group by flag",
        // no native implementation: these run in chDB (the rest natively)
        "select country, uniqExact(name), uniq(n), median(amount) from events group by country",
        "select country, any(country), anyLast(country), uniqExact(score) from events group by country",
        "select median(score), uniqExact(flag) from events",
        // groupArray (array_agg): every value (NULLs skipped), in no defined order
        "select country, groupArray(name), array_agg(score) from events group by country",
        "select groupArray(flag), ARRAY_AGG(ts), count() from events where n > 0",
        // ordered and limited: checked in order, tie groups compared as sets, and with LIMIT
        // the last tie group by key and size only (which of its rows make it is arbitrary)
        "select country, count() as n from events group by country order by n desc, country",
        "select country, count() as n, sum(amount) as s from events group by country order by n desc limit 2",
        "select country, flag, avg(amount) as a from events group by country, flag order by a desc nulls first limit 3",
        "select country, flag, max(score) from events group by country, flag order by max(score) desc nulls first, country, flag",
        "select country, flag from events group by country, flag order by all desc",
        "select country, uniqExact(name) as u from events where n > 0 group by country order by u, country limit 3",
        // by a hidden aggregate: the order isn't visible in the rows, only the groups
        "select country, count() from events group by country order by sum(amount)",
        // LIMIT alone: any (the first) groups
        "select country from events group by country limit 2",
        // OFFSET: groups come in and go out at both ends of the window
        "select country, count() as n, sum(amount) as s from events group by country order by n desc limit 2 offset 1",
        "select country, flag, avg(amount) as a from events group by country, flag order by a desc nulls first limit 3 offset 2",
        "select country, flag, count() as c from events group by country, flag order by c, country, flag offset 3",
        "select country, flag from events group by country, flag order by all desc limit 2, 3",
        "select country from events group by country limit 2 offset 1",
    ];
    let mut engines = Vec::new();
    for (database, in_clickhouse) in [
        ("chlive_engine_agg_history", events.len()),
        ("chlive_engine_agg_stream", 0),
        ("chlive_engine_agg_split", events.len() / 2),
    ] {
        exec(&format!("DROP DATABASE IF EXISTS {database}")).await;
        exec(&format!("CREATE DATABASE {database}")).await;
        exec(&format!(
            "CREATE TABLE {database}.events (ts DateTime64(3), name String, \
             country LowCardinality(String), score Nullable(Float64), n Int32, amount Float64, \
             flag Bool) ENGINE = MergeTree ORDER BY tuple()"
        ))
        .await;
        if in_clickhouse > 0 {
            let rows: String = events[..in_clickhouse]
                .iter()
                .map(|e| format!("{e}\n"))
                .collect();
            exec(&format!(
                "INSERT INTO {database}.events FORMAT JSONEachRow\n{rows}"
            ))
            .await;
        }
        let mut source = MemorySource::new();
        for e in &events {
            source.push("events", 0, e.as_str());
        }
        let engine = engine_at(database.into(), source, upto(in_clickhouse)).with_max_batch(7);
        engines.push((database, engine));
    }
    let direct = ClickHouseHttp::new(&url(), "default", "");
    // rows as comparable text: floats to 9 significant digits (ClickHouse adds in parallel)
    // a value as comparable text: floats to 9 significant digits (ClickHouse adds in
    // parallel), arrays (groupArray) as sorted lists (their order isn't defined)
    fn canon_value(v: &serde_json::Value) -> String {
        match (v, v.as_f64()) {
            (serde_json::Value::Array(items), _) => {
                let mut items: Vec<String> = items.iter().map(canon_value).collect();
                items.sort();
                format!("[{}]", items.join(","))
            }
            (_, Some(f)) if !v.is_i64() && !v.is_u64() => format!("{f:.9e}"),
            _ => v.to_string(),
        }
    }
    let canon_row = |r: &chlive_engine::diff::Row| -> String {
        r.iter()
            .map(|(k, v)| format!("{k}={}", canon_value(v)))
            .collect::<Vec<_>>()
            .join(",")
    };
    let canon = |rows: &[chlive_engine::diff::Row]| -> Vec<String> {
        let mut v: Vec<String> = rows.iter().map(canon_row).collect();
        v.sort();
        v
    };
    // the output columns an ordered case is ordered by (none: hidden, or unordered)
    let order_columns = |sql: &str| -> Vec<String> {
        let Some((_, order)) = sql.split_once(" order by ") else {
            return Vec::new();
        };
        let order = order
            .split(" limit ")
            .next()
            .unwrap()
            .split(" offset ")
            .next()
            .unwrap();
        if order.starts_with("sum(") {
            return Vec::new();
        }
        if order.starts_with("all") {
            return vec!["country".into(), "flag".into()];
        }
        order
            .split(", ")
            .map(|t| t.split(' ').next().unwrap().to_string())
            .collect()
    };
    // in order, ties as sets; with LIMIT the last tie group by key and size, with OFFSET the first
    let tie_groups =
        |rows: &[chlive_engine::diff::Row], keys: &[String], limited: bool, offset: bool| {
            let mut out: Vec<(String, Vec<String>)> = Vec::new();
            for r in rows {
                let key: Vec<String> = keys
                    .iter()
                    .map(|k| canon_row(&[(k.clone(), r[k].clone())].into_iter().collect()))
                    .collect();
                let key = key.join("|");
                match out.last_mut() {
                    Some((last, rs)) if *last == key => rs.push(canon_row(r)),
                    _ => out.push((key, vec![canon_row(r)])),
                }
            }
            for (_, rs) in &mut out {
                rs.sort();
            }
            if limited {
                if let Some((k, rs)) = out.last_mut() {
                    *rs = vec![format!("{k}: {} rows", rs.len())];
                }
            }
            if offset {
                if let Some((k, rs)) = out.first_mut() {
                    *rs = vec![format!("{k}: {} rows", rs.len())];
                }
            }
            out
        };
    for sql in CASES {
        use chlive_engine::history::HistorySource;
        let (limit, offset) = window_of(sql);
        let keys = order_columns(sql);
        let want = direct
            .fetch(&sql.replace("from events", "from chlive_engine_agg_history.events"))
            .await
            .unwrap();
        for (database, engine) in &engines {
            let iterations: Vec<_> = engine
                .subscribe(sql)
                .await
                .unwrap_or_else(|e| panic!("{sql}: {e}"))
                .map(|i| i.unwrap_or_else(|e| panic!("{database}: {sql}: {e}")))
                .collect()
                .await;
            let mut list = chlive_engine::diff::ResultList::default();
            let (mut inserts, mut updates) = (0, 0);
            for it in &iterations {
                for d in &it.diffs {
                    match d {
                        Diff::Insert { .. } => inserts += 1,
                        Diff::Update { .. } => updates += 1,
                        Diff::Delete { .. } => {
                            assert!(limit.is_some() || offset.is_some(), "{sql}: a delete")
                        }
                    }
                    list.apply(d)
                        .unwrap_or_else(|e| panic!("{database}: {sql}: {e}"));
                }
            }
            let mut ids: Vec<_> = list.ids.iter().map(|i| i.clone().expect("an id")).collect();
            ids.sort();
            ids.dedup();
            assert_eq!(
                ids.len(),
                list.rows.len(),
                "{database}: {sql}: ids not unique"
            );
            if !keys.is_empty() {
                assert_eq!(
                    tie_groups(&list.rows, &keys, limit.is_some(), offset.is_some()),
                    tie_groups(&want, &keys, limit.is_some(), offset.is_some()),
                    "{database}: {sql}\n  live   {:?}\n  direct {:?}",
                    canon(&list.rows),
                    canon(&want)
                );
            } else if limit.is_some() || offset.is_some() {
                // any n groups (after any m): as many, each one of the whole result's
                let all = direct
                    .fetch(
                        &sql.split(" limit ")
                            .next()
                            .unwrap()
                            .split(" offset ")
                            .next()
                            .unwrap()
                            .replace("from events", "from chlive_engine_agg_history.events"),
                    )
                    .await
                    .unwrap();
                let all = canon(&all);
                let after = all.len().saturating_sub(offset.unwrap_or(0));
                assert_eq!(
                    list.rows.len(),
                    limit.map_or(after, |n| n.min(after)),
                    "{database}: {sql}"
                );
                for r in canon(&list.rows) {
                    assert!(all.contains(&r), "{database}: {sql}: {r} isn't a group");
                }
            } else {
                assert_eq!(canon(&list.rows), canon(&want), "{database}: {sql}");
            }
            eprintln!(
                "{database:>28}: {sql}: {} groups, {inserts} inserts, {updates} updates",
                list.rows.len()
            );
        }
    }
    for (database, _) in &engines {
        exec(&format!("DROP DATABASE {database}")).await;
    } // the only test here using chDB: close it, or the test process crashes on exit
    chlive_engine::chdb::shutdown();
}

/// `IN (SELECT …)` over two tables, in every split of history and stream for both: the live
/// result (history, then events, then users entering S with back-fills) equals ClickHouse's
/// answer over all the data.
#[tokio::test]
async fn semi_joins_match_clickhouse() {
    if std::env::var_os("CHLIVE_E2E").is_none() {
        eprintln!("skipping: set CHLIVE_E2E=1 (requires `dnvr up`)");
        return;
    }
    let users = ["PL", "DE", "PL", "FR", "PL", "DE", "US", "PL"];
    let users: Vec<String> = users
        .iter()
        .enumerate()
        .map(|(i, c)| format!(r#"{{"id":"u{i}","country":"{c}"}}"#))
        .collect();
    let events: Vec<String> = (0..30)
        .map(|i| {
            format!(
                r#"{{"ts":"2026-10-01 12:00:{:02}.000","name":"e{i}","user_id":"u{}","amount":{}}}"#,
                i % 60,
                (i * 5) % 9, // u8 never has a user row: never in S
                (i * 17 % 60) as f64
            )
        })
        .collect();
    // (query, ORDER BY columns, the reference query when ClickHouse needs another form)
    const CASES: &[(&str, &[&str], Option<&str>)] = &[
        ("select name, user_id from events where user_id in (select id from users where country = 'PL')", &[], None),
        ("select name from events where amount > 10 and user_id in (select id from users where country != 'DE')", &[], None),
        ("select name, amount from events where user_id in (select id from users where country = 'PL') order by amount desc limit 4", &["amount"], None),
        ("select name, ts from events where user_id in (select id from users) order by ts", &["ts"], None),
        ("select name, amount from events where user_id in (select id from users where country = 'PL') order by amount desc limit 3 offset 2", &["amount"], None),
        // the same table on both sides
        ("select name from events where user_id in (select user_id from events where amount > 40)", &[], None),
        // correlated EXISTS, with aliases: the same semi-join (ClickHouse answers the IN form)
        (
            "select e.name, e.user_id from events e where exists (select 1 from users u where u.id = e.user_id and u.country = 'PL')",
            &[],
            Some("select name, user_id from events where user_id in (select id from users where country = 'PL')"),
        ),
        (
            "select e.name, e.amount from events e where exists (select * from users where country = 'PL' and id = e.user_id) order by e.amount desc limit 4",
            &["amount"],
            Some("select name, amount from events where user_id in (select id from users where country = 'PL') order by amount desc limit 4"),
        ),
        // uncorrelated EXISTS: u6 (US) arrives late in most setups; nobody is in ZZ
        ("select name from events where exists (select 1 from users where country = 'US')", &[], None),
        ("select name from events where exists (select 1 from users where country = 'ZZ')", &[], None),
        // an ordinary query with an alias and qualified names
        ("select e.name, e.amount from events e where e.amount > 30 order by e.amount, e.name", &["amount", "name"], None),
    ];
    let create = |db: &str| {
        [
            format!("DROP DATABASE IF EXISTS {db}"),
            format!("CREATE DATABASE {db}"),
            format!(
                "CREATE TABLE {db}.events (ts DateTime64(3), name String, user_id String, \
                 amount Float64) ENGINE = MergeTree ORDER BY tuple()"
            ),
            format!("CREATE TABLE {db}.users (id String, country String) ENGINE = MergeTree ORDER BY id"),
        ]
    };
    let insert = |db: &str, table: &str, rows: &[String]| {
        (!rows.is_empty()).then(|| {
            format!(
                "INSERT INTO {db}.{table} FORMAT JSONEachRow\n{}",
                rows.join("\n")
            )
        })
    };
    // the reference: everything in ClickHouse
    for sql in create("chlive_engine_semi_ref") {
        exec(&sql).await;
    }
    exec(&insert("chlive_engine_semi_ref", "events", &events).unwrap()).await;
    exec(&insert("chlive_engine_semi_ref", "users", &users).unwrap()).await;
    let direct = ClickHouseHttp::new(&url(), "default", "");
    let (ne, nu) = (events.len(), users.len());
    let setups = [
        ("chlive_engine_semi_history", ne, nu),
        ("chlive_engine_semi_stream", 0, 0),
        ("chlive_engine_semi_split", ne / 2, nu / 2),
        ("chlive_engine_semi_users_later", ne, 0),
        ("chlive_engine_semi_events_later", 0, nu),
    ];
    for (sql, keys, reference) in CASES {
        use chlive_engine::history::HistorySource;
        let want = direct
            .fetch(
                &reference
                    .unwrap_or(sql)
                    .replace("from events", "from chlive_engine_semi_ref.events")
                    .replace("from users", "from chlive_engine_semi_ref.users"),
            )
            .await
            .unwrap();
        let (limit, offset) = window_of(sql);
        for (db, in_events, in_users) in setups {
            for q in create(db) {
                exec(&q).await;
            }
            if let Some(q) = insert(db, "events", &events[..in_events]) {
                exec(&q).await;
            }
            if let Some(q) = insert(db, "users", &users[..in_users]) {
                exec(&q).await;
            }
            let mut source = MemorySource::new();
            for e in &events {
                source.push("events", 0, e.as_str());
            }
            for u in &users {
                source.push("users", 0, u.as_str());
            }
            let engine = engine_with(
                db.into(),
                source,
                upto(in_events),
                [("users".to_string(), upto(in_users))].into(),
            );
            let iterations: Vec<_> = engine
                .subscribe(sql)
                .await
                .unwrap_or_else(|e| panic!("{db}: {sql}: {e}"))
                .map(|i| i.unwrap_or_else(|e| panic!("{db}: {sql}: {e}")))
                .collect()
                .await;
            let mut rows = Vec::new();
            for it in &iterations {
                for d in &it.diffs {
                    d.apply(&mut rows)
                        .unwrap_or_else(|e| panic!("{db}: {sql}: {e}"));
                }
                if let Some(n) = limit {
                    assert!(rows.len() <= n, "{db}: {sql}: {} rows", rows.len());
                }
            }
            let canon = |rows: &[chlive_engine::diff::Row]| -> Vec<String> {
                let mut v: Vec<String> = rows
                    .iter()
                    .map(|r| serde_json::to_string(r).unwrap())
                    .collect();
                v.sort();
                v
            };
            if keys.is_empty() {
                assert_eq!(canon(&rows), canon(&want), "{db}: {sql}");
            } else {
                // in order; ties as sets; with LIMIT, the last tie group by key and size
                let groups = |rows: &[chlive_engine::diff::Row]| {
                    let mut out: Vec<(String, Vec<String>)> = Vec::new();
                    for r in rows {
                        let k: Vec<String> = keys.iter().map(|k| r[*k].to_string()).collect();
                        let k = k.join("|");
                        let text = serde_json::to_string(r).unwrap();
                        match out.last_mut() {
                            Some((last, rs)) if *last == k => rs.push(text),
                            _ => out.push((k, vec![text])),
                        }
                    }
                    for (_, rs) in &mut out {
                        rs.sort();
                    }
                    if limit.is_some() {
                        if let Some((k, rs)) = out.last_mut() {
                            *rs = vec![format!("{k}: {}", rs.len())];
                        }
                    }
                    if offset.is_some() {
                        if let Some((k, rs)) = out.first_mut() {
                            *rs = vec![format!("{k}: {}", rs.len())];
                        }
                    }
                    out
                };
                assert_eq!(groups(&rows), groups(&want), "{db}: {sql}");
            }
            engine.shutdown().await;
            eprintln!("{db:>32}: {sql}: {} rows", rows.len());
        }
    }
    for (db, _, _) in setups {
        exec(&format!("DROP DATABASE IF EXISTS {db}")).await;
    }
    exec("DROP DATABASE chlive_engine_semi_ref").await;
}
