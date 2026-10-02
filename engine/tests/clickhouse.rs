//! Against a real ClickHouse (the dev shell's, or CHLIVE_CLICKHOUSE_URL); runs only with
//! CHLIVE_E2E=1. Uses its own small table instead of the seeded `default.events`.

use chlive_engine::diff::Diff;
use chlive_engine::history::ClickHouseHttp;
use chlive_engine::rules::HardcodedRules;
use chlive_engine::source::MemorySource;
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
    let engine = Engine::new(
        HardcodedRules {
            database: "chlive_engine_test".into(),
        },
        ClickHouseHttp::new(&url(), "default", ""),
        source,
    );
    let subscription = engine.subscribe("SELECT * FROM events").await.unwrap();
    assert_eq!(subscription.watermark.describe(), "{0: 0, 1: 0}");
    assert_eq!(
        subscription.history_sql,
        "SELECT ts, user_id, latency_ms FROM chlive_engine_test.events \
         WHERE toInt64(_offset) <= multiIf(_partition = 0, 0, _partition = 1, 0, -1)"
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
        subscription.plan.history.sql(None),
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

    exec("DROP DATABASE chlive_engine_test").await;
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
    // In ClickHouse, each event is at partition 0 and its offset in the topic.
    let with_offsets = |events: &[&str]| -> String {
        events
            .iter()
            .enumerate()
            .map(|(offset, e)| format!("{{\"_partition\":0,\"_offset\":{offset},{}\n", &e[1..]))
            .collect()
    };
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
             coupon Nullable(String), _partition UInt64, _offset UInt64) \
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
        let engine = Engine::new(
            HardcodedRules {
                database: database.into(),
            },
            ClickHouseHttp::new(&url(), "default", ""),
            source.clone(),
        );
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
             _partition UInt64, _offset UInt64) ENGINE = MergeTree ORDER BY tuple()"
        ))
        .await;
        if in_clickhouse > 0 {
            let rows: String = EVENTS[..in_clickhouse]
                .iter()
                .enumerate()
                .map(|(o, e)| format!("{{\"_partition\":0,\"_offset\":{o},{}\n", &e[1..]))
                .collect();
            exec(&format!(
                "INSERT INTO {database}.events FORMAT JSONEachRow\n{rows}"
            ))
            .await;
        }
        let engine = Engine::new(
            HardcodedRules {
                database: database.into(),
            },
            ClickHouseHttp::new(&url(), "default", ""),
            source.clone(),
        );
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
            let Diff::Insert { row, .. } = d;
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
            let mut list: Vec<String> = Vec::new();
            for it in &iterations {
                for d in &it.diffs {
                    let Diff::Insert { row, index } = d;
                    let name = row["name"].as_str().unwrap().to_string();
                    match (it.origin == Origin::History, index) {
                        (true, None) => list.push(name),
                        (false, Some(i)) => list.insert(*i, name),
                        other => panic!("{database}: {sql}: unexpected insert {other:?}"),
                    }
                }
            }
            lists.push((database, list));
        }
        let key = |name: &String| -> String {
            keys.iter()
                .map(|k| by_name[name][*k].to_string())
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
        let (_, reference) = &lists[0];
        for (database, list) in &lists[1..] {
            assert_eq!(
                groups(list),
                groups(reference),
                "{database} disagrees with ClickHouse on: {sql}\n  got      {list:?}\n  expected {reference:?}"
            );
        }
        eprintln!("{sql}: {}", reference.join(" "));
    }
    for (database, _) in &engines {
        exec(&format!("DROP DATABASE {database}")).await;
    }
}
