//! The engine loop without ClickHouse or Kafka: fake history, events in memory.

use async_trait::async_trait;
use chlive_engine::diff::{Diff, Row};
use chlive_engine::history::{DescribedColumn, HistoryError, HistorySource};
use chlive_engine::rules::{ConfiguredRules, TopicId};
use chlive_engine::snapshot::{Lease, Snapshot, SnapshotError, SnapshotTable, Snapshots};
use chlive_engine::source::{Event, EventSource, MemorySource, SourceError, TopicStart};
use chlive_engine::table_engine::{Key, TableEngine};
use chlive_engine::watermark::Watermark;
use chlive_engine::{Engine, Iteration, Origin};
use futures::stream::BoxStream;
use futures::StreamExt;
use serde_json::json;
use std::sync::Arc;

/// History as read from a snapshot (the fake snapshots are the tables themselves).
struct FakeHistory;

/// `events`: the snapshot has offsets 0..=1 of partition 0 and nothing of partition 1.
/// `requests`: the snapshot has offset 0 of partition 0.
struct FakeSnapshots;

#[async_trait]
impl Snapshots for FakeSnapshots {
    async fn acquire(&self, table: &SnapshotTable) -> Result<Lease, SnapshotError> {
        let w = match table.table.as_str() {
            "events" => Watermark([(0, 1), (1, -1)].into()),
            _ => Watermark([(0, 0)].into()),
        };
        Ok(Arc::new(Snapshot::fixed(&table.database, &table.table, w)))
    }
}

#[async_trait]
impl HistorySource for FakeHistory {
    async fn describe(&self, table: &str) -> Result<Vec<DescribedColumn>, HistoryError> {
        let col = |n: &str, ty: &str| DescribedColumn {
            name: n.into(),
            ty: ty.into(),
        };
        match table {
            "default.events" => Ok(vec![col("ts", "String"), col("user_id", "String")]),
            "default.requests" => Ok(vec![col("user_id", "String"), col("latency_ms", "UInt32")]),
            "default.products" => Ok(vec![
                col("product_id", "String"),
                col("price", "UInt32"),
                col("version", "UInt64"),
                col("deleted", "UInt8"),
            ]),
            _ => Err(HistoryError::UnknownTable(table.into())),
        }
    }

    async fn table_engine(&self, _: &str, table: &str) -> Result<TableEngine, HistoryError> {
        Ok(match table {
            "products" => TableEngine::Replacing {
                ver: Some("version".into()),
                is_deleted: Some("deleted".into()),
                key: Key::from_system("product_id", ""),
            },
            _ => TableEngine::MergeTree,
        })
    }

    async fn fetch(&self, sql: &str) -> Result<Vec<Row>, HistoryError> {
        // ClickHouse returns exactly the selected columns, in order
        let events = String::from("SELECT ts, user_id FROM default.events");
        let events_users = String::from("SELECT user_id FROM default.events");
        let slow = String::from("SELECT user_id FROM default.requests WHERE latency_ms > 100");
        let slowest = String::from(
            "SELECT user_id, latency_ms AS __order_0 FROM default.requests \
             WHERE latency_ms > 100 ORDER BY latency_ms DESC NULLS LAST",
        );
        match sql {
            s if s == events => Ok(vec![
                row(json!({"ts": "h1", "user_id": "u1"})),
                row(json!({"ts": "h2", "user_id": "u2"})),
            ]),
            s if s == events_users => Ok(vec![
                row(json!({"user_id": "u1"})),
                row(json!({"user_id": "u2"})),
            ]),
            s if s == slow => Ok(vec![row(json!({"user_id": "slow-history"}))]),
            // ordered by an unselected column: ClickHouse returns it under a hidden alias
            s if s == slowest || s == format!("{slowest} LIMIT 3") => Ok(vec![
                row(json!({"user_id": "h900", "__order_0": 900})),
                row(json!({"user_id": "h500", "__order_0": 500})),
                row(json!({"user_id": "h500b", "__order_0": 500})),
            ]),
            s if s == format!("{slow} LIMIT 2") => Ok(vec![row(json!({"user_id": "h900"}))]),
            // LIMIT 2 OFFSET 1 without ORDER BY: the first 3, in ClickHouse's order
            s if s == format!("{slow} LIMIT 3") => Ok(vec![
                row(json!({"user_id": "h1"})),
                row(json!({"user_id": "h2"})),
                row(json!({"user_id": "h3"})),
            ]),
            "SELECT user_id FROM default.requests" => {
                Ok(vec![row(json!({"user_id": "slow-history"}))])
            }
            "SELECT user_id AS __k0, count() AS __a0_0, sum(latency_ms) AS __a1_0, \
                     max(latency_ms) AS __a2_0, count(latency_ms) AS __a2_1 \
                     FROM default.requests GROUP BY __k0" => Ok(vec![row(
                json!({"__k0": "u1", "__a0_0": 1, "__a1_0": 900, "__a2_0": 900, "__a2_1": 1}),
            )]),
            // top groups by total latency
            "SELECT user_id AS __k0, sum(latency_ms) AS __a0_0 FROM default.requests GROUP BY __k0" => {
                Ok(vec![
                    row(json!({"__k0": "u1", "__a0_0": 900})),
                    row(json!({"__k0": "u2", "__a0_0": 500})),
                    row(json!({"__k0": "u3", "__a0_0": 100})),
                ])
            }
            // the same top 3, with aliases
            "SELECT user_id AS who, latency_ms AS ms FROM default.requests \
                     WHERE latency_ms > 100 \
                     ORDER BY latency_ms DESC NULLS LAST LIMIT 3" => Ok(vec![
                row(json!({"who": "h900", "ms": 900})),
                row(json!({"who": "h500", "ms": 500})),
                row(json!({"who": "h500b", "ms": 500})),
            ]),
            // a resume's seed: the snapshot's sort keys
            "SELECT latency_ms AS __order_0 FROM default.requests WHERE latency_ms > 100 \
             ORDER BY latency_ms DESC NULLS LAST LIMIT 3" => Ok(vec![
                row(json!({"__order_0": 900})),
                row(json!({"__order_0": 500})),
                row(json!({"__order_0": 500})),
            ]),
            // FINAL: one row per product, with its key and version under hidden aliases
            "SELECT product_id, price, product_id AS __key_0, version AS __ver \
             FROM default.products FINAL WHERE price > 2" => Ok(vec![row(
                json!({"product_id": "a", "price": 5, "__key_0": "a", "__ver": 1}),
            )]),
            "SELECT product_id, price, product_id AS __key_0, version AS __ver \
             FROM default.products FINAL ORDER BY price DESC NULLS LAST" => Ok(vec![
                row(json!({"product_id": "a", "price": 5, "__key_0": "a", "__ver": 1})),
                row(json!({"product_id": "b", "price": 3, "__key_0": "b", "__ver": 1})),
            ]),
            other => panic!("unexpected history query: {other}"),
        }
    }
}

fn row(v: serde_json::Value) -> Row {
    v.as_object().unwrap().clone()
}

/// Partition 0 of `events` starts with the two events ClickHouse already has (offsets 0, 1).
fn events_topic() -> MemorySource {
    let mut source = MemorySource::new();
    source
        .push("events", 0, r#"{"ts": "h1", "user_id": "u1"}"#)
        .push("events", 0, r#"{"ts": "h2", "user_id": "u2"}"#);
    source
}

fn engine(source: MemorySource) -> Engine {
    Engine::new(
        ConfiguredRules::default(),
        FakeHistory,
        source,
        FakeSnapshots,
    )
}

#[tokio::test]
async fn history_up_to_the_watermark_then_events_after_it() {
    let mut source = events_topic();
    source
        .push(
            "events",
            0,
            r#"{"ts": "e1", "user_id": "u3", "_offset": 9}"#,
        )
        .push("events", 0, "not json")
        .push("events", 0, r#"{"ts": "e2"}"#)
        .push("events", 1, r#"{"ts": "e3", "user_id": "u4"}"#)
        .push("orders", 0, r#"{"order_id": "o1"}"#);

    let subscription = engine(source)
        .subscribe("select * from events")
        .await
        .unwrap();
    assert_eq!(subscription.plan.stream.topics, [TopicId("events".into())]);
    assert_eq!(subscription.watermark.describe(), "{0: 1, 1: -1}");

    let iterations: Vec<Iteration> = subscription.map(|i| i.unwrap()).collect().await;
    let history = &iterations[0];
    assert_eq!(history.origin, Origin::History);
    assert_eq!(
        history.diffs,
        [
            Diff::insert(row(json!({"ts": "h1", "user_id": "u1"}))),
            Diff::insert(row(json!({"ts": "h2", "user_id": "u2"})))
        ]
    );

    // Partition 0 resumes after the watermark (offsets 0 and 1 are history, not repeated);
    // partition 1 has nothing in ClickHouse, so it is read from its start. The malformed
    // message (0:3) is skipped; hidden columns are not output; `orders` is never read.
    let events: Vec<_> = iterations[1..]
        .iter()
        .map(|i| (i.origin.clone(), i.diffs.clone()))
        .collect();
    let at = |partition, offset| Origin::Event {
        topic: TopicId("events".into()),
        partition,
        offset,
    };
    assert_eq!(
        events,
        [
            (
                at(0, 2),
                vec![Diff::insert(row(json!({"ts": "e1", "user_id": "u3"})))]
            ),
            (
                at(0, 4),
                vec![Diff::insert(row(json!({"ts": "e2", "user_id": ""})))]
            ),
            (
                at(1, 0),
                vec![Diff::insert(row(json!({"ts": "e3", "user_id": "u4"})))]
            ),
        ]
    );
}

#[tokio::test]
async fn errors_are_clear() {
    let engine = engine(events_topic());
    let e = engine
        .explain("select * form events")
        .await
        .unwrap_err()
        .to_string();
    assert_eq!(
        e,
        "syntax error: expected FROM, found identifier `form` at 1:10"
    );
    let e = engine
        .explain("select * from nope")
        .await
        .unwrap_err()
        .to_string();
    assert_eq!(e, "unknown table nope in ClickHouse (default.nope) at 1:15");
    let e = engine
        .explain("select ts, nope from events")
        .await
        .unwrap_err()
        .to_string();
    assert_eq!(
        e,
        "unknown column nope in table events (columns: ts, user_id) at 1:12"
    );
}

#[tokio::test]
async fn missing_topic_is_a_bind_error() {
    // ClickHouse has the table, but Kafka has no `events` topic
    let e = engine(MemorySource::new())
        .subscribe("select * from events")
        .await
        .err()
        .unwrap()
        .to_string();
    assert_eq!(e, "no event stream for table events (topic events) at 1:15");
}

#[tokio::test]
async fn column_list_prunes_history_and_events() {
    let mut source = events_topic();
    source
        .push(
            "events",
            0,
            r#"{"ts": "e1", "user_id": "u3", "big": {"nested": [1, 2, 3]}, "_offset": 9}"#,
        )
        .push("events", 0, r#"{"ts": "e2"}"#);
    let subscription = engine(source)
        .subscribe("select user_id from events")
        .await
        .unwrap();
    assert_eq!(
        subscription.plan.history.sql("default.events"),
        "SELECT user_id FROM default.events"
    );
    assert_eq!(
        subscription.history_sql,
        "SELECT user_id FROM default.events"
    );
    assert_eq!(subscription.plan.stream.required_keys(), ["user_id"]);
    let iterations: Vec<Iteration> = subscription.map(|i| i.unwrap()).collect().await;
    let lines: Vec<String> = iterations.iter().map(|i| i.to_string()).collect();
    assert_eq!(
        lines,
        [
            "+ {\"user_id\":\"u1\"}\n+ {\"user_id\":\"u2\"}\nhistory: 2 rows",
            "events/0:2 + {\"user_id\":\"u3\"}",
            "events/0:3 + {\"user_id\":\"\"}",
        ]
    );
}

#[tokio::test]
async fn where_filters_events_by_typed_values() {
    let mut source = MemorySource::new();
    source
        // offset 0 is already in ClickHouse (the history's "slow-history")
        .push(
            "requests",
            0,
            r#"{"user_id": "slow-history", "latency_ms": 900}"#,
        )
        .push("requests", 0, r#"{"user_id": "fast", "latency_ms": 50}"#)
        .push("requests", 0, r#"{"user_id": "slow", "latency_ms": 500}"#)
        .push(
            "requests",
            0,
            r#"{"user_id": "as-text", "latency_ms": "250"}"#,
        )
        .push("requests", 0, r#"{"user_id": "missing"}"#)
        .push(
            "requests",
            0,
            r#"{"user_id": "broken", "latency_ms": "abc"}"#,
        );
    let subscription = engine(source)
        .subscribe("select user_id from requests where latency_ms > 100")
        .await
        .unwrap();
    // the filter's column is decoded (as UInt32) but not output
    assert_eq!(
        subscription.plan.stream.required_keys(),
        ["user_id", "latency_ms"]
    );
    let lines: Vec<String> = subscription
        .map(|i| i.unwrap())
        .filter(|i| std::future::ready(!i.diffs.is_empty()))
        .map(|i| i.to_string())
        .collect()
        .await;
    assert_eq!(
        lines,
        [
            "+ {\"user_id\":\"slow-history\"}\nhistory: 1 rows",
            "requests/0:2 + {\"user_id\":\"slow\"}",
            // "250" is read as UInt32, like ClickHouse does; a missing latency_ms is 0;
            // "abc" can't be read as UInt32, so that event is skipped with a warning
            "requests/0:3 + {\"user_id\":\"as-text\"}",
        ]
    );
}

#[tokio::test]
async fn order_by_places_each_new_row() {
    let mut source = MemorySource::new();
    source
        .push("requests", 0, r#"{"user_id": "h900", "latency_ms": 900}"#) // offset 0: history
        .push("requests", 0, r#"{"user_id": "e1000", "latency_ms": 1000}"#)
        .push("requests", 0, r#"{"user_id": "e500", "latency_ms": 500}"#)
        .push("requests", 0, r#"{"user_id": "fast", "latency_ms": 50}"#)
        .push("requests", 0, r#"{"user_id": "e700", "latency_ms": "700"}"#);
    let subscription = engine(source)
        .subscribe("select user_id from requests where latency_ms > 100 order by latency_ms desc")
        .await
        .unwrap();
    let iterations: Vec<Iteration> = subscription.map(|i| i.unwrap()).collect().await;
    // history comes in ClickHouse's order, without the hidden sort field
    assert_eq!(
        iterations[0].to_string(),
        "+ {\"user_id\":\"h900\"}\n+ {\"user_id\":\"h500\"}\n+ {\"user_id\":\"h500b\"}\nhistory: 3 rows"
    );
    let lines: Vec<String> = iterations[1..]
        .iter()
        .filter(|i| !i.diffs.is_empty())
        .map(|i| i.to_string())
        .collect();
    assert_eq!(
        lines,
        [
            "requests/0:1 + @0 {\"user_id\":\"e1000\"}",
            // a tie goes after the last row it ties with (h500, h500b)
            "requests/0:2 + @4 {\"user_id\":\"e500\"}",
            "requests/0:4 + @2 {\"user_id\":\"e700\"}",
        ]
    );
    // applying the indexes the way a client does gives the ordered result
    assert_eq!(
        names(&iterations),
        ["e1000", "h900", "e700", "h500", "h500b", "e500"]
    );
}

/// The result a client ends up with, as user ids; checks every iteration against `limit`.
fn names_within(iterations: &[Iteration], limit: Option<usize>) -> Vec<String> {
    let mut rows: Vec<Row> = Vec::new();
    for it in iterations {
        for d in &it.diffs {
            d.apply(&mut rows).unwrap();
        }
        if let Some(n) = limit {
            assert!(rows.len() <= n, "{} rows after {it}", rows.len());
        }
    }
    rows.iter()
        .map(|r| r["user_id"].as_str().unwrap().to_string())
        .collect()
}

fn names(iterations: &[Iteration]) -> Vec<String> {
    names_within(iterations, None)
}

#[tokio::test]
async fn limit_without_order_by_is_a_window_of_the_latest_rows() {
    let mut source = MemorySource::new();
    source
        .push("requests", 0, r#"{"user_id": "h900", "latency_ms": 900}"#) // offset 0: history
        .push("requests", 0, r#"{"user_id": "e1", "latency_ms": 200}"#)
        .push("requests", 0, r#"{"user_id": "fast", "latency_ms": 5}"#)
        .push("requests", 0, r#"{"user_id": "e2", "latency_ms": 300}"#)
        .push("requests", 0, r#"{"user_id": "e3", "latency_ms": 400}"#);
    let subscription = engine(source)
        .subscribe("select user_id from requests where latency_ms > 100 limit 2")
        .await
        .unwrap();
    assert!(
        subscription.history_sql.ends_with(" LIMIT 2"),
        "{}",
        subscription.history_sql
    );
    let iterations: Vec<Iteration> = subscription.map(|i| i.unwrap()).collect().await;
    let lines: Vec<String> = iterations[1..]
        .iter()
        .filter(|i| !i.diffs.is_empty())
        .map(|i| i.to_string())
        .collect();
    assert_eq!(
        lines,
        [
            // history had 1 row: not full yet
            "requests/0:1 + {\"user_id\":\"e1\"}",
            // full: append, then the first row goes, in the same iteration
            "requests/0:3 + {\"user_id\":\"e2\"}\nrequests/0:3 - @0",
            "requests/0:4 + {\"user_id\":\"e3\"}\nrequests/0:4 - @0",
        ]
    );
    assert_eq!(names_within(&iterations, Some(2)), ["e2", "e3"]);
}

#[tokio::test]
async fn offset_without_order_by_holds_back_the_newest_rows() {
    let mut source = MemorySource::new();
    source
        .push("requests", 0, r#"{"user_id": "h1", "latency_ms": 900}"#) // offset 0: history
        .push("requests", 0, r#"{"user_id": "e1", "latency_ms": 200}"#)
        .push("requests", 0, r#"{"user_id": "fast", "latency_ms": 5}"#)
        .push("requests", 0, r#"{"user_id": "e2", "latency_ms": 300}"#);
    let subscription = engine(source)
        .subscribe("select user_id from requests where latency_ms > 100 limit 2 offset 1")
        .await
        .unwrap();
    // the rows held back come from ClickHouse too
    assert!(
        subscription.history_sql.ends_with(" LIMIT 3"),
        "{}",
        subscription.history_sql
    );
    let iterations: Vec<Iteration> = subscription.map(|i| i.unwrap()).collect().await;
    let lines: Vec<String> = iterations
        .iter()
        .filter(|i| !i.diffs.is_empty())
        .map(|i| i.to_string())
        .collect();
    assert_eq!(
        lines,
        [
            // h3, the newest of the history, is held back
            "+ {\"user_id\":\"h1\"}\n+ {\"user_id\":\"h2\"}\nhistory: 2 rows",
            // each new row is held back, and the oldest held one slides in
            "requests/0:1 + {\"user_id\":\"h3\"}\nrequests/0:1 - @0",
            "requests/0:3 + {\"user_id\":\"e1\"}\nrequests/0:3 - @0",
        ]
    );
    assert_eq!(names_within(&iterations, Some(2)), ["h3", "e1"]);
}

#[tokio::test]
async fn order_by_with_offset_slides_rows_in_from_above() {
    let mut source = MemorySource::new();
    source
        .push("requests", 0, r#"{"user_id": "h900", "latency_ms": 900}"#) // offset 0: history
        .push("requests", 0, r#"{"user_id": "e1000", "latency_ms": 1000}"#) // above the result
        .push("requests", 0, r#"{"user_id": "e700", "latency_ms": 700}"#) // inside it
        .push("requests", 0, r#"{"user_id": "e150", "latency_ms": 150}"#); // below it
    let subscription = engine(source)
        .subscribe(
            "select user_id from requests where latency_ms > 100 \
             order by latency_ms desc limit 2 offset 1",
        )
        .await
        .unwrap();
    let iterations: Vec<Iteration> = subscription.map(|i| i.unwrap()).collect().await;
    let lines: Vec<String> = iterations
        .iter()
        .filter(|i| !i.diffs.is_empty())
        .map(|i| i.to_string())
        .collect();
    assert_eq!(
        lines,
        [
            // the top 3 fetched; h900 is above the result
            "+ {\"user_id\":\"h500\"}\n+ {\"user_id\":\"h500b\"}\nhistory: 2 rows",
            // e1000 lands above: h900 slides in at the top, h500b out at the bottom
            "requests/0:1 + @0 {\"user_id\":\"h900\"}\nrequests/0:1 - @2",
            "requests/0:2 + @1 {\"user_id\":\"e700\"}\nrequests/0:2 - @2",
        ]
    );
    assert_eq!(names_within(&iterations, Some(2)), ["h900", "e700"]);
}

#[tokio::test]
async fn order_by_with_limit_keeps_the_top_n() {
    let mut source = MemorySource::new();
    source
        .push("requests", 0, r#"{"user_id": "h900", "latency_ms": 900}"#) // offset 0: history
        .push("requests", 0, r#"{"user_id": "e100", "latency_ms": 150}"#) // past the end
        .push("requests", 0, r#"{"user_id": "e500", "latency_ms": 500}"#) // ties with the last
        .push("requests", 0, r#"{"user_id": "e700", "latency_ms": 700}"#) // pushes h500b out
        .push("requests", 0, r#"{"user_id": "e1000", "latency_ms": 1000}"#); // pushes h500 out
    let subscription = engine(source)
        .subscribe(
            "select user_id from requests where latency_ms > 100 order by latency_ms desc limit 3",
        )
        .await
        .unwrap();
    let iterations: Vec<Iteration> = subscription.map(|i| i.unwrap()).collect().await;
    let lines: Vec<String> = iterations[1..]
        .iter()
        .filter(|i| !i.diffs.is_empty())
        .map(|i| i.to_string())
        .collect();
    assert_eq!(
        lines,
        [
            "requests/0:3 + @1 {\"user_id\":\"e700\"}\nrequests/0:3 - @3",
            "requests/0:4 + @0 {\"user_id\":\"e1000\"}\nrequests/0:4 - @3",
        ]
    );
    assert_eq!(
        names_within(&iterations, Some(3)),
        ["e1000", "h900", "e700"]
    );
}

#[tokio::test]
async fn resume_falls_back_to_a_snapshot_when_the_topic_was_recreated() {
    let sql = "select user_id from requests";
    let mut source = MemorySource::new();
    source
        .push(
            "requests",
            0,
            r#"{"user_id": "slow-history", "latency_ms": 900}"#,
        )
        .push("requests", 0, r#"{"user_id": "e1", "latency_ms": 200}"#)
        .push("requests", 0, r#"{"user_id": "e2", "latency_ms": 300}"#);
    let subscription = engine(source).subscribe(sql).await.unwrap();
    let plan = subscription.plan.clone();
    let iterations: Vec<Iteration> = subscription.map(|i| i.unwrap()).collect().await;
    let token = chlive_engine::cursor::encode(&plan, &iterations.last().unwrap().at);
    assert!(token.contains(r#""o":{"0":2}"#), "{token}");

    // the same topic, still there: resumes (nothing new)
    let mut same = MemorySource::new();
    same.push("requests", 0, r#"{"user_id": "x"}"#)
        .push("requests", 0, r#"{"user_id": "e1"}"#)
        .push("requests", 0, r#"{"user_id": "e2"}"#);
    assert!(
        engine(same)
            .subscribe_from(sql, Some(&token))
            .await
            .unwrap()
            .resumed
    );

    // recreated: only one message now, so the cursor is past its end
    let mut recreated = MemorySource::new();
    recreated.push("requests", 0, r#"{"user_id": "new"}"#);
    let s = engine(recreated)
        .subscribe_from(sql, Some(&token))
        .await
        .unwrap();
    assert!(!s.resumed);
    let first = s.map(|i| i.unwrap()).collect::<Vec<_>>().await.remove(0);
    assert_eq!(first.origin, Origin::History);
}

#[tokio::test]
async fn group_by_runs_natively_without_chdb() {
    let mut source = MemorySource::new();
    source
        .push("requests", 0, r#"{"user_id": "u1", "latency_ms": 900}"#) // offset 0: history
        .push("requests", 0, r#"{"user_id": "u1", "latency_ms": 100}"#) // count and sum change
        .push("requests", 0, r#"{"user_id": "u2", "latency_ms": 50}"#) // a new group
        .push("requests", 0, r#"{"user_id": "u1", "latency_ms": 1000}"#); // the max changes too
    let sql = "select user_id, count() as n, sum(latency_ms) as total, max(latency_ms) as top \
               from requests group by user_id";
    let subscription = engine(source)
        .with_max_batch(1)
        .subscribe(sql)
        .await
        .unwrap();
    let agg = subscription.plan.aggregate.as_ref().unwrap();
    assert!(agg.native.is_some(), "{}", subscription.plan);
    let iterations: Vec<Iteration> = subscription.map(|i| i.unwrap()).collect().await;
    let lines: Vec<String> = iterations.iter().map(|i| i.to_string()).collect();
    assert_eq!(
        lines,
        [
            "+ [\"u1\"] {\"user_id\":\"u1\",\"n\":1,\"total\":900,\"top\":900}\nhistory: 1 rows",
            // updates carry only the columns that changed (the max stays 900)
            "requests/0:1 ~ [\"u1\"] {\"n\":2,\"total\":1000}",
            "requests/0:2 + [\"u2\"] {\"user_id\":\"u2\",\"n\":1,\"total\":50,\"top\":50}",
            "requests/0:3 ~ [\"u1\"] {\"n\":3,\"total\":2000,\"top\":1000}",
        ]
    );
}

/// FINAL on a ReplacingMergeTree: one row per key, its latest version; new versions update
/// it, and leave the result when deleted or no longer matching.
#[tokio::test]
async fn final_replaces_each_keys_row_with_its_latest_version() {
    let mut source = MemorySource::new();
    let p = |id: &str, price: u32, version: u64, deleted: u8| {
        format!(
            r#"{{"product_id": "{id}", "price": {price}, "version": {version}, "deleted": {deleted}}}"#
        )
    };
    source
        .push("products", 0, p("a", 5, 1, 0)) // offset 0: history
        .push("products", 0, p("a", 7, 2, 0)) // a new version: updated
        .push("products", 0, p("b", 1, 1, 0)) // doesn't match: nothing
        .push("products", 0, p("b", 3, 2, 0)) // now it does: inserted
        .push("products", 0, p("a", 9, 1, 0)) // older than a's row: ignored
        .push("products", 0, p("a", 9, 3, 1)) // deleted
        .push("products", 0, p("b", 1, 3, 0)) // no longer matches: deleted
        .push("products", 0, p("a", 4, 4, 0)); // back
    let sql = "select product_id, price from products final where price > 2";
    let subscription = engine(source)
        .with_max_batch(1)
        .subscribe(sql)
        .await
        .unwrap();
    assert!(subscription.checkpointable());
    let lines: Vec<String> = subscription.map(|i| i.unwrap().to_string()).collect().await;
    assert_eq!(
        lines,
        [
            "+ [\"a\"] {\"product_id\":\"a\",\"price\":5}\nhistory: 1 rows",
            "products/0:1 ~ [\"a\"] {\"product_id\":\"a\",\"price\":7}",
            "",
            "products/0:3 + [\"b\"] {\"product_id\":\"b\",\"price\":3}",
            "",
            "products/0:5 - [\"a\"]",
            "products/0:6 - [\"b\"]",
            "products/0:7 + [\"a\"] {\"product_id\":\"a\",\"price\":4}",
        ]
    );
}

/// FINAL with ORDER BY: rows are inserted at their places, and move (an update with an index)
/// when a new version changes their sort values.
#[tokio::test]
async fn final_rows_move_as_their_versions_change() {
    let mut source = MemorySource::new();
    let p = |id: &str, price: u32, version: u64, deleted: u8| {
        format!(
            r#"{{"product_id": "{id}", "price": {price}, "version": {version}, "deleted": {deleted}}}"#
        )
    };
    source
        .push("products", 0, p("a", 5, 1, 0)) // offset 0: history (a 5, b 3)
        .push("products", 0, p("c", 4, 1, 0)) // between them
        .push("products", 0, p("a", 5, 2, 0)) // the same price: stays
        .push("products", 0, p("b", 9, 2, 0)) // to the top
        .push("products", 0, p("c", 4, 2, 1)) // deleted
        .push("products", 0, p("a", 1, 3, 0)); // already last: no move
    let sql = "select product_id, price from products final order by price desc";
    let lines: Vec<String> = engine(source)
        .with_max_batch(1)
        .subscribe(sql)
        .await
        .unwrap()
        .map(|i| i.unwrap().to_string())
        .collect()
        .await;
    assert_eq!(
        lines,
        [
            "+ [\"a\"] {\"product_id\":\"a\",\"price\":5}\n\
             + [\"b\"] {\"product_id\":\"b\",\"price\":3}\nhistory: 2 rows",
            "products/0:1 + @1 [\"c\"] {\"product_id\":\"c\",\"price\":4}",
            "products/0:2 ~ [\"a\"] {\"product_id\":\"a\",\"price\":5}",
            "products/0:3 ~ [\"b\"] @0 {\"product_id\":\"b\",\"price\":9}",
            "products/0:4 - [\"c\"]",
            "products/0:5 ~ [\"a\"] {\"product_id\":\"a\",\"price\":1}",
        ]
    );
}

#[tokio::test]
async fn aliases_name_history_and_stream_rows() {
    let mut source = MemorySource::new();
    source
        .push("requests", 0, r#"{"user_id": "h900", "latency_ms": 900}"#) // offset 0: history
        .push("requests", 0, r#"{"user_id": "e700", "latency_ms": 700}"#);
    let subscription = engine(source)
        .subscribe(
            "select user_id as who, latency_ms as ms from requests where latency_ms > 100 \
             order by latency_ms desc limit 3",
        )
        .await
        .unwrap();
    let names: Vec<&str> = subscription
        .plan
        .columns
        .iter()
        .map(|c| c.name.as_str())
        .collect();
    assert_eq!(names, ["who", "ms"]);
    let iterations: Vec<Iteration> = subscription.map(|i| i.unwrap()).collect().await;
    let lines: Vec<String> = iterations.iter().map(|i| i.to_string()).collect();
    assert_eq!(
        lines,
        [
            "+ {\"who\":\"h900\",\"ms\":900}\n+ {\"who\":\"h500\",\"ms\":500}\n\
             + {\"who\":\"h500b\",\"ms\":500}\nhistory: 3 rows",
            // placed by the aliased ORDER BY column; pushes the last tie out
            "requests/0:1 + @1 {\"who\":\"e700\",\"ms\":700}\nrequests/0:1 - @3",
        ]
    );
}

fn token(plan: &chlive_engine::Plan, at: &[(i32, i64)]) -> String {
    chlive_engine::cursor::encode(plan, &Watermark(at.iter().copied().collect()))
}

#[tokio::test]
async fn ordered_resume_rebuilds_from_the_snapshot_and_the_clients_own_events() {
    let sql =
        "select user_id from requests where latency_ms > 100 order by latency_ms desc limit 3";
    let source = || {
        let mut s = MemorySource::new();
        s.push("requests", 0, r#"{"user_id": "h900", "latency_ms": 900}"#) // 0: in the snapshot
            .push("requests", 0, r#"{"user_id": "e700", "latency_ms": 700}"#) // 1: the client's
            .push("requests", 0, r#"{"user_id": "e1000", "latency_ms": 1000}"#); // 2: new
        s
    };
    let e = engine(source());
    let plan = e.explain(sql).await.unwrap();

    // the client saw offset 1, past the snapshot (0): resumed
    let s = e
        .subscribe_from(sql, Some(&token(&plan, &[(0, 1)])))
        .await
        .unwrap();
    assert!(s.resumed);
    let lines: Vec<String> = s.map(|i| i.unwrap().to_string()).collect().await;
    assert_eq!(
        lines,
        [
            // the state as the client has it (900, 700, 500), rebuilt, nothing sent
            "caught up: 0 changes",
            "requests/0:2 + @0 {\"user_id\":\"e1000\"}\nrequests/0:2 - @3",
        ]
    );

    // behind the snapshot: its state can't be rebuilt, so a full snapshot
    let e = engine(source());
    let s = e
        .subscribe_from(sql, Some(&token(&plan, &[(0, -1)])))
        .await
        .unwrap();
    assert!(!s.resumed);
}

#[tokio::test]
async fn unordered_resume_continues_from_the_cursor_without_clickhouse() {
    let sql = "select user_id from events";
    let e = engine(events_topic());
    let plan = e.explain(sql).await.unwrap();
    // behind the snapshot ({0: 1}): fine, the stream replays from right after the cursor
    let s = e
        .subscribe_from(sql, Some(&token(&plan, &[(0, 0), (1, -1)])))
        .await
        .unwrap();
    assert!(s.resumed);
    assert_eq!(s.history_sql, "");
    let lines: Vec<String> = s.map(|i| i.unwrap().to_string()).collect().await;
    assert_eq!(
        lines,
        ["caught up: 0 changes", "events/0:1 + {\"user_id\":\"u2\"}"]
    );
}

#[tokio::test]
async fn ordered_groups_move_and_come_in_and_out_of_the_limit() {
    let mut source = MemorySource::new();
    source
        .push("requests", 0, r#"{"user_id": "h", "latency_ms": 1}"#) // offset 0: history
        .push("requests", 0, r#"{"user_id": "u3", "latency_ms": 1000}"#) // 1100: climbs to the top
        .push("requests", 0, r#"{"user_id": "u1", "latency_ms": 300}"#) // 1200: overtakes u3
        .push("requests", 0, r#"{"user_id": "u2", "latency_ms": 10}"#) // 510: still out
        .push("requests", 0, r#"{"user_id": "u4", "latency_ms": 5}"#); // new, out
    let sql = "select user_id, sum(latency_ms) as total from requests \
               group by user_id order by total desc limit 2";
    let subscription = engine(source)
        .with_max_batch(1)
        .subscribe(sql)
        .await
        .unwrap();
    let lines: Vec<String> = subscription.map(|i| i.unwrap().to_string()).collect().await;
    assert_eq!(
        lines,
        [
            // the top 2 of the history (u3, at 100, is kept for later)
            "+ [\"u1\"] {\"user_id\":\"u1\",\"total\":900}\n\
             + [\"u2\"] {\"user_id\":\"u2\",\"total\":500}\nhistory: 2 rows",
            // u3 climbs in at the top and pushes u2 out
            "requests/0:1 + @0 [\"u3\"] {\"user_id\":\"u3\",\"total\":1100}\nrequests/0:1 - @2",
            // u1 overtakes u3: an update that moves it
            "requests/0:2 ~ [\"u1\"] @0 {\"total\":1200}",
            // below the LIMIT: nothing to send
            "",
            "",
        ]
    );
}

/// A source whose cached partition list can lag: `partitions` lists only `cached`, while
/// `refresh_partitions` reads the real ones, counting each call.
struct LaggingSource {
    inner: MemorySource,
    cached: Vec<i32>,
    refreshes: Arc<std::sync::atomic::AtomicUsize>,
}

#[async_trait]
impl EventSource for LaggingSource {
    async fn partitions(&self, topic: &TopicId) -> Result<Option<Vec<i32>>, SourceError> {
        Ok(self
            .inner
            .partitions(topic)
            .await?
            .map(|_| self.cached.clone()))
    }

    async fn refresh_partitions(&self, topic: &TopicId) -> Result<Option<Vec<i32>>, SourceError> {
        self.refreshes
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        self.inner.partitions(topic).await
    }

    async fn subscribe(
        &self,
        starts: &[TopicStart],
        keys: &[String],
    ) -> Result<BoxStream<'static, Event>, SourceError> {
        self.inner.subscribe(starts, keys).await
    }
}

/// Events of a partition the snapshot knows but the cached list doesn't (added since) are read
/// all the same: the partitions are read anew, once. With an up-to-date list, they aren't.
#[tokio::test]
async fn a_partition_missing_from_the_cached_list_is_read_anew() {
    for (cached, refreshes, events) in [(vec![0], 1, 2), (vec![0, 1], 0, 2)] {
        let mut inner = events_topic();
        inner
            .push("events", 0, r#"{"ts": "e1", "user_id": "u3"}"#)
            .push("events", 1, r#"{"ts": "e2", "user_id": "u4"}"#);
        let count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let source = LaggingSource {
            inner,
            cached,
            refreshes: count.clone(),
        };
        let engine = Engine::new(
            ConfiguredRules::default(),
            FakeHistory,
            source,
            FakeSnapshots,
        );
        let subscription = engine.subscribe("select * from events").await.unwrap();
        let iterations: Vec<Iteration> = subscription.map(|i| i.unwrap()).collect().await;
        assert_eq!(iterations.len() - 1, events);
        assert_eq!(count.load(std::sync::atomic::Ordering::Relaxed), refreshes);
    }
}
