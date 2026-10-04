//! The engine loop without ClickHouse or Kafka: fake history, events in memory.

use async_trait::async_trait;
use chlive_engine::diff::{Diff, Row};
use chlive_engine::history::{DescribedColumn, HistoryError, HistorySource};
use chlive_engine::rules::{HardcodedRules, TopicId};
use chlive_engine::source::MemorySource;
use chlive_engine::{Engine, Iteration, Origin};
use futures::StreamExt;
use serde_json::json;

/// `events`: ClickHouse has offsets 0..=1 of partition 0 and nothing of partition 1.
/// `requests`: ClickHouse has offset 0 of partition 0.
struct FakeHistory;

const EVENTS_BOUND: &str = "toInt64(_offset) <= multiIf(_partition = 0, 1, -1)";
const REQUESTS_BOUND: &str = "toInt64(_offset) <= multiIf(_partition = 0, 0, -1)";

#[async_trait]
impl HistorySource for FakeHistory {
    async fn describe(&self, table: &str) -> Result<Vec<DescribedColumn>, HistoryError> {
        let col = |n: &str, ty: &str| DescribedColumn {
            name: n.into(),
            ty: ty.into(),
        };
        match table {
            "default.events" => Ok(vec![
                col("ts", "String"),
                col("user_id", "String"),
                col("_partition", "UInt64"),
                col("_offset", "UInt64"),
            ]),
            "default.requests" => Ok(vec![
                col("user_id", "String"),
                col("latency_ms", "UInt32"),
                col("_partition", "UInt64"),
                col("_offset", "UInt64"),
            ]),
            _ => Err(HistoryError::UnknownTable(table.into())),
        }
    }

    async fn fetch(&self, sql: &str) -> Result<Vec<Row>, HistoryError> {
        // ClickHouse returns exactly the selected columns, in order
        let events = format!("SELECT ts, user_id FROM default.events WHERE {EVENTS_BOUND}");
        let events_users = format!("SELECT user_id FROM default.events WHERE {EVENTS_BOUND}");
        let slow = format!(
            "SELECT user_id FROM default.requests WHERE (latency_ms > 100) AND ({REQUESTS_BOUND})"
        );
        let slowest = format!(
            "SELECT user_id, latency_ms AS __order_0 FROM default.requests \
             WHERE (latency_ms > 100) AND ({REQUESTS_BOUND}) ORDER BY latency_ms DESC NULLS LAST"
        );
        match sql {
            "SELECT _partition AS p, max(_offset) AS o FROM default.events GROUP BY p" => {
                Ok(vec![row(json!({"p": 0, "o": 1}))])
            }
            "SELECT _partition AS p, max(_offset) AS o FROM default.requests GROUP BY p" => {
                Ok(vec![row(json!({"p": 0, "o": 0}))])
            }
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
            s if s == format!("SELECT user_id FROM default.requests WHERE {REQUESTS_BOUND}") => {
                Ok(vec![row(json!({"user_id": "slow-history"}))])
            }
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
    Engine::new(HardcodedRules::default(), FakeHistory, source)
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
    assert_eq!(subscription.watermark.describe(), "{0: 1}");

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
        subscription.plan.history.sql(None),
        "SELECT user_id FROM default.events"
    );
    assert_eq!(
        subscription.history_sql,
        format!("SELECT user_id FROM default.events WHERE {EVENTS_BOUND}")
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
