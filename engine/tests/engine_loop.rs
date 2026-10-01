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
            Diff::Insert(row(json!({"ts": "h1", "user_id": "u1"}))),
            Diff::Insert(row(json!({"ts": "h2", "user_id": "u2"})))
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
                vec![Diff::Insert(row(json!({"ts": "e1", "user_id": "u3"})))]
            ),
            (
                at(0, 4),
                vec![Diff::Insert(row(json!({"ts": "e2", "user_id": ""})))]
            ),
            (
                at(1, 0),
                vec![Diff::Insert(row(json!({"ts": "e3", "user_id": "u4"})))]
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
