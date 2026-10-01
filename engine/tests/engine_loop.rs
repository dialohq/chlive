//! The engine loop without ClickHouse or Kafka: fake history, temporary JSONL topics.

use async_trait::async_trait;
use chlive_engine::diff::{Diff, Row};
use chlive_engine::history::{DescribedColumn, HistoryError, HistorySource};
use chlive_engine::rules::{HardcodedRules, TopicId};
use chlive_engine::stream::JsonlTopics;
use chlive_engine::{Engine, Iteration, Origin};
use futures::StreamExt;
use serde_json::json;

struct FakeHistory;

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
            "default.requests" => Ok(vec![col("user_id", "String"), col("latency_ms", "UInt32")]),
            _ => Err(HistoryError::UnknownTable(table.into())),
        }
    }

    async fn fetch(&self, sql: &str) -> Result<Vec<Row>, HistoryError> {
        // ClickHouse returns exactly the selected columns, in order
        match sql {
            "SELECT ts, user_id FROM default.events" => Ok(vec![
                row(json!({"ts": "h1", "user_id": "u1"})),
                row(json!({"ts": "h2", "user_id": "u2"})),
            ]),
            "SELECT user_id FROM default.events" => Ok(vec![
                row(json!({"user_id": "u1"})),
                row(json!({"user_id": "u2"})),
            ]),
            "SELECT user_id FROM default.requests WHERE latency_ms > 100" => {
                Ok(vec![row(json!({"user_id": "slow-history"}))])
            }
            other => panic!("unexpected history query: {other}"),
        }
    }
}

fn row(v: serde_json::Value) -> Row {
    v.as_object().unwrap().clone()
}

fn engine(topics: JsonlTopics) -> Engine {
    Engine::new(HardcodedRules::default(), FakeHistory, topics)
}

#[tokio::test]
async fn history_then_one_iteration_per_event_of_the_subscribed_topic_only() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("events.jsonl"),
        concat!(
            r#"{"ts": "e1", "user_id": "u3", "_offset": 9}"#,
            "\n",
            "not json\n",
            "\n",
            r#"{"ts": "e2"}"#,
            "\n",
        ),
    )
    .unwrap();
    std::fs::write(dir.path().join("orders.jsonl"), r#"{"order_id": "o1"}"#).unwrap();

    let engine = engine(JsonlTopics::new(dir.path()));
    let subscription = engine.subscribe("select * from events").await.unwrap();
    assert_eq!(subscription.plan.stream.topics, [TopicId("events".into())]);

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

    // the malformed line (2) and the empty line (3) are skipped; hidden columns are not output
    let events: Vec<_> = iterations[1..]
        .iter()
        .map(|i| (i.origin.clone(), i.diffs.clone()))
        .collect();
    let at = |line| Origin::Event {
        topic: TopicId("events".into()),
        line,
    };
    assert_eq!(
        events,
        [
            (
                at(1),
                vec![Diff::Insert(row(json!({"ts": "e1", "user_id": "u3"})))]
            ),
            (
                at(4),
                vec![Diff::Insert(row(json!({"ts": "e2", "user_id": ""})))]
            ),
        ]
    );
}

#[tokio::test]
async fn follow_mode_picks_up_appended_events() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("events.jsonl");
    std::fs::write(&path, "{\"ts\": \"e1\"}\n").unwrap();
    let mut topics = JsonlTopics::new(dir.path());
    topics.follow = true;
    let engine = engine(topics);
    let mut iterations = engine.subscribe("select * from events").await.unwrap();
    assert_eq!(
        iterations.next().await.unwrap().unwrap().origin,
        Origin::History
    );
    assert_eq!(iterations.next().await.unwrap().unwrap().diffs.len(), 1);

    use std::io::Write;
    let mut f = std::fs::OpenOptions::new()
        .append(true)
        .open(&path)
        .unwrap();
    writeln!(f, "{{\"ts\": \"e2\", \"user_id\": \"late\"}}").unwrap();
    let next = tokio::time::timeout(std::time::Duration::from_secs(5), iterations.next())
        .await
        .expect("appended event arrives")
        .unwrap()
        .unwrap();
    assert_eq!(
        next.diffs,
        [Diff::Insert(row(json!({"ts": "e2", "user_id": "late"})))]
    );
}

#[tokio::test]
async fn errors_are_clear() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("events.jsonl"), "").unwrap();
    let engine = engine(JsonlTopics::new(dir.path()));
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
    // ClickHouse has the table, but there is no events.jsonl
    let dir = tempfile::tempdir().unwrap();
    let engine = engine(JsonlTopics::new(dir.path()));
    let e = engine
        .subscribe("select * from events")
        .await
        .err()
        .unwrap()
        .to_string();
    assert_eq!(e, "no event stream for table events (topic events) at 1:15");
}

#[tokio::test]
async fn column_list_prunes_history_and_events() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("events.jsonl"),
        concat!(
            r#"{"ts": "e1", "user_id": "u3", "big": {"nested": [1, 2, 3]}, "_offset": 9}"#,
            "\n",
            r#"{"ts": "e2"}"#,
            "\n",
        ),
    )
    .unwrap();
    let engine = engine(JsonlTopics::new(dir.path()));
    let subscription = engine
        .subscribe("select user_id from events")
        .await
        .unwrap();
    assert_eq!(
        subscription.plan.history.sql,
        "SELECT user_id FROM default.events"
    );
    assert_eq!(subscription.plan.stream.required_keys(), ["user_id"]);
    let iterations: Vec<Iteration> = subscription.map(|i| i.unwrap()).collect().await;
    let lines: Vec<String> = iterations.iter().map(|i| i.to_string()).collect();
    assert_eq!(
        lines,
        [
            "+ {\"user_id\":\"u1\"}\n+ {\"user_id\":\"u2\"}\nhistory: 2 rows",
            "events:1 + {\"user_id\":\"u3\"}",
            "events:2 + {\"user_id\":\"\"}",
        ]
    );
}

#[tokio::test]
async fn where_filters_events_by_typed_values() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("requests.jsonl"),
        concat!(
            r#"{"user_id": "fast", "latency_ms": 50}"#,
            "\n",
            r#"{"user_id": "slow", "latency_ms": 500}"#,
            "\n",
            r#"{"user_id": "as-text", "latency_ms": "250"}"#,
            "\n",
            r#"{"user_id": "missing"}"#,
            "\n",
            r#"{"user_id": "broken", "latency_ms": "abc"}"#,
            "\n",
        ),
    )
    .unwrap();
    let engine = engine(JsonlTopics::new(dir.path()));
    let subscription = engine
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
            "requests:2 + {\"user_id\":\"slow\"}",
            // "250" is read as UInt32, like ClickHouse does; a missing latency_ms is 0;
            // "abc" can't be read as UInt32, so that event is skipped with a warning
            "requests:3 + {\"user_id\":\"as-text\"}",
        ]
    );
}
