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
        if table != "default.events" {
            return Err(HistoryError::UnknownTable(table.into()));
        }
        let col = |n: &str| DescribedColumn {
            name: n.into(),
            ty: "String".into(),
        };
        Ok(vec![
            col("ts"),
            col("user_id"),
            col("_partition"),
            col("_offset"),
        ])
    }

    async fn fetch(&self, sql: &str) -> Result<Vec<Row>, HistoryError> {
        assert_eq!(sql, "SELECT ts, user_id FROM default.events");
        Ok(vec![
            row(json!({"ts": "h1", "user_id": "u1"})),
            row(json!({"ts": "h2", "user_id": "u2"})),
        ])
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
                vec![Diff::Insert(row(json!({"ts": "e2", "user_id": null})))]
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
    assert_eq!(e, "unknown table default.nope");
}
