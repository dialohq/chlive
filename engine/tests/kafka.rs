//! Against the engine shell's Redpanda and ClickHouse, which ingests the `events` topic into
//! `default.events` (via CHLIVE_KAFKA and CHLIVE_CLICKHOUSE_URL); runs only with CHLIVE_E2E=1.

use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use chlive_engine::{Config, Engine, Origin};
use futures::StreamExt;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::ClientConfig;

fn clickhouse_url() -> String {
    std::env::var("CHLIVE_CLICKHOUSE_URL").unwrap_or_else(|_| "http://127.0.0.1:28123".into())
}

fn brokers() -> String {
    std::env::var("CHLIVE_KAFKA").unwrap_or_else(|_| "127.0.0.1:29092".into())
}

async fn count(path: &str) -> u64 {
    let sql = format!("SELECT count() FROM default.events WHERE path = '{path}'");
    let text = reqwest::Client::new()
        .post(clickhouse_url())
        .body(sql)
        .send()
        .await
        .expect("ClickHouse reachable (is `dnvr up` running in the engine shell?)")
        .text()
        .await
        .unwrap();
    text.trim()
        .parse()
        .unwrap_or_else(|_| panic!("count: {text}"))
}

/// Events of one test run share a unique `path`; `user_id` numbers them.
async fn produce(producer: &FutureProducer, path: &str, run: &str, ids: std::ops::Range<usize>) {
    for i in ids {
        let user_id = format!("{run}-{i}");
        let payload = format!(
            r#"{{"ts":"2026-10-01 12:00:00.000","user_id":"{user_id}","country":"PL","path":"{path}","latency_ms":{i},"amount":0}}"#
        );
        producer
            .send(
                FutureRecord::to("events").key(&user_id).payload(&payload),
                Duration::from_secs(10),
            )
            .await
            .map_err(|(e, _)| e)
            .expect("produced (is the engine shell's Redpanda up?)");
    }
}

#[tokio::test]
async fn every_event_exactly_once_across_the_watermark() {
    if std::env::var_os("CHLIVE_E2E").is_none() {
        eprintln!("skipping: set CHLIVE_E2E=1 (requires `dnvr up` in the engine shell)");
        return;
    }
    let run = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos()
        .to_string();
    let path = format!("/e2e/{run}");
    let producer: FutureProducer = ClientConfig::new()
        .set("bootstrap.servers", brokers())
        .create()
        .unwrap();

    // 0..3: in ClickHouse before subscribing
    produce(&producer, &path, &run, 0..3).await;
    let deadline = Instant::now() + Duration::from_secs(60);
    while count(&path).await < 3 {
        assert!(
            Instant::now() < deadline,
            "ClickHouse did not ingest the events"
        );
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    // 3..6: in Kafka, likely not yet in ClickHouse when subscribing
    produce(&producer, &path, &run, 3..6).await;

    let engine = Engine::connect(Config {
        clickhouse_url: clickhouse_url(),
        kafka_brokers: brokers(),
        ..Config::default()
    });
    let mut subscription = engine
        .subscribe(&format!("select user_id from events where path = '{path}'"))
        .await
        .unwrap();
    eprintln!("watermark {}", subscription.watermark.describe());

    // 6..9: after subscribing
    produce(&producer, &path, &run, 6..9).await;

    let (mut history, mut stream) = (Vec::new(), Vec::new());
    let deadline = Instant::now() + Duration::from_secs(30);
    // Read until all 9 arrived, then 2 more seconds to catch any repeated event.
    let mut quiet_until: Option<Instant> = None;
    loop {
        let until = quiet_until.unwrap_or(deadline);
        let Ok(next) = tokio::time::timeout_at(until.into(), subscription.next()).await else {
            break;
        };
        let iteration = next.expect("the stream does not end").unwrap();
        let ids = iteration
            .diffs
            .iter()
            .map(|d| d.to_string())
            .collect::<Vec<_>>();
        match iteration.origin {
            Origin::History => history.extend(ids),
            Origin::Event { .. } => stream.extend(ids),
        }
        if quiet_until.is_none() && history.len() + stream.len() >= 9 {
            quiet_until = Some(Instant::now() + Duration::from_secs(2));
        }
    }
    eprintln!(
        "{} from history, {} from the stream",
        history.len(),
        stream.len()
    );
    assert!(
        history.len() >= 3,
        "events ingested before subscribing are history"
    );
    let mut all = [history, stream].concat();
    all.sort();
    let mut expected: Vec<String> = (0..9)
        .map(|i| format!(r#"+ {{"user_id":"{run}-{i}"}}"#))
        .collect();
    expected.sort();
    assert_eq!(all, expected, "each event exactly once");
}
