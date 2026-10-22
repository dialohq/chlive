//! The pause server (continuous-backup's `pause-server`): pauses a table's ingestion and
//! reports exactly which Kafka offsets the table holds.
//!
//! `POST /pause {"table": "<name>"}` pauses the table's Kafka Connect connector (once its
//! in-flight batch is in ClickHouse) and answers with a token, its TTL and, per pipeline, the
//! next offset to ingest of each partition, read from the sink's exactly-once state. The pause
//! lasts until `POST /resume {"token"}`, or until the TTL passes without a `POST /renew`.
//!
//! Its replicas elect a leader; the others answer `503`, as does every replica while none leads.

use std::collections::BTreeMap;
use std::time::Duration;

use async_trait::async_trait;
use serde::Deserialize;
use serde_json::json;

use super::{SnapshotError, SnapshotTable};
use crate::watermark::Watermark;

/// A table's ingestion, paused.
#[derive(Debug, Clone, PartialEq)]
pub struct Paused {
    pub token: String,
    /// Resumed by the server this long after the pause or the last renewal.
    pub ttl: Duration,
    /// Per partition, the last offset in the table (the next to ingest, minus one).
    pub watermark: Watermark,
}

#[async_trait]
pub trait Pauser: Send + Sync {
    async fn pause(&self, table: &SnapshotTable) -> Result<Paused, SnapshotError>;
    async fn resume(&self, token: &str) -> Result<(), SnapshotError>;
    async fn renew(&self, token: &str) -> Result<(), SnapshotError>;
}

/// How long a request is retried while no pause server leads or can be reached: a standby takes
/// over once the leader's Keeper session times out (about 10 s).
const FAILOVER_RETRY: Duration = Duration::from_secs(30);
const RETRY_DELAY: Duration = Duration::from_secs(1);

/// The pause server over HTTP.
pub struct PauseServer {
    client: reqwest::Client,
    url: String,
}

impl PauseServer {
    /// e.g. `http://127.0.0.1:18091`
    pub fn new(url: &str) -> Self {
        Self {
            client: reqwest::Client::builder()
                // a pause waits for connectors to stop, which can take a while
                .timeout(Duration::from_secs(120))
                .build()
                .expect("an HTTP client"),
            url: url.trim_end_matches('/').to_string(),
        }
    }

    /// Retried through a failover. That's safe even if a lost request did something: whatever it
    /// paused is held by a token nobody renews, so it expires.
    async fn post(&self, path: &str, body: serde_json::Value) -> Result<String, SnapshotError> {
        let deadline = tokio::time::Instant::now() + FAILOVER_RETRY;
        let resp = loop {
            let result = self
                .client
                .post(format!("{}/{path}", self.url))
                .json(&body)
                .send()
                .await;
            let retry = match &result {
                Ok(resp) => resp.status() == reqwest::StatusCode::SERVICE_UNAVAILABLE,
                Err(e) => e.is_connect(),
            };
            if !retry || tokio::time::Instant::now() + RETRY_DELAY > deadline {
                break result.map_err(|e| SnapshotError(format!("pause server /{path}: {e}")))?;
            }
            tokio::time::sleep(RETRY_DELAY).await;
        };
        let status = resp.status();
        let text = resp
            .text()
            .await
            .map_err(|e| SnapshotError(format!("pause server /{path}: {e}")))?;
        if !status.is_success() {
            return Err(SnapshotError(format!(
                "pause server /{path} returned {status}: {}",
                text.trim()
            )));
        }
        Ok(text)
    }
}

#[async_trait]
impl Pauser for PauseServer {
    async fn pause(&self, table: &SnapshotTable) -> Result<Paused, SnapshotError> {
        let text = self.post("pause", json!({ "table": table.table })).await?;
        let response: PauseResponse = serde_json::from_str(&text)
            .map_err(|e| SnapshotError(format!("pause server /pause: unexpected answer: {e}")))?;
        let paused = response.paused(&table.topic);
        if paused.is_err() {
            // don't leave it paused until the TTL
            let _ = self.resume(&response.token).await;
        }
        paused
    }

    async fn resume(&self, token: &str) -> Result<(), SnapshotError> {
        self.post("resume", json!({ "token": token }))
            .await
            .map(|_| ())
    }

    async fn renew(&self, token: &str) -> Result<(), SnapshotError> {
        self.post("renew", json!({ "token": token }))
            .await
            .map(|_| ())
    }
}

#[derive(Debug, Deserialize)]
struct PauseResponse {
    token: String,
    ttl_seconds: u64,
    watermark: Vec<PipelineOffsets>,
}

#[derive(Debug, Deserialize)]
struct PipelineOffsets {
    connector: String,
    /// Exact: from the sink's KeeperMap state (Connect's own offsets may lag).
    offsets: Vec<KafkaOffset>,
}

#[derive(Debug, Deserialize)]
struct KafkaOffset {
    partition: KafkaPartition,
    offset: OffsetValue,
}

#[derive(Debug, Deserialize)]
struct KafkaPartition {
    kafka_topic: String,
    kafka_partition: i32,
}

#[derive(Debug, Deserialize)]
struct OffsetValue {
    /// The next offset to ingest.
    kafka_offset: i64,
}

impl PauseResponse {
    /// The table must be ingested by exactly one pipeline, from `topic`.
    fn paused(&self, topic: &str) -> Result<Paused, SnapshotError> {
        let [pipeline] = self.watermark.as_slice() else {
            let connectors: Vec<&str> = self
                .watermark
                .iter()
                .map(|p| p.connector.as_str())
                .collect();
            return Err(SnapshotError(format!(
                "the table must be ingested by exactly one connector, not {} ({})",
                connectors.len(),
                connectors.join(", ")
            )));
        };
        let mut w = BTreeMap::new();
        for o in &pipeline.offsets {
            if o.partition.kafka_topic != topic {
                return Err(SnapshotError(format!(
                    "connector {} ingests topic {}, not {topic}",
                    pipeline.connector, o.partition.kafka_topic
                )));
            }
            w.insert(o.partition.kafka_partition, o.offset.kafka_offset - 1);
        }
        Ok(Paused {
            token: self.token.clone(),
            ttl: Duration::from_secs(self.ttl_seconds),
            watermark: Watermark(w),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// As continuous-backup's pause server answers (`backup/src/pause/api.rs`).
    const RESPONSE: &str = r#"{"token":"7d1f0c1e-0000-4000-8000-000000000001","ttl_seconds":60,
        "watermark":[{"connector":"chlive-sink-events",
          "offsets":[{"partition":{"kafka_topic":"events","kafka_partition":0},"offset":{"kafka_offset":10}},
                     {"partition":{"kafka_topic":"events","kafka_partition":1},"offset":{"kafka_offset":0}}],
          "connect_offsets":[],
          "keeper_rows":[{"key":"events-0","minOffset":5,"maxOffset":9,"state":"AFTER_PROCESSING"}]}]}"#;

    #[test]
    fn next_offsets_become_the_last_offsets_held() {
        let r: PauseResponse = serde_json::from_str(RESPONSE).unwrap();
        let p = r.paused("events").unwrap();
        assert_eq!(p.token, "7d1f0c1e-0000-4000-8000-000000000001");
        assert_eq!(p.ttl, Duration::from_secs(60));
        // partition 1 has nothing: -1, so the stream starts at 0
        assert_eq!(p.watermark.describe(), "{0: 9, 1: -1}");
        assert_eq!(p.watermark.start(0), 10);
    }

    #[test]
    fn one_connector_of_the_right_topic() {
        let r: PauseResponse = serde_json::from_str(RESPONSE).unwrap();
        assert_eq!(
            r.paused("orders").unwrap_err().0,
            "connector chlive-sink-events ingests topic events, not orders"
        );
        let two = RESPONSE.replace(
            r#""watermark":[{"#,
            r#""watermark":[{"connector":"other","offsets":[]},{"#,
        );
        let r: PauseResponse = serde_json::from_str(&two).unwrap();
        assert_eq!(
            r.paused("events").unwrap_err().0,
            "the table must be ingested by exactly one connector, not 2 (other, chlive-sink-events)"
        );
    }
}
