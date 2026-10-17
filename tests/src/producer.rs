//! Synthetic event producer for the e2e tests (historical seeding is `scripts/seed.sql`).
//!
//! Every message is one `JSONEachRow` row matching `default.events_queue` on main ClickHouse.

use std::collections::BTreeMap;
use std::time::Duration;

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use futures::stream::{FuturesUnordered, StreamExt};
use rand::Rng;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer as _};

const COUNTRIES: &[(&str, u32)] = &[
    ("US", 30),
    ("DE", 12),
    ("GB", 10),
    ("PL", 8),
    ("FR", 8),
    ("IN", 7),
    ("BR", 5),
    ("JP", 5),
    ("CA", 4),
    ("ES", 3),
    ("IT", 3),
    ("NL", 2),
    ("SE", 1),
    ("AU", 1),
    ("MX", 1),
];
const PATHS: &[&str] = &[
    "/",
    "/search",
    "/product",
    "/cart",
    "/checkout",
    "/login",
    "/account",
    "/api/orders",
];
const USERS: u32 = 5_000;
/// `session_id`s (Nullable(UUID)). They differ in both halves, because ClickHouse orders
/// UUIDs by the second half first.
pub const SESSIONS: &[&str] = &[
    "6f1d3a20-1b2c-4d5e-8f90-a1b2c3d4e5f6",
    "00000000-0000-0001-ffff-000000000001",
    "ffffffff-ffff-ffff-0000-000000000000",
    "0a0a0a0a-0000-4000-8000-00000000000a",
    "9b8c7d6e-5f40-4321-9abc-def012345678",
    "12345678-9abc-4def-8123-456789abcdef",
];

pub struct Producer {
    producer: FutureProducer,
    topic: String,
    partitions: i32,
}

/// Highest acknowledged offset per partition for the messages produced in one call.
pub type Offsets = BTreeMap<i32, i64>;

impl Producer {
    pub fn new(brokers: &str, topic: &str) -> Result<Self> {
        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", brokers)
            .set("linger.ms", "20")
            .set("batch.num.messages", "10000")
            .set("compression.type", "lz4")
            .set("queue.buffering.max.messages", "1000000")
            .create()
            .context("creating kafka producer")?;
        let md = producer
            .client()
            .fetch_metadata(Some(topic), Duration::from_secs(10))
            .context("fetching topic metadata")?;
        let partitions = md
            .topics()
            .first()
            .map(|t| t.partitions().len() as i32)
            .filter(|n| *n > 0)
            .with_context(|| format!("topic {topic} has no partitions"))?;
        Ok(Self {
            producer,
            topic: topic.to_string(),
            partitions,
        })
    }

    /// Produce `count` live events at up to `rate` events/s (unthrottled when `None`).
    /// Returns the highest acknowledged offset per partition.
    pub async fn produce(&self, count: u64, rate: Option<f64>) -> Result<Offsets> {
        let mut offsets = Offsets::new();
        let mut inflight = FuturesUnordered::new();
        let started = tokio::time::Instant::now();
        let mut sent: u64 = 0;
        while sent < count {
            if let Some(r) = rate {
                let due = started + Duration::from_secs_f64(sent as f64 / r);
                tokio::time::sleep_until(due).await;
            }
            let (payload, partition) = {
                let mut rng = rand::rng();
                (
                    random_event(&mut rng, Utc::now()),
                    rng.random_range(0..self.partitions),
                )
            };
            let producer = &self.producer;
            let topic = &self.topic;
            inflight.push(async move {
                producer
                    .send(
                        FutureRecord::<(), _>::to(topic)
                            .payload(&payload)
                            .partition(partition),
                        Duration::from_secs(30),
                    )
                    .await
                    .map_err(|(e, _)| anyhow::anyhow!("kafka delivery failed: {e}"))
            });
            sent += 1;
            // keep memory bounded during large seeds
            while inflight.len() >= 50_000 {
                record(&mut offsets, inflight.next().await.expect("non-empty")?);
            }
            // drain completed deliveries opportunistically
            while let Some(Some(done)) = futures::FutureExt::now_or_never(inflight.next()) {
                record(&mut offsets, done?);
            }
        }
        while let Some(done) = inflight.next().await {
            record(&mut offsets, done?);
        }
        self.producer.flush(Duration::from_secs(30))?;
        Ok(offsets)
    }
}

impl Producer {
    /// Produce these messages (JSON), spread over the partitions. Returns the highest
    /// acknowledged offset per partition.
    pub async fn produce_json(&self, messages: &[String]) -> Result<Offsets> {
        let mut offsets = Offsets::new();
        for (i, payload) in messages.iter().enumerate() {
            let d = self
                .producer
                .send(
                    FutureRecord::<(), _>::to(&self.topic)
                        .payload(payload)
                        .partition(i as i32 % self.partitions),
                    Duration::from_secs(30),
                )
                .await
                .map_err(|(e, _)| anyhow::anyhow!("kafka delivery failed: {e}"))?;
            record(&mut offsets, d);
        }
        Ok(offsets)
    }
}

impl Producer {
    /// Produce these (key, JSON) messages with their Kafka keys, so each key's messages are in
    /// one partition, in order (a keyed topic). Returns the highest acknowledged offset per
    /// partition.
    pub async fn produce_keyed(&self, messages: &[(String, String)]) -> Result<Offsets> {
        let mut offsets = Offsets::new();
        for (key, payload) in messages {
            let d = self
                .producer
                .send(
                    FutureRecord::to(&self.topic).key(key).payload(payload),
                    Duration::from_secs(30),
                )
                .await
                .map_err(|(e, _)| anyhow::anyhow!("kafka delivery failed: {e}"))?;
            record(&mut offsets, d);
        }
        Ok(offsets)
    }
}

fn record(offsets: &mut Offsets, d: rdkafka::producer::future_producer::Delivery) {
    let e = offsets.entry(d.partition).or_insert(-1);
    *e = (*e).max(d.offset);
}

pub fn random_event(rng: &mut impl Rng, ts: DateTime<Utc>) -> String {
    let total: u32 = COUNTRIES.iter().map(|c| c.1).sum();
    let mut pick = rng.random_range(0..total);
    let country = COUNTRIES
        .iter()
        .find(|(_, w)| {
            if pick < *w {
                true
            } else {
                pick -= w;
                false
            }
        })
        .map(|c| c.0)
        .unwrap_or("US");
    let path = PATHS[rng.random_range(0..PATHS.len())];
    // long-tailed latency: mostly 10-200ms, occasionally seconds
    let u: f64 = rng.random_range(0.0..1.0);
    let latency_ms = (20.0 * (-(1.0 - u).ln()).powf(1.6) * 10.0) as u32 + 1;
    let amount = if rng.random_bool(0.6) {
        0.0
    } else {
        (rng.random_range(100..50_000) as f64) / 100.0
    };
    // none for 1 in 7; otherwise in any of the forms ClickHouse reads (upper case, no dashes)
    let session = rng
        .random_range(0..=SESSIONS.len())
        .checked_sub(1)
        .map(|i| {
            let s = SESSIONS[i].to_string();
            let s = if rng.random_bool(0.3) {
                s.to_uppercase()
            } else {
                s
            };
            if rng.random_bool(0.3) {
                s.replace('-', "")
            } else {
                s
            }
        });
    serde_json::json!({
        "ts": ts.format("%Y-%m-%d %H:%M:%S%.3f").to_string(),
        "user_id": format!("u{}", rng.random_range(0..USERS)),
        "country": country,
        "path": path,
        "latency_ms": latency_ms,
        "amount": amount,
        "session_id": session,
    })
    .to_string()
}
