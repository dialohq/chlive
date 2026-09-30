//! Synthetic event producer for the `events` topic.
//!
//! Every message is one `JSONEachRow` row matching `default.events_queue` on main ClickHouse.

use std::collections::BTreeMap;
use std::time::Duration;

use anyhow::{Context, Result};
use chrono::{DateTime, Utc};
use futures::stream::{FuturesUnordered, StreamExt};
use rand::Rng;
use rdkafka::config::ClientConfig;
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};

const COUNTRIES: &[(&str, u32)] = &[
    ("US", 30), ("DE", 12), ("GB", 10), ("PL", 8), ("FR", 8), ("IN", 7), ("BR", 5), ("JP", 5),
    ("CA", 4), ("ES", 3), ("IT", 3), ("NL", 2), ("SE", 1), ("AU", 1), ("MX", 1),
];
pub const PATHS: &[&str] = &[
    "/", "/search", "/product", "/cart", "/checkout", "/login", "/account", "/api/orders",
];
const USERS: u32 = 5_000;

/// How event timestamps are chosen.
#[derive(Clone, Copy, Debug)]
pub enum TsMode {
    /// Uniformly spread over the `days` before now (historical seed).
    Backdated { days: u32 },
    /// Wall clock at production time (live stream).
    Now,
}

pub struct Generator {
    producer: FutureProducer,
    topic: String,
    partitions: i32,
}

/// Highest acknowledged offset per partition for the messages produced in one call.
pub type Offsets = BTreeMap<i32, i64>;

impl Generator {
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
        Ok(Self { producer, topic: topic.to_string(), partitions })
    }

    /// Produce `count` events (forever when `None`) at up to `rate` events/s (unthrottled
    /// when `None`). Returns the highest acknowledged offset per partition.
    pub async fn produce(&self, count: Option<u64>, rate: Option<f64>, ts: TsMode) -> Result<Offsets> {
        let mut offsets = Offsets::new();
        let mut inflight = FuturesUnordered::new();
        let started = tokio::time::Instant::now();
        let mut sent: u64 = 0;
        while count.is_none_or(|c| sent < c) {
            if let Some(r) = rate {
                let due = started + Duration::from_secs_f64(sent as f64 / r);
                tokio::time::sleep_until(due).await;
            }
            let (payload, partition) = {
                let mut rng = rand::rng();
                (random_event(&mut rng, ts, Utc::now()), rng.random_range(0..self.partitions))
            };
            let producer = &self.producer;
            let topic = &self.topic;
            inflight.push(async move {
                producer
                    .send(
                        FutureRecord::<(), _>::to(topic).payload(&payload).partition(partition),
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
            if sent % 100_000 == 0 {
                tracing::info!(sent, "produced");
            }
        }
        while let Some(done) = inflight.next().await {
            record(&mut offsets, done?);
        }
        self.producer.flush(Duration::from_secs(30))?;
        Ok(offsets)
    }
}

fn record(offsets: &mut Offsets, d: rdkafka::producer::future_producer::Delivery) {
    let e = offsets.entry(d.partition).or_insert(-1);
    *e = (*e).max(d.offset);
}

pub fn random_event(rng: &mut impl Rng, ts: TsMode, now: DateTime<Utc>) -> String {
    let ts = match ts {
        TsMode::Now => now,
        TsMode::Backdated { days } => {
            now - chrono::Duration::milliseconds(rng.random_range(0..i64::from(days) * 86_400_000))
        }
    };
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
    serde_json::json!({
        "ts": ts.format("%Y-%m-%d %H:%M:%S%.3f").to_string(),
        "user_id": format!("u{}", rng.random_range(0..USERS)),
        "country": country,
        "path": path,
        "latency_ms": latency_ms,
        "amount": amount,
    })
    .to_string()
}
