//! Tail ingestion: the server consumes Kafka itself and appends raw rows to
//! `tail_<table>` in the cache, starting right after the startup watermark `W0`.

use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};
use std::time::Duration;

use anyhow::{Context, Result};
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::{ClientConfig, Message, Offset, TopicPartitionList};
use serde_json::Value;
use tokio::sync::broadcast;
use tracing::{error, info, warn};

use crate::cache_engine::CacheEngine;
use crate::ch_http::{ChHttp, Params};
use crate::config::KafkaConfig;
use crate::rewriter::{quote_ident, TableSchema};
use crate::watermark::{max_offsets_sql, Watermark};

/// Published after every successful insert into `tail_<table>`.
#[derive(Debug, Clone)]
pub struct TableAdvanced {
    pub table: String,
    pub max_offsets: Watermark,
}

/// Per-table runtime info shared with the subscription manager.
#[derive(Debug)]
pub struct TableRuntime {
    pub schema: TableSchema,
    pub topic: String,
    pub partitions: u32,
    /// Startup watermark; the tail holds every row after it.
    pub w0: Watermark,
    /// Highest offset per partition inserted into the tail so far.
    pub tail_max: RwLock<Watermark>,
}

pub fn tail_table(table: &str) -> String {
    format!("tail_{table}")
}

pub async fn describe(main: &ChHttp, table: &str) -> Result<TableSchema> {
    let r = main
        .query(&format!("DESCRIBE TABLE {}.{}", main.database(), quote_ident(table)), &Params::new())
        .await
        .with_context(|| format!("describing {table} on main"))?;
    let columns: Vec<(String, String)> = r
        .data
        .iter()
        .map(|row| {
            (
                row["name"].as_str().unwrap_or_default().to_string(),
                row["type"].as_str().unwrap_or_default().to_string(),
            )
        })
        .collect();
    for required in ["_partition", "_offset"] {
        anyhow::ensure!(
            columns.iter().any(|(n, _)| n == required),
            "source table {table} must have a {required} column (see NOTES.md assumptions)"
        );
    }
    Ok(TableSchema { name: table.to_string(), columns })
}

/// Current `max(_offset)` per partition on main.
pub async fn fetch_watermark(main: &ChHttp, table: &str, partitions: u32) -> Result<Watermark> {
    let r = main
        .query(&max_offsets_sql(main.database(), table), &Params::new())
        .await
        .with_context(|| format!("fetching watermark of {table}"))?;
    let mut max = BTreeMap::new();
    for row in &r.data {
        let p = row.get("p").and_then(Value::as_u64).context("bad _partition")? as u32;
        let o = row.get("o").and_then(Value::as_i64).context("bad max(_offset)")?;
        max.insert(p, o);
    }
    Ok(Watermark::from_max_offsets(partitions, &max))
}

/// Drop and recreate `tail_<table>` with the source schema.
pub async fn create_tail_table(cache: &dyn CacheEngine, schema: &TableSchema) -> Result<()> {
    let t = tail_table(&schema.name);
    cache.exec(&format!("DROP TABLE IF EXISTS {t} SYNC"), &Params::new()).await?;
    let cols: Vec<String> = schema.columns.iter().map(|(n, ty)| format!("{} {ty}", quote_ident(n))).collect();
    cache
        .exec(
            &format!("CREATE TABLE {t} ({}) ENGINE = MergeTree ORDER BY (_partition, _offset)", cols.join(", ")),
            &Params::new(),
        )
        .await
        .with_context(|| format!("creating {t}"))
}

fn consumer(kafka: &KafkaConfig) -> Result<StreamConsumer> {
    ClientConfig::new()
        .set("bootstrap.servers", &kafka.brokers)
        // rdkafka requires a group id even with manual assignment; nothing is ever committed.
        .set("group.id", format!("chlive-tail-{}", std::process::id()))
        .set("enable.auto.commit", "false")
        .set("enable.auto.offset.store", "false")
        .set("enable.partition.eof", "false")
        .set("fetch.wait.max.ms", "10")
        .create()
        .context("creating kafka consumer")
}

pub fn partition_count(kafka: &KafkaConfig, topic: &str) -> Result<u32> {
    let c = consumer(kafka)?;
    let md = c.fetch_metadata(Some(topic), Duration::from_secs(10)).context("fetching kafka metadata")?;
    let n = md.topics().first().map(|t| t.partitions().len()).unwrap_or(0) as u32;
    anyhow::ensure!(n > 0, "topic {topic} has no partitions");
    Ok(n)
}

/// Consume `rt.topic` from `W0 + 1` and append into the tail, forever.
pub fn spawn(
    kafka: KafkaConfig,
    cache: Arc<dyn CacheEngine>,
    rt: Arc<TableRuntime>,
    tx: broadcast::Sender<TableAdvanced>,
) -> Result<tokio::task::JoinHandle<()>> {
    let consumer = consumer(&kafka)?;
    let mut tpl = TopicPartitionList::new();
    for p in 0..rt.partitions {
        tpl.add_partition_offset(&rt.topic, p as i32, Offset::Offset(rt.w0.get(p) + 1))?;
    }
    consumer.assign(&tpl).context("assigning partitions")?;
    info!(table = rt.schema.name, w0 = ?rt.w0.0, "tail consumer assigned");
    Ok(tokio::spawn(async move {
        run(consumer, kafka, cache, rt, tx).await;
    }))
}

async fn run(
    consumer: StreamConsumer,
    kafka: KafkaConfig,
    cache: Arc<dyn CacheEngine>,
    rt: Arc<TableRuntime>,
    tx: broadcast::Sender<TableAdvanced>,
) {
    let table = rt.schema.name.clone();
    let tail = tail_table(&table);
    let window = Duration::from_millis(kafka.batch_ms);
    loop {
        let mut body = String::new();
        let mut n = 0usize;
        let mut batch_max: BTreeMap<u32, i64> = BTreeMap::new();
        let mut deadline = None;
        while n < kafka.batch_rows {
            let recv = consumer.recv();
            let msg = match deadline {
                None => recv.await,
                Some(d) => match tokio::time::timeout_at(d, recv).await {
                    Ok(m) => m,
                    Err(_) => break,
                },
            };
            let msg = match msg {
                Ok(m) => m,
                Err(e) => {
                    warn!(error = %e, "kafka error");
                    continue;
                }
            };
            if deadline.is_none() {
                deadline = Some(tokio::time::Instant::now() + window);
            }
            let (p, o) = (msg.partition() as u32, msg.offset());
            let mut row = match msg.payload().map(serde_json::from_slice::<serde_json::Map<String, Value>>) {
                Some(Ok(r)) => r,
                other => {
                    // Main's Kafka engine would reject the same message; see NOTES.md.
                    error!(partition = p, offset = o, ?other, "skipping undecodable message");
                    continue;
                }
            };
            row.insert("_partition".into(), p.into());
            row.insert("_offset".into(), o.into());
            body.push_str(&Value::Object(row).to_string());
            body.push('\n');
            n += 1;
            let e = batch_max.entry(p).or_insert(-1);
            *e = (*e).max(o);
        }
        if n == 0 {
            continue;
        }
        insert_with_retry(cache.as_ref(), &tail, body).await;
        let max_offsets = {
            let mut w = rt.tail_max.write().expect("tail_max lock");
            for (p, o) in batch_max {
                let e = w.0.entry(p).or_insert(-1);
                *e = (*e).max(o);
            }
            w.clone()
        };
        tracing::debug!(table, rows = n, "tail advanced");
        // No receivers is fine (no subscriptions yet).
        let _ = tx.send(TableAdvanced { table: table.clone(), max_offsets });
    }
}

async fn insert_with_retry(cache: &dyn CacheEngine, tail: &str, body: String) {
    let mut backoff = Duration::from_millis(100);
    loop {
        match cache.insert_json_each_row(tail, body.clone()).await {
            Ok(()) => return,
            Err(e) => {
                error!(error = %e, table = tail, "tail insert failed; retrying");
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(5));
            }
        }
    }
}
