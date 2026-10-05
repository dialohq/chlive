//! What do live aggregates cost, natively and in chDB? Needs `dnvr up` (ClickHouse on
//! 18123/19000).
//!
//!     cargo run --release -p chlive-engine --example agg_bench
//!
//! 1. chDB per batch: adding a batch's states (INSERT … SELECT …State() FROM format(…)) and
//!    reading the touched groups back finalized (…Merge), by batch size and group count.
//! 2. The same aggregation hand-rolled (a HashMap of count/sum per group, finalized into rows):
//!    the floor for any native implementation.
//! 3. End to end through the engine: events in, diffs out, with the native states and with
//!    chDB only, against a plain WHERE query over the same events.
//! 4. Subscribing: the states of 1M history rows and the first snapshot, natively (ClickHouse
//!    computes the states' parts over HTTP) and in chDB (remote() → chDB), against main
//!    ClickHouse answering the GROUP BY itself.

use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

use chlive_engine::chdb::Chdb;
use chlive_engine::diff::Row;
use chlive_engine::history::{ClickHouseHttp, HistorySource};
use chlive_engine::rules::HardcodedRules;
use chlive_engine::source::MemorySource;
use chlive_engine::{Aggregates, Engine};
use futures::StreamExt;
use serde_json::{json, Value};

const URL: &str = "http://127.0.0.1:18123";
const DB: &str = "chlive_bench";
const HISTORY_ROWS: u64 = 1_000_000;

async fn exec(sql: &str) {
    let r = reqwest::Client::new()
        .post(URL)
        .body(sql.to_string())
        .send()
        .await
        .unwrap();
    assert!(
        r.status().is_success(),
        "{sql}: {}",
        r.text().await.unwrap()
    );
}

/// A stream event: `k10` has 10 distinct values, `k10k` 10 000; `x` is 0..999.
fn event(i: u64) -> String {
    format!(
        r#"{{"ts":"2026-10-05 12:00:00.000","k10":"k{}","k10k":"k{}","x":{}}}"#,
        i % 10,
        (i * 7919) % 10_000,
        (i * 31) % 1000
    )
}

fn engine(events: u64, max_batch: usize, aggregates: Aggregates) -> Engine {
    let mut source = MemorySource::new();
    // history is partition 0 in ClickHouse; the stream's events are partition 1
    source.partition("events", 1);
    for i in 0..events {
        source.push("events", 1, event(i));
    }
    Engine::new(
        HardcodedRules {
            database: DB.into(),
        },
        ClickHouseHttp::new(URL, "default", ""),
        source,
    )
    .with_max_batch(max_batch)
    .with_aggregates(aggregates)
}

/// Events through the engine, after the history: the time and the number of diffs.
async fn run(sql: &str, events: u64, max_batch: usize, aggregates: Aggregates) -> (Duration, u64) {
    let engine = engine(events, max_batch, aggregates);
    let mut sub = engine.subscribe(sql).await.unwrap();
    let _history = sub.next().await.unwrap().unwrap();
    let started = Instant::now();
    let mut diffs = 0u64;
    while let Some(it) = sub.next().await {
        diffs += it.unwrap().diffs.len() as u64;
    }
    (started.elapsed(), diffs)
}

fn per(d: Duration, n: u64) -> String {
    let ns = d.as_nanos() as f64 / n as f64;
    if ns >= 1_000_000.0 {
        format!("{:.2} ms", ns / 1_000_000.0)
    } else if ns >= 1_000.0 {
        format!("{:.1} µs", ns / 1_000.0)
    } else {
        format!("{ns:.0} ns")
    }
}

fn rate(d: Duration, n: u64) -> String {
    let r = n as f64 / d.as_secs_f64();
    if r >= 1e6 {
        format!("{:.1} M/s", r / 1e6)
    } else if r >= 1e3 {
        format!("{:.1} k/s", r / 1e3)
    } else {
        format!("{r:.0} /s")
    }
}

/// The native baseline: per batch, update a HashMap of (count, sum) per group, then finalize
/// the touched groups into rows and diff them against the last sent ones, like the engine does.
fn native(keys: &[String], xs: &[u64], batch: usize, with_avg: bool) -> (Duration, u64) {
    let started = Instant::now();
    let mut groups: HashMap<&str, (u64, u64)> = HashMap::new();
    let mut sent: HashMap<&str, Row> = HashMap::new();
    let mut diffs = 0;
    for (ks, xs) in keys.chunks(batch).zip(xs.chunks(batch)) {
        let mut touched = HashSet::new();
        for (k, x) in ks.iter().zip(xs) {
            let g = groups.entry(k.as_str()).or_default();
            g.0 += 1;
            g.1 += x;
            touched.insert(k.as_str());
        }
        for k in touched {
            let (c, s) = groups[k];
            let mut row = Row::new();
            row.insert("count()".into(), json!(c));
            if with_avg {
                row.insert("avg(x)".into(), json!(s as f64 / c as f64));
            }
            if sent.get(k) != Some(&row) {
                sent.insert(k, row);
                diffs += 1;
            }
        }
    }
    (started.elapsed(), diffs)
}

#[tokio::main]
async fn main() {
    println!("setting up {DB}.events with {HISTORY_ROWS} history rows…");
    exec(&format!("DROP DATABASE IF EXISTS {DB}")).await;
    exec(&format!("CREATE DATABASE {DB}")).await;
    exec(&format!(
        "CREATE TABLE {DB}.events (ts DateTime64(3), k10 String, k10k String, x UInt32, \
         _partition UInt64, _offset UInt64) ENGINE = MergeTree ORDER BY tuple()"
    ))
    .await;
    exec(&format!(
        "INSERT INTO {DB}.events SELECT toDateTime64('2026-10-05 11:00:00', 3), \
         concat('k', toString(number % 10)), concat('k', toString(number * 7919 % 10000)), \
         number * 31 % 1000, 0, number FROM numbers({HISTORY_ROWS})"
    ))
    .await;
    let chdb = Chdb::shared().unwrap();

    // ---------------------------------------------------------------- 1 + 2: per batch
    println!("\n## 1. chDB per batch vs hand-rolled native (aggregation only)\n");
    println!("| query | groups | batch | chDB insert | chDB read | chDB per event | chDB events/s | native per event | native events/s | slowdown |");
    println!("|---|---|---|---|---|---|---|---|---|---|");
    let cases: &[(&str, &str, &str, bool)] = &[
        ("count()", "select count() from events", "", false),
        ("avg(x)", "select avg(x) from events", "", true),
        (
            "k, count(), avg(x)",
            "select k10, count(), avg(x) from events group by k10",
            "k10",
            true,
        ),
        (
            "k, count(), avg(x)",
            "select k10k, count(), avg(x) from events group by k10k",
            "k10k",
            true,
        ),
    ];
    for (label, sql, key, with_avg) in cases {
        let groups = match *key {
            "" => 1,
            "k10" => 10,
            _ => 10_000,
        };
        let plan = engine(0, 1, Aggregates::ChdbOnly)
            .explain(sql)
            .await
            .unwrap();
        let agg = plan.aggregate.clone().unwrap();
        for batch in [1usize, 10, 100, 1000] {
            let table = format!("bench_{}_{batch}", key.replace(' ', ""));
            let remote = agg.remote("127.0.0.1:19000", "default", "");
            chdb.exec(format!("DROP TABLE IF EXISTS {table}"))
                .await
                .unwrap();
            chdb.exec(agg.create_sql(&table, &remote, "1"))
                .await
                .unwrap();
            // enough batches for ~a second, at least 20k events when batching
            let batches = match batch {
                1 => 2000,
                10 => 1000,
                100 => 300,
                _ => 100,
            };
            let n = (batches * batch) as u64;
            let rows: Vec<Value> = (0..n)
                .map(|i| serde_json::from_str(&event(i)).unwrap())
                .collect();
            let (mut insert, mut read) = (Duration::ZERO, Duration::ZERO);
            for chunk in rows.chunks(batch) {
                let mut lines = String::new();
                for r in chunk {
                    let mut o = serde_json::Map::new();
                    for (_, name) in &agg.batch_columns {
                        o.insert(name.clone(), r[name.as_str()].clone());
                    }
                    if o.is_empty() {
                        o.insert("__one".into(), json!(1));
                    }
                    lines.push_str(&Value::Object(o).to_string());
                    lines.push('\n');
                }
                let t = Instant::now();
                chdb.exec(agg.insert_sql(&table, &lines)).await.unwrap();
                insert += t.elapsed();
                let t = Instant::now();
                let got = chdb.rows(agg.touched_sql(&table, &lines)).await.unwrap();
                read += t.elapsed();
                assert!(!got.is_empty());
            }
            chdb.exec(format!("DROP TABLE {table}")).await.unwrap();
            let keys: Vec<String> = rows
                .iter()
                .map(|r| {
                    r.get(*key)
                        .and_then(Value::as_str)
                        .unwrap_or("")
                        .to_string()
                })
                .collect();
            let xs: Vec<u64> = rows.iter().map(|r| r["x"].as_u64().unwrap()).collect();
            // native is fast: repeat it to get a stable time
            let reps = 50;
            let mut native_total = Duration::ZERO;
            for _ in 0..reps {
                native_total += native(&keys, &xs, batch, *with_avg).0;
            }
            let native_time = native_total / reps;
            let total = insert + read;
            println!(
                "| {label} | {groups} | {batch} | {} | {} | {} | {} | {} | {} | {:.0}× |",
                per(insert, batches as u64),
                per(read, batches as u64),
                per(total, n),
                rate(total, n),
                per(native_time, n),
                rate(native_time, n),
                total.as_secs_f64() / native_time.as_secs_f64()
            );
        }
    }

    // ---------------------------------------------------------------- 3: end to end
    println!("\n## 3. End to end through the engine (decode, filter, aggregate, diff)\n");
    println!("| query | max batch | native events/s | native per event | chDB events/s | chDB per event | native vs chDB | diffs |");
    println!("|---|---|---|---|---|---|---|---|");
    let e2e: &[(&str, usize)] = &[
        ("select k10, x from events where x > 500", 1000),
        ("select count() from events", 1),
        ("select count() from events", 1000),
        ("select k10, count(), avg(x) from events group by k10", 1),
        ("select k10, count(), avg(x) from events group by k10", 1000),
        ("select k10k, count(), avg(x) from events group by k10k", 1),
        (
            "select k10k, count(), avg(x) from events group by k10k",
            1000,
        ),
        (
            "select k10k, count(), sum(x), min(x), max(x) from events group by k10k",
            1000,
        ),
    ];
    for (sql, max_batch) in e2e {
        // chDB one event at a time is slow: fewer events
        let chdb_n = if *max_batch == 1 { 5_000 } else { 200_000 };
        let native_n = 1_000_000;
        let (native_t, native_diffs) = run(sql, native_n, *max_batch, Aggregates::Auto).await;
        let (chdb_t, chdb_diffs) = run(sql, chdb_n, *max_batch, Aggregates::ChdbOnly).await;
        let speedup =
            (chdb_t.as_secs_f64() / chdb_n as f64) / (native_t.as_secs_f64() / native_n as f64);
        println!(
            "| `{sql}` | {max_batch} | {} | {} | {} | {} | {speedup:.0}× | {native_diffs} / {chdb_diffs} |",
            rate(native_t, native_n),
            per(native_t, native_n),
            rate(chdb_t, chdb_n),
            per(chdb_t, chdb_n),
        );
    }

    // ---------------------------------------------------------------- 4: subscribing
    println!("\n## 4. Subscribing over {HISTORY_ROWS} history rows\n");
    println!("| query | native: states over HTTP + snapshot | chDB: states via remote() + snapshot | ClickHouse GROUP BY directly |");
    println!("|---|---|---|---|");
    let direct = ClickHouseHttp::new(URL, "default", "");
    for sql in [
        "select count() from events",
        "select k10, count(), avg(x) from events group by k10",
        "select k10k, count(), avg(x) from events group by k10k",
    ] {
        let mut times = Vec::new();
        for aggregates in [Aggregates::Auto, Aggregates::ChdbOnly] {
            let engine = engine(0, 1000, aggregates);
            let t = Instant::now();
            let mut sub = engine.subscribe(sql).await.unwrap();
            let history = sub.next().await.unwrap().unwrap();
            times.push((t.elapsed(), history.diffs.len()));
        }
        let t = Instant::now();
        let rows = direct
            .fetch(&sql.replace("from events", &format!("from {DB}.events")))
            .await
            .unwrap();
        let plain = t.elapsed();
        assert!(times.iter().all(|(_, n)| *n == rows.len()));
        println!(
            "| `{sql}` | {:.0} ms | {:.0} ms | {:.0} ms |",
            times[0].0.as_secs_f64() * 1000.0,
            times[1].0.as_secs_f64() * 1000.0,
            plain.as_secs_f64() * 1000.0
        );
    }

    exec(&format!("DROP DATABASE {DB}")).await;
    tokio::time::sleep(Duration::from_millis(200)).await; // let the subscriptions' tables drop
    chlive_engine::chdb::shutdown();
}
