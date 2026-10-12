//! An event-driven service on `chlive-client`: it reacts to what changes in two live queries.
//!
//! - The 5 slowest requests: an alert when one enters, a note when one is pushed out.
//! - Requests per country (the top 3): each country's new count and average latency, and its
//!   rank when it moves.
//!
//! `cargo run -p chlive-example-rust-alerts` (against `CHLIVE_URL`, e.g. `dnvr-state get server.url`),
//! then e.g. `produce events '{"user_id": "u7", "path": "/checkout", "latency_ms": 99999}'`.

use chlive_client::{Change, ClientOptions, Kind, LiveClient, Status, Update};
use futures::StreamExt;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Slow {
    ts: String,
    user_id: String,
    path: String,
    latency_ms: u32,
}

#[derive(Debug, Deserialize)]
struct CountryStats {
    country: String,
    n: u64,
    avg_latency: f64,
}

#[tokio::main]
async fn main() {
    let url = std::env::var("CHLIVE_URL").unwrap_or_else(|_| "ws://127.0.0.1:8080/live".into());
    let client = LiveClient::new(ClientOptions::new(url));
    let mut slowest = client.subscribe::<Slow>(
        "select ts, user_id, path, latency_ms from events order by latency_ms desc limit 5",
    );
    let mut countries = client.subscribe::<CountryStats>(
        "select country, count() as n, avg(latency_ms) as avg_latency from events \
         group by country order by n desc limit 3",
    );

    loop {
        // read both: they share one connection
        tokio::select! {
            Some(u) = slowest.next() => {
                if !on_slowest(u) { break; }
            }
            Some(u) = countries.next() => {
                if !on_countries(u) { break; }
            }
            _ = tokio::signal::ctrl_c() => break,
            else => break,
        }
    }
    client.close().await;
}

/// Whether to go on.
fn status<R>(what: &str, u: &Update<R>) -> bool {
    match (&u.kind, &u.status) {
        (Kind::Snapshot, _) => println!("[{what}] live: {} rows", u.rows.len()),
        (Kind::Disconnected, _) => println!("[{what}] disconnected; reconnecting"),
        (_, Status::Error(e)) => {
            eprintln!("[{what}] failed: {e}");
            return false;
        }
        _ => {}
    }
    true
}

fn on_slowest(u: Update<Slow>) -> bool {
    if !status("slowest", &u) {
        return false;
    }
    if u.kind == Kind::Snapshot {
        for (i, r) in u.rows.iter().enumerate() {
            println!(
                "  #{} {} {} ({} ms at {})",
                i + 1,
                r.user_id,
                r.path,
                r.latency_ms,
                r.ts
            );
        }
    }
    for change in &u.changes {
        match change {
            Change::Insert { row, index, .. } => println!(
                "ALERT: {} entered the top 5 slowest at #{}: {} took {} ms",
                row.user_id,
                index.unwrap_or(0) + 1,
                row.path,
                row.latency_ms
            ),
            Change::Delete { row, .. } => println!(
                "  {} ({} ms) dropped out of the top 5",
                row.user_id, row.latency_ms
            ),
            Change::Update { .. } => {}
        }
    }
    true
}

fn on_countries(u: Update<CountryStats>) -> bool {
    if !status("countries", &u) {
        return false;
    }
    if u.kind == Kind::Snapshot {
        for (i, r) in u.rows.iter().enumerate() {
            println!(
                "  #{} {}: {} requests, {:.1} ms on average",
                i + 1,
                r.country,
                r.n,
                r.avg_latency
            );
        }
    }
    for change in &u.changes {
        match change {
            Change::Update { row, index, .. } => {
                print!(
                    "  {}: {} requests, {:.1} ms on average",
                    row.country, row.n, row.avg_latency
                );
                match index {
                    Some(i) => println!(", now #{}", i + 1),
                    None => println!(),
                }
            }
            Change::Insert { row, index, .. } => println!(
                "  {} entered the top 3 at #{} ({} requests)",
                row.country,
                index.unwrap_or(0) + 1,
                row.n
            ),
            Change::Delete { row, .. } => println!("  {} left the top 3", row.country),
        }
    }
    true
}
