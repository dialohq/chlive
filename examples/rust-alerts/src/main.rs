//! An event-driven service on `chlive-client`: it reacts to what changes in three live queries,
//! each through the view that fits.
//!
//! - The 5 slowest requests (`inserts`): an alert when one enters.
//! - How many requests there are (`cell`): the count as it changes, skipping counts in between
//!   when it's busy.
//! - Requests per country, the top 3 (`raw`): each country's new count and average latency,
//!   and its rank when it moves.
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
    let mut slow = client
        .query("select ts, user_id, path, latency_ms from events order by latency_ms desc limit 5")
        .inserts::<Slow>();
    let mut total = client
        .query("select count() from events")
        .cell::<u64>()
        .into_stream();
    let mut countries = client
        .query(
            "select country, count() as n, avg(latency_ms) as avg_latency from events \
             group by country order by n desc limit 3",
        )
        .raw::<CountryStats>();

    loop {
        // read them all: `inserts` and `raw` share the connection (the `cell` has a task of its own)
        tokio::select! {
            Some(r) = slow.next() => match r {
                Ok(s) => println!(
                    "ALERT: {} entered the top 5 slowest: {} took {} ms (at {})",
                    s.user_id, s.path, s.latency_ms, s.ts
                ),
                Err(e) => {
                    eprintln!("[slowest] failed: {e}");
                    break;
                }
            },
            Some(n) = total.next() => match n {
                Ok(n) => println!("{} requests", n.unwrap_or(0)),
                Err(e) => {
                    eprintln!("[total] failed: {e}");
                    break;
                }
            },
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
fn on_countries(u: Update<CountryStats>) -> bool {
    match (&u.kind, &u.status) {
        (Kind::Snapshot, _) => {
            println!("[countries] live: {} rows", u.rows.len());
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
        (Kind::Disconnected, _) => println!("[countries] disconnected; reconnecting"),
        (Kind::Resumed, _) => println!("[countries] resumed"),
        (_, Status::Error(e)) => {
            eprintln!("[countries] failed: {e}");
            return false;
        }
        _ => {}
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
