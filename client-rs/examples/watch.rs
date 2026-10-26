//! Print a live query's updates: `cargo run -p chlive-client --example watch -- "<sql>"`
//! (against `CHLIVE_URL`, by default ws://127.0.0.1:8080/live; the dev server: `dnvr-state get server.url`).

use chlive_client::{Change, ClientOptions, Kind, LiveClient, Row, Status};
use futures::StreamExt;

#[tokio::main]
async fn main() {
    let sql = std::env::args().nth(1).expect("usage: watch \"<sql>\"");
    let url = std::env::var("CHLIVE_URL").unwrap_or_else(|_| "ws://127.0.0.1:8080/live".into());
    let client = LiveClient::new(ClientOptions::new(url));
    let mut sub = client.subscribe::<Row>(sql);
    while let Some(u) = sub.next().await {
        match u.kind {
            Kind::Subscribed => println!("subscribed as {}", u.stats.id),
            Kind::Snapshot => {
                for row in u.rows.iter() {
                    println!("{}", json(row));
                }
                println!("snapshot: {} rows", u.rows.len());
            }
            Kind::Resumed => println!("resumed"),
            Kind::Diff => {
                for c in &u.changes {
                    match c {
                        Change::Insert {
                            row,
                            index: Some(i),
                            ..
                        } => println!("+ @{i} {}", json(row)),
                        Change::Insert { row, .. } => println!("+ {}", json(row)),
                        Change::Delete { index, row } => println!("- @{index} {}", json(row)),
                        Change::Update {
                            id,
                            row,
                            index: Some(i),
                        } => println!("~ {id} @{i} {}", json(row)),
                        Change::Update { id, row, .. } => println!("~ {id} {}", json(row)),
                    }
                }
            }
            Kind::Disconnected => println!("disconnected; reconnecting"),
            Kind::Error => {
                if let Status::Error(e) = &u.status {
                    eprintln!("{e}");
                }
                std::process::exit(1);
            }
        }
    }
}

fn json(row: &Row) -> String {
    serde_json::to_string(row).expect("rows are JSON")
}
