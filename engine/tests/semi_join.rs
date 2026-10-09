//! `IN (SELECT …)` semi-joins without ClickHouse or Kafka: fake snapshots and history, events
//! in memory.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use chlive_engine::diff::Row;
use chlive_engine::history::{DescribedColumn, HistoryError, HistorySource};
use chlive_engine::rules::HardcodedRules;
use chlive_engine::snapshot::{Lease, Snapshot, SnapshotError, SnapshotTable, Snapshots};
use chlive_engine::source::MemorySource;
use chlive_engine::watermark::Watermark;
use chlive_engine::{Engine, Iteration};
use futures::StreamExt;
use serde_json::json;

fn row(v: serde_json::Value) -> Row {
    v.as_object().unwrap().clone()
}

/// `events` (ts, user_id) and `users` (id, country). The events snapshot holds offsets 0..=1 of
/// partition 0 (`h1` of u1, `h2` of u2); after the first, later snapshots hold up to offset 2
/// (`e1` of u3). The users snapshot holds offset 0 (u1, PL).
#[derive(Clone, Default)]
struct Fake {
    events_snapshots: Arc<AtomicUsize>,
    /// Snapshots after the first are newer (offset 2); otherwise always the first.
    newer_later: bool,
    /// Snapshots after the first are ahead of everything the stream will read (offset 9).
    ahead_later: bool,
    queries: Arc<Mutex<Vec<String>>>,
}

#[async_trait]
impl Snapshots for Fake {
    async fn acquire(&self, table: &SnapshotTable) -> Result<Lease, SnapshotError> {
        let (name, w) = match table.table.as_str() {
            "events" => {
                let n = self.events_snapshots.fetch_add(1, Ordering::SeqCst);
                if n > 0 && self.newer_later {
                    ("events_v2", Watermark([(0, 2)].into()))
                } else if n > 0 && self.ahead_later {
                    ("events_v9", Watermark([(0, 9)].into()))
                } else {
                    ("events_v1", Watermark([(0, 1)].into()))
                }
            }
            _ => ("users_v1", Watermark([(0, 0)].into())),
        };
        Ok(Arc::new(Snapshot::fixed(&table.database, name, w)))
    }
}

#[async_trait]
impl HistorySource for Fake {
    async fn describe(&self, table: &str) -> Result<Vec<DescribedColumn>, HistoryError> {
        let col = |n: &str, ty: &str| DescribedColumn {
            name: n.into(),
            ty: ty.into(),
        };
        match table {
            "default.events" => Ok(vec![col("ts", "String"), col("user_id", "String")]),
            "default.users" => Ok(vec![col("id", "String"), col("country", "String")]),
            _ => Err(HistoryError::UnknownTable(table.into())),
        }
    }

    async fn fetch(&self, sql: &str) -> Result<Vec<Row>, HistoryError> {
        self.queries.lock().unwrap().push(sql.to_string());
        match sql {
            "SELECT ts, user_id FROM default.events_v1 \
             WHERE user_id IN (SELECT id FROM default.users_v1 WHERE country = 'PL')" => {
                Ok(vec![row(json!({"ts": "h1", "user_id": "u1"}))])
            }
            "SELECT DISTINCT id AS k FROM default.users_v1 WHERE country = 'PL'" => {
                Ok(vec![row(json!({"k": "u1"}))])
            }
            // back-fills: u3's rows in a snapshot of events (an older one, then e1 too)
            "SELECT ts, user_id FROM default.events_v1 WHERE user_id IN ('u3')" => {
                Ok(vec![row(json!({"ts": "h0", "user_id": "u3"}))])
            }
            "SELECT ts, user_id FROM default.events_v2 WHERE user_id IN ('u3')" => Ok(vec![
                row(json!({"ts": "h0", "user_id": "u3"})),
                row(json!({"ts": "e1", "user_id": "u3"})),
            ]),
            other => panic!("unexpected query: {other}"),
        }
    }
}

/// Events: the snapshot's two, then e1 (u3, not in S yet) and e2 (u1). Users: u1 (snapshot),
/// then u3 in PL (enters S), u2 in DE (doesn't). Outer topic first, as subscribed.
fn source() -> MemorySource {
    let mut s = MemorySource::new();
    s.push("events", 0, r#"{"ts": "h1", "user_id": "u1"}"#)
        .push("events", 0, r#"{"ts": "h2", "user_id": "u2"}"#)
        .push("events", 0, r#"{"ts": "e1", "user_id": "u3"}"#)
        .push("events", 0, r#"{"ts": "e2", "user_id": "u1"}"#)
        .push("users", 0, r#"{"id": "u1", "country": "PL"}"#)
        .push("users", 0, r#"{"id": "u3", "country": "PL"}"#)
        .push("users", 0, r#"{"id": "u2", "country": "DE"}"#);
    s
}

const SQL: &str =
    "select ts, user_id from events where user_id in (select id from users where country = 'PL')";

async fn run(fake: &Fake, refresh: Duration) -> Vec<String> {
    let engine = Engine::new(
        HardcodedRules::default(),
        fake.clone(),
        source(),
        fake.clone(),
    )
    .with_max_batch(1)
    .with_semi_refresh(refresh);
    let s = engine.subscribe(SQL).await.unwrap();
    assert!(s.plan.semi.is_some());
    let iterations: Vec<Iteration> = s.map(|i| i.unwrap()).collect().await;
    iterations.iter().map(|i| i.to_string()).collect()
}

#[tokio::test]
async fn kept_back_rows_come_when_their_key_enters() {
    let fake = Fake::default();
    let lines = run(&fake, Duration::from_secs(3600)).await;
    assert_eq!(
        lines,
        [
            // history: the join over both snapshots
            "+ {\"ts\":\"h1\",\"user_id\":\"u1\"}\nhistory: 1 rows",
            // e1: u3 isn't in S yet: kept back
            "",
            // e2: u1 is
            "events/0:3 + {\"ts\":\"e2\",\"user_id\":\"u1\"}",
            // u3 enters: its rows up to the snapshot (h0), then the kept-back e1
            "users/0:1 + {\"ts\":\"h0\",\"user_id\":\"u3\"}\nusers/0:1 + {\"ts\":\"e1\",\"user_id\":\"u3\"}",
            // u2 is in DE: nothing
            "",
        ]
    );
}

#[tokio::test]
async fn a_newer_snapshot_replaces_the_tail() {
    // a new snapshot of events (up to e1) at every batch: once the stream is past it, back-fills
    // read it, and e1 leaves the tail, so it comes once, from the snapshot
    let fake = Fake {
        newer_later: true,
        ..Default::default()
    };
    let lines = run(&fake, Duration::ZERO).await;
    assert_eq!(
        lines[3],
        "users/0:1 + {\"ts\":\"h0\",\"user_id\":\"u3\"}\nusers/0:1 + {\"ts\":\"e1\",\"user_id\":\"u3\"}"
    );
    let queries = fake.queries.lock().unwrap();
    assert!(
        queries
            .iter()
            .any(|q| q.contains("events_v2 WHERE user_id IN ('u3')")),
        "{queries:?}"
    );
}

#[tokio::test]
async fn not_while_the_newer_snapshot_is_ahead_of_the_stream() {
    // ClickHouse ahead of the engine: back-filling from that snapshot would send rows the
    // stream sends again, so the run keeps the one it has
    let fake = Fake {
        ahead_later: true,
        ..Default::default()
    };
    let lines = run(&fake, Duration::ZERO).await;
    assert_eq!(lines.join("\n").matches("\"e1\"").count(), 1);
    let queries = fake.queries.lock().unwrap();
    assert!(
        queries
            .iter()
            .any(|q| q.contains("events_v1 WHERE user_id IN ('u3')")),
        "{queries:?}"
    );
    assert!(
        !queries.iter().any(|q| q.contains("events_v9")),
        "{queries:?}"
    );
}
