//! Checkpoints: a subscription checkpointed after any iteration and restored (on a new engine,
//! as after a restart) continues exactly as the uninterrupted one does, for every shape that
//! has checkpoints.

use std::sync::Arc;

use async_trait::async_trait;
use chlive_engine::diff::{ResultList, Row};
use chlive_engine::history::{DescribedColumn, HistoryError, HistorySource};
use chlive_engine::rules::ConfiguredRules;
use chlive_engine::snapshot::{Lease, Snapshot, SnapshotError, SnapshotTable, Snapshots};
use chlive_engine::source::MemorySource;
use chlive_engine::table_engine::{Key, TableEngine};
use chlive_engine::watermark::Watermark;
use chlive_engine::{Checkpoint, Engine, EngineError, Iteration, Params};
use futures::StreamExt;

/// `requests`; the snapshot is empty, so every row comes from the events (and the states are
/// built by them).
struct EmptyHistory;

#[async_trait]
impl HistorySource for EmptyHistory {
    async fn describe(&self, table: &str) -> Result<Vec<DescribedColumn>, HistoryError> {
        let col = |n: &str, ty: &str| DescribedColumn {
            name: n.into(),
            ty: ty.into(),
        };
        match table {
            "default.requests" => Ok(vec![
                col("user_id", "String"),
                col("latency_ms", "UInt32"),
                col("cost", "Nullable(Float64)"),
            ]),
            _ => Err(HistoryError::UnknownTable(table.into())),
        }
    }

    /// A ReplacingMergeTree by user (without a version column: the later event wins), so
    /// FINAL queries are keyed; the others read it like a MergeTree.
    async fn table_engine(&self, _: &str, _: &str) -> Result<TableEngine, HistoryError> {
        Ok(TableEngine::Replacing {
            ver: None,
            is_deleted: None,
            key: Key::from_system("user_id", ""),
        })
    }

    async fn fetch(&self, _sql: &str) -> Result<Vec<Row>, HistoryError> {
        Ok(Vec::new())
    }
}

struct EmptySnapshots;

#[async_trait]
impl Snapshots for EmptySnapshots {
    async fn acquire(&self, table: &SnapshotTable) -> Result<Lease, SnapshotError> {
        Ok(Arc::new(Snapshot::fixed(
            &table.database,
            &table.table,
            Watermark([(0, -1), (1, -1)].into()),
        )))
    }
}

/// 60 events over two partitions: users u0..u6, latencies with many ties, some NULL costs.
fn source() -> MemorySource {
    let mut s = MemorySource::new();
    for i in 0..60u32 {
        let latency = (i * 37) % 11 * 100;
        let cost = if i % 5 == 0 {
            "null".to_string()
        } else {
            format!("{}", f64::from(i) * 0.1)
        };
        s.push(
            "requests",
            (i % 2) as i32,
            format!(
                r#"{{"user_id": "u{}", "latency_ms": {latency}, "cost": {cost}}}"#,
                i % 7
            ),
        );
    }
    s
}

fn engine() -> Engine {
    // one event per iteration, so a checkpoint can fall between any two
    Engine::new(
        ConfiguredRules::default(),
        EmptyHistory,
        source(),
        EmptySnapshots,
    )
    .with_max_batch(1)
}

fn apply(list: &mut ResultList, its: &[Iteration]) {
    for it in its {
        for d in &it.diffs {
            list.apply(d).unwrap();
        }
    }
}

const SHAPES: &[(&str, &str)] = &[
    ("append", "select user_id, latency_ms from requests where latency_ms > 100"),
    ("window", "select user_id, latency_ms from requests limit 5"),
    ("ordered", "select user_id, latency_ms from requests order by latency_ms desc"),
    ("ordered", "select user_id from requests order by latency_ms desc, user_id limit 5"),
    ("offset", "select user_id, latency_ms from requests order by latency_ms desc limit 4 offset 3"),
    ("offset", "select user_id from requests order by latency_ms offset 5"),
    ("held", "select user_id, latency_ms from requests limit 4 offset 3"),
    ("held", "select user_id from requests offset 2"),
    ("native", "select user_id, count() as n, sum(latency_ms) as s, avg(latency_ms) as a, min(cost) as lo, max(cost) as hi from requests group by user_id"),
    ("native", "select user_id, sum(latency_ms) as s from requests group by user_id order by s desc limit 3"),
    ("native", "select user_id, count() as n from requests group by user_id order by n, user_id limit 2 offset 1"),
    ("native", "select count(), max(latency_ms), avg(cost) from requests"),
    ("keyed", "select user_id, latency_ms from requests final where latency_ms > 300"),
    ("keyed", "select * from requests final"),
    ("keyed", "select user_id, latency_ms from requests final order by latency_ms desc"),
    ("keyed", "select user_id from requests final where latency_ms > 0 order by cost nulls first, latency_ms"),
    ("keyed", "select user_id, latency_ms from requests final limit 3"),
    ("keyed", "select user_id, cost from requests final where latency_ms > 300 limit 2"),
    ("keyed", "select user_id, latency_ms from requests final order by latency_ms desc limit 3"),
    ("keyed", "select user_id from requests final order by cost nulls first, user_id limit 2"),
    ("keyed", "select user_id, latency_ms from requests final limit 2 offset 3"),
    ("keyed", "select user_id from requests final offset 4"),
    ("keyed", "select user_id, latency_ms from requests final order by latency_ms desc limit 2 offset 2"),
    ("keyed", "select user_id from requests final order by cost nulls first offset 3"),
    ("native_final", "select latency_ms, count() as n, sum(cost) as s, avg(cost) as a, min(cost) as lo, max(cost) as hi from requests final group by latency_ms"),
    ("native_final", "select latency_ms, count() as n from requests final group by latency_ms order by n desc, latency_ms limit 2 offset 1"),
    ("native_final", "select count(), sum(latency_ms), min(cost) from requests final where latency_ms > 300"),
    ("native", "select user_id, groupArray(cost) as costs, count() as n from requests group by user_id order by n desc, user_id limit 3"),
    ("native_final", "select latency_ms, array_agg(user_id) as users from requests final group by latency_ms"),
];

#[tokio::test]
async fn restored_subscriptions_continue_exactly_like_uninterrupted_ones() {
    let params = Params::new();
    for (shape, sql) in SHAPES {
        // the reference: one run, start to end
        let full: Vec<Iteration> = engine()
            .subscribe(sql)
            .await
            .unwrap()
            .map(|i| i.unwrap())
            .collect()
            .await;
        let mut want = ResultList::default();
        apply(&mut want, &full);
        // the history (empty) and one iteration per event: cuts fall between any two
        assert_eq!(full.len(), 61, "{sql}");
        assert!(!want.rows.is_empty(), "{sql}: the events make a result");

        for cut in [1, 2, 7, 20, 33, full.len() - 1, full.len()] {
            // run until `cut` iterations, checkpoint, stop
            let mut sub = engine().subscribe(sql).await.unwrap();
            let mut first = Vec::new();
            for _ in 0..cut {
                first.push(sub.next().await.unwrap().unwrap());
            }
            let checkpoint = sub.checkpoint().expect("a checkpoint");
            assert_eq!(checkpoint.shape(), *shape, "{sql}");
            assert_eq!(
                checkpoint.at,
                first.last().unwrap().at,
                "{sql}: the last iteration's cursor"
            );
            drop(sub);
            // as stored: JSON and back
            let checkpoint = Checkpoint::from_json(&checkpoint.to_json()).unwrap();

            // a new engine (another day) restores it and goes on
            let restored = engine()
                .restore(sql, &params, &Default::default(), &checkpoint)
                .await
                .unwrap();
            let rest: Vec<Iteration> = restored.map(|i| i.unwrap()).collect().await;
            assert!(
                rest[0].diffs.is_empty(),
                "{sql}: first comes an empty catch-up"
            );
            assert_eq!(rest[0].at, checkpoint.at);
            let mut got = ResultList::default();
            apply(&mut got, &first);
            apply(&mut got, &rest);
            assert_eq!(
                got,
                want,
                "{sql}: checkpointed after {cut} of {} iterations",
                full.len()
            );
            // the same diffs, one for one, as the uninterrupted run sent after the cut
            let diffs = |its: &[Iteration]| -> Vec<String> {
                its.iter()
                    .flat_map(|i| i.diffs.iter().map(|d| d.to_string()))
                    .collect()
            };
            assert_eq!(diffs(&rest[1..]), diffs(&full[cut..]), "{sql} after {cut}");
        }
    }
}

#[tokio::test]
async fn a_checkpoint_only_restores_its_own_kind_of_query() {
    let params = Params::new();
    let sub = engine()
        .subscribe("select user_id from requests order by latency_ms")
        .await
        .unwrap();
    let ordered = sub.checkpoint().unwrap();
    for other in [
        "select user_id from requests",
        "select user_id from requests final",
        "select user_id from requests limit 3",
        "select user_id, count() from requests group by user_id",
    ] {
        let e = engine()
            .restore(other, &params, &Default::default(), &ordered)
            .await
            .err()
            .unwrap();
        assert!(matches!(e, EngineError::Checkpoint(_)), "{other}: {e}");
    }
    let mut broken = ordered.clone();
    broken.state["groups"] = serde_json::json!("nope");
    let e = engine()
        .restore(
            "select user_id from requests order by latency_ms",
            &params,
            &Default::default(),
            &broken,
        )
        .await
        .err()
        .unwrap();
    assert!(e.to_string().contains("checkpoint"), "{e}");
}

#[tokio::test]
async fn catching_up_counts_the_events_a_restore_would_read() {
    let e = engine();
    let plan = e.explain("select user_id from requests").await.unwrap();
    // 30 events in each partition (offsets 0..=29)
    let at = |a: i64, b: i64| Watermark([(0, a), (1, b)].into());
    assert_eq!(
        e.catch_up_events(&plan, &at(29, 29)).await.unwrap(),
        Some(0)
    );
    assert_eq!(
        e.catch_up_events(&plan, &at(9, 19)).await.unwrap(),
        Some(30)
    );
    assert_eq!(
        e.catch_up_events(&plan, &at(-1, -1)).await.unwrap(),
        Some(60)
    );
    // past the end: another topic now (recreated)
    assert_eq!(e.catch_up_events(&plan, &at(40, 0)).await.unwrap(), None);
}
