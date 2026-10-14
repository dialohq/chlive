//! The chDB backend: aggregate states as ClickHouse's own `-State` values in a chDB table per
//! subscription, merged with `-Merge` (used when a query has an aggregate without a native
//! implementation, or when forced).

use std::sync::Arc;

use futures::stream::{BoxStream, StreamExt};
use serde_json::Value;

use super::result::GroupResult;
use crate::chdb::Chdb;
use crate::diff::Row;
use crate::engine::{EngineError, Iteration, Origin};
use crate::plan::{AggregatePlan, StreamPlan};
use crate::source::Event;
use crate::watermark::Watermark;

/// A chDB table that goes away with its subscription.
pub(crate) struct ChdbTable {
    pub(crate) chdb: Chdb,
    pub(crate) name: String,
}

impl Drop for ChdbTable {
    fn drop(&mut self) {
        let (chdb, sql) = (
            self.chdb.clone(),
            format!("DROP TABLE IF EXISTS {}", self.name),
        );
        if let Ok(rt) = tokio::runtime::Handle::try_current() {
            rt.spawn(async move {
                if let Err(e) = chdb.exec(sql).await {
                    tracing::warn!(error = %e, "dropping an aggregate table");
                }
            });
        }
    }
}

/// A running aggregate subscription (see [`Engine::subscribe_aggregate`]).
pub(crate) struct AggregateRun {
    pub(crate) chdb: Chdb,
    pub(crate) table: Arc<ChdbTable>,
    /// The history's `CREATE TABLE … AS SELECT * FROM url(…)` (main ClickHouse's states, over
    /// HTTP), until it has run.
    pub(crate) create: Option<String>,
    /// The snapshot `create` reads, until it has run.
    pub(crate) snapshot: Option<crate::snapshot::Lease>,
    pub(crate) aggregate: AggregatePlan,
    pub(crate) stream: StreamPlan,
    pub(crate) events: BoxStream<'static, Vec<Event>>,
    pub(crate) result: GroupResult,
    pub(crate) at: Watermark,
    pub(crate) batches: u64,
    pub(crate) failed: bool,
}

/// Merged state rows pile up in the chDB table until merged; force that every this many batches.
const OPTIMIZE_EVERY: u64 = 1000;

impl AggregateRun {
    pub(crate) async fn step(&mut self) -> Option<Result<Iteration, EngineError>> {
        if self.failed {
            return None;
        }
        let result = match self.create.take() {
            Some(create) => self.history(create).await,
            None => {
                let batch = self.events.next().await?;
                self.batch(batch).await
            }
        };
        self.failed = result.is_err();
        Some(result)
    }

    async fn history(&mut self, create: String) -> Result<Iteration, EngineError> {
        self.chdb.exec(create).await?;
        self.snapshot = None; // its states are in chDB now
        let rows = self
            .chdb
            .rows(self.aggregate.snapshot_sql(&self.table.name))
            .await?;
        Ok(Iteration {
            origin: Origin::History,
            diffs: {
                let groups = self.groups(rows);
                self.result.history(groups)
            },
            at: self.at.clone(),
        })
    }

    async fn batch(&mut self, events: Vec<Event>) -> Result<Iteration, EngineError> {
        let last = events.last().expect("a non-empty batch");
        let origin = Origin::Event {
            topic: last.topic.clone(),
            partition: last.partition,
            offset: last.offset,
        };
        let mut lines = String::new();
        for event in &events {
            let position = self.at.0.entry(event.partition).or_insert(event.offset);
            *position = (*position).max(event.offset);
            match self.stream.decode(&event.value) {
                Ok(row) if self.stream.matches(&row) => {
                    let mut object: Row = self
                        .aggregate
                        .batch_columns
                        .iter()
                        .map(|(id, name)| (name.clone(), row[id.0].to_json()))
                        .collect();
                    if object.is_empty() {
                        object.insert(crate::plan::PLACEHOLDER.into(), Value::from(1));
                    }
                    lines.push_str(&Value::Object(object).to_string());
                    lines.push('\n');
                }
                Ok(_) => {}
                Err(e) => tracing::warn!(
                    topic = %event.topic,
                    partition = event.partition,
                    offset = event.offset,
                    error = %e,
                    "skipping event"
                ),
            }
        }
        if lines.is_empty() {
            return Ok(Iteration {
                origin,
                diffs: Vec::new(),
                at: self.at.clone(),
            });
        }
        let table = &self.table.name;
        self.chdb
            .exec(self.aggregate.insert_sql(table, &lines))
            .await?;
        let rows = self
            .chdb
            .rows(self.aggregate.touched_sql(table, &lines))
            .await?;
        self.batches += 1;
        if self.batches.is_multiple_of(OPTIMIZE_EVERY) {
            self.chdb
                .exec(format!("OPTIMIZE TABLE {table} FINAL"))
                .await?;
        }
        Ok(Iteration {
            origin,
            diffs: {
                let groups = self.groups(rows);
                self.result.batch(groups)
            },
            at: self.at.clone(),
        })
    }

    /// Finalized group rows (with their keys as `__k0…`), by group id.
    fn groups(&self, rows: Vec<Row>) -> Vec<(String, Row)> {
        rows.into_iter()
            .map(|mut row| {
                let key: Vec<Value> = (0..self.aggregate.keys)
                    .map(|i| row.shift_remove(&format!("__k{i}")).unwrap_or(Value::Null))
                    .collect();
                (Value::Array(key).to_string(), row)
            })
            .collect()
    }
}
