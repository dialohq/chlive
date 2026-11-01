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
use crate::types::quote_string;
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
            match self.stream.decode(event) {
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

/// chDB's `url()` running `sql` on main ClickHouse over HTTP. The result comes as
/// RowBinaryWithNamesAndTypes, which carries aggregate states as they are and, unlike Native,
/// names its columns and types even when there are no rows (no history), so chDB always
/// knows the table's structure. The credentials go in headers (so they're in no URL or query
/// log).
pub fn http_source(http_url: &str, sql: &str, user: &str, password: &str) -> String {
    let mut url = reqwest::Url::parse(http_url)
        .unwrap_or_else(|_| reqwest::Url::parse("http://invalid/").expect("a URL"));
    url.query_pairs_mut()
        .append_pair("query", &format!("{sql} FORMAT RowBinaryWithNamesAndTypes"));
    format!(
        "url({}, 'RowBinaryWithNamesAndTypes', headers('X-ClickHouse-User' = {}, 'X-ClickHouse-Key' = {}))",
        quote_string(url.as_str()),
        quote_string(user),
        quote_string(password)
    )
}

#[cfg(test)]
mod http_tests {
    use super::http_source;

    #[test]
    fn chdb_reads_states_over_http() {
        let sql = "SELECT country AS __k0, uniqExactState(user_id) AS __a0_0 \
                   FROM db.`__chlive_snap_events_1` WHERE path = 'a&b' GROUP BY __k0";
        let src = http_source("http://ch:8123", sql, "ro", "it's secret");
        // the query (with its FORMAT) is in the URL, encoded; the credentials are headers
        assert_eq!(
            src,
            "url('http://ch:8123/?query=SELECT+country+AS+__k0%2C+uniqExactState%28user_id%29+AS+__a0_0+\
             FROM+db.%60__chlive_snap_events_1%60+WHERE+path+%3D+%27a%26b%27+GROUP+BY+__k0+FORMAT+RowBinaryWithNamesAndTypes', \
             'RowBinaryWithNamesAndTypes', headers('X-ClickHouse-User' = 'ro', 'X-ClickHouse-Key' = 'it\\'s secret'))"
        );
        assert!(
            !src.contains("secret") || !src.split("headers").next().unwrap().contains("secret")
        );
        // HTTPS, a path, and parameters already in the URL are kept
        let src = http_source(
            "https://ch.example:8443/proxy?database=x",
            "SELECT 1",
            "u",
            "",
        );
        assert!(
            src.starts_with(
                "url('https://ch.example:8443/proxy?database=x&query=SELECT+1+FORMAT+RowBinaryWithNamesAndTypes'"
            ),
            "{src}"
        );
    }
}
