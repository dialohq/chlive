//! Lining history up with the stream: per partition, the highest Kafka offset already in
//! ClickHouse. History covers offsets `<= W[p]`; the stream starts at `W[p] + 1`. So every event
//! is in exactly one of them, however far ClickHouse's ingestion is behind.

use std::collections::BTreeMap;

use serde_json::Value;

use crate::history::{HistoryError, HistorySource};
use crate::rules::quote_ident;

/// `W[p]` = highest offset of partition `p` present in ClickHouse; absent = none (`-1`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Watermark(pub BTreeMap<i32, i64>);

impl Watermark {
    /// Read `max(offset)` per partition from a ClickHouse table.
    pub async fn fetch(
        history: &dyn HistorySource,
        clickhouse_table: &str,
        partition_column: &str,
        offset_column: &str,
    ) -> Result<Self, HistoryError> {
        let sql = format!(
            "SELECT {} AS p, max({}) AS o FROM {clickhouse_table} GROUP BY p",
            quote_ident(partition_column),
            quote_ident(offset_column)
        );
        let mut w = BTreeMap::new();
        for row in history.fetch(&sql).await? {
            let p = row.get("p").and_then(Value::as_i64);
            let o = row.get("o").and_then(Value::as_i64);
            match (p, o) {
                (Some(p), Some(o)) => {
                    w.insert(p as i32, o);
                }
                _ => {
                    return Err(HistoryError::ClickHouse(format!(
                        "unexpected watermark row {row:?}"
                    )))
                }
            }
        }
        Ok(Self(w))
    }

    pub fn get(&self, partition: i32) -> i64 {
        self.0.get(&partition).copied().unwrap_or(-1)
    }

    /// Where the stream starts in a partition.
    pub fn start(&self, partition: i32) -> i64 {
        self.get(partition) + 1
    }

    /// ClickHouse condition selecting exactly the rows at or below the watermark, e.g.
    /// `toInt64(_offset) <= multiIf(_partition = 0, 120, _partition = 1, 98, -1)`.
    pub fn history_predicate(&self, partition_column: &str, offset_column: &str) -> String {
        let (p, o) = (quote_ident(partition_column), quote_ident(offset_column));
        let arms: Vec<String> = self
            .0
            .iter()
            .filter(|(_, o)| **o >= 0)
            .map(|(part, off)| format!("{p} = {part}, {off}"))
            .collect();
        let threshold = if arms.is_empty() {
            "-1".to_string()
        } else {
            format!("multiIf({}, -1)", arms.join(", "))
        };
        format!("toInt64({o}) <= {threshold}")
    }

    /// ClickHouse condition selecting the rows after `self` and at or below `upper`, per
    /// partition: what a client at `self` is missing of what ClickHouse has up to `upper`.
    /// Empty for a partition where `upper` isn't past `self`.
    pub fn range_predicate(
        &self,
        upper: &Watermark,
        partition_column: &str,
        offset_column: &str,
    ) -> String {
        let p = quote_ident(partition_column);
        let o = quote_ident(offset_column);
        let lower: Vec<String> = self
            .0
            .iter()
            .map(|(part, off)| format!("{p} = {part}, {off}"))
            .collect();
        let lower = if lower.is_empty() {
            "-1".to_string()
        } else {
            format!("multiIf({}, -1)", lower.join(", "))
        };
        format!(
            "toInt64({o}) > {lower} AND {}",
            upper.history_predicate(partition_column, offset_column)
        )
    }

    /// Per partition, the further of the two.
    pub fn max(&self, other: &Watermark) -> Watermark {
        let mut out = self.0.clone();
        for (p, o) in &other.0 {
            let e = out.entry(*p).or_insert(*o);
            *e = (*e).max(*o);
        }
        Watermark(out)
    }

    /// For logs and explain: `{0: 120, 1: 98}`.
    pub fn describe(&self) -> String {
        let parts: Vec<String> = self.0.iter().map(|(p, o)| format!("{p}: {o}")).collect();
        format!("{{{}}}", parts.join(", "))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn predicate_and_starts() {
        let w = Watermark(BTreeMap::from([(0, 120), (1, 98)]));
        assert_eq!(
            w.history_predicate("_partition", "_offset"),
            "toInt64(_offset) <= multiIf(_partition = 0, 120, _partition = 1, 98, -1)"
        );
        assert_eq!((w.start(0), w.start(1), w.start(2)), (121, 99, 0));
        assert_eq!(
            Watermark::default().history_predicate("_partition", "_offset"),
            "toInt64(_offset) <= -1"
        );
    }

    /// Every (partition, offset) is in exactly one of history (`<= W`) and stream (`>= start`).
    #[test]
    fn history_and_stream_partition_all_offsets() {
        let w = Watermark(BTreeMap::from([(0, 5), (2, 0)]));
        for p in 0..4 {
            for o in 0..10 {
                let in_history = o <= w.get(p);
                let in_stream = o >= w.start(p);
                assert_ne!(in_history, in_stream, "partition {p} offset {o}");
            }
        }
    }

    #[test]
    fn range_and_max() {
        let lower = Watermark(BTreeMap::from([(0, 5), (1, 9)]));
        let upper = Watermark(BTreeMap::from([(0, 8), (1, 7), (2, 3)]));
        assert_eq!(
            lower.range_predicate(&upper, "_partition", "_offset"),
            "toInt64(_offset) > multiIf(_partition = 0, 5, _partition = 1, 9, -1) \
             AND toInt64(_offset) <= multiIf(_partition = 0, 8, _partition = 1, 7, _partition = 2, 3, -1)"
        );
        assert_eq!(
            lower.max(&upper),
            Watermark(BTreeMap::from([(0, 8), (1, 9), (2, 3)]))
        );
    }
}
