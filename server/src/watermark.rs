//! Per-partition Kafka offset watermarks and the SQL predicates that split rows into
//! history (`_offset <= W[p]`) and tail (`_offset > W[p]`).

use std::collections::BTreeMap;

use serde::Serialize;

/// `W[p]` = highest offset of partition `p` included in history; `-1` (or absent) means none.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
pub struct Watermark(pub BTreeMap<u32, i64>);

impl Watermark {
    /// Watermark covering `partitions`, filling partitions without rows with `-1`.
    pub fn from_max_offsets(partitions: u32, max: &BTreeMap<u32, i64>) -> Self {
        Self(
            (0..partitions)
                .map(|p| (p, max.get(&p).copied().unwrap_or(-1)))
                .collect(),
        )
    }

    pub fn get(&self, partition: u32) -> i64 {
        self.0.get(&partition).copied().unwrap_or(-1)
    }

    /// `multiIf(_partition = 0, 120, _partition = 1, 98, -1)`; partitions absent from `W`
    /// map to `-1`, so all their rows fall in the tail. Partitions at `-1` are omitted since
    /// the default already covers them.
    pub fn threshold_expr(&self) -> String {
        let arms: Vec<String> = self
            .0
            .iter()
            .filter(|(_, o)| **o >= 0)
            .map(|(p, o)| format!("_partition = {p}, {o}"))
            .collect();
        if arms.is_empty() {
            "-1".to_string()
        } else {
            format!("multiIf({}, -1)", arms.join(", "))
        }
    }

    pub fn history_pred(&self) -> String {
        format!("toInt64(_offset) <= {}", self.threshold_expr())
    }

    pub fn tail_pred(&self) -> String {
        format!("toInt64(_offset) > {}", self.threshold_expr())
    }

    /// Rust mirror of [`Self::history_pred`] used by tests.
    pub fn in_history(&self, partition: u32, offset: u64) -> bool {
        (offset as i64) <= self.get(partition)
    }

    /// Rust mirror of [`Self::tail_pred`] used by tests.
    pub fn in_tail(&self, partition: u32, offset: u64) -> bool {
        (offset as i64) > self.get(partition)
    }

    /// True when every partition in `self` is at or beyond `other`.
    pub fn covers(&self, other: &Watermark) -> bool {
        other.0.iter().all(|(p, o)| self.get(*p) >= *o)
    }
}

/// SQL returning `p = _partition, o = max(_offset)` for a source table on main.
pub fn max_offsets_sql(database: &str, table: &str) -> String {
    format!("SELECT _partition AS p, max(_offset) AS o FROM {database}.{table} GROUP BY _partition")
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::Rng;

    #[test]
    fn predicate_sql() {
        let w = Watermark(BTreeMap::from([(0, 120), (1, 98), (2, -1)]));
        assert_eq!(
            w.history_pred(),
            "toInt64(_offset) <= multiIf(_partition = 0, 120, _partition = 1, 98, -1)"
        );
        assert_eq!(
            w.tail_pred(),
            "toInt64(_offset) > multiIf(_partition = 0, 120, _partition = 1, 98, -1)"
        );
        assert_eq!(
            Watermark::default().history_pred(),
            "toInt64(_offset) <= -1"
        );
    }

    #[test]
    fn from_max_offsets_fills_missing() {
        let w = Watermark::from_max_offsets(3, &BTreeMap::from([(1, 7)]));
        assert_eq!(w.0, BTreeMap::from([(0, -1), (1, 7), (2, -1)]));
    }

    /// History and tail are disjoint and together cover every row, for random watermarks
    /// including partitions missing from W and partitions at -1.
    #[test]
    fn history_and_tail_partition_all_rows() {
        let mut rng = rand::rng();
        for _ in 0..200 {
            let partitions = rng.random_range(1..6u32);
            let mut w = Watermark::default();
            for p in 0..partitions {
                match rng.random_range(0..3) {
                    0 => {} // missing
                    1 => {
                        w.0.insert(p, -1);
                    }
                    _ => {
                        w.0.insert(p, rng.random_range(0..500));
                    }
                }
            }
            for _ in 0..500 {
                let p = rng.random_range(0..partitions + 1);
                let o = rng.random_range(0..600u64);
                assert_ne!(w.in_history(p, o), w.in_tail(p, o), "W={w:?} p={p} o={o}");
            }
        }
    }
}
