//! Lining history up with the stream: per partition, the last Kafka offset in a snapshot of the
//! ClickHouse table (see [`crate::snapshot`]). History covers offsets `<= W[p]`; the stream
//! starts at `W[p] + 1`. So every event is in exactly one of them, however far ClickHouse's
//! ingestion is behind.

use std::collections::BTreeMap;

/// `W[p]` = last offset of partition `p` in the snapshot; absent = none (`-1`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Watermark(pub BTreeMap<i32, i64>);

impl Watermark {
    pub fn get(&self, partition: i32) -> i64 {
        self.0.get(&partition).copied().unwrap_or(-1)
    }

    /// Where the stream starts in a partition.
    pub fn start(&self, partition: i32) -> i64 {
        self.get(partition) + 1
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

    /// Whether this is at or past `other` in every partition (a missing partition is `-1`): a
    /// result complete up to `self` has everything one complete up to `other` has.
    pub fn covers(&self, other: &Watermark) -> bool {
        let partitions = self.0.keys().chain(other.0.keys());
        partitions
            .into_iter()
            .all(|p| self.get(*p) >= other.get(*p))
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
    fn covers() {
        let w = |pairs: &[(i32, i64)]| Watermark(pairs.iter().copied().collect());
        assert!(w(&[(0, 5), (1, 3)]).covers(&w(&[(0, 5), (1, 3)])));
        assert!(w(&[(0, 6), (1, 3)]).covers(&w(&[(0, 5), (1, 3)])));
        assert!(!w(&[(0, 6), (1, 2)]).covers(&w(&[(0, 5), (1, 3)])));
        // a partition missing on one side is -1 there
        assert!(w(&[(0, 5), (1, 0)]).covers(&w(&[(0, 5)])));
        assert!(!w(&[(0, 5)]).covers(&w(&[(0, 5), (1, 0)])));
        assert!(w(&[]).covers(&w(&[])));
        assert!(w(&[(0, -1)]).covers(&w(&[])));
    }

    #[test]
    fn starts() {
        let w = Watermark(BTreeMap::from([(0, 120), (1, 98)]));
        assert_eq!((w.start(0), w.start(1), w.start(2)), (121, 99, 0));
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
    fn max() {
        let lower = Watermark(BTreeMap::from([(0, 5), (1, 9)]));
        let upper = Watermark(BTreeMap::from([(0, 8), (1, 7), (2, 3)]));
        assert_eq!(
            lower.max(&upper),
            Watermark(BTreeMap::from([(0, 8), (1, 9), (2, 3)]))
        );
    }
}
