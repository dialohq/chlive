//! Time for `now()`: the server's clock, and when each row of a time-windowed result leaves.
//!
//! A query with `col > now() - INTERVAL …` changes with no event: as the clock passes a row's
//! deadline (`col` minus the shift), the row leaves. The engine re-reads the clock every refresh
//! interval (`now(ms)`, or the server's default) and deletes the rows whose deadline it passed.

use std::time::{SystemTime, UNIX_EPOCH};

use serde_json::{json, Value as Json};

use crate::diff::Diff;

/// Nanoseconds since the Unix epoch, now.
pub trait Clock: Send + Sync {
    fn now_ns(&self) -> i128;
}

/// The system's clock.
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_ns(&self) -> i128 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |d| d.as_nanos() as i128)
    }
}

/// For tests: `base` plus tokio's time since the clock was made, so a paused tokio runtime
/// (`start_paused`) moves it exactly as far as its timers.
pub struct TokioClock {
    base: i128,
    start: tokio::time::Instant,
}

impl TokioClock {
    /// Now is `base` nanoseconds since the epoch.
    pub fn at(base: i128) -> Self {
        Self {
            base,
            start: tokio::time::Instant::now(),
        }
    }
}

impl Clock for TokioClock {
    fn now_ns(&self) -> i128 {
        self.base + self.start.elapsed().as_nanos() as i128
    }
}

/// When each row of a result leaves, in the client's order: kept in step with the diffs the
/// result's state sends.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct Deadlines(Vec<i128>);

impl Deadlines {
    /// A history row, appended.
    pub fn push(&mut self, deadline: i128) {
        self.0.push(deadline);
    }

    /// An event's diffs: its row (with this deadline) inserted, and rows deleted.
    pub fn follow(&mut self, diffs: &[Diff], deadline: i128) {
        for d in diffs {
            match d {
                Diff::Insert { index: None, .. } => self.0.push(deadline),
                Diff::Insert { index: Some(i), .. } => self.0.insert(*i, deadline),
                Diff::Delete { index } => {
                    self.0.remove(*index);
                }
                Diff::Update { .. } | Diff::DeleteId { .. } => {}
            }
        }
    }

    /// The rows whose deadline is at or before `now`, highest first (to delete in that order),
    /// taken out.
    pub fn expire(&mut self, now: i128) -> Vec<usize> {
        let due: Vec<usize> = (0..self.0.len())
            .rev()
            .filter(|i| self.0[*i] <= now)
            .collect();
        for i in &due {
            self.0.remove(*i);
        }
        due
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// For a checkpoint: the deadlines as strings (they don't fit JSON's numbers).
    pub fn save(&self) -> Json {
        json!(self.0.iter().map(|d| d.to_string()).collect::<Vec<_>>())
    }

    pub fn load(json: &Json) -> Result<Self, String> {
        json.as_array()
            .ok_or("checkpoint: bad deadlines")?
            .iter()
            .map(|d| {
                d.as_str()
                    .and_then(|d| d.parse().ok())
                    .ok_or_else(|| "checkpoint: bad deadline".to_string())
            })
            .collect::<Result<_, _>>()
            .map(Deadlines)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Map;

    #[test]
    fn deadlines_follow_the_rows_and_expire_highest_first() {
        let mut d = Deadlines::default();
        d.push(30);
        d.push(10);
        let insert = |index| Diff::Insert {
            row: Map::new(),
            index,
            id: None,
        };
        d.follow(&[insert(Some(0)), Diff::Delete { index: 2 }], 20);
        d.follow(&[insert(None)], 5);
        assert_eq!(d.0, [20, 30, 5]);
        assert_eq!(d.expire(20), [2, 0]);
        assert_eq!(d.0, [30]);
        assert_eq!(Deadlines::load(&d.save()).unwrap(), d);
        assert!(Deadlines::load(&json!([1])).is_err());
    }
}
