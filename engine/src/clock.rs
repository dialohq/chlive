//! Time for `now()`: the server's clock, and when each row of a time-windowed result leaves.
//!
//! A query with `col > now() - INTERVAL …` changes with no event: as the clock passes a row's
//! deadline (`col` minus the shift), the row leaves. The engine wakes at the earliest deadline
//! (plus the server's coalescing window, so rows due close together leave together) and deletes
//! the rows whose deadline the clock passed.

use std::collections::BTreeMap;
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

/// Sleep until `earliest + coalescing` on `clock` (now, if that's past); forever without one.
pub async fn wait_for(clock: &dyn Clock, earliest: Option<i128>, coalescing: i128) {
    match earliest {
        Some(d) => {
            let ns = (d + coalescing - clock.now_ns()).max(0);
            tokio::time::sleep(std::time::Duration::from_nanos(
                u64::try_from(ns).unwrap_or(u64::MAX),
            ))
            .await
        }
        None => std::future::pending().await,
    }
}

/// When each row of a result leaves, in the client's order: kept in step with the diffs the
/// result's state sends. Also counted by deadline, for the earliest.
#[derive(Debug, Clone, Default)]
pub struct Deadlines {
    rows: Vec<i128>,
    sorted: BTreeMap<i128, usize>,
}

impl PartialEq for Deadlines {
    fn eq(&self, other: &Self) -> bool {
        self.rows == other.rows
    }
}

impl Deadlines {
    fn add(&mut self, deadline: i128) {
        *self.sorted.entry(deadline).or_default() += 1;
    }

    fn take(&mut self, deadline: i128) {
        if let Some(n) = self.sorted.get_mut(&deadline) {
            *n -= 1;
            if *n == 0 {
                self.sorted.remove(&deadline);
            }
        }
    }

    /// A history row, appended.
    pub fn push(&mut self, deadline: i128) {
        self.rows.push(deadline);
        self.add(deadline);
    }

    /// The earliest deadline, if any row has one.
    pub fn earliest(&self) -> Option<i128> {
        self.sorted.keys().next().copied()
    }

    /// An event's diffs: its row (with this deadline) inserted, and rows deleted.
    pub fn follow(&mut self, diffs: &[Diff], deadline: i128) {
        for d in diffs {
            match d {
                Diff::Insert { index: None, .. } => {
                    self.rows.push(deadline);
                    self.add(deadline);
                }
                Diff::Insert { index: Some(i), .. } => {
                    self.rows.insert(*i, deadline);
                    self.add(deadline);
                }
                Diff::Delete { index } => {
                    let gone = self.rows.remove(*index);
                    self.take(gone);
                }
                Diff::Update { .. } | Diff::DeleteId { .. } => {}
            }
        }
    }

    /// The rows whose deadline is at or before `now`, highest first (to delete in that order),
    /// taken out.
    pub fn expire(&mut self, now: i128) -> Vec<usize> {
        if self.earliest().is_none_or(|d| d > now) {
            return Vec::new();
        }
        let due: Vec<usize> = (0..self.rows.len())
            .rev()
            .filter(|i| self.rows[*i] <= now)
            .collect();
        for i in &due {
            let gone = self.rows.remove(*i);
            self.take(gone);
        }
        due
    }

    pub fn len(&self) -> usize {
        self.rows.len()
    }

    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// For a checkpoint: the deadlines as strings (they don't fit JSON's numbers).
    pub fn save(&self) -> Json {
        json!(self.rows.iter().map(|d| d.to_string()).collect::<Vec<_>>())
    }

    pub fn load(json: &Json) -> Result<Self, String> {
        let mut d = Deadlines::default();
        for v in json.as_array().ok_or("checkpoint: bad deadlines")? {
            let v = v
                .as_str()
                .and_then(|v| v.parse().ok())
                .ok_or("checkpoint: bad deadline")?;
            d.push(v);
        }
        Ok(d)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::Map;

    fn insert(index: Option<usize>) -> Diff {
        Diff::Insert {
            row: Map::new(),
            index,
            id: None,
        }
    }

    #[test]
    fn deadlines_follow_the_rows_and_expire_highest_first() {
        let mut d = Deadlines::default();
        d.push(30);
        d.push(10);
        d.follow(&[insert(Some(0)), Diff::Delete { index: 2 }], 20);
        d.follow(&[insert(None)], 5);
        assert_eq!(d.rows, [20, 30, 5]);
        assert_eq!(d.earliest(), Some(5));
        assert!(d.expire(4).is_empty());
        assert_eq!(d.expire(20), [2, 0]);
        assert_eq!(d.rows, [30]);
        assert_eq!(d.earliest(), Some(30));
        assert_eq!(Deadlines::load(&d.save()).unwrap(), d);
        assert!(Deadlines::load(&json!([1])).is_err());
    }

    /// The earliest is always the smallest of the rows' deadlines, whatever comes and goes.
    #[test]
    fn the_earliest_follows_every_change() {
        let mut seed: u64 = 4;
        let mut rand = |n: u64| {
            seed = seed
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            (seed >> 33) % n
        };
        let mut d = Deadlines::default();
        for _ in 0..2000 {
            match rand(4) {
                0 => d.push(rand(50) as i128),
                1 => {
                    let i = rand(d.len() as u64 + 1) as usize;
                    let v = rand(50) as i128;
                    d.follow(&[insert(Some(i))], v);
                }
                2 if !d.is_empty() => {
                    let i = rand(d.len() as u64) as usize;
                    d.follow(&[Diff::Delete { index: i }], 0);
                }
                _ => {
                    let now = rand(20) as i128;
                    d.expire(now);
                }
            }
            assert_eq!(d.earliest(), d.rows.iter().min().copied());
            let back = Deadlines::load(&d.save()).unwrap();
            assert_eq!(back.earliest(), d.earliest());
        }
    }
}
