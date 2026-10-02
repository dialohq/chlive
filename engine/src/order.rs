//! Managed ordering: where a new row goes in an ORDER BY result.
//!
//! The state holds only the ORDER BY values of the result, never its rows, with equal keys
//! merged into one tie group: `(key, how many rows have it)`, in result order. The history
//! seeds it in the order ClickHouse returned (ties included, in whatever order ClickHouse
//! chose); after that, a new row goes after the last row of its tie group. So the result always
//! has the same sequence of keys as a fresh query would, and only the order of rows within a
//! tie group can differ.

use std::cmp::Ordering;

use crate::binder::OrderKey;
use crate::types::Value;

/// The ORDER BY values of one row, most significant first.
pub type SortKey = Vec<Value>;

/// Compares sort keys by ORDER BY's keys: each one's direction and NULL placement.
#[derive(Debug, Clone, PartialEq)]
pub struct KeyOrder(pub Vec<OrderKey>);

impl KeyOrder {
    pub fn cmp(&self, a: &SortKey, b: &SortKey) -> Ordering {
        for (k, (x, y)) in self.0.iter().zip(a.iter().zip(b)) {
            match x.sort_cmp(y, k.desc, k.nulls_first) {
                Ordering::Equal => continue,
                other => return other,
            }
        }
        Ordering::Equal
    }
}

/// The tie groups of an ordered result, at most `limit` rows with LIMIT (the top N).
#[derive(Debug, Clone)]
pub struct OrderedState {
    order: KeyOrder,
    groups: Vec<(SortKey, usize)>,
    len: usize,
    limit: Option<usize>,
}

/// Where a new row goes in an ordered result.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Placement {
    /// Insert the row here.
    pub index: usize,
    /// Then delete the row at this index (the last one, pushed past the LIMIT).
    pub evicts: Option<usize>,
}

impl OrderedState {
    pub fn new(order: KeyOrder) -> Self {
        Self::with_limit(order, None)
    }

    pub fn with_limit(order: KeyOrder, limit: Option<u64>) -> Self {
        Self {
            order,
            groups: Vec::new(),
            len: 0,
            limit: limit.map(|n| usize::try_from(n).unwrap_or(usize::MAX)),
        }
    }

    /// Append a row of the (already ordered and limited) history. A key equal to the previous
    /// one joins its tie group; anything else starts a new group.
    pub fn push_history(&mut self, key: SortKey) {
        if self.is_full() {
            return; // ClickHouse already applied the LIMIT
        }
        match self.groups.last_mut() {
            Some((last, n)) if self.order.cmp(last, &key) == Ordering::Equal => *n += 1,
            _ => self.groups.push((key, 1)),
        }
        self.len += 1;
    }

    /// Place a new row after every row that sorts before it or ties with it. With a LIMIT and a
    /// full result, a row that would land past the end is dropped (`None`, nothing changes), and
    /// any other pushes the last row out.
    pub fn insert(&mut self, key: SortKey) -> Option<Placement> {
        let (at, existing, index) = self.locate(&key);
        if self.is_full() && Some(index) >= self.limit {
            return None;
        }
        if existing {
            self.groups[at].1 += 1;
        } else {
            self.groups.insert(at, (key, 1));
        }
        self.len += 1;
        let evicts = match self.limit {
            Some(n) if self.len > n => {
                let (_, count) = self.groups.last_mut().expect("not empty");
                *count -= 1;
                if *count == 0 {
                    self.groups.pop();
                }
                self.len -= 1;
                Some(n)
            }
            _ => None,
        };
        Some(Placement { index, evicts })
    }

    /// The group a key belongs at, whether it already exists, and the row index after the end
    /// of its tie group.
    fn locate(&self, key: &SortKey) -> (usize, bool, usize) {
        // fast path: at or after the last group (e.g. ORDER BY ts with rising timestamps)
        match self.groups.last() {
            None => return (0, false, 0),
            Some((last, _)) => match self.order.cmp(key, last) {
                Ordering::Equal => return (self.groups.len() - 1, true, self.len),
                Ordering::Greater => return (self.groups.len(), false, self.len),
                Ordering::Less => {}
            },
        }
        let at = self
            .groups
            .partition_point(|(g, _)| self.order.cmp(g, key) == Ordering::Less);
        let before: usize = self.groups[..at].iter().map(|(_, n)| n).sum();
        if self.order.cmp(&self.groups[at].0, key) == Ordering::Equal {
            (at, true, before + self.groups[at].1)
        } else {
            (at, false, before)
        }
    }

    fn is_full(&self) -> bool {
        self.limit.is_some_and(|n| self.len >= n)
    }

    /// Rows in the result.
    pub fn len(&self) -> usize {
        self.len
    }

    pub fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// Tie groups held (what the state costs).
    pub fn groups(&self) -> usize {
        self.groups.len()
    }
}

/// LIMIT without ORDER BY: a window of the latest N rows. Only the row count is kept.
#[derive(Debug, Clone)]
pub struct WindowState {
    len: u64,
    limit: u64,
}

impl WindowState {
    pub fn new(limit: u64) -> Self {
        Self { len: 0, limit }
    }

    pub fn push_history(&mut self) {
        self.len = (self.len + 1).min(self.limit);
    }

    /// A new row: `None` with `LIMIT 0` (it never shows), otherwise whether, after appending it,
    /// the first row must be deleted.
    pub fn push(&mut self) -> Option<bool> {
        if self.limit == 0 {
            return None;
        }
        if self.len < self.limit {
            self.len += 1;
            Some(false)
        } else {
            Some(true)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::binder::ColumnId;

    fn order(keys: &[(bool, bool)]) -> KeyOrder {
        KeyOrder(
            keys.iter()
                .enumerate()
                .map(|(i, (desc, nulls_first))| OrderKey {
                    column: ColumnId(i),
                    desc: *desc,
                    nulls_first: *nulls_first,
                })
                .collect(),
        )
    }

    fn int(n: i128) -> SortKey {
        vec![Value::Int(n)]
    }

    /// Insert keys one by one into a list of labels, the way a client applies indexes.
    fn apply(state: &mut OrderedState, list: &mut Vec<String>, rows: &[(SortKey, &str)]) {
        for (key, label) in rows {
            if let Some(p) = state.insert(key.clone()) {
                list.insert(p.index, label.to_string());
                if let Some(e) = p.evicts {
                    list.remove(e);
                }
            }
        }
    }

    #[test]
    fn ties_go_after_their_group_and_groups_merge() {
        let mut s = OrderedState::new(order(&[(false, false)]));
        let mut list: Vec<String> = Vec::new();
        for (k, l) in [(1, "h1"), (3, "h3a"), (3, "h3b"), (5, "h5")] {
            s.push_history(int(k));
            list.push(l.into());
        }
        assert_eq!(s.groups(), 3);
        apply(
            &mut s,
            &mut list,
            &[
                (int(3), "e3"), // end of the 3s
                (int(0), "e0"), // first
                (int(9), "e9"), // last (fast path)
                (int(4), "e4"), // a new group in the middle
                (int(1), "e1"), // end of the 1s
                (int(9), "e9b"),
            ],
        );
        assert_eq!(
            list,
            ["e0", "h1", "e1", "h3a", "h3b", "e3", "e4", "h5", "e9", "e9b"]
        );
        assert_eq!((s.len(), s.groups()), (10, 6));
    }

    #[test]
    fn desc_and_nulls() {
        // ORDER BY a DESC NULLS FIRST
        let mut s = OrderedState::new(order(&[(true, true)]));
        let mut list = Vec::new();
        apply(
            &mut s,
            &mut list,
            &[
                (int(1), "1"),
                (int(5), "5"),
                (vec![Value::Null], "null"),
                (int(3), "3"),
                (vec![Value::Null], "null2"),
            ],
        );
        assert_eq!(list, ["null", "null2", "5", "3", "1"]);
    }

    #[test]
    fn multiple_keys() {
        // ORDER BY country, latency DESC
        let mut s = OrderedState::new(order(&[(false, false), (true, false)]));
        let k = |c: &str, l: i128| vec![Value::String(c.into()), Value::Int(l)];
        let mut list = Vec::new();
        apply(
            &mut s,
            &mut list,
            &[
                (k("PL", 10), "pl10"),
                (k("DE", 5), "de5"),
                (k("PL", 50), "pl50"),
                (k("DE", 5), "de5b"),
                (k("PL", 1), "pl1"),
            ],
        );
        assert_eq!(list, ["de5", "de5b", "pl50", "pl10", "pl1"]);
    }

    #[test]
    fn top_n() {
        // ORDER BY a DESC LIMIT 3, history 9 5 5
        let mut s = OrderedState::with_limit(order(&[(true, false)]), Some(3));
        let mut list: Vec<String> = Vec::new();
        for (k, l) in [(9, "h9"), (5, "h5a"), (5, "h5b")] {
            s.push_history(int(k));
            list.push(l.into());
        }
        // past the end, and a tie with the last row: dropped, nothing changes
        assert_eq!(s.insert(int(1)), None);
        assert_eq!(s.insert(int(5)), None);
        assert_eq!((s.len(), s.groups()), (3, 2));
        // inside: inserted, then the last row (now at index N) is deleted
        assert_eq!(
            s.clone().insert(int(7)),
            Some(Placement {
                index: 1,
                evicts: Some(3)
            })
        );
        apply(
            &mut s,
            &mut list,
            &[
                (int(7), "e7"),
                (int(10), "e10"),
                (int(6), "e6"),
                (int(8), "e8"),
            ],
        );
        assert_eq!(list, ["e10", "h9", "e8"]);
        // the state never holds more than N
        assert_eq!((s.len(), s.groups()), (3, 3));
    }

    #[test]
    fn limit_zero_and_short_history() {
        let mut s = OrderedState::with_limit(order(&[(false, false)]), Some(0));
        assert_eq!(s.insert(int(1)), None);
        // history shorter than the limit: no evictions until full
        let mut s = OrderedState::with_limit(order(&[(false, false)]), Some(2));
        s.push_history(int(5));
        assert_eq!(
            s.insert(int(1)),
            Some(Placement {
                index: 0,
                evicts: None
            })
        );
        assert_eq!(
            s.insert(int(3)),
            Some(Placement {
                index: 1,
                evicts: Some(2)
            })
        );
        assert_eq!(s.insert(int(9)), None);
    }

    #[test]
    fn window() {
        let mut w = WindowState::new(2);
        w.push_history();
        assert_eq!(w.push(), Some(false)); // now full
        assert_eq!(w.push(), Some(true)); // append, then delete the first
        assert_eq!(w.push(), Some(true));
        assert_eq!(WindowState::new(0).push(), None);
    }
}
