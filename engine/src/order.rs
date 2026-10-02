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

/// The tie groups of an ordered result.
#[derive(Debug, Clone)]
pub struct OrderedState {
    order: KeyOrder,
    groups: Vec<(SortKey, usize)>,
    len: usize,
}

impl OrderedState {
    pub fn new(order: KeyOrder) -> Self {
        Self {
            order,
            groups: Vec::new(),
            len: 0,
        }
    }

    /// Append a row of the (already ordered) history. A key equal to the previous one joins its
    /// tie group; anything else starts a new group.
    pub fn push_history(&mut self, key: SortKey) {
        match self.groups.last_mut() {
            Some((last, n)) if self.order.cmp(last, &key) == Ordering::Equal => *n += 1,
            _ => self.groups.push((key, 1)),
        }
        self.len += 1;
    }

    /// Place a new row: returns its index in the result, after every row that sorts before it
    /// or ties with it.
    pub fn insert(&mut self, key: SortKey) -> usize {
        let index;
        // fast path: at or after the last group (e.g. ORDER BY ts with rising timestamps)
        match self.groups.last_mut() {
            None => {
                self.groups.push((key, 1));
                index = 0;
            }
            Some((last, n)) => match self.order.cmp(&key, last) {
                Ordering::Equal => {
                    *n += 1;
                    index = self.len;
                }
                Ordering::Greater => {
                    self.groups.push((key, 1));
                    index = self.len;
                }
                Ordering::Less => {
                    let order = &self.order;
                    let at = self
                        .groups
                        .partition_point(|(g, _)| order.cmp(g, &key) == Ordering::Less);
                    let before: usize = self.groups[..at].iter().map(|(_, n)| n).sum();
                    if order.cmp(&self.groups[at].0, &key) == Ordering::Equal {
                        self.groups[at].1 += 1;
                        index = before + self.groups[at].1 - 1;
                    } else {
                        self.groups.insert(at, (key, 1));
                        index = before;
                    }
                }
            },
        }
        self.len += 1;
        index
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
            let i = state.insert(key.clone());
            list.insert(i, label.to_string());
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
}
