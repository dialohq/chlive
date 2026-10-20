//! What a [`Grant`](super::grants::Grant) grants, kept up to date row by row: by the row's
//! replica identity (its primary key), the (subject, value) it grants, so a delete or an update
//! that only carries the key still finds what to take back.

use std::collections::{BTreeMap, BTreeSet, HashMap};

/// A row's replica identity, as text.
pub type Key = Vec<Option<String>>;

#[derive(Debug, Default)]
pub struct GrantIndex {
    rows: HashMap<Key, (String, String)>,
    /// Per subject, its values and how many rows grant each.
    by_subject: HashMap<String, BTreeMap<String, usize>>,
}

impl GrantIndex {
    /// The row `key` now grants `granted` (none: nothing, or it's gone). Returns the subjects
    /// whose values may have changed.
    pub fn apply(&mut self, key: Key, granted: Option<(String, String)>) -> Vec<String> {
        let mut touched = Vec::new();
        let old = match &granted {
            Some(g) => self.rows.insert(key, g.clone()),
            None => self.rows.remove(&key),
        };
        if old == granted {
            return touched;
        }
        if let Some((subject, value)) = old {
            if let Some(values) = self.by_subject.get_mut(&subject) {
                if let Some(n) = values.get_mut(&value) {
                    *n -= 1;
                    if *n == 0 {
                        values.remove(&value);
                    }
                }
                if values.is_empty() {
                    self.by_subject.remove(&subject);
                }
            }
            touched.push(subject);
        }
        if let Some((subject, value)) = granted {
            *self
                .by_subject
                .entry(subject.clone())
                .or_default()
                .entry(value)
                .or_default() += 1;
            if !touched.contains(&subject) {
                touched.push(subject);
            }
        }
        touched
    }

    /// Everything gone (a truncate, or a copy starting over). Returns the subjects it had.
    pub fn clear(&mut self) -> Vec<String> {
        self.rows.clear();
        self.by_subject.drain().map(|(s, _)| s).collect()
    }

    /// The values `subject` may see, sorted.
    pub fn values(&self, subject: &str) -> Vec<String> {
        self.by_subject
            .get(subject)
            .map(|v| v.keys().cloned().collect())
            .unwrap_or_default()
    }

    pub fn subjects(&self) -> BTreeSet<String> {
        self.by_subject.keys().cloned().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(k: &str) -> Key {
        vec![Some(k.to_string())]
    }

    fn g(s: &str, v: &str) -> Option<(String, String)> {
        Some((s.into(), v.into()))
    }

    #[test]
    fn rows_grant_and_take_back() {
        let mut i = GrantIndex::default();
        assert_eq!(i.apply(key("1"), g("alice", "bob")), ["alice"]);
        assert_eq!(i.apply(key("2"), g("alice", "carol")), ["alice"]);
        // the same grant from another row: counted, taken back only with the last
        assert_eq!(i.apply(key("3"), g("alice", "bob")), ["alice"]);
        assert_eq!(i.values("alice"), ["bob", "carol"]);
        assert_eq!(i.apply(key("1"), None), ["alice"]);
        assert_eq!(i.values("alice"), ["bob", "carol"]);
        // an update moving a row to another subject touches both
        assert_eq!(i.apply(key("3"), g("dave", "bob")), ["alice", "dave"]);
        assert_eq!(i.values("alice"), ["carol"]);
        assert_eq!(i.values("dave"), ["bob"]);
        // an unchanged row, or a delete of an unknown one: nothing
        assert!(i.apply(key("3"), g("dave", "bob")).is_empty());
        assert!(i.apply(key("9"), None).is_empty());
        // a row no longer passing the conditions grants nothing
        assert_eq!(i.apply(key("2"), None), ["alice"]);
        assert!(i.values("alice").is_empty());
        assert_eq!(i.subjects(), BTreeSet::from(["dave".to_string()]));
        assert_eq!(i.clear(), ["dave"]);
        assert!(i.values("dave").is_empty());
    }
}
