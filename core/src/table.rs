//! A table as snapshots take it: where it is in ClickHouse, and the topic it's ingested from.

/// Clones are named `__chlive_snap_<table>_<n>` next to their source table.
pub const CLONE_PREFIX: &str = "__chlive_snap_";

/// A table to snapshot: where it is in ClickHouse, and the topic it's ingested from.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SnapshotTable {
    pub database: String,
    pub table: String,
    pub topic: String,
}

impl SnapshotTable {
    pub fn new(database: &str, table: &str, topic: &str) -> Self {
        Self {
            database: database.into(),
            table: table.into(),
            topic: topic.into(),
        }
    }
}
