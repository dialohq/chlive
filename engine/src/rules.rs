//! How dialect names map to ClickHouse tables/columns and to Kafka topics / JSON keys.
//! Hard-coded for now; later these become user-configurable.

use std::fmt;

/// A Kafka topic.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TopicId(pub String);

impl fmt::Display for TopicId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// Where a dialect column lives on each side.
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnMapping {
    /// Name in the dialect (and in output rows).
    pub name: String,
    /// Key in the topic's JSON objects.
    pub json_key: String,
}

pub trait NamingRules: Send + Sync {
    /// The ClickHouse database and table holding a dialect table's history.
    fn clickhouse_location(&self, table: &str) -> (String, String);
    /// The same, quoted for SQL: `db.table`.
    fn clickhouse_table(&self, table: &str) -> String {
        let (db, name) = self.clickhouse_location(table);
        format!("{}.{}", quote_ident(&db), quote_ident(&name))
    }
    /// Topic carrying a dialect table's new rows.
    fn topic(&self, table: &str) -> TopicId;
    /// Dialect name and JSON key of a ClickHouse column.
    fn column(&self, clickhouse_column: &str) -> ColumnMapping;
    /// ClickHouse columns that are not part of the dialect table (e.g. bookkeeping).
    fn is_hidden(&self, clickhouse_column: &str) -> bool;
}

/// Identity mapping: table `t` is ClickHouse `<database>.t` and topic `t`; columns keep their
/// names everywhere; columns starting with `_` are hidden.
#[derive(Debug, Clone)]
pub struct HardcodedRules {
    pub database: String,
}

impl Default for HardcodedRules {
    fn default() -> Self {
        Self {
            database: "default".into(),
        }
    }
}

impl NamingRules for HardcodedRules {
    fn clickhouse_location(&self, table: &str) -> (String, String) {
        (self.database.clone(), table.to_string())
    }

    fn topic(&self, table: &str) -> TopicId {
        TopicId(table.to_string())
    }

    fn column(&self, clickhouse_column: &str) -> ColumnMapping {
        ColumnMapping {
            name: clickhouse_column.to_string(),
            json_key: clickhouse_column.to_string(),
        }
    }

    fn is_hidden(&self, clickhouse_column: &str) -> bool {
        clickhouse_column.starts_with('_')
    }
}

/// ClickHouse identifier, back-quoted unless it is a plain name.
pub fn quote_ident(name: &str) -> String {
    let plain = name
        .chars()
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_');
    if plain {
        name.to_string()
    } else {
        format!("`{}`", name.replace('\\', "\\\\").replace('`', "\\`"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hardcoded_rules() {
        let r = HardcodedRules::default();
        assert_eq!(r.clickhouse_table("events"), "default.events");
        assert_eq!(r.clickhouse_table("my events"), "default.`my events`");
        assert_eq!(r.topic("events"), TopicId("events".into()));
        assert_eq!(r.column("ts").json_key, "ts");
        assert!(r.is_hidden("_offset"));
        assert!(!r.is_hidden("ts"));
    }
}
