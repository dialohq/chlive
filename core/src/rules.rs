//! How dialect names map to ClickHouse tables/columns and to Kafka topics / JSON keys, and which
//! tables can be queried at all: [`ConfiguredRules`], from the server config's `[tables]`
//! ([`TablesConfig`]).

use std::collections::BTreeMap;
use std::fmt;

use serde::de::Error as _;
use serde::ser::SerializeMap;
use serde::{Deserialize, Deserializer, Serialize, Serializer};

/// A Kafka topic.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct TopicId(pub String);

impl fmt::Display for TopicId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

/// A field of the Kafka message itself rather than of its JSON value, which Kafka Connect's
/// `InsertField$Value` transform stores in a column (`offset.field`, `partition.field`,
/// `timestamp.field`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum KafkaField {
    Offset,
    Partition,
    /// The message's timestamp, in milliseconds.
    Timestamp,
}

impl fmt::Display for KafkaField {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            KafkaField::Offset => "offset",
            KafkaField::Partition => "partition",
            KafkaField::Timestamp => "timestamp",
        })
    }
}

/// Where an event holds a column's value: `{"json": key}` or `{"kafka": field}` in a plan.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ColumnSource {
    /// A key of the message's JSON object.
    Json(String),
    /// A field of the message.
    Kafka(KafkaField),
}

impl fmt::Display for ColumnSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ColumnSource::Json(key) => f.write_str(key),
            ColumnSource::Kafka(field) => write!(f, "kafka {field}"),
        }
    }
}

/// Where a dialect column lives on each side.
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnMapping {
    /// Name in the dialect (and in output rows).
    pub name: String,
    /// Where events hold it.
    pub source: ColumnSource,
}

pub trait NamingRules: Send + Sync {
    /// Whether queries may use the table at all.
    fn exposes(&self, table: &str) -> bool;
    /// The ClickHouse database and table holding a dialect table's history.
    fn clickhouse_location(&self, table: &str) -> (String, String);
    /// The same, quoted for SQL: `db.table`.
    fn clickhouse_table(&self, table: &str) -> String {
        let (db, name) = self.clickhouse_location(table);
        format!("{}.{}", quote_ident(&db), quote_ident(&name))
    }
    /// Topic carrying a dialect table's new rows.
    fn topic(&self, table: &str) -> TopicId;
    /// Dialect name and event source of one of the table's ClickHouse columns.
    fn column(&self, table: &str, clickhouse_column: &str) -> ColumnMapping;
    /// ClickHouse columns that are not part of the dialect table (e.g. bookkeeping). A column
    /// filled from a Kafka field is never hidden: it was configured to be read.
    fn is_hidden(&self, table: &str, clickhouse_column: &str) -> bool;
    /// Whether the table takes inserts, and how (`[tables.<name>.insert]`); none: it doesn't.
    fn insert(&self, _table: &str) -> Option<crate::security::InsertConfig> {
        None
    }
}

/// Which tables queries can use.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Expose {
    /// Only the tables listed (`[tables.<name>]`).
    Listed,
    /// Every table in `database`, besides the ones listed.
    All,
}

/// The `[tables]` section: defaults for every table, then `[tables.<name>]` for each one listed.
#[derive(Debug, Clone, PartialEq)]
pub struct TablesConfig {
    pub expose: Expose,
    /// Where a table not mapped elsewhere lives.
    pub database: String,
    /// A table's topic, unless set per table: `{table}` and `{database}` are replaced.
    pub topic: String,
    /// ClickHouse columns hidden from every table (globs: `*`, `?`).
    pub hidden_columns: Vec<String>,
    /// The listed tables, by dialect name.
    pub tables: BTreeMap<String, TableConfig>,
}

impl Default for TablesConfig {
    fn default() -> Self {
        Self {
            expose: Expose::Listed,
            database: "default".into(),
            topic: "{table}".into(),
            hidden_columns: vec!["_*".into()],
            tables: BTreeMap::new(),
        }
    }
}

impl TablesConfig {
    /// Every table of `database` exposed, with the default mapping (tests, tools).
    pub fn expose_all(database: &str) -> Self {
        Self {
            expose: Expose::All,
            database: database.into(),
            ..Self::default()
        }
    }

    /// These tables, with the default mapping.
    pub fn listed<'a>(tables: impl IntoIterator<Item = &'a str>) -> Self {
        Self {
            tables: tables
                .into_iter()
                .map(|t| (t.to_string(), TableConfig::default()))
                .collect(),
            ..Self::default()
        }
    }
}

/// `[tables.<name>]`: where one table lives, if not where the defaults say, and its columns.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct TableConfig {
    /// `db.table`, or `table` in the default database.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub clickhouse: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub topic: Option<String>,
    /// Hidden besides the global `hidden_columns`.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub hidden_columns: Vec<String>,
    /// Renamed columns, by dialect name.
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    pub columns: BTreeMap<String, ColumnConfig>,
    /// Who may read which rows (the server's row-level security).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub security: Option<crate::security::TableSecurity>,
    /// Whether it takes inserts, and from whom (see [`crate::security::InsertConfig`]).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub insert: Option<crate::security::InsertConfig>,
}

/// A column's ClickHouse name (default: its dialect name) and where events hold it: a JSON key
/// (default: its ClickHouse name), or a Kafka field (`kafka = "offset"`).
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ColumnConfig {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub clickhouse: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub json: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kafka: Option<KafkaField>,
}

const SETTINGS: [&str; 4] = ["expose", "database", "topic", "hidden_columns"];

// `[tables]` mixes its settings with one sub-table per listed table, which serde's `flatten`
// can't combine with rejecting unknown fields: settings by name, everything else is a table.
impl<'de> Deserialize<'de> for TablesConfig {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let mut raw = BTreeMap::<String, serde_json::Value>::deserialize(d)?;
        let mut out = TablesConfig::default();
        let take = |raw: &mut BTreeMap<String, serde_json::Value>, key: &str| raw.remove(key);
        if let Some(v) = take(&mut raw, "expose") {
            out.expose =
                serde_json::from_value(v).map_err(|e| D::Error::custom(format!("expose: {e}")))?;
        }
        if let Some(v) = take(&mut raw, "database") {
            out.database = serde_json::from_value(v)
                .map_err(|e| D::Error::custom(format!("database: {e}")))?;
        }
        if let Some(v) = take(&mut raw, "topic") {
            out.topic =
                serde_json::from_value(v).map_err(|e| D::Error::custom(format!("topic: {e}")))?;
        }
        if let Some(v) = take(&mut raw, "hidden_columns") {
            out.hidden_columns = serde_json::from_value(v)
                .map_err(|e| D::Error::custom(format!("hidden_columns: {e}")))?;
        }
        for (name, v) in raw {
            if !v.is_object() {
                return Err(D::Error::custom(format!(
                    "{name}: unknown setting (a table is a [tables.{name}] section; settings \
                     are {})",
                    SETTINGS.join(", ")
                )));
            }
            let table =
                serde_json::from_value(v).map_err(|e| D::Error::custom(format!("{name}: {e}")))?;
            out.tables.insert(name, table);
        }
        Ok(out)
    }
}

impl Serialize for TablesConfig {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut m = s.serialize_map(Some(SETTINGS.len() + self.tables.len()))?;
        m.serialize_entry("expose", &self.expose)?;
        m.serialize_entry("database", &self.database)?;
        m.serialize_entry("topic", &self.topic)?;
        m.serialize_entry("hidden_columns", &self.hidden_columns)?;
        for (name, table) in &self.tables {
            m.serialize_entry(name, table)?;
        }
        m.end()
    }
}

/// An invalid `[tables]` section.
#[derive(Debug, Clone, PartialEq, thiserror::Error)]
#[error("tables.{0}")]
pub struct ConfigError(pub String);

/// [`NamingRules`] from a [`TablesConfig`], checked once.
#[derive(Debug, Clone)]
pub struct ConfiguredRules {
    config: TablesConfig,
    /// Per listed table, its renamed columns by ClickHouse name.
    renames: BTreeMap<String, BTreeMap<String, ColumnMapping>>,
}

impl ConfiguredRules {
    pub fn new(config: TablesConfig) -> Result<Self, ConfigError> {
        check_template(&config.topic).map_err(|e| ConfigError(format!("topic: {e}")))?;
        if !is_ident(&config.database) {
            return Err(ConfigError(format!(
                "database: {:?} is not a name",
                config.database
            )));
        }
        let mut renames = BTreeMap::new();
        for (name, t) in &config.tables {
            let err = |what: String| ConfigError(format!("{name}: {what}"));
            if !is_ident(name) {
                return Err(ConfigError(format!(
                    "{name:?}: a table name is letters, digits and _"
                )));
            }
            if let Some(ch) = &t.clickhouse {
                parse_location(ch).map_err(err)?;
            }
            if let Some(topic) = &t.topic {
                if topic.is_empty() {
                    return Err(err("topic: empty".into()));
                }
            }
            let mut by_clickhouse = BTreeMap::new();
            for (col, c) in &t.columns {
                if !is_ident(col) {
                    return Err(err(format!(
                        "columns.{col:?}: a column name is letters, digits and _"
                    )));
                }
                let clickhouse = c.clickhouse.clone().unwrap_or_else(|| col.clone());
                let source = match (&c.json, c.kafka) {
                    (Some(_), Some(_)) => {
                        return Err(err(format!(
                            "columns.{col}: json and kafka both say where it is; set one"
                        )));
                    }
                    (_, Some(field)) => ColumnSource::Kafka(field),
                    (json, None) => {
                        ColumnSource::Json(json.clone().unwrap_or_else(|| clickhouse.clone()))
                    }
                };
                if clickhouse.is_empty() || source == ColumnSource::Json(String::new()) {
                    return Err(err(format!("columns.{col}: empty name")));
                }
                let mapping = ColumnMapping {
                    name: col.clone(),
                    source,
                };
                if let Some(other) = by_clickhouse.insert(clickhouse.clone(), mapping) {
                    return Err(err(format!(
                        "columns: {} and {col} both map ClickHouse column {clickhouse}",
                        other.name
                    )));
                }
            }
            if let Some(i) = &t.insert {
                if i.public && (i.user.is_some() || i.has_grants()) {
                    return Err(err(
                        "insert: public takes anyone's rows, so it has no user or grants".into(),
                    ));
                }
                let names = [("user", &i.user), ("key", &i.key)];
                let row_column = i.rows.as_ref().map(|r| ("rows.column", &r.column));
                for (what, col) in names
                    .iter()
                    .filter_map(|(w, c)| c.as_ref().map(|c| (*w, c)))
                    .chain(row_column)
                {
                    if !is_ident(col) {
                        return Err(err(format!("insert.{what}: {col:?} is not a column name")));
                    }
                }
                if i.user.is_some() && i.user == i.rows.as_ref().map(|r| r.column.clone()) {
                    return Err(err(
                        "insert: the user column is the server's, so rows can't grant it".into(),
                    ));
                }
            }
            renames.insert(name.clone(), by_clickhouse);
        }
        Ok(Self { config, renames })
    }

    /// Every table of `database` exposed, with the default mapping (tests, tools).
    pub fn expose_all(database: &str) -> Self {
        Self::new(TablesConfig::expose_all(database)).expect("the default mapping is valid")
    }

    pub fn config(&self) -> &TablesConfig {
        &self.config
    }
}

impl Default for ConfiguredRules {
    /// Every table of `default`, as is.
    fn default() -> Self {
        Self::expose_all("default")
    }
}

impl NamingRules for ConfiguredRules {
    fn insert(&self, table: &str) -> Option<crate::security::InsertConfig> {
        self.config.tables.get(table).and_then(|t| t.insert.clone())
    }

    fn exposes(&self, table: &str) -> bool {
        self.config.tables.contains_key(table) || self.config.expose == Expose::All
    }

    fn clickhouse_location(&self, table: &str) -> (String, String) {
        match self
            .config
            .tables
            .get(table)
            .and_then(|t| t.clickhouse.as_deref())
        {
            Some(ch) => {
                let (db, name) = parse_location(ch).expect("checked in new");
                (
                    db.unwrap_or(&self.config.database).to_string(),
                    name.to_string(),
                )
            }
            None => (self.config.database.clone(), table.to_string()),
        }
    }

    fn topic(&self, table: &str) -> TopicId {
        if let Some(topic) = self.config.tables.get(table).and_then(|t| t.topic.clone()) {
            return TopicId(topic);
        }
        let (database, _) = self.clickhouse_location(table);
        TopicId(
            self.config
                .topic
                .replace("{table}", table)
                .replace("{database}", &database),
        )
    }

    fn column(&self, table: &str, clickhouse_column: &str) -> ColumnMapping {
        self.renames
            .get(table)
            .and_then(|r| r.get(clickhouse_column))
            .cloned()
            .unwrap_or_else(|| ColumnMapping {
                name: clickhouse_column.to_string(),
                source: ColumnSource::Json(clickhouse_column.to_string()),
            })
    }

    fn is_hidden(&self, table: &str, clickhouse_column: &str) -> bool {
        let configured = self
            .renames
            .get(table)
            .and_then(|r| r.get(clickhouse_column));
        if configured.is_some_and(|m| matches!(m.source, ColumnSource::Kafka(_))) {
            return false;
        }
        let per_table = self.config.tables.get(table).map(|t| &t.hidden_columns);
        self.config
            .hidden_columns
            .iter()
            .chain(per_table.into_iter().flatten())
            .any(|g| glob(g, clickhouse_column))
    }
}

/// `db.table` or `table`.
fn parse_location(s: &str) -> Result<(Option<&str>, &str), String> {
    let bad = || format!("clickhouse: {s:?} is not `database.table` or `table`");
    match s.split_once('.') {
        Some((db, t)) if is_ident(db) && is_ident(t) => Ok((Some(db), t)),
        None if is_ident(s) => Ok((None, s)),
        _ => Err(bad()),
    }
}

/// Only `{table}` and `{database}` may appear in braces.
fn check_template(t: &str) -> Result<(), String> {
    let rest = t.replace("{table}", "").replace("{database}", "");
    if rest.contains('{') || rest.contains('}') {
        return Err(format!(
            "{t:?}: only {{table}} and {{database}} can be replaced"
        ));
    }
    if t.is_empty() {
        return Err("empty".into());
    }
    Ok(())
}

fn is_ident(s: &str) -> bool {
    s.chars()
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && s.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// `*` (any run of characters) and `?` (one character); everything else literally.
pub fn glob(pattern: &str, text: &str) -> bool {
    fn go(p: &[char], t: &[char]) -> bool {
        match p.split_first() {
            None => t.is_empty(),
            Some(('*', rest)) => (0..=t.len()).any(|i| go(rest, &t[i..])),
            Some(('?', rest)) => !t.is_empty() && go(rest, &t[1..]),
            Some((c, rest)) => t.first() == Some(c) && go(rest, &t[1..]),
        }
    }
    let p: Vec<char> = pattern.chars().collect();
    let t: Vec<char> = text.chars().collect();
    go(&p, &t)
}

/// ClickHouse identifier, back-quoted unless it is a plain name.
pub fn quote_ident(name: &str) -> String {
    if is_ident(name) {
        name.to_string()
    } else {
        format!("`{}`", name.replace('\\', "\\\\").replace('`', "\\`"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rules(toml_like: serde_json::Value) -> ConfiguredRules {
        ConfiguredRules::new(serde_json::from_value(toml_like).unwrap()).unwrap()
    }

    fn error(v: serde_json::Value) -> String {
        match serde_json::from_value::<TablesConfig>(v) {
            Err(e) => e.to_string(),
            Ok(c) => ConfiguredRules::new(c).unwrap_err().to_string(),
        }
    }

    #[test]
    fn defaults() {
        let d = TablesConfig::default();
        assert_eq!(d.expose, Expose::Listed);
        assert_eq!(d.database, "default");
        assert_eq!(d.topic, "{table}");
        assert_eq!(d.hidden_columns, ["_*"]);
        assert!(d.tables.is_empty());
        // an empty section is the defaults
        assert_eq!(
            serde_json::from_value::<TablesConfig>(serde_json::json!({})).unwrap(),
            d
        );
        let r = ConfiguredRules::new(d).unwrap();
        assert!(!r.exposes("events"), "listed: nothing without a list");
    }

    #[test]
    fn expose_listed_and_all() {
        let listed = rules(serde_json::json!({ "events": {} }));
        assert!(listed.exposes("events"));
        assert!(!listed.exposes("orders"));
        let all = rules(serde_json::json!({ "expose": "all", "events": {} }));
        assert!(all.exposes("events") && all.exposes("orders"));
        assert!(error(serde_json::json!({ "expose": "some" })).contains("expose"));
    }

    #[test]
    fn locations_default_and_per_table() {
        let r = rules(serde_json::json!({
            "database": "analytics",
            "events": {},
            "orders": { "clickhouse": "shop.orders_v2" },
            "customers": { "clickhouse": "people" },
        }));
        assert_eq!(r.clickhouse_table("events"), "analytics.events");
        assert_eq!(
            r.clickhouse_location("orders"),
            ("shop".into(), "orders_v2".into())
        );
        assert_eq!(r.clickhouse_table("customers"), "analytics.people");
        // expose = all: unlisted tables are in `database` under their own name
        assert_eq!(
            ConfiguredRules::expose_all("x").clickhouse_table("my events"),
            "x.`my events`"
        );
        for bad in ["a.b.c", "", ".t", "d.", "a b"] {
            let e = error(serde_json::json!({ "t": { "clickhouse": bad } }));
            assert!(e.contains("tables.t: clickhouse:"), "{bad:?}: {e}");
        }
    }

    #[test]
    fn topics_per_table_and_from_the_template() {
        let r = rules(serde_json::json!({
            "topic": "{database}.{table}.v1",
            "events": {},
            "orders": { "topic": "prod.orders" },
            "customers": { "clickhouse": "crm.people" },
        }));
        assert_eq!(r.topic("events"), TopicId("default.events.v1".into()));
        assert_eq!(r.topic("orders"), TopicId("prod.orders".into()));
        // the template's {database} is the table's own, {table} its dialect name
        assert_eq!(r.topic("customers"), TopicId("crm.customers.v1".into()));
        // the default template
        assert_eq!(
            ConfiguredRules::default().topic("events"),
            TopicId("events".into())
        );
        assert!(error(serde_json::json!({ "topic": "{schema}.{table}" })).contains("tables.topic"));
        assert!(error(serde_json::json!({ "t": { "topic": "" } })).contains("tables.t: topic"));
    }

    #[test]
    fn hidden_columns_global_and_per_table() {
        let r = rules(serde_json::json!({
            "hidden_columns": ["_*", "tmp?"],
            "events": { "hidden_columns": ["internal_*", "secret"] },
            "orders": {},
        }));
        assert!(r.is_hidden("events", "_offset"));
        assert!(r.is_hidden("events", "tmp1") && !r.is_hidden("events", "tmp12"));
        assert!(r.is_hidden("events", "internal_note") && r.is_hidden("events", "secret"));
        assert!(!r.is_hidden("events", "secrets") && !r.is_hidden("events", "ts"));
        // per-table lists add to the global one, for that table only
        assert!(r.is_hidden("orders", "_offset") && !r.is_hidden("orders", "secret"));
        // the default: `_*`
        let d = ConfiguredRules::default();
        assert!(d.is_hidden("events", "_partition") && !d.is_hidden("events", "a_b"));
        // no hiding at all
        let none = rules(serde_json::json!({ "hidden_columns": [] , "t": {} }));
        assert!(!none.is_hidden("t", "_offset"));
    }

    #[test]
    fn column_renames() {
        let r = rules(serde_json::json!({
            "events": { "columns": {
                "customer": { "clickhouse": "customer_id", "json": "customerId" },
                "who": { "clickhouse": "user_id" },
                "latency_ms": { "json": "latencyMs" },
                "same": {},
            }},
        }));
        let m = |c: &str| {
            let m = r.column("events", c);
            let ColumnSource::Json(key) = m.source else {
                panic!("{c}: not from JSON");
            };
            (m.name, key)
        };
        assert_eq!(m("customer_id"), ("customer".into(), "customerId".into()));
        // the JSON key defaults to the ClickHouse name
        assert_eq!(m("user_id"), ("who".into(), "user_id".into()));
        assert_eq!(m("latency_ms"), ("latency_ms".into(), "latencyMs".into()));
        assert_eq!(m("same"), ("same".into(), "same".into()));
        // unmapped columns, and other tables, keep their names
        assert_eq!(m("ts"), ("ts".into(), "ts".into()));
        assert_eq!(r.column("orders", "customer_id").name, "customer_id");
        let e = error(serde_json::json!({ "t": { "columns": {
            "a": { "clickhouse": "x" }, "b": { "clickhouse": "x" } } } }));
        assert!(e.contains("both map ClickHouse column x"), "{e}");
        assert!(
            error(serde_json::json!({ "t": { "columns": { "a b": {} } } })).contains("columns")
        );
    }

    #[test]
    fn kafka_columns() {
        let r = rules(serde_json::json!({
            "events": { "columns": {
                "kafka_offset": { "kafka": "offset" },
                "part": { "clickhouse": "_partition", "kafka": "partition" },
                "kafka_ts": { "kafka": "timestamp" },
                "_internal": {},
            }},
        }));
        let source = |c: &str| r.column("events", c).source;
        assert_eq!(
            source("kafka_offset"),
            ColumnSource::Kafka(KafkaField::Offset)
        );
        assert_eq!(
            source("_partition"),
            ColumnSource::Kafka(KafkaField::Partition)
        );
        assert_eq!(r.column("events", "_partition").name, "part");
        assert_eq!(
            source("kafka_ts"),
            ColumnSource::Kafka(KafkaField::Timestamp)
        );
        assert_eq!(source("ts"), ColumnSource::Json("ts".into()));
        assert_eq!(source("kafka_ts").to_string(), "kafka timestamp");
        // a Kafka column is read whatever the hiding globs say; a JSON one isn't
        assert!(!r.is_hidden("events", "_partition"));
        assert!(r.is_hidden("events", "_internal") && r.is_hidden("events", "_offset"));
        assert!(
            r.is_hidden("orders", "_partition"),
            "only for its own table"
        );
        let e = error(serde_json::json!({ "t": { "columns": {
            "a": { "json": "a", "kafka": "offset" } } } }));
        assert!(e.contains("columns.a: json and kafka both"), "{e}");
        let e = error(serde_json::json!({ "t": { "columns": { "a": { "kafka": "key" } } } }));
        assert!(e.contains("unknown variant `key`"), "{e}");
    }

    #[test]
    fn names_and_unknown_settings() {
        assert!(error(serde_json::json!({ "my-table": {} })).contains("a table name"));
        assert!(error(serde_json::json!({ "database": "a.b" })).contains("tables.database"));
        // a scalar that isn't a setting: a typo, not a table
        let e = error(serde_json::json!({ "exposed": "all" }));
        assert!(e.contains("exposed: unknown setting"), "{e}");
        // inside a table, unknown keys are rejected too
        let e = error(serde_json::json!({ "t": { "topik": "x" } }));
        assert!(e.contains("t: unknown field `topik`"), "{e}");
    }

    #[test]
    fn round_trips_through_serialize() {
        let c: TablesConfig = serde_json::from_value(serde_json::json!({
            "expose": "all", "database": "d", "topic": "t.{table}", "hidden_columns": ["x"],
            "events": { "clickhouse": "a.b", "topic": "e", "hidden_columns": ["y"],
                        "columns": { "c": { "clickhouse": "cc", "json": "cj" }, "k": { "kafka": "timestamp" } } },
            "orders": {},
        }))
        .unwrap();
        let back: TablesConfig = serde_json::from_value(serde_json::to_value(&c).unwrap()).unwrap();
        assert_eq!(back, c);
    }

    #[test]
    fn globs() {
        assert!(glob("_*", "_") && glob("_*", "_offset") && !glob("_*", "x_"));
        assert!(glob("a?c", "abc") && !glob("a?c", "ac"));
        assert!(glob("*x*", "axb") && glob("*", "") && !glob("", "a"));
        assert!(glob("exact", "exact") && !glob("exact", "exactly"));
    }

    #[test]
    fn quoting() {
        assert_eq!(quote_ident("events"), "events");
        assert_eq!(quote_ident("my events"), "`my events`");
        assert_eq!(quote_ident("a`b"), "`a\\`b`");
    }
}
