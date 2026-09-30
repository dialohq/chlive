use std::path::Path;

use anyhow::{Context, Result};
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(default = "default_listen")]
    pub listen: String,
    /// Per-subscription debounce between a tail advance and the merge query.
    #[serde(default = "default_debounce_ms")]
    pub debounce_ms: u64,
    /// Maximum LIMIT for Rows plans and maximum result size for Aggregate plans.
    #[serde(default = "default_rows_limit_cap")]
    pub rows_limit_cap: u64,
    pub main: ClickHouseConfig,
    pub cache: CacheConfig,
    pub kafka: KafkaConfig,
    /// Allowlisted source tables.
    pub tables: Vec<TableConfig>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClickHouseConfig {
    pub url: String,
    pub user: String,
    #[serde(default)]
    pub password: String,
    #[serde(default = "default_database")]
    pub database: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CacheConfig {
    pub url: String,
    pub user: String,
    #[serde(default)]
    pub password: String,
    /// Database in the cache server holding `tail_*` and `sub_*_hist` tables. It is wiped
    /// on startup: the cache holds no durable state.
    #[serde(default = "default_cache_database")]
    pub database: String,
    /// Table expression the cache uses to read main. `{database}` and `{table}` are replaced.
    #[serde(default = "default_remote_source")]
    pub remote_source: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KafkaConfig {
    pub brokers: String,
    #[serde(default = "default_batch_rows")]
    pub batch_rows: usize,
    #[serde(default = "default_batch_ms")]
    pub batch_ms: u64,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TableConfig {
    pub name: String,
    pub topic: String,
}

fn default_listen() -> String {
    "0.0.0.0:8080".into()
}
fn default_debounce_ms() -> u64 {
    100
}
fn default_rows_limit_cap() -> u64 {
    1000
}
fn default_database() -> String {
    "default".into()
}
fn default_cache_database() -> String {
    "chlive".into()
}
fn default_remote_source() -> String {
    "remote(main_ro, database = '{database}', table = '{table}')".into()
}
fn default_batch_rows() -> usize {
    1000
}
fn default_batch_ms() -> u64 {
    50
}

impl Config {
    pub fn load(path: &Path) -> Result<Self> {
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("reading config {}", path.display()))?;
        let cfg: Config =
            toml::from_str(&text).with_context(|| format!("parsing {}", path.display()))?;
        anyhow::ensure!(
            !cfg.tables.is_empty(),
            "config must allowlist at least one table"
        );
        for t in &cfg.tables {
            anyhow::ensure!(
                t.name
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || c == '_'),
                "table name {:?} must be a plain identifier",
                t.name
            );
        }
        Ok(cfg)
    }
}
