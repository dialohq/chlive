use std::path::Path;

use anyhow::{Context, Result};
use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(default = "default_listen")]
    pub listen: String,
    /// Minimum interval between merges of one subscription (leading edge: a change after a
    /// quiet period merges immediately; bursts coalesce to one merge per interval).
    #[serde(default = "default_debounce_ms")]
    pub debounce_ms: u64,
    pub main: ClickHouseConfig,
    #[serde(default)]
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
    /// Native-protocol address of main as seen from this process, used by `remote()` from
    /// the embedded chDB cache.
    #[serde(default = "default_native_address")]
    pub native_address: String,
}

/// The embedded chDB cache (in-process, in-memory; it holds no durable state).
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CacheConfig {
    /// Number of chDB connections; each runs one statement at a time.
    #[serde(default = "default_pool_size")]
    pub pool_size: usize,
    /// Table expression the cache uses to read main. `{database}` and `{table}` are replaced.
    /// `main_ro` is a named collection created at startup from `[main]`.
    #[serde(default = "default_remote_source")]
    pub remote_source: String,
}

impl Default for CacheConfig {
    fn default() -> Self {
        Self {
            pool_size: default_pool_size(),
            remote_source: default_remote_source(),
        }
    }
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
fn default_database() -> String {
    "default".into()
}
fn default_native_address() -> String {
    "localhost:19000".into()
}
fn default_pool_size() -> usize {
    4
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
