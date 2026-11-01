//! The server's configuration: built-in defaults, then a TOML file (`--config`), then
//! environment variables (`CHLIVE_<SECTION>__<KEY>`), then flags (`--set key=value` and a few
//! shortcuts), each layer overriding the one before. Unknown keys are errors.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{bail, Context, Result};
use chlive_engine::rules::{ConfiguredRules, TablesConfig};
use chlive_engine::snapshot::SnapshotConfig;
use figment::providers::{Env, Format, Serialized, Toml};
use figment::value::{Dict, Map};
use figment::{Figment, Metadata, Profile, Provider};
use serde::{Deserialize, Serialize};

use crate::security::{Security, SecurityConfig};
use crate::ServerSettings;

/// The sections, in order; env vars are read only for these (`CHLIVE_URL` and the like aren't
/// config).
pub const SECTIONS: [&str; 9] = [
    "server",
    "cache",
    "clickhouse",
    "kafka",
    "pause_server",
    "snapshots",
    "log",
    "tables",
    "security",
];

#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Config {
    pub server: Server,
    pub cache: Cache,
    pub clickhouse: ClickHouse,
    pub kafka: Kafka,
    pub pause_server: PauseServer,
    pub snapshots: Snapshots,
    pub log: Log,
    pub tables: TablesConfig,
    /// Who may read which rows (see [`crate::security`]).
    pub security: SecurityConfig,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Server {
    pub listen: String,
    pub path: String,
    pub allowed_origins: Vec<String>,
    pub max_connections: usize,
    pub max_subscriptions_per_connection: usize,
    #[serde(with = "humantime_serde")]
    pub progress_interval: Duration,
    /// `now()`: rows due within this long of each other leave together (at most this late).
    #[serde(with = "humantime_serde")]
    pub now_coalescing: Duration,
    /// A `complete` subscription waits at most this long for its snapshot (else `CATCH_UP_TIMEOUT`).
    #[serde(with = "humantime_serde")]
    pub complete_wait: Duration,
    /// Rows one `insert` carries at most.
    pub max_insert_rows: usize,
    /// Inserts one connection has unanswered at most.
    pub max_inserts_in_flight: usize,
}

impl Default for Server {
    fn default() -> Self {
        let s = ServerSettings::default();
        Self {
            listen: "0.0.0.0:8080".into(),
            path: s.path,
            allowed_origins: s.allowed_origins,
            max_connections: s.max_connections,
            max_subscriptions_per_connection: s.max_subscriptions_per_connection,
            progress_interval: s.progress_interval,
            now_coalescing: chlive_engine::engine::NOW_COALESCING,
            complete_wait: s.complete_wait,
            max_insert_rows: s.max_insert_rows,
            max_inserts_in_flight: s.max_inserts_in_flight,
        }
    }
}

/// Shared results (see `crate::hub`): how long they're kept after the last subscriber, and
/// their change logs.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Cache {
    /// The disk store of results (warm and cold stages); "" turns it off.
    pub dir: String,
    /// After the last subscriber leaves, still live (in memory).
    #[serde(with = "humantime_serde")]
    pub keep_alive: Duration,
    /// Then warm (following Kafka, rows on disk), then cold.
    #[serde(with = "humantime_serde")]
    pub keep_warm: Duration,
    /// Results warm at most; the one warm the longest goes cold first.
    pub max_warm: usize,
    /// `diff` messages kept in memory per live result, for resuming clients.
    pub log_length: usize,
    /// Cold results are evicted, least recently used first, beyond this on disk.
    #[serde(with = "size")]
    pub disk_budget: u64,
    /// A stored result whose snapshot is older is rebuilt from ClickHouse.
    #[serde(with = "humantime_serde")]
    pub max_age: Duration,
    /// A stored result more events behind Kafka is rebuilt from ClickHouse.
    pub max_catch_up: u64,
    /// How often running results save their engine state.
    #[serde(with = "humantime_serde")]
    pub checkpoint_interval: Duration,
    /// A result's log on disk past this is folded into its rows.
    #[serde(with = "size")]
    pub log_bytes: u64,
}

impl Default for Cache {
    fn default() -> Self {
        let d = crate::CacheSettings::default();
        Self {
            dir: "./chlive-cache".into(),
            keep_alive: d.keep_alive,
            keep_warm: d.keep_warm,
            max_warm: d.max_warm,
            log_length: d.log_length,
            disk_budget: d.disk_budget,
            max_age: d.max_age,
            max_catch_up: d.max_catch_up,
            checkpoint_interval: d.checkpoint_interval,
            log_bytes: d.log_bytes,
        }
    }
}

/// Sizes: "10GiB", "16MiB", "512KiB", "100B", "1GB", or a number of bytes.
pub mod size {
    use serde::{de, Deserialize, Deserializer, Serializer};

    pub fn parse(text: &str) -> Result<u64, String> {
        let t = text.trim();
        let split = t
            .find(|c: char| !c.is_ascii_digit() && c != '.')
            .unwrap_or(t.len());
        let (n, unit) = (t[..split].trim(), t[split..].trim());
        let n: f64 = n
            .parse()
            .map_err(|_| format!("{text:?}: not a size (e.g. \"10GiB\", \"16MiB\")"))?;
        let mult: u64 = match unit.to_ascii_lowercase().as_str() {
            "" | "b" => 1,
            "kib" => 1 << 10,
            "mib" => 1 << 20,
            "gib" => 1 << 30,
            "tib" => 1 << 40,
            "kb" => 1_000,
            "mb" => 1_000_000,
            "gb" => 1_000_000_000,
            "tb" => 1_000_000_000_000,
            _ => {
                return Err(format!(
                    "{text:?}: unknown unit {unit:?} (B, KiB, MiB, GiB, TiB, KB, MB, GB, TB)"
                ))
            }
        };
        Ok((n * mult as f64) as u64)
    }

    pub fn format(bytes: u64) -> String {
        for (unit, m) in [
            ("TiB", 1u64 << 40),
            ("GiB", 1 << 30),
            ("MiB", 1 << 20),
            ("KiB", 1 << 10),
        ] {
            if bytes >= m && bytes.is_multiple_of(m) {
                return format!("{}{unit}", bytes / m);
            }
        }
        format!("{bytes}B")
    }

    pub fn serialize<S: Serializer>(v: &u64, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&format(*v))
    }

    pub fn deserialize<'de, D: Deserializer<'de>>(d: D) -> Result<u64, D::Error> {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum Either {
            N(u64),
            S(String),
        }
        match Either::deserialize(d)? {
            Either::N(n) => Ok(n),
            Either::S(s) => parse(&s).map_err(de::Error::custom),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ClickHouse {
    /// HTTP: history, snapshots, and the states aggregates start from (the node snapshots are
    /// made on).
    pub url: String,
    /// Reads history (SELECT on the tables and their snapshots).
    pub user: String,
    /// The user's password: as is, from an env var, or from a file (one at most; none: empty).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub password: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub password_env: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub password_file: Option<PathBuf>,
    /// Creates and drops snapshots.
    pub snapshot_user: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snapshot_password: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snapshot_password_env: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snapshot_password_file: Option<PathBuf>,
    /// Where snapshots are created; unset: next to their tables.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub snapshot_database: Option<String>,
}

impl Default for ClickHouse {
    fn default() -> Self {
        Self {
            url: "http://127.0.0.1:8123".into(),
            user: "chlive_ro".into(),
            password: None,
            password_env: None,
            password_file: None,
            snapshot_user: "chlive_snap".into(),
            snapshot_password: None,
            snapshot_password_env: None,
            snapshot_password_file: None,
            snapshot_database: None,
        }
    }
}

impl ClickHouse {
    pub fn password(&self) -> Result<String> {
        resolve_secret(
            "clickhouse.password",
            &self.password,
            &self.password_env,
            &self.password_file,
        )
    }

    pub fn snapshot_password(&self) -> Result<String> {
        resolve_secret(
            "clickhouse.snapshot_password",
            &self.snapshot_password,
            &self.snapshot_password_env,
            &self.snapshot_password_file,
        )
    }
}

/// A password given as is, from an env var, or from a file (its trailing newline trimmed);
/// `name` is its key, for errors.
fn resolve_secret(
    name: &str,
    value: &Option<String>,
    env: &Option<String>,
    file: &Option<PathBuf>,
) -> Result<String> {
    match (value, env, file) {
        (None, None, None) => Ok(String::new()),
        (Some(p), None, None) => Ok(p.clone()),
        (None, Some(var), None) => std::env::var(var)
            .with_context(|| format!("{name}_env: environment variable {var} is not set")),
        (None, None, Some(path)) => std::fs::read_to_string(path)
            .map(|s| s.trim_end_matches(['\n', '\r']).to_string())
            .with_context(|| format!("{name}_file: reading {}", path.display())),
        _ => bail!("{name}: set only one of {name}, {name}_env and {name}_file"),
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Kafka {
    pub brokers: String,
    #[serde(with = "humantime_serde")]
    pub metadata_refresh: Duration,
    /// librdkafka properties for every client (TLS, SASL, …).
    pub properties: BTreeMap<String, String>,
}

impl Default for Kafka {
    fn default() -> Self {
        Self {
            brokers: "127.0.0.1:9092".into(),
            metadata_refresh: Duration::from_secs(30),
            properties: BTreeMap::new(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct PauseServer {
    pub url: String,
}

impl Default for PauseServer {
    fn default() -> Self {
        Self {
            url: "http://127.0.0.1:18091".into(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Snapshots {
    /// How often a table in use gets a new snapshot.
    #[serde(with = "humantime_serde")]
    pub refresh: Duration,
    /// After this long without subscriptions, a table's snapshots stop.
    #[serde(with = "humantime_serde")]
    pub idle: Duration,
}

impl Default for Snapshots {
    fn default() -> Self {
        let d = SnapshotConfig::default();
        Self {
            refresh: d.refresh,
            idle: d.idle,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct Log {
    /// An `EnvFilter` directive (`info`, `warn,chlive_server=debug`); `RUST_LOG` wins when set.
    pub level: String,
    pub format: LogFormat,
}

impl Default for Log {
    fn default() -> Self {
        Self {
            level: "info".into(),
            format: LogFormat::Text,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LogFormat {
    Text,
    Json,
}

/// Flags: a config file, `--set key=value`s and shortcuts for single keys.
#[derive(Debug, Clone, Default)]
pub struct Overrides {
    pub file: Option<PathBuf>,
    /// `(key.path, value)`, the value as TOML (`5`, `true`, `["a"]`) or else a string.
    pub sets: Vec<(String, String)>,
}

impl Overrides {
    /// `key.path=value`.
    pub fn set(&mut self, assignment: &str) -> Result<()> {
        let (k, v) = assignment
            .split_once('=')
            .with_context(|| format!("--set {assignment}: expected key.path=value"))?;
        self.sets.push((k.trim().to_string(), v.to_string()));
        Ok(())
    }
}

/// A provider with its own name, for where values came from (`config show`).
struct Named<P>(P, &'static str);

impl<P: Provider> Provider for Named<P> {
    fn metadata(&self) -> Metadata {
        let mut m = self.0.metadata();
        m.name = self.1.into();
        m
    }
    fn data(&self) -> figment::Result<Map<Profile, Dict>> {
        self.0.data()
    }
}

/// A `--set` value: TOML if it reads as one, else the text itself.
fn flag_value(text: &str) -> figment::value::Value {
    match toml::from_str::<toml::Table>(&format!("v = {text}")) {
        Ok(mut t) => {
            let v = t.remove("v").expect("just parsed");
            figment::value::Value::serialize(v).expect("TOML values serialize")
        }
        Err(_) => figment::value::Value::from(text.to_string()),
    }
}

/// The layers, without extracting (also what `config show` reads the sources from).
pub fn figment(o: &Overrides) -> Figment {
    let mut f = Figment::from(Named(Serialized::defaults(Config::default()), "default"));
    if let Some(file) = &o.file {
        f = f.merge(Named(Toml::file_exact(file), "file"));
    }
    let env = Env::prefixed("CHLIVE_").split("__").filter(|k| {
        let first = k.as_str().split("__").next().unwrap_or_default();
        let first = first.split('.').next().unwrap_or_default();
        SECTIONS.iter().any(|s| first.eq_ignore_ascii_case(s))
    });
    f = f.merge(Named(env, "env"));
    for (key, value) in &o.sets {
        f = f.merge(Named(Serialized::default(key, flag_value(value)), "flag"));
    }
    f
}

impl Config {
    /// Every layer, extracted and checked.
    pub fn load(o: &Overrides) -> Result<Self> {
        let config: Config = figment(o).extract().map_err(|e| {
            // one line per error, with the key it's about
            let lines: Vec<String> = e.into_iter().map(|e| e.to_string()).collect();
            anyhow::anyhow!("invalid config:\n  {}", lines.join("\n  "))
        })?;
        config.check()?;
        Ok(config)
    }

    /// What deserializing can't tell: values that must make sense together, or as names.
    pub fn check(&self) -> Result<()> {
        let positive = |name: &str, d: Duration| {
            if d.is_zero() {
                bail!("{name}: must be more than 0")
            }
            Ok(())
        };
        positive("server.progress_interval", self.server.progress_interval)?;
        positive("server.complete_wait", self.server.complete_wait)?;
        positive("kafka.metadata_refresh", self.kafka.metadata_refresh)?;
        positive("snapshots.refresh", self.snapshots.refresh)?;
        positive("snapshots.idle", self.snapshots.idle)?;
        if !self.server.path.starts_with('/') {
            bail!("server.path: {:?} must start with /", self.server.path);
        }
        if self.server.max_insert_rows == 0 || self.server.max_inserts_in_flight == 0 {
            bail!("server.max_insert_rows and server.max_inserts_in_flight: must be more than 0");
        }
        if self.server.max_connections == 0 || self.server.max_subscriptions_per_connection == 0 {
            bail!("server.max_connections and server.max_subscriptions_per_connection: must be more than 0");
        }
        if self.cache.log_length == 0 {
            bail!("cache.log_length: must be more than 0");
        }
        positive("cache.checkpoint_interval", self.cache.checkpoint_interval)?;
        if self.cache.disk_budget == 0 || self.cache.log_bytes == 0 {
            bail!("cache.disk_budget and cache.log_bytes: must be more than 0");
        }
        if self.server.listen.is_empty() {
            bail!("server.listen: empty");
        }
        if let Some(db) = &self.clickhouse.snapshot_database {
            if db.is_empty() {
                bail!("clickhouse.snapshot_database: empty (leave it out to snapshot next to the tables)");
            }
        }
        self.clickhouse.password()?;
        self.clickhouse.snapshot_password()?;
        tracing_subscriber::EnvFilter::try_new(&self.log.level)
            .map_err(|e| anyhow::anyhow!("log.level: {:?}: {e}", self.log.level))?;
        ConfiguredRules::new(self.tables.clone())?;
        Security::check(&self.security, &self.tables)?;
        Ok(())
    }

    /// The engine's config, passwords read.
    pub fn engine(&self) -> Result<chlive_engine::Config> {
        Ok(chlive_engine::Config {
            clickhouse_url: self.clickhouse.url.clone(),
            user: self.clickhouse.user.clone(),
            password: self.clickhouse.password()?,
            tables: self.tables.clone(),
            kafka_brokers: self.kafka.brokers.clone(),
            kafka_properties: self.kafka.properties.clone(),
            metadata_refresh: self.kafka.metadata_refresh,
            pause_server_url: self.pause_server.url.clone(),
            snapshot_user: self.clickhouse.snapshot_user.clone(),
            snapshot_password: self.clickhouse.snapshot_password()?,
            snapshots: SnapshotConfig {
                refresh: self.snapshots.refresh,
                idle: self.snapshots.idle,
                // the server owns the clones: leftovers of an earlier run go
                sweep: true,
                database: self.clickhouse.snapshot_database.clone(),
            },
            now_coalescing: self.server.now_coalescing,
        })
    }

    /// The server's settings; its security is open until [`Security::start`] replaces it.
    pub fn server_settings(&self) -> ServerSettings {
        ServerSettings {
            security: std::sync::Arc::new(Security::open()),
            path: self.server.path.clone(),
            allowed_origins: self.server.allowed_origins.clone(),
            max_connections: self.server.max_connections,
            max_subscriptions_per_connection: self.server.max_subscriptions_per_connection,
            progress_interval: self.server.progress_interval,
            complete_wait: self.server.complete_wait,
            max_insert_rows: self.server.max_insert_rows,
            max_inserts_in_flight: self.server.max_inserts_in_flight,
            cache: crate::CacheSettings {
                dir: (!self.cache.dir.is_empty()).then(|| self.cache.dir.clone().into()),
                keep_alive: self.cache.keep_alive,
                keep_warm: self.cache.keep_warm,
                max_warm: self.cache.max_warm,
                log_length: self.cache.log_length,
                disk_budget: self.cache.disk_budget,
                max_age: self.cache.max_age,
                max_catch_up: self.cache.max_catch_up,
                checkpoint_interval: self.cache.checkpoint_interval,
                log_bytes: self.cache.log_bytes,
            },
        }
    }
}

/// The effective config as TOML, passwords hidden, each value followed by where it came from.
pub fn show(o: &Overrides) -> Result<String> {
    let config = Config::load(o)?;
    let figment = figment(o);
    let toml::Value::Table(mut value) =
        toml::Value::try_from(&config).context("rendering the config")?
    else {
        unreachable!("a struct is a table")
    };
    for key in ["password", "snapshot_password"] {
        if let Some(toml::Value::Table(ch)) = value.get_mut("clickhouse") {
            if let Some(v) = ch.get_mut(key) {
                *v = toml::Value::String("<redacted>".into());
            }
        }
    }
    let mut out = String::new();
    render(&figment, &mut out, &mut Vec::new(), &value);
    Ok(out)
}

fn render(f: &Figment, out: &mut String, path: &mut Vec<String>, table: &toml::Table) {
    let (scalars, tables): (Vec<_>, Vec<_>) = table
        .iter()
        .partition(|(_, v)| !matches!(v, toml::Value::Table(_)));
    if !path.is_empty() && (!scalars.is_empty() || tables.is_empty()) {
        out.push_str(&format!("\n[{}]\n", path.join(".")));
    }
    for (k, v) in scalars {
        path.push(k.clone());
        let line = format!("{} = {}", toml_key(k), v);
        out.push_str(&format!("{line:<48} # {}\n", source(f, path)));
        path.pop();
    }
    for (k, v) in tables {
        if let toml::Value::Table(t) = v {
            path.push(toml_key(k));
            render(f, out, path, t);
            path.pop();
        }
    }
}

fn toml_key(k: &str) -> String {
    if !k.is_empty()
        && k.chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
    {
        k.to_string()
    } else {
        format!("{k:?}")
    }
}

/// Where a key's value came from: `default`, `file <path>`, `env <VAR>` or `flag`.
fn source(f: &Figment, path: &[String]) -> String {
    let key = path.join(".");
    let Some(m) = f.find_metadata(&key) else {
        return "default".into();
    };
    match m.name.as_ref() {
        "file" => match &m.source {
            Some(figment::Source::File(p)) => format!("file {}", p.display()),
            _ => "file".into(),
        },
        "env" => format!(
            "env CHLIVE_{}",
            path.iter()
                .map(|k| k.trim_matches('"').to_uppercase())
                .collect::<Vec<_>>()
                .join("__")
        ),
        other => other.to_string(),
    }
}

/// `config check`: load, or say what's wrong.
pub fn check_file(file: &Path, mut o: Overrides) -> Result<Config> {
    o.file = Some(file.to_path_buf());
    Config::load(&o)
}

#[cfg(test)]
// figment's `Jail` returns its large error type
#[allow(clippy::result_large_err)]
mod tests {
    use super::*;
    use chlive_engine::rules::{ColumnConfig, Expose, KafkaField, TableConfig};
    use chlive_engine::security::InsertConfig;
    use figment::Jail;

    fn load(jail: &mut Jail, toml: &str) -> Result<Config> {
        jail.create_file("chlive.toml", toml).unwrap();
        Config::load(&Overrides {
            file: Some("chlive.toml".into()),
            ..Default::default()
        })
    }

    fn err(jail: &mut Jail, toml: &str) -> String {
        format!("{:#}", load(jail, toml).unwrap_err())
    }

    /// Every default, spelled out: what the README and the example file document.
    #[test]
    fn defaults() {
        let d = Config::default();
        assert_eq!(
            d.server,
            Server {
                listen: "0.0.0.0:8080".into(),
                path: "/live".into(),
                allowed_origins: vec![],
                max_connections: 10_000,
                max_subscriptions_per_connection: 100,
                progress_interval: Duration::from_secs(1),
                now_coalescing: Duration::from_millis(100),
                complete_wait: Duration::from_secs(30),
                max_insert_rows: 1000,
                max_inserts_in_flight: 100,
            }
        );
        assert_eq!(
            d.cache,
            Cache {
                dir: "./chlive-cache".into(),
                keep_alive: Duration::from_secs(30),
                keep_warm: Duration::from_secs(3600),
                max_warm: 100,
                log_length: 1000,
                disk_budget: 10 << 30,
                max_age: Duration::from_secs(24 * 3600),
                max_catch_up: 1_000_000,
                checkpoint_interval: Duration::from_secs(60),
                log_bytes: 16 << 20,
            }
        );
        assert_eq!(
            d.clickhouse,
            ClickHouse {
                url: "http://127.0.0.1:8123".into(),
                user: "chlive_ro".into(),
                password: None,
                password_env: None,
                password_file: None,
                snapshot_user: "chlive_snap".into(),
                snapshot_password: None,
                snapshot_password_env: None,
                snapshot_password_file: None,
                snapshot_database: None,
            }
        );
        assert_eq!(
            d.kafka,
            Kafka {
                brokers: "127.0.0.1:9092".into(),
                metadata_refresh: Duration::from_secs(30),
                properties: BTreeMap::new(),
            }
        );
        assert_eq!(d.pause_server.url, "http://127.0.0.1:18091");
        assert_eq!(
            d.snapshots,
            Snapshots {
                refresh: Duration::from_secs(10),
                idle: Duration::from_secs(3600),
            }
        );
        assert_eq!(
            d.log,
            Log {
                level: "info".into(),
                format: LogFormat::Text,
            }
        );
        assert_eq!(d.tables, TablesConfig::default());
        assert_eq!(d.tables.expose, Expose::Listed);

        // nothing given: the defaults, valid
        Jail::expect_with(|_| {
            assert_eq!(
                Config::load(&Overrides::default()).unwrap(),
                Config::default()
            );
            Ok(())
        });

        // and what they become for the engine and the server
        let e = d.engine().unwrap();
        assert_eq!(e.clickhouse_url, "http://127.0.0.1:8123");
        assert_eq!((e.user.as_str(), e.password.as_str()), ("chlive_ro", ""));
        assert_eq!(
            (e.snapshot_user.as_str(), e.snapshot_password.as_str()),
            ("chlive_snap", "")
        );
        assert_eq!(e.kafka_brokers, "127.0.0.1:9092");
        assert!(e.kafka_properties.is_empty());
        assert_eq!(e.metadata_refresh, Duration::from_secs(30));
        assert_eq!(e.pause_server_url, "http://127.0.0.1:18091");
        assert_eq!(
            e.snapshots,
            SnapshotConfig {
                refresh: Duration::from_secs(10),
                idle: Duration::from_secs(3600),
                sweep: true,
                database: None,
            }
        );
        assert_eq!(e.tables, TablesConfig::default());
        assert_eq!(
            d.server_settings(),
            ServerSettings {
                cache: d.server_settings().cache,
                ..Default::default()
            }
        );
        // the server's default store; the library's is none (embedders choose)
        assert_eq!(
            d.server_settings().cache,
            crate::CacheSettings {
                dir: Some("./chlive-cache".into()),
                ..Default::default()
            }
        );
    }

    /// The example file lists every key with its default.
    #[test]
    fn the_example_file_is_the_defaults() {
        let text = include_str!("../chlive.example.toml");
        // uncommented, it must still be the defaults
        let uncommented: String = text
            .lines()
            .map(|l| {
                l.strip_prefix("# ")
                    .filter(|l| l.contains(" = ") || l.starts_with('['))
                    .unwrap_or(l)
            })
            .collect::<Vec<_>>()
            .join("\n");
        Jail::expect_with(|jail| {
            assert_eq!(load(jail, text).unwrap(), Config::default());
            assert_eq!(load(jail, &uncommented).unwrap(), Config::default());
            Ok(())
        });
    }

    /// Every key, set to something else, ends up exactly there.
    #[test]
    fn every_key_from_the_file() {
        Jail::expect_with(|jail| {
            jail.set_env("CH_PASS", "from-env");
            jail.create_file("snap.secret", "from-file\n")?;
            let c = load(
                jail,
                r#"
                [server]
                listen = "127.0.0.1:9999"
                path = "/ws"
                allowed_origins = ["https://a.example", "https://b.example"]
                max_connections = 7
                max_subscriptions_per_connection = 3
                progress_interval = "250ms"
                now_coalescing = "0s"
                complete_wait = "5s"
                max_insert_rows = 10
                max_inserts_in_flight = 2

                [cache]
                keep_alive = "5s"
                keep_warm = "1h"
                max_warm = 7
                log_length = 50
                dir = "/var/cache/chlive"
                disk_budget = "2GiB"
                max_age = "6h"
                max_catch_up = 5000
                checkpoint_interval = "10s"
                log_bytes = 1048576

                [clickhouse]
                url = "http://ch:8123"
                user = "reader"
                password_env = "CH_PASS"
                snapshot_user = "snapper"
                snapshot_password_file = "snap.secret"
                snapshot_database = "snaps"

                [kafka]
                brokers = "k1:9092,k2:9092"
                metadata_refresh = "5s"
                [kafka.properties]
                "security.protocol" = "SASL_SSL"
                "sasl.mechanism" = "PLAIN"

                [pause_server]
                url = "http://pause:1"

                [snapshots]
                refresh = "2m"
                idle = "10m"

                [log]
                level = "warn,chlive_server=debug"
                format = "json"

                [tables]
                expose = "all"
                database = "analytics"
                topic = "prod.{table}"
                hidden_columns = ["_*", "tmp_*"]

                [tables.events]
                [tables.orders]
                clickhouse = "shop.orders_v2"
                topic = "orders-topic"
                hidden_columns = ["internal"]
                [tables.orders.columns]
                customer = { clickhouse = "customer_id", json = "customerId" }
                placed = { clickhouse = "_offset", kafka = "offset" }
                [tables.orders.insert]
                user = "customer"
                key = "customer"
                "#,
            )
            .unwrap();
            assert_eq!(
                c.server,
                Server {
                    listen: "127.0.0.1:9999".into(),
                    path: "/ws".into(),
                    allowed_origins: vec!["https://a.example".into(), "https://b.example".into()],
                    max_connections: 7,
                    max_subscriptions_per_connection: 3,
                    progress_interval: Duration::from_millis(250),
                    now_coalescing: Duration::ZERO,
                    complete_wait: Duration::from_secs(5),
                    max_insert_rows: 10,
                    max_inserts_in_flight: 2,
                }
            );
            assert_eq!(c.clickhouse.url, "http://ch:8123");
            assert_eq!(c.clickhouse.user, "reader");
            assert_eq!(c.clickhouse.snapshot_user, "snapper");
            assert_eq!(c.clickhouse.snapshot_database.as_deref(), Some("snaps"));
            assert_eq!(c.kafka.brokers, "k1:9092,k2:9092");
            assert_eq!(c.kafka.metadata_refresh, Duration::from_secs(5));
            assert_eq!(c.kafka.properties["security.protocol"], "SASL_SSL");
            assert_eq!(c.kafka.properties["sasl.mechanism"], "PLAIN");
            assert_eq!(c.pause_server.url, "http://pause:1");
            assert_eq!(c.snapshots.refresh, Duration::from_secs(120));
            assert_eq!(c.snapshots.idle, Duration::from_secs(600));
            assert_eq!(c.log.level, "warn,chlive_server=debug");
            assert_eq!(c.log.format, LogFormat::Json);
            assert_eq!(c.tables.expose, Expose::All);
            assert_eq!(c.tables.database, "analytics");
            assert_eq!(c.tables.topic, "prod.{table}");
            assert_eq!(c.tables.hidden_columns, ["_*", "tmp_*"]);
            assert_eq!(c.tables.tables["events"], TableConfig::default());
            assert_eq!(
                c.tables.tables["orders"],
                TableConfig {
                    clickhouse: Some("shop.orders_v2".into()),
                    topic: Some("orders-topic".into()),
                    hidden_columns: vec!["internal".into()],
                    columns: [
                        (
                            "customer".to_string(),
                            ColumnConfig {
                                clickhouse: Some("customer_id".into()),
                                json: Some("customerId".into()),
                                kafka: None,
                            }
                        ),
                        (
                            "placed".to_string(),
                            ColumnConfig {
                                clickhouse: Some("_offset".into()),
                                json: None,
                                kafka: Some(KafkaField::Offset),
                            }
                        )
                    ]
                    .into(),
                    security: None,
                    insert: Some(InsertConfig {
                        user: Some("customer".into()),
                        key: Some("customer".into()),
                        ..Default::default()
                    }),
                }
            );

            // through to the engine and the server
            let e = c.engine().unwrap();
            assert_eq!(e.password, "from-env");
            assert_eq!(e.snapshot_password, "from-file", "the newline trimmed");
            assert_eq!(e.snapshots.database.as_deref(), Some("snaps"));
            assert_eq!(e.snapshots.refresh, Duration::from_secs(120));
            assert_eq!(e.kafka_properties.len(), 2);
            assert_eq!(e.metadata_refresh, Duration::from_secs(5));
            assert_eq!(e.tables, c.tables);
            assert_eq!(
                c.cache,
                Cache {
                    dir: "/var/cache/chlive".into(),
                    keep_alive: Duration::from_secs(5),
                    keep_warm: Duration::from_secs(3600),
                    max_warm: 7,
                    log_length: 50,
                    disk_budget: 2 << 30,
                    max_age: Duration::from_secs(6 * 3600),
                    max_catch_up: 5000,
                    checkpoint_interval: Duration::from_secs(10),
                    log_bytes: 1 << 20,
                }
            );
            assert_eq!(
                c.server_settings().cache,
                crate::CacheSettings {
                    dir: Some("/var/cache/chlive".into()),
                    keep_alive: Duration::from_secs(5),
                    keep_warm: Duration::from_secs(3600),
                    max_warm: 7,
                    log_length: 50,
                    disk_budget: 2 << 30,
                    max_age: Duration::from_secs(6 * 3600),
                    max_catch_up: 5000,
                    checkpoint_interval: Duration::from_secs(10),
                    log_bytes: 1 << 20,
                }
            );
            let s = c.server_settings();
            assert_eq!(s.path, "/ws");
            assert_eq!(s.allowed_origins.len(), 2);
            assert_eq!(
                (s.max_connections, s.max_subscriptions_per_connection),
                (7, 3)
            );
            assert_eq!(s.progress_interval, Duration::from_millis(250));
            assert_eq!(s.complete_wait, Duration::from_secs(5));
            assert_eq!((s.max_insert_rows, s.max_inserts_in_flight), (10, 2));

            // a literal password
            let c = load(
                jail,
                "[clickhouse]\npassword = \"p\"\nsnapshot_password = \"s\"",
            )
            .unwrap();
            let e = c.engine().unwrap();
            assert_eq!(
                (e.password.as_str(), e.snapshot_password.as_str()),
                ("p", "s")
            );
            Ok(())
        });
    }

    #[test]
    fn env_overrides_the_file_and_flags_override_env() {
        Jail::expect_with(|jail| {
            jail.create_file("chlive.toml", "[kafka]\nbrokers = \"file:9092\"\n[server]\nmax_connections = 5\n[tables.events]\n")?;
            jail.set_env("CHLIVE_KAFKA__BROKERS", "env:9092");
            jail.set_env("CHLIVE_SERVER__MAX_CONNECTIONS", "6");
            jail.set_env("CHLIVE_TABLES__EVENTS__TOPIC", "env.events");
            jail.set_env("CHLIVE_CACHE__KEEP_WARM", "0s");
            jail.set_env("CHLIVE_SERVER__ALLOWED_ORIGINS", "[\"https://x\"]");
            // not config: the clients', the tests', the old flags'
            jail.set_env("CHLIVE_URL", "ws://x");
            jail.set_env("CHLIVE_E2E", "1");
            jail.set_env("CHLIVE_CLICKHOUSE_URL", "http://ignored");
            jail.set_env("CHLIVE_CONFIG", "ignored.toml");
            let mut o = Overrides {
                file: Some("chlive.toml".into()),
                ..Default::default()
            };
            let c = Config::load(&o).unwrap();
            assert_eq!(c.kafka.brokers, "env:9092");
            assert_eq!(c.server.max_connections, 6);
            assert_eq!(
                c.tables.tables["events"].topic.as_deref(),
                Some("env.events")
            );
            assert_eq!(c.server.allowed_origins, ["https://x"]);
            assert_eq!(c.cache.keep_warm, Duration::ZERO, "warm off by env");
            assert_eq!(c.clickhouse.url, "http://127.0.0.1:8123");

            o.set("kafka.brokers=flag:9092").unwrap();
            o.set("server.max_connections=7").unwrap();
            o.set("server.allowed_origins=[\"https://a\", \"https://b\"]")
                .unwrap();
            o.set("log.level=debug").unwrap();
            let c = Config::load(&o).unwrap();
            assert_eq!(c.kafka.brokers, "flag:9092", "a string");
            assert_eq!(c.server.max_connections, 7, "a number");
            assert_eq!(
                c.server.allowed_origins,
                ["https://a", "https://b"],
                "a list"
            );
            assert_eq!(c.log.level, "debug");
            assert!(o.set("no-equals").is_err());
            Ok(())
        });
    }

    #[test]
    fn sizes() {
        for (t, n) in [
            ("10GiB", 10u64 << 30),
            ("16MiB", 16 << 20),
            ("512 KiB", 512 << 10),
            ("1.5GiB", 3 << 29),
            ("100", 100),
            ("100B", 100),
            ("2GB", 2_000_000_000),
            ("3kb", 3000),
        ] {
            assert_eq!(size::parse(t), Ok(n), "{t}");
        }
        assert!(size::parse("lots").is_err());
        assert!(size::parse("5 parsecs").is_err());
        assert_eq!(size::format(10 << 30), "10GiB");
        assert_eq!(size::format(16 << 20), "16MiB");
        assert_eq!(size::format(1000), "1000B");
    }

    #[test]
    fn errors_name_the_key() {
        Jail::expect_with(|jail| {
            // the native address is gone: everything is HTTP
            let e = err(jail, "[clickhouse]\nnative = \"ch:9000\"");
            assert!(e.contains("native"), "{e}");
            let e = err(jail, "[server]\nlisten_on = \"x\"");
            assert!(e.contains("listen_on"), "{e}");
            let e = err(jail, "[nope]\nx = 1");
            assert!(e.contains("nope"), "{e}");
            let e = err(jail, "[snapshots]\nrefresh = \"soon\"");
            assert!(e.contains("refresh"), "{e}");
            let e = err(jail, "[snapshots]\nrefresh = \"0s\"");
            assert!(e.contains("snapshots.refresh: must be more than 0"), "{e}");
            let e = err(jail, "[log]\nformat = \"xml\"");
            assert!(e.contains("format"), "{e}");
            let e = err(jail, "[log]\nlevel = \"chlive=nope=x\"");
            assert!(e.contains("log.level"), "{e}");
            let e = err(jail, "[tables]\nexpose = \"some\"");
            assert!(e.contains("expose"), "{e}");
            let e = err(jail, "[tables.t]\nclickhouse = \"a.b.c\"");
            assert!(e.contains("tables.t: clickhouse"), "{e}");
            let e = err(jail, "[clickhouse]\npassword = \"a\"\npassword_env = \"B\"");
            assert!(e.contains("set only one of clickhouse.password"), "{e}");
            let e = err(
                jail,
                "[clickhouse]\nsnapshot_password_env = \"CHLIVE_TEST_UNSET\"",
            );
            assert!(e.contains("CHLIVE_TEST_UNSET is not set"), "{e}");
            let e = err(jail, "[clickhouse]\npassword_file = \"missing.secret\"");
            assert!(e.contains("password_file"), "{e}");
            let e = err(jail, "[clickhouse]\nsnapshot_database = \"\"");
            assert!(e.contains("snapshot_database"), "{e}");
            let e = err(jail, "[server]\npath = \"live\"");
            assert!(e.contains("server.path"), "{e}");
            let e = err(jail, "[server]\nmax_connections = 0");
            assert!(e.contains("max_connections"), "{e}");
            let e = err(jail, "[server]\nprogress_interval = \"0s\"");
            assert!(e.contains("progress_interval"), "{e}");
            let e = err(jail, "[cache]\nlog_length = 0");
            assert!(e.contains("cache.log_length"), "{e}");
            let e = err(jail, "[cache]\nkeep_warm = \"later\"");
            assert!(e.contains("keep_warm"), "{e}");
            let e = err(jail, "[cache]\ndisk_budget = \"lots\"");
            assert!(e.contains("disk_budget"), "{e}");
            let e = err(jail, "[cache]\nlog_bytes = \"5 parsecs\"");
            assert!(e.contains("log_bytes"), "{e}");
            let e = err(jail, "[cache]\ncheckpoint_interval = \"0s\"");
            assert!(e.contains("checkpoint_interval"), "{e}");
            let e = err(jail, "[cache]\nmax_shared_rows = 5");
            assert!(e.contains("max_shared_rows"), "removed: {e}");
            // "" turns the store off
            let c = load(jail, "[cache]\ndir = \"\"").unwrap();
            assert_eq!(c.server_settings().cache.dir, None);
            let e = err(jail, "[cache]\nwarm = true");
            assert!(e.contains("warm"), "{e}");
            // zero keep-alive and warm are allowed: dropped as soon as nobody's subscribed
            assert!(load(
                jail,
                "[cache]\nkeep_alive = \"0s\"\nkeep_warm = \"0s\"\nmax_warm = 0"
            )
            .is_ok());
            let e = err(jail, "[kafka]\nmetadata_refresh = \"0s\"");
            assert!(e.contains("metadata_refresh"), "{e}");
            // a missing file is an error, not the defaults
            let e = Config::load(&Overrides {
                file: Some("absent.toml".into()),
                ..Default::default()
            })
            .unwrap_err();
            assert!(format!("{e:#}").contains("absent.toml"), "{e:#}");
            Ok(())
        });
    }

    #[test]
    fn show_hides_passwords_and_names_sources() {
        Jail::expect_with(|jail| {
            jail.create_file(
                "chlive.toml",
                "[clickhouse]\npassword = \"hunter2\"\n[tables.events]\ntopic = \"e\"\n",
            )?;
            jail.set_env("CHLIVE_KAFKA__BROKERS", "env:9092");
            let mut o = Overrides {
                file: Some("chlive.toml".into()),
                ..Default::default()
            };
            o.set("server.listen=127.0.0.1:1").unwrap();
            let out = show(&o).unwrap();
            assert!(!out.contains("hunter2"), "{out}");
            let line = |key: &str| {
                out.lines()
                    .find(|l| l.starts_with(&format!("{key} =")))
                    .unwrap_or_else(|| panic!("{key} not in\n{out}"))
                    .to_string()
            };
            assert!(
                line("password").contains("\"<redacted>\"") && line("password").contains("# file"),
                "{out}"
            );
            assert!(
                line("brokers").ends_with("# env CHLIVE_KAFKA__BROKERS"),
                "{out}"
            );
            assert!(line("listen").ends_with("# flag"), "{out}");
            assert!(line("url").ends_with("# default"), "{out}");
            assert!(out.contains("[tables.events]"), "{out}");
            assert!(line("topic").contains("# "), "{out}");
            // what `show` prints loads back to the same config (bar the hidden password)
            let back: Config =
                toml::from_str(&out.replace("\"<redacted>\"", "\"hunter2\"")).unwrap();
            assert_eq!(back, Config::load(&o).unwrap());
            Ok(())
        });
    }
}
