//! Row-level security: which rows of each table a connection's user may see.
//!
//! A table is public, or secured by a permission provider: a source of permissions (Postgres
//! over CDC, …) that, for each of its grants and each subject, keeps the values of the table's
//! column the subject may see, and tells when they change ([`PermissionProvider::watch`]). The
//! server ANDs them into every reference to the table ([`chlive_engine::RowFilter`]); when they
//! change, the subscription moves to the plan with the new filter and gets a new snapshot.
//!
//! Without a `[security]` section, every table is open, to anyone.

pub mod grants;
pub mod index;
pub mod pgoutput;
pub mod postgres;

use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::sync::{Arc, Mutex};

use anyhow::{bail, Context, Result};
use chlive_engine::rules::TablesConfig;
use chlive_engine::RowFilter;
use serde::{Deserialize, Serialize};
use tokio::sync::watch;

use self::grants::Grant;

/// The values a subject may see; none until the provider has loaded its permissions.
pub type Granted = Option<Arc<Vec<String>>>;

/// A source of permissions. Each grant (by index, as given when it was made) tells, per
/// subject, the values of a table's column the subject may see.
pub trait PermissionProvider: Send + Sync {
    /// What `subject` is granted by `grant`, now and as it changes.
    fn watch(&self, grant: usize, subject: &str) -> watch::Receiver<Granted>;
}

/// The `[security]` section.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct SecurityConfig {
    /// For a table with no `security` of its own: `allow` (anyone reads it whole) or `deny`.
    /// Without this section, every table is allowed.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default: Option<Default>,
    /// The permission providers, by name.
    pub providers: BTreeMap<String, ProviderConfig>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Default {
    Allow,
    Deny,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "lowercase", deny_unknown_fields)]
pub enum ProviderConfig {
    /// Permissions in Postgres tables, followed by logical replication.
    Postgres(postgres::PostgresConfig),
}

/// How a table is read.
enum Rule {
    /// Whole, by anyone.
    Public,
    /// Not at all.
    Denied,
    /// The rows whose `column` is one of what the subject is granted by the provider's grant.
    Granted {
        provider: Arc<dyn PermissionProvider>,
        grant: usize,
        column: String,
    },
}

/// What a connection may read of a table.
pub enum Access {
    All,
    /// The rows whose ClickHouse `column` is one of the granted values, as they change.
    Rows {
        column: String,
        granted: watch::Receiver<Granted>,
    },
}

impl Access {
    /// The row filter now, once the provider has loaded.
    pub async fn filter(&mut self) -> RowFilter {
        match self {
            Access::All => RowFilter::All,
            Access::Rows { column, granted } => {
                let values = match granted.wait_for(Option::is_some).await {
                    Ok(v) => v.clone().unwrap_or_default(),
                    // the provider is gone: nothing
                    Err(_) => return RowFilter::None,
                };
                RowFilter::In {
                    column: column.clone(),
                    values: values.as_ref().clone(),
                }
            }
        }
    }

    /// The filter as it is, not waiting (`None` while loading).
    pub fn current(&self) -> Option<RowFilter> {
        match self {
            Access::All => Some(RowFilter::All),
            Access::Rows { column, granted } => {
                let values = granted.borrow().clone()?;
                Some(RowFilter::In {
                    column: column.clone(),
                    values: values.as_ref().clone(),
                })
            }
        }
    }
}

/// Every table's rule, and the providers behind them.
pub struct Security {
    /// For tables without a rule; `None`: no security at all.
    default: Option<Default>,
    tables: BTreeMap<String, Rule>,
}

impl fmt::Debug for Security {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Security")
            .field("default", &self.default)
            .field("tables", &self.tables.keys().collect::<Vec<_>>())
            .finish()
    }
}

/// Open ones are alike; secured ones only themselves (their providers are running).
impl PartialEq for Security {
    fn eq(&self, other: &Self) -> bool {
        (self.is_open() && other.is_open()) || std::ptr::eq(self, other)
    }
}

impl Security {
    /// Every table open, to anyone.
    pub fn open() -> Self {
        Self {
            default: None,
            tables: BTreeMap::new(),
        }
    }

    /// Whether any table is secured (connections then say who they are).
    pub fn is_open(&self) -> bool {
        self.default.is_none()
    }

    /// Check the config without starting anything: each table's rule, its provider, its grants.
    pub fn check(security: &SecurityConfig, tables: &TablesConfig) -> Result<()> {
        plan(security, tables).map(|_| ())
    }

    /// Start the providers (in the background) and serve their tables. Must run in a Tokio
    /// runtime.
    pub fn start(security: &SecurityConfig, tables: &TablesConfig) -> Result<Self> {
        let (rules, grants) = plan(security, tables)?;
        let mut providers: HashMap<String, Arc<dyn PermissionProvider>> = HashMap::new();
        for (name, grants) in grants {
            let provider: Arc<dyn PermissionProvider> = match &security.providers[&name] {
                ProviderConfig::Postgres(c) => postgres::PostgresProvider::start(&name, c, grants)?,
            };
            providers.insert(name, provider);
        }
        let tables = rules
            .into_iter()
            .map(|(table, rule)| {
                let rule = match rule {
                    Planned::Public => Rule::Public,
                    Planned::Denied => Rule::Denied,
                    Planned::Granted {
                        provider,
                        grant,
                        column,
                    } => Rule::Granted {
                        provider: providers[&provider].clone(),
                        grant,
                        column,
                    },
                };
                (table, rule)
            })
            .collect();
        Ok(Self {
            default: Some(security.default.unwrap_or(Default::Allow)),
            tables,
        })
    }

    /// Tables served by `provider`'s grants, by index (tests, other providers).
    pub fn with_provider(
        default: Default,
        public: &[&str],
        provider: Arc<dyn PermissionProvider>,
        granted: &[(&str, &str)],
    ) -> Self {
        let mut tables: BTreeMap<String, Rule> = public
            .iter()
            .map(|t| (t.to_string(), Rule::Public))
            .collect();
        for (grant, (table, column)) in granted.iter().enumerate() {
            tables.insert(
                table.to_string(),
                Rule::Granted {
                    provider: provider.clone(),
                    grant,
                    column: column.to_string(),
                },
            );
        }
        Self {
            default: Some(default),
            tables,
        }
    }

    /// What `user` (none: anonymous) may read of `table`; an error says why not at all.
    pub fn access(&self, user: Option<&str>, table: &str) -> Result<Access, String> {
        let Some(default) = self.default else {
            return Ok(Access::All);
        };
        match self.tables.get(table) {
            None if default == Default::Allow => Ok(Access::All),
            None | Some(Rule::Denied) => Err(format!("table {table} isn't readable here")),
            Some(Rule::Public) => Ok(Access::All),
            Some(Rule::Granted {
                provider,
                grant,
                column,
            }) => match user {
                None => Err(format!(
                    "table {table} needs a user (send `hello` before subscribing)"
                )),
                Some(user) => Ok(Access::Rows {
                    column: column.clone(),
                    granted: provider.watch(*grant, user),
                }),
            },
        }
    }
}

/// A table's rule, before its provider is started.
enum Planned {
    Public,
    Denied,
    Granted {
        provider: String,
        grant: usize,
        column: String,
    },
}

/// Each secured table's rule, and per provider its grants (in the order of their indexes).
#[allow(clippy::type_complexity)]
fn plan(
    security: &SecurityConfig,
    tables: &TablesConfig,
) -> Result<(BTreeMap<String, Planned>, BTreeMap<String, Vec<Grant>>)> {
    let mut rules = BTreeMap::new();
    let mut grants: BTreeMap<String, Vec<Grant>> = BTreeMap::new();
    for (table, config) in &tables.tables {
        let Some(s) = &config.security else {
            continue;
        };
        let at = format!("tables.{table}.security");
        if security.default.is_none() {
            bail!("{at}: secured tables need a [security] section (with `default`)");
        }
        let rule = match (s.public, &s.provider) {
            (true, Some(_)) => bail!("{at}: `public` or a `provider`, not both"),
            (true, None) => {
                if s.column.is_some() || s.grants.is_some() {
                    bail!("{at}: a public table has no `column` or `grants`");
                }
                Planned::Public
            }
            (false, None) => {
                if s.column.is_some() || s.grants.is_some() {
                    bail!("{at}: `column` and `grants` need a `provider`");
                }
                Planned::Denied
            }
            (false, Some(provider)) => {
                let kind = security.providers.get(provider).with_context(|| {
                    format!("{at}: no provider {provider} in [security.providers]")
                })?;
                let column = s
                    .column
                    .clone()
                    .with_context(|| format!("{at}: `column` (the ClickHouse column to filter)"))?;
                match kind {
                    ProviderConfig::Postgres(_) => {
                        let sql = s.grants.as_deref().with_context(|| {
                            format!("{at}: `grants` (which values of {column} a subject may see)")
                        })?;
                        let grant =
                            Grant::parse(sql).map_err(|e| anyhow::anyhow!("{at}.grants: {e}"))?;
                        let list = grants.entry(provider.clone()).or_default();
                        list.push(grant);
                        Planned::Granted {
                            provider: provider.clone(),
                            grant: list.len() - 1,
                            column,
                        }
                    }
                }
            }
        };
        rules.insert(table.clone(), rule);
    }
    for (name, provider) in &security.providers {
        match provider {
            ProviderConfig::Postgres(c) => c
                .check()
                .with_context(|| format!("security.providers.{name}"))?,
        }
    }
    Ok((rules, grants))
}

/// The watched (grant, subject) pairs of a provider, each with what it's been told.
#[derive(Default)]
pub struct Watchers {
    senders: HashMap<(usize, String), watch::Sender<Granted>>,
}

impl Watchers {
    /// A receiver of what `subject` is granted by `grant`; `current` is what it is now, for a
    /// pair nobody watched yet.
    pub fn watch(
        &mut self,
        grant: usize,
        subject: &str,
        current: impl FnOnce() -> Granted,
    ) -> watch::Receiver<Granted> {
        self.senders
            .entry((grant, subject.to_string()))
            .or_insert_with(|| watch::channel(current()).0)
            .subscribe()
    }

    /// Tell the pair's watchers, if it has any and it changed; pairs nobody watches anymore go.
    pub fn publish(&mut self, grant: usize, subject: &str, granted: Granted) {
        let key = (grant, subject.to_string());
        let Some(tx) = self.senders.get(&key) else {
            return;
        };
        if tx.receiver_count() == 0 {
            self.senders.remove(&key);
            return;
        }
        tx.send_if_modified(|now| {
            let changed = *now != granted;
            *now = granted;
            changed
        });
    }

    /// The watched pairs.
    pub fn watched(&self) -> Vec<(usize, String)> {
        self.senders.keys().cloned().collect()
    }
}

/// A provider whose permissions are set by hand (tests, embedding).
#[derive(Default)]
pub struct MemoryProvider {
    state: Mutex<MemoryState>,
}

#[derive(Default)]
struct MemoryState {
    loaded: bool,
    sets: HashMap<(usize, String), Vec<String>>,
    watchers: Watchers,
}

impl MemoryState {
    fn granted(&self, grant: usize, subject: &str) -> Granted {
        self.loaded.then(|| {
            Arc::new(
                self.sets
                    .get(&(grant, subject.to_string()))
                    .cloned()
                    .unwrap_or_default(),
            )
        })
    }
}

impl MemoryProvider {
    /// Loaded, with nothing granted.
    pub fn loaded() -> Arc<Self> {
        let p = Self::default();
        p.state.lock().unwrap().loaded = true;
        Arc::new(p)
    }

    /// What `subject` is granted by `grant` from now on.
    pub fn set(&self, grant: usize, subject: &str, values: &[&str]) {
        let mut s = self.state.lock().unwrap();
        let mut values: Vec<String> = values.iter().map(|v| v.to_string()).collect();
        values.sort();
        s.sets.insert((grant, subject.to_string()), values);
        let granted = s.granted(grant, subject);
        s.watchers.publish(grant, subject, granted);
    }
}

impl PermissionProvider for MemoryProvider {
    fn watch(&self, grant: usize, subject: &str) -> watch::Receiver<Granted> {
        let mut s = self.state.lock().unwrap();
        let current = s.granted(grant, subject);
        s.watchers.watch(grant, subject, || current)
    }
}
