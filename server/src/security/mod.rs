//! Security: which tables, rows and columns a connection's user may read.
//!
//! A permission provider (Postgres over CDC, …) answers, for each of its grants and each
//! subject, a set of strings, and tells when it changes ([`PermissionProvider::watch`]). What
//! the strings mean is the configuration's: the tables a user may read (`[security.tables]`),
//! the values of a table's column whose rows they may read (`[tables.<t>.security]`), or the
//! columns of a table they may read (`[tables.<t>.security.columns]`). The server turns them
//! into each table's [`TableFilter`]; when they change, the subscription moves to the plan
//! with the new filter (a new snapshot), or ends with `PERMISSION_DENIED` if it lost a table or
//! a column it names.
//!
//! Without a `[security]` section, everything is open, to anyone.

pub mod grants;
pub mod index;
pub mod pgoutput;
pub mod postgres;

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt;
use std::sync::{Arc, Mutex};

use anyhow::{bail, Context, Result};
use chlive_engine::rules::TablesConfig;
use chlive_engine::security::GrantSource;
use chlive_engine::{RowFilter, TableFilter};
use serde::{Deserialize, Serialize};
use tokio::sync::watch;

use self::grants::Grant;

/// The values a subject is granted; none until the provider has loaded its permissions.
pub type Granted = Option<Arc<Vec<String>>>;

/// A source of permissions. Each grant (by index, as given when it was made) tells, per
/// subject, a set of strings: what they mean is up to whoever asks.
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
    /// Which tables (dialect names) a subject may read; with it, every table needs a grant.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub tables: Option<GrantSource>,
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

/// One grant of a running provider.
#[derive(Clone)]
pub struct Source {
    pub provider: Arc<dyn PermissionProvider>,
    pub grant: usize,
}

/// Which rows of a table are read.
enum Rows {
    /// All of them.
    All,
    /// None: the table isn't readable.
    Denied,
    /// The rows whose `column` is one of what the subject is granted.
    Granted { source: Source, column: String },
}

/// A table's rule: its rows, and its columns (none: every one).
struct Rule {
    rows: Rows,
    columns: Option<Source>,
}

/// What a connection may read of a table, as it changes.
#[derive(Debug)]
pub struct TableAccess {
    table: String,
    /// Its name must be among these (`[security.tables]`).
    tables: Option<watch::Receiver<Granted>>,
    /// Its rows: the ClickHouse column, and the values (none: all rows).
    rows: Option<(String, watch::Receiver<Granted>)>,
    /// Its columns (none: every one).
    columns: Option<watch::Receiver<Granted>>,
}

/// What a [`TableAccess`] allows now.
#[derive(Debug, Clone, PartialEq)]
pub enum Decision {
    /// A provider is still loading.
    Loading,
    /// The table isn't readable (anymore).
    Denied(String),
    Filter(TableFilter),
}

impl TableAccess {
    /// Everything.
    fn open(table: &str) -> Self {
        Self {
            table: table.to_string(),
            tables: None,
            rows: None,
            columns: None,
        }
    }

    fn receivers(&self) -> impl Iterator<Item = &watch::Receiver<Granted>> {
        self.tables
            .iter()
            .chain(self.rows.iter().map(|(_, r)| r))
            .chain(self.columns.iter())
    }

    /// Receivers to be told by when anything here changes.
    pub fn watches(&self) -> Vec<watch::Receiver<Granted>> {
        self.receivers().cloned().collect()
    }

    /// What it allows now.
    pub fn current(&self) -> Decision {
        let get = |r: &watch::Receiver<Granted>| r.borrow().clone();
        let mut filter = TableFilter::default();
        if let Some(r) = &self.tables {
            let Some(tables) = get(r) else {
                return Decision::Loading;
            };
            if !tables.contains(&self.table) {
                return Decision::Denied(format!("table {} isn't readable here", self.table));
            }
        }
        if let Some((column, r)) = &self.rows {
            let Some(values) = get(r) else {
                return Decision::Loading;
            };
            filter.rows = RowFilter::In {
                column: column.clone(),
                values: values.as_ref().clone(),
            };
        }
        if let Some(r) = &self.columns {
            let Some(columns) = get(r) else {
                return Decision::Loading;
            };
            filter.columns = Some(columns.iter().cloned().collect::<BTreeSet<_>>());
        }
        Decision::Filter(filter)
    }

    /// What it allows, once its providers have loaded.
    pub async fn decide(&mut self) -> Decision {
        let mut receivers: Vec<_> = self.watches();
        for r in &mut receivers {
            if r.wait_for(Option::is_some).await.is_err() {
                return Decision::Denied(format!(
                    "the permissions for {} are unavailable",
                    self.table
                ));
            }
        }
        self.current()
    }
}

/// Every table's rule, and the providers behind them.
pub struct Security {
    /// For tables without a rule; `None`: no security at all.
    default: Option<Default>,
    /// Which tables a subject may read (`[security.tables]`).
    tables_grant: Option<Source>,
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
            tables_grant: None,
            tables: BTreeMap::new(),
        }
    }

    /// Whether nothing is secured.
    pub fn is_open(&self) -> bool {
        self.default.is_none()
    }

    /// Secured, with no rules yet: tables without one are read whole (`Allow`) or not at all.
    pub fn secured(default: Default) -> Self {
        Self {
            default: Some(default),
            ..Self::open()
        }
    }

    /// `table`'s rows, all of them.
    pub fn public(mut self, table: &str) -> Self {
        self.rule(table).rows = Rows::All;
        self
    }

    /// `table`'s rows whose `column` is one of what `source` grants the subject.
    pub fn rows(mut self, table: &str, column: &str, source: Source) -> Self {
        self.rule(table).rows = Rows::Granted {
            source,
            column: column.to_string(),
        };
        self
    }

    /// `table`'s columns: those `source` grants the subject.
    pub fn columns(mut self, table: &str, source: Source) -> Self {
        self.rule(table).columns = Some(source);
        self
    }

    /// The tables a subject may read: those `source` grants them.
    pub fn tables(mut self, source: Source) -> Self {
        self.tables_grant = Some(source);
        self
    }

    fn rule(&mut self, table: &str) -> &mut Rule {
        self.tables.entry(table.to_string()).or_insert(Rule {
            rows: Rows::All,
            columns: None,
        })
    }

    /// Check the config without starting anything: each table's rule, its provider, its grants.
    pub fn check(security: &SecurityConfig, tables: &TablesConfig) -> Result<()> {
        plan(security, tables).map(|_| ())
    }

    /// Start the providers (in the background) and serve their tables. Must run in a Tokio
    /// runtime.
    pub fn start(security: &SecurityConfig, tables: &TablesConfig) -> Result<Self> {
        let planned = plan(security, tables)?;
        let mut providers: HashMap<String, Arc<dyn PermissionProvider>> = HashMap::new();
        for (name, grants) in planned.grants {
            let provider: Arc<dyn PermissionProvider> = match &security.providers[&name] {
                ProviderConfig::Postgres(c) => postgres::PostgresProvider::start(&name, c, grants)?,
            };
            providers.insert(name, provider);
        }
        let source = |(provider, grant): (String, usize)| Source {
            provider: providers[&provider].clone(),
            grant,
        };
        let mut out = Self::secured(security.default.unwrap_or(Default::Allow));
        out.tables_grant = planned.tables.map(source);
        for (table, rule) in planned.rules {
            let rows = match rule.rows {
                PlannedRows::All => Rows::All,
                PlannedRows::Denied => Rows::Denied,
                PlannedRows::Granted { source: s, column } => Rows::Granted {
                    source: source(s),
                    column,
                },
            };
            out.tables.insert(
                table,
                Rule {
                    rows,
                    columns: rule.columns.map(source),
                },
            );
        }
        Ok(out)
    }

    /// What `user` (none: anonymous) may read of `table`; an error says why not at all.
    pub fn access(&self, user: Option<&str>, table: &str) -> Result<TableAccess, String> {
        let Some(default) = self.default else {
            return Ok(TableAccess::open(table));
        };
        let needs_user = || format!("table {table} needs a user (send `hello` before subscribing)");
        let watch = |s: &Source| -> Result<watch::Receiver<Granted>, String> {
            let user = user.ok_or_else(needs_user)?;
            Ok(s.provider.watch(s.grant, user))
        };
        let mut access = TableAccess::open(table);
        if let Some(s) = &self.tables_grant {
            access.tables = Some(watch(s)?);
        }
        match self.tables.get(table) {
            None if default == Default::Allow => {}
            None => return Err(format!("table {table} isn't readable here")),
            Some(rule) => {
                match &rule.rows {
                    Rows::All => {}
                    Rows::Denied => return Err(format!("table {table} isn't readable here")),
                    Rows::Granted { source, column } => {
                        access.rows = Some((column.clone(), watch(source)?));
                    }
                }
                if let Some(s) = &rule.columns {
                    access.columns = Some(watch(s)?);
                }
            }
        }
        Ok(access)
    }
}

/// The rules, before the providers are started: grants as (provider, index).
struct Planned {
    tables: Option<(String, usize)>,
    rules: BTreeMap<String, PlannedRule>,
    /// Per provider, its grants (in the order of their indexes).
    grants: BTreeMap<String, Vec<Grant>>,
}

struct PlannedRule {
    rows: PlannedRows,
    columns: Option<(String, usize)>,
}

enum PlannedRows {
    All,
    Denied,
    Granted {
        source: (String, usize),
        column: String,
    },
}

/// Every rule checked, and every grant given its provider and index.
fn plan(security: &SecurityConfig, tables: &TablesConfig) -> Result<Planned> {
    let mut grants: BTreeMap<String, Vec<Grant>> = BTreeMap::new();
    let mut register = |at: &str, provider: &str, text: &str| -> Result<(String, usize)> {
        let kind = security
            .providers
            .get(provider)
            .with_context(|| format!("{at}: no provider {provider} in [security.providers]"))?;
        let grant = match kind {
            ProviderConfig::Postgres(_) => {
                Grant::parse(text).map_err(|e| anyhow::anyhow!("{at}.grants: {e}"))?
            }
        };
        let list = grants.entry(provider.to_string()).or_default();
        list.push(grant);
        Ok((provider.to_string(), list.len() - 1))
    };
    let tables_grant = match &security.tables {
        Some(g) => {
            if security.default.is_none() {
                bail!("security.tables: needs `default` in [security]");
            }
            Some(register("security.tables", &g.provider, &g.grants)?)
        }
        None => None,
    };
    let mut rules = BTreeMap::new();
    for (table, config) in &tables.tables {
        let Some(s) = &config.security else {
            continue;
        };
        let at = format!("tables.{table}.security");
        if security.default.is_none() {
            bail!("{at}: secured tables need a [security] section (with `default`)");
        }
        let rows = match (s.public, &s.provider) {
            (true, Some(_)) => bail!("{at}: `public` or a `provider`, not both"),
            (true, None) => {
                if s.column.is_some() || s.grants.is_some() {
                    bail!("{at}: a public table has no `column` or `grants`");
                }
                PlannedRows::All
            }
            (false, None) => {
                if s.column.is_some() || s.grants.is_some() {
                    bail!("{at}: `column` and `grants` need a `provider`");
                }
                if s.columns.is_some() {
                    bail!("{at}: with `columns`, say which rows: `public = true`, or a `provider`");
                }
                PlannedRows::Denied
            }
            (false, Some(provider)) => {
                let column = s
                    .column
                    .clone()
                    .with_context(|| format!("{at}: `column` (the ClickHouse column to filter)"))?;
                let text = s.grants.as_deref().with_context(|| {
                    format!("{at}: `grants` (which values of {column} a subject may see)")
                })?;
                PlannedRows::Granted {
                    source: register(&at, provider, text)?,
                    column,
                }
            }
        };
        let columns = match &s.columns {
            Some(g) => Some(register(&format!("{at}.columns"), &g.provider, &g.grants)?),
            None => None,
        };
        rules.insert(table.clone(), PlannedRule { rows, columns });
    }
    for (name, provider) in &security.providers {
        match provider {
            ProviderConfig::Postgres(c) => c
                .check()
                .with_context(|| format!("security.providers.{name}"))?,
        }
    }
    Ok(Planned {
        tables: tables_grant,
        rules,
        grants,
    })
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

#[cfg(test)]
mod tests {
    use super::*;

    fn config(toml: &str) -> (SecurityConfig, TablesConfig) {
        #[derive(Deserialize)]
        struct Both {
            security: SecurityConfig,
            tables: TablesConfig,
        }
        let both: Both = toml::from_str(toml).unwrap();
        (both.security, both.tables)
    }

    const PROVIDER: &str = r#"
        [security]
        default = "deny"
        tables = { provider = "pg", grants = "SELECT table_name FROM t WHERE u = {subject:String}" }
        [security.providers.pg]
        kind = "postgres"
        url = "postgres://postgres@127.0.0.1:5432/db"
        publication = "p"
        slot = "s"
    "#;

    #[test]
    fn every_grant_gets_its_provider_and_index() {
        let (security, tables) = config(&format!(
            r#"{PROVIDER}
            [tables.messages.security]
            provider = "pg"
            column = "user_id"
            grants = "SELECT a FROM r WHERE v = {{subject:String}}"
            [tables.users.security]
            public = true
            columns = {{ provider = "pg", grants = "SELECT c FROM cols WHERE u = {{subject:String}}" }}
            "#
        ));
        let p = plan(&security, &tables).unwrap();
        let pg = |i| Some(("pg".to_string(), i));
        assert_eq!(p.tables, pg(0));
        let tables: Vec<_> = p.grants["pg"].iter().map(|g| g.table.as_str()).collect();
        assert_eq!(tables, ["t", "r", "cols"]);
        assert!(
            matches!(&p.rules["messages"].rows, PlannedRows::Granted { source, .. } if Some(source.clone()) == pg(1))
        );
        assert!(matches!(p.rules["users"].rows, PlannedRows::All));
        assert_eq!(p.rules["users"].columns, pg(2));
    }

    #[test]
    fn rules_that_dont_add_up_are_errors() {
        for (table, error) in [
            ("columns = { provider = \"pg\", grants = \"SELECT c FROM x WHERE u = {subject:String}\" }", "say which rows"),
            ("public = true\ncolumns = { provider = \"nope\", grants = \"x\" }", "no provider nope"),
            ("public = true\ncolumns = { provider = \"pg\", grants = \"SELECT c FROM x\" }", "users.security.columns.grants"),
            ("public = true\nprovider = \"pg\"", "not both"),
        ] {
            let (security, tables) = config(&format!("{PROVIDER}\n[tables.users.security]\n{table}\n"));
            let e = format!("{:#}", plan(&security, &tables).err().expect(table));
            assert!(e.contains(error), "{table}: {e}");
        }
    }

    #[test]
    fn access_needs_a_user_for_any_grant() {
        let p = MemoryProvider::loaded();
        let source = |grant| Source {
            provider: p.clone(),
            grant,
        };
        let open = Security::secured(Default::Allow).public("users");
        assert!(open.access(None, "users").is_ok());
        assert!(open.access(None, "other").is_ok());
        let deny = Security::secured(Default::Deny).public("users");
        assert!(deny.access(None, "other").is_err());
        let tables = Security::secured(Default::Allow).tables(source(0));
        assert!(tables
            .access(None, "users")
            .unwrap_err()
            .contains("needs a user"));
        let columns = Security::secured(Default::Deny)
            .public("users")
            .columns("users", source(1));
        assert!(columns
            .access(None, "users")
            .unwrap_err()
            .contains("needs a user"));

        // what it decides, as the grants say
        p.set(0, "alice", &["users"]);
        p.set(1, "alice", &["name", "user_id"]);
        let both = Security::secured(Default::Deny)
            .tables(source(0))
            .public("users")
            .columns("users", source(1));
        let a = both.access(Some("alice"), "users").unwrap();
        assert_eq!(
            a.current(),
            Decision::Filter(TableFilter {
                rows: RowFilter::All,
                columns: Some(["name".to_string(), "user_id".to_string()].into()),
            })
        );
        p.set(0, "alice", &[]);
        assert!(matches!(a.current(), Decision::Denied(e) if e.contains("table users")));
    }
}
