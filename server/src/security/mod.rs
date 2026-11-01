//! Security: which tables, rows and columns a connection's user may read.
//!
//! A permission provider (Postgres over CDC, WorkOS by polling) answers, for each of its grants
//! and each subject, a set of strings, and tells when it changes ([`PermissionProvider::watch`]).
//! What the strings mean is the configuration's: the tables a user may read
//! (`[security.tables]`), whether they may read a table at all (`[tables.<t>.security.table]`:
//! anything), the values of a table's column whose rows they may read (`[tables.<t>.security]`),
//! or the columns of a table they may read (`[tables.<t>.security.columns]`). The server turns them
//! into each table's [`TableFilter`]; when they change, the subscription moves to the plan
//! with the new filter (a new snapshot), or ends with `PERMISSION_DENIED` if it lost a table or
//! a column it names.
//!
//! Without a `[security]` section, everything is open, to anyone.

pub mod grants;
pub mod index;
pub mod pgoutput;
pub mod postgres;
pub mod workos;

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::fmt;
use std::sync::{Arc, Mutex};

use anyhow::{bail, Context, Result};
use chlive_engine::rules::TablesConfig;
use chlive_engine::security::{GrantSource, InsertFilter};
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
    /// Permissions in WorkOS (FGA), polled.
    WorkOs(workos::WorkOsConfig),
}

/// What a grant is asked: a set of values (tables, rows, columns), or yes/no (a table's
/// `table`: yes is any value). A provider that can answer both alike (Postgres) ignores it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GrantKind {
    Values,
    Check,
}

/// A provider's grants, as its kind takes them (in the order of their indexes).
enum ProviderGrants {
    Postgres(Vec<Grant>),
    WorkOs(Vec<workos::Grant>),
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

/// A table's rule: whether it's readable at all (none: yes), its rows, and its columns (none:
/// every one).
struct Rule {
    table: Option<Source>,
    rows: Rows,
    columns: Option<Source>,
}

/// What a connection may read of a table, as it changes.
#[derive(Debug)]
pub struct TableAccess {
    table: String,
    /// Its name must be among these (`[security.tables]`).
    tables: Option<watch::Receiver<Granted>>,
    /// Readable while this grants anything (its `table`).
    allowed: Option<watch::Receiver<Granted>>,
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
            allowed: None,
            rows: None,
            columns: None,
        }
    }

    fn receivers(&self) -> impl Iterator<Item = &watch::Receiver<Granted>> {
        self.tables
            .iter()
            .chain(self.allowed.iter())
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
        if let Some(r) = &self.allowed {
            let Some(allowed) = get(r) else {
                return Decision::Loading;
            };
            if allowed.is_empty() {
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

/// Who may insert what into a table: while `table` grants them anything, rows whose `rows`
/// column is one of what it grants them, and the columns `columns` grants them (each: none,
/// no limit).
#[derive(Clone, Default)]
pub struct InsertRule {
    pub table: Option<Source>,
    /// The column (dialect name), and its granted values.
    pub rows: Option<(String, Source)>,
    pub columns: Option<Source>,
}

/// What a user may insert into a table, once its providers have answered.
pub struct InsertAccess {
    table: String,
    user: Option<String>,
    allowed: Option<watch::Receiver<Granted>>,
    rows: Option<(String, watch::Receiver<Granted>)>,
    columns: Option<watch::Receiver<Granted>>,
}

impl InsertAccess {
    /// What it allows, once its providers have loaded; an error says why nothing.
    pub async fn decide(&mut self) -> Result<InsertFilter, String> {
        let receivers = self
            .allowed
            .iter_mut()
            .chain(self.rows.iter_mut().map(|(_, r)| r))
            .chain(self.columns.iter_mut());
        let mut values = Vec::new();
        for r in receivers {
            match r.wait_for(Option::is_some).await {
                Ok(v) => values.push(v.clone().expect("loaded")),
                Err(_) => {
                    return Err(format!(
                        "the permissions for inserting into {} are unavailable",
                        self.table
                    ))
                }
            }
        }
        let mut values = values.into_iter();
        let mut filter = InsertFilter::open(self.user.as_deref());
        if self.allowed.is_some() && values.next().expect("one per receiver").is_empty() {
            return Err(format!("you may not insert into {}", self.table));
        }
        if let Some((column, _)) = &self.rows {
            filter.rows = RowFilter::In {
                column: column.clone(),
                values: values.next().expect("one per receiver").as_ref().clone(),
            };
        }
        if self.columns.is_some() {
            let granted = values.next().expect("one per receiver");
            filter.columns = Some(granted.iter().cloned().collect());
        }
        Ok(filter)
    }
}

/// Every table's rule, and the providers behind them.
pub struct Security {
    /// For tables without a rule; `None`: no security at all.
    default: Option<Default>,
    /// Which tables a subject may read (`[security.tables]`).
    tables_grant: Option<Source>,
    tables: BTreeMap<String, Rule>,
    /// Who may insert what, for tables whose inserts have grants (`[tables.<t>.insert]`).
    inserts: BTreeMap<String, InsertRule>,
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
            inserts: BTreeMap::new(),
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

    /// `table` is readable only while `source` grants the subject anything.
    pub fn table(mut self, table: &str, source: Source) -> Self {
        self.rule(table).table = Some(source);
        self
    }

    /// The tables a subject may read: those `source` grants them.
    pub fn tables(mut self, source: Source) -> Self {
        self.tables_grant = Some(source);
        self
    }

    /// Who may insert what into `table`.
    pub fn inserts(mut self, table: &str, rule: InsertRule) -> Self {
        self.inserts.insert(table.to_string(), rule);
        self
    }

    fn rule(&mut self, table: &str) -> &mut Rule {
        self.tables.entry(table.to_string()).or_insert(Rule {
            table: None,
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
            let provider: Arc<dyn PermissionProvider> = match (&security.providers[&name], grants) {
                (ProviderConfig::Postgres(c), ProviderGrants::Postgres(g)) => {
                    postgres::PostgresProvider::start(&name, c, g)?
                }
                (ProviderConfig::WorkOs(c), ProviderGrants::WorkOs(g)) => {
                    workos::WorkOsProvider::start(&name, c, g)?
                }
                _ => unreachable!("grants are planned per their provider's kind"),
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
                    table: rule.table.map(source),
                    rows,
                    columns: rule.columns.map(source),
                },
            );
        }
        for (table, i) in planned.inserts {
            let rule = InsertRule {
                table: i.table.map(source),
                rows: i.rows.map(|(column, s)| (column, source(s))),
                columns: i.columns.map(source),
            };
            out.inserts.insert(table, rule);
        }
        Ok(out)
    }

    /// What `user` (none: anonymous) may insert into `table`, as its grants say; without
    /// grants, any row (whether the table takes inserts at all, and from whom, is the engine's
    /// check: `[tables.<t>.insert]`). Reads' security doesn't apply.
    pub fn insert_access(&self, user: Option<&str>, table: &str) -> Result<InsertAccess, String> {
        let mut access = InsertAccess {
            table: table.to_string(),
            user: user.map(str::to_string),
            allowed: None,
            rows: None,
            columns: None,
        };
        let Some(rule) = self.inserts.get(table) else {
            return Ok(access);
        };
        let watch = |s: &Source| -> Result<watch::Receiver<Granted>, String> {
            let user = user.ok_or_else(|| {
                format!("inserting into {table} needs a user (send `hello` first)")
            })?;
            Ok(s.provider.watch(s.grant, user))
        };
        access.allowed = rule.table.as_ref().map(watch).transpose()?;
        if let Some((column, s)) = &rule.rows {
            access.rows = Some((column.clone(), watch(s)?));
        }
        access.columns = rule.columns.as_ref().map(watch).transpose()?;
        Ok(access)
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
                if let Some(s) = &rule.table {
                    access.allowed = Some(watch(s)?);
                }
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
    /// Inserts' grants: table, rows (column, grant), columns.
    inserts: BTreeMap<String, PlannedInsert>,
    /// Per provider, its grants.
    grants: BTreeMap<String, ProviderGrants>,
}

struct PlannedInsert {
    table: Option<(String, usize)>,
    rows: Option<(String, (String, usize))>,
    columns: Option<(String, usize)>,
}

struct PlannedRule {
    table: Option<(String, usize)>,
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
    let mut grants: BTreeMap<String, ProviderGrants> = BTreeMap::new();
    let mut register = |at: &str, provider: &str, text: &str, kind: GrantKind| {
        let config = security
            .providers
            .get(provider)
            .with_context(|| format!("{at}: no provider {provider} in [security.providers]"))?;
        let list = grants.entry(provider.to_string()).or_insert(match config {
            ProviderConfig::Postgres(_) => ProviderGrants::Postgres(Vec::new()),
            ProviderConfig::WorkOs(_) => ProviderGrants::WorkOs(Vec::new()),
        });
        let index = match list {
            ProviderGrants::Postgres(list) => {
                list.push(Grant::parse(text).map_err(|e| anyhow::anyhow!("{at}.grants: {e}"))?);
                list.len() - 1
            }
            ProviderGrants::WorkOs(list) => {
                list.push(workos::Grant::new(text, kind).with_context(|| format!("{at}.grants"))?);
                list.len() - 1
            }
        };
        anyhow::Ok((provider.to_string(), index))
    };
    let tables_grant = match &security.tables {
        Some(g) => {
            if security.default.is_none() {
                bail!("security.tables: needs `default` in [security]");
            }
            Some(register(
                "security.tables",
                &g.provider,
                &g.grants,
                GrantKind::Values,
            )?)
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
        let table_grant = match &s.table {
            Some(g) => Some(register(
                &format!("{at}.table"),
                &g.provider,
                &g.grants,
                GrantKind::Check,
            )?),
            None => None,
        };
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
                // with only a `table` grant: all rows, while it allows the table
                if table_grant.is_some() {
                    PlannedRows::All
                } else {
                    PlannedRows::Denied
                }
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
                    source: register(&at, provider, text, GrantKind::Values)?,
                    column,
                }
            }
        };
        let columns = match &s.columns {
            Some(g) => Some(register(
                &format!("{at}.columns"),
                &g.provider,
                &g.grants,
                GrantKind::Values,
            )?),
            None => None,
        };
        rules.insert(
            table.clone(),
            PlannedRule {
                table: table_grant,
                rows,
                columns,
            },
        );
    }
    let mut inserts = BTreeMap::new();
    for (table, config) in &tables.tables {
        let Some(i) = config.insert.as_ref().filter(|i| i.has_grants()) else {
            continue;
        };
        let at = format!("tables.{table}.insert");
        let table_grant = match &i.table {
            Some(g) => Some(register(
                &format!("{at}.table"),
                &g.provider,
                &g.grants,
                GrantKind::Check,
            )?),
            None => None,
        };
        let planned = PlannedInsert {
            table: table_grant,
            rows: match &i.rows {
                Some(r) => Some((
                    r.column.clone(),
                    register(
                        &format!("{at}.rows"),
                        &r.provider,
                        &r.grants,
                        GrantKind::Values,
                    )?,
                )),
                None => None,
            },
            columns: match &i.columns {
                Some(g) => Some(register(
                    &format!("{at}.columns"),
                    &g.provider,
                    &g.grants,
                    GrantKind::Values,
                )?),
                None => None,
            },
        };
        inserts.insert(table.clone(), planned);
    }
    for (name, provider) in &security.providers {
        let checked = match provider {
            ProviderConfig::Postgres(c) => c.check(),
            ProviderConfig::WorkOs(c) => c.check(),
        };
        checked.with_context(|| format!("security.providers.{name}"))?;
    }
    Ok(Planned {
        tables: tables_grant,
        rules,
        inserts,
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

    /// Pairs nobody watches anymore go.
    pub fn prune(&mut self) {
        self.senders.retain(|_, tx| tx.receiver_count() > 0);
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
        let ProviderGrants::Postgres(grants) = &p.grants["pg"] else {
            panic!("postgres grants");
        };
        let tables: Vec<_> = grants.iter().map(|g| g.table.as_str()).collect();
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
    fn workos_grants_are_permissions_asked_as_their_use_needs() {
        let (security, tables) = config(
            r#"
            [security]
            default = "deny"
            [security.providers.workos]
            kind = "workos"
            organization = "acme"
            poll = "2s"
            [tables.messages.security]
            table = { provider = "workos", grants = "messages:read" }
            provider = "workos"
            column = "user_id"
            grants = "feed:read"
            [tables.users.security]
            table = { provider = "workos", grants = "users:read" }
            "#,
        );
        let p = plan(&security, &tables).unwrap();
        let ProviderGrants::WorkOs(grants) = &p.grants["workos"] else {
            panic!("workos grants");
        };
        let asked: Vec<_> = grants
            .iter()
            .map(|g| (g.permission.as_str(), g.kind))
            .collect();
        assert_eq!(
            asked,
            [
                ("messages:read", GrantKind::Check),
                ("feed:read", GrantKind::Values),
                ("users:read", GrantKind::Check),
            ]
        );
        assert_eq!(p.rules["messages"].table, Some(("workos".to_string(), 0)));
        // only a `table` grant: every row, while it allows the table
        assert!(matches!(p.rules["users"].rows, PlannedRows::All));
        assert_eq!(p.rules["users"].table, Some(("workos".to_string(), 2)));
    }

    #[test]
    fn a_table_grant_allows_while_it_answers_anything() {
        let p = MemoryProvider::loaded();
        let security = Security::secured(Default::Deny).table(
            "messages",
            Source {
                provider: p.clone(),
                grant: 0,
            },
        );
        assert!(security
            .access(None, "messages")
            .unwrap_err()
            .contains("needs a user"));
        let a = security.access(Some("dave"), "messages").unwrap();
        assert!(matches!(a.current(), Decision::Denied(e) if e.contains("table messages")));
        p.set(0, "dave", &["messages:read"]);
        assert_eq!(a.current(), Decision::Filter(TableFilter::default()));
        p.set(0, "dave", &[]);
        assert!(matches!(a.current(), Decision::Denied(_)));
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

    #[test]
    fn insert_grants_are_planned_without_read_security() {
        let (security, tables) = config(&format!(
            r#"{PROVIDER}
            [tables.messages.insert]
            user = "author"
            table = {{ provider = "pg", grants = "SELECT a FROM can_write WHERE u = {{subject:String}}" }}
            rows = {{ column = "chat_id", provider = "pg", grants = "SELECT c FROM members WHERE u = {{subject:String}}" }}
            [tables.notes.insert]
            public = true
            "#
        ));
        let p = plan(&security, &tables).unwrap();
        let pg = |i| ("pg".to_string(), i);
        assert_eq!(p.inserts["messages"].table, Some(pg(1)));
        assert_eq!(p.inserts["messages"].rows, Some(("chat_id".into(), pg(2))));
        assert_eq!(p.inserts["messages"].columns, None);
        assert!(!p.inserts.contains_key("notes"), "nothing to ask for");
        assert!(
            !p.rules.contains_key("messages"),
            "reads are another matter"
        );
        let (security, tables) = config(&format!(
            "{PROVIDER}\n[tables.m.insert]\ncolumns = {{ provider = \"nope\", grants = \"x\" }}\n"
        ));
        let e = format!("{:#}", plan(&security, &tables).err().unwrap());
        assert!(
            e.contains("tables.m.insert.columns: no provider nope"),
            "{e}"
        );
    }

    #[tokio::test]
    async fn what_a_user_may_insert() {
        let p = MemoryProvider::loaded();
        let source = |grant| Source {
            provider: p.clone(),
            grant,
        };
        let security = Security::open().inserts(
            "messages",
            InsertRule {
                table: Some(source(0)),
                rows: Some(("chat_id".into(), source(1))),
                columns: Some(source(2)),
            },
        );
        // tables without grants: anything (the engine checks whether they take inserts at all)
        let mut a = security.insert_access(None, "notes").unwrap();
        assert_eq!(a.decide().await, Ok(InsertFilter::open(None)));
        assert!(security
            .insert_access(None, "messages")
            .err()
            .unwrap()
            .contains("needs a user"));
        let mut a = security.insert_access(Some("alice"), "messages").unwrap();
        assert!(a.decide().await.unwrap_err().contains("may not insert"));
        p.set(0, "alice", &["yes"]);
        p.set(1, "alice", &["c1", "c2"]);
        p.set(2, "alice", &["body", "chat_id"]);
        let mut a = security.insert_access(Some("alice"), "messages").unwrap();
        assert_eq!(
            a.decide().await,
            Ok(InsertFilter {
                rows: RowFilter::In {
                    column: "chat_id".into(),
                    values: vec!["c1".into(), "c2".into()],
                },
                columns: Some(["body".to_string(), "chat_id".to_string()].into()),
                user: Some("alice".into()),
            })
        );
    }
}
