//! Permissions in Postgres tables, followed by logical replication: the grants tables are read
//! once, then every change streams in (`pgoutput`). Each row is run through the grants reading
//! its table ([`Grant::eval`]) into their [`GrantIndex`], and on each commit the subjects whose
//! values changed are told.
//!
//! Everything is kept in memory: at each start (and whenever the stream fails) the provider
//! makes its replication slot anew, then reads the tables. Changes from the slot's start on are
//! all in the stream, some also in what was read; applying a row is setting it by its key, so
//! replaying them converges, and the values are told only once the stream is past the read.
//!
//! Grants tables are in the provider's `schema` (`public` by default), or the one a grant names
//! (`FROM app.session_grants`); a table is always matched by its schema and name, so a table of
//! the same name elsewhere (in `public`) is never read.
//!
//! Postgres needs `wal_level = logical`, a publication of the grants tables (with a primary key
//! as replica identity, the default, or REPLICA IDENTITY FULL), and a user with REPLICATION,
//! USAGE on the tables' schemas and SELECT on the tables; nothing else (not even `public`). The
//! publication is the database's own (chlive only checks that the tables are in it); the slot is
//! the provider's own: one per server.

use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{bail, Context, Result};
use pgwire_replication::{Lsn, ReplicationClient, ReplicationConfig, ReplicationEvent, TlsConfig};
use serde::{Deserialize, Serialize};
use tokio::sync::watch;
use tokio_postgres::config::Host;
use tokio_postgres::NoTls;
use tracing::{info, warn};

use super::grants::Grant;
use super::index::{GrantIndex, Key};
use super::pgoutput::{self, Cell, Message, Relation};
use super::{Granted, PermissionProvider, Watchers};

/// `[security.providers.<name>]` with `kind = "postgres"`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PostgresConfig {
    /// `postgres://user[:password]@host[:port]/database`.
    pub url: String,
    /// The publication of the grants tables.
    pub publication: String,
    /// The provider's replication slot, made anew at each start: one per server.
    pub slot: String,
    /// Where grants tables are when a grant doesn't say (`FROM app.grants` does).
    #[serde(default = "public")]
    pub schema: String,
}

fn public() -> String {
    "public".into()
}

impl PostgresConfig {
    pub fn check(&self) -> Result<()> {
        self.replication().map(|_| ())
    }

    fn replication(&self) -> Result<ReplicationConfig> {
        let c: tokio_postgres::Config = self.url.parse().context("url")?;
        let host = match c.get_hosts() {
            [Host::Tcp(h)] => h.clone(),
            [] => bail!("url: no host"),
            _ => bail!("url: one TCP host"),
        };
        let password = c
            .get_password()
            .map(|p| String::from_utf8_lossy(p).into_owned())
            .unwrap_or_default();
        let config = ReplicationConfig::new(
            host,
            c.get_user().unwrap_or("postgres"),
            password,
            c.get_dbname()
                .context("url: no database (postgres://…/<database>)")?,
            &self.slot,
            self.publication.as_str(),
        )
        .with_tls(TlsConfig::disabled());
        Ok(match c.get_ports() {
            [port] => config.with_port(*port),
            _ => config,
        })
    }
}

/// A grants table as Postgres has it.
struct Table {
    columns: Vec<String>,
    /// Its replica identity's columns, by index into `columns`.
    key: Vec<usize>,
}

/// A table, by its schema and name.
type TableName = (String, String);

/// What the stream has built so far, and who's watching it.
struct State {
    grants: Vec<Grant>,
    /// Each grant's table, its schema resolved.
    tables: Vec<TableName>,
    indexes: Vec<GrantIndex>,
    /// The values are complete (the stream is past the tables' read), until it starts over.
    loaded: bool,
    /// (grant, subject) pairs changed since they were last told.
    touched: BTreeSet<(usize, String)>,
    watchers: Watchers,
}

impl State {
    fn granted(&self, grant: usize, subject: &str) -> Granted {
        self.loaded
            .then(|| Arc::new(self.indexes[grant].values(subject)))
    }

    /// Tell the watchers what changed (once loaded).
    fn publish(&mut self) {
        if !self.loaded {
            return;
        }
        for (grant, subject) in std::mem::take(&mut self.touched) {
            let granted = self.granted(grant, &subject);
            self.watchers.publish(grant, &subject, granted);
        }
    }

    /// Loaded: tell every watcher.
    fn load(&mut self) {
        self.loaded = true;
        for pair in self.watchers.watched() {
            self.touched.insert(pair);
        }
        self.publish();
    }

    fn reset(&mut self) {
        self.loaded = false;
        self.touched.clear();
        for i in &mut self.indexes {
            i.clear();
        }
    }

    /// The grants reading the table `schema.name`.
    fn grants_of(&self, schema: &str, name: &str) -> Vec<usize> {
        (0..self.grants.len())
            .filter(|&g| self.tables[g].0 == schema && self.tables[g].1 == name)
            .collect()
    }

    /// The row `key` of table `schema.name`, with these `columns`, now is `row` (none: gone).
    fn apply(
        &mut self,
        (schema, name): (&str, &str),
        columns: &[String],
        key: Key,
        row: Option<&[Option<String>]>,
    ) {
        for g in self.grants_of(schema, name) {
            let granted = row.and_then(|row| {
                self.grants[g].eval(|column| {
                    let i = columns.iter().position(|c| c == column)?;
                    row[i].as_deref()
                })
            });
            for subject in self.indexes[g].apply(key.clone(), granted) {
                self.touched.insert((g, subject));
            }
        }
    }

    fn clear(&mut self, schema: &str, name: &str) {
        for g in self.grants_of(schema, name) {
            for subject in self.indexes[g].clear() {
                self.touched.insert((g, subject));
            }
        }
    }
}

pub struct PostgresProvider {
    state: Arc<Mutex<State>>,
}

impl PermissionProvider for PostgresProvider {
    fn watch(&self, grant: usize, subject: &str) -> watch::Receiver<Granted> {
        let mut s = self.state.lock().unwrap();
        let current = s.granted(grant, subject);
        s.watchers.watch(grant, subject, || current)
    }
}

impl PostgresProvider {
    /// Follow `grants`' tables in the background (read again whenever the stream fails).
    pub fn start(name: &str, config: &PostgresConfig, grants: Vec<Grant>) -> Result<Arc<Self>> {
        config.check()?;
        let tables = grants
            .iter()
            .map(|g| {
                let schema = g.schema.as_ref().unwrap_or(&config.schema);
                (schema.clone(), g.table.clone())
            })
            .collect();
        let state = Arc::new(Mutex::new(State {
            indexes: grants.iter().map(|_| GrantIndex::default()).collect(),
            grants,
            tables,
            loaded: false,
            touched: BTreeSet::new(),
            watchers: Watchers::default(),
        }));
        let provider = Arc::new(Self {
            state: state.clone(),
        });
        let (name, config) = (name.to_string(), config.clone());
        tokio::spawn(async move {
            let mut backoff = Duration::from_secs(1);
            loop {
                state.lock().unwrap().reset();
                let Err(e) = follow(&name, &config, &state, &mut backoff).await;
                warn!(provider = %name, error = %format!("{e:#}"), secs = backoff.as_secs(), "permissions: following Postgres failed; starting over");
                tokio::time::sleep(backoff).await;
                backoff = (backoff * 2).min(Duration::from_secs(30));
            }
        });
        Ok(provider)
    }
}

/// Read the grants tables and follow their changes, until something fails. `backoff` is reset
/// once loaded.
async fn follow(
    name: &str,
    config: &PostgresConfig,
    state: &Mutex<State>,
    backoff: &mut Duration,
) -> Result<std::convert::Infallible> {
    let (client, connection) = tokio_postgres::connect(&config.url, NoTls)
        .await
        .context("connecting")?;
    let _connection = AbortOnDrop(tokio::spawn(connection));

    let names: BTreeSet<TableName> = state.lock().unwrap().tables.iter().cloned().collect();
    let mut tables = HashMap::new();
    for table in &names {
        tables.insert(
            table.clone(),
            describe(&client, config, state, table).await?,
        );
    }

    // a new slot: every change from here on is in the stream
    let dropped = client
        .execute(
            "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots \
             WHERE slot_name = $1 AND NOT active",
            &[&config.slot],
        )
        .await
        .context("dropping the old slot")?;
    let in_use = client
        .query_opt(
            "SELECT 1 FROM pg_replication_slots WHERE slot_name = $1",
            &[&config.slot],
        )
        .await?
        .is_some();
    if in_use {
        bail!(
            "replication slot {} is in use (by another server? each needs its own `slot`)",
            config.slot
        );
    }
    client
        .execute(
            "SELECT pg_create_logical_replication_slot($1, 'pgoutput')",
            &[&config.slot],
        )
        .await
        .context("making the replication slot")?;
    let mut stream = ReplicationClient::connect(config.replication()?)
        .await
        .context("starting replication")?;

    // the tables as they are now (changes since the slot was made replay over it)
    for ((schema, table), t) in &tables {
        let columns: Vec<String> = t
            .columns
            .iter()
            .map(|c| format!("{}::text", quote(c)))
            .collect();
        let qualified = qualified(schema, table);
        let sql = format!("SELECT {} FROM {qualified}", columns.join(", "));
        let rows = client
            .query(&sql, &[])
            .await
            .with_context(|| format!("reading {qualified}"))?;
        let mut s = state.lock().unwrap();
        for row in rows {
            let values: Vec<Option<String>> = (0..t.columns.len()).map(|i| row.get(i)).collect();
            let key = t.key.iter().map(|&i| values[i].clone()).collect();
            s.apply((schema, table), &t.columns, key, Some(&values));
        }
    }
    // the stream is past the read once it's past this
    let read_at: String = client
        .query_one("SELECT pg_current_wal_lsn()::text", &[])
        .await?
        .get(0);
    let read_at = Lsn::parse(&read_at).map_err(|e| anyhow::anyhow!("{e}"))?;
    info!(provider = %name, slot = %config.slot, dropped_old_slot = dropped > 0, tables = ?names, "permissions: read from Postgres; following changes");

    let mut relations: HashMap<u32, Relation> = HashMap::new();
    loop {
        let event = stream
            .recv()
            .await
            .context("replication")?
            .context("the replication stream ended")?;
        let mut s = state.lock().unwrap();
        let at = match event {
            ReplicationEvent::XLogData { data, .. } => {
                match pgoutput::decode(data).map_err(|e| anyhow::anyhow!("pgoutput: {e}"))? {
                    Message::Relation(r) => {
                        relations.insert(r.id, r);
                    }
                    Message::Insert { relation, new } => {
                        change(&mut s, relation_of(&relations, relation)?, None, Some(&new));
                    }
                    Message::Update { relation, old, new } => {
                        let r = relation_of(&relations, relation)?;
                        if new.contains(&Cell::Unchanged) {
                            warn!(provider = %name, table = %r.name, "permissions: an update without its whole row (TOASTed values) is skipped");
                        } else {
                            change(&mut s, r, old.as_deref(), Some(&new));
                        }
                    }
                    Message::Delete { relation, old } => {
                        change(&mut s, relation_of(&relations, relation)?, Some(&old), None);
                    }
                    Message::Truncate { relations: ids } => {
                        for r in ids.iter().filter_map(|id| relations.get(id)) {
                            s.clear(&r.namespace, &r.name);
                        }
                    }
                    Message::Other(_) => {}
                }
                None
            }
            ReplicationEvent::Commit { end_lsn, .. } => {
                stream.update_applied_lsn(end_lsn);
                Some(end_lsn)
            }
            ReplicationEvent::KeepAlive { wal_end, .. } => Some(wal_end),
            _ => None,
        };
        if let Some(at) = at {
            if !s.loaded && at.as_u64() >= read_at.as_u64() {
                info!(provider = %name, "permissions: loaded from Postgres");
                s.load();
                *backoff = Duration::from_secs(1);
            }
            // what a transaction changed is told whole
            s.publish();
        }
    }
}

/// A table's columns and replica identity, checked against what its grants read.
async fn describe(
    client: &tokio_postgres::Client,
    config: &PostgresConfig,
    state: &Mutex<State>,
    (schema, table): &TableName,
) -> Result<Table> {
    let qualified = qualified(schema, table);
    let Some(identity) = client
        .query_opt(
            "SELECT relreplident::text FROM pg_class WHERE oid = to_regclass($1)",
            &[&qualified],
        )
        .await?
        .map(|r| r.get::<_, String>(0))
    else {
        bail!("no table {qualified} in Postgres");
    };
    let rows = client
        .query(
            "SELECT a.attname::text, coalesce(a.attnum = ANY(i.indkey), false) \
             FROM pg_attribute a \
             JOIN pg_class c ON c.oid = a.attrelid \
             LEFT JOIN pg_index i ON i.indrelid = a.attrelid \
               AND ((c.relreplident = 'd' AND i.indisprimary) \
                 OR (c.relreplident = 'i' AND i.indisreplident)) \
             WHERE a.attrelid = to_regclass($1) AND a.attnum > 0 AND NOT a.attisdropped \
             ORDER BY a.attnum",
            &[&qualified],
        )
        .await?;
    let columns: Vec<String> = rows.iter().map(|r| r.get(0)).collect();
    let key: Vec<usize> = match identity.as_str() {
        // FULL: the whole row is the key
        "f" => (0..columns.len()).collect(),
        _ => rows
            .iter()
            .enumerate()
            .filter(|(_, r)| r.get::<_, bool>(1))
            .map(|(i, _)| i)
            .collect(),
    };
    if key.is_empty() {
        bail!(
            "{qualified} has no replica identity: give it a primary key (or REPLICA IDENTITY FULL)"
        );
    }
    let missing = {
        let s = state.lock().unwrap();
        s.grants_of(schema, table).into_iter().find_map(|g| {
            s.grants[g]
                .columns()
                .find(|c| !columns.iter().any(|x| x == c))
                .map(str::to_string)
        })
    };
    if let Some(c) = missing {
        bail!("{qualified} has no column {c} (its grants read it)");
    }
    let published = client
        .query_opt(
            "SELECT 1 FROM pg_publication_tables \
             WHERE pubname = $1 AND schemaname = $2 AND tablename = $3",
            &[&config.publication, schema, table],
        )
        .await?
        .is_some();
    if !published {
        bail!(
            "{qualified} isn't in publication {0} (ALTER PUBLICATION {0} ADD TABLE {qualified})",
            config.publication
        );
    }
    Ok(Table { columns, key })
}

fn relation_of(relations: &HashMap<u32, Relation>, id: u32) -> Result<&Relation> {
    relations
        .get(&id)
        .with_context(|| format!("a change of relation {id} before its description"))
}

/// A change of a row of `r`: `old` its key (or whole row) before, when sent; `new` the row now
/// (none: deleted).
fn change(s: &mut State, r: &Relation, old: Option<&[Cell]>, new: Option<&[Cell]>) {
    let table = (r.namespace.as_str(), r.name.as_str());
    if s.grants_of(table.0, table.1).is_empty() {
        return;
    }
    let columns: Vec<String> = r.columns.iter().map(|c| c.name.clone()).collect();
    let text = |row: &[Cell]| -> Vec<Option<String>> {
        row.iter()
            .map(|c| match c {
                Cell::Text(t) => Some(t.clone()),
                Cell::Null | Cell::Unchanged => None,
            })
            .collect()
    };
    let key_of = |row: &[Option<String>]| -> Key {
        r.columns
            .iter()
            .zip(row)
            .filter(|(c, _)| c.key)
            .map(|(_, v)| v.clone())
            .collect()
    };
    let new = new.map(text);
    let new_key = new.as_deref().map(key_of);
    if let Some(old) = old {
        let old_key = key_of(&text(old));
        // deleted, or its key changed: the old key is gone
        if new_key.as_ref() != Some(&old_key) {
            s.apply(table, &columns, old_key, None);
        }
    }
    if let (Some(key), Some(new)) = (new_key, &new) {
        s.apply(table, &columns, key, Some(new));
    }
}

/// An identifier, double-quoted.
fn quote(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}

/// `"schema"."table"`.
fn qualified(schema: &str, table: &str) -> String {
    format!("{}.{}", quote(schema), quote(table))
}

/// Ends a task when dropped (the connection, with the stream that uses it).
struct AbortOnDrop<T>(tokio::task::JoinHandle<T>);

impl<T> Drop for AbortOnDrop<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}
