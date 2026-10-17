//! Cloning tables in ClickHouse: `CREATE TABLE … AS` (the schema) then `ATTACH PARTITION ALL
//! FROM`, which links the source's existing data parts instead of copying them, so it's fast
//! whatever the table's size. The same steps as continuous-backup's backups.

use async_trait::async_trait;

use super::SnapshotError;
use crate::table_engine::TableEngine;

#[async_trait]
pub trait Cloner: Send + Sync {
    /// Clone `database.source` as `clone_database.clone`, with all its data.
    async fn clone_table(
        &self,
        database: &str,
        source: &str,
        clone_database: &str,
        clone: &str,
    ) -> Result<(), SnapshotError>;
    /// Drop `database.name`, if it exists.
    async fn drop_table(&self, database: &str, name: &str) -> Result<(), SnapshotError>;
    /// Every table, as (database, name), whose name starts with `prefix`.
    async fn list(&self, prefix: &str) -> Result<Vec<(String, String)>, SnapshotError>;
}

/// ClickHouse over HTTP, as a user allowed to create, alter and drop tables in the databases
/// of the cloned tables.
pub struct ClickHouseCloner {
    client: reqwest::Client,
    url: String,
    user: String,
    password: String,
}

impl ClickHouseCloner {
    pub fn new(url: &str, user: &str, password: &str) -> Self {
        Self {
            client: reqwest::Client::new(),
            url: url.trim_end_matches('/').to_string(),
            user: user.into(),
            password: password.into(),
        }
    }

    async fn query(&self, sql: &str) -> Result<String, SnapshotError> {
        let resp = self
            .client
            .post(&self.url)
            .header("X-ClickHouse-User", &self.user)
            .header("X-ClickHouse-Key", &self.password)
            .body(sql.to_string())
            .send()
            .await
            .map_err(|e| SnapshotError(format!("ClickHouse request failed: {e}")))?;
        let ok = resp.status().is_success();
        let text = resp
            .text()
            .await
            .map_err(|e| SnapshotError(format!("ClickHouse request failed: {e}")))?;
        if !ok {
            return Err(SnapshotError(format!("ClickHouse: {}", text.trim())));
        }
        Ok(text)
    }
}

/// `` `name` `` with backquotes escaped.
fn ident(name: &str) -> String {
    format!("`{}`", name.replace('\\', "\\\\").replace('`', "\\`"))
}

/// `'text'` with quotes escaped.
fn string(text: &str) -> String {
    format!("'{}'", text.replace('\\', "\\\\").replace('\'', "\\'"))
}

#[async_trait]
impl Cloner for ClickHouseCloner {
    async fn clone_table(
        &self,
        database: &str,
        source: &str,
        clone_database: &str,
        clone: &str,
    ) -> Result<(), SnapshotError> {
        let src = format!("{}.{}", ident(database), ident(source));
        let dst = format!("{}.{}", ident(clone_database), ident(clone));
        let described = self
            .query(&format!(
                "SELECT engine, engine_full FROM system.tables WHERE database = {} AND name = {} \
                 FORMAT JSONCompactEachRow",
                string(database),
                string(source)
            ))
            .await?;
        let (name, full) = serde_json::from_str::<(String, String)>(described.trim())
            .map_err(|e| SnapshotError(format!("ClickHouse: no engine for {src}: {e}")))?;
        let engine = TableEngine::from_system(&name, &full);
        let clone_engine = engine
            .clone_engine()
            .ok_or_else(|| SnapshotError(format!("can't snapshot a {engine} table ({src})")))?;
        // a replica must have every part the connector inserted before its offsets were read
        if TableEngine::is_replicated(&name) {
            self.query(&format!("SYSTEM SYNC REPLICA {src}")).await?;
        }
        // `CREATE TABLE … CLONE AS` would do both, but the Replicated database engine lacks it;
        // the clone keeps the table's keys, and a ReplacingMergeTree its version columns
        self.query(&format!(
            "CREATE TABLE {dst} ENGINE = {clone_engine} AS {src}"
        ))
        .await?;
        self.query(&format!(
            "ALTER TABLE {dst} ATTACH PARTITION ALL FROM {src}"
        ))
        .await?;
        Ok(())
    }

    async fn drop_table(&self, database: &str, name: &str) -> Result<(), SnapshotError> {
        self.query(&format!(
            "DROP TABLE IF EXISTS {}.{} SYNC",
            ident(database),
            ident(name)
        ))
        .await
        .map(|_| ())
    }

    async fn list(&self, prefix: &str) -> Result<Vec<(String, String)>, SnapshotError> {
        let out = self
            .query(&format!(
                "SELECT database, name FROM system.tables WHERE startsWith(name, {}) \
                 FORMAT JSONCompactEachRow",
                string(prefix)
            ))
            .await?;
        out.lines()
            .filter(|l| !l.is_empty())
            .map(|l| {
                serde_json::from_str::<(String, String)>(l)
                    .map_err(|e| SnapshotError(format!("ClickHouse: unexpected row {l}: {e}")))
            })
            .collect()
    }
}
