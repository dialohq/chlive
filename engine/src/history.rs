//! Historical data: ClickHouse over its HTTP interface.

use async_trait::async_trait;
use serde::Deserialize;

use crate::diff::Row;
use crate::table_engine::TableEngine;
use crate::types::quote_string;

#[derive(Debug, thiserror::Error)]
pub enum HistoryError {
    #[error("unknown table {0}")]
    UnknownTable(String),
    #[error("ClickHouse: {0}")]
    ClickHouse(String),
    #[error("ClickHouse request failed: {0}")]
    Transport(String),
}

/// A ClickHouse column as reported by `DESCRIBE TABLE`.
#[derive(Debug, Clone, PartialEq, Deserialize)]
pub struct DescribedColumn {
    pub name: String,
    #[serde(rename = "type")]
    pub ty: String,
}

#[async_trait]
pub trait HistorySource: Send + Sync {
    async fn describe(&self, clickhouse_table: &str) -> Result<Vec<DescribedColumn>, HistoryError>;
    /// The table's engine (fakes: a MergeTree).
    async fn table_engine(&self, database: &str, table: &str) -> Result<TableEngine, HistoryError> {
        let _ = (database, table);
        Ok(TableEngine::MergeTree)
    }
    async fn fetch(&self, sql: &str) -> Result<Vec<Row>, HistoryError>;
}

pub struct ClickHouseHttp {
    client: reqwest::Client,
    url: String,
    user: String,
    password: String,
}

impl ClickHouseHttp {
    pub fn new(url: &str, user: &str, password: &str) -> Self {
        Self {
            client: reqwest::Client::new(),
            url: url.trim_end_matches('/').to_string(),
            user: user.into(),
            password: password.into(),
        }
    }

    /// Run `sql` and return its `JSONEachRow` output, one JSON object per line.
    async fn json_each_row<T: serde::de::DeserializeOwned>(
        &self,
        sql: &str,
    ) -> Result<Vec<T>, HistoryError> {
        let resp = self
            .client
            .post(&self.url)
            .header("X-ClickHouse-User", &self.user)
            .header("X-ClickHouse-Key", &self.password)
            .query(&[
                ("default_format", "JSONEachRow"),
                ("output_format_json_quote_64bit_integers", "0"),
                // NaN and ±inf as "nan"/"inf"/"-inf" rather than null, so they read back as
                // floats (see `Value::to_json`)
                ("output_format_json_quote_denormals", "1"),
            ])
            .body(sql.to_string())
            .send()
            .await
            .map_err(|e| HistoryError::Transport(e.to_string()))?;
        let ok = resp.status().is_success();
        let text = resp
            .text()
            .await
            .map_err(|e| HistoryError::Transport(e.to_string()))?;
        if !ok {
            return Err(HistoryError::ClickHouse(text.trim().to_string()));
        }
        text.lines()
            .filter(|l| !l.is_empty())
            .map(|l| {
                serde_json::from_str(l)
                    .map_err(|e| HistoryError::ClickHouse(format!("bad JSON row: {e}")))
            })
            .collect()
    }
}

#[async_trait]
impl HistorySource for ClickHouseHttp {
    async fn describe(&self, clickhouse_table: &str) -> Result<Vec<DescribedColumn>, HistoryError> {
        match self
            .json_each_row(&format!("DESCRIBE TABLE {clickhouse_table}"))
            .await
        {
            // UNKNOWN_TABLE / UNKNOWN_DATABASE
            Err(HistoryError::ClickHouse(msg))
                if msg.contains("Code: 60.") || msg.contains("Code: 81.") =>
            {
                Err(HistoryError::UnknownTable(clickhouse_table.to_string()))
            }
            other => other,
        }
    }

    async fn table_engine(&self, database: &str, table: &str) -> Result<TableEngine, HistoryError> {
        #[derive(Deserialize)]
        struct Described {
            engine: String,
            engine_full: String,
        }
        let rows: Vec<Described> = self
            .json_each_row(&format!(
                "SELECT engine, engine_full FROM system.tables WHERE database = {} AND name = {}",
                quote_string(database),
                quote_string(table)
            ))
            .await?;
        match rows.first() {
            Some(d) => Ok(TableEngine::from_system(&d.engine, &d.engine_full)),
            None => Err(HistoryError::UnknownTable(format!("{database}.{table}"))),
        }
    }

    async fn fetch(&self, sql: &str) -> Result<Vec<Row>, HistoryError> {
        self.json_each_row(sql).await
    }
}
