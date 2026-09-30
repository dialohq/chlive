//! Minimal ClickHouse HTTP interface client.

use std::collections::BTreeMap;
use std::time::Duration;

use anyhow::{bail, Context, Result};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// Bound query parameters: name → textual value, sent as `param_<name>`.
pub type Params = BTreeMap<String, String>;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct Column {
    pub name: String,
    #[serde(rename = "type")]
    pub ty: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct QueryResult {
    pub meta: Vec<Column>,
    pub data: Vec<Map<String, Value>>,
}

#[derive(Clone)]
pub struct ChHttp {
    client: reqwest::Client,
    url: String,
    user: String,
    password: String,
    database: String,
}

/// An error reported by ClickHouse itself (as opposed to a transport failure).
#[derive(Debug, thiserror::Error)]
#[error("ClickHouse error: {0}")]
pub struct ClickHouseError(pub String);

impl ChHttp {
    pub fn new(url: &str, user: &str, password: &str, database: &str) -> Self {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(300))
            .build()
            .expect("reqwest client");
        Self {
            client,
            url: url.trim_end_matches('/').to_string(),
            user: user.into(),
            password: password.into(),
            database: database.into(),
        }
    }

    pub fn database(&self) -> &str {
        &self.database
    }

    pub fn with_database(&self, database: &str) -> Self {
        Self { database: database.into(), ..self.clone() }
    }

    async fn post(&self, query_in_url: Option<&str>, body: String, params: &Params, settings: &[(&str, &str)]) -> Result<String> {
        let mut qs: Vec<(String, String)> = vec![("database".into(), self.database.clone())];
        if let Some(q) = query_in_url {
            qs.push(("query".into(), q.into()));
        }
        for (k, v) in settings {
            qs.push(((*k).into(), (*v).into()));
        }
        for (k, v) in params {
            qs.push((format!("param_{k}"), v.clone()));
        }
        let resp = self
            .client
            .post(&self.url)
            .header("X-ClickHouse-User", &self.user)
            .header("X-ClickHouse-Key", &self.password)
            .query(&qs)
            .body(body)
            .send()
            .await
            .with_context(|| format!("sending request to {}", self.url))?;
        let status = resp.status();
        let text = resp.text().await.context("reading ClickHouse response")?;
        if !status.is_success() {
            return Err(ClickHouseError(text.trim().to_string()).into());
        }
        Ok(text)
    }

    /// Run a statement that returns no rows (DDL, INSERT ... SELECT).
    pub async fn exec(&self, sql: &str, params: &Params) -> Result<()> {
        self.post(None, sql.to_string(), params, &[]).await.map(|_| ())
    }

    /// Run a SELECT and return its rows as JSON objects.
    pub async fn query(&self, sql: &str, params: &Params) -> Result<QueryResult> {
        let text = self
            .post(
                None,
                sql.to_string(),
                params,
                &[
                    ("default_format", "JSON"),
                    ("output_format_json_quote_64bit_integers", "0"),
                    ("output_format_json_quote_denormals", "1"),
                ],
            )
            .await?;
        serde_json::from_str(&text).with_context(|| {
            format!("decoding ClickHouse JSON response: {}", text.chars().take(200).collect::<String>())
        })
    }

    /// Insert newline-delimited JSON rows.
    pub async fn insert_json_each_row(&self, table: &str, rows: String) -> Result<()> {
        if rows.is_empty() {
            bail!("empty insert");
        }
        let q = format!("INSERT INTO {table} FORMAT JSONEachRow");
        self.post(Some(&q), rows, &Params::new(), &[]).await.map(|_| ())
    }
}
