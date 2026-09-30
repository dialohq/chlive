//! The local cache the server merges in. Currently a separate ClickHouse server over HTTP;
//! the trait exists so it can be swapped for embedded chDB later.

use async_trait::async_trait;

use crate::ch_http::{ChHttp, Params, QueryResult};

#[async_trait]
pub trait CacheEngine: Send + Sync + 'static {
    async fn exec(&self, sql: &str, params: &Params) -> anyhow::Result<()>;
    async fn query(&self, sql: &str, params: &Params) -> anyhow::Result<QueryResult>;
    async fn insert_json_each_row(&self, table: &str, rows: String) -> anyhow::Result<()>;
}

pub struct ClickHouseCache(pub ChHttp);

#[async_trait]
impl CacheEngine for ClickHouseCache {
    async fn exec(&self, sql: &str, params: &Params) -> anyhow::Result<()> {
        self.0.exec(sql, params).await
    }
    async fn query(&self, sql: &str, params: &Params) -> anyhow::Result<QueryResult> {
        self.0.query(sql, params).await
    }
    async fn insert_json_each_row(&self, table: &str, rows: String) -> anyhow::Result<()> {
        self.0.insert_json_each_row(table, rows).await
    }
}
