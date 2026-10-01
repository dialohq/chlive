//! A dialect table's columns, resolved from ClickHouse through the naming rules.

use crate::history::{HistoryError, HistorySource};
use crate::rules::NamingRules;

#[derive(Debug, Clone, PartialEq)]
pub struct Column {
    /// Name in the dialect and in output rows.
    pub name: String,
    /// Column in ClickHouse.
    pub clickhouse: String,
    /// Key in the topic's JSON objects.
    pub json_key: String,
    /// ClickHouse type, informational for now.
    pub ty: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct TableSchema {
    pub table: String,
    pub clickhouse_table: String,
    pub columns: Vec<Column>,
}

pub async fn resolve(
    table: &str,
    rules: &dyn NamingRules,
    history: &dyn HistorySource,
) -> Result<TableSchema, HistoryError> {
    let clickhouse_table = rules.clickhouse_table(table);
    let columns = history
        .describe(&clickhouse_table)
        .await?
        .into_iter()
        .filter(|c| !rules.is_hidden(&c.name))
        .map(|c| {
            let m = rules.column(&c.name);
            Column {
                name: m.name,
                clickhouse: c.name,
                json_key: m.json_key,
                ty: c.ty,
            }
        })
        .collect();
    Ok(TableSchema {
        table: table.to_string(),
        clickhouse_table,
        columns,
    })
}
