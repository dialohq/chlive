//! Row-level security, as the engine sees it: per table, which rows a subscriber may see
//! ([`RowFilter`]). Where the filters come from (permission providers, who's asking) is the
//! server's business; the binder ANDs each table's filter into every reference to that table,
//! so it's part of the plan, its history SQL and its identity.

use std::collections::BTreeMap;

/// The rows of one table a subscriber may see.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum RowFilter {
    /// Every row.
    All,
    /// None at all.
    None,
    /// The rows whose ClickHouse column `column` is one of `values` (read as the column's type,
    /// like parameters are; none: no rows).
    In { column: String, values: Vec<String> },
}

/// Row filters by table (dialect name); a table without one is read whole.
pub type RowFilters = BTreeMap<String, RowFilter>;

/// `[tables.<name>.security]`: how the server secures a table. The engine only carries it; the
/// server reads it (its permission providers are the server's).
#[derive(Debug, Clone, Default, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct TableSecurity {
    /// Anyone reads it whole.
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub public: bool,
    /// The permission provider (in `[security.providers]`) deciding its rows.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub provider: Option<String>,
    /// The ClickHouse column its rows are filtered by.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub column: Option<String>,
    /// Which values of `column` a subject may see, as the provider takes it (for Postgres, a
    /// query over a permissions table).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub grants: Option<String>,
}
