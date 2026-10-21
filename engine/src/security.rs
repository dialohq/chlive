//! Security, as the engine sees it: per table, which rows and which columns a subscriber may
//! see ([`TableFilter`]). Where they come from (permission providers, who's asking) is the
//! server's business. The binder ANDs each table's row filter into every reference to that
//! table, and leaves the columns it may not see out of the table (they can't be named, and `*`
//! skips them); both are part of the plan and its identity.

use std::collections::{BTreeMap, BTreeSet};

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

/// What a subscriber may see of one table.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TableFilter {
    pub rows: RowFilter,
    /// The columns (dialect names) it may see; none: every one.
    pub columns: Option<BTreeSet<String>>,
}

impl Default for TableFilter {
    fn default() -> Self {
        Self {
            rows: RowFilter::All,
            columns: None,
        }
    }
}

impl TableFilter {
    /// Every row and column: as if there were no filter.
    pub fn is_open(&self) -> bool {
        self.rows == RowFilter::All && self.columns.is_none()
    }
}

/// Filters by table (dialect name); a table without one is read whole.
pub type Filters = BTreeMap<String, TableFilter>;

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
    /// query over a permissions table; for WorkOS, a permission slug).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub grants: Option<String>,
    /// Which of its columns (dialect names) a subject may see; without it, every one.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub columns: Option<GrantSource>,
    /// Whether a subject may read it at all: while this grant answers anything (for WorkOS, a
    /// permission check).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub table: Option<GrantSource>,
}

/// A provider's grant: which strings (table names, column names, …) a subject is granted.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GrantSource {
    /// The permission provider (in `[security.providers]`).
    pub provider: String,
    /// As the provider takes it (for Postgres, a query over a permissions table; for WorkOS, a
    /// permission slug).
    pub grants: String,
}
