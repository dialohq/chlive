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

/// `[tables.<name>.insert]`: the table takes inserts (without it, it doesn't), and from whom. The
/// engine checks rows against an [`InsertFilter`]; the server decides the filters (its
/// permission providers are the server's). Column names are dialect names, as clients write
/// them.
#[derive(Debug, Clone, Default, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct InsertConfig {
    /// Anyone inserts any row, anonymous connections too.
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub public: bool,
    /// A column the server sets to the inserting user (`hello`); a row may not set it itself.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user: Option<String>,
    /// The column whose value is the message's Kafka key (one key's rows stay in order, in one
    /// partition); without it, messages have no key.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key: Option<String>,
    /// Whether a subject may insert at all: while this grant answers anything.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub table: Option<GrantSource>,
    /// Which values of columns a subject's rows may have: each grant on its own, and a row
    /// must pass all of them. One grant (`rows = { … }`) or a list (`rows = [{ … }, …]`).
    #[serde(
        skip_serializing_if = "Vec::is_empty",
        deserialize_with = "one_or_many"
    )]
    pub rows: Vec<RowGrant>,
    /// Which columns a subject may set; without it, every one.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub columns: Option<GrantSource>,
}

impl InsertConfig {
    /// Whether any grant decides who may insert what.
    pub fn has_grants(&self) -> bool {
        self.table.is_some() || !self.rows.is_empty() || self.columns.is_some()
    }
}

/// `rows`: one grant, or a list of them.
fn one_or_many<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Vec<RowGrant>, D::Error> {
    use serde::de::value::{MapAccessDeserializer, SeqAccessDeserializer};
    use serde::Deserialize;
    struct OneOrMany;
    impl<'de> serde::de::Visitor<'de> for OneOrMany {
        type Value = Vec<RowGrant>;
        fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            f.write_str("a row grant `{ column, provider, grants }`, or a list of them")
        }
        fn visit_map<A: serde::de::MapAccess<'de>>(self, m: A) -> Result<Self::Value, A::Error> {
            RowGrant::deserialize(MapAccessDeserializer::new(m)).map(|g| vec![g])
        }
        fn visit_seq<A: serde::de::SeqAccess<'de>>(self, s: A) -> Result<Self::Value, A::Error> {
            Vec::deserialize(SeqAccessDeserializer::new(s))
        }
    }
    d.deserialize_any(OneOrMany)
}

/// A grant of a column's values: a row's `column` must be one of them.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RowGrant {
    /// The column (dialect name).
    pub column: String,
    /// The permission provider (in `[security.providers]`).
    pub provider: String,
    /// As the provider takes it.
    pub grants: String,
}

/// What one user may insert into one table: the server's decision, which the engine checks
/// every row against.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InsertFilter {
    /// The rows it may insert: each filter must allow a row (none: any row); for
    /// [`RowFilter::In`], `column` is a dialect name.
    pub rows: Vec<RowFilter>,
    /// The columns (dialect names) it may set; none: every one.
    pub columns: Option<BTreeSet<String>>,
    /// Who inserts (`hello`), for the table's `user` column.
    pub user: Option<String>,
}

impl InsertFilter {
    /// Any row, any column, as `user`.
    pub fn open(user: Option<&str>) -> Self {
        Self {
            rows: Vec::new(),
            columns: None,
            user: user.map(str::to_string),
        }
    }
}
