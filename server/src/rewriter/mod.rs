//! Query rewriter: turns one client SQL query into the history / tail / merge queries that
//! implement it incrementally. See README.md for the model.

mod aggregate;
mod catalog;
pub mod params;
mod rows;
mod validate;

#[cfg(test)]
mod tests;

use sqlparser::ast::{Expr, Ident, Statement};
use sqlparser::dialect::ClickHouseDialect;
use sqlparser::parser::Parser;

pub use catalog::{Catalog, FunctionCatalog, TableSchema};
pub use params::ParamTypes;

use crate::watermark::Watermark;

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize)]
pub struct RewriteError {
    pub code: &'static str,
    pub message: String,
}

impl RewriteError {
    pub fn unsupported(msg: impl Into<String>) -> Self {
        Self {
            code: "UNSUPPORTED_QUERY",
            message: msg.into(),
        }
    }
    pub fn params(msg: impl Into<String>) -> Self {
        Self {
            code: "INVALID_PARAMS",
            message: msg.into(),
        }
    }
    pub fn parse(msg: impl Into<String>) -> Self {
        Self {
            code: "PARSE_ERROR",
            message: msg.into(),
        }
    }
}

impl std::fmt::Display for RewriteError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.code, self.message)
    }
}

impl std::error::Error for RewriteError {}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize)]
#[serde(rename_all = "lowercase")]
pub enum PlanKind {
    Aggregate,
    Rows,
}

/// Name of the hidden columns carrying the Kafka position of a row in Rows plans.
pub const ROW_KEY_COLUMNS: [&str; 2] = ["__partition", "__offset"];

/// The result of rewriting one query. The SQL depends on the watermark `W` taken at
/// subscribe time, so it is produced by methods rather than stored.
///
/// * `history_sql(W)` runs in the cache, reading main through `remote()`; its result is
///   stored as `sub_<id>_hist`.
/// * `tail_sql(W)` computes the same shape over the local `tail_<table>`.
/// * `merge_sql(W, hist)` combines both and produces the final result.
#[derive(Debug, Clone)]
pub struct Plan {
    pub kind: PlanKind,
    pub table: String,
    /// Columns of the merge result that form the row key.
    pub key_columns: Vec<String>,
    /// Columns sent to clients, in order. Never contains `__` columns.
    pub output_columns: Vec<String>,
    pub has_order: bool,
    pub param_types: ParamTypes,
    /// Canonical SQL (as re-printed by the parser) used for subscription dedupe.
    pub normalized_sql: String,
    select_list: String,
    filter: Option<String>,
    inner_suffix: String,
    merge_select: String,
    merge_suffix: String,
    source: String,
    tail_table: String,
}

impl Plan {
    fn inner(&self, from: &str, pred: &str) -> String {
        let cond = match &self.filter {
            Some(f) => format!("({f}) AND ({pred})"),
            None => pred.to_string(),
        };
        format!(
            "SELECT {} FROM {from} WHERE {cond}{}",
            self.select_list, self.inner_suffix
        )
    }

    pub fn history_sql(&self, w: &Watermark) -> String {
        self.inner(&self.source, &w.history_pred())
    }

    pub fn tail_sql(&self, w: &Watermark) -> String {
        self.inner(&self.tail_table, &w.tail_pred())
    }

    pub fn merge_sql(&self, w: &Watermark, hist_table: &str) -> String {
        format!(
            "SELECT {} FROM (SELECT * FROM {hist_table} UNION ALL {}){}",
            self.merge_select,
            self.tail_sql(w),
            self.merge_suffix
        )
    }
}

/// Knobs that are normally fixed; exposed for tests.
#[derive(Debug, Clone)]
pub struct Options {
    /// Expand references to aggregate aliases (e.g. `big_orders / count()`) into the aliased
    /// expression before rewriting. When false, ClickHouse resolves the alias inside the
    /// merge query instead. Both are tested; expansion is the default.
    pub expand_aliases: bool,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            expand_aliases: true,
        }
    }
}

pub fn rewrite(sql: &str, catalog: &Catalog) -> Result<Plan, RewriteError> {
    rewrite_with(sql, catalog, &Options::default())
}

pub fn rewrite_with(sql: &str, catalog: &Catalog, opts: &Options) -> Result<Plan, RewriteError> {
    let (sql, param_types) = params::extract(sql)?;
    let mut stmts = Parser::parse_sql(&ClickHouseDialect {}, &sql)
        .map_err(|e| RewriteError::parse(e.to_string()))?;
    if stmts.len() != 1 {
        return Err(RewriteError::unsupported(
            "exactly one SELECT statement is expected",
        ));
    }
    let Statement::Query(query) = stmts.remove(0) else {
        return Err(RewriteError::unsupported(
            "only SELECT queries are supported",
        ));
    };
    let normalized_sql = params::restore(&query.to_string(), &param_types);
    let parts = validate::validate(*query, catalog)?;
    let is_aggregate = parts.has_group_by || parts.contains_aggregate(&catalog.functions);
    let mut plan = if is_aggregate {
        aggregate::plan(parts, catalog, opts, &param_types)?
    } else {
        rows::plan(parts, catalog, &param_types)?
    };
    plan.normalized_sql = normalized_sql;
    for s in [
        &mut plan.select_list,
        &mut plan.inner_suffix,
        &mut plan.merge_select,
        &mut plan.merge_suffix,
    ] {
        *s = params::restore(s, &param_types);
    }
    if let Some(f) = &mut plan.filter {
        *f = params::restore(f, &param_types);
    }
    plan.param_types = param_types;
    Ok(plan)
}

/// Quote an output column name as a ClickHouse identifier when needed.
pub(crate) fn quote_ident(name: &str) -> String {
    let plain = !name.is_empty()
        && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
        && !name.starts_with(|c: char| c.is_ascii_digit());
    if plain {
        name.to_string()
    } else {
        format!("`{}`", name.replace('\\', "\\\\").replace('`', "\\`"))
    }
}

pub(crate) fn ident_expr(name: &str) -> Expr {
    Expr::Identifier(Ident::new(name))
}
