use std::collections::{HashMap, HashSet};

/// Columns of an allowlisted source table, as reported by `DESCRIBE TABLE` on main.
#[derive(Debug, Clone)]
pub struct TableSchema {
    pub name: String,
    pub columns: Vec<(String, String)>,
}

/// Which function names ClickHouse treats as aggregates. Loaded from main's
/// `system.functions` / `system.aggregate_function_combinators` at startup so that
/// non-allowlisted aggregates are detected reliably; [`FunctionCatalog::builtin`] is a static
/// fallback used by unit tests.
#[derive(Debug, Clone)]
pub struct FunctionCatalog {
    aggregates: HashSet<String>,
    aggregates_ci: HashSet<String>,
    combinators: Vec<String>,
}

/// Aggregates the rewriter can split into State/Merge. Values are the canonical spelling.
const ALLOWED: &[&str] = &[
    "count", "sum", "min", "max", "avg", "any", "uniq", "uniqExact", "argMin", "argMax",
    "quantile", "quantiles",
];
/// Allowlisted aggregates whose names ClickHouse matches case-insensitively.
const ALLOWED_CI: &[&str] = &["count", "sum", "min", "max", "avg"];

impl FunctionCatalog {
    pub fn new(
        aggregates: impl IntoIterator<Item = (String, bool)>,
        combinators: impl IntoIterator<Item = String>,
    ) -> Self {
        let mut exact = HashSet::new();
        let mut ci = HashSet::new();
        for (name, case_insensitive) in aggregates {
            if case_insensitive {
                ci.insert(name.to_lowercase());
            }
            exact.insert(name);
        }
        let mut combinators: Vec<String> = combinators.into_iter().collect();
        // longest first so e.g. "OrDefault" is tried before "If"-like short suffixes
        combinators.sort_by_key(|c| std::cmp::Reverse(c.len()));
        Self { aggregates: exact, aggregates_ci: ci, combinators }
    }

    pub fn builtin() -> Self {
        const AGGS: &[&str] = &[
            "count", "sum", "min", "max", "avg", "any", "anyLast", "anyHeavy", "uniq",
            "uniqExact", "uniqCombined", "uniqCombined64", "uniqHLL12", "uniqTheta", "argMin",
            "argMax", "quantile", "quantiles", "quantileExact", "quantilesExact",
            "quantileTDigest", "quantileTiming", "quantileDeterministic", "median",
            "medianExact", "groupArray", "groupUniqArray", "groupBitmap", "topK", "sumMap",
            "stddevPop", "stddevSamp", "varPop", "varSamp", "corr", "covarPop", "covarSamp",
            "avgWeighted", "entropy", "histogram", "first_value", "last_value", "any_value",
            "sumWithOverflow", "deltaSum", "windowFunnel", "retention", "sequenceMatch",
            "sequenceCount", "simpleLinearRegression", "countDistinct", "groupConcat",
        ];
        const COMBINATORS: &[&str] = &[
            "If", "Array", "Map", "State", "Merge", "MergeState", "ForEach", "Distinct",
            "OrDefault", "OrNull", "Resample", "SimpleState", "ArgMin", "ArgMax",
        ];
        Self::new(
            AGGS.iter().map(|a| (a.to_string(), ALLOWED_CI.contains(a))),
            COMBINATORS.iter().map(|c| c.to_string()),
        )
    }

    pub fn is_aggregate(&self, name: &str) -> bool {
        self.is_aggregate_depth(name, 0)
    }

    fn is_aggregate_depth(&self, name: &str, depth: usize) -> bool {
        if self.aggregates.contains(name) || self.aggregates_ci.contains(&name.to_lowercase()) {
            return true;
        }
        depth < 4
            && self.combinators.iter().any(|c| {
                name.strip_suffix(c.as_str())
                    .is_some_and(|base| !base.is_empty() && self.is_aggregate_depth(base, depth + 1))
            })
    }

    /// If `name` is an allowlisted aggregate (optionally with the `-If` combinator), return
    /// its canonical spelling, e.g. `COUNT` → `count`, `sumIf` → `sumIf`.
    pub fn allowed(&self, name: &str) -> Option<String> {
        let (base, suffix) = match name.strip_suffix("If") {
            Some(b) if !b.is_empty() => (b, "If"),
            _ => (name, ""),
        };
        let canonical = ALLOWED.iter().find(|a| {
            **a == base || (ALLOWED_CI.contains(a) && a.eq_ignore_ascii_case(base))
        })?;
        Some(format!("{canonical}{suffix}"))
    }
}

/// Everything the rewriter needs to know about the environment.
#[derive(Debug, Clone)]
pub struct Catalog {
    pub database: String,
    pub tables: HashMap<String, TableSchema>,
    pub functions: FunctionCatalog,
    pub rows_limit_cap: u64,
    /// Table expression for reading main from the cache; `{table}` is substituted.
    pub source_template: String,
}

impl Catalog {
    pub fn source_for(&self, table: &str) -> String {
        self.source_template
            .replace("{database}", &self.database)
            .replace("{table}", table)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn detects_aggregates_and_combinators() {
        let c = FunctionCatalog::builtin();
        for name in ["count", "COUNT", "sumIf", "uniqState", "quantilesExactIf", "groupArrayDistinct"] {
            assert!(c.is_aggregate(name), "{name}");
        }
        for name in ["toStartOfMinute", "if", "plus", "Merge"] {
            assert!(!c.is_aggregate(name), "{name}");
        }
        assert_eq!(c.allowed("COUNT").as_deref(), Some("count"));
        assert_eq!(c.allowed("countIf").as_deref(), Some("countIf"));
        assert_eq!(c.allowed("uniqExactIf").as_deref(), Some("uniqExactIf"));
        assert_eq!(c.allowed("uniqState"), None);
        assert_eq!(c.allowed("anyLast"), None);
        assert_eq!(c.allowed("If"), None);
    }
}
