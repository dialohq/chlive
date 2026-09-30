//! Structural validation: accept only the grammar in the spec and pull out its parts.

use std::ops::ControlFlow;

use sqlparser::ast::{
    visit_expressions, DuplicateTreatment, Expr, Function, FunctionArguments, GroupByExpr,
    LimitClause, ObjectNamePart, OrderByExpr, OrderByKind, Query, SelectItem, SetExpr, TableFactor,
    Value,
};

use super::{params, Catalog, FunctionCatalog, RewriteError};

/// The accepted pieces of a validated `SELECT`.
#[derive(Debug)]
pub struct Parts {
    pub table: String,
    pub projection: Vec<SelectItem>,
    pub selection: Option<Expr>,
    pub group_by: Vec<Expr>,
    pub has_group_by: bool,
    pub having: Option<Expr>,
    pub order_by: Vec<OrderByExpr>,
    pub limit: Option<u64>,
}

impl Parts {
    pub fn contains_aggregate(&self, f: &FunctionCatalog) -> bool {
        self.projection.iter().any(|item| match item {
            SelectItem::UnnamedExpr(e) | SelectItem::ExprWithAlias { expr: e, .. } => {
                contains_aggregate(e, f)
            }
            _ => false,
        }) || self
            .having
            .as_ref()
            .is_some_and(|h| contains_aggregate(h, f))
            || self.order_by.iter().any(|o| contains_aggregate(&o.expr, f))
    }
}

pub fn func_name(f: &Function) -> Option<&str> {
    match f.name.0.as_slice() {
        [ObjectNamePart::Identifier(i)] => Some(i.value.as_str()),
        _ => None,
    }
}

pub fn contains_aggregate(e: &Expr, f: &FunctionCatalog) -> bool {
    visit_expressions(e, |e| match e {
        Expr::Function(func) if func_name(func).is_some_and(|n| f.is_aggregate(n)) => {
            ControlFlow::Break(())
        }
        _ => ControlFlow::Continue(()),
    })
    .is_break()
}

fn unsupported<T>(msg: impl Into<String>) -> Result<T, RewriteError> {
    Err(RewriteError::unsupported(msg))
}

pub fn validate(q: Query, catalog: &Catalog) -> Result<Parts, RewriteError> {
    if q.with.is_some() {
        return unsupported("WITH / CTEs are not supported");
    }
    if q.settings.is_some() {
        return unsupported("SETTINGS is not supported");
    }
    if q.format_clause.is_some() {
        return unsupported("FORMAT is not supported");
    }
    if q.fetch.is_some()
        || !q.locks.is_empty()
        || q.for_clause.is_some()
        || !q.pipe_operators.is_empty()
    {
        return unsupported("FETCH / FOR / locking clauses are not supported");
    }
    let limit = match &q.limit_clause {
        None => None,
        Some(LimitClause::LimitOffset {
            limit,
            offset,
            limit_by,
        }) => {
            if offset.is_some() {
                return unsupported("OFFSET is not supported");
            }
            if !limit_by.is_empty() {
                return unsupported("LIMIT BY is not supported");
            }
            match limit {
                None => None,
                Some(Expr::Value(v)) => match &v.value {
                    Value::Number(n, _) => Some(n.parse::<u64>().map_err(|_| {
                        RewriteError::unsupported("LIMIT must be a non-negative integer literal")
                    })?),
                    _ => return unsupported("LIMIT must be a non-negative integer literal"),
                },
                Some(_) => return unsupported("LIMIT must be a non-negative integer literal"),
            }
        }
        Some(LimitClause::OffsetCommaLimit { .. }) => {
            return unsupported("OFFSET is not supported")
        }
    };
    let order_by = match q.order_by {
        None => Vec::new(),
        Some(ob) => {
            if ob.interpolate.is_some() {
                return unsupported("INTERPOLATE is not supported");
            }
            match ob.kind {
                OrderByKind::All(_) => return unsupported("ORDER BY ALL is not supported"),
                OrderByKind::Expressions(exprs) => {
                    if exprs.iter().any(|e| e.with_fill.is_some()) {
                        return unsupported("WITH FILL is not supported");
                    }
                    exprs
                }
            }
        }
    };
    let select = match *q.body {
        SetExpr::Select(s) => s,
        SetExpr::SetOperation { .. } => {
            return unsupported("UNION / INTERSECT / EXCEPT are not supported")
        }
        SetExpr::Query(_) => return unsupported("subqueries are not supported"),
        _ => return unsupported("only plain SELECT queries are supported"),
    };
    let s = *select;
    if s.distinct.is_some() {
        return unsupported("DISTINCT is not supported; use GROUP BY");
    }
    if s.into.is_some() {
        return unsupported("INTO is not supported");
    }
    if s.top.is_some()
        || s.exclude.is_some()
        || s.select_modifiers.is_some()
        || !s.optimizer_hints.is_empty()
    {
        return unsupported("SELECT modifiers are not supported");
    }
    if s.prewhere.is_some() {
        return unsupported("PREWHERE is not supported; use WHERE");
    }
    if !s.named_window.is_empty() || s.qualify.is_some() {
        return unsupported("window functions are not supported");
    }
    if !s.lateral_views.is_empty()
        || !s.connect_by.is_empty()
        || !s.cluster_by.is_empty()
        || !s.distribute_by.is_empty()
        || !s.sort_by.is_empty()
        || s.value_table_mode.is_some()
    {
        return unsupported("unsupported SELECT clause");
    }
    let table = validate_from(&s.from, catalog)?;
    let (group_by, has_group_by) = match s.group_by {
        GroupByExpr::All(_) => return unsupported("GROUP BY ALL is not supported; list the keys"),
        GroupByExpr::Expressions(exprs, modifiers) => {
            if !modifiers.is_empty() {
                return unsupported("GROUP BY modifiers (ROLLUP, CUBE, TOTALS) are not supported");
            }
            let has = !exprs.is_empty();
            (exprs, has)
        }
    };
    let parts = Parts {
        table,
        projection: s.projection,
        selection: s.selection,
        group_by,
        has_group_by,
        having: s.having,
        order_by,
        limit,
    };
    check_expressions(&parts, &catalog.functions)?;
    Ok(parts)
}

fn validate_from(
    from: &[sqlparser::ast::TableWithJoins],
    catalog: &Catalog,
) -> Result<String, RewriteError> {
    let [twj] = from else {
        return if from.is_empty() {
            unsupported("a FROM clause with one table is required")
        } else {
            unsupported("joins are not supported (only a single table)")
        };
    };
    if !twj.joins.is_empty() {
        return unsupported("joins (including ARRAY JOIN) are not supported");
    }
    let TableFactor::Table {
        name,
        alias,
        args,
        sample,
        version,
        with_hints,
        partitions,
        json_path,
        index_hints,
        with_ordinality,
    } = &twj.relation
    else {
        return unsupported("FROM must name a table directly; subqueries are not supported");
    };
    if args.is_some() {
        return unsupported("table functions are not supported");
    }
    if let Some(a) = alias {
        if a.name.value.eq_ignore_ascii_case("FINAL") {
            return unsupported(
                "FINAL is not supported (source tables are plain append-only MergeTree)",
            );
        }
        return unsupported("table aliases are not supported");
    }
    if sample.is_some() {
        return unsupported("SAMPLE is not supported");
    }
    if version.is_some()
        || !with_hints.is_empty()
        || !partitions.is_empty()
        || json_path.is_some()
        || !index_hints.is_empty()
        || *with_ordinality
    {
        return unsupported("unsupported table modifier");
    }
    let parts: Vec<&str> = name
        .0
        .iter()
        .map(|p| match p {
            ObjectNamePart::Identifier(i) => Ok(i.value.as_str()),
            _ => unsupported("unsupported table name"),
        })
        .collect::<Result<_, _>>()?;
    let table = match parts.as_slice() {
        [t] => *t,
        [db, t] if *db == catalog.database => *t,
        _ => return unsupported(format!("unknown table '{name}'")),
    };
    if !catalog.tables.contains_key(table) {
        let mut allowed: Vec<_> = catalog.tables.keys().cloned().collect();
        allowed.sort();
        return unsupported(format!(
            "table '{table}' is not allowlisted (allowed: {})",
            allowed.join(", ")
        ));
    }
    Ok(table.to_string())
}

const TIME_FUNCTIONS: &[&str] = &[
    "now",
    "now64",
    "nowinblock",
    "today",
    "yesterday",
    "current_timestamp",
    "current_date",
    "curdate",
    "localtimestamp",
    "utctimestamp",
    "utc_timestamp",
    "sysdate",
    "timezoneof",
];
const NONDETERMINISTIC: &[&str] = &[
    "uptime",
    "rownumberinblock",
    "rownumberinallblocks",
    "blocknumber",
    "blocksize",
    "runningdifference",
    "runningdifferencestartingwithfirstvalue",
    "runningaccumulate",
    "neighbor",
    "hostname",
    "fqdn",
    "serveruuid",
    "currentuser",
    "currentdatabase",
    "currentprofiles",
    "currentroles",
    "initialqueryid",
    "queryid",
    "sleep",
    "sleepeachrow",
    "throwif",
    "getsetting",
    "dictget",
    "joinget",
];
const NONDETERMINISTIC_PREFIXES: &[&str] = &[
    "rand",
    "random",
    "generateuuid",
    "generateulid",
    "generatesnowflake",
    "fuzzbits",
    "dictget",
];

fn check_function(f: &Function) -> Result<(), RewriteError> {
    let Some(name) = func_name(f) else {
        return unsupported(format!(
            "qualified function names are not supported: {}",
            f.name
        ));
    };
    let lower = name.to_ascii_lowercase();
    if TIME_FUNCTIONS.contains(&lower.as_str()) {
        return unsupported(format!(
            "{name}() is not allowed: time-relative filters would silently go stale in a live query; \
             pass the time as a parameter instead, e.g. ts >= {{since:DateTime64(3)}}"
        ));
    }
    if NONDETERMINISTIC.contains(&lower.as_str())
        || NONDETERMINISTIC_PREFIXES
            .iter()
            .any(|p| lower.starts_with(p))
    {
        return unsupported(format!(
            "{name}() is not allowed: live queries must be deterministic"
        ));
    }
    if f.over.is_some() {
        return unsupported("window functions (OVER) are not supported");
    }
    if f.filter.is_some()
        || !f.within_group.is_empty()
        || f.null_treatment.is_some()
        || f.uses_odbc_syntax
    {
        return unsupported(format!("unsupported syntax in call to {name}"));
    }
    for args in [&f.args, &f.parameters] {
        match args {
            FunctionArguments::Subquery(_) => return unsupported("subqueries are not supported"),
            FunctionArguments::List(l) => {
                if matches!(l.duplicate_treatment, Some(DuplicateTreatment::Distinct)) {
                    return unsupported(format!(
                        "{name}(DISTINCT ...) is not supported; use uniqExact(...)"
                    ));
                }
                if !l.clauses.is_empty() {
                    return unsupported(format!("unsupported clause in call to {name}"));
                }
            }
            FunctionArguments::None => {}
        }
    }
    Ok(())
}

fn check_expr(e: &Expr) -> Result<(), RewriteError> {
    match e {
        Expr::Subquery(_) | Expr::InSubquery { .. } | Expr::Exists { .. } => {
            unsupported("subqueries are not supported")
        }
        Expr::Function(f) => check_function(f),
        Expr::Identifier(i) => check_ident(&i.value),
        // There is exactly one table and table aliases are rejected, so qualifiers are noise.
        Expr::CompoundIdentifier(parts) => unsupported(format!(
            "qualified column names are not supported: {}",
            parts
                .iter()
                .map(|p| p.value.as_str())
                .collect::<Vec<_>>()
                .join(".")
        )),
        _ => Ok(()),
    }
}

fn check_ident(name: &str) -> Result<(), RewriteError> {
    if name.starts_with("__") && !name.starts_with(params::PREFIX) {
        return unsupported(format!(
            "identifiers starting with '__' are reserved: {name}"
        ));
    }
    Ok(())
}

fn check_expressions(parts: &Parts, functions: &FunctionCatalog) -> Result<(), RewriteError> {
    let mut exprs: Vec<&Expr> = Vec::new();
    for item in &parts.projection {
        match item {
            SelectItem::UnnamedExpr(e) => exprs.push(e),
            SelectItem::ExprWithAlias { expr, alias } => {
                check_ident(&alias.value)?;
                exprs.push(expr);
            }
            SelectItem::Wildcard(opts) => {
                if opts.opt_ilike.is_some()
                    || opts.opt_exclude.is_some()
                    || opts.opt_except.is_some()
                    || opts.opt_replace.is_some()
                    || opts.opt_rename.is_some()
                {
                    return unsupported("SELECT * modifiers are not supported");
                }
            }
            SelectItem::QualifiedWildcard(..) => {
                return unsupported("qualified wildcards are not supported")
            }
            _ => return unsupported("unsupported select item"),
        }
    }
    exprs.extend(parts.selection.iter());
    exprs.extend(parts.group_by.iter());
    exprs.extend(parts.having.iter());
    exprs.extend(parts.order_by.iter().map(|o| &o.expr));
    for e in exprs {
        if let ControlFlow::Break(err) = visit_expressions(e, |e| match check_expr(e) {
            Ok(()) => ControlFlow::Continue(()),
            Err(err) => ControlFlow::Break(err),
        }) {
            return Err(err);
        }
    }
    if let Some(w) = &parts.selection {
        if contains_aggregate(w, functions) {
            return unsupported("aggregate functions are not allowed in WHERE; use HAVING");
        }
    }
    for g in &parts.group_by {
        if contains_aggregate(g, functions) {
            return unsupported("aggregate functions are not allowed in GROUP BY");
        }
    }
    Ok(())
}
