//! Aggregate plan: split every aggregate into `fState` (history + tail) and `fMerge` (merge).

use std::collections::{HashMap, HashSet};
use std::ops::ControlFlow;

use sqlparser::ast::{
    Expr, Function, FunctionArg, FunctionArgExpr, FunctionArgumentList, FunctionArguments, Ident,
    ObjectName, ObjectNamePart, SelectItem, VisitMut, VisitorMut,
};

use super::validate::{contains_aggregate, func_name, Parts};
use super::{
    ident_expr, params, quote_ident, Catalog, Options, ParamTypes, Plan, PlanKind, RewriteError,
};

struct Item {
    expr: Expr,
    alias: Option<String>,
    /// Output column name: the alias, or the original expression text.
    name: String,
}

struct Key {
    expr: Expr,
    alias: String,
}

pub fn plan(
    parts: Parts,
    catalog: &Catalog,
    opts: &Options,
    types: &ParamTypes,
) -> Result<Plan, RewriteError> {
    let schema = &catalog.tables[&parts.table];
    let columns: HashSet<&str> = schema.columns.iter().map(|(n, _)| n.as_str()).collect();
    let functions = &catalog.functions;

    let mut items = Vec::new();
    for item in parts.projection {
        match item {
            SelectItem::UnnamedExpr(e) => {
                let name = params::restore(&e.to_string(), types);
                items.push(Item {
                    expr: e,
                    alias: None,
                    name,
                });
            }
            SelectItem::ExprWithAlias { expr, alias } => items.push(Item {
                expr,
                name: alias.value.clone(),
                alias: Some(alias.value),
            }),
            _ => {
                return Err(RewriteError::unsupported(
                    "SELECT * cannot be combined with aggregates or GROUP BY",
                ))
            }
        }
    }
    check_unique_names(items.iter().map(|i| i.name.as_str()))?;

    // Aliases whose expression (transitively) contains an aggregate.
    let mut agg_aliases: HashMap<String, Expr> = HashMap::new();
    loop {
        let before = agg_aliases.len();
        for it in &items {
            let Some(alias) = &it.alias else { continue };
            if agg_aliases.contains_key(alias) {
                continue;
            }
            if contains_aggregate(&it.expr, functions)
                || references_any(&it.expr, &agg_aliases, alias)
            {
                if columns.contains(alias.as_str()) {
                    return Err(RewriteError::unsupported(format!(
                        "alias '{alias}' of an aggregate expression shadows a column of '{}'; pick another name",
                        parts.table
                    )));
                }
                agg_aliases.insert(alias.clone(), it.expr.clone());
            }
        }
        if agg_aliases.len() == before {
            break;
        }
    }

    let mut having = parts.having;
    let mut order_by = parts.order_by;
    if opts.expand_aliases {
        for it in &mut items {
            expand_aliases(&mut it.expr, &agg_aliases, it.alias.as_deref())?;
        }
        if let Some(h) = &mut having {
            expand_aliases(h, &agg_aliases, None)?;
        }
        for o in &mut order_by {
            expand_aliases(&mut o.expr, &agg_aliases, None)?;
        }
    }

    // Group keys.
    let mut keys: Vec<Key> = Vec::new();
    for (i, g) in parts.group_by.iter().enumerate() {
        reject_positional(g, "GROUP BY")?;
        let by_alias = match g {
            Expr::Identifier(id) => items
                .iter()
                .find(|it| it.alias.as_deref() == Some(id.value.as_str())),
            _ => None,
        };
        let key = if let Some(it) = by_alias.filter(|it| !agg_aliases.contains_key(&it.name)) {
            Key {
                expr: it.expr.clone(),
                alias: it.name.clone(),
            }
        } else if by_alias.is_some() {
            return Err(RewriteError::unsupported(
                "GROUP BY cannot reference an aggregate",
            ));
        } else if let Expr::Identifier(id) = g {
            if !columns.contains(id.value.as_str()) && !id.value.starts_with(params::PREFIX) {
                return Err(RewriteError::unsupported(format!(
                    "unknown GROUP BY column '{}'",
                    id.value
                )));
            }
            Key {
                expr: g.clone(),
                alias: id.value.clone(),
            }
        } else {
            let text = g.to_string();
            let aliased = items
                .iter()
                .find(|it| it.alias.is_some() && it.expr.to_string() == text);
            match aliased {
                Some(it) => Key {
                    expr: g.clone(),
                    alias: it.name.clone(),
                },
                None => Key {
                    expr: g.clone(),
                    alias: format!("__key_{i}"),
                },
            }
        };
        if columns.contains(key.alias.as_str())
            && !matches!(&key.expr, Expr::Identifier(id) if id.value == key.alias)
        {
            return Err(RewriteError::unsupported(format!(
                "alias '{}' of a GROUP BY expression shadows a column; pick another name",
                key.alias
            )));
        }
        if !keys.iter().any(|k| k.alias == key.alias) {
            keys.push(key);
        }
    }

    // Rewrite merge-side expressions: aggregates → fMerge(__agg_i), key exprs → key alias.
    let mut rw = SlotRewriter {
        keys: keys
            .iter()
            .filter(|k| !matches!(&k.expr, Expr::Identifier(id) if id.value == k.alias))
            .map(|k| (k.expr.to_string(), k.alias.clone()))
            .collect(),
        functions,
        slots: Vec::new(),
        slot_index: HashMap::new(),
        err: None,
    };
    for it in &mut items {
        rw.rewrite(&mut it.expr)?;
    }
    if let Some(h) = &mut having {
        rw.rewrite(h)?;
    }
    for o in &mut order_by {
        reject_positional(&o.expr, "ORDER BY")?;
        rw.rewrite(&mut o.expr)?;
    }

    // Every output column that names a key must actually be that key.
    let output_names: HashSet<&str> = items.iter().map(|i| i.name.as_str()).collect();
    for k in &keys {
        if let Some(it) = items.iter().find(|it| it.name == k.alias) {
            if !matches!(&it.expr, Expr::Identifier(id) if id.value == k.alias) {
                return Err(RewriteError::unsupported(format!(
                    "output column '{}' conflicts with the GROUP BY key of the same name",
                    k.alias
                )));
            }
        }
    }

    let mut select_list: Vec<String> = keys
        .iter()
        .map(|k| match &k.expr {
            Expr::Identifier(id) if id.value == k.alias => quote_ident(&k.alias),
            e => format!("{e} AS {}", quote_ident(&k.alias)),
        })
        .collect();
    select_list.extend(
        rw.slots
            .iter()
            .enumerate()
            .map(|(i, s)| format!("{} AS __agg_{i}", s.state)),
    );

    let key_aliases: Vec<String> = keys.iter().map(|k| quote_ident(&k.alias)).collect();
    let inner_suffix = if keys.is_empty() {
        String::new()
    } else {
        format!(" GROUP BY {}", key_aliases.join(", "))
    };

    let mut merge_select: Vec<String> = items
        .iter()
        .map(|it| {
            let e = it.expr.to_string();
            let q = quote_ident(&it.name);
            if e == q {
                e
            } else {
                format!("{e} AS {q}")
            }
        })
        .collect();
    merge_select.extend(
        keys.iter()
            .filter(|k| !output_names.contains(k.alias.as_str()))
            .map(|k| quote_ident(&k.alias)),
    );

    let mut merge_suffix = inner_suffix.clone();
    if let Some(h) = &having {
        merge_suffix.push_str(&format!(" HAVING {h}"));
    }
    if !order_by.is_empty() {
        let o: Vec<String> = order_by.iter().map(|o| o.to_string()).collect();
        merge_suffix.push_str(&format!(" ORDER BY {}", o.join(", ")));
    }
    let result_cap = match parts.limit {
        Some(n) if n > catalog.rows_limit_cap => {
            return Err(RewriteError::unsupported(format!(
                "LIMIT {n} exceeds the maximum of {}",
                catalog.rows_limit_cap
            )))
        }
        Some(n) => {
            merge_suffix.push_str(&format!(" LIMIT {n}"));
            None
        }
        None => {
            merge_suffix.push_str(&format!(" LIMIT {}", catalog.rows_limit_cap + 1));
            Some(catalog.rows_limit_cap)
        }
    };

    Ok(Plan {
        kind: PlanKind::Aggregate,
        table: parts.table.clone(),
        key_columns: keys.iter().map(|k| k.alias.clone()).collect(),
        output_columns: items.iter().map(|i| i.name.clone()).collect(),
        has_order: !order_by.is_empty(),
        result_cap,
        param_types: ParamTypes::new(),
        normalized_sql: String::new(),
        select_list: select_list.join(", "),
        filter: parts.selection.map(|w| w.to_string()),
        inner_suffix,
        merge_select: merge_select.join(", "),
        merge_suffix,
        source: catalog.source_for(&parts.table),
        tail_table: format!("tail_{}", parts.table),
    })
}

pub(super) fn check_unique_names<'a>(
    names: impl Iterator<Item = &'a str>,
) -> Result<(), RewriteError> {
    let mut seen = HashSet::new();
    for n in names {
        if n.starts_with("__") {
            return Err(RewriteError::unsupported(format!(
                "column names starting with '__' are reserved: {n}"
            )));
        }
        if !seen.insert(n) {
            return Err(RewriteError::unsupported(format!(
                "duplicate output column '{n}'; add an alias"
            )));
        }
    }
    Ok(())
}

pub(super) fn reject_positional(e: &Expr, clause: &str) -> Result<(), RewriteError> {
    if matches!(e, Expr::Value(_)) {
        return Err(RewriteError::unsupported(format!(
            "positional or constant {clause} items are not supported; use column names or aliases"
        )));
    }
    Ok(())
}

fn references_any(e: &Expr, aliases: &HashMap<String, Expr>, own: &str) -> bool {
    sqlparser::ast::visit_expressions(e, |e| match e {
        Expr::Identifier(id) if id.value != own && aliases.contains_key(&id.value) => {
            ControlFlow::Break(())
        }
        _ => ControlFlow::Continue(()),
    })
    .is_break()
}

/// Replace references to aggregate aliases with the aliased expression.
fn expand_aliases(
    e: &mut Expr,
    aliases: &HashMap<String, Expr>,
    own: Option<&str>,
) -> Result<(), RewriteError> {
    struct Expander<'a> {
        aliases: &'a HashMap<String, Expr>,
        own: Option<&'a str>,
        steps: usize,
    }
    impl VisitorMut for Expander<'_> {
        type Break = RewriteError;
        fn pre_visit_expr(&mut self, e: &mut Expr) -> ControlFlow<RewriteError> {
            if let Expr::Identifier(id) = e {
                if Some(id.value.as_str()) != self.own {
                    if let Some(target) = self.aliases.get(&id.value) {
                        self.steps += 1;
                        if self.steps > 256 {
                            return ControlFlow::Break(RewriteError::unsupported(
                                "cyclic alias references",
                            ));
                        }
                        *e = Expr::Nested(Box::new(target.clone()));
                    }
                }
            }
            ControlFlow::Continue(())
        }
    }
    match e.visit(&mut Expander {
        aliases,
        own,
        steps: 0,
    }) {
        ControlFlow::Break(err) => Err(err),
        ControlFlow::Continue(()) => Ok(()),
    }
}

struct Slot {
    state: String,
}

struct SlotRewriter<'a> {
    keys: HashMap<String, String>,
    functions: &'a super::FunctionCatalog,
    slots: Vec<Slot>,
    slot_index: HashMap<String, usize>,
    err: Option<RewriteError>,
}

impl SlotRewriter<'_> {
    fn rewrite(&mut self, e: &mut Expr) -> Result<(), RewriteError> {
        let _ = e.visit(self);
        match self.err.take() {
            Some(err) => Err(err),
            None => Ok(()),
        }
    }

    fn slot_for(&mut self, f: &Function) -> Result<Expr, RewriteError> {
        let name = func_name(f).unwrap_or_default();
        let canonical = self.functions.allowed(name).ok_or_else(|| {
            RewriteError::unsupported(format!(
                "aggregate function '{name}' is not supported (supported: count, sum, min, max, avg, any, \
                 uniq, uniqExact, argMin, argMax, quantile, quantiles and their -If forms)"
            ))
        })?;
        let mut args = f.args.clone();
        if let FunctionArguments::List(l) = &mut args {
            for a in &l.args {
                if let FunctionArg::Unnamed(FunctionArgExpr::Expr(e)) = a {
                    if contains_aggregate(e, self.functions) {
                        return Err(RewriteError::unsupported(
                            "nested aggregate functions are not supported",
                        ));
                    }
                }
            }
            // count(*) ≡ count()
            if canonical == "count"
                && matches!(
                    l.args.as_slice(),
                    [FunctionArg::Unnamed(FunctionArgExpr::Wildcard)]
                )
            {
                l.args.clear();
            }
        }
        let mut state = f.clone();
        state.name = fn_name(&format!("{canonical}State"));
        state.args = args;
        let mut normalized = state.clone();
        normalized.name = fn_name(&canonical);
        let signature = normalized.to_string();
        let idx = match self.slot_index.get(&signature) {
            Some(i) => *i,
            None => {
                self.slots.push(Slot {
                    state: state.to_string(),
                });
                self.slot_index.insert(signature, self.slots.len() - 1);
                self.slots.len() - 1
            }
        };
        let mut merge = f.clone();
        merge.name = fn_name(&format!("{canonical}Merge"));
        merge.args = FunctionArguments::List(FunctionArgumentList {
            duplicate_treatment: None,
            args: vec![FunctionArg::Unnamed(FunctionArgExpr::Expr(ident_expr(
                &format!("__agg_{idx}"),
            )))],
            clauses: vec![],
        });
        Ok(Expr::Function(merge))
    }
}

impl VisitorMut for SlotRewriter<'_> {
    type Break = ();
    fn pre_visit_expr(&mut self, e: &mut Expr) -> ControlFlow<()> {
        if let Some(alias) = self.keys.get(&e.to_string()) {
            *e = ident_expr(alias);
            return ControlFlow::Continue(());
        }
        if let Expr::Function(f) = e {
            if func_name(f).is_some_and(|n| self.functions.is_aggregate(n)) {
                match self.slot_for(f) {
                    Ok(m) => *e = m,
                    Err(err) => {
                        self.err = Some(err);
                        return ControlFlow::Break(());
                    }
                }
            }
        }
        ControlFlow::Continue(())
    }
}

fn fn_name(name: &str) -> ObjectName {
    ObjectName(vec![ObjectNamePart::Identifier(Ident::new(name))])
}
