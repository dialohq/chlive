//! Rows plan: `topN(history ∪ tail) == topN(topN(history) ∪ tail)`.

use sqlparser::ast::{Expr, SelectItem};

use super::aggregate::{check_unique_names, reject_positional};
use super::validate::Parts;
use super::{ident_expr, params, quote_ident, Catalog, ParamTypes, Plan, PlanKind, RewriteError};

pub fn plan(parts: Parts, catalog: &Catalog, types: &ParamTypes) -> Result<Plan, RewriteError> {
    let cap = catalog.rows_limit_cap;
    let limit = match parts.limit {
        Some(n) if !parts.order_by.is_empty() => n,
        _ => {
            return Err(RewriteError::unsupported(format!(
                "queries without aggregates must have ORDER BY and LIMIT n (n ≤ {cap})"
            )))
        }
    };
    if limit > cap {
        return Err(RewriteError::unsupported(format!("LIMIT {limit} exceeds the maximum of {cap}")));
    }
    if parts.having.is_some() {
        return Err(RewriteError::unsupported("HAVING requires GROUP BY or aggregates"));
    }
    let schema = &catalog.tables[&parts.table];

    let mut items: Vec<(Expr, String)> = Vec::new();
    for item in parts.projection {
        match item {
            SelectItem::Wildcard(_) => {
                items.extend(schema.columns.iter().map(|(c, _)| (ident_expr(c), c.clone())))
            }
            SelectItem::UnnamedExpr(e) => {
                let name = params::restore(&e.to_string(), types);
                items.push((e, name));
            }
            SelectItem::ExprWithAlias { expr, alias } => items.push((expr, alias.value)),
            _ => return Err(RewriteError::unsupported("unsupported select item")),
        }
    }
    check_unique_names(items.iter().map(|(_, n)| n.as_str()))?;

    let mut select: Vec<String> = items
        .iter()
        .map(|(e, name)| {
            let e = e.to_string();
            let q = quote_ident(name);
            if e == q { e } else { format!("{e} AS {q}") }
        })
        .collect();
    let mut order = Vec::new();
    for (i, o) in parts.order_by.iter().enumerate() {
        reject_positional(&o.expr, "ORDER BY")?;
        select.push(format!("{} AS __ord_{i}", o.expr));
        let mut o = o.clone();
        o.expr = ident_expr(&format!("__ord_{i}"));
        order.push(o.to_string());
    }
    select.push("_partition AS __partition".into());
    select.push("_offset AS __offset".into());
    // Tie-break on the Kafka position so the result (and thus the diff) is deterministic.
    order.push("__partition".into());
    order.push("__offset".into());
    let suffix = format!(" ORDER BY {} LIMIT {limit}", order.join(", "));

    Ok(Plan {
        kind: PlanKind::Rows,
        table: parts.table.clone(),
        key_columns: super::ROW_KEY_COLUMNS.iter().map(|s| s.to_string()).collect(),
        output_columns: items.into_iter().map(|(_, n)| n).collect(),
        has_order: true,
        result_cap: None,
        param_types: ParamTypes::new(),
        normalized_sql: String::new(),
        select_list: select.join(", "),
        filter: parts.selection.map(|w| w.to_string()),
        inner_suffix: suffix.clone(),
        merge_select: "*".into(),
        merge_suffix: suffix,
        source: catalog.source_for(&parts.table),
        tail_table: format!("tail_{}", parts.table),
    })
}
