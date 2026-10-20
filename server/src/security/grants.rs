//! A grants query: which values of a secured table's column a subject may see, as one
//! restricted query over a permissions table, parsed once (with the dialect's parser) and
//! compiled into a [`Grant`] that's evaluated on each row as it replicates:
//!
//! ```sql
//! SELECT author_id FROM permissions WHERE viewer_id = {subject:String} AND role IN ('reader', 'admin')
//! ```
//!
//! One column, one table, no joins, sub-queries, GROUP BY, ORDER BY or LIMIT; the WHERE is
//! AND-ed conditions, exactly one of them `column = {subject:String}`, the others
//! `column = 'literal'` or `column IN ('a', …)`.

use chlive_engine::ast::{CmpOp, ColumnRef, Expr, Literal, Projection, Query, SelectExpr};
use chlive_engine::error::{render_at, Span};
use chlive_engine::parser::parse;

/// A compiled grants query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Grant {
    /// The permissions table (in Postgres' `public` schema).
    pub table: String,
    /// Its column holding the subject.
    pub subject: String,
    /// Its column holding the values the subject may see.
    pub value: String,
    /// Each other condition: a column and the values it must have one of.
    pub conds: Vec<(String, Vec<String>)>,
}

impl Grant {
    pub fn parse(sql: &str) -> Result<Self, String> {
        let q = parse(sql).map_err(|e| e.render(sql))?;
        compile(&q).map_err(|(message, span)| render_at(sql, span, &message))
    }

    /// Every column it reads.
    pub fn columns(&self) -> impl Iterator<Item = &str> {
        [self.subject.as_str(), self.value.as_str()]
            .into_iter()
            .chain(self.conds.iter().map(|(c, _)| c.as_str()))
    }

    /// The (subject, value) a row grants, if it passes the conditions; `cell` gives a column's
    /// value as text (none: NULL).
    pub fn eval<'a>(&self, cell: impl Fn(&str) -> Option<&'a str>) -> Option<(String, String)> {
        for (column, values) in &self.conds {
            let v = cell(column)?;
            if !values.iter().any(|x| x == v) {
                return None;
            }
        }
        Some((
            cell(&self.subject)?.to_string(),
            cell(&self.value)?.to_string(),
        ))
    }
}

type Error = (String, Span);

fn compile(q: &Query) -> Result<Grant, Error> {
    let whole = q.from.span;
    if let Some((_, span)) = &q.group_by {
        return Err(("GROUP BY isn't allowed in grants".into(), *span));
    }
    if q.order.is_some() {
        return Err(("ORDER BY isn't allowed in grants".into(), whole));
    }
    if let Some(l) = q.limit.or(q.offset) {
        return Err(("LIMIT and OFFSET aren't allowed in grants".into(), l.span));
    }
    if let Some(span) = q.from.final_span {
        return Err(("FINAL isn't allowed in grants".into(), span));
    }
    let value = match &q.projection {
        Projection::Columns(cols) if cols.len() == 1 => &cols[0],
        Projection::Items(items) if items.len() == 1 && items[0].alias.is_none() => {
            match &items[0].expr {
                SelectExpr::Column(c) => c,
                SelectExpr::Call { span, .. } | SelectExpr::Literal(_, span) => {
                    return Err(("grants select one column".into(), *span))
                }
            }
        }
        _ => return Err(("grants select exactly one column".into(), whole)),
    };
    let mut conjuncts = Vec::new();
    match &q.selection {
        Some(e) => split(e, &mut conjuncts),
        None => {
            return Err((
                "grants need a WHERE with `column = {subject:String}`".into(),
                whole,
            ))
        }
    }
    let mut subject: Option<String> = None;
    let mut conds = Vec::new();
    for c in conjuncts {
        match c {
            Expr::Compare {
                op: CmpOp::Eq,
                left,
                right,
                span,
            } => {
                let (col, lit) = match (left.as_ref(), right.as_ref()) {
                    (Expr::Column(c), Expr::Literal { value, .. })
                    | (Expr::Literal { value, .. }, Expr::Column(c)) => (c, value),
                    _ => return Err(("compare a column with a value".into(), *span)),
                };
                let col = name(col, q)?;
                match lit {
                    Literal::Param { name, ty } if name == "subject" && ty == "String" => {
                        if subject.is_some() {
                            return Err(("{subject:String} appears twice".into(), *span));
                        }
                        subject = Some(col);
                    }
                    Literal::Param { .. } => {
                        return Err((
                            "the only parameter grants have is {subject:String}".into(),
                            *span,
                        ))
                    }
                    lit => conds.push((col, vec![text(lit, *span)?])),
                }
            }
            Expr::InList {
                expr,
                list,
                negated: false,
                span,
            } => {
                let Expr::Column(col) = expr.as_ref() else {
                    return Err(("IN needs a column on its left".into(), *span));
                };
                let values = list
                    .iter()
                    .map(|(lit, span)| text(lit, *span))
                    .collect::<Result<_, _>>()?;
                conds.push((name(col, q)?, values));
            }
            e => {
                return Err((
                    "grants' conditions are `column = {subject:String}`, `column = 'value'` \
                     and `column IN ('a', …)`, AND-ed"
                        .into(),
                    e.span(),
                ))
            }
        }
    }
    let Some(subject) = subject else {
        return Err((
            "grants need a `column = {subject:String}` condition".into(),
            whole,
        ));
    };
    Ok(Grant {
        table: q.from.name.clone(),
        subject,
        value: name(value, q)?,
        conds,
    })
}

fn split<'a>(e: &'a Expr, out: &mut Vec<&'a Expr>) {
    match e {
        Expr::And(l, r) => {
            split(l, out);
            split(r, out);
        }
        e => out.push(e),
    }
}

/// A column of the grants' table (qualified by its name or alias, or not at all).
fn name(c: &ColumnRef, q: &Query) -> Result<String, Error> {
    match &c.qualifier {
        Some(t) if *t != q.from.name && Some(t) != q.from.alias.as_ref() => {
            Err((format!("{t} isn't the grants' table"), c.span))
        }
        _ => Ok(c.name.clone()),
    }
}

/// A literal, as the text Postgres' value is compared with.
fn text(lit: &Literal, span: Span) -> Result<String, Error> {
    match lit {
        Literal::String(s) | Literal::Number(s) => Ok(s.clone()),
        Literal::Bool(b) => Ok(b.to_string()),
        _ => Err(("a string or number".into(), span)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn grants_compile() {
        let g = Grant::parse(
            "SELECT author_id FROM permissions p WHERE p.viewer_id = {subject:String} \
             AND role IN ('reader', 'admin') AND 'yes' = active",
        )
        .unwrap();
        assert_eq!(
            g,
            Grant {
                table: "permissions".into(),
                subject: "viewer_id".into(),
                value: "author_id".into(),
                conds: vec![
                    ("role".into(), vec!["reader".into(), "admin".into()]),
                    ("active".into(), vec!["yes".into()]),
                ],
            }
        );
        let row = |role: &'static str| {
            move |c: &str| match c {
                "viewer_id" => Some("alice"),
                "author_id" => Some("bob"),
                "role" => Some(role),
                "active" => Some("yes"),
                _ => None,
            }
        };
        assert_eq!(g.eval(row("admin")), Some(("alice".into(), "bob".into())));
        assert_eq!(g.eval(row("guest")), None);
    }

    #[test]
    fn other_shapes_are_refused() {
        for (sql, error) in [
            (
                "select a, b from t where s = {subject:String}",
                "exactly one column",
            ),
            (
                "select count() from t where s = {subject:String}",
                "one column",
            ),
            ("select a from t", "need a WHERE"),
            (
                "select a from t where b = 'x'",
                "need a `column = {subject:String}`",
            ),
            (
                "select a from t where s = {subject:String} or b = 'x'",
                "AND-ed",
            ),
            ("select a from t where s = {who:String}", "only parameter"),
            (
                "select a from t where s = {subject:String} and r = {subject:String}",
                "twice",
            ),
            (
                "select a from t where s = {subject:String} limit 3",
                "LIMIT",
            ),
            (
                "select a from t where s = {subject:String} order by a",
                "ORDER BY",
            ),
            (
                "select a from t where s = {subject:String} and a in (select b from u)",
                "AND-ed",
            ),
            (
                "select a from t where u.s = {subject:String}",
                "isn't the grants' table",
            ),
        ] {
            let e = Grant::parse(sql).unwrap_err();
            assert!(e.contains(error), "{sql}: {e}");
            assert!(e.contains(" at 1:"), "{sql}: {e}");
        }
    }
}
