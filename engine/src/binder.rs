//! Resolves a parsed query against the catalog, clause by clause in SQL's logical evaluation
//! order: FROM, then SELECT. Everything after binding works with resolved identifiers, never
//! with the names as written.

use std::fmt;

use crate::ast::{Projection, Query};
use crate::error::{render_at, Span};
use crate::history::{HistoryError, HistorySource};
use crate::rules::{NamingRules, TopicId};
use crate::stream::TopicCatalog;

/// What the binder can look things up in.
pub struct Catalog<'a> {
    pub rules: &'a dyn NamingRules,
    pub history: &'a dyn HistorySource,
    pub topics: &'a dyn TopicCatalog,
}

/// Index of a column in [`BoundTable::columns`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ColumnId(pub usize);

/// A column of a dialect table, with where it lives on each side.
#[derive(Debug, Clone, PartialEq)]
pub struct BoundColumn {
    pub id: ColumnId,
    /// Name in the dialect and in output rows.
    pub name: String,
    /// Column in ClickHouse.
    pub clickhouse: String,
    /// Key in the topic's JSON objects.
    pub json_key: String,
    /// ClickHouse type, informational for now.
    pub ty: String,
}

/// The FROM table: its ClickHouse table, its topic, and its (visible) columns.
#[derive(Debug, Clone, PartialEq)]
pub struct BoundTable {
    pub name: String,
    pub clickhouse_table: String,
    pub topic: TopicId,
    pub columns: Vec<BoundColumn>,
}

impl BoundTable {
    pub fn column(&self, id: ColumnId) -> &BoundColumn {
        &self.columns[id.0]
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct BoundQuery {
    pub table: BoundTable,
    /// Output columns, in output order.
    pub projection: Vec<ColumnId>,
}

impl BoundQuery {
    pub fn output(&self) -> impl Iterator<Item = &BoundColumn> {
        self.projection.iter().map(|id| self.table.column(*id))
    }
}

#[derive(Debug)]
pub enum BindError {
    /// A problem with the query itself, at a position in it.
    At { message: String, span: Span },
    /// The catalog could not be consulted (ClickHouse unreachable, …).
    History(HistoryError),
}

impl BindError {
    fn at(message: impl Into<String>, span: Span) -> Self {
        Self::At {
            message: message.into(),
            span,
        }
    }

    /// With line and column for query errors.
    pub fn render(&self, src: &str) -> String {
        match self {
            BindError::At { message, span } => render_at(src, *span, message),
            BindError::History(e) => e.to_string(),
        }
    }
}

impl fmt::Display for BindError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BindError::At { message, span } => write!(f, "{message} at byte {}", span.start),
            BindError::History(e) => write!(f, "{e}"),
        }
    }
}

impl std::error::Error for BindError {}

pub async fn bind(query: &Query, catalog: &Catalog<'_>) -> Result<BoundQuery, BindError> {
    let table = bind_from(query, catalog).await?;
    let projection = bind_select(query, &table)?;
    Ok(BoundQuery { table, projection })
}

/// FROM: the table must exist in ClickHouse and have an event stream.
async fn bind_from(query: &Query, catalog: &Catalog<'_>) -> Result<BoundTable, BindError> {
    let from = &query.from;
    let clickhouse_table = catalog.rules.clickhouse_table(&from.name);
    let described = match catalog.history.describe(&clickhouse_table).await {
        Ok(cols) => cols,
        Err(HistoryError::UnknownTable(_)) => {
            return Err(BindError::at(
                format!(
                    "unknown table {} in ClickHouse ({clickhouse_table})",
                    from.name
                ),
                from.span,
            ))
        }
        Err(e) => return Err(BindError::History(e)),
    };
    let topic = catalog.rules.topic(&from.name);
    if !catalog.topics.exists(&topic) {
        return Err(BindError::at(
            format!("no event stream for table {} (topic {topic})", from.name),
            from.span,
        ));
    }
    let columns = described
        .into_iter()
        .filter(|c| !catalog.rules.is_hidden(&c.name))
        .enumerate()
        .map(|(i, c)| {
            let m = catalog.rules.column(&c.name);
            BoundColumn {
                id: ColumnId(i),
                name: m.name,
                clickhouse: c.name,
                json_key: m.json_key,
                ty: c.ty,
            }
        })
        .collect();
    Ok(BoundTable {
        name: from.name.clone(),
        clickhouse_table,
        topic,
        columns,
    })
}

/// SELECT: `*` is every column of the table; a list must name existing columns, once each.
fn bind_select(query: &Query, table: &BoundTable) -> Result<Vec<ColumnId>, BindError> {
    match &query.projection {
        Projection::Wildcard => Ok(table.columns.iter().map(|c| c.id).collect()),
        Projection::Columns(refs) => {
            let mut ids: Vec<ColumnId> = Vec::with_capacity(refs.len());
            for r in refs {
                let Some(col) = table.columns.iter().find(|c| c.name == r.name) else {
                    let names: Vec<&str> = table.columns.iter().map(|c| c.name.as_str()).collect();
                    return Err(BindError::at(
                        format!(
                            "unknown column {} in table {} (columns: {})",
                            r.name,
                            table.name,
                            names.join(", ")
                        ),
                        r.span,
                    ));
                };
                if ids.contains(&col.id) {
                    return Err(BindError::at(
                        format!("column {} is selected twice", r.name),
                        r.span,
                    ));
                }
                ids.push(col.id);
            }
            Ok(ids)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::diff::Row;
    use crate::history::DescribedColumn;
    use crate::parser::parse;
    use crate::rules::HardcodedRules;
    use async_trait::async_trait;

    struct FakeHistory;

    #[async_trait]
    impl HistorySource for FakeHistory {
        async fn describe(&self, table: &str) -> Result<Vec<DescribedColumn>, HistoryError> {
            let col = |n: &str, ty: &str| DescribedColumn {
                name: n.into(),
                ty: ty.into(),
            };
            match table {
                "default.events" => Ok(vec![
                    col("ts", "DateTime64(3)"),
                    col("user_id", "String"),
                    col("latency_ms", "UInt32"),
                    col("_partition", "UInt64"),
                    col("_offset", "UInt64"),
                ]),
                "default.orders" => Ok(vec![col("order_id", "String")]),
                _ => Err(HistoryError::UnknownTable(table.into())),
            }
        }
        async fn fetch(&self, _sql: &str) -> Result<Vec<Row>, HistoryError> {
            unreachable!("the binder never fetches")
        }
    }

    /// Only `events` has a stream.
    struct FakeTopics;

    impl TopicCatalog for FakeTopics {
        fn exists(&self, topic: &TopicId) -> bool {
            topic.0 == "events"
        }
    }

    async fn bind_sql(sql: &str) -> Result<BoundQuery, String> {
        let rules = HardcodedRules::default();
        let catalog = Catalog {
            rules: &rules,
            history: &FakeHistory,
            topics: &FakeTopics,
        };
        let q = parse(sql).map_err(|e| e.render(sql))?;
        bind(&q, &catalog).await.map_err(|e| e.render(sql))
    }

    fn names(b: &BoundQuery) -> Vec<&str> {
        b.output().map(|c| c.name.as_str()).collect()
    }

    #[tokio::test]
    async fn from_resolves_clickhouse_table_topic_and_visible_columns() {
        let b = bind_sql("select * from events").await.unwrap();
        assert_eq!(b.table.clickhouse_table, "default.events");
        assert_eq!(b.table.topic, TopicId("events".into()));
        assert_eq!(names(&b), ["ts", "user_id", "latency_ms"]);
        assert_eq!(b.table.column(ColumnId(2)).ty, "UInt32");
    }

    #[tokio::test]
    async fn select_list_keeps_written_order() {
        let b = bind_sql("select latency_ms, ts from events").await.unwrap();
        assert_eq!(names(&b), ["latency_ms", "ts"]);
        assert_eq!(b.projection, [ColumnId(2), ColumnId(0)]);
    }

    #[tokio::test]
    async fn errors_point_at_the_offending_name() {
        assert_eq!(
            bind_sql("select * from nope").await.unwrap_err(),
            "unknown table nope in ClickHouse (default.nope) at 1:15"
        );
        assert_eq!(
            bind_sql("select * from orders").await.unwrap_err(),
            "no event stream for table orders (topic orders) at 1:15"
        );
        assert_eq!(
            bind_sql("select ts, nope from events").await.unwrap_err(),
            "unknown column nope in table events (columns: ts, user_id, latency_ms) at 1:12"
        );
        // hidden bookkeeping columns are not part of the dialect table
        assert!(bind_sql("select _offset from events")
            .await
            .unwrap_err()
            .starts_with("unknown column _offset"));
        assert_eq!(
            bind_sql("select ts, user_id, ts from events")
                .await
                .unwrap_err(),
            "column ts is selected twice at 1:21"
        );
        // column names are case-sensitive, like ClickHouse's
        assert!(bind_sql("select TS from events")
            .await
            .unwrap_err()
            .starts_with("unknown column TS"));
    }
}
