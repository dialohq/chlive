//! From a parsed query to what to run: the ClickHouse query for history, and which topics to
//! consume and how to turn their events into result rows.

use crate::ast::{Projection, Query};
use crate::diff::Row;
use crate::rules::{quote_ident, NamingRules, TopicId};
use crate::schema::{Column, TableSchema};
use serde_json::Value;

#[derive(Debug, Clone, PartialEq)]
pub struct Plan {
    pub table: String,
    pub columns: Vec<Column>,
    pub history: HistoryPlan,
    pub stream: StreamPlan,
}

#[derive(Debug, Clone, PartialEq)]
pub struct HistoryPlan {
    pub sql: String,
}

#[derive(Debug, Clone, PartialEq)]
pub struct StreamPlan {
    /// The only topics the query needs; nothing else is consumed.
    pub topics: Vec<TopicId>,
    /// Which events are relevant to the query.
    pub filter: Filter,
    /// Output column ← JSON key, in output order.
    pub projection: Vec<(String, String)>,
}

/// Decides whether an event is relevant to the query (the future WHERE clause).
#[derive(Debug, Clone, PartialEq)]
pub enum Filter {
    /// Every event of the subscribed topics.
    All,
}

impl Filter {
    pub fn matches(&self, _event: &Row) -> bool {
        match self {
            Filter::All => true,
        }
    }
}

impl StreamPlan {
    /// The result row for an event. Keys missing from the event become `null`.
    pub fn project(&self, event: &Row) -> Row {
        self.projection
            .iter()
            .map(|(out, key)| (out.clone(), event.get(key).cloned().unwrap_or(Value::Null)))
            .collect()
    }
}

pub fn plan(query: &Query, rules: &dyn NamingRules, schema: &TableSchema) -> Plan {
    let columns = match query.projection {
        Projection::Wildcard => schema.columns.clone(),
    };
    let select_list: Vec<String> = columns
        .iter()
        .map(|c| {
            if c.clickhouse == c.name {
                quote_ident(&c.clickhouse)
            } else {
                format!("{} AS {}", quote_ident(&c.clickhouse), quote_ident(&c.name))
            }
        })
        .collect();
    let history = HistoryPlan {
        sql: format!(
            "SELECT {} FROM {}",
            select_list.join(", "),
            schema.clickhouse_table
        ),
    };
    let stream = StreamPlan {
        topics: vec![rules.topic(&query.from.name)],
        filter: Filter::All,
        projection: columns
            .iter()
            .map(|c| (c.name.clone(), c.json_key.clone()))
            .collect(),
    };
    Plan {
        table: query.from.name.clone(),
        columns,
        history,
        stream,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::parse;
    use crate::rules::HardcodedRules;
    use serde_json::json;

    fn schema() -> TableSchema {
        let col = |n: &str, ty: &str| Column {
            name: n.into(),
            clickhouse: n.into(),
            json_key: n.into(),
            ty: ty.into(),
        };
        TableSchema {
            table: "events".into(),
            clickhouse_table: "default.events".into(),
            columns: vec![
                col("ts", "DateTime64(3)"),
                col("user_id", "String"),
                col("latency_ms", "UInt32"),
            ],
        }
    }

    #[test]
    fn select_star_plan() {
        let p = plan(
            &parse("select * from events").unwrap(),
            &HardcodedRules::default(),
            &schema(),
        );
        assert_eq!(
            p.history.sql,
            "SELECT ts, user_id, latency_ms FROM default.events"
        );
        assert_eq!(p.stream.topics, [TopicId("events".into())]);
        assert_eq!(p.stream.filter, Filter::All);
    }

    #[test]
    fn projection_orders_columns_and_fills_missing_keys() {
        let p = plan(
            &parse("select * from events").unwrap(),
            &HardcodedRules::default(),
            &schema(),
        );
        let event = json!({"latency_ms": 5, "extra": true, "ts": "2026-10-01 12:00:00.000"});
        let row = p.stream.project(event.as_object().unwrap());
        assert_eq!(
            serde_json::to_string(&row).unwrap(),
            r#"{"ts":"2026-10-01 12:00:00.000","user_id":null,"latency_ms":5}"#
        );
    }
}
