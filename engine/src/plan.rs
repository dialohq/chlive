//! From a bound query to what to run: the ClickHouse query for history, and which topics to
//! consume, which keys of their events to decode, and how to turn events into result rows.

use crate::binder::{BoundColumn, BoundQuery};
use crate::diff::Row;
use crate::rules::{quote_ident, TopicId};
use serde_json::Value;

#[derive(Debug, Clone, PartialEq)]
pub struct Plan {
    pub table: String,
    /// Output columns, in output order.
    pub columns: Vec<BoundColumn>,
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

    /// JSON keys the filter reads.
    pub fn keys(&self) -> Vec<String> {
        match self {
            Filter::All => Vec::new(),
        }
    }
}

impl StreamPlan {
    /// The JSON keys the stream must decode: the projection's and the filter's. Every other
    /// key of an event is skipped while decoding.
    pub fn required_keys(&self) -> Vec<String> {
        let mut keys: Vec<String> = Vec::new();
        for key in self
            .projection
            .iter()
            .map(|(_, k)| k.clone())
            .chain(self.filter.keys())
        {
            if !keys.contains(&key) {
                keys.push(key);
            }
        }
        keys
    }

    /// The result row for an event. Keys missing from the event become `null`.
    pub fn project(&self, event: &Row) -> Row {
        self.projection
            .iter()
            .map(|(out, key)| (out.clone(), event.get(key).cloned().unwrap_or(Value::Null)))
            .collect()
    }
}

/// What `--explain` shows: table, columns, the history query, and the stream side.
impl std::fmt::Display for Plan {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let columns: Vec<&str> = self.columns.iter().map(|c| c.name.as_str()).collect();
        let topics: Vec<&str> = self.stream.topics.iter().map(|t| t.0.as_str()).collect();
        writeln!(f, "table:   {}", self.table)?;
        writeln!(f, "columns: {}", columns.join(", "))?;
        writeln!(f, "history: {}", self.history.sql)?;
        writeln!(f, "topics:  {}", topics.join(", "))?;
        writeln!(f, "keys:    {}", self.stream.required_keys().join(", "))?;
        write!(f, "filter:  {:?}", self.stream.filter)
    }
}

pub fn plan(bound: &BoundQuery) -> Plan {
    let columns: Vec<BoundColumn> = bound.output().cloned().collect();
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
            bound.table.clickhouse_table
        ),
    };
    let stream = StreamPlan {
        topics: vec![bound.table.topic.clone()],
        filter: Filter::All,
        projection: columns
            .iter()
            .map(|c| (c.name.clone(), c.json_key.clone()))
            .collect(),
    };
    Plan {
        table: bound.table.name.clone(),
        columns,
        history,
        stream,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::binder::{BoundTable, ColumnId};
    use serde_json::json;

    fn bound(projection: &[usize]) -> BoundQuery {
        let col = |i: usize, n: &str, ty: &str| BoundColumn {
            id: ColumnId(i),
            name: n.into(),
            clickhouse: n.into(),
            json_key: n.into(),
            ty: ty.into(),
        };
        BoundQuery {
            table: BoundTable {
                name: "events".into(),
                clickhouse_table: "default.events".into(),
                topic: TopicId("events".into()),
                columns: vec![
                    col(0, "ts", "DateTime64(3)"),
                    col(1, "user_id", "String"),
                    col(2, "latency_ms", "UInt32"),
                ],
            },
            projection: projection.iter().map(|i| ColumnId(*i)).collect(),
        }
    }

    #[test]
    fn select_star_plan() {
        let p = plan(&bound(&[0, 1, 2]));
        assert_eq!(
            p.history.sql,
            "SELECT ts, user_id, latency_ms FROM default.events"
        );
        assert_eq!(p.stream.topics, [TopicId("events".into())]);
        assert_eq!(p.stream.required_keys(), ["ts", "user_id", "latency_ms"]);
        assert_eq!(p.stream.filter, Filter::All);
    }

    #[test]
    fn column_list_prunes_history_and_stream() {
        let p = plan(&bound(&[2, 0]));
        assert_eq!(p.history.sql, "SELECT latency_ms, ts FROM default.events");
        assert_eq!(p.stream.required_keys(), ["latency_ms", "ts"]);
        let event = json!({"ts": "t", "user_id": "u", "latency_ms": 5});
        let row = p.stream.project(event.as_object().unwrap());
        assert_eq!(
            serde_json::to_string(&row).unwrap(),
            r#"{"latency_ms":5,"ts":"t"}"#
        );
    }

    #[test]
    fn missing_keys_become_null() {
        let p = plan(&bound(&[0, 1]));
        let row = p.stream.project(json!({"ts": "t"}).as_object().unwrap());
        assert_eq!(
            serde_json::to_string(&row).unwrap(),
            r#"{"ts":"t","user_id":null}"#
        );
    }
}
