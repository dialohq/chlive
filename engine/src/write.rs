//! Inserts: rows a client writes into a table, produced to its topic as JSON rows, which
//! ClickHouse's ingestion stores and every subscription reads like any other event.
//!
//! Only tables with `[tables.<name>.insert]` take them. Every row is checked before anything is
//! produced: each key is one of the table's columns (not hidden, not filled from Kafka), the
//! server's [`InsertFilter`] allows its columns and its row, and each value reads as its
//! column's type, strictly (no NULL in a non-Nullable column, no integer out of its range).
//! The message holds each value as ClickHouse renders it, under the column's JSON key, so what
//! ClickHouse stores and what the stream decodes are the same. Columns a row leaves out are
//! left out of the message (ClickHouse stores their DEFAULT; the stream reads them as NULL or
//! the type's zero, as for any message).
//!
//! There is only inserting: an update or a delete is an insert by the table's own convention
//! (a new version of a key in a ReplacingMergeTree, with its is-deleted column set).

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde_json::Value as Json;

use crate::diff::Row;
use crate::engine::{Engine, EngineError};
use crate::history::HistoryError;
use crate::rules::{ColumnSource, TopicId};
use crate::security::{InsertConfig, InsertFilter, RowFilter};
use crate::source::Outgoing;
use crate::types::{Kind, Type, Value};

/// How long a table's description is used for inserts before it's read again.
const DESCRIBED_FOR: Duration = Duration::from_secs(30);

/// What a table takes: its topic, and its columns by dialect name.
#[derive(Debug, Clone)]
pub struct WriteTable {
    pub topic: TopicId,
    pub columns: BTreeMap<String, WriteColumn>,
}

#[derive(Debug, Clone)]
pub struct WriteColumn {
    /// Where events hold it.
    pub source: ColumnSource,
    pub ty: Type,
}

/// Described tables, by dialect name, with when they were described.
pub(crate) type Described = std::sync::Mutex<BTreeMap<String, (Instant, Arc<WriteTable>)>>;

impl Engine {
    /// Insert rows into a table, as one user may (`filter`, decided by the server): each row's
    /// partition and offset once Kafka has it, in order. Nothing is produced unless every row
    /// passes. Not atomic beyond that: if producing fails partway, the error says which rows
    /// went in.
    pub async fn insert(
        &self,
        table: &str,
        rows: &[Row],
        filter: &InsertFilter,
    ) -> Result<Vec<(i32, i64)>, EngineError> {
        let Some(config) = self.rules.insert(table) else {
            return Err(EngineError::Denied(format!("{table} doesn't take inserts")));
        };
        if !config.public && filter.user.is_none() {
            return Err(EngineError::Denied(format!(
                "inserting into {table} needs a user (send `hello` first)"
            )));
        }
        let mut written = self.writable(table, false).await?;
        // a column it doesn't know may be new: described again, once
        let unknown = |w: &WriteTable| {
            rows.iter()
                .flat_map(|r| r.keys())
                .any(|k| !w.columns.contains_key(k))
        };
        if unknown(&written) {
            written = self.writable(table, true).await?;
        }
        let messages = rows
            .iter()
            .enumerate()
            .map(|(i, row)| {
                message(table, &written, &config, filter, row).map_err(|e| {
                    EngineError::Insert(match rows.len() {
                        1 => e,
                        _ => format!("row {i}: {e}"),
                    })
                })
            })
            .collect::<Result<Vec<_>, _>>()?;
        let Some(sink) = &self.sink else {
            return Err(EngineError::Write("no Kafka producer".into()));
        };
        let results = sink.produce(&written.topic, messages).await;
        let failed: Vec<usize> = (0..results.len())
            .filter(|i| results[*i].is_err())
            .collect();
        if let Some(first) = failed.first() {
            let error = results[*first].as_ref().unwrap_err();
            let went_in: Vec<usize> = (0..results.len()).filter(|i| results[*i].is_ok()).collect();
            return Err(EngineError::Write(match went_in.is_empty() {
                true => error.0.clone(),
                false => format!("{} (rows {went_in:?} went in, the others didn't)", error.0),
            }));
        }
        Ok(results.into_iter().map(|r| r.expect("checked")).collect())
    }

    /// A table's columns as inserts see them, described at most [`DESCRIBED_FOR`] ago (or now,
    /// with `fresh`).
    async fn writable(&self, table: &str, fresh: bool) -> Result<Arc<WriteTable>, EngineError> {
        if !fresh {
            let described = self.described.lock().unwrap();
            if let Some((at, w)) = described.get(table) {
                if at.elapsed() < DESCRIBED_FOR {
                    return Ok(w.clone());
                }
            }
        }
        let clickhouse = self.rules.clickhouse_table(table);
        let described = match self.history.describe(&clickhouse).await {
            Ok(c) => c,
            Err(HistoryError::UnknownTable(_)) => {
                return Err(EngineError::Insert(format!(
                    "unknown table {table} in ClickHouse ({clickhouse})"
                )))
            }
            Err(e) => return Err(EngineError::History(e)),
        };
        let topic = self.rules.topic(table);
        if self.source.partitions(&topic).await?.is_none() {
            return Err(EngineError::Insert(format!(
                "no topic {topic} for table {table}"
            )));
        }
        let columns = described
            .iter()
            .filter(|c| !self.rules.is_hidden(table, &c.name))
            .map(|c| {
                let m = self.rules.column(table, &c.name);
                let column = WriteColumn {
                    source: m.source,
                    ty: Type::parse(&c.ty),
                };
                (m.name, column)
            })
            .collect();
        let w = Arc::new(WriteTable { topic, columns });
        self.described
            .lock()
            .unwrap()
            .insert(table.to_string(), (Instant::now(), w.clone()));
        Ok(w)
    }
}

/// One row as the message produced for it, or why it can't be.
fn message(
    table: &str,
    w: &WriteTable,
    config: &InsertConfig,
    filter: &InsertFilter,
    row: &Row,
) -> Result<Outgoing, String> {
    let column = |name: &str, what: &str| {
        w.columns.get(name).ok_or_else(|| {
            format!("the server's config names {name} as {table}'s {what}, which it doesn't have")
        })
    };
    let mut values: Vec<(&str, &WriteColumn, Value)> = Vec::new();
    for (name, json) in row {
        let c = w
            .columns
            .get(name)
            .ok_or_else(|| format!("no column {name} in {table}"))?;
        if let ColumnSource::Kafka(field) = c.source {
            return Err(format!("{name} is the Kafka {field}, which Kafka sets"));
        }
        if config.user.as_deref() == Some(name) {
            return Err(format!("{name} is set by the server, to who inserts"));
        }
        if filter.columns.as_ref().is_some_and(|a| !a.contains(name)) {
            return Err(format!("you may not set {name} of {table}"));
        }
        values.push((
            name,
            c,
            decode(json, &c.ty).map_err(|e| format!("{name}: {e}"))?,
        ));
    }
    if let Some(user_column) = &config.user {
        let c = column(user_column, "user column")?;
        let user = filter.user.as_deref().expect("checked: a user");
        let v = decode(&Json::String(user.to_string()), &c.ty)
            .map_err(|e| format!("{user_column} (the user): {e}"))?;
        values.push((user_column, c, v));
    }
    match &filter.rows {
        RowFilter::All => {}
        RowFilter::None => return Err(format!("you may not insert into {table}")),
        RowFilter::In {
            column: name,
            values: granted,
        } => {
            let c = column(name, "rows' column")?;
            let v = values
                .iter()
                .find(|(n, ..)| n == name)
                .map(|(.., v)| v.clone())
                .unwrap_or_else(|| c.ty.default_value());
            let allowed = granted.iter().any(|g| {
                c.ty.kind
                    .column_type()
                    .decode(&Json::String(g.clone()))
                    .as_ref()
                    == Some(&v)
            });
            if !allowed {
                return Err(format!(
                    "you may not insert rows of {table} with {name} = {}",
                    v.to_json()
                ));
            }
        }
    }
    let key = match &config.key {
        None => None,
        Some(name) => {
            column(name, "key")?;
            values
                .iter()
                .find(|(n, ..)| n == name)
                .and_then(|(.., v)| match v.to_json() {
                    Json::Null => None,
                    Json::String(s) => Some(s),
                    other => Some(other.to_string()),
                })
        }
    };
    let mut out = serde_json::Map::new();
    for (_, c, v) in &values {
        let ColumnSource::Json(json_key) = &c.source else {
            unreachable!("checked: Kafka columns aren't set");
        };
        out.insert(json_key.clone(), v.to_json());
    }
    Ok(Outgoing {
        key,
        value: Json::Object(out).to_string(),
    })
}

/// A value as its column takes it, strictly: NULL only for a Nullable column, and an integer
/// only within its type's range.
fn decode(json: &Json, ty: &Type) -> Result<Value, String> {
    let value = match (json, &ty.kind) {
        (Json::Null, _) if ty.nullable => Some(Value::Null),
        (Json::Null, _) => return Err(format!("{ty} isn't Nullable")),
        (json, Kind::Int(t)) => t.decode_exact(json),
        (json, kind) => kind.column_type().decode(json),
    };
    value.ok_or_else(|| {
        let mut found = json.to_string();
        if found.len() > 40 {
            found.truncate(40);
            found.push('…');
        }
        format!("cannot read {found} as {ty}")
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::history::{DescribedColumn, HistorySource};
    use crate::rules::{ConfiguredRules, TablesConfig};
    use crate::snapshot::{Lease, SnapshotError, SnapshotTable, Snapshots};
    use crate::source::{MemorySink, MemorySource};
    use async_trait::async_trait;
    use serde_json::json;
    use std::collections::BTreeSet;

    struct Described;

    #[async_trait]
    impl HistorySource for Described {
        async fn describe(&self, table: &str) -> Result<Vec<DescribedColumn>, HistoryError> {
            let col = |n: &str, ty: &str| DescribedColumn {
                name: n.into(),
                ty: ty.into(),
            };
            match table {
                "default.messages" => Ok(vec![
                    col("ts", "DateTime64(3)"),
                    col("chat_id", "String"),
                    col("author", "String"),
                    col("body", "String"),
                    col("likes", "UInt8"),
                    col("reply_to", "Nullable(UUID)"),
                    col("_version", "UInt64"),
                    col("offset", "UInt64"),
                    col("extra", "Map(String, String)"),
                ]),
                _ => Err(HistoryError::UnknownTable(table.into())),
            }
        }

        async fn fetch(&self, _: &str) -> Result<Vec<Row>, HistoryError> {
            unreachable!()
        }
    }

    struct NoSnapshots;

    #[async_trait]
    impl Snapshots for NoSnapshots {
        async fn acquire(&self, _: &SnapshotTable) -> Result<Lease, SnapshotError> {
            unreachable!()
        }
    }

    fn engine(insert: Json) -> (Engine, MemorySink) {
        let tables: TablesConfig = serde_json::from_value(json!({
            "messages": {
                "columns": {"body": {"json": "text"}, "offset": {"kafka": "offset"}},
                "insert": insert,
            },
            "logs": {},
        }))
        .unwrap();
        let mut source = MemorySource::new();
        source.partition("messages", 0).partition("logs", 0);
        let sink = MemorySink::new();
        let engine = Engine::new(
            ConfiguredRules::new(tables).unwrap(),
            Described,
            source,
            NoSnapshots,
        )
        .with_sink(sink.clone());
        (engine, sink)
    }

    fn row(v: Json) -> Row {
        v.as_object().unwrap().clone()
    }

    fn as_user(user: &str) -> InsertFilter {
        InsertFilter::open(Some(user))
    }

    async fn refused(engine: &Engine, rows: &[Json], filter: &InsertFilter) -> String {
        let rows: Vec<Row> = rows.iter().cloned().map(row).collect();
        match engine.insert("messages", &rows, filter).await {
            Err(EngineError::Insert(e) | EngineError::Denied(e)) => e,
            other => panic!("{other:?}"),
        }
    }

    #[tokio::test]
    async fn rows_become_messages_with_their_json_keys_and_rendered_values() {
        let (engine, sink) = engine(json!({"user": "author", "key": "chat_id"}));
        let rows = [
            row(
                json!({"ts": "2026-11-01 10:00:00.5", "chat_id": "c1", "body": "hi", "likes": "7",
                       "reply_to": "6F1D3A201B2C4D5E8F90A1B2C3D4E5F6", "extra": {"a": "b"}}),
            ),
            row(json!({"chat_id": "c2", "reply_to": null})),
        ];
        let offsets = engine
            .insert("messages", &rows, &as_user("u1"))
            .await
            .unwrap();
        assert_eq!(offsets, [(0, 0), (0, 1)]);
        let produced = sink.produced("messages");
        assert_eq!(produced[0].key.as_deref(), Some("c1"));
        assert_eq!(
            serde_json::from_str::<Json>(&produced[0].value).unwrap(),
            json!({"ts": "2026-11-01 10:00:00.500", "chat_id": "c1", "text": "hi", "likes": 7,
                   "reply_to": "6f1d3a20-1b2c-4d5e-8f90-a1b2c3d4e5f6", "extra": {"a": "b"},
                   "author": "u1"})
        );
        assert_eq!(
            serde_json::from_str::<Json>(&produced[1].value).unwrap(),
            json!({"chat_id": "c2", "reply_to": null, "author": "u1"}),
            "what a row leaves out stays out (ClickHouse's DEFAULT)"
        );
    }

    #[tokio::test]
    async fn bad_rows_are_refused_and_nothing_is_produced() {
        let (engine, sink) = engine(json!({"user": "author"}));
        let u = as_user("u1");
        let cases = [
            (json!({"nope": 1}), "no column nope in messages"),
            (json!({"_version": 1}), "no column _version"),
            (json!({"offset": 1}), "offset is the Kafka offset"),
            (json!({"author": "u2"}), "author is set by the server"),
            (json!({"likes": 256}), "cannot read 256 as UInt8"),
            (json!({"likes": -1}), "cannot read -1 as UInt8"),
            (json!({"body": null}), "body: String isn't Nullable"),
            (
                json!({"ts": "yesterday"}),
                "cannot read \"yesterday\" as DateTime64(3)",
            ),
        ];
        for (r, expected) in cases {
            let e = refused(&engine, std::slice::from_ref(&r), &u).await;
            assert!(e.contains(expected), "{r}: {e}");
        }
        // one bad row: none of them go
        let e = refused(&engine, &[json!({"body": "ok"}), json!({"likes": 300})], &u).await;
        assert!(e.starts_with("row 1: "), "{e}");
        assert!(sink.produced("messages").is_empty());
    }

    #[tokio::test]
    async fn who_may_insert_what() {
        let (engine, sink) = engine(json!({"user": "author"}));
        // not a table that takes inserts
        let rows = [row(json!({"x": 1}))];
        assert!(matches!(
            engine.insert("logs", &rows, &as_user("u1")).await,
            Err(EngineError::Denied(e)) if e == "logs doesn't take inserts"
        ));
        // a user is needed
        let e = refused(&engine, &[json!({"body": "hi"})], &InsertFilter::open(None)).await;
        assert!(e.contains("needs a user"), "{e}");
        // the rows it may insert
        let chats = InsertFilter {
            rows: RowFilter::In {
                column: "chat_id".into(),
                values: vec!["c1".into()],
            },
            ..as_user("u1")
        };
        let e = refused(&engine, &[json!({"chat_id": "c2"})], &chats).await;
        assert!(e.contains("with chat_id = \"c2\""), "{e}");
        let e = refused(&engine, &[json!({"body": "no chat"})], &chats).await;
        assert!(e.contains("with chat_id = \"\""), "{e}");
        let none = InsertFilter {
            rows: RowFilter::None,
            ..as_user("u1")
        };
        assert!(refused(&engine, &[json!({})], &none)
            .await
            .contains("may not insert"));
        // the columns it may set
        let some = InsertFilter {
            columns: Some(BTreeSet::from(["chat_id".to_string(), "body".to_string()])),
            ..chats.clone()
        };
        let e = refused(&engine, &[json!({"chat_id": "c1", "likes": 1})], &some).await;
        assert!(e.contains("may not set likes"), "{e}");
        assert!(sink.produced("messages").is_empty());
        let rows = [row(json!({"chat_id": "c1", "body": "hi"}))];
        engine.insert("messages", &rows, &some).await.unwrap();
        assert_eq!(sink.produced("messages").len(), 1);
    }

    #[tokio::test]
    async fn public_tables_take_anyone_and_a_misnamed_column_says_so() {
        let (public, _) = engine(json!({"public": true}));
        let rows = [row(json!({"body": "hi"}))];
        public
            .insert("messages", &rows, &InsertFilter::open(None))
            .await
            .unwrap();
        let (misnamed, _) = engine(json!({"user": "who"}));
        let e = refused(&misnamed, &[json!({"body": "hi"})], &as_user("u1")).await;
        assert!(e.contains("names who as messages's user column"), "{e}");
    }

    #[test]
    fn strict_decoding() {
        let int = Type::parse("Int8");
        assert_eq!(decode(&json!(-128), &int), Ok(Value::Int(-128)));
        assert!(decode(&json!(128), &int).is_err());
        assert_eq!(decode(&json!("127"), &int), Ok(Value::Int(127)));
        assert_eq!(
            decode(&json!(null), &Type::parse("Nullable(Int8)")),
            Ok(Value::Null)
        );
        let max = Type::parse("UInt64");
        assert_eq!(
            decode(&json!(u64::MAX), &max),
            Ok(Value::Int(u64::MAX as i128))
        );
    }
}
