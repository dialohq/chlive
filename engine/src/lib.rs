//! chlive engine: a custom SQL dialect for live queries over ClickHouse history plus a stream
//! of events (Kafka, simulated here by JSONL files).
//!
//! Pipeline: [`parser`] → [`schema`] (via [`rules`]) → [`plan`] → [`engine`], which yields the
//! history from ClickHouse and then one iteration per event of the subscribed topics.

pub mod ast;
pub mod diff;
pub mod engine;
pub mod error;
pub mod history;
pub mod lexer;
pub mod parser;
pub mod plan;
pub mod rules;
pub mod schema;
pub mod stream;
