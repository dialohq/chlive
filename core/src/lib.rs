//! chlive's core: the dialect ([`parser`]), queries as bound ([`bound`]), their plans
//! ([`plan`]), and the state a live result keeps as events come ([`order`], [`keyed`],
//! [`timeline`], [`aggregate`]), with the diffs it makes ([`diff`]). No IO: the engine
//! (ClickHouse, Kafka, snapshots) and the clients build on it, natively or as WebAssembly.

pub mod aggregate;
pub mod ast;
pub mod bound;
pub mod calendar;
pub mod checkpoint;
pub mod cursor;
pub mod diff;
pub mod error;
pub mod event;
pub mod keyed;
pub mod lexer;
pub mod order;
pub mod parser;
pub mod plan;
pub mod result;
pub mod rules;
pub mod security;
pub mod state;
pub mod table;
pub mod table_engine;
pub mod timeline;
pub mod types;
pub mod watermark;
