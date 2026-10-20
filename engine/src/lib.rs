//! chlive engine: a custom SQL dialect for live queries over ClickHouse history plus a stream
//! of events from Kafka.
//!
//! ```no_run
//! use chlive_engine::rules::TablesConfig;
//! use chlive_engine::{Config, Engine};
//! use futures::StreamExt;
//!
//! # async fn demo() -> Result<(), Box<dyn std::error::Error>> {
//! let engine = Engine::connect(Config {
//!     tables: TablesConfig::listed(["events"]),
//!     ..Config::default()
//! })?;
//! println!("{}", engine.explain("select * from events").await?);
//! let mut subscription = engine.subscribe("select * from events").await?;
//! while let Some(iteration) = subscription.next().await {
//!     println!("{}", iteration?); // the diffs of this iteration
//! }
//! # Ok(())
//! # }
//! ```
//!
//! Inside: [`parser`] → [`binder`] (via [`rules`]) → [`plan`] → [`engine`], which yields the
//! history from ClickHouse and then one iteration per event of the subscribed topics. History
//! is read from a [`snapshot`] of the table, taken while its ingestion was paused, so it holds
//! exactly the offsets up to a per-partition [`watermark`]; the stream starts right after, so
//! each event is in exactly one.

pub mod aggregate;
pub mod ast;
pub mod binder;
pub mod calendar;
pub mod chdb;
pub mod checkpoint;
pub mod clock;
pub mod cursor;
pub mod diff;
pub mod engine;
pub mod error;
pub mod history;
pub mod keyed;
pub mod lexer;
pub mod order;
pub mod parser;
pub mod plan;
pub mod rules;
pub mod security;
pub mod semi;
pub mod snapshot;
pub mod source;
pub mod table_engine;
pub mod timeline;
pub mod types;
pub mod watermark;

pub use binder::{BoundColumn, BoundExpr, BoundQuery, Params};
pub use checkpoint::Checkpoint;
pub use engine::{Config, Engine, EngineError, Iteration, Origin, Subscription};
pub use plan::Plan;
pub use security::{RowFilter, RowFilters};
