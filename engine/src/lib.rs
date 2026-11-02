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
pub mod binder;
pub mod chdb;
pub mod clock;
pub mod engine;
pub mod history;
pub mod semi;
pub mod snapshot;
pub mod source;
pub mod write;

// the pure part (the dialect, the plans, the stream's state), shared with the clients
pub use chlive_core::{
    ast, bound, calendar, checkpoint, cursor, diff, error, event, keyed, lexer, order, parser,
    plan, predict, result, rules, security, state, table, table_engine, timeline, types, watermark,
};

pub use binder::{BoundColumn, BoundExpr, BoundQuery, Params};
pub use checkpoint::Checkpoint;
pub use engine::{Config, Engine, EngineError, Iteration, Origin, Subscription};
pub use plan::Plan;
pub use security::{Filters, RowFilter, TableFilter};
