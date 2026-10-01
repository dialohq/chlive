//! chlive engine: a custom SQL dialect for live queries over ClickHouse history plus a stream
//! of events (Kafka, simulated here by JSONL files).
//!
//! ```no_run
//! use chlive_engine::{Config, Engine};
//! use futures::StreamExt;
//!
//! # async fn demo() -> Result<(), chlive_engine::EngineError> {
//! let engine = Engine::connect(Config::default());
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
//! history from ClickHouse and then one iteration per event of the subscribed topics.

pub mod ast;
pub mod binder;
pub mod diff;
pub mod engine;
pub mod error;
pub mod history;
pub mod lexer;
pub mod parser;
pub mod plan;
pub mod rules;
pub mod stream;

pub use binder::{BoundColumn, BoundQuery};
pub use engine::{Config, Engine, EngineError, Iteration, Origin, Subscription};
pub use plan::Plan;
