//! Aggregate functions (see [`chlive_core::aggregate`]), and how a query's aggregates run: natively
//! (`native_run`: the groups' states in the engine, seeded from ClickHouse) or in chDB
//! (`chdb_run`).

pub use chlive_core::aggregate::*;

pub mod chdb_run;
pub mod native_run;
