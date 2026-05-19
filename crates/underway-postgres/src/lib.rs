//! Durable Postgres backend for Underway.
//!
//! This crate owns Underway's Postgres implementation details, including SQLx
//! integration, migrations, advisory scheduler locks, and database
//! notifications. Those details adapt Postgres to the backend-neutral semantics
//! defined by `underway-core`.
//!
//! Notifications reduce worker latency only; polling and shared backend
//! contracts define correctness.
#![forbid(unsafe_code)]

mod backend;
mod error;
mod lease;
mod rows;
mod transaction;

#[cfg(all(test, feature = "contracts"))]
mod tests;

pub use backend::PostgresBackend;
pub use error::Error;
pub use lease::PostgresSchedulerLease;
pub use transaction::PostgresTransaction;
