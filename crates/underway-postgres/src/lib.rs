//! Durable Postgres backend for Underway.
//!
//! This crate owns Underway's Postgres implementation details, including SQLx
//! integration, migrations, and advisory scheduler locks. Those details adapt
//! Postgres to the backend-neutral semantics defined by `underway-core`.
//!
//! Worker wakeups currently use the backend-neutral polling path. The database
//! schema still includes notification triggers for future listener support, but
//! polling and shared backend contracts define correctness.
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
