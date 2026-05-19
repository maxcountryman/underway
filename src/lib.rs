//! Durable background workflows with pluggable backends.
//!
//! The `underway` crate is a small facade over the backend-neutral core and
//! optional backend crates.
#![forbid(unsafe_code)]

pub use underway_core::*;
#[cfg(feature = "memory")]
pub use underway_memory as memory;
#[cfg(feature = "memory")]
pub use underway_memory::MemoryBackend;
#[cfg(feature = "postgres")]
pub use underway_postgres as postgres;
#[cfg(feature = "postgres")]
pub use underway_postgres::PostgresBackend;
