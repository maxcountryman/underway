//! Backend-neutral durable workflow semantics.
#![forbid(unsafe_code)]

use std::{
    collections::HashMap,
    fmt::{self, Display},
    future::Future,
    marker::PhantomData,
    ops::Deref,
    pin::Pin,
    result::Result as StdResult,
    sync::{Arc, Mutex},
    time::Duration as StdDuration,
};

pub use activity::{Activity, CallState as ActivityCallState, Error as ActivityError};
pub use backend::{
    ActivityCallRecord, Backend, BackendTransaction, ClaimedActivityCall, ClaimedTask, EnqueueTask,
    SchedulerLease,
};
use jiff::{SignedDuration, Span, ToSpan, Zoned};
use jiff_cron::{Schedule, ScheduleIterator};
pub use queue::{Queue, QueueBuilder};
pub use runtime::{Runtime, RuntimeHandle};
pub use scheduler::{Scheduler, ZonedSchedule};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
pub use task::{Task, TaskConfig, TaskConfigBuilder, TaskId, ToTaskResult};
use tokio::{task::JoinSet, time::sleep};
use tokio_util::sync::CancellationToken;
use tracing::instrument;
use ulid::Ulid;
use uuid::Uuid;
pub use worker::Worker;
pub use workflow::{
    Context, EnqueuedWorkflow, InvokeActivity, Transition, Workflow, WorkflowRunId,
};

/// A crate-level result.
pub type Result<T = ()> = StdResult<T, Error>;

/// Errors produced by backend-neutral orchestration.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Error returned by a backend.
    #[error("backend error: {0}")]
    Backend(String),

    /// Error returned by task execution.
    #[error(transparent)]
    Task(#[from] task::Error),

    /// Invalid task configuration.
    #[error(transparent)]
    TaskConfig(#[from] task::ConfigError),

    /// Error returned by JSON serialization/deserialization.
    #[error(transparent)]
    Json(#[from] serde_json::Error),

    /// Error returned by `jiff`.
    #[error(transparent)]
    Jiff(#[from] jiff::Error),

    /// Error returned by cron parsing.
    #[error(transparent)]
    Cron(#[from] jiff_cron::error::Error),

    /// Error returned by Tokio task joins.
    #[error(transparent)]
    Join(#[from] tokio::task::JoinError),
}

impl Error {
    fn backend(err: impl Display) -> Self {
        Self::Backend(err.to_string())
    }
}

/// Activity handlers and durable error envelopes.
pub mod activity;

/// Task values and the user task trait.
pub mod task;

/// Backend traits and semantic records.
pub mod backend;

/// Reusable backend contract checks.
#[cfg(any(test, feature = "contracts"))]
pub mod contracts;

/// Queue API.
pub mod queue;

/// Workers execute tasks from queues.
pub mod worker;

/// Cron scheduler.
pub mod scheduler;

mod activity_worker;

/// Workflow API.
pub mod workflow;

/// Runtime orchestration.
pub mod runtime;
