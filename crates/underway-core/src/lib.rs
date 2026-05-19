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
pub mod activity {
    use super::*;

    /// A type alias for activity execution results.
    pub type Result<T> = StdResult<T, Error>;

    /// Durable activity call state.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
    pub enum CallState {
        /// Activity call is queued and waiting to be processed.
        Pending,

        /// Activity call is currently being processed.
        InProgress,

        /// Activity call completed successfully.
        Succeeded,

        /// Activity call completed unsuccessfully.
        Failed,
    }

    /// Standard activity error envelope.
    #[derive(Debug, Clone, Deserialize, Serialize, thiserror::Error)]
    #[error("[{code}] {message}")]
    pub struct Error {
        /// Stable activity-local error code.
        pub code: String,

        /// Human-readable failure message.
        pub message: String,

        /// Whether the failure should be retried.
        pub retryable: bool,

        /// Optional structured details.
        pub details: Option<serde_json::Value>,
    }

    impl Error {
        /// Creates a retryable activity error.
        pub fn retryable(code: impl Into<String>, message: impl Into<String>) -> Self {
            Self {
                code: code.into(),
                message: message.into(),
                retryable: true,
                details: None,
            }
        }

        /// Creates a non-retryable activity error.
        pub fn fatal(code: impl Into<String>, message: impl Into<String>) -> Self {
            Self {
                code: code.into(),
                message: message.into(),
                retryable: false,
                details: None,
            }
        }

        /// Adds structured details.
        pub fn with_details(mut self, details: serde_json::Value) -> Self {
            self.details = Some(details);
            self
        }
    }

    /// Trait for durable side-effect handlers.
    pub trait Activity: Send + Sync + 'static {
        /// Stable persisted activity name.
        const NAME: &'static str;

        /// Activity input payload.
        type Input: DeserializeOwned + Serialize + Send + 'static;

        /// Activity output payload.
        type Output: DeserializeOwned + Serialize + Send + 'static;

        /// Execute the side effect.
        fn execute(&self, input: Self::Input) -> impl Future<Output = Result<Self::Output>> + Send;

        /// Retry policy for this activity.
        fn retry_policy(&self) -> task::RetryPolicy {
            task::RetryPolicy::default()
        }

        /// Activity timeout.
        fn timeout(&self) -> Span {
            15.minutes()
        }
    }
}

/// Task values and the user task trait.
pub mod task {
    use super::*;

    pub(crate) type RetryCount = i32;

    /// Task identifier.
    #[derive(Debug, Clone, Copy, Serialize, Deserialize, Hash, Eq, PartialEq)]
    pub struct TaskId(Uuid);

    impl TaskId {
        /// Generates a new task ID.
        pub fn new() -> Self {
            Self(Ulid::new().into())
        }

        /// Reconstitutes a task ID from a UUID.
        pub const fn from_uuid(id: Uuid) -> Self {
            Self(id)
        }

        /// Returns the underlying UUID.
        pub const fn as_uuid(self) -> Uuid {
            self.0
        }

        /// Returns hyphenated UUID formatting.
        pub fn as_hyphenated(&self) -> &uuid::fmt::Hyphenated {
            self.0.as_hyphenated()
        }
    }

    impl Default for TaskId {
        fn default() -> Self {
            Self::new()
        }
    }

    impl Deref for TaskId {
        type Target = Uuid;

        fn deref(&self) -> &Self::Target {
            &self.0
        }
    }

    impl Display for TaskId {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "{}", self.0)
        }
    }

    /// A type alias for task execution results.
    pub type Result<T> = StdResult<T, Error>;

    /// A type alias for task configuration results.
    pub type ConfigResult<T = TaskConfig> = StdResult<T, ConfigError>;

    /// Task execution errors.
    #[derive(Debug, thiserror::Error)]
    #[non_exhaustive]
    pub enum Error {
        /// Indicates the task timed out.
        #[error("Task timed out after {0} during execution")]
        TimedOut(SignedDuration),

        /// Indicates execution suspended while waiting for durable external
        /// work.
        #[error("{0}")]
        Suspended(String),

        /// Unrecoverable error.
        #[error("{0}")]
        Fatal(String),

        /// Recoverable error.
        #[error("{0}")]
        Retryable(String),
    }

    /// Task configuration validation errors.
    #[derive(Debug, thiserror::Error)]
    #[non_exhaustive]
    pub enum ConfigError {
        /// Retry policy must allow at least one attempt.
        #[error("retry policy max_attempts must be at least 1, got {0}")]
        InvalidMaxAttempts(i32),

        /// Retry initial interval must not be negative.
        #[error("retry policy initial_interval_ms must be non-negative, got {0}")]
        InvalidInitialInterval(i32),

        /// Retry maximum interval must not be negative.
        #[error("retry policy max_interval_ms must be non-negative, got {0}")]
        InvalidMaxInterval(i32),

        /// Retry maximum interval must be greater than or equal to the initial
        /// interval.
        #[error(
            "retry policy max_interval_ms must be greater than or equal to initial_interval_ms, \
             got max={max} and initial={initial}"
        )]
        InvalidIntervalRange {
            /// Configured maximum interval.
            max: i32,
            /// Configured initial interval.
            initial: i32,
        },

        /// Retry backoff coefficient must be finite and at least one.
        #[error("retry policy backoff_coefficient must be finite and at least 1, got {0}")]
        InvalidBackoffCoefficient(f64),

        /// Retry jitter factor must be finite and between zero and one.
        #[error("retry policy jitter_factor must be finite and between 0 and 1, got {0}")]
        InvalidJitterFactor(f64),

        /// Duration field must be representable as an invariant unsigned
        /// duration.
        #[error("task config {field} must be a non-negative invariant duration: {message}")]
        InvalidDuration {
            /// Field being validated.
            field: &'static str,
            /// Conversion failure message.
            message: String,
        },

        /// Duration field must be greater than zero.
        #[error("task config {0} must be greater than zero")]
        NonPositiveDuration(&'static str),

        /// Heartbeat interval cannot be greater than the task timeout.
        #[error("task config heartbeat must be less than or equal to timeout")]
        HeartbeatExceedsTimeout,

        /// Concurrency keys must not be empty.
        #[error("task config concurrency_key must not be empty")]
        EmptyConcurrencyKey,

        /// Error returned by span arithmetic.
        #[error(transparent)]
        Jiff(#[from] jiff::Error),
    }

    /// Converts external errors into task errors.
    pub trait ToTaskResult<T> {
        /// Converts the error to retryable.
        fn retryable(self) -> StdResult<T, Error>;

        /// Converts the error to fatal.
        fn fatal(self) -> StdResult<T, Error>;
    }

    impl<T, E: Display> ToTaskResult<T> for StdResult<T, E> {
        fn retryable(self) -> StdResult<T, Error> {
            self.map_err(|err| Error::Retryable(err.to_string()))
        }

        fn fatal(self) -> StdResult<T, Error> {
            self.map_err(|err| Error::Fatal(err.to_string()))
        }
    }

    /// Retry policy for task or activity attempts.
    #[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
    pub struct RetryPolicy {
        /// Maximum attempts before terminal failure.
        pub max_attempts: i32,
        /// Initial backoff interval in milliseconds.
        pub initial_interval_ms: i32,
        /// Maximum backoff interval in milliseconds.
        pub max_interval_ms: i32,
        /// Exponential backoff coefficient.
        pub backoff_coefficient: f64,
        /// Jitter coefficient.
        pub jitter_factor: f64,
    }

    impl RetryPolicy {
        /// Creates a retry policy builder.
        pub fn builder() -> RetryPolicyBuilder {
            RetryPolicyBuilder::default()
        }

        /// Validates the retry policy invariants.
        pub fn validate(&self) -> ConfigResult<()> {
            if self.max_attempts < 1 {
                return Err(ConfigError::InvalidMaxAttempts(self.max_attempts));
            }
            if self.initial_interval_ms < 0 {
                return Err(ConfigError::InvalidInitialInterval(
                    self.initial_interval_ms,
                ));
            }
            if self.max_interval_ms < 0 {
                return Err(ConfigError::InvalidMaxInterval(self.max_interval_ms));
            }
            if self.max_interval_ms < self.initial_interval_ms {
                return Err(ConfigError::InvalidIntervalRange {
                    max: self.max_interval_ms,
                    initial: self.initial_interval_ms,
                });
            }
            if !self.backoff_coefficient.is_finite() || self.backoff_coefficient < 1.0 {
                return Err(ConfigError::InvalidBackoffCoefficient(
                    self.backoff_coefficient,
                ));
            }
            if !self.jitter_factor.is_finite() || !(0.0..=1.0).contains(&self.jitter_factor) {
                return Err(ConfigError::InvalidJitterFactor(self.jitter_factor));
            }

            Ok(())
        }

        /// Calculates the delay for a retry count.
        pub fn calculate_delay(&self, retry_count: RetryCount) -> Span {
            let base_delay = self.initial_interval_ms as f64;
            let backoff_delay = base_delay * self.backoff_coefficient.powi(retry_count - 1);
            let target_delay = backoff_delay.min(self.max_interval_ms as f64);
            let jitter = target_delay * self.jitter_factor;
            let delay = if jitter == 0.0 {
                target_delay
            } else {
                target_delay * (1.0 - self.jitter_factor) + rand::random::<f64>() * jitter
            };
            (delay as i64).milliseconds()
        }
    }

    impl Default for RetryPolicy {
        fn default() -> Self {
            Self {
                max_attempts: 5,
                initial_interval_ms: 1_000,
                max_interval_ms: 60_000,
                backoff_coefficient: 2.0,
                jitter_factor: 0.5,
            }
        }
    }

    /// Builder for [`RetryPolicy`].
    #[derive(Debug, Default)]
    pub struct RetryPolicyBuilder {
        inner: RetryPolicy,
    }

    impl RetryPolicyBuilder {
        /// Sets maximum attempts.
        pub const fn max_attempts(mut self, max_attempts: i32) -> Self {
            self.inner.max_attempts = max_attempts;
            self
        }

        /// Sets initial interval in milliseconds.
        pub const fn initial_interval_ms(mut self, initial_interval_ms: i32) -> Self {
            self.inner.initial_interval_ms = initial_interval_ms;
            self
        }

        /// Sets max interval in milliseconds.
        pub const fn max_interval_ms(mut self, max_interval_ms: i32) -> Self {
            self.inner.max_interval_ms = max_interval_ms;
            self
        }

        /// Sets backoff coefficient.
        pub const fn backoff_coefficient(mut self, backoff_coefficient: f64) -> Self {
            self.inner.backoff_coefficient = backoff_coefficient;
            self
        }

        /// Sets jitter factor.
        pub const fn jitter_factor(mut self, jitter_factor: f64) -> Self {
            self.inner.jitter_factor = jitter_factor;
            self
        }

        /// Builds the policy.
        pub const fn build(self) -> RetryPolicy {
            self.inner
        }
    }

    /// Possible task states.
    #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
    pub enum State {
        /// Awaiting execution.
        Pending,
        /// Currently in progress.
        InProgress,
        /// Suspended on durable activity completion.
        Waiting,
        /// Succeeded.
        Succeeded,
        /// Cancelled.
        Cancelled,
        /// Failed.
        Failed,
    }

    /// Task execution configuration.
    #[derive(Debug, Clone, PartialEq, Serialize)]
    pub struct TaskConfig {
        retry_policy: RetryPolicy,
        timeout: Span,
        ttl: Span,
        delay: Span,
        heartbeat: Span,
        concurrency_key: Option<String>,
        priority: i32,
    }

    impl Default for TaskConfig {
        fn default() -> Self {
            Self {
                retry_policy: RetryPolicy::default(),
                timeout: 15.minutes(),
                ttl: 14.days(),
                delay: Span::new(),
                heartbeat: 30.seconds(),
                concurrency_key: None,
                priority: 0,
            }
        }
    }

    impl<'de> Deserialize<'de> for TaskConfig {
        fn deserialize<D>(deserializer: D) -> StdResult<Self, D::Error>
        where
            D: serde::Deserializer<'de>,
        {
            #[derive(Deserialize)]
            struct RawTaskConfig {
                retry_policy: RetryPolicy,
                timeout: Span,
                ttl: Span,
                delay: Span,
                heartbeat: Span,
                concurrency_key: Option<String>,
                priority: i32,
            }

            let raw = RawTaskConfig::deserialize(deserializer)?;
            Self {
                retry_policy: raw.retry_policy,
                timeout: raw.timeout,
                ttl: raw.ttl,
                delay: raw.delay,
                heartbeat: raw.heartbeat,
                concurrency_key: raw.concurrency_key,
                priority: raw.priority,
            }
            .validated()
            .map_err(serde::de::Error::custom)
        }
    }

    impl TaskConfig {
        /// Creates a task configuration builder.
        pub fn builder() -> TaskConfigBuilder {
            TaskConfigBuilder::default()
        }

        /// Creates a builder initialized from this configuration.
        pub fn into_builder(self) -> TaskConfigBuilder {
            TaskConfigBuilder { inner: self }
        }

        /// Validates this configuration.
        pub fn validate(&self) -> ConfigResult<()> {
            self.retry_policy.validate()?;

            let timeout = positive_duration("timeout", self.timeout)?;
            positive_duration("ttl", self.ttl)?;
            non_negative_duration("delay", self.delay)?;
            let heartbeat = positive_duration("heartbeat", self.heartbeat)?;

            if heartbeat > timeout {
                return Err(ConfigError::HeartbeatExceedsTimeout);
            }

            if self
                .concurrency_key
                .as_deref()
                .is_some_and(|key| key.is_empty())
            {
                return Err(ConfigError::EmptyConcurrencyKey);
            }

            Ok(())
        }

        /// Returns this configuration after validating it.
        pub fn validated(self) -> ConfigResult<Self> {
            self.validate()?;
            Ok(self)
        }

        /// Returns this configuration with additional enqueue delay applied.
        pub fn with_additional_delay(mut self, delay: Span) -> ConfigResult<Self> {
            non_negative_duration("additional delay", delay)?;
            self.delay = self.delay.checked_add(delay)?;
            self.validated()
        }

        /// Returns the retry policy.
        pub const fn retry_policy(&self) -> RetryPolicy {
            self.retry_policy
        }

        /// Returns the execution timeout.
        pub const fn timeout(&self) -> Span {
            self.timeout
        }

        /// Returns the retention TTL.
        pub const fn ttl(&self) -> Span {
            self.ttl
        }

        /// Returns the base delay.
        pub const fn delay(&self) -> Span {
            self.delay
        }

        /// Returns the heartbeat interval.
        pub const fn heartbeat(&self) -> Span {
            self.heartbeat
        }

        /// Returns the optional serializing key.
        pub fn concurrency_key(&self) -> Option<&str> {
            self.concurrency_key.as_deref()
        }

        /// Returns the priority. Higher values are claimed first.
        pub const fn priority(&self) -> i32 {
            self.priority
        }

        pub(crate) fn set_retry_policy(&mut self, retry_policy: RetryPolicy) {
            self.retry_policy = retry_policy;
        }

        pub(crate) fn set_timeout(&mut self, timeout: Span) {
            self.timeout = timeout;
        }

        pub(crate) fn set_ttl(&mut self, ttl: Span) {
            self.ttl = ttl;
        }

        pub(crate) fn set_delay(&mut self, delay: Span) {
            self.delay = delay;
        }

        pub(crate) fn set_heartbeat(&mut self, heartbeat: Span) {
            self.heartbeat = heartbeat;
        }

        pub(crate) fn set_concurrency_key(&mut self, concurrency_key: Option<String>) {
            self.concurrency_key = concurrency_key;
        }

        pub(crate) fn set_priority(&mut self, priority: i32) {
            self.priority = priority;
        }
    }

    /// Builder for [`TaskConfig`].
    #[derive(Debug, Clone, Default)]
    pub struct TaskConfigBuilder {
        inner: TaskConfig,
    }

    impl TaskConfigBuilder {
        /// Sets the retry policy.
        pub fn retry_policy(mut self, retry_policy: RetryPolicy) -> Self {
            self.inner.retry_policy = retry_policy;
            self
        }

        /// Sets the execution timeout.
        pub fn timeout(mut self, timeout: Span) -> Self {
            self.inner.timeout = timeout;
            self
        }

        /// Sets the retention TTL.
        pub fn ttl(mut self, ttl: Span) -> Self {
            self.inner.ttl = ttl;
            self
        }

        /// Sets the base delay.
        pub fn delay(mut self, delay: Span) -> Self {
            self.inner.delay = delay;
            self
        }

        /// Sets the heartbeat interval.
        pub fn heartbeat(mut self, heartbeat: Span) -> Self {
            self.inner.heartbeat = heartbeat;
            self
        }

        /// Sets the optional serializing key.
        pub fn concurrency_key(mut self, concurrency_key: impl Into<String>) -> Self {
            self.inner.concurrency_key = Some(concurrency_key.into());
            self
        }

        /// Sets the optional serializing key.
        pub fn maybe_concurrency_key(mut self, concurrency_key: Option<String>) -> Self {
            self.inner.concurrency_key = concurrency_key;
            self
        }

        /// Clears the optional serializing key.
        pub fn clear_concurrency_key(mut self) -> Self {
            self.inner.concurrency_key = None;
            self
        }

        /// Sets the priority. Higher values are claimed first.
        pub fn priority(mut self, priority: i32) -> Self {
            self.inner.priority = priority;
            self
        }

        /// Builds a validated task configuration.
        pub fn build(self) -> ConfigResult<TaskConfig> {
            self.inner.validated()
        }
    }

    fn non_negative_duration(field: &'static str, span: Span) -> ConfigResult<StdDuration> {
        StdDuration::try_from(span).map_err(|err| ConfigError::InvalidDuration {
            field,
            message: err.to_string(),
        })
    }

    fn positive_duration(field: &'static str, span: Span) -> ConfigResult<StdDuration> {
        let duration = non_negative_duration(field, span)?;
        if duration.is_zero() {
            return Err(ConfigError::NonPositiveDuration(field));
        }
        Ok(duration)
    }

    /// Trait for defining executable tasks.
    pub trait Task<B: backend::Backend>: Send + Sync + 'static {
        /// Input payload.
        type Input: DeserializeOwned + Serialize + Send + 'static;

        /// Output payload.
        type Output: Serialize + Send + 'static;

        /// Executes a task inside the backend transaction that claimed it.
        fn execute(
            &self,
            tx: &mut B::Transaction<'_>,
            input: Self::Input,
        ) -> impl Future<Output = Result<Self::Output>> + Send;

        /// Default task configuration.
        fn config(&self) -> ConfigResult {
            Ok(TaskConfig::default())
        }

        /// Per-input task configuration.
        fn config_for(&self, _input: &Self::Input) -> ConfigResult {
            self.config()
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;

        #[test]
        fn default_task_config_is_valid() {
            TaskConfig::default().validate().unwrap();
        }

        #[test]
        fn task_config_rejects_zero_timeout() {
            let err = TaskConfig::builder()
                .timeout(Span::new())
                .build()
                .unwrap_err();

            assert!(matches!(err, ConfigError::NonPositiveDuration("timeout")));
        }

        #[test]
        fn task_config_rejects_heartbeat_greater_than_timeout() {
            let err = TaskConfig::builder()
                .timeout(10.seconds())
                .heartbeat(30.seconds())
                .build()
                .unwrap_err();

            assert!(matches!(err, ConfigError::HeartbeatExceedsTimeout));
        }

        #[test]
        fn task_config_accepts_zero_delay() {
            TaskConfig::builder().delay(Span::new()).build().unwrap();
        }

        #[test]
        fn retry_policy_rejects_invalid_attempts() {
            let policy = RetryPolicy {
                max_attempts: 0,
                ..RetryPolicy::default()
            };

            assert!(matches!(
                policy.validate().unwrap_err(),
                ConfigError::InvalidMaxAttempts(0)
            ));
        }
    }
}

/// Backend traits and semantic records.
pub mod backend {
    use super::*;

    /// Task enqueue command.
    #[derive(Debug, Clone)]
    pub struct EnqueueTask {
        /// Serialized task input.
        pub input: serde_json::Value,
        /// Task config.
        pub config: task::TaskConfig,
        /// Additional delay, e.g. transition or retry delay.
        pub delay: Span,
    }

    /// Claimed task record.
    #[derive(Debug, Clone)]
    pub struct ClaimedTask {
        /// Task ID.
        pub id: task::TaskId,
        /// Queue name.
        pub queue_name: String,
        /// Serialized input.
        pub input: serde_json::Value,
        /// Effective task config.
        pub config: task::TaskConfig,
        /// Current attempt number.
        pub attempt_number: i32,
    }

    /// Durable activity call record loaded for workflow replay.
    #[derive(Debug, Clone)]
    pub struct ActivityCallRecord {
        /// Deterministic call key.
        pub call_key: String,
        /// Activity name.
        pub activity: String,
        /// Serialized input.
        pub input: serde_json::Value,
        /// Serialized output.
        pub output: Option<serde_json::Value>,
        /// Serialized activity error envelope.
        pub error: Option<serde_json::Value>,
        /// Durable state.
        pub state: activity::CallState,
    }

    /// Activity call command.
    #[derive(Debug, Clone)]
    pub struct ActivityCallCommand {
        /// Deterministic call key.
        pub call_key: String,
        /// Activity name.
        pub activity: String,
        /// Serialized input.
        pub input: serde_json::Value,
        /// Activity timeout.
        pub timeout: Span,
        /// Activity retry policy.
        pub retry_policy: task::RetryPolicy,
    }

    /// Claimed activity call.
    #[derive(Debug, Clone)]
    pub struct ClaimedActivityCall {
        /// Activity call ID.
        pub id: Uuid,
        /// Queue name.
        pub queue_name: String,
        /// Workflow run ID.
        pub workflow_run_id: workflow::WorkflowRunId,
        /// Workflow step index.
        pub step_index: usize,
        /// Deterministic call key.
        pub call_key: String,
        /// Activity name.
        pub activity: String,
        /// Serialized input.
        pub input: serde_json::Value,
        /// Current attempt count.
        pub attempt_count: i32,
        /// Current attempt number.
        pub attempt_number: i32,
    }

    /// Durable schedule row.
    #[derive(Debug, Clone)]
    pub struct StoredSchedule {
        /// Zoned schedule.
        pub schedule: scheduler::ZonedSchedule,
        /// Serialized task input template.
        pub input: serde_json::Value,
    }

    /// Long-lived scheduler lease.
    pub trait SchedulerLease: Send {
        /// Backend error type.
        type Error: std::error::Error + Send + Sync + 'static;

        /// Releases the lease.
        fn release(&mut self) -> impl Future<Output = StdResult<(), Self::Error>> + Send;
    }

    /// Backend transaction semantic API.
    pub trait BackendTransaction: Send {
        /// Backend error type.
        type Error: std::error::Error + Send + Sync + 'static;

        /// Commits the transaction.
        fn commit(self) -> impl Future<Output = StdResult<(), Self::Error>> + Send;

        /// Rolls back the transaction.
        fn rollback(self) -> impl Future<Output = StdResult<(), Self::Error>> + Send;

        /// Creates a queue and optional DLQ.
        fn create_queue(
            &mut self,
            name: &str,
            dlq_name: Option<&str>,
        ) -> impl Future<Output = StdResult<(), Self::Error>> + Send;

        /// Enqueues one task.
        fn enqueue_task(
            &mut self,
            queue_name: &str,
            task: EnqueueTask,
        ) -> impl Future<Output = StdResult<task::TaskId, Self::Error>> + Send;

        /// Claims the next available task.
        fn claim_task(
            &mut self,
            queue_name: &str,
        ) -> impl Future<Output = StdResult<Option<ClaimedTask>, Self::Error>> + Send;

        /// Marks the claimed task succeeded.
        fn mark_task_succeeded(
            &mut self,
            task: &ClaimedTask,
        ) -> impl Future<Output = StdResult<(), Self::Error>> + Send;

        /// Marks the claimed task waiting.
        fn mark_task_waiting(
            &mut self,
            task: &ClaimedTask,
        ) -> impl Future<Output = StdResult<(), Self::Error>> + Send;

        /// Records a task failure without terminalizing.
        fn record_task_failure(
            &mut self,
            task: &ClaimedTask,
            error: &task::Error,
        ) -> impl Future<Output = StdResult<(), Self::Error>> + Send;

        /// Requeues a task after delay.
        fn retry_task(
            &mut self,
            task: &ClaimedTask,
            delay: Span,
        ) -> impl Future<Output = StdResult<(), Self::Error>> + Send;

        /// Marks a task failed.
        fn mark_task_failed(
            &mut self,
            task: &ClaimedTask,
        ) -> impl Future<Output = StdResult<(), Self::Error>> + Send;

        /// Moves a task to a dead-letter queue.
        fn move_task_to_dlq(
            &mut self,
            task_id: task::TaskId,
            from_queue: &str,
            dlq_name: &str,
        ) -> impl Future<Output = StdResult<(), Self::Error>> + Send;

        /// Cancels pending tasks for a workflow run.
        fn cancel_workflow(
            &mut self,
            queue_name: &str,
            workflow_run_id: workflow::WorkflowRunId,
        ) -> impl Future<Output = StdResult<bool, Self::Error>> + Send;

        /// Stores or replaces a schedule.
        fn schedule_task(
            &mut self,
            queue_name: &str,
            schedule: scheduler::ZonedSchedule,
            input: serde_json::Value,
        ) -> impl Future<Output = StdResult<(), Self::Error>> + Send;

        /// Removes a schedule.
        fn unschedule_task(
            &mut self,
            queue_name: &str,
        ) -> impl Future<Output = StdResult<(), Self::Error>> + Send;

        /// Loads a schedule.
        fn task_schedule(
            &mut self,
            queue_name: &str,
        ) -> impl Future<Output = StdResult<Option<StoredSchedule>, Self::Error>> + Send;

        /// Loads activity calls for deterministic workflow replay.
        fn load_activity_calls(
            &mut self,
            queue_name: &str,
            workflow_run_id: workflow::WorkflowRunId,
            step_index: usize,
        ) -> impl Future<Output = StdResult<Vec<ActivityCallRecord>, Self::Error>> + Send;

        /// Persists an activity call intent idempotently.
        fn insert_activity_call(
            &mut self,
            queue_name: &str,
            workflow_run_id: workflow::WorkflowRunId,
            step_index: usize,
            command: ActivityCallCommand,
        ) -> impl Future<Output = StdResult<(), Self::Error>> + Send;

        /// Claims the next matching activity call.
        fn claim_activity_call(
            &mut self,
            queue_name: &str,
            activities: &[String],
        ) -> impl Future<Output = StdResult<Option<ClaimedActivityCall>, Self::Error>> + Send;

        /// Completes an activity call.
        fn complete_activity_call(
            &mut self,
            call: &ClaimedActivityCall,
            output: serde_json::Value,
        ) -> impl Future<Output = StdResult<(), Self::Error>> + Send;

        /// Requeues an activity call after a retryable failure.
        fn retry_activity_call(
            &mut self,
            call: &ClaimedActivityCall,
            error: activity::Error,
            delay: Span,
        ) -> impl Future<Output = StdResult<(), Self::Error>> + Send;

        /// Terminally fails an activity call.
        fn fail_activity_call(
            &mut self,
            call: &ClaimedActivityCall,
            error: activity::Error,
        ) -> impl Future<Output = StdResult<(), Self::Error>> + Send;

        /// Wakes a waiting workflow task after activity completion.
        fn wake_workflow_task(
            &mut self,
            queue_name: &str,
            workflow_run_id: workflow::WorkflowRunId,
            step_index: usize,
        ) -> impl Future<Output = StdResult<(), Self::Error>> + Send;

        /// Deletes expired terminal tasks.
        fn delete_expired(&mut self) -> impl Future<Output = StdResult<(), Self::Error>> + Send;
    }

    /// Durable backend.
    pub trait Backend: Clone + Send + Sync + 'static {
        /// Backend error type.
        type Error: std::error::Error + Send + Sync + 'static;

        /// Backend transaction type.
        type Transaction<'a>: BackendTransaction<Error = Self::Error> + Send
        where
            Self: 'a;

        /// Scheduler lease type.
        type SchedulerLease<'a>: SchedulerLease<Error = Self::Error> + Send
        where
            Self: 'a;

        /// Begins a transaction.
        fn begin(
            &self,
        ) -> impl Future<Output = StdResult<Self::Transaction<'_>, Self::Error>> + Send;

        /// Records a heartbeat outside the task transaction.
        fn record_heartbeat(
            &self,
            task: &ClaimedTask,
        ) -> impl Future<Output = StdResult<(), Self::Error>> + Send;

        /// Tries to acquire a singleton scheduler lease.
        fn try_acquire_scheduler(
            &self,
            queue_name: &str,
        ) -> impl Future<Output = StdResult<Option<Self::SchedulerLease<'_>>, Self::Error>> + Send;

        /// Waits for possible task work. Backends may implement this with
        /// notifications; polling remains authoritative.
        fn wait_for_task_change(
            &self,
            _queue_name: &str,
            timeout: StdDuration,
        ) -> impl Future<Output = StdResult<(), Self::Error>> + Send {
            async move {
                sleep(timeout).await;
                Ok(())
            }
        }

        /// Waits for possible activity work.
        fn wait_for_activity_change(
            &self,
            _queue_name: &str,
            timeout: StdDuration,
        ) -> impl Future<Output = StdResult<(), Self::Error>> + Send {
            async move {
                sleep(timeout).await;
                Ok(())
            }
        }
    }
}

/// Reusable backend contract checks.
#[cfg(any(test, feature = "contracts"))]
pub mod contracts;

/// Queue API.
pub mod queue {
    use super::*;

    /// Task queue.
    #[derive(Debug)]
    pub struct Queue<B, T>
    where
        B: backend::Backend,
        T: task::Task<B>,
    {
        pub(crate) name: String,
        pub(crate) dlq_name: Option<String>,
        pub(crate) backend: B,
        _marker: PhantomData<T>,
    }

    impl<B, T> Clone for Queue<B, T>
    where
        B: backend::Backend,
        T: task::Task<B>,
    {
        fn clone(&self) -> Self {
            Self {
                name: self.name.clone(),
                dlq_name: self.dlq_name.clone(),
                backend: self.backend.clone(),
                _marker: PhantomData,
            }
        }
    }

    impl<B, T> Queue<B, T>
    where
        B: backend::Backend,
        T: task::Task<B>,
    {
        /// Creates a new builder.
        pub fn builder() -> QueueBuilder<B, T, Initial> {
            QueueBuilder::default()
        }

        /// Queue name.
        pub fn name(&self) -> &str {
            &self.name
        }

        /// Backend handle.
        pub fn backend(&self) -> &B {
            &self.backend
        }

        /// Enqueues a task.
        pub async fn enqueue(&self, task: &T, input: &T::Input) -> Result<task::TaskId> {
            let mut tx = self.backend.begin().await.map_err(Error::backend)?;
            let id = self.enqueue_using(&mut tx, task, input).await?;
            tx.commit().await.map_err(Error::backend)?;
            Ok(id)
        }

        /// Enqueues a task after an additional delay.
        pub async fn enqueue_after(
            &self,
            task: &T,
            input: &T::Input,
            delay: Span,
        ) -> Result<task::TaskId> {
            let mut tx = self.backend.begin().await.map_err(Error::backend)?;
            let id = self
                .enqueue_after_using(&mut tx, task, input, delay)
                .await?;
            tx.commit().await.map_err(Error::backend)?;
            Ok(id)
        }

        /// Enqueues a task using an existing transaction.
        pub async fn enqueue_using(
            &self,
            tx: &mut B::Transaction<'_>,
            task: &T,
            input: &T::Input,
        ) -> Result<task::TaskId> {
            self.enqueue_after_using(tx, task, input, Span::new()).await
        }

        /// Enqueues a task after delay using an existing transaction.
        pub async fn enqueue_after_using(
            &self,
            tx: &mut B::Transaction<'_>,
            task: &T,
            input: &T::Input,
            delay: Span,
        ) -> Result<task::TaskId> {
            let config = task.config_for(input)?.with_additional_delay(delay)?;
            let command = backend::EnqueueTask {
                input: serde_json::to_value(input)?,
                config,
                delay: Span::new(),
            };
            tx.enqueue_task(&self.name, command)
                .await
                .map_err(Error::backend)
        }

        /// Enqueues many tasks.
        pub async fn enqueue_many(
            &self,
            task: &T,
            inputs: &[T::Input],
        ) -> Result<Vec<task::TaskId>> {
            let mut tx = self.backend.begin().await.map_err(Error::backend)?;
            let mut ids = Vec::with_capacity(inputs.len());
            for input in inputs {
                ids.push(self.enqueue_using(&mut tx, task, input).await?);
            }
            tx.commit().await.map_err(Error::backend)?;
            Ok(ids)
        }

        /// Dequeues the next available task.
        pub async fn dequeue(&self) -> Result<Option<backend::ClaimedTask>> {
            let mut tx = self.backend.begin().await.map_err(Error::backend)?;
            let task = tx.claim_task(&self.name).await.map_err(Error::backend)?;
            tx.commit().await.map_err(Error::backend)?;
            Ok(task)
        }

        /// Stores or replaces a schedule.
        pub async fn schedule(
            &self,
            schedule: &scheduler::ZonedSchedule,
            input: &T::Input,
        ) -> Result {
            let mut tx = self.backend.begin().await.map_err(Error::backend)?;
            self.schedule_using(&mut tx, schedule, input).await?;
            tx.commit().await.map_err(Error::backend)?;
            Ok(())
        }

        /// Stores or replaces a schedule using a transaction.
        pub async fn schedule_using(
            &self,
            tx: &mut B::Transaction<'_>,
            schedule: &scheduler::ZonedSchedule,
            input: &T::Input,
        ) -> Result {
            tx.schedule_task(&self.name, schedule.clone(), serde_json::to_value(input)?)
                .await
                .map_err(Error::backend)
        }

        /// Removes a schedule.
        pub async fn unschedule(&self) -> Result {
            let mut tx = self.backend.begin().await.map_err(Error::backend)?;
            self.unschedule_using(&mut tx).await?;
            tx.commit().await.map_err(Error::backend)?;
            Ok(())
        }

        /// Removes a schedule using a transaction.
        pub async fn unschedule_using(&self, tx: &mut B::Transaction<'_>) -> Result {
            tx.unschedule_task(&self.name).await.map_err(Error::backend)
        }

        pub(crate) async fn task_schedule(
            &self,
            tx: &mut B::Transaction<'_>,
        ) -> Result<Option<backend::StoredSchedule>> {
            tx.task_schedule(&self.name).await.map_err(Error::backend)
        }
    }

    /// Initial queue builder state.
    pub struct Initial;
    /// Name-set queue builder state.
    pub struct NameSet {
        name: String,
        dlq_name: Option<String>,
    }
    /// Backend-set queue builder state.
    pub struct BackendSet<B> {
        name: String,
        dlq_name: Option<String>,
        backend: B,
    }

    /// Queue builder.
    pub struct QueueBuilder<B, T, S>
    where
        B: backend::Backend,
        T: task::Task<B>,
    {
        state: S,
        _marker: PhantomData<(B, T)>,
    }

    impl<B, T> Default for QueueBuilder<B, T, Initial>
    where
        B: backend::Backend,
        T: task::Task<B>,
    {
        fn default() -> Self {
            Self {
                state: Initial,
                _marker: PhantomData,
            }
        }
    }

    impl<B, T> QueueBuilder<B, T, Initial>
    where
        B: backend::Backend,
        T: task::Task<B>,
    {
        /// Sets the queue name.
        pub fn name(self, name: impl Into<String>) -> QueueBuilder<B, T, NameSet> {
            QueueBuilder {
                state: NameSet {
                    name: name.into(),
                    dlq_name: None,
                },
                _marker: PhantomData,
            }
        }
    }

    impl<B, T> QueueBuilder<B, T, NameSet>
    where
        B: backend::Backend,
        T: task::Task<B>,
    {
        /// Sets a DLQ name.
        pub fn dead_letter_queue(mut self, dlq_name: impl Into<String>) -> Self {
            self.state.dlq_name = Some(dlq_name.into());
            self
        }

        /// Sets the backend.
        pub fn backend(self, backend: B) -> QueueBuilder<B, T, BackendSet<B>> {
            QueueBuilder {
                state: BackendSet {
                    name: self.state.name,
                    dlq_name: self.state.dlq_name,
                    backend,
                },
                _marker: PhantomData,
            }
        }
    }

    impl<B, T> QueueBuilder<B, T, BackendSet<B>>
    where
        B: backend::Backend,
        T: task::Task<B>,
    {
        /// Builds the queue.
        pub async fn build(self) -> Result<Queue<B, T>> {
            let state = self.state;
            let mut tx = state.backend.begin().await.map_err(Error::backend)?;
            tx.create_queue(&state.name, state.dlq_name.as_deref())
                .await
                .map_err(Error::backend)?;
            tx.commit().await.map_err(Error::backend)?;
            Ok(Queue {
                name: state.name,
                dlq_name: state.dlq_name,
                backend: state.backend,
                _marker: PhantomData,
            })
        }
    }

    /// Runs deletion clean up once.
    pub async fn run_deletion_once<B: backend::Backend>(backend: &B) -> Result {
        let mut tx = backend.begin().await.map_err(Error::backend)?;
        tx.delete_expired().await.map_err(Error::backend)?;
        tx.commit().await.map_err(Error::backend)?;
        Ok(())
    }

    /// Runs deletion clean up repeatedly.
    pub async fn run_deletion_every<B: backend::Backend>(backend: &B, period: Span) -> Result {
        let mut interval = tokio::time::interval(period.try_into()?);
        interval.tick().await;
        loop {
            run_deletion_once(backend).await?;
            interval.tick().await;
        }
    }
}

/// Workers execute tasks from queues.
pub mod worker {
    use super::*;

    /// Worker for one task type.
    pub struct Worker<B, T>
    where
        B: backend::Backend,
        T: task::Task<B>,
    {
        queue: Arc<queue::Queue<B, T>>,
        task: Arc<T>,
        concurrency_limit: usize,
        shutdown_token: CancellationToken,
    }

    impl<B, T> Clone for Worker<B, T>
    where
        B: backend::Backend,
        T: task::Task<B>,
    {
        fn clone(&self) -> Self {
            Self {
                queue: Arc::clone(&self.queue),
                task: Arc::clone(&self.task),
                concurrency_limit: self.concurrency_limit,
                shutdown_token: self.shutdown_token.clone(),
            }
        }
    }

    impl<B, T> Worker<B, T>
    where
        B: backend::Backend,
        T: task::Task<B>,
    {
        /// Creates a worker.
        pub fn new(queue: Arc<queue::Queue<B, T>>, task: T) -> Self {
            Self {
                queue,
                task: Arc::new(task),
                concurrency_limit: num_cpus::get(),
                shutdown_token: CancellationToken::new(),
            }
        }

        /// Sets concurrency limit.
        pub fn set_concurrency_limit(&mut self, concurrency_limit: usize) {
            self.concurrency_limit = concurrency_limit;
        }

        /// Sets shutdown token.
        pub fn set_shutdown_token(&mut self, shutdown_token: CancellationToken) {
            self.shutdown_token = shutdown_token;
        }

        /// Cancels shutdown token.
        pub fn shutdown(&self) {
            self.shutdown_token.cancel();
        }

        /// Runs the worker.
        pub async fn run(&self) -> Result {
            self.run_every(1.minute()).await
        }

        /// Runs the worker with polling period.
        pub async fn run_every(&self, period: Span) -> Result {
            let period: StdDuration = period.try_into()?;
            let mut tasks = JoinSet::new();

            loop {
                tokio::select! {
                    _ = self.shutdown_token.cancelled() => break,
                    maybe_joined = tasks.join_next(), if !tasks.is_empty() => {
                        if let Some(joined) = maybe_joined {
                            joined??;
                        }
                    }
                    waited = self.queue.backend.wait_for_task_change(&self.queue.name, period) => {
                        waited.map_err(Error::backend)?;
                        if tasks.len() < self.concurrency_limit {
                            let worker = self.clone();
                            tasks.spawn(async move {
                                while worker.process_next_task().await?.is_some() {
                                    if worker.shutdown_token.is_cancelled() {
                                        break;
                                    }
                                }
                                Ok::<_, Error>(())
                            });
                        }
                    }
                }
            }

            while let Some(joined) = tasks.join_next().await {
                joined??;
            }

            Ok(())
        }

        /// Processes the next available task.
        #[instrument(skip(self), fields(queue.name = self.queue.name), err)]
        pub async fn process_next_task(&self) -> Result<Option<task::TaskId>> {
            let mut tx = self.queue.backend.begin().await.map_err(Error::backend)?;
            let Some(claimed) = tx
                .claim_task(&self.queue.name)
                .await
                .map_err(Error::backend)?
            else {
                tx.commit().await.map_err(Error::backend)?;
                return Ok(None);
            };

            let input: T::Input = serde_json::from_value(claimed.input.clone())?;
            let timeout: StdDuration = claimed.config.timeout().try_into()?;
            let heartbeat: StdDuration = claimed.config.heartbeat().try_into()?;

            let heartbeat_task = tokio::spawn({
                let backend = self.queue.backend.clone();
                let claimed = claimed.clone();
                async move {
                    let mut interval = tokio::time::interval(heartbeat);
                    interval.tick().await;
                    loop {
                        if let Err(err) = backend.record_heartbeat(&claimed).await {
                            tracing::error!(%err, "failed to record heartbeat");
                        }
                        interval.tick().await;
                    }
                }
            });

            let result = tokio::time::timeout(timeout, self.task.execute(&mut tx, input)).await;
            let task_id = claimed.id;

            match result {
                Ok(Ok(_)) => {
                    tx.mark_task_succeeded(&claimed)
                        .await
                        .map_err(Error::backend)?;
                }
                Ok(Err(task::Error::Suspended(_))) => {
                    tx.mark_task_waiting(&claimed)
                        .await
                        .map_err(Error::backend)?;
                }
                Ok(Err(err)) => {
                    handle_task_error(
                        &mut tx,
                        &self.queue,
                        &claimed,
                        &claimed.config.retry_policy(),
                        &err,
                    )
                    .await?;
                }
                Err(_) => {
                    let err = task::Error::TimedOut(timeout.try_into()?);
                    handle_task_error(
                        &mut tx,
                        &self.queue,
                        &claimed,
                        &claimed.config.retry_policy(),
                        &err,
                    )
                    .await?;
                }
            }

            heartbeat_task.abort();
            tx.commit().await.map_err(Error::backend)?;

            Ok(Some(task_id))
        }
    }

    async fn handle_task_error<B, T>(
        tx: &mut B::Transaction<'_>,
        queue: &queue::Queue<B, T>,
        claimed: &backend::ClaimedTask,
        retry_policy: &task::RetryPolicy,
        err: &task::Error,
    ) -> Result
    where
        B: backend::Backend,
        T: task::Task<B>,
    {
        if matches!(err, task::Error::Fatal(_)) {
            tx.mark_task_failed(claimed).await.map_err(Error::backend)?;
            if let Some(dlq_name) = &queue.dlq_name {
                tx.move_task_to_dlq(claimed.id, &queue.name, dlq_name)
                    .await
                    .map_err(Error::backend)?;
            }
            return Ok(());
        }

        tx.record_task_failure(claimed, err)
            .await
            .map_err(Error::backend)?;

        if claimed.attempt_number < retry_policy.max_attempts {
            let delay = retry_policy.calculate_delay(claimed.attempt_number);
            tx.retry_task(claimed, delay)
                .await
                .map_err(Error::backend)?;
        } else {
            tx.mark_task_failed(claimed).await.map_err(Error::backend)?;
            if let Some(dlq_name) = &queue.dlq_name {
                tx.move_task_to_dlq(claimed.id, &queue.name, dlq_name)
                    .await
                    .map_err(Error::backend)?;
            }
        }

        Ok(())
    }
}

/// Cron scheduler.
pub mod scheduler {
    use std::str::FromStr;

    use jiff::tz::TimeZone;

    use super::*;

    /// Scheduler for task schedules.
    pub struct Scheduler<B, T>
    where
        B: backend::Backend,
        T: task::Task<B>,
    {
        queue: Arc<queue::Queue<B, T>>,
        task: Arc<T>,
        shutdown_token: CancellationToken,
    }

    impl<B, T> Scheduler<B, T>
    where
        B: backend::Backend,
        T: task::Task<B>,
    {
        /// Creates a scheduler.
        pub fn new(queue: Arc<queue::Queue<B, T>>, task: T) -> Self {
            Self {
                queue,
                task: Arc::new(task),
                shutdown_token: CancellationToken::new(),
            }
        }

        /// Sets shutdown token.
        pub fn set_shutdown_token(&mut self, shutdown_token: CancellationToken) {
            self.shutdown_token = shutdown_token;
        }

        /// Stops this scheduler.
        pub fn shutdown(&self) {
            self.shutdown_token.cancel();
        }

        /// Runs the scheduler.
        pub async fn run(&self) -> Result {
            let Some(mut lease) = self
                .queue
                .backend
                .try_acquire_scheduler(&self.queue.name)
                .await
                .map_err(Error::backend)?
            else {
                return Ok(());
            };

            let refresh = StdDuration::from_secs(5);

            loop {
                let mut tx = self.queue.backend.begin().await.map_err(Error::backend)?;
                let schedule = self.queue.task_schedule(&mut tx).await?;
                tx.commit().await.map_err(Error::backend)?;

                let Some(stored) = schedule else {
                    break;
                };

                let Some(next) = stored.schedule.iter().next() else {
                    sleep(refresh).await;
                    continue;
                };

                let tz = next.time_zone();
                let now = Zoned::now().with_time_zone(tz.to_owned());
                let until_next = next.duration_until(&now).unsigned_abs();
                let should_dispatch = until_next <= refresh;
                let wait_for = if should_dispatch { until_next } else { refresh };

                tokio::select! {
                    _ = self.shutdown_token.cancelled() => break,
                    _ = sleep(wait_for) => {
                        if should_dispatch {
                            let input: T::Input = serde_json::from_value(stored.input.clone())?;
                            self.queue.enqueue(&self.task, &input).await?;
                        }
                    }
                }
            }

            lease.release().await.map_err(Error::backend)?;
            Ok(())
        }
    }

    /// Schedule paired with a time zone.
    #[derive(Debug, Clone, PartialEq)]
    pub struct ZonedSchedule {
        schedule: Schedule,
        timezone: TimeZone,
    }

    impl ZonedSchedule {
        /// Creates a schedule.
        pub fn new(cron_expr: &str, time_zone_name: &str) -> StdResult<Self, ZonedScheduleError> {
            let schedule = cron_expr.parse()?;
            let timezone = TimeZone::get(time_zone_name)?;
            if timezone.iana_name().is_none() {
                return Err(ZonedScheduleError::Parse(
                    "time zone must use an IANA name".to_string(),
                ));
            }
            Ok(Self { schedule, timezone })
        }

        /// Cron expression string.
        pub fn cron_expr(&self) -> String {
            self.schedule.to_string()
        }

        /// IANA time zone name.
        pub fn iana_name(&self) -> &str {
            self.timezone.iana_name().expect("validated by constructor")
        }

        /// Upcoming schedule iterator.
        pub fn iter(&self) -> ZonedScheduleIterator<'_> {
            ZonedScheduleIterator {
                upcoming: self.schedule.upcoming(self.timezone.clone()),
            }
        }
    }

    /// Upcoming schedule iterator.
    pub struct ZonedScheduleIterator<'a> {
        upcoming: ScheduleIterator<'a>,
    }

    impl Iterator for ZonedScheduleIterator<'_> {
        type Item = Zoned;

        fn next(&mut self) -> Option<Self::Item> {
            self.upcoming.next()
        }
    }

    /// Zoned schedule parsing errors.
    #[derive(Debug, thiserror::Error)]
    pub enum ZonedScheduleError {
        /// Jiff error.
        #[error(transparent)]
        Jiff(#[from] jiff::Error),
        /// Cron error.
        #[error(transparent)]
        Cron(#[from] jiff_cron::error::Error),
        /// Parse error.
        #[error("Parsing error: {0}")]
        Parse(String),
    }

    impl FromStr for ZonedSchedule {
        type Err = ZonedScheduleError;

        fn from_str(s: &str) -> StdResult<Self, Self::Err> {
            if !s.ends_with(']') {
                return Err(ZonedScheduleError::Parse("Missing closing ']'".to_string()));
            }
            let open = s
                .find('[')
                .ok_or_else(|| ZonedScheduleError::Parse("Missing opening '['".to_string()))?;
            Self::new(&s[..open], &s[open + 1..s.len() - 1])
        }
    }
}

mod activity_worker {
    use super::*;

    trait ActivityHandler: Send + Sync {
        fn name(&self) -> &'static str;
        fn retry_policy(&self) -> task::RetryPolicy;
        fn timeout(&self) -> Span;
        fn execute_json<'a>(
            &'a self,
            input: serde_json::Value,
        ) -> Pin<Box<dyn Future<Output = activity::Result<serde_json::Value>> + Send + 'a>>;
    }

    struct RegisteredActivity<A: activity::Activity> {
        inner: A,
    }

    impl<A: activity::Activity> ActivityHandler for RegisteredActivity<A> {
        fn name(&self) -> &'static str {
            A::NAME
        }

        fn retry_policy(&self) -> task::RetryPolicy {
            self.inner.retry_policy()
        }

        fn timeout(&self) -> Span {
            self.inner.timeout()
        }

        fn execute_json<'a>(
            &'a self,
            input: serde_json::Value,
        ) -> Pin<Box<dyn Future<Output = activity::Result<serde_json::Value>> + Send + 'a>>
        {
            Box::pin(async move {
                let input: A::Input = serde_json::from_value(input).map_err(|err| {
                    activity::Error::fatal(
                        "deserialize_input",
                        format!(
                            "Failed to deserialize input for activity `{}`: {err}",
                            A::NAME
                        ),
                    )
                })?;
                let output = self.inner.execute(input).await?;
                serde_json::to_value(output).map_err(|err| {
                    activity::Error::fatal(
                        "serialize_output",
                        format!(
                            "Failed to serialize output for activity `{}`: {err}",
                            A::NAME
                        ),
                    )
                })
            })
        }
    }

    #[derive(Clone, Default)]
    pub(crate) struct ActivityRegistry {
        handlers: HashMap<String, Arc<dyn ActivityHandler>>,
    }

    impl ActivityRegistry {
        pub(crate) fn register<A: activity::Activity>(&mut self, activity: A) {
            self.handlers.insert(
                A::NAME.to_string(),
                Arc::new(RegisteredActivity { inner: activity }),
            );
        }

        pub(crate) fn is_empty(&self) -> bool {
            self.handlers.is_empty()
        }

        pub(crate) fn call_policy(&self, activity: &str) -> Option<(task::RetryPolicy, Span)> {
            self.handlers
                .get(activity)
                .map(|handler| (handler.retry_policy(), handler.timeout()))
        }
    }

    #[derive(Clone)]
    pub(crate) struct ActivityWorker<B: backend::Backend> {
        backend: B,
        queue_name: String,
        registry: ActivityRegistry,
        shutdown_token: CancellationToken,
    }

    impl<B: backend::Backend> ActivityWorker<B> {
        pub(crate) fn with_registry(
            backend: B,
            queue_name: String,
            registry: ActivityRegistry,
        ) -> Self {
            Self {
                backend,
                queue_name,
                registry,
                shutdown_token: CancellationToken::new(),
            }
        }

        pub(crate) fn set_shutdown_token(&mut self, shutdown_token: CancellationToken) {
            self.shutdown_token = shutdown_token;
        }

        pub(crate) fn is_empty(&self) -> bool {
            self.registry.is_empty()
        }

        pub(crate) async fn run(&self) -> Result {
            self.run_every(1.second()).await
        }

        pub(crate) async fn run_every(&self, period: Span) -> Result {
            let period: StdDuration = period.try_into()?;
            loop {
                tokio::select! {
                    _ = self.shutdown_token.cancelled() => return Ok(()),
                    waited = self.backend.wait_for_activity_change(&self.queue_name, period) => {
                        waited.map_err(Error::backend)?;
                        self.drain_pending_calls().await?;
                    }
                }
            }
        }

        async fn drain_pending_calls(&self) -> Result {
            while self.process_next_call().await?.is_some() {}
            Ok(())
        }

        async fn process_next_call(&self) -> Result<Option<Uuid>> {
            let activities = self.activity_claim_specs();
            if activities.is_empty() {
                return Ok(None);
            }

            let mut tx = self.backend.begin().await.map_err(Error::backend)?;
            let call = tx
                .claim_activity_call(&self.queue_name, &activities)
                .await
                .map_err(Error::backend)?;
            tx.commit().await.map_err(Error::backend)?;

            let Some(call) = call else {
                return Ok(None);
            };

            let Some(handler) = self.registry.handlers.get(&call.activity).cloned() else {
                let err = activity::Error::fatal(
                    "activity_not_registered",
                    format!("No activity handler registered for `{}`", call.activity),
                );
                let mut tx = self.backend.begin().await.map_err(Error::backend)?;
                tx.fail_activity_call(&call, err)
                    .await
                    .map_err(Error::backend)?;
                tx.wake_workflow_task(&call.queue_name, call.workflow_run_id, call.step_index)
                    .await
                    .map_err(Error::backend)?;
                tx.commit().await.map_err(Error::backend)?;
                return Ok(Some(call.id));
            };

            let timeout: StdDuration = handler.timeout().try_into()?;
            let result =
                tokio::time::timeout(timeout, handler.execute_json(call.input.clone())).await;

            let mut tx = self.backend.begin().await.map_err(Error::backend)?;
            match result {
                Ok(Ok(output)) => {
                    tx.complete_activity_call(&call, output)
                        .await
                        .map_err(Error::backend)?;
                    tx.wake_workflow_task(&call.queue_name, call.workflow_run_id, call.step_index)
                        .await
                        .map_err(Error::backend)?;
                }
                Ok(Err(err)) => {
                    self.handle_activity_error(&mut tx, &call, handler.retry_policy(), err)
                        .await?;
                }
                Err(_) => {
                    let err = activity::Error::retryable(
                        "activity_timeout",
                        format!("Activity `{}` timed out", handler.name()),
                    );
                    self.handle_activity_error(&mut tx, &call, handler.retry_policy(), err)
                        .await?;
                }
            }
            tx.commit().await.map_err(Error::backend)?;

            Ok(Some(call.id))
        }

        async fn handle_activity_error(
            &self,
            tx: &mut B::Transaction<'_>,
            call: &backend::ClaimedActivityCall,
            retry_policy: task::RetryPolicy,
            err: activity::Error,
        ) -> Result {
            if err.retryable && call.attempt_count < retry_policy.max_attempts {
                let delay = retry_policy.calculate_delay(call.attempt_count);
                tx.retry_activity_call(call, err, delay)
                    .await
                    .map_err(Error::backend)?;
            } else {
                tx.fail_activity_call(call, err)
                    .await
                    .map_err(Error::backend)?;
                tx.wake_workflow_task(&call.queue_name, call.workflow_run_id, call.step_index)
                    .await
                    .map_err(Error::backend)?;
            }
            Ok(())
        }

        fn activity_claim_specs(&self) -> Vec<String> {
            let mut activities = self.registry.handlers.keys().cloned().collect::<Vec<_>>();
            activities.sort();
            activities
        }
    }
}

/// Workflow API.
pub mod workflow {
    use super::*;

    pub(crate) mod registration {
        use super::*;

        /// Marker for no activities.
        pub struct NoActivities;
        /// Type-level registered set.
        pub struct Registered<Head, Tail>(pub(crate) PhantomData<(Head, Tail)>);
        /// Type-level head index.
        pub struct Here;
        /// Type-level tail index.
        pub struct There<T>(pub(crate) PhantomData<T>);

        mod sealed {
            pub trait ActivitySet {}
        }

        /// Type-level set of registered activities.
        pub trait ActivitySet: sealed::ActivitySet + 'static {}

        impl<T> ActivitySet for T where T: sealed::ActivitySet + 'static {}
        impl sealed::ActivitySet for NoActivities {}
        impl<Head, Tail> sealed::ActivitySet for Registered<Head, Tail>
        where
            Head: activity::Activity,
            Tail: ActivitySet,
        {
        }

        mod private {
            use super::{activity, Here, Registered, There};

            pub trait Contains<A: activity::Activity, Idx> {}
            impl<A, Tail> Contains<A, Here> for Registered<A, Tail> where A: activity::Activity {}
            impl<A, Head, Tail, Idx> Contains<A, There<Idx>> for Registered<Head, Tail>
            where
                A: activity::Activity,
                Head: activity::Activity,
                Tail: Contains<A, Idx>,
            {
            }
        }

        /// Marker trait indicating activity membership.
        pub trait Contains<A: activity::Activity, Idx>: ActivitySet {}
        impl<Set, A, Idx> Contains<A, Idx> for Set
        where
            Set: ActivitySet + private::Contains<A, Idx>,
            A: activity::Activity,
        {
        }
    }

    use activity_worker::ActivityRegistry;
    use registration::{Contains, NoActivities, Registered};

    /// Unique identifier of a workflow run.
    #[derive(Debug, Clone, Copy, Serialize, Deserialize, Hash, Eq, PartialEq)]
    pub struct WorkflowRunId(Uuid);

    impl WorkflowRunId {
        /// Generates a new workflow run ID.
        pub fn new() -> Self {
            Self(Ulid::new().into())
        }

        /// Reconstitutes from UUID.
        pub const fn from_uuid(id: Uuid) -> Self {
            Self(id)
        }

        /// Returns UUID.
        pub const fn as_uuid(self) -> Uuid {
            self.0
        }

        /// Returns hyphenated UUID formatting.
        pub fn as_hyphenated(&self) -> &uuid::fmt::Hyphenated {
            self.0.as_hyphenated()
        }
    }

    impl Default for WorkflowRunId {
        fn default() -> Self {
            Self::new()
        }
    }

    impl Deref for WorkflowRunId {
        type Target = Uuid;

        fn deref(&self) -> &Self::Target {
            &self.0
        }
    }

    impl Display for WorkflowRunId {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            write!(f, "{}", self.0)
        }
    }

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    /// Persisted workflow task input used internally by workflow queues.
    pub struct WorkflowState {
        step_index: usize,
        step_input: serde_json::Value,
        #[serde(default = "WorkflowRunId::new")]
        workflow_run_id: WorkflowRunId,
    }

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    struct WorkflowScheduleTemplate {
        step_index: usize,
        step_input: serde_json::Value,
    }

    /// Enqueued workflow handle.
    pub struct EnqueuedWorkflow<B, T>
    where
        B: backend::Backend,
        T: task::Task<B>,
    {
        run_id: WorkflowRunId,
        queue: Arc<queue::Queue<B, T>>,
    }

    impl<B, T> EnqueuedWorkflow<B, T>
    where
        B: backend::Backend,
        T: task::Task<B>,
    {
        /// Workflow run ID.
        pub fn run_id(&self) -> WorkflowRunId {
            self.run_id
        }

        /// Cancels pending workflow tasks.
        pub async fn cancel(&self) -> Result<bool> {
            let mut tx = self.queue.backend.begin().await.map_err(Error::backend)?;
            let cancelled = tx
                .cancel_workflow(&self.queue.name, self.run_id)
                .await
                .map_err(Error::backend)?;
            tx.commit().await.map_err(Error::backend)?;
            Ok(cancelled)
        }
    }

    /// Context passed to workflow steps.
    pub struct Context<S, Set = NoActivities> {
        /// Shared, non-durable workflow state.
        pub state: S,
        /// Current zero-based step index.
        pub step_index: usize,
        /// Total step count.
        pub step_count: usize,
        /// Workflow run ID.
        pub workflow_run_id: WorkflowRunId,
        /// Queue name.
        pub queue_name: String,
        activity_call_buffer: Arc<Mutex<ActivityCallBuffer>>,
        call_sequence_state: Arc<Mutex<CallSequenceState>>,
        _activity_set: PhantomData<fn() -> Set>,
    }

    struct ContextParts<S> {
        state: S,
        step_index: usize,
        step_count: usize,
        workflow_run_id: WorkflowRunId,
        queue_name: String,
        activity_call_buffer: Arc<Mutex<ActivityCallBuffer>>,
        call_sequence_state: Arc<Mutex<CallSequenceState>>,
    }

    #[derive(Default)]
    struct ActivityCallBuffer {
        records: HashMap<String, backend::ActivityCallRecord>,
        commands: Vec<ActivityCallCommand>,
    }

    #[derive(Clone)]
    struct ActivityCallCommand {
        call_key: String,
        activity: String,
        input: serde_json::Value,
    }

    #[derive(Default)]
    struct CallSequenceState {
        unresolved_call_key: Option<String>,
        next_activity_call_index: usize,
    }

    impl ActivityCallBuffer {
        fn new(records: Vec<backend::ActivityCallRecord>) -> Self {
            Self {
                records: records
                    .into_iter()
                    .map(|record| (record.call_key.clone(), record))
                    .collect(),
                commands: Vec::new(),
            }
        }

        fn register_call(
            &mut self,
            key: &str,
            activity: &str,
            input: &serde_json::Value,
        ) -> task::Result<backend::ActivityCallRecord> {
            if let Some(record) = self.records.get(key) {
                if record.activity != activity {
                    return Err(task::Error::Fatal(format!(
                        "Non-deterministic activity call detected: key `{key}` was previously \
                         bound to `{}` but now `{activity}`.",
                        record.activity
                    )));
                }
                if record.input != *input {
                    return Err(task::Error::Fatal(format!(
                        "Non-deterministic activity call input detected for key `{key}`."
                    )));
                }
                return Ok(record.clone());
            }

            let record = backend::ActivityCallRecord {
                call_key: key.to_string(),
                activity: activity.to_string(),
                input: input.clone(),
                output: None,
                error: None,
                state: activity::CallState::Pending,
            };
            self.records.insert(key.to_string(), record.clone());
            self.commands.push(ActivityCallCommand {
                call_key: key.to_string(),
                activity: activity.to_string(),
                input: input.clone(),
            });
            Ok(record)
        }

        fn drain_commands(&mut self) -> Vec<ActivityCallCommand> {
            std::mem::take(&mut self.commands)
        }
    }

    impl<S, Set> Context<S, Set> {
        fn from_parts(parts: ContextParts<S>) -> Self {
            Self {
                state: parts.state,
                step_index: parts.step_index,
                step_count: parts.step_count,
                workflow_run_id: parts.workflow_run_id,
                queue_name: parts.queue_name,
                activity_call_buffer: parts.activity_call_buffer,
                call_sequence_state: parts.call_sequence_state,
                _activity_set: PhantomData,
            }
        }

        pub(crate) fn emit_indexed<A, Idx>(&mut self, input: &A::Input) -> task::Result<()>
        where
            A: activity::Activity,
            Set: Contains<A, Idx>,
        {
            let key = self.next_activity_call_key()?;
            let input =
                serde_json::to_value(input).map_err(|err| task::Error::Fatal(err.to_string()))?;
            self.register_activity_call(&key, A::NAME, &input)?;
            Ok(())
        }

        pub(crate) fn call_indexed<A, Idx>(&mut self, input: &A::Input) -> task::Result<A::Output>
        where
            A: activity::Activity,
            Set: Contains<A, Idx>,
        {
            let key = self.next_activity_call_key()?;
            let input =
                serde_json::to_value(input).map_err(|err| task::Error::Fatal(err.to_string()))?;
            let record = self.register_activity_call(&key, A::NAME, &input)?;

            match record.state {
                activity::CallState::Succeeded => {
                    let output = record.output.ok_or_else(|| {
                        task::Error::Fatal(format!(
                            "Missing output for activity call `{}` with key `{key}`.",
                            A::NAME
                        ))
                    })?;
                    serde_json::from_value(output)
                        .map_err(|err| task::Error::Fatal(err.to_string()))
                }
                activity::CallState::Failed => {
                    let error = record
                        .error
                        .and_then(|value| serde_json::from_value::<activity::Error>(value).ok())
                        .unwrap_or_else(|| {
                            activity::Error::fatal(
                                "activity_call_failed",
                                format!("Activity call `{}` with key `{key}` failed.", A::NAME),
                            )
                        });
                    if error.retryable {
                        Err(task::Error::Retryable(error.to_string()))
                    } else {
                        Err(task::Error::Fatal(error.to_string()))
                    }
                }
                activity::CallState::Pending | activity::CallState::InProgress => {
                    self.register_call_sequence_wait(&key)?;
                    Err(task::Error::Suspended(format!(
                        "Waiting on activity call `{}` with key `{key}`.",
                        A::NAME
                    )))
                }
            }
        }

        fn next_activity_call_key(&self) -> task::Result<String> {
            let mut state = self.call_sequence_state.lock().map_err(|_| {
                task::Error::Fatal("Call sequence state lock was poisoned.".to_string())
            })?;
            let operation_index = state.next_activity_call_index;
            state.next_activity_call_index = state
                .next_activity_call_index
                .checked_add(1)
                .ok_or_else(|| {
                    task::Error::Fatal("Activity operation index overflowed.".to_string())
                })?;
            Ok(format!(
                "activity:{}:{}:{operation_index}",
                self.workflow_run_id.as_hyphenated(),
                self.step_index
            ))
        }

        fn register_activity_call(
            &self,
            key: &str,
            activity: &str,
            input: &serde_json::Value,
        ) -> task::Result<backend::ActivityCallRecord> {
            let mut buffer = self.activity_call_buffer.lock().map_err(|_| {
                task::Error::Fatal("Activity call buffer lock was poisoned.".to_string())
            })?;
            buffer.register_call(key, activity, input)
        }

        fn register_call_sequence_wait(&self, key: &str) -> task::Result<()> {
            let mut state = self.call_sequence_state.lock().map_err(|_| {
                task::Error::Fatal("Call sequence state lock was poisoned.".to_string())
            })?;
            if let Some(existing) = &state.unresolved_call_key {
                if existing != key {
                    return Err(task::Error::Fatal(
                        "Multiple unresolved `call` operations in one step execution are not \
                         supported."
                            .to_string(),
                    ));
                }
            } else {
                state.unresolved_call_key = Some(key.to_string());
            }
            Ok(())
        }
    }

    /// Activity invocation helpers.
    pub trait InvokeActivity: activity::Activity + Sized {
        /// Emits a durable fire-and-forget activity call.
        fn emit<'a, S, Set, Idx>(
            cx: &'a mut Context<S, Set>,
            input: &'a Self::Input,
        ) -> impl Future<Output = task::Result<()>> + Send + 'a
        where
            Set: Contains<Self, Idx>,
            S: Send + 'a;

        /// Calls an activity and returns its durable output.
        fn call<'a, S, Set, Idx>(
            cx: &'a mut Context<S, Set>,
            input: &'a Self::Input,
        ) -> impl Future<Output = task::Result<Self::Output>> + Send + 'a
        where
            Set: Contains<Self, Idx>,
            S: Send + 'a;
    }

    impl<A> InvokeActivity for A
    where
        A: activity::Activity,
    {
        fn emit<'a, S, Set, Idx>(
            cx: &'a mut Context<S, Set>,
            input: &'a Self::Input,
        ) -> impl Future<Output = task::Result<()>> + Send + 'a
        where
            Set: Contains<Self, Idx>,
            S: Send + 'a,
        {
            std::future::ready(cx.emit_indexed::<Self, Idx>(input))
        }

        fn call<'a, S, Set, Idx>(
            cx: &'a mut Context<S, Set>,
            input: &'a Self::Input,
        ) -> impl Future<Output = task::Result<Self::Output>> + Send + 'a
        where
            Set: Contains<Self, Idx>,
            S: Send + 'a,
        {
            std::future::ready(cx.call_indexed::<Self, Idx>(input))
        }
    }

    /// Step transition.
    #[derive(Deserialize, Serialize)]
    pub enum Transition<N> {
        /// Continue with next step payload after delay.
        Continue {
            /// Next step payload.
            next: N,
            /// Delay before next step runs.
            delay: Span,
        },
        /// Complete workflow.
        Complete,
    }

    impl<N> Transition<N> {
        /// Continue to next step.
        pub fn next(next: N) -> task::Result<Self> {
            Ok(Self::Continue {
                next,
                delay: Span::new(),
            })
        }

        /// Continue after delay.
        pub fn after(next: N, delay: Span) -> task::Result<Self> {
            Ok(Self::Continue { next, delay })
        }
    }

    impl Transition<()> {
        /// Complete workflow.
        pub fn complete() -> task::Result<Self> {
            Ok(Self::Complete)
        }
    }

    struct StepConfig<S> {
        executor: Box<dyn StepExecutor<S>>,
        task_config: task::TaskConfig,
    }

    impl<S> StepConfig<S> {
        fn new<I, O, A, F, Fut>(func: F) -> Self
        where
            I: DeserializeOwned + Serialize + Send + Sync + 'static,
            O: Serialize + Send + Sync + 'static,
            S: Send + Sync + 'static,
            A: 'static,
            F: Fn(Context<S, A>, I) -> Fut + Send + Sync + 'static,
            Fut: Future<Output = task::Result<Transition<O>>> + Send + 'static,
        {
            let step_fn = StepFn::new(move |cx, input| {
                Box::pin(func(cx, input))
                    as Pin<Box<dyn Future<Output = task::Result<Transition<O>>> + Send>>
            });
            Self {
                executor: Box::new(step_fn),
                task_config: task::TaskConfig::default(),
            }
        }
    }

    type StepResult = task::Result<Option<(serde_json::Value, Span)>>;

    trait StepExecutor<S>: Send + Sync {
        fn execute_step(
            &self,
            cx: ContextParts<S>,
            input: serde_json::Value,
        ) -> Pin<Box<dyn Future<Output = StepResult> + Send>>;
    }

    type StepFnMarker<I, O, S, A> = fn() -> (I, O, S, A);

    struct StepFn<I, O, S, A, F> {
        func: Arc<F>,
        _marker: PhantomData<StepFnMarker<I, O, S, A>>,
    }

    impl<I, O, S, A, F> StepFn<I, O, S, A, F> {
        fn new(func: F) -> Self {
            Self {
                func: Arc::new(func),
                _marker: PhantomData,
            }
        }
    }

    impl<I, O, S, A, F> StepExecutor<S> for StepFn<I, O, S, A, F>
    where
        I: DeserializeOwned + Serialize + Send + Sync + 'static,
        O: Serialize + Send + Sync + 'static,
        S: Send + Sync + 'static,
        A: 'static,
        F: Fn(
                Context<S, A>,
                I,
            ) -> Pin<Box<dyn Future<Output = task::Result<Transition<O>>> + Send>>
            + Send
            + Sync
            + 'static,
    {
        fn execute_step(
            &self,
            cx: ContextParts<S>,
            input: serde_json::Value,
        ) -> Pin<Box<dyn Future<Output = StepResult> + Send>> {
            let input: I = match serde_json::from_value(input) {
                Ok(input) => input,
                Err(err) => {
                    return Box::pin(async move { Err(task::Error::Fatal(err.to_string())) })
                }
            };
            let cx: Context<S, A> = Context::from_parts(cx);
            let fut = (self.func)(cx, input);
            Box::pin(async move {
                match fut.await? {
                    Transition::Continue { next, delay } => {
                        let next = serde_json::to_value(next)
                            .map_err(|err| task::Error::Fatal(err.to_string()))?;
                        Ok(Some((next, delay)))
                    }
                    Transition::Complete => Ok(None),
                }
            })
        }
    }

    /// Sequential typed workflow.
    pub struct Workflow<B, I, S = ()>
    where
        B: backend::Backend,
        I: Send + Sync + 'static,
        S: Clone + Send + Sync + 'static,
    {
        queue: Arc<queue::Queue<B, Self>>,
        steps: Arc<Vec<StepConfig<S>>>,
        state: S,
        activity_registry: ActivityRegistry,
        _marker: PhantomData<I>,
    }

    impl<B, I> Workflow<B, I, ()>
    where
        B: backend::Backend,
        I: Serialize + Send + Sync + 'static,
    {
        /// Creates a workflow builder.
        pub fn builder() -> Builder<B, I, I, (), Initial, NoActivities> {
            Builder::new()
        }
    }

    impl<B, I, S> Clone for Workflow<B, I, S>
    where
        B: backend::Backend,
        I: Send + Sync + 'static,
        S: Clone + Send + Sync + 'static,
    {
        fn clone(&self) -> Self {
            Self {
                queue: Arc::clone(&self.queue),
                steps: Arc::clone(&self.steps),
                state: self.state.clone(),
                activity_registry: self.activity_registry.clone(),
                _marker: PhantomData,
            }
        }
    }

    impl<B, I, S> Workflow<B, I, S>
    where
        B: backend::Backend,
        I: Serialize + Send + Sync + 'static,
        S: Clone + Send + Sync + 'static,
    {
        /// Enqueues a workflow.
        pub async fn enqueue(&self, input: &I) -> Result<EnqueuedWorkflow<B, Self>> {
            let mut tx = self.queue.backend.begin().await.map_err(Error::backend)?;
            let enqueued = self.enqueue_using(&mut tx, input).await?;
            tx.commit().await.map_err(Error::backend)?;
            Ok(enqueued)
        }

        /// Enqueues many workflows.
        pub async fn enqueue_many(&self, inputs: &[I]) -> Result<Vec<EnqueuedWorkflow<B, Self>>> {
            let mut tx = self.queue.backend.begin().await.map_err(Error::backend)?;
            let mut enqueued = Vec::with_capacity(inputs.len());
            for input in inputs {
                enqueued.push(self.enqueue_using(&mut tx, input).await?);
            }
            tx.commit().await.map_err(Error::backend)?;
            Ok(enqueued)
        }

        /// Enqueues a workflow using an existing transaction.
        pub async fn enqueue_using(
            &self,
            tx: &mut B::Transaction<'_>,
            input: &I,
        ) -> Result<EnqueuedWorkflow<B, Self>> {
            self.enqueue_after_using(tx, input, Span::new()).await
        }

        /// Enqueues a workflow after a delay.
        pub async fn enqueue_after(
            &self,
            input: &I,
            delay: Span,
        ) -> Result<EnqueuedWorkflow<B, Self>> {
            let mut tx = self.queue.backend.begin().await.map_err(Error::backend)?;
            let enqueued = self.enqueue_after_using(&mut tx, input, delay).await?;
            tx.commit().await.map_err(Error::backend)?;
            Ok(enqueued)
        }

        /// Enqueues a workflow after delay using an existing transaction.
        pub async fn enqueue_after_using(
            &self,
            tx: &mut B::Transaction<'_>,
            input: &I,
            delay: Span,
        ) -> Result<EnqueuedWorkflow<B, Self>> {
            let workflow_input = self.first_workflow_input(input)?;
            self.queue
                .enqueue_after_using(tx, self, &workflow_input, delay)
                .await?;
            Ok(EnqueuedWorkflow {
                run_id: workflow_input.workflow_run_id,
                queue: Arc::clone(&self.queue),
            })
        }

        /// Stores or replaces a workflow schedule.
        pub async fn schedule(&self, schedule: &scheduler::ZonedSchedule, input: &I) -> Result {
            let mut tx = self.queue.backend.begin().await.map_err(Error::backend)?;
            self.schedule_using(&mut tx, schedule, input).await?;
            tx.commit().await.map_err(Error::backend)?;
            Ok(())
        }

        /// Stores or replaces a workflow schedule using a transaction.
        pub async fn schedule_using(
            &self,
            tx: &mut B::Transaction<'_>,
            schedule: &scheduler::ZonedSchedule,
            input: &I,
        ) -> Result {
            let template = self.first_workflow_schedule_template(input)?;
            tx.schedule_task(
                &self.queue.name,
                schedule.clone(),
                serde_json::to_value(template)?,
            )
            .await
            .map_err(Error::backend)
        }

        /// Removes workflow schedule.
        pub async fn unschedule(&self) -> Result {
            self.queue.unschedule().await
        }

        /// Removes workflow schedule using a transaction.
        pub async fn unschedule_using(&self, tx: &mut B::Transaction<'_>) -> Result {
            self.queue.unschedule_using(tx).await
        }

        /// Returns queue.
        pub fn queue(&self) -> Arc<queue::Queue<B, Self>> {
            Arc::clone(&self.queue)
        }

        pub(crate) fn activity_registry(&self) -> ActivityRegistry {
            self.activity_registry.clone()
        }

        /// Returns runtime.
        pub fn runtime(&self) -> runtime::Runtime<B, I, S> {
            runtime::Runtime::new(self.clone())
        }

        fn first_workflow_input(&self, input: &I) -> Result<WorkflowState> {
            Ok(WorkflowState {
                step_index: 0,
                step_input: serde_json::to_value(input)?,
                workflow_run_id: WorkflowRunId::new(),
            })
        }

        fn first_workflow_schedule_template(&self, input: &I) -> Result<WorkflowScheduleTemplate> {
            Ok(WorkflowScheduleTemplate {
                step_index: 0,
                step_input: serde_json::to_value(input)?,
            })
        }
    }

    impl<B, I, S> Workflow<B, I, S>
    where
        B: backend::Backend,
        I: Send + Sync + 'static,
        S: Clone + Send + Sync + 'static,
    {
        fn step_task_config(&self, index: usize) -> task::TaskConfig {
            self.steps
                .get(index)
                .map(|step| step.task_config.clone())
                .unwrap_or_default()
        }

        async fn persist_activity_call_commands(
            &self,
            tx: &mut B::Transaction<'_>,
            workflow_run_id: WorkflowRunId,
            step_index: usize,
            activity_call_buffer: &Arc<Mutex<ActivityCallBuffer>>,
        ) -> task::Result<()> {
            let commands = {
                let mut buffer = activity_call_buffer.lock().map_err(|_| {
                    task::Error::Fatal("Activity call buffer lock was poisoned.".to_string())
                })?;
                buffer.drain_commands()
            };

            for command in commands {
                let Some((retry_policy, timeout)) =
                    self.activity_registry.call_policy(&command.activity)
                else {
                    return Err(task::Error::Fatal(format!(
                        "No activity policy registered for `{}`.",
                        command.activity
                    )));
                };
                tx.insert_activity_call(
                    &self.queue.name,
                    workflow_run_id,
                    step_index,
                    backend::ActivityCallCommand {
                        call_key: command.call_key,
                        activity: command.activity,
                        input: command.input,
                        timeout,
                        retry_policy,
                    },
                )
                .await
                .map_err(|err| task::Error::Fatal(err.to_string()))?;
            }

            Ok(())
        }
    }

    impl<B, I, S> task::Task<B> for Workflow<B, I, S>
    where
        B: backend::Backend,
        I: Send + Sync + 'static,
        S: Clone + Send + Sync + 'static,
    {
        type Input = WorkflowState;
        type Output = ();

        async fn execute(
            &self,
            tx: &mut B::Transaction<'_>,
            input: Self::Input,
        ) -> task::Result<Self::Output> {
            let WorkflowState {
                step_index,
                step_input,
                workflow_run_id,
            } = input;

            if step_index >= self.steps.len() {
                return Err(task::Error::Fatal("Invalid step index.".to_string()));
            }

            let activity_calls = tx
                .load_activity_calls(&self.queue.name, workflow_run_id, step_index)
                .await
                .map_err(|err| task::Error::Retryable(err.to_string()))?;
            let activity_call_buffer =
                Arc::new(Mutex::new(ActivityCallBuffer::new(activity_calls)));
            let call_sequence_state = Arc::new(Mutex::new(CallSequenceState::default()));

            let cx = ContextParts {
                state: self.state.clone(),
                step_index,
                step_count: self.steps.len(),
                workflow_run_id,
                queue_name: self.queue.name.clone(),
                activity_call_buffer: Arc::clone(&activity_call_buffer),
                call_sequence_state,
            };

            let step_result = self.steps[step_index]
                .executor
                .execute_step(cx, step_input)
                .await;

            if matches!(step_result.as_ref(), Ok(_) | Err(task::Error::Suspended(_))) {
                self.persist_activity_call_commands(
                    tx,
                    workflow_run_id,
                    step_index,
                    &activity_call_buffer,
                )
                .await?;
            }

            let step_result = step_result?;

            if let Some((next_input, delay)) = step_result {
                let next = WorkflowState {
                    step_index: step_index + 1,
                    step_input: next_input,
                    workflow_run_id,
                };
                self.queue
                    .enqueue_after_using(tx, self, &next, delay)
                    .await
                    .map_err(|err| task::Error::Retryable(err.to_string()))?;
            }

            Ok(())
        }

        fn config(&self) -> task::ConfigResult {
            Ok(self.step_task_config(0))
        }

        fn config_for(&self, input: &Self::Input) -> task::ConfigResult {
            Ok(self.step_task_config(input.step_index))
        }
    }

    type BuilderMarker<B, I, O, S, A> = fn() -> (B, I, O, S, A);

    /// Workflow builder.
    pub struct Builder<B, I, O, S, State, A = NoActivities> {
        builder_state: State,
        steps: Vec<StepConfig<S>>,
        activity_registry: ActivityRegistry,
        _marker: PhantomData<BuilderMarker<B, I, O, S, A>>,
    }

    /// Initial builder state.
    pub struct Initial;
    /// State-set builder state.
    pub struct StateSet<S> {
        state: S,
    }
    /// Step-set builder state.
    pub struct StepSet<Current, S> {
        state: S,
        _marker: PhantomData<Current>,
    }
    /// Name-set builder state.
    pub struct NameSet<Current, S> {
        state: S,
        queue_name: String,
        _marker: PhantomData<Current>,
    }
    /// Backend-set builder state.
    pub struct BackendSet<Current, S, B> {
        state: S,
        queue_name: String,
        backend: B,
        _marker: PhantomData<Current>,
    }

    impl<B, I> Builder<B, I, I, (), Initial, NoActivities>
    where
        B: backend::Backend,
    {
        fn new() -> Self {
            Self {
                builder_state: Initial,
                steps: Vec::new(),
                activity_registry: ActivityRegistry::default(),
                _marker: PhantomData,
            }
        }
    }

    impl<B, I, ASet> Builder<B, I, I, (), Initial, ASet>
    where
        B: backend::Backend,
        ASet: 'static,
    {
        /// Registers an activity handler.
        pub fn activity<A>(
            mut self,
            activity: A,
        ) -> Builder<B, I, I, (), Initial, Registered<A, ASet>>
        where
            ASet: registration::ActivitySet,
            A: activity::Activity,
        {
            self.activity_registry.register(activity);
            Builder {
                builder_state: Initial,
                steps: Vec::new(),
                activity_registry: self.activity_registry,
                _marker: PhantomData,
            }
        }

        /// Sets non-durable state.
        pub fn state<NewS>(self, state: NewS) -> Builder<B, I, I, NewS, StateSet<NewS>, ASet> {
            Builder {
                builder_state: StateSet { state },
                steps: Vec::new(),
                activity_registry: self.activity_registry,
                _marker: PhantomData,
            }
        }

        /// Adds first step.
        pub fn step<F, O, Fut>(mut self, func: F) -> Builder<B, I, O, (), StepSet<O, ()>, ASet>
        where
            I: DeserializeOwned + Serialize + Send + Sync + 'static,
            O: Serialize + Send + Sync + 'static,
            F: Fn(Context<(), ASet>, I) -> Fut + Send + Sync + 'static,
            Fut: Future<Output = task::Result<Transition<O>>> + Send + 'static,
        {
            self.steps.push(StepConfig::new(func));
            Builder {
                builder_state: StepSet {
                    state: (),
                    _marker: PhantomData,
                },
                steps: self.steps,
                activity_registry: self.activity_registry,
                _marker: PhantomData,
            }
        }
    }

    impl<B, I, S, ASet> Builder<B, I, I, S, StateSet<S>, ASet>
    where
        B: backend::Backend,
        ASet: 'static,
    {
        /// Registers an activity handler.
        pub fn activity<A>(
            mut self,
            activity: A,
        ) -> Builder<B, I, I, S, StateSet<S>, Registered<A, ASet>>
        where
            ASet: registration::ActivitySet,
            A: activity::Activity,
        {
            self.activity_registry.register(activity);
            Builder {
                builder_state: self.builder_state,
                steps: Vec::new(),
                activity_registry: self.activity_registry,
                _marker: PhantomData,
            }
        }

        /// Adds first step.
        pub fn step<F, O, Fut>(mut self, func: F) -> Builder<B, I, O, S, StepSet<O, S>, ASet>
        where
            I: DeserializeOwned + Serialize + Send + Sync + 'static,
            O: Serialize + Send + Sync + 'static,
            S: Clone + Send + Sync + 'static,
            F: Fn(Context<S, ASet>, I) -> Fut + Send + Sync + 'static,
            Fut: Future<Output = task::Result<Transition<O>>> + Send + 'static,
        {
            self.steps.push(StepConfig::new(func));
            Builder {
                builder_state: StepSet {
                    state: self.builder_state.state,
                    _marker: PhantomData,
                },
                steps: self.steps,
                activity_registry: self.activity_registry,
                _marker: PhantomData,
            }
        }
    }

    impl<B, I, Current, S, ASet> Builder<B, I, Current, S, StepSet<Current, S>, ASet>
    where
        B: backend::Backend,
        S: Clone + Send + Sync + 'static,
        ASet: 'static,
    {
        /// Adds subsequent step.
        pub fn step<F, New, Fut>(mut self, func: F) -> Builder<B, I, New, S, StepSet<New, S>, ASet>
        where
            Current: DeserializeOwned + Serialize + Send + Sync + 'static,
            New: Serialize + Send + Sync + 'static,
            F: Fn(Context<S, ASet>, Current) -> Fut + Send + Sync + 'static,
            Fut: Future<Output = task::Result<Transition<New>>> + Send + 'static,
        {
            self.steps.push(StepConfig::new(func));
            Builder {
                builder_state: StepSet {
                    state: self.builder_state.state,
                    _marker: PhantomData,
                },
                steps: self.steps,
                activity_registry: self.activity_registry,
                _marker: PhantomData,
            }
        }

        /// Sets retry policy for previous step.
        pub fn retry_policy(mut self, retry_policy: task::RetryPolicy) -> Self {
            self.steps
                .last_mut()
                .expect("step must exist")
                .task_config
                .set_retry_policy(retry_policy);
            self
        }

        /// Sets timeout for previous step.
        pub fn timeout(mut self, timeout: Span) -> Self {
            self.steps
                .last_mut()
                .expect("step must exist")
                .task_config
                .set_timeout(timeout);
            self
        }

        /// Sets TTL for previous step.
        pub fn ttl(mut self, ttl: Span) -> Self {
            self.steps
                .last_mut()
                .expect("step must exist")
                .task_config
                .set_ttl(ttl);
            self
        }

        /// Sets delay for previous step.
        pub fn delay(mut self, delay: Span) -> Self {
            self.steps
                .last_mut()
                .expect("step must exist")
                .task_config
                .set_delay(delay);
            self
        }

        /// Sets heartbeat for previous step.
        pub fn heartbeat(mut self, heartbeat: Span) -> Self {
            self.steps
                .last_mut()
                .expect("step must exist")
                .task_config
                .set_heartbeat(heartbeat);
            self
        }

        /// Sets concurrency key for previous step.
        pub fn concurrency_key(mut self, concurrency_key: impl Into<String>) -> Self {
            self.steps
                .last_mut()
                .expect("step must exist")
                .task_config
                .set_concurrency_key(Some(concurrency_key.into()));
            self
        }

        /// Sets priority for previous step.
        pub fn priority(mut self, priority: i32) -> Self {
            self.steps
                .last_mut()
                .expect("step must exist")
                .task_config
                .set_priority(priority);
            self
        }
    }

    impl<B, I, S, ASet> Builder<B, I, (), S, StepSet<(), S>, ASet>
    where
        B: backend::Backend,
        I: Send + Sync + 'static,
        S: Clone + Send + Sync + 'static,
    {
        /// Sets queue name.
        pub fn name(
            self,
            queue_name: impl Into<String>,
        ) -> Builder<B, I, (), S, NameSet<(), S>, ASet> {
            Builder {
                builder_state: NameSet {
                    state: self.builder_state.state,
                    queue_name: queue_name.into(),
                    _marker: PhantomData,
                },
                steps: self.steps,
                activity_registry: self.activity_registry,
                _marker: PhantomData,
            }
        }
    }

    impl<B, I, S, ASet> Builder<B, I, (), S, NameSet<(), S>, ASet>
    where
        B: backend::Backend,
        I: Send + Sync + 'static,
        S: Clone + Send + Sync + 'static,
    {
        /// Sets backend.
        pub fn backend(self, backend: B) -> Builder<B, I, (), S, BackendSet<(), S, B>, ASet> {
            Builder {
                builder_state: BackendSet {
                    state: self.builder_state.state,
                    queue_name: self.builder_state.queue_name,
                    backend,
                    _marker: PhantomData,
                },
                steps: self.steps,
                activity_registry: self.activity_registry,
                _marker: PhantomData,
            }
        }
    }

    impl<B, I, S, ASet> Builder<B, I, (), S, BackendSet<(), S, B>, ASet>
    where
        B: backend::Backend,
        I: Send + Sync + 'static,
        S: Clone + Send + Sync + 'static,
    {
        /// Builds workflow.
        pub async fn build(self) -> Result<Workflow<B, I, S>> {
            let state = self.builder_state;
            for step in &self.steps {
                step.task_config.validate()?;
            }
            let queue = queue::Queue::builder()
                .name(state.queue_name)
                .backend(state.backend.clone())
                .build()
                .await?;
            Ok(Workflow {
                queue: Arc::new(queue),
                steps: Arc::new(self.steps),
                state: state.state,
                activity_registry: self.activity_registry,
                _marker: PhantomData,
            })
        }
    }
}

/// Runtime orchestration.
pub mod runtime {
    use super::*;

    /// Runtime handle.
    pub struct RuntimeHandle {
        workers: JoinSet<Result>,
        shutdown_token: CancellationToken,
    }

    impl RuntimeHandle {
        /// Signals shutdown and waits for loops to end.
        pub async fn shutdown(mut self) -> Result {
            self.shutdown_token.cancel();
            while let Some(result) = self.workers.join_next().await {
                result??;
            }
            Ok(())
        }
    }

    /// High-level workflow runtime.
    pub struct Runtime<B, I, S = ()>
    where
        B: backend::Backend,
        I: Serialize + Send + Sync + 'static,
        S: Clone + Send + Sync + 'static,
    {
        workflow: workflow::Workflow<B, I, S>,
        activity_worker: activity_worker::ActivityWorker<B>,
    }

    impl<B, I, S> Clone for Runtime<B, I, S>
    where
        B: backend::Backend,
        I: Serialize + Send + Sync + 'static,
        S: Clone + Send + Sync + 'static,
    {
        fn clone(&self) -> Self {
            Self {
                workflow: self.workflow.clone(),
                activity_worker: self.activity_worker.clone(),
            }
        }
    }

    impl<B, I, S> Runtime<B, I, S>
    where
        B: backend::Backend,
        I: Serialize + Send + Sync + 'static,
        S: Clone + Send + Sync + 'static,
    {
        /// Creates a runtime.
        pub fn new(workflow: workflow::Workflow<B, I, S>) -> Self {
            let queue = workflow.queue();
            let activity_worker = activity_worker::ActivityWorker::with_registry(
                queue.backend().clone(),
                queue.name().to_string(),
                workflow.activity_registry(),
            );
            Self {
                workflow,
                activity_worker,
            }
        }

        /// Workflow.
        pub fn workflow(&self) -> &workflow::Workflow<B, I, S> {
            &self.workflow
        }

        /// Worker.
        pub fn worker(&self) -> worker::Worker<B, workflow::Workflow<B, I, S>> {
            worker::Worker::new(self.workflow.queue(), self.workflow.clone())
        }

        /// Scheduler.
        pub fn scheduler(&self) -> scheduler::Scheduler<B, workflow::Workflow<B, I, S>> {
            scheduler::Scheduler::new(self.workflow.queue(), self.workflow.clone())
        }

        /// Runs runtime loops.
        pub async fn run(&self) -> Result {
            self.start().shutdown().await
        }

        /// Starts runtime loops.
        pub fn start(&self) -> RuntimeHandle {
            let shutdown_token = CancellationToken::new();
            let mut workers = JoinSet::new();

            let mut worker = self.worker();
            worker.set_shutdown_token(shutdown_token.clone());
            workers.spawn(async move { worker.run().await });

            let mut scheduler = self.scheduler();
            scheduler.set_shutdown_token(shutdown_token.clone());
            workers.spawn(async move { scheduler.run().await });

            let mut activity_worker = self.activity_worker.clone();
            activity_worker.set_shutdown_token(shutdown_token.clone());
            if !activity_worker.is_empty() {
                workers.spawn(async move { activity_worker.run().await });
            }

            RuntimeHandle {
                workers,
                shutdown_token,
            }
        }
    }
}
