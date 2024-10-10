//! Tasks represent a well-structure unit of work.
//!
//! A task is defined by implementing the [`execute`](crate::Task::execute)
//! method and specifying the associated type [`Input`](crate::Task::Input).
//! This provides a strongly-typed interface to execute invocations.
//!
//! Once a task is implemented, it can be enqueued on a [`Queue`](crate::Queue)
//! for processing. A [`Worker`](crate::Worker) can then dequeue the task and
//! invoke its `execute` method, providing the input that has been deserialized
//! into the specified [`Input`](crate::Task::Input) type.
//!
//! Queues and workers operate over tasks to make them useful in the context of
//! your application.
//!
//! # Implementing `Task`
//!
//! Generally you'll want to use the higher-level [`Job`](crate::Job)
//! abstraction instead of implementing `Task` yourself. Its workflow is more
//! ergonomic and therefore preferred for virtually all cases.
//!
//! However, it's possible to implement the trait directly. This may be useful
//! for building more sophisticated behavior on top of the task concept that
//! isn't already provided by `Job`.
//!
//! ```
//! use serde::{Deserialize, Serialize};
//! use underway::{task::Result as TaskResult, Task};
//! # use tokio::runtime::Runtime;
//! # fn main() {
//! # let rt = Runtime::new().unwrap();
//! # rt.block_on(async {
//!
//! // Task input representing the data needed to send a welcome email.
//! #[derive(Debug, Deserialize, Serialize)]
//! struct WelcomeEmail {
//!     user_id: i32,
//!     email: String,
//!     name: String,
//! }
//!
//! // Task that sends a welcome email to a user.
//! struct WelcomeEmailTask;
//!
//! impl Task for WelcomeEmailTask {
//!     type Input = WelcomeEmail;
//!
//!     /// Simulate sending a welcome email by printing a message to the console.
//!     async fn execute(&self, input: Self::Input) -> TaskResult {
//!         println!(
//!             "Sending welcome email to {} <{}> (user_id: {})",
//!             input.name, input.email, input.user_id
//!         );
//!
//!         // Here you would integrate with an email service.
//!         // If email sending fails, you could return an error to trigger retries.
//!         Ok(())
//!     }
//! }
//! # let task = WelcomeEmailTask;
//! # let input = WelcomeEmail {
//! #     user_id: 1,
//! #     email: "user@example.com".to_string(),
//! #     name: "Alice".to_string(),
//! # };
//! # task.execute(input).await.unwrap();
//! # });
//! # }
//! ```
use std::future::Future;

use jiff::{Span, ToSpan};
use serde::{de::DeserializeOwned, Serialize};
use sqlx::postgres::types::PgInterval;
use uuid::Uuid;

/// A type alias for task identifiers.
pub type Id = Uuid;

/// A type alias for task execution results.
pub type Result = std::result::Result<(), Error>;

/// Task errors.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Error returned by the `sqlx` crate during database operations.
    #[error(transparent)]
    Database(#[from] sqlx::Error),

    /// Error indicating that the task has encountered an unrecoverable error
    /// state.
    #[error("{0}")]
    Fatal(String),

    /// Error indicating that the task has encountered a recoverable error
    /// state.
    #[error("{0}")]
    Retryable(String),
}

/// The task interface.
pub trait Task: Send + 'static {
    /// The input type that the execute method will take.
    ///
    /// This type must be serialized to and deserialized from the database.
    type Input: DeserializeOwned + Serialize + Send + 'static;

    /// Executes the task with the provided input.
    ///
    /// The core of a task, this method is called when the task is picked up by
    /// a worker. The input type is passed to the method, and the task is
    /// expected to handle it.
    ///
    /// # Example
    ///
    /// ```
    /// use serde::{Deserialize, Serialize};
    /// use underway::{task::Error as TaskError, Task};
    ///
    /// // Task input representing the data needed to send a welcome email.
    /// #[derive(Debug, Deserialize, Serialize)]
    /// struct WelcomeEmail {
    ///     user_id: i32,
    ///     email: String,
    ///     name: String,
    /// }
    ///
    /// // Task that sends a welcome email to a user.
    /// struct WelcomeEmailTask;
    ///
    /// impl Task for WelcomeEmailTask {
    ///     type Input = WelcomeEmail;
    ///
    ///     /// Simulate sending a welcome email by printing a message to the console.
    ///     async fn execute(&self, input: Self::Input) -> Result<(), TaskError> {
    ///         println!(
    ///             "Sending welcome email to {} <{}> (user_id: {})",
    ///             input.name, input.email, input.user_id
    ///         );
    ///
    ///         // Here you would integrate with an email service.
    ///         // If email sending fails, you could return an error to trigger retries.
    ///         Ok(())
    ///     }
    /// }
    /// ```
    fn execute(&self, input: Self::Input) -> impl Future<Output = Result> + Send;

    /// Defines the retry policy of the task.
    ///
    /// The retry policy determines how many times the task should be retried in
    /// the event of failure, and the interval between retries. This is useful
    /// for handling transient failures like network issues or external API
    /// errors.
    fn retry_policy(&self) -> RetryPolicy {
        RetryPolicy::default()
    }

    /// Provides the task execution timeout.
    ///
    /// The default expiration is set to 15 minutes. Override this if your tasks
    /// need to have shorter or longer timeouts.
    fn timeout(&self) -> Span {
        15.minutes()
    }

    /// Provides task time-to-live (TTL) duration in queue.
    ///
    /// After the duration has elapsed, a task may be removed from the queue,
    /// e.g. via [`delete_expired`](Queue.delete_expired).
    fn ttl(&self) -> Span {
        14.days()
    }

    /// Provides a delay before which the task won't be dequeued.
    ///
    /// Delays are used to prevent a task from being run immediately.
    ///
    /// This may be useful when a task should not be run after it's been
    /// constructed and enqueued. However, delays differ from scheduled tasks
    /// and should not be used for recurring execution. Instead, use
    /// [`Worker::run_scheduled`](crate::Worker::run_scheduler).
    fn delay(&self) -> Span {
        Span::new()
    }

    /// Provides an optional concurrency key for the task.
    ///
    /// Concurrency keys are used to limit how many tasks of a specific type are
    /// allowed to run concurrently. By providing a unique key, tasks with
    /// the same key can be processed sequentially rather than in parallel.
    ///
    /// This can be useful when working with shared resources or preventing race
    /// conditions. If no concurrency key is provided, tasks will be
    /// executed concurrently.
    ///
    /// # Example:
    ///
    /// If you're processing files, you might want to use the file path as the
    /// concurrency key to prevent two workers from processing the same file
    /// simultaneously.
    ///
    /// ```
    /// use std::path::PathBuf;
    ///
    /// use underway::{task::Result as TaskResult, Task};
    ///
    /// struct MyUniqueTask(PathBuf);
    ///
    /// impl Task for MyUniqueTask {
    ///     type Input = ();
    ///
    ///     async fn execute(&self, _input: Self::Input) -> TaskResult {
    ///         Ok(())
    ///     }
    ///
    ///     fn concurrency_key(&self) -> Option<String> {
    ///         Some(self.0.display().to_string())
    ///     }
    /// }
    /// ```
    fn concurrency_key(&self) -> Option<String> {
        None
    }

    /// Specifies the priority of the task.
    ///
    /// Higher-priority tasks will be processed before lower-priority ones. This
    /// can be useful in systems where certain jobs are time-sensitive and
    /// need to be handled before others.
    ///
    /// The default priority is `0`, and higher numbers represent higher
    /// priority.
    ///
    /// # Example:
    ///
    /// ```
    /// use underway::{task::Result as TaskResult, Task};
    ///
    /// struct HighPriorityTask;
    ///
    /// impl Task for HighPriorityTask {
    ///     type Input = ();
    ///
    ///     async fn execute(&self, _input: Self::Input) -> TaskResult {
    ///         Ok(())
    ///     }
    ///
    ///     fn priority(&self) -> i32 {
    ///         10 // High-priority task
    ///     }
    /// }
    /// ```
    fn priority(&self) -> i32 {
        0
    }
}

/// Dequeued task.
#[derive(Debug)]
pub struct DequeuedTask {
    /// Task ID.
    pub id: Id,

    /// Input as a `serde_json::Value`.
    pub input: serde_json::Value,

    /// Timeout.
    pub timeout: PgInterval,

    /// Total times retried.
    pub retry_count: i32,

    /// Maximum retry attempts.
    pub max_attempts: i32,

    /// Initial interval in milliseconds.
    pub initial_interval_ms: i32,

    /// Maximum interval in milliseconds.
    pub max_interval_ms: i32,

    /// Backoff coefficient.
    pub backoff_coefficient: f32,

    /// Concurrency key.
    pub concurrency_key: Option<String>,
}

/// Configuration of a policy for retries in case of task failure.
///
/// # Example
///
/// ```rust
/// use underway::task::RetryPolicy;
///
/// let retry_policy = RetryPolicy::builder()
///     .max_attempts(10)
///     .backoff_coefficient(4.0)
///     .build();
/// ```
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RetryPolicy {
    pub(crate) max_attempts: i32,
    pub(crate) initial_interval_ms: i32,
    pub(crate) max_interval_ms: i32,
    pub(crate) backoff_coefficient: f32,
}

pub(crate) type RetryCount = i32;

impl RetryPolicy {
    /// Create a new builder.
    pub fn builder() -> RetryPolicyBuilder {
        RetryPolicyBuilder::default()
    }

    /// Returns the delay relative to the given retry count.
    pub fn calculate_delay(&self, retry_count: RetryCount) -> Span {
        let base_delay = self.initial_interval_ms as f32;
        let backoff_delay = base_delay * self.backoff_coefficient.powi(retry_count - 1);
        let delay = backoff_delay.min(self.max_interval_ms as f32) as i64;
        delay.milliseconds()
    }
}

impl From<DequeuedTask> for RetryPolicy {
    fn from(
        DequeuedTask {
            max_attempts,
            initial_interval_ms,
            max_interval_ms,
            backoff_coefficient,
            ..
        }: DequeuedTask,
    ) -> Self {
        Self {
            max_attempts,
            initial_interval_ms,
            max_interval_ms,
            backoff_coefficient,
        }
    }
}

const DEFAULT_RETRY_POLICY: RetryPolicy = RetryPolicy {
    max_attempts: 5,
    initial_interval_ms: 1_000,
    max_interval_ms: 60_000,
    backoff_coefficient: 2.0,
};

impl Default for RetryPolicy {
    fn default() -> Self {
        DEFAULT_RETRY_POLICY
    }
}

/// A builder for constructing `RetryPolicy`.
///
/// # Example
///
/// ```
/// use underway::task::RetryPolicyBuilder;
///
/// let retry_policy = RetryPolicyBuilder::new()
///     .max_attempts(3)
///     .initial_interval_ms(500)
///     .max_interval_ms(5_000)
///     .backoff_coefficient(1.5)
///     .build();
/// ```
#[derive(Debug, Default)]
pub struct RetryPolicyBuilder {
    inner: RetryPolicy,
}

impl RetryPolicyBuilder {
    /// Creates a new `RetryPolicyBuilder` with the default retry settings.
    pub const fn new() -> Self {
        Self {
            inner: DEFAULT_RETRY_POLICY,
        }
    }

    /// Sets the maximum number of retry attempts.
    ///
    /// Default value is `5`.
    pub const fn max_attempts(mut self, max_attempts: i32) -> Self {
        self.inner.max_attempts = max_attempts;
        self
    }

    /// Sets the initial interval before the first retry (in milliseconds).
    ///
    /// Default value is `1_000`.
    pub const fn initial_interval_ms(mut self, initial_interval_ms: i32) -> Self {
        self.inner.initial_interval_ms = initial_interval_ms;
        self
    }

    /// Sets the maximum interval between retries (in milliseconds).
    ///
    /// Default value is `60_000`.
    pub const fn max_interval_ms(mut self, max_interval_ms: i32) -> Self {
        self.inner.max_interval_ms = max_interval_ms;
        self
    }

    /// Sets the backoff coefficient to apply after each retry.
    ///
    /// Default value is `2.0`.
    pub const fn backoff_coefficient(mut self, backoff_coefficient: f32) -> Self {
        self.inner.backoff_coefficient = backoff_coefficient;
        self
    }

    /// Builds the `RetryPolicy` with the configured parameters.
    pub const fn build(self) -> RetryPolicy {
        self.inner
    }
}

/// Represents the possible states a task can be in.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, sqlx::Type)]
#[sqlx(type_name = "underway.task_state", rename_all = "snake_case")]
pub enum State {
    /// Awaiting execution.
    Pending,

    /// Currently being processed.
    InProgress,

    /// Execute completed successfully.
    Succeeded,

    /// Execute cancelled.
    Cancelled,

    /// Execute completed unsucessfully.
    Failed,
}

#[cfg(test)]
mod tests {
    use serde::{Deserialize, Serialize};

    use super::*;

    #[derive(Debug, Deserialize, Serialize)]
    struct TestTaskInput {
        message: String,
    }

    struct TestTask;

    impl Task for TestTask {
        type Input = TestTaskInput;

        async fn execute(&self, input: Self::Input) -> Result {
            println!("Executing task with message: {}", input.message);
            if input.message == "fail" {
                return Err(Error::Retryable("Task failed".to_string()));
            }
            Ok(())
        }
    }

    #[tokio::test]
    async fn task_execution_success() {
        let task = TestTask;
        let input = TestTaskInput {
            message: "Hello, World!".to_string(),
        };

        let result = task.execute(input).await;
        assert!(result.is_ok())
    }

    #[tokio::test]
    async fn task_execution_failure() {
        let task = TestTask;
        let input = TestTaskInput {
            message: "fail".to_string(),
        };

        let result = task.execute(input).await;
        assert!(result.is_err())
    }

    #[test]
    fn retry_policy_defaults() {
        let default_policy = RetryPolicy::default();
        assert_eq!(default_policy.max_attempts, 5);
        assert_eq!(default_policy.initial_interval_ms, 1_000);
        assert_eq!(default_policy.max_interval_ms, 60_000);
        assert_eq!(default_policy.backoff_coefficient, 2.0);
    }

    #[test]
    fn retry_policy_custom() {
        let retry_policy = RetryPolicyBuilder::new()
            .max_attempts(3)
            .initial_interval_ms(500)
            .max_interval_ms(5_000)
            .backoff_coefficient(1.5)
            .build();

        assert_eq!(retry_policy.max_attempts, 3);
        assert_eq!(retry_policy.initial_interval_ms, 500);
        assert_eq!(retry_policy.max_interval_ms, 5_000);
        assert_eq!(retry_policy.backoff_coefficient, 1.5);
    }
}
