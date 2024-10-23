//! Tasks represent a well-structured unit of work.
//!
//! A task is defined by implementing the [`execute`](crate::Task::execute)
//! method and specifying the associated type [`Input`](crate::Task::Input).
//! This provides a strongly-typed interface to execute task invocations.
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
//! use sqlx::{Postgres, Transaction};
//! use underway::{task::Result as TaskResult, Task};
//! # use sqlx::PgPool;
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
//!     type Output = ();
//!
//!     /// Simulate sending a welcome email by printing a message to the console.
//!     async fn execute(
//!         &self,
//!         _tx: Transaction<'_, Postgres>,
//!         input: Self::Input,
//!     ) -> TaskResult<Self::Output> {
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
//! # let pool = PgPool::connect(&std::env::var("DATABASE_URL")?).await?;
//! # let tx = pool.begin().await?;
//! # let task = WelcomeEmailTask;
//! # let input = WelcomeEmail {
//! #     user_id: 1,
//! #     email: "user@example.com".to_string(),
//! #     name: "Alice".to_string(),
//! # };
//! # task.execute(tx, input).await?;
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! # });
//! # }
//! ```
use std::{future::Future, result::Result as StdResult};

use jiff::{Span, ToSpan};
use serde::{de::DeserializeOwned, Serialize};
use sqlx::{postgres::types::PgInterval, Postgres, Transaction};
use uuid::Uuid;

pub(crate) use self::retry_policy::RetryCount;
pub use self::retry_policy::RetryPolicy;

mod retry_policy;

/// A type alias for task identifiers.
///
/// Task IDs are [ULID][ULID]s which are converted to UUIDv4 for storage.
///
/// [ULID]: https://github.com/ulid/spec?tab=readme-ov-file#specification
pub type Id = Uuid;

/// A type alias for task execution results.
pub type Result<T> = StdResult<T, Error>;

/// Task errors.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Error returned by the `sqlx` crate during database operations.
    #[error(transparent)]
    Database(#[from] sqlx::Error),

    /// Error indicating that the task has encountered an unrecoverable error
    /// state.
    ///
    /// **Note:** Returning this error from an execute future will override any
    /// remaining retries and set the task to [`State::Failed`].
    #[error("{0}")]
    Fatal(String),

    /// Error indicating that the task has encountered a recoverable error
    /// state.
    #[error("{0}")]
    Retryable(String),
}

/// Convenience trait for converting results into task results.
///
/// This makes it easier to convert execution errors to either
/// [`Retryable`](Error::Retryable) or [`Fatal`](Error::Fatal). These are
/// recoverable and unrecoverable, respectively.
///
/// # Examples
///
/// Sometimes errors are retryable:
///
///```rust
/// use tokio::net;
/// use underway::{Job, To, ToTaskResult};
///
/// Job::<(), ()>::builder().step(|_, _| async {
///     // If we can't resolve DNS the issue may be transient and recoverable.
///     net::lookup_host("example.com:80").await.retryable()?;
///
///     To::done()
/// });
/// ```
///
/// And other times they're fatal:
///
/// ```rust
/// use std::env;
///
/// use underway::{Job, To, ToTaskResult};
///
/// Job::<(), ()>::builder().step(|_, _| async {
///     // If the API_KEY environment variable isn't set we can't recover.
///     let api_key = env::var("API_KEY").fatal()?;
///
///     To::done()
/// });
/// ```
pub trait ToTaskResult<T> {
    /// Converts the error into a [`Retryable`](Error::Retryable) task error.
    fn retryable(self) -> StdResult<T, Error>;

    /// Converts the error into a [`Fatal`](Error::Fatal) task error.
    fn fatal(self) -> StdResult<T, Error>;
}

impl<T, E: std::fmt::Display> ToTaskResult<T> for StdResult<T, E> {
    fn retryable(self) -> StdResult<T, Error> {
        self.map_err(|err| Error::Retryable(err.to_string()))
    }

    fn fatal(self) -> StdResult<T, Error> {
        self.map_err(|err| Error::Fatal(err.to_string()))
    }
}

/// Trait for defining tasks.
///
/// Queues and workers operate over types that implement this trait.
pub trait Task: Send + 'static {
    /// The input type that the execute method will take.
    ///
    /// This type must be serialized to and deserialized from the database.
    type Input: DeserializeOwned + Serialize + Send + 'static;

    /// The output type that the execute method will return upon success.
    type Output: Serialize + Send + 'static;

    /// Executes the task with the provided input.
    ///
    /// The core of a task, this method is called when the task is picked up by
    /// a worker.
    ///
    /// Typically this method will do something with the provided input. If no
    /// input is needed, then the unit type, `()`, can be used instead and the
    /// input ignored.
    ///
    /// # Example
    ///
    /// ```
    /// use serde::{Deserialize, Serialize};
    /// use sqlx::{Postgres, Transaction};
    /// use underway::{task::Result as TaskResult, Task};
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
    ///     type Output = ();
    ///
    ///     /// Simulate sending a welcome email by printing a message to the console.
    ///     async fn execute(
    ///         &self,
    ///         tx: Transaction<'_, Postgres>,
    ///         input: Self::Input,
    ///     ) -> TaskResult<Self::Output> {
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
    fn execute(
        &self,
        tx: Transaction<'_, Postgres>,
        input: Self::Input,
    ) -> impl Future<Output = Result<Self::Output>> + Send;

    /// Defines the retry policy of the task.
    ///
    /// The retry policy determines how many times the task should be retried in
    /// the event of failure, and the interval between retries. This is useful
    /// for handling transient failures like network issues or external API
    /// errors.
    ///
    /// # Example
    ///
    /// ```rust
    /// use sqlx::{Postgres, Transaction};
    /// use underway::{
    ///     task::{Result as TaskResult, RetryPolicy},
    ///     Task,
    /// };
    ///
    /// struct MyCustomRetryTask;
    ///
    /// impl Task for MyCustomRetryTask {
    ///     type Input = ();
    ///     type Output = ();
    ///
    ///     async fn execute(
    ///         &self,
    ///         _tx: Transaction<'_, Postgres>,
    ///         _input: Self::Input,
    ///     ) -> TaskResult<Self::Output> {
    ///         Ok(())
    ///     }
    ///
    ///     // Specify our own retry policy for the task.
    ///     fn retry_policy(&self) -> RetryPolicy {
    ///         RetryPolicy::builder()
    ///             .max_attempts(20)
    ///             .initial_interval_ms(2_500)
    ///             .backoff_coefficient(1.5)
    ///             .build()
    ///     }
    /// }
    /// ```
    fn retry_policy(&self) -> RetryPolicy {
        RetryPolicy::default()
    }

    /// Provides the task execution timeout.
    ///
    /// Override this if your tasks need to have shorter or longer timeouts.
    ///
    /// Defaults to 15 minutes.
    ///
    /// # Example
    ///
    /// ```rust
    /// use jiff::{Span, ToSpan};
    /// use sqlx::{Postgres, Transaction};
    /// use underway::{task::Result as TaskResult, Task};
    ///
    /// struct MyImpatientTask;
    ///
    /// impl Task for MyImpatientTask {
    ///     type Input = ();
    ///     type Output = ();
    ///
    ///     async fn execute(
    ///         &self,
    ///         _tx: Transaction<'_, Postgres>,
    ///         _input: Self::Input,
    ///     ) -> TaskResult<Self::Output> {
    ///         Ok(())
    ///     }
    ///
    ///     // Only give the task a short time to complete execution.
    ///     fn timeout(&self) -> Span {
    ///         1.second()
    ///     }
    /// }
    /// ```
    fn timeout(&self) -> Span {
        15.minutes()
    }

    /// Provides task time-to-live (TTL) duration in queue.
    ///
    /// After the duration has elapsed, a task may be removed from the queue,
    /// e.g. via [`run_deletion`](crate::queue::run_deletion).
    ///
    /// **Note:** Tasks are not removed from the queue unless the `run_deletion`
    /// routine is active.
    ///
    /// Defaults to 14 days.
    ///
    /// # Example
    ///
    /// ```rust
    /// use jiff::{Span, ToSpan};
    /// use sqlx::{Postgres, Transaction};
    /// use underway::{task::Result as TaskResult, Task};
    ///
    /// struct MyLongLivedTask;
    ///
    /// impl Task for MyLongLivedTask {
    ///     type Input = ();
    ///     type Output = ();
    ///
    ///     async fn execute(
    ///         &self,
    ///         _tx: Transaction<'_, Postgres>,
    ///         _input: Self::Input,
    ///     ) -> TaskResult<Self::Output> {
    ///         Ok(())
    ///     }
    ///
    ///     // Keep the task around in the queue for a very long time.
    ///     fn ttl(&self) -> Span {
    ///         10.years()
    ///     }
    /// }
    /// ```
    fn ttl(&self) -> Span {
        14.days()
    }

    /// Provides a delay before which the task won't be dequeued.
    ///
    /// Delays are used to prevent a task from running immediately after being
    /// enqueued.
    ///
    /// Defaults to no delay.
    ///
    /// # Example
    ///
    /// ```rust
    /// use jiff::{Span, ToSpan};
    /// use sqlx::{Postgres, Transaction};
    /// use underway::{task::Result as TaskResult, Task};
    ///
    /// struct MyDelayedTask;
    ///
    /// impl Task for MyDelayedTask {
    ///     type Input = ();
    ///     type Output = ();
    ///
    ///     async fn execute(
    ///         &self,
    ///         _tx: Transaction<'_, Postgres>,
    ///         _input: Self::Input,
    ///     ) -> TaskResult<Self::Output> {
    ///         Ok(())
    ///     }
    ///
    ///     // Delay dequeuing the task for one hour.
    ///     fn delay(&self) -> Span {
    ///         1.hour()
    ///     }
    /// }
    /// ```
    fn delay(&self) -> Span {
        Span::new()
    }

    /// Provides an optional concurrency key for the task.
    ///
    /// Concurrency keys are used to limit how many tasks of a specific type are
    /// allowed to run concurrently. By providing a unique key, tasks with
    /// the same key can be processed sequentially rather than concurrently.
    ///
    /// This can be useful when working with shared resources or for preventing
    /// race conditions. If no concurrency key is provided, tasks will be
    /// executed concurrently.
    ///
    /// # Example
    ///
    /// If you're processing files, you might want to use the file path as the
    /// concurrency key to prevent two workers from processing the same file
    /// simultaneously.
    ///
    /// ```rust
    /// use std::path::PathBuf;
    ///
    /// use sqlx::{Postgres, Transaction};
    /// use underway::{task::Result as TaskResult, Task};
    ///
    /// struct MyUniqueTask(PathBuf);
    ///
    /// impl Task for MyUniqueTask {
    ///     type Input = ();
    ///     type Output = ();
    ///
    ///     async fn execute(
    ///         &self,
    ///         _tx: Transaction<'_, Postgres>,
    ///         _input: Self::Input,
    ///     ) -> TaskResult<Self::Output> {
    ///         Ok(())
    ///     }
    ///
    ///     // Use the path buf as our concurrency key.
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
    /// # Example
    ///
    /// ```rust
    /// use sqlx::{Postgres, Transaction};
    /// use underway::{task::Result as TaskResult, Task};
    ///
    /// struct HighPriorityTask;
    ///
    /// impl Task for HighPriorityTask {
    ///     type Input = ();
    ///     type Output = ();
    ///
    ///     async fn execute(
    ///         &self,
    ///         _tx: Transaction<'_, Postgres>,
    ///         _input: Self::Input,
    ///     ) -> TaskResult<Self::Output> {
    ///         Ok(())
    ///     }
    ///
    ///     // Set the priority to 10.
    ///     fn priority(&self) -> i32 {
    ///         10
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
    use sqlx::PgPool;

    use super::*;

    #[derive(Debug, Deserialize, Serialize)]
    struct TestTaskInput {
        message: String,
    }

    struct TestTask;

    impl Task for TestTask {
        type Input = TestTaskInput;
        type Output = ();

        async fn execute(
            &self,
            _tx: Transaction<'_, Postgres>,
            input: Self::Input,
        ) -> Result<Self::Output> {
            println!("Executing task with message: {}", input.message);
            if input.message == "fail" {
                return Err(Error::Retryable("Task failed".to_string()));
            }
            Ok(())
        }
    }

    #[sqlx::test]
    async fn task_execution_success(pool: PgPool) {
        let task = TestTask;
        let input = TestTaskInput {
            message: "Hello, World!".to_string(),
        };

        let tx = pool.begin().await.unwrap();
        let result = task.execute(tx, input).await;
        assert!(result.is_ok())
    }

    #[sqlx::test]
    async fn task_execution_failure(pool: PgPool) {
        let task = TestTask;
        let input = TestTaskInput {
            message: "fail".to_string(),
        };

        let tx = pool.begin().await.unwrap();
        let result = task.execute(tx, input).await;
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
        let retry_policy = RetryPolicy::builder()
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
