use std::future::Future;

use jiff::{tz::TimeZone, Span, Timestamp, Zoned};
use serde::{de::DeserializeOwned, Serialize};
use uuid::Uuid;

use crate::queue::DequeuedTask;

/// A type alias for task identifiers.
pub type Id = Uuid;

/// A type alias for task execution results.
pub type Result = std::result::Result<(), Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Error returned by the `sqlx` crate during database operations.
    #[error(transparent)]
    Database(#[from] sqlx::Error),

    /// Error indicating that the task has encountered an unrecoverable state.
    #[error("{0}")]
    Fatal(String),

    /// Error indicating that the task has encountered a retriable state.
    #[error("{0}")]
    Generic(String),
}

/// A task which defines an input type and a function that is executed using
/// that type.
///
/// This trait allows you to define the behavior of tasks, including the input
/// structure, the function that processes the input, and various task policies
/// like retries, expiration, and concurrency. Tasks are strongly typed, which
/// ensures that data passed to the task is well-formed and predictable.
///
/// # Example
///
/// ```
/// use serde::{Deserialize, Serialize};
/// use underway::{task::Error as TaskError, Task};
/// # use tokio::runtime::Runtime;
///
/// # fn main() {
/// # let rt = Runtime::new().unwrap();
/// # rt.block_on(async {
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
///
/// # let task = WelcomeEmailTask;
/// # let input = WelcomeEmail {
/// #     user_id: 1,
/// #     email: "user@example.com".to_string(),
/// #     name: "Alice".to_string(),
/// # };
///
/// # task.execute(input).await.unwrap();
/// # });
/// # }
/// ```
///
/// ## Associated Types:
///
/// - `Input`: The type that represents the input for this task. The input must
///   implement `DeserializeOwned` and `Serialize` to allow seamless
///   serialization and deserialization when enqueuing or executing jobs. This
///   ensures that the input can be sent across the queue and passed to workers
///   in a structured format.
pub trait Task: Send + 'static {
    /// Type used by the executor.
    ///
    /// This represents the structured input data required for the task. For
    /// example, if you're sending an email, your input might include a
    /// `recipient`, `subject`, and `body`. The type must be serializable to
    /// allow safe transmission through the queue.
    type Input: DeserializeOwned + Serialize + Send + 'static;

    /// Executes the task with the provided input.
    ///
    /// The core of a task, this method is called when the task is picked up by
    /// a worker. The input type is passed to the method, and the task is
    /// expected to handle it. The function returns a `Future` that resolves
    /// to a `Result<(), Error>`, where `Ok(())` signals a successful task
    /// execution, and `Err(Error)` indicates a failure.
    ///
    /// # Arguments
    ///
    /// - `input`: The data required to perform the task, defined by the
    ///   associated type `Input`.
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
        Span::new().minutes(15)
    }

    /// Provides task time-to-live duration in queue.
    ///
    /// After the duration has elapsed, a task may be removed from the queue,
    /// e.g. via [`delete_expired`](Queue.delete_expired).
    fn ttl(&self) -> Span {
        Span::new().days(14)
    }

    /// Provides the time at which the task is available for processing.
    ///
    /// Any future value will delay the task execution until that point in time.
    fn available_at(&self) -> Zoned {
        Timestamp::now().to_zoned(TimeZone::UTC)
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

/// Configuration of a policy for retries in case of task failure.
///
/// The `RetryPolicy` struct defines how failed tasks are retried. It controls
/// how many times the task should be retried, the intervals between retries,
/// and how much time each retry will take. The retry interval can also grow
/// over time using an exponential backoff coefficient.
#[derive(Debug, Clone)]
pub struct RetryPolicy {
    pub(crate) max_attempts: i32,
    pub(crate) initial_interval_ms: i32,
    pub(crate) max_interval_ms: i32,
    pub(crate) backoff_coefficient: f32,
}

pub(crate) type RetryCount = i32;

impl RetryPolicy {
    pub fn calculate_delay(&self, retry_count: RetryCount) -> Span {
        let base_delay = self.initial_interval_ms as f32;
        let backoff_delay = base_delay * self.backoff_coefficient.powi(retry_count - 1);
        let delay = backoff_delay.min(self.max_interval_ms as f32);
        Span::new().milliseconds(delay as i64)
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

/// The default implementation of the `RetryPolicy` provides a baseline retry
/// configuration for most tasks, with 5 retry attempts, an initial 1-second
/// delay, and exponential backoff.
impl Default for RetryPolicy {
    fn default() -> Self {
        DEFAULT_RETRY_POLICY
    }
}

/// A builder for constructing custom `RetryPolicy` objects.
///
/// The `RetryPolicyBuilder` allows for more flexible configuration of retry
/// behavior. You can specify the number of retry attempts, the initial and
/// maximum retry intervals, and the backoff coefficient.
///
/// # Example:
///
/// ```
/// use underway::task::RetryPolicyBuilder;
///
/// let retry_policy = RetryPolicyBuilder::new()
///     .max_attempts(3)
///     .initial_interval_ms(500)
///     .max_interval_ms(5000)
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
    pub const fn max_attempts(mut self, max_attempts: i32) -> Self {
        self.inner.max_attempts = max_attempts;
        self
    }

    /// Sets the initial interval before the first retry (in milliseconds).
    pub const fn initial_interval_ms(mut self, initial_interval_ms: i32) -> Self {
        self.inner.initial_interval_ms = initial_interval_ms;
        self
    }

    /// Sets the maximum interval between retries (in milliseconds).
    pub const fn max_interval_ms(mut self, max_interval_ms: i32) -> Self {
        self.inner.max_interval_ms = max_interval_ms;
        self
    }

    /// Sets the backoff coefficient to apply after each retry.
    pub const fn backoff_coefficient(mut self, backoff_coefficient: f32) -> Self {
        self.inner.backoff_coefficient = backoff_coefficient;
        self
    }

    /// Builds the `RetryPolicy` with the configured parameters.
    pub const fn build(self) -> RetryPolicy {
        self.inner
    }
}

/// Represents the possible states of a task in the `underway` system.
///
/// Task states track the progress of tasks as they move through the system.
/// Each state represents a specific phase of a task's lifecycle, from being
/// queued to being completed or cancelled.
#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, sqlx::Type)]
#[sqlx(type_name = "underway.task_state", rename_all = "snake_case")]
pub enum State {
    /// Task is waiting to be executed.
    Pending,

    /// Task is currently being processed by a worker.
    InProgress,

    /// Task has completed successfully.
    Succeeded,

    /// Task has been cancelled.
    Cancelled,

    /// Task has failed and will not be retried further.
    Failed,
}

#[cfg(test)]
mod tests {
    use serde::{Deserialize, Serialize};

    use super::*;

    /// A basic task for testing purposes.
    #[derive(Debug, Deserialize, Serialize)]
    struct TestTask {
        message: String,
    }

    /// Task implementation for testing.
    struct PrintTask;

    impl Task for PrintTask {
        type Input = TestTask;

        async fn execute(&self, input: Self::Input) -> Result {
            println!("Executing task with message: {}", input.message);
            if input.message == "fail" {
                return Err(Error::Generic("Task failed".to_string()));
            }
            Ok(())
        }
    }

    #[tokio::test]
    async fn task_execution_success() {
        let task = PrintTask;
        let input = TestTask {
            message: "Hello, World!".to_string(),
        };

        let result = task.execute(input).await;
        assert!(result.is_ok())
    }

    #[tokio::test]
    async fn task_execution_failure() {
        let task = PrintTask;
        let input = TestTask {
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
