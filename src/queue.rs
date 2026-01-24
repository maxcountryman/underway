//! Queues provide an interface for managing task lifecycle.
//!
//! Tasks are enqueued onto the queue, using the [`Queue::enqueue`] method, and
//! later dequeued, using the [`Queue::dequeue`] method, when they're executed.
//!
//! Each queue is identified by a unique name and manages a specific
//! implementation of `Task`. In other words, a given queue always manages
//! exactly one kind of task and only that kind of task.
//!
//! The semantics for retrieving a task from the queue are defined by the order
//! of insertion, first-in, first-out (FIFO), or the priority the task defines.
//! If a priority is defined, then priority is considered before the order the
//! task was inserted.
//!
//! # Dead-letter queues
//!
//! When a dead-letter queue name is provided, a secondary queue is created with
//! this name. This is a queue of "dead letters". In other words, it's a queue
//! of tasks that have failed and can't be retried.
//!
//! Dead-letter queues can be useful for identifying patterns of failures or
//! reprocessing failed tasks at later date.
//!
//! To enable dead-letter queues, simply provide its name:
//!
//! ```rust,no_run
//! # use tokio::runtime::Runtime;
//! # use underway::Task;
//! # use underway::task::Result as TaskResult;
//! # use sqlx::PgPool;
//! # use sqlx::{Transaction, Postgres};
//! use underway::Queue;
//!
//! # struct MyTask;
//! # impl Task for MyTask {
//! #    type Input = ();
//! #    type Output = ();
//! #    async fn execute(&self, _tx: Transaction<'_, Postgres>, input: Self::Input) -> TaskResult<Self::Output> {
//! #        Ok(())
//! #    }
//! # }
//! # fn main() {
//! # let rt = Runtime::new().unwrap();
//! # rt.block_on(async {
//! # /*
//! let pool = { /* A `PgPool`. */ };
//! # */
//! # let pool = PgPool::connect(&std::env::var("DATABASE_URL")?).await?;
//! let queue = Queue::builder()
//!     .name("example_queue")
//!     // Enable the dead-letter queue.
//!     .dead_letter_queue("example_dlq")
//!     .pool(pool.clone())
//!     .build()
//!     .await?;
//! # queue.enqueue(&pool, &MyTask, &()).await?;
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! # });
//! # }
//! ```
//!
//! # Processing tasks
//!
//! Note that while queues provide methods for enqueuing and dequeuing tasks,
//! you will generaly not use these methods directly and instead use jobs and
//! their workers, respectively.
//!
//! For example, `Job` provides an [`enqueue`](crate::Job::enqueue) method,
//! which wraps its queue's enqueue method. Likewise, when a job spins up a
//! worker via its [`run`](crate::Job::run) method, that worker uses its queue's
//! dequeue method and in both cases there's no need to use the queue methods
//! directly.
//!
//! With that said, a queue may be interfaced with directly and operated
//! manually if desired:
//!
//! ```rust,no_run
//! # use tokio::runtime::Runtime;
//! # use underway::Task;
//! # use underway::task::Result as TaskResult;
//! # use sqlx::{Transaction, Postgres, PgPool};
//! use underway::Queue;
//!
//! # struct MyTask;
//! # impl Task for MyTask {
//! #    type Input = ();
//! #    type Output = ();
//! #    async fn execute(&self, _tx: Transaction<'_, Postgres>, input: Self::Input) -> TaskResult<Self::Output> {
//! #        Ok(())
//! #    }
//! # }
//! # fn main() {
//! # let rt = Runtime::new().unwrap();
//! # rt.block_on(async {
//! # /*
//! let pool = { /* A `PgPool`. */ };
//! # */
//! # let pool = PgPool::connect(&std::env::var("DATABASE_URL")?).await?;
//! let queue = Queue::builder()
//!     .name("example_queue")
//!     .pool(pool.clone())
//!     .build()
//!     .await?;
//!
//! # /*
//! let task = { /* A type that implements `Task`. */ };
//! # */
//! # let task = MyTask;
//!
//! // Enqueue the task.
//! queue.enqueue(&pool, &task, &()).await?;
//!
//! // Retrieve an available task for processing.
//! if let Some(in_progress_task) = queue.dequeue().await? {
//!     // Process the task here
//! }
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! # });
//! # }
//! ```
//!
//! Note that dequeuing a task will set its state to "in-progress" and create an
//! associated attempt row. Once dequeued in this manner, the state of the
//! in-progress task must be managed through the [`InProgressTask`] interface.
//! In particular, the task should eventually reach a terminal state.
//!
//! Only tasks which are in the "pending" state or are otherwise considered
//! stale (i.e. they have not updated their heartbeat within the expected time
//! frame) and with remaining retries can be dequeued.
//!
//! # Scheduling tasks
//!
//! It's important to note that a schedule **must** be set on the queue before
//! the scheduler can be run.
//!
//! As with task processing, jobs provide an interface for scheduling. For
//! example, the [`schedule`](crate::Job::schedule) method schedules the job.
//! Once scheduled, a scheduler must be run to ensure exeuction.
//!
//! Of course it's also possible to interface directly with the queue to achieve
//! the same if desired. Schedules can be set with the
//! [`Queue::schedule`](Queue::schedule) method. Once set, a scheduler can be
//! used to run the schedule via the [`Scheduler::run`](crate::Scheduler::run)
//! method:
//!
//! ```rust,no_run
//! # use tokio::runtime::Runtime;
//! # use underway::Task;
//! # use underway::task::Result as TaskResult;
//! # use sqlx::{Transaction, Postgres, PgPool};
//! use underway::{Queue, Scheduler};
//!
//! # struct MyTask;
//! # impl Task for MyTask {
//! #    type Input = ();
//! #    type Output = ();
//! #    async fn execute(&self, _tx: Transaction<'_, Postgres>, input: Self::Input) -> TaskResult<Self::Output> {
//! #        Ok(())
//! #    }
//! # }
//! # fn main() {
//! # let rt = Runtime::new().unwrap();
//! # rt.block_on(async {
//! # /*
//! let pool = { /* A `PgPool`. */ };
//! # */
//! # let pool = PgPool::connect(&std::env::var("DATABASE_URL")?).await?;
//! let queue = Queue::builder()
//!     .name("example_queue")
//!     .pool(pool.clone())
//!     .build()
//!     .await?;
//!
//! // Set a quarter-hour schedule; IANA timezones are mandatory.
//! let quarter_hour = "0 */15 * * * *[America/Los_Angeles]".parse()?;
//! queue.schedule(&pool, &quarter_hour, &()).await?;
//!
//! # /*
//! let task = { /* A type that implements `Task`. */ };
//! # */
//! # let task = MyTask;
//! let scheduler = Scheduler::new(queue.into(), task);
//!
//! // Run a scheduler based on our configured schedule.
//! scheduler.run().await?;
//!
//! // Don't forget that there's no workers running, so even if we schedule work, nothing will
//! // happen!
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! # });
//! # }
//! ```
//!
//! # Deleting expired tasks
//!
//! Tasks configure a time-to-live (TTL) which specifies how long they'll remain
//! in the queue. However, tasks are only deleted from the queue if the deletion
//! routine is explicitly invoked. Either [`run_deletion`] or
//! [`run_deletion_every`] should be called to start the deletion routine.
//!
//! **Note**: Tasks will not be deleted from the queue if this routine is not
//! running!
//!
//! ```rust,no_run
//! # use tokio::runtime::Runtime;
//! # use underway::Task;
//! # use underway::task::Result as TaskResult;
//! # use sqlx::{Postgres, PgPool, Transaction};
//! use underway::queue;
//!
//! # struct MyTask;
//! # impl Task for MyTask {
//! #    type Input = ();
//! #    type Output = ();
//! #    async fn execute(&self, _tx: Transaction<'_, Postgres>, input: Self::Input) -> TaskResult<Self::Output> {
//! #        Ok(())
//! #    }
//! # }
//! # fn main() {
//! # let rt = Runtime::new().unwrap();
//! # rt.block_on(async {
//! # /*
//! let pool = { /* A `PgPool`. */ };
//! # */
//! # let pool = PgPool::connect(&std::env::var("DATABASE_URL")?).await?;
//!
//! // Ensure we remove tasks that have an expired TTL.
//! queue::run_deletion(&pool).await?;
//!
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! # });
//! # }
//! ```

use std::{borrow::Cow, marker::PhantomData, sync::OnceLock, time::Duration as StdDuration};

use builder_states::{Initial, NameSet, PoolSet};
use jiff::{Span, ToSpan};
use sqlx::{
    postgres::{PgAdvisoryLock, PgAdvisoryLockGuard},
    Acquire, PgConnection, PgExecutor, PgPool, Postgres, Transaction,
};
use tracing::instrument;
use uuid::Uuid;

use crate::{
    task::{Error as TaskError, RetryPolicy, State as TaskState, Task, TaskId},
    ZonedSchedule,
};

type Result<T = ()> = std::result::Result<T, Error>;

/// Queue errors.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Error returned by the `sqlx` crate during database operations.
    #[error(transparent)]
    Database(#[from] sqlx::Error),

    /// Error returned by the `serde_json` crate when serializing or
    /// deserializing task input.
    #[error(transparent)]
    Json(#[from] serde_json::Error),

    /// Error returned by the `jiff_cron` crate.
    #[error(transparent)]
    Cron(#[from] jiff_cron::error::Error),

    /// Error returned by the `jiff` crate.
    #[error(transparent)]
    Jiff(#[from] jiff::Error),

    /// Indicates that the task couldn't be found.
    ///
    /// This could be due to the task not existing at all or other clauses in
    /// the query that prevent a row from being returned.
    #[error("Task with ID {0} not found.")]
    TaskNotFound(TaskId),

    /// Indicates that the schedule associated with the task is malformed.
    #[error("A malformed schedule was retrieved.")]
    MalformedSchedule,
}

/// Task queue.
///
/// Queues are responsible for managing task lifecycle.
#[derive(Debug)]
pub struct Queue<T: Task> {
    pub(crate) name: String,
    pub(crate) dlq_name: Option<String>,
    pub(crate) pool: PgPool,
    _marker: PhantomData<T>,
}

#[derive(Debug, Clone)]
pub(crate) struct TaskConfig {
    retry_policy: RetryPolicy,
    timeout: Span,
    heartbeat: Span,
    ttl: Span,
    delay: Span,
    concurrency_key: Option<String>,
    priority: i32,
}

impl TaskConfig {
    pub(crate) fn new(
        retry_policy: RetryPolicy,
        timeout: Span,
        ttl: Span,
        delay: Span,
        heartbeat: Span,
        concurrency_key: Option<String>,
        priority: i32,
    ) -> Self {
        Self {
            retry_policy,
            timeout,
            heartbeat,
            ttl,
            delay,
            concurrency_key,
            priority,
        }
    }

    fn for_task<T: Task>(task: &T) -> Self {
        Self {
            retry_policy: task.retry_policy(),
            timeout: task.timeout(),
            heartbeat: task.heartbeat(),
            ttl: task.ttl(),
            delay: task.delay(),
            concurrency_key: task.concurrency_key(),
            priority: task.priority(),
        }
    }
}

impl<T: Task> Clone for Queue<T> {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            dlq_name: self.dlq_name.clone(),
            pool: self.pool.clone(),
            _marker: PhantomData,
        }
    }
}

impl<T: Task> Queue<T> {
    /// Creates a builder for a new queue.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use sqlx::{PgPool, Transaction, Postgres};
    /// # use underway::{Task, task::Result as TaskResult};
    /// use underway::Queue;
    ///
    /// # struct ExampleTask;
    /// # impl Task for ExampleTask {
    /// #     type Input = ();
    /// #     type Output = ();
    /// #     async fn execute(
    /// #         &self,
    /// #         _: Transaction<'_, Postgres>,
    /// #         _: Self::Input,
    /// #     ) -> TaskResult<Self::Output> {
    /// #         Ok(())
    /// #     }
    /// # }
    /// # use tokio::runtime::Runtime;
    /// # fn main() {
    /// # let rt = Runtime::new().unwrap();
    /// # rt.block_on(async {
    /// # let pool = PgPool::connect(&std::env::var("DATABASE_URL")?).await?;
    /// # /*
    /// let pool = { /* A `PgPool`. */ };
    /// # */
    /// let queue: Queue<ExampleTask> = Queue::builder()
    ///     .name("example")
    ///     .dead_letter_queue("example_dlq")
    ///     .pool(pool)
    ///     .build()
    ///     .await?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # });
    /// # }
    /// ```
    pub fn builder() -> Builder<T, Initial> {
        Builder::default()
    }

    /// Enqueues a new task into the task queue, returning the task's unique ID.
    ///
    /// This function inserts a new task into the database using the provided
    /// executor. By using a transaction as the executor, you can ensure
    /// that the task is only enqueued if the transaction successfully
    /// commits.
    ///
    /// The enqueued task will have its retry policy, timeout, time-to-live,
    /// delay, concurrency key, and priority configured as specified by the task
    /// type.
    ///
    /// An ID, which is a [`ULID`][ULID] converted to `UUIDv4`, is also assigned
    /// to the task and returned upon successful enqueue.
    ///
    /// **Note:** If you pass a transactional executor and the transaction is
    /// rolled back, the returned task ID will not correspond to any persisted
    /// task.
    ///
    /// # Errors
    ///
    /// This function will return an error if:
    ///
    /// - The `input` cannot be serialized to JSON.
    /// - The database operation fails during the insertion.
    /// - Any of `timeout`, `delay`, or `ttl` cannot be converted to
    ///   `std::time::Duration`.
    ///
    /// [ULID]: https://github.com/ulid/spec?tab=readme-ov-file#specification
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use sqlx::{PgPool, Transaction, Postgres};
    /// # use underway::{Task, task::Result as TaskResult};
    /// # use underway::Queue;
    /// # struct ExampleTask;
    /// # impl Task for ExampleTask {
    /// #     type Input = ();
    /// #     type Output = ();
    /// #     async fn execute(
    /// #         &self,
    /// #         _: Transaction<'_, Postgres>,
    /// #         _: Self::Input,
    /// #     ) -> TaskResult<Self::Output> {
    /// #         Ok(())
    /// #     }
    /// # }
    /// # use tokio::runtime::Runtime;
    /// # fn main() {
    /// # let rt = Runtime::new().unwrap();
    /// # rt.block_on(async {
    /// # let pool = PgPool::connect(&std::env::var("DATABASE_URL")?).await?;
    /// # /*
    /// let pool = { /* A `PgPool`. */ };
    /// # */
    /// #
    /// # let queue = Queue::builder()
    /// #     .name("example")
    /// #     .pool(pool.clone())
    /// #     .build()
    /// #     .await?;
    /// # /*
    /// let queue = { /* A `Queue`. */ };
    /// # */
    /// # let task = ExampleTask;
    /// # /*
    /// let task = { /* An implementer of `Task`. */ };
    /// # */
    /// #
    ///
    /// // Enqueue a new task with input.
    /// let task_id = queue.enqueue(&pool, &task, &()).await?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # });
    /// # }
    /// ```
    pub async fn enqueue<'a, E>(&self, executor: E, task: &T, input: &T::Input) -> Result<TaskId>
    where
        E: PgExecutor<'a>,
    {
        let config = TaskConfig::for_task(task);
        self.enqueue_with_config(executor, input, &config, Span::new())
            .await
    }

    /// Same as [`enqueue`](Queue::enqueue), but the task doesn't become
    /// available until after the specified delay.
    ///
    /// **Note:** The provided delay is added to the task's configured delay.
    /// This means that if you provide a five-minute delay and the task is
    /// already configured with a thirty-second delay the task will not be
    /// dequeued for at least five and half minutes.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use sqlx::{PgPool, Transaction, Postgres};
    /// # use underway::{Task, task::Result as TaskResult};
    /// # use underway::Queue;
    /// use jiff::ToSpan;
    ///
    /// # struct ExampleTask;
    /// # impl Task for ExampleTask {
    /// #     type Input = ();
    /// #     type Output = ();
    /// #     async fn execute(
    /// #         &self,
    /// #         _: Transaction<'_, Postgres>,
    /// #         _: Self::Input,
    /// #     ) -> TaskResult<Self::Output> {
    /// #         Ok(())
    /// #     }
    /// # }
    /// # use tokio::runtime::Runtime;
    /// # fn main() {
    /// # let rt = Runtime::new().unwrap();
    /// # rt.block_on(async {
    /// # let pool = PgPool::connect(&std::env::var("DATABASE_URL")?).await?;
    /// # /*
    /// let pool = { /* A `PgPool`. */ };
    /// # */
    /// # let queue = Queue::builder()
    /// #     .name("example")
    /// #     .pool(pool.clone())
    /// #     .build()
    /// #     .await?;
    /// # /*
    /// let queue = { /* A `Queue`. */ };
    /// # */
    /// # let task = ExampleTask;
    /// # /*
    /// let task = { /* An implementer of `Task`. */ };
    /// # */
    /// #
    ///
    /// // Enqueue a new task with input after five minutes.
    /// let task_id = queue.enqueue_after(&pool, &task, &(), 5.minutes()).await?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # });
    /// # }
    /// ```
    pub async fn enqueue_after<'a, E>(
        &self,
        executor: E,
        task: &T,
        input: &T::Input,
        delay: Span,
    ) -> Result<TaskId>
    where
        E: PgExecutor<'a>,
    {
        let config = TaskConfig::for_task(task);
        self.enqueue_with_config(executor, input, &config, delay)
            .await
    }

    pub(crate) async fn enqueue_with_config<'a, E>(
        &self,
        executor: E,
        input: &T::Input,
        config: &TaskConfig,
        delay: Span,
    ) -> Result<TaskId>
    where
        E: PgExecutor<'a>,
    {
        let calculated_delay = config.delay.checked_add(delay)?;
        self.enqueue_with_delay(executor, input, config, calculated_delay)
            .await
    }

    // Explicitly provide for a delay so that we can also facilitate calculated
    // retries, i.e. `enqueue_after`.
    #[instrument(
        name = "enqueue",
        skip(self, executor, input, config),
        fields(queue.name = self.name, task.id = tracing::field::Empty),
        err
    )]
    async fn enqueue_with_delay<'a, E>(
        &self,
        executor: E,
        input: &T::Input,
        config: &TaskConfig,
        delay: Span,
    ) -> Result<TaskId>
    where
        E: PgExecutor<'a>,
    {
        let id = TaskId::new();

        let input_value = serde_json::to_value(input)?;

        let retry_policy = config.retry_policy;
        let timeout = config.timeout;
        let heartbeat = config.heartbeat;
        let ttl = config.ttl;
        let concurrency_key = config.concurrency_key.clone();
        let priority = config.priority;

        tracing::Span::current().record("task.id", id.as_hyphenated().to_string());

        sqlx::query!(
            r#"
            insert into underway.task (
              id,
              task_queue_name,
              input,
              timeout,
              heartbeat,
              ttl,
              delay,
              retry_policy,
              concurrency_key,
              priority
            ) values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
            "#,
            id as TaskId,
            self.name,
            input_value,
            StdDuration::try_from(timeout)? as _,
            StdDuration::try_from(heartbeat)? as _,
            StdDuration::try_from(ttl)? as _,
            StdDuration::try_from(delay)? as _,
            retry_policy as RetryPolicy,
            concurrency_key,
            priority
        )
        .execute(executor)
        .await?;

        Ok(id)
    }

    /// Enqueues tasks in chunks (max 5000 per batch) within a single
    /// transaction.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use sqlx::{PgPool, Transaction, Postgres};
    /// # use underway::{Task, task::Result as TaskResult};
    /// # use underway::Queue;
    ///
    /// # struct ExampleTask;
    /// # impl Task for ExampleTask {
    /// #     type Input = ();
    /// #     type Output = ();
    /// #     async fn execute(
    /// #         &self,
    /// #         _: Transaction<'_, Postgres>,
    /// #         _: Self::Input,
    /// #     ) -> TaskResult<Self::Output> {
    /// #         Ok(())
    /// #     }
    /// # }
    /// # use tokio::runtime::Runtime;
    /// # fn main() {
    /// # let rt = Runtime::new().unwrap();
    /// # rt.block_on(async {
    /// # let pool = PgPool::connect(&std::env::var("DATABASE_URL")?).await?;
    /// # /*
    /// let pool = { /* A `PgPool`. */ };
    /// # */
    /// # let queue = Queue::builder()
    /// #     .name("example")
    /// #     .pool(pool.clone())
    /// #     .build()
    /// #     .await?;
    /// # /*
    /// let queue = { /* A `Queue`. */ };
    /// # */
    /// # let task = ExampleTask;
    /// # /*
    /// let task = { /* An implementer of `Task`. */ };
    /// # */
    /// #
    ///
    /// // Enqueue a new task with input after five minutes.
    /// let _ = queue.enqueue_multi(&pool, &task, &[(), ()]).await?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # });
    /// # }
    /// ```
    #[instrument(
        name = "enqueue_multi",
        skip(self, executor, task, inputs),
        fields(queue.name = self.name, task.id = tracing::field::Empty),
        err
    )]
    pub async fn enqueue_multi<'a, E>(
        &self,
        executor: E,
        task: &T,
        inputs: &[T::Input],
    ) -> Result<usize>
    where
        E: PgExecutor<'a> + sqlx::Acquire<'a, Database = sqlx::Postgres>,
    {
        self.enqueue_multi_wth_chunk_size(executor, task, inputs, 5000)
            .await
    }

    /// Same as `enqueue_multi`, but allows you to specify `chunk_size`

    #[instrument(
        name = "enqueue_multi",
        skip(self, executor, task, inputs),
        fields(queue.name = self.name, task.id = tracing::field::Empty),
        err
    )]
    pub async fn enqueue_multi_wth_chunk_size<'a, E>(
        &self,
        executor: E,
        task: &T,
        inputs: &[T::Input],
        chunk_size: usize,
    ) -> Result<usize>
    where
        E: PgExecutor<'a> + sqlx::Acquire<'a, Database = sqlx::Postgres>,
    {
        let tasks_number = inputs.len();

        tracing::Span::current().record("tasks_numbers", tasks_number.to_string());

        let config = TaskConfig::for_task(task);
        let timeout = config.timeout;
        let heartbeat = config.heartbeat;
        let ttl = config.ttl;
        let retry_policy = config.retry_policy;
        let concurrency_key = config.concurrency_key.clone();
        let priority = config.priority;

        let mut tx = executor.begin().await?;

        for chunk in inputs.chunks(chunk_size) {
            let mut ids = Vec::with_capacity(chunk.len());
            let mut input_values = Vec::with_capacity(chunk.len());
            let mut delays = Vec::with_capacity(chunk.len());

            for input in chunk {
                ids.push(TaskId::new());
                input_values.push(serde_json::to_value(input)?);
                delays.push(StdDuration::try_from(config.delay)?);
            }

            sqlx::query!(
            r#"
            insert into underway.task (
              id,
              task_queue_name,
              input,
              timeout,
              heartbeat,
              ttl,
              delay,
              retry_policy,
              concurrency_key,
              priority
            )
            select t.id, $1 as task_queue_name, t.input, $2 as timeout, $3 as heartbeat, $4 as ttl, t.delay, $5 as retry_policy, $6 as concurrency_key, $7 as priority
            from unnest($8::uuid[], $9::jsonb[], $10::interval[]) as t(id, input, delay)
            "#,
            self.name,
            StdDuration::try_from(timeout)? as _,
            StdDuration::try_from(heartbeat)? as _,
            StdDuration::try_from(ttl)? as _,
            retry_policy as RetryPolicy,
            concurrency_key,
            priority,

            &ids as _,
            &input_values,
            delays as _,
        )
        .execute(tx.as_mut())
        .await?;
        }

        tx.commit().await?;

        Ok(tasks_number)
    }

    /// Dequeues the next available task.
    ///
    /// This method uses the `FOR UPDATE SKIP LOCKED` clause to ensure efficient
    /// retrieval of pending task rows.
    ///
    /// If an available task is found, it's marked as "in progress".
    ///
    /// # Transactions, locks, and timeouts
    ///
    /// At the beginning of this operation, a new transaction is started
    /// (otherwise if already in a transaction a savepoint is created). Before
    /// returning, this transaction is committed. It's important to point out
    /// that the row locked by this transaction will be locked until the
    /// transaction is committed, rolled back, or is otherwise reset,
    /// e.g. via a timeout.
    ///
    /// More specifically the implication is that transactions should set a
    /// reasonable timeout to ensure tasks are returned to the queue on a
    /// reasonable time horizon. Note that Underway does not currently set any
    /// database-level timeouts.
    ///
    /// # Errors
    ///
    /// This function will return an error if:
    ///
    /// - The database operation fails during select.
    /// - The database operation fails during update.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use sqlx::{PgPool, Transaction, Postgres};
    /// # use underway::{Task, task::Result as TaskResult};
    /// # use underway::Queue;
    /// # struct ExampleTask;
    /// # impl Task for ExampleTask {
    /// #     type Input = ();
    /// #     type Output = ();
    /// #     async fn execute(
    /// #         &self,
    /// #         _: Transaction<'_, Postgres>,
    /// #         _: Self::Input,
    /// #     ) -> TaskResult<Self::Output> {
    /// #         Ok(())
    /// #     }
    /// # }
    /// # use tokio::runtime::Runtime;
    /// # fn main() {
    /// # let rt = Runtime::new().unwrap();
    /// # rt.block_on(async {
    /// # let pool = PgPool::connect(&std::env::var("DATABASE_URL")?).await?;
    /// # /*
    /// let pool = { /* A `PgPool`. */ };
    /// # */
    /// # let queue = Queue::builder()
    /// #     .name("example")
    /// #     .pool(pool.clone())
    /// #     .build()
    /// #     .await?;
    /// # /*
    /// let queue = { /* A `Queue`. */ };
    /// # */
    /// # let task = ExampleTask;
    /// # /*
    /// let task = { /* An implementer of `Task`. */ };
    /// # */
    /// #
    ///
    /// // Enqueue a new task.
    /// queue.enqueue(&pool, &task, &()).await?;
    ///
    /// // Dequeue the enqueued task.
    /// let mut tx = pool.begin().await?;
    /// let pending_task = queue
    ///     .dequeue()
    ///     .await?
    ///     .expect("There should be a pending task.");
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # });
    /// # }
    /// ```
    #[instrument(
        skip(self),
        fields(queue.name = self.name, task.id = tracing::field::Empty),
        err
    )]
    pub async fn dequeue(&self) -> Result<Option<InProgressTask>> {
        // Transaction scoped to finding the next task and setting its state to
        // "in-progress".
        let mut tx = self.pool.begin().await?;

        let in_progress_task = sqlx::query_as!(
            InProgressTask,
            r#"
            with available_task as (
                select id
                from underway.task
                where task_queue_name = $1
                  and (
                      -- Find pending tasks...
                      state = $2
                      -- ...Or look for stalled tasks.
                      or (
                          state = $3
                          -- Has heartbeat stalled?
                          and last_heartbeat_at < now() - heartbeat
                          -- Are there remaining retries?
                          and (retry_policy).max_attempts > (
                              select count(*)
                              from underway.task_attempt
                              where task_queue_name = $1
                                and task_id = id
                          )
                      )
                  )
                  and created_at + delay <= now()
                order by
                  priority desc,
                  created_at,
                  id
                limit 1
                for update skip locked
            )
            update underway.task t
            set state = $3,
                last_attempt_at = now(),
                last_heartbeat_at = now()
            from available_task
            where t.task_queue_name = $1
              and t.id = available_task.id
            returning
                t.id as "id: TaskId",
                t.task_queue_name as "queue_name",
                t.input,
                t.timeout,
                t.heartbeat,
                t.retry_policy as "retry_policy: RetryPolicy",
                t.concurrency_key
            "#,
            self.name,
            TaskState::Pending as TaskState,
            TaskState::InProgress as TaskState,
        )
        .fetch_optional(&mut *tx)
        .await?;

        if let Some(in_progress_task) = &in_progress_task {
            let task_id = in_progress_task.id;
            tracing::Span::current().record("task.id", task_id.as_hyphenated().to_string());

            // Update previous in-progress task attempts to mark them as failed.
            //
            // This ensures that if a stuck task was selected, we also update its attempt
            // row.
            sqlx::query!(
                r#"
                update underway.task_attempt
                set state = $3
                where task_id = $1
                  and task_queue_name = $2
                  and state = $4
                "#,
                task_id as TaskId,
                self.name,
                TaskState::Failed as TaskState,
                TaskState::InProgress as TaskState
            )
            .execute(&mut *tx)
            .await?;

            // Insert a new task attempt row
            sqlx::query!(
                r#"
                with next_attempt as (
                    select coalesce(max(attempt_number) + 1, 1) as attempt_number
                    from underway.task_attempt
                    where task_id = $1
                      and task_queue_name = $2
                )
                insert into underway.task_attempt (
                    task_id,
                    task_queue_name,
                    state,
                    attempt_number
                )
                values (
                    $1,
                    $2,
                    $3,
                    (select attempt_number from next_attempt)
                )
                "#,
                task_id as TaskId,
                self.name,
                TaskState::InProgress as TaskState
            )
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;

        Ok(in_progress_task)
    }

    /// Creates a schedule for the queue.
    ///
    /// Schedules are useful when a task should be run periodically, according
    /// to a crontab definition.
    ///
    /// **Note:** After a schedule has been set, [a scheduler
    /// instance](crate::Scheduler) must be run in order for schedules to
    /// fire.
    ///
    /// # Errors
    ///
    /// This function will return an error if:
    ///
    /// - The input value cannot be serialized.
    /// - The database operation fails during insert.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use sqlx::{PgPool, Transaction, Postgres};
    /// # use underway::{Task, task::Result as TaskResult};
    /// # use underway::Queue;
    /// # struct ExampleTask;
    /// # impl Task for ExampleTask {
    /// #     type Input = ();
    /// #     type Output = ();
    /// #     async fn execute(
    /// #         &self,
    /// #         _: Transaction<'_, Postgres>,
    /// #         _: Self::Input,
    /// #     ) -> TaskResult<Self::Output> {
    /// #         Ok(())
    /// #     }
    /// # }
    /// # use tokio::runtime::Runtime;
    /// # fn main() {
    /// # let rt = Runtime::new().unwrap();
    /// # rt.block_on(async {
    /// # let pool = PgPool::connect(&std::env::var("DATABASE_URL")?).await?;
    /// # /*
    /// let pool = { /* A `PgPool`. */ };
    /// # */
    /// # let task = ExampleTask;
    /// # let queue = Queue::builder()
    /// #     .name("example")
    /// #     .pool(pool.clone())
    /// #     .build()
    /// #     .await?;
    /// # /*
    /// let queue = { /* A `Queue`. */ };
    /// # */
    /// # queue.enqueue(&pool, &task, &()).await?;
    ///
    /// // Set a schedule on the queue with the given input.
    /// let daily = "@daily[America/Los_Angeles]".parse()?;
    /// queue.schedule(&pool, &daily, &()).await?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # });
    /// # }
    /// ```
    #[instrument(
        skip(self, executor, zoned_schedule, input),
        fields(queue.name = self.name),
        err
    )]
    pub async fn schedule<'a, E>(
        &self,
        executor: E,
        zoned_schedule: &ZonedSchedule,
        input: &T::Input,
    ) -> Result
    where
        E: PgExecutor<'a>,
    {
        let input_value = serde_json::to_value(input)?;

        sqlx::query!(
            r#"
            insert into underway.task_schedule (
              task_queue_name,
              schedule,
              timezone,
              input
            ) values ($1, $2, $3, $4)
            on conflict (task_queue_name) do update
            set
              schedule = excluded.schedule,
              timezone = excluded.timezone,
              input = excluded.input
            "#,
            self.name,
            zoned_schedule.cron_expr(),
            zoned_schedule.iana_name(),
            input_value
        )
        .execute(executor)
        .await?;

        Ok(())
    }

    /// Removes the configured schedule for the queue, if one exsists..
    ///
    /// # Errors
    ///
    /// This function will return an error if:
    ///
    /// - The database operation fails during insert.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use sqlx::{PgPool, Transaction, Postgres};
    /// # use underway::{Task, task::Result as TaskResult};
    /// # use underway::Queue;
    /// # struct ExampleTask;
    /// # impl Task for ExampleTask {
    /// #     type Input = ();
    /// #     type Output = ();
    /// #     async fn execute(
    /// #         &self,
    /// #         _: Transaction<'_, Postgres>,
    /// #         _: Self::Input,
    /// #     ) -> TaskResult<Self::Output> {
    /// #         Ok(())
    /// #     }
    /// # }
    /// # use tokio::runtime::Runtime;
    /// # fn main() {
    /// # let rt = Runtime::new().unwrap();
    /// # rt.block_on(async {
    /// # let pool = PgPool::connect(&std::env::var("DATABASE_URL")?).await?;
    /// # /*
    /// let pool = { /* A `PgPool`. */ };
    /// # */
    /// # let task = ExampleTask;
    /// # let queue = Queue::builder()
    /// #     .name("example")
    /// #     .pool(pool.clone())
    /// #     .build()
    /// #     .await?;
    /// # /*
    /// let queue = { /* A `Queue`. */ };
    /// # */
    /// # queue.enqueue(&pool, &task, &()).await?;
    ///
    /// // Unset the schedule if one was set.
    /// queue.unschedule(&pool).await?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # });
    /// # }
    /// ```
    #[instrument(
        skip_all,
        fields(queue.name = self.name),
        err
    )]
    pub async fn unschedule<'a, E>(&self, executor: E) -> Result
    where
        E: PgExecutor<'a>,
    {
        sqlx::query!(
            r#"
            delete from underway.task_schedule
            where task_queue_name = $1
            "#,
            self.name,
        )
        .execute(executor)
        .await?;

        Ok(())
    }

    #[instrument(skip(self, executor), err)]
    pub(crate) async fn task_schedule<'a, E>(
        &self,
        executor: E,
    ) -> Result<Option<(ZonedSchedule, T::Input)>>
    where
        E: PgExecutor<'a>,
    {
        let Some(schedule_row) = sqlx::query!(
            r#"
            select schedule, timezone, input from underway.task_schedule where task_queue_name = $1
            limit 1
            "#,
            self.name,
        )
        .fetch_optional(executor)
        .await?
        else {
            return Ok(None);
        };

        let zoned_schedule = ZonedSchedule::new(&schedule_row.schedule, &schedule_row.timezone)
            .map_err(|_| Error::MalformedSchedule)?;
        let input = serde_json::from_value(schedule_row.input)?;

        Ok(Some((zoned_schedule, input)))
    }

    #[instrument(skip(executor, name), fields(queue.name = name), err)]
    pub(crate) async fn create<'a, E>(executor: E, name: &str) -> Result
    where
        E: PgExecutor<'a>,
    {
        sqlx::query!(
            r#"
            insert into underway.task_queue (name) values ($1)
            on conflict do nothing
            "#,
            name
        )
        .execute(executor)
        .await?;

        Ok(())
    }

    pub(crate) async fn move_task_to_dlq<'a, E>(
        &self,
        executor: E,
        task_id: TaskId,
        dlq_name: &str,
    ) -> Result
    where
        E: PgExecutor<'a>,
    {
        let result = sqlx::query!(
            r#"
            update underway.task
            set task_queue_name = $2
            where id = $1
            "#,
            task_id as TaskId,
            dlq_name
        )
        .execute(executor)
        .await?;

        if result.rows_affected() == 0 {
            return Err(Error::TaskNotFound(task_id));
        }

        Ok(())
    }
}

/// Represents an in-progress task that's been dequeued.
///
/// Importantly, Postgres manages a lock over the row this task belongs to. This
/// means that so long as database operations over this are performed in the
/// same transaction they are atomic.
///
/// Typically this will not be used directly. Instead workers use this type to
/// manage state transitions as they process the task. In fact, all valid state
/// transitions are encapsulated by it and this is the only interface through
/// which task state should be altered.
#[derive(Debug, Clone, sqlx::FromRow)]
pub struct InProgressTask {
    pub(crate) id: TaskId,
    pub(crate) queue_name: String,
    pub(crate) input: serde_json::Value,
    pub(crate) timeout: sqlx::postgres::types::PgInterval,
    pub(crate) heartbeat: sqlx::postgres::types::PgInterval,
    pub(crate) retry_policy: RetryPolicy,
    pub(crate) concurrency_key: Option<String>,
}

impl InProgressTask {
    pub(crate) async fn mark_succeeded(&self, conn: &mut PgConnection) -> Result {
        // Update the task attempt row.
        let result = sqlx::query!(
            r#"
            update underway.task_attempt
            set state = $3,
                updated_at = now(),
                completed_at = now()
            where task_id = $1
              and task_queue_name = $2
              and attempt_number = (
                  select attempt_number
                  from underway.task_attempt
                  where task_id = $1
                    and task_queue_name = $2
                  order by attempt_number desc
                  limit 1
              )
            "#,
            self.id as TaskId,
            self.queue_name,
            TaskState::Succeeded as TaskState
        )
        .execute(&mut *conn)
        .await?;

        if result.rows_affected() == 0 {
            return Err(Error::TaskNotFound(self.id));
        }

        // Update the task row.
        let result = sqlx::query!(
            r#"
            update underway.task
            set state = $2,
                updated_at = now(),
                completed_at = now()
            where id = $1
            "#,
            self.id as TaskId,
            TaskState::Succeeded as TaskState
        )
        .execute(&mut *conn)
        .await?;

        if result.rows_affected() == 0 {
            return Err(Error::TaskNotFound(self.id));
        }

        Ok(())
    }

    pub(crate) async fn mark_cancelled(&self, conn: &mut PgConnection) -> Result<bool> {
        // Update task attempt row if one exists.
        //
        // N.B.: A task may be cancelled before an attempt row has been created.
        sqlx::query!(
            r#"
            update underway.task_attempt
            set state = $3,
                updated_at = now(),
                completed_at = now()
            where task_id = $1
              and task_queue_name = $2
              and attempt_number = (
                  select attempt_number
                  from underway.task_attempt
                  where task_id = $1
                    and task_queue_name = $2
                    and state < $4
                  order by attempt_number desc
                  limit 1
              )
            "#,
            self.id as TaskId,
            self.queue_name,
            TaskState::Cancelled as TaskState,
            TaskState::Succeeded as TaskState
        )
        .execute(&mut *conn)
        .await?;

        // Update task row.
        let result = sqlx::query!(
            r#"
            update underway.task
            set state = $2,
                updated_at = now(),
                completed_at = now()
            where id = $1 and state < $3
            "#,
            self.id as TaskId,
            TaskState::Cancelled as TaskState,
            TaskState::Succeeded as TaskState
        )
        .execute(&mut *conn)
        .await?;

        if result.rows_affected() == 0 {
            return Err(Error::TaskNotFound(self.id));
        }

        Ok(result.rows_affected() > 0)
    }

    #[instrument(
        skip(self, conn),
        fields(queue.name = self.queue_name, task.id = %self.id.as_hyphenated()),
        err
    )]
    pub(crate) async fn retry_after(&self, conn: &mut PgConnection, delay: Span) -> Result {
        let result = sqlx::query!(
            r#"
            update underway.task_attempt
            set state = $3,
                updated_at = now(),
                completed_at = now()
            where task_id = $1
              and task_queue_name = $2
              and attempt_number = (
                  select attempt_number
                  from underway.task_attempt
                  where task_id = $1
                    and task_queue_name = $2
                  order by attempt_number desc
                  limit 1
              )
            "#,
            self.id as TaskId,
            self.queue_name,
            TaskState::Failed as TaskState
        )
        .execute(&mut *conn)
        .await?;

        if result.rows_affected() == 0 {
            return Err(Error::TaskNotFound(self.id));
        }

        let result = sqlx::query!(
            r#"
            update underway.task
            set state = $3,
                delay = $2,
                updated_at = now()
            where id = $1
            "#,
            self.id as TaskId,
            StdDuration::try_from(delay)? as _,
            TaskState::Pending as TaskState
        )
        .execute(&mut *conn)
        .await?;

        if result.rows_affected() == 0 {
            return Err(Error::TaskNotFound(self.id));
        }

        Ok(())
    }

    pub(crate) async fn record_failure(
        &self,
        conn: &mut PgConnection,
        error: &TaskError,
    ) -> Result {
        let result = sqlx::query!(
            r#"
            update underway.task_attempt
            set state = $3,
                updated_at = now(),
                error_message = $4
            where task_id = $1
              and task_queue_name = $2
              and attempt_number = (
                  select attempt_number
                  from underway.task_attempt
                  where task_id = $1
                    and task_queue_name = $2
                  order by attempt_number desc
                  limit 1
              )
            "#,
            self.id as TaskId,
            self.queue_name,
            TaskState::Failed as TaskState,
            error.to_string(),
        )
        .execute(&mut *conn)
        .await?;

        if result.rows_affected() == 0 {
            return Err(Error::TaskNotFound(self.id));
        }

        let result = sqlx::query!(
            r#"
            update underway.task
            set updated_at = now()
            where id = $1
              and task_queue_name = $2
            "#,
            self.id as TaskId,
            self.queue_name,
        )
        .execute(&mut *conn)
        .await?;

        if result.rows_affected() == 0 {
            return Err(Error::TaskNotFound(self.id));
        }

        Ok(())
    }

    pub(crate) async fn mark_failed(&self, conn: &mut PgConnection) -> Result {
        // Update the task attempt row.
        let result = sqlx::query!(
            r#"
            update underway.task_attempt
            set state = $3,
                updated_at = now(),
                completed_at = now()
            where task_id = $1
              and task_queue_name = $2
              and attempt_number = (
                  select attempt_number
                  from underway.task_attempt
                  where task_id = $1
                    and task_queue_name = $2
                  order by attempt_number desc
                  limit 1
              )
            "#,
            self.id as TaskId,
            self.queue_name,
            TaskState::Failed as TaskState
        )
        .execute(&mut *conn)
        .await?;

        if result.rows_affected() == 0 {
            return Err(Error::TaskNotFound(self.id));
        }

        // Update the task row.
        let result = sqlx::query!(
            r#"
            update underway.task
            set state = $2,
                updated_at = now(),
                completed_at = now()
            where id = $1
            "#,
            self.id as TaskId,
            TaskState::Failed as TaskState
        )
        .execute(&mut *conn)
        .await?;

        if result.rows_affected() == 0 {
            return Err(Error::TaskNotFound(self.id));
        }

        Ok(())
    }

    pub(crate) async fn retry_count<'a, E>(&self, executor: E) -> Result<i32>
    where
        E: PgExecutor<'a>,
    {
        Ok(sqlx::query_scalar!(
            r#"
            select count(*)::int as "count!"
            from underway.task_attempt
            where task_id = $1
              and task_queue_name = $2
              and state = $3
            "#,
            self.id as TaskId,
            self.queue_name,
            TaskState::Failed as TaskState
        )
        .fetch_one(executor)
        .await?)
    }

    pub(crate) async fn record_heartbeat<'a, E>(&self, executor: E) -> Result
    where
        E: PgExecutor<'a>,
    {
        sqlx::query!(
            r#"
            update underway.task
            set updated_at = now(),
                last_heartbeat_at = now()
            where id = $1
              and task_queue_name = $2
            "#,
            self.id as TaskId,
            self.queue_name
        )
        .execute(executor)
        .await?;

        Ok(())
    }

    pub(crate) async fn try_acquire_lock(
        &self,
        tx: &mut Transaction<'_, Postgres>,
    ) -> Result<bool> {
        try_acquire_advisory_xact_lock(tx, &self.lock_key()).await
    }

    fn lock_key(&self) -> Cow<'_, str> {
        match &self.concurrency_key {
            Some(concurrency_key) => Cow::Borrowed(concurrency_key),
            None => Cow::Owned(self.id.to_string()),
        }
    }
}

#[instrument(skip(tx), err)]
async fn try_acquire_advisory_xact_lock(
    tx: &mut Transaction<'_, Postgres>,
    key: &str,
) -> Result<bool> {
    Ok(
        sqlx::query_scalar!("select pg_try_advisory_xact_lock(hashtext($1))", key)
            .fetch_one(tx.acquire().await?)
            .await?
            .unwrap_or(false),
    )
}

#[instrument(skip(conn, lock), err)]
pub(crate) async fn try_acquire_advisory_lock<'lock, C>(
    conn: C,
    lock: &'lock PgAdvisoryLock,
) -> Result<Option<PgAdvisoryLockGuard<'lock, C>>>
where
    C: AsMut<PgConnection>,
{
    let guard = match lock.try_acquire(conn).await? {
        sqlx::Either::Left(guard) => Some(guard),
        sqlx::Either::Right(_) => None,
    };

    Ok(guard)
}

/// Runs deletion clean up of expired tasks in a loop, sleeping between
/// deletions for the specified period.
///
/// **Note:** Tasks are only deleted when this routine or `run_deletion` is
/// running.
///
/// # Errors
///
/// This function returns an error if:
///
/// - The database operation fails during deletion.
pub async fn run_deletion_every(pool: &PgPool, period: Span) -> Result {
    let mut interval = tokio::time::interval(period.try_into()?);
    interval.tick().await;
    loop {
        delete_expired(pool).await?;
        interval.tick().await;
    }
}

/// Runs deletion clean up of expired tasks every hour.
///
/// **Note:** Tasks are only deleted when this routine or `run_deletion` is
/// running.
///
/// # Errors
///
/// This function returns an error if:
///
/// - The database operation fails during deletion.
pub async fn run_deletion(pool: &PgPool) -> Result {
    run_deletion_every(pool, 1.hour()).await
}

#[instrument(skip(executor), err)]
async fn delete_expired<'a, E>(executor: E) -> Result
where
    E: PgExecutor<'a>,
{
    sqlx::query!(
        r#"
        delete from underway.task
        where state != $1 and created_at + ttl < now()
        "#,
        TaskState::InProgress as _,
    )
    .execute(executor)
    .await?;

    Ok(())
}

mod builder_states {
    use sqlx::PgPool;

    pub struct Initial;

    pub struct NameSet {
        pub name: String,
        pub dlq_name: Option<String>,
    }

    pub struct PoolSet {
        pub name: String,
        pub pool: PgPool,
        pub dlq_name: Option<String>,
    }
}

/// Builds a [`Queue`].
#[derive(Debug)]
pub struct Builder<T: Task, S> {
    state: S,
    _marker: PhantomData<T>,
}

impl<T: Task> Default for Builder<T, Initial> {
    fn default() -> Self {
        Builder::new()
    }
}

impl<T: Task> Builder<T, Initial> {
    /// Create a new queue builder.
    ///
    /// # Example
    ///
    /// ```rust
    /// # use sqlx::{Postgres, Transaction};
    /// # use underway::{Task, task::Result as TaskResult};
    /// # struct ExampleTask;
    /// # impl Task for ExampleTask {
    /// #     type Input = ();
    /// #     type Output = ();
    /// #     async fn execute(
    /// #         &self,
    /// #         _: Transaction<'_, Postgres>,
    /// #         _: Self::Input,
    /// #     ) -> TaskResult<Self::Output> {
    /// #         Ok(())
    /// #     }
    /// # }
    /// use underway::Queue;
    ///
    /// let queue_builder = Queue::<ExampleTask>::builder();
    /// ```
    pub fn new() -> Self {
        Self {
            state: Initial,
            _marker: PhantomData,
        }
    }

    /// Set the queue name.
    ///
    /// # Example
    ///
    /// ```rust
    /// # use sqlx::{Postgres, Transaction};
    /// # use underway::{Task, task::Result as TaskResult};
    /// # struct ExampleTask;
    /// # impl Task for ExampleTask {
    /// #     type Input = ();
    /// #     type Output = ();
    /// #     async fn execute(
    /// #         &self,
    /// #         _: Transaction<'_, Postgres>,
    /// #         _: Self::Input,
    /// #     ) -> TaskResult<Self::Output> {
    /// #         Ok(())
    /// #     }
    /// # }
    /// use underway::Queue;
    ///
    /// let queue_builder = Queue::<ExampleTask>::builder().name("my-queue");
    /// ```
    pub fn name(self, name: impl Into<String>) -> Builder<T, NameSet> {
        Builder {
            state: NameSet {
                name: name.into(),
                dlq_name: None,
            },
            _marker: PhantomData,
        }
    }
}

impl<T: Task> Builder<T, NameSet> {
    /// Set the dead-letter queue name.
    ///
    /// # Example
    ///
    /// ```rust
    /// # use sqlx::{Postgres, Transaction};
    /// # use underway::{Task, task::Result as TaskResult};
    /// # struct ExampleTask;
    /// # impl Task for ExampleTask {
    /// #     type Input = ();
    /// #     type Output = ();
    /// #     async fn execute(
    /// #         &self,
    /// #         _: Transaction<'_, Postgres>,
    /// #         _: Self::Input,
    /// #     ) -> TaskResult<Self::Output> {
    /// #         Ok(())
    /// #     }
    /// # }
    /// use underway::Queue;
    ///
    /// let queue_builder = Queue::<ExampleTask>::builder()
    ///     .name("example")
    ///     .dead_letter_queue("example-dlq");
    /// ```
    pub fn dead_letter_queue(mut self, dlq_name: impl Into<String>) -> Self {
        self.state.dlq_name = Some(dlq_name.into());
        self
    }

    /// Set the database connection pool.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use sqlx::{Postgres, Transaction};
    /// # use underway::{Task, task::Result as TaskResult};
    /// # struct ExampleTask;
    /// # impl Task for ExampleTask {
    /// #     type Input = ();
    /// #     type Output = ();
    /// #     async fn execute(
    /// #         &self,
    /// #         _: Transaction<'_, Postgres>,
    /// #         _: Self::Input,
    /// #     ) -> TaskResult<Self::Output> {
    /// #         Ok(())
    /// #     }
    /// # }
    /// use sqlx::PgPool;
    /// use underway::Queue;
    ///
    /// # use tokio::runtime::Runtime;
    /// # fn main() {
    /// # let rt = Runtime::new().unwrap();
    /// # rt.block_on(async {
    /// let pool = PgPool::connect(&std::env::var("DATABASE_URL")?).await?;
    /// let queue_builder = Queue::<ExampleTask>::builder()
    ///     .name("example")
    ///     .pool(pool.clone());
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # });
    /// # }
    /// ```
    pub fn pool(self, pool: PgPool) -> Builder<T, PoolSet> {
        Builder {
            state: PoolSet {
                name: self.state.name,
                dlq_name: self.state.dlq_name,
                pool,
            },
            _marker: PhantomData,
        }
    }
}

impl<T: Task> Builder<T, PoolSet> {
    /// Builds the queue.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use sqlx::{Postgres, Transaction};
    /// # use underway::{Task, task::Result as TaskResult};
    /// # struct ExampleTask;
    /// # impl Task for ExampleTask {
    /// #     type Input = ();
    /// #     type Output = ();
    /// #     async fn execute(
    /// #         &self,
    /// #         _: Transaction<'_, Postgres>,
    /// #         _: Self::Input,
    /// #     ) -> TaskResult<Self::Output> {
    /// #         Ok(())
    /// #     }
    /// # }
    /// use sqlx::PgPool;
    /// use underway::Queue;
    ///
    /// # use tokio::runtime::Runtime;
    /// # fn main() {
    /// # let rt = Runtime::new().unwrap();
    /// # rt.block_on(async {
    /// let pool = PgPool::connect(&std::env::var("DATABASE_URL")?).await?;
    /// let queue_builder = Queue::<ExampleTask>::builder()
    ///     .name("example")
    ///     .pool(pool.clone())
    ///     .build()
    ///     .await?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # });
    /// # }
    /// ```
    pub async fn build(self) -> Result<Queue<T>> {
        let state = self.state;

        let mut tx = state.pool.begin().await?;

        // Create the queue in the database
        Queue::<T>::create(&mut *tx, &state.name).await?;

        // Create the DLQ in the database if specified
        if let Some(dlq_name) = &state.dlq_name {
            Queue::<T>::create(&mut *tx, dlq_name).await?;
        }

        tx.commit().await?;

        Ok(Queue {
            name: state.name,
            dlq_name: state.dlq_name,
            pool: state.pool,
            _marker: PhantomData,
        })
    }
}

static SHUTDOWN_CHANNEL: OnceLock<String> = OnceLock::new();

pub(crate) fn shutdown_channel() -> &'static str {
    SHUTDOWN_CHANNEL.get_or_init(|| format!("underway_shutdown_{}", Uuid::new_v4()))
}

/// Initiates a graceful shutdown by sending a `NOTIFY` to the
/// `underway_shutdown` channel via the `pg_notify` function.
///
/// Workers listen on this channel and when a message is received will stop
/// processing further tasks and wait for in-progress tasks to finish or
/// timeout.
///
/// This can be useful when combined with [`tokio::signal`] to ensure queues are
/// stopped cleanly when stopping your application.
pub async fn graceful_shutdown<'a, E>(executor: E) -> Result
where
    E: PgExecutor<'a>,
{
    let chan = shutdown_channel();
    sqlx::query!("select pg_notify($1, $2)", chan, "")
        .execute(executor)
        .await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, path::PathBuf};

    use serde_json::json;
    use sqlx::{Postgres, Transaction};

    use super::*;
    use crate::{task::Result as TaskResult, worker::pg_interval_to_span};

    struct TestTask;

    impl Task for TestTask {
        type Input = serde_json::Value;
        type Output = ();

        async fn execute(
            &self,
            _: Transaction<'_, Postgres>,
            _: Self::Input,
        ) -> TaskResult<Self::Output> {
            Ok(())
        }
    }

    #[sqlx::test]
    async fn build_queue(pool: PgPool) -> sqlx::Result<(), Error> {
        let queue: Queue<TestTask> = Queue::builder()
            .name("test_queue")
            .pool(pool)
            .build()
            .await?;

        assert_eq!(queue.name, "test_queue");
        assert!(queue.dlq_name.is_none());

        Ok(())
    }

    #[sqlx::test]
    async fn build_queue_with_dlq(pool: PgPool) -> sqlx::Result<(), Error> {
        let queue: Queue<TestTask> = Queue::builder()
            .name("test_queue_with_dlq")
            .dead_letter_queue("dlq_test")
            .pool(pool)
            .build()
            .await?;

        assert_eq!(queue.name, "test_queue_with_dlq");
        assert_eq!(queue.dlq_name, Some("dlq_test".to_string()));

        Ok(())
    }

    #[sqlx::test]
    async fn enqueue_task(pool: PgPool) -> sqlx::Result<(), Error> {
        let queue = Queue::builder()
            .name("test_enqueue")
            .pool(pool.clone())
            .build()
            .await?;

        let input = serde_json::json!({ "key": "value" });
        let task = TestTask;

        let task_id = queue.enqueue(&pool, &task, &input).await?;

        // Query the database to verify the task was enqueued
        let in_progress_task = sqlx::query!(
            r#"
            select id, input, retry_policy as "retry_policy: RetryPolicy", concurrency_key, priority
            from underway.task
            where id = $1
            "#,
            task_id as _
        )
        .fetch_one(&pool)
        .await?;

        assert_eq!(in_progress_task.id, *task_id);
        assert_eq!(in_progress_task.input, input);

        let expected_retry_policy = task.retry_policy();
        let retry_policy = in_progress_task.retry_policy;
        assert_eq!(
            retry_policy.max_attempts,
            expected_retry_policy.max_attempts
        );
        assert_eq!(
            retry_policy.initial_interval_ms,
            expected_retry_policy.initial_interval_ms
        );
        assert_eq!(
            retry_policy.max_interval_ms,
            expected_retry_policy.max_interval_ms
        );
        assert_eq!(
            retry_policy.backoff_coefficient,
            expected_retry_policy.backoff_coefficient
        );

        assert_eq!(in_progress_task.concurrency_key, task.concurrency_key());
        assert_eq!(in_progress_task.priority, task.priority());

        Ok(())
    }

    #[sqlx::test]
    async fn enqueue_task_with_retry(pool: PgPool) -> sqlx::Result<(), Error> {
        let queue = Queue::builder()
            .name("test_enqueue_with_retry")
            .pool(pool.clone())
            .build()
            .await?;

        struct MyCustomRetryTask;

        impl Task for MyCustomRetryTask {
            type Input = ();
            type Output = ();

            async fn execute(
                &self,
                _tx: Transaction<'_, Postgres>,
                _input: Self::Input,
            ) -> TaskResult<Self::Output> {
                Ok(())
            }

            // Specify our own retry policy for the task.
            fn retry_policy(&self) -> RetryPolicy {
                RetryPolicy::builder()
                    .max_attempts(20)
                    .initial_interval_ms(2_500)
                    .max_interval_ms(300_000)
                    .backoff_coefficient(1.5)
                    .build()
            }
        }
        let task_id = queue.enqueue(&pool, &MyCustomRetryTask, &()).await?;

        let in_progress_task = sqlx::query!(
            r#"
            select retry_policy as "retry_policy: RetryPolicy"
            from underway.task
            where id = $1
            "#,
            task_id as TaskId
        )
        .fetch_one(&pool)
        .await?;

        // Ensure the retry policy was set
        assert_eq!(in_progress_task.retry_policy.max_attempts, 20);
        assert_eq!(in_progress_task.retry_policy.initial_interval_ms, 2_500);
        assert_eq!(in_progress_task.retry_policy.max_interval_ms, 300_000);
        assert_eq!(in_progress_task.retry_policy.backoff_coefficient, 1.5);

        Ok(())
    }

    #[sqlx::test]
    async fn enqueue_task_with_timeout(pool: PgPool) -> sqlx::Result<(), Error> {
        let queue = Queue::builder()
            .name("test_enqueue_with_timeout")
            .pool(pool.clone())
            .build()
            .await?;

        struct MyImpatientTask;

        impl Task for MyImpatientTask {
            type Input = ();
            type Output = ();

            async fn execute(
                &self,
                _tx: Transaction<'_, Postgres>,
                _input: Self::Input,
            ) -> TaskResult<Self::Output> {
                Ok(())
            }

            fn timeout(&self) -> Span {
                1.second()
            }
        }

        let task_id = queue.enqueue(&pool, &MyImpatientTask, &()).await?;

        let in_progress_task = sqlx::query!(
            r#"
            select timeout
            from underway.task
            where id = $1
            "#,
            task_id as TaskId
        )
        .fetch_one(&pool)
        .await?;

        // Ensure the timeout was set
        assert_eq!(
            pg_interval_to_span(&in_progress_task.timeout).compare(1.second())?,
            std::cmp::Ordering::Equal
        );

        Ok(())
    }

    #[sqlx::test]
    async fn enqueue_task_with_ttl(pool: PgPool) -> sqlx::Result<(), Error> {
        let queue = Queue::builder()
            .name("test_enqueue_with_ttl")
            .pool(pool.clone())
            .build()
            .await?;

        struct MyLongLivedTask;

        impl Task for MyLongLivedTask {
            type Input = ();
            type Output = ();

            async fn execute(
                &self,
                _tx: Transaction<'_, Postgres>,
                _input: Self::Input,
            ) -> TaskResult<Self::Output> {
                Ok(())
            }

            fn ttl(&self) -> Span {
                15.days()
            }
        }

        let task_id = queue.enqueue(&pool, &MyLongLivedTask, &()).await?;

        let in_progress_task = sqlx::query!(
            r#"
            select ttl
            from underway.task
            where id = $1
            "#,
            task_id as TaskId
        )
        .fetch_one(&pool)
        .await?;

        // Ensure the TTL was set
        assert_eq!(
            pg_interval_to_span(&in_progress_task.ttl).compare(15.days())?,
            std::cmp::Ordering::Equal
        );

        Ok(())
    }

    #[sqlx::test]
    async fn enqueue_task_with_delay(pool: PgPool) -> sqlx::Result<(), Error> {
        let queue = Queue::builder()
            .name("test_enqueue_with_delay")
            .pool(pool.clone())
            .build()
            .await?;

        struct MyDelayedTask;

        impl Task for MyDelayedTask {
            type Input = ();
            type Output = ();

            async fn execute(
                &self,
                _tx: Transaction<'_, Postgres>,
                _input: Self::Input,
            ) -> TaskResult<Self::Output> {
                Ok(())
            }

            fn delay(&self) -> Span {
                1.hour()
            }
        }
        let task_id = queue.enqueue(&pool, &MyDelayedTask, &()).await?;

        let in_progress_task = sqlx::query!(
            r#"
            select delay
            from underway.task
            where id = $1
            "#,
            task_id as TaskId
        )
        .fetch_one(&pool)
        .await?;

        // Ensure the delay was set
        assert_eq!(
            pg_interval_to_span(&in_progress_task.delay).compare(1.hour())?,
            std::cmp::Ordering::Equal
        );

        Ok(())
    }

    #[sqlx::test]
    async fn enqueue_multi_with_delay(pool: PgPool) -> sqlx::Result<(), Error> {
        let queue = Queue::builder()
            .name("test_enqueue_multi")
            .pool(pool.clone())
            .build()
            .await?;

        struct MyDelayedTask;

        impl Task for MyDelayedTask {
            type Input = ();
            type Output = ();

            async fn execute(
                &self,
                _tx: Transaction<'_, Postgres>,
                _input: Self::Input,
            ) -> TaskResult<Self::Output> {
                Ok(())
            }

            fn delay(&self) -> Span {
                1.hour()
            }
        }
        let tasks_number = queue
            .enqueue_multi(&pool, &MyDelayedTask, &[(), (), ()])
            .await?;

        assert_eq!(3, tasks_number);

        let scheduled_tasks = sqlx::query!(
            r#"
            select delay
            from underway.task
            "#
        )
        .fetch_all(&pool)
        .await?;

        assert_eq!(3, scheduled_tasks.len());

        // Ensure the delay was set
        assert_eq!(
            pg_interval_to_span(&scheduled_tasks[0].delay).compare(1.hour())?,
            std::cmp::Ordering::Equal
        );

        Ok(())
    }

    #[sqlx::test]
    async fn enqueue_task_with_heartbeat(pool: PgPool) -> sqlx::Result<(), Error> {
        let queue = Queue::builder()
            .name("test_enqueue_with_heartbeat")
            .pool(pool.clone())
            .build()
            .await?;

        struct MyLivelyTask;

        impl Task for MyLivelyTask {
            type Input = ();
            type Output = ();

            async fn execute(
                &self,
                _tx: Transaction<'_, Postgres>,
                _input: Self::Input,
            ) -> TaskResult<Self::Output> {
                Ok(())
            }

            fn heartbeat(&self) -> Span {
                1.second()
            }
        }
        let task_id = queue.enqueue(&pool, &MyLivelyTask, &()).await?;

        let in_progress_task = sqlx::query!(
            r#"
            select heartbeat
            from underway.task
            where id = $1
            "#,
            task_id as TaskId
        )
        .fetch_one(&pool)
        .await?;

        // Ensure the heartbeat was set
        assert_eq!(
            pg_interval_to_span(&in_progress_task.heartbeat).compare(1.second())?,
            std::cmp::Ordering::Equal
        );

        Ok(())
    }

    #[sqlx::test]
    async fn enqueue_task_with_concurrency_key(pool: PgPool) -> sqlx::Result<(), Error> {
        let queue = Queue::builder()
            .name("enqueue_task_with_concurrency_key")
            .pool(pool.clone())
            .build()
            .await?;

        struct MyUniqueTask(PathBuf);

        impl Task for MyUniqueTask {
            type Input = ();
            type Output = ();

            async fn execute(
                &self,
                _tx: Transaction<'_, Postgres>,
                _input: Self::Input,
            ) -> TaskResult<Self::Output> {
                Ok(())
            }

            // Use the path buf as our concurrency key.
            fn concurrency_key(&self) -> Option<String> {
                Some(self.0.display().to_string())
            }
        }
        let task_id = queue
            .enqueue(&pool, &MyUniqueTask(PathBuf::from("/foo/bar")), &())
            .await?;

        let in_progress_task = sqlx::query!(
            r#"
            select concurrency_key
            from underway.task
            where id = $1
            "#,
            task_id as TaskId
        )
        .fetch_one(&pool)
        .await?;

        // Ensure the concurrency key was set
        assert_eq!(
            in_progress_task.concurrency_key,
            Some("/foo/bar".to_string())
        );

        Ok(())
    }

    #[sqlx::test]
    async fn enqueue_task_with_priority(pool: PgPool) -> sqlx::Result<(), Error> {
        let queue = Queue::builder()
            .name("enqueue_task_with_priority")
            .pool(pool.clone())
            .build()
            .await?;

        struct MyHighPriorityTask;

        impl Task for MyHighPriorityTask {
            type Input = ();
            type Output = ();

            async fn execute(
                &self,
                _tx: Transaction<'_, Postgres>,
                _input: Self::Input,
            ) -> TaskResult<Self::Output> {
                Ok(())
            }

            fn priority(&self) -> i32 {
                10
            }
        }
        let task_id = queue.enqueue(&pool, &MyHighPriorityTask, &()).await?;

        let in_progress_task = sqlx::query!(
            r#"
            select priority
            from underway.task
            where id = $1
            "#,
            task_id as TaskId
        )
        .fetch_one(&pool)
        .await?;

        // Ensure the priority was set
        assert_eq!(in_progress_task.priority, 10);

        Ok(())
    }

    #[sqlx::test]
    async fn enqueue_after(pool: PgPool) -> sqlx::Result<(), Error> {
        let queue = Queue::builder()
            .name("test_enqueue_after")
            .pool(pool.clone())
            .build()
            .await?;

        let input = serde_json::json!({ "key": "value" });
        let task = TestTask;

        let task_id = queue
            .enqueue_after(&pool, &task, &input, 5.minutes())
            .await?;

        // Check the delay
        let dequeued_task = sqlx::query!(
            r#"
            select delay from underway.task
            where id = $1
            "#,
            task_id as _
        )
        .fetch_one(&pool)
        .await?;

        assert_eq!(
            pg_interval_to_span(&dequeued_task.delay).compare(5.minutes())?,
            std::cmp::Ordering::Equal
        );

        Ok(())
    }

    #[sqlx::test]
    async fn dequeue_task(pool: PgPool) -> sqlx::Result<(), Error> {
        let queue = Queue::builder()
            .name("test_dequeue")
            .pool(pool.clone())
            .build()
            .await?;

        let input = serde_json::json!({ "key": "value" });
        let task = TestTask;

        // Enqueue a task
        let task_id = queue.enqueue(&pool, &task, &input).await?;

        // Dequeue the task
        let in_progress_task = queue
            .dequeue()
            .await?
            .expect("There should be a task to dequeue");

        assert_eq!(in_progress_task.id, task_id);
        assert_eq!(in_progress_task.input, input);
        assert_eq!(in_progress_task.retry_count(&pool).await?, 0);

        let retry_policy = in_progress_task.retry_policy;
        let expected_retry_policy = task.retry_policy();
        assert_eq!(
            retry_policy.max_attempts,
            expected_retry_policy.max_attempts
        );
        assert_eq!(
            retry_policy.initial_interval_ms,
            expected_retry_policy.initial_interval_ms
        );
        assert_eq!(
            retry_policy.max_interval_ms,
            expected_retry_policy.max_interval_ms
        );
        assert_eq!(
            retry_policy.backoff_coefficient,
            expected_retry_policy.backoff_coefficient
        );
        assert_eq!(in_progress_task.concurrency_key, task.concurrency_key());

        // Query the database to verify the task's state was set.
        let dequeued_task = sqlx::query!(
            r#"
            select state as "state: TaskState"
            from underway.task
            where id = $1
            "#,
            task_id as _
        )
        .fetch_one(&pool)
        .await?;

        assert_eq!(dequeued_task.state, TaskState::InProgress);

        Ok(())
    }

    #[sqlx::test]
    async fn concurrent_dequeue(pool: PgPool) -> sqlx::Result<(), Box<dyn std::error::Error>> {
        let queue = Queue::builder()
            .name("test_concurrent_dequeue")
            .pool(pool.clone())
            .build()
            .await?;

        let input = serde_json::json!({ "key": "value" });
        let task = TestTask;

        // Enqueue multiple tasks
        for _ in 0..5 {
            queue.enqueue(&pool, &task, &input).await?;
        }

        // Simulate concurrent dequeues
        let handles: Vec<_> = (0..5)
            .map(|_| {
                let queue = queue.clone();
                tokio::spawn(async move { queue.dequeue().await })
            })
            .collect();

        // Collect results
        let results: Vec<Option<_>> = futures::future::try_join_all(handles)
            .await?
            .into_iter()
            .collect::<Result<Vec<_>>>()?;

        // Ensure all tasks were dequeued without duplicates
        let mut task_ids = HashSet::new();
        for dequeued_task in results.into_iter().flatten() {
            assert!(
                task_ids.insert(dequeued_task.id),
                "Task ID should be unique"
            );
        }

        assert_eq!(task_ids.len(), 5);

        Ok(())
    }

    #[sqlx::test]
    async fn dequeue_from_empty_queue(pool: PgPool) -> sqlx::Result<(), Error> {
        let queue: Queue<TestTask> = Queue::builder()
            .name("test_empty_dequeue")
            .pool(pool.clone())
            .build()
            .await?;

        // Attempt to dequeue without enqueuing any tasks
        let dequeued_task = queue.dequeue().await?;

        assert!(dequeued_task.is_none());

        Ok(())
    }

    #[sqlx::test]
    async fn mark_task_in_progress(pool: PgPool) -> sqlx::Result<(), Error> {
        let queue = Queue::builder()
            .name("test_in_progress")
            .pool(pool.clone())
            .build()
            .await?;

        let input = serde_json::json!({ "key": "value" });
        let task = TestTask;

        // Enqueue a task
        let task_id = queue.enqueue(&pool, &task, &input).await?;

        let mut tx = pool.begin().await?;

        // N.B. Task must be dequeued to ensure an attempt row is created.
        queue.dequeue().await?.expect("A task should be dequeued");

        // Verify the task state
        let task_row = sqlx::query!(
            r#"
            select id, state as "state: TaskState"
            from underway.task
            where id = $1
            "#,
            task_id as TaskId
        )
        .fetch_one(&mut *tx)
        .await?;

        assert_eq!(task_row.state, TaskState::InProgress);

        // Verify task attempt exists.
        let task_attempt_row = sqlx::query!(
            r#"
            select task_id
            from underway.task_attempt
            where task_id = $1
            "#,
            task_row.id
        )
        .fetch_optional(&mut *tx)
        .await?;

        assert!(task_attempt_row.is_some());

        Ok(())
    }

    #[sqlx::test]
    async fn reschedule_task_for_retry(pool: PgPool) -> sqlx::Result<(), Error> {
        let queue = Queue::builder()
            .name("test_reschedule")
            .pool(pool.clone())
            .build()
            .await?;

        let input = serde_json::json!({ "key": "value" });
        let task = TestTask;

        // Enqueue a task
        let task_id = queue.enqueue(&pool, &task, &input).await?;

        // TODO: Converting PgInterval to Span may need to be revisited as minutes and
        // seconds do no properly line up.
        let delay = 1.minute();

        let mut conn = pool.acquire().await?;
        let in_progress_task = queue.dequeue().await?.expect("A task should be dequeued");

        in_progress_task.retry_after(&mut conn, delay).await?;

        // Query to verify rescheduled task
        let dequeued_task = sqlx::query!(
            r#"
            select state as "state: TaskState", delay from underway.task where id = $1
            "#,
            task_id as TaskId
        )
        .fetch_optional(&pool)
        .await?
        .expect("Task should be available");

        assert_eq!(
            in_progress_task.retry_count(&pool).await?,
            1,
            "Task should show one retry since it's been rescheduled"
        );
        assert_eq!(
            dequeued_task.state,
            TaskState::Pending,
            "Task should be set to Pending so it'll be retried"
        );

        assert_eq!(
            pg_interval_to_span(&dequeued_task.delay).compare(delay)?,
            std::cmp::Ordering::Equal
        );

        Ok(())
    }

    #[sqlx::test]
    async fn mark_task_cancelled(pool: PgPool) -> sqlx::Result<(), Error> {
        let queue = Queue::builder()
            .name("test_cancel")
            .pool(pool.clone())
            .build()
            .await?;

        let input = serde_json::json!({ "key": "value" });
        let task = TestTask;

        // Enqueue a task
        let task_id = queue.enqueue(&pool, &task, &input).await?;

        let mut conn = pool.acquire().await?;
        let in_progress_task = queue.dequeue().await?.expect("A task should be dequeued");

        // Cancel the task
        in_progress_task.mark_cancelled(&mut conn).await?;

        // Verify the task state
        let task_row = sqlx::query!(
            r#"
            select state as "state: TaskState" from underway.task where id = $1
            "#,
            task_id as _
        )
        .fetch_one(&pool)
        .await?;

        assert_eq!(task_row.state, TaskState::Cancelled);

        Ok(())
    }

    #[sqlx::test]
    async fn mark_task_succeeded(pool: PgPool) -> sqlx::Result<(), Error> {
        let queue = Queue::builder()
            .name("test_success")
            .pool(pool.clone())
            .build()
            .await?;

        let input = serde_json::json!({ "key": "value" });
        let task = TestTask;

        // Enqueue a task
        let task_id = queue.enqueue(&pool, &task, &input).await?;

        let mut tx = pool.begin().await?;

        // N.B. Task must be dequeued to ensure attempt row is created.
        let in_progress_task = queue.dequeue().await?.expect("A task should be dequeued");

        // Mark the task as succeeded
        in_progress_task.mark_succeeded(&mut tx).await?;

        // Verify the task state
        let task_row = sqlx::query!(
            r#"
            select state as "state: TaskState" from underway.task where id = $1
            "#,
            task_id as _
        )
        .fetch_one(&mut *tx)
        .await?;

        assert_eq!(task_row.state, TaskState::Succeeded);

        Ok(())
    }

    #[sqlx::test]
    async fn mark_task_failed(pool: PgPool) -> sqlx::Result<(), Error> {
        let queue = Queue::builder()
            .name("test_fail")
            .pool(pool.clone())
            .build()
            .await?;

        let input = serde_json::json!({ "key": "value" });
        let task = TestTask;

        // Enqueue a task
        let task_id = queue.enqueue(&pool, &task, &input).await?;

        let mut tx = pool.begin().await?;

        // N.B. We can't mark a task failed if it hasn't been dequeued.
        let in_progress_task = queue.dequeue().await?.expect("A task should be dequeued");

        // Mark the task as failed
        in_progress_task.mark_failed(&mut tx).await?;

        // Verify the task state
        let task_row = sqlx::query!(
            r#"
            select id, state as "state: TaskState" from underway.task where id = $1
            "#,
            task_id as TaskId
        )
        .fetch_optional(&mut *tx)
        .await?;

        assert!(task_row.is_some());
        assert_eq!(task_row.unwrap().state, TaskState::Failed);

        Ok(())
    }

    #[sqlx::test]
    async fn update_task_failure(pool: PgPool) -> sqlx::Result<(), Error> {
        let queue = Queue::builder()
            .name("test_update_failure")
            .pool(pool.clone())
            .build()
            .await?;

        let input = serde_json::json!({ "key": "value" });
        let task = TestTask;

        // Enqueue a task
        let task_id = queue.enqueue(&pool, &task, &input).await?;

        // Update task failure details
        let error = &TaskError::Retryable("Some failure occurred".to_string());

        let mut tx = pool.begin().await?;

        // N.B. Task must be dequeued to ensure attempt row is created.
        let in_progress_task = queue.dequeue().await?.expect("A task should be dequeued");

        // Update task failure.
        in_progress_task.record_failure(&mut tx, error).await?;

        // Query to verify the failure update
        let task_attempt_row = sqlx::query!(
            r#"
            select error_message from underway.task_attempt where task_id = $1 order by attempt_number desc
            "#,
            task_id as TaskId
        )
        .fetch_optional(&mut *tx)
        .await?.expect("Task attempt row should exist");

        assert_eq!(in_progress_task.retry_count(&mut *tx).await?, 1);
        assert_eq!(task_attempt_row.error_message, Some(error.to_string()));

        Ok(())
    }

    #[sqlx::test]
    async fn move_task_to_dlq(pool: PgPool) -> sqlx::Result<(), Error> {
        let queue = Queue::builder()
            .name("test_move_to_dlq")
            .dead_letter_queue("test_dlq")
            .pool(pool.clone())
            .build()
            .await?;

        let input = serde_json::json!({ "key": "value" });
        let task = TestTask;

        // Enqueue a task
        let task_id = queue.enqueue(&pool, &task, &input).await?;

        // Move the task to DLQ
        queue.move_task_to_dlq(&pool, task_id, "test_dlq").await?;

        // Query to verify the task is in the DLQ
        let task_row = sqlx::query!(
            r#"
            select id, task_queue_name from underway.task where id = $1
            "#,
            task_id as _
        )
        .fetch_optional(&pool)
        .await?;

        assert!(task_row.is_some());
        assert_eq!(task_row.unwrap().task_queue_name, "test_dlq");

        Ok(())
    }

    #[sqlx::test]
    async fn schedule(pool: PgPool) -> sqlx::Result<(), Error> {
        let queue = Queue::<TestTask>::builder()
            .name("schedule")
            .pool(pool.clone())
            .build()
            .await?;

        // Set the schedule
        let input = serde_json::json!(());
        let daily = "@daily[America/Los_Angeles]"
            .parse()
            .expect("Zoned schedule should parse");
        queue.schedule(&pool, &daily, &input).await?;

        // Check the schedule was actually set
        let schedule_row = sqlx::query!(
            r#"
            select schedule, timezone, input from underway.task_schedule where task_queue_name = $1
            "#,
            "schedule"
        )
        .fetch_optional(&pool)
        .await?;

        let schedule = schedule_row.expect("Schedule should be set");
        assert_eq!(schedule.schedule, "@daily");
        assert_eq!(schedule.timezone, "America/Los_Angeles");
        assert_eq!(schedule.input, input);

        Ok(())
    }

    #[sqlx::test]
    async fn schedule_twice(pool: PgPool) -> sqlx::Result<(), Error> {
        let queue = Queue::<TestTask>::builder()
            .name("schedule_twice")
            .pool(pool.clone())
            .build()
            .await?;

        // Set the first schedule
        let input = serde_json::json!(());
        let daily = "@daily[America/Los_Angeles]"
            .parse()
            .expect("Zoned schedule should parse");
        queue.schedule(&pool, &daily, &input).await?;

        // Set the second schedule, overwriting the first
        let input = serde_json::json!(());
        let monthly = "@monthly[America/Los_Angeles]"
            .parse()
            .expect("Zoned schedule should parse");
        queue.schedule(&pool, &monthly, &input).await?;

        // Check the schedule was actually set
        let schedule_row = sqlx::query!(
            r#"
            select schedule, timezone, input
            from underway.task_schedule
            where task_queue_name = $1
            "#,
            queue.name
        )
        .fetch_optional(&pool)
        .await?;

        let schedule = schedule_row.expect("Schedule should be set");
        assert_eq!(schedule.schedule, "@monthly");
        assert_eq!(schedule.timezone, "America/Los_Angeles");
        assert_eq!(schedule.input, input);

        Ok(())
    }

    #[sqlx::test]
    async fn task_schedule(pool: PgPool) -> sqlx::Result<(), Error> {
        let queue = Queue::<TestTask>::builder()
            .name("task_schedule")
            .pool(pool.clone())
            .build()
            .await?;

        // Set the schedule
        let input = serde_json::json!(());
        let daily = "@daily[America/Los_Angeles]"
            .parse()
            .expect("Zoned schedule should parse");
        queue.schedule(&pool, &daily, &input).await?;

        // Check the task schedule for the queue
        let (zoned_schedule, schedule_input) = queue
            .task_schedule(&pool)
            .await?
            .expect("Schedule should be set");

        assert_eq!(zoned_schedule, daily);
        assert_eq!(schedule_input, input);

        Ok(())
    }

    #[sqlx::test]
    async fn task_schedule_without_schedule(pool: PgPool) -> sqlx::Result<(), Error> {
        let queue = Queue::<TestTask>::builder()
            .name("task_schedule")
            .pool(pool.clone())
            .build()
            .await?;

        assert!(queue.task_schedule(&pool).await?.is_none());

        Ok(())
    }

    #[sqlx::test]
    async fn unschedule(pool: PgPool) -> sqlx::Result<(), Error> {
        let queue = Queue::<TestTask>::builder()
            .name("unschedule")
            .pool(pool.clone())
            .build()
            .await?;

        // Set the schedule
        let input = serde_json::json!(());
        let daily = "@daily[America/Los_Angeles]"
            .parse()
            .expect("Zoned schedule should parse");
        queue.schedule(&pool, &daily, &input).await?;
        queue.unschedule(&pool).await?;

        // Check the schedule was actually set
        let schedule_row = sqlx::query!(
            r#"
            select schedule
            from underway.task_schedule
            where task_queue_name = $1
            "#,
            "schedule"
        )
        .fetch_optional(&pool)
        .await?;

        assert!(schedule_row.is_none());

        Ok(())
    }

    #[sqlx::test]
    async fn unschedule_without_schedule(pool: PgPool) -> sqlx::Result<(), Error> {
        let queue = Queue::<TestTask>::builder()
            .name("unschedule_without_schedule")
            .pool(pool.clone())
            .build()
            .await?;

        assert!(queue.unschedule(&pool).await.is_ok());

        Ok(())
    }

    #[sqlx::test]
    async fn verify_attempt_rows_for_success(pool: PgPool) -> sqlx::Result<(), Error> {
        let queue = Queue::builder()
            .name("verify_attempt_rows_for_success")
            .pool(pool.clone())
            .build()
            .await?;

        let task_id = queue.enqueue(&pool, &TestTask, &json!("{}")).await?;

        // Dequeue the task to ensure the attempt row is created.
        let mut conn = pool.acquire().await?;
        let in_progress_task = queue.dequeue().await?.expect("A task should be dequeued");

        let attempt_rows = sqlx::query!(
            r#"
            select task_id, state as "state: TaskState", completed_at as "completed_at: i64"
            from underway.task_attempt
            "#
        )
        .fetch_all(&pool)
        .await?;

        assert_eq!(attempt_rows.len(), 1);
        for attempt_row in attempt_rows {
            assert_eq!(attempt_row.task_id, *task_id);
            assert_eq!(attempt_row.state, TaskState::InProgress);
            assert!(attempt_row.completed_at.is_none());
        }

        // Simulate the task succeeding.
        in_progress_task.mark_succeeded(&mut conn).await?;

        let task_row = sqlx::query!(
            r#"select state as "state: TaskState", completed_at as "completed_at: i64"
            from underway.task where id = $1"#,
            task_id as TaskId
        )
        .fetch_one(&pool)
        .await?;

        assert_eq!(task_row.state, TaskState::Succeeded);
        assert!(task_row.completed_at.is_some());

        let attempt_rows = sqlx::query!(
            r#"
            select task_id, state as "state: TaskState", completed_at as "completed_at: i64"
            from underway.task_attempt
            "#
        )
        .fetch_all(&pool)
        .await?;

        assert_eq!(attempt_rows.len(), 1);
        for attempt_row in attempt_rows {
            assert_eq!(attempt_row.task_id, *task_id);
            assert_eq!(attempt_row.state, TaskState::Succeeded);
            assert!(attempt_row.completed_at.is_some());
        }

        assert!(
            queue.dequeue().await?.is_none(),
            "The task succeeded so nothing else should be queued"
        );

        Ok(())
    }

    #[sqlx::test]
    async fn verify_attempt_rows_for_failure(pool: PgPool) -> sqlx::Result<(), Error> {
        let queue = Queue::builder()
            .name("verify_attempt_rows_for_failure")
            .pool(pool.clone())
            .build()
            .await?;

        let task_id = queue.enqueue(&pool, &TestTask, &json!("{}")).await?;

        // Dequeue the task to ensure the attempt row is created.
        let mut conn = pool.acquire().await?;
        let in_progress_task = queue.dequeue().await?.expect("A task should be dequeued");

        let attempt_rows = sqlx::query!(
            r#"
            select task_id, state as "state: TaskState", completed_at as "completed_at: i64"
            from underway.task_attempt
            "#
        )
        .fetch_all(&pool)
        .await?;

        assert_eq!(attempt_rows.len(), 1);
        for attempt_row in attempt_rows {
            assert_eq!(attempt_row.task_id, *task_id);
            assert_eq!(attempt_row.state, TaskState::InProgress);
            assert!(attempt_row.completed_at.is_none());
        }

        // Simulate the task succeeding.
        in_progress_task.mark_failed(&mut conn).await?;

        let task_row = sqlx::query!(
            r#"
            select state as "state: TaskState", completed_at as "completed_at: i64"
            from underway.task where id = $1
            "#,
            task_id as TaskId
        )
        .fetch_one(&pool)
        .await?;

        assert_eq!(task_row.state, TaskState::Failed);
        assert!(task_row.completed_at.is_some());

        let attempt_rows = sqlx::query!(
            r#"
            select task_id, state as "state: TaskState", completed_at as "completed_at: i64"
            from underway.task_attempt
            "#
        )
        .fetch_all(&pool)
        .await?;

        assert_eq!(attempt_rows.len(), 1);
        for attempt_row in attempt_rows {
            assert_eq!(attempt_row.task_id, *task_id);
            assert_eq!(attempt_row.state, TaskState::Failed);
            assert!(attempt_row.completed_at.is_some());
        }

        assert!(
            queue.dequeue().await?.is_none(),
            "The task failed so nothing else should be queued"
        );

        Ok(())
    }

    #[sqlx::test]
    async fn verify_attempt_rows_for_cancelled(pool: PgPool) -> sqlx::Result<(), Error> {
        let queue = Queue::builder()
            .name("verify_attempt_rows_for_cancelled")
            .pool(pool.clone())
            .build()
            .await?;

        let task_id = queue.enqueue(&pool, &TestTask, &json!("{}")).await?;

        // Dequeue the task to ensure the attempt row is created.
        let mut conn = pool.acquire().await?;
        let in_progress_task = queue.dequeue().await?.expect("A task should be dequeued");

        let attempt_rows = sqlx::query!(
            r#"
            select task_id, state as "state: TaskState", completed_at as "completed_at: i64"
            from underway.task_attempt
            "#
        )
        .fetch_all(&pool)
        .await?;

        assert_eq!(attempt_rows.len(), 1);
        for attempt_row in attempt_rows {
            assert_eq!(attempt_row.task_id, *task_id);
            assert_eq!(attempt_row.state, TaskState::InProgress);
            assert!(attempt_row.completed_at.is_none());
        }

        // Simulate the task succeeding.
        in_progress_task.mark_cancelled(&mut conn).await?;

        let task_row = sqlx::query!(
            r#"
            select state as "state: TaskState", completed_at as "completed_at: i64"
            from underway.task where id = $1
            "#,
            task_id as TaskId
        )
        .fetch_one(&pool)
        .await?;

        assert_eq!(task_row.state, TaskState::Cancelled);
        assert!(task_row.completed_at.is_some());

        let attempt_rows = sqlx::query!(
            r#"
            select task_id, state as "state: TaskState", completed_at as "completed_at: i64"
            from underway.task_attempt
            "#
        )
        .fetch_all(&pool)
        .await?;

        assert_eq!(attempt_rows.len(), 1);
        for attempt_row in attempt_rows {
            assert_eq!(attempt_row.task_id, *task_id);
            assert_eq!(attempt_row.state, TaskState::Cancelled);
            assert!(attempt_row.completed_at.is_some());
        }

        assert!(
            queue.dequeue().await?.is_none(),
            "The task is cancelled so nothing else should be queued"
        );

        Ok(())
    }

    #[sqlx::test]
    async fn verify_attempt_rows_for_reschedule(pool: PgPool) -> sqlx::Result<(), Error> {
        let queue = Queue::builder()
            .name("verify_attempt_rows_for_reschedule")
            .pool(pool.clone())
            .build()
            .await?;

        let task_id = queue.enqueue(&pool, &TestTask, &json!("{}")).await?;

        let mut conn = pool.acquire().await?;
        let mut in_progress_task = queue.dequeue().await?.expect("A task should be dequeued");

        async fn fetch_and_verify_attempts(
            pool: &PgPool,
            task_id: TaskId,
            expected_attempts: &[(TaskState, bool)],
        ) -> sqlx::Result<()> {
            let attempt_rows = sqlx::query!(
                r#"
                select
                    task_id,
                    state as "state: TaskState",
                    completed_at as "completed_at: i64"
                from underway.task_attempt
                where task_id = $1
                order by started_at
                "#,
                task_id as TaskId
            )
            .fetch_all(pool)
            .await?;

            assert_eq!(attempt_rows.len(), expected_attempts.len());

            let mut prev_completed_at: Option<i64> = None;

            for (i, attempt_row) in attempt_rows.into_iter().enumerate() {
                assert_eq!(attempt_row.task_id, *task_id);

                let (expected_state, expect_completed) = &expected_attempts[i];

                if *expect_completed {
                    assert!(attempt_row.completed_at.is_some());
                    if let Some(prev) = prev_completed_at {
                        assert!(
                            prev <= attempt_row.completed_at.unwrap(),
                            "completed_at timestamps are not in chronological order"
                        );
                    }
                    prev_completed_at = attempt_row.completed_at;
                } else {
                    assert!(attempt_row.completed_at.is_none());
                }

                assert_eq!(attempt_row.state, *expected_state);
            }

            Ok(())
        }

        // First verification
        fetch_and_verify_attempts(&pool, task_id, &[(TaskState::InProgress, false)]).await?;

        // Simulate the task being rescheduled.
        in_progress_task.retry_after(&mut conn, Span::new()).await?;

        in_progress_task = queue
            .dequeue()
            .await?
            .expect("Task should be dequeued again");

        // Second verification
        fetch_and_verify_attempts(
            &pool,
            task_id,
            &[(TaskState::Failed, true), (TaskState::InProgress, false)],
        )
        .await?;

        // Simulate the task being rescheduled again.
        in_progress_task.retry_after(&mut conn, Span::new()).await?;

        assert!(queue.dequeue().await?.is_some());

        // Third verification
        fetch_and_verify_attempts(
            &pool,
            task_id,
            &[
                (TaskState::Failed, true),
                (TaskState::Failed, true),
                (TaskState::InProgress, false),
            ],
        )
        .await?;

        Ok(())
    }

    #[sqlx::test]
    async fn dequeue_stale_task(pool: PgPool) -> sqlx::Result<(), Error> {
        let queue = Queue::builder()
            .name("dequeue_stale_task")
            .pool(pool.clone())
            .build()
            .await?;

        let task_id = queue.enqueue(&pool, &TestTask, &json!("{}")).await?;

        assert!(
            queue.dequeue().await?.is_some(),
            "A task should be dequeued"
        );

        assert!(
            queue.dequeue().await?.is_none(),
            "No tasks should be dequeued since task is in-progress and not stale"
        );

        // Check attempt row is in-progress.
        let attempt_rows = sqlx::query!(
            r#"
            select state as "state: TaskState"
            from underway.task_attempt
            where task_id = $1 and state = $2
            "#,
            task_id as TaskId,
            TaskState::InProgress as TaskState
        )
        .fetch_all(&pool)
        .await?;

        assert_eq!(attempt_rows.len(), 1);

        // Set a stale heartbeat.
        sqlx::query!(
            r#"
            update underway.task
            set last_heartbeat_at = now() - interval '30 seconds'
            where id = $1
            "#,
            task_id as TaskId
        )
        .execute(&pool)
        .await?;

        assert!(
            queue.dequeue().await?.is_some(),
            "A stale task should be dequeued"
        );

        // Check attempt row is failed.
        let attempt_rows = sqlx::query!(
            r#"
            select state as "state: TaskState"
            from underway.task_attempt
            where task_id = $1 and state = $2
            "#,
            task_id as TaskId,
            TaskState::Failed as TaskState
        )
        .fetch_all(&pool)
        .await?;

        assert_eq!(attempt_rows.len(), 1);

        Ok(())
    }
}
