//! Queues provide an interface for managing task lifecycle.
//!
//! Tasks are enqueued onto the queue, using the [`Queue::enqueue`] method, and
//! later dequeued, using the [`Queue::dequeue`] method, when they're executed.
//!
//! The semantics for retrieving a task from the queue are defined by the order
//! of insertion, first-in, first-out (FIFO), or the priority the task defines.
//! If a priority is defined, then it's considered before the order the task was
//! inserted.
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
//! let mut tx = pool.begin().await?;
//! if let Some(task) = queue.dequeue(&mut *tx).await? {
//!     // Process the task here
//! }
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! # });
//! # }
//! ```
//!
//! # Scheduling tasks
//!
//! It's important to note that a schedule **must** be set on the queue before
//! the scheduler can be run.
//!
//! As with task processing, jobs provide an interface for scheduling. For
//! example, the [`schedule`](crate::Job::schedule) method along with
//! [`run_scheduler`](crate::Job::run_scheduler) are often what you want to use.
//! Use these to set the schedule and run the scheduler, respectively.
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
//! let scheduler = Scheduler::new(queue, task);
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

use std::{marker::PhantomData, time::Duration as StdDuration};

use builder_states::{Initial, NameSet, PoolSet};
use jiff::{Span, ToSpan};
use sqlx::{
    postgres::{PgAdvisoryLock, PgAdvisoryLockGuard},
    types::Json,
    PgConnection, PgExecutor, PgPool,
};
use tracing::instrument;

use crate::{
    task::{DequeuedTask, Error as TaskError, RetryPolicy, State as TaskState, Task, TaskId},
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
    /// rolled back, the returned tasl ID will not correspond to any persisted
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
        self.enqueue_with_delay(executor, task, input, task.delay())
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
        let calculated_delay = task.delay().checked_add(delay)?;
        self.enqueue_with_delay(executor, task, input, calculated_delay)
            .await
    }

    // Explicitly provide for a delay so that we can also facilitate calculated
    // retries, i.e. `enqueue_after`.
    #[instrument(
        name = "enqueue",
        skip(self, executor, task, input),
        fields(queue.name = self.name, task.id = tracing::field::Empty),
        err
    )]
    async fn enqueue_with_delay<'a, E>(
        &self,
        executor: E,
        task: &T,
        input: &T::Input,
        delay: Span,
    ) -> Result<TaskId>
    where
        E: PgExecutor<'a>,
    {
        let id = TaskId::new();

        let input_value = serde_json::to_value(input)?;

        let retry_policy = task.retry_policy();
        let timeout = task.timeout();
        let ttl = task.ttl();
        let concurrency_key = task.concurrency_key();
        let priority = task.priority();

        tracing::Span::current().record("task.id", id.as_hyphenated().to_string());

        sqlx::query!(
            r#"
            insert into underway.task (
              id,
              task_queue_name,
              input,
              timeout,
              ttl,
              delay,
              retry_policy,
              concurrency_key,
              priority
            ) values ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            "#,
            id as _,
            self.name,
            input_value,
            StdDuration::try_from(timeout)? as _,
            StdDuration::try_from(ttl)? as _,
            StdDuration::try_from(delay)? as _,
            serde_json::to_value(retry_policy)?,
            concurrency_key,
            priority
        )
        .execute(executor)
        .await?;

        Ok(id)
    }

    /// Dequeues the next available task.
    ///
    /// This method uses the `FOR UPDATE SKIP LOCKED` clause to ensure efficient
    /// retrievial of pending task rows.
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
    ///     .dequeue(&mut *tx)
    ///     .await?
    ///     .expect("There should be a pending task.");
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # });
    /// # }
    /// ```
    #[instrument(
        skip(self, conn),
        fields(queue.name = self.name, task.id = tracing::field::Empty),
        err
    )]
    pub async fn dequeue(&self, conn: &mut PgConnection) -> Result<Option<DequeuedTask>> {
        let task_row = sqlx::query_as!(
            DequeuedTask,
            r#"
            select
              id as "id: TaskId",
              input,
              timeout,
              retry_policy as "retry_policy: Json<RetryPolicy>",
              concurrency_key
            from underway.task
            where task_queue_name = $1
              and state = $2
              and created_at + delay <= now()
            order by priority desc, created_at, id
            limit 1
            for update skip locked
            "#,
            self.name,
            TaskState::Pending as TaskState,
        )
        .fetch_optional(&mut *conn)
        .await?;

        if let Some(task_row) = &task_row {
            let task_id = task_row.id;
            tracing::Span::current().record("task.id", task_id.as_hyphenated().to_string());

            self.mark_task_in_progress(&mut *conn, task_row.id).await?;
        }

        Ok(task_row)
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
              name,
              schedule,
              timezone,
              input
            ) values ($1, $2, $3, $4)
            on conflict (name) do update
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
            where name = $1
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
            select schedule, timezone, input from underway.task_schedule where name = $1
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

    async fn mark_task_in_progress(&self, conn: &mut PgConnection, task_id: TaskId) -> Result {
        // Update task attempt row.
        let result = sqlx::query!(
            r#"
            with next_attempt as (
                select coalesce(max(attempt_number) + 1, 1) as attempt_number
                from underway.task_attempt
                where task_id = $1 and
                      task_queue_name = $2
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
        .execute(&mut *conn)
        .await?;

        if result.rows_affected() == 0 {
            return Err(Error::TaskNotFound(task_id));
        }

        // Update task row.
        let result = sqlx::query!(
            r#"
            update underway.task
            set state = $2,
                updated_at = now(),
                last_attempt_at = now()
            where id = $1
            "#,
            task_id as TaskId,
            TaskState::InProgress as TaskState
        )
        .execute(&mut *conn)
        .await?;

        if result.rows_affected() == 0 {
            return Err(Error::TaskNotFound(task_id));
        }

        Ok(())
    }

    pub(crate) async fn mark_task_cancelled(
        &self,
        conn: &mut PgConnection,
        task_id: TaskId,
    ) -> Result<bool> {
        // Update task attempt row if one exists.
        //
        // N.B.: A task may be cancelled before an attempt row has been created.
        sqlx::query!(
            r#"
            update underway.task_attempt
            set state = $3,
                updated_at = now()
            where task_id = (
                select task_id
                from underway.task_attempt
                where task_id = $1 and
                      task_queue_name = $2 and
                      state < $4
                order by attempt_number desc
                limit 1
            )
            "#,
            task_id as TaskId,
            self.name,
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
                updated_at = now()
            where id = $1 and state < $3
            "#,
            task_id as TaskId,
            TaskState::Cancelled as TaskState,
            TaskState::Succeeded as TaskState
        )
        .execute(&mut *conn)
        .await?;

        if result.rows_affected() == 0 {
            return Err(Error::TaskNotFound(task_id));
        }

        Ok(result.rows_affected() > 0)
    }

    pub(crate) async fn mark_task_succeeded(
        &self,
        conn: &mut PgConnection,
        task_id: TaskId,
    ) -> Result {
        // Update the task attempt row.
        let result = sqlx::query!(
            r#"
            update underway.task_attempt
            set state = $3,
                updated_at = now()
            where task_id = (
                select task_id
                from underway.task_attempt
                where task_id = $1 and
                      task_queue_name = $2
                order by attempt_number desc
                limit 1
            ) 
            "#,
            task_id as TaskId,
            self.name,
            TaskState::Succeeded as TaskState
        )
        .execute(&mut *conn)
        .await?;

        if result.rows_affected() == 0 {
            return Err(Error::TaskNotFound(task_id));
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
            task_id as TaskId,
            TaskState::Succeeded as TaskState
        )
        .execute(&mut *conn)
        .await?;

        if result.rows_affected() == 0 {
            return Err(Error::TaskNotFound(task_id));
        }

        Ok(())
    }

    #[instrument(
        skip(self, executor, task_id),
        fields(queue.name = self.name, task.id = %task_id.as_hyphenated()),
        err
    )]
    pub(crate) async fn reschedule_task_for_retry<'a, E>(
        &self,
        executor: E,
        task_id: TaskId,
        delay: Span,
    ) -> Result
    where
        E: PgExecutor<'a>,
    {
        let result = sqlx::query!(
            r#"
            update underway.task
            set state = $3,
                delay = $2,
                updated_at = now()
            where id = $1
            "#,
            task_id as TaskId,
            StdDuration::try_from(delay)? as _,
            TaskState::Pending as TaskState
        )
        .execute(executor)
        .await?;

        if result.rows_affected() == 0 {
            return Err(Error::TaskNotFound(task_id));
        }

        Ok(())
    }

    pub(crate) async fn mark_task_failed(
        &self,
        conn: &mut PgConnection,
        task_id: TaskId,
    ) -> Result {
        // Update the task attempt row.
        let result = sqlx::query!(
            r#"
            update underway.task_attempt
            set state = $3,
                updated_at = now()
            where task_id = $1 and 
                  task_queue_name = $2
            "#,
            task_id as TaskId,
            self.name,
            TaskState::Failed as TaskState
        )
        .execute(&mut *conn)
        .await?;

        if result.rows_affected() == 0 {
            return Err(Error::TaskNotFound(task_id));
        }

        // Update the task row.
        let result = sqlx::query!(
            r#"
            update underway.task
            set state = $2,
                updated_at = now()
            where id = $1
            "#,
            task_id as TaskId,
            TaskState::Failed as TaskState
        )
        .execute(&mut *conn)
        .await?;

        if result.rows_affected() == 0 {
            return Err(Error::TaskNotFound(task_id));
        }

        Ok(())
    }

    pub(crate) async fn update_task_failure(
        &self,
        conn: &mut PgConnection,
        task_id: TaskId,
        error: &TaskError,
    ) -> Result {
        let result = sqlx::query!(
            r#"
            update underway.task_attempt
            set state = $3,
                updated_at = now(),
                error_message = $4
            where task_id = (
                select task_id
                from underway.task_attempt
                where task_id = $1 and
                      task_queue_name = $2
                order by attempt_number desc
                limit 1
            ) 
            "#,
            task_id as TaskId,
            self.name,
            TaskState::Failed as TaskState,
            error.to_string(),
        )
        .execute(&mut *conn)
        .await?;

        if result.rows_affected() == 0 {
            return Err(Error::TaskNotFound(task_id));
        }

        let result = sqlx::query!(
            r#"
            update underway.task
            set updated_at = now()
            where id = $1 and
                  task_queue_name = $2 
            "#,
            task_id as TaskId,
            self.name,
        )
        .execute(&mut *conn)
        .await?;

        if result.rows_affected() == 0 {
            return Err(Error::TaskNotFound(task_id));
        }

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
            task_id as _,
            dlq_name
        )
        .execute(executor)
        .await?;

        if result.rows_affected() == 0 {
            return Err(Error::TaskNotFound(task_id));
        }

        Ok(())
    }

    pub(crate) async fn retry_count<'a, E>(&self, executor: E, task_id: &TaskId) -> Result<i32>
    where
        E: PgExecutor<'a>,
    {
        let count = sqlx::query_scalar!(
            r#"
            select count(*)::int as "count!"
            from underway.task_attempt
            where task_id = $1 and
                  task_queue_name = $2 and
                  state = $3
            "#,
            task_id as &TaskId,
            self.name,
            TaskState::Failed as TaskState
        )
        .fetch_one(executor)
        .await?;

        Ok(count)
    }
}

#[instrument(skip(executor), err)]
pub(crate) async fn acquire_advisory_xact_lock<'a, E>(executor: E, key: &str) -> Result
where
    E: PgExecutor<'a>,
{
    sqlx::query!("select pg_advisory_xact_lock(hashtext($1))", key)
        .execute(executor)
        .await?;

    Ok(())
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

pub(crate) const SHUTDOWN_CHANNEL: &str = "underway_shutdown";

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
    sqlx::query!("select pg_notify($1, $2)", SHUTDOWN_CHANNEL, "")
        .execute(executor)
        .await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

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
        let dequeued_task = sqlx::query!(
            r#"
            select id, input, retry_policy as "retry_policy: Json<RetryPolicy>", concurrency_key, priority
            from underway.task
            where id = $1
            "#,
            task_id as _
        )
        .fetch_one(&pool)
        .await?;

        assert_eq!(dequeued_task.id, *task_id);
        assert_eq!(dequeued_task.input, input);
        assert_eq!(queue.retry_count(&pool, &task_id).await?, 0);

        let expected_retry_policy = task.retry_policy();
        let retry_policy = dequeued_task.retry_policy;
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

        assert_eq!(dequeued_task.concurrency_key, task.concurrency_key());
        assert_eq!(dequeued_task.priority, task.priority());

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
        let mut conn = pool.acquire().await?;
        let dequeued_task = queue.dequeue(&mut conn).await?;

        assert!(dequeued_task.is_some(), "We should have a task enqueued");

        let dequeued_task = dequeued_task.unwrap();
        assert_eq!(dequeued_task.id, task_id);
        assert_eq!(dequeued_task.input, input);
        assert_eq!(queue.retry_count(&pool, &task_id).await?, 0);

        let retry_policy = dequeued_task.retry_policy;
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
        assert_eq!(dequeued_task.concurrency_key, task.concurrency_key());

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
                let pool = pool.clone();
                tokio::spawn(async move {
                    let mut tx = pool.begin().await?;
                    let ret = queue.dequeue(&mut tx).await;
                    tx.commit().await?;
                    ret
                })
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
        let mut conn = pool.acquire().await?;
        let dequeued_task = queue.dequeue(&mut conn).await?;

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
        queue.dequeue(&mut tx).await?;

        // Mark the task as in progress
        queue.mark_task_in_progress(&mut tx, task_id).await?;

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

        queue
            .reschedule_task_for_retry(&pool, task_id, delay)
            .await?;

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
            queue.retry_count(&pool, &task_id).await?,
            0,
            "Task should still show zero retries since it hasn't been retried yet"
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

        // Cancel the task
        let mut conn = pool.acquire().await?;
        queue.mark_task_cancelled(&mut conn, task_id).await?;

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
        queue.dequeue(&mut tx).await?;

        // Mark the task as succeeded
        queue.mark_task_succeeded(&mut tx, task_id).await?;

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
    async fn mark_nonexistent_task_succeeded(pool: PgPool) -> sqlx::Result<(), Error> {
        let queue: Queue<TestTask> = Queue::builder()
            .name("test_nonexistent_task")
            .pool(pool.clone())
            .build()
            .await?;

        let nonexistent_task_id = TaskId::new();

        // Attempt to mark a non-existent task as succeeded
        let mut conn = pool.acquire().await?;
        let result = queue
            .mark_task_succeeded(&mut conn, nonexistent_task_id)
            .await;

        assert!(result.is_err(), "Expected an error, but got Ok");

        if let Err(Error::TaskNotFound(id)) = result {
            assert_eq!(id, nonexistent_task_id, "Task IDs should match");
        } else {
            panic!(
                "Expected TaskNotFound error, but got {:?}",
                result.unwrap_err()
            );
        }

        Ok(())
    }

    #[sqlx::test]
    async fn mark_nonexistent_task_failed(pool: PgPool) -> sqlx::Result<(), Error> {
        let queue: Queue<TestTask> = Queue::builder()
            .name("test_nonexistent_task")
            .pool(pool.clone())
            .build()
            .await?;

        let nonexistent_task_id = TaskId::new();

        // Attempt to mark a non-existent task as succeeded
        let mut conn = pool.acquire().await?;
        let result = queue.mark_task_failed(&mut conn, nonexistent_task_id).await;

        assert!(result.is_err(), "Expected an error, but got Ok");

        if let Err(Error::TaskNotFound(id)) = result {
            assert_eq!(id, nonexistent_task_id, "Task IDs should match");
        } else {
            panic!(
                "Expected TaskNotFound error, but got {:?}",
                result.unwrap_err()
            );
        }

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
        queue.dequeue(&mut tx).await?;

        // Mark the task as failed
        queue.mark_task_failed(&mut tx, task_id).await?;

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
        queue.dequeue(&mut tx).await?;

        // Update task failure.
        queue.update_task_failure(&mut tx, task_id, error).await?;

        // Query to verify the failure update
        let task_attempt_row = sqlx::query!(
            r#"
            select error_message from underway.task_attempt where task_id = $1 order by attempt_number desc
            "#,
            task_id as TaskId
        )
        .fetch_optional(&mut *tx)
        .await?.expect("Task attempt row should exist");

        assert_eq!(queue.retry_count(&mut *tx, &task_id).await?, 1);
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
            select schedule, timezone, input from underway.task_schedule where name = $1
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
            where name = $1
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
            select schedule from underway.task_schedule where name = $1
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
}
