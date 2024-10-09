//! Queues provide an interface for managing task execution.
//!
//! Tasks are enqueued onto the queue, using the [`Queue::enqueue`] method, and
//! later dequeued, using the [`Queue::dequeue`] method, when they're executed.
//!
//! The semantics for retrieving a task from the queue are defined by the order
//! of insertion (first-in, first-out) or the priority of the task
//! defines. If a priority is defined, then it's considered before the order the
//! task was inserted.
//!
//! Queues may also define an optional "dead-letter" queue, which provides a
//! secondary queue where failed tasks can be stored for later inspection.
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
//! dequeue method.
//!
//! # Scheduling tasks
//!
//! In order to enable cron-like task scheduling, a schedule must be set on the
//! queue. This can be done via the [`schedule`](Queue::schedule) method. Once
//! set, a worker can be used to run the schedule via the
//! [`run_scheduler`](crate::Job::run_scheduler) method.

use std::{marker::PhantomData, time::Duration as StdDuration};

use builder_states::{Initial, NameSet, PoolSet};
use jiff::{tz::TimeZone, Span, Zoned};
use jiff_cron::Schedule;
use sqlx::{Acquire, PgExecutor, PgPool, Postgres};
use tracing::instrument;
use ulid::Ulid;
use uuid::Uuid;

use crate::task::{DequeuedTask, Id as TaskId, State as TaskState, Task};

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

    /// Error returned by the `cron` crate.
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
    TaskNotFound(Uuid),
}

/// Task queue.
///
/// Queues are responsible for managing tasks that implement the `Task` trait.
/// They are generic over task types, meaning that each queue contains only a
/// specific type of task, ensuring type safety and correctness at compile time.
///
/// # Dead-letter queues
///
/// When a dead-letter queue name is provided, a secondary queue is created with
/// this name. This is a queue of "dead letters". In other words, it's a queue
/// of tasks that have failed on the queue and can't be retried. This can be
/// useful for identifying patterns of failures or reprocessing failed tasks
/// when they're likely to succeed again.
///
/// # Example
///
/// ```rust
/// # use tokio::runtime::Runtime;
/// # use underway::Task;
/// # use underway::task::Result as TaskResult;
/// use sqlx::PgPool;
/// use underway::Queue;
///
/// # struct MyTask;
/// # impl Task for MyTask {
/// #    type Input = ();
/// #    async fn execute(&self, input: Self::Input) -> TaskResult {
/// #        Ok(())
/// #    }
/// # }
/// # fn main() {
/// # let rt = Runtime::new().unwrap();
/// # rt.block_on(async {
/// let pool = PgPool::connect("postgres://user:password@localhost/database").await?;
///
/// let queue = Queue::builder()
///     .name("example_queue")
///     .dead_letter_queue("example_dlq")
///     .pool(pool.clone())
///     .build()
///     .await?;
///
///  # /*
/// let my_task = { /* A type that implements `Task`. */ };
/// # */
/// # let my_task = MyTask;
/// let task_id = queue.enqueue(&pool, &my_task, ()).await?;
///
/// if let Some(task) = queue.dequeue(&pool).await? {
///     // Process the task here
/// }
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// # });
/// # }
/// ```
#[derive(Debug)]
pub struct Queue<T: Task> {
    name: String,
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
    pub fn builder() -> QueueBuilder<T, Initial> {
        QueueBuilder::default()
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
    /// # Errors
    ///
    /// This function will return an error if:
    ///
    /// - The `input` cannot be serialized to JSON.
    /// - The database operation fails during the insertion.
    #[instrument(skip(self, executor, task, input), fields(task.id = tracing::field::Empty), err)]
    pub async fn enqueue<'a, E>(&self, executor: E, task: &T, input: T::Input) -> Result<TaskId>
    where
        E: PgExecutor<'a>,
    {
        let id: TaskId = Ulid::new().into();

        let input_value = serde_json::to_value(&input)?;

        let retry_policy = task.retry_policy();
        let timeout = task.timeout();
        let ttl = task.ttl();
        let delay = task.delay();
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
              max_attempts,
              initial_interval_ms,
              max_interval_ms,
              backoff_coefficient,
              concurrency_key,
              priority
            ) values ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
            "#,
            id,
            self.name,
            input_value,
            StdDuration::try_from(timeout)? as _,
            StdDuration::try_from(ttl)? as _,
            StdDuration::try_from(delay)? as _,
            retry_policy.max_attempts,
            retry_policy.initial_interval_ms,
            retry_policy.max_interval_ms,
            retry_policy.backoff_coefficient,
            concurrency_key as _,
            priority
        )
        .execute(executor)
        .await?;

        Ok(id)
    }

    /// Returns the next available task.
    #[instrument(skip(self, conn), fields(task.id = tracing::field::Empty), err)]
    pub async fn dequeue<'a, A>(&self, conn: A) -> Result<Option<DequeuedTask>>
    where
        A: Acquire<'a, Database = Postgres>,
    {
        let mut tx = conn.begin().await?;

        let task_row = sqlx::query_as!(
            DequeuedTask,
            r#"
            select
              id,
              input,
              timeout,
              retry_count,
              max_attempts,
              initial_interval_ms,
              max_interval_ms,
              backoff_coefficient,
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
            TaskState::Pending as _,
        )
        .fetch_optional(&mut *tx)
        .await?;

        if let Some(task_row) = &task_row {
            let task_id = task_row.id;
            tracing::Span::current().record("task.id", task_id.as_hyphenated().to_string());

            self.mark_task_in_progress(&mut *tx, task_row.id).await?;
        }

        tx.commit().await?;

        Ok(task_row)
    }

    /// Creates a schedule for the queue.
    ///
    /// Schedules are useful when a task should be run periodically, according
    /// to a crontab definition.
    #[instrument(skip(self, executor, zoned_schedule, input), err)]
    pub async fn schedule<'a, E>(
        &self,
        executor: E,
        zoned_schedule: ZonedSchedule,
        input: T::Input,
    ) -> Result
    where
        E: PgExecutor<'a>,
    {
        let input_value = serde_json::to_value(&input)?;

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

    /// Runs an infinite loop that deletes expired tasks on an interval.
    ///
    /// Task time-to-live semantics are only enforced when this method is used.
    pub async fn continuously_delete_expired(&self, period: Span) -> Result {
        let mut interval = tokio::time::interval(period.try_into()?);
        interval.tick().await;
        loop {
            self.delete_expired(&self.pool).await?;
            interval.tick().await;
        }
    }

    #[instrument(skip(self, executor), err)]
    pub(crate) async fn task_schedule<'a, E>(
        &self,
        executor: E,
    ) -> Result<(ZonedSchedule, T::Input)>
    where
        E: PgExecutor<'a>,
    {
        let schedule_row = sqlx::query!(
            r#"
            select schedule, timezone, input from underway.task_schedule where name = $1
            "#,
            self.name,
        )
        .fetch_one(executor)
        .await?;

        let zoned_schedule = ZonedSchedule::new(&schedule_row.schedule, &schedule_row.timezone)?;
        let input = serde_json::from_value(schedule_row.input)?;

        Ok((zoned_schedule, input))
    }

    pub(crate) async fn create<'a, E>(executor: E, name: impl Into<String>) -> Result
    where
        E: PgExecutor<'a>,
    {
        sqlx::query!(
            r#"
            insert into underway.task_queue (name) values ($1)
            on conflict do nothing
            "#,
            name.into()
        )
        .execute(executor)
        .await?;

        Ok(())
    }

    #[instrument(skip(self, executor, task_id), fields(task.id = %task_id.as_hyphenated()), err)]
    async fn mark_task_in_progress<'a, E>(&self, executor: E, task_id: TaskId) -> Result
    where
        E: PgExecutor<'a>,
    {
        let result = sqlx::query!(
            r#"
            update underway.task
            set state = $2,
                started_at = now(),
                updated_at = now()
            where id = $1
            "#,
            task_id,
            TaskState::InProgress as _
        )
        .execute(executor)
        .await?;

        if result.rows_affected() == 0 {
            return Err(Error::TaskNotFound(task_id));
        }

        Ok(())
    }

    #[instrument(skip(self, executor, task_id), fields(task.id = %task_id.as_hyphenated()), err)]
    pub(crate) async fn mark_task_cancelled<'a, E>(&self, executor: E, task_id: TaskId) -> Result
    where
        E: PgExecutor<'a>,
    {
        let result = sqlx::query!(
            r#"
            update underway.task
            set state = $2,
                updated_at = now()
            where id = $1 and state < $3
            "#,
            task_id,
            TaskState::Cancelled as _,
            TaskState::Succeeded as _
        )
        .execute(executor)
        .await?;

        if result.rows_affected() == 0 {
            return Err(Error::TaskNotFound(task_id));
        }

        Ok(())
    }

    #[instrument(skip(self, executor, task_id), fields(task.id = %task_id.as_hyphenated()), err)]
    pub(crate) async fn mark_task_succeeded<'a, E>(&self, executor: E, task_id: TaskId) -> Result
    where
        E: PgExecutor<'a>,
    {
        let result = sqlx::query!(
            r#"
            update underway.task
            set state = $2,
                succeeded_at = now(),
                updated_at = now()
            where id = $1
            "#,
            task_id,
            TaskState::Succeeded as _
        )
        .execute(executor)
        .await?;

        if result.rows_affected() == 0 {
            return Err(Error::TaskNotFound(task_id));
        }

        Ok(())
    }

    #[instrument(skip(self, executor, task_id), fields(task.id = %task_id.as_hyphenated()), err)]
    pub(crate) async fn reschedule_task_for_retry<'a, E>(
        &self,
        executor: E,
        task_id: TaskId,
        retry_count: i32,
        delay: Span,
    ) -> Result
    where
        E: PgExecutor<'a>,
    {
        let result = sqlx::query!(
            r#"
            update underway.task
            set state = $4,
                retry_count = $1,
                delay = $2,
                updated_at = now()
            where id = $3
            "#,
            retry_count,
            std::time::Duration::try_from(delay)? as _,
            task_id,
            TaskState::Pending as _
        )
        .execute(executor)
        .await?;

        if result.rows_affected() == 0 {
            return Err(Error::TaskNotFound(task_id));
        }

        Ok(())
    }

    #[instrument(skip(self, executor, task_id), fields(task.id = %task_id.as_hyphenated()), err)]
    pub(crate) async fn mark_task_failed<'a, E>(&self, executor: E, task_id: TaskId) -> Result
    where
        E: PgExecutor<'a>,
    {
        let result = sqlx::query!(
            r#"
            update underway.task
            set state = $2,
                updated_at = now()
            where id = $1
            "#,
            task_id,
            TaskState::Failed as _
        )
        .execute(executor)
        .await?;

        if result.rows_affected() == 0 {
            return Err(Error::TaskNotFound(task_id));
        }

        Ok(())
    }

    #[instrument(skip(self, executor, task_id), fields(task.id = %task_id.as_hyphenated()), err)]
    pub(crate) async fn update_task_failure<'a, E>(
        &self,
        executor: E,
        task_id: TaskId,
        retry_count: i32,
        error_message: &str,
    ) -> Result
    where
        E: PgExecutor<'a>,
    {
        let result = sqlx::query!(
            r#"
            update underway.task
            set retry_count = $2,
                error_message = $3,
                last_failed_at = now(),
                updated_at = now()
            where id = $1
            "#,
            task_id,
            retry_count,
            error_message,
        )
        .execute(executor)
        .await?;

        if result.rows_affected() == 0 {
            return Err(Error::TaskNotFound(task_id));
        }

        Ok(())
    }

    #[instrument(skip(self, executor, task_id), fields(task.id = %task_id.as_hyphenated()), err)]
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
            task_id,
            dlq_name
        )
        .execute(executor)
        .await?;

        if result.rows_affected() == 0 {
            return Err(Error::TaskNotFound(task_id));
        }

        Ok(())
    }

    #[instrument(skip(self, executor), err)]
    pub(crate) async fn delete_expired<'a, E>(&self, executor: E) -> Result
    where
        E: PgExecutor<'a>,
    {
        if let Some(dlq_name) = &self.dlq_name {
            sqlx::query!(
                r#"
                delete from underway.task
                where (task_queue_name = $1 or task_queue_name = $2) and
                       state != $3 and
                       created_at + ttl < now()
                "#,
                self.name,
                dlq_name,
                TaskState::InProgress as _
            )
            .execute(executor)
            .await?;
        } else {
            sqlx::query!(
                r#"
                delete from underway.task
                where task_queue_name = $1 and
                      state != $2 and
                      created_at + ttl < now()
                "#,
                self.name,
                TaskState::InProgress as _
            )
            .execute(executor)
            .await?;
        }

        Ok(())
    }

    #[instrument(skip(self, executor), err)]
    pub(crate) async fn lock_task<'a, E>(&self, executor: E, key: &str) -> Result
    where
        E: PgExecutor<'a>,
    {
        sqlx::query!("select pg_advisory_xact_lock(hashtext($1))", key,)
            .execute(executor)
            .await?;

        Ok(())
    }
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

/// A builder for [`Queue`].
#[derive(Debug)]
pub struct QueueBuilder<T: Task, S> {
    state: S,
    _marker: PhantomData<T>,
}

impl<T: Task> Default for QueueBuilder<T, Initial> {
    fn default() -> Self {
        QueueBuilder::new()
    }
}

impl<T: Task> QueueBuilder<T, Initial> {
    /// Create a new queue builder.
    pub fn new() -> Self {
        Self {
            state: Initial,
            _marker: PhantomData,
        }
    }

    /// Set the queue name.
    pub fn name(self, name: impl Into<String>) -> QueueBuilder<T, NameSet> {
        QueueBuilder {
            state: NameSet {
                name: name.into(),
                dlq_name: None,
            },
            _marker: PhantomData,
        }
    }
}

impl<T: Task> QueueBuilder<T, NameSet> {
    /// Set the dead-letter queue name.
    pub fn dead_letter_queue(mut self, dlq_name: impl Into<String>) -> Self {
        self.state.dlq_name = Some(dlq_name.into());
        self
    }

    /// Set the database connection pool.
    pub fn pool(self, pool: PgPool) -> QueueBuilder<T, PoolSet> {
        QueueBuilder {
            state: PoolSet {
                name: self.state.name,
                dlq_name: self.state.dlq_name,
                pool,
            },
            _marker: PhantomData,
        }
    }
}

impl<T: Task> QueueBuilder<T, PoolSet> {
    /// Builds the queue.
    pub async fn build(self) -> Result<Queue<T>> {
        let state = self.state;

        let mut tx = state.pool.begin().await?;

        // Create the queue in the database
        Queue::<T>::create(&mut *tx, &state.name).await?;

        // Create the DLQ in the database if specified
        if let Some(ref dlq_name) = state.dlq_name {
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

/// Schedule paired with its time zone.
pub struct ZonedSchedule {
    schedule: Schedule,
    timezone: TimeZone,
}

impl ZonedSchedule {
    /// Create a new schedule which is associated with a time zone.
    pub fn new(cron_expr: &str, time_zone_name: &str) -> Result<Self> {
        let schedule = cron_expr.parse()?;
        let timezone = TimeZone::get(time_zone_name)?;

        assert!(
            timezone.iana_name().is_some(),
            "Time zones must use IANA names for now"
        );

        Ok(Self { schedule, timezone })
    }

    fn cron_expr(&self) -> String {
        self.schedule.to_string()
    }

    fn iana_name(&self) -> &str {
        self.timezone
            .iana_name()
            .expect("iana_name should always be Some because new ensures valid time zone")
    }

    pub(crate) fn duration_until_next(&self) -> Option<StdDuration> {
        // Construct a date-time with the schedule's time zone.
        let now_with_tz = Zoned::now().with_time_zone(self.timezone.clone());
        if let Some(next_timestamp) = self.schedule.upcoming(self.timezone.clone()).next() {
            let until_next = next_timestamp.duration_since(&now_with_tz);
            // N.B. We're assigning default on failure here.
            return Some(until_next.try_into().unwrap_or_default());
        }

        None
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use jiff::Span;

    use super::*;
    use crate::task::Result as TaskResult;

    struct TestTask;

    impl Task for TestTask {
        type Input = serde_json::Value;

        async fn execute(&self, _: Self::Input) -> TaskResult {
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

        let task_id = queue.enqueue(&pool, &task, input.clone()).await?;

        // Query the database to verify the task was enqueued
        let dequeued_task = sqlx::query!(
            r#"
            select id, input, retry_count, max_attempts, initial_interval_ms, max_interval_ms, backoff_coefficient, concurrency_key, priority
            from underway.task
            where id = $1
            "#,
            task_id
        )
        .fetch_one(&pool)
        .await?;

        assert_eq!(dequeued_task.id, task_id);
        assert_eq!(dequeued_task.input, input);
        assert_eq!(dequeued_task.retry_count, 0);

        let expected_retry_policy = task.retry_policy();
        assert_eq!(
            dequeued_task.max_attempts,
            expected_retry_policy.max_attempts
        );
        assert_eq!(
            dequeued_task.initial_interval_ms,
            expected_retry_policy.initial_interval_ms
        );
        assert_eq!(
            dequeued_task.max_interval_ms,
            expected_retry_policy.max_interval_ms
        );
        assert_eq!(
            dequeued_task.backoff_coefficient,
            expected_retry_policy.backoff_coefficient
        );

        assert_eq!(dequeued_task.concurrency_key, task.concurrency_key());
        assert_eq!(dequeued_task.priority, task.priority());

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
        let task_id = queue.enqueue(&pool, &task, input.clone()).await?;

        // Dequeue the task
        let dequeued_task = queue.dequeue(&pool).await?;

        assert!(dequeued_task.is_some(), "We should have a task enqueued");

        let dequeued_task = dequeued_task.unwrap();
        assert_eq!(dequeued_task.id, task_id);
        assert_eq!(dequeued_task.input, input);
        assert_eq!(dequeued_task.retry_count, 0);

        let expected_retry_policy = task.retry_policy();
        assert_eq!(
            dequeued_task.max_attempts,
            expected_retry_policy.max_attempts
        );
        assert_eq!(
            dequeued_task.initial_interval_ms,
            expected_retry_policy.initial_interval_ms
        );
        assert_eq!(
            dequeued_task.max_interval_ms,
            expected_retry_policy.max_interval_ms
        );
        assert_eq!(
            dequeued_task.backoff_coefficient,
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
            task_id
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
            queue.enqueue(&pool, &task, input.clone()).await?;
        }

        // Simulate concurrent dequeues
        let handles: Vec<_> = (0..5)
            .map(|_| {
                let queue = queue.clone();
                let pool = pool.clone();
                tokio::spawn(async move { queue.dequeue(&pool).await })
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
            assert!(task_ids.insert(dequeued_task.id));
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
        let dequeued_task = queue.dequeue(&pool).await?;

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
        let task_id = queue.enqueue(&pool, &task, input).await?;

        // Mark the task as in progress
        queue.mark_task_in_progress(&pool, task_id).await?;

        // Verify the task state
        let task_row = sqlx::query!(
            r#"
            select id, state as "state: TaskState" from underway.task where id = $1
            "#,
            task_id
        )
        .fetch_one(&pool)
        .await?;

        assert_eq!(task_row.state, TaskState::InProgress);

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
        let task_id = queue.enqueue(&pool, &task, input).await?;

        // Reschedule the task for retry
        let retry_count = 1;

        // TODO: Converting PgInterval to Span may need to be revisited as minutes and
        // seconds do no properly line up.
        let delay = Span::new().seconds(60);

        queue
            .reschedule_task_for_retry(&pool, task_id, retry_count, delay)
            .await?;

        // Query to verify rescheduled task
        let dequeued_task = sqlx::query!(
            r#"
            select id, retry_count, delay from underway.task where id = $1
            "#,
            task_id
        )
        .fetch_optional(&pool)
        .await?;

        assert!(dequeued_task.is_some());

        let dequeued_task = dequeued_task.unwrap();
        assert_eq!(dequeued_task.retry_count, retry_count);

        // TODO:
        // assertion `left == right` failed
        //  left: PT60s
        // right: PT60s
        // assert_eq!(pg_interval_to_span(&dequeued_task.delay), delay);

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
        let task_id = queue.enqueue(&pool, &task, input).await?;

        // Cancel the task
        queue.mark_task_cancelled(&pool, task_id).await?;

        // Verify the task state
        let task_row = sqlx::query!(
            r#"
            select id, state as "state: TaskState" from underway.task where id = $1
            "#,
            task_id
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
        let task_id = queue.enqueue(&pool, &task, input).await?;

        // Mark the task as succeeded
        queue.mark_task_succeeded(&pool, task_id).await?;

        // Verify the task state
        let task_row = sqlx::query!(
            r#"
            select id, state as "state: TaskState" from underway.task where id = $1
            "#,
            task_id
        )
        .fetch_one(&pool)
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

        let nonexistent_task_id = Uuid::new_v4();

        // Attempt to mark a non-existent task as succeeded
        let result = queue.mark_task_succeeded(&pool, nonexistent_task_id).await;

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

        let nonexistent_task_id = Uuid::new_v4();

        // Attempt to mark a non-existent task as succeeded
        let result = queue.mark_task_failed(&pool, nonexistent_task_id).await;

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
        let task_id = queue.enqueue(&pool, &task, input).await?;

        // Mark the task as failed
        queue.mark_task_failed(&pool, task_id).await?;

        // Verify the task state
        let task_row = sqlx::query!(
            r#"
            select id, state as "state: TaskState" from underway.task where id = $1
            "#,
            task_id
        )
        .fetch_optional(&pool)
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
        let task_id = queue.enqueue(&pool, &task, input).await?;

        // Update task failure details
        let retry_count = 2;
        let error_message = "Some failure occurred";

        queue
            .update_task_failure(&pool, task_id, retry_count, error_message)
            .await?;

        // Query to verify the failure update
        let task_row = sqlx::query!(
            r#"
            select id, retry_count, error_message from underway.task where id = $1
            "#,
            task_id
        )
        .fetch_optional(&pool)
        .await?;

        assert!(task_row.is_some());
        let task_row = task_row.unwrap();
        assert_eq!(task_row.retry_count, retry_count);
        assert_eq!(task_row.error_message, Some(error_message.to_string()));

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
        let task_id = queue.enqueue(&pool, &task, input).await?;

        // Move the task to DLQ
        queue.move_task_to_dlq(&pool, task_id, "test_dlq").await?;

        // Query to verify the task is in the DLQ
        let task_row = sqlx::query!(
            r#"
            select id, task_queue_name from underway.task where id = $1
            "#,
            task_id
        )
        .fetch_optional(&pool)
        .await?;

        assert!(task_row.is_some());
        assert_eq!(task_row.unwrap().task_queue_name, "test_dlq");

        Ok(())
    }
}
