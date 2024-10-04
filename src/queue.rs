//! Queues provide an interface for managing task execution.
//!
//! ## Example
//! ```rust
//! let queue = QueueBuilder::new()
//!     .name("example_queue")
//!     .dead_letter_queue("example_dlq")
//!     .pool(pg_pool)
//!     .build()
//!     .await?;
//!
//! let task_id = queue.enqueue(&pg_pool, &my_task, task_input).await?;
//!
//! if let Some(task) = queue.dequeue(&pg_pool).await? {
//!     // Process the task here
//! }
//! ```
use std::marker::PhantomData;

use builder_states::{Initial, NameSet, PoolSet};
use cron::Schedule;
use jiff::{tz::TimeZone, Span, Zoned};
use sqlx::{postgres::types::PgInterval, Acquire, PgExecutor, PgPool, Postgres};
use tracing::instrument;
use ulid::Ulid;
use uuid::Uuid;

use crate::{
    task::{Id as TaskId, State as TaskState, Task},
    timestamp,
};

type Result<T = ()> = std::result::Result<T, Error>;

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
    Cron(#[from] cron::error::Error),

    /// Error returned by the `jiff` crate.
    #[error(transparent)]
    Jiff(#[from] jiff::Error),

    /// Error returned by the `chrono_tz` crate when parsing time zones.
    #[error(transparent)]
    ChronoTz(#[from] chrono_tz::ParseError),

    #[error("Task with ID {0} not found.")]
    TaskNotFound(Uuid),
}

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
    /// Creates a new queue with the given name.
    pub async fn create<'a, E>(executor: E, name: impl Into<String>) -> Result
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

    /// Enqueues a new task, returning the task ID.
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
        let available_at = task.available_at().timestamp();
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
              available_at,
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
            std::time::Duration::try_from(timeout)? as _,
            std::time::Duration::try_from(ttl)? as _,
            timestamp::Timestamp(available_at) as _,
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
    pub async fn dequeue<'a, A>(&self, conn: A) -> Result<Option<TaskRow>>
    where
        A: Acquire<'a, Database = Postgres>,
    {
        let mut tx = conn.begin().await?;

        let task_row = sqlx::query_as!(
            TaskRow,
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
              and available_at <= now()
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

    /// Returns a tuple of the zoned schedule and task input.
    #[instrument(skip(self, executor), err)]
    pub async fn task_schedule<'a, E>(&self, executor: E) -> Result<(ZonedSchedule, T::Input)>
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

    /// Runs an infinite loop that deletes expired tasks on an interval.
    pub async fn continuously_delete_expired(&self, period: Span) -> Result {
        let mut interval = tokio::time::interval(period.try_into()?);
        interval.tick().await;
        loop {
            self.delete_expired(&self.pool).await?;
            interval.tick().await;
        }
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
        next_available_at: Zoned,
    ) -> Result
    where
        E: PgExecutor<'a>,
    {
        let next_available_at = next_available_at.timestamp();
        let result = sqlx::query!(
            r#"
            update underway.task
            set state = $4,
                retry_count = $1,
                available_at = $2,
                updated_at = now()
            where id = $3
            "#,
            retry_count,
            timestamp::Timestamp(next_available_at) as _,
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

#[derive(Debug, sqlx::FromRow)]
pub struct TaskRow {
    pub id: Uuid,
    pub input: serde_json::Value,
    pub timeout: PgInterval,
    pub retry_count: i32,
    pub max_attempts: i32,
    pub initial_interval_ms: i32,
    pub max_interval_ms: i32,
    pub backoff_coefficient: f32,
    pub concurrency_key: Option<String>,
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

#[derive(Debug, Default)]
pub struct QueueBuilder<T: Task, S> {
    state: S,
    _marker: PhantomData<T>,
}

impl<T: Task> QueueBuilder<T, Initial> {
    pub fn new() -> Self {
        Self {
            state: Initial,
            _marker: PhantomData,
        }
    }

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
    pub fn dead_letter_queue(mut self, dlq_name: impl Into<String>) -> Self {
        self.state.dlq_name = Some(dlq_name.into());
        self
    }

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
///
/// # Note
///
/// This is something of a bandage: `jiff` doesn't offer a way to serialize time
/// zone identifiers, which makes them difficult to use as types in an API.
/// Likewise, because `cron` depends on `chrono` and `chrono_tz`, IANA names are
/// necessary for the time being.
///
/// In the future, removing `chrono` and `chrono_tz` as dependencies is a goal
/// at which point we'll prefer to remove this type and avoid directly parsing
/// cron expressions and time zone names, offloading that responsibility to the
/// requisite crates and relocating this particular flavor of falibility with
/// the surrounding application.
pub struct ZonedSchedule {
    schedule: Schedule,
    timezone: TimeZone,
}

impl ZonedSchedule {
    /// Create a new schedule which is associated with a time zone.
    pub fn new(cron_expr: &str, time_zone_name: &str) -> Result<Self> {
        let schedule = cron_expr.parse()?;
        let timezone = TimeZone::get(time_zone_name)?;

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

    pub(crate) fn duration_until_next(&self) -> Result<Option<std::time::Duration>> {
        // TODO: We need to map jiff to chrono for the time being. Ideally the cron
        // parser would support jiff directly, but for now we'll do this.

        // Convert to Chrono's time zone.
        let schedule_tz: chrono_tz::Tz = self.iana_name().parse()?;

        // Construct a date-time with the schedule's time zone.
        let now_with_tz = chrono::Utc::now().with_timezone(&schedule_tz);

        if let Some(next_timestamp) = self.schedule.upcoming(schedule_tz).next() {
            let until_next = next_timestamp.signed_duration_since(now_with_tz);
            // N.B. We're assigning default on failure here.
            return Ok(Some(until_next.to_std().unwrap_or_default()));
        }

        Ok(None)
    }
}

mod tests {
    use std::collections::HashSet;

    use jiff::{Span, Timestamp};

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
        let queue: Queue<TestTask> = QueueBuilder::new()
            .name("test_queue")
            .pool(pool)
            .build()
            .await?;

        assert_eq!(queue.name, "test_queue");

        Ok(())
    }

    #[sqlx::test]
    async fn build_queue_with_dlq(pool: PgPool) -> sqlx::Result<(), Error> {
        let queue: Queue<TestTask> = QueueBuilder::new()
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
        let queue = QueueBuilder::new()
            .name("test_enqueue")
            .pool(pool.clone())
            .build()
            .await?;

        let input = serde_json::json!({ "key": "value" });
        let task = TestTask;

        let task_id = queue.enqueue(&pool, &task, input.clone()).await?;

        // Query the database to verify the task was enqueued
        let task_row = sqlx::query!(
            r#"
            select id, input, retry_count, max_attempts, initial_interval_ms, max_interval_ms, backoff_coefficient, concurrency_key, priority
            from underway.task
            where id = $1
            "#,
            task_id
        )
        .fetch_one(&pool)
        .await?;

        assert_eq!(task_row.retry_count, 0);
        assert_eq!(task_row.input, input);
        assert_eq!(task_row.priority, task.priority());

        Ok(())
    }

    #[sqlx::test]
    async fn dequeue_task(pool: PgPool) -> sqlx::Result<(), Error> {
        let queue = QueueBuilder::new()
            .name("test_dequeue")
            .pool(pool.clone())
            .build()
            .await?;

        let input = serde_json::json!({ "key": "value" });
        let task = TestTask;

        // Enqueue a task
        queue.enqueue(&pool, &task, input).await?;

        // Dequeue the task
        let task_row = queue.dequeue(&pool).await?;

        assert!(task_row.is_some());
        let task_row = task_row.unwrap();
        assert_eq!(task_row.retry_count, 0);

        Ok(())
    }

    #[sqlx::test]
    async fn concurrent_dequeue(pool: PgPool) -> sqlx::Result<(), Error> {
        let queue = QueueBuilder::new()
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
        let results = futures::future::join_all(handles).await;

        // Ensure all tasks were dequeued without duplicates
        let mut task_ids = HashSet::new();
        for res in results {
            let task_row = res.unwrap().unwrap().unwrap();
            assert!(task_ids.insert(task_row.id));
        }

        assert_eq!(task_ids.len(), 5);

        Ok(())
    }

    #[sqlx::test]
    async fn dequeue_from_empty_queue(pool: PgPool) -> sqlx::Result<(), Error> {
        let queue: Queue<TestTask> = QueueBuilder::new()
            .name("test_empty_dequeue")
            .pool(pool.clone())
            .build()
            .await?;

        // Attempt to dequeue without enqueuing any tasks
        let task_row = queue.dequeue(&pool).await?;

        assert!(task_row.is_none());

        Ok(())
    }

    #[sqlx::test]
    async fn mark_task_in_progress(pool: PgPool) -> sqlx::Result<(), Error> {
        let queue = QueueBuilder::new()
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
    async fn reschedule_task(pool: PgPool) -> sqlx::Result<(), Error> {
        let queue = QueueBuilder::new()
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
        let next_available_at = Timestamp::now() + Span::new().seconds(60);

        queue
            .reschedule_task_for_retry(
                &pool,
                task_id,
                retry_count,
                next_available_at.to_zoned(TimeZone::UTC),
            )
            .await?;

        // Query to verify rescheduled task
        let task_row = sqlx::query!(
            r#"
            select id, retry_count, available_at as "available_at: timestamp::Timestamp" from underway.task where id = $1
            "#,
            task_id
        )
        .fetch_optional(&pool)
        .await?;

        assert!(task_row.is_some());
        let task_row = task_row.unwrap();
        assert_eq!(task_row.retry_count, retry_count);

        // Verify the next available time
        assert_eq!(task_row.available_at.0, next_available_at);

        Ok(())
    }

    #[sqlx::test]
    async fn mark_task_cancelled(pool: PgPool) -> sqlx::Result<(), Error> {
        let queue = QueueBuilder::new()
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
        let queue = QueueBuilder::new()
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
        let queue: Queue<TestTask> = QueueBuilder::new()
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
        let queue: Queue<TestTask> = QueueBuilder::new()
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
        let queue = QueueBuilder::new()
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
        let queue = QueueBuilder::new()
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
        let queue = QueueBuilder::new()
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
