use chrono::Utc;
use jiff::{tz::TimeZone, Span, Zoned};
use serde::{de::DeserializeOwned, Serialize};
use sqlx::{postgres::types::PgInterval, PgConnection};
use tracing::instrument;

use crate::{
    job::Job,
    queue::{Error as QueueError, Queue, TaskRow},
    task::{Error as TaskError, Id as TaskId, RetryCount, RetryPolicy, Task},
};
pub(crate) type Result = std::result::Result<(), Error>;

pub struct Worker<T: Task> {
    queue: Queue<T>,
    task: T,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Error returned by the `sqlx` crate during database operations.
    #[error(transparent)]
    Database(#[from] sqlx::Error),

    /// Error returned by the `serde_json` crate when serializing or
    /// deserializing task input.
    #[error(transparent)]
    Json(#[from] serde_json::Error),

    /// Error returned by the `chrono_tz` crate when parsing time zones.
    #[error(transparent)]
    ChronoTz(#[from] chrono_tz::ParseError),

    /// Error returned from queue operations.
    #[error(transparent)]
    Queue(#[from] QueueError),

    /// Error returned from task execution.
    #[error(transparent)]
    Task(#[from] TaskError),
}

impl<I> From<Job<I>> for Worker<Job<I>>
where
    I: Clone + DeserializeOwned + Serialize + Send + 'static,
{
    fn from(job: Job<I>) -> Self {
        Self {
            queue: job.queue.clone(),
            task: job,
        }
    }
}

impl<I> From<&Job<I>> for Worker<Job<I>>
where
    I: Clone + DeserializeOwned + Serialize + Send + 'static,
{
    fn from(job: &Job<I>) -> Self {
        Self {
            queue: job.queue.clone(),
            task: job.clone(),
        }
    }
}

impl<T: Task> Worker<T> {
    pub const fn new(queue: Queue<T>, task: T) -> Self {
        Self { queue, task }
    }

    pub async fn run(&self) -> Result {
        let period = tokio::time::Duration::from_secs(1);
        let mut interval = tokio::time::interval(period);
        interval.tick().await;
        loop {
            self.process_next_task().await?;
            interval.tick().await;
        }
    }

    pub async fn run_on(&self, interval: &mut tokio::time::Interval) -> Result {
        interval.tick().await;
        loop {
            self.process_next_task().await?;
            interval.tick().await;
        }
    }

    pub async fn run_scheduler(&self) -> Result {
        let period = tokio::time::Duration::from_secs(1);
        let mut interval = tokio::time::interval(period);
        self.run_scheduler_on(&mut interval).await
    }

    pub async fn run_scheduler_on(&self, interval: &mut tokio::time::Interval) -> Result {
        interval.tick().await;
        loop {
            self.process_next_schedule().await?;
            interval.tick().await;
        }
    }

    #[instrument(skip(self), fields(task.id = tracing::field::Empty), err)]
    pub async fn process_next_task(&self) -> Result {
        let mut tx = self.queue.pool.begin().await?;

        if let Some(task_row) = self.queue.dequeue(&mut tx).await? {
            let task_id = task_row.id;
            tracing::Span::current().record("task.id", task_id.as_hyphenated().to_string());

            // Ensure that only one worker may process a task of a given concurrency key at
            // a time.
            if let Some(concurrency_key) = &task_row.concurrency_key {
                self.queue.lock_task(&mut *tx, concurrency_key).await?;
            }

            let input: T::Input = serde_json::from_value(task_row.input.clone())?;

            let timeout = pg_interval_to_span(&task_row.timeout)
                .try_into()
                .expect("Task timeout should be compatible with std::time");

            tokio::select! {
                result = self.task.execute(input) => {
                    match result {
                        Ok(_) => {
                            self.queue.mark_task_succeeded(&mut *tx, task_id).await?;
                        }
                        Err(err) => {
                            self.handle_task_error(err, &mut tx, task_id, task_row)
                                .await?;
                        }
                    }
                }

                _ = tokio::time::sleep(timeout) => {
                    tracing::error!("Task execution timed out");
                    self.handle_task_timeout(&mut tx, task_id, task_row).await?;
                }
            }

            tx.commit().await?;
        }

        Ok(())
    }

    #[instrument(skip(self), err)]
    pub async fn process_next_schedule(&self) -> Result {
        let (schedule, timezone, input) = self.queue.task_schedule(&self.queue.pool).await?;

        // TODO: We need to map jiff to chrono for the time being. Ideally the cron
        // parser would support jiff directly, but for now we'll do this.
        let schedule_tz: chrono_tz::Tz = timezone
            .iana_name()
            .ok_or(QueueError::IncompatibleTimeZone)?
            .parse()?;
        let now_with_tz = Utc::now().with_timezone(&schedule_tz);

        if let Some(next_timestamp) = schedule.upcoming(schedule_tz).next() {
            let duration_until_next = next_timestamp.signed_duration_since(now_with_tz);
            let deadline = duration_until_next.to_std().unwrap_or_default(); // TODO: Is default
                                                                             // what we want?
            tokio::time::sleep(deadline).await;
            self.queue
                .enqueue(&self.queue.pool, &self.task, input)
                .await?;
        }

        Ok(())
    }

    async fn handle_task_error(
        &self,
        err: TaskError,
        conn: &mut PgConnection,
        task_id: TaskId,
        task_row: TaskRow,
    ) -> Result {
        tracing::error!(err = %err, "Task execution encountered an error");

        // Short-circuit on fatal errors.
        if matches!(err, TaskError::Fatal(_)) {
            return self.finalize_task_failure(conn, task_id).await;
        }

        let retry_count = task_row.retry_count + 1;
        let retry_policy: RetryPolicy = task_row.into();

        self.queue
            .update_task_failure(&mut *conn, task_id, retry_count, &err.to_string())
            .await?;

        if retry_count < retry_policy.max_attempts {
            self.schedule_task_retry(conn, task_id, retry_count, &retry_policy)
                .await?;
        } else {
            self.finalize_task_failure(conn, task_id).await?;
        }

        Ok(())
    }

    async fn handle_task_timeout<'a>(
        &self,
        conn: &mut PgConnection,
        task_id: TaskId,
        task_row: TaskRow,
    ) -> Result {
        tracing::error!("Task execution timed out");

        let retry_count = task_row.retry_count + 1;
        let retry_policy: RetryPolicy = task_row.into();

        self.queue
            .update_task_failure(&mut *conn, task_id, retry_count, "Task timed out")
            .await?;

        if retry_count < retry_policy.max_attempts {
            self.schedule_task_retry(conn, task_id, retry_count, &retry_policy)
                .await?;
        } else {
            self.finalize_task_failure(conn, task_id).await?;
        }

        Ok(())
    }

    async fn schedule_task_retry(
        &self,
        conn: &mut PgConnection,
        task_id: TaskId,
        retry_count: RetryCount,
        retry_policy: &RetryPolicy,
    ) -> Result {
        tracing::info!("Retry policy available, scheduling retry");

        let delay = retry_policy.calculate_delay(retry_count);
        let next_available_at = Zoned::now()
            .with_time_zone(TimeZone::UTC)
            .saturating_add(delay);

        self.queue
            .reschedule_task_for_retry(&mut *conn, task_id, retry_count, next_available_at)
            .await?;

        Ok(())
    }

    async fn finalize_task_failure(&self, conn: &mut PgConnection, task_id: TaskId) -> Result {
        tracing::info!("Retry policy exhausted, handling failed task");

        self.queue.mark_task_failed(&mut *conn, task_id).await?;

        if let Some(dlq_name) = &self.queue.dlq_name {
            self.queue
                .move_task_to_dlq(&mut *conn, task_id, dlq_name)
                .await?;
        }

        Ok(())
    }
}

fn pg_interval_to_span(
    PgInterval {
        months,
        days,
        microseconds,
    }: &PgInterval,
) -> Span {
    Span::new()
        .months(*months)
        .days(*days)
        .microseconds(*microseconds)
}
