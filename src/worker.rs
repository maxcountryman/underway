//! Workers interface with queues to execute tasks.
//!
//! Until a worker is run, tasks remain on the queue, waiting to be processed.
//! Workers complement queues by providing the mechanism to process tasks, first
//! by finding an available task, deserializing its input, and then passing this
//! input to an invocation of its execute method.
//!
//! As a task is being processed, the worker will assign various state
//! transitions to the task. Eventually the task will either be completed or
//! failed.
//!
//! Also note that workers retry failed tasks in accordance with their
//! configured retry policies.
//!
//! # Running workers
//!
//! Oftentimes you'll define [a job](crate::job) and use its methods to run a
//! worker. However, workers can be manually constructed and only require a
//! queue and task:
//!
//! ```rust
//! # use tokio::runtime::Runtime;
//! # use underway::Task;
//! # use underway::task::Result as TaskResult;
//! # use sqlx::PgPool;
//! use underway::{Queue, Worker};
//!
//! # struct MyTask;
//! # impl Task for MyTask {
//! #    type Input = ();
//! #    async fn execute(&self, input: Self::Input) -> TaskResult {
//! #        Ok(())
//! #    }
//! # }
//! # fn main() {
//! # let rt = Runtime::new().unwrap();
//! # rt.block_on(async {
//! # /*
//! let pool = { /* A `PgPool`. */ };
//! # */
//! # let pool = PgPool::connect("postgres://user:password@localhost/database").await?;
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
//! // Create a new worker from the queue and task.
//! let worker = Worker::new(queue, task);
//!
//! // Run the worker.
//! worker.run().await?;
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! # });
//! # }
//! ```
//!
//! # Scaling task processing
//!
//! Workers can also be used to scale task processing. To do so, we can spin up
//! as many workers as we might like to ensure tasks are processed in a timely
//! manner. Also note that workers do not need to be run in-process, and can be
//! run from a separate binary altogether.
//!
//! ```rust
//! # use tokio::runtime::Runtime;
//! # use underway::Task;
//! # use underway::task::Result as TaskResult;
//! # use sqlx::PgPool;
//! # use underway::{Queue, Worker};
//! # #[derive(Clone)]
//! # struct MyTask;
//! # impl Task for MyTask {
//! #    type Input = ();
//! #    async fn execute(&self, input: Self::Input) -> TaskResult {
//! #        Ok(())
//! #    }
//! # }
//! # fn main() {
//! # let rt = Runtime::new().unwrap();
//! # rt.block_on(async {
//! # let pool = PgPool::connect("postgres://user:password@localhost/database").await?;
//! # let queue = Queue::builder()
//! #    .name("example_queue")
//! #    .pool(pool.clone())
//! #    .build()
//! #    .await?;
//! # let task = MyTask;
//! # let worker = Worker::new(queue, task);
//! // Spin up a number of workers to process tasks concurrently.
//! for _ in 0..4 {
//!     let task_worker = worker.clone();
//!     tokio::task::spawn(async move { task_worker.run().await });
//! }
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! # });
//! # }
//! ```

use jiff::{Span, ToSpan};
use serde::{de::DeserializeOwned, Serialize};
use sqlx::{postgres::types::PgInterval, PgConnection};
use tracing::instrument;

use crate::{
    job::Job,
    queue::{Error as QueueError, Queue},
    task::{DequeuedTask, Error as TaskError, Id as TaskId, RetryCount, RetryPolicy, Task},
};
pub(crate) type Result = std::result::Result<(), Error>;

/// A worker that's generic over the task it processes.
#[derive(Debug, Clone)]
pub struct Worker<T: Task> {
    queue: Queue<T>,
    task: T,
}

/// Worker errors.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Error returned from queue operations.
    #[error(transparent)]
    Queue(#[from] QueueError),

    /// Error returned from task execution.
    #[error(transparent)]
    Task(#[from] TaskError),

    /// Error returned from database operations.
    #[error(transparent)]
    Database(#[from] sqlx::Error),

    /// Error returned by the `serde_json` crate when serializing or
    /// deserializing task input.
    #[error(transparent)]
    Json(#[from] serde_json::Error),

    /// Error returned by the `jiff` crate.
    #[error(transparent)]
    Jiff(#[from] jiff::Error),
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
    /// Creates a new worker with the given queue and task.
    pub const fn new(queue: Queue<T>, task: T) -> Self {
        Self { queue, task }
    }

    /// Runs the worker, processing tasks as they become available.
    ///
    /// Tasks are processed via polling in a loop. A one-second sleep occurs
    /// between polls.
    pub async fn run(&self) -> Result {
        self.run_every(1.second()).await
    }

    /// Same as `run` but allows for the configuration of the span between
    /// polls.
    pub async fn run_every(&self, span: Span) -> Result {
        let mut interval = tokio::time::interval(span.try_into()?);
        interval.tick().await;
        loop {
            self.process_next_task().await?;
            interval.tick().await;
        }
    }

    /// Processes the next available task in the queue.
    ///
    /// When a task is found, its execute method will be invoked with the
    /// deqeued input.
    ///
    /// If the execution exceeds the task's timeout then the execute future is
    /// cancelled. This can also result in the task being marked failed if its
    /// retry policy has been exhausted.
    ///
    /// Tasks may also fail for other reasons during execution. Unless a failure
    /// is [explicitly fatal](crate::task::Error::Fatal) or no more retries are
    /// left, then the task will be re-queued in a
    /// [`Pending`](crate::task::State::Pending) state.
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

    async fn handle_task_error(
        &self,
        err: TaskError,
        conn: &mut PgConnection,
        task_id: TaskId,
        dequeued_task: DequeuedTask,
    ) -> Result {
        tracing::error!(err = %err, "Task execution encountered an error");

        // Short-circuit on fatal errors.
        if matches!(err, TaskError::Fatal(_)) {
            return self.finalize_task_failure(conn, task_id).await;
        }

        let retry_count = dequeued_task.retry_count + 1;
        let retry_policy: RetryPolicy = dequeued_task.into();

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
        dequeued_task: DequeuedTask,
    ) -> Result {
        tracing::error!("Task execution timed out");

        let retry_count = dequeued_task.retry_count + 1;
        let retry_policy: RetryPolicy = dequeued_task.into();

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

        self.queue
            .reschedule_task_for_retry(&mut *conn, task_id, retry_count, delay)
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

pub(crate) fn pg_interval_to_span(
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

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use sqlx::PgPool;
    use tokio::sync::Mutex;

    use super::*;
    use crate::task::{Result as TaskResult, State as TaskState};

    struct TestTask;

    impl Task for TestTask {
        type Input = ();

        async fn execute(&self, _: Self::Input) -> TaskResult {
            Ok(())
        }
    }

    #[derive(Clone)]
    struct FailingTask {
        fail_times: Arc<Mutex<u32>>,
    }

    impl Task for FailingTask {
        type Input = ();

        async fn execute(&self, _: Self::Input) -> TaskResult {
            let mut fail_times = self.fail_times.lock().await;
            if *fail_times > 0 {
                *fail_times -= 1;
                Err(TaskError::Retryable("Simulated failure".into()))
            } else {
                Ok(())
            }
        }
    }

    #[sqlx::test]
    async fn process_next_task(pool: PgPool) -> sqlx::Result<(), Error> {
        let queue = Queue::builder()
            .name("process_next_task")
            .pool(pool.clone())
            .build()
            .await?;

        // Enqueue a task.
        let task = TestTask;
        queue.enqueue(&pool, &task, ()).await?;
        assert!(queue.dequeue(&pool).await?.is_some());

        // Process the task.
        let worker = Worker::new(queue.clone(), task);
        worker.process_next_task().await?;

        // Ensure the task is no longer available on the queue.
        assert!(queue.dequeue(&pool).await?.is_none());

        Ok(())
    }

    #[sqlx::test]
    async fn process_retries(pool: PgPool) -> sqlx::Result<(), Error> {
        let queue = Queue::builder()
            .name("retry_test_queue")
            .pool(pool.clone())
            .build()
            .await?;

        let fail_times = Arc::new(Mutex::new(2));
        let task = FailingTask {
            fail_times: fail_times.clone(),
        };
        let worker = Worker::new(queue.clone(), task.clone());

        // Enqueue the task
        let task_id = queue.enqueue(&pool, &worker.task, ()).await?;

        // Process the task multiple times to simulate retries
        for retries in 0..3 {
            let delay = task.retry_policy().calculate_delay(retries);
            tokio::time::sleep(delay.try_into()?).await;
            worker.process_next_task().await?;
        }

        // Verify that the fail_times counter has reached zero
        let remaining_fail_times = *fail_times.lock().await;
        assert_eq!(remaining_fail_times, 0);

        // Verify the task eventually succeeded
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

        assert_eq!(dequeued_task.state, TaskState::Succeeded);

        Ok(())
    }
}
