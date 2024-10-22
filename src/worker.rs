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
//! ```rust,no_run
//! # use tokio::runtime::Runtime;
//! # use underway::Task;
//! # use underway::task::Result as TaskResult;
//! # use sqlx::{PgPool, Transaction, Postgres};
//! # use std::sync::Arc;
//! use underway::{Queue, Worker};
//!
//! # struct MyTask;
//! # impl Task for MyTask {
//! #    type Input = ();
//! #    type Output = ();
//! #    async fn execute(&self, tx: Transaction<'_, Postgres>, input: Self::Input) -> TaskResult<Self::Output> {
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
//! let task = { /* An `Task`. */ };
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
//! ```rust,no_run
//! # use tokio::runtime::Runtime;
//! # use underway::Task;
//! # use underway::task::Result as TaskResult;
//! # use sqlx::{Transaction, Postgres, PgPool};
//! # use underway::{Queue, Worker};
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
//! # let pool = PgPool::connect(&std::env::var("DATABASE_URL")?).await?;
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
//!
//! # Stopping workers safely
//!
//! In order to ensure that workers are interrupted while handling in-progress
//! tasks, the [`graceful_shutdown`](crate::queue::graceful_shutdown) function
//! is provided.
//!
//! This function allows you to politely ask all workers to stop processing new
//! tasks. At the same time, workers are also aware of any in-progress tasks
//! they're working on and will wait for these to be done or timeout.
//!
//! For cases where it's unimportant to wait for tasks to complete, this routine
//! can be ignored.

use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};

use jiff::{Span, ToSpan};
use sqlx::{
    postgres::{types::PgInterval, PgListener},
    Acquire, PgConnection,
};
use tokio::{sync::Semaphore, task::JoinSet};
use tracing::instrument;

use crate::{
    queue::{Error as QueueError, Queue, SHUTDOWN_CHANNEL},
    task::{DequeuedTask, Error as TaskError, Id as TaskId, RetryCount, RetryPolicy, Task},
};
pub(crate) type Result = std::result::Result<(), Error>;

/// A worker that's generic over the task it processes.
#[derive(Debug)]
pub struct Worker<T: Task> {
    queue: Queue<T>,
    task: Arc<T>,

    // Limits the number of concurrent `Task::execute` invocations this worker will be allowed.
    concurrency_limit: usize,

    // Indicates the underlying queue has received a shutdown signal.
    queue_shutdown: Arc<AtomicBool>,
}

impl<T: Task> Clone for Worker<T> {
    fn clone(&self) -> Self {
        Self {
            queue: self.queue.clone(),
            task: self.task.clone(),
            concurrency_limit: self.concurrency_limit,
            queue_shutdown: self.queue_shutdown.clone(),
        }
    }
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

impl<T: Task + Sync> Worker<T> {
    /// Creates a new worker with the given queue and task.
    pub fn new(queue: Queue<T>, task: T) -> Self {
        Self {
            queue,
            task: Arc::new(task),
            concurrency_limit: num_cpus::get(),
            queue_shutdown: Arc::new(false.into()),
        }
    }

    /// Sets the concurrency limit for this worker.
    ///
    /// Defaults to CPU count as per [`num_cpus::get`].
    pub fn concurrency_limit(mut self, concurrency_limit: usize) -> Self {
        self.concurrency_limit = concurrency_limit;
        self
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

        // Set up a listener for shutdown notifications
        let mut shutdown_listener = PgListener::connect_with(&self.queue.pool).await?;
        shutdown_listener.listen(SHUTDOWN_CHANNEL).await?;

        let concurrency_limit = Arc::new(Semaphore::new(self.concurrency_limit));
        let mut processing_tasks = JoinSet::new();

        loop {
            tokio::select! {
                notify_shutdown = shutdown_listener.recv() => {
                    if let Err(err) = notify_shutdown {
                        tracing::error!(%err, "Postgres notification error");
                        continue;
                    }

                    self.queue_shutdown.store(true, Ordering::SeqCst);

                    let task_timeout = self.task.timeout();

                    tracing::info!(
                        task.timeout = ?task_timeout,
                        "Waiting for all processing tasks or timeout"
                    );

                    // Try to join all the processing tasks before the task timeout.
                    let shutdown_result = tokio::time::timeout(
                        task_timeout.try_into()?,
                        async {
                            while let Some(res) = processing_tasks.join_next().await {
                                if let Err(err) = res {
                                    tracing::error!(%err, "A processing task failed during shutdown");
                                }
                            }
                        }
                    ).await;

                    match shutdown_result {
                        Ok(_) => {
                            tracing::debug!("All processing tasks completed gracefully");
                        },
                        Err(_) => {
                            let remaining_tasks = processing_tasks.len();
                            tracing::warn!(remaining_tasks, "Reached task timeout before all tasks completed");
                        },
                    }

                    break;
                },

                _ = interval.tick() => {
                    if self.queue_shutdown.load(Ordering::SeqCst) {
                        tracing::info!("Queue is shutdown so no new tasks will be processed");
                        break;
                    }

                    let permit = concurrency_limit.clone().acquire_owned().await.expect("Concurrency limit semaphore should be open");
                    processing_tasks.spawn({
                        // TODO: Rather than clone the worker, we could have a separate type that
                        // owns task processing.
                        let worker = self.clone();

                        async move {
                            if let Err(err) = worker.process_next_task().await {
                                tracing::error!(%err, "Error processing next task");
                            }
                            drop(permit);
                        }
                    });
                }
            }
        }

        Ok(())
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

            let execute_tx = tx.begin().await?;
            tokio::select! {
                result = self.task.execute(execute_tx, input) => {
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
    use std::{sync::Arc, time::Duration as StdDuration};

    use sqlx::{PgPool, Postgres, Transaction};
    use tokio::sync::Mutex;

    use super::*;
    use crate::{
        queue::graceful_shutdown,
        task::{Result as TaskResult, State as TaskState},
    };

    struct TestTask;

    impl Task for TestTask {
        type Input = ();
        type Output = ();

        async fn execute(
            &self,
            _: Transaction<'_, Postgres>,
            _: Self::Input,
        ) -> TaskResult<Self::Output> {
            Ok(())
        }
    }

    #[derive(Clone)]
    struct FailingTask {
        fail_times: Arc<Mutex<u32>>,
    }

    impl Task for FailingTask {
        type Input = ();
        type Output = ();

        async fn execute(
            &self,
            _: Transaction<'_, Postgres>,
            _: Self::Input,
        ) -> TaskResult<Self::Output> {
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

    #[sqlx::test]
    async fn test_graceful_shutdown(pool: PgPool) -> sqlx::Result<(), Error> {
        let queue = Queue::builder()
            .name("test_queue")
            .pool(pool.clone())
            .build()
            .await?;

        #[derive(Debug, Clone)]
        struct LongRunningTask;

        impl Task for LongRunningTask {
            type Input = ();
            type Output = ();

            async fn execute(
                &self,
                _: Transaction<'_, Postgres>,
                _: Self::Input,
            ) -> TaskResult<Self::Output> {
                tokio::time::sleep(StdDuration::from_secs(1)).await;
                Ok(())
            }
        }

        let task = LongRunningTask;

        // Enqueue some tasks
        for _ in 0..5 {
            queue.enqueue(&pool, &task, ()).await?;
        }

        // Start workers
        let worker = Worker::new(queue.clone(), task);
        for _ in 0..2 {
            let worker = worker.clone();
            tokio::spawn(async move { worker.run().await });
        }

        let pending = sqlx::query_scalar!(
            r#"
            select count(*)
            from underway.task
            where state = $1
            "#,
            TaskState::Pending as _
        )
        .fetch_one(&pool)
        .await?;
        assert_eq!(pending, Some(5));

        // Wait briefly to ensure workers are listening
        tokio::time::sleep(StdDuration::from_secs(2)).await;

        // Initiate graceful shutdown
        graceful_shutdown(&pool).await?;

        // Wait for tasks to be done
        tokio::time::sleep(StdDuration::from_secs(5)).await;

        let succeeded = sqlx::query_scalar!(
            r#"
            select count(*)
            from underway.task
            where state = $1
            "#,
            TaskState::Succeeded as _
        )
        .fetch_one(&pool)
        .await?;
        assert_eq!(succeeded, Some(5));

        // New tasks shouldn't be processed
        queue.enqueue(&pool, &LongRunningTask, ()).await?;

        // Wait to ensure a worker would have seen the new task if one were processing
        tokio::time::sleep(StdDuration::from_secs(5)).await;

        let succeeded = sqlx::query_scalar!(
            r#"
            select count(*)
            from underway.task
            where state = $1
            "#,
            TaskState::Succeeded as _
        )
        .fetch_one(&pool)
        .await?;

        // Succeeded count remains the same since workers have been shutdown
        assert_eq!(succeeded, Some(5));

        Ok(())
    }
}
