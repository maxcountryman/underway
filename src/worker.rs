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

use std::sync::Arc;

use jiff::{Span, ToSpan};
use serde::Deserialize;
use sqlx::{
    postgres::{types::PgInterval, PgListener, PgNotification},
    Acquire, PgConnection,
};
use tokio::{sync::Semaphore, task::JoinSet};
use tokio_util::sync::CancellationToken;
use tracing::instrument;

use crate::{
    queue::{acquire_advisory_xact_lock, Error as QueueError, Queue, SHUTDOWN_CHANNEL},
    task::{DequeuedTask, Error as TaskError, RetryCount, RetryPolicy, Task, TaskId},
};
pub(crate) type Result<T = ()> = std::result::Result<T, Error>;

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

/// A worker that's generic over the task it processes.
#[derive(Debug)]
pub struct Worker<T: Task> {
    queue: Queue<T>,
    task: Arc<T>,

    // Limits the number of concurrent `Task::execute` invocations this worker will be allowed.
    concurrency_limit: usize,

    // When this token is cancelled the queue has been shutdown.
    shutdown_token: CancellationToken,
}

impl<T: Task> Clone for Worker<T> {
    fn clone(&self) -> Self {
        Self {
            queue: self.queue.clone(),
            task: self.task.clone(),
            concurrency_limit: self.concurrency_limit,
            shutdown_token: self.shutdown_token.clone(),
        }
    }
}

impl<T: Task + Sync> Worker<T> {
    /// Creates a new worker with the given queue and task.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use sqlx::{PgPool, Transaction, Postgres};
    /// # use underway::{Task, task::Result as TaskResult, Queue};
    /// use underway::Worker;
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
    /// # let queue = Queue::builder()
    /// #    .name("example")
    /// #    .pool(pool.clone())
    /// #    .build()
    /// #    .await?;
    /// # /*
    /// let queue = { /* A `Queue`. */ };
    /// # */
    /// # let task = ExampleTask;
    /// # /*
    /// let task = { /* An implementer of `Task`. */ };
    /// # */
    /// #
    ///
    /// Worker::new(queue, task);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # });
    /// # }
    /// ```
    pub fn new(queue: Queue<T>, task: T) -> Self {
        Self {
            queue,
            task: Arc::new(task),
            concurrency_limit: num_cpus::get(),
            shutdown_token: CancellationToken::new(),
        }
    }

    /// Sets the concurrency limit for this worker.
    ///
    /// Defaults to CPU count as per [`num_cpus::get`].
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use sqlx::{PgPool, Transaction, Postgres};
    /// # use underway::{Task, task::Result as TaskResult, Queue, Worker};
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
    /// # let queue = Queue::builder()
    /// #    .name("example")
    /// #    .pool(pool.clone())
    /// #    .build()
    /// #    .await?;
    /// # let task = ExampleTask;
    /// # let mut worker = Worker::new(queue, task);
    /// # /*
    /// let mut worker = { /* A `Worker`. */ };
    /// # */
    /// #
    ///
    /// // Set a fixed concurrency limit.
    /// worker.set_concurrency_limit(32);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # });
    /// # }
    /// ```
    pub fn set_concurrency_limit(&mut self, concurrency_limit: usize) {
        self.concurrency_limit = concurrency_limit;
    }

    /// Sets the shutdown token.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use sqlx::{PgPool, Transaction, Postgres};
    /// # use underway::{Task, task::Result as TaskResult, Queue, Worker};
    /// use tokio_util::sync::CancellationToken;
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
    /// # let queue = Queue::builder()
    /// #    .name("example")
    /// #    .pool(pool.clone())
    /// #    .build()
    /// #    .await?;
    /// # let task = ExampleTask;
    /// # let mut worker = Worker::new(queue, task);
    /// # /*
    /// let mut worker = { /* A `Worker`. */ };
    /// # */
    /// #
    ///
    /// // Set a custom cancellation token.
    /// let token = CancellationToken::new();
    /// worker.set_shutdown_token(token);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # });
    /// # }
    /// ```
    pub fn set_shutdown_token(&mut self, shutdown_token: CancellationToken) {
        self.shutdown_token = shutdown_token;
    }

    /// Cancels the shutdown token and begins a graceful shutdown of in-progress
    /// tasks.
    ///
    /// Note that tasks are given until their configured timeout to complete
    /// before the worker will exit.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use sqlx::{PgPool, Transaction, Postgres};
    /// # use underway::{Task, task::Result as TaskResult, Queue, Worker};
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
    /// # let queue = Queue::builder()
    /// #    .name("example")
    /// #    .pool(pool.clone())
    /// #    .build()
    /// #    .await?;
    /// # let task = ExampleTask;
    /// # let worker = Worker::new(queue, task);
    /// # /*
    /// let worker = { /* A `Worker`. */ };
    /// # */
    /// #
    ///
    /// // Begin graceful shutdown.
    /// worker.shutdown();
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # });
    /// # }
    /// ```
    pub fn shutdown(&self) {
        self.shutdown_token.cancel();
    }

    /// Runs the worker, processing tasks as they become available.
    ///
    /// Tasks are processed via a subscription to a Postgres channel and polling
    /// in a loop. A one-minute sleep occurs between polls.
    ///
    /// # Errors
    ///
    /// This function will return an error if:
    /// - It fails to listen on either the shutdown channel or the task change
    ///   channel.
    /// - Task timeouts fails to be converted to std::time::Duration.
    ///
    /// It also has the same error conditions as [`Queue::dequeue`], as this is
    /// used internally.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use sqlx::{PgPool, Transaction, Postgres};
    /// # use underway::{Task, task::Result as TaskResult, Queue, Worker};
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
    /// # let queue = Queue::builder()
    /// #    .name("example")
    /// #    .pool(pool)
    /// #    .build()
    /// #    .await?;
    /// # let task = ExampleTask;
    /// # let worker = Worker::new(queue, task);
    /// # /*
    /// let worker = { /* A `Worker`. */ };
    /// # */
    /// #
    ///
    /// // Run the worker in a separate task.
    /// tokio::spawn(async move { worker.run().await });
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # });
    /// # }
    /// ```
    pub async fn run(&self) -> Result {
        self.run_every(1.minute()).await
    }

    /// Same as `run` but allows for the configuration of the delay between
    /// polls.
    ///
    /// # Errors
    ///
    /// This has the same error conditions as [`Worker::run`].
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use sqlx::{PgPool, Transaction, Postgres};
    /// # use underway::{Task, task::Result as TaskResult, Queue, Worker};
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
    /// # let queue = Queue::builder()
    /// #    .name("example")
    /// #    .pool(pool)
    /// #    .build()
    /// #    .await?;
    /// # let task = ExampleTask;
    /// # let worker = Worker::new(queue, task);
    /// # /*
    /// let worker = { /* A `Worker`. */ };
    /// # */
    /// #
    ///
    /// // Increase the polling interval to every ten seconds.
    /// tokio::spawn(async move { worker.run_every(10.seconds()).await });
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # });
    /// # }
    /// ```
    pub async fn run_every(&self, period: Span) -> Result {
        let mut polling_interval = tokio::time::interval(period.try_into()?);

        // Set up a listener for shutdown notifications
        let mut shutdown_listener = PgListener::connect_with(&self.queue.pool).await?;
        shutdown_listener.listen(SHUTDOWN_CHANNEL).await?;

        // Set up a listener for task change notifications
        let mut task_change_listener = PgListener::connect_with(&self.queue.pool).await?;
        task_change_listener.listen("task_change").await?;

        let concurrency_limit = Arc::new(Semaphore::new(self.concurrency_limit));
        let mut processing_tasks = JoinSet::new();

        loop {
            tokio::select! {
                notify_shutdown = shutdown_listener.recv() => {
                    match notify_shutdown {
                        Ok(_) => {
                            self.shutdown_token.cancel();
                        },

                        Err(err) => {
                            tracing::error!(%err, "Postgres shutdown notification error");
                        }
                    }
                }

                _ = self.shutdown_token.cancelled() => {
                    self.handle_shutdown(&mut processing_tasks).await?;
                    break
                }

                // Listen for new pending tasks.
                notify_task_change = task_change_listener.recv() => {
                    match notify_task_change {
                        Ok(task_change) => self.handle_task_change(task_change, concurrency_limit.clone(), &mut processing_tasks).await?,

                        Err(err) => {
                            tracing::error!(%err, "Postgres task change notification error");
                        }
                    };

                }

                // Pending task polling fallback.
                _ = polling_interval.tick() => {
                    self.trigger_task_processing(concurrency_limit.clone(), &mut processing_tasks).await;
                }
            }
        }

        Ok(())
    }

    async fn handle_shutdown(&self, processing_tasks: &mut JoinSet<()>) -> Result {
        let task_timeout = self.task.timeout();

        tracing::info!(
            task.timeout = ?task_timeout,
            "Waiting for all processing tasks or timeout"
        );

        // Wait for processing tasks to complete or timeout
        let shutdown_result = tokio::time::timeout(task_timeout.try_into()?, async {
            while let Some(res) = processing_tasks.join_next().await {
                if let Err(err) = res {
                    tracing::error!(%err, "A processing task failed during shutdown");
                }
            }
        })
        .await;

        match shutdown_result {
            Ok(_) => {
                tracing::debug!("All processing tasks completed gracefully");
            }
            Err(_) => {
                let remaining_tasks = processing_tasks.len();
                tracing::warn!(
                    remaining_tasks,
                    "Reached task timeout before all tasks completed"
                );
            }
        }

        Ok(())
    }

    async fn handle_task_change(
        &self,
        task_change: PgNotification,
        concurrency_limit: Arc<Semaphore>,
        processing_tasks: &mut JoinSet<()>,
    ) -> Result {
        let payload = task_change.payload();
        let decoded: TaskChange = serde_json::from_str(payload).map_err(|err| {
            tracing::error!(%err, "Invalid task change payload; ignoring");
            err
        })?;

        if decoded.queue_name == self.queue.name {
            self.trigger_task_processing(concurrency_limit, processing_tasks)
                .await;
        }

        Ok(())
    }

    async fn trigger_task_processing(
        &self,
        concurrency_limit: Arc<Semaphore>,
        processing_tasks: &mut JoinSet<()>,
    ) {
        let Ok(permit) = concurrency_limit.try_acquire_owned() else {
            tracing::debug!("Concurrency limit reached");
            return;
        };

        processing_tasks.spawn({
            let worker = self.clone();
            async move {
                while !worker.shutdown_token.is_cancelled() {
                    match worker.process_next_task().await {
                        Err(err) => {
                            tracing::error!(err = %err, "Error processing next task");
                            continue;
                        }
                        Ok(Some(_)) => {
                            // Since we just processed a task, we'll try again in case there's more.
                            continue;
                        }
                        Ok(None) => {
                            // We tried to process a task but found none so we'll stop trying.
                            break;
                        }
                    }
                }
                drop(permit);
            }
        });
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
    ///
    /// # Errors
    ///
    /// This function will return an error if a new transaction cannot be
    /// acquired from the queue pool.
    ///
    /// It also has the same error conditions as [`Queue::dequeue`] as this is
    /// used internally.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use sqlx::{PgPool, Transaction, Postgres};
    /// # use underway::{Task, task::Result as TaskResult, Queue, Worker};
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
    /// # let queue = Queue::builder()
    /// #    .name("example")
    /// #    .pool(pool.clone())
    /// #    .build()
    /// #    .await?;
    /// # let task = ExampleTask;
    /// # let worker = Worker::new(queue, task);
    /// # /*
    /// let worker = { /* A `Worker`. */ };
    /// # */
    /// #
    ///
    /// worker.process_next_task().await?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # });
    /// # }
    /// ```
    #[instrument(
        skip(self),
        fields(
            queue.name = self.queue.name,
            task.id = tracing::field::Empty,
        ),
        err
    )]
    pub async fn process_next_task(&self) -> Result<Option<TaskId>> {
        let mut tx = self.queue.pool.begin().await?;

        let Some(pending_task) = self.queue.dequeue(&mut tx).await? else {
            return Ok(None);
        };

        let task_id = pending_task.id;
        tracing::Span::current().record("task.id", task_id.as_hyphenated().to_string());

        // Ensure that only one worker may process a task of a given concurrency key at
        // a time.
        if let Some(concurrency_key) = &pending_task.concurrency_key {
            acquire_advisory_xact_lock(&mut *tx, concurrency_key).await?;
        }

        let input: T::Input = serde_json::from_value(pending_task.input.clone())?;

        let timeout = pg_interval_to_span(&pending_task.timeout)
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
                        self.handle_task_error(&mut tx, task_id, pending_task, err)
                            .await?;
                    }
                }
            }

            _ = tokio::time::sleep(timeout) => {
                tracing::error!("Task execution timed out");
                self.handle_task_timeout(&mut tx, task_id, pending_task).await?;
            }
        }

        tx.commit().await?;

        Ok(Some(task_id))
    }

    async fn handle_task_error(
        &self,
        conn: &mut PgConnection,
        task_id: TaskId,
        dequeued_task: DequeuedTask,
        err: TaskError,
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

#[derive(Debug, Deserialize)]
struct TaskChange {
    #[serde(rename = "task_queue_name")]
    queue_name: String,
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
        queue.enqueue(&pool, &task, &()).await?;
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
        let task_id = queue.enqueue(&pool, &worker.task, &()).await?;

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
            task_id as _
        )
        .fetch_one(&pool)
        .await?;

        assert_eq!(dequeued_task.state, TaskState::Succeeded);

        Ok(())
    }

    #[sqlx::test]
    async fn gracefully_shutdown(pool: PgPool) -> sqlx::Result<(), Error> {
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

        let queue = Queue::builder()
            .name("gracefully_shutdown")
            .pool(pool.clone())
            .build()
            .await?;

        // Start workers before queuing tasks
        let worker = Worker::new(queue.clone(), LongRunningTask);
        for _ in 0..2 {
            let worker = worker.clone();
            tokio::spawn(async move { worker.run().await });
        }

        // Wait briefly to ensure workers are listening
        tokio::time::sleep(StdDuration::from_secs(1)).await;

        // Enqueue some tasks now that the worker is listening
        for _ in 0..5 {
            queue.enqueue(&pool, &LongRunningTask, &()).await?;
        }

        // Initiate graceful shutdown
        graceful_shutdown(&pool).await?;

        // Wait for tasks to be done
        tokio::time::sleep(StdDuration::from_secs(2)).await;

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
        queue.enqueue(&pool, &LongRunningTask, &()).await?;

        // Wait to ensure a worker would have seen the new task if one were processing
        tokio::time::sleep(StdDuration::from_secs(1)).await;

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
