//! Workers interface with queues to execute tasks.
//!
//! Until a worker is run, tasks remain on the queue, waiting to be processed.
//! Workers complement queues by providing the mechanism to process tasks, first
//! by finding an available task, deserializing its input, and then passing this
//! input to an invocation of its execute method.
//!
//! As a task is being processed, the worker will assign various state
//! transitions to the task. Eventually the task will either succeed or fail.
//!
//! Also note that workers retry failed tasks in accordance with their
//! configured retry policies. When a task has remaining retries, it will be
//! re-enqueued in a "pending" state.
//!
//! To ensure that only a single worker can process a task at a time, a
//! [transaction-level advisory lock][advisory-locks] is held over either the
//! task ID or concurrency key, if one is provided. In the case of the latter,
//! this ensures a serial processing property of the task.
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
//! let worker = Worker::new(queue.into(), task);
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
//! # let worker = Worker::new(queue.into(), task);
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
//!
//! [advisory-locks]: https://www.postgresql.org/docs/current/explicit-locking.html#ADVISORY-LOCKS

use std::{sync::Arc, time::Duration};

use jiff::{Span, ToSpan};
use serde::Deserialize;
use sqlx::{
    postgres::{types::PgInterval, PgNotification},
    Acquire, PgConnection,
};
use tokio::{sync::Semaphore, task::JoinSet};
use tokio_util::sync::CancellationToken;
use tracing::instrument;

use crate::{
    queue::{
        connect_listeners_with_retry, shutdown_channel, Error as QueueError, InProgressTask, Queue,
    },
    task::{Error as TaskError, RetryCount, RetryPolicy, Task, TaskId},
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
    queue: Arc<Queue<T>>,
    task: Arc<T>,

    // Limits the number of concurrent `Task::execute` invocations this worker will be allowed.
    concurrency_limit: usize,

    // Backoff policy for reconnecting when PostgreSQL connection is lost.
    reconnect_backoff: RetryPolicy,

    // When this token is cancelled the queue has been shutdown.
    shutdown_token: CancellationToken,
}

impl<T: Task> Clone for Worker<T> {
    fn clone(&self) -> Self {
        Self {
            queue: Arc::clone(&self.queue),
            task: Arc::clone(&self.task),
            concurrency_limit: self.concurrency_limit,
            reconnect_backoff: self.reconnect_backoff,
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
    /// Worker::new(queue.into(), task);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # });
    /// # }
    /// ```
    pub fn new(queue: Arc<Queue<T>>, task: T) -> Self {
        let task = Arc::new(task);
        Self {
            queue,
            task,
            concurrency_limit: num_cpus::get(),
            reconnect_backoff: RetryPolicy::default(),
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
    /// # let mut worker = Worker::new(queue.into(), task);
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
    /// # let mut worker = Worker::new(queue.into(), task);
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

    /// Sets the backoff policy for PostgreSQL reconnection attempts.
    ///
    /// This policy controls the exponential backoff behavior when the
    /// worker's PostgreSQL listener connection is lost and needs to
    /// reconnect. This helps avoid overwhelming the database during
    /// connection issues.
    ///
    /// Defaults to 1 second initial interval, 60 second max interval, 2.0
    /// coefficient and 0.5 jitter_factor.
    ///
    /// **Note**: The `max_attempts` field is ignored for reconnection - the
    /// worker will keep retrying until successful or until shutdown.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use sqlx::{PgPool, Transaction, Postgres};
    /// # use underway::{Task, task::Result as TaskResult, Queue, Worker};
    /// use underway::task::RetryPolicy;
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
    /// # let mut worker = Worker::new(queue.into(), task);
    /// # /*
    /// let mut worker = { /* A `Worker`. */ };
    /// # */
    /// #
    ///
    /// // Set a custom backoff policy for reconnection.
    /// let backoff = RetryPolicy::builder()
    ///     .initial_interval_ms(2_000) // 2 seconds
    ///     .max_interval_ms(60_000) // 1 minute
    ///     .backoff_coefficient(2.5)
    ///     .jitter_factor(0.5)
    ///     .build();
    /// worker.set_reconnect_backoff(backoff);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # });
    /// # }
    /// ```
    pub fn set_reconnect_backoff(&mut self, backoff: RetryPolicy) {
        self.reconnect_backoff = backoff;
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
    /// # let worker = Worker::new(queue.into(), task);
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
    /// # let worker = Worker::new(queue.into(), task);
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
    /// # let worker = Worker::new(queue.into(), task);
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
    #[instrument(skip(self), fields(queue.name = self.queue.name), err)]
    pub async fn run_every(&self, period: Span) -> Result {
        let mut polling_interval = tokio::time::interval(period.try_into()?);
        let chan = shutdown_channel();
        let concurrency_limit = Arc::new(Semaphore::new(self.concurrency_limit));
        let mut processing_tasks = JoinSet::new();

        // Outer loop: handle reconnection logic
        'reconnect: loop {
            // Connect to PostgreSQL listeners with retry logic
            let mut listeners = connect_listeners_with_retry(
                &self.queue.pool,
                &[chan, "task_change"],
                &self.reconnect_backoff,
            )
            .await?;

            let mut shutdown_listener = listeners.remove(0);
            let mut task_change_listener = listeners.remove(0);

            tracing::info!("PostgreSQL listeners connected successfully");

            // Inner loop: handle normal events
            loop {
                tokio::select! {
                    // Reap completed processing tasks so JoinSet doesn't grow unbounded.
                    Some(completed) = processing_tasks.join_next(), if !processing_tasks.is_empty() => {
                        if let Err(err) = completed {
                            tracing::error!(%err, "A processing task failed");
                        }

                        reap_completed(&mut processing_tasks);
                    }

                    notify_shutdown = shutdown_listener.recv() => {
                        match notify_shutdown {
                            Ok(_) => {
                                self.shutdown_token.cancel();
                            },
                            Err(err) => {
                                tracing::warn!(%err, "Shutdown listener connection lost, reconnecting");
                                continue 'reconnect;
                            }
                        }
                    }

                    _ = self.shutdown_token.cancelled() => {
                        self.handle_shutdown(&mut processing_tasks).await?;
                        return Ok(());
                    }

                    // Listen for new pending tasks
                    notify_task_change = task_change_listener.recv() => {
                        match notify_task_change {
                            Ok(task_change) => {
                                self.handle_task_change(task_change, concurrency_limit.clone(), &mut processing_tasks).await?;
                            },
                            Err(err) => {
                                tracing::warn!(%err, "Task change listener connection lost, reconnecting");
                                continue 'reconnect;
                            }
                        }
                    }

                    // Pending task polling fallback
                    _ = polling_interval.tick() => {
                        self.trigger_task_processing(concurrency_limit.clone(), &mut processing_tasks).await;
                    }
                }
            }
        }
    }

    async fn handle_shutdown(&self, processing_tasks: &mut JoinSet<()>) -> Result {
        let task_timeout = self.task.timeout();

        tracing::debug!(
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

    #[instrument(
        skip_all,
        fields(
            processing = processing_tasks.len(),
            permits = concurrency_limit.available_permits()
        )
    )]
    async fn trigger_task_processing(
        &self,
        concurrency_limit: Arc<Semaphore>,
        processing_tasks: &mut JoinSet<()>,
    ) {
        let Ok(permit) = concurrency_limit.try_acquire_owned() else {
            tracing::trace!("Concurrency limit reached");
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
                            tracing::trace!("No task found");
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
    /// dequeued input.
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
    /// # let worker = Worker::new(queue.into(), task);
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
        let Some(in_progress_task) = self.queue.dequeue().await? else {
            return Ok(None);
        };

        let task_id = in_progress_task.id;
        tracing::Span::current().record("task.id", task_id.as_hyphenated().to_string());

        // Transaction scoped to the task execution.
        let mut tx = self.queue.pool.begin().await?;

        // Acquire an advisory lock on either the concurrency key or the task ID.
        if !in_progress_task.try_acquire_lock(&mut tx).await? {
            return Ok(None);
        }

        let input: T::Input = serde_json::from_value(in_progress_task.input.clone())?;

        let timeout = pg_interval_to_span(&in_progress_task.timeout)
            .try_into()
            .expect("Task timeout should be compatible with std::time");

        let heartbeat = pg_interval_to_span(&in_progress_task.heartbeat)
            .try_into()
            .expect("Task heartbeat should be compatible with std::time");

        // Spawn a task to send heartbeats alongside task processing.
        let heartbeat_task = tokio::spawn({
            let pool = self.queue.pool.clone();
            let in_progress_task = in_progress_task.clone();
            async move {
                let mut heartbeat_interval = tokio::time::interval(heartbeat);
                heartbeat_interval.tick().await;
                loop {
                    tracing::trace!("Recording task heartbeat");

                    // N.B.: Heartbeats are recorded outside of the transaction to ensure
                    // visibility.
                    if let Err(err) = in_progress_task.record_heartbeat(&pool).await {
                        tracing::error!(err = %err, "Failed to record task heartbeat");
                    };

                    heartbeat_interval.tick().await;
                }
            }
        });

        // Execute savepoint, available directly to the task execute method.
        let execute_tx = tx.begin().await?;

        tokio::select! {
            result = self.task.execute(execute_tx, input) => {
                match result {
                    Ok(_) => {
                        in_progress_task.mark_succeeded(&mut tx).await?;
                    }

                    Err(ref error) => {
                        let retry_policy = &in_progress_task.retry_policy;
                        self.handle_task_error(&mut tx, &in_progress_task, retry_policy, error)
                            .await?;
                    }
                }
            }

            // Handle timed-out task execution.
            _ = tokio::time::sleep(timeout) => {
                tracing::error!("Task execution timed out");
                let retry_policy = &in_progress_task.retry_policy;
                self.handle_task_timeout(&mut tx, &in_progress_task, retry_policy, timeout).await?;
            }
        }

        heartbeat_task.abort();

        tx.commit().await?;

        Ok(Some(task_id))
    }

    async fn handle_task_error(
        &self,
        conn: &mut PgConnection,
        in_progress_task: &InProgressTask,
        retry_policy: &RetryPolicy,
        error: &TaskError,
    ) -> Result {
        tracing::error!(err = %error, "Task execution encountered an error");

        if let TaskError::Suspended(reason) = error {
            tracing::info!(%reason, "Task execution suspended");
            in_progress_task.mark_waiting(conn).await?;
            return Ok(());
        }

        // Short-circuit on fatal errors.
        if matches!(error, TaskError::Fatal(_)) {
            return self.finalize_task_failure(conn, in_progress_task).await;
        }

        in_progress_task.record_failure(conn, error).await?;

        let retry_count = in_progress_task.retry_count(&mut *conn).await?;
        if retry_count < retry_policy.max_attempts {
            self.schedule_task_retry(conn, in_progress_task, retry_count, retry_policy)
                .await?;
        } else {
            self.finalize_task_failure(conn, in_progress_task).await?;
        }

        Ok(())
    }

    async fn handle_task_timeout(
        &self,
        conn: &mut PgConnection,
        in_progress_task: &InProgressTask,
        retry_policy: &RetryPolicy,
        timeout: Duration,
    ) -> Result {
        tracing::error!("Task execution timed out");

        let error = &TaskError::TimedOut(timeout.try_into()?);
        in_progress_task.record_failure(&mut *conn, error).await?;

        // Poll count after updating task failure.
        let retry_count = in_progress_task.retry_count(&mut *conn).await?;
        if retry_count < retry_policy.max_attempts {
            self.schedule_task_retry(conn, in_progress_task, retry_count, retry_policy)
                .await?;
        } else {
            self.finalize_task_failure(conn, in_progress_task).await?;
        }

        Ok(())
    }

    async fn schedule_task_retry(
        &self,
        conn: &mut PgConnection,
        in_progress_task: &InProgressTask,
        retry_count: RetryCount,
        retry_policy: &RetryPolicy,
    ) -> Result {
        tracing::debug!("Retry policy available, scheduling retry");

        let delay = retry_policy.calculate_delay(retry_count);
        in_progress_task.retry_after(&mut *conn, delay).await?;

        Ok(())
    }

    async fn finalize_task_failure(
        &self,
        conn: &mut PgConnection,
        in_progress_task: &InProgressTask,
    ) -> Result {
        tracing::debug!("Retry policy exhausted, handling failed task");

        in_progress_task.mark_failed(&mut *conn).await?;

        if let Some(dlq_name) = &self.queue.dlq_name {
            self.queue
                .move_task_to_dlq(&mut *conn, in_progress_task.id, dlq_name)
                .await?;
        }

        Ok(())
    }
}

fn reap_completed(processing_tasks: &mut JoinSet<()>) {
    while let Some(res) = processing_tasks.try_join_next() {
        if let Err(err) = res {
            tracing::error!(%err, "A processing task failed");
        }
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
    use std::{
        sync::Arc,
        time::{Duration as StdDuration, Instant},
    };

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
        let task_id = queue.enqueue(&pool, &task, &()).await?;

        // Process the task.
        let queue = Arc::new(queue);
        let worker = Worker::new(queue.clone(), task);
        let processed_task_id = worker
            .process_next_task()
            .await?
            .expect("A task should be processed");
        assert_eq!(task_id, processed_task_id);

        // Check that the task was processed successfully.
        let task_row = sqlx::query!(
            r#"select state as "state: TaskState" from underway.task where id = $1"#,
            task_id as TaskId
        )
        .fetch_one(&pool)
        .await?;
        assert_eq!(task_row.state, TaskState::Succeeded);

        // Ensure the task is no longer available on the queue.
        assert!(queue.dequeue().await?.is_none());

        Ok(())
    }

    #[sqlx::test]
    async fn process_retries(pool: PgPool) -> sqlx::Result<(), Error> {
        let queue = Queue::builder()
            .name("process_retries")
            .pool(pool.clone())
            .build()
            .await?;

        let fail_times = Arc::new(Mutex::new(2));
        let task = FailingTask {
            fail_times: fail_times.clone(),
        };
        let queue = Arc::new(queue);
        let worker = Worker::new(queue.clone(), task.clone());

        // Enqueue the task
        let task_id = queue.enqueue(&pool, &worker.task, &()).await?;

        // Process the task multiple times to simulate retries
        for retries in 0..3 {
            let start = Instant::now();
            let timeout = StdDuration::from_secs(10);

            loop {
                if let Some(processed_task_id) = worker.process_next_task().await? {
                    assert_eq!(task_id, processed_task_id);
                    break;
                }

                if start.elapsed() > timeout {
                    panic!("Timed out waiting for retry {retries}");
                }

                tokio::time::sleep(StdDuration::from_millis(100)).await;
            }
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
            task_id as TaskId
        )
        .fetch_one(&pool)
        .await?;

        assert_eq!(dequeued_task.state, TaskState::Succeeded);

        Ok(())
    }

    #[sqlx::test]
    async fn fenced_attempts_ignore_stale_workers(pool: PgPool) -> sqlx::Result<(), Error> {
        struct FenceTask;

        impl Task for FenceTask {
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

        let queue = Queue::builder()
            .name("fenced_attempts_ignore_stale_workers")
            .pool(pool.clone())
            .build()
            .await?;

        let task = FenceTask;
        let task_id = queue.enqueue(&pool, &task, &()).await?;

        let in_progress_task = queue.dequeue().await?.expect("A task should be dequeued");
        assert_eq!(in_progress_task.attempt_number, 1);

        sqlx::query!(
            r#"
            update underway.task
            set last_heartbeat_at = now() - interval '1 hour'
            where id = $1
              and task_queue_name = $2
            "#,
            task_id as TaskId,
            queue.name
        )
        .execute(&pool)
        .await?;

        let reclaimed_task = queue
            .dequeue()
            .await?
            .expect("A reclaimed task should be dequeued");
        assert!(reclaimed_task.attempt_number > in_progress_task.attempt_number);

        let mut stale_tx = pool.begin().await?;
        let stale_result = in_progress_task.mark_succeeded(&mut stale_tx).await;
        stale_tx.rollback().await?;
        assert!(matches!(
            stale_result,
            Err(crate::queue::Error::TaskNotFound(_))
        ));

        let mut reclaimed_tx = pool.begin().await?;
        reclaimed_task.mark_succeeded(&mut reclaimed_tx).await?;
        reclaimed_tx.commit().await?;

        let task_state = sqlx::query_scalar!(
            r#"
            select state as "state: TaskState"
            from underway.task
            where id = $1
              and task_queue_name = $2
            "#,
            task_id as TaskId,
            queue.name
        )
        .fetch_one(&pool)
        .await?;

        assert_eq!(task_state, TaskState::Succeeded);

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
        let queue = Arc::new(queue);
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

    #[sqlx::test]
    async fn heartbeat_stops_after_task_completion(pool: PgPool) -> sqlx::Result<(), Error> {
        // Define a task that sleeps for a short duration
        struct SleepTask;

        impl Task for SleepTask {
            type Input = ();
            type Output = ();

            async fn execute(
                &self,
                _: Transaction<'_, Postgres>,
                _: Self::Input,
            ) -> TaskResult<Self::Output> {
                // Simulate work by sleeping
                tokio::time::sleep(StdDuration::from_secs(5)).await;
                Ok(())
            }

            fn heartbeat(&self) -> Span {
                1.second()
            }
        }

        let queue = Queue::builder()
            .name("heartbeat_stops_after_task_completion")
            .pool(pool.clone())
            .build()
            .await?;

        // Enqueue the sleep task
        let task_id = queue.enqueue(&pool, &SleepTask, &()).await?;

        // Start the worker
        let queue = Arc::new(queue);
        let worker = Worker::new(queue.clone(), SleepTask);
        let worker_handle = tokio::spawn(async move { worker.run_every(1.second()).await });

        // Ensure the worker has time to dequeue the task
        tokio::time::sleep(StdDuration::from_secs(1)).await;

        // Monitor last_heartbeat_at during task execution
        let mut last_heartbeat_at = None;
        let start_time = Instant::now();

        while start_time.elapsed() < StdDuration::from_secs(6) {
            // Fetch last_heartbeat_at from the database
            let task_row = sqlx::query!(
                r#"
                select last_heartbeat_at as "last_heartbeat_at: i64"
                from underway.task
                where id = $1
                  and task_queue_name = $2
                "#,
                task_id as TaskId,
                queue.name
            )
            .fetch_one(&pool)
            .await?;

            let current_heartbeat = task_row
                .last_heartbeat_at
                .expect("A heartbeat should be set");

            if let Some(prev_heartbeat) = last_heartbeat_at {
                // Ensure last_heartbeat_at is being updated
                assert!(current_heartbeat > prev_heartbeat);
            }
            last_heartbeat_at = Some(current_heartbeat);

            // Sleep before the next check
            tokio::time::sleep(StdDuration::from_secs(1)).await;
        }

        // Wait for the task to complete
        worker_handle.abort(); // Ensure the worker task is stopped

        // Record the last heartbeat timestamp after task completion
        let final_task_row = sqlx::query!(
            r#"
            select last_heartbeat_at as "last_heartbeat_at: i64"
            from underway.task
            where id = $1
            "#,
            task_id as TaskId
        )
        .fetch_one(&pool)
        .await?;

        // Wait for a duration longer than the heartbeat interval
        tokio::time::sleep(StdDuration::from_secs(2)).await;

        // Check if last_heartbeat_at has not been updated after task completion
        let post_completion_task_row = sqlx::query!(
            r#"
            select last_heartbeat_at as "last_heartbeat_at: i64"
            from underway.task
            where id = $1
            "#,
            task_id as TaskId
        )
        .fetch_one(&pool)
        .await?;

        // Assert that last_heartbeat_at did not change after task completion
        assert_eq!(
            final_task_row.last_heartbeat_at,
            post_completion_task_row.last_heartbeat_at
        );

        // Confirm that the task has succeeded
        let task_state = sqlx::query_scalar!(
            r#"
            select state as "state: TaskState"
            from underway.task
            where id = $1
            "#,
            task_id as TaskId
        )
        .fetch_one(&pool)
        .await?;

        assert_eq!(task_state, TaskState::Succeeded);

        Ok(())
    }

    #[tokio::test]
    async fn reap_completed_drains_joinset() {
        let mut processing_tasks = JoinSet::new();

        for _ in 0..3 {
            processing_tasks.spawn(async {
                tokio::time::sleep(StdDuration::from_millis(10)).await;
            });
        }

        tokio::time::sleep(StdDuration::from_millis(25)).await;

        assert!(!processing_tasks.is_empty());

        reap_completed(&mut processing_tasks);

        assert!(processing_tasks.is_empty());
    }
}
