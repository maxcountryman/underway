//! Jobs are a higher-level abstraction over the [`Task`] trait.
//!
//! # Defining jobs
//!
//! A builder is provided allowing applications to define the configuration of
//! their jobs. Minimally an execute method must be provided. Reasonable
//! defaults are provided for other configuration and these may be defined with
//! their respective builder methods.
//!
//! ```rust
//! # use sqlx::PgPool;
//! # use underway::Queue;
//! use serde::{Deserialize, Serialize};
//! use underway::Job;
//!
//! # use tokio::runtime::Runtime;
//! # fn main() {
//! # let rt = Runtime::new().unwrap();
//! # rt.block_on(async {
//! # let pool = PgPool::connect("postgres://user:password@localhost/database").await?;
//! # let queue = Queue::builder()
//! #    .name("example_queue")
//! #    .pool(pool)
//! #    .build()
//! #    .await?;
//! # /*
//! let queue = { /* The queue we've defined for our job */ };
//! # */
//! #[derive(Clone, Serialize, Deserialize)]
//! struct Message {
//!     content: String,
//! }
//! let job = Job::builder()
//!     .queue(queue)
//!     .execute(|Message { content }| async move {
//!         println!("Received: {content}");
//!         Ok(())
//!     })
//!     .build();
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! # });
//! # }
//! ```
use std::{future::Future, marker::PhantomData, pin::Pin, sync::Arc};

use builder_states::{ExecutorSet, Initial, QueueSet};
use jiff::Span;
use serde::{de::DeserializeOwned, Serialize};
use sqlx::PgExecutor;

use crate::{
    queue::{Error as QueueError, Queue, ZonedSchedule},
    task::{Error as TaskError, Id as TaskId, Result as TaskResult, RetryPolicy, Task},
    worker::{Result as WorkerResult, Worker},
};

type JobInput<I> = <Job<I> as Task>::Input;

type ExecuteFn<I> =
    Arc<dyn Fn(I) -> Pin<Box<dyn Future<Output = TaskResult> + Send>> + Send + Sync>;

type Result<T = ()> = std::result::Result<T, Error>;

/// Job errors.
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
}

/// An ergnomic implementation of the `Task` trait.
#[derive(Clone)]
pub struct Job<I>
where
    Self: Task,
    I: Clone,
{
    pub(crate) queue: Queue<Self>,
    execute_fn: ExecuteFn<I>,
    retry_policy: RetryPolicy,
    timeout: Span,
    ttl: Span,
    delay: Span,
    concurrency_key: Option<String>,
    priority: i32,
}

impl<I> Job<I>
where
    I: Clone + DeserializeOwned + Serialize + Send + 'static,
    Self: Task,
{
    /// Create a new job builder.
    pub fn builder() -> JobBuilder<I, Initial> {
        JobBuilder::default()
    }

    /// Enqueue the job using a connection from the queue's pool.
    pub async fn enqueue(&self, input: JobInput<I>) -> Result<TaskId> {
        let mut conn = self.queue.pool.acquire().await?;
        self.enqueue_using(&mut *conn, input).await
    }

    /// Enqueue the job using the provided executor.
    ///
    /// This allows jobs to be enqueued using the same transaction as an
    /// application may already be using in a given context.
    pub async fn enqueue_using<'a, E>(&self, executor: E, input: JobInput<I>) -> Result<TaskId>
    where
        E: PgExecutor<'a>,
    {
        let id = self.queue.enqueue(executor, self, input).await?;

        Ok(id)
    }

    /// Schedule the job using a connection from the queue's pool.
    pub async fn schedule(&self, zoned_schedule: ZonedSchedule, input: JobInput<I>) -> Result {
        let mut conn = self.queue.pool.acquire().await?;
        self.schedule_using(&mut *conn, zoned_schedule, input).await
    }

    /// Schedule the job using the provided executor.
    ///
    /// This allows jobs to be scheduled using the same transaction as an
    /// application may already be using in a given context.
    pub async fn schedule_using<'a, E>(
        &self,
        executor: E,
        zoned_schedule: ZonedSchedule,
        input: JobInput<I>,
    ) -> Result
    where
        E: PgExecutor<'a>,
    {
        self.queue.schedule(executor, zoned_schedule, input).await?;

        Ok(())
    }

    /// Marks the given task as cancelled using a connection from the queue's
    /// pool.
    pub async fn cancel(self, task_id: TaskId) -> Result {
        let mut conn = self.queue.pool.acquire().await?;
        self.cancel_using(&mut *conn, task_id).await
    }

    /// Marks the given task a cancelled using the provided executor.
    pub async fn cancel_using<'a, E>(self, executor: E, task_id: TaskId) -> Result
    where
        E: PgExecutor<'a>,
    {
        self.queue.mark_task_cancelled(executor, task_id).await?;

        Ok(())
    }

    /// Constructs a worker which then immediately runs task processing.
    pub async fn run(&self) -> WorkerResult {
        let worker = Worker::from(self);
        worker.run().await
    }

    ///Contructs a worker which then immediately runs schedule processing.
    pub async fn run_scheduler(&self) -> WorkerResult {
        let worker = Worker::from(self);
        worker.run_scheduler().await
    }
}

mod builder_states {
    use super::{DeserializeOwned, ExecuteFn, Job, Queue, Serialize};

    pub struct Initial;

    pub struct QueueSet<I>
    where
        I: Clone + DeserializeOwned + Serialize + Send + 'static,
    {
        pub queue: Queue<Job<I>>,
    }

    pub struct ExecutorSet<I>
    where
        I: Clone + DeserializeOwned + Serialize + Send + 'static,
    {
        pub queue: Queue<Job<I>>,
        pub execute_fn: ExecuteFn<I>,
    }
}

/// A builder of [`Job`].
#[derive(Debug)]
pub struct JobBuilder<I, S = Initial>
where
    I: Clone + DeserializeOwned + Serialize + Send + 'static,
{
    state: S,
    retry_policy: RetryPolicy,
    timeout: Span,
    ttl: Span,
    delay: Span,
    concurrency_key: Option<String>,
    priority: i32,
    _marker: PhantomData<I>,
}

impl<I, S> JobBuilder<I, S>
where
    I: Clone + DeserializeOwned + Serialize + Send + 'static,
{
    /// Sets the job's retry policy.
    pub fn retry_policy(mut self, retry_policy: RetryPolicy) -> Self {
        self.retry_policy = retry_policy;
        self
    }

    /// Sets the job's timeout.
    pub fn timeout(mut self, timeout: Span) -> Self {
        self.timeout = timeout;
        self
    }

    /// Sets the job's time-to-live (TTL).
    pub fn ttl(mut self, ttl: Span) -> Self {
        self.ttl = ttl;
        self
    }

    /// Sets the job's delay.
    ///
    /// The job will not be processed before this delay has elapsed.
    pub fn delay(mut self, delay: Span) -> Self {
        self.delay = delay;
        self
    }

    /// Sets the job's concurrency key.
    ///
    /// Concurrency keys ensure that only one job of the given key may be
    /// processed at a time.
    pub fn concurrency_key(mut self, concurrency_key: impl Into<String>) -> Self {
        self.concurrency_key = Some(concurrency_key.into());
        self
    }

    /// Sets the job's priority.
    ///
    /// Higher priorities are processed first.
    pub fn priority(mut self, priority: i32) -> Self {
        self.priority = priority;
        self
    }
}

impl<I> JobBuilder<I, Initial>
where
    I: Clone + DeserializeOwned + Serialize + Send + 'static,
{
    /// Create a job builder.
    pub fn new() -> JobBuilder<I, Initial> {
        JobBuilder::<I, _> {
            state: Initial,
            retry_policy: RetryPolicy::default(),
            timeout: Span::new().minutes(15),
            ttl: Span::new().days(14),
            delay: Span::new(),
            concurrency_key: None,
            priority: 0,
            _marker: PhantomData,
        }
    }

    /// Set the queue name.
    pub fn queue(self, queue: Queue<Job<I>>) -> JobBuilder<I, QueueSet<I>> {
        JobBuilder {
            state: QueueSet { queue },
            retry_policy: self.retry_policy,
            timeout: self.timeout,
            ttl: self.ttl,
            delay: self.delay,
            concurrency_key: self.concurrency_key,
            priority: self.priority,
            _marker: PhantomData,
        }
    }
}

impl<I> Default for JobBuilder<I, Initial>
where
    I: Clone + DeserializeOwned + Serialize + Send + 'static,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<I> JobBuilder<I, QueueSet<I>>
where
    I: Clone + DeserializeOwned + Serialize + Send + 'static,
{
    /// Set's the job's execute method.
    pub fn execute<F, Fut>(self, f: F) -> JobBuilder<I, ExecutorSet<I>>
    where
        F: Fn(I) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = TaskResult> + Send + 'static,
    {
        JobBuilder {
            state: ExecutorSet {
                queue: self.state.queue,
                execute_fn: Arc::new(move |input: I| {
                    let fut = f(input);
                    Box::pin(fut)
                }),
            },
            retry_policy: self.retry_policy,
            timeout: self.timeout,
            ttl: self.ttl,
            delay: self.delay,
            concurrency_key: self.concurrency_key,
            priority: self.priority,
            _marker: PhantomData,
        }
    }
}

impl<I> JobBuilder<I, ExecutorSet<I>>
where
    I: Clone + DeserializeOwned + Serialize + Send + 'static,
{
    /// Returns a configured [`Job`].
    pub fn build(self) -> Job<I> {
        let ExecutorSet { queue, execute_fn } = self.state;
        Job {
            queue,
            execute_fn,
            retry_policy: self.retry_policy,
            timeout: self.timeout,
            ttl: self.ttl,
            delay: self.delay,
            concurrency_key: self.concurrency_key,
            priority: self.priority,
        }
    }
}

impl<I> Task for Job<I>
where
    I: Clone + DeserializeOwned + Serialize + Send + 'static,
{
    type Input = I;

    async fn execute(&self, input: Self::Input) -> TaskResult {
        (self.execute_fn)(input).await
    }

    fn retry_policy(&self) -> RetryPolicy {
        self.retry_policy
    }

    fn timeout(&self) -> Span {
        self.timeout
    }

    fn ttl(&self) -> Span {
        self.ttl
    }

    fn delay(&self) -> Span {
        self.delay
    }

    fn concurrency_key(&self) -> Option<String> {
        self.concurrency_key.clone()
    }

    fn priority(&self) -> i32 {
        self.priority
    }
}
