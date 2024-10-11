//! Jobs are a higher-level abstraction over the [`Task`] trait.
//!
//! This interface is intended to make defining and operating jobs streamlined.
//! While it's possible to implement the `Task` trait directly, it's generally
//! not needed and instead `Job` can be used.
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
//! #
//!
//! // The execute input type.
//! #[derive(Clone, Serialize, Deserialize)]
//! struct Message {
//!     content: String,
//! }
//!
//! // Define the job.
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
//!
//! # Getting work done
//!
//! Of course the point of creating a job is to use it to do something useful
//! for us. In order to do so, we need to enqueue some input onto the job's
//! queue. [`Job::enqueue`] does exactly this:
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
//! #
//!
//! #[derive(Clone, Serialize, Deserialize)]
//! struct Order {
//!     id: usize,
//! }
//!
//! let job = Job::builder()
//!     .queue(queue)
//!     .execute(|Order { id }| async move {
//!         println!("Order ID: {id}");
//!         Ok(())
//!     })
//!     .build();
//!
//! // Enqueue a new message.
//! job.enqueue(Order { id: 42 }).await?;
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! # });
//! # }
//! ```
//!
//! ## Transactional enqueue
//!
//! Sometimes a job should only be enqueued when other conditions are met.
//!
//! For example, perhaps we're implementing user registration and we'd like to
//! send a welcome email upon completing the flow. However, if something goes
//! wrong and we need to reset the flow, we'd like to avoid sending such an
//! email.
//!
//! To accomodate use cases like this, we can make use of
//! [`Job::enqueue_using`], which allows us to specify a transaction. Should the
//! transaction be rolled back, then our job won't be enqueued. (An ID will
//! still be returned by this method, so it's up to our application to recognize
//! when a failure has occurred and ignore any such IDs.)
//!
//! ```rust
//! # use sqlx::PgPool;
//! # use underway::Queue;
//! # use serde::{Deserialize, Serialize};
//! # use underway::Job;
//! # use tokio::runtime::Runtime;
//! # fn main() {
//! # let rt = Runtime::new().unwrap();
//! # rt.block_on(async {
//! # let pool = PgPool::connect("postgres://user:password@localhost/database").await?;
//! # let queue = Queue::builder()
//! #    .name("example_queue")
//! #    .pool(pool.clone())
//! #    .build()
//! #    .await?;
//! #
//! # #[derive(Clone, Serialize, Deserialize)]
//! # struct Order {
//! #     id: usize,
//! # }
//! # let job = Job::builder()
//! #     .queue(queue)
//! #     .execute(|Order { id }| async move {
//! #         println!("Order ID: {id}");
//! #         Ok(())
//! #     })
//! #     .build();
//! let mut tx = pool.begin().await?;
//!
//! # /*
//! /* Some intervening logic... */
//! # */
//! #
//!
//! // Enqueue a new message using a transaction.
//! job.enqueue_using(&mut *tx, Order { id: 42 }).await?;
//!
//! # /*
//! /* ..And more intervening logic. */
//! # */
//! #
//!
//! tx.commit().await?;
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! # });
//! # }
//! ```
//!
//! # Running jobs
//!
//! Once a job has been enqueued, a worker must be run in order to process it.
//! Workers can be consructed from tasks, such as jobs. Jobs also provide a
//! convenience method, [`Job::run`], for constructing and running a worker:
//! ```rust
//! # use sqlx::PgPool;
//! # use underway::Queue;
//! # use underway::Job;
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
//! # let job = Job::builder()
//! #    .queue(queue)
//! #    .execute(|_: ()| async move { Ok(()) })
//! #    .build();
//! // Run the worker directly from the job.
//! job.run().await?;
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! # });
//! # }
//! ```
//!
//! # Scheduling jobs
//!
//! Since jobs are tasks, they can be scheduled using cron-like expressions just
//! as tasks can be. Both setting the schedule and running the scheduler can be
//! done via the job with [`Job::schedule`] and [`Job::run_scheduler`],
//! respectively.
//!
//! Note that schedules are executed in a given time zone. This helps ensure
//! that daylight saving time (DST) arithmetic is performed correctly. DST can
//! introduce subtle scheduling errors, so by explicitly running schedules in
//! the provided time zone we ensure to account for it.
//!
//! ```rust
//! # use sqlx::PgPool;
//! # use underway::Queue;
//! # use underway::Job;
//! # use tokio::runtime::Runtime;
//! # use serde::{Serialize, Deserialize};
//! # fn main() {
//! # let rt = Runtime::new().unwrap();
//! # rt.block_on(async {
//! # let pool = PgPool::connect("postgres://user:password@localhost/database").await?;
//! # let queue = Queue::builder()
//! #    .name("example_queue")
//! #    .pool(pool)
//! #    .build()
//! #    .await?;
//! // Our task input type, used as config context here.
//! #[derive(Clone, Serialize, Deserialize)]
//! struct JobConfig {
//!     report_title: String,
//! }
//! # let job = Job::builder()
//! #     .queue(queue)
//! #     .execute(|_: JobConfig| async move { Ok(()) })
//! #     .build();
//! #
//!
//! // Set a schedule for the job.
//! let daily = "@daily[America/Los_Angeles]".parse()?;
//! job.schedule(
//!     daily,
//!     JobConfig {
//!         report_title: "Daily sales report".to_string(),
//!     },
//! )
//! .await?;
//!
//! // Run the scheduler directly from the job.
//! job.run_scheduler().await?;
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
    queue::{Error as QueueError, Queue},
    scheduler::{Result as SchedulerResult, Scheduler, ZonedSchedule},
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
    ///
    /// **Note:** If you pass a transactional executor and the transaction is
    /// rolled back, the returned task ID will not correspond to any persisted
    /// task.
    pub async fn enqueue_using<'a, E>(&self, executor: E, input: JobInput<I>) -> Result<TaskId>
    where
        E: PgExecutor<'a>,
    {
        let id = self.queue.enqueue(executor, self, input).await?;

        Ok(id)
    }

    /// Enqueue the job after the given delay using a connection from the
    /// queue's pool
    ///
    /// The given delay is added to the task's configured delay, if one is set.
    pub async fn enqueue_after<'a, E>(
        &self,
        executor: E,
        input: JobInput<I>,
        delay: Span,
    ) -> Result<TaskId>
    where
        E: PgExecutor<'a>,
    {
        self.enqueue_after_using(executor, input, delay).await
    }

    /// Enqueue the job using the provided executor after the given delay.
    ///
    /// The given delay is added to the task's configured delay, if one is set.
    ///
    /// This allows jobs to be enqueued using the same transaction as an
    /// application may already be using in a given context.
    ///
    /// **Note:** If you pass a transactional executor and the transaction is
    /// rolled back, the returned task ID will not correspond to any persisted
    /// task.
    pub async fn enqueue_after_using<'a, E>(
        &self,
        executor: E,
        input: JobInput<I>,
        delay: Span,
    ) -> Result<TaskId>
    where
        E: PgExecutor<'a>,
    {
        let id = self
            .queue
            .enqueue_after(executor, self, input, delay)
            .await?;

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

    /// Contructs a worker which then immediately runs schedule processing.
    pub async fn run_scheduler(&self) -> SchedulerResult {
        let scheduler = Scheduler::from(self);
        scheduler.run().await
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

#[cfg(test)]
mod tests {
    use jiff::ToSpan;
    use serde::{Deserialize, Serialize};
    use sqlx::PgPool;

    use super::*;

    #[sqlx::test]
    async fn create_job(pool: PgPool) -> sqlx::Result<(), Error> {
        let queue = Queue::builder()
            .name("create_job")
            .pool(pool)
            .build()
            .await?;

        #[derive(Clone, Serialize, Deserialize)]
        struct Input {
            message: String,
        }

        let job = Job::builder()
            .queue(queue.clone())
            .execute(|input: Input| async move {
                println!("Executing job with message: {}", input.message);
                Ok(())
            })
            .retry_policy(RetryPolicy::default())
            .timeout(5.minutes())
            .ttl(1.day())
            .delay(10.seconds())
            .concurrency_key("test_key")
            .priority(10)
            .build();

        // Assert that job properties are correctly set.
        assert_eq!(job.retry_policy(), RetryPolicy::default());
        assert_eq!(job.timeout(), 5.minutes());
        assert_eq!(job.ttl(), 1.day());
        assert_eq!(job.delay(), 10.seconds());
        assert_eq!(job.concurrency_key(), Some("test_key".to_string()));
        assert_eq!(job.priority(), 10);

        Ok(())
    }

    #[sqlx::test]
    async fn enqueue_job(pool: PgPool) -> sqlx::Result<(), Error> {
        let queue = Queue::builder()
            .name("enqueue_job")
            .pool(pool.clone())
            .build()
            .await?;

        #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
        struct Input {
            message: String,
        }

        let job = Job::builder()
            .queue(queue.clone())
            .execute(|_: Input| async { Ok(()) })
            .build();

        let input = Input {
            message: "Hello, world!".to_string(),
        };
        let task_id = job.enqueue(input.clone()).await?;

        let Some(dequeued_task) = queue.dequeue(&pool).await? else {
            panic!("Task should exist");
        };

        assert_eq!(task_id, dequeued_task.id);
        assert_eq!(
            input,
            serde_json::from_value::<Input>(dequeued_task.input).unwrap(),
        );

        Ok(())
    }

    #[sqlx::test]
    async fn schedule_job(pool: PgPool) -> sqlx::Result<(), Error> {
        let queue = Queue::builder()
            .name("schedule_job")
            .pool(pool.clone())
            .build()
            .await?;

        let job = Job::builder()
            .queue(queue.clone())
            .execute(|_: ()| async { Ok(()) })
            .build();

        let monthly = "@monthly[America/Los_Angeles]"
            .parse()
            .expect("A valid zoned scheduled should be provided");
        job.schedule(monthly, ()).await?;

        let (schedule, _) = queue.task_schedule(&pool).await?;

        assert_eq!(
            schedule,
            "@monthly[America/Los_Angeles]"
                .parse()
                .expect("A valid zoned scheduled should be provided")
        );

        Ok(())
    }
}
