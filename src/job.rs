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
//!     .execute(|Message { content }| async move {
//!         println!("Received: {content}");
//!         Ok(())
//!     })
//!     .queue(queue)
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
//!     .execute(|Order { id }| async move {
//!         println!("Order ID: {id}");
//!         Ok(())
//!     })
//!     .queue(queue)
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
//! #     .execute(|Order { id }| async move {
//! #         println!("Order ID: {id}");
//! #         Ok(())
//! #     })
//! #     .queue(queue)
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
//! #    .execute(|_: ()| async move { Ok(()) })
//! #    .queue(queue)
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
//! #     .execute(|_: JobConfig| async move { Ok(()) })
//! #     .queue(queue)
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
//!
//! # Jobs with state
//!
//! Sometimes it's helpful to provide jobs with access to a shared context or
//! state.
//!
//! For example, your jobs might need access to a database connection pool.
//! Defining a state can make it easier to access these kinds of resources
//! in the scope of your execute function.
//!
//! Note that when a state is provided, the execute function must take first the
//! input and then the state.
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
//! #    .pool(pool.clone())
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
//! // The execute state type.
//! #[derive(Clone)]
//! struct State {
//!     db_pool: PgPool,
//! }
//!
//! // Define the job with state.
//! let job = Job::builder()
//!     .state(State { db_pool: pool })
//!     .execute(|_: Message, State { db_pool }| async move {
//!         // Do something with our connection pool...
//!         Ok(())
//!     })
//!     .queue(queue)
//!     .build();
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! # });
//! # }
//! ```
//!
//! ## Shared mutable state
//!
//! In order to mutate state from within an execution, some form of interior
//! mutability should be used. The most straightfoward is `Arc<Mutex<T>>`.
//!
//! If the mutex is held across await points then an async-aware lock (such as
//! [`tokio::sync::Mutex`]) is needed. That said, generally a synchronous lock
//! is what you want and should be preferred.
//! ```rust
//! # use sqlx::PgPool;
//! # use underway::Queue;
//! use std::sync::{Arc, Mutex};
//!
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
//! // The execute state type.
//! #[derive(Clone)]
//! struct State {
//!     data: Arc<Mutex<String>>,
//! }
//!
//! // Define the job with state.
//! let job = Job::builder()
//!     .state(State {
//!         data: Arc::new(Mutex::new("foo".to_string())),
//!     })
//!     .execute(|_: Message, State { data }| async move {
//!         let mut data = data.lock().expect("Mutex should not be poisoned");
//!         *data = "bar".to_string();
//!         Ok(())
//!     })
//!     .queue(queue)
//!     .build();
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! # });
//! # }
//! ```

use std::{future::Future, marker::PhantomData, pin::Pin, sync::Arc};

use builder_states::{ExecutorSet, Initial, QueueSet, StateSet};
use jiff::Span;
use serde::{de::DeserializeOwned, Serialize};
use sqlx::PgExecutor;

use crate::{
    queue::{Error as QueueError, Queue},
    scheduler::{Error as SchedulerError, Result as SchedulerResult, Scheduler, ZonedSchedule},
    task::{Error as TaskError, Id as TaskId, Result as TaskResult, RetryPolicy, Task},
    worker::{Error as WorkerError, Result as WorkerResult, Worker},
};

type JobInput<I, S> = <Job<I, S> as Task>::Input;

type SimpleExecuteFn<I> =
    Arc<dyn Fn(I) -> Pin<Box<dyn Future<Output = TaskResult> + Send>> + Send + Sync>;

type StatefulExecuteFn<I, S> =
    Arc<dyn Fn(I, S) -> Pin<Box<dyn Future<Output = TaskResult> + Send>> + Send + Sync>;

#[derive(Clone)]
pub(crate) enum ExecuteFn<I, S> {
    Simple(SimpleExecuteFn<I>),
    Stateful(StatefulExecuteFn<I, S>),
}

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

    /// Error returned from worker operation.
    #[error(transparent)]
    Worker(#[from] WorkerError),

    /// Error returned from scheduler operation.
    #[error(transparent)]
    Scheduler(#[from] SchedulerError),

    /// Error returned from Tokio task joins.
    #[error(transparent)]
    Join(#[from] tokio::task::JoinError),

    /// Error returned from database operations.
    #[error(transparent)]
    Database(#[from] sqlx::Error),
}

/// Ergnomic implementation of the `Task` trait.
#[derive(Clone)]
pub struct Job<I, S = ()>
where
    Self: Task,
    I: Clone + DeserializeOwned + Serialize + Send + 'static,
    S: Clone + Send + Sync + 'static,
{
    pub(crate) queue: Queue<Self>,
    execute_fn: ExecuteFn<I, S>,
    state: S,
    retry_policy: RetryPolicy,
    timeout: Span,
    ttl: Span,
    delay: Span,
    concurrency_key: Option<String>,
    priority: i32,
}

impl<I, S> Job<I, S>
where
    Self: Task,
    I: Clone + DeserializeOwned + Serialize + Send + 'static,
    S: Clone + Send + Sync + 'static,
{
    /// Create a new job builder.
    pub fn builder() -> Builder<I, S, Initial> {
        Builder::default()
    }

    /// Enqueue the job using a connection from the queue's pool.
    pub async fn enqueue(&self, input: JobInput<I, S>) -> Result<TaskId> {
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
    pub async fn enqueue_using<'a, E>(&self, executor: E, input: JobInput<I, S>) -> Result<TaskId>
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
        input: JobInput<I, S>,
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
        input: JobInput<I, S>,
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
    pub async fn schedule(&self, zoned_schedule: ZonedSchedule, input: JobInput<I, S>) -> Result {
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
        input: JobInput<I, S>,
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
    pub async fn run_worker(&self) -> WorkerResult {
        let worker = Worker::from(self);
        worker.run().await
    }

    /// Contructs a worker which then immediately runs schedule processing.
    pub async fn run_scheduler(&self) -> SchedulerResult {
        let scheduler = Scheduler::from(self);
        scheduler.run().await
    }

    /// Runs both a worker and scheduler for the job.
    pub async fn run(&self) -> Result {
        let worker = Worker::from(self);
        let scheduler = Scheduler::from(self);

        let worker_task = tokio::spawn(async move { worker.run().await });
        let scheduler_task = tokio::spawn(async move { scheduler.run().await });

        tokio::select! {
            res =  worker_task => {
                match res {
                    Ok(inner_res) => inner_res?,
                    Err(join_err) => return Err(Error::from(join_err)),
                }
            },

            res = scheduler_task => {
                match res {
                    Ok(inner_res) => inner_res?,
                    Err(join_err) => return Err(Error::from(join_err)),
                }
            },
        }

        Ok(())
    }
}

mod builder_states {
    use super::{DeserializeOwned, ExecuteFn, Job, Queue, Serialize};

    pub struct Initial;

    pub struct StateSet<S>
    where
        S: Clone + Send + Sync + 'static,
    {
        pub state: S,
    }

    pub struct ExecutorSet<I, S>
    where
        I: Clone + DeserializeOwned + Serialize + Send + 'static,
        S: Clone + Send + Sync + 'static,
    {
        pub state: S,
        pub(crate) execute_fn: ExecuteFn<I, S>,
    }

    pub struct QueueSet<I, S>
    where
        I: Clone + DeserializeOwned + Serialize + Send + 'static,
        S: Clone + Send + Sync + 'static,
    {
        pub state: S,
        pub(crate) execute_fn: ExecuteFn<I, S>,
        pub queue: Queue<Job<I, S>>,
    }
}

/// Builder for [`Job`].
#[derive(Debug)]
pub struct Builder<I, S, B = Initial>
where
    I: Clone + DeserializeOwned + Serialize + Send + 'static,
    S: Clone + Send + Sync + 'static,
{
    builder_state: B,
    retry_policy: RetryPolicy,
    timeout: Span,
    ttl: Span,
    delay: Span,
    concurrency_key: Option<String>,
    priority: i32,
    _marker: PhantomData<(I, S)>,
}

impl<I, S> Builder<I, S, ExecutorSet<I, S>>
where
    I: Clone + DeserializeOwned + Serialize + Send + 'static,
    S: Clone + Send + Sync + 'static,
{
    /// Set retry policy.
    ///
    /// See [`Task::retry_policy`].
    ///
    /// # Example
    ///
    /// ```rust
    /// # use underway::Job;
    /// use underway::task::RetryPolicy;
    ///
    /// // Make a custom retry policy.
    /// let retry_policy = RetryPolicy::builder().max_interval_ms(20_000).build();
    ///
    /// Job::builder()
    /// #   .execute(|_: ()| async { Ok(()) })
    ///     //.execute(...)
    ///     .retry_policy(retry_policy);
    /// ```
    pub fn retry_policy(mut self, retry_policy: RetryPolicy) -> Self {
        self.retry_policy = retry_policy;
        self
    }

    /// Set execution timeout.
    ///
    /// See [`Task::timeout`].
    ///
    /// # Example
    ///
    /// ```rust
    /// # use underway::Job;
    /// use jiff::ToSpan;
    ///
    /// // Set a one-hour timeout on task execution.
    /// let timeout = 1.hour();
    ///
    /// Job::builder()
    /// #   .execute(|_: ()| async { Ok(()) })
    ///     //.execute(...)
    ///     .timeout(timeout);
    /// ```
    pub fn timeout(mut self, timeout: Span) -> Self {
        self.timeout = timeout;
        self
    }

    /// Set task time-to-live.
    ///
    /// See [`Task::ttl`].
    ///
    /// # Example
    ///
    /// ```rust
    /// # use underway::Job;
    /// use jiff::ToSpan;
    ///
    /// // Set a one-year time-to-live for tasks.
    /// let ttl = 1.year();
    ///
    /// Job::builder()
    /// #   .execute(|_: ()| async { Ok(()) })
    ///     //.execute(...)
    ///     .ttl(ttl);
    /// ```
    pub fn ttl(mut self, ttl: Span) -> Self {
        self.ttl = ttl;
        self
    }

    /// Set dequeue delay.
    ///
    /// See [`Task::delay`].
    ///
    /// # Example
    ///
    /// ```rust
    /// # use underway::Job;
    /// use jiff::ToSpan;
    ///
    /// // Set a five-minute delay before the task will be dequeued.
    /// let delay = 5.minutes();
    ///
    /// Job::builder()
    /// #   .execute(|_: ()| async { Ok(()) })
    ///     //.execute(...)
    ///     .delay(delay);
    /// ```
    pub fn delay(mut self, delay: Span) -> Self {
        self.delay = delay;
        self
    }

    /// Set concurrency key.
    ///
    /// See [`Task::concurrency_key`].
    ///
    /// # Example
    ///
    /// ```rust
    /// # use underway::Job;
    /// // Use a unique string to ensure only one instance of the task can be run at a time.
    /// let concurrency_key = "unique".to_string();
    ///
    /// Job::builder()
    /// #   .execute(|_: ()| async { Ok(()) })
    ///     //.execute(...)
    ///     .concurrency_key(concurrency_key);
    /// ```
    pub fn concurrency_key(mut self, concurrency_key: impl Into<String>) -> Self {
        self.concurrency_key = Some(concurrency_key.into());
        self
    }

    /// Set priority.
    ///
    /// See [`Task::priority`].
    ///
    /// # Example
    ///
    /// ```rust
    /// # use underway::Job;
    /// // Use a higher priority to ensure task is prioritized.
    /// let priority = 10;
    ///
    /// Job::builder()
    /// #   .execute(|_: ()| async { Ok(()) })
    ///     //.execute(...)
    ///     .priority(priority);
    /// ```
    pub fn priority(mut self, priority: i32) -> Self {
        self.priority = priority;
        self
    }
}

impl<I, S> Builder<I, S, Initial>
where
    I: Clone + DeserializeOwned + Serialize + Send + 'static,
    S: Clone + Send + Sync + 'static,
{
    /// Create a job builder.
    pub fn new() -> Builder<I, S, Initial> {
        Builder::<I, S, _> {
            builder_state: Initial,
            retry_policy: RetryPolicy::default(),
            timeout: Span::new().minutes(15),
            ttl: Span::new().days(14),
            delay: Span::new(),
            concurrency_key: None,
            priority: 0,
            _marker: PhantomData,
        }
    }

    /// Set state.
    ///
    /// State allows you to inject dependencies into your execute closures.
    /// When given, state is passed to the execute closure as its second
    /// argument.
    ///
    /// # Mutable state
    ///
    /// Note that if your state needs to mutable, some form of interior
    /// mutability is required. The most straightforward approach is to use
    /// `Arc<Mutex<T>>`.
    ///
    /// # Example
    ///
    /// ```rust
    /// # use underway::Job;
    /// // The type we'll use for our state.
    /// #[derive(Clone)]
    /// struct State {
    ///     data: String,
    /// }
    ///
    /// // Our state value.
    /// let state = State {
    ///     data: "foo".to_string(),
    /// };
    ///
    /// Job::<(), _>::builder().state(state);
    /// ```
    pub fn state(self, state: S) -> Builder<I, S, StateSet<S>> {
        Builder {
            builder_state: StateSet { state },
            retry_policy: self.retry_policy,
            timeout: self.timeout,
            ttl: self.ttl,
            delay: self.delay,
            concurrency_key: self.concurrency_key,
            priority: self.priority,
            _marker: PhantomData,
        }
    }

    /// Set execute closure.
    ///
    /// The provided closure must take input and return a future that outputs
    /// [`task::Result`](crate::task::Result).
    ///
    /// # Example
    ///
    /// ```rust
    /// # use serde::{Deserialize, Serialize};
    /// # use underway::Job;
    /// #[derive(Clone, Deserialize, Serialize)]
    /// struct Message {
    ///     content: String,
    /// }
    ///
    /// Job::<_, ()>::builder().execute(|Message { content }| async move {
    ///     println!("{content}");
    ///     Ok(())
    /// });
    /// ```
    pub fn execute<F, Fut>(self, f: F) -> Builder<I, S, ExecutorSet<I, ()>>
    where
        F: Fn(I) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = TaskResult> + Send + 'static,
    {
        Builder {
            builder_state: ExecutorSet {
                execute_fn: ExecuteFn::Simple(Arc::new(move |input: I| {
                    let fut = f(input);
                    Box::pin(fut)
                })),
                state: (),
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

impl<I, S> Default for Builder<I, S, Initial>
where
    I: Clone + DeserializeOwned + Serialize + Send + 'static,
    S: Clone + Send + Sync + 'static,
{
    fn default() -> Self {
        Self::new()
    }
}

impl<I, S> Builder<I, S, StateSet<S>>
where
    I: Clone + DeserializeOwned + Serialize + Send + 'static,
    S: Clone + Send + Sync + 'static,
{
    /// Set execute closure with state.
    ///
    /// The provided closure must take input and then state and return
    /// a future which outputs [`task::Result`](crate::task::Result).
    ///
    /// # Example
    ///
    /// ```rust
    /// # use serde::{Deserialize, Serialize};
    /// # use underway::Job;
    /// #[derive(Clone, Deserialize, Serialize)]
    /// // The type we'll use for our input.
    /// struct Message {
    ///     content: String,
    /// }
    ///
    /// // The type we'll use for our state.
    /// #[derive(Clone)]
    /// struct State {
    ///     data: String,
    /// }
    ///
    /// // Our state value.
    /// let state = State {
    ///     data: "foo".to_string(),
    /// };
    ///
    /// Job::builder()
    ///     .state(state)
    ///     .execute(|Message { content }, State { data }| async move {
    ///         println!("{content} {data}");
    ///         Ok(())
    ///     });
    /// ```
    pub fn execute<F, Fut>(self, f: F) -> Builder<I, S, ExecutorSet<I, S>>
    where
        F: Fn(I, S) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = TaskResult> + Send + 'static,
    {
        Builder {
            builder_state: ExecutorSet {
                execute_fn: ExecuteFn::Stateful(Arc::new(move |input: I, state: S| {
                    let fut = f(input, state);
                    Box::pin(fut)
                })),
                state: self.builder_state.state,
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

impl<I, S> Builder<I, S, ExecutorSet<I, S>>
where
    I: Clone + DeserializeOwned + Serialize + Send + 'static,
    S: Clone + Send + Sync + 'static,
{
    /// Set queue.
    ///
    /// # Example
    ///
    /// ```rust
    /// # use serde::{Deserialize, Serialize};
    /// # use underway::{Job, Queue};
    /// # use sqlx::PgPool;
    /// # use tokio::runtime::Runtime;
    /// # fn main() {
    /// # let rt = Runtime::new().unwrap();
    /// # rt.block_on(async {
    /// # let pool = PgPool::connect("postgres://user:password@localhost/database").await?;
    /// # let queue = Queue::builder()
    /// #    .name("example_queue")
    /// #    .pool(pool)
    /// #    .build()
    /// #    .await?;
    /// # /*
    /// let queue = { /* The queue we've defined for our job */ };
    /// # */
    /// #
    ///
    /// Job::builder()
    /// #    .execute(|_: ()| async { Ok(()) })
    ///     //.execute(...)
    ///     .queue(queue);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # });
    /// # }
    /// ```
    pub fn queue(self, queue: Queue<Job<I, S>>) -> Builder<I, S, QueueSet<I, S>> {
        Builder {
            builder_state: QueueSet {
                state: self.builder_state.state,
                execute_fn: self.builder_state.execute_fn,
                queue,
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

impl<I, S> Builder<I, S, QueueSet<I, S>>
where
    I: Clone + DeserializeOwned + Serialize + Send + 'static,
    S: Clone + Send + Sync + 'static,
{
    /// Returns a new [`Job`].
    ///
    ///# Example
    ///
    /// ```rust
    /// # use serde::{Deserialize, Serialize};
    /// # use underway::{Job, Queue};
    /// # use sqlx::PgPool;
    /// # use tokio::runtime::Runtime;
    /// # fn main() {
    /// # let rt = Runtime::new().unwrap();
    /// # rt.block_on(async {
    /// # let pool = PgPool::connect("postgres://user:password@localhost/database").await?;
    /// # let queue = Queue::builder()
    /// #    .name("example_queue")
    /// #    .pool(pool)
    /// #    .build()
    /// #    .await?;
    /// let job = Job::builder()
    /// #    .execute(|_: ()| async { Ok(()) })
    ///     //.execute(...)
    /// #    .queue(queue)
    ///     //.queue(queue)
    ///     .build();
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # });
    /// # }
    /// ```
    pub fn build(self) -> Job<I, S> {
        let QueueSet {
            queue,
            execute_fn,
            state,
        } = self.builder_state;
        Job {
            queue,
            execute_fn,
            state,
            retry_policy: self.retry_policy,
            timeout: self.timeout,
            ttl: self.ttl,
            delay: self.delay,
            concurrency_key: self.concurrency_key,
            priority: self.priority,
        }
    }
}

impl<I, S> Task for Job<I, S>
where
    I: Clone + DeserializeOwned + Serialize + Send + 'static,
    S: Clone + Send + Sync + 'static,
{
    type Input = I;

    async fn execute(&self, input: Self::Input) -> TaskResult {
        match self.execute_fn.to_owned() {
            ExecuteFn::Simple(f) => f(input).await,
            ExecuteFn::Stateful(f) => f(input, self.state.to_owned()).await,
        }
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
        self.concurrency_key.to_owned()
    }

    fn priority(&self) -> i32 {
        self.priority
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use jiff::ToSpan;
    use serde::{Deserialize, Serialize};
    use sqlx::PgPool;

    use super::*;
    use crate::queue::graceful_shutdown;

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
            .queue(queue.clone())
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
    async fn create_job_with_state(pool: PgPool) -> sqlx::Result<(), Error> {
        let queue = Queue::builder()
            .name("create_job")
            .pool(pool)
            .build()
            .await?;

        #[derive(Clone)]
        struct State {
            env: String,
        }

        #[derive(Clone, Serialize, Deserialize)]
        struct Input {
            message: String,
        }

        let job = Job::builder()
            .state(State {
                env: "test".to_string(),
            })
            .execute(|input: Input, state: State| async move {
                println!(
                    "Executing job with message: {} in environment {}",
                    input.message, state.env
                );
                Ok(())
            })
            .retry_policy(RetryPolicy::default())
            .timeout(5.minutes())
            .ttl(1.day())
            .delay(10.seconds())
            .concurrency_key("test_key")
            .priority(10)
            .queue(queue.clone())
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
            .execute(|_: Input| async { Ok(()) })
            .queue(queue.clone())
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
            .execute(|_: ()| async { Ok(()) })
            .queue(queue.clone())
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

    #[sqlx::test]
    async fn shared_mutable_state(pool: PgPool) -> sqlx::Result<(), Error> {
        let queue = Queue::builder()
            .name("shared_mutable_state")
            .pool(pool.clone())
            .build()
            .await?;

        #[derive(Clone)]
        struct State {
            data: Arc<Mutex<String>>,
        }

        let state = State {
            data: Arc::new(Mutex::new("foo".to_string())),
        };

        let job = Job::builder()
            .state(state.clone())
            .execute(|_: (), State { data }| async move {
                let mut data = data.lock().unwrap();
                *data = "bar".to_string();
                Ok(())
            })
            .queue(queue)
            .build();

        job.enqueue(()).await?;

        tokio::spawn(async move { job.run().await });

        // Wait for job to complete
        tokio::time::sleep(std::time::Duration::from_millis(500)).await;

        assert_eq!(*state.data.lock().unwrap(), "bar".to_string());

        // Shutdown and wait for a bit to ensure the test can exit.
        tokio::spawn(async move { graceful_shutdown(&pool).await });
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        Ok(())
    }
}
