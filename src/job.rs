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

use builder_states::{Initial, PoolSet, QueueNameSet, QueueSet, StepSet};
use futures::FutureExt;
use jiff::Span;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use sqlx::{PgConnection, PgExecutor, PgPool};

use crate::{
    queue::{Error as QueueError, Queue},
    scheduler::{Error as SchedulerError, Result as SchedulerResult, Scheduler, ZonedSchedule},
    task::{self, Error as TaskError, Id as TaskId, Result as TaskResult, RetryPolicy, Task},
    worker::{Error as WorkerError, Result as WorkerResult, Worker},
};
type JobQueue<S> = Queue<Job<S>>;

/// Represents the state after executing a step.
#[derive(Deserialize, Serialize)]
pub enum StepState<S> {
    Next(S),
    Delay((S, Span)),
    Done,
}

impl<S> StepState<S> {
    pub fn to_next(step: S) -> TaskResult<Self> {
        Ok(Self::Next(step))
    }

    pub fn delay_for(step: S, span: Span) -> TaskResult<Self> {
        Ok(Self::Delay((step, span)))
    }
}

impl StepState<()> {
    pub fn done() -> TaskResult<StepState<()>> {
        Ok(StepState::Done)
    }
}

/// Trait for defining a single step in a job.
pub trait Step: Send + Sync {
    type Input: DeserializeOwned + Serialize + Send + 'static;
    type Output: Serialize + Send + 'static;

    /// Executes the step with the provided input.
    fn execute(
        &self,
        input: Self::Input,
    ) -> Pin<Box<dyn Future<Output = TaskResult<StepState<Self::Output>>> + Send>>;
}

/// A concrete implementation of a step using a closure.
pub struct StepFn<I, O, F>
where
    F: Fn(I) -> Pin<Box<dyn Future<Output = TaskResult<StepState<O>>> + Send>>
        + Send
        + Sync
        + 'static,
{
    func: Arc<F>,
    _marker: PhantomData<(I, O)>,
}

impl<I, O, F> StepFn<I, O, F>
where
    F: Fn(I) -> Pin<Box<dyn Future<Output = TaskResult<StepState<O>>> + Send>>
        + Send
        + Sync
        + 'static,
{
    pub fn new(func: F) -> Self {
        Self {
            func: Arc::new(func),
            _marker: PhantomData,
        }
    }
}

impl<I, O, F> Step for StepFn<I, O, F>
where
    I: serde::de::DeserializeOwned + serde::Serialize + Send + Sync + 'static,
    O: serde::Serialize + Send + Sync + 'static,
    F: Fn(I) -> Pin<Box<dyn Future<Output = TaskResult<StepState<O>>> + Send>>
        + Send
        + Sync
        + 'static,
{
    type Input = I;
    type Output = O;

    fn execute(
        &self,
        input: Self::Input,
    ) -> Pin<Box<dyn Future<Output = TaskResult<StepState<Self::Output>>> + Send>> {
        (self.func)(input)
    }
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
pub struct Job<S>
where
    S: Sync + Send + 'static,
{
    pub(crate) queue: JobQueue<S>,
    steps: Arc<Vec<Box<dyn StepExecutor>>>,
    _marker: PhantomData<S>,
}

impl<I> Job<I>
where
    I: Serialize + Sync + Send + 'static,
{
    /// Create a new job builder.
    pub fn builder() -> Builder<I, I, Initial> {
        Builder::new()
    }

    /// Enqueue the job using a connection from the queue's pool.
    pub async fn enqueue(&self, input: I) -> Result<TaskId> {
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
    pub async fn enqueue_using<'a, E>(&self, executor: E, input: I) -> Result<TaskId>
    where
        E: PgExecutor<'a>,
        I: Serialize,
    {
        let step_input = serde_json::to_value(input).unwrap(); // TODO: Queue error
        let input = JobState::new(step_input);
        let id = self.queue.enqueue(executor, self, input).await?;

        Ok(id)
    }

    /// Enqueue the job after the given delay using a connection from the
    /// queue's pool
    ///
    /// The given delay is added to the task's configured delay, if one is set.
    pub async fn enqueue_after<'a, E>(&self, executor: E, input: I, delay: Span) -> Result<TaskId>
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
        input: I,
        delay: Span,
    ) -> Result<TaskId>
    where
        E: PgExecutor<'a>,
    {
        let step_input = serde_json::to_value(input).unwrap(); // TODO: Queue error
        let input = JobState::new(step_input);

        let id = self
            .queue
            .enqueue_after(executor, self, input, delay)
            .await?;

        Ok(id)
    }

    ///// Schedule the job using a connection from the queue's pool.
    //pub async fn schedule(&self, zoned_schedule: ZonedSchedule, input:
    // JobInput<I, O>) -> Result {    let mut conn =
    // self.queue.pool.acquire().await?;    self.schedule_using(&mut *conn,
    // zoned_schedule, input).await
    //}

    ///// Schedule the job using the provided executor.
    /////
    ///// This allows jobs to be scheduled using the same transaction as an
    ///// application may already be using in a given context.
    //pub async fn schedule_using<'a, E>(
    //    &self,
    //    executor: E,
    //    zoned_schedule: ZonedSchedule,
    //    input: JobInput<I, O>,
    //) -> Result
    //where
    //    E: PgExecutor<'a>,
    //{
    //    self.queue.schedule(executor, zoned_schedule, input).await?;

    //    Ok(())
    //}

    ///// Constructs a worker which then immediately runs task processing.
    //pub async fn run_worker(&self) -> WorkerResult {
    //    let worker = Worker::from(self);
    //    worker.run().await
    //}

    ///// Contructs a worker which then immediately runs schedule processing.
    //pub async fn run_scheduler(&self) -> SchedulerResult {
    //    let scheduler = Scheduler::from(self);
    //    scheduler.run().await
    //}

    /// Runs both a worker and scheduler for the job.
    pub async fn run(self) -> Result {
        let queue = self.queue.clone();
        let job = Arc::new(self);

        let worker = Worker::new(queue.clone(), job.clone());
        let scheduler = Scheduler::new(queue, job.clone());

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

// TODO: Versioning?
#[derive(Serialize, Deserialize)]
pub struct JobState {
    step_index: usize,
    step_input: serde_json::Value,
}

impl JobState {
    fn new(step_input: serde_json::Value) -> Self {
        Self {
            step_index: 0,
            step_input,
        }
    }
}

impl<S> Task for Job<S>
where
    S: Send + Sync + 'static,
{
    type Input = JobState;
    type Output = ();

    async fn execute(
        &self,
        conn: &mut PgConnection,
        input: Self::Input,
    ) -> TaskResult<Self::Output> {
        let JobState {
            step_index,
            step_input,
        } = input;

        if step_index >= self.steps.len() {
            return Err(TaskError::Fatal("Invalid step index.".into()));
        }

        let step = &self.steps[step_index];

        // Enqueue the next step if one is given.
        if let Some((step_input, delay)) = step.execute_step(step_input).await? {
            let next_job = Job {
                queue: self.queue.clone(),
                steps: Arc::clone(&self.steps),
                _marker: PhantomData,
            };
            let next_job_input = JobState {
                step_input,
                step_index: step_index + 1,
            };
            self.queue
                .enqueue_after(conn, &next_job, next_job_input, delay)
                .await
                .map_err(|err| TaskError::Retryable(err.to_string()))?;
        };

        Ok(())
    }
}
/// A trait object wrapper for steps to allow heterogeneous step types in a
/// vector.
pub trait StepExecutor: Send + Sync {
    /// Execute the step with the given input serialized as JSON.
    fn execute_step(
        &self,
        input: serde_json::Value,
    ) -> Pin<Box<dyn Future<Output = TaskResult<Option<(serde_json::Value, Span)>>> + Send>>;
}

impl<I, O, F> StepExecutor for StepFn<I, O, F>
where
    I: DeserializeOwned + Serialize + Send + Sync + 'static,
    O: Serialize + Send + Sync + 'static,
    F: Fn(I) -> Pin<Box<dyn Future<Output = TaskResult<StepState<O>>> + Send>>
        + Send
        + Sync
        + 'static,
{
    fn execute_step(
        &self,
        input: serde_json::Value,
    ) -> Pin<Box<dyn Future<Output = TaskResult<Option<(serde_json::Value, Span)>>> + Send>> {
        let deserialized_input: I = match serde_json::from_value(input) {
            Ok(val) => val,
            Err(e) => return Box::pin(async move { Err(TaskError::Fatal(e.to_string())) }),
        };
        let fut = self.execute(deserialized_input);
        Box::pin(async move {
            match fut.await {
                Ok(StepState::Next(output)) => {
                    let serialized_output = serde_json::to_value(output)
                        .map_err(|e| TaskError::Fatal(e.to_string()))?;
                    Ok(Some((serialized_output, Span::new())))
                }

                Ok(StepState::Delay((output, span))) => {
                    let serialized_output = serde_json::to_value(output)
                        .map_err(|e| TaskError::Fatal(e.to_string()))?;
                    Ok(Some((serialized_output, span)))
                }

                Ok(StepState::Done) => Ok(None),

                Err(e) => Err(e),
            }
        })
    }
}

mod builder_states {
    use std::marker::PhantomData;

    use sqlx::PgPool;

    use super::JobQueue;

    pub struct Initial;

    pub struct StepSet<Current> {
        pub _marker: PhantomData<Current>,
    }

    pub struct QueueSet<I>
    where
        I: Send + Sync + 'static,
    {
        pub queue: JobQueue<I>,
    }

    pub struct QueueNameSet {
        pub queue_name: String,
    }

    pub struct PoolSet {
        pub queue_name: String,
        pub pool: PgPool,
    }
}

/// A builder for constructing a `Job` with a sequence of steps.
pub struct Builder<I, O, State = Initial> {
    builder_state: State,
    steps: Vec<Box<dyn StepExecutor>>,
    _marker: PhantomData<(I, O)>,
}

impl<I> Default for Builder<I, I, Initial> {
    fn default() -> Self {
        Self::new()
    }
}

impl<I> Builder<I, I, Initial> {
    /// Create a new builder.
    pub fn new() -> Builder<I, I, Initial> {
        Builder::<I, I, _> {
            builder_state: Initial,
            steps: Vec::new(),
            _marker: PhantomData,
        }
    }

    /// Add a step to the job.
    ///
    /// This method ensures that the input type of the new step matches the
    /// output type of the previous step.
    pub fn step<F, O, Fut>(mut self, func: F) -> Builder<I, O, StepSet<O>>
    where
        I: DeserializeOwned + Serialize + Send + Sync + 'static,
        O: Serialize + Send + Sync + 'static,
        F: Fn(I) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = TaskResult<StepState<O>>> + Send + 'static,
    {
        let step_fn = StepFn::new(move |input: I| func(input).boxed());
        self.steps.push(Box::new(step_fn));

        Builder {
            builder_state: StepSet {
                _marker: PhantomData,
            },
            steps: self.steps,
            _marker: PhantomData,
        }
    }
}

impl<I, Current> Builder<I, Current, StepSet<Current>> {
    /// Add a subsequent step to the job.
    ///
    /// This method ensures that the input type of the new step matches the
    /// output type of the previous step.
    pub fn step<F, New, Fut>(mut self, func: F) -> Builder<I, New, StepSet<New>>
    where
        Current: DeserializeOwned + Serialize + Send + Sync + 'static,
        New: Serialize + Send + Sync + 'static,
        F: Fn(Current) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = TaskResult<StepState<New>>> + Send + 'static,
    {
        let step_fn = StepFn::new(move |input: Current| func(input).boxed());
        self.steps.push(Box::new(step_fn));

        Builder {
            builder_state: StepSet {
                _marker: PhantomData,
            },
            steps: self.steps,
            _marker: PhantomData,
        }
    }
}

// Encapsulate queue creation.
impl<I> Builder<I, (), StepSet<()>>
where
    I: Send + Sync + 'static,
{
    /// Set the name of the job's queue.
    pub fn name(self, name: impl Into<String>) -> Builder<I, (), QueueNameSet> {
        Builder {
            builder_state: QueueNameSet {
                queue_name: name.into(),
            },
            steps: self.steps,
            _marker: PhantomData,
        }
    }
}

impl<I> Builder<I, (), QueueNameSet>
where
    I: Send + Sync + 'static,
{
    /// Set the name of the job's queue.
    pub fn pool(self, pool: PgPool) -> Builder<I, (), PoolSet> {
        let QueueNameSet { queue_name } = self.builder_state;
        Builder {
            builder_state: PoolSet { queue_name, pool },
            steps: self.steps,
            _marker: PhantomData,
        }
    }
}

impl<I> Builder<I, (), PoolSet>
where
    I: Send + Sync + 'static,
{
    /// Finalize the builder into a `Job`.
    pub async fn build(self) -> Result<Job<I>> {
        let PoolSet { queue_name, pool } = self.builder_state;
        let queue = Queue::builder().name(queue_name).pool(pool).build().await?;
        Ok(Job {
            queue,
            steps: Arc::new(self.steps),
            _marker: PhantomData,
        })
    }
}

// Directly provide queue.
impl<I> Builder<I, (), StepSet<()>>
where
    I: Send + Sync + 'static,
{
    /// Set the queue.
    pub fn queue(self, queue: JobQueue<I>) -> Builder<I, (), QueueSet<I>> {
        Builder {
            builder_state: QueueSet { queue },
            steps: self.steps,
            _marker: PhantomData,
        }
    }
}

impl<I> Builder<I, (), QueueSet<I>>
where
    I: Send + Sync + 'static,
{
    /// Finalize the builder into a `Job`.
    pub fn build(self) -> Job<I> {
        let QueueSet { queue } = self.builder_state;
        Job {
            queue,
            steps: Arc::new(self.steps),
            _marker: PhantomData,
        }
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
