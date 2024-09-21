use std::{future::Future, marker::PhantomData, pin::Pin, sync::Arc};

use builder_states::{ExecutorSet, Initial, QueueSet};
use chrono_tz::Tz;
use cron::Schedule;
use serde::{de::DeserializeOwned, Serialize};
use sqlx::PgExecutor;
use time::{Duration, OffsetDateTime};

use crate::{
    queue::{Error as QueueError, Queue},
    task::{Id as TaskId, Result as TaskResult, RetryPolicy, Task},
    worker::{Result as WorkerResult, Worker},
};

type ExecuteFn<I> =
    Arc<dyn Fn(I) -> Pin<Box<dyn Future<Output = TaskResult> + Send>> + Send + Sync>;

type Result<T = ()> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Error returned by the `sqlx` crate during database operations.
    #[error(transparent)]
    Database(#[from] sqlx::Error),

    /// Error return from queue operations.
    #[error(transparent)]
    Queue(#[from] QueueError),

    /// Custom error that accepts any string. This allows for more flexible
    /// error reporting.
    #[error("{0}")]
    Custom(String),
}

#[derive(Clone)]
pub struct Job<I>
where
    Self: Task,
    I: Clone,
{
    pub(crate) queue: Queue<Self>,
    execute_fn: ExecuteFn<I>,
    retry_policy: RetryPolicy,
    timeout: Duration,
    available_at: OffsetDateTime,
    concurrency_key: Option<String>,
    priority: i32,
}

impl<I> Job<I>
where
    I: Clone + DeserializeOwned + Serialize + Send + 'static,
    Self: Task,
{
    /// Enqueue the job using a connection from the queue's pool.
    pub async fn enqueue(&self, input: <Job<I> as Task>::Input) -> Result<TaskId> {
        let mut conn = self.queue.pool.acquire().await?;
        self.enqueue_using(&mut *conn, input).await
    }

    /// Enqueue the job using the provided executor.
    ///
    /// This allows jobs to be enqueued using the same transaction as an
    /// application may already be using in a given context.
    pub async fn enqueue_using<'a, E>(
        &self,
        executor: E,
        input: <Job<I> as Task>::Input,
    ) -> Result<TaskId>
    where
        E: PgExecutor<'a>,
    {
        let id = self.queue.enqueue(executor, self, input).await?;

        Ok(id)
    }

    pub async fn schedule_using<'a, E>(
        &self,
        executor: E,
        schedule: Schedule,
        timezone: Tz,
        input: <Job<I> as Task>::Input,
    ) -> Result
    where
        E: PgExecutor<'a>,
    {
        self.queue
            .schedule(executor, schedule, timezone, input)
            .await?;

        Ok(())
    }

    pub async fn schedule(
        &self,
        schedule: Schedule,
        timezone: Tz,
        input: <Job<I> as Task>::Input,
    ) -> Result {
        let mut conn = self.queue.pool.acquire().await?;
        self.schedule_using(&mut *conn, schedule, timezone, input)
            .await
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

pub struct JobBuilder<I, S = Initial>
where
    I: Clone + DeserializeOwned + Serialize + Send + 'static,
{
    state: S,
    retry_policy: RetryPolicy,
    timeout: Duration,
    available_at: OffsetDateTime,
    concurrency_key: Option<String>,
    priority: i32,
    _marker: PhantomData<I>,
}

impl<I, S> JobBuilder<I, S>
where
    I: Clone + DeserializeOwned + Serialize + Send + 'static,
{
    pub fn retry_policy(mut self, retry_policy: RetryPolicy) -> Self {
        self.retry_policy = retry_policy;
        self
    }

    pub fn timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    pub fn available_at(mut self, available_at: OffsetDateTime) -> Self {
        self.available_at = available_at;
        self
    }

    pub fn concurrency_key(mut self, concurrency_key: impl Into<String>) -> Self {
        self.concurrency_key = Some(concurrency_key.into());
        self
    }

    pub fn priority(mut self, priority: i32) -> Self {
        self.priority = priority;
        self
    }
}

impl<I> JobBuilder<I, Initial>
where
    I: Clone + DeserializeOwned + Serialize + Send + 'static,
{
    /// Create a job builder that will use the provided queue.
    pub fn new(queue: Queue<Job<I>>) -> JobBuilder<I, QueueSet<I>> {
        JobBuilder::<I, _> {
            state: QueueSet { queue },
            retry_policy: RetryPolicy::default(),
            timeout: Duration::minutes(15),
            available_at: OffsetDateTime::now_utc(),
            concurrency_key: None,
            priority: 0,
            _marker: PhantomData,
        }
    }
}

impl<I> JobBuilder<I, QueueSet<I>>
where
    I: Clone + DeserializeOwned + Serialize + Send + 'static,
{
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
            available_at: self.available_at,
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
    pub async fn build(self) -> Result<Job<I>> {
        let ExecutorSet { queue, execute_fn } = self.state;
        Ok(Job {
            queue,
            execute_fn,
            retry_policy: self.retry_policy,
            timeout: self.timeout,
            available_at: self.available_at,
            concurrency_key: self.concurrency_key,
            priority: self.priority,
        })
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
        self.retry_policy.clone()
    }

    fn timeout(&self) -> Duration {
        self.timeout
    }

    fn available_at(&self) -> OffsetDateTime {
        self.available_at
    }

    fn concurrency_key(&self) -> Option<String> {
        self.concurrency_key.clone()
    }

    fn priority(&self) -> i32 {
        self.priority
    }
}
