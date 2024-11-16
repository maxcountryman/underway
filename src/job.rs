//! Jobs are a series of sequential steps, where each step is a [`Task`].
//!
//! Each step is given the output of the previous step as input. In other words,
//! steps form a chain, where one step is linked to the next, until the last
//! step is reached.
//!
//!```text
//! ╭ StepFn 1 ─────╮   ╭ StepFn 2 ─────╮   ╭ StepFn 3 ─────╮
//! │ input:     13 │ ┌╴│ input:     42 │ ┌╴│ input:  "foo" │
//! │ output:    42 │╶┘ │ output: "foo" │╶┘ │ output:     ∅ │
//! ╰───────────────╯   ╰───────────────╯   ╰───────────────╯
//! ```
//!
//! Because each step is treated as its own task, steps are executed and then
//! persisted, before the next task is enqueued. This means that when a step,
//! the job will be resumed from where it was.
//!
//! # Defining jobs
//!
//! Jobs are formed from at least one step function.
//!
//! ```rust
//! use underway::{Job, To};
//!
//! let job_builder = Job::<(), ()>::builder().step(|_cx, _| async move { To::done() });
//! ```
//!
//! Instead of a closure, we could also use a named function.
//!
//! ```rust
//! use underway::{job::Context, task::Result as TaskResult, Job, To};
//!
//! async fn named_step(_cx: Context<()>, _: ()) -> TaskResult<To<()>> {
//!     To::done()
//! }
//!
//! let job_builder = Job::<_, ()>::builder().step(named_step);
//! ```
//!
//! Notice that the first argument to our function is [a context
//! binding](crate::job::Context). This provides access to fields like
//! [`state`](crate::job::Context::state) and
//! [`tx`](crate::job::Context::tx).
//!
//! The second argument is a type we provide as input to the step. In our
//! example it's the unit type. But if we were to specify another type it would
//! be that.
//!
//! To give a step input, we need a type that's `Serialize` and `Deserialize`.
//! This is because this input will be persisted by the database.
//!
//! ```rust
//! use serde::{Deserialize, Serialize};
//! use underway::{Job, To};
//!
//! // Our very own input type.
//! #[derive(Serialize, Deserialize)]
//! struct MyArgs {
//!     name: String,
//! };
//!
//! let job_builder =
//!     Job::<_, ()>::builder().step(|_cx, MyArgs { name }| async move { To::done() });
//! ```
//!
//! Besides input, it's also important to point out that we return a specific
//! type that indicates what to do next. So far, we've only had a single step
//! and so we've returned [`To::done`].
//!
//! But jobs may be composed of any number of sequential steps. Each step is
//! structured to take the output of the last step. By returning [`To::next`]
//! with the input of the next step, we move on to that step.
//!
//! ```rust
//! use serde::{Deserialize, Serialize};
//! use underway::{Job, To};
//!
//! // Here's the input for our first step.
//! #[derive(Serialize, Deserialize)]
//! struct Step1 {
//!     n: usize,
//! };
//!
//! // And this is the input for our second step.
//! #[derive(Serialize, Deserialize)]
//! struct Step2 {
//!     original: usize,
//!     new: usize,
//! };
//!
//! let job_builder = Job::<_, ()>::builder()
//!     .step(|_cx, Step1 { n }| async move {
//!         println!("Got {n}");
//!
//!         // Go to the next step with step 2's input.
//!         To::next(Step2 {
//!             original: n,
//!             new: n + 2,
//!         })
//!     })
//!     .step(|_cx, Step2 { original, new }| async move {
//!         println!("Was {original} now is {new}");
//!
//!         // No more steps, so we're done.
//!         To::done()
//!     });
//! ```
//!
//! Inputs to steps are strongly typed and so the compiler will complain if we
//! try to return a type that the next step isn't expecting.
//!
//! In the following example, we've made a mistake and tried to use `Step1` as
//! the output of our first step. This will not compile.
//!
//! ```rust,compile_fail
//! use serde::{Deserialize, Serialize};
//! use underway::{Job, To};
//!
//! #[derive(Serialize, Deserialize)]
//! struct Step1 {
//!     n: usize,
//! };
//!
//! #[derive(Serialize, Deserialize)]
//! struct Step2 {
//!     original: usize,
//!     new: usize,
//! };
//!
//! let job_builder = Job::<_, ()>::builder()
//!     .step(|_cx, Step1 { n }| async move {
//!         println!("Got {n}");
//!
//!         // This is does not compile!
//!         To::next(Step1 { n })
//!     })
//!     .step(|_cx, Step2 { original, new }| async move {
//!         println!("Was {original} now is {new}");
//!         To::done()
//!     });
//! ```
//!
//! Often we want to immediately transition to the next step. However, there may
//! be cases were we want to wait some time beforehand. We can return
//! [`To::delay_for`] to express this.
//!
//! Like transitioning to the next step, we give the input still but also supply
//! a delay as the second argument.
//!
//! ```rust
//! use jiff::ToSpan;
//! use serde::{Deserialize, Serialize};
//! use underway::{Job, To};
//!
//! #[derive(Serialize, Deserialize)]
//! struct Step1 {
//!     n: usize,
//! };
//!
//! #[derive(Serialize, Deserialize)]
//! struct Step2 {
//!     original: usize,
//!     new: usize,
//! };
//!
//! let job_builder = Job::<_, ()>::builder()
//!     .step(|_cx, Step1 { n }| async move {
//!         println!("Got {n}");
//!
//!         // We aren't quite ready for the next step so we wait one hour.
//!         To::delay_for(
//!             Step2 {
//!                 original: n,
//!                 new: n + 2,
//!             },
//!             1.hour(),
//!         )
//!     })
//!     .step(|_cx, Step2 { original, new }| async move {
//!         println!("Was {original} now is {new}");
//!         To::done()
//!     });
//! ```
//!
//! A common use case is delaying a step until a specific time, whenever that
//! might be from now. We can create a span from now until the desired future
//! point.
//!
//! ```rust
//! use jiff::Timestamp;
//!
//! let now = Timestamp::now().intz("America/Los_Angeles").unwrap();
//! let until_tomorrow_morning = now
//!     .tomorrow()
//!     // Tomorrow morning at 9:30 AM in Los Angeles.
//!     .and_then(|tomorrow| tomorrow.date().at(9, 30, 0, 0).intz("America/Los_Angeles"))
//!     .and_then(|tomorrow_morning| now.until(&tomorrow_morning))
//!     .unwrap();
//! # assert!(until_tomorrow_morning.is_positive());
//! ```
//!
//! Then this span may be used as our delay given to `To::delay`.
//!
//! # Stateful jobs
//!
//! So far, we've ignored the context binding. One reason we need it is to
//! access shared state we've set on the job.
//!
//! State like this can be useful when there may be resources or configuration
//! that all steps should have access to. The only requirement is that the state
//! type be `Clone`, as it will be cloned into each step.
//! ```rust
//! use underway::{job::Context, Job, To};
//!
//! // A simple state that'll be provided to our steps.
//! #[derive(Clone)]
//! struct State {
//!     data: String,
//! }
//!
//! let state = State {
//!     data: "data".to_string(),
//! };
//!
//! let job_builder = Job::<(), _>::builder()
//!     .state(state) // Here we've set the state.
//!     .step(|Context { state, .. }, _| async move {
//!         println!("State data is: {}", state.data);
//!         To::done()
//!     });
//! ```
//!
//! For simplicity, we only have a single step. But in practice, state may be
//! accessed by any step in a sequence.
//!
//! Note that state is not serialized to the database and therefore not
//! persisted between process restarts. **State should not be used for durable
//! data.**
//!
//! ## Shared mutable state
//!
//! It's possible to use state in shared mutable fashion via patterns for
//! interior mutability like `Arc<Mutex<..>>`.
//!
//! However, **please use caution**: state is maintained between job executions
//! and is not reset. This means that independent step executions, **including
//! those that may have originated from different enqueues of the job**,
//! will have access to the same state. For this reason, this pattern is
//! discouraged.
//!
//! # Atomicity
//!
//! Apart from state, context also provides another useful field: a transaction
//! that's shared wtih the worker.
//!
//! Access to this transaction means we can make updates to the database that
//! are only visible if the execution itself succeeds and the transaction is
//! committed by the worker.
//!
//! ```rust
//! use serde::{Deserialize, Serialize};
//! use underway::{job::Context, Job, To};
//!
//! #[derive(Serialize, Deserialize)]
//! struct UserSub {
//!     user_id: i64,
//! }
//!
//! let job_builder =
//!     Job::<_, ()>::builder().step(|Context { mut tx, .. }, UserSub { user_id }| async move {
//!         sqlx::query(
//!             r#"
//!             update user
//!             set subscribed_at = now()
//!             where id = $1
//!             "#,
//!         )
//!         .bind(user_id)
//!         .fetch_one(&mut *tx)
//!         .await?;
//!
//!         tx.commit().await?;
//!
//!         To::done()
//!     });
//! ```
//!
//! The special thing about this code is that we've leveraged the transaction
//! provided by the context to make an update to the user table. What this means
//! is the execution either succeeds and this update becomes visible or it
//! doesn't and it's like nothing ever happened.
//!
//! # Retry policies
//!
//! Steps being tasks also have associated retry policies. This policy inherits
//! the default but can be providied for each step.
//!
//! ```rust
//! use serde::{Deserialize, Serialize};
//! use underway::{task::RetryPolicy, Job, To};
//!
//! #[derive(Serialize, Deserialize)]
//! struct Step1 {
//!     n: usize,
//! };
//!
//! #[derive(Serialize, Deserialize)]
//! struct Step2 {
//!     original: usize,
//!     new: usize,
//! };
//!
//! let job_builder = Job::<_, ()>::builder()
//!     .step(|_cx, Step1 { n }| async move {
//!         println!("Got {n}");
//!         To::next(Step2 {
//!             original: n,
//!             new: n + 2,
//!         })
//!     })
//!     .retry_policy(RetryPolicy::builder().max_attempts(1).build())
//!     .step(|_cx, Step2 { original, new }| async move {
//!         println!("Was {original} now is {new}");
//!         To::done()
//!     })
//!     .retry_policy(RetryPolicy::builder().max_interval_ms(15_000).build());
//! ```
//!
//! # Enqueuing jobs
//!
//! Once we've configured our job with its sequence of one or more steps we can
//! build the job and enqueue it with input.
//!
//! ```rust,no_run
//! # use sqlx::PgPool;
//! use underway::{Job, To};
//!
//! # use tokio::runtime::Runtime;
//! # fn main() {
//! # let rt = Runtime::new().unwrap();
//! # rt.block_on(async {
//! # let pool = PgPool::connect(&std::env::var("DATABASE_URL")?).await?;
//! # /*
//! let pool = { /* A `PgPool`. */ };
//! # */
//! #
//!
//! let job = Job::builder()
//!     .step(|_cx, _| async move { To::done() })
//!     .name("example-job")
//!     .pool(pool)
//!     .build()
//!     .await?;
//!
//! // Enqueue a new job with the given input `()`.
//! job.enqueue(&()).await?;
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! # });
//! # }
//! ```
//!
//! We could also supply a queue that's already been constructed, to use as our
//! job's queue. This obviates the need to await the job build method.
//!
//! ```rust,no_run
//! # use sqlx::PgPool;
//! use underway::{Job, Queue, To};
//!
//! # use tokio::runtime::Runtime;
//! # fn main() {
//! # let rt = Runtime::new().unwrap();
//! # rt.block_on(async {
//! # let pool = PgPool::connect(&std::env::var("DATABASE_URL")?).await?;
//! # /*
//! let pool = { /* A `PgPool`. */ };
//! # */
//! #
//!
//! // We've defined a queue directly.
//! let queue = Queue::builder()
//!     .name("example-job")
//!     .pool(pool)
//!     .build()
//!     .await?;
//!
//! let job = Job::builder()
//!     .step(|_cx, _| async move { To::done() })
//!     .queue(queue)
//!     .build();
//!
//! job.enqueue(&()).await?;
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! # });
//! # }
//! ```
//!
//! Notice that we've enqueued a value of a type that's compatible with our
//! first step's input--in this case that's unit since we haven't given another
//! type.
//!
//! ```rust,no_run
//! # use sqlx::PgPool;
//! use serde::{Deserialize, Serialize};
//! use underway::{Job, To};
//!
//! # use tokio::runtime::Runtime;
//! # fn main() {
//! # let rt = Runtime::new().unwrap();
//! # rt.block_on(async {
//! # let pool = PgPool::connect(&std::env::var("DATABASE_URL")?).await?;
//! # /*
//! let pool = { /* A `PgPool`. */ };
//! # */
//! #
//!
//! #[derive(Serialize, Deserialize)]
//! struct Input {
//!     bucket_name: String,
//! }
//!
//! let job = Job::builder()
//!     .step(|_cx, Input { bucket_name }| async move { To::done() })
//!     .name("example-job")
//!     .pool(pool)
//!     .build()
//!     .await?;
//!
//! // Enqueue a new job with a slightly more interesting value.
//! job.enqueue(&Input {
//!     bucket_name: "my_bucket".to_string(),
//! })
//! .await?;
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! # });
//! # }
//! ```
//!
//! While we're only demonstrating a single step here for brevity, the process
//! is the same for jobs with multiple steps.
//!
//! ## Atomic enqueue
//!
//! The `enqueue` method uses a connection from the queue's pool. If we prefer
//! instead to use a transaction supplied by the surrounding code we can use
//! [`enqueue_using`](Job::enqueue_using).
//!
//! By doing so, we can ensure that the enqueue will only happen when the
//! transaction is committed.
//!
//! ```rust,no_run
//! # use sqlx::PgPool;
//! use serde::{Deserialize, Serialize};
//! use underway::{Job, To};
//!
//! # use tokio::runtime::Runtime;
//! # fn main() {
//! # let rt = Runtime::new().unwrap();
//! # rt.block_on(async {
//! # let pool = PgPool::connect(&std::env::var("DATABASE_URL")?).await?;
//! # /*
//! let pool = { /* A `PgPool`. */ };
//! # */
//! #
//!
//! let job = Job::builder()
//!     .step(|_cx, _| async move { To::done() })
//!     .name("atomic-enqueue")
//!     .pool(pool.clone())
//!     .build()
//!     .await?;
//!
//! let mut tx = pool.begin().await?;
//!
//! # /*
//! /* Some intervening logic involving `tx`... */
//! # */
//! #
//!
//! // Enqueue using a transaction that we supply.
//! job.enqueue_using(&mut *tx, &()).await?;
//!
//! # /*
//! /* ...And more intervening logic involving `tx`. */
//! # */
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! # });
//! # }
//! ```
//!
//! # Runnning jobs
//!
//! Jobs are run via workers and schedulers, where the former processes tasks
//! and the latter processes schedules for enqueuing tasks. Starting both is
//! encapsulated by the job interface.
//!
//! ```rust,no_run
//! # use sqlx::PgPool;
//! use underway::{Job, To};
//!
//! # use tokio::runtime::Runtime;
//! # fn main() {
//! # let rt = Runtime::new().unwrap();
//! # rt.block_on(async {
//! # let pool = PgPool::connect(&std::env::var("DATABASE_URL")?).await?;
//! # /*
//! let pool = { /* A `PgPool`. */ };
//! # */
//! #
//!
//! let job = Job::builder()
//!     .step(|_cx, _: ()| async move { To::done() })
//!     .name("example-job")
//!     .pool(pool)
//!     .build()
//!     .await?;
//!
//! // This starts the worker and scheduler in the background (non-blocking).
//! job.start();
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! # });
//! # }
//! ```
//!
//! Typically starting the job such that our program isn't blocked is desirable.
//! However, we can also run the job in a blocking manner directly.
//!
//! ```rust,no_run
//! # use sqlx::PgPool;
//! use underway::{Job, To};
//!
//! # use tokio::runtime::Runtime;
//! # fn main() {
//! # let rt = Runtime::new().unwrap();
//! # rt.block_on(async {
//! # let pool = PgPool::connect(&std::env::var("DATABASE_URL")?).await?;
//! # /*
//! let pool = { /* A `PgPool`. */ };
//! # */
//! #
//!
//! let job = Job::builder()
//!     .step(|_cx, _: ()| async move { To::done() })
//!     .name("example-job")
//!     .pool(pool)
//!     .build()
//!     .await?;
//!
//! // This starts the worker and scheduler and blocks.
//! job.run().await?;
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! # });
//! # }
//! ```
//!
//! Workers and schedulers can be used directly too.
//!
//! ```rust,no_run
//! # use sqlx::PgPool;
//! use underway::{Job, Queue, Scheduler, To, Worker};
//!
//! # use tokio::runtime::Runtime;
//! # fn main() {
//! # let rt = Runtime::new().unwrap();
//! # rt.block_on(async {
//! # let pool = PgPool::connect(&std::env::var("DATABASE_URL")?).await?;
//! # /*
//! let pool = { /* A `PgPool`. */ };
//! # */
//! #
//!
//! let queue = Queue::builder()
//!     .name("example-job")
//!     .pool(pool)
//!     .build()
//!     .await?;
//!
//! let job = Job::builder()
//!     .step(|_cx, _: ()| async move { To::done() })
//!     .queue(queue.clone())
//!     .build();
//!
//! let worker = Worker::new(queue.clone(), job.clone());
//! let scheduler = Scheduler::new(queue, job);
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! # });
//! # }
//! ```
//!
//! # Scheduling jobs
//!
//! Jobs may also be run on a schedule that follows the form of a cron-like
//! expression.
//!
//! ```rust,no_run
//! # use sqlx::PgPool;
//! use underway::{Job, To};
//!
//! # use tokio::runtime::Runtime;
//! # fn main() {
//! # let rt = Runtime::new().unwrap();
//! # rt.block_on(async {
//! # let pool = PgPool::connect(&std::env::var("DATABASE_URL")?).await?;
//! # /*
//! let pool = { /* A `PgPool`. */ };
//! # */
//! #
//!
//! let job = Job::builder()
//!     .step(|_cx, _: ()| async move { To::done() })
//!     .name("scheduled-job")
//!     .pool(pool)
//!     .build()
//!     .await?;
//!
//! // Sets a weekly schedule with the given input.
//! let weekly = "@weekly[America/Los_Angeles]".parse()?;
//! job.schedule(&weekly, &()).await?;
//!
//! job.start();
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! # });
//! # }
//! ```
//!
//! Often input to a scheduled job would consist of static configuration or
//! other fields that are shared for scheduled runs.
//!
//! Also note that jobs with schedules may still be enqueued manually when
//! desired.

use std::{
    future::Future,
    marker::PhantomData,
    mem,
    ops::Deref,
    pin::Pin,
    result::Result as StdResult,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    task::Poll,
};

use builder_states::{Initial, PoolSet, QueueNameSet, QueueSet, StateSet, StepSet};
use jiff::Span;
use sealed::JobState;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use sqlx::{PgExecutor, PgPool, Postgres, Transaction};
use tokio::task::{JoinError, JoinSet};
use tokio_util::sync::CancellationToken;
use tracing::instrument;
use ulid::Ulid;
use uuid::Uuid;

use crate::{
    queue::{Error as QueueError, InProgressTask, Queue},
    scheduler::{Error as SchedulerError, Result as SchedulerResult, Scheduler, ZonedSchedule},
    task::{
        Error as TaskError, Result as TaskResult, RetryPolicy, State as TaskState, Task, TaskId,
    },
    worker::{Error as WorkerError, Result as WorkerResult, Worker},
};

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

    /// Error returned from serde_json.
    #[error(transparent)]
    Json(#[from] serde_json::Error),

    /// Error returned from database operations.
    #[error(transparent)]
    Database(#[from] sqlx::Error),
}

type JobQueue<T, S> = Queue<Job<T, S>>;

/// Context passed in to each step.
pub struct Context<S> {
    /// Shared step state.
    ///
    /// This value is set via [`state`](Builder::state).
    ///
    /// **Note:** State is not persisted and therefore should not be
    /// relied on when durability is needed.
    pub state: S,

    /// A savepoint that originates from the worker executing the task.
    ///
    /// This is useful for ensuring atomicity: writes made to the database with
    /// this handle are only realized if the worker succeeds in processing
    /// the task.
    ///
    /// Put another way, this savepoint is derived from the same transaction
    /// that holds the lock on the underlying task row.
    pub tx: Transaction<'static, Postgres>,
}

type StepConfig<S> = (Box<dyn StepExecutor<S>>, RetryPolicy);

mod sealed {
    use serde::{Deserialize, Serialize};

    use super::JobId;

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    pub struct JobState {
        pub step_index: usize,
        pub step_input: serde_json::Value,
        pub(crate) job_id: JobId,
    } // TODO: Versioning?
}

/// Container for the runtime of the job instance.
///
/// Provides a method to gracefully stop the worker and scheduler.
pub struct JobHandle {
    workers: JoinSet<StdResult<Result<()>, JoinError>>,
    shutdown_token: CancellationToken,
}

impl JobHandle {
    /// Signals the worker and scheduler to shutdown and waits for them to
    /// finish.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use sqlx::PgPool;
    /// # use underway::{Job, To};
    /// # use tokio::runtime::Runtime;
    /// # fn main() {
    /// # let rt = Runtime::new().unwrap();
    /// # rt.block_on(async {
    /// # let pool = PgPool::connect(&std::env::var("DATABASE_URL")?).await?;
    /// # let job = Job::<(), ()>::builder()
    /// #     .step(|_cx, _| async move { To::done() })
    /// #     .name("example")
    /// #     .pool(pool)
    /// #     .build()
    /// #     .await?;
    /// # /*
    /// let job = { /* A `Job`. */ };
    /// # */
    /// #
    ///
    /// let job_handle = job.start();
    ///
    /// // Gracefully stop worker and scheduler.
    /// job_handle.shutdown().await?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # });
    /// # }
    /// ```
    pub async fn shutdown(mut self) -> Result<()> {
        self.shutdown_token.cancel();

        // Await all tasks in the join set
        while let Some(result) = self.workers.join_next().await {
            match result? {
                Ok(Ok(())) => {}
                Ok(Err(err)) => return Err(err),
                Err(join_err) => return Err(join_err.into()),
            }
        }

        Ok(())
    }
}

impl Unpin for JobHandle {}

impl Future for JobHandle {
    type Output = StdResult<Result<()>, JoinError>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut std::task::Context<'_>) -> Poll<Self::Output> {
        // Poll the join set to see if all tasks are done
        while let Poll::Ready(Some(result)) = self.workers.poll_join_next(cx) {
            match result? {
                Ok(Ok(())) => continue,
                Ok(Err(err)) => return Poll::Ready(Ok(Err(err))),
                Err(join_err) => return Poll::Ready(Err(join_err)),
            }
        }

        if self.workers.is_empty() {
            Poll::Ready(Ok(Ok(())))
        } else {
            Poll::Pending
        }
    }
}

/// Unique identifier of a job.
///
/// Wraps a UUID which is generated via a ULID.
///
/// Each enqueue of a job receives its own ID. IDs are embedded in the input of
/// tasks on the queue. This means that each task related to a job will have
/// the same job ID.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub(crate) struct JobId(Uuid);

impl JobId {
    fn new() -> Self {
        Self(Ulid::new().into())
    }
}

impl Deref for JobId {
    type Target = Uuid;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// Represents a specific job that's been enqueued.
///
/// This handle allows for manipulating the state of the job in the queue.
pub struct EnqueuedJob<T: Task> {
    id: JobId,
    queue: Queue<T>,
}

impl<T: Task> EnqueuedJob<T> {
    /// Cancels the job if it's still pending.
    ///
    /// Because jobs may be composed of multiple steps, the full set of tasks is
    /// searched and any pending tasks are cancelled.
    ///
    /// Returns `true` if any tasks were successfully cancelled. Put another
    /// way, if tasks are already cancelled or not eligible for cancellation
    /// then this returns `false`.
    ///
    /// # Errors
    ///
    /// This will return an error if the database operation fails and if the
    /// task ID cannot be found.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use sqlx::PgPool;
    /// # use underway::{Job, To};
    /// # use tokio::runtime::Runtime;
    /// # fn main() {
    /// # let rt = Runtime::new().unwrap();
    /// # rt.block_on(async {
    /// # let pool = PgPool::connect(&std::env::var("DATABASE_URL")?).await?;
    /// # let job = Job::builder()
    /// #     .step(|_cx, _| async move { To::done() })
    /// #     .name("example")
    /// #     .pool(pool)
    /// #     .build()
    /// #     .await?;
    /// # /*
    /// let job = { /* A `Job`. */ };
    /// # */
    /// #
    ///
    /// let enqueued = job.enqueue(&()).await?;
    ///
    /// // Cancel the enqueued job.
    /// enqueued.cancel().await?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # });
    /// # }
    /// ```
    pub async fn cancel(&self) -> Result<bool> {
        let mut tx = self.queue.pool.begin().await?;
        let in_progress_tasks = sqlx::query_as!(
            InProgressTask,
            r#"
            select
              id as "id: TaskId",
              task_queue_name as "queue_name",
              input,
              retry_policy as "retry_policy: RetryPolicy",
              timeout,
              heartbeat,
              concurrency_key
            from underway.task
            where input->>'job_id' = $1
              and state = $2
            for update skip locked
            "#,
            self.id.to_string(),
            TaskState::Pending as TaskState
        )
        .fetch_all(&mut *tx)
        .await?;

        let mut cancelled = false;
        for in_progress_task in in_progress_tasks {
            if in_progress_task.mark_cancelled(&mut tx).await? {
                cancelled = true;
            }
        }
        tx.commit().await?;

        Ok(cancelled)
    }
}

/// Sequential set of functions, where the output of the last is the input to
/// the next.
pub struct Job<I, S>
where
    I: Sync + Send + 'static,
    S: Clone + Sync + Send + 'static,
{
    queue: JobQueue<I, S>,
    steps: Arc<Vec<StepConfig<S>>>,
    state: S,
    current_index: Arc<AtomicUsize>,
    _marker: PhantomData<I>,
}

impl<I, S> Job<I, S>
where
    I: Serialize + Sync + Send + 'static,
    S: Clone + Send + Sync + 'static,
{
    /// Creates a new job builder.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use sqlx::PgPool;
    /// use underway::{Job, To};
    ///
    /// # use tokio::runtime::Runtime;
    /// # fn main() {
    /// # let rt = Runtime::new().unwrap();
    /// # rt.block_on(async {
    /// # let pool = PgPool::connect(&std::env::var("DATABASE_URL")?).await?;
    /// # /*
    /// let pool = { /* A `PgPool`. */ };
    /// # */
    /// #
    ///
    /// let job = Job::<(), ()>::builder()
    ///     .step(|_cx, _| async move { To::done() })
    ///     .name("example")
    ///     .pool(pool)
    ///     .build()
    ///     .await?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # });
    /// # }
    /// ```
    pub fn builder() -> Builder<I, I, S, Initial> {
        Builder::new()
    }

    /// Enqueue the job using a connection from the queue's pool.
    ///
    /// # Errors
    ///
    /// This has the same error conditions as [`Queue::enqueue`].
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use sqlx::PgPool;
    /// # use underway::{Job, To};
    /// # use tokio::runtime::Runtime;
    /// # fn main() {
    /// # let rt = Runtime::new().unwrap();
    /// # rt.block_on(async {
    /// # let pool = PgPool::connect(&std::env::var("DATABASE_URL")?).await?;
    /// # let job = Job::builder()
    /// #     .step(|_cx, _| async move { To::done() })
    /// #     .name("example")
    /// #     .pool(pool)
    /// #     .build()
    /// #     .await?;
    /// # /*
    /// let job = { /* A `Job`. */ };
    /// # */
    /// #
    ///
    /// // Enqueue a new job with the given input.
    /// job.enqueue(&()).await?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # });
    /// # }
    /// ```
    pub async fn enqueue(&self, input: &I) -> Result<EnqueuedJob<Self>> {
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
    ///
    /// # Errors
    ///
    /// This has the same error conditions as [`Queue::enqueue`].
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use sqlx::PgPool;
    /// # use underway::{Job, To};
    /// # use tokio::runtime::Runtime;
    /// # fn main() {
    /// # let rt = Runtime::new().unwrap();
    /// # rt.block_on(async {
    /// # let pool = PgPool::connect(&std::env::var("DATABASE_URL")?).await?;
    /// # /*
    /// let pool = { /* A `PgPool`. */ };
    /// # */
    /// #
    ///
    /// // Our own transaction.
    /// let mut tx = pool.begin().await?;
    ///
    /// # let job = Job::builder()
    /// #     .step(|_cx, _| async move { To::done() })
    /// #     .name("example")
    /// #     .pool(pool.clone())
    /// #     .build()
    /// #     .await?;
    /// # /*
    /// let job = { /* A `Job`. */ };
    /// # */
    /// #
    ///
    /// // Enqueue using the transaction we already have.
    /// job.enqueue_using(&mut *tx, &()).await?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # });
    /// # }
    /// ```
    pub async fn enqueue_using<'a, E>(&self, executor: E, input: &I) -> Result<EnqueuedJob<Self>>
    where
        E: PgExecutor<'a>,
    {
        self.enqueue_after_using(executor, input, Span::new()).await
    }

    /// Enqueue the job after the given delay using a connection from the
    /// queue's pool
    ///
    /// The given delay is added to the task's configured delay, if one is set.
    ///
    /// # Errors
    ///
    /// This has the same error conditions as [`Queue::enqueue_after`].
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use sqlx::PgPool;
    /// # use underway::{Job, To};
    /// use jiff::ToSpan;
    ///
    /// # use tokio::runtime::Runtime;
    /// # fn main() {
    /// # let rt = Runtime::new().unwrap();
    /// # rt.block_on(async {
    /// # let pool = PgPool::connect(&std::env::var("DATABASE_URL")?).await?;
    /// # let job = Job::builder()
    /// #     .step(|_cx, _| async move { To::done() })
    /// #     .name("example")
    /// #     .pool(pool)
    /// #     .build()
    /// #     .await?;
    /// # /*
    /// let job = { /* A `Job`. */ };
    /// # */
    /// #
    ///
    /// // Enqueue after an hour.
    /// job.enqueue_after(&(), 1.hour()).await?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # });
    /// # }
    /// ```
    pub async fn enqueue_after(&self, input: &I, delay: Span) -> Result<EnqueuedJob<Self>> {
        let mut conn = self.queue.pool.acquire().await?;
        self.enqueue_after_using(&mut *conn, input, delay).await
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
    ///
    /// # Errors
    ///
    /// This has the same error conditions as [`Queue::enqueue_after`].
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use sqlx::PgPool;
    /// # use underway::{Job, To};
    /// use jiff::ToSpan;
    ///
    /// # use tokio::runtime::Runtime;
    /// # fn main() {
    /// # let rt = Runtime::new().unwrap();
    /// # rt.block_on(async {
    /// # let pool = PgPool::connect(&std::env::var("DATABASE_URL")?).await?;
    /// # /*
    /// let pool = { /* A `PgPool`. */ };
    /// # */
    /// #
    ///
    /// // Our own transaction.
    /// let mut tx = pool.begin().await?;
    ///
    /// # let job = Job::builder()
    /// #     .step(|_cx, _| async move { To::done() })
    /// #     .name("example")
    /// #     .pool(pool.clone())
    /// #     .build()
    /// #     .await?;
    /// # /*
    /// let job = { /* A `Job`. */ };
    /// # */
    /// #
    ///
    /// // Enqueue after two days using the transaction we already have.
    /// job.enqueue_after_using(&mut *tx, &(), 2.days()).await?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # });
    /// # }
    /// ```
    pub async fn enqueue_after_using<'a, E>(
        &self,
        executor: E,
        input: &I,
        delay: Span,
    ) -> Result<EnqueuedJob<Self>>
    where
        E: PgExecutor<'a>,
    {
        let job_input = self.first_job_input(input)?;

        self.queue
            .enqueue_after(executor, self, &job_input, delay)
            .await?;

        let enqueue = EnqueuedJob {
            id: job_input.job_id,
            queue: self.queue.clone(),
        };

        Ok(enqueue)
    }

    /// Schedule the job using a connection from the queue's pool.
    ///
    /// # Errors
    ///
    /// This has the same error conditions as [`Queue::schedule`].
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use sqlx::PgPool;
    /// # use underway::{Job, To};
    /// # use tokio::runtime::Runtime;
    /// # fn main() {
    /// # let rt = Runtime::new().unwrap();
    /// # rt.block_on(async {
    /// # let pool = PgPool::connect(&std::env::var("DATABASE_URL")?).await?;
    /// # let job = Job::builder()
    /// #     .step(|_cx, _| async move { To::done() })
    /// #     .name("example")
    /// #     .pool(pool)
    /// #     .build()
    /// #     .await?;
    /// # /*
    /// let job = { /* A `Job`. */ };
    /// # */
    /// #
    ///
    /// let every_minute = "0 * * * *[America/Los_Angeles]".parse()?;
    /// job.schedule(&every_minute, &()).await?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # });
    /// # }
    /// ```
    pub async fn schedule(&self, zoned_schedule: &ZonedSchedule, input: &I) -> Result {
        let mut conn = self.queue.pool.acquire().await?;
        self.schedule_using(&mut *conn, zoned_schedule, input).await
    }

    /// Schedule the job using the provided executor.
    ///
    /// This allows jobs to be scheduled using the same transaction as an
    /// application may already be using in a given context.
    ///
    /// # Errors
    ///
    /// This has the same error conditions as [`Queue::schedule`].
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use sqlx::PgPool;
    /// # use underway::{Job, To};
    /// # use tokio::runtime::Runtime;
    /// # fn main() {
    /// # let rt = Runtime::new().unwrap();
    /// # rt.block_on(async {
    /// # let pool = PgPool::connect(&std::env::var("DATABASE_URL")?).await?;
    /// # /*
    /// let pool = { /* A `PgPool`. */ };
    /// # */
    /// #
    ///
    /// // Our own transaction.
    /// let mut tx = pool.acquire().await?;
    ///
    /// # let job = Job::builder()
    /// #     .step(|_cx, _| async move { To::done() })
    /// #     .name("example")
    /// #     .pool(pool.clone())
    /// #     .build()
    /// #     .await?;
    /// # /*
    /// let job = { /* A `Job`. */ };
    /// # */
    /// #
    ///
    /// // Schedule weekly using the transaction we already have.
    /// let weekly = "@weekly[America/Los_Angeles]".parse()?;
    /// job.schedule_using(&mut *tx, &weekly, &()).await?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # });
    /// # }
    /// ```
    pub async fn schedule_using<'a, E>(
        &self,
        executor: E,
        zoned_schedule: &ZonedSchedule,
        input: &I,
    ) -> Result
    where
        E: PgExecutor<'a>,
    {
        let job_input = self.first_job_input(input)?;
        self.queue
            .schedule(executor, zoned_schedule, &job_input)
            .await?;

        Ok(())
    }

    /// Removes the job's schedule using a connection from the queue's pool.
    ///
    /// # Errors
    ///
    /// This has the same error conditions as [`Queue::unschedule`].
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use sqlx::PgPool;
    /// # use underway::{Job, To};
    /// # use tokio::runtime::Runtime;
    /// # fn main() {
    /// # let rt = Runtime::new().unwrap();
    /// # rt.block_on(async {
    /// # let pool = PgPool::connect(&std::env::var("DATABASE_URL")?).await?;
    /// # let job = Job::<(), _>::builder()
    /// #     .step(|_cx, _| async move { To::done() })
    /// #     .name("example")
    /// #     .pool(pool)
    /// #     .build()
    /// #     .await?;
    /// # /*
    /// let job = { /* A `Job`. */ };
    /// # */
    /// #
    ///
    /// // Remove the schedule if one is set.
    /// job.unschedule().await?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # });
    /// # }
    pub async fn unschedule(&self) -> Result {
        let mut conn = self.queue.pool.acquire().await?;
        self.unschedule_using(&mut *conn).await
    }

    /// Removes the job's schedule using the provided executor.
    ///
    /// This allows jobs to be unscheduled using the same transaction as an
    /// application may already be using in a given context.
    ///
    /// # Errors
    ///
    /// This has the same error conditions as [`Queue::unschedule`].
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use sqlx::PgPool;
    /// # use underway::{Job, To};
    /// # use tokio::runtime::Runtime;
    /// # fn main() {
    /// # let rt = Runtime::new().unwrap();
    /// # rt.block_on(async {
    /// # let pool = PgPool::connect(&std::env::var("DATABASE_URL")?).await?;
    /// # /*
    /// let pool = { /* A `PgPool`. */ };
    /// # */
    /// #
    ///
    /// let mut tx = pool.acquire().await?;
    ///
    /// # let job = Job::<(), _>::builder()
    /// #     .step(|_cx, _| async move { To::done() })
    /// #     .name("example")
    /// #     .pool(pool.clone())
    /// #     .build()
    /// #     .await?;
    /// # /*
    /// let job = { /* A `Job`. */ };
    /// # */
    /// #
    ///
    /// // Remove the schedule using a transaction we provide.
    /// job.unschedule_using(&mut *tx).await?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # });
    /// # }
    /// ```
    pub async fn unschedule_using<'a, E>(&self, executor: E) -> Result
    where
        E: PgExecutor<'a>,
    {
        self.queue.unschedule(executor).await?;

        Ok(())
    }

    /// Constructs a worker which then immediately runs task processing.
    ///
    /// # Errors
    ///
    /// This has the same error conditions as [`Worker::run`].
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use sqlx::PgPool;
    /// # use underway::{Job, To};
    /// # use tokio::runtime::Runtime;
    /// # fn main() {
    /// # let rt = Runtime::new().unwrap();
    /// # rt.block_on(async {
    /// # let pool = PgPool::connect(&std::env::var("DATABASE_URL")?).await?;
    /// # let job = Job::<(), _>::builder()
    /// #     .step(|_cx, _| async move { To::done() })
    /// #     .name("example")
    /// #     .pool(pool)
    /// #     .build()
    /// #     .await?;
    /// # /*
    /// let job = { /* A `Job`. */ };
    /// # */
    /// #
    ///
    /// job.run_worker().await?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # });
    /// # }
    /// ```
    pub async fn run_worker(self) -> WorkerResult {
        let queue = self.queue.clone();
        let job = self.clone();
        let worker = Worker::new(queue, job);
        worker.run().await
    }

    /// Contructs a worker which then immediately runs schedule processing.
    ///
    /// # Errors
    ///
    /// This has the same error conditions as [`Scheduler::run`].
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use sqlx::PgPool;
    /// # use underway::{Job, To};
    /// # use tokio::runtime::Runtime;
    /// # fn main() {
    /// # let rt = Runtime::new().unwrap();
    /// # rt.block_on(async {
    /// # let pool = PgPool::connect(&std::env::var("DATABASE_URL")?).await?;
    /// # let job = Job::<(), _>::builder()
    /// #     .step(|_cx, _| async move { To::done() })
    /// #     .name("example")
    /// #     .pool(pool)
    /// #     .build()
    /// #     .await?;
    /// # /*
    /// let job = { /* A `Job`. */ };
    /// # */
    /// #
    ///
    /// job.run_scheduler().await?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # });
    /// # }
    /// ```
    pub async fn run_scheduler(self) -> SchedulerResult {
        let queue = self.queue.clone();
        let job = self.clone();
        let scheduler = Scheduler::new(queue, job);
        scheduler.run().await
    }

    /// Runs both a worker and scheduler for the job.
    ///
    /// # Errors
    ///
    /// This has the same error conditions as [`Worker::run`] and
    /// [`Scheduler::run`]. It will also return an error if either of the
    /// spawned worker or scheduler cannot be joined.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use sqlx::PgPool;
    /// # use underway::{Job, To};
    /// # use tokio::runtime::Runtime;
    /// # fn main() {
    /// # let rt = Runtime::new().unwrap();
    /// # rt.block_on(async {
    /// # let pool = PgPool::connect(&std::env::var("DATABASE_URL")?).await?;
    /// # let job = Job::<(), _>::builder()
    /// #     .step(|_cx, _| async move { To::done() })
    /// #     .name("example")
    /// #     .pool(pool)
    /// #     .build()
    /// #     .await?;
    /// # /*
    /// let job = { /* A `Job`. */ };
    /// # */
    /// #
    ///
    /// job.run().await?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # });
    /// # }
    /// ```
    pub async fn run(self) -> Result {
        let queue = self.queue.clone();
        let job = self.clone();

        let worker = Worker::new(queue.clone(), job.clone());
        let scheduler = Scheduler::new(queue, job);

        let mut workers = JoinSet::new();
        workers.spawn(async move { worker.run().await.map_err(Error::from) });
        workers.spawn(async move { scheduler.run().await.map_err(Error::from) });

        while let Some(ret) = workers.join_next().await {
            match ret {
                Ok(Err(err)) => return Err(err),
                Err(err) => return Err(Error::from(err)),
                _ => continue,
            }
        }

        Ok(())
    }

    /// Starts both a worker and scheduler for the job and returns a handle.
    ///
    /// The returned handle may be used to gracefully shutdown the worker and
    /// scheduler.
    ///
    /// # Errors
    ///
    /// This has the same error conditions as [`Worker::run`] and
    /// [`Scheduler::run`]. It will also return an error if either of the
    /// spawned worker or scheduler cannot be joined.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use sqlx::PgPool;
    /// # use underway::{Job, To};
    /// # use tokio::runtime::Runtime;
    /// # fn main() {
    /// # let rt = Runtime::new().unwrap();
    /// # rt.block_on(async {
    /// # let pool = PgPool::connect(&std::env::var("DATABASE_URL")?).await?;
    /// # let job = Job::<(), _>::builder()
    /// #     .step(|_cx, _| async move { To::done() })
    /// #     .name("example")
    /// #     .pool(pool)
    /// #     .build()
    /// #     .await?;
    /// # /*
    /// let job = { /* A `Job`. */ };
    /// # */
    /// #
    ///
    /// job.start().await??;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # });
    /// # }
    /// ```
    pub fn start(self) -> JobHandle {
        let shutdown_token = CancellationToken::new();
        let mut workers = JoinSet::new();

        let queue = self.queue.clone();
        let job = self.clone();

        let mut worker = Worker::new(queue.clone(), job.clone());
        worker.set_shutdown_token(shutdown_token.clone());

        let mut scheduler = Scheduler::new(queue, job);
        scheduler.set_shutdown_token(shutdown_token.clone());

        // Spawn the tasks using `tokio::spawn` to decouple them from polling the
        // `Future`.
        let worker_handle = tokio::spawn(async move { worker.run().await.map_err(Error::from) });
        let scheduler_handle =
            tokio::spawn(async move { scheduler.run().await.map_err(Error::from) });

        workers.spawn(worker_handle);
        workers.spawn(scheduler_handle);

        JobHandle {
            workers,
            shutdown_token,
        }
    }

    fn first_job_input(&self, input: &I) -> Result<JobState> {
        let step_input = serde_json::to_value(input)?;
        let step_index = self.current_index.load(Ordering::SeqCst);
        let job_id = JobId::new();
        Ok(JobState {
            step_input,
            step_index,
            job_id,
        })
    }
}

impl<I, S> Task for Job<I, S>
where
    I: Send + Sync + 'static,
    S: Clone + Send + Sync + 'static,
{
    type Input = JobState;
    type Output = ();

    #[instrument(
        skip_all,
        fields(
            job.id = %input.job_id.as_hyphenated(),
            step = input.step_index + 1,
            steps = self.steps.len()
        ),
        err
    )]
    async fn execute(
        &self,
        mut tx: Transaction<'_, Postgres>,
        input: Self::Input,
    ) -> TaskResult<Self::Output> {
        let JobState {
            step_index,
            step_input,
            job_id,
        } = input;

        if step_index >= self.steps.len() {
            return Err(TaskError::Fatal("Invalid step index.".into()));
        }

        let (step, _) = &self.steps[step_index];

        // SAFETY:
        //
        // We are extending the lifetime of `tx` to `'static` to satisfy the trait
        // object requirements in `StepExecutor`. This is sound because:
        //
        // 1. The `execute` method awaits the future returned by `execute_step`
        //    immediately, ensuring that `tx` remains valid during the entire operation.
        // 2. `step_tx` does not escape the scope of the `execute` method; it is not
        //    stored or moved elsewhere.
        // 3. The `Context` and any data derived from `step_tx` are used only within the
        //    `execute_step` method and its returned future.
        //
        // As a result, even though we are claiming a `'static` lifetime for `tx`, we
        // ensure that it does not actually outlive its true lifetime, maintaining
        // soundness.
        //
        // Note: This is a workaround due to limitations with trait objects and
        // lifetimes in async contexts. Be cautious with any changes that might
        // allow `step_tx` to outlive `tx`.
        let step_tx: Transaction<'static, Postgres> = unsafe { mem::transmute_copy(&tx) };

        let cx = Context {
            state: self.state.clone(),
            tx: step_tx,
        };

        // Execute the step and handle any errors.
        let step_result = match step.execute_step(cx, step_input).await {
            Ok(result) => result,
            Err(err) => {
                // N.B.: Commit the transaction to ensure attempt rows are persisted.
                tx.commit().await?;
                return Err(err);
            }
        };

        // If there's a next step, enqueue it.
        if let Some((next_input, delay)) = step_result {
            // Advance current index after executing the step.
            let next_index = step_index + 1;
            self.current_index.store(next_index, Ordering::SeqCst);

            let next_job_input = JobState {
                step_input: next_input,
                step_index: next_index,
                job_id,
            };

            self.queue
                .enqueue_after(&mut *tx, self, &next_job_input, delay)
                .await
                .map_err(|err| TaskError::Retryable(err.to_string()))?;
        }

        // Commit the transaction.
        tx.commit().await?;

        Ok(())
    }

    fn retry_policy(&self) -> RetryPolicy {
        let current_index = self.current_index.load(Ordering::SeqCst);
        let (_, retry_policy) = self.steps[current_index];
        retry_policy
    }
}

impl<I, S> Clone for Job<I, S>
where
    I: Send + Sync + 'static,
    S: Clone + Send + Sync + 'static,
{
    fn clone(&self) -> Self {
        Self {
            queue: self.queue.clone(),
            state: self.state.clone(),
            steps: self.steps.clone(),
            current_index: self.current_index.clone(),
            _marker: PhantomData,
        }
    }
}

/// Represents the state after executing a step.
#[derive(Deserialize, Serialize)]
pub enum To<N> {
    /// The next step to transition to.
    Next(N),

    /// The next step to transition to after the delay.
    Delay {
        /// The step itself.
        next: N,

        /// The delay before which the step will not be run.
        delay: Span,
    },

    /// The terminal state.
    Done,
}

impl<S> To<S> {
    /// Transitions from the current step to the next step.
    pub fn next(step: S) -> TaskResult<Self> {
        Ok(Self::Next(step))
    }

    /// Transitions from the current step to the next step, but after the given
    /// delay.
    ///
    /// The next step will be enqueued immediately, but won't be dequeued until
    /// the span has elapsed.
    pub fn delay_for(step: S, delay: Span) -> TaskResult<Self> {
        Ok(Self::Delay { next: step, delay })
    }
}

impl To<()> {
    /// Signals that this is the final step and no more steps will follow.
    pub fn done() -> TaskResult<To<()>> {
        Ok(To::Done)
    }
}

// A concrete implementation of a step using a closure.
struct StepFn<I, O, S, F>
where
    F: Fn(Context<S>, I) -> Pin<Box<dyn Future<Output = TaskResult<To<O>>> + Send>>
        + Send
        + Sync
        + 'static,
{
    func: Arc<F>,
    _marker: PhantomData<(I, O, S)>,
}

impl<I, O, S, F> StepFn<I, O, S, F>
where
    F: Fn(Context<S>, I) -> Pin<Box<dyn Future<Output = TaskResult<To<O>>> + Send>>
        + Send
        + Sync
        + 'static,
{
    fn new(func: F) -> Self {
        Self {
            func: Arc::new(func),
            _marker: PhantomData,
        }
    }
}

type StepResult = TaskResult<Option<(serde_json::Value, Span)>>;

// A trait object wrapper for steps to allow heterogeneous step types in a
// vector.
trait StepExecutor<S>: Send + Sync {
    // Execute the step with the given input serialized as JSON.
    fn execute_step(
        &self,
        cx: Context<S>,
        input: serde_json::Value,
    ) -> Pin<Box<dyn Future<Output = StepResult> + Send>>;
}

impl<I, O, S, F> StepExecutor<S> for StepFn<I, O, S, F>
where
    I: DeserializeOwned + Serialize + Send + Sync + 'static,
    O: Serialize + Send + Sync + 'static,
    S: Send + Sync + 'static,
    F: Fn(Context<S>, I) -> Pin<Box<dyn Future<Output = TaskResult<To<O>>> + Send>>
        + Send
        + Sync
        + 'static,
{
    fn execute_step(
        &self,
        cx: Context<S>,
        input: serde_json::Value,
    ) -> Pin<Box<dyn Future<Output = StepResult> + Send>> {
        let deserialized_input: I = match serde_json::from_value(input) {
            Ok(val) => val,
            Err(e) => return Box::pin(async move { Err(TaskError::Fatal(e.to_string())) }),
        };
        let fut = (self.func)(cx, deserialized_input);

        Box::pin(async move {
            match fut.await {
                Ok(To::Next(output)) => {
                    let serialized_output = serde_json::to_value(output)
                        .map_err(|e| TaskError::Fatal(e.to_string()))?;
                    Ok(Some((serialized_output, Span::new())))
                }

                Ok(To::Delay {
                    next: output,
                    delay,
                }) => {
                    let serialized_output = serde_json::to_value(output)
                        .map_err(|e| TaskError::Fatal(e.to_string()))?;
                    Ok(Some((serialized_output, delay)))
                }

                Ok(To::Done) => Ok(None),

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

    pub struct StateSet<S> {
        pub state: S,
    }

    pub struct StepSet<Current, S> {
        pub state: S,
        pub _marker: PhantomData<Current>,
    }

    pub struct QueueSet<I, S>
    where
        I: Send + Sync + 'static,
        S: Clone + Send + Sync + 'static,
    {
        pub state: S,
        pub queue: JobQueue<I, S>,
    }

    pub struct QueueNameSet<S> {
        pub state: S,
        pub queue_name: String,
    }

    pub struct PoolSet<S> {
        pub state: S,
        pub queue_name: String,
        pub pool: PgPool,
    }
}

/// Builder for constructing a `Job` with a sequence of steps.
pub struct Builder<I, O, S, B> {
    builder_state: B,
    steps: Vec<(Box<dyn StepExecutor<S>>, RetryPolicy)>,
    _marker: PhantomData<(I, O, S)>,
}

impl<I, S> Default for Builder<I, I, S, Initial> {
    fn default() -> Self {
        Self::new()
    }
}

impl<I, S> Builder<I, I, S, Initial> {
    /// Create a new builder.
    ///
    /// # Example
    ///
    /// ```rust
    /// use underway::Job;
    ///
    /// // Instantiate a new builder from the `Job` method.
    /// let job_builder = Job::<(), ()>::builder();
    /// ```
    pub fn new() -> Builder<I, I, S, Initial> {
        Builder::<I, I, S, _> {
            builder_state: Initial,
            steps: Vec::new(),
            _marker: PhantomData,
        }
    }

    /// Provides a state shared amongst all steps.
    ///
    /// The state type must be `Clone`.
    ///
    /// State is useful for providing shared resources to steps. This could
    /// include shared connections, clients, or other configuration that may be
    /// used throughout step functions.
    ///
    /// **Note:** State is not persisted and therefore should not be relied on
    /// when durability is needed.
    ///
    /// # Example
    ///
    /// ```rust
    /// use underway::Job;
    ///
    /// #[derive(Clone)]
    /// struct State {
    ///     data: String,
    /// }
    ///
    /// // Set state.
    /// let job_builder = Job::<(), _>::builder().state(State {
    ///     data: "foo".to_string(),
    /// });
    /// ```
    pub fn state(self, state: S) -> Builder<I, I, S, StateSet<S>> {
        Builder {
            builder_state: StateSet { state },
            steps: self.steps,
            _marker: PhantomData,
        }
    }

    /// Add a step to the job.
    ///
    /// A step function should take the job context as its first argument
    /// followed by some type that's `Serialize` and `Deserialize` as its
    /// second argument.
    ///
    /// It should also return one of the [`To`] variants. For convenience, `To`
    /// provides methods that return the correct types. Most commonly these
    /// will be [`To::next`], when going on to another step, or [`To::done`],
    /// when there are no more steps.
    ///
    /// # Example
    ///
    /// ```rust
    /// use underway::{Job, To};
    ///
    /// // Set a step.
    /// let job_builder = Job::<(), ()>::builder().step(|_cx, _| async move { To::done() });
    /// ```
    pub fn step<F, O, Fut>(mut self, func: F) -> Builder<I, O, S, StepSet<O, ()>>
    where
        I: DeserializeOwned + Serialize + Send + Sync + 'static,
        O: Serialize + Send + Sync + 'static,
        S: Send + Sync + 'static,
        F: Fn(Context<S>, I) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = TaskResult<To<O>>> + Send + 'static,
    {
        let step_fn = StepFn::new(move |cx, input| Box::pin(func(cx, input)));
        self.steps.push((Box::new(step_fn), RetryPolicy::default()));

        Builder {
            builder_state: StepSet {
                state: (),
                _marker: PhantomData,
            },
            steps: self.steps,
            _marker: PhantomData,
        }
    }
}

// After state set, before first step set.
impl<I, S> Builder<I, I, S, StateSet<S>> {
    /// Add a step to the job.
    ///
    /// A step function should take the job context as its first argument
    /// followed by some type that's `Serialize` and `Deserialize` as its
    /// second argument.
    ///
    /// It should also return one of the [`To`] variants. For convenience, `To`
    /// provides methods that return the correct types. Most commonly these
    /// will be [`To::next`], when going on to another step, or [`To::done`],
    /// when there are no more steps.
    ///
    /// # Example
    ///
    /// ```rust
    /// use underway::{Job, To};
    ///
    /// #[derive(Clone)]
    /// struct State {
    ///     data: String,
    /// }
    ///
    /// // Set a step with state.
    /// let job_builder = Job::<(), _>::builder()
    ///     .state(State {
    ///         data: "foo".to_string(),
    ///     })
    ///     .step(|cx, _| async move {
    ///         println!("State data: {}", cx.state.data);
    ///         To::done()
    ///     });
    /// ```
    pub fn step<F, O, Fut>(mut self, func: F) -> Builder<I, O, S, StepSet<O, S>>
    where
        I: DeserializeOwned + Serialize + Send + Sync + 'static,
        O: Serialize + Send + Sync + 'static,
        S: Send + Sync + 'static,
        F: Fn(Context<S>, I) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = TaskResult<To<O>>> + Send + 'static,
    {
        let step_fn = StepFn::new(move |cx, input| Box::pin(func(cx, input)));
        self.steps.push((Box::new(step_fn), RetryPolicy::default()));

        Builder {
            builder_state: StepSet {
                state: self.builder_state.state,
                _marker: PhantomData,
            },
            steps: self.steps,
            _marker: PhantomData,
        }
    }
}

// After first step set.
impl<I, Current, S> Builder<I, Current, S, StepSet<Current, S>> {
    /// Add a subsequent step to the job.
    ///
    /// This method ensures that the input type of the new step matches the
    /// output type of the previous step.
    ///
    /// # Example
    ///
    /// ```rust
    /// use serde::{Deserialize, Serialize};
    /// use underway::{Job, To};
    ///
    /// #[derive(Deserialize, Serialize)]
    /// struct Step2 {
    ///     n: usize,
    /// }
    ///
    /// // Set one step after another.
    /// let job_builder = Job::<(), ()>::builder()
    ///     .step(|_cx, _| async move { To::next(Step2 { n: 42 }) })
    ///     .step(|_cx, Step2 { n }| async move { To::done() });
    /// ```
    pub fn step<F, New, Fut>(mut self, func: F) -> Builder<I, New, S, StepSet<New, S>>
    where
        Current: DeserializeOwned + Serialize + Send + Sync + 'static,
        New: Serialize + Send + Sync + 'static,
        S: Send + Sync + 'static,
        F: Fn(Context<S>, Current) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = TaskResult<To<New>>> + Send + 'static,
    {
        let step_fn = StepFn::new(move |cx, input| Box::pin(func(cx, input)));
        self.steps.push((Box::new(step_fn), RetryPolicy::default()));

        Builder {
            builder_state: StepSet {
                state: self.builder_state.state,
                _marker: PhantomData,
            },
            steps: self.steps,
            _marker: PhantomData,
        }
    }

    /// Sets the retry policy of the previous step.
    ///
    /// This policy applies to the step immediately before the method. That
    /// means that a retry policy may be defined for each step and each
    /// step's policy may differ from the others.
    ///
    /// # Example
    ///
    /// ```rust
    /// use underway::{task::RetryPolicy, Job, To};
    ///
    /// // Set a retry policy for the step.
    /// let retry_policy = RetryPolicy::builder().max_attempts(15).build();
    /// let job_builder = Job::<(), ()>::builder()
    ///     .step(|_cx, _| async move { To::done() })
    ///     .retry_policy(retry_policy);
    /// ```
    pub fn retry_policy(
        mut self,
        retry_policy: RetryPolicy,
    ) -> Builder<I, Current, S, StepSet<Current, S>> {
        let (_, default_policy) = self.steps.last_mut().expect("Steps should not be empty");
        *default_policy = retry_policy;

        Builder {
            builder_state: StepSet {
                state: self.builder_state.state,
                _marker: PhantomData,
            },
            steps: self.steps,
            _marker: PhantomData,
        }
    }
}

// Encapsulate queue creation.
impl<I, S> Builder<I, (), S, StepSet<(), S>>
where
    I: Send + Sync + 'static,
    S: Clone + Send + Sync + 'static,
{
    /// Set the name of the job's queue.
    ///
    /// This provides the name of the underlying queue that will be created for
    /// this job.
    ///
    /// **Note:** It's important that this name be unique amongst all tasks. If
    /// it's not and other tasks define differing input types this will
    /// cause runtime errors when mismatching types are deserialized from
    /// the database.
    ///
    /// # Example
    ///
    /// ```rust
    /// use underway::{Job, To};
    ///
    /// // Set a name.
    /// let job_builder = Job::<(), ()>::builder()
    ///     .step(|_cx, _| async move { To::done() })
    ///     .name("example");
    /// ```
    pub fn name(self, name: impl Into<String>) -> Builder<I, (), S, QueueNameSet<S>> {
        Builder {
            builder_state: QueueNameSet {
                state: self.builder_state.state,
                queue_name: name.into(),
            },
            steps: self.steps,
            _marker: PhantomData,
        }
    }
}

impl<I, S> Builder<I, (), S, QueueNameSet<S>>
where
    I: Send + Sync + 'static,
    S: Clone + Send + Sync + 'static,
{
    /// Set the pool of the job's queue.
    ///
    /// This provides the connection pool to the database that the underlying
    /// queue will use for this job.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use std::env;
    ///
    /// use sqlx::PgPool;
    /// use underway::{Job, To};
    /// # use tokio::runtime::Runtime;
    /// # fn main() {
    /// # let rt = Runtime::new().unwrap();
    /// # rt.block_on(async {
    ///
    /// let pool = PgPool::connect(&env::var("DATABASE_URL").unwrap()).await?;
    ///
    /// // Set a pool.
    /// let job_builder = Job::<(), ()>::builder()
    ///     .step(|_cx, _| async move { To::done() })
    ///     .name("example")
    ///     .pool(pool);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # });
    /// # }
    /// ```
    pub fn pool(self, pool: PgPool) -> Builder<I, (), S, PoolSet<S>> {
        let QueueNameSet { queue_name, state } = self.builder_state;
        Builder {
            builder_state: PoolSet {
                state,
                queue_name,
                pool,
            },
            steps: self.steps,
            _marker: PhantomData,
        }
    }
}

impl<I, S> Builder<I, (), S, PoolSet<S>>
where
    I: Send + Sync + 'static,
    S: Clone + Send + Sync + 'static,
{
    /// Finalize the builder into a `Job`.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use std::env;
    ///
    /// use sqlx::PgPool;
    /// use underway::{Job, To};
    ///
    /// # use tokio::runtime::Runtime;
    /// # fn main() {
    /// # let rt = Runtime::new().unwrap();
    /// # rt.block_on(async {
    /// let pool = PgPool::connect(&env::var("DATABASE_URL").unwrap()).await?;
    ///
    /// // Build the job.
    /// let job = Job::<(), ()>::builder()
    ///     .step(|_cx, _| async move { To::done() })
    ///     .name("example")
    ///     .pool(pool)
    ///     .build()
    ///     .await?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # });
    /// # }
    /// ```
    pub async fn build(self) -> Result<Job<I, S>> {
        let PoolSet {
            state,
            queue_name,
            pool,
        } = self.builder_state;
        let queue = Queue::builder().name(queue_name).pool(pool).build().await?;
        Ok(Job {
            queue,
            steps: Arc::new(self.steps),
            state,
            current_index: Arc::new(AtomicUsize::new(0)),
            _marker: PhantomData,
        })
    }
}

// Directly provide queue.
impl<I, S> Builder<I, (), S, StepSet<(), S>>
where
    I: Send + Sync + 'static,
    S: Clone + Send + Sync + 'static,
{
    /// Set the queue.
    ///
    /// This allows providing a `Queue` directly, for situations where the queue
    /// has been defined separately from the job.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use sqlx::PgPool;
    /// use underway::{Job, Queue, To};
    ///
    /// # use tokio::runtime::Runtime;
    /// # fn main() {
    /// # let rt = Runtime::new().unwrap();
    /// # rt.block_on(async {
    /// # let pool = PgPool::connect(&std::env::var("DATABASE_URL")?).await?;
    /// # /*
    /// let pool = { /* A `PgPool`. */ };
    /// # */
    /// #
    /// let queue = Queue::builder().name("example").pool(pool).build().await?;
    ///
    /// // Set a queue.
    /// let job = Job::<(), ()>::builder()
    ///     .step(|_cx, _| async move { To::done() })
    ///     .queue(queue);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # });
    /// # }
    /// ```
    pub fn queue(self, queue: JobQueue<I, S>) -> Builder<I, (), S, QueueSet<I, S>> {
        Builder {
            builder_state: QueueSet {
                state: self.builder_state.state,
                queue,
            },
            steps: self.steps,
            _marker: PhantomData,
        }
    }
}

impl<I, S> Builder<I, (), S, QueueSet<I, S>>
where
    I: Send + Sync + 'static,
    S: Clone + Send + Sync + 'static,
{
    /// Finalize the builder into a `Job`.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use sqlx::PgPool;
    /// use underway::{Job, To, Queue};
    ///
    /// # use tokio::runtime::Runtime;
    /// # fn main() {
    /// # let rt = Runtime::new().unwrap();
    /// # rt.block_on(async {
    /// # let pool = PgPool::connect(&std::env::var("DATABASE_URL")?).await?;
    /// # /*
    /// let pool = { /* A `PgPool`. */ };
    /// # */
    /// #
    /// let queue = Queue::builder().name("example").pool(pool).build().await?;
    ///
    /// // Build the job.
    /// let job = Job::<(), ()>::builder()
    ///     .step(|_cx, _| async move { To::done() })
    ///     .queue(queue)
    ///     .build();
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # });
    /// # }
    pub fn build(self) -> Job<I, S> {
        let QueueSet { state, queue } = self.builder_state;
        Job {
            queue,
            steps: Arc::new(self.steps),
            state,
            current_index: Arc::new(AtomicUsize::new(0)),
            _marker: PhantomData,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use serde::{Deserialize, Serialize};
    use sqlx::PgPool;

    use super::*;
    use crate::queue::graceful_shutdown;

    #[sqlx::test]
    async fn one_step(pool: PgPool) -> sqlx::Result<(), Error> {
        #[derive(Serialize, Deserialize)]
        struct Input {
            message: String,
        }

        let queue = Queue::builder()
            .name("one_step")
            .pool(pool.clone())
            .build()
            .await?;

        let job = Job::builder()
            .step(|_cx, Input { message }| async move {
                println!("Executing job with message: {message}");
                To::done()
            })
            .queue(queue.clone())
            .build();

        assert_eq!(job.retry_policy(), RetryPolicy::default());

        let input = Input {
            message: "Hello, world!".to_string(),
        };
        job.enqueue(&input).await?;

        let pending_task = queue
            .dequeue()
            .await?
            .expect("There should be an enqueued task");

        let job_state: JobState = serde_json::from_value(pending_task.input)?;
        assert_eq!(job_state.step_index, 0);
        assert_eq!(job_state.step_input, serde_json::to_value(&input)?);

        Ok(())
    }

    #[sqlx::test]
    async fn one_step_named(pool: PgPool) -> sqlx::Result<(), Error> {
        #[derive(Serialize, Deserialize)]
        struct Input {
            message: String,
        }

        async fn step(_cx: Context<()>, Input { message }: Input) -> TaskResult<To<()>> {
            println!("Executing job with message: {message}");
            To::done()
        }

        let queue = Queue::builder()
            .name("one_step_named")
            .pool(pool.clone())
            .build()
            .await?;

        let job = Job::builder().step(step).queue(queue.clone()).build();

        assert_eq!(job.retry_policy(), RetryPolicy::default());

        let input = Input {
            message: "Hello, world!".to_string(),
        };
        job.enqueue(&input).await?;

        let pending_task = queue
            .dequeue()
            .await?
            .expect("There should be an enqueued task");

        let job_state: JobState = serde_json::from_value(pending_task.input)?;
        assert_eq!(job_state.step_index, 0);
        assert_eq!(job_state.step_input, serde_json::to_value(&input)?);

        Ok(())
    }

    #[sqlx::test]
    async fn one_step_with_state(pool: PgPool) -> sqlx::Result<(), Error> {
        #[derive(Clone)]
        struct State {
            data: String,
        }

        #[derive(Serialize, Deserialize)]
        struct Input {
            message: String,
        }

        let queue = Queue::builder()
            .name("one_step_with_state")
            .pool(pool.clone())
            .build()
            .await?;

        let job = Job::builder()
            .state(State {
                data: "data".to_string(),
            })
            .step(|cx, Input { message }| async move {
                println!(
                    "Executing job with message: {message} and state: {state}",
                    state = cx.state.data
                );
                To::done()
            })
            .queue(queue.clone())
            .build();

        assert_eq!(job.retry_policy(), RetryPolicy::default());

        let input = Input {
            message: "Hello, world!".to_string(),
        };
        job.enqueue(&input).await?;

        let pending_task = queue
            .dequeue()
            .await?
            .expect("There should be an enqueued task");

        let job_state: JobState = serde_json::from_value(pending_task.input)?;
        assert_eq!(job_state.step_index, 0);
        assert_eq!(job_state.step_input, serde_json::to_value(&input)?);

        Ok(())
    }

    #[sqlx::test]
    async fn one_step_with_mutable_state(pool: PgPool) -> sqlx::Result<(), Error> {
        #[derive(Clone)]
        struct State {
            data: Arc<Mutex<String>>,
        }

        let state = State {
            data: Arc::new(Mutex::new("foo".to_string())),
        };

        let job = Job::builder()
            .state(state.clone())
            .step(|cx, _| async move {
                let mut data = cx.state.data.lock().expect("Mutex should not be poisoned");
                *data = "bar".to_string();
                To::done()
            })
            .name("one_step_with_mutable_state")
            .pool(pool.clone())
            .build()
            .await?;

        job.enqueue(&()).await?;

        job.start();

        // Give the job a moment to process.
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

        assert_eq!(
            *state.data.lock().expect("Mutex should not be poisoned"),
            "bar".to_string()
        );

        // Shutdown and wait for a bit to ensure the test can exit.
        tokio::spawn(async move { graceful_shutdown(&pool).await });
        tokio::time::sleep(std::time::Duration::from_millis(250)).await;

        Ok(())
    }

    #[sqlx::test]
    async fn one_step_with_state_named(pool: PgPool) -> sqlx::Result<(), Error> {
        #[derive(Clone)]
        struct State {
            data: String,
        }

        #[derive(Serialize, Deserialize)]
        struct Input {
            message: String,
        }

        async fn step(cx: Context<State>, Input { message }: Input) -> TaskResult<To<()>> {
            println!(
                "Executing job with message: {message} and state: {data}",
                data = cx.state.data
            );
            To::done()
        }

        let state = State {
            data: "data".to_string(),
        };

        let queue = Queue::builder()
            .name("one_step_named")
            .pool(pool.clone())
            .build()
            .await?;

        let job = Job::builder()
            .state(state)
            .step(step)
            .queue(queue.clone())
            .build();

        assert_eq!(job.retry_policy(), RetryPolicy::default());

        let input = Input {
            message: "Hello, world!".to_string(),
        };
        job.enqueue(&input).await?;

        let pending_task = queue
            .dequeue()
            .await?
            .expect("There should be an enqueued task");

        let job_state: JobState = serde_json::from_value(pending_task.input)?;
        assert_eq!(job_state.step_index, 0);
        assert_eq!(job_state.step_input, serde_json::to_value(&input)?);

        Ok(())
    }

    #[sqlx::test]
    async fn one_step_enqueue(pool: PgPool) -> sqlx::Result<(), Error> {
        #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
        struct Input {
            message: String,
        }

        let queue = Queue::builder()
            .name("one_step_enqueue")
            .pool(pool.clone())
            .build()
            .await?;

        let job = Job::builder()
            .step(|_cx, Input { message }| async move {
                println!("Executing job with message: {message}");
                To::done()
            })
            .queue(queue.clone())
            .build();

        let input = Input {
            message: "Hello, world!".to_string(),
        };
        job.enqueue(&input).await?;

        let pending_task = queue
            .dequeue()
            .await?
            .expect("There should be an enqueued task");

        let job_state: JobState = serde_json::from_value(pending_task.input)?;
        assert_eq!(job_state.step_index, 0);
        assert_eq!(job_state.step_input, serde_json::to_value(&input)?);

        Ok(())
    }

    #[sqlx::test]
    async fn one_step_schedule(pool: PgPool) -> sqlx::Result<(), Error> {
        let queue = Queue::builder()
            .name("one_step_schedule")
            .pool(pool.clone())
            .build()
            .await?;

        let job = Job::builder()
            .step(|_, _| async { To::done() })
            .queue(queue.clone())
            .build();

        let monthly = "@monthly[America/Los_Angeles]"
            .parse()
            .expect("A valid zoned scheduled should be provided");
        job.schedule(&monthly, &()).await?;

        let (schedule, _) = queue
            .task_schedule(&pool)
            .await?
            .expect("A schedule should be set");

        assert_eq!(
            schedule,
            "@monthly[America/Los_Angeles]"
                .parse()
                .expect("A valid zoned scheduled should be provided")
        );

        Ok(())
    }

    #[sqlx::test]
    async fn multi_step(pool: PgPool) -> sqlx::Result<(), Error> {
        #[derive(Serialize, Deserialize)]
        struct Step1 {
            message: String,
        }

        #[derive(Serialize, Deserialize)]
        struct Step2 {
            data: Vec<u8>,
        }

        let queue = Queue::builder()
            .name("multi_step")
            .pool(pool.clone())
            .build()
            .await?;

        let job = Job::builder()
            .step(|_cx, Step1 { message }| async move {
                println!("Executing job with message: {message}");
                To::next(Step2 {
                    data: message.as_bytes().into(),
                })
            })
            .step(|_cx, Step2 { data }| async move {
                println!("Executing job with data: {data:?}");
                To::done()
            })
            .queue(queue.clone())
            .build();

        assert_eq!(job.retry_policy(), RetryPolicy::default());

        let input = Step1 {
            message: "Hello, world!".to_string(),
        };
        job.enqueue(&input).await?;
        let worker = Worker::new(queue.clone(), job.clone());

        // Process the first task.
        worker.process_next_task().await?;

        // Inspect the second task.
        let pending_task = queue
            .dequeue()
            .await?
            .expect("There should be an enqueued task");

        let job_state: JobState = serde_json::from_value(pending_task.input)?;
        assert_eq!(job_state.step_index, 1);
        assert_eq!(
            job_state.step_input,
            serde_json::to_value(&Step2 {
                data: "Hello, world!".as_bytes().to_vec()
            })?
        );

        Ok(())
    }

    #[sqlx::test]
    async fn multi_step_retry_policy(pool: PgPool) -> sqlx::Result<(), Error> {
        #[derive(Serialize, Deserialize)]
        struct Step1 {
            message: String,
        }

        let step1_policy = RetryPolicy::builder().max_attempts(1).build();

        #[derive(Serialize, Deserialize)]
        struct Step2 {
            data: Vec<u8>,
        }

        let step2_policy = RetryPolicy::builder().max_attempts(15).build();

        let queue = Queue::builder()
            .name("multi_step_retry_policy")
            .pool(pool.clone())
            .build()
            .await?;

        let job = Job::builder()
            .step(|_cx, Step1 { message }| async move {
                println!("Executing job with message: {message}");
                To::next(Step2 {
                    data: message.as_bytes().into(),
                })
            })
            .retry_policy(step1_policy)
            .step(|_cx, Step2 { data }| async move {
                println!("Executing job with data: {data:?}");
                To::done()
            })
            .retry_policy(step2_policy)
            .queue(queue.clone())
            .build();

        assert_eq!(job.retry_policy(), step1_policy);

        let input = Step1 {
            message: "Hello, world!".to_string(),
        };
        let enqueued_job = job.enqueue(&input).await?;

        // Dequeue the first task.
        let Some(dequeued_task) = queue.dequeue().await? else {
            panic!("Task should exist");
        };

        assert_eq!(
            enqueued_job.id,
            dequeued_task
                .input
                .get("job_id")
                .cloned()
                .map(serde_json::from_value)
                .expect("Failed to deserialize 'job_id'")?
        );
        assert_eq!(dequeued_task.retry_policy.max_attempts, 1);

        // TODO: This really should be a method on `Queue`.
        //
        // Return the task back to the queue so we can process it with the worker.
        sqlx::query!(
            "update underway.task set state = $2 where id = $1",
            dequeued_task.id as _,
            TaskState::Pending as _
        )
        .execute(&pool)
        .await?;

        // Process the task to ensure the next task is enqueued.
        let worker = {
            let worker_job = job.clone();
            Worker::new(queue.clone(), worker_job)
        };
        worker.process_next_task().await?;

        // Dequeue the second task.
        let Some(dequeued_task) = queue.dequeue().await? else {
            panic!("Next task should exist");
        };

        assert_eq!(dequeued_task.retry_policy.max_attempts, 15);

        Ok(())
    }

    #[sqlx::test]
    async fn multi_step_with_state(pool: PgPool) -> sqlx::Result<(), Error> {
        #[derive(Clone)]
        struct State {
            data: String,
        }

        #[derive(Serialize, Deserialize)]
        struct Step1 {
            message: String,
        }

        #[derive(Serialize, Deserialize)]
        struct Step2 {
            data: Vec<u8>,
        }

        let queue = Queue::builder()
            .name("multi_step_with_state")
            .pool(pool.clone())
            .build()
            .await?;

        let job = Job::builder()
            .state(State {
                data: "data".to_string(),
            })
            .step(|cx, Step1 { message }| async move {
                println!(
                    "Executing job with message: {message} and state: {state}",
                    state = cx.state.data
                );
                To::next(Step2 {
                    data: message.as_bytes().into(),
                })
            })
            .step(|cx, Step2 { data }| async move {
                println!(
                    "Executing job with data: {data:?} and state: {state}",
                    state = cx.state.data
                );
                To::done()
            })
            .queue(queue.clone())
            .build();

        assert_eq!(job.retry_policy(), RetryPolicy::default());

        let input = Step1 {
            message: "Hello, world!".to_string(),
        };
        job.enqueue(&input).await?;
        let worker = Worker::new(queue.clone(), job.clone());

        // Process the first task.
        worker.process_next_task().await?;

        // Inspect the second task.
        let pending_task = queue
            .dequeue()
            .await?
            .expect("There should be an enqueued task");

        let job_state: JobState = serde_json::from_value(pending_task.input)?;
        assert_eq!(job_state.step_index, 1);
        assert_eq!(
            job_state.step_input,
            serde_json::to_value(&Step2 {
                data: "Hello, world!".as_bytes().to_vec()
            })?
        );

        Ok(())
    }

    #[sqlx::test]
    async fn multi_step_enqueue(pool: PgPool) -> sqlx::Result<(), Error> {
        #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
        struct Step1 {
            message: String,
        }

        #[derive(Debug, Serialize, Deserialize, PartialEq)]
        struct Step2 {
            data: Vec<u8>,
        }

        let queue = Queue::builder()
            .name("multi_step_enqueue")
            .pool(pool.clone())
            .build()
            .await?;

        let job = Job::builder()
            .step(|_cx, Step1 { message }| async move {
                println!("Executing job with message: {message}",);
                To::next(Step2 {
                    data: message.as_bytes().into(),
                })
            })
            .step(|_cx, Step2 { data }| async move {
                println!("Executing job with data: {data:?}");
                To::done()
            })
            .queue(queue.clone())
            .build();

        let input = Step1 {
            message: "Hello, world!".to_string(),
        };
        let enqueued_job = job.enqueue(&input).await?;

        // Dequeue the first task.
        let Some(dequeued_task) = queue.dequeue().await? else {
            panic!("Task should exist");
        };

        assert_eq!(
            enqueued_job.id,
            dequeued_task
                .input
                .get("job_id")
                .cloned()
                .map(serde_json::from_value)
                .expect("Failed to deserialize 'job_id'")?
        );

        let job_state: JobState = serde_json::from_value(dequeued_task.input).unwrap();
        assert_eq!(
            JobState {
                step_index: 0,
                step_input: serde_json::to_value(input).unwrap(),
                job_id: job_state.job_id
            },
            job_state
        );

        // TODO: This really should be a method on `Queue`.
        //
        // Return the task back to the queue so we can process it with the worker.
        sqlx::query!(
            "update underway.task set state = $2 where id = $1",
            dequeued_task.id as _,
            TaskState::Pending as _
        )
        .execute(&pool)
        .await?;

        // Process the task to ensure the next task is enqueued.
        let worker = {
            let worker_job = job.clone();
            Worker::new(queue.clone(), worker_job)
        };
        worker.process_next_task().await?;

        // Dequeue the second task.
        let Some(dequeued_task) = queue.dequeue().await? else {
            panic!("Next task should exist");
        };

        let step2_input = Step2 {
            data: "Hello, world!".to_string().as_bytes().into(),
        };
        let job_state: JobState = serde_json::from_value(dequeued_task.input).unwrap();
        assert_eq!(
            JobState {
                step_index: 1,
                step_input: serde_json::to_value(step2_input).unwrap(),
                job_id: job_state.job_id
            },
            job_state
        );

        Ok(())
    }

    #[sqlx::test]
    async fn schedule(pool: PgPool) -> sqlx::Result<(), Error> {
        #[derive(Serialize, Deserialize)]
        struct Input {
            message: String,
        }

        let queue = Queue::builder()
            .name("schedule")
            .pool(pool.clone())
            .build()
            .await?;

        let job = Job::builder()
            .step(|_cx, Input { message }| async move {
                println!("Executing job with message: {message}");
                To::done()
            })
            .queue(queue.clone())
            .build();

        assert_eq!(job.retry_policy(), RetryPolicy::default());

        let daily = "@daily[America/Los_Angeles]"
            .parse()
            .expect("Schedule should parse");
        let input = Input {
            message: "Hello, world!".to_string(),
        };
        job.schedule(&daily, &input).await?;

        let (zoned_schedule, schedule_input) = queue
            .task_schedule(&pool)
            .await?
            .expect("Schedule should be set");

        assert_eq!(zoned_schedule, daily);
        assert_eq!(schedule_input.step_index, 0);
        assert_eq!(schedule_input.step_input, serde_json::to_value(input)?);

        Ok(())
    }

    #[sqlx::test]
    async fn unschedule(pool: PgPool) -> sqlx::Result<(), Error> {
        #[derive(Serialize, Deserialize)]
        struct Input {
            message: String,
        }

        let queue = Queue::builder()
            .name("unschedule")
            .pool(pool.clone())
            .build()
            .await?;

        let job = Job::builder()
            .step(|_cx, Input { message }| async move {
                println!("Executing job with message: {message}");
                To::done()
            })
            .queue(queue.clone())
            .build();

        assert_eq!(job.retry_policy(), RetryPolicy::default());

        let daily = "@daily[America/Los_Angeles]"
            .parse()
            .expect("Schedule should parse");
        let input = Input {
            message: "Hello, world!".to_string(),
        };
        job.schedule(&daily, &input).await?;
        job.unschedule().await?;

        assert!(queue.task_schedule(&pool).await?.is_none());

        Ok(())
    }

    #[sqlx::test]
    async fn unschedule_without_schedule(pool: PgPool) -> sqlx::Result<(), Error> {
        #[derive(Serialize, Deserialize)]
        struct Input {
            message: String,
        }

        let queue = Queue::builder()
            .name("unschedule_without_schedule")
            .pool(pool.clone())
            .build()
            .await?;

        let job = Job::builder()
            .step(|_cx, Input { message }| async move {
                println!("Executing job with message: {message}");
                To::done()
            })
            .queue(queue.clone())
            .build();

        assert_eq!(job.retry_policy(), RetryPolicy::default());

        assert!(job.unschedule().await.is_ok());
        assert!(queue.task_schedule(&pool).await?.is_none());

        Ok(())
    }

    #[sqlx::test]
    async fn enqueued_job_cancel(pool: PgPool) -> sqlx::Result<(), Error> {
        let queue = Queue::builder()
            .name("enqueued_job_cancel")
            .pool(pool.clone())
            .build()
            .await?;

        let job = Job::builder()
            .step(|_cx, _| async move { To::done() })
            .queue(queue.clone())
            .build();

        let enqueued_job = job.enqueue(&()).await?;

        // Should return `true`.
        assert!(enqueued_job.cancel().await?);

        let task = sqlx::query!(
            r#"
            select state as "state: TaskState"
            from underway.task
            where input->>'job_id' = $1
            "#,
            enqueued_job.id.to_string()
        )
        .fetch_one(&pool)
        .await?;

        assert_eq!(task.state, TaskState::Cancelled);

        // Should return `false` since the job is already cancelled.
        assert!(!enqueued_job.cancel().await?);

        Ok(())
    }
}
