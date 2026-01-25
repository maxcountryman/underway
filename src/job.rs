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
//! use underway::{job::StepContext, task::Result as TaskResult, Job, To};
//!
//! async fn named_step(_cx: StepContext<()>, _: ()) -> TaskResult<To<()>> {
//!     To::done()
//! }
//!
//! let job_builder = Job::<_, ()>::builder().step(named_step);
//! ```
//!
//! Notice that the first argument to our function is [a context
//! binding](crate::job::StepContext). This provides access to fields like
//! [`state`](crate::job::StepContext::state),
//! [`step_index`](crate::job::StepContext::step_index),
//! and [`job_id`](crate::job::StepContext::job_id).
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
//! use underway::{job::StepContext, Job, To};
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
//!     .step(|StepContext { state, .. }, _| async move {
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
//! # Side effects
//!
//! Steps should focus on durable state transitions. When a step needs to
//! perform an external side effect (HTTP calls, emails, etc.), return
//! [`To::effect`] and add an effect handler. Effects run on a dedicated queue
//! after the step's transaction commits, and only then enqueue the next step.
//! Effect handlers should be idempotent since they may be retried.
//!
//! ```rust
//! use serde::{Deserialize, Serialize};
//! use underway::{Job, To};
//!
//! #[derive(Serialize, Deserialize)]
//! struct UserSub {
//!     user_id: i64,
//! }
//!
//! #[derive(Serialize, Deserialize)]
//! struct SendEmail {
//!     user_id: i64,
//! }
//!
//! #[derive(Serialize, Deserialize)]
//! struct FinishSub {
//!     user_id: i64,
//! }
//!
//! let job_builder = Job::<_, ()>::builder()
//!     .step(|_cx, UserSub { user_id }| async move {
//!         // Persist state to the database here.
//!         To::effect(SendEmail { user_id })
//!     })
//!     .effect(|_cx, SendEmail { user_id }| async move {
//!         // Perform the external side effect.
//!         println!("Sending email for user {user_id}");
//!         To::next(FinishSub { user_id })
//!     })
//!     .step(|_cx, FinishSub { user_id }| async move {
//!         println!("Finished subscription for {user_id}");
//!         To::done()
//!     });
//!
//! # Retry policies
//!
//! Steps being tasks also have associated retry policies. This policy inherits
//! the default but can be provided for each step.
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
//! # Step configuration
//!
//! Steps and effects can configure the same task-level settings as standalone
//! tasks. Each step or effect can set its timeout, TTL, delay, heartbeat
//! interval, concurrency key, and priority. Base delays are added to any delay
//! returned by [`To::delay_for`].
//! ```rust
//! use jiff::ToSpan;
//! use underway::{Job, To};
//!
//! let job_builder = Job::<(), ()>::builder()
//!     .step(|_cx, _| async move { To::done() })
//!     .timeout(2.minutes())
//!     .ttl(7.days())
//!     .delay(10.seconds())
//!     .heartbeat(5.seconds())
//!     .concurrency_key("customer:42")
//!     .priority(10);
//! ```
//! 
//! # Enqueuing jobs
//!
//! Once we've configured our job with its sequence of one or more steps we can
//! build the job and enqueue it with input.
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
//! job's queue. The job build still awaits to create the effect queue.
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
//!     .build()
//!     .await?;
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
//! # Running jobs
//!
//! Jobs are run via workers and schedulers, where the former processes tasks
//! and the latter processes schedules for enqueuing tasks. Starting both is
//! encapsulated by the job interface.
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
//! let job = Job::builder()
//!     .step(|_cx, _: ()| async move { To::done() })
//!     .name("example-job")
//!     .pool(pool)
//!     .build()
//!     .await?;
//!
//! let worker = job.worker();
//! let scheduler = job.scheduler();
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! # });
//! # }
//! ```
//! 
//! # Scheduling jobs
//!
//! Jobs may also be run on a schedule that follows the form of a cron-like
//! expression.
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
    future::Future, marker::PhantomData, ops::Deref, pin::Pin, result::Result as StdResult,
    sync::Arc, task::Poll,
};

use builder_states::{Initial, PoolSet, QueueNameSet, QueueSet, StateSet, StepSet};
use jiff::{Span, ToSpan};
use sealed::{EffectState, JobState};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use sqlx::{PgExecutor, PgPool, Postgres, Transaction};
use tokio::task::{JoinError, JoinSet};
use tokio_util::sync::CancellationToken;
use tracing::instrument;
use ulid::Ulid;
use uuid::Uuid;

use crate::{
    queue::{Error as QueueError, InProgressTask, Queue},
    scheduler::{Error as SchedulerError, Scheduler, ZonedSchedule},
    task::{
        Error as TaskError, Result as TaskResult, RetryPolicy, State as TaskState, Task, TaskId,
    },
    worker::{Error as WorkerError, Worker},
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
type EffectQueue<T, S> = Queue<EffectTask<T, S>>;

/// Step context passed in to each step.
pub struct StepContext<S> {
    /// Shared step state.
    ///
    /// This value is set via [`state`](Builder::state).
    ///
    /// **Note:** State is not persisted and therefore should not be
    /// relied on when durability is needed.
    pub state: S,

    /// Current index of the step being executed zero-based.
    ///
    /// In multi-step job definitions, this points to the current step the job
    /// is processing currently.
    pub step_index: usize,

    /// Total steps count.
    ///
    /// The number of steps in this job definition.
    pub step_count: usize,

    /// This `JobId`.
    pub job_id: JobId,

    /// Queue name.
    ///
    /// Name of the queue the current step is currently running on.
    pub queue_name: String,
}

/// Effect context passed in to each effect handler.
pub struct EffectContext<S> {
    /// Shared step state.
    ///
    /// This value is set via [`state`](Builder::state).
    ///
    /// **Note:** State is not persisted and therefore should not be
    /// relied on when durability is needed.
    pub state: S,

    /// Current index of the step that produced this effect, zero-based.
    pub step_index: usize,

    /// Total steps count.
    ///
    /// The number of steps in this job definition.
    pub step_count: usize,

    /// Current index of the effect being executed, zero-based.
    pub effect_index: usize,

    /// This `JobId`.
    pub job_id: JobId,

    /// Queue name.
    ///
    /// Name of the queue the job step is currently running on.
    pub queue_name: String,

    /// Effect queue name.
    ///
    /// Name of the queue the effect handler is running on.
    pub effect_queue_name: String,
}

struct StepConfig<S> {
    executor: Box<dyn StepExecutor<S>>,
    task_config: StepTaskConfig,
    next_effect_index: Option<usize>,
}

struct EffectConfig<S> {
    executor: Box<dyn EffectExecutor<S>>,
    task_config: StepTaskConfig,
    next_effect_index: Option<usize>,
}

#[derive(Clone)]
struct StepTaskConfig {
    retry_policy: RetryPolicy,
    timeout: Span,
    ttl: Span,
    delay: Span,
    heartbeat: Span,
    concurrency_key: Option<String>,
    priority: i32,
}

impl Default for StepTaskConfig {
    fn default() -> Self {
        Self {
            retry_policy: RetryPolicy::default(),
            timeout: 15.minutes(),
            ttl: 14.days(),
            delay: Span::new(),
            heartbeat: 30.seconds(),
            concurrency_key: None,
            priority: 0,
        }
    }
}

mod sealed {
    use serde::{Deserialize, Serialize};

    use super::JobId;

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    pub struct JobState {
        pub step_index: usize,
        pub step_input: serde_json::Value,
        pub(crate) job_id: JobId,
    } // TODO: Versioning?

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    pub struct EffectState {
        pub effect_index: usize,
        pub step_index: usize,
        pub effect_input: serde_json::Value,
        pub(crate) job_id: JobId,
    }
}

enum StageInput {
    Step(JobState),
    Effect(EffectState),
}

trait TaskConfig {
    type Input;

    fn task_config_default(&self) -> StepTaskConfig;
    fn task_config_for(&self, input: &Self::Input) -> StepTaskConfig;
}

macro_rules! impl_task_config {
    () => {
        fn retry_policy(&self) -> RetryPolicy {
            self.task_config_default().retry_policy
        }

        fn timeout(&self) -> Span {
            self.task_config_default().timeout
        }

        fn ttl(&self) -> Span {
            self.task_config_default().ttl
        }

        fn delay(&self) -> Span {
            self.task_config_default().delay
        }

        fn heartbeat(&self) -> Span {
            self.task_config_default().heartbeat
        }

        fn concurrency_key(&self) -> Option<String> {
            self.task_config_default().concurrency_key
        }

        fn priority(&self) -> i32 {
            self.task_config_default().priority
        }

        fn retry_policy_for(&self, input: &Self::Input) -> RetryPolicy {
            self.task_config_for(input).retry_policy
        }

        fn timeout_for(&self, input: &Self::Input) -> Span {
            self.task_config_for(input).timeout
        }

        fn ttl_for(&self, input: &Self::Input) -> Span {
            self.task_config_for(input).ttl
        }

        fn delay_for(&self, input: &Self::Input) -> Span {
            self.task_config_for(input).delay
        }

        fn heartbeat_for(&self, input: &Self::Input) -> Span {
            self.task_config_for(input).heartbeat
        }

        fn concurrency_key_for(&self, input: &Self::Input) -> Option<String> {
            self.task_config_for(input).concurrency_key
        }

        fn priority_for(&self, input: &Self::Input) -> i32 {
            self.task_config_for(input).priority
        }
    };
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
pub struct JobId(Uuid);

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
    queue: Arc<Queue<T>>,
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
              concurrency_key,
              0 as "attempt_number!"
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
    queue: Arc<JobQueue<I, S>>,
    effect_queue: Arc<EffectQueue<I, S>>,
    steps: Arc<Vec<StepConfig<S>>>,
    effects: Arc<Vec<EffectConfig<S>>>,
    state: S,
    _marker: PhantomData<I>,
}

struct EffectTask<I, S>
where
    I: Send + Sync + 'static,
    S: Clone + Send + Sync + 'static,
{
    job: Job<I, S>,
}

/// Worker wrapper for job effect handlers.
pub struct EffectWorker<I, S>
where
    I: Send + Sync + 'static,
    S: Clone + Send + Sync + 'static,
{
    worker: Worker<EffectTask<I, S>>,
}

impl<I, S> Clone for EffectWorker<I, S>
where
    I: Send + Sync + 'static,
    S: Clone + Send + Sync + 'static,
{
    fn clone(&self) -> Self {
        Self {
            worker: self.worker.clone(),
        }
    }
}

impl<I, S> EffectWorker<I, S>
where
    I: Send + Sync + 'static,
    S: Clone + Send + Sync + 'static,
{
    fn new(worker: Worker<EffectTask<I, S>>) -> Self {
        Self { worker }
    }

    /// Sets the concurrency limit for this worker.
    pub fn set_concurrency_limit(&mut self, concurrency_limit: usize) {
        self.worker.set_concurrency_limit(concurrency_limit);
    }

    /// Sets the shutdown token.
    pub fn set_shutdown_token(&mut self, shutdown_token: CancellationToken) {
        self.worker.set_shutdown_token(shutdown_token);
    }

    /// Sets the backoff policy for PostgreSQL reconnection attempts.
    pub fn set_reconnect_backoff(&mut self, backoff: RetryPolicy) {
        self.worker.set_reconnect_backoff(backoff);
    }

    /// Cancels the shutdown token and begins a graceful shutdown of in-progress
    /// tasks.
    pub fn shutdown(&self) {
        self.worker.shutdown();
    }

    /// Runs the worker, processing tasks as they become available.
    pub async fn run(&self) -> std::result::Result<(), WorkerError> {
        self.worker.run().await
    }

    /// Same as `run` but allows configuration of the delay between polls.
    pub async fn run_every(&self, period: Span) -> std::result::Result<(), WorkerError> {
        self.worker.run_every(period).await
    }

    /// Processes the next available task in the queue.
    pub async fn process_next_task(&self) -> std::result::Result<Option<TaskId>, WorkerError> {
        self.worker.process_next_task().await
    }
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

    /// Enqueue the job multiple times using a connection from the queue's pool.
    ///
    /// # Errors
    ///
    /// This has the same error conditions as [`Queue::enqueue_many`].
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
    /// // Enqueue a few jobs at once.
    /// let enqueued = job.enqueue_many(&[(), ()]).await?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # });
    /// # }
    /// ```
    pub async fn enqueue_many(&self, inputs: &[I]) -> Result<Vec<EnqueuedJob<Self>>> {
        let mut conn = self.queue.pool.acquire().await?;
        self.enqueue_many_using(&mut *conn, inputs).await
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

    /// Enqueue the job multiple times using the provided executor.
    ///
    /// This allows jobs to be enqueued using the same transaction as an
    /// application may already be using in a given context.
    ///
    /// **Note:** If you pass a transactional executor and the transaction is
    /// rolled back, the returned job IDs will not correspond to any persisted
    /// tasks.
    ///
    /// # Errors
    ///
    /// This has the same error conditions as [`Queue::enqueue_many`].
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
    /// let enqueued = job.enqueue_many_using(&mut *tx, &[(), ()]).await?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # });
    /// # }
    /// ```
    pub async fn enqueue_many_using<'a, E>(
        &self,
        executor: E,
        inputs: &[I],
    ) -> Result<Vec<EnqueuedJob<Self>>>
    where
        E: PgExecutor<'a> + sqlx::Acquire<'a, Database = sqlx::Postgres>,
    {
        if inputs.is_empty() {
            return Ok(Vec::new());
        }

        let job_inputs = inputs
            .iter()
            .map(|input| self.first_job_input(input))
            .collect::<Result<Vec<_>>>()?;

        self.queue.enqueue_many(executor, self, &job_inputs).await?;

        let enqueued = job_inputs
            .into_iter()
            .map(|job_input| EnqueuedJob {
                id: job_input.job_id,
                queue: self.queue.clone(),
            })
            .collect();

        Ok(enqueued)
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

    /// Returns this job's `Queue`.
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
    /// let job_queue = job.queue();
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # });
    /// # }
    pub fn queue(&self) -> Arc<Queue<Self>> {
        Arc::clone(&self.queue)
    }

    fn effect_queue(&self) -> Arc<Queue<EffectTask<I, S>>> {
        Arc::clone(&self.effect_queue)
    }

    /// Creates a `Worker` for this job.
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
    /// let job_worker = job.worker();
    /// job_worker.run().await?; // Run the worker directly.
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # });
    /// # }
    pub fn worker(&self) -> Worker<Self> {
        Worker::new(self.queue(), self.clone())
    }

    /// Creates a `Worker` for this job's effects.
    pub fn effect_worker(&self) -> EffectWorker<I, S> {
        EffectWorker::new(Worker::new(self.effect_queue(), self.effect_task()))
    }

    /// Creates a `Scheduler` for this job.
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
    /// let job_scheduler = job.scheduler();
    /// job_scheduler.run().await?; // Run the scheduler directly.
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # });
    /// # }
    pub fn scheduler(&self) -> Scheduler<Self> {
        Scheduler::new(self.queue(), self.clone())
    }

    /// Runs the worker, effect worker, and scheduler for the job.
    ///
    /// # Errors
    ///
    /// This has the same error conditions as [`Worker::run`] and
    /// [`Scheduler::run`]. It will also return an error if either of the
    /// spawned workers or scheduler cannot be joined.
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
    pub async fn run(&self) -> Result {
        let worker = self.worker();
        let effect_worker = self.effect_worker();
        let scheduler = self.scheduler();

        let mut workers = JoinSet::new();
        workers.spawn(async move { worker.run().await.map_err(Error::from) });
        workers.spawn(async move { effect_worker.run().await.map_err(Error::from) });
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

    /// Starts the worker, effect worker, and scheduler for the job and returns
    /// a handle.
    ///
    /// The returned handle may be used to gracefully shutdown the worker and
    /// scheduler.
    ///
    /// # Errors
    ///
    /// This has the same error conditions as [`Worker::run`] and
    /// [`Scheduler::run`]. It will also return an error if either of the
    /// spawned workers or scheduler cannot be joined.
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
    pub fn start(&self) -> JobHandle {
        let shutdown_token = CancellationToken::new();
        let mut workers = JoinSet::new();

        let mut worker = self.worker();
        worker.set_shutdown_token(shutdown_token.clone());
        let mut effect_worker = self.effect_worker();
        effect_worker.set_shutdown_token(shutdown_token.clone());
        let mut scheduler = self.scheduler();
        scheduler.set_shutdown_token(shutdown_token.clone());

        // Spawn the tasks using `tokio::spawn` to decouple them from polling the
        // `Future`.
        let worker_handle = tokio::spawn(async move { worker.run().await.map_err(Error::from) });
        let effect_handle =
            tokio::spawn(async move { effect_worker.run().await.map_err(Error::from) });
        let scheduler_handle =
            tokio::spawn(async move { scheduler.run().await.map_err(Error::from) });

        workers.spawn(worker_handle);
        workers.spawn(effect_handle);
        workers.spawn(scheduler_handle);

        JobHandle {
            workers,
            shutdown_token,
        }
    }

    fn first_job_input(&self, input: &I) -> Result<JobState> {
        let step_input = serde_json::to_value(input)?;
        let step_index = 0;
        let job_id = JobId::new();
        Ok(JobState {
            step_input,
            step_index,
            job_id,
        })
    }
}

impl<I, S> Job<I, S>
where
    I: Send + Sync + 'static,
    S: Clone + Send + Sync + 'static,
{
    fn effect_task(&self) -> EffectTask<I, S> {
        EffectTask { job: self.clone() }
    }

    fn step_task_config(&self, step_index: usize) -> StepTaskConfig {
        self.steps
            .get(step_index)
            .map(|step| step.task_config.clone())
            .unwrap_or_default()
    }

    fn effect_task_config(&self, effect_index: usize) -> StepTaskConfig {
        self.effects
            .get(effect_index)
            .map(|effect| effect.task_config.clone())
            .unwrap_or_default()
    }

    fn step_context(&self, job_id: JobId, step_index: usize) -> StepContext<S> {
        StepContext {
            state: self.state.clone(),
            step_index,
            step_count: self.steps.len(),
            job_id,
            queue_name: self.queue.name.clone(),
        }
    }

    fn effect_context(
        &self,
        job_id: JobId,
        step_index: usize,
        effect_index: usize,
    ) -> EffectContext<S> {
        EffectContext {
            state: self.state.clone(),
            step_index,
            step_count: self.steps.len(),
            effect_index,
            job_id,
            queue_name: self.queue.name.clone(),
            effect_queue_name: self.effect_queue.name.clone(),
        }
    }

    fn validate_step_index(&self, step_index: usize) -> TaskResult<()> {
        if step_index >= self.steps.len() {
            return Err(TaskError::Fatal("Invalid step index.".into()));
        }

        Ok(())
    }

    fn validate_effect_index(&self, effect_index: usize) -> TaskResult<()> {
        if effect_index >= self.effects.len() {
            return Err(TaskError::Fatal("Invalid effect index.".into()));
        }

        Ok(())
    }

    async fn enqueue_transition(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        job_id: JobId,
        step_index: usize,
        next_effect_index: Option<usize>,
        next_input: serde_json::Value,
        delay: Span,
    ) -> TaskResult<()> {
        let executor = &mut **tx;

        if let Some(effect_index) = next_effect_index {
            self.validate_effect_index(effect_index)?;

            let effect_task = self.effect_task();
            let effect_input = EffectState {
                effect_index,
                step_index,
                effect_input: next_input,
                job_id,
            };

            self.effect_queue
                .enqueue_after(executor, &effect_task, &effect_input, delay)
                .await
                .map_err(|err| TaskError::Retryable(err.to_string()))?;
        } else {
            let next_index = step_index + 1;
            let next_job_input = JobState {
                step_input: next_input,
                step_index: next_index,
                job_id,
            };

            self.queue
                .enqueue_after(executor, self, &next_job_input, delay)
                .await
                .map_err(|err| TaskError::Retryable(err.to_string()))?;
        }

        Ok(())
    }

    async fn execute_stage(
        &self,
        mut tx: Transaction<'_, Postgres>,
        input: StageInput,
    ) -> TaskResult<()> {
        let (step_index, job_id, next_effect_index, stage_result) = match input {
            StageInput::Step(JobState {
                step_index,
                step_input,
                job_id,
            }) => {
                self.validate_step_index(step_index)?;

                let step_config = &self.steps[step_index];
                let cx = self.step_context(job_id, step_index);
                let stage_result = match step_config.executor.execute_step(cx, step_input).await {
                    Ok(result) => result,
                    Err(err) => {
                        tx.commit().await?;
                        return Err(err);
                    }
                };

                (
                    step_index,
                    job_id,
                    step_config.next_effect_index,
                    stage_result,
                )
            }
            StageInput::Effect(EffectState {
                effect_index,
                step_index,
                effect_input,
                job_id,
            }) => {
                self.validate_step_index(step_index)?;
                self.validate_effect_index(effect_index)?;

                let effect_config = &self.effects[effect_index];
                let cx = self.effect_context(job_id, step_index, effect_index);
                let stage_result = match effect_config
                    .executor
                    .execute_effect(cx, effect_input)
                    .await
                {
                    Ok(result) => result,
                    Err(err) => {
                        tx.commit().await?;
                        return Err(err);
                    }
                };

                (
                    step_index,
                    job_id,
                    effect_config.next_effect_index,
                    stage_result,
                )
            }
        };

        if let Some((next_input, delay)) = stage_result {
            self.enqueue_transition(
                &mut tx,
                job_id,
                step_index,
                next_effect_index,
                next_input,
                delay,
            )
            .await?;
        }

        tx.commit().await?;

        Ok(())
    }
}

impl<I, S> TaskConfig for Job<I, S>
where
    I: Send + Sync + 'static,
    S: Clone + Send + Sync + 'static,
{
    type Input = JobState;

    fn task_config_default(&self) -> StepTaskConfig {
        self.step_task_config(0)
    }

    fn task_config_for(&self, input: &Self::Input) -> StepTaskConfig {
        self.step_task_config(input.step_index)
    }
}

impl<I, S> TaskConfig for EffectTask<I, S>
where
    I: Send + Sync + 'static,
    S: Clone + Send + Sync + 'static,
{
    type Input = EffectState;

    fn task_config_default(&self) -> StepTaskConfig {
        self.job.effect_task_config(0)
    }

    fn task_config_for(&self, input: &Self::Input) -> StepTaskConfig {
        self.job.effect_task_config(input.effect_index)
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
        tx: Transaction<'_, Postgres>,
        input: Self::Input,
    ) -> TaskResult<Self::Output> {
        self.execute_stage(tx, StageInput::Step(input)).await
    }

    impl_task_config!();
}

impl<I, S> Task for EffectTask<I, S>
where
    I: Send + Sync + 'static,
    S: Clone + Send + Sync + 'static,
{
    type Input = EffectState;
    type Output = ();

    #[instrument(
        skip_all,
        fields(
            job.id = %input.job_id.as_hyphenated(),
            step = input.step_index + 1,
            effect = input.effect_index + 1,
            steps = self.job.steps.len()
        ),
        err
    )]
    async fn execute(
        &self,
        tx: Transaction<'_, Postgres>,
        input: Self::Input,
    ) -> TaskResult<Self::Output> {
        self.job.execute_stage(tx, StageInput::Effect(input)).await
    }

    impl_task_config!();
}

impl<I, S> Clone for Job<I, S>
where
    I: Send + Sync + 'static,
    S: Clone + Send + Sync + 'static,
{
    fn clone(&self) -> Self {
        Self {
            queue: self.queue.clone(),
            effect_queue: self.effect_queue.clone(),
            state: self.state.clone(),
            steps: self.steps.clone(),
            effects: self.effects.clone(),
            _marker: PhantomData,
        }
    }
}

/// Marker type for effect inputs.
#[derive(Debug, Deserialize, Serialize)]
#[serde(transparent)]
pub struct Effect<E>(
    /// The effect payload.
    pub E,
);

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

impl<E> To<Effect<E>> {
    /// Transitions to an effect handler.
    pub fn effect(effect: E) -> TaskResult<Self> {
        Ok(Self::Next(Effect(effect)))
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
    F: Fn(StepContext<S>, I) -> Pin<Box<dyn Future<Output = TaskResult<To<O>>> + Send>>
        + Send
        + Sync
        + 'static,
{
    func: Arc<F>,
    _marker: PhantomData<(I, O, S)>,
}

impl<I, O, S, F> StepFn<I, O, S, F>
where
    F: Fn(StepContext<S>, I) -> Pin<Box<dyn Future<Output = TaskResult<To<O>>> + Send>>
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

// A concrete implementation of an effect using a closure.
struct EffectFn<I, O, S, F>
where
    F: Fn(EffectContext<S>, I) -> Pin<Box<dyn Future<Output = TaskResult<To<O>>> + Send>>
        + Send
        + Sync
        + 'static,
{
    func: Arc<F>,
    _marker: PhantomData<(I, O, S)>,
}

impl<I, O, S, F> EffectFn<I, O, S, F>
where
    F: Fn(EffectContext<S>, I) -> Pin<Box<dyn Future<Output = TaskResult<To<O>>> + Send>>
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
        cx: StepContext<S>,
        input: serde_json::Value,
    ) -> Pin<Box<dyn Future<Output = StepResult> + Send>>;
}

// A trait object wrapper for effects to allow heterogeneous effect types in a
// vector.
trait EffectExecutor<S>: Send + Sync {
    // Execute the effect with the given input serialized as JSON.
    fn execute_effect(
        &self,
        cx: EffectContext<S>,
        input: serde_json::Value,
    ) -> Pin<Box<dyn Future<Output = StepResult> + Send>>;
}

impl<I, O, S, F> StepExecutor<S> for StepFn<I, O, S, F>
where
    I: DeserializeOwned + Serialize + Send + Sync + 'static,
    O: Serialize + Send + Sync + 'static,
    S: Send + Sync + 'static,
    F: Fn(StepContext<S>, I) -> Pin<Box<dyn Future<Output = TaskResult<To<O>>> + Send>>
        + Send
        + Sync
        + 'static,
{
    fn execute_step(
        &self,
        cx: StepContext<S>,
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

impl<I, O, S, F> EffectExecutor<S> for EffectFn<I, O, S, F>
where
    I: DeserializeOwned + Serialize + Send + Sync + 'static,
    O: Serialize + Send + Sync + 'static,
    S: Send + Sync + 'static,
    F: Fn(EffectContext<S>, I) -> Pin<Box<dyn Future<Output = TaskResult<To<O>>> + Send>>
        + Send
        + Sync
        + 'static,
{
    fn execute_effect(
        &self,
        cx: EffectContext<S>,
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

#[derive(Clone, Copy)]
enum ConfigTarget {
    Step(usize),
    Effect(usize),
}

/// Builder for constructing a `Job` with a sequence of steps.
pub struct Builder<I, O, S, B> {
    builder_state: B,
    steps: Vec<StepConfig<S>>,
    effects: Vec<EffectConfig<S>>,
    last_config: Option<ConfigTarget>,
    effect_queue_name: Option<String>,
    _marker: PhantomData<(I, O, S)>,
}

impl<I, S> Default for Builder<I, I, S, Initial> {
    fn default() -> Self {
        Self::new()
    }
}

impl<I, O, S, B> Builder<I, O, S, B> {
    /// Sets the queue name for effects.
    ///
    /// Defaults to `"<queue_name>__effects"` when not provided.
    pub fn effect_queue_name(mut self, name: impl Into<String>) -> Self {
        self.effect_queue_name = Some(name.into());
        self
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
            effects: Vec::new(),
            last_config: None,
            effect_queue_name: None,
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
            effects: self.effects,
            last_config: self.last_config,
            effect_queue_name: self.effect_queue_name,
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
        F: Fn(StepContext<S>, I) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = TaskResult<To<O>>> + Send + 'static,
    {
        let step_fn = StepFn::new(move |cx, input| Box::pin(func(cx, input)));
        let step_index = self.steps.len();
        self.steps.push(StepConfig {
            executor: Box::new(step_fn),
            task_config: StepTaskConfig::default(),
            next_effect_index: None,
        });
        self.last_config = Some(ConfigTarget::Step(step_index));

        Builder {
            builder_state: StepSet {
                state: (),
                _marker: PhantomData,
            },
            steps: self.steps,
            effects: self.effects,
            last_config: self.last_config,
            effect_queue_name: self.effect_queue_name,
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
        F: Fn(StepContext<S>, I) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = TaskResult<To<O>>> + Send + 'static,
    {
        let step_fn = StepFn::new(move |cx, input| Box::pin(func(cx, input)));
        let step_index = self.steps.len();
        self.steps.push(StepConfig {
            executor: Box::new(step_fn),
            task_config: StepTaskConfig::default(),
            next_effect_index: None,
        });
        self.last_config = Some(ConfigTarget::Step(step_index));

        Builder {
            builder_state: StepSet {
                state: self.builder_state.state,
                _marker: PhantomData,
            },
            steps: self.steps,
            effects: self.effects,
            last_config: self.last_config,
            effect_queue_name: self.effect_queue_name,
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
        F: Fn(StepContext<S>, Current) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = TaskResult<To<New>>> + Send + 'static,
    {
        let step_fn = StepFn::new(move |cx, input| Box::pin(func(cx, input)));
        let step_index = self.steps.len();
        self.steps.push(StepConfig {
            executor: Box::new(step_fn),
            task_config: StepTaskConfig::default(),
            next_effect_index: None,
        });
        self.last_config = Some(ConfigTarget::Step(step_index));

        Builder {
            builder_state: StepSet {
                state: self.builder_state.state,
                _marker: PhantomData,
            },
            steps: self.steps,
            effects: self.effects,
            last_config: self.last_config,
            effect_queue_name: self.effect_queue_name,
            _marker: PhantomData,
        }
    }

    fn update_task_config(&mut self, update: impl FnOnce(&mut StepTaskConfig)) {
        let target = self.last_config.expect("Steps should not be empty");

        match target {
            ConfigTarget::Step(step_index) => {
                let step_config = self
                    .steps
                    .get_mut(step_index)
                    .expect("Step config should exist");
                update(&mut step_config.task_config);
            }
            ConfigTarget::Effect(effect_index) => {
                let effect_config = self
                    .effects
                    .get_mut(effect_index)
                    .expect("Effect config should exist");
                update(&mut effect_config.task_config);
            }
        }
    }

    /// Sets the retry policy of the previous step or effect.
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
        self.update_task_config(|task_config| task_config.retry_policy = retry_policy);
        self
    }

    /// Sets the timeout of the previous step or effect.
    ///
    /// # Example
    ///
    /// ```rust
    /// use jiff::ToSpan;
    /// use underway::{Job, To};
    ///
    /// let job_builder = Job::<(), ()>::builder()
    ///     .step(|_cx, _| async move { To::done() })
    ///     .timeout(1.minute());
    /// ```
    pub fn timeout(mut self, timeout: Span) -> Builder<I, Current, S, StepSet<Current, S>> {
        self.update_task_config(|task_config| task_config.timeout = timeout);
        self
    }

    /// Sets the TTL of the previous step or effect.
    ///
    /// # Example
    ///
    /// ```rust
    /// use jiff::ToSpan;
    /// use underway::{Job, To};
    ///
    /// let job_builder = Job::<(), ()>::builder()
    ///     .step(|_cx, _| async move { To::done() })
    ///     .ttl(7.days());
    /// ```
    pub fn ttl(mut self, ttl: Span) -> Builder<I, Current, S, StepSet<Current, S>> {
        self.update_task_config(|task_config| task_config.ttl = ttl);
        self
    }

    /// Sets a base delay before the previous step or effect can be dequeued.
    ///
    /// This delay is added to any delay specified by [`To::delay_for`].
    ///
    /// # Example
    ///
    /// ```rust
    /// use jiff::ToSpan;
    /// use underway::{Job, To};
    ///
    /// let job_builder = Job::<(), ()>::builder()
    ///     .step(|_cx, _| async move { To::done() })
    ///     .delay(30.seconds());
    /// ```
    pub fn delay(mut self, delay: Span) -> Builder<I, Current, S, StepSet<Current, S>> {
        self.update_task_config(|task_config| task_config.delay = delay);
        self
    }

    /// Sets the heartbeat interval of the previous step or effect.
    ///
    /// # Example
    ///
    /// ```rust
    /// use jiff::ToSpan;
    /// use underway::{Job, To};
    ///
    /// let job_builder = Job::<(), ()>::builder()
    ///     .step(|_cx, _| async move { To::done() })
    ///     .heartbeat(5.seconds());
    /// ```
    pub fn heartbeat(mut self, heartbeat: Span) -> Builder<I, Current, S, StepSet<Current, S>> {
        self.update_task_config(|task_config| task_config.heartbeat = heartbeat);
        self
    }

    /// Sets the concurrency key of the previous step or effect.
    ///
    /// # Example
    ///
    /// ```rust
    /// use underway::{Job, To};
    ///
    /// let job_builder = Job::<(), ()>::builder()
    ///     .step(|_cx, _| async move { To::done() })
    ///     .concurrency_key("customer:42");
    /// ```
    pub fn concurrency_key(
        mut self,
        concurrency_key: impl Into<String>,
    ) -> Builder<I, Current, S, StepSet<Current, S>> {
        let concurrency_key = concurrency_key.into();
        self.update_task_config(|task_config| task_config.concurrency_key = Some(concurrency_key));
        self
    }

    /// Sets the priority of the previous step or effect.
    ///
    /// # Example
    ///
    /// ```rust
    /// use underway::{Job, To};
    ///
    /// let job_builder = Job::<(), ()>::builder()
    ///     .step(|_cx, _| async move { To::done() })
    ///     .priority(10);
    /// ```
    pub fn priority(mut self, priority: i32) -> Builder<I, Current, S, StepSet<Current, S>> {
        self.update_task_config(|task_config| task_config.priority = priority);
        self
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
            effects: self.effects,
            last_config: self.last_config,
            effect_queue_name: self.effect_queue_name,
            _marker: PhantomData,
        }
    }
}

impl<I, E, S> Builder<I, Effect<E>, S, StepSet<Effect<E>, S>> {
    /// Add an effect handler after the previous step.
    ///
    /// Effects are intended for external side effects (HTTP calls, emails,
    /// etc.) and are executed on a dedicated effect queue. The next step is
    /// only enqueued after the effect handler succeeds.
    ///
    /// # Example
    ///
    /// ```rust
    /// use serde::{Deserialize, Serialize};
    /// use underway::{Job, To};
    ///
    /// #[derive(Deserialize, Serialize)]
    /// struct SendEmail {
    ///     user_id: i64,
    /// }
    ///
    /// #[derive(Deserialize, Serialize)]
    /// struct NextStep {
    ///     user_id: i64,
    /// }
    ///
    /// let job_builder = Job::<(), ()>::builder()
    ///     .step(|_cx, _| async move { To::effect(SendEmail { user_id: 42 }) })
    ///     .effect(|_cx, SendEmail { user_id }| async move {
    ///         // send the email
    ///         To::next(NextStep { user_id })
    ///     })
    ///     .step(|_cx, _| async move { To::done() });
    /// ```
    pub fn effect<F, New, Fut>(mut self, func: F) -> Builder<I, New, S, StepSet<New, S>>
    where
        E: DeserializeOwned + Serialize + Send + Sync + 'static,
        New: Serialize + Send + Sync + 'static,
        S: Send + Sync + 'static,
        F: Fn(EffectContext<S>, E) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = TaskResult<To<New>>> + Send + 'static,
    {
        let effect_fn = EffectFn::new(move |cx, input| Box::pin(func(cx, input)));
        let next_effect_index = self.effects.len();

        self.effects.push(EffectConfig {
            executor: Box::new(effect_fn),
            task_config: StepTaskConfig::default(),
            next_effect_index: None,
        });

        match self.last_config {
            Some(ConfigTarget::Step(step_index)) => {
                let step_config = self
                    .steps
                    .get_mut(step_index)
                    .expect("Step config should exist");
                step_config.next_effect_index = Some(next_effect_index);
            }
            Some(ConfigTarget::Effect(effect_index)) => {
                let effect_config = self
                    .effects
                    .get_mut(effect_index)
                    .expect("Effect config should exist");
                effect_config.next_effect_index = Some(next_effect_index);
            }
            None => panic!("Effects require a preceding step"),
        }

        self.last_config = Some(ConfigTarget::Effect(next_effect_index));

        Builder {
            builder_state: StepSet {
                state: self.builder_state.state,
                _marker: PhantomData,
            },
            steps: self.steps,
            effects: self.effects,
            last_config: self.last_config,
            effect_queue_name: self.effect_queue_name,
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
            effects: self.effects,
            last_config: self.last_config,
            effect_queue_name: self.effect_queue_name,
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
        let effect_queue_name = self
            .effect_queue_name
            .unwrap_or_else(|| format!("{queue_name}__effects"));
        let queue = Queue::builder()
            .name(queue_name)
            .pool(pool.clone())
            .build()
            .await?;
        let effect_queue = Queue::builder()
            .name(effect_queue_name)
            .pool(pool)
            .build()
            .await?;
        Ok(Job {
            queue: Arc::new(queue),
            effect_queue: Arc::new(effect_queue),
            steps: Arc::new(self.steps),
            effects: Arc::new(self.effects),
            state,
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
    ///     .queue(queue)
    ///     .build()
    ///     .await?;
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
            effects: self.effects,
            last_config: self.last_config,
            effect_queue_name: self.effect_queue_name,
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
    ///     .build()
    ///     .await?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # });
    /// # }
    pub async fn build(self) -> Result<Job<I, S>> {
        let QueueSet { state, queue } = self.builder_state;
        let effect_queue_name = self
            .effect_queue_name
            .unwrap_or_else(|| format!("{}__effects", queue.name));
        let effect_queue = Queue::builder()
            .name(effect_queue_name)
            .pool(queue.pool.clone())
            .build()
            .await?;

        Ok(Job {
            queue: Arc::new(queue),
            effect_queue: Arc::new(effect_queue),
            steps: Arc::new(self.steps),
            effects: Arc::new(self.effects),
            state,
            _marker: PhantomData,
        })
    }
}

#[cfg(test)]
mod tests {
    use std::{
        cmp::Ordering,
        sync::{Arc, Mutex},
    };

    use jiff::ToSpan;
    use serde::{Deserialize, Serialize};
    use sqlx::{postgres::types::PgInterval, PgPool};

    use super::*;
    use crate::worker::pg_interval_to_span;

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
            .build()
            .await?;

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

        async fn step(_cx: StepContext<()>, Input { message }: Input) -> TaskResult<To<()>> {
            println!("Executing job with message: {message}");
            To::done()
        }

        let queue = Queue::builder()
            .name("one_step_named")
            .pool(pool.clone())
            .build()
            .await?;

        let job = Job::builder()
            .step(step)
            .queue(queue.clone())
            .build()
            .await?;

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
            .build()
            .await?;

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

        job.worker().process_next_task().await?;

        assert_eq!(
            *state.data.lock().expect("Mutex should not be poisoned"),
            "bar".to_string()
        );

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

        async fn step(cx: StepContext<State>, Input { message }: Input) -> TaskResult<To<()>> {
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
            .build()
            .await?;

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
            .build()
            .await?;

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
    async fn enqueue_many(pool: PgPool) -> sqlx::Result<(), Error> {
        #[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
        struct Input {
            message: String,
        }

        let queue = Queue::builder()
            .name("enqueue_many")
            .pool(pool.clone())
            .build()
            .await?;

        let job = Job::builder()
            .step(|_cx, Input { message }| async move {
                println!("Processing {message}");
                To::done()
            })
            .queue(queue.clone())
            .build()
            .await?;

        let inputs = [
            Input {
                message: "first".to_string(),
            },
            Input {
                message: "second".to_string(),
            },
        ];

        let enqueued = job.enqueue_many(&inputs).await?;
        assert_eq!(enqueued.len(), 2);

        let mut job_ids: Vec<String> = enqueued
            .iter()
            .map(|handle| handle.id.to_string())
            .collect();
        job_ids.sort();

        let pending_tasks = sqlx::query!(
            r#"
            select input
            from underway.task
            where task_queue_name = $1
              and state = $2
            "#,
            queue.name,
            TaskState::Pending as TaskState
        )
        .fetch_all(&pool)
        .await?;

        let mut pending_job_ids = Vec::new();
        for task in pending_tasks {
            let job_state: JobState = serde_json::from_value(task.input)?;
            assert_eq!(job_state.step_index, 0);
            pending_job_ids.push(job_state.job_id.to_string());
        }

        pending_job_ids.sort();

        assert_eq!(pending_job_ids, job_ids);

        Ok(())
    }

    #[sqlx::test]
    async fn step_task_config(pool: PgPool) -> sqlx::Result<(), Error> {
        let queue = Queue::builder()
            .name("step_task_config")
            .pool(pool.clone())
            .build()
            .await?;

        let retry_policy = RetryPolicy::builder()
            .max_attempts(2)
            .initial_interval_ms(500)
            .max_interval_ms(5_000)
            .backoff_coefficient(1.25)
            .build();

        let job = Job::builder()
            .step(|_cx, _| async move { To::done() })
            .retry_policy(retry_policy)
            .timeout(2.minutes())
            .ttl(3.days())
            .delay(45.seconds())
            .heartbeat(5.seconds())
            .concurrency_key("customer:42")
            .priority(9)
            .queue(queue.clone())
            .build()
            .await?;

        job.enqueue(&()).await?;

        let timeout: PgInterval = sqlx::query_scalar(
            r#"
            select timeout
            from underway.task
            where task_queue_name = $1
            "#,
        )
        .bind(queue.name.clone())
        .fetch_one(&pool)
        .await?;

        let ttl: PgInterval = sqlx::query_scalar(
            r#"
            select ttl
            from underway.task
            where task_queue_name = $1
            "#,
        )
        .bind(queue.name.clone())
        .fetch_one(&pool)
        .await?;

        let delay: PgInterval = sqlx::query_scalar(
            r#"
            select delay
            from underway.task
            where task_queue_name = $1
            "#,
        )
        .bind(queue.name.clone())
        .fetch_one(&pool)
        .await?;

        let heartbeat: PgInterval = sqlx::query_scalar(
            r#"
            select heartbeat
            from underway.task
            where task_queue_name = $1
            "#,
        )
        .bind(queue.name.clone())
        .fetch_one(&pool)
        .await?;

        let concurrency_key: Option<String> = sqlx::query_scalar(
            r#"
            select concurrency_key
            from underway.task
            where task_queue_name = $1
            "#,
        )
        .bind(queue.name.clone())
        .fetch_one(&pool)
        .await?;

        let priority: i32 = sqlx::query_scalar(
            r#"
            select priority
            from underway.task
            where task_queue_name = $1
            "#,
        )
        .bind(queue.name.clone())
        .fetch_one(&pool)
        .await?;

        let stored_retry_policy: RetryPolicy = sqlx::query_scalar(
            r#"
            select retry_policy
            from underway.task
            where task_queue_name = $1
            "#,
        )
        .bind(queue.name.clone())
        .fetch_one(&pool)
        .await?;

        assert_eq!(
            pg_interval_to_span(&timeout)
                .compare(2.minutes())
                .expect("Timeout span should compare"),
            Ordering::Equal
        );
        assert_eq!(
            pg_interval_to_span(&ttl)
                .compare(3.days())
                .expect("TTL span should compare"),
            Ordering::Equal
        );
        assert_eq!(
            pg_interval_to_span(&delay)
                .compare(45.seconds())
                .expect("Delay span should compare"),
            Ordering::Equal
        );
        assert_eq!(
            pg_interval_to_span(&heartbeat)
                .compare(5.seconds())
                .expect("Heartbeat span should compare"),
            Ordering::Equal
        );
        assert_eq!(concurrency_key.as_deref(), Some("customer:42"));
        assert_eq!(priority, 9);
        assert_eq!(stored_retry_policy.max_attempts, 2);
        assert_eq!(stored_retry_policy.initial_interval_ms, 500);
        assert_eq!(stored_retry_policy.max_interval_ms, 5_000);
        assert_eq!(stored_retry_policy.backoff_coefficient, 1.25);

        Ok(())
    }

    #[sqlx::test]
    async fn step_config_is_per_job_run(pool: PgPool) -> sqlx::Result<(), Error> {
        #[derive(Serialize, Deserialize)]
        struct Step1 {
            message: String,
        }

        #[derive(Serialize, Deserialize)]
        struct Step2 {
            message: String,
        }

        let queue = Queue::builder()
            .name("step_config_is_per_job_run")
            .pool(pool.clone())
            .build()
            .await?;

        let step2_policy = RetryPolicy::builder().max_attempts(9).build();

        let job = Job::builder()
            .step(|_cx, Step1 { message }| async move { To::next(Step2 { message }) })
            .step(|_cx, Step2 { message }| async move {
                println!("Processed {message}");
                To::done()
            })
            .retry_policy(step2_policy)
            .priority(7)
            .queue(queue.clone())
            .build()
            .await?;

        job.enqueue(&Step1 {
            message: "first".to_string(),
        })
        .await?;
        job.worker().process_next_task().await?;

        job.enqueue(&Step1 {
            message: "second".to_string(),
        })
        .await?;

        let pending_tasks = sqlx::query!(
            r#"
            select
              input,
              retry_policy as "retry_policy: RetryPolicy",
              priority
            from underway.task
            where task_queue_name = $1
              and state = $2
            "#,
            queue.name,
            TaskState::Pending as TaskState
        )
        .fetch_all(&pool)
        .await?;

        let mut step0 = None;
        let mut step1 = None;

        for task in pending_tasks {
            let job_state: JobState = serde_json::from_value(task.input.clone())?;
            match job_state.step_index {
                0 => step0 = Some(task),
                1 => step1 = Some(task),
                _ => {}
            }
        }

        let step0 = step0.expect("Expected step 0 task");
        let step1 = step1.expect("Expected step 1 task");

        assert_eq!(step0.retry_policy, RetryPolicy::default());
        assert_eq!(step0.priority, 0);
        assert_eq!(step1.retry_policy, step2_policy);
        assert_eq!(step1.priority, 7);

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
            .build()
            .await?;

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
    async fn one_step_context_attributes(pool: PgPool) -> sqlx::Result<(), Error> {
        let job = Job::builder()
            .step(|ctx, _| async move {
                assert_eq!(ctx.step_index, 0);
                assert_eq!(ctx.step_count, 1);
                assert_eq!(ctx.queue_name.as_str(), "one_step_context_attributes");
                To::done()
            })
            .name("one_step_context_attributes")
            .pool(pool.clone())
            .build()
            .await?;

        job.enqueue(&()).await?;

        // Process the first task.
        let task_id = job.worker().process_next_task().await?;

        assert!(task_id.is_some());

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
            .build()
            .await?;

        assert_eq!(job.retry_policy(), RetryPolicy::default());

        let input = Step1 {
            message: "Hello, world!".to_string(),
        };
        job.enqueue(&input).await?;

        // Process the first task.
        job.worker().process_next_task().await?;

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
            .build()
            .await?;

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
        job.worker().process_next_task().await?;

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
            .build()
            .await?;

        assert_eq!(job.retry_policy(), RetryPolicy::default());

        let input = Step1 {
            message: "Hello, world!".to_string(),
        };
        job.enqueue(&input).await?;

        // Process the first task.
        job.worker().process_next_task().await?;

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
            .build()
            .await?;

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
        job.worker().process_next_task().await?;

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
    async fn effect_steps_run_in_order(pool: PgPool) -> sqlx::Result<(), Error> {
        #[derive(Clone)]
        struct State {
            events: Arc<Mutex<Vec<String>>>,
        }

        #[derive(Serialize, Deserialize)]
        struct Step1 {
            message: String,
        }

        #[derive(Serialize, Deserialize)]
        struct SendEmail {
            message: String,
        }

        #[derive(Serialize, Deserialize)]
        struct Step2 {
            message: String,
        }

        let queue = Queue::builder()
            .name("effect_steps_run_in_order")
            .pool(pool.clone())
            .build()
            .await?;

        let events = Arc::new(Mutex::new(Vec::new()));
        let state = State {
            events: Arc::clone(&events),
        };

        let job = Job::builder()
            .state(state)
            .step(|cx, Step1 { message }| async move {
                cx.state
                    .events
                    .lock()
                    .expect("Mutex should not be poisoned")
                    .push("step1".to_string());
                To::effect(SendEmail { message })
            })
            .effect(|cx, SendEmail { message }| async move {
                cx.state
                    .events
                    .lock()
                    .expect("Mutex should not be poisoned")
                    .push("effect".to_string());
                To::next(Step2 { message })
            })
            .step(|cx, Step2 { message }| async move {
                cx.state
                    .events
                    .lock()
                    .expect("Mutex should not be poisoned")
                    .push(format!("step2:{message}"));
                To::done()
            })
            .queue(queue.clone())
            .build()
            .await?;

        job.enqueue(&Step1 {
            message: "hello".to_string(),
        })
        .await?;

        job.worker().process_next_task().await?;
        job.effect_worker().process_next_task().await?;
        job.worker().process_next_task().await?;

        let events = events.lock().expect("Mutex should not be poisoned").clone();
        assert_eq!(
            events,
            vec![
                "step1".to_string(),
                "effect".to_string(),
                "step2:hello".to_string()
            ]
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
            .build()
            .await?;

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
            .build()
            .await?;

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
            .build()
            .await?;

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
            .build()
            .await?;

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
