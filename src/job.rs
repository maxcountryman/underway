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
//! use underway::{job::EffectOutcome, Job, To};
//!
//! let job_builder = Job::<(), ()>::builder().step(|_cx, _| async move { To::done() });
//! ```
//!
//! Instead of a closure, we could also use a named function.
//!
//! ```rust
//! use underway::{job::Context, task::Result as TaskResult, Job, To};
//!
//! async fn named_step(_cx: Context<(), ()>, _: ()) -> TaskResult<To<()>> {
//!     To::done()
//! }
//!
//! let job_builder = Job::<_, ()>::builder().step(named_step);
//! ```
//!
//! Notice that the first argument to our function is [a context
//! binding](crate::job::Context). This provides access to fields like
//! [`step_state`](crate::job::Context::step_state),
//! [`effect_state`](crate::job::Context::effect_state),
//! and [`phase`](crate::job::Context::phase).
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
//! use underway::{Job, To};
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
//!     .step(|cx, _| async move {
//!         let state = cx.step_state().expect("step context");
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
//! Returning `To::effect` delegates the next step type to the effect handler.
//! If a step needs to branch between `next`, `done`, and `effect`, use
//! [`To::effect_for`] or [`To::done_for`] to keep the next step type explicit.
//! Effect handlers have the same pattern via [`EffectOutcome::effect_for`].
//!
//! ```rust
//! use serde::{Deserialize, Serialize};
//! use underway::{job::EffectOutcome, Job, To};
//!
//! #[derive(Serialize, Deserialize)]
//! struct Step1 {
//!     send_email: bool,
//! }
//!
//! #[derive(Serialize, Deserialize)]
//! struct SendEmail;
//!
//! #[derive(Serialize, Deserialize)]
//! struct Step2;
//!
//! let job_builder = Job::<_, ()>::builder()
//!     .step(|_cx, Step1 { send_email }| async move {
//!         if send_email {
//!             To::effect_for(SendEmail)
//!         } else {
//!             To::next(Step2)
//!         }
//!     })
//!     .effect(|_cx, SendEmail| async move { Ok(EffectOutcome::next(Step2)) })
//!     .step(|_cx, Step2| async move { To::done() });
//! ```
//!
//! ```rust
//! use serde::{Deserialize, Serialize};
//! use underway::{job::EffectOutcome, Job, To};
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
//!         Ok(EffectOutcome::next(FinishSub { user_id }))
//!     })
//!     .step(|_cx, FinishSub { user_id }| async move {
//!         println!("Finished subscription for {user_id}");
//!         To::done()
//!     });
//! ```
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

use builder_states::{Initial, PoolSet, QueueNameSet, QueueSet, StepSet};
use jiff::{Span, ToSpan};
use sealed::{EffectState as EffectJobState, JobState};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use sqlx::{PgConnection, PgExecutor, PgPool, Postgres, Transaction};
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

type JobQueue<T, StepState, EffectState> = Queue<Job<T, StepState, EffectState>>;
type EffectQueue<T, StepState, EffectState> = Queue<EffectTask<T, StepState, EffectState>>;

/// The execution phase for a job context.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
pub enum Phase {
    /// A step is executing.
    Step,
    /// An effect is executing.
    EffectExecute,
    /// A compensation is executing.
    EffectCompensate,
}

/// Context passed into steps and effects.
pub enum Context<StepState, EffectState> {
    /// Context for steps.
    Step {
        /// Shared step state.
        ///
        /// This value is set via [`state`](Builder::state).
        ///
        /// **Note:** State is not persisted and therefore should not be
        /// relied on when durability is needed.
        state: StepState,
        /// Current index of the step being executed zero-based.
        step_index: usize,
        /// Total steps count.
        step_count: usize,
        /// This `JobId`.
        job_id: JobId,
        /// Queue name.
        queue_name: String,
    },
    /// Context for effects and compensations.
    Effect {
        /// Shared effect state.
        ///
        /// This value is set via [`effect_state`](Builder::effect_state).
        ///
        /// **Note:** State is not persisted and therefore should not be
        /// relied on when durability is needed.
        state: EffectState,
        /// Phase for the effect context.
        phase: Phase,
        /// Current index of the step that produced this effect, zero-based.
        step_index: usize,
        /// Current index of the effect being executed, zero-based.
        effect_index: usize,
        /// This `JobId`.
        job_id: JobId,
        /// Queue name.
        queue_name: String,
        /// Effect queue name.
        effect_queue_name: String,
    },
}

impl<StepState, EffectState> Context<StepState, EffectState> {
    /// Returns the current phase for the context.
    pub fn phase(&self) -> Phase {
        match self {
            Self::Step { .. } => Phase::Step,
            Self::Effect { phase, .. } => *phase,
        }
    }

    /// Returns the step state, if available.
    pub fn step_state(&self) -> Option<&StepState> {
        match self {
            Self::Step { state, .. } => Some(state),
            _ => None,
        }
    }

    /// Returns the effect state, if available.
    pub fn effect_state(&self) -> Option<&EffectState> {
        match self {
            Self::Effect { state, .. } => Some(state),
            _ => None,
        }
    }
}

enum StageExecutor<StepState, EffectState> {
    Step(Box<dyn StepExecutor<StepState, EffectState>>),
    Effect(Box<dyn EffectExecutor<StepState, EffectState>>),
}

struct StageConfig<StepState, EffectState> {
    executor: StageExecutor<StepState, EffectState>,
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

    use super::{JobId, Phase};

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
        pub phase: Phase,
        pub(crate) job_id: JobId,
    }
}

enum StageInput {
    Step(JobState),
    Effect(EffectJobState),
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

type CompensationHook = Arc<
    dyn for<'a> Fn(
            &'a mut PgConnection,
            JobId,
        ) -> Pin<Box<dyn Future<Output = TaskResult<()>> + Send + 'a>>
        + Send
        + Sync,
>;

/// Represents a specific job that's been enqueued.
///
/// This handle allows for manipulating the state of the job in the queue.
pub struct EnqueuedJob<T: Task> {
    id: JobId,
    queue: Arc<Queue<T>>,
    compensator: Option<CompensationHook>,
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

        if cancelled {
            if let Some(compensator) = &self.compensator {
                compensator(tx.as_mut(), self.id).await?;
            }
        }
        tx.commit().await?;

        Ok(cancelled)
    }
}

/// Sequential set of functions, where the output of the last is the input to
/// the next.
pub struct Job<I, StepState = (), EffectState = ()>
where
    I: Sync + Send + 'static,
    StepState: Clone + Sync + Send + 'static,
    EffectState: Clone + Sync + Send + 'static,
{
    queue: Arc<JobQueue<I, StepState, EffectState>>,
    effect_queue: Arc<EffectQueue<I, StepState, EffectState>>,
    steps: Arc<Vec<StageConfig<StepState, EffectState>>>,
    effects: Arc<Vec<StageConfig<StepState, EffectState>>>,
    step_state: StepState,
    effect_state: EffectState,
    _marker: PhantomData<I>,
}

struct EffectTask<I, StepState, EffectState>
where
    I: Send + Sync + 'static,
    StepState: Clone + Send + Sync + 'static,
    EffectState: Clone + Send + Sync + 'static,
{
    job: Job<I, StepState, EffectState>,
}

/// Worker wrapper for job effect handlers.
pub struct EffectWorker<I, StepState, EffectState>
where
    I: Send + Sync + 'static,
    StepState: Clone + Send + Sync + 'static,
    EffectState: Clone + Send + Sync + 'static,
{
    worker: Worker<EffectTask<I, StepState, EffectState>>,
}

impl<I, StepState, EffectState> Clone for EffectWorker<I, StepState, EffectState>
where
    I: Send + Sync + 'static,
    StepState: Clone + Send + Sync + 'static,
    EffectState: Clone + Send + Sync + 'static,
{
    fn clone(&self) -> Self {
        Self {
            worker: self.worker.clone(),
        }
    }
}

impl<I, StepState, EffectState> EffectWorker<I, StepState, EffectState>
where
    I: Send + Sync + 'static,
    StepState: Clone + Send + Sync + 'static,
    EffectState: Clone + Send + Sync + 'static,
{
    fn new(worker: Worker<EffectTask<I, StepState, EffectState>>) -> Self {
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

impl<I> Job<I, (), ()>
where
    I: Serialize + Sync + Send + 'static,
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
    pub fn builder() -> Builder<I, I, NoEffect, (), (), Initial> {
        Builder::new()
    }
}

impl<I, StepState, EffectState> Job<I, StepState, EffectState>
where
    I: Serialize + Sync + Send + 'static,
    StepState: Clone + Send + Sync + 'static,
    EffectState: Clone + Send + Sync + 'static,
{

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

        let compensator = Some(self.compensation_hook());
        let enqueued = job_inputs
            .into_iter()
            .map(|job_input| EnqueuedJob {
                id: job_input.job_id,
                queue: self.queue.clone(),
                compensator: compensator.clone(),
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
            compensator: Some(self.compensation_hook()),
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

    fn effect_queue(&self) -> Arc<Queue<EffectTask<I, StepState, EffectState>>> {
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
    pub fn effect_worker(&self) -> EffectWorker<I, StepState, EffectState> {
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

impl<I, StepState, EffectState> Job<I, StepState, EffectState>
where
    I: Send + Sync + 'static,
    StepState: Clone + Send + Sync + 'static,
    EffectState: Clone + Send + Sync + 'static,
{
    fn effect_task(&self) -> EffectTask<I, StepState, EffectState> {
        EffectTask { job: self.clone() }
    }

    fn compensation_hook(&self) -> CompensationHook {
        let job = self.clone();
        Arc::new(move |conn, job_id| {
            let job = job.clone();
            Box::pin(async move { job.enqueue_compensations(conn, job_id).await })
        })
    }

    fn step_task_config(&self, step_index: usize) -> StepTaskConfig {
        self.steps
            .get(step_index)
            .map(|step| step.task_config.clone())
            .unwrap_or_default()
    }

    fn effect_task_config(&self, effect_index: usize, phase: Phase) -> StepTaskConfig {
        let mut config = self
            .effects
            .get(effect_index)
            .map(|effect| effect.task_config.clone())
            .unwrap_or_default();

        if phase == Phase::EffectCompensate {
            if let Some(effect_config) = self.effects.get(effect_index) {
                let StageExecutor::Effect(effect) = &effect_config.executor else {
                    return config;
                };
                config.retry_policy = effect.compensation_policy();
            }
        }

        config
    }

    fn step_context(&self, job_id: JobId, step_index: usize) -> Context<StepState, EffectState> {
        Context::Step {
            state: self.step_state.clone(),
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
        phase: Phase,
    ) -> Context<StepState, EffectState> {
        Context::Effect {
            state: self.effect_state.clone(),
            phase,
            step_index,
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
            let effect_input = EffectJobState {
                effect_index,
                step_index,
                effect_input: next_input,
                phase: Phase::EffectExecute,
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

    async fn record_effect(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        job_id: JobId,
        step_index: usize,
        effect_index: usize,
        compensation: Option<serde_json::Value>,
    ) -> TaskResult<()> {
        sqlx::query!(
            r#"
            insert into underway.job_effect (
              job_id,
              step_index,
              effect_index,
              compensation,
              compensated_at
            )
            values ($1, $2, $3, $4::jsonb, case when $4::jsonb is null then now() else null end)
            on conflict (job_id, effect_index)
            do update
              set step_index = excluded.step_index,
                  compensation = excluded.compensation,
                  compensated_at = excluded.compensated_at,
                  updated_at = now()
            "#,
            *job_id,
            step_index as i32,
            effect_index as i32,
            compensation
        )
        .execute(&mut **tx)
        .await
        .map_err(|err| TaskError::Retryable(err.to_string()))?;

        Ok(())
    }

    async fn mark_effect_compensated(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        job_id: JobId,
        effect_index: usize,
    ) -> TaskResult<()> {
        let result = sqlx::query!(
            r#"
            update underway.job_effect
            set compensated_at = now(),
                updated_at = now()
            where job_id = $1
              and effect_index = $2
            "#,
            *job_id,
            effect_index as i32
        )
        .execute(&mut **tx)
        .await
        .map_err(|err| TaskError::Retryable(err.to_string()))?;

        if result.rows_affected() == 0 {
            return Err(TaskError::Fatal("Missing effect log entry.".into()));
        }

        Ok(())
    }

    async fn enqueue_compensations(
        &self,
        conn: &mut PgConnection,
        job_id: JobId,
    ) -> TaskResult<()> {
        let rows = sqlx::query!(
            r#"
            with to_enqueue as (
                update underway.job_effect
                set compensation_enqueued_at = now(),
                    updated_at = now()
                where job_id = $1
                  and compensation is not null
                  and compensation_enqueued_at is null
                  and compensated_at is null
                returning effect_index, step_index, compensation
            )
            select
              effect_index,
              step_index,
              compensation as "compensation!: serde_json::Value"
            from to_enqueue
            order by effect_index desc
            "#,
            *job_id
        )
        .fetch_all(&mut *conn)
        .await
        .map_err(|err| TaskError::Retryable(err.to_string()))?;

        if rows.is_empty() {
            return Ok(());
        }

        let effect_task = self.effect_task();

        for row in rows {
            let effect_input = EffectJobState {
                effect_index: row.effect_index as usize,
                step_index: row.step_index as usize,
                effect_input: row.compensation,
                phase: Phase::EffectCompensate,
                job_id,
            };

            self.effect_queue
                .enqueue_after(&mut *conn, &effect_task, &effect_input, Span::new())
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
        let mut current_effect_index = None;
        let mut current_phase = None;

        let (step_index, job_id, next_effect_index, stage_result) = match input {
            StageInput::Step(JobState {
                step_index,
                step_input,
                job_id,
            }) => {
                self.validate_step_index(step_index)?;

                let step_config = &self.steps[step_index];
                let cx = self.step_context(job_id, step_index);
                let StageExecutor::Step(step) = &step_config.executor else {
                    return Err(TaskError::Fatal("Invalid step executor.".into()));
                };
                let stage_result = match step.execute_step(cx, step_input).await {
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
            StageInput::Effect(EffectJobState {
                effect_index,
                step_index,
                effect_input,
                phase,
                job_id,
            }) => {
                self.validate_step_index(step_index)?;
                self.validate_effect_index(effect_index)?;

                let effect_config = &self.effects[effect_index];
                let cx = self.effect_context(job_id, step_index, effect_index, phase);
                let StageExecutor::Effect(effect) = &effect_config.executor else {
                    return Err(TaskError::Fatal("Invalid effect executor.".into()));
                };
                let stage_result = match phase {
                    Phase::EffectExecute => match effect.execute_effect(cx, effect_input).await {
                        Ok(result) => result,
                        Err(err) => {
                            tx.commit().await?;
                            return Err(err);
                        }
                    },
                    Phase::EffectCompensate => match effect.compensate_effect(cx, effect_input).await {
                        Ok(()) => StageResult {
                            transition: StageTransition::Done,
                            delay: Span::new(),
                            compensation: None,
                        },
                        Err(err) => {
                            tx.commit().await?;
                            return Err(err);
                        }
                    },
                    Phase::Step => {
                        return Err(TaskError::Fatal("Invalid effect phase.".into()));
                    }
                };

                current_effect_index = Some(effect_index);
                current_phase = Some(phase);

                (
                    step_index,
                    job_id,
                    if phase == Phase::EffectExecute {
                        effect_config.next_effect_index
                    } else {
                        None
                    },
                    stage_result,
                )
            }
        };

        let StageResult {
            transition,
            delay,
            compensation,
        } = stage_result;

        if let Some(effect_index) = current_effect_index {
            match current_phase {
                Some(Phase::EffectExecute) => {
                    self.record_effect(&mut tx, job_id, step_index, effect_index, compensation)
                        .await?;
                }
                Some(Phase::EffectCompensate) => {
                    let _ = compensation;
                    self.mark_effect_compensated(&mut tx, job_id, effect_index)
                        .await?;
                }
                _ => {
                    let _ = compensation;
                }
            }
        } else {
            let _ = compensation;
        }

        match transition {
            StageTransition::NextStep(next_input) => {
                self.enqueue_transition(
                    &mut tx,
                    job_id,
                    step_index,
                    None,
                    next_input,
                    delay,
                )
                .await?;
            }
            StageTransition::NextEffect(next_input) => {
                let next_effect_index = next_effect_index
                    .ok_or_else(|| TaskError::Fatal("Missing effect for transition.".into()))?;
                self.enqueue_transition(
                    &mut tx,
                    job_id,
                    step_index,
                    Some(next_effect_index),
                    next_input,
                    delay,
                )
                .await?;
            }
            StageTransition::Done => {}
        }

        tx.commit().await?;

        Ok(())
    }
}

impl<I, StepState, EffectState> TaskConfig for Job<I, StepState, EffectState>
where
    I: Send + Sync + 'static,
    StepState: Clone + Send + Sync + 'static,
    EffectState: Clone + Send + Sync + 'static,
{
    type Input = JobState;

    fn task_config_default(&self) -> StepTaskConfig {
        self.step_task_config(0)
    }

    fn task_config_for(&self, input: &Self::Input) -> StepTaskConfig {
        self.step_task_config(input.step_index)
    }
}

impl<I, StepState, EffectState> TaskConfig for EffectTask<I, StepState, EffectState>
where
    I: Send + Sync + 'static,
    StepState: Clone + Send + Sync + 'static,
    EffectState: Clone + Send + Sync + 'static,
{
    type Input = EffectJobState;

    fn task_config_default(&self) -> StepTaskConfig {
        self.job.effect_task_config(0, Phase::EffectExecute)
    }

    fn task_config_for(&self, input: &Self::Input) -> StepTaskConfig {
        self.job.effect_task_config(input.effect_index, input.phase)
    }
}

impl<I, StepState, EffectState> Task for Job<I, StepState, EffectState>
where
    I: Send + Sync + 'static,
    StepState: Clone + Send + Sync + 'static,
    EffectState: Clone + Send + Sync + 'static,
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

    fn on_final_failure(
        &self,
        conn: &mut PgConnection,
        input: &Self::Input,
        _error: &TaskError,
    ) -> impl Future<Output = TaskResult<()>> + Send {
        self.enqueue_compensations(conn, input.job_id)
    }

    impl_task_config!();
}

impl<I, StepState, EffectState> Task for EffectTask<I, StepState, EffectState>
where
    I: Send + Sync + 'static,
    StepState: Clone + Send + Sync + 'static,
    EffectState: Clone + Send + Sync + 'static,
{
    type Input = EffectJobState;
    type Output = ();

    #[instrument(
        skip_all,
        fields(
            job.id = %input.job_id.as_hyphenated(),
            step = input.step_index + 1,
            effect = input.effect_index + 1,
            phase = ?input.phase,
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

    fn on_final_failure(
        &self,
        conn: &mut PgConnection,
        input: &Self::Input,
        _error: &TaskError,
    ) -> impl Future<Output = TaskResult<()>> + Send {
        async move {
            if input.phase == Phase::EffectCompensate {
                return Ok(());
            }

            self.job.enqueue_compensations(conn, input.job_id).await
        }
    }

    impl_task_config!();
}

impl<I, StepState, EffectState> Clone for Job<I, StepState, EffectState>
where
    I: Send + Sync + 'static,
    StepState: Clone + Send + Sync + 'static,
    EffectState: Clone + Send + Sync + 'static,
{
    fn clone(&self) -> Self {
        Self {
            queue: self.queue.clone(),
            effect_queue: self.effect_queue.clone(),
            steps: self.steps.clone(),
            effects: self.effects.clone(),
            step_state: self.step_state.clone(),
            effect_state: self.effect_state.clone(),
            _marker: PhantomData,
        }
    }
}

/// Marker type used when no effect follows a step or effect.
#[derive(Debug, Default, Clone, Copy, Deserialize, Serialize)]
pub struct NoEffect;

/// Marker type used when the next step is defined by an effect handler.
///
/// This is returned by [`To::effect`] and [`EffectOutcome::effect`] when the
/// next step will be determined by the effect handler itself.
#[derive(Debug, Clone, Copy, Serialize)]
pub struct PendingStep {
    _private: (),
}

/// Marker type for effect inputs.
#[derive(Debug, Deserialize, Serialize)]
#[serde(transparent)]
pub struct EffectInput<E>(pub E);

/// Marker type used when no compensation payload is produced.
#[derive(Debug, Default, Clone, Copy, Deserialize, Serialize)]
pub struct NoCompensation;

/// Marker trait for pending effects returned from steps or effects.
pub trait EffectPending {
    /// Input type provided to the effect handler.
    type Input: DeserializeOwned + Serialize + Send + Sync + 'static;
}

impl<E> EffectPending for EffectInput<E>
where
    E: DeserializeOwned + Serialize + Send + Sync + 'static,
{
    type Input = E;
}

/// Represents the state after executing a step.
#[derive(Deserialize, Serialize)]
pub enum To<N, E = NoEffect> {
    /// The next step to transition to.
    Next(N),

    /// The next step to transition to after the delay.
    Delay {
        /// The step itself.
        next: N,

        /// The delay before which the step will not be run.
        delay: Span,
    },

    /// The next effect to transition to.
    Effect(E),

    /// The terminal state.
    Done,
}

impl<N, E> To<N, E> {
    /// Transitions from the current step to the next step.
    pub fn next(step: N) -> TaskResult<Self> {
        Ok(Self::Next(step))
    }

    /// Transitions from the current step to the next step, but after the given delay.
    pub fn delay_for(step: N, delay: Span) -> TaskResult<Self> {
        Ok(Self::Delay { next: step, delay })
    }

}

impl<N> To<N, NoEffect> {
    /// Transitions from the current step to an effect handler when the next
    /// step type is already known.
    ///
    /// Use this when branching between `next`, `done`, and `effect` in a single
    /// step so the next step type stays explicit.
    pub fn effect_for<EffectInputT>(
        effect: EffectInputT,
    ) -> TaskResult<To<N, EffectInput<EffectInputT>>> {
        Ok(To::Effect(EffectInput(effect)))
    }
}

impl To<PendingStep, NoEffect> {
    /// Transitions from the current step to an effect handler.
    ///
    /// Use [`Self::effect_for`] when the next step type is already known (for
    /// example, when branching between `next` and `effect`).
    pub fn effect<EffectInputT>(
        effect: EffectInputT,
    ) -> TaskResult<To<PendingStep, EffectInput<EffectInputT>>> {
        Ok(To::Effect(EffectInput(effect)))
    }
}

impl<E> To<(), E> {
    /// Signals that this is the final step when the next step type is already known.
    ///
    /// This is useful for branching between `next`, `done`, and `effect` within a
    /// single step.
    pub fn done_for<N>() -> TaskResult<To<N, E>> {
        Ok(To::Done)
    }
}

impl To<(), NoEffect> {
    /// Signals that this is the final step and no more steps will follow.
    pub fn done() -> TaskResult<Self> {
        Ok(Self::Done)
    }
}

/// Outcome of executing an effect.
#[derive(Debug)]
pub enum EffectOutcome<N, E = NoEffect, C = NoCompensation> {
    /// Transition to the next step.
    Next {
        /// Input for the next step.
        next: N,
        /// Delay before the next step executes.
        delay: Span,
        /// Optional compensation payload.
        compensation: Option<C>,
    },
    /// Transition to the next effect.
    Effect {
        /// Input for the next effect.
        effect: E,
        /// Delay before the next effect executes.
        delay: Span,
        /// Optional compensation payload.
        compensation: Option<C>,
    },
    /// Finish the job.
    Done {
        /// Optional compensation payload.
        compensation: Option<C>,
    },
}

impl<N, E, C> EffectOutcome<N, E, C> {
    /// Transition to the next step.
    pub fn next(next: N) -> Self {
        Self::Next {
            next,
            delay: Span::new(),
            compensation: None,
        }
    }

    /// Transition to the next step with compensation.
    pub fn next_with_compensation(next: N, compensation: C) -> Self {
        Self::Next {
            next,
            delay: Span::new(),
            compensation: Some(compensation),
        }
    }

    /// Delay the next step.
    pub fn delay_for(next: N, delay: Span) -> Self {
        Self::Next {
            next,
            delay,
            compensation: None,
        }
    }

    /// Delay the next step with compensation.
    pub fn delay_with_compensation(next: N, delay: Span, compensation: C) -> Self {
        Self::Next {
            next,
            delay,
            compensation: Some(compensation),
        }
    }

    /// Finish the job.
    pub fn done() -> Self {
        Self::Done { compensation: None }
    }

    /// Finish the job with compensation.
    pub fn done_with_compensation(compensation: C) -> Self {
        Self::Done {
            compensation: Some(compensation),
        }
    }

}

impl<C> EffectOutcome<PendingStep, NoEffect, C> {
    /// Transition to the next effect.
    ///
    /// Use [`EffectOutcome::effect_for`] when the next step type is already
    /// known (for example, when branching between `next` and `effect`).
    pub fn effect<E>(effect: E) -> EffectOutcome<PendingStep, EffectInput<E>, C>
    where
        E: Send,
    {
        EffectOutcome::Effect {
            effect: EffectInput(effect),
            delay: Span::new(),
            compensation: None,
        }
    }

    /// Transition to the next effect with compensation.
    pub fn effect_with_compensation<E>(
        effect: E,
        compensation: C,
    ) -> EffectOutcome<PendingStep, EffectInput<E>, C>
    where
        E: Send,
    {
        EffectOutcome::Effect {
            effect: EffectInput(effect),
            delay: Span::new(),
            compensation: Some(compensation),
        }
    }
}

impl<N, C> EffectOutcome<N, NoEffect, C> {
    /// Transition to the next effect when the next step type is already known.
    ///
    /// Use this when branching between `next`, `done`, and `effect` in a single
    /// effect handler so the next step type stays explicit.
    pub fn effect_for<EffectInputT>(
        effect: EffectInputT,
    ) -> EffectOutcome<N, EffectInput<EffectInputT>, C>
    where
        EffectInputT: Send,
    {
        EffectOutcome::Effect {
            effect: EffectInput(effect),
            delay: Span::new(),
            compensation: None,
        }
    }

    /// Transition to the next effect with compensation when the next step type
    /// is already known.
    pub fn effect_with_compensation_for<EffectInputT>(
        effect: EffectInputT,
        compensation: C,
    ) -> EffectOutcome<N, EffectInput<EffectInputT>, C>
    where
        EffectInputT: Send,
    {
        EffectOutcome::Effect {
            effect: EffectInput(effect),
            delay: Span::new(),
            compensation: Some(compensation),
        }
    }
}

/// Trait describing an effect with optional compensation.
pub trait Effect: Send + Sync + 'static {
    /// Shared step state type.
    type StepState: Clone + Send + Sync + 'static;
    /// Shared effect state type.
    type EffectState: Clone + Send + Sync + 'static;
    /// Input passed to the effect handler.
    type Input: DeserializeOwned + Serialize + Send + Sync + 'static;
    /// Output for the next step.
    type Next: Serialize + Send + Sync + 'static;
    /// Output for the next effect.
    type NextEffect: Serialize + Send + Sync + 'static;
    /// Compensation payload for rollback.
    type Compensation: DeserializeOwned + Serialize + Send + Sync + 'static;

    /// Execute the effect and decide the next transition.
    fn execute(
        &self,
        cx: Context<Self::StepState, Self::EffectState>,
        input: Self::Input,
    ) -> impl Future<Output = TaskResult<EffectOutcome<Self::Next, Self::NextEffect, Self::Compensation>>>
           + Send;

    /// Execute the compensation handler.
    fn compensate(
        &self,
        cx: Context<Self::StepState, Self::EffectState>,
        input: Self::Compensation,
    ) -> impl Future<Output = TaskResult<()>> + Send {
        let _ = cx;
        let _ = input;
        async { Ok(()) }
    }

    /// Retry policy to apply to compensations.
    fn compensation_policy(&self) -> RetryPolicy {
        RetryPolicy::default()
    }
}

// A concrete implementation of a step using a closure.
struct StepFn<I, O, E, StepState, EffectState, F>
where
    F: Fn(Context<StepState, EffectState>, I)
            -> Pin<Box<dyn Future<Output = TaskResult<To<O, E>>> + Send>>
        + Send
        + Sync
        + 'static,
{
    func: Arc<F>,
    _marker: PhantomData<(I, O, E, StepState, EffectState)>,
}

impl<I, O, E, StepState, EffectState, F> StepFn<I, O, E, StepState, EffectState, F>
where
    F: Fn(Context<StepState, EffectState>, I)
            -> Pin<Box<dyn Future<Output = TaskResult<To<O, E>>> + Send>>
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

struct NoCompensate;

trait CompensateFn<StepState, EffectState, CompInput>: Send + Sync + 'static {
    type Fut: Future<Output = TaskResult<()>> + Send;

    fn call(&self, cx: Context<StepState, EffectState>, input: CompInput) -> Self::Fut;
}

impl<StepState, EffectState> CompensateFn<StepState, EffectState, NoCompensation>
    for NoCompensate
{
    type Fut = Pin<Box<dyn Future<Output = TaskResult<()>> + Send>>;

    fn call(&self, _cx: Context<StepState, EffectState>, _input: NoCompensation) -> Self::Fut {
        Box::pin(async { Ok(()) })
    }
}

impl<F, Fut, StepState, EffectState, CompInput> CompensateFn<StepState, EffectState, CompInput>
    for F
where
    F: Fn(Context<StepState, EffectState>, CompInput) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = TaskResult<()>> + Send + 'static,
{
    type Fut = Fut;

    fn call(&self, cx: Context<StepState, EffectState>, input: CompInput) -> Self::Fut {
        (self)(cx, input)
    }
}

struct EffectClosure<Exec, Comp, StepState, EffectState, Input, Next, NextEffect, CompInput> {
    exec: Exec,
    compensate: Comp,
    _marker: PhantomData<(StepState, EffectState, Input, Next, NextEffect, CompInput)>,
}

impl<Exec, StepState, EffectState, Input, Next, NextEffect>
    EffectClosure<Exec, NoCompensate, StepState, EffectState, Input, Next, NextEffect, NoCompensation>
{
    fn new(exec: Exec) -> Self {
        Self {
            exec,
            compensate: NoCompensate,
            _marker: PhantomData,
        }
    }

    fn with_compensation<Comp, CompInput>(
        self,
        compensate: Comp,
    ) -> EffectClosure<Exec, Comp, StepState, EffectState, Input, Next, NextEffect, CompInput> {
        EffectClosure {
            exec: self.exec,
            compensate,
            _marker: PhantomData,
        }
    }
}

impl<
        Exec,
        Comp,
        StepState,
        EffectState,
        Input,
        Next,
        NextEffect,
        CompInput,
        ExecFut,
    > Effect for EffectClosure<Exec, Comp, StepState, EffectState, Input, Next, NextEffect, CompInput>
where
    StepState: Clone + Send + Sync + 'static,
    EffectState: Clone + Send + Sync + 'static,
    Input: DeserializeOwned + Serialize + Send + Sync + 'static,
    Next: Serialize + Send + Sync + 'static,
    NextEffect: Serialize + Send + Sync + 'static,
    CompInput: DeserializeOwned + Serialize + Send + Sync + 'static,
    Exec: Fn(Context<StepState, EffectState>, Input) -> ExecFut + Send + Sync + 'static,
    ExecFut: Future<Output = TaskResult<EffectOutcome<Next, NextEffect, CompInput>>> + Send + 'static,
    Comp: CompensateFn<StepState, EffectState, CompInput>,
{
    type StepState = StepState;
    type EffectState = EffectState;
    type Input = Input;
    type Next = Next;
    type NextEffect = NextEffect;
    type Compensation = CompInput;

    fn execute(
        &self,
        cx: Context<Self::StepState, Self::EffectState>,
        input: Self::Input,
    ) -> impl Future<Output = TaskResult<EffectOutcome<Self::Next, Self::NextEffect, Self::Compensation>>>
           + Send {
        (self.exec)(cx, input)
    }

    fn compensate(
        &self,
        cx: Context<Self::StepState, Self::EffectState>,
        input: Self::Compensation,
    ) -> impl Future<Output = TaskResult<()>> + Send {
        self.compensate.call(cx, input)
    }
}

enum StageTransition {
    NextStep(serde_json::Value),
    NextEffect(serde_json::Value),
    Done,
}

struct StageResult {
    transition: StageTransition,
    delay: Span,
    compensation: Option<serde_json::Value>,
}

type StageResultFuture<'a> = Pin<Box<dyn Future<Output = TaskResult<StageResult>> + Send + 'a>>;

// A trait object wrapper for steps to allow heterogeneous step types in a
// vector.
trait StepExecutor<StepState, EffectState>: Send + Sync {
    // Execute the step with the given input serialized as JSON.
    fn execute_step(
        &self,
        cx: Context<StepState, EffectState>,
        input: serde_json::Value,
    ) -> StageResultFuture<'_>;
}

// A trait object wrapper for effects to allow heterogeneous effect types in a
// vector.
trait EffectExecutor<StepState, EffectState>: Send + Sync {
    // Execute the effect with the given input serialized as JSON.
    fn execute_effect(
        &self,
        cx: Context<StepState, EffectState>,
        input: serde_json::Value,
    ) -> StageResultFuture<'_>;
    #[allow(dead_code)]
    fn compensate_effect(
        &self,
        cx: Context<StepState, EffectState>,
        input: serde_json::Value,
    ) -> Pin<Box<dyn Future<Output = TaskResult<()>> + Send + '_>>;
    #[allow(dead_code)]
    fn compensation_policy(&self) -> RetryPolicy;
}

impl<I, O, E, StepState, EffectState, F> StepExecutor<StepState, EffectState>
    for StepFn<I, O, E, StepState, EffectState, F>
where
    I: DeserializeOwned + Serialize + Send + Sync + 'static,
    O: Serialize + Send + Sync + 'static,
    E: Serialize + Send + Sync + 'static,
    StepState: Send + Sync + 'static,
    EffectState: Send + Sync + 'static,
    F: Fn(Context<StepState, EffectState>, I)
            -> Pin<Box<dyn Future<Output = TaskResult<To<O, E>>> + Send>>
        + Send
        + Sync
        + 'static,
{
    fn execute_step(
        &self,
        cx: Context<StepState, EffectState>,
        input: serde_json::Value,
    ) -> StageResultFuture<'_> {
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
                    Ok(StageResult {
                        transition: StageTransition::NextStep(serialized_output),
                        delay: Span::new(),
                        compensation: None,
                    })
                }

                Ok(To::Delay { next, delay }) => {
                    let serialized_output = serde_json::to_value(next)
                        .map_err(|e| TaskError::Fatal(e.to_string()))?;
                    Ok(StageResult {
                        transition: StageTransition::NextStep(serialized_output),
                        delay,
                        compensation: None,
                    })
                }

                Ok(To::Effect(effect)) => {
                    let serialized_output = serde_json::to_value(effect)
                        .map_err(|e| TaskError::Fatal(e.to_string()))?;
                    Ok(StageResult {
                        transition: StageTransition::NextEffect(serialized_output),
                        delay: Span::new(),
                        compensation: None,
                    })
                }

                Ok(To::Done) => Ok(StageResult {
                    transition: StageTransition::Done,
                    delay: Span::new(),
                    compensation: None,
                }),

                Err(e) => Err(e),
            }
        })
    }
}

impl<E, StepState, EffectState> EffectExecutor<StepState, EffectState> for E
where
    E: Effect<StepState = StepState, EffectState = EffectState>,
    StepState: Clone + Send + Sync + 'static,
    EffectState: Clone + Send + Sync + 'static,
    E::Input: Serialize + Send + Sync + 'static,
    E::Next: Serialize + Send + Sync + 'static,
    E::NextEffect: Serialize + Send + Sync + 'static,
    E::Compensation: Serialize + Send + Sync + 'static,
{
    fn execute_effect(
        &self,
        cx: Context<StepState, EffectState>,
        input: serde_json::Value,
    ) -> StageResultFuture<'_> {
        let deserialized_input: E::Input = match serde_json::from_value(input) {
            Ok(val) => val,
            Err(e) => return Box::pin(async move { Err(TaskError::Fatal(e.to_string())) }),
        };
        let fut = self.execute(cx, deserialized_input);

        Box::pin(async move {
            match fut.await {
                Ok(EffectOutcome::Next {
                    next,
                    delay,
                    compensation,
                }) => {
                    let serialized_output = serde_json::to_value(next)
                        .map_err(|e| TaskError::Fatal(e.to_string()))?;
                    let serialized_comp = match compensation {
                        Some(comp) => Some(
                            serde_json::to_value(comp)
                                .map_err(|e| TaskError::Fatal(e.to_string()))?,
                        ),
                        None => None,
                    };
                    Ok(StageResult {
                        transition: StageTransition::NextStep(serialized_output),
                        delay,
                        compensation: serialized_comp,
                    })
                }
                Ok(EffectOutcome::Effect {
                    effect,
                    delay,
                    compensation,
                }) => {
                    let serialized_output = serde_json::to_value(effect)
                        .map_err(|e| TaskError::Fatal(e.to_string()))?;
                    let serialized_comp = match compensation {
                        Some(comp) => Some(
                            serde_json::to_value(comp)
                                .map_err(|e| TaskError::Fatal(e.to_string()))?,
                        ),
                        None => None,
                    };
                    Ok(StageResult {
                        transition: StageTransition::NextEffect(serialized_output),
                        delay,
                        compensation: serialized_comp,
                    })
                }
                Ok(EffectOutcome::Done { compensation }) => {
                    let serialized_comp = match compensation {
                        Some(comp) => Some(
                            serde_json::to_value(comp)
                                .map_err(|e| TaskError::Fatal(e.to_string()))?,
                        ),
                        None => None,
                    };
                    Ok(StageResult {
                        transition: StageTransition::Done,
                        delay: Span::new(),
                        compensation: serialized_comp,
                    })
                }
                Err(e) => Err(e),
            }
        })
    }

    fn compensate_effect(
        &self,
        cx: Context<StepState, EffectState>,
        input: serde_json::Value,
    ) -> Pin<Box<dyn Future<Output = TaskResult<()>> + Send + '_>> {
        let deserialized_input: E::Compensation = match serde_json::from_value(input) {
            Ok(val) => val,
            Err(e) => return Box::pin(async move { Err(TaskError::Fatal(e.to_string())) }),
        };

        Box::pin(self.compensate(cx, deserialized_input))
    }

    fn compensation_policy(&self) -> RetryPolicy {
        Effect::compensation_policy(self)
    }
}

mod builder_states {
    use std::marker::PhantomData;

    use sqlx::PgPool;

    use super::JobQueue;

    pub struct Initial;

    pub struct StepSet<Current, Effect> {
        pub _marker: PhantomData<(Current, Effect)>,
    }

    pub struct QueueSet<I, StepState, EffectState>
    where
        I: Send + Sync + 'static,
        StepState: Clone + Send + Sync + 'static,
        EffectState: Clone + Send + Sync + 'static,
    {
        pub queue: JobQueue<I, StepState, EffectState>,
    }

    pub struct QueueNameSet {
        pub queue_name: String,
    }

    pub struct PoolSet {
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
pub struct Builder<I, O, E, StepState, EffectState, B> {
    builder_state: B,
    step_state: StepState,
    effect_state: EffectState,
    steps: Vec<StageConfig<StepState, EffectState>>,
    effects: Vec<StageConfig<StepState, EffectState>>,
    last_config: Option<ConfigTarget>,
    effect_queue_name: Option<String>,
    _marker: PhantomData<(I, O, E)>,
}

impl<I> Default for Builder<I, I, NoEffect, (), (), Initial> {
    fn default() -> Self {
        Self::new()
    }
}

impl<I, O, E, StepState, EffectState, B> Builder<I, O, E, StepState, EffectState, B> {
    /// Sets the queue name for effects.
    ///
    /// Defaults to `"<queue_name>__effects"` when not provided.
    pub fn effect_queue_name(mut self, name: impl Into<String>) -> Self {
        self.effect_queue_name = Some(name.into());
        self
    }

}

impl<I, StepState, EffectState> Builder<I, I, NoEffect, StepState, EffectState, Initial> {
    /// Provides a state shared amongst all steps.
    pub fn state<NewStepState>(
        self,
        state: NewStepState,
    ) -> Builder<I, I, NoEffect, NewStepState, EffectState, Initial> {
        Builder {
            builder_state: self.builder_state,
            step_state: state,
            effect_state: self.effect_state,
            steps: Vec::new(),
            effects: Vec::new(),
            last_config: self.last_config,
            effect_queue_name: self.effect_queue_name,
            _marker: PhantomData,
        }
    }

    /// Provides a state shared amongst all effects.
    pub fn effect_state<NewEffectState>(
        self,
        state: NewEffectState,
    ) -> Builder<I, I, NoEffect, StepState, NewEffectState, Initial> {
        Builder {
            builder_state: self.builder_state,
            step_state: self.step_state,
            effect_state: state,
            steps: Vec::new(),
            effects: Vec::new(),
            last_config: self.last_config,
            effect_queue_name: self.effect_queue_name,
            _marker: PhantomData,
        }
    }
}

impl<I> Builder<I, I, NoEffect, (), (), Initial> {
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
    pub fn new() -> Builder<I, I, NoEffect, (), (), Initial> {
        Builder::<I, I, NoEffect, (), (), _> {
            builder_state: Initial,
            step_state: (),
            effect_state: (),
            steps: Vec::new(),
            effects: Vec::new(),
            last_config: None,
            effect_queue_name: None,
            _marker: PhantomData,
        }
    }

}

// After state set, before first step set.
impl<I, StepState, EffectState> Builder<I, I, NoEffect, StepState, EffectState, Initial>
where
    StepState: Clone + Send + Sync + 'static,
    EffectState: Clone + Send + Sync + 'static,
{
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
    ///         let state = cx.step_state().expect("step context");
    ///         println!("State data: {}", state.data);
    ///         To::done()
    ///     });
    /// ```
    pub fn step<F, O, E, Fut>(mut self, func: F) -> Builder<I, O, E, StepState, EffectState, StepSet<O, E>>
    where
        I: DeserializeOwned + Serialize + Send + Sync + 'static,
        O: Serialize + Send + Sync + 'static,
        E: Serialize + Send + Sync + 'static,
        F: Fn(Context<StepState, EffectState>, I) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = TaskResult<To<O, E>>> + Send + 'static,
    {
        let step_fn = StepFn::new(move |cx, input| Box::pin(func(cx, input)));
        let step_index = self.steps.len();
        self.steps.push(StageConfig {
            executor: StageExecutor::Step(Box::new(step_fn)),
            task_config: StepTaskConfig::default(),
            next_effect_index: None,
        });
        self.last_config = Some(ConfigTarget::Step(step_index));

        Builder {
            builder_state: StepSet {
                _marker: PhantomData,
            },
            step_state: self.step_state,
            effect_state: self.effect_state,
            steps: self.steps,
            effects: self.effects,
            last_config: self.last_config,
            effect_queue_name: self.effect_queue_name,
            _marker: PhantomData,
        }
    }
}

// After first step set.
impl<I, Current, StepState, EffectState>
    Builder<I, Current, NoEffect, StepState, EffectState, StepSet<Current, NoEffect>>
where
    StepState: Clone + Send + Sync + 'static,
    EffectState: Clone + Send + Sync + 'static,
{
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
    pub fn step<F, New, E, Fut>(mut self, func: F) -> Builder<I, New, E, StepState, EffectState, StepSet<New, E>>
    where
        Current: DeserializeOwned + Serialize + Send + Sync + 'static,
        New: Serialize + Send + Sync + 'static,
        E: Serialize + Send + Sync + 'static,
        F: Fn(Context<StepState, EffectState>, Current) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = TaskResult<To<New, E>>> + Send + 'static,
    {
        let step_fn = StepFn::new(move |cx, input| Box::pin(func(cx, input)));
        let step_index = self.steps.len();
        self.steps.push(StageConfig {
            executor: StageExecutor::Step(Box::new(step_fn)),
            task_config: StepTaskConfig::default(),
            next_effect_index: None,
        });
        self.last_config = Some(ConfigTarget::Step(step_index));

        Builder {
            builder_state: StepSet {
                _marker: PhantomData,
            },
            step_state: self.step_state,
            effect_state: self.effect_state,
            steps: self.steps,
            effects: self.effects,
            last_config: self.last_config,
            effect_queue_name: self.effect_queue_name,
            _marker: PhantomData,
        }
    }
}

impl<I, Current, Effect, StepState, EffectState>
    Builder<I, Current, Effect, StepState, EffectState, StepSet<Current, Effect>>
{
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
    ) -> Builder<I, Current, Effect, StepState, EffectState, StepSet<Current, Effect>> {
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
    pub fn timeout(
        mut self,
        timeout: Span,
    ) -> Builder<I, Current, Effect, StepState, EffectState, StepSet<Current, Effect>> {
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
    pub fn ttl(
        mut self,
        ttl: Span,
    ) -> Builder<I, Current, Effect, StepState, EffectState, StepSet<Current, Effect>> {
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
    pub fn delay(
        mut self,
        delay: Span,
    ) -> Builder<I, Current, Effect, StepState, EffectState, StepSet<Current, Effect>> {
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
    pub fn heartbeat(
        mut self,
        heartbeat: Span,
    ) -> Builder<I, Current, Effect, StepState, EffectState, StepSet<Current, Effect>> {
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
    ) -> Builder<I, Current, Effect, StepState, EffectState, StepSet<Current, Effect>> {
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
    pub fn priority(
        mut self,
        priority: i32,
    ) -> Builder<I, Current, Effect, StepState, EffectState, StepSet<Current, Effect>> {
        self.update_task_config(|task_config| task_config.priority = priority);
        self
    }
}

// Encapsulate queue creation.
impl<I, StepState, EffectState>
    Builder<I, (), NoEffect, StepState, EffectState, StepSet<(), NoEffect>>
where
    I: Send + Sync + 'static,
    StepState: Clone + Send + Sync + 'static,
    EffectState: Clone + Send + Sync + 'static,
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
    pub fn name(
        self,
        name: impl Into<String>,
    ) -> Builder<I, (), NoEffect, StepState, EffectState, QueueNameSet> {
        Builder {
            builder_state: QueueNameSet {
                queue_name: name.into(),
            },
            step_state: self.step_state,
            effect_state: self.effect_state,
            steps: self.steps,
            effects: self.effects,
            last_config: self.last_config,
            effect_queue_name: self.effect_queue_name,
            _marker: PhantomData,
        }
    }
}

impl<I, Current, Pending, StepState, EffectState>
    Builder<I, Current, Pending, StepState, EffectState, StepSet<Current, Pending>>
where
    Pending: EffectPending,
    StepState: Clone + Send + Sync + 'static,
    EffectState: Clone + Send + Sync + 'static,
{
    fn effect_handler<Eff>(
        mut self,
        effect: Eff,
    ) -> Builder<I, Eff::Next, Eff::NextEffect, StepState, EffectState, StepSet<Eff::Next, Eff::NextEffect>>
    where
        Eff: Effect<StepState = StepState, EffectState = EffectState, Input = Pending::Input>,
        Eff::Next: Serialize + Send + Sync + 'static,
        Eff::NextEffect: Serialize + Send + Sync + 'static,
        Eff::Compensation: DeserializeOwned + Serialize + Send + Sync + 'static,
    {
        let next_effect_index = self.effects.len();

        self.effects.push(StageConfig {
            executor: StageExecutor::Effect(Box::new(effect)),
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
                _marker: PhantomData,
            },
            step_state: self.step_state,
            effect_state: self.effect_state,
            steps: self.steps,
            effects: self.effects,
            last_config: self.last_config,
            effect_queue_name: self.effect_queue_name,
            _marker: PhantomData,
        }
    }

    /// Add an effect handler after the previous step or effect.
    ///
    /// Effects are intended for external side effects (HTTP calls, emails,
    /// etc.) and are executed on a dedicated effect queue. The next step is
    /// only enqueued after the effect handler succeeds.
    ///
    /// # Example
    ///
    /// ```rust
    /// use serde::{Deserialize, Serialize};
    /// use underway::{job::EffectOutcome, Job, To};
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
    ///         Ok(EffectOutcome::next(NextStep { user_id }))
    ///     })
    ///     .step(|_cx, _| async move { To::done() });
    /// ```
    pub fn effect<F, Fut, Next, NextEffect>(
        self,
        func: F,
    ) -> Builder<I, Next, NextEffect, StepState, EffectState, StepSet<Next, NextEffect>>
    where
        F: Fn(Context<StepState, EffectState>, Pending::Input) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = TaskResult<EffectOutcome<Next, NextEffect, NoCompensation>>>
            + Send
            + 'static,
        Next: Serialize + Send + Sync + 'static,
        NextEffect: Serialize + Send + Sync + 'static,
    {
        let effect = EffectClosure::new(func);
        self.effect_handler(effect)
    }

    /// Add an effect handler with a compensation callback.
    pub fn effect_with_compensation<F, Fut, Comp, CompFut, Next, NextEffect, CompInput>(
        self,
        func: F,
        compensate: Comp,
    ) -> Builder<I, Next, NextEffect, StepState, EffectState, StepSet<Next, NextEffect>>
    where
        F: Fn(Context<StepState, EffectState>, Pending::Input) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = TaskResult<EffectOutcome<Next, NextEffect, CompInput>>>
            + Send
            + 'static,
        Comp: Fn(Context<StepState, EffectState>, CompInput) -> CompFut + Send + Sync + 'static,
        CompFut: Future<Output = TaskResult<()>> + Send + 'static,
        Next: Serialize + Send + Sync + 'static,
        NextEffect: Serialize + Send + Sync + 'static,
        CompInput: DeserializeOwned + Serialize + Send + Sync + 'static,
    {
        let effect = EffectClosure::new(func).with_compensation(compensate);
        self.effect_handler(effect)
    }
}

impl<I, StepState, EffectState> Builder<I, (), NoEffect, StepState, EffectState, QueueNameSet>
where
    I: Send + Sync + 'static,
    StepState: Clone + Send + Sync + 'static,
    EffectState: Clone + Send + Sync + 'static,
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
    pub fn pool(self, pool: PgPool) -> Builder<I, (), NoEffect, StepState, EffectState, PoolSet> {
        let QueueNameSet { queue_name } = self.builder_state;
        Builder {
            builder_state: PoolSet { queue_name, pool },
            step_state: self.step_state,
            effect_state: self.effect_state,
            steps: self.steps,
            effects: self.effects,
            last_config: self.last_config,
            effect_queue_name: self.effect_queue_name,
            _marker: PhantomData,
        }
    }
}

impl<I, StepState, EffectState> Builder<I, (), NoEffect, StepState, EffectState, PoolSet>
where
    I: Send + Sync + 'static,
    StepState: Clone + Send + Sync + 'static,
    EffectState: Clone + Send + Sync + 'static,
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
    pub async fn build(self) -> Result<Job<I, StepState, EffectState>> {
        let PoolSet { queue_name, pool } = self.builder_state;
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
            step_state: self.step_state,
            effect_state: self.effect_state,
            _marker: PhantomData,
        })
    }
}

// Directly provide queue.
impl<I, StepState, EffectState>
    Builder<I, (), NoEffect, StepState, EffectState, StepSet<(), NoEffect>>
where
    I: Send + Sync + 'static,
    StepState: Clone + Send + Sync + 'static,
    EffectState: Clone + Send + Sync + 'static,
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
    pub fn queue(
        self,
        queue: JobQueue<I, StepState, EffectState>,
    ) -> Builder<I, (), NoEffect, StepState, EffectState, QueueSet<I, StepState, EffectState>> {
        Builder {
            builder_state: QueueSet { queue },
            step_state: self.step_state,
            effect_state: self.effect_state,
            steps: self.steps,
            effects: self.effects,
            last_config: self.last_config,
            effect_queue_name: self.effect_queue_name,
            _marker: PhantomData,
        }
    }
}

impl<I, StepState, EffectState>
    Builder<I, (), NoEffect, StepState, EffectState, QueueSet<I, StepState, EffectState>>
where
    I: Send + Sync + 'static,
    StepState: Clone + Send + Sync + 'static,
    EffectState: Clone + Send + Sync + 'static,
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
    pub async fn build(self) -> Result<Job<I, StepState, EffectState>> {
        let QueueSet { queue } = self.builder_state;
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
            step_state: self.step_state,
            effect_state: self.effect_state,
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

        async fn step(_cx: Context<(), ()>, Input { message }: Input) -> TaskResult<To<()>> {
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
                let Context::Step { state, .. } = cx else {
                    return Err(TaskError::Fatal("Expected step context.".into()));
                };
                println!(
                    "Executing job with message: {message} and state: {state}",
                    state = state.data
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
                let Context::Step { state, .. } = cx else {
                    return Err(TaskError::Fatal("Expected step context.".into()));
                };
                let mut data = state.data.lock().expect("Mutex should not be poisoned");
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

        async fn step(cx: Context<State, ()>, Input { message }: Input) -> TaskResult<To<()>> {
            let Context::Step { state, .. } = cx else {
                return Err(TaskError::Fatal("Expected step context.".into()));
            };
            println!(
                "Executing job with message: {message} and state: {data}",
                data = state.data
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
            .step(|cx, _| async move {
                let Context::Step {
                    step_index,
                    step_count,
                    queue_name,
                    ..
                } = cx
                else {
                    return Err(TaskError::Fatal("Expected step context.".into()));
                };
                assert_eq!(step_index, 0);
                assert_eq!(step_count, 1);
                assert_eq!(queue_name.as_str(), "one_step_context_attributes");
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
                let Context::Step { state, .. } = cx else {
                    return Err(TaskError::Fatal("Expected step context.".into()));
                };
                println!(
                    "Executing job with message: {message} and state: {state}",
                    state = state.data
                );
                To::next(Step2 {
                    data: message.as_bytes().into(),
                })
            })
            .step(|cx, Step2 { data }| async move {
                let Context::Step { state, .. } = cx else {
                    return Err(TaskError::Fatal("Expected step context.".into()));
                };
                println!(
                    "Executing job with data: {data:?} and state: {state}",
                    state = state.data
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
            .state(state.clone())
            .effect_state(state)
            .step(|cx, Step1 { message }| async move {
                let Context::Step { state, .. } = cx else {
                    return Err(TaskError::Fatal("Expected step context.".into()));
                };
                state
                    .events
                    .lock()
                    .expect("Mutex should not be poisoned")
                    .push("step1".to_string());
                To::effect(SendEmail { message })
            })
            .effect(|cx, SendEmail { message }| async move {
                let Context::Effect { state, .. } = cx else {
                    return Err(TaskError::Fatal("Expected effect context.".into()));
                };
                state
                    .events
                    .lock()
                    .expect("Mutex should not be poisoned")
                    .push("effect".to_string());
                Ok(EffectOutcome::next(Step2 { message }))
            })
            .step(|cx, Step2 { message }| async move {
                let Context::Step { state, .. } = cx else {
                    return Err(TaskError::Fatal("Expected step context.".into()));
                };
                state
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
    async fn compensation_runs_on_failure(pool: PgPool) -> sqlx::Result<(), Error> {
        #[derive(Clone)]
        struct State {
            events: Arc<Mutex<Vec<String>>>,
        }

        #[derive(Serialize, Deserialize)]
        struct Step1 {
            message: String,
        }

        #[derive(Serialize, Deserialize)]
        struct Step2 {
            message: String,
        }

        #[derive(Serialize, Deserialize)]
        struct SendEmail {
            message: String,
        }

        #[derive(Serialize, Deserialize)]
        struct UndoEmail {
            message: String,
        }

        let queue = Queue::builder()
            .name("compensation_runs_on_failure")
            .pool(pool.clone())
            .build()
            .await?;

        let events = Arc::new(Mutex::new(Vec::new()));
        let state = State {
            events: Arc::clone(&events),
        };

        let job = Job::builder()
            .state(state.clone())
            .effect_state(state)
            .step(|cx, Step1 { message }| async move {
                let Context::Step { state, .. } = cx else {
                    return Err(TaskError::Fatal("Expected step context.".into()));
                };
                state
                    .events
                    .lock()
                    .expect("Mutex should not be poisoned")
                    .push("step1".to_string());
                To::effect(SendEmail { message })
            })
            .effect_with_compensation(
                |cx, SendEmail { message }| async move {
                    let Context::Effect { state, .. } = cx else {
                        return Err(TaskError::Fatal("Expected effect context.".into()));
                    };
                    state
                        .events
                        .lock()
                        .expect("Mutex should not be poisoned")
                        .push(format!("effect:{message}"));
                    Ok(EffectOutcome::next_with_compensation(
                        Step2 {
                            message: message.clone(),
                        },
                        UndoEmail { message },
                    ))
                },
                |cx, UndoEmail { message }| async move {
                    let Context::Effect { state, .. } = cx else {
                        return Err(TaskError::Fatal("Expected effect context.".into()));
                    };
                    state
                        .events
                        .lock()
                        .expect("Mutex should not be poisoned")
                        .push(format!("compensate:{message}"));
                    Ok(())
                },
            )
            .step(|_cx, _input: Step2| async move { Err(TaskError::Fatal("boom".into())) })
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
        job.effect_worker().process_next_task().await?;

        let events = events.lock().expect("Mutex should not be poisoned").clone();
        assert_eq!(
            events,
            vec![
                "step1".to_string(),
                "effect:hello".to_string(),
                "compensate:hello".to_string()
            ]
        );

        let job_effect = sqlx::query!(
            r#"
            select
              compensation_enqueued_at is not null as "compensation_enqueued!: bool",
              compensated_at is not null as "compensated!: bool"
            from underway.job_effect
            "#,
        )
        .fetch_one(&pool)
        .await?;

        assert!(job_effect.compensation_enqueued);
        assert!(job_effect.compensated);

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
