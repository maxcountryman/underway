//! Workflows are a series of sequential steps, where each step is a [`Task`].
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
//! Each step executes as its own task and is durably persisted before the next
//! step is enqueued. If execution is interrupted, the workflow resumes from the
//! last persisted step.
//!
//! # Defining workflows
//!
//! Workflows are formed from at least one step function.
//!
//! ```rust
//! use underway::{To, Workflow};
//!
//! let workflow_builder = Workflow::<(), ()>::builder().step(|_cx, _| async move { To::done() });
//! ```
//!
//! Instead of a closure, we could also use a named function.
//!
//! ```rust
//! use underway::{task::Result as TaskResult, workflow::Context, To, Workflow};
//!
//! async fn named_step(_cx: Context<()>, _: ()) -> TaskResult<To<()>> {
//!     To::done()
//! }
//!
//! let workflow_builder = Workflow::<_, ()>::builder().step(named_step);
//! ```
//!
//! Notice that the first argument to our function is [a context
//! binding](crate::workflow::Context). This provides access to fields like
//! [`state`](crate::workflow::Context::state) and
//! [`workflow_run_id`](crate::workflow::Context::workflow_run_id), as well as
//! workflow helpers like [`call`](crate::workflow::Context::call) and
//! [`emit`](crate::workflow::Context::emit).
//! Activity calls are type-checked against handlers registered on
//! [`workflow::Builder::activity`](crate::workflow::Builder::activity).
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
//! use underway::{To, Workflow};
//!
//! // Our very own input type.
//! #[derive(Serialize, Deserialize)]
//! struct MyArgs {
//!     name: String,
//! };
//!
//! let workflow_builder =
//!     Workflow::<_, ()>::builder().step(|_cx, MyArgs { name }| async move { To::done() });
//! ```
//!
//! Besides input, it's also important to point out that we return a specific
//! type that indicates what to do next. So far, we've only had a single step
//! and so we've returned [`To::done`].
//!
//! But workflows may be composed of any number of sequential steps. Each step
//! is structured to take the output of the last step. By returning [`To::next`]
//! with the input of the next step, we move on to that step.
//!
//! ```rust
//! use serde::{Deserialize, Serialize};
//! use underway::{To, Workflow};
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
//! let workflow_builder = Workflow::<_, ()>::builder()
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
//! use underway::{Workflow, To};
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
//! let workflow_builder = Workflow::<_, ()>::builder()
//!     .step(|_cx, Step1 { n }| async move {
//!         println!("Got {n}");
//!
//!         // This does not compile!
//!         To::next(Step1 { n })
//!     })
//!     .step(|_cx, Step2 { original, new }| async move {
//!         println!("Was {original} now is {new}");
//!         To::done()
//!     });
//! ```
//!
//! Often we want to immediately transition to the next step. However, there may
//! be cases where we want to wait some time beforehand. We can return
//! [`To::delay_for`] to express this.
//!
//! Like transitioning to the next step, we give the input still but also supply
//! a delay as the second argument.
//!
//! ```rust
//! use jiff::ToSpan;
//! use serde::{Deserialize, Serialize};
//! use underway::{To, Workflow};
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
//! let workflow_builder = Workflow::<_, ()>::builder()
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
//! Then this span may be used as our delay given to `To::delay_for`.
//!
//! # Stateful workflows
//!
//! So far, we've ignored the context binding. One reason we need it is to
//! access shared state we've set on the workflow.
//!
//! State like this can be useful when there may be resources or configuration
//! that all steps should have access to. The only requirement is that the state
//! type be `Clone`, as it will be cloned into each step.
//! ```rust
//! use underway::{workflow::Context, To, Workflow};
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
//! let workflow_builder = Workflow::<(), _>::builder()
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
//! However, **please use caution**: state is maintained between workflow
//! executions and is not reset. This means that independent step executions,
//! **including those that may have originated from different enqueues of the
//! workflow**, will have access to the same state. For this reason, this
//! pattern is discouraged.
//!
//! # Durable side effects
//!
//! Workflow steps do not expose a raw database transaction directly.
//! Use durable activity helpers for side effects instead.
//!
//! Instead, durable side effects are modeled with workflow helpers:
//! - [`Context::emit`] for fire-and-forget intents, and
//! - [`Context::call`] for request/response effects.
//!
//! Activities must be registered on the workflow builder before any step that
//! calls or emits them. Missing registrations fail at compile time.
//!
//! A `call` does **not** require user polling loops. When a call has not
//! completed yet, it suspends the current step. The worker marks the task as
//! waiting, and once the activity completes the step is replayed and the same
//! call key resolves to the persisted result.
//!
//! ```rust
//! use serde::{Deserialize, Serialize};
//! use underway::{Activity, To, Workflow};
//!
//! #[derive(Serialize, Deserialize)]
//! struct Step1 {
//!     user_id: i64,
//! }
//!
//! #[derive(Serialize, Deserialize)]
//! struct Step2 {
//!     profile_id: i64,
//! }
//!
//! struct CreateProfile;
//!
//! impl Activity for CreateProfile {
//!     const NAME: &'static str = "create-profile";
//!
//!     type Input = i64;
//!     type Output = i64;
//!
//!     async fn execute(&self, user_id: Self::Input) -> underway::activity::Result<Self::Output> {
//!         Ok(user_id)
//!     }
//! }
//!
//! struct WriteAuditLog;
//!
//! impl Activity for WriteAuditLog {
//!     const NAME: &'static str = "write-audit-log";
//!
//!     type Input = i64;
//!     type Output = ();
//!
//!     async fn execute(&self, _user_id: Self::Input) -> underway::activity::Result<Self::Output> {
//!         Ok(())
//!     }
//! }
//!
//! let workflow_builder = Workflow::<_, ()>::builder()
//!     .activity(CreateProfile)
//!     .activity(WriteAuditLog)
//!     .step(|mut cx, Step1 { user_id }| async move {
//!         // Fire-and-forget effect intent.
//!         cx.emit::<WriteAuditLog, _>("audit", &user_id).await?;
//!
//!         // Suspends/resumes durably until completion.
//!         let profile_id: i64 = cx.call::<CreateProfile, _>("profile", &user_id).await?;
//!         To::next(Step2 { profile_id })
//!     })
//!     .step(|_cx, Step2 { profile_id: _ }| async move { To::done() });
//! ```
//!
//! Call keys should be stable and deterministic for a given step execution.
//! Reusing a key with a different activity name or input is treated as
//! non-deterministic and fails execution.
//!
//! Because [`Context::call`] requires mutable context, issuing multiple
//! unresolved calls in parallel fails to compile. Issue calls sequentially as
//! `call(...).await?`.
//!
//! If a workflow needs direct transaction-level database semantics, implement
//! [`Task`] directly and use its `execute` method, which receives
//! a transaction.
//!
//! # Retry policies
//!
//! Steps being tasks also have associated retry policies. This policy inherits
//! the default but can be provided for each step.
//!
//! ```rust
//! use serde::{Deserialize, Serialize};
//! use underway::{task::RetryPolicy, To, Workflow};
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
//! let workflow_builder = Workflow::<_, ()>::builder()
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
//! Steps can configure the same task-level settings as standalone tasks. Each
//! step can set its timeout, TTL, delay, heartbeat interval, concurrency key,
//! and priority. Base delays are added to any delay returned by
//! [`To::delay_for`].
//!
//! ```rust
//! use jiff::ToSpan;
//! use underway::{To, Workflow};
//!
//! let workflow_builder = Workflow::<(), ()>::builder()
//!     .step(|_cx, _| async move { To::done() })
//!     .timeout(2.minutes())
//!     .ttl(7.days())
//!     .delay(10.seconds())
//!     .heartbeat(5.seconds())
//!     .concurrency_key("customer:42")
//!     .priority(10);
//! ```
//!
//! # Enqueuing workflows
//!
//! Once we've configured our workflow with its sequence of one or more steps we
//! can build the workflow and enqueue it with input.
//!
//! ```rust,no_run
//! # use sqlx::PgPool;
//! use underway::{To, Workflow};
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
//! let workflow = Workflow::builder()
//!     .step(|_cx, _| async move { To::done() })
//!     .name("example-workflow")
//!     .pool(pool)
//!     .build()
//!     .await?;
//!
//! // Enqueue a new workflow with the given input `()`.
//! workflow.enqueue(&()).await?;
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! # });
//! # }
//! ```
//!
//! We could also supply a queue that's already been constructed, to use as our
//! workflow's queue. This obviates the need to await the workflow build method.
//!
//! ```rust,no_run
//! # use sqlx::PgPool;
//! use underway::{Queue, To, Workflow};
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
//!     .name("example-workflow")
//!     .pool(pool)
//!     .build()
//!     .await?;
//!
//! let workflow = Workflow::builder()
//!     .step(|_cx, _| async move { To::done() })
//!     .queue(queue)
//!     .build();
//!
//! workflow.enqueue(&()).await?;
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
//! use underway::{To, Workflow};
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
//! let workflow = Workflow::builder()
//!     .step(|_cx, Input { bucket_name }| async move { To::done() })
//!     .name("example-workflow")
//!     .pool(pool)
//!     .build()
//!     .await?;
//!
//! // Enqueue a new workflow with a slightly more interesting value.
//! workflow
//!     .enqueue(&Input {
//!         bucket_name: "my_bucket".to_string(),
//!     })
//!     .await?;
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! # });
//! # }
//! ```
//!
//! While we're only demonstrating a single step here for brevity, the process
//! is the same for workflows with multiple steps.
//!
//! ## Atomic enqueue
//!
//! The `enqueue` method uses a connection from the queue's pool. If we prefer
//! instead to use a transaction supplied by the surrounding code we can use
//! [`enqueue_using`](Workflow::enqueue_using).
//!
//! By doing so, we can ensure that the enqueue will only happen when the
//! transaction is committed.
//!
//! ```rust,no_run
//! # use sqlx::PgPool;
//! use serde::{Deserialize, Serialize};
//! use underway::{To, Workflow};
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
//! let workflow = Workflow::builder()
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
//! workflow.enqueue_using(&mut *tx, &()).await?;
//!
//! # /*
//! /* ...And more intervening logic involving `tx`. */
//! # */
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! # });
//! # }
//! ```
//!
//! # Running workflows
//!
//! Workflows are run via [`Runtime`], which orchestrates workers,
//! schedulers, and activity execution.
//!
//! ```rust,no_run
//! # use sqlx::PgPool;
//! use underway::{To, Workflow};
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
//! let workflow = Workflow::builder()
//!     .step(|_cx, _: ()| async move { To::done() })
//!     .name("example-workflow")
//!     .pool(pool)
//!     .build()
//!     .await?;
//!
//! // This starts runtime workers in the background (non-blocking).
//! let runtime_handle = workflow.runtime().start();
//! runtime_handle.shutdown().await?;
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! # });
//! # }
//! ```
//!
//! We can also run the runtime in a blocking manner directly.
//!
//! ```rust,no_run
//! # use sqlx::PgPool;
//! use underway::{To, Workflow};
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
//! let workflow = Workflow::builder()
//!     .step(|_cx, _: ()| async move { To::done() })
//!     .name("example-workflow")
//!     .pool(pool)
//!     .build()
//!     .await?;
//!
//! // This starts runtime workers and blocks.
//! workflow.runtime().run().await?;
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! # });
//! # }
//! ```
//!
//! Workers and schedulers can be used directly via runtime helpers too.
//!
//! ```rust,no_run
//! # use sqlx::PgPool;
//! use underway::{To, Workflow};
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
//! let workflow = Workflow::builder()
//!     .step(|_cx, _: ()| async move { To::done() })
//!     .name("example-workflow")
//!     .pool(pool)
//!     .build()
//!     .await?;
//!
//! let runtime = workflow.runtime();
//! let worker = runtime.worker();
//! let scheduler = runtime.scheduler();
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! # });
//! # }
//! ```
//!
//! # Scheduling workflows
//!
//! Workflows may also be run on a schedule that follows the form of a cron-like
//! expression.
//!
//! ```rust,no_run
//! # use sqlx::PgPool;
//! use underway::{To, Workflow};
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
//! let workflow = Workflow::builder()
//!     .step(|_cx, _: ()| async move { To::done() })
//!     .name("scheduled-workflow")
//!     .pool(pool)
//!     .build()
//!     .await?;
//!
//! // Sets a weekly schedule with the given input.
//! let weekly = "@weekly[America/Los_Angeles]".parse()?;
//! workflow.schedule(&weekly, &()).await?;
//!
//! let runtime_handle = workflow.runtime().start();
//! runtime_handle.shutdown().await?;
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! # });
//! # }
//! ```
//!
//! Often input to a scheduled workflow would consist of static configuration or
//! other fields that are shared for scheduled runs.
//!
//! Also note that workflows with schedules may still be enqueued manually when
//! desired.

mod builder;
mod context;
#[doc(hidden)]
pub mod registration;
mod step;

use std::{
    marker::PhantomData,
    ops::Deref,
    sync::{Arc, Mutex},
    time::Duration as StdDuration,
};

pub use builder::Builder;
use builder::Initial;
pub use context::Context;
use context::{ActivityCallBuffer, ActivityCallRecord, CallSequenceState, ContextParts};
use jiff::Span;
use sealed::{WorkflowScheduleTemplate, WorkflowState};
use serde::{Deserialize, Serialize};
use sqlx::{PgConnection, PgExecutor, Postgres, Transaction};
pub use step::To;
use step::{StepConfig, StepTaskConfig};
use tracing::instrument;
use ulid::Ulid;
use uuid::Uuid;

use crate::{
    activity::CallState,
    activity_worker::ActivityRegistry,
    queue::{Error as QueueError, InProgressTask, Queue},
    runtime::Runtime,
    scheduler::{Error as SchedulerError, ZonedSchedule},
    task::{
        Error as TaskError, Result as TaskResult, RetryPolicy, State as TaskState, Task, TaskId,
    },
    worker::Error as WorkerError,
    workflow::registration::NoActivities,
};

type Result<T = ()> = std::result::Result<T, Error>;

/// Workflow errors.
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

    /// Error returned from serde_json.
    #[error(transparent)]
    Json(#[from] serde_json::Error),

    /// Error returned from database operations.
    #[error(transparent)]
    Database(#[from] sqlx::Error),
}

type WorkflowQueue<T, S> = Queue<Workflow<T, S>>;

mod sealed {
    use serde::{Deserialize, Serialize};

    use super::WorkflowRunId;

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    pub struct WorkflowScheduleTemplate {
        pub step_index: usize,
        pub step_input: serde_json::Value,
    } // TODO: Versioning?

    #[derive(Debug, Serialize, Deserialize, PartialEq)]
    pub struct WorkflowState {
        pub step_index: usize,
        pub step_input: serde_json::Value,
        #[serde(default = "WorkflowRunId::new")]
        pub(crate) workflow_run_id: WorkflowRunId,
    } // TODO: Versioning?
}

/// Unique identifier of a workflow run.
///
/// Wraps a UUID which is generated via a ULID.
///
/// Each enqueue of a workflow receives its own ID. IDs are embedded in the
/// input of tasks on the queue. This means that each task related to a workflow
/// run will have the same workflow run ID.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
pub struct WorkflowRunId(Uuid);

impl WorkflowRunId {
    fn new() -> Self {
        Self(Ulid::new().into())
    }
}

impl Deref for WorkflowRunId {
    type Target = Uuid;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// Represents a specific workflow that's been enqueued.
///
/// This handle allows for manipulating the state of the workflow in the queue.
pub struct EnqueuedWorkflow<T: Task> {
    run_id: WorkflowRunId,
    queue: Arc<Queue<T>>,
}

impl<T: Task> EnqueuedWorkflow<T> {
    /// Cancels the workflow if it's still pending.
    ///
    /// Because workflows may be composed of multiple steps, the full set of
    /// tasks is searched and any pending tasks are cancelled.
    ///
    /// Returns `true` if any tasks were successfully cancelled. Put another
    /// way, if tasks are already cancelled or not eligible for cancellation
    /// then this returns `false`.
    ///
    /// # Errors
    ///
    /// This will return an error if the database operation fails and if the
    /// workflow run ID cannot be found.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use sqlx::PgPool;
    /// # use underway::{Workflow, To};
    /// # use tokio::runtime::Runtime;
    /// # fn main() {
    /// # let rt = Runtime::new().unwrap();
    /// # rt.block_on(async {
    /// # let pool = PgPool::connect(&std::env::var("DATABASE_URL")?).await?;
    /// # let workflow = Workflow::builder()
    /// #     .step(|_cx, _| async move { To::done() })
    /// #     .name("example")
    /// #     .pool(pool)
    /// #     .build()
    /// #     .await?;
    /// # /*
    /// let workflow = { /* A `Workflow`. */ };
    /// # */
    /// #
    ///
    /// let enqueued = workflow.enqueue(&()).await?;
    ///
    /// // Cancel the enqueued workflow.
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
            where input->>'workflow_run_id' = $1
              and state = $2
            for update skip locked
            "#,
            self.run_id.to_string(),
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
pub struct Workflow<I, S>
where
    I: Sync + Send + 'static,
    S: Clone + Sync + Send + 'static,
{
    queue: Arc<WorkflowQueue<I, S>>,
    steps: Arc<Vec<StepConfig<S>>>,
    state: S,
    activity_registry: ActivityRegistry,
    _marker: PhantomData<fn() -> I>,
}

impl<I, S> Workflow<I, S>
where
    I: Serialize + Sync + Send + 'static,
    S: Clone + Send + Sync + 'static,
{
    /// Creates a new workflow builder.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use sqlx::PgPool;
    /// use underway::{To, Workflow};
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
    /// let workflow = Workflow::<(), ()>::builder()
    ///     .step(|_cx, _| async move { To::done() })
    ///     .name("example")
    ///     .pool(pool)
    ///     .build()
    ///     .await?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # });
    /// # }
    /// ```
    pub fn builder() -> Builder<I, I, S, Initial, NoActivities> {
        Builder::<I, I, S, Initial, NoActivities>::new()
    }
}

impl<I, S> Workflow<I, S>
where
    I: Serialize + Sync + Send + 'static,
    S: Clone + Send + Sync + 'static,
{
    /// Enqueue the workflow using a connection from the queue's pool.
    ///
    /// # Errors
    ///
    /// This has the same error conditions as [`Queue::enqueue`].
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use sqlx::PgPool;
    /// # use underway::{Workflow, To};
    /// # use tokio::runtime::Runtime;
    /// # fn main() {
    /// # let rt = Runtime::new().unwrap();
    /// # rt.block_on(async {
    /// # let pool = PgPool::connect(&std::env::var("DATABASE_URL")?).await?;
    /// # let workflow = Workflow::builder()
    /// #     .step(|_cx, _| async move { To::done() })
    /// #     .name("example")
    /// #     .pool(pool)
    /// #     .build()
    /// #     .await?;
    /// # /*
    /// let workflow = { /* A `Workflow`. */ };
    /// # */
    /// #
    ///
    /// // Enqueue a new workflow with the given input.
    /// workflow.enqueue(&()).await?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # });
    /// # }
    /// ```
    pub async fn enqueue(&self, input: &I) -> Result<EnqueuedWorkflow<Self>> {
        let mut conn = self.queue.pool.acquire().await?;
        self.enqueue_using(&mut *conn, input).await
    }

    /// Enqueue the workflow multiple times using a connection from the queue's
    /// pool.
    ///
    /// # Errors
    ///
    /// This has the same error conditions as [`Queue::enqueue_many`].
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use sqlx::PgPool;
    /// # use underway::{Workflow, To};
    /// # use tokio::runtime::Runtime;
    /// # fn main() {
    /// # let rt = Runtime::new().unwrap();
    /// # rt.block_on(async {
    /// # let pool = PgPool::connect(&std::env::var("DATABASE_URL")?).await?;
    /// # let workflow = Workflow::builder()
    /// #     .step(|_cx, _| async move { To::done() })
    /// #     .name("example")
    /// #     .pool(pool)
    /// #     .build()
    /// #     .await?;
    /// # /*
    /// let workflow = { /* A `Workflow`. */ };
    /// # */
    /// #
    ///
    /// // Enqueue a few workflows at once.
    /// let enqueued = workflow.enqueue_many(&[(), ()]).await?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # });
    /// # }
    /// ```
    pub async fn enqueue_many(&self, inputs: &[I]) -> Result<Vec<EnqueuedWorkflow<Self>>> {
        let mut conn = self.queue.pool.acquire().await?;
        self.enqueue_many_using(&mut *conn, inputs).await
    }

    /// Enqueue the workflow using the provided executor.
    ///
    /// This allows workflows to be enqueued using the same transaction as an
    /// application may already be using in a given context.
    ///
    /// **Note:** If you pass a transactional executor and the transaction is
    /// rolled back, the returned workflow run ID will not correspond to any
    /// persisted task.
    ///
    /// # Errors
    ///
    /// This has the same error conditions as [`Queue::enqueue`].
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use sqlx::PgPool;
    /// # use underway::{Workflow, To};
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
    /// # let workflow = Workflow::builder()
    /// #     .step(|_cx, _| async move { To::done() })
    /// #     .name("example")
    /// #     .pool(pool.clone())
    /// #     .build()
    /// #     .await?;
    /// # /*
    /// let workflow = { /* A `Workflow`. */ };
    /// # */
    /// #
    ///
    /// // Enqueue using the transaction we already have.
    /// workflow.enqueue_using(&mut *tx, &()).await?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # });
    /// # }
    /// ```
    pub async fn enqueue_using<'a, E>(
        &self,
        executor: E,
        input: &I,
    ) -> Result<EnqueuedWorkflow<Self>>
    where
        E: PgExecutor<'a>,
    {
        self.enqueue_after_using(executor, input, Span::new()).await
    }

    /// Enqueue the workflow multiple times using the provided executor.
    ///
    /// This allows workflows to be enqueued using the same transaction as an
    /// application may already be using in a given context.
    ///
    /// **Note:** If you pass a transactional executor and the transaction is
    /// rolled back, the returned workflow run IDs will not correspond to any
    /// persisted tasks.
    ///
    /// # Errors
    ///
    /// This has the same error conditions as [`Queue::enqueue_many`].
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use sqlx::PgPool;
    /// # use underway::{Workflow, To};
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
    /// # let workflow = Workflow::builder()
    /// #     .step(|_cx, _| async move { To::done() })
    /// #     .name("example")
    /// #     .pool(pool.clone())
    /// #     .build()
    /// #     .await?;
    /// # /*
    /// let workflow = { /* A `Workflow`. */ };
    /// # */
    /// #
    ///
    /// // Enqueue using the transaction we already have.
    /// let enqueued = workflow.enqueue_many_using(&mut *tx, &[(), ()]).await?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # });
    /// # }
    /// ```
    pub async fn enqueue_many_using<'a, E>(
        &self,
        executor: E,
        inputs: &[I],
    ) -> Result<Vec<EnqueuedWorkflow<Self>>>
    where
        E: PgExecutor<'a> + sqlx::Acquire<'a, Database = sqlx::Postgres>,
    {
        if inputs.is_empty() {
            return Ok(Vec::new());
        }

        let workflow_inputs = inputs
            .iter()
            .map(|input| self.first_workflow_input(input))
            .collect::<Result<Vec<_>>>()?;

        self.queue
            .enqueue_many(executor, self, &workflow_inputs)
            .await?;

        let enqueued = workflow_inputs
            .into_iter()
            .map(|workflow_input| EnqueuedWorkflow {
                run_id: workflow_input.workflow_run_id,
                queue: self.queue.clone(),
            })
            .collect();

        Ok(enqueued)
    }

    /// Enqueue the workflow after the given delay using a connection from the
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
    /// # use underway::{Workflow, To};
    /// use jiff::ToSpan;
    ///
    /// # use tokio::runtime::Runtime;
    /// # fn main() {
    /// # let rt = Runtime::new().unwrap();
    /// # rt.block_on(async {
    /// # let pool = PgPool::connect(&std::env::var("DATABASE_URL")?).await?;
    /// # let workflow = Workflow::builder()
    /// #     .step(|_cx, _| async move { To::done() })
    /// #     .name("example")
    /// #     .pool(pool)
    /// #     .build()
    /// #     .await?;
    /// # /*
    /// let workflow = { /* A `Workflow`. */ };
    /// # */
    /// #
    ///
    /// // Enqueue after an hour.
    /// workflow.enqueue_after(&(), 1.hour()).await?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # });
    /// # }
    /// ```
    pub async fn enqueue_after(&self, input: &I, delay: Span) -> Result<EnqueuedWorkflow<Self>> {
        let mut conn = self.queue.pool.acquire().await?;
        self.enqueue_after_using(&mut *conn, input, delay).await
    }

    /// Enqueue the workflow using the provided executor after the given delay.
    ///
    /// The given delay is added to the task's configured delay, if one is set.
    ///
    /// This allows workflows to be enqueued using the same transaction as an
    /// application may already be using in a given context.
    ///
    /// **Note:** If you pass a transactional executor and the transaction is
    /// rolled back, the returned workflow run ID will not correspond to any
    /// persisted task.
    ///
    /// # Errors
    ///
    /// This has the same error conditions as [`Queue::enqueue_after`].
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use sqlx::PgPool;
    /// # use underway::{Workflow, To};
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
    /// # let workflow = Workflow::builder()
    /// #     .step(|_cx, _| async move { To::done() })
    /// #     .name("example")
    /// #     .pool(pool.clone())
    /// #     .build()
    /// #     .await?;
    /// # /*
    /// let workflow = { /* A `Workflow`. */ };
    /// # */
    /// #
    ///
    /// // Enqueue after two days using the transaction we already have.
    /// workflow
    ///     .enqueue_after_using(&mut *tx, &(), 2.days())
    ///     .await?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # });
    /// # }
    /// ```
    pub async fn enqueue_after_using<'a, E>(
        &self,
        executor: E,
        input: &I,
        delay: Span,
    ) -> Result<EnqueuedWorkflow<Self>>
    where
        E: PgExecutor<'a>,
    {
        let workflow_input = self.first_workflow_input(input)?;

        self.queue
            .enqueue_after(executor, self, &workflow_input, delay)
            .await?;

        let enqueue = EnqueuedWorkflow {
            run_id: workflow_input.workflow_run_id,
            queue: self.queue.clone(),
        };

        Ok(enqueue)
    }

    /// Schedule the workflow using a connection from the queue's pool.
    ///
    /// # Errors
    ///
    /// This has the same error conditions as [`Queue::schedule`].
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use sqlx::PgPool;
    /// # use underway::{Workflow, To};
    /// # use tokio::runtime::Runtime;
    /// # fn main() {
    /// # let rt = Runtime::new().unwrap();
    /// # rt.block_on(async {
    /// # let pool = PgPool::connect(&std::env::var("DATABASE_URL")?).await?;
    /// # let workflow = Workflow::builder()
    /// #     .step(|_cx, _| async move { To::done() })
    /// #     .name("example")
    /// #     .pool(pool)
    /// #     .build()
    /// #     .await?;
    /// # /*
    /// let workflow = { /* A `Workflow`. */ };
    /// # */
    /// #
    ///
    /// let every_minute = "0 * * * *[America/Los_Angeles]".parse()?;
    /// workflow.schedule(&every_minute, &()).await?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # });
    /// # }
    /// ```
    pub async fn schedule(&self, zoned_schedule: &ZonedSchedule, input: &I) -> Result {
        let mut conn = self.queue.pool.acquire().await?;
        self.schedule_using(&mut *conn, zoned_schedule, input).await
    }

    /// Schedule the workflow using the provided executor.
    ///
    /// This allows workflows to be scheduled using the same transaction as an
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
    /// # use underway::{Workflow, To};
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
    /// # let workflow = Workflow::builder()
    /// #     .step(|_cx, _| async move { To::done() })
    /// #     .name("example")
    /// #     .pool(pool.clone())
    /// #     .build()
    /// #     .await?;
    /// # /*
    /// let workflow = { /* A `Workflow`. */ };
    /// # */
    /// #
    ///
    /// // Schedule weekly using the transaction we already have.
    /// let weekly = "@weekly[America/Los_Angeles]".parse()?;
    /// workflow.schedule_using(&mut *tx, &weekly, &()).await?;
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
        let workflow_input = self.first_workflow_schedule_template(input)?;
        let workflow_input = serde_json::to_value(workflow_input)?;
        self.queue
            .schedule_value(executor, zoned_schedule, workflow_input)
            .await?;

        Ok(())
    }

    /// Removes the workflow's schedule using a connection from the queue's
    /// pool.
    ///
    /// # Errors
    ///
    /// This has the same error conditions as [`Queue::unschedule`].
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use sqlx::PgPool;
    /// # use underway::{Workflow, To};
    /// # use tokio::runtime::Runtime;
    /// # fn main() {
    /// # let rt = Runtime::new().unwrap();
    /// # rt.block_on(async {
    /// # let pool = PgPool::connect(&std::env::var("DATABASE_URL")?).await?;
    /// # let workflow = Workflow::<(), _>::builder()
    /// #     .step(|_cx, _| async move { To::done() })
    /// #     .name("example")
    /// #     .pool(pool)
    /// #     .build()
    /// #     .await?;
    /// # /*
    /// let workflow = { /* A `Workflow`. */ };
    /// # */
    /// #
    ///
    /// // Remove the schedule if one is set.
    /// workflow.unschedule().await?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # });
    /// # }
    pub async fn unschedule(&self) -> Result {
        let mut conn = self.queue.pool.acquire().await?;
        self.unschedule_using(&mut *conn).await
    }

    /// Removes the workflow's schedule using the provided executor.
    ///
    /// This allows workflows to be unscheduled using the same transaction as an
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
    /// # use underway::{Workflow, To};
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
    /// # let workflow = Workflow::<(), _>::builder()
    /// #     .step(|_cx, _| async move { To::done() })
    /// #     .name("example")
    /// #     .pool(pool.clone())
    /// #     .build()
    /// #     .await?;
    /// # /*
    /// let workflow = { /* A `Workflow`. */ };
    /// # */
    /// #
    ///
    /// // Remove the schedule using a transaction we provide.
    /// workflow.unschedule_using(&mut *tx).await?;
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

    /// Returns this workflow's `Queue`.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use sqlx::PgPool;
    /// # use underway::{Workflow, To};
    /// # use tokio::runtime::Runtime;
    /// # fn main() {
    /// # let rt = Runtime::new().unwrap();
    /// # rt.block_on(async {
    /// # let pool = PgPool::connect(&std::env::var("DATABASE_URL")?).await?;
    /// # let workflow = Workflow::<(), _>::builder()
    /// #     .step(|_cx, _| async move { To::done() })
    /// #     .name("example")
    /// #     .pool(pool)
    /// #     .build()
    /// #     .await?;
    /// # /*
    /// let workflow = { /* A `Workflow`. */ };
    /// # */
    /// #
    ///
    /// let workflow_queue = workflow.queue();
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # });
    /// # }
    pub fn queue(&self) -> Arc<Queue<Self>> {
        Arc::clone(&self.queue)
    }

    pub(crate) fn activity_registry(&self) -> ActivityRegistry {
        self.activity_registry.clone()
    }

    /// Creates a [`Runtime`] for this workflow.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use sqlx::PgPool;
    /// # use underway::{Workflow, To};
    /// # use tokio::runtime::Runtime as TokioRuntime;
    /// # fn main() {
    /// # let rt = TokioRuntime::new().unwrap();
    /// # rt.block_on(async {
    /// # let pool = PgPool::connect(&std::env::var("DATABASE_URL")?).await?;
    /// # let workflow = Workflow::<(), _>::builder()
    /// #     .step(|_cx, _| async move { To::done() })
    /// #     .name("example")
    /// #     .pool(pool)
    /// #     .build()
    /// #     .await?;
    /// # /*
    /// let workflow = { /* A `Workflow`. */ };
    /// # */
    /// #
    /// let runtime = workflow.runtime();
    /// runtime.run().await?;
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # });
    /// # }
    /// ```
    pub fn runtime(&self) -> Runtime<I, S> {
        Runtime::new(self.clone())
    }

    fn first_workflow_input(&self, input: &I) -> Result<WorkflowState> {
        let step_input = serde_json::to_value(input)?;
        let step_index = 0;
        let workflow_run_id = WorkflowRunId::new();
        Ok(WorkflowState {
            step_input,
            step_index,
            workflow_run_id,
        })
    }

    fn first_workflow_schedule_template(&self, input: &I) -> Result<WorkflowScheduleTemplate> {
        let step_input = serde_json::to_value(input)?;
        let step_index = 0;
        Ok(WorkflowScheduleTemplate {
            step_input,
            step_index,
        })
    }
}

impl<I, S> Workflow<I, S>
where
    I: Send + Sync + 'static,
    S: Clone + Send + Sync + 'static,
{
    fn step_task_config(&self, step_index: usize) -> StepTaskConfig {
        self.steps
            .get(step_index)
            .map(|step| step.task_config.clone())
            .unwrap_or_default()
    }

    async fn fetch_activity_calls(
        &self,
        conn: &mut PgConnection,
        workflow_run_id: WorkflowRunId,
        step_index: usize,
    ) -> TaskResult<Vec<ActivityCallRecord>> {
        sqlx::query_as!(
            ActivityCallRecord,
            r#"
            select
                call_key,
                activity,
                input,
                output,
                error,
                state as "state: CallState"
            from underway.activity_call
            where task_queue_name = $1
              and workflow_run_id = $2
              and step_index = $3
            "#,
            self.queue.name,
            *workflow_run_id,
            step_index as i32,
        )
        .fetch_all(&mut *conn)
        .await
        .map_err(TaskError::from)
    }

    async fn persist_activity_call_commands(
        &self,
        conn: &mut PgConnection,
        workflow_run_id: WorkflowRunId,
        step_index: usize,
        activity_call_buffer: &Arc<Mutex<ActivityCallBuffer>>,
    ) -> TaskResult<()> {
        let commands = {
            let mut activity_call_buffer = activity_call_buffer.lock().map_err(|_| {
                TaskError::Fatal("Activity call buffer lock was poisoned.".to_string())
            })?;
            activity_call_buffer.drain_commands()
        };

        for command in commands {
            let Some((retry_policy, timeout)) =
                self.activity_registry.call_policy(&command.activity)
            else {
                return Err(TaskError::Fatal(format!(
                    "No activity policy registered for `{}`.",
                    command.activity
                )));
            };

            let timeout_ms = StdDuration::try_from(timeout)
                .map_err(|err| TaskError::Fatal(err.to_string()))?
                .as_millis()
                .min(i32::MAX as u128) as i32;

            sqlx::query!(
                r#"
                insert into underway.activity_call (
                    id,
                    task_queue_name,
                    workflow_run_id,
                    step_index,
                    call_key,
                    activity,
                    input,
                    timeout_ms,
                    max_attempts,
                    state
                ) values ($1, $2, $3, $4, $5, $6, $7, $8, $9, 'pending'::underway.activity_call_state)
                on conflict (task_queue_name, workflow_run_id, step_index, call_key) do nothing
                "#,
                Uuid::new_v4(),
                self.queue.name,
                *workflow_run_id,
                step_index as i32,
                command.call_key,
                command.activity,
                command.input,
                timeout_ms,
                retry_policy.max_attempts,
            )
            .execute(&mut *conn)
            .await
            .map_err(TaskError::from)?;
        }

        Ok(())
    }
}

impl<I, S> Task for Workflow<I, S>
where
    I: Send + Sync + 'static,
    S: Clone + Send + Sync + 'static,
{
    type Input = WorkflowState;
    type Output = ();

    #[instrument(
        skip_all,
        fields(
            workflow.run_id = %input.workflow_run_id.as_hyphenated(),
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
        let WorkflowState {
            step_index,
            step_input,
            workflow_run_id,
        } = input;

        if step_index >= self.steps.len() {
            return Err(TaskError::Fatal("Invalid step index.".into()));
        }

        let step = &self.steps[step_index].executor;
        let activity_call_buffer = Arc::new(Mutex::new(ActivityCallBuffer::new(
            self.fetch_activity_calls(&mut tx, workflow_run_id, step_index)
                .await?,
        )));
        let call_sequence_state = Arc::new(Mutex::new(CallSequenceState::default()));

        let cx = ContextParts {
            state: self.state.clone(),
            step_index,
            workflow_run_id,
            step_count: self.steps.len(),
            queue_name: self.queue.name.clone(),
            activity_call_buffer: Arc::clone(&activity_call_buffer),
            call_sequence_state,
        };

        // Execute the step and handle any errors.
        let step_result = step.execute_step(cx, step_input).await;

        // Persist activity call intents only when step execution reached a
        // transactional boundary (`Ok`) or intentionally suspended.
        if matches!(step_result.as_ref(), Ok(_) | Err(TaskError::Suspended(_))) {
            self.persist_activity_call_commands(
                &mut tx,
                workflow_run_id,
                step_index,
                &activity_call_buffer,
            )
            .await?;
        }

        // Execute the step and handle any errors.
        let step_result = match step_result {
            Ok(result) => result,
            Err(err) => {
                // N.B.: Commit the transaction to ensure attempt rows are persisted.
                tx.commit().await?;
                return Err(err);
            }
        };

        // If there's a next step, enqueue it.
        if let Some((next_input, delay)) = step_result {
            // Advance to the next step after executing the step.
            let next_index = step_index + 1;

            let next_workflow_input = WorkflowState {
                step_input: next_input,
                step_index: next_index,
                workflow_run_id,
            };

            self.queue
                .enqueue_after(&mut *tx, self, &next_workflow_input, delay)
                .await
                .map_err(|err| TaskError::Retryable(err.to_string()))?;
        }

        // Commit the transaction.
        tx.commit().await?;

        Ok(())
    }

    fn retry_policy(&self) -> RetryPolicy {
        self.step_task_config(0).retry_policy
    }

    fn timeout(&self) -> Span {
        self.step_task_config(0).timeout
    }

    fn ttl(&self) -> Span {
        self.step_task_config(0).ttl
    }

    fn delay(&self) -> Span {
        self.step_task_config(0).delay
    }

    fn heartbeat(&self) -> Span {
        self.step_task_config(0).heartbeat
    }

    fn concurrency_key(&self) -> Option<String> {
        self.step_task_config(0).concurrency_key
    }

    fn priority(&self) -> i32 {
        self.step_task_config(0).priority
    }

    fn retry_policy_for(&self, input: &Self::Input) -> RetryPolicy {
        self.step_task_config(input.step_index).retry_policy
    }

    fn timeout_for(&self, input: &Self::Input) -> Span {
        self.step_task_config(input.step_index).timeout
    }

    fn ttl_for(&self, input: &Self::Input) -> Span {
        self.step_task_config(input.step_index).ttl
    }

    fn delay_for(&self, input: &Self::Input) -> Span {
        self.step_task_config(input.step_index).delay
    }

    fn heartbeat_for(&self, input: &Self::Input) -> Span {
        self.step_task_config(input.step_index).heartbeat
    }

    fn concurrency_key_for(&self, input: &Self::Input) -> Option<String> {
        self.step_task_config(input.step_index).concurrency_key
    }

    fn priority_for(&self, input: &Self::Input) -> i32 {
        self.step_task_config(input.step_index).priority
    }
}

impl<I, S> Clone for Workflow<I, S>
where
    I: Send + Sync + 'static,
    S: Clone + Send + Sync + 'static,
{
    fn clone(&self) -> Self {
        Self {
            queue: self.queue.clone(),
            state: self.state.clone(),
            steps: self.steps.clone(),
            activity_registry: self.activity_registry.clone(),
            _marker: PhantomData,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{cmp::Ordering, sync::Mutex};

    use jiff::ToSpan;
    use serde::{Deserialize, Serialize};
    use sqlx::{postgres::types::PgInterval, PgPool};

    use super::*;
    use crate::{
        activity::{Activity, Error as ActivityError, Result as ActivityResult},
        queue::graceful_shutdown,
        worker::pg_interval_to_span,
    };

    struct EchoActivity;

    impl Activity for EchoActivity {
        const NAME: &'static str = "echo";

        type Input = String;
        type Output = String;

        async fn execute(&self, input: Self::Input) -> ActivityResult<Self::Output> {
            Ok(format!("echo:{input}"))
        }
    }

    struct EmailActivity;

    impl Activity for EmailActivity {
        const NAME: &'static str = "email";

        type Input = String;
        type Output = ();

        async fn execute(&self, input: Self::Input) -> ActivityResult<Self::Output> {
            if input.is_empty() {
                return Err(ActivityError::fatal(
                    "empty_message",
                    "email message is empty",
                ));
            }

            Ok(())
        }
    }

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

        let workflow = Workflow::builder()
            .step(|_cx, Input { message }| async move {
                println!("Executing workflow with message: {message}");
                To::done()
            })
            .queue(queue.clone())
            .build();

        assert_eq!(workflow.retry_policy(), RetryPolicy::default());

        let input = Input {
            message: "Hello, world!".to_string(),
        };
        workflow.enqueue(&input).await?;

        let pending_task = queue
            .dequeue()
            .await?
            .expect("There should be an enqueued task");

        let workflow_state: WorkflowState = serde_json::from_value(pending_task.input)?;
        assert_eq!(workflow_state.step_index, 0);
        assert_eq!(workflow_state.step_input, serde_json::to_value(&input)?);

        Ok(())
    }

    #[sqlx::test]
    async fn one_step_named(pool: PgPool) -> sqlx::Result<(), Error> {
        #[derive(Serialize, Deserialize)]
        struct Input {
            message: String,
        }

        async fn step(_cx: Context<()>, Input { message }: Input) -> TaskResult<To<()>> {
            println!("Executing workflow with message: {message}");
            To::done()
        }

        let queue = Queue::builder()
            .name("one_step_named")
            .pool(pool.clone())
            .build()
            .await?;

        let workflow = Workflow::builder().step(step).queue(queue.clone()).build();

        assert_eq!(workflow.retry_policy(), RetryPolicy::default());

        let input = Input {
            message: "Hello, world!".to_string(),
        };
        workflow.enqueue(&input).await?;

        let pending_task = queue
            .dequeue()
            .await?
            .expect("There should be an enqueued task");

        let workflow_state: WorkflowState = serde_json::from_value(pending_task.input)?;
        assert_eq!(workflow_state.step_index, 0);
        assert_eq!(workflow_state.step_input, serde_json::to_value(&input)?);

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

        let workflow = Workflow::builder()
            .state(State {
                data: "data".to_string(),
            })
            .step(|cx, Input { message }| async move {
                println!(
                    "Executing workflow with message: {message} and state: {state}",
                    state = cx.state.data
                );
                To::done()
            })
            .queue(queue.clone())
            .build();

        assert_eq!(workflow.retry_policy(), RetryPolicy::default());

        let input = Input {
            message: "Hello, world!".to_string(),
        };
        workflow.enqueue(&input).await?;

        let pending_task = queue
            .dequeue()
            .await?
            .expect("There should be an enqueued task");

        let workflow_state: WorkflowState = serde_json::from_value(pending_task.input)?;
        assert_eq!(workflow_state.step_index, 0);
        assert_eq!(workflow_state.step_input, serde_json::to_value(&input)?);

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

        let workflow = Workflow::builder()
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

        workflow.enqueue(&()).await?;

        let runtime_handle = workflow.runtime().start();

        // Give the workflow a moment to process.
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

        assert_eq!(
            *state.data.lock().expect("Mutex should not be poisoned"),
            "bar".to_string()
        );

        // Shutdown and wait for a bit to ensure the test can exit.
        runtime_handle
            .shutdown()
            .await
            .expect("Runtime should shutdown cleanly");
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
                "Executing workflow with message: {message} and state: {data}",
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

        let workflow = Workflow::builder()
            .state(state)
            .step(step)
            .queue(queue.clone())
            .build();

        assert_eq!(workflow.retry_policy(), RetryPolicy::default());

        let input = Input {
            message: "Hello, world!".to_string(),
        };
        workflow.enqueue(&input).await?;

        let pending_task = queue
            .dequeue()
            .await?
            .expect("There should be an enqueued task");

        let workflow_state: WorkflowState = serde_json::from_value(pending_task.input)?;
        assert_eq!(workflow_state.step_index, 0);
        assert_eq!(workflow_state.step_input, serde_json::to_value(&input)?);

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

        let workflow = Workflow::builder()
            .step(|_cx, Input { message }| async move {
                println!("Executing workflow with message: {message}");
                To::done()
            })
            .queue(queue.clone())
            .build();

        let input = Input {
            message: "Hello, world!".to_string(),
        };
        workflow.enqueue(&input).await?;

        let pending_task = queue
            .dequeue()
            .await?
            .expect("There should be an enqueued task");

        let workflow_state: WorkflowState = serde_json::from_value(pending_task.input)?;
        assert_eq!(workflow_state.step_index, 0);
        assert_eq!(workflow_state.step_input, serde_json::to_value(&input)?);

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

        let workflow = Workflow::builder()
            .step(|_cx, Input { message }| async move {
                println!("Processing {message}");
                To::done()
            })
            .queue(queue.clone())
            .build();

        let inputs = [
            Input {
                message: "first".to_string(),
            },
            Input {
                message: "second".to_string(),
            },
        ];

        let enqueued = workflow.enqueue_many(&inputs).await?;
        assert_eq!(enqueued.len(), 2);

        let mut workflow_run_ids: Vec<String> = enqueued
            .iter()
            .map(|handle| handle.run_id.to_string())
            .collect();
        workflow_run_ids.sort();

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

        let mut pending_workflow_run_ids = Vec::new();
        for task in pending_tasks {
            let workflow_state: WorkflowState = serde_json::from_value(task.input)?;
            assert_eq!(workflow_state.step_index, 0);
            pending_workflow_run_ids.push(workflow_state.workflow_run_id.to_string());
        }

        pending_workflow_run_ids.sort();

        assert_eq!(pending_workflow_run_ids, workflow_run_ids);

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

        let workflow = Workflow::builder()
            .step(|_cx, _| async move { To::done() })
            .retry_policy(retry_policy)
            .timeout(2.minutes())
            .ttl(3.days())
            .delay(45.seconds())
            .heartbeat(5.seconds())
            .concurrency_key("customer:42")
            .priority(9)
            .queue(queue.clone())
            .build();

        workflow.enqueue(&()).await?;

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
    async fn step_config_is_per_workflow_run(pool: PgPool) -> sqlx::Result<(), Error> {
        #[derive(Serialize, Deserialize)]
        struct Step1 {
            message: String,
        }

        #[derive(Serialize, Deserialize)]
        struct Step2 {
            message: String,
        }

        let queue = Queue::builder()
            .name("step_config_is_per_workflow_run")
            .pool(pool.clone())
            .build()
            .await?;

        let step2_policy = RetryPolicy::builder().max_attempts(9).build();

        let workflow = Workflow::builder()
            .step(|_cx, Step1 { message }| async move { To::next(Step2 { message }) })
            .step(|_cx, Step2 { message }| async move {
                println!("Processed {message}");
                To::done()
            })
            .retry_policy(step2_policy)
            .priority(7)
            .queue(queue.clone())
            .build();

        workflow
            .enqueue(&Step1 {
                message: "first".to_string(),
            })
            .await?;
        workflow.runtime().worker().process_next_task().await?;

        workflow
            .enqueue(&Step1 {
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
            let workflow_state: WorkflowState = serde_json::from_value(task.input.clone())?;
            match workflow_state.step_index {
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

        let workflow = Workflow::builder()
            .step(|_, _| async { To::done() })
            .queue(queue.clone())
            .build();

        let monthly = "@monthly[America/Los_Angeles]"
            .parse()
            .expect("A valid zoned scheduled should be provided");
        workflow.schedule(&monthly, &()).await?;

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
    async fn scheduled_dispatch_regenerates_workflow_run_id(
        pool: PgPool,
    ) -> sqlx::Result<(), Error> {
        #[derive(Serialize, Deserialize)]
        struct Input {
            message: String,
        }

        let queue = Queue::builder()
            .name("scheduled_dispatch_regenerates_workflow_run_id")
            .pool(pool.clone())
            .build()
            .await?;

        let workflow: Workflow<Input, ()> = Workflow::builder()
            .step(|_cx, Input { message }| async move {
                println!("Executing workflow with message: {message}");
                To::done()
            })
            .queue(queue.clone())
            .build();

        let daily = "@daily[UTC]".parse().expect("Schedule should parse");
        let input = Input {
            message: "Hello, world!".to_string(),
        };
        workflow.schedule(&daily, &input).await?;

        let (_, scheduled_input_one) = queue
            .task_schedule(&pool)
            .await?
            .expect("Schedule should be set");
        let (_, scheduled_input_two) = queue
            .task_schedule(&pool)
            .await?
            .expect("Schedule should be set");

        assert_eq!(scheduled_input_one.step_index, 0);
        assert_eq!(scheduled_input_two.step_index, 0);
        assert_eq!(
            scheduled_input_one.step_input,
            serde_json::to_value(&input)?
        );
        assert_eq!(
            scheduled_input_two.step_input,
            serde_json::to_value(&input)?
        );
        assert_ne!(
            scheduled_input_one.workflow_run_id,
            scheduled_input_two.workflow_run_id
        );

        let stored_input = sqlx::query_scalar!(
            r#"
            select input
            from underway.task_schedule
            where task_queue_name = $1
            "#,
            queue.name.as_str(),
        )
        .fetch_one(&pool)
        .await?;

        assert!(stored_input.as_object().is_some_and(|value| {
            !value.contains_key("workflow_run_id") && !value.contains_key("workflow_id")
        }));

        Ok(())
    }

    #[sqlx::test]
    async fn one_step_context_attributes(pool: PgPool) -> sqlx::Result<(), Error> {
        let workflow = Workflow::builder()
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

        workflow.enqueue(&()).await?;

        // Process the first task.
        let task_id = workflow.runtime().worker().process_next_task().await?;

        assert!(task_id.is_some());

        Ok(())
    }

    #[sqlx::test]
    async fn call_suspends_and_persists_activity_intent(pool: PgPool) -> sqlx::Result<(), Error> {
        #[derive(Serialize, Deserialize)]
        struct Input {
            message: String,
        }

        let workflow = Workflow::builder()
            .activity(EchoActivity)
            .step(|mut cx, Input { message }| async move {
                let _: String = cx.call::<EchoActivity, _>("echo-main", &message).await?;
                To::done()
            })
            .name("call_suspends_and_persists_activity_intent")
            .pool(pool.clone())
            .build()
            .await?;

        let enqueued = workflow
            .enqueue(&Input {
                message: "hello".to_string(),
            })
            .await?;

        workflow.runtime().worker().process_next_task().await?;

        let task_state = sqlx::query_scalar!(
            r#"
            select state as "state: TaskState"
            from underway.task
            where task_queue_name = $1
              and (input->>'workflow_run_id')::uuid = $2
            "#,
            workflow.queue.name,
            *enqueued.run_id,
        )
        .fetch_one(&pool)
        .await?;

        assert_eq!(task_state, TaskState::Waiting);

        let call_row = sqlx::query!(
            r#"
            select state as "state: CallState"
            from underway.activity_call
            where task_queue_name = $1
              and workflow_run_id = $2
              and step_index = $3
              and call_key = $4
            "#,
            workflow.queue.name,
            *enqueued.run_id,
            0_i32,
            "echo-main",
        )
        .fetch_one(&pool)
        .await?;

        assert_eq!(call_row.state, CallState::Pending);

        Ok(())
    }

    #[sqlx::test]
    async fn emit_not_persisted_on_retryable_failure(pool: PgPool) -> sqlx::Result<(), Error> {
        #[derive(Serialize, Deserialize)]
        struct Input {
            message: String,
        }

        let workflow = Workflow::builder()
            .activity(EmailActivity)
            .step(|cx, Input { message }| async move {
                cx.emit::<EmailActivity, _>("notify", &message).await?;
                Err(TaskError::Retryable("retry me".to_string()))
            })
            .name("emit_not_persisted_on_retryable_failure")
            .pool(pool.clone())
            .build()
            .await?;

        let enqueued = workflow
            .enqueue(&Input {
                message: "hello".to_string(),
            })
            .await?;

        workflow.runtime().worker().process_next_task().await?;

        let activity_call_count = sqlx::query_scalar!(
            r#"
            select count(*)::int as "count!"
            from underway.activity_call
            where task_queue_name = $1
              and workflow_run_id = $2
            "#,
            workflow.queue.name,
            *enqueued.run_id,
        )
        .fetch_one(&pool)
        .await?;

        assert_eq!(activity_call_count, 0);

        Ok(())
    }

    #[sqlx::test]
    async fn emit_persisted_on_success(pool: PgPool) -> sqlx::Result<(), Error> {
        #[derive(Serialize, Deserialize)]
        struct Input {
            message: String,
        }

        let workflow = Workflow::builder()
            .activity(EmailActivity)
            .step(|cx, Input { message }| async move {
                cx.emit::<EmailActivity, _>("notify", &message).await?;
                To::done()
            })
            .name("emit_persisted_on_success")
            .pool(pool.clone())
            .build()
            .await?;

        let enqueued = workflow
            .enqueue(&Input {
                message: "hello".to_string(),
            })
            .await?;

        workflow.runtime().worker().process_next_task().await?;

        let activity_call_count = sqlx::query_scalar!(
            r#"
            select count(*)::int as "count!"
            from underway.activity_call
            where task_queue_name = $1
              and workflow_run_id = $2
            "#,
            workflow.queue.name,
            *enqueued.run_id,
        )
        .fetch_one(&pool)
        .await?;

        assert_eq!(activity_call_count, 1);

        Ok(())
    }

    #[sqlx::test]
    async fn second_call_after_suspension_is_rejected(pool: PgPool) -> sqlx::Result<(), Error> {
        #[derive(Serialize, Deserialize)]
        struct Input {
            message: String,
        }

        let workflow = Workflow::builder()
            .activity(EchoActivity)
            .step(|mut cx, Input { message }| async move {
                let first = cx.call::<EchoActivity, _>("call-1", &message).await;
                assert!(matches!(first, Err(TaskError::Suspended(_))));

                let _ = cx.call::<EchoActivity, _>("call-2", &message).await?;

                To::done()
            })
            .name("second_call_after_suspension_is_rejected")
            .pool(pool.clone())
            .build()
            .await?;

        let enqueued = workflow
            .enqueue(&Input {
                message: "hello".to_string(),
            })
            .await?;

        workflow.runtime().worker().process_next_task().await?;

        let task_state = sqlx::query_scalar!(
            r#"
            select state as "state: TaskState"
            from underway.task
            where task_queue_name = $1
              and (input->>'workflow_run_id')::uuid = $2
            "#,
            workflow.queue.name,
            *enqueued.run_id,
        )
        .fetch_one(&pool)
        .await?;

        assert_eq!(task_state, TaskState::Failed);

        let activity_call_count = sqlx::query_scalar!(
            r#"
            select count(*)::int as "count!"
            from underway.activity_call
            where task_queue_name = $1
              and workflow_run_id = $2
            "#,
            workflow.queue.name,
            *enqueued.run_id,
        )
        .fetch_one(&pool)
        .await?;

        assert_eq!(activity_call_count, 0);

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

        let workflow = Workflow::builder()
            .step(|_cx, Step1 { message }| async move {
                println!("Executing workflow with message: {message}");
                To::next(Step2 {
                    data: message.as_bytes().into(),
                })
            })
            .step(|_cx, Step2 { data }| async move {
                println!("Executing workflow with data: {data:?}");
                To::done()
            })
            .queue(queue.clone())
            .build();

        assert_eq!(workflow.retry_policy(), RetryPolicy::default());

        let input = Step1 {
            message: "Hello, world!".to_string(),
        };
        workflow.enqueue(&input).await?;

        // Process the first task.
        workflow.runtime().worker().process_next_task().await?;

        // Inspect the second task.
        let pending_task = queue
            .dequeue()
            .await?
            .expect("There should be an enqueued task");

        let workflow_state: WorkflowState = serde_json::from_value(pending_task.input)?;
        assert_eq!(workflow_state.step_index, 1);
        assert_eq!(
            workflow_state.step_input,
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

        let workflow = Workflow::builder()
            .step(|_cx, Step1 { message }| async move {
                println!("Executing workflow with message: {message}");
                To::next(Step2 {
                    data: message.as_bytes().into(),
                })
            })
            .retry_policy(step1_policy)
            .step(|_cx, Step2 { data }| async move {
                println!("Executing workflow with data: {data:?}");
                To::done()
            })
            .retry_policy(step2_policy)
            .queue(queue.clone())
            .build();

        assert_eq!(workflow.retry_policy(), step1_policy);

        let input = Step1 {
            message: "Hello, world!".to_string(),
        };
        let enqueued_workflow = workflow.enqueue(&input).await?;

        // Dequeue the first task.
        let Some(dequeued_task) = queue.dequeue().await? else {
            panic!("Task should exist");
        };

        assert_eq!(
            enqueued_workflow.run_id,
            dequeued_task
                .input
                .get("workflow_run_id")
                .cloned()
                .map(serde_json::from_value)
                .expect("Failed to deserialize 'workflow_run_id'")?
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
        workflow.runtime().worker().process_next_task().await?;

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

        let workflow = Workflow::builder()
            .state(State {
                data: "data".to_string(),
            })
            .step(|cx, Step1 { message }| async move {
                println!(
                    "Executing workflow with message: {message} and state: {state}",
                    state = cx.state.data
                );
                To::next(Step2 {
                    data: message.as_bytes().into(),
                })
            })
            .step(|cx, Step2 { data }| async move {
                println!(
                    "Executing workflow with data: {data:?} and state: {state}",
                    state = cx.state.data
                );
                To::done()
            })
            .queue(queue.clone())
            .build();

        assert_eq!(workflow.retry_policy(), RetryPolicy::default());

        let input = Step1 {
            message: "Hello, world!".to_string(),
        };
        workflow.enqueue(&input).await?;

        // Process the first task.
        workflow.runtime().worker().process_next_task().await?;

        // Inspect the second task.
        let pending_task = queue
            .dequeue()
            .await?
            .expect("There should be an enqueued task");

        let workflow_state: WorkflowState = serde_json::from_value(pending_task.input)?;
        assert_eq!(workflow_state.step_index, 1);
        assert_eq!(
            workflow_state.step_input,
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

        let workflow = Workflow::builder()
            .step(|_cx, Step1 { message }| async move {
                println!("Executing workflow with message: {message}",);
                To::next(Step2 {
                    data: message.as_bytes().into(),
                })
            })
            .step(|_cx, Step2 { data }| async move {
                println!("Executing workflow with data: {data:?}");
                To::done()
            })
            .queue(queue.clone())
            .build();

        let input = Step1 {
            message: "Hello, world!".to_string(),
        };
        let enqueued_workflow = workflow.enqueue(&input).await?;

        // Dequeue the first task.
        let Some(dequeued_task) = queue.dequeue().await? else {
            panic!("Task should exist");
        };

        assert_eq!(
            enqueued_workflow.run_id,
            dequeued_task
                .input
                .get("workflow_run_id")
                .cloned()
                .map(serde_json::from_value)
                .expect("Failed to deserialize 'workflow_run_id'")?
        );

        let workflow_state: WorkflowState = serde_json::from_value(dequeued_task.input).unwrap();
        assert_eq!(
            WorkflowState {
                step_index: 0,
                step_input: serde_json::to_value(input).unwrap(),
                workflow_run_id: workflow_state.workflow_run_id
            },
            workflow_state
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
        workflow.runtime().worker().process_next_task().await?;

        // Dequeue the second task.
        let Some(dequeued_task) = queue.dequeue().await? else {
            panic!("Next task should exist");
        };

        let step2_input = Step2 {
            data: "Hello, world!".to_string().as_bytes().into(),
        };
        let workflow_state: WorkflowState = serde_json::from_value(dequeued_task.input).unwrap();
        assert_eq!(
            WorkflowState {
                step_index: 1,
                step_input: serde_json::to_value(step2_input).unwrap(),
                workflow_run_id: workflow_state.workflow_run_id
            },
            workflow_state
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

        let workflow = Workflow::builder()
            .step(|_cx, Input { message }| async move {
                println!("Executing workflow with message: {message}");
                To::done()
            })
            .queue(queue.clone())
            .build();

        assert_eq!(workflow.retry_policy(), RetryPolicy::default());

        let daily = "@daily[America/Los_Angeles]"
            .parse()
            .expect("Schedule should parse");
        let input = Input {
            message: "Hello, world!".to_string(),
        };
        workflow.schedule(&daily, &input).await?;

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

        let workflow = Workflow::builder()
            .step(|_cx, Input { message }| async move {
                println!("Executing workflow with message: {message}");
                To::done()
            })
            .queue(queue.clone())
            .build();

        assert_eq!(workflow.retry_policy(), RetryPolicy::default());

        let daily = "@daily[America/Los_Angeles]"
            .parse()
            .expect("Schedule should parse");
        let input = Input {
            message: "Hello, world!".to_string(),
        };
        workflow.schedule(&daily, &input).await?;
        workflow.unschedule().await?;

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

        let workflow = Workflow::builder()
            .step(|_cx, Input { message }| async move {
                println!("Executing workflow with message: {message}");
                To::done()
            })
            .queue(queue.clone())
            .build();

        assert_eq!(workflow.retry_policy(), RetryPolicy::default());

        assert!(workflow.unschedule().await.is_ok());
        assert!(queue.task_schedule(&pool).await?.is_none());

        Ok(())
    }

    #[sqlx::test]
    async fn enqueued_workflow_cancel(pool: PgPool) -> sqlx::Result<(), Error> {
        let queue = Queue::builder()
            .name("enqueued_workflow_cancel")
            .pool(pool.clone())
            .build()
            .await?;

        let workflow = Workflow::builder()
            .step(|_cx, _| async move { To::done() })
            .queue(queue.clone())
            .build();

        let enqueued_workflow = workflow.enqueue(&()).await?;

        // Should return `true`.
        assert!(enqueued_workflow.cancel().await?);

        let task = sqlx::query!(
            r#"
            select state as "state: TaskState"
            from underway.task
            where input->>'workflow_run_id' = $1
            "#,
            enqueued_workflow.run_id.to_string()
        )
        .fetch_one(&pool)
        .await?;

        assert_eq!(task.state, TaskState::Cancelled);

        // Should return `false` since the workflow is already cancelled.
        assert!(!enqueued_workflow.cancel().await?);

        Ok(())
    }
}
