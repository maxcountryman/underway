use std::{future::Future, marker::PhantomData, sync::Arc};

use jiff::Span;
use serde::{de::DeserializeOwned, Serialize};
use sqlx::PgPool;

use super::{
    registration::{ActivitySet, NoActivities, Registered},
    step::{StepConfig, To},
    Context, Result, Workflow, WorkflowQueue,
};
use crate::{
    activity::Activity,
    activity_worker::ActivityRegistry,
    queue::Queue,
    task::{Result as TaskResult, RetryPolicy},
};

pub(super) mod builder_states {
    use std::marker::PhantomData;

    use sqlx::PgPool;

    use super::WorkflowQueue;

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
        pub queue: WorkflowQueue<I, S>,
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

pub(super) use builder_states::{Initial, PoolSet, QueueNameSet, QueueSet, StateSet, StepSet};

type BuilderMarker<I, O, S, A> = fn() -> (I, O, S, A);

/// Builder for constructing a `Workflow` with a sequence of steps.
pub struct Builder<I, O, S, B, A = NoActivities> {
    builder_state: B,
    steps: Vec<StepConfig<S>>,
    activity_registry: ActivityRegistry,
    _marker: PhantomData<BuilderMarker<I, O, S, A>>,
}

impl<I, S> Default for Builder<I, I, S, Initial, NoActivities> {
    fn default() -> Self {
        Self::new()
    }
}

impl<I, S, ASet> Builder<I, I, S, Initial, ASet>
where
    ASet: 'static,
{
    /// Registers an activity handler for subsequent steps.
    pub fn activity<A>(mut self, activity: A) -> Builder<I, I, S, Initial, Registered<A, ASet>>
    where
        ASet: ActivitySet,
        A: Activity,
    {
        self.activity_registry.register(activity);

        Builder::<I, I, S, Initial, Registered<A, ASet>> {
            builder_state: Initial,
            steps: Vec::new(),
            activity_registry: self.activity_registry,
            _marker: PhantomData,
        }
    }

    /// Create a new builder.
    ///
    /// # Example
    ///
    /// ```rust
    /// use underway::Workflow;
    ///
    /// // Instantiate a new builder from the `Workflow` method.
    /// let workflow_builder = Workflow::<(), ()>::builder();
    /// ```
    pub fn new() -> Builder<I, I, S, Initial, NoActivities> {
        Builder::<I, I, S, _, NoActivities> {
            builder_state: Initial,
            steps: Vec::new(),
            activity_registry: ActivityRegistry::default(),
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
    /// use underway::Workflow;
    ///
    /// #[derive(Clone)]
    /// struct State {
    ///     data: String,
    /// }
    ///
    /// // Set state.
    /// let workflow_builder = Workflow::<(), _>::builder().state(State {
    ///     data: "foo".to_string(),
    /// });
    /// ```
    pub fn state(self, state: S) -> Builder<I, I, S, StateSet<S>, ASet> {
        Builder {
            builder_state: StateSet { state },
            steps: self.steps,
            activity_registry: self.activity_registry,
            _marker: PhantomData,
        }
    }

    /// Add a step to the workflow.
    ///
    /// A step function should take the workflow context as its first argument
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
    /// use underway::{To, Workflow};
    ///
    /// // Set a step.
    /// let workflow_builder = Workflow::<(), ()>::builder().step(|_cx, _| async move { To::done() });
    /// ```
    pub fn step<F, O, Fut>(mut self, func: F) -> Builder<I, O, S, StepSet<O, ()>, ASet>
    where
        I: DeserializeOwned + Serialize + Send + Sync + 'static,
        O: Serialize + Send + Sync + 'static,
        S: Send + Sync + 'static,
        F: Fn(Context<S, ASet>, I) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = TaskResult<To<O>>> + Send + 'static,
    {
        self.steps.push(StepConfig::new(func));

        Builder {
            builder_state: StepSet {
                state: (),
                _marker: PhantomData,
            },
            steps: self.steps,
            activity_registry: self.activity_registry,
            _marker: PhantomData,
        }
    }
}

// After state set, before first step set.
impl<I, S, ASet> Builder<I, I, S, StateSet<S>, ASet>
where
    ASet: 'static,
{
    /// Registers an activity handler for subsequent steps.
    pub fn activity<A>(mut self, activity: A) -> Builder<I, I, S, StateSet<S>, Registered<A, ASet>>
    where
        ASet: ActivitySet,
        A: Activity,
    {
        self.activity_registry.register(activity);

        Builder::<I, I, S, StateSet<S>, Registered<A, ASet>> {
            builder_state: StateSet {
                state: self.builder_state.state,
            },
            steps: Vec::new(),
            activity_registry: self.activity_registry,
            _marker: PhantomData,
        }
    }

    /// Add a step to the workflow.
    ///
    /// A step function should take the workflow context as its first argument
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
    /// use underway::{To, Workflow};
    ///
    /// #[derive(Clone)]
    /// struct State {
    ///     data: String,
    /// }
    ///
    /// // Set a step with state.
    /// let workflow_builder = Workflow::<(), _>::builder()
    ///     .state(State {
    ///         data: "foo".to_string(),
    ///     })
    ///     .step(|cx, _| async move {
    ///         println!("State data: {}", cx.state.data);
    ///         To::done()
    ///     });
    /// ```
    pub fn step<F, O, Fut>(mut self, func: F) -> Builder<I, O, S, StepSet<O, S>, ASet>
    where
        I: DeserializeOwned + Serialize + Send + Sync + 'static,
        O: Serialize + Send + Sync + 'static,
        S: Send + Sync + 'static,
        F: Fn(Context<S, ASet>, I) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = TaskResult<To<O>>> + Send + 'static,
    {
        self.steps.push(StepConfig::new(func));

        Builder {
            builder_state: StepSet {
                state: self.builder_state.state,
                _marker: PhantomData,
            },
            steps: self.steps,
            activity_registry: self.activity_registry,
            _marker: PhantomData,
        }
    }
}

// After first step set.
impl<I, Current, S, ASet> Builder<I, Current, S, StepSet<Current, S>, ASet>
where
    ASet: 'static,
{
    /// Add a subsequent step to the workflow.
    ///
    /// This method ensures that the input type of the new step matches the
    /// output type of the previous step.
    ///
    /// # Example
    ///
    /// ```rust
    /// use serde::{Deserialize, Serialize};
    /// use underway::{To, Workflow};
    ///
    /// #[derive(Deserialize, Serialize)]
    /// struct Step2 {
    ///     n: usize,
    /// }
    ///
    /// // Set one step after another.
    /// let workflow_builder = Workflow::<(), ()>::builder()
    ///     .step(|_cx, _| async move { To::next(Step2 { n: 42 }) })
    ///     .step(|_cx, Step2 { n }| async move { To::done() });
    /// ```
    pub fn step<F, New, Fut>(mut self, func: F) -> Builder<I, New, S, StepSet<New, S>, ASet>
    where
        Current: DeserializeOwned + Serialize + Send + Sync + 'static,
        New: Serialize + Send + Sync + 'static,
        S: Send + Sync + 'static,
        F: Fn(Context<S, ASet>, Current) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = TaskResult<To<New>>> + Send + 'static,
    {
        self.steps.push(StepConfig::new(func));

        Builder {
            builder_state: StepSet {
                state: self.builder_state.state,
                _marker: PhantomData,
            },
            steps: self.steps,
            activity_registry: self.activity_registry,
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
    /// use underway::{task::RetryPolicy, To, Workflow};
    ///
    /// // Set a retry policy for the step.
    /// let retry_policy = RetryPolicy::builder().max_attempts(15).build();
    /// let workflow_builder = Workflow::<(), ()>::builder()
    ///     .step(|_cx, _| async move { To::done() })
    ///     .retry_policy(retry_policy);
    /// ```
    pub fn retry_policy(
        mut self,
        retry_policy: RetryPolicy,
    ) -> Builder<I, Current, S, StepSet<Current, S>, ASet> {
        let step_config = self.steps.last_mut().expect("Steps should not be empty");
        step_config.task_config.retry_policy = retry_policy;

        Builder {
            builder_state: StepSet {
                state: self.builder_state.state,
                _marker: PhantomData,
            },
            steps: self.steps,
            activity_registry: self.activity_registry,
            _marker: PhantomData,
        }
    }

    /// Sets the timeout of the previous step.
    ///
    /// # Example
    ///
    /// ```rust
    /// use jiff::ToSpan;
    /// use underway::{To, Workflow};
    ///
    /// let workflow_builder = Workflow::<(), ()>::builder()
    ///     .step(|_cx, _| async move { To::done() })
    ///     .timeout(1.minute());
    /// ```
    pub fn timeout(mut self, timeout: Span) -> Builder<I, Current, S, StepSet<Current, S>, ASet> {
        let step_config = self.steps.last_mut().expect("Steps should not be empty");
        step_config.task_config.timeout = timeout;

        Builder {
            builder_state: StepSet {
                state: self.builder_state.state,
                _marker: PhantomData,
            },
            steps: self.steps,
            activity_registry: self.activity_registry,
            _marker: PhantomData,
        }
    }

    /// Sets the TTL of the previous step.
    ///
    /// # Example
    ///
    /// ```rust
    /// use jiff::ToSpan;
    /// use underway::{To, Workflow};
    ///
    /// let workflow_builder = Workflow::<(), ()>::builder()
    ///     .step(|_cx, _| async move { To::done() })
    ///     .ttl(7.days());
    /// ```
    pub fn ttl(mut self, ttl: Span) -> Builder<I, Current, S, StepSet<Current, S>, ASet> {
        let step_config = self.steps.last_mut().expect("Steps should not be empty");
        step_config.task_config.ttl = ttl;

        Builder {
            builder_state: StepSet {
                state: self.builder_state.state,
                _marker: PhantomData,
            },
            steps: self.steps,
            activity_registry: self.activity_registry,
            _marker: PhantomData,
        }
    }

    /// Sets a base delay before the previous step can be dequeued.
    ///
    /// This delay is added to any delay specified by [`To::delay_for`].
    ///
    /// # Example
    ///
    /// ```rust
    /// use jiff::ToSpan;
    /// use underway::{To, Workflow};
    ///
    /// let workflow_builder = Workflow::<(), ()>::builder()
    ///     .step(|_cx, _| async move { To::done() })
    ///     .delay(30.seconds());
    /// ```
    pub fn delay(mut self, delay: Span) -> Builder<I, Current, S, StepSet<Current, S>, ASet> {
        let step_config = self.steps.last_mut().expect("Steps should not be empty");
        step_config.task_config.delay = delay;

        Builder {
            builder_state: StepSet {
                state: self.builder_state.state,
                _marker: PhantomData,
            },
            steps: self.steps,
            activity_registry: self.activity_registry,
            _marker: PhantomData,
        }
    }

    /// Sets the heartbeat interval of the previous step.
    ///
    /// # Example
    ///
    /// ```rust
    /// use jiff::ToSpan;
    /// use underway::{To, Workflow};
    ///
    /// let workflow_builder = Workflow::<(), ()>::builder()
    ///     .step(|_cx, _| async move { To::done() })
    ///     .heartbeat(5.seconds());
    /// ```
    pub fn heartbeat(
        mut self,
        heartbeat: Span,
    ) -> Builder<I, Current, S, StepSet<Current, S>, ASet> {
        let step_config = self.steps.last_mut().expect("Steps should not be empty");
        step_config.task_config.heartbeat = heartbeat;

        Builder {
            builder_state: StepSet {
                state: self.builder_state.state,
                _marker: PhantomData,
            },
            steps: self.steps,
            activity_registry: self.activity_registry,
            _marker: PhantomData,
        }
    }

    /// Sets the concurrency key of the previous step.
    ///
    /// # Example
    ///
    /// ```rust
    /// use underway::{To, Workflow};
    ///
    /// let workflow_builder = Workflow::<(), ()>::builder()
    ///     .step(|_cx, _| async move { To::done() })
    ///     .concurrency_key("customer:42");
    /// ```
    pub fn concurrency_key(
        mut self,
        concurrency_key: impl Into<String>,
    ) -> Builder<I, Current, S, StepSet<Current, S>, ASet> {
        let step_config = self.steps.last_mut().expect("Steps should not be empty");
        step_config.task_config.concurrency_key = Some(concurrency_key.into());

        Builder {
            builder_state: StepSet {
                state: self.builder_state.state,
                _marker: PhantomData,
            },
            steps: self.steps,
            activity_registry: self.activity_registry,
            _marker: PhantomData,
        }
    }

    /// Sets the priority of the previous step.
    ///
    /// # Example
    ///
    /// ```rust
    /// use underway::{To, Workflow};
    ///
    /// let workflow_builder = Workflow::<(), ()>::builder()
    ///     .step(|_cx, _| async move { To::done() })
    ///     .priority(10);
    /// ```
    pub fn priority(mut self, priority: i32) -> Builder<I, Current, S, StepSet<Current, S>, ASet> {
        let step_config = self.steps.last_mut().expect("Steps should not be empty");
        step_config.task_config.priority = priority;

        Builder {
            builder_state: StepSet {
                state: self.builder_state.state,
                _marker: PhantomData,
            },
            steps: self.steps,
            activity_registry: self.activity_registry,
            _marker: PhantomData,
        }
    }
}

// Encapsulate queue creation.
impl<I, S, ASet> Builder<I, (), S, StepSet<(), S>, ASet>
where
    I: Send + Sync + 'static,
    S: Clone + Send + Sync + 'static,
{
    /// Set the name of the workflow's queue.
    ///
    /// This provides the name of the underlying queue that will be created for
    /// this workflow.
    ///
    /// **Note:** It's important that this name be unique amongst all tasks. If
    /// it's not and other tasks define differing input types this will
    /// cause runtime errors when mismatching types are deserialized from
    /// the database.
    ///
    /// # Example
    ///
    /// ```rust
    /// use underway::{To, Workflow};
    ///
    /// // Set a name.
    /// let workflow_builder = Workflow::<(), ()>::builder()
    ///     .step(|_cx, _| async move { To::done() })
    ///     .name("example");
    /// ```
    pub fn name(self, name: impl Into<String>) -> Builder<I, (), S, QueueNameSet<S>, ASet> {
        Builder {
            builder_state: QueueNameSet {
                state: self.builder_state.state,
                queue_name: name.into(),
            },
            steps: self.steps,
            activity_registry: self.activity_registry,
            _marker: PhantomData,
        }
    }
}

impl<I, S, ASet> Builder<I, (), S, QueueNameSet<S>, ASet>
where
    I: Send + Sync + 'static,
    S: Clone + Send + Sync + 'static,
{
    /// Set the pool of the workflow's queue.
    ///
    /// This provides the connection pool to the database that the underlying
    /// queue will use for this workflow.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use std::env;
    ///
    /// use sqlx::PgPool;
    /// use underway::{To, Workflow};
    /// # use tokio::runtime::Runtime;
    /// # fn main() {
    /// # let rt = Runtime::new().unwrap();
    /// # rt.block_on(async {
    ///
    /// let pool = PgPool::connect(&env::var("DATABASE_URL").unwrap()).await?;
    ///
    /// // Set a pool.
    /// let workflow_builder = Workflow::<(), ()>::builder()
    ///     .step(|_cx, _| async move { To::done() })
    ///     .name("example")
    ///     .pool(pool);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # });
    /// # }
    /// ```
    pub fn pool(self, pool: PgPool) -> Builder<I, (), S, PoolSet<S>, ASet> {
        let QueueNameSet { queue_name, state } = self.builder_state;
        Builder {
            builder_state: PoolSet {
                state,
                queue_name,
                pool,
            },
            steps: self.steps,
            activity_registry: self.activity_registry,
            _marker: PhantomData,
        }
    }
}

impl<I, S, ASet> Builder<I, (), S, PoolSet<S>, ASet>
where
    I: Send + Sync + 'static,
    S: Clone + Send + Sync + 'static,
{
    /// Finalize the builder into a `Workflow`.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use std::env;
    ///
    /// use sqlx::PgPool;
    /// use underway::{To, Workflow};
    ///
    /// # use tokio::runtime::Runtime;
    /// # fn main() {
    /// # let rt = Runtime::new().unwrap();
    /// # rt.block_on(async {
    /// let pool = PgPool::connect(&env::var("DATABASE_URL").unwrap()).await?;
    ///
    /// // Build the workflow.
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
    pub async fn build(self) -> Result<Workflow<I, S>> {
        let PoolSet {
            state,
            queue_name,
            pool,
        } = self.builder_state;
        let queue = Queue::builder().name(queue_name).pool(pool).build().await?;
        Ok(Workflow {
            queue: Arc::new(queue),
            steps: Arc::new(self.steps),
            state,
            activity_registry: self.activity_registry,
            _marker: PhantomData,
        })
    }
}

// Directly provide queue.
impl<I, S, ASet> Builder<I, (), S, StepSet<(), S>, ASet>
where
    I: Send + Sync + 'static,
    S: Clone + Send + Sync + 'static,
{
    /// Set the queue.
    ///
    /// This allows providing a `Queue` directly, for situations where the queue
    /// has been defined separately from the workflow.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use sqlx::PgPool;
    /// use underway::{Queue, To, Workflow};
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
    /// let workflow = Workflow::<(), ()>::builder()
    ///     .step(|_cx, _| async move { To::done() })
    ///     .queue(queue);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # });
    /// # }
    /// ```
    pub fn queue(self, queue: WorkflowQueue<I, S>) -> Builder<I, (), S, QueueSet<I, S>, ASet> {
        Builder {
            builder_state: QueueSet {
                state: self.builder_state.state,
                queue,
            },
            steps: self.steps,
            activity_registry: self.activity_registry,
            _marker: PhantomData,
        }
    }
}

impl<I, S, ASet> Builder<I, (), S, QueueSet<I, S>, ASet>
where
    I: Send + Sync + 'static,
    S: Clone + Send + Sync + 'static,
{
    /// Finalize the builder into a `Workflow`.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use sqlx::PgPool;
    /// use underway::{Workflow, To, Queue};
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
    /// // Build the workflow.
    /// let workflow = Workflow::<(), ()>::builder()
    ///     .step(|_cx, _| async move { To::done() })
    ///     .queue(queue)
    ///     .build();
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # });
    /// # }
    pub fn build(self) -> Workflow<I, S> {
        let QueueSet { state, queue } = self.builder_state;
        Workflow {
            queue: Arc::new(queue),
            steps: Arc::new(self.steps),
            state,
            activity_registry: self.activity_registry,
            _marker: PhantomData,
        }
    }
}
