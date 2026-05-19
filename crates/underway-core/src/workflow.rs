use super::*;

pub(crate) mod registration {
    use super::*;

    /// Marker for no activities.
    pub struct NoActivities;
    /// Type-level registered set.
    pub struct Registered<Head, Tail>(pub(crate) PhantomData<(Head, Tail)>);
    /// Type-level head index.
    pub struct Here;
    /// Type-level tail index.
    pub struct There<T>(pub(crate) PhantomData<T>);

    mod sealed {
        pub trait ActivitySet {}
    }

    /// Type-level set of registered activities.
    pub trait ActivitySet: sealed::ActivitySet + 'static {}

    impl<T> ActivitySet for T where T: sealed::ActivitySet + 'static {}
    impl sealed::ActivitySet for NoActivities {}
    impl<Head, Tail> sealed::ActivitySet for Registered<Head, Tail>
    where
        Head: activity::Activity,
        Tail: ActivitySet,
    {
    }

    mod private {
        use super::{activity, Here, Registered, There};

        pub trait Contains<A: activity::Activity, Idx> {}
        impl<A, Tail> Contains<A, Here> for Registered<A, Tail> where A: activity::Activity {}
        impl<A, Head, Tail, Idx> Contains<A, There<Idx>> for Registered<Head, Tail>
        where
            A: activity::Activity,
            Head: activity::Activity,
            Tail: Contains<A, Idx>,
        {
        }
    }

    /// Marker trait indicating activity membership.
    pub trait Contains<A: activity::Activity, Idx>: ActivitySet {}
    impl<Set, A, Idx> Contains<A, Idx> for Set
    where
        Set: ActivitySet + private::Contains<A, Idx>,
        A: activity::Activity,
    {
    }
}

use activity_worker::ActivityRegistry;
use registration::{Contains, NoActivities, Registered};

/// Unique identifier of a workflow run.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Hash, Eq, PartialEq)]
pub struct WorkflowRunId(Uuid);

impl WorkflowRunId {
    /// Generates a new workflow run ID.
    pub fn new() -> Self {
        Self(Ulid::new().into())
    }

    /// Reconstitutes from UUID.
    pub const fn from_uuid(id: Uuid) -> Self {
        Self(id)
    }

    /// Returns UUID.
    pub const fn as_uuid(self) -> Uuid {
        self.0
    }

    /// Returns hyphenated UUID formatting.
    pub fn as_hyphenated(&self) -> &uuid::fmt::Hyphenated {
        self.0.as_hyphenated()
    }
}

impl Default for WorkflowRunId {
    fn default() -> Self {
        Self::new()
    }
}

impl Deref for WorkflowRunId {
    type Target = Uuid;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Display for WorkflowRunId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
/// Persisted workflow task input used internally by workflow queues.
pub struct WorkflowState {
    step_index: usize,
    step_input: serde_json::Value,
    #[serde(default = "WorkflowRunId::new")]
    workflow_run_id: WorkflowRunId,
}

#[derive(Debug, Serialize, Deserialize, PartialEq)]
struct WorkflowScheduleTemplate {
    step_index: usize,
    step_input: serde_json::Value,
}

/// Enqueued workflow handle.
pub struct EnqueuedWorkflow<B, T>
where
    B: backend::Backend,
    T: task::Task<B>,
{
    run_id: WorkflowRunId,
    queue: Arc<queue::Queue<B, T>>,
}

impl<B, T> EnqueuedWorkflow<B, T>
where
    B: backend::Backend,
    T: task::Task<B>,
{
    /// Workflow run ID.
    pub fn run_id(&self) -> WorkflowRunId {
        self.run_id
    }

    /// Cancels pending workflow tasks.
    pub async fn cancel(&self) -> Result<bool> {
        let mut tx = self.queue.backend.begin().await.map_err(Error::backend)?;
        let cancelled = tx
            .cancel_workflow(&self.queue.name, self.run_id)
            .await
            .map_err(Error::backend)?;
        tx.commit().await.map_err(Error::backend)?;
        Ok(cancelled)
    }
}

/// Context passed to workflow steps.
pub struct Context<S, Set = NoActivities> {
    /// Shared, non-durable workflow state.
    pub state: S,
    /// Current zero-based step index.
    pub step_index: usize,
    /// Total step count.
    pub step_count: usize,
    /// Workflow run ID.
    pub workflow_run_id: WorkflowRunId,
    /// Queue name.
    pub queue_name: String,
    activity_call_buffer: Arc<Mutex<ActivityCallBuffer>>,
    call_sequence_state: Arc<Mutex<CallSequenceState>>,
    _activity_set: PhantomData<fn() -> Set>,
}

struct ContextParts<S> {
    state: S,
    step_index: usize,
    step_count: usize,
    workflow_run_id: WorkflowRunId,
    queue_name: String,
    activity_call_buffer: Arc<Mutex<ActivityCallBuffer>>,
    call_sequence_state: Arc<Mutex<CallSequenceState>>,
}

#[derive(Default)]
struct ActivityCallBuffer {
    records: HashMap<String, backend::ActivityCallRecord>,
    commands: Vec<ActivityCallCommand>,
}

#[derive(Clone)]
struct ActivityCallCommand {
    call_key: String,
    activity: String,
    input: serde_json::Value,
}

#[derive(Default)]
struct CallSequenceState {
    unresolved_call_key: Option<String>,
    next_activity_call_index: usize,
}

impl ActivityCallBuffer {
    fn new(records: Vec<backend::ActivityCallRecord>) -> Self {
        Self {
            records: records
                .into_iter()
                .map(|record| (record.call_key.clone(), record))
                .collect(),
            commands: Vec::new(),
        }
    }

    fn register_call(
        &mut self,
        key: &str,
        activity: &str,
        input: &serde_json::Value,
    ) -> task::Result<backend::ActivityCallRecord> {
        if let Some(record) = self.records.get(key) {
            if record.activity != activity {
                return Err(task::Error::Fatal(format!(
                    "Non-deterministic activity call detected: key `{key}` was previously bound \
                     to `{}` but now `{activity}`.",
                    record.activity
                )));
            }
            if record.input != *input {
                return Err(task::Error::Fatal(format!(
                    "Non-deterministic activity call input detected for key `{key}`."
                )));
            }
            return Ok(record.clone());
        }

        let record = backend::ActivityCallRecord {
            call_key: key.to_string(),
            activity: activity.to_string(),
            input: input.clone(),
            output: None,
            error: None,
            state: activity::CallState::Pending,
        };
        self.records.insert(key.to_string(), record.clone());
        self.commands.push(ActivityCallCommand {
            call_key: key.to_string(),
            activity: activity.to_string(),
            input: input.clone(),
        });
        Ok(record)
    }

    fn drain_commands(&mut self) -> Vec<ActivityCallCommand> {
        std::mem::take(&mut self.commands)
    }
}

impl<S, Set> Context<S, Set> {
    fn from_parts(parts: ContextParts<S>) -> Self {
        Self {
            state: parts.state,
            step_index: parts.step_index,
            step_count: parts.step_count,
            workflow_run_id: parts.workflow_run_id,
            queue_name: parts.queue_name,
            activity_call_buffer: parts.activity_call_buffer,
            call_sequence_state: parts.call_sequence_state,
            _activity_set: PhantomData,
        }
    }

    pub(crate) fn emit_indexed<A, Idx>(&mut self, input: &A::Input) -> task::Result<()>
    where
        A: activity::Activity,
        Set: Contains<A, Idx>,
    {
        let key = self.next_activity_call_key()?;
        let input =
            serde_json::to_value(input).map_err(|err| task::Error::Fatal(err.to_string()))?;
        self.register_activity_call(&key, A::NAME, &input)?;
        Ok(())
    }

    pub(crate) fn call_indexed<A, Idx>(&mut self, input: &A::Input) -> task::Result<A::Output>
    where
        A: activity::Activity,
        Set: Contains<A, Idx>,
    {
        let key = self.next_activity_call_key()?;
        let input =
            serde_json::to_value(input).map_err(|err| task::Error::Fatal(err.to_string()))?;
        let record = self.register_activity_call(&key, A::NAME, &input)?;

        match record.state {
            activity::CallState::Succeeded => {
                let output = record.output.ok_or_else(|| {
                    task::Error::Fatal(format!(
                        "Missing output for activity call `{}` with key `{key}`.",
                        A::NAME
                    ))
                })?;
                serde_json::from_value(output).map_err(|err| task::Error::Fatal(err.to_string()))
            }
            activity::CallState::Failed => {
                let error = record
                    .error
                    .and_then(|value| serde_json::from_value::<activity::Error>(value).ok())
                    .unwrap_or_else(|| {
                        activity::Error::fatal(
                            "activity_call_failed",
                            format!("Activity call `{}` with key `{key}` failed.", A::NAME),
                        )
                    });
                if error.retryable {
                    Err(task::Error::Retryable(error.to_string()))
                } else {
                    Err(task::Error::Fatal(error.to_string()))
                }
            }
            activity::CallState::Pending | activity::CallState::InProgress => {
                self.register_call_sequence_wait(&key)?;
                Err(task::Error::Suspended(format!(
                    "Waiting on activity call `{}` with key `{key}`.",
                    A::NAME
                )))
            }
        }
    }

    fn next_activity_call_key(&self) -> task::Result<String> {
        let mut state = self.call_sequence_state.lock().map_err(|_| {
            task::Error::Fatal("Call sequence state lock was poisoned.".to_string())
        })?;
        let operation_index = state.next_activity_call_index;
        state.next_activity_call_index =
            state
                .next_activity_call_index
                .checked_add(1)
                .ok_or_else(|| {
                    task::Error::Fatal("Activity operation index overflowed.".to_string())
                })?;
        Ok(format!(
            "activity:{}:{}:{operation_index}",
            self.workflow_run_id.as_hyphenated(),
            self.step_index
        ))
    }

    fn register_activity_call(
        &self,
        key: &str,
        activity: &str,
        input: &serde_json::Value,
    ) -> task::Result<backend::ActivityCallRecord> {
        let mut buffer = self.activity_call_buffer.lock().map_err(|_| {
            task::Error::Fatal("Activity call buffer lock was poisoned.".to_string())
        })?;
        buffer.register_call(key, activity, input)
    }

    fn register_call_sequence_wait(&self, key: &str) -> task::Result<()> {
        let mut state = self.call_sequence_state.lock().map_err(|_| {
            task::Error::Fatal("Call sequence state lock was poisoned.".to_string())
        })?;
        if let Some(existing) = &state.unresolved_call_key {
            if existing != key {
                return Err(task::Error::Fatal(
                    "Multiple unresolved `call` operations in one step execution are not \
                     supported."
                        .to_string(),
                ));
            }
        } else {
            state.unresolved_call_key = Some(key.to_string());
        }
        Ok(())
    }
}

/// Activity invocation helpers.
pub trait InvokeActivity: activity::Activity + Sized {
    /// Emits a durable fire-and-forget activity call.
    fn emit<'a, S, Set, Idx>(
        cx: &'a mut Context<S, Set>,
        input: &'a Self::Input,
    ) -> impl Future<Output = task::Result<()>> + Send + 'a
    where
        Set: Contains<Self, Idx>,
        S: Send + 'a;

    /// Calls an activity and returns its durable output.
    fn call<'a, S, Set, Idx>(
        cx: &'a mut Context<S, Set>,
        input: &'a Self::Input,
    ) -> impl Future<Output = task::Result<Self::Output>> + Send + 'a
    where
        Set: Contains<Self, Idx>,
        S: Send + 'a;
}

impl<A> InvokeActivity for A
where
    A: activity::Activity,
{
    fn emit<'a, S, Set, Idx>(
        cx: &'a mut Context<S, Set>,
        input: &'a Self::Input,
    ) -> impl Future<Output = task::Result<()>> + Send + 'a
    where
        Set: Contains<Self, Idx>,
        S: Send + 'a,
    {
        std::future::ready(cx.emit_indexed::<Self, Idx>(input))
    }

    fn call<'a, S, Set, Idx>(
        cx: &'a mut Context<S, Set>,
        input: &'a Self::Input,
    ) -> impl Future<Output = task::Result<Self::Output>> + Send + 'a
    where
        Set: Contains<Self, Idx>,
        S: Send + 'a,
    {
        std::future::ready(cx.call_indexed::<Self, Idx>(input))
    }
}

/// Step transition.
#[derive(Deserialize, Serialize)]
pub enum Transition<N> {
    /// Continue with next step payload after delay.
    Continue {
        /// Next step payload.
        next: N,
        /// Delay before next step runs.
        delay: Span,
    },
    /// Complete workflow.
    Complete,
}

impl<N> Transition<N> {
    /// Continue to next step.
    pub fn next(next: N) -> task::Result<Self> {
        Ok(Self::Continue {
            next,
            delay: Span::new(),
        })
    }

    /// Continue after delay.
    pub fn after(next: N, delay: Span) -> task::Result<Self> {
        Ok(Self::Continue { next, delay })
    }
}

impl Transition<()> {
    /// Complete workflow.
    pub fn complete() -> task::Result<Self> {
        Ok(Self::Complete)
    }
}

struct StepConfig<S> {
    executor: Box<dyn StepExecutor<S>>,
    task_config: task::TaskConfig,
}

impl<S> StepConfig<S> {
    fn new<I, O, A, F, Fut>(func: F) -> Self
    where
        I: DeserializeOwned + Serialize + Send + Sync + 'static,
        O: Serialize + Send + Sync + 'static,
        S: Send + Sync + 'static,
        A: 'static,
        F: Fn(Context<S, A>, I) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = task::Result<Transition<O>>> + Send + 'static,
    {
        let step_fn = StepFn::new(move |cx, input| {
            Box::pin(func(cx, input))
                as Pin<Box<dyn Future<Output = task::Result<Transition<O>>> + Send>>
        });
        Self {
            executor: Box::new(step_fn),
            task_config: task::TaskConfig::default(),
        }
    }
}

type StepResult = task::Result<Option<(serde_json::Value, Span)>>;

trait StepExecutor<S>: Send + Sync {
    fn execute_step(
        &self,
        cx: ContextParts<S>,
        input: serde_json::Value,
    ) -> Pin<Box<dyn Future<Output = StepResult> + Send>>;
}

type StepFnMarker<I, O, S, A> = fn() -> (I, O, S, A);

struct StepFn<I, O, S, A, F> {
    func: Arc<F>,
    _marker: PhantomData<StepFnMarker<I, O, S, A>>,
}

impl<I, O, S, A, F> StepFn<I, O, S, A, F> {
    fn new(func: F) -> Self {
        Self {
            func: Arc::new(func),
            _marker: PhantomData,
        }
    }
}

impl<I, O, S, A, F> StepExecutor<S> for StepFn<I, O, S, A, F>
where
    I: DeserializeOwned + Serialize + Send + Sync + 'static,
    O: Serialize + Send + Sync + 'static,
    S: Send + Sync + 'static,
    A: 'static,
    F: Fn(Context<S, A>, I) -> Pin<Box<dyn Future<Output = task::Result<Transition<O>>> + Send>>
        + Send
        + Sync
        + 'static,
{
    fn execute_step(
        &self,
        cx: ContextParts<S>,
        input: serde_json::Value,
    ) -> Pin<Box<dyn Future<Output = StepResult> + Send>> {
        let input: I = match serde_json::from_value(input) {
            Ok(input) => input,
            Err(err) => return Box::pin(async move { Err(task::Error::Fatal(err.to_string())) }),
        };
        let cx: Context<S, A> = Context::from_parts(cx);
        let fut = (self.func)(cx, input);
        Box::pin(async move {
            match fut.await? {
                Transition::Continue { next, delay } => {
                    let next = serde_json::to_value(next)
                        .map_err(|err| task::Error::Fatal(err.to_string()))?;
                    Ok(Some((next, delay)))
                }
                Transition::Complete => Ok(None),
            }
        })
    }
}

/// Sequential typed workflow.
pub struct Workflow<B, I, S = ()>
where
    B: backend::Backend,
    I: Send + Sync + 'static,
    S: Clone + Send + Sync + 'static,
{
    queue: Arc<queue::Queue<B, Self>>,
    steps: Arc<Vec<StepConfig<S>>>,
    state: S,
    activity_registry: ActivityRegistry,
    _marker: PhantomData<I>,
}

impl<B, I> Workflow<B, I, ()>
where
    B: backend::Backend,
    I: Serialize + Send + Sync + 'static,
{
    /// Creates a workflow builder.
    pub fn builder() -> Builder<B, I, I, (), Initial, NoActivities> {
        Builder::new()
    }
}

impl<B, I, S> Clone for Workflow<B, I, S>
where
    B: backend::Backend,
    I: Send + Sync + 'static,
    S: Clone + Send + Sync + 'static,
{
    fn clone(&self) -> Self {
        Self {
            queue: Arc::clone(&self.queue),
            steps: Arc::clone(&self.steps),
            state: self.state.clone(),
            activity_registry: self.activity_registry.clone(),
            _marker: PhantomData,
        }
    }
}

impl<B, I, S> Workflow<B, I, S>
where
    B: backend::Backend,
    I: Serialize + Send + Sync + 'static,
    S: Clone + Send + Sync + 'static,
{
    /// Enqueues a workflow.
    pub async fn enqueue(&self, input: &I) -> Result<EnqueuedWorkflow<B, Self>> {
        let mut tx = self.queue.backend.begin().await.map_err(Error::backend)?;
        let enqueued = self.enqueue_using(&mut tx, input).await?;
        tx.commit().await.map_err(Error::backend)?;
        Ok(enqueued)
    }

    /// Enqueues many workflows.
    pub async fn enqueue_many(&self, inputs: &[I]) -> Result<Vec<EnqueuedWorkflow<B, Self>>> {
        let mut tx = self.queue.backend.begin().await.map_err(Error::backend)?;
        let mut enqueued = Vec::with_capacity(inputs.len());
        for input in inputs {
            enqueued.push(self.enqueue_using(&mut tx, input).await?);
        }
        tx.commit().await.map_err(Error::backend)?;
        Ok(enqueued)
    }

    /// Enqueues a workflow using an existing transaction.
    pub async fn enqueue_using(
        &self,
        tx: &mut B::Transaction<'_>,
        input: &I,
    ) -> Result<EnqueuedWorkflow<B, Self>> {
        self.enqueue_after_using(tx, input, Span::new()).await
    }

    /// Enqueues a workflow after a delay.
    pub async fn enqueue_after(&self, input: &I, delay: Span) -> Result<EnqueuedWorkflow<B, Self>> {
        let mut tx = self.queue.backend.begin().await.map_err(Error::backend)?;
        let enqueued = self.enqueue_after_using(&mut tx, input, delay).await?;
        tx.commit().await.map_err(Error::backend)?;
        Ok(enqueued)
    }

    /// Enqueues a workflow after delay using an existing transaction.
    pub async fn enqueue_after_using(
        &self,
        tx: &mut B::Transaction<'_>,
        input: &I,
        delay: Span,
    ) -> Result<EnqueuedWorkflow<B, Self>> {
        let workflow_input = self.first_workflow_input(input)?;
        self.queue
            .enqueue_after_using(tx, self, &workflow_input, delay)
            .await?;
        Ok(EnqueuedWorkflow {
            run_id: workflow_input.workflow_run_id,
            queue: Arc::clone(&self.queue),
        })
    }

    /// Stores or replaces a workflow schedule.
    pub async fn schedule(&self, schedule: &scheduler::ZonedSchedule, input: &I) -> Result {
        let mut tx = self.queue.backend.begin().await.map_err(Error::backend)?;
        self.schedule_using(&mut tx, schedule, input).await?;
        tx.commit().await.map_err(Error::backend)?;
        Ok(())
    }

    /// Stores or replaces a workflow schedule using a transaction.
    pub async fn schedule_using(
        &self,
        tx: &mut B::Transaction<'_>,
        schedule: &scheduler::ZonedSchedule,
        input: &I,
    ) -> Result {
        let template = self.first_workflow_schedule_template(input)?;
        tx.schedule_task(
            &self.queue.name,
            schedule.clone(),
            serde_json::to_value(template)?,
        )
        .await
        .map_err(Error::backend)
    }

    /// Removes workflow schedule.
    pub async fn unschedule(&self) -> Result {
        self.queue.unschedule().await
    }

    /// Removes workflow schedule using a transaction.
    pub async fn unschedule_using(&self, tx: &mut B::Transaction<'_>) -> Result {
        self.queue.unschedule_using(tx).await
    }

    /// Returns queue.
    pub fn queue(&self) -> Arc<queue::Queue<B, Self>> {
        Arc::clone(&self.queue)
    }

    pub(crate) fn activity_registry(&self) -> ActivityRegistry {
        self.activity_registry.clone()
    }

    /// Returns runtime.
    pub fn runtime(&self) -> runtime::Runtime<B, I, S> {
        runtime::Runtime::new(self.clone())
    }

    fn first_workflow_input(&self, input: &I) -> Result<WorkflowState> {
        Ok(WorkflowState {
            step_index: 0,
            step_input: serde_json::to_value(input)?,
            workflow_run_id: WorkflowRunId::new(),
        })
    }

    fn first_workflow_schedule_template(&self, input: &I) -> Result<WorkflowScheduleTemplate> {
        Ok(WorkflowScheduleTemplate {
            step_index: 0,
            step_input: serde_json::to_value(input)?,
        })
    }
}

impl<B, I, S> Workflow<B, I, S>
where
    B: backend::Backend,
    I: Send + Sync + 'static,
    S: Clone + Send + Sync + 'static,
{
    fn step_task_config(&self, index: usize) -> task::TaskConfig {
        self.steps
            .get(index)
            .map(|step| step.task_config.clone())
            .unwrap_or_default()
    }

    async fn persist_activity_call_commands(
        &self,
        tx: &mut B::Transaction<'_>,
        workflow_run_id: WorkflowRunId,
        step_index: usize,
        activity_call_buffer: &Arc<Mutex<ActivityCallBuffer>>,
    ) -> task::Result<()> {
        let commands = {
            let mut buffer = activity_call_buffer.lock().map_err(|_| {
                task::Error::Fatal("Activity call buffer lock was poisoned.".to_string())
            })?;
            buffer.drain_commands()
        };

        for command in commands {
            let Some((retry_policy, timeout)) =
                self.activity_registry.call_policy(&command.activity)
            else {
                return Err(task::Error::Fatal(format!(
                    "No activity policy registered for `{}`.",
                    command.activity
                )));
            };
            tx.insert_activity_call(
                &self.queue.name,
                workflow_run_id,
                step_index,
                backend::ActivityCallCommand {
                    call_key: command.call_key,
                    activity: command.activity,
                    input: command.input,
                    timeout,
                    retry_policy,
                },
            )
            .await
            .map_err(|err| task::Error::Fatal(err.to_string()))?;
        }

        Ok(())
    }
}

impl<B, I, S> task::Task<B> for Workflow<B, I, S>
where
    B: backend::Backend,
    I: Send + Sync + 'static,
    S: Clone + Send + Sync + 'static,
{
    type Input = WorkflowState;
    type Output = ();

    async fn execute(
        &self,
        tx: &mut B::Transaction<'_>,
        input: Self::Input,
    ) -> task::Result<Self::Output> {
        let WorkflowState {
            step_index,
            step_input,
            workflow_run_id,
        } = input;

        if step_index >= self.steps.len() {
            return Err(task::Error::Fatal("Invalid step index.".to_string()));
        }

        let activity_calls = tx
            .load_activity_calls(&self.queue.name, workflow_run_id, step_index)
            .await
            .map_err(|err| task::Error::Retryable(err.to_string()))?;
        let activity_call_buffer = Arc::new(Mutex::new(ActivityCallBuffer::new(activity_calls)));
        let call_sequence_state = Arc::new(Mutex::new(CallSequenceState::default()));

        let cx = ContextParts {
            state: self.state.clone(),
            step_index,
            step_count: self.steps.len(),
            workflow_run_id,
            queue_name: self.queue.name.clone(),
            activity_call_buffer: Arc::clone(&activity_call_buffer),
            call_sequence_state,
        };

        let step_result = self.steps[step_index]
            .executor
            .execute_step(cx, step_input)
            .await;

        if matches!(step_result.as_ref(), Ok(_) | Err(task::Error::Suspended(_))) {
            self.persist_activity_call_commands(
                tx,
                workflow_run_id,
                step_index,
                &activity_call_buffer,
            )
            .await?;
        }

        let step_result = step_result?;

        if let Some((next_input, delay)) = step_result {
            let next = WorkflowState {
                step_index: step_index + 1,
                step_input: next_input,
                workflow_run_id,
            };
            self.queue
                .enqueue_after_using(tx, self, &next, delay)
                .await
                .map_err(|err| task::Error::Retryable(err.to_string()))?;
        }

        Ok(())
    }

    fn config(&self) -> task::ConfigResult {
        Ok(self.step_task_config(0))
    }

    fn config_for(&self, input: &Self::Input) -> task::ConfigResult {
        Ok(self.step_task_config(input.step_index))
    }
}

type BuilderMarker<B, I, O, S, A> = fn() -> (B, I, O, S, A);

/// Workflow builder.
pub struct Builder<B, I, O, S, State, A = NoActivities> {
    builder_state: State,
    steps: Vec<StepConfig<S>>,
    activity_registry: ActivityRegistry,
    _marker: PhantomData<BuilderMarker<B, I, O, S, A>>,
}

/// Initial builder state.
pub struct Initial;
/// State-set builder state.
pub struct StateSet<S> {
    state: S,
}
/// Step-set builder state.
pub struct StepSet<Current, S> {
    state: S,
    _marker: PhantomData<Current>,
}
/// Name-set builder state.
pub struct NameSet<Current, S> {
    state: S,
    queue_name: String,
    _marker: PhantomData<Current>,
}
/// Backend-set builder state.
pub struct BackendSet<Current, S, B> {
    state: S,
    queue_name: String,
    backend: B,
    _marker: PhantomData<Current>,
}

impl<B, I> Builder<B, I, I, (), Initial, NoActivities>
where
    B: backend::Backend,
{
    fn new() -> Self {
        Self {
            builder_state: Initial,
            steps: Vec::new(),
            activity_registry: ActivityRegistry::default(),
            _marker: PhantomData,
        }
    }
}

impl<B, I, ASet> Builder<B, I, I, (), Initial, ASet>
where
    B: backend::Backend,
    ASet: 'static,
{
    /// Registers an activity handler.
    pub fn activity<A>(mut self, activity: A) -> Builder<B, I, I, (), Initial, Registered<A, ASet>>
    where
        ASet: registration::ActivitySet,
        A: activity::Activity,
    {
        self.activity_registry.register(activity);
        Builder {
            builder_state: Initial,
            steps: Vec::new(),
            activity_registry: self.activity_registry,
            _marker: PhantomData,
        }
    }

    /// Sets non-durable state.
    pub fn state<NewS>(self, state: NewS) -> Builder<B, I, I, NewS, StateSet<NewS>, ASet> {
        Builder {
            builder_state: StateSet { state },
            steps: Vec::new(),
            activity_registry: self.activity_registry,
            _marker: PhantomData,
        }
    }

    /// Adds first step.
    pub fn step<F, O, Fut>(mut self, func: F) -> Builder<B, I, O, (), StepSet<O, ()>, ASet>
    where
        I: DeserializeOwned + Serialize + Send + Sync + 'static,
        O: Serialize + Send + Sync + 'static,
        F: Fn(Context<(), ASet>, I) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = task::Result<Transition<O>>> + Send + 'static,
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

impl<B, I, S, ASet> Builder<B, I, I, S, StateSet<S>, ASet>
where
    B: backend::Backend,
    ASet: 'static,
{
    /// Registers an activity handler.
    pub fn activity<A>(
        mut self,
        activity: A,
    ) -> Builder<B, I, I, S, StateSet<S>, Registered<A, ASet>>
    where
        ASet: registration::ActivitySet,
        A: activity::Activity,
    {
        self.activity_registry.register(activity);
        Builder {
            builder_state: self.builder_state,
            steps: Vec::new(),
            activity_registry: self.activity_registry,
            _marker: PhantomData,
        }
    }

    /// Adds first step.
    pub fn step<F, O, Fut>(mut self, func: F) -> Builder<B, I, O, S, StepSet<O, S>, ASet>
    where
        I: DeserializeOwned + Serialize + Send + Sync + 'static,
        O: Serialize + Send + Sync + 'static,
        S: Clone + Send + Sync + 'static,
        F: Fn(Context<S, ASet>, I) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = task::Result<Transition<O>>> + Send + 'static,
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

impl<B, I, Current, S, ASet> Builder<B, I, Current, S, StepSet<Current, S>, ASet>
where
    B: backend::Backend,
    S: Clone + Send + Sync + 'static,
    ASet: 'static,
{
    /// Adds subsequent step.
    pub fn step<F, New, Fut>(mut self, func: F) -> Builder<B, I, New, S, StepSet<New, S>, ASet>
    where
        Current: DeserializeOwned + Serialize + Send + Sync + 'static,
        New: Serialize + Send + Sync + 'static,
        F: Fn(Context<S, ASet>, Current) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = task::Result<Transition<New>>> + Send + 'static,
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

    /// Sets retry policy for previous step.
    pub fn retry_policy(mut self, retry_policy: task::RetryPolicy) -> Self {
        self.steps
            .last_mut()
            .expect("step must exist")
            .task_config
            .set_retry_policy(retry_policy);
        self
    }

    /// Sets timeout for previous step.
    pub fn timeout(mut self, timeout: Span) -> Self {
        self.steps
            .last_mut()
            .expect("step must exist")
            .task_config
            .set_timeout(timeout);
        self
    }

    /// Sets TTL for previous step.
    pub fn ttl(mut self, ttl: Span) -> Self {
        self.steps
            .last_mut()
            .expect("step must exist")
            .task_config
            .set_ttl(ttl);
        self
    }

    /// Sets delay for previous step.
    pub fn delay(mut self, delay: Span) -> Self {
        self.steps
            .last_mut()
            .expect("step must exist")
            .task_config
            .set_delay(delay);
        self
    }

    /// Sets heartbeat for previous step.
    pub fn heartbeat(mut self, heartbeat: Span) -> Self {
        self.steps
            .last_mut()
            .expect("step must exist")
            .task_config
            .set_heartbeat(heartbeat);
        self
    }

    /// Sets concurrency key for previous step.
    pub fn concurrency_key(mut self, concurrency_key: impl Into<String>) -> Self {
        self.steps
            .last_mut()
            .expect("step must exist")
            .task_config
            .set_concurrency_key(Some(concurrency_key.into()));
        self
    }

    /// Sets priority for previous step.
    pub fn priority(mut self, priority: i32) -> Self {
        self.steps
            .last_mut()
            .expect("step must exist")
            .task_config
            .set_priority(priority);
        self
    }
}

impl<B, I, S, ASet> Builder<B, I, (), S, StepSet<(), S>, ASet>
where
    B: backend::Backend,
    I: Send + Sync + 'static,
    S: Clone + Send + Sync + 'static,
{
    /// Sets queue name.
    pub fn name(self, queue_name: impl Into<String>) -> Builder<B, I, (), S, NameSet<(), S>, ASet> {
        Builder {
            builder_state: NameSet {
                state: self.builder_state.state,
                queue_name: queue_name.into(),
                _marker: PhantomData,
            },
            steps: self.steps,
            activity_registry: self.activity_registry,
            _marker: PhantomData,
        }
    }
}

impl<B, I, S, ASet> Builder<B, I, (), S, NameSet<(), S>, ASet>
where
    B: backend::Backend,
    I: Send + Sync + 'static,
    S: Clone + Send + Sync + 'static,
{
    /// Sets backend.
    pub fn backend(self, backend: B) -> Builder<B, I, (), S, BackendSet<(), S, B>, ASet> {
        Builder {
            builder_state: BackendSet {
                state: self.builder_state.state,
                queue_name: self.builder_state.queue_name,
                backend,
                _marker: PhantomData,
            },
            steps: self.steps,
            activity_registry: self.activity_registry,
            _marker: PhantomData,
        }
    }
}

impl<B, I, S, ASet> Builder<B, I, (), S, BackendSet<(), S, B>, ASet>
where
    B: backend::Backend,
    I: Send + Sync + 'static,
    S: Clone + Send + Sync + 'static,
{
    /// Builds workflow.
    pub async fn build(self) -> Result<Workflow<B, I, S>> {
        let state = self.builder_state;
        for step in &self.steps {
            step.task_config.validate()?;
        }
        let queue = queue::Queue::builder()
            .name(state.queue_name)
            .backend(state.backend.clone())
            .build()
            .await?;
        Ok(Workflow {
            queue: Arc::new(queue),
            steps: Arc::new(self.steps),
            state: state.state,
            activity_registry: self.activity_registry,
            _marker: PhantomData,
        })
    }
}
