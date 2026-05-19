use super::*;

/// Task enqueue command.
#[derive(Debug, Clone)]
pub struct EnqueueTask {
    /// Serialized task input.
    pub input: serde_json::Value,
    /// Task config.
    pub config: task::TaskConfig,
    /// Additional delay, e.g. transition or retry delay.
    pub delay: Span,
}

/// Claimed task record.
#[derive(Debug, Clone)]
pub struct ClaimedTask {
    /// Task ID.
    pub id: task::TaskId,
    /// Queue name.
    pub queue_name: String,
    /// Serialized input.
    pub input: serde_json::Value,
    /// Effective task config.
    pub config: task::TaskConfig,
    /// Current attempt number.
    pub attempt_number: i32,
}

/// Durable activity call record loaded for workflow replay.
#[derive(Debug, Clone)]
pub struct ActivityCallRecord {
    /// Deterministic call key.
    pub call_key: String,
    /// Activity name.
    pub activity: String,
    /// Serialized input.
    pub input: serde_json::Value,
    /// Serialized output.
    pub output: Option<serde_json::Value>,
    /// Serialized activity error envelope.
    pub error: Option<serde_json::Value>,
    /// Durable state.
    pub state: activity::CallState,
}

/// Activity call command.
#[derive(Debug, Clone)]
pub struct ActivityCallCommand {
    /// Deterministic call key.
    pub call_key: String,
    /// Activity name.
    pub activity: String,
    /// Serialized input.
    pub input: serde_json::Value,
    /// Activity timeout.
    pub timeout: Span,
    /// Activity retry policy.
    pub retry_policy: task::RetryPolicy,
}

/// Claimed activity call.
#[derive(Debug, Clone)]
pub struct ClaimedActivityCall {
    /// Activity call ID.
    pub id: Uuid,
    /// Queue name.
    pub queue_name: String,
    /// Workflow run ID.
    pub workflow_run_id: workflow::WorkflowRunId,
    /// Workflow step index.
    pub step_index: usize,
    /// Deterministic call key.
    pub call_key: String,
    /// Activity name.
    pub activity: String,
    /// Serialized input.
    pub input: serde_json::Value,
    /// Current attempt count.
    pub attempt_count: i32,
    /// Current attempt number.
    pub attempt_number: i32,
}

/// Durable schedule row.
#[derive(Debug, Clone)]
pub struct StoredSchedule {
    /// Zoned schedule.
    pub schedule: scheduler::ZonedSchedule,
    /// Serialized task input template.
    pub input: serde_json::Value,
}

/// Long-lived scheduler lease.
pub trait SchedulerLease: Send {
    /// Backend error type.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Releases the lease.
    fn release(&mut self) -> impl Future<Output = StdResult<(), Self::Error>> + Send;
}

/// Backend transaction semantic API.
pub trait BackendTransaction: Send {
    /// Backend error type.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Commits the transaction.
    fn commit(self) -> impl Future<Output = StdResult<(), Self::Error>> + Send;

    /// Rolls back the transaction.
    fn rollback(self) -> impl Future<Output = StdResult<(), Self::Error>> + Send;

    /// Creates a queue and optional DLQ.
    fn create_queue(
        &mut self,
        name: &str,
        dlq_name: Option<&str>,
    ) -> impl Future<Output = StdResult<(), Self::Error>> + Send;

    /// Enqueues one task.
    fn enqueue_task(
        &mut self,
        queue_name: &str,
        task: EnqueueTask,
    ) -> impl Future<Output = StdResult<task::TaskId, Self::Error>> + Send;

    /// Claims the next available task.
    fn claim_task(
        &mut self,
        queue_name: &str,
    ) -> impl Future<Output = StdResult<Option<ClaimedTask>, Self::Error>> + Send;

    /// Marks the claimed task succeeded.
    fn mark_task_succeeded(
        &mut self,
        task: &ClaimedTask,
    ) -> impl Future<Output = StdResult<(), Self::Error>> + Send;

    /// Marks the claimed task waiting.
    fn mark_task_waiting(
        &mut self,
        task: &ClaimedTask,
    ) -> impl Future<Output = StdResult<(), Self::Error>> + Send;

    /// Records a task failure without terminalizing.
    fn record_task_failure(
        &mut self,
        task: &ClaimedTask,
        error: &task::Error,
    ) -> impl Future<Output = StdResult<(), Self::Error>> + Send;

    /// Requeues a task after delay.
    fn retry_task(
        &mut self,
        task: &ClaimedTask,
        delay: Span,
    ) -> impl Future<Output = StdResult<(), Self::Error>> + Send;

    /// Marks a task failed.
    fn mark_task_failed(
        &mut self,
        task: &ClaimedTask,
    ) -> impl Future<Output = StdResult<(), Self::Error>> + Send;

    /// Moves a task to a dead-letter queue.
    fn move_task_to_dlq(
        &mut self,
        task_id: task::TaskId,
        from_queue: &str,
        dlq_name: &str,
    ) -> impl Future<Output = StdResult<(), Self::Error>> + Send;

    /// Cancels pending tasks for a workflow run.
    fn cancel_workflow(
        &mut self,
        queue_name: &str,
        workflow_run_id: workflow::WorkflowRunId,
    ) -> impl Future<Output = StdResult<bool, Self::Error>> + Send;

    /// Stores or replaces a schedule.
    fn schedule_task(
        &mut self,
        queue_name: &str,
        schedule: scheduler::ZonedSchedule,
        input: serde_json::Value,
    ) -> impl Future<Output = StdResult<(), Self::Error>> + Send;

    /// Removes a schedule.
    fn unschedule_task(
        &mut self,
        queue_name: &str,
    ) -> impl Future<Output = StdResult<(), Self::Error>> + Send;

    /// Loads a schedule.
    fn task_schedule(
        &mut self,
        queue_name: &str,
    ) -> impl Future<Output = StdResult<Option<StoredSchedule>, Self::Error>> + Send;

    /// Loads activity calls for deterministic workflow replay.
    fn load_activity_calls(
        &mut self,
        queue_name: &str,
        workflow_run_id: workflow::WorkflowRunId,
        step_index: usize,
    ) -> impl Future<Output = StdResult<Vec<ActivityCallRecord>, Self::Error>> + Send;

    /// Persists an activity call intent idempotently.
    fn insert_activity_call(
        &mut self,
        queue_name: &str,
        workflow_run_id: workflow::WorkflowRunId,
        step_index: usize,
        command: ActivityCallCommand,
    ) -> impl Future<Output = StdResult<(), Self::Error>> + Send;

    /// Claims the next matching activity call.
    fn claim_activity_call(
        &mut self,
        queue_name: &str,
        activities: &[String],
    ) -> impl Future<Output = StdResult<Option<ClaimedActivityCall>, Self::Error>> + Send;

    /// Completes an activity call.
    fn complete_activity_call(
        &mut self,
        call: &ClaimedActivityCall,
        output: serde_json::Value,
    ) -> impl Future<Output = StdResult<(), Self::Error>> + Send;

    /// Requeues an activity call after a retryable failure.
    fn retry_activity_call(
        &mut self,
        call: &ClaimedActivityCall,
        error: activity::Error,
        delay: Span,
    ) -> impl Future<Output = StdResult<(), Self::Error>> + Send;

    /// Terminally fails an activity call.
    fn fail_activity_call(
        &mut self,
        call: &ClaimedActivityCall,
        error: activity::Error,
    ) -> impl Future<Output = StdResult<(), Self::Error>> + Send;

    /// Wakes a waiting workflow task after activity completion.
    fn wake_workflow_task(
        &mut self,
        queue_name: &str,
        workflow_run_id: workflow::WorkflowRunId,
        step_index: usize,
    ) -> impl Future<Output = StdResult<(), Self::Error>> + Send;

    /// Deletes expired terminal tasks.
    fn delete_expired(&mut self) -> impl Future<Output = StdResult<(), Self::Error>> + Send;
}

/// Durable backend.
pub trait Backend: Clone + Send + Sync + 'static {
    /// Backend error type.
    type Error: std::error::Error + Send + Sync + 'static;

    /// Backend transaction type.
    type Transaction<'a>: BackendTransaction<Error = Self::Error> + Send
    where
        Self: 'a;

    /// Scheduler lease type.
    type SchedulerLease<'a>: SchedulerLease<Error = Self::Error> + Send
    where
        Self: 'a;

    /// Begins a transaction.
    fn begin(&self) -> impl Future<Output = StdResult<Self::Transaction<'_>, Self::Error>> + Send;

    /// Records a heartbeat outside the task transaction.
    fn record_heartbeat(
        &self,
        task: &ClaimedTask,
    ) -> impl Future<Output = StdResult<(), Self::Error>> + Send;

    /// Tries to acquire a singleton scheduler lease.
    fn try_acquire_scheduler(
        &self,
        queue_name: &str,
    ) -> impl Future<Output = StdResult<Option<Self::SchedulerLease<'_>>, Self::Error>> + Send;

    /// Waits for possible task work. Backends may implement this with
    /// notifications; polling remains authoritative.
    fn wait_for_task_change(
        &self,
        _queue_name: &str,
        timeout: StdDuration,
    ) -> impl Future<Output = StdResult<(), Self::Error>> + Send {
        async move {
            sleep(timeout).await;
            Ok(())
        }
    }

    /// Waits for possible activity work.
    fn wait_for_activity_change(
        &self,
        _queue_name: &str,
        timeout: StdDuration,
    ) -> impl Future<Output = StdResult<(), Self::Error>> + Send {
        async move {
            sleep(timeout).await;
            Ok(())
        }
    }
}
