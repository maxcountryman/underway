//! Deterministic in-process backend for Underway.
#![forbid(unsafe_code)]

use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
    time::Duration,
};

use jiff::Span;
use tokio::sync::Notify;
use underway_core::{
    activity::{self, CallState},
    backend::{
        ActivityCallCommand, ActivityCallRecord, Backend, BackendTransaction, ClaimedActivityCall,
        ClaimedTask, EnqueueTask, SchedulerLease, StoredSchedule,
    },
    scheduler::ZonedSchedule,
    task::{self, State as TaskState, TaskConfig, TaskId},
    workflow::WorkflowRunId,
};
use uuid::Uuid;

/// Memory backend error.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Shared state lock was poisoned.
    #[error("memory backend state lock poisoned")]
    LockPoisoned,

    /// Duration conversion failed.
    #[error(transparent)]
    Jiff(#[from] jiff::Error),

    /// JSON serialization failed.
    #[error(transparent)]
    Json(#[from] serde_json::Error),

    /// Requested record was not found.
    #[error("{0} not found")]
    NotFound(String),
}

/// Deterministic in-process backend.
#[derive(Clone, Default)]
pub struct MemoryBackend {
    inner: Arc<Mutex<State>>,
    task_notify: Arc<Notify>,
    activity_notify: Arc<Notify>,
}

impl MemoryBackend {
    /// Creates an empty backend.
    pub fn new() -> Self {
        Self::default()
    }

    /// Advances the deterministic clock.
    pub fn advance(&self, span: Span) -> Result<(), Error> {
        let mut state = self.state()?;
        state.now_ms = state.now_ms.saturating_add(duration_ms(span)?);
        self.task_notify.notify_waiters();
        self.activity_notify.notify_waiters();
        Ok(())
    }

    fn state(&self) -> Result<std::sync::MutexGuard<'_, State>, Error> {
        self.inner.lock().map_err(|_| Error::LockPoisoned)
    }
}

#[derive(Default)]
struct State {
    now_ms: u64,
    sequence: u64,
    queues: HashMap<String, QueueRow>,
    tasks: HashMap<(String, TaskId), TaskRow>,
    schedules: HashMap<String, StoredSchedule>,
    activity_calls: HashMap<Uuid, ActivityCallRow>,
    activity_call_keys: HashMap<(String, WorkflowRunId, usize, String), Uuid>,
    scheduler_locks: HashSet<String>,
}

impl State {
    fn next_sequence(&mut self) -> u64 {
        self.sequence = self.sequence.saturating_add(1);
        self.sequence
    }
}

#[derive(Clone)]
struct QueueRow {
    dlq_name: Option<String>,
}

#[derive(Clone)]
struct TaskRow {
    id: TaskId,
    queue_name: String,
    input: serde_json::Value,
    state: TaskState,
    config: TaskConfig,
    attempt_count: i32,
    current_attempt: i32,
    run_at_ms: u64,
    lease_expires_at_ms: Option<u64>,
    created_at_ms: u64,
    created_seq: u64,
    completed: bool,
    last_error: Option<String>,
}

#[derive(Clone)]
struct ActivityCallRow {
    id: Uuid,
    queue_name: String,
    workflow_run_id: WorkflowRunId,
    step_index: usize,
    call_key: String,
    activity: String,
    input: serde_json::Value,
    output: Option<serde_json::Value>,
    error: Option<serde_json::Value>,
    state: CallState,
    attempt_count: i32,
    available_at_ms: u64,
    lease_expires_at_ms: Option<u64>,
    timeout: Span,
    created_seq: u64,
}

/// Memory transaction.
pub struct MemoryTransaction {
    backend: MemoryBackend,
    finished: bool,
}

/// Memory scheduler lease.
pub struct MemorySchedulerLease {
    backend: MemoryBackend,
    queue_name: String,
    released: bool,
}

impl Drop for MemorySchedulerLease {
    fn drop(&mut self) {
        if !self.released {
            if let Ok(mut state) = self.backend.state() {
                state.scheduler_locks.remove(&self.queue_name);
            }
        }
    }
}

impl SchedulerLease for MemorySchedulerLease {
    type Error = Error;

    async fn release(&mut self) -> Result<(), Self::Error> {
        let mut state = self.backend.state()?;
        state.scheduler_locks.remove(&self.queue_name);
        self.released = true;
        Ok(())
    }
}

impl Backend for MemoryBackend {
    type Error = Error;
    type Transaction<'a> = MemoryTransaction;
    type SchedulerLease<'a> = MemorySchedulerLease;

    async fn begin(&self) -> Result<Self::Transaction<'_>, Self::Error> {
        Ok(MemoryTransaction {
            backend: self.clone(),
            finished: false,
        })
    }

    async fn record_heartbeat(&self, task: &ClaimedTask) -> Result<(), Self::Error> {
        let mut state = self.state()?;
        let now = state.now_ms;
        if let Some(row) = state
            .tasks
            .get_mut(&(task.queue_name.clone(), task.id))
            .filter(|row| {
                row.state == TaskState::InProgress && row.current_attempt == task.attempt_number
            })
        {
            row.lease_expires_at_ms =
                Some(now.saturating_add(duration_ms(row.config.heartbeat())?));
        }
        Ok(())
    }

    async fn try_acquire_scheduler(
        &self,
        queue_name: &str,
    ) -> Result<Option<Self::SchedulerLease<'_>>, Self::Error> {
        let mut state = self.state()?;
        if state.scheduler_locks.contains(queue_name) {
            return Ok(None);
        }
        state.scheduler_locks.insert(queue_name.to_string());
        Ok(Some(MemorySchedulerLease {
            backend: self.clone(),
            queue_name: queue_name.to_string(),
            released: false,
        }))
    }

    async fn wait_for_task_change(
        &self,
        _queue_name: &str,
        timeout: Duration,
    ) -> Result<(), Self::Error> {
        tokio::select! {
            _ = self.task_notify.notified() => Ok(()),
            _ = tokio::time::sleep(timeout) => Ok(()),
        }
    }

    async fn wait_for_activity_change(
        &self,
        _queue_name: &str,
        timeout: Duration,
    ) -> Result<(), Self::Error> {
        tokio::select! {
            _ = self.activity_notify.notified() => Ok(()),
            _ = tokio::time::sleep(timeout) => Ok(()),
        }
    }
}

impl BackendTransaction for MemoryTransaction {
    type Error = Error;

    async fn commit(mut self) -> Result<(), Self::Error> {
        self.finished = true;
        Ok(())
    }

    async fn rollback(mut self) -> Result<(), Self::Error> {
        self.finished = true;
        Ok(())
    }

    async fn create_queue(
        &mut self,
        name: &str,
        dlq_name: Option<&str>,
    ) -> Result<(), Self::Error> {
        let mut state = self.backend.state()?;
        state.queues.entry(name.to_string()).or_insert(QueueRow {
            dlq_name: dlq_name.map(ToOwned::to_owned),
        });
        if let Some(dlq_name) = dlq_name {
            state
                .queues
                .entry(dlq_name.to_string())
                .or_insert(QueueRow { dlq_name: None });
            if let Some(queue) = state.queues.get_mut(name) {
                queue.dlq_name = Some(dlq_name.to_string());
            }
        }
        Ok(())
    }

    async fn enqueue_task(
        &mut self,
        queue_name: &str,
        task: EnqueueTask,
    ) -> Result<TaskId, Self::Error> {
        let mut state = self.backend.state()?;
        let id = TaskId::new();
        let delay_ms = duration_ms(task.config.delay().checked_add(task.delay)?)?;
        let row = TaskRow {
            id,
            queue_name: queue_name.to_string(),
            input: task.input,
            state: TaskState::Pending,
            config: task.config,
            attempt_count: 0,
            current_attempt: 0,
            run_at_ms: state.now_ms.saturating_add(delay_ms),
            lease_expires_at_ms: None,
            created_at_ms: state.now_ms,
            created_seq: state.next_sequence(),
            completed: false,
            last_error: None,
        };
        state.tasks.insert((queue_name.to_string(), id), row);
        self.backend.task_notify.notify_waiters();
        Ok(id)
    }

    async fn claim_task(&mut self, queue_name: &str) -> Result<Option<ClaimedTask>, Self::Error> {
        let mut state = self.backend.state()?;
        let now = state.now_ms;
        terminalize_stale_exhausted(&mut state, queue_name)?;

        let selected = state
            .tasks
            .iter()
            .filter(|((queue, _), row)| {
                queue == queue_name
                    && ((row.state == TaskState::Pending && row.run_at_ms <= now)
                        || (row.state == TaskState::InProgress
                            && row
                                .lease_expires_at_ms
                                .is_some_and(|expires| expires <= now)
                            && row.attempt_count < row.config.retry_policy().max_attempts))
            })
            .max_by(|(_, a), (_, b)| {
                a.config
                    .priority()
                    .cmp(&b.config.priority())
                    .then_with(|| b.run_at_ms.cmp(&a.run_at_ms))
                    .then_with(|| b.created_seq.cmp(&a.created_seq))
            })
            .map(|(key, _)| key.clone());

        let Some(key) = selected else {
            return Ok(None);
        };
        let row = state
            .tasks
            .get_mut(&key)
            .ok_or_else(|| Error::NotFound("task".to_string()))?;
        row.state = TaskState::InProgress;
        row.attempt_count += 1;
        row.current_attempt = row.attempt_count;
        row.lease_expires_at_ms = Some(now.saturating_add(duration_ms(row.config.heartbeat())?));
        Ok(Some(ClaimedTask {
            id: row.id,
            queue_name: row.queue_name.clone(),
            input: row.input.clone(),
            config: row.config.clone(),
            attempt_number: row.current_attempt,
        }))
    }

    async fn mark_task_succeeded(&mut self, task: &ClaimedTask) -> Result<(), Self::Error> {
        update_claimed_task(&self.backend, task, |row| {
            row.state = TaskState::Succeeded;
            row.completed = true;
            row.lease_expires_at_ms = None;
            Ok(())
        })
    }

    async fn mark_task_waiting(&mut self, task: &ClaimedTask) -> Result<(), Self::Error> {
        update_claimed_task(&self.backend, task, |row| {
            row.state = TaskState::Waiting;
            row.lease_expires_at_ms = None;
            Ok(())
        })
    }

    async fn record_task_failure(
        &mut self,
        task: &ClaimedTask,
        error: &task::Error,
    ) -> Result<(), Self::Error> {
        update_claimed_task(&self.backend, task, |row| {
            row.last_error = Some(error.to_string());
            Ok(())
        })
    }

    async fn retry_task(&mut self, task: &ClaimedTask, delay: Span) -> Result<(), Self::Error> {
        let mut state = self.backend.state()?;
        let now = state.now_ms;
        let row = claimed_task_row(&mut state, task)?;
        row.state = TaskState::Pending;
        row.run_at_ms = now.saturating_add(duration_ms(delay)?);
        row.lease_expires_at_ms = None;
        self.backend.task_notify.notify_waiters();
        Ok(())
    }

    async fn mark_task_failed(&mut self, task: &ClaimedTask) -> Result<(), Self::Error> {
        update_claimed_task(&self.backend, task, |row| {
            row.state = TaskState::Failed;
            row.completed = true;
            row.lease_expires_at_ms = None;
            Ok(())
        })
    }

    async fn move_task_to_dlq(
        &mut self,
        task_id: TaskId,
        from_queue: &str,
        dlq_name: &str,
    ) -> Result<(), Self::Error> {
        let mut state = self.backend.state()?;
        let key = (from_queue.to_string(), task_id);
        let mut row = state
            .tasks
            .remove(&key)
            .ok_or_else(|| Error::NotFound("task".to_string()))?;
        row.queue_name = dlq_name.to_string();
        state.tasks.insert((dlq_name.to_string(), task_id), row);
        Ok(())
    }

    async fn cancel_workflow(
        &mut self,
        queue_name: &str,
        workflow_run_id: WorkflowRunId,
    ) -> Result<bool, Self::Error> {
        let mut state = self.backend.state()?;
        let mut cancelled = false;
        for ((queue, _), row) in state.tasks.iter_mut() {
            if queue == queue_name
                && row.state == TaskState::Pending
                && workflow_run_id_from_input(&row.input) == Some(workflow_run_id)
            {
                row.state = TaskState::Cancelled;
                row.completed = true;
                cancelled = true;
            }
        }
        Ok(cancelled)
    }

    async fn schedule_task(
        &mut self,
        queue_name: &str,
        schedule: ZonedSchedule,
        input: serde_json::Value,
    ) -> Result<(), Self::Error> {
        let mut state = self.backend.state()?;
        state
            .schedules
            .insert(queue_name.to_string(), StoredSchedule { schedule, input });
        Ok(())
    }

    async fn unschedule_task(&mut self, queue_name: &str) -> Result<(), Self::Error> {
        let mut state = self.backend.state()?;
        state.schedules.remove(queue_name);
        Ok(())
    }

    async fn task_schedule(
        &mut self,
        queue_name: &str,
    ) -> Result<Option<StoredSchedule>, Self::Error> {
        let state = self.backend.state()?;
        Ok(state.schedules.get(queue_name).cloned())
    }

    async fn load_activity_calls(
        &mut self,
        queue_name: &str,
        workflow_run_id: WorkflowRunId,
        step_index: usize,
    ) -> Result<Vec<ActivityCallRecord>, Self::Error> {
        let state = self.backend.state()?;
        Ok(state
            .activity_calls
            .values()
            .filter(|call| {
                call.queue_name == queue_name
                    && call.workflow_run_id == workflow_run_id
                    && call.step_index == step_index
            })
            .map(|call| ActivityCallRecord {
                call_key: call.call_key.clone(),
                activity: call.activity.clone(),
                input: call.input.clone(),
                output: call.output.clone(),
                error: call.error.clone(),
                state: call.state,
            })
            .collect())
    }

    async fn insert_activity_call(
        &mut self,
        queue_name: &str,
        workflow_run_id: WorkflowRunId,
        step_index: usize,
        command: ActivityCallCommand,
    ) -> Result<(), Self::Error> {
        let mut state = self.backend.state()?;
        let key = (
            queue_name.to_string(),
            workflow_run_id,
            step_index,
            command.call_key.clone(),
        );
        if state.activity_call_keys.contains_key(&key) {
            return Ok(());
        }

        let id = Uuid::new_v4();
        let row = ActivityCallRow {
            id,
            queue_name: queue_name.to_string(),
            workflow_run_id,
            step_index,
            call_key: command.call_key,
            activity: command.activity,
            input: command.input,
            output: None,
            error: None,
            state: CallState::Pending,
            attempt_count: 0,
            available_at_ms: state.now_ms,
            lease_expires_at_ms: None,
            timeout: command.timeout,
            created_seq: state.next_sequence(),
        };
        state.activity_call_keys.insert(key, id);
        state.activity_calls.insert(id, row);
        self.backend.activity_notify.notify_waiters();
        Ok(())
    }

    async fn claim_activity_call(
        &mut self,
        queue_name: &str,
        activities: &[String],
    ) -> Result<Option<ClaimedActivityCall>, Self::Error> {
        let mut state = self.backend.state()?;
        let now = state.now_ms;
        let selected = state
            .activity_calls
            .iter()
            .filter(|(_, call)| {
                call.queue_name == queue_name
                    && activities.iter().any(|activity| activity == &call.activity)
                    && ((call.state == CallState::Pending && call.available_at_ms <= now)
                        || (call.state == CallState::InProgress
                            && call
                                .lease_expires_at_ms
                                .is_some_and(|expires| expires <= now)))
            })
            .min_by_key(|(_, call)| (call.available_at_ms, call.created_seq, call.id))
            .map(|(id, _)| *id);

        let Some(id) = selected else {
            return Ok(None);
        };

        let call = state
            .activity_calls
            .get_mut(&id)
            .ok_or_else(|| Error::NotFound("activity call".to_string()))?;
        call.state = CallState::InProgress;
        call.attempt_count += 1;
        call.lease_expires_at_ms = Some(
            now.saturating_add(duration_ms(call.timeout)?)
                .saturating_add(30_000),
        );

        Ok(Some(ClaimedActivityCall {
            id: call.id,
            queue_name: call.queue_name.clone(),
            workflow_run_id: call.workflow_run_id,
            step_index: call.step_index,
            call_key: call.call_key.clone(),
            activity: call.activity.clone(),
            input: call.input.clone(),
            attempt_count: call.attempt_count,
            attempt_number: call.attempt_count,
        }))
    }

    async fn complete_activity_call(
        &mut self,
        call: &ClaimedActivityCall,
        output: serde_json::Value,
    ) -> Result<(), Self::Error> {
        let mut state = self.backend.state()?;
        let row = claimed_activity_row(&mut state, call)?;
        row.state = CallState::Succeeded;
        row.output = Some(output);
        row.lease_expires_at_ms = None;
        Ok(())
    }

    async fn retry_activity_call(
        &mut self,
        call: &ClaimedActivityCall,
        error: activity::Error,
        delay: Span,
    ) -> Result<(), Self::Error> {
        let mut state = self.backend.state()?;
        let now = state.now_ms;
        let row = claimed_activity_row(&mut state, call)?;
        row.state = CallState::Pending;
        row.error = Some(serde_json::to_value(error)?);
        row.available_at_ms = now.saturating_add(duration_ms(delay)?);
        row.lease_expires_at_ms = None;
        self.backend.activity_notify.notify_waiters();
        Ok(())
    }

    async fn fail_activity_call(
        &mut self,
        call: &ClaimedActivityCall,
        error: activity::Error,
    ) -> Result<(), Self::Error> {
        let mut state = self.backend.state()?;
        let row = claimed_activity_row(&mut state, call)?;
        row.state = CallState::Failed;
        row.error = Some(serde_json::to_value(error)?);
        row.lease_expires_at_ms = None;
        Ok(())
    }

    async fn wake_workflow_task(
        &mut self,
        queue_name: &str,
        workflow_run_id: WorkflowRunId,
        step_index: usize,
    ) -> Result<(), Self::Error> {
        let mut state = self.backend.state()?;
        let now = state.now_ms;
        for ((queue, _), row) in state.tasks.iter_mut() {
            if queue == queue_name
                && row.state == TaskState::Waiting
                && workflow_run_id_from_input(&row.input) == Some(workflow_run_id)
                && row
                    .input
                    .get("step_index")
                    .and_then(serde_json::Value::as_u64)
                    == Some(step_index as u64)
            {
                row.state = TaskState::Pending;
                row.run_at_ms = now;
                row.lease_expires_at_ms = None;
            }
        }
        self.backend.task_notify.notify_waiters();
        Ok(())
    }

    async fn delete_expired(&mut self) -> Result<(), Self::Error> {
        let mut state = self.backend.state()?;
        let now = state.now_ms;
        state.tasks.retain(|_, row| {
            row.state == TaskState::InProgress
                || !row.completed
                || row
                    .created_at_ms
                    .checked_add(duration_ms(row.config.ttl()).unwrap_or(u64::MAX))
                    .is_some_and(|expires| expires >= now)
        });
        Ok(())
    }
}

fn duration_ms(span: Span) -> Result<u64, Error> {
    let duration: Duration = span.try_into()?;
    Ok(duration.as_millis().min(u64::MAX as u128) as u64)
}

fn terminalize_stale_exhausted(state: &mut State, queue_name: &str) -> Result<(), Error> {
    let now = state.now_ms;
    for ((queue, _), row) in state.tasks.iter_mut() {
        if queue == queue_name
            && row.state == TaskState::InProgress
            && row
                .lease_expires_at_ms
                .is_some_and(|expires| expires <= now)
            && row.attempt_count >= row.config.retry_policy().max_attempts
        {
            row.state = TaskState::Failed;
            row.completed = true;
            row.lease_expires_at_ms = None;
        }
    }
    Ok(())
}

fn update_claimed_task(
    backend: &MemoryBackend,
    task: &ClaimedTask,
    f: impl FnOnce(&mut TaskRow) -> Result<(), Error>,
) -> Result<(), Error> {
    let mut state = backend.state()?;
    let row = claimed_task_row(&mut state, task)?;
    f(row)
}

fn claimed_task_row<'a>(
    state: &'a mut State,
    task: &ClaimedTask,
) -> Result<&'a mut TaskRow, Error> {
    let row = state
        .tasks
        .get_mut(&(task.queue_name.clone(), task.id))
        .ok_or_else(|| Error::NotFound("task".to_string()))?;
    if row.state != TaskState::InProgress || row.current_attempt != task.attempt_number {
        return Err(Error::NotFound("claimed task".to_string()));
    }
    Ok(row)
}

fn claimed_activity_row<'a>(
    state: &'a mut State,
    call: &ClaimedActivityCall,
) -> Result<&'a mut ActivityCallRow, Error> {
    let row = state
        .activity_calls
        .get_mut(&call.id)
        .ok_or_else(|| Error::NotFound("activity call".to_string()))?;
    if row.state != CallState::InProgress || row.attempt_count != call.attempt_count {
        return Err(Error::NotFound("claimed activity call".to_string()));
    }
    Ok(row)
}

fn workflow_run_id_from_input(input: &serde_json::Value) -> Option<WorkflowRunId> {
    input
        .get("workflow_run_id")
        .and_then(serde_json::Value::as_str)
        .and_then(|id| Uuid::parse_str(id).ok())
        .map(WorkflowRunId::from_uuid)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use serde::{Deserialize, Serialize};
    use tokio::sync::Mutex as TokioMutex;
    use underway_core::{BackendTransaction, Task, Transition, Workflow};

    use super::*;

    #[derive(Debug, Deserialize, Serialize)]
    struct Input {
        message: String,
    }

    struct EchoTask;

    impl Task<MemoryBackend> for EchoTask {
        type Input = Input;
        type Output = ();

        async fn execute(
            &self,
            _tx: &mut <MemoryBackend as Backend>::Transaction<'_>,
            input: Self::Input,
        ) -> task::Result<Self::Output> {
            if input.message == "fail" {
                Err(task::Error::Retryable("failed".to_string()))
            } else {
                Ok(())
            }
        }
    }

    #[tokio::test]
    async fn queue_processes_task() {
        let backend = MemoryBackend::new();
        let queue = underway_core::Queue::builder()
            .name("queue_processes_task")
            .backend(backend.clone())
            .build()
            .await
            .unwrap();
        let task = EchoTask;
        queue
            .enqueue(
                &task,
                &Input {
                    message: "ok".to_string(),
                },
            )
            .await
            .unwrap();
        let worker = underway_core::Worker::new(Arc::new(queue), task);
        assert!(worker.process_next_task().await.unwrap().is_some());
    }

    #[tokio::test]
    async fn workflow_runs_steps() {
        #[derive(Deserialize, Serialize)]
        struct First;
        #[derive(Deserialize, Serialize)]
        struct Second(String);

        let backend = MemoryBackend::new();
        let seen = Arc::new(TokioMutex::new(Vec::new()));
        let workflow = Workflow::builder()
            .state(seen.clone())
            .step(|_cx, _input: First| async move { Transition::next(Second("done".to_string())) })
            .step(|cx, Second(value)| async move {
                cx.state.lock().await.push(value);
                Transition::complete()
            })
            .name("workflow_runs_steps")
            .backend(backend.clone())
            .build()
            .await
            .unwrap();

        workflow.enqueue(&First).await.unwrap();
        let worker = workflow.runtime().worker();
        assert!(worker.process_next_task().await.unwrap().is_some());
        assert!(worker.process_next_task().await.unwrap().is_some());
        assert_eq!(seen.lock().await.as_slice(), ["done"]);

        let tx = backend.begin().await.unwrap();
        tx.commit().await.unwrap();
    }

    #[tokio::test]
    async fn satisfies_queue_ordering_and_retry_contract() {
        underway_core::contracts::queue_ordering_and_retry(MemoryBackend::new())
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn satisfies_activity_call_idempotency_contract() {
        underway_core::contracts::activity_call_idempotency(MemoryBackend::new())
            .await
            .unwrap();
    }
}
