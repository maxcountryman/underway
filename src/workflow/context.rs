use std::{
    collections::HashMap,
    marker::PhantomData,
    sync::{Arc, Mutex},
};

use super::{
    registration::{Contains, NoActivities},
    WorkflowRunId,
};
use crate::{
    activity::{self, Activity, CallState},
    task::{Error as TaskError, Result as TaskResult},
};

/// Context passed in to each step.
pub struct Context<S, Set = NoActivities> {
    /// Shared step state.
    ///
    /// This value is set via
    /// [`workflow::Builder::state`](crate::workflow::Builder::state).
    ///
    /// **Note:** State is not persisted and therefore should not be
    /// relied on when durability is needed.
    pub state: S,

    /// Current index of the step being executed zero-based.
    ///
    /// In multi-step workflow definitions, this points to the current step the
    /// workflow is processing currently.
    pub step_index: usize,

    /// Total steps count.
    ///
    /// The number of steps in this workflow definition.
    pub step_count: usize,

    /// This `WorkflowRunId`.
    pub workflow_run_id: WorkflowRunId,

    /// Queue name.
    ///
    /// Name of the queue the current step is currently running on.
    pub queue_name: String,

    pub(super) activity_call_buffer: Arc<Mutex<ActivityCallBuffer>>,
    pub(super) call_sequence_state: Arc<Mutex<CallSequenceState>>,
    pub(super) _activity_set: PhantomData<fn() -> Set>,
}

pub(super) struct ContextParts<S> {
    pub(super) state: S,
    pub(super) step_index: usize,
    pub(super) step_count: usize,
    pub(super) workflow_run_id: WorkflowRunId,
    pub(super) queue_name: String,
    pub(super) activity_call_buffer: Arc<Mutex<ActivityCallBuffer>>,
    pub(super) call_sequence_state: Arc<Mutex<CallSequenceState>>,
}

#[derive(Clone, sqlx::FromRow)]
pub(super) struct ActivityCallRecord {
    pub(super) call_key: String,
    pub(super) activity: String,
    pub(super) input: serde_json::Value,
    pub(super) output: Option<serde_json::Value>,
    pub(super) error: Option<serde_json::Value>,
    pub(super) state: CallState,
}

#[derive(Clone)]
pub(super) struct ActivityCallCommand {
    pub(super) call_key: String,
    pub(super) activity: String,
    pub(super) input: serde_json::Value,
}

#[derive(Default)]
pub(super) struct ActivityCallBuffer {
    records: HashMap<String, ActivityCallRecord>,
    commands: Vec<ActivityCallCommand>,
}

#[derive(Default)]
pub(super) struct CallSequenceState {
    unresolved_call_key: Option<String>,
    next_activity_call_index: usize,
}

impl ActivityCallBuffer {
    pub(super) fn new(records: Vec<ActivityCallRecord>) -> Self {
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
    ) -> TaskResult<ActivityCallRecord> {
        if let Some(record) = self.records.get(key) {
            if record.activity != activity {
                return Err(TaskError::Fatal(format!(
                    "Non-deterministic activity call detected: key `{key}` was previously bound \
                     to `{previous}` but now `{current}`.",
                    previous = record.activity,
                    current = activity,
                )));
            }

            if record.input != *input {
                return Err(TaskError::Fatal(format!(
                    "Non-deterministic activity call input detected for key `{key}`."
                )));
            }

            return Ok(record.clone());
        }

        let record = ActivityCallRecord {
            call_key: key.to_string(),
            activity: activity.to_string(),
            input: input.clone(),
            output: None,
            error: None,
            state: CallState::Pending,
        };

        self.records.insert(key.to_string(), record.clone());
        self.commands.push(ActivityCallCommand {
            call_key: key.to_string(),
            activity: activity.to_string(),
            input: input.clone(),
        });

        Ok(record)
    }

    pub(super) fn drain_commands(&mut self) -> Vec<ActivityCallCommand> {
        std::mem::take(&mut self.commands)
    }
}

impl<S, Set> Context<S, Set> {
    pub(super) fn from_parts(parts: ContextParts<S>) -> Self {
        let ContextParts {
            state,
            step_index,
            step_count,
            workflow_run_id,
            queue_name,
            activity_call_buffer,
            call_sequence_state,
        } = parts;

        Self {
            state,
            step_index,
            step_count,
            workflow_run_id,
            queue_name,
            activity_call_buffer,
            call_sequence_state,
            _activity_set: PhantomData,
        }
    }

    pub(crate) async fn emit_indexed<A, Idx>(&mut self, input: &A::Input) -> TaskResult<()>
    where
        A: Activity,
        Set: Contains<A, Idx>,
    {
        let key = self.next_activity_call_key()?;
        let input = serde_json::to_value(input).map_err(|err| TaskError::Fatal(err.to_string()))?;

        self.register_activity_call(&key, A::NAME, &input)?;

        Ok(())
    }

    pub(crate) async fn call_indexed<A, Idx>(&mut self, input: &A::Input) -> TaskResult<A::Output>
    where
        A: Activity,
        Set: Contains<A, Idx>,
    {
        let key = self.next_activity_call_key()?;
        let input = serde_json::to_value(input).map_err(|err| TaskError::Fatal(err.to_string()))?;

        let record = self.register_activity_call(&key, A::NAME, &input)?;

        match record.state {
            CallState::Succeeded => {
                let output = record.output.ok_or_else(|| {
                    TaskError::Fatal(format!(
                        "Missing output for activity call `{activity}` with key `{key}`.",
                        activity = A::NAME
                    ))
                })?;

                serde_json::from_value(output).map_err(|err| TaskError::Fatal(err.to_string()))
            }

            CallState::Failed => {
                let error = record
                    .error
                    .and_then(|value| serde_json::from_value::<activity::Error>(value).ok())
                    .unwrap_or_else(|| {
                        activity::Error::fatal(
                            "activity_call_failed",
                            format!(
                                "Activity call `{activity}` with key `{key}` failed without an \
                                 error envelope.",
                                activity = A::NAME
                            ),
                        )
                    });

                if error.retryable {
                    Err(TaskError::Retryable(error.to_string()))
                } else {
                    Err(TaskError::Fatal(error.to_string()))
                }
            }

            CallState::Pending | CallState::InProgress => {
                self.register_call_sequence_wait(&key)?;

                Err(TaskError::Suspended(format!(
                    "Waiting on activity call `{activity}` with key `{key}`.",
                    activity = A::NAME
                )))
            }
        }
    }

    fn next_activity_call_key(&self) -> TaskResult<String> {
        let mut call_sequence_state = self
            .call_sequence_state
            .lock()
            .map_err(|_| TaskError::Fatal("Call sequence state lock was poisoned.".to_string()))?;

        let operation_index = call_sequence_state.next_activity_call_index;
        call_sequence_state.next_activity_call_index = call_sequence_state
            .next_activity_call_index
            .checked_add(1)
            .ok_or_else(|| TaskError::Fatal("Activity operation index overflowed.".to_string()))?;

        Ok(format!(
            "activity:{workflow_run_id}:{step_index}:{operation_index}",
            workflow_run_id = self.workflow_run_id.as_hyphenated(),
            step_index = self.step_index,
        ))
    }

    fn register_activity_call(
        &self,
        key: &str,
        activity: &str,
        input: &serde_json::Value,
    ) -> TaskResult<ActivityCallRecord> {
        let mut activity_call_buffer = self
            .activity_call_buffer
            .lock()
            .map_err(|_| TaskError::Fatal("Activity call buffer lock was poisoned.".to_string()))?;

        activity_call_buffer.register_call(key, activity, input)
    }

    fn register_call_sequence_wait(&self, key: &str) -> TaskResult<()> {
        let mut call_sequence_state = self
            .call_sequence_state
            .lock()
            .map_err(|_| TaskError::Fatal("Call sequence state lock was poisoned.".to_string()))?;

        if let Some(existing) = &call_sequence_state.unresolved_call_key {
            if existing != key {
                return Err(TaskError::Fatal(
                    "Multiple unresolved `call` operations in a single step execution are not \
                     supported. Await each call result before issuing another call."
                        .to_string(),
                ));
            }
        } else {
            call_sequence_state.unresolved_call_key = Some(key.to_string());
        }

        Ok(())
    }
}

/// Activity invocation helpers implemented on activity contracts.
///
/// This trait provides ergonomic invocation methods while preserving
/// compile-time checks that the activity was declared on the workflow builder.
#[allow(async_fn_in_trait)]
pub trait InvokeActivity: Activity + Sized {
    /// Emits a durable fire-and-forget activity call.
    async fn emit<S, Set, Idx>(cx: &mut Context<S, Set>, input: &Self::Input) -> TaskResult<()>
    where
        Set: Contains<Self, Idx>;

    /// Calls an activity and waits for its durable result.
    async fn call<S, Set, Idx>(
        cx: &mut Context<S, Set>,
        input: &Self::Input,
    ) -> TaskResult<Self::Output>
    where
        Set: Contains<Self, Idx>;
}

impl<A> InvokeActivity for A
where
    A: Activity,
{
    async fn emit<S, Set, Idx>(cx: &mut Context<S, Set>, input: &Self::Input) -> TaskResult<()>
    where
        Set: Contains<Self, Idx>,
    {
        cx.emit_indexed::<Self, Idx>(input).await
    }

    async fn call<S, Set, Idx>(
        cx: &mut Context<S, Set>,
        input: &Self::Input,
    ) -> TaskResult<Self::Output>
    where
        Set: Contains<Self, Idx>,
    {
        cx.call_indexed::<Self, Idx>(input).await
    }
}
