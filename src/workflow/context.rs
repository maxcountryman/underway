use std::{
    collections::HashMap,
    marker::PhantomData,
    sync::{Arc, Mutex},
};

use super::WorkflowId;
use crate::{
    activity::{self, registration::Contains, CallState},
    task::{Error as TaskError, Result as TaskResult},
};

/// Context passed in to each step.
pub struct Context<S, Set = activity::registration::Nil> {
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

    /// This `WorkflowId`.
    pub workflow_id: WorkflowId,

    /// Queue name.
    ///
    /// Name of the queue the current step is currently running on.
    pub queue_name: String,

    pub(super) activity_call_buffer: Arc<Mutex<ActivityCallBuffer>>,
    pub(super) call_sequence_state: Arc<Mutex<CallSequenceState>>,
    pub(super) _activity_set: PhantomData<fn() -> Set>,
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
    /// Emits a durable fire-and-forget activity call.
    ///
    /// The call is persisted as part of the current step transaction boundary.
    /// If the step fails and retries before that boundary is reached, the emit
    /// intent is not recorded.
    ///
    /// `key` should be deterministic for this step execution.
    pub async fn emit<A, Idx>(&self, key: impl Into<String>, input: &A::Input) -> TaskResult<()>
    where
        A: activity::Activity,
        Set: Contains<A, Idx>,
    {
        let key = key.into();
        let input = serde_json::to_value(input).map_err(|err| TaskError::Fatal(err.to_string()))?;

        self.register_activity_call(&key, A::NAME, &input)?;

        Ok(())
    }

    /// Calls an activity and waits for its durable result.
    ///
    /// If the call has not completed yet this returns
    /// [`task::Error::Suspended`](crate::task::Error::Suspended).
    ///
    /// In normal step code you should propagate that with `?` instead of
    /// looping manually. The worker marks the task as waiting and re-runs the
    /// step after the activity completes, at which point this call returns the
    /// durable result for the same call key.
    ///
    /// This method takes `&mut self`, which enforces sequential call issuance
    /// within a step at compile time.
    ///
    /// For v1 semantics, calls are sequential: only one unresolved call is
    /// permitted at a time in a single step execution.
    pub async fn call<A, Idx>(
        &mut self,
        key: impl Into<String>,
        input: &A::Input,
    ) -> TaskResult<A::Output>
    where
        A: activity::Activity,
        Set: Contains<A, Idx>,
    {
        let key = key.into();
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
                    "Waiting on activity call `{activity}` with key `{key}",
                    activity = A::NAME
                )))
            }
        }
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
