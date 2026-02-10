use std::{future::Future, marker::PhantomData, pin::Pin, sync::Arc};

use jiff::{Span, ToSpan};
use serde::{de::DeserializeOwned, Deserialize, Serialize};

use super::{context::ContextParts, Context};
use crate::task::{Error as TaskError, Result as TaskResult, RetryPolicy};

pub(super) struct StepConfig<S> {
    pub(super) executor: Box<dyn StepExecutor<S>>,
    pub(super) task_config: StepTaskConfig,
}

impl<S> StepConfig<S> {
    pub(super) fn new<I, O, A, F, Fut>(func: F) -> Self
    where
        I: DeserializeOwned + Serialize + Send + Sync + 'static,
        O: Serialize + Send + Sync + 'static,
        S: Send + Sync + 'static,
        A: 'static,
        F: Fn(Context<S, A>, I) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = TaskResult<To<O>>> + Send + 'static,
    {
        let step_fn = StepFn::new(move |cx, input| Box::pin(func(cx, input)));
        Self {
            executor: Box::new(step_fn),
            task_config: StepTaskConfig::default(),
        }
    }
}

#[derive(Clone)]
pub(super) struct StepTaskConfig {
    pub(super) retry_policy: RetryPolicy,
    pub(super) timeout: Span,
    pub(super) ttl: Span,
    pub(super) delay: Span,
    pub(super) heartbeat: Span,
    pub(super) concurrency_key: Option<String>,
    pub(super) priority: i32,
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

type StepFnMarker<I, O, S, A> = fn() -> (I, O, S, A);

struct StepFn<I, O, S, A, F>
where
    F: Fn(Context<S, A>, I) -> Pin<Box<dyn Future<Output = TaskResult<To<O>>> + Send>>
        + Send
        + Sync
        + 'static,
{
    func: Arc<F>,
    _marker: PhantomData<StepFnMarker<I, O, S, A>>,
}

impl<I, O, S, A, F> StepFn<I, O, S, A, F>
where
    F: Fn(Context<S, A>, I) -> Pin<Box<dyn Future<Output = TaskResult<To<O>>> + Send>>
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

pub(super) trait StepExecutor<S>: Send + Sync {
    fn execute_step(
        &self,
        cx: ContextParts<S>,
        input: serde_json::Value,
    ) -> Pin<Box<dyn Future<Output = StepResult> + Send>>;
}

impl<I, O, S, A, F> StepExecutor<S> for StepFn<I, O, S, A, F>
where
    I: DeserializeOwned + Serialize + Send + Sync + 'static,
    O: Serialize + Send + Sync + 'static,
    S: Send + Sync + 'static,
    F: Fn(Context<S, A>, I) -> Pin<Box<dyn Future<Output = TaskResult<To<O>>> + Send>>
        + Send
        + Sync
        + 'static,
{
    fn execute_step(
        &self,
        cx: ContextParts<S>,
        input: serde_json::Value,
    ) -> Pin<Box<dyn Future<Output = StepResult> + Send>> {
        let deserialized_input: I = match serde_json::from_value(input) {
            Ok(val) => val,
            Err(err) => return Box::pin(async move { Err(TaskError::Fatal(err.to_string())) }),
        };
        let cx: Context<S, A> = Context::from_parts(cx);
        let fut = (self.func)(cx, deserialized_input);

        Box::pin(async move {
            match fut.await {
                Ok(To::Next(output)) => {
                    let serialized_output = serde_json::to_value(output)
                        .map_err(|err| TaskError::Fatal(err.to_string()))?;
                    Ok(Some((serialized_output, Span::new())))
                }

                Ok(To::Delay {
                    next: output,
                    delay,
                }) => {
                    let serialized_output = serde_json::to_value(output)
                        .map_err(|err| TaskError::Fatal(err.to_string()))?;
                    Ok(Some((serialized_output, delay)))
                }

                Ok(To::Done) => Ok(None),

                Err(err) => Err(err),
            }
        })
    }
}
