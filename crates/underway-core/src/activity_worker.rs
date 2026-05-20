use super::*;

trait ActivityHandler: Send + Sync {
    fn name(&self) -> &'static str;
    fn retry_policy(&self) -> task::RetryPolicy;
    fn timeout(&self) -> Span;
    fn execute_json<'a>(
        &'a self,
        input: serde_json::Value,
    ) -> Pin<Box<dyn Future<Output = activity::Result<serde_json::Value>> + Send + 'a>>;
}

struct RegisteredActivity<A: activity::Activity> {
    inner: A,
}

impl<A: activity::Activity> ActivityHandler for RegisteredActivity<A> {
    fn name(&self) -> &'static str {
        A::NAME
    }

    fn retry_policy(&self) -> task::RetryPolicy {
        self.inner.retry_policy()
    }

    fn timeout(&self) -> Span {
        self.inner.timeout()
    }

    fn execute_json<'a>(
        &'a self,
        input: serde_json::Value,
    ) -> Pin<Box<dyn Future<Output = activity::Result<serde_json::Value>> + Send + 'a>> {
        Box::pin(async move {
            let input: A::Input = serde_json::from_value(input).map_err(|err| {
                activity::Error::fatal(
                    "deserialize_input",
                    format!(
                        "Failed to deserialize input for activity `{}`: {err}",
                        A::NAME
                    ),
                )
            })?;
            let output = self.inner.execute(input).await?;
            serde_json::to_value(output).map_err(|err| {
                activity::Error::fatal(
                    "serialize_output",
                    format!(
                        "Failed to serialize output for activity `{}`: {err}",
                        A::NAME
                    ),
                )
            })
        })
    }
}

#[derive(Clone, Default)]
pub(crate) struct ActivityRegistry {
    handlers: HashMap<String, Arc<dyn ActivityHandler>>,
}

impl ActivityRegistry {
    pub(crate) fn register<A: activity::Activity>(&mut self, activity: A) {
        self.handlers.insert(
            A::NAME.to_string(),
            Arc::new(RegisteredActivity { inner: activity }),
        );
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.handlers.is_empty()
    }

    pub(crate) fn call_policy(&self, activity: &str) -> Option<(task::RetryPolicy, Span)> {
        self.handlers
            .get(activity)
            .map(|handler| (handler.retry_policy(), handler.timeout()))
    }
}

#[derive(Clone)]
pub(crate) struct ActivityWorker<B: backend::Backend> {
    backend: B,
    queue_name: String,
    registry: ActivityRegistry,
    shutdown_token: CancellationToken,
}

impl<B: backend::Backend> ActivityWorker<B> {
    pub(crate) fn with_registry(
        backend: B,
        queue_name: String,
        registry: ActivityRegistry,
    ) -> Self {
        Self {
            backend,
            queue_name,
            registry,
            shutdown_token: CancellationToken::new(),
        }
    }

    pub(crate) fn set_shutdown_token(&mut self, shutdown_token: CancellationToken) {
        self.shutdown_token = shutdown_token;
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.registry.is_empty()
    }

    pub(crate) async fn run(&self) -> Result {
        self.run_every(1.second()).await
    }

    pub(crate) async fn run_every(&self, period: Span) -> Result {
        let period: StdDuration = period.try_into()?;
        self.drain_pending_calls().await?;

        loop {
            tokio::select! {
                _ = self.shutdown_token.cancelled() => return Ok(()),
                waited = self.backend.wait_for_activity_change(&self.queue_name, period) => {
                    waited.map_err(Error::backend)?;
                    self.drain_pending_calls().await?;
                }
            }
        }
    }

    async fn drain_pending_calls(&self) -> Result {
        while !self.shutdown_token.is_cancelled() && self.process_next_call().await?.is_some() {}
        Ok(())
    }

    async fn process_next_call(&self) -> Result<Option<Uuid>> {
        let activities = self.activity_claim_specs();
        if activities.is_empty() {
            return Ok(None);
        }

        let mut tx = self.backend.begin().await.map_err(Error::backend)?;
        let call = tx
            .claim_activity_call(&self.queue_name, &activities)
            .await
            .map_err(Error::backend)?;
        tx.commit().await.map_err(Error::backend)?;

        let Some(call) = call else {
            return Ok(None);
        };

        let Some(handler) = self.registry.handlers.get(&call.activity).cloned() else {
            let err = activity::Error::fatal(
                "activity_not_registered",
                format!("No activity handler registered for `{}`", call.activity),
            );
            let mut tx = self.backend.begin().await.map_err(Error::backend)?;
            let applied = tx
                .fail_activity_call(&call, err)
                .await
                .map_err(Error::backend)?;
            if applied.applied() {
                tx.wake_workflow_task(&call.queue_name, call.workflow_run_id, call.step_index)
                    .await
                    .map_err(Error::backend)?;
                tx.commit().await.map_err(Error::backend)?;
            } else {
                tx.rollback().await.map_err(Error::backend)?;
            }
            return Ok(Some(call.id));
        };

        let timeout: StdDuration = handler.timeout().try_into()?;
        let result = tokio::time::timeout(timeout, handler.execute_json(call.input.clone())).await;

        let mut tx = self.backend.begin().await.map_err(Error::backend)?;
        let applied = match result {
            Ok(Ok(output)) => {
                let applied = tx
                    .complete_activity_call(&call, output)
                    .await
                    .map_err(Error::backend)?;
                if applied.applied() {
                    tx.wake_workflow_task(&call.queue_name, call.workflow_run_id, call.step_index)
                        .await
                        .map_err(Error::backend)?;
                }
                applied
            }
            Ok(Err(err)) => {
                self.handle_activity_error(&mut tx, &call, handler.retry_policy(), err)
                    .await?
            }
            Err(_) => {
                let err = activity::Error::retryable(
                    "activity_timeout",
                    format!("Activity `{}` timed out", handler.name()),
                );
                self.handle_activity_error(&mut tx, &call, handler.retry_policy(), err)
                    .await?
            }
        };
        if applied.applied() {
            tx.commit().await.map_err(Error::backend)?;
        } else {
            tx.rollback().await.map_err(Error::backend)?;
        }

        Ok(Some(call.id))
    }

    async fn handle_activity_error(
        &self,
        tx: &mut B::Transaction<'_>,
        call: &backend::ClaimedActivityCall,
        retry_policy: task::RetryPolicy,
        err: activity::Error,
    ) -> Result<backend::ApplyResult> {
        if err.retryable && call.attempt_count < retry_policy.max_attempts {
            tx.retry_activity_call(call, err, retry_policy.calculate_delay(call.attempt_count))
                .await
                .map_err(Error::backend)
        } else {
            let applied = tx
                .fail_activity_call(call, err)
                .await
                .map_err(Error::backend)?;
            if applied.applied() {
                tx.wake_workflow_task(&call.queue_name, call.workflow_run_id, call.step_index)
                    .await
                    .map_err(Error::backend)?;
            }
            Ok(applied)
        }
    }

    fn activity_claim_specs(&self) -> Vec<String> {
        let mut activities = self.registry.handlers.keys().cloned().collect::<Vec<_>>();
        activities.sort();
        activities
    }
}
