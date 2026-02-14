use std::{collections::HashMap, future::Future, pin::Pin, sync::Arc, time::Duration};

use jiff::{Span, ToSpan};
use serde_json::Value;
use sqlx::{PgPool, Postgres, Transaction};
use tokio_util::sync::CancellationToken;
use tracing::instrument;
use uuid::Uuid;

use crate::{
    activity::{self, Activity},
    queue::connect_listener_with_retry,
    task::RetryPolicy,
};

pub(crate) type Result<T = ()> = std::result::Result<T, Error>;

const IN_PROGRESS_RECLAIM_GRACE_MS: i64 = 30_000;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Database(#[from] sqlx::Error),

    #[error(transparent)]
    Queue(#[from] crate::queue::Error),

    #[error(transparent)]
    Json(#[from] serde_json::Error),

    #[error(transparent)]
    Jiff(#[from] jiff::Error),
}

trait ActivityHandler: Send + Sync {
    fn name(&self) -> &'static str;
    fn retry_policy(&self) -> RetryPolicy;
    fn timeout(&self) -> Span;
    fn execute_json<'a>(
        &'a self,
        input: Value,
    ) -> Pin<Box<dyn Future<Output = activity::Result<Value>> + Send + 'a>>;
}

struct RegisteredActivity<A: Activity> {
    inner: A,
}

impl<A: Activity> ActivityHandler for RegisteredActivity<A> {
    fn name(&self) -> &'static str {
        A::NAME
    }

    fn retry_policy(&self) -> RetryPolicy {
        self.inner.retry_policy()
    }

    fn timeout(&self) -> Span {
        self.inner.timeout()
    }

    fn execute_json<'a>(
        &'a self,
        input: Value,
    ) -> Pin<Box<dyn Future<Output = activity::Result<Value>> + Send + 'a>> {
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
    pub(crate) fn register<A: Activity>(&mut self, activity: A) {
        self.handlers.insert(
            A::NAME.to_string(),
            Arc::new(RegisteredActivity { inner: activity }),
        );
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.handlers.is_empty()
    }

    pub(crate) fn call_policy(&self, activity: &str) -> Option<(RetryPolicy, Span)> {
        self.handlers
            .get(activity)
            .map(|handler| (handler.retry_policy(), handler.timeout()))
    }
}

#[derive(Clone)]
pub(crate) struct ActivityWorker {
    pool: PgPool,
    queue_name: String,
    registry: ActivityRegistry,
    shutdown_token: CancellationToken,
}

#[derive(Debug, sqlx::FromRow)]
struct ClaimedCall {
    id: Uuid,
    task_queue_name: String,
    workflow_run_id: Uuid,
    step_index: i32,
    call_key: String,
    activity: String,
    input: Value,
    attempt_count: i32,
    attempt_number: i32,
}

impl ActivityWorker {
    pub(crate) fn with_registry(
        pool: PgPool,
        queue_name: String,
        registry: ActivityRegistry,
    ) -> Self {
        Self {
            pool,
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

    #[instrument(skip(self), err)]
    pub(crate) async fn run_every(&self, period: Span) -> Result {
        let mut polling_interval = tokio::time::interval(period.try_into()?);

        'reconnect: loop {
            let mut activity_change_listener = connect_listener_with_retry(
                &self.pool,
                &["activity_call_change"],
                &RetryPolicy::default(),
            )
            .await?;

            tracing::info!("Activity call listener connected successfully");

            // Drain immediately after (re)connect in case notifications were
            // missed while disconnected.
            self.drain_pending_calls().await?;

            loop {
                tokio::select! {
                    _ = self.shutdown_token.cancelled() => {
                        return Ok(());
                    }

                    notify_activity_call_change = activity_change_listener.recv() => {
                        match notify_activity_call_change {
                            Ok(_) => {
                                self.drain_pending_calls().await?;
                            }
                            Err(err) => {
                                tracing::warn!(%err, "Activity call listener connection lost, reconnecting");
                                continue 'reconnect;
                            }
                        }
                    }

                    _ = polling_interval.tick() => {
                        self.drain_pending_calls().await?;
                    }
                }
            }
        }
    }

    async fn drain_pending_calls(&self) -> Result {
        while self.process_next_call().await?.is_some() {
            continue;
        }

        Ok(())
    }

    async fn process_next_call(&self) -> Result<Option<Uuid>> {
        let activities = self.activity_claim_specs();

        let mut tx = self.pool.begin().await?;
        let call = self.claim_next_call(&mut tx, &activities).await?;
        tx.commit().await?;

        let Some(call) = call else {
            return Ok(None);
        };

        tracing::debug!(
            call.id = %call.id,
            call.activity = %call.activity,
            call.key = %call.call_key,
            "Processing activity call"
        );

        let Some(handler) = self.registry.handlers.get(&call.activity).cloned() else {
            let err = activity::Error::fatal(
                "activity_not_registered",
                format!("No activity handler registered for `{}`", call.activity),
            );
            let mut tx = self.pool.begin().await?;
            self.mark_failed_terminal(&mut tx, &call, err).await?;
            tx.commit().await?;
            return Ok(Some(call.id));
        };

        let timeout: Duration = handler.timeout().try_into()?;
        let result = tokio::time::timeout(timeout, handler.execute_json(call.input.clone())).await;

        match result {
            Ok(Ok(output)) => {
                self.mark_succeeded(&call, output).await?;
            }

            Ok(Err(err)) => {
                self.mark_failed(&call, handler.retry_policy(), err).await?;
            }

            Err(_) => {
                let err = activity::Error::retryable(
                    "activity_timeout",
                    format!("Activity `{}` timed out", handler.name()),
                );
                self.mark_failed(&call, handler.retry_policy(), err).await?;
            }
        }

        Ok(Some(call.id))
    }

    async fn claim_next_call(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        activities: &[String],
    ) -> Result<Option<ClaimedCall>> {
        let call = sqlx::query_as!(
            ClaimedCall,
            r#"
            with
            next_call as (
                select c.id
                from underway.activity_call c
                where c.task_queue_name = $1
                  and c.activity = any($2::text[])
                  and (
                    (
                        c.state = 'pending'::underway.activity_call_state
                        and c.available_at <= now()
                    )
                    or (
                        c.state = 'in_progress'::underway.activity_call_state
                        and c.lease_expires_at is not null
                        and c.lease_expires_at <= now()
                    )
                  )
                order by c.available_at, c.created_at, c.id
                limit 1
                for update skip locked
            ),
            claimed as (
                update underway.activity_call c
                set state = 'in_progress'::underway.activity_call_state,
                    attempt_count = c.attempt_count + 1,
                    updated_at = now(),
                    started_at = now(),
                    lease_expires_at =
                        now() + ((c.timeout_ms::bigint + $3) * interval '1 millisecond')
                from next_call
                where c.id = next_call.id
                returning
                    c.id,
                    c.task_queue_name,
                    c.workflow_run_id,
                    c.step_index,
                    c.call_key,
                    c.activity,
                    c.input,
                    c.attempt_count
            ),
            attempt as (
                insert into underway.activity_call_attempt (
                    activity_call_id,
                    attempt_number,
                    state
                )
                select
                    claimed.id,
                    claimed.attempt_count,
                    'in_progress'::underway.activity_call_state
                from claimed
                on conflict (activity_call_id, attempt_number)
                do update
                  set state = 'in_progress'::underway.activity_call_state,
                      error = null,
                      started_at = now(),
                      updated_at = now(),
                      completed_at = null
                returning
                    activity_call_id
            )
            select
                claimed.id,
                claimed.task_queue_name,
                claimed.workflow_run_id,
                claimed.step_index,
                claimed.call_key,
                claimed.activity,
                claimed.input,
                claimed.attempt_count,
                claimed.attempt_count as "attempt_number!"
            from claimed
            inner join attempt
              on attempt.activity_call_id = claimed.id
            "#,
            self.queue_name,
            activities as _,
            IN_PROGRESS_RECLAIM_GRACE_MS,
        )
        .fetch_optional(&mut **tx)
        .await?;

        Ok(call)
    }

    fn activity_claim_specs(&self) -> Vec<String> {
        let mut activities = self.registry.handlers.keys().cloned().collect::<Vec<_>>();

        activities.sort();

        activities
    }

    async fn mark_succeeded(&self, call: &ClaimedCall, output: Value) -> Result {
        let mut tx = self.pool.begin().await?;

        sqlx::query!(
            r#"
            update underway.activity_call
            set state = 'succeeded'::underway.activity_call_state,
                output = $2,
                attempt_count = $3,
                updated_at = now(),
                completed_at = now(),
                lease_expires_at = null
            where id = $1
              and state = 'in_progress'::underway.activity_call_state
              and attempt_count = $3
            "#,
            call.id,
            output,
            call.attempt_count,
        )
        .execute(&mut *tx)
        .await?;

        sqlx::query!(
            r#"
            update underway.activity_call_attempt
            set state = 'succeeded'::underway.activity_call_state,
                updated_at = now(),
                completed_at = now()
            where activity_call_id = $1
              and attempt_number = $2
            "#,
            call.id,
            call.attempt_number,
        )
        .execute(&mut *tx)
        .await?;

        self.wake_waiting_task(&mut tx, call).await?;

        tx.commit().await?;

        Ok(())
    }

    async fn mark_failed(
        &self,
        call: &ClaimedCall,
        retry_policy: RetryPolicy,
        error: activity::Error,
    ) -> Result {
        let retry_count = call.attempt_count;
        let error_value = serde_json::to_value(&error)?;
        let mut tx = self.pool.begin().await?;

        if error.retryable && retry_count < retry_policy.max_attempts {
            let delay = retry_policy.calculate_delay(retry_count);
            let delay: Duration = delay.try_into()?;

            sqlx::query!(
                r#"
                update underway.activity_call
                set state = 'pending'::underway.activity_call_state,
                    error = $2,
                    attempt_count = $3,
                    available_at = now() + $4,
                    updated_at = now(),
                    lease_expires_at = null
                where id = $1
                  and state = 'in_progress'::underway.activity_call_state
                  and attempt_count = $3
                "#,
                call.id,
                error_value,
                retry_count,
                delay as _,
            )
            .execute(&mut *tx)
            .await?;

            sqlx::query!(
                r#"
                update underway.activity_call_attempt
                set state = 'failed'::underway.activity_call_state,
                    error = $3,
                    updated_at = now(),
                    completed_at = now()
                where activity_call_id = $1
                  and attempt_number = $2
                "#,
                call.id,
                call.attempt_number,
                error_value,
            )
            .execute(&mut *tx)
            .await?;

            tx.commit().await?;

            return Ok(());
        }

        self.mark_failed_terminal(&mut tx, call, error).await?;
        tx.commit().await?;

        Ok(())
    }

    async fn mark_failed_terminal(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        call: &ClaimedCall,
        error: activity::Error,
    ) -> Result {
        let error_value = serde_json::to_value(error)?;

        sqlx::query!(
            r#"
            update underway.activity_call
            set state = 'failed'::underway.activity_call_state,
                error = $2,
                attempt_count = $3,
                updated_at = now(),
                completed_at = now(),
                lease_expires_at = null
            where id = $1
              and state = 'in_progress'::underway.activity_call_state
              and attempt_count = $3
            "#,
            call.id,
            error_value,
            call.attempt_count,
        )
        .execute(&mut **tx)
        .await?;

        sqlx::query!(
            r#"
            update underway.activity_call_attempt
            set state = 'failed'::underway.activity_call_state,
                error = $3,
                updated_at = now(),
                completed_at = now()
            where activity_call_id = $1
              and attempt_number = $2
            "#,
            call.id,
            call.attempt_number,
            error_value,
        )
        .execute(&mut **tx)
        .await?;

        self.wake_waiting_task(tx, call).await?;

        Ok(())
    }

    async fn wake_waiting_task(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        call: &ClaimedCall,
    ) -> Result {
        sqlx::query!(
            r#"
            update underway.task
            set state = 'pending'::underway.task_state,
                run_at = now(),
                lease_expires_at = null,
                updated_at = now()
            where task_queue_name = $1
              and state = 'waiting'::underway.task_state
              and (input->>'workflow_run_id')::uuid = $2
              and (input->>'step_index')::integer = $3
            "#,
            call.task_queue_name,
            call.workflow_run_id,
            call.step_index,
        )
        .execute(&mut **tx)
        .await?;

        Ok(())
    }
}
