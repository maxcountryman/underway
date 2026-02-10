use std::{collections::HashMap, future::Future, pin::Pin, sync::Arc, time::Duration};

use jiff::{Span, ToSpan};
use serde_json::Value;
use sqlx::{PgPool, Postgres, Transaction};
use tokio_util::sync::CancellationToken;
use tracing::instrument;
use uuid::Uuid;

use crate::{
    activity::{self, Activity},
    queue::connect_listeners_with_retry,
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
    workflow_id: Uuid,
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
            let mut listeners = connect_listeners_with_retry(
                &self.pool,
                &["activity_call_change"],
                &RetryPolicy::default(),
            )
            .await?;

            let mut activity_change_listener = listeners.remove(0);

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
        let (activities, timeout_millis) = self.activity_claim_specs()?;

        let mut tx = self.pool.begin().await?;
        let call = self
            .claim_next_call(&mut tx, &activities, &timeout_millis)
            .await?;
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
        timeout_millis: &[i64],
    ) -> Result<Option<ClaimedCall>> {
        let call = sqlx::query_as!(
            ClaimedCall,
            r#"
            with handler as (
                select *
                from unnest($1::text[], $2::bigint[]) as h(activity, timeout_ms)
            ),
            next_call as (
                select c.id
                from underway.activity_call c
                inner join handler h
                  on h.activity = c.activity
                where c.task_queue_name = $3
                  and (
                    (
                        c.state = 'pending'::underway.activity_call_state
                        and c.available_at <= now()
                    )
                    or (
                        c.state = 'in_progress'::underway.activity_call_state
                        and coalesce(c.started_at, c.updated_at, c.created_at)
                              + ((h.timeout_ms + $4) * interval '1 millisecond')
                            <= now()
                    )
                  )
                order by c.created_at, c.id
                limit 1
                for update skip locked
            ),
            claimed as (
                update underway.activity_call c
                set state = 'in_progress'::underway.activity_call_state,
                    updated_at = now(),
                    started_at = now()
                from next_call
                where c.id = next_call.id
                returning
                    c.id,
                    c.task_queue_name,
                    c.workflow_id,
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
                    claimed.attempt_count + 1,
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
                    activity_call_id,
                    attempt_number
            )
            select
                claimed.id,
                claimed.task_queue_name,
                claimed.workflow_id,
                claimed.step_index,
                claimed.call_key,
                claimed.activity,
                claimed.input,
                claimed.attempt_count,
                attempt.attempt_number
            from claimed
            inner join attempt
              on attempt.activity_call_id = claimed.id
            "#,
            activities as _,
            timeout_millis as _,
            self.queue_name,
            IN_PROGRESS_RECLAIM_GRACE_MS,
        )
        .fetch_optional(&mut **tx)
        .await?;

        Ok(call)
    }

    fn activity_claim_specs(&self) -> Result<(Vec<String>, Vec<i64>)> {
        let mut handlers = self
            .registry
            .handlers
            .iter()
            .map(|(activity, handler)| {
                let timeout: Duration = handler.timeout().try_into()?;
                let timeout_millis = timeout.as_millis().min(i64::MAX as u128) as i64;
                Ok((activity.clone(), timeout_millis))
            })
            .collect::<Result<Vec<_>>>()?;

        handlers.sort_by(|left, right| left.0.cmp(&right.0));

        let mut activities = Vec::with_capacity(handlers.len());
        let mut timeout_millis = Vec::with_capacity(handlers.len());
        for (activity, timeout_ms) in handlers {
            activities.push(activity);
            timeout_millis.push(timeout_ms);
        }

        Ok((activities, timeout_millis))
    }

    async fn mark_succeeded(&self, call: &ClaimedCall, output: Value) -> Result {
        let attempt_count = call.attempt_count + 1;
        let mut tx = self.pool.begin().await?;

        sqlx::query!(
            r#"
            update underway.activity_call
            set state = 'succeeded'::underway.activity_call_state,
                output = $2,
                attempt_count = $3,
                updated_at = now(),
                completed_at = now()
            where id = $1
            "#,
            call.id,
            output,
            attempt_count,
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
        let retry_count = call.attempt_count + 1;
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
                    updated_at = now()
                where id = $1
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
        let retry_count = call.attempt_count + 1;
        let error_value = serde_json::to_value(error)?;

        sqlx::query!(
            r#"
            update underway.activity_call
            set state = 'failed'::underway.activity_call_state,
                error = $2,
                attempt_count = $3,
                updated_at = now(),
                completed_at = now()
            where id = $1
            "#,
            call.id,
            error_value,
            retry_count,
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
                delay = interval '0',
                updated_at = now()
            where task_queue_name = $1
              and state = 'waiting'::underway.task_state
              and (input->>'workflow_id')::uuid = $2
              and (input->>'step_index')::integer = $3
            "#,
            call.task_queue_name,
            call.workflow_id,
            call.step_index,
        )
        .execute(&mut **tx)
        .await?;

        Ok(())
    }
}
