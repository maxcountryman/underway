//! Postgres backend for Underway.
#![forbid(unsafe_code)]

use std::time::Duration;

use jiff::{Span, ToSpan};
use sqlx::{
    migrate::Migrator,
    postgres::{types::PgInterval, PgPool, PgPoolOptions},
    Postgres, Row, Transaction,
};
use underway_core::{
    activity::{self, CallState},
    backend::{
        ActivityCallCommand, ActivityCallRecord, Backend, BackendTransaction, ClaimedActivityCall,
        ClaimedTask, EnqueueTask, SchedulerLease, StoredSchedule,
    },
    scheduler::ZonedSchedule,
    task::{self, TaskConfig, TaskId},
    workflow::WorkflowRunId,
};
use uuid::Uuid;

static MIGRATOR: Migrator = sqlx::migrate!("./migrations");

/// Postgres backend error.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// SQLx error.
    #[error(transparent)]
    Sqlx(#[from] sqlx::Error),

    /// Migration error.
    #[error(transparent)]
    Migrate(#[from] sqlx::migrate::MigrateError),

    /// JSON error.
    #[error(transparent)]
    Json(#[from] serde_json::Error),

    /// Jiff error.
    #[error(transparent)]
    Jiff(#[from] jiff::Error),

    /// Invalid task configuration loaded from storage.
    #[error(transparent)]
    TaskConfig(#[from] task::ConfigError),

    /// Missing record.
    #[error("{0} not found")]
    NotFound(String),
}

/// SQLx/Postgres backend.
#[derive(Clone)]
pub struct PostgresBackend {
    pool: PgPool,
}

impl PostgresBackend {
    /// Builds a backend from an existing pool.
    pub fn from_pool(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Connects a new pool from a database URL.
    pub async fn connect(database_url: &str) -> Result<Self, Error> {
        let pool = PgPoolOptions::new().connect(database_url).await?;
        Ok(Self::from_pool(pool))
    }

    /// Returns the underlying pool.
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Runs Underway migrations in the `underway` schema.
    pub async fn run_migrations(&self) -> Result<(), Error> {
        let mut tx = self.pool.begin().await?;
        sqlx::query("create schema if not exists underway;")
            .execute(&mut *tx)
            .await?;
        sqlx::query("set local search_path to underway;")
            .execute(&mut *tx)
            .await?;
        MIGRATOR.run(&mut *tx).await?;
        tx.commit().await?;
        Ok(())
    }
}

/// Postgres transaction.
pub struct PostgresTransaction<'a> {
    inner: Transaction<'a, Postgres>,
}

/// Postgres scheduler lease.
pub struct PostgresSchedulerLease<'a> {
    conn: Option<sqlx::pool::PoolConnection<Postgres>>,
    key: String,
    _marker: std::marker::PhantomData<&'a ()>,
}

impl SchedulerLease for PostgresSchedulerLease<'_> {
    type Error = Error;

    async fn release(&mut self) -> Result<(), Self::Error> {
        if let Some(mut conn) = self.conn.take() {
            sqlx::query("select pg_advisory_unlock(hashtext($1))")
                .bind(&self.key)
                .execute(&mut *conn)
                .await?;
        }
        Ok(())
    }
}

impl Drop for PostgresSchedulerLease<'_> {
    fn drop(&mut self) {
        // The connection drop releases session advisory locks. Explicit release
        // is still used on the normal path.
        self.conn.take();
    }
}

impl Backend for PostgresBackend {
    type Error = Error;
    type Transaction<'a> = PostgresTransaction<'a>;
    type SchedulerLease<'a> = PostgresSchedulerLease<'a>;

    async fn begin(&self) -> Result<Self::Transaction<'_>, Self::Error> {
        Ok(PostgresTransaction {
            inner: self.pool.begin().await?,
        })
    }

    async fn record_heartbeat(&self, task: &ClaimedTask) -> Result<(), Self::Error> {
        sqlx::query(
            r#"
            update underway.task
            set updated_at = now(),
                last_heartbeat_at = now(),
                lease_expires_at = now() + heartbeat
            where id = $1
              and task_queue_name = $2
              and current_attempt = $3
              and state = 'in_progress'::underway.task_state
            "#,
        )
        .bind(task.id.as_uuid())
        .bind(&task.queue_name)
        .bind(task.attempt_number)
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn try_acquire_scheduler(
        &self,
        queue_name: &str,
    ) -> Result<Option<Self::SchedulerLease<'_>>, Self::Error> {
        let mut conn = self.pool.acquire().await?;
        let key = format!("{queue_name}-scheduler");
        let acquired: bool =
            sqlx::query_scalar::<_, bool>("select pg_try_advisory_lock(hashtext($1))")
                .bind(&key)
                .fetch_one(&mut *conn)
                .await?;

        if !acquired {
            return Ok(None);
        }

        Ok(Some(PostgresSchedulerLease {
            conn: Some(conn),
            key,
            _marker: std::marker::PhantomData,
        }))
    }
}

impl BackendTransaction for PostgresTransaction<'_> {
    type Error = Error;

    async fn commit(self) -> Result<(), Self::Error> {
        self.inner.commit().await?;
        Ok(())
    }

    async fn rollback(self) -> Result<(), Self::Error> {
        self.inner.rollback().await?;
        Ok(())
    }

    async fn create_queue(
        &mut self,
        name: &str,
        dlq_name: Option<&str>,
    ) -> Result<(), Self::Error> {
        sqlx::query(
            r#"
            insert into underway.task_queue (name)
            values ($1)
            on conflict do nothing
            "#,
        )
        .bind(name)
        .execute(&mut *self.inner)
        .await?;

        if let Some(dlq_name) = dlq_name {
            sqlx::query(
                r#"
                insert into underway.task_queue (name)
                values ($1)
                on conflict do nothing
                "#,
            )
            .bind(dlq_name)
            .execute(&mut *self.inner)
            .await?;

            sqlx::query(
                r#"
                update underway.task_queue
                set dlq_name = $2
                where name = $1
                "#,
            )
            .bind(name)
            .bind(dlq_name)
            .execute(&mut *self.inner)
            .await?;
        }

        Ok(())
    }

    async fn enqueue_task(
        &mut self,
        queue_name: &str,
        task: EnqueueTask,
    ) -> Result<TaskId, Self::Error> {
        let id = TaskId::new();
        let delay = task.config.delay().checked_add(task.delay)?;
        sqlx::query(
            r#"
            insert into underway.task (
              id,
              task_queue_name,
              input,
              timeout,
              heartbeat,
              ttl,
              delay,
              run_at,
              retry_policy,
              concurrency_key,
              priority
            ) values (
              $1,
              $2,
              $3,
              $4,
              $5,
              $6,
              $7,
              now() + $7,
              row($8, $9, $10, $11, $12)::underway.task_retry_policy,
              $13,
              $14
            )
            "#,
        )
        .bind(id.as_uuid())
        .bind(queue_name)
        .bind(task.input)
        .bind(pg_interval(task.config.timeout())?)
        .bind(pg_interval(task.config.heartbeat())?)
        .bind(pg_interval(task.config.ttl())?)
        .bind(pg_interval(delay)?)
        .bind(task.config.retry_policy().max_attempts)
        .bind(task.config.retry_policy().initial_interval_ms)
        .bind(task.config.retry_policy().max_interval_ms)
        .bind(task.config.retry_policy().backoff_coefficient)
        .bind(task.config.retry_policy().jitter_factor)
        .bind(task.config.concurrency_key())
        .bind(task.config.priority())
        .execute(&mut *self.inner)
        .await?;
        Ok(id)
    }

    async fn claim_task(&mut self, queue_name: &str) -> Result<Option<ClaimedTask>, Self::Error> {
        fail_stale_exhausted(&mut self.inner, queue_name).await?;

        let row = sqlx::query(
            r#"
            with available_task as (
                select id
                from underway.task
                where task_queue_name = $1
                  and (
                    (state = 'pending'::underway.task_state and run_at <= now())
                    or (
                        state = 'in_progress'::underway.task_state
                        and lease_expires_at is not null
                        and lease_expires_at <= now()
                        and attempt_count < (retry_policy).max_attempts
                    )
                  )
                order by priority desc, run_at, created_at, id
                limit 1
                for update skip locked
            ),
            claimed as (
                update underway.task t
                set state = 'in_progress'::underway.task_state,
                    run_at = now(),
                    last_attempt_at = now(),
                    last_heartbeat_at = now(),
                    attempt_count = t.attempt_count + 1,
                    current_attempt = t.attempt_count + 1,
                    lease_expires_at = now() + t.heartbeat
                from available_task
                where t.task_queue_name = $1
                  and t.id = available_task.id
                returning
                    t.id,
                    t.task_queue_name,
                    t.input,
                    t.timeout,
                    t.heartbeat,
                    t.ttl,
                    t.delay,
                    (t.retry_policy).max_attempts as retry_max_attempts,
                    (t.retry_policy).initial_interval_ms as retry_initial_interval_ms,
                    (t.retry_policy).max_interval_ms as retry_max_interval_ms,
                    (t.retry_policy).backoff_coefficient as retry_backoff_coefficient,
                    (t.retry_policy).jitter_factor as retry_jitter_factor,
                    t.concurrency_key,
                    t.priority,
                    t.current_attempt
            ),
            finalized_previous_attempt as (
                update underway.task_attempt ta
                set state = 'failed'::underway.task_state,
                    updated_at = now(),
                    completed_at = now(),
                    error_message = coalesce(
                        ta.error_message,
                        'Task lease expired and was reclaimed by another worker.'
                    )
                from claimed c
                where c.current_attempt > 1
                  and ta.task_id = c.id
                  and ta.task_queue_name = c.task_queue_name
                  and ta.attempt_number = c.current_attempt - 1
                  and ta.state = 'in_progress'::underway.task_state
            ),
            upsert_current_attempt as (
                insert into underway.task_attempt (
                    task_id,
                    task_queue_name,
                    state,
                    attempt_number
                )
                select
                    c.id,
                    c.task_queue_name,
                    'in_progress'::underway.task_state,
                    c.current_attempt
                from claimed c
                on conflict (task_queue_name, task_id, attempt_number)
                do update
                set state = excluded.state,
                    error_message = null,
                    started_at = now(),
                    updated_at = now(),
                    completed_at = null
            )
            select * from claimed
            "#,
        )
        .bind(queue_name)
        .fetch_optional(&mut *self.inner)
        .await?;

        row.map(claimed_task_from_row).transpose()
    }

    async fn mark_task_succeeded(&mut self, task: &ClaimedTask) -> Result<(), Self::Error> {
        update_task_attempt_state(&mut self.inner, task, "succeeded", true, None).await?;
        update_task_state(&mut self.inner, task, "succeeded", true).await
    }

    async fn mark_task_waiting(&mut self, task: &ClaimedTask) -> Result<(), Self::Error> {
        update_task_attempt_state(&mut self.inner, task, "waiting", false, None).await?;
        update_task_state(&mut self.inner, task, "waiting", false).await
    }

    async fn record_task_failure(
        &mut self,
        task: &ClaimedTask,
        error: &task::Error,
    ) -> Result<(), Self::Error> {
        update_task_attempt_state(
            &mut self.inner,
            task,
            "failed",
            false,
            Some(error.to_string()),
        )
        .await?;
        Ok(())
    }

    async fn retry_task(&mut self, task: &ClaimedTask, delay: Span) -> Result<(), Self::Error> {
        sqlx::query(
            r#"
            update underway.task
            set state = 'pending'::underway.task_state,
                delay = $2,
                run_at = now() + $2,
                updated_at = now(),
                lease_expires_at = null
            where id = $1
              and task_queue_name = $3
              and current_attempt = $4
            "#,
        )
        .bind(task.id.as_uuid())
        .bind(pg_interval(delay)?)
        .bind(&task.queue_name)
        .bind(task.attempt_number)
        .execute(&mut *self.inner)
        .await?;
        Ok(())
    }

    async fn mark_task_failed(&mut self, task: &ClaimedTask) -> Result<(), Self::Error> {
        update_task_attempt_state(&mut self.inner, task, "failed", true, None).await?;
        update_task_state(&mut self.inner, task, "failed", true).await
    }

    async fn move_task_to_dlq(
        &mut self,
        task_id: TaskId,
        from_queue: &str,
        dlq_name: &str,
    ) -> Result<(), Self::Error> {
        sqlx::query(
            r#"
            with moved_task as (
                update underway.task
                set task_queue_name = $2
                where id = $1 and task_queue_name = $3
                returning id
            )
            update underway.task_attempt
            set task_queue_name = $2
            where task_id = $1 and task_queue_name = $3
            "#,
        )
        .bind(task_id.as_uuid())
        .bind(dlq_name)
        .bind(from_queue)
        .execute(&mut *self.inner)
        .await?;
        Ok(())
    }

    async fn cancel_workflow(
        &mut self,
        queue_name: &str,
        workflow_run_id: WorkflowRunId,
    ) -> Result<bool, Self::Error> {
        let result = sqlx::query(
            r#"
            update underway.task
            set state = 'cancelled'::underway.task_state,
                updated_at = now(),
                completed_at = now(),
                lease_expires_at = null
            where task_queue_name = $1
              and state = 'pending'::underway.task_state
              and (input->>'workflow_run_id')::uuid = $2
            "#,
        )
        .bind(queue_name)
        .bind(workflow_run_id.as_uuid())
        .execute(&mut *self.inner)
        .await?;
        Ok(result.rows_affected() > 0)
    }

    async fn schedule_task(
        &mut self,
        queue_name: &str,
        schedule: ZonedSchedule,
        input: serde_json::Value,
    ) -> Result<(), Self::Error> {
        sqlx::query(
            r#"
            insert into underway.task_schedule (
              task_queue_name,
              schedule,
              timezone,
              input
            ) values ($1, $2, $3, $4)
            on conflict (task_queue_name) do update
            set schedule = excluded.schedule,
                timezone = excluded.timezone,
                input = excluded.input
            "#,
        )
        .bind(queue_name)
        .bind(schedule.cron_expr())
        .bind(schedule.iana_name())
        .bind(input)
        .execute(&mut *self.inner)
        .await?;
        Ok(())
    }

    async fn unschedule_task(&mut self, queue_name: &str) -> Result<(), Self::Error> {
        sqlx::query("delete from underway.task_schedule where task_queue_name = $1")
            .bind(queue_name)
            .execute(&mut *self.inner)
            .await?;
        Ok(())
    }

    async fn task_schedule(
        &mut self,
        queue_name: &str,
    ) -> Result<Option<StoredSchedule>, Self::Error> {
        let row = sqlx::query(
            "select schedule, timezone, input from underway.task_schedule where task_queue_name = \
             $1",
        )
        .bind(queue_name)
        .fetch_optional(&mut *self.inner)
        .await?;

        row.map(|row| {
            let schedule: String = row.try_get("schedule")?;
            let timezone: String = row.try_get("timezone")?;
            let input: serde_json::Value = row.try_get("input")?;
            let schedule = ZonedSchedule::new(&schedule, &timezone)
                .map_err(|err| Error::NotFound(err.to_string()))?;
            Ok(StoredSchedule { schedule, input })
        })
        .transpose()
    }

    async fn load_activity_calls(
        &mut self,
        queue_name: &str,
        workflow_run_id: WorkflowRunId,
        step_index: usize,
    ) -> Result<Vec<ActivityCallRecord>, Self::Error> {
        let rows = sqlx::query(
            r#"
            select call_key, activity, input, output, error, state::text as state
            from underway.activity_call
            where task_queue_name = $1
              and workflow_run_id = $2
              and step_index = $3
            "#,
        )
        .bind(queue_name)
        .bind(workflow_run_id.as_uuid())
        .bind(step_index as i32)
        .fetch_all(&mut *self.inner)
        .await?;

        rows.into_iter().map(activity_record_from_row).collect()
    }

    async fn insert_activity_call(
        &mut self,
        queue_name: &str,
        workflow_run_id: WorkflowRunId,
        step_index: usize,
        command: ActivityCallCommand,
    ) -> Result<(), Self::Error> {
        sqlx::query(
            r#"
            insert into underway.activity_call (
                id,
                task_queue_name,
                workflow_run_id,
                step_index,
                call_key,
                activity,
                input,
                timeout_ms,
                max_attempts,
                state
            ) values (
                $1,
                $2,
                $3,
                $4,
                $5,
                $6,
                $7,
                $8,
                $9,
                'pending'::underway.activity_call_state
            )
            on conflict (task_queue_name, workflow_run_id, step_index, call_key) do nothing
            "#,
        )
        .bind(Uuid::new_v4())
        .bind(queue_name)
        .bind(workflow_run_id.as_uuid())
        .bind(step_index as i32)
        .bind(command.call_key)
        .bind(command.activity)
        .bind(command.input)
        .bind(duration_ms_i32(command.timeout)?)
        .bind(command.retry_policy.max_attempts)
        .execute(&mut *self.inner)
        .await?;
        Ok(())
    }

    async fn claim_activity_call(
        &mut self,
        queue_name: &str,
        activities: &[String],
    ) -> Result<Option<ClaimedActivityCall>, Self::Error> {
        let row = sqlx::query(
            r#"
            with next_call as (
                select c.id
                from underway.activity_call c
                where c.task_queue_name = $1
                  and c.activity = any($2::text[])
                  and (
                    (c.state = 'pending'::underway.activity_call_state and c.available_at <= now())
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
                        now() + ((c.timeout_ms::bigint + 30000) * interval '1 millisecond')
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
                do update set
                    state = 'in_progress'::underway.activity_call_state,
                    error = null,
                    started_at = now(),
                    updated_at = now(),
                    completed_at = null
            )
            select * from claimed
            "#,
        )
        .bind(queue_name)
        .bind(activities)
        .fetch_optional(&mut *self.inner)
        .await?;

        row.map(claimed_activity_from_row).transpose()
    }

    async fn complete_activity_call(
        &mut self,
        call: &ClaimedActivityCall,
        output: serde_json::Value,
    ) -> Result<(), Self::Error> {
        sqlx::query(
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
        )
        .bind(call.id)
        .bind(output)
        .bind(call.attempt_count)
        .execute(&mut *self.inner)
        .await?;
        update_activity_attempt(&mut self.inner, call, "succeeded", None).await
    }

    async fn retry_activity_call(
        &mut self,
        call: &ClaimedActivityCall,
        error: activity::Error,
        delay: Span,
    ) -> Result<(), Self::Error> {
        let error = serde_json::to_value(error)?;
        sqlx::query(
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
        )
        .bind(call.id)
        .bind(error.clone())
        .bind(call.attempt_count)
        .bind(pg_interval(delay)?)
        .execute(&mut *self.inner)
        .await?;
        update_activity_attempt(&mut self.inner, call, "failed", Some(error)).await
    }

    async fn fail_activity_call(
        &mut self,
        call: &ClaimedActivityCall,
        error: activity::Error,
    ) -> Result<(), Self::Error> {
        let error = serde_json::to_value(error)?;
        sqlx::query(
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
        )
        .bind(call.id)
        .bind(error.clone())
        .bind(call.attempt_count)
        .execute(&mut *self.inner)
        .await?;
        update_activity_attempt(&mut self.inner, call, "failed", Some(error)).await
    }

    async fn wake_workflow_task(
        &mut self,
        queue_name: &str,
        workflow_run_id: WorkflowRunId,
        step_index: usize,
    ) -> Result<(), Self::Error> {
        sqlx::query(
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
        )
        .bind(queue_name)
        .bind(workflow_run_id.as_uuid())
        .bind(step_index as i32)
        .execute(&mut *self.inner)
        .await?;
        Ok(())
    }

    async fn delete_expired(&mut self) -> Result<(), Self::Error> {
        sqlx::query(
            r#"
            delete from underway.task
            where state != 'in_progress'::underway.task_state
              and created_at + ttl < now()
            "#,
        )
        .execute(&mut *self.inner)
        .await?;
        Ok(())
    }
}

async fn fail_stale_exhausted(
    tx: &mut Transaction<'_, Postgres>,
    queue_name: &str,
) -> Result<(), Error> {
    sqlx::query(
        r#"
        with stale_exhausted as (
            select t.id, t.current_attempt
            from underway.task t
            where t.task_queue_name = $1
              and t.state = 'in_progress'::underway.task_state
              and t.lease_expires_at is not null
              and t.lease_expires_at <= now()
              and t.attempt_count >= (t.retry_policy).max_attempts
            for update skip locked
        ),
        finalized_attempts as (
            update underway.task_attempt ta
            set state = 'failed'::underway.task_state,
                updated_at = now(),
                completed_at = now(),
                error_message = coalesce(
                    ta.error_message,
                    'Task lease expired and retry policy was exhausted.'
                )
            from stale_exhausted se
            where ta.task_queue_name = $1
              and ta.task_id = se.id
              and ta.attempt_number = se.current_attempt
              and ta.state = 'in_progress'::underway.task_state
        )
        update underway.task t
        set state = 'failed'::underway.task_state,
            updated_at = now(),
            completed_at = now(),
            lease_expires_at = null
        from stale_exhausted se
        where t.task_queue_name = $1
          and t.id = se.id
        "#,
    )
    .bind(queue_name)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

async fn update_task_attempt_state(
    tx: &mut Transaction<'_, Postgres>,
    task: &ClaimedTask,
    state: &str,
    completed: bool,
    error: Option<String>,
) -> Result<(), Error> {
    let completed_at = if completed { "now()" } else { "completed_at" };
    let sql = format!(
        r#"
        update underway.task_attempt
        set state = $3::underway.task_state,
            updated_at = now(),
            completed_at = {completed_at},
            error_message = coalesce($5, error_message)
        where task_id = $1
          and task_queue_name = $2
          and attempt_number = $4
        "#
    );
    sqlx::query(&sql)
        .bind(task.id.as_uuid())
        .bind(&task.queue_name)
        .bind(state)
        .bind(task.attempt_number)
        .bind(error)
        .execute(&mut **tx)
        .await?;
    Ok(())
}

async fn update_task_state(
    tx: &mut Transaction<'_, Postgres>,
    task: &ClaimedTask,
    state: &str,
    completed: bool,
) -> Result<(), Error> {
    let completed_at = if completed { "now()" } else { "completed_at" };
    let sql = format!(
        r#"
        update underway.task
        set state = $2::underway.task_state,
            updated_at = now(),
            completed_at = {completed_at},
            lease_expires_at = null
        where id = $1
          and task_queue_name = $3
          and current_attempt = $4
        "#
    );
    sqlx::query(&sql)
        .bind(task.id.as_uuid())
        .bind(state)
        .bind(&task.queue_name)
        .bind(task.attempt_number)
        .execute(&mut **tx)
        .await?;
    Ok(())
}

async fn update_activity_attempt(
    tx: &mut Transaction<'_, Postgres>,
    call: &ClaimedActivityCall,
    state: &str,
    error: Option<serde_json::Value>,
) -> Result<(), Error> {
    sqlx::query(
        r#"
        update underway.activity_call_attempt
        set state = $3::underway.activity_call_state,
            error = coalesce($4, error),
            updated_at = now(),
            completed_at = now()
        where activity_call_id = $1
          and attempt_number = $2
        "#,
    )
    .bind(call.id)
    .bind(call.attempt_number)
    .bind(state)
    .bind(error)
    .execute(&mut **tx)
    .await?;
    Ok(())
}

fn claimed_task_from_row(row: sqlx::postgres::PgRow) -> Result<ClaimedTask, Error> {
    let id: Uuid = row.try_get("id")?;
    let timeout: PgInterval = row.try_get("timeout")?;
    let heartbeat: PgInterval = row.try_get("heartbeat")?;
    let ttl: PgInterval = row.try_get("ttl")?;
    let delay: PgInterval = row.try_get("delay")?;
    Ok(ClaimedTask {
        id: TaskId::from_uuid(id),
        queue_name: row.try_get("task_queue_name")?,
        input: row.try_get("input")?,
        config: TaskConfig::builder()
            .retry_policy(task::RetryPolicy {
                max_attempts: row.try_get("retry_max_attempts")?,
                initial_interval_ms: row.try_get("retry_initial_interval_ms")?,
                max_interval_ms: row.try_get("retry_max_interval_ms")?,
                backoff_coefficient: row.try_get("retry_backoff_coefficient")?,
                jitter_factor: row.try_get("retry_jitter_factor")?,
            })
            .timeout(span_from_interval(timeout))
            .ttl(span_from_interval(ttl))
            .delay(span_from_interval(delay))
            .heartbeat(span_from_interval(heartbeat))
            .maybe_concurrency_key(row.try_get("concurrency_key")?)
            .priority(row.try_get("priority")?)
            .build()?,
        attempt_number: row.try_get("current_attempt")?,
    })
}

fn activity_record_from_row(row: sqlx::postgres::PgRow) -> Result<ActivityCallRecord, Error> {
    let state: String = row.try_get("state")?;
    Ok(ActivityCallRecord {
        call_key: row.try_get("call_key")?,
        activity: row.try_get("activity")?,
        input: row.try_get("input")?,
        output: row.try_get("output")?,
        error: row.try_get("error")?,
        state: call_state(&state)?,
    })
}

fn claimed_activity_from_row(row: sqlx::postgres::PgRow) -> Result<ClaimedActivityCall, Error> {
    let workflow_run_id: Uuid = row.try_get("workflow_run_id")?;
    let step_index: i32 = row.try_get("step_index")?;
    let attempt_count: i32 = row.try_get("attempt_count")?;
    Ok(ClaimedActivityCall {
        id: row.try_get("id")?,
        queue_name: row.try_get("task_queue_name")?,
        workflow_run_id: WorkflowRunId::from_uuid(workflow_run_id),
        step_index: step_index as usize,
        call_key: row.try_get("call_key")?,
        activity: row.try_get("activity")?,
        input: row.try_get("input")?,
        attempt_count,
        attempt_number: attempt_count,
    })
}

fn call_state(state: &str) -> Result<CallState, Error> {
    match state {
        "pending" => Ok(CallState::Pending),
        "in_progress" => Ok(CallState::InProgress),
        "succeeded" => Ok(CallState::Succeeded),
        "failed" => Ok(CallState::Failed),
        other => Err(Error::NotFound(format!("activity state `{other}`"))),
    }
}

fn pg_interval(span: Span) -> Result<PgInterval, Error> {
    let duration: Duration = span.try_into()?;
    Ok(PgInterval {
        months: 0,
        days: 0,
        microseconds: duration.as_micros().min(i64::MAX as u128) as i64,
    })
}

fn span_from_interval(interval: PgInterval) -> Span {
    let day_ms = i64::from(interval.days).saturating_mul(86_400_000);
    let micros_ms = interval.microseconds.saturating_div(1_000);
    day_ms.saturating_add(micros_ms).milliseconds()
}

fn duration_ms_i32(span: Span) -> Result<i32, Error> {
    let duration: Duration = span.try_into()?;
    Ok(duration.as_millis().min(i32::MAX as u128) as i32)
}
