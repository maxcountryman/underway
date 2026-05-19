use std::time::Duration;

use jiff::{Span, ToSpan};
use sqlx::{
    postgres::{types::PgInterval, PgRow},
    Row,
};
use underway_core::{
    activity::CallState,
    backend::{ActivityCallRecord, ClaimedActivityCall, ClaimedTask},
    task::{self, TaskConfig, TaskId},
    workflow::WorkflowRunId,
};
use uuid::Uuid;

use crate::Error;

pub(crate) fn claimed_task_from_row(row: PgRow) -> Result<ClaimedTask, Error> {
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

pub(crate) fn activity_record_from_row(row: PgRow) -> Result<ActivityCallRecord, Error> {
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

pub(crate) fn claimed_activity_from_row(row: PgRow) -> Result<ClaimedActivityCall, Error> {
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

pub(crate) fn pg_interval(span: Span) -> Result<PgInterval, Error> {
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

pub(crate) fn duration_ms_i32(span: Span) -> Result<i32, Error> {
    let duration: Duration = span.try_into()?;
    Ok(duration.as_millis().min(i32::MAX as u128) as i32)
}
