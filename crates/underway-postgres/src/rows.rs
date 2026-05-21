use std::time::Duration;

use jiff::{Span, ToSpan};
use sqlx::postgres::types::PgInterval;
use underway_core::{
    activity::CallState,
    backend::{ActivityCallRecord, ClaimedActivityCall, ClaimedTask},
    task::{self, TaskConfig, TaskId},
    workflow::WorkflowRunId,
};
use uuid::Uuid;

use crate::Error;

pub(crate) struct ClaimedTaskRow {
    pub(crate) id: Uuid,
    pub(crate) task_queue_name: String,
    pub(crate) input: serde_json::Value,
    pub(crate) timeout: PgInterval,
    pub(crate) heartbeat: PgInterval,
    pub(crate) ttl: PgInterval,
    pub(crate) delay: PgInterval,
    pub(crate) retry_max_attempts: i32,
    pub(crate) retry_initial_interval_ms: i32,
    pub(crate) retry_max_interval_ms: i32,
    pub(crate) retry_backoff_coefficient: f64,
    pub(crate) retry_jitter_factor: f64,
    pub(crate) concurrency_key: Option<String>,
    pub(crate) priority: i32,
    pub(crate) current_attempt: i32,
}

pub(crate) fn claimed_task_from_row(row: ClaimedTaskRow) -> Result<ClaimedTask, Error> {
    Ok(ClaimedTask {
        id: TaskId::from_uuid(row.id),
        queue_name: row.task_queue_name,
        input: row.input,
        config: TaskConfig::builder()
            .retry_policy(task::RetryPolicy {
                max_attempts: row.retry_max_attempts,
                initial_interval_ms: row.retry_initial_interval_ms,
                max_interval_ms: row.retry_max_interval_ms,
                backoff_coefficient: row.retry_backoff_coefficient,
                jitter_factor: row.retry_jitter_factor,
            })
            .timeout(span_from_interval(row.timeout))
            .ttl(span_from_interval(row.ttl))
            .delay(span_from_interval(row.delay))
            .heartbeat(span_from_interval(row.heartbeat))
            .maybe_concurrency_key(row.concurrency_key)
            .priority(row.priority)
            .build()?,
        attempt_number: row.current_attempt,
    })
}

pub(crate) struct ActivityRecordRow {
    pub(crate) call_key: String,
    pub(crate) activity: String,
    pub(crate) input: serde_json::Value,
    pub(crate) output: Option<serde_json::Value>,
    pub(crate) error: Option<serde_json::Value>,
    pub(crate) state: String,
}

pub(crate) fn activity_record_from_row(
    row: ActivityRecordRow,
) -> Result<ActivityCallRecord, Error> {
    Ok(ActivityCallRecord {
        call_key: row.call_key,
        activity: row.activity,
        input: row.input,
        output: row.output,
        error: row.error,
        state: call_state(&row.state)?,
    })
}

pub(crate) struct ClaimedActivityRow {
    pub(crate) id: Uuid,
    pub(crate) task_queue_name: String,
    pub(crate) workflow_run_id: Uuid,
    pub(crate) step_index: i32,
    pub(crate) call_key: String,
    pub(crate) activity: String,
    pub(crate) input: serde_json::Value,
    pub(crate) attempt_count: i32,
}

pub(crate) fn claimed_activity_from_row(
    row: ClaimedActivityRow,
) -> Result<ClaimedActivityCall, Error> {
    Ok(ClaimedActivityCall {
        id: row.id,
        queue_name: row.task_queue_name,
        workflow_run_id: WorkflowRunId::from_uuid(row.workflow_run_id),
        step_index: row.step_index as usize,
        call_key: row.call_key,
        activity: row.activity,
        input: row.input,
        attempt_count: row.attempt_count,
        attempt_number: row.attempt_count,
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
