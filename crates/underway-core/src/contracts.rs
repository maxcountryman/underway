//! Optional backend contract checks.
//!
//! This module is compiled only for core tests or when the `contracts` feature
//! is enabled. Backend crates can enable that feature in dev-dependencies to
//! validate semantic behavior without pulling contract code into normal builds.

use std::{fmt::Debug, result::Result as StdResult};

use super::*;

/// Result type for backend contract checks.
pub type ContractResult<T = ()> = StdResult<T, ContractError>;

/// Failure produced by a backend contract check.
#[derive(Debug, thiserror::Error)]
pub enum ContractError {
    /// A backend operation failed while exercising the contract.
    #[error("backend error while checking `{contract}`: {message}")]
    Backend {
        /// Contract being checked.
        contract: &'static str,
        /// Backend error message.
        message: String,
    },

    /// The backend violated an expected semantic rule.
    #[error("contract `{contract}` failed: expected {expected}, got {actual}")]
    Violation {
        /// Contract being checked.
        contract: &'static str,
        /// Expected state or value.
        expected: String,
        /// Actual state or value.
        actual: String,
    },
}

fn backend_error(contract: &'static str, err: impl Display) -> ContractError {
    ContractError::Backend {
        contract,
        message: err.to_string(),
    }
}

fn violation(
    contract: &'static str,
    expected: impl Into<String>,
    actual: impl Into<String>,
) -> ContractError {
    ContractError::Violation {
        contract,
        expected: expected.into(),
        actual: actual.into(),
    }
}

fn expect_eq<T>(contract: &'static str, actual: T, expected: T) -> ContractResult
where
    T: Debug + PartialEq,
{
    if actual == expected {
        Ok(())
    } else {
        Err(violation(
            contract,
            format!("{expected:?}"),
            format!("{actual:?}"),
        ))
    }
}

fn expect_some<T>(
    contract: &'static str,
    value: Option<T>,
    expected: &'static str,
) -> ContractResult<T> {
    value.ok_or_else(|| violation(contract, expected, "none"))
}

fn expect_none<T>(
    contract: &'static str,
    value: Option<T>,
    expected: &'static str,
) -> ContractResult
where
    T: Debug,
{
    match value {
        None => Ok(()),
        Some(actual) => Err(violation(contract, expected, format!("{actual:?}"))),
    }
}

/// Verifies priority ordering, FIFO ordering within equal priority, retry, and
/// attempt numbering.
pub async fn queue_ordering_and_retry<B>(backend: B) -> ContractResult
where
    B: backend::Backend,
{
    const CONTRACT: &str = "queue_ordering_and_retry";

    let queue_name = format!("contract-queue-{}", task::TaskId::new());
    let mut tx = backend
        .begin()
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    tx.create_queue(&queue_name, None)
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;

    let low = task::TaskConfig::builder()
        .priority(0)
        .build()
        .map_err(|err| backend_error(CONTRACT, err))?;
    let high = task::TaskConfig::builder()
        .priority(10)
        .build()
        .map_err(|err| backend_error(CONTRACT, err))?;

    tx.enqueue_task(
        &queue_name,
        backend::EnqueueTask {
            input: serde_json::json!("a"),
            config: low.clone(),
            delay: Span::new(),
        },
    )
    .await
    .map_err(|err| backend_error(CONTRACT, err))?;
    tx.enqueue_task(
        &queue_name,
        backend::EnqueueTask {
            input: serde_json::json!("b"),
            config: low,
            delay: Span::new(),
        },
    )
    .await
    .map_err(|err| backend_error(CONTRACT, err))?;
    tx.enqueue_task(
        &queue_name,
        backend::EnqueueTask {
            input: serde_json::json!("high"),
            config: high,
            delay: Span::new(),
        },
    )
    .await
    .map_err(|err| backend_error(CONTRACT, err))?;
    tx.commit()
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;

    let mut tx = backend
        .begin()
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    let claimed = tx
        .claim_task(&queue_name)
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    let claimed = expect_some(CONTRACT, claimed, "a claimable high-priority task")?;
    expect_eq(CONTRACT, claimed.input.clone(), serde_json::json!("high"))?;
    expect_eq(CONTRACT, claimed.attempt_number, 1)?;
    tx.record_task_failure(&claimed, &task::Error::Retryable("retry".to_string()))
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    tx.retry_task(&claimed, Span::new())
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    tx.commit()
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;

    let mut tx = backend
        .begin()
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    let retried = tx
        .claim_task(&queue_name)
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    let retried = expect_some(CONTRACT, retried, "a claimable retried task")?;
    expect_eq(CONTRACT, retried.input.clone(), serde_json::json!("high"))?;
    expect_eq(CONTRACT, retried.attempt_number, 2)?;
    tx.mark_task_succeeded(&retried)
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;

    let first_fifo = tx
        .claim_task(&queue_name)
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    let first_fifo = expect_some(CONTRACT, first_fifo, "first FIFO task")?;
    expect_eq(CONTRACT, first_fifo.input.clone(), serde_json::json!("a"))?;
    tx.mark_task_succeeded(&first_fifo)
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;

    let second_fifo = tx
        .claim_task(&queue_name)
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    let second_fifo = expect_some(CONTRACT, second_fifo, "second FIFO task")?;
    expect_eq(CONTRACT, second_fifo.input.clone(), serde_json::json!("b"))?;
    tx.mark_task_succeeded(&second_fifo)
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    tx.commit()
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;

    Ok(())
}

/// Verifies activity call idempotency, claiming, completion, and replay record
/// loading.
pub async fn activity_call_idempotency<B>(backend: B) -> ContractResult
where
    B: backend::Backend,
{
    const CONTRACT: &str = "activity_call_idempotency";

    let queue_name = format!("contract-activity-{}", task::TaskId::new());
    let workflow_run_id = workflow::WorkflowRunId::new();
    let mut tx = backend
        .begin()
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    tx.create_queue(&queue_name, None)
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;

    let command = backend::ActivityCallCommand {
        call_key: "activity:0:0".to_string(),
        activity: "activity".to_string(),
        input: serde_json::json!({"n": 1}),
        timeout: 15.seconds(),
        retry_policy: task::RetryPolicy::default(),
    };
    tx.insert_activity_call(&queue_name, workflow_run_id, 0, command.clone())
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    tx.insert_activity_call(&queue_name, workflow_run_id, 0, command)
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    tx.commit()
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;

    let mut tx = backend
        .begin()
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    let claimed = tx
        .claim_activity_call(&queue_name, &["activity".to_string()])
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    let claimed = expect_some(CONTRACT, claimed, "a claimable activity call")?;
    expect_eq(CONTRACT, claimed.workflow_run_id, workflow_run_id)?;
    expect_eq(CONTRACT, claimed.step_index, 0)?;
    expect_eq(CONTRACT, claimed.attempt_number, 1)?;

    let duplicate_claim = tx
        .claim_activity_call(&queue_name, &["activity".to_string()])
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    expect_none(CONTRACT, duplicate_claim, "no duplicate claim while leased")?;

    tx.complete_activity_call(&claimed, serde_json::json!({"ok": true}))
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;

    let records = tx
        .load_activity_calls(&queue_name, workflow_run_id, 0)
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    expect_eq(CONTRACT, records.len(), 1)?;
    expect_eq(CONTRACT, records[0].state, activity::CallState::Succeeded)?;
    expect_eq(
        CONTRACT,
        records[0].output.clone(),
        Some(serde_json::json!({"ok": true})),
    )?;
    tx.commit()
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;

    Ok(())
}
