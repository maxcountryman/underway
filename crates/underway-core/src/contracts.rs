//! Optional backend contract checks.
//!
//! This module is compiled only for core tests or when the `contracts` feature
//! is enabled. Backend crates can enable that feature in dev-dependencies to
//! validate semantic behavior without pulling contract code into normal builds.

use std::{
    fmt::Debug,
    future::Future,
    result::Result as StdResult,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration as StdDuration,
};

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

fn expect_none_without_debug<T>(
    contract: &'static str,
    value: Option<T>,
    expected: &'static str,
) -> ContractResult {
    match value {
        None => Ok(()),
        Some(_) => Err(violation(contract, expected, "some value")),
    }
}

fn expect_applied(contract: &'static str, actual: backend::ApplyResult) -> ContractResult {
    expect_eq(contract, actual, backend::ApplyResult::Applied)
}

fn expect_stale(contract: &'static str, actual: backend::ApplyResult) -> ContractResult {
    expect_eq(contract, actual, backend::ApplyResult::Stale)
}

fn default_task_config(contract: &'static str) -> ContractResult<task::TaskConfig> {
    task::TaskConfig::builder()
        .build()
        .map_err(|err| backend_error(contract, err))
}

fn task_config_with_short_heartbeat(contract: &'static str) -> ContractResult<task::TaskConfig> {
    let retry_policy = task::RetryPolicy {
        max_attempts: 3,
        ..task::RetryPolicy::default()
    };
    task::TaskConfig::builder()
        .retry_policy(retry_policy)
        .heartbeat(100.milliseconds())
        .build()
        .map_err(|err| backend_error(contract, err))
}

fn task_config_with_ttl(
    contract: &'static str,
    ttl: Span,
    priority: i32,
) -> ContractResult<task::TaskConfig> {
    task::TaskConfig::builder()
        .ttl(ttl)
        .priority(priority)
        .build()
        .map_err(|err| backend_error(contract, err))
}

fn task_config_with_heartbeat(
    contract: &'static str,
    heartbeat: Span,
    priority: i32,
) -> ContractResult<task::TaskConfig> {
    let retry_policy = task::RetryPolicy {
        max_attempts: 3,
        ..task::RetryPolicy::default()
    };
    task::TaskConfig::builder()
        .retry_policy(retry_policy)
        .heartbeat(heartbeat)
        .priority(priority)
        .build()
        .map_err(|err| backend_error(contract, err))
}

fn workflow_input(
    workflow_run_id: workflow::WorkflowRunId,
    step_index: usize,
) -> serde_json::Value {
    serde_json::json!({
        "step_index": step_index,
        "step_input": {},
        "workflow_run_id": workflow_run_id.to_string(),
    })
}

/// Verifies rolled-back changes are not visible and committed changes are
/// visible.
pub async fn transaction_commit_and_rollback<B>(backend: B) -> ContractResult
where
    B: backend::Backend,
{
    const CONTRACT: &str = "transaction_commit_and_rollback";

    let rolled_back_queue = format!("contract-rollback-{}", task::TaskId::new());
    let mut tx = backend
        .begin()
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    tx.create_queue(&rolled_back_queue, None)
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    tx.enqueue_task(
        &rolled_back_queue,
        backend::EnqueueTask {
            input: serde_json::json!("rolled-back"),
            config: default_task_config(CONTRACT)?,
            delay: Span::new(),
        },
    )
    .await
    .map_err(|err| backend_error(CONTRACT, err))?;
    tx.rollback()
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;

    let mut tx = backend
        .begin()
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    let rolled_back = tx
        .claim_task(&rolled_back_queue)
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    expect_none(
        CONTRACT,
        rolled_back,
        "no task from rolled-back transaction",
    )?;
    tx.commit()
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;

    let committed_queue = format!("contract-commit-{}", task::TaskId::new());
    let mut tx = backend
        .begin()
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    tx.create_queue(&committed_queue, None)
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    tx.enqueue_task(
        &committed_queue,
        backend::EnqueueTask {
            input: serde_json::json!("committed"),
            config: default_task_config(CONTRACT)?,
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
    let committed = tx
        .claim_task(&committed_queue)
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    let committed = expect_some(CONTRACT, committed, "task from committed transaction")?;
    expect_eq(
        CONTRACT,
        committed.input.clone(),
        serde_json::json!("committed"),
    )?;
    tx.mark_task_succeeded(&committed)
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    tx.commit()
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;

    Ok(())
}

/// Verifies scheduler leases are exclusive until release.
pub async fn scheduler_lease_exclusivity<B>(backend: B) -> ContractResult
where
    B: backend::Backend,
{
    const CONTRACT: &str = "scheduler_lease_exclusivity";

    let queue_name = format!("contract-scheduler-{}", task::TaskId::new());
    let mut first = backend
        .try_acquire_scheduler(&queue_name)
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    let mut first = expect_some(CONTRACT, first.take(), "first scheduler lease")?;

    let second = backend
        .try_acquire_scheduler(&queue_name)
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    expect_none_without_debug(CONTRACT, second, "no second scheduler lease while held")?;

    first
        .release()
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;

    let mut reacquired = backend
        .try_acquire_scheduler(&queue_name)
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    let mut reacquired = expect_some(CONTRACT, reacquired.take(), "scheduler lease after release")?;
    reacquired
        .release()
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;

    Ok(())
}

/// Verifies heartbeats extend the active task lease.
pub async fn heartbeat_extends_task_lease<B, Advance, AdvanceFuture, AdvanceError>(
    backend: B,
    mut advance: Advance,
) -> ContractResult
where
    B: backend::Backend,
    Advance: FnMut(Span) -> AdvanceFuture,
    AdvanceFuture: Future<Output = StdResult<(), AdvanceError>>,
    AdvanceError: Display,
{
    const CONTRACT: &str = "heartbeat_extends_task_lease";

    let queue_name = format!("contract-heartbeat-{}", task::TaskId::new());
    let mut tx = backend
        .begin()
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    tx.create_queue(&queue_name, None)
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    tx.enqueue_task(
        &queue_name,
        backend::EnqueueTask {
            input: serde_json::json!("heartbeat"),
            config: task_config_with_heartbeat(CONTRACT, 500.milliseconds(), 0)?,
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
    let claimed = expect_some(CONTRACT, claimed, "initial task claim")?;
    tx.commit()
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;

    advance(100.milliseconds())
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    backend
        .record_heartbeat(&claimed)
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    advance(200.milliseconds())
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;

    let mut tx = backend
        .begin()
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    let before_extended_expiry = tx
        .claim_task(&queue_name)
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    expect_none(
        CONTRACT,
        before_extended_expiry,
        "no reclaim before heartbeat-extended lease expires",
    )?;
    tx.commit()
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;

    advance(350.milliseconds())
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;

    let mut tx = backend
        .begin()
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    let reclaimed = tx
        .claim_task(&queue_name)
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    let reclaimed = expect_some(CONTRACT, reclaimed, "reclaim after extended lease expires")?;
    expect_eq(CONTRACT, reclaimed.attempt_number, 2)?;
    tx.mark_task_succeeded(&reclaimed)
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    tx.commit()
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;

    Ok(())
}

/// Verifies concurrent workers do not execute the same claimed task.
pub async fn concurrent_worker_claim_exclusivity<B>(backend: B) -> ContractResult
where
    B: backend::Backend + Clone + Send + Sync + 'static,
{
    const CONTRACT: &str = "concurrent_worker_claim_exclusivity";

    struct CountingTask {
        executions: Arc<AtomicUsize>,
        started: Arc<tokio::sync::Notify>,
        release: Arc<tokio::sync::Notify>,
    }

    impl<B> task::Task<B> for CountingTask
    where
        B: backend::Backend,
    {
        type Input = ();
        type Output = ();

        async fn execute(
            &self,
            _tx: &mut B::Transaction<'_>,
            _input: Self::Input,
        ) -> task::Result<Self::Output> {
            self.executions.fetch_add(1, Ordering::SeqCst);
            self.started.notify_waiters();
            self.release.notified().await;
            Ok(())
        }
    }

    let executions = Arc::new(AtomicUsize::new(0));
    let started = Arc::new(tokio::sync::Notify::new());
    let release = Arc::new(tokio::sync::Notify::new());
    let task_to_run = CountingTask {
        executions: Arc::clone(&executions),
        started: Arc::clone(&started),
        release: Arc::clone(&release),
    };
    let queue_name = format!("contract-worker-{}", task::TaskId::new());
    let queue: Arc<queue::Queue<B, CountingTask>> = Arc::new(
        queue::Queue::<B, CountingTask>::builder()
            .name(&queue_name)
            .backend(backend)
            .build()
            .await
            .map_err(|err| backend_error(CONTRACT, err))?,
    );
    queue
        .enqueue(&task_to_run, &())
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;

    let worker = worker::Worker::new(queue, task_to_run);
    let first_worker = worker.clone();
    let second_worker = worker;
    let release_control = async {
        while executions.load(Ordering::SeqCst) == 0 {
            tokio::time::timeout(StdDuration::from_secs(1), started.notified())
                .await
                .map_err(|err| backend_error(CONTRACT, err))?;
        }

        tokio::time::sleep(StdDuration::from_millis(50)).await;
        expect_eq(CONTRACT, executions.load(Ordering::SeqCst), 1)?;

        release.notify_waiters();
        Ok::<_, ContractError>(())
    };
    let (first_claim, second_claim, release_result) = tokio::join!(
        first_worker.process_next_task(),
        second_worker.process_next_task(),
        release_control,
    );
    release_result?;
    let first_claim = first_claim.map_err(|err| backend_error(CONTRACT, err))?;
    let second_claim = second_claim.map_err(|err| backend_error(CONTRACT, err))?;
    let claimed_count = usize::from(first_claim.is_some()) + usize::from(second_claim.is_some());
    expect_eq(CONTRACT, claimed_count, 1)?;
    expect_eq(CONTRACT, executions.load(Ordering::SeqCst), 1)?;

    Ok(())
}

/// Verifies expired terminal work is deleted while expired live work remains
/// available to progress.
pub async fn delete_expired_removes_only_terminal_work<B, Advance, AdvanceFuture, AdvanceError>(
    backend: B,
    mut advance: Advance,
) -> ContractResult
where
    B: backend::Backend,
    Advance: FnMut(Span) -> AdvanceFuture,
    AdvanceFuture: Future<Output = StdResult<(), AdvanceError>>,
    AdvanceError: Display,
{
    const CONTRACT: &str = "delete_expired_removes_only_terminal_work";

    let queue_name = format!("contract-delete-expired-{}", task::TaskId::new());
    let mut tx = backend
        .begin()
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    tx.create_queue(&queue_name, None)
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    tx.enqueue_task(
        &queue_name,
        backend::EnqueueTask {
            input: serde_json::json!("terminal"),
            config: task_config_with_ttl(CONTRACT, 100.milliseconds(), 30)?,
            delay: Span::new(),
        },
    )
    .await
    .map_err(|err| backend_error(CONTRACT, err))?;
    tx.enqueue_task(
        &queue_name,
        backend::EnqueueTask {
            input: serde_json::json!("in-progress"),
            config: task_config_with_heartbeat(CONTRACT, 100.milliseconds(), 20)?
                .into_builder()
                .ttl(100.milliseconds())
                .build()
                .map_err(|err| backend_error(CONTRACT, err))?,
            delay: Span::new(),
        },
    )
    .await
    .map_err(|err| backend_error(CONTRACT, err))?;
    tx.enqueue_task(
        &queue_name,
        backend::EnqueueTask {
            input: serde_json::json!("pending"),
            config: task_config_with_ttl(CONTRACT, 100.milliseconds(), 10)?,
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
    let terminal = tx
        .claim_task(&queue_name)
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    let terminal = expect_some(CONTRACT, terminal, "terminal task claim")?;
    expect_eq(
        CONTRACT,
        terminal.input.clone(),
        serde_json::json!("terminal"),
    )?;
    tx.mark_task_succeeded(&terminal)
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;

    let in_progress = tx
        .claim_task(&queue_name)
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    let in_progress = expect_some(CONTRACT, in_progress, "in-progress task claim")?;
    expect_eq(
        CONTRACT,
        in_progress.input.clone(),
        serde_json::json!("in-progress"),
    )?;
    tx.commit()
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;

    advance(150.milliseconds())
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;

    let mut tx = backend
        .begin()
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    let deleted = tx
        .delete_expired()
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    expect_eq(CONTRACT, deleted, 1)?;

    let reclaimed = tx
        .claim_task(&queue_name)
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    let reclaimed = expect_some(CONTRACT, reclaimed, "expired in-progress task remains")?;
    expect_eq(
        CONTRACT,
        reclaimed.input.clone(),
        serde_json::json!("in-progress"),
    )?;
    expect_eq(CONTRACT, reclaimed.attempt_number, 2)?;
    tx.mark_task_succeeded(&reclaimed)
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;

    let pending = tx
        .claim_task(&queue_name)
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    let pending = expect_some(CONTRACT, pending, "expired pending task remains")?;
    expect_eq(
        CONTRACT,
        pending.input.clone(),
        serde_json::json!("pending"),
    )?;
    tx.mark_task_succeeded(&pending)
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    tx.commit()
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;

    Ok(())
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
    tx.record_activity_call(&queue_name, workflow_run_id, 0, command.clone())
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    tx.record_activity_call(&queue_name, workflow_run_id, 0, command)
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

/// Verifies delayed task availability. The `advance` callback lets
/// deterministic backends move logical time while real-time backends can sleep.
pub async fn delayed_availability<B, Advance, AdvanceFuture, AdvanceError>(
    backend: B,
    mut advance: Advance,
) -> ContractResult
where
    B: backend::Backend,
    Advance: FnMut(Span) -> AdvanceFuture,
    AdvanceFuture: Future<Output = StdResult<(), AdvanceError>>,
    AdvanceError: Display,
{
    const CONTRACT: &str = "delayed_availability";

    let queue_name = format!("contract-delay-{}", task::TaskId::new());
    let mut tx = backend
        .begin()
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    tx.create_queue(&queue_name, None)
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    tx.enqueue_task(
        &queue_name,
        backend::EnqueueTask {
            input: serde_json::json!("delayed"),
            config: default_task_config(CONTRACT)?,
            delay: 100.milliseconds(),
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
    let unavailable = tx
        .claim_task(&queue_name)
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    expect_none(CONTRACT, unavailable, "no claim before delay elapses")?;
    tx.commit()
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;

    advance(120.milliseconds())
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
    let claimed = expect_some(CONTRACT, claimed, "claim after delay elapses")?;
    expect_eq(
        CONTRACT,
        claimed.input.clone(),
        serde_json::json!("delayed"),
    )?;
    tx.mark_task_succeeded(&claimed)
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    tx.commit()
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;

    Ok(())
}

/// Verifies retry delay semantics and stale lease reclaim attempt numbering.
pub async fn retry_delay_and_stale_lease_reclaim<B, Advance, AdvanceFuture, AdvanceError>(
    backend: B,
    mut advance: Advance,
) -> ContractResult
where
    B: backend::Backend,
    Advance: FnMut(Span) -> AdvanceFuture,
    AdvanceFuture: Future<Output = StdResult<(), AdvanceError>>,
    AdvanceError: Display,
{
    const CONTRACT: &str = "retry_delay_and_stale_lease_reclaim";

    let queue_name = format!("contract-reclaim-{}", task::TaskId::new());
    let mut tx = backend
        .begin()
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    tx.create_queue(&queue_name, None)
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    tx.enqueue_task(
        &queue_name,
        backend::EnqueueTask {
            input: serde_json::json!("reclaim"),
            config: task_config_with_short_heartbeat(CONTRACT)?,
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
    let first = tx
        .claim_task(&queue_name)
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    let first = expect_some(CONTRACT, first, "first claim")?;
    expect_eq(CONTRACT, first.attempt_number, 1)?;
    tx.record_task_failure(&first, &task::Error::Retryable("retry".to_string()))
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    tx.retry_task(&first, 100.milliseconds())
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    tx.commit()
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;

    let mut tx = backend
        .begin()
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    let unavailable = tx
        .claim_task(&queue_name)
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    expect_none(CONTRACT, unavailable, "no claim during retry backoff")?;
    tx.commit()
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;

    advance(120.milliseconds())
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;

    let mut tx = backend
        .begin()
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    let second = tx
        .claim_task(&queue_name)
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    let second = expect_some(CONTRACT, second, "claim after retry backoff")?;
    expect_eq(CONTRACT, second.attempt_number, 2)?;
    tx.commit()
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;

    advance(120.milliseconds())
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;

    let mut tx = backend
        .begin()
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    let reclaimed = tx
        .claim_task(&queue_name)
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    let reclaimed = expect_some(CONTRACT, reclaimed, "stale lease reclaim")?;
    expect_eq(CONTRACT, reclaimed.attempt_number, 3)?;
    tx.mark_task_succeeded(&reclaimed)
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    tx.commit()
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;

    Ok(())
}

/// Verifies stale attempts cannot complete or hide the current attempt.
pub async fn attempt_fencing<B>(backend: B) -> ContractResult
where
    B: backend::Backend,
{
    const CONTRACT: &str = "attempt_fencing";

    let queue_name = format!("contract-fencing-{}", task::TaskId::new());
    let mut tx = backend
        .begin()
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    tx.create_queue(&queue_name, None)
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    tx.enqueue_task(
        &queue_name,
        backend::EnqueueTask {
            input: serde_json::json!("fenced"),
            config: task_config_with_short_heartbeat(CONTRACT)?,
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
    let first = tx
        .claim_task(&queue_name)
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    let first = expect_some(CONTRACT, first, "first claim")?;
    let applied = tx
        .retry_task(&first, Span::new())
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    expect_applied(CONTRACT, applied)?;
    tx.commit()
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;

    let mut tx = backend
        .begin()
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    let second = tx
        .claim_task(&queue_name)
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    let second = expect_some(CONTRACT, second, "second claim")?;
    expect_eq(CONTRACT, second.attempt_number, 2)?;

    let stale = tx
        .mark_task_succeeded(&first)
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    expect_stale(CONTRACT, stale)?;
    let duplicate = tx
        .claim_task(&queue_name)
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    expect_none(
        CONTRACT,
        duplicate,
        "no duplicate claim after stale completion attempt",
    )?;

    let applied = tx
        .record_task_failure(&second, &task::Error::Retryable("retry".to_string()))
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    expect_applied(CONTRACT, applied)?;
    let applied = tx
        .retry_task(&second, Span::new())
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    expect_applied(CONTRACT, applied)?;
    tx.commit()
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;

    let mut tx = backend
        .begin()
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    let third = tx
        .claim_task(&queue_name)
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    let third = expect_some(CONTRACT, third, "third claim after current retry")?;
    expect_eq(CONTRACT, third.attempt_number, 3)?;
    tx.mark_task_succeeded(&third)
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    tx.commit()
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;

    Ok(())
}

/// Verifies moving a pending task to a DLQ removes it from the source queue and
/// leaves it claimable from the DLQ.
pub async fn dlq_move<B>(backend: B) -> ContractResult
where
    B: backend::Backend,
{
    const CONTRACT: &str = "dlq_move";

    let queue_name = format!("contract-dlq-{}", task::TaskId::new());
    let dlq_name = format!("{queue_name}-dead");
    let mut tx = backend
        .begin()
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    tx.create_queue(&queue_name, Some(&dlq_name))
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    let task_id = tx
        .enqueue_task(
            &queue_name,
            backend::EnqueueTask {
                input: serde_json::json!("dead-letter"),
                config: default_task_config(CONTRACT)?,
                delay: Span::new(),
            },
        )
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    let applied = tx
        .move_task_to_dlq(task_id, &queue_name, &dlq_name)
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    expect_applied(CONTRACT, applied)?;
    tx.commit()
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;

    let mut tx = backend
        .begin()
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    let original = tx
        .claim_task(&queue_name)
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    expect_none(
        CONTRACT,
        original,
        "no claim from source queue after DLQ move",
    )?;
    let moved = tx
        .claim_task(&dlq_name)
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    let moved = expect_some(CONTRACT, moved, "claim from DLQ after move")?;
    expect_eq(
        CONTRACT,
        moved.input.clone(),
        serde_json::json!("dead-letter"),
    )?;
    tx.mark_task_succeeded(&moved)
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    tx.commit()
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;

    Ok(())
}

/// Verifies a failed task moved to a DLQ is pending, claimable work.
pub async fn failed_task_moves_to_claimable_dlq<B>(backend: B) -> ContractResult
where
    B: backend::Backend,
{
    const CONTRACT: &str = "failed_task_moves_to_claimable_dlq";

    let queue_name = format!("contract-failed-dlq-{}", task::TaskId::new());
    let dlq_name = format!("{queue_name}-dead");
    let mut tx = backend
        .begin()
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    tx.create_queue(&queue_name, Some(&dlq_name))
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    tx.enqueue_task(
        &queue_name,
        backend::EnqueueTask {
            input: serde_json::json!("failed-dead-letter"),
            config: default_task_config(CONTRACT)?,
            delay: Span::new(),
        },
    )
    .await
    .map_err(|err| backend_error(CONTRACT, err))?;
    let claimed = tx
        .claim_task(&queue_name)
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    let claimed = expect_some(CONTRACT, claimed, "claim task before failure")?;
    let applied = tx
        .record_task_failure(&claimed, &task::Error::Fatal("failed".to_string()))
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    expect_applied(CONTRACT, applied)?;
    let applied = tx
        .move_task_to_dlq(claimed.id, &queue_name, &dlq_name)
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    expect_applied(CONTRACT, applied)?;
    tx.commit()
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;

    let mut tx = backend
        .begin()
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    let original = tx
        .claim_task(&queue_name)
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    expect_none(
        CONTRACT,
        original,
        "no claim from source queue after failed DLQ move",
    )?;
    let moved = tx
        .claim_task(&dlq_name)
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    let moved = expect_some(CONTRACT, moved, "failed task claim from DLQ")?;
    expect_eq(
        CONTRACT,
        moved.input.clone(),
        serde_json::json!("failed-dead-letter"),
    )?;
    let applied = tx
        .mark_task_succeeded(&moved)
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    expect_applied(CONTRACT, applied)?;
    tx.commit()
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;

    Ok(())
}

/// Verifies schedule storage, replacement, and removal.
pub async fn schedule_round_trip<B>(backend: B) -> ContractResult
where
    B: backend::Backend,
{
    const CONTRACT: &str = "schedule_round_trip";

    let queue_name = format!("contract-schedule-{}", task::TaskId::new());
    let schedule = scheduler::ZonedSchedule::new("* * * * * * *", "UTC")
        .map_err(|err| backend_error(CONTRACT, err))?;
    let input = serde_json::json!({"scheduled": true});

    let mut tx = backend
        .begin()
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    tx.create_queue(&queue_name, None)
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    tx.schedule_task(&queue_name, schedule.clone(), input.clone())
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    let stored = tx
        .load_task_schedule(&queue_name)
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    let stored = expect_some(CONTRACT, stored, "stored schedule")?;
    expect_eq(CONTRACT, stored.schedule.cron_expr(), schedule.cron_expr())?;
    expect_eq(CONTRACT, stored.schedule.iana_name(), schedule.iana_name())?;
    expect_eq(CONTRACT, stored.input, input)?;
    tx.unschedule_task(&queue_name)
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    let removed = tx
        .load_task_schedule(&queue_name)
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    expect_none(CONTRACT, removed, "no schedule after unschedule")?;
    tx.commit()
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;

    Ok(())
}

/// Verifies cancellation of pending workflow work by workflow run ID.
pub async fn cancellation<B>(backend: B) -> ContractResult
where
    B: backend::Backend,
{
    const CONTRACT: &str = "cancellation";

    let queue_name = format!("contract-cancel-{}", task::TaskId::new());
    let workflow_run_id = workflow::WorkflowRunId::new();
    let mut tx = backend
        .begin()
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    tx.create_queue(&queue_name, None)
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    tx.enqueue_task(
        &queue_name,
        backend::EnqueueTask {
            input: workflow_input(workflow_run_id, 0),
            config: default_task_config(CONTRACT)?,
            delay: Span::new(),
        },
    )
    .await
    .map_err(|err| backend_error(CONTRACT, err))?;
    let cancelled = tx
        .cancel_workflow(&queue_name, workflow_run_id)
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    expect_eq(CONTRACT, cancelled, true)?;
    let cancelled_again = tx
        .cancel_workflow(&queue_name, workflow_run_id)
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    expect_eq(CONTRACT, cancelled_again, false)?;
    let claimed = tx
        .claim_task(&queue_name)
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    expect_none(CONTRACT, claimed, "no claim after cancellation")?;
    tx.commit()
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;

    Ok(())
}

/// Verifies activity retry delay, replay idempotency, and stale activity claim
/// fencing.
pub async fn activity_retry_and_replay_fencing<B, Advance, AdvanceFuture, AdvanceError>(
    backend: B,
    mut advance: Advance,
) -> ContractResult
where
    B: backend::Backend,
    Advance: FnMut(Span) -> AdvanceFuture,
    AdvanceFuture: Future<Output = StdResult<(), AdvanceError>>,
    AdvanceError: Display,
{
    const CONTRACT: &str = "activity_retry_and_replay_fencing";

    let queue_name = format!("contract-activity-retry-{}", task::TaskId::new());
    let workflow_run_id = workflow::WorkflowRunId::new();
    let command = backend::ActivityCallCommand {
        call_key: "activity:0:0".to_string(),
        activity: "activity".to_string(),
        input: serde_json::json!({"original": true}),
        timeout: 100.milliseconds(),
        retry_policy: task::RetryPolicy::default(),
    };
    let conflicting = backend::ActivityCallCommand {
        call_key: command.call_key.clone(),
        activity: "other-activity".to_string(),
        input: serde_json::json!({"original": false}),
        timeout: 100.milliseconds(),
        retry_policy: task::RetryPolicy::default(),
    };

    let mut tx = backend
        .begin()
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    tx.create_queue(&queue_name, None)
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    tx.record_activity_call(&queue_name, workflow_run_id, 0, command)
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    tx.record_activity_call(&queue_name, workflow_run_id, 0, conflicting)
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    let records = tx
        .load_activity_calls(&queue_name, workflow_run_id, 0)
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    expect_eq(CONTRACT, records.len(), 1)?;
    expect_eq(
        CONTRACT,
        records[0].activity.clone(),
        "activity".to_string(),
    )?;
    expect_eq(
        CONTRACT,
        records[0].input.clone(),
        serde_json::json!({"original": true}),
    )?;
    tx.commit()
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;

    let mut tx = backend
        .begin()
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    let first = tx
        .claim_activity_call(&queue_name, &["activity".to_string()])
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    let first = expect_some(CONTRACT, first, "first activity claim")?;
    expect_eq(CONTRACT, first.attempt_number, 1)?;
    let applied = tx
        .retry_activity_call(
            &first,
            activity::Error::retryable("retry", "try again"),
            100.milliseconds(),
        )
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    expect_applied(CONTRACT, applied)?;
    tx.commit()
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;

    let mut tx = backend
        .begin()
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    let unavailable = tx
        .claim_activity_call(&queue_name, &["activity".to_string()])
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    expect_none(
        CONTRACT,
        unavailable,
        "no activity claim during retry delay",
    )?;
    tx.commit()
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;

    advance(120.milliseconds())
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;

    let mut tx = backend
        .begin()
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    let second = tx
        .claim_activity_call(&queue_name, &["activity".to_string()])
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    let second = expect_some(CONTRACT, second, "second activity claim")?;
    expect_eq(CONTRACT, second.attempt_number, 2)?;
    let stale = tx
        .complete_activity_call(&first, serde_json::json!("stale"))
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    expect_stale(CONTRACT, stale)?;
    let applied = tx
        .complete_activity_call(&second, serde_json::json!({"ok": true}))
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    expect_applied(CONTRACT, applied)?;
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

/// Verifies exhausted stale activity calls fail and wake waiting workflow work.
pub async fn activity_stale_exhaustion_wakes_workflow<B, Advance, AdvanceFuture, AdvanceError>(
    backend: B,
    mut advance: Advance,
) -> ContractResult
where
    B: backend::Backend,
    Advance: FnMut(Span) -> AdvanceFuture,
    AdvanceFuture: Future<Output = StdResult<(), AdvanceError>>,
    AdvanceError: Display,
{
    const CONTRACT: &str = "activity_stale_exhaustion_wakes_workflow";

    let queue_name = format!("contract-activity-exhausted-{}", task::TaskId::new());
    let workflow_run_id = workflow::WorkflowRunId::new();
    let retry_policy = task::RetryPolicy {
        max_attempts: 1,
        ..task::RetryPolicy::default()
    };
    let command = backend::ActivityCallCommand {
        call_key: "activity:0:0".to_string(),
        activity: "activity".to_string(),
        input: serde_json::json!({"stale": true}),
        timeout: 1.milliseconds(),
        retry_policy,
    };

    let mut tx = backend
        .begin()
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    tx.create_queue(&queue_name, None)
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    tx.enqueue_task(
        &queue_name,
        backend::EnqueueTask {
            input: workflow_input(workflow_run_id, 0),
            config: default_task_config(CONTRACT)?,
            delay: Span::new(),
        },
    )
    .await
    .map_err(|err| backend_error(CONTRACT, err))?;
    let workflow_task = tx
        .claim_task(&queue_name)
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    let workflow_task = expect_some(CONTRACT, workflow_task, "workflow task claim")?;
    let applied = tx
        .mark_task_waiting(&workflow_task)
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    expect_applied(CONTRACT, applied)?;
    tx.record_activity_call(&queue_name, workflow_run_id, 0, command)
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    tx.commit()
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;

    let mut tx = backend
        .begin()
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    let first = tx
        .claim_activity_call(&queue_name, &["activity".to_string()])
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    let first = expect_some(CONTRACT, first, "first activity claim")?;
    expect_eq(CONTRACT, first.attempt_number, 1)?;
    tx.commit()
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;

    advance(31.seconds())
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;

    let mut tx = backend
        .begin()
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    let exhausted = tx
        .claim_activity_call(&queue_name, &["activity".to_string()])
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    expect_none(
        CONTRACT,
        exhausted,
        "no reclaim after activity max attempts",
    )?;
    let records = tx
        .load_activity_calls(&queue_name, workflow_run_id, 0)
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    expect_eq(CONTRACT, records.len(), 1)?;
    expect_eq(CONTRACT, records[0].state, activity::CallState::Failed)?;
    let resumed = tx
        .claim_task(&queue_name)
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    let resumed = expect_some(CONTRACT, resumed, "workflow task woken by activity failure")?;
    expect_eq(CONTRACT, resumed.attempt_number, 2)?;
    tx.commit()
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;

    Ok(())
}

/// Verifies waiting workflow tasks can be woken for the exact run and step.
pub async fn workflow_wait_and_resume<B>(backend: B) -> ContractResult
where
    B: backend::Backend,
{
    const CONTRACT: &str = "workflow_wait_and_resume";

    let queue_name = format!("contract-wait-{}", task::TaskId::new());
    let workflow_run_id = workflow::WorkflowRunId::new();
    let mut tx = backend
        .begin()
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    tx.create_queue(&queue_name, None)
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    tx.enqueue_task(
        &queue_name,
        backend::EnqueueTask {
            input: workflow_input(workflow_run_id, 0),
            config: default_task_config(CONTRACT)?,
            delay: Span::new(),
        },
    )
    .await
    .map_err(|err| backend_error(CONTRACT, err))?;
    let claimed = tx
        .claim_task(&queue_name)
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    let claimed = expect_some(CONTRACT, claimed, "claim workflow task")?;
    tx.mark_task_waiting(&claimed)
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    let unavailable = tx
        .claim_task(&queue_name)
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    expect_none(CONTRACT, unavailable, "no claim while workflow is waiting")?;
    tx.wake_workflow_task(&queue_name, workflow_run_id, 1)
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    let wrong_step = tx
        .claim_task(&queue_name)
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    expect_none(CONTRACT, wrong_step, "wrong step does not wake workflow")?;
    tx.wake_workflow_task(&queue_name, workflow_run_id, 0)
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    let resumed = tx
        .claim_task(&queue_name)
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    let resumed = expect_some(CONTRACT, resumed, "claim after workflow wake")?;
    expect_eq(CONTRACT, resumed.attempt_number, 2)?;
    tx.mark_task_succeeded(&resumed)
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;
    tx.commit()
        .await
        .map_err(|err| backend_error(CONTRACT, err))?;

    Ok(())
}
