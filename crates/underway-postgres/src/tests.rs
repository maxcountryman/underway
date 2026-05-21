use std::time::Duration as StdDuration;

use jiff::Span;
use sqlx::PgPool;
use underway_core::{
    backend::{
        ActivityCallCommand, Backend, BackendTransaction, EnqueueTask, SchedulerLease,
        TransitionResult,
    },
    contracts as core_contracts,
    task::{RetryPolicy, TaskConfig},
    WorkflowRunId,
};

use crate::PostgresBackend;

async fn setup(pool: PgPool) -> PostgresBackend {
    let backend = PostgresBackend::from_pool(pool);
    backend.run_migrations().await.unwrap();
    backend
}

async fn sleep_for(span: Span) -> Result<(), jiff::Error> {
    tokio::time::sleep(StdDuration::try_from(span)?).await;
    Ok(())
}

#[sqlx::test(migrations = false)]
async fn satisfies_transaction_commit_and_rollback_contract(pool: PgPool) {
    core_contracts::transaction_commit_and_rollback(setup(pool).await)
        .await
        .unwrap();
}

#[sqlx::test(migrations = false)]
async fn satisfies_scheduler_lease_exclusivity_contract(pool: PgPool) {
    core_contracts::scheduler_lease_exclusivity(setup(pool).await)
        .await
        .unwrap();
}

#[sqlx::test(migrations = false)]
async fn scheduler_lease_drop_allows_reacquire(pool: PgPool) {
    let backend = setup(pool).await;
    let queue_name = format!("postgres-drop-lease-{}", uuid::Uuid::new_v4());
    let lease = backend
        .try_acquire_scheduler(&queue_name)
        .await
        .unwrap()
        .unwrap();
    drop(lease);

    let mut reacquired = None;
    for _ in 0..20 {
        if let Some(lease) = backend.try_acquire_scheduler(&queue_name).await.unwrap() {
            reacquired = Some(lease);
            break;
        }
        tokio::time::sleep(StdDuration::from_millis(50)).await;
    }
    let mut reacquired = reacquired.unwrap();
    reacquired.release().await.unwrap();
}

#[sqlx::test(migrations = false)]
async fn concurrent_scheduler_lease_race_allows_one_winner(pool: PgPool) {
    let backend = setup(pool).await;
    let queue_name = format!("postgres-race-lease-{}", uuid::Uuid::new_v4());
    let first_backend = backend.clone();
    let second_backend = backend;
    let (first, second) = tokio::join!(
        first_backend.try_acquire_scheduler(&queue_name),
        second_backend.try_acquire_scheduler(&queue_name),
    );
    let mut leases = [first.unwrap(), second.unwrap()];
    assert_eq!(leases.iter().filter(|lease| lease.is_some()).count(), 1);
    for lease in leases.iter_mut().flatten() {
        lease.release().await.unwrap();
    }
}

#[sqlx::test(migrations = false)]
async fn satisfies_heartbeat_extends_task_lease_contract(pool: PgPool) {
    core_contracts::heartbeat_extends_task_lease(setup(pool).await, sleep_for)
        .await
        .unwrap();
}

#[sqlx::test(migrations = false)]
async fn satisfies_concurrent_worker_claim_exclusivity_contract(pool: PgPool) {
    core_contracts::concurrent_worker_claim_exclusivity(setup(pool).await)
        .await
        .unwrap();
}

#[sqlx::test(migrations = false)]
async fn satisfies_delete_expired_removes_only_terminal_work_contract(pool: PgPool) {
    core_contracts::delete_expired_removes_only_terminal_work(setup(pool).await, sleep_for)
        .await
        .unwrap();
}

#[sqlx::test(migrations = false)]
async fn satisfies_queue_ordering_and_retry_contract(pool: PgPool) {
    core_contracts::queue_ordering_and_retry(setup(pool).await)
        .await
        .unwrap();
}

#[sqlx::test(migrations = false)]
async fn satisfies_concurrency_key_exclusivity_contract(pool: PgPool) {
    core_contracts::concurrency_key_exclusivity(setup(pool).await)
        .await
        .unwrap();
}

#[sqlx::test(migrations = false)]
async fn satisfies_activity_call_idempotency_contract(pool: PgPool) {
    core_contracts::activity_call_idempotency(setup(pool).await)
        .await
        .unwrap();
}

#[sqlx::test(migrations = false)]
async fn satisfies_delayed_availability_contract(pool: PgPool) {
    core_contracts::delayed_availability(setup(pool).await, sleep_for)
        .await
        .unwrap();
}

#[sqlx::test(migrations = false)]
async fn satisfies_retry_delay_and_stale_lease_reclaim_contract(pool: PgPool) {
    core_contracts::retry_delay_and_stale_lease_reclaim(setup(pool).await, sleep_for)
        .await
        .unwrap();
}

#[sqlx::test(migrations = false)]
async fn satisfies_attempt_fencing_contract(pool: PgPool) {
    core_contracts::attempt_fencing(setup(pool).await)
        .await
        .unwrap();
}

#[sqlx::test(migrations = false)]
async fn stale_task_transition_does_not_mutate_attempt_row(pool: PgPool) {
    let backend = setup(pool.clone()).await;
    let queue_name = format!("postgres-stale-attempt-{}", uuid::Uuid::new_v4());
    let mut tx = backend.begin().await.unwrap();
    tx.create_queue(&queue_name, None).await.unwrap();
    tx.enqueue_task(
        &queue_name,
        EnqueueTask {
            input: serde_json::json!("fenced"),
            config: TaskConfig::builder().build().unwrap(),
            delay: Span::new(),
        },
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();

    let mut tx = backend.begin().await.unwrap();
    let first = tx.claim_task(&queue_name).await.unwrap().unwrap();
    assert_eq!(
        tx.retry_task(&first, Span::new()).await.unwrap(),
        TransitionResult::Applied
    );
    tx.commit().await.unwrap();

    let mut tx = backend.begin().await.unwrap();
    let second = tx.claim_task(&queue_name).await.unwrap().unwrap();
    assert_eq!(second.attempt_number, 2);
    assert_eq!(
        tx.mark_task_succeeded(&first).await.unwrap(),
        TransitionResult::Stale
    );
    tx.commit().await.unwrap();

    let state: String = sqlx::query_scalar(
        r#"
        select state::text
        from underway.task_attempt
        where task_queue_name = $1
          and task_id = $2
          and attempt_number = $3
        "#,
    )
    .bind(&queue_name)
    .bind(first.id.as_uuid())
    .bind(first.attempt_number)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(state, "failed");
}

#[sqlx::test(migrations = false)]
async fn satisfies_dlq_move_contract(pool: PgPool) {
    core_contracts::dlq_move(setup(pool).await).await.unwrap();
}

#[sqlx::test(migrations = false)]
async fn satisfies_failed_task_moves_to_claimable_dlq_contract(pool: PgPool) {
    core_contracts::failed_task_moves_to_claimable_dlq(setup(pool).await)
        .await
        .unwrap();
}

#[sqlx::test(migrations = false)]
async fn satisfies_schedule_round_trip_contract(pool: PgPool) {
    core_contracts::schedule_round_trip(setup(pool).await)
        .await
        .unwrap();
}

#[sqlx::test(migrations = false)]
async fn satisfies_cancellation_contract(pool: PgPool) {
    core_contracts::cancellation(setup(pool).await)
        .await
        .unwrap();
}

#[sqlx::test(migrations = false)]
async fn satisfies_activity_retry_and_replay_fencing_contract(pool: PgPool) {
    core_contracts::activity_retry_and_replay_fencing(setup(pool).await, sleep_for)
        .await
        .unwrap();
}

#[sqlx::test(migrations = false)]
async fn stale_activity_reclaim_finalizes_previous_attempt(pool: PgPool) {
    let backend = setup(pool.clone()).await;
    let queue_name = format!("postgres-stale-activity-{}", uuid::Uuid::new_v4());
    let workflow_run_id = WorkflowRunId::new();
    let mut tx = backend.begin().await.unwrap();
    tx.create_queue(&queue_name, None).await.unwrap();
    tx.record_activity_call(
        &queue_name,
        workflow_run_id,
        0,
        ActivityCallCommand {
            call_key: "activity:0:0".to_string(),
            activity: "activity".to_string(),
            input: serde_json::json!("input"),
            timeout: Span::new(),
            retry_policy: RetryPolicy {
                max_attempts: 2,
                ..RetryPolicy::default()
            },
        },
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();

    let mut tx = backend.begin().await.unwrap();
    let first = tx
        .claim_activity_call(&queue_name, &["activity".to_string()])
        .await
        .unwrap()
        .unwrap();
    tx.commit().await.unwrap();

    sqlx::query(
        "update underway.activity_call set lease_expires_at = now() - interval '1 second' where \
         id = $1",
    )
    .bind(first.id)
    .execute(&pool)
    .await
    .unwrap();

    let mut tx = backend.begin().await.unwrap();
    let second = tx
        .claim_activity_call(&queue_name, &["activity".to_string()])
        .await
        .unwrap()
        .unwrap();
    tx.commit().await.unwrap();
    assert_eq!(second.attempt_number, 2);

    let first_attempt_state: String = sqlx::query_scalar(
        r#"
        select state::text
        from underway.activity_call_attempt
        where activity_call_id = $1 and attempt_number = 1
        "#,
    )
    .bind(first.id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(first_attempt_state, "failed");
}

#[sqlx::test(migrations = false)]
async fn exhausted_stale_activity_fails_and_wakes_workflow(pool: PgPool) {
    let backend = setup(pool.clone()).await;
    let queue_name = format!("postgres-exhausted-activity-{}", uuid::Uuid::new_v4());
    let workflow_run_id = WorkflowRunId::new();
    let mut tx = backend.begin().await.unwrap();
    tx.create_queue(&queue_name, None).await.unwrap();
    tx.enqueue_task(
        &queue_name,
        EnqueueTask {
            input: serde_json::json!({
                "step_index": 0,
                "step_input": {},
                "workflow_run_id": workflow_run_id.to_string(),
            }),
            config: TaskConfig::builder().build().unwrap(),
            delay: Span::new(),
        },
    )
    .await
    .unwrap();
    let workflow_task = tx.claim_task(&queue_name).await.unwrap().unwrap();
    assert_eq!(
        tx.mark_task_waiting(&workflow_task).await.unwrap(),
        TransitionResult::Applied
    );
    tx.record_activity_call(
        &queue_name,
        workflow_run_id,
        0,
        ActivityCallCommand {
            call_key: "activity:0:0".to_string(),
            activity: "activity".to_string(),
            input: serde_json::json!("input"),
            timeout: Span::new(),
            retry_policy: RetryPolicy {
                max_attempts: 1,
                ..RetryPolicy::default()
            },
        },
    )
    .await
    .unwrap();
    tx.commit().await.unwrap();

    let mut tx = backend.begin().await.unwrap();
    let activity = tx
        .claim_activity_call(&queue_name, &["activity".to_string()])
        .await
        .unwrap()
        .unwrap();
    tx.commit().await.unwrap();

    sqlx::query(
        "update underway.activity_call set lease_expires_at = now() - interval '1 second' where \
         id = $1",
    )
    .bind(activity.id)
    .execute(&pool)
    .await
    .unwrap();

    let mut tx = backend.begin().await.unwrap();
    let exhausted = tx
        .claim_activity_call(&queue_name, &["activity".to_string()])
        .await
        .unwrap();
    assert!(exhausted.is_none());
    let resumed = tx.claim_task(&queue_name).await.unwrap().unwrap();
    tx.commit().await.unwrap();
    assert_eq!(resumed.attempt_number, 2);

    let activity_state: String =
        sqlx::query_scalar("select state::text from underway.activity_call where id = $1")
            .bind(activity.id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(activity_state, "failed");

    let attempt_state: String = sqlx::query_scalar(
        r#"
        select state::text
        from underway.activity_call_attempt
        where activity_call_id = $1 and attempt_number = 1
        "#,
    )
    .bind(activity.id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(attempt_state, "failed");
}

#[sqlx::test(migrations = false)]
async fn satisfies_workflow_wait_and_resume_contract(pool: PgPool) {
    core_contracts::workflow_wait_and_resume(setup(pool).await)
        .await
        .unwrap();
}
