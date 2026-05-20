use std::time::Duration as StdDuration;

use jiff::Span;
use sqlx::PgPool;
use underway_core::contracts as core_contracts;

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
async fn satisfies_heartbeat_extends_task_lease_contract(pool: PgPool) {
    core_contracts::heartbeat_extends_task_lease(setup(pool).await, sleep_for)
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
async fn satisfies_dlq_move_contract(pool: PgPool) {
    core_contracts::dlq_move(setup(pool).await).await.unwrap();
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
async fn satisfies_workflow_wait_and_resume_contract(pool: PgPool) {
    core_contracts::workflow_wait_and_resume(setup(pool).await)
        .await
        .unwrap();
}
