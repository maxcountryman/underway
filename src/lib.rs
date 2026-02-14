//! # Underway
//!
//! ⏳ Durable background workflows on Postgres.
//!
//! # Overview
//!
//! **Underway** runs durable background workflows on the Postgres you already
//! operate. Model business flows as typed Rust steps, execute durable side
//! effects through activities, and recover cleanly across retries, restarts,
//! and deploys.
//!
//! Key Features:
//!
//! - **Recover from Failures Automatically** Workflow progress and activity
//!   intent are persisted, so work resumes after restarts, crashes, and
//!   deploys.
//! - **Use the Postgres You Already Run** No extra broker or orchestration
//!   layer; queue coordination and task claiming happen in PostgreSQL.
//! - **Model Business Flows in Typed Rust** Build multi-step workflows with
//!   compile-time checked step inputs, outputs, and transitions.
//! - **Make Side Effects Durable and Replay-Safe** `Context::call` and
//!   `Context::emit` persist side-effect intent, and registered activities are
//!   compile-time checked.
//! - **Operate with Production Controls** Transactional `*_using` APIs,
//!   retries, cron scheduling, heartbeats, and fencing support reliable
//!   high-concurrency execution.
//!
//! ## Quick Start
//!
//! The example below shows the smallest runtime-first flow: run migrations,
//! build a workflow, enqueue input, and start the runtime.
//!
//! ```rust,no_run
//! use serde::{Deserialize, Serialize};
//! use sqlx::PgPool;
//! use underway::{To, Workflow};
//!
//! #[derive(Deserialize, Serialize)]
//! struct SendWelcomeEmail {
//!     user_id: i64,
//! }
//!
//! # use tokio::runtime::Runtime as TokioRuntime;
//! # fn main() {
//! # let rt = TokioRuntime::new().unwrap();
//! # rt.block_on(async {
//! # let pool = PgPool::connect(&std::env::var("DATABASE_URL")?).await?;
//! underway::run_migrations(&pool).await?;
//!
//! let workflow = Workflow::builder()
//!     .step(|_cx, SendWelcomeEmail { user_id }| async move {
//!         println!("sending welcome email to user {user_id}");
//!         To::done()
//!     })
//!     .name("send-welcome-email")
//!     .pool(pool)
//!     .build()
//!     .await?;
//!
//! workflow.enqueue(&SendWelcomeEmail { user_id: 42 }).await?;
//!
//! let runtime_handle = workflow.runtime().start();
//! runtime_handle.shutdown().await?;
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! # });
//! # }
//! ```
//!
//! This same pattern scales to multi-step workflows by returning `To::next`.
//! For durable side effects, see the Workflow Activities section below.
//!
//! ## Workflow Activities
//!
//! For durable side effects beyond plain step chaining, workflows can invoke
//! activities through
//! [`workflow::Context::call`](crate::workflow::Context::call)
//! and [`workflow::Context::emit`](crate::workflow::Context::emit).
//!
//! - `call` is request/response and may suspend a step until the activity
//!   completes.
//! - `emit` is fire-and-forget and records intent durably.
//!
//! Activity handlers are registered on [`Workflow::builder`] via
//! [`workflow::Builder::activity`](crate::workflow::Builder::activity), and
//! then run by the workflow's runtime.
//!
//! ```rust,no_run
//! use serde::{Deserialize, Serialize};
//! use sqlx::PgPool;
//! use underway::{Activity, ActivityError, To, Workflow};
//!
//! #[derive(Deserialize, Serialize)]
//! struct Input {
//!     user_id: i64,
//! }
//!
//! struct ResolveEmail;
//!
//! impl Activity for ResolveEmail {
//!     const NAME: &'static str = "resolve-email";
//!
//!     type Input = i64;
//!     type Output = String;
//!
//!     async fn execute(&self, user_id: Self::Input) -> underway::activity::Result<Self::Output> {
//!         if user_id <= 0 {
//!             return Err(ActivityError::fatal(
//!                 "invalid_user",
//!                 "user_id must be positive",
//!             ));
//!         }
//!
//!         Ok(format!("{user_id}@example.com"))
//!     }
//! }
//!
//! # use tokio::runtime::Runtime as TokioRuntime;
//! # fn main() {
//! # let rt = TokioRuntime::new().unwrap();
//! # rt.block_on(async {
//! # let pool = PgPool::connect(&std::env::var("DATABASE_URL")?).await?;
//! let workflow = Workflow::builder()
//!     .activity(ResolveEmail)
//!     .step(|mut cx, Input { user_id }| async move {
//!         let _email: String = cx.call::<ResolveEmail, _>(&user_id).await?;
//!         To::done()
//!     })
//!     .name("resolve-email")
//!     .pool(pool)
//!     .build()
//!     .await?;
//!
//! workflow.runtime().run().await?;
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! # });
//! # }
//! ```
//!
//! # Examples
//!
//! The following examples mirror common production patterns.
//!
//! ## Build and run a workflow
//!
//! ```rust,no_run
//! use serde::{Deserialize, Serialize};
//! use sqlx::PgPool;
//! use underway::{To, Workflow};
//!
//! #[derive(Deserialize, Serialize)]
//! struct ResizeImage {
//!     asset_id: i64,
//! }
//!
//! #[derive(Deserialize, Serialize)]
//! struct PublishImage {
//!     object_key: String,
//! }
//!
//! # use tokio::runtime::Runtime as TokioRuntime;
//! # fn main() {
//! # let rt = TokioRuntime::new().unwrap();
//! # rt.block_on(async {
//! let pool = PgPool::connect(&std::env::var("DATABASE_URL")?).await?;
//! underway::run_migrations(&pool).await?;
//!
//! let workflow = Workflow::builder()
//!     .step(|_cx, ResizeImage { asset_id }| async move {
//!         let object_key = format!("images/{asset_id}.webp");
//!         To::next(PublishImage { object_key })
//!     })
//!     .step(|_cx, PublishImage { object_key }| async move {
//!         println!("Publishing {object_key}");
//!         To::done()
//!     })
//!     .name("image-pipeline")
//!     .pool(pool)
//!     .build()
//!     .await?;
//!
//! workflow.enqueue(&ResizeImage { asset_id: 42 }).await?;
//!
//! let runtime_handle = workflow.runtime().start();
//! runtime_handle.shutdown().await?;
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! # });
//! # }
//! ```
//!
//! ## Durable side effects with activities
//!
//! ```rust,no_run
//! use serde::{Deserialize, Serialize};
//! use sqlx::PgPool;
//! use underway::{Activity, ActivityError, To, Workflow};
//!
//! #[derive(Clone)]
//! struct LookupEmail {
//!     pool: PgPool,
//! }
//!
//! impl Activity for LookupEmail {
//!     const NAME: &'static str = "lookup-email";
//!
//!     type Input = i64;
//!     type Output = String;
//!
//!     async fn execute(&self, user_id: Self::Input) -> underway::activity::Result<Self::Output> {
//!         let email =
//!             sqlx::query_scalar::<_, String>("select concat('user-', $1::text, '@example.com')")
//!                 .bind(user_id)
//!                 .fetch_one(&self.pool)
//!                 .await
//!                 .map_err(|err| ActivityError::retryable("db_error", err.to_string()))?;
//!
//!         Ok(email)
//!     }
//! }
//!
//! struct TrackSignupMetric;
//!
//! impl Activity for TrackSignupMetric {
//!     const NAME: &'static str = "track-signup-metric";
//!
//!     type Input = String;
//!     type Output = ();
//!
//!     async fn execute(&self, email: Self::Input) -> underway::activity::Result<Self::Output> {
//!         println!("tracking signup metric for {email}");
//!         Ok(())
//!     }
//! }
//!
//! #[derive(Deserialize, Serialize)]
//! struct Signup {
//!     user_id: i64,
//! }
//!
//! # use tokio::runtime::Runtime as TokioRuntime;
//! # fn main() {
//! # let rt = TokioRuntime::new().unwrap();
//! # rt.block_on(async {
//! let pool = PgPool::connect(&std::env::var("DATABASE_URL")?).await?;
//! underway::run_migrations(&pool).await?;
//!
//! let workflow = Workflow::builder()
//!     .activity(LookupEmail { pool: pool.clone() })
//!     .activity(TrackSignupMetric)
//!     .step(|mut cx, Signup { user_id }| async move {
//!         let email: String = cx.call::<LookupEmail, _>(&user_id).await?;
//!         cx.emit::<TrackSignupMetric, _>(&email).await?;
//!         To::done()
//!     })
//!     .name("signup-side-effects")
//!     .pool(pool)
//!     .build()
//!     .await?;
//!
//! workflow.enqueue(&Signup { user_id: 42 }).await?;
//! workflow.runtime().run().await?;
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! # });
//! # }
//! ```
//!
//! ## Enqueue and schedule in your transaction
//!
//! ```rust,no_run
//! use serde::{Deserialize, Serialize};
//! use sqlx::PgPool;
//! use underway::{To, Workflow};
//!
//! #[derive(Deserialize, Serialize)]
//! struct TenantCleanup {
//!     tenant_id: i64,
//! }
//!
//! # use tokio::runtime::Runtime as TokioRuntime;
//! # fn main() {
//! # let rt = TokioRuntime::new().unwrap();
//! # rt.block_on(async {
//! let pool = PgPool::connect(&std::env::var("DATABASE_URL")?).await?;
//! underway::run_migrations(&pool).await?;
//!
//! let workflow = Workflow::builder()
//!     .step(|_cx, TenantCleanup { tenant_id }| async move {
//!         println!("Running cleanup for tenant {tenant_id}");
//!         To::done()
//!     })
//!     .name("tenant-cleanup")
//!     .pool(pool.clone())
//!     .build()
//!     .await?;
//!
//! let nightly = "0 2 * * *[UTC]".parse()?;
//! let tenant_id = 7;
//! let input = TenantCleanup { tenant_id };
//!
//! let mut tx = pool.begin().await?;
//!
//! sqlx::query("update app_tenant set cleanup_enabled = true where id = $1")
//!     .bind(tenant_id)
//!     .execute(&mut *tx)
//!     .await?;
//!
//! workflow.enqueue_using(&mut *tx, &input).await?;
//! workflow.schedule_using(&mut *tx, &nightly, &input).await?;
//!
//! tx.commit().await?;
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! # });
//! # }
//! ```
//!
//! # Concepts
//!
//! Underway has been designed around several core concepts, which build on one
//! another to deliver a robust background-workflow framework:
//!
//! - [Tasks](#tasks) represent a well-structured unit of work.
//! - [Workflows](#workflows) are a series of sequential steps, where each step
//!   is a [`Task`].
//! - [Runtime](#runtime) orchestrates workflow workers, schedulers, and
//!   activities.
//! - [Activities](#activities) model durable side effects for workflow steps.
//! - [Queues](#queues) provide an interface for managing task lifecycle.
//! - [Workers](#workers) interface with queues to execute tasks.
//!
//! ## Tasks
//!
//! Tasks are units of work to be executed, with clearly defined behavior and
//! input.
//!
//! This is the lowest-level concept in our design, with everything else being
//! built on top of or around this idea.
//!
//! See [`task`] for more details about tasks.
//!
//! ## Workflows
//!
//! Workflows are a series of sequential steps. Each step provides input to the
//! next step in the series.
//!
//! In most cases, applications will use workflows to define tasks instead of
//! using the `Task` trait directly.
//!
//! See [`workflow`] for more details about workflows.
//!
//! ## Runtime
//!
//! Runtime is the high-level execution entrypoint for workflows. It starts and
//! coordinates worker, scheduler, and activity-worker loops so suspended
//! workflow calls can resume automatically.
//!
//! See [`runtime`] for more details about runtime orchestration.
//!
//! ## Activities
//!
//! Activities are durable side-effect handlers invoked from workflow steps
//! through [`workflow::Context::call`](crate::workflow::Context::call) and
//! [`workflow::Context::emit`](crate::workflow::Context::emit).
//!
//! See [`activity`] for more details about activity handlers and errors.
//!
//! ## Queues
//!
//! Queues manage task lifecycle, including enqueuing and dequeuing them from
//! the database.
//!
//! See [`queue`] for more details about queues.
//!
//! ## Workers
//!
//! Workers are responsible for executing tasks. They poll the queue for new
//! tasks, and when found, try to invoke the task's execute routine.
//!
//! See [`worker`] for more details about workers.
#![warn(clippy::all, nonstandard_style, future_incompatible, missing_docs)]
#![forbid(unsafe_code)]

use sqlx::{migrate::Migrator, Acquire, Postgres};

pub use crate::{
    activity::{Activity, CallState as ActivityCallState, Error as ActivityError},
    queue::Queue,
    runtime::Runtime,
    scheduler::{Scheduler, ZonedSchedule},
    task::{Task, ToTaskResult},
    worker::Worker,
    workflow::{To, Workflow},
};

pub mod activity;
mod activity_worker;
pub mod queue;
pub mod runtime;
mod scheduler;
pub mod task;
pub mod worker;
pub mod workflow;

static MIGRATOR: Migrator = sqlx::migrate!();

/// Runs Underway migrations.
///
/// These migrations must be applied before queues, tasks, and workers can be
/// run.
///
/// A transaction is acquired via the provided connection and migrations are run
/// via this transaction.
///
/// As there is no direct support for specifying the schema under which the
/// migrations table will live, we manually specify this via the search path.
/// This ensures that migrations are isolated to underway._sqlx_migrations.
///
/// **Note**: Changes are managed within a dedicated schema, called "underway".
///
/// # Example
///
///```rust,no_run
/// # use tokio::runtime::Runtime;
/// use std::env;
///
/// use sqlx::PgPool;
///
/// # fn main() {
/// # let rt = Runtime::new().unwrap();
/// # rt.block_on(async {
/// // Set up the database connection pool.
/// let database_url = &env::var("DATABASE_URL")?;
/// let pool = PgPool::connect(database_url).await?;
///
/// // Run migrations.
/// underway::run_migrations(&pool).await?;
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// # });
/// # }
pub async fn run_migrations<'a, A>(conn: A) -> Result<(), sqlx::Error>
where
    A: Acquire<'a, Database = Postgres>,
{
    let mut tx = conn.begin().await?;

    // Ensure the 'underway' schema exists
    sqlx::query!("create schema if not exists underway;")
        .execute(&mut *tx)
        .await?;

    // Temporarily set search_path for this transaction
    sqlx::query!("set local search_path to underway;")
        .execute(&mut *tx)
        .await?;

    // Run migrations within the 'underway' schema
    MIGRATOR.run(&mut *tx).await?;

    tx.commit().await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use sqlx::PgPool;

    use super::run_migrations;

    #[sqlx::test(migrations = false)]
    async fn sanity_check_run_migrations(pool: PgPool) -> Result<(), sqlx::Error> {
        run_migrations(&pool).await?;

        let schema_exists: bool = sqlx::query_scalar!(
            r#"
            select exists (
              select 1 from pg_namespace where nspname = 'underway'
            );
            "#,
        )
        .fetch_one(&pool)
        .await?
        .unwrap();
        assert!(
            schema_exists,
            "Schema 'underway' should exist after migrations."
        );

        let migrations_table_exists: bool = sqlx::query_scalar!(
            r#"
            select exists (
                select 1 from information_schema.tables
                where table_schema = 'underway' and 
                      table_name = '_sqlx_migrations'
            );
            "#,
        )
        .fetch_one(&pool)
        .await?
        .unwrap();
        assert!(
            migrations_table_exists,
            "Migrations table should exist in 'underway' schema."
        );

        let search_path: String = sqlx::query_scalar("show search_path;")
            .fetch_one(&pool)
            .await?;

        assert!(
            !search_path.contains("underway"),
            "search_path should not include 'underway' after the transaction."
        );

        assert!(
            search_path.contains("public"),
            "Default search_path should include 'public'."
        );

        Ok(())
    }
}
