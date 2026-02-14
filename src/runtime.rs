//! Runtime orchestration for workflows.
//!
//! [`Runtime`] is the high-level entrypoint for workflow execution.
//! It wraps a [`Workflow`] and coordinates:
//! - the workflow worker,
//! - the workflow scheduler,
//! - and the activity worker used by workflow calls/emit.
//!
//! # Why runtime?
//!
//! A workflow step may suspend while waiting on an activity call.
//! [`Runtime`] is responsible for running both sides of that interaction so a
//! suspended step can be resumed once the activity completes.
//!
//! # Example
//!
//! ```rust,no_run
//! use serde::{Deserialize, Serialize};
//! use sqlx::PgPool;
//! use underway::{Activity, ActivityError, To, Workflow};
//!
//! #[derive(Deserialize, Serialize)]
//! struct FetchUser {
//!     user_id: i64,
//! }
//!
//! struct LookupEmail;
//!
//! impl Activity for LookupEmail {
//!     const NAME: &'static str = "lookup-email";
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
//!         Ok(format!("user-{user_id}@example.com"))
//!     }
//! }
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let pool = PgPool::connect(&std::env::var("DATABASE_URL")?).await?;
//!     let workflow = Workflow::builder()
//!         .activity(LookupEmail)
//!         .step(|mut cx, FetchUser { user_id }| async move {
//!             let email: String = cx.call::<LookupEmail, _>("lookup", &user_id).await?;
//!             println!("Got email {email}");
//!             To::done()
//!         })
//!         .name("lookup-email-workflow")
//!         .pool(pool)
//!         .build()
//!         .await?;
//!
//!     workflow.runtime().run().await?;
//!     Ok(())
//! }
//! ```

use serde::Serialize;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::{
    activity_worker::{ActivityWorker, Error as ActivityWorkerError},
    scheduler::{Error as SchedulerError, Scheduler},
    worker::{Error as WorkerError, Worker},
    workflow::Workflow,
};

/// A type alias for runtime execution results.
pub type Result<T = ()> = std::result::Result<T, Error>;

/// Runtime errors.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// Error returned from worker operation.
    #[error(transparent)]
    Worker(#[from] WorkerError),

    /// Error returned from scheduler operation.
    #[error(transparent)]
    Scheduler(#[from] SchedulerError),

    /// Error returned from activity worker operation.
    #[error(transparent)]
    ActivityWorker(#[from] ActivityWorkerError),

    /// Error returned from Tokio task joins.
    #[error(transparent)]
    Join(#[from] tokio::task::JoinError),
}

/// Handle returned by [`Runtime::start`].
pub struct RuntimeHandle {
    workers: JoinSet<Result<()>>,
    shutdown_token: CancellationToken,
}

impl RuntimeHandle {
    /// Signals all runtime workers to shutdown and waits for them to terminate.
    ///
    /// This cancels worker, scheduler, and activity worker loops started by
    /// [`Runtime::start`].
    pub async fn shutdown(mut self) -> Result {
        self.shutdown_token.cancel();

        while let Some(ret) = self.workers.join_next().await {
            match ret {
                Ok(Err(err)) => return Err(err),
                Err(err) => return Err(Error::from(err)),
                _ => continue,
            }
        }

        Ok(())
    }
}

/// High-level workflow runtime.
pub struct Runtime<I, S>
where
    I: Serialize + Send + Sync + 'static,
    S: Clone + Send + Sync + 'static,
{
    workflow: Workflow<I, S>,
    activity_worker: ActivityWorker,
}

impl<I, S> Clone for Runtime<I, S>
where
    I: Serialize + Send + Sync + 'static,
    S: Clone + Send + Sync + 'static,
{
    fn clone(&self) -> Self {
        Self {
            workflow: self.workflow.clone(),
            activity_worker: self.activity_worker.clone(),
        }
    }
}

impl<I, S> Runtime<I, S>
where
    I: Serialize + Send + Sync + 'static,
    S: Clone + Send + Sync + 'static,
{
    /// Creates a runtime from a workflow.
    ///
    /// Registered activity handlers are sourced from the workflow's builder.
    pub fn new(workflow: Workflow<I, S>) -> Self {
        let activity_worker = ActivityWorker::with_registry(
            workflow.queue().pool.clone(),
            workflow.queue().name.clone(),
            workflow.activity_registry(),
        );
        Self {
            workflow,
            activity_worker,
        }
    }

    /// Returns a reference to the workflow managed by this runtime.
    pub fn workflow(&self) -> &Workflow<I, S> {
        &self.workflow
    }

    /// Creates a `Worker` for this runtime's workflow.
    ///
    /// This is useful when running worker loops manually instead of calling
    /// [`Runtime::run`] or [`Runtime::start`].
    pub fn worker(&self) -> Worker<Workflow<I, S>> {
        Worker::new(self.workflow.queue(), self.workflow.clone())
    }

    /// Creates a `Scheduler` for this runtime's workflow.
    ///
    /// This is useful when running scheduler loops manually instead of calling
    /// [`Runtime::run`] or [`Runtime::start`].
    pub fn scheduler(&self) -> Scheduler<Workflow<I, S>> {
        Scheduler::new(self.workflow.queue(), self.workflow.clone())
    }

    /// Runs this runtime to completion.
    ///
    /// This starts worker, scheduler, and activity worker loops in the current
    /// task and returns when one of them exits with an error or when all loops
    /// stop.
    pub async fn run(&self) -> Result {
        let shutdown_token = CancellationToken::new();

        let mut worker = self.worker();
        worker.set_shutdown_token(shutdown_token.clone());

        let mut scheduler = self.scheduler();
        scheduler.set_shutdown_token(shutdown_token.clone());

        let mut activity_worker = self.activity_worker.clone();
        activity_worker.set_shutdown_token(shutdown_token.clone());

        let mut workers = JoinSet::new();
        workers.spawn(async move { worker.run().await.map_err(Error::from) });
        workers.spawn(async move { scheduler.run().await.map_err(Error::from) });

        if !activity_worker.is_empty() {
            workers.spawn(async move { activity_worker.run().await.map_err(Error::from) });
        }

        while let Some(ret) = workers.join_next().await {
            match ret {
                Ok(Err(err)) => {
                    shutdown_token.cancel();
                    return Err(err);
                }
                Err(err) => {
                    shutdown_token.cancel();
                    return Err(Error::from(err));
                }
                _ => continue,
            }
        }

        Ok(())
    }

    /// Starts this runtime in background tasks and returns a handle.
    ///
    /// Use [`RuntimeHandle::shutdown`] for graceful stop.
    pub fn start(&self) -> RuntimeHandle {
        let shutdown_token = CancellationToken::new();

        let mut worker = self.worker();
        worker.set_shutdown_token(shutdown_token.clone());

        let mut scheduler = self.scheduler();
        scheduler.set_shutdown_token(shutdown_token.clone());

        let mut activity_worker = self.activity_worker.clone();
        activity_worker.set_shutdown_token(shutdown_token.clone());

        let mut workers = JoinSet::new();
        workers.spawn(async move { worker.run().await.map_err(Error::from) });
        workers.spawn(async move { scheduler.run().await.map_err(Error::from) });

        if !activity_worker.is_empty() {
            workers.spawn(async move { activity_worker.run().await.map_err(Error::from) });
        }

        RuntimeHandle {
            workers,
            shutdown_token,
        }
    }
}

impl<I, S> From<Workflow<I, S>> for Runtime<I, S>
where
    I: Serialize + Send + Sync + 'static,
    S: Clone + Send + Sync + 'static,
{
    fn from(workflow: Workflow<I, S>) -> Self {
        Self::new(workflow)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use serde::{Deserialize, Serialize};
    use sqlx::PgPool;
    use tokio::{
        sync::Mutex,
        time::{sleep, timeout, Duration},
    };

    use super::Runtime;
    use crate::{
        activity::{Activity, CallState, Result as ActivityResult},
        To, Workflow,
    };

    struct EchoActivity;

    impl Activity for EchoActivity {
        const NAME: &'static str = "echo";

        type Input = String;
        type Output = String;

        async fn execute(&self, input: Self::Input) -> ActivityResult<Self::Output> {
            Ok(format!("echo:{input}"))
        }
    }

    struct ReverseActivity;

    impl Activity for ReverseActivity {
        const NAME: &'static str = "reverse";

        type Input = String;
        type Output = String;

        async fn execute(&self, input: Self::Input) -> ActivityResult<Self::Output> {
            Ok(input.chars().rev().collect())
        }
    }

    #[derive(Debug, Deserialize, Serialize)]
    struct Step1 {
        message: String,
    }

    #[derive(Debug, Deserialize, Serialize)]
    struct Step2 {
        echoed: String,
    }

    #[derive(Debug, Deserialize, Serialize)]
    struct MessageInput {
        message: String,
    }

    #[sqlx::test]
    async fn call_suspends_then_resumes(
        pool: PgPool,
    ) -> std::result::Result<(), Box<dyn std::error::Error>> {
        let outputs = Arc::new(Mutex::new(Vec::new()));
        let outputs_step = outputs.clone();

        let workflow = Workflow::builder()
            .activity(EchoActivity)
            .step(|mut cx, Step1 { message }| async move {
                let echoed: String = cx.call::<EchoActivity, _>("echo-main", &message).await?;
                To::next(Step2 { echoed })
            })
            .step(move |_cx, Step2 { echoed }| {
                let outputs_step = outputs_step.clone();
                async move {
                    outputs_step.lock().await.push(echoed);
                    To::done()
                }
            })
            .name("runtime_call_suspends_then_resumes")
            .pool(pool.clone())
            .build()
            .await?;

        let runtime = Runtime::new(workflow);

        runtime
            .workflow()
            .enqueue(&Step1 {
                message: "hello".to_string(),
            })
            .await?;

        let handle = runtime.start();

        timeout(Duration::from_secs(10), async {
            loop {
                if outputs.lock().await.len() == 1 {
                    break;
                }

                sleep(Duration::from_millis(50)).await;
            }
        })
        .await?;

        assert_eq!(outputs.lock().await.as_slice(), ["echo:hello"]);

        let attempt_state = sqlx::query_scalar!(
            r#"
            select a.state as "state: CallState"
            from underway.activity_call_attempt a
            inner join underway.activity_call c
              on c.id = a.activity_call_id
            where c.task_queue_name = $1
              and c.call_key = $2
            order by a.attempt_number desc
            limit 1
            "#,
            runtime.workflow().queue().name,
            "echo-main",
        )
        .fetch_one(&pool)
        .await?;

        assert_eq!(attempt_state, CallState::Succeeded);

        handle.shutdown().await?;

        Ok(())
    }

    #[sqlx::test]
    async fn runtime_does_not_claim_unregistered_activity_calls_on_shared_queue(
        pool: PgPool,
    ) -> std::result::Result<(), Box<dyn std::error::Error>> {
        let queue_name = "runtime_shared_queue_activity_scope";

        let workflow = Workflow::builder()
            .activity(EchoActivity)
            .step(|mut cx, MessageInput { message }| async move {
                let _: String = cx.call::<EchoActivity, _>("shared-call", &message).await?;
                To::done()
            })
            .name(queue_name)
            .pool(pool.clone())
            .build()
            .await?;

        workflow
            .enqueue(&MessageInput {
                message: "hello".to_string(),
            })
            .await?;

        let workflow_worker = workflow.runtime().worker();
        let _ = workflow_worker.process_next_task().await?;

        let call_state = sqlx::query_scalar!(
            r#"
            select state as "state: CallState"
            from underway.activity_call
            where task_queue_name = $1
              and call_key = $2
            "#,
            queue_name,
            "shared-call",
        )
        .fetch_one(&pool)
        .await?;
        assert_eq!(call_state, CallState::Pending);

        let unrelated_workflow = Workflow::builder()
            .activity(ReverseActivity)
            .step(|_cx, _: MessageInput| async move { To::done() })
            .name(queue_name)
            .pool(pool.clone())
            .build()
            .await?;

        let handle = unrelated_workflow.runtime().start();
        sleep(Duration::from_millis(300)).await;
        handle.shutdown().await?;

        let call_state_after = sqlx::query_scalar!(
            r#"
            select state as "state: CallState"
            from underway.activity_call
            where task_queue_name = $1
              and call_key = $2
            "#,
            queue_name,
            "shared-call",
        )
        .fetch_one(&pool)
        .await?;

        assert_eq!(call_state_after, CallState::Pending);

        Ok(())
    }

    #[sqlx::test]
    async fn runtime_reclaims_stale_in_progress_activity_calls(
        pool: PgPool,
    ) -> std::result::Result<(), Box<dyn std::error::Error>> {
        let outputs = Arc::new(Mutex::new(Vec::new()));
        let outputs_step = outputs.clone();

        let workflow = Workflow::builder()
            .activity(EchoActivity)
            .step(|mut cx, Step1 { message }| async move {
                let echoed: String = cx.call::<EchoActivity, _>("echo-main", &message).await?;
                To::next(Step2 { echoed })
            })
            .step(move |_cx, Step2 { echoed }| {
                let outputs_step = outputs_step.clone();
                async move {
                    outputs_step.lock().await.push(echoed);
                    To::done()
                }
            })
            .name("runtime_reclaims_stale_in_progress_activity_calls")
            .pool(pool.clone())
            .build()
            .await?;

        workflow
            .enqueue(&Step1 {
                message: "hello".to_string(),
            })
            .await?;

        let workflow_worker = workflow.runtime().worker();
        let _ = workflow_worker.process_next_task().await?;

        let call_id = sqlx::query_scalar!(
            r#"
            select id
            from underway.activity_call
            where task_queue_name = $1
              and call_key = $2
            limit 1
            "#,
            workflow.queue().name,
            "echo-main",
        )
        .fetch_one(&pool)
        .await?;

        sqlx::query!(
            r#"
            update underway.activity_call
            set state = 'in_progress'::underway.activity_call_state,
                attempt_count = 1,
                started_at = now() - interval '2 hours',
                updated_at = now() - interval '2 hours',
                lease_expires_at = now() - interval '2 hours'
            where id = $1
            "#,
            call_id,
        )
        .execute(&pool)
        .await?;

        sqlx::query!(
            r#"
            insert into underway.activity_call_attempt (
                activity_call_id,
                attempt_number,
                state,
                started_at,
                updated_at
            ) values (
                $1,
                1,
                'in_progress'::underway.activity_call_state,
                now() - interval '2 hours',
                now() - interval '2 hours'
            )
            "#,
            call_id,
        )
        .execute(&pool)
        .await?;

        let runtime = workflow.runtime();
        let handle = runtime.start();

        timeout(Duration::from_secs(10), async {
            loop {
                if outputs.lock().await.len() == 1 {
                    break;
                }

                sleep(Duration::from_millis(50)).await;
            }
        })
        .await?;

        assert_eq!(outputs.lock().await.as_slice(), ["echo:hello"]);

        let call_state = sqlx::query_scalar!(
            r#"
            select state as "state: CallState"
            from underway.activity_call
            where id = $1
            "#,
            call_id,
        )
        .fetch_one(&pool)
        .await?;
        assert_eq!(call_state, CallState::Succeeded);

        let attempt_state = sqlx::query_scalar!(
            r#"
            select state as "state: CallState"
            from underway.activity_call_attempt
            where activity_call_id = $1
            order by attempt_number desc
            limit 1
            "#,
            call_id,
        )
        .fetch_one(&pool)
        .await?;
        assert_eq!(attempt_state, CallState::Succeeded);

        handle.shutdown().await?;

        Ok(())
    }
}
