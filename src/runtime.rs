//! Runtime orchestration for workflows.
//!
//! `Runtime` is the high-level entrypoint for running a workflow.
//! It wraps a [`Job`] and exposes a single interface to run or
//! start processing.

use serde::Serialize;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;

use crate::{
    activity::Activity,
    activity_worker::{ActivityWorker, Error as ActivityWorkerError},
    job::Job,
    scheduler::Error as SchedulerError,
    worker::Error as WorkerError,
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
#[derive(Clone)]
pub struct Runtime<I, S>
where
    I: Serialize + Send + Sync + 'static,
    S: Clone + Send + Sync + 'static,
{
    workflow: Job<I, S>,
    activity_worker: ActivityWorker,
}

impl<I, S> Runtime<I, S>
where
    I: Serialize + Send + Sync + 'static,
    S: Clone + Send + Sync + 'static,
{
    /// Creates a runtime from a workflow.
    pub fn new(workflow: Job<I, S>) -> Self {
        let activity_worker = ActivityWorker::new(workflow.queue().pool.clone());
        Self {
            workflow,
            activity_worker,
        }
    }

    /// Returns a reference to the workflow managed by this runtime.
    pub fn workflow(&self) -> &Job<I, S> {
        &self.workflow
    }

    /// Registers an activity handler for this runtime.
    pub fn activity<A: Activity>(mut self, activity: A) -> Self {
        self.activity_worker = self.activity_worker.activity(activity);
        self
    }

    /// Runs this runtime to completion.
    pub async fn run(&self) -> Result {
        let shutdown_token = CancellationToken::new();

        let mut worker = self.workflow.worker();
        worker.set_shutdown_token(shutdown_token.clone());

        let mut scheduler = self.workflow.scheduler();
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

    /// Starts this runtime and returns a handle.
    pub fn start(&self) -> RuntimeHandle {
        let shutdown_token = CancellationToken::new();

        let mut worker = self.workflow.worker();
        worker.set_shutdown_token(shutdown_token.clone());

        let mut scheduler = self.workflow.scheduler();
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

impl<I, S> From<Job<I, S>> for Runtime<I, S>
where
    I: Serialize + Send + Sync + 'static,
    S: Clone + Send + Sync + 'static,
{
    fn from(workflow: Job<I, S>) -> Self {
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
        Job, To,
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

    #[derive(Debug, Deserialize, Serialize)]
    struct Step1 {
        message: String,
    }

    #[derive(Debug, Deserialize, Serialize)]
    struct Step2 {
        echoed: String,
    }

    #[sqlx::test]
    async fn call_suspends_then_resumes(
        pool: PgPool,
    ) -> std::result::Result<(), Box<dyn std::error::Error>> {
        let outputs = Arc::new(Mutex::new(Vec::new()));
        let outputs_step = outputs.clone();

        let workflow = Job::builder()
            .step(|cx, Step1 { message }| async move {
                let echoed: String = cx.call("echo-main", EchoActivity::NAME, &message).await?;
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

        let runtime = Runtime::new(workflow).activity(EchoActivity);

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
}
