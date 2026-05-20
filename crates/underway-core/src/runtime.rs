use super::*;

/// Runtime handle.
pub struct RuntimeHandle {
    workers: JoinSet<Result>,
    shutdown_token: CancellationToken,
}

impl RuntimeHandle {
    /// Signals shutdown and waits for loops to end.
    pub async fn shutdown(self) -> Result {
        self.shutdown_token.cancel();
        self.wait().await
    }

    /// Waits for runtime loops to end.
    pub async fn wait(mut self) -> Result {
        while let Some(result) = self.workers.join_next().await {
            result??;
        }
        Ok(())
    }
}

/// High-level workflow runtime.
pub struct Runtime<B, I, S = ()>
where
    B: backend::Backend,
    I: Serialize + Send + Sync + 'static,
    S: Clone + Send + Sync + 'static,
{
    workflow: workflow::Workflow<B, I, S>,
    activity_worker: activity_worker::ActivityWorker<B>,
}

impl<B, I, S> Clone for Runtime<B, I, S>
where
    B: backend::Backend,
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

impl<B, I, S> Runtime<B, I, S>
where
    B: backend::Backend,
    I: Serialize + Send + Sync + 'static,
    S: Clone + Send + Sync + 'static,
{
    /// Creates a runtime.
    pub fn new(workflow: workflow::Workflow<B, I, S>) -> Self {
        let queue = workflow.queue();
        let activity_worker = activity_worker::ActivityWorker::with_registry(
            queue.backend().clone(),
            queue.name().to_string(),
            workflow.activity_registry(),
        );
        Self {
            workflow,
            activity_worker,
        }
    }

    /// Workflow.
    pub fn workflow(&self) -> &workflow::Workflow<B, I, S> {
        &self.workflow
    }

    /// Worker.
    pub fn worker(&self) -> worker::Worker<B, workflow::Workflow<B, I, S>> {
        worker::Worker::new(self.workflow.queue(), self.workflow.clone())
    }

    /// Scheduler.
    pub fn scheduler(&self) -> scheduler::Scheduler<B, workflow::Workflow<B, I, S>> {
        scheduler::Scheduler::new(self.workflow.queue(), self.workflow.clone())
    }

    /// Runs runtime loops.
    pub async fn run(&self) -> Result {
        self.start().wait().await
    }

    /// Starts runtime loops.
    pub fn start(&self) -> RuntimeHandle {
        let shutdown_token = CancellationToken::new();
        let mut workers = JoinSet::new();

        let mut worker = self.worker();
        worker.set_shutdown_token(shutdown_token.clone());
        workers.spawn(async move { worker.run().await });

        let mut scheduler = self.scheduler();
        scheduler.set_shutdown_token(shutdown_token.clone());
        workers.spawn(async move { scheduler.run().await });

        let mut activity_worker = self.activity_worker.clone();
        activity_worker.set_shutdown_token(shutdown_token.clone());
        if !activity_worker.is_empty() {
            workers.spawn(async move { activity_worker.run().await });
        }

        RuntimeHandle {
            workers,
            shutdown_token,
        }
    }
}
