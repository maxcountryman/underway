use super::*;

struct HeartbeatGuard {
    handle: tokio::task::JoinHandle<()>,
}

impl HeartbeatGuard {
    fn spawn<B>(backend: B, claimed: backend::ClaimedTask, heartbeat: StdDuration) -> Self
    where
        B: backend::Backend,
    {
        let handle = tokio::spawn(async move {
            let mut interval = tokio::time::interval(heartbeat);
            interval.tick().await;
            loop {
                if let Err(err) = backend.record_heartbeat(&claimed).await {
                    tracing::error!(%err, "failed to record heartbeat");
                }
                interval.tick().await;
            }
        });
        Self { handle }
    }
}

impl Drop for HeartbeatGuard {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

/// Worker for one task type.
pub struct Worker<B, T>
where
    B: backend::Backend,
    T: task::Task<B>,
{
    queue: Arc<queue::Queue<B, T>>,
    task: Arc<T>,
    concurrency_limit: usize,
    shutdown_token: CancellationToken,
}

impl<B, T> Clone for Worker<B, T>
where
    B: backend::Backend,
    T: task::Task<B>,
{
    fn clone(&self) -> Self {
        Self {
            queue: Arc::clone(&self.queue),
            task: Arc::clone(&self.task),
            concurrency_limit: self.concurrency_limit,
            shutdown_token: self.shutdown_token.clone(),
        }
    }
}

impl<B, T> Worker<B, T>
where
    B: backend::Backend,
    T: task::Task<B>,
{
    /// Creates a worker.
    pub fn new(queue: Arc<queue::Queue<B, T>>, task: T) -> Self {
        Self {
            queue,
            task: Arc::new(task),
            concurrency_limit: num_cpus::get(),
            shutdown_token: CancellationToken::new(),
        }
    }

    /// Sets concurrency limit.
    pub fn set_concurrency_limit(&mut self, concurrency_limit: usize) {
        self.concurrency_limit = concurrency_limit;
    }

    /// Sets shutdown token.
    pub fn set_shutdown_token(&mut self, shutdown_token: CancellationToken) {
        self.shutdown_token = shutdown_token;
    }

    /// Cancels shutdown token.
    pub fn shutdown(&self) {
        self.shutdown_token.cancel();
    }

    /// Runs the worker.
    pub async fn run(&self) -> Result {
        self.run_every(1.second()).await
    }

    /// Runs the worker with polling period.
    pub async fn run_every(&self, period: Span) -> Result {
        let period: StdDuration = period.try_into()?;
        let mut tasks = JoinSet::new();

        if self.concurrency_limit > 0 {
            let worker = self.clone();
            tasks.spawn(async move {
                while worker.process_next_task().await?.is_some() {
                    if worker.shutdown_token.is_cancelled() {
                        break;
                    }
                }
                Ok::<_, Error>(())
            });
        }

        loop {
            tokio::select! {
                _ = self.shutdown_token.cancelled() => break,
                maybe_joined = tasks.join_next(), if !tasks.is_empty() => {
                    if let Some(joined) = maybe_joined {
                        joined??;
                    }
                }
                waited = self.queue.backend.wait_for_task_change(&self.queue.name, period) => {
                    waited.map_err(Error::backend)?;
                    if tasks.len() < self.concurrency_limit {
                        let worker = self.clone();
                        tasks.spawn(async move {
                            while worker.process_next_task().await?.is_some() {
                                if worker.shutdown_token.is_cancelled() {
                                    break;
                                }
                            }
                            Ok::<_, Error>(())
                        });
                    }
                }
            }
        }

        while let Some(joined) = tasks.join_next().await {
            joined??;
        }

        Ok(())
    }

    /// Processes the next available task.
    #[instrument(skip(self), fields(queue.name = self.queue.name), err)]
    pub async fn process_next_task(&self) -> Result<Option<task::TaskId>> {
        let mut claim_tx = self.queue.backend.begin().await.map_err(Error::backend)?;
        let Some(claimed) = claim_tx
            .claim_task(&self.queue.name)
            .await
            .map_err(Error::backend)?
        else {
            claim_tx.commit().await.map_err(Error::backend)?;
            return Ok(None);
        };
        claim_tx.commit().await.map_err(Error::backend)?;

        let input: T::Input = serde_json::from_value(claimed.input.clone())?;
        let timeout: StdDuration = claimed.config.timeout().try_into()?;
        let heartbeat: StdDuration = claimed.config.heartbeat().try_into()?;
        let task_id = claimed.id;

        let mut tx = self.queue.backend.begin().await.map_err(Error::backend)?;
        let _heartbeat =
            HeartbeatGuard::spawn(self.queue.backend.clone(), claimed.clone(), heartbeat);

        let result = tokio::time::timeout(timeout, self.task.execute(&mut tx, input)).await;

        match result {
            Ok(Ok(_)) => {
                let applied = tx
                    .mark_task_succeeded(&claimed)
                    .await
                    .map_err(Error::backend)?;
                finish_execution_transaction::<B>(tx, applied).await?;
            }
            Ok(Err(task::Error::Suspended(_))) => {
                let applied = tx
                    .mark_task_waiting(&claimed)
                    .await
                    .map_err(Error::backend)?;
                finish_execution_transaction::<B>(tx, applied).await?;
            }
            Ok(Err(err)) => {
                tx.rollback().await.map_err(Error::backend)?;
                finalize_task_error(&self.queue, &claimed, &err).await?;
            }
            Err(_) => {
                let err = task::Error::TimedOut(timeout.try_into()?);
                tx.rollback().await.map_err(Error::backend)?;
                finalize_task_error(&self.queue, &claimed, &err).await?;
            }
        }

        Ok(Some(task_id))
    }
}

async fn finish_execution_transaction<B>(
    tx: B::Transaction<'_>,
    applied: backend::ApplyResult,
) -> Result
where
    B: backend::Backend,
{
    if applied.applied() {
        tx.commit().await.map_err(Error::backend)
    } else {
        tx.rollback().await.map_err(Error::backend)
    }
}

async fn finalize_task_error<B, T>(
    queue: &queue::Queue<B, T>,
    claimed: &backend::ClaimedTask,
    err: &task::Error,
) -> Result
where
    B: backend::Backend,
    T: task::Task<B>,
{
    let mut tx = queue.backend.begin().await.map_err(Error::backend)?;
    let applied =
        handle_task_error(&mut tx, queue, claimed, &claimed.config.retry_policy(), err).await?;
    if applied.applied() {
        tx.commit().await.map_err(Error::backend)
    } else {
        tx.rollback().await.map_err(Error::backend)
    }
}

async fn handle_task_error<B, T>(
    tx: &mut B::Transaction<'_>,
    queue: &queue::Queue<B, T>,
    claimed: &backend::ClaimedTask,
    retry_policy: &task::RetryPolicy,
    err: &task::Error,
) -> Result<backend::ApplyResult>
where
    B: backend::Backend,
    T: task::Task<B>,
{
    if matches!(err, task::Error::Fatal(_)) {
        let applied = tx
            .record_task_failure(claimed, err)
            .await
            .map_err(Error::backend)?;
        if !applied.applied() {
            return Ok(applied);
        }
        if let Some(dlq_name) = &queue.dlq_name {
            return tx
                .move_task_to_dlq(claimed.id, &queue.name, dlq_name)
                .await
                .map_err(Error::backend);
        }
        return tx.mark_task_failed(claimed).await.map_err(Error::backend);
    }

    let applied = tx
        .record_task_failure(claimed, err)
        .await
        .map_err(Error::backend)?;
    if !applied.applied() {
        return Ok(applied);
    }

    if claimed.attempt_number < retry_policy.max_attempts {
        return tx
            .retry_task(
                claimed,
                retry_policy.calculate_delay(claimed.attempt_number),
            )
            .await
            .map_err(Error::backend);
    }

    if let Some(dlq_name) = &queue.dlq_name {
        return tx
            .move_task_to_dlq(claimed.id, &queue.name, dlq_name)
            .await
            .map_err(Error::backend);
    }

    tx.mark_task_failed(claimed).await.map_err(Error::backend)
}

#[cfg(test)]
mod tests {
    use std::{
        error::Error as StdError,
        fmt,
        sync::{
            atomic::{AtomicBool, AtomicUsize, Ordering},
            Arc,
        },
    };

    use super::*;

    #[derive(Debug)]
    struct TestError;

    impl fmt::Display for TestError {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("test backend transition failed")
        }
    }

    impl StdError for TestError {}

    #[derive(Clone, Default)]
    struct TestBackend {
        enqueued: Arc<AtomicBool>,
        claimed: Arc<AtomicBool>,
        heartbeats: Arc<AtomicUsize>,
    }

    impl TestBackend {
        fn heartbeats(&self) -> usize {
            self.heartbeats.load(Ordering::SeqCst)
        }
    }

    struct TestTransaction {
        backend: TestBackend,
    }

    struct TestSchedulerLease;

    impl backend::SchedulerLease for TestSchedulerLease {
        type Error = TestError;

        async fn release(&mut self) -> StdResult<(), Self::Error> {
            Ok(())
        }
    }

    impl backend::Backend for TestBackend {
        type Error = TestError;
        type Transaction<'a> = TestTransaction;
        type SchedulerLease<'a> = TestSchedulerLease;

        async fn begin(&self) -> StdResult<Self::Transaction<'_>, Self::Error> {
            Ok(TestTransaction {
                backend: self.clone(),
            })
        }

        async fn record_heartbeat(
            &self,
            _task: &backend::ClaimedTask,
        ) -> StdResult<(), Self::Error> {
            self.heartbeats.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }

        async fn try_acquire_scheduler(
            &self,
            _queue_name: &str,
        ) -> StdResult<Option<Self::SchedulerLease<'_>>, Self::Error> {
            Ok(Some(TestSchedulerLease))
        }
    }

    impl backend::BackendTransaction for TestTransaction {
        type Error = TestError;

        async fn commit(self) -> StdResult<(), Self::Error> {
            Ok(())
        }

        async fn rollback(self) -> StdResult<(), Self::Error> {
            Ok(())
        }

        async fn create_queue(
            &mut self,
            _name: &str,
            _dlq_name: Option<&str>,
        ) -> StdResult<(), Self::Error> {
            Ok(())
        }

        async fn enqueue_task(
            &mut self,
            _queue_name: &str,
            _task: backend::EnqueueTask,
        ) -> StdResult<task::TaskId, Self::Error> {
            self.backend.enqueued.store(true, Ordering::SeqCst);
            Ok(task::TaskId::new())
        }

        async fn claim_task(
            &mut self,
            queue_name: &str,
        ) -> StdResult<Option<backend::ClaimedTask>, Self::Error> {
            if !self.backend.enqueued.load(Ordering::SeqCst)
                || self.backend.claimed.swap(true, Ordering::SeqCst)
            {
                return Ok(None);
            }

            Ok(Some(backend::ClaimedTask {
                id: task::TaskId::new(),
                queue_name: queue_name.to_string(),
                input: serde_json::json!(null),
                config: task::TaskConfig::builder()
                    .heartbeat(1.milliseconds())
                    .timeout(1.second())
                    .build()
                    .expect("valid test task config"),
                attempt_number: 1,
            }))
        }

        async fn mark_task_succeeded(
            &mut self,
            _task: &backend::ClaimedTask,
        ) -> StdResult<backend::ApplyResult, Self::Error> {
            Err(TestError)
        }

        async fn mark_task_waiting(
            &mut self,
            _task: &backend::ClaimedTask,
        ) -> StdResult<backend::ApplyResult, Self::Error> {
            Ok(backend::ApplyResult::Stale)
        }

        async fn record_task_failure(
            &mut self,
            _task: &backend::ClaimedTask,
            _error: &task::Error,
        ) -> StdResult<backend::ApplyResult, Self::Error> {
            Ok(backend::ApplyResult::Stale)
        }

        async fn retry_task(
            &mut self,
            _task: &backend::ClaimedTask,
            _delay: Span,
        ) -> StdResult<backend::ApplyResult, Self::Error> {
            Ok(backend::ApplyResult::Stale)
        }

        async fn mark_task_failed(
            &mut self,
            _task: &backend::ClaimedTask,
        ) -> StdResult<backend::ApplyResult, Self::Error> {
            Ok(backend::ApplyResult::Stale)
        }

        async fn move_task_to_dlq(
            &mut self,
            _task_id: task::TaskId,
            _from_queue: &str,
            _dlq_name: &str,
        ) -> StdResult<backend::ApplyResult, Self::Error> {
            Ok(backend::ApplyResult::Stale)
        }

        async fn cancel_workflow(
            &mut self,
            _queue_name: &str,
            _workflow_run_id: workflow::WorkflowRunId,
        ) -> StdResult<bool, Self::Error> {
            Ok(false)
        }

        async fn schedule_task(
            &mut self,
            _queue_name: &str,
            _schedule: scheduler::ZonedSchedule,
            _input: serde_json::Value,
        ) -> StdResult<(), Self::Error> {
            Ok(())
        }

        async fn unschedule_task(&mut self, _queue_name: &str) -> StdResult<(), Self::Error> {
            Ok(())
        }

        async fn load_task_schedule(
            &mut self,
            _queue_name: &str,
        ) -> StdResult<Option<backend::StoredSchedule>, Self::Error> {
            Ok(None)
        }

        async fn load_activity_calls(
            &mut self,
            _queue_name: &str,
            _workflow_run_id: workflow::WorkflowRunId,
            _step_index: usize,
        ) -> StdResult<Vec<backend::ActivityCallRecord>, Self::Error> {
            Ok(Vec::new())
        }

        async fn record_activity_call(
            &mut self,
            _queue_name: &str,
            _workflow_run_id: workflow::WorkflowRunId,
            _step_index: usize,
            _command: backend::ActivityCallCommand,
        ) -> StdResult<(), Self::Error> {
            Ok(())
        }

        async fn claim_activity_call(
            &mut self,
            _queue_name: &str,
            _activities: &[String],
        ) -> StdResult<Option<backend::ClaimedActivityCall>, Self::Error> {
            Ok(None)
        }

        async fn complete_activity_call(
            &mut self,
            _call: &backend::ClaimedActivityCall,
            _output: serde_json::Value,
        ) -> StdResult<backend::ApplyResult, Self::Error> {
            Ok(backend::ApplyResult::Stale)
        }

        async fn retry_activity_call(
            &mut self,
            _call: &backend::ClaimedActivityCall,
            _error: activity::Error,
            _delay: Span,
        ) -> StdResult<backend::ApplyResult, Self::Error> {
            Ok(backend::ApplyResult::Stale)
        }

        async fn fail_activity_call(
            &mut self,
            _call: &backend::ClaimedActivityCall,
            _error: activity::Error,
        ) -> StdResult<backend::ApplyResult, Self::Error> {
            Ok(backend::ApplyResult::Stale)
        }

        async fn wake_workflow_task(
            &mut self,
            _queue_name: &str,
            _workflow_run_id: workflow::WorkflowRunId,
            _step_index: usize,
        ) -> StdResult<(), Self::Error> {
            Ok(())
        }

        async fn delete_expired(&mut self) -> StdResult<u64, Self::Error> {
            Ok(0)
        }
    }

    struct SuccessfulTask;

    impl task::Task<TestBackend> for SuccessfulTask {
        type Input = ();
        type Output = ();

        async fn execute(
            &self,
            _tx: &mut <TestBackend as backend::Backend>::Transaction<'_>,
            _input: Self::Input,
        ) -> task::Result<Self::Output> {
            Ok(())
        }

        fn config(&self) -> task::ConfigResult {
            task::TaskConfig::builder()
                .heartbeat(1.milliseconds())
                .timeout(1.second())
                .build()
        }
    }

    #[tokio::test]
    async fn heartbeat_stops_when_finalization_errors() {
        let backend = TestBackend::default();
        let queue = Arc::new(
            queue::Queue::builder()
                .name("heartbeat_stops_when_finalization_errors")
                .backend(backend.clone())
                .build()
                .await
                .unwrap(),
        );
        let task = SuccessfulTask;
        queue.enqueue(&task, &()).await.unwrap();

        let worker = Worker::new(queue, task);
        worker.process_next_task().await.unwrap_err();
        let heartbeats_after_error = backend.heartbeats();

        tokio::time::sleep(StdDuration::from_millis(20)).await;
        assert_eq!(backend.heartbeats(), heartbeats_after_error);
    }
}
