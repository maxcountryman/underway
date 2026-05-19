use super::*;

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
        self.run_every(1.minute()).await
    }

    /// Runs the worker with polling period.
    pub async fn run_every(&self, period: Span) -> Result {
        let period: StdDuration = period.try_into()?;
        let mut tasks = JoinSet::new();

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
        let mut tx = self.queue.backend.begin().await.map_err(Error::backend)?;
        let Some(claimed) = tx
            .claim_task(&self.queue.name)
            .await
            .map_err(Error::backend)?
        else {
            tx.commit().await.map_err(Error::backend)?;
            return Ok(None);
        };

        let input: T::Input = serde_json::from_value(claimed.input.clone())?;
        let timeout: StdDuration = claimed.config.timeout().try_into()?;
        let heartbeat: StdDuration = claimed.config.heartbeat().try_into()?;

        let heartbeat_task = tokio::spawn({
            let backend = self.queue.backend.clone();
            let claimed = claimed.clone();
            async move {
                let mut interval = tokio::time::interval(heartbeat);
                interval.tick().await;
                loop {
                    if let Err(err) = backend.record_heartbeat(&claimed).await {
                        tracing::error!(%err, "failed to record heartbeat");
                    }
                    interval.tick().await;
                }
            }
        });

        let result = tokio::time::timeout(timeout, self.task.execute(&mut tx, input)).await;
        let task_id = claimed.id;

        match result {
            Ok(Ok(_)) => {
                tx.mark_task_succeeded(&claimed)
                    .await
                    .map_err(Error::backend)?;
            }
            Ok(Err(task::Error::Suspended(_))) => {
                tx.mark_task_waiting(&claimed)
                    .await
                    .map_err(Error::backend)?;
            }
            Ok(Err(err)) => {
                handle_task_error(
                    &mut tx,
                    &self.queue,
                    &claimed,
                    &claimed.config.retry_policy(),
                    &err,
                )
                .await?;
            }
            Err(_) => {
                let err = task::Error::TimedOut(timeout.try_into()?);
                handle_task_error(
                    &mut tx,
                    &self.queue,
                    &claimed,
                    &claimed.config.retry_policy(),
                    &err,
                )
                .await?;
            }
        }

        heartbeat_task.abort();
        tx.commit().await.map_err(Error::backend)?;

        Ok(Some(task_id))
    }
}

async fn handle_task_error<B, T>(
    tx: &mut B::Transaction<'_>,
    queue: &queue::Queue<B, T>,
    claimed: &backend::ClaimedTask,
    retry_policy: &task::RetryPolicy,
    err: &task::Error,
) -> Result
where
    B: backend::Backend,
    T: task::Task<B>,
{
    if matches!(err, task::Error::Fatal(_)) {
        tx.mark_task_failed(claimed).await.map_err(Error::backend)?;
        if let Some(dlq_name) = &queue.dlq_name {
            tx.move_task_to_dlq(claimed.id, &queue.name, dlq_name)
                .await
                .map_err(Error::backend)?;
        }
        return Ok(());
    }

    tx.record_task_failure(claimed, err)
        .await
        .map_err(Error::backend)?;

    if claimed.attempt_number < retry_policy.max_attempts {
        let delay = retry_policy.calculate_delay(claimed.attempt_number);
        tx.retry_task(claimed, delay)
            .await
            .map_err(Error::backend)?;
    } else {
        tx.mark_task_failed(claimed).await.map_err(Error::backend)?;
        if let Some(dlq_name) = &queue.dlq_name {
            tx.move_task_to_dlq(claimed.id, &queue.name, dlq_name)
                .await
                .map_err(Error::backend)?;
        }
    }

    Ok(())
}
