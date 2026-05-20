use super::*;

/// Task queue.
#[derive(Debug)]
pub struct Queue<B, T>
where
    B: backend::Backend,
    T: task::Task<B>,
{
    pub(crate) name: String,
    pub(crate) dlq_name: Option<String>,
    pub(crate) backend: B,
    _marker: PhantomData<T>,
}

impl<B, T> Clone for Queue<B, T>
where
    B: backend::Backend,
    T: task::Task<B>,
{
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            dlq_name: self.dlq_name.clone(),
            backend: self.backend.clone(),
            _marker: PhantomData,
        }
    }
}

impl<B, T> Queue<B, T>
where
    B: backend::Backend,
    T: task::Task<B>,
{
    /// Creates a new builder.
    pub fn builder() -> QueueBuilder<B, T, Initial> {
        QueueBuilder::default()
    }

    /// Queue name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Backend handle.
    pub fn backend(&self) -> &B {
        &self.backend
    }

    /// Enqueues a task.
    pub async fn enqueue(&self, task: &T, input: &T::Input) -> Result<task::TaskId> {
        let mut tx = self.backend.begin().await.map_err(Error::backend)?;
        let id = self.enqueue_using(&mut tx, task, input).await?;
        tx.commit().await.map_err(Error::backend)?;
        Ok(id)
    }

    /// Enqueues a task after an additional delay.
    pub async fn enqueue_after(
        &self,
        task: &T,
        input: &T::Input,
        delay: Span,
    ) -> Result<task::TaskId> {
        let mut tx = self.backend.begin().await.map_err(Error::backend)?;
        let id = self
            .enqueue_after_using(&mut tx, task, input, delay)
            .await?;
        tx.commit().await.map_err(Error::backend)?;
        Ok(id)
    }

    /// Enqueues a task using an existing transaction.
    pub async fn enqueue_using(
        &self,
        tx: &mut B::Transaction<'_>,
        task: &T,
        input: &T::Input,
    ) -> Result<task::TaskId> {
        self.enqueue_after_using(tx, task, input, Span::new()).await
    }

    /// Enqueues a task after delay using an existing transaction.
    pub async fn enqueue_after_using(
        &self,
        tx: &mut B::Transaction<'_>,
        task: &T,
        input: &T::Input,
        delay: Span,
    ) -> Result<task::TaskId> {
        let config = task.config_for(input)?.with_additional_delay(delay)?;
        let command = backend::EnqueueTask {
            input: serde_json::to_value(input)?,
            config,
            delay: Span::new(),
        };
        tx.enqueue_task(&self.name, command)
            .await
            .map_err(Error::backend)
    }

    /// Enqueues many tasks.
    pub async fn enqueue_many(&self, task: &T, inputs: &[T::Input]) -> Result<Vec<task::TaskId>> {
        let mut tx = self.backend.begin().await.map_err(Error::backend)?;
        let mut ids = Vec::with_capacity(inputs.len());
        for input in inputs {
            ids.push(self.enqueue_using(&mut tx, task, input).await?);
        }
        tx.commit().await.map_err(Error::backend)?;
        Ok(ids)
    }

    /// Dequeues the next available task.
    pub async fn dequeue(&self) -> Result<Option<backend::ClaimedTask>> {
        let mut tx = self.backend.begin().await.map_err(Error::backend)?;
        let task = tx.claim_task(&self.name).await.map_err(Error::backend)?;
        tx.commit().await.map_err(Error::backend)?;
        Ok(task)
    }

    /// Stores or replaces a schedule.
    pub async fn schedule(&self, schedule: &scheduler::ZonedSchedule, input: &T::Input) -> Result {
        let mut tx = self.backend.begin().await.map_err(Error::backend)?;
        self.schedule_using(&mut tx, schedule, input).await?;
        tx.commit().await.map_err(Error::backend)?;
        Ok(())
    }

    /// Stores or replaces a schedule using a transaction.
    pub async fn schedule_using(
        &self,
        tx: &mut B::Transaction<'_>,
        schedule: &scheduler::ZonedSchedule,
        input: &T::Input,
    ) -> Result {
        tx.schedule_task(&self.name, schedule.clone(), serde_json::to_value(input)?)
            .await
            .map_err(Error::backend)
    }

    /// Removes a schedule.
    pub async fn unschedule(&self) -> Result {
        let mut tx = self.backend.begin().await.map_err(Error::backend)?;
        self.unschedule_using(&mut tx).await?;
        tx.commit().await.map_err(Error::backend)?;
        Ok(())
    }

    /// Removes a schedule using a transaction.
    pub async fn unschedule_using(&self, tx: &mut B::Transaction<'_>) -> Result {
        tx.unschedule_task(&self.name).await.map_err(Error::backend)
    }

    pub(crate) async fn load_task_schedule(
        &self,
        tx: &mut B::Transaction<'_>,
    ) -> Result<Option<backend::StoredSchedule>> {
        tx.load_task_schedule(&self.name)
            .await
            .map_err(Error::backend)
    }
}

/// Initial queue builder state.
pub struct Initial;
/// Name-set queue builder state.
pub struct NameSet {
    name: String,
    dlq_name: Option<String>,
}
/// Backend-set queue builder state.
pub struct BackendSet<B> {
    name: String,
    dlq_name: Option<String>,
    backend: B,
}

/// Queue builder.
pub struct QueueBuilder<B, T, S>
where
    B: backend::Backend,
    T: task::Task<B>,
{
    state: S,
    _marker: PhantomData<(B, T)>,
}

impl<B, T> Default for QueueBuilder<B, T, Initial>
where
    B: backend::Backend,
    T: task::Task<B>,
{
    fn default() -> Self {
        Self {
            state: Initial,
            _marker: PhantomData,
        }
    }
}

impl<B, T> QueueBuilder<B, T, Initial>
where
    B: backend::Backend,
    T: task::Task<B>,
{
    /// Sets the queue name.
    pub fn name(self, name: impl Into<String>) -> QueueBuilder<B, T, NameSet> {
        QueueBuilder {
            state: NameSet {
                name: name.into(),
                dlq_name: None,
            },
            _marker: PhantomData,
        }
    }
}

impl<B, T> QueueBuilder<B, T, NameSet>
where
    B: backend::Backend,
    T: task::Task<B>,
{
    /// Sets a DLQ name.
    pub fn dead_letter_queue(mut self, dlq_name: impl Into<String>) -> Self {
        self.state.dlq_name = Some(dlq_name.into());
        self
    }

    /// Sets the backend.
    pub fn backend(self, backend: B) -> QueueBuilder<B, T, BackendSet<B>> {
        QueueBuilder {
            state: BackendSet {
                name: self.state.name,
                dlq_name: self.state.dlq_name,
                backend,
            },
            _marker: PhantomData,
        }
    }
}

impl<B, T> QueueBuilder<B, T, BackendSet<B>>
where
    B: backend::Backend,
    T: task::Task<B>,
{
    /// Builds the queue.
    pub async fn build(self) -> Result<Queue<B, T>> {
        let state = self.state;
        let mut tx = state.backend.begin().await.map_err(Error::backend)?;
        tx.create_queue(&state.name, state.dlq_name.as_deref())
            .await
            .map_err(Error::backend)?;
        tx.commit().await.map_err(Error::backend)?;
        Ok(Queue {
            name: state.name,
            dlq_name: state.dlq_name,
            backend: state.backend,
            _marker: PhantomData,
        })
    }
}

/// Runs deletion clean up once.
pub async fn run_deletion_once<B: backend::Backend>(backend: &B) -> Result {
    let mut tx = backend.begin().await.map_err(Error::backend)?;
    let _ = tx.delete_expired().await.map_err(Error::backend)?;
    tx.commit().await.map_err(Error::backend)?;
    Ok(())
}

/// Runs deletion clean up repeatedly.
pub async fn run_deletion_every<B: backend::Backend>(backend: &B, period: Span) -> Result {
    let mut interval = tokio::time::interval(period.try_into()?);
    interval.tick().await;
    loop {
        run_deletion_once(backend).await?;
        interval.tick().await;
    }
}
