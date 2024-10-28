use std::{result::Result as StdResult, str::FromStr, sync::Arc, time::Duration as StdDuration};

use jiff::{tz::TimeZone, Zoned};
use jiff_cron::Schedule;
use sqlx::postgres::{PgAdvisoryLock, PgListener};
use tokio_util::sync::CancellationToken;
use tracing::instrument;

use crate::{
    queue::{try_acquire_advisory_lock, Error as QueueError, SHUTDOWN_CHANNEL},
    Queue, Task,
};

pub(crate) type Result<T = ()> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Queue(#[from] QueueError),

    #[error(transparent)]
    Database(#[from] sqlx::Error),

    #[error(transparent)]
    Jiff(#[from] jiff::Error),

    #[error(transparent)]
    Cron(#[from] jiff_cron::error::Error),
}

/// Scheduler for running task schedules.
///
/// # Singleton behavior
///
/// In order to ensure schedules are dispatched at most once, only a single
/// instance of a scheduler is allowed to run per queue. Internally this is
/// managed via an [advisory lock][advisory-lock]. The lock is keyed to the name
/// of the queue the scheduler belongs to.
///
/// When a scheduler is run it will attempt to acquire its lock. When it can,
/// the run method loops indefinitely. However, when the lock cannot be
/// acquired, e.g. because another scheduler is already running, it will return.
///
/// [advisory-lock]: https://www.postgresql.org/docs/current/explicit-locking.html#ADVISORY-LOCKS
pub struct Scheduler<T: Task> {
    queue: Queue<T>,
    queue_lock: PgAdvisoryLock,
    task: Arc<T>,

    // When this token is cancelled the queue has been shutdown.
    shutdown_token: CancellationToken,
}

impl<T: Task> Scheduler<T> {
    /// Creates a new scheduler with the given queue and task.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use sqlx::{PgPool, Transaction, Postgres};
    /// # use underway::{Task, task::Result as TaskResult, Queue};
    /// use underway::Scheduler;
    ///
    /// # struct ExampleTask;
    /// # impl Task for ExampleTask {
    /// #     type Input = ();
    /// #     type Output = ();
    /// #     async fn execute(
    /// #         &self,
    /// #         _: Transaction<'_, Postgres>,
    /// #         _: Self::Input,
    /// #     ) -> TaskResult<Self::Output> {
    /// #         Ok(())
    /// #     }
    /// # }
    /// # use tokio::runtime::Runtime;
    /// # fn main() {
    /// # let rt = Runtime::new().unwrap();
    /// # rt.block_on(async {
    /// # let pool = PgPool::connect(&std::env::var("DATABASE_URL")?).await?;
    /// # let queue = Queue::builder()
    /// #    .name("example")
    /// #    .pool(pool.clone())
    /// #    .build()
    /// #    .await?;
    /// # /*
    /// let queue = { /* A `Queue`. */ };
    /// # */
    /// # let task = ExampleTask;
    /// # /*
    /// let task = { /* An implementer of `Task`. */ };
    /// # */
    /// #
    ///
    /// Scheduler::new(queue, task);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # });
    /// # }
    /// ```
    pub fn new(queue: Queue<T>, task: T) -> Self {
        let queue_lock = queue_scheduler_lock(&queue.name);
        Self {
            queue,
            queue_lock,
            task: Arc::new(task),
            shutdown_token: CancellationToken::new(),
        }
    }

    /// Sets the shutdown token.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use sqlx::{PgPool, Transaction, Postgres};
    /// # use underway::{Task, task::Result as TaskResult, Queue, Scheduler};
    /// use tokio_util::sync::CancellationToken;
    ///
    /// # struct ExampleTask;
    /// # impl Task for ExampleTask {
    /// #     type Input = ();
    /// #     type Output = ();
    /// #     async fn execute(
    /// #         &self,
    /// #         _: Transaction<'_, Postgres>,
    /// #         _: Self::Input,
    /// #     ) -> TaskResult<Self::Output> {
    /// #         Ok(())
    /// #     }
    /// # }
    /// # use tokio::runtime::Runtime;
    /// # fn main() {
    /// # let rt = Runtime::new().unwrap();
    /// # rt.block_on(async {
    /// # let pool = PgPool::connect(&std::env::var("DATABASE_URL")?).await?;
    /// # let queue = Queue::builder()
    /// #    .name("example")
    /// #    .pool(pool.clone())
    /// #    .build()
    /// #    .await?;
    /// # let task = ExampleTask;
    /// # let mut scheduler = Scheduler::new(queue, task);
    /// # /*
    /// let mut scheduler = { /* A `Scheduler`. */ };
    /// # */
    /// #
    ///
    /// // Set a custom cancellation token.
    /// let token = CancellationToken::new();
    /// scheduler.set_shutdown_token(token);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # });
    /// # }
    /// ```
    pub fn set_shutdown_token(&mut self, shutdown_token: CancellationToken) {
        self.shutdown_token = shutdown_token;
    }

    /// Cancels the shutdown token causing the scheduler to exit.
    ///
    /// ```rust,no_run
    /// # use sqlx::{PgPool, Transaction, Postgres};
    /// # use underway::{Task, task::Result as TaskResult, Queue, Scheduler};
    /// use tokio_util::sync::CancellationToken;
    ///
    /// # struct ExampleTask;
    /// # impl Task for ExampleTask {
    /// #     type Input = ();
    /// #     type Output = ();
    /// #     async fn execute(
    /// #         &self,
    /// #         _: Transaction<'_, Postgres>,
    /// #         _: Self::Input,
    /// #     ) -> TaskResult<Self::Output> {
    /// #         Ok(())
    /// #     }
    /// # }
    /// # use tokio::runtime::Runtime;
    /// # fn main() {
    /// # let rt = Runtime::new().unwrap();
    /// # rt.block_on(async {
    /// # let pool = PgPool::connect(&std::env::var("DATABASE_URL")?).await?;
    /// # let queue = Queue::builder()
    /// #    .name("example")
    /// #    .pool(pool.clone())
    /// #    .build()
    /// #    .await?;
    /// # let task = ExampleTask;
    /// # let scheduler = Scheduler::new(queue, task);
    /// # /*
    /// let scheduler = { /* A `Scheduler`. */ };
    /// # */
    /// #
    ///
    /// // Stop the scheduler.
    /// scheduler.shutdown();
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # });
    /// # }
    /// ```
    pub fn shutdown(&self) {
        self.shutdown_token.cancel();
    }

    /// Loops over the configured schedule, enqueuing tasks as the duration
    /// arrives.
    ///
    /// # Errors
    ///
    /// This function returns an error if:
    ///
    /// - It cannot acquire a new connection from the queue's pool.
    /// - It fails to listen on either the shutdown channel.
    /// - The cron expression or timezone IANA name are malformed.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use sqlx::{PgPool, Transaction, Postgres};
    /// # use underway::{Task, task::Result as TaskResult, Queue, Scheduler};
    /// # struct ExampleTask;
    /// # impl Task for ExampleTask {
    /// #     type Input = ();
    /// #     type Output = ();
    /// #     async fn execute(
    /// #         &self,
    /// #         _: Transaction<'_, Postgres>,
    /// #         _: Self::Input,
    /// #     ) -> TaskResult<Self::Output> {
    /// #         Ok(())
    /// #     }
    /// # }
    /// # use tokio::runtime::Runtime;
    /// # fn main() {
    /// # let rt = Runtime::new().unwrap();
    /// # rt.block_on(async {
    /// # let pool = PgPool::connect(&std::env::var("DATABASE_URL")?).await?;
    /// # let queue = Queue::builder()
    /// #    .name("example")
    /// #    .pool(pool)
    /// #    .build()
    /// #    .await?;
    /// # let task = ExampleTask;
    /// # let scheduler = Scheduler::new(queue, task);
    /// # /*
    /// let scheduler = { /* A `Scheduler`. */ };
    /// # */
    /// #
    ///
    /// // Run the scheduler in separate task.
    /// tokio::spawn(async move { scheduler.run().await });
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # });
    /// # }
    /// ```
    pub async fn run(&self) -> Result {
        let conn = self.queue.pool.acquire().await?;
        let Some(_guard) = try_acquire_advisory_lock(conn, &self.queue_lock).await? else {
            tracing::debug!("Scheduler could not acquire lock, exiting");
            return Ok(());
        };

        let Some((zoned_schedule, input)) = self.queue.task_schedule(&self.queue.pool).await?
        else {
            // No schedule configured, so we'll exit.
            return Ok(());
        };

        // Set up a listener for shutdown notifications
        let mut shutdown_listener = PgListener::connect_with(&self.queue.pool).await?;
        shutdown_listener.listen(SHUTDOWN_CHANNEL).await?;

        // TODO: Handle updates to schedules?

        for until_next in zoned_schedule.into_iter() {
            tokio::select! {
                notify_shutdown = shutdown_listener.recv() => {
                    match notify_shutdown {
                        Ok(_) => {
                            self.shutdown_token.cancel();
                        },
                        Err(err) => {
                            tracing::error!(%err, "Postgres shutdown notification error");
                        }
                    }
                }

                _ = self.shutdown_token.cancelled() => {
                    break
                }

                _ = tokio::time::sleep(until_next) => {
                    tracing::debug!(?until_next, "Sleeping until next scheduled task enqueue");
                    self.process_next_schedule(&input).await?
                }
            }
        }

        Ok(())
    }

    #[instrument(
        skip_all,
        fields(
            queue.name = self.queue.name,
            task.id = tracing::field::Empty,
            until_next
        ),
        err
    )]
    async fn process_next_schedule(&self, input: &T::Input) -> Result {
        let task_id = self
            .queue
            .enqueue(&self.queue.pool, &self.task, input)
            .await?;

        tracing::Span::current().record("task.id", task_id.as_hyphenated().to_string());

        Ok(())
    }
}

fn queue_scheduler_lock(queue_name: &str) -> PgAdvisoryLock {
    PgAdvisoryLock::new(format!("{queue_name}-scheduler"))
}

/// Schedule paired with its time zone.
#[derive(Debug, PartialEq)]
pub struct ZonedSchedule {
    schedule: Schedule,
    timezone: TimeZone,
}

impl ZonedSchedule {
    /// Create a new schedule which is associated with a time zone.
    pub fn new(cron_expr: &str, time_zone_name: &str) -> StdResult<Self, ZonedScheduleError> {
        let schedule = cron_expr.parse()?;
        let timezone = TimeZone::get(time_zone_name)?;

        assert!(
            timezone.iana_name().is_some(),
            "Time zones must use IANA names for now"
        );

        Ok(Self { schedule, timezone })
    }

    pub(crate) fn cron_expr(&self) -> String {
        self.schedule.to_string()
    }

    pub(crate) fn iana_name(&self) -> &str {
        self.timezone
            .iana_name()
            .expect("iana_name should always be Some because new ensures valid time zone")
    }

    fn tz(&self) -> TimeZone {
        self.timezone.to_owned()
    }

    fn now_with_tz(&self) -> Zoned {
        Zoned::now().with_time_zone(self.tz())
    }
}

impl Iterator for ZonedSchedule {
    type Item = StdDuration;

    fn next(&mut self) -> Option<Self::Item> {
        self.schedule.upcoming(self.tz()).next().map(|next_zoned| {
            self.now_with_tz()
                .duration_until(&next_zoned)
                .unsigned_abs()
        })
    }
}

#[derive(Debug, thiserror::Error)]
pub enum ZonedScheduleError {
    #[error(transparent)]
    Jiff(#[from] jiff::Error),

    #[error(transparent)]
    Cron(#[from] jiff_cron::error::Error),

    #[error("Parsing error: {0}")]
    Parse(String),
}

impl FromStr for ZonedSchedule {
    type Err = ZonedScheduleError;

    fn from_str(s: &str) -> StdResult<Self, Self::Err> {
        // Check if the string ends with a closing bracket ']'
        if !s.ends_with(']') {
            return Err(ZonedScheduleError::Parse("Missing closing ']'".to_string()));
        }

        // Find the position of the opening bracket '['
        let open_bracket_pos = s
            .find('[')
            .ok_or_else(|| ZonedScheduleError::Parse("Missing opening '['".to_string()))?;

        // Extract the cron expression and time zone string
        let cron_expr = &s[..open_bracket_pos];
        let time_zone_name = &s[open_bracket_pos + 1..s.len() - 1]; // Exclude the closing ']'

        ZonedSchedule::new(cron_expr, time_zone_name)
    }
}
