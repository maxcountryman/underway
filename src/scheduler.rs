use std::{result::Result as StdResult, str::FromStr, sync::Arc, time::Duration as StdDuration};

use jiff::{tz::TimeZone, Zoned};
use jiff_cron::{Schedule, ScheduleIterator};
use sqlx::postgres::{PgAdvisoryLock, PgListener};
use tokio::time::{sleep, Instant};
use tokio_util::sync::CancellationToken;
use tracing::instrument;

use crate::{
    queue::{shutdown_channel, try_acquire_advisory_lock, Error as QueueError},
    task::{RetryCount, RetryPolicy},
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
    queue: Arc<Queue<T>>,
    queue_lock: PgAdvisoryLock,

    task: Arc<T>,

    // When this token is cancelled the queue has been shutdown.
    shutdown_token: CancellationToken,

    // Policy for reconnection backoff when PostgreSQL connection is lost.
    reconnection_policy: RetryPolicy,
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
    /// Scheduler::new(queue.into(), task);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # });
    /// # }
    /// ```
    pub fn new(queue: Arc<Queue<T>>, task: T) -> Self {
        let task = Arc::new(task);
        let queue_lock = queue_scheduler_lock(&queue.name);
        Self {
            queue,
            queue_lock,
            task,
            shutdown_token: CancellationToken::new(),
            reconnection_policy: RetryPolicy::default(),
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
    /// # let mut scheduler = Scheduler::new(queue.into(), task);
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

    /// Sets the reconnection policy for PostgreSQL connection failures.
    ///
    /// This policy controls how the scheduler retries connecting when the
    /// PostgreSQL connection is lost. Uses exponential backoff to avoid
    /// overwhelming the database.
    ///
    /// Defaults to 1 second initial interval, 60 second max interval, 2.0
    /// coefficient and 0.5 jitter_factor.
    ///
    /// **Note**: The `max_attempts` field is ignored for reconnection - the
    /// scheduler will keep retrying until successful or until shutdown.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use sqlx::{PgPool, Transaction, Postgres};
    /// # use underway::{Task, task::Result as TaskResult, Queue, Scheduler};
    /// use underway::task::RetryPolicy;
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
    /// # let mut scheduler = Scheduler::new(queue.into(), task);
    /// # /*
    /// let mut scheduler = { /* A `Scheduler`. */ };
    /// # */
    /// #
    ///
    /// // Set a custom reconnection policy using RetryPolicy.
    /// let policy = RetryPolicy::builder()
    ///     .initial_interval_ms(2_000) // 2 seconds
    ///     .max_interval_ms(60_000) // 1 minute
    ///     .backoff_coefficient(2.5)
    ///     .jitter_factor(0.5)
    ///     .build();
    /// scheduler.set_reconnection_policy(policy);
    /// # Ok::<(), Box<dyn std::error::Error>>(())
    /// # });
    /// # }
    /// ```
    pub fn set_reconnection_policy(&mut self, reconnection_policy: RetryPolicy) {
        self.reconnection_policy = reconnection_policy;
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
    /// # let scheduler = Scheduler::new(queue.into(), task);
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
    /// # let scheduler = Scheduler::new(queue.into(), task);
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
    #[instrument(skip(self), fields(queue.name = self.queue.name), err)]
    pub async fn run(&self) -> Result {
        let mut retry_count: RetryCount = 1;

        // Outer loop: handle reconnection logic for the scheduler's Postgres listener.
        'reconnect: loop {
            // Compute current reconnect backoff
            let reconnect_backoff_span = self.reconnection_policy.calculate_delay(retry_count);
            let reconnect_backoff: StdDuration = reconnect_backoff_span.try_into()?;

            let conn = match self.queue.pool.acquire().await {
                Ok(conn) => conn,
                Err(err) => {
                    tracing::error!(
                        %err,
                        backoff_secs = reconnect_backoff.as_secs(),
                        attempt = retry_count,
                        "Failed to acquire database connection for scheduler, retrying after backoff"
                    );
                    sleep(reconnect_backoff).await;
                    retry_count = retry_count.saturating_add(1);
                    continue 'reconnect;
                }
            };

            let Some(guard) = try_acquire_advisory_lock(conn, &self.queue_lock).await? else {
                tracing::trace!("Scheduler could not acquire lock, exiting");
                return Ok(());
            };

            let Some((zoned_schedule, input)) = self.queue.task_schedule(&self.queue.pool).await?
            else {
                // No schedule configured, so we'll exit.
                return Ok(());
            };

            // Set up a listener for shutdown notifications
            let mut shutdown_listener = match PgListener::connect_with(&self.queue.pool).await {
                Ok(listener) => listener,
                Err(err) => {
                    tracing::error!(
                        %err,
                        backoff_secs = reconnect_backoff.as_secs(),
                        attempt = retry_count,
                        "Failed to connect scheduler shutdown listener, retrying after backoff"
                    );
                    sleep(reconnect_backoff).await;
                    retry_count = retry_count.saturating_add(1);
                    continue 'reconnect;
                }
            };

            let chan = shutdown_channel();
            if let Err(err) = shutdown_listener.listen(chan).await {
                tracing::error!(
                    %err,
                    backoff_secs = reconnect_backoff.as_secs(),
                    attempt = retry_count,
                    "Failed to listen on scheduler shutdown channel, retrying after backoff"
                );
                sleep(reconnect_backoff).await;
                retry_count = retry_count.saturating_add(1);
                continue 'reconnect;
            }

            // Connection succeeded, reset retry counter
            tracing::info!("Scheduler PostgreSQL listener connected successfully");
            retry_count = 1;

            // TODO: Handle updates to schedules?

            for next in zoned_schedule.iter() {
                tracing::debug!(?next, "Waiting until next scheduled task enqueue");

                tokio::select! {
                    notify_shutdown = shutdown_listener.recv() => {
                        match notify_shutdown {
                            Ok(_) => {
                                self.shutdown_token.cancel();
                            },
                            Err(err) => {
                                tracing::warn!(%err, "Scheduler shutdown listener connection lost, reconnecting");
                                continue 'reconnect;
                            }
                        }
                    }

                    _ = self.shutdown_token.cancelled() => {
                        break;
                    }

                    _ = wait_until(&next) => {
                        self.process_next_schedule(&input).await?
                    }
                }
            }

            guard.release_now().await?;

            // Exit the reconnect loop once we have completed the schedule iteration.
            break;
        }

        Ok(())
    }

    #[instrument(skip_all, fields(task.id = tracing::field::Empty), err)]
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

async fn wait_until(next: &Zoned) {
    let tz = next.time_zone();
    loop {
        let now = Zoned::now().with_time_zone(tz.to_owned());
        if now >= *next {
            break;
        }

        let until_next = next.duration_until(&now).unsigned_abs();
        if until_next == StdDuration::ZERO {
            break;
        }

        tokio::time::sleep_until(Instant::now() + until_next).await;
    }
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

    /// Returns an iterator of `Zoned` where each is a time at which the
    /// schedule should fire.
    pub fn iter(&self) -> ZonedScheduleIterator<'_> {
        ZonedScheduleIterator {
            upcoming: self.schedule.upcoming(self.timezone.clone()),
        }
    }
}

pub struct ZonedScheduleIterator<'a> {
    upcoming: ScheduleIterator<'a>,
}

impl Iterator for ZonedScheduleIterator<'_> {
    type Item = Zoned;

    fn next(&mut self) -> Option<Self::Item> {
        self.upcoming.next()
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

#[cfg(test)]
mod tests {
    use std::time::SystemTime;

    use jiff::ToSpan;

    use super::*;

    #[test]
    fn zoned_schedule_creation_valid() {
        let cron_expr = "0 0 * * * * *"; // Every hour at minute 0
        let time_zone_name = "UTC";
        let schedule = ZonedSchedule::new(cron_expr, time_zone_name);

        assert!(
            schedule.is_ok(),
            "Expected ZonedSchedule to be created successfully"
        );
    }

    #[test]
    fn zoned_schedule_creation_invalid_cron() {
        let cron_expr = "invalid cron";
        let time_zone_name = "UTC";
        let schedule = ZonedSchedule::new(cron_expr, time_zone_name);

        assert!(
            schedule.is_err(),
            "Expected error due to invalid cron expression"
        );
    }

    #[test]
    fn zoned_schedule_creation_invalid_time_zone() {
        let cron_expr = "0 0 * * * * *";
        let time_zone_name = "Invalid/TimeZone";
        let schedule = ZonedSchedule::new(cron_expr, time_zone_name);

        assert!(schedule.is_err(), "Expected error due to invalid time zone");
    }

    #[test]
    fn zoned_schedule_parses() {
        "0 0 * * * *[America/Los_Angeles]"
            .parse::<ZonedSchedule>()
            .expect("A schedule should be parsed");
    }

    #[tokio::test]
    async fn wait_until_past_time() {
        let tz = TimeZone::UTC;
        let next = Zoned::now()
            .with_time_zone(tz.to_owned())
            .saturating_sub(10.seconds());

        let start = SystemTime::now();
        wait_until(&next).await;
        let elapsed = start.elapsed().unwrap();
        assert!(
            elapsed < StdDuration::from_millis(10),
            "Expected immediate return"
        );
    }

    #[tokio::test]
    async fn wait_until_future_time() {
        let tz = TimeZone::UTC;
        let next = Zoned::now()
            .with_time_zone(tz.to_owned())
            .saturating_add(5.seconds());

        // Pause and control tokio's time
        tokio::time::pause();

        let handle = tokio::spawn({
            let next = next.clone();
            async move { wait_until(&next).await }
        });
        tokio::time::advance(StdDuration::from_secs(5)).await;

        handle.await.expect("Failed to run wait_until");
        let elapsed: StdDuration = (&Zoned::now().with_time_zone(tz.to_owned()) - &next)
            .try_into()
            .unwrap();
        assert!(
            elapsed < StdDuration::from_millis(10),
            "Expected precise completion"
        );
    }

    #[tokio::test]
    async fn wait_until_exact_time() {
        let tz = TimeZone::UTC;
        let next = Zoned::now().with_time_zone(tz.to_owned());

        let start = SystemTime::now();
        wait_until(&next).await;
        let elapsed = start.elapsed().unwrap();
        assert!(
            elapsed < StdDuration::from_millis(10),
            "Expected immediate return"
        );
    }
}
