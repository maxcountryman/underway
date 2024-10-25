use std::{
    future, result::Result as StdResult, str::FromStr, sync::Arc, time::Duration as StdDuration,
};

use jiff::{tz::TimeZone, Span, ToSpan, Zoned};
use jiff_cron::Schedule;
use sqlx::postgres::PgAdvisoryLock;
use tracing::instrument;

use crate::{
    queue::{try_acquire_advisory_lock, Error as QueueError},
    Queue, Task,
};

pub(crate) type Result<T = ()> = std::result::Result<T, Error>;

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error(transparent)]
    Queue(#[from] QueueError),

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
}

impl<T: Task> Scheduler<T> {
    /// Creates a new scheduler.
    pub fn new(queue: Queue<T>, task: T) -> Self {
        let queue_lock = queue_scheduler_lock(&queue.name);
        Self {
            queue,
            queue_lock,
            task: Arc::new(task),
        }
    }

    /// Runs the scheduler in a loop, sleeping one-second per iteration.
    pub async fn run(&self) -> Result {
        self.run_every(1.second()).await
    }

    /// Runs the scheduler in a loop, sleeping for the given period per
    /// iteration.
    pub async fn run_every(&self, period: Span) -> Result {
        let conn = self.queue.pool.acquire().await.map_err(QueueError::from)?;
        let Some(_guard) = try_acquire_advisory_lock(conn, &self.queue_lock).await? else {
            // We can't acquire the lock, so we'll return a future that waits forever.
            return future::pending().await;
        };

        let mut interval = tokio::time::interval(period.try_into()?);
        interval.tick().await;
        loop {
            // TODO: It would be preferrable to not check the schedule every second and wait
            // for a NOTIFY instead.
            if let Some((zoned_schedule, input)) =
                self.queue.task_schedule(&self.queue.pool).await?
            {
                // TODO: If we were waiting for a NOTIFY or timeout, we could keep processing
                // the same schedule without fetching from the database.
                if let Some(until_next) = zoned_schedule.into_iter().next() {
                    self.process_next_schedule(until_next, input).await?
                }
            }

            interval.tick().await;
        }
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
    async fn process_next_schedule(&self, until_next: StdDuration, input: T::Input) -> Result {
        tracing::debug!(?until_next, "Sleeping until the next scheduled enqueue");
        tokio::time::sleep(until_next).await;

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
