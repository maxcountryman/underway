use std::{result::Result as StdResult, str::FromStr, time::Duration as StdDuration};

use jiff::{tz::TimeZone, Span, ToSpan, Zoned};
use jiff_cron::Schedule;
use serde::{de::DeserializeOwned, Serialize};
use sqlx::postgres::PgAdvisoryLock;

use crate::{queue::Error as QueueError, Job, Queue, Task};

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
    task: T,
}

impl<T: Task> Scheduler<T> {
    /// Creates a new scheduler.
    pub fn new(queue: Queue<T>, task: T) -> Self {
        let queue_lock = queue_scheduler_lock(&queue.name);
        Self {
            queue,
            queue_lock,
            task,
        }
    }

    /// Runs the scheduler in a loop, sleeping one-second per iteration.
    pub async fn run(&self) -> Result {
        self.run_every(1.second()).await
    }

    /// Runs the scheduler in a loop, sleeping for the given span per iteration.
    pub async fn run_every(&self, span: Span) -> Result {
        let Some(_guard) = self
            .queue
            .try_acquire_advisory_lock(&self.queue_lock)
            .await?
        else {
            tracing::warn!("Scheduler lock could not be acquired, scheduler exiting");
            return Ok(());
        };

        let mut interval = tokio::time::interval(span.try_into()?);
        interval.tick().await;
        loop {
            self.process_next_schedule().await?;
            interval.tick().await;
        }
    }

    async fn process_next_schedule(&self) -> Result {
        let (zoned_schedule, input) = self.queue.task_schedule(&self.queue.pool).await?;

        if let Some(until_next) = zoned_schedule.duration_until_next() {
            tokio::time::sleep(until_next).await;
            self.queue
                .enqueue(&self.queue.pool, &self.task, input)
                .await?;
        }

        Ok(())
    }
}

impl<I, O, S> From<Job<I, O, S>> for Scheduler<Job<I, O, S>>
where
    I: Clone + DeserializeOwned + Serialize + Send + 'static,
    O: Clone + Serialize + Send + 'static,
    S: Clone + Send + Sync + 'static,
{
    fn from(job: Job<I, O, S>) -> Self {
        Self {
            queue: job.queue.clone(),
            queue_lock: queue_scheduler_lock(&job.queue.name),
            task: job,
        }
    }
}

impl<I, O, S> From<&Job<I, O, S>> for Scheduler<Job<I, O, S>>
where
    I: Clone + DeserializeOwned + Serialize + Send + 'static,
    O: Clone + Serialize + Send + 'static,
    S: Clone + Send + Sync + 'static,
{
    fn from(job: &Job<I, O, S>) -> Self {
        Self {
            queue: job.queue.clone(),
            queue_lock: queue_scheduler_lock(&job.queue.name),
            task: job.clone(),
        }
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

    pub(crate) fn duration_until_next(&self) -> Option<StdDuration> {
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
