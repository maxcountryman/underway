use std::{result::Result as StdResult, str::FromStr, time::Duration as StdDuration};

use jiff::{tz::TimeZone, Span, ToSpan, Zoned};
use jiff_cron::Schedule;
use serde::{de::DeserializeOwned, Serialize};

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
pub struct Scheduler<T: Task> {
    queue: Queue<T>,
    task: T,
}

impl<T: Task> Scheduler<T> {
    /// Creates a new scheduler.
    pub fn new(queue: Queue<T>, task: T) -> Self {
        Self { queue, task }
    }

    /// Runs the scheduler in a loop, sleeping one-second per iteration.
    pub async fn run(&self) -> Result {
        self.run_every(1.second()).await
    }

    /// Runs the scheduler in a loop, sleeping for the given span per iteration.
    pub async fn run_every(&self, span: Span) -> Result {
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

impl<I> From<Job<I>> for Scheduler<Job<I>>
where
    I: Clone + DeserializeOwned + Serialize + Send + 'static,
{
    fn from(job: Job<I>) -> Self {
        Self {
            queue: job.queue.clone(),
            task: job,
        }
    }
}

impl<I> From<&Job<I>> for Scheduler<Job<I>>
where
    I: Clone + DeserializeOwned + Serialize + Send + 'static,
{
    fn from(job: &Job<I>) -> Self {
        Self {
            queue: job.queue.clone(),
            task: job.clone(),
        }
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

    pub(crate) fn duration_until_next(&self) -> Option<StdDuration> {
        // Construct a date-time with the schedule's time zone.
        let now_with_tz = Zoned::now().with_time_zone(self.timezone.clone());
        if let Some(next_timestamp) = self.schedule.upcoming(self.timezone.clone()).next() {
            let until_next = now_with_tz.duration_until(&next_timestamp).unsigned_abs();
            return Some(until_next);
        }

        None
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
