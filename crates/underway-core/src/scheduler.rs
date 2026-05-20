use std::str::FromStr;

use jiff::tz::TimeZone;

use super::*;

/// Scheduler for task schedules.
pub struct Scheduler<B, T>
where
    B: backend::Backend,
    T: task::Task<B>,
{
    queue: Arc<queue::Queue<B, T>>,
    task: Arc<T>,
    shutdown_token: CancellationToken,
}

impl<B, T> Scheduler<B, T>
where
    B: backend::Backend,
    T: task::Task<B>,
{
    /// Creates a scheduler.
    pub fn new(queue: Arc<queue::Queue<B, T>>, task: T) -> Self {
        Self {
            queue,
            task: Arc::new(task),
            shutdown_token: CancellationToken::new(),
        }
    }

    /// Sets shutdown token.
    pub fn set_shutdown_token(&mut self, shutdown_token: CancellationToken) {
        self.shutdown_token = shutdown_token;
    }

    /// Stops this scheduler.
    pub fn shutdown(&self) {
        self.shutdown_token.cancel();
    }

    /// Runs the scheduler.
    pub async fn run(&self) -> Result {
        let refresh = StdDuration::from_secs(5);

        loop {
            if self.shutdown_token.is_cancelled() {
                return Ok(());
            }

            match self
                .queue
                .backend
                .try_acquire_scheduler(&self.queue.name)
                .await
                .map_err(Error::backend)?
            {
                Some(mut lease) => {
                    let result = self.run_with_lease(refresh).await;
                    lease.release().await.map_err(Error::backend)?;
                    result?;
                }
                None => {
                    tokio::select! {
                        _ = self.shutdown_token.cancelled() => return Ok(()),
                        _ = sleep(refresh) => {}
                    }
                }
            }
        }
    }

    async fn run_with_lease(&self, refresh: StdDuration) -> Result {
        loop {
            let mut tx = self.queue.backend.begin().await.map_err(Error::backend)?;
            let schedule = self.queue.load_task_schedule(&mut tx).await?;
            tx.commit().await.map_err(Error::backend)?;

            if let Some(stored) = schedule {
                let Some(next) = stored.schedule.iter().next() else {
                    tokio::select! {
                        _ = self.shutdown_token.cancelled() => return Ok(()),
                        _ = sleep(refresh) => continue,
                    }
                };

                let tz = next.time_zone();
                let now = Zoned::now().with_time_zone(tz.to_owned());
                let until_next = next.duration_until(&now).unsigned_abs();
                let should_dispatch = until_next <= refresh;
                let wait_for = if should_dispatch { until_next } else { refresh };

                tokio::select! {
                    _ = self.shutdown_token.cancelled() => return Ok(()),
                    _ = sleep(wait_for) => {
                        if should_dispatch {
                            let input: T::Input = serde_json::from_value(stored.input.clone())?;
                            self.queue.enqueue(&self.task, &input).await?;
                        }
                    }
                }
            } else {
                tokio::select! {
                    _ = self.shutdown_token.cancelled() => return Ok(()),
                    _ = sleep(refresh) => {}
                }
            }
        }
    }
}

/// Schedule paired with a time zone.
#[derive(Debug, Clone, PartialEq)]
pub struct ZonedSchedule {
    schedule: Schedule,
    timezone: TimeZone,
}

impl ZonedSchedule {
    /// Creates a schedule.
    pub fn new(cron_expr: &str, time_zone_name: &str) -> StdResult<Self, ZonedScheduleError> {
        let schedule = cron_expr.parse()?;
        let timezone = TimeZone::get(time_zone_name)?;
        if timezone.iana_name().is_none() {
            return Err(ZonedScheduleError::Parse(
                "time zone must use an IANA name".to_string(),
            ));
        }
        Ok(Self { schedule, timezone })
    }

    /// Cron expression string.
    pub fn cron_expr(&self) -> String {
        self.schedule.to_string()
    }

    /// IANA time zone name.
    pub fn iana_name(&self) -> &str {
        self.timezone.iana_name().expect("validated by constructor")
    }

    /// Upcoming schedule iterator.
    pub fn iter(&self) -> ZonedScheduleIterator<'_> {
        ZonedScheduleIterator {
            upcoming: self.schedule.upcoming(self.timezone.clone()),
        }
    }
}

/// Upcoming schedule iterator.
pub struct ZonedScheduleIterator<'a> {
    upcoming: ScheduleIterator<'a>,
}

impl Iterator for ZonedScheduleIterator<'_> {
    type Item = Zoned;

    fn next(&mut self) -> Option<Self::Item> {
        self.upcoming.next()
    }
}

/// Zoned schedule parsing errors.
#[derive(Debug, thiserror::Error)]
pub enum ZonedScheduleError {
    /// Jiff error.
    #[error(transparent)]
    Jiff(#[from] jiff::Error),
    /// Cron error.
    #[error(transparent)]
    Cron(#[from] jiff_cron::error::Error),
    /// Parse error.
    #[error("Parsing error: {0}")]
    Parse(String),
}

impl FromStr for ZonedSchedule {
    type Err = ZonedScheduleError;

    fn from_str(s: &str) -> StdResult<Self, Self::Err> {
        if !s.ends_with(']') {
            return Err(ZonedScheduleError::Parse("Missing closing ']'".to_string()));
        }
        let open = s
            .find('[')
            .ok_or_else(|| ZonedScheduleError::Parse("Missing opening '['".to_string()))?;
        Self::new(&s[..open], &s[open + 1..s.len() - 1])
    }
}
