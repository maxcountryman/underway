use super::*;

pub(crate) type RetryCount = i32;

/// Task identifier.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Hash, Eq, PartialEq)]
pub struct TaskId(Uuid);

impl TaskId {
    /// Generates a new task ID.
    pub fn new() -> Self {
        Self(Ulid::new().into())
    }

    /// Reconstitutes a task ID from a UUID.
    pub const fn from_uuid(id: Uuid) -> Self {
        Self(id)
    }

    /// Returns the underlying UUID.
    pub const fn as_uuid(self) -> Uuid {
        self.0
    }

    /// Returns hyphenated UUID formatting.
    pub fn as_hyphenated(&self) -> &uuid::fmt::Hyphenated {
        self.0.as_hyphenated()
    }
}

impl Default for TaskId {
    fn default() -> Self {
        Self::new()
    }
}

impl Deref for TaskId {
    type Target = Uuid;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Display for TaskId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// A type alias for task execution results.
pub type Result<T> = StdResult<T, Error>;

/// A type alias for task configuration results.
pub type ConfigResult<T = TaskConfig> = StdResult<T, ConfigError>;

/// Task execution errors.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// Indicates the task timed out.
    #[error("Task timed out after {0} during execution")]
    TimedOut(SignedDuration),

    /// Indicates execution suspended while waiting for durable external
    /// work.
    #[error("{0}")]
    Suspended(String),

    /// Unrecoverable error.
    #[error("{0}")]
    Fatal(String),

    /// Recoverable error.
    #[error("{0}")]
    Retryable(String),
}

/// Task configuration validation errors.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum ConfigError {
    /// Retry policy must allow at least one attempt.
    #[error("retry policy max_attempts must be at least 1, got {0}")]
    InvalidMaxAttempts(i32),

    /// Retry initial interval must not be negative.
    #[error("retry policy initial_interval_ms must be non-negative, got {0}")]
    InvalidInitialInterval(i32),

    /// Retry maximum interval must not be negative.
    #[error("retry policy max_interval_ms must be non-negative, got {0}")]
    InvalidMaxInterval(i32),

    /// Retry maximum interval must be greater than or equal to the initial
    /// interval.
    #[error(
        "retry policy max_interval_ms must be greater than or equal to initial_interval_ms, got \
         max={max} and initial={initial}"
    )]
    InvalidIntervalRange {
        /// Configured maximum interval.
        max: i32,
        /// Configured initial interval.
        initial: i32,
    },

    /// Retry backoff coefficient must be finite and at least one.
    #[error("retry policy backoff_coefficient must be finite and at least 1, got {0}")]
    InvalidBackoffCoefficient(f64),

    /// Retry jitter factor must be finite and between zero and one.
    #[error("retry policy jitter_factor must be finite and between 0 and 1, got {0}")]
    InvalidJitterFactor(f64),

    /// Duration field must be representable as an invariant unsigned
    /// duration.
    #[error("task config {field} must be a non-negative invariant duration: {message}")]
    InvalidDuration {
        /// Field being validated.
        field: &'static str,
        /// Conversion failure message.
        message: String,
    },

    /// Duration field must be greater than zero.
    #[error("task config {0} must be greater than zero")]
    NonPositiveDuration(&'static str),

    /// Heartbeat interval cannot be greater than the task timeout.
    #[error("task config heartbeat must be less than or equal to timeout")]
    HeartbeatExceedsTimeout,

    /// Concurrency keys must not be empty.
    #[error("task config concurrency_key must not be empty")]
    EmptyConcurrencyKey,

    /// Error returned by span arithmetic.
    #[error(transparent)]
    Jiff(#[from] jiff::Error),
}

/// Converts external errors into task errors.
pub trait ToTaskResult<T> {
    /// Converts the error to retryable.
    fn retryable(self) -> StdResult<T, Error>;

    /// Converts the error to fatal.
    fn fatal(self) -> StdResult<T, Error>;
}

impl<T, E: Display> ToTaskResult<T> for StdResult<T, E> {
    fn retryable(self) -> StdResult<T, Error> {
        self.map_err(|err| Error::Retryable(err.to_string()))
    }

    fn fatal(self) -> StdResult<T, Error> {
        self.map_err(|err| Error::Fatal(err.to_string()))
    }
}

/// Retry policy for task or activity attempts.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct RetryPolicy {
    /// Maximum attempts before terminal failure.
    pub max_attempts: i32,
    /// Initial backoff interval in milliseconds.
    pub initial_interval_ms: i32,
    /// Maximum backoff interval in milliseconds.
    pub max_interval_ms: i32,
    /// Exponential backoff coefficient.
    pub backoff_coefficient: f64,
    /// Jitter coefficient.
    pub jitter_factor: f64,
}

impl RetryPolicy {
    /// Creates a retry policy builder.
    pub fn builder() -> RetryPolicyBuilder {
        RetryPolicyBuilder::default()
    }

    /// Validates the retry policy invariants.
    pub fn validate(&self) -> ConfigResult<()> {
        if self.max_attempts < 1 {
            return Err(ConfigError::InvalidMaxAttempts(self.max_attempts));
        }
        if self.initial_interval_ms < 0 {
            return Err(ConfigError::InvalidInitialInterval(
                self.initial_interval_ms,
            ));
        }
        if self.max_interval_ms < 0 {
            return Err(ConfigError::InvalidMaxInterval(self.max_interval_ms));
        }
        if self.max_interval_ms < self.initial_interval_ms {
            return Err(ConfigError::InvalidIntervalRange {
                max: self.max_interval_ms,
                initial: self.initial_interval_ms,
            });
        }
        if !self.backoff_coefficient.is_finite() || self.backoff_coefficient < 1.0 {
            return Err(ConfigError::InvalidBackoffCoefficient(
                self.backoff_coefficient,
            ));
        }
        if !self.jitter_factor.is_finite() || !(0.0..=1.0).contains(&self.jitter_factor) {
            return Err(ConfigError::InvalidJitterFactor(self.jitter_factor));
        }

        Ok(())
    }

    /// Calculates the delay for a retry count.
    pub fn calculate_delay(&self, retry_count: RetryCount) -> Span {
        let base_delay = self.initial_interval_ms as f64;
        let backoff_delay = base_delay * self.backoff_coefficient.powi(retry_count - 1);
        let target_delay = backoff_delay.min(self.max_interval_ms as f64);
        let jitter = target_delay * self.jitter_factor;
        let delay = if jitter == 0.0 {
            target_delay
        } else {
            target_delay * (1.0 - self.jitter_factor) + rand::random::<f64>() * jitter
        };
        (delay as i64).milliseconds()
    }
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            max_attempts: 5,
            initial_interval_ms: 1_000,
            max_interval_ms: 60_000,
            backoff_coefficient: 2.0,
            jitter_factor: 0.5,
        }
    }
}

/// Builder for [`RetryPolicy`].
#[derive(Debug, Default)]
pub struct RetryPolicyBuilder {
    inner: RetryPolicy,
}

impl RetryPolicyBuilder {
    /// Sets maximum attempts.
    pub const fn max_attempts(mut self, max_attempts: i32) -> Self {
        self.inner.max_attempts = max_attempts;
        self
    }

    /// Sets initial interval in milliseconds.
    pub const fn initial_interval_ms(mut self, initial_interval_ms: i32) -> Self {
        self.inner.initial_interval_ms = initial_interval_ms;
        self
    }

    /// Sets max interval in milliseconds.
    pub const fn max_interval_ms(mut self, max_interval_ms: i32) -> Self {
        self.inner.max_interval_ms = max_interval_ms;
        self
    }

    /// Sets backoff coefficient.
    pub const fn backoff_coefficient(mut self, backoff_coefficient: f64) -> Self {
        self.inner.backoff_coefficient = backoff_coefficient;
        self
    }

    /// Sets jitter factor.
    pub const fn jitter_factor(mut self, jitter_factor: f64) -> Self {
        self.inner.jitter_factor = jitter_factor;
        self
    }

    /// Builds the policy.
    pub const fn build(self) -> RetryPolicy {
        self.inner
    }
}

/// Possible task states.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum State {
    /// Awaiting execution.
    Pending,
    /// Currently in progress.
    InProgress,
    /// Suspended on durable activity completion.
    Waiting,
    /// Succeeded.
    Succeeded,
    /// Cancelled.
    Cancelled,
    /// Failed.
    Failed,
}

/// Task execution configuration.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct TaskConfig {
    retry_policy: RetryPolicy,
    timeout: Span,
    ttl: Span,
    delay: Span,
    heartbeat: Span,
    concurrency_key: Option<String>,
    priority: i32,
}

impl Default for TaskConfig {
    fn default() -> Self {
        Self {
            retry_policy: RetryPolicy::default(),
            timeout: 15.minutes(),
            ttl: 14.days(),
            delay: Span::new(),
            heartbeat: 30.seconds(),
            concurrency_key: None,
            priority: 0,
        }
    }
}

impl<'de> Deserialize<'de> for TaskConfig {
    fn deserialize<D>(deserializer: D) -> StdResult<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct RawTaskConfig {
            retry_policy: RetryPolicy,
            timeout: Span,
            ttl: Span,
            delay: Span,
            heartbeat: Span,
            concurrency_key: Option<String>,
            priority: i32,
        }

        let raw = RawTaskConfig::deserialize(deserializer)?;
        Self {
            retry_policy: raw.retry_policy,
            timeout: raw.timeout,
            ttl: raw.ttl,
            delay: raw.delay,
            heartbeat: raw.heartbeat,
            concurrency_key: raw.concurrency_key,
            priority: raw.priority,
        }
        .validated()
        .map_err(serde::de::Error::custom)
    }
}

impl TaskConfig {
    /// Creates a task configuration builder.
    pub fn builder() -> TaskConfigBuilder {
        TaskConfigBuilder::default()
    }

    /// Creates a builder initialized from this configuration.
    pub fn into_builder(self) -> TaskConfigBuilder {
        TaskConfigBuilder { inner: self }
    }

    /// Validates this configuration.
    pub fn validate(&self) -> ConfigResult<()> {
        self.retry_policy.validate()?;

        let timeout = positive_duration("timeout", self.timeout)?;
        positive_duration("ttl", self.ttl)?;
        non_negative_duration("delay", self.delay)?;
        let heartbeat = positive_duration("heartbeat", self.heartbeat)?;

        if heartbeat > timeout {
            return Err(ConfigError::HeartbeatExceedsTimeout);
        }

        if self
            .concurrency_key
            .as_deref()
            .is_some_and(|key| key.is_empty())
        {
            return Err(ConfigError::EmptyConcurrencyKey);
        }

        Ok(())
    }

    /// Returns this configuration after validating it.
    pub fn validated(self) -> ConfigResult<Self> {
        self.validate()?;
        Ok(self)
    }

    /// Returns this configuration with additional enqueue delay applied.
    pub fn with_additional_delay(mut self, delay: Span) -> ConfigResult<Self> {
        non_negative_duration("additional delay", delay)?;
        self.delay = self.delay.checked_add(delay)?;
        self.validated()
    }

    /// Returns the retry policy.
    pub const fn retry_policy(&self) -> RetryPolicy {
        self.retry_policy
    }

    /// Returns the execution timeout.
    pub const fn timeout(&self) -> Span {
        self.timeout
    }

    /// Returns the retention TTL.
    pub const fn ttl(&self) -> Span {
        self.ttl
    }

    /// Returns the base delay.
    pub const fn delay(&self) -> Span {
        self.delay
    }

    /// Returns the heartbeat interval.
    pub const fn heartbeat(&self) -> Span {
        self.heartbeat
    }

    /// Returns the optional serializing key.
    pub fn concurrency_key(&self) -> Option<&str> {
        self.concurrency_key.as_deref()
    }

    /// Returns the priority. Higher values are claimed first.
    pub const fn priority(&self) -> i32 {
        self.priority
    }

    pub(crate) fn set_retry_policy(&mut self, retry_policy: RetryPolicy) {
        self.retry_policy = retry_policy;
    }

    pub(crate) fn set_timeout(&mut self, timeout: Span) {
        self.timeout = timeout;
    }

    pub(crate) fn set_ttl(&mut self, ttl: Span) {
        self.ttl = ttl;
    }

    pub(crate) fn set_delay(&mut self, delay: Span) {
        self.delay = delay;
    }

    pub(crate) fn set_heartbeat(&mut self, heartbeat: Span) {
        self.heartbeat = heartbeat;
    }

    pub(crate) fn set_concurrency_key(&mut self, concurrency_key: Option<String>) {
        self.concurrency_key = concurrency_key;
    }

    pub(crate) fn set_priority(&mut self, priority: i32) {
        self.priority = priority;
    }
}

/// Builder for [`TaskConfig`].
#[derive(Debug, Clone, Default)]
pub struct TaskConfigBuilder {
    inner: TaskConfig,
}

impl TaskConfigBuilder {
    /// Sets the retry policy.
    pub fn retry_policy(mut self, retry_policy: RetryPolicy) -> Self {
        self.inner.retry_policy = retry_policy;
        self
    }

    /// Sets the execution timeout.
    pub fn timeout(mut self, timeout: Span) -> Self {
        self.inner.timeout = timeout;
        self
    }

    /// Sets the retention TTL.
    pub fn ttl(mut self, ttl: Span) -> Self {
        self.inner.ttl = ttl;
        self
    }

    /// Sets the base delay.
    pub fn delay(mut self, delay: Span) -> Self {
        self.inner.delay = delay;
        self
    }

    /// Sets the heartbeat interval.
    pub fn heartbeat(mut self, heartbeat: Span) -> Self {
        self.inner.heartbeat = heartbeat;
        self
    }

    /// Sets the optional serializing key.
    pub fn concurrency_key(mut self, concurrency_key: impl Into<String>) -> Self {
        self.inner.concurrency_key = Some(concurrency_key.into());
        self
    }

    /// Sets the optional serializing key.
    pub fn maybe_concurrency_key(mut self, concurrency_key: Option<String>) -> Self {
        self.inner.concurrency_key = concurrency_key;
        self
    }

    /// Clears the optional serializing key.
    pub fn clear_concurrency_key(mut self) -> Self {
        self.inner.concurrency_key = None;
        self
    }

    /// Sets the priority. Higher values are claimed first.
    pub fn priority(mut self, priority: i32) -> Self {
        self.inner.priority = priority;
        self
    }

    /// Builds a validated task configuration.
    pub fn build(self) -> ConfigResult<TaskConfig> {
        self.inner.validated()
    }
}

fn non_negative_duration(field: &'static str, span: Span) -> ConfigResult<StdDuration> {
    StdDuration::try_from(span).map_err(|err| ConfigError::InvalidDuration {
        field,
        message: err.to_string(),
    })
}

fn positive_duration(field: &'static str, span: Span) -> ConfigResult<StdDuration> {
    let duration = non_negative_duration(field, span)?;
    if duration.is_zero() {
        return Err(ConfigError::NonPositiveDuration(field));
    }
    Ok(duration)
}

/// Trait for defining executable tasks.
pub trait Task<B: backend::Backend>: Send + Sync + 'static {
    /// Input payload.
    type Input: DeserializeOwned + Serialize + Send + 'static;

    /// Output payload.
    type Output: Serialize + Send + 'static;

    /// Executes a task inside the backend transaction that claimed it.
    fn execute(
        &self,
        tx: &mut B::Transaction<'_>,
        input: Self::Input,
    ) -> impl Future<Output = Result<Self::Output>> + Send;

    /// Default task configuration.
    fn config(&self) -> ConfigResult {
        Ok(TaskConfig::default())
    }

    /// Per-input task configuration.
    fn config_for(&self, _input: &Self::Input) -> ConfigResult {
        self.config()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_task_config_is_valid() {
        TaskConfig::default().validate().unwrap();
    }

    #[test]
    fn task_config_rejects_zero_timeout() {
        let err = TaskConfig::builder()
            .timeout(Span::new())
            .build()
            .unwrap_err();

        assert!(matches!(err, ConfigError::NonPositiveDuration("timeout")));
    }

    #[test]
    fn task_config_rejects_heartbeat_greater_than_timeout() {
        let err = TaskConfig::builder()
            .timeout(10.seconds())
            .heartbeat(30.seconds())
            .build()
            .unwrap_err();

        assert!(matches!(err, ConfigError::HeartbeatExceedsTimeout));
    }

    #[test]
    fn task_config_accepts_zero_delay() {
        TaskConfig::builder().delay(Span::new()).build().unwrap();
    }

    #[test]
    fn retry_policy_rejects_invalid_attempts() {
        let policy = RetryPolicy {
            max_attempts: 0,
            ..RetryPolicy::default()
        };

        assert!(matches!(
            policy.validate().unwrap_err(),
            ConfigError::InvalidMaxAttempts(0)
        ));
    }
}
