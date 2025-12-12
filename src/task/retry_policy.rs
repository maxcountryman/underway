use jiff::{Span, ToSpan};
use rand::Rng;

/// Configuration of a policy for retries in case of task failure.
///
/// # Example
///
/// ```rust
/// use underway::task::RetryPolicy;
///
/// let retry_policy = RetryPolicy::builder()
///     .max_attempts(10)
///     .backoff_coefficient(4.0)
///     .build();
/// ```
#[derive(Debug, Clone, Copy, PartialEq, sqlx::Type)]
#[sqlx(type_name = "underway.task_retry_policy")]
pub struct RetryPolicy {
    pub(crate) max_attempts: i32,
    pub(crate) initial_interval_ms: i32,
    pub(crate) max_interval_ms: i32,
    pub(crate) backoff_coefficient: f64,
    pub(crate) jitter_factor: f64,
}

pub(crate) type RetryCount = i32;

impl RetryPolicy {
    /// Create a new builder.
    ///
    /// # Example
    ///
    /// ```rust
    /// use underway::task::RetryPolicy;
    ///
    /// let retry_policy = RetryPolicy::builder().backoff_coefficient(3.0).build();
    /// ```
    pub fn builder() -> Builder {
        Builder::default()
    }

    pub(crate) fn calculate_delay(&self, retry_count: RetryCount) -> Span {
        let base_delay = self.initial_interval_ms as f64;
        let backoff_delay = base_delay * self.backoff_coefficient.powi(retry_count - 1);
        let target_delay = backoff_delay.min(self.max_interval_ms as f64);
        let delay=(target_delay * (1.0 - self.jitter_factor)+rand::thread_rng().gen_range(0.0..=(target_delay * self.jitter_factor))) as i64;
        delay.milliseconds()
    }
}

const DEFAULT_RETRY_POLICY: RetryPolicy = RetryPolicy {
    max_attempts: 5,
    initial_interval_ms: 1_000,
    max_interval_ms: 60_000,
    backoff_coefficient: 2.0,
    jitter_factor: 0.5,
};

impl Default for RetryPolicy {
    fn default() -> Self {
        DEFAULT_RETRY_POLICY
    }
}

#[derive(Debug, Default)]
pub struct Builder {
    inner: RetryPolicy,
}

impl Builder {
    /// Creates a new `Builder` with the default retry settings.
    pub const fn new() -> Self {
        Self {
            inner: DEFAULT_RETRY_POLICY,
        }
    }

    /// Sets the maximum number of retry attempts.
    ///
    /// Default value is `5`.
    ///
    /// # Example
    ///
    /// ```rust
    /// use underway::task::RetryPolicy;
    ///
    /// // The max attempts to two.
    /// let retry_policy_builder = RetryPolicy::builder().max_attempts(2);
    /// ```
    pub const fn max_attempts(mut self, max_attempts: i32) -> Self {
        self.inner.max_attempts = max_attempts;
        self
    }

    /// Sets the initial interval before the first retry (in milliseconds).
    ///
    /// Default value is `1_000`.
    ///
    /// # Example
    ///
    /// ```rust
    /// use underway::task::RetryPolicy;
    ///
    /// // Set an initial interval of two and a half seconds.
    /// let retry_policy_builder = RetryPolicy::builder().initial_interval_ms(2_500);
    /// ```
    pub const fn initial_interval_ms(mut self, initial_interval_ms: i32) -> Self {
        self.inner.initial_interval_ms = initial_interval_ms;
        self
    }

    /// Sets the maximum interval between retries (in milliseconds).
    ///
    /// Default value is `60_000`.
    ///
    /// # Example
    ///
    /// ```rust
    /// use underway::task::RetryPolicy;
    ///
    /// // Set a maximum interval of five minutes.
    /// let retry_policy_builder = RetryPolicy::builder().max_interval_ms(300_000);
    /// ```
    pub const fn max_interval_ms(mut self, max_interval_ms: i32) -> Self {
        self.inner.max_interval_ms = max_interval_ms;
        self
    }

    /// Sets the backoff coefficient to apply after each retry.
    ///
    /// The backoff coefficient controls the rate at which the delay grows with
    /// each retry. By adjusting this coefficient:
    ///
    /// - Higher Coefficient: Increases the delay faster with each retry.
    /// - Lower Coefficient: Slows the delay growth, creating smaller increases
    ///   between retries.
    ///
    /// Default value is `2.0`.
    ///
    /// # Example
    ///
    /// ```rust
    /// use underway::task::RetryPolicy;
    ///
    /// // Set a backoff coefficient of one point five.
    /// let retry_policy_builder = RetryPolicy::builder().backoff_coefficient(1.5);
    /// ```
    pub const fn backoff_coefficient(mut self, backoff_coefficient: f64) -> Self {
        self.inner.backoff_coefficient = backoff_coefficient;
        self
    }

    /// The jitter_factor is a coefficient (typically between 0.0 and 1.0)
    /// that determines what percentage of the backoff duration should
    /// be randomized. It controls the balance between predictability
    /// and collision avoidance.
    ///
    /// Default value is `0.5`.
    ///
    /// # Example
    ///
    /// ```rust
    /// use underway::task::RetryPolicy;
    ///
    /// // Set a backoff coefficient of one point five.
    /// let retry_policy_builder = RetryPolicy::builder().jitter_factor(0.5);
    /// ```
    pub const fn jitter_factor(mut self, jitter_factor: f64) -> Self {
        self.inner.jitter_factor=jitter_factor;
        self
    }

    /// Builds the `RetryPolicy` with the configured parameters.
    ///
    /// # Example
    ///
    /// ```rust
    /// use underway::task::RetryPolicy;
    ///
    /// // Build a custom retry policy.
    /// let retry_policy = RetryPolicy::builder()
    ///     .max_attempts(20)
    ///     .backoff_coefficient(3.0)
    ///     .build();
    /// ```
    pub const fn build(self) -> RetryPolicy {
        self.inner
    }
}
