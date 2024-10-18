use jiff::{Span, ToSpan};

use crate::task::DequeuedTask;

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
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct RetryPolicy {
    pub(crate) max_attempts: i32,
    pub(crate) initial_interval_ms: i32,
    pub(crate) max_interval_ms: i32,
    pub(crate) backoff_coefficient: f32,
}

pub(crate) type RetryCount = i32;

impl RetryPolicy {
    /// Create a new builder.
    pub fn builder() -> Builder {
        Builder::default()
    }

    pub(crate) fn calculate_delay(&self, retry_count: RetryCount) -> Span {
        let base_delay = self.initial_interval_ms as f32;
        let backoff_delay = base_delay * self.backoff_coefficient.powi(retry_count - 1);
        let delay = backoff_delay.min(self.max_interval_ms as f32) as i64;
        delay.milliseconds()
    }
}

impl From<DequeuedTask> for RetryPolicy {
    fn from(
        DequeuedTask {
            max_attempts,
            initial_interval_ms,
            max_interval_ms,
            backoff_coefficient,
            ..
        }: DequeuedTask,
    ) -> Self {
        Self {
            max_attempts,
            initial_interval_ms,
            max_interval_ms,
            backoff_coefficient,
        }
    }
}

const DEFAULT_RETRY_POLICY: RetryPolicy = RetryPolicy {
    max_attempts: 5,
    initial_interval_ms: 1_000,
    max_interval_ms: 60_000,
    backoff_coefficient: 2.0,
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
    /// Creates a new `RetryPolicyBuilder` with the default retry settings.
    pub const fn new() -> Self {
        Self {
            inner: DEFAULT_RETRY_POLICY,
        }
    }

    /// Sets the maximum number of retry attempts.
    ///
    /// Default value is `5`.
    pub const fn max_attempts(mut self, max_attempts: i32) -> Self {
        self.inner.max_attempts = max_attempts;
        self
    }

    /// Sets the initial interval before the first retry (in milliseconds).
    ///
    /// Default value is `1_000`.
    pub const fn initial_interval_ms(mut self, initial_interval_ms: i32) -> Self {
        self.inner.initial_interval_ms = initial_interval_ms;
        self
    }

    /// Sets the maximum interval between retries (in milliseconds).
    ///
    /// Default value is `60_000`.
    pub const fn max_interval_ms(mut self, max_interval_ms: i32) -> Self {
        self.inner.max_interval_ms = max_interval_ms;
        self
    }

    /// Sets the backoff coefficient to apply after each retry.
    ///
    /// Default value is `2.0`.
    pub const fn backoff_coefficient(mut self, backoff_coefficient: f32) -> Self {
        self.inner.backoff_coefficient = backoff_coefficient;
        self
    }

    /// Builds the `RetryPolicy` with the configured parameters.
    pub const fn build(self) -> RetryPolicy {
        self.inner
    }
}
