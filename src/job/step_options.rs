use jiff::{Span, ToSpan};

use crate::task::RetryPolicy;

/// Options for changing the defaults of each step in a `Job`.
///
/// These options are used as default implementations of the `Task` trait.
///
/// # Example
///
/// ```rust
/// use jiff::ToSpan;
/// use underway::job::StepOptions;
///
/// let options = StepOptions::builder()
///     .priority(1)
///     .timeout(30.minutes())
///     .build();
/// ```
#[derive(Debug, Clone, PartialEq)]
pub struct StepOptions {
    pub(crate) retry_policy: RetryPolicy,
    pub(crate) timeout: Span,
    pub(crate) ttl: Span,
    pub(crate) heartbeat: Span,
    pub(crate) delay: Span,
    pub(crate) concurrency_key: Option<String>,
    pub(crate) priority: i32,
}

impl Default for StepOptions {
    fn default() -> Self {
        Self {
            retry_policy: Default::default(),
            timeout: 15.minutes(),
            ttl: 14.days(),
            heartbeat: 30.seconds(),
            delay: Span::new(),
            concurrency_key: None,
            priority: 0,
        }
    }
}

impl StepOptions {
    /// Create a new builder.
    ///
    /// # Example
    ///
    /// ```rust
    /// use jiff::ToSpan;
    /// use underway::{job::StepOptions, task::RetryPolicy};
    ///
    /// let retry_policy = StepOptions::builder().timeout(1.hour()).build();
    /// ```
    pub fn builder() -> Builder {
        Builder {
            inner: StepOptions::default(),
        }
    }
}

#[derive(Debug, Default)]
pub struct Builder {
    inner: StepOptions,
}

impl Builder {
    /// Creates a new `Builder` with the default task options.
    pub fn new() -> Self {
        Self {
            inner: StepOptions::default(),
        }
    }

    /// Sets the retry policy.
    ///
    /// # Example
    ///
    /// ```rust
    /// use underway::{job::StepOptions, task::RetryPolicy};
    ///
    /// // Set max attempts to two.
    ///
    /// let policy = RetryPolicy::builder().max_attempts(2).build();
    /// let options = StepOptions::builder().retry_policy(policy).build();
    /// ```
    pub const fn retry_policy(mut self, retry_policy: RetryPolicy) -> Self {
        self.inner.retry_policy = retry_policy;
        self
    }

    /// Sets the timeout period for the task execution.
    ///
    /// Default value is 15 minutes.
    ///
    /// # Example
    ///
    /// ```rust
    /// use jiff::ToSpan;
    /// use underway::job::StepOptions;
    ///
    /// // Set the task timeout to 1 hour
    /// let options = StepOptions::builder().timeout(1.hour());
    /// ```
    pub const fn timeout(mut self, timeout: Span) -> Self {
        self.inner.timeout = timeout;
        self
    }

    /// Sets the TTL (time to live) period for the task to be kept in the
    /// database.
    ///
    /// Default value is 14 days.
    ///
    /// # Example
    ///
    /// ```rust
    /// use jiff::ToSpan;
    /// use underway::job::StepOptions;
    ///
    /// // Set the task TTL to 4 days
    /// let options = StepOptions::builder().ttl(4.days());
    /// ```
    pub const fn ttl(mut self, ttl: Span) -> Self {
        self.inner.ttl = ttl;
        self
    }

    /// Sets the task heartbeat.
    ///     
    /// Heartbeat is used to check the tasks liveness.
    ///
    /// Default value is 30 seconds.
    ///
    /// # Example
    ///
    /// ```rust
    /// use jiff::ToSpan;
    /// use underway::job::StepOptions;
    ///
    /// // Set the task heartbeat to 15 seconds
    /// let options = StepOptions::builder().heartbeat(15.seconds());
    /// ```
    pub const fn heartbeat(mut self, heartbeat: Span) -> Self {
        self.inner.heartbeat = heartbeat;
        self
    }

    /// Sets a delay for the task to wait before executing.
    ///
    /// Default value is 0.
    ///
    /// # Example
    ///
    /// ```rust
    /// use jiff::ToSpan;
    /// use underway::job::StepOptions;
    ///
    /// // Set a delay of 15 minutes for the task to begin
    /// let options = StepOptions::builder().delay(15.minutes());
    /// ```
    pub const fn delay(mut self, delay: Span) -> Self {
        self.inner.delay = delay;
        self
    }

    /// Sets a concurrency key that controls how many tasks run concurrently.
    ///
    /// Default value is None
    ///
    /// # Example
    ///
    /// ```rust
    /// use jiff::ToSpan;
    /// use underway::job::StepOptions;
    ///
    /// // Set the task TTL to 4 days
    /// let options = StepOptions::builder().concurrency_key("key".to_string());
    /// ```
    pub fn concurrency_key(mut self, concurrenty_key: String) -> Self {
        self.inner.concurrency_key = Some(concurrenty_key);
        self
    }

    /// Sets the task priority.
    ///
    /// Task priority makes polling for new tasks configurable.
    ///
    /// Default value is priority 0 .
    ///
    /// # Example
    ///
    /// ```rust
    /// use jiff::ToSpan;
    /// use underway::job::StepOptions;
    ///
    /// // Set the task priority to 1
    /// let options = StepOptions::builder().priority(1);
    /// ```
    pub const fn priority(mut self, priority: i32) -> Self {
        self.inner.priority = priority;
        self
    }

    /// Builds the `StepOptions` with the configured parameters.
    ///
    /// # Example
    ///
    /// ```rust
    /// use jiff::ToSpan;
    /// use underway::job::StepOptions;
    ///
    /// // Build a custom step options
    /// let options = StepOptions::builder()
    ///     .priority(1)
    ///     .timeout(45.minutes())
    ///     .build();
    /// ```
    pub fn build(self) -> StepOptions {
        self.inner
    }
}
