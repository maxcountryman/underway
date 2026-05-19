use super::*;

/// A type alias for activity execution results.
pub type Result<T> = StdResult<T, Error>;

/// Durable activity call state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum CallState {
    /// Activity call is queued and waiting to be processed.
    Pending,

    /// Activity call is currently being processed.
    InProgress,

    /// Activity call completed successfully.
    Succeeded,

    /// Activity call completed unsuccessfully.
    Failed,
}

/// Standard activity error envelope.
#[derive(Debug, Clone, Deserialize, Serialize, thiserror::Error)]
#[error("[{code}] {message}")]
pub struct Error {
    /// Stable activity-local error code.
    pub code: String,

    /// Human-readable failure message.
    pub message: String,

    /// Whether the failure should be retried.
    pub retryable: bool,

    /// Optional structured details.
    pub details: Option<serde_json::Value>,
}

impl Error {
    /// Creates a retryable activity error.
    pub fn retryable(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
            retryable: true,
            details: None,
        }
    }

    /// Creates a non-retryable activity error.
    pub fn fatal(code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
            retryable: false,
            details: None,
        }
    }

    /// Adds structured details.
    pub fn with_details(mut self, details: serde_json::Value) -> Self {
        self.details = Some(details);
        self
    }
}

/// Trait for durable side-effect handlers.
pub trait Activity: Send + Sync + 'static {
    /// Stable persisted activity name.
    const NAME: &'static str;

    /// Activity input payload.
    type Input: DeserializeOwned + Serialize + Send + 'static;

    /// Activity output payload.
    type Output: DeserializeOwned + Serialize + Send + 'static;

    /// Execute the side effect.
    fn execute(&self, input: Self::Input) -> impl Future<Output = Result<Self::Output>> + Send;

    /// Retry policy for this activity.
    fn retry_policy(&self) -> task::RetryPolicy {
        task::RetryPolicy::default()
    }

    /// Activity timeout.
    fn timeout(&self) -> Span {
        15.minutes()
    }
}
