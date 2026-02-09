//! Activities for workflow side effects.
//!
//! Activities are units of side-effecting work that can be invoked by a
//! workflow. Unlike workflow code, activity handlers are expected to perform
//! external I/O such as HTTP requests, emails, or writes to other systems.

use std::{future::Future, result::Result as StdResult};

use jiff::{Span, ToSpan};
use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::task::RetryPolicy;

/// A type alias for activity execution results.
pub type Result<T> = StdResult<T, Error>;

/// Standard activity error envelope.
#[derive(Debug, Clone, Deserialize, Serialize, thiserror::Error)]
#[error("[{code}] {message}")]
pub struct Error {
    /// Stable activity-local error code.
    pub code: String,

    /// Human-readable description of the failure.
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

    /// Adds structured details to the error envelope.
    pub fn with_details(mut self, details: serde_json::Value) -> Self {
        self.details = Some(details);
        self
    }
}

/// Trait for activity handlers.
pub trait Activity: Send + Sync + 'static {
    /// Stable activity name.
    const NAME: &'static str;

    /// Input payload for this activity.
    type Input: DeserializeOwned + Serialize + Send + 'static;

    /// Output payload for this activity.
    type Output: DeserializeOwned + Serialize + Send + 'static;

    /// Executes the activity.
    fn execute(&self, input: Self::Input) -> impl Future<Output = Result<Self::Output>> + Send;

    /// Retry policy for this activity.
    fn retry_policy(&self) -> RetryPolicy {
        RetryPolicy::default()
    }

    /// Timeout for this activity invocation.
    fn timeout(&self) -> Span {
        15.minutes()
    }
}
