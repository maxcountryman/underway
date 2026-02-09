//! Activities for durable workflow side effects.
//!
//! Activities are units of side-effecting work invoked by workflow steps.
//! Unlike workflow code, activity handlers are expected to perform external
//! I/O such as HTTP requests, emails, or writes to other systems.
//!
//! Workflow steps call activities via [`crate::job::Context::call`] and
//! [`crate::job::Context::emit`]. Calls are persisted and executed by the
//! activity worker managed by [`crate::Runtime`].
//!
//! # Defining activities
//!
//! Activities are ordinary Rust types implementing [`Activity`].
//!
//! ```rust
//! use serde::{Deserialize, Serialize};
//! use underway::activity::{Activity, Error, Result};
//!
//! #[derive(Deserialize, Serialize)]
//! struct SendEmail {
//!     to: String,
//!     subject: String,
//! }
//!
//! struct SendWelcomeEmail;
//!
//! impl Activity for SendWelcomeEmail {
//!     const NAME: &'static str = "send-welcome-email";
//!
//!     type Input = SendEmail;
//!     type Output = ();
//!
//!     async fn execute(&self, input: Self::Input) -> Result<Self::Output> {
//!         if input.to.is_empty() {
//!             return Err(Error::fatal(
//!                 "missing_recipient",
//!                 "recipient email is empty",
//!             ));
//!         }
//!
//!         Ok(())
//!     }
//! }
//! ```
//!
//! # Error handling
//!
//! Activity errors use a standard envelope ([`Error`]) with:
//! - a stable machine-friendly code,
//! - a human-readable message,
//! - a retryable flag,
//! - optional structured details.
//!
//! This shape keeps failures durable and serializable across retries,
//! process restarts, and replays.

use std::{future::Future, result::Result as StdResult};

use jiff::{Span, ToSpan};
use serde::{de::DeserializeOwned, Deserialize, Serialize};

use crate::task::RetryPolicy;

/// A type alias for activity execution results.
pub type Result<T> = StdResult<T, Error>;

/// Durable activity call state.
#[derive(Debug, Clone, Copy, PartialEq, Eq, sqlx::Type)]
#[sqlx(type_name = "underway.activity_call_state", rename_all = "snake_case")]
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
    ///
    /// This name is persisted in the database and used for handler lookup.
    /// Treat it as part of your durable contract.
    const NAME: &'static str;

    /// Input payload for this activity.
    type Input: DeserializeOwned + Serialize + Send + 'static;

    /// Output payload for this activity.
    type Output: DeserializeOwned + Serialize + Send + 'static;

    /// Executes the activity.
    ///
    /// This is expected to perform side-effecting I/O.
    fn execute(&self, input: Self::Input) -> impl Future<Output = Result<Self::Output>> + Send;

    /// Retry policy for this activity.
    ///
    /// Defaults to [`RetryPolicy::default`].
    fn retry_policy(&self) -> RetryPolicy {
        RetryPolicy::default()
    }

    /// Timeout for this activity invocation.
    ///
    /// Defaults to 15 minutes.
    fn timeout(&self) -> Span {
        15.minutes()
    }
}
