//! Activities for durable workflow side effects.
//!
//! Activity definitions are split into two parts:
//! - [`Activity`], the durable contract (name + input/output types), and
//! - [`ActivityHandler`], the runtime implementation bound on a runtime.
//!
//! Workflow steps invoke activities through
//! [`workflow::InvokeActivity`](crate::workflow::InvokeActivity). Calls are
//! persisted and executed asynchronously by the runtime's activity worker.
//!
//! # Defining activity contracts and handlers
//!
//! ```rust,no_run
//! use serde::{Deserialize, Serialize};
//! use sqlx::PgPool;
//! use underway::{
//!     activity::{Activity, ActivityHandler, Error, Result},
//!     workflow::InvokeActivity,
//!     Transition, Workflow,
//! };
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
//! }
//!
//! struct SendWelcomeEmailHandler;
//!
//! impl ActivityHandler<SendWelcomeEmail> for SendWelcomeEmailHandler {
//!     async fn execute(&self, input: SendEmail) -> Result<()> {
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
//!
//! # use tokio::runtime::Runtime;
//! # fn main() {
//! # let rt = Runtime::new().unwrap();
//! # rt.block_on(async {
//! let pool = PgPool::connect(&std::env::var("DATABASE_URL")?).await?;
//!
//! let workflow = Workflow::<SendEmail, ()>::builder()
//!     .declare::<SendWelcomeEmail>()
//!     .step(|mut cx, input| async move {
//!         SendWelcomeEmail::emit(&mut cx, &input).await?;
//!         Transition::complete()
//!     })
//!     .name("send-welcome-email")
//!     .pool(pool.clone())
//!     .build()
//!     .await?;
//!
//! workflow
//!     .runtime()
//!     .bind::<SendWelcomeEmail>(SendWelcomeEmailHandler)?
//!     .run()
//!     .await?;
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! # });
//! # }
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

/// Durable activity contract.
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
}

/// Runtime implementation for an [`Activity`] contract.
pub trait ActivityHandler<A>: Send + Sync + 'static
where
    A: Activity,
{
    /// Executes the activity implementation.
    fn execute(&self, input: A::Input) -> impl Future<Output = Result<A::Output>> + Send;

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
