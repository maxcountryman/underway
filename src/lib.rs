//! # Underway
//!
//! ⏳ A PostgreSQL-backed job queue for reliable background task processing.
//!
//! Key Features:
//!
//! - **PostgreSQL-Backed** Built on PostgreSQL for robust task storage and
//!   coordination, ensuring consistency and safe concurrency across all
//!   operations.
//! - **Transactional Task Management** Supports enqueuing tasks within existing
//!   database transactions, guaranteeing that tasks are only added if the
//!   transaction commits successfully—perfect for operations like user
//!   registration.
//! - **Automatic Retries** Offers customizable retry strategies for failed
//!   executions, ensuring tasks are reliably completed even after transient
//!   failures.
//! - **Cron-Like Scheduling** Supports scheduling recurring tasks with
//!   cron-like expressions, enabling automated, time-based job execution.
//! - **Scalable and Flexible** Scales from a single worker to multiple workers
//!   with minimal configuration, allowing seamless background job processing.
//!
//! # Overview
//!
//! Underway provides a robust and efficient way to execute asynchronous tasks
//! using PostgreSQL as the backend for task storage and coordination. It is
//! designed to be simple, scalable, and resilient, handling job processing in
//! a way that ensures safe concurrency and reliable task execution. Whether
//! you're processing tasks on a single server or across multiple workers,
//! Underway makes it easy to manage background jobs with confidence.
//!
//! # Example
//!
//! Let's say we wanted to send welcome emails upon the successful registration
//! of a new user. If we're building a web application, we want to defer work
//! like this such that we aren't slowing down the response. We can use Underway
//! to create a background job for sending emails. For instance, this basic
//! example illustrates:
//!
//! ```rust,no_run
//! use std::env;
//!
//! use serde::{Deserialize, Serialize};
//! use sqlx::PgPool;
//! use underway::{Job, Queue};
//!
//! const QUEUE_NAME: &str = "email";
//!
//! #[derive(Debug, Clone, Deserialize, Serialize)]
//! struct WelcomeEmail {
//!     user_id: i32,
//!     email: String,
//!     name: String,
//! }
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // Set up the database connection pool.
//!     let database_url = &env::var("DATABASE_URL").expect("DATABASE_URL should be set");
//!     let pool = PgPool::connect(database_url).await?;
//!
//!     // Run migrations.
//!     underway::MIGRATOR.run(&pool).await?;
//!
//!     // Create the task queue.
//!     let queue = Queue::builder().name(QUEUE_NAME).pool(pool).build().await?;
//!
//!     // Build the job.
//!     let job = Job::builder()
//!         .queue(queue)
//!         .execute(
//!             |WelcomeEmail {
//!                  user_id,
//!                  email,
//!                  name,
//!              }| async move {
//!                 // Simulate sending an email.
//!                 println!("Sending welcome email to {name} <{email}> (user_id: {user_id})");
//!                 Ok(())
//!             },
//!         )
//!         .build();
//!
//!     // Enqueue a task.
//!     let task_id = job
//!         .enqueue(WelcomeEmail {
//!             user_id: 42,
//!             email: "ferris@example.com".to_string(),
//!             name: "Ferris".to_string(),
//!         })
//!         .await?;
//!
//!     println!("Enqueued task with ID: {}", task_id);
//!
//!     // Start the worker to process tasks.
//!     job.run().await?;
//!
//!     Ok(())
//! }
//! ```
//!
//! # Concepts
//!
//! Underway has been designed around several core concepts:
//!
//! - [Tasks](#tasks) represent a well-structure unit of work.
//! - [Jobs](#jobs) are a higher-level abstraction over the [`Task`] trait.
//! - [Queues](#queues) provide an interface for managing task execution.
//! - [Workers](#workers) interface with queues to execute tasks.
//!
//! ## Tasks
//!
//! Tasks are units of work to be executed, with clearly defined behavior and
//! input.
//!
//! This is the lowest-level concept in our design, with everything else being
//! built around this idea.
//!
//! See [`task`] for more details about tasks.
//!
//! ## Jobs
//!
//! Jobs are a specialized task which provide a higher-level API for defining
//! and operating tasks.
//!
//! In most cases, applications will use jobs to define tasks instead of using
//! the task trait directly.
//!
//! See [`job`] for more details about jobs.
//!
//! ## Queues
//!
//! Queues manage tasks, including enqueuing and dequeuing them from the
//! database.
//!
//! See [`queue`] for more details about queues.
//!
//! ## Workers
//!
//! Workers are derived from tasks and use their queue to find new work to
//! execute.
//!
//! Besides this polling style of work execution, workers are also used to
//! manage cron-like schedules.
//!
//! See [`worker`] for more details about workers.

#![warn(clippy::all, nonstandard_style, future_incompatible, missing_docs)]
#![forbid(unsafe_code)]

use sqlx::migrate::Migrator;

pub use crate::{
    job::Job,
    queue::Queue,
    scheduler::{Scheduler, ZonedSchedule},
    task::Task,
    worker::Worker,
};

pub mod job;
pub mod queue;
mod scheduler;
pub mod task;
pub mod worker;

/// SQLx [`Migrator`] which provides `underway`'s schema migrations.
///
/// These migrations must be applied before queues, tasks, and workers can be
/// run.
///
/// **Note: Changes are managed within a dedicated schema, called "underway".**
///
/// # Example
///
///```rust
/// # use tokio::runtime::Runtime;
/// use std::env;
///
/// use sqlx::PgPool;
///
/// # fn main() {
/// # let rt = Runtime::new().unwrap();
/// # rt.block_on(async {
/// // Set up the database connection pool.
/// let database_url = &env::var("DATABASE_URL")?;
/// let pool = PgPool::connect(database_url).await?;
///
/// // Run migrations.
/// underway::MIGRATOR.run(&pool).await?;
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// # });
/// # }
pub static MIGRATOR: Migrator = sqlx::migrate!();

/// Error enum which provides all `underway` error types.
#[derive(Debug, thiserror::Error)]
enum Error {
    /// Job-related errors.
    #[error(transparent)]
    Job(#[from] job::Error),

    /// Queue-relayed errors.
    #[error(transparent)]
    Queue(#[from] queue::Error),

    /// Task-relayed errors.
    #[error(transparent)]
    Task(#[from] task::Error),

    /// Worker-relayed errors.
    #[error(transparent)]
    Worker(#[from] worker::Error),
}
