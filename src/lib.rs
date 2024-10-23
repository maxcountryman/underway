//! # Underway
//!
//! ⏳ A PostgreSQL-backed job queue for reliable background task processing.
//!
//! Key Features:
//!
//! - **PostgreSQL-Backed** Leverages PostgreSQL with `FOR UPDATE SKIP LOCKED`
//!   for reliable task storage and coordination, ensuring efficient, safe
//!   concurrency.
//! - **Atomic Task Management**: Enqueue tasks within your transactions and use
//!   the worker's transaction within your tasks for atomic database
//!   queries—ensuring consistent workflows.
//! - **Automatic Retries**: Configurable retry strategies ensure tasks are
//!   reliably completed, even after transient failures.
//! - **Cron-Like Scheduling**: Schedule recurring tasks with cron-like
//!   expressions for automated, time-based job execution.
//! - **Scalable and Flexible**: Easily scales from a single worker to many,
//!   enabling seamless background job processing with minimal setup.
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
//! like this so we can return a response quickly back to the browser. We can
//! use Underway to create a background job for sending emails without blocking
//! the response:
//!
//! ```rust,no_run
//! use std::env;
//!
//! use serde::{Deserialize, Serialize};
//! use sqlx::PgPool;
//! use underway::{Job, To};
//!
//! #[derive(Deserialize, Serialize)]
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
//!     // Build the job.
//!     let job = Job::builder()
//!         .step(
//!             |_ctx,
//!              WelcomeEmail {
//!                  user_id,
//!                  email,
//!                  name,
//!              }| async move {
//!                 // Simulate sending an email.
//!                 println!("Sending welcome email to {name} <{email}> (user_id: {user_id})");
//!                 To::done()
//!             },
//!         )
//!         .name("welcome-email")
//!         .pool(pool)
//!         .build()
//!         .await?;
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
//!     // Start processing enqueued tasks.
//!     job.start().await??;
//!
//!     Ok(())
//! }
//! ```
//!
//! Another common use case is doing something after some expensive bit of work
//! has been done with that output. For instance, we might generate PDF receipts
//!  of orders and then send these via email to our customers. Because jobs are
//! a series of sequential steps, we can write a job that handles PDF generation
//! and then sends a receipt email.
//!
//! ```rust,no_run
//! use std::env;
//!
//! use serde::{Deserialize, Serialize};
//! use sqlx::PgPool;
//! use underway::{Job, To};
//!
//! #[derive(Deserialize, Serialize)]
//! struct GenerateReceipt {
//!     // The order we want to generate a receipt for.
//!     order_id: i32,
//! }
//!
//! #[derive(Deserialize, Serialize)]
//! struct EmailReceipt {
//!     // The object store key to our receipt PDF.
//!     receipt_key: String,
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
//!     // Build the job.
//!     let job = Job::builder()
//!         .step(|_ctx, GenerateReceipt { order_id }| async move {
//!             // Use the order ID to build a receipt PDF...
//!             // ...store the PDF in an object store.
//!             let receipt_key = format!("receipts_bucket/{order_id}-receipt.pdf");
//!             To::next(EmailReceipt { receipt_key })
//!         })
//!         .step(|_ctx, EmailReceipt { receipt_key }| async move {
//!             // Retrieve the PDF from the object store, and send the email.
//!             println!("Emailing receipt for {receipt_key}");
//!             To::done()
//!         })
//!         .name("email-receipt")
//!         .pool(pool)
//!         .build()
//!         .await?;
//!
//!     // Enqueue a task.
//!     let task_id = job.enqueue(GenerateReceipt { order_id: 42 }).await?;
//!
//!     println!("Enqueued task with ID: {}", task_id);
//!
//!     // Start processing enqueued tasks.
//!     job.start().await??;
//!
//!     Ok(())
//! }
//! ```
//!
//! If the email sending service is down, our email receipt step might fail. But
//! because the receipt PDF has already been built, when we retry we'll only
//! retry the email receipt step, without redoing the PDF generation step.
//!
//! # Concepts
//!
//! Underway has been designed around several core concepts, which build on one
//! another to deliver a robust background-job framework:
//!
//! - [Tasks](#tasks) represent a well-structure unit of work.
//! - [Jobs](#jobs) are a series of sequential steps, where each step is a
//!   [`Task`].
//! - [Queues](#queues) provide an interface for managing task execution.
//! - [Workers](#workers) interface with queues to execute tasks.
//!
//! ## Tasks
//!
//! Tasks are units of work to be executed, with clearly defined behavior and
//! input.
//!
//! This is the lowest-level concept in our design, with everything else being
//! built on top of or around this idea.
//!
//! See [`task`] for more details about tasks.
//!
//! ## Jobs
//!
//! Jobs are a series of sequential steps. Each step provides input to the next
//! step in the series.
//!
//! In most cases, applications will use jobs to define tasks instead of using
//! the `Task` trait directly.
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
//! Workers are responsible for executing tasks. They poll the queue for new
//! tasks, and when found, try to invoke the task's execute routine.
//!
//! See [`worker`] for more details about workers.

#![warn(clippy::all, nonstandard_style, future_incompatible, missing_docs)]

use sqlx::migrate::Migrator;

pub use crate::{
    job::{Job, To},
    queue::Queue,
    scheduler::{Scheduler, ZonedSchedule},
    task::{Task, ToTaskResult},
    worker::Worker,
};

pub mod job;
pub mod queue;
mod scheduler;
pub mod task;
pub mod worker;

/// A SQLx [`Migrator`] which provides Underway's schema migrations.
///
/// These migrations must be applied before queues, tasks, and workers can be
/// run.
///
/// **Note**: Changes are managed within a dedicated schema, called "underway".
///
/// # Example
///
///```rust,no_run
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
