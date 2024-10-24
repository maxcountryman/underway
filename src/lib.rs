//! # Underway
//!
//! ⏳ Durable step functions via Postgres.
//!
//! # Overview
//!
//! **Underway** is a framework for building robust, asynchronous background
//! jobs in Rust, leveraging PostgreSQL as its queuing backend. It provides a
//! streamlined interface for defining jobs as a series of "steps," where each
//! step's output becomes the input for the next. This design enables the
//! construction of complex, durable, and resilient workflows with ease.
//!
//! Key Features:
//!
//! - **PostgreSQL-Backed** Leverages PostgreSQL with `FOR UPDATE SKIP LOCKED`
//!   for reliable task storage and coordination, ensuring efficient, safe
//!   concurrency.
//! - **Atomic Task Management**: Enqueue tasks within your transactions and use
//!   the worker's transaction within your tasks for atomic database
//!   queries--ensuring consisteny.
//! - **Automatic Retries**: Configurable retry strategies ensure tasks are
//!   reliably completed, even after transient failures.
//! - **Cron-Like Scheduling**: Schedule recurring tasks with cron-like
//!   expressions for automated, time-based job execution.
//! - **Scalable and Flexible**: Easily scales from a single worker to many,
//!   enabling seamless background job processing with minimal setup.
//!
//! # Examples
//!
//! Underway is suitable for many different use cases, ranging from simple
//! single-step jobs to more sophisticated multi-step jobs, where dependencies
//! are built up between steps.
//!
//! ## Welcome emails
//!
//! A common use case is deferring work that can be processed later. For
//! instance, during user registration, we might want to send a welcome email to
//! new users. Rather than handling this within the registration process (e.g.,
//! form validation, database insertion), we can offload it to run "out-of-band"
//! using Underway. By defining a job for sending the welcome email, Underway
//! ensures it gets processed in the background, without slowing down the user
//! registration flow.
//!
//! ```rust,no_run
//! use std::env;
//!
//! use serde::{Deserialize, Serialize};
//! use sqlx::PgPool;
//! use underway::{Job, To};
//!
//! // This is the input we'll provide to the job when we enqueue it.
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
//!             |_cx,
//!              WelcomeEmail {
//!                  user_id,
//!                  email,
//!                  name,
//!              }| async move {
//!                 // Simulate sending an email.
//!                 println!("Sending welcome email to {name} <{email}> (user_id: {user_id})");
//!                 // Returning this indicates this is the final step.
//!                 To::done()
//!             },
//!         )
//!         .name("welcome-email")
//!         .pool(pool)
//!         .build()
//!         .await?;
//!
//!     // Here we enqueue a new job to be processed later.
//!     job.enqueue(WelcomeEmail {
//!         user_id: 42,
//!         email: "ferris@example.com".to_string(),
//!         name: "Ferris".to_string(),
//!     })
//!     .await?;
//!
//!     // Start processing enqueued tasks.
//!     job.start().await??;
//!
//!     Ok(())
//! }
//! ```
//!
//! ## Order receipts
//!
//! Another common use case is defining dependencies between discrete steps of a
//! job. For instance, we might generate PDF receipts for orders and then email
//! these to customers. With Underway, each step is handled separately, making
//! it easy to create a job that first generates the PDF and, once
//! completed, proceeds to send the email.
//!
//! This separation provides significant value: if the email sending service
//! is temporarily unavailable, we can retry the email step without having to
//! regenerate the PDF, avoiding unnecessary repeated work.
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
//!     // An order we want to generate a receipt for.
//!     order_id: i32,
//! }
//!
//! #[derive(Deserialize, Serialize)]
//! struct EmailReceipt {
//!     // An object store key to our receipt PDF.
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
//!         .step(|_cx, GenerateReceipt { order_id }| async move {
//!             // Use the order ID to build a receipt PDF...
//!             let receipt_key = format!("receipts_bucket/{order_id}-receipt.pdf");
//!             // ...store the PDF in an object store.
//!
//!             // We proceed to the next step with the receipt_key as its input.
//!             To::next(EmailReceipt { receipt_key })
//!         })
//!         .step(|_cx, EmailReceipt { receipt_key }| async move {
//!             // Retrieve the PDF from the object store, and send the email.
//!             println!("Emailing receipt for {receipt_key}");
//!             To::done()
//!         })
//!         .name("order-receipt")
//!         .pool(pool)
//!         .build()
//!         .await?;
//!
//!     // Enqueue the job for the given order.
//!     job.enqueue(GenerateReceipt { order_id: 42 }).await?;
//!
//!     // Start processing enqueued jobs.
//!     job.start().await??;
//!
//!     Ok(())
//! }
//! ```
//!
//! With this setup, if the email service is down, the `EmailReceipt` step can
//! be retried without redoing the PDF generation, saving time and resources by
//! not repeating the expensive step of generating the PDF.
//!
//! ## Daily reports
//!
//! Jobs may also be run on a schedule. This makes them useful for situations
//! where we want to do things on a regular cadence, such as creating a daily
//! business report.
//!
//! ```rust,no_run
//! use std::env;
//!
//! use serde::{Deserialize, Serialize};
//! use sqlx::PgPool;
//! use underway::{Job, To};
//!
//! #[derive(Deserialize, Serialize)]
//! struct GenerateReport;
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
//!         .step(|_cx, _| async move {
//!             // Here we would generate and store the report.
//!             To::done()
//!         })
//!         .name("daily-report")
//!         .pool(pool)
//!         .build()
//!         .await?;
//!
//!     // Set a daily schedule with the given input.
//!     let daily = "@daily[America/Los_Angeles]".parse()?;
//!     job.schedule(daily, GenerateReport).await?;
//!
//!     // Start processing enqueued jobs.
//!     job.start().await??;
//!
//!     Ok(())
//! }
//! ```
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
