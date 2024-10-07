//! # Underway
//!
//! ⏳ A PostgreSQL-backed job queue for reliable background task processing.
//!
//! Key Features:
//!
//! - **PostgreSQL-Backed** Leverages PostgreSQL for reliable task storage and
//!   coordination, ensuring high consistency and safe concurrency.
//! - **Transactional Task Enqueueing** Supports enqueuing tasks within a
//!   provided database transaction, ensuring they are only added if the
//!   transaction commits successfully—ideal for operations like user
//!   registration.
//! - **Automatic Retries** Offers customizable retry strategies for failed
//!   jobs, ensuring tasks are reliably completed even after transient failures.
//! - **Cron-Like Scheduling** Supports scheduling recurring tasks with
//!   cron-like expressions, enabling automated, time-based job execution.
//! - **Scalable and Flexible** Scales from a single worker to multiple workers
//!   with minimal configuration, allowing seamless background job processing.
//!
//! ## Overview
//!
//! Underway provides a robust and efficient way to execute asynchronous tasks
//! using PostgreSQL as the backend for task storage and coordination. It is
//! designed to be simple, scalable, and resilient, handling job processing in
//! a way that ensures safe concurrency and reliable task execution. Whether
//! you're processing tasks on a single server or across multiple workers,
//! Underway makes it easy to manage background jobs with confidence.
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
//! use underway::{JobBuilder, QueueBuilder};
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
//!     let queue = QueueBuilder::new()
//!         .name(QUEUE_NAME)
//!         .pool(pool)
//!         .build()
//!         .await?;
//!
//!     // Build the job.
//!     let job = JobBuilder::new(queue)
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
//!     // Start the worker to process tasks.
//!     job.run().await?;
//!
//!     Ok(())
//! }
//! ```
//!
//! ## Concepts
//!
//! Underway has been designed around several core concepts:
//!
//! - [Tasks](#tasks) represent a well-structure unit of work.
//! - [Jobs](#jobs) are a higher-level abstraction over the [`Task`] trait.
//! - [Queues](#queues) manage the lifecycle of tasks.
//! - [Workers](#workers) interface with queues to execute tasks.
//!
//! Each of these is described in more detail below. Additionally, each concept
//! is contained by module that provides comprehensive documentation of its API
//! and how you might use it in your own applications.
//!
//! ### Tasks
//!
//! A [`Task`] defines the core behavior and input type of a unit of work to be
//! executed. Tasks are strongly typed, ensuring that the data passed to the
//! task is well-formed and predictable.
//!
//! Implementations of `Task` also may provide various configuration of the task
//! itself. For instance, tasks may define a custom
//! [`RetryPolicy`](task::RetryPolicy) or configure their priority.
//!
//! ```rust
//! use serde::{Deserialize, Serialize};
//! use underway::{task::Error as TaskError, Task};
//! # use tokio::runtime::Runtime;
//! #
//! # fn main() {
//! # let rt = Runtime::new().unwrap();
//! # rt.block_on(async {
//!
//! // Define the input data for the task.
//! #[derive(Debug, Deserialize, Serialize)]
//! struct WelcomeEmail {
//!     user_id: i32,
//!     email: String,
//!     name: String,
//! }
//!
//! // Implement the task.
//! struct WelcomeEmailTask;
//!
//! impl Task for WelcomeEmailTask {
//!     type Input = WelcomeEmail;
//!
//!     async fn execute(&self, input: Self::Input) -> Result<(), TaskError> {
//!         // Simulate sending an email.
//!         println!(
//!             "Sending welcome email to {} <{}> (user_id: {})",
//!             input.name, input.email, input.user_id
//!         );
//!         Ok(())
//!     }
//! }
//! # });
//! # }
//! ```
//!
//! ### Jobs
//!
//! A [`Job`] is an implementation of [`Task`] that provides a higher-level API
//! for defining and operating tasks.
//!
//! Jobs are defined using the [`JobBuilder`]. Because jobs are tasks, they also
//! encode the same configuration, such as retry policies. This configuration is
//! achieved through the builder.
//!
//! ```rust,no_run
//! use serde::{Deserialize, Serialize};
//! use sqlx::PgPool;
//! use underway::{JobBuilder, QueueBuilder};
//!
//! #[derive(Debug, Clone, Deserialize, Serialize)]
//! struct DataProcessingInput {
//!     data_id: u64,
//! }
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let pool = PgPool::connect("postgres://user:password@localhost/database").await?;
//!
//!     // Create a queue.
//!     let queue = QueueBuilder::new()
//!         .name("data_processing_queue")
//!         .pool(pool)
//!         .build()
//!         .await?;
//!
//!     // Build a job.
//!     let job = JobBuilder::new(queue)
//!         .execute(|DataProcessingInput { data_id }| async move {
//!             // Process the data.
//!             println!("Processing data with ID: {data_id}");
//!             Ok(())
//!         })
//!         .build()
//!         .await?;
//!
//!     // Enqueue the job.
//!     job.enqueue(DataProcessingInput { data_id: 42 }).await?;
//!
//!     // Optionally, run the job immediately.
//!     // job.run().await?;
//!
//!     Ok(())
//! }
//! ```
//!
//! ### Queues
//!
//! A [`Queue`] manages tasks, including enqueuing and dequeuing them from the
//! database. It serves as a conduit between tasks and the underlying storage
//! system.
//!
//! Queues provide tasks in accordance with their configuration. For example, a
//! task may define a priority, but otherwise is dequeued in a first-in,
//! first-out manner.
//!
//! Because queues are generic over the task, there is always a one-to-one
//! relationship between a queue and the concrete task type it contains.
//!
//! ```rust,no_run
//! use serde::{Deserialize, Serialize};
//! use sqlx::PgPool;
//! use underway::{
//!     queue::Error as QueueError, task::Result as TaskResult, Queue, QueueBuilder, Task,
//! };
//!
//! #[derive(Debug, Clone, Deserialize, Serialize)]
//! struct MyTaskInput {
//!     data: String,
//! }
//!
//! struct MyTask;
//!
//! impl Task for MyTask {
//!     type Input = MyTaskInput;
//!
//!     async fn execute(&self, input: Self::Input) -> TaskResult {
//!         println!("Executing task with data: {}", input.data);
//!         Ok(())
//!     }
//! }
//!
//! async fn create_queue(pool: PgPool) -> Result<Queue<MyTask>, QueueError> {
//!     let queue = QueueBuilder::new()
//!         .name("my_task_queue")
//!         .pool(pool)
//!         .build()
//!         .await?;
//!     Ok(queue)
//! }
//! ```
//!
//! ### Workers
//!
//! A [`Worker`] continuously polls the queue for tasks and executes them. It
//! works with any type that implements [`Task`].
//!
//! Until a worker is run, tasks remain on their queue, waiting to be processed.
//! ```rust,no_run
//! use serde::{Deserialize, Serialize};
//! use sqlx::PgPool;
//! use underway::{task::Result as TaskResult, QueueBuilder, Task, Worker};
//!
//! #[derive(Debug, Clone, Deserialize, Serialize)]
//! struct MyTaskInput {
//!     data: String,
//! }
//!
//! struct MyTask;
//!
//! impl Task for MyTask {
//!     type Input = MyTaskInput;
//!
//!     async fn execute(&self, input: Self::Input) -> TaskResult {
//!         println!("Executing task with data: {}", input.data);
//!         Ok(())
//!     }
//! }
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let pool = PgPool::connect("postgres://user:password@localhost/database").await?;
//!
//!     let queue = QueueBuilder::new()
//!         .name("my_task_queue")
//!         .pool(pool.clone())
//!         .build()
//!         .await?;
//!
//!     let task = MyTask;
//!     let worker = Worker::new(queue, task);
//!     worker.run().await?;
//!
//!     Ok(())
//! }
//! ```

#![forbid(unsafe_code)]

use sqlx::migrate::Migrator;

pub use crate::{
    job::{Job, JobBuilder},
    queue::{Queue, QueueBuilder},
    task::Task,
    worker::Worker,
};

pub mod job;
pub mod queue;
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
