//! # Underway
//!
//! ⏳ **Underway** is a distributed task queue built on top of PostgreSQL.
//!
//! ## Overview
//!
//! Underway provides a reliable and efficient way to execute asynchronous tasks
//! in a distributed environment, leveraging PostgreSQL as a backend for task
//! storage and coordination. It is designed to be simple, scalable, and
//! resilient, making it suitable for a wide range of applications.
//!
//!
//!
//! ## Overview
//!
//! Underway is composed of several key concepts that are designed to make
//! authoring well-structured background jobs straightforward.
//!
//! - [Task](#task)
//! - [Job](#job)
//! - [Queue](#queue)
//! - [Worker](#worker)
//!
//! ### Task
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
//! ### Job
//!
//! A [`Job`] is a provided implementation of [`Task`] that offers a
//! higher-level abstraction for interacting with its queue and workers.
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
//!         .execute(|input: DataProcessingInput| async move {
//!             // Process the data.
//!             println!("Processing data with ID: {}", input.data_id);
//!             Ok(())
//!         })
//!         .build()
//!         .await?;
//!
//!     // Enqueue the job.
//!     let input = DataProcessingInput { data_id: 42 };
//!     job.enqueue(input).await?;
//!
//!     // Optionally, run the job immediately.
//!     // job.run().await?;
//!
//!     Ok(())
//! }
//! ```
//!
//! ### Queue
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
//! ### Worker
//!
//! A [`Worker`] continuously polls the queue for tasks and executes them. It
//! works with any type that implements [`Task`].
//!
//! Until a worker is run, tasks remain on their queue, waiting to be processed.
//!
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
//!
//! ## Example
//!
//! Combining the concepts, here's how you can set up a task queue and worker to
//! send welcome emails using [`Job`] for added convenience:
//!
//! ```rust,no_run
//! use serde::{Deserialize, Serialize};
//! use sqlx::PgPool;
//! use underway::{JobBuilder, QueueBuilder};
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
//!     let pool = PgPool::connect("postgres://user:password@localhost/database").await?;
//!
//!     // Run migrations.
//!     underway::MIGRATOR.run(&pool).await?;
//!
//!     // Create the task queue.
//!     let queue = QueueBuilder::new()
//!         .name("email_queue")
//!         .pool(pool.clone())
//!         .build()
//!         .await?;
//!
//!     // Build the job.
//!     let job = JobBuilder::new(queue)
//!         .execute(|input: WelcomeEmail| async move {
//!             // Simulate sending an email.
//!             println!(
//!                 "Sending welcome email to {} <{}> (user_id: {})",
//!                 input.name, input.email, input.user_id
//!             );
//!             Ok(())
//!         })
//!         .build()
//!         .await?;
//!
//!     // Enqueue a task.
//!     let input = WelcomeEmail {
//!         user_id: 1,
//!         email: "user@example.com".to_string(),
//!         name: "Alice".to_string(),
//!     };
//!     let task_id = job.enqueue(input).await?;
//!     println!("Enqueued task with ID: {}", task_id);
//!
//!     // Start the worker to process tasks.
//!     job.run().await?;
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

pub static MIGRATOR: Migrator = sqlx::migrate!();
