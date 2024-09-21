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
//! **Key Features:**
//!
//! - **Distributed Task Execution:** Easily distribute tasks across multiple
//!   workers and machines.
//! - **Reliable Persistence:** Tasks are stored in PostgreSQL, ensuring
//!   durability and reliability.
//! - **Strongly Typed Tasks:** Define tasks with strongly typed inputs using
//!   Rust's type system.
//! - **Flexible Retry Policies:** Configure custom retry strategies with
//!   exponential backoff.
//! - **Concurrency Control:** Limit task execution with concurrency keys to
//!   prevent race conditions.
//! - **Priority Queueing:** Assign priorities to tasks to control execution
//!   order.
//! - **Dead Letter Queue (DLQ):** Automatically handle failed tasks by moving
//!   them to a DLQ for later inspection.
//!
//! ## Getting Started
//!
//! ### Database Setup
//!
//! Underway requires a PostgreSQL database. You need to apply the necessary
//! migrations before using the library. You can apply the migrations using
//! `sqlx`:
//! ```rust,no_run
//! use sqlx::PgPool;
//! use underway::MIGRATOR;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), sqlx::Error> {
//!     let pool = PgPool::connect("postgres://user:password@localhost/database").await?;
//!     MIGRATOR.run(&pool).await?;
//!     Ok(())
//! }
//! ```
//!
//! ## Concepts
//!
//! - [Task](#task)
//! - [Job](#job)
//! - [Queue](#queue)
//! - [Worker](#worker)
//!
//! ### Task
//!
//! A `Task` defines the core behavior and input type of a unit of work to be
//! executed. Tasks are strongly typed, ensuring that the data passed to the
//! task is well-formed and predictable.
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
//! A `Job` is a specialized implementation of a `Task` that provides additional
//! functionality for task management within the queueing system. It not only
//! defines how to execute a task but also includes methods to enqueue, cancel,
//! and run tasks, integrating tightly with the queue infrastructure.
//!
//! **In essence, `Job` is a higher-level abstraction built on top of `Task`,
//! providing a richer API and convenience methods for common task operations.**
//! ```rust,no_run
//! use serde::{Deserialize, Serialize};
//! use sqlx::PgPool;
//! use underway::{JobBuilder, QueueBuilder};
//!
//! #[derive(Debug, Deserialize, Serialize)]
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
//!         .pool(pool.clone())
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
//! A `Queue` manages tasks, including enqueuing and dequeuing them from the
//! database. It serves as a conduit between tasks/jobs and the underlying
//! storage system.
//! ```rust,no_run
//! use serde::{Deserialize, Serialize};
//! use sqlx::PgPool;
//! use underway::{
//!     queue::Error as QueueError, task::Result as TaskResult, Queue, QueueBuilder, Task,
//! };
//!
//! #[derive(Debug, Deserialize, Serialize)]
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
//! A `Worker` continuously polls the queue for tasks and executes them. It
//! works with any type that implements the `Task` trait, including both custom
//! `Task` implementations and `Job`s.
//! ```rust,no_run
//! use serde::{Deserialize, Serialize};
//! use sqlx::PgPool;
//! use underway::{task::Result as TaskResult, QueueBuilder, Task, Worker};
//!
//! #[derive(Debug, Deserialize, Serialize)]
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
//! send welcome emails using `Job` for added convenience:
//! ```rust,no_run
//! use serde::{Deserialize, Serialize};
//! use sqlx::PgPool;
//! use underway::{JobBuilder, QueueBuilder};
//!
//! #[derive(Debug, Deserialize, Serialize)]
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
//!
//! ## Advanced Features
//!
//! ### Retry Policies
//!
//! Customize how tasks are retried upon failure using `RetryPolicy`. Both
//! `Task` and `Job` can specify retry policies.
//! ```rust
//! use underway::{
//!     task::{Result as TaskResult, RetryPolicy, RetryPolicyBuilder},
//!     Task,
//! };
//!
//! struct MyTask;
//!
//! impl Task for MyTask {
//!     type Input = ();
//!
//!     async fn execute(&self, _input: Self::Input) -> TaskResult {
//!         // Task logic here.
//!         Ok(())
//!     }
//!
//!     fn retry_policy(&self) -> RetryPolicy {
//!         RetryPolicyBuilder::new()
//!             .max_attempts(3)
//!             .initial_interval_ms(500)
//!             .backoff_coefficient(2.0)
//!             .build()
//!     }
//! }
//! ```
//!
//! ### Concurrency Keys
//!
//! Prevent multiple tasks with the same concurrency key from executing
//! simultaneously. This is useful for avoiding race conditions when tasks
//! operate on shared resources.
//! ```rust
//! use std::path::PathBuf;
//!
//! use underway::{task::Result as TaskResult, Task};
//!
//! struct FileProcessingTask {
//!     path: PathBuf,
//! };
//!
//! impl Task for FileProcessingTask {
//!     type Input = ();
//!
//!     async fn execute(&self, input: Self::Input) -> TaskResult {
//!         // Process the file.
//!         Ok(())
//!     }
//!
//!     fn concurrency_key(&self) -> Option<String> {
//!         // Use the file path as the concurrency key.
//!         self.path.to_str().map(String::from)
//!     }
//! }
//! ```
//!
//! ### Dead Letter Queues
//!
//! Handle failed tasks by specifying a dead letter queue. Tasks that exceed the
//! maximum retry attempts are moved to the DLQ for inspection or manual
//! intervention.
//! ```rust,no_run
//! use sqlx::PgPool;
//! use underway::{
//!     queue::Error as QueueError, task::Result as TaskResult, Queue, QueueBuilder, Task,
//! };
//!
//! struct MyTask;
//!
//! impl Task for MyTask {
//!     type Input = ();
//!
//!     async fn execute(&self, _input: Self::Input) -> TaskResult {
//!         // Task logic here.
//!         Ok(())
//!     }
//! }
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     let pool = PgPool::connect("postgres://user:password@localhost/database").await?;
//!
//!     let queue: Queue<MyTask> = QueueBuilder::new()
//!         .name("main_queue")
//!         .dead_letter_queue("dead_letter_queue")
//!         .pool(pool)
//!         .build()
//!         .await?;
//!
//!     Ok(())
//! }
//! ```
//!
//! ## Tasks and Jobs
//!
//! The relationship between `Task` and `Job` is central to Underway's design:
//!
//! - **`Task` Trait**: Represents the core interface for any unit of work. It
//!   defines the input type and the execution logic. Developers can implement
//!   `Task` directly for maximum flexibility.
//!
//! - **`Job` Struct**: A specialized implementation of `Task` that provides
//!   additional management capabilities, such as enqueuing, cancelling, and
//!   running tasks. It abstracts over the lower-level building blocks, offering
//!   a higher-level API for common operations.
//!
//! **In other words, `Job` is a kind of `Task` with extra features to simplify
//! task management within the queueing system.** This design allows developers
//! to choose between implementing `Task` directly or using `Job` for
//! convenience, depending on their needs.

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
pub mod schedule;
pub mod task;
pub mod worker;

pub static MIGRATOR: Migrator = sqlx::migrate!();
