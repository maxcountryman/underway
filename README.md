<h1 align="center">
    underway
</h1>

<p align="center">
    ⏳ <strong>Underway</strong> is a distributed task queue built on top of PostgreSQL.
</p>

<div align="center">
    <a href="https://crates.io/crates/underway">
        <img src="https://img.shields.io/crates/v/underway.svg" />
    </a>
    <a href="https://docs.rs/underway">
        <img src="https://docs.rs/underway/badge.svg" />
    </a>
    <a href="https://github.com/maxcountryman/underway/actions/workflows/rust.yml">
        <img src="https://github.com/maxcountryman/underway/actions/workflows/rust.yml/badge.svg" />
    </a>
</div>

## 🎨 Overview

Underway provides a reliable and efficient way to execute asynchronous tasks in a distributed environment, leveraging PostgreSQL as a backend for task storage and coordination. It is designed to be simple, scalable, and resilient, making it suitable for a wide range of applications.

Key Features:

- **Distributed Task Execution**: Easily distribute tasks across multiple workers and machines.
- **Reliable Persistence**: Tasks are stored in PostgreSQL, ensuring durability and reliability.
- **Strongly Typed Tasks**: Define tasks with strongly typed inputs using Rust’s type system.
- **Flexible Retry Policies**: Configure custom retry strategies with exponential backoff.
- **Concurrency Control**: Limit task execution with concurrency keys to prevent race conditions.
- **Priority Queueing**: Assign priorities to tasks to control execution order.
- **Dead Letter Queue (DLQ)**: Automatically handle failed tasks by moving them to a DLQ for later inspection.

## 🤸 Usage

```rust
use std::env;

use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use underway::{queue::QueueBuilder, JobBuilder};

const QUEUE_NAME: &str = "email";

#[derive(Debug, Deserialize, Serialize)]
struct WelcomeEmail {
    user_id: i32,
    email: String,
    name: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Set up the database connection pool.
    let database_url = &env::var("DATABASE_URL").expect("DATABASE_URL should be set");
    let pool = PgPool::connect(database_url).await?;

    // Run migrations.
    underway::MIGRATOR.run(&pool).await?;

    // Create the task queue.
    let queue = QueueBuilder::new()
        .name(QUEUE_NAME)
        .pool(pool)
        .build()
        .await?;

    // Build the job.
    let job = JobBuilder::new(queue)
        .execute(
            |WelcomeEmail {
                 user_id,
                 email,
                 name,
             }| async move {
                // Simulate sending an email.
                println!("Sending welcome email to {name} <{email}> (user_id: {user_id})");
                Ok(())
            },
        )
        .build()
        .await?;

    // Enqueue a job task.
    let task_id = job
        .enqueue(WelcomeEmail {
            user_id: 42,
            email: "ferris@example.com".to_string(),
            name: "Ferris".to_string(),
        })
        .await?;

    println!("Enqueued task with ID: {task_id}");

    // Start the worker to process tasks.
    job.run().await?;

    Ok(())
}
```

## 🦺 Safety

This crate uses `#![forbid(unsafe_code)]` to ensure everything is implemented in 100% safe Rust.

## 🛟 Getting Help

We've put together a number of [examples][examples] to help get you started. You're also welcome to [open a discussion](https://github.com/maxcountryman/underway/discussions/new?category=q-a) and ask additional questions you might have.

## 👯 Contributing

We appreciate all kinds of contributions, thank you!

[examples]: https://github.com/maxcountryman/underway/tree/main/examples
[docs]: https://docs.rs/underway
