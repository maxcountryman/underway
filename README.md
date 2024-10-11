<h1 align="center">
    underway
</h1>

<p align="center">
    ⏳ <strong>Underway</strong> is a PostgreSQL-backed job queue for reliable background task processing.
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

Underway provides a robust and efficient way to execute asynchronous tasks using PostgreSQL as the backend for task storage and coordination. It is designed to be simple, scalable, and resilient, handling job processing in a way that ensures safe concurrency and reliable task execution. Whether you're processing tasks on a single server or across multiple workers, Underway makes it easy to manage background jobs with confidence.

Key Features:

- **PostgreSQL-Backed** Built on PostgreSQL for robust task storage and
  coordination, ensuring consistency and safe concurrency across all
  operations.
- **Transactional Task Management** Supports enqueuing tasks within existing
  database transactions, guaranteeing that tasks are only added if the
  transaction commits successfully—perfect for operations like user
  registration.
- **Automatic Retries** Offers customizable retry strategies for failed
  executions, ensuring tasks are reliably completed even after transient
  failures.
- **Cron-Like Scheduling** Supports scheduling recurring tasks with
  cron-like expressions, enabling automated, time-based job execution.
- **Scalable and Flexible** Scales from a single worker to multiple workers
  with minimal configuration, allowing seamless background job processing.

## 🤸 Usage

```rust
use std::env;

use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use underway::{Job, Queue};

const QUEUE_NAME: &str = "email";

#[derive(Debug, Clone, Deserialize, Serialize)]
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
    let queue = Queue::builder().name(QUEUE_NAME).pool(pool).build().await?;

    // Build the job.
    let job = Job::builder()
        .queue(queue)
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
        .build();

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
