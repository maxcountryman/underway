<h1 align="center">
    underway
</h1>

<p align="center">
    ⏳ Durable step functions via Postgres.
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

**Underway** provides durable background jobs over Postgres. Jobs are composed of a sequence of one more steps. Each step takes the output of the previous step as its input. These simple workflows provide a powerful interface to common deferred work use cases.

Key Features:

- **PostgreSQL-Backed** Leverages PostgreSQL with `FOR UPDATE SKIP LOCKED`
  for reliable task storage and coordination.
- **Atomic Task Management** Enqueue tasks within your transactions and use
  the worker's transaction within your tasks for atomic queries.
- **Automatic Retries** Configurable retry strategies ensure tasks are
  reliably completed, even after transient failures.
- **Cron-Like Scheduling** Schedule recurring tasks with cron-like
  expressions for automated, time-based job execution.
- **Scalable and Flexible** Easily scales from a single worker to many,
  enabling seamless background job processing with minimal setup.

## 🤸 Usage

Underway is suitable for many different use cases, ranging from simple
single-step jobs to more sophisticated multi-step jobs, where dependencies
are built up between steps.

## Welcome emails

A common use case is deferring work that can be processed later. For
instance, during user registration, we might want to send a welcome email to
new users. Rather than handling this within the registration process (e.g.,
form validation, database insertion), we can offload it to run "out-of-band"
using Underway. By defining a job for sending the welcome email, Underway
ensures it gets processed in the background, without slowing down the user
registration flow.

```rust
use std::env;

use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use underway::{Job, To};

// This is the input we'll provide to the job when we enqueue it.
#[derive(Deserialize, Serialize)]
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

    // Build the job.
    let job = Job::builder()
        .step(
            |_cx,
             WelcomeEmail {
                 user_id,
                 email,
                 name,
             }| async move {
                // Simulate sending an email.
                println!("Sending welcome email to {name} <{email}> (user_id: {user_id})");
                // Returning this indicates this is the final step.
                To::done()
            },
        )
        .name("welcome-email")
        .pool(pool)
        .build()
        .await?;

    // Here we enqueue a new job to be processed later.
    job.enqueue(WelcomeEmail {
        user_id: 42,
        email: "ferris@example.com".to_string(),
        name: "Ferris".to_string(),
    })
    .await?;

    // Start processing enqueued jobs.
    job.start().await??;

    Ok(())
}
```

## Order receipts

Another common use case is defining dependencies between discrete steps of a
job. For instance, we might generate PDF receipts for orders and then email
these to customers. With Underway, each step is handled separately, making
it easy to create a job that first generates the PDF and, once
completed, proceeds to send the email.

This separation provides significant value: if the email sending service
is temporarily unavailable, we can retry the email step without having to
regenerate the PDF, avoiding unnecessary repeated work.

```rust
use std::env;

use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use underway::{Job, To};

#[derive(Deserialize, Serialize)]
struct GenerateReceipt {
    // An order we want to generate a receipt for.
    order_id: i32,
}

#[derive(Deserialize, Serialize)]
struct EmailReceipt {
    // An object store key to our receipt PDF.
    receipt_key: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Set up the database connection pool.
    let database_url = &env::var("DATABASE_URL").expect("DATABASE_URL should be set");
    let pool = PgPool::connect(database_url).await?;

    // Run migrations.
    underway::MIGRATOR.run(&pool).await?;

    // Build the job.
    let job = Job::builder()
        .step(|_cx, GenerateReceipt { order_id }| async move {
            // Use the order ID to build a receipt PDF...
            let receipt_key = format!("receipts_bucket/{order_id}-receipt.pdf");
            // ...store the PDF in an object store.

            // We proceed to the next step with the receipt_key as its input.
            To::next(EmailReceipt { receipt_key })
        })
        .step(|_cx, EmailReceipt { receipt_key }| async move {
            // Retrieve the PDF from the object store, and send the email.
            println!("Emailing receipt for {receipt_key}");
            To::done()
        })
        .name("order-receipt")
        .pool(pool)
        .build()
        .await?;

    // Enqueue the job for the given order.
    job.enqueue(GenerateReceipt { order_id: 42 }).await?;

    // Start processing enqueued jobs.
    job.start().await??;

    Ok(())
}
```

With this setup, if the email service is down, the `EmailReceipt` step can
be retried without redoing the PDF generation, saving time and resources by
not repeating the expensive step of generating the PDF.

## Daily reports

Jobs may also be run on a schedule. This makes them useful for situations
where we want to do things on a regular cadence, such as creating a daily
business report.

```rust
use std::env;

use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use underway::{Job, To};

#[derive(Deserialize, Serialize)]
struct DailyReport;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Set up the database connection pool.
    let database_url = &env::var("DATABASE_URL").expect("DATABASE_URL should be set");
    let pool = PgPool::connect(database_url).await?;

    // Run migrations.
    underway::MIGRATOR.run(&pool).await?;

    // Build the job.
    let job = Job::builder()
        .step(|_cx, _| async move {
            // Here we would generate and store the report.
            To::done()
        })
        .name("daily-report")
        .pool(pool)
        .build()
        .await?;

    // Set a daily schedule with the given input.
    let daily = "@daily[America/Los_Angeles]".parse()?;
    job.schedule(daily, DailyReport).await?;

    // Start processing enqueued jobs.
    job.start().await??;

    Ok(())
}
```

## 🛟 Getting Help

We've put together a number of [examples][examples] to help get you started. You're also welcome to [open a discussion](https://github.com/maxcountryman/underway/discussions/new?category=q-a) and ask additional questions you might have.

## 👯 Contributing

We appreciate all kinds of contributions, thank you!

[examples]: https://github.com/maxcountryman/underway/tree/main/examples
[docs]: https://docs.rs/underway
