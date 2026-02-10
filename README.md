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

**Underway** provides durable background workflows over Postgres. Workflows are composed of a sequence of one or more steps. Each step takes the output of the previous step as its input. These simple workflows provide a powerful interface to common deferred work use cases.

Key Features:

- **Runtime-First Execution** Run workflows via `workflow.runtime().run()` or
  `workflow.runtime().start()`.
- **Durable Activity Effects** Use `Context::call` and `Context::emit` for
  persisted side effects with suspend/resume behavior.
- **Compile-Time Safe Activities** Register handlers on
  `Workflow::builder().activity(...)`; calls are typed and unregistered
  activities fail at compile time.
- **Transactional Enqueue and Schedule** Use `*_using` APIs to enqueue or
  schedule workflows inside your own transactions.
- **PostgreSQL-Backed Coordination** Uses PostgreSQL with `FOR UPDATE SKIP
  LOCKED` for reliable queue coordination.
- **Leased Execution and Fencing** Heartbeats act as task leases; stale
  heartbeats trigger new attempts and fence out old workers.
- **Automatic Retries and Scheduling** Configure retries per task/workflow and
  run recurring workflows with cron-like schedules.

## 🤸 Usage

Underway supports a few common patterns out of the box:

1. Build a typed workflow and run it with `runtime()`.
2. Use durable activity calls for side effects.
3. Enqueue and schedule atomically inside your own transaction.

### 1) Build and run a workflow

```rust
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use underway::{To, Workflow};

#[derive(Deserialize, Serialize)]
struct ResizeImage {
    asset_id: i64,
}

#[derive(Deserialize, Serialize)]
struct PublishImage {
    object_key: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let pool = PgPool::connect(&std::env::var("DATABASE_URL")?).await?;
    underway::run_migrations(&pool).await?;

    let workflow = Workflow::builder()
        .step(|_cx, ResizeImage { asset_id }| async move {
            let object_key = format!("images/{asset_id}.webp");
            To::next(PublishImage { object_key })
        })
        .step(|_cx, PublishImage { object_key }| async move {
            println!("Publishing {object_key}");
            To::done()
        })
        .name("image-pipeline")
        .pool(pool)
        .build()
        .await?;

    workflow.enqueue(&ResizeImage { asset_id: 42 }).await?;

    let runtime_handle = workflow.runtime().start();
    runtime_handle.shutdown().await?;
    Ok(())
}
```

### 2) Durable side effects with activities

```rust
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use underway::{Activity, ActivityError, To, Workflow};

#[derive(Clone)]
struct LookupEmail {
    pool: PgPool,
}

impl Activity for LookupEmail {
    const NAME: &'static str = "lookup-email";

    type Input = i64;
    type Output = String;

    async fn execute(&self, user_id: Self::Input) -> underway::activity::Result<Self::Output> {
        let email = sqlx::query_scalar::<_, String>("select email from app_user where id = $1")
            .bind(user_id)
            .fetch_optional(&self.pool)
            .await
            .map_err(|err| ActivityError::retryable("db_error", err.to_string()))?;

        email.ok_or_else(|| {
            ActivityError::fatal("missing_user", format!("No user found for id {user_id}"))
        })
    }
}

struct TrackSignupMetric;

impl Activity for TrackSignupMetric {
    const NAME: &'static str = "track-signup-metric";

    type Input = String;
    type Output = ();

    async fn execute(&self, email: Self::Input) -> underway::activity::Result<Self::Output> {
        println!("tracking signup metric for {email}");
        Ok(())
    }
}

#[derive(Deserialize, Serialize)]
struct Signup {
    user_id: i64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let pool = PgPool::connect(&std::env::var("DATABASE_URL")?).await?;
    underway::run_migrations(&pool).await?;

    let workflow = Workflow::builder()
        .activity(LookupEmail { pool: pool.clone() })
        .activity(TrackSignupMetric)
        .step(|mut cx, Signup { user_id }| async move {
            let email: String = cx.call::<LookupEmail, _>("lookup", &user_id).await?;
            cx.emit::<TrackSignupMetric, _>("track", &email).await?;
            To::done()
        })
        .name("signup-side-effects")
        .pool(pool)
        .build()
        .await?;

    workflow.enqueue(&Signup { user_id: 42 }).await?;
    workflow.runtime().run().await?;
    Ok(())
}
```

### 3) Enqueue and schedule in your transaction

```rust
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use underway::{To, Workflow};

#[derive(Deserialize, Serialize)]
struct TenantCleanup {
    tenant_id: i64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let pool = PgPool::connect(&std::env::var("DATABASE_URL")?).await?;
    underway::run_migrations(&pool).await?;

    let workflow = Workflow::builder()
        .step(|_cx, TenantCleanup { tenant_id }| async move {
            println!("Running cleanup for tenant {tenant_id}");
            To::done()
        })
        .name("tenant-cleanup")
        .pool(pool.clone())
        .build()
        .await?;

    let nightly = "0 2 * * *[UTC]".parse()?;
    let tenant_id = 7;

    let mut tx = pool.begin().await?;

    sqlx::query("update app_tenant set cleanup_enabled = true where id = $1")
        .bind(tenant_id)
        .execute(&mut *tx)
        .await?;

    let input = TenantCleanup { tenant_id };
    workflow.enqueue_using(&mut *tx, &input).await?;
    workflow.schedule_using(&mut *tx, &nightly, &input).await?;

    tx.commit().await?;

    Ok(())
}
```

## 🛟 Getting Help

The [API docs][docs] include module-level walkthroughs and runnable snippets.
You're also welcome to [open a discussion](https://github.com/maxcountryman/underway/discussions/new?category=q-a) and ask additional questions you might have.

## 👯 Contributing

We appreciate all kinds of contributions, thank you!

[docs]: https://docs.rs/underway
