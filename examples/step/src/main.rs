use std::env;

use jiff::ToSpan;
use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};
use underway::{job::StepState, Job};

#[derive(Serialize, Deserialize)]
struct Start {
    n: usize,
}

#[derive(Serialize, Deserialize)]
struct Power {
    n: usize,
}

#[derive(Serialize, Deserialize)]
struct Modulo {
    n: usize,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize the tracing subscriber.
    tracing_subscriber::registry()
        .with(EnvFilter::new(
            env::var("RUST_LOG").unwrap_or_else(|_| "debug,underway=info,sqlx=warn".into()),
        ))
        .with(tracing_subscriber::fmt::layer())
        .try_init()?;

    // Set up the database connection pool.
    let database_url = &env::var("DATABASE_URL").expect("DATABASE_URL should be set");
    let pool = PgPool::connect(database_url).await?;

    // Run migrations.
    underway::MIGRATOR.run(&pool).await?;

    // Create our job.
    let job = Job::builder()
        .step(|Start { n }| async move {
            tracing::info!("Starting with {}", n);
            let n = n.pow(2);

            StepState::to_next(Power { n })
        })
        .step(|Power { n }| async move {
            tracing::info!("Squared: {}", n);
            let n = n % 10;

            tracing::info!("The next step is delayed for five seconds");
            StepState::delay_for(Modulo { n }, 3.seconds())
        })
        .step(|Modulo { n }| async move {
            tracing::info!("Modulo 10 result: {}", n);
            StepState::done()
        })
        .name("example-step")
        .pool(pool)
        .build()
        .await?;

    // Enqueue the first step.
    job.enqueue(Start { n: 42 }).await?;

    // Run the job worker.
    job.run().await?;

    Ok(())
}
