use std::env;

use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};
use underway::{Job, To};

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
        // Step 1: Start with an initial number `n`.
        .step(|_ctx, Start { n }| async move {
            tracing::info!("Starting computation with n = {n}");
            // Proceed to the next step, passing the current state.
            To::next(Power { n })
        })
        // Step 2: Compute the power of `n`.
        .step(|_ctx, Power { n }| async move {
            let squared = n.pow(2);
            tracing::info!("Squared value: {n}^2 = {squared}");
            // Proceed to the next step with the new state.
            To::next(Modulo { n })
        })
        // Step 3: Compute modulo of the result.
        .step(|_ctx, Modulo { n }| async move {
            let modulo_result = n % 10;
            tracing::info!("Modulo 10 of {n} is {modulo_result}");
            // Mark the job as done.
            To::done()
        })
        .name("example-step")
        .pool(pool)
        .build()
        .await?;

    // Enqueue the first step.
    job.enqueue(&Start { n: 42 }).await?;

    // Run the job worker.
    job.run().await?;

    Ok(())
}
