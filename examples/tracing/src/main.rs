use std::env;

use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};
use underway::{Job, To};

const QUEUE_NAME: &str = "example-tracing";

#[derive(Clone, Deserialize, Serialize)]
struct WelcomeEmail {
    user_id: i32,
    email: String,
    name: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize the tracing subscriber.
    tracing_subscriber::registry()
        .with(EnvFilter::new(
            env::var("RUST_LOG").unwrap_or_else(|_| "info,underway=info,sqlx=warn".into()),
        ))
        .with(tracing_subscriber::fmt::layer())
        .try_init()?;

    // Set up the database connection pool.
    let database_url = &env::var("DATABASE_URL").expect("DATABASE_URL should be set");
    let pool = PgPool::connect(database_url).await?;

    // Run migrations.
    underway::run_migrations(&pool).await?;

    // Build the job.
    let job = Job::builder()
        .step(|_ctx, input| async move { To::effect(input) })
        .effect(
            |_ctx,
             WelcomeEmail {
                 user_id,
                 email,
                 name,
             }| async move {
                // Simulate sending an email.
                tracing::info!("Sending welcome email to {name} <{email}> (user_id: {user_id})");
                To::done()
            },
        )
        .name(QUEUE_NAME)
        .pool(pool)
        .build()
        .await?;

    // Enqueue a job task.
    let task_id = job
        .enqueue(&WelcomeEmail {
            user_id: 42,
            email: "ferris@example.com".to_string(),
            name: "Ferris".to_string(),
        })
        .await?;

    tracing::info!("Enqueued job");

    // Start the worker to process tasks.
    job.run().await?;

    Ok(())
}
