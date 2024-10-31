use std::env;

use sqlx::{postgres::PgPoolOptions, PgPool};
use tokio::{signal, task::JoinSet};
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};
use underway::{Job, To};

const QUEUE_NAME: &str = "graceful-shutdown";

async fn shutdown_signal(pool: &PgPool) {
    let ctrl_c = async {
        signal::ctrl_c().await.unwrap();
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .unwrap()
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {
            underway::queue::graceful_shutdown(pool).await.unwrap();
        },
        _ = terminate => {
            underway::queue::graceful_shutdown(pool).await.unwrap();
        },
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize the tracing subscriber.
    tracing_subscriber::registry()
        .with(EnvFilter::new(
            env::var("RUST_LOG").unwrap_or_else(|_| "info,underway=debug,sqlx=warn".into()),
        ))
        .with(tracing_subscriber::fmt::layer())
        .try_init()?;

    // Set up the database connection pool.
    let database_url = &env::var("DATABASE_URL").expect("DATABASE_URL should be set");
    let pool = PgPoolOptions::new()
        .max_connections(25)
        .connect(database_url)
        .await?;

    // Run migrations.
    underway::run_migrations(&pool).await?;

    // Build the job.
    let job = Job::builder()
        .step(|_ctx, _input| async move {
            let sleep_duration = std::time::Duration::from_secs(10);

            tracing::info!(?sleep_duration, "Hello from a long-running task");

            // Artificial delay to simulate a long-running job.
            tokio::time::sleep(sleep_duration).await;

            To::done()
        })
        .name(QUEUE_NAME)
        .pool(pool.clone())
        .build()
        .await?;

    let every_second = "* * * * * *[America/Los_Angeles]".parse()?;
    job.schedule(&every_second, &()).await?;

    // Await the shutdown signal handler in its own task.
    tokio::spawn(async move { shutdown_signal(&pool).await });

    // All jobs will run until the queue signals shutdown.
    let mut jobs = JoinSet::new();
    for _ in 0..2 {
        jobs.spawn({
            let job = job.clone();
            async move { job.run().await }
        });
    }
    jobs.join_all().await;

    Ok(())
}
