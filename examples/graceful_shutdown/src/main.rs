use std::env;

use sqlx::PgPool;
use tokio::signal;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};
use underway::{Job, Queue};

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
    let pool = PgPool::connect(database_url).await?;

    // Run migrations.
    underway::MIGRATOR.run(&pool).await?;

    // Create the task queue.
    let queue = Queue::builder()
        .name(QUEUE_NAME)
        .pool(pool.clone())
        .build()
        .await?;

    // Build the job.
    let job = Job::builder()
        .execute(|_| async move {
            let sleep_duration = std::time::Duration::from_secs(5);

            tracing::info!(?sleep_duration, "Hello from a long-running task");

            // Artificial delay to simulate a long-running job.
            tokio::time::sleep(sleep_duration).await;

            Ok(())
        })
        .queue(queue)
        .build();

    let every_second = "* * * * * *[America/Los_Angeles]".parse()?;
    job.schedule(every_second, ()).await?;

    // Await the shutdown signal handler in its own task.
    tokio::spawn(async move { shutdown_signal(&pool).await });

    // The job will run until the queue signals a shutdown.
    job.run().await?;

    Ok(())
}
