use std::env;

use chrono_tz::UTC;
use sqlx::PgPool;
use underway::{JobBuilder, QueueBuilder};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Set up the database connection pool.
    let database_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let pool = PgPool::connect(&database_url).await?;

    // Run migrations.
    underway::MIGRATOR.run(&pool).await?;

    // Create the task queue.
    let queue = QueueBuilder::new()
        .name("example_queue")
        .pool(pool)
        .build()
        .await?;

    // Build the job.
    let job = JobBuilder::new(queue)
        .execute(|_| async move {
            println!("Hello, World!");
            Ok(())
        })
        .build()
        .await?;

    // Schedule the job to run every minute.
    let every_minute = "0 * * * * *".parse()?; // Runs at every minute.
    job.schedule(every_minute, UTC, ()).await?;

    // Run the scheduler and worker concurrently.
    let _ = tokio::join!(job.run_scheduler(), job.run());

    Ok(())
}
