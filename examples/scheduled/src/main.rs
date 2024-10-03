use std::env;

use jiff::tz::TimeZone;
use sqlx::PgPool;
use underway::{JobBuilder, QueueBuilder};

const QUEUE_NAME: &str = "hello-world";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Set up the database connection pool.
    let database_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let pool = PgPool::connect(&database_url).await?;

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
        .execute(|_| async move {
            println!("Hello, World!");
            Ok(())
        })
        .build()
        .await?;

    // Schedule the job to run every minute in the given time zone.
    let every_minute = "0 * * * * *".parse()?;
    let tz = TimeZone::get("America/Los_Angeles").expect("Should be a recognized time zone");
    job.schedule(every_minute, tz, ()).await?;

    // Run the scheduler and worker concurrently.
    let _ = tokio::join!(job.run_scheduler(), job.run());

    Ok(())
}
