use std::env;

use sqlx::PgPool;
use underway::{queue::ZonedSchedule, Job, Queue};

const QUEUE_NAME: &str = "hello-world";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Set up the database connection pool.
    let database_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let pool = PgPool::connect(&database_url).await?;

    // Run migrations.
    underway::MIGRATOR.run(&pool).await?;

    // Create the task queue.
    let queue = Queue::builder().name(QUEUE_NAME).pool(pool).build().await?;

    // Build the job.
    let job = Job::builder()
        .queue(queue)
        .execute(|_| async move {
            println!("Hello, World!");
            Ok(())
        })
        .build();

    // Schedule the job to run every minute in the given time zone.
    let every_minute = ZonedSchedule::new("0 * * * * *", "America/Los_Angeles")?;
    job.schedule(every_minute, ()).await?;

    // Run the scheduler and worker concurrently.
    let _ = tokio::join!(job.run_scheduler(), job.run());

    Ok(())
}
