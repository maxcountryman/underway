use std::env;

use sqlx::PgPool;
use underway::{Job, StepState};

const QUEUE_NAME: &str = "example-scheduled";

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Set up the database connection pool.
    let database_url = env::var("DATABASE_URL").expect("DATABASE_URL must be set");
    let pool = PgPool::connect(&database_url).await?;

    // Run migrations.
    underway::MIGRATOR.run(&pool).await?;

    // Build the job.
    let job = Job::builder()
        .step(|_ctx, _input| async move {
            println!("Hello, World!");
            StepState::done()
        })
        .name(QUEUE_NAME)
        .pool(pool)
        .build()
        .await?;

    // Schedule the job to run every minute in the given time zone.
    let every_minute = "0 * * * * *[America/Los_Angeles]".parse()?;
    job.schedule(every_minute, ()).await?;

    job.run().await?;

    Ok(())
}
