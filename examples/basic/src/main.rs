use std::env;

use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use underway::{Job, Queue};

const QUEUE_NAME: &str = "email";

#[derive(Debug, Clone, Deserialize, Serialize)]
struct WelcomeEmail {
    user_id: i32,
    email: String,
    name: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Set up the database connection pool.
    let database_url = &env::var("DATABASE_URL").expect("DATABASE_URL should be set");
    let pool = PgPool::connect(database_url).await?;

    // Run migrations.
    underway::MIGRATOR.run(&pool).await?;

    // Create the task queue.
    let queue = Queue::builder().name(QUEUE_NAME).pool(pool).build().await?;

    // Build the job.
    let job = Job::builder()
        .queue(queue)
        .execute(
            |WelcomeEmail {
                 user_id,
                 email,
                 name,
             }| async move {
                // Simulate sending an email.
                println!("Sending welcome email to {name} <{email}> (user_id: {user_id})");
                Ok(())
            },
        )
        .build();

    // Enqueue a job task.
    let task_id = job
        .enqueue(WelcomeEmail {
            user_id: 42,
            email: "ferris@example.com".to_string(),
            name: "Ferris".to_string(),
        })
        .await?;

    println!("Enqueued task with ID: {task_id}");

    // Start the worker to process tasks.
    job.run().await?;

    Ok(())
}
