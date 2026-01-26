use std::env;

use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use tracing_subscriber::{layer::SubscriberExt, util::SubscriberInitExt, EnvFilter};
use underway::{job::EffectOutcome, Job, To};

#[derive(Serialize, Deserialize)]
struct Start {
    user_id: i64,
    send_email: bool,
}

#[derive(Serialize, Deserialize)]
struct SendEmail {
    user_id: i64,
}

#[derive(Serialize, Deserialize)]
struct Finalize {
    user_id: i64,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::registry()
        .with(EnvFilter::new(
            env::var("RUST_LOG").unwrap_or_else(|_| "info,underway=info,sqlx=warn".into()),
        ))
        .with(tracing_subscriber::fmt::layer())
        .try_init()?;

    let database_url = &env::var("DATABASE_URL").expect("DATABASE_URL should be set");
    let pool = PgPool::connect(database_url).await?;

    underway::run_migrations(&pool).await?;

    let job = Job::builder()
        .step(|_cx, Start { user_id, send_email }| async move {
            if send_email {
                tracing::info!("Queueing welcome email for user {user_id}");
                To::effect_for(SendEmail { user_id })
            } else {
                tracing::info!("Skipping email for user {user_id}");
                To::next(Finalize { user_id })
            }
        })
        .effect(|_cx, SendEmail { user_id }| async move {
            tracing::info!("Sending email for user {user_id}");
            Ok(EffectOutcome::next(Finalize { user_id }))
        })
        .step(|_cx, Finalize { user_id }| async move {
            tracing::info!("Finalizing signup for user {user_id}");
            To::done()
        })
        .name("example-branching")
        .pool(pool)
        .build()
        .await?;

    job.enqueue(&Start {
        user_id: 42,
        send_email: true,
    })
    .await?;

    job.enqueue(&Start {
        user_id: 43,
        send_email: false,
    })
    .await?;

    job.run().await?;

    Ok(())
}
