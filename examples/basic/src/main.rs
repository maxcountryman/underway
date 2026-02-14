use std::{sync::Arc, time::Duration};

use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use tokio::{
    sync::Mutex,
    time::{sleep, timeout},
};
use underway::{Transition, Workflow};

#[derive(Debug, Deserialize, Serialize)]
struct ResizeImage {
    asset_id: i64,
}

#[derive(Debug, Deserialize, Serialize)]
struct PublishImage {
    object_key: String,
}

#[derive(Clone, Default)]
struct State {
    completions: Arc<Mutex<Vec<String>>>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let database_url = std::env::var("DATABASE_URL")?;
    let pool = PgPool::connect(&database_url).await?;

    underway::run_migrations(&pool).await?;

    let state = State::default();

    let workflow = Workflow::builder()
        .state(state.clone())
        .step(|_cx, ResizeImage { asset_id }| async move {
            let object_key = format!("images/{asset_id}.webp");
            println!("Resized image {asset_id} -> {object_key}");

            Transition::next(PublishImage { object_key })
        })
        .step(|cx, PublishImage { object_key }| {
            let completions = cx.state.completions.clone();

            async move {
                println!("Publishing {object_key}");
                completions.lock().await.push(object_key);
                Transition::complete()
            }
        })
        .name("example-basic-workflow")
        .pool(pool)
        .build()
        .await?;

    workflow.enqueue(&ResizeImage { asset_id: 42 }).await?;

    let runtime_handle = workflow.runtime().start()?;

    timeout(Duration::from_secs(10), async {
        loop {
            if !state.completions.lock().await.is_empty() {
                break;
            }

            sleep(Duration::from_millis(50)).await;
        }
    })
    .await?;

    runtime_handle.shutdown().await?;

    let completions = state.completions.lock().await.clone();
    println!("Completed workflow outputs: {completions:?}");

    Ok(())
}
