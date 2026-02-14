use std::{sync::Arc, time::Duration};

use serde::{Deserialize, Serialize};
use sqlx::PgPool;
use tokio::{
    sync::Mutex,
    time::{sleep, timeout},
};
use underway::{Activity, ActivityError, Transition, Workflow};

#[derive(Clone)]
struct LookupEmail {
    pool: PgPool,
}

impl Activity for LookupEmail {
    const NAME: &'static str = "lookup-email";

    type Input = i64;
    type Output = String;

    async fn execute(&self, user_id: Self::Input) -> underway::activity::Result<Self::Output> {
        let email = sqlx::query_scalar::<_, String>("select concat('user-', $1::text, '@example.com')")
            .bind(user_id)
            .fetch_one(&self.pool)
            .await
            .map_err(|err| ActivityError::retryable("db_query_failed", err.to_string()))?;

        Ok(email)
    }
}

struct TrackSignupMetric;

impl Activity for TrackSignupMetric {
    const NAME: &'static str = "track-signup-metric";

    type Input = String;
    type Output = ();

    async fn execute(&self, email: Self::Input) -> underway::activity::Result<Self::Output> {
        println!("Tracked signup metric for {email}");
        Ok(())
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct Signup {
    user_id: i64,
}

#[derive(Clone, Default)]
struct State {
    resolved_emails: Arc<Mutex<Vec<String>>>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let database_url = std::env::var("DATABASE_URL")?;
    let pool = PgPool::connect(&database_url).await?;

    underway::run_migrations(&pool).await?;

    let state = State::default();

    let workflow = Workflow::builder()
        .state(state.clone())
        .activity(LookupEmail { pool: pool.clone() })
        .activity(TrackSignupMetric)
        .step(|mut cx, Signup { user_id }| {
            let resolved_emails = cx.state.resolved_emails.clone();

            async move {
                let email: String = cx.call::<LookupEmail, _>(&user_id).await?;
                cx.emit::<TrackSignupMetric, _>(&email).await?;
                resolved_emails.lock().await.push(email);
                Transition::complete()
            }
        })
        .name("example-activities-workflow")
        .pool(pool)
        .build()
        .await?;

    workflow.enqueue(&Signup { user_id: 42 }).await?;

    let runtime_handle = workflow.runtime().start();

    timeout(Duration::from_secs(10), async {
        loop {
            if !state.resolved_emails.lock().await.is_empty() {
                break;
            }

            sleep(Duration::from_millis(50)).await;
        }
    })
    .await?;

    runtime_handle.shutdown().await?;

    let resolved = state.resolved_emails.lock().await.clone();
    println!("Resolved emails: {resolved:?}");

    Ok(())
}
