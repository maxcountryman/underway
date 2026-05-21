use sqlx::{
    migrate::Migrator,
    postgres::{PgPool, PgPoolOptions},
};
use underway_core::backend::{Backend, ClaimedTask};

use crate::{Error, PostgresSchedulerLease, PostgresTransaction};

static MIGRATOR: Migrator = sqlx::migrate!("./migrations");

/// SQLx/Postgres backend.
#[derive(Clone)]
pub struct PostgresBackend {
    pool: PgPool,
}

impl PostgresBackend {
    /// Builds a backend from an existing pool.
    pub fn from_pool(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Connects a new pool from a database URL.
    pub async fn connect(database_url: &str) -> Result<Self, Error> {
        let pool = PgPoolOptions::new().connect(database_url).await?;
        Ok(Self::from_pool(pool))
    }

    /// Returns the underlying pool.
    pub fn pool(&self) -> &PgPool {
        &self.pool
    }

    /// Runs Underway migrations in the `underway` schema.
    pub async fn run_migrations(&self) -> Result<(), Error> {
        let mut tx = self.pool.begin().await?;
        sqlx::query!("create schema if not exists underway;")
            .execute(&mut *tx)
            .await?;
        sqlx::query!("set local search_path to underway;")
            .execute(&mut *tx)
            .await?;
        MIGRATOR.run(&mut *tx).await?;
        tx.commit().await?;
        Ok(())
    }
}

impl Backend for PostgresBackend {
    type Error = Error;
    type Transaction<'a> = PostgresTransaction<'a>;
    type SchedulerLease<'a> = PostgresSchedulerLease<'a>;

    async fn begin(&self) -> Result<Self::Transaction<'_>, Self::Error> {
        Ok(PostgresTransaction {
            inner: self.pool.begin().await?,
        })
    }

    async fn record_heartbeat(&self, task: &ClaimedTask) -> Result<(), Self::Error> {
        sqlx::query!(
            r#"
            update underway.task
            set updated_at = now(),
                last_heartbeat_at = now(),
                lease_expires_at = now() + heartbeat
            where id = $1
              and task_queue_name = $2
              and current_attempt = $3
              and state = 'in_progress'::underway.task_state
            "#,
            task.id.as_uuid(),
            &task.queue_name,
            task.attempt_number,
        )
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    async fn try_acquire_scheduler(
        &self,
        queue_name: &str,
    ) -> Result<Option<Self::SchedulerLease<'_>>, Self::Error> {
        let mut conn = self.pool.acquire().await?;
        let key = format!("{queue_name}-scheduler");
        let acquired = sqlx::query_scalar!(
            r#"select pg_try_advisory_lock(hashtextextended($1, 0)) as "acquired!""#,
            &key,
        )
        .fetch_one(&mut *conn)
        .await?;

        if !acquired {
            return Ok(None);
        }

        Ok(Some(PostgresSchedulerLease {
            conn: Some(conn),
            key,
            _marker: std::marker::PhantomData,
        }))
    }
}
