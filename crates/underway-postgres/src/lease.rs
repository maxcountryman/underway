use sqlx::Postgres;
use underway_core::backend::SchedulerLease;

use crate::Error;

/// Postgres scheduler lease.
pub struct PostgresSchedulerLease<'a> {
    pub(crate) conn: Option<sqlx::pool::PoolConnection<Postgres>>,
    pub(crate) key: String,
    pub(crate) _marker: std::marker::PhantomData<&'a ()>,
}

impl SchedulerLease for PostgresSchedulerLease<'_> {
    type Error = Error;

    async fn release(&mut self) -> Result<(), Self::Error> {
        if let Some(conn) = self.conn.as_mut() {
            sqlx::query_scalar!(
                r#"select pg_advisory_unlock(hashtextextended($1, 0)) as "unlocked!""#,
                &self.key,
            )
            .fetch_one(&mut **conn)
            .await?;
            self.conn.take();
        }
        Ok(())
    }
}

impl Drop for PostgresSchedulerLease<'_> {
    fn drop(&mut self) {
        if let Some(mut conn) = self.conn.take() {
            // Returning this connection to the pool would keep the session-level
            // advisory lock held for the next borrower.
            conn.close_on_drop();
        }
    }
}
