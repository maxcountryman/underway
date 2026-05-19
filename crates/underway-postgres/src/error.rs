use underway_core::task;

/// Postgres backend error.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    /// SQLx error.
    #[error(transparent)]
    Sqlx(#[from] sqlx::Error),

    /// Migration error.
    #[error(transparent)]
    Migrate(#[from] sqlx::migrate::MigrateError),

    /// JSON error.
    #[error(transparent)]
    Json(#[from] serde_json::Error),

    /// Jiff error.
    #[error(transparent)]
    Jiff(#[from] jiff::Error),

    /// Invalid task configuration loaded from storage.
    #[error(transparent)]
    TaskConfig(#[from] task::ConfigError),

    /// Missing record.
    #[error("{0} not found")]
    NotFound(String),
}
