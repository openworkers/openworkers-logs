use chrono::{DateTime, Utc};
use serde::Serialize;
use sqlx::{postgres::PgPoolOptions, PgPool};
use std::str::FromStr;
use uuid::Uuid;

#[derive(Clone, Debug, PartialEq, sqlx::Type, Serialize)]
#[sqlx(type_name = "enum_logs_level", rename_all = "lowercase")]
pub enum LogLevel {
    Error,
    Warn,
    Info,
    Log,
    Debug,
    Trace,
}

impl FromStr for LogLevel {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "error" => Ok(LogLevel::Error),
            "warn" => Ok(LogLevel::Warn),
            "info" => Ok(LogLevel::Info),
            "log" => Ok(LogLevel::Log),
            "debug" => Ok(LogLevel::Debug),
            "trace" => Ok(LogLevel::Trace),
            _ => Ok(LogLevel::Info), // Default to Info for unknown levels
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct LogEntry {
    pub date: DateTime<Utc>,
    pub worker_id: Uuid,
    pub message: String,
    pub level: LogLevel,
}

pub async fn create_pool() -> Result<PgPool, sqlx::Error> {
    let database_url = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");

    log::info!("Connecting to database...");

    PgPoolOptions::new()
        .max_connections(5)
        .connect(&database_url)
        .await
}

pub async fn insert_log(pool: &PgPool, log: LogEntry) -> Result<(), sqlx::Error> {
    sqlx::query!(
        r#"
        INSERT INTO logs (date, worker_id, message, level)
        VALUES ($1, $2, $3, $4)
        "#,
        log.date,
        log.worker_id,
        log.message,
        log.level as LogLevel
    )
    .execute(pool)
    .await?;

    Ok(())
}

pub async fn get_logs(
    pool: &PgPool,
    worker_id: Uuid,
    limit: i64,
) -> Result<Vec<LogEntry>, sqlx::Error> {
    let logs = sqlx::query_as!(
        LogEntry,
        r#"
        SELECT date, worker_id, message, level as "level: LogLevel"
        FROM logs
        WHERE worker_id = $1
        ORDER BY date DESC
        LIMIT $2
        "#,
        worker_id,
        limit
    )
    .fetch_all(pool)
    .await?;

    Ok(logs)
}
