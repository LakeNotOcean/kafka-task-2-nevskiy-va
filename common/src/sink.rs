use std::path::Path;
use std::str::FromStr;
use std::time::Duration;

use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePool, SqlitePoolOptions};

use crate::event::Event;

// общее хранилище обработанных событий
#[derive(Clone)]
pub struct Sink {
    pool: SqlitePool,
}

impl Sink {
    /// Открывает (создаёт) sink: WAL + busy_timeout для конкурентной записи.
    pub async fn open(path: &str) -> Result<Self, sqlx::Error> {
        if let Some(parent) = Path::new(path).parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent).map_err(sqlx::Error::Io)?;
            }
        }
        let opts = SqliteConnectOptions::from_str(&format!("sqlite://{path}"))?
            .create_if_missing(true)
            .journal_mode(SqliteJournalMode::Wal)
            .busy_timeout(Duration::from_secs(5));
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(opts)
            .await?;
        sqlx::migrate!("migrations/sink").run(&pool).await?;
        Ok(Self { pool })
    }

    /// запись события, дубль записывается отдельно
    pub async fn record(
        &self,
        e: &Event,
        pod: &str,
        topic: &str,
        partition: i32,
        offset: i64,
    ) -> Result<bool, sqlx::Error> {
        let mut tx = self.pool.begin().await?;
        let res = sqlx::query(
            "INSERT INTO processed
                 (event_id, user_id, seq, consumer_pod, topic, \"partition\", \"offset\", processed_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?)
             ON CONFLICT(event_id) DO NOTHING",
        )
        .bind(e.event_id.to_string())
        .bind(e.user_id as i64)
        .bind(e.seq as i64)
        .bind(pod)
        .bind(topic)
        .bind(partition)
        .bind(offset)
        .bind(chrono::Utc::now().to_rfc3339())
        .execute(&mut *tx)
        .await?;
        let inserted = res.rows_affected() == 1;
        if !inserted {
            sqlx::query(
                "INSERT INTO dup_stats (consumer_pod, duplicates) VALUES (?, 1)
                 ON CONFLICT(consumer_pod) DO UPDATE SET duplicates = duplicates + 1",
            )
            .bind(pod)
            .execute(&mut *tx)
            .await?;
        }
        tx.commit().await?;
        Ok(inserted)
    }
}
