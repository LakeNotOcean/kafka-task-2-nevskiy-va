//! SQLite-хранилище

use std::path::Path;
use std::str::FromStr;

use sqlx::sqlite::{SqliteConnectOptions, SqlitePool, SqlitePoolOptions, SqliteRow};
use sqlx::Row;
use uuid::Uuid;

use crate::event::Event;

#[derive(Clone)]
pub struct Store {
    pool: SqlitePool,
}

impl Store {
    // Открытие и миграция при необходимости
    pub async fn open(path: &str) -> Result<Self, sqlx::Error> {
        if let Some(parent) = Path::new(path).parent() {
            if !parent.as_os_str().is_empty() {
                std::fs::create_dir_all(parent).map_err(sqlx::Error::Io)?;
            }
        }
        let opts =
            SqliteConnectOptions::from_str(&format!("sqlite://{path}"))?.create_if_missing(true);
        let pool = SqlitePoolOptions::new()
            .max_connections(1)
            .connect_with(opts)
            .await?;
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS events (
                event_id TEXT PRIMARY KEY,
                user_id INTEGER NOT NULL,
                type TEXT NOT NULL,
                seq INTEGER NOT NULL,
                produced_at TEXT NOT NULL,
                payload TEXT NOT NULL,
                sent INTEGER NOT NULL DEFAULT 0
            )",
        )
        .execute(&pool)
        .await?;
        sqlx::query("CREATE INDEX IF NOT EXISTS idx_events_user_seq ON events(user_id, seq)")
            .execute(&pool)
            .await?;
        sqlx::query("CREATE INDEX IF NOT EXISTS idx_events_sent ON events(sent)")
            .execute(&pool)
            .await?;
        Ok(Self { pool })
    }

    // следующий номер события
    pub async fn next_seq(&self, user_id: u64) -> Result<u64, sqlx::Error> {
        let row =
            sqlx::query("SELECT COALESCE(MAX(seq), 0) AS max_seq FROM events WHERE user_id = ?")
                .bind(user_id as i64)
                .fetch_one(&self.pool)
                .await?;
        let max_seq: i64 = row.try_get("max_seq")?;
        Ok(max_seq as u64 + 1)
    }

    // количество сгенерированных событий
    pub async fn event_count(&self) -> Result<u64, sqlx::Error> {
        let row = sqlx::query("SELECT COUNT(*) AS n FROM events")
            .fetch_one(&self.pool)
            .await?;
        let n: i64 = row.try_get("n")?;
        Ok(n as u64)
    }

    // запись события
    pub async fn insert_event(&self, e: &Event) -> Result<(), sqlx::Error> {
        sqlx::query(
            "INSERT INTO events (event_id, user_id, type, seq, produced_at, payload, sent)
             VALUES (?, ?, ?, ?, ?, ?, 0)",
        )
        .bind(e.event_id.to_string())
        .bind(e.user_id as i64)
        .bind(e.event_type.as_str())
        .bind(e.seq as i64)
        .bind(e.produced_at.to_rfc3339())
        .bind(e.payload.to_string())
        .execute(&self.pool)
        .await?;
        Ok(())
    }

    // отметить событие досталвенным
    pub async fn mark_sent(&self, event_id: Uuid) -> Result<(), sqlx::Error> {
        sqlx::query("UPDATE events SET sent = 1 WHERE event_id = ?")
            .bind(event_id.to_string())
            .execute(&self.pool)
            .await?;
        Ok(())
    }

    // получение недоставленных событий
    pub async fn unsent(&self) -> Result<Vec<Event>, sqlx::Error> {
        let rows = sqlx::query(
            "SELECT event_id, user_id, type, seq, produced_at, payload FROM events
             WHERE sent = 0 ORDER BY user_id, seq",
        )
        .fetch_all(&self.pool)
        .await?;
        rows.into_iter().map(row_to_event).collect()
    }
}

fn row_to_event(row: SqliteRow) -> Result<Event, sqlx::Error> {
    fn dec<E: Into<Box<dyn std::error::Error + Send + Sync>>>(e: E) -> sqlx::Error {
        sqlx::Error::Decode(e.into())
    }
    let event_id: String = row.try_get("event_id")?;
    let user_id: i64 = row.try_get("user_id")?;
    let event_type: String = row.try_get("type")?;
    let seq: i64 = row.try_get("seq")?;
    let produced_at: String = row.try_get("produced_at")?;
    let payload: String = row.try_get("payload")?;
    Ok(Event {
        event_id: Uuid::parse_str(&event_id).map_err(dec)?,
        user_id: user_id as u64,
        event_type: event_type.parse().map_err(dec)?,
        seq: seq as u64,
        produced_at: chrono::DateTime::parse_from_rfc3339(&produced_at)
            .map_err(dec)?
            .with_timezone(&chrono::Utc),
        payload: serde_json::from_str(&payload).map_err(dec)?,
    })
}
