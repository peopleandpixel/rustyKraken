use sqlx::{Row, SqlitePool};
use tracing::info;

use crate::model::Ink;

/// Der Mantle ist die Speicherschicht von rustyKraken.
/// Er speichert alle Ink-Nachrichten in einer SQLite-Datenbank.
pub struct Mantle {
    pool: SqlitePool,
}

impl Mantle {
    /// Erstellt eine neue Mantle-Instanz mit einer SQLite-Datenbank
    pub async fn new(db_path: &str) -> anyhow::Result<Self> {
        info!("Initializing Mantle with database: {}", db_path);

        let connection_string = format!("sqlite:{}", db_path);
        let pool = SqlitePool::connect(&connection_string).await?;

        let mantle = Self { pool };

        // Tabellen erstellen
        mantle.initialize().await?;

        info!("Mantle initialized successfully");
        Ok(mantle)
    }

    /// Erstellt die notwendigen Tabellen
    async fn initialize(&self) -> anyhow::Result<()> {
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS ink (
                id TEXT PRIMARY KEY,
                timestamp TEXT NOT NULL,
                topic TEXT NOT NULL,
                source_type TEXT NOT NULL,
                source_name TEXT NOT NULL,
                body TEXT NOT NULL,
                metadata TEXT,
                status TEXT NOT NULL DEFAULT 'new'
            )
        "#,
        )
        .execute(&self.pool)
        .await?;

        // Index für Topic-Suche
        sqlx::query("CREATE INDEX IF NOT EXISTS idx_ink_topic ON ink(topic)")
            .execute(&self.pool)
            .await?;

        // Index für Status-Suche
        sqlx::query("CREATE INDEX IF NOT EXISTS idx_ink_status ON ink(status)")
            .execute(&self.pool)
            .await?;

        info!("Mantle tables created");
        Ok(())
    }

    /// Speichert eine Ink-Nachricht
    pub async fn store(&self, ink: &Ink) -> anyhow::Result<()> {
        sqlx::query(
            r#"
            INSERT INTO ink (id, timestamp, topic, source_type, source_name, body, metadata, status)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        "#,
        )
        .bind(ink.id.to_string())
        .bind(ink.timestamp.to_rfc3339())
        .bind(&ink.topic)
        .bind(&ink.source.r#type)
        .bind(&ink.source.name)
        .bind(&ink.body)
        .bind(
            ink.metadata
                .as_ref()
                .map(serde_json::to_value)
                .transpose()?,
        )
        .bind(format!("{:?}", ink.status))
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// Holt alle Ink-Nachrichten für ein bestimmtes Topic
    #[allow(dead_code)]
    pub async fn get_by_topic(&self, topic: &str) -> anyhow::Result<Vec<Ink>> {
        let rows = sqlx::query(
            r#"
            SELECT id, timestamp, topic, source_type, source_name, body, metadata, status
            FROM ink
            WHERE topic = ?
            ORDER BY timestamp DESC
        "#,
        )
        .bind(topic)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter().map(Self::row_to_ink).collect()
    }

    /// Holt die neuesten N Nachrichten
    #[allow(dead_code)]
    pub async fn get_latest(&self, limit: u32) -> anyhow::Result<Vec<Ink>> {
        let rows = sqlx::query(
            r#"
            SELECT id, timestamp, topic, source_type, source_name, body, metadata, status
            FROM ink
            ORDER BY timestamp DESC
            LIMIT ?
        "#,
        )
        .bind(limit as i64)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter().map(Self::row_to_ink).collect()
    }

    /// Aktualisiert den Status einer Nachricht
    #[allow(dead_code)]
    pub async fn update_status(&self, ink_id: &str, status: &str) -> anyhow::Result<()> {
        sqlx::query(
            r#"
            UPDATE ink SET status = ? WHERE id = ?
        "#,
        )
        .bind(status)
        .bind(ink_id)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    /// Konvertiert eine Datenbankzeile in eine Ink-Struktur
    #[allow(dead_code)]
    fn row_to_ink(row: sqlx::sqlite::SqliteRow) -> anyhow::Result<Ink> {
        use chrono::{DateTime, Utc};
        use uuid::Uuid;

        let id: String = row.get("id");
        let timestamp: String = row.get("timestamp");
        let topic: String = row.get("topic");
        let source_type: String = row.get("source_type");
        let source_name: String = row.get("source_name");
        let body: String = row.get("body");
        let metadata: Option<String> = row.get("metadata");
        let status: String = row.get("status");

        let id = Uuid::parse_str(&id)?;
        let timestamp: DateTime<Utc> =
            DateTime::parse_from_rfc3339(&timestamp)?.with_timezone(&Utc);

        let metadata = match metadata {
            Some(m) => Some(serde_json::from_str(&m)?),
            None => None,
        };

        let status = match status.as_str() {
            "new" => crate::model::InkStatus::New,
            "processed" => crate::model::InkStatus::Processed,
            "archived" => crate::model::InkStatus::Archived,
            _ => crate::model::InkStatus::New,
        };

        Ok(Ink {
            id,
            timestamp,
            topic,
            source: crate::model::SourceInfo {
                r#type: source_type,
                name: source_name,
            },
            body,
            metadata,
            status,
        })
    }
}
