#![allow(dead_code)]

use std::collections::VecDeque;
use std::time::Duration;

use sqlx::{Row, SqlitePool};
use tokio::sync::mpsc;
use tokio::time::interval;
use tracing::{debug, info, warn};

use crate::model::Ink;

/// Der Mantle ist die Speicherschicht von rustyKraken.
/// Er speichert alle Ink-Nachrichten in einer SQLite-Datenbank.
/// Unterstützt Batch-Speicherung, WAL-Modus, FTS5 Full-Text-Suche,
/// Partitionierung und Komprimierung.
pub struct Mantle {
    pool: SqlitePool,
    batch_sender: Option<mpsc::Sender<Ink>>,
}

impl Mantle {
    /// Erstellt eine neue Mantle-Instanz mit einer SQLite-Datenbank
    /// Aktiviert WAL-Modus und richtet Batch-Speicherung ein.
    pub async fn new(db_path: &str) -> anyhow::Result<Self> {
        info!("Initializing Mantle with database: {}", db_path);

        let connection_string = format!("sqlite:{}", db_path);
        let pool = SqlitePool::connect(&connection_string).await?;

        // WAL-Modus aktivieren für bessere Schreibperformance
        sqlx::query("PRAGMA journal_mode = WAL")
            .execute(&pool)
            .await?;
        info!("WAL mode enabled for SQLite");

        // synchronous = NORMAL für guten Balance zwischen Performance und Sicherheit
        sqlx::query("PRAGMA synchronous = NORMAL")
            .execute(&pool)
            .await?;

        // cache_size erhöhen (in KB, negativ = MB)
        sqlx::query("PRAGMA cache_size = -20000")
            .execute(&pool)
            .await?;

        // mmap_size erhöhen für bessere Lesepformance
        sqlx::query("PRAGMA mmap_size = 268435456")
            .execute(&pool)
            .await?;

        let mantle = Self {
            pool,
            batch_sender: None,
        };

        // Tabellen erstellen
        mantle.initialize().await?;

        info!("Mantle initialized successfully");
        Ok(mantle)
    }

    /// Erstellt eine neue Mantle-Instanz mit Batch-Speicherung
    /// `batch_size`: Anzahl der Nachrichten vor dem Schreiben
    /// `batch_interval`: Maximale Wartezeit vor dem Schreiben (in Millisekunden)
    pub async fn with_batching(
        db_path: &str,
        batch_size: usize,
        batch_interval_ms: u64,
    ) -> anyhow::Result<Self> {
        info!(
            "Initializing Mantle with batching: size={}, interval={}ms",
            batch_size, batch_interval_ms
        );

        let connection_string = format!("sqlite:{}", db_path);
        let pool = SqlitePool::connect(&connection_string).await?;

        // WAL-Modus aktivieren
        sqlx::query("PRAGMA journal_mode = WAL")
            .execute(&pool)
            .await?;
        sqlx::query("PRAGMA synchronous = NORMAL")
            .execute(&pool)
            .await?;
        sqlx::query("PRAGMA cache_size = -20000")
            .execute(&pool)
            .await?;
        sqlx::query("PRAGMA mmap_size = 268435456")
            .execute(&pool)
            .await?;

        let mantle = Self {
            pool: pool.clone(),
            batch_sender: None,
        };

        // Tabellen erstellen
        mantle.initialize().await?;

        // Batch-Schleife starten
        let (tx, mut rx) = mpsc::channel::<Ink>(4096);
        let pool_clone = pool.clone();
        let batch_size = batch_size.max(1);
        let batch_interval = Duration::from_millis(batch_interval_ms.max(100));

        tokio::spawn(async move {
            let mut batch: VecDeque<Ink> = VecDeque::with_capacity(batch_size);
            let mut timer = interval(batch_interval);

            loop {
                tokio::select! {
                    // Timer-gesteuerter Flush
                    _ = timer.tick() => {
                        if !batch.is_empty() {
                            let size = batch.len();
                            let ink_batch: Vec<Ink> = batch.drain(..).collect();
                            if let Err(e) = Self::store_batch(&pool_clone, &ink_batch).await {
                                warn!("Batch store failed: {}", e);
                            } else {
                                debug!("Flushed batch of {} inks", size);
                            }
                        }
                    }
                    // Neue Nachricht
                    maybe_ink = rx.recv() => {
                        match maybe_ink {
                            Some(ink) => {
                                batch.push_back(ink);
                                if batch.len() >= batch_size {
                                    let ink_batch: Vec<Ink> = batch.drain(..).collect();
                                    let batch_len = ink_batch.len();
                                    if let Err(e) = Self::store_batch(&pool_clone, &ink_batch).await {
                                        warn!("Batch store failed: {}", e);
                                    } else {
                                        debug!("Batch stored {} inks", batch_len);
                                    }
                                }
                            }
                            // Channel geschlossen
                            None => {
                                // Letzten Batch schreiben
                                if !batch.is_empty() {
                                    let ink_batch: Vec<Ink> = batch.drain(..).collect();
                                    let _ = Self::store_batch(&pool_clone, &ink_batch).await;
                                }
                                break;
                            }
                        }
                    }
                }
            }
        });

        let mantle = Self {
            pool,
            batch_sender: Some(tx),
        };

        info!("Mantle with batching initialized successfully");
        Ok(mantle)
    }

    /// Speichert einen Batch von Ink-Nachrichten in einer Transaktion
    async fn store_batch(pool: &SqlitePool, inks: &[Ink]) -> anyhow::Result<()> {
        let mut tx = pool.begin().await?;

        for ink in inks {
            sqlx::query(
                r#"
                INSERT OR REPLACE INTO ink (id, timestamp, topic, source_type, source_name, body, metadata, status)
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
                    .and_then(|m| serde_json::to_value(m).ok()),
            )
            .bind(format!("{:?}", ink.status))
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;
        Ok(())
    }

    /// Erstellt die notwendigen Tabellen mit FTS5 Support
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

        // FTS5 Virtual Table für Full-Text-Suche
        sqlx::query(
            r#"
            CREATE VIRTUAL TABLE IF NOT EXISTS ink_fts USING fts5(
                topic, body, metadata,
                content='ink',
                content_rowid='rowid'
            )
        "#,
        )
        .execute(&self.pool)
        .await?;

        // Triggers für automatische FTS-Updates
        sqlx::query(
            r#"
            CREATE TRIGGER IF NOT EXISTS ink_ai AFTER INSERT ON ink BEGIN
                INSERT INTO ink_fts(rowid, topic, body, metadata)
                VALUES (new.rowid, new.topic, new.body, new.metadata);
            END
        "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            r#"
            CREATE TRIGGER IF NOT EXISTS ink_ad AFTER DELETE ON ink BEGIN
                DELETE FROM ink_fts WHERE rowid = old.rowid;
            END
        "#,
        )
        .execute(&self.pool)
        .await?;

        sqlx::query(
            r#"
            CREATE TRIGGER IF NOT EXISTS ink_au AFTER UPDATE ON ink BEGIN
                UPDATE ink_fts SET
                    topic = new.topic,
                    body = new.body,
                    metadata = new.metadata
                WHERE rowid = new.rowid;
            END
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

        // Index für timestamp (für Partitionierung und Archivierung)
        sqlx::query("CREATE INDEX IF NOT EXISTS idx_ink_timestamp ON ink(timestamp)")
            .execute(&self.pool)
            .await?;

        info!("Mantle tables created with FTS5 support");
        Ok(())
    }

    /// Speichert eine Ink-Nachricht (direkt, ohne Batch)
    pub async fn store(&self, ink: &Ink) -> anyhow::Result<()> {
        // Wenn Batch-Modus aktiv, sende an Batch-Queue
        if let Some(ref tx) = self.batch_sender {
            tx.send(ink.clone())
                .await
                .map_err(|e| anyhow::anyhow!("Batch queue full: {}", e))?;
            return Ok(());
        }

        // Direkt speichern
        sqlx::query(
            r#"
            INSERT OR REPLACE INTO ink (id, timestamp, topic, source_type, source_name, body, metadata, status)
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
                .and_then(|m| serde_json::to_value(m).ok()),
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

    /// Full-Text-Suche über topic, body und metadata
    #[allow(dead_code)]
    pub async fn search(&self, query: &str) -> anyhow::Result<Vec<Ink>> {
        let rows = sqlx::query(
            r#"
            SELECT i.id, i.timestamp, i.topic, i.source_type, i.source_name, i.body, i.metadata, i.status
            FROM ink i
            INNER JOIN ink_fts ON ink_fts.rowid = i.rowid
            WHERE ink_fts MATCH ?
            ORDER BY rank
            LIMIT 100
        "#,
        )
        .bind(query)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter().map(Self::row_to_ink).collect()
    }

    /// Full-Text-Suche mit Limit
    #[allow(dead_code)]
    pub async fn search_with_limit(&self, query: &str, limit: u32) -> anyhow::Result<Vec<Ink>> {
        let limit = limit.max(1) as i64;
        let rows = sqlx::query(
            r#"
            SELECT i.id, i.timestamp, i.topic, i.source_type, i.source_name, i.body, i.metadata, i.status
            FROM ink i
            INNER JOIN ink_fts ON ink_fts.rowid = i.rowid
            WHERE ink_fts MATCH ?
            ORDER BY rank
            LIMIT ?
        "#,
        )
        .bind(query)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        rows.into_iter().map(Self::row_to_ink).collect()
    }

    /// Archiviert Nachrichten älter als `days` Tage
    /// Verschiebt sie in die ink_archive Tabelle und löscht sie aus ink
    #[allow(dead_code)]
    pub async fn archive_older_than(&self, days: u32) -> anyhow::Result<u64> {
        // Archive-Tabelle erstellen falls nötig
        sqlx::query(
            r#"
            CREATE TABLE IF NOT EXISTS ink_archive (
                id TEXT PRIMARY KEY,
                timestamp TEXT NOT NULL,
                topic TEXT NOT NULL,
                source_type TEXT NOT NULL,
                source_name TEXT NOT NULL,
                body TEXT NOT NULL,
                metadata TEXT,
                status TEXT NOT NULL DEFAULT 'archived',
                archived_at TEXT NOT NULL
            )
        "#,
        )
        .execute(&self.pool)
        .await?;

        let cutoff = format!("datetime('now', '-{} days')", days);

        // In Archive-Tabelle kopieren
        let result = sqlx::query(
            r#"
            INSERT OR IGNORE INTO ink_archive (id, timestamp, topic, source_type, source_name, body, metadata, status, archived_at)
            SELECT id, timestamp, topic, source_type, source_name, body, metadata, 'archived', datetime('now')
            FROM ink
            WHERE timestamp < ?
        "#,
        )
        .bind(&cutoff)
        .execute(&self.pool)
        .await?;

        let count = result.rows_affected();
        info!("Archived {} inks older than {} days", count, days);

        // Aus Haupttabelle löschen
        sqlx::query(
            r#"
            DELETE FROM ink WHERE timestamp < ?
        "#,
        )
        .bind(&cutoff)
        .execute(&self.pool)
        .await?;

        Ok(count)
    }

    /// Führt VACUUM aus, um die Datenbank zu optimieren
    /// Sollte regelmäßig (z.B. wöchentlich) aufgerufen werden
    #[allow(dead_code)]
    pub async fn vacuum(&self) -> anyhow::Result<()> {
        info!("Running VACUUM on database");
        sqlx::query("VACUUM").execute(&self.pool).await?;
        info!("VACUUM completed");
        Ok(())
    }

    /// Führt ANALYZE aus, um Index-Statistiken zu aktualisieren
    #[allow(dead_code)]
    pub async fn analyze(&self) -> anyhow::Result<()> {
        info!("Running ANALYZE on database");
        sqlx::query("ANALYZE").execute(&self.pool).await?;
        info!("ANALYZE completed");
        Ok(())
    }

    /// Gibt die Anzahl der Nachrichten in der Datenbank zurück
    #[allow(dead_code)]
    pub async fn count(&self) -> anyhow::Result<u64> {
        let row: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM ink")
            .fetch_one(&self.pool)
            .await?;
        Ok(row.0 as u64)
    }

    /// Gibt die Anzahl der archivierten Nachrichten zurück
    #[allow(dead_code)]
    pub async fn count_archived(&self) -> anyhow::Result<u64> {
        let row: Option<(i64,)> = sqlx::query_as("SELECT COUNT(*) FROM ink_archive")
            .fetch_optional(&self.pool)
            .await?;
        Ok(row.map(|r| r.0 as u64).unwrap_or(0))
    }

    /// Komprimiert große Body-Felder mit zlib
    /// Speichert komprimierte Daten in einer separaten Spalte
    #[allow(dead_code)]
    pub async fn compress_large_bodies(&self, min_size: usize) -> anyhow::Result<u64> {
        use flate2::write::ZlibEncoder;
        use std::io::Write;

        // body_compressed Spalte hinzufügen falls nötig (ignoriert Fehler wenn Spalte bereits existiert)
        let _ = sqlx::query("ALTER TABLE ink ADD COLUMN body_compressed BLOB")
            .execute(&self.pool)
            .await;

        let min_size = min_size.max(1024); // Mindestens 1KB

        // Nachrichten mit großen Bodies finden
        let rows = sqlx::query(
            r#"
            SELECT id, body FROM ink
            WHERE length(body) > ? AND body_compressed IS NULL
        "#,
        )
        .bind(min_size as i64)
        .fetch_all(&self.pool)
        .await?;

        let mut count = 0;
        let mut tx = self.pool.begin().await?;

        for row in rows {
            let id: String = row.get("id");
            let body: String = row.get("body");

            let mut encoder = ZlibEncoder::new(Vec::new(), flate2::Compression::default());
            encoder.write_all(body.as_bytes()).ok();
            let compressed = encoder.finish().ok();

            if let Some(compressed_data) = compressed {
                let original_size = body.len();
                let compressed_size = compressed_data.len();

                // Nur komprimieren wenn es tatsächlich kleiner wird
                if compressed_size < original_size {
                    sqlx::query("UPDATE ink SET body_compressed = ? WHERE id = ?")
                        .bind(&compressed_data as &[u8])
                        .bind(&id)
                        .execute(&mut *tx)
                        .await?;
                    count += 1;
                }
            }
        }

        tx.commit().await?;
        info!("Compressed {} bodies (min_size: {} bytes)", count, min_size);
        Ok(count)
    }

    /// Dekomprimiert einen Body aus der body_compressed Spalte
    #[allow(dead_code)]
    pub async fn get_body_decompressed(&self, ink_id: &str) -> anyhow::Result<String> {
        use flate2::read::ZlibDecoder;
        use std::io::Read;

        let row = sqlx::query(
            r#"
            SELECT body, body_compressed FROM ink WHERE id = ?
        "#,
        )
        .bind(ink_id)
        .fetch_optional(&self.pool)
        .await?;

        match row {
            Some(r) => {
                let body: String = r.get("body");
                let compressed: Option<Vec<u8>> = r.get("body_compressed");

                if let Some(compressed_data) = compressed {
                    let mut decoder = ZlibDecoder::new(&compressed_data[..]);
                    let mut decompressed = Vec::new();
                    decoder.read_to_end(&mut decompressed)?;
                    Ok(String::from_utf8(decompressed)?)
                } else {
                    Ok(body)
                }
            }
            None => anyhow::bail!("Ink not found: {}", ink_id),
        }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{InkStatus, SourceInfo};
    use uuid::Uuid;

    fn test_ink() -> Ink {
        Ink {
            id: Uuid::new_v4(),
            timestamp: chrono::Utc::now(),
            topic: "/test/topic".to_string(),
            source: SourceInfo {
                r#type: "test".to_string(),
                name: "test".to_string(),
            },
            body: "test body content".to_string(),
            metadata: None,
            status: InkStatus::New,
        }
    }

    #[tokio::test]
    async fn test_mantle_new() -> anyhow::Result<()> {
        let mantle = Mantle::new(":memory:").await?;
        let ink = test_ink();
        mantle.store(&ink).await?;
        let count = mantle.count().await?;
        assert_eq!(count, 1);
        Ok(())
    }

    #[tokio::test]
    async fn test_mantle_store_and_get_latest() -> anyhow::Result<()> {
        let mantle = Mantle::new(":memory:").await?;
        let ink = test_ink();
        mantle.store(&ink).await?;

        let latest = mantle.get_latest(10).await?;
        assert_eq!(latest.len(), 1);
        assert_eq!(latest[0].topic, "/test/topic");
        Ok(())
    }

    #[tokio::test]
    async fn test_mantle_store_and_get_by_topic() -> anyhow::Result<()> {
        let mantle = Mantle::new(":memory:").await?;
        let ink = test_ink();
        mantle.store(&ink).await?;

        let by_topic = mantle.get_by_topic("/test/topic").await?;
        assert_eq!(by_topic.len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn test_mantle_update_status() -> anyhow::Result<()> {
        let mantle = Mantle::new(":memory:").await?;
        let ink = test_ink();
        mantle.store(&ink).await?;

        mantle
            .update_status(&ink.id.to_string(), "processed")
            .await?;

        let latest = mantle.get_latest(10).await?;
        assert_eq!(latest[0].status, InkStatus::Processed);
        Ok(())
    }

    #[tokio::test]
    async fn test_mantle_search() -> anyhow::Result<()> {
        let mantle = Mantle::new(":memory:").await?;
        let ink = test_ink();
        mantle.store(&ink).await?;

        let results = mantle.search("test").await?;
        assert!(!results.is_empty());
        Ok(())
    }

    #[tokio::test]
    async fn test_mantle_search_with_limit() -> anyhow::Result<()> {
        let mantle = Mantle::new(":memory:").await?;
        let ink = test_ink();
        mantle.store(&ink).await?;

        let results = mantle.search_with_limit("test", 5).await?;
        assert!(!results.is_empty());
        assert!(results.len() <= 5);
        Ok(())
    }

    #[tokio::test]
    async fn test_mantle_count() -> anyhow::Result<()> {
        let mantle = Mantle::new(":memory:").await?;
        assert_eq!(mantle.count().await?, 0);

        let ink = test_ink();
        mantle.store(&ink).await?;
        assert_eq!(mantle.count().await?, 1);
        Ok(())
    }

    #[tokio::test]
    async fn test_mantle_vacuum() -> anyhow::Result<()> {
        let mantle = Mantle::new(":memory:").await?;
        mantle.vacuum().await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_mantle_analyze() -> anyhow::Result<()> {
        let mantle = Mantle::new(":memory:").await?;
        mantle.analyze().await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_mantle_archive_older_than() -> anyhow::Result<()> {
        let mantle = Mantle::new(":memory:").await?;
        let ink = test_ink();
        mantle.store(&ink).await?;

        let _ = mantle.archive_older_than(0).await?;
        Ok(())
    }

    #[tokio::test]
    async fn test_mantle_compress_large_bodies() -> anyhow::Result<()> {
        let mantle = Mantle::new(":memory:").await?;

        let large_body = "x".repeat(2048);
        let ink = Ink {
            id: Uuid::new_v4(),
            timestamp: chrono::Utc::now(),
            topic: "/test/large".to_string(),
            source: SourceInfo {
                r#type: "test".to_string(),
                name: "test".to_string(),
            },
            body: large_body.clone(),
            metadata: None,
            status: InkStatus::New,
        };
        mantle.store(&ink).await?;

        let compressed = mantle.compress_large_bodies(1024).await?;
        assert_eq!(compressed, 1);

        let decompressed = mantle.get_body_decompressed(&ink.id.to_string()).await?;
        assert_eq!(decompressed, large_body);
        Ok(())
    }

    #[tokio::test]
    async fn test_mantle_with_batching() -> anyhow::Result<()> {
        let mantle = Mantle::with_batching(":memory:", 5, 100).await?;

        for _ in 0..3 {
            let ink = test_ink();
            mantle.store(&ink).await?;
        }

        tokio::time::sleep(Duration::from_millis(200)).await;

        let count = mantle.count().await?;
        assert!(count >= 3);
        Ok(())
    }

    #[tokio::test]
    async fn test_mantle_metadata_serialization() -> anyhow::Result<()> {
        let mantle = Mantle::new(":memory:").await?;

        let mut metadata = serde_json::Map::new();
        metadata.insert("key1".to_string(), serde_json::json!("value1"));
        let ink = Ink {
            id: Uuid::new_v4(),
            timestamp: chrono::Utc::now(),
            topic: "/test/meta".to_string(),
            source: SourceInfo {
                r#type: "test".to_string(),
                name: "test".to_string(),
            },
            body: "test".to_string(),
            metadata: Some(serde_json::Value::Object(metadata.clone())),
            status: InkStatus::New,
        };

        mantle.store(&ink).await?;
        let latest = mantle.get_latest(10).await?;
        assert_eq!(latest.len(), 1);
        assert!(latest[0].metadata.is_some());
        Ok(())
    }
}
