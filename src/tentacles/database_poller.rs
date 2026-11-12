use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use sqlx::{AnyPool, Row};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info};

use super::Tentacle;
use crate::eventbus::EventBus;
use crate::model::Ink;

/// Topic-Erkennungsmodus für Database-Poller-Ergebnisse
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "lowercase")]
pub enum DbPollerTopicMode {
    /// Topic aus der Tabellennamen ableiten
    #[default]
    Table,
    /// Topic aus einem Spaltenwert extrahieren
    Field,
    /// Alle Ergebnisse an ein festes Topic
    All,
}

/// Datenbank-Typ
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "lowercase")]
pub enum DbPollerDatabaseType {
    #[default]
    Sqlite,
    Postgres,
    MySql,
}

/// Vergleichsoperator für Change-Detection
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "lowercase")]
pub enum DbPollerComparison {
    /// Neuer Wert größer als letzter Wert
    #[default]
    GreaterThan,
    /// Neuer Wert kleiner als letzter Wert
    LessThan,
    /// Neuer Wert ungleich letzter Wert
    NotEqual,
    /// Jede Änderung (Standard)
    Any,
}

/// Konfiguration für den Database Poller Tentacle
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DbPollerTentacleConfig {
    /// Datenbank-URL (z.B. "sqlite:///data/app.db", "postgres://user:pass@localhost/db", "mysql://user:pass@localhost/db")
    pub database_url: String,
    /// Datenbank-Typ (wird automatisch erkannt, kann explizit gesetzt werden)
    #[serde(default)]
    pub database_type: DbPollerDatabaseType,
    /// SQL-Abfrage zum Pollen (z.B. "SELECT * FROM events WHERE created_at > ?")
    pub query: String,
    /// Polling-Interval in Sekunden
    #[serde(default = "default_poll_interval")]
    pub poll_interval_secs: u64,
    /// Topic-Erkennungsmodus
    #[serde(default)]
    pub topic_mode: DbPollerTopicMode,
    /// Spaltenname für Topic-Extraktion (bei field-Modus)
    #[serde(default)]
    pub topic_column: Option<String>,
    /// Fallback-Topic
    #[serde(default = "default_fallback_topic")]
    pub fallback_topic: String,
    /// Optional: Tabellen-basiertes Topic-Mapping
    #[serde(default)]
    pub table_topic_mapping: HashMap<String, String>,
    /// Spalte für Change-Detection (nur geänderte Zeilen verarbeiten)
    #[serde(default)]
    pub change_column: Option<String>,
    /// Vergleichsoperator für Change-Detection
    #[serde(default)]
    pub change_comparison: DbPollerComparison,
    /// Spalte für die letzte gelesene Position (Offset-Tracking)
    #[serde(default)]
    pub offset_column: Option<String>,
    /// Initialer Offset-Wert
    #[serde(default)]
    pub initial_offset: Option<String>,
    /// Maximale Zeilen pro Poll
    #[serde(default = "default_max_rows")]
    pub max_rows_per_poll: u32,
    /// Timeout für Abfragen in Sekunden
    #[serde(default = "default_query_timeout")]
    pub query_timeout_secs: u64,
    /// Nur neue Zeilen seit letztem Poll (benötigt offset_column)
    #[serde(default)]
    pub incremental_only: bool,
}

fn default_poll_interval() -> u64 {
    30
}

fn default_fallback_topic() -> String {
    "/database/poller".to_string()
}

fn default_max_rows() -> u32 {
    1000
}

fn default_query_timeout() -> u64 {
    30
}

pub struct DbPollerTentacle {
    name: String,
    config: DbPollerTentacleConfig,
}

impl DbPollerTentacle {
    pub fn new(name: String, config: DbPollerTentacleConfig) -> Self {
        Self { name, config }
    }

    /// Bestimmt den Datenbank-Typ aus der URL
    fn detect_database_type(&self, url: &str) -> DbPollerDatabaseType {
        if url.starts_with("sqlite") {
            DbPollerDatabaseType::Sqlite
        } else if url.starts_with("postgres") || url.starts_with("postgresql") {
            DbPollerDatabaseType::Postgres
        } else if url.starts_with("mysql") {
            DbPollerDatabaseType::MySql
        } else {
            self.config.database_type.clone()
        }
    }
}

/// Helper: wrap a dynamic SQL string for sqlx.
/// Query comes from config files, not user input — safe for dynamic SQL.
fn dynamic_sql(sql: String) -> sqlx::AssertSqlSafe<String> {
    sqlx::AssertSqlSafe(sql)
}

/// Freistehende Funktion: Bestimmt das Topic basierend auf der Zeile
fn resolve_topic(
    config: &DbPollerTentacleConfig,
    table_name: &str,
    row: &HashMap<String, String>,
) -> String {
    match config.topic_mode {
        DbPollerTopicMode::Table => {
            if let Some(topic) = config.table_topic_mapping.get(table_name) {
                return topic.clone();
            }
            format!("/database/{}", table_name)
        }
        DbPollerTopicMode::Field => {
            if let Some(ref column) = config.topic_column
                && let Some(value) = row.get(column)
            {
                return value.clone();
            }
            config.fallback_topic.clone()
        }
        DbPollerTopicMode::All => config.fallback_topic.clone(),
    }
}

/// Freistehende Funktion: Erstellt eine Ink-Nachricht aus einer Datenbankzeile
fn create_db_ink(topic: &str, tentacle_name: &str, row: &HashMap<String, String>) -> Ink {
    let body: Vec<String> = row.iter().map(|(k, v)| format!("{}: {}", k, v)).collect();
    let body_str = body.join(", ");

    let mut metadata = serde_json::Map::new();
    for (key, value) in row {
        metadata.insert(key.clone(), serde_json::Value::String(value.clone()));
    }

    let mut ink = Ink::new(
        topic,
        "database_poller",
        tentacle_name,
        format!("Database row: {}", body_str),
    );
    ink = ink.with_metadata(serde_json::Value::Object(metadata));
    ink
}

#[async_trait]
impl Tentacle for DbPollerTentacle {
    fn name(&self) -> &str {
        &self.name
    }

    fn r#type(&self) -> &str {
        "database_poller"
    }

    async fn start(&self, event_bus: Arc<EventBus>) -> Result<()> {
        info!(
            "Database Poller Tentacle '{}' starting with interval {}s",
            self.name, self.config.poll_interval_secs
        );

        let config = self.config.clone();
        let tentacle_name = self.name.clone();
        let event_bus_clone = Arc::clone(&event_bus);

        // Datenbank-Verbindung herstellen
        let db_type = self.detect_database_type(&config.database_url);
        info!(
            "Database Poller Tentacle '{}': detected database type {:?}",
            tentacle_name, db_type
        );

        let pool = AnyPool::connect(&config.database_url).await.map_err(|e| {
            anyhow::anyhow!(
                "Database Poller Tentacle '{}': failed to connect: {}",
                tentacle_name,
                e
            )
        })?;

        info!(
            "Database Poller Tentacle '{}': connected to database",
            tentacle_name
        );

        // Offset-Tracking
        let mut last_offset = config.initial_offset.clone();

        tokio::spawn(async move {
            loop {
                // Abfrage aufbauen
                let mut query = config.query.clone();

                // Incremental-Modus: Offset in die Abfrage einfügen
                if config.incremental_only
                    && let Some(ref _offset_col) = config.offset_column
                    && let Some(ref offset) = last_offset
                {
                    // Ersetze ? durch den Offset-Wert
                    query = query.replace("?", &format!("'{}'", offset.replace("'", "''")));
                    debug!(
                        "Database Poller Tentacle '{}': incremental query with offset '{}'",
                        tentacle_name, offset
                    );
                }

                // LIMIT hinzufügen, falls nicht vorhanden
                if !query.to_lowercase().contains("limit") {
                    query = format!("{} LIMIT {}", query, config.max_rows_per_poll);
                }

                debug!(
                    "Database Poller Tentacle '{}': executing query: {}",
                    tentacle_name, query
                );

                // Abfrage ausführen
                let rows: Vec<_> = match sqlx::query(dynamic_sql(query.clone()))
                    .fetch_all(&pool)
                    .await
                {
                    Ok(rows) => rows,
                    Err(e) => {
                        error!(
                            "Database Poller Tentacle '{}': query failed: {}",
                            tentacle_name, e
                        );
                        tokio::time::sleep(Duration::from_secs(config.poll_interval_secs)).await;
                        continue;
                    }
                };

                let row_count = rows.len();
                if row_count == 0 {
                    debug!("Database Poller Tentacle '{}': no new rows", tentacle_name);
                } else {
                    info!(
                        "Database Poller Tentacle '{}': received {} rows",
                        tentacle_name, row_count
                    );
                }

                // Zeilen verarbeiten
                for row in &rows {
                    // Spaltennamen extrahieren
                    let columns: Vec<String> =
                        row.columns().iter().map(|c| c.name.to_string()).collect();

                    // Zeile als HashMap konvertieren
                    let mut row_map = HashMap::new();
                    for col in &columns {
                        let value: String = row
                            .try_get::<Option<String>, &str>(col.as_str())
                            .unwrap_or(None)
                            .unwrap_or_else(|| "null".to_string());
                        row_map.insert(col.clone(), value);
                    }

                    // Topic bestimmen
                    let table_name = columns.first().map(|s| s.as_str()).unwrap_or("unknown");
                    let topic = resolve_topic(&config, table_name, &row_map);

                    // Ink erstellen und publishen
                    let ink = create_db_ink(&topic, &tentacle_name, &row_map);
                    event_bus_clone.publish(ink);

                    debug!(
                        "Database Poller Tentacle '{}': published row to topic '{}'",
                        tentacle_name, topic
                    );

                    // Offset aktualisieren
                    if let Some(ref offset_col) = config.offset_column
                        && let Some(offset_value) = row_map.get(offset_col)
                    {
                        last_offset = Some(offset_value.clone());
                    }
                }

                tokio::time::sleep(Duration::from_secs(config.poll_interval_secs)).await;
            }
        });

        // Blockiere den Task, damit er läuft
        loop {
            tokio::time::sleep(Duration::from_secs(60)).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_config() -> DbPollerTentacleConfig {
        DbPollerTentacleConfig {
            database_url: "sqlite:///data/test.db".to_string(),
            database_type: DbPollerDatabaseType::Sqlite,
            query: "SELECT * FROM events".to_string(),
            poll_interval_secs: 30,
            topic_mode: DbPollerTopicMode::Table,
            topic_column: None,
            fallback_topic: "/database/poller".to_string(),
            table_topic_mapping: HashMap::new(),
            change_column: None,
            change_comparison: DbPollerComparison::Any,
            offset_column: None,
            initial_offset: None,
            max_rows_per_poll: 1000,
            query_timeout_secs: 30,
            incremental_only: false,
        }
    }

    #[test]
    fn test_db_poller_topic_mode_table() {
        let config = make_config();
        let mut row = HashMap::new();
        row.insert("id".to_string(), "1".to_string());
        assert_eq!(resolve_topic(&config, "events", &row), "/database/events");
    }

    #[test]
    fn test_db_poller_topic_mode_table_mapping() {
        let mut config = make_config();
        let mut mapping = HashMap::new();
        mapping.insert("events".to_string(), "/events/new".to_string());
        config.table_topic_mapping = mapping;

        let mut row = HashMap::new();
        row.insert("id".to_string(), "1".to_string());
        assert_eq!(resolve_topic(&config, "events", &row), "/events/new");
    }

    #[test]
    fn test_db_poller_topic_mode_field() {
        let mut config = make_config();
        config.topic_mode = DbPollerTopicMode::Field;
        config.topic_column = Some("event_type".to_string());

        let mut row = HashMap::new();
        row.insert("event_type".to_string(), "user.login".to_string());
        assert_eq!(resolve_topic(&config, "events", &row), "user.login");
    }

    #[test]
    fn test_db_poller_topic_mode_field_missing() {
        let mut config = make_config();
        config.topic_mode = DbPollerTopicMode::Field;
        config.topic_column = Some("event_type".to_string());

        let mut row = HashMap::new();
        row.insert("other".to_string(), "value".to_string());
        assert_eq!(resolve_topic(&config, "events", &row), "/database/poller");
    }

    #[test]
    fn test_db_poller_topic_mode_all() {
        let mut config = make_config();
        config.topic_mode = DbPollerTopicMode::All;
        config.fallback_topic = "/database/all".to_string();

        let mut row = HashMap::new();
        row.insert("id".to_string(), "1".to_string());
        assert_eq!(resolve_topic(&config, "events", &row), "/database/all");
    }

    #[test]
    fn test_create_db_ink() {
        let mut row = HashMap::new();
        row.insert("id".to_string(), "123".to_string());
        row.insert("name".to_string(), "test".to_string());

        let ink = create_db_ink("/database/test", "poller-1", &row);

        assert_eq!(ink.topic, "/database/test");
        assert_eq!(ink.source.r#type, "database_poller");
        assert_eq!(ink.source.name, "poller-1");

        if let Some(serde_json::Value::Object(meta)) = &ink.metadata {
            assert_eq!(meta.get("id").and_then(|v| v.as_str()), Some("123"));
            assert_eq!(meta.get("name").and_then(|v| v.as_str()), Some("test"));
        } else {
            panic!("Expected metadata to be an object");
        }
    }

    #[test]
    fn test_detect_database_type_sqlite() {
        let config = make_config();
        let tentacle = DbPollerTentacle::new("test".to_string(), config);
        assert_eq!(
            tentacle.detect_database_type("sqlite:///data/app.db"),
            DbPollerDatabaseType::Sqlite
        );
    }

    #[test]
    fn test_detect_database_type_postgres() {
        let config = make_config();
        let tentacle = DbPollerTentacle::new("test".to_string(), config);
        assert_eq!(
            tentacle.detect_database_type("postgres://user:pass@localhost/db"),
            DbPollerDatabaseType::Postgres
        );
    }

    #[test]
    fn test_detect_database_type_mysql() {
        let config = make_config();
        let tentacle = DbPollerTentacle::new("test".to_string(), config);
        assert_eq!(
            tentacle.detect_database_type("mysql://user:pass@localhost/db"),
            DbPollerDatabaseType::MySql
        );
    }

    #[test]
    fn test_db_poller_tentacle_new() {
        let config = make_config();
        let tentacle = DbPollerTentacle::new("test-poller".to_string(), config);
        assert_eq!(tentacle.name(), "test-poller");
        assert_eq!(tentacle.r#type(), "database_poller");
    }

    #[test]
    fn test_db_poller_config_deserialization() {
        let yaml = r#"
database_url: "sqlite:///data/app.db"
query: "SELECT * FROM events WHERE created_at > ?"
poll_interval_secs: 60
topic_mode: field
topic_column: event_type
fallback_topic: "/events"
incremental_only: true
offset_column: id
initial_offset: "0"
max_rows_per_poll: 500
"#;

        let config: DbPollerTentacleConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.database_url, "sqlite:///data/app.db");
        assert_eq!(config.poll_interval_secs, 60);
        assert_eq!(config.topic_mode, DbPollerTopicMode::Field);
        assert_eq!(config.topic_column, Some("event_type".to_string()));
        assert!(config.incremental_only);
        assert_eq!(config.offset_column, Some("id".to_string()));
        assert_eq!(config.initial_offset, Some("0".to_string()));
    }
}
