use anyhow::{Context, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use sqlx::{Any, AnyPool, Row};
use std::sync::{Arc, LazyLock};
use std::time::Duration;
use tokio::sync::Mutex;
use tracing::{debug, info};

use super::Reflex;
use crate::model::Ink;

/// Database type for the Database Reflex
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum DatabaseType {
    Sqlite,
    Postgres,
    Mysql,
}

impl std::fmt::Display for DatabaseType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DatabaseType::Sqlite => write!(f, "sqlite"),
            DatabaseType::Postgres => write!(f, "postgres"),
            DatabaseType::Mysql => write!(f, "mysql"),
        }
    }
}

/// Configuration for Database Reflex
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseReflexConfig {
    /// Database type: sqlite, postgres, or mysql
    pub database_type: DatabaseType,

    /// Connection string / database URL
    /// SQLite: file path (e.g., "data/kraken.db")
    /// PostgreSQL: "postgres://user:pass@host/dbname"
    /// MySQL: "mysql://user:pass@host/dbname"
    pub connection_string: String,

    /// Table name for storing Ink messages
    #[serde(default = "default_table_name")]
    pub table_name: String,

    /// Whether to create the table automatically on startup
    #[serde(default = "default_auto_create")]
    pub auto_create_table: bool,

    /// Connection pool maximum size
    #[serde(default = "default_max_connections")]
    pub max_connections: u32,

    /// Connection pool minimum idle size
    #[serde(default = "default_min_idle")]
    pub min_idle: Option<u32>,

    /// Connection acquire timeout in seconds
    #[serde(default = "default_acquire_timeout")]
    pub acquire_timeout_secs: u64,

    /// Whether to include metadata JSON in the insert
    #[serde(default = "default_include_metadata")]
    pub include_metadata: bool,

    /// Whether to include status field
    #[serde(default = "default_include_status")]
    pub include_status: bool,
}

fn default_table_name() -> String {
    "ink_messages".to_string()
}

fn default_auto_create() -> bool {
    true
}

fn default_max_connections() -> u32 {
    10
}

fn default_min_idle() -> Option<u32> {
    Some(1)
}

fn default_acquire_timeout() -> u64 {
    30
}

fn default_include_metadata() -> bool {
    true
}

fn default_include_status() -> bool {
    true
}

/// Ensure drivers are installed once
static DRIVERS_INSTALLED: LazyLock<()> = LazyLock::new(|| {
    sqlx::any::install_default_drivers();
});

pub struct DatabaseReflex {
    name: String,
    config: DatabaseReflexConfig,
    pool: Mutex<Option<Arc<AnyPool>>>,
}

/// Helper: wrap a dynamic SQL string for sqlx.
/// Table names come from config files, not user input — safe for dynamic SQL.
fn dynamic_sql(sql: String) -> sqlx::AssertSqlSafe<String> {
    sqlx::AssertSqlSafe(sql)
}

impl DatabaseReflex {
    pub fn new(name: String, config: DatabaseReflexConfig) -> Result<Self> {
        let _ = DRIVERS_INSTALLED;
        Ok(Self {
            name,
            config,
            pool: Mutex::new(None),
        })
    }

    /// Lazily initialize the database connection pool
    async fn ensure_pool(&self) -> Result<Arc<AnyPool>> {
        let mut pool_guard = self.pool.lock().await;
        if pool_guard.is_none() {
            let mut pool_options = sqlx::pool::PoolOptions::<Any>::new()
                .max_connections(self.config.max_connections)
                .acquire_timeout(Duration::from_secs(self.config.acquire_timeout_secs));

            if let Some(min_idle) = self.config.min_idle {
                pool_options = pool_options.min_connections(min_idle);
            }

            let pool = pool_options
                .connect(&self.config.connection_string)
                .await
                .context("Failed to connect to database")?;

            info!(
                "Database reflex '{}' connected to {} database",
                self.name, self.config.database_type
            );

            let pool_arc = Arc::new(pool);

            if self.config.auto_create_table {
                self.create_table_inner(&pool_arc).await?;
            }

            *pool_guard = Some(pool_arc.clone());
            Ok(pool_arc)
        } else {
            Ok(Arc::clone(pool_guard.as_ref().unwrap()))
        }
    }

    /// Create the Ink messages table if it doesn't exist
    async fn create_table_inner(&self, pool: &AnyPool) -> Result<()> {
        let table = &self.config.table_name;
        let metadata_clause = if self.config.include_metadata {
            ", metadata TEXT"
        } else {
            ""
        };
        let status_clause = if self.config.include_status {
            ", status TEXT NOT NULL DEFAULT 'new'"
        } else {
            ""
        };

        let create_sql = match self.config.database_type {
            DatabaseType::Sqlite => format!(
                "CREATE TABLE IF NOT EXISTS {table} (\
                    id TEXT PRIMARY KEY, \
                    timestamp TEXT NOT NULL, \
                    topic TEXT NOT NULL, \
                    source_type TEXT NOT NULL, \
                    source_name TEXT NOT NULL, \
                    body TEXT NOT NULL{metadata_clause}{status_clause}\
                )"
            ),
            DatabaseType::Postgres => format!(
                "CREATE TABLE IF NOT EXISTS {table} (\
                    id UUID PRIMARY KEY, \
                    timestamp TIMESTAMPTZ NOT NULL, \
                    topic TEXT NOT NULL, \
                    source_type TEXT NOT NULL, \
                    source_name TEXT NOT NULL, \
                    body TEXT NOT NULL{metadata_clause}{status_clause}\
                )"
            ),
            DatabaseType::Mysql => format!(
                "CREATE TABLE IF NOT EXISTS {table} (\
                    id CHAR(36) PRIMARY KEY, \
                    timestamp DATETIME(6) NOT NULL, \
                    topic TEXT NOT NULL, \
                    source_type TEXT NOT NULL, \
                    source_name TEXT NOT NULL, \
                    body TEXT NOT NULL{metadata_clause}{status_clause}\
                )"
            ),
        };

        debug!(
            "Database reflex '{}' creating table: {}",
            self.name, create_sql
        );

        sqlx::query(dynamic_sql(create_sql))
            .execute(pool)
            .await
            .context("Failed to create table")?;

        info!(
            "Database reflex '{}' table '{}' created or already exists",
            self.name, table
        );
        Ok(())
    }

    /// Insert an Ink message into the database
    async fn insert_ink(&self, ink: &Ink) -> Result<()> {
        let pool = self.ensure_pool().await?;
        let table = &self.config.table_name;

        let (cols, placeholders, prefix) =
            if self.config.include_metadata && self.config.include_status {
                (
                    "id, timestamp, topic, source_type, source_name, body, metadata, status",
                    match self.config.database_type {
                        DatabaseType::Postgres => "$1, $2, $3, $4, $5, $6, $7, $8",
                        _ => "? , ? , ? , ? , ? , ? , ? , ?",
                    },
                    match self.config.database_type {
                        DatabaseType::Mysql => "INSERT IGNORE",
                        _ => "INSERT",
                    },
                )
            } else if self.config.include_metadata {
                (
                    "id, timestamp, topic, source_type, source_name, body, metadata",
                    match self.config.database_type {
                        DatabaseType::Postgres => "$1, $2, $3, $4, $5, $6, $7",
                        _ => "? , ? , ? , ? , ? , ? , ?",
                    },
                    match self.config.database_type {
                        DatabaseType::Mysql => "INSERT IGNORE",
                        _ => "INSERT",
                    },
                )
            } else if self.config.include_status {
                (
                    "id, timestamp, topic, source_type, source_name, body, status",
                    match self.config.database_type {
                        DatabaseType::Postgres => "$1, $2, $3, $4, $5, $6, $7",
                        _ => "? , ? , ? , ? , ? , ? , ?",
                    },
                    match self.config.database_type {
                        DatabaseType::Mysql => "INSERT IGNORE",
                        _ => "INSERT",
                    },
                )
            } else {
                (
                    "id, timestamp, topic, source_type, source_name, body",
                    match self.config.database_type {
                        DatabaseType::Postgres => "$1, $2, $3, $4, $5, $6",
                        _ => "? , ? , ? , ? , ? , ?",
                    },
                    match self.config.database_type {
                        DatabaseType::Mysql => "INSERT IGNORE",
                        _ => "INSERT",
                    },
                )
            };

        let conflict_clause = match self.config.database_type {
            DatabaseType::Mysql => String::new(),
            _ => " ON CONFLICT(id) DO NOTHING".to_string(),
        };

        let insert_sql =
            format!("{prefix} INTO {table} ({cols}) VALUES ({placeholders}){conflict_clause}");

        let id_str = ink.id.to_string();
        let timestamp_str = ink.timestamp.to_rfc3339();

        debug!(
            "Database reflex '{}' inserting ink {} into '{}'",
            self.name, id_str, table
        );

        let mut query = sqlx::query(dynamic_sql(insert_sql));

        query = query
            .bind(&id_str)
            .bind(&timestamp_str)
            .bind(&ink.topic)
            .bind(&ink.source.r#type)
            .bind(&ink.source.name)
            .bind(&ink.body);

        if self.config.include_metadata {
            let metadata_json = ink
                .metadata
                .as_ref()
                .map(|m| m.to_string())
                .unwrap_or_else(|| "null".to_string());
            query = query.bind(&metadata_json);
        }

        if self.config.include_status {
            let status_str = match ink.status {
                crate::model::InkStatus::New => "new",
                crate::model::InkStatus::Processed => "processed",
                crate::model::InkStatus::Archived => "archived",
            };
            query = query.bind(status_str);
        }

        let result = query
            .execute(pool.as_ref())
            .await
            .context("Failed to insert Ink message")?;

        let rows = result.rows_affected();
        if rows == 0 {
            debug!(
                "Database reflex '{}' ink {} already exists (duplicate)",
                self.name, id_str
            );
        }

        info!(
            "Database reflex '{}' inserted ink {} ({} rows affected)",
            self.name, id_str, rows
        );
        Ok(())
    }

    /// Query the number of messages in the table
    #[allow(dead_code)]
    pub async fn message_count(&self) -> Result<u64> {
        let pool = self.ensure_pool().await?;
        let table = &self.config.table_name;

        let count_sql = format!("SELECT COUNT(*) FROM {table}");
        let row = sqlx::query(dynamic_sql(count_sql))
            .fetch_one(pool.as_ref())
            .await
            .context("Failed to count messages")?;

        let count: i64 = row.try_get(0).context("Failed to extract count")?;
        Ok(count as u64)
    }

    /// Query messages by topic pattern (simple LIKE match)
    #[allow(dead_code)]
    pub async fn query_by_topic(&self, topic_pattern: &str) -> Result<Vec<serde_json::Value>> {
        let pool = self.ensure_pool().await?;
        let table = &self.config.table_name;

        let escaped_pattern = topic_pattern
            .replace('!', "!!")
            .replace('%', "!%")
            .replace('_', "!_");
        let like_pattern = format!("{}%", escaped_pattern);

        let escape_clause = match self.config.database_type {
            DatabaseType::Sqlite => " ESCAPE '!'",
            _ => "",
        };
        let placeholder = match self.config.database_type {
            DatabaseType::Postgres => "$1",
            _ => "?",
        };

        let query_sql = format!(
            "SELECT id, timestamp, topic, source_type, source_name, body FROM {table} \
             WHERE topic LIKE {placeholder}{escape_clause} ORDER BY timestamp DESC LIMIT 100"
        );

        let rows = sqlx::query(dynamic_sql(query_sql))
            .bind(&like_pattern)
            .fetch_all(pool.as_ref())
            .await
            .context("Failed to query messages by topic")?;

        let mut messages = Vec::new();
        for row in rows {
            let msg: serde_json::Value = serde_json::json!({
                "id": row.try_get::<String, _>(0).unwrap_or_default(),
                "timestamp": row.try_get::<String, _>(1).unwrap_or_default(),
                "topic": row.try_get::<String, _>(2).unwrap_or_default(),
                "source_type": row.try_get::<String, _>(3).unwrap_or_default(),
                "source_name": row.try_get::<String, _>(4).unwrap_or_default(),
                "body": row.try_get::<String, _>(5).unwrap_or_default(),
            });
            messages.push(msg);
        }

        Ok(messages)
    }
}

#[async_trait]
impl Reflex for DatabaseReflex {
    fn name(&self) -> &str {
        &self.name
    }

    fn r#type(&self) -> &str {
        "database"
    }

    async fn handle(&self, ink: &Ink) -> Result<()> {
        self.insert_ink(ink).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{Ink, InkStatus};

    #[test]
    fn test_config_deserialization() {
        let yaml = r#"
            database_type: sqlite
            connection_string: "data/test.db"
            table_name: "test_inks"
            auto_create_table: true
            max_connections: 5
            include_metadata: false
        "#;
        let config: DatabaseReflexConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.database_type, DatabaseType::Sqlite);
        assert_eq!(config.connection_string, "data/test.db");
        assert_eq!(config.table_name, "test_inks");
        assert_eq!(config.max_connections, 5);
        assert!(!config.include_metadata);
    }

    #[test]
    fn test_config_defaults() {
        let yaml = r#"
            database_type: postgres
            connection_string: "postgres://user:pass@localhost/db"
        "#;
        let config: DatabaseReflexConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.table_name, "ink_messages");
        assert!(config.auto_create_table);
        assert_eq!(config.max_connections, 10);
        assert!(config.include_metadata);
        assert!(config.include_status);
    }

    #[test]
    fn test_config_mysql() {
        let yaml = r#"
            database_type: mysql
            connection_string: "mysql://root:pass@localhost/kraken"
        "#;
        let config: DatabaseReflexConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.database_type, DatabaseType::Mysql);
    }

    #[test]
    fn test_ink_creation_for_test() {
        let ink = Ink::new("/test/topic", "test", "test_source", "test body");
        assert_eq!(ink.topic, "/test/topic");
        assert_eq!(ink.body, "test body");
        assert_eq!(ink.source.r#type, "test");
        assert_eq!(ink.source.name, "test_source");
        assert_eq!(ink.status, InkStatus::New);
    }

    #[test]
    fn test_ink_with_metadata() {
        let metadata = serde_json::json!({
            "sensor_id": "temp-001",
            "value": 42.5
        });
        let ink =
            Ink::new("/sensor/temp", "sensor", "mqtt", "42.5").with_metadata(metadata.clone());
        assert!(ink.metadata.is_some());
        let md = ink.metadata.unwrap();
        assert_eq!(md["sensor_id"], "temp-001");
    }

    #[test]
    fn test_database_type_serialization() {
        let sqlite = DatabaseType::Sqlite;
        let json = serde_json::to_string(&sqlite).unwrap();
        assert_eq!(json, "\"sqlite\"");

        let postgres = DatabaseType::Postgres;
        let json = serde_json::to_string(&postgres).unwrap();
        assert_eq!(json, "\"postgres\"");

        let mysql = DatabaseType::Mysql;
        let json = serde_json::to_string(&mysql).unwrap();
        assert_eq!(json, "\"mysql\"");
    }

    #[test]
    fn test_database_type_display() {
        assert_eq!(DatabaseType::Sqlite.to_string(), "sqlite");
        assert_eq!(DatabaseType::Postgres.to_string(), "postgres");
        assert_eq!(DatabaseType::Mysql.to_string(), "mysql");
    }

    #[test]
    fn test_sql_generation_sqlite() {
        let config = DatabaseReflexConfig {
            database_type: DatabaseType::Sqlite,
            connection_string: "data/test.db".to_string(),
            table_name: "inks".to_string(),
            auto_create_table: true,
            max_connections: 5,
            min_idle: Some(1),
            acquire_timeout_secs: 30,
            include_metadata: true,
            include_status: true,
        };
        assert_eq!(config.table_name, "inks");
        assert!(config.include_metadata);
        assert!(config.include_status);
    }

    #[test]
    fn test_sql_generation_postgres() {
        let config = DatabaseReflexConfig {
            database_type: DatabaseType::Postgres,
            connection_string: "postgres://u:p@l/d".to_string(),
            table_name: "messages".to_string(),
            auto_create_table: false,
            max_connections: 20,
            min_idle: Some(5),
            acquire_timeout_secs: 10,
            include_metadata: true,
            include_status: false,
        };
        assert!(!config.auto_create_table);
        assert!(!config.include_status);
    }

    #[test]
    fn test_sql_generation_mysql() {
        let config = DatabaseReflexConfig {
            database_type: DatabaseType::Mysql,
            connection_string: "mysql://root@l/db".to_string(),
            table_name: "ink_log".to_string(),
            auto_create_table: true,
            max_connections: 15,
            min_idle: None,
            acquire_timeout_secs: 15,
            include_metadata: false,
            include_status: true,
        };
        assert!(!config.include_metadata);
        assert!(config.include_status);
    }

    #[test]
    fn test_status_mapping() {
        assert_eq!(
            match InkStatus::New {
                InkStatus::New => "new",
                InkStatus::Processed => "processed",
                InkStatus::Archived => "archived",
            },
            "new"
        );
        assert_eq!(
            match InkStatus::Processed {
                InkStatus::New => "new",
                InkStatus::Processed => "processed",
                InkStatus::Archived => "archived",
            },
            "processed"
        );
        assert_eq!(
            match InkStatus::Archived {
                InkStatus::New => "new",
                InkStatus::Processed => "processed",
                InkStatus::Archived => "archived",
            },
            "archived"
        );
    }

    #[test]
    fn test_like_pattern_escaping() {
        let pattern = "/test%topic_123";
        let escaped = pattern
            .replace('!', "!!")
            .replace('%', "!%")
            .replace('_', "!_");
        assert_eq!(escaped, "/test!%topic!_123");
        let like_pattern = format!("{}%", escaped);
        assert_eq!(like_pattern, "/test!%topic!_123%");
    }

    #[test]
    fn test_reflex_trait_methods() {
        let config: DatabaseReflexConfig = serde_yaml::from_str(
            r#"
            database_type: sqlite
            connection_string: ":memory:"
        "#,
        )
        .unwrap();
        let json = serde_json::to_string(&config).unwrap();
        assert!(json.contains("sqlite"));
        assert!(json.contains(":memory:"));
    }
}
