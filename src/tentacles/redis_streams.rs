use anyhow::Result;
use async_trait::async_trait;
use redis::{AsyncCommands, cmd};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use tokio::time;
use tracing::{debug, error, info, warn};

use super::Tentacle;
use crate::eventbus::EventBus;
use crate::model::Ink;

/// Redis-Modus für den Tentacle
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "lowercase")]
pub enum RedisMode {
    /// Redis Streams mit XREADGROUP
    #[default]
    Stream,
    /// Redis Lists mit BRPOP
    List,
}

/// Topic-Erkennungsmodus für Redis-Nachrichten
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "lowercase")]
pub enum RedisTopicMode {
    /// Topic aus dem Stream/Listen-Namen
    #[default]
    Name,
    /// Topic aus einem JSON-Feld extrahieren
    Field,
    /// Alle Nachrichten an ein festes Topic
    All,
}

/// Authentifizierung für Redis
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum RedisAuth {
    /// Keine Authentifizierung
    #[default]
    None,
    /// Passwort-basiert
    Password { password: String },
    /// URL-basiert (redis://user:pass@host:port/db)
    Url { url: String },
}

/// Konfiguration für den Redis Streams Tentacle
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RedisTentacleConfig {
    /// Redis Server-URL (z.B. "redis://localhost:6379")
    #[serde(default = "default_server")]
    pub server: String,
    /// Redis-Streams oder -Listen zum Konsumieren
    pub streams: Vec<String>,
    /// Redis-Modus
    #[serde(default)]
    pub mode: RedisMode,
    /// Topic-Erkennungsmodus
    #[serde(default)]
    pub topic_mode: RedisTopicMode,
    /// JSON-Feld für Topic-Extraktion (bei field-Modus)
    #[serde(default)]
    pub topic_field: Option<String>,
    /// Fallback-Topic
    #[serde(default = "default_fallback_topic")]
    pub fallback_topic: String,
    /// Consumer Group Name (für Streams)
    #[serde(default = "default_consumer_group")]
    pub consumer_group: String,
    /// Consumer Name (für Streams)
    #[serde(default = "default_consumer_name")]
    pub consumer_name: String,
    /// Authentifizierung
    #[serde(default)]
    pub auth: RedisAuth,
    /// Block-Timeout in Millisekunden (für XREAD/BRPOP)
    #[serde(default = "default_block_timeout_ms")]
    pub block_timeout_ms: u64,
    /// Batch-Größe für XREAD
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    /// Reconnect-Verzögerung in Sekunden
    #[serde(default = "default_reconnect_delay")]
    pub reconnect_delay_seconds: u64,
}

fn default_server() -> String {
    "redis://localhost:6379".to_string()
}

fn default_fallback_topic() -> String {
    "/redis/default".to_string()
}

fn default_consumer_group() -> String {
    "kraken".to_string()
}

fn default_consumer_name() -> String {
    "kraken_consumer".to_string()
}

fn default_block_timeout_ms() -> u64 {
    5000
}

fn default_batch_size() -> usize {
    10
}

fn default_reconnect_delay() -> u64 {
    5
}

pub struct RedisTentacle {
    name: String,
    config: RedisTentacleConfig,
}

impl RedisTentacle {
    pub fn new(name: String, config: RedisTentacleConfig) -> Self {
        Self { name, config }
    }

    /// Bestimmt das Topic basierend auf der Redis-Nachricht
    fn resolve_topic(&self, stream_name: &str, payload: &str) -> String {
        match self.config.topic_mode {
            RedisTopicMode::Name => {
                format!("/redis/{}", stream_name.replace('.', "/"))
            }
            RedisTopicMode::Field => {
                if let Ok(json) = serde_json::from_str::<serde_json::Value>(payload)
                    && let Some(field) = &self.config.topic_field
                    && let Some(topic_val) = json.get(field)
                    && let Some(topic_str) = topic_val.as_str()
                    && !topic_str.is_empty()
                {
                    return topic_str.to_string();
                }
                self.config.fallback_topic.clone()
            }
            RedisTopicMode::All => self.config.fallback_topic.clone(),
        }
    }

    /// Erstellt eine Ink-Nachricht aus einer Redis-Nachricht
    fn create_ink(&self, stream_name: &str, message_id: &str, payload: &str) -> Ink {
        let topic = self.resolve_topic(stream_name, payload);

        let mut metadata = serde_json::Map::new();
        metadata.insert("redis_stream".to_string(), serde_json::json!(stream_name));
        metadata.insert(
            "redis_message_id".to_string(),
            serde_json::json!(message_id),
        );

        Ink::new(topic, "redis", &self.name, payload.to_string())
            .with_metadata(serde_json::Value::Object(metadata))
    }

    /// Verbindet mit Redis
    async fn connect(&self) -> Result<redis::Client> {
        let client = redis::Client::open(self.config.server.as_str())?;
        Ok(client)
    }

    /// Redis Streams Modus
    async fn run_streams(&self, event_bus: Arc<EventBus>) -> Result<()> {
        info!(
            "Redis Tentacle '{}' starting in stream mode with {} stream(s)",
            self.name,
            self.config.streams.len()
        );

        loop {
            let client = match self.connect().await {
                Ok(c) => c,
                Err(e) => {
                    error!(
                        "Redis Tentacle '{}' failed to connect: {}. Retrying...",
                        self.name, e
                    );
                    time::sleep(Duration::from_secs(self.config.reconnect_delay_seconds)).await;
                    continue;
                }
            };

            let mut conn = match client.get_multiplexed_async_connection().await {
                Ok(c) => c,
                Err(e) => {
                    error!(
                        "Redis Tentacle '{}' failed to get connection: {}. Retrying...",
                        self.name, e
                    );
                    time::sleep(Duration::from_secs(self.config.reconnect_delay_seconds)).await;
                    continue;
                }
            };

            // Consumer Groups erstellen (XGROUP CREATE IFNOTEXISTS)
            for stream in &self.config.streams {
                let result: Result<(), redis::RedisError> = (cmd("XGROUP")
                    .arg("CREATE")
                    .arg(stream)
                    .arg(&self.config.consumer_group)
                    .arg("0")
                    .arg("MKSTREAM")
                    .query_async(&mut conn))
                .await;

                if let Err(e) = result
                    && !e.to_string().contains("BUSYGROUP")
                {
                    warn!(
                        "Redis Tentacle '{}' could not create consumer group for '{}': {}",
                        self.name, stream, e
                    );
                }
            }

            info!("Redis Tentacle '{}' connected and ready", self.name);

            // XREADGROUP Loop
            loop {
                for stream in &self.config.streams {
                    // XREADGROUP GROUP consumer consumer NAME COUNT block
                    let mut command = cmd("XREADGROUP");
                    command
                        .arg("GROUP")
                        .arg(&self.config.consumer_group)
                        .arg(&self.config.consumer_name)
                        .arg("STREAMS")
                        .arg(stream)
                        .arg(">")
                        .arg("COUNT")
                        .arg(self.config.batch_size)
                        .arg("BLOCK")
                        .arg(self.config.block_timeout_ms);

                    type StreamResult = Result<
                        Vec<(String, Vec<(String, Vec<(String, String)>)>)>,
                        redis::RedisError,
                    >;

                    let result: StreamResult = command.query_async(&mut conn).await;

                    match result {
                        Ok(messages) => {
                            for (_stream_key, entries) in messages {
                                for (msg_id, fields) in entries {
                                    // Payload aus den Feldern extrahieren
                                    let payload = fields
                                        .iter()
                                        .find(|(k, _)| k == "message" || k == "data")
                                        .map(|(_, v)| v.clone())
                                        .unwrap_or_else(|| {
                                            // Alle Felder als JSON
                                            let json_map: serde_json::Map<
                                                String,
                                                serde_json::Value,
                                            > = fields
                                                .iter()
                                                .map(|(k, v)| (k.clone(), serde_json::json!(v)))
                                                .collect();
                                            serde_json::to_string(&json_map).unwrap_or_default()
                                        });

                                    debug!(
                                        "Redis Tentacle '{}' received message from '{}': {}",
                                        self.name, stream, &msg_id
                                    );

                                    let ink = self.create_ink(stream, &msg_id, &payload);
                                    event_bus.publish(ink);
                                }
                            }
                        }
                        Err(e) => {
                            if e.is_connection_dropped() {
                                error!(
                                    "Redis Tentacle '{}' connection dropped. Reconnecting...",
                                    self.name
                                );
                                break;
                            }
                            // Timeout ist normal
                            debug!("Redis XREADGROUP: {}", e);
                        }
                    }
                }
            }
        }
    }

    /// Redis Lists Modus (BRPOP)
    async fn run_lists(&self, event_bus: Arc<EventBus>) -> Result<()> {
        info!(
            "Redis Tentacle '{}' starting in list mode with {} list(s)",
            self.name,
            self.config.streams.len()
        );

        loop {
            let client = match self.connect().await {
                Ok(c) => c,
                Err(e) => {
                    error!(
                        "Redis Tentacle '{}' failed to connect: {}. Retrying...",
                        self.name, e
                    );
                    time::sleep(Duration::from_secs(self.config.reconnect_delay_seconds)).await;
                    continue;
                }
            };

            let mut conn = match client.get_multiplexed_async_connection().await {
                Ok(c) => c,
                Err(e) => {
                    error!(
                        "Redis Tentacle '{}' failed to get connection: {}. Retrying...",
                        self.name, e
                    );
                    time::sleep(Duration::from_secs(self.config.reconnect_delay_seconds)).await;
                    continue;
                }
            };

            info!(
                "Redis Tentacle '{}' connected and ready (list mode)",
                self.name
            );

            // BRPOP Loop
            loop {
                let lists: Vec<&str> = self.config.streams.iter().map(|s| s.as_str()).collect();

                let timeout_secs = (self.config.block_timeout_ms / 1000).max(1);

                let result: Result<Option<(String, String)>, redis::RedisError> =
                    conn.brpop(&lists, timeout_secs as f64).await;

                match result {
                    Ok(Some((list_name, value))) => {
                        debug!(
                            "Redis Tentacle '{}' received message from list '{}'",
                            self.name, list_name
                        );

                        let msg_id = uuid::Uuid::new_v4().to_string();
                        let ink = self.create_ink(&list_name, &msg_id, &value);
                        event_bus.publish(ink);
                    }
                    Ok(None) => {
                        // Timeout, normal
                    }
                    Err(e) => {
                        if e.is_connection_dropped() {
                            error!(
                                "Redis Tentacle '{}' connection dropped. Reconnecting...",
                                self.name
                            );
                            break;
                        }
                        warn!("Redis BRPOP error: {}", e);
                    }
                }
            }
        }
    }
}

#[async_trait]
impl Tentacle for RedisTentacle {
    fn name(&self) -> &str {
        &self.name
    }

    fn r#type(&self) -> &str {
        "redis"
    }

    async fn start(&self, event_bus: Arc<EventBus>) -> Result<()> {
        info!(
            "Redis Tentacle '{}' starting on {} ({:?} mode)",
            self.name, self.config.server, self.config.mode
        );

        match self.config.mode {
            RedisMode::Stream => self.run_streams(event_bus).await,
            RedisMode::List => self.run_lists(event_bus).await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resolve_topic_name_mode() {
        let config = RedisTentacleConfig {
            server: "redis://localhost:6379".to_string(),
            streams: vec!["mystream".to_string()],
            mode: RedisMode::Stream,
            topic_mode: RedisTopicMode::Name,
            topic_field: None,
            fallback_topic: "/redis/default".to_string(),
            consumer_group: "kraken".to_string(),
            consumer_name: "kraken_consumer".to_string(),
            auth: RedisAuth::None,
            block_timeout_ms: 5000,
            batch_size: 10,
            reconnect_delay_seconds: 5,
        };
        let tentacle = RedisTentacle::new("test".to_string(), config);

        let topic = tentacle.resolve_topic("orders.new", "{}");
        assert_eq!(topic, "/redis/orders/new");
    }

    #[test]
    fn test_resolve_topic_field_mode() {
        let config = RedisTentacleConfig {
            server: "redis://localhost:6379".to_string(),
            streams: vec!["mystream".to_string()],
            mode: RedisMode::Stream,
            topic_mode: RedisTopicMode::Field,
            topic_field: Some("event_type".to_string()),
            fallback_topic: "/redis/default".to_string(),
            consumer_group: "kraken".to_string(),
            consumer_name: "kraken_consumer".to_string(),
            auth: RedisAuth::None,
            block_timeout_ms: 5000,
            batch_size: 10,
            reconnect_delay_seconds: 5,
        };
        let tentacle = RedisTentacle::new("test".to_string(), config);

        let payload = r#"{"event_type": "/events/order", "data": "test"}"#;
        let topic = tentacle.resolve_topic("mystream", payload);
        assert_eq!(topic, "/events/order");
    }

    #[test]
    fn test_resolve_topic_all_mode() {
        let config = RedisTentacleConfig {
            server: "redis://localhost:6379".to_string(),
            streams: vec!["mystream".to_string()],
            mode: RedisMode::Stream,
            topic_mode: RedisTopicMode::All,
            topic_field: None,
            fallback_topic: "/redis/all".to_string(),
            consumer_group: "kraken".to_string(),
            consumer_name: "kraken_consumer".to_string(),
            auth: RedisAuth::None,
            block_timeout_ms: 5000,
            batch_size: 10,
            reconnect_delay_seconds: 5,
        };
        let tentacle = RedisTentacle::new("test".to_string(), config);

        let topic = tentacle.resolve_topic("any_stream", "payload");
        assert_eq!(topic, "/redis/all");
    }

    #[test]
    fn test_create_ink_metadata() {
        let config = RedisTentacleConfig {
            server: "redis://localhost:6379".to_string(),
            streams: vec!["mystream".to_string()],
            mode: RedisMode::Stream,
            topic_mode: RedisTopicMode::Name,
            topic_field: None,
            fallback_topic: "/redis/default".to_string(),
            consumer_group: "kraken".to_string(),
            consumer_name: "kraken_consumer".to_string(),
            auth: RedisAuth::None,
            block_timeout_ms: 5000,
            batch_size: 10,
            reconnect_delay_seconds: 5,
        };
        let tentacle = RedisTentacle::new("test".to_string(), config);

        let ink = tentacle.create_ink("orders.new", "12345-0", "Hello Redis");
        assert_eq!(ink.topic, "/redis/orders/new");
        assert_eq!(ink.body, "Hello Redis");

        let meta = ink.metadata.unwrap();
        let meta_obj = meta.as_object().unwrap();
        assert_eq!(
            meta_obj.get("redis_stream").unwrap(),
            &serde_json::json!("orders.new")
        );
        assert_eq!(
            meta_obj.get("redis_message_id").unwrap(),
            &serde_json::json!("12345-0")
        );
    }

    #[test]
    fn test_redis_config_deserialization() {
        let yaml = r#"
        server: "redis://localhost:6379"
        streams:
          - "orders.>"
          - "events"
        mode: "stream"
        topic_mode: "name"
        consumer_group: "kraken"
        consumer_name: "consumer1"
        auth:
          type: "password"
          password: "secret"
        block_timeout_ms: 10000
        "#;

        let config: RedisTentacleConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.server, "redis://localhost:6379");
        assert_eq!(config.streams.len(), 2);
        assert!(matches!(config.mode, RedisMode::Stream));
        assert!(matches!(config.auth, RedisAuth::Password { .. }));
        assert_eq!(config.block_timeout_ms, 10000);
    }

    #[test]
    fn test_redis_tentacle_name_and_type() {
        let config = RedisTentacleConfig {
            server: "redis://localhost:6379".to_string(),
            streams: vec!["mystream".to_string()],
            mode: RedisMode::Stream,
            topic_mode: RedisTopicMode::Name,
            topic_field: None,
            fallback_topic: "/redis/default".to_string(),
            consumer_group: "kraken".to_string(),
            consumer_name: "kraken_consumer".to_string(),
            auth: RedisAuth::None,
            block_timeout_ms: 5000,
            batch_size: 10,
            reconnect_delay_seconds: 5,
        };
        let tentacle = RedisTentacle::new("test_redis".to_string(), config);
        assert_eq!(tentacle.name(), "test_redis");
        assert_eq!(tentacle.r#type(), "redis");
    }

    #[test]
    fn test_redis_mode_default() {
        let mode: RedisMode = serde_yaml::from_str("stream").unwrap();
        assert!(matches!(mode, RedisMode::Stream));

        let mode: RedisMode = serde_yaml::from_str("list").unwrap();
        assert!(matches!(mode, RedisMode::List));
    }

    #[test]
    fn test_redis_auth_deserialization() {
        let yaml = r#"
        type: "password"
        password: "mysecret"
        "#;
        let auth: RedisAuth = serde_yaml::from_str(yaml).unwrap();
        assert!(matches!(auth, RedisAuth::Password { .. }));

        let yaml = r#"type: "none""#;
        let auth: RedisAuth = serde_yaml::from_str(yaml).unwrap();
        assert!(matches!(auth, RedisAuth::None));
    }
}
