use anyhow::Result;
use async_trait::async_trait;
use futures_util::StreamExt;
use rdkafka::config::ClientConfig;
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::message::{Headers, Message};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, error, info};

use super::Tentacle;
use crate::eventbus::EventBus;
use crate::model::Ink;

/// Topic-Erkennungsmodus für Kafka-Nachrichten
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "lowercase")]
pub enum KafkaTopicMode {
    /// Topic aus dem Kafka-Topic verwenden
    #[default]
    Topic,
    /// Topic aus einem JSON-Feld extrahieren
    Field,
    /// Alle Nachrichten an ein festes Topic
    All,
}

/// Authentifizierungstyp für Kafka
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum KafkaAuth {
    /// Keine Authentifizierung
    #[default]
    None,
    /// SASL Plain (Username/Password)
    SaslPlain { username: String, password: String },
    /// SASL SCRAM-256
    Scram256 { username: String, password: String },
    /// SASL SCRAM-512
    Scram512 { username: String, password: String },
}

/// Konfiguration für den Kafka Tentacle
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaTentacleConfig {
    /// Kafka Broker-URLs (kommagetrennt, z.B. "localhost:9092,localhost:9093")
    #[serde(default = "default_brokers")]
    pub brokers: String,
    /// Kafka-Topics zum Abonnieren
    pub topics: Vec<String>,
    /// Consumer Group ID
    #[serde(default = "default_group_id")]
    pub group_id: String,
    /// Topic-Erkennungsmodus
    #[serde(default)]
    pub topic_mode: KafkaTopicMode,
    /// JSON-Feld für Topic-Extraktion (bei field-Modus)
    #[serde(default)]
    pub topic_field: Option<String>,
    /// Fallback-Topic
    #[serde(default = "default_fallback_topic")]
    pub fallback_topic: String,
    /// Optional: Kafka-Topic-basiertes Topic-Mapping
    #[serde(default)]
    pub topic_mapping: HashMap<String, String>,
    /// Authentifizierung
    #[serde(default)]
    pub auth: KafkaAuth,
    /// Optional: SSL/TLS aktivieren
    #[serde(default)]
    pub ssl_enabled: bool,
    /// Optional: SSL CA-Zertifikat Pfad
    #[serde(default)]
    pub ssl_ca_location: Option<String>,
    /// Auto Offset Reset (earliest oder latest)
    #[serde(default = "default_auto_offset_reset")]
    pub auto_offset_reset: String,
    /// Session Timeout in Millisekunden
    #[serde(default = "default_session_timeout")]
    pub session_timeout_ms: i32,
    /// Heartbeat Interval in Millisekunden
    #[serde(default = "default_heartbeat_interval")]
    pub heartbeat_interval_ms: i32,
    /// Max Poll Interval in Millisekunden
    #[serde(default = "default_max_poll_interval")]
    pub max_poll_interval_ms: i32,
}

fn default_brokers() -> String {
    "localhost:9092".to_string()
}

fn default_group_id() -> String {
    "rustykraken".to_string()
}

fn default_fallback_topic() -> String {
    "/kafka/default".to_string()
}

fn default_auto_offset_reset() -> String {
    "latest".to_string()
}

fn default_session_timeout() -> i32 {
    10000
}

fn default_heartbeat_interval() -> i32 {
    3000
}

fn default_max_poll_interval() -> i32 {
    300000
}

pub struct KafkaTentacle {
    name: String,
    config: KafkaTentacleConfig,
}

/// Freistehende Funktion: Bestimmt das Topic basierend auf der Kafka-Nachricht
fn resolve_topic(config: &KafkaTentacleConfig, kafka_topic: &str, payload: &[u8]) -> String {
    // Prüfe topic_mapping
    if let Some(mapped) = config.topic_mapping.get(kafka_topic) {
        return mapped.clone();
    }

    match config.topic_mode {
        KafkaTopicMode::Topic => {
            format!("/kafka/{}", kafka_topic)
        }
        KafkaTopicMode::Field => {
            // Versuche JSON-Feld zu extrahieren
            let extracted = serde_json::from_slice::<serde_json::Value>(payload)
                .ok()
                .zip(config.topic_field.as_deref())
                .and_then(|(v, f)| v.get(f).and_then(|val| val.as_str().map(|s| s.to_string())))
                .filter(|s| !s.is_empty());

            if let Some(extracted) = extracted {
                return if extracted.starts_with('/') {
                    extracted
                } else {
                    format!("/kafka/{}", extracted)
                };
            }
            // Fallback auf Kafka-Topic
            format!("/kafka/{}", kafka_topic)
        }
        KafkaTopicMode::All => config.fallback_topic.clone(),
    }
}

/// Freistehende Funktion: Extrahiert den Body aus der Nachricht
fn extract_body(payload: &[u8]) -> String {
    // Versuche UTF-8-String
    if let Ok(text) = std::str::from_utf8(payload) {
        return text.to_string();
    }
    // Fallback: Base64
    base64::Engine::encode(&base64::engine::general_purpose::STANDARD, payload)
}

impl KafkaTentacle {
    pub fn new(name: String, config: KafkaTentacleConfig) -> Self {
        Self { name, config }
    }

    /// Erstellt eine ClientConfig basierend auf der Konfiguration
    pub fn create_client_config(&self) -> ClientConfig {
        let mut client_config = ClientConfig::new();
        client_config
            .set("bootstrap.servers", &self.config.brokers)
            .set("group.id", &self.config.group_id)
            .set("auto.offset.reset", &self.config.auto_offset_reset)
            .set("enable.auto.commit", "true")
            .set(
                "session.timeout.ms",
                self.config.session_timeout_ms.to_string(),
            )
            .set(
                "heartbeat.interval.ms",
                self.config.heartbeat_interval_ms.to_string(),
            )
            .set(
                "max.poll.interval.ms",
                self.config.max_poll_interval_ms.to_string(),
            );

        // SSL-Konfiguration
        if self.config.ssl_enabled {
            client_config.set("security.protocol", "ssl");
            if let Some(ca_path) = &self.config.ssl_ca_location {
                client_config.set("ssl.ca.location", ca_path);
            }
        }

        // SASL-Konfiguration
        match &self.config.auth {
            KafkaAuth::None => {}
            KafkaAuth::SaslPlain { username, password } => {
                client_config.set("security.protocol", "sasl_plaintext");
                client_config.set("sasl.mechanisms", "PLAIN");
                client_config.set("sasl.username", username);
                client_config.set("sasl.password", password);
            }
            KafkaAuth::Scram256 { username, password } => {
                client_config.set("security.protocol", "sasl_plaintext");
                client_config.set("sasl.mechanisms", "SCRAM-SHA-256");
                client_config.set("sasl.username", username);
                client_config.set("sasl.password", password);
            }
            KafkaAuth::Scram512 { username, password } => {
                client_config.set("security.protocol", "sasl_plaintext");
                client_config.set("sasl.mechanisms", "SCRAM-SHA-512");
                client_config.set("sasl.username", username);
                client_config.set("sasl.password", password);
            }
        }

        client_config
    }
}

#[async_trait]
impl Tentacle for KafkaTentacle {
    fn name(&self) -> &str {
        &self.name
    }

    fn r#type(&self) -> &str {
        "kafka"
    }

    async fn start(&self, event_bus: Arc<EventBus>) -> Result<()> {
        info!(
            "Kafka Tentacle '{}' starting, connecting to {}",
            self.name, self.config.brokers
        );

        let client_config = self.create_client_config();
        let topics_str = self.config.topics.join(",");

        // Erstelle StreamConsumer
        let consumer: StreamConsumer = client_config
            .create()
            .map_err(|e| anyhow::anyhow!("Failed to create Kafka consumer: {}", e))?;

        // Abonnieren der Topics
        consumer
            .subscribe(&[&topics_str])
            .map_err(|e| anyhow::anyhow!("Failed to subscribe to topics: {}", e))?;

        info!(
            "Kafka Tentacle '{}' subscribed to topics: {:?}",
            self.name, self.config.topics
        );

        // Nachrichtenschleife mit tokio Stream
        let config = self.config.clone();
        let tentacle_name = self.name.clone();
        let event_bus_clone = Arc::clone(&event_bus);

        tokio::spawn(async move {
            let mut stream = consumer.stream();
            while let Some(message) = stream.next().await {
                match message {
                    Ok(native_message) => {
                        let kafka_topic = native_message.topic().to_string();
                        let partition = native_message.partition();
                        let offset = native_message.offset();

                        let payload = native_message.payload().unwrap_or(&[]);
                        let topic = resolve_topic(&config, &kafka_topic, payload);
                        let body = extract_body(payload);

                        debug!(
                            "Kafka Tentacle '{}': received message on topic '{}' partition {} offset {}",
                            tentacle_name, kafka_topic, partition, offset
                        );

                        // Metadaten erstellen
                        let mut metadata = serde_json::Map::new();
                        metadata.insert(
                            "kafka_topic".to_string(),
                            serde_json::Value::String(kafka_topic.clone()),
                        );
                        metadata.insert(
                            "kafka_partition".to_string(),
                            serde_json::Value::Number(serde_json::Number::from(partition as u64)),
                        );
                        metadata.insert(
                            "kafka_offset".to_string(),
                            serde_json::Value::Number(serde_json::Number::from(offset as u64)),
                        );

                        // Header als Metadaten hinzufügen
                        if let Some(headers) = native_message.headers() {
                            for idx in 0..headers.count() {
                                let header = headers.try_get(idx);
                                if let Some(h) = header {
                                    let value = std::str::from_utf8(h.value.unwrap_or(&[]));
                                    if let Ok(v) = value {
                                        metadata.insert(
                                            format!("kafka_header_{}", h.key),
                                            serde_json::Value::String(v.to_string()),
                                        );
                                    }
                                }
                            }
                        }

                        let mut ink = Ink::new(&topic, "kafka", &tentacle_name, body);
                        ink = ink.with_metadata(serde_json::Value::Object(metadata));

                        info!(
                            "Kafka Tentacle '{}': published ink on topic: {}",
                            tentacle_name, topic
                        );

                        event_bus_clone.publish(ink);
                    }
                    Err(e) => {
                        error!(
                            "Kafka Tentacle '{}': error receiving message: {}",
                            tentacle_name, e
                        );
                    }
                }
            }
        });

        // Blockiere den Task, damit er läuft
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(60)).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_kafka_topic_mode_topic() {
        let config = KafkaTentacleConfig {
            brokers: "localhost:9092".to_string(),
            topics: vec!["test".to_string()],
            group_id: "test-group".to_string(),
            topic_mode: KafkaTopicMode::Topic,
            topic_field: None,
            fallback_topic: "/kafka/default".to_string(),
            topic_mapping: HashMap::new(),
            auth: KafkaAuth::None,
            ssl_enabled: false,
            ssl_ca_location: None,
            auto_offset_reset: "latest".to_string(),
            session_timeout_ms: 10000,
            heartbeat_interval_ms: 3000,
            max_poll_interval_ms: 300000,
        };

        assert_eq!(
            resolve_topic(&config, "sensor/data", b"payload"),
            "/kafka/sensor/data"
        );
    }

    #[test]
    fn test_kafka_topic_mode_field() {
        let config = KafkaTentacleConfig {
            brokers: "localhost:9092".to_string(),
            topics: vec!["test".to_string()],
            group_id: "test-group".to_string(),
            topic_mode: KafkaTopicMode::Field,
            topic_field: Some("kraken_topic".to_string()),
            fallback_topic: "/kafka/default".to_string(),
            topic_mapping: HashMap::new(),
            auth: KafkaAuth::None,
            ssl_enabled: false,
            ssl_ca_location: None,
            auto_offset_reset: "latest".to_string(),
            session_timeout_ms: 10000,
            heartbeat_interval_ms: 3000,
            max_poll_interval_ms: 300000,
        };

        // JSON mit kraken_topic-Feld
        let payload = r#"{"kraken_topic": "/sensor/temp", "value": 42}"#;
        assert_eq!(
            resolve_topic(&config, "test", payload.as_bytes()),
            "/sensor/temp"
        );

        // JSON ohne kraken_topic-Feld → Fallback auf Kafka-Topic
        let payload = r#"{"value": 42}"#;
        assert_eq!(
            resolve_topic(&config, "test", payload.as_bytes()),
            "/kafka/test"
        );
    }

    #[test]
    fn test_kafka_topic_mode_all() {
        let config = KafkaTentacleConfig {
            brokers: "localhost:9092".to_string(),
            topics: vec!["test".to_string()],
            group_id: "test-group".to_string(),
            topic_mode: KafkaTopicMode::All,
            topic_field: None,
            fallback_topic: "/kafka/all".to_string(),
            topic_mapping: HashMap::new(),
            auth: KafkaAuth::None,
            ssl_enabled: false,
            ssl_ca_location: None,
            auto_offset_reset: "latest".to_string(),
            session_timeout_ms: 10000,
            heartbeat_interval_ms: 3000,
            max_poll_interval_ms: 300000,
        };

        assert_eq!(
            resolve_topic(&config, "any/topic", b"payload"),
            "/kafka/all"
        );
    }

    #[test]
    fn test_kafka_topic_mapping() {
        let mut mapping = HashMap::new();
        mapping.insert(
            "sensor/temperature".to_string(),
            "/sensors/temp".to_string(),
        );
        mapping.insert(
            "sensor/humidity".to_string(),
            "/sensors/humidity".to_string(),
        );

        let config = KafkaTentacleConfig {
            brokers: "localhost:9092".to_string(),
            topics: vec!["sensor/+".to_string()],
            group_id: "test-group".to_string(),
            topic_mode: KafkaTopicMode::Topic,
            topic_field: None,
            fallback_topic: "/kafka/default".to_string(),
            topic_mapping: mapping,
            auth: KafkaAuth::None,
            ssl_enabled: false,
            ssl_ca_location: None,
            auto_offset_reset: "latest".to_string(),
            session_timeout_ms: 10000,
            heartbeat_interval_ms: 3000,
            max_poll_interval_ms: 300000,
        };

        assert_eq!(
            resolve_topic(&config, "sensor/temperature", b"payload"),
            "/sensors/temp"
        );
        assert_eq!(
            resolve_topic(&config, "sensor/humidity", b"payload"),
            "/sensors/humidity"
        );
        // Kein Mapping → Standard-Verhalten
        assert_eq!(
            resolve_topic(&config, "sensor/pressure", b"payload"),
            "/kafka/sensor/pressure"
        );
    }

    #[test]
    fn test_kafka_extract_body_utf8() {
        assert_eq!(extract_body(b"Hello World"), "Hello World");
    }

    #[test]
    fn test_kafka_extract_body_binary() {
        // Binäre Daten (nicht gültiges UTF-8)
        let binary = vec![0xff, 0xfe, 0xfd];
        let body = extract_body(&binary);
        // Sollte Base64-codiert sein
        assert!(!body.is_empty());
        assert_eq!(
            body,
            base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &binary)
        );
    }

    #[test]
    fn test_kafka_config_deserialization() {
        let yaml = r#"
        brokers: "kafka1:9092,kafka2:9092"
        topics:
          - "sensor/temperature"
          - "sensor/humidity"
        group_id: "rustykraken-sensors"
        topic_mode: field
        topic_field: "kraken_topic"
        fallback_topic: "/kafka/sensors"
        topic_mapping:
          "sensor/temperature": "/sensors/temp"
          "sensor/humidity": "/sensors/humidity"
        auth:
          type: saslplain
          username: "kraken"
          password: "secret"
        ssl_enabled: true
        auto_offset_reset: "earliest"
        session_timeout_ms: 15000
        "#;

        let config: KafkaTentacleConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.brokers, "kafka1:9092,kafka2:9092");
        assert_eq!(config.topics.len(), 2);
        assert_eq!(config.group_id, "rustykraken-sensors");
        assert_eq!(config.topic_mode, KafkaTopicMode::Field);
        assert_eq!(config.topic_field.as_deref(), Some("kraken_topic"));
        assert!(config.ssl_enabled);
        assert_eq!(config.auto_offset_reset, "earliest");
        assert_eq!(config.session_timeout_ms, 15000);
    }

    #[test]
    fn test_kafka_tentacle_name_and_type() {
        let config = KafkaTentacleConfig {
            brokers: "localhost:9092".to_string(),
            topics: vec!["test".to_string()],
            group_id: "test-group".to_string(),
            topic_mode: KafkaTopicMode::Topic,
            topic_field: None,
            fallback_topic: "/kafka/default".to_string(),
            topic_mapping: HashMap::new(),
            auth: KafkaAuth::None,
            ssl_enabled: false,
            ssl_ca_location: None,
            auto_offset_reset: "latest".to_string(),
            session_timeout_ms: 10000,
            heartbeat_interval_ms: 3000,
            max_poll_interval_ms: 300000,
        };
        let tentacle = KafkaTentacle::new("test_kafka".to_string(), config);
        assert_eq!(tentacle.name(), "test_kafka");
        assert_eq!(tentacle.r#type(), "kafka");
    }

    #[test]
    fn test_kafka_auth_variants() {
        let yaml_plain = r#"
        brokers: "localhost:9092"
        topics: ["test"]
        auth:
          type: saslplain
          username: "user"
          password: "pass"
        "#;
        let config: KafkaTentacleConfig = serde_yaml::from_str(yaml_plain).unwrap();
        assert!(matches!(config.auth, KafkaAuth::SaslPlain { .. }));

        let yaml_scram256 = r#"
        brokers: "localhost:9092"
        topics: ["test"]
        auth:
          type: scram256
          username: "user"
          password: "pass"
        "#;
        let config: KafkaTentacleConfig = serde_yaml::from_str(yaml_scram256).unwrap();
        assert!(matches!(config.auth, KafkaAuth::Scram256 { .. }));

        let yaml_scram512 = r#"
        brokers: "localhost:9092"
        topics: ["test"]
        auth:
          type: scram512
          username: "user"
          password: "pass"
        "#;
        let config: KafkaTentacleConfig = serde_yaml::from_str(yaml_scram512).unwrap();
        assert!(matches!(config.auth, KafkaAuth::Scram512 { .. }));
    }

    #[test]
    fn test_kafka_client_config_ssl() {
        let config = KafkaTentacleConfig {
            brokers: "localhost:9092".to_string(),
            topics: vec!["test".to_string()],
            group_id: "test-group".to_string(),
            topic_mode: KafkaTopicMode::Topic,
            topic_field: None,
            fallback_topic: "/kafka/default".to_string(),
            topic_mapping: HashMap::new(),
            auth: KafkaAuth::None,
            ssl_enabled: true,
            ssl_ca_location: Some("/etc/ssl/ca.pem".to_string()),
            auto_offset_reset: "latest".to_string(),
            session_timeout_ms: 10000,
            heartbeat_interval_ms: 3000,
            max_poll_interval_ms: 300000,
        };
        let tentacle = KafkaTentacle::new("test".to_string(), config);
        let client_config = tentacle.create_client_config();
        assert_eq!(client_config.get("security.protocol"), Some("ssl"));
        assert_eq!(
            client_config.get("ssl.ca.location"),
            Some("/etc/ssl/ca.pem")
        );
    }

    #[test]
    fn test_kafka_client_config_sasl() {
        let config = KafkaTentacleConfig {
            brokers: "localhost:9092".to_string(),
            topics: vec!["test".to_string()],
            group_id: "test-group".to_string(),
            topic_mode: KafkaTopicMode::Topic,
            topic_field: None,
            fallback_topic: "/kafka/default".to_string(),
            topic_mapping: HashMap::new(),
            auth: KafkaAuth::SaslPlain {
                username: "kraken".to_string(),
                password: "secret".to_string(),
            },
            ssl_enabled: false,
            ssl_ca_location: None,
            auto_offset_reset: "latest".to_string(),
            session_timeout_ms: 10000,
            heartbeat_interval_ms: 3000,
            max_poll_interval_ms: 300000,
        };
        let tentacle = KafkaTentacle::new("test".to_string(), config);
        let client_config = tentacle.create_client_config();
        assert_eq!(
            client_config.get("security.protocol"),
            Some("sasl_plaintext")
        );
        assert_eq!(client_config.get("sasl.mechanisms"), Some("PLAIN"));
        assert_eq!(client_config.get("sasl.username"), Some("kraken"));
    }

    #[test]
    fn test_kafka_field_mode_with_slash_prefix() {
        let config = KafkaTentacleConfig {
            brokers: "localhost:9092".to_string(),
            topics: vec!["test".to_string()],
            group_id: "test-group".to_string(),
            topic_mode: KafkaTopicMode::Field,
            topic_field: Some("topic".to_string()),
            fallback_topic: "/kafka/default".to_string(),
            topic_mapping: HashMap::new(),
            auth: KafkaAuth::None,
            ssl_enabled: false,
            ssl_ca_location: None,
            auto_offset_reset: "latest".to_string(),
            session_timeout_ms: 10000,
            heartbeat_interval_ms: 3000,
            max_poll_interval_ms: 300000,
        };

        // Feld mit /-Präfix → direkt verwenden
        let payload = r#"{"topic": "/custom/path"}"#;
        assert_eq!(
            resolve_topic(&config, "test", payload.as_bytes()),
            "/custom/path"
        );

        // Feld ohne /-Präfix → /kafka/ voranstellen
        let payload = r#"{"topic": "custom/path"}"#;
        assert_eq!(
            resolve_topic(&config, "test", payload.as_bytes()),
            "/kafka/custom/path"
        );
    }
}
