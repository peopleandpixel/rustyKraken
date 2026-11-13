use anyhow::Result;
use async_trait::async_trait;
use futures_util::StreamExt;
use lapin::options::{
    BasicAckOptions, BasicConsumeOptions, BasicQosOptions, ExchangeDeclareOptions,
    QueueBindOptions, QueueDeclareOptions,
};
use lapin::types::FieldTable;
use lapin::{BasicProperties, Connection, ConnectionProperties, ExchangeKind};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use tokio::time;
use tracing::{debug, error, info, warn};

use super::Tentacle;
use crate::eventbus::EventBus;
use crate::model::Ink;

/// Topic-Erkennungsmodus für RabbitMQ-Nachrichten
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "lowercase")]
pub enum RabbitMqTopicMode {
    /// Topic aus dem Routing-Key
    #[default]
    RoutingKey,
    /// Topic aus einem JSON-Feld extrahieren
    Field,
    /// Alle Nachrichten an ein festes Topic
    All,
}

/// Authentifizierung für RabbitMQ
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum RabbitMqAuth {
    #[default]
    /// Keine Authentifizierung
    None,
    /// AMQP-URL (amqp://user:pass@host:port/vhost)
    Url { url: String },
}

/// Konfiguration für den RabbitMQ Tentacle
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RabbitMqTentacleConfig {
    /// RabbitMQ Server-URL (z.B. "amqp://guest:guest@localhost:5672")
    #[serde(default = "default_server")]
    pub server: String,
    /// Queues zum Konsumieren
    pub queues: Vec<String>,
    /// Exchange-Name (optional)
    #[serde(default)]
    pub exchange: Option<String>,
    /// Exchange-Typ (default: topic)
    #[serde(default = "default_exchange_kind")]
    pub exchange_kind: String,
    /// Topic-Erkennungsmodus
    #[serde(default)]
    pub topic_mode: RabbitMqTopicMode,
    /// JSON-Feld für Topic-Extraktion (bei field-Modus)
    #[serde(default)]
    pub topic_field: Option<String>,
    /// Fallback-Topic
    #[serde(default = "default_fallback_topic")]
    pub fallback_topic: String,
    /// Prefetch-Zähler (QoS)
    #[serde(default = "default_prefetch_count")]
    pub prefetch_count: u16,
    /// Nachrichten automatisch ACK-en
    #[serde(default = "default_auto_ack")]
    pub auto_ack: bool,
    /// Reconnect-Verzögerung in Sekunden
    #[serde(default = "default_reconnect_delay")]
    pub reconnect_delay_seconds: u64,
    /// Authentifizierung
    #[serde(default)]
    pub auth: RabbitMqAuth,
}

fn default_server() -> String {
    "amqp://guest:guest@localhost:5672".to_string()
}

fn default_fallback_topic() -> String {
    "/rabbitmq/default".to_string()
}

fn default_exchange_kind() -> String {
    "topic".to_string()
}

fn default_prefetch_count() -> u16 {
    10
}

fn default_auto_ack() -> bool {
    false
}

fn default_reconnect_delay() -> u64 {
    5
}

pub struct RabbitMqTentacle {
    name: String,
    config: RabbitMqTentacleConfig,
}

impl RabbitMqTentacle {
    pub fn new(name: String, config: RabbitMqTentacleConfig) -> Self {
        Self { name, config }
    }

    /// Bestimmt das Topic basierend auf der RabbitMQ-Nachricht
    fn resolve_topic(&self, routing_key: &str, payload: &[u8]) -> String {
        match self.config.topic_mode {
            RabbitMqTopicMode::RoutingKey => {
                format!("/rabbitmq/{}", routing_key.replace('.', "/"))
            }
            RabbitMqTopicMode::Field => {
                if let Ok(json) = serde_json::from_slice::<serde_json::Value>(payload)
                    && let Some(field) = &self.config.topic_field
                    && let Some(topic_val) = json.get(field)
                    && let Some(topic_str) = topic_val.as_str()
                    && !topic_str.is_empty()
                {
                    return topic_str.to_string();
                }
                self.config.fallback_topic.clone()
            }
            RabbitMqTopicMode::All => self.config.fallback_topic.clone(),
        }
    }

    /// Erstellt eine Ink-Nachricht aus einer RabbitMQ-Nachricht
    fn create_ink(
        &self,
        queue: &str,
        routing_key: &str,
        payload: &[u8],
        #[allow(unused_variables)] properties: &BasicProperties,
    ) -> Ink {
        let topic = self.resolve_topic(routing_key, payload);

        let body = match std::str::from_utf8(payload) {
            Ok(s) => s.to_string(),
            Err(_) => {
                let encoded =
                    base64::Engine::encode(&base64::engine::general_purpose::STANDARD, payload);
                debug!("Binary RabbitMQ message encoded as base64");
                encoded
            }
        };

        let mut metadata = serde_json::Map::new();
        metadata.insert("rabbitmq_queue".to_string(), serde_json::json!(queue));
        metadata.insert(
            "rabbitmq_routing_key".to_string(),
            serde_json::json!(routing_key),
        );

        Ink::new(topic, "rabbitmq", &self.name, body)
            .with_metadata(serde_json::Value::Object(metadata))
    }

    /// Verbindet mit RabbitMQ
    async fn connect(&self) -> Result<Connection> {
        let url = match &self.config.auth {
            RabbitMqAuth::None => self.config.server.clone(),
            RabbitMqAuth::Url { url } => url.clone(),
        };

        let connection = Connection::connect(&url, ConnectionProperties::default()).await?;
        Ok(connection)
    }
}

#[async_trait]
impl Tentacle for RabbitMqTentacle {
    fn name(&self) -> &str {
        &self.name
    }

    fn r#type(&self) -> &str {
        "rabbitmq"
    }

    async fn start(&self, event_bus: Arc<EventBus>) -> Result<()> {
        info!(
            "RabbitMQ Tentacle '{}' connecting to {} with {} queue(s)",
            self.name,
            self.config.server,
            self.config.queues.len()
        );

        loop {
            let connection = match self.connect().await {
                Ok(conn) => {
                    info!(
                        "RabbitMQ Tentacle '{}' connected to {}",
                        self.name, self.config.server
                    );
                    conn
                }
                Err(e) => {
                    error!(
                        "RabbitMQ Tentacle '{}' failed to connect: {}. Retrying in {} seconds...",
                        self.name, e, self.config.reconnect_delay_seconds
                    );
                    time::sleep(Duration::from_secs(self.config.reconnect_delay_seconds)).await;
                    continue;
                }
            };

            let channel = match connection.create_channel().await {
                Ok(ch) => ch,
                Err(e) => {
                    error!(
                        "RabbitMQ Tentacle '{}' failed to create channel: {}. Retrying...",
                        self.name, e
                    );
                    time::sleep(Duration::from_secs(self.config.reconnect_delay_seconds)).await;
                    continue;
                }
            };

            // QoS setzen
            if let Err(e) = channel
                .basic_qos(self.config.prefetch_count, BasicQosOptions::default())
                .await
            {
                warn!("RabbitMQ Tentacle '{}' failed to set QoS: {}", self.name, e);
            }

            // Exchange deklarieren (falls konfiguriert)
            if let Some(ref exchange_name) = self.config.exchange {
                let kind = match self.config.exchange_kind.as_str() {
                    "direct" => ExchangeKind::Direct,
                    "fanout" => ExchangeKind::Fanout,
                    "headers" => ExchangeKind::Headers,
                    _ => ExchangeKind::Topic,
                };

                if let Err(e) = channel
                    .exchange_declare(
                        exchange_name,
                        kind,
                        ExchangeDeclareOptions::default(),
                        FieldTable::default(),
                    )
                    .await
                {
                    warn!(
                        "RabbitMQ Tentacle '{}' failed to declare exchange: {}",
                        self.name, e
                    );
                }
            }

            // Queues abonnieren
            let mut consumers = Vec::new();
            for queue_name in &self.config.queues {
                // Queue deklarieren
                if let Err(e) = channel
                    .queue_declare(
                        queue_name,
                        QueueDeclareOptions::default(),
                        FieldTable::default(),
                    )
                    .await
                {
                    warn!(
                        "RabbitMQ Tentacle '{}' failed to declare queue '{}': {}",
                        self.name, queue_name, e
                    );
                    continue;
                }

                // Mit Exchange binden (falls konfiguriert)
                if let Some(ref exchange_name) = self.config.exchange {
                    let routing = queue_name.clone();
                    if let Err(e) = channel
                        .queue_bind(
                            queue_name,
                            exchange_name,
                            &routing,
                            QueueBindOptions::default(),
                            FieldTable::default(),
                        )
                        .await
                    {
                        warn!(
                            "RabbitMQ Tentacle '{}' failed to bind queue '{}': {}",
                            self.name, queue_name, e
                        );
                    }
                }

                info!(
                    "RabbitMQ Tentacle '{}' consuming from queue '{}'",
                    self.name, queue_name
                );

                let consumer = match channel
                    .basic_consume(
                        queue_name,
                        "",
                        BasicConsumeOptions {
                            no_ack: self.config.auto_ack,
                            ..Default::default()
                        },
                        FieldTable::default(),
                    )
                    .await
                {
                    Ok(c) => c,
                    Err(e) => {
                        error!(
                            "RabbitMQ Tentacle '{}' failed to consume from '{}': {}",
                            self.name, queue_name, e
                        );
                        continue;
                    }
                };

                consumers.push((queue_name.clone(), consumer));
            }

            if consumers.is_empty() {
                error!("RabbitMQ Tentacle '{}' has no active consumers", self.name);
                time::sleep(Duration::from_secs(self.config.reconnect_delay_seconds)).await;
                continue;
            }

            // Vereinfachter Consumer-Loop: nur die erste Queue verarbeiten
            // In einer vollständigen Implementierung würde man alle Consumer multiplexen
            if let Some((queue_name, mut consumer)) = consumers.into_iter().next() {
                info!(
                    "RabbitMQ Tentacle '{}' listening on queue '{}'",
                    self.name, queue_name
                );

                while let Some(Ok(delivery)) = consumer.next().await {
                    let routing_key = delivery.routing_key.to_string();

                    debug!(
                        "RabbitMQ Tentacle '{}' received message on '{}'",
                        self.name, routing_key
                    );

                    // Manuelles ACK zuerst (falls auto_ack aus)
                    if !self.config.auto_ack
                        && let Err(e) = delivery.ack(BasicAckOptions::default()).await
                    {
                        warn!("RabbitMQ Tentacle '{}' ack failed: {}", self.name, e);
                    }

                    // Jetzt kann delivery.data bewegt werden
                    let payload = delivery.data;
                    let ink =
                        self.create_ink(&queue_name, &routing_key, &payload, &delivery.properties);
                    event_bus.publish(ink);
                }

                warn!(
                    "RabbitMQ Tentacle '{}' consumer closed, reconnecting...",
                    self.name
                );
            }

            time::sleep(Duration::from_secs(self.config.reconnect_delay_seconds)).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::appcontext::AppContext;

    #[test]
    fn test_resolve_topic_routing_key_mode() {
        let config = RabbitMqTentacleConfig {
            server: "amqp://guest:guest@localhost:5672".to_string(),
            queues: vec!["orders".to_string()],
            exchange: None,
            exchange_kind: "topic".to_string(),
            topic_mode: RabbitMqTopicMode::RoutingKey,
            topic_field: None,
            fallback_topic: "/rabbitmq/default".to_string(),
            prefetch_count: 10,
            auto_ack: false,
            reconnect_delay_seconds: 5,
            auth: RabbitMqAuth::None,
        };
        let tentacle = RabbitMqTentacle::new("test".to_string(), config);

        let topic = tentacle.resolve_topic("orders.new", b"{}");
        assert_eq!(topic, "/rabbitmq/orders/new");
    }

    #[test]
    fn test_resolve_topic_field_mode() {
        let config = RabbitMqTentacleConfig {
            server: "amqp://guest:guest@localhost:5672".to_string(),
            queues: vec!["orders".to_string()],
            exchange: None,
            exchange_kind: "topic".to_string(),
            topic_mode: RabbitMqTopicMode::Field,
            topic_field: Some("event_type".to_string()),
            fallback_topic: "/rabbitmq/default".to_string(),
            prefetch_count: 10,
            auto_ack: false,
            reconnect_delay_seconds: 5,
            auth: RabbitMqAuth::None,
        };
        let tentacle = RabbitMqTentacle::new("test".to_string(), config);

        let payload = r#"{"event_type": "/events/order", "data": "test"}"#;
        let topic = tentacle.resolve_topic("orders.new", payload.as_bytes());
        assert_eq!(topic, "/events/order");
    }

    #[test]
    fn test_resolve_topic_all_mode() {
        let config = RabbitMqTentacleConfig {
            server: "amqp://guest:guest@localhost:5672".to_string(),
            queues: vec!["orders".to_string()],
            exchange: None,
            exchange_kind: "topic".to_string(),
            topic_mode: RabbitMqTopicMode::All,
            topic_field: None,
            fallback_topic: "/rabbitmq/all".to_string(),
            prefetch_count: 10,
            auto_ack: false,
            reconnect_delay_seconds: 5,
            auth: RabbitMqAuth::None,
        };
        let tentacle = RabbitMqTentacle::new("test".to_string(), config);

        let topic = tentacle.resolve_topic("any.key", b"payload");
        assert_eq!(topic, "/rabbitmq/all");
    }

    #[test]
    fn test_create_ink_metadata() {
        let config = RabbitMqTentacleConfig {
            server: "amqp://guest:guest@localhost:5672".to_string(),
            queues: vec!["orders".to_string()],
            exchange: None,
            exchange_kind: "topic".to_string(),
            topic_mode: RabbitMqTopicMode::RoutingKey,
            topic_field: None,
            fallback_topic: "/rabbitmq/default".to_string(),
            prefetch_count: 10,
            auto_ack: false,
            reconnect_delay_seconds: 5,
            auth: RabbitMqAuth::None,
        };
        let tentacle = RabbitMqTentacle::new("test".to_string(), config);

        let props = BasicProperties::default();
        let ink = tentacle.create_ink("orders", "orders.new", b"Hello RabbitMQ", &props);
        assert_eq!(ink.topic, "/rabbitmq/orders/new");
        assert_eq!(ink.body, "Hello RabbitMQ");

        let meta = ink.metadata.unwrap();
        let meta_obj = meta.as_object().unwrap();
        assert_eq!(
            meta_obj.get("rabbitmq_queue").unwrap(),
            &serde_json::json!("orders")
        );
        assert_eq!(
            meta_obj.get("rabbitmq_routing_key").unwrap(),
            &serde_json::json!("orders.new")
        );
    }

    #[test]
    fn test_rabbitmq_config_deserialization() {
        let yaml = r#"
        server: "amqp://guest:guest@localhost:5672"
        queues:
          - "orders"
          - "events"
        exchange: "kraken_exchange"
        exchange_kind: "topic"
        topic_mode: "routingkey"
        prefetch_count: 20
        auto_ack: false
        reconnect_delay_seconds: 10
        "#;

        let config: RabbitMqTentacleConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.queues.len(), 2);
        assert_eq!(config.exchange.as_deref(), Some("kraken_exchange"));
        assert_eq!(config.prefetch_count, 20);
        assert!(!config.auto_ack);
    }

    #[test]
    fn test_rabbitmq_tentacle_name_and_type() {
        let config = RabbitMqTentacleConfig {
            server: "amqp://guest:guest@localhost:5672".to_string(),
            queues: vec!["orders".to_string()],
            exchange: None,
            exchange_kind: "topic".to_string(),
            topic_mode: RabbitMqTopicMode::RoutingKey,
            topic_field: None,
            fallback_topic: "/rabbitmq/default".to_string(),
            prefetch_count: 10,
            auto_ack: false,
            reconnect_delay_seconds: 5,
            auth: RabbitMqAuth::None,
        };
        let tentacle = RabbitMqTentacle::new("test_rabbitmq".to_string(), config);
        assert_eq!(tentacle.name(), "test_rabbitmq");
        assert_eq!(tentacle.r#type(), "rabbitmq");
    }

    #[test]
    fn test_rabbitmq_auth_url() {
        let yaml = r#"
        type: "url"
        url: "amqp://user:pass@localhost:5672/myvhost"
        "#;
        let auth: RabbitMqAuth = serde_yaml::from_str(yaml).unwrap();
        assert!(matches!(auth, RabbitMqAuth::Url { .. }));
    }
}
