use anyhow::Result;
use async_trait::async_trait;
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::Sha256;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::time;
use tracing::{debug, info, warn};

type HmacSha256 = Hmac<Sha256>;

use super::Tentacle;
use crate::appcontext::AppContext;
use crate::eventbus::EventBus;
use crate::model::Ink;

/// Topic-Erkennungsmodus für Azure Service Bus-Nachrichten
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "lowercase")]
pub enum AsbTopicMode {
    /// Topic aus dem Azure Service Bus Queue/Topic-Namen verwenden
    #[default]
    Entity,
    /// Topic aus einem JSON-Feld extrahieren
    Field,
    /// Alle Nachrichten an ein festes Topic
    All,
}

/// Authentifizierungstyp für Azure Service Bus
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum AsbAuth {
    /// SAS Shared Access Signature
    Sas { key_name: String, key_value: String },
    /// Connection String
    ConnectionString { connection_string: String },
}

/// Konfiguration für den Azure Service Bus Tentacle
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AsbTentacleConfig {
    /// Azure Service Bus Namespace (z.B. "mynamespace.servicebus.windows.net")
    pub namespace: String,
    /// Queue-Namen zum Abonnieren
    #[serde(default)]
    pub queues: Vec<String>,
    /// Service Bus Topic-Namen zum Abonnieren (mit Subscription)
    #[serde(default)]
    pub topics: Vec<AsbTopicSubscription>,
    /// Topic-Erkennungsmodus
    #[serde(default)]
    pub topic_mode: AsbTopicMode,
    /// JSON-Feld für Topic-Extraktion (bei field-Modus)
    #[serde(default)]
    pub topic_field: Option<String>,
    /// Fallback-Topic
    #[serde(default = "default_fallback_topic")]
    pub fallback_topic: String,
    /// Optional: Entity-basiertes Topic-Mapping
    #[serde(default)]
    pub entity_mapping: HashMap<String, String>,
    /// Authentifizierung
    pub auth: AsbAuth,
    /// Poll-Interval in Sekunden
    #[serde(default = "default_poll_interval")]
    pub poll_interval_seconds: u64,
    /// Max Nachrichten pro Poll
    #[serde(default = "default_max_messages")]
    pub max_messages_per_poll: u32,
    /// Reconnect-Verzögerung in Sekunden
    #[serde(default = "default_reconnect_delay")]
    pub reconnect_delay_seconds: u64,
}

/// Topic-Subscription für Service Bus Topics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AsbTopicSubscription {
    /// Topic-Name
    pub topic: String,
    /// Subscription-Name
    pub subscription: String,
}

fn default_fallback_topic() -> String {
    "/azure/servicebus/default".to_string()
}

fn default_poll_interval() -> u64 {
    5
}

fn default_max_messages() -> u32 {
    10
}

fn default_reconnect_delay() -> u64 {
    10
}

pub struct AsbTentacle {
    name: String,
    config: AsbTentacleConfig,
    client: Arc<reqwest::Client>,
}

/// Freistehende Funktion: Bestimmt das Topic basierend auf der ASB-Nachricht
fn resolve_topic(config: &AsbTentacleConfig, entity_name: &str, payload: &[u8]) -> String {
    // Prüfe entity_mapping
    if let Some(mapped) = config.entity_mapping.get(entity_name) {
        return mapped.clone();
    }

    match config.topic_mode {
        AsbTopicMode::Entity => {
            format!("/azure/servicebus/{}", entity_name)
        }
        AsbTopicMode::Field => {
            let extracted = serde_json::from_slice::<serde_json::Value>(payload)
                .ok()
                .zip(config.topic_field.as_deref())
                .and_then(|(v, f)| v.get(f).and_then(|val| val.as_str().map(|s| s.to_string())))
                .filter(|s| !s.is_empty());

            if let Some(extracted) = extracted {
                return if extracted.starts_with('/') {
                    extracted
                } else {
                    format!("/azure/servicebus/{}", extracted)
                };
            }
            format!("/azure/servicebus/{}", entity_name)
        }
        AsbTopicMode::All => config.fallback_topic.clone(),
    }
}

/// Extrahiert den Body aus der Nachricht
fn extract_body(payload: &[u8]) -> String {
    if let Ok(text) = std::str::from_utf8(payload) {
        return text.to_string();
    }
    base64::Engine::encode(&base64::engine::general_purpose::STANDARD, payload)
}

impl AsbTentacle {
    pub fn new(name: String, config: AsbTentacleConfig, app_ctx: &AppContext) -> Self {
        Self {
            name,
            config,
            client: Arc::clone(app_ctx.http_client()),
        }
    }
}

#[async_trait]
impl Tentacle for AsbTentacle {
    fn name(&self) -> &str {
        &self.name
    }

    fn r#type(&self) -> &str {
        "azure_servicebus"
    }

    async fn start(&self, event_bus: Arc<EventBus>) -> Result<()> {
        info!(
            "Azure Service Bus Tentacle '{}' starting, namespace: {}",
            self.name, self.config.namespace
        );

        let config = self.config.clone();
        let tentacle_name = self.name.clone();
        let event_bus_clone = Arc::clone(&event_bus);
        let client = self.client.as_ref().clone();

        tokio::spawn(async move {
            loop {
                // Von Queues empfangen
                for queue_name in &config.queues {
                    match receive_from_queue_impl(&client, &config, queue_name).await {
                        Ok(messages) => {
                            for msg in messages {
                                let payload_bytes = match &msg {
                                    serde_json::Value::String(s) => s.as_bytes().to_vec(),
                                    _ => msg.to_string().into_bytes(),
                                };
                                let payload_slice = payload_bytes.as_slice();
                                let topic = resolve_topic(&config, queue_name, payload_slice);
                                let body = extract_body(payload_slice);

                                debug!(
                                    "ASB Tentacle '{}': received message from queue '{}'",
                                    tentacle_name, queue_name
                                );

                                let mut metadata = serde_json::Map::new();
                                metadata.insert(
                                    "asb_entity".to_string(),
                                    serde_json::Value::String(queue_name.clone()),
                                );
                                metadata.insert(
                                    "asb_entity_type".to_string(),
                                    serde_json::Value::String("queue".to_string()),
                                );

                                let mut ink =
                                    Ink::new(&topic, "azure_servicebus", &tentacle_name, body);
                                ink = ink.with_metadata(serde_json::Value::Object(metadata));

                                info!(
                                    "ASB Tentacle '{}': published ink on topic: {}",
                                    tentacle_name, topic
                                );
                                event_bus_clone.publish(ink);
                            }
                        }
                        Err(e) => {
                            warn!(
                                "ASB Tentacle '{}': error receiving from queue '{}': {}",
                                tentacle_name, queue_name, e
                            );
                        }
                    }
                }

                // Von Topics/Subscriptions empfangen
                for sub in &config.topics {
                    let entity_name = format!("{}/{}", sub.topic, sub.subscription);
                    match receive_from_queue_impl(
                        &client,
                        &config,
                        &format!("{}/subscriptions/{}", sub.topic, sub.subscription),
                    )
                    .await
                    {
                        Ok(messages) => {
                            for msg in messages {
                                let payload_bytes = match &msg {
                                    serde_json::Value::String(s) => s.as_bytes().to_vec(),
                                    _ => msg.to_string().into_bytes(),
                                };
                                let payload_slice = payload_bytes.as_slice();
                                let topic = resolve_topic(&config, &entity_name, payload_slice);
                                let body = extract_body(payload_slice);

                                debug!(
                                    "ASB Tentacle '{}': received message from topic '{}' subscription '{}'",
                                    tentacle_name, sub.topic, sub.subscription
                                );

                                let mut metadata = serde_json::Map::new();
                                metadata.insert(
                                    "asb_entity".to_string(),
                                    serde_json::Value::String(entity_name.clone()),
                                );
                                metadata.insert(
                                    "asb_entity_type".to_string(),
                                    serde_json::Value::String("topic".to_string()),
                                );
                                metadata.insert(
                                    "asb_subscription".to_string(),
                                    serde_json::Value::String(sub.subscription.clone()),
                                );

                                let mut ink =
                                    Ink::new(&topic, "azure_servicebus", &tentacle_name, body);
                                ink = ink.with_metadata(serde_json::Value::Object(metadata));

                                info!(
                                    "ASB Tentacle '{}': published ink on topic: {}",
                                    tentacle_name, topic
                                );
                                event_bus_clone.publish(ink);
                            }
                        }
                        Err(e) => {
                            warn!(
                                "ASB Tentacle '{}': error receiving from topic '{}': {}",
                                tentacle_name, sub.topic, e
                            );
                        }
                    }
                }

                time::sleep(Duration::from_secs(config.poll_interval_seconds)).await;
            }
        });

        // Blockiere den Task, damit er läuft
        loop {
            time::sleep(Duration::from_secs(60)).await;
        }
    }
}

/// Freistehende Funktion für Queue-Empfang (für spawn)
async fn receive_from_queue_impl(
    client: &reqwest::Client,
    config: &AsbTentacleConfig,
    path: &str,
) -> Result<Vec<serde_json::Value>> {
    let auth = match &config.auth {
        AsbAuth::Sas {
            key_name,
            key_value,
        } => {
            let resource_uri = format!("https://{}.servicebus.windows.net", config.namespace);
            generate_sas_token_impl(&resource_uri, key_name, key_value)
        }
        AsbAuth::ConnectionString { connection_string } => {
            let parts: Vec<&str> = connection_string.split(';').collect();
            let mut endpoint = String::new();
            let mut key_name = String::new();
            let mut key_value = String::new();

            for part in parts {
                if let Some((k, v)) = part.split_once('=') {
                    match k {
                        "Endpoint" => endpoint = v.trim_end_matches('.').to_string(),
                        "SharedAccessKeyName" => key_name = v.to_string(),
                        "SharedAccessKey" => key_value = v.to_string(),
                        _ => {}
                    }
                }
            }
            generate_sas_token_impl(&endpoint, &key_name, &key_value)
        }
    };

    let url = format!(
        "https://{}.servicebus.windows.net/{}/messages?timeout=30&api-version=8.0&maxMessageCount={}",
        config.namespace, path, config.max_messages_per_poll
    );

    let resp = client
        .get(&url)
        .header("Authorization", &auth)
        .header("Accept", "application/atom+xml")
        .send()
        .await?;

    if !resp.status().is_success() {
        return Err(anyhow::anyhow!(
            "Failed to receive from {}: {}",
            path,
            resp.status()
        ));
    }

    let body = resp.text().await?;
    let mut messages = Vec::new();

    for entry in body.split("<entry>") {
        if let Some(content_start) = entry.find("<Content>")
            && let Some(content_end) = entry[content_start + 9..].find("</Content>")
        {
            let content = &entry[content_start + 9..content_start + 9 + content_end];
            if let Ok(json) = serde_json::from_str::<serde_json::Value>(content) {
                messages.push(json);
            } else if !content.is_empty() {
                messages.push(serde_json::Value::String(content.to_string()));
            }
        }
    }

    Ok(messages)
}

fn generate_sas_token_impl(resource_uri: &str, policy_name: &str, key: &str) -> String {
    let expires = (chrono::Utc::now() + chrono::Duration::hours(1)).timestamp();
    let sign_hmac = format!("{}\n{}", resource_uri, expires);

    let mut mac = HmacSha256::new_from_slice(key.as_bytes()).unwrap();
    mac.update(sign_hmac.as_bytes());
    let sig = mac.finalize();
    let encoded_sig =
        base64::Engine::encode(&base64::engine::general_purpose::STANDARD, sig.into_bytes());
    let encoded_uri = urlencoding::encode(resource_uri);

    format!(
        "SharedAccessSignature sr={}&sig={}&se={}&skn={}",
        encoded_uri,
        urlencoding::encode(&encoded_sig),
        expires,
        policy_name
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::appcontext::AppContext;

    #[test]
    fn test_asb_topic_mode_entity() {
        let config = AsbTentacleConfig {
            namespace: "testns".to_string(),
            queues: vec!["test-queue".to_string()],
            topics: vec![],
            topic_mode: AsbTopicMode::Entity,
            topic_field: None,
            fallback_topic: "/azure/servicebus/default".to_string(),
            entity_mapping: HashMap::new(),
            auth: AsbAuth::Sas {
                key_name: "key".to_string(),
                key_value: "value".to_string(),
            },
            poll_interval_seconds: 5,
            max_messages_per_poll: 10,
            reconnect_delay_seconds: 10,
        };

        assert_eq!(
            resolve_topic(&config, "orders", b"payload"),
            "/azure/servicebus/orders"
        );
    }

    #[test]
    fn test_asb_topic_mode_field() {
        let config = AsbTentacleConfig {
            namespace: "testns".to_string(),
            queues: vec!["test-queue".to_string()],
            topics: vec![],
            topic_mode: AsbTopicMode::Field,
            topic_field: Some("kraken_topic".to_string()),
            fallback_topic: "/azure/servicebus/default".to_string(),
            entity_mapping: HashMap::new(),
            auth: AsbAuth::Sas {
                key_name: "key".to_string(),
                key_value: "value".to_string(),
            },
            poll_interval_seconds: 5,
            max_messages_per_poll: 10,
            reconnect_delay_seconds: 10,
        };

        let payload = r#"{"kraken_topic": "/orders/new", "id": 123}"#;
        assert_eq!(
            resolve_topic(&config, "orders", payload.as_bytes()),
            "/orders/new"
        );

        let payload = r#"{"id": 123}"#;
        assert_eq!(
            resolve_topic(&config, "orders", payload.as_bytes()),
            "/azure/servicebus/orders"
        );
    }

    #[test]
    fn test_asb_topic_mode_all() {
        let config = AsbTentacleConfig {
            namespace: "testns".to_string(),
            queues: vec!["test-queue".to_string()],
            topics: vec![],
            topic_mode: AsbTopicMode::All,
            topic_field: None,
            fallback_topic: "/azure/all".to_string(),
            entity_mapping: HashMap::new(),
            auth: AsbAuth::Sas {
                key_name: "key".to_string(),
                key_value: "value".to_string(),
            },
            poll_interval_seconds: 5,
            max_messages_per_poll: 10,
            reconnect_delay_seconds: 10,
        };

        assert_eq!(resolve_topic(&config, "any", b"payload"), "/azure/all");
    }

    #[test]
    fn test_asb_entity_mapping() {
        let mut mapping = HashMap::new();
        mapping.insert("orders".to_string(), "/orders/incoming".to_string());
        mapping.insert("shipments".to_string(), "/shipments/incoming".to_string());

        let config = AsbTentacleConfig {
            namespace: "testns".to_string(),
            queues: vec!["orders".to_string(), "shipments".to_string()],
            topics: vec![],
            topic_mode: AsbTopicMode::Entity,
            topic_field: None,
            fallback_topic: "/azure/servicebus/default".to_string(),
            entity_mapping: mapping,
            auth: AsbAuth::Sas {
                key_name: "key".to_string(),
                key_value: "value".to_string(),
            },
            poll_interval_seconds: 5,
            max_messages_per_poll: 10,
            reconnect_delay_seconds: 10,
        };

        assert_eq!(
            resolve_topic(&config, "orders", b"payload"),
            "/orders/incoming"
        );
        assert_eq!(
            resolve_topic(&config, "shipments", b"payload"),
            "/shipments/incoming"
        );
        assert_eq!(
            resolve_topic(&config, "unknown", b"payload"),
            "/azure/servicebus/unknown"
        );
    }

    #[test]
    fn test_asb_extract_body_utf8() {
        assert_eq!(extract_body(b"Hello World"), "Hello World");
    }

    #[test]
    fn test_asb_extract_body_binary() {
        let binary = vec![0xff, 0xfe, 0xfd];
        let body = extract_body(&binary);
        assert!(!body.is_empty());
        assert_eq!(
            body,
            base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &binary)
        );
    }

    #[test]
    fn test_asb_config_deserialization() {
        let yaml = r#"
        namespace: "mynamespace"
        queues:
          - "orders"
          - "shipments"
        topics:
          - topic: "events"
            subscription: "kraken"
        topic_mode: field
        topic_field: "kraken_topic"
        fallback_topic: "/azure/sb/default"
        entity_mapping:
          "orders": "/orders/incoming"
        auth:
          type: sas
          key_name: "RootManageSharedAccessKey"
          key_value: "secret123"
        poll_interval_seconds: 10
        max_messages_per_poll: 20
        "#;

        let config: AsbTentacleConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.namespace, "mynamespace");
        assert_eq!(config.queues.len(), 2);
        assert_eq!(config.topics.len(), 1);
        assert_eq!(config.topics[0].topic, "events");
        assert_eq!(config.topics[0].subscription, "kraken");
        assert_eq!(config.topic_mode, AsbTopicMode::Field);
        assert_eq!(config.poll_interval_seconds, 10);
        assert_eq!(config.max_messages_per_poll, 20);
    }

    #[test]
    fn test_asb_tentacle_name_and_type() {
        let config = AsbTentacleConfig {
            namespace: "testns".to_string(),
            queues: vec!["test".to_string()],
            topics: vec![],
            topic_mode: AsbTopicMode::Entity,
            topic_field: None,
            fallback_topic: "/azure/servicebus/default".to_string(),
            entity_mapping: HashMap::new(),
            auth: AsbAuth::Sas {
                key_name: "key".to_string(),
                key_value: "value".to_string(),
            },
            poll_interval_seconds: 5,
            max_messages_per_poll: 10,
            reconnect_delay_seconds: 10,
        };
        let tentacle = AsbTentacle::new("test_asb".to_string(), config, &AppContext::new());
        assert_eq!(tentacle.name(), "test_asb");
        assert_eq!(tentacle.r#type(), "azure_servicebus");
    }

    #[test]
    fn test_asb_auth_variants() {
        let yaml_sas = r#"
        namespace: "testns"
        queues: ["test"]
        auth:
          type: sas
          key_name: "key"
          key_value: "value"
        "#;
        let config: AsbTentacleConfig = serde_yaml::from_str(yaml_sas).unwrap();
        assert!(matches!(config.auth, AsbAuth::Sas { .. }));

        let yaml_conn = r#"
        namespace: "testns"
        queues: ["test"]
        auth:
          type: connectionstring
          connection_string: "Endpoint=sb://test.servicebus.windows.net;/SharedAccessKeyName=Root;SharedAccessKey=secret"
        "#;
        let config: AsbTentacleConfig = serde_yaml::from_str(yaml_conn).unwrap();
        assert!(matches!(config.auth, AsbAuth::ConnectionString { .. }));
    }

    #[test]
    fn test_asb_field_mode_with_slash_prefix() {
        let config = AsbTentacleConfig {
            namespace: "testns".to_string(),
            queues: vec!["test".to_string()],
            topics: vec![],
            topic_mode: AsbTopicMode::Field,
            topic_field: Some("topic".to_string()),
            fallback_topic: "/azure/servicebus/default".to_string(),
            entity_mapping: HashMap::new(),
            auth: AsbAuth::Sas {
                key_name: "key".to_string(),
                key_value: "value".to_string(),
            },
            poll_interval_seconds: 5,
            max_messages_per_poll: 10,
            reconnect_delay_seconds: 10,
        };

        let payload = r#"{"topic": "/custom/path"}"#;
        assert_eq!(
            resolve_topic(&config, "test", payload.as_bytes()),
            "/custom/path"
        );

        let payload = r#"{"topic": "custom/path"}"#;
        assert_eq!(
            resolve_topic(&config, "test", payload.as_bytes()),
            "/azure/servicebus/custom/path"
        );
    }
}
