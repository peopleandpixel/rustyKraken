use anyhow::{Context, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info, warn};

use super::Tentacle;
use crate::appcontext::AppContext;
use crate::eventbus::EventBus;
use crate::model::Ink;

/// Konfiguration für Google Pub/Sub Tentacle
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GooglePubSubTentacleConfig {
    /// Google Cloud Projekt ID
    pub project_id: String,
    /// Subscription Name (z.B. "my-subscription")
    pub subscription: String,
    /// Service Account JSON Key Datei oder Inhalt (optional, falls nicht gesetzt wird GOOGLE_APPLICATION_CREDENTIALS verwendet)
    #[serde(default)]
    pub service_account_key: Option<String>,
    /// Polling Intervall in Sekunden (Standard: 5)
    #[serde(default = "default_poll_interval")]
    pub poll_interval: u64,
    /// Maximale Anzahl von Nachrichten pro Pull (Standard: 10)
    #[serde(default = "default_max_messages")]
    pub max_messages: i32,
    /// Nachrichten nach erfolgreichem Processing acknowledge-n (Standard: true)
    #[serde(default = "default_auto_ack")]
    pub auto_ack: bool,
    /// Fallback Topic wenn kein Topic aus Attributen erkannt wird
    #[serde(default = "default_fallback_topic")]
    pub fallback_topic: String,
    /// Topic-Modus: "attribute" (aus Message-Attributen), "data" (aus decodierten Daten), "all" (kombiniert)
    #[serde(default = "default_topic_mode")]
    pub topic_mode: String,
    /// Optional: Mapping von Pub/Sub Topics auf rustyKraken Topics
    #[serde(default)]
    pub topic_mapping: HashMap<String, String>,
    /// Optional: Google Cloud Endpoint (für Testing)
    #[serde(default)]
    pub endpoint_url: Option<String>,
}

fn default_poll_interval() -> u64 {
    5
}

fn default_max_messages() -> i32 {
    10
}

fn default_auto_ack() -> bool {
    true
}

fn default_fallback_topic() -> String {
    "/pubsub/default".to_string()
}

fn default_topic_mode() -> String {
    "all".to_string()
}

pub struct GooglePubSubTentacle {
    name: String,
    config: GooglePubSubTentacleConfig,
    client: Arc<reqwest::Client>,
}

/// Pub/Sub Message Struktur
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct PubSubMessage {
    message_id: String,
    data: String, // Base64-encoded
    #[serde(default)]
    attributes: HashMap<String, String>,
}

/// ReceivedMessage aus der Pull-Response
#[derive(Debug, Deserialize)]
struct ReceivedMessage {
    #[serde(rename = "messageId")]
    #[allow(dead_code)]
    message_id: String,
    #[serde(default)]
    message: PubSubMessage,
}

impl GooglePubSubTentacle {
    pub fn new(name: String, config: GooglePubSubTentacleConfig, app_ctx: &AppContext) -> Self {
        Self {
            name,
            config,
            client: Arc::clone(app_ctx.http_client()),
        }
    }

    /// Bestimmt das Topic basierend auf Message-Attributen oder Daten
    fn resolve_topic(&self, message: &PubSubMessage) -> String {
        match self.config.topic_mode.as_str() {
            "attribute" => self.topic_from_attributes(message).unwrap_or_else(|| {
                self.config
                    .topic_mapping
                    .get(&self.config.subscription)
                    .cloned()
                    .unwrap_or_else(|| self.config.fallback_topic.clone())
            }),
            "data" => self.topic_from_data(message).unwrap_or_else(|| {
                self.config
                    .topic_mapping
                    .get(&self.config.subscription)
                    .cloned()
                    .unwrap_or_else(|| self.config.fallback_topic.clone())
            }),
            "all" => {
                // Attribute haben Vorrang, dann Daten, dann Topic-Mapping, dann Fallback
                if let Some(topic) = self.topic_from_attributes(message) {
                    return topic;
                }
                if let Some(topic) = self.topic_from_data(message) {
                    return topic;
                }
                // Topic-Mapping prüfen (basierend auf dem Pub/Sub Topic Name)
                if let Some(topic) = self.config.topic_mapping.get(&self.config.subscription) {
                    return topic.clone();
                }
                self.config.fallback_topic.clone()
            }
            _ => self.config.fallback_topic.clone(),
        }
    }

    /// Extrahiert Topic aus Message-Attributen
    fn topic_from_attributes(&self, message: &PubSubMessage) -> Option<String> {
        // "topic" Attribut hat Vorrang
        if let Some(topic) = message.attributes.get("topic")
            && !topic.is_empty()
        {
            return Some(topic.clone());
        }
        // "subject" Attribut als Fallback
        if let Some(subject) = message.attributes.get("subject")
            && !subject.is_empty()
        {
            return Some(subject.clone());
        }
        None
    }

    /// Extrahiert Topic aus den decodierten Daten
    fn topic_from_data(&self, message: &PubSubMessage) -> Option<String> {
        // Base64-dekodieren
        let decoded = base64_decode(&message.data);
        if let Some(text) = decoded {
            // Versuche, die Daten als JSON zu parsen
            if let Ok(body_json) = serde_json::from_str::<serde_json::Value>(&text) {
                // "topic" Feld im Body
                if let Some(topic) = body_json.get("topic").and_then(|t| t.as_str())
                    && !topic.is_empty()
                {
                    return Some(topic.to_string());
                }
                // "subject" Feld als Fallback
                if let Some(subject) = body_json.get("subject").and_then(|s| s.as_str())
                    && !subject.is_empty()
                {
                    return Some(subject.to_string());
                }
            }
            // Wenn Text mit "/" beginnt, ist er selbst das Topic
            if text.starts_with('/') {
                return Some(text);
            }
        }
        None
    }

    /// Decodiert die Base64-Daten und erstellt ein Ink
    fn create_ink(&self, message: &PubSubMessage) -> Ink {
        let topic = self.resolve_topic(message);
        let body = base64_decode(&message.data).unwrap_or_else(|| message.data.clone());

        // Metadata erstellen
        let mut metadata_map = serde_json::Map::new();
        metadata_map.insert(
            "message_id".to_string(),
            serde_json::Value::String(message.message_id.clone()),
        );
        metadata_map.insert(
            "subscription".to_string(),
            serde_json::Value::String(self.config.subscription.clone()),
        );
        metadata_map.insert(
            "project_id".to_string(),
            serde_json::Value::String(self.config.project_id.clone()),
        );

        // Attribute zur Metadata hinzufügen
        if !message.attributes.is_empty() {
            let attrs_map: serde_json::Map<String, serde_json::Value> = message
                .attributes
                .iter()
                .map(|(k, v)| (k.clone(), serde_json::Value::String(v.clone())))
                .collect();
            metadata_map.insert(
                "attributes".to_string(),
                serde_json::Value::Object(attrs_map),
            );
        }

        let metadata = serde_json::Value::Object(metadata_map);

        Ink::new(topic, "pubsub", &self.name, body).with_metadata(metadata)
    }

    /// Holt ein OAuth2 Access Token via Service Account JWT
    async fn get_access_token(&self) -> Result<String> {
        // Service Account Key holen
        let key_content = self
            .config
            .service_account_key
            .clone()
            .or_else(|| std::env::var("GOOGLE_APPLICATION_CREDENTIALS").ok())
            .context(
                "Google service account key not configured (set service_account_key or GOOGLE_APPLICATION_CREDENTIALS)",
            )?;

        // Prüfen, ob es ein Dateipfad ist
        let json_str =
            if key_content.ends_with(".json") && std::path::Path::new(&key_content).exists() {
                std::fs::read_to_string(&key_content).with_context(|| {
                    format!("Failed to read service account key file: {}", key_content)
                })?
            } else {
                key_content
            };

        // Service Account Key parsen
        let key: serde_json::Value =
            serde_json::from_str(&json_str).context("Invalid service account key JSON")?;

        let client_email = key
            .get("client_email")
            .and_then(|v| v.as_str())
            .context("Missing client_email in service account key")?;

        // JWT erstellen
        let now = chrono::Utc::now();
        let header = base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            r#"{"alg":"RS256","typ":"JWT"}"#,
        );
        let claim = format!(
            r#"{{"iss":"{}","scope":"https://www.googleapis.com/auth/cloud-platform","aud":"https://oauth2.googleapis.com/token","iat":{},"exp":{}}}"#,
            client_email,
            now.timestamp(),
            (now + chrono::TimeDelta::seconds(3600)).timestamp()
        );
        let claim_b64 = base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD_NO_PAD,
            claim.as_bytes(),
        );

        let signing_input = format!("{}.{}", header, claim_b64);

        // JWT signieren (vereinfacht – in Produktion sollte eine richtige JWT-Bibliothek verwendet werden)
        let signature = sign_jwt_simple(&signing_input)?;
        let jwt = format!("{}.{}", signing_input, signature);

        // Token anfordern
        let body = format!(
            "grant_type=urn:ietf:params:oauth:grant-type:jwt-bearer&assertion={}",
            urlencoding::encode(&jwt)
        );

        let response = self
            .client
            .post("https://oauth2.googleapis.com/token")
            .header("Content-Type", "application/x-www-form-urlencoded")
            .body(body)
            .send()
            .await?;

        let status = response.status();
        if !status.is_success() {
            return Err(anyhow::anyhow!(
                "Failed to get access token (status {}): {}",
                status,
                response.text().await.unwrap_or_default()
            ));
        }

        let token_response: serde_json::Value = response.json().await?;
        let token = token_response
            .get("access_token")
            .and_then(|v| v.as_str())
            .context("Missing access_token in response")?
            .to_string();

        Ok(token)
    }

    /// Empfängt Nachrichten von der Pub/Sub Subscription
    async fn pull_messages(&self) -> Result<Vec<ReceivedMessage>> {
        let token = self.get_access_token().await?;
        let endpoint = self
            .config
            .endpoint_url
            .as_deref()
            .unwrap_or("https://pubsub.googleapis.com/v1");

        let url = format!(
            "{}/projects/{}/subscriptions/{}:pull",
            endpoint, self.config.project_id, self.config.subscription
        );

        let body = serde_json::json!({
            "maxMessages": self.config.max_messages
        });

        let response = self
            .client
            .post(&url)
            .bearer_auth(&token)
            .json(&body)
            .send()
            .await?;

        if !response.status().is_success() {
            return Err(anyhow::anyhow!(
                "Pub/Sub pull failed with status {}: {}",
                response.status(),
                response.text().await.unwrap_or_default()
            ));
        }

        let response_body: serde_json::Value = response.json().await?;
        let messages: Vec<ReceivedMessage> = response_body
            .get("receivedMessages")
            .and_then(|v| v.as_array())
            .map(|arr| {
                arr.iter()
                    .filter_map(|m| serde_json::from_value(m.clone()).ok())
                    .collect()
            })
            .unwrap_or_default();

        Ok(messages)
    }

    /// Acknowledgt eine Nachricht
    async fn acknowledge(&self, message_id: &str) -> Result<()> {
        let token = self.get_access_token().await?;
        let endpoint = self
            .config
            .endpoint_url
            .as_deref()
            .unwrap_or("https://pubsub.googleapis.com/v1");

        let url = format!(
            "{}/projects/{}/subscriptions/{}:acknowledge",
            endpoint, self.config.project_id, self.config.subscription
        );

        let body = serde_json::json!({
            "ackIds": [message_id]
        });

        let response = self
            .client
            .post(&url)
            .bearer_auth(&token)
            .json(&body)
            .send()
            .await?;

        if !response.status().is_success() {
            warn!(
                "Pub/Sub tentacle '{}' failed to acknowledge message: {}",
                self.name,
                response.status()
            );
        }

        Ok(())
    }
}

/// Vereinfachte JWT-Signatur (SHA256-Hash als Platzhalter)
fn sign_jwt_simple(input: &str) -> Result<String> {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(input.as_bytes());
    let hash = hasher.finalize();
    Ok(base64::Engine::encode(
        &base64::engine::general_purpose::STANDARD_NO_PAD,
        hash,
    ))
}

fn base64_decode(data: &str) -> Option<String> {
    if data.is_empty() {
        return Some(String::new());
    }
    match base64::Engine::decode(&base64::engine::general_purpose::STANDARD, data) {
        Ok(bytes) => String::from_utf8(bytes).ok(),
        Err(_) => None,
    }
}

#[async_trait]
impl Tentacle for GooglePubSubTentacle {
    fn name(&self) -> &str {
        &self.name
    }

    fn r#type(&self) -> &str {
        "google_pubsub"
    }

    async fn start(&self, event_bus: Arc<EventBus>) -> Result<()> {
        info!(
            "Google Pub/Sub tentacle '{}' pulling from subscription: {}/{}",
            self.name, self.config.project_id, self.config.subscription
        );

        loop {
            match self.pull_messages().await {
                Ok(messages) => {
                    for received in &messages {
                        debug!(
                            "Pub/Sub tentacle '{}' received message: {}",
                            self.name, received.message.message_id
                        );

                        let ink = self.create_ink(&received.message);
                        debug!(
                            "Pub/Sub tentacle '{}' publishing message on topic: {}",
                            self.name, ink.topic
                        );
                        event_bus.publish(ink);

                        if self.config.auto_ack
                            && let Err(e) = self.acknowledge(&received.message.message_id).await
                        {
                            error!(
                                "Pub/Sub tentacle '{}' failed to acknowledge message: {}",
                                self.name, e
                            );
                        }
                    }
                }
                Err(e) => {
                    error!(
                        "Pub/Sub tentacle '{}' error pulling messages: {}",
                        self.name, e
                    );
                }
            }

            tokio::time::sleep(Duration::from_secs(self.config.poll_interval)).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_tentacle() -> GooglePubSubTentacle {
        let app_ctx = crate::appcontext::AppContext::new();
        GooglePubSubTentacle::new(
            "test_pubsub".to_string(),
            GooglePubSubTentacleConfig {
                project_id: "my-project".to_string(),
                subscription: "my-subscription".to_string(),
                service_account_key: None,
                poll_interval: 5,
                max_messages: 10,
                auto_ack: true,
                fallback_topic: "/pubsub/default".to_string(),
                topic_mode: "all".to_string(),
                topic_mapping: HashMap::new(),
                endpoint_url: None,
            },
            &app_ctx,
        )
    }

    #[test]
    fn test_tentacle_name_and_type() {
        let tentacle = create_tentacle();
        assert_eq!(tentacle.name(), "test_pubsub");
        assert_eq!(tentacle.r#type(), "google_pubsub");
    }

    #[test]
    fn test_resolve_topic_from_attribute() {
        let config = GooglePubSubTentacleConfig {
            project_id: "my-project".to_string(),
            subscription: "my-subscription".to_string(),
            service_account_key: None,
            poll_interval: 5,
            max_messages: 10,
            auto_ack: true,
            fallback_topic: "/pubsub/default".to_string(),
            topic_mode: "attribute".to_string(),
            topic_mapping: HashMap::new(),
            endpoint_url: None,
        };

        let mut attrs = HashMap::new();
        attrs.insert("topic".to_string(), "/pubsub/alerts".to_string());

        let message = PubSubMessage {
            message_id: "msg-1".to_string(),
            data: "".to_string(),
            attributes: attrs,
        };

        let tentacle = GooglePubSubTentacle::new(
            "test".to_string(),
            config,
            &crate::appcontext::AppContext::new(),
        );
        assert_eq!(tentacle.resolve_topic(&message), "/pubsub/alerts");

        // Subject Fallback
        let mut attrs2 = HashMap::new();
        attrs2.insert("subject".to_string(), "/pubsub/subject-topic".to_string());

        let message2 = PubSubMessage {
            message_id: "msg-2".to_string(),
            data: "".to_string(),
            attributes: attrs2,
        };

        assert_eq!(tentacle.resolve_topic(&message2), "/pubsub/subject-topic");
    }

    #[test]
    fn test_resolve_topic_from_data() {
        let config = GooglePubSubTentacleConfig {
            project_id: "my-project".to_string(),
            subscription: "my-subscription".to_string(),
            service_account_key: None,
            poll_interval: 5,
            max_messages: 10,
            auto_ack: true,
            fallback_topic: "/pubsub/default".to_string(),
            topic_mode: "data".to_string(),
            topic_mapping: HashMap::new(),
            endpoint_url: None,
        };

        let tentacle = GooglePubSubTentacle::new(
            "test".to_string(),
            config,
            &crate::appcontext::AppContext::new(),
        );

        // JSON Daten mit topic Feld
        let json_data = r#"{"topic": "/pubsub/json-topic", "data": "value"}"#;
        let b64_data = base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            json_data.as_bytes(),
        );

        let message = PubSubMessage {
            message_id: "msg-1".to_string(),
            data: b64_data,
            attributes: HashMap::new(),
        };

        assert_eq!(tentacle.resolve_topic(&message), "/pubsub/json-topic");

        // Plain Text beginnt mit /
        let plain_data = "/pubsub/plain-topic";
        let b64_plain = base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            plain_data.as_bytes(),
        );

        let message2 = PubSubMessage {
            message_id: "msg-2".to_string(),
            data: b64_plain,
            attributes: HashMap::new(),
        };

        assert_eq!(tentacle.resolve_topic(&message2), "/pubsub/plain-topic");
    }

    #[test]
    fn test_resolve_topic_all_mode() {
        let mut mapping = HashMap::new();
        mapping.insert("my-subscription".to_string(), "/pubsub/mapped".to_string());

        let config = GooglePubSubTentacleConfig {
            project_id: "my-project".to_string(),
            subscription: "my-subscription".to_string(),
            service_account_key: None,
            poll_interval: 5,
            max_messages: 10,
            auto_ack: true,
            fallback_topic: "/pubsub/default".to_string(),
            topic_mode: "all".to_string(),
            topic_mapping: mapping,
            endpoint_url: None,
        };

        let tentacle = GooglePubSubTentacle::new(
            "test".to_string(),
            config.clone(),
            &crate::appcontext::AppContext::new(),
        );

        // Kein Attribut, keine Daten -> Subscription Mapping
        let message = PubSubMessage {
            message_id: "msg-1".to_string(),
            data: "".to_string(),
            attributes: HashMap::new(),
        };

        assert_eq!(tentacle.resolve_topic(&message), "/pubsub/mapped");

        // Fallback wenn kein Mapping
        let mut config2 = config.clone();
        config2.topic_mapping = HashMap::new();
        let app_ctx2 = crate::appcontext::AppContext::new();
        let tentacle2 = GooglePubSubTentacle::new("test2".to_string(), config2, &app_ctx2);
        assert_eq!(tentacle2.resolve_topic(&message), "/pubsub/default");
    }

    #[test]
    fn test_config_defaults() {
        let config: GooglePubSubTentacleConfig = serde_yaml::from_str(
            r#"
            project_id: "my-project"
            subscription: "my-sub"
        "#,
        )
        .unwrap();

        assert_eq!(config.poll_interval, 5);
        assert_eq!(config.max_messages, 10);
        assert!(config.auto_ack);
        assert_eq!(config.fallback_topic, "/pubsub/default");
        assert_eq!(config.topic_mode, "all");
    }

    #[test]
    fn test_base64_decode() {
        assert_eq!(base64_decode(""), Some("".to_string()));

        let encoded =
            base64::Engine::encode(&base64::engine::general_purpose::STANDARD, b"hello world");
        assert_eq!(base64_decode(&encoded), Some("hello world".to_string()));

        assert_eq!(base64_decode("not-valid-base64!!"), None);
    }

    #[test]
    fn test_create_ink() {
        let config = GooglePubSubTentacleConfig {
            project_id: "my-project".to_string(),
            subscription: "my-subscription".to_string(),
            service_account_key: None,
            poll_interval: 5,
            max_messages: 10,
            auto_ack: true,
            fallback_topic: "/pubsub/default".to_string(),
            topic_mode: "all".to_string(),
            topic_mapping: HashMap::new(),
            endpoint_url: None,
        };

        let tentacle = GooglePubSubTentacle::new(
            "test".to_string(),
            config,
            &crate::appcontext::AppContext::new(),
        );

        let mut attrs = HashMap::new();
        attrs.insert("source".to_string(), "test-source".to_string());

        let message = PubSubMessage {
            message_id: "msg-123".to_string(),
            data: base64::Engine::encode(&base64::engine::general_purpose::STANDARD, b"test body"),
            attributes: attrs,
        };

        let ink = tentacle.create_ink(&message);
        assert_eq!(ink.topic, "/pubsub/default");
        assert_eq!(ink.source.name, "test");
        assert_eq!(ink.body, "test body");
    }
}
