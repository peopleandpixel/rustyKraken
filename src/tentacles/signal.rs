use anyhow::Result;
use async_trait::async_trait;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info, warn};

use super::Tentacle;
use crate::eventbus::EventBus;
use crate::model::Ink;

/// Authentifizierung für Signal
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum SignalAuth {
    #[default]
    None,
    /// Signal Service API Token
    ApiToken { token: String },
}

/// Konfiguration für den Signal Tentacle
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignalTentacleConfig {
    /// Signal Service URL (z.B. für Signal Desktop oder Gateway)
    #[serde(default = "default_service_url")]
    pub service_url: String,
    /// Authentifizierung
    #[serde(default)]
    pub auth: SignalAuth,
    /// Eigene Signal-Nummer
    pub own_number: String,
    /// Zu überwachende Kontakte/Gruppen (Telefonnummern oder Group IDs)
    pub contacts: Vec<String>,
    /// Topic für Events
    #[serde(default = "default_topic")]
    pub topic: String,
    /// Polling-Interval in Sekunden
    #[serde(default = "default_poll_interval")]
    pub poll_interval_secs: u64,
    /// Timeout in Sekunden
    #[serde(default = "default_timeout")]
    pub timeout_secs: u64,
    /// REST API Mode (für Signal Gateway/Provisioning)
    #[serde(default)]
    pub use_rest_api: bool,
    /// REST API Endpoint für Nachrichten
    #[serde(default)]
    pub rest_api_endpoint: Option<String>,
}

fn default_service_url() -> String {
    "https://service.signal.org".to_string()
}

fn default_topic() -> String {
    "/signal/message".to_string()
}

fn default_poll_interval() -> u64 {
    30
}

fn default_timeout() -> u64 {
    30
}

/// Signal-Nachricht
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignalMessage {
    pub source: String,
    pub destination: String,
    pub body: String,
    pub timestamp: u64,
    pub r#type: String,
    pub metadata: Option<serde_json::Value>,
}

pub struct SignalTentacle {
    name: String,
    config: SignalTentacleConfig,
}

impl SignalTentacle {
    pub fn new(name: String, config: SignalTentacleConfig) -> Self {
        Self { name, config }
    }

    #[allow(dead_code)]
    fn create_message_ink(&self, message: &SignalMessage, tentacle_name: &str) -> Ink {
        let mut meta_map = serde_json::Map::new();
        meta_map.insert("source".into(), serde_json::json!(message.source));
        meta_map.insert("destination".into(), serde_json::json!(message.destination));
        meta_map.insert("timestamp".into(), serde_json::json!(message.timestamp));
        meta_map.insert("message_type".into(), serde_json::json!(message.r#type));

        if let Some(ref md) = message.metadata {
            meta_map.insert("signal_metadata".into(), md.clone());
        }

        Ink::new(
            &self.config.topic,
            "signal",
            tentacle_name,
            format!(
                "[{}] {}: {}",
                message.source, message.destination, message.body
            ),
        )
        .with_metadata(serde_json::Value::Object(meta_map))
    }
}

#[async_trait]
impl Tentacle for SignalTentacle {
    fn name(&self) -> &str {
        &self.name
    }

    fn r#type(&self) -> &str {
        "signal"
    }

    async fn start(&self, event_bus: Arc<EventBus>) -> Result<()> {
        info!(
            "Signal Tentacle '{}' starting for {} contacts",
            self.name,
            self.config.contacts.len()
        );

        let config = self.config.clone();
        let tentacle_name = self.name.clone();
        let event_bus_clone = Arc::clone(&event_bus);

        tokio::spawn(async move {
            let client = match Client::builder()
                .timeout(Duration::from_secs(config.timeout_secs))
                .build()
            {
                Ok(c) => c,
                Err(e) => {
                    error!(
                        "Signal Tentacle '{}': failed to build HTTP client: {}",
                        tentacle_name, e
                    );
                    return;
                }
            };

            loop {
                if config.use_rest_api {
                    match fetch_messages_rest(&client, &config, &tentacle_name).await {
                        Ok(messages) => {
                            for message in messages {
                                let ink = create_signal_ink(&config, &tentacle_name, &message);
                                event_bus_clone.publish(ink);
                            }
                        }
                        Err(e) => {
                            error!(
                                "Signal Tentacle '{}': failed to fetch messages: {}",
                                tentacle_name, e
                            );
                        }
                    }
                } else {
                    warn!(
                        "Signal Tentacle '{}': direct Signal Service mode requires 'use_rest_api: true'. Use a Signal Gateway or REST API wrapper.",
                        tentacle_name
                    );
                }

                tokio::time::sleep(Duration::from_secs(config.poll_interval_secs)).await;
            }
        });

        loop {
            tokio::time::sleep(Duration::from_secs(60)).await;
        }
    }
}

async fn fetch_messages_rest(
    client: &Client,
    config: &SignalTentacleConfig,
    tentacle_name: &str,
) -> Result<Vec<SignalMessage>> {
    let endpoint = config
        .rest_api_endpoint
        .as_deref()
        .unwrap_or("/api/v1/messages");
    let url = format!("{}{}", config.service_url.trim_end_matches('/'), endpoint);

    debug!(
        "Signal Tentacle '{}': fetching messages from {}",
        tentacle_name, url
    );

    let mut req = client.get(&url);
    if let SignalAuth::ApiToken { token } = &config.auth {
        req = req.header("Authorization", format!("Bearer {}", token));
    }

    let response = req.send().await?;
    if !response.status().is_success() {
        return Err(anyhow::anyhow!(
            "Signal REST API request failed with status: {}",
            response.status()
        ));
    }

    let body = response.text().await?;

    // Versuche JSON-Array zu parsen
    if let Ok(messages) = serde_json::from_str::<Vec<serde_json::Value>>(&body) {
        let mut result = Vec::new();
        for m in messages {
            let source = m
                .get("source")
                .and_then(|s| s.as_str())
                .unwrap_or("unknown")
                .to_string();
            let destination = m
                .get("destination")
                .and_then(|d| d.as_str())
                .unwrap_or(&config.own_number)
                .to_string();
            let body = m
                .get("body")
                .and_then(|b| b.as_str())
                .unwrap_or("")
                .to_string();
            let timestamp = m.get("timestamp").and_then(|t| t.as_u64()).unwrap_or(0);
            let r#type = m
                .get("type")
                .and_then(|t| t.as_str())
                .unwrap_or("message")
                .to_string();
            let metadata = m.get("metadata").cloned();

            result.push(SignalMessage {
                source,
                destination,
                body,
                timestamp,
                r#type,
                metadata,
            });
        }
        debug!(
            "Signal Tentacle '{}': fetched {} messages",
            tentacle_name,
            result.len()
        );
        return Ok(result);
    }

    Ok(Vec::new())
}

fn create_signal_ink(
    config: &SignalTentacleConfig,
    tentacle_name: &str,
    message: &SignalMessage,
) -> Ink {
    let mut meta_map = serde_json::Map::new();
    meta_map.insert("source".into(), serde_json::json!(message.source));
    meta_map.insert("destination".into(), serde_json::json!(message.destination));
    meta_map.insert("timestamp".into(), serde_json::json!(message.timestamp));
    meta_map.insert("message_type".into(), serde_json::json!(message.r#type));

    if let Some(ref md) = message.metadata {
        meta_map.insert("signal_metadata".into(), md.clone());
    }

    Ink::new(
        &config.topic,
        "signal",
        tentacle_name,
        format!(
            "[{}] {}: {}",
            message.source, message.destination, message.body
        ),
    )
    .with_metadata(serde_json::Value::Object(meta_map))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_defaults() {
        let config: SignalTentacleConfig =
            serde_json::from_str(r#"{"own_number": "+1234567890", "contacts": ["+0987654321"]}"#)
                .unwrap();

        assert_eq!(config.service_url, "https://service.signal.org");
        assert_eq!(config.topic, "/signal/message");
        assert_eq!(config.poll_interval_secs, 30);
        assert_eq!(config.timeout_secs, 30);
        assert!(!config.use_rest_api);
    }

    #[test]
    fn test_auth_api_token() {
        let config: SignalTentacleConfig = serde_json::from_str(
            r#"{
                "own_number": "+1234567890",
                "contacts": [],
                "auth": {"type": "apitoken", "token": "secret_token_123"}
            }"#,
        )
        .unwrap();

        match config.auth {
            SignalAuth::ApiToken { token } => {
                assert_eq!(token, "secret_token_123");
            }
            _ => panic!("Expected ApiToken auth"),
        }
    }

    #[test]
    fn test_create_message_ink() {
        let tentacle = SignalTentacle::new(
            "test".to_string(),
            SignalTentacleConfig {
                service_url: "https://service.signal.org".to_string(),
                auth: SignalAuth::None,
                own_number: "+1234567890".to_string(),
                contacts: vec!["+0987654321".to_string()],
                topic: "/signal/test".to_string(),
                poll_interval_secs: 30,
                timeout_secs: 30,
                use_rest_api: false,
                rest_api_endpoint: None,
            },
        );

        let message = SignalMessage {
            source: "+0987654321".to_string(),
            destination: "+1234567890".to_string(),
            body: "Hello from Signal!".to_string(),
            timestamp: 1234567890,
            r#type: "message".to_string(),
            metadata: None,
        };

        let ink = tentacle.create_message_ink(&message, "test");

        assert_eq!(ink.topic, "/signal/test");
        assert!(ink.body.contains("Hello from Signal!"));
        assert!(ink.body.contains("+0987654321"));
    }

    #[test]
    fn test_rest_api_mode() {
        let config: SignalTentacleConfig = serde_json::from_str(
            r#"{
                "own_number": "+1234567890",
                "contacts": [],
                "use_rest_api": true,
                "rest_api_endpoint": "/api/v1/inbox"
            }"#,
        )
        .unwrap();

        assert!(config.use_rest_api);
        assert_eq!(config.rest_api_endpoint, Some("/api/v1/inbox".to_string()));
    }

    #[test]
    fn test_custom_service_url() {
        let config: SignalTentacleConfig = serde_json::from_str(
            r#"{
                "service_url": "https://signal.gateway.local",
                "own_number": "+1234567890",
                "contacts": []
            }"#,
        )
        .unwrap();

        assert_eq!(config.service_url, "https://signal.gateway.local");
    }

    #[test]
    fn test_multiple_contacts() {
        let config: SignalTentacleConfig = serde_json::from_str(
            r#"{
                "own_number": "+1234567890",
                "contacts": ["+1111111111", "+2222222222", "+3333333333"]
            }"#,
        )
        .unwrap();

        assert_eq!(config.contacts.len(), 3);
    }

    #[test]
    fn test_create_signal_ink() {
        let config = SignalTentacleConfig {
            service_url: "https://service.signal.org".to_string(),
            auth: SignalAuth::None,
            own_number: "+1234567890".to_string(),
            contacts: vec![],
            topic: "/signal/test".to_string(),
            poll_interval_secs: 30,
            timeout_secs: 30,
            use_rest_api: false,
            rest_api_endpoint: None,
        };

        let message = SignalMessage {
            source: "+9999999999".to_string(),
            destination: "+1234567890".to_string(),
            body: "Test message".to_string(),
            timestamp: 9876543210,
            r#type: "message".to_string(),
            metadata: None,
        };

        let ink = create_signal_ink(&config, "test", &message);

        assert_eq!(ink.topic, "/signal/test");
        assert!(ink.body.contains("Test message"));
        assert!(ink.body.contains("+9999999999"));
    }

    #[test]
    fn test_auth_none() {
        let config: SignalTentacleConfig = serde_json::from_str(
            r#"{"own_number": "+1234567890", "contacts": [], "auth": {"type": "none"}}"#,
        )
        .unwrap();

        assert_eq!(config.auth, SignalAuth::None);
    }

    #[test]
    fn test_custom_poll_interval() {
        let config: SignalTentacleConfig = serde_json::from_str(
            r#"{
                "own_number": "+1234567890",
                "contacts": [],
                "poll_interval_secs": 10,
                "timeout_secs": 60
            }"#,
        )
        .unwrap();

        assert_eq!(config.poll_interval_secs, 10);
        assert_eq!(config.timeout_secs, 60);
    }

    #[test]
    fn test_message_with_metadata() {
        let config = SignalTentacleConfig {
            service_url: "https://service.signal.org".to_string(),
            auth: SignalAuth::None,
            own_number: "+1234567890".to_string(),
            contacts: vec![],
            topic: "/signal/test".to_string(),
            poll_interval_secs: 30,
            timeout_secs: 30,
            use_rest_api: false,
            rest_api_endpoint: None,
        };

        let message = SignalMessage {
            source: "+1111111111".to_string(),
            destination: "+1234567890".to_string(),
            body: "Message with metadata".to_string(),
            timestamp: 1111111111,
            r#type: "message".to_string(),
            metadata: Some(serde_json::json!({"attachment": true})),
        };

        let ink = create_signal_ink(&config, "test", &message);

        assert!(ink.body.contains("Message with metadata"));
    }
}
