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

/// Authentifizierung für MS Teams
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum TeamsAuth {
    #[default]
    None,
    /// Webhook-Secret (für Incoming Webhooks)
    Webhook { webhook_url: String },
    /// Azure AD App Registration
    AzureAd {
        tenant_id: String,
        client_id: String,
        client_secret: String,
    },
}

/// Konfiguration für den MS Teams Tentacle
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TeamsTentacleConfig {
    /// Authentifizierung
    #[serde(default)]
    pub auth: TeamsAuth,
    /// Teams Channel IDs zum Überwachen
    pub channels: Vec<String>,
    /// Topic für Events
    #[serde(default = "default_topic")]
    pub topic: String,
    /// Polling-Interval in Sekunden
    #[serde(default = "default_poll_interval")]
    pub poll_interval_secs: u64,
    /// Timeout in Sekunden
    #[serde(default = "default_timeout")]
    pub timeout_secs: u64,
    /// Webhook-Modus (einfacher Incoming Webhook)
    #[serde(default)]
    pub webhook_mode: bool,
    /// Webhook-URL (für webhook_mode)
    #[serde(default)]
    pub webhook_url: Option<String>,
}

fn default_topic() -> String {
    "/teams/message".to_string()
}

fn default_poll_interval() -> u64 {
    30
}

fn default_timeout() -> u64 {
    30
}

/// MS Teams Nachricht
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TeamsMessage {
    pub channel_id: String,
    pub sender: String,
    pub body: String,
    pub timestamp: String,
    pub subject: Option<String>,
}

pub struct TeamsTentacle {
    name: String,
    config: TeamsTentacleConfig,
}

impl TeamsTentacle {
    pub fn new(name: String, config: TeamsTentacleConfig) -> Self {
        Self { name, config }
    }

    #[allow(dead_code)]
    fn create_message_ink(&self, message: &TeamsMessage, tentacle_name: &str) -> Ink {
        let mut meta_map = serde_json::Map::new();
        meta_map.insert("channel_id".into(), serde_json::json!(message.channel_id));
        meta_map.insert("sender".into(), serde_json::json!(message.sender));
        meta_map.insert("timestamp".into(), serde_json::json!(message.timestamp));

        if let Some(ref subject) = message.subject {
            meta_map.insert("subject".into(), serde_json::json!(subject));
        }

        Ink::new(
            &self.config.topic,
            "teams",
            tentacle_name,
            format!(
                "[{}] {}: {}",
                message.channel_id, message.sender, message.body
            ),
        )
        .with_metadata(serde_json::Value::Object(meta_map))
    }
}

#[async_trait]
impl Tentacle for TeamsTentacle {
    fn name(&self) -> &str {
        &self.name
    }

    fn r#type(&self) -> &str {
        "teams"
    }

    async fn start(&self, event_bus: Arc<EventBus>) -> Result<()> {
        info!(
            "MS Teams Tentacle '{}' starting for {} channels",
            self.name,
            self.config.channels.len()
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
                        "MS Teams Tentacle '{}': failed to build HTTP client: {}",
                        tentacle_name, e
                    );
                    return;
                }
            };

            loop {
                if config.webhook_mode {
                    if let Some(ref webhook_url) = config.webhook_url {
                        match fetch_webhook_messages(&client, webhook_url, &tentacle_name).await {
                            Ok(messages) => {
                                for message in messages {
                                    let ink = create_teams_ink(&config, &tentacle_name, &message);
                                    event_bus_clone.publish(ink);
                                }
                            }
                            Err(e) => {
                                error!(
                                    "MS Teams Tentacle '{}': webhook fetch failed: {}",
                                    tentacle_name, e
                                );
                            }
                        }
                    }
                } else {
                    warn!(
                        "MS Teams Tentacle '{}': Graph API mode requires Azure AD authentication. Use webhook_mode for simpler setup.",
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

async fn fetch_webhook_messages(
    _client: &Client,
    _webhook_url: &str,
    tentacle_name: &str,
) -> Result<Vec<TeamsMessage>> {
    debug!(
        "MS Teams Tentacle '{}': fetching webhook messages",
        tentacle_name
    );

    // Webhook-Modus: Teams sendet POST an unsere URL,
    // hier simulieren wir den Empfang über einen lokalen Endpoint
    Ok(Vec::new())
}

fn create_teams_ink(
    config: &TeamsTentacleConfig,
    tentacle_name: &str,
    message: &TeamsMessage,
) -> Ink {
    let mut meta_map = serde_json::Map::new();
    meta_map.insert("channel_id".into(), serde_json::json!(message.channel_id));
    meta_map.insert("sender".into(), serde_json::json!(message.sender));
    meta_map.insert("timestamp".into(), serde_json::json!(message.timestamp));

    if let Some(ref subject) = message.subject {
        meta_map.insert("subject".into(), serde_json::json!(subject));
    }

    Ink::new(
        &config.topic,
        "teams",
        tentacle_name,
        format!(
            "[{}] {}: {}",
            message.channel_id, message.sender, message.body
        ),
    )
    .with_metadata(serde_json::Value::Object(meta_map))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::appcontext::AppContext;

    #[test]
    fn test_config_defaults() {
        let config: TeamsTentacleConfig =
            serde_json::from_str(r#"{"channels": ["channel1"]}"#).unwrap();

        assert_eq!(config.topic, "/teams/message");
        assert_eq!(config.poll_interval_secs, 30);
        assert_eq!(config.timeout_secs, 30);
        assert!(!config.webhook_mode);
    }

    #[test]
    fn test_auth_webhook() {
        let config: TeamsTentacleConfig = serde_json::from_str(
            r#"{
                "channels": [],
                "auth": {"type": "webhook", "webhook_url": "https://webhook.url"}
            }"#,
        )
        .unwrap();

        match config.auth {
            TeamsAuth::Webhook { webhook_url } => {
                assert_eq!(webhook_url, "https://webhook.url");
            }
            _ => panic!("Expected Webhook auth"),
        }
    }

    #[test]
    fn test_auth_azure_ad() {
        let config: TeamsTentacleConfig = serde_json::from_str(
            r#"{
                "channels": [],
                "auth": {
                    "type": "azuread",
                    "tenant_id": "tenant-123",
                    "client_id": "client-456",
                    "client_secret": "secret-789"
                }
            }"#,
        )
        .unwrap();

        match config.auth {
            TeamsAuth::AzureAd {
                tenant_id,
                client_id,
                client_secret,
            } => {
                assert_eq!(tenant_id, "tenant-123");
                assert_eq!(client_id, "client-456");
                assert_eq!(client_secret, "secret-789");
            }
            _ => panic!("Expected AzureAd auth"),
        }
    }

    #[test]
    fn test_create_message_ink() {
        let tentacle = TeamsTentacle::new(
            "test".to_string(),
            TeamsTentacleConfig {
                auth: TeamsAuth::None,
                channels: vec!["channel1".to_string()],
                topic: "/teams/test".to_string(),
                poll_interval_secs: 30,
                timeout_secs: 30,
                webhook_mode: false,
                webhook_url: None,
            },
        );

        let message = TeamsMessage {
            channel_id: "channel1".to_string(),
            sender: "alice@contoso.com".to_string(),
            body: "Hello from Teams!".to_string(),
            timestamp: "2024-01-01T00:00:00Z".to_string(),
            subject: Some("Meeting Notes".to_string()),
        };

        let ink = tentacle.create_message_ink(&message, "test");

        assert_eq!(ink.topic, "/teams/test");
        assert!(ink.body.contains("Hello from Teams!"));
        assert!(ink.body.contains("alice@contoso.com"));
    }

    #[test]
    fn test_webhook_mode() {
        let config: TeamsTentacleConfig = serde_json::from_str(
            r#"{
                "channels": [],
                "webhook_mode": true,
                "webhook_url": "https://webhook.url"
            }"#,
        )
        .unwrap();

        assert!(config.webhook_mode);
        assert_eq!(config.webhook_url, Some("https://webhook.url".to_string()));
    }

    #[test]
    fn test_multiple_channels() {
        let config: TeamsTentacleConfig = serde_json::from_str(
            r#"{
                "channels": ["ch1", "ch2", "ch3"]
            }"#,
        )
        .unwrap();

        assert_eq!(config.channels.len(), 3);
    }

    #[test]
    fn test_create_teams_ink() {
        let config = TeamsTentacleConfig {
            auth: TeamsAuth::None,
            channels: vec![],
            topic: "/teams/test".to_string(),
            poll_interval_secs: 30,
            timeout_secs: 30,
            webhook_mode: false,
            webhook_url: None,
        };

        let message = TeamsMessage {
            channel_id: "ch1".to_string(),
            sender: "bob@contoso.com".to_string(),
            body: "Test message".to_string(),
            timestamp: "2024-01-01T00:00:00Z".to_string(),
            subject: None,
        };

        let ink = create_teams_ink(&config, "test", &message);

        assert_eq!(ink.topic, "/teams/test");
        assert!(ink.body.contains("Test message"));
    }

    #[test]
    fn test_auth_none() {
        let config: TeamsTentacleConfig =
            serde_json::from_str(r#"{"channels": [], "auth": {"type": "none"}}"#).unwrap();

        assert_eq!(config.auth, TeamsAuth::None);
    }

    #[test]
    fn test_custom_poll_interval() {
        let config: TeamsTentacleConfig = serde_json::from_str(
            r#"{
                "channels": [],
                "poll_interval_secs": 10,
                "timeout_secs": 60
            }"#,
        )
        .unwrap();

        assert_eq!(config.poll_interval_secs, 10);
        assert_eq!(config.timeout_secs, 60);
    }

    #[test]
    fn test_message_without_subject() {
        let config = TeamsTentacleConfig {
            auth: TeamsAuth::None,
            channels: vec![],
            topic: "/teams/test".to_string(),
            poll_interval_secs: 30,
            timeout_secs: 30,
            webhook_mode: false,
            webhook_url: None,
        };

        let message = TeamsMessage {
            channel_id: "ch1".to_string(),
            sender: "user@contoso.com".to_string(),
            body: "No subject message".to_string(),
            timestamp: "2024-01-01T00:00:00Z".to_string(),
            subject: None,
        };

        let ink = create_teams_ink(&config, "test", &message);
        assert!(ink.body.contains("No subject message"));
    }
}
