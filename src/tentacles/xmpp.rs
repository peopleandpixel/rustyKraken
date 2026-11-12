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

/// Authentifizierung für XMPP
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum XmppAuth {
    #[default]
    None,
    /// Passwort-basiert
    Password { username: String, password: String },
    /// REST API Token (für Prosody HTTP API etc.)
    ApiToken { token: String },
}

/// Konfiguration für den XMPP Tentacle
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct XmppTentacleConfig {
    /// XMPP Server-Adresse
    #[serde(default = "default_server")]
    pub server: String,
    /// Server-Port
    #[serde(default = "default_port")]
    pub port: u16,
    /// Authentifizierung
    #[serde(default)]
    pub auth: XmppAuth,
    /// JID (Jabber ID)
    pub jid: String,
    /// MUC-Räume zum Beitreten
    #[serde(default)]
    pub muc_rooms: Vec<String>,
    /// Private Kontakte zum Überwachen
    #[serde(default)]
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
    /// REST API Mode (für Prosody HTTP API)
    #[serde(default)]
    pub use_rest_api: bool,
    /// REST API Endpoint
    #[serde(default)]
    pub rest_api_endpoint: Option<String>,
    /// TLS aktivieren
    #[serde(default)]
    pub tls_enabled: bool,
}

fn default_server() -> String {
    "xmpp.example.com".to_string()
}

fn default_port() -> u16 {
    5222
}

fn default_topic() -> String {
    "/xmpp/message".to_string()
}

fn default_poll_interval() -> u64 {
    30
}

fn default_timeout() -> u64 {
    30
}

/// XMPP-Nachricht
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct XmppMessage {
    pub from: String,
    pub to: String,
    pub body: String,
    pub r#type: String,
    pub timestamp: Option<String>,
}

pub struct XmppTentacle {
    name: String,
    config: XmppTentacleConfig,
}

impl XmppTentacle {
    pub fn new(name: String, config: XmppTentacleConfig) -> Self {
        Self { name, config }
    }

    #[allow(dead_code)]
    fn create_message_ink(&self, message: &XmppMessage, tentacle_name: &str) -> Ink {
        let mut meta_map = serde_json::Map::new();
        meta_map.insert("from".into(), serde_json::json!(message.from));
        meta_map.insert("to".into(), serde_json::json!(message.to));
        meta_map.insert("message_type".into(), serde_json::json!(message.r#type));

        if let Some(ref ts) = message.timestamp {
            meta_map.insert("timestamp".into(), serde_json::json!(ts));
        }

        Ink::new(
            &self.config.topic,
            "xmpp",
            tentacle_name,
            format!("[{}] {}: {}", message.from, message.to, message.body),
        )
        .with_metadata(serde_json::Value::Object(meta_map))
    }
}

#[async_trait]
impl Tentacle for XmppTentacle {
    fn name(&self) -> &str {
        &self.name
    }

    fn r#type(&self) -> &str {
        "xmpp"
    }

    async fn start(&self, event_bus: Arc<EventBus>) -> Result<()> {
        info!(
            "XMPP Tentacle '{}' starting for JID '{}' with {} MUC rooms",
            self.name,
            self.config.jid,
            self.config.muc_rooms.len()
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
                        "XMPP Tentacle '{}': failed to build HTTP client: {}",
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
                                let ink = create_xmpp_ink(&config, &tentacle_name, &message);
                                event_bus_clone.publish(ink);
                            }
                        }
                        Err(e) => {
                            error!(
                                "XMPP Tentacle '{}': failed to fetch messages: {}",
                                tentacle_name, e
                            );
                        }
                    }
                } else {
                    warn!(
                        "XMPP Tentacle '{}': direct XMPP mode requires 'use_rest_api: true'. Use Prosody HTTP API or similar REST wrapper.",
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
    config: &XmppTentacleConfig,
    tentacle_name: &str,
) -> Result<Vec<XmppMessage>> {
    let endpoint = config
        .rest_api_endpoint
        .as_deref()
        .unwrap_or("/httpapi/messages");
    let url = format!("{}{}", config.server.trim_end_matches('/'), endpoint);

    debug!(
        "XMPP Tentacle '{}': fetching messages from {}",
        tentacle_name, url
    );

    let mut req = client.get(&url);
    if let XmppAuth::ApiToken { token } = &config.auth {
        req = req.header("Authorization", format!("Bearer {}", token));
    }

    let response = req.send().await?;
    if !response.status().is_success() {
        return Err(anyhow::anyhow!(
            "XMPP REST API request failed with status: {}",
            response.status()
        ));
    }

    let body = response.text().await?;

    if let Ok(messages) = serde_json::from_str::<Vec<serde_json::Value>>(&body) {
        let mut result = Vec::new();
        for m in messages {
            let from = m
                .get("from")
                .and_then(|f| f.as_str())
                .unwrap_or("unknown")
                .to_string();
            let to = m
                .get("to")
                .and_then(|t| t.as_str())
                .unwrap_or(&config.jid)
                .to_string();
            let body = m
                .get("body")
                .and_then(|b| b.as_str())
                .unwrap_or("")
                .to_string();
            let r#type = m
                .get("type")
                .and_then(|t| t.as_str())
                .unwrap_or("chat")
                .to_string();
            let timestamp = m
                .get("timestamp")
                .and_then(|t| t.as_str())
                .map(|s| s.to_string());

            result.push(XmppMessage {
                from,
                to,
                body,
                r#type,
                timestamp,
            });
        }
        debug!(
            "XMPP Tentacle '{}': fetched {} messages",
            tentacle_name,
            result.len()
        );
        return Ok(result);
    }

    Ok(Vec::new())
}

fn create_xmpp_ink(config: &XmppTentacleConfig, tentacle_name: &str, message: &XmppMessage) -> Ink {
    let mut meta_map = serde_json::Map::new();
    meta_map.insert("from".into(), serde_json::json!(message.from));
    meta_map.insert("to".into(), serde_json::json!(message.to));
    meta_map.insert("message_type".into(), serde_json::json!(message.r#type));

    if let Some(ref ts) = message.timestamp {
        meta_map.insert("timestamp".into(), serde_json::json!(ts));
    }

    Ink::new(
        &config.topic,
        "xmpp",
        tentacle_name,
        format!("[{}] {}: {}", message.from, message.to, message.body),
    )
    .with_metadata(serde_json::Value::Object(meta_map))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_defaults() {
        let config: XmppTentacleConfig =
            serde_json::from_str(r#"{"jid": "user@example.com"}"#).unwrap();

        assert_eq!(config.server, "xmpp.example.com");
        assert_eq!(config.port, 5222);
        assert_eq!(config.topic, "/xmpp/message");
        assert_eq!(config.poll_interval_secs, 30);
        assert_eq!(config.timeout_secs, 30);
        assert!(!config.use_rest_api);
        assert!(!config.tls_enabled);
    }

    #[test]
    fn test_auth_password() {
        let config: XmppTentacleConfig = serde_json::from_str(
            r#"{
                "jid": "user@example.com",
                "auth": {"type": "password", "username": "user", "password": "secret"}
            }"#,
        )
        .unwrap();

        match config.auth {
            XmppAuth::Password { username, password } => {
                assert_eq!(username, "user");
                assert_eq!(password, "secret");
            }
            _ => panic!("Expected Password auth"),
        }
    }

    #[test]
    fn test_auth_api_token() {
        let config: XmppTentacleConfig = serde_json::from_str(
            r#"{
                "jid": "user@example.com",
                "auth": {"type": "apitoken", "token": "token123"}
            }"#,
        )
        .unwrap();

        match config.auth {
            XmppAuth::ApiToken { token } => {
                assert_eq!(token, "token123");
            }
            _ => panic!("Expected ApiToken auth"),
        }
    }

    #[test]
    fn test_create_message_ink() {
        let tentacle = XmppTentacle::new(
            "test".to_string(),
            XmppTentacleConfig {
                server: "xmpp.example.com".to_string(),
                port: 5222,
                auth: XmppAuth::None,
                jid: "user@example.com".to_string(),
                muc_rooms: vec!["room@example.com".to_string()],
                contacts: vec![],
                topic: "/xmpp/test".to_string(),
                poll_interval_secs: 30,
                timeout_secs: 30,
                use_rest_api: false,
                rest_api_endpoint: None,
                tls_enabled: false,
            },
        );

        let message = XmppMessage {
            from: "alice@example.com".to_string(),
            to: "user@example.com".to_string(),
            body: "Hello via XMPP!".to_string(),
            r#type: "chat".to_string(),
            timestamp: Some("2024-01-01T00:00:00Z".to_string()),
        };

        let ink = tentacle.create_message_ink(&message, "test");

        assert_eq!(ink.topic, "/xmpp/test");
        assert!(ink.body.contains("Hello via XMPP!"));
        assert!(ink.body.contains("alice@example.com"));
    }

    #[test]
    fn test_muc_rooms() {
        let config: XmppTentacleConfig = serde_json::from_str(
            r#"{
                "jid": "user@example.com",
                "muc_rooms": ["room1@example.com", "room2@example.com"]
            }"#,
        )
        .unwrap();

        assert_eq!(config.muc_rooms.len(), 2);
    }

    #[test]
    fn test_contacts() {
        let config: XmppTentacleConfig = serde_json::from_str(
            r#"{
                "jid": "user@example.com",
                "contacts": ["alice@example.com", "bob@example.com"]
            }"#,
        )
        .unwrap();

        assert_eq!(config.contacts.len(), 2);
    }

    #[test]
    fn test_rest_api_mode() {
        let config: XmppTentacleConfig = serde_json::from_str(
            r#"{
                "jid": "user@example.com",
                "use_rest_api": true,
                "rest_api_endpoint": "/api/messages"
            }"#,
        )
        .unwrap();

        assert!(config.use_rest_api);
        assert_eq!(config.rest_api_endpoint, Some("/api/messages".to_string()));
    }

    #[test]
    fn test_tls_config() {
        let config: XmppTentacleConfig = serde_json::from_str(
            r#"{"jid": "user@example.com", "tls_enabled": true, "port": 5223}"#,
        )
        .unwrap();

        assert!(config.tls_enabled);
        assert_eq!(config.port, 5223);
    }

    #[test]
    fn test_create_xmpp_ink() {
        let config = XmppTentacleConfig {
            server: "xmpp.example.com".to_string(),
            port: 5222,
            auth: XmppAuth::None,
            jid: "user@example.com".to_string(),
            muc_rooms: vec![],
            contacts: vec![],
            topic: "/xmpp/test".to_string(),
            poll_interval_secs: 30,
            timeout_secs: 30,
            use_rest_api: false,
            rest_api_endpoint: None,
            tls_enabled: false,
        };

        let message = XmppMessage {
            from: "bob@example.com".to_string(),
            to: "user@example.com".to_string(),
            body: "Test message".to_string(),
            r#type: "chat".to_string(),
            timestamp: None,
        };

        let ink = create_xmpp_ink(&config, "test", &message);
        assert_eq!(ink.topic, "/xmpp/test");
        assert!(ink.body.contains("Test message"));
    }

    #[test]
    fn test_auth_none() {
        let config: XmppTentacleConfig =
            serde_json::from_str(r#"{"jid": "user@example.com", "auth": {"type": "none"}}"#)
                .unwrap();

        assert_eq!(config.auth, XmppAuth::None);
    }

    #[test]
    fn test_custom_server() {
        let config: XmppTentacleConfig = serde_json::from_str(
            r#"{"server": "jabber.org", "port": 5222, "jid": "user@jabber.org"}"#,
        )
        .unwrap();

        assert_eq!(config.server, "jabber.org");
    }

    #[test]
    fn test_custom_poll_interval() {
        let config: XmppTentacleConfig = serde_json::from_str(
            r#"{
                "jid": "user@example.com",
                "poll_interval_secs": 10,
                "timeout_secs": 60
            }"#,
        )
        .unwrap();

        assert_eq!(config.poll_interval_secs, 10);
        assert_eq!(config.timeout_secs, 60);
    }
}
