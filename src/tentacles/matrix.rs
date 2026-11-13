use anyhow::Result;
use async_trait::async_trait;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info};

use super::Tentacle;
use crate::eventbus::EventBus;
use crate::model::Ink;

/// Authentifizierung f黵 Matrix
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum MatrixAuth {
    #[default]
    None,
    AccessToken {
        access_token: String,
    },
}

fn default_true() -> bool {
    true
}

/// Konfiguration f黵 den Matrix Tentacle
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MatrixTentacleConfig {
    #[serde(default = "default_home_server")]
    pub home_server: String,
    #[serde(default)]
    pub auth: MatrixAuth,
    pub username: String,
    pub rooms: Vec<String>,
    #[serde(default = "default_topic")]
    pub topic: String,
    #[serde(default = "default_poll_interval")]
    pub poll_interval_secs: u64,
    #[serde(default = "default_timeout")]
    pub timeout_secs: u64,
    #[serde(default = "default_true")]
    pub messages_only: bool,
    #[serde(default)]
    pub disable_tls_verification: bool,
}

fn default_home_server() -> String {
    "https://matrix.org".to_string()
}

fn default_topic() -> String {
    "/matrix/message".to_string()
}

fn default_poll_interval() -> u64 {
    30
}

fn default_timeout() -> u64 {
    30
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MatrixMessage {
    pub event_id: String,
    pub room_id: String,
    pub sender: String,
    pub content: serde_json::Value,
    pub r#type: String,
    pub timestamp: u64,
}

pub struct MatrixTentacle {
    name: String,
    config: MatrixTentacleConfig,
}

impl MatrixTentacle {
    pub fn new(name: String, config: MatrixTentacleConfig) -> Self {
        Self { name, config }
    }

    #[allow(dead_code)]
    fn create_message_ink(&self, message: &MatrixMessage, tentacle_name: &str) -> Ink {
        let mut meta_map = serde_json::Map::new();
        meta_map.insert("room_id".into(), serde_json::json!(message.room_id));
        meta_map.insert("sender".into(), serde_json::json!(message.sender));
        meta_map.insert("event_id".into(), serde_json::json!(message.event_id));
        meta_map.insert("message_type".into(), serde_json::json!(message.r#type));
        meta_map.insert("timestamp".into(), serde_json::json!(message.timestamp));

        let body = message
            .content
            .get("body")
            .and_then(|b| b.as_str())
            .unwrap_or("")
            .to_string();

        Ink::new(
            &self.config.topic,
            "matrix",
            tentacle_name,
            format!("[{}] {}: {}", message.room_id, message.sender, body),
        )
        .with_metadata(serde_json::Value::Object(meta_map))
    }
}

#[async_trait]
impl Tentacle for MatrixTentacle {
    fn name(&self) -> &str {
        &self.name
    }

    fn r#type(&self) -> &str {
        "matrix"
    }

    async fn start(&self, event_bus: Arc<EventBus>) -> Result<()> {
        info!(
            "Matrix Tentacle '{}' starting for {} rooms",
            self.name,
            self.config.rooms.len()
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
                        "Matrix Tentacle '{}': failed to build HTTP client: {}",
                        tentacle_name, e
                    );
                    return;
                }
            };

            let mut since_token: Option<String> = None;

            loop {
                for room_id in &config.rooms {
                    match fetch_room_timeline(
                        &client,
                        &config,
                        &tentacle_name,
                        room_id,
                        since_token.as_deref(),
                    )
                    .await
                    {
                        Ok(response) => {
                            for event in response.chunks.unwrap_or_default() {
                                if config.messages_only && event.r#type != "m.room.message" {
                                    continue;
                                }

                                let message = MatrixMessage {
                                    event_id: event.event_id.clone(),
                                    room_id: room_id.clone(),
                                    sender: event.sender.clone(),
                                    content: event.content.clone(),
                                    r#type: event.r#type.clone(),
                                    timestamp: event.origin_server_ts,
                                };

                                let ink = create_matrix_ink(&config, &tentacle_name, &message);
                                event_bus_clone.publish(ink);
                            }

                            if let Some(end) = response.end {
                                since_token = Some(end);
                            }
                        }
                        Err(e) => {
                            error!(
                                "Matrix Tentacle '{}': failed to fetch timeline for {}: {}",
                                tentacle_name, room_id, e
                            );
                        }
                    }
                }

                tokio::time::sleep(Duration::from_secs(config.poll_interval_secs)).await;
            }
        });

        loop {
            tokio::time::sleep(Duration::from_secs(60)).await;
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MatrixTimelineResponse {
    chunks: Option<Vec<MatrixEvent>>,
    #[serde(default)]
    end: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct MatrixEvent {
    event_id: String,
    sender: String,
    content: serde_json::Value,
    #[serde(rename = "type")]
    r#type: String,
    #[serde(rename = "origin_server_ts")]
    origin_server_ts: u64,
}

async fn fetch_room_timeline(
    client: &Client,
    config: &MatrixTentacleConfig,
    tentacle_name: &str,
    room_id: &str,
    since: Option<&str>,
) -> Result<MatrixTimelineResponse> {
    let mut url = format!(
        "{}/_matrix/client/v3/rooms/{}/timeline?limit=50",
        config.home_server.trim_end_matches('/'),
        room_id
    );

    if let Some(s) = since {
        url.push_str(&format!("&since={}", urlencoding::encode(s)));
    }

    debug!(
        "Matrix Tentacle '{}': fetching timeline for {}",
        tentacle_name, room_id
    );

    let mut req = client.get(&url);
    if let MatrixAuth::AccessToken { access_token } = &config.auth {
        req = req.header("Authorization", format!("Bearer {}", access_token));
    }

    let response = req.send().await?;
    if !response.status().is_success() {
        return Err(anyhow::anyhow!(
            "Matrix API request failed with status: {}",
            response.status()
        ));
    }

    let body = response.text().await?;
    let result: MatrixTimelineResponse = serde_json::from_str(&body)?;
    Ok(result)
}

fn create_matrix_ink(
    config: &MatrixTentacleConfig,
    tentacle_name: &str,
    message: &MatrixMessage,
) -> Ink {
    let mut meta_map = serde_json::Map::new();
    meta_map.insert("room_id".into(), serde_json::json!(message.room_id));
    meta_map.insert("sender".into(), serde_json::json!(message.sender));
    meta_map.insert("event_id".into(), serde_json::json!(message.event_id));
    meta_map.insert("message_type".into(), serde_json::json!(message.r#type));
    meta_map.insert("timestamp".into(), serde_json::json!(message.timestamp));

    let body = message
        .content
        .get("body")
        .and_then(|b| b.as_str())
        .unwrap_or("")
        .to_string();

    Ink::new(
        &config.topic,
        "matrix",
        tentacle_name,
        format!("[{}] {}: {}", message.room_id, message.sender, body),
    )
    .with_metadata(serde_json::Value::Object(meta_map))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::appcontext::AppContext;

    #[test]
    fn test_config_defaults() {
        let config: MatrixTentacleConfig = serde_json::from_str(
            r#"{"username": "@user:matrix.org", "rooms": ["!room1:matrix.org"]}"#,
        )
        .unwrap();

        assert_eq!(config.home_server, "https://matrix.org");
        assert_eq!(config.topic, "/matrix/message");
        assert_eq!(config.poll_interval_secs, 30);
        assert_eq!(config.timeout_secs, 30);
        assert!(config.messages_only);
        assert!(!config.disable_tls_verification);
    }

    #[test]
    fn test_auth_access_token() {
        let config: MatrixTentacleConfig = serde_json::from_str(
            r#"{
                "username": "@user:matrix.org",
                "rooms": [],
                "auth": {"type": "accesstoken", "access_token": "secret_token_123"}
            }"#,
        )
        .unwrap();

        match config.auth {
            MatrixAuth::AccessToken { access_token } => {
                assert_eq!(access_token, "secret_token_123");
            }
            _ => panic!("Expected AccessToken auth"),
        }
    }

    #[test]
    fn test_create_message_ink() {
        let tentacle = MatrixTentacle::new(
            "test".to_string(),
            MatrixTentacleConfig {
                home_server: "https://matrix.org".to_string(),
                auth: MatrixAuth::None,
                username: "@user:matrix.org".to_string(),
                rooms: vec!["!room1:matrix.org".to_string()],
                topic: "/matrix/test".to_string(),
                poll_interval_secs: 30,
                timeout_secs: 30,
                messages_only: true,
                disable_tls_verification: false,
            },
        );

        let content = serde_json::json!({
            "body": "Hello, World!",
            "msgtype": "m.text"
        });

        let message = MatrixMessage {
            event_id: "$event123".to_string(),
            room_id: "!room1:matrix.org".to_string(),
            sender: "@alice:matrix.org".to_string(),
            content: content.clone(),
            r#type: "m.room.message".to_string(),
            timestamp: 1234567890,
        };

        let ink = tentacle.create_message_ink(&message, "test");

        assert_eq!(ink.topic, "/matrix/test");
        assert!(ink.body.contains("Hello, World!"));
        assert!(ink.body.contains("@alice:matrix.org"));
    }

    #[test]
    fn test_multiple_rooms() {
        let config: MatrixTentacleConfig = serde_json::from_str(
            r#"{
                "username": "@user:matrix.org",
                "rooms": ["!room1:matrix.org", "!room2:matrix.org", "!room3:matrix.org"]
            }"#,
        )
        .unwrap();

        assert_eq!(config.rooms.len(), 3);
    }

    #[test]
    fn test_custom_home_server() {
        let config: MatrixTentacleConfig = serde_json::from_str(
            r#"{
                "home_server": "https://custom.matrix.server",
                "username": "@user:custom.server",
                "rooms": []
            }"#,
        )
        .unwrap();

        assert_eq!(config.home_server, "https://custom.matrix.server");
    }

    #[test]
    fn test_messages_only_false() {
        let config: MatrixTentacleConfig = serde_json::from_str(
            r#"{
                "username": "@user:matrix.org",
                "rooms": [],
                "messages_only": false
            }"#,
        )
        .unwrap();

        assert!(!config.messages_only);
    }

    #[test]
    fn test_tls_verification_disabled() {
        let config: MatrixTentacleConfig = serde_json::from_str(
            r#"{
                "username": "@user:matrix.org",
                "rooms": [],
                "disable_tls_verification": true
            }"#,
        )
        .unwrap();

        assert!(config.disable_tls_verification);
    }

    #[test]
    fn test_custom_poll_interval() {
        let config: MatrixTentacleConfig = serde_json::from_str(
            r#"{
                "username": "@user:matrix.org",
                "rooms": [],
                "poll_interval_secs": 10,
                "timeout_secs": 60
            }"#,
        )
        .unwrap();

        assert_eq!(config.poll_interval_secs, 10);
        assert_eq!(config.timeout_secs, 60);
    }

    #[test]
    fn test_create_matrix_ink() {
        let config = MatrixTentacleConfig {
            home_server: "https://matrix.org".to_string(),
            auth: MatrixAuth::None,
            username: "@user:matrix.org".to_string(),
            rooms: vec![],
            topic: "/matrix/test".to_string(),
            poll_interval_secs: 30,
            timeout_secs: 30,
            messages_only: true,
            disable_tls_verification: false,
        };

        let message = MatrixMessage {
            event_id: "$evt1".to_string(),
            room_id: "!r1:matrix.org".to_string(),
            sender: "@bob:matrix.org".to_string(),
            content: serde_json::json!({"body": "Test message", "msgtype": "m.text"}),
            r#type: "m.room.message".to_string(),
            timestamp: 9876543210,
        };

        let ink = create_matrix_ink(&config, "test", &message);

        assert_eq!(ink.topic, "/matrix/test");
        assert!(ink.body.contains("Test message"));
        assert!(ink.body.contains("@bob:matrix.org"));
    }

    #[test]
    fn test_auth_none() {
        let config: MatrixTentacleConfig = serde_json::from_str(
            r#"{"username": "@user:matrix.org", "rooms": [], "auth": {"type": "none"}}"#,
        )
        .unwrap();

        assert_eq!(config.auth, MatrixAuth::None);
    }
}
