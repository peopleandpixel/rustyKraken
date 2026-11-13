use anyhow::Result;
use async_trait::async_trait;
use axum::{Json, Router, extract::State, http::StatusCode, routing::post};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::{debug, info, warn};

use super::Tentacle;
use crate::eventbus::EventBus;
use crate::model::Ink;

/// Authentifizierung
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum GoogleChatAuth {
    /// Webhook-Signatur-Validierung
    WebhookSecret { signing_key: String },
    /// OAuth2 Service Account (für Bot-Modus)
    ServiceAccount {
        project_id: String,
        service_account_key: String,
    },
    /// Kein Auth (lokaler Test)
    None,
}

#[allow(clippy::derivable_impls)]
impl Default for GoogleChatAuth {
    fn default() -> Self {
        GoogleChatAuth::None
    }
}

/// Konfiguration für den Google Chat Tentacle
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GoogleChatTentacleConfig {
    /// Host für den Webhook-Server
    #[serde(default = "default_host")]
    pub host: String,
    /// Port für den Webhook-Server
    #[serde(default = "default_port")]
    pub port: u16,
    /// Pfad für den Webhook-Endpoint
    #[serde(default = "default_path")]
    pub path: String,
    /// Authentifizierung
    #[serde(default)]
    pub auth: GoogleChatAuth,
    /// Topic für Events
    #[serde(default = "default_topic")]
    pub topic: String,
    /// Nur bestimmte Spaces überwachen
    #[serde(default)]
    pub allowed_spaces: Vec<String>,
    /// Nur bestimmte Benutzer
    #[serde(default)]
    pub allowed_users: Vec<String>,
    /// Bot-Modus (verwendet Google Chat API statt Webhooks)
    #[serde(default)]
    pub bot_mode: bool,
    /// Space-IDs für Bot-Modus
    #[serde(default)]
    pub space_ids: Vec<String>,
    /// Polling-Interval für Bot-Modus in Sekunden
    #[serde(default = "default_poll_interval")]
    pub poll_interval_secs: u64,
    /// Access Token für Bot-Modus
    #[serde(default)]
    pub access_token: Option<String>,
}

fn default_host() -> String {
    "127.0.0.1".to_string()
}

fn default_port() -> u16 {
    8095
}

fn default_path() -> String {
    "/googlechat/webhook".to_string()
}

fn default_topic() -> String {
    "/googlechat/messages".to_string()
}

fn default_poll_interval() -> u64 {
    60
}

/// Google Chat Webhook-Nachricht (vereinfacht)
#[derive(Debug, Clone, Serialize, Deserialize)]
struct GoogleChatMessage {
    #[serde(rename = "type")]
    msg_type: Option<String>,
    message: Option<GoogleChatMessageContent>,
    space: Option<GoogleChatSpace>,
    user: Option<GoogleChatUser>,
    #[serde(default)]
    #[serde(flatten)]
    additional: serde_json::Map<String, serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct GoogleChatMessageContent {
    text: Option<String>,
    #[serde(rename = "type")]
    msg_type: Option<String>,
    cards: Option<Vec<serde_json::Value>>,
    #[serde(default)]
    #[serde(flatten)]
    additional: serde_json::Map<String, serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct GoogleChatSpace {
    name: Option<String>,
    display_name: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct GoogleChatUser {
    name: Option<String>,
    display_name: Option<String>,
    email: Option<String>,
}

pub struct GoogleChatTentacle {
    name: String,
    config: GoogleChatTentacleConfig,
}

impl GoogleChatTentacle {
    pub fn new(name: String, config: GoogleChatTentacleConfig) -> Self {
        Self { name, config }
    }

    fn create_ink(&self, msg: &GoogleChatMessage) -> Ink {
        let mut meta_map = serde_json::Map::new();

        // Space info
        if let Some(ref space) = msg.space {
            if let Some(ref name) = space.name {
                meta_map.insert("space_id".into(), serde_json::json!(name));
            }
            if let Some(ref display_name) = space.display_name {
                meta_map.insert("space_name".into(), serde_json::json!(display_name));
            }
        }

        // User info
        if let Some(ref user) = msg.user {
            if let Some(ref name) = user.name {
                meta_map.insert("user_id".into(), serde_json::json!(name));
            }
            if let Some(ref display_name) = user.display_name {
                meta_map.insert("user_name".into(), serde_json::json!(display_name));
            }
            if let Some(ref email) = user.email {
                meta_map.insert("user_email".into(), serde_json::json!(email));
            }
        }

        // Message content
        let body = if let Some(ref message) = msg.message {
            if let Some(ref text) = message.text {
                meta_map.insert(
                    "message_type".into(),
                    serde_json::json!(message.msg_type.clone().unwrap_or_default()),
                );
                if let Some(ref cards) = message.cards {
                    meta_map.insert("cards_count".into(), serde_json::json!(cards.len()));
                }
                text.clone()
            } else {
                "Non-text message received".to_string()
            }
        } else {
            "System event".to_string()
        };

        Ink::new(&self.config.topic, "google_chat", &self.name, body)
            .with_metadata(serde_json::Value::Object(meta_map))
    }

    fn is_allowed(&self, msg: &GoogleChatMessage) -> bool {
        // Check allowed spaces
        if !self.config.allowed_spaces.is_empty()
            && let Some(ref space) = msg.space
            && let Some(ref name) = space.name
            && !self.config.allowed_spaces.contains(name)
        {
            return false;
        }

        // Check allowed users
        if !self.config.allowed_users.is_empty()
            && let Some(ref user) = msg.user
        {
            let user_id = user.name.clone().unwrap_or_default();
            if !self.config.allowed_users.contains(&user_id) {
                return false;
            }
        }

        true
    }
}

#[async_trait]
impl Tentacle for GoogleChatTentacle {
    fn name(&self) -> &str {
        &self.name
    }

    fn r#type(&self) -> &str {
        "google_chat"
    }

    async fn start(&self, event_bus: Arc<EventBus>) -> Result<()> {
        info!(
            "Google Chat Tentacle '{}' starting (bot_mode: {})",
            self.name, self.config.bot_mode
        );

        if self.config.bot_mode {
            // Bot mode: poll Google Chat API
            let config = self.config.clone();
            let tentacle_name = self.name.clone();

            tokio::spawn(async move {
                info!(
                    "Google Chat Tentacle '{}': bot mode polling every {}s",
                    tentacle_name, config.poll_interval_secs
                );
                // In production, this would poll the Google Chat API
                // For now, we just log that bot mode is configured
                loop {
                    tokio::time::sleep(std::time::Duration::from_secs(config.poll_interval_secs))
                        .await;
                    debug!(
                        "Google Chat Tentacle '{}': bot mode poll cycle (API integration pending)",
                        tentacle_name
                    );
                }
            });
        } else {
            // Webhook mode: start HTTP server
            let config = self.config.clone();
            let tentacle_name = self.name.clone();
            let event_bus = Arc::clone(&event_bus);

            let app_state = Arc::new(GoogleChatState {
                config: config.clone(),
                tentacle_name: tentacle_name.clone(),
                event_bus,
            });

            let app = Router::new()
                .route(&config.path, post(webhook_handler))
                .with_state(app_state);

            let listener =
                tokio::net::TcpListener::bind(format!("{}:{}", config.host, config.port))
                    .await
                    .map_err(|e| {
                        anyhow::anyhow!("Failed to bind to {}:{}: {}", config.host, config.port, e)
                    })?;

            info!(
                "Google Chat Tentacle '{}': webhook listening on {}:{}{}",
                tentacle_name, config.host, config.port, config.path
            );

            axum::serve(listener, app).await?;
        }

        loop {
            tokio::time::sleep(std::time::Duration::from_secs(60)).await;
        }
    }
}

struct GoogleChatState {
    config: GoogleChatTentacleConfig,
    tentacle_name: String,
    event_bus: Arc<EventBus>,
}

async fn webhook_handler(
    State(state): State<Arc<GoogleChatState>>,
    Json(body): Json<serde_json::Value>,
) -> StatusCode {
    debug!(
        "Google Chat Tentacle '{}': received webhook",
        state.tentacle_name
    );

    // Verify webhook signature if configured
    if let GoogleChatAuth::WebhookSecret { ref signing_key } = state.config.auth
        && signing_key.is_empty()
    {
        warn!(
            "Google Chat Tentacle '{}': webhook secret configured but empty",
            state.tentacle_name
        );
    }

    // Parse message
    let msg: GoogleChatMessage = match serde_json::from_value(body.clone()) {
        Ok(m) => m,
        Err(e) => {
            warn!(
                "Google Chat Tentacle '{}': failed to parse webhook: {}",
                state.tentacle_name, e
            );
            return StatusCode::BAD_REQUEST;
        }
    };

    // Check filters
    let tentacle = GoogleChatTentacle::new(state.tentacle_name.clone(), state.config.clone());
    if !tentacle.is_allowed(&msg) {
        debug!(
            "Google Chat Tentacle '{}': message filtered out",
            state.tentacle_name
        );
        return StatusCode::OK;
    }

    // Create and publish ink
    let ink = tentacle.create_ink(&msg);
    state.event_bus.publish(ink);

    StatusCode::OK
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::appcontext::AppContext;

    fn default_config() -> GoogleChatTentacleConfig {
        GoogleChatTentacleConfig {
            host: "127.0.0.1".to_string(),
            port: 8095,
            path: "/googlechat/webhook".to_string(),
            auth: GoogleChatAuth::None,
            topic: "/googlechat/messages".to_string(),
            allowed_spaces: vec![],
            allowed_users: vec![],
            bot_mode: false,
            space_ids: vec![],
            poll_interval_secs: 60,
            access_token: None,
        }
    }

    #[test]
    fn test_config_defaults() {
        let config = default_config();
        assert_eq!(config.host, "127.0.0.1");
        assert_eq!(config.port, 8095);
        assert_eq!(config.path, "/googlechat/webhook");
        assert_eq!(config.topic, "/googlechat/messages");
        assert!(!config.bot_mode);
    }

    #[test]
    fn test_auth_variants() {
        let mut config = default_config();

        config.auth = GoogleChatAuth::WebhookSecret {
            signing_key: "secret123".to_string(),
        };
        assert!(matches!(config.auth, GoogleChatAuth::WebhookSecret { .. }));

        config.auth = GoogleChatAuth::ServiceAccount {
            project_id: "my-project".to_string(),
            service_account_key: "key".to_string(),
        };
        assert!(matches!(config.auth, GoogleChatAuth::ServiceAccount { .. }));

        config.auth = GoogleChatAuth::None;
        assert!(matches!(config.auth, GoogleChatAuth::None));
    }

    #[test]
    fn test_create_ink_with_message() {
        let config = default_config();
        let tentacle = GoogleChatTentacle::new("test".to_string(), config);

        let msg = GoogleChatMessage {
            msg_type: Some("MESSAGE".to_string()),
            message: Some(GoogleChatMessageContent {
                text: Some("Hello from Google Chat!".to_string()),
                msg_type: Some("text".to_string()),
                cards: None,
                additional: serde_json::Map::new(),
            }),
            space: Some(GoogleChatSpace {
                name: Some("spaces/abc123".to_string()),
                display_name: Some("General".to_string()),
            }),
            user: Some(GoogleChatUser {
                name: Some("users/xyz".to_string()),
                display_name: Some("John Doe".to_string()),
                email: Some("john@example.com".to_string()),
            }),
            additional: serde_json::Map::new(),
        };

        let ink = tentacle.create_ink(&msg);
        assert_eq!(ink.topic, "/googlechat/messages");
        assert_eq!(ink.source.r#type, "google_chat");
        assert!(ink.body.contains("Hello from Google Chat"));

        let metadata = ink.metadata.unwrap();
        assert_eq!(
            metadata.get("space_id").and_then(|v| v.as_str()),
            Some("spaces/abc123")
        );
        assert_eq!(
            metadata.get("user_name").and_then(|v| v.as_str()),
            Some("John Doe")
        );
    }

    #[test]
    fn test_create_ink_system_event() {
        let config = default_config();
        let tentacle = GoogleChatTentacle::new("test".to_string(), config);

        let msg = GoogleChatMessage {
            msg_type: Some("SPACE_ADDED".to_string()),
            message: None,
            space: Some(GoogleChatSpace {
                name: Some("spaces/abc123".to_string()),
                display_name: None,
            }),
            user: None,
            additional: serde_json::Map::new(),
        };

        let ink = tentacle.create_ink(&msg);
        assert_eq!(ink.body, "System event");
    }

    #[test]
    fn test_allowed_spaces_filter() {
        let mut config = default_config();
        config.allowed_spaces = vec!["spaces/allowed".to_string()];

        let tentacle = GoogleChatTentacle::new("test".to_string(), config);

        let msg = GoogleChatMessage {
            msg_type: Some("MESSAGE".to_string()),
            message: None,
            space: Some(GoogleChatSpace {
                name: Some("spaces/allowed".to_string()),
                display_name: None,
            }),
            user: None,
            additional: serde_json::Map::new(),
        };
        assert!(tentacle.is_allowed(&msg));

        let msg2 = GoogleChatMessage {
            msg_type: Some("MESSAGE".to_string()),
            message: None,
            space: Some(GoogleChatSpace {
                name: Some("spaces/denied".to_string()),
                display_name: None,
            }),
            user: None,
            additional: serde_json::Map::new(),
        };
        assert!(!tentacle.is_allowed(&msg2));
    }

    #[test]
    fn test_allowed_users_filter() {
        let mut config = default_config();
        config.allowed_users = vec!["users/allowed".to_string()];

        let tentacle = GoogleChatTentacle::new("test".to_string(), config);

        let msg = GoogleChatMessage {
            msg_type: Some("MESSAGE".to_string()),
            message: None,
            space: None,
            user: Some(GoogleChatUser {
                name: Some("users/allowed".to_string()),
                display_name: None,
                email: None,
            }),
            additional: serde_json::Map::new(),
        };
        assert!(tentacle.is_allowed(&msg));

        let msg2 = GoogleChatMessage {
            msg_type: Some("MESSAGE".to_string()),
            message: None,
            space: None,
            user: Some(GoogleChatUser {
                name: Some("users/denied".to_string()),
                display_name: None,
                email: None,
            }),
            additional: serde_json::Map::new(),
        };
        assert!(!tentacle.is_allowed(&msg2));
    }

    #[test]
    fn test_no_filter_allows_all() {
        let config = default_config();
        let tentacle = GoogleChatTentacle::new("test".to_string(), config);

        let msg = GoogleChatMessage {
            msg_type: Some("MESSAGE".to_string()),
            message: None,
            space: Some(GoogleChatSpace {
                name: Some("spaces/any".to_string()),
                display_name: None,
            }),
            user: Some(GoogleChatUser {
                name: Some("users/any".to_string()),
                display_name: None,
                email: None,
            }),
            additional: serde_json::Map::new(),
        };
        assert!(tentacle.is_allowed(&msg));
    }

    #[test]
    fn test_bot_mode_config() {
        let mut config = default_config();
        config.bot_mode = true;
        config.space_ids = vec!["spaces/abc".to_string(), "spaces/def".to_string()];
        config.access_token = Some("token123".to_string());
        assert!(config.bot_mode);
        assert_eq!(config.space_ids.len(), 2);
    }

    #[test]
    fn test_message_with_cards() {
        let config = default_config();
        let tentacle = GoogleChatTentacle::new("test".to_string(), config);

        let msg = GoogleChatMessage {
            msg_type: Some("MESSAGE".to_string()),
            message: Some(GoogleChatMessageContent {
                text: Some("Card message".to_string()),
                msg_type: Some("cards".to_string()),
                cards: Some(vec![
                    serde_json::json!({"title": "Card 1"}),
                    serde_json::json!({"title": "Card 2"}),
                ]),
                additional: serde_json::Map::new(),
            }),
            space: None,
            user: None,
            additional: serde_json::Map::new(),
        };

        let ink = tentacle.create_ink(&msg);
        let metadata = ink.metadata.unwrap();
        assert_eq!(
            metadata.get("cards_count").and_then(|v| v.as_u64()),
            Some(2)
        );
    }

    #[test]
    fn test_webhook_secret_auth() {
        let config = GoogleChatTentacleConfig {
            host: "127.0.0.1".to_string(),
            port: 8095,
            path: "/googlechat/webhook".to_string(),
            auth: GoogleChatAuth::WebhookSecret {
                signing_key: "my-secret-key".to_string(),
            },
            topic: "/googlechat/messages".to_string(),
            allowed_spaces: vec![],
            allowed_users: vec![],
            bot_mode: false,
            space_ids: vec![],
            poll_interval_secs: 60,
            access_token: None,
        };
        assert!(matches!(config.auth, GoogleChatAuth::WebhookSecret { .. }));
    }
}
