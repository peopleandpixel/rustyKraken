use anyhow::{Context, Result};
use async_trait::async_trait;
use axum::Router;
use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::post;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, error, info, warn};

use super::Tentacle;
use crate::eventbus::EventBus;
use crate::model::Ink;

/// Konfiguration für Slack Tentacle
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SlackTentacleConfig {
    /// Slack Signing Secret (aus Slack App Konfiguration)
    pub signing_secret: String,
    /// Bot Token (optional, für API-Aufrufe)
    #[serde(default)]
    pub bot_token: Option<String>,
    /// App Token (optional, für Event Subscription API)
    #[serde(default)]
    pub app_token: Option<String>,
    /// Host, auf dem der Webhook-Server läuft (Standard: "0.0.0.0")
    #[serde(default = "default_host")]
    pub host: String,
    /// Port, auf dem der Webhook-Server hört (Standard: 9880)
    #[serde(default = "default_port")]
    pub port: u16,
    /// Webhook-Pfad (Standard: "/slack/events")
    #[serde(default = "default_path")]
    pub path: String,
    /// Fallback Topic wenn kein Topic aus Channel oder Nachricht erkannt wird
    #[serde(default = "default_fallback_topic")]
    pub fallback_topic: String,
    /// Topic-Modus: "channel" (aus Channel-Name), "content" (aus Nachrichteninhalt), "all" (kombiniert)
    #[serde(default = "default_topic_mode")]
    pub topic_mode: String,
    /// Optional: Mapping von Channel-IDs auf Topics
    #[serde(default)]
    pub channel_topic_mapping: HashMap<String, String>,
    /// Optional: Erlaubte Channels (leer = alle)
    #[serde(default)]
    pub allowed_channels: Vec<String>,
    /// Optional: Erlaubte Users (leer = alle)
    #[serde(default)]
    pub allowed_users: Vec<String>,
}

fn default_host() -> String {
    "0.0.0.0".to_string()
}

fn default_port() -> u16 {
    9880
}

fn default_path() -> String {
    "/slack/events".to_string()
}

fn default_fallback_topic() -> String {
    "/slack/default".to_string()
}

fn default_topic_mode() -> String {
    "all".to_string()
}

pub struct SlackTentacle {
    name: String,
    config: SlackTentacleConfig,
}

impl Clone for SlackTentacle {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            config: self.config.clone(),
        }
    }
}

/// Slack Event Request
#[derive(Debug, Deserialize)]
struct SlackEventRequest {
    #[serde(default)]
    #[allow(dead_code)]
    token: String,
    challenge: Option<String>,
    #[serde(default, rename = "type")]
    #[allow(dead_code)]
    event_type: String,
    #[serde(default)]
    #[allow(dead_code)]
    event_id: String,
    #[serde(default)]
    #[allow(dead_code)]
    event_time: i64,
    event: Option<SlackEvent>,
}

/// Slack Event
#[derive(Debug, Deserialize)]
struct SlackEvent {
    #[serde(default, rename = "type")]
    event_type: String,
    #[serde(default)]
    user: String,
    #[serde(default)]
    user_profile: Option<SlackUserProfile>,
    #[serde(default)]
    channel: String,
    #[serde(default)]
    #[allow(dead_code)]
    channel_types: Option<String>,
    #[serde(default)]
    text: String,
    #[serde(default)]
    ts: String,
    #[serde(default)]
    thread_ts: Option<String>,
    #[serde(default)]
    team: String,
    #[serde(default)]
    files: Vec<SlackFile>,
}

#[derive(Debug, Deserialize)]
struct SlackUserProfile {
    #[serde(default)]
    real_name: String,
    #[serde(default)]
    display_name: String,
    #[serde(default)]
    email: Option<String>,
    #[serde(default)]
    team_id: String,
}

#[derive(Debug, Deserialize)]
struct SlackFile {
    #[serde(default)]
    id: String,
    #[serde(default)]
    title: Option<String>,
    #[serde(default)]
    mime_type: String,
    #[serde(default)]
    url_private: Option<String>,
}

/// Server State für Axum
struct ServerState {
    event_bus: Arc<EventBus>,
    tentacle: Arc<SlackTentacle>,
}

impl SlackTentacle {
    pub fn new(name: String, config: SlackTentacleConfig) -> Self {
        Self { name, config }
    }

    /// Verifiziert die Slack Signing Secret
    fn verify_signing_secret(
        &self,
        signing_secret: &str,
        body: &[u8],
        timestamp: &str,
        signature: &str,
    ) -> bool {
        use hmac::{Hmac, Mac};
        type HmacSha256 = Hmac<sha2::Sha256>;

        let mut mac = match HmacSha256::new_from_slice(signing_secret.as_bytes()) {
            Ok(m) => m,
            Err(_) => return false,
        };
        mac.update(format!("v0:{}", timestamp).as_bytes());
        mac.update(body);

        let result = mac.finalize();
        let expected = format!("v0={:x}", result.into_bytes());
        expected == signature
    }

    /// Bestimmt das Topic basierend auf Channel oder Inhalt
    fn resolve_topic(&self, event: &SlackEvent) -> String {
        match self.config.topic_mode.as_str() {
            "channel" => self
                .topic_from_channel(event)
                .unwrap_or_else(|| self.config.fallback_topic.clone()),
            "content" => self
                .topic_from_content(event)
                .unwrap_or_else(|| self.config.fallback_topic.clone()),
            "all" => {
                // Channel-Mapping hat Vorrang, dann Inhalt, dann Fallback
                if let Some(topic) = self.topic_from_channel(event) {
                    return topic;
                }
                if let Some(topic) = self.topic_from_content(event) {
                    return topic;
                }
                self.config.fallback_topic.clone()
            }
            _ => self.config.fallback_topic.clone(),
        }
    }

    /// Extrahiert Topic aus Channel
    fn topic_from_channel(&self, event: &SlackEvent) -> Option<String> {
        // Direktes Channel-Mapping
        if let Some(topic) = self.config.channel_topic_mapping.get(&event.channel) {
            return Some(topic.clone());
        }
        None
    }

    /// Extrahiert Topic aus dem Nachrichteninhalt
    fn topic_from_content(&self, event: &SlackEvent) -> Option<String> {
        // Wenn Text mit "/" beginnt, ist er selbst das Topic
        if event.text.starts_with('/') {
            // "/topic message" -> "/topic"
            if event.text.contains(' ') {
                let first_word = event.text.split(' ').next().unwrap_or("");
                if first_word.starts_with('/') {
                    return Some(first_word.to_string());
                }
                return Some(format!("/{}", first_word));
            }
            return Some(event.text.clone());
        }

        // Versuche, den Text als JSON zu parsen
        if let Ok(body_json) = serde_json::from_str::<serde_json::Value>(&event.text)
            && let Some(topic) = body_json.get("topic").and_then(|t| t.as_str())
            && !topic.is_empty()
        {
            return Some(topic.to_string());
        }

        None
    }

    /// Prüft, ob der Channel erlaubt ist
    fn is_channel_allowed(&self, channel: &str) -> bool {
        if self.config.allowed_channels.is_empty() {
            return true;
        }
        self.config.allowed_channels.contains(&channel.to_string())
    }

    /// Prüft, ob der User erlaubt ist
    fn is_user_allowed(&self, user: &str) -> bool {
        if self.config.allowed_users.is_empty() {
            return true;
        }
        self.config.allowed_users.contains(&user.to_string())
    }

    /// Erstellt ein Ink aus einem Slack Event
    fn create_ink(&self, event: &SlackEvent) -> Ink {
        let topic = self.resolve_topic(event);

        // Nachrichtentyp bestimmen
        let message_type = if !event.files.is_empty() {
            "file"
        } else if event.thread_ts.is_some() {
            "reply"
        } else {
            "text"
        };

        // Body erstellen
        let body = if event.text.is_empty() {
            format!("[Slack message with {} file(s)]", event.files.len())
        } else {
            event.text.clone()
        };

        // Metadata erstellen
        let metadata = serde_json::json!({
            "channel": event.channel,
            "user": event.user,
            "timestamp": event.ts,
            "team": event.team,
            "message_type": message_type,
            "thread_ts": event.thread_ts,
            "user_profile": event.user_profile.as_ref().map(|p| serde_json::json!({
                "real_name": p.real_name,
                "display_name": p.display_name,
                "email": p.email,
                "team_id": p.team_id,
            })),
            "files": event.files.iter().map(|f| serde_json::json!({
                "id": f.id,
                "title": f.title,
                "mime_type": f.mime_type,
                "url_private": f.url_private,
            })).collect::<Vec<_>>(),
        });

        Ink::new(topic, "slack", &self.name, body).with_metadata(metadata)
    }
}

/// Axum Handler für Slack Events
async fn handle_slack_event(
    State(state): State<Arc<ServerState>>,
    headers: axum::http::HeaderMap,
    body: axum::body::Bytes,
) -> (StatusCode, String) {
    let signing_secret = &state.tentacle.config.signing_secret;
    let timestamp = headers
        .get("X-Slack-Request-Timestamp")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    let signature = headers
        .get("X-Slack-Signature")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");

    // Signing Secret verifizieren (falls konfiguriert)
    if !signature.is_empty()
        && !state
            .tentacle
            .verify_signing_secret(signing_secret, &body, timestamp, signature)
    {
        warn!(
            "Slack tentacle '{}' invalid signing secret",
            state.tentacle.name
        );
        return (StatusCode::FORBIDDEN, "Invalid signature".to_string());
    }

    // Body parsen
    let request: SlackEventRequest = match serde_json::from_slice(&body) {
        Ok(r) => r,
        Err(e) => {
            error!(
                "Slack tentacle '{}' failed to parse request: {}",
                state.tentacle.name, e
            );
            return (StatusCode::BAD_REQUEST, "Invalid JSON".to_string());
        }
    };

    // URL Verification Challenge
    if let Some(challenge) = request.challenge {
        return (
            StatusCode::OK,
            serde_json::json!({ "challenge": challenge }).to_string(),
        );
    }

    // Event verarbeiten
    if let Some(event) = request.event {
        debug!(
            "Slack tentacle '{}' received event: {}",
            state.tentacle.name, event.event_type
        );

        // Nur message Events verarbeiten
        if event.event_type == "message" {
            // Channel und User filtern
            if !state.tentacle.is_channel_allowed(&event.channel) {
                debug!(
                    "Slack tentacle '{}' filtered channel: {}",
                    state.tentacle.name, event.channel
                );
                return (StatusCode::OK, "".to_string());
            }

            if !state.tentacle.is_user_allowed(&event.user) {
                debug!(
                    "Slack tentacle '{}' filtered user: {}",
                    state.tentacle.name, event.user
                );
                return (StatusCode::OK, "".to_string());
            }

            let ink = state.tentacle.create_ink(&event);
            debug!(
                "Slack tentacle '{}' publishing message on topic: {}",
                state.tentacle.name, ink.topic
            );
            state.event_bus.publish(ink);
        }
    }

    (StatusCode::OK, "".to_string())
}

#[async_trait]
impl Tentacle for SlackTentacle {
    fn name(&self) -> &str {
        &self.name
    }

    fn r#type(&self) -> &str {
        "slack"
    }

    async fn start(&self, event_bus: Arc<EventBus>) -> Result<()> {
        info!(
            "Slack tentacle '{}' listening on {}:{}{}",
            self.name, self.config.host, self.config.port, self.config.path
        );

        let app = Router::new()
            .route(&self.config.path, post(handle_slack_event))
            .with_state(Arc::new(ServerState {
                event_bus,
                tentacle: Arc::new(self.clone()),
            }));

        let addr = format!("{}:{}", self.config.host, self.config.port);
        let listener = tokio::net::TcpListener::bind(&addr)
            .await
            .with_context(|| {
                format!("Slack tentacle '{}' failed to bind to {}", self.name, addr)
            })?;

        info!("Slack tentacle '{}' started on {}", self.name, addr);

        axum::serve(listener, app)
            .await
            .with_context(|| format!("Slack tentacle '{}' server error", self.name))?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::appcontext::AppContext;

    fn create_tentacle() -> SlackTentacle {
        SlackTentacle::new(
            "test_slack".to_string(),
            SlackTentacleConfig {
                signing_secret: "test-secret".to_string(),
                bot_token: None,
                app_token: None,
                host: "0.0.0.0".to_string(),
                port: 9880,
                path: "/slack/events".to_string(),
                fallback_topic: "/slack/default".to_string(),
                topic_mode: "all".to_string(),
                channel_topic_mapping: HashMap::new(),
                allowed_channels: Vec::new(),
                allowed_users: Vec::new(),
            },
        )
    }

    #[test]
    fn test_tentacle_name_and_type() {
        let tentacle = create_tentacle();
        assert_eq!(tentacle.name(), "test_slack");
        assert_eq!(tentacle.r#type(), "slack");
    }

    #[test]
    fn test_resolve_topic_from_channel() {
        let mut mapping = HashMap::new();
        mapping.insert("C123456".to_string(), "/slack/alerts".to_string());

        let config = SlackTentacleConfig {
            signing_secret: "test-secret".to_string(),
            bot_token: None,
            app_token: None,
            host: "0.0.0.0".to_string(),
            port: 9880,
            path: "/slack/events".to_string(),
            fallback_topic: "/slack/default".to_string(),
            topic_mode: "channel".to_string(),
            channel_topic_mapping: mapping,
            allowed_channels: Vec::new(),
            allowed_users: Vec::new(),
        };

        let tentacle = SlackTentacle::new("test".to_string(), config);

        let event = SlackEvent {
            event_type: "message".to_string(),
            user: "U123456".to_string(),
            user_profile: None,
            channel: "C123456".to_string(),
            channel_types: None,
            text: "Hello".to_string(),
            ts: "1234567890.123456".to_string(),
            thread_ts: None,
            team: "T123456".to_string(),
            files: Vec::new(),
        };

        assert_eq!(tentacle.resolve_topic(&event), "/slack/alerts");
    }

    #[test]
    fn test_resolve_topic_from_content() {
        let config = SlackTentacleConfig {
            signing_secret: "test-secret".to_string(),
            bot_token: None,
            app_token: None,
            host: "0.0.0.0".to_string(),
            port: 9880,
            path: "/slack/events".to_string(),
            fallback_topic: "/slack/default".to_string(),
            topic_mode: "content".to_string(),
            channel_topic_mapping: HashMap::new(),
            allowed_channels: Vec::new(),
            allowed_users: Vec::new(),
        };

        let tentacle = SlackTentacle::new("test".to_string(), config);

        // Text beginnt mit /
        let event = SlackEvent {
            event_type: "message".to_string(),
            user: "U123456".to_string(),
            user_profile: None,
            channel: "C123456".to_string(),
            channel_types: None,
            text: "/slack/topic message content".to_string(),
            ts: "1234567890.123456".to_string(),
            thread_ts: None,
            team: "T123456".to_string(),
            files: Vec::new(),
        };

        assert_eq!(tentacle.resolve_topic(&event), "/slack/topic");

        // JSON mit topic Feld
        let config2 = SlackTentacleConfig {
            signing_secret: "test-secret".to_string(),
            bot_token: None,
            app_token: None,
            host: "0.0.0.0".to_string(),
            port: 9880,
            path: "/slack/events".to_string(),
            fallback_topic: "/slack/default".to_string(),
            topic_mode: "content".to_string(),
            channel_topic_mapping: HashMap::new(),
            allowed_channels: Vec::new(),
            allowed_users: Vec::new(),
        };

        let tentacle2 = SlackTentacle::new("test2".to_string(), config2);
        let event2 = SlackEvent {
            event_type: "message".to_string(),
            user: "U123456".to_string(),
            user_profile: None,
            channel: "C123456".to_string(),
            channel_types: None,
            text: r#"{"topic": "/slack/json-topic", "data": "value"}"#.to_string(),
            ts: "1234567890.123456".to_string(),
            thread_ts: None,
            team: "T123456".to_string(),
            files: Vec::new(),
        };

        assert_eq!(tentacle2.resolve_topic(&event2), "/slack/json-topic");
    }

    #[test]
    fn test_resolve_topic_all_mode() {
        let mut mapping = HashMap::new();
        mapping.insert("C123456".to_string(), "/slack/channel-mapped".to_string());

        let config = SlackTentacleConfig {
            signing_secret: "test-secret".to_string(),
            bot_token: None,
            app_token: None,
            host: "0.0.0.0".to_string(),
            port: 9880,
            path: "/slack/events".to_string(),
            fallback_topic: "/slack/default".to_string(),
            topic_mode: "all".to_string(),
            channel_topic_mapping: mapping,
            allowed_channels: Vec::new(),
            allowed_users: Vec::new(),
        };

        let tentacle = SlackTentacle::new("test".to_string(), config.clone());

        // Channel-Mapping hat Vorrang
        let event = SlackEvent {
            event_type: "message".to_string(),
            user: "U123456".to_string(),
            user_profile: None,
            channel: "C123456".to_string(),
            channel_types: None,
            text: "/slack/content-topic".to_string(),
            ts: "1234567890.123456".to_string(),
            thread_ts: None,
            team: "T123456".to_string(),
            files: Vec::new(),
        };

        assert_eq!(tentacle.resolve_topic(&event), "/slack/channel-mapped");

        // Kein Channel-Mapping, Inhalt hat Vorrang
        let mut config2 = config.clone();
        config2.channel_topic_mapping = HashMap::new();
        let tentacle2 = SlackTentacle::new("test2".to_string(), config2);
        assert_eq!(tentacle2.resolve_topic(&event), "/slack/content-topic");

        // Fallback
        let event2 = SlackEvent {
            event_type: "message".to_string(),
            user: "U123456".to_string(),
            user_profile: None,
            channel: "C789".to_string(),
            channel_types: None,
            text: "No topic here".to_string(),
            ts: "1234567890.123456".to_string(),
            thread_ts: None,
            team: "T123456".to_string(),
            files: Vec::new(),
        };

        assert_eq!(tentacle2.resolve_topic(&event2), "/slack/default");
    }

    #[test]
    fn test_channel_filtering() {
        let config = SlackTentacleConfig {
            signing_secret: "test-secret".to_string(),
            bot_token: None,
            app_token: None,
            host: "0.0.0.0".to_string(),
            port: 9880,
            path: "/slack/events".to_string(),
            fallback_topic: "/slack/default".to_string(),
            topic_mode: "all".to_string(),
            channel_topic_mapping: HashMap::new(),
            allowed_channels: vec!["C123456".to_string()],
            allowed_users: Vec::new(),
        };

        let tentacle = SlackTentacle::new("test".to_string(), config.clone());
        assert!(tentacle.is_channel_allowed("C123456"));
        assert!(!tentacle.is_channel_allowed("C789"));
    }

    #[test]
    fn test_user_filtering() {
        let config = SlackTentacleConfig {
            signing_secret: "test-secret".to_string(),
            bot_token: None,
            app_token: None,
            host: "0.0.0.0".to_string(),
            port: 9880,
            path: "/slack/events".to_string(),
            fallback_topic: "/slack/default".to_string(),
            topic_mode: "all".to_string(),
            channel_topic_mapping: HashMap::new(),
            allowed_channels: Vec::new(),
            allowed_users: vec!["U123456".to_string()],
        };

        let tentacle = SlackTentacle::new("test".to_string(), config.clone());
        assert!(tentacle.is_user_allowed("U123456"));
        assert!(!tentacle.is_user_allowed("U789"));
    }

    #[test]
    fn test_config_defaults() {
        let config: SlackTentacleConfig = serde_yaml::from_str(
            r#"
            signing_secret: "my-secret"
        "#,
        )
        .unwrap();

        assert_eq!(config.host, "0.0.0.0");
        assert_eq!(config.port, 9880);
        assert_eq!(config.path, "/slack/events");
        assert_eq!(config.fallback_topic, "/slack/default");
        assert_eq!(config.topic_mode, "all");
    }

    #[test]
    fn test_create_ink() {
        let tentacle = create_tentacle();

        let event = SlackEvent {
            event_type: "message".to_string(),
            user: "U123456".to_string(),
            user_profile: Some(SlackUserProfile {
                real_name: "John Doe".to_string(),
                display_name: "johndoe".to_string(),
                email: Some("john@example.com".to_string()),
                team_id: "T123456".to_string(),
            }),
            channel: "C123456".to_string(),
            channel_types: Some("channel".to_string()),
            text: "Hello world".to_string(),
            ts: "1234567890.123456".to_string(),
            thread_ts: None,
            team: "T123456".to_string(),
            files: Vec::new(),
        };

        let ink = tentacle.create_ink(&event);
        assert_eq!(ink.topic, "/slack/default");
        assert_eq!(ink.source.name, "test_slack");
        assert_eq!(ink.body, "Hello world");
    }

    #[test]
    fn test_create_ink_with_file() {
        let tentacle = create_tentacle();

        let event = SlackEvent {
            event_type: "message".to_string(),
            user: "U123456".to_string(),
            user_profile: None,
            channel: "C123456".to_string(),
            channel_types: None,
            text: "".to_string(),
            ts: "1234567890.123456".to_string(),
            thread_ts: None,
            team: "T123456".to_string(),
            files: vec![SlackFile {
                id: "F123".to_string(),
                title: Some("document.pdf".to_string()),
                mime_type: "application/pdf".to_string(),
                url_private: Some("https://...".to_string()),
            }],
        };

        let ink = tentacle.create_ink(&event);
        assert!(ink.body.contains("file"));
    }
}
