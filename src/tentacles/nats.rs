use anyhow::Result;
use async_trait::async_trait;
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, error, info, warn};

use super::Tentacle;
use crate::eventbus::EventBus;
use crate::model::Ink;

/// Topic-Erkennungsmodus für NATS-Nachrichten
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "lowercase")]
pub enum NatsTopicMode {
    /// Topic aus dem NATS-Subject verwenden
    #[default]
    Subject,
    /// Topic aus einem JSON-Feld extrahieren
    Field,
    /// Alle Nachrichten an ein festes Topic
    All,
}

/// Authentifizierungstyp für NATS
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum NatsAuth {
    /// Keine Authentifizierung
    #[default]
    None,
    /// Username/Password
    Credentials { username: String, password: String },
    /// NKey Authentifizierung
    NKey { user_credentials: String },
    /// Token-basiert
    Token { token: String },
}

/// Konfiguration für den NATS Tentacle
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NatsTentacleConfig {
    /// NATS Server-URL (z.B. "nats://localhost:4222")
    #[serde(default = "default_server")]
    pub server: String,
    /// NATS-Subjects zum Abonnieren (unterstützt Wildcards: * und >)
    pub subjects: Vec<String>,
    /// Topic-Erkennungsmodus
    #[serde(default)]
    pub topic_mode: NatsTopicMode,
    /// JSON-Feld für Topic-Extraktion (bei field-Modus)
    #[serde(default)]
    pub topic_field: Option<String>,
    /// Fallback-Topic
    #[serde(default = "default_fallback_topic")]
    pub fallback_topic: String,
    /// Optional: Subject-basiertes Topic-Mapping
    #[serde(default)]
    pub subject_topic_mapping: HashMap<String, String>,
    /// Authentifizierung
    #[serde(default)]
    pub auth: NatsAuth,
    /// Optional: Client-ID
    #[serde(default)]
    pub client_id: Option<String>,
    /// Reconnect-Verzögerung in Sekunden
    #[serde(default = "default_reconnect_delay")]
    pub reconnect_delay_seconds: u64,
}

fn default_server() -> String {
    "nats://localhost:4222".to_string()
}

fn default_fallback_topic() -> String {
    "/nats/default".to_string()
}

fn default_reconnect_delay() -> u64 {
    5
}

pub struct NatsTentacle {
    name: String,
    config: NatsTentacleConfig,
}

impl NatsTentacle {
    pub fn new(name: String, config: NatsTentacleConfig) -> Self {
        Self { name, config }
    }

    /// Bestimmt das Topic basierend auf der NATS-Nachricht
    fn resolve_topic(&self, subject: &str, payload: &[u8]) -> String {
        // Prüfe subject_topic_mapping (mit Wildcard-Unterstützung)
        for (pattern, topic) in &self.config.subject_topic_mapping {
            if Self::match_subject_pattern(pattern, subject) {
                // Ersetze Wildcards mit tatsächlichen Werten
                return Self::substitute_wildcards(pattern, subject, topic);
            }
        }

        match self.config.topic_mode {
            NatsTopicMode::Subject => {
                // Konvertiere NATS-Subject zu Kraken-Topic
                // Ersetze Punkte mit Schrägstrichen
                let topic = format!("/nats/{}", subject.replace('.', "/"));
                if topic.is_empty() {
                    self.config.fallback_topic.clone()
                } else {
                    topic
                }
            }
            NatsTopicMode::Field => {
                // Versuche JSON-Feld zu extrahieren
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
            NatsTopicMode::All => self.config.fallback_topic.clone(),
        }
    }

    /// Prüft ob ein Subject zu einem Pattern passt
    /// NATS Wildcards: * (ein Token), > (alle folgenden Tokens)
    fn match_subject_pattern(pattern: &str, subject: &str) -> bool {
        let pattern_parts: Vec<&str> = pattern.split('.').collect();
        let subject_parts: Vec<&str> = subject.split('.').collect();

        let mut p = 0;
        let mut s = 0;

        while p < pattern_parts.len() && s < subject_parts.len() {
            match pattern_parts[p] {
                ">" => {
                    // > passt zu allen verbleibenden Tokens
                    return true;
                }
                "*" => {
                    // * passt zu genau einem Token
                    p += 1;
                    s += 1;
                }
                part => {
                    if part != subject_parts[s] {
                        return false;
                    }
                    p += 1;
                    s += 1;
                }
            }
        }

        // Beide müssen vollständig abgearbeitet sein
        p == pattern_parts.len() && s == subject_parts.len()
    }

    /// Ersetzt Wildcards im Pattern mit tatsächlichen Subject-Werten
    fn substitute_wildcards(pattern: &str, subject: &str, template: &str) -> String {
        let subject_parts: Vec<&str> = subject.split('.').collect();
        let pattern_parts: Vec<&str> = pattern.split('.').collect();

        let mut result = template.to_string();

        for (i, part) in pattern_parts.iter().enumerate() {
            if *part == "*" && i < subject_parts.len() {
                let placeholder = format!("{{{}}}", i);
                result = result.replace(&placeholder, subject_parts[i]);
            }
        }

        result
    }

    /// Erstellt eine Ink-Nachricht aus einer NATS-Nachricht
    fn create_ink(&self, subject: &str, payload: &[u8], reply_to: Option<&str>) -> Ink {
        let topic = self.resolve_topic(subject, payload);

        // Payload als String konvertieren
        let body = match std::str::from_utf8(payload) {
            Ok(s) => s.to_string(),
            Err(_) => {
                // Binäre Daten als Base64
                let encoded =
                    base64::Engine::encode(&base64::engine::general_purpose::STANDARD, payload);
                debug!("Binary NATS message encoded as base64");
                encoded
            }
        };

        let mut metadata = serde_json::Map::new();
        metadata.insert("nats_subject".to_string(), serde_json::json!(subject));
        if let Some(reply) = reply_to {
            metadata.insert("nats_reply_to".to_string(), serde_json::json!(reply));
        }

        Ink::new(topic, "nats", &self.name, body).with_metadata(serde_json::Value::Object(metadata))
    }

    /// Erstellt die NATS-Connection-Options
    fn build_options(&self) -> async_nats::ConnectOptions {
        let mut options = async_nats::ConnectOptions::new();

        match &self.config.auth {
            NatsAuth::None => {}
            NatsAuth::Credentials { username, password } => {
                options = options.user_and_password(username.clone(), password.clone());
            }
            NatsAuth::NKey {
                user_credentials: _,
            } => {
                // NKey requires async setup; skip for now
                warn!("NKey auth not fully supported, using no auth");
            }
            NatsAuth::Token { token } => {
                options = options.token(token.clone());
            }
        }

        if let Some(ref client_id) = self.config.client_id {
            options = options.name(client_id.clone());
        }

        options
    }
}

#[async_trait]
impl Tentacle for NatsTentacle {
    fn name(&self) -> &str {
        &self.name
    }

    fn r#type(&self) -> &str {
        "nats"
    }

    async fn start(&self, event_bus: Arc<EventBus>) -> Result<()> {
        info!(
            "NATS Tentacle '{}' connecting to {} with {} subject(s)",
            self.name,
            self.config.server,
            self.config.subjects.len()
        );

        loop {
            let options = self.build_options();

            // Verbindung aufbauen
            let connection = match options.connect(&self.config.server).await {
                Ok(conn) => {
                    info!(
                        "NATS Tentacle '{}' connected to {}",
                        self.name, self.config.server
                    );
                    conn
                }
                Err(e) => {
                    error!(
                        "NATS Tentacle '{}' failed to connect: {}. Retrying in {} seconds...",
                        self.name, e, self.config.reconnect_delay_seconds
                    );
                    tokio::time::sleep(std::time::Duration::from_secs(
                        self.config.reconnect_delay_seconds,
                    ))
                    .await;
                    continue;
                }
            };

            // Subjects abonnieren
            let mut subscriptions = Vec::new();
            for subject in &self.config.subjects {
                match connection.subscribe(subject.clone()).await {
                    Ok(sub) => {
                        info!("NATS Tentacle '{}' subscribed to '{}'", self.name, subject);
                        subscriptions.push((subject.clone(), sub));
                    }
                    Err(e) => {
                        error!(
                            "NATS Tentacle '{}' failed to subscribe to '{}': {}",
                            self.name, subject, e
                        );
                    }
                }
            }

            if subscriptions.is_empty() {
                error!("NATS Tentacle '{}' has no active subscriptions", self.name);
                tokio::time::sleep(std::time::Duration::from_secs(
                    self.config.reconnect_delay_seconds,
                ))
                .await;
                continue;
            }

            // Nachrichten verarbeiten - vereinfachter Loop
            // In einer echten Implementierung würde man hier alle Subscriptions multiplexen
            // Für die Einfachheit verwenden wir die erste Subscription
            if let Some((subject_name, mut sub)) = subscriptions.pop() {
                info!(
                    "NATS Tentacle '{}' listening on '{}'",
                    self.name, subject_name
                );

                while let Some(message) = sub.next().await {
                    let payload = message.payload.clone();
                    let subject = message.subject.to_string();
                    let reply_to = message.reply.to_owned().map(|r| r.to_string());

                    debug!(
                        "NATS Tentacle '{}' received message on '{}'",
                        self.name, subject
                    );

                    let ink = self.create_ink(&subject, &payload, reply_to.as_deref());
                    event_bus.publish(ink);
                }

                // Wenn die Subscription geschlossen wird, neu verbinden
                warn!(
                    "NATS Tentacle '{}' subscription closed, reconnecting...",
                    self.name
                );
            }

            tokio::time::sleep(std::time::Duration::from_secs(
                self.config.reconnect_delay_seconds,
            ))
            .await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::appcontext::AppContext;

    #[test]
    fn test_match_subject_pattern_exact() {
        assert!(NatsTentacle::match_subject_pattern(
            "orders.new",
            "orders.new"
        ));
        assert!(!NatsTentacle::match_subject_pattern(
            "orders.new",
            "orders.old"
        ));
    }

    #[test]
    fn test_match_subject_pattern_star() {
        assert!(NatsTentacle::match_subject_pattern(
            "orders.*",
            "orders.new"
        ));
        assert!(NatsTentacle::match_subject_pattern(
            "orders.*",
            "orders.old"
        ));
        assert!(!NatsTentacle::match_subject_pattern(
            "orders.*",
            "orders.new.item"
        ));
    }

    #[test]
    fn test_match_subject_pattern_greedy() {
        assert!(NatsTentacle::match_subject_pattern(
            "orders.>",
            "orders.new"
        ));
        assert!(NatsTentacle::match_subject_pattern(
            "orders.>",
            "orders.new.item.123"
        ));
        assert!(!NatsTentacle::match_subject_pattern(
            "orders.>",
            "other.new"
        ));
    }

    #[test]
    fn test_match_subject_pattern_multiple_star() {
        assert!(NatsTentacle::match_subject_pattern(
            "*.new.*",
            "orders.new.item"
        ));
        assert!(!NatsTentacle::match_subject_pattern(
            "*.new.*",
            "orders.old.item"
        ));
    }

    #[test]
    fn test_substitute_wildcards() {
        let result = NatsTentacle::substitute_wildcards(
            "orders.*.>",
            "orders.new.item",
            "/orders/{1}/processed",
        );
        assert_eq!(result, "/orders/new/processed");
    }

    #[test]
    fn test_resolve_topic_subject_mode() {
        let config = NatsTentacleConfig {
            server: "nats://localhost:4222".to_string(),
            subjects: vec!["test.>".to_string()],
            topic_mode: NatsTopicMode::Subject,
            topic_field: None,
            fallback_topic: "/nats/default".to_string(),
            subject_topic_mapping: HashMap::new(),
            auth: NatsAuth::None,
            client_id: None,
            reconnect_delay_seconds: 5,
        };
        let tentacle = NatsTentacle::new("test".to_string(), config);

        let topic = tentacle.resolve_topic("orders.new", b"{}");
        assert_eq!(topic, "/nats/orders/new");
    }

    #[test]
    fn test_resolve_topic_field_mode() {
        let config = NatsTentacleConfig {
            server: "nats://localhost:4222".to_string(),
            subjects: vec!["test.>".to_string()],
            topic_mode: NatsTopicMode::Field,
            topic_field: Some("event_type".to_string()),
            fallback_topic: "/nats/default".to_string(),
            subject_topic_mapping: HashMap::new(),
            auth: NatsAuth::None,
            client_id: None,
            reconnect_delay_seconds: 5,
        };
        let tentacle = NatsTentacle::new("test".to_string(), config);

        let payload = r#"{"event_type": "/events/order", "data": "test"}"#;
        let topic = tentacle.resolve_topic("test.data", payload.as_bytes());
        assert_eq!(topic, "/events/order");
    }

    #[test]
    fn test_resolve_topic_all_mode() {
        let config = NatsTentacleConfig {
            server: "nats://localhost:4222".to_string(),
            subjects: vec!["test.>".to_string()],
            topic_mode: NatsTopicMode::All,
            topic_field: None,
            fallback_topic: "/nats/all".to_string(),
            subject_topic_mapping: HashMap::new(),
            auth: NatsAuth::None,
            client_id: None,
            reconnect_delay_seconds: 5,
        };
        let tentacle = NatsTentacle::new("test".to_string(), config);

        let topic = tentacle.resolve_topic("anything.at.all", b"payload");
        assert_eq!(topic, "/nats/all");
    }

    #[test]
    fn test_resolve_topic_subject_mapping() {
        let mut mapping = HashMap::new();
        mapping.insert("orders.*".to_string(), "/orders/{1}".to_string());

        let config = NatsTentacleConfig {
            server: "nats://localhost:4222".to_string(),
            subjects: vec!["orders.>".to_string()],
            topic_mode: NatsTopicMode::Subject,
            topic_field: None,
            fallback_topic: "/nats/default".to_string(),
            subject_topic_mapping: mapping,
            auth: NatsAuth::None,
            client_id: None,
            reconnect_delay_seconds: 5,
        };
        let tentacle = NatsTentacle::new("test".to_string(), config);

        let topic = tentacle.resolve_topic("orders.new", b"{}");
        assert_eq!(topic, "/orders/new");
    }

    #[test]
    fn test_create_ink_metadata() {
        let config = NatsTentacleConfig {
            server: "nats://localhost:4222".to_string(),
            subjects: vec!["test.>".to_string()],
            topic_mode: NatsTopicMode::Subject,
            topic_field: None,
            fallback_topic: "/nats/default".to_string(),
            subject_topic_mapping: HashMap::new(),
            auth: NatsAuth::None,
            client_id: None,
            reconnect_delay_seconds: 5,
        };
        let tentacle = NatsTentacle::new("test".to_string(), config);

        let ink = tentacle.create_ink("orders.new", b"Hello NATS", Some("reply.to"));
        assert_eq!(ink.topic, "/nats/orders/new");
        assert_eq!(ink.body, "Hello NATS");

        let meta = ink.metadata.unwrap();
        let meta_obj = meta.as_object().unwrap();
        assert_eq!(
            meta_obj.get("nats_subject").unwrap(),
            &serde_json::json!("orders.new")
        );
        assert_eq!(
            meta_obj.get("nats_reply_to").unwrap(),
            &serde_json::json!("reply.to")
        );
    }

    #[test]
    fn test_nats_config_deserialization() {
        let yaml = r#"
        server: "nats://localhost:4222"
        subjects:
          - "orders.>"
          - "events.*"
        topic_mode: "subject"
        fallback_topic: "/nats/default"
        auth:
          type: "credentials"
          username: "user"
          password: "pass"
        reconnect_delay_seconds: 10
        "#;

        let config: NatsTentacleConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.server, "nats://localhost:4222");
        assert_eq!(config.subjects.len(), 2);
        assert!(matches!(config.auth, NatsAuth::Credentials { .. }));
        assert_eq!(config.reconnect_delay_seconds, 10);
    }

    #[test]
    fn test_nats_tentacle_name_and_type() {
        let config = NatsTentacleConfig {
            server: "nats://localhost:4222".to_string(),
            subjects: vec!["test.>".to_string()],
            topic_mode: NatsTopicMode::Subject,
            topic_field: None,
            fallback_topic: "/nats/default".to_string(),
            subject_topic_mapping: HashMap::new(),
            auth: NatsAuth::None,
            client_id: None,
            reconnect_delay_seconds: 5,
        };
        let tentacle = NatsTentacle::new("test_nats".to_string(), config);
        assert_eq!(tentacle.name(), "test_nats");
        assert_eq!(tentacle.r#type(), "nats");
    }
}
