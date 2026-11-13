use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::http::Request as WsRequest;
use tracing::{debug, error, info, warn};

use super::Tentacle;
use crate::eventbus::EventBus;
use crate::model::Ink;

/// Topic-Erkennungsmodus für WebSocket-Nachrichten
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "lowercase")]
pub enum WsTopicMode {
    /// Topic aus einem JSON-Feld in der Nachricht
    #[default]
    Field,
    /// Topic aus dem Inhalt, wenn er mit / beginnt
    Content,
    /// Alle Nachrichten erhalten das Fallback-Topic
    All,
}

/// Konfiguration für den WebSocket Tentacle
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebSocketTentacleConfig {
    /// WebSocket-URL (ws:// oder wss://)
    pub url: String,
    /// Topic-Modus: "field", "content", "all"
    #[serde(default)]
    pub topic_mode: WsTopicMode,
    /// Fallback-Topic, wenn kein Topic erkannt wird
    #[serde(default = "default_fallback_topic")]
    pub fallback_topic: String,
    /// Optional: JSON-Feld für das Topic (bei topic_mode "field")
    #[serde(default)]
    pub topic_field: Option<String>,
    /// Optional: JSON-Feld für den Body (bei topic_mode "field")
    #[serde(default)]
    pub body_field: Option<String>,
    /// Optional: HTTP-Header für die WebSocket-Verbindung
    #[serde(default)]
    pub headers: HashMap<String, String>,
    /// Optional: Subprotocol (z.B. "graphql-ws")
    #[serde(default)]
    pub subprotocol: Option<String>,
    /// Wiederverbindungsversuche bei Verbindungsabbruch (Standard: true)
    #[serde(default = "default_reconnect")]
    pub reconnect: bool,
    /// Verzögerung zwischen Wiederverbindungsversuchen in Sekunden (Standard: 5)
    #[serde(default = "default_reconnect_delay")]
    pub reconnect_delay_seconds: u64,
    /// Maximale Nachrichtengröße in MB (Standard: 10)
    #[serde(default = "default_max_message_size")]
    pub max_message_size_mb: u64,
}

fn default_fallback_topic() -> String {
    "/websocket/default".to_string()
}

fn default_reconnect() -> bool {
    true
}

fn default_reconnect_delay() -> u64 {
    5
}

fn default_max_message_size() -> u64 {
    10
}

pub struct WebSocketTentacle {
    name: String,
    config: WebSocketTentacleConfig,
}

impl WebSocketTentacle {
    pub fn new(name: String, config: WebSocketTentacleConfig) -> Self {
        Self { name, config }
    }

    /// Erstellt eine WebSocket-Anfrage mit konfigurierter Header
    fn build_request(&self) -> Result<WsRequest<()>> {
        let url = &self.config.url;
        let mut builder = WsRequest::builder().uri(url);

        // Header hinzufügen
        for (key, value) in &self.config.headers {
            builder = builder.header(key, value);
        }

        // Subprotocol hinzufügen
        if let Some(ref subprotocol) = self.config.subprotocol {
            builder = builder.header("Sec-WebSocket-Protocol", subprotocol);
        }

        Ok(builder.body(())?)
    }

    /// Extrahiert das Topic aus einer WebSocket-Nachricht
    fn extract_topic(&self, message: &str) -> String {
        match self.config.topic_mode {
            WsTopicMode::Field => {
                // Versuche, JSON zu parsen und das Topic-Feld zu extrahieren
                if let Ok(Value::Object(map)) = serde_json::from_str::<Value>(message)
                    && let Some(ref field) = self.config.topic_field
                    && let Some(topic) = map.get(field).and_then(|v| v.as_str())
                {
                    return topic.to_string();
                }
                self.config.fallback_topic.clone()
            }
            WsTopicMode::Content => {
                let trimmed = message.trim();
                if trimmed.starts_with('/') {
                    if let Some(space_pos) = trimmed.find(' ') {
                        return trimmed[..space_pos].to_string();
                    }
                    return trimmed.to_string();
                }
                self.config.fallback_topic.clone()
            }
            WsTopicMode::All => self.config.fallback_topic.clone(),
        }
    }

    /// Extrahiert den Body aus einer WebSocket-Nachricht
    fn extract_body(&self, message: &str) -> String {
        match self.config.topic_mode {
            WsTopicMode::Field => {
                // Versuche, JSON zu parsen und das Body-Feld zu extrahieren
                if let Ok(Value::Object(map)) = serde_json::from_str::<Value>(message) {
                    if let Some(ref field) = self.config.body_field
                        && let Some(body) = map.get(field).and_then(|v| v.as_str())
                    {
                        return body.to_string();
                    }
                    // Kein body_field oder nicht gefunden — gesamte JSON als Body
                    return message.to_string();
                }
                message.to_string()
            }
            WsTopicMode::Content => {
                let trimmed = message.trim();
                if trimmed.starts_with('/') {
                    if let Some(space_pos) = trimmed.find(' ') {
                        return trimmed[space_pos..].trim().to_string();
                    }
                    return String::new();
                }
                trimmed.to_string()
            }
            WsTopicMode::All => message.trim().to_string(),
        }
    }

    /// Stellt eine WebSocket-Verbindung her und empfängt Nachrichten
    async fn connect_and_receive(&self, event_bus: &Arc<EventBus>) -> Result<()> {
        let request = self.build_request()?;

        info!(
            "WebSocket tentacle '{}' connecting to {}",
            self.name, self.config.url
        );

        let (ws_stream, _) = tokio_tungstenite::connect_async(request).await?;
        info!(
            "WebSocket tentacle '{}' connected to {}",
            self.name, self.config.url
        );

        use futures_util::StreamExt;

        let (write, mut read) = ws_stream.split();

        // Wir senden keine Nachrichten, also können wir write unbenutzt lassen
        drop(write);

        while let Some(msg_result) = read.next().await {
            match msg_result {
                Ok(Message::Text(text)) => {
                    // Nachrichtengröße prüfen
                    if text.len() > self.config.max_message_size_mb as usize * 1024 * 1024 {
                        warn!(
                            "WebSocket tentacle '{}' received message too large ({} bytes), skipping",
                            self.name,
                            text.len()
                        );
                        continue;
                    }

                    let topic = self.extract_topic(&text);
                    let body = self.extract_body(&text);

                    let ink = Ink::new(topic.clone(), "websocket", &self.name, body);
                    debug!(
                        "WebSocket tentacle '{}' received message on topic: {}",
                        self.name, topic
                    );
                    event_bus.publish(ink);
                }
                Ok(Message::Binary(data)) => {
                    // Binärdaten als Base64 codieren
                    let body =
                        base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &data);
                    let topic = self.config.fallback_topic.clone();
                    let ink = Ink::new(topic.clone(), "websocket", &self.name, body);
                    debug!(
                        "WebSocket tentacle '{}' received binary message on topic: {}",
                        self.name, topic
                    );
                    event_bus.publish(ink);
                }
                Ok(Message::Close(Some(close_frame))) => {
                    info!(
                        "WebSocket tentacle '{}' received close frame: {} - {}",
                        self.name, close_frame.code, close_frame.reason
                    );
                    break;
                }
                Ok(Message::Close(None)) => {
                    info!("WebSocket tentacle '{}' received close frame", self.name);
                    break;
                }
                Ok(Message::Ping(_) | Message::Pong(_) | Message::Frame(_)) => {
                    // Kontrollnachrichten ignorieren
                    debug!(
                        "WebSocket tentacle '{}' received control message",
                        self.name
                    );
                }
                Err(e) => {
                    warn!("WebSocket tentacle '{}' received error: {}", self.name, e);
                    break;
                }
            }
        }

        Ok(())
    }
}

#[async_trait]
impl Tentacle for WebSocketTentacle {
    fn name(&self) -> &str {
        &self.name
    }

    fn r#type(&self) -> &str {
        "websocket"
    }

    async fn start(&self, event_bus: Arc<EventBus>) -> Result<()> {
        info!(
            "WebSocket tentacle '{}' started (url: {}, topic_mode: {:?})",
            self.name, self.config.url, self.config.topic_mode
        );

        loop {
            match self.connect_and_receive(&event_bus).await {
                Ok(()) => {
                    if self.config.reconnect {
                        warn!(
                            "WebSocket tentacle '{}' connection closed, reconnecting in {}s...",
                            self.name, self.config.reconnect_delay_seconds
                        );
                        tokio::time::sleep(Duration::from_secs(
                            self.config.reconnect_delay_seconds,
                        ))
                        .await;
                    } else {
                        info!(
                            "WebSocket tentacle '{}' connection closed, not reconnecting",
                            self.name
                        );
                        break;
                    }
                }
                Err(e) => {
                    error!("WebSocket tentacle '{}' connection error: {}", self.name, e);
                    if self.config.reconnect {
                        warn!(
                            "WebSocket tentacle '{}' reconnecting in {}s...",
                            self.name, self.config.reconnect_delay_seconds
                        );
                        tokio::time::sleep(Duration::from_secs(
                            self.config.reconnect_delay_seconds,
                        ))
                        .await;
                    } else {
                        return Err(e);
                    }
                }
            }
        }

        info!("WebSocket tentacle '{}' stopped", self.name);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::appcontext::AppContext;

    fn create_tentacle_field_mode() -> WebSocketTentacle {
        WebSocketTentacle::new(
            "test_ws".to_string(),
            WebSocketTentacleConfig {
                url: "wss://example.com/ws".to_string(),
                topic_mode: WsTopicMode::Field,
                fallback_topic: "/ws/default".to_string(),
                topic_field: Some("topic".to_string()),
                body_field: Some("data".to_string()),
                headers: HashMap::new(),
                subprotocol: None,
                reconnect: true,
                reconnect_delay_seconds: 5,
                max_message_size_mb: 10,
            },
        )
    }

    fn create_tentacle_content_mode() -> WebSocketTentacle {
        WebSocketTentacle::new(
            "test_ws".to_string(),
            WebSocketTentacleConfig {
                url: "wss://example.com/ws".to_string(),
                topic_mode: WsTopicMode::Content,
                fallback_topic: "/ws/fallback".to_string(),
                topic_field: None,
                body_field: None,
                headers: HashMap::new(),
                subprotocol: None,
                reconnect: true,
                reconnect_delay_seconds: 5,
                max_message_size_mb: 10,
            },
        )
    }

    fn create_tentacle_all_mode() -> WebSocketTentacle {
        WebSocketTentacle::new(
            "test_ws".to_string(),
            WebSocketTentacleConfig {
                url: "wss://example.com/ws".to_string(),
                topic_mode: WsTopicMode::All,
                fallback_topic: "/ws/all".to_string(),
                topic_field: None,
                body_field: None,
                headers: HashMap::new(),
                subprotocol: None,
                reconnect: false,
                reconnect_delay_seconds: 5,
                max_message_size_mb: 10,
            },
        )
    }

    #[test]
    fn test_extract_topic_field_mode() {
        let tentacle = create_tentacle_field_mode();
        let json = r#"{"topic": "/ws/alerts", "data": "hello"}"#;
        assert_eq!(tentacle.extract_topic(json), "/ws/alerts");
    }

    #[test]
    fn test_extract_topic_field_mode_no_field() {
        let tentacle = create_tentacle_field_mode();
        let json = r#"{"key": "value"}"#;
        assert_eq!(tentacle.extract_topic(json), "/ws/default");
    }

    #[test]
    fn test_extract_topic_content_mode() {
        let tentacle = create_tentacle_content_mode();
        assert_eq!(tentacle.extract_topic("/alerts hello world"), "/alerts");
    }

    #[test]
    fn test_extract_topic_content_mode_no_slash() {
        let tentacle = create_tentacle_content_mode();
        assert_eq!(tentacle.extract_topic("just text"), "/ws/fallback");
    }

    #[test]
    fn test_extract_topic_all_mode() {
        let tentacle = create_tentacle_all_mode();
        assert_eq!(tentacle.extract_topic("/anything"), "/ws/all");
        assert_eq!(tentacle.extract_topic("anything"), "/ws/all");
    }

    #[test]
    fn test_extract_body_field_mode() {
        let tentacle = create_tentacle_field_mode();
        let json = r#"{"topic": "/ws/alerts", "data": "hello world"}"#;
        assert_eq!(tentacle.extract_body(json), "hello world");
    }

    #[test]
    fn test_extract_body_field_mode_no_body_field() {
        let tentacle = create_tentacle_field_mode();
        let json = r#"{"topic": "/ws/alerts", "other": "value"}"#;
        // Kein "data"-Feld, gesamte JSON als Body
        assert_eq!(
            tentacle.extract_body(json),
            r#"{"topic": "/ws/alerts", "other": "value"}"#
        );
    }

    #[test]
    fn test_extract_body_content_mode() {
        let tentacle = create_tentacle_content_mode();
        assert_eq!(tentacle.extract_body("/alerts hello world"), "hello world");
    }

    #[test]
    fn test_extract_body_content_mode_no_slash() {
        let tentacle = create_tentacle_content_mode();
        assert_eq!(tentacle.extract_body("just text"), "just text");
    }

    #[test]
    fn test_extract_body_all_mode() {
        let tentacle = create_tentacle_all_mode();
        assert_eq!(tentacle.extract_body("  hello world  "), "hello world");
    }

    #[test]
    fn test_config_deserialization() {
        let yaml = r#"
            url: "wss://example.com/ws"
            topic_mode: "field"
            fallback_topic: "/ws/custom"
            topic_field: "event_type"
            body_field: "payload"
            reconnect: true
            reconnect_delay_seconds: 10
            subprotocol: "graphql-ws"
        "#;
        let config: WebSocketTentacleConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.url, "wss://example.com/ws");
        assert_eq!(config.topic_mode, WsTopicMode::Field);
        assert_eq!(config.topic_field, Some("event_type".to_string()));
        assert_eq!(config.subprotocol, Some("graphql-ws".to_string()));
    }

    #[test]
    fn test_config_defaults() {
        let yaml = r#"
            url: "wss://example.com/ws"
        "#;
        let config: WebSocketTentacleConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.topic_mode, WsTopicMode::Field);
        assert_eq!(config.fallback_topic, "/websocket/default");
        assert!(config.reconnect);
        assert_eq!(config.reconnect_delay_seconds, 5);
    }

    #[test]
    fn test_build_request() {
        let tentacle = create_tentacle_field_mode();
        let request = tentacle.build_request().unwrap();
        assert_eq!(request.uri().to_string(), "wss://example.com/ws");
    }

    #[test]
    fn test_build_request_with_headers() {
        let mut headers = HashMap::new();
        headers.insert("Authorization".to_string(), "Bearer token123".to_string());

        let tentacle = WebSocketTentacle::new(
            "test_ws".to_string(),
            WebSocketTentacleConfig {
                url: "wss://example.com/ws".to_string(),
                topic_mode: WsTopicMode::Field,
                fallback_topic: "/ws/default".to_string(),
                topic_field: None,
                body_field: None,
                headers,
                subprotocol: Some("graphql-ws".to_string()),
                reconnect: true,
                reconnect_delay_seconds: 5,
                max_message_size_mb: 10,
            },
        );
        let request = tentacle.build_request().unwrap();
        assert_eq!(
            request
                .headers()
                .get("Authorization")
                .unwrap()
                .to_str()
                .unwrap(),
            "Bearer token123"
        );
        assert_eq!(
            request
                .headers()
                .get("Sec-WebSocket-Protocol")
                .unwrap()
                .to_str()
                .unwrap(),
            "graphql-ws"
        );
    }

    #[test]
    fn test_binary_message_encoding() {
        let data = vec![0x00, 0x01, 0x02, 0xFF];
        let encoded = base64::Engine::encode(&base64::engine::general_purpose::STANDARD, &data);
        let decoded =
            base64::Engine::decode(&base64::engine::general_purpose::STANDARD, &encoded).unwrap();
        assert_eq!(decoded, data);
    }
}
