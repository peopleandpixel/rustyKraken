use anyhow::Result;
use async_trait::async_trait;
use http::Request as HttpRequest;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::{debug, info, warn};

use super::Tentacle;
use crate::eventbus::EventBus;
use crate::model::Ink;

/// Konfiguration für die ntfy-Verbindung
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NtfyTentacleConfig {
    /// ntfy-Server-URL (z.B. "https://ntfy.sh" oder "http://localhost:8080")
    pub server: String,
    /// Topics, auf die abonniert werden soll (kommasepariert oder als Liste)
    pub topics: Vec<String>,
    /// Optional: Username für Authentifizierung
    #[serde(default)]
    pub username: Option<String>,
    /// Optional: Passwort für Authentifizierung
    #[serde(default)]
    pub password: Option<String>,
    /// Optional: Auth-Token (Bearer Token)
    #[serde(default)]
    pub token: Option<String>,
    /// Verbindungsart: "json" (JSON Stream, Standard) oder "websocket"
    #[serde(default = "default_connection_type")]
    pub connection_type: String,
    /// Optional: Topic-Mapping von ntfy-Topics auf rustyKraken-Topics
    #[serde(default)]
    pub topic_mapping: Option<std::collections::HashMap<String, String>>,
    /// Optional: Fallback-Topic, wenn kein Mapping gefunden wird
    #[serde(default)]
    pub fallback_topic: Option<String>,
    /// Optional: Verzögerung bei Reconnection in Sekunden (Standard: 5)
    #[serde(default = "default_reconnect_delay")]
    pub reconnect_delay: u64,
    /// Optional: Maximale Anzahl von Reconnection-Versuchen (0 = unendlich)
    #[serde(default)]
    pub max_reconnect_attempts: u32,
}

fn default_connection_type() -> String {
    "json".to_string()
}

fn default_reconnect_delay() -> u64 {
    5
}

/// ntfy-Nachrichtenformat (wie vom JSON Stream zurückgegeben)
/// Einige Felder sind für zukünftige Erweiterungen vorgesehen.
#[allow(dead_code)]
#[derive(Debug, Clone, Deserialize)]
struct NtfyMessage {
    #[serde(default)]
    pub id: Option<String>,
    #[serde(default)]
    pub topic: Option<String>,
    #[serde(default)]
    pub title: Option<String>,
    #[serde(default)]
    pub message: Option<String>,
    #[serde(default)]
    pub priority: Option<u8>,
    #[serde(default)]
    pub tags: Option<Vec<String>>,
    #[serde(default)]
    pub actions: Option<Vec<serde_json::Value>>,
    #[serde(default)]
    pub click: Option<String>,
    #[serde(default)]
    pub icon: Option<String>,
    #[serde(default)]
    pub tags_shortcodes: Option<Vec<String>>,
    #[serde(default)]
    pub markdown: Option<bool>,
    #[serde(default)]
    pub delay: Option<String>,
    #[serde(default)]
    pub email: Option<String>,
    #[serde(default)]
    pub filename: Option<String>,
    #[serde(default)]
    pub attach: Option<String>,
    #[serde(default)]
    pub to: Option<String>,
    #[serde(default)]
    pub cache: Option<bool>,
    #[serde(default)]
    pub expire: Option<u64>,
    #[serde(default)]
    pub event: Option<String>,
    #[serde(default)]
    pub time: Option<u64>,
    #[serde(default)]
    pub expires: Option<u64>,
}

pub struct NtfyTentacle {
    name: String,
    config: NtfyTentacleConfig,
}

impl NtfyTentacle {
    pub fn new(name: String, config: NtfyTentacleConfig) -> Self {
        Self { name, config }
    }

    /// Bestimmt den rustyKraken-Topic aus einem ntfy-Topic
    fn resolve_topic(&self, ntfy_topic: &str) -> String {
        if let Some(mapping) = &self.config.topic_mapping
            && let Some(kraken_topic) = mapping.get(ntfy_topic)
        {
            return kraken_topic.clone();
        }
        // Fallback: ntfy-Topic als kraken-Topic verwenden (mit führendem /)
        if ntfy_topic.starts_with('/') {
            ntfy_topic.to_string()
        } else {
            format!("/{}", ntfy_topic)
        }
    }

    /// Erstellt eine Ink-Nachricht aus einer ntfy-Nachricht
    fn create_ink(&self, msg: &NtfyMessage) -> Result<Ink> {
        let topic = msg.topic.as_deref().unwrap_or("unknown").to_string();
        let kraken_topic = self.resolve_topic(&topic);

        let body = msg.message.as_deref().unwrap_or("").to_string();

        let mut metadata = serde_json::Map::new();
        if let Some(title) = &msg.title {
            metadata.insert("title".into(), serde_json::Value::String(title.clone()));
        }
        if let Some(priority) = msg.priority {
            metadata.insert(
                "priority".into(),
                serde_json::Value::Number(priority.into()),
            );
        }
        if let Some(tags) = &msg.tags {
            metadata.insert("tags".into(), serde_json::json!(tags));
        }
        if let Some(click) = &msg.click {
            metadata.insert("click".into(), serde_json::Value::String(click.clone()));
        }
        if let Some(icon) = &msg.icon {
            metadata.insert("icon".into(), serde_json::Value::String(icon.clone()));
        }
        if let Some(id) = &msg.id {
            metadata.insert("ntfy_id".into(), serde_json::Value::String(id.clone()));
        }

        let ink = Ink::new(kraken_topic, "ntfy", &self.name, body)
            .with_metadata(serde_json::Value::Object(metadata));

        Ok(ink)
    }

    /// Subscribiert via JSON Stream (HTTP long-polling)
    async fn subscribe_json_stream(&self, event_bus: Arc<EventBus>) -> Result<()> {
        let server = self.config.server.clone();
        let topics: Vec<&str> = self.config.topics.iter().map(|s| s.as_str()).collect();
        let topics_str = topics.join(",");
        let url = format!("{}/{}/json", server.trim_end_matches('/'), topics_str);

        let mut attempts = 0u32;
        let max_attempts = self.config.max_reconnect_attempts;
        let reconnect_delay = self.config.reconnect_delay;

        loop {
            attempts += 1;
            info!(
                "ntfy Tentacle '{}': Connecting to JSON stream: {} (attempt {})",
                self.name, url, attempts
            );

            let mut req = reqwest::Client::new()
                .get(&url)
                .header("Accept", "application/x-ndjson");

            // Authentifizierung hinzufügen
            req = self.add_auth_headers(req);

            match req.send().await {
                Ok(resp) => {
                    info!("ntfy Tentacle '{}': Connected to JSON stream", self.name);
                    let status = resp.status();
                    if !status.is_success() {
                        warn!(
                            "ntfy Tentacle '{}': Server returned status {}",
                            self.name, status
                        );
                        self.handle_reconnect(attempts, max_attempts, reconnect_delay)
                            .await?;
                        continue;
                    }

                    // Stream zeilenweise lesen
                    let stream = resp.bytes_stream();
                    use futures_util::StreamExt;

                    let mut line_buf = String::new();
                    tokio::pin!(stream);

                    while let Some(chunk_result) = stream.next().await {
                        match chunk_result {
                            Ok(chunk) => {
                                let chunk_str = String::from_utf8_lossy(&chunk);
                                line_buf.push_str(&chunk_str);

                                // Zeilenweise verarbeiten
                                while let Some(pos) = line_buf.find('\n') {
                                    let line = line_buf[..pos].to_string();
                                    line_buf.drain(..=pos);

                                    if line.trim().is_empty() {
                                        continue;
                                    }

                                    self.process_line(&line, &event_bus).await;
                                }
                            }
                            Err(e) => {
                                warn!("ntfy Tentacle '{}': Stream error: {}", self.name, e);
                                break;
                            }
                        }
                    }

                    warn!(
                        "ntfy Tentacle '{}': JSON stream closed, reconnecting...",
                        self.name
                    );
                }
                Err(e) => {
                    warn!(
                        "ntfy Tentacle '{}': Failed to connect to JSON stream: {}",
                        self.name, e
                    );
                }
            }

            // Reconnection-Logik
            self.handle_reconnect(attempts, max_attempts, reconnect_delay)
                .await?;
        }
    }

    /// Subscribiert via WebSocket
    async fn subscribe_websocket(&self, event_bus: Arc<EventBus>) -> Result<()> {
        let server = self.config.server.clone();
        let topics: Vec<&str> = self.config.topics.iter().map(|s| s.as_str()).collect();
        let topics_str = topics.join(",");

        // WebSocket-URL bauen
        let ws_scheme = if server.starts_with("https://") {
            "wss://"
        } else {
            "ws://"
        };
        let ws_host = server
            .trim_start_matches("https://")
            .trim_start_matches("http://");
        let url = format!("{}{}/{}/ws", ws_scheme, ws_host, topics_str);

        let mut attempts = 0u32;
        let max_attempts = self.config.max_reconnect_attempts;
        let reconnect_delay = self.config.reconnect_delay;

        loop {
            attempts += 1;
            info!(
                "ntfy Tentacle '{}': Connecting to WebSocket: {} (attempt {})",
                self.name, url, attempts
            );

            // WebSocket-Verbindung mit connect_async
            let mut request = HttpRequest::builder()
                .uri(&url)
                .header("Accept", "application/json");

            // Basic Auth, wenn konfiguriert
            if let (Some(username), Some(password)) = (&self.config.username, &self.config.password)
            {
                let creds = format!("{}:{}", username, password);
                let encoded = base64::Engine::encode(
                    &base64::engine::general_purpose::STANDARD,
                    creds.as_bytes(),
                );
                request = request.header("Authorization", format!("Basic {}", encoded));
            } else if let Some(token) = &self.config.token {
                request = request.header("Authorization", format!("Bearer {}", token));
            }

            let request = request
                .body(())
                .map_err(|e| anyhow::anyhow!("Request build error: {}", e))?;

            match tokio_tungstenite::connect_async(request).await {
                Ok((ws_stream, _)) => {
                    info!("ntfy Tentacle '{}': Connected to WebSocket", self.name);

                    let (write, read) = ws_stream.split();
                    drop(write); // Wir brauchen nur lesen

                    use futures_util::StreamExt;
                    tokio::pin!(read);

                    while let Some(msg_result) = read.next().await {
                        match msg_result {
                            Ok(tokio_tungstenite::tungstenite::Message::Text(text)) => {
                                if text.trim().is_empty() {
                                    continue;
                                }
                                self.process_line(&text, &event_bus).await;
                            }
                            Ok(tokio_tungstenite::tungstenite::Message::Binary(_)) => {
                                debug!("ntfy Tentacle '{}': Ignoring binary message", self.name);
                            }
                            Ok(tokio_tungstenite::tungstenite::Message::Close(_)) => {
                                info!("ntfy Tentacle '{}': WebSocket closed by server", self.name);
                                break;
                            }
                            Ok(_) => {}
                            Err(e) => {
                                warn!("ntfy Tentacle '{}': WebSocket error: {}", self.name, e);
                                break;
                            }
                        }
                    }

                    warn!(
                        "ntfy Tentacle '{}': WebSocket disconnected, reconnecting...",
                        self.name
                    );
                }
                Err(e) => {
                    warn!(
                        "ntfy Tentacle '{}': Failed to connect to WebSocket: {}",
                        self.name, e
                    );
                }
            }

            // Reconnection-Logik
            self.handle_reconnect(attempts, max_attempts, reconnect_delay)
                .await?;
        }
    }

    /// Verarbeitet eine einzelne JSON-Zeile vom ntfy-Stream
    async fn process_line(&self, line: &str, event_bus: &EventBus) {
        // Event-Typ prüfen (WebSocket hat ein "event"-Feld)
        if let Ok(msg) = serde_json::from_str::<NtfyMessage>(line) {
            // Bei WebSocket: nur "message"-Events verarbeiten
            if let Some(event) = &msg.event
                && event != "message"
            {
                debug!(
                    "ntfy Tentacle '{}': Ignoring event type: {}",
                    self.name, event
                );
                return;
            }

            match self.create_ink(&msg) {
                Ok(ink) => {
                    info!(
                        "ntfy Tentacle '{}': Received message on topic: {}",
                        self.name, ink.topic
                    );
                    event_bus.publish(ink);
                }
                Err(e) => {
                    warn!(
                        "ntfy Tentacle '{}': Failed to create ink from message: {}",
                        self.name, e
                    );
                }
            }
        } else {
            debug!(
                "ntfy Tentacle '{}': Failed to parse JSON line: {}",
                self.name, line
            );
        }
    }

    /// Fügt Authentifizierungs-Header hinzu
    fn add_auth_headers(&self, mut req: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        if let (Some(username), Some(password)) = (&self.config.username, &self.config.password) {
            let creds = format!("{}:{}", username, password);
            let encoded = base64::Engine::encode(
                &base64::engine::general_purpose::STANDARD,
                creds.as_bytes(),
            );
            req = req.header("Authorization", format!("Basic {}", encoded));
        } else if let Some(token) = &self.config.token {
            req = req.header("Authorization", format!("Bearer {}", token));
        }
        req
    }

    /// Handhabt Reconnection-Logik
    async fn handle_reconnect(
        &self,
        attempts: u32,
        max_attempts: u32,
        reconnect_delay: u64,
    ) -> Result<()> {
        if max_attempts > 0 && attempts >= max_attempts {
            anyhow::bail!(
                "ntfy Tentacle '{}': Max reconnection attempts ({}) reached",
                self.name,
                max_attempts
            );
        }

        info!(
            "ntfy Tentacle '{}': Reconnecting in {} seconds...",
            self.name, reconnect_delay
        );
        tokio::time::sleep(std::time::Duration::from_secs(reconnect_delay)).await;
        Ok(())
    }
}

#[allow(dead_code)]
#[async_trait]
impl Tentacle for NtfyTentacle {
    fn name(&self) -> &str {
        &self.name
    }

    fn r#type(&self) -> &str {
        "ntfy"
    }

    async fn start(&self, event_bus: Arc<EventBus>) -> Result<()> {
        info!(
            "ntfy Tentacle '{}': Starting (connection: {})",
            self.name, self.config.connection_type
        );

        match self.config.connection_type.as_str() {
            "websocket" | "ws" => self.subscribe_websocket(event_bus).await,
            _ => self.subscribe_json_stream(event_bus).await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resolve_topic_with_mapping() {
        let mut mapping = std::collections::HashMap::new();
        mapping.insert("alerts".to_string(), "/kraken/alerts".to_string());
        mapping.insert("status".to_string(), "/kraken/status".to_string());

        let config = NtfyTentacleConfig {
            server: "https://ntfy.sh".to_string(),
            topics: vec!["alerts".to_string()],
            username: None,
            password: None,
            token: None,
            connection_type: "json".to_string(),
            topic_mapping: Some(mapping),
            fallback_topic: None,
            reconnect_delay: 5,
            max_reconnect_attempts: 0,
        };
        let tentacle = NtfyTentacle::new("test".to_string(), config);

        assert_eq!(tentacle.resolve_topic("alerts"), "/kraken/alerts");
        assert_eq!(tentacle.resolve_topic("status"), "/kraken/status");
        // Kein Mapping -> Fallback
        assert_eq!(tentacle.resolve_topic("unknown"), "/unknown");
    }

    #[test]
    fn test_resolve_topic_without_mapping() {
        let config = NtfyTentacleConfig {
            server: "https://ntfy.sh".to_string(),
            topics: vec!["test".to_string()],
            username: None,
            password: None,
            token: None,
            connection_type: "json".to_string(),
            topic_mapping: None,
            fallback_topic: None,
            reconnect_delay: 5,
            max_reconnect_attempts: 0,
        };
        let tentacle = NtfyTentacle::new("test".to_string(), config);

        // Ohne Mapping: Topic mit führendem /
        assert_eq!(tentacle.resolve_topic("test"), "/test");
        assert_eq!(
            tentacle.resolve_topic("/already/prefixed"),
            "/already/prefixed"
        );
    }

    #[test]
    fn test_create_ink_from_ntfy_message() {
        let config = NtfyTentacleConfig {
            server: "https://ntfy.sh".to_string(),
            topics: vec!["alerts".to_string()],
            username: None,
            password: None,
            token: None,
            connection_type: "json".to_string(),
            topic_mapping: None,
            fallback_topic: None,
            reconnect_delay: 5,
            max_reconnect_attempts: 0,
        };
        let tentacle = NtfyTentacle::new("test".to_string(), config);

        let msg = NtfyMessage {
            id: Some("msg123".to_string()),
            topic: Some("alerts".to_string()),
            title: Some("Disk Space Low".to_string()),
            message: Some("Only 5GB remaining".to_string()),
            priority: Some(4),
            tags: Some(vec!["warning".to_string(), "cd".to_string()]),
            actions: None,
            click: Some("https://example.com/admin".to_string()),
            icon: Some("https://example.com/icon.png".to_string()),
            tags_shortcodes: None,
            markdown: None,
            delay: None,
            email: None,
            filename: None,
            attach: None,
            to: None,
            cache: None,
            expire: None,
            event: None,
            time: None,
            expires: None,
        };

        let ink = tentacle.create_ink(&msg).unwrap();

        assert_eq!(ink.topic, "/alerts");
        assert_eq!(ink.body, "Only 5GB remaining");
        assert_eq!(ink.source.r#type, "ntfy");
        assert_eq!(ink.source.name, "test");

        let metadata = ink.metadata.unwrap();
        assert_eq!(
            metadata.get("title").unwrap().as_str().unwrap(),
            "Disk Space Low"
        );
        assert_eq!(metadata.get("priority").unwrap().as_u64().unwrap(), 4);
        assert_eq!(metadata.get("ntfy_id").unwrap().as_str().unwrap(), "msg123");
    }

    #[test]
    fn test_parse_ntfy_message_json() {
        let json = r#"{
            "id": "abc123",
            "topic": "home-alerts",
            "title": "Temperature Warning",
            "message": "Temperature too high: 42°C",
            "priority": 5,
            "tags": ["warning", "thermometer"],
            "click": "https://home.example.com"
        }"#;

        let msg: NtfyMessage = serde_json::from_str(json).unwrap();
        assert_eq!(msg.id, Some("abc123".to_string()));
        assert_eq!(msg.topic, Some("home-alerts".to_string()));
        assert_eq!(msg.title, Some("Temperature Warning".to_string()));
        assert_eq!(msg.message, Some("Temperature too high: 42°C".to_string()));
        assert_eq!(msg.priority, Some(5));
        assert_eq!(
            msg.tags,
            Some(vec!["warning".to_string(), "thermometer".to_string()])
        );
    }

    #[test]
    fn test_parse_ntfy_websocket_event() {
        // WebSocket-Event mit "event"-Feld
        let json = r#"{
            "id": "hwQ2YpKdmg",
            "time": 1635528741,
            "event": "message",
            "topic": "disk-alerts",
            "message": "Disk space low"
        }"#;

        let msg: NtfyMessage = serde_json::from_str(json).unwrap();
        assert_eq!(msg.event, Some("message".to_string()));
        assert_eq!(msg.topic, Some("disk-alerts".to_string()));

        // Nicht-message Event
        let json_keepalive = r#"{
            "event": "keepalive",
            "topic": "disk-alerts"
        }"#;
        let msg: NtfyMessage = serde_json::from_str(json_keepalive).unwrap();
        assert_eq!(msg.event, Some("keepalive".to_string()));
    }
}
