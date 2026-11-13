use anyhow::Result;
use async_trait::async_trait;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info, warn};

use super::Tentacle;
use crate::eventbus::EventBus;
use crate::model::Ink;

/// gRPC Aufruftyp
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "lowercase")]
pub enum GrpcCallType {
    /// Einfacher Unary-Aufruf (Request/Response)
    #[default]
    Unary,
    /// Server-Streaming (ein Request, viele Responses)
    ServerStreaming,
    /// Client-Streaming (viele Requests, eine Response)
    ClientStreaming,
    /// Bidirektionales Streaming
    Bidirectional,
}

/// Topic-Erkennungsmodus für gRPC-Nachrichten
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "lowercase")]
pub enum GrpcTopicMode {
    /// Topic aus dem Method-Namen ableiten
    #[default]
    Method,
    /// Topic aus einem JSON-Feld extrahieren
    Field,
    /// Alle Nachrichten an ein festes Topic
    All,
}

/// Authentifizierungstyp für gRPC
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum GrpcAuth {
    /// Keine Authentifizierung
    #[default]
    None,
    /// Bearer Token
    Bearer { token: String },
    /// API Key als Header
    ApiKey { header: String, value: String },
    /// Custom Header
    Custom { header: String, value: String },
}

/// Konfiguration für den gRPC Tentacle
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GrpcTentacleConfig {
    /// gRPC Server-URL (z.B. "http://localhost:50051")
    #[serde(default = "default_endpoint")]
    pub endpoint: String,
    /// gRPC Service.Name (z.B. "helloworld.Greeter")
    pub service_method: String,
    /// Aufruftyp
    #[serde(default)]
    pub call_type: GrpcCallType,
    /// Topic-Erkennungsmodus
    #[serde(default)]
    pub topic_mode: GrpcTopicMode,
    /// JSON-Feld für Topic-Extraktion (bei field-Modus)
    #[serde(default)]
    pub topic_field: Option<String>,
    /// Fallback-Topic
    #[serde(default = "default_fallback_topic")]
    pub fallback_topic: String,
    /// Optional: Method-basiertes Topic-Mapping
    #[serde(default)]
    pub method_topic_mapping: HashMap<String, String>,
    /// Authentifizierung
    #[serde(default)]
    pub auth: GrpcAuth,
    /// Request-Payload (JSON-String für Unary/Client-Streaming)
    #[serde(default)]
    pub request_payload: Option<String>,
    /// Polling-Interval in Sekunden (für Unary-Mode)
    #[serde(default = "default_poll_interval")]
    pub poll_interval_secs: u64,
    /// Streaming-Timeout in Sekunden (0 = kein Timeout)
    #[serde(default)]
    pub streaming_timeout_secs: u64,
    /// Maximale Nachrichten pro Stream-Session
    #[serde(default = "default_max_messages")]
    pub max_messages_per_session: u32,
    /// Reconnect-Verzögerung in Sekunden
    #[serde(default = "default_reconnect_delay")]
    pub reconnect_delay_secs: u64,
    /// TLS aktivieren
    #[serde(default)]
    pub tls_enabled: bool,
    /// TLS CA-Zertifikat Pfad (optional)
    #[serde(default)]
    pub tls_ca_path: Option<String>,
    /// Additional gRPC-Metadata als Header
    #[serde(default)]
    pub metadata: HashMap<String, String>,
}

fn default_endpoint() -> String {
    "http://localhost:50051".to_string()
}

fn default_fallback_topic() -> String {
    "/grpc/default".to_string()
}

fn default_poll_interval() -> u64 {
    30
}

fn default_max_messages() -> u32 {
    10000
}

fn default_reconnect_delay() -> u64 {
    5
}

pub struct GrpcTentacle {
    name: String,
    config: GrpcTentacleConfig,
}

impl GrpcTentacle {
    pub fn new(name: String, config: GrpcTentacleConfig) -> Self {
        Self { name, config }
    }
}

/// Freistehende Funktion: Bestimmt das Topic basierend auf der gRPC-Nachricht
fn resolve_topic(config: &GrpcTentacleConfig, method: &str, response_body: &str) -> String {
    match config.topic_mode {
        GrpcTopicMode::Method => {
            if let Some(topic) = config.method_topic_mapping.get(method) {
                return topic.clone();
            }
            // Aus "helloworld.Greeter.SayHello" wird "/grpc/helloworld/Greeter/SayHello"
            let parts: Vec<&str> = method.split('.').collect();
            if parts.len() >= 2 {
                format!("/grpc/{}/{}", parts[0], parts[1])
            } else {
                format!("/grpc/{}", method)
            }
        }
        GrpcTopicMode::Field => {
            if let Some(ref field) = config.topic_field
                && let Ok(json) = serde_json::from_str::<serde_json::Value>(response_body)
                && let Some(val) = json.get(field)
                && let Some(str_val) = val.as_str()
            {
                return str_val.to_string();
            }
            config.fallback_topic.clone()
        }
        GrpcTopicMode::All => config.fallback_topic.clone(),
    }
}

/// Freistehende Funktion: Erstellt gRPC-JSON-Request-Header
fn grpc_headers(config: &GrpcTentacleConfig) -> HashMap<String, String> {
    let mut headers = HashMap::new();
    headers.insert(
        "Content-Type".to_string(),
        "application/grpc+json".to_string(),
    );
    headers.insert("Te".to_string(), "trailers".to_string());

    match &config.auth {
        GrpcAuth::None => {}
        GrpcAuth::Bearer { token } => {
            headers.insert("Authorization".to_string(), format!("Bearer {}", token));
        }
        GrpcAuth::ApiKey { header, value } => {
            headers.insert(header.clone(), value.clone());
        }
        GrpcAuth::Custom { header, value } => {
            headers.insert(header.clone(), value.clone());
        }
    }

    for (key, value) in &config.metadata {
        headers.insert(key.clone(), value.clone());
    }

    headers
}

/// Freistehende Funktion: Prüft ob eine gRPC-Response erfolgreich war
fn is_grpc_success(status_code: u16) -> bool {
    // gRPC-JSON verwendet HTTP-Status-Codes
    // 200 = OK, andere Codes sind Fehler
    status_code == 200
}

/// Freistehende Funktion: Extrahiert gRPC-Status aus Response
fn extract_grpc_status(body: &str) -> Option<String> {
    if let Ok(json) = serde_json::from_str::<serde_json::Value>(body) {
        if let Some(error) = json.get("error") {
            return Some(error.to_string());
        }
        if let Some(status) = json.get("status") {
            return Some(status.to_string());
        }
    }
    None
}

#[async_trait]
impl Tentacle for GrpcTentacle {
    fn name(&self) -> &str {
        &self.name
    }

    fn r#type(&self) -> &str {
        "grpc"
    }

    async fn start(&self, event_bus: Arc<EventBus>) -> Result<()> {
        info!(
            "gRPC Tentacle '{}' starting for service '{}'",
            self.name, self.config.service_method
        );

        let config = self.config.clone();
        let tentacle_name = self.name.clone();
        let event_bus_clone = Arc::clone(&event_bus);

        // Client erstellen
        let client_builder = Client::builder();
        if config.tls_enabled
            && let Some(ref ca_path) = config.tls_ca_path
        {
            info!(
                "gRPC Tentacle '{}': loading CA certificate from '{}'",
                tentacle_name, ca_path
            );
            let _ = std::fs::read_to_string(ca_path).map_err(|e| {
                warn!(
                    "gRPC Tentacle '{}': failed to load CA: {}",
                    tentacle_name, e
                )
            });
        }

        let client = client_builder
            .timeout(Duration::from_secs(60))
            .build()
            .map_err(|e| anyhow::anyhow!("Failed to build HTTP client: {}", e))?;

        let headers = grpc_headers(&config);
        let method = config.service_method.clone();

        match config.call_type {
            GrpcCallType::Unary => {
                // Polling-Modus für Unary-Aufrufe
                tokio::spawn(async move {
                    loop {
                        match send_unary_call(&client, &config, &method, &tentacle_name, &headers)
                            .await
                        {
                            Ok(response_body) => {
                                let topic = resolve_topic(&config, &method, &response_body);
                                let ink = create_grpc_ink(
                                    &topic,
                                    &tentacle_name,
                                    &method,
                                    &response_body,
                                    "unary",
                                );
                                event_bus_clone.publish(ink);
                            }
                            Err(e) => {
                                error!(
                                    "gRPC Tentacle '{}': unary call failed: {}",
                                    tentacle_name, e
                                );
                            }
                        }

                        tokio::time::sleep(Duration::from_secs(config.poll_interval_secs)).await;
                    }
                });
            }
            GrpcCallType::ServerStreaming => {
                // Server-Streaming-Modus
                tokio::spawn(async move {
                    loop {
                        match start_server_stream(
                            &client,
                            &config,
                            &method,
                            &tentacle_name,
                            &headers,
                            &event_bus_clone,
                        )
                        .await
                        {
                            Ok(count) => {
                                info!(
                                    "gRPC Tentacle '{}': stream session completed with {} messages",
                                    tentacle_name, count
                                );
                            }
                            Err(e) => {
                                error!(
                                    "gRPC Tentacle '{}': stream session failed: {}",
                                    tentacle_name, e
                                );
                            }
                        }

                        if config.streaming_timeout_secs == 0 {
                            // Unendlicher Stream, reconnect nach Fehler
                            warn!(
                                "gRPC Tentacle '{}': stream ended, reconnecting in {} seconds...",
                                tentacle_name, config.reconnect_delay_secs
                            );
                        }
                        tokio::time::sleep(Duration::from_secs(config.reconnect_delay_secs)).await;
                    }
                });
            }
            GrpcCallType::ClientStreaming | GrpcCallType::Bidirectional => {
                warn!(
                    "gRPC Tentacle '{}': {:?} is not fully supported in JSON-Transcoding mode, falling back to unary polling",
                    tentacle_name, config.call_type
                );
                // Fallback auf Unary-Polling
                tokio::spawn(async move {
                    loop {
                        match send_unary_call(&client, &config, &method, &tentacle_name, &headers)
                            .await
                        {
                            Ok(response_body) => {
                                let topic = resolve_topic(&config, &method, &response_body);
                                let ink = create_grpc_ink(
                                    &topic,
                                    &tentacle_name,
                                    &method,
                                    &response_body,
                                    "streaming_fallback",
                                );
                                event_bus_clone.publish(ink);
                            }
                            Err(e) => {
                                error!("gRPC Tentacle '{}': call failed: {}", tentacle_name, e);
                            }
                        }

                        tokio::time::sleep(Duration::from_secs(config.poll_interval_secs)).await;
                    }
                });
            }
        }

        // Blockiere den Task, damit er läuft
        loop {
            tokio::time::sleep(Duration::from_secs(60)).await;
        }
    }
}

/// Freistehende Funktion: Sendet einen Unary-gRPC-Aufruf
async fn send_unary_call(
    client: &Client,
    config: &GrpcTentacleConfig,
    method: &str,
    tentacle_name: &str,
    headers: &HashMap<String, String>,
) -> Result<String> {
    let url = format!("{}/{}", config.endpoint.trim_end_matches('/'), method);

    debug!("gRPC Tentacle '{}': calling {}", tentacle_name, url);

    let mut request_builder = client.post(&url);

    for (key, value) in headers {
        request_builder = request_builder.header(key, value);
    }

    if let Some(ref payload) = config.request_payload {
        request_builder = request_builder.body(payload.clone());
    }

    let response = request_builder.send().await?;
    let status = response.status().as_u16();
    let body = response.text().await?;

    if !is_grpc_success(status) {
        return Err(anyhow::anyhow!(
            "gRPC call failed with status {}: {}",
            status,
            body
        ));
    }

    debug!(
        "gRPC Tentacle '{}': received response ({} bytes)",
        tentacle_name,
        body.len()
    );

    Ok(body)
}

/// Freistehende Funktion: Startet einen Server-Streaming-gRPC-Aufruf
async fn start_server_stream(
    client: &Client,
    config: &GrpcTentacleConfig,
    method: &str,
    tentacle_name: &str,
    headers: &HashMap<String, String>,
    event_bus: &Arc<EventBus>,
) -> Result<u32> {
    let url = format!("{}/{}", config.endpoint.trim_end_matches('/'), method);

    info!(
        "gRPC Tentacle '{}': starting server stream to {}",
        tentacle_name, url
    );

    let mut request_builder = client.post(&url);
    for (key, value) in headers {
        request_builder = request_builder.header(key, value);
    }

    if let Some(ref payload) = config.request_payload {
        request_builder = request_builder.body(payload.clone());
    }

    let response = request_builder.send().await?;
    if !is_grpc_success(response.status().as_u16()) {
        return Err(anyhow::anyhow!(
            "gRPC stream failed with status: {}",
            response.status()
        ));
    }

    // Stream lesen
    let mut count = 0u32;
    let max_msgs = config.max_messages_per_session;
    let timeout = if config.streaming_timeout_secs > 0 {
        Some(Duration::from_secs(config.streaming_timeout_secs))
    } else {
        None
    };

    // Für gRPC-JSON-Streaming lesen wir die Response als Text-Stream
    let mut stream = response.bytes_stream();

    use futures_util::StreamExt;
    let mut buffer = String::new();

    while let Some(chunk_result) = stream.next().await {
        // Timeout-Prüfung
        if let Some(to) = timeout
            && tokio::time::timeout(to, async {}).await.is_err()
        {
            warn!("gRPC Tentacle '{}': stream timeout reached", tentacle_name);
            break;
        }

        match chunk_result {
            Ok(bytes) => {
                let text = String::from_utf8_lossy(&bytes).to_string();
                buffer.push_str(&text);

                // Versuche, vollständige JSON-Objekte zu parsen
                while let Some(pos) = buffer.rfind('}') {
                    let candidate = &buffer[..=pos];
                    if serde_json::from_str::<serde_json::Value>(candidate).is_ok() {
                        let json_str = candidate.to_string();
                        buffer.drain(..=pos);

                        count += 1;
                        if max_msgs > 0 && count > max_msgs {
                            info!(
                                "gRPC Tentacle '{}': reached max messages ({})",
                                tentacle_name, max_msgs
                            );
                            break;
                        }

                        let topic = resolve_topic(config, method, &json_str);
                        let ink = create_grpc_ink(
                            &topic,
                            tentacle_name,
                            method,
                            &json_str,
                            "server_stream",
                        );
                        event_bus.publish(ink);

                        debug!(
                            "gRPC Tentacle '{}': stream message {}/{}",
                            tentacle_name, count, max_msgs
                        );
                    } else {
                        break;
                    }
                }
            }
            Err(e) => {
                error!(
                    "gRPC Tentacle '{}': stream chunk error: {}",
                    tentacle_name, e
                );
                break;
            }
        }

        if max_msgs > 0 && count >= max_msgs {
            break;
        }
    }

    Ok(count)
}

/// Freistehende Funktion: Erstellt eine Ink-Nachricht aus gRPC-Daten
fn create_grpc_ink(
    topic: &str,
    tentacle_name: &str,
    method: &str,
    body: &str,
    call_type: &str,
) -> Ink {
    let mut metadata = serde_json::Map::new();
    metadata.insert(
        "grpc_method".to_string(),
        serde_json::Value::String(method.to_string()),
    );
    metadata.insert(
        "call_type".to_string(),
        serde_json::Value::String(call_type.to_string()),
    );

    // Versuche, gRPC-Status zu extrahieren
    if let Some(status) = extract_grpc_status(body) {
        metadata.insert("grpc_status".to_string(), serde_json::Value::String(status));
    }

    let mut ink = Ink::new(topic, "grpc", tentacle_name, body.trim().to_string());
    ink = ink.with_metadata(serde_json::Value::Object(metadata));
    ink
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::appcontext::AppContext;

    fn make_config() -> GrpcTentacleConfig {
        GrpcTentacleConfig {
            endpoint: "http://localhost:50051".to_string(),
            service_method: "helloworld.Greeter.SayHello".to_string(),
            call_type: GrpcCallType::Unary,
            topic_mode: GrpcTopicMode::Method,
            topic_field: None,
            fallback_topic: "/grpc/default".to_string(),
            method_topic_mapping: HashMap::new(),
            auth: GrpcAuth::None,
            request_payload: None,
            poll_interval_secs: 30,
            streaming_timeout_secs: 0,
            max_messages_per_session: 10000,
            reconnect_delay_secs: 5,
            tls_enabled: false,
            tls_ca_path: None,
            metadata: HashMap::new(),
        }
    }

    #[test]
    fn test_grpc_topic_mode_method() {
        let config = make_config();
        assert_eq!(
            resolve_topic(&config, "helloworld.Greeter.SayHello", "{}"),
            "/grpc/helloworld/Greeter"
        );
    }

    #[test]
    fn test_grpc_topic_mode_method_mapping() {
        let mut config = make_config();
        let mut mapping = HashMap::new();
        mapping.insert(
            "helloworld.Greeter.SayHello".to_string(),
            "/greetings/incoming".to_string(),
        );
        config.method_topic_mapping = mapping;

        assert_eq!(
            resolve_topic(&config, "helloworld.Greeter.SayHello", "{}"),
            "/greetings/incoming"
        );
    }

    #[test]
    fn test_grpc_topic_mode_field() {
        let mut config = make_config();
        config.topic_mode = GrpcTopicMode::Field;
        config.topic_field = Some("event_type".to_string());

        let body = r#"{"event_type": "user.login", "user": "alice"}"#;
        assert_eq!(resolve_topic(&config, "service.Method", body), "user.login");
    }

    #[test]
    fn test_grpc_topic_mode_field_missing() {
        let mut config = make_config();
        config.topic_mode = GrpcTopicMode::Field;
        config.topic_field = Some("event_type".to_string());

        let body = r#"{"other_field": "value"}"#;
        assert_eq!(
            resolve_topic(&config, "service.Method", body),
            "/grpc/default"
        );
    }

    #[test]
    fn test_grpc_topic_mode_all() {
        let mut config = make_config();
        config.topic_mode = GrpcTopicMode::All;
        config.fallback_topic = "/grpc/all".to_string();

        assert_eq!(resolve_topic(&config, "service.Method", "{}"), "/grpc/all");
    }

    #[test]
    fn test_grpc_headers_none_auth() {
        let config = make_config();
        let headers = grpc_headers(&config);
        assert_eq!(
            headers.get("Content-Type"),
            Some(&"application/grpc+json".to_string())
        );
        assert_eq!(headers.get("Te"), Some(&"trailers".to_string()));
        assert!(!headers.contains_key("Authorization"));
    }

    #[test]
    fn test_grpc_headers_bearer_auth() {
        let mut config = make_config();
        config.auth = GrpcAuth::Bearer {
            token: "my-secret-token".to_string(),
        };
        let headers = grpc_headers(&config);
        assert_eq!(
            headers.get("Authorization"),
            Some(&"Bearer my-secret-token".to_string())
        );
    }

    #[test]
    fn test_grpc_headers_api_key_auth() {
        let mut config = make_config();
        config.auth = GrpcAuth::ApiKey {
            header: "X-API-Key".to_string(),
            value: "api-key-123".to_string(),
        };
        let headers = grpc_headers(&config);
        assert_eq!(headers.get("X-API-Key"), Some(&"api-key-123".to_string()));
    }

    #[test]
    fn test_grpc_headers_custom_auth() {
        let mut config = make_config();
        config.auth = GrpcAuth::Custom {
            header: "X-Custom-Auth".to_string(),
            value: "custom-value".to_string(),
        };
        let headers = grpc_headers(&config);
        assert_eq!(
            headers.get("X-Custom-Auth"),
            Some(&"custom-value".to_string())
        );
    }

    #[test]
    fn test_grpc_headers_metadata() {
        let mut config = make_config();
        let mut meta = HashMap::new();
        meta.insert("X-Custom-Header".to_string(), "custom-value".to_string());
        config.metadata = meta;
        let headers = grpc_headers(&config);
        assert_eq!(
            headers.get("X-Custom-Header"),
            Some(&"custom-value".to_string())
        );
    }

    #[test]
    fn test_is_grpc_success() {
        assert!(is_grpc_success(200));
        assert!(!is_grpc_success(400));
        assert!(!is_grpc_success(500));
    }

    #[test]
    fn test_extract_grpc_status_error() {
        let body = r#"{"error": "NOT_FOUND", "message": "User not found"}"#;
        assert_eq!(extract_grpc_status(body), Some("\"NOT_FOUND\"".to_string()));
    }

    #[test]
    fn test_extract_grpc_status_field() {
        let body = r#"{"status": "OK", "data": "result"}"#;
        assert_eq!(extract_grpc_status(body), Some("\"OK\"".to_string()));
    }

    #[test]
    fn test_extract_grpc_status_none() {
        let body = r#"{"data": "result"}"#;
        assert_eq!(extract_grpc_status(body), None);
    }

    #[test]
    fn test_create_grpc_ink() {
        let ink = create_grpc_ink(
            "/grpc/test",
            "test-tentacle",
            "service.Method",
            r#"{"result": "ok"}"#,
            "unary",
        );

        assert_eq!(ink.topic, "/grpc/test");
        assert_eq!(ink.source.r#type, "grpc");
        assert_eq!(ink.source.name, "test-tentacle");

        if let Some(serde_json::Value::Object(meta)) = &ink.metadata {
            assert_eq!(
                meta.get("grpc_method").and_then(|v| v.as_str()),
                Some("service.Method")
            );
            assert_eq!(
                meta.get("call_type").and_then(|v| v.as_str()),
                Some("unary")
            );
        } else {
            panic!("Expected metadata to be an object");
        }
    }

    #[test]
    fn test_grpc_tentacle_new() {
        let config = make_config();
        let tentacle = GrpcTentacle::new("test-grpc".to_string(), config);
        assert_eq!(tentacle.name(), "test-grpc");
        assert_eq!(tentacle.r#type(), "grpc");
    }
}
