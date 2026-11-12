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

/// Authentifizierung
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum PollerAuth {
    #[default]
    None,
    Basic {
        username: String,
        password: String,
    },
    Bearer {
        token: String,
    },
    ApiKey {
        name: String,
        value: String,
    },
}

/// Topic-Modus
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "lowercase")]
pub enum TopicMode {
    #[default]
    Fixed,
    Field,
    Header,
}

/// Endpoint-Konfiguration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PollEndpoint {
    /// URL des Endpoints
    pub url: String,
    /// HTTP-Methode
    #[serde(default = "default_method")]
    pub method: String,
    /// Topic für Events
    #[serde(default = "default_topic")]
    pub topic: String,
    /// Topic-Modus
    #[serde(default)]
    pub topic_mode: TopicMode,
    /// JSON-Feld für Topic (bei topic_mode: field)
    #[serde(default)]
    pub topic_field: Option<String>,
    /// Header-Name für Topic (bei topic_mode: header)
    #[serde(default)]
    pub topic_header: Option<String>,
    /// Custom Headers
    #[serde(default)]
    pub headers: HashMap<String, String>,
    /// Authentifizierung
    #[serde(default)]
    pub auth: PollerAuth,
    /// Polling-Interval in Sekunden (überschreibt globales Interval)
    #[serde(default)]
    pub poll_interval_secs: Option<u64>,
    /// Erwarteter Content-Type
    #[serde(default)]
    pub expected_content_type: Option<String>,
    /// Body für POST/PUT Requests
    #[serde(default)]
    pub body: Option<String>,
}

fn default_method() -> String {
    "GET".to_string()
}

fn default_topic() -> String {
    "/webhooks/poller".to_string()
}

/// Konfiguration für den Webhooks Poller Tentacle
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebhooksPollerTentacleConfig {
    /// Liste der Endpoints
    pub endpoints: Vec<PollEndpoint>,
    /// Globales Polling-Interval in Sekunden
    #[serde(default = "default_poll_interval")]
    pub poll_interval_secs: u64,
    /// Timeout in Sekunden
    #[serde(default = "default_timeout")]
    pub timeout_secs: u64,
    /// Retry-Anzahl bei Fehlern
    #[serde(default = "default_retries")]
    pub max_retries: u32,
    /// Retry-Verzögerung in Sekunden
    #[serde(default = "default_retry_delay")]
    pub retry_delay_secs: u64,
    /// TLS-Zertifikat validieren
    #[serde(default = "default_verify_tls")]
    pub verify_tls: bool,
    /// Globale Custom Headers
    #[serde(default)]
    pub headers: HashMap<String, String>,
    /// Topic-Präfix
    #[serde(default)]
    pub topic_prefix: Option<String>,
}

fn default_poll_interval() -> u64 {
    60
}

fn default_timeout() -> u64 {
    30
}

fn default_retries() -> u32 {
    3
}

fn default_retry_delay() -> u64 {
    5
}

fn default_verify_tls() -> bool {
    true
}

pub struct WebhooksPollerTentacle {
    name: String,
    config: WebhooksPollerTentacleConfig,
}

impl WebhooksPollerTentacle {
    pub fn new(name: String, config: WebhooksPollerTentacleConfig) -> Self {
        Self { name, config }
    }

    #[allow(dead_code)]
    fn build_client(&self) -> Result<Client> {
        Client::builder()
            .timeout(Duration::from_secs(self.config.timeout_secs))
            .danger_accept_invalid_certs(!self.config.verify_tls)
            .build()
            .map_err(|e| anyhow::anyhow!("Failed to build HTTP client: {}", e))
    }

    #[allow(dead_code)]
    fn apply_auth(
        &self,
        req: reqwest::RequestBuilder,
        auth: &PollerAuth,
    ) -> reqwest::RequestBuilder {
        match auth {
            PollerAuth::None => req,
            PollerAuth::Basic { username, password } => req.basic_auth(username, Some(password)),
            PollerAuth::Bearer { token } => req.bearer_auth(token),
            PollerAuth::ApiKey { name, value } => req.header(name, value),
        }
    }

    fn resolve_topic(
        &self,
        endpoint: &PollEndpoint,
        body: &str,
        headers: &HashMap<String, String>,
    ) -> String {
        let base_topic = match &self.config.topic_prefix {
            Some(prefix) => format!("{}/{}", prefix, endpoint.topic),
            None => endpoint.topic.clone(),
        };

        match endpoint.topic_mode {
            TopicMode::Fixed => base_topic,
            TopicMode::Field => {
                if let Some(ref field) = endpoint.topic_field {
                    // Try to extract field from JSON body
                    if let Ok(json) = serde_json::from_str::<serde_json::Value>(body) {
                        let parts: Vec<&str> = field.split('.').collect();
                        let mut val = json;
                        for part in parts {
                            val = match val.get(part) {
                                Some(v) => v.clone(),
                                None => return base_topic,
                            };
                        }
                        if let Some(topic) = val.as_str() {
                            return topic.to_string();
                        }
                    }
                }
                base_topic
            }
            TopicMode::Header => {
                if let Some(ref header_name) = endpoint.topic_header
                    && let Some(topic) = headers.get(header_name)
                {
                    return topic.clone();
                }
                base_topic
            }
        }
    }

    fn create_ink(
        &self,
        endpoint: &PollEndpoint,
        body: &str,
        headers: &HashMap<String, String>,
        status: u16,
    ) -> Ink {
        let topic = self.resolve_topic(endpoint, body, headers);

        let mut meta_map = serde_json::Map::new();
        meta_map.insert("endpoint_url".into(), serde_json::json!(&endpoint.url));
        meta_map.insert("method".into(), serde_json::json!(&endpoint.method));
        meta_map.insert("status_code".into(), serde_json::json!(status));
        meta_map.insert("tentacle".into(), serde_json::json!(&self.name));

        // Try to parse body as JSON for metadata
        if let Ok(json) = serde_json::from_str::<serde_json::Value>(body)
            && let Some(obj) = json.as_object()
        {
            for (key, value) in obj {
                if !meta_map.contains_key(key) {
                    meta_map.insert(key.clone(), value.clone());
                }
            }
        }

        Ink::new(
            &topic,
            "webhooks_poller",
            &self.name,
            format!("Poll response: {} (status {})", endpoint.url, status),
        )
        .with_metadata(serde_json::Value::Object(meta_map))
    }
}

#[async_trait]
impl Tentacle for WebhooksPollerTentacle {
    fn name(&self) -> &str {
        &self.name
    }

    fn r#type(&self) -> &str {
        "webhooks_poller"
    }

    async fn start(&self, event_bus: Arc<EventBus>) -> Result<()> {
        info!(
            "Webhooks Poller Tentacle '{}' starting with {} endpoint(s)",
            self.name,
            self.config.endpoints.len()
        );

        if self.config.endpoints.is_empty() {
            warn!(
                "Webhooks Poller Tentacle '{}': no endpoints configured",
                self.name
            );
            return Ok(());
        }

        let config = self.config.clone();
        let tentacle_name = self.name.clone();
        let event_bus_clone = Arc::clone(&event_bus);

        tokio::spawn(async move {
            let client = match Client::builder()
                .timeout(Duration::from_secs(config.timeout_secs))
                .danger_accept_invalid_certs(!config.verify_tls)
                .build()
            {
                Ok(c) => c,
                Err(e) => {
                    error!(
                        "Webhooks Poller Tentacle '{}': failed to build client: {}",
                        tentacle_name, e
                    );
                    return;
                }
            };

            loop {
                for endpoint in &config.endpoints {
                    let poll_interval = endpoint
                        .poll_interval_secs
                        .unwrap_or(config.poll_interval_secs);

                    poll_endpoint(&client, endpoint, &tentacle_name, &event_bus_clone, &config)
                        .await;

                    // Sleep for this endpoint's interval
                    tokio::time::sleep(Duration::from_secs(poll_interval)).await;
                }
            }
        });

        loop {
            tokio::time::sleep(Duration::from_secs(60)).await;
        }
    }
}

async fn poll_endpoint(
    client: &Client,
    endpoint: &PollEndpoint,
    tentacle_name: &str,
    event_bus: &Arc<EventBus>,
    config: &WebhooksPollerTentacleConfig,
) {
    let mut last_error = None;

    for attempt in 0..=config.max_retries {
        debug!(
            "Webhooks Poller Tentacle '{}': polling {} (attempt {}/{})",
            tentacle_name,
            endpoint.url,
            attempt + 1,
            config.max_retries + 1
        );

        match do_poll(client, endpoint, tentacle_name).await {
            Ok(Some(body)) => {
                // Parse response headers into a HashMap
                let headers = HashMap::new();
                let ink = WebhooksPollerTentacle::new(tentacle_name.to_string(), config.clone())
                    .create_ink(endpoint, &body, &headers, 200);
                event_bus.publish(ink);
                return;
            }
            Ok(None) => {
                debug!(
                    "Webhooks Poller Tentacle '{}': empty response from {}",
                    tentacle_name, endpoint.url
                );
                return;
            }
            Err(e) => {
                last_error = Some(e);
                if attempt < config.max_retries {
                    warn!(
                        "Webhooks Poller Tentacle '{}': poll failed (attempt {}): {}",
                        tentacle_name,
                        attempt + 1,
                        last_error.as_ref().unwrap()
                    );
                    tokio::time::sleep(Duration::from_secs(config.retry_delay_secs)).await;
                }
            }
        }
    }

    error!(
        "Webhooks Poller Tentacle '{}': poll failed after {} retries: {}",
        tentacle_name,
        config.max_retries,
        last_error.unwrap_or_else(|| anyhow::anyhow!("unknown error"))
    );
}

async fn do_poll(
    client: &Client,
    endpoint: &PollEndpoint,
    tentacle_name: &str,
) -> Result<Option<String>> {
    let mut req_builder = match endpoint.method.to_uppercase().as_str() {
        "GET" => client.get(&endpoint.url),
        "POST" => client.post(&endpoint.url),
        "PUT" => client.put(&endpoint.url),
        "DELETE" => client.delete(&endpoint.url),
        "PATCH" => client.patch(&endpoint.url),
        _ => {
            warn!(
                "Webhooks Poller Tentacle '{}': unsupported method: {}",
                tentacle_name, endpoint.method
            );
            return Ok(None);
        }
    };

    // Apply auth
    req_builder = apply_auth_fn(req_builder, &endpoint.auth);

    // Apply custom headers
    for (key, value) in &endpoint.headers {
        req_builder = req_builder.header(key, value);
    }

    // Apply body for POST/PUT/PATCH
    if let Some(ref body) = endpoint.body {
        let method_upper = endpoint.method.to_uppercase();
        if method_upper == "POST" || method_upper == "PUT" || method_upper == "PATCH" {
            req_builder = req_builder.body(body.clone());
        }
    }

    let response = req_builder.send().await?;
    let status = response.status();

    if !status.is_success() {
        return Err(anyhow::anyhow!(
            "HTTP request failed with status: {}",
            status
        ));
    }

    let body = response.text().await?;
    if body.is_empty() {
        return Ok(None);
    }

    Ok(Some(body))
}

fn apply_auth_fn(req: reqwest::RequestBuilder, auth: &PollerAuth) -> reqwest::RequestBuilder {
    match auth {
        PollerAuth::None => req,
        PollerAuth::Basic { username, password } => req.basic_auth(username, Some(password)),
        PollerAuth::Bearer { token } => req.bearer_auth(token),
        PollerAuth::ApiKey { name, value } => req.header(name, value),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn default_config() -> WebhooksPollerTentacleConfig {
        WebhooksPollerTentacleConfig {
            endpoints: vec![PollEndpoint {
                url: "https://api.example.com/data".to_string(),
                method: "GET".to_string(),
                topic: "/webhooks/poller".to_string(),
                topic_mode: TopicMode::Fixed,
                topic_field: None,
                topic_header: None,
                headers: HashMap::new(),
                auth: PollerAuth::None,
                poll_interval_secs: None,
                expected_content_type: None,
                body: None,
            }],
            poll_interval_secs: 60,
            timeout_secs: 30,
            max_retries: 3,
            retry_delay_secs: 5,
            verify_tls: true,
            headers: HashMap::new(),
            topic_prefix: None,
        }
    }

    #[test]
    fn test_config_defaults() {
        let config = default_config();
        assert_eq!(config.poll_interval_secs, 60);
        assert_eq!(config.timeout_secs, 30);
        assert_eq!(config.max_retries, 3);
        assert!(config.verify_tls);
        assert_eq!(config.endpoints.len(), 1);
    }

    #[test]
    fn test_auth_variants() {
        let mut config = default_config();

        // Basic auth
        config.endpoints[0].auth = PollerAuth::Basic {
            username: "user".to_string(),
            password: "pass".to_string(),
        };
        assert!(matches!(config.endpoints[0].auth, PollerAuth::Basic { .. }));

        // Bearer auth
        config.endpoints[0].auth = PollerAuth::Bearer {
            token: "token123".to_string(),
        };
        assert!(matches!(
            config.endpoints[0].auth,
            PollerAuth::Bearer { .. }
        ));

        // API Key auth
        config.endpoints[0].auth = PollerAuth::ApiKey {
            name: "X-API-Key".to_string(),
            value: "key123".to_string(),
        };
        assert!(matches!(
            config.endpoints[0].auth,
            PollerAuth::ApiKey { .. }
        ));
    }

    #[test]
    fn test_topic_mode_fixed() {
        let config = default_config();
        let tentacle = WebhooksPollerTentacle::new("test".to_string(), config);

        let endpoint = &tentacle.config.endpoints[0];
        let topic = tentacle.resolve_topic(endpoint, "{}", &HashMap::new());
        assert_eq!(topic, "/webhooks/poller");
    }

    #[test]
    fn test_topic_mode_field() {
        let mut config = default_config();
        config.endpoints[0].topic_mode = TopicMode::Field;
        config.endpoints[0].topic_field = Some("event.type".to_string());

        let tentacle = WebhooksPollerTentacle::new("test".to_string(), config);
        let endpoint = &tentacle.config.endpoints[0];

        let body = r#"{"event": {"type": "/custom/topic", "id": 123}}"#;
        let topic = tentacle.resolve_topic(endpoint, body, &HashMap::new());
        assert_eq!(topic, "/custom/topic");
    }

    #[test]
    fn test_topic_mode_field_missing() {
        let mut config = default_config();
        config.endpoints[0].topic_mode = TopicMode::Field;
        config.endpoints[0].topic_field = Some("missing.field".to_string());

        let tentacle = WebhooksPollerTentacle::new("test".to_string(), config);
        let endpoint = &tentacle.config.endpoints[0];

        let body = r#"{"event": {"type": "/custom/topic"}}"#;
        let topic = tentacle.resolve_topic(endpoint, body, &HashMap::new());
        assert_eq!(topic, "/webhooks/poller");
    }

    #[test]
    fn test_topic_mode_header() {
        let mut config = default_config();
        config.endpoints[0].topic_mode = TopicMode::Header;
        config.endpoints[0].topic_header = Some("X-Topic".to_string());

        let tentacle = WebhooksPollerTentacle::new("test".to_string(), config);
        let endpoint = &tentacle.config.endpoints[0];

        let mut headers = HashMap::new();
        headers.insert("X-Topic".to_string(), "/header/topic".to_string());

        let topic = tentacle.resolve_topic(endpoint, "{}", &headers);
        assert_eq!(topic, "/header/topic");
    }

    #[test]
    fn test_topic_prefix() {
        let mut config = default_config();
        config.topic_prefix = Some("myapp".to_string());

        let tentacle = WebhooksPollerTentacle::new("test".to_string(), config);
        let endpoint = &tentacle.config.endpoints[0];

        let topic = tentacle.resolve_topic(endpoint, "{}", &HashMap::new());
        assert_eq!(topic, "myapp//webhooks/poller");
    }

    #[test]
    fn test_create_ink() {
        let config = default_config();
        let tentacle = WebhooksPollerTentacle::new("test".to_string(), config);

        let endpoint = &tentacle.config.endpoints[0];
        let body = r#"{"message": "hello", "count": 42}"#;
        let headers = HashMap::new();

        let ink = tentacle.create_ink(endpoint, body, &headers, 200);
        assert_eq!(ink.topic, "/webhooks/poller");
        assert_eq!(ink.source.r#type, "webhooks_poller");
        assert!(ink.body.contains("api.example.com"));

        let metadata = ink.metadata.unwrap();
        assert_eq!(
            metadata.get("endpoint_url").and_then(|v| v.as_str()),
            Some("https://api.example.com/data")
        );
        assert_eq!(
            metadata.get("status_code").and_then(|v| v.as_u64()),
            Some(200)
        );
        assert_eq!(
            metadata.get("message").and_then(|v| v.as_str()),
            Some("hello")
        );
    }

    #[test]
    fn test_multiple_endpoints() {
        let mut config = default_config();
        config.endpoints.push(PollEndpoint {
            url: "https://api2.example.com/status".to_string(),
            method: "GET".to_string(),
            topic: "/webhooks/status".to_string(),
            topic_mode: TopicMode::Fixed,
            topic_field: None,
            topic_header: None,
            headers: HashMap::new(),
            auth: PollerAuth::None,
            poll_interval_secs: Some(30),
            expected_content_type: None,
            body: None,
        });
        assert_eq!(config.endpoints.len(), 2);
    }

    #[test]
    fn test_endpoint_custom_interval() {
        let mut config = default_config();
        config.endpoints[0].poll_interval_secs = Some(15);
        assert_eq!(config.endpoints[0].poll_interval_secs, Some(15));
    }

    #[test]
    fn test_post_method_config() {
        let mut config = default_config();
        config.endpoints[0].method = "POST".to_string();
        config.endpoints[0].body = Some(r#"{"action": "sync"}"#.to_string());
        assert_eq!(config.endpoints[0].method, "POST");
        assert!(config.endpoints[0].body.is_some());
    }
}
