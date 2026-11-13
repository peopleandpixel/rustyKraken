use anyhow::{Context, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::{debug, error, info, warn};

use super::Reflex;
use crate::appcontext::AppContext;
use crate::model::Ink;

/// Message type for Matrix messages
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum MatrixMsgType {
    Text,
    Notice,
    Emote,
}

impl std::fmt::Display for MatrixMsgType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MatrixMsgType::Text => write!(f, "m.text"),
            MatrixMsgType::Notice => write!(f, "m.notice"),
            MatrixMsgType::Emote => write!(f, "m.emote"),
        }
    }
}

/// Format for Matrix message body
#[derive(Debug, Clone, PartialEq)]
pub enum MatrixFormat {
    Html,
    Mvc,
}

impl std::fmt::Display for MatrixFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            MatrixFormat::Html => write!(f, "org.matrix.custom.html"),
            MatrixFormat::Mvc => write!(f, "org.matrix.mxc"),
        }
    }
}

impl serde::Serialize for MatrixFormat {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&self.to_string())
    }
}

impl<'de> serde::Deserialize<'de> for MatrixFormat {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        match s.as_str() {
            "html" | "org.matrix.custom.html" => Ok(MatrixFormat::Html),
            "mxc" | "org.matrix.mxc" => Ok(MatrixFormat::Mvc),
            _ => Err(serde::de::Error::unknown_variant(
                &s,
                &["html", "org.matrix.custom.html", "mxc", "org.matrix.mxc"],
            )),
        }
    }
}

/// Configuration for Matrix/Element Reflex
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MatrixReflexConfig {
    /// Matrix homeserver URL (e.g., "https://matrix.org" or "https://chat.example.com")
    pub server: String,

    /// Access token for authentication (from Element settings or via login API)
    pub access_token: String,

    /// Default room ID to send messages to (e.g., "!ZkngAyfszzfCqwNZUd:example.com")
    pub room_id: String,

    /// Template for the message body
    /// Supported placeholders: {{topic}}, {{body}}, {{id}}, {{timestamp}}, {{metadata.<key>}}
    #[serde(default = "default_body_template")]
    pub body_template: String,

    /// Message type: text, notice, or emote
    #[serde(default = "default_msg_type")]
    pub msg_type: MatrixMsgType,

    /// Optional: HTML formatted body (requires format field)
    /// Supported placeholders: {{topic}}, {{body}}, {{id}}, {{timestamp}}, {{metadata.<key>}}
    #[serde(default)]
    pub formatted_body_template: Option<String>,

    /// Format of the formatted_body (e.g., "org.matrix.custom.html")
    #[serde(default)]
    pub format: Option<MatrixFormat>,

    /// Optional: custom username to display (for bots without accounts)
    #[serde(default)]
    pub username: Option<String>,

    /// Optional: mapping from Ink topics to Matrix room IDs
    #[serde(default)]
    pub topic_room_mapping: HashMap<String, String>,

    /// Optional: mapping from Ink topics to message types
    #[serde(default)]
    pub topic_msg_type_mapping: HashMap<String, MatrixMsgType>,

    /// Optional: mapping from Ink topics to body templates
    #[serde(default)]
    pub topic_body_mapping: HashMap<String, String>,

    /// Request timeout in seconds
    #[serde(default = "default_timeout_secs")]
    pub timeout_secs: u64,

    /// Number of retry attempts on failure
    #[serde(default = "default_retries")]
    pub retries: u32,

    /// Delay between retries in milliseconds
    #[serde(default = "default_retry_delay_ms")]
    pub retry_delay_ms: u64,
}

fn default_body_template() -> String {
    "{{body}}".to_string()
}

fn default_msg_type() -> MatrixMsgType {
    MatrixMsgType::Notice
}

fn default_timeout_secs() -> u64 {
    30
}

fn default_retries() -> u32 {
    3
}

fn default_retry_delay_ms() -> u64 {
    1000
}

pub struct MatrixReflex {
    name: String,
    config: MatrixReflexConfig,
    client: reqwest::Client,
}

impl MatrixReflex {
    pub fn new(name: String, config: MatrixReflexConfig, app_ctx: &AppContext) -> Result<Self> {
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(config.timeout_secs))
            .build()
            .unwrap_or_else(|_| app_ctx.http_client().as_ref().clone());

        Ok(Self {
            name,
            config,
            client,
        })
    }

    /// Resolve room ID for a given Ink, checking topic-based mapping first
    fn resolve_room_id(&self, ink: &Ink) -> String {
        if let Some(room_id) = self
            .config
            .topic_room_mapping
            .get(&ink.topic)
            .filter(|r| !r.is_empty())
        {
            return room_id.clone();
        }

        self.config.room_id.clone()
    }

    /// Resolve message type for a given Ink, checking topic-based mapping first
    fn resolve_msg_type(&self, ink: &Ink) -> MatrixMsgType {
        if let Some(msg_type) = self.config.topic_msg_type_mapping.get(&ink.topic) {
            return msg_type.clone();
        }
        self.config.msg_type.clone()
    }

    /// Resolve body template for a given Ink, checking topic-based mapping first
    fn resolve_body_template(&self, ink: &Ink) -> String {
        if let Some(body_template) = self
            .config
            .topic_body_mapping
            .get(&ink.topic)
            .filter(|b| !b.is_empty())
        {
            return self.render_template(body_template, ink);
        }
        self.render_template(&self.config.body_template, ink)
    }

    /// Render a template string with Ink data
    fn render_template(&self, template: &str, ink: &Ink) -> String {
        let mut result = template.to_string();
        result = result.replace("{{topic}}", &ink.topic);
        result = result.replace("{{body}}", &ink.body);
        result = result.replace("{{id}}", &ink.id.to_string());
        result = result.replace("{{timestamp}}", &ink.timestamp.to_rfc3339());

        if let Some(obj) = ink.metadata.as_ref().and_then(|m| m.as_object()) {
            for (key, value) in obj {
                let placeholder = format!("{{{{metadata.{}}}}}", key);
                let value_str = match value {
                    serde_json::Value::String(s) => s.clone(),
                    _ => value.to_string(),
                };
                result = result.replace(&placeholder, &value_str);
            }
        }

        result
    }

    /// Build the Matrix message payload
    fn build_payload(&self, ink: &Ink) -> serde_json::Value {
        let body = self.resolve_body_template(ink);
        let msg_type = self.resolve_msg_type(ink);

        let mut payload = serde_json::json!({
            "msgtype": msg_type.to_string(),
            "body": body
        });

        // Add formatted body if configured
        if let Some(ref formatted_template) = self.config.formatted_body_template {
            let formatted_body = self.render_template(formatted_template, ink);
            payload["formatted_body"] = serde_json::json!(formatted_body);

            if let Some(ref format) = self.config.format {
                payload["format"] = serde_json::json!(format.to_string());
            }
        }

        payload
    }

    /// Send a message to a Matrix room
    async fn send_message(&self, room_id: &str, payload: &serde_json::Value) -> Result<()> {
        let txn_id = format!(
            "ts{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis()
        );

        let url = format!(
            "{}/_matrix/client/v3/rooms/{}/send/m.room.message/{}",
            self.config.server.trim_end_matches('/'),
            room_id,
            txn_id
        );

        debug!(
            "Matrix reflex '{}' sending message to room {} via {}",
            self.name, room_id, url
        );

        let mut request_builder = self
            .client
            .post(&url)
            .bearer_auth(&self.config.access_token);

        // Set custom username if configured
        if let Some(ref username) = self.config.username {
            request_builder = request_builder.header("X-Custom-Username", username);
        }

        let response = request_builder
            .json(payload)
            .send()
            .await
            .with_context(|| format!("Failed to send Matrix message to {}", room_id))?;

        let status = response.status();
        if status.is_success() {
            info!(
                "Matrix reflex '{}' successfully sent message to room {}",
                self.name, room_id
            );
            Ok(())
        } else {
            let error_body = response.text().await.unwrap_or_default();
            anyhow::bail!("Matrix API returned error {}: {}", status, error_body)
        }
    }

    /// Execute the reflex with retry logic
    async fn execute_with_retry(&self, ink: &Ink) -> Result<()> {
        let room_id = self.resolve_room_id(ink);
        let payload = self.build_payload(ink);

        let mut last_error = None;

        for attempt in 0..=self.config.retries {
            if attempt > 0 {
                warn!(
                    "Matrix reflex '{}' retrying send (attempt {}/{}) after {}ms delay",
                    self.name, attempt, self.config.retries, self.config.retry_delay_ms
                );
                tokio::time::sleep(std::time::Duration::from_millis(self.config.retry_delay_ms))
                    .await;
            }

            match self.send_message(&room_id, &payload).await {
                Ok(()) => return Ok(()),
                Err(e) => {
                    error!(
                        "Matrix reflex '{}' failed to send message (attempt {}/{}): {}",
                        self.name,
                        attempt + 1,
                        self.config.retries + 1,
                        e
                    );
                    last_error = Some(e);
                }
            }
        }

        Err(last_error.unwrap_or_else(|| anyhow::anyhow!("Matrix send failed")))
    }
}

#[async_trait]
impl Reflex for MatrixReflex {
    fn name(&self) -> &str {
        &self.name
    }

    fn r#type(&self) -> &str {
        "matrix"
    }

    async fn handle(&self, ink: &Ink) -> Result<()> {
        info!(
            "Matrix reflex '{}' handling ink on topic: {}",
            self.name, ink.topic
        );

        match self.execute_with_retry(ink).await {
            Ok(()) => {
                info!(
                    "Matrix reflex '{}' successfully handled ink for topic: {}",
                    self.name, ink.topic
                );
                Ok(())
            }
            Err(e) => {
                error!("Matrix reflex '{}' failed to handle ink: {}", self.name, e);
                Err(e)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::appcontext::AppContext;
    use serde_json::json;

    fn create_test_config() -> MatrixReflexConfig {
        MatrixReflexConfig {
            server: "https://matrix.org".to_string(),
            access_token: "test_token".to_string(),
            room_id: "!test:matrix.org".to_string(),
            body_template: "{{body}}".to_string(),
            msg_type: MatrixMsgType::Notice,
            formatted_body_template: None,
            format: None,
            username: None,
            topic_room_mapping: HashMap::new(),
            topic_msg_type_mapping: HashMap::new(),
            topic_body_mapping: HashMap::new(),
            timeout_secs: 30,
            retries: 3,
            retry_delay_ms: 1000,
        }
    }

    fn create_test_ink() -> Ink {
        let mut ink = Ink::new("/test/topic", "test", "test_source", "Hello, Matrix!");
        ink.metadata = Some(json!({"key1": "value1", "key2": 42}));
        ink
    }

    #[test]
    fn test_config_deserialization() {
        let yaml = r#"
            server: "https://chat.example.com"
            access_token: "my_secret_token"
            room_id: "!abc123:example.com"
            body_template: "[{{topic}}] {{body}}"
            msg_type: "text"
            formatted_body_template: "<b>{{topic}}</b>: {{body}}"
            format: "html"
            timeout_secs: 60
            retries: 5
            retry_delay_ms: 2000
            topic_room_mapping:
              "/alerts/critical": "!critical:example.com"
            topic_msg_type_mapping:
              "/alerts/critical": "notice"
            topic_body_mapping:
              "/alerts/critical": "🚨 {{body}}"
        "#;

        let config: MatrixReflexConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.server, "https://chat.example.com");
        assert_eq!(config.access_token, "my_secret_token");
        assert_eq!(config.room_id, "!abc123:example.com");
        assert_eq!(config.body_template, "[{{topic}}] {{body}}");
        assert_eq!(config.msg_type, MatrixMsgType::Text);
        assert_eq!(
            config.formatted_body_template,
            Some("<b>{{topic}}</b>: {{body}}".to_string())
        );
        assert_eq!(config.format, Some(MatrixFormat::Html));
        assert_eq!(config.timeout_secs, 60);
        assert_eq!(config.retries, 5);
        assert_eq!(config.retry_delay_ms, 2000);
        assert_eq!(
            config.topic_room_mapping.get("/alerts/critical"),
            Some(&"!critical:example.com".to_string())
        );
        assert_eq!(
            config.topic_msg_type_mapping.get("/alerts/critical"),
            Some(&MatrixMsgType::Notice)
        );
    }

    #[test]
    fn test_resolve_room_id_from_topic_mapping() {
        let mut config = create_test_config();
        config.topic_room_mapping.insert(
            "/alerts/critical".to_string(),
            "!critical:matrix.org".to_string(),
        );

        let app_ctx = AppContext::new();
        let reflex = MatrixReflex::new("test".to_string(), config, &app_ctx).unwrap();

        let ink = Ink::new("/alerts/critical", "test", "test", "Critical alert");

        assert_eq!(reflex.resolve_room_id(&ink), "!critical:matrix.org");
    }

    #[test]
    fn test_resolve_room_id_default() {
        let config = create_test_config();
        let app_ctx = AppContext::new();
        let reflex = MatrixReflex::new("test".to_string(), config, &app_ctx).unwrap();

        let ink = create_test_ink();
        assert_eq!(reflex.resolve_room_id(&ink), "!test:matrix.org");
    }

    #[test]
    fn test_resolve_msg_type_from_topic_mapping() {
        let mut config = create_test_config();
        config
            .topic_msg_type_mapping
            .insert("/alerts/critical".to_string(), MatrixMsgType::Text);

        let app_ctx = AppContext::new();
        let reflex = MatrixReflex::new("test".to_string(), config, &app_ctx).unwrap();

        let ink = Ink::new("/alerts/critical", "test", "test", "Critical");

        assert_eq!(reflex.resolve_msg_type(&ink), MatrixMsgType::Text);
    }

    #[test]
    fn test_resolve_msg_type_default() {
        let config = create_test_config();
        let app_ctx = AppContext::new();
        let reflex = MatrixReflex::new("test".to_string(), config, &app_ctx).unwrap();

        let ink = create_test_ink();
        assert_eq!(reflex.resolve_msg_type(&ink), MatrixMsgType::Notice);
    }

    #[test]
    fn test_render_template() {
        let config = create_test_config();
        let app_ctx = AppContext::new();
        let reflex = MatrixReflex::new("test".to_string(), config, &app_ctx).unwrap();

        let ink = create_test_ink();
        let result = reflex.render_template("[{{topic}}] {{body}} - {{id}}", &ink);

        assert!(result.contains("[/test/topic]"));
        assert!(result.contains("Hello, Matrix!"));
        // ID is a UUID, so check it contains the ink's ID
        assert!(result.contains(&ink.id.to_string()));
    }

    #[test]
    fn test_render_template_with_metadata() {
        let config = create_test_config();
        let app_ctx = AppContext::new();
        let reflex = MatrixReflex::new("test".to_string(), config, &app_ctx).unwrap();

        let ink = create_test_ink();
        let result = reflex.render_template("{{metadata.key1}}: {{metadata.key2}}", &ink);

        assert!(result.contains("value1"));
        assert!(result.contains("42"));
    }

    #[test]
    fn test_build_payload_basic() {
        let config = create_test_config();
        let app_ctx = AppContext::new();
        let reflex = MatrixReflex::new("test".to_string(), config, &app_ctx).unwrap();

        let ink = create_test_ink();
        let payload = reflex.build_payload(&ink);

        assert_eq!(payload["msgtype"], "m.notice");
        assert_eq!(payload["body"], "Hello, Matrix!");
    }

    #[test]
    fn test_build_payload_with_formatted_body() {
        let mut config = create_test_config();
        config.formatted_body_template = Some("<b>{{body}}</b>".to_string());
        config.format = Some(MatrixFormat::Html);

        let app_ctx = AppContext::new();
        let reflex = MatrixReflex::new("test".to_string(), config, &app_ctx).unwrap();

        let ink = create_test_ink();
        let payload = reflex.build_payload(&ink);

        assert_eq!(payload["msgtype"], "m.notice");
        assert_eq!(payload["body"], "Hello, Matrix!");
        assert_eq!(payload["formatted_body"], "<b>Hello, Matrix!</b>");
        assert_eq!(payload["format"], "org.matrix.custom.html");
    }

    #[test]
    fn test_build_payload_with_topic_body_mapping() {
        let mut config = create_test_config();
        config
            .topic_body_mapping
            .insert("/test/topic".to_string(), "Custom: {{body}}".to_string());

        let app_ctx = AppContext::new();
        let reflex = MatrixReflex::new("test".to_string(), config, &app_ctx).unwrap();

        let ink = create_test_ink();
        let payload = reflex.build_payload(&ink);

        assert_eq!(payload["body"], "Custom: Hello, Matrix!");
    }

    #[test]
    fn test_msg_type_display() {
        assert_eq!(MatrixMsgType::Text.to_string(), "m.text");
        assert_eq!(MatrixMsgType::Notice.to_string(), "m.notice");
        assert_eq!(MatrixMsgType::Emote.to_string(), "m.emote");
    }

    #[test]
    fn test_format_display() {
        assert_eq!(MatrixFormat::Html.to_string(), "org.matrix.custom.html");
        assert_eq!(MatrixFormat::Mvc.to_string(), "org.matrix.mxc");
    }

    #[test]
    fn test_new_reflex() {
        let config = create_test_config();
        let app_ctx = AppContext::new();
        let reflex = MatrixReflex::new("test_reflex".to_string(), config, &app_ctx).unwrap();
        assert_eq!(reflex.name(), "test_reflex");
    }

    #[test]
    fn test_default_config_values() {
        let yaml = r#"
            server: "https://matrix.org"
            access_token: "token"
            room_id: "!room:server.com"
        "#;

        let config: MatrixReflexConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.body_template, "{{body}}");
        assert_eq!(config.msg_type, MatrixMsgType::Notice);
        assert_eq!(config.timeout_secs, 30);
        assert_eq!(config.retries, 3);
        assert_eq!(config.retry_delay_ms, 1000);
        assert!(config.formatted_body_template.is_none());
        assert!(config.format.is_none());
    }
}
