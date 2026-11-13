use anyhow::{Context, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, error, info, warn};

use super::Reflex;
use crate::appcontext::AppContext;
use crate::model::Ink;

/// Configuration for Telegram Bot API Reflex
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TelegramReflexConfig {
    /// Telegram Bot Token from @BotFather
    pub bot_token: String,

    /// Default chat ID(s) to send messages to
    pub chat_ids: Vec<String>,

    /// Parse mode for messages: "Markdown", "HTML", or "None"
    #[serde(default = "default_parse_mode")]
    pub parse_mode: String,

    /// Template for the message body with placeholders
    /// Supported placeholders: {{topic}}, {{body}}, {{id}}, {{timestamp}}, {{metadata.<key>}}
    #[serde(default = "default_body_template")]
    pub body_template: String,

    /// Optional: mapping from topics to chat IDs
    #[serde(default)]
    pub topic_chat_mapping: HashMap<String, Vec<String>>,

    /// Optional: mapping from topics to parse modes
    #[serde(default)]
    pub topic_parse_mode_mapping: HashMap<String, String>,

    /// Optional: custom API base URL (for proxies)
    #[serde(default)]
    pub api_base_url: Option<String>,

    /// Whether to include Ink metadata in the message
    #[serde(default)]
    pub include_metadata: bool,

    /// Maximum message length (Telegram limit is 4096 characters)
    #[serde(default = "default_max_length")]
    pub max_length: usize,
}

fn default_parse_mode() -> String {
    "Markdown".to_string()
}

fn default_body_template() -> String {
    "{{body}}".to_string()
}

fn default_max_length() -> usize {
    4096
}

pub struct TelegramReflex {
    name: String,
    config: TelegramReflexConfig,
    client: Arc<reqwest::Client>,
}

impl TelegramReflex {
    pub fn new(name: String, config: TelegramReflexConfig, app_ctx: &AppContext) -> Self {
        Self {
            name,
            config,
            client: Arc::clone(app_ctx.http_client()),
        }
    }

    /// Resolve chat IDs for a given Ink, checking topic-based mapping first
    fn resolve_chat_ids(&self, ink: &Ink) -> Vec<String> {
        // Check topic-based chat mapping
        if let Some(chat_ids) = self
            .config
            .topic_chat_mapping
            .get(&ink.topic)
            .filter(|c| !c.is_empty())
        {
            return chat_ids.clone();
        }

        // Fall back to default chat IDs
        self.config.chat_ids.clone()
    }

    /// Resolve parse mode for a given Ink, checking topic-based mapping first
    fn resolve_parse_mode(&self, ink: &Ink) -> String {
        // Check topic-based parse mode mapping
        if let Some(parse_mode) = self
            .config
            .topic_parse_mode_mapping
            .get(&ink.topic)
            .filter(|p| !p.is_empty())
        {
            return parse_mode.clone();
        }

        // Fall back to default parse mode
        self.config.parse_mode.clone()
    }

    /// Render the message body from the template and Ink data
    fn render_body(&self, ink: &Ink) -> String {
        let mut body = self.config.body_template.clone();

        // Replace {{topic}}
        body = body.replace("{{topic}}", &ink.topic);

        // Replace {{body}}
        body = body.replace("{{body}}", &ink.body);

        // Replace {{id}}
        body = body.replace("{{id}}", &ink.id.to_string());

        // Replace {{timestamp}}
        body = body.replace("{{timestamp}}", &ink.timestamp.to_rfc3339());

        // Replace {{metadata.<key>}} placeholders
        if let Some(obj) = ink.metadata.as_ref().and_then(|m| m.as_object()) {
            for (key, value) in obj {
                let placeholder = format!("{{{{metadata.{}}}}}", key);
                let value_str = match value {
                    serde_json::Value::String(s) => s.clone(),
                    _ => value.to_string(),
                };
                body = body.replace(&placeholder, &value_str);
            }
        }

        body
    }

    /// Build the full message text, optionally including metadata
    fn build_message(&self, ink: &Ink) -> String {
        let rendered = self.render_body(ink);

        let mut message = if self.config.include_metadata {
            let metadata_json = if let Some(ref metadata) = ink.metadata {
                serde_json::to_string_pretty(metadata).unwrap_or_else(|_| "{}".to_string())
            } else {
                "{}".to_string()
            };
            format!("{}\n\n```\n{}\n```", rendered, metadata_json)
        } else {
            rendered
        };

        // Truncate if exceeds max length
        if message.len() > self.config.max_length {
            let truncated = &message[..self.config.max_length];
            // Try to truncate at a word boundary
            if let Some(pos) = truncated.rfind(' ') {
                message = truncated[..pos].to_string();
            } else {
                message = truncated.to_string();
            }
            message.push_str("\n\n*(message truncated)*");
        }

        message
    }

    /// Send a message via Telegram Bot API
    async fn send_message(&self, chat_id: &str, text: &str, parse_mode: &str) -> Result<()> {
        let base_url = self
            .config
            .api_base_url
            .as_deref()
            .unwrap_or("https://api.telegram.org");
        let url = format!("{}/bot{}/sendMessage", base_url, self.config.bot_token);

        let mut payload = serde_json::Map::new();
        payload.insert(
            "chat_id".to_string(),
            serde_json::Value::String(chat_id.to_string()),
        );
        payload.insert(
            "text".to_string(),
            serde_json::Value::String(text.to_string()),
        );

        // Only add parse_mode if not "None"
        if parse_mode != "None" && !parse_mode.is_empty() {
            payload.insert(
                "parse_mode".to_string(),
                serde_json::Value::String(parse_mode.to_string()),
            );
        }

        let response = self
            .client
            .post(&url)
            .header("Content-Type", "application/json")
            .json(&payload)
            .send()
            .await
            .with_context(|| format!("Failed to send Telegram message to {}", chat_id))?;

        if !response.status().is_success() {
            let status = response.status();
            let error_body = response.text().await.unwrap_or_default();
            return Err(anyhow::anyhow!(
                "Telegram API error ({}): {}",
                status,
                error_body
            ));
        }

        Ok(())
    }
}

#[async_trait]
impl Reflex for TelegramReflex {
    fn name(&self) -> &str {
        &self.name
    }

    fn r#type(&self) -> &str {
        "telegram"
    }

    async fn handle(&self, ink: &Ink) -> Result<()> {
        info!(
            "Telegram reflex '{}' handling ink on topic: {}",
            self.name, ink.topic
        );

        let chat_ids = self.resolve_chat_ids(ink);
        if chat_ids.is_empty() {
            warn!(
                "Telegram reflex '{}' has no chat IDs for topic: {}",
                self.name, ink.topic
            );
            return Ok(());
        }

        let parse_mode = self.resolve_parse_mode(ink);
        let message = self.build_message(ink);

        debug!(
            "Telegram reflex '{}' sending message to {:?} with parse_mode '{}' for topic: {}",
            self.name, chat_ids, parse_mode, ink.topic
        );

        let mut success_count = 0;
        let mut error_count = 0;

        for chat_id in &chat_ids {
            match self.send_message(chat_id, &message, &parse_mode).await {
                Ok(()) => {
                    info!(
                        "Telegram reflex '{}' sent message to {} for topic: {}",
                        self.name, chat_id, ink.topic
                    );
                    success_count += 1;
                }
                Err(e) => {
                    error!(
                        "Telegram reflex '{}' failed to send to {}: {}",
                        self.name, chat_id, e
                    );
                    error_count += 1;
                }
            }
        }

        info!(
            "Telegram reflex '{}' completed: {} succeeded, {} failed",
            self.name, success_count, error_count
        );

        if error_count > 0 && success_count == 0 {
            Err(anyhow::anyhow!(
                "Telegram reflex '{}' failed to send to all chat IDs",
                self.name
            ))
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_config() -> TelegramReflexConfig {
        TelegramReflexConfig {
            bot_token: "123456:ABC-DEF".to_string(),
            chat_ids: vec!["-1001234567890".to_string()],
            parse_mode: "Markdown".to_string(),
            body_template: "{{body}}".to_string(),
            topic_chat_mapping: HashMap::new(),
            topic_parse_mode_mapping: HashMap::new(),
            api_base_url: None,
            include_metadata: false,
            max_length: 4096,
        }
    }

    fn create_test_ink() -> Ink {
        Ink::new("/test/topic", "test", "test_source", "test body")
    }

    #[test]
    fn test_default_chat_ids() {
        let config = create_test_config();
        let reflex = TelegramReflex::new(
            "test".to_string(),
            config,
            &crate::appcontext::AppContext::new(),
        );

        let ink = create_test_ink();
        let chat_ids = reflex.resolve_chat_ids(&ink);
        assert_eq!(chat_ids, vec!["-1001234567890"]);
    }

    #[test]
    fn test_topic_chat_mapping() {
        let mut config = create_test_config();
        let mut mapping = HashMap::new();
        mapping.insert(
            "/test/topic".to_string(),
            vec!["-1009876543210".to_string(), "-1001111111111".to_string()],
        );
        config.topic_chat_mapping = mapping;

        let reflex = TelegramReflex::new(
            "test".to_string(),
            config,
            &crate::appcontext::AppContext::new(),
        );

        let ink = create_test_ink();
        let chat_ids = reflex.resolve_chat_ids(&ink);
        assert_eq!(chat_ids.len(), 2);
        assert!(chat_ids.contains(&"-1009876543210".to_string()));
        assert!(chat_ids.contains(&"-1001111111111".to_string()));
    }

    #[test]
    fn test_default_parse_mode() {
        let config = create_test_config();
        let reflex = TelegramReflex::new(
            "test".to_string(),
            config,
            &crate::appcontext::AppContext::new(),
        );

        let ink = create_test_ink();
        assert_eq!(reflex.resolve_parse_mode(&ink), "Markdown");
    }

    #[test]
    fn test_topic_parse_mode_mapping() {
        let mut config = create_test_config();
        let mut mapping = HashMap::new();
        mapping.insert("/test/topic".to_string(), "HTML".to_string());
        config.topic_parse_mode_mapping = mapping;

        let reflex = TelegramReflex::new(
            "test".to_string(),
            config,
            &crate::appcontext::AppContext::new(),
        );

        let ink = create_test_ink();
        assert_eq!(reflex.resolve_parse_mode(&ink), "HTML");
    }

    #[test]
    fn test_body_template_rendering() {
        let mut config = create_test_config();
        config.body_template = "Topic: {{topic}}, Body: {{body}}".to_string();

        let reflex = TelegramReflex::new(
            "test".to_string(),
            config,
            &crate::appcontext::AppContext::new(),
        );

        let ink = create_test_ink();
        assert_eq!(
            reflex.render_body(&ink),
            "Topic: /test/topic, Body: test body"
        );
    }

    #[test]
    fn test_metadata_in_template() {
        let mut config = create_test_config();
        config.body_template = "Value: {{metadata.temp}}".to_string();

        let reflex = TelegramReflex::new(
            "test".to_string(),
            config,
            &crate::appcontext::AppContext::new(),
        );

        let ink = Ink::new("/test", "test", "test_source", "data")
            .with_metadata(serde_json::json!({ "temp": "42", "humidity": "60" }));
        assert_eq!(reflex.render_body(&ink), "Value: 42");
    }

    #[test]
    fn test_message_truncation() {
        let mut config = create_test_config();
        config.max_length = 50;
        config.body_template = "{{body}}".to_string();

        let reflex = TelegramReflex::new(
            "test".to_string(),
            config,
            &crate::appcontext::AppContext::new(),
        );

        let ink = Ink::new(
            "/test",
            "test",
            "test_source",
            "This is a very long message that exceeds the maximum length limit",
        );
        let message = reflex.build_message(&ink);
        assert!(message.len() <= 70); // max_length + truncation note
        assert!(message.contains("truncated"));
    }

    #[test]
    fn test_include_metadata() {
        let mut config = create_test_config();
        config.include_metadata = true;
        config.body_template = "{{body}}".to_string();

        let reflex = TelegramReflex::new(
            "test".to_string(),
            config,
            &crate::appcontext::AppContext::new(),
        );

        let ink = Ink::new("/test", "test", "test_source", "data")
            .with_metadata(serde_json::json!({ "key": "value" }));
        let message = reflex.build_message(&ink);
        assert!(message.contains("data"));
        assert!(message.contains("key"));
        assert!(message.contains("value"));
    }

    #[test]
    fn test_config_deserialization() {
        let yaml = r#"
            bot_token: "123456:ABC-DEF"
            chat_ids: ["-1001234567890", "-1009876543210"]
            parse_mode: "HTML"
            body_template: "Alert: {{topic}} - {{body}}"
            topic_chat_mapping:
              "/alerts": ["-1001111111111"]
            topic_parse_mode_mapping:
              "/html_topic": "HTML"
            include_metadata: true
            max_length: 2048
        "#;

        let config: TelegramReflexConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.bot_token, "123456:ABC-DEF");
        assert_eq!(config.chat_ids.len(), 2);
        assert_eq!(config.parse_mode, "HTML");
        assert_eq!(config.body_template, "Alert: {{topic}} - {{body}}");
        assert!(config.include_metadata);
        assert_eq!(config.max_length, 2048);
    }

    #[test]
    fn test_timestamp_and_id_in_template() {
        let mut config = create_test_config();
        config.body_template = "ID: {{id}}, Time: {{timestamp}}".to_string();

        let reflex = TelegramReflex::new(
            "test".to_string(),
            config,
            &crate::appcontext::AppContext::new(),
        );

        let ink = create_test_ink();
        let body = reflex.render_body(&ink);
        assert!(body.starts_with("ID: "));
        assert!(body.contains(", Time: "));
    }
}
