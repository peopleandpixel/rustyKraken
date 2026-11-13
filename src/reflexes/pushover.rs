use anyhow::{Context, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::{debug, info};

use super::Reflex;
use crate::model::Ink;

/// Configuration for Pushover Reflex
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PushoverReflexConfig {
    /// Pushover API token
    pub api_token: String,
    /// Pushover user key
    pub user_key: String,
    /// Template for the message title
    #[serde(default = "default_title_template")]
    pub title_template: String,
    /// Template for the message body
    #[serde(default = "default_message_template")]
    pub message_template: String,
    /// Priority: -2 (quiet) to 2 (emergency)
    #[serde(default = "default_priority")]
    pub priority: i8,
    /// Optional device name
    #[serde(default)]
    pub device: Option<String>,
    /// Optional: mapping from Ink topics to priorities
    #[serde(default)]
    pub topic_priority_mapping: HashMap<String, i8>,
}

fn default_title_template() -> String { "{{topic}}".to_string() }
fn default_message_template() -> String { "{{body}}".to_string() }
fn default_priority() -> i8 { 0 }

pub struct PushoverReflex {
    name: String,
    config: PushoverReflexConfig,
    client: reqwest::Client,
}

impl PushoverReflex {
    pub fn new(name: String, config: PushoverReflexConfig) -> Self {
        Self { name, config, client: reqwest::Client::new() }
    }

    fn resolve_priority(&self, ink: &Ink) -> i8 {
        if let Some(p) = self.config.topic_priority_mapping.get(&ink.topic) {
            return *p;
        }
        self.config.priority
    }

    fn render_template(&self, template: &str, ink: &Ink) -> String {
        let mut rendered = template.to_string();
        rendered = rendered.replace("{{topic}}", &ink.topic);
        rendered = rendered.replace("{{body}}", &ink.body);
        rendered = rendered.replace("{{id}}", &ink.id.to_string());
        rendered = rendered.replace("{{timestamp}}", &ink.timestamp.to_rfc3339());
        if let Some(obj) = ink.metadata.as_ref().and_then(|m| m.as_object()) {
            for (key, value) in obj {
                let placeholder = format!("{{{{metadata.{}}}}}", key);
                let value_str = match value {
                    serde_json::Value::String(s) => s.clone(),
                    _ => value.to_string(),
                };
                rendered = rendered.replace(&placeholder, &value_str);
            }
        }
        rendered
    }
}

#[async_trait]
impl Reflex for PushoverReflex {
    fn name(&self) -> &str { &self.name }
    fn r#type(&self) -> &str { "pushover" }

    async fn handle(&self, ink: &Ink) -> Result<()> {
        info!("Pushover reflex '{}' handling ink on topic: {}", self.name, ink.topic);
        let priority = self.resolve_priority(ink);
        let title = self.render_template(&self.config.title_template, ink);
        let message = self.render_template(&self.config.message_template, ink);
        debug!("Pushover reflex '{}' sending notification (priority {}) for topic: {}", self.name, priority, ink.topic);

        let payload = serde_json::json!({
            "token": self.config.api_token,
            "user": self.config.user_key,
            "title": title,
            "message": message,
            "priority": priority,
        });

        let response = self.client.post("https://api.pushover.net/1/messages.json")
            .json(&payload)
            .send()
            .await
            .context("Failed to send Pushover notification")?;

        if !response.status().is_success() {
            let status = response.status();
            let error_body = response.text().await.unwrap_or_default();
            return Err(anyhow::anyhow!("Pushover API error ({}): {}", status, error_body));
        }
        info!("Pushover reflex '{}' sent notification for topic: {}", self.name, ink.topic);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_config() -> PushoverReflexConfig {
        PushoverReflexConfig {
            api_token: "test_token".to_string(),
            user_key: "test_user".to_string(),
            title_template: "{{topic}}".to_string(),
            message_template: "{{body}}".to_string(),
            priority: 0,
            device: None,
            topic_priority_mapping: HashMap::new(),
        }
    }

    fn create_test_ink() -> Ink {
        Ink::new("/test/topic", "test", "test_source", "test body")
    }

    #[test]
    fn test_default_priority() {
        let config = create_test_config();
        let reflex = PushoverReflex::new("test".to_string(), config);
        let ink = create_test_ink();
        assert_eq!(reflex.resolve_priority(&ink), 0);
    }

    #[test]
    fn test_template_rendering() {
        let config = create_test_config();
        let reflex = PushoverReflex::new("test".to_string(), config);
        let ink = create_test_ink();
        assert_eq!(reflex.render_template("[{{topic}}] {{body}}", &ink), "[/test/topic] test body");
    }

    #[test]
    fn test_config_deserialization() {
        let yaml = r#"
            api_token: "token123"
            user_key: "user456"
            priority: 2
        "#;
        let config: PushoverReflexConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.priority, 2);
    }

    #[test]
    fn test_reflex_type() {
        let config = create_test_config();
        let reflex = PushoverReflex::new("test".to_string(), config);
        assert_eq!(reflex.r#type(), "pushover");
    }
}
