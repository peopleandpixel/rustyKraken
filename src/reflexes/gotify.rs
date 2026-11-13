use anyhow::{Context, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, error, info};

use super::Reflex;
use crate::appcontext::AppContext;
use crate::model::Ink;

/// Configuration for Gotify Reflex
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GotifyReflexConfig {
    /// Gotify server URL (e.g., "https://gotify.example.com")
    pub server_url: String,

    /// Gotify application token
    pub app_token: String,

    /// Template for the notification title
    #[serde(default = "default_title_template")]
    pub title_template: String,

    /// Template for the notification message
    #[serde(default = "default_message_template")]
    pub message_template: String,

    /// Priority (0-8)
    #[serde(default = "default_priority")]
    pub priority: u8,

    /// Optional: mapping from Ink topics to priorities
    #[serde(default)]
    pub topic_priority_mapping: HashMap<String, u8>,

    /// Optional extra fields
    #[serde(default)]
    pub extras: serde_json::Value,
}

fn default_title_template() -> String {
    "{{topic}}".to_string()
}

fn default_message_template() -> String {
    "{{body}}".to_string()
}

fn default_priority() -> u8 {
    3
}

pub struct GotifyReflex {
    name: String,
    config: GotifyReflexConfig,
    client: Arc<reqwest::Client>,
}

impl GotifyReflex {
    pub fn new(name: String, config: GotifyReflexConfig, app_ctx: &AppContext) -> Self {
        Self {
            name,
            config,
            client: Arc::clone(app_ctx.http_client()),
        }
    }

    fn resolve_priority(&self, ink: &Ink) -> u8 {
        if let Some(priority) = self.config.topic_priority_mapping.get(&ink.topic) {
            return *priority;
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

    async fn send_notification(&self, title: &str, message: &str, priority: u8) -> Result<()> {
        let url = format!(
            "{}/message?token={}",
            self.config.server_url.trim_end_matches('/'),
            self.config.app_token
        );

        let payload = serde_json::json!({
            "title": title,
            "message": message,
            "priority": priority,
            "extras": self.config.extras.clone()
        });

        let response = self
            .client
            .post(&url)
            .json(&payload)
            .send()
            .await
            .with_context(|| {
                format!(
                    "Failed to send Gotify notification to {}",
                    self.config.server_url
                )
            })?;

        if !response.status().is_success() {
            let status = response.status();
            let error_body = response.text().await.unwrap_or_default();
            return Err(anyhow::anyhow!(
                "Gotify API error ({}): {}",
                status,
                error_body
            ));
        }
        Ok(())
    }
}

#[async_trait]
impl Reflex for GotifyReflex {
    fn name(&self) -> &str {
        &self.name
    }

    fn r#type(&self) -> &str {
        "gotify"
    }

    async fn handle(&self, ink: &Ink) -> Result<()> {
        info!(
            "Gotify reflex '{}' handling ink on topic: {}",
            self.name, ink.topic
        );
        let priority = self.resolve_priority(ink);
        let title = self.render_template(&self.config.title_template, ink);
        let message = self.render_template(&self.config.message_template, ink);
        debug!(
            "Gotify reflex '{}' sending notification (priority {}) for topic: {}",
            self.name, priority, ink.topic
        );

        match self.send_notification(&title, &message, priority).await {
            Ok(()) => {
                info!(
                    "Gotify reflex '{}' sent notification for topic: {}",
                    self.name, ink.topic
                );
                Ok(())
            }
            Err(e) => {
                error!("Gotify reflex '{}' failed: {}", self.name, e);
                Err(e)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::appcontext::AppContext;

    fn create_test_config() -> GotifyReflexConfig {
        GotifyReflexConfig {
            server_url: "https://gotify.example.com".to_string(),
            app_token: "test_token".to_string(),
            title_template: "{{topic}}".to_string(),
            message_template: "{{body}}".to_string(),
            priority: 3,
            topic_priority_mapping: HashMap::new(),
            extras: serde_json::Value::Null,
        }
    }

    fn create_test_ink() -> Ink {
        Ink::new("/test/topic", "test", "test_source", "test body")
    }

    #[test]
    fn test_default_priority() {
        let config = create_test_config();
        let reflex = GotifyReflex::new("test".to_string(), config, &AppContext::new());
        let ink = create_test_ink();
        assert_eq!(reflex.resolve_priority(&ink), 3);
    }

    #[test]
    fn test_topic_priority_mapping() {
        let mut config = create_test_config();
        let mut mapping = HashMap::new();
        mapping.insert("/test/topic".to_string(), 5);
        config.topic_priority_mapping = mapping;
        let reflex = GotifyReflex::new("test".to_string(), config, &AppContext::new());
        let ink = create_test_ink();
        assert_eq!(reflex.resolve_priority(&ink), 5);
    }

    #[test]
    fn test_template_rendering() {
        let config = create_test_config();
        let reflex = GotifyReflex::new("test".to_string(), config, &AppContext::new());
        let ink = create_test_ink();
        assert_eq!(
            reflex.render_template("[{{topic}}] {{body}}", &ink),
            "[/test/topic] test body"
        );
    }

    #[test]
    fn test_config_deserialization() {
        let yaml = r#"
            server_url: "https://gotify.example.com"
            app_token: "token123"
            priority: 5
        "#;
        let config: GotifyReflexConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.priority, 5);
    }

    #[test]
    fn test_reflex_type() {
        let config = create_test_config();
        let reflex = GotifyReflex::new("test".to_string(), config, &AppContext::new());
        assert_eq!(reflex.r#type(), "gotify");
    }
}
