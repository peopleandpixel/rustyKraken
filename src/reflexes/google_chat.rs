use anyhow::{Context, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::{debug, error, info};

use super::Reflex;
use crate::model::Ink;

/// Configuration for Google Chat Reflex
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GoogleChatReflexConfig {
    /// Webhook URL for Google Chat
    pub webhook_url: String,

    /// Template for the message text
    #[serde(default = "default_message_template")]
    pub message_template: String,

    /// Optional: mapping from Ink topics to webhook URLs
    #[serde(default)]
    pub topic_webhook_mapping: HashMap<String, String>,

    /// Whether to use cards with sections
    #[serde(default)]
    pub use_card: bool,

    /// Card header title template
    #[serde(default = "default_card_title")]
    pub card_title_template: String,
}

fn default_message_template() -> String {
    "{{body}}".to_string()
}

fn default_card_title() -> String {
    "{{topic}}".to_string()
}

pub struct GoogleChatReflex {
    name: String,
    config: GoogleChatReflexConfig,
    client: reqwest::Client,
}

impl GoogleChatReflex {
    pub fn new(name: String, config: GoogleChatReflexConfig) -> Self {
        Self {
            name,
            config,
            client: reqwest::Client::new(),
        }
    }

    fn resolve_webhook_url(&self, ink: &Ink) -> String {
        if let Some(url) = self
            .config
            .topic_webhook_mapping
            .get(&ink.topic)
            .filter(|u| !u.is_empty())
        {
            return url.clone();
        }
        self.config.webhook_url.clone()
    }

    fn render_template(&self, template: &str, ink: &Ink) -> String {
        let mut rendered = template.to_string();
        rendered = rendered.replace("{{topic}}", &ink.topic);
        rendered = rendered.replace("{{body}}", &ink.body);
        rendered = rendered.replace("{{id}}", &ink.id.to_string());
        rendered = rendered.replace("{{timestamp}}", &ink.timestamp.to_rfc3339());

        if let Some(obj) = ink.metadata.as_ref().and_then(|m| m.as_object()) {
            for (key, value) in obj {
                let placeholder = format!("{{{{metadata.{}}}", key);
                let value_str = match value {
                    serde_json::Value::String(s) => s.clone(),
                    _ => value.to_string(),
                };
                rendered = rendered.replace(&placeholder, &value_str);
            }
        }
        rendered
    }

    async fn send_message(&self, webhook_url: &str, message: &str) -> Result<()> {
        let payload = if self.config.use_card {
            let card_title =
                self.render_template(&self.config.card_title_template, &Ink::new("", "", "", ""));
            serde_json::json!({
                "cards": [{
                    "header": {
                        "title": card_title
                    },
                    "sections": [{
                        "widgets": [{
                            "textParagraph": {
                                "text": message
                            }
                        }]
                    }]
                }]
            })
        } else {
            serde_json::json!({
                "text": message
            })
        };

        let response = self
            .client
            .post(webhook_url)
            .json(&payload)
            .send()
            .await
            .with_context(|| format!("Failed to send Google Chat message to {}", webhook_url))?;

        if !response.status().is_success() {
            let status = response.status();
            let error_body = response.text().await.unwrap_or_default();
            return Err(anyhow::anyhow!(
                "Google Chat API error ({}): {}",
                status,
                error_body
            ));
        }

        Ok(())
    }
}

#[async_trait]
impl Reflex for GoogleChatReflex {
    fn name(&self) -> &str {
        &self.name
    }

    fn r#type(&self) -> &str {
        "google_chat"
    }

    async fn handle(&self, ink: &Ink) -> Result<()> {
        info!(
            "Google Chat reflex '{}' handling ink on topic: {}",
            self.name, ink.topic
        );

        let webhook_url = self.resolve_webhook_url(ink);
        let message = self.render_template(&self.config.message_template, ink);

        debug!(
            "Google Chat reflex '{}' sending message for topic: {}",
            self.name, ink.topic
        );

        match self.send_message(&webhook_url, &message).await {
            Ok(()) => {
                info!(
                    "Google Chat reflex '{}' sent message for topic: {}",
                    self.name, ink.topic
                );
                Ok(())
            }
            Err(e) => {
                error!(
                    "Google Chat reflex '{}' failed to send message: {}",
                    self.name, e
                );
                Err(e)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_config() -> GoogleChatReflexConfig {
        GoogleChatReflexConfig {
            webhook_url: "https://chat.googleapis.com/v1/webhooks/test".to_string(),
            message_template: "{{body}}".to_string(),
            topic_webhook_mapping: HashMap::new(),
            use_card: false,
            card_title_template: "{{topic}}".to_string(),
        }
    }

    fn create_test_ink() -> Ink {
        Ink::new("/test/topic", "test", "test_source", "test body")
    }

    #[test]
    fn test_default_webhook() {
        let config = create_test_config();
        let reflex = GoogleChatReflex::new("test".to_string(), config);
        let ink = create_test_ink();
        assert_eq!(
            reflex.resolve_webhook_url(&ink),
            "https://chat.googleapis.com/v1/webhooks/test"
        );
    }

    #[test]
    fn test_template_rendering() {
        let config = create_test_config();
        let reflex = GoogleChatReflex::new("test".to_string(), config);
        let ink = create_test_ink();
        assert_eq!(
            reflex.render_template("[{{topic}}] {{body}}", &ink),
            "[/test/topic] test body"
        );
    }

    #[test]
    fn test_config_deserialization() {
        let yaml = r#"
            webhook_url: "https://chat.googleapis.com/v1/webhooks/test"
            message_template: "[{{topic}}] {{body}}"
            use_card: true
        "#;
        let config: GoogleChatReflexConfig = serde_yaml::from_str(yaml).unwrap();
        assert!(config.use_card);
    }

    #[test]
    fn test_reflex_type() {
        let config = create_test_config();
        let reflex = GoogleChatReflex::new("test".to_string(), config);
        assert_eq!(reflex.r#type(), "google_chat");
    }
}
