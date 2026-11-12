use anyhow::{Context, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::{debug, error, info};

use super::Reflex;
use crate::model::Ink;

/// Configuration for Signal Reflex (HTTP API)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignalReflexConfig {
    /// Signal CLI base URL (e.g., "http://localhost:8080/v1")
    pub base_url: String,
    /// Signal CLI API password
    #[serde(default)]
    pub password: Option<String>,
    /// Default recipient number(s)
    pub recipients: Vec<String>,
    /// Template for the message body
    #[serde(default = "default_message_template")]
    pub message_template: String,
    /// Optional: mapping from Ink topics to recipient numbers
    #[serde(default)]
    pub topic_recipient_mapping: HashMap<String, Vec<String>>,
}

fn default_message_template() -> String { "{{body}}".to_string() }

pub struct SignalReflex {
    name: String,
    config: SignalReflexConfig,
    client: reqwest::Client,
}

impl SignalReflex {
    pub fn new(name: String, config: SignalReflexConfig) -> Self {
        Self { name, config, client: reqwest::Client::new() }
    }

    fn resolve_recipients(&self, ink: &Ink) -> Vec<String> {
        if let Some(recipients) = self.config.topic_recipient_mapping.get(&ink.topic).filter(|r| !r.is_empty()) {
            return recipients.clone();
        }
        self.config.recipients.clone()
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
impl Reflex for SignalReflex {
    fn name(&self) -> &str { &self.name }
    fn r#type(&self) -> &str { "signal" }

    async fn handle(&self, ink: &Ink) -> Result<()> {
        info!("Signal reflex '{}' handling ink on topic: {}", self.name, ink.topic);
        let recipients = self.resolve_recipients(ink);
        let message = self.render_template(&self.config.message_template, ink);
        debug!("Signal reflex '{}' sending to {:?} for topic: {}", self.name, recipients, ink.topic);

        for recipient in &recipients {
            let url = format!("{}/message/send/{}", self.config.base_url, recipient);
            let body = serde_json::json!({
                "type": "text",
                "message": message,
            });

            let mut request = self.client.post(&url).json(&body);
            if let Some(ref password) = self.config.password {
                request = request.header("X-Signal-Password", password);
            }

            let response = request.send().await
                .with_context(|| format!("Failed to send Signal message to {}", recipient))?;

            if !response.status().is_success() {
                let status = response.status();
                let error_body = response.text().await.unwrap_or_default();
                return Err(anyhow::anyhow!("Signal API error ({}): {}", status, error_body));
            }
        }

        info!("Signal reflex '{}' sent message to {:?} for topic: {}", self.name, recipients, ink.topic);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_config() -> SignalReflexConfig {
        SignalReflexConfig {
            base_url: "http://localhost:8080/v1".to_string(),
            password: Some("testpass".to_string()),
            recipients: vec!["+1234567890".to_string()],
            message_template: "{{body}}".to_string(),
            topic_recipient_mapping: HashMap::new(),
        }
    }

    fn create_test_ink() -> Ink {
        Ink::new("/test/topic", "test", "test_source", "test body")
    }

    #[test]
    fn test_default_recipients() {
        let config = create_test_config();
        let reflex = SignalReflex::new("test".to_string(), config);
        let ink = create_test_ink();
        let recipients = reflex.resolve_recipients(&ink);
        assert_eq!(recipients, vec!["+1234567890"]);
    }

    #[test]
    fn test_template_rendering() {
        let config = create_test_config();
        let reflex = SignalReflex::new("test".to_string(), config);
        let ink = create_test_ink();
        assert_eq!(reflex.render_template("[{{topic}}] {{body}}", &ink), "[/test/topic] test body");
    }

    #[test]
    fn test_config_deserialization() {
        let yaml = r#"
            base_url: "http://localhost:8080/v1"
            password: "secret"
            recipients: ["+1234567890"]
        "#;
        let config: SignalReflexConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.base_url, "http://localhost:8080/v1");
    }

    #[test]
    fn test_reflex_type() {
        let config = create_test_config();
        let reflex = SignalReflex::new("test".to_string(), config);
        assert_eq!(reflex.r#type(), "signal");
    }
}
