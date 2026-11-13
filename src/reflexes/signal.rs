use anyhow::{Context, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::{debug, info};

use super::Reflex;
use crate::appcontext::AppContext;
use crate::model::Ink;

/// Configuration for Signal Reflex
/// Sends messages via Signal CLI HTTP API
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SignalReflexConfig {
    /// Signal CLI HTTP API URL (e.g., "http://localhost:8080/v2")
    pub api_url: String,

    /// API password for authentication
    pub password: String,

    /// Default recipient (e.g., "+49123456789" or "group-name")
    #[serde(default)]
    pub recipient: Option<String>,

    /// Template for the message body
    #[serde(default = "default_message_template")]
    pub message_template: String,

    /// Optional: mapping from Ink topics to recipients
    #[serde(default)]
    pub topic_recipient_mapping: HashMap<String, String>,

    /// Whether to use HTTPS
    #[serde(default = "default_use_https")]
    pub use_https: bool,

    /// Request timeout in seconds
    #[serde(default = "default_timeout")]
    pub timeout: u64,
}

fn default_message_template() -> String {
    "{{body}}".to_string()
}
fn default_use_https() -> bool {
    false
}
fn default_timeout() -> u64 {
    30
}

pub struct SignalReflex {
    name: String,
    config: SignalReflexConfig,
    client: reqwest::Client,
}

impl SignalReflex {
    pub fn new(name: String, config: SignalReflexConfig, app_ctx: &AppContext) -> Self {
        let client = if config.use_https {
            app_ctx.http_client().as_ref().clone()
        } else {
            app_ctx.http_client_danger_accept_invalid_certs()
        };
        Self {
            name,
            config,
            client,
        }
    }

    /// Resolve recipient for a given Ink
    fn resolve_recipient(&self, ink: &Ink) -> Result<String> {
        if let Some(recipient) = self
            .config
            .topic_recipient_mapping
            .get(&ink.topic)
            .filter(|r| !r.is_empty())
        {
            return Ok(recipient.clone());
        }
        self.config
            .recipient
            .clone()
            .context("Signal recipient is required - set recipient or topic_recipient_mapping")
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
    fn name(&self) -> &str {
        &self.name
    }
    fn r#type(&self) -> &str {
        "signal"
    }

    async fn handle(&self, ink: &Ink) -> Result<()> {
        info!(
            "Signal reflex '{}' handling ink on topic: {}",
            self.name, ink.topic
        );

        let recipient = self.resolve_recipient(ink)?;
        let message = self.render_template(&self.config.message_template, ink);

        debug!(
            "Signal reflex '{}' sending to {} with message: {}",
            self.name, recipient, message
        );

        // Signal CLI HTTP API: POST /v2/message/{recipient}
        let url = format!(
            "{}/message/{}",
            self.config.api_url.trim_end_matches('/'),
            urlencoding::encode(&recipient)
        );

        let payload = serde_json::json!({
            "message": message,
            "is_group": false
        });

        let response = self
            .client
            .post(&url)
            .header("X-Signal-Password", &self.config.password)
            .json(&payload)
            .send()
            .await
            .with_context(|| format!("Failed to send Signal message to {}", recipient))?;

        if !response.status().is_success() {
            let status = response.status();
            let error_body = response.text().await.unwrap_or_default();
            return Err(anyhow::anyhow!(
                "Signal API error ({}): {}",
                status,
                error_body
            ));
        }

        info!(
            "Signal reflex '{}' sent message to recipient: {}",
            self.name, recipient
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_config() -> SignalReflexConfig {
        SignalReflexConfig {
            api_url: "http://localhost:8080/v2".to_string(),
            password: "test-password".to_string(),
            recipient: Some("+49123456789".to_string()),
            message_template: "{{body}}".to_string(),
            topic_recipient_mapping: HashMap::new(),
            use_https: false,
            timeout: 30,
        }
    }

    fn create_test_ink() -> Ink {
        Ink::new("/test/topic", "test", "test_source", "test body")
    }

    #[test]
    fn test_template_rendering() {
        let config = create_test_config();
        let app_ctx = AppContext::new();
        let reflex = SignalReflex::new("test".to_string(), config, &app_ctx);
        let ink = create_test_ink();
        assert_eq!(
            reflex.render_template("[{{topic}}] {{body}}", &ink),
            "[/test/topic] test body"
        );
    }

    #[test]
    fn test_resolve_recipient() {
        let config = create_test_config();
        let app_ctx = AppContext::new();
        let reflex = SignalReflex::new("test".to_string(), config, &app_ctx);
        let ink = create_test_ink();
        assert_eq!(reflex.resolve_recipient(&ink).unwrap(), "+49123456789");
    }

    #[test]
    fn test_resolve_recipient_mapping() {
        let mut config = create_test_config();
        config
            .topic_recipient_mapping
            .insert("/test/topic".to_string(), "+49987654321".to_string());
        let app_ctx = AppContext::new();
        let reflex = SignalReflex::new("test".to_string(), config, &app_ctx);
        let ink = create_test_ink();
        assert_eq!(reflex.resolve_recipient(&ink).unwrap(), "+49987654321");
    }

    #[test]
    fn test_config_deserialization() {
        let yaml = r#"
            api_url: "http://signal-cli:8080/v2"
            password: "secret"
            recipient: "+49123456789"
        "#;
        let config: SignalReflexConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.api_url, "http://signal-cli:8080/v2");
    }

    #[test]
    fn test_reflex_type() {
        let config = create_test_config();
        let app_ctx = AppContext::new();
        let reflex = SignalReflex::new("test".to_string(), config, &app_ctx);
        assert_eq!(reflex.r#type(), "signal");
    }
}
