use anyhow::{Context, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::{debug, info};

use super::Reflex;
use crate::model::Ink;

/// Configuration for Zapier Reflex
/// Triggers Zapier webhooks
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ZapierReflexConfig {
    /// Zapier webhook URL
    pub webhook_url: String,

    /// Template for webhook URL
    #[serde(default)]
    pub webhook_url_template: Option<String>,

    /// Template for payload (JSON)
    #[serde(default = "default_payload_template")]
    pub payload_template: String,

    /// HTTP method: "POST", "GET"
    #[serde(default = "default_method")]
    pub method: String,

    /// Optional: mapping from Ink topics to webhook URLs
    #[serde(default)]
    pub topic_webhook_mapping: HashMap<String, String>,

    /// Optional: mapping from Ink topics to payload templates
    #[serde(default)]
    pub topic_payload_mapping: HashMap<String, String>,

    /// Whether to verify TLS certificates
    #[serde(default = "default_verify_ssl")]
    pub verify_ssl: bool,

    /// Timeout in seconds
    #[serde(default = "default_timeout")]
    pub timeout: u64,
}

fn default_payload_template() -> String {
    r#"{"topic": "{{topic}}", "body": "{{body}}", "id": "{{id}}", "timestamp": "{{timestamp}}"}"#.to_string()
}
fn default_method() -> String {
    "POST".to_string()
}
fn default_verify_ssl() -> bool {
    true
}
fn default_timeout() -> u64 {
    30
}

pub struct ZapierReflex {
    name: String,
    config: ZapierReflexConfig,
    client: reqwest::Client,
}

impl ZapierReflex {
    pub fn new(name: String, config: ZapierReflexConfig) -> Self {
        let verify_ssl = config.verify_ssl;
        Self {
            name,
            config,
            client: reqwest::Client::builder()
                .danger_accept_invalid_certs(!verify_ssl)
                .build()
                .unwrap_or_else(|_| reqwest::Client::new()),
        }
    }

    /// Resolve webhook URL for a given Ink
    fn resolve_webhook_url(&self, ink: &Ink) -> String {
        if let Some(url) = self
            .config
            .topic_webhook_mapping
            .get(&ink.topic)
            .filter(|u| !u.is_empty())
        {
            return self.render_template(url, ink);
        }
        if let Some(ref template) = self.config.webhook_url_template {
            return self.render_template(template, ink);
        }
        self.config.webhook_url.clone()
    }

    /// Resolve payload template for a given Ink
    fn resolve_payload_template(&self, ink: &Ink) -> String {
        if let Some(template) = self
            .config
            .topic_payload_mapping
            .get(&ink.topic)
            .filter(|t| !t.is_empty())
        {
            return template.clone();
        }
        self.config.payload_template.clone()
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

    /// Trigger Zapier webhook
    async fn trigger_webhook(
        &self,
        url: &str,
        payload: &serde_json::Value,
    ) -> Result<()> {
        let request = match self.config.method.as_str() {
            "GET" => self.client.get(url),
            _ => self.client.post(url).json(payload),
        };

        let response = request
            .send()
            .await
            .with_context(|| format!("Failed to trigger Zapier webhook at {}", url))?;

        if !response.status().is_success() {
            let status = response.status();
            let error_body = response.text().await.unwrap_or_default();
            return Err(anyhow::anyhow!(
                "Zapier webhook error ({}): {}",
                status,
                error_body
            ));
        }

        Ok(())
    }
}

#[async_trait]
impl Reflex for ZapierReflex {
    fn name(&self) -> &str {
        &self.name
    }

    fn r#type(&self) -> &str {
        "zapier"
    }

    async fn handle(&self, ink: &Ink) -> Result<()> {
        info!(
            "Zapier reflex '{}' handling ink on topic: {}",
            self.name, ink.topic
        );

        // Resolve webhook URL
        let webhook_url = self.resolve_webhook_url(ink);
        debug!(
            "Zapier reflex '{}' resolved webhook URL: {}",
            self.name, webhook_url
        );

        // Resolve and render payload
        let payload_template = self.resolve_payload_template(ink);
        let payload_rendered = self.render_template(&payload_template, ink);
        let payload: serde_json::Value = serde_json::from_str(&payload_rendered)
            .context("payload_template must render to valid JSON")?;

        debug!(
            "Zapier reflex '{}' payload: {}",
            self.name, payload
        );

        // Trigger webhook
        self.trigger_webhook(&webhook_url, &payload).await?;

        info!(
            "Zapier reflex '{}' triggered webhook successfully",
            self.name
        );

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_config() -> ZapierReflexConfig {
        ZapierReflexConfig {
            webhook_url: "https://hooks.zapier.com/hooks/catch/test".to_string(),
            webhook_url_template: None,
            payload_template: default_payload_template(),
            method: "POST".to_string(),
            topic_webhook_mapping: HashMap::new(),
            topic_payload_mapping: HashMap::new(),
            verify_ssl: true,
            timeout: 30,
        }
    }

    fn create_test_ink() -> Ink {
        Ink::new("/test/topic", "test", "test_source", "test body")
    }

    #[test]
    fn test_template_rendering() {
        let config = create_test_config();
        let reflex = ZapierReflex::new("test".to_string(), config.clone());
        let ink = create_test_ink();
        let rendered = reflex.render_template(&config.payload_template, &ink);
        let payload: serde_json::Value = serde_json::from_str(&rendered).unwrap();
        assert_eq!(payload["topic"], "/test/topic");
        assert_eq!(payload["body"], "test body");
    }

    #[test]
    fn test_config_deserialization() {
        let yaml = r#"
            webhook_url: "https://hooks.zapier.com/hooks/catch/myhook"
            method: "POST"
        "#;
        let config: ZapierReflexConfig = serde_yaml::from_str(yaml).unwrap();
        assert!(config.webhook_url.contains("zapier"));
    }

    #[test]
    fn test_reflex_type() {
        let config = create_test_config();
        let reflex = ZapierReflex::new("test".to_string(), config);
        assert_eq!(reflex.r#type(), "zapier");
    }

    #[test]
    fn test_resolve_webhook_from_mapping() {
        let mut config = create_test_config();
        let mut mapping = HashMap::new();
        mapping.insert("/zapier/alerts".to_string(), "https://hooks.zapier.com/alerts".to_string());
        config.topic_webhook_mapping = mapping;

        let reflex = ZapierReflex::new("test".to_string(), config);
        let ink = Ink::new("/zapier/alerts", "test", "test_source", "body");
        assert!(reflex.resolve_webhook_url(&ink).contains("alerts"));
    }
}
