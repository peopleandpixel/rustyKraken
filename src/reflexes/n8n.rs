use anyhow::{Context, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::{debug, info};

use super::Reflex;
use crate::model::Ink;

/// Configuration for n8n Reflex
/// Triggers n8n workflows via webhook
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct N8nReflexConfig {
    /// n8n server URL (e.g., "http://localhost:5678")
    pub host: String,

    /// Webhook path or ID
    pub webhook: String,

    /// Template for webhook path
    #[serde(default)]
    pub webhook_template: Option<String>,

    /// Template for payload (JSON)
    #[serde(default = "default_payload_template")]
    pub payload_template: String,

    /// HTTP method: "POST", "GET", "PUT", "PATCH"
    #[serde(default = "default_method")]
    pub method: String,

    /// Optional: mapping from Ink topics to webhook paths
    #[serde(default)]
    pub topic_webhook_mapping: HashMap<String, String>,

    /// Optional: mapping from Ink topics to payload templates
    #[serde(default)]
    pub topic_payload_mapping: HashMap<String, String>,

    /// API key for authentication (optional)
    #[serde(default)]
    pub api_key: Option<String>,

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

pub struct N8nReflex {
    name: String,
    config: N8nReflexConfig,
    client: reqwest::Client,
}

impl N8nReflex {
    pub fn new(name: String, config: N8nReflexConfig) -> Self {
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

    /// Resolve webhook path for a given Ink
    fn resolve_webhook(&self, ink: &Ink) -> String {
        if let Some(webhook) = self
            .config
            .topic_webhook_mapping
            .get(&ink.topic)
            .filter(|w| !w.is_empty())
        {
            return self.render_template(webhook, ink);
        }
        if let Some(ref template) = self.config.webhook_template {
            return self.render_template(template, ink);
        }
        self.config.webhook.clone()
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

    /// Build n8n webhook URL
    fn webhook_url(&self, webhook: &str) -> String {
        let host = self.config.host.trim_end_matches('/');
        let webhook_path = webhook.trim_start_matches('/');
        format!("{}/webhook/{}", host, webhook_path)
    }

    /// Trigger a webhook
    async fn trigger_webhook(
        &self,
        webhook: &str,
        payload: &serde_json::Value,
    ) -> Result<serde_json::Value> {
        let url = self.webhook_url(webhook);

        let mut request = match self.config.method.as_str() {
            "GET" => self.client.get(&url),
            "PUT" => self.client.put(&url).json(payload),
            "PATCH" => self.client.patch(&url).json(payload),
            _ => self.client.post(&url).json(payload),
        };

        if let Some(ref api_key) = self.config.api_key {
            request = request.header("Authorization", format!("Bearer {}", api_key));
        }

        let response = request
            .send()
            .await
            .with_context(|| format!("Failed to trigger n8n webhook at {}", url))?;

        if !response.status().is_success() {
            let status = response.status();
            let error_body = response.text().await.unwrap_or_default();
            return Err(anyhow::anyhow!(
                "n8n webhook error ({}): {}",
                status,
                error_body
            ));
        }

        // Try to parse JSON, fall back to empty object
        let text = response.text().await?;
        if text.trim().is_empty() {
            return Ok(serde_json::json!({}));
        }
        serde_json::from_str(&text).context("Failed to parse n8n webhook response")
    }
}

#[async_trait]
impl Reflex for N8nReflex {
    fn name(&self) -> &str {
        &self.name
    }

    fn r#type(&self) -> &str {
        "n8n"
    }

    async fn handle(&self, ink: &Ink) -> Result<()> {
        info!(
            "n8n reflex '{}' handling ink on topic: {}",
            self.name, ink.topic
        );

        // Resolve webhook
        let webhook = self.resolve_webhook(ink);
        debug!(
            "n8n reflex '{}' resolved webhook: {}",
            self.name, webhook
        );

        // Resolve and render payload
        let payload_template = self.resolve_payload_template(ink);
        let payload_rendered = self.render_template(&payload_template, ink);
        let payload: serde_json::Value = serde_json::from_str(&payload_rendered)
            .context("payload_template must render to valid JSON")?;

        debug!(
            "n8n reflex '{}' payload: {}",
            self.name, payload
        );

        // Trigger webhook
        let result = self.trigger_webhook(&webhook, &payload).await?;

        info!(
            "n8n reflex '{}' triggered webhook: {}",
            self.name, result
        );

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_config() -> N8nReflexConfig {
        N8nReflexConfig {
            host: "http://localhost:5678".to_string(),
            webhook: "test-webhook".to_string(),
            webhook_template: None,
            payload_template: default_payload_template(),
            method: "POST".to_string(),
            topic_webhook_mapping: HashMap::new(),
            topic_payload_mapping: HashMap::new(),
            api_key: None,
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
        let reflex = N8nReflex::new("test".to_string(), config.clone());
        let ink = create_test_ink();
        let rendered = reflex.render_template(&config.payload_template, &ink);
        let payload: serde_json::Value = serde_json::from_str(&rendered).unwrap();
        assert_eq!(payload["topic"], "/test/topic");
        assert_eq!(payload["body"], "test body");
    }

    #[test]
    fn test_config_deserialization() {
        let yaml = r#"
            host: "http://n8n:5678"
            webhook: "my-workflow"
            method: "POST"
        "#;
        let config: N8nReflexConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.host, "http://n8n:5678");
        assert_eq!(config.webhook, "my-workflow");
    }

    #[test]
    fn test_reflex_type() {
        let config = create_test_config();
        let reflex = N8nReflex::new("test".to_string(), config);
        assert_eq!(reflex.r#type(), "n8n");
    }

    #[test]
    fn test_resolve_webhook_from_mapping() {
        let mut config = create_test_config();
        let mut mapping = HashMap::new();
        mapping.insert("/n8n/alerts".to_string(), "alert-workflow".to_string());
        config.topic_webhook_mapping = mapping;

        let reflex = N8nReflex::new("test".to_string(), config);
        let ink = Ink::new("/n8n/alerts", "test", "test_source", "body");
        assert_eq!(reflex.resolve_webhook(&ink), "alert-workflow");
    }
}
