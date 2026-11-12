use anyhow::{Context, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::{debug, error, info};

use super::Reflex;
use crate::model::Ink;

/// Configuration for RabbitMQ Reflex (HTTP management API)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RabbitMqReflexConfig {
    /// RabbitMQ management API URL
    pub api_url: String,
    /// Username
    pub username: String,
    /// Password
    pub password: String,
    /// Virtual host
    #[serde(default = "default_vhost")]
    pub vhost: String,
    /// Exchange name
    pub exchange: String,
    /// Routing key
    #[serde(default = "default_routing_key")]
    pub routing_key: String,
    /// Template for the message body
    #[serde(default = "default_message_template")]
    pub message_template: String,
    /// Optional: mapping from Ink topics to routing keys
    #[serde(default)]
    pub topic_routing_key_mapping: HashMap<String, String>,
}

fn default_vhost() -> String { "/".to_string() }
fn default_routing_key() -> String { "kraken".to_string() }
fn default_message_template() -> String { "{{body}}".to_string() }

pub struct RabbitMqReflex {
    name: String,
    config: RabbitMqReflexConfig,
    client: reqwest::Client,
}

impl RabbitMqReflex {
    pub fn new(name: String, config: RabbitMqReflexConfig) -> Self {
        Self { name, config, client: reqwest::Client::new() }
    }

    fn resolve_routing_key(&self, ink: &Ink) -> String {
        if let Some(key) = self.config.topic_routing_key_mapping.get(&ink.topic).filter(|k| !k.is_empty()) {
            return key.clone();
        }
        self.config.routing_key.clone()
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
impl Reflex for RabbitMqReflex {
    fn name(&self) -> &str { &self.name }
    fn r#type(&self) -> &str { "rabbitmq" }

    async fn handle(&self, ink: &Ink) -> Result<()> {
        info!("RabbitMQ reflex '{}' handling ink on topic: {}", self.name, ink.topic);
        let routing_key = self.resolve_routing_key(ink);
        let body = self.render_template(&self.config.message_template, ink);
        debug!("RabbitMQ reflex '{}' publishing to '{}' with key '{}' for topic: {}", self.name, self.config.exchange, routing_key, ink.topic);

        let vhost_encoded = urlencoding::encode(&self.config.vhost);
        let url = format!("{}/api/exchanges/{}/{}", self.config.api_url.trim_end_matches('/'), vhost_encoded, urlencoding::encode(&self.config.exchange));

        let payload = serde_json::json!({
            "routing_key": routing_key,
            "props": {},
            "body": body,
            "headers": {}
        });

        let response = self.client.post(&url)
            .basic_auth(&self.config.username, Some(&self.config.password))
            .json(&payload)
            .send()
            .await
            .context("Failed to publish to RabbitMQ")?;

        if !response.status().is_success() {
            let status = response.status();
            let error_body = response.text().await.unwrap_or_default();
            return Err(anyhow::anyhow!("RabbitMQ API error ({}): {}", status, error_body));
        }
        info!("RabbitMQ reflex '{}' published message for topic: {}", self.name, ink.topic);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_config() -> RabbitMqReflexConfig {
        RabbitMqReflexConfig {
            api_url: "http://localhost:15672".to_string(),
            username: "guest".to_string(),
            password: "guest".to_string(),
            vhost: "/".to_string(),
            exchange: "kraken".to_string(),
            routing_key: "kraken".to_string(),
            message_template: "{{body}}".to_string(),
            topic_routing_key_mapping: HashMap::new(),
        }
    }

    fn create_test_ink() -> Ink {
        Ink::new("/test/topic", "test", "test_source", "test body")
    }

    #[test]
    fn test_default_routing_key() {
        let config = create_test_config();
        let reflex = RabbitMqReflex::new("test".to_string(), config);
        let ink = create_test_ink();
        assert_eq!(reflex.resolve_routing_key(&ink), "kraken");
    }

    #[test]
    fn test_template_rendering() {
        let config = create_test_config();
        let reflex = RabbitMqReflex::new("test".to_string(), config);
        let ink = create_test_ink();
        assert_eq!(reflex.render_template("[{{topic}}] {{body}}", &ink), "[/test/topic] test body");
    }

    #[test]
    fn test_config_deserialization() {
        let yaml = r#"
            api_url: "http://localhost:15672"
            username: "guest"
            password: "guest"
            exchange: "kraken"
        "#;
        let config: RabbitMqReflexConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.api_url, "http://localhost:15672");
    }

    #[test]
    fn test_reflex_type() {
        let config = create_test_config();
        let reflex = RabbitMqReflex::new("test".to_string(), config);
        assert_eq!(reflex.r#type(), "rabbitmq");
    }
}
