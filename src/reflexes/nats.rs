use anyhow::{Context, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::{debug, info};

use super::Reflex;
use crate::model::Ink;

/// Configuration for NATS Reflex
/// Publishes messages to NATS subjects
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NatsReflexConfig {
    /// NATS server URL (e.g., "nats://localhost:4222")
    pub server: String,

    /// Default NATS subject
    #[serde(default = "default_subject")]
    pub subject: String,

    /// Template for NATS subject
    #[serde(default)]
    pub subject_template: Option<String>,

    /// Template for the message payload
    #[serde(default = "default_payload_template")]
    pub payload_template: String,

    /// Optional: mapping from Ink topics to NATS subjects
    #[serde(default)]
    pub subject_mapping: HashMap<String, String>,

    /// Username (optional)
    #[serde(default)]
    pub username: Option<String>,

    /// Password (optional)
    #[serde(default)]
    pub password: Option<String>,

    /// Token (optional)
    #[serde(default)]
    pub token: Option<String>,
}

fn default_subject() -> String { "kraken.events".to_string() }
fn default_payload_template() -> String { "{{body}}".to_string() }

pub struct NatsReflex {
    name: String,
    config: NatsReflexConfig,
}

impl NatsReflex {
    pub fn new(name: String, config: NatsReflexConfig) -> Self {
        Self { name, config }
    }

    fn resolve_subject(&self, ink: &Ink) -> String {
        if let Some(subject) = self
            .config
            .subject_mapping
            .get(&ink.topic)
            .filter(|s| !s.is_empty())
        {
            return self.render_template(subject, ink);
        }
        if let Some(ref template) = self.config.subject_template {
            return self.render_template(template, ink);
        }
        self.config.subject.clone()
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
impl Reflex for NatsReflex {
    fn name(&self) -> &str { &self.name }
    fn r#type(&self) -> &str { "nats" }

    async fn handle(&self, ink: &Ink) -> Result<()> {
        info!("NATS reflex '{}' handling ink on topic: {}", self.name, ink.topic);

        let subject = self.resolve_subject(ink);
        let payload = self.render_template(&self.config.payload_template, ink);

        debug!(
            "NATS reflex '{}' publishing to {} with payload: {}",
            self.name, subject, payload
        );

        let connection = async_nats::connect(&self.config.server)
            .await
            .with_context(|| format!("Failed to connect to NATS server at {}", self.config.server))?;

        connection
            .publish(subject.clone(), payload.into())
            .await
            .with_context(|| format!("Failed to publish to NATS subject {}", subject))?;

        info!(
            "NATS reflex '{}' published to subject: {}",
            self.name, subject
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_config() -> NatsReflexConfig {
        NatsReflexConfig {
            server: "nats://localhost:4222".to_string(),
            subject: "kraken.events".to_string(),
            subject_template: None,
            payload_template: "{{body}}".to_string(),
            subject_mapping: HashMap::new(),
            username: None,
            password: None,
            token: None,
        }
    }

    fn create_test_ink() -> Ink {
        Ink::new("/test/topic", "test", "test_source", "test body")
    }

    #[test]
    fn test_template_rendering() {
        let config = create_test_config();
        let reflex = NatsReflex::new("test".to_string(), config);
        let ink = create_test_ink();
        assert_eq!(reflex.render_template("[{{topic}}] {{body}}", &ink), "[/test/topic] test body");
    }

    #[test]
    fn test_resolve_subject() {
        let config = create_test_config();
        let reflex = NatsReflex::new("test".to_string(), config);
        let ink = create_test_ink();
        assert_eq!(reflex.resolve_subject(&ink), "kraken.events");
    }

    #[test]
    fn test_config_deserialization() {
        let yaml = r#"
            server: "nats://nats.example.com:4222"
            subject: "alerts"
        "#;
        let config: NatsReflexConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.server, "nats://nats.example.com:4222");
    }

    #[test]
    fn test_reflex_type() {
        let config = create_test_config();
        let reflex = NatsReflex::new("test".to_string(), config);
        assert_eq!(reflex.r#type(), "nats");
    }
}
