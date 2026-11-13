use anyhow::{Context, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::{debug, info};

use super::Reflex;
use crate::model::Ink;

/// Configuration for openHAB Reflex
/// Sends commands to openHAB via REST API
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OpenhabReflexConfig {
    /// openHAB server URL (e.g., "http://192.168.1.100:8080")
    pub host: String,

    /// API username (optional)
    #[serde(default)]
    pub username: Option<String>,

    /// API password (optional)
    #[serde(default)]
    pub password: Option<String>,

    /// Default item name
    #[serde(default)]
    pub item: Option<String>,

    /// Template for item name
    #[serde(default)]
    pub item_template: Option<String>,

    /// Template for command value
    #[serde(default = "default_value_template")]
    pub value_template: String,

    /// Optional: mapping from Ink topics to items
    #[serde(default)]
    pub topic_item_mapping: HashMap<String, String>,

    /// Verify SSL
    #[serde(default = "default_verify_ssl")]
    pub verify_ssl: bool,
}

fn default_value_template() -> String { "{{body}}".to_string() }
fn default_verify_ssl() -> bool { true }

pub struct OpenhabReflex {
    name: String,
    config: OpenhabReflexConfig,
    client: reqwest::Client,
}

impl OpenhabReflex {
    pub fn new(name: String, config: OpenhabReflexConfig) -> Self {
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

    fn resolve_item(&self, ink: &Ink) -> Result<String> {
        if let Some(item) = self
            .config
            .topic_item_mapping
            .get(&ink.topic)
            .filter(|i| !i.is_empty())
        {
            return Ok(item.clone());
        }
        if let Some(ref template) = self.config.item_template {
            return Ok(self.render_template(template, ink));
        }
        self.config
            .item
            .clone()
            .context("openHAB item is required - set item, item_template, or topic_item_mapping")
    }
}

#[async_trait]
impl Reflex for OpenhabReflex {
    fn name(&self) -> &str { &self.name }
    fn r#type(&self) -> &str { "openhab" }

    async fn handle(&self, ink: &Ink) -> Result<()> {
        info!("openHAB reflex '{}' handling ink on topic: {}", self.name, ink.topic);

        let item = self.resolve_item(ink)?;
        let value = self.render_template(&self.config.value_template, ink);
        let value_for_log = value.clone();

        debug!(
            "openHAB reflex '{}' item={} value={} on {}",
            self.name, item, value_for_log, self.config.host
        );

        // openHAB REST API: PUT /rest/items/{item}/state
        let url = format!(
            "{}/rest/items/{}/state",
            self.config.host.trim_end_matches('/'),
            item
        );

        let mut request = self.client.put(&url).body(value);
        if let (Some(user), Some(pass)) = (&self.config.username, &self.config.password) {
            request = request.basic_auth(user, Some(pass));
        }

        let response = request
            .send()
            .await
            .with_context(|| format!("Failed to send openHAB command to {}", self.config.host))?;

        if !response.status().is_success() {
            let status = response.status();
            let error_body = response.text().await.unwrap_or_default();
            return Err(anyhow::anyhow!("openHAB API error ({}): {}", status, error_body));
        }

        info!(
            "openHAB reflex '{}' sent value '{}' to item {}",
            self.name, value_for_log, item
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_config() -> OpenhabReflexConfig {
        OpenhabReflexConfig {
            host: "http://192.168.1.100:8080".to_string(),
            username: None,
            password: None,
            item: Some("LivingRoom_Light".to_string()),
            item_template: None,
            value_template: "{{body}}".to_string(),
            topic_item_mapping: HashMap::new(),
            verify_ssl: true,
        }
    }

    fn create_test_ink() -> Ink {
        Ink::new("/test/topic", "test", "test_source", "test body")
    }

    #[test]
    fn test_template_rendering() {
        let config = create_test_config();
        let reflex = OpenhabReflex::new("test".to_string(), config);
        let ink = create_test_ink();
        assert_eq!(reflex.render_template("{{topic}}-{{body}}", &ink), "/test/topic-test body");
    }

    #[test]
    fn test_resolve_item() {
        let config = create_test_config();
        let reflex = OpenhabReflex::new("test".to_string(), config);
        let ink = create_test_ink();
        assert_eq!(reflex.resolve_item(&ink).unwrap(), "LivingRoom_Light");
    }

    #[test]
    fn test_config_deserialization() {
        let yaml = r#"
            host: "http://openhab.local:8080"
            item: "GarageDoor"
            value_template: "{{body}}"
        "#;
        let config: OpenhabReflexConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.host, "http://openhab.local:8080");
    }

    #[test]
    fn test_reflex_type() {
        let config = create_test_config();
        let reflex = OpenhabReflex::new("test".to_string(), config);
        assert_eq!(reflex.r#type(), "openhab");
    }
}
