use anyhow::{Context, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::{debug, info};

use super::Reflex;
use crate::model::Ink;

/// Configuration for ESPHome Reflex
/// Controls ESPHome devices via REST API
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EsphomeReflexConfig {
    /// ESPHome device URL (e.g., "http://192.168.1.100")
    pub host: String,

    /// API port (default: 80)
    #[serde(default = "default_port")]
    pub port: u16,

    /// API password (optional)
    #[serde(default)]
    pub password: Option<String>,

    /// Default action: command, status
    #[serde(default = "default_action")]
    pub action: String,

    /// Default command (e.g., "light.toggle", "switch.turn_on")
    #[serde(default)]
    pub command: Option<String>,

    /// Default entity ID
    #[serde(default)]
    pub entity_id: Option<String>,

    /// Template for entity ID
    #[serde(default)]
    pub entity_id_template: Option<String>,

    /// Template for command data
    #[serde(default)]
    pub command_data_template: Option<String>,

    /// Optional: mapping from Ink topics to entity IDs
    #[serde(default)]
    pub topic_entity_mapping: HashMap<String, String>,

    /// Optional: mapping from Ink topics to commands
    #[serde(default)]
    pub topic_command_mapping: HashMap<String, String>,

    /// Verify SSL
    #[serde(default = "default_verify_ssl")]
    pub verify_ssl: bool,
}

fn default_port() -> u16 { 80 }
fn default_action() -> String { "command".to_string() }
fn default_verify_ssl() -> bool { true }

pub struct EsphomeReflex {
    name: String,
    config: EsphomeReflexConfig,
    client: reqwest::Client,
}

impl EsphomeReflex {
    pub fn new(name: String, config: EsphomeReflexConfig) -> Self {
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

    fn resolve_entity_id(&self, ink: &Ink) -> Result<String> {
        if let Some(entity) = self
            .config
            .topic_entity_mapping
            .get(&ink.topic)
            .filter(|e| !e.is_empty())
        {
            return Ok(entity.clone());
        }
        if let Some(ref template) = self.config.entity_id_template {
            return Ok(self.render_template(template, ink));
        }
        self.config
            .entity_id
            .clone()
            .context("ESPHome entity_id is required - set entity_id, entity_id_template, or topic_entity_mapping")
    }

    fn resolve_command(&self, ink: &Ink) -> String {
        if let Some(cmd) = self
            .config
            .topic_command_mapping
            .get(&ink.topic)
            .filter(|c| !c.is_empty())
        {
            return cmd.clone();
        }
        self.config.command.clone().unwrap_or("light.toggle".to_string())
    }
}

#[async_trait]
impl Reflex for EsphomeReflex {
    fn name(&self) -> &str { &self.name }
    fn r#type(&self) -> &str { "esphome" }

    async fn handle(&self, ink: &Ink) -> Result<()> {
        info!("ESPHome reflex '{}' handling ink on topic: {}", self.name, ink.topic);

        let entity_id = self.resolve_entity_id(ink)?;
        let command = self.resolve_command(ink);

        debug!(
            "ESPHome reflex '{}' command={} entity={} on {}",
            self.name, command, entity_id, self.config.host
        );

        let host = format!("{}:{}", self.config.host, self.config.port);

        // ESPHome REST API: POST /command/{entity_id}
        let url = format!("{}/command/{}", host, entity_id);

        let payload = if let Some(ref template) = self.config.command_data_template {
            let data = self.render_template(template, ink);
            serde_json::from_str(&data).unwrap_or(serde_json::json!({
                "command": command,
                "value": ink.body.clone()
            }))
        } else {
            serde_json::json!({
                "command": command,
                "value": ink.body.clone()
            })
        };

        let mut request = self.client.post(&url).json(&payload);
        if let Some(ref password) = self.config.password {
            request = request.basic_auth("", Some(password));
        }

        let response = request
            .send()
            .await
            .with_context(|| format!("Failed to send ESPHome command to {}", host))?;

        if !response.status().is_success() {
            let status = response.status();
            let error_body = response.text().await.unwrap_or_default();
            return Err(anyhow::anyhow!("ESPHome API error ({}): {}", status, error_body));
        }

        info!(
            "ESPHome reflex '{}' sent command {} to entity {}",
            self.name, command, entity_id
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_config() -> EsphomeReflexConfig {
        EsphomeReflexConfig {
            host: "192.168.1.100".to_string(),
            port: 80,
            password: None,
            action: "command".to_string(),
            command: Some("light.toggle".to_string()),
            entity_id: Some("light.living_room".to_string()),
            entity_id_template: None,
            command_data_template: None,
            topic_entity_mapping: HashMap::new(),
            topic_command_mapping: HashMap::new(),
            verify_ssl: true,
        }
    }

    fn create_test_ink() -> Ink {
        Ink::new("/test/topic", "test", "test_source", "test body")
    }

    #[test]
    fn test_template_rendering() {
        let config = create_test_config();
        let reflex = EsphomeReflex::new("test".to_string(), config);
        let ink = create_test_ink();
        assert_eq!(reflex.render_template("{{topic}}-{{body}}", &ink), "/test/topic-test body");
    }

    #[test]
    fn test_resolve_entity_id() {
        let config = create_test_config();
        let reflex = EsphomeReflex::new("test".to_string(), config);
        let ink = create_test_ink();
        assert_eq!(reflex.resolve_entity_id(&ink).unwrap(), "light.living_room");
    }

    #[test]
    fn test_resolve_command() {
        let config = create_test_config();
        let reflex = EsphomeReflex::new("test".to_string(), config);
        let ink = create_test_ink();
        assert_eq!(reflex.resolve_command(&ink), "light.toggle");
    }

    #[test]
    fn test_config_deserialization() {
        let yaml = r#"
            host: "192.168.1.50"
            port: 8080
            entity_id: "switch.garage"
            command: "switch.turn_on"
        "#;
        let config: EsphomeReflexConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.host, "192.168.1.50");
    }

    #[test]
    fn test_reflex_type() {
        let config = create_test_config();
        let reflex = EsphomeReflex::new("test".to_string(), config);
        assert_eq!(reflex.r#type(), "esphome");
    }
}
