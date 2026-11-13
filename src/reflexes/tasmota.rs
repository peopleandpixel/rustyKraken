use anyhow::{Context, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::{debug, info};

use super::Reflex;
use crate::model::Ink;

/// Configuration for Tasmota Reflex
/// Controls Tasmota devices via HTTP API
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TasmotaReflexConfig {
    /// Tasmota device IP or hostname
    pub host: String,

    /// Username (optional)
    #[serde(default)]
    pub username: Option<String>,

    /// Password (optional)
    #[serde(default)]
    pub password: Option<String>,

    /// Default command (e.g., "Power1", "Backlight", "Dimmer")
    #[serde(default)]
    pub command: Option<String>,

    /// Template for command
    #[serde(default)]
    pub command_template: Option<String>,

    /// Optional: mapping from Ink topics to commands
    #[serde(default)]
    pub topic_command_mapping: HashMap<String, String>,

    /// Verify SSL
    #[serde(default = "default_verify_ssl")]
    pub verify_ssl: bool,
}

fn default_verify_ssl() -> bool {
    true
}

pub struct TasmotaReflex {
    name: String,
    config: TasmotaReflexConfig,
    client: reqwest::Client,
}

impl TasmotaReflex {
    pub fn new(name: String, config: TasmotaReflexConfig) -> Self {
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

    fn resolve_command(&self, ink: &Ink) -> String {
        if let Some(cmd) = self
            .config
            .topic_command_mapping
            .get(&ink.topic)
            .filter(|c| !c.is_empty())
        {
            return cmd.clone();
        }
        if let Some(ref template) = self.config.command_template {
            return self.render_template(template, ink);
        }
        self.config
            .command
            .clone()
            .unwrap_or_else(|| ink.body.clone())
    }
}

#[async_trait]
impl Reflex for TasmotaReflex {
    fn name(&self) -> &str {
        &self.name
    }
    fn r#type(&self) -> &str {
        "tasmota"
    }

    async fn handle(&self, ink: &Ink) -> Result<()> {
        info!(
            "Tasmota reflex '{}' handling ink on topic: {}",
            self.name, ink.topic
        );

        let command = self.resolve_command(ink);

        debug!(
            "Tasmota reflex '{}' command={} on {}",
            self.name, command, self.config.host
        );

        // Tasmota HTTP API: /cm?cmnd={command}
        let url = format!(
            "http://{}/cm?cmnd={}",
            self.config.host,
            urlencoding::encode(&command)
        );

        let mut request = self.client.get(&url);
        if let (Some(user), Some(pass)) = (&self.config.username, &self.config.password) {
            request = request.basic_auth(user, Some(pass));
        }

        let response = request
            .send()
            .await
            .with_context(|| format!("Failed to send Tasmota command to {}", self.config.host))?;

        if !response.status().is_success() {
            let status = response.status();
            let error_body = response.text().await.unwrap_or_default();
            return Err(anyhow::anyhow!(
                "Tasmota API error ({}): {}",
                status,
                error_body
            ));
        }

        info!(
            "Tasmota reflex '{}' sent command '{}' to {}",
            self.name, command, self.config.host
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_config() -> TasmotaReflexConfig {
        TasmotaReflexConfig {
            host: "192.168.1.50".to_string(),
            username: None,
            password: None,
            command: Some("Power1".to_string()),
            command_template: None,
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
        let reflex = TasmotaReflex::new("test".to_string(), config);
        let ink = create_test_ink();
        assert_eq!(
            reflex.render_template("{{topic}}-{{body}}", &ink),
            "/test/topic-test body"
        );
    }

    #[test]
    fn test_resolve_command() {
        let config = create_test_config();
        let reflex = TasmotaReflex::new("test".to_string(), config);
        let ink = create_test_ink();
        assert_eq!(reflex.resolve_command(&ink), "Power1");
    }

    #[test]
    fn test_config_deserialization() {
        let yaml = r#"
            host: "192.168.1.100"
            command: "Dimmer 50"
        "#;
        let config: TasmotaReflexConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.host, "192.168.1.100");
    }

    #[test]
    fn test_reflex_type() {
        let config = create_test_config();
        let reflex = TasmotaReflex::new("test".to_string(), config);
        assert_eq!(reflex.r#type(), "tasmota");
    }
}
