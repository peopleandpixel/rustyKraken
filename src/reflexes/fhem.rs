use anyhow::{Context, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::{debug, info};

use super::Reflex;
use crate::model::Ink;

/// Configuration for FHEM Reflex
/// Controls FHEM devices via JSON2 API
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FhemReflexConfig {
    /// FHEM server URL (e.g., "http://192.168.1.100:8083")
    pub host: String,

    /// FHEM password (optional)
    #[serde(default)]
    pub password: Option<String>,

    /// Default FHEM command
    #[serde(default)]
    pub command: Option<String>,

    /// Default device name
    #[serde(default)]
    pub device: Option<String>,

    /// Template for device name
    #[serde(default)]
    pub device_template: Option<String>,

    /// Template for command
    #[serde(default)]
    pub command_template: Option<String>,

    /// Optional: mapping from Ink topics to devices
    #[serde(default)]
    pub topic_device_mapping: HashMap<String, String>,

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

pub struct FhemReflex {
    name: String,
    config: FhemReflexConfig,
    client: reqwest::Client,
}

impl FhemReflex {
    pub fn new(name: String, config: FhemReflexConfig) -> Self {
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

    fn resolve_device(&self, ink: &Ink) -> Result<String> {
        if let Some(device) = self
            .config
            .topic_device_mapping
            .get(&ink.topic)
            .filter(|d| !d.is_empty())
        {
            return Ok(device.clone());
        }
        if let Some(ref template) = self.config.device_template {
            return Ok(self.render_template(template, ink));
        }
        self.config.device.clone().context(
            "FHEM device is required - set device, device_template, or topic_device_mapping",
        )
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
        self.config.command.clone().unwrap_or(ink.body.clone())
    }
}

#[async_trait]
impl Reflex for FhemReflex {
    fn name(&self) -> &str {
        &self.name
    }
    fn r#type(&self) -> &str {
        "fhem"
    }

    async fn handle(&self, ink: &Ink) -> Result<()> {
        info!(
            "FHEM reflex '{}' handling ink on topic: {}",
            self.name, ink.topic
        );

        let device = self.resolve_device(ink)?;
        let command = self.resolve_command(ink);

        debug!(
            "FHEM reflex '{}' device={} command={} on {}",
            self.name, device, command, self.config.host
        );

        // FHEM JSON2 API: /fhem?cmd={device}%20{command}&fwid=0&json=2
        let fhem_cmd = format!("{} {}", device, command);
        let url = format!(
            "{}/fhem?cmd={}&fwid=0&json=2",
            self.config.host.trim_end_matches('/'),
            urlencoding::encode(&fhem_cmd)
        );

        let mut request = self.client.get(&url);
        if let Some(ref password) = self.config.password {
            request = request.basic_auth("admin", Some(password));
        }

        let response = request
            .send()
            .await
            .with_context(|| format!("Failed to send FHEM command to {}", self.config.host))?;

        if !response.status().is_success() {
            let status = response.status();
            let error_body = response.text().await.unwrap_or_default();
            return Err(anyhow::anyhow!(
                "FHEM API error ({}): {}",
                status,
                error_body
            ));
        }

        info!(
            "FHEM reflex '{}' sent command '{}' to device {}",
            self.name, command, device
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_config() -> FhemReflexConfig {
        FhemReflexConfig {
            host: "http://192.168.1.100:8083".to_string(),
            password: None,
            command: Some("on".to_string()),
            device: Some("Light_LivingRoom".to_string()),
            device_template: None,
            command_template: None,
            topic_device_mapping: HashMap::new(),
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
        let reflex = FhemReflex::new("test".to_string(), config);
        let ink = create_test_ink();
        assert_eq!(
            reflex.render_template("{{topic}}-{{body}}", &ink),
            "/test/topic-test body"
        );
    }

    #[test]
    fn test_resolve_device() {
        let config = create_test_config();
        let reflex = FhemReflex::new("test".to_string(), config);
        let ink = create_test_ink();
        assert_eq!(reflex.resolve_device(&ink).unwrap(), "Light_LivingRoom");
    }

    #[test]
    fn test_resolve_command() {
        let config = create_test_config();
        let reflex = FhemReflex::new("test".to_string(), config);
        let ink = create_test_ink();
        assert_eq!(reflex.resolve_command(&ink), "on");
    }

    #[test]
    fn test_config_deserialization() {
        let yaml = r#"
            host: "http://fhem.local:8083"
            device: "Switch_Garage"
            command: "on"
        "#;
        let config: FhemReflexConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.host, "http://fhem.local:8083");
    }

    #[test]
    fn test_reflex_type() {
        let config = create_test_config();
        let reflex = FhemReflex::new("test".to_string(), config);
        assert_eq!(reflex.r#type(), "fhem");
    }
}
