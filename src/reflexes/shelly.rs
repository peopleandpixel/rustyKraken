#![allow(dead_code)]

use anyhow::{Context, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::{debug, error, info};

use super::Reflex;
use crate::model::Ink;

/// Configuration for Shelly Reflex
/// Controls Shelly smart devices via HTTP API
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShellyReflexConfig {
    /// Shelly device IP or hostname
    pub host: String,

    /// Default action: "on", "off", "toggle", "status", "relay"
    #[serde(default = "default_action")]
    pub action: String,

    /// Default relay ID (0, 1, etc.)
    #[serde(default)]
    pub relay: Option<u8>,

    /// Template for relay ID
    #[serde(default)]
    pub relay_template: Option<String>,

    /// Optional: mapping from Ink topics to actions
    #[serde(default)]
    pub topic_action_mapping: HashMap<String, String>,

    /// Optional: mapping from Ink topics to relay IDs
    #[serde(default)]
    pub topic_relay_mapping: HashMap<String, u8>,

    /// Username for authentication (optional)
    #[serde(default)]
    pub username: Option<String>,

    /// Password for authentication (optional)
    #[serde(default)]
    pub password: Option<String>,

    /// Whether to verify TLS certificates
    #[serde(default = "default_verify_ssl")]
    pub verify_ssl: bool,

    /// Timeout in seconds
    #[serde(default = "default_timeout")]
    pub timeout: u64,
}

fn default_action() -> String {
    "toggle".to_string()
}
fn default_verify_ssl() -> bool {
    true
}
fn default_timeout() -> u64 {
    10
}

pub struct ShellyReflex {
    name: String,
    config: ShellyReflexConfig,
    client: reqwest::Client,
}

impl ShellyReflex {
    pub fn new(name: String, config: ShellyReflexConfig) -> Self {
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

    /// Resolve action for a given Ink
    fn resolve_action(&self, ink: &Ink) -> String {
        if let Some(action) = self
            .config
            .topic_action_mapping
            .get(&ink.topic)
            .filter(|a| !a.is_empty())
        {
            return action.clone();
        }
        self.config.action.clone()
    }

    /// Resolve relay ID for a given Ink
    fn resolve_relay(&self, ink: &Ink) -> u8 {
        if let Some(relay) = self.config.topic_relay_mapping.get(&ink.topic) {
            return *relay;
        }
        self.config.relay.unwrap_or(0)
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

    /// Build Shelly URL
    fn shelly_url(&self, path: &str) -> String {
        let host = self.config.host.trim_end_matches('/');
        if host.starts_with("http://") || host.starts_with("https://") {
            format!("{}{}", host, path)
        } else {
            format!("http://{}{}", host, path)
        }
    }

    /// Build auth request
    fn build_auth_request(&self, request: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        let mut req = request;
        if let (Some(user), Some(pass)) = (&self.config.username, &self.config.password) {
            req = req.basic_auth(user, Some(pass));
        }
        req
    }

    /// Turn relay on
    async fn turn_on(&self, relay: u8) -> Result<()> {
        info!("Shelly reflex '{}' turning relay {} on", self.name, relay);

        let url = self.shelly_url(&format!("/relay/{}", relay));
        let response = self
            .build_auth_request(self.client.get(format!("{}?turn=on", url)))
            .send()
            .await
            .with_context(|| format!("Failed to turn on Shelly relay {}", relay))?;

        if !response.status().is_success() {
            let status = response.status();
            let error_body = response.text().await.unwrap_or_default();
            return Err(anyhow::anyhow!(
                "Shelly turn on error ({}): {}",
                status,
                error_body
            ));
        }

        info!("Shelly reflex '{}' turned relay {} on", self.name, relay);
        Ok(())
    }

    /// Turn relay off
    async fn turn_off(&self, relay: u8) -> Result<()> {
        info!("Shelly reflex '{}' turning relay {} off", self.name, relay);

        let url = self.shelly_url(&format!("/relay/{}", relay));
        let response = self
            .build_auth_request(self.client.get(format!("{}?turn=off", url)))
            .send()
            .await
            .with_context(|| format!("Failed to turn off Shelly relay {}", relay))?;

        if !response.status().is_success() {
            let status = response.status();
            let error_body = response.text().await.unwrap_or_default();
            return Err(anyhow::anyhow!(
                "Shelly turn off error ({}): {}",
                status,
                error_body
            ));
        }

        info!("Shelly reflex '{}' turned relay {} off", self.name, relay);
        Ok(())
    }

    /// Toggle relay
    async fn toggle(&self, relay: u8) -> Result<()> {
        info!("Shelly reflex '{}' toggling relay {}", self.name, relay);

        let url = self.shelly_url(&format!("/relay/{}", relay));
        let response = self
            .build_auth_request(self.client.get(format!("{}?turn=toggle", url)))
            .send()
            .await
            .with_context(|| format!("Failed to toggle Shelly relay {}", relay))?;

        if !response.status().is_success() {
            let status = response.status();
            let error_body = response.text().await.unwrap_or_default();
            return Err(anyhow::anyhow!(
                "Shelly toggle error ({}): {}",
                status,
                error_body
            ));
        }

        info!("Shelly reflex '{}' toggled relay {}", self.name, relay);
        Ok(())
    }

    /// Get device status
    async fn get_status(&self) -> Result<serde_json::Value> {
        let url = self.shelly_url("/status");

        let response = self
            .build_auth_request(self.client.get(&url))
            .send()
            .await
            .context("Failed to get Shelly status")?;

        if !response.status().is_success() {
            let status = response.status();
            return Err(anyhow::anyhow!("Shelly status error: {}", status));
        }

        response
            .json()
            .await
            .context("Failed to parse Shelly status response")
    }
}

#[async_trait]
impl Reflex for ShellyReflex {
    fn name(&self) -> &str {
        &self.name
    }

    fn r#type(&self) -> &str {
        "shelly"
    }

    async fn handle(&self, ink: &Ink) -> Result<()> {
        info!(
            "Shelly reflex '{}' handling ink on topic: {}",
            self.name, ink.topic
        );

        let action = self.resolve_action(ink);
        let relay = self.resolve_relay(ink);

        debug!(
            "Shelly reflex '{}' action={}, relay={}",
            self.name, action, relay
        );

        match action.as_str() {
            "on" => self.turn_on(relay).await?,
            "off" => self.turn_off(relay).await?,
            "toggle" => self.toggle(relay).await?,
            "status" => {
                let status = self.get_status().await?;
                debug!("Shelly reflex '{}' status: {}", self.name, status);
            }
            _ => {
                error!("Shelly reflex '{}' unknown action: {}", self.name, action);
                return Err(anyhow::anyhow!("Unknown Shelly action: {}", action));
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_config() -> ShellyReflexConfig {
        ShellyReflexConfig {
            host: "192.168.1.100".to_string(),
            action: "toggle".to_string(),
            relay: Some(0),
            relay_template: None,
            topic_action_mapping: HashMap::new(),
            topic_relay_mapping: HashMap::new(),
            username: None,
            password: None,
            verify_ssl: true,
            timeout: 10,
        }
    }

    fn create_test_ink() -> Ink {
        Ink::new("/test/topic", "test", "test_source", "test body")
    }

    #[test]
    fn test_config_deserialization() {
        let yaml = r#"
            host: "192.168.1.100"
            action: "on"
            relay: 1
        "#;
        let config: ShellyReflexConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.host, "192.168.1.100");
        assert_eq!(config.action, "on");
        assert_eq!(config.relay, Some(1));
    }

    #[test]
    fn test_reflex_type() {
        let config = create_test_config();
        let reflex = ShellyReflex::new("test".to_string(), config);
        assert_eq!(reflex.r#type(), "shelly");
    }

    #[test]
    fn test_resolve_action_from_mapping() {
        let mut config = create_test_config();
        let mut mapping = HashMap::new();
        mapping.insert("/shelly/on".to_string(), "on".to_string());
        config.topic_action_mapping = mapping;

        let reflex = ShellyReflex::new("test".to_string(), config);
        let ink = Ink::new("/shelly/on", "test", "test_source", "body");
        assert_eq!(reflex.resolve_action(&ink), "on");
    }
}
