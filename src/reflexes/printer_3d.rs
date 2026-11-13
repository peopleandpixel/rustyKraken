use anyhow::{Context, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::{debug, error, info};

use super::Reflex;
use crate::model::Ink;

/// Configuration for 3D Printer Reflex
/// Controls 3D printers via OctoPrint API
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Printer3dReflexConfig {
    /// OctoPrint server URL (e.g., "http://localhost:5000")
    pub host: String,

    /// OctoPrint API key
    pub api_key: String,

    /// Default action: "print", "cancel", "pause", "resume", "status", "temperature", "sd"
    #[serde(default = "default_action")]
    pub action: String,

    /// Template for G-code file path
    #[serde(default)]
    pub gcode_template: Option<String>,

    /// Optional: mapping from Ink topics to actions
    #[serde(default)]
    pub topic_action_mapping: HashMap<String, String>,

    /// Optional: mapping from Ink topics to G-code files
    #[serde(default)]
    pub topic_gcode_mapping: HashMap<String, String>,

    /// Whether to verify TLS certificates
    #[serde(default = "default_verify_ssl")]
    pub verify_ssl: bool,

    /// Timeout in seconds
    #[serde(default = "default_timeout")]
    pub timeout: u64,
}

fn default_action() -> String {
    "status".to_string()
}
fn default_verify_ssl() -> bool {
    true
}
fn default_timeout() -> u64 {
    30
}

pub struct Printer3dReflex {
    name: String,
    config: Printer3dReflexConfig,
    client: reqwest::Client,
}

impl Printer3dReflex {
    pub fn new(name: String, config: Printer3dReflexConfig) -> Self {
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

    /// Resolve G-code file for a given Ink
    fn resolve_gcode(&self, ink: &Ink) -> Option<String> {
        if let Some(gcode) = self
            .config
            .topic_gcode_mapping
            .get(&ink.topic)
            .filter(|g| !g.is_empty())
        {
            return Some(gcode.clone());
        }
        if let Some(ref template) = self.config.gcode_template {
            return Some(self.render_template(template, ink));
        }
        None
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

    /// Build OctoPrint API URL
    fn octoprint_url(&self, path: &str) -> String {
        let host = self.config.host.trim_end_matches('/');
        format!("{}/api/{}", host, path)
    }

    /// Build auth request
    fn build_auth_request(&self, request: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        request.header("X-Api-Key", &self.config.api_key)
    }

    /// Get printer status
    async fn get_status(&self) -> Result<serde_json::Value> {
        let url = self.octoprint_url("printer");

        let response = self
            .build_auth_request(self.client.get(&url))
            .send()
            .await
            .context("Failed to get OctoPrint printer status")?;

        if !response.status().is_success() {
            let status = response.status();
            let error_body = response.text().await.unwrap_or_default();
            return Err(anyhow::anyhow!(
                "OctoPrint status error ({}): {}",
                status,
                error_body
            ));
        }

        response
            .json()
            .await
            .context("Failed to parse OctoPrint status response")
    }

    /// Get temperature
    async fn get_temperature(&self) -> Result<serde_json::Value> {
        let url = self.octoprint_url("printer/temperature");

        let response = self
            .build_auth_request(self.client.get(&url))
            .send()
            .await
            .context("Failed to get OctoPrint temperature")?;

        if !response.status().is_success() {
            let status = response.status();
            return Err(anyhow::anyhow!("OctoPrint temperature error: {}", status));
        }

        response
            .json()
            .await
            .context("Failed to parse OctoPrint temperature response")
    }

    /// Start printing a file
    async fn start_print(&self, file_path: &str) -> Result<()> {
        info!(
            "3D Printer reflex '{}' starting print: {}",
            self.name, file_path
        );

        let url = self.octoprint_url(&format!(
            "printer/select?command=start&file={}",
            urlencoding::encode(file_path)
        ));

        let response = self
            .build_auth_request(self.client.post(&url))
            .send()
            .await
            .with_context(|| "Failed to start print on OctoPrint".to_string())?;

        if !response.status().is_success() {
            let status = response.status();
            let error_body = response.text().await.unwrap_or_default();
            return Err(anyhow::anyhow!(
                "OctoPrint start print error ({}): {}",
                status,
                error_body
            ));
        }

        info!(
            "3D Printer reflex '{}' started printing: {}",
            self.name, file_path
        );
        Ok(())
    }

    /// Cancel print
    async fn cancel_print(&self) -> Result<()> {
        info!("3D Printer reflex '{}' canceling print", self.name);

        let url = self.octoprint_url("printer/command");
        let payload = serde_json::json!([{"command": "cancel"}]);

        let response = self
            .build_auth_request(self.client.post(&url).json(&payload))
            .send()
            .await
            .context("Failed to cancel print on OctoPrint")?;

        if !response.status().is_success() {
            let status = response.status();
            let error_body = response.text().await.unwrap_or_default();
            return Err(anyhow::anyhow!(
                "OctoPrint cancel error ({}): {}",
                status,
                error_body
            ));
        }

        info!("3D Printer reflex '{}' canceled print", self.name);
        Ok(())
    }

    /// Pause print
    async fn pause_print(&self) -> Result<()> {
        info!("3D Printer reflex '{}' pausing print", self.name);

        let url = self.octoprint_url("printer/pause");
        let response = self
            .build_auth_request(self.client.post(&url))
            .send()
            .await
            .context("Failed to pause print on OctoPrint")?;

        if !response.status().is_success() {
            let status = response.status();
            return Err(anyhow::anyhow!("OctoPrint pause error: {}", status));
        }

        info!("3D Printer reflex '{}' paused print", self.name);
        Ok(())
    }

    /// Resume print
    async fn resume_print(&self) -> Result<()> {
        info!("3D Printer reflex '{}' resuming print", self.name);

        let url = self.octoprint_url("printer/resume");
        let response = self
            .build_auth_request(self.client.post(&url))
            .send()
            .await
            .context("Failed to resume print on OctoPrint")?;

        if !response.status().is_success() {
            let status = response.status();
            return Err(anyhow::anyhow!("OctoPrint resume error: {}", status));
        }

        info!("3D Printer reflex '{}' resumed print", self.name);
        Ok(())
    }

    /// List SD card files
    async fn list_sd_files(&self) -> Result<Vec<String>> {
        let url = self.octoprint_url("files/local");

        let response = self
            .build_auth_request(self.client.get(&url))
            .send()
            .await
            .context("Failed to list OctoPrint SD files")?;

        if !response.status().is_success() {
            let status = response.status();
            return Err(anyhow::anyhow!("OctoPrint list files error: {}", status));
        }

        let result: serde_json::Value = response
            .json()
            .await
            .context("Failed to parse OctoPrint files response")?;

        let files = result["files"]
            .as_array()
            .map(|arr| {
                arr.iter()
                    .filter_map(|f| f["name"].as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default();

        Ok(files)
    }
}

#[async_trait]
impl Reflex for Printer3dReflex {
    fn name(&self) -> &str {
        &self.name
    }

    fn r#type(&self) -> &str {
        "printer_3d"
    }

    async fn handle(&self, ink: &Ink) -> Result<()> {
        info!(
            "3D Printer reflex '{}' handling ink on topic: {}",
            self.name, ink.topic
        );

        let action = self.resolve_action(ink);
        debug!(
            "3D Printer reflex '{}' resolved action: {}",
            self.name, action
        );

        match action.as_str() {
            "status" => {
                let status = self.get_status().await?;
                let state = status["state"].as_str().unwrap_or("unknown");
                info!("3D Printer reflex '{}' printer state: {}", self.name, state);
            }
            "temperature" | "temp" => {
                let temp = self.get_temperature().await?;
                debug!("3D Printer reflex '{}' temperature: {}", self.name, temp);
            }
            "print" | "start" => {
                let file = self
                    .resolve_gcode(ink)
                    .context("G-code file is required for print action")?;
                self.start_print(&file).await?;
            }
            "cancel" | "stop" => {
                self.cancel_print().await?;
            }
            "pause" => {
                self.pause_print().await?;
            }
            "resume" => {
                self.resume_print().await?;
            }
            "sd" | "files" | "list-files" => {
                let files = self.list_sd_files().await?;
                info!(
                    "3D Printer reflex '{}' found {} files on SD",
                    self.name,
                    files.len()
                );
            }
            _ => {
                error!(
                    "3D Printer reflex '{}' unknown action: {}",
                    self.name, action
                );
                return Err(anyhow::anyhow!("Unknown 3D printer action: {}", action));
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_config() -> Printer3dReflexConfig {
        Printer3dReflexConfig {
            host: "http://localhost:5000".to_string(),
            api_key: "my-api-key".to_string(),
            action: "status".to_string(),
            gcode_template: None,
            topic_action_mapping: HashMap::new(),
            topic_gcode_mapping: HashMap::new(),
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
        let reflex = Printer3dReflex::new("test".to_string(), config);
        let ink = create_test_ink();
        assert_eq!(
            reflex.render_template("{{topic}}.gcode", &ink),
            "/test/topic.gcode"
        );
    }

    #[test]
    fn test_config_deserialization() {
        let yaml = r#"
            host: "http://octoprint:5000"
            api_key: "secret"
            action: "print"
        "#;
        let config: Printer3dReflexConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.host, "http://octoprint:5000");
        assert_eq!(config.action, "print");
    }

    #[test]
    fn test_reflex_type() {
        let config = create_test_config();
        let reflex = Printer3dReflex::new("test".to_string(), config);
        assert_eq!(reflex.r#type(), "printer_3d");
    }

    #[test]
    fn test_resolve_action_from_mapping() {
        let mut config = create_test_config();
        let mut mapping = HashMap::new();
        mapping.insert("/3dprint/start".to_string(), "print".to_string());
        config.topic_action_mapping = mapping;

        let reflex = Printer3dReflex::new("test".to_string(), config);
        let ink = Ink::new("/3dprint/start", "test", "test_source", "body");
        assert_eq!(reflex.resolve_action(&ink), "print");
    }
}
