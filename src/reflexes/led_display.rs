use anyhow::{Context, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::{debug, error, info};

use super::Reflex;
use crate::model::Ink;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LedDisplayReflexConfig {
    /// Display type: "http", "serial", "file"
    #[serde(default = "default_display_type")]
    pub display_type: String,
    /// URL or path for the display
    pub endpoint: String,
    /// Template for the display text
    #[serde(default = "default_text_template")]
    pub text_template: String,
    /// Color (hex, for LED displays)
    #[serde(default = "default_color")]
    pub color: String,
    /// Duration in seconds (0 = permanent)
    #[serde(default)]
    pub duration: u32,
}

fn default_display_type() -> String { "http".to_string() }
fn default_text_template() -> String { "{{body}}".to_string() }
fn default_color() -> String { "FF0000".to_string() }

pub struct LedDisplayReflex {
    name: String,
    config: LedDisplayReflexConfig,
    client: reqwest::Client,
}

impl LedDisplayReflex {
    pub fn new(name: String, config: LedDisplayReflexConfig) -> Self {
        Self { name, config, client: reqwest::Client::new() }
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
impl Reflex for LedDisplayReflex {
    fn name(&self) -> &str { &self.name }
    fn r#type(&self) -> &str { "led_display" }

    async fn handle(&self, ink: &Ink) -> Result<()> {
        info!("LED/Display reflex '{}' handling ink on topic: {}", self.name, ink.topic);
        let text = self.render_template(&self.config.text_template, ink);
        debug!("LED/Display reflex '{}' updating display with: {}", self.name, text);

        match self.config.display_type.as_str() {
            "http" => {
                let payload = serde_json::json!({ "text": text, "color": self.config.color, "duration": self.config.duration });
                let response = self.client.post(&self.config.endpoint).json(&payload).send().await
                    .with_context(|| format!("Failed to send to display at {}", self.config.endpoint))?;
                if !response.status().is_success() {
                    return Err(anyhow::anyhow!("Display API error: {}", response.status()));
                }
            }
            "file" => {
                std::fs::write(&self.config.endpoint, &text)
                    .with_context(|| format!("Failed to write to display file {}", self.config.endpoint))?;
            }
            _ => {
                info!("LED/Display reflex '{}' using unsupported type: {}", self.name, self.config.display_type);
            }
        }

        info!("LED/Display reflex '{}' updated display for topic: {}", self.name, ink.topic);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_config() -> LedDisplayReflexConfig {
        LedDisplayReflexConfig {
            display_type: "http".to_string(),
            endpoint: "http://localhost:8080/display".to_string(),
            text_template: "{{body}}".to_string(),
            color: "FF0000".to_string(),
            duration: 0,
        }
    }

    #[test]
    fn test_template_rendering() {
        let config = create_test_config();
        let reflex = LedDisplayReflex::new("test".to_string(), config);
        let ink = Ink::new("/test/topic", "test", "test_source", "test body");
        assert_eq!(reflex.render_template("[{{topic}}] {{body}}", &ink), "[/test/topic] test body");
    }

    #[test]
    fn test_config_deserialization() {
        let yaml = r#"
            display_type: "file"
            endpoint: "/tmp/display.txt"
            color: "00FF00"
        "#;
        let config: LedDisplayReflexConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.display_type, "file");
    }

    #[test]
    fn test_reflex_type() {
        let config = create_test_config();
        let reflex = LedDisplayReflex::new("test".to_string(), config);
        assert_eq!(reflex.r#type(), "led_display");
    }
}
