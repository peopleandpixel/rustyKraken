use anyhow::{Context, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::{debug, error, info};

use super::Reflex;
use crate::model::Ink;

/// Configuration for Make.com Reflex
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MakeReflexConfig {
    /// API endpoint https://hook.make.com
    pub api_url: String,

    /// API key or token (optional)
    #[serde(default)]
    pub api_key: Option<String>,

    /// Template for the message body
    #[serde(default = "default_message_template")]
    pub message_template: String,

    /// Template for the title/subject
    #[serde(default = "default_title_template")]
    pub title_template: String,

    /// Optional: mapping from Ink topics to API https://hook.make.coms
    #[serde(default)]
    pub topic_url_mapping: HashMap<String, String>,

    /// HTTP method
    #[serde(default = "default_method")]
    pub method: String,

    /// Custom headers
    #[serde(default)]
    pub headers: HashMap<String, String>,
}

fn default_message_template() -> String { "{{body}}".to_string() }
fn default_title_template() -> String { "{{topic}}".to_string() }
fn default_method() -> String { "POST".to_string() }

pub struct MakeReflex {
    name: String,
    config: MakeReflexConfig,
    client: reqwest::Client,
}

impl MakeReflex {
    pub fn new(name: String, config: MakeReflexConfig) -> Self {
        Self { name, config, client: reqwest::Client::new() }
    }

    fn resolve_url(&self, ink: &Ink) -> String {
        if let Some(url) = self.config.topic_url_mapping.get(&ink.topic).filter(|u| !u.is_empty()) {
            return url.clone();
        }
        self.config.api_url.clone()
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
impl Reflex for MakeReflex {
    fn name(&self) -> &str { &self.name }
    fn r#type(&self) -> &str { "make" }

    async fn handle(&self, ink: &Ink) -> Result<()> {
        info!("Make.com reflex '{}' handling ink on topic: {}", self.name, ink.topic);
        let url = self.resolve_url(ink);
        let title = self.render_template(&self.config.title_template, ink);
        let body = self.render_template(&self.config.message_template, ink);
        debug!("Make.com reflex '{}' sending to {} for topic: {}", self.name, url, ink.topic);

        let payload = serde_json::json!({"title": title, "message": body});
        let mut request = self.client.post(&url).json(&payload);
        if let Some(ref key) = self.config.api_key {
            request = request.header("Authorization", format!("Bearer {}", key));
        }
        for (k, v) in &self.config.headers {
            request = request.header(k, v);
        }
        let response = request.send().await.with_context(|| format!("Failed to send Make.com request"))?;
        if !response.status().is_success() {
            let status = response.status();
            let error_body = response.text().await.unwrap_or_default();
            return Err(anyhow::anyhow!("Make.com API error ({}): {}", status, error_body));
        }
        info!("Make.com reflex '{}' sent message for topic: {}", self.name, ink.topic);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_config() -> MakeReflexConfig {
        MakeReflexConfig {
            api_url: "https://hook.make.com".to_string(),
            api_key: None,
            message_template: "{{body}}".to_string(),
            title_template: "{{topic}}".to_string(),
            topic_url_mapping: HashMap::new(),
            method: "POST".to_string(),
            headers: HashMap::new(),
        }
    }

    fn create_test_ink() -> Ink {
        Ink::new("/test/topic", "test", "test_source", "test body")
    }

    #[test]
    fn test_template_rendering() {
        let config = create_test_config();
        let reflex = MakeReflex::new("test".to_string(), config);
        let ink = create_test_ink();
        assert_eq!(reflex.render_template("[{{topic}}] {{body}}", &ink), "[/test/topic] test body");
    }

    #[test]
    fn test_config_deserialization() {
        let yaml = r#"
            api_url: "https://hook.make.com"
            message_template: "[{{topic}}] {{body}}"
        "#;
        let config: MakeReflexConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.api_url, "https://hook.make.com");
    }

    #[test]
    fn test_reflex_type() {
        let config = create_test_config();
        let reflex = MakeReflex::new("test".to_string(), config);
        assert_eq!(reflex.r#type(), "make");
    }
}
