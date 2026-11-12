use anyhow::{Context, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::{debug, error, info};

use super::Reflex;
use crate::model::Ink;

/// Configuration for Microsoft Teams Reflex
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TeamsReflexConfig {
    /// Webhook URL for Teams incoming webhook
    pub webhook_url: String,

    /// Template for the message title
    #[serde(default = "default_title_template")]
    pub title_template: String,

    /// Template for the message text
    #[serde(default = "default_text_template")]
    pub text_template: String,

    /// Theme color (hex, e.g., "0078D4")
    #[serde(default = "default_theme_color")]
    pub theme_color: String,

    /// Optional: mapping from Ink topics to webhook URLs
    #[serde(default)]
    pub topic_webhook_mapping: HashMap<String, String>,

    /// Optional: mapping from Ink topics to theme colors
    #[serde(default)]
    pub topic_color_mapping: HashMap<String, String>,
}

fn default_title_template() -> String {
    "{{topic}}".to_string()
}

fn default_text_template() -> String {
    "{{body}}".to_string()
}

fn default_theme_color() -> String {
    "0078D4".to_string()
}

pub struct TeamsReflex {
    name: String,
    config: TeamsReflexConfig,
    client: reqwest::Client,
}

impl TeamsReflex {
    pub fn new(name: String, config: TeamsReflexConfig) -> Self {
        Self {
            name,
            config,
            client: reqwest::Client::new(),
        }
    }

    fn resolve_webhook_url(&self, ink: &Ink) -> String {
        if let Some(url) = self
            .config
            .topic_webhook_mapping
            .get(&ink.topic)
            .filter(|u| !u.is_empty())
        {
            return url.clone();
        }
        self.config.webhook_url.clone()
    }

    fn resolve_theme_color(&self, ink: &Ink) -> String {
        if let Some(color) = self.config.topic_color_mapping.get(&ink.topic) {
            return color.clone();
        }
        self.config.theme_color.clone()
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

    async fn send_message(&self, webhook_url: &str, title: &str, text: &str, theme_color: &str) -> Result<()> {
        let payload = serde_json::json!({
            "@type": "MessageCard",
            "@context": "https://schema.org/extensions",
            "themeColor": theme_color,
            "summary": title,
            "sections": [{
                "activityTitle": title,
                "text": text
            }]
        });

        let response = self
            .client
            .post(webhook_url)
            .json(&payload)
            .send()
            .await
            .with_context(|| format!("Failed to send Teams message to {}", webhook_url))?;

        if !response.status().is_success() {
            let status = response.status();
            let error_body = response.text().await.unwrap_or_default();
            return Err(anyhow::anyhow!(
                "Teams API error ({}): {}",
                status,
                error_body
            ));
        }

        Ok(())
    }
}

#[async_trait]
impl Reflex for TeamsReflex {
    fn name(&self) -> &str {
        &self.name
    }

    fn r#type(&self) -> &str {
        "teams"
    }

    async fn handle(&self, ink: &Ink) -> Result<()> {
        info!(
            "Teams reflex '{}' handling ink on topic: {}",
            self.name, ink.topic
        );

        let webhook_url = self.resolve_webhook_url(ink);
        let theme_color = self.resolve_theme_color(ink);
        let title = self.render_template(&self.config.title_template, ink);
        let text = self.render_template(&self.config.text_template, ink);

        debug!(
            "Teams reflex '{}' sending to webhook for topic: {}",
            self.name, ink.topic
        );

        match self.send_message(&webhook_url, &title, &text, &theme_color).await {
            Ok(()) => {
                info!(
                    "Teams reflex '{}' sent message for topic: {}",
                    self.name, ink.topic
                );
                Ok(())
            }
            Err(e) => {
                error!(
                    "Teams reflex '{}' failed to send message: {}",
                    self.name, e
                );
                Err(e)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_config() -> TeamsReflexConfig {
        TeamsReflexConfig {
            webhook_url: "https://outlook.office.com/webhook/test".to_string(),
            title_template: "{{topic}}".to_string(),
            text_template: "{{body}}".to_string(),
            theme_color: "0078D4".to_string(),
            topic_webhook_mapping: HashMap::new(),
            topic_color_mapping: HashMap::new(),
        }
    }

    fn create_test_ink() -> Ink {
        Ink::new("/test/topic", "test", "test_source", "test body")
    }

    #[test]
    fn test_default_webhook() {
        let config = create_test_config();
        let reflex = TeamsReflex::new("test".to_string(), config);
        let ink = create_test_ink();
        assert_eq!(
            reflex.resolve_webhook_url(&ink),
            "https://outlook.office.com/webhook/test"
        );
    }

    #[test]
    fn test_topic_webhook_mapping() {
        let mut config = create_test_config();
        let mut mapping = HashMap::new();
        mapping.insert(
            "/test/topic".to_string(),
            "https://outlook.office.com/webhook/other".to_string(),
        );
        config.topic_webhook_mapping = mapping;
        let reflex = TeamsReflex::new("test".to_string(), config);
        let ink = create_test_ink();
        assert_eq!(
            reflex.resolve_webhook_url(&ink),
            "https://outlook.office.com/webhook/other"
        );
    }

    #[test]
    fn test_template_rendering() {
        let config = create_test_config();
        let reflex = TeamsReflex::new("test".to_string(), config);
        let ink = create_test_ink();
        assert_eq!(
            reflex.render_template("[{{topic}}] Alert", &ink),
            "[/test/topic] Alert"
        );
    }

    #[test]
    fn test_metadata_in_template() {
        let config = create_test_config();
        let reflex = TeamsReflex::new("test".to_string(), config);
        let ink = Ink::new("/test", "test", "test_source", "data")
            .with_metadata(serde_json::json!({ "temp": "42" }));
        assert_eq!(
            reflex.render_template("Temp: {{metadata.temp}}°C", &ink),
            "Temp: 42°C"
        );
    }

    #[test]
    fn test_config_deserialization() {
        let yaml = r#"
            webhook_url: "https://outlook.office.com/webhook/test"
            title_template: "[{{topic}}] {{body}}"
            text_template: "{{body}}"
            theme_color: "FF0000"
        "#;
        let config: TeamsReflexConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.theme_color, "FF0000");
    }

    #[test]
    fn test_reflex_type() {
        let config = create_test_config();
        let reflex = TeamsReflex::new("test".to_string(), config);
        assert_eq!(reflex.r#type(), "teams");
    }
}
