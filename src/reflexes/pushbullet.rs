use anyhow::{Context, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, error, info};

use super::Reflex;
use crate::appcontext::AppContext;
use crate::model::Ink;

/// Configuration for Pushbullet Reflex
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PushbulletReflexConfig {
    /// Pushbullet API access token
    pub api_token: String,

    /// Default device nickname to push to (optional)
    #[serde(default)]
    pub device_nickname: Option<String>,

    /// Template for the push title
    #[serde(default = "default_title_template")]
    pub title_template: String,

    /// Template for the push body
    #[serde(default = "default_body_template")]
    pub body_template: String,

    /// Push type: "note" or "link"
    #[serde(default = "default_push_type")]
    pub push_type: String,

    /// Optional: mapping from Ink topics to device nicknames
    #[serde(default)]
    pub topic_device_mapping: HashMap<String, String>,
}

fn default_title_template() -> String {
    "{{topic}}".to_string()
}

fn default_body_template() -> String {
    "{{body}}".to_string()
}

fn default_push_type() -> String {
    "note".to_string()
}

pub struct PushbulletReflex {
    name: String,
    config: PushbulletReflexConfig,
    client: Arc<reqwest::Client>,
}

impl PushbulletReflex {
    pub fn new(name: String, config: PushbulletReflexConfig, app_ctx: &AppContext) -> Self {
        Self {
            name,
            config,
            client: Arc::clone(app_ctx.http_client()),
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

    async fn send_push(&self, title: &str, body: &str) -> Result<()> {
        let payload = serde_json::json!({
            "type": "note",
            "title": title,
            "body": body,
        });

        let response = self
            .client
            .post("https://api.pushbullet.com/v2/pushes")
            .header("Authorization", format!("Bearer {}", self.config.api_token))
            .header("Content-Type", "application/json")
            .json(&payload)
            .send()
            .await
            .context("Failed to send Pushbullet push")?;

        if !response.status().is_success() {
            let status = response.status();
            let error_body = response.text().await.unwrap_or_default();
            return Err(anyhow::anyhow!(
                "Pushbullet API error ({}): {}",
                status,
                error_body
            ));
        }
        Ok(())
    }
}

#[async_trait]
impl Reflex for PushbulletReflex {
    fn name(&self) -> &str {
        &self.name
    }

    fn r#type(&self) -> &str {
        "pushbullet"
    }

    async fn handle(&self, ink: &Ink) -> Result<()> {
        info!(
            "Pushbullet reflex '{}' handling ink on topic: {}",
            self.name, ink.topic
        );
        let title = self.render_template(&self.config.title_template, ink);
        let body = self.render_template(&self.config.body_template, ink);
        debug!(
            "Pushbullet reflex '{}' sending push for topic: {}",
            self.name, ink.topic
        );

        match self.send_push(&title, &body).await {
            Ok(()) => {
                info!(
                    "Pushbullet reflex '{}' sent push for topic: {}",
                    self.name, ink.topic
                );
                Ok(())
            }
            Err(e) => {
                error!("Pushbullet reflex '{}' failed: {}", self.name, e);
                Err(e)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::appcontext::AppContext;

    fn create_test_config() -> PushbulletReflexConfig {
        PushbulletReflexConfig {
            api_token: "test_token".to_string(),
            device_nickname: None,
            title_template: "{{topic}}".to_string(),
            body_template: "{{body}}".to_string(),
            push_type: "note".to_string(),
            topic_device_mapping: HashMap::new(),
        }
    }

    fn create_test_ink() -> Ink {
        Ink::new("/test/topic", "test", "test_source", "test body")
    }

    #[test]
    fn test_template_rendering() {
        let config = create_test_config();
        let reflex = PushbulletReflex::new("test".to_string(), config, &AppContext::new());
        let ink = create_test_ink();
        assert_eq!(
            reflex.render_template("[{{topic}}] {{body}}", &ink),
            "[/test/topic] test body"
        );
    }

    #[test]
    fn test_config_deserialization() {
        let yaml = r#"
            api_token: "token123"
            title_template: "[{{topic}}] {{body}}"
            body_template: "{{body}}"
        "#;
        let config: PushbulletReflexConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.api_token, "token123");
    }

    #[test]
    fn test_reflex_type() {
        let config = create_test_config();
        let reflex = PushbulletReflex::new("test".to_string(), config, &AppContext::new());
        assert_eq!(reflex.r#type(), "pushbullet");
    }
}
