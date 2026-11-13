use anyhow::{Context, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, info};

use super::Reflex;
use crate::appcontext::AppContext;
use crate::model::Ink;

/// Configuration for IRC Reflex (HTTP webhook mode)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IrcReflexConfig {
    /// IRC gateway webhook URL (e.g., Shlink or similar)
    pub webhook_url: String,
    /// Default channel(s) to send to
    pub channels: Vec<String>,
    /// Template for the message
    #[serde(default = "default_message_template")]
    pub message_template: String,
    /// Optional: mapping from Ink topics to channels
    #[serde(default)]
    pub topic_channel_mapping: HashMap<String, Vec<String>>,
    /// Nickname to use
    #[serde(default = "default_nickname")]
    pub nickname: String,
}

fn default_message_template() -> String {
    "[{{topic}}] {{body}}".to_string()
}
fn default_nickname() -> String {
    "rustyKraken".to_string()
}

pub struct IrcReflex {
    name: String,
    config: IrcReflexConfig,
    client: Arc<reqwest::Client>,
}

impl IrcReflex {
    pub fn new(name: String, config: IrcReflexConfig, app_ctx: &AppContext) -> Self {
        Self {
            name,
            config,
            client: Arc::clone(app_ctx.http_client()),
        }
    }

    fn resolve_channels(&self, ink: &Ink) -> Vec<String> {
        if let Some(channels) = self
            .config
            .topic_channel_mapping
            .get(&ink.topic)
            .filter(|c| !c.is_empty())
        {
            return channels.clone();
        }
        self.config.channels.clone()
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
impl Reflex for IrcReflex {
    fn name(&self) -> &str {
        &self.name
    }
    fn r#type(&self) -> &str {
        "irc"
    }

    async fn handle(&self, ink: &Ink) -> Result<()> {
        info!(
            "IRC reflex '{}' handling ink on topic: {}",
            self.name, ink.topic
        );
        let channels = self.resolve_channels(ink);
        let message = self.render_template(&self.config.message_template, ink);
        debug!(
            "IRC reflex '{}' sending to {:?} for topic: {}",
            self.name, channels, ink.topic
        );

        let payload = serde_json::json!({
            "channel": channels[0],
            "message": message,
            "nick": self.config.nickname
        });

        let response = self
            .client
            .post(&self.config.webhook_url)
            .json(&payload)
            .send()
            .await
            .with_context(|| "Failed to send IRC message via webhook".to_string())?;

        if !response.status().is_success() {
            let status = response.status();
            let error_body = response.text().await.unwrap_or_default();
            return Err(anyhow::anyhow!(
                "IRC webhook error ({}): {}",
                status,
                error_body
            ));
        }
        info!(
            "IRC reflex '{}' sent message for topic: {}",
            self.name, ink.topic
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::appcontext::AppContext;

    fn create_test_config() -> IrcReflexConfig {
        IrcReflexConfig {
            webhook_url: "http://localhost:8080/irc".to_string(),
            channels: vec!["general".to_string()],
            message_template: "[{{topic}}] {{body}}".to_string(),
            topic_channel_mapping: HashMap::new(),
            nickname: "rustyKraken".to_string(),
        }
    }

    fn create_test_ink() -> Ink {
        Ink::new("/test/topic", "test", "test_source", "test body")
    }

    #[test]
    fn test_default_channels() {
        let config = create_test_config();
        let reflex = IrcReflex::new("test".to_string(), config, &AppContext::new());
        let ink = create_test_ink();
        assert_eq!(reflex.resolve_channels(&ink), vec!["general"]);
    }

    #[test]
    fn test_template_rendering() {
        let config = create_test_config();
        let reflex = IrcReflex::new("test".to_string(), config, &AppContext::new());
        let ink = create_test_ink();
        assert_eq!(
            reflex.render_template("[{{topic}}] {{body}}", &ink),
            "[/test/topic] test body"
        );
    }

    #[test]
    fn test_config_deserialization() {
        let yaml = r#"
            webhook_url: "http://localhost:8080/irc"
            channels: ["general", "alerts"]
            nickname: "bot"
        "#;
        let config: IrcReflexConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.channels.len(), 2);
    }

    #[test]
    fn test_reflex_type() {
        let config = create_test_config();
        let reflex = IrcReflex::new("test".to_string(), config, &AppContext::new());
        assert_eq!(reflex.r#type(), "irc");
    }
}
