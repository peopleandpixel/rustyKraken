use anyhow::{Context, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::{debug, info};

use super::Reflex;
use crate::model::Ink;

/// Configuration for Bark Reflex
/// Sends push notifications to iOS devices via Bark server
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BarkReflexConfig {
    /// Bark server URL (e.g., "https://api.day.app" or self-hosted)
    pub server: String,

    /// Device key (from Bark app)
    pub device_key: String,

    /// Template for the title
    #[serde(default = "default_title_template")]
    pub title_template: String,

    /// Template for the body/message
    #[serde(default = "default_body_template")]
    pub body_template: String,

    /// Priority: 5 (emergency), 10 (high), 25 (default), 50 (low)
    #[serde(default = "default_priority")]
    pub priority: u32,

    /// Optional: mapping from Ink topics to device keys
    #[serde(default)]
    pub topic_device_mapping: HashMap<String, String>,

    /// Optional: mapping from Ink topics to priorities
    #[serde(default)]
    pub topic_priority_mapping: HashMap<String, u32>,

    /// Sound name (e.g., "minuet")
    #[serde(default)]
    pub sound: Option<String>,

    /// Icon URL
    #[serde(default)]
    pub icon: Option<String>,

    /// Group ID for grouping notifications
    #[serde(default)]
    pub group: Option<String>,

    /// Is it a push notification (isPush: 1)
    #[serde(default)]
    pub is_push: bool,

    /// Copy URL to clipboard
    #[serde(default)]
    pub url: Option<String>,
}

fn default_title_template() -> String {
    "{{topic}}".to_string()
}
fn default_body_template() -> String {
    "{{body}}".to_string()
}
fn default_priority() -> u32 {
    25
}

pub struct BarkReflex {
    name: String,
    config: BarkReflexConfig,
    client: reqwest::Client,
}

impl BarkReflex {
    pub fn new(name: String, config: BarkReflexConfig) -> Self {
        Self {
            name,
            config,
            client: reqwest::Client::new(),
        }
    }

    /// Resolve device key for a given Ink
    fn resolve_device_key(&self, ink: &Ink) -> String {
        if let Some(key) = self
            .config
            .topic_device_mapping
            .get(&ink.topic)
            .filter(|k| !k.is_empty())
        {
            return key.clone();
        }
        self.config.device_key.clone()
    }

    /// Resolve priority for a given Ink
    fn resolve_priority(&self, ink: &Ink) -> u32 {
        if let Some(priority) = self.config.topic_priority_mapping.get(&ink.topic) {
            return *priority;
        }
        self.config.priority
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
impl Reflex for BarkReflex {
    fn name(&self) -> &str {
        &self.name
    }
    fn r#type(&self) -> &str {
        "bark"
    }

    async fn handle(&self, ink: &Ink) -> Result<()> {
        info!(
            "Bark reflex '{}' handling ink on topic: {}",
            self.name, ink.topic
        );

        let device_key = self.resolve_device_key(ink);
        let priority = self.resolve_priority(ink);
        let title = self.render_template(&self.config.title_template, ink);
        let body = self.render_template(&self.config.body_template, ink);

        debug!(
            "Bark reflex '{}' sending to device {} with priority {}",
            self.name, device_key, priority
        );

        // Bark API: POST /push/:deviceKey
        let url = format!(
            "{}/push/{}",
            self.config.server.trim_end_matches('/'),
            device_key
        );

        // Bark API: POST /push/:deviceKey with form-encoded body
        let mut form_parts = vec![
            format!("title={}", urlencoding::encode(&title)),
            format!("body={}", urlencoding::encode(&body)),
            format!("device_key={}", urlencoding::encode(&device_key)),
            format!("priority={}", priority),
        ];

        if let Some(ref sound) = self.config.sound {
            form_parts.push(format!("sound={}", urlencoding::encode(sound)));
        }
        if let Some(ref icon) = self.config.icon {
            form_parts.push(format!("icon={}", urlencoding::encode(icon)));
        }
        if let Some(ref group) = self.config.group {
            form_parts.push(format!("group={}", urlencoding::encode(group)));
        }
        if self.config.is_push {
            form_parts.push("isPush=1".to_string());
        }
        if let Some(ref u) = self.config.url {
            form_parts.push(format!("url={}", urlencoding::encode(u)));
        }

        let form_body = form_parts.join("&");
        let request = self
            .client
            .post(&url)
            .header("Content-Type", "application/x-www-form-urlencoded")
            .body(form_body);

        let response = request
            .send()
            .await
            .with_context(|| format!("Failed to send Bark request to {}", url))?;

        if !response.status().is_success() {
            let status = response.status();
            let error_body = response.text().await.unwrap_or_default();
            return Err(anyhow::anyhow!(
                "Bark API error ({}): {}",
                status,
                error_body
            ));
        }

        info!(
            "Bark reflex '{}' sent notification to device: {}",
            self.name, device_key
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_config() -> BarkReflexConfig {
        BarkReflexConfig {
            server: "https://api.day.app".to_string(),
            device_key: "test-device-key".to_string(),
            title_template: "{{topic}}".to_string(),
            body_template: "{{body}}".to_string(),
            priority: 25,
            topic_device_mapping: HashMap::new(),
            topic_priority_mapping: HashMap::new(),
            sound: None,
            icon: None,
            group: None,
            is_push: false,
            url: None,
        }
    }

    fn create_test_ink() -> Ink {
        Ink::new("/test/topic", "test", "test_source", "test body")
    }

    #[test]
    fn test_template_rendering() {
        let config = create_test_config();
        let reflex = BarkReflex::new("test".to_string(), config);
        let ink = create_test_ink();
        assert_eq!(
            reflex.render_template("[{{topic}}] {{body}}", &ink),
            "[/test/topic] test body"
        );
    }

    #[test]
    fn test_resolve_device_key() {
        let config = create_test_config();
        let reflex = BarkReflex::new("test".to_string(), config);
        let ink = create_test_ink();
        assert_eq!(reflex.resolve_device_key(&ink), "test-device-key");
    }

    #[test]
    fn test_resolve_priority() {
        let mut config = create_test_config();
        config
            .topic_priority_mapping
            .insert("/test/topic".to_string(), 10);
        let reflex = BarkReflex::new("test".to_string(), config);
        let ink = create_test_ink();
        assert_eq!(reflex.resolve_priority(&ink), 10);
    }

    #[test]
    fn test_config_deserialization() {
        let yaml = r#"
            server: "https://bark.example.com"
            device_key: "my-key"
            priority: 10
            sound: "minuet"
        "#;
        let config: BarkReflexConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.server, "https://bark.example.com");
        assert_eq!(config.priority, 10);
    }

    #[test]
    fn test_reflex_type() {
        let config = create_test_config();
        let reflex = BarkReflex::new("test".to_string(), config);
        assert_eq!(reflex.r#type(), "bark");
    }
}
