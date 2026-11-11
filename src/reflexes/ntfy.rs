use anyhow::{Context, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::{debug, error, info};

use super::Reflex;
use crate::model::Ink;

/// Configuration for ntfy Push Notification Reflex
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NtfyReflexConfig {
    /// ntfy broker URL (e.g., "https://ntfy.sh" or "http://localhost:8080")
    pub server: String,

    /// Default ntfy topic to publish to
    pub topic: String,

    /// Template for the notification title
    /// Supported placeholders: {{topic}}, {{body}}, {{id}}, {{timestamp}}, {{metadata.<key>}}
    #[serde(default = "default_title_template")]
    pub title_template: String,

    /// Template for the notification message body
    /// Supported placeholders: {{topic}}, {{body}}, {{id}}, {{timestamp}}, {{metadata.<key>}}
    #[serde(default = "default_message_template")]
    pub message_template: String,

    /// Priority: 1 (low), 3 (normal), 5 (high), 5 (urgent)
    #[serde(default = "default_priority")]
    pub priority: u8,

    /// Tags as emoji or tag names (e.g., ["fire", "warning"])
    #[serde(default)]
    pub tags: Vec<String>,

    /// Click action URL opened when tapping the notification
    #[serde(default)]
    pub click: Option<String>,

    /// Icon URL shown in the notification
    #[serde(default)]
    pub icon: Option<String>,

    /// Optional: mapping from Ink topics to ntfy topics
    #[serde(default)]
    pub topic_mapping: HashMap<String, String>,

    /// Optional: mapping from Ink topics to priorities
    #[serde(default)]
    pub topic_priority_mapping: HashMap<String, u8>,

    /// Optional: mapping from Ink topics to tags
    #[serde(default)]
    pub topic_tags_mapping: HashMap<String, Vec<String>>,

    /// Optional: username for authentication
    #[serde(default)]
    pub username: Option<String>,

    /// Optional: password for authentication
    #[serde(default)]
    pub password: Option<String>,

    /// Whether to enable markdown formatting
    #[serde(default)]
    pub markdown: bool,
}

fn default_title_template() -> String {
    "{{topic}}".to_string()
}

fn default_message_template() -> String {
    "{{body}}".to_string()
}

fn default_priority() -> u8 {
    3
}

pub struct NtfyReflex {
    name: String,
    config: NtfyReflexConfig,
    client: reqwest::Client,
}

impl NtfyReflex {
    pub fn new(name: String, config: NtfyReflexConfig) -> Self {
        Self {
            name,
            config,
            client: reqwest::Client::new(),
        }
    }

    /// Resolve ntfy topic for a given Ink, checking topic-based mapping first
    fn resolve_topic(&self, ink: &Ink) -> String {
        if let Some(topic) = self
            .config
            .topic_mapping
            .get(&ink.topic)
            .filter(|t| !t.is_empty())
        {
            return topic.clone();
        }

        self.config.topic.clone()
    }

    /// Resolve priority for a given Ink, checking topic-based mapping first
    fn resolve_priority(&self, ink: &Ink) -> u8 {
        if let Some(priority) = self.config.topic_priority_mapping.get(&ink.topic) {
            return *priority;
        }

        self.config.priority
    }

    /// Resolve tags for a given Ink, checking topic-based mapping first
    fn resolve_tags(&self, ink: &Ink) -> Vec<String> {
        if let Some(tags) = self.config.topic_tags_mapping.get(&ink.topic)
            && !tags.is_empty()
        {
            return tags.clone();
        }

        self.config.tags.clone()
    }

    /// Render a template string with Ink data
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

    /// Send a notification via ntfy broker
    async fn send_notification(
        &self,
        topic: &str,
        title: &str,
        message: &str,
        priority: u8,
        tags: &[String],
    ) -> Result<()> {
        let server = self.config.server.trim_end_matches('/');
        let url = format!("{}/{}", server, topic);

        let mut payload = serde_json::Map::new();

        if !title.is_empty() {
            payload.insert(
                "title".to_string(),
                serde_json::Value::String(title.to_string()),
            );
        }

        payload.insert(
            "message".to_string(),
            serde_json::Value::String(message.to_string()),
        );

        payload.insert(
            "priority".to_string(),
            serde_json::Value::Number(priority.into()),
        );

        if !tags.is_empty() {
            let tags_json: Vec<serde_json::Value> = tags
                .iter()
                .map(|t| serde_json::Value::String(t.clone()))
                .collect();
            payload.insert("tags".to_string(), serde_json::Value::Array(tags_json));
        }

        if let Some(ref click) = self.config.click {
            let click_rendered = self.render_template(click, &Ink::new(topic, "", "", ""));
            payload.insert(
                "click".to_string(),
                serde_json::Value::String(click_rendered),
            );
        }

        if let Some(ref icon) = self.config.icon {
            payload.insert("icon".to_string(), serde_json::Value::String(icon.clone()));
        }

        if self.config.markdown {
            payload.insert("markdown".to_string(), serde_json::Value::Bool(true));
        }

        let mut request = self
            .client
            .post(&url)
            .header("Content-Type", "application/json")
            .json(&payload);

        // Add authentication if configured
        if let Some(ref username) = self.config.username {
            let password = self.config.password.clone().unwrap_or_default();
            request = request.basic_auth(username, Some(password));
        }

        let response = request
            .send()
            .await
            .with_context(|| format!("Failed to send ntfy notification to {}", url))?;

        if !response.status().is_success() {
            let status = response.status();
            let error_body = response.text().await.unwrap_or_default();
            return Err(anyhow::anyhow!(
                "ntfy API error ({}): {}",
                status,
                error_body
            ));
        }

        Ok(())
    }
}

#[async_trait]
impl Reflex for NtfyReflex {
    fn name(&self) -> &str {
        &self.name
    }

    fn r#type(&self) -> &str {
        "ntfy"
    }

    async fn handle(&self, ink: &Ink) -> Result<()> {
        info!(
            "ntfy reflex '{}' handling ink on topic: {}",
            self.name, ink.topic
        );

        let ntfy_topic = self.resolve_topic(ink);
        let priority = self.resolve_priority(ink);
        let tags = self.resolve_tags(ink);

        let title = self.render_template(&self.config.title_template, ink);
        let message = self.render_template(&self.config.message_template, ink);

        debug!(
            "ntfy reflex '{}' sending to topic '{}' with priority {} for ink topic: {}",
            self.name, ntfy_topic, priority, ink.topic
        );

        match self
            .send_notification(&ntfy_topic, &title, &message, priority, &tags)
            .await
        {
            Ok(()) => {
                info!(
                    "ntfy reflex '{}' sent notification to '{}' for topic: {}",
                    self.name, ntfy_topic, ink.topic
                );
                Ok(())
            }
            Err(e) => {
                error!(
                    "ntfy reflex '{}' failed to send notification: {}",
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

    fn create_test_config() -> NtfyReflexConfig {
        NtfyReflexConfig {
            server: "https://ntfy.sh".to_string(),
            topic: "default-topic".to_string(),
            title_template: "{{topic}}".to_string(),
            message_template: "{{body}}".to_string(),
            priority: 3,
            tags: vec![],
            click: None,
            icon: None,
            topic_mapping: HashMap::new(),
            topic_priority_mapping: HashMap::new(),
            topic_tags_mapping: HashMap::new(),
            username: None,
            password: None,
            markdown: false,
        }
    }

    fn create_test_ink() -> Ink {
        Ink::new("/test/topic", "test", "test_source", "test body")
    }

    #[test]
    fn test_default_topic() {
        let config = create_test_config();
        let reflex = NtfyReflex::new("test".to_string(), config);

        let ink = create_test_ink();
        assert_eq!(reflex.resolve_topic(&ink), "default-topic");
    }

    #[test]
    fn test_topic_mapping() {
        let mut config = create_test_config();
        let mut mapping = HashMap::new();
        mapping.insert("/test/topic".to_string(), "alerts".to_string());
        config.topic_mapping = mapping;

        let reflex = NtfyReflex::new("test".to_string(), config);

        let ink = create_test_ink();
        assert_eq!(reflex.resolve_topic(&ink), "alerts");
    }

    #[test]
    fn test_default_priority() {
        let config = create_test_config();
        let reflex = NtfyReflex::new("test".to_string(), config);

        let ink = create_test_ink();
        assert_eq!(reflex.resolve_priority(&ink), 3);
    }

    #[test]
    fn test_topic_priority_mapping() {
        let mut config = create_test_config();
        let mut mapping = HashMap::new();
        mapping.insert("/test/topic".to_string(), 5);
        config.topic_priority_mapping = mapping;

        let reflex = NtfyReflex::new("test".to_string(), config);

        let ink = create_test_ink();
        assert_eq!(reflex.resolve_priority(&ink), 5);
    }

    #[test]
    fn test_default_tags() {
        let mut config = create_test_config();
        config.tags = vec!["fire".to_string(), "warning".to_string()];

        let reflex = NtfyReflex::new("test".to_string(), config);

        let ink = create_test_ink();
        let tags = reflex.resolve_tags(&ink);
        assert_eq!(tags.len(), 2);
        assert!(tags.contains(&"fire".to_string()));
    }

    #[test]
    fn test_topic_tags_mapping() {
        let mut config = create_test_config();
        config.tags = vec!["default".to_string()];
        let mut mapping = HashMap::new();
        mapping.insert(
            "/test/topic".to_string(),
            vec!["fire".to_string(), "urgent".to_string()],
        );
        config.topic_tags_mapping = mapping;

        let reflex = NtfyReflex::new("test".to_string(), config);

        let ink = create_test_ink();
        let tags = reflex.resolve_tags(&ink);
        assert_eq!(tags.len(), 2);
        assert!(tags.contains(&"fire".to_string()));
        assert!(tags.contains(&"urgent".to_string()));
    }

    #[test]
    fn test_title_template_rendering() {
        let mut config = create_test_config();
        let title_template = "[{{topic}}] Alert".to_string();
        config.title_template = title_template.clone();

        let reflex = NtfyReflex::new("test".to_string(), config);

        let ink = create_test_ink();
        assert_eq!(
            reflex.render_template(&title_template, &ink),
            "[/test/topic] Alert"
        );
    }

    #[test]
    fn test_message_template_rendering() {
        let mut config = create_test_config();
        let message_template = "Topic: {{topic}}, Body: {{body}}".to_string();
        config.message_template = message_template.clone();

        let reflex = NtfyReflex::new("test".to_string(), config);

        let ink = create_test_ink();
        assert_eq!(
            reflex.render_template(&message_template, &ink),
            "Topic: /test/topic, Body: test body"
        );
    }

    #[test]
    fn test_metadata_in_template() {
        let mut config = create_test_config();
        let message_template = "Temp: {{metadata.temp}}°C".to_string();
        config.message_template = message_template.clone();

        let reflex = NtfyReflex::new("test".to_string(), config);

        let ink = Ink::new("/test", "test", "test_source", "data")
            .with_metadata(serde_json::json!({ "temp": "42", "humidity": "60" }));
        assert_eq!(
            reflex.render_template(&message_template, &ink),
            "Temp: 42°C"
        );
    }

    #[test]
    fn test_timestamp_and_id_in_template() {
        let mut config = create_test_config();
        let title_template = "{{id}} at {{timestamp}}".to_string();
        config.title_template = title_template.clone();

        let reflex = NtfyReflex::new("test".to_string(), config);

        let ink = create_test_ink();
        let rendered = reflex.render_template(&title_template, &ink);
        assert!(rendered.contains(" at "));
        assert!(rendered.contains("+00:00")); // RFC3339 format
    }

    #[test]
    fn test_config_deserialization() {
        let yaml = r#"
            server: "http://localhost:8080"
            topic: "home-alerts"
            title_template: "[{{topic}}] {{body}}"
            message_template: "{{body}}"
            priority: 5
            tags: ["fire", "warning"]
            markdown: true
        "#;

        let config: NtfyReflexConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.server, "http://localhost:8080");
        assert_eq!(config.topic, "home-alerts");
        assert_eq!(config.priority, 5);
        assert_eq!(config.tags.len(), 2);
        assert!(config.markdown);
    }

    #[test]
    fn test_reflex_type() {
        let config = create_test_config();
        let reflex = NtfyReflex::new("test".to_string(), config);
        assert_eq!(reflex.r#type(), "ntfy");
    }
}
