use anyhow::{Context, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, error, info};

use super::Reflex;
use crate::appcontext::AppContext;
use crate::model::Ink;

/// Configuration for Discord Webhook Reflex
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscordReflexConfig {
    /// Discord Webhook URL
    pub webhook_url: String,

    /// Template for the message content with placeholders
    /// Supported placeholders: {{topic}}, {{body}}, {{id}}, {{timestamp}}, {{metadata.<key>}}
    #[serde(default = "default_content_template")]
    pub content_template: String,

    /// Whether to use embed messages (rich formatting)
    #[serde(default = "default_use_embeds")]
    pub use_embeds: bool,

    /// Embed title template
    #[serde(default)]
    pub embed_title: Option<String>,

    /// Embed description template (if not set, uses content_template)
    #[serde(default)]
    pub embed_description: Option<String>,

    /// Embed color in decimal (e.g., 16711680 for red, 65280 for green)
    #[serde(default)]
    pub embed_color: Option<u32>,

    /// Embed footer text
    #[serde(default)]
    pub embed_footer: Option<String>,

    /// Optional: mapping from topics to webhook URLs
    #[serde(default)]
    pub topic_webhook_mapping: HashMap<String, String>,

    /// Optional: mapping from topics to embed colors
    #[serde(default)]
    pub topic_color_mapping: HashMap<String, u32>,

    /// Custom username for the webhook
    #[serde(default)]
    pub username: Option<String>,

    /// Custom avatar URL for the webhook
    #[serde(default)]
    pub avatar_url: Option<String>,

    /// Whether to include Ink metadata as embed fields
    #[serde(default)]
    pub include_metadata: bool,

    /// Maximum embed fields (Discord limit is 25)
    #[serde(default = "default_max_fields")]
    pub max_embed_fields: usize,

    /// Whether to send as ephemeral (only for bot mode, ignored for webhooks)
    #[serde(default)]
    pub tts: bool,
}

fn default_content_template() -> String {
    "{{body}}".to_string()
}

fn default_use_embeds() -> bool {
    true
}

fn default_max_fields() -> usize {
    25
}

pub struct DiscordReflex {
    name: String,
    config: DiscordReflexConfig,
    client: Arc<reqwest::Client>,
}

impl DiscordReflex {
    pub fn new(name: String, config: DiscordReflexConfig, app_ctx: &AppContext) -> Self {
        Self {
            name,
            config,
            client: Arc::clone(app_ctx.http_client()),
        }
    }

    /// Resolve webhook URL for a given Ink, checking topic-based mapping first
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

    /// Resolve embed color for a given Ink, checking topic-based mapping first
    fn resolve_color(&self, ink: &Ink) -> Option<u32> {
        if let Some(color) = self.config.topic_color_mapping.get(&ink.topic) {
            return Some(*color);
        }

        self.config.embed_color
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

    /// Build the Discord webhook payload
    fn build_payload(&self, ink: &Ink) -> serde_json::Value {
        let content = self.render_template(&self.config.content_template, ink);

        let mut payload = serde_json::Map::new();

        // Add content (plain text, max 2000 chars for Discord)
        let truncated_content = if content.len() > 2000 {
            &content[..2000]
        } else {
            &content
        };
        payload.insert(
            "content".to_string(),
            serde_json::Value::String(truncated_content.to_string()),
        );

        // Add username if configured
        if let Some(ref username) = self.config.username {
            payload.insert(
                "username".to_string(),
                serde_json::Value::String(username.clone()),
            );
        }

        // Add avatar URL if configured
        if let Some(ref avatar_url) = self.config.avatar_url {
            payload.insert(
                "avatar_url".to_string(),
                serde_json::Value::String(avatar_url.clone()),
            );
        }

        // Add TTS flag
        if self.config.tts {
            payload.insert("tts".to_string(), serde_json::Value::Bool(true));
        }

        // Build embed if enabled
        if self.config.use_embeds {
            let mut embed = serde_json::Map::new();

            // Embed title
            if let Some(ref title_template) = self.config.embed_title {
                let title = self.render_template(title_template, ink);
                let truncated_title = if title.len() > 256 {
                    &title[..256]
                } else {
                    &title
                };
                embed.insert(
                    "title".to_string(),
                    serde_json::Value::String(truncated_title.to_string()),
                );
            }

            // Embed description
            let desc_template = self
                .config
                .embed_description
                .as_deref()
                .unwrap_or(&self.config.content_template);
            let description = self.render_template(desc_template, ink);
            let truncated_desc = if description.len() > 4096 {
                &description[..4096]
            } else {
                &description
            };
            embed.insert(
                "description".to_string(),
                serde_json::Value::String(truncated_desc.to_string()),
            );

            // Embed color
            if let Some(color) = self.resolve_color(ink) {
                embed.insert("color".to_string(), serde_json::Value::Number(color.into()));
            }

            // Embed fields from metadata
            if self.config.include_metadata
                && let Some(obj) = ink.metadata.as_ref().and_then(|m| m.as_object())
            {
                let mut fields_arr: Vec<serde_json::Value> = Vec::new();
                let mut count = 0;
                for (key, value) in obj {
                    if count >= self.config.max_embed_fields {
                        break;
                    }
                    let mut field = serde_json::Map::new();
                    field.insert("name".to_string(), serde_json::Value::String(key.clone()));
                    let value_str = match value {
                        serde_json::Value::String(s) => s.clone(),
                        _ => value.to_string(),
                    };
                    // Discord field value limit is 1024 chars
                    let truncated_value = if value_str.len() > 1024 {
                        &value_str[..1024]
                    } else {
                        &value_str
                    };
                    field.insert(
                        "value".to_string(),
                        serde_json::Value::String(truncated_value.to_string()),
                    );
                    field.insert("inline".to_string(), serde_json::Value::Bool(false));
                    fields_arr.push(serde_json::Value::Object(field));
                    count += 1;
                }
                if count > 0 {
                    embed.insert("fields".to_string(), serde_json::Value::Array(fields_arr));
                }
            }

            // Embed footer
            if let Some(ref footer) = self.config.embed_footer {
                let rendered_footer = self.render_template(footer, ink);
                let mut footer_obj = serde_json::Map::new();
                footer_obj.insert(
                    "text".to_string(),
                    serde_json::Value::String(rendered_footer),
                );
                embed.insert("footer".to_string(), serde_json::Value::Object(footer_obj));
            }

            payload.insert(
                "embeds".to_string(),
                serde_json::Value::Array(vec![serde_json::Value::Object(embed)]),
            );
        }

        serde_json::Value::Object(payload)
    }

    /// Send a message via Discord Webhook
    async fn send_webhook(&self, webhook_url: &str, payload: &serde_json::Value) -> Result<()> {
        let response = self
            .client
            .post(webhook_url)
            .header("Content-Type", "application/json")
            .json(&payload)
            .send()
            .await
            .with_context(|| format!("Failed to send Discord webhook to {}", webhook_url))?;

        if !response.status().is_success() {
            let status = response.status();
            let error_body = response.text().await.unwrap_or_default();
            return Err(anyhow::anyhow!(
                "Discord webhook error ({}): {}",
                status,
                error_body
            ));
        }

        Ok(())
    }
}

#[async_trait]
impl Reflex for DiscordReflex {
    fn name(&self) -> &str {
        &self.name
    }

    fn r#type(&self) -> &str {
        "discord"
    }

    async fn handle(&self, ink: &Ink) -> Result<()> {
        info!(
            "Discord reflex '{}' handling ink on topic: {}",
            self.name, ink.topic
        );

        let webhook_url = self.resolve_webhook_url(ink);
        let payload = self.build_payload(ink);

        debug!(
            "Discord reflex '{}' sending to webhook for topic: {}",
            self.name, ink.topic
        );

        match self.send_webhook(&webhook_url, &payload).await {
            Ok(()) => {
                info!(
                    "Discord reflex '{}' sent message for topic: {}",
                    self.name, ink.topic
                );
                Ok(())
            }
            Err(e) => {
                error!(
                    "Discord reflex '{}' failed to send message for topic {}: {}",
                    self.name, ink.topic, e
                );
                Err(e)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::appcontext::AppContext;

    fn create_test_config() -> DiscordReflexConfig {
        DiscordReflexConfig {
            webhook_url: "https://discord.com/api/webhooks/123/abc".to_string(),
            content_template: "{{body}}".to_string(),
            use_embeds: true,
            embed_title: None,
            embed_description: None,
            embed_color: Some(65280),
            embed_footer: None,
            topic_webhook_mapping: HashMap::new(),
            topic_color_mapping: HashMap::new(),
            username: None,
            avatar_url: None,
            include_metadata: false,
            max_embed_fields: 25,
            tts: false,
        }
    }

    fn create_test_ink() -> Ink {
        Ink::new("/test/topic", "test", "test_source", "test body")
    }

    #[test]
    fn test_default_webhook_url() {
        let config = create_test_config();
        let reflex = DiscordReflex::new("test".to_string(), config, &AppContext::new());

        let ink = create_test_ink();
        assert_eq!(
            reflex.resolve_webhook_url(&ink),
            "https://discord.com/api/webhooks/123/abc"
        );
    }

    #[test]
    fn test_topic_webhook_mapping() {
        let mut config = create_test_config();
        let mut mapping = HashMap::new();
        mapping.insert(
            "/test/topic".to_string(),
            "https://discord.com/api/webhooks/456/def".to_string(),
        );
        config.topic_webhook_mapping = mapping;

        let reflex = DiscordReflex::new("test".to_string(), config, &AppContext::new());

        let ink = create_test_ink();
        assert_eq!(
            reflex.resolve_webhook_url(&ink),
            "https://discord.com/api/webhooks/456/def"
        );
    }

    #[test]
    fn test_default_color() {
        let config = create_test_config();
        let reflex = DiscordReflex::new("test".to_string(), config, &AppContext::new());

        let ink = create_test_ink();
        assert_eq!(reflex.resolve_color(&ink), Some(65280));
    }

    #[test]
    fn test_topic_color_mapping() {
        let mut config = create_test_config();
        let mut mapping = HashMap::new();
        mapping.insert("/test/topic".to_string(), 16711680);
        config.topic_color_mapping = mapping;

        let reflex = DiscordReflex::new("test".to_string(), config, &AppContext::new());

        let ink = create_test_ink();
        assert_eq!(reflex.resolve_color(&ink), Some(16711680));
    }

    #[test]
    fn test_template_rendering() {
        let mut config = create_test_config();
        config.content_template = "Topic: {{topic}}, Body: {{body}}".to_string();

        let reflex = DiscordReflex::new("test".to_string(), config.clone(), &AppContext::new());

        let ink = create_test_ink();
        assert_eq!(
            reflex.render_template(&config.content_template, &ink),
            "Topic: /test/topic, Body: test body"
        );
    }

    #[test]
    fn test_metadata_in_template() {
        let mut config = create_test_config();
        config.content_template = "Value: {{metadata.temp}}".to_string();

        let reflex = DiscordReflex::new("test".to_string(), config.clone(), &AppContext::new());

        let ink = Ink::new("/test", "test", "test_source", "data")
            .with_metadata(serde_json::json!({ "temp": "42", "humidity": "60" }));
        assert_eq!(
            reflex.render_template(&config.content_template, &ink),
            "Value: 42"
        );
    }

    #[test]
    fn test_embed_payload() {
        let mut config = create_test_config();
        config.embed_title = Some("{{topic}}".to_string());
        config.embed_color = Some(65280);

        let reflex = DiscordReflex::new("test".to_string(), config, &AppContext::new());

        let ink = create_test_ink();
        let payload = reflex.build_payload(&ink);

        let obj = payload.as_object().unwrap();
        assert!(obj.contains_key("embeds"));

        let embeds = obj.get("embeds").unwrap().as_array().unwrap();
        assert_eq!(embeds.len(), 1);

        let embed = embeds[0].as_object().unwrap();
        assert_eq!(embed.get("title").unwrap().as_str().unwrap(), "/test/topic");
        assert_eq!(
            embed.get("description").unwrap().as_str().unwrap(),
            "test body"
        );
        assert_eq!(embed.get("color").unwrap().as_u64().unwrap(), 65280);
    }

    #[test]
    fn test_metadata_as_embed_fields() {
        let mut config = create_test_config();
        config.include_metadata = true;

        let reflex = DiscordReflex::new("test".to_string(), config, &AppContext::new());

        let ink = Ink::new("/test", "test", "test_source", "data")
            .with_metadata(serde_json::json!({ "key1": "value1", "key2": "value2" }));
        let payload = reflex.build_payload(&ink);

        let obj = payload.as_object().unwrap();
        let embeds = obj.get("embeds").unwrap().as_array().unwrap();
        let embed = embeds[0].as_object().unwrap();

        let fields = embed.get("fields").unwrap().as_array().unwrap();
        assert_eq!(fields.len(), 2);
    }

    #[test]
    fn test_username_and_avatar() {
        let mut config = create_test_config();
        config.username = Some("Kraken Bot".to_string());
        config.avatar_url = Some("https://example.com/avatar.png".to_string());

        let reflex = DiscordReflex::new("test".to_string(), config, &AppContext::new());

        let ink = create_test_ink();
        let payload = reflex.build_payload(&ink);

        let obj = payload.as_object().unwrap();
        assert_eq!(obj.get("username").unwrap().as_str().unwrap(), "Kraken Bot");
        assert_eq!(
            obj.get("avatar_url").unwrap().as_str().unwrap(),
            "https://example.com/avatar.png"
        );
    }

    #[test]
    fn test_content_truncation() {
        let mut config = create_test_config();
        config.content_template = "{{body}}".to_string();

        let reflex = DiscordReflex::new("test".to_string(), config, &AppContext::new());

        let long_body = "a".repeat(3000);
        let ink = Ink::new("/test", "test", "test_source", &long_body);
        let payload = reflex.build_payload(&ink);

        let obj = payload.as_object().unwrap();
        let content = obj.get("content").unwrap().as_str().unwrap();
        assert_eq!(content.len(), 2000);
    }

    #[test]
    fn test_footer_template() {
        let mut config = create_test_config();
        config.embed_footer = Some("Sent at {{timestamp}}".to_string());

        let reflex = DiscordReflex::new("test".to_string(), config, &AppContext::new());

        let ink = create_test_ink();
        let payload = reflex.build_payload(&ink);

        let obj = payload.as_object().unwrap();
        let embeds = obj.get("embeds").unwrap().as_array().unwrap();
        let embed = embeds[0].as_object().unwrap();
        let footer = embed.get("footer").unwrap().as_object().unwrap();
        let footer_text = footer.get("text").unwrap().as_str().unwrap();
        assert!(footer_text.starts_with("Sent at "));
    }

    #[test]
    fn test_config_deserialization() {
        let yaml = r#"
            webhook_url: "https://discord.com/api/webhooks/123/abc"
            content_template: "Alert: {{topic}} - {{body}}"
            use_embeds: true
            embed_title: "{{topic}}"
            embed_color: 16711680
            embed_footer: "rustyKraken"
            username: "Kraken Bot"
            avatar_url: "https://example.com/bot.png"
            topic_webhook_mapping:
              "/alerts": "https://discord.com/api/webhooks/456/def"
            topic_color_mapping:
              "/alerts": 16711680
              "/info": 65280
            include_metadata: true
            max_embed_fields: 10
            tts: false
        "#;

        let config: DiscordReflexConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(
            config.webhook_url,
            "https://discord.com/api/webhooks/123/abc"
        );
        assert_eq!(config.content_template, "Alert: {{topic}} - {{body}}");
        assert!(config.use_embeds);
        assert_eq!(config.embed_color, Some(16711680));
        assert_eq!(config.username, Some("Kraken Bot".to_string()));
        assert!(config.include_metadata);
        assert_eq!(config.max_embed_fields, 10);
    }

    #[test]
    fn test_no_embeds_mode() {
        let mut config = create_test_config();
        config.use_embeds = false;

        let reflex = DiscordReflex::new("test".to_string(), config, &AppContext::new());

        let ink = create_test_ink();
        let payload = reflex.build_payload(&ink);

        let obj = payload.as_object().unwrap();
        assert!(!obj.contains_key("embeds"));
        assert_eq!(obj.get("content").unwrap().as_str().unwrap(), "test body");
    }

    #[test]
    fn test_timestamp_and_id_in_template() {
        let mut config = create_test_config();
        config.content_template = "ID: {{id}}, Time: {{timestamp}}".to_string();

        let reflex = DiscordReflex::new("test".to_string(), config.clone(), &AppContext::new());

        let ink = create_test_ink();
        let rendered = reflex.render_template(&config.content_template, &ink);
        assert!(rendered.starts_with("ID: "));
        assert!(rendered.contains(", Time: "));
    }

    #[test]
    fn test_max_embed_fields_limit() {
        let mut config = create_test_config();
        config.include_metadata = true;
        config.max_embed_fields = 2;

        let reflex = DiscordReflex::new("test".to_string(), config, &AppContext::new());

        let ink = Ink::new("/test", "test", "test_source", "data")
            .with_metadata(serde_json::json!({ "a": "1", "b": "2", "c": "3", "d": "4" }));
        let payload = reflex.build_payload(&ink);

        let obj = payload.as_object().unwrap();
        let embeds = obj.get("embeds").unwrap().as_array().unwrap();
        let embed = embeds[0].as_object().unwrap();
        let fields = embed.get("fields").unwrap().as_array().unwrap();
        assert_eq!(fields.len(), 2); // limited to max_embed_fields
    }
}
