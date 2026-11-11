use anyhow::{Context, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::{debug, error, info, warn};

use super::Reflex;
use crate::model::Ink;

/// Configuration for WhatsApp Business API Reflex
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WhatsAppReflexConfig {
    /// WhatsApp Business API access token
    pub access_token: String,
    /// Phone number ID from WhatsApp Business Manager
    pub phone_number_id: String,
    /// Business account ID (optional)
    #[serde(default)]
    pub business_account_id: Option<String>,
    /// Recipient phone numbers in international format (e.g., "+1234567890")
    pub recipients: Vec<String>,
    /// Message template name (for template messages via WhatsApp API)
    #[serde(default)]
    pub template_name: Option<String>,
    /// Template language code (e.g., "en_US", "de_DE")
    #[serde(default = "default_language")]
    pub language: String,
    /// Message type: "template", "text", or "auto" (template if configured, else text)
    #[serde(default = "default_message_type")]
    pub message_type: String,
    /// Template for the message body with placeholders
    /// Supported placeholders: {{topic}}, {{body}}, {{id}}, {{timestamp}}, {{metadata.<key>}}
    #[serde(default = "default_body_template")]
    pub body_template: String,
    /// Optional: mapping from topics to recipient numbers
    #[serde(default)]
    pub topic_recipient_mapping: HashMap<String, Vec<String>>,
    /// Optional: mapping from topics to template names
    #[serde(default)]
    pub topic_template_mapping: HashMap<String, String>,
    /// API version (default: "v18.0")
    #[serde(default = "default_api_version")]
    pub api_version: String,
    /// Optional: custom API base URL (for third-party providers)
    #[serde(default)]
    pub api_base_url: Option<String>,
    /// Whether to include Ink metadata in the message
    #[serde(default)]
    pub include_metadata: bool,
}

fn default_language() -> String {
    "en_US".to_string()
}

fn default_message_type() -> String {
    "auto".to_string()
}

fn default_body_template() -> String {
    "{{body}}".to_string()
}

fn default_api_version() -> String {
    "v18.0".to_string()
}

pub struct WhatsAppReflex {
    name: String,
    config: WhatsAppReflexConfig,
    client: reqwest::Client,
}

impl WhatsAppReflex {
    pub fn new(name: String, config: WhatsAppReflexConfig) -> Self {
        Self {
            name,
            config,
            client: reqwest::Client::new(),
        }
    }

    /// Resolve the effective message type
    fn resolve_message_type(&self) -> &str {
        match self.config.message_type.as_str() {
            "auto" => {
                if self.config.template_name.is_some() {
                    "template"
                } else {
                    "text"
                }
            }
            other => other,
        }
    }

    /// Resolve recipients for a given Ink, checking topic-based mapping first
    fn resolve_recipients(&self, ink: &Ink) -> Vec<String> {
        // Check topic-based recipient mapping
        if let Some(recipients) = self
            .config
            .topic_recipient_mapping
            .get(&ink.topic)
            .filter(|r| !r.is_empty())
        {
            return recipients.clone();
        }

        // Fall back to default recipients
        self.config.recipients.clone()
    }

    /// Resolve template name for a given Ink, checking topic-based mapping first
    fn resolve_template_name(&self, ink: &Ink) -> Option<String> {
        // Check topic-based template mapping
        if let Some(template) = self
            .config
            .topic_template_mapping
            .get(&ink.topic)
            .filter(|t| !t.is_empty())
        {
            return Some(template.clone());
        }

        // Fall back to default template
        self.config.template_name.clone()
    }

    /// Render the message body from the template and Ink data
    fn render_body(&self, ink: &Ink) -> String {
        let mut body = self.config.body_template.clone();

        // Replace {{topic}}
        body = body.replace("{{topic}}", &ink.topic);

        // Replace {{body}}
        body = body.replace("{{body}}", &ink.body);

        // Replace {{id}}
        body = body.replace("{{id}}", &ink.id.to_string());

        // Replace {{timestamp}}
        body = body.replace("{{timestamp}}", &ink.timestamp.to_rfc3339());

        // Replace {{metadata.<key>}} placeholders
        if let Some(obj) = ink.metadata.as_ref().and_then(|m| m.as_object()) {
            for (key, value) in obj {
                let placeholder = format!("{{{{metadata.{}}}}}", key);
                let value_str = match value {
                    serde_json::Value::String(s) => s.clone(),
                    _ => value.to_string(),
                };
                body = body.replace(&placeholder, &value_str);
            }
        }

        body
    }

    /// Build the full message text, optionally including metadata
    fn build_message(&self, ink: &Ink) -> String {
        let rendered = self.render_body(ink);

        if self.config.include_metadata {
            let metadata_json = if let Some(ref metadata) = ink.metadata {
                serde_json::to_string_pretty(metadata).unwrap_or_else(|_| "{}".to_string())
            } else {
                "{}".to_string()
            };
            format!("{}\n\n--- Metadata ---\n{}", rendered, metadata_json)
        } else {
            rendered
        }
    }

    /// Send a text message via WhatsApp Business API
    async fn send_text_message(&self, recipient: &str, message: &str) -> Result<()> {
        let base_url = self
            .config
            .api_base_url
            .as_deref()
            .unwrap_or("https://graph.facebook.com");
        let url = format!(
            "{}/{}/{}/messages",
            base_url, self.config.api_version, self.config.phone_number_id
        );

        let mut payload = serde_json::Map::new();
        payload.insert(
            "messaging_product".to_string(),
            serde_json::Value::String("whatsapp".to_string()),
        );
        payload.insert(
            "to".to_string(),
            serde_json::Value::String(recipient.to_string()),
        );
        payload.insert(
            "type".to_string(),
            serde_json::Value::String("text".to_string()),
        );

        let mut text_obj = serde_json::Map::new();
        text_obj.insert(
            "body".to_string(),
            serde_json::Value::String(message.to_string()),
        );
        payload.insert("text".to_string(), serde_json::Value::Object(text_obj));

        let response = self
            .client
            .post(&url)
            .header(
                "Authorization",
                format!("Bearer {}", self.config.access_token),
            )
            .header("Content-Type", "application/json")
            .json(&payload)
            .send()
            .await
            .with_context(|| format!("Failed to send WhatsApp message to {}", recipient))?;

        if !response.status().is_success() {
            let status = response.status();
            let error_body = response.text().await.unwrap_or_default();
            return Err(anyhow::anyhow!(
                "WhatsApp API error ({}): {}",
                status,
                error_body
            ));
        }

        Ok(())
    }

    /// Send a template message via WhatsApp Business API
    async fn send_template_message(
        &self,
        recipient: &str,
        template_name: &str,
        ink: &Ink,
    ) -> Result<()> {
        let base_url = self
            .config
            .api_base_url
            .as_deref()
            .unwrap_or("https://graph.facebook.com");
        let url = format!(
            "{}/{}/{}/messages",
            base_url, self.config.api_version, self.config.phone_number_id
        );

        let mut payload = serde_json::Map::new();
        payload.insert(
            "messaging_product".to_string(),
            serde_json::Value::String("whatsapp".to_string()),
        );
        payload.insert(
            "to".to_string(),
            serde_json::Value::String(recipient.to_string()),
        );
        payload.insert(
            "type".to_string(),
            serde_json::Value::String("template".to_string()),
        );

        let mut template_obj = serde_json::Map::new();
        template_obj.insert(
            "name".to_string(),
            serde_json::Value::String(template_name.to_string()),
        );
        template_obj.insert(
            "language".to_string(),
            serde_json::Value::Object(
                [(
                    "code".to_string(),
                    serde_json::Value::String(self.config.language.clone()),
                )]
                .into_iter()
                .collect(),
            ),
        );

        // Build template components from Ink data
        let components: Vec<serde_json::Value> = vec![serde_json::json!({
            "type": "body",
            "parameters": [
                { "type": "text", "text": ink.topic.clone() },
                { "type": "text", "text": ink.body.clone() }
            ]
        })];
        template_obj.insert(
            "components".to_string(),
            serde_json::Value::Array(components),
        );

        payload.insert(
            "template".to_string(),
            serde_json::Value::Object(template_obj),
        );

        let response = self
            .client
            .post(&url)
            .header(
                "Authorization",
                format!("Bearer {}", self.config.access_token),
            )
            .header("Content-Type", "application/json")
            .json(&payload)
            .send()
            .await
            .with_context(|| format!("Failed to send WhatsApp template to {}", recipient))?;

        if !response.status().is_success() {
            let status = response.status();
            let error_body = response.text().await.unwrap_or_default();
            return Err(anyhow::anyhow!(
                "WhatsApp API error ({}): {}",
                status,
                error_body
            ));
        }

        Ok(())
    }
}

#[async_trait]
impl Reflex for WhatsAppReflex {
    fn name(&self) -> &str {
        &self.name
    }

    fn r#type(&self) -> &str {
        "whatsapp"
    }

    async fn handle(&self, ink: &Ink) -> Result<()> {
        info!(
            "WhatsApp reflex '{}' handling ink on topic: {}",
            self.name, ink.topic
        );

        let recipients = self.resolve_recipients(ink);
        if recipients.is_empty() {
            warn!(
                "WhatsApp reflex '{}' has no recipients for topic: {}",
                self.name, ink.topic
            );
            return Ok(());
        }

        let message_type = self.resolve_message_type();
        debug!(
            "WhatsApp reflex '{}' sending {} message to {:?} for topic: {}",
            self.name, message_type, recipients, ink.topic
        );

        let mut success_count = 0;
        let mut error_count = 0;

        for recipient in &recipients {
            let result = match message_type {
                "template" => {
                    let template_name = self
                        .resolve_template_name(ink)
                        .unwrap_or_else(|| "default_template".to_string());
                    self.send_template_message(recipient, &template_name, ink)
                        .await
                }
                "text" => {
                    let message = self.build_message(ink);
                    self.send_text_message(recipient, &message).await
                }
                _ => {
                    warn!("Unknown message type: {}", message_type);
                    continue;
                }
            };

            match result {
                Ok(()) => {
                    info!(
                        "WhatsApp reflex '{}' sent message to {} for topic: {}",
                        self.name, recipient, ink.topic
                    );
                    success_count += 1;
                }
                Err(e) => {
                    error!(
                        "WhatsApp reflex '{}' failed to send to {}: {}",
                        self.name, recipient, e
                    );
                    error_count += 1;
                }
            }
        }

        info!(
            "WhatsApp reflex '{}' completed: {} succeeded, {} failed",
            self.name, success_count, error_count
        );

        if error_count > 0 && success_count == 0 {
            Err(anyhow::anyhow!(
                "WhatsApp reflex '{}' failed to send to all recipients",
                self.name
            ))
        } else {
            Ok(())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_config() -> WhatsAppReflexConfig {
        WhatsAppReflexConfig {
            access_token: "test_token".to_string(),
            phone_number_id: "1234567890".to_string(),
            business_account_id: None,
            recipients: vec!["+1234567890".to_string()],
            template_name: None,
            language: "en_US".to_string(),
            message_type: "auto".to_string(),
            body_template: "{{body}}".to_string(),
            topic_recipient_mapping: HashMap::new(),
            topic_template_mapping: HashMap::new(),
            api_version: "v18.0".to_string(),
            api_base_url: None,
            include_metadata: false,
        }
    }

    fn create_test_ink() -> Ink {
        Ink::new(
            "/test/topic".to_string(),
            "whatsapp",
            "test",
            "Hello, World!",
        )
    }

    #[test]
    fn test_resolve_message_type_auto_with_template() {
        let mut config = create_test_config();
        config.template_name = Some("welcome_template".to_string());
        let reflex = WhatsAppReflex::new("test".to_string(), config);

        assert_eq!(reflex.resolve_message_type(), "template");
    }

    #[test]
    fn test_resolve_message_type_auto_without_template() {
        let config = create_test_config();
        let reflex = WhatsAppReflex::new("test".to_string(), config);

        assert_eq!(reflex.resolve_message_type(), "text");
    }

    #[test]
    fn test_resolve_message_type_explicit() {
        let mut config = create_test_config();
        config.message_type = "text".to_string();
        let reflex = WhatsAppReflex::new("test".to_string(), config);

        assert_eq!(reflex.resolve_message_type(), "text");
    }

    #[test]
    fn test_resolve_recipients_default() {
        let config = create_test_config();
        let reflex = WhatsAppReflex::new("test".to_string(), config);
        let ink = create_test_ink();

        let recipients = reflex.resolve_recipients(&ink);
        assert_eq!(recipients, vec!["+1234567890".to_string()]);
    }

    #[test]
    fn test_resolve_recipients_from_topic_mapping() {
        let mut config = create_test_config();
        let mut mapping = HashMap::new();
        mapping.insert("/test/topic".to_string(), vec!["+9999999999".to_string()]);
        config.topic_recipient_mapping = mapping;
        let reflex = WhatsAppReflex::new("test".to_string(), config);
        let ink = create_test_ink();

        let recipients = reflex.resolve_recipients(&ink);
        assert_eq!(recipients, vec!["+9999999999".to_string()]);
    }

    #[test]
    fn test_resolve_recipients_fallback_for_unknown_topic() {
        let mut config = create_test_config();
        let mut mapping = HashMap::new();
        mapping.insert("/other/topic".to_string(), vec!["+9999999999".to_string()]);
        config.topic_recipient_mapping = mapping;
        let reflex = WhatsAppReflex::new("test".to_string(), config);
        let ink = create_test_ink();

        // Topic doesn't match mapping, should fall back to default
        let recipients = reflex.resolve_recipients(&ink);
        assert_eq!(recipients, vec!["+1234567890".to_string()]);
    }

    #[test]
    fn test_resolve_template_name_default() {
        let mut config = create_test_config();
        config.template_name = Some("default_template".to_string());
        let reflex = WhatsAppReflex::new("test".to_string(), config);
        let ink = create_test_ink();

        assert_eq!(
            reflex.resolve_template_name(&ink),
            Some("default_template".to_string())
        );
    }

    #[test]
    fn test_resolve_template_name_from_topic_mapping() {
        let mut config = create_test_config();
        config.template_name = Some("default_template".to_string());
        let mut mapping = HashMap::new();
        mapping.insert(
            "/test/topic".to_string(),
            "topic_specific_template".to_string(),
        );
        config.topic_template_mapping = mapping;
        let reflex = WhatsAppReflex::new("test".to_string(), config);
        let ink = create_test_ink();

        assert_eq!(
            reflex.resolve_template_name(&ink),
            Some("topic_specific_template".to_string())
        );
    }

    #[test]
    fn test_render_body_simple() {
        let config = create_test_config();
        let reflex = WhatsAppReflex::new("test".to_string(), config);
        let ink = create_test_ink();

        let body = reflex.render_body(&ink);
        assert_eq!(body, "Hello, World!");
    }

    #[test]
    fn test_render_body_with_placeholders() {
        let mut config = create_test_config();
        config.body_template = "Topic: {{topic}}\nMessage: {{body}}\nID: {{id}}".to_string();
        let reflex = WhatsAppReflex::new("test".to_string(), config);
        let ink = create_test_ink();

        let body = reflex.render_body(&ink);
        assert!(body.starts_with("Topic: /test/topic"));
        assert!(body.contains("Message: Hello, World!"));
        assert!(body.contains("ID: "));
    }

    #[test]
    fn test_render_body_with_metadata() {
        let mut config = create_test_config();
        config.body_template = "{{body}} - {{metadata.sender}}".to_string();
        let reflex = WhatsAppReflex::new("test".to_string(), config);

        let mut ink = create_test_ink();
        let mut metadata = serde_json::Map::new();
        metadata.insert(
            "sender".to_string(),
            serde_json::Value::String("Alice".to_string()),
        );
        ink.metadata = Some(serde_json::Value::Object(metadata));

        let body = reflex.render_body(&ink);
        assert_eq!(body, "Hello, World! - Alice");
    }

    #[test]
    fn test_build_message_with_metadata_included() {
        let mut config = create_test_config();
        config.include_metadata = true;
        let reflex = WhatsAppReflex::new("test".to_string(), config);

        let mut ink = create_test_ink();
        let mut metadata = serde_json::Map::new();
        metadata.insert(
            "key".to_string(),
            serde_json::Value::String("value".to_string()),
        );
        ink.metadata = Some(serde_json::Value::Object(metadata));

        let message = reflex.build_message(&ink);
        assert!(message.contains("Hello, World!"));
        assert!(message.contains("--- Metadata ---"));
        assert!(message.contains("\"key\""));
    }

    #[test]
    fn test_build_message_without_metadata() {
        let config = create_test_config();
        let reflex = WhatsAppReflex::new("test".to_string(), config);
        let ink = create_test_ink();

        let message = reflex.build_message(&ink);
        assert_eq!(message, "Hello, World!");
        assert!(!message.contains("--- Metadata ---"));
    }

    #[test]
    fn test_config_deserialization() {
        let yaml = r#"
            access_token: "my_token"
            phone_number_id: "1234567890"
            recipients:
              - "+1111111111"
              - "+2222222222"
            message_type: "text"
            body_template: "Alert: {{body}}"
            language: "de_DE"
        "#;

        let config: WhatsAppReflexConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.access_token, "my_token");
        assert_eq!(config.phone_number_id, "1234567890");
        assert_eq!(config.recipients.len(), 2);
        assert_eq!(config.message_type, "text");
        assert_eq!(config.language, "de_DE");
    }

    #[test]
    fn test_empty_recipients() {
        let mut config = create_test_config();
        config.recipients = vec![];
        let reflex = WhatsAppReflex::new("test".to_string(), config);
        let ink = create_test_ink();

        let recipients = reflex.resolve_recipients(&ink);
        assert!(recipients.is_empty());
    }

    #[test]
    fn test_timestamp_placeholder() {
        let mut config = create_test_config();
        config.body_template = "Sent at: {{timestamp}}".to_string();
        let reflex = WhatsAppReflex::new("test".to_string(), config);
        let ink = create_test_ink();

        let body = reflex.render_body(&ink);
        assert!(body.starts_with("Sent at: "));
        // Verify it's a valid RFC3339 timestamp
        let ts_str = &body["Sent at: ".len()..];
        assert!(
            chrono::DateTime::parse_from_rfc3339(ts_str).is_ok(),
            "Timestamp should be valid RFC3339"
        );
    }
}
