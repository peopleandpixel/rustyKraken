#![allow(dead_code)]

use anyhow::{Context, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, info};

use super::Reflex;
use crate::appcontext::AppContext;
use crate::model::Ink;

/// Configuration for Kafka Reflex
/// Publishes messages to Apache Kafka via REST Proxy
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KafkaReflexConfig {
    /// Kafka REST Proxy URL (e.g., "http://localhost:8082")
    pub broker_url: String,

    /// Default topic to publish to
    #[serde(default)]
    pub topic: Option<String>,

    /// Template for the topic name
    #[serde(default)]
    pub topic_template: Option<String>,

    /// Template for the message key
    #[serde(default)]
    pub key_template: Option<String>,

    /// Template for the message value
    #[serde(default = "default_value_template")]
    pub value_template: String,

    /// Optional: mapping from Ink topics to Kafka topics
    #[serde(default)]
    pub topic_mapping: HashMap<String, String>,

    /// Optional: mapping from Ink topics to message keys
    #[serde(default)]
    pub key_mapping: HashMap<String, String>,

    /// API key for REST Proxy authentication
    #[serde(default)]
    pub api_key: Option<String>,

    /// API secret for REST Proxy authentication
    #[serde(default)]
    pub api_secret: Option<String>,

    /// Username for basic auth
    #[serde(default)]
    pub username: Option<String>,

    /// Password for basic auth
    #[serde(default)]
    pub password: Option<String>,

    /// Custom headers
    #[serde(default)]
    pub custom_headers: HashMap<String, String>,

    /// Whether to partition by key
    #[serde(default)]
    pub partition_by_key: bool,

    /// Specific partition number (overrides partition_by_key)
    #[serde(default)]
    pub partition: Option<i32>,

    /// Timeout in milliseconds
    #[serde(default = "default_timeout_ms")]
    pub timeout_ms: i32,

    /// Message headers (JSON object template)
    #[serde(default)]
    pub headers_template: Option<String>,
}

fn default_value_template() -> String {
    "{{body}}".to_string()
}
fn default_timeout_ms() -> i32 {
    5000
}

pub struct KafkaReflex {
    name: String,
    config: KafkaReflexConfig,
    client: Arc<reqwest::Client>,
}

impl KafkaReflex {
    pub fn new(name: String, config: KafkaReflexConfig, app_ctx: &AppContext) -> Self {
        Self {
            name,
            config,
            client: Arc::clone(app_ctx.http_client()),
        }
    }

    /// Resolve Kafka topic for a given Ink
    fn resolve_topic(&self, ink: &Ink) -> Result<String> {
        if let Some(topic) = self
            .config
            .topic_mapping
            .get(&ink.topic)
            .filter(|t| !t.is_empty())
        {
            return Ok(topic.clone());
        }
        if let Some(ref template) = self.config.topic_template {
            return Ok(self.render_template(template, ink));
        }
        self.config
            .topic
            .clone()
            .context("Kafka topic is required - set topic, topic_template, or topic_mapping")
    }

    /// Resolve message key for a given Ink
    fn resolve_key(&self, ink: &Ink) -> Option<String> {
        if let Some(key) = self
            .config
            .key_mapping
            .get(&ink.topic)
            .filter(|k| !k.is_empty())
        {
            return Some(key.clone());
        }
        if let Some(ref template) = self.config.key_template {
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

    /// Build message headers from template
    fn build_headers(&self, ink: &Ink) -> Option<Vec<(String, String)>> {
        if let Some(ref template) = self.config.headers_template {
            let rendered = self.render_template(template, ink);
            if let Ok(headers) =
                serde_json::from_str::<serde_json::Map<String, serde_json::Value>>(&rendered)
            {
                let mut result = Vec::new();
                for (k, v) in headers {
                    result.push((k, v.to_string()));
                }
                return Some(result);
            }
        }
        None
    }

    /// Publish a message to Kafka via REST Proxy
    async fn publish_message(
        &self,
        kafka_topic: &str,
        key: Option<&str>,
        value: &str,
        headers: Option<Vec<(String, String)>>,
    ) -> Result<()> {
        let url = format!(
            "{}/api/v3/topics/{}",
            self.config.broker_url.trim_end_matches('/'),
            kafka_topic
        );

        // Build the record
        let mut record = serde_json::json!({
            "value": value
        });

        if let Some(k) = key {
            record["key"] = serde_json::json!(k);
        }

        if let Some(partition) = self.config.partition {
            record["partition"] = serde_json::json!(partition);
        }

        if let Some(h) = headers {
            let header_vec: Vec<serde_json::Value> = h
                .into_iter()
                .map(|(k, v)| serde_json::json!({"key": k, "value": v}))
                .collect();
            record["headers"] = serde_json::json!(header_vec);
        }

        let payload = serde_json::json!({
            "records": vec![record],
            "timeout": self.config.timeout_ms
        });

        let mut request = self.client.post(&url).json(&payload);

        // Add authentication headers
        if let Some(ref api_key) = self.config.api_key {
            if let Some(ref api_secret) = self.config.api_secret {
                request = request.basic_auth(api_key, Some(api_secret));
            } else {
                request = request.header("Authorization", format!("Bearer {}", api_key));
            }
        } else if let (Some(user), Some(pass)) = (&self.config.username, &self.config.password) {
            request = request.basic_auth(user, Some(pass));
        }

        // Add custom headers
        for (k, v) in &self.config.custom_headers {
            request = request.header(k, v);
        }

        let response = request
            .send()
            .await
            .with_context(|| format!("Failed to send Kafka REST Proxy request to {}", url))?;

        if !response.status().is_success() {
            let status = response.status();
            let error_body = response.text().await.unwrap_or_default();
            return Err(anyhow::anyhow!(
                "Kafka REST Proxy error ({}): {}",
                status,
                error_body
            ));
        }

        // Parse response to get offset info
        let body = response
            .text()
            .await
            .context("Failed to read Kafka REST Proxy response")?;
        debug!("Kafka REST Proxy response: {}", body);

        Ok(())
    }

    /// List available topics via REST Proxy
    async fn list_topics(&self) -> Result<Vec<String>> {
        let url = format!(
            "{}/api/v3/topics",
            self.config.broker_url.trim_end_matches('/')
        );

        let mut request = self.client.get(&url);
        if let Some(ref api_key) = self.config.api_key
            && let Some(ref api_secret) = self.config.api_secret
        {
            request = request.basic_auth(api_key, Some(api_secret));
        }

        let response = request
            .send()
            .await
            .context("Failed to list Kafka topics")?;

        if !response.status().is_success() {
            let status = response.status();
            let error_body = response.text().await.unwrap_or_default();
            return Err(anyhow::anyhow!(
                "Kafka REST Proxy list topics error ({}): {}",
                status,
                error_body
            ));
        }

        let body: serde_json::Value = response
            .json()
            .await
            .context("Failed to parse Kafka topics response")?;

        let topics = body["data"]
            .as_array()
            .map(|arr| {
                arr.iter()
                    .filter_map(|t| t["name"].as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default();

        Ok(topics)
    }

    /// Check if a topic exists
    async fn topic_exists(&self, topic: &str) -> Result<bool> {
        let url = format!(
            "{}/api/v3/topics/{}",
            self.config.broker_url.trim_end_matches('/'),
            topic
        );

        let mut request = self.client.get(&url);
        if let Some(ref api_key) = self.config.api_key
            && let Some(ref api_secret) = self.config.api_secret
        {
            request = request.basic_auth(api_key, Some(api_secret));
        }

        let response = request
            .send()
            .await
            .context("Failed to check Kafka topic")?;
        Ok(response.status() == 200)
    }
}

#[async_trait]
impl Reflex for KafkaReflex {
    fn name(&self) -> &str {
        &self.name
    }

    fn r#type(&self) -> &str {
        "kafka"
    }

    async fn handle(&self, ink: &Ink) -> Result<()> {
        info!(
            "Kafka reflex '{}' handling ink on topic: {}",
            self.name, ink.topic
        );

        // Resolve Kafka topic
        let kafka_topic = self.resolve_topic(ink)?;
        debug!(
            "Kafka reflex '{}' resolved topic: {}",
            self.name, kafka_topic
        );

        // Resolve message key
        let key = self.resolve_key(ink);
        if let Some(ref k) = key {
            debug!("Kafka reflex '{}' resolved key: {}", self.name, k);
        }

        // Render message value
        let value = self.render_template(&self.config.value_template, ink);
        debug!("Kafka reflex '{}' rendered value: {}", self.name, value);

        // Build headers
        let headers = self.build_headers(ink);

        // Publish the message
        self.publish_message(&kafka_topic, key.as_deref(), &value, headers)
            .await?;

        info!(
            "Kafka reflex '{}' published message to topic: {}",
            self.name, kafka_topic
        );

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::appcontext::AppContext;

    fn create_test_config() -> KafkaReflexConfig {
        KafkaReflexConfig {
            broker_url: "http://localhost:8082".to_string(),
            topic: Some("test-topic".to_string()),
            topic_template: None,
            key_template: None,
            value_template: "{{body}}".to_string(),
            topic_mapping: HashMap::new(),
            key_mapping: HashMap::new(),
            api_key: None,
            api_secret: None,
            username: None,
            password: None,
            custom_headers: HashMap::new(),
            partition_by_key: false,
            partition: None,
            timeout_ms: 5000,
            headers_template: None,
        }
    }

    fn create_test_ink() -> Ink {
        Ink::new("/test/topic", "test", "test_source", "test body")
    }

    #[test]
    fn test_template_rendering() {
        let config = create_test_config();
        let reflex = KafkaReflex::new("test".to_string(), config, &AppContext::new());
        let ink = create_test_ink();
        assert_eq!(
            reflex.render_template("[{{topic}}] {{body}}", &ink),
            "[/test/topic] test body"
        );
    }

    #[test]
    fn test_config_deserialization() {
        let yaml = r#"
            broker_url: "http://kafka:8082"
            topic: "alerts"
            value_template: "[{{topic}}] {{body}}"
        "#;
        let config: KafkaReflexConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.broker_url, "http://kafka:8082");
        assert_eq!(config.topic, Some("alerts".to_string()));
    }

    #[test]
    fn test_reflex_type() {
        let config = create_test_config();
        let reflex = KafkaReflex::new("test".to_string(), config, &AppContext::new());
        assert_eq!(reflex.r#type(), "kafka");
    }

    #[test]
    fn test_resolve_topic_from_mapping() {
        let mut config = create_test_config();
        let mut mapping = HashMap::new();
        mapping.insert(
            "/alerts/critical".to_string(),
            "critical-alerts".to_string(),
        );
        config.topic_mapping = mapping;
        config.topic = None;

        let reflex = KafkaReflex::new("test".to_string(), config, &AppContext::new());
        let ink = Ink::new("/alerts/critical", "test", "test_source", "body");
        assert_eq!(reflex.resolve_topic(&ink).unwrap(), "critical-alerts");
    }

    #[test]
    fn test_resolve_key_from_template() {
        let mut config = create_test_config();
        config.key_template = Some("{{id}}".to_string());

        let reflex = KafkaReflex::new("test".to_string(), config, &AppContext::new());
        let ink = create_test_ink();
        let key = reflex.resolve_key(&ink);
        assert!(key.is_some());
        assert_eq!(key.unwrap(), ink.id.to_string());
    }
}
