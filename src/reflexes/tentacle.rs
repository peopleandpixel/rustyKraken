use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, info, warn};

use super::Reflex;
use crate::eventbus::EventBus;
use crate::model::Ink;

/// Configuration for Tentacle Reflex – forwards Ink messages to the EventBus
/// with optional topic transformation, body templating, and metadata filtering.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TentacleReflexConfig {
    /// Target topic template – supports {{topic}}, {{body}}, {{id}}, {{timestamp}}, {{metadata.<key>}}
    #[serde(default = "default_target_topic")]
    pub target_topic: String,

    /// Optional: mapping from source topics to target topics (exact match)
    #[serde(default)]
    pub topic_mapping: HashMap<String, String>,

    /// Optional: mapping from source topic prefixes to target topic prefixes (longest prefix wins)
    #[serde(default)]
    pub topic_prefix_mapping: HashMap<String, String>,

    /// Body template – supports {{topic}}, {{body}}, {{id}}, {{timestamp}}, {{metadata.<key>}}
    #[serde(default = "default_body_template")]
    pub body_template: String,

    /// Optional: only forward if body matches this regex pattern
    #[serde(default)]
    pub body_filter: Option<String>,

    /// Optional: only forward if source topic matches this regex pattern
    #[serde(default)]
    pub topic_filter: Option<String>,

    /// Whether to preserve original metadata (true) or replace with filtered metadata (false)
    #[serde(default = "default_preserve_metadata")]
    pub preserve_metadata: bool,

    /// Optional: list of metadata keys to include (when preserve_metadata is false)
    #[serde(default)]
    pub metadata_keys: Vec<String>,

    /// Optional: add custom metadata fields to forwarded messages
    #[serde(default)]
    pub add_metadata: HashMap<String, String>,
}

fn default_target_topic() -> String {
    "{{topic}}".to_string()
}

fn default_body_template() -> String {
    "{{body}}".to_string()
}

fn default_preserve_metadata() -> bool {
    true
}

pub struct TentacleReflex {
    name: String,
    config: TentacleReflexConfig,
    event_bus: Arc<EventBus>,
}

impl TentacleReflex {
    pub fn new(name: String, config: TentacleReflexConfig, event_bus: Arc<EventBus>) -> Self {
        Self {
            name,
            config,
            event_bus,
        }
    }

    /// Resolve the target topic for a given Ink, checking mappings first
    fn resolve_target_topic(&self, ink: &Ink) -> String {
        // 1. Check exact topic mapping
        if let Some(target) = self
            .config
            .topic_mapping
            .get(&ink.topic)
            .filter(|t| !t.is_empty())
        {
            return target.clone();
        }

        // 2. Check prefix mapping (longest prefix wins)
        let mut best_prefix: Option<(&str, &str)> = None;
        for (prefix, target_prefix) in &self.config.topic_prefix_mapping {
            if ink.topic.starts_with(prefix) {
                match best_prefix {
                    None => best_prefix = Some((prefix.as_str(), target_prefix.as_str())),
                    Some((best_p, _)) => {
                        if prefix.len() > best_p.len() {
                            best_prefix = Some((prefix.as_str(), target_prefix.as_str()));
                        }
                    }
                }
            }
        }
        if let Some((prefix, target_prefix)) = best_prefix {
            let suffix = &ink.topic[prefix.len()..];
            return format!("{}{}", target_prefix, suffix);
        }

        // 3. Fall back to template
        self.render_template(&self.config.target_topic, ink)
    }

    /// Render the body from the template and Ink data
    fn render_body(&self, ink: &Ink) -> String {
        self.render_template(&self.config.body_template, ink)
    }

    /// Render a template string with Ink data
    fn render_template(&self, template: &str, ink: &Ink) -> String {
        let mut result = template.to_string();

        // Replace {{topic}}
        result = result.replace("{{topic}}", &ink.topic);

        // Replace {{body}}
        result = result.replace("{{body}}", &ink.body);

        // Replace {{id}}
        result = result.replace("{{id}}", &ink.id.to_string());

        // Replace {{timestamp}}
        result = result.replace("{{timestamp}}", &ink.timestamp.to_rfc3339());

        // Replace {{metadata.<key>}} placeholders
        if let Some(obj) = ink.metadata.as_ref().and_then(|m| m.as_object()) {
            for (key, value) in obj {
                let placeholder = format!("{{{{metadata.{}}}}}", key);
                let value_str = match value {
                    serde_json::Value::String(s) => s.clone(),
                    _ => value.to_string(),
                };
                result = result.replace(&placeholder, &value_str);
            }
        }

        result
    }

    /// Check if the Ink passes the configured filters
    fn passes_filters(&self, ink: &Ink) -> bool {
        // Check topic filter
        if let Some(ref topic_filter) = self.config.topic_filter {
            match regex::Regex::new(topic_filter) {
                Ok(re) => {
                    if !re.is_match(&ink.topic) {
                        return false;
                    }
                }
                Err(e) => {
                    warn!(
                        "Tentacle reflex '{}' has invalid topic_filter regex: {}",
                        self.name, e
                    );
                }
            }
        }

        // Check body filter
        if let Some(ref body_filter) = self.config.body_filter {
            match regex::Regex::new(body_filter) {
                Ok(re) => {
                    if !re.is_match(&ink.body) {
                        return false;
                    }
                }
                Err(e) => {
                    warn!(
                        "Tentacle reflex '{}' has invalid body_filter regex: {}",
                        self.name, e
                    );
                }
            }
        }

        true
    }

    /// Build the metadata for the forwarded Ink
    fn build_metadata(&self, ink: &Ink) -> Option<serde_json::Value> {
        if self.config.preserve_metadata {
            // Preserve original metadata and add custom fields
            let mut meta = ink
                .metadata
                .clone()
                .unwrap_or_else(|| serde_json::json!({}));

            if let Some(obj) = meta.as_object_mut() {
                for (key, value) in &self.config.add_metadata {
                    obj.insert(key.clone(), serde_json::Value::String(value.clone()));
                }
            }

            // Add original topic as metadata for tracing
            if let Some(obj) = meta.as_object_mut() {
                obj.insert(
                    "__original_topic".to_string(),
                    serde_json::Value::String(ink.topic.clone()),
                );
            }

            Some(meta)
        } else {
            // Filter metadata by configured keys
            let mut obj = serde_json::Map::new();

            if let Some(source_obj) = ink.metadata.as_ref().and_then(|m| m.as_object()) {
                for key in &self.config.metadata_keys {
                    if let Some(value) = source_obj.get(key) {
                        obj.insert(key.clone(), value.clone());
                    }
                }
            }

            // Add custom metadata fields
            for (key, value) in &self.config.add_metadata {
                obj.insert(key.clone(), serde_json::Value::String(value.clone()));
            }

            // Add original topic for tracing
            obj.insert(
                "__original_topic".to_string(),
                serde_json::Value::String(ink.topic.clone()),
            );

            if obj.is_empty() {
                None
            } else {
                Some(serde_json::Value::Object(obj))
            }
        }
    }
}

#[async_trait]
impl Reflex for TentacleReflex {
    fn name(&self) -> &str {
        &self.name
    }

    fn r#type(&self) -> &str {
        "tentacle"
    }

    async fn handle(&self, ink: &Ink) -> Result<()> {
        // Check filters first
        if !self.passes_filters(ink) {
            debug!(
                "Tentacle reflex '{}' filtered out ink on topic: {}",
                self.name, ink.topic
            );
            return Ok(());
        }

        // Resolve target topic
        let target_topic = self.resolve_target_topic(ink);

        // Render body
        let body = self.render_body(ink);

        // Build metadata
        let metadata = self.build_metadata(ink);

        // Create forwarded Ink
        let forwarded_ink = Ink {
            id: ink.id,
            timestamp: ink.timestamp,
            topic: target_topic.clone(),
            source: ink.source.clone(),
            body,
            metadata,
            status: ink.status.clone(),
        };

        info!(
            "Tentacle reflex '{}' forwarding ink from '{}' to '{}'",
            self.name, ink.topic, forwarded_ink.topic
        );

        // Publish to event bus
        self.event_bus.publish(forwarded_ink);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_config() -> TentacleReflexConfig {
        TentacleReflexConfig {
            target_topic: "{{topic}}".to_string(),
            topic_mapping: HashMap::new(),
            topic_prefix_mapping: HashMap::new(),
            body_template: "{{body}}".to_string(),
            body_filter: None,
            topic_filter: None,
            preserve_metadata: true,
            metadata_keys: vec![],
            add_metadata: HashMap::new(),
        }
    }

    fn create_test_ink() -> Ink {
        Ink::new("/test/topic", "test", "test_source", "test body")
    }

    #[test]
    fn test_exact_topic_mapping() {
        let mut config = create_test_config();
        let mut mapping = HashMap::new();
        mapping.insert("/test/topic".to_string(), "/forwarded/topic".to_string());
        config.topic_mapping = mapping;

        let reflex = TentacleReflex::new("test".to_string(), config, Arc::new(EventBus::new(100)));

        let ink = create_test_ink();
        assert_eq!(reflex.resolve_target_topic(&ink), "/forwarded/topic");
    }

    #[test]
    fn test_prefix_topic_mapping() {
        let mut config = create_test_config();
        let mut prefix_mapping = HashMap::new();
        prefix_mapping.insert("/sensor/".to_string(), "/iot/sensor/".to_string());
        config.topic_prefix_mapping = prefix_mapping;

        let reflex = TentacleReflex::new("test".to_string(), config, Arc::new(EventBus::new(100)));

        let ink = Ink::new("/sensor/temp/1", "test", "test_source", "data");
        assert_eq!(reflex.resolve_target_topic(&ink), "/iot/sensor/temp/1");
    }

    #[test]
    fn test_longest_prefix_wins() {
        let mut config = create_test_config();
        let mut prefix_mapping = HashMap::new();
        prefix_mapping.insert("/a".to_string(), "/x".to_string());
        prefix_mapping.insert("/a/b".to_string(), "/y".to_string());
        config.topic_prefix_mapping = prefix_mapping;

        let reflex = TentacleReflex::new("test".to_string(), config, Arc::new(EventBus::new(100)));

        let ink = Ink::new("/a/b/c", "test", "test_source", "data");
        assert_eq!(reflex.resolve_target_topic(&ink), "/y/c");
    }

    #[test]
    fn test_template_topic() {
        let config = create_test_config();
        let reflex = TentacleReflex::new("test".to_string(), config, Arc::new(EventBus::new(100)));

        let ink = create_test_ink();
        // Default template is {{topic}}, so it should return the same topic
        assert_eq!(reflex.resolve_target_topic(&ink), "/test/topic");
    }

    #[test]
    fn test_body_template_rendering() {
        let mut config = create_test_config();
        config.body_template = "Topic: {{topic}}, Body: {{body}}".to_string();

        let reflex = TentacleReflex::new("test".to_string(), config, Arc::new(EventBus::new(100)));

        let ink = create_test_ink();
        assert_eq!(
            reflex.render_body(&ink),
            "Topic: /test/topic, Body: test body"
        );
    }

    #[test]
    fn test_metadata_in_template() {
        let mut config = create_test_config();
        config.body_template = "Value: {{metadata.temp}}".to_string();

        let reflex = TentacleReflex::new("test".to_string(), config, Arc::new(EventBus::new(100)));

        let ink = Ink::new("/test", "test", "test_source", "data")
            .with_metadata(serde_json::json!({ "temp": "42", "humidity": "60" }));
        assert_eq!(reflex.render_body(&ink), "Value: 42");
    }

    #[test]
    fn test_body_filter_pass() {
        let mut config = create_test_config();
        config.body_filter = Some(".*error.*".to_string());

        let reflex = TentacleReflex::new("test".to_string(), config, Arc::new(EventBus::new(100)));

        let ink = Ink::new("/test", "test", "test_source", "this is an error message");
        assert!(reflex.passes_filters(&ink));
    }

    #[test]
    fn test_body_filter_fail() {
        let mut config = create_test_config();
        config.body_filter = Some(".*error.*".to_string());

        let reflex = TentacleReflex::new("test".to_string(), config, Arc::new(EventBus::new(100)));

        let ink = Ink::new("/test", "test", "test_source", "this is fine");
        assert!(!reflex.passes_filters(&ink));
    }

    #[test]
    fn test_topic_filter_pass() {
        let mut config = create_test_config();
        config.topic_filter = Some("^/sensor/.*".to_string());

        let reflex = TentacleReflex::new("test".to_string(), config, Arc::new(EventBus::new(100)));

        let ink = Ink::new("/sensor/temp", "test", "test_source", "data");
        assert!(reflex.passes_filters(&ink));
    }

    #[test]
    fn test_topic_filter_fail() {
        let mut config = create_test_config();
        config.topic_filter = Some("^/sensor/.*".to_string());

        let reflex = TentacleReflex::new("test".to_string(), config, Arc::new(EventBus::new(100)));

        let ink = Ink::new("/other/topic", "test", "test_source", "data");
        assert!(!reflex.passes_filters(&ink));
    }

    #[test]
    fn test_preserve_metadata() {
        let mut config = create_test_config();
        config.preserve_metadata = true;
        let mut add_meta = HashMap::new();
        add_meta.insert("forwarded_by".to_string(), "tentacle-reflex".to_string());
        config.add_metadata = add_meta;

        let reflex = TentacleReflex::new("test".to_string(), config, Arc::new(EventBus::new(100)));

        let ink = Ink::new("/test", "test", "test_source", "data")
            .with_metadata(serde_json::json!({ "original_key": "original_value" }));

        let metadata = reflex.build_metadata(&ink);
        assert!(metadata.is_some());
        let meta = metadata.unwrap();
        let obj = meta.as_object().unwrap();
        assert_eq!(
            obj.get("original_key").unwrap().as_str().unwrap(),
            "original_value"
        );
        assert_eq!(
            obj.get("forwarded_by").unwrap().as_str().unwrap(),
            "tentacle-reflex"
        );
        assert_eq!(
            obj.get("__original_topic").unwrap().as_str().unwrap(),
            "/test"
        );
    }

    #[test]
    fn test_filter_metadata() {
        let mut config = create_test_config();
        config.preserve_metadata = false;
        config.metadata_keys = vec!["temp".to_string()];

        let reflex = TentacleReflex::new("test".to_string(), config, Arc::new(EventBus::new(100)));

        let ink = Ink::new("/test", "test", "test_source", "data").with_metadata(
            serde_json::json!({ "temp": "42", "humidity": "60", "pressure": "1013" }),
        );

        let metadata = reflex.build_metadata(&ink);
        assert!(metadata.is_some());
        let meta = metadata.unwrap();
        let obj = meta.as_object().unwrap();
        assert_eq!(obj.get("temp").unwrap().as_str().unwrap(), "42");
        assert!(obj.get("humidity").is_none());
        assert!(obj.get("pressure").is_none());
        assert!(obj.get("__original_topic").is_some());
    }

    #[test]
    fn test_topic_mapping_takes_precedence_over_template() {
        let mut config = create_test_config();
        config.target_topic = "/custom/{{topic}}".to_string();
        let mut mapping = HashMap::new();
        mapping.insert("/test/topic".to_string(), "/exact/match".to_string());
        config.topic_mapping = mapping;

        let reflex = TentacleReflex::new("test".to_string(), config, Arc::new(EventBus::new(100)));

        let ink = create_test_ink();
        assert_eq!(reflex.resolve_target_topic(&ink), "/exact/match");
    }

    #[test]
    fn test_template_with_metadata_topic() {
        let mut config = create_test_config();
        config.target_topic = "/forwarded/{{metadata.device}}".to_string();

        let reflex = TentacleReflex::new("test".to_string(), config, Arc::new(EventBus::new(100)));

        let ink = Ink::new("/test", "test", "test_source", "data")
            .with_metadata(serde_json::json!({ "device": "sensor-1" }));

        assert_eq!(reflex.resolve_target_topic(&ink), "/forwarded/sensor-1");
    }

    #[test]
    fn test_no_filters_passes_all() {
        let config = create_test_config();
        let reflex = TentacleReflex::new("test".to_string(), config, Arc::new(EventBus::new(100)));

        let ink = create_test_ink();
        assert!(reflex.passes_filters(&ink));
    }

    #[test]
    fn test_config_deserialization() {
        let yaml = r#"
            target_topic: "/forwarded/{{topic}}"
            body_template: "[{{topic}}] {{body}}"
            topic_mapping:
              "/sensor/temp": "/iot/temperature"
            topic_prefix_mapping:
              "/sensor/": "/iot/sensor/"
            body_filter: ".*alert.*"
            topic_filter: "^/sensor/.*"
            preserve_metadata: false
            metadata_keys:
              - "temp"
              - "humidity"
            add_metadata:
              forwarded_by: "tentacle-reflex"
        "#;

        let config: TentacleReflexConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.target_topic, "/forwarded/{{topic}}");
        assert_eq!(config.body_template, "[{{topic}}] {{body}}");
        assert_eq!(config.body_filter, Some(".*alert.*".to_string()));
        assert!(!config.preserve_metadata);
        assert_eq!(config.metadata_keys, vec!["temp", "humidity"]);
    }
}
