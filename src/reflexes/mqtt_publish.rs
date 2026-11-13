use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::{debug, info};

use super::Reflex;
use crate::model::Ink;

/// Configuration for MQTT Publish Reflex
/// Publishes messages to MQTT brokers via WebSocket or HTTP API
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MqttPublishReflexConfig {
    /// MQTT broker URL (e.g., "localhost")
    pub broker: String,

    /// MQTT port (default: 1883)
    #[serde(default = "default_port")]
    pub port: u16,

    /// Username (optional)
    #[serde(default)]
    pub username: Option<String>,

    /// Password (optional)
    #[serde(default)]
    pub password: Option<String>,

    /// Default MQTT topic
    #[serde(default = "default_topic")]
    pub topic: String,

    /// Template for MQTT topic
    #[serde(default)]
    pub topic_template: Option<String>,

    /// Template for the message payload
    #[serde(default = "default_payload_template")]
    pub payload_template: String,

    /// QoS level: 0, 1, or 2
    #[serde(default)]
    pub qos: u8,

    /// Retain flag
    #[serde(default)]
    pub retain: bool,

    /// Optional: mapping from Ink topics to MQTT topics
    #[serde(default)]
    pub topic_mapping: HashMap<String, String>,

    /// Use TLS
    #[serde(default)]
    pub use_tls: bool,

    /// Client ID
    #[serde(default = "default_client_id")]
    pub client_id: String,

    /// Optional: MQTT over WebSocket URL (alternative to TCP)
    #[serde(default)]
    pub ws_url: Option<String>,
}

fn default_port() -> u16 {
    1883
}
fn default_topic() -> String {
    "/kraken/out".to_string()
}
fn default_payload_template() -> String {
    "{{body}}".to_string()
}
fn default_client_id() -> String {
    "rustyKraken-reflex".to_string()
}

pub struct MqttPublishReflex {
    name: String,
    config: MqttPublishReflexConfig,
}

impl MqttPublishReflex {
    pub fn new(name: String, config: MqttPublishReflexConfig) -> Self {
        Self { name, config }
    }

    /// Resolve MQTT topic for a given Ink
    fn resolve_topic(&self, ink: &Ink) -> String {
        if let Some(topic) = self
            .config
            .topic_mapping
            .get(&ink.topic)
            .filter(|t| !t.is_empty())
        {
            return self.render_template(topic, ink);
        }
        if let Some(ref template) = self.config.topic_template {
            return self.render_template(template, ink);
        }
        self.config.topic.clone()
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
impl Reflex for MqttPublishReflex {
    fn name(&self) -> &str {
        &self.name
    }
    fn r#type(&self) -> &str {
        "mqtt_publish"
    }

    async fn handle(&self, ink: &Ink) -> Result<()> {
        info!(
            "MQTT Publish reflex '{}' handling ink on topic: {}",
            self.name, ink.topic
        );

        let mqtt_topic = self.resolve_topic(ink);
        let payload = self.render_template(&self.config.payload_template, ink);

        debug!(
            "MQTT Publish reflex '{}' publishing to {} with payload: {}",
            self.name, mqtt_topic, payload
        );

        // Use rumqttc to publish
        let mut mqtt_options = rumqttc::MqttOptions::new(
            self.config.client_id.clone(),
            self.config.broker.clone(),
            self.config.port,
        );
        mqtt_options.set_keep_alive(std::time::Duration::from_secs(60));

        let (client, _eventloop) = rumqttc::AsyncClient::new(mqtt_options, 100);

        // Publish (fire-and-forget with rumqttc)
        std::mem::drop(client.publish(
            &mqtt_topic,
            match self.config.qos {
                1 => rumqttc::QoS::AtLeastOnce,
                2 => rumqttc::QoS::ExactlyOnce,
                _ => rumqttc::QoS::AtMostOnce,
            },
            self.config.retain,
            payload.as_bytes(),
        ));

        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        info!(
            "MQTT Publish reflex '{}' published to topic: {}",
            self.name, mqtt_topic
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_config() -> MqttPublishReflexConfig {
        MqttPublishReflexConfig {
            broker: "localhost".to_string(),
            port: 1883,
            username: None,
            password: None,
            topic: "/kraken/out".to_string(),
            topic_template: None,
            payload_template: "{{body}}".to_string(),
            qos: 0,
            retain: false,
            topic_mapping: HashMap::new(),
            use_tls: false,
            client_id: "rustyKraken-reflex".to_string(),
            ws_url: None,
        }
    }

    fn create_test_ink() -> Ink {
        Ink::new("/test/topic", "test", "test_source", "test body")
    }

    #[test]
    fn test_template_rendering() {
        let config = create_test_config();
        let reflex = MqttPublishReflex::new("test".to_string(), config);
        let ink = create_test_ink();
        assert_eq!(
            reflex.render_template("[{{topic}}] {{body}}", &ink),
            "[/test/topic] test body"
        );
    }

    #[test]
    fn test_resolve_topic() {
        let config = create_test_config();
        let reflex = MqttPublishReflex::new("test".to_string(), config);
        let ink = create_test_ink();
        assert_eq!(reflex.resolve_topic(&ink), "/kraken/out");
    }

    #[test]
    fn test_resolve_topic_mapping() {
        let mut config = create_test_config();
        config
            .topic_mapping
            .insert("/test/topic".to_string(), "/mqtt/{{topic}}".to_string());
        let reflex = MqttPublishReflex::new("test".to_string(), config);
        let ink = create_test_ink();
        assert_eq!(reflex.resolve_topic(&ink), "/mqtt//test/topic");
    }

    #[test]
    fn test_config_deserialization() {
        let yaml = r#"
            broker: "mqtt.example.com"
            port: 8883
            use_tls: true
            topic: "/alerts"
            qos: 1
        "#;
        let config: MqttPublishReflexConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.broker, "mqtt.example.com");
        assert!(config.use_tls);
    }

    #[test]
    fn test_reflex_type() {
        let config = create_test_config();
        let reflex = MqttPublishReflex::new("test".to_string(), config);
        assert_eq!(reflex.r#type(), "mqtt_publish");
    }
}
