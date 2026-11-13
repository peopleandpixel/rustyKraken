use anyhow::{Context, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::{debug, info};

use super::Reflex;
use crate::model::Ink;

/// Configuration for Zigbee2MQTT Reflex
/// Controls Zigbee devices via Zigbee2MQTT MQTT bridge
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Zigbee2MqttReflexConfig {
    /// MQTT broker URL
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

    /// Default device friendly name
    #[serde(default)]
    pub device: Option<String>,

    /// Template for device name
    #[serde(default)]
    pub device_template: Option<String>,

    /// Default state/command (e.g., "ON", "OFF", "TOGGLE")
    #[serde(default)]
    pub state: Option<String>,

    /// Template for state/command
    #[serde(default)]
    pub state_template: Option<String>,

    /// Optional: mapping from Ink topics to devices
    #[serde(default)]
    pub topic_device_mapping: HashMap<String, String>,

    /// Use TLS
    #[serde(default)]
    pub use_tls: bool,

    /// Client ID
    #[serde(default = "default_client_id")]
    pub client_id: String,
}

fn default_port() -> u16 { 1883 }
fn default_client_id() -> String { "rustyKraken-z2m".to_string() }

pub struct Zigbee2MqttReflex {
    name: String,
    config: Zigbee2MqttReflexConfig,
}

impl Zigbee2MqttReflex {
    pub fn new(name: String, config: Zigbee2MqttReflexConfig) -> Self {
        Self { name, config }
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

    fn resolve_device(&self, ink: &Ink) -> Result<String> {
        if let Some(device) = self
            .config
            .topic_device_mapping
            .get(&ink.topic)
            .filter(|d| !d.is_empty())
        {
            return Ok(device.clone());
        }
        if let Some(ref template) = self.config.device_template {
            return Ok(self.render_template(template, ink));
        }
        self.config
            .device
            .clone()
            .context("Zigbee2MQTT device is required - set device, device_template, or topic_device_mapping")
    }

    fn resolve_state(&self, ink: &Ink) -> String {
        if let Some(ref template) = self.config.state_template {
            return self.render_template(template, ink);
        }
        self.config.state.clone().unwrap_or_else(|| ink.body.clone())
    }
}

#[async_trait]
impl Reflex for Zigbee2MqttReflex {
    fn name(&self) -> &str { &self.name }
    fn r#type(&self) -> &str { "zigbee2mqtt" }

    async fn handle(&self, ink: &Ink) -> Result<()> {
        info!("Zigbee2MQTT reflex '{}' handling ink on topic: {}", self.name, ink.topic);

        let device = self.resolve_device(ink)?;
        let state = self.resolve_state(ink);

        debug!(
            "Zigbee2MQTT reflex '{}' device={} state={} on {}",
            self.name, device, state, self.config.broker
        );

        let publish_topic = format!("zigbee2mqtt/{}/set", device);

        let payload = if state == "ON" || state == "OFF" || state == "TOGGLE" {
            serde_json::json!({ "state": state })
        } else {
            match serde_json::from_str::<serde_json::Value>(&state) {
                Ok(json) => json,
                Err(_) => serde_json::json!({ "state": state }),
            }
        };

        let payload_str = serde_json::to_string(&payload)?;

        // Use rumqttc to publish
        let mut mqtt_options = rumqttc::MqttOptions::new(
            self.config.client_id.clone(),
            self.config.broker.clone(),
            self.config.port,
        );
        mqtt_options.set_keep_alive(std::time::Duration::from_secs(60));

        let (client, _eventloop) = rumqttc::AsyncClient::new(mqtt_options, 100);

        std::mem::drop(client.publish(
            &publish_topic, rumqttc::QoS::AtLeastOnce, false, payload_str.as_bytes()
        ));

        tokio::time::sleep(std::time::Duration::from_millis(200)).await;

        info!(
            "Zigbee2MQTT reflex '{}' sent state '{}' to device {}",
            self.name, state, device
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_config() -> Zigbee2MqttReflexConfig {
        Zigbee2MqttReflexConfig {
            broker: "localhost".to_string(),
            port: 1883,
            username: None,
            password: None,
            device: Some("LivingRoom_Bulb".to_string()),
            device_template: None,
            state: Some("ON".to_string()),
            state_template: None,
            topic_device_mapping: HashMap::new(),
            use_tls: false,
            client_id: "rustyKraken-z2m".to_string(),
        }
    }

    fn create_test_ink() -> Ink {
        Ink::new("/test/topic", "test", "test_source", "test body")
    }

    #[test]
    fn test_template_rendering() {
        let config = create_test_config();
        let reflex = Zigbee2MqttReflex::new("test".to_string(), config);
        let ink = create_test_ink();
        assert_eq!(reflex.render_template("{{topic}}-{{body}}", &ink), "/test/topic-test body");
    }

    #[test]
    fn test_resolve_device() {
        let config = create_test_config();
        let reflex = Zigbee2MqttReflex::new("test".to_string(), config);
        let ink = create_test_ink();
        assert_eq!(reflex.resolve_device(&ink).unwrap(), "LivingRoom_Bulb");
    }

    #[test]
    fn test_resolve_state() {
        let config = create_test_config();
        let reflex = Zigbee2MqttReflex::new("test".to_string(), config);
        let ink = create_test_ink();
        assert_eq!(reflex.resolve_state(&ink), "ON");
    }

    #[test]
    fn test_config_deserialization() {
        let yaml = r#"
            broker: "mqtt.example.com"
            device: "Bedroom_Switch"
            state: "OFF"
        "#;
        let config: Zigbee2MqttReflexConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.broker, "mqtt.example.com");
    }

    #[test]
    fn test_reflex_type() {
        let config = create_test_config();
        let reflex = Zigbee2MqttReflex::new("test".to_string(), config);
        assert_eq!(reflex.r#type(), "zigbee2mqtt");
    }
}
