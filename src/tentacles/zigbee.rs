use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::RwLock;
use std::time::Duration;
use tracing::{debug, error, info};

use super::Tentacle;
use crate::eventbus::EventBus;
use crate::model::Ink;

/// Zigbee-Kommunikationsmodus
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "lowercase")]
pub enum ZigbeeMode {
    /// Via Zigbee2MQTT Bridge über MQTT
    #[default]
    Zigbee2Mqtt,
    /// Native Zigbee (theoretical, requires hardware)
    Native,
}

/// Konfiguration für den Zigbee Tentacle
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ZigbeeTentacleConfig {
    /// Kommunikationsmodus
    #[serde(default)]
    pub mode: ZigbeeMode,
    /// MQTT-Server für Zigbee2MQTT
    #[serde(default = "default_mqtt_broker")]
    pub mqtt_broker: String,
    /// MQTT-Port
    #[serde(default = "default_mqtt_port")]
    pub mqtt_port: u16,
    /// MQTT-Topic für Zigbee2MQTT
    #[serde(default = "default_mqtt_topic")]
    pub mqtt_topic: String,
    /// MQTT-Username (optional)
    pub mqtt_username: Option<String>,
    /// MQTT-Password (optional)
    pub mqtt_password: Option<String>,
    /// Topic für Zigbee Events
    #[serde(default = "default_topic")]
    pub topic: String,
    /// Zu überwachende Geräte (leer = alle)
    #[serde(default)]
    pub watched_devices: Vec<String>,
    /// Zu überwachende Modell-IDs (leer = alle)
    #[serde(default)]
    pub watched_models: Vec<String>,
    /// Polling-Interval in Sekunden (für native Mode)
    #[serde(default = "default_poll_interval")]
    pub poll_interval_secs: u64,
    /// Timeout in Sekunden
    #[serde(default = "default_timeout")]
    pub timeout_secs: u64,
}

fn default_mqtt_broker() -> String {
    "127.0.0.1".to_string()
}

fn default_mqtt_port() -> u16 {
    1883
}

fn default_mqtt_topic() -> String {
    "zigbee2mqtt/#".to_string()
}

fn default_topic() -> String {
    "/zigbee/devices".to_string()
}

fn default_poll_interval() -> u64 {
    30
}

fn default_timeout() -> u64 {
    30
}

/// Zigbee-Geräte-Daten
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ZigbeeDevice {
    pub ieee_address: String,
    pub friendly_name: String,
    pub model: Option<String>,
    pub manufacturer: Option<String>,
    pub type_: String,
    pub state: serde_json::Value,
    pub battery: Option<u32>,
    pub link_quality: Option<u32>,
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

pub struct ZigbeeTentacle {
    name: String,
    config: ZigbeeTentacleConfig,
    last_states: Arc<RwLock<HashMap<String, serde_json::Value>>>,
}

impl ZigbeeTentacle {
    pub fn new(name: String, config: ZigbeeTentacleConfig) -> Self {
        Self {
            name,
            config,
            last_states: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    fn matches_filter(&self, device: &ZigbeeDevice) -> bool {
        if self.config.watched_devices.is_empty() && self.config.watched_models.is_empty() {
            return true;
        }

        if !self.config.watched_devices.is_empty() {
            let has_device = self
                .config
                .watched_devices
                .iter()
                .any(|d| d == &device.friendly_name || d == &device.ieee_address);
            if !has_device {
                return false;
            }
        }

        if !self.config.watched_models.is_empty() {
            if let Some(ref model) = device.model {
                if !self
                    .config
                    .watched_models
                    .iter()
                    .any(|m| model.to_lowercase().contains(&m.to_lowercase()))
                {
                    return false;
                }
            } else {
                return false;
            }
        }

        true
    }

    fn create_ink(&self, device: &ZigbeeDevice) -> Ink {
        let mut meta_map = serde_json::Map::new();
        meta_map.insert(
            "ieee_address".into(),
            serde_json::json!(&device.ieee_address),
        );
        meta_map.insert(
            "friendly_name".into(),
            serde_json::json!(&device.friendly_name),
        );
        meta_map.insert("type".into(), serde_json::json!(&device.type_));
        meta_map.insert("state".into(), device.state.clone());

        if let Some(ref model) = device.model {
            meta_map.insert("model".into(), serde_json::json!(model));
        }
        if let Some(ref manufacturer) = device.manufacturer {
            meta_map.insert("manufacturer".into(), serde_json::json!(manufacturer));
        }
        if let Some(battery) = device.battery {
            meta_map.insert("battery".into(), serde_json::json!(battery));
        }
        if let Some(lq) = device.link_quality {
            meta_map.insert("link_quality".into(), serde_json::json!(lq));
        }

        let topic = format!("{}/{}", self.config.topic, device.friendly_name);

        Ink::new(
            &topic,
            "zigbee",
            &self.name,
            format!("Zigbee {}: state={:?}", device.friendly_name, device.state),
        )
        .with_metadata(serde_json::Value::Object(meta_map))
    }

    #[allow(dead_code)]
    /// Parse Zigbee2MQTT JSON message
    fn parse_z2m_message(&self, payload: &str) -> Option<ZigbeeDevice> {
        let json: serde_json::Value = serde_json::from_str(payload).ok()?;

        let ieee = json["ieee_address"]
            .as_str()
            .or_else(|| json["ieeeAddr"].as_str())?
            .to_string();

        let name = json["friendly_name"].as_str().unwrap_or(&ieee).to_string();

        let model = json["model"].as_str().map(|s| s.to_string());
        let manufacturer = json["manufacturer"].as_str().map(|s| s.to_string());
        let type_ = json["type"].as_str().unwrap_or("Router").to_string();

        // Extract state object (excluding metadata fields)
        let mut state_map = serde_json::Map::new();
        if let Some(obj) = json.as_object() {
            for (key, value) in obj {
                match key.as_str() {
                    "ieee_address" | "ieeeAddr" | "friendly_name" | "model" | "manufacturer"
                    | "type" | "date" | "definition" => continue,
                    _ => {
                        state_map.insert(key.clone(), value.clone());
                    }
                }
            }
        }

        let battery = json["battery"].as_u64().map(|v| v as u32);
        let link_quality = json["linkquality"].as_u64().map(|v| v as u32);

        Some(ZigbeeDevice {
            ieee_address: ieee,
            friendly_name: name,
            model,
            manufacturer,
            type_,
            state: serde_json::Value::Object(state_map),
            battery,
            link_quality,
            timestamp: chrono::Utc::now(),
        })
    }

    /// Simulate Zigbee devices for testing
    fn simulate_devices(&self) -> Vec<ZigbeeDevice> {
        vec![
            ZigbeeDevice {
                ieee_address: "00:15:8d:00:02:a1:b2:c3".to_string(),
                friendly_name: "Living Room Sensor".to_string(),
                model: Some("SMS-4ZB".to_string()),
                manufacturer: Some("Develco".to_string()),
                type_: "EndDevice".to_string(),
                state: serde_json::json!({"temperature": 22.5, "humidity": 55}),
                battery: Some(85),
                link_quality: Some(200),
                timestamp: chrono::Utc::now(),
            },
            ZigbeeDevice {
                ieee_address: "00:15:8d:00:03:b2:c3:d4".to_string(),
                friendly_name: "Kitchen Light".to_string(),
                model: Some("E1746".to_string()),
                manufacturer: Some("IKEA".to_string()),
                type_: "Router".to_string(),
                state: serde_json::json!({"state": "ON", "brightness": 254}),
                battery: None,
                link_quality: Some(180),
                timestamp: chrono::Utc::now(),
            },
        ]
    }

    async fn poll_cycle(&self, event_bus: &EventBus) -> Result<usize> {
        let mut count = 0;

        // In Zigbee2MQTT mode, we rely on MQTT subscription
        // In native mode, we simulate polling
        let devices = if self.config.mode == ZigbeeMode::Native {
            debug!("Zigbee Tentacle '{}': native mode polling", self.name);
            self.simulate_devices()
        } else {
            // Zigbee2MQTT mode: try connecting to MQTT
            debug!(
                "Zigbee Tentacle '{}': waiting for Zigbee2MQTT messages on {}",
                self.name, self.config.mqtt_topic
            );
            Vec::new()
        };

        for device in &devices {
            if self.matches_filter(device) {
                let key = device.ieee_address.clone();
                let changed = {
                    let last = self.last_states.read().unwrap();
                    match last.get(&key) {
                        Some(prev) => prev != &device.state,
                        None => true,
                    }
                };

                if changed {
                    {
                        let mut last = self.last_states.write().unwrap();
                        last.insert(key, device.state.clone());
                    }
                    let ink = self.create_ink(device);
                    event_bus.publish(ink);
                    count += 1;
                }
            }
        }

        Ok(count)
    }
}

#[async_trait]
impl Tentacle for ZigbeeTentacle {
    fn name(&self) -> &str {
        &self.name
    }

    fn r#type(&self) -> &str {
        "zigbee"
    }

    async fn start(&self, event_bus: Arc<EventBus>) -> Result<()> {
        info!(
            "Zigbee Tentacle '{}' starting (mode: {:?}, broker: {}:{})",
            self.name, self.config.mode, self.config.mqtt_broker, self.config.mqtt_port
        );

        let config = self.config.clone();
        let tentacle_name = self.name.clone();
        let event_bus_clone = Arc::clone(&event_bus);
        let last_states = Arc::clone(&self.last_states);

        tokio::spawn(async move {
            let tentacle = ZigbeeTentacle {
                name: tentacle_name.clone(),
                config,
                last_states,
            };

            loop {
                match tentacle.poll_cycle(&event_bus_clone).await {
                    Ok(count) => {
                        if count > 0 {
                            info!(
                                "Zigbee Tentacle '{}': published {} device updates",
                                tentacle_name, count
                            );
                        }
                    }
                    Err(e) => {
                        error!("Zigbee Tentacle '{}': poll failed: {}", tentacle_name, e);
                    }
                }

                tokio::time::sleep(Duration::from_secs(tentacle.config.poll_interval_secs)).await;
            }
        });

        // Keep the main task alive
        loop {
            tokio::time::sleep(Duration::from_secs(60)).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::appcontext::AppContext;

    fn make_config() -> ZigbeeTentacleConfig {
        ZigbeeTentacleConfig {
            mode: ZigbeeMode::Zigbee2Mqtt,
            mqtt_broker: "127.0.0.1".to_string(),
            mqtt_port: 1883,
            mqtt_topic: "zigbee2mqtt/#".to_string(),
            mqtt_username: None,
            mqtt_password: None,
            topic: "/zigbee/devices".to_string(),
            watched_devices: Vec::new(),
            watched_models: Vec::new(),
            poll_interval_secs: 30,
            timeout_secs: 30,
        }
    }

    fn make_device() -> ZigbeeDevice {
        ZigbeeDevice {
            ieee_address: "00:15:8d:00:02:a1:b2:c3".to_string(),
            friendly_name: "Test Sensor".to_string(),
            model: Some("SMS-4ZB".to_string()),
            manufacturer: Some("Develco".to_string()),
            type_: "EndDevice".to_string(),
            state: serde_json::json!({"temperature": 22.5}),
            battery: Some(85),
            link_quality: Some(200),
            timestamp: chrono::Utc::now(),
        }
    }

    #[test]
    fn test_tentacle_creation() {
        let config = make_config();
        let tentacle = ZigbeeTentacle::new("test".to_string(), config);
        assert_eq!(tentacle.name(), "test");
        assert_eq!(tentacle.r#type(), "zigbee");
    }

    #[test]
    fn test_filter_by_device_name() {
        let mut config = make_config();
        config.watched_devices = vec!["Test Sensor".to_string()];
        let tentacle = ZigbeeTentacle::new("test".to_string(), config);
        let device = make_device();
        assert!(tentacle.matches_filter(&device));
    }

    #[test]
    fn test_filter_by_device_ieee() {
        let mut config = make_config();
        config.watched_devices = vec!["00:15:8d:00:02:a1:b2:c3".to_string()];
        let tentacle = ZigbeeTentacle::new("test".to_string(), config);
        let device = make_device();
        assert!(tentacle.matches_filter(&device));
    }

    #[test]
    fn test_filter_by_model() {
        let mut config = make_config();
        config.watched_models = vec!["SMS".to_string()];
        let tentacle = ZigbeeTentacle::new("test".to_string(), config);
        let device = make_device();
        assert!(tentacle.matches_filter(&device));
    }

    #[test]
    fn test_filter_no_match() {
        let mut config = make_config();
        config.watched_devices = vec!["Other Device".to_string()];
        let tentacle = ZigbeeTentacle::new("test".to_string(), config);
        let device = make_device();
        assert!(!tentacle.matches_filter(&device));
    }

    #[test]
    fn test_parse_z2m_message() {
        let config = make_config();
        let tentacle = ZigbeeTentacle::new("test".to_string(), config);
        let payload = r#"{
            "ieee_address": "00:15:8d:00:02:a1:b2:c3",
            "friendly_name": "Test Sensor",
            "model": "SMS-4ZB",
            "manufacturer": "Develco",
            "type": "EndDevice",
            "temperature": 22.5,
            "humidity": 55,
            "battery": 85,
            "linkquality": 200
        }"#;
        let device = tentacle.parse_z2m_message(payload).unwrap();
        assert_eq!(device.ieee_address, "00:15:8d:00:02:a1:b2:c3");
        assert_eq!(device.friendly_name, "Test Sensor");
        assert_eq!(device.model, Some("SMS-4ZB".to_string()));
        assert_eq!(device.battery, Some(85));
        assert_eq!(device.link_quality, Some(200));
        let state = device.state.as_object().unwrap();
        assert_eq!(state.get("temperature").unwrap().as_f64().unwrap(), 22.5);
        assert_eq!(state.get("humidity").unwrap().as_f64().unwrap(), 55.0);
    }

    #[test]
    fn test_ink_creation() {
        let config = make_config();
        let tentacle = ZigbeeTentacle::new("test".to_string(), config);
        let device = make_device();
        let ink = tentacle.create_ink(&device);
        assert!(ink.topic.contains("/zigbee/devices"));
        assert_eq!(ink.source.r#type, "zigbee");
        assert!(ink.body.contains("Test Sensor"));
    }

    #[test]
    fn test_simulate_devices() {
        let config = make_config();
        let tentacle = ZigbeeTentacle::new("test".to_string(), config);
        let devices = tentacle.simulate_devices();
        assert_eq!(devices.len(), 2);
        assert_eq!(devices[0].friendly_name, "Living Room Sensor");
        assert_eq!(devices[1].friendly_name, "Kitchen Light");
    }

    #[test]
    fn test_z2m_message_with_ieee_addr() {
        let config = make_config();
        let tentacle = ZigbeeTentacle::new("test".to_string(), config);
        let payload = r#"{
            "ieeeAddr": "00:15:8d:00:03:b2:c3:d4",
            "friendly_name": "Kitchen Light",
            "state": "ON",
            "brightness": 254
        }"#;
        let device = tentacle.parse_z2m_message(payload).unwrap();
        assert_eq!(device.ieee_address, "00:15:8d:00:03:b2:c3:d4");
        assert_eq!(device.friendly_name, "Kitchen Light");
    }

    #[test]
    fn test_mode_enum() {
        assert_eq!(ZigbeeMode::Zigbee2Mqtt, ZigbeeMode::default());
    }
}
