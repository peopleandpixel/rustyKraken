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

/// Z-Wave-Kommunikationsmodus
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "lowercase")]
pub enum ZwaveMode {
    /// Via Z-Wave JS über WebSocket
    #[default]
    ZwaveJs,
    /// Via OpenZWave
    OpenZwave,
}

/// Konfiguration für den Z-Wave Tentacle
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ZwaveTentacleConfig {
    /// Kommunikationsmodus
    #[serde(default)]
    pub mode: ZwaveMode,
    /// Server-Adresse (z.B. Home Assistant Z-Wave JS)
    #[serde(default = "default_server")]
    pub server: String,
    /// Port
    #[serde(default = "default_port")]
    pub port: u16,
    /// API-Token (optional)
    pub api_token: Option<String>,
    /// Topic für Z-Wave Events
    #[serde(default = "default_topic")]
    pub topic: String,
    /// Zu überwachende Geräte-IDs (leer = alle)
    #[serde(default)]
    pub watched_nodes: Vec<u32>,
    /// Zu überwachende Wert-Typen (leer = alle)
    #[serde(default)]
    pub watched_value_types: Vec<String>,
    /// Polling-Interval in Sekunden
    #[serde(default = "default_poll_interval")]
    pub poll_interval_secs: u64,
    /// Timeout in Sekunden
    #[serde(default = "default_timeout")]
    pub timeout_secs: u64,
}

fn default_server() -> String {
    "127.0.0.1".to_string()
}

fn default_port() -> u16 {
    3000
}

fn default_topic() -> String {
    "/zwave/devices".to_string()
}

fn default_poll_interval() -> u64 {
    30
}

fn default_timeout() -> u64 {
    30
}

/// Z-Wave-Geräte-Daten
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ZwaveNode {
    pub node_id: u32,
    pub product_name: String,
    pub manufacturer: String,
    pub device_class: String,
    pub is_awake: bool,
    pub is_ready: bool,
    pub values: HashMap<String, serde_json::Value>,
    pub battery_level: Option<u32>,
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

pub struct ZwaveTentacle {
    name: String,
    config: ZwaveTentacleConfig,
    last_values: Arc<RwLock<HashMap<u32, serde_json::Value>>>,
}

impl ZwaveTentacle {
    pub fn new(name: String, config: ZwaveTentacleConfig) -> Self {
        Self {
            name,
            config,
            last_values: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    fn matches_filter(&self, node: &ZwaveNode) -> bool {
        if self.config.watched_nodes.is_empty() && self.config.watched_value_types.is_empty() {
            return true;
        }

        if !self.config.watched_nodes.is_empty()
            && !self.config.watched_nodes.contains(&node.node_id)
        {
            return false;
        }

        if !self.config.watched_value_types.is_empty() {
            let has_type = node.values.keys().any(|key| {
                self.config
                    .watched_value_types
                    .iter()
                    .any(|wt| key.to_lowercase().contains(&wt.to_lowercase()))
            });
            if !has_type {
                return false;
            }
        }

        true
    }

    fn create_ink(&self, node: &ZwaveNode) -> Ink {
        let mut meta_map = serde_json::Map::new();
        meta_map.insert("node_id".into(), serde_json::json!(node.node_id));
        meta_map.insert("product_name".into(), serde_json::json!(&node.product_name));
        meta_map.insert("manufacturer".into(), serde_json::json!(&node.manufacturer));
        meta_map.insert("device_class".into(), serde_json::json!(&node.device_class));
        meta_map.insert("is_awake".into(), serde_json::json!(node.is_awake));
        meta_map.insert("is_ready".into(), serde_json::json!(node.is_ready));
        meta_map.insert("values".into(), serde_json::json!(&node.values));

        if let Some(battery) = node.battery_level {
            meta_map.insert("battery_level".into(), serde_json::json!(battery));
        }

        let topic = format!("{}/node_{}", self.config.topic, node.node_id);

        Ink::new(
            &topic,
            "zwave",
            &self.name,
            format!(
                "Z-Wave node {}: {} values={:?}",
                node.node_id, node.product_name, node.values
            ),
        )
        .with_metadata(serde_json::Value::Object(meta_map))
    }

    /// Simulate Z-Wave nodes for testing
    fn simulate_nodes(&self) -> Vec<ZwaveNode> {
        let mut node1_values = HashMap::new();
        node1_values.insert("temperature".to_string(), serde_json::json!(21.5));
        node1_values.insert("humidity".to_string(), serde_json::json!(45));

        let mut node2_values = HashMap::new();
        node2_values.insert("current_value".to_string(), serde_json::json!(128));
        node2_values.insert("target_value".to_string(), serde_json::json!(128));

        vec![
            ZwaveNode {
                node_id: 1,
                product_name: "HS-WD100".to_string(),
                manufacturer: "HEIMAN".to_string(),
                device_class: "Multichannel".to_string(),
                is_awake: true,
                is_ready: true,
                values: node1_values,
                battery_level: Some(100),
                timestamp: chrono::Utc::now(),
            },
            ZwaveNode {
                node_id: 2,
                product_name: "Dimmer Switch".to_string(),
                manufacturer: "AEON Labs".to_string(),
                device_class: "MultilevelSwitch".to_string(),
                is_awake: true,
                is_ready: true,
                values: node2_values,
                battery_level: None,
                timestamp: chrono::Utc::now(),
            },
        ]
    }

    async fn poll_cycle(&self, event_bus: &EventBus) -> Result<usize> {
        let mut count = 0;

        debug!("Z-Wave Tentacle '{}': polling cycle", self.name);

        // In real implementation, connect to Z-Wave JS WebSocket or OpenZWave
        // For now, simulate nodes
        let nodes = self.simulate_nodes();

        for node in &nodes {
            if self.matches_filter(node) {
                let current_json = serde_json::to_string(&node.values).unwrap_or_default();
                let changed = {
                    let last = self.last_values.read().unwrap();
                    match last.get(&node.node_id) {
                        Some(prev) => prev.as_str() != Some(&current_json),
                        None => true,
                    }
                };

                if changed {
                    {
                        let mut last = self.last_values.write().unwrap();
                        last.insert(node.node_id, serde_json::json!(current_json));
                    }
                    let ink = self.create_ink(node);
                    event_bus.publish(ink);
                    count += 1;
                }
            }
        }

        Ok(count)
    }
}

#[async_trait]
impl Tentacle for ZwaveTentacle {
    fn name(&self) -> &str {
        &self.name
    }

    fn r#type(&self) -> &str {
        "zwave"
    }

    async fn start(&self, event_bus: Arc<EventBus>) -> Result<()> {
        info!(
            "Z-Wave Tentacle '{}' starting (mode: {:?}, server: {}:{})",
            self.name, self.config.mode, self.config.server, self.config.port
        );

        let config = self.config.clone();
        let tentacle_name = self.name.clone();
        let event_bus_clone = Arc::clone(&event_bus);
        let last_values = Arc::clone(&self.last_values);

        tokio::spawn(async move {
            let tentacle = ZwaveTentacle {
                name: tentacle_name.clone(),
                config,
                last_values,
            };

            loop {
                match tentacle.poll_cycle(&event_bus_clone).await {
                    Ok(count) => {
                        if count > 0 {
                            info!(
                                "Z-Wave Tentacle '{}': published {} node updates",
                                tentacle_name, count
                            );
                        }
                    }
                    Err(e) => {
                        error!("Z-Wave Tentacle '{}': poll failed: {}", tentacle_name, e);
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

    fn make_config() -> ZwaveTentacleConfig {
        ZwaveTentacleConfig {
            mode: ZwaveMode::ZwaveJs,
            server: "127.0.0.1".to_string(),
            port: 3000,
            api_token: None,
            topic: "/zwave/devices".to_string(),
            watched_nodes: Vec::new(),
            watched_value_types: Vec::new(),
            poll_interval_secs: 30,
            timeout_secs: 30,
        }
    }

    #[test]
    fn test_tentacle_creation() {
        let config = make_config();
        let tentacle = ZwaveTentacle::new("test".to_string(), config);
        assert_eq!(tentacle.name(), "test");
        assert_eq!(tentacle.r#type(), "zwave");
    }

    #[test]
    fn test_filter_by_node_id() {
        let mut config = make_config();
        config.watched_nodes = vec![1];
        let tentacle = ZwaveTentacle::new("test".to_string(), config);
        let nodes = tentacle.simulate_nodes();
        assert!(tentacle.matches_filter(&nodes[0]));
        assert!(!tentacle.matches_filter(&nodes[1]));
    }

    #[test]
    fn test_filter_by_value_type() {
        let mut config = make_config();
        config.watched_value_types = vec!["temperature".to_string()];
        let tentacle = ZwaveTentacle::new("test".to_string(), config);
        let nodes = tentacle.simulate_nodes();
        assert!(tentacle.matches_filter(&nodes[0]));
        assert!(!tentacle.matches_filter(&nodes[1]));
    }

    #[test]
    fn test_filter_no_match() {
        let mut config = make_config();
        config.watched_nodes = vec![99];
        let tentacle = ZwaveTentacle::new("test".to_string(), config);
        let nodes = tentacle.simulate_nodes();
        assert!(!tentacle.matches_filter(&nodes[0]));
    }

    #[test]
    fn test_ink_creation() {
        let config = make_config();
        let tentacle = ZwaveTentacle::new("test".to_string(), config);
        let nodes = tentacle.simulate_nodes();
        let ink = tentacle.create_ink(&nodes[0]);
        assert!(ink.topic.contains("/zwave/devices"));
        assert_eq!(ink.source.r#type, "zwave");
        assert!(ink.body.contains("HS-WD100"));
    }

    #[test]
    fn test_simulate_nodes() {
        let config = make_config();
        let tentacle = ZwaveTentacle::new("test".to_string(), config);
        let nodes = tentacle.simulate_nodes();
        assert_eq!(nodes.len(), 2);
        assert_eq!(nodes[0].node_id, 1);
        assert_eq!(nodes[1].node_id, 2);
    }

    #[test]
    fn test_mode_enum() {
        assert_eq!(ZwaveMode::ZwaveJs, ZwaveMode::default());
    }

    #[test]
    fn test_node_with_battery() {
        let config = make_config();
        let tentacle = ZwaveTentacle::new("test".to_string(), config);
        let nodes = tentacle.simulate_nodes();
        assert_eq!(nodes[0].battery_level, Some(100));
        assert_eq!(nodes[1].battery_level, None);
    }
}
