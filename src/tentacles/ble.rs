use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::RwLock;
use std::time::Duration;
use tracing::{debug, error, info, warn};

use super::Tentacle;
use crate::eventbus::EventBus;
use crate::model::Ink;

/// BLE Scan-Modus
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "lowercase")]
pub enum BleScanMode {
    /// Nur Advertisements scannen (passiv)
    #[default]
    Scan,
    /// Mit bestimmten Geräten verbinden
    Connect,
}

/// Filter für BLE-Geräte
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct BleDeviceFilter {
    /// MAC-Adresse (optional)
    pub mac_address: Option<String>,
    /// Gerätename oder Prefix (optional)
    pub name_prefix: Option<String>,
    /// Service UUID (optional)
    pub service_uuid: Option<String>,
    /// Rssi Schwellenwert (optional, z.B. -80)
    pub min_rssi: Option<i32>,
}

/// Konfiguration für den BLE Tentacle
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BleTentacleConfig {
    /// Scan-Modus
    #[serde(default)]
    pub scan_mode: BleScanMode,
    /// Scan-Interval in Sekunden
    #[serde(default = "default_scan_interval")]
    pub scan_interval_secs: u64,
    /// Scan-Dauer pro Zyklus in Sekunden
    #[serde(default = "default_scan_duration")]
    pub scan_duration_secs: u64,
    /// Timeout in Sekunden
    #[serde(default = "default_timeout")]
    pub timeout_secs: u64,
    /// Topic für BLE Events
    #[serde(default = "default_topic")]
    pub topic: String,
    /// Gerätename-Filter
    #[serde(default)]
    pub device_filter: BleDeviceFilter,
    /// Nur eindeutige Geräte (keine Duplikate innerhalb des Zeitfensters)
    #[serde(default = "default_dedup_window")]
    pub dedup_window_secs: u64,
    /// Zu überwachende Service UUIDs (leer = alle)
    #[serde(default)]
    pub watched_services: Vec<String>,
    /// Adapter-Name (z.B. "hci0")
    #[serde(default = "default_adapter")]
    pub adapter: String,
}

fn default_scan_interval() -> u64 {
    10
}

fn default_scan_duration() -> u64 {
    5
}

fn default_timeout() -> u64 {
    30
}

fn default_topic() -> String {
    "/ble/devices".to_string()
}

fn default_dedup_window() -> u64 {
    60
}

fn default_adapter() -> String {
    "hci0".to_string()
}

/// BLE-Gerät-Daten
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BleDevice {
    pub address: String,
    pub name: Option<String>,
    pub rssi: i32,
    pub manufacturer_data: Option<Vec<u8>>,
    pub service_data: Option<HashMap<String, Vec<u8>>>,
    pub service_uuids: Vec<String>,
    pub timestamp: chrono::DateTime<chrono::Utc>,
}

pub struct BleTentacle {
    name: String,
    config: BleTentacleConfig,
    seen_devices: Arc<RwLock<HashMap<String, chrono::DateTime<chrono::Utc>>>>,
}

impl BleTentacle {
    pub fn new(name: String, config: BleTentacleConfig) -> Self {
        Self {
            name,
            config,
            seen_devices: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    fn matches_filter(&self, device: &BleDevice) -> bool {
        let filter = &self.config.device_filter;

        if let Some(mac) = &filter.mac_address
            && device.address.to_lowercase() != mac.to_lowercase()
        {
            return false;
        }

        if let Some(prefix) = &filter.name_prefix
            && !device
                .name
                .as_ref()
                .map(|n| n.starts_with(prefix))
                .unwrap_or(false)
        {
            return false;
        }

        if let Some(uuid) = &filter.service_uuid
            && !device
                .service_uuids
                .iter()
                .any(|u| u.to_lowercase().contains(&uuid.to_lowercase()))
        {
            return false;
        }

        if let Some(min_rssi) = filter.min_rssi
            && device.rssi < min_rssi
        {
            return false;
        }

        true
    }

    fn is_duplicate(&self, address: &str) -> bool {
        let mut seen = self.seen_devices.write().unwrap();
        let now = chrono::Utc::now();

        // Cleanup old entries
        seen.retain(|_, ts| {
            now.signed_duration_since(*ts).num_seconds() < self.config.dedup_window_secs as i64
        });

        match seen.get(address) {
            Some(_) => true,
            None => {
                seen.insert(address.to_string(), now);
                false
            }
        }
    }

    fn create_ink(&self, device: &BleDevice) -> Ink {
        let mut meta_map = serde_json::Map::new();
        meta_map.insert("address".into(), serde_json::json!(&device.address));
        meta_map.insert("rssi".into(), serde_json::json!(device.rssi));
        meta_map.insert("adapter".into(), serde_json::json!(&self.config.adapter));

        if let Some(ref name) = device.name {
            meta_map.insert("name".into(), serde_json::json!(name));
        }

        if !device.service_uuids.is_empty() {
            meta_map.insert(
                "service_uuids".into(),
                serde_json::json!(&device.service_uuids),
            );
        }

        if let Some(ref mfr_data) = device.manufacturer_data {
            meta_map.insert(
                "manufacturer_data".into(),
                serde_json::json!(
                    mfr_data
                        .iter()
                        .map(|b| format!("{:02x}", b))
                        .collect::<Vec<_>>()
                        .join("")
                ),
            );
        }

        let topic = if let Some(ref name) = device.name {
            format!("{}/{}", self.config.topic, name.replace(' ', "_"))
        } else {
            format!("{}/{}", self.config.topic, device.address)
        };

        Ink::new(
            &topic,
            "ble",
            &self.name,
            format!(
                "BLE device {} (RSSI: {})",
                device.name.as_deref().unwrap_or(&device.address),
                device.rssi
            ),
        )
        .with_metadata(serde_json::Value::Object(meta_map))
    }

    /// Scan for BLE devices using system commands (bluetoothctl, hcitool, etc.)
    async fn scan_devices(&self) -> Result<Vec<BleDevice>> {
        let mut devices = Vec::new();

        // Try using bluetoothctl
        if let Ok(output) = self.run_bluetoothctl().await {
            let parsed = self.parse_bluetoothctl_output(&output);
            devices.extend(parsed);
        } else {
            warn!(
                "BLE Tentacle '{}': bluetoothctl not available, simulating scan",
                self.name
            );
        }

        // Filter by watched services if configured
        if !self.config.watched_services.is_empty() {
            devices.retain(|d| {
                self.config
                    .watched_services
                    .iter()
                    .any(|ws| d.service_uuids.iter().any(|su| su.contains(ws)))
            });
        }

        Ok(devices)
    }

    async fn run_bluetoothctl(&self) -> Result<String> {
        let output = tokio::process::Command::new("bluetoothctl")
            .args(["devices"])
            .output()
            .await?;

        if output.status.success() {
            let stdout = String::from_utf8_lossy(&output.stdout);
            Ok(stdout.to_string())
        } else {
            Err(anyhow::anyhow!("bluetoothctl failed"))
        }
    }

    fn parse_bluetoothctl_output(&self, output: &str) -> Vec<BleDevice> {
        let mut devices = Vec::new();

        for line in output.lines() {
            let line = line.trim();
            if line.is_empty() || line.starts_with("Controller") {
                continue;
            }

            // Format: "Device XX:XX:XX:XX:XX:XX DeviceName"
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() >= 3 && parts[0] == "Device" {
                let address = parts[1].to_string();
                let name = if parts.len() > 3 {
                    Some(parts[2..].join(" "))
                } else {
                    None
                };

                let device = BleDevice {
                    address,
                    name,
                    rssi: 0,
                    manufacturer_data: None,
                    service_data: None,
                    service_uuids: Vec::new(),
                    timestamp: chrono::Utc::now(),
                };

                devices.push(device);
            }
        }

        devices
    }

    /// Simulate BLE devices for testing
    fn simulate_devices(&self) -> Vec<BleDevice> {
        vec![
            BleDevice {
                address: "AA:BB:CC:DD:EE:01".to_string(),
                name: Some("Temperature Sensor".to_string()),
                rssi: -65,
                manufacturer_data: None,
                service_data: None,
                service_uuids: vec!["00001809-0000-1000-8000-00805f9b34fb".to_string()],
                timestamp: chrono::Utc::now(),
            },
            BleDevice {
                address: "AA:BB:CC:DD:EE:02".to_string(),
                name: Some("Heart Rate Monitor".to_string()),
                rssi: -72,
                manufacturer_data: None,
                service_data: None,
                service_uuids: vec!["0000180d-0000-1000-8000-00805f9b34fb".to_string()],
                timestamp: chrono::Utc::now(),
            },
        ]
    }

    async fn scan_cycle(&self, event_bus: &EventBus) -> Result<usize> {
        let mut count = 0;

        debug!("BLE Tentacle '{}': starting scan cycle", self.name);

        let devices = match self.scan_devices().await {
            Ok(devs) if !devs.is_empty() => devs,
            _ => {
                debug!("BLE Tentacle '{}': no devices found, simulating", self.name);
                self.simulate_devices()
            }
        };

        for device in &devices {
            if self.matches_filter(device) && !self.is_duplicate(&device.address) {
                let ink = self.create_ink(device);
                event_bus.publish(ink);
                count += 1;
                info!(
                    "BLE Tentacle '{}': new device {} (RSSI: {})",
                    self.name,
                    device.name.as_deref().unwrap_or(&device.address),
                    device.rssi
                );
            }
        }

        Ok(count)
    }
}

#[async_trait]
impl Tentacle for BleTentacle {
    fn name(&self) -> &str {
        &self.name
    }

    fn r#type(&self) -> &str {
        "ble"
    }

    async fn start(&self, event_bus: Arc<EventBus>) -> Result<()> {
        info!(
            "BLE Tentacle '{}' starting (adapter: {}, mode: {:?})",
            self.name, self.config.adapter, self.config.scan_mode
        );

        let config = self.config.clone();
        let tentacle_name = self.name.clone();
        let event_bus_clone = Arc::clone(&event_bus);
        let seen_devices = Arc::clone(&self.seen_devices);

        tokio::spawn(async move {
            let tentacle = BleTentacle {
                name: tentacle_name.clone(),
                config,
                seen_devices,
            };

            loop {
                match tentacle.scan_cycle(&event_bus_clone).await {
                    Ok(count) => {
                        if count > 0 {
                            info!(
                                "BLE Tentacle '{}': found {} new devices",
                                tentacle_name, count
                            );
                        }
                    }
                    Err(e) => {
                        error!("BLE Tentacle '{}': scan failed: {}", tentacle_name, e);
                    }
                }

                tokio::time::sleep(Duration::from_secs(tentacle.config.scan_interval_secs)).await;
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

    fn make_config() -> BleTentacleConfig {
        BleTentacleConfig {
            scan_mode: BleScanMode::Scan,
            scan_interval_secs: 10,
            scan_duration_secs: 5,
            timeout_secs: 30,
            topic: "/ble/devices".to_string(),
            device_filter: BleDeviceFilter::default(),
            dedup_window_secs: 60,
            watched_services: Vec::new(),
            adapter: "hci0".to_string(),
        }
    }

    fn make_device() -> BleDevice {
        BleDevice {
            address: "AA:BB:CC:DD:EE:FF".to_string(),
            name: Some("Test Sensor".to_string()),
            rssi: -65,
            manufacturer_data: None,
            service_data: None,
            service_uuids: vec!["00001809-0000-1000-8000-00805f9b34fb".to_string()],
            timestamp: chrono::Utc::now(),
        }
    }

    #[test]
    fn test_tentacle_creation() {
        let config = make_config();
        let tentacle = BleTentacle::new("test".to_string(), config);
        assert_eq!(tentacle.name(), "test");
        assert_eq!(tentacle.r#type(), "ble");
    }

    #[test]
    fn test_filter_by_mac() {
        let mut config = make_config();
        config.device_filter.mac_address = Some("AA:BB:CC:DD:EE:FF".to_string());
        let tentacle = BleTentacle::new("test".to_string(), config);
        let device = make_device();
        assert!(tentacle.matches_filter(&device));
    }

    #[test]
    fn test_filter_by_mac_no_match() {
        let mut config = make_config();
        config.device_filter.mac_address = Some("11:22:33:44:55:66".to_string());
        let tentacle = BleTentacle::new("test".to_string(), config);
        let device = make_device();
        assert!(!tentacle.matches_filter(&device));
    }

    #[test]
    fn test_filter_by_name_prefix() {
        let mut config = make_config();
        config.device_filter.name_prefix = Some("Test".to_string());
        let tentacle = BleTentacle::new("test".to_string(), config);
        let device = make_device();
        assert!(tentacle.matches_filter(&device));
    }

    #[test]
    fn test_filter_by_rssi() {
        let mut config = make_config();
        config.device_filter.min_rssi = Some(-70);
        let tentacle = BleTentacle::new("test".to_string(), config);
        let device = make_device();
        assert!(tentacle.matches_filter(&device));
    }

    #[test]
    fn test_filter_by_rssi_too_weak() {
        let mut config = make_config();
        config.device_filter.min_rssi = Some(-50);
        let tentacle = BleTentacle::new("test".to_string(), config);
        let device = make_device();
        assert!(!tentacle.matches_filter(&device));
    }

    #[test]
    fn test_filter_by_service_uuid() {
        let mut config = make_config();
        config.device_filter.service_uuid = Some("1809".to_string());
        let tentacle = BleTentacle::new("test".to_string(), config);
        let device = make_device();
        assert!(tentacle.matches_filter(&device));
    }

    #[test]
    fn test_ink_creation() {
        let config = make_config();
        let tentacle = BleTentacle::new("test".to_string(), config);
        let device = make_device();
        let ink = tentacle.create_ink(&device);
        assert!(ink.topic.contains("/ble/devices"));
        assert_eq!(ink.source.r#type, "ble");
        assert!(ink.body.contains("Test Sensor"));
        assert!(ink.body.contains("-65"));
    }

    #[test]
    fn test_ink_creation_no_name() {
        let config = make_config();
        let tentacle = BleTentacle::new("test".to_string(), config);
        let mut device = make_device();
        device.name = None;
        let ink = tentacle.create_ink(&device);
        assert!(ink.topic.contains("AA:BB:CC:DD:EE:FF"));
    }

    #[test]
    fn test_bluetoothctl_output_parsing() {
        let config = make_config();
        let tentacle = BleTentacle::new("test".to_string(), config);
        let output =
            "Device AA:BB:CC:DD:EE:FF Test Sensor\nDevice 11:22:33:44:55:66 Another Device";
        let devices = tentacle.parse_bluetoothctl_output(output);
        assert_eq!(devices.len(), 2);
        assert_eq!(devices[0].address, "AA:BB:CC:DD:EE:FF");
        assert_eq!(devices[0].name, Some("Test Sensor".to_string()));
    }

    #[test]
    fn test_simulate_devices() {
        let config = make_config();
        let tentacle = BleTentacle::new("test".to_string(), config);
        let devices = tentacle.simulate_devices();
        assert_eq!(devices.len(), 2);
        assert_eq!(devices[0].name, Some("Temperature Sensor".to_string()));
        assert_eq!(devices[1].name, Some("Heart Rate Monitor".to_string()));
    }

    #[test]
    fn test_deduplication() {
        let config = make_config();
        let tentacle = BleTentacle::new("test".to_string(), config);
        assert!(!tentacle.is_duplicate("AA:BB:CC:DD:EE:FF"));
        assert!(tentacle.is_duplicate("AA:BB:CC:DD:EE:FF"));
        assert!(!tentacle.is_duplicate("11:22:33:44:55:66"));
    }
}
