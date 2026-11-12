use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info, warn};

use super::Tentacle;
use crate::eventbus::EventBus;
use crate::model::Ink;

/// Geofence-Form
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "lowercase")]
pub enum GeofenceShape {
    #[default]
    Circle,
    Polygon,
}

/// Event-Typ beim Betreten/Verlassen
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum GeofenceEventType {
    Enter,
    Exit,
    Both,
}

/// Konfiguration für einen Geofence-Bereich
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GeofenceZone {
    /// Name der Zone
    pub name: String,
    /// Form der Zone
    #[serde(default)]
    pub shape: GeofenceShape,
    /// Mittelpunkt (für Circle)
    pub latitude: Option<f64>,
    pub longitude: Option<f64>,
    /// Radius in Metern (für Circle)
    #[serde(default = "default_radius")]
    pub radius_meters: f64,
    /// Eckpunkte (für Polygon)
    #[serde(default)]
    pub polygon_coords: Vec<(f64, f64)>,
    /// Event-Typ
    #[serde(default = "default_event_type")]
    pub event_type: GeofenceEventType,
    /// Additional metadata
    #[serde(default)]
    pub metadata: HashMap<String, String>,
}

fn default_radius() -> f64 {
    100.0
}

fn default_event_type() -> GeofenceEventType {
    GeofenceEventType::Both
}

/// Konfiguration für den Geofence Tentacle
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GeofenceTentacleConfig {
    /// Geofence-Zonen
    pub zones: Vec<GeofenceZone>,
    /// Polling-Interval in Sekunden (für Position-Updates)
    #[serde(default = "default_poll_interval")]
    pub poll_interval_secs: u64,
    /// REST API Endpoint für Position-Updates
    pub position_endpoint: String,
    /// API Key für den Position-Service
    #[serde(default)]
    pub api_key: Option<String>,
    /// Topic für Geofence-Events
    #[serde(default = "default_topic")]
    pub topic: String,
    /// Timeout in Sekunden
    #[serde(default = "default_timeout")]
    pub timeout_secs: u64,
    /// Hysteresis-Radius in Metern (vermeidet Flapping)
    #[serde(default = "default_hysteresis")]
    pub hysteresis_meters: f64,
    /// Mindest-Zeit zwischen Events in Sekunden
    #[serde(default = "default_cooldown")]
    pub cooldown_secs: u64,
}

fn default_poll_interval() -> u64 {
    30
}

fn default_topic() -> String {
    "/geofence/event".to_string()
}

fn default_timeout() -> u64 {
    10
}

fn default_hysteresis() -> f64 {
    10.0
}

fn default_cooldown() -> u64 {
    60
}

/// Position-Daten
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Position {
    pub device_id: String,
    pub latitude: f64,
    pub longitude: f64,
    pub accuracy: Option<f64>,
    pub timestamp: Option<String>,
}

pub struct GeofenceTentacle {
    name: String,
    config: GeofenceTentacleConfig,
}

impl GeofenceTentacle {
    pub fn new(name: String, config: GeofenceTentacleConfig) -> Self {
        Self { name, config }
    }

    /// Berechnet die Distanz zwischen zwei Punkten (Haversine-Formel) in Metern
    fn haversine_distance(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
        let earth_radius = 6371000.0; // Meter
        let lat1 = lat1.to_radians();
        let lat2 = lat2.to_radians();
        let delta_lat = lat2 - lat1;
        let delta_lon = (lon2 - lon1).to_radians();

        let a = (delta_lat / 2.0).sin() * (delta_lat / 2.0).sin()
            + lat1.cos() * lat2.cos() * (delta_lon / 2.0).sin() * (delta_lon / 2.0).sin();
        let c = 2.0 * a.sqrt().atan2((1.0 - a).sqrt());

        earth_radius * c
    }

    /// Prüft, ob ein Punkt (lat, lon) innerhalb eines Polygons liegt (Ray-Casting-Algorithmus)
    fn point_in_polygon(lat: f64, lon: f64, polygon: &[(f64, f64)]) -> bool {
        let mut inside = false;
        let n = polygon.len();
        if n < 3 {
            return false;
        }

        for i in 0..n {
            let j = (i + 1) % n;
            let pi_lat = polygon[i].0;
            let pi_lon = polygon[i].1;
            let pj_lat = polygon[j].0;
            let pj_lon = polygon[j].1;

            if ((pi_lat > lat) != (pj_lat > lat))
                && (lon < (pj_lon - pi_lon) * (lat - pi_lat) / (pj_lat - pi_lat) + pi_lon)
            {
                inside = !inside;
            }
        }

        inside
    }

    /// Prüft, ob eine Position in einer Zone ist
    #[allow(dead_code)]
    fn is_in_zone(&self, position: &Position, zone: &GeofenceZone) -> bool {
        match zone.shape {
            GeofenceShape::Circle => {
                if let (Some(lat), Some(lon)) = (zone.latitude, zone.longitude) {
                    let distance =
                        Self::haversine_distance(position.latitude, position.longitude, lat, lon);
                    distance <= zone.radius_meters
                } else {
                    warn!(
                        "Geofence Tentacle '{}': circle zone '{}' missing center coordinates",
                        self.name, zone.name
                    );
                    false
                }
            }
            GeofenceShape::Polygon => {
                if zone.polygon_coords.is_empty() {
                    warn!(
                        "Geofence Tentacle '{}': polygon zone '{}' has no coordinates",
                        self.name, zone.name
                    );
                    false
                } else {
                    Self::point_in_polygon(
                        position.latitude,
                        position.longitude,
                        &zone.polygon_coords,
                    )
                }
            }
        }
    }
}

#[async_trait]
impl Tentacle for GeofenceTentacle {
    fn name(&self) -> &str {
        &self.name
    }

    fn r#type(&self) -> &str {
        "geofence"
    }

    async fn start(&self, event_bus: Arc<EventBus>) -> Result<()> {
        info!(
            "Geofence Tentacle '{}' starting with {} zones",
            self.name,
            self.config.zones.len()
        );

        let config = self.config.clone();
        let tentacle_name = self.name.clone();
        let event_bus_clone = Arc::clone(&event_bus);

        tokio::spawn(async move {
            let client = match reqwest::Client::builder()
                .timeout(Duration::from_secs(config.timeout_secs))
                .build()
            {
                Ok(c) => c,
                Err(e) => {
                    error!(
                        "Geofence Tentacle '{}': failed to build HTTP client: {}",
                        tentacle_name, e
                    );
                    return;
                }
            };

            // Track letzte Zustände pro Device
            let mut last_states: HashMap<String, HashMap<String, bool>> = HashMap::new();
            let mut last_event_times: HashMap<String, chrono::DateTime<chrono::Utc>> =
                HashMap::new();

            loop {
                // Position-Updates vom Endpoint abrufen
                let positions = match fetch_positions(&client, &config, &tentacle_name).await {
                    Ok(p) => p,
                    Err(e) => {
                        error!(
                            "Geofence Tentacle '{}': failed to fetch positions: {}",
                            tentacle_name, e
                        );
                        tokio::time::sleep(Duration::from_secs(config.poll_interval_secs)).await;
                        continue;
                    }
                };

                let now = chrono::Utc::now();

                for position in positions {
                    for zone in &config.zones {
                        let in_zone = is_in_zone_simple(&position, zone);

                        // Cooldown prüfen
                        let key = format!("{}_{}", position.device_id, zone.name);
                        if let Some(last_time) = last_event_times.get(&key)
                            && (now - *last_time).num_seconds() < config.cooldown_secs as i64
                        {
                            debug!(
                                "Geofence Tentacle '{}': cooldown for {} in zone {}",
                                tentacle_name, position.device_id, zone.name
                            );
                            continue;
                        }

                        // Letzten Zustand holen
                        let was_in_zone = last_states
                            .get(&position.device_id)
                            .and_then(|states| states.get(&zone.name))
                            .copied()
                            .unwrap_or(false);

                        // Hysteresis anwenden
                        let hysteresis_in_zone = if in_zone {
                            in_zone // Wenn drin, bleib drin
                        } else {
                            // Nur als "draußen" markieren, wenn deutlich außerhalb
                            if let (Some(lat), Some(lon)) = (zone.latitude, zone.longitude) {
                                let distance = Self::haversine_distance(
                                    position.latitude,
                                    position.longitude,
                                    lat,
                                    lon,
                                );
                                distance > zone.radius_meters + config.hysteresis_meters
                            } else {
                                !in_zone
                            }
                        };

                        let effective_in_zone = if zone.shape == GeofenceShape::Circle {
                            hysteresis_in_zone
                        } else {
                            in_zone
                        };

                        // Zustand ändern?
                        if effective_in_zone != was_in_zone {
                            let event_type = if effective_in_zone { "enter" } else { "exit" };

                            // Event-Typ filtern
                            match zone.event_type {
                                GeofenceEventType::Enter if !effective_in_zone => continue,
                                GeofenceEventType::Exit if effective_in_zone => continue,
                                _ => {}
                            }

                            info!(
                                "Geofence Tentacle '{}': device {} {} zone {}",
                                tentacle_name, position.device_id, event_type, zone.name
                            );

                            let ink = create_geofence_ink(
                                &config,
                                &tentacle_name,
                                &position,
                                zone,
                                event_type,
                            );
                            event_bus_clone.publish(ink);

                            // Zeit speichern
                            last_event_times.insert(key, now);
                        }

                        // Zustand aktualisieren
                        last_states
                            .entry(position.device_id.clone())
                            .or_default()
                            .insert(zone.name.clone(), effective_in_zone);
                    }
                }

                tokio::time::sleep(Duration::from_secs(config.poll_interval_secs)).await;
            }
        });

        loop {
            tokio::time::sleep(Duration::from_secs(60)).await;
        }
    }
}

/// Einfache Version von is_in_zone für den spawned task
fn is_in_zone_simple(position: &Position, zone: &GeofenceZone) -> bool {
    match zone.shape {
        GeofenceShape::Circle => {
            if let (Some(lat), Some(lon)) = (zone.latitude, zone.longitude) {
                let distance = GeofenceTentacle::haversine_distance(
                    position.latitude,
                    position.longitude,
                    lat,
                    lon,
                );
                distance <= zone.radius_meters
            } else {
                false
            }
        }
        GeofenceShape::Polygon => {
            if zone.polygon_coords.is_empty() {
                false
            } else {
                GeofenceTentacle::point_in_polygon(
                    position.latitude,
                    position.longitude,
                    &zone.polygon_coords,
                )
            }
        }
    }
}

async fn fetch_positions(
    client: &reqwest::Client,
    config: &GeofenceTentacleConfig,
    tentacle_name: &str,
) -> Result<Vec<Position>> {
    let mut req = client.get(&config.position_endpoint);

    if let Some(ref api_key) = config.api_key {
        req = req.header("Authorization", format!("Bearer {}", api_key));
    }

    let response = req.send().await?;
    if !response.status().is_success() {
        return Err(anyhow::anyhow!(
            "Position API request failed with status: {}",
            response.status()
        ));
    }

    let body = response.text().await?;

    // Versuche JSON-Array zu parsen
    if let Ok(positions) = serde_json::from_str::<Vec<serde_json::Value>>(&body) {
        let mut result = Vec::new();
        for p in positions {
            let device_id = p
                .get("device_id")
                .and_then(|d| d.as_str())
                .unwrap_or("unknown")
                .to_string();
            let latitude = p.get("latitude").and_then(|l| l.as_f64()).unwrap_or(0.0);
            let longitude = p.get("longitude").and_then(|l| l.as_f64()).unwrap_or(0.0);
            let accuracy = p.get("accuracy").and_then(|a| a.as_f64());
            let timestamp = p
                .get("timestamp")
                .and_then(|t| t.as_str())
                .map(|s| s.to_string());

            result.push(Position {
                device_id,
                latitude,
                longitude,
                accuracy,
                timestamp,
            });
        }
        debug!(
            "Geofence Tentacle '{}': fetched {} positions",
            tentacle_name,
            result.len()
        );
        return Ok(result);
    }

    Ok(Vec::new())
}

fn create_geofence_ink(
    config: &GeofenceTentacleConfig,
    tentacle_name: &str,
    position: &Position,
    zone: &GeofenceZone,
    event_type: &str,
) -> Ink {
    let mut meta_map = serde_json::Map::new();
    meta_map.insert("zone_name".into(), serde_json::json!(zone.name));
    meta_map.insert(
        "zone_shape".into(),
        serde_json::json!(match zone.shape {
            GeofenceShape::Circle => "circle",
            GeofenceShape::Polygon => "polygon",
        }),
    );
    meta_map.insert("event_type".into(), serde_json::json!(event_type));
    meta_map.insert("device_id".into(), serde_json::json!(position.device_id));
    meta_map.insert("latitude".into(), serde_json::json!(position.latitude));
    meta_map.insert("longitude".into(), serde_json::json!(position.longitude));

    if let Some(accuracy) = position.accuracy {
        meta_map.insert("accuracy".into(), serde_json::json!(accuracy));
    }

    if let Some(ref ts) = position.timestamp {
        meta_map.insert("position_timestamp".into(), serde_json::json!(ts));
    }

    for (key, value) in &zone.metadata {
        meta_map.insert(key.clone(), serde_json::json!(value));
    }

    Ink::new(
        &config.topic,
        "geofence",
        tentacle_name,
        format!(
            "Device {} {} zone {}",
            position.device_id, event_type, zone.name
        ),
    )
    .with_metadata(serde_json::Value::Object(meta_map))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_haversine_distance_same_point() {
        let dist = GeofenceTentacle::haversine_distance(52.52, 13.405, 52.52, 13.405);
        assert!((dist - 0.0).abs() < 0.01);
    }

    #[test]
    fn test_haversine_distance_berlin_munich() {
        // Berlin ~52.52, 13.405; München ~48.135, 11.582
        let dist = GeofenceTentacle::haversine_distance(52.52, 13.405, 48.135, 11.582);
        // Etwa 504 km
        assert!(dist > 500_000.0 && dist < 520_000.0);
    }

    #[test]
    fn test_point_in_polygon_simple() {
        // Einfaches Rechteck: (lat, lon) Koordinaten
        let polygon = vec![(0.0, 0.0), (0.0, 10.0), (10.0, 10.0), (10.0, 0.0)];
        assert!(GeofenceTentacle::point_in_polygon(5.0, 5.0, &polygon));
        assert!(!GeofenceTentacle::point_in_polygon(15.0, 15.0, &polygon));
    }

    #[test]
    fn test_circle_zone_config() {
        let config: GeofenceTentacleConfig = serde_json::from_str(
            r#"{
                "position_endpoint": "http://localhost:8080/positions",
                "zones": [{
                    "name": "home",
                    "shape": "circle",
                    "latitude": 52.52,
                    "longitude": 13.405,
                    "radius_meters": 100
                }]
            }"#,
        )
        .unwrap();

        assert_eq!(config.zones.len(), 1);
        assert_eq!(config.zones[0].name, "home");
        assert_eq!(config.zones[0].shape, GeofenceShape::Circle);
        assert_eq!(config.zones[0].radius_meters, 100.0);
    }

    #[test]
    fn test_polygon_zone_config() {
        let config: GeofenceTentacleConfig = serde_json::from_str(
            r#"{
                "position_endpoint": "http://localhost:8080/positions",
                "zones": [{
                    "name": "office",
                    "shape": "polygon",
                    "polygon_coords": [[52.52, 13.405], [52.53, 13.405], [52.53, 13.415], [52.52, 13.415]]
                }]
            }"#,
        )
        .unwrap();

        assert_eq!(config.zones[0].shape, GeofenceShape::Polygon);
        assert_eq!(config.zones[0].polygon_coords.len(), 4);
    }

    #[test]
    fn test_geofence_event_types() {
        assert_eq!(
            serde_json::from_str::<GeofenceEventType>("\"enter\"").unwrap(),
            GeofenceEventType::Enter
        );
        assert_eq!(
            serde_json::from_str::<GeofenceEventType>("\"exit\"").unwrap(),
            GeofenceEventType::Exit
        );
        assert_eq!(
            serde_json::from_str::<GeofenceEventType>("\"both\"").unwrap(),
            GeofenceEventType::Both
        );
    }

    #[test]
    fn test_config_defaults() {
        let config: GeofenceTentacleConfig = serde_json::from_str(
            r#"{"position_endpoint": "http://localhost:8080/positions", "zones": []}"#,
        )
        .unwrap();

        assert_eq!(config.poll_interval_secs, 30);
        assert_eq!(config.topic, "/geofence/event");
        assert_eq!(config.timeout_secs, 10);
        assert_eq!(config.hysteresis_meters, 10.0);
        assert_eq!(config.cooldown_secs, 60);
    }

    #[test]
    fn test_create_geofence_ink() {
        let config = GeofenceTentacleConfig {
            zones: Vec::new(),
            position_endpoint: "http://localhost:8080/positions".to_string(),
            api_key: None,
            topic: "/geofence/test".to_string(),
            poll_interval_secs: 30,
            timeout_secs: 10,
            hysteresis_meters: 10.0,
            cooldown_secs: 60,
        };

        let position = Position {
            device_id: "device1".to_string(),
            latitude: 52.52,
            longitude: 13.405,
            accuracy: Some(5.0),
            timestamp: None,
        };

        let zone = GeofenceZone {
            name: "home".to_string(),
            shape: GeofenceShape::Circle,
            latitude: Some(52.52),
            longitude: Some(13.405),
            radius_meters: 100.0,
            polygon_coords: Vec::new(),
            event_type: GeofenceEventType::Both,
            metadata: HashMap::new(),
        };

        let ink = create_geofence_ink(&config, "test", &position, &zone, "enter");

        assert_eq!(ink.topic, "/geofence/test");
        assert!(ink.body.contains("device1"));
        assert!(ink.body.contains("enter"));
        assert!(ink.body.contains("home"));
    }

    #[test]
    fn test_is_in_zone_circle() {
        let tentacle = GeofenceTentacle::new(
            "test".to_string(),
            GeofenceTentacleConfig {
                zones: Vec::new(),
                position_endpoint: "http://localhost:8080/positions".to_string(),
                api_key: None,
                topic: "/geofence/test".to_string(),
                poll_interval_secs: 30,
                timeout_secs: 10,
                hysteresis_meters: 10.0,
                cooldown_secs: 60,
            },
        );

        let zone = GeofenceZone {
            name: "test".to_string(),
            shape: GeofenceShape::Circle,
            latitude: Some(52.52),
            longitude: Some(13.405),
            radius_meters: 100.0,
            polygon_coords: Vec::new(),
            event_type: GeofenceEventType::Both,
            metadata: HashMap::new(),
        };

        // Punkt genau im Zentrum
        let position = Position {
            device_id: "d1".to_string(),
            latitude: 52.52,
            longitude: 13.405,
            accuracy: None,
            timestamp: None,
        };
        assert!(tentacle.is_in_zone(&position, &zone));

        // Punkt weit weg
        let far_position = Position {
            device_id: "d1".to_string(),
            latitude: 48.135,
            longitude: 11.582,
            accuracy: None,
            timestamp: None,
        };
        assert!(!tentacle.is_in_zone(&far_position, &zone));
    }

    #[test]
    fn test_is_in_zone_polygon() {
        let tentacle = GeofenceTentacle::new(
            "test".to_string(),
            GeofenceTentacleConfig {
                zones: Vec::new(),
                position_endpoint: "http://localhost:8080/positions".to_string(),
                api_key: None,
                topic: "/geofence/test".to_string(),
                poll_interval_secs: 30,
                timeout_secs: 10,
                hysteresis_meters: 10.0,
                cooldown_secs: 60,
            },
        );

        let zone = GeofenceZone {
            name: "test".to_string(),
            shape: GeofenceShape::Polygon,
            latitude: None,
            longitude: None,
            radius_meters: 0.0,
            polygon_coords: vec![
                (52.52, 13.405),
                (52.52, 13.415),
                (52.53, 13.415),
                (52.53, 13.405),
            ],
            event_type: GeofenceEventType::Both,
            metadata: HashMap::new(),
        };

        // Punkt innerhalb
        let inside = Position {
            device_id: "d1".to_string(),
            latitude: 52.525,
            longitude: 13.41,
            accuracy: None,
            timestamp: None,
        };
        assert!(tentacle.is_in_zone(&inside, &zone));

        // Punkt außerhalb
        let outside = Position {
            device_id: "d1".to_string(),
            latitude: 52.6,
            longitude: 13.5,
            accuracy: None,
            timestamp: None,
        };
        assert!(!tentacle.is_in_zone(&outside, &zone));
    }

    #[test]
    fn test_api_key_config() {
        let config: GeofenceTentacleConfig = serde_json::from_str(
            r#"{
                "position_endpoint": "http://localhost:8080/positions",
                "api_key": "secret-key-123",
                "zones": []
            }"#,
        )
        .unwrap();

        assert_eq!(config.api_key, Some("secret-key-123".to_string()));
    }

    #[test]
    fn test_cooldown_config() {
        let config: GeofenceTentacleConfig = serde_json::from_str(
            r#"{
                "position_endpoint": "http://localhost:8080/positions",
                "cooldown_secs": 300,
                "hysteresis_meters": 25.0,
                "zones": []
            }"#,
        )
        .unwrap();

        assert_eq!(config.cooldown_secs, 300);
        assert_eq!(config.hysteresis_meters, 25.0);
    }
}
