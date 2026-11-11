use anyhow::Result;
use async_trait::async_trait;
use futures_util::StreamExt;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, error, info};

use super::Tentacle;
use crate::eventbus::EventBus;
use crate::model::Ink;

/// Topic-Erkennungsmodus für Docker Events
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "lowercase")]
pub enum DockerTopicMode {
    /// Topic aus dem Event-Type verwenden (z.B. "container", "image")
    #[default]
    Type,
    /// Topic aus dem Event-Action verwenden (z.B. "start", "stop")
    Action,
    /// Topic aus Actor-Attributen extrahieren
    Attribute,
    /// Alle Events an ein festes Topic
    All,
}

/// Konfiguration für den Docker Events Tentacle
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DockerTentacleConfig {
    /// Docker Socket-Pfad oder TCP-URL
    #[serde(default = "default_host")]
    pub host: String,
    /// Event-Typen zum Filtern (z.B. "container", "image", "network")
    #[serde(default)]
    pub events: Vec<String>,
    /// Event-Actions zum Filtern (z.B. "start", "stop", "die")
    #[serde(default)]
    pub actions: Vec<String>,
    /// Topic-Erkennungsmodus
    #[serde(default)]
    pub topic_mode: DockerTopicMode,
    /// Attribut-Feld für Topic-Extraktion (bei attribute-Modus)
    #[serde(default)]
    pub topic_attribute: Option<String>,
    /// Fallback-Topic
    #[serde(default = "default_fallback_topic")]
    pub fallback_topic: String,
    /// Optional: Type-basiertes Topic-Mapping
    #[serde(default)]
    pub type_mapping: HashMap<String, String>,
    /// Optional: Action-basiertes Topic-Mapping
    #[serde(default)]
    pub action_mapping: HashMap<String, String>,
    /// TLS-Verifizierung deaktivieren (für TCP mit Self-Signed Zertifikaten)
    #[serde(default)]
    pub tls_verify: bool,
    /// TLS CA-Pfad
    #[serde(default)]
    pub tls_ca: Option<String>,
    /// TLS Client-Zertifikat
    #[serde(default)]
    pub tls_cert: Option<String>,
    /// TLS Client-Key
    #[serde(default)]
    pub tls_key: Option<String>,
}

fn default_host() -> String {
    "unix:///var/run/docker.sock".to_string()
}

fn default_fallback_topic() -> String {
    "/docker/events/default".to_string()
}

pub struct DockerTentacle {
    name: String,
    config: DockerTentacleConfig,
}

/// Docker Event-Struktur (vereinfacht)
#[derive(Debug, Clone, Serialize, Deserialize)]
struct DockerEvent {
    #[serde(rename = "Type")]
    r#type: Option<String>,
    #[serde(rename = "Action")]
    action: Option<String>,
    #[serde(rename = "Actor")]
    actor: Option<DockerActor>,
    #[serde(rename = "time")]
    time: Option<i64>,
    #[serde(rename = "scope")]
    scope: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct DockerActor {
    #[serde(rename = "ID")]
    id: Option<String>,
    #[serde(rename = "Attributes")]
    attributes: Option<HashMap<String, String>>,
}

/// Freistehende Funktion: Bestimmt das Topic basierend auf dem Docker Event
fn resolve_topic(
    config: &DockerTentacleConfig,
    event_type: &str,
    action: &str,
    attributes: &HashMap<String, String>,
) -> String {
    match config.topic_mode {
        DockerTopicMode::Type => {
            if let Some(mapped) = config.type_mapping.get(event_type) {
                return mapped.clone();
            }
            if event_type.is_empty() {
                config.fallback_topic.clone()
            } else {
                format!("/docker/events/{}", event_type)
            }
        }
        DockerTopicMode::Action => {
            if let Some(mapped) = config.action_mapping.get(action) {
                return mapped.clone();
            }
            if action.is_empty() {
                config.fallback_topic.clone()
            } else {
                format!("/docker/events/action/{}", action)
            }
        }
        DockerTopicMode::Attribute => {
            let attr_value = config
                .topic_attribute
                .as_deref()
                .and_then(|n| attributes.get(n))
                .filter(|v| !v.is_empty());

            if let Some(v) = attr_value {
                return if v.starts_with('/') {
                    v.clone()
                } else {
                    format!("/docker/events/attr/{}", v)
                };
            }
            config.fallback_topic.clone()
        }
        DockerTopicMode::All => config.fallback_topic.clone(),
    }
}

impl DockerTentacle {
    pub fn new(name: String, config: DockerTentacleConfig) -> Self {
        Self { name, config }
    }
}

#[async_trait]
impl Tentacle for DockerTentacle {
    fn name(&self) -> &str {
        &self.name
    }

    fn r#type(&self) -> &str {
        "docker"
    }

    async fn start(&self, event_bus: Arc<EventBus>) -> Result<()> {
        info!(
            "Docker Events Tentacle '{}' starting, host: {}",
            self.name, self.config.host
        );

        let config = self.config.clone();
        let tentacle_name = self.name.clone();
        let event_bus_clone = Arc::clone(&event_bus);

        tokio::spawn(async move {
            loop {
                // Baue die Events-URL
                let mut url = match config.host.as_str() {
                    h if h.starts_with("unix://") => {
                        let _socket_path = h.strip_prefix("unix://").unwrap_or("");
                        // Für Unix-Socket verwenden wir einen Proxy-Ansatz über curl oder direkten HTTP
                        // Da reqwest Unix-Sockets nicht direkt unterstützt, verwenden wir einen Workaround
                        "http://localhost/events".to_string()
                    }
                    h if h.starts_with("tcp://") || h.starts_with("http://") => {
                        let base = h
                            .strip_prefix("tcp://")
                            .or_else(|| h.strip_prefix("http://"))
                            .unwrap_or(h);
                        format!("http://{}/events", base)
                    }
                    h if h.starts_with("https://") => {
                        format!("{}/events", h)
                    }
                    _ => "http://localhost/events".to_string(),
                };

                // Füge Filter hinzu
                let mut filters = serde_json::Map::new();
                if !config.events.is_empty() {
                    filters.insert(
                        "type".to_string(),
                        serde_json::Value::Array(
                            config
                                .events
                                .iter()
                                .map(|e| serde_json::Value::String(e.clone()))
                                .collect(),
                        ),
                    );
                }
                if !config.actions.is_empty() {
                    filters.insert(
                        "action".to_string(),
                        serde_json::Value::Array(
                            config
                                .actions
                                .iter()
                                .map(|a| serde_json::Value::String(a.clone()))
                                .collect(),
                        ),
                    );
                }

                if !filters.is_empty() {
                    url.push_str(&format!(
                        "?filters={}",
                        urlencoding::encode(&serde_json::to_string(&filters).unwrap_or_default())
                    ));
                }

                // Verbinde mit Docker Events API
                let client = reqwest::Client::builder()
                    .danger_accept_invalid_certs(!config.tls_verify)
                    .build()
                    .unwrap_or_default();

                match client.get(&url).send().await {
                    Ok(resp) => {
                        info!(
                            "Docker Events Tentacle '{}': connected to events stream",
                            tentacle_name
                        );

                        // Lese die Event-Stream zeilenweise
                        let mut lines = resp.bytes_stream();
                        while let Some(chunk_result) = lines.next().await {
                            match chunk_result {
                                Ok(ref chunk) => {
                                    let text = String::from_utf8_lossy(chunk);
                                    for line in text.lines() {
                                        if line.is_empty() {
                                            continue;
                                        }

                                        // Parse Docker Event JSON
                                        if let Ok(event) = serde_json::from_str::<DockerEvent>(line)
                                        {
                                            let event_type = event.r#type.unwrap_or_default();
                                            let action = event.action.unwrap_or_default();
                                            let actor = event.actor;

                                            let mut attributes = HashMap::new();
                                            let mut metadata = serde_json::Map::new();

                                            let actor_id = actor
                                                .as_ref()
                                                .and_then(|a| a.id.as_ref())
                                                .cloned()
                                                .unwrap_or_default();

                                            if let Some(ref a) = actor
                                                && let Some(attrs) = &a.attributes
                                            {
                                                attributes = attrs.clone();
                                            }

                                            // Metadaten hinzufügen
                                            if !event_type.is_empty() {
                                                metadata.insert(
                                                    "docker_type".to_string(),
                                                    serde_json::Value::String(event_type.clone()),
                                                );
                                            }
                                            if !action.is_empty() {
                                                metadata.insert(
                                                    "docker_action".to_string(),
                                                    serde_json::Value::String(action.clone()),
                                                );
                                            }
                                            if !actor_id.is_empty() {
                                                metadata.insert(
                                                    "docker_actor_id".to_string(),
                                                    serde_json::Value::String(actor_id),
                                                );
                                            }
                                            if let Some(scope) = &event.scope {
                                                metadata.insert(
                                                    "docker_scope".to_string(),
                                                    serde_json::Value::String(scope.clone()),
                                                );
                                            }

                                            // Alle Actor-Attribute als Metadaten
                                            for (key, value) in &attributes {
                                                metadata.insert(
                                                    format!("docker_attr_{}", key),
                                                    serde_json::Value::String(value.clone()),
                                                );
                                            }

                                            let topic = resolve_topic(
                                                &config,
                                                &event_type,
                                                &action,
                                                &attributes,
                                            );

                                            debug!(
                                                "Docker Events Tentacle '{}': received {} event (action: {})",
                                                tentacle_name, event_type, action
                                            );

                                            let body = line.to_string();
                                            let mut ink =
                                                Ink::new(&topic, "docker", &tentacle_name, body);
                                            ink = ink
                                                .with_metadata(serde_json::Value::Object(metadata));

                                            info!(
                                                "Docker Events Tentacle '{}': published ink on topic: {}",
                                                tentacle_name, topic
                                            );
                                            event_bus_clone.publish(ink);
                                        }
                                    }
                                }
                                Err(e) => {
                                    error!(
                                        "Docker Events Tentacle '{}': error reading event stream: {}",
                                        tentacle_name, e
                                    );
                                    break;
                                }
                            }
                        }
                    }
                    Err(e) => {
                        error!(
                            "Docker Events Tentacle '{}': failed to connect to Docker: {}",
                            tentacle_name, e
                        );
                    }
                }

                // Warte vor dem Reconnect
                tokio::time::sleep(std::time::Duration::from_secs(5)).await;
            }
        });

        // Blockiere den Task, damit er läuft
        loop {
            tokio::time::sleep(std::time::Duration::from_secs(60)).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_docker_topic_mode_type() {
        let config = DockerTentacleConfig {
            host: "unix:///var/run/docker.sock".to_string(),
            events: vec!["container".to_string()],
            actions: vec![],
            topic_mode: DockerTopicMode::Type,
            topic_attribute: None,
            fallback_topic: "/docker/events/default".to_string(),
            type_mapping: HashMap::new(),
            action_mapping: HashMap::new(),
            tls_verify: false,
            tls_ca: None,
            tls_cert: None,
            tls_key: None,
        };

        assert_eq!(
            resolve_topic(&config, "container", "start", &HashMap::new()),
            "/docker/events/container"
        );
        assert_eq!(
            resolve_topic(&config, "image", "pull", &HashMap::new()),
            "/docker/events/image"
        );
        assert_eq!(
            resolve_topic(&config, "", "start", &HashMap::new()),
            "/docker/events/default"
        );
    }

    #[test]
    fn test_docker_topic_mode_action() {
        let config = DockerTentacleConfig {
            host: "unix:///var/run/docker.sock".to_string(),
            events: vec![],
            actions: vec![],
            topic_mode: DockerTopicMode::Action,
            topic_attribute: None,
            fallback_topic: "/docker/events/default".to_string(),
            type_mapping: HashMap::new(),
            action_mapping: HashMap::new(),
            tls_verify: false,
            tls_ca: None,
            tls_cert: None,
            tls_key: None,
        };

        assert_eq!(
            resolve_topic(&config, "container", "start", &HashMap::new()),
            "/docker/events/action/start"
        );
        assert_eq!(
            resolve_topic(&config, "container", "stop", &HashMap::new()),
            "/docker/events/action/stop"
        );
    }

    #[test]
    fn test_docker_topic_mode_attribute() {
        let mut attrs = HashMap::new();
        attrs.insert("name".to_string(), "my-container".to_string());
        attrs.insert("image".to_string(), "nginx:latest".to_string());

        let config = DockerTentacleConfig {
            host: "unix:///var/run/docker.sock".to_string(),
            events: vec![],
            actions: vec![],
            topic_mode: DockerTopicMode::Attribute,
            topic_attribute: Some("name".to_string()),
            fallback_topic: "/docker/events/default".to_string(),
            type_mapping: HashMap::new(),
            action_mapping: HashMap::new(),
            tls_verify: false,
            tls_ca: None,
            tls_cert: None,
            tls_key: None,
        };

        assert_eq!(
            resolve_topic(&config, "container", "start", &attrs),
            "/docker/events/attr/my-container"
        );
    }

    #[test]
    fn test_docker_topic_mode_all() {
        let config = DockerTentacleConfig {
            host: "unix:///var/run/docker.sock".to_string(),
            events: vec![],
            actions: vec![],
            topic_mode: DockerTopicMode::All,
            topic_attribute: None,
            fallback_topic: "/docker/all".to_string(),
            type_mapping: HashMap::new(),
            action_mapping: HashMap::new(),
            tls_verify: false,
            tls_ca: None,
            tls_cert: None,
            tls_key: None,
        };

        assert_eq!(
            resolve_topic(&config, "container", "start", &HashMap::new()),
            "/docker/all"
        );
    }

    #[test]
    fn test_docker_type_mapping() {
        let mut mapping = HashMap::new();
        mapping.insert("container".to_string(), "/containers/events".to_string());
        mapping.insert("image".to_string(), "/images/events".to_string());

        let config = DockerTentacleConfig {
            host: "unix:///var/run/docker.sock".to_string(),
            events: vec![],
            actions: vec![],
            topic_mode: DockerTopicMode::Type,
            topic_attribute: None,
            fallback_topic: "/docker/events/default".to_string(),
            type_mapping: mapping,
            action_mapping: HashMap::new(),
            tls_verify: false,
            tls_ca: None,
            tls_cert: None,
            tls_key: None,
        };

        assert_eq!(
            resolve_topic(&config, "container", "start", &HashMap::new()),
            "/containers/events"
        );
        assert_eq!(
            resolve_topic(&config, "image", "pull", &HashMap::new()),
            "/images/events"
        );
        assert_eq!(
            resolve_topic(&config, "network", "connect", &HashMap::new()),
            "/docker/events/network"
        );
    }

    #[test]
    fn test_docker_config_deserialization() {
        let yaml = r#"
        host: "tcp://docker:2375"
        events:
          - "container"
          - "image"
        actions:
          - "start"
          - "stop"
          - "die"
        topic_mode: type
        fallback_topic: "/docker/events/default"
        type_mapping:
          "container": "/containers/events"
        tls_verify: false
        "#;

        let config: DockerTentacleConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.host, "tcp://docker:2375");
        assert_eq!(config.events.len(), 2);
        assert_eq!(config.actions.len(), 3);
        assert_eq!(config.topic_mode, DockerTopicMode::Type);
        assert!(!config.tls_verify);
    }

    #[test]
    fn test_docker_tentacle_name_and_type() {
        let config = DockerTentacleConfig {
            host: "unix:///var/run/docker.sock".to_string(),
            events: vec![],
            actions: vec![],
            topic_mode: DockerTopicMode::Type,
            topic_attribute: None,
            fallback_topic: "/docker/events/default".to_string(),
            type_mapping: HashMap::new(),
            action_mapping: HashMap::new(),
            tls_verify: false,
            tls_ca: None,
            tls_cert: None,
            tls_key: None,
        };
        let tentacle = DockerTentacle::new("test_docker".to_string(), config);
        assert_eq!(tentacle.name(), "test_docker");
        assert_eq!(tentacle.r#type(), "docker");
    }

    #[test]
    fn test_docker_attribute_with_slash_prefix() {
        let mut attrs = HashMap::new();
        attrs.insert("topic".to_string(), "/custom/docker/path".to_string());

        let config = DockerTentacleConfig {
            host: "unix:///var/run/docker.sock".to_string(),
            events: vec![],
            actions: vec![],
            topic_mode: DockerTopicMode::Attribute,
            topic_attribute: Some("topic".to_string()),
            fallback_topic: "/docker/events/default".to_string(),
            type_mapping: HashMap::new(),
            action_mapping: HashMap::new(),
            tls_verify: false,
            tls_ca: None,
            tls_cert: None,
            tls_key: None,
        };

        assert_eq!(
            resolve_topic(&config, "container", "start", &attrs),
            "/custom/docker/path"
        );
    }
}
