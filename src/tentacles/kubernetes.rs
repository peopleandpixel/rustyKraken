use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, error, info};

use super::Tentacle;
use crate::eventbus::EventBus;
use crate::model::Ink;

/// Topic-Erkennungsmodus für Kubernetes Events
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "lowercase")]
pub enum K8sTopicMode {
    /// Topic aus dem Event-Type verwenden (Normal/Warning)
    #[default]
    Type,
    /// Topic aus dem Resource-Type verwenden (Pod, Node, etc.)
    Resource,
    /// Topic aus dem Namespace verwenden
    Namespace,
    /// Alle Events an ein festes Topic
    All,
}

/// Konfiguration für den Kubernetes Events Tentacle
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct K8sTentacleConfig {
    /// Kubernetes API Server URL
    #[serde(default = "default_api_server")]
    pub api_server: String,
    /// Kubernetes Namespace zum Filtern (leer = alle)
    #[serde(default)]
    pub namespace: Option<String>,
    /// Token für die Authentifizierung (Service Account Token)
    #[serde(default)]
    pub token: Option<String>,
    /// kubeconfig-Pfad (als Alternative zu Token)
    #[serde(default)]
    pub kubeconfig: Option<String>,
    /// Topic-Erkennungsmodus
    #[serde(default)]
    pub topic_mode: K8sTopicMode,
    /// Fallback-Topic
    #[serde(default = "default_fallback_topic")]
    pub fallback_topic: String,
    /// Optional: Resource-Type-basiertes Topic-Mapping
    #[serde(default)]
    pub resource_mapping: HashMap<String, String>,
    /// TLS-Verifizierung deaktivieren
    #[serde(default)]
    pub tls_verify: bool,
    /// CA-Zertifikat-Pfad
    #[serde(default)]
    pub ca_cert: Option<String>,
    /// Watch-Timeout in Sekunden
    #[serde(default = "default_watch_timeout")]
    pub watch_timeout_seconds: u64,
}

fn default_api_server() -> String {
    "https://kubernetes.default.svc".to_string()
}

fn default_fallback_topic() -> String {
    "/kubernetes/events/default".to_string()
}

fn default_watch_timeout() -> u64 {
    300
}

pub struct K8sTentacle {
    name: String,
    config: K8sTentacleConfig,
}

/// Kubernetes Event-Struktur (vereinfacht)
#[derive(Debug, Clone, Serialize, Deserialize)]
struct K8sEvent {
    #[serde(rename = "type")]
    event_type: Option<String>,
    #[serde(rename = "object")]
    object: Option<K8sObject>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct K8sObject {
    #[serde(rename = "kind")]
    kind: Option<String>,
    #[serde(rename = "apiVersion")]
    api_version: Option<String>,
    #[serde(rename = "metadata")]
    metadata: Option<K8sMetadata>,
    #[serde(rename = "reason")]
    reason: Option<String>,
    #[serde(rename = "message")]
    message: Option<String>,
    #[serde(rename = "involvedObject")]
    involved_object: Option<K8sInvolvedObject>,
    #[serde(rename = "firstTimestamp")]
    first_timestamp: Option<String>,
    #[serde(rename = "lastTimestamp")]
    last_timestamp: Option<String>,
    #[serde(rename = "count")]
    count: Option<u32>,
    #[serde(rename = "source")]
    source: Option<K8sSource>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct K8sMetadata {
    name: Option<String>,
    namespace: Option<String>,
    #[serde(rename = "uid")]
    uid: Option<String>,
    #[serde(rename = "labels")]
    labels: Option<HashMap<String, String>>,
    #[serde(rename = "creationTimestamp")]
    creation_timestamp: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct K8sInvolvedObject {
    kind: Option<String>,
    name: Option<String>,
    namespace: Option<String>,
    #[serde(rename = "uid")]
    uid: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct K8sSource {
    component: Option<String>,
    host: Option<String>,
}

/// Freistehende Funktion: Bestimmt das Topic basierend auf dem K8s Event
fn resolve_topic(
    config: &K8sTentacleConfig,
    event_type: &str,
    resource_kind: &str,
    namespace: &str,
) -> String {
    match config.topic_mode {
        K8sTopicMode::Type => {
            if event_type.is_empty() {
                config.fallback_topic.clone()
            } else {
                format!("/kubernetes/events/{}", event_type.to_lowercase())
            }
        }
        K8sTopicMode::Resource => {
            if let Some(mapped) = config.resource_mapping.get(resource_kind) {
                return mapped.clone();
            }
            if resource_kind.is_empty() {
                config.fallback_topic.clone()
            } else {
                format!("/kubernetes/events/{}", resource_kind.to_lowercase())
            }
        }
        K8sTopicMode::Namespace => {
            if namespace.is_empty() {
                config.fallback_topic.clone()
            } else {
                format!("/kubernetes/events/namespace/{}", namespace)
            }
        }
        K8sTopicMode::All => config.fallback_topic.clone(),
    }
}

impl K8sTentacle {
    pub fn new(name: String, config: K8sTentacleConfig) -> Self {
        Self { name, config }
    }
}

#[async_trait]
impl Tentacle for K8sTentacle {
    fn name(&self) -> &str {
        &self.name
    }

    fn r#type(&self) -> &str {
        "kubernetes"
    }

    async fn start(&self, event_bus: Arc<EventBus>) -> Result<()> {
        info!(
            "Kubernetes Events Tentacle '{}' starting, API server: {}",
            self.name, self.config.api_server
        );

        let config = self.config.clone();
        let tentacle_name = self.name.clone();
        let event_bus_clone = Arc::clone(&event_bus);

        tokio::spawn(async move {
            loop {
                // Baue die Watch-URL
                let mut url = format!("{}/api/v1/watch/events", config.api_server);

                if let Some(ns) = &config.namespace {
                    url.push_str(&format!("?namespace={}", urlencoding::encode(ns)));
                } else {
                    url.push_str("?namespace=");
                }

                url.push_str(&format!("&timeoutSeconds={}", config.watch_timeout_seconds));

                // Erstelle HTTP-Client
                let client = reqwest::Client::builder()
                    .danger_accept_invalid_certs(!config.tls_verify)
                    .build()
                    .unwrap_or_default();

                let mut req = client.get(&url);

                // Füge Authentifizierung hinzu
                if let Some(token) = &config.token {
                    req = req.header("Authorization", format!("Bearer {}", token));
                }

                match req.send().await {
                    Ok(resp) => {
                        info!(
                            "Kubernetes Events Tentacle '{}': connected to watch stream",
                            tentacle_name
                        );

                        let status = resp.status();
                        if !status.is_success() {
                            error!(
                                "Kubernetes Events Tentacle '{}': API returned status {}",
                                tentacle_name, status
                            );
                        }

                        // Lese den Stream zeilenweise
                        let mut lines = resp.bytes_stream();
                        use futures_util::StreamExt;

                        while let Some(chunk_result) = lines.next().await {
                            match chunk_result {
                                Ok(ref chunk) => {
                                    let text = String::from_utf8_lossy(chunk);
                                    for line in text.lines() {
                                        if line.is_empty() {
                                            continue;
                                        }

                                        // Parse Kubernetes Event
                                        if let Ok(k8s_event) =
                                            serde_json::from_str::<K8sEvent>(line)
                                        {
                                            let event_type =
                                                k8s_event.event_type.unwrap_or_default();
                                            let object = k8s_event.object;

                                            let mut metadata = serde_json::Map::new();
                                            let mut resource_kind = String::new();
                                            let mut ns = String::new();
                                            let mut message = String::new();

                                            if let Some(ref obj) = object {
                                                resource_kind =
                                                    obj.kind.clone().unwrap_or_default();
                                                message = obj.message.clone().unwrap_or_default();

                                                if let Some(ref meta) = obj.metadata {
                                                    ns = meta.namespace.clone().unwrap_or_default();
                                                    if let Some(name) = &meta.name {
                                                        metadata.insert(
                                                            "k8s_name".to_string(),
                                                            serde_json::Value::String(name.clone()),
                                                        );
                                                    }
                                                    if let Some(uid) = &meta.uid {
                                                        metadata.insert(
                                                            "k8s_uid".to_string(),
                                                            serde_json::Value::String(uid.clone()),
                                                        );
                                                    }
                                                    if let Some(labels) = &meta.labels {
                                                        for (key, value) in labels {
                                                            metadata.insert(
                                                                format!("k8s_label_{}", key),
                                                                serde_json::Value::String(
                                                                    value.clone(),
                                                                ),
                                                            );
                                                        }
                                                    }
                                                }
                                                if let Some(reason) = &obj.reason {
                                                    metadata.insert(
                                                        "k8s_reason".to_string(),
                                                        serde_json::Value::String(reason.clone()),
                                                    );
                                                }
                                                if let Some(involved) = &obj.involved_object {
                                                    if let Some(kind) = &involved.kind {
                                                        metadata.insert(
                                                            "k8s_involved_kind".to_string(),
                                                            serde_json::Value::String(kind.clone()),
                                                        );
                                                    }
                                                    if let Some(name) = &involved.name {
                                                        metadata.insert(
                                                            "k8s_involved_name".to_string(),
                                                            serde_json::Value::String(name.clone()),
                                                        );
                                                    }
                                                }
                                                if let Some(source) = &obj.source
                                                    && let Some(component) = &source.component
                                                {
                                                    metadata.insert(
                                                        "k8s_source".to_string(),
                                                        serde_json::Value::String(
                                                            component.clone(),
                                                        ),
                                                    );
                                                }
                                                if let Some(count) = &obj.count {
                                                    metadata.insert(
                                                        "k8s_count".to_string(),
                                                        serde_json::Value::Number(
                                                            serde_json::Number::from(*count),
                                                        ),
                                                    );
                                                }
                                            }

                                            // Event-Typ und Namespace als Metadaten
                                            if !event_type.is_empty() {
                                                metadata.insert(
                                                    "k8s_event_type".to_string(),
                                                    serde_json::Value::String(event_type.clone()),
                                                );
                                            }
                                            if !resource_kind.is_empty() {
                                                metadata.insert(
                                                    "k8s_resource_kind".to_string(),
                                                    serde_json::Value::String(
                                                        resource_kind.clone(),
                                                    ),
                                                );
                                            }
                                            if !ns.is_empty() {
                                                metadata.insert(
                                                    "k8s_namespace".to_string(),
                                                    serde_json::Value::String(ns.clone()),
                                                );
                                            }

                                            let topic = resolve_topic(
                                                &config,
                                                &event_type,
                                                &resource_kind,
                                                &ns,
                                            );

                                            debug!(
                                                "Kubernetes Events Tentacle '{}': received {} event (resource: {})",
                                                tentacle_name, event_type, resource_kind
                                            );

                                            let body = if !message.is_empty() {
                                                message
                                            } else {
                                                line.to_string()
                                            };

                                            let mut ink = Ink::new(
                                                &topic,
                                                "kubernetes",
                                                &tentacle_name,
                                                body,
                                            );
                                            ink = ink
                                                .with_metadata(serde_json::Value::Object(metadata));

                                            info!(
                                                "Kubernetes Events Tentacle '{}': published ink on topic: {}",
                                                tentacle_name, topic
                                            );
                                            event_bus_clone.publish(ink);
                                        }
                                    }
                                }
                                Err(e) => {
                                    error!(
                                        "Kubernetes Events Tentacle '{}': error reading stream: {}",
                                        tentacle_name, e
                                    );
                                    break;
                                }
                            }
                        }
                    }
                    Err(e) => {
                        error!(
                            "Kubernetes Events Tentacle '{}': failed to connect to API: {}",
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
    fn test_k8s_topic_mode_type() {
        let config = K8sTentacleConfig {
            api_server: "https://k8s-api:6443".to_string(),
            namespace: Some("default".to_string()),
            token: None,
            kubeconfig: None,
            topic_mode: K8sTopicMode::Type,
            fallback_topic: "/kubernetes/events/default".to_string(),
            resource_mapping: HashMap::new(),
            tls_verify: false,
            ca_cert: None,
            watch_timeout_seconds: 300,
        };

        assert_eq!(
            resolve_topic(&config, "Normal", "Pod", "default"),
            "/kubernetes/events/normal"
        );
        assert_eq!(
            resolve_topic(&config, "Warning", "Node", "kube-system"),
            "/kubernetes/events/warning"
        );
        assert_eq!(
            resolve_topic(&config, "", "Pod", "default"),
            "/kubernetes/events/default"
        );
    }

    #[test]
    fn test_k8s_topic_mode_resource() {
        let config = K8sTentacleConfig {
            api_server: "https://k8s-api:6443".to_string(),
            namespace: None,
            token: None,
            kubeconfig: None,
            topic_mode: K8sTopicMode::Resource,
            fallback_topic: "/kubernetes/events/default".to_string(),
            resource_mapping: HashMap::new(),
            tls_verify: false,
            ca_cert: None,
            watch_timeout_seconds: 300,
        };

        assert_eq!(
            resolve_topic(&config, "Normal", "Pod", "default"),
            "/kubernetes/events/pod"
        );
        assert_eq!(
            resolve_topic(&config, "Warning", "Node", "kube-system"),
            "/kubernetes/events/node"
        );
    }

    #[test]
    fn test_k8s_topic_mode_namespace() {
        let config = K8sTentacleConfig {
            api_server: "https://k8s-api:6443".to_string(),
            namespace: None,
            token: None,
            kubeconfig: None,
            topic_mode: K8sTopicMode::Namespace,
            fallback_topic: "/kubernetes/events/default".to_string(),
            resource_mapping: HashMap::new(),
            tls_verify: false,
            ca_cert: None,
            watch_timeout_seconds: 300,
        };

        assert_eq!(
            resolve_topic(&config, "Normal", "Pod", "default"),
            "/kubernetes/events/namespace/default"
        );
        assert_eq!(
            resolve_topic(&config, "Warning", "Node", "kube-system"),
            "/kubernetes/events/namespace/kube-system"
        );
    }

    #[test]
    fn test_k8s_topic_mode_all() {
        let config = K8sTentacleConfig {
            api_server: "https://k8s-api:6443".to_string(),
            namespace: None,
            token: None,
            kubeconfig: None,
            topic_mode: K8sTopicMode::All,
            fallback_topic: "/kubernetes/all".to_string(),
            resource_mapping: HashMap::new(),
            tls_verify: false,
            ca_cert: None,
            watch_timeout_seconds: 300,
        };

        assert_eq!(
            resolve_topic(&config, "Normal", "Pod", "default"),
            "/kubernetes/all"
        );
    }

    #[test]
    fn test_k8s_resource_mapping() {
        let mut mapping = HashMap::new();
        mapping.insert("Pod".to_string(), "/k8s/pods".to_string());
        mapping.insert("Node".to_string(), "/k8s/nodes".to_string());

        let config = K8sTentacleConfig {
            api_server: "https://k8s-api:6443".to_string(),
            namespace: None,
            token: None,
            kubeconfig: None,
            topic_mode: K8sTopicMode::Resource,
            fallback_topic: "/kubernetes/events/default".to_string(),
            resource_mapping: mapping,
            tls_verify: false,
            ca_cert: None,
            watch_timeout_seconds: 300,
        };

        assert_eq!(
            resolve_topic(&config, "Normal", "Pod", "default"),
            "/k8s/pods"
        );
        assert_eq!(
            resolve_topic(&config, "Warning", "Node", "kube-system"),
            "/k8s/nodes"
        );
        assert_eq!(
            resolve_topic(&config, "Normal", "Deployment", "default"),
            "/kubernetes/events/deployment"
        );
    }

    #[test]
    fn test_k8s_config_deserialization() {
        let yaml = r#"
        api_server: "https://k8s-api:6443"
        namespace: "production"
        token: "eyJhbGciOiJSUzI1NiJ9..."
        topic_mode: resource
        fallback_topic: "/kubernetes/events/default"
        resource_mapping:
          "Pod": "/k8s/pods"
          "Deployment": "/k8s/deployments"
        tls_verify: true
        watch_timeout_seconds: 600
        "#;

        let config: K8sTentacleConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.api_server, "https://k8s-api:6443");
        assert_eq!(config.namespace.as_deref(), Some("production"));
        assert!(config.token.is_some());
        assert_eq!(config.topic_mode, K8sTopicMode::Resource);
        assert!(config.tls_verify);
        assert_eq!(config.watch_timeout_seconds, 600);
    }

    #[test]
    fn test_k8s_tentacle_name_and_type() {
        let config = K8sTentacleConfig {
            api_server: "https://k8s-api:6443".to_string(),
            namespace: None,
            token: None,
            kubeconfig: None,
            topic_mode: K8sTopicMode::Type,
            fallback_topic: "/kubernetes/events/default".to_string(),
            resource_mapping: HashMap::new(),
            tls_verify: false,
            ca_cert: None,
            watch_timeout_seconds: 300,
        };
        let tentacle = K8sTentacle::new("test_k8s".to_string(), config);
        assert_eq!(tentacle.name(), "test_k8s");
        assert_eq!(tentacle.r#type(), "kubernetes");
    }
}
