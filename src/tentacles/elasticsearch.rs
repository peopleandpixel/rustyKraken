use anyhow::Result;
use async_trait::async_trait;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info};

use super::Tentacle;
use crate::eventbus::EventBus;
use crate::model::Ink;

/// Topic-Erkennungsmodus für Elasticsearch-Dokumente
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "lowercase")]
pub enum EsTopicMode {
    /// Topic aus dem Index-Namen ableiten
    #[default]
    Index,
    /// Topic aus einem Dokument-Feld extrahieren
    Field,
    /// Alle Dokumente an ein festes Topic
    All,
}

/// Authentifizierungstyp für Elasticsearch
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum EsAuth {
    #[default]
    None,
    Basic {
        username: String,
        password: String,
    },
    ApiKey {
        api_key: String,
    },
    Bearer {
        token: String,
    },
}

/// Konfiguration für den Elasticsearch Tentacle
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EsTentacleConfig {
    /// Elasticsearch-URL (z.B. "http://localhost:9200")
    #[serde(default = "default_endpoint")]
    pub endpoint: String,
    /// Index-Namen zum Überwachen (kommagetrennt oder Wildcard "*")
    #[serde(default = "default_indices")]
    pub indices: String,
    /// Such-Query (JSON-String, Standard: match_all)
    #[serde(default = "default_query")]
    pub query: String,
    /// Polling-Interval in Sekunden
    #[serde(default = "default_poll_interval")]
    pub poll_interval_secs: u64,
    /// Topic-Erkennungsmodus
    #[serde(default)]
    pub topic_mode: EsTopicMode,
    /// Feldname für Topic-Extraktion (bei field-Modus)
    #[serde(default)]
    pub topic_field: Option<String>,
    /// Fallback-Topic
    #[serde(default = "default_fallback_topic")]
    pub fallback_topic: String,
    /// Optional: Index-basiertes Topic-Mapping
    #[serde(default)]
    pub index_topic_mapping: HashMap<String, String>,
    /// Authentifizierung
    #[serde(default)]
    pub auth: EsAuth,
    /// Scroll-Timeout für große Ergebnismengen
    #[serde(default = "default_scroll_timeout")]
    pub scroll_timeout: String,
    /// Maximale Dokumente pro Poll
    #[serde(default = "default_max_docs")]
    pub max_docs_per_poll: u32,
    /// Feld für Change-Detection (z.B. "@timestamp" oder "_id")
    #[serde(default)]
    pub last_seen_field: Option<String>,
    /// Letzter gesehener Wert (wird automatisch aktualisiert)
    #[serde(default)]
    pub last_seen_value: Option<String>,
    /// TLS-Zertifikatsprüfung deaktivieren
    #[serde(default)]
    pub insecure: bool,
}

fn default_endpoint() -> String {
    "http://localhost:9200".to_string()
}

fn default_indices() -> String {
    "*".to_string()
}

fn default_query() -> String {
    r#"{"query":{"match_all":{}}}"#.to_string()
}

fn default_poll_interval() -> u64 {
    30
}

fn default_fallback_topic() -> String {
    "/elasticsearch/default".to_string()
}

fn default_scroll_timeout() -> String {
    "1m".to_string()
}

fn default_max_docs() -> u32 {
    100
}

pub struct EsTentacle {
    name: String,
    config: EsTentacleConfig,
}

impl EsTentacle {
    pub fn new(name: String, config: EsTentacleConfig) -> Self {
        Self { name, config }
    }
}

/// Freistehende Funktion: Bestimmt das Topic basierend auf dem Dokument
fn resolve_topic(config: &EsTentacleConfig, index_name: &str, doc: &serde_json::Value) -> String {
    match config.topic_mode {
        EsTopicMode::Index => {
            if let Some(topic) = config.index_topic_mapping.get(index_name) {
                return topic.clone();
            }
            format!("/elasticsearch/{}", index_name)
        }
        EsTopicMode::Field => {
            if let Some(ref field) = config.topic_field
                && let Some(val) = doc.get(field)
                && let Some(str_val) = val.as_str()
            {
                return str_val.to_string();
            }
            config.fallback_topic.clone()
        }
        EsTopicMode::All => config.fallback_topic.clone(),
    }
}

/// Freistehende Funktion: Erstellt HTTP-Header für Elasticsearch
fn es_headers(config: &EsAuth) -> HashMap<String, String> {
    let mut headers = HashMap::new();
    headers.insert("Content-Type".to_string(), "application/json".to_string());

    match config {
        EsAuth::None => {}
        EsAuth::Basic { username, password } => {
            let creds = base64::Engine::encode(
                &base64::engine::general_purpose::STANDARD,
                format!("{}:{}", username, password).as_bytes(),
            );
            headers.insert("Authorization".to_string(), format!("Basic {}", creds));
        }
        EsAuth::ApiKey { api_key } => {
            headers.insert("Authorization".to_string(), format!("ApiKey {}", api_key));
        }
        EsAuth::Bearer { token } => {
            headers.insert("Authorization".to_string(), format!("Bearer {}", token));
        }
    }

    headers
}

/// Freistehende Funktion: Erstellt eine Ink-Nachricht aus einem Elasticsearch-Dokument
fn create_es_ink(
    topic: &str,
    tentacle_name: &str,
    index: &str,
    doc_id: &str,
    doc: &serde_json::Value,
) -> Ink {
    let mut metadata = serde_json::Map::new();
    metadata.insert(
        "es_index".to_string(),
        serde_json::Value::String(index.to_string()),
    );
    metadata.insert(
        "es_doc_id".to_string(),
        serde_json::Value::String(doc_id.to_string()),
    );

    // Dokument-Felder als Metadaten hinzufügen
    if let serde_json::Value::Object(fields) = doc {
        for (key, value) in fields {
            metadata.insert(key.clone(), value.clone());
        }
    }

    let body = doc.to_string();
    let mut ink = Ink::new(topic, "elasticsearch", tentacle_name, body);
    ink = ink.with_metadata(serde_json::Value::Object(metadata));
    ink
}

#[async_trait]
impl Tentacle for EsTentacle {
    fn name(&self) -> &str {
        &self.name
    }

    fn r#type(&self) -> &str {
        "elasticsearch"
    }

    async fn start(&self, event_bus: Arc<EventBus>) -> Result<()> {
        info!(
            "Elasticsearch Tentacle '{}' starting for indices '{}'",
            self.name, self.config.indices
        );

        let config = self.config.clone();
        let tentacle_name = self.name.clone();
        let event_bus_clone = Arc::clone(&event_bus);

        let client = Client::builder()
            .timeout(Duration::from_secs(60))
            .build()
            .map_err(|e| anyhow::anyhow!("Failed to build HTTP client: {}", e))?;

        let headers = es_headers(&config.auth);
        let mut last_seen = config.last_seen_value.clone();

        tokio::spawn(async move {
            loop {
                // Such-URL aufbauen
                let indices = config.indices.replace(' ', "");
                let url = format!(
                    "{}/{}/_search?scroll={}&size={}",
                    config.endpoint.trim_end_matches('/'),
                    indices,
                    config.scroll_timeout,
                    config.max_docs_per_poll
                );

                debug!(
                    "Elasticsearch Tentacle '{}': polling {}",
                    tentacle_name, url
                );

                // Query anpassen für Change-Detection
                let mut query = config.query.clone();
                if let Some(ref field) = config.last_seen_field
                    && let Some(ref last_val) = last_seen
                {
                    // Einfache Filter-Erweiterung
                    let filter = format!(
                        "{{\"query\":{{\"bool\":{{\"must\":[{}],{{\"range\":{{\"{}\":{{\"gt\":\"{}\"}}}}}}]}}}}}}",
                        config.query, field, last_val
                    );
                    query = filter;
                }

                let mut request_builder = client.post(&url);
                for (key, value) in &headers {
                    request_builder = request_builder.header(key, value);
                }
                request_builder = request_builder.body(query.clone());

                match request_builder.send().await {
                    Ok(response) => {
                        if response.status().is_success()
                            && let Ok(body) = response.text().await
                            && let Ok(search_result) =
                                serde_json::from_str::<serde_json::Value>(&body)
                        {
                            let hits = search_result
                                .get("hits")
                                .and_then(|h| h.get("hits"))
                                .and_then(|h| h.as_array());

                            if let Some(hits) = hits {
                                info!(
                                    "Elasticsearch Tentacle '{}': received {} hits",
                                    tentacle_name,
                                    hits.len()
                                );

                                for hit in hits {
                                    let index = hit
                                        .get("_index")
                                        .and_then(|i| i.as_str())
                                        .unwrap_or("unknown")
                                        .to_string();

                                    let doc_id = hit
                                        .get("_id")
                                        .and_then(|i| i.as_str())
                                        .unwrap_or("unknown")
                                        .to_string();

                                    let doc =
                                        hit.get("_source").unwrap_or(&serde_json::Value::Null);

                                    let topic = resolve_topic(&config, &index, doc);
                                    let ink =
                                        create_es_ink(&topic, &tentacle_name, &index, &doc_id, doc);
                                    event_bus_clone.publish(ink);

                                    // Last seen aktualisieren
                                    if let Some(ref field) = config.last_seen_field
                                        && let Some(val) = doc.get(field)
                                        && let Some(str_val) = val.as_str()
                                    {
                                        last_seen = Some(str_val.to_string());
                                    }
                                }
                            }
                        }
                    }
                    Err(e) => {
                        error!(
                            "Elasticsearch Tentacle '{}': request failed: {}",
                            tentacle_name, e
                        );
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::appcontext::AppContext;

    fn make_config() -> EsTentacleConfig {
        EsTentacleConfig {
            endpoint: "http://localhost:9200".to_string(),
            indices: "*".to_string(),
            query: r#"{"query":{"match_all":{}}}"#.to_string(),
            poll_interval_secs: 30,
            topic_mode: EsTopicMode::Index,
            topic_field: None,
            fallback_topic: "/elasticsearch/default".to_string(),
            index_topic_mapping: HashMap::new(),
            auth: EsAuth::None,
            scroll_timeout: "1m".to_string(),
            max_docs_per_poll: 100,
            last_seen_field: None,
            last_seen_value: None,
            insecure: false,
        }
    }

    #[test]
    fn test_es_topic_mode_index() {
        let config = make_config();
        let doc = serde_json::json!({"message": "test"});
        assert_eq!(
            resolve_topic(&config, "logs-2024.01.01", &doc),
            "/elasticsearch/logs-2024.01.01"
        );
    }

    #[test]
    fn test_es_topic_mode_index_mapping() {
        let mut config = make_config();
        let mut mapping = HashMap::new();
        mapping.insert("logs-2024.01.01".to_string(), "/logs/app".to_string());
        config.index_topic_mapping = mapping;

        let doc = serde_json::json!({"message": "test"});
        assert_eq!(resolve_topic(&config, "logs-2024.01.01", &doc), "/logs/app");
    }

    #[test]
    fn test_es_topic_mode_field() {
        let mut config = make_config();
        config.topic_mode = EsTopicMode::Field;
        config.topic_field = Some("event_type".to_string());

        let doc = serde_json::json!({"event_type": "error", "message": "fail"});
        assert_eq!(resolve_topic(&config, "logs", &doc), "error");
    }

    #[test]
    fn test_es_topic_mode_field_missing() {
        let mut config = make_config();
        config.topic_mode = EsTopicMode::Field;
        config.topic_field = Some("event_type".to_string());

        let doc = serde_json::json!({"other": "value"});
        assert_eq!(
            resolve_topic(&config, "logs", &doc),
            "/elasticsearch/default"
        );
    }

    #[test]
    fn test_es_topic_mode_all() {
        let mut config = make_config();
        config.topic_mode = EsTopicMode::All;
        config.fallback_topic = "/elasticsearch/all".to_string();

        let doc = serde_json::json!({"message": "test"});
        assert_eq!(resolve_topic(&config, "logs", &doc), "/elasticsearch/all");
    }

    #[test]
    fn test_es_headers_basic_auth() {
        let auth = EsAuth::Basic {
            username: "elastic".to_string(),
            password: "changeme".to_string(),
        };
        let headers = es_headers(&auth);
        assert!(headers.get("Authorization").unwrap().starts_with("Basic "));
    }

    #[test]
    fn test_es_headers_api_key() {
        let auth = EsAuth::ApiKey {
            api_key: "my-api-key".to_string(),
        };
        let headers = es_headers(&auth);
        assert_eq!(
            headers.get("Authorization"),
            Some(&"ApiKey my-api-key".to_string())
        );
    }

    #[test]
    fn test_es_headers_bearer() {
        let auth = EsAuth::Bearer {
            token: "my-token".to_string(),
        };
        let headers = es_headers(&auth);
        assert_eq!(
            headers.get("Authorization"),
            Some(&"Bearer my-token".to_string())
        );
    }

    #[test]
    fn test_create_es_ink() {
        let doc = serde_json::json!({"message": "test", "level": "info"});
        let ink = create_es_ink("/elasticsearch/test", "es-1", "logs", "doc-123", &doc);

        assert_eq!(ink.topic, "/elasticsearch/test");
        assert_eq!(ink.source.r#type, "elasticsearch");
        assert_eq!(ink.source.name, "es-1");

        if let Some(serde_json::Value::Object(meta)) = &ink.metadata {
            assert_eq!(meta.get("es_index").and_then(|v| v.as_str()), Some("logs"));
            assert_eq!(
                meta.get("es_doc_id").and_then(|v| v.as_str()),
                Some("doc-123")
            );
            assert_eq!(meta.get("message").and_then(|v| v.as_str()), Some("test"));
        } else {
            panic!("Expected metadata to be an object");
        }
    }

    #[test]
    fn test_es_tentacle_new() {
        let config = make_config();
        let tentacle = EsTentacle::new("test-es".to_string(), config);
        assert_eq!(tentacle.name(), "test-es");
        assert_eq!(tentacle.r#type(), "elasticsearch");
    }

    #[test]
    fn test_es_config_deserialization() {
        let yaml = r#"
endpoint: "http://localhost:9200"
indices: "logs-*,metrics-*"
poll_interval_secs: 60
topic_mode: field
topic_field: event_type
auth:
  type: basic
  username: elastic
  password: changeme
max_docs_per_poll: 500
last_seen_field: "@timestamp"
"#;

        let config: EsTentacleConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.endpoint, "http://localhost:9200");
        assert_eq!(config.indices, "logs-*,metrics-*");
        assert_eq!(config.poll_interval_secs, 60);
        assert_eq!(config.topic_mode, EsTopicMode::Field);
        assert_eq!(config.max_docs_per_poll, 500);
        assert_eq!(config.last_seen_field, Some("@timestamp".to_string()));
    }
}
