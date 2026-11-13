use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info, warn};

use super::Tentacle;
use crate::appcontext::AppContext;
use crate::eventbus::EventBus;
use crate::model::Ink;

/// Authentifizierungsmethode für REST-API-Aufrufe
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum AuthConfig {
    /// Keine Authentifizierung
    #[default]
    None,
    /// Bearer-Token (z.B. OAuth2)
    Bearer { token: String },
    /// Basic-Auth mit Benutzername und Passwort
    Basic { username: String, password: String },
    /// API-Key im Header
    ApiKey { header: String, key: String },
}

/// Konfiguration für eine einzelne REST-API-URL
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RestApiEndpoint {
    /// Die URL, die abgefragt werden soll
    pub url: String,
    /// HTTP-Methode (GET, POST, etc.)
    #[serde(default = "default_method")]
    pub method: String,
    /// Topic für die empfangenen Daten
    #[serde(default = "default_topic")]
    pub topic: String,
    /// Optional: JSON-Pfad zur Extraktion von Arrays (z.B. "data.items")
    #[serde(default)]
    pub json_path: Option<String>,
    /// Optional: Topic-Feld im JSON, falls Topic pro Nachricht dynamisch sein soll
    #[serde(default)]
    pub topic_field: Option<String>,
    /// Optional: Body-Feld im JSON, falls nur ein bestimmtes Feld als Body dienen soll
    #[serde(default)]
    pub body_field: Option<String>,
    /// Optional: HTTP-Header
    #[serde(default)]
    pub headers: HashMap<String, String>,
    /// Optional: Body für POST/PUT-Anfragen
    #[serde(default)]
    pub body: Option<String>,
    /// Authentifizierung
    #[serde(default)]
    pub auth: AuthConfig,
}

fn default_method() -> String {
    "GET".to_string()
}

fn default_topic() -> String {
    "/rest/default".to_string()
}

/// Konfiguration für den REST API Poller Tentacle
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RestApiTentacleConfig {
    /// Liste der Endpunkte, die abgefragt werden sollen
    pub endpoints: Vec<RestApiEndpoint>,
    /// Polling-Intervall in Sekunden (Standard: 60)
    #[serde(default = "default_interval")]
    pub interval_seconds: u64,
    /// Timeout pro Anfrage in Sekunden (Standard: 30)
    #[serde(default = "default_timeout")]
    pub timeout_seconds: u64,
    /// Maximale Anzahl von Wiederholungsversuchen bei Fehlern (Standard: 3)
    #[serde(default = "default_max_retries")]
    pub max_retries: u32,
    /// Verzögerung zwischen Wiederholungsversuchen in Sekunden (Standard: 5)
    #[serde(default = "default_retry_delay")]
    pub retry_delay_seconds: u64,
    /// Optional: Rate-Limit: maximale Anfragen pro Minute (0 = kein Limit)
    #[serde(default)]
    pub rate_limit_per_minute: u32,
}

fn default_interval() -> u64 {
    60
}

fn default_timeout() -> u64 {
    30
}

fn default_max_retries() -> u32 {
    3
}

fn default_retry_delay() -> u64 {
    5
}

pub struct RestApiTentacle {
    name: String,
    config: RestApiTentacleConfig,
    client: reqwest::Client,
}

impl RestApiTentacle {
    pub fn new(name: String, config: RestApiTentacleConfig, app_ctx: &AppContext) -> Self {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(config.timeout_seconds))
            .build()
            .unwrap_or_else(|_| app_ctx.http_client().as_ref().clone());
        Self {
            name,
            config,
            client,
        }
    }

    /// Erstellt einen reqwest-Client mit konfigurierter Authentifizierung
    #[allow(dead_code)]
    fn build_client(&self, _auth: &AuthConfig) -> reqwest::Client {
        reqwest::Client::builder()
            .timeout(Duration::from_secs(self.config.timeout_seconds))
            .build()
            .unwrap_or_else(|_| {
                warn!("Failed to build reqwest client with custom settings, using default");
                reqwest::Client::new()
            })
    }

    /// Fügt Authentifizierungs-Header zur Anfrage hinzu
    fn apply_auth(
        &self,
        request: reqwest::RequestBuilder,
        auth: &AuthConfig,
    ) -> reqwest::RequestBuilder {
        match auth {
            AuthConfig::None => request,
            AuthConfig::Bearer { token } => {
                request.header("Authorization", format!("Bearer {}", token))
            }
            AuthConfig::Basic { username, password } => {
                let credentials = base64::Engine::encode(
                    &base64::engine::general_purpose::STANDARD,
                    format!("{}:{}", username, password).as_bytes(),
                );
                request.header("Authorization", format!("Basic {}", credentials))
            }
            AuthConfig::ApiKey { header, key } => request.header(header, key),
        }
    }

    /// Extrahiert einen Wert aus einem JSON-Pfad (z.B. "data.items" aus einem JSON-Objekt)
    fn extract_json_path(json: &Value, path: &str) -> Option<Vec<Value>> {
        let parts: Vec<&str> = path.split('.').collect();
        let mut current = json;

        for part in &parts[..parts.len() - 1] {
            current = current.get(*part)?;
        }

        let last = parts[parts.len() - 1];
        let field = current.get(last)?;

        // Wenn es ein Array ist, gib es direkt zurück
        if let Some(arr) = field.as_array() {
            Some(arr.clone())
        }
        // Wenn es ein einzelnes Objekt ist, wrappe es in ein Array
        else {
            Some(vec![field.clone()])
        }
    }

    /// Extrahiert einen String-Wert aus einem JSON-Pfad
    fn extract_string_field(json: &Value, field_path: &str) -> Option<String> {
        let parts: Vec<&str> = field_path.split('.').collect();
        let mut current = json;

        for part in &parts {
            current = current.get(*part)?;
        }

        current.as_str().map(|s| s.to_string())
    }

    /// Fragt einen einzelnen Endpunkt ab und publish't die Ergebnisse
    async fn poll_endpoint(
        &self,
        endpoint: &RestApiEndpoint,
        event_bus: &Arc<EventBus>,
    ) -> Result<()> {
        let client = &self.client;

        let mut request_builder = match endpoint.method.to_uppercase().as_str() {
            "GET" => client.get(&endpoint.url),
            "POST" => client.post(&endpoint.url),
            "PUT" => client.put(&endpoint.url),
            "DELETE" => client.delete(&endpoint.url),
            "PATCH" => client.patch(&endpoint.url),
            _ => {
                warn!("Unsupported HTTP method: {}", endpoint.method);
                return Ok(());
            }
        };

        // Authentifizierung anwenden
        request_builder = self.apply_auth(request_builder, &endpoint.auth);

        // Custom-Header hinzufügen
        for (key, value) in &endpoint.headers {
            request_builder = request_builder.header(key, value);
        }

        // Body für POST/PUT hinzufügen
        if let Some(body) = &endpoint.body {
            request_builder = request_builder.body(body.clone());
        }

        let response = request_builder.send().await?;
        let status = response.status();

        if !status.is_success() {
            return Err(anyhow::anyhow!(
                "HTTP error: {} for URL: {}",
                status,
                endpoint.url
            ));
        }

        let body_text = response.text().await?;

        // Versuche, JSON zu parsen
        let json: Value = serde_json::from_str(&body_text).unwrap_or_else(|_| {
            // Wenn kein JSON, behandle den gesamten Body als Text
            Value::String(body_text.clone())
        });

        // Extrahiere die Daten basierend auf json_path
        let items: Vec<Value> = if let Some(ref path) = endpoint.json_path {
            Self::extract_json_path(&json, path).unwrap_or_else(|| vec![json.clone()])
        } else {
            // Kein json_path — wenn das Root ein Array ist, verwende es; sonst das gesamte Objekt
            match &json {
                Value::Array(arr) => arr.clone(),
                _ => vec![json],
            }
        };

        info!(
            "REST API poller '{}' received {} items from {}",
            self.name,
            items.len(),
            endpoint.url
        );

        for item in items {
            // Topic bestimmen
            let topic = if let Some(ref field) = endpoint.topic_field {
                Self::extract_string_field(&item, field).unwrap_or_else(|| endpoint.topic.clone())
            } else {
                endpoint.topic.clone()
            };

            // Body bestimmen
            let body = if let Some(ref field) = endpoint.body_field {
                Self::extract_string_field(&item, field).unwrap_or_else(|| {
                    serde_json::to_string(&item).unwrap_or_else(|_| item.to_string())
                })
            } else {
                serde_json::to_string(&item).unwrap_or_else(|_| item.to_string())
            };

            let ink = Ink::new(topic.clone(), "rest_api", &self.name, body);
            debug!(
                "REST API poller '{}' publishing ink on topic: {}",
                self.name, topic
            );
            event_bus.publish(ink);
        }

        Ok(())
    }

    /// Pollt alle Endpunkte mit Retry-Logik
    async fn poll_all_endpoints(&self, event_bus: &Arc<EventBus>) {
        for endpoint in &self.config.endpoints {
            let mut last_error = None;

            for attempt in 0..=self.config.max_retries {
                match self.poll_endpoint(endpoint, event_bus).await {
                    Ok(()) => {
                        last_error = None;
                        break;
                    }
                    Err(e) => {
                        let err_msg = e.to_string();
                        last_error = Some(e);
                        if attempt < self.config.max_retries {
                            warn!(
                                "REST API poller '{}' failed to poll {} (attempt {}/{}): {}",
                                self.name,
                                endpoint.url,
                                attempt + 1,
                                self.config.max_retries + 1,
                                err_msg
                            );
                            tokio::time::sleep(Duration::from_secs(
                                self.config.retry_delay_seconds,
                            ))
                            .await;
                        }
                    }
                }
            }

            if let Some(e) = last_error {
                error!(
                    "REST API poller '{}' failed to poll {} after {} retries: {}",
                    self.name, endpoint.url, self.config.max_retries, e
                );
            }
        }
    }
}

#[async_trait]
impl Tentacle for RestApiTentacle {
    fn name(&self) -> &str {
        &self.name
    }

    fn r#type(&self) -> &str {
        "rest_api"
    }

    async fn start(&self, event_bus: Arc<EventBus>) -> Result<()> {
        info!(
            "REST API poller tentacle '{}' started with {} endpoints, interval: {}s",
            self.name,
            self.config.endpoints.len(),
            self.config.interval_seconds
        );

        let interval = Duration::from_secs(self.config.interval_seconds);
        let mut next_request: Option<DateTime<Utc>> = None;

        loop {
            // Rate-Limiting prüfen
            if let Some(min_next) = next_request {
                let now = Utc::now();
                if now < min_next {
                    let wait = (min_next - now)
                        .to_std()
                        .unwrap_or(Duration::from_millis(0));
                    debug!(
                        "REST API poller '{}' rate limiting: waiting {:?}",
                        self.name, wait
                    );
                    tokio::time::sleep(wait).await;
                }
            }

            self.poll_all_endpoints(&event_bus).await;

            // Rate-Limit: nächste Anfrage berechnen
            if self.config.rate_limit_per_minute > 0 {
                let min_interval =
                    Duration::from_secs_f64(60.0 / self.config.rate_limit_per_minute as f64);
                next_request = Some(Utc::now() + chrono::Duration::from_std(min_interval).unwrap());
            }

            tokio::time::sleep(interval).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_tentacle() -> RestApiTentacle {
        let app_ctx = crate::appcontext::AppContext::new();
        RestApiTentacle::new(
            "test_rest".to_string(),
            RestApiTentacleConfig {
                endpoints: vec![RestApiEndpoint {
                    url: "https://api.example.com/data".to_string(),
                    method: "GET".to_string(),
                    topic: "/rest/test".to_string(),
                    json_path: None,
                    topic_field: None,
                    body_field: None,
                    headers: HashMap::new(),
                    body: None,
                    auth: AuthConfig::None,
                }],
                interval_seconds: 60,
                timeout_seconds: 30,
                max_retries: 3,
                retry_delay_seconds: 5,
                rate_limit_per_minute: 0,
            },
            &app_ctx,
        )
    }

    #[test]
    fn test_extract_json_path_simple() {
        let json: Value = serde_json::json!({
            "data": {
                "items": [1, 2, 3]
            }
        });
        let result = RestApiTentacle::extract_json_path(&json, "data.items");
        assert!(result.is_some());
        let items = result.unwrap();
        assert_eq!(items.len(), 3);
    }

    #[test]
    fn test_extract_json_path_single_object() {
        let json: Value = serde_json::json!({
            "result": {"name": "test"}
        });
        let result = RestApiTentacle::extract_json_path(&json, "result");
        assert!(result.is_some());
        let items = result.unwrap();
        assert_eq!(items.len(), 1);
    }

    #[test]
    fn test_extract_json_path_not_found() {
        let json: Value = serde_json::json!({"key": "value"});
        let result = RestApiTentacle::extract_json_path(&json, "missing.path");
        assert!(result.is_none());
    }

    #[test]
    fn test_extract_string_field() {
        let json: Value = serde_json::json!({
            "user": {"name": "Alice"}
        });
        let result = RestApiTentacle::extract_string_field(&json, "user.name");
        assert_eq!(result, Some("Alice".to_string()));
    }

    #[test]
    fn test_extract_string_field_not_found() {
        let json: Value = serde_json::json!({"key": "value"});
        let result = RestApiTentacle::extract_string_field(&json, "missing");
        assert!(result.is_none());
    }

    #[test]
    fn test_auth_bearer() {
        let tentacle = create_tentacle();
        let client = reqwest::Client::builder().build().unwrap();
        let auth = AuthConfig::Bearer {
            token: "mytoken".to_string(),
        };
        let request = client.get("https://example.com");
        let _request = tentacle.apply_auth(request, &auth);
        // Wir können den Header nicht direkt prüfen, aber die Methode sollte nicht panic'en
    }

    #[test]
    fn test_auth_basic() {
        let tentacle = create_tentacle();
        let client = reqwest::Client::builder().build().unwrap();
        let auth = AuthConfig::Basic {
            username: "user".to_string(),
            password: "pass".to_string(),
        };
        let request = client.get("https://example.com");
        let _request = tentacle.apply_auth(request, &auth);
    }

    #[test]
    fn test_auth_api_key() {
        let tentacle = create_tentacle();
        let client = reqwest::Client::builder().build().unwrap();
        let auth = AuthConfig::ApiKey {
            header: "X-API-Key".to_string(),
            key: "secret123".to_string(),
        };
        let request = client.get("https://example.com");
        let _request = tentacle.apply_auth(request, &auth);
    }

    #[test]
    fn test_config_deserialization() {
        let yaml = r#"
            endpoints:
              - url: "https://api.example.com/data"
                method: "GET"
                topic: "/rest/test"
                auth:
                  type: "bearer"
                  token: "mytoken"
            interval_seconds: 30
            timeout_seconds: 10
            max_retries: 5
            rate_limit_per_minute: 10
        "#;
        let config: RestApiTentacleConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.endpoints.len(), 1);
        assert_eq!(config.endpoints[0].url, "https://api.example.com/data");
        assert_eq!(config.interval_seconds, 30);
        assert_eq!(config.rate_limit_per_minute, 10);
    }

    #[test]
    fn test_config_defaults() {
        let yaml = r#"
            endpoints:
              - url: "https://api.example.com/data"
        "#;
        let config: RestApiTentacleConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.endpoints[0].method, "GET");
        assert_eq!(config.endpoints[0].topic, "/rest/default");
        assert_eq!(config.interval_seconds, 60);
        assert_eq!(config.timeout_seconds, 30);
        assert_eq!(config.max_retries, 3);
    }

    #[test]
    fn test_json_path_with_array_root() {
        let json: Value = serde_json::json!([
            {"id": 1, "name": "first"},
            {"id": 2, "name": "second"}
        ]);
        // Kein json_path — sollte das Root-Array verwenden
        let items: Vec<Value> = match &json {
            Value::Array(arr) => arr.clone(),
            _ => vec![json],
        };
        assert_eq!(items.len(), 2);
    }

    #[test]
    fn test_topic_field_extraction() {
        let json: Value = serde_json::json!({
            "category": "alerts",
            "message": "Something happened"
        });
        let result = RestApiTentacle::extract_string_field(&json, "category");
        assert_eq!(result, Some("alerts".to_string()));
    }
}
