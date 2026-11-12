use anyhow::Result;
use async_trait::async_trait;
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio_tungstenite::tungstenite::Message;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use super::Tentacle;
use crate::eventbus::EventBus;
use crate::model::Ink;

/// Topic-Erkennungsmodus für GraphQL-Subscriptions
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "lowercase")]
pub enum GraphqlTopicMode {
    /// Topic aus dem Subscription-Namen ableiten
    #[default]
    Subscription,
    /// Topic aus einem JSON-Feld im Ergebnis extrahieren
    Field,
    /// Alle Events an ein festes Topic
    All,
}

/// Konfiguration für eine einzelne GraphQL-Subscription
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphqlSubscription {
    /// Name der Subscription (wird im GraphQL-Query verwendet)
    pub name: String,
    /// GraphQL-Query-String für die Subscription
    pub query: String,
    /// Optional: Variablen als JSON-String
    #[serde(default)]
    pub variables: Option<serde_json::Value>,
    /// Optional: Override-Topic für diese Subscription
    #[serde(default)]
    pub topic: Option<String>,
}

/// Konfiguration für den GraphQL Subscriptions Tentacle
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphqlTentacleConfig {
    /// GraphQL-WebSocket-URL (z.B. "ws://localhost:4000/graphql")
    pub endpoint: String,
    /// Subscriptions die abonniert werden sollen
    pub subscriptions: Vec<GraphqlSubscription>,
    /// Topic-Erkennungsmodus
    #[serde(default)]
    pub topic_mode: GraphqlTopicMode,
    /// JSON-Feld für Topic-Extraktion (bei field-Modus)
    #[serde(default)]
    pub topic_field: Option<String>,
    /// Fallback-Topic
    #[serde(default = "default_fallback_topic")]
    pub fallback_topic: String,
    /// Optional: Subscription-Namen zu Topics mappen
    #[serde(default)]
    pub subscription_topic_mapping: HashMap<String, String>,
    /// Optional: Custom-Header für die WebSocket-Verbindung
    #[serde(default)]
    pub headers: HashMap<String, String>,
    /// Reconnect-Verzögerung in Sekunden
    #[serde(default = "default_reconnect_delay")]
    pub reconnect_delay_secs: u64,
    /// Keep-Alive-Interval in Sekunden (0 = deaktiviert)
    #[serde(default = "default_keepalive_interval")]
    pub keepalive_interval_secs: u64,
}

fn default_fallback_topic() -> String {
    "/graphql/default".to_string()
}

fn default_reconnect_delay() -> u64 {
    5
}

fn default_keepalive_interval() -> u64 {
    30
}

pub struct GraphqlTentacle {
    name: String,
    config: GraphqlTentacleConfig,
}

/// Freistehende Funktion: Bestimmt das Topic basierend auf der Subscription
fn resolve_topic(
    config: &GraphqlTentacleConfig,
    subscription_name: &str,
    data: &serde_json::Value,
) -> String {
    // Explizites Mapping hat Vorrang
    if let Some(topic) = config.subscription_topic_mapping.get(subscription_name) {
        return topic.clone();
    }

    match config.topic_mode {
        GraphqlTopicMode::Subscription => {
            format!("/graphql/{}", subscription_name)
        }
        GraphqlTopicMode::Field => {
            let topic = config
                .topic_field
                .as_deref()
                .and_then(|f| data.get(f).and_then(|v| v.as_str()))
                .filter(|s| !s.is_empty())
                .map(|s| {
                    if s.starts_with('/') {
                        s.to_string()
                    } else {
                        format!("/graphql/{}", s)
                    }
                });

            topic.unwrap_or_else(|| format!("/graphql/{}", subscription_name))
        }
        GraphqlTopicMode::All => config.fallback_topic.clone(),
    }
}

impl GraphqlTentacle {
    pub fn new(name: String, config: GraphqlTentacleConfig) -> Self {
        Self { name, config }
    }

    /// Erstellt den initialen Connection-Init-Message
    fn connection_init() -> serde_json::Value {
        json!({
            "id": "1",
            "type": "connection_init",
        })
    }

    /// Erstellt eine Subscribe-Message für eine Subscription
    fn subscribe_message(id: &str, subscription: &GraphqlSubscription) -> serde_json::Value {
        let mut payload = json!({
            "id": id,
            "type": "subscribe",
            "payload": {
                "query": subscription.query,
            },
        });

        if let Some(vars) = &subscription.variables {
            payload["payload"]["variables"] = vars.clone();
        }

        payload
    }
}

#[async_trait]
impl Tentacle for GraphqlTentacle {
    fn name(&self) -> &str {
        &self.name
    }

    fn r#type(&self) -> &str {
        "graphql"
    }

    async fn start(&self, event_bus: Arc<EventBus>) -> Result<()> {
        info!(
            "GraphQL Tentacle '{}' starting, connecting to {}",
            self.name, self.config.endpoint
        );

        let config = self.config.clone();
        let tentacle_name = self.name.clone();
        let event_bus_clone = Arc::clone(&event_bus);

        tokio::spawn(async move {
            loop {
                info!(
                    "GraphQL Tentacle '{}': connecting to {}",
                    tentacle_name, config.endpoint
                );

                // WebSocket-Verbindung aufbauen
                let (ws_stream, response) = match tokio_tungstenite::connect_async(&config.endpoint)
                    .await
                {
                    Ok((stream, resp)) => (stream, resp),
                    Err(e) => {
                        error!(
                            "GraphQL Tentacle '{}': failed to connect: {}",
                            tentacle_name, e
                        );
                        tokio::time::sleep(Duration::from_secs(config.reconnect_delay_secs)).await;
                        continue;
                    }
                };

                info!(
                    "GraphQL Tentacle '{}': connected, status {}",
                    tentacle_name,
                    response.status()
                );

                let mut ws = ws_stream;

                // Custom Header setzen (nach dem Upgrade nicht mehr möglich, daher im connect)
                // Connection-Init senden
                let init_msg = Message::Text(GraphqlTentacle::connection_init().to_string().into());
                if let Err(e) = ws.send(init_msg).await {
                    error!(
                        "GraphQL Tentacle '{}': failed to send init: {}",
                        tentacle_name, e
                    );
                    tokio::time::sleep(Duration::from_secs(config.reconnect_delay_secs)).await;
                    continue;
                }

                // Warte auf connection_ack
                let connected = matches!(
                    tokio::time::timeout(Duration::from_secs(10), ws.next()).await,
                    Ok(Some(Ok(Message::Text(ref text)))) if {
                        if let Ok(ref value) = serde_json::from_str::<serde_json::Value>(text) {
                            value.get("type").and_then(|t| t.as_str()) == Some("connection_ack")
                        } else {
                            false
                        }
                    }
                );

                if connected {
                    debug!(
                        "GraphQL Tentacle '{}': connection acknowledged",
                        tentacle_name
                    );
                } else {
                    warn!(
                        "GraphQL Tentacle '{}': no connection_ack received",
                        tentacle_name
                    );
                }

                // Subscriptions abonnieren
                let mut subscription_ids = Vec::new();
                for sub in &config.subscriptions {
                    let id = Uuid::new_v4().to_string();
                    subscription_ids.push((id.clone(), sub.clone()));

                    let sub_msg = Message::Text(
                        GraphqlTentacle::subscribe_message(&id, sub)
                            .to_string()
                            .into(),
                    );

                    if let Err(e) = ws.send(sub_msg).await {
                        error!(
                            "GraphQL Tentacle '{}': failed to subscribe to '{}': {}",
                            tentacle_name, sub.name, e
                        );
                        break;
                    }

                    info!(
                        "GraphQL Tentacle '{}': subscribed to '{}' (id: {})",
                        tentacle_name, sub.name, id
                    );
                }

                let ws_clone_name = tentacle_name.clone();

                // Nachrichtenschleife
                while let Some(result) = ws.next().await {
                    let msg = match result {
                        Ok(m) => m,
                        Err(e) => {
                            error!(
                                "GraphQL Tentacle '{}': WebSocket error: {}",
                                tentacle_name, e
                            );
                            break;
                        }
                    };

                    match msg {
                        Message::Text(text) => {
                            if let Ok(value) = serde_json::from_str::<serde_json::Value>(&text) {
                                let msg_type = value.get("type").and_then(|t| t.as_str());
                                let sub_id = value.get("id").and_then(|i| i.as_str());

                                match msg_type {
                                    Some("data") => {
                                        // Finde die passende Subscription
                                        let (subscription_name, data) = if let Some(sid) = sub_id {
                                            if let Some((_, sub)) =
                                                subscription_ids.iter().find(|(id, _)| id == sid)
                                            {
                                                if let Some(data) =
                                                    value.get("payload").and_then(|p| p.get("data"))
                                                {
                                                    (sub.name.clone(), data.clone())
                                                } else {
                                                    continue;
                                                }
                                            } else {
                                                continue;
                                            }
                                        } else {
                                            continue;
                                        };

                                        let topic =
                                            resolve_topic(&config, &subscription_name, &data);
                                        let body = data.to_string();

                                        debug!(
                                            "GraphQL Tentacle '{}': received data from '{}' on topic '{}'",
                                            tentacle_name, subscription_name, topic
                                        );

                                        // Metadaten erstellen
                                        let mut metadata = serde_json::Map::new();
                                        metadata.insert(
                                            "subscription_name".to_string(),
                                            serde_json::Value::String(subscription_name.clone()),
                                        );
                                        metadata.insert(
                                            "subscription_id".to_string(),
                                            serde_json::Value::String(
                                                sub_id.map(|s| s.to_string()).unwrap_or_default(),
                                            ),
                                        );

                                        let mut ink =
                                            Ink::new(&topic, "graphql", &ws_clone_name, body);
                                        ink =
                                            ink.with_metadata(serde_json::Value::Object(metadata));

                                        event_bus_clone.publish(ink);
                                    }
                                    Some("error") => {
                                        let sub_name = sub_id.unwrap_or("unknown");
                                        error!(
                                            "GraphQL Tentacle '{}': error from subscription '{}': {}",
                                            tentacle_name,
                                            sub_name,
                                            value
                                                .get("payload")
                                                .and_then(|p| p.get("message"))
                                                .and_then(|m| m.as_str())
                                                .unwrap_or("unknown")
                                        );
                                    }
                                    Some("complete") => {
                                        let sub_name = sub_id.unwrap_or("unknown");
                                        info!(
                                            "GraphQL Tentacle '{}': subscription '{}' completed",
                                            tentacle_name, sub_name
                                        );
                                    }
                                    Some("pong") => {
                                        debug!(
                                            "GraphQL Tentacle '{}': received pong",
                                            tentacle_name
                                        );
                                    }
                                    _ => {
                                        debug!(
                                            "GraphQL Tentacle '{}': unknown message type: {:?}",
                                            tentacle_name, msg_type
                                        );
                                    }
                                }
                            }
                        }
                        Message::Binary(_) => {
                            debug!(
                                "GraphQL Tentacle '{}': received binary message",
                                tentacle_name
                            );
                        }
                        Message::Close(_) => {
                            info!(
                                "GraphQL Tentacle '{}': connection closed by server",
                                tentacle_name
                            );
                            break;
                        }
                        _ => {}
                    }
                }

                warn!(
                    "GraphQL Tentacle '{}': connection lost, reconnecting in {} seconds",
                    tentacle_name, config.reconnect_delay_secs
                );
                tokio::time::sleep(Duration::from_secs(config.reconnect_delay_secs)).await;
            }
        });

        // Blockiere den Task
        loop {
            tokio::time::sleep(Duration::from_secs(60)).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_config() -> GraphqlTentacleConfig {
        GraphqlTentacleConfig {
            endpoint: "ws://localhost:4000/graphql".to_string(),
            subscriptions: vec![],
            topic_mode: GraphqlTopicMode::Subscription,
            topic_field: None,
            fallback_topic: "/graphql/default".to_string(),
            subscription_topic_mapping: HashMap::new(),
            headers: HashMap::new(),
            reconnect_delay_secs: 5,
            keepalive_interval_secs: 30,
        }
    }

    #[test]
    fn test_graphql_topic_mode_subscription() {
        let config = make_config();
        let data = json!({"value": 42});
        assert_eq!(
            resolve_topic(&config, "newMessage", &data),
            "/graphql/newMessage"
        );
    }

    #[test]
    fn test_graphql_topic_mode_field() {
        let mut config = make_config();
        config.topic_mode = GraphqlTopicMode::Field;
        config.topic_field = Some("topic".to_string());

        let data = json!({"topic": "/sensor/temp", "value": 42});
        assert_eq!(resolve_topic(&config, "newReading", &data), "/sensor/temp");

        // Ohne topic-Feld → Fallback
        let data = json!({"value": 42});
        assert_eq!(
            resolve_topic(&config, "newReading", &data),
            "/graphql/newReading"
        );
    }

    #[test]
    fn test_graphql_topic_mode_all() {
        let mut config = make_config();
        config.topic_mode = GraphqlTopicMode::All;
        config.fallback_topic = "/graphql/all".to_string();

        let data = json!({"value": 42});
        assert_eq!(resolve_topic(&config, "anySub", &data), "/graphql/all");
    }

    #[test]
    fn test_graphql_subscription_mapping() {
        let mut config = make_config();
        let mut mapping = HashMap::new();
        mapping.insert("newMessage".to_string(), "/chat/messages".to_string());
        mapping.insert("newUser".to_string(), "/users/created".to_string());
        config.subscription_topic_mapping = mapping;

        let data = json!({"value": 42});
        assert_eq!(
            resolve_topic(&config, "newMessage", &data),
            "/chat/messages"
        );
        assert_eq!(resolve_topic(&config, "newUser", &data), "/users/created");
        // Kein Mapping
        assert_eq!(resolve_topic(&config, "unknown", &data), "/graphql/unknown");
    }

    #[test]
    fn test_graphql_connection_init() {
        let init = GraphqlTentacle::connection_init();
        assert_eq!(init["id"], "1");
        assert_eq!(init["type"], "connection_init");
    }

    #[test]
    fn test_graphql_subscribe_message() {
        let sub = GraphqlSubscription {
            name: "newMessage".to_string(),
            query: "subscription { newMessage { id text } }".to_string(),
            variables: None,
            topic: None,
        };

        let msg = GraphqlTentacle::subscribe_message("abc123", &sub);
        assert_eq!(msg["id"], "abc123");
        assert_eq!(msg["type"], "subscribe");
        assert_eq!(
            msg["payload"]["query"],
            "subscription { newMessage { id text } }"
        );
    }

    #[test]
    fn test_graphql_subscribe_message_with_variables() {
        let sub = GraphqlSubscription {
            name: "sensorData".to_string(),
            query: "subscription($deviceId: ID!) { sensorData(deviceId: $deviceId) { value } }"
                .to_string(),
            variables: Some(json!({"deviceId": "sensor-1"})),
            topic: None,
        };

        let msg = GraphqlTentacle::subscribe_message("xyz789", &sub);
        assert_eq!(msg["payload"]["variables"]["deviceId"], "sensor-1");
    }

    #[test]
    fn test_graphql_config_deserialization() {
        let yaml = r#"
        endpoint: "wss://api.example.com/graphql"
        subscriptions:
          - name: "newMessage"
            query: "subscription { newMessage { id text } }"
          - name: "sensorData"
            query: "subscription($deviceId: ID!) { sensorData(deviceId: $deviceId) { value } }"
            variables:
              deviceId: "sensor-1"
        topic_mode: field
        topic_field: "topic"
        fallback_topic: "/graphql/events"
        subscription_topic_mapping:
          "newMessage": "/chat/messages"
        reconnect_delay_secs: 10
        keepalive_interval_secs: 60
        "#;

        let config: GraphqlTentacleConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.endpoint, "wss://api.example.com/graphql");
        assert_eq!(config.subscriptions.len(), 2);
        assert_eq!(config.topic_mode, GraphqlTopicMode::Field);
        assert_eq!(config.topic_field.as_deref(), Some("topic"));
        assert_eq!(config.reconnect_delay_secs, 10);
        assert_eq!(config.keepalive_interval_secs, 60);
    }

    #[test]
    fn test_graphql_tentacle_name_and_type() {
        let config = make_config();
        let tentacle = GraphqlTentacle::new("test-graphql".to_string(), config);

        assert_eq!(tentacle.name(), "test-graphql");
        assert_eq!(tentacle.r#type(), "graphql");
    }
}
