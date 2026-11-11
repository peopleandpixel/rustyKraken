use anyhow::Result;
use async_trait::async_trait;
use axum::{Router, extract::State, http::StatusCode, response::Json, routing::post};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::info;

use super::Tentacle;
use crate::eventbus::EventBus;
use crate::model::Ink;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HttpTentacleConfig {
    pub host: String,
    pub port: u16,
    #[serde(default)]
    pub path: String,
}

pub struct HttpTentacle {
    name: String,
    config: HttpTentacleConfig,
}

#[derive(Deserialize, Clone)]
#[allow(dead_code)]
pub struct WebhookPayload {
    pub topic: Option<String>,
    pub body: String,
    #[serde(default)]
    pub metadata: Option<HashMap<String, String>>,
}

impl HttpTentacle {
    pub fn new(name: String, config: HttpTentacleConfig) -> Self {
        Self { name, config }
    }
}

#[async_trait]
impl Tentacle for HttpTentacle {
    fn name(&self) -> &str {
        &self.name
    }

    fn r#type(&self) -> &str {
        "http"
    }

    async fn start(&self, event_bus: Arc<EventBus>) -> Result<()> {
        info!(
            "Starting HTTP tentacle '{}' on {}:{}",
            self.name, self.config.host, self.config.port
        );

        let path = if self.config.path.is_empty() {
            "/webhook".to_string()
        } else {
            self.config.path.clone()
        };

        let tentacle_name = self.name.clone();
        let event_bus_clone = Arc::clone(&event_bus);

        // Axum-App erstellen
        let app = Router::new()
            .route(&path, post(webhook_handler))
            .with_state((tentacle_name, event_bus_clone));

        let addr = format!("{}:{}", self.config.host, self.config.port);
        let listener = tokio::net::TcpListener::bind(&addr).await?;

        info!("HTTP tentacle '{}' listening on {}", self.name, addr);
        axum::serve(listener, app).await?;

        Ok(())
    }
}

async fn webhook_handler(
    State(tentacle_name): State<(String, Arc<EventBus>)>,
    Json(payload): Json<WebhookPayload>,
) -> StatusCode {
    let (tentacle_name, event_bus) = tentacle_name;

    // Topic bestimmen
    let topic = payload
        .topic
        .unwrap_or_else(|| "/webhook/default".to_string());

    // Ink erstellen
    let ink = Ink::new(topic, "http", &tentacle_name, &payload.body);

    // In Event Bus publishen
    event_bus.publish(ink);

    StatusCode::OK
}
