use anyhow::{Context, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::{debug, error, info};

use super::Reflex;
use crate::model::Ink;

/// Configuration for Grafana Reflex
/// Interacts with Grafana API for dashboards, alerts, and annotations
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GrafanaReflexConfig {
    /// Grafana server URL (e.g., "http://localhost:3000")
    pub host: String,

    /// Grafana API key
    pub api_key: String,

    /// Default action: "annotation", "alert", "dashboard", "provision"
    #[serde(default = "default_action")]
    pub action: String,

    /// Default dashboard UID
    #[serde(default)]
    pub dashboard_uid: Option<String>,

    /// Template for dashboard UID
    #[serde(default)]
    pub dashboard_uid_template: Option<String>,

    /// Template for annotation text
    #[serde(default = "default_annotation_template")]
    pub annotation_template: String,

    /// Optional: mapping from Ink topics to actions
    #[serde(default)]
    pub topic_action_mapping: HashMap<String, String>,

    /// Optional: mapping from Ink topics to dashboard UIDs
    #[serde(default)]
    pub topic_dashboard_mapping: HashMap<String, String>,

    /// Whether to verify TLS certificates
    #[serde(default = "default_verify_ssl")]
    pub verify_ssl: bool,

    /// Timeout in seconds
    #[serde(default = "default_timeout")]
    pub timeout: u64,
}

fn default_action() -> String {
    "annotation".to_string()
}
fn default_annotation_template() -> String {
    "[{{topic}}] {{body}}".to_string()
}
fn default_verify_ssl() -> bool {
    true
}
fn default_timeout() -> u64 {
    30
}

pub struct GrafanaReflex {
    name: String,
    config: GrafanaReflexConfig,
    client: reqwest::Client,
}

impl GrafanaReflex {
    pub fn new(name: String, config: GrafanaReflexConfig) -> Self {
        let verify_ssl = config.verify_ssl;
        Self {
            name,
            config,
            client: reqwest::Client::builder()
                .danger_accept_invalid_certs(!verify_ssl)
                .build()
                .unwrap_or_else(|_| reqwest::Client::new()),
        }
    }

    /// Resolve action for a given Ink
    fn resolve_action(&self, ink: &Ink) -> String {
        if let Some(action) = self
            .config
            .topic_action_mapping
            .get(&ink.topic)
            .filter(|a| !a.is_empty())
        {
            return action.clone();
        }
        self.config.action.clone()
    }

    /// Resolve dashboard UID for a given Ink
    fn resolve_dashboard_uid(&self, ink: &Ink) -> Option<String> {
        if let Some(uid) = self
            .config
            .topic_dashboard_mapping
            .get(&ink.topic)
            .filter(|u| !u.is_empty())
        {
            return Some(uid.clone());
        }
        if let Some(ref template) = self.config.dashboard_uid_template {
            return Some(self.render_template(template, ink));
        }
        self.config.dashboard_uid.clone()
    }

    fn render_template(&self, template: &str, ink: &Ink) -> String {
        let mut rendered = template.to_string();
        rendered = rendered.replace("{{topic}}", &ink.topic);
        rendered = rendered.replace("{{body}}", &ink.body);
        rendered = rendered.replace("{{id}}", &ink.id.to_string());
        rendered = rendered.replace("{{timestamp}}", &ink.timestamp.to_rfc3339());
        if let Some(obj) = ink.metadata.as_ref().and_then(|m| m.as_object()) {
            for (key, value) in obj {
                let placeholder = format!("{{{{metadata.{}}}}}", key);
                let value_str = match value {
                    serde_json::Value::String(s) => s.clone(),
                    _ => value.to_string(),
                };
                rendered = rendered.replace(&placeholder, &value_str);
            }
        }
        rendered
    }

    /// Build Grafana API URL
    fn grafana_url(&self, path: &str) -> String {
        let host = self.config.host.trim_end_matches('/');
        format!("{}/api/{}", host, path)
    }

    /// Build auth request
    fn build_auth_request(&self, request: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        request.header("Authorization", format!("Bearer {}", self.config.api_key))
    }

    /// Add an annotation to a dashboard
    async fn add_annotation(&self, dashboard_uid: &str, ink: &Ink) -> Result<serde_json::Value> {
        let url = self.grafana_url(&format!("dashboards/uid/{}/annotations", dashboard_uid));

        let annotation_text = self.render_template(&self.config.annotation_template, ink);

        let payload = serde_json::json!({
            "dashboardId": 0,
            "panelId": 0,
            "time": ink.timestamp.timestamp_nanos_opt().unwrap_or(0) / 1_000_000,
            "timeEnd": 0,
            "text": annotation_text,
            "tags": ["kraken", ink.topic.replace('/', "_")],
            "type": "event"
        });

        let response = self
            .build_auth_request(self.client.post(&url).json(&payload))
            .send()
            .await
            .with_context(|| {
                format!(
                    "Failed to add annotation to Grafana dashboard {}",
                    dashboard_uid
                )
            })?;

        if !response.status().is_success() {
            let status = response.status();
            let error_body = response.text().await.unwrap_or_default();
            return Err(anyhow::anyhow!(
                "Grafana annotation error ({}): {}",
                status,
                error_body
            ));
        }

        response
            .json()
            .await
            .context("Failed to parse Grafana annotation response")
    }

    /// Get dashboard by UID
    async fn get_dashboard(&self, uid: &str) -> Result<serde_json::Value> {
        let url = self.grafana_url(&format!("dashboards/uid/{}", uid));

        let response = self
            .build_auth_request(self.client.get(&url))
            .send()
            .await
            .with_context(|| format!("Failed to get Grafana dashboard {}", uid))?;

        if !response.status().is_success() {
            let status = response.status();
            let error_body = response.text().await.unwrap_or_default();
            return Err(anyhow::anyhow!(
                "Grafana dashboard error ({}): {}",
                status,
                error_body
            ));
        }

        response
            .json()
            .await
            .context("Failed to parse Grafana dashboard response")
    }

    /// List alert rules
    async fn list_alerts(&self) -> Result<Vec<serde_json::Value>> {
        let url = self.grafana_url("alerts");

        let response = self
            .build_auth_request(self.client.get(&url))
            .send()
            .await
            .context("Failed to list Grafana alerts")?;

        if !response.status().is_success() {
            let status = response.status();
            return Err(anyhow::anyhow!("Grafana list alerts error: {}", status));
        }

        let result: Vec<serde_json::Value> = response
            .json()
            .await
            .context("Failed to parse alerts response")?;
        Ok(result)
    }

    /// Get Grafana health
    async fn health_check(&self) -> Result<String> {
        let url = format!("{}/api/health", self.config.host.trim_end_matches('/'));

        let response = self
            .client
            .get(&url)
            .send()
            .await
            .context("Failed to check Grafana health")?;

        if !response.status().is_success() {
            let status = response.status();
            return Err(anyhow::anyhow!("Grafana health check error: {}", status));
        }

        response
            .text()
            .await
            .context("Failed to read Grafana health response")
    }

    /// Search dashboards
    async fn search_dashboards(&self, query: &str) -> Result<Vec<serde_json::Value>> {
        let url = self.grafana_url(&format!("search?query={}", urlencoding::encode(query)));

        let response = self
            .build_auth_request(self.client.get(&url))
            .send()
            .await
            .with_context(|| "Failed to search Grafana dashboards".to_string())?;

        if !response.status().is_success() {
            let status = response.status();
            return Err(anyhow::anyhow!("Grafana search error: {}", status));
        }

        let result: Vec<serde_json::Value> = response
            .json()
            .await
            .context("Failed to parse search response")?;
        Ok(result)
    }
}

#[async_trait]
impl Reflex for GrafanaReflex {
    fn name(&self) -> &str {
        &self.name
    }

    fn r#type(&self) -> &str {
        "grafana"
    }

    async fn handle(&self, ink: &Ink) -> Result<()> {
        info!(
            "Grafana reflex '{}' handling ink on topic: {}",
            self.name, ink.topic
        );

        let action = self.resolve_action(ink);
        debug!("Grafana reflex '{}' resolved action: {}", self.name, action);

        match action.as_str() {
            "annotation" | "annotate" => {
                let dashboard_uid = self
                    .resolve_dashboard_uid(ink)
                    .context("Dashboard UID is required for annotation action")?;
                let result = self.add_annotation(&dashboard_uid, ink).await?;
                info!(
                    "Grafana reflex '{}' added annotation to dashboard {}: {}",
                    self.name, dashboard_uid, result
                );
            }
            "dashboard" | "get-dashboard" => {
                let dashboard_uid = self
                    .resolve_dashboard_uid(ink)
                    .context("Dashboard UID is required for dashboard action")?;
                let dashboard = self.get_dashboard(&dashboard_uid).await?;
                debug!(
                    "Grafana reflex '{}' got dashboard: {}",
                    self.name, dashboard
                );
            }
            "alerts" | "list-alerts" => {
                let alerts = self.list_alerts().await?;
                info!(
                    "Grafana reflex '{}' found {} alerts",
                    self.name,
                    alerts.len()
                );
            }
            "health" | "ping" => {
                let health = self.health_check().await?;
                debug!("Grafana reflex '{}' health: {}", self.name, health);
            }
            "search" | "search-dashboards" => {
                let query = ink.body.clone();
                let results = self.search_dashboards(&query).await?;
                info!(
                    "Grafana reflex '{}' search for '{}' found {} results",
                    self.name,
                    query,
                    results.len()
                );
            }
            _ => {
                error!("Grafana reflex '{}' unknown action: {}", self.name, action);
                return Err(anyhow::anyhow!("Unknown Grafana action: {}", action));
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_config() -> GrafanaReflexConfig {
        GrafanaReflexConfig {
            host: "http://localhost:3000".to_string(),
            api_key: "my-api-key".to_string(),
            action: "annotation".to_string(),
            dashboard_uid: None,
            dashboard_uid_template: None,
            annotation_template: "[{{topic}}] {{body}}".to_string(),
            topic_action_mapping: HashMap::new(),
            topic_dashboard_mapping: HashMap::new(),
            verify_ssl: true,
            timeout: 30,
        }
    }

    fn create_test_ink() -> Ink {
        Ink::new("/test/topic", "test", "test_source", "test body")
    }

    #[test]
    fn test_template_rendering() {
        let config = create_test_config();
        let reflex = GrafanaReflex::new("test".to_string(), config);
        let ink = create_test_ink();
        assert_eq!(
            reflex.render_template("[{{topic}}] {{body}}", &ink),
            "[/test/topic] test body"
        );
    }

    #[test]
    fn test_config_deserialization() {
        let yaml = r#"
            host: "http://grafana:3000"
            api_key: "secret"
            action: "alerts"
        "#;
        let config: GrafanaReflexConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.host, "http://grafana:3000");
        assert_eq!(config.action, "alerts");
    }

    #[test]
    fn test_reflex_type() {
        let config = create_test_config();
        let reflex = GrafanaReflex::new("test".to_string(), config);
        assert_eq!(reflex.r#type(), "grafana");
    }

    #[test]
    fn test_resolve_action_from_mapping() {
        let mut config = create_test_config();
        let mut mapping = HashMap::new();
        mapping.insert("/grafana/alerts".to_string(), "alerts".to_string());
        config.topic_action_mapping = mapping;

        let reflex = GrafanaReflex::new("test".to_string(), config);
        let ink = Ink::new("/grafana/alerts", "test", "test_source", "body");
        assert_eq!(reflex.resolve_action(&ink), "alerts");
    }
}
