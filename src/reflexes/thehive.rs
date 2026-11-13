use anyhow::{Context, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::{debug, error, info};

use super::Reflex;
use crate::model::Ink;

/// Configuration for TheHive Reflex
/// Interacts with TheHive SOAR platform API
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TheHiveReflexConfig {
    /// TheHive server URL (e.g., "http://localhost:9000")
    pub host: String,

    /// TheHive API key
    pub api_key: String,

    /// Default action: "case", "alert", "observe", "ioc"
    #[serde(default = "default_action")]
    pub action: String,

    /// Default case title template
    #[serde(default = "default_title_template")]
    pub title_template: String,

    /// Default case description template
    #[serde(default = "default_description_template")]
    pub description_template: String,

    /// Default case severity (1-4)
    #[serde(default = "default_severity")]
    pub severity: u8,

    /// Default case tags
    #[serde(default)]
    pub tags: Vec<String>,

    /// Optional: mapping from Ink topics to actions
    #[serde(default)]
    pub topic_action_mapping: HashMap<String, String>,

    /// Optional: mapping from Ink topics to severity
    #[serde(default)]
    pub topic_severity_mapping: HashMap<String, u8>,

    /// Whether to verify TLS certificates
    #[serde(default = "default_verify_ssl")]
    pub verify_ssl: bool,

    /// Timeout in seconds
    #[serde(default = "default_timeout")]
    pub timeout: u64,
}

fn default_action() -> String {
    "case".to_string()
}
fn default_title_template() -> String {
    "[Kraken] {{topic}}".to_string()
}
fn default_description_template() -> String {
    "{{body}}".to_string()
}
fn default_severity() -> u8 {
    2
}
fn default_verify_ssl() -> bool {
    true
}
fn default_timeout() -> u64 {
    30
}

pub struct TheHiveReflex {
    name: String,
    config: TheHiveReflexConfig,
    client: reqwest::Client,
}

impl TheHiveReflex {
    pub fn new(name: String, config: TheHiveReflexConfig) -> Self {
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

    /// Resolve severity for a given Ink
    fn resolve_severity(&self, ink: &Ink) -> u8 {
        if let Some(severity) = self.config.topic_severity_mapping.get(&ink.topic) {
            return *severity;
        }
        self.config.severity
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

    /// Build TheHive API URL
    fn thehive_url(&self, path: &str) -> String {
        let host = self.config.host.trim_end_matches('/');
        format!("{}/api/{}", host, path)
    }

    /// Build auth request
    fn build_auth_request(&self, request: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        request.header("X-Auth-Token", &self.config.api_key)
    }

    /// Create a new case
    async fn create_case(&self, ink: &Ink) -> Result<serde_json::Value> {
        let url = self.thehive_url("case");

        let title = self.render_template(&self.config.title_template, ink);
        let description = self.render_template(&self.config.description_template, ink);
        let severity = self.resolve_severity(ink);

        let mut tags = self.config.tags.clone();
        tags.push("kraken".to_string());
        tags.push(ink.topic.replace('/', "_"));

        let payload = serde_json::json!({
            "title": title,
            "description": description,
            "severity": severity,
            "tags": tags,
            "flags": [],
            "tlp": 2,
            "pap": 1,
            "status": "New",
            "owner": "kraken",
            "severityLabel": match severity {
                1 => "low",
                2 => "medium",
                3 => "high",
                _ => "critical"
            }
        });

        let response = self
            .build_auth_request(self.client.post(&url).json(&payload))
            .send()
            .await
            .with_context(|| "Failed to create case in TheHive".to_string())?;

        if !response.status().is_success() {
            let status = response.status();
            let error_body = response.text().await.unwrap_or_default();
            return Err(anyhow::anyhow!(
                "TheHive create case error ({}): {}",
                status,
                error_body
            ));
        }

        response.json().await.context("Failed to parse TheHive case response")
    }

    /// Create an alert
    async fn create_alert(&self, ink: &Ink) -> Result<serde_json::Value> {
        let url = self.thehive_url("alert");

        let _title = self.render_template(&self.config.title_template, ink);
        let description = self.render_template(&self.config.description_template, ink);

        let payload = serde_json::json!({
            "type": "kraken_alert",
            "source": "kraken",
            "sourceRef": ink.id.to_string(),
            "sightings": 1,
            "message": description,
            "severity": self.resolve_severity(ink),
            "tags": ["kraken", ink.topic.replace('/', "_")],
            "tlp": 2,
            "pap": 1,
            "status": "New",
            "artifacts": []
        });

        let response = self
            .build_auth_request(self.client.post(&url).json(&payload))
            .send()
            .await
            .with_context(|| "Failed to create alert in TheHive".to_string())?;

        if !response.status().is_success() {
            let status = response.status();
            let error_body = response.text().await.unwrap_or_default();
            return Err(anyhow::anyhow!(
                "TheHive create alert error ({}): {}",
                status,
                error_body
            ));
        }

        response.json().await.context("Failed to parse TheHive alert response")
    }

    /// Add an observation to a case
    async fn add_observation(
        &self,
        case_id: &str,
        message: &str,
        tlp: u8,
    ) -> Result<serde_json::Value> {
        let url = self.thehive_url(&format!("case/{}/observation", case_id));

        let payload = serde_json::json!({
            "message": message,
            "tlp": tlp,
            "artifacts": []
        });

        let response = self
            .build_auth_request(self.client.post(&url).json(&payload))
            .send()
            .await
            .with_context(|| format!("Failed to add observation to TheHive case {}", case_id))?;

        if !response.status().is_success() {
            let status = response.status();
            let error_body = response.text().await.unwrap_or_default();
            return Err(anyhow::anyhow!(
                "TheHive observation error ({}): {}",
                status,
                error_body
            ));
        }

        response.json().await.context("Failed to parse TheHive observation response")
    }

    /// Search cases
    async fn search_cases(&self, query: &str) -> Result<Vec<serde_json::Value>> {
        let url = self.thehive_url(&format!("case/search/1/100?query={}", urlencoding::encode(query)));

        let response = self
            .build_auth_request(self.client.get(&url))
            .send()
            .await
            .with_context(|| "Failed to search TheHive cases".to_string())?;

        if !response.status().is_success() {
            let status = response.status();
            return Err(anyhow::anyhow!("TheHive search error: {}", status));
        }

        let result: Vec<serde_json::Value> =
            response.json().await.context("Failed to parse search response")?;
        Ok(result)
    }

    /// Get case by ID
    async fn get_case(&self, case_id: &str) -> Result<serde_json::Value> {
        let url = self.thehive_url(&format!("case/{}", case_id));

        let response = self
            .build_auth_request(self.client.get(&url))
            .send()
            .await
            .with_context(|| format!("Failed to get TheHive case {}", case_id))?;

        if !response.status().is_success() {
            let status = response.status();
            let error_body = response.text().await.unwrap_or_default();
            return Err(anyhow::anyhow!(
                "TheHive get case error ({}): {}",
                status,
                error_body
            ));
        }

        response.json().await.context("Failed to parse TheHive case response")
    }
}

#[async_trait]
impl Reflex for TheHiveReflex {
    fn name(&self) -> &str {
        &self.name
    }

    fn r#type(&self) -> &str {
        "thehive"
    }

    async fn handle(&self, ink: &Ink) -> Result<()> {
        info!(
            "TheHive reflex '{}' handling ink on topic: {}",
            self.name, ink.topic
        );

        let action = self.resolve_action(ink);
        debug!(
            "TheHive reflex '{}' resolved action: {}",
            self.name, action
        );

        match action.as_str() {
            "case" | "create-case" => {
                let result = self.create_case(ink).await?;
                let case_id = result["id"].as_str().unwrap_or("unknown");
                info!(
                    "TheHive reflex '{}' created case: {}",
                    self.name, case_id
                );
            }
            "alert" | "create-alert" => {
                let result = self.create_alert(ink).await?;
                let alert_id = result["id"].as_str().unwrap_or("unknown");
                info!(
                    "TheHive reflex '{}' created alert: {}",
                    self.name, alert_id
                );
            }
            "observe" | "observation" => {
                let case_id = ink.body.clone();
                let message = self.render_template(&self.config.description_template, ink);
                let _result = self.add_observation(&case_id, &message, 2).await?;
                info!(
                    "TheHive reflex '{}' added observation to case: {}",
                    self.name, case_id
                );
            }
            "search" | "search-cases" => {
                let query = ink.body.clone();
                let results = self.search_cases(&query).await?;
                info!(
                    "TheHive reflex '{}' search for '{}' found {} results",
                    self.name, query, results.len()
                );
            }
            "get" | "get-case" => {
                let case_id = ink.body.clone();
                let case = self.get_case(&case_id).await?;
                debug!(
                    "TheHive reflex '{}' got case: {}",
                    self.name, case
                );
            }
            _ => {
                error!(
                    "TheHive reflex '{}' unknown action: {}",
                    self.name, action
                );
                return Err(anyhow::anyhow!("Unknown TheHive action: {}", action));
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_config() -> TheHiveReflexConfig {
        TheHiveReflexConfig {
            host: "http://localhost:9000".to_string(),
            api_key: "my-api-key".to_string(),
            action: "case".to_string(),
            title_template: "[Kraken] {{topic}}".to_string(),
            description_template: "{{body}}".to_string(),
            severity: 2,
            tags: vec!["kraken".to_string()],
            topic_action_mapping: HashMap::new(),
            topic_severity_mapping: HashMap::new(),
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
        let reflex = TheHiveReflex::new("test".to_string(), config);
        let ink = create_test_ink();
        assert_eq!(
            reflex.render_template("[Kraken] {{topic}}", &ink),
            "[Kraken] /test/topic"
        );
    }

    #[test]
    fn test_config_deserialization() {
        let yaml = r#"
            host: "http://thehive:9000"
            api_key: "secret"
            action: "alert"
            severity: 3
        "#;
        let config: TheHiveReflexConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.host, "http://thehive:9000");
        assert_eq!(config.severity, 3);
    }

    #[test]
    fn test_reflex_type() {
        let config = create_test_config();
        let reflex = TheHiveReflex::new("test".to_string(), config);
        assert_eq!(reflex.r#type(), "thehive");
    }

    #[test]
    fn test_resolve_severity_from_mapping() {
        let mut config = create_test_config();
        let mut mapping = HashMap::new();
        mapping.insert("/alerts/critical".to_string(), 4);
        config.topic_severity_mapping = mapping;

        let reflex = TheHiveReflex::new("test".to_string(), config);
        let ink = Ink::new("/alerts/critical", "test", "test_source", "body");
        assert_eq!(reflex.resolve_severity(&ink), 4);
    }
}
