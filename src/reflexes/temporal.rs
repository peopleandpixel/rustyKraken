#![allow(dead_code)]

use anyhow::{Context, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::{debug, info};

use super::Reflex;
use crate::model::Ink;

/// Configuration for Temporal Reflex
/// Starts Temporal workflows via gRPC/HTTP
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TemporalReflexConfig {
    /// Temporal frontend address (e.g., "localhost:7233")
    pub host: String,

    /// Workflow type name
    pub workflow_type: String,

    /// Task queue
    #[serde(default = "default_task_queue")]
    pub task_queue: String,

    /// Template for workflow input (JSON)
    #[serde(default = "default_input_template")]
    pub input_template: String,

    /// Optional: mapping from Ink topics to workflow types
    #[serde(default)]
    pub topic_workflow_mapping: HashMap<String, String>,

    /// Optional: mapping from Ink topics to input templates
    #[serde(default)]
    pub topic_input_mapping: HashMap<String, String>,

    /// Namespace
    #[serde(default = "default_namespace")]
    pub namespace: String,

    /// API key for authentication (optional)
    #[serde(default)]
    pub api_key: Option<String>,

    /// Whether to verify TLS certificates
    #[serde(default = "default_verify_ssl")]
    pub verify_ssl: bool,

    /// Timeout in seconds
    #[serde(default = "default_timeout")]
    pub timeout: u64,
}

fn default_task_queue() -> String {
    "default".to_string()
}
fn default_input_template() -> String {
    r#"{"topic": "{{topic}}", "body": "{{body}}"}"#.to_string()
}
fn default_namespace() -> String {
    "default".to_string()
}
fn default_verify_ssl() -> bool {
    true
}
fn default_timeout() -> u64 {
    30
}

pub struct TemporalReflex {
    name: String,
    config: TemporalReflexConfig,
    client: reqwest::Client,
}

impl TemporalReflex {
    pub fn new(name: String, config: TemporalReflexConfig) -> Self {
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

    /// Resolve workflow type for a given Ink
    fn resolve_workflow_type(&self, ink: &Ink) -> String {
        if let Some(workflow) = self
            .config
            .topic_workflow_mapping
            .get(&ink.topic)
            .filter(|w| !w.is_empty())
        {
            return workflow.clone();
        }
        self.config.workflow_type.clone()
    }

    /// Resolve input template for a given Ink
    fn resolve_input_template(&self, ink: &Ink) -> String {
        if let Some(template) = self
            .config
            .topic_input_mapping
            .get(&ink.topic)
            .filter(|t| !t.is_empty())
        {
            return template.clone();
        }
        self.config.input_template.clone()
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

    /// Build Temporal API URL
    fn temporal_url(&self, path: &str) -> String {
        let host = self.config.host.trim_end_matches('/');
        let scheme = if self.config.verify_ssl { "https" } else { "http" };
        format!("{}/{}{}", scheme, host, path)
    }

    /// Start a workflow
    async fn start_workflow(
        &self,
        workflow_type: &str,
        input: &serde_json::Value,
    ) -> Result<serde_json::Value> {
        let url = self.temporal_url(&format!(
            "/api/v1/namespaces/{}/workflows",
            self.config.namespace
        ));

        let payload = serde_json::json!({
            "workflowType": workflow_type,
            "taskQueue": self.config.task_queue,
            "input": input,
            "workflowId": format!("kraken-{}", uuid::Uuid::new_v4())
        });

        let mut request = self.client.post(&url).json(&payload);

        if let Some(ref api_key) = self.config.api_key {
            request = request.header("Authorization", format!("Bearer {}", api_key));
        }

        let response = request
            .send()
            .await
            .with_context(|| format!("Failed to start Temporal workflow {}", workflow_type))?;

        if !response.status().is_success() {
            let status = response.status();
            let error_body = response.text().await.unwrap_or_default();
            return Err(anyhow::anyhow!(
                "Temporal workflow error ({}): {}",
                status,
                error_body
            ));
        }

        response.json().await.context("Failed to parse Temporal response")
    }

    /// Signal a workflow
    async fn signal_workflow(
        &self,
        workflow_id: &str,
        signal_name: &str,
        input: &serde_json::Value,
    ) -> Result<()> {
        let url = self.temporal_url(&format!(
            "/api/v1/namespaces/{}/workflows/{}/signal/{}",
            self.config.namespace, workflow_id, signal_name
        ));

        let payload = serde_json::json!({
            "input": input
        });

        let mut request = self.client.post(&url).json(&payload);

        if let Some(ref api_key) = self.config.api_key {
            request = request.header("Authorization", format!("Bearer {}", api_key));
        }

        let response = request
            .send()
            .await
            .with_context(|| format!("Failed to signal Temporal workflow {}", workflow_id))?;

        if !response.status().is_success() {
            let status = response.status();
            let error_body = response.text().await.unwrap_or_default();
            return Err(anyhow::anyhow!(
                "Temporal signal error ({}): {}",
                status,
                error_body
            ));
        }

        Ok(())
    }

    /// Query a workflow
    async fn query_workflow(
        &self,
        workflow_id: &str,
        query_type: &str,
    ) -> Result<serde_json::Value> {
        let url = self.temporal_url(&format!(
            "/api/v1/namespaces/{}/workflows/{}/query/{}",
            self.config.namespace, workflow_id, query_type
        ));

        let mut request = self.client.get(&url);

        if let Some(ref api_key) = self.config.api_key {
            request = request.header("Authorization", format!("Bearer {}", api_key));
        }

        let response = request
            .send()
            .await
            .with_context(|| format!("Failed to query Temporal workflow {}", workflow_id))?;

        if !response.status().is_success() {
            let status = response.status();
            let error_body = response.text().await.unwrap_or_default();
            return Err(anyhow::anyhow!(
                "Temporal query error ({}): {}",
                status,
                error_body
            ));
        }

        response.json().await.context("Failed to parse Temporal query response")
    }
}

#[async_trait]
impl Reflex for TemporalReflex {
    fn name(&self) -> &str {
        &self.name
    }

    fn r#type(&self) -> &str {
        "temporal"
    }

    async fn handle(&self, ink: &Ink) -> Result<()> {
        info!(
            "Temporal reflex '{}' handling ink on topic: {}",
            self.name, ink.topic
        );

        let workflow_type = self.resolve_workflow_type(ink);
        debug!(
            "Temporal reflex '{}' resolved workflow type: {}",
            self.name, workflow_type
        );

        // Render input
        let input_template = self.resolve_input_template(ink);
        let input_rendered = self.render_template(&input_template, ink);
        let input: serde_json::Value = serde_json::from_str(&input_rendered)
            .context("input_template must render to valid JSON")?;

        debug!(
            "Temporal reflex '{}' input: {}",
            self.name, input
        );

        // Start workflow
        let result = self.start_workflow(&workflow_type, &input).await?;
        let workflow_id = result["workflowId"].as_str().unwrap_or("unknown");

        info!(
            "Temporal reflex '{}' started workflow {} (ID: {})",
            self.name, workflow_type, workflow_id
        );

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_config() -> TemporalReflexConfig {
        TemporalReflexConfig {
            host: "localhost:7233".to_string(),
            workflow_type: "my-workflow".to_string(),
            task_queue: "default".to_string(),
            input_template: default_input_template(),
            topic_workflow_mapping: HashMap::new(),
            topic_input_mapping: HashMap::new(),
            namespace: "default".to_string(),
            api_key: None,
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
        let reflex = TemporalReflex::new("test".to_string(), config.clone());
        let ink = create_test_ink();
        let rendered = reflex.render_template(&config.input_template, &ink);
        let input: serde_json::Value = serde_json::from_str(&rendered).unwrap();
        assert_eq!(input["topic"], "/test/topic");
        assert_eq!(input["body"], "test body");
    }

    #[test]
    fn test_config_deserialization() {
        let yaml = r#"
            host: "temporal:7233"
            workflow_type: "deploy-workflow"
            task_queue: "deploy"
            namespace: "production"
        "#;
        let config: TemporalReflexConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.workflow_type, "deploy-workflow");
        assert_eq!(config.namespace, "production");
    }

    #[test]
    fn test_reflex_type() {
        let config = create_test_config();
        let reflex = TemporalReflex::new("test".to_string(), config);
        assert_eq!(reflex.r#type(), "temporal");
    }

    #[test]
    fn test_resolve_workflow_from_mapping() {
        let mut config = create_test_config();
        let mut mapping = HashMap::new();
        mapping.insert("/temporal/deploy".to_string(), "deploy-workflow".to_string());
        config.topic_workflow_mapping = mapping;

        let reflex = TemporalReflex::new("test".to_string(), config);
        let ink = Ink::new("/temporal/deploy", "test", "test_source", "body");
        assert_eq!(reflex.resolve_workflow_type(&ink), "deploy-workflow");
    }
}
