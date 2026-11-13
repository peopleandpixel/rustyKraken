#![allow(dead_code)]

use anyhow::{Context, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::{debug, info};

use super::Reflex;
use crate::model::Ink;

/// Configuration for Elasticsearch Reflex
/// Indexes documents to Elasticsearch
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ElasticsearchReflexConfig {
    /// Elasticsearch host URL (e.g., "http://localhost:9200")
    pub host: String,

    /// Default index name
    #[serde(default = "default_index")]
    pub index: String,

    /// Template for index name
    #[serde(default)]
    pub index_template: Option<String>,

    /// Document ID field
    #[serde(default)]
    pub document_id: Option<String>,

    /// Template for document ID
    #[serde(default)]
    pub document_id_template: Option<String>,

    /// Template for document body (JSON)
    #[serde(default = "default_body_template")]
    pub body_template: String,

    /// Optional: mapping from Ink topics to index names
    #[serde(default)]
    pub topic_index_mapping: HashMap<String, String>,

    /// API key for authentication (optional)
    #[serde(default)]
    pub api_key: Option<String>,

    /// Username for basic auth (optional)
    #[serde(default)]
    pub username: Option<String>,

    /// Password for basic auth (optional)
    #[serde(default)]
    pub password: Option<String>,

    /// Whether to verify TLS certificates
    #[serde(default = "default_verify_ssl")]
    pub verify_ssl: bool,

    /// Timeout in seconds
    #[serde(default = "default_timeout")]
    pub timeout: u64,

    /// Custom headers
    #[serde(default)]
    pub custom_headers: HashMap<String, String>,
}

fn default_index() -> String {
    "kraken-logs".to_string()
}
fn default_body_template() -> String {
    r#"{"topic": "{{topic}}", "body": "{{body}}", "id": "{{id}}", "timestamp": "{{timestamp}}"}"#
        .to_string()
}
fn default_verify_ssl() -> bool {
    true
}
fn default_timeout() -> u64 {
    30
}

pub struct ElasticsearchReflex {
    name: String,
    config: ElasticsearchReflexConfig,
    client: reqwest::Client,
}

impl ElasticsearchReflex {
    pub fn new(name: String, config: ElasticsearchReflexConfig) -> Self {
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

    /// Resolve index name for a given Ink
    fn resolve_index(&self, ink: &Ink) -> String {
        if let Some(index) = self
            .config
            .topic_index_mapping
            .get(&ink.topic)
            .filter(|i| !i.is_empty())
        {
            return index.clone();
        }
        if let Some(ref template) = self.config.index_template {
            return self.render_template(template, ink);
        }
        self.config.index.clone()
    }

    /// Resolve document ID for a given Ink
    fn resolve_document_id(&self, ink: &Ink) -> Option<String> {
        if let Some(ref template) = self.config.document_id_template {
            return Some(self.render_template(template, ink));
        }
        self.config.document_id.clone()
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

    /// Build auth request
    fn build_auth_request(&self, request: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        let mut req = request;
        if let Some(ref api_key) = self.config.api_key {
            req = req.header("Authorization", format!("ApiKey {}", api_key));
        } else if let (Some(user), Some(pass)) = (&self.config.username, &self.config.password) {
            req = req.basic_auth(user, Some(pass));
        }
        for (k, v) in &self.config.custom_headers {
            req = req.header(k, v);
        }
        req
    }

    /// Index a document
    async fn index_document(
        &self,
        index: &str,
        document_id: Option<&str>,
        body: &serde_json::Value,
    ) -> Result<serde_json::Value> {
        let url = if let Some(id) = document_id {
            format!(
                "{}/{}/_doc/{}",
                self.config.host.trim_end_matches('/'),
                index,
                id
            )
        } else {
            format!("{}/{}/_doc", self.config.host.trim_end_matches('/'), index)
        };

        let method = if document_id.is_some() { "PUT" } else { "POST" };

        let response = self
            .build_auth_request(match method {
                "PUT" => self.client.put(&url),
                _ => self.client.post(&url),
            })
            .json(body)
            .send()
            .await
            .with_context(|| format!("Failed to index document to Elasticsearch at {}", url))?;

        if !response.status().is_success() {
            let status = response.status();
            let error_body = response.text().await.unwrap_or_default();
            return Err(anyhow::anyhow!(
                "Elasticsearch index error ({}): {}",
                status,
                error_body
            ));
        }

        response
            .json()
            .await
            .context("Failed to parse Elasticsearch response")
    }

    /// Search documents
    async fn search(&self, index: &str, query: &serde_json::Value) -> Result<serde_json::Value> {
        let url = format!(
            "{}/{}/_search",
            self.config.host.trim_end_matches('/'),
            index
        );

        let response = self
            .build_auth_request(self.client.post(&url))
            .json(query)
            .send()
            .await
            .with_context(|| format!("Failed to search Elasticsearch at {}", url))?;

        if !response.status().is_success() {
            let status = response.status();
            let error_body = response.text().await.unwrap_or_default();
            return Err(anyhow::anyhow!(
                "Elasticsearch search error ({}): {}",
                status,
                error_body
            ));
        }

        response
            .json()
            .await
            .context("Failed to parse Elasticsearch search response")
    }

    /// Delete a document
    async fn delete_document(&self, index: &str, document_id: &str) -> Result<()> {
        let url = format!(
            "{}/{}/_doc/{}",
            self.config.host.trim_end_matches('/'),
            index,
            document_id
        );

        let response = self
            .build_auth_request(self.client.delete(&url))
            .send()
            .await
            .with_context(|| "Failed to delete document from Elasticsearch".to_string())?;

        if !response.status().is_success() {
            let status = response.status();
            let error_body = response.text().await.unwrap_or_default();
            return Err(anyhow::anyhow!(
                "Elasticsearch delete error ({}): {}",
                status,
                error_body
            ));
        }

        Ok(())
    }

    /// Check cluster health
    async fn cluster_health(&self) -> Result<serde_json::Value> {
        let url = format!("{}/_cluster/health", self.config.host.trim_end_matches('/'));

        let response = self
            .build_auth_request(self.client.get(&url))
            .send()
            .await
            .context("Failed to get Elasticsearch cluster health")?;

        if !response.status().is_success() {
            let status = response.status();
            return Err(anyhow::anyhow!(
                "Elasticsearch cluster health error: {}",
                status
            ));
        }

        response
            .json()
            .await
            .context("Failed to parse cluster health response")
    }
}

#[async_trait]
impl Reflex for ElasticsearchReflex {
    fn name(&self) -> &str {
        &self.name
    }

    fn r#type(&self) -> &str {
        "elasticsearch"
    }

    async fn handle(&self, ink: &Ink) -> Result<()> {
        info!(
            "Elasticsearch reflex '{}' handling ink on topic: {}",
            self.name, ink.topic
        );

        // Resolve index
        let index = self.resolve_index(ink);
        debug!(
            "Elasticsearch reflex '{}' resolved index: {}",
            self.name, index
        );

        // Resolve document ID
        let document_id = self.resolve_document_id(ink);

        // Render document body
        let body_rendered = self.render_template(&self.config.body_template, ink);
        let body: serde_json::Value = serde_json::from_str(&body_rendered)
            .context("body_template must render to valid JSON")?;

        debug!(
            "Elasticsearch reflex '{}' document body: {}",
            self.name, body
        );

        // Index the document
        let result = self
            .index_document(&index, document_id.as_deref(), &body)
            .await?;

        info!(
            "Elasticsearch reflex '{}' indexed document in {}: {}",
            self.name, index, result
        );

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_config() -> ElasticsearchReflexConfig {
        ElasticsearchReflexConfig {
            host: "http://localhost:9200".to_string(),
            index: "kraken-logs".to_string(),
            index_template: None,
            document_id: None,
            document_id_template: None,
            body_template: default_body_template(),
            topic_index_mapping: HashMap::new(),
            api_key: None,
            username: None,
            password: None,
            verify_ssl: true,
            timeout: 30,
            custom_headers: HashMap::new(),
        }
    }

    fn create_test_ink() -> Ink {
        Ink::new("/test/topic", "test", "test_source", "test body")
    }

    #[test]
    fn test_template_rendering() {
        let config = create_test_config();
        let reflex = ElasticsearchReflex::new("test".to_string(), config.clone());
        let ink = create_test_ink();
        let rendered = reflex.render_template(&config.body_template, &ink);
        let body: serde_json::Value = serde_json::from_str(&rendered).unwrap();
        assert_eq!(body["topic"], "/test/topic");
        assert_eq!(body["body"], "test body");
    }

    #[test]
    fn test_config_deserialization() {
        let yaml = r#"
            host: "http://es.example.com:9200"
            index: "alerts"
            username: "elastic"
            password: "secret"
        "#;
        let config: ElasticsearchReflexConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.host, "http://es.example.com:9200");
        assert_eq!(config.index, "alerts");
    }

    #[test]
    fn test_reflex_type() {
        let config = create_test_config();
        let reflex = ElasticsearchReflex::new("test".to_string(), config);
        assert_eq!(reflex.r#type(), "elasticsearch");
    }

    #[test]
    fn test_resolve_index_from_mapping() {
        let mut config = create_test_config();
        let mut mapping = HashMap::new();
        mapping.insert(
            "/alerts/critical".to_string(),
            "critical-alerts".to_string(),
        );
        config.topic_index_mapping = mapping;

        let reflex = ElasticsearchReflex::new("test".to_string(), config);
        let ink = Ink::new("/alerts/critical", "test", "test_source", "body");
        assert_eq!(reflex.resolve_index(&ink), "critical-alerts");
    }
}
