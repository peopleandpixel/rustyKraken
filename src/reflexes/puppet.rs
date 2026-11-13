use anyhow::{Context, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::{debug, error, info};

use super::Reflex;
use crate::model::Ink;

/// Configuration for PuppetDB Reflex
/// Interacts with PuppetDB REST API
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PuppetReflexConfig {
    /// PuppetDB server URL (e.g., "https://puppetdb.example.com:8081")
    pub host: String,

    /// API key for authentication (optional)
    #[serde(default)]
    pub api_key: Option<String>,

    /// API key ID (optional)
    #[serde(default)]
    pub api_key_id: Option<String>,

    /// Default action: "query", "catalog", "facts", "resources", "command"
    #[serde(default = "default_action")]
    pub action: String,

    /// Default PQL query
    #[serde(default)]
    pub query: Option<String>,

    /// Template for PQL query
    #[serde(default)]
    pub query_template: Option<String>,

    /// Optional: mapping from Ink topics to actions
    #[serde(default)]
    pub topic_action_mapping: HashMap<String, String>,

    /// Optional: mapping from Ink topics to PQL queries
    #[serde(default)]
    pub topic_query_mapping: HashMap<String, String>,

    /// Optional: mapping from Ink topics to node names
    #[serde(default)]
    pub topic_node_mapping: HashMap<String, String>,

    /// Default node name
    #[serde(default)]
    pub node: Option<String>,

    /// Template for node name
    #[serde(default)]
    pub node_template: Option<String>,

    /// API version
    #[serde(default = "default_api_version")]
    pub api_version: String,

    /// Whether to verify TLS certificates
    #[serde(default = "default_verify_ssl")]
    pub verify_ssl: bool,

    /// Timeout in seconds
    #[serde(default = "default_timeout")]
    pub timeout: u64,
}

fn default_action() -> String {
    "query".to_string()
}
fn default_api_version() -> String {
    "v4".to_string()
}
fn default_verify_ssl() -> bool {
    true
}
fn default_timeout() -> u64 {
    30
}

pub struct PuppetReflex {
    name: String,
    config: PuppetReflexConfig,
    client: reqwest::Client,
}

impl PuppetReflex {
    pub fn new(name: String, config: PuppetReflexConfig) -> Self {
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

    /// Resolve PQL query for a given Ink
    fn resolve_query(&self, ink: &Ink) -> Option<String> {
        if let Some(query) = self
            .config
            .topic_query_mapping
            .get(&ink.topic)
            .filter(|q| !q.is_empty())
        {
            return Some(self.render_template(query, ink));
        }
        if let Some(ref template) = self.config.query_template {
            return Some(self.render_template(template, ink));
        }
        self.config.query.clone()
    }

    /// Resolve node name for a given Ink
    fn resolve_node(&self, ink: &Ink) -> Option<String> {
        if let Some(node) = self
            .config
            .topic_node_mapping
            .get(&ink.topic)
            .filter(|n| !n.is_empty())
        {
            return Some(node.clone());
        }
        if let Some(ref template) = self.config.node_template {
            return Some(self.render_template(template, ink));
        }
        self.config.node.clone()
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

    /// Build PuppetDB API URL
    fn puppetdb_url(&self, path: &str) -> String {
        let host = self.config.host.trim_end_matches('/');
        format!("{}/{}", host, path)
    }

    /// Build auth headers
    fn build_auth_request(&self, request: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        let mut req = request;
        if let Some(ref key) = self.config.api_key {
            req = req.header("X-Authentication", key.clone());
        }
        if let Some(ref key_id) = self.config.api_key_id {
            req = req.header("X-Authentication-Id", key_id.clone());
        }
        req
    }

    /// Execute a PQL query
    async fn execute_query(&self, query: &str) -> Result<Vec<serde_json::Value>> {
        let url = self.puppetdb_url(&format!(
            "/pdb/{}/query/{}/query?query={}",
            self.config.api_version,
            self.config.api_version,
            urlencoding_pql(query)
        ));

        let response = self
            .build_auth_request(self.client.get(&url))
            .send()
            .await
            .with_context(|| format!("Failed to execute PuppetDB query: {}", query))?;

        if !response.status().is_success() {
            let status = response.status();
            let error_body = response.text().await.unwrap_or_default();
            return Err(anyhow::anyhow!(
                "PuppetDB query error ({}): {}",
                status,
                error_body
            ));
        }

        let results: Vec<serde_json::Value> = response
            .json()
            .await
            .context("Failed to parse PuppetDB query response")?;

        Ok(results)
    }

    /// Get facts for a node
    async fn get_facts(&self, node: &str) -> Result<serde_json::Value> {
        let url = self.puppetdb_url(&format!(
            "/pdb/{}/facts/{}/{}",
            self.config.api_version, node, self.config.api_version
        ));

        let response = self
            .build_auth_request(self.client.get(&url))
            .send()
            .await
            .with_context(|| format!("Failed to get facts for node {}", node))?;

        if !response.status().is_success() {
            let status = response.status();
            let error_body = response.text().await.unwrap_or_default();
            return Err(anyhow::anyhow!(
                "PuppetDB facts error ({}): {}",
                status,
                error_body
            ));
        }

        response
            .json()
            .await
            .context("Failed to parse PuppetDB facts response")
    }

    /// Get catalog for a node
    async fn get_catalog(&self, node: &str) -> Result<serde_json::Value> {
        let url = self.puppetdb_url(&format!(
            "/pdb/{}/catalogs/{}/{}",
            self.config.api_version, node, self.config.api_version
        ));

        let response = self
            .build_auth_request(self.client.get(&url))
            .send()
            .await
            .with_context(|| format!("Failed to get catalog for node {}", node))?;

        if !response.status().is_success() {
            let status = response.status();
            let error_body = response.text().await.unwrap_or_default();
            return Err(anyhow::anyhow!(
                "PuppetDB catalog error ({}): {}",
                status,
                error_body
            ));
        }

        response
            .json()
            .await
            .context("Failed to parse PuppetDB catalog response")
    }

    /// Get resources for a node
    async fn get_resources(
        &self,
        node: &str,
        r#type: Option<&str>,
    ) -> Result<Vec<serde_json::Value>> {
        let url = self.puppetdb_url(&format!(
            "/pdb/{}/resources/{}/{}",
            self.config.api_version,
            r#type.unwrap_or("*"),
            node
        ));

        let response = self
            .build_auth_request(self.client.get(&url))
            .send()
            .await
            .with_context(|| format!("Failed to get resources for node {}", node))?;

        if !response.status().is_success() {
            let status = response.status();
            let error_body = response.text().await.unwrap_or_default();
            return Err(anyhow::anyhow!(
                "PuppetDB resources error ({}): {}",
                status,
                error_body
            ));
        }

        let results: Vec<serde_json::Value> = response
            .json()
            .await
            .context("Failed to parse PuppetDB resources response")?;

        Ok(results)
    }

    /// Run a command via PuppetDB
    async fn run_command(&self, command: &str) -> Result<String> {
        let url = self.puppetdb_url(&format!("/pdb/{}/commands", self.config.api_version));

        let payload = serde_json::json!({
            "cmd": command
        });

        let response = self
            .build_auth_request(self.client.post(&url).json(&payload))
            .send()
            .await
            .with_context(|| format!("Failed to run command via PuppetDB: {}", command))?;

        if !response.status().is_success() {
            let status = response.status();
            let error_body = response.text().await.unwrap_or_default();
            return Err(anyhow::anyhow!(
                "PuppetDB command error ({}): {}",
                status,
                error_body
            ));
        }

        let result: serde_json::Value = response
            .json()
            .await
            .context("Failed to parse PuppetDB command response")?;

        result["command-id"]
            .as_str()
            .map(|s| s.to_string())
            .context("Command ID not found in response")
    }

    /// Get server info
    async fn server_info(&self) -> Result<serde_json::Value> {
        let url = self.puppetdb_url(&format!("/pdb/{}/version", self.config.api_version));

        let response = self
            .build_auth_request(self.client.get(&url))
            .send()
            .await
            .context("Failed to get PuppetDB server info")?;

        if !response.status().is_success() {
            let status = response.status();
            return Err(anyhow::anyhow!("PuppetDB server info error: {}", status));
        }

        response
            .text()
            .await
            .context("Failed to read PuppetDB version")
            .map(serde_json::Value::String)
    }
}

/// URL-encode a PQL query for PuppetDB
fn urlencoding_pql(query: &str) -> String {
    query
        .replace("(", "%28")
        .replace(")", "%29")
        .replace("?", "%3F")
        .replace("=", "%3D")
        .replace(",", "%2C")
        .replace(" ", "%20")
}

#[async_trait]
impl Reflex for PuppetReflex {
    fn name(&self) -> &str {
        &self.name
    }

    fn r#type(&self) -> &str {
        "puppet"
    }

    async fn handle(&self, ink: &Ink) -> Result<()> {
        info!(
            "Puppet reflex '{}' handling ink on topic: {}",
            self.name, ink.topic
        );

        let action = self.resolve_action(ink);
        debug!("Puppet reflex '{}' resolved action: {}", self.name, action);

        match action.as_str() {
            "query" => {
                let query = self.resolve_query(ink).context(
                    "PQL query is required - set query, query_template, or topic_query_mapping",
                )?;
                let results = self.execute_query(&query).await?;
                info!(
                    "Puppet reflex '{}' query returned {} results",
                    self.name,
                    results.len()
                );
                debug!(
                    "Puppet reflex '{}' query results: {}",
                    self.name,
                    serde_json::to_string(&results).unwrap_or_default()
                );
            }
            "facts" => {
                let node = self.resolve_node(ink).context(
                    "Node name is required for facts action - set node, node_template, or topic_node_mapping",
                )?;
                let facts = self.get_facts(&node).await?;
                debug!(
                    "Puppet reflex '{}' facts for {}: {}",
                    self.name, node, facts
                );
            }
            "catalog" => {
                let node = self
                    .resolve_node(ink)
                    .context("Node name is required for catalog action")?;
                let catalog = self.get_catalog(&node).await?;
                debug!(
                    "Puppet reflex '{}' catalog for {}: {}",
                    self.name, node, catalog
                );
            }
            "resources" => {
                let node = self
                    .resolve_node(ink)
                    .context("Node name is required for resources action")?;
                let resources = self.get_resources(&node, None).await?;
                info!(
                    "Puppet reflex '{}' found {} resources for {}",
                    self.name,
                    resources.len(),
                    node
                );
            }
            "command" => {
                let cmd = self.resolve_query(ink).context(
                    "Command is required for command action - use query or query_template",
                )?;
                let command_id = self.run_command(&cmd).await?;
                info!(
                    "Puppet reflex '{}' submitted command: {}",
                    self.name, command_id
                );
            }
            "info" | "server-info" => {
                let info = self.server_info().await?;
                debug!("Puppet reflex '{}' server info: {}", self.name, info);
            }
            _ => {
                error!("Puppet reflex '{}' unknown action: {}", self.name, action);
                return Err(anyhow::anyhow!("Unknown Puppet action: {}", action));
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_config() -> PuppetReflexConfig {
        PuppetReflexConfig {
            host: "https://puppetdb.example.com:8081".to_string(),
            api_key: None,
            api_key_id: None,
            action: "query".to_string(),
            query: Some("nodes {}".to_string()),
            query_template: None,
            topic_action_mapping: HashMap::new(),
            topic_query_mapping: HashMap::new(),
            topic_node_mapping: HashMap::new(),
            node: None,
            node_template: None,
            api_version: "v4".to_string(),
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
        let reflex = PuppetReflex::new("test".to_string(), config);
        let ink = create_test_ink();
        assert_eq!(
            reflex.render_template("nodes { certname = '{{topic}}' }", &ink),
            "nodes { certname = '/test/topic' }"
        );
    }

    #[test]
    fn test_config_deserialization() {
        let yaml = r#"
            host: "https://puppetdb.example.com:8081"
            action: "facts"
            node: "web01.example.com"
        "#;
        let config: PuppetReflexConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.host, "https://puppetdb.example.com:8081");
        assert_eq!(config.action, "facts");
    }

    #[test]
    fn test_reflex_type() {
        let config = create_test_config();
        let reflex = PuppetReflex::new("test".to_string(), config);
        assert_eq!(reflex.r#type(), "puppet");
    }

    #[test]
    fn test_pql_encoding() {
        let encoded = urlencoding_pql("nodes[certname = 'web01']");
        assert!(encoded.contains("%3D"));
        assert!(encoded.contains("%20"));
    }
}
