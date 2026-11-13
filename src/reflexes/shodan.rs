use anyhow::{Context, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::{debug, error, info};

use super::Reflex;
use crate::model::Ink;

/// Configuration for Shodan Reflex
/// Queries Shodan API for internet-exposed services
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShodanReflexConfig {
    /// Shodan API key
    pub api_key: String,

    /// Default action: "host", "search", "ports", "cpe"
    #[serde(default = "default_action")]
    pub action: String,

    /// Default query
    #[serde(default)]
    pub query: Option<String>,

    /// Template for query
    #[serde(default)]
    pub query_template: Option<String>,

    /// Optional: mapping from Ink topics to actions
    #[serde(default)]
    pub topic_action_mapping: HashMap<String, String>,

    /// Optional: mapping from Ink topics to queries
    #[serde(default)]
    pub topic_query_mapping: HashMap<String, String>,

    /// Timeout in seconds
    #[serde(default = "default_timeout")]
    pub timeout: u64,
}

fn default_action() -> String {
    "host".to_string()
}
fn default_timeout() -> u64 {
    30
}

pub struct ShodanReflex {
    name: String,
    config: ShodanReflexConfig,
    client: reqwest::Client,
}

impl ShodanReflex {
    pub fn new(name: String, config: ShodanReflexConfig) -> Self {
        Self {
            name,
            config,
            client: reqwest::Client::new(),
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

    /// Resolve query for a given Ink
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

    /// Build Shodan API URL
    fn shodan_url(&self, path: &str) -> String {
        format!("https://api.shodan.io/{}", path)
    }

    /// Query a host by IP
    async fn query_host(&self, ip: &str) -> Result<serde_json::Value> {
        let url = self.shodan_url(&format!("shodan/host/{}?key={}", ip, self.config.api_key));

        let response = self
            .client
            .get(&url)
            .send()
            .await
            .with_context(|| format!("Failed to query Shodan for host {}", ip))?;

        if !response.status().is_success() {
            let status = response.status();
            let error_body = response.text().await.unwrap_or_default();
            return Err(anyhow::anyhow!(
                "Shodan host error ({}): {}",
                status,
                error_body
            ));
        }

        response
            .json()
            .await
            .context("Failed to parse Shodan host response")
    }

    /// Search for services
    async fn search(&self, query: &str) -> Result<serde_json::Value> {
        let url = self.shodan_url(&format!(
            "shodan/search?key={}&query={}",
            self.config.api_key,
            urlencoding::encode(query)
        ));

        let response = self
            .client
            .get(&url)
            .send()
            .await
            .with_context(|| format!("Failed to search Shodan for query: {}", query))?;

        if !response.status().is_success() {
            let status = response.status();
            let error_body = response.text().await.unwrap_or_default();
            return Err(anyhow::anyhow!(
                "Shodan search error ({}): {}",
                status,
                error_body
            ));
        }

        response
            .json()
            .await
            .context("Failed to parse Shodan search response")
    }

    /// Get list of ports
    async fn get_ports(&self) -> Result<serde_json::Value> {
        let url = self.shodan_url(&format!("shodan/ports?key={}", self.config.api_key));

        let response = self
            .client
            .get(&url)
            .send()
            .await
            .context("Failed to get Shodan ports")?;

        if !response.status().is_success() {
            let status = response.status();
            return Err(anyhow::anyhow!("Shodan ports error: {}", status));
        }

        response
            .json()
            .await
            .context("Failed to parse Shodan ports response")
    }

    /// Get CPE list
    async fn get_cpe(&self) -> Result<serde_json::Value> {
        let url = self.shodan_url(&format!("shodan/cpe?key={}", self.config.api_key));

        let response = self
            .client
            .get(&url)
            .send()
            .await
            .context("Failed to get Shodan CPE")?;

        if !response.status().is_success() {
            let status = response.status();
            return Err(anyhow::anyhow!("Shodan CPE error: {}", status));
        }

        response
            .json()
            .await
            .context("Failed to parse Shodan CPE response")
    }

    /// Get API plan info
    async fn plan_info(&self) -> Result<serde_json::Value> {
        let url = self.shodan_url(&format!("api-info?key={}", self.config.api_key));

        let response = self
            .client
            .get(&url)
            .send()
            .await
            .context("Failed to get Shodan API info")?;

        if !response.status().is_success() {
            let status = response.status();
            return Err(anyhow::anyhow!("Shodan API info error: {}", status));
        }

        response
            .json()
            .await
            .context("Failed to parse Shodan API info response")
    }
}

#[async_trait]
impl Reflex for ShodanReflex {
    fn name(&self) -> &str {
        &self.name
    }

    fn r#type(&self) -> &str {
        "shodan"
    }

    async fn handle(&self, ink: &Ink) -> Result<()> {
        info!(
            "Shodan reflex '{}' handling ink on topic: {}",
            self.name, ink.topic
        );

        let action = self.resolve_action(ink);
        debug!("Shodan reflex '{}' resolved action: {}", self.name, action);

        match action.as_str() {
            "host" | "query-host" => {
                let ip = ink.body.trim().to_string();
                let result = self.query_host(&ip).await?;
                let hostname = result["hostname"].as_str().unwrap_or("unknown");
                let ports = result["ports"].as_array().map(|p| p.len()).unwrap_or(0);
                info!(
                    "Shodan reflex '{}' host {} has {} ports, hostname: {}",
                    self.name, ip, ports, hostname
                );
            }
            "search" | "query" => {
                let query = self.resolve_query(ink).unwrap_or_else(|| ink.body.clone());
                let result = self.search(&query).await?;
                let total = result["total"].as_u64().unwrap_or(0);
                info!(
                    "Shodan reflex '{}' search for '{}' found {} results",
                    self.name, query, total
                );
            }
            "ports" => {
                let ports = self.get_ports().await?;
                debug!("Shodan reflex '{}' ports: {}", self.name, ports);
            }
            "cpe" => {
                let cpe = self.get_cpe().await?;
                debug!("Shodan reflex '{}' CPE: {}", self.name, cpe);
            }
            "plan" | "api-info" => {
                let plan = self.plan_info().await?;
                let queries_left = plan["queries_left"].as_u64().unwrap_or(0);
                info!(
                    "Shodan reflex '{}' plan: {} queries left",
                    self.name, queries_left
                );
            }
            _ => {
                error!("Shodan reflex '{}' unknown action: {}", self.name, action);
                return Err(anyhow::anyhow!("Unknown Shodan action: {}", action));
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_config() -> ShodanReflexConfig {
        ShodanReflexConfig {
            api_key: "my-api-key".to_string(),
            action: "host".to_string(),
            query: None,
            query_template: None,
            topic_action_mapping: HashMap::new(),
            topic_query_mapping: HashMap::new(),
            timeout: 30,
        }
    }

    fn create_test_ink() -> Ink {
        Ink::new("/test/topic", "test", "test_source", "1.2.3.4")
    }

    #[test]
    fn test_template_rendering() {
        let config = create_test_config();
        let reflex = ShodanReflex::new("test".to_string(), config);
        let ink = create_test_ink();
        assert_eq!(
            reflex.render_template("port:80 {{body}}", &ink),
            "port:80 1.2.3.4"
        );
    }

    #[test]
    fn test_config_deserialization() {
        let yaml = r#"
            api_key: "secret"
            action: "search"
            query: "port:80"
        "#;
        let config: ShodanReflexConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.action, "search");
        assert_eq!(config.query, Some("port:80".to_string()));
    }

    #[test]
    fn test_reflex_type() {
        let config = create_test_config();
        let reflex = ShodanReflex::new("test".to_string(), config);
        assert_eq!(reflex.r#type(), "shodan");
    }

    #[test]
    fn test_resolve_action_from_mapping() {
        let mut config = create_test_config();
        let mut mapping = HashMap::new();
        mapping.insert("/shodan/search".to_string(), "search".to_string());
        config.topic_action_mapping = mapping;

        let reflex = ShodanReflex::new("test".to_string(), config);
        let ink = Ink::new("/shodan/search", "test", "test_source", "body");
        assert_eq!(reflex.resolve_action(&ink), "search");
    }
}
