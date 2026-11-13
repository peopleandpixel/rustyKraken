use anyhow::{Context, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::{debug, info};

use super::Reflex;
use crate::model::Ink;

/// Configuration for PowerDNS Reflex
/// Manages DNS records via PowerDNS API
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PowerDnsReflexConfig {
    /// PowerDNS API URL (e.g., "http://localhost:8081")
    pub api_url: String,

    /// API key for authentication
    pub api_key: String,

    /// Default action: create, update, delete, query
    #[serde(default = "default_action")]
    pub action: String,

    /// Default zone name
    #[serde(default)]
    pub zone: Option<String>,

    /// Default record name
    #[serde(default)]
    pub record_name: Option<String>,

    /// Default record type: A, AAAA, CNAME, TXT, MX, SRV
    #[serde(default = "default_record_type")]
    pub record_type: String,

    /// Default record content
    #[serde(default)]
    pub record_content: Option<String>,

    /// Default TTL
    #[serde(default = "default_ttl")]
    pub ttl: u32,

    /// Template for record name
    #[serde(default)]
    pub record_name_template: Option<String>,

    /// Template for record content
    #[serde(default)]
    pub record_content_template: Option<String>,

    /// Optional: mapping from Ink topics to zones
    #[serde(default)]
    pub topic_zone_mapping: HashMap<String, String>,

    /// Optional: mapping from Ink topics to record types
    #[serde(default)]
    pub topic_type_mapping: HashMap<String, String>,

    /// Optional: mapping from Ink topics to actions
    #[serde(default)]
    pub topic_action_mapping: HashMap<String, String>,

    /// Verify SSL certificates
    #[serde(default = "default_verify_ssl")]
    pub verify_ssl: bool,
}

fn default_action() -> String { "create".to_string() }
fn default_record_type() -> String { "A".to_string() }
fn default_ttl() -> u32 { 3600 }
fn default_verify_ssl() -> bool { true }

pub struct PowerDnsReflex {
    name: String,
    config: PowerDnsReflexConfig,
    client: reqwest::Client,
}

impl PowerDnsReflex {
    pub fn new(name: String, config: PowerDnsReflexConfig) -> Self {
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

    fn resolve_zone(&self, ink: &Ink) -> Result<String> {
        if let Some(zone) = self
            .config
            .topic_zone_mapping
            .get(&ink.topic)
            .filter(|z| !z.is_empty())
        {
            return Ok(zone.clone());
        }
        self.config
            .zone
            .clone()
            .context("PowerDNS zone is required - set zone or topic_zone_mapping")
    }

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

    fn resolve_record_type(&self, ink: &Ink) -> String {
        if let Some(rtype) = self
            .config
            .topic_type_mapping
            .get(&ink.topic)
            .filter(|t| !t.is_empty())
        {
            return rtype.clone();
        }
        self.config.record_type.clone()
    }
}

#[async_trait]
impl Reflex for PowerDnsReflex {
    fn name(&self) -> &str { &self.name }
    fn r#type(&self) -> &str { "powerdns" }

    async fn handle(&self, ink: &Ink) -> Result<()> {
        info!("PowerDNS reflex '{}' handling ink on topic: {}", self.name, ink.topic);

        let zone = self.resolve_zone(ink)?;
        let action = self.resolve_action(ink);
        let record_type = self.resolve_record_type(ink);

        let record_name = if let Some(ref template) = self.config.record_name_template {
            self.render_template(template, ink)
        } else {
            self.config
                .record_name
                .clone()
                .context("PowerDNS record_name is required - set record_name or record_name_template")?
        };

        let record_content = if let Some(ref template) = self.config.record_content_template {
            self.render_template(template, ink)
        } else {
            self.config
                .record_content
                .clone()
                .context("PowerDNS record_content is required - set record_content or record_content_template")?
        };

        debug!(
            "PowerDNS reflex '{}' action={} zone={} name={} type={} content={} ttl={}",
            self.name, action, zone, record_name, record_type, record_content, self.config.ttl
        );

        let api_url = self.config.api_url.trim_end_matches('/');
        let zone_url = format!("{}/api/v1/servers/localhost/zones/{}", api_url, urlencoding::encode(&zone));

        match action.as_str() {
            "create" | "update" => {
                // Get current zone
                let response = self
                    .client
                    .get(&zone_url)
                    .header("X-API-Key", &self.config.api_key)
                    .send()
                    .await
                    .with_context(|| format!("Failed to get PowerDNS zone {}", zone))?;

                if !response.status().is_success() {
                    let status = response.status();
                    return Err(anyhow::anyhow!("PowerDNS API error: {}", status));
                }

                let zone_data: serde_json::Value = response
                    .json()
                    .await
                    .context("Failed to parse PowerDNS zone response")?;

                // Build record
                let record = serde_json::json!({
                    "name": record_name,
                    "type": record_type,
                    "content": record_content,
                    "ttl": self.config.ttl
                });

                // Get existing records and add/update
                let mut rrset: serde_json::Value = zone_data["rrsets"].as_array().map(|a| serde_json::Value::Array(a.clone())).unwrap_or_else(|| serde_json::Value::Array(vec![]));

                if let Some(arr) = rrset.as_array_mut() {
                    // Check if record exists
                    let mut found = false;
                    for item in arr.iter_mut() {
                        if item["name"] == record_name && item["type"] == record_type {
                            if let Some(records) = item["records"].as_array_mut() {
                                // Remove old records with same content
                                records.retain(|r| r["content"] != record_content);
                                records.push(record.clone());
                            }
                            found = true;
                            break;
                        }
                    }
                    if !found {
                        arr.push(serde_json::json!({
                            "name": record_name,
                            "type": record_type,
                            "changetype": "replace",
                            "records": [record]
                        }));
                    }
                }

                // Patch zone
                let patch_response = self
                    .client
                    .patch(&zone_url)
                    .header("X-API-Key", &self.config.api_key)
                    .json(&serde_json::json!({
                        "rrsets": rrset
                    }))
                    .send()
                    .await
                    .with_context(|| format!("Failed to update PowerDNS zone {}", zone))?;

                if !patch_response.status().is_success() {
                    let status = patch_response.status();
                    let error_body = patch_response.text().await.unwrap_or_default();
                    return Err(anyhow::anyhow!("PowerDNS update error ({}): {}", status, error_body));
                }

                info!(
                    "PowerDNS reflex '{}' created/updated record {} {} in zone {}",
                    self.name, record_name, record_type, zone
                );
            }
            "delete" => {
                // Get zone and remove record
                let response = self
                    .client
                    .get(&zone_url)
                    .header("X-API-Key", &self.config.api_key)
                    .send()
                    .await?;

                if !response.status().is_success() {
                    return Err(anyhow::anyhow!("PowerDNS API error: {}", response.status()));
                }

                let zone_data: serde_json::Value = response.json().await?;
                let mut rrset: serde_json::Value = zone_data["rrsets"].as_array().map(|a| serde_json::Value::Array(a.clone())).unwrap_or_else(|| serde_json::Value::Array(vec![]));

                if let Some(arr) = rrset.as_array_mut() {
                    for item in arr.iter_mut() {
                        if item["name"] == record_name && item["type"] == record_type
                            && let Some(records) = item["records"].as_array_mut() {
                                records.retain(|r| r["content"] != record_content);
                                if records.is_empty() {
                                    item["changetype"] = serde_json::json!("delete");
                                } else {
                                    item["changetype"] = serde_json::json!("replace");
                                }
                            }
                    }
                }

                let patch_response = self
                    .client
                    .patch(&zone_url)
                    .header("X-API-Key", &self.config.api_key)
                    .json(&serde_json::json!({ "rrsets": rrset }))
                    .send()
                    .await?;

                if !patch_response.status().is_success() {
                    return Err(anyhow::anyhow!("PowerDNS delete error: {}", patch_response.status()));
                }

                info!(
                    "PowerDNS reflex '{}' deleted record {} {} from zone {}",
                    self.name, record_name, record_type, zone
                );
            }
            "query" => {
                let response = self
                    .client
                    .get(&zone_url)
                    .header("X-API-Key", &self.config.api_key)
                    .send()
                    .await?;

                if !response.status().is_success() {
                    return Err(anyhow::anyhow!("PowerDNS query error: {}", response.status()));
                }

                let zone_data: serde_json::Value = response.json().await?;
                if let Some(rrsets) = zone_data["rrsets"].as_array() {
                    for rrset in rrsets {
                        if rrset["name"] == record_name && rrset["type"] == record_type
                            && let Some(records) = rrset["records"].as_array() {
                                info!(
                                    "PowerDNS reflex '{}' found {} records for {} {} in zone {}",
                                    self.name, records.len(), record_name, record_type, zone
                                );
                            }
                    }
                }
            }
            _ => {
                return Err(anyhow::anyhow!("Unknown PowerDNS action: {}", action));
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_config() -> PowerDnsReflexConfig {
        PowerDnsReflexConfig {
            api_url: "http://localhost:8081".to_string(),
            api_key: "test-api-key".to_string(),
            action: "create".to_string(),
            zone: Some("example.com".to_string()),
            record_name: Some("test".to_string()),
            record_type: "A".to_string(),
            record_content: Some("127.0.0.1".to_string()),
            ttl: 3600,
            record_name_template: None,
            record_content_template: None,
            topic_zone_mapping: HashMap::new(),
            topic_type_mapping: HashMap::new(),
            topic_action_mapping: HashMap::new(),
            verify_ssl: true,
        }
    }

    fn create_test_ink() -> Ink {
        Ink::new("/test/topic", "test", "test_source", "test body")
    }

    #[test]
    fn test_template_rendering() {
        let config = create_test_config();
        let reflex = PowerDnsReflex::new("test".to_string(), config);
        let ink = create_test_ink();
        assert_eq!(reflex.render_template("{{topic}}-{{body}}", &ink), "/test/topic-test body");
    }

    #[test]
    fn test_resolve_zone() {
        let config = create_test_config();
        let reflex = PowerDnsReflex::new("test".to_string(), config);
        let ink = create_test_ink();
        assert_eq!(reflex.resolve_zone(&ink).unwrap(), "example.com");
    }

    #[test]
    fn test_resolve_action() {
        let config = create_test_config();
        let reflex = PowerDnsReflex::new("test".to_string(), config);
        let ink = create_test_ink();
        assert_eq!(reflex.resolve_action(&ink), "create");
    }

    #[test]
    fn test_config_deserialization() {
        let yaml = r#"
            api_url: "http://pdns:8081"
            api_key: "secret"
            zone: "example.com"
            record_type: "CNAME"
        "#;
        let config: PowerDnsReflexConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.api_url, "http://pdns:8081");
    }

    #[test]
    fn test_reflex_type() {
        let config = create_test_config();
        let reflex = PowerDnsReflex::new("test".to_string(), config);
        assert_eq!(reflex.r#type(), "powerdns");
    }
}
