#![allow(dead_code)]

use anyhow::{Context, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::{debug, info};

use super::Reflex;
use crate::appcontext::AppContext;
use crate::model::Ink;

/// Configuration for InfluxDB Reflex
/// Writes metrics to InfluxDB via HTTP API
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct InfluxdbReflexConfig {
    /// InfluxDB host URL (e.g., "http://localhost:8086")
    pub host: String,

    /// InfluxDB organization
    pub organization: String,

    /// InfluxDB bucket
    pub bucket: String,

    /// InfluxDB token
    pub token: String,

    /// Default measurement name
    #[serde(default = "default_measurement")]
    pub measurement: String,

    /// Template for measurement name
    #[serde(default)]
    pub measurement_template: Option<String>,

    /// Template for tags (JSON object)
    #[serde(default)]
    pub tags_template: Option<String>,

    /// Template for fields (JSON object)
    #[serde(default = "default_fields_template")]
    pub fields_template: String,

    /// Optional: mapping from Ink topics to measurements
    #[serde(default)]
    pub topic_measurement_mapping: HashMap<String, String>,

    /// Precision for timestamps: "ns", "us", "ms", "s", "m", "h"
    #[serde(default = "default_precision")]
    pub precision: String,

    /// Whether to verify TLS certificates
    #[serde(default = "default_verify_ssl")]
    pub verify_ssl: bool,

    /// Timeout in seconds
    #[serde(default = "default_timeout")]
    pub timeout: u64,
}

fn default_measurement() -> String {
    "kraken_events".to_string()
}
fn default_fields_template() -> String {
    r#"{"value": 1}"#.to_string()
}
fn default_precision() -> String {
    "ns".to_string()
}
fn default_verify_ssl() -> bool {
    true
}
fn default_timeout() -> u64 {
    30
}

pub struct InfluxdbReflex {
    name: String,
    config: InfluxdbReflexConfig,
    client: reqwest::Client,
}

impl InfluxdbReflex {
    pub fn new(name: String, config: InfluxdbReflexConfig, app_ctx: &AppContext) -> Self {
        let client = if config.verify_ssl {
            app_ctx.http_client().as_ref().clone()
        } else {
            app_ctx.http_client_danger_accept_invalid_certs()
        };
        Self {
            name,
            config,
            client,
        }
    }

    /// Resolve measurement name for a given Ink
    fn resolve_measurement(&self, ink: &Ink) -> String {
        if let Some(measurement) = self
            .config
            .topic_measurement_mapping
            .get(&ink.topic)
            .filter(|m| !m.is_empty())
        {
            return measurement.clone();
        }
        if let Some(ref template) = self.config.measurement_template {
            return self.render_template(template, ink);
        }
        self.config.measurement.clone()
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

    /// Build Line Protocol from tags and fields
    fn build_line_protocol(
        &self,
        measurement: &str,
        tags: &serde_json::Value,
        fields: &serde_json::Value,
        timestamp: i64,
    ) -> String {
        let mut line = measurement.to_string();

        // Add tags
        if let Some(tag_obj) = tags.as_object() {
            for (key, value) in tag_obj {
                let value_str = match value {
                    serde_json::Value::String(s) => s.clone(),
                    _ => value.to_string(),
                };
                line.push_str(&format!(",{}={}", key, value_str));
            }
        }

        line.push(' ');

        // Add fields
        if let Some(field_obj) = fields.as_object() {
            let mut first = true;
            for (key, value) in field_obj {
                if !first {
                    line.push(',');
                }
                first = false;

                match value {
                    serde_json::Value::String(s) => {
                        let escaped = s.replace('\\', "\\\\").replace('"', "\\\"");
                        line.push_str(&format!("{}=\"{}\"", key, escaped));
                    }
                    serde_json::Value::Number(n) => {
                        if let Some(i) = n.as_i64() {
                            line.push_str(&format!("{}={}", key, i));
                        } else if let Some(f) = n.as_f64() {
                            line.push_str(&format!("{}={}", key, f));
                        }
                    }
                    serde_json::Value::Bool(b) => {
                        line.push_str(&format!("{}={}", key, b));
                    }
                    _ => {
                        line.push_str(&format!("{}=\"{}\"", key, value));
                    }
                }
            }
        }

        // Add timestamp
        line.push_str(&format!(" {}", timestamp));

        line
    }

    /// Write data to InfluxDB
    async fn write_data(&self, line_protocol: &str) -> Result<()> {
        let url = format!("{}/api/v2/write", self.config.host.trim_end_matches('/'));

        let query = format!(
            "?org={}&bucket={}&precision={}",
            self.config.organization, self.config.bucket, self.config.precision
        );

        let response = self
            .client
            .post(format!("{}{}", url, query))
            .header("Authorization", format!("Token {}", self.config.token))
            .header("Content-Type", "text/plain")
            .body(line_protocol.to_string())
            .send()
            .await
            .with_context(|| format!("Failed to write to InfluxDB at {}", url))?;

        if !response.status().is_success() {
            let status = response.status();
            let error_body = response.text().await.unwrap_or_default();
            return Err(anyhow::anyhow!(
                "InfluxDB write error ({}): {}",
                status,
                error_body
            ));
        }

        Ok(())
    }

    /// Query data from InfluxDB
    async fn query_data(&self, query: &str) -> Result<serde_json::Value> {
        let url = format!("{}/api/v2/query", self.config.host.trim_end_matches('/'));

        let payload = serde_json::json!({
            "query": query,
            "org": self.config.organization
        });

        let response = self
            .client
            .post(&url)
            .header("Authorization", format!("Token {}", self.config.token))
            .header("Content-Type", "application/json")
            .json(&payload)
            .send()
            .await
            .with_context(|| format!("Failed to query InfluxDB at {}", url))?;

        if !response.status().is_success() {
            let status = response.status();
            let error_body = response.text().await.unwrap_or_default();
            return Err(anyhow::anyhow!(
                "InfluxDB query error ({}): {}",
                status,
                error_body
            ));
        }

        response
            .json()
            .await
            .context("Failed to parse InfluxDB query response")
    }

    /// List buckets
    async fn list_buckets(&self) -> Result<Vec<String>> {
        let url = format!(
            "{}/api/v2/buckets?org={}",
            self.config.host.trim_end_matches('/'),
            self.config.organization
        );

        let response = self
            .client
            .get(&url)
            .header("Authorization", format!("Token {}", self.config.token))
            .send()
            .await
            .context("Failed to list InfluxDB buckets")?;

        if !response.status().is_success() {
            let status = response.status();
            return Err(anyhow::anyhow!("InfluxDB list buckets error: {}", status));
        }

        let result: serde_json::Value = response
            .json()
            .await
            .context("Failed to parse buckets response")?;

        Ok(result["buckets"]
            .as_array()
            .map(|arr| {
                arr.iter()
                    .filter_map(|b| b["name"].as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default())
    }
}

#[async_trait]
impl Reflex for InfluxdbReflex {
    fn name(&self) -> &str {
        &self.name
    }

    fn r#type(&self) -> &str {
        "influxdb"
    }

    async fn handle(&self, ink: &Ink) -> Result<()> {
        info!(
            "InfluxDB reflex '{}' handling ink on topic: {}",
            self.name, ink.topic
        );

        // Resolve measurement
        let measurement = self.resolve_measurement(ink);
        debug!(
            "InfluxDB reflex '{}' resolved measurement: {}",
            self.name, measurement
        );

        // Render tags
        let tags: serde_json::Value = if let Some(ref template) = self.config.tags_template {
            let rendered = self.render_template(template, ink);
            serde_json::from_str(&rendered).context("tags_template must render to valid JSON")?
        } else {
            serde_json::json!({
                "topic": ink.topic,
                "source": ink.source
            })
        };

        // Render fields
        let fields_rendered = self.render_template(&self.config.fields_template, ink);
        let fields: serde_json::Value = serde_json::from_str(&fields_rendered)
            .context("fields_template must render to valid JSON")?;

        debug!(
            "InfluxDB reflex '{}' tags={}, fields={}",
            self.name, tags, fields
        );

        // Build Line Protocol
        let timestamp = ink.timestamp.timestamp_nanos_opt().unwrap_or(0);
        let line_protocol = self.build_line_protocol(&measurement, &tags, &fields, timestamp);

        debug!(
            "InfluxDB reflex '{}' line protocol: {}",
            self.name, line_protocol
        );

        // Write data
        self.write_data(&line_protocol).await?;

        info!(
            "InfluxDB reflex '{}' wrote data to measurement: {}",
            self.name, measurement
        );

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_config() -> InfluxdbReflexConfig {
        InfluxdbReflexConfig {
            host: "http://localhost:8086".to_string(),
            organization: "kraken".to_string(),
            bucket: "events".to_string(),
            token: "my-token".to_string(),
            measurement: "kraken_events".to_string(),
            measurement_template: None,
            tags_template: None,
            fields_template: default_fields_template(),
            topic_measurement_mapping: HashMap::new(),
            precision: "ns".to_string(),
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
        let app_ctx = AppContext::new();
        let reflex = InfluxdbReflex::new("test".to_string(), config, &app_ctx);
        let ink = create_test_ink();
        assert_eq!(
            reflex.render_template("{{topic}}-{{body}}", &ink),
            "/test/topic-test body"
        );
    }

    #[test]
    fn test_config_deserialization() {
        let yaml = r#"
            host: "http://influx.example.com:8086"
            organization: "myorg"
            bucket: "metrics"
            token: "secret"
        "#;
        let config: InfluxdbReflexConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.organization, "myorg");
        assert_eq!(config.bucket, "metrics");
    }

    #[test]
    fn test_reflex_type() {
        let config = create_test_config();
        let app_ctx = AppContext::new();
        let reflex = InfluxdbReflex::new("test".to_string(), config, &app_ctx);
        assert_eq!(reflex.r#type(), "influxdb");
    }

    #[test]
    fn test_line_protocol() {
        let config = create_test_config();
        let app_ctx = AppContext::new();
        let reflex = InfluxdbReflex::new("test".to_string(), config, &app_ctx);
        let tags = serde_json::json!({"host": "web01"});
        let fields = serde_json::json!({"value": 42, "name": "test"});
        let line = reflex.build_line_protocol("cpu", &tags, &fields, 1234567890);
        assert!(line.contains("cpu,host=web01"));
        assert!(line.contains("value=42"));
        assert!(line.contains("name=\"test\""));
    }
}
