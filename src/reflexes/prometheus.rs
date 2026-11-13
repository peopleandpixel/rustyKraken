#![allow(dead_code)]

use anyhow::{Context, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::{debug, info};

use super::Reflex;
use crate::model::Ink;

/// Configuration for Prometheus Pushgateway Reflex
/// Pushes metrics to Prometheus via Pushgateway
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrometheusReflexConfig {
    /// Pushgateway URL (e.g., "http://localhost:9091")
    pub host: String,

    /// Job name for the metric
    #[serde(default = "default_job")]
    pub job: String,

    /// Template for job name
    #[serde(default)]
    pub job_template: Option<String>,

    /// Default metric name
    #[serde(default = "default_metric")]
    pub metric: String,

    /// Template for metric name
    #[serde(default)]
    pub metric_template: Option<String>,

    /// Default metric value
    #[serde(default = "default_value")]
    pub value: f64,

    /// Template for metric value
    #[serde(default)]
    pub value_template: Option<String>,

    /// Template for labels (JSON object)
    #[serde(default)]
    pub labels_template: Option<String>,

    /// Optional: mapping from Ink topics to metric names
    #[serde(default)]
    pub topic_metric_mapping: HashMap<String, String>,

    /// Metric type: "gauge", "counter", "histogram", "summary"
    #[serde(default = "default_type")]
    pub metric_type: String,

    /// Whether to verify TLS certificates
    #[serde(default = "default_verify_ssl")]
    pub verify_ssl: bool,

    /// Timeout in seconds
    #[serde(default = "default_timeout")]
    pub timeout: u64,
}

fn default_job() -> String {
    "kraken".to_string()
}
fn default_metric() -> String {
    "kraken_event_total".to_string()
}
fn default_value() -> f64 {
    1.0
}
fn default_type() -> String {
    "counter".to_string()
}
fn default_verify_ssl() -> bool {
    true
}
fn default_timeout() -> u64 {
    30
}

pub struct PrometheusReflex {
    name: String,
    config: PrometheusReflexConfig,
    client: reqwest::Client,
}

impl PrometheusReflex {
    pub fn new(name: String, config: PrometheusReflexConfig) -> Self {
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

    /// Resolve job name for a given Ink
    fn resolve_job(&self, ink: &Ink) -> String {
        if let Some(ref template) = self.config.job_template {
            return self.render_template(template, ink);
        }
        self.config.job.clone()
    }

    /// Resolve metric name for a given Ink
    fn resolve_metric(&self, ink: &Ink) -> String {
        if let Some(metric) = self
            .config
            .topic_metric_mapping
            .get(&ink.topic)
            .filter(|m| !m.is_empty())
        {
            return metric.clone();
        }
        if let Some(ref template) = self.config.metric_template {
            return self.render_template(template, ink);
        }
        self.config.metric.clone()
    }

    /// Resolve metric value for a given Ink
    fn resolve_value(&self, ink: &Ink) -> f64 {
        if let Some(ref template) = self.config.value_template {
            let rendered = self.render_template(template, ink);
            return rendered.parse::<f64>().unwrap_or(self.config.value);
        }
        self.config.value
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

    /// Build Prometheus text format
    fn build_prometheus_format(
        &self,
        metric: &str,
        value: f64,
        labels: &serde_json::Value,
        metric_type: &str,
    ) -> String {
        let mut output = String::new();

        // Add HELP and TYPE comments
        output.push_str(&format!("# HELP {} Kraken metric\n", metric));
        output.push_str(&format!("# TYPE {} {}\n", metric, metric_type));

        // Build label string
        let mut label_parts = Vec::new();
        if let Some(label_obj) = labels.as_object() {
            for (key, value) in label_obj {
                let value_str = match value {
                    serde_json::Value::String(s) => s.clone(),
                    _ => value.to_string(),
                };
                // Sanitize label name: replace non-alphanumeric with underscore
                let sanitized_key: String = key
                    .chars()
                    .map(|c| {
                        if c.is_alphanumeric() || c == '_' {
                            c
                        } else {
                            '_'
                        }
                    })
                    .collect();
                label_parts.push(format!("{}=\"{}\"", sanitized_key, value_str));
            }
        }

        let label_str = if label_parts.is_empty() {
            String::new()
        } else {
            format!("{{{}}}", label_parts.join(","))
        };

        output.push_str(&format!("{}{} {}\n", metric, label_str, value));
        output
    }

    /// Push metrics to Pushgateway
    async fn push_metrics(&self, job: &str, prometheus_format: &str) -> Result<()> {
        let url = format!(
            "{}/metrics/job/{}",
            self.config.host.trim_end_matches('/'),
            job
        );

        let response = self
            .client
            .put(&url)
            .header("Content-Type", "text/plain")
            .body(prometheus_format.to_string())
            .send()
            .await
            .with_context(|| format!("Failed to push metrics to Pushgateway at {}", url))?;

        if !response.status().is_success() {
            let status = response.status();
            let error_body = response.text().await.unwrap_or_default();
            return Err(anyhow::anyhow!(
                "Prometheus Pushgateway error ({}): {}",
                status,
                error_body
            ));
        }

        Ok(())
    }

    /// Delete metrics from Pushgateway
    async fn delete_metrics(&self, job: &str) -> Result<()> {
        let url = format!(
            "{}/metrics/job/{}",
            self.config.host.trim_end_matches('/'),
            job
        );

        let response = self
            .client
            .delete(&url)
            .send()
            .await
            .with_context(|| "Failed to delete metrics from Pushgateway".to_string())?;

        if !response.status().is_success() {
            let status = response.status();
            let error_body = response.text().await.unwrap_or_default();
            return Err(anyhow::anyhow!(
                "Prometheus Pushgateway delete error ({}): {}",
                status,
                error_body
            ));
        }

        Ok(())
    }
}

#[async_trait]
impl Reflex for PrometheusReflex {
    fn name(&self) -> &str {
        &self.name
    }

    fn r#type(&self) -> &str {
        "prometheus"
    }

    async fn handle(&self, ink: &Ink) -> Result<()> {
        info!(
            "Prometheus reflex '{}' handling ink on topic: {}",
            self.name, ink.topic
        );

        // Resolve job
        let job = self.resolve_job(ink);
        debug!("Prometheus reflex '{}' resolved job: {}", self.name, job);

        // Resolve metric
        let metric = self.resolve_metric(ink);
        debug!(
            "Prometheus reflex '{}' resolved metric: {}",
            self.name, metric
        );

        // Resolve value
        let value = self.resolve_value(ink);
        debug!(
            "Prometheus reflex '{}' resolved value: {}",
            self.name, value
        );

        // Render labels
        let labels: serde_json::Value = if let Some(ref template) = self.config.labels_template {
            let rendered = self.render_template(template, ink);
            serde_json::from_str(&rendered).context("labels_template must render to valid JSON")?
        } else {
            serde_json::json!({
                "topic": ink.topic,
                "source": ink.source
            })
        };

        // Build Prometheus format
        let prometheus_format =
            self.build_prometheus_format(&metric, value, &labels, &self.config.metric_type);

        debug!(
            "Prometheus reflex '{}' format:\n{}",
            self.name, prometheus_format
        );

        // Push metrics
        self.push_metrics(&job, &prometheus_format).await?;

        info!(
            "Prometheus reflex '{}' pushed metric {}={}",
            self.name, metric, value
        );

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_config() -> PrometheusReflexConfig {
        PrometheusReflexConfig {
            host: "http://localhost:9091".to_string(),
            job: "kraken".to_string(),
            job_template: None,
            metric: "kraken_event_total".to_string(),
            metric_template: None,
            value: 1.0,
            value_template: None,
            labels_template: None,
            topic_metric_mapping: HashMap::new(),
            metric_type: "counter".to_string(),
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
        let reflex = PrometheusReflex::new("test".to_string(), config);
        let ink = create_test_ink();
        assert_eq!(
            reflex.render_template("{{topic}}-{{body}}", &ink),
            "/test/topic-test body"
        );
    }

    #[test]
    fn test_config_deserialization() {
        let yaml = r#"
            host: "http://pushgateway:9091"
            job: "my_job"
            metric: "my_metric"
            metric_type: "gauge"
        "#;
        let config: PrometheusReflexConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.job, "my_job");
        assert_eq!(config.metric_type, "gauge");
    }

    #[test]
    fn test_reflex_type() {
        let config = create_test_config();
        let reflex = PrometheusReflex::new("test".to_string(), config);
        assert_eq!(reflex.r#type(), "prometheus");
    }

    #[test]
    fn test_prometheus_format() {
        let config = create_test_config();
        let reflex = PrometheusReflex::new("test".to_string(), config);
        let labels = serde_json::json!({"host": "web01", "env": "prod"});
        let format = reflex.build_prometheus_format("cpu_usage", 42.5, &labels, "gauge");
        assert!(format.contains("# HELP cpu_usage"));
        assert!(format.contains("# TYPE cpu_usage gauge"));
        assert!(format.contains("cpu_usage"));
        assert!(format.contains("42.5"));
    }
}
