#![allow(dead_code)]

use anyhow::{Context, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::{debug, error, info};

use super::Reflex;
use crate::model::Ink;

/// Configuration for Ansible Reflex using AWX/Tower API
/// See: https://docs.ansible.com/ansible-tower/latest/html/developer/api.html
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AnsibleReflexConfig {
    /// AWX/Tower server URL (e.g., "https://awx.example.com")
    pub host: String,

    /// AWX/Tower username
    pub username: String,

    /// AWX/Tower password
    pub password: String,

    /// Default Job Template name to launch
    #[serde(default)]
    pub job_template: Option<String>,

    /// Default Job Template ID to launch
    #[serde(default)]
    pub job_template_id: Option<u64>,

    /// Template for extra variables (JSON string)
    #[serde(default)]
    pub extra_vars_template: Option<String>,

    /// Template for the message body
    #[serde(default = "default_message_template")]
    pub message_template: String,

    /// Optional: mapping from Ink topics to Job Template names
    #[serde(default)]
    pub topic_template_mapping: HashMap<String, String>,

    /// Optional: mapping from Ink topics to extra variables
    #[serde(default)]
    pub topic_extra_vars_mapping: HashMap<String, String>,

    /// Limit hosts to run on (e.g., "web01,web02")
    #[serde(default)]
    pub limit: Option<String>,

    /// Tags to run (e.g., "install,configure")
    #[serde(default)]
    pub tags: Option<String>,

    /// Skip tags
    #[serde(default)]
    pub skip_tags: Option<String>,

    /// Whether to wait for job completion
    #[serde(default)]
    pub wait_for_completion: bool,

    /// Poll interval in seconds for job status
    #[serde(default = "default_poll_interval")]
    pub poll_interval: u64,

    /// Max wait time in seconds for job completion
    #[serde(default = "default_max_wait")]
    pub max_wait: u64,

    /// Whether to verify TLS certificates
    #[serde(default = "default_verify_ssl")]
    pub verify_ssl: bool,

    /// Custom headers for API requests
    #[serde(default)]
    pub custom_headers: HashMap<String, String>,
}

fn default_message_template() -> String {
    "{{body}}".to_string()
}

fn default_poll_interval() -> u64 {
    5
}

fn default_max_wait() -> u64 {
    3600
}

fn default_verify_ssl() -> bool {
    true
}

pub struct AnsibleReflex {
    name: String,
    config: AnsibleReflexConfig,
    client: reqwest::Client,
}

impl AnsibleReflex {
    pub fn new(name: String, config: AnsibleReflexConfig) -> Self {
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

    /// Resolve Job Template for a given Ink
    fn resolve_job_template(&self, ink: &Ink) -> Option<String> {
        if let Some(template) = self
            .config
            .topic_template_mapping
            .get(&ink.topic)
            .filter(|t| !t.is_empty())
        {
            return Some(template.clone());
        }
        self.config.job_template.clone()
    }

    /// Resolve extra variables for a given Ink
    fn resolve_extra_vars(&self, ink: &Ink) -> serde_json::Value {
        if let Some(template) = self.config.topic_extra_vars_mapping.get(&ink.topic) {
            let rendered = self.render_template(template, ink);
            if let Ok(vars) = serde_json::from_str(&rendered) {
                return vars;
            }
        }
        if let Some(ref template) = self.config.extra_vars_template {
            let rendered = self.render_template(template, ink);
            if let Ok(vars) = serde_json::from_str(&rendered) {
                return vars;
            }
        }
        serde_json::json!({
            "ink_topic": ink.topic,
            "ink_body": ink.body,
            "ink_id": ink.id.to_string(),
            "ink_timestamp": ink.timestamp.to_rfc3339()
        })
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

    /// Build AWX API URL
    fn awx_api_url(&self, path: &str) -> String {
        let host = self.config.host.trim_end_matches('/');
        format!("{}/api/v2/{}", host, path.trim_start_matches('/'))
    }

    /// Authenticate and get OAuth2 token
    async fn authenticate(&self) -> Result<String> {
        let url = self.awx_api_url("token/");
        let payload = serde_json::json!({
            "grant_type": "password",
            "username": self.config.username,
            "password": self.config.password
        });

        let response = self
            .client
            .post(&url)
            .json(&payload)
            .send()
            .await
            .context("Failed to authenticate with AWX")?;

        if !response.status().is_success() {
            let status = response.status();
            let error_body = response.text().await.unwrap_or_default();
            return Err(anyhow::anyhow!(
                "AWX authentication error ({}): {}",
                status,
                error_body
            ));
        }

        let token_response: serde_json::Value = response
            .json()
            .await
            .context("Failed to parse AWX token response")?;

        token_response["access_token"]
            .as_str()
            .map(|s| s.to_string())
            .context("AWX token response missing access_token")
    }

    /// Launch a job template
    async fn launch_job_template(
        &self,
        job_template: &str,
        extra_vars: &serde_json::Value,
        token: &str,
    ) -> Result<u64> {
        let url = self.awx_api_url(&format!("job_templates/{}/launch/", job_template));

        let mut payload = serde_json::json!({
            "extra_vars": extra_vars
        });

        if let Some(ref limit) = self.config.limit {
            payload["limit"] = serde_json::json!(limit);
        }
        if let Some(ref tags) = self.config.tags {
            payload["tags"] = serde_json::json!(tags);
        }
        if let Some(ref skip_tags) = self.config.skip_tags {
            payload["skip_tags"] = serde_json::json!(skip_tags);
        }

        let response = self
            .client
            .post(&url)
            .header("Authorization", format!("Bearer {}", token))
            .json(&payload)
            .send()
            .await
            .with_context(|| format!("Failed to launch job template: {}", job_template))?;

        if !response.status().is_success() {
            let status = response.status();
            let error_body = response.text().await.unwrap_or_default();
            return Err(anyhow::anyhow!(
                "AWX launch job template error ({}): {}",
                status,
                error_body
            ));
        }

        // The response location header contains the job URL
        let location = response
            .headers()
            .get(reqwest::header::LOCATION)
            .and_then(|h| h.to_str().ok())
            .context("AWX response missing Location header")?;

        // Extract job ID from URL path
        let job_id = location
            .split('/')
            .rfind(|s| !s.is_empty())
            .and_then(|s| s.parse::<u64>().ok())
            .context("Failed to parse job ID from AWX response")?;

        Ok(job_id)
    }

    /// Get job status
    async fn get_job_status(&self, job_id: u64, token: &str) -> Result<String> {
        let url = self.awx_api_url(&format!("jobs/{}/", job_id));

        let response = self
            .client
            .get(&url)
            .header("Authorization", format!("Bearer {}", token))
            .send()
            .await
            .with_context(|| format!("Failed to get job status for job {}", job_id))?;

        if !response.status().is_success() {
            let status = response.status();
            let error_body = response.text().await.unwrap_or_default();
            return Err(anyhow::anyhow!(
                "AWX get job status error ({}): {}",
                status,
                error_body
            ));
        }

        let job_response: serde_json::Value = response
            .json()
            .await
            .context("Failed to parse AWX job status response")?;

        job_response["status"]
            .as_str()
            .map(|s| s.to_string())
            .context("Job status not found in response")
    }

    /// Wait for job to complete
    async fn wait_for_job(&self, job_id: u64, token: &str) -> Result<String> {
        use std::time::Duration;
        use tokio::time::{Instant, sleep};

        let start = Instant::now();
        let max_wait = Duration::from_secs(self.config.max_wait);
        let poll_interval = Duration::from_secs(self.config.poll_interval);

        loop {
            if start.elapsed() > max_wait {
                return Err(anyhow::anyhow!(
                    "Ansible reflex '{}' job {} timed out after {} seconds",
                    self.name,
                    job_id,
                    self.config.max_wait
                ));
            }

            let status = self.get_job_status(job_id, token).await?;
            debug!(
                "Ansible reflex '{}' job {} status: {}",
                self.name, job_id, status
            );

            match status.as_str() {
                "successful" | "error" | "failed" | "canceled" => return Ok(status),
                _ => sleep(poll_interval).await,
            }
        }
    }

    /// List available job templates
    async fn list_job_templates(&self, token: &str) -> Result<Vec<String>> {
        let url = self.awx_api_url("job_templates/");

        let response = self
            .client
            .get(&url)
            .header("Authorization", format!("Bearer {}", token))
            .send()
            .await
            .context("Failed to fetch AWX job templates")?;

        let templates_response: serde_json::Value = response
            .json()
            .await
            .context("Failed to parse AWX job templates response")?;

        let templates = templates_response["results"]
            .as_array()
            .map(|arr| {
                arr.iter()
                    .filter_map(|t| t["name"].as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default();

        Ok(templates)
    }

    /// Get job output/logs
    async fn get_job_output(&self, job_id: u64, token: &str) -> Result<String> {
        let url = self.awx_api_url(&format!("jobs/{}/job_events/", job_id));

        let response = self
            .client
            .get(&url)
            .header("Authorization", format!("Bearer {}", token))
            .send()
            .await
            .with_context(|| format!("Failed to get job output for job {}", job_id))?;

        if !response.status().is_success() {
            let status = response.status();
            let error_body = response.text().await.unwrap_or_default();
            return Err(anyhow::anyhow!(
                "AWX get job output error ({}): {}",
                status,
                error_body
            ));
        }

        let events_response: serde_json::Value = response
            .json()
            .await
            .context("Failed to parse AWX job events response")?;

        let output = events_response["results"]
            .as_array()
            .map(|arr| {
                arr.iter()
                    .filter_map(|e| e["event_data"]["stdout"].as_str().map(|s| s.to_string()))
                    .collect::<Vec<_>>()
                    .join("\n")
            })
            .unwrap_or_default();

        Ok(output)
    }
}

#[async_trait]
impl Reflex for AnsibleReflex {
    fn name(&self) -> &str {
        &self.name
    }

    fn r#type(&self) -> &str {
        "ansible"
    }

    async fn handle(&self, ink: &Ink) -> Result<()> {
        info!(
            "Ansible reflex '{}' handling ink on topic: {}",
            self.name, ink.topic
        );

        // Resolve the job template
        let job_template = self
            .resolve_job_template(ink)
            .context("Job template is required - set job_template or topic_template_mapping")?;
        debug!(
            "Ansible reflex '{}' resolved job template: {}",
            self.name, job_template
        );

        // Resolve extra variables
        let extra_vars = self.resolve_extra_vars(ink);
        debug!(
            "Ansible reflex '{}' resolved extra vars: {}",
            self.name, extra_vars
        );

        // Authenticate
        let token = self.authenticate().await?;
        debug!("Ansible reflex '{}' authenticated successfully", self.name);

        // Launch the job template
        let job_id = self
            .launch_job_template(&job_template, &extra_vars, &token)
            .await?;
        info!(
            "Ansible reflex '{}' launched job template '{}' (job ID: {})",
            self.name, job_template, job_id
        );

        // Optionally wait for completion
        if self.config.wait_for_completion {
            match self.wait_for_job(job_id, &token).await {
                Ok(status) => {
                    info!(
                        "Ansible reflex '{}' job {} completed with status: {}",
                        self.name, job_id, status
                    );
                }
                Err(e) => {
                    error!(
                        "Ansible reflex '{}' job {} failed: {}",
                        self.name, job_id, e
                    );
                    return Err(e);
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_config() -> AnsibleReflexConfig {
        AnsibleReflexConfig {
            host: "https://awx.example.com".to_string(),
            username: "admin".to_string(),
            password: "secret".to_string(),
            job_template: Some("deploy_app".to_string()),
            job_template_id: None,
            extra_vars_template: None,
            message_template: "{{body}}".to_string(),
            topic_template_mapping: HashMap::new(),
            topic_extra_vars_mapping: HashMap::new(),
            limit: None,
            tags: None,
            skip_tags: None,
            wait_for_completion: false,
            poll_interval: 5,
            max_wait: 3600,
            verify_ssl: true,
            custom_headers: HashMap::new(),
        }
    }

    fn create_test_ink() -> Ink {
        Ink::new("/test/topic", "test", "test_source", "test body")
    }

    #[test]
    fn test_template_rendering() {
        let config = create_test_config();
        let reflex = AnsibleReflex::new("test".to_string(), config);
        let ink = create_test_ink();
        assert_eq!(
            reflex.render_template("[{{topic}}] {{body}}", &ink),
            "[/test/topic] test body"
        );
    }

    #[test]
    fn test_config_deserialization() {
        let yaml = r#"
            host: "https://awx.example.com"
            username: "admin"
            password: "secret"
            job_template: "deploy_app"
            wait_for_completion: true
        "#;
        let config: AnsibleReflexConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.host, "https://awx.example.com");
        assert_eq!(config.job_template, Some("deploy_app".to_string()));
        assert!(config.wait_for_completion);
    }

    #[test]
    fn test_reflex_type() {
        let config = create_test_config();
        let reflex = AnsibleReflex::new("test".to_string(), config);
        assert_eq!(reflex.r#type(), "ansible");
    }

    #[test]
    fn test_resolve_job_template_from_mapping() {
        let mut config = create_test_config();
        let mut mapping = HashMap::new();
        mapping.insert("/deploy/web".to_string(), "deploy_web".to_string());
        config.topic_template_mapping = mapping;
        config.job_template = None;

        let reflex = AnsibleReflex::new("test".to_string(), config);
        let ink = Ink::new("/deploy/web", "test", "test_source", "body");
        assert_eq!(
            reflex.resolve_job_template(&ink),
            Some("deploy_web".to_string())
        );
    }

    #[test]
    fn test_resolve_extra_vars_from_template() {
        let mut config = create_test_config();
        config.extra_vars_template =
            Some(r#"{"env": "{{topic}}", "message": "{{body}}"}"#.to_string());

        let reflex = AnsibleReflex::new("test".to_string(), config);
        let ink = create_test_ink();
        let vars = reflex.resolve_extra_vars(&ink);
        assert_eq!(vars["env"], "/test/topic");
        assert_eq!(vars["message"], "test body");
    }
}
