#![allow(dead_code)]

use anyhow::{Context, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::{debug, info};

use super::Reflex;
use crate::appcontext::AppContext;
use crate::model::Ink;

/// Configuration for SaltStack Reflex
/// Interacts with Salt API (SaltAPI/CherryPy)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SaltStackReflexConfig {
    /// Salt API server URL (e.g., "https://salt.example.com:8000")
    pub host: String,

    /// Salt username
    pub username: String,

    /// Salt password
    pub password: String,

    /// Authentication backend: "pam", "ldap", "default"
    #[serde(default = "default_eauth")]
    pub eauth: String,

    /// Default Salt function to execute
    #[serde(default = "default_function")]
    pub function: String,

    /// Default target (minion glob)
    #[serde(default = "default_target")]
    pub target: String,

    /// Template for argument
    #[serde(default = "default_arg_template")]
    pub arg_template: String,

    /// Optional: mapping from Ink topics to Salt functions
    #[serde(default)]
    pub topic_function_mapping: HashMap<String, String>,

    /// Optional: mapping from Ink topics to targets
    #[serde(default)]
    pub topic_target_mapping: HashMap<String, String>,

    /// Optional: mapping from Ink topics to arguments
    #[serde(default)]
    pub topic_arg_mapping: HashMap<String, String>,

    /// Whether to verify TLS certificates
    #[serde(default = "default_verify_ssl")]
    pub verify_ssl: bool,

    /// Timeout in seconds
    #[serde(default = "default_timeout")]
    pub timeout: u64,
}

fn default_eauth() -> String {
    "pam".to_string()
}
fn default_function() -> String {
    "test.ping".to_string()
}
fn default_target() -> String {
    "*".to_string()
}
fn default_arg_template() -> String {
    "{{body}}".to_string()
}
fn default_verify_ssl() -> bool {
    true
}
fn default_timeout() -> u64 {
    30
}

pub struct SaltStackReflex {
    name: String,
    config: SaltStackReflexConfig,
    client: reqwest::Client,
    token: Option<String>,
}

impl SaltStackReflex {
    pub fn new(name: String, config: SaltStackReflexConfig, app_ctx: &AppContext) -> Self {
        let client = if config.verify_ssl {
            app_ctx.http_client().as_ref().clone()
        } else {
            app_ctx.http_client_danger_accept_invalid_certs()
        };
        Self {
            name,
            config,
            client,
            token: None,
        }
    }

    /// Resolve Salt function for a given Ink
    fn resolve_function(&self, ink: &Ink) -> String {
        if let Some(function) = self
            .config
            .topic_function_mapping
            .get(&ink.topic)
            .filter(|f| !f.is_empty())
        {
            return function.clone();
        }
        self.config.function.clone()
    }

    /// Resolve target for a given Ink
    fn resolve_target(&self, ink: &Ink) -> String {
        if let Some(target) = self
            .config
            .topic_target_mapping
            .get(&ink.topic)
            .filter(|t| !t.is_empty())
        {
            return target.clone();
        }
        self.config.target.clone()
    }

    /// Resolve argument for a given Ink
    fn resolve_arg(&self, ink: &Ink) -> String {
        if let Some(arg) = self
            .config
            .topic_arg_mapping
            .get(&ink.topic)
            .filter(|a| !a.is_empty())
        {
            return self.render_template(arg, ink);
        }
        self.render_template(&self.config.arg_template, ink)
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

    /// Build Salt API URL
    fn salt_api_url(&self, path: &str) -> String {
        let host = self.config.host.trim_end_matches('/');
        format!("{}/{}", host, path)
    }

    /// Authenticate and get token
    async fn authenticate(&self) -> Result<String> {
        let url = self.salt_api_url("/login");
        let payload = serde_json::json!({
            "username": self.config.username,
            "password": self.config.password,
            "eauth": self.config.eauth
        });

        let response = self
            .client
            .post(&url)
            .json(&payload)
            .send()
            .await
            .context("Failed to authenticate with Salt API")?;

        if !response.status().is_success() {
            let status = response.status();
            let error_body = response.text().await.unwrap_or_default();
            return Err(anyhow::anyhow!(
                "Salt API authentication error ({}): {}",
                status,
                error_body
            ));
        }

        let token_response: serde_json::Value = response
            .json()
            .await
            .context("Failed to parse Salt API token response")?;

        token_response["return"][0]["token"]
            .as_str()
            .map(|s| s.to_string())
            .context("Salt API token not found in response")
    }

    /// Execute a Salt function
    async fn execute_function(
        &self,
        function: &str,
        target: &str,
        arg: &str,
        token: &str,
    ) -> Result<serde_json::Value> {
        let url = self.salt_api_url("/");
        let payload = serde_json::json!({
            "client": "local",
            "fun": function,
            "tgt": target,
            "arg": [arg]
        });

        let response = self
            .client
            .post(&url)
            .header("X-Auth-Token", token)
            .header("Content-Type", "application/json")
            .json(&payload)
            .send()
            .await
            .with_context(|| format!("Failed to execute Salt function: {}", function))?;

        if !response.status().is_success() {
            let status = response.status();
            let error_body = response.text().await.unwrap_or_default();
            return Err(anyhow::anyhow!(
                "Salt API execution error ({}): {}",
                status,
                error_body
            ));
        }

        let result: serde_json::Value = response
            .json()
            .await
            .context("Failed to parse Salt API response")?;

        Ok(result)
    }

    /// Run a Salt state
    async fn run_state(&self, state: &str, target: &str, token: &str) -> Result<serde_json::Value> {
        let url = self.salt_api_url("/");
        let payload = serde_json::json!({
            "client": "local",
            "fun": "state.sls",
            "tgt": target,
            "arg": [state]
        });

        let response = self
            .client
            .post(&url)
            .header("X-Auth-Token", token)
            .header("Content-Type", "application/json")
            .json(&payload)
            .send()
            .await
            .with_context(|| format!("Failed to run Salt state: {}", state))?;

        if !response.status().is_success() {
            let status = response.status();
            let error_body = response.text().await.unwrap_or_default();
            return Err(anyhow::anyhow!(
                "Salt API state error ({}): {}",
                status,
                error_body
            ));
        }

        response
            .json()
            .await
            .context("Failed to parse Salt state response")
    }

    /// Run a Salt runner
    async fn run_runner(
        &self,
        runner: &str,
        args: Vec<String>,
        token: &str,
    ) -> Result<serde_json::Value> {
        let url = self.salt_api_url("/");
        let payload = serde_json::json!({
            "client": "runner",
            "fun": runner,
            "arg": args
        });

        let response = self
            .client
            .post(&url)
            .header("X-Auth-Token", token)
            .header("Content-Type", "application/json")
            .json(&payload)
            .send()
            .await
            .with_context(|| format!("Failed to run Salt runner: {}", runner))?;

        if !response.status().is_success() {
            let status = response.status();
            let error_body = response.text().await.unwrap_or_default();
            return Err(anyhow::anyhow!(
                "Salt API runner error ({}): {}",
                status,
                error_body
            ));
        }

        response
            .json()
            .await
            .context("Failed to parse Salt runner response")
    }

    /// List available minions
    async fn list_minions(&self, token: &str) -> Result<Vec<String>> {
        let url = self.salt_api_url("/");
        let payload = serde_json::json!({
            "client": "local",
            "fun": "sys.list_minions"
        });

        let response = self
            .client
            .post(&url)
            .header("X-Auth-Token", token)
            .header("Content-Type", "application/json")
            .json(&payload)
            .send()
            .await
            .context("Failed to list Salt minions")?;

        if !response.status().is_success() {
            let status = response.status();
            return Err(anyhow::anyhow!("Salt API list minions error: {}", status));
        }

        let result: serde_json::Value = response
            .json()
            .await
            .context("Failed to parse minions response")?;

        Ok(result["return"][0]
            .as_array()
            .map(|arr| {
                arr.iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .collect()
            })
            .unwrap_or_default())
    }
}

#[async_trait]
impl Reflex for SaltStackReflex {
    fn name(&self) -> &str {
        &self.name
    }

    fn r#type(&self) -> &str {
        "saltstack"
    }

    async fn handle(&self, ink: &Ink) -> Result<()> {
        info!(
            "SaltStack reflex '{}' handling ink on topic: {}",
            self.name, ink.topic
        );

        let function = self.resolve_function(ink);
        let target = self.resolve_target(ink);
        let arg = self.resolve_arg(ink);

        debug!(
            "SaltStack reflex '{}' function={}, target={}, arg={}",
            self.name, function, target, arg
        );

        // Authenticate
        let token = self.authenticate().await?;
        debug!("SaltStack reflex '{}' authenticated", self.name);

        // Execute the function
        let result = self
            .execute_function(&function, &target, &arg, &token)
            .await?;

        info!(
            "SaltStack reflex '{}' executed {} on {}: {}",
            self.name, function, target, result
        );

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_config() -> SaltStackReflexConfig {
        SaltStackReflexConfig {
            host: "https://salt.example.com:8000".to_string(),
            username: "saltapi".to_string(),
            password: "secret".to_string(),
            eauth: "pam".to_string(),
            function: "test.ping".to_string(),
            target: "*".to_string(),
            arg_template: "{{body}}".to_string(),
            topic_function_mapping: HashMap::new(),
            topic_target_mapping: HashMap::new(),
            topic_arg_mapping: HashMap::new(),
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
        let reflex = SaltStackReflex::new("test".to_string(), config, &app_ctx);
        let ink = create_test_ink();
        assert_eq!(
            reflex.render_template("{{topic}}-{{body}}", &ink),
            "/test/topic-test body"
        );
    }

    #[test]
    fn test_config_deserialization() {
        let yaml = r#"
            host: "https://salt.example.com:8000"
            username: "saltapi"
            password: "secret"
            function: "cmd.run"
            target: "web*"
        "#;
        let config: SaltStackReflexConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.function, "cmd.run");
        assert_eq!(config.target, "web*");
    }

    #[test]
    fn test_reflex_type() {
        let config = create_test_config();
        let app_ctx = AppContext::new();
        let reflex = SaltStackReflex::new("test".to_string(), config, &app_ctx);
        assert_eq!(reflex.r#type(), "saltstack");
    }

    #[test]
    fn test_resolve_function_from_mapping() {
        let mut config = create_test_config();
        let mut mapping = HashMap::new();
        mapping.insert("/salt/restart".to_string(), "service.restart".to_string());
        config.topic_function_mapping = mapping;

        let app_ctx = AppContext::new();
        let reflex = SaltStackReflex::new("test".to_string(), config, &app_ctx);
        let ink = Ink::new("/salt/restart", "test", "test_source", "nginx");
        assert_eq!(reflex.resolve_function(&ink), "service.restart");
    }
}
