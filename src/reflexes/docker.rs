#![allow(dead_code)]

use anyhow::{Context, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, error, info};

use super::Reflex;
use crate::appcontext::AppContext;
use crate::model::Ink;

/// Configuration for Docker Reflex
/// Controls Docker containers via the Docker Engine API
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DockerReflexConfig {
    /// Docker socket path or TCP host (e.g., "unix:///var/run/docker.sock" or "tcp://localhost:2375")
    #[serde(default = "default_host")]
    pub host: String,

    /// Default action: "start", "stop", "restart", "rm", "create", "exec", "logs", "inspect"
    #[serde(default = "default_action")]
    pub action: String,

    /// Default container name or ID
    #[serde(default)]
    pub container: Option<String>,

    /// Template for container name
    #[serde(default)]
    pub container_template: Option<String>,

    /// Optional: mapping from Ink topics to actions
    #[serde(default)]
    pub topic_action_mapping: HashMap<String, String>,

    /// Optional: mapping from Ink topics to container names
    #[serde(default)]
    pub topic_container_mapping: HashMap<String, String>,

    /// Image name for create/start actions
    #[serde(default)]
    pub image: Option<String>,

    /// Command to run (for exec or create)
    #[serde(default)]
    pub command: Option<String>,

    /// Environment variables (JSON object template)
    #[serde(default)]
    pub env_template: Option<String>,

    /// Network name
    #[serde(default)]
    pub network: Option<String>,

    /// Restart policy: "no", "always", "on-failure", "unless-stopped"
    #[serde(default)]
    pub restart_policy: Option<String>,

    /// Remove container after stop
    #[serde(default)]
    pub remove_on_stop: bool,

    /// TLS verification for TCP connections
    #[serde(default = "default_tls_verify")]
    pub tls_verify: bool,

    /// Timeout in seconds for operations
    #[serde(default = "default_timeout")]
    pub timeout: u64,
}

fn default_host() -> String {
    "unix:///var/run/docker.sock".to_string()
}
fn default_action() -> String {
    "restart".to_string()
}
fn default_tls_verify() -> bool {
    true
}
fn default_timeout() -> u64 {
    30
}

pub struct DockerReflex {
    name: String,
    config: DockerReflexConfig,
    client: Arc<reqwest::Client>,
}

impl DockerReflex {
    pub fn new(name: String, config: DockerReflexConfig, app_ctx: &AppContext) -> Self {
        Self {
            name,
            config,
            client: Arc::clone(app_ctx.http_client()),
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

    /// Resolve container name for a given Ink
    fn resolve_container(&self, ink: &Ink) -> Result<String> {
        if let Some(container) = self
            .config
            .topic_container_mapping
            .get(&ink.topic)
            .filter(|c| !c.is_empty())
        {
            return Ok(container.clone());
        }
        if let Some(ref template) = self.config.container_template {
            return Ok(self.render_template(template, ink));
        }
        self.config
            .container
            .clone()
            .context("Container name is required - set container, container_template, or topic_container_mapping")
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

    /// Build Docker API URL
    fn docker_url(&self, path: &str) -> String {
        let host = self.config.host.trim_end_matches('/');
        // Docker Engine API version
        format!("{}/v1.41/{}", host, path.trim_start_matches('/'))
    }

    /// Execute a Docker API request via HTTP (for TCP hosts)
    async fn http_request(
        &self,
        method: &str,
        url: &str,
        body: Option<&serde_json::Value>,
    ) -> Result<serde_json::Value> {
        let mut request = match method {
            "GET" => self.client.get(url),
            "POST" => self.client.post(url),
            "PUT" => self.client.put(url),
            "DELETE" => self.client.delete(url),
            _ => return Err(anyhow::anyhow!("Unsupported HTTP method: {}", method)),
        };

        if let Some(b) = body {
            request = request.json(b);
        }

        let response = request
            .send()
            .await
            .with_context(|| format!("Failed to send Docker API request to {}", url))?;

        if !response.status().is_success() {
            let status = response.status();
            let error_body = response.text().await.unwrap_or_default();
            return Err(anyhow::anyhow!(
                "Docker API error ({}): {}",
                status,
                error_body
            ));
        }

        // Try to parse JSON, fall back to empty object
        let text = response.text().await?;
        if text.trim().is_empty() {
            return Ok(serde_json::json!({}));
        }
        serde_json::from_str(&text).context("Failed to parse Docker API response")
    }

    /// Execute a Docker API request via Unix socket using tokio::net::UnixStream
    async fn socket_request(
        &self,
        method: &str,
        path: &str,
        body: Option<&serde_json::Value>,
    ) -> Result<serde_json::Value> {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let socket_path = self
            .config
            .host
            .strip_prefix("unix://")
            .context("Invalid Unix socket path")?;

        let mut stream = tokio::net::UnixStream::connect(socket_path)
            .await
            .context("Failed to connect to Docker socket")?;

        let body_str = body
            .map(|b| serde_json::to_string(b).unwrap_or_default())
            .unwrap_or_default();
        let content_length = body_str.len();

        let request = format!(
            "{} {} HTTP/1.1\r\nHost: localhost\r\nContent-Length: {}\r\nContent-Type: application/json\r\n\r\n{}",
            method, path, content_length, body_str
        );

        stream
            .write_all(request.as_bytes())
            .await
            .context("Failed to write to Docker socket")?;

        let mut response = Vec::new();
        stream
            .read_to_end(&mut response)
            .await
            .context("Failed to read from Docker socket")?;

        let response_str = String::from_utf8_lossy(&response);
        let parts: Vec<&str> = response_str.splitn(2, "\r\n\r\n").collect();
        if parts.is_empty() {
            return Err(anyhow::anyhow!("Empty Docker response"));
        }

        let headers = parts[0];
        let body = if parts.len() > 1 { parts[1] } else { "" };

        // Check status line
        let status_line = headers.lines().next().unwrap_or("");
        if !status_line.contains("200")
            && !status_line.contains("201")
            && !status_line.contains("204")
        {
            return Err(anyhow::anyhow!(
                "Docker API error: {} — {}",
                status_line,
                body
            ));
        }

        if body.trim().is_empty() {
            return Ok(serde_json::json!({}));
        }
        serde_json::from_str(body).context("Failed to parse Docker API response")
    }

    /// Start a container
    async fn start_container(&self, container: &str) -> Result<()> {
        info!(
            "Docker reflex '{}' starting container: {}",
            self.name, container
        );
        let path = format!("/containers/{}/start", container);
        if self.config.host.starts_with("unix://") {
            self.socket_request("POST", &path, None).await?;
        } else {
            self.http_request("POST", &self.docker_url(&path), None)
                .await?;
        }
        info!(
            "Docker reflex '{}' started container: {}",
            self.name, container
        );
        Ok(())
    }

    /// Stop a container
    async fn stop_container(&self, container: &str) -> Result<()> {
        info!(
            "Docker reflex '{}' stopping container: {}",
            self.name, container
        );
        let timeout = self.config.timeout;
        let path = format!("/containers/{}/stop?t={}", container, timeout);
        if self.config.host.starts_with("unix://") {
            self.socket_request("POST", &path, None).await?;
        } else {
            self.http_request("POST", &self.docker_url(&path), None)
                .await?;
        }
        info!(
            "Docker reflex '{}' stopped container: {}",
            self.name, container
        );
        Ok(())
    }

    /// Restart a container
    async fn restart_container(&self, container: &str) -> Result<()> {
        info!(
            "Docker reflex '{}' restarting container: {}",
            self.name, container
        );
        let timeout = self.config.timeout;
        let path = format!("/containers/{}/restart?t={}", container, timeout);
        if self.config.host.starts_with("unix://") {
            self.socket_request("POST", &path, None).await?;
        } else {
            self.http_request("POST", &self.docker_url(&path), None)
                .await?;
        }
        info!(
            "Docker reflex '{}' restarted container: {}",
            self.name, container
        );
        Ok(())
    }

    /// Remove a container
    async fn remove_container(&self, container: &str) -> Result<()> {
        info!(
            "Docker reflex '{}' removing container: {}",
            self.name, container
        );
        let path = format!("/containers/{}?force=true", container);
        if self.config.host.starts_with("unix://") {
            self.socket_request("DELETE", &path, None).await?;
        } else {
            self.http_request("DELETE", &self.docker_url(&path), None)
                .await?;
        }
        info!(
            "Docker reflex '{}' removed container: {}",
            self.name, container
        );
        Ok(())
    }

    /// Execute a command in a container
    async fn exec_in_container(&self, container: &str, cmd: &str) -> Result<serde_json::Value> {
        info!(
            "Docker reflex '{}' executing in container {}: {}",
            self.name, container, cmd
        );

        // Create exec instance
        let exec_payload = serde_json::json!({
            "AttachStdout": true,
            "AttachStderr": true,
            "Cmd": if cmd.starts_with('[') {
                serde_json::from_str(cmd).unwrap_or_default()
            } else {
                serde_json::json!(["sh", "-c", cmd])
            },
            "Tty": false
        });

        let create_path = format!("/containers/{}/exec", container);
        let exec_result = if self.config.host.starts_with("unix://") {
            self.socket_request("POST", &create_path, Some(&exec_payload))
                .await?
        } else {
            self.http_request("POST", &self.docker_url(&create_path), Some(&exec_payload))
                .await?
        };

        let exec_id = exec_result["Id"]
            .as_str()
            .context("Failed to get exec ID from Docker")?;

        // Start exec
        let start_path = format!("/exec/{}/start", exec_id);
        let _ = if self.config.host.starts_with("unix://") {
            self.socket_request("POST", &start_path, Some(&serde_json::json!({})))
                .await?
        } else {
            self.http_request(
                "POST",
                &self.docker_url(&start_path),
                Some(&serde_json::json!({})),
            )
            .await?
        };

        info!(
            "Docker reflex '{}' executed command in container: {}",
            self.name, container
        );
        Ok(exec_result)
    }

    /// Get container logs
    async fn get_container_logs(&self, container: &str) -> Result<String> {
        info!(
            "Docker reflex '{}' getting logs for container: {}",
            self.name, container
        );
        let path = format!("/containers/{}/logs?stdout=true&tail=100", container);

        // For logs, we need raw text response
        if self.config.host.starts_with("unix://") {
            // Socket-based log retrieval
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let socket_path = self
                .config
                .host
                .strip_prefix("unix://")
                .context("Invalid Unix socket path")?;

            let mut stream = tokio::net::UnixStream::connect(socket_path)
                .await
                .context("Failed to connect to Docker socket")?;

            let request = format!("GET {} HTTP/1.1\r\nHost: localhost\r\n\r\n", path);
            stream
                .write_all(request.as_bytes())
                .await
                .context("Failed to write to Docker socket")?;

            let mut response = Vec::new();
            stream
                .read_to_end(&mut response)
                .await
                .context("Failed to read from Docker socket")?;

            let response_str = String::from_utf8_lossy(&response);
            let parts: Vec<&str> = response_str.splitn(2, "\r\n\r\n").collect();
            let body = if parts.len() > 1 { parts[1] } else { "" };

            // Docker logs have 8-byte frame headers, strip them
            let clean: String = body
                .chars()
                .skip_while(|c| *c != '\r' && *c != '\n')
                .skip(1) // skip the \n after headers
                .collect();

            Ok(clean)
        } else {
            let url = self.docker_url(&path);
            let response = self
                .client
                .get(&url)
                .send()
                .await
                .with_context(|| format!("Failed to get logs from {}", container))?;

            if !response.status().is_success() {
                let status = response.status();
                return Err(anyhow::anyhow!("Docker API error: {}", status));
            }

            response.text().await.context("Failed to read logs")
        }
    }

    /// Inspect a container
    async fn inspect_container(&self, container: &str) -> Result<serde_json::Value> {
        info!(
            "Docker reflex '{}' inspecting container: {}",
            self.name, container
        );
        let path = format!("/containers/{}/json", container);
        if self.config.host.starts_with("unix://") {
            self.socket_request("GET", &path, None).await
        } else {
            self.http_request("GET", &self.docker_url(&path), None)
                .await
        }
    }

    /// List containers
    async fn list_containers(&self) -> Result<Vec<serde_json::Value>> {
        let path = "/containers/json?all=true";
        let result = if self.config.host.starts_with("unix://") {
            self.socket_request("GET", path, None).await?
        } else {
            self.http_request("GET", &self.docker_url(path), None)
                .await?
        };

        Ok(result.as_array().cloned().unwrap_or_default())
    }

    /// Get Docker info
    async fn docker_info(&self) -> Result<serde_json::Value> {
        let path = "/info";
        if self.config.host.starts_with("unix://") {
            self.socket_request("GET", path, None).await
        } else {
            self.http_request("GET", &self.docker_url(path), None).await
        }
    }
}

#[async_trait]
impl Reflex for DockerReflex {
    fn name(&self) -> &str {
        &self.name
    }

    fn r#type(&self) -> &str {
        "docker"
    }

    async fn handle(&self, ink: &Ink) -> Result<()> {
        info!(
            "Docker reflex '{}' handling ink on topic: {}",
            self.name, ink.topic
        );

        let action = self.resolve_action(ink);
        let container = self.resolve_container(ink)?;

        debug!(
            "Docker reflex '{}' action={}, container={}",
            self.name, action, container
        );

        match action.as_str() {
            "start" => self.start_container(&container).await?,
            "stop" => {
                self.stop_container(&container).await?;
                if self.config.remove_on_stop {
                    self.remove_container(&container).await?;
                }
            }
            "restart" => self.restart_container(&container).await?,
            "rm" | "remove" => self.remove_container(&container).await?,
            "exec" => {
                let cmd = self
                    .config
                    .command
                    .as_ref()
                    .map(|c| self.render_template(c, ink))
                    .unwrap_or_else(|| ink.body.clone());
                self.exec_in_container(&container, &cmd).await?;
            }
            "logs" => {
                let logs = self.get_container_logs(&container).await?;
                info!(
                    "Docker reflex '{}' logs for {}: {}",
                    self.name, container, logs
                );
            }
            "inspect" => {
                let info = self.inspect_container(&container).await?;
                debug!(
                    "Docker reflex '{}' inspect result for {}: {}",
                    self.name, container, info
                );
            }
            "create" => {
                let image = self.config.image.as_deref().unwrap_or("nginx:latest");
                let payload = serde_json::json!({
                    "Image": image,
                    "name": container,
                });
                if self.config.host.starts_with("unix://") {
                    self.socket_request("POST", "/containers/create", Some(&payload))
                        .await?;
                } else {
                    self.http_request(
                        "POST",
                        &self.docker_url("/containers/create"),
                        Some(&payload),
                    )
                    .await?;
                }
                info!(
                    "Docker reflex '{}' created container: {}",
                    self.name, container
                );
            }
            _ => {
                error!("Docker reflex '{}' unknown action: {}", self.name, action);
                return Err(anyhow::anyhow!("Unknown Docker action: {}", action));
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::appcontext::AppContext;

    fn create_test_config() -> DockerReflexConfig {
        DockerReflexConfig {
            host: "unix:///var/run/docker.sock".to_string(),
            action: "restart".to_string(),
            container: Some("my_container".to_string()),
            container_template: None,
            topic_action_mapping: HashMap::new(),
            topic_container_mapping: HashMap::new(),
            image: None,
            command: None,
            env_template: None,
            network: None,
            restart_policy: None,
            remove_on_stop: false,
            tls_verify: true,
            timeout: 30,
        }
    }

    fn create_test_ink() -> Ink {
        Ink::new("/test/topic", "test", "test_source", "test body")
    }

    #[test]
    fn test_template_rendering() {
        let config = create_test_config();
        let reflex = DockerReflex::new("test".to_string(), config, &AppContext::new());
        let ink = create_test_ink();
        assert_eq!(
            reflex.render_template("{{topic}}-{{body}}", &ink),
            "/test/topic-test body"
        );
    }

    #[test]
    fn test_config_deserialization() {
        let yaml = r#"
            host: "tcp://localhost:2375"
            action: "stop"
            container: "web_server"
            remove_on_stop: true
        "#;
        let config: DockerReflexConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.host, "tcp://localhost:2375");
        assert_eq!(config.action, "stop");
        assert!(config.remove_on_stop);
    }

    #[test]
    fn test_reflex_type() {
        let config = create_test_config();
        let reflex = DockerReflex::new("test".to_string(), config, &AppContext::new());
        assert_eq!(reflex.r#type(), "docker");
    }

    #[test]
    fn test_resolve_action_from_mapping() {
        let mut config = create_test_config();
        let mut mapping = HashMap::new();
        mapping.insert("/docker/stop".to_string(), "stop".to_string());
        config.topic_action_mapping = mapping;

        let reflex = DockerReflex::new("test".to_string(), config, &AppContext::new());
        let ink = Ink::new("/docker/stop", "test", "test_source", "body");
        assert_eq!(reflex.resolve_action(&ink), "stop");
    }

    #[test]
    fn test_resolve_container_from_mapping() {
        let mut config = create_test_config();
        config.container = None;
        let mut mapping = HashMap::new();
        mapping.insert("/docker/web".to_string(), "web_server".to_string());
        config.topic_container_mapping = mapping;

        let reflex = DockerReflex::new("test".to_string(), config, &AppContext::new());
        let ink = Ink::new("/docker/web", "test", "test_source", "body");
        assert_eq!(reflex.resolve_container(&ink).unwrap(), "web_server");
    }
}
