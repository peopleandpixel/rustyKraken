#![allow(dead_code)]

use anyhow::{Context, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::{debug, error, info};

use super::Reflex;
use crate::appcontext::AppContext;
use crate::model::Ink;

/// Configuration for Kubernetes Reflex
/// Interacts with the Kubernetes API server
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KubernetesReflexConfig {
    /// Kubernetes API server URL
    pub api_url: String,

    /// Service account token or bearer token
    #[serde(default)]
    pub token: Option<String>,

    /// kubeconfig file path (alternative to token)
    #[serde(default)]
    pub kubeconfig: Option<String>,

    /// Default namespace
    #[serde(default = "default_namespace")]
    pub namespace: String,

    /// Default action: "exec", "scale", "restart", "logs", "apply", "delete", "get", "label"
    #[serde(default = "default_action")]
    pub action: String,

    /// Default resource type: "deployment", "pod", "service", "configmap", "secret", "statefulset"
    #[serde(default = "default_resource")]
    pub resource: String,

    /// Default resource name
    #[serde(default)]
    pub resource_name: Option<String>,

    /// Template for resource name
    #[serde(default)]
    pub resource_name_template: Option<String>,

    /// Optional: mapping from Ink topics to actions
    #[serde(default)]
    pub topic_action_mapping: HashMap<String, String>,

    /// Optional: mapping from Ink topics to resource names
    #[serde(default)]
    pub topic_resource_mapping: HashMap<String, String>,

    /// Replicas count for scale action
    #[serde(default)]
    pub replicas: Option<u32>,

    /// Command to execute (for exec action)
    #[serde(default)]
    pub command: Option<String>,

    /// YAML/JSON manifest for apply action
    #[serde(default)]
    pub manifest: Option<String>,

    /// Labels to add (JSON object template)
    #[serde(default)]
    pub labels_template: Option<String>,

    /// Whether to verify TLS certificates
    #[serde(default = "default_verify_ssl")]
    pub verify_ssl: bool,

    /// Timeout in seconds
    #[serde(default = "default_timeout")]
    pub timeout: u64,
}

fn default_namespace() -> String {
    "default".to_string()
}
fn default_action() -> String {
    "get".to_string()
}
fn default_resource() -> String {
    "deployment".to_string()
}
fn default_verify_ssl() -> bool {
    true
}
fn default_timeout() -> u64 {
    30
}

pub struct KubernetesReflex {
    name: String,
    config: KubernetesReflexConfig,
    client: reqwest::Client,
}

impl KubernetesReflex {
    pub fn new(name: String, config: KubernetesReflexConfig, app_ctx: &AppContext) -> Self {
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

    /// Resolve resource name for a given Ink
    fn resolve_resource_name(&self, ink: &Ink) -> Result<String> {
        if let Some(name) = self
            .config
            .topic_resource_mapping
            .get(&ink.topic)
            .filter(|n| !n.is_empty())
        {
            return Ok(name.clone());
        }
        if let Some(ref template) = self.config.resource_name_template {
            return Ok(self.render_template(template, ink));
        }
        self.config.resource_name.clone().context(
            "Resource name is required - set resource_name, resource_name_template, or topic_resource_mapping",
        )
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

    /// Build Kubernetes API URL
    fn k8s_url(&self, path: &str) -> String {
        let api = self.config.api_url.trim_end_matches('/');
        format!("{}{}", api, path)
    }

    /// Build auth request
    fn build_auth_request(&self, request: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        if let Some(ref token) = self.config.token {
            return request.bearer_auth(token);
        }
        request
    }

    /// Execute a Kubernetes API request
    async fn api_request(
        &self,
        method: &str,
        path: &str,
        body: Option<&serde_json::Value>,
    ) -> Result<serde_json::Value> {
        let url = self.k8s_url(path);
        let mut request = match method {
            "GET" => self.client.get(&url),
            "POST" => self.client.post(&url),
            "PUT" => self.client.put(&url),
            "PATCH" => self.client.patch(&url),
            "DELETE" => self.client.delete(&url),
            _ => return Err(anyhow::anyhow!("Unsupported HTTP method: {}", method)),
        };

        request = self.build_auth_request(request);

        if let Some(b) = body {
            request = request.json(b);
        }

        let response = request
            .send()
            .await
            .with_context(|| format!("Failed to send Kubernetes API request to {}", url))?;

        if !response.status().is_success() {
            let status = response.status();
            let error_body = response.text().await.unwrap_or_default();
            return Err(anyhow::anyhow!(
                "Kubernetes API error ({}): {}",
                status,
                error_body
            ));
        }

        let text = response.text().await?;
        if text.trim().is_empty() {
            return Ok(serde_json::json!({}));
        }
        serde_json::from_str(&text).context("Failed to parse Kubernetes API response")
    }

    /// Get a resource
    async fn get_resource(&self, _resource: &str, name: &str) -> Result<serde_json::Value> {
        let path = format!(
            "/apis/apps/v1/namespaces/{}/deployments/{}",
            self.config.namespace, name
        );
        self.api_request("GET", &path, None).await
    }

    /// Scale a deployment
    async fn scale_deployment(&self, name: &str, replicas: u32) -> Result<()> {
        info!(
            "Kubernetes reflex '{}' scaling deployment {} to {} replicas",
            self.name, name, replicas
        );

        let path = format!(
            "/apis/apps/v1/namespaces/{}/deployments/{}",
            self.config.namespace, name
        );

        // First get current state
        let _current = self.api_request("GET", &path, None).await?;

        let patch = serde_json::json!({
            "spec": { "replicas": replicas }
        });

        let response = self
            .client
            .patch(self.k8s_url(&path))
            .header("Content-Type", "application/merge-patch+json")
            .json(&patch)
            .send()
            .await
            .with_context(|| format!("Failed to scale deployment {}", name))?;

        if !response.status().is_success() {
            let status = response.status();
            let error_body = response.text().await.unwrap_or_default();
            return Err(anyhow::anyhow!(
                "Kubernetes scale error ({}): {}",
                status,
                error_body
            ));
        }

        info!(
            "Kubernetes reflex '{}' scaled deployment {} to {} replicas",
            self.name, name, replicas
        );
        Ok(())
    }

    /// Restart a deployment (rollout restart)
    async fn restart_deployment(&self, name: &str) -> Result<()> {
        info!(
            "Kubernetes reflex '{}' restarting deployment: {}",
            self.name, name
        );

        let path = format!(
            "/apis/apps/v1/namespaces/{}/deployments/{}/restart",
            self.config.namespace, name
        );

        self.api_request("POST", &path, None).await?;

        info!(
            "Kubernetes reflex '{}' restarted deployment: {}",
            self.name, name
        );
        Ok(())
    }

    /// Get pod logs
    async fn get_pod_logs(&self, pod_name: &str, container: Option<&str>) -> Result<String> {
        info!(
            "Kubernetes reflex '{}' getting logs for pod: {}",
            self.name, pod_name
        );

        let mut path = format!(
            "/api/v1/namespaces/{}/pods/{}/log",
            self.config.namespace, pod_name
        );
        if let Some(c) = container {
            path = format!("{}?container={}", path, c);
        }

        let response = self
            .build_auth_request(self.client.get(self.k8s_url(&path)))
            .send()
            .await
            .with_context(|| format!("Failed to get logs for pod {}", pod_name))?;

        if !response.status().is_success() {
            let status = response.status();
            return Err(anyhow::anyhow!("Kubernetes logs error: {}", status));
        }

        response.text().await.context("Failed to read pod logs")
    }

    /// Apply a YAML/JSON manifest
    async fn apply_manifest(&self, manifest: &str) -> Result<serde_json::Value> {
        info!("Kubernetes reflex '{}' applying manifest", self.name);

        // Parse the manifest to determine the resource type
        let parsed: serde_json::Value =
            serde_json::from_str(manifest).context("Manifest must be valid JSON")?;

        let _api_version = parsed["apiVersion"].as_str().unwrap_or("v1");
        let kind = parsed["kind"].as_str().unwrap_or("");
        let namespace = parsed["metadata"]["namespace"]
            .as_str()
            .unwrap_or(&self.config.namespace);

        let path = match kind {
            "Pod" => format!("/api/v1/namespaces/{}/pods", namespace),
            "Service" => format!("/api/v1/namespaces/{}/services", namespace),
            "ConfigMap" => format!("/api/v1/namespaces/{}/configmaps", namespace),
            "Secret" => format!("/api/v1/namespaces/{}/secrets", namespace),
            "Deployment" => format!("/apis/apps/v1/namespaces/{}/deployments", namespace),
            "StatefulSet" => format!("/apis/apps/v1/namespaces/{}/statefulsets", namespace),
            "DaemonSet" => format!("/apis/apps/v1/namespaces/{}/daemonsets", namespace),
            _ => return Err(anyhow::anyhow!("Unsupported resource kind: {}", kind)),
        };

        self.api_request("POST", &path, Some(&parsed)).await
    }

    /// Delete a resource
    async fn delete_resource(&self, resource: &str, name: &str) -> Result<()> {
        info!(
            "Kubernetes reflex '{}' deleting {}/{}",
            self.name, resource, name
        );

        let path = match resource {
            "deployment" | "Deployment" => {
                format!(
                    "/apis/apps/v1/namespaces/{}/deployments/{}",
                    self.config.namespace, name
                )
            }
            "pod" | "Pod" => {
                format!("/api/v1/namespaces/{}/pods/{}", self.config.namespace, name)
            }
            "service" | "Service" => {
                format!(
                    "/api/v1/namespaces/{}/services/{}",
                    self.config.namespace, name
                )
            }
            "configmap" | "ConfigMap" => {
                format!(
                    "/api/v1/namespaces/{}/configmaps/{}",
                    self.config.namespace, name
                )
            }
            "secret" | "Secret" => {
                format!(
                    "/api/v1/namespaces/{}/secrets/{}",
                    self.config.namespace, name
                )
            }
            "statefulset" | "StatefulSet" => {
                format!(
                    "/apis/apps/v1/namespaces/{}/statefulsets/{}",
                    self.config.namespace, name
                )
            }
            _ => return Err(anyhow::anyhow!("Unsupported resource type: {}", resource)),
        };

        self.api_request("DELETE", &path, None).await?;

        info!(
            "Kubernetes reflex '{}' deleted {}/{}",
            self.name, resource, name
        );
        Ok(())
    }

    /// Add labels to a resource
    async fn add_labels(
        &self,
        resource: &str,
        name: &str,
        labels: &serde_json::Value,
    ) -> Result<()> {
        info!(
            "Kubernetes reflex '{}' adding labels to {}/{}",
            self.name, resource, name
        );

        let path = match resource {
            "deployment" | "Deployment" => {
                format!(
                    "/apis/apps/v1/namespaces/{}/deployments/{}",
                    self.config.namespace, name
                )
            }
            "pod" | "Pod" => {
                format!("/api/v1/namespaces/{}/pods/{}", self.config.namespace, name)
            }
            _ => {
                return Err(anyhow::anyhow!(
                    "Unsupported resource type for labeling: {}",
                    resource
                ));
            }
        };

        let patch = serde_json::json!({
            "metadata": {
                "labels": labels
            }
        });

        self.api_request("PATCH", &path, Some(&patch)).await?;

        info!(
            "Kubernetes reflex '{}' added labels to {}/{}",
            self.name, resource, name
        );
        Ok(())
    }

    /// Get cluster info (node list)
    async fn cluster_info(&self) -> Result<serde_json::Value> {
        let path = "/api/v1/nodes";
        self.api_request("GET", path, None).await
    }
}

#[async_trait]
impl Reflex for KubernetesReflex {
    fn name(&self) -> &str {
        &self.name
    }

    fn r#type(&self) -> &str {
        "kubernetes"
    }

    async fn handle(&self, ink: &Ink) -> Result<()> {
        info!(
            "Kubernetes reflex '{}' handling ink on topic: {}",
            self.name, ink.topic
        );

        let action = self.resolve_action(ink);
        let resource_name = self.resolve_resource_name(ink)?;
        let resource = &self.config.resource;

        debug!(
            "Kubernetes reflex '{}' action={}, resource={}/{}",
            self.name, action, resource, resource_name
        );

        match action.as_str() {
            "get" | "inspect" => {
                let result = self.get_resource(resource, &resource_name).await?;
                debug!("Kubernetes reflex '{}' get result: {}", self.name, result);
            }
            "scale" => {
                let replicas = self
                    .config
                    .replicas
                    .context("Replicas count is required for scale action")?;
                self.scale_deployment(&resource_name, replicas).await?;
            }
            "restart" => {
                self.restart_deployment(&resource_name).await?;
            }
            "logs" => {
                let logs = self.get_pod_logs(&resource_name, None).await?;
                info!(
                    "Kubernetes reflex '{}' logs for {}: {}",
                    self.name, resource_name, logs
                );
            }
            "apply" => {
                let manifest = self
                    .config
                    .manifest
                    .as_ref()
                    .context("Manifest is required for apply action")?;
                self.apply_manifest(manifest).await?;
            }
            "delete" | "rm" => {
                self.delete_resource(resource, &resource_name).await?;
            }
            "label" => {
                let labels_str = self
                    .config
                    .labels_template
                    .as_ref()
                    .context("labels_template is required for label action")?;
                let labels_rendered = self.render_template(labels_str, ink);
                let labels: serde_json::Value =
                    serde_json::from_str(&labels_rendered).context("Invalid labels JSON")?;
                self.add_labels(resource, &resource_name, &labels).await?;
            }
            "exec" => {
                let cmd = self
                    .config
                    .command
                    .as_ref()
                    .map(|c| self.render_template(c, ink))
                    .unwrap_or_else(|| ink.body.clone());
                // Exec requires WebSocket, use kubectl proxy or HTTP exec
                info!(
                    "Kubernetes reflex '{}' exec action requires WebSocket — logging command: {}",
                    self.name, cmd
                );
            }
            _ => {
                error!(
                    "Kubernetes reflex '{}' unknown action: {}",
                    self.name, action
                );
                return Err(anyhow::anyhow!("Unknown Kubernetes action: {}", action));
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_config() -> KubernetesReflexConfig {
        KubernetesReflexConfig {
            api_url: "https://kubernetes.default.svc".to_string(),
            token: Some("my-token".to_string()),
            kubeconfig: None,
            namespace: "default".to_string(),
            action: "get".to_string(),
            resource: "deployment".to_string(),
            resource_name: Some("my-app".to_string()),
            resource_name_template: None,
            topic_action_mapping: HashMap::new(),
            topic_resource_mapping: HashMap::new(),
            replicas: None,
            command: None,
            manifest: None,
            labels_template: None,
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
        let reflex = KubernetesReflex::new("test".to_string(), config, &app_ctx);
        let ink = create_test_ink();
        assert_eq!(
            reflex.render_template("{{topic}}-{{body}}", &ink),
            "/test/topic-test body"
        );
    }

    #[test]
    fn test_config_deserialization() {
        let yaml = r#"
            api_url: "https://k8s.example.com"
            namespace: "production"
            action: "scale"
            replicas: 3
        "#;
        let config: KubernetesReflexConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.namespace, "production");
        assert_eq!(config.action, "scale");
        assert_eq!(config.replicas, Some(3));
    }

    #[test]
    fn test_reflex_type() {
        let config = create_test_config();
        let app_ctx = AppContext::new();
        let reflex = KubernetesReflex::new("test".to_string(), config, &app_ctx);
        assert_eq!(reflex.r#type(), "kubernetes");
    }

    #[test]
    fn test_resolve_action_from_mapping() {
        let mut config = create_test_config();
        let mut mapping = HashMap::new();
        mapping.insert("/k8s/scale".to_string(), "scale".to_string());
        config.topic_action_mapping = mapping;

        let app_ctx = AppContext::new();
        let reflex = KubernetesReflex::new("test".to_string(), config, &app_ctx);
        let ink = Ink::new("/k8s/scale", "test", "test_source", "body");
        assert_eq!(reflex.resolve_action(&ink), "scale");
    }
}
