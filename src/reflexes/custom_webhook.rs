use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::{debug, info};

use super::Reflex;
use crate::appcontext::AppContext;
use crate::model::Ink;

/// Configuration for Custom Webhook Reflex
/// Sends HTTP requests to arbitrary webhook URLs with full templating
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CustomWebhookReflexConfig {
    /// Webhook URL
    pub url: String,

    /// HTTP method (GET, POST, PUT, DELETE, PATCH)
    #[serde(default = "default_method")]
    pub method: String,

    /// Content-Type header
    #[serde(default = "default_content_type")]
    pub content_type: String,

    /// Template for the request body
    #[serde(default = "default_body_template")]
    pub body_template: String,

    /// Authentication type: none, bearer, basic, api_key
    #[serde(default = "default_auth_type")]
    pub auth_type: String,

    /// Auth token (for bearer)
    #[serde(default)]
    pub auth_token: Option<String>,

    /// Auth username (for basic)
    #[serde(default)]
    pub auth_username: Option<String>,

    /// Auth password (for basic)
    #[serde(default)]
    pub auth_password: Option<String>,

    /// API key header name (for api_key auth)
    #[serde(default = "default_api_key_header")]
    pub api_key_header: String,

    /// API key value (for api_key auth)
    #[serde(default)]
    pub api_key_value: Option<String>,

    /// Custom headers
    #[serde(default)]
    pub headers: HashMap<String, String>,

    /// Optional: mapping from Ink topics to URLs
    #[serde(default)]
    pub topic_url_mapping: HashMap<String, String>,

    /// Optional: mapping from Ink topics to methods
    #[serde(default)]
    pub topic_method_mapping: HashMap<String, String>,

    /// Follow redirects
    #[serde(default = "default_follow_redirects")]
    pub follow_redirects: bool,

    /// Request timeout in seconds
    #[serde(default = "default_timeout")]
    pub timeout: u64,

    /// Verify SSL
    #[serde(default = "default_verify_ssl")]
    pub verify_ssl: bool,

    /// Number of retries on failure
    #[serde(default)]
    pub retries: u32,

    /// Retry delay in milliseconds
    #[serde(default = "default_retry_delay")]
    pub retry_delay_ms: u64,
}

fn default_method() -> String {
    "POST".to_string()
}
fn default_content_type() -> String {
    "application/json".to_string()
}
fn default_body_template() -> String {
    "{\"topic\":\"{{topic}}\",\"body\":\"{{body}}\"}".to_string()
}
fn default_auth_type() -> String {
    "none".to_string()
}
fn default_api_key_header() -> String {
    "X-API-Key".to_string()
}
fn default_follow_redirects() -> bool {
    true
}
fn default_timeout() -> u64 {
    30
}
fn default_verify_ssl() -> bool {
    true
}
fn default_retry_delay() -> u64 {
    1000
}

pub struct CustomWebhookReflex {
    name: String,
    config: CustomWebhookReflexConfig,
    client: reqwest::Client,
}

impl CustomWebhookReflex {
    pub fn new(name: String, config: CustomWebhookReflexConfig, app_ctx: &AppContext) -> Self {
        let client = app_ctx.http_client_custom(config.verify_ssl, config.follow_redirects);
        Self {
            name,
            config,
            client,
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

    fn resolve_url(&self, ink: &Ink) -> String {
        if let Some(url) = self
            .config
            .topic_url_mapping
            .get(&ink.topic)
            .filter(|u| !u.is_empty())
        {
            return self.render_template(url, ink);
        }
        self.render_template(&self.config.url, ink)
    }

    fn resolve_method(&self, ink: &Ink) -> String {
        if let Some(method) = self
            .config
            .topic_method_mapping
            .get(&ink.topic)
            .filter(|m| !m.is_empty())
        {
            return method.clone();
        }
        self.config.method.clone()
    }

    fn build_request(&self, method: &str, url: &str, body: String) -> reqwest::RequestBuilder {
        let mut request = match method.to_uppercase().as_str() {
            "GET" => self.client.get(url),
            "PUT" => self.client.put(url),
            "DELETE" => self.client.delete(url),
            "PATCH" => self.client.patch(url),
            _ => self.client.post(url),
        };

        request = request.header("Content-Type", &self.config.content_type);

        // Apply authentication
        match self.config.auth_type.as_str() {
            "bearer" => {
                if let Some(ref token) = self.config.auth_token {
                    request = request.header("Authorization", format!("Bearer {}", token));
                }
            }
            "basic" => {
                if let (Some(user), Some(pass)) =
                    (&self.config.auth_username, &self.config.auth_password)
                {
                    request = request.basic_auth(user, Some(pass));
                }
            }
            "api_key" => {
                if let Some(ref key) = self.config.api_key_value {
                    request = request.header(&self.config.api_key_header, key);
                }
            }
            _ => {}
        }

        // Apply custom headers
        for (k, v) in &self.config.headers {
            request = request.header(k, v);
        }

        // Add body for non-GET methods
        if method.to_uppercase() != "GET" {
            request = request.body(body);
        }

        request
    }
}

#[async_trait]
impl Reflex for CustomWebhookReflex {
    fn name(&self) -> &str {
        &self.name
    }
    fn r#type(&self) -> &str {
        "custom_webhook"
    }

    async fn handle(&self, ink: &Ink) -> Result<()> {
        info!(
            "Custom Webhook reflex '{}' handling ink on topic: {}",
            self.name, ink.topic
        );

        let url = self.resolve_url(ink);
        let method = self.resolve_method(ink);
        let body = self.render_template(&self.config.body_template, ink);

        debug!(
            "Custom Webhook reflex '{}' {} {} with body: {}",
            self.name, method, url, body
        );

        let mut last_error = None;
        let attempts = self.config.retries.max(1);

        for attempt in 0..attempts {
            let request = self.build_request(&method, &url, body.clone());
            match request.send().await {
                Ok(response) => {
                    if response.status().is_success() {
                        info!(
                            "Custom Webhook reflex '{}' successfully sent {} to {}",
                            self.name, method, url
                        );
                        return Ok(());
                    } else {
                        let status = response.status();
                        let error_body = response.text().await.unwrap_or_default();
                        last_error = Some(anyhow::anyhow!(
                            "Webhook API error ({}): {}",
                            status,
                            error_body
                        ));
                    }
                }
                Err(e) => {
                    last_error = Some(e.into());
                }
            }

            if attempt < attempts - 1 {
                debug!(
                    "Custom Webhook reflex '{}' retry {} after {}ms",
                    self.name,
                    attempt + 1,
                    self.config.retry_delay_ms
                );
                tokio::time::sleep(std::time::Duration::from_millis(self.config.retry_delay_ms))
                    .await;
            }
        }

        Err(last_error.unwrap_or_else(|| {
            anyhow::anyhow!(
                "Custom Webhook reflex '{}' failed after {} attempts",
                self.name,
                attempts
            )
        }))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::appcontext::AppContext;

    fn create_test_config() -> CustomWebhookReflexConfig {
        CustomWebhookReflexConfig {
            url: "https://webhook.example.com/hook".to_string(),
            method: "POST".to_string(),
            content_type: "application/json".to_string(),
            body_template: "{\"topic\":\"{{topic}}\",\"body\":\"{{body}}\"}".to_string(),
            auth_type: "none".to_string(),
            auth_token: None,
            auth_username: None,
            auth_password: None,
            api_key_header: "X-API-Key".to_string(),
            api_key_value: None,
            headers: HashMap::new(),
            topic_url_mapping: HashMap::new(),
            topic_method_mapping: HashMap::new(),
            follow_redirects: true,
            timeout: 30,
            verify_ssl: true,
            retries: 1,
            retry_delay_ms: 1000,
        }
    }

    fn create_test_ink() -> Ink {
        Ink::new("/test/topic", "test", "test_source", "test body")
    }

    #[test]
    fn test_template_rendering() {
        let config = create_test_config();
        let reflex = CustomWebhookReflex::new("test".to_string(), config, &AppContext::new());
        let ink = create_test_ink();
        let rendered = reflex.render_template(&reflex.config.body_template, &ink);
        assert!(rendered.contains("/test/topic"));
        assert!(rendered.contains("test body"));
    }

    #[test]
    fn test_resolve_url() {
        let config = create_test_config();
        let reflex = CustomWebhookReflex::new("test".to_string(), config, &AppContext::new());
        let ink = create_test_ink();
        assert_eq!(reflex.resolve_url(&ink), "https://webhook.example.com/hook");
    }

    #[test]
    fn test_resolve_method() {
        let config = create_test_config();
        let reflex = CustomWebhookReflex::new("test".to_string(), config, &AppContext::new());
        let ink = create_test_ink();
        assert_eq!(reflex.resolve_method(&ink), "POST");
    }

    #[test]
    fn test_config_deserialization() {
        let yaml = r#"
            url: "https://hooks.example.com/kraken"
            method: "POST"
            auth_type: "bearer"
            auth_token: "secret-token"
            retries: 3
        "#;
        let config: CustomWebhookReflexConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.url, "https://hooks.example.com/kraken");
        assert_eq!(config.auth_type, "bearer");
    }

    #[test]
    fn test_reflex_type() {
        let config = create_test_config();
        let reflex = CustomWebhookReflex::new("test".to_string(), config, &AppContext::new());
        assert_eq!(reflex.r#type(), "custom_webhook");
    }
}
