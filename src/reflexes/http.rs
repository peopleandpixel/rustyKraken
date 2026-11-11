use anyhow::{Context, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;
use tracing::{debug, error, info, warn};

use super::Reflex;
use crate::model::Ink;

/// HTTP method for the request
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum HttpMethod {
    Get,
    Post,
    Put,
    Delete,
    Patch,
}

impl std::fmt::Display for HttpMethod {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            HttpMethod::Get => write!(f, "GET"),
            HttpMethod::Post => write!(f, "POST"),
            HttpMethod::Put => write!(f, "PUT"),
            HttpMethod::Delete => write!(f, "DELETE"),
            HttpMethod::Patch => write!(f, "PATCH"),
        }
    }
}

impl From<HttpMethod> for http::Method {
    fn from(method: HttpMethod) -> Self {
        match method {
            HttpMethod::Get => http::Method::GET,
            HttpMethod::Post => http::Method::POST,
            HttpMethod::Put => http::Method::PUT,
            HttpMethod::Delete => http::Method::DELETE,
            HttpMethod::Patch => http::Method::PATCH,
        }
    }
}

/// Authentication configuration for HTTP requests
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum HttpAuth {
    #[serde(rename = "bearer")]
    Bearer { token: String },
    #[serde(rename = "basic")]
    Basic { username: String, password: String },
    #[serde(rename = "api_key")]
    ApiKey {
        /// Header name for the API key (e.g., "X-API-Key")
        header: String,
        value: String,
    },
}

/// Configuration for HTTP/Webhook Reflex
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HttpReflexConfig {
    /// Default URL to send requests to
    pub url: String,

    /// HTTP method (GET, POST, PUT, DELETE, PATCH)
    #[serde(default = "default_method")]
    pub method: HttpMethod,

    /// Optional: custom headers
    #[serde(default)]
    pub headers: HashMap<String, String>,

    /// Optional: authentication configuration
    #[serde(default)]
    pub auth: Option<HttpAuth>,

    /// Template for the request body (for POST/PUT/PATCH)
    /// Supported placeholders: {{topic}}, {{body}}, {{id}}, {{timestamp}}, {{metadata.<key>}}
    #[serde(default = "default_body_template")]
    pub body_template: String,

    /// Template for the URL with placeholders
    /// Supported placeholders: {{topic}}, {{body}}, {{id}}, {{timestamp}}, {{metadata.<key>}}
    #[serde(default)]
    pub url_template: Option<String>,

    /// Content-Type header for the request body
    #[serde(default = "default_content_type")]
    pub content_type: String,

    /// Number of retry attempts on failure
    #[serde(default = "default_retries")]
    pub retries: u32,

    /// Delay between retries in milliseconds
    #[serde(default = "default_retry_delay_ms")]
    pub retry_delay_ms: u64,

    /// Request timeout in seconds
    #[serde(default = "default_timeout_secs")]
    pub timeout_secs: u64,

    /// Optional: mapping from Ink topics to URLs
    #[serde(default)]
    pub topic_url_mapping: HashMap<String, String>,

    /// Optional: mapping from Ink topics to HTTP methods
    #[serde(default)]
    pub topic_method_mapping: HashMap<String, HttpMethod>,

    /// Optional: mapping from Ink topics to body templates
    #[serde(default)]
    pub topic_body_mapping: HashMap<String, String>,

    /// Whether to follow redirects
    #[serde(default = "default_follow_redirects")]
    pub follow_redirects: bool,
}

fn default_method() -> HttpMethod {
    HttpMethod::Post
}

fn default_body_template() -> String {
    serde_json::json!({
        "topic": "{{topic}}",
        "body": "{{body}}",
        "id": "{{id}}",
        "timestamp": "{{timestamp}}"
    })
    .to_string()
}

fn default_content_type() -> String {
    "application/json".to_string()
}

fn default_retries() -> u32 {
    3
}

fn default_retry_delay_ms() -> u64 {
    1000
}

fn default_timeout_secs() -> u64 {
    30
}

fn default_follow_redirects() -> bool {
    true
}

pub struct HttpReflex {
    name: String,
    config: HttpReflexConfig,
    client: reqwest::Client,
}

impl HttpReflex {
    pub fn new(name: String, config: HttpReflexConfig) -> Result<Self> {
        let mut client_builder = reqwest::Client::builder()
            .timeout(Duration::from_secs(config.timeout_secs))
            .redirect(if config.follow_redirects {
                reqwest::redirect::Policy::default()
            } else {
                reqwest::redirect::Policy::none()
            });

        // Set default User-Agent
        client_builder = client_builder.user_agent("rustyKraken/0.17.0");

        let client = client_builder
            .build()
            .context("Failed to build HTTP client")?;

        Ok(Self {
            name,
            config,
            client,
        })
    }

    /// Resolve URL for a given Ink, checking topic-based mapping first
    fn resolve_url(&self, ink: &Ink) -> String {
        // Check topic-based URL mapping first
        if let Some(url) = self
            .config
            .topic_url_mapping
            .get(&ink.topic)
            .filter(|u| !u.is_empty())
        {
            return self.render_template(url, ink);
        }

        // Use url_template if configured, otherwise use the static url
        if let Some(ref url_template) = self.config.url_template {
            return self.render_template(url_template, ink);
        }

        self.config.url.clone()
    }

    /// Resolve HTTP method for a given Ink, checking topic-based mapping first
    fn resolve_method(&self, ink: &Ink) -> HttpMethod {
        if let Some(method) = self.config.topic_method_mapping.get(&ink.topic) {
            return method.clone();
        }
        self.config.method.clone()
    }

    /// Resolve body template for a given Ink, checking topic-based mapping first
    fn resolve_body_template(&self, ink: &Ink) -> String {
        if let Some(body_template) = self
            .config
            .topic_body_mapping
            .get(&ink.topic)
            .filter(|b| !b.is_empty())
        {
            return self.render_template(body_template, ink);
        }
        self.render_template(&self.config.body_template, ink)
    }

    /// Render a template string with Ink data
    fn render_template(&self, template: &str, ink: &Ink) -> String {
        let mut result = template.to_string();
        result = result.replace("{{topic}}", &ink.topic);
        result = result.replace("{{body}}", &ink.body);
        result = result.replace("{{id}}", &ink.id.to_string());
        result = result.replace("{{timestamp}}", &ink.timestamp.to_rfc3339());

        if let Some(obj) = ink.metadata.as_ref().and_then(|m| m.as_object()) {
            for (key, value) in obj {
                let placeholder = format!("{{{{metadata.{}}}}}", key);
                let value_str = match value {
                    serde_json::Value::String(s) => s.clone(),
                    _ => value.to_string(),
                };
                result = result.replace(&placeholder, &value_str);
            }
        }

        result
    }

    /// Build the HTTP request
    fn build_request(&self, ink: &Ink) -> Result<reqwest::Request> {
        let url = self.resolve_url(ink);
        let method = self.resolve_method(ink);
        let body = self.resolve_body_template(ink);

        debug!(
            "HTTP reflex '{}' building {} request to {} for topic: {}",
            self.name, method, url, ink.topic
        );

        let mut request_builder = self.client.request(method.clone().into(), &url);

        // Set Content-Type
        request_builder = request_builder.header("Content-Type", &self.config.content_type);

        // Set custom headers
        for (key, value) in &self.config.headers {
            request_builder = request_builder.header(key, value);
        }

        // Set authentication
        if let Some(ref auth) = self.config.auth {
            request_builder = match auth {
                HttpAuth::Bearer { token } => {
                    request_builder.header("Authorization", format!("Bearer {}", token))
                }
                HttpAuth::Basic { username, password } => {
                    let credentials = base64::Engine::encode(
                        &base64::engine::general_purpose::STANDARD,
                        format!("{}:{}", username, password).as_bytes(),
                    );
                    request_builder.header("Authorization", format!("Basic {}", credentials))
                }
                HttpAuth::ApiKey { header, value } => request_builder.header(header, value),
            };
        }

        // Add body for non-GET methods
        let request_builder = match method {
            HttpMethod::Get => request_builder,
            _ => request_builder.body(body),
        };

        let request = request_builder
            .build()
            .with_context(|| format!("Failed to build HTTP request to {}", url))?;

        Ok(request)
    }

    /// Execute the request with retry logic
    async fn execute_with_retry(&self, request: &reqwest::Request) -> Result<()> {
        let mut last_error = None;

        for attempt in 0..=self.config.retries {
            if attempt > 0 {
                warn!(
                    "HTTP reflex '{}' retrying request (attempt {}/{}) after {}ms delay",
                    self.name, attempt, self.config.retries, self.config.retry_delay_ms
                );
                tokio::time::sleep(Duration::from_millis(self.config.retry_delay_ms)).await;
            }

            // Clone the request for each attempt
            let url = request.url().to_string();
            let method: HttpMethod = request
                .method()
                .as_str()
                .parse_method()
                .unwrap_or(HttpMethod::Post);
            let body = request
                .body()
                .and_then(|b| b.as_bytes())
                .map(|b| b.to_vec());

            let mut request_builder = self
                .client
                .request(method.clone().into(), &url)
                .header("Content-Type", &self.config.content_type);

            // Re-apply headers
            for (key, value) in &self.config.headers {
                request_builder = request_builder.header(key, value);
            }

            // Re-apply auth
            if let Some(ref auth) = self.config.auth {
                request_builder = match auth {
                    HttpAuth::Bearer { token } => {
                        request_builder.header("Authorization", format!("Bearer {}", token))
                    }
                    HttpAuth::Basic { username, password } => {
                        let credentials = base64::Engine::encode(
                            &base64::engine::general_purpose::STANDARD,
                            format!("{}:{}", username, password).as_bytes(),
                        );
                        request_builder.header("Authorization", format!("Basic {}", credentials))
                    }
                    HttpAuth::ApiKey { header, value } => request_builder.header(header, value),
                };
            }

            // Re-apply body
            if let Some(ref b) = body {
                request_builder = request_builder.body(b.clone());
            }

            let response = request_builder.send().await;

            match response {
                Ok(resp) => {
                    let status = resp.status();
                    if status.is_success() {
                        info!(
                            "HTTP reflex '{}' successfully sent {} to {} (status: {})",
                            self.name, method, url, status
                        );
                        return Ok(());
                    } else {
                        let error_msg = format!(
                            "HTTP {} returned status {}: {}",
                            method,
                            status,
                            resp.text().await.unwrap_or_default()
                        );
                        error!("HTTP reflex '{}' request failed: {}", self.name, error_msg);
                        last_error = Some(anyhow::anyhow!(error_msg));
                    }
                }
                Err(e) => {
                    error!("HTTP reflex '{}' request failed: {}", self.name, e);
                    last_error = Some(anyhow::anyhow!(e));
                }
            }
        }

        Err(last_error.unwrap_or_else(|| {
            anyhow::anyhow!(
                "HTTP reflex '{}' request failed after {} retries",
                self.name,
                self.config.retries
            )
        }))
    }
}

/// Helper trait to parse HttpMethod from string
trait ParseMethod {
    fn parse_method(self) -> Option<HttpMethod>;
}

impl ParseMethod for &str {
    fn parse_method(self) -> Option<HttpMethod> {
        match self {
            "GET" => Some(HttpMethod::Get),
            "POST" => Some(HttpMethod::Post),
            "PUT" => Some(HttpMethod::Put),
            "DELETE" => Some(HttpMethod::Delete),
            "PATCH" => Some(HttpMethod::Patch),
            _ => None,
        }
    }
}

#[async_trait]
impl Reflex for HttpReflex {
    fn name(&self) -> &str {
        &self.name
    }

    fn r#type(&self) -> &str {
        "http"
    }

    async fn handle(&self, ink: &Ink) -> Result<()> {
        info!(
            "HTTP reflex '{}' handling ink on topic: {}",
            self.name, ink.topic
        );

        let request = self
            .build_request(ink)
            .with_context(|| "Failed to build HTTP request")?;

        self.execute_with_retry(&request)
            .await
            .with_context(|| format!("HTTP reflex '{}' failed to send request", self.name))?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_config() -> HttpReflexConfig {
        HttpReflexConfig {
            url: "http://example.com/webhook".to_string(),
            method: HttpMethod::Post,
            headers: HashMap::new(),
            auth: None,
            body_template: r#"{"topic":"{{topic}}","body":"{{body}}"}"#.to_string(),
            url_template: None,
            content_type: "application/json".to_string(),
            retries: 3,
            retry_delay_ms: 1000,
            timeout_secs: 30,
            topic_url_mapping: HashMap::new(),
            topic_method_mapping: HashMap::new(),
            topic_body_mapping: HashMap::new(),
            follow_redirects: true,
        }
    }

    fn create_test_ink() -> Ink {
        Ink::new("/test/topic", "test", "test_source", "test body")
    }

    #[test]
    fn test_default_url() {
        let config = create_test_config();
        let reflex = HttpReflex::new("test".to_string(), config).unwrap();

        let ink = create_test_ink();
        let url = reflex.resolve_url(&ink);
        assert_eq!(url, "http://example.com/webhook");
    }

    #[test]
    fn test_url_template() {
        let mut config = create_test_config();
        config.url_template = Some("http://example.com/{{topic}}".to_string());

        let reflex = HttpReflex::new("test".to_string(), config).unwrap();

        let ink = create_test_ink();
        let url = reflex.resolve_url(&ink);
        assert_eq!(url, "http://example.com//test/topic");
    }

    #[test]
    fn test_topic_url_mapping() {
        let mut config = create_test_config();
        let mut mapping = HashMap::new();
        mapping.insert(
            "/test/topic".to_string(),
            "http://example.com/alerts".to_string(),
        );
        config.topic_url_mapping = mapping;

        let reflex = HttpReflex::new("test".to_string(), config).unwrap();

        let ink = create_test_ink();
        let url = reflex.resolve_url(&ink);
        assert_eq!(url, "http://example.com/alerts");
    }

    #[test]
    fn test_default_method() {
        let config = create_test_config();
        let reflex = HttpReflex::new("test".to_string(), config).unwrap();

        let ink = create_test_ink();
        let method = reflex.resolve_method(&ink);
        assert_eq!(method, HttpMethod::Post);
    }

    #[test]
    fn test_topic_method_mapping() {
        let mut config = create_test_config();
        let mut mapping = HashMap::new();
        mapping.insert("/test/topic".to_string(), HttpMethod::Get);
        config.topic_method_mapping = mapping;

        let reflex = HttpReflex::new("test".to_string(), config).unwrap();

        let ink = create_test_ink();
        let method = reflex.resolve_method(&ink);
        assert_eq!(method, HttpMethod::Get);
    }

    #[test]
    fn test_body_template_rendering() {
        let config = create_test_config();
        let reflex = HttpReflex::new("test".to_string(), config).unwrap();

        let ink = create_test_ink();
        let body = reflex.resolve_body_template(&ink);
        assert!(body.contains("\"topic\":\"/test/topic\""));
        assert!(body.contains("\"body\":\"test body\""));
    }

    #[test]
    fn test_topic_body_mapping() {
        let mut config = create_test_config();
        let mut mapping = HashMap::new();
        mapping.insert(
            "/test/topic".to_string(),
            r#"{"alert":"{{body}}"}"#.to_string(),
        );
        config.topic_body_mapping = mapping;

        let reflex = HttpReflex::new("test".to_string(), config).unwrap();

        let ink = create_test_ink();
        let body = reflex.resolve_body_template(&ink);
        assert_eq!(body, r#"{"alert":"test body"}"#);
    }

    #[test]
    fn test_metadata_in_template() {
        let mut config = create_test_config();
        config.body_template = r#"{"temp":"{{metadata.temp}}"}"#.to_string();

        let reflex = HttpReflex::new("test".to_string(), config).unwrap();

        let ink = Ink::new("/test", "test", "test_source", "data")
            .with_metadata(serde_json::json!({ "temp": "42", "humidity": "60" }));
        let body = reflex.resolve_body_template(&ink);
        assert_eq!(body, r#"{"temp":"42"}"#);
    }

    #[test]
    fn test_http_method_display() {
        assert_eq!(format!("{}", HttpMethod::Get), "GET");
        assert_eq!(format!("{}", HttpMethod::Post), "POST");
        assert_eq!(format!("{}", HttpMethod::Put), "PUT");
        assert_eq!(format!("{}", HttpMethod::Delete), "DELETE");
        assert_eq!(format!("{}", HttpMethod::Patch), "PATCH");
    }

    #[test]
    fn test_bearer_auth_config() {
        let mut config = create_test_config();
        config.auth = Some(HttpAuth::Bearer {
            token: "my-secret-token".to_string(),
        });

        let reflex = HttpReflex::new("test".to_string(), config).unwrap();
        assert_eq!(reflex.name(), "test");
        assert_eq!(reflex.r#type(), "http");
    }

    #[test]
    fn test_basic_auth_config() {
        let mut config = create_test_config();
        config.auth = Some(HttpAuth::Basic {
            username: "user".to_string(),
            password: "pass".to_string(),
        });

        let reflex = HttpReflex::new("test".to_string(), config).unwrap();
        assert_eq!(reflex.name(), "test");
    }

    #[test]
    fn test_api_key_auth_config() {
        let mut config = create_test_config();
        config.auth = Some(HttpAuth::ApiKey {
            header: "X-API-Key".to_string(),
            value: "my-api-key".to_string(),
        });

        let reflex = HttpReflex::new("test".to_string(), config).unwrap();
        assert_eq!(reflex.name(), "test");
    }

    #[test]
    fn test_config_deserialization() {
        let yaml = r#"
            url: "http://example.com/webhook"
            method: "POST"
            content_type: "application/json"
            retries: 5
            retry_delay_ms: 2000
            timeout_secs: 60
            auth:
              type: "bearer"
              token: "secret"
        "#;

        let config: HttpReflexConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.url, "http://example.com/webhook");
        assert_eq!(config.method, HttpMethod::Post);
        assert_eq!(config.retries, 5);
        assert_eq!(config.retry_delay_ms, 2000);
        assert!(matches!(config.auth, Some(HttpAuth::Bearer { .. })));
    }

    #[test]
    fn test_timestamp_and_id_in_template() {
        let mut config = create_test_config();
        config.body_template = "{{id}}:{{timestamp}}".to_string();

        let reflex = HttpReflex::new("test".to_string(), config).unwrap();

        let ink = create_test_ink();
        let body = reflex.resolve_body_template(&ink);
        assert!(body.starts_with(&ink.id.to_string()));
        assert!(body.contains(&ink.timestamp.to_rfc3339()));
    }
}
