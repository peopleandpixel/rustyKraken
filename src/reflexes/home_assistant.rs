use anyhow::{Context, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::{debug, error, info, warn};

use super::Reflex;
use crate::model::Ink;

/// Action type for Home Assistant Reflex
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum HaActionType {
    Event,
    Service,
}

/// Configuration for Home Assistant Reflex
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HomeAssistantReflexConfig {
    /// Home Assistant server URL (e.g., "http://homeassistant.local:8123")
    pub server: String,

    /// Long-lived access token for authentication
    pub access_token: String,

    /// Action type: event (fire custom event) or service (call a service)
    #[serde(default = "default_action_type")]
    pub action_type: HaActionType,

    /// Default event name for event action type
    #[serde(default = "default_event_name")]
    pub event_name: String,

    /// Default service domain for service action type (e.g., "notify")
    #[serde(default)]
    pub service_domain: Option<String>,

    /// Default service name for service action type (e.g., "mobile_notification")
    #[serde(default)]
    pub service_name: Option<String>,

    /// Template for event data message / service data message
    /// Supported placeholders: {{topic}}, {{body}}, {{id}}, {{timestamp}}, {{metadata.<key>}}
    #[serde(default = "default_message_template")]
    pub message_template: String,

    /// Template for event data title / notification title
    /// Supported placeholders: {{topic}}, {{body}}, {{id}}, {{timestamp}}, {{metadata.<key>}}
    #[serde(default = "default_title_template")]
    pub title_template: String,

    /// Optional: mapping from Ink topics to event names
    #[serde(default)]
    pub topic_event_mapping: HashMap<String, String>,

    /// Optional: mapping from Ink topics to service calls (format: "domain/service")
    #[serde(default)]
    pub topic_service_mapping: HashMap<String, String>,

    /// Optional: mapping from Ink topics to message templates
    #[serde(default)]
    pub topic_message_mapping: HashMap<String, String>,

    /// Optional: mapping from Ink topics to title templates
    #[serde(default)]
    pub topic_title_mapping: HashMap<String, String>,

    /// Optional: additional static event data / service data (merged with template data)
    #[serde(default)]
    pub additional_data: HashMap<String, String>,

    /// Request timeout in seconds
    #[serde(default = "default_timeout_secs")]
    pub timeout_secs: u64,

    /// Number of retry attempts on failure
    #[serde(default = "default_retries")]
    pub retries: u32,

    /// Delay between retries in milliseconds
    #[serde(default = "default_retry_delay_ms")]
    pub retry_delay_ms: u64,
}

fn default_action_type() -> HaActionType {
    HaActionType::Event
}

fn default_event_name() -> String {
    "rustykraken_event".to_string()
}

fn default_message_template() -> String {
    "{{body}}".to_string()
}

fn default_title_template() -> String {
    "{{topic}}".to_string()
}

fn default_timeout_secs() -> u64 {
    30
}

fn default_retries() -> u32 {
    3
}

fn default_retry_delay_ms() -> u64 {
    1000
}

pub struct HomeAssistantReflex {
    name: String,
    config: HomeAssistantReflexConfig,
    client: reqwest::Client,
}

impl HomeAssistantReflex {
    pub fn new(name: String, config: HomeAssistantReflexConfig) -> Result<Self> {
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(config.timeout_secs))
            .build()
            .context("Failed to build HTTP client")?;

        Ok(Self {
            name,
            config,
            client,
        })
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

    /// Resolve event name for a given Ink
    fn resolve_event_name(&self, ink: &Ink) -> String {
        if let Some(event_name) = self
            .config
            .topic_event_mapping
            .get(&ink.topic)
            .filter(|e| !e.is_empty())
        {
            return event_name.clone();
        }
        self.config.event_name.clone()
    }

    /// Resolve service call for a given Ink. Returns (domain, service).
    fn resolve_service(&self, ink: &Ink) -> (String, String) {
        if let Some(service_call) = self
            .config
            .topic_service_mapping
            .get(&ink.topic)
            .filter(|s| !s.is_empty())
            && let Some((domain, service)) = service_call.split_once('/')
        {
            return (domain.to_string(), service.to_string());
        }
        // Fallback to configured defaults

        (
            self.config
                .service_domain
                .clone()
                .unwrap_or_else(|| "persistent_notification".to_string()),
            self.config
                .service_name
                .clone()
                .unwrap_or_else(|| "create".to_string()),
        )
    }

    /// Resolve message template for a given Ink
    fn resolve_message_template(&self, ink: &Ink) -> String {
        if let Some(template) = self
            .config
            .topic_message_mapping
            .get(&ink.topic)
            .filter(|t| !t.is_empty())
        {
            return self.render_template(template, ink);
        }
        self.render_template(&self.config.message_template, ink)
    }

    /// Resolve title template for a given Ink
    fn resolve_title_template(&self, ink: &Ink) -> String {
        if let Some(template) = self
            .config
            .topic_title_mapping
            .get(&ink.topic)
            .filter(|t| !t.is_empty())
        {
            return self.render_template(template, ink);
        }
        self.render_template(&self.config.title_template, ink)
    }

    /// Build the event data JSON payload
    fn build_event_data(&self, ink: &Ink) -> serde_json::Value {
        let message = self.resolve_message_template(ink);
        let title = self.resolve_title_template(ink);

        let mut data = serde_json::Map::new();
        data.insert("title".to_string(), serde_json::json!(title));
        data.insert("message".to_string(), serde_json::json!(message));
        data.insert("topic".to_string(), serde_json::json!(ink.topic.clone()));
        data.insert("id".to_string(), serde_json::json!(ink.id.to_string()));
        data.insert(
            "timestamp".to_string(),
            serde_json::json!(ink.timestamp.to_rfc3339()),
        );

        // Merge additional static data
        for (key, value) in &self.config.additional_data {
            data.insert(key.clone(), serde_json::json!(value));
        }

        // Include Ink metadata if present
        if let Some(metadata) = ink.metadata.as_ref()
            && let Some(obj) = metadata.as_object()
        {
            for (key, value) in obj {
                data.insert(format!("metadata_{}", key), value.clone());
            }
        }

        serde_json::Value::Object(data)
    }

    /// Build the service data JSON payload
    fn build_service_data(&self, ink: &Ink) -> serde_json::Value {
        let message = self.resolve_message_template(ink);
        let title = self.resolve_title_template(ink);

        let mut data = serde_json::Map::new();
        data.insert("title".to_string(), serde_json::json!(title));
        data.insert("message".to_string(), serde_json::json!(message));

        // Merge additional static data
        for (key, value) in &self.config.additional_data {
            data.insert(key.clone(), serde_json::json!(value));
        }

        serde_json::Value::Object(data)
    }

    /// Fire a custom event in Home Assistant
    async fn fire_event(&self, ink: &Ink) -> Result<()> {
        let event_name = self.resolve_event_name(ink);
        let data = self.build_event_data(ink);

        // For event firing, we use the event API
        let event_url = format!("{}/api/event/{}", self.config.server, event_name);

        debug!(
            "Home Assistant reflex '{}' firing event '{}' via {}",
            self.name, event_name, event_url
        );

        let response = self
            .client
            .post(&event_url)
            .header(
                "Authorization",
                format!("Bearer {}", self.config.access_token),
            )
            .header("Content-Type", "application/json")
            .json(&data)
            .send()
            .await
            .context("Failed to send event to Home Assistant")?;

        let status = response.status();
        if !status.is_success() {
            let error_text = response
                .text()
                .await
                .unwrap_or_else(|_| "Unknown error".to_string());
            anyhow::bail!(
                "Home Assistant event API returned {}: {}",
                status,
                error_text
            );
        }

        info!(
            "Home Assistant reflex '{}' successfully fired event '{}'",
            self.name, event_name
        );
        Ok(())
    }

    /// Call a service in Home Assistant
    async fn call_service(&self, ink: &Ink) -> Result<()> {
        let (domain, service) = self.resolve_service(ink);
        let data = self.build_service_data(ink);

        let url = format!("{}/api/services/{}/{}", self.config.server, domain, service);

        debug!(
            "Home Assistant reflex '{}' calling service '{}/{}' via {}",
            self.name, domain, service, url
        );

        let response = self
            .client
            .post(&url)
            .header(
                "Authorization",
                format!("Bearer {}", self.config.access_token),
            )
            .header("Content-Type", "application/json")
            .json(&data)
            .send()
            .await
            .context("Failed to call service in Home Assistant")?;

        let status = response.status();
        if !status.is_success() {
            let error_text = response
                .text()
                .await
                .unwrap_or_else(|_| "Unknown error".to_string());
            anyhow::bail!(
                "Home Assistant service API returned {}: {}",
                status,
                error_text
            );
        }

        info!(
            "Home Assistant reflex '{}' successfully called service '{}/{}'",
            self.name, domain, service
        );
        Ok(())
    }

    /// Send to Home Assistant with retry logic
    async fn send_with_retry(&self, ink: &Ink) -> Result<()> {
        let mut last_error = None;

        for attempt in 0..=self.config.retries {
            if attempt > 0 {
                warn!(
                    "Home Assistant reflex '{}' retrying (attempt {}/{}) after {}ms delay",
                    self.name, attempt, self.config.retries, self.config.retry_delay_ms
                );
                tokio::time::sleep(std::time::Duration::from_millis(self.config.retry_delay_ms))
                    .await;
            }

            match self.config.action_type {
                HaActionType::Event => match self.fire_event(ink).await {
                    Ok(()) => return Ok(()),
                    Err(e) => {
                        error!(
                            "Home Assistant reflex '{}' failed to fire event (attempt {}/{}): {}",
                            self.name,
                            attempt + 1,
                            self.config.retries + 1,
                            e
                        );
                        last_error = Some(e);
                    }
                },
                HaActionType::Service => match self.call_service(ink).await {
                    Ok(()) => return Ok(()),
                    Err(e) => {
                        error!(
                            "Home Assistant reflex '{}' failed to call service (attempt {}/{}): {}",
                            self.name,
                            attempt + 1,
                            self.config.retries + 1,
                            e
                        );
                        last_error = Some(e);
                    }
                },
            }
        }

        Err(last_error
            .unwrap_or_else(|| anyhow::anyhow!("Home Assistant send failed after retries")))
    }
}

#[async_trait]
impl Reflex for HomeAssistantReflex {
    fn name(&self) -> &str {
        &self.name
    }

    fn r#type(&self) -> &str {
        "home_assistant"
    }

    async fn handle(&self, ink: &Ink) -> Result<()> {
        info!(
            "Home Assistant reflex '{}' handling ink on topic: {}",
            self.name, ink.topic
        );

        match self.send_with_retry(ink).await {
            Ok(()) => {
                info!(
                    "Home Assistant reflex '{}' successfully handled ink for topic: {}",
                    self.name, ink.topic
                );
                Ok(())
            }
            Err(e) => {
                error!(
                    "Home Assistant reflex '{}' failed to handle ink: {}",
                    self.name, e
                );
                Err(e)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn create_test_config() -> HomeAssistantReflexConfig {
        HomeAssistantReflexConfig {
            server: "http://homeassistant.local:8123".to_string(),
            access_token: "eyJ...".to_string(),
            action_type: HaActionType::Event,
            event_name: "rustykraken_event".to_string(),
            service_domain: None,
            service_name: None,
            message_template: "{{body}}".to_string(),
            title_template: "{{topic}}".to_string(),
            topic_event_mapping: HashMap::new(),
            topic_service_mapping: HashMap::new(),
            topic_message_mapping: HashMap::new(),
            topic_title_mapping: HashMap::new(),
            additional_data: HashMap::new(),
            timeout_secs: 30,
            retries: 3,
            retry_delay_ms: 1000,
        }
    }

    fn create_test_ink() -> Ink {
        let mut ink = Ink::new(
            "/test/topic",
            "test",
            "test_source",
            "Test Home Assistant message",
        );
        ink.metadata = Some(json!({
            "sensor_id": "temp_01",
            "value": 23.5
        }));
        ink
    }

    #[test]
    fn test_reflex_name_and_type() {
        let config = create_test_config();
        let reflex = HomeAssistantReflex::new("ha_test".to_string(), config).unwrap();
        assert_eq!(reflex.name(), "ha_test");
        assert_eq!(reflex.r#type(), "home_assistant");
    }

    #[test]
    fn test_template_rendering() {
        let config = create_test_config();
        let reflex = HomeAssistantReflex::new("ha_test".to_string(), config).unwrap();
        let ink = create_test_ink();

        let rendered = reflex.render_template("{{topic}}: {{body}}", &ink);
        assert_eq!(rendered, "/test/topic: Test Home Assistant message");

        let rendered = reflex.render_template("{{metadata.sensor_id}}", &ink);
        assert_eq!(rendered, "temp_01");

        let rendered = reflex.render_template("{{id}}", &ink);
        assert_eq!(rendered, ink.id.to_string());
    }

    #[test]
    fn test_event_name_resolution() {
        let mut config = create_test_config();
        config
            .topic_event_mapping
            .insert("/sensor/temp".to_string(), "temperature_event".to_string());

        let reflex = HomeAssistantReflex::new("ha_test".to_string(), config).unwrap();

        let mut ink = create_test_ink();
        ink.topic = "/sensor/temp".to_string();
        assert_eq!(reflex.resolve_event_name(&ink), "temperature_event");

        ink.topic = "/other/topic".to_string();
        assert_eq!(reflex.resolve_event_name(&ink), "rustykraken_event");
    }

    #[test]
    fn test_service_resolution() {
        let mut config = create_test_config();
        config
            .topic_service_mapping
            .insert("/notify".to_string(), "notify/mobile_app".to_string());

        let reflex = HomeAssistantReflex::new("ha_test".to_string(), config).unwrap();

        let mut ink = create_test_ink();
        ink.topic = "/notify".to_string();
        let (domain, service) = reflex.resolve_service(&ink);
        assert_eq!(domain, "notify");
        assert_eq!(service, "mobile_app");

        ink.topic = "/other".to_string();
        let (domain, service) = reflex.resolve_service(&ink);
        assert_eq!(domain, "persistent_notification");
        assert_eq!(service, "create");
    }

    #[test]
    fn test_message_template_resolution() {
        let mut config = create_test_config();
        config.topic_message_mapping.insert(
            "/sensor/**".to_string(),
            "Sensor reading: {{body}}".to_string(),
        );
        config.message_template = "{{body}}".to_string();

        let reflex = HomeAssistantReflex::new("ha_test".to_string(), config).unwrap();

        let ink = create_test_ink();
        let message = reflex.resolve_message_template(&ink);
        assert_eq!(message, "Test Home Assistant message");
    }

    #[test]
    fn test_title_template_resolution() {
        let mut config = create_test_config();
        config
            .topic_title_mapping
            .insert("/alerts".to_string(), "Alert: {{topic}}".to_string());

        let reflex = HomeAssistantReflex::new("ha_test".to_string(), config).unwrap();

        let mut ink = create_test_ink();
        ink.topic = "/alerts".to_string();
        let title = reflex.resolve_title_template(&ink);
        assert_eq!(title, "Alert: /alerts");

        ink.topic = "/other".to_string();
        let title = reflex.resolve_title_template(&ink);
        assert_eq!(title, "/other");
    }

    #[test]
    fn test_event_data_building() {
        let config = create_test_config();
        let reflex = HomeAssistantReflex::new("ha_test".to_string(), config).unwrap();
        let ink = create_test_ink();

        let data = reflex.build_event_data(&ink);
        let obj = data.as_object().expect("event data should be an object");

        assert_eq!(
            obj.get("message").and_then(|v| v.as_str()),
            Some("Test Home Assistant message")
        );
        assert_eq!(
            obj.get("title").and_then(|v| v.as_str()),
            Some("/test/topic")
        );
        assert_eq!(
            obj.get("topic").and_then(|v| v.as_str()),
            Some("/test/topic")
        );
        assert_eq!(
            obj.get("metadata_sensor_id").and_then(|v| v.as_str()),
            Some("temp_01")
        );
    }

    #[test]
    fn test_service_data_building() {
        let config = create_test_config();
        let reflex = HomeAssistantReflex::new("ha_test".to_string(), config).unwrap();
        let ink = create_test_ink();

        let data = reflex.build_service_data(&ink);
        let obj = data.as_object().expect("service data should be an object");

        assert_eq!(
            obj.get("message").and_then(|v| v.as_str()),
            Some("Test Home Assistant message")
        );
        assert_eq!(
            obj.get("title").and_then(|v| v.as_str()),
            Some("/test/topic")
        );
    }

    #[test]
    fn test_additional_data_merging() {
        let mut config = create_test_config();
        let mut additional = HashMap::new();
        additional.insert("custom_field".to_string(), "custom_value".to_string());
        additional.insert("priority".to_string(), "high".to_string());
        config.additional_data = additional;

        let reflex = HomeAssistantReflex::new("ha_test".to_string(), config).unwrap();
        let ink = create_test_ink();

        let data = reflex.build_event_data(&ink);
        let obj = data.as_object().expect("event data should be an object");

        assert_eq!(
            obj.get("custom_field").and_then(|v| v.as_str()),
            Some("custom_value")
        );
        assert_eq!(obj.get("priority").and_then(|v| v.as_str()), Some("high"));
    }

    #[test]
    fn test_config_deserialization() {
        let yaml = r#"
            server: "http://homeassistant.local:8123"
            access_token: "eyJ..."
            action_type: "service"
            service_domain: "notify"
            service_name: "mobile_app"
            event_name: "custom_event"
            message_template: "Alert: {{body}}"
            title_template: "{{topic}}"
            topic_event_mapping:
              "/sensor/temp": "temperature_event"
            topic_service_mapping:
              "/notify/phone": "notify/mobile_app_phone"
            additional_data:
              priority: "high"
            timeout_secs: 60
            retries: 5
        "#;

        let config: HomeAssistantReflexConfig =
            serde_yaml::from_str(yaml).expect("Failed to deserialize config");

        assert_eq!(config.server, "http://homeassistant.local:8123");
        assert_eq!(config.action_type, HaActionType::Service);
        assert_eq!(config.service_domain, Some("notify".to_string()));
        assert_eq!(config.service_name, Some("mobile_app".to_string()));
        assert_eq!(config.event_name, "custom_event");
        assert_eq!(config.retries, 5);
        assert_eq!(
            config
                .topic_event_mapping
                .get("/sensor/temp")
                .map(|s| s.as_str()),
            Some("temperature_event")
        );
    }

    #[test]
    fn test_default_config_values() {
        let yaml = r#"
            server: "http://localhost:8123"
            access_token: "token"
        "#;

        let config: HomeAssistantReflexConfig =
            serde_yaml::from_str(yaml).expect("Failed to deserialize config");

        assert_eq!(config.action_type, HaActionType::Event);
        assert_eq!(config.event_name, "rustykraken_event");
        assert_eq!(config.message_template, "{{body}}");
        assert_eq!(config.title_template, "{{topic}}");
        assert_eq!(config.timeout_secs, 30);
        assert_eq!(config.retries, 3);
        assert_eq!(config.retry_delay_ms, 1000);
    }

    #[test]
    fn test_metadata_template_rendering() {
        let config = create_test_config();
        let reflex = HomeAssistantReflex::new("ha_test".to_string(), config).unwrap();

        let mut ink = create_test_ink();
        ink.metadata = Some(json!({
            "temperature": 23.5,
            "humidity": 60,
            "location": "kitchen"
        }));

        let template = "{{metadata.temperature}}°C at {{metadata.location}}";
        let rendered = reflex.render_template(template, &ink);
        assert_eq!(rendered, "23.5°C at kitchen");
    }
}
