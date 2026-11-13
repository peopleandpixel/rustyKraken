use anyhow::{Context, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, info};

use super::Reflex;
use crate::appcontext::AppContext;
use crate::model::Ink;

/// Configuration for Azure Service Bus Reflex (HTTP API)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AzureServiceBusReflexConfig {
    /// Service Bus namespace
    pub namespace: String,
    /// SAS key name
    pub sas_key_name: String,
    /// SAS key value
    pub sas_key_value: String,
    /// Default topic or queue name
    pub destination: String,
    /// Template for the message body
    #[serde(default = "default_message_template")]
    pub message_template: String,
    /// Optional: mapping from Ink topics to destinations
    #[serde(default)]
    pub topic_destination_mapping: HashMap<String, String>,
}

fn default_message_template() -> String {
    "{{body}}".to_string()
}

pub struct AzureServiceBusReflex {
    name: String,
    config: AzureServiceBusReflexConfig,
    client: Arc<reqwest::Client>,
}

impl AzureServiceBusReflex {
    pub fn new(name: String, config: AzureServiceBusReflexConfig, app_ctx: &AppContext) -> Self {
        Self {
            name,
            config,
            client: Arc::clone(app_ctx.http_client()),
        }
    }

    fn resolve_destination(&self, ink: &Ink) -> String {
        if let Some(dest) = self
            .config
            .topic_destination_mapping
            .get(&ink.topic)
            .filter(|d| !d.is_empty())
        {
            return dest.clone();
        }
        self.config.destination.clone()
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
}

#[async_trait]
impl Reflex for AzureServiceBusReflex {
    fn name(&self) -> &str {
        &self.name
    }
    fn r#type(&self) -> &str {
        "azure_servicebus"
    }

    async fn handle(&self, ink: &Ink) -> Result<()> {
        info!(
            "Azure Service Bus reflex '{}' handling ink on topic: {}",
            self.name, ink.topic
        );
        let destination = self.resolve_destination(ink);
        let body = self.render_template(&self.config.message_template, ink);
        debug!(
            "Azure Service Bus reflex '{}' sending to '{}' for topic: {}",
            self.name, destination, ink.topic
        );

        let url = format!(
            "https://{}.servicebus.windows.net/{}/messages?api-version=2014-09",
            self.config.namespace, destination
        );

        // Generate SAS token
        let expires = (chrono::Utc::now() + chrono::Duration::seconds(300)).timestamp();
        let string_to_sign = format!("{}\n{}", urlencoding::encode(&url), expires);
        use hmac::{Hmac, Mac};
        use sha2::Sha256;
        type HmacSha256 = Hmac<Sha256>;
        let mut mac = HmacSha256::new_from_slice(self.config.sas_key_value.as_bytes())
            .map_err(|e| anyhow::anyhow!("HMAC error: {}", e))?;
        mac.update(string_to_sign.as_bytes());
        let signature = mac.finalize().into_bytes();
        let encoded_sig =
            base64::Engine::encode(&base64::engine::general_purpose::STANDARD, signature);

        let sas_token = format!(
            "SharedAccessSignature sr={}&sig={}&se={}&skn={}",
            urlencoding::encode(&url),
            urlencoding::encode(&encoded_sig),
            expires,
            self.config.sas_key_name
        );

        let response = self
            .client
            .post(&url)
            .header("Authorization", &sas_token)
            .header("Content-Type", "application/json")
            .body(body)
            .send()
            .await
            .with_context(|| "Failed to send message to Azure Service Bus".to_string())?;

        if !response.status().is_success() {
            let status = response.status();
            let error_body = response.text().await.unwrap_or_default();
            return Err(anyhow::anyhow!(
                "Azure Service Bus API error ({}): {}",
                status,
                error_body
            ));
        }
        info!(
            "Azure Service Bus reflex '{}' sent message for topic: {}",
            self.name, ink.topic
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::appcontext::AppContext;

    fn create_test_config() -> AzureServiceBusReflexConfig {
        AzureServiceBusReflexConfig {
            namespace: "mynamespace".to_string(),
            sas_key_name: "RootManageSharedAccessKey".to_string(),
            sas_key_value: "test_key".to_string(),
            destination: "mytopic".to_string(),
            message_template: "{{body}}".to_string(),
            topic_destination_mapping: HashMap::new(),
        }
    }

    fn create_test_ink() -> Ink {
        Ink::new("/test/topic", "test", "test_source", "test body")
    }

    #[test]
    fn test_default_destination() {
        let config = create_test_config();
        let reflex = AzureServiceBusReflex::new("test".to_string(), config, &AppContext::new());
        let ink = create_test_ink();
        assert_eq!(reflex.resolve_destination(&ink), "mytopic");
    }

    #[test]
    fn test_template_rendering() {
        let config = create_test_config();
        let reflex = AzureServiceBusReflex::new("test".to_string(), config, &AppContext::new());
        let ink = create_test_ink();
        assert_eq!(
            reflex.render_template("[{{topic}}] {{body}}", &ink),
            "[/test/topic] test body"
        );
    }

    #[test]
    fn test_config_deserialization() {
        let yaml = r#"
            namespace: "mynamespace"
            sas_key_name: "key"
            sas_key_value: "value"
            destination: "myqueue"
        "#;
        let config: AzureServiceBusReflexConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.destination, "myqueue");
    }

    #[test]
    fn test_reflex_type() {
        let config = create_test_config();
        let reflex = AzureServiceBusReflex::new("test".to_string(), config, &AppContext::new());
        assert_eq!(reflex.r#type(), "azure_servicebus");
    }
}
