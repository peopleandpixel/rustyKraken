use anyhow::{Context, Result};
use async_trait::async_trait;
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;
use tracing::{debug, info};

use super::Reflex;
use crate::model::Ink;

/// Configuration for AWS SNS Reflex
/// Publishes notifications to Amazon Simple Notification Service
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AwsSnsReflexConfig {
    /// AWS Region (e.g., "us-east-1")
    pub region: String,

    /// AWS Access Key ID
    pub access_key_id: String,

    /// AWS Secret Access Key
    pub secret_access_key: String,

    /// SNS Topic ARN (e.g., "arn:aws:sns:us-east-1:123456789:my-topic")
    #[serde(default)]
    pub topic_arn: Option<String>,

    /// Template for the message body
    #[serde(default = "default_message_template")]
    pub message_template: String,

    /// Template for the subject/title
    #[serde(default = "default_subject_template")]
    pub subject_template: String,

    /// Optional: mapping from Ink topics to SNS Topic ARNs
    #[serde(default)]
    pub topic_arn_mapping: HashMap<String, String>,

    /// Message attributes (key-value pairs passed to SNS)
    #[serde(default)]
    pub message_attributes: HashMap<String, String>,

    /// Whether to use HTTPS (default: true)
    #[serde(default = "default_use_https")]
    pub use_https: bool,

    /// Custom User-Agent suffix
    #[serde(default)]
    pub user_agent_suffix: Option<String>,
}

fn default_message_template() -> String { "{{body}}".to_string() }
fn default_subject_template() -> String { "{{topic}}".to_string() }
fn default_use_https() -> bool { true }

pub struct AwsSnsReflex {
    name: String,
    config: AwsSnsReflexConfig,
    client: reqwest::Client,
}

impl AwsSnsReflex {
    pub fn new(name: String, config: AwsSnsReflexConfig) -> Self {
        Self { name, config, client: reqwest::Client::new() }
    }

    /// Resolve SNS Topic ARN for a given Ink
    fn resolve_topic_arn(&self, ink: &Ink) -> Result<String> {
        if let Some(arn) = self
            .config
            .topic_arn_mapping
            .get(&ink.topic)
            .filter(|a| !a.is_empty())
        {
            return Ok(arn.clone());
        }
        self.config
            .topic_arn
            .clone()
            .context("SNS Topic ARN is required - set topic_arn or topic_arn_mapping")
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

    /// Build SNS endpoint URL for the region
    fn sns_endpoint(&self) -> String {
        let scheme = if self.config.use_https { "https" } else { "http" };
        format!(
            "{}://sns.{}.amazonaws.com",
            scheme, self.config.region
        )
    }

    /// Create message attributes for SNS
    fn build_message_attributes(&self, ink: &Ink) -> serde_json::Value {
        let mut attrs = serde_json::Map::new();
        for (key, value) in &self.config.message_attributes {
            attrs.insert(
                key.clone(),
                serde_json::json!({
                    "DataType": "String",
                    "StringValue": value
                }),
            );
        }
        // Add topic as attribute
        attrs.insert(
            "kraken_topic".to_string(),
            serde_json::json!({
                "DataType": "String",
                "StringValue": ink.topic.clone()
            }),
        );
        serde_json::Value::Object(attrs)
    }

    /// AWS Signature v4 signing
    fn sign_request(
        &self,
        method: &str,
        body: &str,
        timestamp: &str,
        date_stamp: &str,
    ) -> (String, String) {
        let region = &self.config.region;
        let service = "sns";

        let canonical_uri = "/";
        let canonical_querystring = "";
        let canonical_headers = format!(
            "content-type:application/x-www-form-urlencoded\nhost:sns.{}.amazonaws.com\nx-amz-date:{}\nx-amz-target:AmazonSNS.Publish\n",
            self.config.region, timestamp
        );
        let signed_headers = "content-type;host;x-amz-date;x-amz-target";

        let payload_hash = hex::encode(Sha256::digest(body.as_bytes()));

        let canonical_request = format!(
            "{}\n{}\n{}\n{}\n{}\n{}",
            method, canonical_uri, canonical_querystring, canonical_headers, signed_headers, payload_hash
        );

        let credential_scope = format!("{}/{}/{}/aws4_request", date_stamp, region, service);
        let string_to_sign = format!(
            "AWS4-HMAC-SHA256\n{}\n{}\n{}",
            timestamp,
            credential_scope,
            hex::encode(Sha256::digest(canonical_request.as_bytes()))
        );

        let signing_key = {
            let key = format!("AWS4{}", self.config.secret_access_key).into_bytes();
            let date_sig = {
                let mut m: Hmac<Sha256> = Hmac::new_from_slice(&key).expect("HMAC key ok");
                m.update(date_stamp.as_bytes());
                m.finalize().into_bytes().to_vec()
            };
            let region_sig = {
                let mut m: Hmac<Sha256> = Hmac::new_from_slice(&date_sig).expect("HMAC key ok");
                m.update(region.as_bytes());
                m.finalize().into_bytes().to_vec()
            };
            let service_sig = {
                let mut m: Hmac<Sha256> = Hmac::new_from_slice(&region_sig).expect("HMAC key ok");
                m.update(service.as_bytes());
                m.finalize().into_bytes().to_vec()
            };
            let mut m: Hmac<Sha256> = Hmac::new_from_slice(&service_sig).expect("HMAC key ok");
            m.update(b"aws4_request");
            m.finalize().into_bytes().to_vec()
        };

        let mut m: Hmac<Sha256> = Hmac::new_from_slice(&signing_key).expect("HMAC key ok");
        m.update(string_to_sign.as_bytes());
        let signature = hex::encode(m.finalize().into_bytes());

        let authorization = format!(
            "AWS4-HMAC-SHA256 Credential={}/{}, SignedHeaders={}, Signature={}",
            self.config.access_key_id, credential_scope, signed_headers, signature
        );

        (authorization, timestamp.to_string())
    }
}

#[async_trait]
impl Reflex for AwsSnsReflex {
    fn name(&self) -> &str { &self.name }
    fn r#type(&self) -> &str { "aws_sns" }

    async fn handle(&self, ink: &Ink) -> Result<()> {
        info!("AWS SNS reflex '{}' handling ink on topic: {}", self.name, ink.topic);

        let topic_arn = self.resolve_topic_arn(ink)?;
        let message = self.render_template(&self.config.message_template, ink);
        let subject = self.render_template(&self.config.subject_template, ink);
        let message_attributes = self.build_message_attributes(ink);

        debug!(
            "AWS SNS reflex '{}' publishing to topic {} with message: {}",
            self.name, topic_arn, message
        );

        // Build SNS Publish action payload (JSON for x-amz-target)
        let payload = serde_json::json!({
            "TopicArn": topic_arn,
            "Message": message,
            "Subject": subject,
            "MessageAttributes": message_attributes
        });
        let body = format!("Action=Publish&Version=2010-03-31&{}", urlencoding::encode(&payload.to_string()));

        // Generate timestamp
        let now = ink.timestamp;
        let timestamp = now.format("%Y%m%dT%H%M%SZ").to_string();
        let date_stamp = now.format("%Y%m%d").to_string();

        let (authorization, _ts) = self.sign_request("POST", &body, &timestamp, &date_stamp);
        let endpoint = self.sns_endpoint();
        let host = format!("sns.{}.amazonaws.com", self.config.region);

        let response = self
            .client
            .post(&endpoint)
            .header("Content-Type", "application/x-www-form-urlencoded")
            .header("Host", &host)
            .header("X-Amz-Date", &timestamp)
            .header("X-Amz-Target", "AmazonSNS.Publish")
            .header("Authorization", &authorization)
            .body(body)
            .send()
            .await
            .with_context(|| format!("Failed to send AWS SNS request to {}", endpoint))?;

        if !response.status().is_success() {
            let status = response.status();
            let error_body = response.text().await.unwrap_or_default();
            return Err(anyhow::anyhow!("AWS SNS API error ({}): {}", status, error_body));
        }

        // Parse response to get MessageId
        let resp_text = response.text().await.unwrap_or_default();
        if let Some(start) = resp_text.find("<MessageId>")
            && let Some(msg_id) = resp_text[start..].find('>') {
                let end = start + msg_id + 1;
                if let Some(end_tag) = resp_text[end..].find("</MessageId>") {
                    let id = &resp_text[end..end + end_tag];
                    info!("AWS SNS reflex '{}' published message with ID: {}", self.name, id);
                }
            }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_config() -> AwsSnsReflexConfig {
        AwsSnsReflexConfig {
            region: "us-east-1".to_string(),
            access_key_id: "AKIAIOSFODNN7EXAMPLE".to_string(),
            secret_access_key: "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY".to_string(),
            topic_arn: Some("arn:aws:sns:us-east-1:123456789:my-topic".to_string()),
            message_template: "{{body}}".to_string(),
            subject_template: "{{topic}}".to_string(),
            topic_arn_mapping: HashMap::new(),
            message_attributes: HashMap::new(),
            use_https: true,
            user_agent_suffix: None,
        }
    }

    fn create_test_ink() -> Ink {
        Ink::new("/test/topic", "test", "test_source", "test body")
    }

    #[test]
    fn test_template_rendering() {
        let config = create_test_config();
        let reflex = AwsSnsReflex::new("test".to_string(), config);
        let ink = create_test_ink();
        assert_eq!(reflex.render_template("[{{topic}}] {{body}}", &ink), "[/test/topic] test body");
    }

    #[test]
    fn test_resolve_topic_arn() {
        let config = create_test_config();
        let reflex = AwsSnsReflex::new("test".to_string(), config);
        let ink = create_test_ink();
        assert_eq!(
            reflex.resolve_topic_arn(&ink).unwrap(),
            "arn:aws:sns:us-east-1:123456789:my-topic"
        );
    }

    #[test]
    fn test_resolve_topic_arn_mapping() {
        let mut config = create_test_config();
        config.topic_arn_mapping.insert(
            "/test/topic".to_string(),
            "arn:aws:sns:us-east-1:123456789:mapped-topic".to_string(),
        );
        let reflex = AwsSnsReflex::new("test".to_string(), config);
        let ink = create_test_ink();
        assert_eq!(
            reflex.resolve_topic_arn(&ink).unwrap(),
            "arn:aws:sns:us-east-1:123456789:mapped-topic"
        );
    }

    #[test]
    fn test_sns_endpoint() {
        let config = create_test_config();
        let reflex = AwsSnsReflex::new("test".to_string(), config);
        assert_eq!(reflex.sns_endpoint(), "https://sns.us-east-1.amazonaws.com");
    }

    #[test]
    fn test_config_deserialization() {
        let yaml = r#"
            region: "eu-west-1"
            access_key_id: "AKIAIOSFODNN7EXAMPLE"
            secret_access_key: "secret"
            topic_arn: "arn:aws:sns:eu-west-1:123:topic"
            message_template: "[{{topic}}] {{body}}"
        "#;
        let config: AwsSnsReflexConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.region, "eu-west-1");
    }

    #[test]
    fn test_reflex_type() {
        let config = create_test_config();
        let reflex = AwsSnsReflex::new("test".to_string(), config);
        assert_eq!(reflex.r#type(), "aws_sns");
    }
}
