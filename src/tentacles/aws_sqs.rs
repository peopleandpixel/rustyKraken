use anyhow::{Context, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info, warn};

use super::Tentacle;
use crate::eventbus::EventBus;
use crate::model::Ink;

/// Konfiguration für AWS SQS Tentacle
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AwsSqsTentacleConfig {
    /// AWS Region (z.B. "eu-central-1")
    pub region: String,
    /// Queue URL oder Queue Name
    pub queue_url: String,
    /// AWS Access Key ID (optional, falls nicht gesetzt wird die Umgebungsvariable verwendet)
    #[serde(default)]
    pub access_key_id: Option<String>,
    /// AWS Secret Access Key (optional, falls nicht gesetzt wird die Umgebungsvariable verwendet)
    #[serde(default)]
    pub secret_access_key: Option<String>,
    /// Session Polling Intervall in Sekunden (Standard: 5)
    #[serde(default = "default_poll_interval")]
    pub poll_interval: u64,
    /// Maximale Anzahl von Nachrichten pro Poll (Standard: 10)
    #[serde(default = "default_max_messages")]
    pub max_messages: i32,
    /// Visibility Timeout in Sekunden (Standard: 30)
    #[serde(default = "default_visibility_timeout")]
    pub visibility_timeout: i32,
    /// Nachrichten nach erfolgreichem Processing löschen (Standard: true)
    #[serde(default = "default_delete_after_processing")]
    pub delete_after_processing: bool,
    /// Fallback Topic wenn kein Topic aus Attributen erkannt wird
    #[serde(default = "default_fallback_topic")]
    pub fallback_topic: String,
    /// Topic-Modus: "attribute" (aus Message-Attributen), "body" (aus JSON-Body), "all" (kombiniert)
    #[serde(default = "default_topic_mode")]
    pub topic_mode: String,
    /// Optional: Mapping von Queue-URLs auf Topics
    #[serde(default)]
    pub queue_topic_mapping: HashMap<String, String>,
    /// Optional: AWS Endpoint URL (für LocalStack o.ä.)
    #[serde(default)]
    pub endpoint_url: Option<String>,
}

fn default_poll_interval() -> u64 {
    5
}

fn default_max_messages() -> i32 {
    10
}

fn default_visibility_timeout() -> i32 {
    30
}

fn default_delete_after_processing() -> bool {
    true
}

fn default_fallback_topic() -> String {
    "/sqs/default".to_string()
}

fn default_topic_mode() -> String {
    "all".to_string()
}

pub struct AwsSqsTentacle {
    name: String,
    config: AwsSqsTentacleConfig,
    client: reqwest::Client,
}

/// SQS Message Struktur
#[derive(Debug, Deserialize)]
struct SqsMessage {
    message_id: String,
    receipt_handle: String,
    body: String,
    #[serde(default)]
    message_attributes: HashMap<String, SqsMessageAttribute>,
}

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
struct SqsMessageAttribute {
    #[serde(rename = "StringValue")]
    string_value: Option<String>,
    #[serde(rename = "DataType")]
    #[allow(dead_code)]
    data_type: String,
}

impl AwsSqsTentacle {
    pub fn new(name: String, config: AwsSqsTentacleConfig) -> Self {
        Self {
            name,
            config,
            client: reqwest::Client::new(),
        }
    }

    /// Bestimmt das Topic basierend auf Message-Attributen oder Body
    fn resolve_topic(&self, message: &SqsMessage) -> String {
        match self.config.topic_mode.as_str() {
            "attribute" => self.topic_from_attributes(message).unwrap_or_else(|| {
                self.config
                    .queue_topic_mapping
                    .get(&self.config.queue_url)
                    .cloned()
                    .unwrap_or_else(|| self.config.fallback_topic.clone())
            }),
            "body" => self.topic_from_body(message).unwrap_or_else(|| {
                self.config
                    .queue_topic_mapping
                    .get(&self.config.queue_url)
                    .cloned()
                    .unwrap_or_else(|| self.config.fallback_topic.clone())
            }),
            "all" => {
                // Attribute haben Vorrang, dann Body, dann Queue-Mapping, dann Fallback
                if let Some(topic) = self.topic_from_attributes(message) {
                    return topic;
                }
                if let Some(topic) = self.topic_from_body(message) {
                    return topic;
                }
                // Queue-Mapping prüfen
                if let Some(topic) = self.config.queue_topic_mapping.get(&self.config.queue_url) {
                    return topic.clone();
                }
                self.config.fallback_topic.clone()
            }
            _ => self.config.fallback_topic.clone(),
        }
    }

    /// Extrahiert Topic aus Message-Attributen
    fn topic_from_attributes(&self, message: &SqsMessage) -> Option<String> {
        // "topic" Attribut hat Vorrang
        message
            .message_attributes
            .get("topic")
            .and_then(|attr| attr.string_value.as_ref())
            .filter(|v| !v.is_empty())
            .cloned()
            .or_else(|| {
                // "Subject" Attribut als Fallback
                message
                    .message_attributes
                    .get("Subject")
                    .and_then(|attr| attr.string_value.as_ref())
                    .filter(|v| !v.is_empty())
                    .cloned()
            })
    }

    /// Extrahiert Topic aus dem JSON-Body
    fn topic_from_body(&self, message: &SqsMessage) -> Option<String> {
        // Versuche, den Body als JSON zu parsen
        if let Ok(body_json) = serde_json::from_str::<serde_json::Value>(&message.body) {
            // "topic" Feld im Body
            if let Some(topic) = body_json.get("topic").and_then(|t| t.as_str())
                && !topic.is_empty()
            {
                return Some(topic.to_string());
            }
            // "subject" Feld als Fallback
            if let Some(subject) = body_json.get("subject").and_then(|s| s.as_str())
                && !subject.is_empty()
            {
                return Some(subject.to_string());
            }
        }
        // Wenn Body mit "/" beginnt, ist er selbst das Topic
        if message.body.starts_with('/') && !message.body.is_empty() {
            return Some(message.body.clone());
        }
        None
    }

    /// Erstellt ein Ink aus einer SQS Message
    fn create_ink(&self, message: &SqsMessage) -> Ink {
        let topic = self.resolve_topic(message);

        // Metadata erstellen
        let mut metadata_map = serde_json::Map::new();
        metadata_map.insert(
            "message_id".to_string(),
            serde_json::Value::String(message.message_id.clone()),
        );
        metadata_map.insert(
            "queue_url".to_string(),
            serde_json::Value::String(self.config.queue_url.clone()),
        );
        metadata_map.insert(
            "region".to_string(),
            serde_json::Value::String(self.config.region.clone()),
        );

        // Message attributes zur Metadata hinzufügen
        if !message.message_attributes.is_empty() {
            let attrs_map: serde_json::Map<String, serde_json::Value> = message
                .message_attributes
                .iter()
                .map(|(k, v)| {
                    (
                        k.clone(),
                        serde_json::Value::String(v.string_value.clone().unwrap_or_default()),
                    )
                })
                .collect();
            metadata_map.insert(
                "message_attributes".to_string(),
                serde_json::Value::Object(attrs_map),
            );
        }

        let metadata = serde_json::Value::Object(metadata_map);

        Ink::new(topic, "sqs", &self.name, message.body.clone()).with_metadata(metadata)
    }

    /// AWS Signature v4 für SQS Requests
    #[allow(clippy::too_many_arguments)]
    fn sign_request(
        &self,
        url: &str,
        method: &str,
        body: &str,
        region: &str,
        access_key: &str,
        secret_key: &str,
        timestamp: &chrono::DateTime<chrono::Utc>,
    ) -> Result<reqwest::RequestBuilder> {
        let date_stamp = timestamp.format("%Y%m%d").to_string();
        let amz_date = timestamp.format("%Y%m%dT%H%M%SZ").to_string();

        // Canonical Request
        let canonical_headers = format!(
            "content-type:application/x-www-form-urlencoded\nhost:{}\nx-amz-date:{}\n",
            url.parse::<reqwest::Url>()?.host_str().unwrap_or(""),
            amz_date
        );
        let signed_headers = "content-type;host;x-amz-date";

        let body_hash = sha256(body);
        let canonical_request = format!(
            "{}\n{}\n\n{}\n{}\n{}",
            method,
            url.parse::<reqwest::Url>()?.path(),
            canonical_headers,
            signed_headers,
            body_hash
        );

        // String to Sign
        let credential_scope = format!("{}/{}/sqs/aws4_request", date_stamp, region);
        let string_to_sign = format!(
            "AWS4-HMAC-SHA256\n{}\n{}\n{}",
            amz_date,
            credential_scope,
            sha256(&canonical_request)
        );

        // Signing Key
        let signing_key = self.get_signing_key(secret_key, &date_stamp, region, "sqs");
        let signature = hmac_sha256(&signing_key, &string_to_sign);

        let authorization = format!(
            "AWS4-HMAC-SHA256 Credential={}/{}, Signed={}, Signature={}",
            access_key, credential_scope, signed_headers, signature
        );

        Ok(self
            .client
            .request(method.parse().unwrap(), url)
            .header("Content-Type", "application/x-www-form-urlencoded")
            .header("X-Amz-Date", amz_date)
            .header("Authorization", authorization))
    }

    fn get_signing_key(
        &self,
        secret_key: &str,
        date_stamp: &str,
        region: &str,
        service: &str,
    ) -> Vec<u8> {
        let k_date = hmac_sha256_bytes(secret_key.as_bytes(), date_stamp);
        let k_region = hmac_sha256_bytes(&k_date, region);
        let k_service = hmac_sha256_bytes(&k_region, service);
        hmac_sha256_bytes(&k_service, "aws4_request")
    }

    /// Holt die AWS Credentials
    fn get_credentials(&self) -> Result<(String, String)> {
        let access_key = self
            .config
            .access_key_id
            .clone()
            .or_else(|| std::env::var("AWS_ACCESS_KEY_ID").ok())
            .context("AWS Access Key ID not configured")?;

        let secret_key = self
            .config
            .secret_access_key
            .clone()
            .or_else(|| std::env::var("AWS_SECRET_ACCESS_KEY").ok())
            .context("AWS Secret Access Key not configured")?;

        Ok((access_key, secret_key))
    }

    /// Empfängt Nachrichten von der SQS Queue
    async fn receive_messages(&self, queue_url: &str) -> Result<Vec<SqsMessage>> {
        let (access_key, secret_key) = self.get_credentials()?;
        let timestamp = chrono::Utc::now();

        let base_url = self.config.endpoint_url.as_deref().unwrap_or(queue_url);
        let url = if base_url.contains("queue/") {
            base_url.to_string()
        } else {
            queue_url.to_string()
        };

        let body = format!(
            "Action=ReceiveMessage&MaxNumberOfMessages={}&VisibilityTimeout={}&Version=2012-11-05",
            self.config.max_messages, self.config.visibility_timeout
        );

        let request = self.sign_request(
            &url,
            "POST",
            &body,
            &self.config.region,
            &access_key,
            &secret_key,
            &timestamp,
        )?;

        let response = request.body(body).send().await?;

        if !response.status().is_success() {
            let status = response.status();
            let error_body = response.text().await.unwrap_or_default();
            return Err(anyhow::anyhow!(
                "SQS ReceiveMessage failed with status {}: {}",
                status,
                error_body
            ));
        }

        let body_text = response.text().await?;
        // XML parsen - einfache Implementierung
        let messages = self.parse_sqs_response(&body_text)?;
        Ok(messages)
    }

    /// Einfacher XML-Parser für SQS ReceiveMessage Response
    fn parse_sqs_response(&self, xml: &str) -> Result<Vec<SqsMessage>> {
        let mut messages = Vec::new();

        // Finde alle <Message>...</Message> Blöcke
        let message_start = "<Message>";
        let message_end = "</Message>";

        let mut start = 0;
        while let Some(pos) = xml[start..].find(message_start) {
            let abs_start = start + pos + message_start.len();
            if let Some(end_pos) = xml[abs_start..].find(message_end) {
                let block = &xml[abs_start..abs_start + end_pos];

                let message_id = extract_xml_tag(block, "MessageId").unwrap_or_default();
                let receipt_handle = extract_xml_tag(block, "ReceiptHandle").unwrap_or_default();
                let body = extract_xml_tag(block, "Body").unwrap_or_default();

                if !message_id.is_empty() {
                    let message = SqsMessage {
                        message_id,
                        receipt_handle,
                        body,
                        message_attributes: HashMap::new(),
                    };
                    messages.push(message);
                }

                start = abs_start + end_pos + message_end.len();
            } else {
                break;
            }
        }

        Ok(messages)
    }

    /// Löscht eine Nachricht aus der Queue
    async fn delete_message(&self, queue_url: &str, receipt_handle: &str) -> Result<()> {
        let (access_key, secret_key) = self.get_credentials()?;
        let timestamp = chrono::Utc::now();

        let encoded_handle = urlencoding::encode(receipt_handle);
        let body = format!(
            "Action=DeleteMessage&ReceiptHandle={}&Version=2012-11-05",
            encoded_handle
        );

        let request = self.sign_request(
            queue_url,
            "POST",
            &body,
            &self.config.region,
            &access_key,
            &secret_key,
            &timestamp,
        )?;

        let response = request.body(body).send().await?;

        if !response.status().is_success() {
            warn!(
                "SQS tentacle '{}' failed to delete message: {}",
                self.name,
                response.status()
            );
        }

        Ok(())
    }
}

fn extract_xml_tag(xml: &str, tag: &str) -> Option<String> {
    let start_tag = format!("<{}>", tag);
    let end_tag = format!("</{}>", tag);

    if let Some(start) = xml.find(&start_tag) {
        let abs_start = start + start_tag.len();
        if let Some(end) = xml[abs_start..].find(&end_tag) {
            let value = &xml[abs_start..abs_start + end];
            // URL-dekodieren
            let decoded = urlencoding::decode(value).unwrap_or_default();
            return Some(decoded.into_owned());
        }
    }
    None
}

fn sha256(input: &str) -> String {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(input.as_bytes());
    let result = hasher.finalize();
    format!("{:x}", result)
}

fn hmac_sha256(key: &[u8], message: &str) -> String {
    use hmac::{Hmac, Mac};
    type HmacSha256 = Hmac<sha2::Sha256>;
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC can take key of any size");
    mac.update(message.as_bytes());
    let result = mac.finalize();
    format!("{:x}", result.into_bytes())
}

fn hmac_sha256_bytes(key: &[u8], message: &str) -> Vec<u8> {
    use hmac::{Hmac, Mac};
    type HmacSha256 = Hmac<sha2::Sha256>;
    let mut mac = HmacSha256::new_from_slice(key).expect("HMAC can take key of any size");
    mac.update(message.as_bytes());
    mac.finalize().into_bytes().to_vec()
}

#[async_trait]
impl Tentacle for AwsSqsTentacle {
    fn name(&self) -> &str {
        &self.name
    }

    fn r#type(&self) -> &str {
        "aws_sqs"
    }

    async fn start(&self, event_bus: Arc<EventBus>) -> Result<()> {
        info!(
            "AWS SQS tentacle '{}' polling queue: {}",
            self.name, self.config.queue_url
        );

        loop {
            match self.receive_messages(&self.config.queue_url).await {
                Ok(messages) => {
                    for message in &messages {
                        debug!(
                            "SQS tentacle '{}' received message: {}",
                            self.name, message.message_id
                        );

                        let ink = self.create_ink(message);
                        debug!(
                            "SQS tentacle '{}' publishing message on topic: {}",
                            self.name, ink.topic
                        );
                        event_bus.publish(ink);

                        if self.config.delete_after_processing
                            && let Err(e) = self
                                .delete_message(&self.config.queue_url, &message.receipt_handle)
                                .await
                        {
                            error!(
                                "SQS tentacle '{}' failed to delete message: {}",
                                self.name, e
                            );
                        }
                    }
                }
                Err(e) => {
                    error!(
                        "SQS tentacle '{}' error receiving messages: {}",
                        self.name, e
                    );
                }
            }

            tokio::time::sleep(Duration::from_secs(self.config.poll_interval)).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_tentacle() -> AwsSqsTentacle {
        AwsSqsTentacle::new(
            "test_sqs".to_string(),
            AwsSqsTentacleConfig {
                region: "eu-central-1".to_string(),
                queue_url: "https://sqs.eu-central-1.amazonaws.com/123456789/my-queue".to_string(),
                access_key_id: Some("test_key".to_string()),
                secret_access_key: Some("test_secret".to_string()),
                poll_interval: 5,
                max_messages: 10,
                visibility_timeout: 30,
                delete_after_processing: true,
                fallback_topic: "/sqs/default".to_string(),
                topic_mode: "all".to_string(),
                queue_topic_mapping: HashMap::new(),
                endpoint_url: None,
            },
        )
    }

    #[test]
    fn test_tentacle_name_and_type() {
        let tentacle = create_tentacle();
        assert_eq!(tentacle.name(), "test_sqs");
        assert_eq!(tentacle.r#type(), "aws_sqs");
    }

    #[test]
    fn test_resolve_topic_from_attribute() {
        let mut config = AwsSqsTentacleConfig {
            region: "eu-central-1".to_string(),
            queue_url: "https://sqs.eu-central-1.amazonaws.com/123456789/my-queue".to_string(),
            access_key_id: Some("test_key".to_string()),
            secret_access_key: Some("test_secret".to_string()),
            poll_interval: 5,
            max_messages: 10,
            visibility_timeout: 30,
            delete_after_processing: true,
            fallback_topic: "/sqs/default".to_string(),
            topic_mode: "attribute".to_string(),
            queue_topic_mapping: HashMap::new(),
            endpoint_url: None,
        };

        let mut attrs = HashMap::new();
        attrs.insert(
            "topic".to_string(),
            SqsMessageAttribute {
                string_value: Some("/sqs/alerts".to_string()),
                data_type: "String".to_string(),
            },
        );

        let message = SqsMessage {
            message_id: "msg-1".to_string(),
            receipt_handle: "handle-1".to_string(),
            body: "test body".to_string(),
            message_attributes: attrs,
        };

        let tentacle = AwsSqsTentacle::new("test".to_string(), config.clone());
        assert_eq!(tentacle.resolve_topic(&message), "/sqs/alerts");

        // Subject Fallback
        config.topic_mode = "attribute".to_string();
        let mut attrs2 = HashMap::new();
        attrs2.insert(
            "Subject".to_string(),
            SqsMessageAttribute {
                string_value: Some("/sqs/subject-topic".to_string()),
                data_type: "String".to_string(),
            },
        );

        let message2 = SqsMessage {
            message_id: "msg-2".to_string(),
            receipt_handle: "handle-2".to_string(),
            body: "test body".to_string(),
            message_attributes: attrs2,
        };

        assert_eq!(tentacle.resolve_topic(&message2), "/sqs/subject-topic");
    }

    #[test]
    fn test_resolve_topic_from_body() {
        let config = AwsSqsTentacleConfig {
            region: "eu-central-1".to_string(),
            queue_url: "https://sqs.eu-central-1.amazonaws.com/123456789/my-queue".to_string(),
            access_key_id: Some("test_key".to_string()),
            secret_access_key: Some("test_secret".to_string()),
            poll_interval: 5,
            max_messages: 10,
            visibility_timeout: 30,
            delete_after_processing: true,
            fallback_topic: "/sqs/default".to_string(),
            topic_mode: "body".to_string(),
            queue_topic_mapping: HashMap::new(),
            endpoint_url: None,
        };

        let tentacle = AwsSqsTentacle::new("test".to_string(), config);

        // JSON Body mit topic Feld
        let message = SqsMessage {
            message_id: "msg-1".to_string(),
            receipt_handle: "handle-1".to_string(),
            body: r#"{"topic": "/sqs/json-topic", "data": "value"}"#.to_string(),
            message_attributes: HashMap::new(),
        };

        assert_eq!(tentacle.resolve_topic(&message), "/sqs/json-topic");

        // Body beginnt mit /
        let message2 = SqsMessage {
            message_id: "msg-2".to_string(),
            receipt_handle: "handle-2".to_string(),
            body: "/sqs/plain-topic".to_string(),
            message_attributes: HashMap::new(),
        };

        assert_eq!(tentacle.resolve_topic(&message2), "/sqs/plain-topic");
    }

    #[test]
    fn test_resolve_topic_all_mode() {
        let mut mapping = HashMap::new();
        mapping.insert(
            "https://sqs.eu-central-1.amazonaws.com/123456789/my-queue".to_string(),
            "/sqs/queue-mapped".to_string(),
        );

        let config = AwsSqsTentacleConfig {
            region: "eu-central-1".to_string(),
            queue_url: "https://sqs.eu-central-1.amazonaws.com/123456789/my-queue".to_string(),
            access_key_id: Some("test_key".to_string()),
            secret_access_key: Some("test_secret".to_string()),
            poll_interval: 5,
            max_messages: 10,
            visibility_timeout: 30,
            delete_after_processing: true,
            fallback_topic: "/sqs/default".to_string(),
            topic_mode: "all".to_string(),
            queue_topic_mapping: mapping,
            endpoint_url: None,
        };

        let tentacle = AwsSqsTentacle::new("test".to_string(), config.clone());

        // Kein Attribut, kein Body Topic -> Queue Mapping
        let message = SqsMessage {
            message_id: "msg-1".to_string(),
            receipt_handle: "handle-1".to_string(),
            body: "no topic here".to_string(),
            message_attributes: HashMap::new(),
        };

        assert_eq!(tentacle.resolve_topic(&message), "/sqs/queue-mapped");

        // Fallback wenn kein Mapping
        let mut config2 = config.clone();
        config2.queue_topic_mapping = HashMap::new();
        let tentacle2 = AwsSqsTentacle::new("test2".to_string(), config2);
        assert_eq!(tentacle2.resolve_topic(&message), "/sqs/default");
    }

    #[test]
    fn test_config_defaults() {
        let config: AwsSqsTentacleConfig = serde_yaml::from_str(
            r#"
            region: "us-east-1"
            queue_url: "https://sqs.us-east-1.amazonaws.com/123456789/test"
        "#,
        )
        .unwrap();

        assert_eq!(config.poll_interval, 5);
        assert_eq!(config.max_messages, 10);
        assert_eq!(config.visibility_timeout, 30);
        assert!(config.delete_after_processing);
        assert_eq!(config.fallback_topic, "/sqs/default");
        assert_eq!(config.topic_mode, "all");
    }

    #[test]
    fn test_extract_xml_tag() {
        let xml = r#"<MessageId>msg-123</MessageId><Body>test body</Body>"#;
        assert_eq!(
            extract_xml_tag(xml, "MessageId"),
            Some("msg-123".to_string())
        );
        assert_eq!(extract_xml_tag(xml, "Body"), Some("test body".to_string()));
        assert_eq!(extract_xml_tag(xml, "Missing"), None);
    }

    #[test]
    fn test_parse_sqs_response() {
        let tentacle = create_tentacle();
        let xml = r#"
<ReceiveMessageResponse>
    <ReceiveMessageResult>
        <Message>
            <MessageId>msg-1</MessageId>
            <ReceiptHandle>handle-1</ReceiptHandle>
            <Body>{"data": "value"}</Body>
        </Message>
        <Message>
            <MessageId>msg-2</MessageId>
            <ReceiptHandle>handle-2</ReceiptHandle>
            <Body>plain text</Body>
        </Message>
    </ReceiveMessageResult>
</ReceiveMessageResponse>
        "#;

        let messages = tentacle.parse_sqs_response(xml).unwrap();
        assert_eq!(messages.len(), 2);
        assert_eq!(messages[0].message_id, "msg-1");
        assert_eq!(messages[1].message_id, "msg-2");
    }
}
