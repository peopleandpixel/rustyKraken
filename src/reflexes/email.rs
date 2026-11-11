use anyhow::{Context, Result};
use async_trait::async_trait;
use lettre::message::header::{ContentDisposition, ContentType};
use lettre::message::{Mailbox, MultiPart, SinglePart};
use lettre::transport::smtp::authentication::Credentials;
use lettre::transport::smtp::client::{Tls, TlsParameters};
use lettre::{AsyncSmtpTransport, AsyncTransport, Message, Tokio1Executor};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;
use tracing::{debug, info, warn};

use super::Reflex;
use crate::model::Ink;

/// Configuration for E-Mail (SMTP) Reflex
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EmailReflexConfig {
    /// SMTP server hostname (e.g., "smtp.gmail.com")
    pub smtp_host: String,

    /// SMTP server port (e.g., 587 for STARTTLS, 465 for implicit TLS)
    #[serde(default = "default_smtp_port")]
    pub smtp_port: u16,

    /// SMTP authentication username (usually the sender email)
    pub smtp_user: String,

    /// SMTP authentication password
    pub smtp_password: String,

    /// Sender email address (e.g., "alerts@mydomain.com")
    pub from_email: String,

    /// Sender display name (optional)
    #[serde(default)]
    pub from_name: String,

    /// Default recipient email address(es)
    pub to_emails: Vec<String>,

    /// Default email subject
    #[serde(default = "default_subject")]
    pub subject: String,

    /// Whether to send HTML body (false = plain text)
    #[serde(default)]
    pub html_body: bool,

    /// Template for the email body with placeholders
    /// Supported placeholders: {{topic}}, {{body}}, {{id}}, {{timestamp}}, {{metadata.<key>}}
    #[serde(default = "default_body_template")]
    pub body_template: String,

    /// Template for the email subject with placeholders
    #[serde(default = "default_subject_template")]
    pub subject_template: String,

    /// Optional: mapping from topics to recipient email addresses
    #[serde(default)]
    pub topic_recipient_mapping: HashMap<String, Vec<String>>,

    /// Optional: mapping from topics to subject templates
    #[serde(default)]
    pub topic_subject_mapping: HashMap<String, String>,

    /// Optional: CC recipient email addresses
    #[serde(default)]
    pub cc_emails: Vec<String>,

    /// Optional: BCC recipient email addresses
    #[serde(default)]
    pub bcc_emails: Vec<String>,

    /// Optional: file paths to attach to the email
    #[serde(default)]
    pub attachments: Vec<String>,

    /// Whether to use implicit TLS (port 465) instead of STARTTLS (port 587)
    #[serde(default)]
    pub implicit_tls: bool,
}

fn default_smtp_port() -> u16 {
    587
}

fn default_subject() -> String {
    "rustyKraken Notification".to_string()
}

fn default_body_template() -> String {
    "{{body}}".to_string()
}

fn default_subject_template() -> String {
    "{{topic}}: {{body}}".to_string()
}

pub struct EmailReflex {
    _name: String,
    config: EmailReflexConfig,
    mailer: AsyncSmtpTransport<Tokio1Executor>,
}

impl EmailReflex {
    pub fn new(name: String, config: EmailReflexConfig) -> Result<Self> {
        let creds = Credentials::new(config.smtp_user.clone(), config.smtp_password.clone());

        let tls_params = TlsParameters::builder(config.smtp_host.clone())
            .build()
            .with_context(|| format!("Failed to create TLS parameters for {}", config.smtp_host))?;

        let mailer: AsyncSmtpTransport<Tokio1Executor> = if config.implicit_tls {
            // Implicit TLS (typically port 465)
            AsyncSmtpTransport::<Tokio1Executor>::builder_dangerous(&config.smtp_host)
                .credentials(creds)
                .tls(Tls::Wrapper(tls_params))
                .port(config.smtp_port)
                .build()
        } else {
            // STARTTLS (typically port 587) or plain
            let port = config.smtp_port;
            AsyncSmtpTransport::<Tokio1Executor>::builder_dangerous(&config.smtp_host)
                .credentials(creds)
                .port(port)
                .tls(if port == 465 {
                    Tls::Wrapper(tls_params)
                } else {
                    Tls::Opportunistic(tls_params)
                })
                .build()
        };

        Ok(Self {
            _name: name,
            config,
            mailer,
        })
    }

    /// Resolve recipient emails for a given Ink, checking topic-based mapping first
    fn resolve_recipients(&self, ink: &Ink) -> Vec<String> {
        if let Some(recipients) = self
            .config
            .topic_recipient_mapping
            .get(&ink.topic)
            .filter(|r| !r.is_empty())
        {
            return recipients.clone();
        }
        self.config.to_emails.clone()
    }

    /// Resolve subject for a given Ink, checking topic-based mapping first
    fn resolve_subject(&self, ink: &Ink) -> String {
        if let Some(subject_template) = self
            .config
            .topic_subject_mapping
            .get(&ink.topic)
            .filter(|s| !s.is_empty())
        {
            return self.render_template(subject_template, ink);
        }
        self.render_template(&self.config.subject_template, ink)
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

    /// Build the email body from the template
    fn build_body(&self, ink: &Ink) -> String {
        self.render_template(&self.config.body_template, ink)
    }

    /// Build the email message
    fn build_message(&self, ink: &Ink) -> Result<Message> {
        let subject = self.resolve_subject(ink);
        let body = self.build_body(ink);
        let recipients = self.resolve_recipients(ink);

        let from_mailbox =
            if self.config.from_name.is_empty() {
                Mailbox::new(
                    None,
                    self.config.from_email.parse().with_context(|| {
                        format!("Invalid from email: {}", self.config.from_email)
                    })?,
                )
            } else {
                Mailbox::new(
                    Some(self.config.from_name.parse().with_context(|| {
                        format!("Invalid from name: {}", self.config.from_name)
                    })?),
                    self.config.from_email.parse().with_context(|| {
                        format!("Invalid from email: {}", self.config.from_email)
                    })?,
                )
            };

        let mut builder = Message::builder().from(from_mailbox).subject(subject);

        // Add To recipients
        for to in &recipients {
            if let Ok(addr) = to.parse() {
                builder = builder.to(Mailbox::new(None, addr));
            }
        }

        // Add CC recipients
        for cc in &self.config.cc_emails {
            if let Ok(addr) = cc.parse() {
                builder = builder.cc(Mailbox::new(None, addr));
            }
        }

        // Add BCC recipients
        for bcc in &self.config.bcc_emails {
            if let Ok(addr) = bcc.parse() {
                builder = builder.bcc(Mailbox::new(None, addr));
            }
        }

        // Build the email body (single-part or multi-part for HTML)
        let email_body = if self.config.html_body {
            MultiPart::alternative_plain_html(body.clone(), body.clone())
        } else {
            MultiPart::alternative_plain_html(body.clone(), String::new())
        };

        // Build the message with optional attachments
        let message = if self.config.attachments.is_empty() {
            builder
                .multipart(email_body)
                .with_context(|| "Failed to build email message")?
        } else {
            // Build multipart with attachments
            let mut mixed = MultiPart::mixed().build();
            mixed = mixed.singlepart(
                SinglePart::builder()
                    .header(ContentType::TEXT_PLAIN)
                    .body(body.clone()),
            );

            for attachment_path in &self.config.attachments {
                let path = PathBuf::from(attachment_path);
                if let Ok(content) = std::fs::read(&path) {
                    let filename = path
                        .file_name()
                        .map(|f| f.to_string_lossy().to_string())
                        .unwrap_or_else(|| "attachment".to_string());

                    let attachment = SinglePart::builder()
                        .header(
                            ContentType::parse("application/octet-stream")
                                .unwrap_or(ContentType::TEXT_PLAIN),
                        )
                        .header(ContentDisposition::attachment(&filename))
                        .body(content);

                    mixed = mixed.singlepart(attachment);
                } else {
                    warn!(
                        "Email reflex could not read attachment: {}",
                        attachment_path
                    );
                }
            }

            builder
                .multipart(mixed)
                .with_context(|| "Failed to build email message with attachments")?
        };

        Ok(message)
    }
}

#[async_trait]
impl Reflex for EmailReflex {
    fn name(&self) -> &str {
        &self._name
    }

    fn r#type(&self) -> &str {
        "email"
    }

    async fn handle(&self, ink: &Ink) -> Result<()> {
        info!(
            "Email reflex '{}' handling ink on topic: {}",
            self._name, ink.topic
        );

        let message = self
            .build_message(ink)
            .with_context(|| "Failed to build email message")?;

        let recipients = self.resolve_recipients(ink);
        debug!(
            "Email reflex '{}' sending to {:?} for topic: {}",
            self._name, recipients, ink.topic
        );

        self.mailer
            .send(message)
            .await
            .with_context(|| format!("Email reflex '{}' failed to send email", self._name))?;

        info!(
            "Email reflex '{}' sent email to {:?} for topic: {}",
            self._name, recipients, ink.topic
        );

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_config() -> EmailReflexConfig {
        EmailReflexConfig {
            smtp_host: "smtp.example.com".to_string(),
            smtp_port: 587,
            smtp_user: "user@example.com".to_string(),
            smtp_password: "password".to_string(),
            from_email: "alerts@example.com".to_string(),
            from_name: "Kraken".to_string(),
            to_emails: vec!["recipient@example.com".to_string()],
            subject: "Notification".to_string(),
            html_body: false,
            body_template: "{{body}}".to_string(),
            subject_template: "{{topic}}: {{body}}".to_string(),
            topic_recipient_mapping: HashMap::new(),
            topic_subject_mapping: HashMap::new(),
            cc_emails: vec![],
            bcc_emails: vec![],
            attachments: vec![],
            implicit_tls: false,
        }
    }

    fn create_test_ink() -> Ink {
        Ink::new("/test/topic", "test", "test_source", "test body")
    }

    #[test]
    fn test_default_recipients() {
        let config = create_test_config();
        let reflex = EmailReflex::new("test".to_string(), config).unwrap();

        let ink = create_test_ink();
        let recipients = reflex.resolve_recipients(&ink);
        assert_eq!(recipients, vec!["recipient@example.com"]);
    }

    #[test]
    fn test_topic_recipient_mapping() {
        let mut config = create_test_config();
        let mut mapping = HashMap::new();
        mapping.insert(
            "/test/topic".to_string(),
            vec![
                "alert1@example.com".to_string(),
                "alert2@example.com".to_string(),
            ],
        );
        config.topic_recipient_mapping = mapping;

        let reflex = EmailReflex::new("test".to_string(), config).unwrap();

        let ink = create_test_ink();
        let recipients = reflex.resolve_recipients(&ink);
        assert_eq!(recipients.len(), 2);
        assert!(recipients.contains(&"alert1@example.com".to_string()));
        assert!(recipients.contains(&"alert2@example.com".to_string()));
    }

    #[test]
    fn test_default_subject() {
        let config = create_test_config();
        let reflex = EmailReflex::new("test".to_string(), config).unwrap();

        let ink = create_test_ink();
        let subject = reflex.resolve_subject(&ink);
        assert_eq!(subject, "/test/topic: test body");
    }

    #[test]
    fn test_topic_subject_mapping() {
        let mut config = create_test_config();
        let mut mapping = HashMap::new();
        mapping.insert("/test/topic".to_string(), "Alert: {{topic}}".to_string());
        config.topic_subject_mapping = mapping;

        let reflex = EmailReflex::new("test".to_string(), config).unwrap();

        let ink = create_test_ink();
        let subject = reflex.resolve_subject(&ink);
        assert_eq!(subject, "Alert: /test/topic");
    }

    #[test]
    fn test_body_template_rendering() {
        let mut config = create_test_config();
        config.body_template = "Topic: {{topic}}, Body: {{body}}".to_string();

        let reflex = EmailReflex::new("test".to_string(), config).unwrap();

        let ink = create_test_ink();
        let body = reflex.build_body(&ink);
        assert_eq!(body, "Topic: /test/topic, Body: test body");
    }

    #[test]
    fn test_metadata_in_template() {
        let mut config = create_test_config();
        config.body_template = "Value: {{metadata.temp}}".to_string();

        let reflex = EmailReflex::new("test".to_string(), config).unwrap();

        let ink = Ink::new("/test", "test", "test_source", "data")
            .with_metadata(serde_json::json!({ "temp": "42", "humidity": "60" }));
        let body = reflex.build_body(&ink);
        assert_eq!(body, "Value: 42");
    }

    #[test]
    fn test_timestamp_and_id_in_template() {
        let mut config = create_test_config();
        config.body_template = "ID: {{id}}, Time: {{timestamp}}".to_string();

        let reflex = EmailReflex::new("test".to_string(), config).unwrap();

        let ink = create_test_ink();
        let body = reflex.build_body(&ink);
        assert!(body.starts_with("ID: "));
        assert!(body.contains(", Time: "));
    }

    #[test]
    fn test_html_body_flag() {
        let mut config = create_test_config();
        config.html_body = true;
        config.body_template = "<h1>{{body}}</h1>".to_string();

        let reflex = EmailReflex::new("test".to_string(), config).unwrap();

        let ink = create_test_ink();
        let body = reflex.build_body(&ink);
        assert_eq!(body, "<h1>test body</h1>");
    }

    #[test]
    fn test_config_deserialization() {
        let yaml = r#"
            smtp_host: "smtp.gmail.com"
            smtp_port: 587
            smtp_user: "user@gmail.com"
            smtp_password: "app_password"
            from_email: "alerts@gmail.com"
            from_name: "Kraken Alerts"
            to_emails: ["admin@example.com", "ops@example.com"]
            subject: "Alert"
            html_body: true
            body_template: "<b>{{topic}}</b>: {{body}}"
            subject_template: "[{{topic}}] {{body}}"
            topic_recipient_mapping:
              "/critical": ["oncall@example.com"]
            topic_subject_mapping:
              "/critical": "CRITICAL: {{body}}"
            cc_emails: ["manager@example.com"]
            bcc_emails: ["archive@example.com"]
            implicit_tls: false
        "#;

        let config: EmailReflexConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.smtp_host, "smtp.gmail.com");
        assert_eq!(config.smtp_port, 587);
        assert_eq!(config.from_email, "alerts@gmail.com");
        assert_eq!(config.from_name, "Kraken Alerts");
        assert_eq!(config.to_emails.len(), 2);
        assert!(config.html_body);
        assert_eq!(config.cc_emails, vec!["manager@example.com"]);
        assert_eq!(config.bcc_emails, vec!["archive@example.com"]);
    }

    #[test]
    fn test_implicit_tls_config() {
        let mut config = create_test_config();
        config.implicit_tls = true;
        config.smtp_port = 465;

        let _reflex = EmailReflex::new("test".to_string(), config).unwrap();
        // Just verify it builds without panic
    }

    #[test]
    fn test_message_building() {
        let config = create_test_config();
        let reflex = EmailReflex::new("test".to_string(), config).unwrap();

        let ink = create_test_ink();
        let _message = reflex.build_message(&ink).unwrap();
        // Verify the message was built without error
    }

    #[test]
    fn test_subject_template_with_placeholders() {
        let mut config = create_test_config();
        config.subject_template = "[{{topic}}] {{body}} ({{id}})".to_string();

        let reflex = EmailReflex::new("test".to_string(), config).unwrap();

        let ink = create_test_ink();
        let subject = reflex.resolve_subject(&ink);
        assert!(subject.starts_with("[/test/topic]"));
        assert!(subject.contains("test body"));
    }
}
