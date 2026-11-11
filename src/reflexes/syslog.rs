use anyhow::{Context, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tokio::io::AsyncWriteExt;
use tokio::net::{TcpStream, UdpSocket};
use tracing::{debug, error, info, warn};

use super::Reflex;
use crate::model::Ink;

/// Syslog facility codes (RFC 5424)
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum SyslogFacility {
    Kernel = 0,
    UserLevel = 1,
    MailSystem = 2,
    SystemDaemon = 3,
    SecurityAuth = 4,
    Syslogd = 5,
    LinePrinter = 6,
    NetworkNews = 7,
    Uucp = 8,
    Clock = 9,
    SecurityAuthPriv = 10,
    Ftald = 11,
    Ntp = 12,
    LogAudit = 13,
    LogAlert = 14,
    Clock2 = 15,
    Local0 = 16,
    Local1 = 17,
    Local2 = 18,
    Local3 = 19,
    Local4 = 20,
    Local5 = 21,
    Local6 = 22,
    Local7 = 23,
}

impl SyslogFacility {
    fn to_value(self) -> u8 {
        self as u8
    }
}

/// Syslog severity levels (RFC 5424)
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum SyslogSeverity {
    Emergency = 0,
    Alert = 1,
    Critical = 2,
    Error = 3,
    Warning = 4,
    Notice = 5,
    Informational = 6,
    Debug = 7,
}

impl SyslogSeverity {
    fn to_value(self) -> u8 {
        self as u8
    }
}

/// Connection type for syslog
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum SyslogConnectionType {
    Udp,
    Tcp,
}

/// Configuration for Syslog Reflex
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyslogReflexConfig {
    /// Syslog server address (e.g., "localhost" or "192.168.1.100")
    pub server: String,

    /// Syslog server port (default: 514)
    #[serde(default = "default_port")]
    pub port: u16,

    /// Connection type: udp or tcp
    #[serde(default = "default_connection_type")]
    pub connection_type: SyslogConnectionType,

    /// Default facility
    #[serde(default = "default_facility")]
    pub facility: SyslogFacility,

    /// Default severity
    #[serde(default = "default_severity")]
    pub severity: SyslogSeverity,

    /// App-name field in RFC 5424 format (empty for BSD format)
    #[serde(default = "default_app_name")]
    pub app_name: String,

    /// Procedure ID field in RFC 5424 format
    #[serde(default)]
    pub proc_id: Option<String>,

    /// Template for the syslog message
    /// Supported placeholders: {{topic}}, {{body}}, {{id}}, {{timestamp}}, {{metadata.<key>}}
    #[serde(default = "default_message_template")]
    pub message_template: String,

    /// Use RFC 5424 format instead of BSD format
    #[serde(default)]
    pub rfc5424: bool,

    /// Optional: mapping from Ink topics to facilities
    #[serde(default)]
    pub topic_facility_mapping: HashMap<String, SyslogFacility>,

    /// Optional: mapping from Ink topics to severities
    #[serde(default)]
    pub topic_severity_mapping: HashMap<String, SyslogSeverity>,

    /// Optional: mapping from Ink topics to message templates
    #[serde(default)]
    pub topic_message_mapping: HashMap<String, String>,

    /// TCP connection timeout in seconds
    #[serde(default = "default_timeout_secs")]
    pub timeout_secs: u64,

    /// Number of retry attempts on failure
    #[serde(default = "default_retries")]
    pub retries: u32,

    /// Delay between retries in milliseconds
    #[serde(default = "default_retry_delay_ms")]
    pub retry_delay_ms: u64,
}

fn default_port() -> u16 {
    514
}

fn default_connection_type() -> SyslogConnectionType {
    SyslogConnectionType::Udp
}

fn default_facility() -> SyslogFacility {
    SyslogFacility::UserLevel
}

fn default_severity() -> SyslogSeverity {
    SyslogSeverity::Informational
}

fn default_app_name() -> String {
    "rustyKraken".to_string()
}

fn default_message_template() -> String {
    "{{body}}".to_string()
}

fn default_timeout_secs() -> u64 {
    10
}

fn default_retries() -> u32 {
    3
}

fn default_retry_delay_ms() -> u64 {
    1000
}

pub struct SyslogReflex {
    name: String,
    config: SyslogReflexConfig,
}

impl SyslogReflex {
    pub fn new(name: String, config: SyslogReflexConfig) -> Result<Self> {
        Ok(Self { name, config })
    }

    /// Resolve facility for a given Ink, checking topic-based mapping first
    fn resolve_facility(&self, ink: &Ink) -> SyslogFacility {
        if let Some(&facility) = self.config.topic_facility_mapping.get(&ink.topic) {
            return facility;
        }
        self.config.facility
    }

    /// Resolve severity for a given Ink, checking topic-based mapping first
    fn resolve_severity(&self, ink: &Ink) -> SyslogSeverity {
        if let Some(&severity) = self.config.topic_severity_mapping.get(&ink.topic) {
            return severity;
        }
        self.config.severity
    }

    /// Resolve message template for a given Ink, checking topic-based mapping first
    fn resolve_message_template(&self, ink: &Ink) -> String {
        if let Some(message_template) = self
            .config
            .topic_message_mapping
            .get(&ink.topic)
            .filter(|m| !m.is_empty())
        {
            return self.render_template(message_template, ink);
        }
        self.render_template(&self.config.message_template, ink)
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

    /// Calculate the combined priority value (facility * 8 + severity)
    fn calculate_priority(facility: SyslogFacility, severity: SyslogSeverity) -> u8 {
        (facility.to_value() << 3) + severity.to_value()
    }

    /// Format a syslog message in BSD format
    fn format_bsd_message(&self, priority: u8, message: &str) -> String {
        format!("<{}>{}", priority, message)
    }

    /// Format a syslog message in RFC 5424 format
    fn format_rfc5424_message(&self, priority: u8, message: &str) -> String {
        let timestamp = time::OffsetDateTime::now_utc()
            .format(&time::format_description::well_known::Rfc3339)
            .unwrap_or_else(|_| "1970-01-01T00:00:00Z".to_string());

        let proc_id = self.config.proc_id.as_deref().unwrap_or("-");

        // RFC 5424 format: <PRI>VERSION TIMESTAMP HOSTNAME APP-NAME PROCID MSGID SD MSG
        // Using empty SD (structured data) and "-" for optional fields
        format!(
            "<{}>1 {} {} {} {} {} - - {}",
            priority,
            timestamp,
            self.hostname(),
            self.config.app_name,
            proc_id,
            "-",
            message
        )
    }

    /// Get the hostname for syslog messages
    fn hostname(&self) -> String {
        hostname::get()
            .map(|h| h.to_string_lossy().to_string())
            .unwrap_or_else(|_| "unknown".to_string())
    }

    /// Format the syslog message based on configuration
    fn format_message(
        &self,
        facility: SyslogFacility,
        severity: SyslogSeverity,
        message: &str,
    ) -> String {
        let priority = Self::calculate_priority(facility, severity);

        if self.config.rfc5424 {
            self.format_rfc5424_message(priority, message)
        } else {
            self.format_bsd_message(priority, message)
        }
    }

    /// Send a syslog message via UDP
    async fn send_udp(&self, message: &str) -> Result<()> {
        let addr = format!("{}:{}", self.config.server, self.config.port);

        // Create a new UDP socket for each message (connectionless)
        let socket = UdpSocket::bind("0.0.0.0:0")
            .await
            .context("Failed to bind UDP socket")?;

        socket
            .send_to(message.as_bytes(), &addr)
            .await
            .context("Failed to send UDP syslog message")?;

        debug!("Syslog reflex '{}' sent UDP message to {}", self.name, addr);
        Ok(())
    }

    /// Send a syslog message via TCP
    async fn send_tcp(&self, message: &str) -> Result<()> {
        let addr = format!("{}:{}", self.config.server, self.config.port);

        let mut stream = TcpStream::connect(&addr)
            .await
            .with_context(|| format!("Failed to connect to TCP syslog server {}", addr))?;

        stream
            .write_all(message.as_bytes())
            .await
            .context("Failed to write TCP syslog message")?;

        stream
            .shutdown()
            .await
            .context("Failed to shutdown TCP connection")?;

        debug!("Syslog reflex '{}' sent TCP message to {}", self.name, addr);
        Ok(())
    }

    /// Send a syslog message with retry logic
    async fn send_with_retry(&self, ink: &Ink) -> Result<()> {
        let facility = self.resolve_facility(ink);
        let severity = self.resolve_severity(ink);
        let message = self.resolve_message_template(ink);
        let formatted = self.format_message(facility, severity, &message);

        debug!(
            "Syslog reflex '{}' sending message with facility {:?}, severity {:?}",
            self.name, facility, severity
        );

        let mut last_error = None;

        for attempt in 0..=self.config.retries {
            if attempt > 0 {
                warn!(
                    "Syslog reflex '{}' retrying send (attempt {}/{}) after {}ms delay",
                    self.name, attempt, self.config.retries, self.config.retry_delay_ms
                );
                tokio::time::sleep(std::time::Duration::from_millis(self.config.retry_delay_ms))
                    .await;
            }

            match self.config.connection_type {
                SyslogConnectionType::Udp => match self.send_udp(&formatted).await {
                    Ok(()) => return Ok(()),
                    Err(e) => {
                        error!(
                            "Syslog reflex '{}' failed to send UDP message (attempt {}/{}): {}",
                            self.name,
                            attempt + 1,
                            self.config.retries + 1,
                            e
                        );
                        last_error = Some(e);
                    }
                },
                SyslogConnectionType::Tcp => match self.send_tcp(&formatted).await {
                    Ok(()) => return Ok(()),
                    Err(e) => {
                        error!(
                            "Syslog reflex '{}' failed to send TCP message (attempt {}/{}): {}",
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

        Err(last_error.unwrap_or_else(|| anyhow::anyhow!("Syslog send failed after retries")))
    }
}

#[async_trait]
impl Reflex for SyslogReflex {
    fn name(&self) -> &str {
        &self.name
    }

    fn r#type(&self) -> &str {
        "syslog"
    }

    async fn handle(&self, ink: &Ink) -> Result<()> {
        info!(
            "Syslog reflex '{}' handling ink on topic: {}",
            self.name, ink.topic
        );

        match self.send_with_retry(ink).await {
            Ok(()) => {
                info!(
                    "Syslog reflex '{}' successfully handled ink for topic: {}",
                    self.name, ink.topic
                );
                Ok(())
            }
            Err(e) => {
                error!("Syslog reflex '{}' failed to handle ink: {}", self.name, e);
                Err(e)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn create_test_config() -> SyslogReflexConfig {
        SyslogReflexConfig {
            server: "localhost".to_string(),
            port: 514,
            connection_type: SyslogConnectionType::Udp,
            facility: SyslogFacility::UserLevel,
            severity: SyslogSeverity::Informational,
            app_name: "rustyKraken".to_string(),
            proc_id: None,
            message_template: "{{body}}".to_string(),
            rfc5424: false,
            topic_facility_mapping: HashMap::new(),
            topic_severity_mapping: HashMap::new(),
            topic_message_mapping: HashMap::new(),
            timeout_secs: 10,
            retries: 3,
            retry_delay_ms: 1000,
        }
    }

    fn create_test_ink() -> Ink {
        let mut ink = Ink::new("/test/topic", "test", "test_source", "Test syslog message");
        ink.metadata = Some(json!({"key1": "value1", "key2": 42}));
        ink
    }

    #[test]
    fn test_config_deserialization() {
        let yaml = r#"
            server: "syslog.example.com"
            port: 1514
            connection_type: "tcp"
            facility: "local0"
            severity: "warning"
            app_name: "myapp"
            proc_id: "12345"
            message_template: "[{{topic}}] {{body}}"
            rfc5424: true
            timeout_secs: 30
            retries: 5
            retry_delay_ms: 2000
            topic_facility_mapping:
              "/alerts/critical": "kernel"
            topic_severity_mapping:
              "/alerts/critical": "emergency"
            topic_message_mapping:
              "/alerts/critical": "🚨 {{body}}"
        "#;

        let config: SyslogReflexConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.server, "syslog.example.com");
        assert_eq!(config.port, 1514);
        assert_eq!(config.connection_type, SyslogConnectionType::Tcp);
        assert_eq!(config.facility, SyslogFacility::Local0);
        assert_eq!(config.severity, SyslogSeverity::Warning);
        assert_eq!(config.app_name, "myapp");
        assert_eq!(config.proc_id, Some("12345".to_string()));
        assert_eq!(config.message_template, "[{{topic}}] {{body}}");
        assert!(config.rfc5424);
        assert_eq!(config.timeout_secs, 30);
        assert_eq!(config.retries, 5);
        assert_eq!(config.retry_delay_ms, 2000);
        assert_eq!(
            config.topic_facility_mapping.get("/alerts/critical"),
            Some(&SyslogFacility::Kernel)
        );
        assert_eq!(
            config.topic_severity_mapping.get("/alerts/critical"),
            Some(&SyslogSeverity::Emergency)
        );
    }

    #[test]
    fn test_resolve_facility_from_topic_mapping() {
        let mut config = create_test_config();
        config
            .topic_facility_mapping
            .insert("/alerts/critical".to_string(), SyslogFacility::Kernel);

        let reflex = SyslogReflex::new("test".to_string(), config).unwrap();
        let mut ink = create_test_ink();
        ink.topic = "/alerts/critical".to_string();

        assert_eq!(reflex.resolve_facility(&ink), SyslogFacility::Kernel);
    }

    #[test]
    fn test_resolve_severity_from_topic_mapping() {
        let mut config = create_test_config();
        config
            .topic_severity_mapping
            .insert("/alerts/critical".to_string(), SyslogSeverity::Emergency);

        let reflex = SyslogReflex::new("test".to_string(), config).unwrap();
        let mut ink = create_test_ink();
        ink.topic = "/alerts/critical".to_string();

        assert_eq!(reflex.resolve_severity(&ink), SyslogSeverity::Emergency);
    }

    #[test]
    fn test_resolve_message_template_from_topic_mapping() {
        let mut config = create_test_config();
        config
            .topic_message_mapping
            .insert("/alerts/critical".to_string(), "🚨 {{body}}".to_string());

        let reflex = SyslogReflex::new("test".to_string(), config).unwrap();
        let mut ink = create_test_ink();
        ink.topic = "/alerts/critical".to_string();

        assert_eq!(
            reflex.resolve_message_template(&ink),
            "🚨 Test syslog message"
        );
    }

    #[test]
    fn test_render_template_with_all_placeholders() {
        let config = create_test_config();
        let reflex = SyslogReflex::new("test".to_string(), config).unwrap();
        let ink = create_test_ink();

        let template =
            "{{topic}}: {{body}} (ID: {{id}}, Time: {{timestamp}}, Key: {{metadata.key1}})";
        let rendered = reflex.render_template(template, &ink);

        assert!(rendered.starts_with("/test/topic: Test syslog message"));
        assert!(rendered.contains("(ID:"));
        assert!(rendered.contains("Time:"));
        assert!(rendered.contains("Key: value1"));
    }

    #[test]
    fn test_render_template_with_numeric_metadata() {
        let config = create_test_config();
        let reflex = SyslogReflex::new("test".to_string(), config).unwrap();
        let ink = create_test_ink();

        let template = "Value: {{metadata.key2}}";
        let rendered = reflex.render_template(template, &ink);

        assert_eq!(rendered, "Value: 42");
    }

    #[test]
    fn test_calculate_priority() {
        // Facility 1 (User) * 8 + Severity 6 (Info) = 14
        assert_eq!(
            SyslogReflex::calculate_priority(
                SyslogFacility::UserLevel,
                SyslogSeverity::Informational
            ),
            14
        );

        // Facility 0 (Kernel) * 8 + Severity 0 (Emergency) = 0
        assert_eq!(
            SyslogReflex::calculate_priority(SyslogFacility::Kernel, SyslogSeverity::Emergency),
            0
        );

        // Facility 16 (Local0) * 8 + Severity 3 (Error) = 131
        assert_eq!(
            SyslogReflex::calculate_priority(SyslogFacility::Local0, SyslogSeverity::Error),
            131
        );
    }

    #[test]
    fn test_format_bsd_message() {
        let config = create_test_config();
        let reflex = SyslogReflex::new("test".to_string(), config).unwrap();

        let message = reflex.format_bsd_message(14, "Test message");
        assert_eq!(message, "<14>Test message");
    }

    #[test]
    fn test_format_rfc5424_message() {
        let mut config = create_test_config();
        config.app_name = "myapp".to_string();
        config.proc_id = Some("12345".to_string());
        let reflex = SyslogReflex::new("test".to_string(), config).unwrap();

        let message = reflex.format_rfc5424_message(14, "Test message");
        assert!(message.starts_with("<14>1"));
        assert!(message.contains("myapp"));
        assert!(message.contains("12345"));
        assert!(message.contains("Test message"));
    }

    #[test]
    fn test_format_message_bsd() {
        let config = create_test_config();
        let reflex = SyslogReflex::new("test".to_string(), config).unwrap();

        let message = reflex.format_message(
            SyslogFacility::UserLevel,
            SyslogSeverity::Informational,
            "Test message",
        );
        assert_eq!(message, "<14>Test message");
    }

    #[test]
    fn test_format_message_rfc5424() {
        let mut config = create_test_config();
        config.rfc5424 = true;
        let reflex = SyslogReflex::new("test".to_string(), config).unwrap();

        let message = reflex.format_message(
            SyslogFacility::Local0,
            SyslogSeverity::Error,
            "Test message",
        );
        assert!(message.starts_with("<131>1"));
        assert!(message.contains("Test message"));
    }

    #[test]
    fn test_default_config_values() {
        let yaml = r#"
            server: "localhost"
        "#;

        let config: SyslogReflexConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.server, "localhost");
        assert_eq!(config.port, 514);
        assert_eq!(config.connection_type, SyslogConnectionType::Udp);
        assert_eq!(config.facility, SyslogFacility::UserLevel);
        assert_eq!(config.severity, SyslogSeverity::Informational);
        assert_eq!(config.app_name, "rustyKraken");
        assert_eq!(config.message_template, "{{body}}");
        assert!(!config.rfc5424);
        assert_eq!(config.timeout_secs, 10);
        assert_eq!(config.retries, 3);
        assert_eq!(config.retry_delay_ms, 1000);
    }
}
