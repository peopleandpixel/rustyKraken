use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::io::AsyncBufReadExt;
use tokio::net::{TcpListener, UdpSocket};
use tokio::sync::Mutex;
use tracing::{debug, error, info, warn};

use super::Tentacle;
use crate::eventbus::EventBus;
use crate::model::Ink;

/// Syslog-Transportprotokoll
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "lowercase")]
pub enum SyslogTransport {
    #[default]
    Udp,
    Tcp,
}

impl std::fmt::Display for SyslogTransport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SyslogTransport::Udp => write!(f, "udp"),
            SyslogTransport::Tcp => write!(f, "tcp"),
        }
    }
}

/// Topic-Erkennungsmodus für Syslog-Nachrichten
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "lowercase")]
pub enum SyslogTopicMode {
    /// Topic aus dem Facility/Severity kombinieren
    #[default]
    Facility,
    /// Alle Nachrichten an ein festes Topic
    All,
    /// Topic aus der App-Name (RFC 5424)
    Appname,
}

/// Konfiguration für den Syslog Listener Tentacle
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyslogTentacleConfig {
    /// Transportprotokoll: "udp" oder "tcp"
    #[serde(default)]
    pub transport: SyslogTransport,
    /// Bind-Adresse (z.B. "0.0.0.0:514")
    #[serde(default = "default_bind_addr")]
    pub bind_addr: String,
    /// Topic-Erkennungsmodus
    #[serde(default)]
    pub topic_mode: SyslogTopicMode,
    /// Fallback-Topic
    #[serde(default = "default_fallback_topic")]
    pub fallback_topic: String,
    /// Puffergröße für UDP-Pakete (Standard: 65535)
    #[serde(default = "default_buffer_size")]
    pub buffer_size: usize,
    /// Maximale Anzahl gleichzeitiger TCP-Verbindungen
    #[serde(default = "default_max_connections")]
    pub max_connections: usize,
    /// Optional: Facility-basiertes Topic-Mapping
    #[serde(default)]
    pub facility_topic_mapping: HashMap<String, String>,
}

fn default_bind_addr() -> String {
    "0.0.0.0:514".to_string()
}

fn default_fallback_topic() -> String {
    "/syslog/default".to_string()
}

fn default_buffer_size() -> usize {
    65535
}

fn default_max_connections() -> usize {
    100
}

/// Geparste Syslog-Nachricht
#[derive(Debug, Clone)]
struct ParsedSyslog {
    priority: u32,
    facility: u32,
    severity: u32,
    timestamp: Option<String>,
    hostname: Option<String>,
    appname: Option<String>,
    procid: Option<String>,
    msgid: Option<String>,
    message: String,
}

pub struct SyslogTentacle {
    name: String,
    config: SyslogTentacleConfig,
}

impl SyslogTentacle {
    pub fn new(name: String, config: SyslogTentacleConfig) -> Self {
        Self { name, config }
    }

    /// Parst eine BSD-Style Syslog-Nachricht (RFC 3164)
    /// Format: <PRI>TIMESTAMP HOSTNAME APP-NAME[PROCID]: MESSAGE
    fn parse_bsd_syslog(line: &str) -> Option<ParsedSyslog> {
        // PRI extrahieren
        if !line.starts_with('<') {
            return None;
        }
        let close_bracket = line.find('>')?;
        let priority: u32 = line[1..close_bracket].parse().ok()?;
        let facility = priority / 8;
        let severity = priority % 8;

        let rest = &line[close_bracket + 1..];

        // BSD-Syslog: TIMESTAMP HOSTNAME ...
        // Timestamp-Format: "Oct  1 00:00:00" (15 Zeichen)
        let (timestamp, rest) = if rest.len() > 15 {
            let ts = &rest[..15];
            let r = &rest[15..];
            (ts, r.trim_start())
        } else {
            ("", rest)
        };

        // HOSTNAME extrahieren
        let (hostname, rest) = match rest.find(' ') {
            Some(pos) => (&rest[..pos], &rest[pos + 1..]),
            None => (rest, ""),
        };

        // APP-NAME[PROCID]: MESSAGE oder APP-NAME: MESSAGE oder MESSAGE
        let (appname, procid, message) = if let Some(colon_pos) = rest.find(':') {
            let tag_part = &rest[..colon_pos];
            let msg_part = &rest[colon_pos + 1..].trim_start();

            if let Some(bracket_pos) = tag_part.find('[') {
                if let Some(close_bracket) = tag_part.find(']') {
                    let app = &tag_part[..bracket_pos];
                    let proc = &tag_part[bracket_pos + 1..close_bracket];
                    (
                        Some(app.to_string()),
                        Some(proc.to_string()),
                        msg_part.to_string(),
                    )
                } else {
                    (Some(tag_part.to_string()), None, msg_part.to_string())
                }
            } else {
                (Some(tag_part.to_string()), None, msg_part.to_string())
            }
        } else {
            (None, None, rest.to_string())
        };

        Some(ParsedSyslog {
            priority,
            facility,
            severity,
            timestamp: if !timestamp.is_empty() {
                Some(timestamp.to_string())
            } else {
                None
            },
            hostname: if !hostname.is_empty() {
                Some(hostname.to_string())
            } else {
                None
            },
            appname,
            procid,
            msgid: None,
            message,
        })
    }

    /// Parst eine RFC 5424 Syslog-Nachricht
    /// Format: <PRI>VERSION TIMESTAMP HOSTNAME APP-NAME PROCID MSGID STRUCTURED-DATA MSG
    fn parse_rfc5424_syslog(line: &str) -> Option<ParsedSyslog> {
        if !line.starts_with('<') {
            return None;
        }
        let close_bracket = line.find('>')?;
        let priority: u32 = line[1..close_bracket].parse().ok()?;
        let facility = priority / 8;
        let severity = priority % 8;

        let rest = &line[close_bracket + 1..];

        // VERSION extrahieren
        let pos = rest.find(' ')?;
        let ver_str = &rest[..pos];
        if ver_str != "1" {
            return Self::parse_bsd_syslog(line);
        }
        let rest = &rest[pos + 1..];

        // TIMESTAMP
        let pos = rest.find(' ')?;
        let timestamp = &rest[..pos];
        let rest = &rest[pos + 1..];

        // HOSTNAME
        let pos = rest.find(' ')?;
        let hostname = &rest[..pos];
        let rest = &rest[pos + 1..];

        // APP-NAME
        let pos = rest.find(' ')?;
        let appname = &rest[..pos];
        let rest = &rest[pos + 1..];

        // PROCID
        let pos = rest.find(' ')?;
        let procid = &rest[..pos];
        let rest = &rest[pos + 1..];

        // MSGID
        let pos = rest.find(' ')?;
        let msgid = &rest[..pos];
        let rest = &rest[pos + 1..];

        // STRUCTURED-DATA
        let rest = if let Some(stripped) = rest.strip_prefix("[") {
            if let Some(end) = stripped.rfind(']') {
                stripped[end + 1..].trim_start()
            } else {
                ""
            }
        } else if let Some(stripped) = rest.strip_prefix("- ") {
            stripped
        } else {
            rest
        };

        let appname_str = if appname != "-" {
            Some(appname.to_string())
        } else {
            None
        };
        let procid_str = if procid != "-" {
            Some(procid.to_string())
        } else {
            None
        };
        let msgid_str = if msgid != "-" {
            Some(msgid.to_string())
        } else {
            None
        };

        Some(ParsedSyslog {
            priority,
            facility,
            severity,
            timestamp: Some(timestamp.to_string()),
            hostname: if !hostname.is_empty() {
                Some(hostname.to_string())
            } else {
                None
            },
            appname: appname_str,
            procid: procid_str,
            msgid: msgid_str,
            message: rest.to_string(),
        })
    }

    /// Parst eine Syslog-Nachricht (automatische Erkennung BSD vs RFC 5424)
    fn parse_syslog(line: &str) -> Option<ParsedSyslog> {
        // Versuche zuerst RFC 5424, dann BSD
        Self::parse_rfc5424_syslog(line).or_else(|| Self::parse_bsd_syslog(line))
    }

    /// Bestimmt das Topic basierend auf der geparsten Syslog-Nachricht
    fn resolve_topic(&self, parsed: &ParsedSyslog) -> String {
        match self.config.topic_mode {
            SyslogTopicMode::Facility => {
                // Prüfe facility_topic_mapping
                if let Some(topic) = self
                    .config
                    .facility_topic_mapping
                    .get(&parsed.facility.to_string())
                {
                    return topic.clone();
                }
                // Standard: /syslog/<facility_name>/<severity_name>
                let facility_name = facility_to_name(parsed.facility);
                let severity_name = severity_to_name(parsed.severity);
                format!("/syslog/{}/{}", facility_name, severity_name)
            }
            SyslogTopicMode::All => self.config.fallback_topic.clone(),
            SyslogTopicMode::Appname => {
                if let Some(ref appname) = parsed.appname {
                    // Ersetze Sonderzeichen durch Unterstriche
                    let safe = appname.replace(|c: char| !c.is_alphanumeric() && c != '_', "_");
                    format!("/syslog/{}", safe)
                } else {
                    self.config.fallback_topic.clone()
                }
            }
        }
    }

    /// Erstellt eine Ink-Nachricht aus einer geparsten Syslog-Nachricht
    fn create_ink(&self, parsed: &ParsedSyslog) -> Ink {
        let topic = self.resolve_topic(parsed);

        let mut metadata = serde_json::Map::new();
        metadata.insert("priority".to_string(), serde_json::json!(parsed.priority));
        metadata.insert("facility".to_string(), serde_json::json!(parsed.facility));
        metadata.insert("severity".to_string(), serde_json::json!(parsed.severity));
        metadata.insert(
            "facility_name".to_string(),
            serde_json::json!(facility_to_name(parsed.facility)),
        );
        metadata.insert(
            "severity_name".to_string(),
            serde_json::json!(severity_to_name(parsed.severity)),
        );

        if let Some(ref ts) = parsed.timestamp {
            metadata.insert("timestamp".to_string(), serde_json::json!(ts));
        }
        if let Some(ref hostname) = parsed.hostname {
            metadata.insert("hostname".to_string(), serde_json::json!(hostname));
        }
        if let Some(ref appname) = parsed.appname {
            metadata.insert("appname".to_string(), serde_json::json!(appname));
        }
        if let Some(ref procid) = parsed.procid {
            metadata.insert("procid".to_string(), serde_json::json!(procid));
        }
        if let Some(ref msgid) = parsed.msgid {
            metadata.insert("msgid".to_string(), serde_json::json!(msgid));
        }

        Ink::new(topic, "syslog", &self.name, parsed.message.clone())
            .with_metadata(serde_json::Value::Object(metadata))
    }

    /// UDP-Listener
    async fn run_udp(&self, event_bus: Arc<EventBus>) -> Result<()> {
        let socket = UdpSocket::bind(&self.config.bind_addr).await?;
        let addr = socket.local_addr()?;
        info!("Syslog UDP listener started on {}", addr);

        let mut buf = vec![0u8; self.config.buffer_size];

        loop {
            let (len, _addr) = socket.recv_from(&mut buf).await?;
            let data = &buf[..len];

            // UTF-8 decodieren
            let line = match std::str::from_utf8(data) {
                Ok(s) => s.trim().to_string(),
                Err(_) => {
                    warn!("Invalid UTF-8 in syslog message");
                    continue;
                }
            };

            if line.is_empty() {
                continue;
            }

            debug!("Received syslog message ({} bytes)", len);

            match Self::parse_syslog(&line) {
                Some(parsed) => {
                    let ink = self.create_ink(&parsed);
                    info!(
                        "Syslog message: facility={}, severity={}, message={}",
                        parsed.facility,
                        parsed.severity,
                        &parsed.message[..parsed.message.len().min(100)]
                    );
                    event_bus.publish(ink);
                }
                None => {
                    warn!(
                        "Could not parse syslog message: {}",
                        &line[..line.len().min(100)]
                    );
                }
            }
        }
    }

    /// TCP-Listener
    async fn run_tcp(&self, event_bus: Arc<EventBus>) -> Result<()> {
        let listener = TcpListener::bind(&self.config.bind_addr).await?;
        let addr = listener.local_addr()?;
        info!("Syslog TCP listener started on {}", addr);

        let active_connections = Arc::new(Mutex::new(0u32));

        loop {
            let (stream, _addr) = listener.accept().await?;

            let mut count = active_connections.lock().await;
            if *count >= self.config.max_connections as u32 {
                warn!("Max TCP connections reached, rejecting new connection");
                continue;
            }
            *count += 1;

            let event_bus = Arc::clone(&event_bus);
            let tentacle_name = self.name.clone();
            let config = self.config.clone();
            let active = Arc::clone(&active_connections);

            tokio::spawn(async move {
                let mut reader = tokio::io::BufReader::new(stream);
                let mut line = String::new();

                loop {
                    line.clear();
                    match reader.read_line(&mut line).await {
                        Ok(0) => {
                            // Verbindung geschlossen
                            break;
                        }
                        Ok(_) => {
                            let line = line.trim().to_string();
                            if line.is_empty() {
                                continue;
                            }

                            debug!("Received TCP syslog message");

                            match SyslogTentacle::parse_syslog(&line) {
                                Some(parsed) => {
                                    let topic = config.topic_mode;
                                    let fallback = config.fallback_topic.clone();
                                    let facility_mapping = config.facility_topic_mapping.clone();

                                    // Topic auflösen
                                    let topic = match topic {
                                        SyslogTopicMode::Facility => {
                                            if let Some(t) =
                                                facility_mapping.get(&parsed.facility.to_string())
                                            {
                                                t.clone()
                                            } else {
                                                let facility_name =
                                                    facility_to_name(parsed.facility);
                                                let severity_name =
                                                    severity_to_name(parsed.severity);
                                                format!(
                                                    "/syslog/{}/{}",
                                                    facility_name, severity_name
                                                )
                                            }
                                        }
                                        SyslogTopicMode::All => fallback,
                                        SyslogTopicMode::Appname => {
                                            if let Some(ref appname) = parsed.appname {
                                                let safe = appname.replace(
                                                    |c: char| !c.is_alphanumeric() && c != '_',
                                                    "_",
                                                );
                                                format!("/syslog/{}", safe)
                                            } else {
                                                fallback
                                            }
                                        }
                                    };

                                    let mut metadata = serde_json::Map::new();
                                    metadata.insert(
                                        "priority".to_string(),
                                        serde_json::json!(parsed.priority),
                                    );
                                    metadata.insert(
                                        "facility".to_string(),
                                        serde_json::json!(parsed.facility),
                                    );
                                    metadata.insert(
                                        "severity".to_string(),
                                        serde_json::json!(parsed.severity),
                                    );
                                    metadata.insert(
                                        "facility_name".to_string(),
                                        serde_json::json!(facility_to_name(parsed.facility)),
                                    );
                                    metadata.insert(
                                        "severity_name".to_string(),
                                        serde_json::json!(severity_to_name(parsed.severity)),
                                    );
                                    if let Some(ref ts) = parsed.timestamp {
                                        metadata
                                            .insert("timestamp".to_string(), serde_json::json!(ts));
                                    }
                                    if let Some(ref hostname) = parsed.hostname {
                                        metadata.insert(
                                            "hostname".to_string(),
                                            serde_json::json!(hostname),
                                        );
                                    }
                                    if let Some(ref appname) = parsed.appname {
                                        metadata.insert(
                                            "appname".to_string(),
                                            serde_json::json!(appname),
                                        );
                                    }

                                    let ink = Ink::new(
                                        topic,
                                        "syslog",
                                        &tentacle_name,
                                        parsed.message.clone(),
                                    )
                                    .with_metadata(serde_json::Value::Object(metadata));

                                    event_bus.publish(ink);
                                }
                                None => {
                                    warn!("Could not parse TCP syslog message");
                                }
                            }
                        }
                        Err(e) => {
                            error!("Error reading TCP syslog: {}", e);
                            break;
                        }
                    }
                }

                // Verbindungszähler dekrementieren
                let mut count = active.lock().await;
                *count = count.saturating_sub(1);
            });
        }
    }
}

/// Konvertiert Facility-Nummer zu Namen
fn facility_to_name(facility: u32) -> String {
    match facility {
        0 => "kern".to_string(),
        1 => "user".to_string(),
        2 => "mail".to_string(),
        3 => "daemon".to_string(),
        4 => "auth".to_string(),
        5 => "syslog".to_string(),
        6 => "lpr".to_string(),
        7 => "news".to_string(),
        8 => "uucp".to_string(),
        9 => "cron".to_string(),
        10 => "authpriv".to_string(),
        11 => "ftp".to_string(),
        12 => "ntp".to_string(),
        13 => "security".to_string(),
        14 => "console".to_string(),
        15 => "solaris-cron".to_string(),
        16..=23 => "local".to_string(),
        _ => format!("unknown-{}", facility),
    }
}

/// Konvertiert Severity-Nummer zu Namen
fn severity_to_name(severity: u32) -> String {
    match severity {
        0 => "emergency".to_string(),
        1 => "alert".to_string(),
        2 => "critical".to_string(),
        3 => "error".to_string(),
        4 => "warning".to_string(),
        5 => "notice".to_string(),
        6 => "info".to_string(),
        7 => "debug".to_string(),
        _ => format!("unknown-{}", severity),
    }
}

#[async_trait]
impl Tentacle for SyslogTentacle {
    fn name(&self) -> &str {
        &self.name
    }

    fn r#type(&self) -> &str {
        "syslog"
    }

    async fn start(&self, event_bus: Arc<EventBus>) -> Result<()> {
        info!(
            "Syslog Tentacle '{}' starting on {} ({:?})",
            self.name, self.config.bind_addr, self.config.transport
        );

        match self.config.transport {
            SyslogTransport::Udp => self.run_udp(event_bus).await,
            SyslogTransport::Tcp => self.run_tcp(event_bus).await,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_bsd_syslog() {
        let line = "<13>Jan  1 00:00:00 myhost myapp[1234]: Hello world";
        let parsed = SyslogTentacle::parse_syslog(line).unwrap();

        assert_eq!(parsed.priority, 13);
        assert_eq!(parsed.facility, 1);
        assert_eq!(parsed.severity, 5);
        assert_eq!(parsed.hostname.as_deref(), Some("myhost"));
        assert_eq!(parsed.appname.as_deref(), Some("myapp"));
        assert_eq!(parsed.procid.as_deref(), Some("1234"));
        assert_eq!(parsed.message, "Hello world");
    }

    #[test]
    fn test_parse_bsd_syslog_no_proc() {
        let line = "<30>Jan  1 00:00:00 myhost myapp: Simple message";
        let parsed = SyslogTentacle::parse_syslog(line).unwrap();

        assert_eq!(parsed.facility, 3);
        assert_eq!(parsed.severity, 6);
        assert_eq!(parsed.appname.as_deref(), Some("myapp"));
        assert_eq!(parsed.procid, None);
        assert_eq!(parsed.message, "Simple message");
    }

    #[test]
    fn test_parse_rfc5424_syslog() {
        let line = "<165>1 2026-01-01T00:00:00Z myhost myapp 1234 MSGID - Hello RFC5424";
        let parsed = SyslogTentacle::parse_syslog(line).unwrap();

        assert_eq!(parsed.facility, 20);
        assert_eq!(parsed.severity, 5);
        assert_eq!(parsed.hostname.as_deref(), Some("myhost"));
        assert_eq!(parsed.appname.as_deref(), Some("myapp"));
        assert_eq!(parsed.procid.as_deref(), Some("1234"));
        assert_eq!(parsed.msgid.as_deref(), Some("MSGID"));
        assert_eq!(parsed.message, "Hello RFC5424");
    }

    #[test]
    fn test_parse_rfc5424_with_structured_data() {
        let line =
            "<14>1 2026-01-01T00:00:00Z myhost myapp 1234 - [meta seq=\"1\"] Message with SD";
        let parsed = SyslogTentacle::parse_syslog(line).unwrap();

        assert_eq!(parsed.facility, 1);
        assert_eq!(parsed.severity, 6);
        assert_eq!(parsed.msgid, None);
        assert_eq!(parsed.message, "Message with SD");
    }

    #[test]
    fn test_facility_to_name() {
        assert_eq!(facility_to_name(0), "kern");
        assert_eq!(facility_to_name(1), "user");
        assert_eq!(facility_to_name(4), "auth");
        assert_eq!(facility_to_name(9), "cron");
        assert_eq!(facility_to_name(16), "local");
        assert_eq!(facility_to_name(99), "unknown-99");
    }

    #[test]
    fn test_severity_to_name() {
        assert_eq!(severity_to_name(0), "emergency");
        assert_eq!(severity_to_name(3), "error");
        assert_eq!(severity_to_name(6), "info");
        assert_eq!(severity_to_name(7), "debug");
        assert_eq!(severity_to_name(99), "unknown-99");
    }

    #[test]
    fn test_resolve_topic_facility_mode() {
        let config = SyslogTentacleConfig {
            transport: SyslogTransport::Udp,
            bind_addr: "0.0.0.0:514".to_string(),
            topic_mode: SyslogTopicMode::Facility,
            fallback_topic: "/syslog/default".to_string(),
            buffer_size: 65535,
            max_connections: 100,
            facility_topic_mapping: HashMap::new(),
        };
        let tentacle = SyslogTentacle::new("test".to_string(), config);

        let parsed = ParsedSyslog {
            priority: 13,
            facility: 1,
            severity: 5,
            timestamp: None,
            hostname: None,
            appname: None,
            procid: None,
            msgid: None,
            message: "test".to_string(),
        };

        assert_eq!(tentacle.resolve_topic(&parsed), "/syslog/user/notice");
    }

    #[test]
    fn test_resolve_topic_facility_mapping() {
        let mut mapping = HashMap::new();
        mapping.insert("1".to_string(), "/syslog/user-custom".to_string());

        let config = SyslogTentacleConfig {
            transport: SyslogTransport::Udp,
            bind_addr: "0.0.0.0:514".to_string(),
            topic_mode: SyslogTopicMode::Facility,
            fallback_topic: "/syslog/default".to_string(),
            buffer_size: 65535,
            max_connections: 100,
            facility_topic_mapping: mapping,
        };
        let tentacle = SyslogTentacle::new("test".to_string(), config);

        let parsed = ParsedSyslog {
            priority: 13,
            facility: 1,
            severity: 5,
            timestamp: None,
            hostname: None,
            appname: None,
            procid: None,
            msgid: None,
            message: "test".to_string(),
        };

        assert_eq!(tentacle.resolve_topic(&parsed), "/syslog/user-custom");
    }

    #[test]
    fn test_resolve_topic_appname_mode() {
        let config = SyslogTentacleConfig {
            transport: SyslogTransport::Udp,
            bind_addr: "0.0.0.0:514".to_string(),
            topic_mode: SyslogTopicMode::Appname,
            fallback_topic: "/syslog/default".to_string(),
            buffer_size: 65535,
            max_connections: 100,
            facility_topic_mapping: HashMap::new(),
        };
        let tentacle = SyslogTentacle::new("test".to_string(), config);

        let parsed = ParsedSyslog {
            priority: 13,
            facility: 1,
            severity: 5,
            timestamp: None,
            hostname: None,
            appname: Some("my-app".to_string()),
            procid: None,
            msgid: None,
            message: "test".to_string(),
        };

        assert_eq!(tentacle.resolve_topic(&parsed), "/syslog/my_app");
    }

    #[test]
    fn test_resolve_topic_all_mode() {
        let config = SyslogTentacleConfig {
            transport: SyslogTransport::Udp,
            bind_addr: "0.0.0.0:514".to_string(),
            topic_mode: SyslogTopicMode::All,
            fallback_topic: "/syslog/all".to_string(),
            buffer_size: 65535,
            max_connections: 100,
            facility_topic_mapping: HashMap::new(),
        };
        let tentacle = SyslogTentacle::new("test".to_string(), config);

        let parsed = ParsedSyslog {
            priority: 13,
            facility: 1,
            severity: 5,
            timestamp: None,
            hostname: None,
            appname: None,
            procid: None,
            msgid: None,
            message: "test".to_string(),
        };

        assert_eq!(tentacle.resolve_topic(&parsed), "/syslog/all");
    }

    #[test]
    fn test_syslog_config_deserialization() {
        let yaml = r#"
        transport: "udp"
        bind_addr: "0.0.0.0:514"
        topic_mode: "facility"
        fallback_topic: "/syslog/default"
        buffer_size: 65535
        facility_topic_mapping:
          "0": "/syslog/kernel"
          "4": "/syslog/auth"
        "#;

        let config: SyslogTentacleConfig = serde_yaml::from_str(yaml).unwrap();
        assert!(matches!(config.transport, SyslogTransport::Udp));
        assert_eq!(config.bind_addr, "0.0.0.0:514");
        assert!(matches!(config.topic_mode, SyslogTopicMode::Facility));
        assert_eq!(config.facility_topic_mapping.len(), 2);
    }

    #[test]
    fn test_syslog_tentacle_name_and_type() {
        let config = SyslogTentacleConfig {
            transport: SyslogTransport::Udp,
            bind_addr: "0.0.0.0:514".to_string(),
            topic_mode: SyslogTopicMode::Facility,
            fallback_topic: "/syslog/default".to_string(),
            buffer_size: 65535,
            max_connections: 100,
            facility_topic_mapping: HashMap::new(),
        };
        let tentacle = SyslogTentacle::new("test_syslog".to_string(), config);
        assert_eq!(tentacle.name(), "test_syslog");
        assert_eq!(tentacle.r#type(), "syslog");
    }

    #[test]
    fn test_create_ink_metadata() {
        let config = SyslogTentacleConfig {
            transport: SyslogTransport::Udp,
            bind_addr: "0.0.0.0:514".to_string(),
            topic_mode: SyslogTopicMode::Facility,
            fallback_topic: "/syslog/default".to_string(),
            buffer_size: 65535,
            max_connections: 100,
            facility_topic_mapping: HashMap::new(),
        };
        let tentacle = SyslogTentacle::new("test".to_string(), config);

        let parsed = ParsedSyslog {
            priority: 28,
            facility: 3,
            severity: 4,
            timestamp: Some("2026-01-01T00:00:00Z".to_string()),
            hostname: Some("server1".to_string()),
            appname: Some("nginx".to_string()),
            procid: Some("1234".to_string()),
            msgid: Some("ERR001".to_string()),
            message: "Connection refused".to_string(),
        };

        let ink = tentacle.create_ink(&parsed);
        assert_eq!(ink.topic, "/syslog/daemon/warning");
        assert_eq!(ink.body, "Connection refused");

        let meta = ink.metadata.unwrap();
        let meta_obj = meta.as_object().unwrap();
        assert_eq!(meta_obj.get("facility").unwrap(), &serde_json::json!(3));
        assert_eq!(meta_obj.get("severity").unwrap(), &serde_json::json!(4));
        assert_eq!(
            meta_obj.get("hostname").unwrap(),
            &serde_json::json!("server1")
        );
        assert_eq!(
            meta_obj.get("appname").unwrap(),
            &serde_json::json!("nginx")
        );
    }
}
