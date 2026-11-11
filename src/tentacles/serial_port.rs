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

/// Protokoll-Parser für serielle Daten
#[derive(Debug, Clone, Serialize, Deserialize, Default, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum ProtocolParser {
    /// Jede Zeile als separate Nachricht (plain text)
    #[default]
    Plain,
    /// JSON-Objekte pro Zeile
    Json,
    /// CSV mit konfigurierbarem Trennzeichen
    Csv,
    /// Custom-Delimiter (z.B. "\r\n" oder spezielles Byte)
    Custom,
}

/// Konfiguration für einen seriellen Port
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SerialPortConfig {
    /// Pfad zum seriellen Port (z.B. "/dev/ttyUSB0" oder "COM1")
    pub port: String,
    /// Baudrate (z.B. 9600, 115200)
    #[serde(default = "default_baud_rate")]
    pub baud_rate: u32,
    /// Datenbits (5, 6, 7, 8; Standard: 8)
    #[serde(default = "default_data_bits")]
    pub data_bits: u8,
    /// Parity (none, even, odd; Standard: none)
    #[serde(default = "default_parity")]
    pub parity: String,
    /// Stopbits (1, 2; Standard: 1)
    #[serde(default = "default_stop_bits")]
    pub stop_bits: u8,
    /// Topic für Nachrichten von diesem Port
    #[serde(default = "default_topic")]
    pub topic: String,
    /// Protokoll-Parser (plain, json, csv, custom)
    #[serde(default)]
    pub parser: ProtocolParser,
    /// Custom-Delimiter (nur bei parser: "custom"), als Hex-String
    #[serde(default)]
    pub delimiter: Option<String>,
    /// CSV-Trennzeichen (nur bei parser: "csv"; Standard: ",")
    #[serde(default = "default_csv_delimiter")]
    pub csv_delimiter: String,
}

fn default_baud_rate() -> u32 {
    9600
}

fn default_data_bits() -> u8 {
    8
}

fn default_parity() -> String {
    "none".to_string()
}

fn default_stop_bits() -> u8 {
    1
}

fn default_topic() -> String {
    "/serial/default".to_string()
}

fn default_csv_delimiter() -> String {
    ",".to_string()
}

/// Konfiguration für den Serial Port Tentacle
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SerialTentacleConfig {
    /// Liste der seriellen Ports
    pub ports: Vec<SerialPortConfig>,
    /// Reconnect-Verzögerung in Sekunden bei Fehlern (Standard: 5)
    #[serde(default = "default_reconnect_delay")]
    pub reconnect_delay: u64,
    /// Zeilenpuffer-Größe in Bytes (Standard: 4096)
    #[serde(default = "default_buffer_size")]
    pub buffer_size: usize,
    /// Optional: Mapping von Port-Pfaden auf Topics
    #[serde(default)]
    pub port_topic_mapping: HashMap<String, String>,
}

fn default_reconnect_delay() -> u64 {
    5
}

fn default_buffer_size() -> usize {
    4096
}

pub struct SerialTentacle {
    name: String,
    config: SerialTentacleConfig,
}

impl SerialTentacle {
    pub fn new(name: String, config: SerialTentacleConfig) -> Self {
        Self { name, config }
    }

    /// Bestimmt das Topic für einen Port
    fn resolve_topic(&self, port_config: &SerialPortConfig) -> String {
        // Port-basiertes Mapping hat Vorrang
        if let Some(topic) = self.config.port_topic_mapping.get(&port_config.port) {
            return topic.clone();
        }
        port_config.topic.clone()
    }

    /// Parst eine Zeile basierend auf dem konfigurierten Parser
    #[cfg(test)]
    fn parse_line(&self, line: &str, parser: &ProtocolParser, csv_delimiter: &str) -> String {
        match parser {
            ProtocolParser::Plain => line.to_string(),
            ProtocolParser::Json => {
                if let Ok(val) = serde_json::from_str::<serde_json::Value>(line) {
                    serde_json::to_string(&val).unwrap_or_else(|_| line.to_string())
                } else {
                    line.to_string()
                }
            }
            ProtocolParser::Csv => {
                let fields: Vec<&str> = line.split(csv_delimiter).collect();
                serde_json::to_string(&fields).unwrap_or_else(|_| line.to_string())
            }
            ProtocolParser::Custom => line.to_string(),
        }
    }
}

#[async_trait]
impl Tentacle for SerialTentacle {
    fn name(&self) -> &str {
        &self.name
    }

    fn r#type(&self) -> &str {
        "serial"
    }

    async fn start(&self, event_bus: Arc<EventBus>) -> Result<()> {
        info!(
            "Serial port tentacle '{}' started with {} port(s)",
            self.name,
            self.config.ports.len()
        );

        // Jeden Port in einem eigenen Task starten
        for port_config in &self.config.ports {
            let event_bus_clone = Arc::clone(&event_bus);
            let port_cfg = port_config.clone();
            let name = self.name.clone();
            let reconnect_delay = self.config.reconnect_delay;
            let buffer_size = self.config.buffer_size;
            let parser = port_cfg.parser.clone();
            let csv_delimiter = port_cfg.csv_delimiter.clone();
            let topic = self.resolve_topic(port_config);
            let delimiter = port_cfg.delimiter.clone();

            tokio::spawn(async move {
                loop {
                    info!(
                        "Serial tentacle '{}' opening port: {} at {} baud",
                        name, port_cfg.port, port_cfg.baud_rate
                    );

                    let result = (|| async {
                        let port = serial2_tokio::SerialPort::open(&port_cfg.port, port_cfg.baud_rate)
                            .with_context(|| format!("Failed to open serial port: {}", port_cfg.port))?;

                    info!(
                        "Serial tentacle '{}' connected to {}",
                        name, port_cfg.port
                    );

                    let mut buffer = Vec::new();
                    let delimiter_bytes = match &delimiter {
                        Some(hex) => {
                            let mut bytes = Vec::new();
                            for i in (0..hex.len()).step_by(2) {
                                if i + 1 < hex.len() {
                                    let _ = u8::from_str_radix(&hex[i..i + 2], 16).map(|b| bytes.push(b));
                                }
                            }
                            bytes
                        }
                        None => vec![b'\n'],
                    };

                    let mut read_buf = [0u8; 1];
                    loop {
                        match port.read(&mut read_buf).await {
                            Ok(n) if n > 0 => {
                                let byte = read_buf[0];
                                if delimiter_bytes.len() == 1 && byte == delimiter_bytes[0] {
                                    if !buffer.is_empty() {
                                        let line = String::from_utf8_lossy(&buffer).to_string();
                                        buffer.clear();
                                        process_line_inner(&line, &topic, &parser, &csv_delimiter, &port_cfg, &name, &event_bus_clone);
                                    }
                                } else if delimiter_bytes.len() > 1 {
                                    buffer.push(byte);
                                    if buffer.len() >= delimiter_bytes.len() {
                                        let start = buffer.len() - delimiter_bytes.len();
                                        if buffer[start..] == delimiter_bytes[..] {
                                            let line_bytes: Vec<u8> = buffer.drain(..start).collect();
                                            let line = String::from_utf8_lossy(&line_bytes).to_string();
                                            process_line_inner(&line, &topic, &parser, &csv_delimiter, &port_cfg, &name, &event_bus_clone);
                                        }
                                    }
                                } else {
                                    buffer.push(byte);
                                }

                                if buffer.len() > buffer_size * 10 {
                                    warn!(
                                        "Serial tentacle '{}' buffer overflow on {}, discarding",
                                        name, port_cfg.port
                                    );
                                    buffer.clear();
                                }
                            }
                            Ok(_) => {
                                warn!(
                                    "Serial tentacle '{}' port {} returned 0 bytes",
                                    name, port_cfg.port
                                );
                                break;
                            }
                            Err(e) => {
                                error!(
                                    "Serial tentacle '{}' read error on {}: {}",
                                    name, port_cfg.port, e
                                );
                                if !buffer.is_empty() {
                                    let line = String::from_utf8_lossy(&buffer).to_string();
                                    buffer.clear();
                                    process_line_inner(&line, &topic, &parser, &csv_delimiter, &port_cfg, &name, &event_bus_clone);
                                }
                                break;
                            }
                        }
                    }

                    Ok::<_, anyhow::Error>(())
                })().await;

                    match result {
                        Ok(()) => {
                            info!(
                                "Serial tentacle '{}' port {} disconnected cleanly",
                                name, port_cfg.port
                            );
                        }
                        Err(e) => {
                            error!(
                                "Serial tentacle '{}' port {} error: {}",
                                name, port_cfg.port, e
                            );
                        }
                    }
                    warn!(
                        "Serial tentacle '{}' reconnecting to {} in {}s...",
                        name, port_cfg.port, reconnect_delay
                    );
                    tokio::time::sleep(Duration::from_secs(reconnect_delay)).await;
                }
            });
        }

        // Blockiere den Tentacle-Task
        loop {
            tokio::time::sleep(Duration::from_secs(60)).await;
        }
    }
}

/// Helper function for spawned tasks
fn process_line_inner(
    line: &str,
    topic: &str,
    parser: &ProtocolParser,
    csv_delimiter: &str,
    port_config: &SerialPortConfig,
    name: &str,
    event_bus: &Arc<EventBus>,
) {
    let trimmed = line.trim();
    if trimmed.is_empty() {
        return;
    }

    let body = match parser {
        ProtocolParser::Plain => trimmed.to_string(),
        ProtocolParser::Json => {
            if let Ok(val) = serde_json::from_str::<serde_json::Value>(trimmed) {
                serde_json::to_string(&val).unwrap_or_else(|_| trimmed.to_string())
            } else {
                trimmed.to_string()
            }
        }
        ProtocolParser::Csv => {
            let fields: Vec<&str> = trimmed.split(csv_delimiter).collect();
            serde_json::to_string(&fields).unwrap_or_else(|_| trimmed.to_string())
        }
        ProtocolParser::Custom => trimmed.to_string(),
    };

    let final_topic = if trimmed.starts_with('/') && matches!(parser, ProtocolParser::Plain) {
        trimmed.to_string()
    } else {
        topic.to_string()
    };

    let mut ink = Ink::new(final_topic.clone(), "serial", name, body);

    let metadata = serde_json::json!({
        "port": port_config.port,
        "baud_rate": port_config.baud_rate,
        "parser": format!("{:?}", parser),
    });
    ink = ink.with_metadata(metadata);

    debug!(
        "Serial tentacle '{}' publishing ink on topic: {}",
        name, final_topic
    );
    event_bus.publish(ink);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_tentacle() -> SerialTentacle {
        SerialTentacle::new(
            "test_serial".to_string(),
            SerialTentacleConfig {
                ports: vec![SerialPortConfig {
                    port: "/dev/ttyUSB0".to_string(),
                    baud_rate: 9600,
                    data_bits: 8,
                    parity: "none".to_string(),
                    stop_bits: 1,
                    topic: "/serial/test".to_string(),
                    parser: ProtocolParser::Plain,
                    delimiter: None,
                    csv_delimiter: ",".to_string(),
                }],
                reconnect_delay: 5,
                buffer_size: 4096,
                port_topic_mapping: HashMap::new(),
            },
        )
    }

    #[test]
    fn test_tentacle_name_and_type() {
        let tentacle = create_tentacle();
        assert_eq!(tentacle.name(), "test_serial");
        assert_eq!(tentacle.r#type(), "serial");
    }

    #[test]
    fn test_resolve_topic_from_config() {
        let tentacle = create_tentacle();
        let port_config = &tentacle.config.ports[0];
        assert_eq!(tentacle.resolve_topic(port_config), "/serial/test");
    }

    #[test]
    fn test_resolve_topic_from_mapping() {
        let mut mapping = HashMap::new();
        mapping.insert("/dev/ttyUSB0".to_string(), "/serial/mapped".to_string());

        let tentacle = SerialTentacle::new(
            "test_serial".to_string(),
            SerialTentacleConfig {
                ports: vec![SerialPortConfig {
                    port: "/dev/ttyUSB0".to_string(),
                    baud_rate: 9600,
                    data_bits: 8,
                    parity: "none".to_string(),
                    stop_bits: 1,
                    topic: "/serial/test".to_string(),
                    parser: ProtocolParser::Plain,
                    delimiter: None,
                    csv_delimiter: ",".to_string(),
                }],
                reconnect_delay: 5,
                buffer_size: 4096,
                port_topic_mapping: mapping,
            },
        );

        let port_config = &tentacle.config.ports[0];
        assert_eq!(tentacle.resolve_topic(port_config), "/serial/mapped");
    }

    #[test]
    fn test_parse_plain() {
        let tentacle = create_tentacle();
        let result = tentacle.parse_line("hello world", &ProtocolParser::Plain, ",");
        assert_eq!(result, "hello world");
    }

    #[test]
    fn test_parse_json() {
        let tentacle = create_tentacle();
        let result = tentacle.parse_line(r#"{"key":"value"}"#, &ProtocolParser::Json, ",");
        assert_eq!(result, r#"{"key":"value"}"#);
    }

    #[test]
    fn test_parse_csv() {
        let tentacle = create_tentacle();
        let result = tentacle.parse_line("a,b,c", &ProtocolParser::Csv, ",");
        assert_eq!(result, r#"["a","b","c"]"#);
    }

    #[test]
    fn test_parse_csv_custom_delimiter() {
        let tentacle = create_tentacle();
        let result = tentacle.parse_line("a;b;c", &ProtocolParser::Csv, ";");
        assert_eq!(result, r#"["a","b","c"]"#);
    }

    #[test]
    fn test_parse_custom() {
        let tentacle = create_tentacle();
        let result = tentacle.parse_line("custom data", &ProtocolParser::Custom, ",");
        assert_eq!(result, "custom data");
    }

    #[test]
    fn test_config_defaults() {
        let config: SerialTentacleConfig = serde_yaml::from_str(
            r#"
            ports:
              - port: "/dev/ttyS0"
        "#,
        )
        .unwrap();

        assert_eq!(config.ports[0].port, "/dev/ttyS0");
        assert_eq!(config.ports[0].baud_rate, 9600);
        assert_eq!(config.ports[0].data_bits, 8);
        assert_eq!(config.ports[0].parity, "none");
        assert_eq!(config.ports[0].stop_bits, 1);
        assert_eq!(config.ports[0].topic, "/serial/default");
        assert_eq!(config.reconnect_delay, 5);
        assert_eq!(config.buffer_size, 4096);
    }

    #[test]
    fn test_config_with_all_options() {
        let config: SerialTentacleConfig = serde_yaml::from_str(
            r#"
            ports:
              - port: "COM3"
                baud_rate: 115200
                data_bits: 7
                parity: "even"
                stop_bits: 2
                topic: "/serial/com3"
                parser: "csv"
                csv_delimiter: ";"
                delimiter: "0d0a"
            reconnect_delay: 10
            buffer_size: 8192
            port_topic_mapping:
              "COM1": "/serial/com1"
        "#,
        )
        .unwrap();

        assert_eq!(config.ports[0].port, "COM3");
        assert_eq!(config.ports[0].baud_rate, 115200);
        assert_eq!(config.ports[0].data_bits, 7);
        assert_eq!(config.ports[0].parity, "even");
        assert_eq!(config.ports[0].stop_bits, 2);
        assert_eq!(config.ports[0].parser, ProtocolParser::Csv);
        assert_eq!(config.ports[0].csv_delimiter, ";");
        assert_eq!(config.ports[0].delimiter, Some("0d0a".to_string()));
        assert_eq!(config.reconnect_delay, 10);
        assert_eq!(config.buffer_size, 8192);
        assert_eq!(
            config.port_topic_mapping.get("COM1"),
            Some(&"/serial/com1".to_string())
        );
    }

    #[test]
    fn test_invalid_json_falls_back_to_plain() {
        let tentacle = create_tentacle();
        let result = tentacle.parse_line("{invalid json}", &ProtocolParser::Json, ",");
        assert_eq!(result, "{invalid json}");
    }
}
