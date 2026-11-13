use anyhow::Result;
use async_trait::async_trait;
use pcap::{Capture, Device, Linktype};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info};

use super::Tentacle;
use crate::eventbus::EventBus;
use crate::model::Ink;

/// Protokollfilter für die Paketanalyse
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "lowercase")]
pub enum PacketProtocol {
    #[default]
    All,
    Tcp,
    Udp,
    Icmp,
    Arp,
    Dns,
    Http,
    Https,
}

/// Topic-Erkennungsmodus für Pakete
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "lowercase")]
pub enum PacketTopicMode {
    /// Topic aus dem Protokoll ableiten
    #[default]
    Protocol,
    /// Topic aus Port-Nummern ableiten
    Port,
    /// Topic aus IP-Adressen ableiten
    Ip,
    /// Alle Pakete an ein festes Topic
    All,
}

/// Konfiguration für den Network Packet Capture Tentacle
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PacketCaptureConfig {
    /// Netzwerk-Schnittstelle (z.B. "eth0", "wlan0", "any")
    #[serde(default = "default_interface")]
    pub interface: String,
    /// BPF-Filter-String (z.B. "tcp port 80", "host 192.168.1.1")
    #[serde(default)]
    pub filter: Option<String>,
    /// Snapshot-Länge in Bytes (Standard: 65535)
    #[serde(default = "default_snaplen")]
    pub snaplen: u32,
    /// Promiscuous-Modus aktivieren
    #[serde(default = "default_promisc")]
    pub promisc: bool,
    /// Timeout in Millisekunden (0 = kein Timeout)
    #[serde(default = "default_timeout")]
    pub timeout_ms: i32,
    /// Protokolle die erfasst werden sollen
    #[serde(default)]
    pub protocols: Vec<PacketProtocol>,
    /// Topic-Erkennungsmodus
    #[serde(default)]
    pub topic_mode: PacketTopicMode,
    /// Fallback-Topic
    #[serde(default = "default_fallback_topic")]
    pub fallback_topic: String,
    /// Optional: Port-basiertes Topic-Mapping
    #[serde(default)]
    pub port_topic_mapping: HashMap<u16, String>,
    /// Optional: IP-basiertes Topic-Mapping
    #[serde(default)]
    pub ip_topic_mapping: HashMap<String, String>,
    /// Maximale Pakete pro Sekunde (Rate-Limiting, 0 = unbegrenzt)
    #[serde(default)]
    pub max_packets_per_second: u32,
    /// Buffer-Größe in KB (Standard: 2MB)
    #[serde(default = "default_buffer_size")]
    pub buffer_size_kb: u32,
}

fn default_interface() -> String {
    "any".to_string()
}

fn default_snaplen() -> u32 {
    65535
}

fn default_promisc() -> bool {
    true
}

fn default_timeout() -> i32 {
    1000
}

fn default_fallback_topic() -> String {
    "/network/packet".to_string()
}

fn default_buffer_size() -> u32 {
    2048
}

pub struct PacketCaptureTentacle {
    name: String,
    config: PacketCaptureConfig,
}

impl PacketCaptureTentacle {
    pub fn new(name: String, config: PacketCaptureConfig) -> Self {
        Self { name, config }
    }
}

/// Freistehende Funktion: Bestimmt das Topic basierend auf dem Paket
fn resolve_topic(
    config: &PacketCaptureConfig,
    protocol: &str,
    src_port: u16,
    dst_port: u16,
    src_ip: &str,
    dst_ip: &str,
) -> String {
    match config.topic_mode {
        PacketTopicMode::Protocol => {
            format!("/network/{}", protocol)
        }
        PacketTopicMode::Port => {
            if let Some(topic) = config.port_topic_mapping.get(&dst_port) {
                return topic.clone();
            }
            if let Some(topic) = config.port_topic_mapping.get(&src_port) {
                return topic.clone();
            }
            format!("/network/port/{}", dst_port)
        }
        PacketTopicMode::Ip => {
            if let Some(topic) = config.ip_topic_mapping.get(dst_ip) {
                return topic.clone();
            }
            if let Some(topic) = config.ip_topic_mapping.get(src_ip) {
                return topic.clone();
            }
            format!("/network/ip/{}", dst_ip)
        }
        PacketTopicMode::All => config.fallback_topic.clone(),
    }
}

/// Freistehende Funktion: Extrahiert Protokollinformationen aus einem Paket
fn parse_packet(packet: &[u8], linktype: Linktype) -> (String, u16, u16, String, String) {
    let ip_start = match linktype {
        Linktype::ETHERNET => 14,
        _ => 0,
    };

    if ip_start >= packet.len() {
        return (
            "unknown".to_string(),
            0,
            0,
            "0.0.0.0".to_string(),
            "0.0.0.0".to_string(),
        );
    }

    let ip_packet = &packet[ip_start..];
    if ip_packet.len() < 20 {
        return (
            "unknown".to_string(),
            0,
            0,
            "0.0.0.0".to_string(),
            "0.0.0.0".to_string(),
        );
    }

    let version = (ip_packet[0] >> 4) & 0xf;
    if version != 4 && version != 6 {
        return (
            "unknown".to_string(),
            0,
            0,
            "0.0.0.0".to_string(),
            "0.0.0.0".to_string(),
        );
    }

    let ihl = ((ip_packet[0] & 0xf) as usize) * 4;
    if ihl < 20 || ip_packet.len() < ihl {
        return (
            "unknown".to_string(),
            0,
            0,
            "0.0.0.0".to_string(),
            "0.0.0.0".to_string(),
        );
    }

    let protocol_num = ip_packet[9];
    let (protocol, transport_start) = match protocol_num {
        6 => ("tcp", ihl),
        17 => ("udp", ihl),
        1 => ("icmp", ihl),
        _ => {
            return (
                "other".to_string(),
                0,
                0,
                "0.0.0.0".to_string(),
                "0.0.0.0".to_string(),
            );
        }
    };

    let src_ip = format!(
        "{}.{}.{}.{}",
        ip_packet[12], ip_packet[13], ip_packet[14], ip_packet[15]
    );
    let dst_ip = format!(
        "{}.{}.{}.{}",
        ip_packet[16], ip_packet[17], ip_packet[18], ip_packet[19]
    );

    let (src_port, dst_port) = if ip_packet.len() > transport_start + 1 {
        match protocol_num {
            6 | 17 => {
                let sp = u16::from_be_bytes([
                    ip_packet[transport_start],
                    ip_packet[transport_start + 1],
                ]);
                let dp = u16::from_be_bytes([
                    ip_packet[transport_start + 2],
                    ip_packet[transport_start + 3],
                ]);
                (sp, dp)
            }
            _ => (0, 0),
        }
    } else {
        (0, 0)
    };

    (protocol.to_string(), src_port, dst_port, src_ip, dst_ip)
}

/// Freistehende Funktion: Prüft ob ein Protokoll erfasst werden soll
fn should_capture(config: &PacketCaptureConfig, protocol: &str) -> bool {
    if config.protocols.is_empty() {
        return true;
    }

    for p in &config.protocols {
        match p {
            PacketProtocol::All => return true,
            PacketProtocol::Tcp => {
                if protocol == "tcp" {
                    return true;
                }
            }
            PacketProtocol::Udp => {
                if protocol == "udp" {
                    return true;
                }
            }
            PacketProtocol::Icmp => {
                if protocol == "icmp" {
                    return true;
                }
            }
            PacketProtocol::Arp => {
                if protocol == "arp" {
                    return true;
                }
            }
            PacketProtocol::Dns => {
                if protocol == "udp" || protocol == "tcp" {
                    return true;
                }
            }
            PacketProtocol::Http => {
                if protocol == "tcp" {
                    return true;
                }
            }
            PacketProtocol::Https => {
                if protocol == "tcp" {
                    return true;
                }
            }
        }
    }
    false
}

#[async_trait]
impl Tentacle for PacketCaptureTentacle {
    fn name(&self) -> &str {
        &self.name
    }

    fn r#type(&self) -> &str {
        "packet_capture"
    }

    async fn start(&self, event_bus: Arc<EventBus>) -> Result<()> {
        info!(
            "Packet Capture Tentacle '{}' starting on interface '{}'",
            self.name, self.config.interface
        );

        let config = self.config.clone();
        let tentacle_name = self.name.clone();
        let event_bus_clone = Arc::clone(&event_bus);

        // Rate-Limiting
        let max_pps = config.max_packets_per_second;
        let mut last_packet_time = std::time::Instant::now();
        let mut packet_count = 0u32;

        // Finde das Gerät
        let device = if config.interface == "any" {
            Device::lookup()
                .unwrap_or(Some(Device {
                    name: "any".to_string(),
                    desc: None,
                    addresses: Vec::new(),
                    flags: pcap::DeviceFlags::empty(),
                }))
                .expect("Default device should always be available")
        } else {
            let devices = pcap::Device::list()
                .map_err(|e| anyhow::anyhow!("Failed to list devices: {}", e))?;
            devices
                .into_iter()
                .find(|d| d.name == config.interface)
                .ok_or_else(|| anyhow::anyhow!("Interface '{}' not found", config.interface))?
        };

        info!(
            "Packet Capture Tentacle '{}': using device '{}'",
            tentacle_name, device.name
        );

        // Erstelle Capture-Instanz
        let mut cap = Capture::from_device(device)
            .map_err(|e| anyhow::anyhow!("Failed to create capture: {}", e))?
            .snaplen(config.snaplen as i32)
            .promisc(config.promisc)
            .immediate_mode(true)
            .open()
            .map_err(|e| anyhow::anyhow!("Failed to open capture: {}", e))?;

        // BPF-Filter setzen
        if let Some(ref filter_str) = config.filter {
            cap.filter(filter_str, true)
                .map_err(|e| anyhow::anyhow!("Failed to set filter '{}': {}", filter_str, e))?;
            info!(
                "Packet Capture Tentacle '{}': applied filter '{}'",
                tentacle_name, filter_str
            );
        }

        let linktype = cap.get_datalink();

        info!(
            "Packet Capture Tentacle '{}': linktype = {:?}",
            tentacle_name, linktype
        );

        // Paket-Schleife in einem separaten Thread
        std::thread::spawn(move || {
            loop {
                // Rate-Limiting
                if max_pps > 0 {
                    let now = std::time::Instant::now();
                    if now.duration_since(last_packet_time).as_secs() >= 1 {
                        packet_count = 0;
                        last_packet_time = now;
                    }
                    if packet_count >= max_pps {
                        std::thread::sleep(Duration::from_millis(100));
                        continue;
                    }
                    packet_count += 1;
                }

                match cap.next_packet() {
                    Ok(p) => {
                        let (protocol, src_port, dst_port, src_ip, dst_ip) =
                            parse_packet(p.data, linktype);

                        // Protokoll-Filter
                        if !should_capture(&config, &protocol) {
                            continue;
                        }

                        let topic =
                            resolve_topic(&config, &protocol, src_port, dst_port, &src_ip, &dst_ip);

                        // Paket-Daten als Hex-String
                        let hex_data: String = p
                            .data
                            .iter()
                            .take(256)
                            .map(|b| format!("{:02x}", b))
                            .collect();

                        debug!(
                            "Packet Capture Tentacle '{}': {} packet {}:{} -> {}:{} ({} bytes)",
                            tentacle_name,
                            protocol,
                            src_ip,
                            src_port,
                            dst_ip,
                            dst_port,
                            p.data.len()
                        );

                        // Metadaten erstellen
                        let mut metadata = serde_json::Map::new();
                        metadata.insert(
                            "protocol".to_string(),
                            serde_json::Value::String(protocol.clone()),
                        );
                        metadata.insert(
                            "src_ip".to_string(),
                            serde_json::Value::String(src_ip.clone()),
                        );
                        metadata.insert(
                            "dst_ip".to_string(),
                            serde_json::Value::String(dst_ip.clone()),
                        );
                        metadata.insert(
                            "src_port".to_string(),
                            serde_json::Value::Number(serde_json::Number::from(src_port as u64)),
                        );
                        metadata.insert(
                            "dst_port".to_string(),
                            serde_json::Value::Number(serde_json::Number::from(dst_port as u64)),
                        );
                        metadata.insert(
                            "packet_length".to_string(),
                            serde_json::Value::Number(
                                serde_json::Number::from(p.data.len() as u64),
                            ),
                        );
                        metadata
                            .insert("hex_data".to_string(), serde_json::Value::String(hex_data));

                        let mut ink = Ink::new(
                            &topic,
                            "packet_capture",
                            &tentacle_name,
                            format!("{}: {} bytes", protocol, p.data.len()),
                        );
                        ink = ink.with_metadata(serde_json::Value::Object(metadata));

                        event_bus_clone.publish(ink);
                    }
                    Err(e) => {
                        error!(
                            "Packet Capture Tentacle '{}': error reading packet: {}",
                            tentacle_name, e
                        );
                        std::thread::sleep(Duration::from_millis(100));
                    }
                }
            }
        });

        // Blockiere den Task, damit er läuft
        loop {
            tokio::time::sleep(Duration::from_secs(60)).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::appcontext::AppContext;

    fn make_config() -> PacketCaptureConfig {
        PacketCaptureConfig {
            interface: "eth0".to_string(),
            filter: None,
            snaplen: 65535,
            promisc: true,
            timeout_ms: 1000,
            protocols: vec![],
            topic_mode: PacketTopicMode::Protocol,
            fallback_topic: "/network/packet".to_string(),
            port_topic_mapping: HashMap::new(),
            ip_topic_mapping: HashMap::new(),
            max_packets_per_second: 0,
            buffer_size_kb: 2048,
        }
    }

    #[test]
    fn test_packet_topic_mode_protocol() {
        let config = make_config();
        assert_eq!(
            resolve_topic(&config, "tcp", 12345, 80, "192.168.1.1", "10.0.0.1"),
            "/network/tcp"
        );
        assert_eq!(
            resolve_topic(&config, "udp", 53, 12345, "10.0.0.1", "192.168.1.1"),
            "/network/udp"
        );
    }

    #[test]
    fn test_packet_topic_mode_port() {
        let mut config = make_config();
        config.topic_mode = PacketTopicMode::Port;

        assert_eq!(
            resolve_topic(&config, "tcp", 12345, 80, "192.168.1.1", "10.0.0.1"),
            "/network/port/80"
        );
    }

    #[test]
    fn test_packet_topic_mode_port_mapping() {
        let mut config = make_config();
        config.topic_mode = PacketTopicMode::Port;
        let mut mapping = HashMap::new();
        mapping.insert(80, "/http/requests".to_string());
        mapping.insert(443, "/https/requests".to_string());
        config.port_topic_mapping = mapping;

        assert_eq!(
            resolve_topic(&config, "tcp", 12345, 80, "192.168.1.1", "10.0.0.1"),
            "/http/requests"
        );
        assert_eq!(
            resolve_topic(&config, "tcp", 12345, 443, "192.168.1.1", "10.0.0.1"),
            "/https/requests"
        );
        // Kein Mapping
        assert_eq!(
            resolve_topic(&config, "tcp", 12345, 8080, "192.168.1.1", "10.0.0.1"),
            "/network/port/8080"
        );
    }

    #[test]
    fn test_packet_topic_mode_ip() {
        let mut config = make_config();
        config.topic_mode = PacketTopicMode::Ip;

        assert_eq!(
            resolve_topic(&config, "tcp", 12345, 80, "192.168.1.1", "10.0.0.1"),
            "/network/ip/10.0.0.1"
        );
    }

    #[test]
    fn test_packet_topic_mode_ip_mapping() {
        let mut config = make_config();
        config.topic_mode = PacketTopicMode::Ip;
        let mut mapping = HashMap::new();
        mapping.insert("10.0.0.1".to_string(), "/server/web".to_string());
        config.ip_topic_mapping = mapping;

        assert_eq!(
            resolve_topic(&config, "tcp", 12345, 80, "192.168.1.1", "10.0.0.1"),
            "/server/web"
        );
    }

    #[test]
    fn test_packet_topic_mode_all() {
        let mut config = make_config();
        config.topic_mode = PacketTopicMode::All;
        config.fallback_topic = "/network/all".to_string();

        assert_eq!(
            resolve_topic(&config, "tcp", 12345, 80, "192.168.1.1", "10.0.0.1"),
            "/network/all"
        );
    }

    #[test]
    fn test_should_capture_empty_protocols() {
        let config = make_config();
        assert!(should_capture(&config, "tcp"));
        assert!(should_capture(&config, "udp"));
        assert!(should_capture(&config, "icmp"));
    }

    #[test]
    fn test_should_capture_specific_protocol() {
        let mut config = make_config();
        config.protocols = vec![PacketProtocol::Tcp];

        assert!(should_capture(&config, "tcp"));
        assert!(!should_capture(&config, "udp"));
        assert!(!should_capture(&config, "icmp"));
    }

    #[test]
    fn test_should_capture_all_protocol() {
        let mut config = make_config();
        config.protocols = vec![PacketProtocol::All];

        assert!(should_capture(&config, "tcp"));
        assert!(should_capture(&config, "udp"));
        assert!(should_capture(&config, "icmp"));
    }

    #[test]
    fn test_packet_parse_packet_ethernet() {
        // Ethernet-Header (14 Bytes) + IPv4-Header (20 Bytes) + TCP-Header (20 Bytes)
        let mut packet = vec![0u8; 54];

        // Ethernet-Header
        packet[0..6].copy_from_slice(&[0x00, 0x11, 0x22, 0x33, 0x44, 0x55]); // Dest MAC
        packet[6..12].copy_from_slice(&[0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff]); // Src MAC
        packet[12] = 0x08; // EtherType IPv4
        packet[13] = 0x00;

        // IPv4-Header (starts at byte 14)
        packet[14] = 0x45; // Version 4, IHL 5
        packet[23] = 0x06; // Protocol TCP
        packet[26] = 192; // Src IP: 192.168.1.1
        packet[27] = 168;
        packet[28] = 1;
        packet[29] = 1;
        packet[30] = 10; // Dst IP: 10.0.0.1
        packet[31] = 0;
        packet[32] = 0;
        packet[33] = 1;

        // TCP-Header (starts at byte 34 = 14 + 20)
        packet[34] = 0x30; // Src Port high (12345 = 0x3039)
        packet[35] = 0x39; // Src Port low
        packet[36] = 0x00; // Dst Port high (80 = 0x0050)
        packet[37] = 0x50; // Dst Port low

        let (protocol, src_port, dst_port, src_ip, dst_ip) =
            parse_packet(&packet, Linktype::ETHERNET);

        assert_eq!(protocol, "tcp");
        assert_eq!(src_port, 12345);
        assert_eq!(dst_port, 80);
        assert_eq!(src_ip, "192.168.1.1");
        assert_eq!(dst_ip, "10.0.0.1");
    }

    #[test]
    fn test_packet_parse_packet_udp() {
        let mut packet = vec![0u8; 42];

        // Ethernet-Header
        packet[12] = 0x08;
        packet[13] = 0x00;

        // IPv4-Header (starts at byte 14)
        packet[14] = 0x45;
        packet[23] = 0x11; // Protocol UDP
        packet[26] = 192;
        packet[27] = 168;
        packet[28] = 1;
        packet[29] = 100;
        packet[30] = 8;
        packet[31] = 8;
        packet[32] = 8;
        packet[33] = 8;

        // UDP-Header (starts at byte 34 = 14 + 20)
        packet[34] = 0x00; // Src Port 53
        packet[35] = 0x35;
        packet[36] = 0x04; // Dst Port 1234 = 0x04D2
        packet[37] = 0xD2;

        let (protocol, src_port, dst_port, src_ip, dst_ip) =
            parse_packet(&packet, Linktype::ETHERNET);

        assert_eq!(protocol, "udp");
        assert_eq!(src_port, 53);
        assert_eq!(dst_port, 1234);
        assert_eq!(src_ip, "192.168.1.100");
        assert_eq!(dst_ip, "8.8.8.8");
    }

    #[test]
    fn test_packet_config_deserialization() {
        let yaml = r#"
        interface: "eth0"
        filter: "tcp port 80 or udp port 53"
        snaplen: 65535
        promisc: true
        timeout_ms: 1000
        protocols:
          - tcp
          - udp
        topic_mode: port
        fallback_topic: "/network/captured"
        port_topic_mapping:
          80: "/http/requests"
          443: "/https/requests"
          53: "/dns/queries"
        ip_topic_mapping:
          "10.0.0.1": "/server/web"
        max_packets_per_second: 100
        buffer_size_kb: 4096
        "#;

        let config: PacketCaptureConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.interface, "eth0");
        assert_eq!(config.filter.as_deref(), Some("tcp port 80 or udp port 53"));
        assert_eq!(config.protocols.len(), 2);
        assert_eq!(config.topic_mode, PacketTopicMode::Port);
        assert_eq!(config.max_packets_per_second, 100);
        assert_eq!(config.buffer_size_kb, 4096);
    }

    #[test]
    fn test_packet_tentacle_name_and_type() {
        let config = make_config();
        let tentacle = PacketCaptureTentacle::new("test-packet".to_string(), config);

        assert_eq!(tentacle.name(), "test-packet");
        assert_eq!(tentacle.r#type(), "packet_capture");
    }
}
