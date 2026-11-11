use anyhow::{Context, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use snmp2::{MessageType, Pdu, Value};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::net::UdpSocket;
use tracing::{debug, error, info, warn};

use super::Tentacle;
use crate::eventbus::EventBus;
use crate::model::Ink;

/// SNMP-Version, die akzeptiert werden soll
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "lowercase")]
pub enum SnmpVersionConfig {
    V1,
    #[default]
    V2c,
    V3,
}

/// Konfiguration für das SNMP Trap Tentacle
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnmpTentacleConfig {
    /// UDP-Adresse, auf die gehört werden soll (z.B. "0.0.0.0:1162")
    #[serde(default = "default_listen_address")]
    pub listen_address: String,
    /// SNMP-Community-String für v2c (Standard: "public")
    #[serde(default = "default_community")]
    pub community: String,
    /// SNMP-Version (v1, v2c, v3; Standard: v2c)
    #[serde(default)]
    pub version: SnmpVersionConfig,
    /// Topic für Trap-Nachrichten
    #[serde(default = "default_topic")]
    pub topic: String,
    /// Optional: Mapping von OIDs auf Topics
    /// Wenn eine Trap eine dieser OIDs enthält, wird das zugehörige Topic verwendet
    #[serde(default)]
    pub oid_topic_mapping: HashMap<String, String>,
    /// Maximale Paketgröße in Bytes (Standard: 65535)
    #[serde(default = "default_max_packet_size")]
    pub max_packet_size: usize,
}

fn default_listen_address() -> String {
    "0.0.0.0:1162".to_string()
}

fn default_community() -> String {
    "public".to_string()
}

fn default_topic() -> String {
    "/snmp/trap".to_string()
}

fn default_max_packet_size() -> usize {
    65535
}

pub struct SnmpTentacle {
    name: String,
    config: SnmpTentacleConfig,
}

impl SnmpTentacle {
    pub fn new(name: String, config: SnmpTentacleConfig) -> Self {
        Self { name, config }
    }

    /// Bestimmt das Topic basierend auf der Trap OID
    fn resolve_topic(&self, trap_oid: &str) -> String {
        // Direktes OID-Mapping hat Vorrang
        if let Some(topic) = self.config.oid_topic_mapping.get(trap_oid) {
            return topic.clone();
        }

        // Prefix-Matching: längstes passendes Prefix verwenden
        let mut best_match: Option<(&String, &String)> = None;
        let mut best_len = 0;

        for (oid, topic) in &self.config.oid_topic_mapping {
            if trap_oid.starts_with(oid.as_str()) && oid.len() > best_len {
                best_match = Some((oid, topic));
                best_len = oid.len();
            }
        }

        if let Some((_, topic)) = best_match {
            return topic.clone();
        }

        // Fallback auf konfiguriertes Topic
        self.config.topic.clone()
    }

    /// Konvertiert einen SNMP-Wert in einen String
    fn snmp_value_to_string(value: &Value) -> String {
        match value {
            Value::Integer(i) => i.to_string(),
            Value::OctetString(bytes) => String::from_utf8_lossy(bytes).to_string(),
            Value::ObjectIdentifier(oid) => oid.to_string(),
            Value::IpAddress(ip) => format!("{}.{}.{}.{}", ip[0], ip[1], ip[2], ip[3]),
            Value::Counter32(c) => c.to_string(),
            Value::Counter64(c) => c.to_string(),
            Value::Unsigned32(g) => g.to_string(),
            Value::Timeticks(t) => t.to_string(),
            Value::Opaque(bytes) => format!("Opaque({})", bytes.len()),
            Value::Null => "null".to_string(),
            Value::NoSuchObject => "noSuchObject".to_string(),
            Value::NoSuchInstance => "noSuchInstance".to_string(),
            Value::Boolean(b) => b.to_string(),
            Value::EndOfMibView => "endOfMibView".to_string(),
            Value::Sequence(_) | Value::Set(_) | Value::Constructed(_, _) => {
                "constructed".to_string()
            }
            Value::GetRequest(_) => "GetRequest".to_string(),
            Value::GetNextRequest(_) => "GetNextRequest".to_string(),
            Value::GetBulkRequest(_) => "GetBulkRequest".to_string(),
            Value::Response(_) => "Response".to_string(),
            Value::SetRequest(_) => "SetRequest".to_string(),
            Value::InformRequest(_) => "InformRequest".to_string(),
            Value::Trap(_) => "Trap".to_string(),
            Value::Report(_) => "Report".to_string(),
        }
    }

    /// Parst eine SNMP Trap und erstellt ein Ink
    fn parse_trap(&self, data: &[u8]) -> Option<Ink> {
        // PDU parsen
        let pdu = match Pdu::from_bytes(data) {
            Ok(p) => p,
            Err(e) => {
                warn!("SNMP tentacle '{}' failed to parse PDU: {}", self.name, e);
                return None;
            }
        };

        // Trap-Typ bestimmen basierend auf message_type
        let trap_oid = match pdu.message_type {
            MessageType::TrapV1 => {
                // SNMPv1 Trap
                if let Some(ref v1_info) = pdu.v1_trap_info {
                    v1_info.enterprise.to_string()
                } else {
                    "unknown".to_string()
                }
            }
            MessageType::Trap | MessageType::InformRequest => {
                // SNMPv2c Trap oder Inform - erste Varbind OID verwenden
                "snmpTrapOID".to_string()
            }
            _ => {
                warn!(
                    "SNMP tentacle '{}' received non-trap PDU type: {:?}, ignoring",
                    self.name, pdu.message_type
                );
                return None;
            }
        };

        // Topic bestimmen
        let topic = self.resolve_topic(&trap_oid);

        // Varbinds als JSON serialisieren
        let mut varbind_map = serde_json::Map::new();
        for (oid, value) in pdu.varbinds {
            let oid_str = oid.to_string();
            let value_str = Self::snmp_value_to_string(&value);
            varbind_map.insert(oid_str, serde_json::Value::String(value_str));
        }

        let body = serde_json::to_string(&varbind_map)
            .unwrap_or_else(|_| format!("SNMP trap with {} varbinds", varbind_map.len()));

        // Metadata erstellen
        let metadata = serde_json::json!({
            "trap_oid": trap_oid,
            "version": match &self.config.version {
                SnmpVersionConfig::V1 => "v1",
                SnmpVersionConfig::V2c => "v2c",
                SnmpVersionConfig::V3 => "v3",
            },
            "community": String::from_utf8_lossy(pdu.community).to_string(),
            "message_type": format!("{:?}", pdu.message_type),
            "varbind_count": varbind_map.len(),
        });

        let ink = Ink::new(topic, "snmp", &self.name, body).with_metadata(metadata);
        Some(ink)
    }
}

#[async_trait]
impl Tentacle for SnmpTentacle {
    fn name(&self) -> &str {
        &self.name
    }

    fn r#type(&self) -> &str {
        "snmp"
    }

    async fn start(&self, event_bus: Arc<EventBus>) -> Result<()> {
        info!(
            "SNMP trap tentacle '{}' listening on {}",
            self.name, self.config.listen_address
        );

        let socket = UdpSocket::bind(&self.config.listen_address)
            .await
            .with_context(|| {
                format!(
                    "Failed to bind UDP socket to {}",
                    self.config.listen_address
                )
            })?;

        info!(
            "SNMP trap tentacle '{}' started on {}",
            self.name, self.config.listen_address
        );

        let mut buf = vec![0u8; self.config.max_packet_size];

        loop {
            match socket.recv(&mut buf).await {
                Ok(received) => {
                    let data = &buf[..received];
                    debug!("SNMP tentacle '{}' received {} bytes", self.name, received);

                    match self.parse_trap(data) {
                        Some(ink) => {
                            debug!(
                                "SNMP tentacle '{}' publishing trap on topic: {}",
                                self.name, ink.topic
                            );
                            event_bus.publish(ink);
                        }
                        None => {
                            debug!("SNMP tentacle '{}' dropped unparseable packet", self.name);
                        }
                    }
                }
                Err(e) => {
                    error!("SNMP tentacle '{}' UDP receive error: {}", self.name, e);
                    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_tentacle() -> SnmpTentacle {
        SnmpTentacle::new(
            "test_snmp".to_string(),
            SnmpTentacleConfig {
                listen_address: "0.0.0.0:1162".to_string(),
                community: "public".to_string(),
                version: SnmpVersionConfig::V2c,
                topic: "/snmp/trap".to_string(),
                oid_topic_mapping: HashMap::new(),
                max_packet_size: 65535,
            },
        )
    }

    #[test]
    fn test_tentacle_name_and_type() {
        let tentacle = create_tentacle();
        assert_eq!(tentacle.name(), "test_snmp");
        assert_eq!(tentacle.r#type(), "snmp");
    }

    #[test]
    fn test_resolve_topic_fallback() {
        let tentacle = create_tentacle();
        assert_eq!(tentacle.resolve_topic("1.3.6.1.4.1.99999.1"), "/snmp/trap");
    }

    #[test]
    fn test_resolve_topic_exact_match() {
        let mut mapping = HashMap::new();
        mapping.insert(
            "1.3.6.1.4.1.99999.1".to_string(),
            "/snmp/linkdown".to_string(),
        );

        let tentacle = SnmpTentacle::new(
            "test_snmp".to_string(),
            SnmpTentacleConfig {
                listen_address: "0.0.0.0:1162".to_string(),
                community: "public".to_string(),
                version: SnmpVersionConfig::V2c,
                topic: "/snmp/trap".to_string(),
                oid_topic_mapping: mapping,
                max_packet_size: 65535,
            },
        );

        assert_eq!(
            tentacle.resolve_topic("1.3.6.1.4.1.99999.1"),
            "/snmp/linkdown"
        );
    }

    #[test]
    fn test_resolve_topic_prefix_match() {
        let mut mapping = HashMap::new();
        mapping.insert("1.3.6.1.4.1.99999".to_string(), "/snmp/cisco".to_string());

        let tentacle = SnmpTentacle::new(
            "test_snmp".to_string(),
            SnmpTentacleConfig {
                listen_address: "0.0.0.0:1162".to_string(),
                community: "public".to_string(),
                version: SnmpVersionConfig::V2c,
                topic: "/snmp/trap".to_string(),
                oid_topic_mapping: mapping,
                max_packet_size: 65535,
            },
        );

        // Prefix match
        assert_eq!(
            tentacle.resolve_topic("1.3.6.1.4.1.99999.1.2.3"),
            "/snmp/cisco"
        );
        // No match
        assert_eq!(tentacle.resolve_topic("1.3.6.1.4.1.88888.1"), "/snmp/trap");
    }

    #[test]
    fn test_resolve_topic_longest_prefix() {
        let mut mapping = HashMap::new();
        mapping.insert("1.3.6.1.4.1.99999".to_string(), "/snmp/cisco".to_string());
        mapping.insert(
            "1.3.6.1.4.1.99999.1".to_string(),
            "/snmp/linkdown".to_string(),
        );

        let tentacle = SnmpTentacle::new(
            "test_snmp".to_string(),
            SnmpTentacleConfig {
                listen_address: "0.0.0.0:1162".to_string(),
                community: "public".to_string(),
                version: SnmpVersionConfig::V2c,
                topic: "/snmp/trap".to_string(),
                oid_topic_mapping: mapping,
                max_packet_size: 65535,
            },
        );

        // Should match the longer prefix
        assert_eq!(
            tentacle.resolve_topic("1.3.6.1.4.1.99999.1.2.3"),
            "/snmp/linkdown"
        );
        // Should match the shorter prefix
        assert_eq!(
            tentacle.resolve_topic("1.3.6.1.4.1.99999.2.3.4"),
            "/snmp/cisco"
        );
    }

    #[test]
    fn test_snmp_value_to_string_integer() {
        let value = Value::Integer(42);
        assert_eq!(SnmpTentacle::snmp_value_to_string(&value), "42");
    }

    #[test]
    fn test_snmp_value_to_string_octet_string() {
        let bytes = b"hello";
        let value = Value::OctetString(bytes);
        assert_eq!(SnmpTentacle::snmp_value_to_string(&value), "hello");
    }

    #[test]
    fn test_snmp_value_to_string_ip() {
        let value = Value::IpAddress([192, 168, 1, 1]);
        assert_eq!(SnmpTentacle::snmp_value_to_string(&value), "192.168.1.1");
    }

    #[test]
    fn test_snmp_value_to_string_counter32() {
        let value = Value::Counter32(1000);
        assert_eq!(SnmpTentacle::snmp_value_to_string(&value), "1000");
    }

    #[test]
    fn test_snmp_value_to_string_boolean() {
        let value = Value::Boolean(true);
        assert_eq!(SnmpTentacle::snmp_value_to_string(&value), "true");
    }

    #[test]
    fn test_snmp_value_to_string_null() {
        let value = Value::Null;
        assert_eq!(SnmpTentacle::snmp_value_to_string(&value), "null");
    }

    #[test]
    fn test_config_defaults() {
        let config: SnmpTentacleConfig = serde_yaml::from_str(
            r#"
            community: "private"
        "#,
        )
        .unwrap();

        assert_eq!(config.listen_address, "0.0.0.0:1162");
        assert_eq!(config.community, "private");
        assert_eq!(config.topic, "/snmp/trap");
        assert_eq!(config.max_packet_size, 65535);
    }

    #[test]
    fn test_config_with_oid_mapping() {
        let config: SnmpTentacleConfig = serde_yaml::from_str(
            r#"
            listen_address: "127.0.0.1:1162"
            community: "monitoring"
            version: "v2c"
            topic: "/snmp/default"
            oid_topic_mapping:
              "1.3.6.1.6.3.1.1.5.2": "/snmp/linkdown"
              "1.3.6.1.6.3.1.1.5.3": "/snmp/linkup"
            max_packet_size: 4096
        "#,
        )
        .unwrap();

        assert_eq!(config.listen_address, "127.0.0.1:1162");
        assert_eq!(config.community, "monitoring");
        assert_eq!(config.version, SnmpVersionConfig::V2c);
        assert_eq!(config.oid_topic_mapping.len(), 2);
        assert_eq!(config.max_packet_size, 4096);
    }
}
