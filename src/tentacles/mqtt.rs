use anyhow::{Context, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info, warn};

use rumqttc::{AsyncClient, Event, MqttOptions, Packet, QoS, Transport};

use super::Tentacle;
use crate::eventbus::EventBus;
use crate::model::Ink;

/// Konfiguration für Last-Will-Messages
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MqttLastWillConfig {
    /// Topic, auf dem die Last-Will-Message veröffentlicht wird
    pub topic: String,
    /// Inhalt der Last-Will-Message
    pub message: String,
    /// QoS-Level (0, 1, 2; Standard: 0)
    #[serde(default = "default_qos")]
    pub qos: u8,
    /// Message soll vom Broker gespeichert und an neue Subscriber gesendet werden
    #[serde(default)]
    pub retain: bool,
}

fn default_qos() -> u8 {
    0
}

fn default_keep_alive() -> u64 {
    60
}

fn default_clean_session() -> bool {
    true
}

fn default_reconnect_delay() -> u64 {
    5
}

/// Konfiguration für TLS-Verbindung
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MqttTlsConfig {
    /// CA-Zertifikat-Pfad (optional, verwendet System-Root-CAs wenn nicht gesetzt)
    #[serde(default)]
    pub ca_file: Option<String>,
    /// Client-Zertifikat-Pfad für gegenseitige TLS-Authentifizierung (optional)
    #[serde(default)]
    pub client_cert: Option<String>,
    /// Client-Privatschlüssel-Pfad für gegenseitige TLS-Authentifizierung (optional)
    #[serde(default)]
    pub client_key: Option<String>,
    /// Hostname-Validierung deaktivieren (NUR für Testing!)
    #[serde(default)]
    pub disable_verification: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MqttTentacleConfig {
    /// MQTT-Broker-Host (z.B. "localhost" oder "broker.example.com")
    pub broker: String,
    /// MQTT-Broker-Port (Standard: 1883 für plain, 8883 für TLS)
    #[serde(default = "default_port")]
    pub port: u16,
    /// TLS verwenden (Standard: false)
    #[serde(default)]
    pub use_tls: bool,
    /// TLS-Konfiguration (optional, wird automatisch verwendet wenn use_tls: true)
    #[serde(default)]
    pub tls: Option<MqttTlsConfig>,
    /// Topics, auf die abonniert werden soll (unterstützt MQTT-Wildcards wie "+" und "#")
    pub topics: Vec<String>,
    /// Optional: Mapping von MQTT-Topics auf rustyKraken-Topics
    #[serde(default)]
    pub topic_mapping: Option<HashMap<String, String>>,
    /// Optional: Client-ID (wird automatisch generiert, wenn nicht gesetzt)
    #[serde(default)]
    pub client_id: Option<String>,
    /// Optional: Username für Authentifizierung
    #[serde(default)]
    pub username: Option<String>,
    /// Optional: Passwort für Authentifizierung
    #[serde(default)]
    pub password: Option<String>,
    /// Optional: QoS-Level für Subscriptions (0, 1, 2; Standard: 0)
    #[serde(default = "default_qos")]
    pub qos: u8,
    /// Optional: Keep-Alive in Sekunden (Standard: 60)
    #[serde(default = "default_keep_alive")]
    pub keep_alive: u64,
    /// Optional: Clean Session Flag (Standard: true)
    /// Wenn false, behält der Broker die Session und sendet verpasste Messages
    #[serde(default = "default_clean_session")]
    pub clean_session: bool,
    /// Optional: Last-Will-Message Konfiguration
    #[serde(default)]
    pub last_will: Option<MqttLastWillConfig>,
    /// Optional: Verzögerung bei Reconnection in Sekunden (Standard: 5)
    #[serde(default = "default_reconnect_delay")]
    pub reconnect_delay: u64,
}

fn default_port() -> u16 {
    1883
}

impl MqttTentacleConfig {
    /// Erstellt MqttOptions aus der Konfiguration
    fn build_mqtt_options(&self, client_id: &str) -> Result<MqttOptions> {
        let mut mqtt_options = MqttOptions::new(client_id, &self.broker, self.port);
        mqtt_options.set_keep_alive(Duration::from_secs(self.keep_alive));
        mqtt_options.set_clean_session(self.clean_session);

        // Authentifizierung
        if let Some(username) = &self.username {
            mqtt_options.set_credentials(username, self.password.clone().unwrap_or_default());
        }

        // TLS-Konfiguration
        if self.use_tls {
            let transport = self.build_tls_transport()?;
            mqtt_options.set_transport(transport);
        }

        // Last-Will-Message
        if let Some(ref last_will) = self.last_will {
            let qos = match last_will.qos {
                0 => QoS::AtMostOnce,
                1 => QoS::AtLeastOnce,
                2 => QoS::ExactlyOnce,
                _ => QoS::AtMostOnce,
            };

            mqtt_options.set_last_will(rumqttc::LastWill {
                topic: last_will.topic.clone(),
                message: last_will.message.clone().into(),
                qos,
                retain: last_will.retain,
            });
        }

        Ok(mqtt_options)
    }

    /// Erstellt den TLS-Transport
    fn build_tls_transport(&self) -> Result<Transport> {
        let tls_config = self.tls.as_ref();

        // Wenn Hostname-Validierung deaktiviert ist (NUR für Testing!)
        if let Some(tls) = tls_config
            && tls.disable_verification
        {
            warn!("MQTT: TLS-Hostname-Validierung ist deaktiviert! NUR für Testing verwenden!");
            return Ok(Transport::tls_with_config(create_no_verify_config().into()));
        }

        let mut root_cert_store = tokio_rustls::rustls::RootCertStore::empty();

        // System-Root-CAs laden
        for cert in rustls_native_certs::load_native_certs().certs {
            root_cert_store.add(cert).ok();
        }

        // Optionales CA-Zertifikat
        if let Some(tls) = tls_config
            && let Some(ca_file) = &tls.ca_file
        {
            let ca_bytes = std::fs::read(ca_file)
                .with_context(|| format!("CA-Zertifikat lesen fehlgeschlagen: {}", ca_file))?;

            let mut cursor = std::io::Cursor::new(ca_bytes);
            let certs = rustls_pemfile::certs(&mut cursor)
                .collect::<Result<Vec<_>, _>>()
                .context("CA-Zertifikat parsen fehlgeschlagen")?;

            for cert in certs {
                root_cert_store.add(cert).ok();
            }
            info!("MQTT: CA-Zertifikat aus {} geladen", ca_file);
        }

        let config_builder =
            tokio_rustls::rustls::ClientConfig::builder().with_root_certificates(root_cert_store);

        // Client-Zertifikat für gegenseitige TLS-Authentifizierung
        let config = if let Some(tls) = tls_config {
            if let (Some(client_cert), Some(client_key)) = (&tls.client_cert, &tls.client_key) {
                let cert_bytes = std::fs::read(client_cert).with_context(|| {
                    format!("Client-Zertifikat lesen fehlgeschlagen: {}", client_cert)
                })?;
                let key_bytes = std::fs::read(client_key).with_context(|| {
                    format!(
                        "Client-Privatschlüssel lesen fehlgeschlagen: {}",
                        client_key
                    )
                })?;

                let mut cert_cursor = std::io::Cursor::new(cert_bytes);
                let certs = rustls_pemfile::certs(&mut cert_cursor)
                    .collect::<Result<Vec<_>, _>>()
                    .context("Client-Zertifikat parsen fehlgeschlagen")?;

                let mut key_cursor = std::io::Cursor::new(key_bytes);
                let key = rustls_pemfile::private_key(&mut key_cursor)
                    .context("Client-Privatschlüssel parsen fehlgeschlagen")?
                    .context("Kein Privatschlüssel im Datei gefunden")?;

                info!("MQTT: Client-Zertifikat für mTLS geladen");
                config_builder
                    .with_client_auth_cert(certs, key)
                    .context("Client-Zertifikat-Konfiguration fehlgeschlagen")?
            } else {
                config_builder.with_no_client_auth()
            }
        } else {
            config_builder.with_no_client_auth()
        };

        Ok(Transport::tls_with_config(config.into()))
    }
}

/// Erstellt eine TLS-Config ohne Zertifikats-Verifikation (NUR für Testing!)
/// Verwendet Transport::tls_with_default_config() als Fallback, da rustls 0.23+
/// die dangerous() API entfernt hat. Für Testing mit selbstsignierten Zertifikaten
/// sollte stattdessen disable_verification: false verwendet und das Zertifikat
/// im System-Trust-Store hinzugefügt werden.
fn create_no_verify_config() -> tokio_rustls::rustls::ClientConfig {
    warn!(
        "MQTT: TLS-Verifikation kann in rustls 0.23+ nicht einfach deaktiviert werden. Verwende Standard-TLS mit System-Root-CAs."
    );

    let mut root_cert_store = tokio_rustls::rustls::RootCertStore::empty();
    for cert in rustls_native_certs::load_native_certs().certs {
        root_cert_store.add(cert).ok();
    }

    tokio_rustls::rustls::ClientConfig::builder()
        .with_root_certificates(root_cert_store)
        .with_no_client_auth()
}

pub struct MqttTentacle {
    name: String,
    config: MqttTentacleConfig,
}

impl MqttTentacle {
    pub fn new(name: String, config: MqttTentacleConfig) -> Self {
        Self { name, config }
    }

    /// Konvertiert QoS u8 zu rumqttc::QoS
    fn qos_from_u8(qos: u8) -> Result<QoS> {
        match qos {
            0 => Ok(QoS::AtMostOnce),
            1 => Ok(QoS::AtLeastOnce),
            2 => Ok(QoS::ExactlyOnce),
            _ => anyhow::bail!("Ungültiger QoS-Level: {} (muss 0, 1 oder 2 sein)", qos),
        }
    }

    /// Abonniert alle konfigurierten Topics
    #[allow(dead_code)]
    async fn subscribe_all(&self, client: &AsyncClient) -> Result<()> {
        let qos = Self::qos_from_u8(self.config.qos)?;

        for topic in &self.config.topics {
            info!("MQTT tentacle '{}' abonniert: {}", self.name, topic);
            client
                .subscribe(topic, qos)
                .await
                .with_context(|| format!("Fehler beim Abonnieren von '{}'", topic))?;
        }
        Ok(())
    }
}

#[async_trait]
impl Tentacle for MqttTentacle {
    fn name(&self) -> &str {
        &self.name
    }

    fn r#type(&self) -> &str {
        "mqtt"
    }

    async fn start(&self, event_bus: Arc<EventBus>) -> Result<()> {
        info!("Starte MQTT tentacle '{}'", self.name);

        let client_id = self
            .config
            .client_id
            .clone()
            .unwrap_or_else(|| format!("kraken-{}", self.name));

        let mqtt_options = self
            .config
            .build_mqtt_options(&client_id)
            .context("Fehler beim Erstellen der MQTT-Optionen")?;

        let (client, mut eventloop) = AsyncClient::new(mqtt_options, 100);

        let topic_mapping = self.config.topic_mapping.clone();
        let tentacle_name = self.name.clone();
        let reconnect_delay = Duration::from_secs(self.config.reconnect_delay);

        info!(
            "MQTT tentacle '{}' verbunden mit {}:{}",
            self.name, self.config.broker, self.config.port
        );

        // Eventloop – empfängt Nachrichten vom Broker
        loop {
            match eventloop.poll().await {
                Ok(Event::Incoming(Packet::Publish(pub_packet))) => {
                    let mqtt_topic = pub_packet.topic;
                    let payload = pub_packet.payload;
                    let qos = pub_packet.qos;
                    let retain = pub_packet.retain;

                    // MQTT-Topic auf rustyKraken-Topic mappen
                    let kraken_topic = map_mqtt_topic(&mqtt_topic, &topic_mapping);

                    // Payload als String konvertieren
                    let body = String::from_utf8_lossy(&payload).to_string();

                    // Ink erstellen
                    let ink = Ink::new(kraken_topic.clone(), "mqtt", &tentacle_name, body)
                        .with_metadata(serde_json::json!({
                            "mqtt_topic": mqtt_topic,
                            "mqtt_qos": qos as u8,
                            "mqtt_retain": retain,
                        }));

                    debug!(
                        "MQTT tentacle '{}' empfangen auf Topic '{}': {}",
                        self.name, mqtt_topic, kraken_topic
                    );
                    event_bus.publish(ink);
                }
                Ok(Event::Incoming(Packet::ConnAck(ack))) => {
                    info!(
                        "MQTT tentacle '{}' verbunden mit Broker (Session present: {})",
                        self.name, ack.session_present
                    );

                    // Nach Verbindung (oder Reconnection) Topics abonnieren
                    // Verwende try_subscribe, da wir im Eventloop-Thread sind
                    for topic in &self.config.topics {
                        let qos = Self::qos_from_u8(self.config.qos).unwrap_or(QoS::AtMostOnce);
                        if let Err(e) = client.try_subscribe(topic, qos) {
                            error!(
                                "MQTT tentacle '{}' Fehler beim Abonnieren von '{}': {}",
                                self.name, topic, e
                            );
                        }
                    }
                }
                Ok(Event::Incoming(Packet::SubAck(_))) => {
                    debug!("MQTT tentacle '{}' Subscription bestätigt", self.name);
                }
                Ok(_) => {
                    // Andere Events (PingResp, PubAck, etc.) ignorieren
                }
                Err(e) => {
                    error!(
                        "MQTT tentacle '{}' Verbindungsfehler: {}; reconnecte in {} Sekunden...",
                        self.name, e, self.config.reconnect_delay
                    );
                    tokio::time::sleep(reconnect_delay).await;

                    // Versuche, die Verbindung wiederherzustellen, indem wir den Eventloop fortsetzen
                    // rumqttc versucht automatisch, die Verbindung wiederherzustellen
                    continue;
                }
            }
        }
    }
}

/// Konvertiert einen MQTT-Topic in ein rustyKraken-Topic
fn map_mqtt_topic(mqtt_topic: &str, mapping: &Option<HashMap<String, String>>) -> String {
    if let Some(map) = mapping {
        // Exaktes Matching zuerst versuchen
        if let Some(mapped) = map.get(mqtt_topic) {
            return mapped.clone();
        }

        // Dann versuchen, Muster mit Wildcards zu matchen
        for (pattern, target) in map {
            if mqtt_topic_matches_pattern(mqtt_topic, pattern) {
                // Wildcards im Topic durch die tatsächlichen Werte ersetzen
                let result = substitute_wildcards(mqtt_topic, pattern, target);
                return result;
            }
        }
    }

    // Standard: MQTT-Topic als rustyKraken-Topic verwenden (mit führendem / falls nicht vorhanden)
    if mqtt_topic.starts_with('/') {
        mqtt_topic.to_string()
    } else {
        format!("/{}", mqtt_topic)
    }
}

/// Ersetzt MQTT-Wildcards (+ und #) im Target-Topic durch die tatsächlichen Werte
fn substitute_wildcards(mqtt_topic: &str, pattern: &str, target: &str) -> String {
    let mqtt_parts: Vec<&str> = mqtt_topic.split('/').collect();
    let pattern_parts: Vec<&str> = pattern.split('/').collect();

    let mut result = target.to_string();
    let mut mqtt_idx = 0;

    for pattern_part in pattern_parts.iter() {
        if *pattern_part == "+" && mqtt_idx < mqtt_parts.len() {
            // Ersetze das erste + im result mit dem MQTT-Wert
            if result.contains('+') {
                result = result.replacen("+", mqtt_parts[mqtt_idx], 1);
                mqtt_idx += 1;
            }
        } else if *pattern_part == "#" {
            // # matcht den Rest – den gesamten restlichen Pfad einfügen
            if mqtt_idx < mqtt_parts.len() {
                let remaining = mqtt_parts[mqtt_idx..].join("/");
                if result.contains('#') {
                    result = result.replacen("#", &remaining, 1);
                }
            }
            break; // # ist immer am Ende
        } else {
            // Kein Wildcard – MQTT-Index nur erhöhen, wenn die Segmente matchen
            if mqtt_idx < mqtt_parts.len() {
                mqtt_idx += 1;
            }
        }
    }

    result
}

/// Prüft, ob ein MQTT-Topic zu einem MQTT-Pattern passt
fn mqtt_topic_matches_pattern(topic: &str, pattern: &str) -> bool {
    let topic_parts: Vec<&str> = topic.split('/').collect();
    let pattern_parts: Vec<&str> = pattern.split('/').collect();

    // "+" matcht ein einzelnes Segment, "#" matcht alles ab dieser Stelle (inkl. leer)
    let mut topic_iter = topic_parts.iter().peekable();
    let mut pattern_iter = pattern_parts.iter().peekable();

    while let (Some(&t_part), Some(&p_part)) = (topic_iter.peek(), pattern_iter.peek()) {
        if *p_part == "#" {
            return true; // "#" matcht den Rest (auch wenn leer)
        }
        if *p_part == "+" {
            topic_iter.next();
            pattern_iter.next();
            continue;
        }
        if t_part != p_part {
            return false;
        }
        topic_iter.next();
        pattern_iter.next();
    }

    // Wenn Pattern noch "#" hat, matcht es (auch wenn Topic aufgebraucht ist)
    if let Some(p_part) = pattern_iter.peek() {
        return **p_part == "#";
    }

    // Beide müssen aufgebraucht sein
    topic_iter.peek().is_none()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::appcontext::AppContext;

    #[test]
    fn test_mqtt_topic_matches_pattern_exact() {
        assert!(mqtt_topic_matches_pattern(
            "kraken/warning",
            "kraken/warning"
        ));
        assert!(!mqtt_topic_matches_pattern(
            "kraken/warning",
            "kraken/error"
        ));
    }

    #[test]
    fn test_mqtt_topic_matches_pattern_plus_wildcard() {
        assert!(mqtt_topic_matches_pattern("kraken/warning", "kraken/+"));
        assert!(mqtt_topic_matches_pattern("home/lights/on", "home/+/on"));
        assert!(!mqtt_topic_matches_pattern("home/lights", "home/+/on"));
    }

    #[test]
    fn test_mqtt_topic_matches_pattern_hash_wildcard() {
        assert!(mqtt_topic_matches_pattern(
            "kraken/warning/critical",
            "kraken/#"
        ));
        assert!(mqtt_topic_matches_pattern("kraken", "kraken/#"));
        assert!(mqtt_topic_matches_pattern("a/b/c/d/e", "a/#"));
    }

    #[test]
    fn test_map_mqtt_topic_exact_mapping() {
        let mut mapping = HashMap::new();
        mapping.insert("kraken/warning".to_string(), "/kraken/warning".to_string());

        assert_eq!(
            map_mqtt_topic("kraken/warning", &Some(mapping.clone())),
            "/kraken/warning"
        );
        assert_eq!(
            map_mqtt_topic("unknown/topic", &Some(mapping)),
            "/unknown/topic"
        );
    }

    #[test]
    fn test_map_mqtt_topic_wildcard_mapping() {
        let mut mapping = HashMap::new();
        mapping.insert("home/+/status".to_string(), "/home/+/status".to_string());

        assert_eq!(
            map_mqtt_topic("home/lights/status", &Some(mapping)),
            "/home/lights/status"
        );
    }

    #[test]
    fn test_map_mqtt_topic_no_mapping() {
        assert_eq!(map_mqtt_topic("kraken/warning", &None), "/kraken/warning");
        assert_eq!(
            map_mqtt_topic("/already/prefixed", &None),
            "/already/prefixed"
        );
    }

    #[test]
    fn test_substitute_wildcards() {
        assert_eq!(
            substitute_wildcards("home/lights/on", "home/+/on", "/home/+/control"),
            "/home/lights/control"
        );
        assert_eq!(
            substitute_wildcards("a/b/c", "a/#", "/prefix/#"),
            "/prefix/b/c"
        );
    }
}
