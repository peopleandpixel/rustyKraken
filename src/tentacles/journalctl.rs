use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::time::{Duration, sleep};
use tracing::{debug, error, info};

use super::Tentacle;
use crate::eventbus::EventBus;
use crate::model::Ink;

/// Topic-Erkennungsmodus für Journalctl-Einträge
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "lowercase")]
pub enum JournalTopicMode {
    /// Topic aus dem SYSLOG_IDENTIFIER verwenden
    #[default]
    Identifier,
    /// Topic aus dem UNIT-Feld verwenden
    Unit,
    /// Topic aus der PRIORITY verwenden
    Priority,
    /// Alle Einträge an ein festes Topic
    All,
}

/// Konfiguration für den Journalctl Tentacle
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JournalTentacleConfig {
    /// Unit-Filter (z.B. "sshd", "docker")
    #[serde(default)]
    pub units: Vec<String>,
    /// Priority-Filter (z.B. "err", "warning", "info")
    #[serde(default)]
    pub priority: Option<String>,
    /// Topic-Erkennungsmodus
    #[serde(default)]
    pub topic_mode: JournalTopicMode,
    /// Fallback-Topic
    #[serde(default = "default_fallback_topic")]
    pub fallback_topic: String,
    /// Optional: Identifier-basiertes Topic-Mapping
    #[serde(default)]
    pub identifier_mapping: HashMap<String, String>,
    /// Optional: Unit-basiertes Topic-Mapping
    #[serde(default)]
    pub unit_mapping: HashMap<String, String>,
    /// Nur neue Einträge (seit Start)
    #[serde(default = "default_follow_new")]
    pub follow_new: bool,
    /// Seit welchem Zeitraum
    #[serde(default)]
    pub since: Option<String>,
    /// Batch-Größe (max Einträge pro Lesezyklus)
    #[serde(default = "default_batch_size")]
    pub batch_size: u32,
    /// Poll-Interval in Millisekunden
    #[serde(default = "default_poll_interval_ms")]
    pub poll_interval_ms: u64,
}

fn default_fallback_topic() -> String {
    "/systemd/journal/default".to_string()
}

fn default_follow_new() -> bool {
    true
}

fn default_batch_size() -> u32 {
    100
}

fn default_poll_interval_ms() -> u64 {
    1000
}

pub struct JournalTentacle {
    name: String,
    config: JournalTentacleConfig,
}

/// Freistehende Funktion: Bestimmt das Topic basierend auf dem Journal-Eintrag
fn resolve_topic(
    config: &JournalTentacleConfig,
    identifier: &str,
    unit: &str,
    priority: &str,
) -> String {
    match config.topic_mode {
        JournalTopicMode::Identifier => {
            if let Some(mapped) = config.identifier_mapping.get(identifier) {
                return mapped.clone();
            }
            if identifier.is_empty() {
                config.fallback_topic.clone()
            } else {
                format!("/systemd/journal/{}", identifier)
            }
        }
        JournalTopicMode::Unit => {
            if let Some(mapped) = config.unit_mapping.get(unit) {
                return mapped.clone();
            }
            if unit.is_empty() {
                config.fallback_topic.clone()
            } else {
                format!("/systemd/journal/unit/{}", unit)
            }
        }
        JournalTopicMode::Priority => {
            format!("/systemd/journal/priority/{}", priority)
        }
        JournalTopicMode::All => config.fallback_topic.clone(),
    }
}

/// Ein geparster Journal-Eintrag
type JournalEntry = (
    String,
    String,
    String,
    String,
    serde_json::Map<String, serde_json::Value>,
);

/// Parst einen Journal-Eintrag im JOURNAL_EXPORT-FORMAT
fn parse_journal_entry(line: &str) -> Option<JournalEntry> {
    // Format: # <monotonic timestamp>\n_PRIORITY=<p>\nSYSLOG_IDENTIFIER=<id>\n...
    if line.starts_with('#') {
        return None; // Kommentarzeile, wird im Hauptloop behandelt
    }

    let mut priority = String::new();
    let mut identifier = String::new();
    let mut unit = String::new();
    let mut message = String::new();
    let mut metadata = serde_json::Map::new();

    for field in line.split('\n') {
        if let Some((key, value)) = field.split_once('=') {
            match key {
                "_PRIORITY" => priority = value.to_string(),
                "SYSLOG_IDENTIFIER" => identifier = value.to_string(),
                "_SYSTEMD_UNIT" => unit = value.to_string(),
                "MESSAGE" => message = value.to_string(),
                _ => {
                    if key.starts_with('_') || key.starts_with("CODE_") {
                        metadata.insert(
                            key.to_string(),
                            serde_json::Value::String(value.to_string()),
                        );
                    }
                }
            }
        }
    }

    if message.is_empty() && identifier.is_empty() {
        None
    } else {
        Some((priority, identifier, unit, message, metadata))
    }
}

impl JournalTentacle {
    pub fn new(name: String, config: JournalTentacleConfig) -> Self {
        Self { name, config }
    }

    /// Baut den journalctl-Befehl
    fn build_command(&self) -> Vec<String> {
        let mut args = vec![
            "journalctl".to_string(),
            "--output=export".to_string(),
            "--no-pager".to_string(),
        ];

        if self.config.follow_new {
            args.push("-n".to_string());
            args.push("0".to_string());
        }

        if let Some(since) = &self.config.since {
            args.push("--since".to_string());
            args.push(since.clone());
        }

        for unit in &self.config.units {
            args.push("-u".to_string());
            args.push(unit.clone());
        }

        if let Some(priority) = &self.config.priority {
            args.push("-p".to_string());
            args.push(priority.clone());
        }

        args
    }
}

#[async_trait]
impl Tentacle for JournalTentacle {
    fn name(&self) -> &str {
        &self.name
    }

    fn r#type(&self) -> &str {
        "journalctl"
    }

    async fn start(&self, event_bus: Arc<EventBus>) -> Result<()> {
        info!(
            "Journalctl Tentacle '{}' starting, units: {:?}",
            self.name, self.config.units
        );

        let config = self.config.clone();
        let tentacle_name = self.name.clone();
        let event_bus_clone = Arc::clone(&event_bus);

        tokio::spawn(async move {
            let args = {
                let c = JournalTentacle::new(tentacle_name.clone(), config.clone());
                c.build_command()
            };

            // Starte journalctl mit -f für Follow-Modus
            let mut follow_args = args.clone();
            follow_args.push("-f".to_string());

            let mut child = match tokio::process::Command::new("journalctl")
                .args(&follow_args[1..])
                .stdout(std::process::Stdio::piped())
                .stderr(std::process::Stdio::piped())
                .spawn()
            {
                Ok(c) => c,
                Err(e) => {
                    error!(
                        "Journalctl Tentacle '{}': failed to start journalctl: {}",
                        tentacle_name, e
                    );
                    // Fallback: periodisches Polling
                    loop {
                        if let Ok(output) = tokio::process::Command::new("journalctl")
                            .args(&args[1..])
                            .output()
                            .await
                        {
                            process_journal_output(
                                &tentacle_name,
                                &config,
                                &event_bus_clone,
                                &String::from_utf8_lossy(&output.stdout),
                            );
                        }
                        sleep(Duration::from_millis(config.poll_interval_ms)).await;
                    }
                }
            };

            info!(
                "Journalctl Tentacle '{}': following journal output",
                tentacle_name
            );

            // Lese stdout zeilenweise
            if let Some(stdout) = child.stdout.take() {
                use tokio::io::{AsyncBufReadExt, BufReader};

                let mut reader = BufReader::new(stdout);
                let mut line = String::new();
                let mut current_entry = String::new();

                loop {
                    line.clear();
                    match reader.read_line(&mut line).await {
                        Ok(0) => break, // EOF
                        Ok(_) => {
                            let trimmed = line.trim_end();

                            if trimmed.starts_with('#') {
                                // Neue Eintrag beginnt
                                if !current_entry.is_empty() {
                                    process_journal_entry(
                                        &tentacle_name,
                                        &config,
                                        &event_bus_clone,
                                        &current_entry,
                                    );
                                    current_entry.clear();
                                }
                            } else if !trimmed.is_empty() {
                                if !current_entry.is_empty() {
                                    current_entry.push('\n');
                                }
                                current_entry.push_str(trimmed);
                            }
                        }
                        Err(e) => {
                            error!(
                                "Journalctl Tentacle '{}': error reading journal: {}",
                                tentacle_name, e
                            );
                            break;
                        }
                    }
                }

                // Letzten Eintrag verarbeiten
                if !current_entry.is_empty() {
                    process_journal_entry(
                        &tentacle_name,
                        &config,
                        &event_bus_clone,
                        &current_entry,
                    );
                }
            }

            // Warte auf Prozessende
            let _ = child.wait().await;
        });

        // Blockiere den Task, damit er läuft
        loop {
            sleep(Duration::from_secs(60)).await;
        }
    }
}

/// Freistehende Funktion: Verarbeitet einen Journal-Eintrag
fn process_journal_entry(
    tentacle_name: &str,
    config: &JournalTentacleConfig,
    event_bus: &Arc<EventBus>,
    entry: &str,
) {
    if let Some((priority, identifier, unit, message, mut metadata)) = parse_journal_entry(entry) {
        let topic = resolve_topic(config, &identifier, &unit, &priority);

        debug!(
            "Journalctl Tentacle '{}': received entry from '{}' (priority: {})",
            tentacle_name, identifier, priority
        );

        // Priority als Metadaten hinzufügen
        metadata.insert(
            "journal_priority".to_string(),
            serde_json::Value::String(priority),
        );
        if !identifier.is_empty() {
            metadata.insert(
                "journal_identifier".to_string(),
                serde_json::Value::String(identifier),
            );
        }
        if !unit.is_empty() {
            metadata.insert("journal_unit".to_string(), serde_json::Value::String(unit));
        }

        let mut ink = Ink::new(&topic, "journalctl", tentacle_name, message);
        ink = ink.with_metadata(serde_json::Value::Object(metadata));

        info!(
            "Journalctl Tentacle '{}': published ink on topic: {}",
            tentacle_name, topic
        );
        event_bus.publish(ink);
    }
}

/// Freistehende Funktion: Verarbeitet journalctl-Ausgabe (für Polling-Fallback)
fn process_journal_output(
    _tentacle_name: &str,
    _config: &JournalTentacleConfig,
    _event_bus: &Arc<EventBus>,
    output: &str,
) {
    // Für Polling-Modus: gleiche Logik wie process_journal_entry
    for line in output.lines() {
        if line.starts_with('#') {
            continue;
        }
        // Vereinfachte Verarbeitung für Polling
        if let Some((_priority, _identifier, _unit, _message, _metadata)) =
            parse_journal_entry(line)
        {
            // TODO: Implementiere vollständige Polling-Unterstützung
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_journal_topic_mode_identifier() {
        let config = JournalTentacleConfig {
            units: vec!["sshd".to_string()],
            priority: None,
            topic_mode: JournalTopicMode::Identifier,
            fallback_topic: "/systemd/journal/default".to_string(),
            identifier_mapping: HashMap::new(),
            unit_mapping: HashMap::new(),
            follow_new: true,
            since: None,
            batch_size: 100,
            poll_interval_ms: 1000,
        };

        assert_eq!(
            resolve_topic(&config, "sshd", "sshd.service", "6"),
            "/systemd/journal/sshd"
        );
        assert_eq!(
            resolve_topic(&config, "", "", "6"),
            "/systemd/journal/default"
        );
    }

    #[test]
    fn test_journal_topic_mode_unit() {
        let config = JournalTentacleConfig {
            units: vec!["docker".to_string()],
            priority: None,
            topic_mode: JournalTopicMode::Unit,
            fallback_topic: "/systemd/journal/default".to_string(),
            identifier_mapping: HashMap::new(),
            unit_mapping: HashMap::new(),
            follow_new: true,
            since: None,
            batch_size: 100,
            poll_interval_ms: 1000,
        };

        assert_eq!(
            resolve_topic(&config, "dockerd", "docker.service", "6"),
            "/systemd/journal/unit/docker.service"
        );
    }

    #[test]
    fn test_journal_topic_mode_priority() {
        let config = JournalTentacleConfig {
            units: vec![],
            priority: Some("err".to_string()),
            topic_mode: JournalTopicMode::Priority,
            fallback_topic: "/systemd/journal/default".to_string(),
            identifier_mapping: HashMap::new(),
            unit_mapping: HashMap::new(),
            follow_new: true,
            since: None,
            batch_size: 100,
            poll_interval_ms: 1000,
        };

        assert_eq!(
            resolve_topic(&config, "kernel", "system", "3"),
            "/systemd/journal/priority/3"
        );
    }

    #[test]
    fn test_journal_topic_mode_all() {
        let config = JournalTentacleConfig {
            units: vec![],
            priority: None,
            topic_mode: JournalTopicMode::All,
            fallback_topic: "/systemd/all".to_string(),
            identifier_mapping: HashMap::new(),
            unit_mapping: HashMap::new(),
            follow_new: true,
            since: None,
            batch_size: 100,
            poll_interval_ms: 1000,
        };

        assert_eq!(
            resolve_topic(&config, "anything", "anything.service", "0"),
            "/systemd/all"
        );
    }

    #[test]
    fn test_journal_identifier_mapping() {
        let mut mapping = HashMap::new();
        mapping.insert("sshd".to_string(), "/security/ssh".to_string());
        mapping.insert("kernel".to_string(), "/system/kernel".to_string());

        let config = JournalTentacleConfig {
            units: vec![],
            priority: None,
            topic_mode: JournalTopicMode::Identifier,
            fallback_topic: "/systemd/journal/default".to_string(),
            identifier_mapping: mapping,
            unit_mapping: HashMap::new(),
            follow_new: true,
            since: None,
            batch_size: 100,
            poll_interval_ms: 1000,
        };

        assert_eq!(
            resolve_topic(&config, "sshd", "sshd.service", "6"),
            "/security/ssh"
        );
        assert_eq!(
            resolve_topic(&config, "kernel", "system", "3"),
            "/system/kernel"
        );
        assert_eq!(
            resolve_topic(&config, "unknown", "unknown.service", "6"),
            "/systemd/journal/unknown"
        );
    }

    #[test]
    fn test_parse_journal_entry() {
        let entry = "_PRIORITY=3\nSYSLOG_IDENTIFIER=sshd\n_SYSTEMD_UNIT=sshd.service\nMESSAGE=Failed password for root";
        let result = parse_journal_entry(entry);

        assert!(result.is_some());
        let (priority, identifier, unit, message, metadata) = result.unwrap();
        assert_eq!(priority, "3");
        assert_eq!(identifier, "sshd");
        assert_eq!(unit, "sshd.service");
        assert_eq!(message, "Failed password for root");
        assert!(metadata.is_empty()); // Keine zusätzlichen Metadaten in diesem Eintrag
    }

    #[test]
    fn test_parse_journal_entry_with_metadata() {
        let entry = "_PRIORITY=6\nSYSLOG_IDENTIFIER=systemd\n_SYSTEMD_UNIT=systemd.service\nCODE_FILE=backlight.c\nMESSAGE=Loaded ACLs";
        let result = parse_journal_entry(entry);

        assert!(result.is_some());
        let (priority, identifier, _unit, message, metadata) = result.unwrap();
        assert_eq!(priority, "6");
        assert_eq!(identifier, "systemd");
        assert_eq!(message, "Loaded ACLs");
        assert!(metadata.contains_key("CODE_FILE"));
    }

    #[test]
    fn test_journal_config_deserialization() {
        let yaml = r#"
        units:
          - "sshd"
          - "docker"
        priority: "warning"
        topic_mode: identifier
        fallback_topic: "/systemd/journal/default"
        identifier_mapping:
          "sshd": "/security/ssh"
          "docker": "/containers/docker"
        follow_new: true
        since: "1 hour ago"
        batch_size: 50
        poll_interval_ms: 500
        "#;

        let config: JournalTentacleConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.units.len(), 2);
        assert_eq!(config.priority.as_deref(), Some("warning"));
        assert_eq!(config.topic_mode, JournalTopicMode::Identifier);
        assert!(config.follow_new);
        assert_eq!(config.since.as_deref(), Some("1 hour ago"));
        assert_eq!(config.batch_size, 50);
        assert_eq!(config.poll_interval_ms, 500);
    }

    #[test]
    fn test_journal_tentacle_name_and_type() {
        let config = JournalTentacleConfig {
            units: vec!["test".to_string()],
            priority: None,
            topic_mode: JournalTopicMode::Identifier,
            fallback_topic: "/systemd/journal/default".to_string(),
            identifier_mapping: HashMap::new(),
            unit_mapping: HashMap::new(),
            follow_new: true,
            since: None,
            batch_size: 100,
            poll_interval_ms: 1000,
        };
        let tentacle = JournalTentacle::new("test_journal".to_string(), config);
        assert_eq!(tentacle.name(), "test_journal");
        assert_eq!(tentacle.r#type(), "journalctl");
    }

    #[test]
    fn test_journal_build_command() {
        let config = JournalTentacleConfig {
            units: vec!["sshd".to_string(), "docker".to_string()],
            priority: Some("err".to_string()),
            topic_mode: JournalTopicMode::Identifier,
            fallback_topic: "/systemd/journal/default".to_string(),
            identifier_mapping: HashMap::new(),
            unit_mapping: HashMap::new(),
            follow_new: true,
            since: Some("1 hour ago".to_string()),
            batch_size: 100,
            poll_interval_ms: 1000,
        };
        let tentacle = JournalTentacle::new("test".to_string(), config);
        let args = tentacle.build_command();

        assert!(args.contains(&"journalctl".to_string()));
        assert!(args.contains(&"--output=export".to_string()));
        assert!(args.contains(&"-u".to_string()));
        assert!(args.contains(&"sshd".to_string()));
        assert!(args.contains(&"docker".to_string()));
        assert!(args.contains(&"-p".to_string()));
        assert!(args.contains(&"err".to_string()));
        assert!(args.contains(&"--since".to_string()));
        assert!(args.contains(&"1 hour ago".to_string()));
    }
}
