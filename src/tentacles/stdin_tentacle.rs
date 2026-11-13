use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::signal;
use tracing::{debug, error, info, warn};

use super::Tentacle;
use crate::eventbus::EventBus;
use crate::model::Ink;

/// Eingabeformat für den Stdin-Tentacle
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "lowercase")]
pub enum StdinFormat {
    /// Jede Zeile ist plain text; Topic aus Inhalt oder Fallback
    Plain,
    /// Jede Zeile ist ein JSON-Objekt mit "topic" und/oder "body"
    Json,
    /// Automatisch erkennen: wenn Zeile mit { beginnt, JSON, sonst plain
    #[default]
    Auto,
}

/// Topic-Erkennungsmodus für plain-text-Eingabe
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "lowercase")]
pub enum TopicMode {
    /// Topic aus der ersten Zeile, wenn sie mit / beginnt; sonst Fallback
    #[default]
    Content,
    /// Alle Nachrichten erhalten das Fallback-Topic
    All,
}

/// Konfiguration für den Stdin-Tentacle
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StdinTentacleConfig {
    /// Eingabeformat: "plain", "json", "auto"
    #[serde(default)]
    pub format: StdinFormat,
    /// Topic-Erkennungsmodus für plain text: "content" oder "all"
    #[serde(default)]
    pub topic_mode: TopicMode,
    /// Fallback-Topic, wenn kein Topic erkannt wird (Standard: "/stdin/default")
    #[serde(default = "default_fallback_topic")]
    pub fallback_topic: String,
    /// Optional: Prefix für JSON-Felder, falls verschachtelt (z.B. "data.")
    #[serde(default)]
    pub json_prefix: Option<String>,
}

fn default_fallback_topic() -> String {
    "/stdin/default".to_string()
}

pub struct StdinTentacle {
    name: String,
    config: StdinTentacleConfig,
}

impl StdinTentacle {
    pub fn new(name: String, config: StdinTentacleConfig) -> Self {
        Self { name, config }
    }

    /// Extrahiert das Topic aus einer plain-text-Zeile
    fn extract_topic_plain(&self, line: &str) -> String {
        match self.config.topic_mode {
            TopicMode::Content => {
                let trimmed = line.trim();
                if trimmed.starts_with('/') {
                    // Topic ist der erste Token, der mit / beginnt
                    if let Some(space_pos) = trimmed.find(' ') {
                        let topic_part = &trimmed[..space_pos];
                        if !topic_part.is_empty() {
                            return topic_part.to_string();
                        }
                    } else {
                        // Ganze Zeile ist das Topic, body ist leer
                        return trimmed.to_string();
                    }
                }
                self.config.fallback_topic.clone()
            }
            TopicMode::All => self.config.fallback_topic.clone(),
        }
    }

    /// Extrahiert den Body aus einer plain-text-Zeile (nach dem Topic)
    fn extract_body_plain(&self, line: &str) -> String {
        match self.config.topic_mode {
            TopicMode::Content => {
                let trimmed = line.trim();
                if trimmed.starts_with('/') {
                    if let Some(space_pos) = trimmed.find(' ') {
                        return trimmed[space_pos..].trim().to_string();
                    }
                    // Keine Leerzeichen — ganze Zeile ist Topic, body ist leer
                    return String::new();
                }
                // Kein Topic erkannt — ganze Zeile ist der Body
                trimmed.to_string()
            }
            TopicMode::All => line.trim().to_string(),
        }
    }

    /// Versucht, eine Zeile als JSON zu parsen und Topic/Body zu extrahieren
    fn parse_json_line(&self, line: &str) -> Option<(String, String)> {
        let parsed: serde_json::Value = serde_json::from_str(line).ok()?;

        let obj = match parsed {
            serde_json::Value::Object(map) => map,
            _ => return None,
        };

        // Topic suchen
        let topic = if let Some(t) = obj.get("topic").and_then(|v| v.as_str()) {
            t.to_string()
        } else {
            self.config.fallback_topic.clone()
        };

        // Body suchen
        let body = if let Some(b) = obj.get("body").and_then(|v| v.as_str()) {
            b.to_string()
        } else if let Some(b) = obj.get("message").and_then(|v| v.as_str()) {
            b.to_string()
        } else {
            // Fallback: gesamte JSON als Body
            line.trim().to_string()
        };

        Some((topic, body))
    }

    /// Bestimmt das Format einer Zeile
    fn detect_format(&self, line: &str) -> StdinFormat {
        let trimmed = line.trim();
        if trimmed.starts_with('{') {
            StdinFormat::Json
        } else {
            StdinFormat::Plain
        }
    }

    /// Verarbeitet eine einzelne Zeile und erstellt ein Ink
    fn process_line(&self, line: &str) -> Option<Ink> {
        let line = line.trim().to_string();
        if line.is_empty() {
            return None;
        }

        let format = match &self.config.format {
            StdinFormat::Auto => self.detect_format(&line),
            f => f.clone(),
        };

        match format {
            StdinFormat::Plain | StdinFormat::Auto => {
                let topic = self.extract_topic_plain(&line);
                let body = self.extract_body_plain(&line);
                Some(Ink::new(
                    topic,
                    "stdin",
                    &self.name,
                    if body.is_empty() { line } else { body },
                ))
            }
            StdinFormat::Json => {
                if let Some((topic, body)) = self.parse_json_line(&line) {
                    Some(Ink::new(topic, "stdin", &self.name, body))
                } else {
                    warn!("Failed to parse JSON line, treating as plain text");
                    let topic = self.extract_topic_plain(&line);
                    Some(Ink::new(topic, "stdin", &self.name, line))
                }
            }
        }
    }
}

#[async_trait]
impl Tentacle for StdinTentacle {
    fn name(&self) -> &str {
        &self.name
    }

    fn r#type(&self) -> &str {
        "stdin"
    }

    async fn start(&self, event_bus: Arc<EventBus>) -> Result<()> {
        info!(
            "Stdin tentacle '{}' started (format: {:?}, topic_mode: {:?})",
            self.name, self.config.format, self.config.topic_mode
        );

        // Stdin als tokio-Stream lesen
        let stdin = tokio::io::stdin();
        let reader = BufReader::new(stdin);
        let mut lines = reader.lines();

        loop {
            // Warten auf Ctrl+C oder neue Zeile
            tokio::select! {
                _ = signal::ctrl_c() => {
                    info!("Stdin tentacle '{}' received shutdown signal", self.name);
                    break;
                }
                result = lines.next_line() => {
                    match result {
                        Ok(Some(line)) => {
                            if let Some(ink) = self.process_line(&line) {
                                debug!(
                                    "Stdin tentacle '{}' received ink on topic: {}",
                                    self.name, ink.topic
                                );
                                event_bus.publish(ink);
                            }
                        }
                        Ok(None) => {
                            // EOF erreicht
                            info!("Stdin tentacle '{}' reached EOF", self.name);
                            break;
                        }
                        Err(e) => {
                            error!("Stdin tentacle '{}' failed to read line: {}", self.name, e);
                        }
                    }
                }
            }
        }

        info!("Stdin tentacle '{}' stopped", self.name);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::appcontext::AppContext;

    fn create_tentacle_plain() -> StdinTentacle {
        StdinTentacle::new(
            "test_stdin".to_string(),
            StdinTentacleConfig {
                format: StdinFormat::Plain,
                topic_mode: TopicMode::Content,
                fallback_topic: "/stdin/default".to_string(),
                json_prefix: None,
            },
        )
    }

    fn create_tentacle_json() -> StdinTentacle {
        StdinTentacle::new(
            "test_stdin".to_string(),
            StdinTentacleConfig {
                format: StdinFormat::Json,
                topic_mode: TopicMode::Content,
                fallback_topic: "/json/fallback".to_string(),
                json_prefix: None,
            },
        )
    }

    fn create_tentacle_auto() -> StdinTentacle {
        StdinTentacle::new(
            "test_stdin".to_string(),
            StdinTentacleConfig {
                format: StdinFormat::Auto,
                topic_mode: TopicMode::Content,
                fallback_topic: "/auto/default".to_string(),
                json_prefix: None,
            },
        )
    }

    fn create_tentacle_all_mode() -> StdinTentacle {
        StdinTentacle::new(
            "test_stdin".to_string(),
            StdinTentacleConfig {
                format: StdinFormat::Plain,
                topic_mode: TopicMode::All,
                fallback_topic: "/all/topic".to_string(),
                json_prefix: None,
            },
        )
    }

    #[test]
    fn test_extract_topic_plain_with_command() {
        let tentacle = create_tentacle_plain();
        assert_eq!(
            tentacle.extract_topic_plain("/alerts hello world"),
            "/alerts"
        );
    }

    #[test]
    fn test_extract_topic_plain_without_command() {
        let tentacle = create_tentacle_plain();
        assert_eq!(
            tentacle.extract_topic_plain("just a message"),
            "/stdin/default"
        );
    }

    #[test]
    fn test_extract_topic_plain_topic_only() {
        let tentacle = create_tentacle_plain();
        assert_eq!(tentacle.extract_topic_plain("/only_topic"), "/only_topic");
    }

    #[test]
    fn test_extract_body_plain_with_command() {
        let tentacle = create_tentacle_plain();
        assert_eq!(
            tentacle.extract_body_plain("/alerts hello world"),
            "hello world"
        );
    }

    #[test]
    fn test_extract_body_plain_without_command() {
        let tentacle = create_tentacle_plain();
        assert_eq!(
            tentacle.extract_body_plain("just a message"),
            "just a message"
        );
    }

    #[test]
    fn test_topic_mode_all() {
        let tentacle = create_tentacle_all_mode();
        assert_eq!(tentacle.extract_topic_plain("/alerts hello"), "/all/topic");
        assert_eq!(
            tentacle.extract_body_plain("/alerts hello"),
            "/alerts hello"
        );
    }

    #[test]
    fn test_parse_json_line_with_topic_and_body() {
        let tentacle = create_tentacle_json();
        let json = r#"{"topic": "/json/test", "body": "hello from JSON"}"#;
        let result = tentacle.parse_json_line(json);
        assert!(result.is_some());
        let (topic, body) = result.unwrap();
        assert_eq!(topic, "/json/test");
        assert_eq!(body, "hello from JSON");
    }

    #[test]
    fn test_parse_json_line_with_message_field() {
        let tentacle = create_tentacle_json();
        let json = r#"{"message": "using message field"}"#;
        let result = tentacle.parse_json_line(json);
        assert!(result.is_some());
        let (topic, body) = result.unwrap();
        assert_eq!(topic, "/json/fallback");
        assert_eq!(body, "using message field");
    }

    #[test]
    fn test_parse_json_line_invalid() {
        let tentacle = create_tentacle_json();
        assert!(tentacle.parse_json_line("not json").is_none());
        assert!(tentacle.parse_json_line("[1,2,3]").is_none());
    }

    #[test]
    fn test_detect_format() {
        let tentacle = create_tentacle_auto();
        assert_eq!(
            tentacle.detect_format(r#"{"key": "value"}"#),
            StdinFormat::Json
        );
        assert_eq!(tentacle.detect_format("plain text"), StdinFormat::Plain);
        assert_eq!(
            tentacle.detect_format("  {\"key\": \"value\"}"),
            StdinFormat::Json
        );
    }

    #[test]
    fn test_process_line_plain_with_topic() {
        let tentacle = create_tentacle_plain();
        let ink = tentacle.process_line("/test hello world").unwrap();
        assert_eq!(ink.topic, "/test");
        assert_eq!(ink.body, "hello world");
        assert_eq!(ink.source.r#type, "stdin");
    }

    #[test]
    fn test_process_line_plain_without_topic() {
        let tentacle = create_tentacle_plain();
        let ink = tentacle.process_line("just text").unwrap();
        assert_eq!(ink.topic, "/stdin/default");
        assert_eq!(ink.body, "just text");
    }

    #[test]
    fn test_process_line_json() {
        let tentacle = create_tentacle_auto();
        let ink = tentacle
            .process_line(r#"{"topic": "/auto/json", "body": "json body"}"#)
            .unwrap();
        assert_eq!(ink.topic, "/auto/json");
        assert_eq!(ink.body, "json body");
    }

    #[test]
    fn test_process_line_empty() {
        let tentacle = create_tentacle_plain();
        assert!(tentacle.process_line("").is_none());
        assert!(tentacle.process_line("   ").is_none());
    }

    #[test]
    fn test_config_deserialization() {
        let yaml = r#"
            format: auto
            topic_mode: content
            fallback_topic: "/my/fallback"
        "#;
        let config: StdinTentacleConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.format, StdinFormat::Auto);
        assert_eq!(config.topic_mode, TopicMode::Content);
        assert_eq!(config.fallback_topic, "/my/fallback");
    }

    #[test]
    fn test_config_defaults() {
        let yaml = r#"{}"#;
        let config: StdinTentacleConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.format, StdinFormat::Auto);
        assert_eq!(config.topic_mode, TopicMode::Content);
        assert_eq!(config.fallback_topic, "/stdin/default");
    }
}
