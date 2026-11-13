use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tracing::{debug, info};

use super::Reflex;
use crate::model::Ink;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SoundAlertReflexConfig {
    /// Sound file path to play
    #[serde(default = "default_sound")]
    pub sound_file: String,
    /// Command to play sound (e.g., "paplay", "afplay", "mpv")
    #[serde(default = "default_player")]
    pub player_command: String,
    /// Whether to show desktop notification
    #[serde(default)]
    pub show_notification: bool,
    /// Template for notification title
    #[serde(default = "default_title_template")]
    pub title_template: String,
    /// Template for notification message
    #[serde(default = "default_message_template")]
    pub message_template: String,
}

fn default_sound() -> String {
    "/usr/share/sounds/freedesktop/stereo/alert.oga".to_string()
}
fn default_player() -> String {
    "paplay".to_string()
}
fn default_title_template() -> String {
    "{{topic}}".to_string()
}
fn default_message_template() -> String {
    "{{body}}".to_string()
}

pub struct SoundAlertReflex {
    name: String,
    config: SoundAlertReflexConfig,
}

impl SoundAlertReflex {
    pub fn new(name: String, config: SoundAlertReflexConfig) -> Self {
        Self { name, config }
    }

    fn render_template(&self, template: &str, ink: &Ink) -> String {
        let mut rendered = template.to_string();
        rendered = rendered.replace("{{topic}}", &ink.topic);
        rendered = rendered.replace("{{body}}", &ink.body);
        rendered = rendered.replace("{{id}}", &ink.id.to_string());
        rendered = rendered.replace("{{timestamp}}", &ink.timestamp.to_rfc3339());
        if let Some(obj) = ink.metadata.as_ref().and_then(|m| m.as_object()) {
            for (key, value) in obj {
                let placeholder = format!("{{{{metadata.{}}}}}", key);
                let value_str = match value {
                    serde_json::Value::String(s) => s.clone(),
                    _ => value.to_string(),
                };
                rendered = rendered.replace(&placeholder, &value_str);
            }
        }
        rendered
    }
}

#[async_trait]
impl Reflex for SoundAlertReflex {
    fn name(&self) -> &str {
        &self.name
    }
    fn r#type(&self) -> &str {
        "sound_alert"
    }

    async fn handle(&self, ink: &Ink) -> Result<()> {
        info!(
            "Sound/Alert reflex '{}' handling ink on topic: {}",
            self.name, ink.topic
        );
        debug!(
            "Sound/Alert reflex '{}' playing sound: {}",
            self.name, self.config.sound_file
        );

        // Play sound
        let _ = tokio::process::Command::new(&self.config.player_command)
            .arg(&self.config.sound_file)
            .status()
            .await;

        // Show notification
        if self.config.show_notification {
            let title = self.render_template(&self.config.title_template, ink);
            let message = self.render_template(&self.config.message_template, ink);
            let _ = tokio::process::Command::new("notify-send")
                .arg(&title)
                .arg(&message)
                .status()
                .await;
        }

        info!(
            "Sound/Alert reflex '{}' triggered for topic: {}",
            self.name, ink.topic
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::appcontext::AppContext;

    fn create_test_config() -> SoundAlertReflexConfig {
        SoundAlertReflexConfig {
            sound_file: "/usr/share/sounds/alert.oga".to_string(),
            player_command: "paplay".to_string(),
            show_notification: true,
            title_template: "{{topic}}".to_string(),
            message_template: "{{body}}".to_string(),
        }
    }

    #[test]
    fn test_template_rendering() {
        let config = create_test_config();
        let reflex = SoundAlertReflex::new("test".to_string(), config);
        let ink = Ink::new("/test/topic", "test", "test_source", "test body");
        assert_eq!(
            reflex.render_template("[{{topic}}] {{body}}", &ink),
            "[/test/topic] test body"
        );
    }

    #[test]
    fn test_config_deserialization() {
        let yaml = r#"
            sound_file: "/path/to/sound.wav"
            player_command: "mpv"
            show_notification: true
        "#;
        let config: SoundAlertReflexConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.player_command, "mpv");
    }

    #[test]
    fn test_reflex_type() {
        let config = create_test_config();
        let reflex = SoundAlertReflex::new("test".to_string(), config);
        assert_eq!(reflex.r#type(), "sound_alert");
    }
}
