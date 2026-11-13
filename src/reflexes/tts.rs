use anyhow::{Context, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::{debug, info};

use super::Reflex;
use crate::model::Ink;

/// Configuration for TTS (Text-to-Speech) Reflex
/// Converts text to speech via various TTS services
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TtsReflexConfig {
    /// TTS service: "espeak", "pico2wave", "gtts", "macos_say"
    #[serde(default = "default_service")]
    pub service: String,

    /// Voice name
    #[serde(default)]
    pub voice: Option<String>,

    /// Template for text to speak
    #[serde(default = "default_text_template")]
    pub text_template: String,

    /// Output directory for audio files
    #[serde(default = "default_output_dir")]
    pub output_dir: String,

    /// Output filename template
    #[serde(default = "default_filename_template")]
    pub filename_template: String,

    /// Audio format: "wav", "mp3", "ogg"
    #[serde(default = "default_format")]
    pub format: String,

    /// Optional: mapping from Ink topics to voices
    #[serde(default)]
    pub topic_voice_mapping: HashMap<String, String>,

    /// Optional: mapping from Ink topics to text templates
    #[serde(default)]
    pub topic_text_mapping: HashMap<String, String>,

    /// Speak immediately (play audio)
    #[serde(default)]
    pub speak_immediately: bool,

    /// Timeout in seconds
    #[serde(default = "default_timeout")]
    pub timeout: u64,
}

fn default_service() -> String {
    "espeak".to_string()
}
fn default_text_template() -> String {
    "{{body}}".to_string()
}
fn default_output_dir() -> String {
    "/tmp".to_string()
}
fn default_filename_template() -> String {
    "tts_{{id}}.wav".to_string()
}
fn default_format() -> String {
    "wav".to_string()
}
fn default_timeout() -> u64 {
    30
}

pub struct TtsReflex {
    name: String,
    config: TtsReflexConfig,
}

impl TtsReflex {
    pub fn new(name: String, config: TtsReflexConfig) -> Self {
        Self { name, config }
    }

    /// Resolve voice for a given Ink
    fn resolve_voice(&self, ink: &Ink) -> Option<String> {
        if let Some(voice) = self
            .config
            .topic_voice_mapping
            .get(&ink.topic)
            .filter(|v| !v.is_empty())
        {
            return Some(voice.clone());
        }
        self.config.voice.clone()
    }

    /// Resolve text template for a given Ink
    fn resolve_text_template(&self, ink: &Ink) -> String {
        if let Some(template) = self
            .config
            .topic_text_mapping
            .get(&ink.topic)
            .filter(|t| !t.is_empty())
        {
            return template.clone();
        }
        self.config.text_template.clone()
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

    /// Generate TTS using espeak
    async fn espeak(&self, text: &str, output_file: &str, voice: Option<&str>) -> Result<()> {
        info!("TTS reflex '{}' generating speech with espeak", self.name);

        let mut cmd = tokio::process::Command::new("espeak");
        cmd.arg(text).arg("-f").arg(output_file);

        if let Some(v) = voice {
            cmd.arg("-v").arg(v);
        }

        let output = cmd.output().await.context("Failed to execute espeak")?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(anyhow::anyhow!("espeak failed: {}", stderr));
        }

        Ok(())
    }

    /// Generate TTS using pico2wave
    async fn pico2wave(&self, text: &str, output_file: &str) -> Result<()> {
        info!(
            "TTS reflex '{}' generating speech with pico2wave",
            self.name
        );

        let output = tokio::process::Command::new("pico2wave")
            .arg("-w")
            .arg(output_file)
            .arg(text)
            .output()
            .await
            .context("Failed to execute pico2wave")?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(anyhow::anyhow!("pico2wave failed: {}", stderr));
        }

        Ok(())
    }

    /// Generate TTS using macOS say command
    async fn macos_say(&self, text: &str, output_file: &str, voice: Option<&str>) -> Result<()> {
        info!(
            "TTS reflex '{}' generating speech with macOS say",
            self.name
        );

        let mut cmd = tokio::process::Command::new("say");
        cmd.arg("-o").arg(output_file).arg(text);

        if let Some(v) = voice {
            cmd.arg("-v").arg(v);
        }

        let output = cmd.output().await.context("Failed to execute say")?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(anyhow::anyhow!("say failed: {}", stderr));
        }

        Ok(())
    }

    /// Play audio file
    async fn play_audio(&self, file: &str) -> Result<()> {
        info!("TTS reflex '{}' playing audio: {}", self.name, file);

        // Try paplay first (PulseAudio), then aplay (ALSA)
        let paplay_result = tokio::process::Command::new("paplay")
            .arg(file)
            .output()
            .await;

        if let Ok(output) = paplay_result
            && output.status.success()
        {
            return Ok(());
        }

        let aplay_result = tokio::process::Command::new("aplay")
            .arg(file)
            .output()
            .await;

        if let Ok(output) = aplay_result
            && output.status.success()
        {
            return Ok(());
        }

        Err(anyhow::anyhow!("No audio player available"))
    }
}

#[async_trait]
impl Reflex for TtsReflex {
    fn name(&self) -> &str {
        &self.name
    }

    fn r#type(&self) -> &str {
        "tts"
    }

    async fn handle(&self, ink: &Ink) -> Result<()> {
        info!(
            "TTS reflex '{}' handling ink on topic: {}",
            self.name, ink.topic
        );

        let voice = self.resolve_voice(ink);
        let text_template = self.resolve_text_template(ink);
        let text = self.render_template(&text_template, ink);

        let filename = self.render_template(&self.config.filename_template, ink);
        let output_file = format!("{}/{}", self.config.output_dir, filename);

        debug!(
            "TTS reflex '{}' text={}, voice={:?}, output={}",
            self.name, text, voice, output_file
        );

        // Generate TTS
        match self.config.service.as_str() {
            "espeak" => self.espeak(&text, &output_file, voice.as_deref()).await?,
            "pico2wave" => self.pico2wave(&text, &output_file).await?,
            "macos_say" => {
                self.macos_say(&text, &output_file, voice.as_deref())
                    .await?
            }
            _ => {
                return Err(anyhow::anyhow!(
                    "Unsupported TTS service: {}",
                    self.config.service
                ));
            }
        }

        info!(
            "TTS reflex '{}' generated audio: {}",
            self.name, output_file
        );

        // Optionally play immediately
        if self.config.speak_immediately {
            self.play_audio(&output_file).await?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_config() -> TtsReflexConfig {
        TtsReflexConfig {
            service: "espeak".to_string(),
            voice: None,
            text_template: "{{body}}".to_string(),
            output_dir: "/tmp".to_string(),
            filename_template: "tts_{{id}}.wav".to_string(),
            format: "wav".to_string(),
            topic_voice_mapping: HashMap::new(),
            topic_text_mapping: HashMap::new(),
            speak_immediately: false,
            timeout: 30,
        }
    }

    fn create_test_ink() -> Ink {
        Ink::new("/test/topic", "test", "test_source", "Hello world")
    }

    #[test]
    fn test_template_rendering() {
        let config = create_test_config();
        let reflex = TtsReflex::new("test".to_string(), config);
        let ink = create_test_ink();
        assert_eq!(reflex.render_template("{{body}}", &ink), "Hello world");
    }

    #[test]
    fn test_config_deserialization() {
        let yaml = r#"
            service: "espeak"
            voice: "en-us"
            speak_immediately: true
        "#;
        let config: TtsReflexConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.service, "espeak");
        assert!(config.speak_immediately);
    }

    #[test]
    fn test_reflex_type() {
        let config = create_test_config();
        let reflex = TtsReflex::new("test".to_string(), config);
        assert_eq!(reflex.r#type(), "tts");
    }

    #[test]
    fn test_resolve_voice_from_mapping() {
        let mut config = create_test_config();
        let mut mapping = HashMap::new();
        mapping.insert("/tts/alerts".to_string(), "en-us".to_string());
        config.topic_voice_mapping = mapping;

        let reflex = TtsReflex::new("test".to_string(), config);
        let ink = Ink::new("/tts/alerts", "test", "test_source", "body");
        assert_eq!(reflex.resolve_voice(&ink), Some("en-us".to_string()));
    }
}
