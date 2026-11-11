use anyhow::{Context, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use tokio::process::Command;
use tracing::{error, info};

use super::Reflex;
use crate::model::Ink;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CommandReflexConfig {
    pub command: String,
    #[serde(default)]
    pub args: Vec<String>,
}

pub struct CommandReflex {
    name: String,
    config: CommandReflexConfig,
}

impl CommandReflex {
    pub fn new(name: String, config: CommandReflexConfig) -> Self {
        Self { name, config }
    }
}

#[async_trait]
impl Reflex for CommandReflex {
    fn name(&self) -> &str {
        &self.name
    }

    fn r#type(&self) -> &str {
        "command"
    }

    async fn handle(&self, ink: &Ink) -> Result<()> {
        info!(
            "Command reflex '{}' executing command: {}",
            self.name, self.config.command
        );

        // Ink als JSON für die Umgebungsvariablen
        let ink_json = serde_json::to_string(ink).context("Failed to serialize ink to JSON")?;

        // Befehl ausführen
        let mut cmd = Command::new(&self.config.command);
        cmd.args(&self.config.args);

        // Ink als Umgebungsvariable bereitstellen
        cmd.env("INK_JSON", &ink_json);
        cmd.env("INK_TOPIC", &ink.topic);
        cmd.env("INK_BODY", &ink.body);
        cmd.env("INK_ID", ink.id.to_string());

        let output = cmd
            .output()
            .await
            .with_context(|| format!("Failed to execute command: {}", self.config.command))?;

        if output.status.success() {
            info!("Command reflex '{}' executed successfully", self.name);
        } else {
            let stderr = String::from_utf8_lossy(&output.stderr);
            error!("Command reflex '{}' failed: {}", self.name, stderr);
        }

        Ok(())
    }
}
