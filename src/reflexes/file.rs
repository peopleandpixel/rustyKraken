use anyhow::{Context, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::PathBuf;
use tracing::{error, info};

use super::Reflex;
use crate::model::Ink;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileReflexConfig {
    pub path: String,
    #[serde(default = "default_mode")]
    pub mode: String,
}

fn default_mode() -> String {
    "append".to_string()
}

pub struct FileReflex {
    name: String,
    config: FileReflexConfig,
}

impl FileReflex {
    pub fn new(name: String, config: FileReflexConfig) -> Self {
        Self { name, config }
    }
}

#[async_trait]
impl Reflex for FileReflex {
    fn name(&self) -> &str {
        &self.name
    }

    fn r#type(&self) -> &str {
        "file"
    }

    async fn handle(&self, ink: &Ink) -> Result<()> {
        let path = PathBuf::from(&self.config.path);

        // Sicherstellen, dass das Verzeichnis existiert
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("Failed to create directory: {:?}", parent))?;
        }

        // Nachricht als JSON schreiben
        let json = serde_json::to_string_pretty(ink).context("Failed to serialize ink to JSON")?;

        match self.config.mode.as_str() {
            "append" => {
                let mut file = fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&path)
                    .with_context(|| format!("Failed to open file: {:?}", path))?;

                use std::io::Write;
                writeln!(file, "{}", json)
                    .with_context(|| format!("Failed to write to file: {:?}", path))?;
            }
            "overwrite" => {
                fs::write(&path, &json)
                    .with_context(|| format!("Failed to write file: {:?}", path))?;
            }
            _ => {
                error!("Unknown file mode: {}", self.config.mode);
            }
        }

        info!("File reflex '{}' wrote ink to {:?}", self.name, path);
        Ok(())
    }
}
