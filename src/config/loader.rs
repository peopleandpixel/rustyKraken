use anyhow::{Context, Result};
use std::fs;
use std::path::Path;
use tracing::info;
use walkdir::WalkDir;

use super::models::{ReflexConfig, TentacleConfig};

/// Lädt alle Tentacle-Konfigurationen aus einem Ordner
pub fn load_tentacles(config_dir: &Path) -> Result<Vec<TentacleConfig>> {
    let mut configs = Vec::new();

    if !config_dir.exists() {
        info!(
            "Tentacle config directory does not exist yet: {:?}",
            config_dir
        );
        return Ok(configs);
    }

    for entry in WalkDir::new(config_dir).max_depth(1) {
        let entry = entry?;
        let path = entry.path();

        if path.is_file()
            && (path
                .extension()
                .is_some_and(|ext| ext == "yaml" || ext == "yml"))
        {
            info!("Loading tentacle config: {:?}", path);

            let content = fs::read_to_string(path)
                .with_context(|| format!("Failed to read config file: {:?}", path))?;

            let config: TentacleConfig = serde_yaml::from_str(&content)
                .with_context(|| format!("Failed to parse config file: {:?}", path))?;

            configs.push(config);
        }
    }

    info!("Loaded {} tentacle configuration(s)", configs.len());
    Ok(configs)
}

/// Lädt alle Reflex-Konfigurationen aus einem Ordner
pub fn load_reflexes(config_dir: &Path) -> Result<Vec<ReflexConfig>> {
    let mut configs = Vec::new();

    if !config_dir.exists() {
        info!(
            "Reflex config directory does not exist yet: {:?}",
            config_dir
        );
        return Ok(configs);
    }

    for entry in WalkDir::new(config_dir).max_depth(1) {
        let entry = entry?;
        let path = entry.path();

        if path.is_file()
            && (path
                .extension()
                .is_some_and(|ext| ext == "yaml" || ext == "yml"))
        {
            info!("Loading reflex config: {:?}", path);

            let content = fs::read_to_string(path)
                .with_context(|| format!("Failed to read config file: {:?}", path))?;

            let config: ReflexConfig = serde_yaml::from_str(&content)
                .with_context(|| format!("Failed to parse config file: {:?}", path))?;

            configs.push(config);
        }
    }

    info!("Loaded {} reflex configuration(s)", configs.len());
    Ok(configs)
}
