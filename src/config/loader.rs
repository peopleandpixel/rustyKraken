use anyhow::Result;
use std::fs;
use std::path::Path;
use tracing::{info, warn};
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
        let entry = match entry {
            Ok(e) => e,
            Err(e) => {
                warn!("Failed to read directory entry: {}", e);
                continue;
            }
        };
        let path = entry.path();

        if path.is_file()
            && (path
                .extension()
                .is_some_and(|ext| ext == "yaml" || ext == "yml"))
        {
            info!("Loading tentacle config: {:?}", path);

            let content = match fs::read_to_string(path) {
                Ok(c) => c,
                Err(e) => {
                    warn!(
                        "Failed to read tentacle config file {:?}: {}, skipping",
                        path, e
                    );
                    continue;
                }
            };

            let config: TentacleConfig = match serde_yaml::from_str(&content) {
                Ok(c) => c,
                Err(e) => {
                    warn!(
                        "Failed to parse tentacle config file {:?}: {}, skipping",
                        path, e
                    );
                    continue;
                }
            };

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
        let entry = match entry {
            Ok(e) => e,
            Err(e) => {
                warn!("Failed to read directory entry: {}", e);
                continue;
            }
        };
        let path = entry.path();

        if path.is_file()
            && (path
                .extension()
                .is_some_and(|ext| ext == "yaml" || ext == "yml"))
        {
            info!("Loading reflex config: {:?}", path);

            let content = match fs::read_to_string(path) {
                Ok(c) => c,
                Err(e) => {
                    warn!(
                        "Failed to read reflex config file {:?}: {}, skipping",
                        path, e
                    );
                    continue;
                }
            };

            let config: ReflexConfig = match serde_yaml::from_str(&content) {
                Ok(c) => c,
                Err(e) => {
                    warn!(
                        "Failed to parse reflex config file {:?}: {}, skipping",
                        path, e
                    );
                    continue;
                }
            };

            configs.push(config);
        }
    }

    info!("Loaded {} reflex configuration(s)", configs.len());
    Ok(configs)
}
