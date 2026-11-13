use anyhow::{Context, Result};
use async_trait::async_trait;
use notify::{Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher as NotifyWatcher};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::mpsc;
use std::time::Duration;
use tokio::time::sleep;
use tracing::{debug, error, info, warn};

use super::Tentacle;
use crate::eventbus::EventBus;
use crate::model::Ink;

/// Was soll mit einer Datei nach der Verarbeitung passieren?
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum PostProcessAction {
    /// Nichts tun (Standard)
    None,
    /// Datei löschen
    Delete,
    /// Datei in einen anderen Ordner verschieben
    Move,
}

/// Konfiguration für einen überwachten Ordner
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WatchedFolder {
    /// Pfad zum Ordner
    pub path: String,
    /// Topic für Dateien in diesem Ordner (wird überschrieben, wenn topic_mode anders konfiguriert)
    #[serde(default)]
    pub topic: Option<String>,
    /// Rekursiv überwachen (Standard: false)
    #[serde(default)]
    pub recursive: bool,
}

/// Konfiguration für den File Watcher Tentacle
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileWatcherTentacleConfig {
    /// Ordner, die überwacht werden sollen
    pub folders: Vec<WatchedFolder>,
    /// Dateityp-Filter (z.B. [".txt", ".json", ".csv"]); leer = alle Dateien
    #[serde(default)]
    pub file_extensions: Vec<String>,
    /// Topic-Erkennungsmodus: "folder" (aus Ordner-Mapping), "filename" (Dateiname als Topic), "content" (erste Zeile als Topic), "all" (kombiniert)
    #[serde(default = "default_topic_mode")]
    pub topic_mode: String,
    /// Fallback-Topic, wenn kein Topic erkannt wird
    #[serde(default)]
    pub fallback_topic: Option<String>,
    /// Optional: Mapping von Dateinamen-Patterns auf Topics (Wildcard: *)
    #[serde(default)]
    pub filename_topic_mapping: Option<HashMap<String, String>>,
    /// Aktion nach Verarbeitung: "none", "delete", "move"
    #[serde(default)]
    pub post_process: Option<PostProcessAction>,
    /// Zielordner für "move"-Aktion (wird ignoriert, wenn post_process != move)
    #[serde(default)]
    pub move_to: Option<String>,
    /// Verzögerung nach Dateierstellung vor Verarbeitung in Millisekunden (Standard: 500)
    /// Gibt dem Dateisystem Zeit, die Datei vollständig zu schreiben
    #[serde(default = "default_processing_delay")]
    pub processing_delay: u64,
    /// Maximale Dateigröße in Bytes (Standard: 10 MB); größere Dateien werden übersprungen
    #[serde(default = "default_max_file_size")]
    pub max_file_size: u64,
}

fn default_topic_mode() -> String {
    "folder".to_string()
}

fn default_processing_delay() -> u64 {
    500
}

fn default_max_file_size() -> u64 {
    10 * 1024 * 1024
}

pub struct FileWatcherTentacle {
    name: String,
    config: FileWatcherTentacleConfig,
}

impl FileWatcherTentacle {
    pub fn new(name: String, config: FileWatcherTentacleConfig) -> Self {
        Self { name, config }
    }

    /// Prüft, ob eine Dateierweiterung erlaubt ist
    fn is_allowed_extension(&self, path: &Path) -> bool {
        if self.config.file_extensions.is_empty() {
            return true;
        }

        let ext = match path.extension() {
            Some(ext) => ext.to_string_lossy().to_lowercase(),
            None => return false,
        };

        let ext_with_dot = format!(".{}", ext);
        self.config
            .file_extensions
            .iter()
            .any(|allowed| allowed.to_lowercase() == ext_with_dot || allowed.to_lowercase() == ext)
    }

    /// Bestimmt das Topic für eine Datei basierend auf dem konfigurierten Modus
    fn resolve_topic(&self, folder_config: &WatchedFolder, file_path: &Path) -> String {
        match self.config.topic_mode.as_str() {
            "folder" => {
                // Ordnerbasiertes Topic
                if let Some(ref topic) = folder_config.topic {
                    topic.clone()
                } else if let Some(ref fallback) = self.config.fallback_topic {
                    fallback.clone()
                } else {
                    format!("/files/{}", folder_config.path)
                }
            }
            "filename" => {
                // Dateiname als Topic (ohne Erweiterung)
                if let Some(ref mapping) = self.config.filename_topic_mapping {
                    let filename = file_path
                        .file_name()
                        .map(|f| f.to_string_lossy().to_string())
                        .unwrap_or_default();

                    for (pattern, topic) in mapping {
                        if Self::match_filename_pattern(pattern, &filename) {
                            return topic.clone();
                        }
                    }
                }

                // Dateiname ohne Erweiterung als Topic
                let stem = file_path
                    .file_stem()
                    .map(|s| s.to_string_lossy().to_string())
                    .unwrap_or_default();

                if !stem.is_empty() {
                    format!("/files/{}", stem)
                } else if let Some(ref fallback) = self.config.fallback_topic {
                    fallback.clone()
                } else {
                    "/files/default".to_string()
                }
            }
            "content" => {
                // Erste Zeile der Datei als Topic
                if let Ok(content) = std::fs::read_to_string(file_path)
                    && let Some(first_line) = content.lines().next()
                {
                    let trimmed = first_line.trim().to_string();
                    if !trimmed.is_empty() && trimmed.starts_with('/') {
                        return trimmed;
                    }
                }

                // Fallback auf Ordner-Topic
                if let Some(ref topic) = folder_config.topic {
                    topic.clone()
                } else if let Some(ref fallback) = self.config.fallback_topic {
                    fallback.clone()
                } else {
                    format!("/files/{}", folder_config.path)
                }
            }
            "all" => {
                // Kombiniert: Ordner-Topic hat Priorität, dann Filename-Mapping, dann Content, dann Filename-Stem, dann Fallback
                if let Some(ref topic) = folder_config.topic {
                    return topic.clone();
                }

                if let Some(ref mapping) = self.config.filename_topic_mapping {
                    let filename = file_path
                        .file_name()
                        .map(|f| f.to_string_lossy().to_string())
                        .unwrap_or_default();

                    for (pattern, topic) in mapping {
                        if Self::match_filename_pattern(pattern, &filename) {
                            return topic.clone();
                        }
                    }
                }

                if let Ok(content) = std::fs::read_to_string(file_path)
                    && let Some(first_line) = content.lines().next()
                {
                    let trimmed = first_line.trim().to_string();
                    if trimmed.starts_with('/') {
                        return trimmed;
                    }
                }

                // Dateiname ohne Erweiterung als Topic
                let stem = file_path
                    .file_stem()
                    .map(|s| s.to_string_lossy().to_string())
                    .unwrap_or_default();

                if !stem.is_empty() {
                    return format!("/files/{}", stem);
                }

                if let Some(ref fallback) = self.config.fallback_topic {
                    fallback.clone()
                } else {
                    format!("/files/{}", folder_config.path)
                }
            }
            _ => {
                if let Some(ref topic) = folder_config.topic {
                    topic.clone()
                } else if let Some(ref fallback) = self.config.fallback_topic {
                    fallback.clone()
                } else {
                    format!("/files/{}", folder_config.path)
                }
            }
        }
    }

    /// Matcht einen Dateinamen gegen ein Pattern mit Wildcard (*)
    fn match_filename_pattern(pattern: &str, filename: &str) -> bool {
        if pattern == filename {
            return true;
        }

        if !pattern.contains('*') {
            return false;
        }

        let parts: Vec<&str> = pattern.splitn(2, '*').collect();
        let prefix = parts[0];
        let suffix = if parts.len() > 1 { parts[1] } else { "" };

        if !filename.starts_with(prefix) {
            return false;
        }

        if suffix.is_empty() {
            return true;
        }

        filename.ends_with(suffix)
    }

    /// Erstellt eine Ink-Nachricht aus einer Datei
    fn create_ink(&self, file_path: &Path, topic: String) -> Result<Ink> {
        let content = std::fs::read_to_string(file_path)
            .with_context(|| format!("Failed to read file: {}", file_path.display()))?;

        let metadata = serde_json::json!({
            "file_path": file_path.to_string_lossy().to_string(),
            "file_name": file_path.file_name().map(|f| f.to_string_lossy().to_string()).unwrap_or_default(),
            "file_size": content.len(),
            "folder": file_path.parent().map(|p| p.to_string_lossy().to_string()).unwrap_or_default(),
        });

        let ink = Ink::new(topic, "file_watcher", &self.name, content).with_metadata(metadata);

        Ok(ink)
    }

    /// Führt die konfigurierte Nachverarbeitungsaktion aus
    fn post_process_file(&self, file_path: &Path) {
        let action = self
            .config
            .post_process
            .clone()
            .unwrap_or(PostProcessAction::None);

        match action {
            PostProcessAction::None => {
                debug!("No post-processing action for {}", file_path.display());
            }
            PostProcessAction::Delete => {
                if let Err(e) = std::fs::remove_file(file_path) {
                    warn!("Failed to delete file {}: {}", file_path.display(), e);
                } else {
                    debug!("Deleted file: {}", file_path.display());
                }
            }
            PostProcessAction::Move => {
                if let Some(ref dest_dir) = self.config.move_to {
                    let dest_path =
                        Path::new(dest_dir).join(file_path.file_name().unwrap_or_default());

                    if let Err(e) = std::fs::create_dir_all(dest_dir) {
                        warn!("Failed to create destination directory {}: {}", dest_dir, e);
                        return;
                    }

                    if let Err(e) = std::fs::rename(file_path, &dest_path) {
                        warn!(
                            "Failed to move file {} to {}: {}",
                            file_path.display(),
                            dest_path.display(),
                            e
                        );
                    } else {
                        debug!(
                            "Moved file: {} -> {}",
                            file_path.display(),
                            dest_path.display()
                        );
                    }
                } else {
                    warn!("Move action configured but no move_to directory specified; skipping");
                }
            }
        }
    }

    /// Verarbeitet eine neue Datei
    async fn process_file(&self, file_path: PathBuf, event_bus: Arc<EventBus>) {
        // Prüfen, ob die Datei existiert
        if !file_path.exists() {
            debug!("File does not exist: {}", file_path.display());
            return;
        }

        // Prüfen, ob es eine Datei (kein Verzeichnis) ist
        if !file_path.is_file() {
            debug!("Not a file: {}", file_path.display());
            return;
        }

        // Dateierweiterung prüfen
        if !self.is_allowed_extension(&file_path) {
            debug!("File extension not allowed: {}", file_path.display());
            return;
        }

        // Dateigröße prüfen
        let metadata = match std::fs::metadata(&file_path) {
            Ok(m) => m,
            Err(e) => {
                warn!("Failed to get metadata for {}: {}", file_path.display(), e);
                return;
            }
        };

        if metadata.len() > self.config.max_file_size {
            warn!(
                "File too large ({} bytes), skipping: {}",
                metadata.len(),
                file_path.display()
            );
            return;
        }

        // Ordner-Konfiguration finden
        let folder_config = self
            .config
            .folders
            .iter()
            .find(|f| file_path.starts_with(&f.path))
            .cloned();

        let folder_config = match folder_config {
            Some(fc) => fc,
            None => {
                debug!("No folder config for: {}", file_path.display());
                return;
            }
        };

        // Topic auflösen
        let topic = self.resolve_topic(&folder_config, &file_path);

        // Ink erstellen und veröffentlichen
        match self.create_ink(&file_path, topic.clone()) {
            Ok(ink) => {
                info!(
                    "Publishing file {} to topic: {}",
                    file_path.display(),
                    ink.topic
                );
                event_bus.publish(ink);
            }
            Err(e) => {
                error!(
                    "Failed to create ink for file {}: {}",
                    file_path.display(),
                    e
                );
                return;
            }
        }

        // Nachverarbeitung
        self.post_process_file(&file_path);
    }
}

#[async_trait]
impl Tentacle for FileWatcherTentacle {
    fn name(&self) -> &str {
        &self.name
    }

    fn r#type(&self) -> &str {
        "file_watcher"
    }

    async fn start(&self, event_bus: Arc<EventBus>) -> Result<()> {
        info!(
            "Starting file watcher tentacle '{}', watching {} folder(s)",
            self.name,
            self.config.folders.len()
        );

        // Channel für notify-Events (std::sync::mpsc für notify-Kompatibilität)
        let (tx, rx) = mpsc::channel::<notify::Result<Event>>();

        // Watcher erstellen
        let mut watcher: RecommendedWatcher = NotifyWatcher::new(
            tx,
            notify::Config::default()
                .with_poll_interval(Duration::from_secs(2))
                .with_compare_contents(false),
        )
        .context("Failed to create file watcher")?;

        // Ordner überwachen
        for folder in &self.config.folders {
            let path = Path::new(&folder.path);
            if !path.exists() {
                warn!("Watch folder does not exist, creating: {}", folder.path);
                std::fs::create_dir_all(path)
                    .with_context(|| format!("Failed to create folder: {}", folder.path))?;
            }

            let mode = if folder.recursive {
                RecursiveMode::Recursive
            } else {
                RecursiveMode::NonRecursive
            };

            watcher
                .watch(path, mode)
                .with_context(|| format!("Failed to watch folder: {}", folder.path))?;

            info!(
                "Watching folder: {} (recursive: {})",
                folder.path, folder.recursive
            );
        }

        // Processing delay und event_bus für den Loop
        let processing_delay = self.config.processing_delay;
        let event_bus_clone = Arc::clone(&event_bus);
        let config_clone = self.config.clone();
        let name_clone = self.name.clone();

        // Watcher und Event-Loop in einem Blocking-Thread laufen lassen
        // Der Watcher muss im selben Thread leben, da er den Sender hält
        tokio::task::spawn_blocking(move || {
            // Event-Loop
            while let Ok(result) = rx.recv() {
                match result {
                    Ok(event) => {
                        // Nur auf Create- und Modify-Events reagieren
                        match event.kind {
                            EventKind::Create(_) | EventKind::Modify(_) => {
                                for path in event.paths {
                                    // Nur Dateien verarbeiten
                                    if !path.is_file() {
                                        continue;
                                    }

                                    debug!("File event detected: {}", path.display());

                                    // Verzögerung, um sicherzustellen, dass die Datei vollständig geschrieben ist
                                    let event_bus = Arc::clone(&event_bus_clone);
                                    let config = config_clone.clone();
                                    let name = name_clone.clone();

                                    // Datei-Verarbeitung in einem eigenen async Task
                                    let path_clone = path.clone();
                                    let delay = processing_delay;
                                    tokio::task::spawn(async move {
                                        sleep(Duration::from_millis(delay)).await;

                                        let tentacle = FileWatcherTentacle::new(name, config);
                                        tentacle.process_file(path_clone, event_bus).await;
                                    });
                                }
                            }
                            EventKind::Remove(_) => {
                                // Bei Remove-Events nichts tun — die Datei wurde bereits gelöscht
                            }
                            _ => {
                                // Andere Events ignorieren
                            }
                        }
                    }
                    Err(e) => {
                        error!("File watcher error: {}", e);
                    }
                }
            }

            // Watcher wird hier automatisch dropped, wenn der Channel geschlossen wird
            drop(watcher);
        });

        info!("File watcher tentacle '{}' started successfully", self.name);

        // Blockieren, damit der Tentacle läuft
        tokio::signal::ctrl_c().await?;
        info!("File watcher tentacle '{}' shutting down", self.name);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::appcontext::AppContext;

    #[test]
    fn test_match_filename_pattern_exact() {
        assert!(FileWatcherTentacle::match_filename_pattern(
            "report.txt",
            "report.txt"
        ));
        assert!(!FileWatcherTentacle::match_filename_pattern(
            "report.txt",
            "report.json"
        ));
    }

    #[test]
    fn test_match_filename_pattern_wildcard() {
        assert!(FileWatcherTentacle::match_filename_pattern(
            "*.txt",
            "report.txt"
        ));
        assert!(FileWatcherTentacle::match_filename_pattern(
            "*.txt", "data.txt"
        ));
        assert!(!FileWatcherTentacle::match_filename_pattern(
            "*.txt",
            "report.json"
        ));

        assert!(FileWatcherTentacle::match_filename_pattern(
            "data_*.csv",
            "data_2024.csv"
        ));
        assert!(FileWatcherTentacle::match_filename_pattern(
            "data_*.csv",
            "data_final.csv"
        ));
        assert!(!FileWatcherTentacle::match_filename_pattern(
            "data_*.csv",
            "report.csv"
        ));
    }

    #[test]
    fn test_resolve_topic_folder_mode() {
        let config = FileWatcherTentacleConfig {
            folders: vec![WatchedFolder {
                path: "/watch/inbox".to_string(),
                topic: Some("/files/inbox".to_string()),
                recursive: false,
            }],
            file_extensions: vec![],
            topic_mode: "folder".to_string(),
            fallback_topic: None,
            filename_topic_mapping: None,
            post_process: None,
            move_to: None,
            processing_delay: 500,
            max_file_size: 10 * 1024 * 1024,
        };

        let tentacle = FileWatcherTentacle::new("test".to_string(), config);
        let folder = &tentacle.config.folders[0];
        let topic = tentacle.resolve_topic(folder, Path::new("/watch/inbox/test.txt"));
        assert_eq!(topic, "/files/inbox");
    }

    #[test]
    fn test_resolve_topic_filename_mode() {
        let config = FileWatcherTentacleConfig {
            folders: vec![WatchedFolder {
                path: "/watch/inbox".to_string(),
                topic: None,
                recursive: false,
            }],
            file_extensions: vec![],
            topic_mode: "filename".to_string(),
            fallback_topic: Some("/files/default".to_string()),
            filename_topic_mapping: None,
            post_process: None,
            move_to: None,
            processing_delay: 500,
            max_file_size: 10 * 1024 * 1024,
        };

        let tentacle = FileWatcherTentacle::new("test".to_string(), config);
        let folder = &tentacle.config.folders[0];
        let topic = tentacle.resolve_topic(folder, Path::new("/watch/inbox/myreport.txt"));
        assert_eq!(topic, "/files/myreport");
    }

    #[test]
    fn test_resolve_topic_filename_with_mapping() {
        let mut mapping = HashMap::new();
        mapping.insert("*.json".to_string(), "/files/json".to_string());
        mapping.insert("*.csv".to_string(), "/files/csv".to_string());

        let config = FileWatcherTentacleConfig {
            folders: vec![WatchedFolder {
                path: "/watch/inbox".to_string(),
                topic: None,
                recursive: false,
            }],
            file_extensions: vec![],
            topic_mode: "filename".to_string(),
            fallback_topic: Some("/files/default".to_string()),
            filename_topic_mapping: Some(mapping),
            post_process: None,
            move_to: None,
            processing_delay: 500,
            max_file_size: 10 * 1024 * 1024,
        };

        let tentacle = FileWatcherTentacle::new("test".to_string(), config);
        let folder = &tentacle.config.folders[0];

        let topic = tentacle.resolve_topic(folder, Path::new("/watch/inbox/data.json"));
        assert_eq!(topic, "/files/json");

        let topic = tentacle.resolve_topic(folder, Path::new("/watch/inbox/data.csv"));
        assert_eq!(topic, "/files/csv");

        let topic = tentacle.resolve_topic(folder, Path::new("/watch/inbox/data.txt"));
        assert_eq!(topic, "/files/data");
    }

    #[test]
    fn test_resolve_topic_all_mode() {
        let mut mapping = HashMap::new();
        mapping.insert("*.json".to_string(), "/files/json".to_string());

        let config = FileWatcherTentacleConfig {
            folders: vec![WatchedFolder {
                path: "/watch/inbox".to_string(),
                topic: Some("/files/inbox".to_string()),
                recursive: false,
            }],
            file_extensions: vec![],
            topic_mode: "all".to_string(),
            fallback_topic: Some("/files/default".to_string()),
            filename_topic_mapping: Some(mapping),
            post_process: None,
            move_to: None,
            processing_delay: 500,
            max_file_size: 10 * 1024 * 1024,
        };

        let tentacle = FileWatcherTentacle::new("test".to_string(), config);
        let folder = &tentacle.config.folders[0];

        // Folder topic hat Priorität
        let topic = tentacle.resolve_topic(folder, Path::new("/watch/inbox/data.json"));
        assert_eq!(topic, "/files/inbox");
    }

    #[test]
    fn test_resolve_topic_all_mode_no_folder_topic() {
        let mut mapping = HashMap::new();
        mapping.insert("*.json".to_string(), "/files/json".to_string());

        let config = FileWatcherTentacleConfig {
            folders: vec![WatchedFolder {
                path: "/watch/inbox".to_string(),
                topic: None,
                recursive: false,
            }],
            file_extensions: vec![],
            topic_mode: "all".to_string(),
            fallback_topic: Some("/files/default".to_string()),
            filename_topic_mapping: Some(mapping),
            post_process: None,
            move_to: None,
            processing_delay: 500,
            max_file_size: 10 * 1024 * 1024,
        };

        let tentacle = FileWatcherTentacle::new("test".to_string(), config);
        let folder = &tentacle.config.folders[0];

        // Kein Folder-Topic, Mapping trifft zu
        let topic = tentacle.resolve_topic(folder, Path::new("/watch/inbox/data.json"));
        assert_eq!(topic, "/files/json");

        // Kein Folder-Topic, kein Mapping, Fallback
        let topic = tentacle.resolve_topic(folder, Path::new("/watch/inbox/data.txt"));
        assert_eq!(topic, "/files/data");
    }

    #[test]
    fn test_is_allowed_extension() {
        let config = FileWatcherTentacleConfig {
            folders: vec![WatchedFolder {
                path: "/watch".to_string(),
                topic: None,
                recursive: false,
            }],
            file_extensions: vec![".txt".to_string(), ".json".to_string()],
            topic_mode: "folder".to_string(),
            fallback_topic: None,
            filename_topic_mapping: None,
            post_process: None,
            move_to: None,
            processing_delay: 500,
            max_file_size: 10 * 1024 * 1024,
        };

        let tentacle = FileWatcherTentacle::new("test".to_string(), config);

        assert!(tentacle.is_allowed_extension(Path::new("test.txt")));
        assert!(tentacle.is_allowed_extension(Path::new("test.json")));
        assert!(tentacle.is_allowed_extension(Path::new("test.TXT")));
        assert!(!tentacle.is_allowed_extension(Path::new("test.csv")));
        assert!(!tentacle.is_allowed_extension(Path::new("test")));
    }

    #[test]
    fn test_is_allowed_extension_empty() {
        let config = FileWatcherTentacleConfig {
            folders: vec![WatchedFolder {
                path: "/watch".to_string(),
                topic: None,
                recursive: false,
            }],
            file_extensions: vec![],
            topic_mode: "folder".to_string(),
            fallback_topic: None,
            filename_topic_mapping: None,
            post_process: None,
            move_to: None,
            processing_delay: 500,
            max_file_size: 10 * 1024 * 1024,
        };

        let tentacle = FileWatcherTentacle::new("test".to_string(), config);

        assert!(tentacle.is_allowed_extension(Path::new("test.txt")));
        assert!(tentacle.is_allowed_extension(Path::new("test.json")));
        assert!(tentacle.is_allowed_extension(Path::new("test")));
    }

    #[test]
    fn test_post_process_action_deserialize() {
        let none: PostProcessAction = serde_yaml::from_str("none").unwrap();
        assert_eq!(none, PostProcessAction::None);

        let delete: PostProcessAction = serde_yaml::from_str("delete").unwrap();
        assert_eq!(delete, PostProcessAction::Delete);

        let move_action: PostProcessAction = serde_yaml::from_str("move").unwrap();
        assert_eq!(move_action, PostProcessAction::Move);
    }

    #[test]
    fn test_fallback_topic() {
        let config = FileWatcherTentacleConfig {
            folders: vec![WatchedFolder {
                path: "/watch".to_string(),
                topic: None,
                recursive: false,
            }],
            file_extensions: vec![],
            topic_mode: "folder".to_string(),
            fallback_topic: Some("/files/fallback".to_string()),
            filename_topic_mapping: None,
            post_process: None,
            move_to: None,
            processing_delay: 500,
            max_file_size: 10 * 1024 * 1024,
        };

        let tentacle = FileWatcherTentacle::new("test".to_string(), config);
        let folder = &tentacle.config.folders[0];
        let topic = tentacle.resolve_topic(folder, Path::new("/watch/test.txt"));
        assert_eq!(topic, "/files/fallback");
    }
}
