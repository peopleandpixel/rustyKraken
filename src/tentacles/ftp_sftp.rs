use anyhow::Result;
use async_trait::async_trait;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::RwLock;
use std::time::Duration;
use tracing::{debug, error, info, warn};

use super::Tentacle;
use crate::eventbus::EventBus;
use crate::model::Ink;

/// Protokolltyp
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "lowercase")]
pub enum FtpProtocol {
    #[default]
    Ftp,
    Ftpes,
    Sftp,
}

/// Authentifizierung
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum FtpAuth {
    Password {
        username: String,
        password: String,
    },
    SshKey {
        username: String,
        key_path: String,
        passphrase: Option<String>,
    },
}

impl Default for FtpAuth {
    fn default() -> Self {
        FtpAuth::Password {
            username: String::new(),
            password: String::new(),
        }
    }
}

/// Konfiguration für den FTP/SFTP Tentacle
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FtpSftpTentacleConfig {
    /// Protokolltyp
    #[serde(default)]
    pub protocol: FtpProtocol,
    /// Server-Adresse
    pub host: String,
    /// Port
    #[serde(default = "default_port")]
    pub port: u16,
    /// Authentifizierung
    #[serde(default)]
    pub auth: FtpAuth,
    /// Fernverzeichnis
    #[serde(default = "default_remote_dir")]
    pub remote_directory: String,
    /// Lokales Verzeichnis
    #[serde(default = "default_local_dir")]
    pub local_directory: String,
    /// Dateimuster (z.B. "*.log")
    #[serde(default)]
    pub file_pattern: Option<String>,
    /// Polling-Interval in Sekunden
    #[serde(default = "default_poll_interval")]
    pub poll_interval_secs: u64,
    /// Dateien nach Download löschen
    #[serde(default)]
    pub delete_after_download: bool,
    /// Nur neue Dateien verarbeiten
    #[serde(default)]
    pub new_files_only: bool,
    /// Maximale Dateigröße in Bytes (0 = unbegrenzt)
    #[serde(default)]
    pub max_file_size: u64,
    /// Timeout in Sekunden
    #[serde(default = "default_timeout")]
    pub timeout_secs: u64,
    /// Reconnect-Verzögerung
    #[serde(default = "default_reconnect_delay")]
    pub reconnect_delay_secs: u64,
    /// Topic für Events
    #[serde(default = "default_topic")]
    pub topic: String,
    /// Rekursive Verzeichnis-Durchsuchung
    #[serde(default)]
    pub recursive: bool,
    /// REST API Endpoint (für FTP-Server mit REST-Schnittstelle)
    #[serde(default)]
    pub rest_api_endpoint: Option<String>,
    /// Benutze REST API statt direkter FTP-Verbindung
    #[serde(default)]
    pub use_rest_api: bool,
}

fn default_port() -> u16 {
    21
}

fn default_remote_dir() -> String {
    "/".to_string()
}

fn default_local_dir() -> String {
    "./data/ftp".to_string()
}

fn default_poll_interval() -> u64 {
    60
}

fn default_timeout() -> u64 {
    30
}

fn default_reconnect_delay() -> u64 {
    5
}

fn default_topic() -> String {
    "/ftp/incoming".to_string()
}

#[derive(Debug, Clone)]
struct FileInfo {
    name: String,
    size: u64,
    #[allow(dead_code)]
    modified: Option<chrono::DateTime<chrono::Utc>>,
}

pub struct FtpSftpTentacle {
    name: String,
    config: FtpSftpTentacleConfig,
    processed_files: Arc<RwLock<HashMap<String, chrono::DateTime<chrono::Utc>>>>,
}

impl FtpSftpTentacle {
    pub fn new(name: String, config: FtpSftpTentacleConfig) -> Self {
        Self {
            name,
            config,
            processed_files: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    #[allow(dead_code)]
    fn matches_pattern(&self, filename: &str) -> bool {
        if let Some(ref pattern) = self.config.file_pattern {
            if pattern.starts_with("*.") {
                let ext = &pattern[1..];
                return filename.ends_with(ext);
            }
            return filename == pattern;
        }
        true
    }

    #[allow(dead_code)]
    fn create_file_ink(&self, file_info: &FileInfo, local_path: &str, tentacle_name: &str) -> Ink {
        let mut meta_map = serde_json::Map::new();
        meta_map.insert(
            "protocol".into(),
            serde_json::json!(match self.config.protocol {
                FtpProtocol::Ftp => "ftp",
                FtpProtocol::Ftpes => "ftpes",
                FtpProtocol::Sftp => "sftp",
            }),
        );
        meta_map.insert("host".into(), serde_json::json!(self.config.host));
        meta_map.insert("port".into(), serde_json::json!(self.config.port));
        meta_map.insert(
            "remote_directory".into(),
            serde_json::json!(self.config.remote_directory),
        );
        meta_map.insert("file_name".into(), serde_json::json!(file_info.name));
        meta_map.insert("file_size".into(), serde_json::json!(file_info.size));
        meta_map.insert("local_path".into(), serde_json::json!(local_path));
        meta_map.insert(
            "delete_after_download".into(),
            serde_json::json!(self.config.delete_after_download),
        );

        if let Some(modified) = file_info.modified {
            meta_map.insert(
                "file_modified".into(),
                serde_json::json!(modified.to_rfc3339()),
            );
        }

        Ink::new(
            &self.config.topic,
            "ftp_sftp",
            tentacle_name,
            format!(
                "File downloaded: {} ({} bytes)",
                file_info.name, file_info.size
            ),
        )
        .with_metadata(serde_json::Value::Object(meta_map))
    }
}

#[async_trait]
impl Tentacle for FtpSftpTentacle {
    fn name(&self) -> &str {
        &self.name
    }

    fn r#type(&self) -> &str {
        "ftp_sftp"
    }

    async fn start(&self, event_bus: Arc<EventBus>) -> Result<()> {
        info!(
            "FTP/SFTP Tentacle '{}' starting for {}:{} ({:?})",
            self.name, self.config.host, self.config.port, self.config.protocol
        );

        let config = self.config.clone();
        let tentacle_name = self.name.clone();
        let event_bus_clone = Arc::clone(&event_bus);
        let processed_files = Arc::clone(&self.processed_files);

        std::fs::create_dir_all(&config.local_directory)
            .map_err(|e| anyhow::anyhow!("Failed to create local directory: {}", e))?;

        tokio::spawn(async move {
            loop {
                match poll_files(&config, &tentacle_name, &event_bus_clone, &processed_files).await
                {
                    Ok(count) => {
                        if count > 0 {
                            info!(
                                "FTP/SFTP Tentacle '{}': processed {} files",
                                tentacle_name, count
                            );
                        }
                    }
                    Err(e) => {
                        error!("FTP/SFTP Tentacle '{}': poll failed: {}", tentacle_name, e);
                    }
                }

                tokio::time::sleep(Duration::from_secs(config.poll_interval_secs)).await;
            }
        });

        loop {
            tokio::time::sleep(Duration::from_secs(60)).await;
        }
    }
}

async fn poll_files(
    config: &FtpSftpTentacleConfig,
    tentacle_name: &str,
    event_bus: &Arc<EventBus>,
    processed_files: &Arc<RwLock<HashMap<String, chrono::DateTime<chrono::Utc>>>>,
) -> Result<u32> {
    let mut count = 0u32;

    // REST API Mode (für FTP-Server mit REST-Schnittstelle)
    if config.use_rest_api {
        let files = fetch_files_via_rest(config, tentacle_name).await?;
        for file_info in files {
            let now = chrono::Utc::now();
            let name = file_info.name.clone();

            // Pattern filter
            if !matches_pattern_simple(&config.file_pattern, &name) {
                continue;
            }

            // Size check
            if config.max_file_size > 0 && file_info.size > config.max_file_size {
                warn!(
                    "FTP/SFTP Tentacle '{}': skipping file (too large): {} ({} bytes)",
                    tentacle_name, name, file_info.size
                );
                continue;
            }

            // Already processed?
            {
                let processed = processed_files
                    .read()
                    .map_err(|e| anyhow::anyhow!("Lock error: {}", e))?;
                if let Some(last_seen) = processed.get(&name)
                    && (!config.new_files_only || *last_seen < now - chrono::Duration::seconds(60))
                {
                    debug!(
                        "FTP/SFTP Tentacle '{}': skipping processed file: {}",
                        tentacle_name, name
                    );
                    continue;
                }
            }

            // Download file
            let local_path = format!("{}/{}", config.local_directory, name);
            if let Err(e) = download_file_via_rest(config, &name, &local_path).await {
                warn!(
                    "FTP/SFTP Tentacle '{}': failed to download {}: {}",
                    tentacle_name, name, e
                );
                continue;
            }

            info!(
                "FTP/SFTP Tentacle '{}': downloaded {} to {}",
                tentacle_name, name, local_path
            );

            // Mark as processed
            {
                let mut processed = processed_files
                    .write()
                    .map_err(|e| anyhow::anyhow!("Lock error: {}", e))?;
                processed.insert(name.clone(), now);
            }

            let ink = Ink::new(
                &config.topic,
                "ftp_sftp",
                tentacle_name,
                format!(
                    "File downloaded: {} ({} bytes)",
                    file_info.name, file_info.size
                ),
            );
            event_bus.publish(ink);
            count += 1;
        }
    } else {
        // Direct FTP/SFTP mode — log a warning that this requires external tools
        warn!(
            "FTP/SFTP Tentacle '{}': direct FTP/SFTP mode requires 'use_rest_api: true' or external FTP client. Use REST API mode for file monitoring.",
            tentacle_name
        );
        // In production, this would use suppaftp/ssh2 crates
        // For now, we only support REST API mode
    }

    Ok(count)
}

fn matches_pattern_simple(pattern: &Option<String>, filename: &str) -> bool {
    if let Some(pat) = pattern {
        if pat.starts_with("*.") {
            let ext = &pat[1..];
            return filename.ends_with(ext);
        }
        return filename == pat;
    }
    true
}

async fn fetch_files_via_rest(
    config: &FtpSftpTentacleConfig,
    tentacle_name: &str,
) -> Result<Vec<FileInfo>> {
    let endpoint = config.rest_api_endpoint.as_deref().unwrap_or("/api/files");
    let url = format!(
        "http://{}:{}{}{}",
        config.host, config.port, endpoint, config.remote_directory
    );

    info!(
        "FTP/SFTP Tentacle '{}': fetching file list from {}",
        tentacle_name, url
    );

    let client = Client::builder()
        .timeout(Duration::from_secs(config.timeout_secs))
        .build()
        .map_err(|e| anyhow::anyhow!("Failed to build HTTP client: {}", e))?;

    let mut req = client.get(&url);
    match &config.auth {
        FtpAuth::Password { username, password } => {
            req = req.basic_auth(username, Some(password));
        }
        FtpAuth::SshKey { .. } => {
            return Err(anyhow::anyhow!(
                "SSH key auth not supported for REST API mode"
            ));
        }
    }

    let response = req.send().await?;
    if !response.status().is_success() {
        return Err(anyhow::anyhow!(
            "REST API request failed with status: {}",
            response.status()
        ));
    }

    let body = response.text().await?;

    // Try to parse as JSON array of file objects
    if let Ok(files) = serde_json::from_str::<Vec<serde_json::Value>>(&body) {
        let mut result = Vec::new();
        for f in files {
            let name = f
                .get("name")
                .and_then(|n| n.as_str())
                .unwrap_or("")
                .to_string();
            let size = f.get("size").and_then(|s| s.as_u64()).unwrap_or(0);
            let modified = f
                .get("modified")
                .and_then(|m| m.as_str())
                .and_then(|d| chrono::DateTime::parse_from_rfc3339(d).ok())
                .map(|dt| dt.with_timezone(&chrono::Utc));

            if !name.is_empty() {
                result.push(FileInfo {
                    name,
                    size,
                    modified,
                });
            }
        }
        debug!(
            "FTP/SFTP Tentacle '{}': fetched {} files via REST API",
            tentacle_name,
            result.len()
        );
        return Ok(result);
    }

    // Fallback: parse as plain text (one filename per line)
    let files: Vec<FileInfo> = body
        .lines()
        .filter(|l| !l.is_empty())
        .map(|line| FileInfo {
            name: line.trim().to_string(),
            size: 0,
            modified: None,
        })
        .collect();

    debug!(
        "FTP/SFTP Tentacle '{}': fetched {} files (plain text)",
        tentacle_name,
        files.len()
    );
    Ok(files)
}

async fn download_file_via_rest(
    config: &FtpSftpTentacleConfig,
    filename: &str,
    local_path: &str,
) -> Result<()> {
    let endpoint = config.rest_api_endpoint.as_deref().unwrap_or("/api/files");
    let url = format!(
        "http://{}:{}{}{}/{}",
        config.host, config.port, endpoint, config.remote_directory, filename
    );

    let client = Client::builder()
        .timeout(Duration::from_secs(config.timeout_secs))
        .build()
        .map_err(|e| anyhow::anyhow!("Failed to build HTTP client: {}", e))?;

    let mut req = client.get(&url);
    match &config.auth {
        FtpAuth::Password { username, password } => {
            req = req.basic_auth(username, Some(password));
        }
        FtpAuth::SshKey { .. } => {}
    }

    let response = req.send().await?;
    if !response.status().is_success() {
        return Err(anyhow::anyhow!(
            "Download failed with status: {}",
            response.status()
        ));
    }

    let bytes = response.bytes().await?;
    tokio::fs::write(local_path, &bytes).await?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::appcontext::AppContext;

    #[test]
    fn test_ftp_config_defaults() {
        let config: FtpSftpTentacleConfig = serde_json::from_str(
            r#"{"host": "ftp.example.com", "auth": {"type": "password", "username": "user", "password": "pass"}}"#,
        )
        .unwrap();

        assert_eq!(config.protocol, FtpProtocol::Ftp);
        assert_eq!(config.port, 21);
        assert_eq!(config.remote_directory, "/");
        assert_eq!(config.local_directory, "./data/ftp");
        assert_eq!(config.poll_interval_secs, 60);
        assert_eq!(config.topic, "/ftp/incoming");
        assert!(!config.delete_after_download);
        assert!(!config.new_files_only);
        assert_eq!(config.max_file_size, 0);
        assert!(!config.recursive);
        assert!(!config.use_rest_api);
    }

    #[test]
    fn test_sftp_config() {
        let config: FtpSftpTentacleConfig = serde_json::from_str(
            r#"{"protocol": "sftp", "host": "sftp.example.com", "auth": {"type": "password", "username": "user", "password": "pass"}}"#,
        )
        .unwrap();

        assert_eq!(config.protocol, FtpProtocol::Sftp);
    }

    #[test]
    fn test_ftpes_protocol() {
        let config: FtpSftpTentacleConfig = serde_json::from_str(
            r#"{"protocol": "ftpes", "host": "ftpes.example.com", "auth": {"type": "password", "username": "user", "password": "pass"}}"#,
        )
        .unwrap();

        assert_eq!(config.protocol, FtpProtocol::Ftpes);
    }

    #[test]
    fn test_ssh_key_auth() {
        let config: FtpSftpTentacleConfig = serde_json::from_str(
            r#"{"protocol": "sftp", "host": "sftp.example.com", "auth": {"type": "sshkey", "username": "user", "key_path": "~/.ssh/id_rsa"}}"#,
        )
        .unwrap();

        match config.auth {
            FtpAuth::SshKey {
                username, key_path, ..
            } => {
                assert_eq!(username, "user");
                assert_eq!(key_path, "~/.ssh/id_rsa");
            }
            _ => panic!("Expected SSH key auth"),
        }
    }

    #[test]
    fn test_file_pattern_filtering() {
        let tentacle = FtpSftpTentacle::new(
            "test".to_string(),
            serde_json::from_str(
                r#"{"host": "ftp.example.com", "auth": {"type": "password", "username": "user", "password": "pass"}, "file_pattern": "*.log"}"#,
            )
            .unwrap(),
        );

        assert!(tentacle.matches_pattern("app.log"));
        assert!(tentacle.matches_pattern("error.log"));
        assert!(!tentacle.matches_pattern("app.txt"));
    }

    #[test]
    fn test_no_pattern_matches_all() {
        let tentacle = FtpSftpTentacle::new(
            "test".to_string(),
            serde_json::from_str(
                r#"{"host": "ftp.example.com", "auth": {"type": "password", "username": "user", "password": "pass"}}"#,
            )
            .unwrap(),
        );

        assert!(tentacle.matches_pattern("anything.txt"));
        assert!(tentacle.matches_pattern("no_extension"));
    }

    #[test]
    fn test_exact_pattern_match() {
        let tentacle = FtpSftpTentacle::new(
            "test".to_string(),
            serde_json::from_str(
                r#"{"host": "ftp.example.com", "auth": {"type": "password", "username": "user", "password": "pass"}, "file_pattern": "specific.txt"}"#,
            )
            .unwrap(),
        );

        assert!(tentacle.matches_pattern("specific.txt"));
        assert!(!tentacle.matches_pattern("other.txt"));
    }

    #[test]
    fn test_create_file_ink() {
        let tentacle = FtpSftpTentacle::new(
            "test".to_string(),
            serde_json::from_str(
                r#"{"protocol": "sftp", "host": "sftp.example.com", "port": 22, "auth": {"type": "password", "username": "user", "password": "pass"}, "topic": "/sftp/incoming"}"#,
            )
            .unwrap(),
        );

        let file_info = FileInfo {
            name: "test.txt".to_string(),
            size: 1024,
            modified: None,
        };

        let ink = tentacle.create_file_ink(&file_info, "/local/test.txt", "test");

        assert_eq!(ink.topic, "/sftp/incoming");
        assert!(ink.body.contains("test.txt"));
        assert!(ink.body.contains("1024"));
    }

    #[test]
    fn test_matches_pattern_simple() {
        let pattern = Some("*.log".to_string());
        assert!(matches_pattern_simple(&pattern, "app.log"));
        assert!(!matches_pattern_simple(&pattern, "app.txt"));

        let no_pattern: Option<String> = None;
        assert!(matches_pattern_simple(&no_pattern, "anything"));
    }

    #[test]
    fn test_custom_port() {
        let config: FtpSftpTentacleConfig = serde_json::from_str(
            r#"{"host": "ftp.example.com", "port": 2121, "auth": {"type": "password", "username": "user", "password": "pass"}}"#,
        )
        .unwrap();

        assert_eq!(config.port, 2121);
    }

    #[test]
    fn test_delete_after_download() {
        let config: FtpSftpTentacleConfig = serde_json::from_str(
            r#"{"host": "ftp.example.com", "auth": {"type": "password", "username": "user", "password": "pass"}, "delete_after_download": true}"#,
        )
        .unwrap();

        assert!(config.delete_after_download);
    }

    #[test]
    fn test_max_file_size() {
        let config: FtpSftpTentacleConfig = serde_json::from_str(
            r#"{"host": "ftp.example.com", "auth": {"type": "password", "username": "user", "password": "pass"}, "max_file_size": 10485760}"#,
        )
        .unwrap();

        assert_eq!(config.max_file_size, 10485760);
    }

    #[test]
    fn test_recursive_mode() {
        let config: FtpSftpTentacleConfig = serde_json::from_str(
            r#"{"host": "ftp.example.com", "auth": {"type": "password", "username": "user", "password": "pass"}, "recursive": true}"#,
        )
        .unwrap();

        assert!(config.recursive);
    }

    #[test]
    fn test_rest_api_mode() {
        let config: FtpSftpTentacleConfig = serde_json::from_str(
            r#"{"host": "ftp.example.com", "auth": {"type": "password", "username": "user", "password": "pass"}, "use_rest_api": true, "rest_api_endpoint": "/api/v1/files"}"#,
        )
        .unwrap();

        assert!(config.use_rest_api);
        assert_eq!(config.rest_api_endpoint, Some("/api/v1/files".to_string()));
    }

    #[test]
    fn test_new_files_only() {
        let config: FtpSftpTentacleConfig = serde_json::from_str(
            r#"{"host": "ftp.example.com", "auth": {"type": "password", "username": "user", "password": "pass"}, "new_files_only": true}"#,
        )
        .unwrap();

        assert!(config.new_files_only);
    }
}
