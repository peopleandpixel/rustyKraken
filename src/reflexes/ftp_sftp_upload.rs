use anyhow::{Context, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, info};

use super::Reflex;
use crate::appcontext::AppContext;
use crate::model::Ink;

/// Configuration for FTP/SFTP Upload Reflex
/// Uploads files via FTP or SFTP
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FtpSftpUploadReflexConfig {
    /// Protocol: "ftp", "ftps", "sftp"
    #[serde(default = "default_protocol")]
    pub protocol: String,

    /// Server host
    pub host: String,

    /// Server port
    #[serde(default = "default_port")]
    pub port: u16,

    /// Username
    pub username: String,

    /// Password or private key path (for SFTP)
    pub password: String,

    /// Remote directory path
    #[serde(default = "default_remote_dir")]
    pub remote_dir: String,

    /// Template for remote file name
    #[serde(default = "default_filename_template")]
    pub filename_template: String,

    /// Template for file body
    #[serde(default = "default_body_template")]
    pub body_template: String,

    /// Optional: mapping from Ink topics to remote directories
    #[serde(default)]
    pub topic_dir_mapping: HashMap<String, String>,

    /// Optional: mapping from Ink topics to filename templates
    #[serde(default)]
    pub topic_filename_mapping: HashMap<String, String>,

    /// Create directories if they don't exist
    #[serde(default)]
    pub create_dirs: bool,

    /// Timeout in seconds
    #[serde(default = "default_timeout")]
    pub timeout: u64,
}

fn default_protocol() -> String {
    "sftp".to_string()
}
fn default_port() -> u16 {
    22
}
fn default_remote_dir() -> String {
    "/".to_string()
}
fn default_filename_template() -> String {
    "{{topic}}_{{id}}.txt".to_string()
}
fn default_body_template() -> String {
    "{{body}}".to_string()
}
fn default_timeout() -> u64 {
    30
}

pub struct FtpSftpUploadReflex {
    name: String,
    config: FtpSftpUploadReflexConfig,
    client: Arc<reqwest::Client>,
}

impl FtpSftpUploadReflex {
    pub fn new(name: String, config: FtpSftpUploadReflexConfig, app_ctx: &AppContext) -> Self {
        Self {
            name,
            config,
            client: Arc::clone(app_ctx.http_client()),
        }
    }

    /// Resolve remote directory for a given Ink
    fn resolve_remote_dir(&self, ink: &Ink) -> String {
        if let Some(dir) = self
            .config
            .topic_dir_mapping
            .get(&ink.topic)
            .filter(|d| !d.is_empty())
        {
            return dir.clone();
        }
        self.config.remote_dir.clone()
    }

    /// Resolve filename for a given Ink
    fn resolve_filename(&self, ink: &Ink) -> String {
        if let Some(filename) = self
            .config
            .topic_filename_mapping
            .get(&ink.topic)
            .filter(|f| !f.is_empty())
        {
            return self.render_template(filename, ink);
        }
        self.render_template(&self.config.filename_template, ink)
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

    /// Upload via FTP (using curl command)
    async fn upload_ftp(&self, remote_path: &str, body: &str) -> Result<()> {
        info!(
            "FTP/SFTP reflex '{}' uploading via FTP to {}",
            self.name, remote_path
        );

        // Write body to temp file
        let temp_file = format!("/tmp/kraken_ftp_{}.tmp", uuid::Uuid::new_v4());
        std::fs::write(&temp_file, body).context("Failed to write temp file for FTP upload")?;

        let url = if self.config.protocol == "ftps" {
            format!("ftps://{}:{}", self.config.host, self.config.port)
        } else {
            format!("ftp://{}:{}", self.config.host, self.config.port)
        };

        let result = tokio::process::Command::new("curl")
            .args([
                "-T",
                &temp_file,
                "-u",
                &format!("{}:{}", self.config.username, self.config.password),
                &format!("{}/{}", url, remote_path),
                "--connect-timeout",
                &self.config.timeout.to_string(),
            ])
            .output()
            .await
            .context("Failed to execute curl for FTP upload")?;

        // Clean up temp file
        let _ = std::fs::remove_file(&temp_file);

        if !result.status.success() {
            let stderr = String::from_utf8_lossy(&result.stderr);
            return Err(anyhow::anyhow!("FTP upload failed: {}", stderr));
        }

        Ok(())
    }

    /// Upload via SFTP (using sftp command or ssh)
    async fn upload_sftp(&self, remote_path: &str, body: &str) -> Result<()> {
        info!(
            "FTP/SFTP reflex '{}' uploading via SFTP to {}",
            self.name, remote_path
        );

        // Write body to temp file
        let temp_file = format!("/tmp/kraken_sftp_{}.tmp", uuid::Uuid::new_v4());
        std::fs::write(&temp_file, body).context("Failed to write temp file for SFTP upload")?;

        // Create remote directory if needed
        if self.config.create_dirs {
            let dir = remote_path.rsplit('/').next().unwrap_or(".");
            if !dir.is_empty() && dir != "." {
                let _ = tokio::process::Command::new("ssh")
                    .args([
                        "-p",
                        &self.config.port.to_string(),
                        &format!("{}@{}", self.config.username, self.config.host),
                        "mkdir",
                        "-p",
                        dir,
                    ])
                    .output()
                    .await;
            }
        }

        let mut cmd = tokio::process::Command::new("sftp");
        cmd.args([
            "-P",
            &self.config.port.to_string(),
            &format!("{}@{}", self.config.username, self.config.host),
        ]);

        // Use password or key
        if self.config.password.starts_with("/最") {
            // It's a key path
            cmd.args(["-i", &self.config.password]);
        }

        let _sftp_command = format!("put {} {}\nbye\n", temp_file, remote_path);

        let result = cmd
            .stdin(std::process::Stdio::piped())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .spawn()
            .context("Failed to spawn sftp")?
            .wait_with_output()
            .await
            .context("Failed to wait for sftp")?;

        // Clean up temp file
        let _ = std::fs::remove_file(&temp_file);

        if !result.status.success() {
            let stderr = String::from_utf8_lossy(&result.stderr);
            return Err(anyhow::anyhow!("SFTP upload failed: {}", stderr));
        }

        Ok(())
    }

    /// Upload via HTTP PUT (fallback for FTP servers with HTTP support)
    async fn upload_http(&self, remote_path: &str, body: &str) -> Result<()> {
        let url = format!(
            "http://{}:{}/{}",
            self.config.host, self.config.port, remote_path
        );

        let response = self
            .client
            .put(&url)
            .basic_auth(&self.config.username, Some(&self.config.password))
            .body(body.to_string())
            .send()
            .await
            .with_context(|| format!("Failed to upload via HTTP to {}", url))?;

        if !response.status().is_success() {
            let status = response.status();
            let error_body = response.text().await.unwrap_or_default();
            return Err(anyhow::anyhow!(
                "HTTP upload error ({}): {}",
                status,
                error_body
            ));
        }

        Ok(())
    }
}

#[async_trait]
impl Reflex for FtpSftpUploadReflex {
    fn name(&self) -> &str {
        &self.name
    }

    fn r#type(&self) -> &str {
        "ftp_sftp_upload"
    }

    async fn handle(&self, ink: &Ink) -> Result<()> {
        info!(
            "FTP/SFTP reflex '{}' handling ink on topic: {}",
            self.name, ink.topic
        );

        // Resolve remote directory
        let remote_dir = self.resolve_remote_dir(ink);
        debug!(
            "FTP/SFTP reflex '{}' resolved remote dir: {}",
            self.name, remote_dir
        );

        // Resolve filename
        let filename = self.resolve_filename(ink);
        debug!(
            "FTP/SFTP reflex '{}' resolved filename: {}",
            self.name, filename
        );

        // Build remote path
        let remote_path = format!("{}/{}", remote_dir.trim_end_matches('/'), filename);

        // Render body
        let body = self.render_template(&self.config.body_template, ink);
        debug!(
            "FTP/SFTP reflex '{}' body length: {}",
            self.name,
            body.len()
        );

        // Upload based on protocol
        match self.config.protocol.as_str() {
            "ftp" | "ftps" => self.upload_ftp(&remote_path, &body).await?,
            "sftp" => self.upload_sftp(&remote_path, &body).await?,
            "http" | "https" => self.upload_http(&remote_path, &body).await?,
            _ => {
                return Err(anyhow::anyhow!(
                    "Unsupported protocol: {}",
                    self.config.protocol
                ));
            }
        }

        info!(
            "FTP/SFTP reflex '{}' uploaded file to {}/{}",
            self.name, remote_dir, filename
        );

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_config() -> FtpSftpUploadReflexConfig {
        FtpSftpUploadReflexConfig {
            protocol: "sftp".to_string(),
            host: "localhost".to_string(),
            port: 22,
            username: "user".to_string(),
            password: "secret".to_string(),
            remote_dir: "/uploads".to_string(),
            filename_template: "{{topic}}_{{id}}.txt".to_string(),
            body_template: "{{body}}".to_string(),
            topic_dir_mapping: HashMap::new(),
            topic_filename_mapping: HashMap::new(),
            create_dirs: true,
            timeout: 30,
        }
    }

    fn create_test_ink() -> Ink {
        Ink::new("/test/topic", "test", "test_source", "test body")
    }

    #[test]
    fn test_template_rendering() {
        let config = create_test_config();
        let reflex = FtpSftpUploadReflex::new(
            "test".to_string(),
            config,
            &crate::appcontext::AppContext::new(),
        );
        let ink = create_test_ink();
        let filename = reflex.resolve_filename(&ink);
        assert!(filename.contains("/test/topic"));
        assert!(filename.ends_with(".txt"));
    }

    #[test]
    fn test_config_deserialization() {
        let yaml = r#"
            protocol: "ftp"
            host: "ftp.example.com"
            port: 21
            username: "ftpuser"
            password: "ftppass"
            remote_dir: "/incoming"
        "#;
        let config: FtpSftpUploadReflexConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.protocol, "ftp");
        assert_eq!(config.port, 21);
    }

    #[test]
    fn test_reflex_type() {
        let config = create_test_config();
        let reflex = FtpSftpUploadReflex::new(
            "test".to_string(),
            config,
            &crate::appcontext::AppContext::new(),
        );
        assert_eq!(reflex.r#type(), "ftp_sftp_upload");
    }

    #[test]
    fn test_resolve_dir_from_mapping() {
        let mut config = create_test_config();
        let mut mapping = HashMap::new();
        mapping.insert("/logs/app".to_string(), "/logs/app".to_string());
        config.topic_dir_mapping = mapping;

        let reflex = FtpSftpUploadReflex::new(
            "test".to_string(),
            config,
            &crate::appcontext::AppContext::new(),
        );
        let ink = Ink::new("/logs/app", "test", "test_source", "body");
        assert_eq!(reflex.resolve_remote_dir(&ink), "/logs/app");
    }
}
