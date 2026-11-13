#![allow(dead_code)]

use anyhow::{Context, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::{debug, info};

use super::Reflex;
use crate::model::Ink;

/// Configuration for Make Reflex
/// Executes make targets via command line
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct MakeReflexConfig {
    /// Working directory for make
    #[serde(default = "default_dir")]
    pub dir: String,

    /// Default make target
    #[serde(default = "default_target")]
    pub target: String,

    /// Template for make target
    #[serde(default)]
    pub target_template: Option<String>,

    /// Optional: mapping from Ink topics to make targets
    #[serde(default)]
    pub topic_target_mapping: HashMap<String, String>,

    /// Optional: mapping from Ink topics to working directories
    #[serde(default)]
    pub topic_dir_mapping: HashMap<String, String>,

    /// Additional make arguments
    #[serde(default)]
    pub args: Vec<String>,

    /// Environment variables (JSON object template)
    #[serde(default)]
    pub env_template: Option<String>,

    /// Timeout in seconds
    #[serde(default = "default_timeout")]
    pub timeout: u64,

    /// Capture stdout
    #[serde(default = "default_capture_stdout")]
    pub capture_stdout: bool,

    /// Capture stderr
    #[serde(default)]
    pub capture_stderr: bool,
}

fn default_dir() -> String {
    ".".to_string()
}
fn default_target() -> String {
    "all".to_string()
}
fn default_timeout() -> u64 {
    120
}
fn default_capture_stdout() -> bool {
    true
}

pub struct MakeReflex {
    name: String,
    config: MakeReflexConfig,
}

impl MakeReflex {
    pub fn new(name: String, config: MakeReflexConfig) -> Self {
        Self { name, config }
    }

    /// Resolve make target for a given Ink
    fn resolve_target(&self, ink: &Ink) -> String {
        if let Some(target) = self
            .config
            .topic_target_mapping
            .get(&ink.topic)
            .filter(|t| !t.is_empty())
        {
            return self.render_template(target, ink);
        }
        if let Some(ref template) = self.config.target_template {
            return self.render_template(template, ink);
        }
        self.config.target.clone()
    }

    /// Resolve working directory for a given Ink
    fn resolve_dir(&self, ink: &Ink) -> String {
        if let Some(dir) = self
            .config
            .topic_dir_mapping
            .get(&ink.topic)
            .filter(|d| !d.is_empty())
        {
            return dir.clone();
        }
        self.config.dir.clone()
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

    /// Execute make command
    async fn execute_make(&self, dir: &str, target: &str) -> Result<String> {
        info!(
            "Make reflex '{}' executing make {} in {}",
            self.name, target, dir
        );

        let mut cmd = tokio::process::Command::new("make");
        cmd.current_dir(dir).arg(target);

        // Add extra arguments
        for arg in &self.config.args {
            cmd.arg(arg);
        }

        // Set environment variables
        if let Some(ref template) = self.config.env_template {
            let rendered = self.render_template(
                template,
                &Ink::new(
                    "/env".to_string(),
                    "env".to_string(),
                    "make".to_string(),
                    "".to_string(),
                ),
            );
            if let Ok(env_vars) =
                serde_json::from_str::<serde_json::Map<String, serde_json::Value>>(&rendered)
            {
                for (key, value) in env_vars {
                    let value_str = match value {
                        serde_json::Value::String(s) => s.clone(),
                        _ => value.to_string(),
                    };
                    cmd.env(key, value_str);
                }
            }
        }

        let output = cmd
            .output()
            .await
            .with_context(|| format!("Failed to execute make {} in {}", target, dir))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(anyhow::anyhow!("make {} failed: {}", target, stderr));
        }

        let stdout = String::from_utf8_lossy(&output.stdout).to_string();
        if self.config.capture_stderr {
            let stderr = String::from_utf8_lossy(&output.stderr).to_string();
            Ok(format!("{}\n{}", stdout, stderr))
        } else {
            Ok(stdout)
        }
    }

    /// List available make targets
    async fn list_targets(&self, dir: &str) -> Result<Vec<String>> {
        let output = tokio::process::Command::new("make")
            .current_dir(dir)
            .arg("-n")
            .arg("-p")
            .arg("--dry-run")
            .output()
            .await
            .with_context(|| format!("Failed to list make targets in {}", dir))?;

        let stdout = String::from_utf8_lossy(&output.stdout);
        let mut targets = Vec::new();
        for line in stdout.lines() {
            // Simple heuristic: lines that look like target definitions
            if line.starts_with('\t') || (line.contains(':') && !line.contains('=')) {
                let target = line
                    .trim()
                    .split(':')
                    .next()
                    .unwrap_or("")
                    .trim()
                    .to_string();
                if !target.is_empty() && !target.starts_with('.') {
                    targets.push(target);
                }
            }
        }
        Ok(targets)
    }
}

#[async_trait]
impl Reflex for MakeReflex {
    fn name(&self) -> &str {
        &self.name
    }

    fn r#type(&self) -> &str {
        "make"
    }

    async fn handle(&self, ink: &Ink) -> Result<()> {
        info!(
            "Make reflex '{}' handling ink on topic: {}",
            self.name, ink.topic
        );

        let target = self.resolve_target(ink);
        let dir = self.resolve_dir(ink);

        debug!("Make reflex '{}' target={}, dir={}", self.name, target, dir);

        let output = self.execute_make(&dir, &target).await?;

        if self.config.capture_stdout {
            info!("Make reflex '{}' output:\n{}", self.name, output);
        } else {
            info!("Make reflex '{}' completed target: {}", self.name, target);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::appcontext::AppContext;

    fn create_test_config() -> MakeReflexConfig {
        MakeReflexConfig {
            dir: ".".to_string(),
            target: "all".to_string(),
            target_template: None,
            topic_target_mapping: HashMap::new(),
            topic_dir_mapping: HashMap::new(),
            args: vec![],
            env_template: None,
            timeout: 120,
            capture_stdout: true,
            capture_stderr: false,
        }
    }

    fn create_test_ink() -> Ink {
        Ink::new("/test/topic", "test", "test_source", "test body")
    }

    #[test]
    fn test_template_rendering() {
        let config = create_test_config();
        let reflex = MakeReflex::new("test".to_string(), config);
        let ink = create_test_ink();
        assert_eq!(
            reflex.render_template("deploy-{{topic}}", &ink),
            "deploy-/test/topic"
        );
    }

    #[test]
    fn test_config_deserialization() {
        let yaml = r#"
            dir: "/opt/app"
            target: "build"
            args: ["-j4"]
        "#;
        let config: MakeReflexConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.dir, "/opt/app");
        assert_eq!(config.target, "build");
        assert_eq!(config.args, vec!["-j4"]);
    }

    #[test]
    fn test_reflex_type() {
        let config = create_test_config();
        let reflex = MakeReflex::new("test".to_string(), config);
        assert_eq!(reflex.r#type(), "make");
    }

    #[test]
    fn test_resolve_target_from_mapping() {
        let mut config = create_test_config();
        let mut mapping = HashMap::new();
        mapping.insert("/make/deploy".to_string(), "deploy".to_string());
        config.topic_target_mapping = mapping;

        let reflex = MakeReflex::new("test".to_string(), config);
        let ink = Ink::new("/make/deploy", "test", "test_source", "body");
        assert_eq!(reflex.resolve_target(&ink), "deploy");
    }
}
