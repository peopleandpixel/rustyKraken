use anyhow::{Context, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::{debug, error, info};

use super::Reflex;
use crate::model::Ink;

/// Configuration for Fail2Ban Reflex
/// Interacts with Fail2Ban via API or command line
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Fail2BanReflexConfig {
    /// Fail2Ban API socket path or REST API URL
    #[serde(default = "default_socket")]
    pub socket: String,

    /// Default action: "ban", "unban", "status", "get_info"
    #[serde(default = "default_action")]
    pub action: String,

    /// Default IP address to ban/unban
    #[serde(default)]
    pub ip: Option<String>,

    /// Template for IP address
    #[serde(default)]
    pub ip_template: Option<String>,

    /// Default jail name
    #[serde(default)]
    pub jail: Option<String>,

    /// Optional: mapping from Ink topics to actions
    #[serde(default)]
    pub topic_action_mapping: HashMap<String, String>,

    /// Optional: mapping from Ink topics to IP addresses
    #[serde(default)]
    pub topic_ip_mapping: HashMap<String, String>,

    /// Optional: mapping from Ink topics to jail names
    #[serde(default)]
    pub topic_jail_mapping: HashMap<String, String>,

    /// Ban duration in seconds (for ban action)
    #[serde(default = "default_ban_time")]
    pub ban_time: i64,

    /// Timeout in seconds
    #[serde(default = "default_timeout")]
    pub timeout: u64,
}

fn default_socket() -> String {
    "/var/run/fail2ban/fail2ban.sock".to_string()
}
fn default_action() -> String {
    "status".to_string()
}
fn default_ban_time() -> i64 {
    3600
}
fn default_timeout() -> u64 {
    30
}

pub struct Fail2BanReflex {
    name: String,
    config: Fail2BanReflexConfig,
}

impl Fail2BanReflex {
    pub fn new(name: String, config: Fail2BanReflexConfig) -> Self {
        Self { name, config }
    }

    /// Resolve action for a given Ink
    fn resolve_action(&self, ink: &Ink) -> String {
        if let Some(action) = self
            .config
            .topic_action_mapping
            .get(&ink.topic)
            .filter(|a| !a.is_empty())
        {
            return action.clone();
        }
        self.config.action.clone()
    }

    /// Resolve IP address for a given Ink
    fn resolve_ip(&self, ink: &Ink) -> Option<String> {
        if let Some(ip) = self
            .config
            .topic_ip_mapping
            .get(&ink.topic)
            .filter(|i| !i.is_empty())
        {
            return Some(ip.clone());
        }
        if let Some(ref template) = self.config.ip_template {
            return Some(self.render_template(template, ink));
        }
        self.config.ip.clone()
    }

    /// Resolve jail name for a given Ink
    fn resolve_jail(&self, ink: &Ink) -> Option<String> {
        if let Some(jail) = self
            .config
            .topic_jail_mapping
            .get(&ink.topic)
            .filter(|j| !j.is_empty())
        {
            return Some(jail.clone());
        }
        self.config.jail.clone()
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

    /// Execute fail2ban-client command
    async fn execute_command(&self, args: &[&str]) -> Result<String> {
        let result = tokio::process::Command::new("fail2ban-client")
            .args(args.iter())
            .output()
            .await
            .context("Failed to execute fail2ban-client")?;

        if !result.status.success() {
            let stderr = String::from_utf8_lossy(&result.stderr);
            return Err(anyhow::anyhow!("fail2ban-client error: {}", stderr));
        }

        Ok(String::from_utf8_lossy(&result.stdout).to_string())
    }

    /// Ban an IP address
    async fn ban_ip(&self, ip: &str, jail: Option<&str>) -> Result<()> {
        info!(
            "Fail2Ban reflex '{}' banning IP: {}",
            self.name, ip
        );

        let jail_name = jail.unwrap_or("sshd");
        let output = self
            .execute_command(&["set", jail_name, "banip", ip])
            .await?;

        info!(
            "Fail2Ban reflex '{}' banned IP {} in jail {}: {}",
            self.name, ip, jail_name, output
        );
        Ok(())
    }

    /// Unban an IP address
    async fn unban_ip(&self, ip: &str, jail: Option<&str>) -> Result<()> {
        info!(
            "Fail2Ban reflex '{}' unbanning IP: {}",
            self.name, ip
        );

        let jail_name = jail.unwrap_or("sshd");
        let output = self
            .execute_command(&["set", jail_name, "unbanip", ip])
            .await?;

        info!(
            "Fail2Ban reflex '{}' unbanned IP {} in jail {}: {}",
            self.name, ip, jail_name, output
        );
        Ok(())
    }

    /// Get status of a jail
    async fn get_status(&self, jail: Option<&str>) -> Result<String> {
        if let Some(jail_name) = jail {
            self.execute_command(&["status", jail_name]).await
        } else {
            self.execute_command(&["status"]).await
        }
    }

    /// Get info about a jail
    async fn get_info(&self, jail: Option<&str>) -> Result<String> {
        if let Some(jail_name) = jail {
            self.execute_command(&["get", jail_name, "usedns"]).await
        } else {
            self.execute_command(&["ping"]).await
        }
    }

    /// List all jails
    async fn list_jails(&self) -> Result<Vec<String>> {
        let output = self.execute_command(&["status"]).await?;
        let mut jails = Vec::new();
        for line in output.lines() {
            if let Some(jail) = line.strip_prefix("  Jail: ") {
                jails.push(jail.to_string());
            }
        }
        Ok(jails)
    }
}

#[async_trait]
impl Reflex for Fail2BanReflex {
    fn name(&self) -> &str {
        &self.name
    }

    fn r#type(&self) -> &str {
        "fail2ban"
    }

    async fn handle(&self, ink: &Ink) -> Result<()> {
        info!(
            "Fail2Ban reflex '{}' handling ink on topic: {}",
            self.name, ink.topic
        );

        let action = self.resolve_action(ink);
        let ip = self.resolve_ip(ink);
        let jail = self.resolve_jail(ink);

        debug!(
            "Fail2Ban reflex '{}' action={}, ip={:?}, jail={:?}",
            self.name, action, ip, jail
        );

        match action.as_str() {
            "ban" => {
                let ip = ip.context("IP address is required for ban action")?;
                self.ban_ip(&ip, jail.as_deref()).await?;
            }
            "unban" => {
                let ip = ip.context("IP address is required for unban action")?;
                self.unban_ip(&ip, jail.as_deref()).await?;
            }
            "status" => {
                let status = self.get_status(jail.as_deref()).await?;
                info!(
                    "Fail2Ban reflex '{}' status: {}",
                    self.name, status
                );
            }
            "get_info" | "info" => {
                let info = self.get_info(jail.as_deref()).await?;
                debug!(
                    "Fail2Ban reflex '{}' info: {}",
                    self.name, info
                );
            }
            "list_jails" | "jails" => {
                let jails = self.list_jails().await?;
                info!(
                    "Fail2Ban reflex '{}' found {} jails: {:?}",
                    self.name,
                    jails.len(),
                    jails
                );
            }
            _ => {
                error!(
                    "Fail2Ban reflex '{}' unknown action: {}",
                    self.name, action
                );
                return Err(anyhow::anyhow!("Unknown Fail2Ban action: {}", action));
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_config() -> Fail2BanReflexConfig {
        Fail2BanReflexConfig {
            socket: "/var/run/fail2ban/fail2ban.sock".to_string(),
            action: "status".to_string(),
            ip: None,
            ip_template: None,
            jail: None,
            topic_action_mapping: HashMap::new(),
            topic_ip_mapping: HashMap::new(),
            topic_jail_mapping: HashMap::new(),
            ban_time: 3600,
            timeout: 30,
        }
    }

    fn create_test_ink() -> Ink {
        Ink::new("/test/topic", "test", "test_source", "test body")
    }

    #[test]
    fn test_template_rendering() {
        let config = create_test_config();
        let reflex = Fail2BanReflex::new("test".to_string(), config);
        let ink = create_test_ink();
        assert_eq!(
            reflex.render_template("{{topic}}-{{body}}", &ink),
            "/test/topic-test body"
        );
    }

    #[test]
    fn test_config_deserialization() {
        let yaml = r#"
            action: "ban"
            ip: "192.168.1.100"
            jail: "sshd"
        "#;
        let config: Fail2BanReflexConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.action, "ban");
        assert_eq!(config.ip, Some("192.168.1.100".to_string()));
    }

    #[test]
    fn test_reflex_type() {
        let config = create_test_config();
        let reflex = Fail2BanReflex::new("test".to_string(), config);
        assert_eq!(reflex.r#type(), "fail2ban");
    }

    #[test]
    fn test_resolve_action_from_mapping() {
        let mut config = create_test_config();
        let mut mapping = HashMap::new();
        mapping.insert("/fail2ban/ban".to_string(), "ban".to_string());
        config.topic_action_mapping = mapping;

        let reflex = Fail2BanReflex::new("test".to_string(), config);
        let ink = Ink::new("/fail2ban/ban", "test", "test_source", "body");
        assert_eq!(reflex.resolve_action(&ink), "ban");
    }
}
