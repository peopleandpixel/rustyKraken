#![allow(dead_code)]

use anyhow::{Context, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tracing::{debug, info};

use super::Reflex;
use crate::model::Ink;

/// Configuration for Printer Reflex
/// Prints documents via CUPS
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrinterReflexConfig {
    /// Default printer name
    #[serde(default = "default_printer")]
    pub printer: String,

    /// Template for printer name
    #[serde(default)]
    pub printer_template: Option<String>,

    /// Template for text to print
    #[serde(default = "default_text_template")]
    pub text_template: String,

    /// Output format: "text", "pdf", "html"
    #[serde(default = "default_format")]
    pub format: String,

    /// Optional: mapping from Ink topics to printers
    #[serde(default)]
    pub topic_printer_mapping: HashMap<String, String>,

    /// Optional: mapping from Ink topics to text templates
    #[serde(default)]
    pub topic_text_mapping: HashMap<String, String>,

    /// Number of copies
    #[serde(default = "default_copies")]
    pub copies: u32,

    /// CUPS server URL
    #[serde(default = "default_cups_url")]
    pub cups_url: String,

    /// CUPS username (optional)
    #[serde(default)]
    pub cups_user: Option<String>,

    /// CUPS password (optional)
    #[serde(default)]
    pub cups_password: Option<String>,

    /// Timeout in seconds
    #[serde(default = "default_timeout")]
    pub timeout: u64,
}

fn default_printer() -> String {
    "default".to_string()
}
fn default_text_template() -> String {
    "{{body}}".to_string()
}
fn default_format() -> String {
    "text".to_string()
}
fn default_copies() -> u32 {
    1
}
fn default_cups_url() -> String {
    "http://localhost:631".to_string()
}
fn default_timeout() -> u64 {
    30
}

pub struct PrinterReflex {
    name: String,
    config: PrinterReflexConfig,
    client: reqwest::Client,
}

impl PrinterReflex {
    pub fn new(name: String, config: PrinterReflexConfig) -> Self {
        Self {
            name,
            config,
            client: reqwest::Client::new(),
        }
    }

    /// Resolve printer name for a given Ink
    fn resolve_printer(&self, ink: &Ink) -> String {
        if let Some(printer) = self
            .config
            .topic_printer_mapping
            .get(&ink.topic)
            .filter(|p| !p.is_empty())
        {
            return printer.clone();
        }
        if let Some(ref template) = self.config.printer_template {
            return self.render_template(template, ink);
        }
        self.config.printer.clone()
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

    /// Print via lp command
    async fn print_via_lp(&self, printer: &str, text: &str) -> Result<()> {
        info!(
            "Printer reflex '{}' printing to {} via lp",
            self.name, printer
        );

        let temp_file = format!("/tmp/kraken_print_{}.txt", uuid::Uuid::new_v4());
        std::fs::write(&temp_file, text).context("Failed to write temp file for printing")?;

        let mut cmd = tokio::process::Command::new("lp");
        cmd.arg("-d").arg(printer).arg(&temp_file);

        if self.config.copies > 1 {
            cmd.arg("-n").arg(self.config.copies.to_string());
        }

        let output = cmd.output().await.context("Failed to execute lp")?;

        let _ = std::fs::remove_file(&temp_file);

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(anyhow::anyhow!("lp failed: {}", stderr));
        }

        Ok(())
    }

    /// Print via CUPS API
    async fn print_via_cups_api(&self, printer: &str, text: &str) -> Result<()> {
        info!(
            "Printer reflex '{}' printing to {} via CUPS API",
            self.name, printer
        );

        let _url = format!(
            "{}/printers/{}/jobs",
            self.config.cups_url.trim_end_matches('/'),
            printer
        );

        let temp_file = format!("/tmp/kraken_print_{}.txt", uuid::Uuid::new_v4());
        std::fs::write(&temp_file, text).context("Failed to write temp file for printing")?;

        let output = tokio::process::Command::new("lp")
            .arg("-d")
            .arg(printer)
            .arg(&temp_file)
            .output()
            .await
            .context("Failed to execute lp")?;

        let _ = std::fs::remove_file(&temp_file);

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(anyhow::anyhow!("CUPS print failed: {}", stderr));
        }

        Ok(())
    }

    /// List available printers
    async fn list_printers(&self) -> Result<Vec<String>> {
        let output = tokio::process::Command::new("lpstat")
            .arg("-p")
            .arg("-a")
            .output()
            .await
            .context("Failed to execute lpstat")?;

        let stdout = String::from_utf8_lossy(&output.stdout);
        let mut printers = Vec::new();
        for line in stdout.lines() {
            if let Some(printer) = line.strip_prefix("printer ") {
                let name = printer.split_whitespace().next().unwrap_or("");
                if !name.is_empty() {
                    printers.push(name.to_string());
                }
            }
        }
        Ok(printers)
    }
}

#[async_trait]
impl Reflex for PrinterReflex {
    fn name(&self) -> &str {
        &self.name
    }

    fn r#type(&self) -> &str {
        "printer"
    }

    async fn handle(&self, ink: &Ink) -> Result<()> {
        info!(
            "Printer reflex '{}' handling ink on topic: {}",
            self.name, ink.topic
        );

        let printer = self.resolve_printer(ink);
        let text_template = self.resolve_text_template(ink);
        let text = self.render_template(&text_template, ink);

        debug!(
            "Printer reflex '{}' printer={}, text={}",
            self.name, printer, text
        );

        // Print
        self.print_via_lp(&printer, &text).await?;

        info!("Printer reflex '{}' printed to {}", self.name, printer);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_test_config() -> PrinterReflexConfig {
        PrinterReflexConfig {
            printer: "default".to_string(),
            printer_template: None,
            text_template: "{{body}}".to_string(),
            format: "text".to_string(),
            topic_printer_mapping: HashMap::new(),
            topic_text_mapping: HashMap::new(),
            copies: 1,
            cups_url: "http://localhost:631".to_string(),
            cups_user: None,
            cups_password: None,
            timeout: 30,
        }
    }

    fn create_test_ink() -> Ink {
        Ink::new("/test/topic", "test", "test_source", "Hello world")
    }

    #[test]
    fn test_template_rendering() {
        let config = create_test_config();
        let reflex = PrinterReflex::new("test".to_string(), config);
        let ink = create_test_ink();
        assert_eq!(reflex.render_template("{{body}}", &ink), "Hello world");
    }

    #[test]
    fn test_config_deserialization() {
        let yaml = r#"
            printer: "HP_LaserJet"
            copies: 2
        "#;
        let config: PrinterReflexConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.printer, "HP_LaserJet");
        assert_eq!(config.copies, 2);
    }

    #[test]
    fn test_reflex_type() {
        let config = create_test_config();
        let reflex = PrinterReflex::new("test".to_string(), config);
        assert_eq!(reflex.r#type(), "printer");
    }

    #[test]
    fn test_resolve_printer_from_mapping() {
        let mut config = create_test_config();
        let mut mapping = HashMap::new();
        mapping.insert("/print/office".to_string(), "Office_Printer".to_string());
        config.topic_printer_mapping = mapping;

        let reflex = PrinterReflex::new("test".to_string(), config);
        let ink = Ink::new("/print/office", "test", "test_source", "body");
        assert_eq!(reflex.resolve_printer(&ink), "Office_Printer");
    }
}
