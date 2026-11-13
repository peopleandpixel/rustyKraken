use anyhow::{Context, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::fs as async_fs;
use tokio::io::AsyncWriteExt;
use tracing::{debug, info};

use super::Reflex;
use crate::model::Ink;

/// Output format for File Write Reflex
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum FileFormat {
    Json,
    Csv,
    Plaintext,
    Xml,
}

impl std::fmt::Display for FileFormat {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FileFormat::Json => write!(f, "json"),
            FileFormat::Csv => write!(f, "csv"),
            FileFormat::Plaintext => write!(f, "plaintext"),
            FileFormat::Xml => write!(f, "xml"),
        }
    }
}

/// File write mode
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "lowercase")]
pub enum FileMode {
    #[default]
    Append,
    Overwrite,
}

impl std::fmt::Display for FileMode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FileMode::Append => write!(f, "append"),
            FileMode::Overwrite => write!(f, "overwrite"),
        }
    }
}

/// Configuration for file rotation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileRotationConfig {
    /// Maximum file size in bytes before rotation (default: 10MB)
    #[serde(default = "default_max_size")]
    pub max_size_bytes: u64,

    /// Maximum number of rotated files to keep (0 = unlimited)
    #[serde(default = "default_max_files")]
    pub max_files: usize,

    /// Suffix pattern for rotated files (e.g., ".1", ".old")
    #[serde(default = "default_rotation_suffix")]
    pub suffix: String,
}

fn default_max_size() -> u64 {
    10 * 1024 * 1024 // 10 MB
}

fn default_max_files() -> usize {
    5
}

fn default_rotation_suffix() -> String {
    ".old".to_string()
}

/// Configuration for CSV output
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CsvConfig {
    /// CSV columns to output (template supported)
    #[serde(default = "default_csv_columns")]
    pub columns: Vec<String>,

    /// Whether to write a header row
    #[serde(default = "default_csv_header")]
    pub header: bool,

    /// CSV delimiter character
    #[serde(default = "default_csv_delimiter")]
    pub delimiter: String,
}

fn default_csv_columns() -> Vec<String> {
    vec![
        "{{timestamp}}".to_string(),
        "{{topic}}".to_string(),
        "{{body}}".to_string(),
    ]
}

fn default_csv_header() -> bool {
    true
}

fn default_csv_delimiter() -> String {
    ",".to_string()
}

/// Configuration for File Write Reflex
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileReflexConfig {
    /// File path to write to (template supported, e.g., "/var/log/kraken/{{topic}}.log")
    pub path: String,

    /// Output format: json, csv, plaintext, xml
    #[serde(default = "default_format")]
    pub format: FileFormat,

    /// Write mode: append or overwrite
    #[serde(default)]
    pub mode: FileMode,

    /// Template for the file content (used in plaintext and csv formats)
    /// Supported placeholders: {{topic}}, {{body}}, {{id}}, {{timestamp}}, {{metadata.<key>}}
    #[serde(default)]
    pub template: Option<String>,

    /// CSV-specific configuration
    #[serde(default)]
    pub csv: Option<CsvConfig>,

    /// Rotation configuration (optional)
    #[serde(default)]
    pub rotation: Option<FileRotationConfig>,

    /// Whether to flush after each write
    #[serde(default = "default_flush")]
    pub flush: bool,

    /// Newline separator
    #[serde(default = "default_newline")]
    pub newline: String,
}

fn default_format() -> FileFormat {
    FileFormat::Json
}

fn default_flush() -> bool {
    true
}

fn default_newline() -> String {
    "\n".to_string()
}

/// File Write Reflex: writes Ink messages to files in various formats with rotation support
pub struct FileReflex {
    name: String,
    config: FileReflexConfig,
    header_written: Arc<std::sync::atomic::AtomicBool>,
}

impl FileReflex {
    pub fn new(name: String, config: FileReflexConfig) -> Self {
        Self {
            name,
            config,
            header_written: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        }
    }

    /// Render a template string with Ink data
    fn render_template(&self, template: &str, ink: &Ink) -> String {
        let mut result = template.to_string();
        result = result.replace("{{topic}}", &ink.topic);
        result = result.replace("{{body}}", &ink.body);
        result = result.replace("{{id}}", &ink.id.to_string());
        result = result.replace("{{timestamp}}", &ink.timestamp.to_rfc3339());
        // Replace metadata placeholders
        if let Some(metadata) = &ink.metadata
            && let Some(obj) = metadata.as_object()
        {
            for (key, value) in obj {
                let placeholder = format!("{{{{metadata.{}}}}}", key);
                let value_str = match value {
                    serde_json::Value::String(s) => s.clone(),
                    serde_json::Value::Number(n) => n.to_string(),
                    serde_json::Value::Bool(b) => b.to_string(),
                    _ => serde_json::to_string(value).unwrap_or_default(),
                };
                result = result.replace(&placeholder, &value_str);
            }
        }
        result
    }

    /// Resolve the file path for a given Ink
    fn resolve_path(&self, ink: &Ink) -> String {
        self.render_template(&self.config.path, ink)
    }

    /// Format Ink as JSON
    fn format_json(&self, ink: &Ink) -> String {
        serde_json::to_string(ink).unwrap_or_else(|_| "{}".to_string())
    }

    /// Format Ink as CSV
    fn format_csv(&self, ink: &Ink) -> String {
        let default_csv = CsvConfig {
            columns: default_csv_columns(),
            header: default_csv_header(),
            delimiter: default_csv_delimiter(),
        };
        let csv_config = self.config.csv.as_ref().unwrap_or(&default_csv);

        let delimiter = csv_config.delimiter.as_str();
        let mut fields: Vec<String> = Vec::new();

        for column in &csv_config.columns {
            let rendered = self.render_template(column, ink);
            // Escape CSV fields: if contains delimiter, newline, or quotes, wrap in quotes
            if rendered.contains(delimiter) || rendered.contains('\n') || rendered.contains('"') {
                let escaped = rendered.replace('"', "\"\"");
                fields.push(format!("\"{}\"", escaped));
            } else {
                fields.push(rendered);
            }
        }

        fields.join(delimiter)
    }

    /// Format Ink as plain text
    fn format_plaintext(&self, ink: &Ink) -> String {
        if let Some(ref template) = self.config.template {
            self.render_template(template, ink)
        } else {
            format!("{}: {}", ink.topic, ink.body)
        }
    }

    /// Format Ink as XML
    fn format_xml(&self, ink: &Ink) -> String {
        let mut xml = String::new();
        xml.push_str("<ink>\n");
        xml.push_str(&format!(
            "  <id>{}</id>\n",
            Self::xml_escape(&ink.id.to_string())
        ));
        xml.push_str(&format!(
            "  <topic>{}</topic>\n",
            Self::xml_escape(&ink.topic)
        ));
        xml.push_str(&format!("  <body>{}</body>\n", Self::xml_escape(&ink.body)));
        xml.push_str(&format!(
            "  <timestamp>{}</timestamp>\n",
            Self::xml_escape(&ink.timestamp.to_rfc3339())
        ));
        if let Some(metadata) = &ink.metadata
            && let Some(obj) = metadata.as_object()
        {
            xml.push_str("  <metadata>\n");
            for (key, value) in obj {
                let value_str = match value {
                    serde_json::Value::String(s) => s.clone(),
                    serde_json::Value::Number(n) => n.to_string(),
                    serde_json::Value::Bool(b) => b.to_string(),
                    _ => serde_json::to_string(value).unwrap_or_default(),
                };
                xml.push_str(&format!(
                    "    <{}>{}</{}>\n",
                    Self::xml_escape(key),
                    Self::xml_escape(&value_str),
                    Self::xml_escape(key)
                ));
            }
            xml.push_str("  </metadata>\n");
        }
        xml.push_str("</ink>");
        xml
    }

    /// Escape special XML characters
    fn xml_escape(s: &str) -> String {
        s.replace('&', "&amp;")
            .replace('<', "&lt;")
            .replace('>', "&gt;")
            .replace('"', "&quot;")
            .replace('\'', "&apos;")
    }

    /// Format Ink based on configured format
    fn format_ink(&self, ink: &Ink) -> String {
        match self.config.format {
            FileFormat::Json => self.format_json(ink),
            FileFormat::Csv => self.format_csv(ink),
            FileFormat::Plaintext => self.format_plaintext(ink),
            FileFormat::Xml => self.format_xml(ink),
        }
    }

    /// Check if file needs rotation and perform rotation
    async fn rotate_if_needed(&self, path: &PathBuf) -> Result<()> {
        if let Some(ref rotation) = self.config.rotation {
            let metadata = match async_fs::metadata(path).await {
                Ok(m) => m,
                Err(_) => return Ok(()), // File doesn't exist, no rotation needed
            };

            if metadata.len() >= rotation.max_size_bytes {
                info!(
                    "File reflex '{}' rotating file {:?} (size: {} bytes, max: {} bytes)",
                    self.name,
                    path,
                    metadata.len(),
                    rotation.max_size_bytes
                );

                let suffix = rotation.suffix.trim_start_matches('.').to_string();

                // Shift existing rotated files
                let mut current_max = 1;
                loop {
                    let next_path_str =
                        format!("{}{}.{}", path.to_string_lossy(), current_max + 1, suffix);
                    let next_path = PathBuf::from(&next_path_str);

                    let curr_path_str =
                        format!("{}{}.{}", path.to_string_lossy(), current_max, suffix);
                    let curr_path = PathBuf::from(&curr_path_str);

                    if rotation.max_files > 0 && current_max >= rotation.max_files {
                        // Delete the oldest rotated file
                        if curr_path.exists() {
                            async_fs::remove_file(&curr_path).await.ok();
                        }
                        break;
                    }

                    if curr_path.exists() {
                        async_fs::rename(&curr_path, &next_path).await.ok();
                        current_max += 1;
                    } else {
                        break;
                    }
                }

                // Rename current file to .1
                let rotated_path_str = format!("{}1.{}", path.to_string_lossy(), suffix);
                let rotated_path = PathBuf::from(&rotated_path_str);
                async_fs::rename(path, &rotated_path)
                    .await
                    .with_context(|| format!("Failed to rotate file: {:?}", path))?;

                debug!(
                    "File reflex '{}' rotated {:?} -> {:?}",
                    self.name, path, rotated_path
                );
            }
        }
        Ok(())
    }

    /// Write content to file
    async fn write_to_file(&self, path: &PathBuf, content: &str) -> Result<()> {
        // Ensure parent directory exists
        if let Some(parent) = path.parent() {
            async_fs::create_dir_all(parent)
                .await
                .with_context(|| format!("Failed to create directory: {:?}", parent))?;
        }

        let newline = self.config.newline.as_str();

        match self.config.mode {
            FileMode::Append => {
                let mut file = async_fs::OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(path)
                    .await
                    .with_context(|| format!("Failed to open file: {:?}", path))?;

                file.write_all(content.as_bytes())
                    .await
                    .with_context(|| format!("Failed to write to file: {:?}", path))?;
                file.write_all(newline.as_bytes())
                    .await
                    .with_context(|| format!("Failed to write newline to file: {:?}", path))?;

                if self.config.flush {
                    file.flush()
                        .await
                        .with_context(|| format!("Failed to flush file: {:?}", path))?;
                }
            }
            FileMode::Overwrite => {
                async_fs::write(path, content.as_bytes())
                    .await
                    .with_context(|| format!("Failed to write file: {:?}", path))?;
            }
        }

        Ok(())
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
        let path_str = self.resolve_path(ink);
        let path = PathBuf::from(&path_str);

        // Check for rotation before writing
        self.rotate_if_needed(&path).await?;

        // Format the content
        let content = self.format_ink(ink);

        // Write CSV header if needed
        if self.config.format == FileFormat::Csv
            && !self
                .header_written
                .load(std::sync::atomic::Ordering::SeqCst)
        {
            if let Some(ref csv_config) = self.config.csv
                && csv_config.header
            {
                let header: String = csv_config.columns.to_vec().join(&csv_config.delimiter);
                self.write_to_file(&path, &header).await?;
            }
            self.header_written
                .store(true, std::sync::atomic::Ordering::SeqCst);
        }

        // Write the content
        self.write_to_file(&path, &content).await?;

        info!(
            "File reflex '{}' wrote {} ink to {:?}",
            self.name, self.config.format, path
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::appcontext::AppContext;
    use crate::model::{InkStatus, SourceInfo};
    use chrono::Utc;
    use uuid::Uuid;

    fn create_test_ink() -> Ink {
        Ink {
            id: Uuid::new_v4(),
            topic: "/test/file".to_string(),
            body: "Hello, World!".to_string(),
            timestamp: Utc::now(),
            source: SourceInfo {
                r#type: "test".to_string(),
                name: "test".to_string(),
            },
            metadata: None,
            status: InkStatus::New,
        }
    }

    fn create_test_ink_with_metadata() -> Ink {
        Ink {
            id: Uuid::new_v4(),
            topic: "/sensor/temp".to_string(),
            body: "42.5".to_string(),
            timestamp: Utc::now(),
            source: SourceInfo {
                r#type: "sensor".to_string(),
                name: "temp01".to_string(),
            },
            metadata: Some(serde_json::json!({
                "unit": "celsius",
                "location": "room1"
            })),
            status: InkStatus::New,
        }
    }

    #[test]
    fn test_template_rendering() {
        let config = FileReflexConfig {
            path: "/var/log/kraken.log".to_string(),
            format: FileFormat::Plaintext,
            mode: FileMode::Append,
            template: Some("{{topic}}: {{body}}".to_string()),
            csv: None,
            rotation: None,
            flush: true,
            newline: "\n".to_string(),
        };
        let reflex = FileReflex::new("test".to_string(), config);
        let ink = create_test_ink();

        let rendered = reflex.render_template("/output/{{topic}}/{{id}}", &ink);
        assert!(rendered.starts_with("/output/"));
        assert!(rendered.contains(&ink.id.to_string()));
    }

    #[test]
    fn test_template_with_metadata() {
        let config = FileReflexConfig {
            path: "/var/log/kraken.log".to_string(),
            format: FileFormat::Plaintext,
            mode: FileMode::Append,
            template: None,
            csv: None,
            rotation: None,
            flush: true,
            newline: "\n".to_string(),
        };
        let reflex = FileReflex::new("test".to_string(), config);
        let ink = create_test_ink_with_metadata();

        assert_eq!(reflex.render_template("{{metadata.unit}}", &ink), "celsius");
        assert_eq!(
            reflex.render_template("{{metadata.location}}", &ink),
            "room1"
        );
    }

    #[test]
    fn test_json_format() {
        let config = FileReflexConfig {
            path: "/var/log/kraken.json".to_string(),
            format: FileFormat::Json,
            mode: FileMode::Append,
            template: None,
            csv: None,
            rotation: None,
            flush: true,
            newline: "\n".to_string(),
        };
        let reflex = FileReflex::new("test".to_string(), config);
        let ink = create_test_ink();

        let json = reflex.format_json(&ink);
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed["topic"], "/test/file");
        assert_eq!(parsed["body"], "Hello, World!");
    }

    #[test]
    fn test_csv_format() {
        let config = FileReflexConfig {
            path: "/var/log/kraken.csv".to_string(),
            format: FileFormat::Csv,
            mode: FileMode::Append,
            template: None,
            csv: Some(CsvConfig {
                columns: vec![
                    "{{timestamp}}".to_string(),
                    "{{topic}}".to_string(),
                    "{{body}}".to_string(),
                ],
                header: true,
                delimiter: ",".to_string(),
            }),
            rotation: None,
            flush: true,
            newline: "\n".to_string(),
        };
        let reflex = FileReflex::new("test".to_string(), config);
        let ink = create_test_ink();

        let csv_line = reflex.format_csv(&ink);
        // "Hello, World!" contains a comma, so it will be quoted
        assert!(csv_line.contains("/test/file"));
        assert!(csv_line.contains("\"Hello, World!\""));
    }

    #[test]
    fn test_csv_escape() {
        let config = FileReflexConfig {
            path: "/var/log/kraken.csv".to_string(),
            format: FileFormat::Csv,
            mode: FileMode::Append,
            template: None,
            csv: Some(CsvConfig {
                columns: vec!["{{body}}".to_string()],
                header: false,
                delimiter: ",".to_string(),
            }),
            rotation: None,
            flush: true,
            newline: "\n".to_string(),
        };
        let reflex = FileReflex::new("test".to_string(), config);

        let ink = Ink {
            id: Uuid::new_v4(),
            topic: "/test".to_string(),
            body: "Hello, \"World\"!".to_string(),
            timestamp: Utc::now(),
            source: SourceInfo {
                r#type: "test".to_string(),
                name: "test".to_string(),
            },
            metadata: None,
            status: InkStatus::New,
        };

        let csv_line = reflex.format_csv(&ink);
        // Should be quoted and escaped
        assert!(csv_line.starts_with('"'));
        assert!(csv_line.contains("\"\""));
    }

    #[test]
    fn test_plaintext_format() {
        let config = FileReflexConfig {
            path: "/var/log/kraken.log".to_string(),
            format: FileFormat::Plaintext,
            mode: FileMode::Append,
            template: Some("[{{timestamp}}] {{topic}}: {{body}}".to_string()),
            csv: None,
            rotation: None,
            flush: true,
            newline: "\n".to_string(),
        };
        let reflex = FileReflex::new("test".to_string(), config);
        let ink = create_test_ink();

        let text = reflex.format_plaintext(&ink);
        assert!(text.contains("/test/file"));
        assert!(text.contains("Hello, World!"));
        assert!(text.starts_with('['));
    }

    #[test]
    fn test_plaintext_default_template() {
        let config = FileReflexConfig {
            path: "/var/log/kraken.log".to_string(),
            format: FileFormat::Plaintext,
            mode: FileMode::Append,
            template: None,
            csv: None,
            rotation: None,
            flush: true,
            newline: "\n".to_string(),
        };
        let reflex = FileReflex::new("test".to_string(), config);
        let ink = create_test_ink();

        let text = reflex.format_plaintext(&ink);
        assert_eq!(text, "/test/file: Hello, World!");
    }

    #[test]
    fn test_xml_format() {
        let config = FileReflexConfig {
            path: "/var/log/kraken.xml".to_string(),
            format: FileFormat::Xml,
            mode: FileMode::Append,
            template: None,
            csv: None,
            rotation: None,
            flush: true,
            newline: "\n".to_string(),
        };
        let reflex = FileReflex::new("test".to_string(), config);
        let ink = create_test_ink_with_metadata();

        let xml = reflex.format_xml(&ink);
        assert!(xml.contains("<ink>"));
        assert!(xml.contains("</ink>"));
        assert!(xml.contains("<topic>/sensor/temp</topic>"));
        assert!(xml.contains("<body>42.5</body>"));
        assert!(xml.contains("<metadata>"));
        assert!(xml.contains("<unit>celsius</unit>"));
        assert!(xml.contains("<location>room1</location>"));
    }

    #[test]
    fn test_xml_escape() {
        assert_eq!(
            FileReflex::xml_escape("<script>alert('xss')</script>"),
            "&lt;script&gt;alert(&apos;xss&apos;)&lt;/script&gt;"
        );
        assert_eq!(FileReflex::xml_escape("a & b"), "a &amp; b");
        assert_eq!(
            FileReflex::xml_escape("say \"hello\""),
            "say &quot;hello&quot;"
        );
    }

    #[test]
    fn test_path_resolution_with_template() {
        let config = FileReflexConfig {
            path: "/var/log/kraken/{{topic}}.log".to_string(),
            format: FileFormat::Json,
            mode: FileMode::Append,
            template: None,
            csv: None,
            rotation: None,
            flush: true,
            newline: "\n".to_string(),
        };
        let reflex = FileReflex::new("test".to_string(), config);
        let ink = create_test_ink();

        let path = reflex.resolve_path(&ink);
        assert_eq!(path, "/var/log/kraken//test/file.log");
    }

    #[test]
    fn test_file_format_display() {
        assert_eq!(FileFormat::Json.to_string(), "json");
        assert_eq!(FileFormat::Csv.to_string(), "csv");
        assert_eq!(FileFormat::Plaintext.to_string(), "plaintext");
        assert_eq!(FileFormat::Xml.to_string(), "xml");
    }

    #[test]
    fn test_file_mode_display() {
        assert_eq!(FileMode::Append.to_string(), "append");
        assert_eq!(FileMode::Overwrite.to_string(), "overwrite");
    }

    #[test]
    fn test_config_deserialization_json() {
        let yaml = r#"
            path: "/var/log/kraken.json"
            format: json
            mode: append
            flush: true
        "#;
        let config: FileReflexConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.format, FileFormat::Json);
        assert_eq!(config.mode, FileMode::Append);
        assert!(config.flush);
    }

    #[test]
    fn test_config_deserialization_csv() {
        let yaml = r#"
            path: "/var/log/kraken.csv"
            format: csv
            mode: append
            csv:
              columns: ["{{timestamp}}", "{{topic}}", "{{body}}"]
              header: true
              delimiter: ";"
        "#;
        let config: FileReflexConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.format, FileFormat::Csv);
        assert!(config.csv.is_some());
        let csv = config.csv.unwrap();
        assert_eq!(csv.delimiter, ";");
        assert!(csv.header);
    }

    #[test]
    fn test_config_deserialization_plaintext() {
        let yaml = r#"
            path: "/var/log/kraken.log"
            format: plaintext
            template: "[{{timestamp}}] {{topic}}: {{body}}"
            mode: append
        "#;
        let config: FileReflexConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.format, FileFormat::Plaintext);
        assert_eq!(
            config.template.unwrap(),
            "[{{timestamp}}] {{topic}}: {{body}}"
        );
    }

    #[test]
    fn test_config_deserialization_xml() {
        let yaml = r#"
            path: "/var/log/kraken.xml"
            format: xml
            mode: overwrite
        "#;
        let config: FileReflexConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.format, FileFormat::Xml);
        assert_eq!(config.mode, FileMode::Overwrite);
    }

    #[test]
    fn test_config_deserialization_with_rotation() {
        let yaml = r#"
            path: "/var/log/kraken.log"
            format: plaintext
            template: "{{topic}}: {{body}}"
            rotation:
              max_size_bytes: 1048576
              max_files: 3
              suffix: ".bak"
        "#;
        let config: FileReflexConfig = serde_yaml::from_str(yaml).unwrap();
        assert!(config.rotation.is_some());
        let rotation = config.rotation.unwrap();
        assert_eq!(rotation.max_size_bytes, 1048576);
        assert_eq!(rotation.max_files, 3);
        assert_eq!(rotation.suffix, ".bak");
    }

    #[test]
    fn test_config_defaults() {
        let yaml = r#"
            path: "/var/log/kraken.log"
        "#;
        let config: FileReflexConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.format, FileFormat::Json);
        assert_eq!(config.mode, FileMode::Append);
        assert!(config.flush);
        assert_eq!(config.newline, "\n");
        assert!(config.rotation.is_none());
    }

    #[test]
    fn test_reflex_type() {
        let config = FileReflexConfig {
            path: "/var/log/kraken.log".to_string(),
            format: FileFormat::Json,
            mode: FileMode::Append,
            template: None,
            csv: None,
            rotation: None,
            flush: true,
            newline: "\n".to_string(),
        };
        let reflex = FileReflex::new("test".to_string(), config);
        assert_eq!(reflex.r#type(), "file");
        assert_eq!(reflex.name(), "test");
    }

    #[test]
    fn test_csv_custom_delimiter() {
        let config = FileReflexConfig {
            path: "/var/log/kraken.tsv".to_string(),
            format: FileFormat::Csv,
            mode: FileMode::Append,
            template: None,
            csv: Some(CsvConfig {
                columns: vec!["{{topic}}".to_string(), "{{body}}".to_string()],
                header: false,
                delimiter: "\t".to_string(),
            }),
            rotation: None,
            flush: true,
            newline: "\n".to_string(),
        };
        let reflex = FileReflex::new("test".to_string(), config);
        let ink = create_test_ink();

        let csv_line = reflex.format_csv(&ink);
        assert!(csv_line.contains('\t'));
        let fields: Vec<&str> = csv_line.split('\t').collect();
        assert_eq!(fields.len(), 2);
        assert_eq!(fields[0], "/test/file");
    }

    #[test]
    fn test_format_ink_dispatches_correctly() {
        let config = FileReflexConfig {
            path: "/var/log/kraken.log".to_string(),
            format: FileFormat::Json,
            mode: FileMode::Append,
            template: None,
            csv: None,
            rotation: None,
            flush: true,
            newline: "\n".to_string(),
        };
        let reflex = FileReflex::new("test".to_string(), config);
        let ink = create_test_ink();

        let json = reflex.format_ink(&ink);
        let parsed: serde_json::Value = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed["topic"], "/test/file");
    }
}
