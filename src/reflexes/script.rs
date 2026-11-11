use anyhow::{Context, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use tokio::process::Command;
use tracing::{debug, error, info};

use super::Reflex;
use crate::model::Ink;

/// Script language type
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum ScriptLanguage {
    Lua,
    JavaScript,
    Python,
}

impl std::fmt::Display for ScriptLanguage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ScriptLanguage::Lua => write!(f, "lua"),
            ScriptLanguage::JavaScript => write!(f, "javascript"),
            ScriptLanguage::Python => write!(f, "python"),
        }
    }
}

/// Configuration for Script Reflex
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScriptReflexConfig {
    /// Script language: lua, javascript, python
    pub language: ScriptLanguage,

    /// Inline script code or script file path
    #[serde(default)]
    pub script: String,

    /// Path to the script file (overrides `script` if set)
    #[serde(default)]
    pub script_path: Option<String>,

    /// Path to the interpreter (defaults depend on language)
    #[serde(default)]
    pub interpreter: Option<String>,

    /// Timeout in seconds for script execution
    #[serde(default = "default_timeout")]
    pub timeout_seconds: u64,

    /// Whether to capture stdout from the script
    #[serde(default = "default_capture_stdout")]
    pub capture_stdout: bool,

    /// Whether to publish the script output as a new Ink message
    #[serde(default)]
    pub publish_output: bool,

    /// Topic for the published output message (template supported)
    #[serde(default = "default_output_topic")]
    pub output_topic: String,

    /// Additional environment variables to pass to the script
    #[serde(default)]
    pub env: HashMap<String, String>,
}

fn default_timeout() -> u64 {
    30
}

fn default_capture_stdout() -> bool {
    true
}

fn default_output_topic() -> String {
    "/script/output".to_string()
}

/// Script Reflex: executes scripts in Lua, JavaScript (Node.js), or Python via subprocess
pub struct ScriptReflex {
    name: String,
    config: ScriptReflexConfig,
}

impl ScriptReflex {
    pub fn new(name: String, config: ScriptReflexConfig) -> Self {
        Self { name, config }
    }

    /// Get the interpreter command for the configured language
    fn get_interpreter(&self) -> &str {
        match &self.config.interpreter {
            Some(interp) => interp.as_str(),
            None => match self.config.language {
                ScriptLanguage::Lua => "lua",
                ScriptLanguage::JavaScript => "node",
                ScriptLanguage::Python => "python3",
            },
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

    /// Build environment variables map from Ink data
    fn build_env(&self, ink: &Ink) -> HashMap<String, String> {
        let mut env = HashMap::new();
        env.insert("INK_TOPIC".to_string(), ink.topic.clone());
        env.insert("INK_BODY".to_string(), ink.body.clone());
        env.insert("INK_ID".to_string(), ink.id.to_string());
        env.insert("INK_TIMESTAMP".to_string(), ink.timestamp.to_rfc3339());

        // Set metadata as JSON
        if let Some(metadata) = &ink.metadata {
            let metadata_json =
                serde_json::to_string(metadata).unwrap_or_else(|_| "{}".to_string());
            env.insert("INK_METADATA".to_string(), metadata_json);
        }

        // Set full Ink JSON
        let ink_json = serde_json::to_string(ink).unwrap_or_else(|_| "{}".to_string());
        env.insert("INK_JSON".to_string(), ink_json);

        // Add custom env vars with template rendering
        for (key, value) in &self.config.env {
            let rendered = self.render_template(value, ink);
            env.insert(key.clone(), rendered);
        }

        env
    }

    /// Execute a script via subprocess
    async fn execute_script(&self, ink: &Ink) -> Result<String> {
        let interpreter = self.get_interpreter();
        let script_source = self
            .config
            .script_path
            .clone()
            .unwrap_or_else(|| self.config.script.clone());

        info!(
            "Script reflex '{}' executing {} script: {}",
            self.name, self.config.language, script_source
        );

        let env = self.build_env(ink);

        let mut cmd = Command::new(interpreter);

        // For Lua and JavaScript, check if script_source is a file path or inline code
        match self.config.language {
            ScriptLanguage::Lua | ScriptLanguage::JavaScript => {
                // If script_source looks like a file path (contains / or .), use it as a file
                // Otherwise, treat it as inline code
                if script_source.contains('/') || script_source.contains('.') {
                    cmd.arg(&script_source);
                } else {
                    // Pass inline code via -e (Lua) or -e (Node)
                    cmd.arg("-e");
                    cmd.arg(&script_source);
                }
            }
            ScriptLanguage::Python => {
                cmd.arg(&script_source);
            }
        }

        // Set environment variables
        for (key, value) in env {
            cmd.env(&key, value);
        }

        let output = cmd
            .output()
            .await
            .with_context(|| format!("Failed to execute script with {}", interpreter))?;

        if output.status.success() {
            if self.config.capture_stdout {
                let stdout = String::from_utf8_lossy(&output.stdout).trim().to_string();
                Ok(stdout)
            } else {
                Ok(String::new())
            }
        } else {
            let stderr = String::from_utf8_lossy(&output.stderr);
            error!(
                "Script reflex '{}' failed (exit {}): {}",
                self.name, output.status, stderr
            );
            anyhow::bail!("Script execution failed: {}", stderr)
        }
    }
}

#[async_trait]
impl Reflex for ScriptReflex {
    fn name(&self) -> &str {
        &self.name
    }

    fn r#type(&self) -> &str {
        "script"
    }

    async fn handle(&self, ink: &Ink) -> Result<()> {
        info!(
            "Script reflex '{}' executing {} script",
            self.name, self.config.language
        );

        let output = self.execute_script(ink).await?;

        if !output.is_empty() {
            debug!("Script reflex '{}' output: {}", self.name, output);
        }

        // If publish_output is enabled, log the output topic
        if self.config.publish_output {
            let output_topic = self.render_template(&self.config.output_topic, ink);
            info!(
                "Script reflex '{}' would publish output to topic: {}",
                self.name, output_topic
            );
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{InkStatus, SourceInfo};
    use chrono::Utc;
    use uuid::Uuid;

    fn create_test_ink() -> Ink {
        Ink {
            id: Uuid::new_v4(),
            topic: "/test/script".to_string(),
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

    #[test]
    fn test_template_rendering() {
        let config = ScriptReflexConfig {
            language: ScriptLanguage::Lua,
            script: "return INK_TOPIC".to_string(),
            script_path: None,
            interpreter: None,
            timeout_seconds: 30,
            capture_stdout: true,
            publish_output: false,
            output_topic: "/script/output".to_string(),
            env: HashMap::new(),
        };
        let reflex = ScriptReflex::new("test".to_string(), config);
        let ink = create_test_ink();

        let rendered = reflex.render_template("/output/{{topic}}/{{id}}", &ink);
        assert!(rendered.starts_with("/output/"));
        assert!(rendered.contains(&ink.id.to_string()));
    }

    #[test]
    fn test_template_with_metadata() {
        let ink = Ink {
            id: Uuid::new_v4(),
            topic: "/test".to_string(),
            body: "42".to_string(),
            timestamp: Utc::now(),
            source: SourceInfo {
                r#type: "test".to_string(),
                name: "test".to_string(),
            },
            metadata: Some(serde_json::json!({"sensor": "temperature"})),
            status: InkStatus::New,
        };

        let config = ScriptReflexConfig {
            language: ScriptLanguage::Lua,
            script: "return INK_BODY".to_string(),
            script_path: None,
            interpreter: None,
            timeout_seconds: 30,
            capture_stdout: true,
            publish_output: false,
            output_topic: "/script/output".to_string(),
            env: HashMap::new(),
        };
        let reflex = ScriptReflex::new("test".to_string(), config);

        let rendered = reflex.render_template("{{metadata.sensor}}", &ink);
        assert_eq!(rendered, "temperature");
    }

    #[test]
    fn test_script_language_display() {
        assert_eq!(ScriptLanguage::Lua.to_string(), "lua");
        assert_eq!(ScriptLanguage::JavaScript.to_string(), "javascript");
        assert_eq!(ScriptLanguage::Python.to_string(), "python");
    }

    #[test]
    fn test_default_interpreters() {
        let lua_config = ScriptReflexConfig {
            language: ScriptLanguage::Lua,
            script: "return 1".to_string(),
            script_path: None,
            interpreter: None,
            timeout_seconds: 30,
            capture_stdout: true,
            publish_output: false,
            output_topic: "/script/output".to_string(),
            env: HashMap::new(),
        };
        let lua_reflex = ScriptReflex::new("test".to_string(), lua_config);
        assert_eq!(lua_reflex.get_interpreter(), "lua");

        let js_config = ScriptReflexConfig {
            language: ScriptLanguage::JavaScript,
            script: "1".to_string(),
            script_path: None,
            interpreter: None,
            timeout_seconds: 30,
            capture_stdout: true,
            publish_output: false,
            output_topic: "/script/output".to_string(),
            env: HashMap::new(),
        };
        let js_reflex = ScriptReflex::new("test".to_string(), js_config);
        assert_eq!(js_reflex.get_interpreter(), "node");

        let py_config = ScriptReflexConfig {
            language: ScriptLanguage::Python,
            script: "print(1)".to_string(),
            script_path: None,
            interpreter: None,
            timeout_seconds: 30,
            capture_stdout: true,
            publish_output: false,
            output_topic: "/script/output".to_string(),
            env: HashMap::new(),
        };
        let py_reflex = ScriptReflex::new("test".to_string(), py_config);
        assert_eq!(py_reflex.get_interpreter(), "python3");
    }

    #[test]
    fn test_custom_interpreter() {
        let config = ScriptReflexConfig {
            language: ScriptLanguage::Python,
            script: "print(1)".to_string(),
            script_path: None,
            interpreter: Some("/usr/bin/python3.12".to_string()),
            timeout_seconds: 30,
            capture_stdout: true,
            publish_output: false,
            output_topic: "/script/output".to_string(),
            env: HashMap::new(),
        };
        let reflex = ScriptReflex::new("test".to_string(), config);
        assert_eq!(reflex.get_interpreter(), "/usr/bin/python3.12");
    }

    #[test]
    fn test_config_deserialization() {
        let yaml = r#"
            language: lua
            script: "return INK_BODY"
            timeout_seconds: 60
            capture_stdout: true
            publish_output: true
            output_topic: "/script/{{topic}}/output"
        "#;

        let config: ScriptReflexConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.language, ScriptLanguage::Lua);
        assert_eq!(config.script, "return INK_BODY");
        assert_eq!(config.timeout_seconds, 60);
        assert!(config.capture_stdout);
        assert!(config.publish_output);
    }

    #[test]
    fn test_config_with_python() {
        let yaml = r#"
            language: python
            script_path: "/path/to/script.py"
            interpreter: "/usr/bin/python3"
            env:
              API_KEY: "secret"
        "#;

        let config: ScriptReflexConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.language, ScriptLanguage::Python);
        assert_eq!(config.script_path, Some("/path/to/script.py".to_string()));
        assert_eq!(config.interpreter, Some("/usr/bin/python3".to_string()));
        assert_eq!(config.env.get("API_KEY").unwrap(), "secret");
    }

    #[test]
    fn test_config_with_javascript() {
        let yaml = r#"
            language: javascript
            script: "INK_BODY.toUpperCase()"
        "#;

        let config: ScriptReflexConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.language, ScriptLanguage::JavaScript);
        assert_eq!(config.script, "INK_BODY.toUpperCase()");
    }

    #[test]
    fn test_reflex_type() {
        let config = ScriptReflexConfig {
            language: ScriptLanguage::Lua,
            script: "return 1".to_string(),
            script_path: None,
            interpreter: None,
            timeout_seconds: 30,
            capture_stdout: true,
            publish_output: false,
            output_topic: "/script/output".to_string(),
            env: HashMap::new(),
        };
        let reflex = ScriptReflex::new("test".to_string(), config);
        assert_eq!(reflex.r#type(), "script");
        assert_eq!(reflex.name(), "test");
    }

    #[test]
    fn test_template_with_all_placeholders() {
        let ink = create_test_ink();
        let config = ScriptReflexConfig {
            language: ScriptLanguage::Lua,
            script: "return 1".to_string(),
            script_path: None,
            interpreter: None,
            timeout_seconds: 30,
            capture_stdout: true,
            publish_output: false,
            output_topic: "/script/output".to_string(),
            env: HashMap::new(),
        };
        let reflex = ScriptReflex::new("test".to_string(), config);

        let template = "{{topic}}/{{body}}/{{id}}/{{timestamp}}";
        let rendered = reflex.render_template(template, &ink);

        assert!(rendered.contains(&ink.topic));
        assert!(rendered.contains(&ink.body));
        assert!(rendered.contains(&ink.id.to_string()));
    }

    #[test]
    fn test_build_env() {
        let ink = Ink {
            id: Uuid::new_v4(),
            topic: "/test".to_string(),
            body: "data".to_string(),
            timestamp: Utc::now(),
            source: SourceInfo {
                r#type: "test".to_string(),
                name: "test".to_string(),
            },
            metadata: Some(serde_json::json!({"key": "value"})),
            status: InkStatus::New,
        };

        let config = ScriptReflexConfig {
            language: ScriptLanguage::Python,
            script: "print(1)".to_string(),
            script_path: None,
            interpreter: None,
            timeout_seconds: 30,
            capture_stdout: true,
            publish_output: false,
            output_topic: "/script/output".to_string(),
            env: HashMap::from([("CUSTOM".to_string(), "{{topic}}".to_string())]),
        };
        let reflex = ScriptReflex::new("test".to_string(), config);

        let env = reflex.build_env(&ink);
        assert_eq!(env.get("INK_TOPIC").unwrap(), "/test");
        assert_eq!(env.get("INK_BODY").unwrap(), "data");
        assert!(env.contains_key("INK_ID"));
        assert!(env.contains_key("INK_TIMESTAMP"));
        assert!(env.contains_key("INK_METADATA"));
        assert!(env.contains_key("INK_JSON"));
        assert_eq!(env.get("CUSTOM").unwrap(), "/test");
    }
}
