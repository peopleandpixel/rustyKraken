use serde::{Deserialize, Serialize};

/// Konfiguration für einen Tentacle (Nachrichtenquelle)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TentacleConfig {
    pub name: String,
    pub r#type: String,
    pub config: serde_yaml::Value,
}

/// Konfiguration für einen Reflex (Aktion)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReflexConfig {
    pub name: String,
    pub trigger: TriggerConfig,
    pub action: ActionConfig,
}

/// Definiert, wann ein Reflex ausgelöst wird
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TriggerConfig {
    /// Themen, auf die der Reflex reagieren soll (unterstützt Wildcards wie `/kraken/**`)
    pub topics: Vec<String>,
}

/// Die Aktion, die bei einem Trigger ausgeführt wird
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActionConfig {
    pub r#type: String,
    pub config: serde_yaml::Value,
}
