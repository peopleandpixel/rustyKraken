use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// Die zentrale Nachrichtenstruktur von rustyKraken.
/// Jede Nachricht, egal von welcher Quelle, wird in dieses Format gebracht.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Ink {
    pub id: Uuid,
    pub timestamp: DateTime<Utc>,
    pub topic: String,
    pub source: SourceInfo,
    pub body: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub metadata: Option<serde_json::Value>,
    pub status: InkStatus,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SourceInfo {
    pub r#type: String,
    pub name: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum InkStatus {
    New,
    Processed,
    Archived,
}

impl Ink {
    pub fn new(
        topic: impl Into<String>,
        source_type: impl Into<String>,
        source_name: impl Into<String>,
        body: impl Into<String>,
    ) -> Self {
        Self {
            id: Uuid::new_v4(),
            timestamp: Utc::now(),
            topic: topic.into(),
            source: SourceInfo {
                r#type: source_type.into(),
                name: source_name.into(),
            },
            body: body.into(),
            metadata: None,
            status: InkStatus::New,
        }
    }

    /// Setzt Metadaten für diese Nachricht
    pub fn with_metadata(mut self, metadata: serde_json::Value) -> Self {
        self.metadata = Some(metadata);
        self
    }
}
