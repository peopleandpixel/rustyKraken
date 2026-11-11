use anyhow::Result;
use async_trait::async_trait;

use crate::eventbus::EventBus;
use std::sync::Arc;

pub mod http;
pub mod imap;
pub mod mqtt;
pub mod ntfy;
pub mod pop3;
pub mod smtp;

pub use http::{HttpTentacle, HttpTentacleConfig};
pub use imap::{ImapTentacle, ImapTentacleConfig};
pub use mqtt::{MqttTentacle, MqttTentacleConfig};
pub use ntfy::{NtfyTentacle, NtfyTentacleConfig};
pub use pop3::{Pop3Tentacle, Pop3TentacleConfig};
pub use smtp::{SmtpTentacle, SmtpTentacleConfig};

/// Ein Tentacle ist eine Nachrichtenquelle, die Ink in das Event Bus einspeist.
#[allow(dead_code)]
#[async_trait]
pub trait Tentacle: Send + Sync {
    /// Name des Tentacles
    fn name(&self) -> &str;

    /// Typ des Tentacles (z.B. "mqtt", "http", "imap")
    fn r#type(&self) -> &str;

    /// Startet den Tentacle und beginnt, Nachrichten zu empfangen
    async fn start(&self, event_bus: Arc<EventBus>) -> Result<()>;
}
