use anyhow::Result;
use async_trait::async_trait;

use crate::model::Ink;

/// Ein Reflex ist eine Aktion, die auf eingehende Ink-Nachrichten reagiert.
#[allow(dead_code)]
#[async_trait]
pub trait Reflex: Send + Sync {
    /// Name des Reflexes
    fn name(&self) -> &str;

    /// Typ des Reflexes (z.B. "file", "command", "whatsapp")
    fn r#type(&self) -> &str;

    /// Führt die Aktion für eine eingehende Nachricht aus
    async fn handle(&self, ink: &Ink) -> Result<()>;
}

pub mod command;
pub mod file;
pub mod whatsapp;

pub use command::{CommandReflex, CommandReflexConfig};
pub use file::{FileReflex, FileReflexConfig};
pub use whatsapp::{WhatsAppReflex, WhatsAppReflexConfig};
