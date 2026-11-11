use anyhow::Result;
use async_trait::async_trait;

use crate::eventbus::EventBus;
use std::sync::Arc;

pub mod aws_sqs;
pub mod cron;
pub mod discord;
pub mod file_watcher;
pub mod google_pubsub;
pub mod http;
pub mod imap;
pub mod mqtt;
pub mod nats;
pub mod ntfy;
pub mod pop3;
pub mod redis_streams;
pub mod rest_api;
pub mod rss;
pub mod serial_port;
pub mod slack;
pub mod smtp;
pub mod snmp;
pub mod stdin_tentacle;
pub mod syslog_listener;
pub mod telegram;
pub mod websocket;

pub use aws_sqs::{AwsSqsTentacle, AwsSqsTentacleConfig};
pub use cron::{CronTentacle, CronTentacleConfig};
pub use discord::{DiscordTentacle, DiscordTentacleConfig};
pub use file_watcher::{FileWatcherTentacle, FileWatcherTentacleConfig};
pub use google_pubsub::{GooglePubSubTentacle, GooglePubSubTentacleConfig};
pub use http::{HttpTentacle, HttpTentacleConfig};
pub use imap::{ImapTentacle, ImapTentacleConfig};
pub use mqtt::{MqttTentacle, MqttTentacleConfig};
pub use nats::{NatsTentacle, NatsTentacleConfig};
pub use ntfy::{NtfyTentacle, NtfyTentacleConfig};
pub use pop3::{Pop3Tentacle, Pop3TentacleConfig};
pub use redis_streams::{RedisTentacle, RedisTentacleConfig};
pub use rest_api::{RestApiTentacle, RestApiTentacleConfig};
pub use rss::{RssTentacle, RssTentacleConfig};
pub use serial_port::{SerialTentacle, SerialTentacleConfig};
pub use slack::{SlackTentacle, SlackTentacleConfig};
pub use smtp::{SmtpTentacle, SmtpTentacleConfig};
pub use snmp::{SnmpTentacle, SnmpTentacleConfig};
pub use stdin_tentacle::{StdinTentacle, StdinTentacleConfig};
pub use syslog_listener::{SyslogTentacle, SyslogTentacleConfig};
pub use telegram::{TelegramTentacle, TelegramTentacleConfig};
pub use websocket::{WebSocketTentacle, WebSocketTentacleConfig};

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
