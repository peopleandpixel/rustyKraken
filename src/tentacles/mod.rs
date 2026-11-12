use anyhow::Result;
use async_trait::async_trait;

use crate::eventbus::EventBus;
use std::sync::Arc;

pub mod aws_sqs;
pub mod azure_servicebus;
pub mod caldav;
pub mod cron;
pub mod database_poller;
pub mod discord;
pub mod docker_events;
pub mod elasticsearch;
pub mod file_watcher;
pub mod ftp_sftp;
pub mod geofence;
pub mod google_pubsub;
pub mod graphql;
pub mod grpc;
pub mod http;
pub mod imap;
pub mod irc;
pub mod journalctl;
pub mod kafka;
pub mod kubernetes;
pub mod matrix;
pub mod mqtt;
pub mod nats;
pub mod ntfy;
pub mod packet_capture;
pub mod pop3;
pub mod rabbitmq;
pub mod redis_streams;
pub mod rest_api;
pub mod rss;
pub mod serial_port;
pub mod signal;
pub mod slack;
pub mod smtp;
pub mod snmp;
pub mod stdin_tentacle;
pub mod syslog_listener;
pub mod teams;
pub mod telegram;
pub mod webhooks_poller;
pub mod websocket;
pub mod xmpp;

pub use aws_sqs::{AwsSqsTentacle, AwsSqsTentacleConfig};
pub use azure_servicebus::{AsbTentacle, AsbTentacleConfig};
pub use caldav::{CalDavTentacle, CalDavTentacleConfig};
pub use cron::{CronTentacle, CronTentacleConfig};
pub use database_poller::{DbPollerTentacle, DbPollerTentacleConfig};
pub use discord::{DiscordTentacle, DiscordTentacleConfig};
pub use docker_events::{DockerTentacle, DockerTentacleConfig};
pub use elasticsearch::{EsTentacle, EsTentacleConfig};
pub use file_watcher::{FileWatcherTentacle, FileWatcherTentacleConfig};
pub use ftp_sftp::{FtpSftpTentacle, FtpSftpTentacleConfig};
pub use geofence::{GeofenceTentacle, GeofenceTentacleConfig};
pub use google_pubsub::{GooglePubSubTentacle, GooglePubSubTentacleConfig};
pub use graphql::{GraphqlTentacle, GraphqlTentacleConfig};
pub use grpc::{GrpcTentacle, GrpcTentacleConfig};
pub use http::{HttpTentacle, HttpTentacleConfig};
pub use imap::{ImapTentacle, ImapTentacleConfig};
pub use irc::{IrcTentacle, IrcTentacleConfig};
pub use journalctl::{JournalTentacle, JournalTentacleConfig};
pub use kafka::{KafkaTentacle, KafkaTentacleConfig};
pub use kubernetes::{K8sTentacle, K8sTentacleConfig};
pub use matrix::{MatrixTentacle, MatrixTentacleConfig};
pub use mqtt::{MqttTentacle, MqttTentacleConfig};
pub use nats::{NatsTentacle, NatsTentacleConfig};
pub use ntfy::{NtfyTentacle, NtfyTentacleConfig};
pub use packet_capture::{PacketCaptureConfig, PacketCaptureTentacle};
pub use pop3::{Pop3Tentacle, Pop3TentacleConfig};
pub use rabbitmq::{RabbitMqTentacle, RabbitMqTentacleConfig};
pub use redis_streams::{RedisTentacle, RedisTentacleConfig};
pub use rest_api::{RestApiTentacle, RestApiTentacleConfig};
pub use rss::{RssTentacle, RssTentacleConfig};
pub use serial_port::{SerialTentacle, SerialTentacleConfig};
pub use signal::{SignalTentacle, SignalTentacleConfig};
pub use slack::{SlackTentacle, SlackTentacleConfig};
pub use smtp::{SmtpTentacle, SmtpTentacleConfig};
pub use snmp::{SnmpTentacle, SnmpTentacleConfig};
pub use stdin_tentacle::{StdinTentacle, StdinTentacleConfig};
pub use syslog_listener::{SyslogTentacle, SyslogTentacleConfig};
pub use teams::{TeamsTentacle, TeamsTentacleConfig};
pub use telegram::{TelegramTentacle, TelegramTentacleConfig};
pub use webhooks_poller::{WebhooksPollerTentacle, WebhooksPollerTentacleConfig};
pub use websocket::{WebSocketTentacle, WebSocketTentacleConfig};
pub use xmpp::{XmppTentacle, XmppTentacleConfig};

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
