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

pub mod aggregator;
pub mod ansible;
pub mod aws_sns;
pub mod azure_servicebus;
pub mod bark;
pub mod command;
pub mod custom_webhook;
pub mod database;
pub mod discord;
pub mod docker;
pub mod elasticsearch;
pub mod email;
pub mod esphome;
pub mod fail2ban;
pub mod fhem;
pub mod file;
pub mod ftp_sftp_upload;
pub mod google_chat;
pub mod gotify;
pub mod grafana;
pub mod home_assistant;
pub mod http;
pub mod influxdb;
pub mod irc;
pub mod kafka;
pub mod kubernetes;
pub mod led_display;
pub mod make;
pub mod matrix;
pub mod mqtt_publish;
pub mod n8n;
pub mod nats;
pub mod ntfy;
pub mod openhab;
pub mod powerdns;
pub mod printer;
pub mod printer_3d;
pub mod prometheus;
pub mod puppet;
pub mod pushbullet;
pub mod pushover;
pub mod rabbitmq;
pub mod s3_minio;
pub mod saltstack;
pub mod script;
pub mod shelly;
pub mod shodan;
pub mod signal;
pub mod sound_alert;
pub mod syslog;
pub mod tasmota;
pub mod teams;
pub mod telegram;
pub mod temporal;
pub mod tentacle;
pub mod thehive;
pub mod tts;
pub mod whatsapp;
pub mod zapier;
pub mod zigbee2mqtt;

pub use aggregator::{AggregatorReflex, AggregatorReflexConfig};
pub use ansible::{AnsibleReflex, AnsibleReflexConfig};
pub use aws_sns::{AwsSnsReflex, AwsSnsReflexConfig};
pub use azure_servicebus::{AzureServiceBusReflex, AzureServiceBusReflexConfig};
pub use bark::{BarkReflex, BarkReflexConfig};
pub use command::{CommandReflex, CommandReflexConfig};
pub use custom_webhook::{CustomWebhookReflex, CustomWebhookReflexConfig};
pub use database::{DatabaseReflex, DatabaseReflexConfig};
pub use discord::{DiscordReflex, DiscordReflexConfig};
pub use docker::{DockerReflex, DockerReflexConfig};
pub use elasticsearch::{ElasticsearchReflex, ElasticsearchReflexConfig};
pub use email::{EmailReflex, EmailReflexConfig};
pub use esphome::{EsphomeReflex, EsphomeReflexConfig};
pub use fail2ban::{Fail2BanReflex, Fail2BanReflexConfig};
pub use fhem::{FhemReflex, FhemReflexConfig};
pub use file::{FileReflex, FileReflexConfig};
pub use ftp_sftp_upload::{FtpSftpUploadReflex, FtpSftpUploadReflexConfig};
pub use google_chat::{GoogleChatReflex, GoogleChatReflexConfig};
pub use gotify::{GotifyReflex, GotifyReflexConfig};
pub use grafana::{GrafanaReflex, GrafanaReflexConfig};
pub use home_assistant::{HomeAssistantReflex, HomeAssistantReflexConfig};
pub use http::{HttpReflex, HttpReflexConfig};
pub use influxdb::{InfluxdbReflex, InfluxdbReflexConfig};
pub use irc::{IrcReflex, IrcReflexConfig};
pub use kafka::{KafkaReflex, KafkaReflexConfig};
pub use kubernetes::{KubernetesReflex, KubernetesReflexConfig};
pub use led_display::{LedDisplayReflex, LedDisplayReflexConfig};
pub use make::{MakeReflex, MakeReflexConfig};
pub use matrix::{MatrixReflex, MatrixReflexConfig};
pub use mqtt_publish::{MqttPublishReflex, MqttPublishReflexConfig};
pub use n8n::{N8nReflex, N8nReflexConfig};
pub use nats::{NatsReflex, NatsReflexConfig};
pub use ntfy::{NtfyReflex, NtfyReflexConfig};
pub use openhab::{OpenhabReflex, OpenhabReflexConfig};
pub use powerdns::{PowerDnsReflex, PowerDnsReflexConfig};
pub use printer::{PrinterReflex, PrinterReflexConfig};
pub use printer_3d::{Printer3dReflex, Printer3dReflexConfig};
pub use prometheus::{PrometheusReflex, PrometheusReflexConfig};
pub use puppet::{PuppetReflex, PuppetReflexConfig};
pub use pushbullet::{PushbulletReflex, PushbulletReflexConfig};
pub use pushover::{PushoverReflex, PushoverReflexConfig};
pub use rabbitmq::{RabbitMqReflex, RabbitMqReflexConfig};
pub use s3_minio::{S3MinioReflex, S3MinioReflexConfig};
pub use saltstack::{SaltStackReflex, SaltStackReflexConfig};
pub use script::{ScriptReflex, ScriptReflexConfig};
pub use shelly::{ShellyReflex, ShellyReflexConfig};
pub use shodan::{ShodanReflex, ShodanReflexConfig};
pub use signal::{SignalReflex, SignalReflexConfig};
pub use sound_alert::{SoundAlertReflex, SoundAlertReflexConfig};
pub use syslog::{SyslogReflex, SyslogReflexConfig};
pub use tasmota::{TasmotaReflex, TasmotaReflexConfig};
pub use teams::{TeamsReflex, TeamsReflexConfig};
pub use telegram::{TelegramReflex, TelegramReflexConfig};
pub use temporal::{TemporalReflex, TemporalReflexConfig};
pub use tentacle::{TentacleReflex, TentacleReflexConfig};
pub use thehive::{TheHiveReflex, TheHiveReflexConfig};
pub use tts::{TtsReflex, TtsReflexConfig};
pub use whatsapp::{WhatsAppReflex, WhatsAppReflexConfig};
pub use zapier::{ZapierReflex, ZapierReflexConfig};
pub use zigbee2mqtt::{Zigbee2MqttReflex, Zigbee2MqttReflexConfig};
