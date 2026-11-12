mod config;
mod eventbus;
mod mantle;
mod model;
mod reflexes;
mod tentacles;

use anyhow::Result;
use std::path::PathBuf;
use std::sync::Arc;
use tracing::{error, info};

use config::{ReflexConfig, TentacleConfig, load_reflexes, load_tentacles};
use eventbus::EventBus;
use mantle::Mantle;
use reflexes::{
    AggregatorReflex, AggregatorReflexConfig, CommandReflex, CommandReflexConfig, DatabaseReflex,
    DatabaseReflexConfig, DiscordReflex, DiscordReflexConfig, EmailReflex, EmailReflexConfig,
    FileReflex, FileReflexConfig, HomeAssistantReflex, HomeAssistantReflexConfig, HttpReflex,
    HttpReflexConfig, MatrixReflex, MatrixReflexConfig, NtfyReflex, NtfyReflexConfig, Reflex,
    ScriptReflex, ScriptReflexConfig, SyslogReflex, SyslogReflexConfig, TelegramReflex,
    TelegramReflexConfig, TentacleReflex, TentacleReflexConfig, WhatsAppReflex,
    WhatsAppReflexConfig,
};
use tentacles::{
    AsbTentacle, AsbTentacleConfig, AwsSqsTentacle, AwsSqsTentacleConfig, CalDavTentacle,
    CalDavTentacleConfig, CronTentacle, CronTentacleConfig, DbPollerTentacle,
    DbPollerTentacleConfig, DiscordTentacle, DiscordTentacleConfig, DockerTentacle,
    DockerTentacleConfig, EsTentacle, EsTentacleConfig, FileWatcherTentacle,
    FileWatcherTentacleConfig, FtpSftpTentacle, FtpSftpTentacleConfig, GeofenceTentacle,
    GeofenceTentacleConfig, GooglePubSubTentacle, GooglePubSubTentacleConfig, GraphqlTentacle,
    GraphqlTentacleConfig, GrpcTentacle, GrpcTentacleConfig, HttpTentacle, HttpTentacleConfig,
    ImapTentacle, ImapTentacleConfig, IrcTentacle, IrcTentacleConfig, JournalTentacle,
    JournalTentacleConfig, K8sTentacle, K8sTentacleConfig, KafkaTentacle, KafkaTentacleConfig,
    MatrixTentacle, MatrixTentacleConfig, MqttTentacle, MqttTentacleConfig, NatsTentacle,
    NatsTentacleConfig, NtfyTentacle, NtfyTentacleConfig, PacketCaptureConfig,
    PacketCaptureTentacle, Pop3Tentacle, Pop3TentacleConfig, RabbitMqTentacle,
    RabbitMqTentacleConfig, RedisTentacle, RedisTentacleConfig, RestApiTentacle,
    RestApiTentacleConfig, RssTentacle, RssTentacleConfig, SerialTentacle, SerialTentacleConfig,
    SignalTentacle, SignalTentacleConfig, SlackTentacle, SlackTentacleConfig, SmtpTentacle,
    SmtpTentacleConfig, SnmpTentacle, SnmpTentacleConfig, StdinTentacle, StdinTentacleConfig,
    SyslogTentacle, SyslogTentacleConfig, TeamsTentacle, TeamsTentacleConfig, TelegramTentacle,
    TelegramTentacleConfig, Tentacle, WebSocketTentacle, WebSocketTentacleConfig,
    WebhooksPollerTentacle, WebhooksPollerTentacleConfig, XmppTentacle, XmppTentacleConfig,
};

#[tokio::main]
async fn main() -> Result<()> {
    // Logging initialisieren
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::from_default_env()
                .add_directive("rustyKraken=info".parse()?),
        )
        .init();

    info!("🦑 Starting rustyKraken...");

    // Konfigurationsordner
    let config_dir = PathBuf::from("config");
    let tentacles_dir = config_dir.join("incoming");
    let reflexes_dir = config_dir.join("listener");

    // Event Bus erstellen
    let event_bus = Arc::new(EventBus::new(1000));

    // Mantle (Speicherung) initialisieren
    let mantle = Arc::new(Mantle::new("data/kraken.db").await?);

    // Event Bus Listener für Mantle-Speicherung
    let mantle_clone = Arc::clone(&mantle);
    let mut event_bus_receiver = event_bus.subscribe();

    tokio::spawn(async move {
        while let Ok(ink) = event_bus_receiver.recv().await {
            if let Err(e) = mantle_clone.store(&ink).await {
                error!("Failed to store ink in mantle: {}", e);
            }
        }
    });

    // Tentacles laden und starten
    let tentacle_configs = load_tentacles(&tentacles_dir)?;
    if tentacle_configs.is_empty() {
        info!("No tentacle configurations found. No tentacles will be started.");
    } else {
        info!("Loaded {} tentacle(s)", tentacle_configs.len());
    }

    for tentacle_config in tentacle_configs {
        match create_tentacle(&tentacle_config) {
            Ok(tentacle) => {
                let event_bus_clone = Arc::clone(&event_bus);
                tokio::spawn(async move {
                    if let Err(e) = tentacle.start(event_bus_clone).await {
                        error!("Tentacle {} failed: {}", tentacle_config.name, e);
                    }
                });
            }
            Err(e) => {
                error!("Failed to create tentacle {}: {}", tentacle_config.name, e);
            }
        }
    }

    // Reflexes laden und starten
    let reflex_configs = load_reflexes(&reflexes_dir)?;
    if reflex_configs.is_empty() {
        info!("No reflex configurations found. No reflexes will be started.");
    } else {
        info!("Loaded {} reflex(es)", reflex_configs.len());
    }

    // Für jeden Reflex einen eigenen Listener starten
    for reflex_config in reflex_configs {
        let reflex = create_reflex_handler(&reflex_config, &event_bus)?;
        let reflex_name = reflex_config.name.clone();
        let topics = reflex_config.trigger.topics.clone();

        let mut reflex_receiver = event_bus.subscribe();

        tokio::spawn(async move {
            info!(
                "Reflex '{}' listener started, watching topics: {:?}",
                reflex_name, topics
            );

            while let Ok(ink) = reflex_receiver.recv().await {
                // Prüfen, ob der Reflex auf dieses Topic reagieren soll
                let mut should_handle = false;
                for topic_pattern in &topics {
                    if EventBus::match_topic(topic_pattern, &ink.topic) {
                        should_handle = true;
                        break;
                    }
                }

                if should_handle {
                    info!(
                        "Reflex '{}' handling ink on topic: {}",
                        reflex_name, ink.topic
                    );
                    if let Err(e) = reflex.handle(&ink).await {
                        error!("Reflex '{}' failed to handle ink: {}", reflex_name, e);
                    }
                }
            }
        });
    }

    info!("🦑 rustyKraken is ready!");

    // Anwendung läuft, bis Ctrl+C gedrückt wird
    tokio::signal::ctrl_c().await?;
    info!("🦑 Shutting down rustyKraken...");

    Ok(())
}

fn create_tentacle(config: &TentacleConfig) -> Result<Box<dyn Tentacle>> {
    match config.r#type.as_str() {
        "mqtt" => {
            let mqtt_config: MqttTentacleConfig = serde_yaml::from_value(config.config.clone())?;
            Ok(Box::new(MqttTentacle::new(
                config.name.clone(),
                mqtt_config,
            )))
        }
        "http" => {
            let http_config: HttpTentacleConfig = serde_yaml::from_value(config.config.clone())?;
            Ok(Box::new(HttpTentacle::new(
                config.name.clone(),
                http_config,
            )))
        }
        "imap" => {
            let imap_config: ImapTentacleConfig = serde_yaml::from_value(config.config.clone())?;
            Ok(Box::new(ImapTentacle::new(
                config.name.clone(),
                imap_config,
            )))
        }
        "pop3" => {
            let pop3_config: Pop3TentacleConfig = serde_yaml::from_value(config.config.clone())?;
            Ok(Box::new(Pop3Tentacle::new(
                config.name.clone(),
                pop3_config,
            )))
        }
        "smtp" => {
            let smtp_config: SmtpTentacleConfig = serde_yaml::from_value(config.config.clone())?;
            Ok(Box::new(SmtpTentacle::new(
                config.name.clone(),
                smtp_config,
            )))
        }
        "ntfy" => {
            let ntfy_config: NtfyTentacleConfig = serde_yaml::from_value(config.config.clone())?;
            Ok(Box::new(NtfyTentacle::new(
                config.name.clone(),
                ntfy_config,
            )))
        }
        "telegram" => {
            let telegram_config: TelegramTentacleConfig =
                serde_yaml::from_value(config.config.clone())?;
            Ok(Box::new(TelegramTentacle::new(
                config.name.clone(),
                telegram_config,
            )))
        }
        "discord" => {
            let discord_config: DiscordTentacleConfig =
                serde_yaml::from_value(config.config.clone())?;
            Ok(Box::new(DiscordTentacle::new(
                config.name.clone(),
                discord_config,
            )))
        }
        "file_watcher" => {
            let file_watcher_config: FileWatcherTentacleConfig =
                serde_yaml::from_value(config.config.clone())?;
            Ok(Box::new(FileWatcherTentacle::new(
                config.name.clone(),
                file_watcher_config,
            )))
        }
        "stdin" => {
            let stdin_config: StdinTentacleConfig = serde_yaml::from_value(config.config.clone())?;
            Ok(Box::new(StdinTentacle::new(
                config.name.clone(),
                stdin_config,
            )))
        }
        "rest_api" => {
            let rest_api_config: RestApiTentacleConfig =
                serde_yaml::from_value(config.config.clone())?;
            Ok(Box::new(RestApiTentacle::new(
                config.name.clone(),
                rest_api_config,
            )))
        }
        "websocket" => {
            let websocket_config: WebSocketTentacleConfig =
                serde_yaml::from_value(config.config.clone())?;
            Ok(Box::new(WebSocketTentacle::new(
                config.name.clone(),
                websocket_config,
            )))
        }
        "rss" => {
            let rss_config: RssTentacleConfig = serde_yaml::from_value(config.config.clone())?;
            Ok(Box::new(RssTentacle::new(config.name.clone(), rss_config)))
        }
        "serial" => {
            let serial_config: SerialTentacleConfig =
                serde_yaml::from_value(config.config.clone())?;
            Ok(Box::new(SerialTentacle::new(
                config.name.clone(),
                serial_config,
            )))
        }
        "snmp" => {
            let snmp_config: SnmpTentacleConfig = serde_yaml::from_value(config.config.clone())?;
            Ok(Box::new(SnmpTentacle::new(
                config.name.clone(),
                snmp_config,
            )))
        }
        "aws_sqs" => {
            let aws_sqs_config: AwsSqsTentacleConfig =
                serde_yaml::from_value(config.config.clone())?;
            Ok(Box::new(AwsSqsTentacle::new(
                config.name.clone(),
                aws_sqs_config,
            )))
        }
        "google_pubsub" => {
            let google_pubsub_config: GooglePubSubTentacleConfig =
                serde_yaml::from_value(config.config.clone())?;
            Ok(Box::new(GooglePubSubTentacle::new(
                config.name.clone(),
                google_pubsub_config,
            )))
        }
        "slack" => {
            let slack_config: SlackTentacleConfig = serde_yaml::from_value(config.config.clone())?;
            Ok(Box::new(SlackTentacle::new(
                config.name.clone(),
                slack_config,
            )))
        }
        "caldav" => {
            let caldav_config: CalDavTentacleConfig =
                serde_yaml::from_value(config.config.clone())?;
            Ok(Box::new(CalDavTentacle::new(
                config.name.clone(),
                caldav_config,
            )))
        }
        "cron" => {
            let cron_config: CronTentacleConfig = serde_yaml::from_value(config.config.clone())?;
            Ok(Box::new(CronTentacle::new(
                config.name.clone(),
                cron_config,
            )))
        }
        "syslog" => {
            let syslog_config: SyslogTentacleConfig =
                serde_yaml::from_value(config.config.clone())?;
            Ok(Box::new(SyslogTentacle::new(
                config.name.clone(),
                syslog_config,
            )))
        }
        "nats" => {
            let nats_config: NatsTentacleConfig = serde_yaml::from_value(config.config.clone())?;
            Ok(Box::new(NatsTentacle::new(
                config.name.clone(),
                nats_config,
            )))
        }
        "redis" => {
            let redis_config: RedisTentacleConfig = serde_yaml::from_value(config.config.clone())?;
            Ok(Box::new(RedisTentacle::new(
                config.name.clone(),
                redis_config,
            )))
        }
        "rabbitmq" => {
            let rabbitmq_config: RabbitMqTentacleConfig =
                serde_yaml::from_value(config.config.clone())?;
            Ok(Box::new(RabbitMqTentacle::new(
                config.name.clone(),
                rabbitmq_config,
            )))
        }
        "kafka" => {
            let kafka_config: KafkaTentacleConfig = serde_yaml::from_value(config.config.clone())?;
            Ok(Box::new(KafkaTentacle::new(
                config.name.clone(),
                kafka_config,
            )))
        }
        "azure_servicebus" => {
            let asb_config: AsbTentacleConfig = serde_yaml::from_value(config.config.clone())?;
            Ok(Box::new(AsbTentacle::new(config.name.clone(), asb_config)))
        }
        "journalctl" => {
            let journal_config: JournalTentacleConfig =
                serde_yaml::from_value(config.config.clone())?;
            Ok(Box::new(JournalTentacle::new(
                config.name.clone(),
                journal_config,
            )))
        }
        "docker" => {
            let docker_config: DockerTentacleConfig =
                serde_yaml::from_value(config.config.clone())?;
            Ok(Box::new(DockerTentacle::new(
                config.name.clone(),
                docker_config,
            )))
        }
        "kubernetes" => {
            let k8s_config: K8sTentacleConfig = serde_yaml::from_value(config.config.clone())?;
            Ok(Box::new(K8sTentacle::new(config.name.clone(), k8s_config)))
        }
        "packet_capture" => {
            let packet_config: PacketCaptureConfig = serde_yaml::from_value(config.config.clone())?;
            Ok(Box::new(PacketCaptureTentacle::new(
                config.name.clone(),
                packet_config,
            )))
        }
        "graphql" => {
            let graphql_config: GraphqlTentacleConfig =
                serde_yaml::from_value(config.config.clone())?;
            Ok(Box::new(GraphqlTentacle::new(
                config.name.clone(),
                graphql_config,
            )))
        }
        "grpc" => {
            let grpc_config: GrpcTentacleConfig = serde_yaml::from_value(config.config.clone())?;
            Ok(Box::new(GrpcTentacle::new(
                config.name.clone(),
                grpc_config,
            )))
        }
        "database_poller" => {
            let db_poller_config: DbPollerTentacleConfig =
                serde_yaml::from_value(config.config.clone())?;
            Ok(Box::new(DbPollerTentacle::new(
                config.name.clone(),
                db_poller_config,
            )))
        }
        "elasticsearch" => {
            let es_config: EsTentacleConfig = serde_yaml::from_value(config.config.clone())?;
            Ok(Box::new(EsTentacle::new(config.name.clone(), es_config)))
        }
        "ftp_sftp" => {
            let ftp_config: FtpSftpTentacleConfig = serde_yaml::from_value(config.config.clone())?;
            Ok(Box::new(FtpSftpTentacle::new(
                config.name.clone(),
                ftp_config,
            )))
        }
        "geofence" => {
            let geofence_config: GeofenceTentacleConfig =
                serde_yaml::from_value(config.config.clone())?;
            Ok(Box::new(GeofenceTentacle::new(
                config.name.clone(),
                geofence_config,
            )))
        }
        "matrix" => {
            let matrix_config: MatrixTentacleConfig =
                serde_yaml::from_value(config.config.clone())?;
            Ok(Box::new(MatrixTentacle::new(
                config.name.clone(),
                matrix_config,
            )))
        }
        "signal" => {
            let signal_config: SignalTentacleConfig =
                serde_yaml::from_value(config.config.clone())?;
            Ok(Box::new(SignalTentacle::new(
                config.name.clone(),
                signal_config,
            )))
        }
        "teams" => {
            let teams_config: TeamsTentacleConfig = serde_yaml::from_value(config.config.clone())?;
            Ok(Box::new(TeamsTentacle::new(
                config.name.clone(),
                teams_config,
            )))
        }
        "irc" => {
            let irc_config: IrcTentacleConfig = serde_yaml::from_value(config.config.clone())?;
            Ok(Box::new(IrcTentacle::new(config.name.clone(), irc_config)))
        }
        "xmpp" => {
            let xmpp_config: XmppTentacleConfig = serde_yaml::from_value(config.config.clone())?;
            Ok(Box::new(XmppTentacle::new(
                config.name.clone(),
                xmpp_config,
            )))
        }
        "webhooks_poller" => {
            let poller_config: WebhooksPollerTentacleConfig =
                serde_yaml::from_value(config.config.clone())?;
            Ok(Box::new(WebhooksPollerTentacle::new(
                config.name.clone(),
                poller_config,
            )))
        }
        _ => anyhow::bail!("Unknown tentacle type: {}", config.r#type),
    }
}

fn create_reflex_handler(
    config: &ReflexConfig,
    event_bus: &Arc<EventBus>,
) -> Result<Box<dyn Reflex>> {
    match config.action.r#type.as_str() {
        "file" => {
            let file_config: FileReflexConfig =
                serde_yaml::from_value(config.action.config.clone())?;
            Ok(Box::new(FileReflex::new(config.name.clone(), file_config)))
        }
        "command" => {
            let command_config: CommandReflexConfig =
                serde_yaml::from_value(config.action.config.clone())?;
            Ok(Box::new(CommandReflex::new(
                config.name.clone(),
                command_config,
            )))
        }
        "whatsapp" => {
            let whatsapp_config: WhatsAppReflexConfig =
                serde_yaml::from_value(config.action.config.clone())?;
            Ok(Box::new(WhatsAppReflex::new(
                config.name.clone(),
                whatsapp_config,
            )))
        }
        "tentacle" => {
            let tentacle_config: TentacleReflexConfig =
                serde_yaml::from_value(config.action.config.clone())?;
            Ok(Box::new(TentacleReflex::new(
                config.name.clone(),
                tentacle_config,
                Arc::clone(event_bus),
            )))
        }
        "telegram" => {
            let telegram_config: TelegramReflexConfig =
                serde_yaml::from_value(config.action.config.clone())?;
            Ok(Box::new(TelegramReflex::new(
                config.name.clone(),
                telegram_config,
            )))
        }
        "discord" => {
            let discord_config: DiscordReflexConfig =
                serde_yaml::from_value(config.action.config.clone())?;
            Ok(Box::new(DiscordReflex::new(
                config.name.clone(),
                discord_config,
            )))
        }
        "ntfy" => {
            let ntfy_config: NtfyReflexConfig =
                serde_yaml::from_value(config.action.config.clone())?;
            Ok(Box::new(NtfyReflex::new(config.name.clone(), ntfy_config)))
        }
        "email" => {
            let email_config: EmailReflexConfig =
                serde_yaml::from_value(config.action.config.clone())?;
            Ok(Box::new(EmailReflex::new(
                config.name.clone(),
                email_config,
            )?))
        }
        "http" => {
            let http_config: HttpReflexConfig =
                serde_yaml::from_value(config.action.config.clone())?;
            Ok(Box::new(HttpReflex::new(config.name.clone(), http_config)?))
        }
        "matrix" => {
            let matrix_config: MatrixReflexConfig =
                serde_yaml::from_value(config.action.config.clone())?;
            Ok(Box::new(MatrixReflex::new(
                config.name.clone(),
                matrix_config,
            )?))
        }
        "syslog" => {
            let syslog_config: SyslogReflexConfig =
                serde_yaml::from_value(config.action.config.clone())?;
            Ok(Box::new(SyslogReflex::new(
                config.name.clone(),
                syslog_config,
            )?))
        }
        "home_assistant" => {
            let ha_config: HomeAssistantReflexConfig =
                serde_yaml::from_value(config.action.config.clone())?;
            Ok(Box::new(HomeAssistantReflex::new(
                config.name.clone(),
                ha_config,
            )?))
        }
        "database" => {
            let db_config: DatabaseReflexConfig =
                serde_yaml::from_value(config.action.config.clone())?;
            Ok(Box::new(DatabaseReflex::new(
                config.name.clone(),
                db_config,
            )?))
        }
        "aggregator" => {
            let agg_config: AggregatorReflexConfig =
                serde_yaml::from_value(config.action.config.clone())?;
            Ok(Box::new(AggregatorReflex::new(
                config.name.clone(),
                agg_config,
                Arc::clone(event_bus),
            )))
        }
        "script" => {
            let script_config: ScriptReflexConfig =
                serde_yaml::from_value(config.action.config.clone())?;
            Ok(Box::new(ScriptReflex::new(
                config.name.clone(),
                script_config,
            )))
        }
        _ => anyhow::bail!("Unknown reflex type: {}", config.action.r#type),
    }
}
