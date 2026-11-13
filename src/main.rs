mod appcontext;
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
    AggregatorReflex, AggregatorReflexConfig, AnsibleReflex, AnsibleReflexConfig, AwsSnsReflex,
    AwsSnsReflexConfig, AzureServiceBusReflex, AzureServiceBusReflexConfig, BarkReflex,
    BarkReflexConfig, CommandReflex, CommandReflexConfig, CustomWebhookReflex,
    CustomWebhookReflexConfig, DatabaseReflex, DatabaseReflexConfig, DiscordReflex,
    DiscordReflexConfig, DockerReflex, DockerReflexConfig, ElasticsearchReflex,
    ElasticsearchReflexConfig, EmailReflex, EmailReflexConfig, EsphomeReflex, EsphomeReflexConfig,
    Fail2BanReflex, Fail2BanReflexConfig, FhemReflex, FhemReflexConfig, FileReflex,
    FileReflexConfig, FtpSftpUploadReflex, FtpSftpUploadReflexConfig, GoogleChatReflex,
    GoogleChatReflexConfig, GotifyReflex, GotifyReflexConfig, GrafanaReflex, GrafanaReflexConfig,
    HomeAssistantReflex, HomeAssistantReflexConfig, HttpReflex, HttpReflexConfig, InfluxdbReflex,
    InfluxdbReflexConfig, IrcReflex, IrcReflexConfig, KafkaReflex, KafkaReflexConfig,
    KubernetesReflex, KubernetesReflexConfig, LedDisplayReflex, LedDisplayReflexConfig, MakeReflex,
    MakeReflexConfig, MatrixReflex, MatrixReflexConfig, MqttPublishReflex, MqttPublishReflexConfig,
    N8nReflex, N8nReflexConfig, NatsReflex, NatsReflexConfig, NtfyReflex, NtfyReflexConfig,
    OpenhabReflex, OpenhabReflexConfig, PowerDnsReflex, PowerDnsReflexConfig, Printer3dReflex,
    Printer3dReflexConfig, PrinterReflex, PrinterReflexConfig, PrometheusReflex,
    PrometheusReflexConfig, PuppetReflex, PuppetReflexConfig, PushbulletReflex,
    PushbulletReflexConfig, PushoverReflex, PushoverReflexConfig, RabbitMqReflex,
    RabbitMqReflexConfig, Reflex, S3MinioReflex, S3MinioReflexConfig, SaltStackReflex,
    SaltStackReflexConfig, ScriptReflex, ScriptReflexConfig, ShellyReflex, ShellyReflexConfig,
    ShodanReflex, ShodanReflexConfig, SignalReflex, SignalReflexConfig, SoundAlertReflex,
    SoundAlertReflexConfig, SyslogReflex, SyslogReflexConfig, TasmotaReflex, TasmotaReflexConfig,
    TeamsReflex, TeamsReflexConfig, TelegramReflex, TelegramReflexConfig, TemporalReflex,
    TemporalReflexConfig, TentacleReflex, TentacleReflexConfig, TheHiveReflex, TheHiveReflexConfig,
    TtsReflex, TtsReflexConfig, WhatsAppReflex, WhatsAppReflexConfig, ZapierReflex,
    ZapierReflexConfig, Zigbee2MqttReflex, Zigbee2MqttReflexConfig,
};
use tentacles::{
    AsbTentacle, AsbTentacleConfig, AwsSqsTentacle, AwsSqsTentacleConfig, BleTentacle,
    BleTentacleConfig, CalDavTentacle, CalDavTentacleConfig, CronTentacle, CronTentacleConfig,
    DbPollerTentacle, DbPollerTentacleConfig, DiscordTentacle, DiscordTentacleConfig,
    DockerTentacle, DockerTentacleConfig, EsTentacle, EsTentacleConfig, FileWatcherTentacle,
    FileWatcherTentacleConfig, FtpSftpTentacle, FtpSftpTentacleConfig, GeofenceTentacle,
    GeofenceTentacleConfig, GoogleChatTentacle, GoogleChatTentacleConfig, GooglePubSubTentacle,
    GooglePubSubTentacleConfig, GraphqlTentacle, GraphqlTentacleConfig, GrpcTentacle,
    GrpcTentacleConfig, HttpTentacle, HttpTentacleConfig, ImapTentacle, ImapTentacleConfig,
    IrcTentacle, IrcTentacleConfig, JournalTentacle, JournalTentacleConfig, K8sTentacle,
    K8sTentacleConfig, KafkaTentacle, KafkaTentacleConfig, MatrixTentacle, MatrixTentacleConfig,
    ModbusTentacle, ModbusTentacleConfig, MqttTentacle, MqttTentacleConfig, NatsTentacle,
    NatsTentacleConfig, NtfyTentacle, NtfyTentacleConfig, PacketCaptureConfig,
    PacketCaptureTentacle, Pop3Tentacle, Pop3TentacleConfig, RabbitMqTentacle,
    RabbitMqTentacleConfig, RedisTentacle, RedisTentacleConfig, RestApiTentacle,
    RestApiTentacleConfig, RssTentacle, RssTentacleConfig, SerialTentacle, SerialTentacleConfig,
    SignalTentacle, SignalTentacleConfig, SlackTentacle, SlackTentacleConfig, SmtpTentacle,
    SmtpTentacleConfig, SnmpTentacle, SnmpTentacleConfig, StdinTentacle, StdinTentacleConfig,
    SyslogTentacle, SyslogTentacleConfig, TeamsTentacle, TeamsTentacleConfig, TelegramTentacle,
    TelegramTentacleConfig, Tentacle, WebSocketTentacle, WebSocketTentacleConfig,
    WebhooksPollerTentacle, WebhooksPollerTentacleConfig, XmppTentacle, XmppTentacleConfig,
    ZigbeeTentacle, ZigbeeTentacleConfig, ZwaveTentacle, ZwaveTentacleConfig,
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

    // App Context mit geteiltem HTTP-Client erstellen
    let app_ctx = appcontext::AppContext::new();
    info!("HTTP client pool initialized");

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
        match create_tentacle(&tentacle_config, &app_ctx) {
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
        let reflex = create_reflex_handler(&reflex_config, &event_bus, &app_ctx)?;
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

fn create_tentacle(
    config: &TentacleConfig,
    app_ctx: &appcontext::AppContext,
) -> Result<Box<dyn Tentacle>> {
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
                app_ctx,
            )))
        }
        "telegram" => {
            let telegram_config: TelegramTentacleConfig =
                serde_yaml::from_value(config.config.clone())?;
            Ok(Box::new(TelegramTentacle::new(
                config.name.clone(),
                telegram_config,
                app_ctx,
            )))
        }
        "discord" => {
            let discord_config: DiscordTentacleConfig =
                serde_yaml::from_value(config.config.clone())?;
            Ok(Box::new(DiscordTentacle::new(
                config.name.clone(),
                discord_config,
                app_ctx,
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
                app_ctx,
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
            Ok(Box::new(RssTentacle::new(
                config.name.clone(),
                rss_config,
                app_ctx,
            )))
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
                app_ctx,
            )))
        }
        "google_chat" => {
            let google_chat_config: GoogleChatTentacleConfig =
                serde_yaml::from_value(config.config.clone())?;
            Ok(Box::new(GoogleChatTentacle::new(
                config.name.clone(),
                google_chat_config,
            )))
        }
        "google_pubsub" => {
            let google_pubsub_config: GooglePubSubTentacleConfig =
                serde_yaml::from_value(config.config.clone())?;
            Ok(Box::new(GooglePubSubTentacle::new(
                config.name.clone(),
                google_pubsub_config,
                app_ctx,
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
            Ok(Box::new(AsbTentacle::new(
                config.name.clone(),
                asb_config,
                app_ctx,
            )))
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
                app_ctx,
            )))
        }
        "kubernetes" => {
            let k8s_config: K8sTentacleConfig = serde_yaml::from_value(config.config.clone())?;
            Ok(Box::new(K8sTentacle::new(
                config.name.clone(),
                k8s_config,
                app_ctx,
            )))
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
                app_ctx,
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
        "modbus" => {
            let modbus_config: ModbusTentacleConfig =
                serde_yaml::from_value(config.config.clone())?;
            Ok(Box::new(ModbusTentacle::new(
                config.name.clone(),
                modbus_config,
            )))
        }
        "ble" => {
            let ble_config: BleTentacleConfig = serde_yaml::from_value(config.config.clone())?;
            Ok(Box::new(BleTentacle::new(config.name.clone(), ble_config)))
        }
        "zigbee" => {
            let zigbee_config: ZigbeeTentacleConfig =
                serde_yaml::from_value(config.config.clone())?;
            Ok(Box::new(ZigbeeTentacle::new(
                config.name.clone(),
                zigbee_config,
            )))
        }
        "zwave" => {
            let zwave_config: ZwaveTentacleConfig = serde_yaml::from_value(config.config.clone())?;
            Ok(Box::new(ZwaveTentacle::new(
                config.name.clone(),
                zwave_config,
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
    app_ctx: &appcontext::AppContext,
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
                app_ctx,
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
                app_ctx,
            )))
        }
        "discord" => {
            let discord_config: DiscordReflexConfig =
                serde_yaml::from_value(config.action.config.clone())?;
            Ok(Box::new(DiscordReflex::new(
                config.name.clone(),
                discord_config,
                app_ctx,
            )))
        }
        "ntfy" => {
            let ntfy_config: NtfyReflexConfig =
                serde_yaml::from_value(config.action.config.clone())?;
            Ok(Box::new(NtfyReflex::new(
                config.name.clone(),
                ntfy_config,
                app_ctx,
            )))
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
            Ok(Box::new(HttpReflex::new(
                config.name.clone(),
                http_config,
                app_ctx,
            )?))
        }
        "matrix" => {
            let matrix_config: MatrixReflexConfig =
                serde_yaml::from_value(config.action.config.clone())?;
            Ok(Box::new(MatrixReflex::new(
                config.name.clone(),
                matrix_config,
                app_ctx,
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
                app_ctx,
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
        "signal" => {
            let signal_config: SignalReflexConfig =
                serde_yaml::from_value(config.action.config.clone())?;
            Ok(Box::new(SignalReflex::new(
                config.name.clone(),
                signal_config,
                app_ctx,
            )))
        }
        "teams" => {
            let teams_config: TeamsReflexConfig =
                serde_yaml::from_value(config.action.config.clone())?;
            Ok(Box::new(TeamsReflex::new(
                config.name.clone(),
                teams_config,
                app_ctx,
            )))
        }
        "google_chat" => {
            let gc_config: GoogleChatReflexConfig =
                serde_yaml::from_value(config.action.config.clone())?;
            Ok(Box::new(GoogleChatReflex::new(
                config.name.clone(),
                gc_config,
                app_ctx,
            )))
        }
        "irc" => {
            let irc_config: IrcReflexConfig = serde_yaml::from_value(config.action.config.clone())?;
            Ok(Box::new(IrcReflex::new(
                config.name.clone(),
                irc_config,
                app_ctx,
            )))
        }
        "pushbullet" => {
            let pb_config: PushbulletReflexConfig =
                serde_yaml::from_value(config.action.config.clone())?;
            Ok(Box::new(PushbulletReflex::new(
                config.name.clone(),
                pb_config,
                app_ctx,
            )))
        }
        "gotify" => {
            let gotify_config: GotifyReflexConfig =
                serde_yaml::from_value(config.action.config.clone())?;
            Ok(Box::new(GotifyReflex::new(
                config.name.clone(),
                gotify_config,
                app_ctx,
            )))
        }
        "azure_servicebus" => {
            let asb_config: AzureServiceBusReflexConfig =
                serde_yaml::from_value(config.action.config.clone())?;
            Ok(Box::new(AzureServiceBusReflex::new(
                config.name.clone(),
                asb_config,
                app_ctx,
            )))
        }
        "rabbitmq" => {
            let rmq_config: RabbitMqReflexConfig =
                serde_yaml::from_value(config.action.config.clone())?;
            Ok(Box::new(RabbitMqReflex::new(
                config.name.clone(),
                rmq_config,
                app_ctx,
            )))
        }
        "kafka" => {
            let kafka_config: KafkaReflexConfig =
                serde_yaml::from_value(config.action.config.clone())?;
            Ok(Box::new(KafkaReflex::new(
                config.name.clone(),
                kafka_config,
                app_ctx,
            )))
        }
        "nats" => {
            let nats_config: NatsReflexConfig =
                serde_yaml::from_value(config.action.config.clone())?;
            Ok(Box::new(NatsReflex::new(config.name.clone(), nats_config)))
        }
        "aws_sns" => {
            let sns_config: AwsSnsReflexConfig =
                serde_yaml::from_value(config.action.config.clone())?;
            Ok(Box::new(AwsSnsReflex::new(
                config.name.clone(),
                sns_config,
                app_ctx,
            )))
        }
        "docker" => {
            let docker_config: DockerReflexConfig =
                serde_yaml::from_value(config.action.config.clone())?;
            Ok(Box::new(DockerReflex::new(
                config.name.clone(),
                docker_config,
                app_ctx,
            )))
        }
        "kubernetes" => {
            let k8s_config: KubernetesReflexConfig =
                serde_yaml::from_value(config.action.config.clone())?;
            Ok(Box::new(KubernetesReflex::new(
                config.name.clone(),
                k8s_config,
                app_ctx,
            )))
        }
        "ansible" => {
            let ansible_config: AnsibleReflexConfig =
                serde_yaml::from_value(config.action.config.clone())?;
            Ok(Box::new(AnsibleReflex::new(
                config.name.clone(),
                ansible_config,
                app_ctx,
            )))
        }
        "saltstack" => {
            let salt_config: SaltStackReflexConfig =
                serde_yaml::from_value(config.action.config.clone())?;
            Ok(Box::new(SaltStackReflex::new(
                config.name.clone(),
                salt_config,
                app_ctx,
            )))
        }
        "puppet" => {
            let puppet_config: PuppetReflexConfig =
                serde_yaml::from_value(config.action.config.clone())?;
            Ok(Box::new(PuppetReflex::new(
                config.name.clone(),
                puppet_config,
                app_ctx,
            )))
        }
        "powerdns" => {
            let pdns_config: PowerDnsReflexConfig =
                serde_yaml::from_value(config.action.config.clone())?;
            Ok(Box::new(PowerDnsReflex::new(
                config.name.clone(),
                pdns_config,
                app_ctx,
            )))
        }
        "mqtt_publish" => {
            let mqtt_config: MqttPublishReflexConfig =
                serde_yaml::from_value(config.action.config.clone())?;
            Ok(Box::new(MqttPublishReflex::new(
                config.name.clone(),
                mqtt_config,
            )))
        }
        "zigbee2mqtt" => {
            let z2m_config: Zigbee2MqttReflexConfig =
                serde_yaml::from_value(config.action.config.clone())?;
            Ok(Box::new(Zigbee2MqttReflex::new(
                config.name.clone(),
                z2m_config,
            )))
        }
        "shelly" => {
            let shelly_config: ShellyReflexConfig =
                serde_yaml::from_value(config.action.config.clone())?;
            Ok(Box::new(ShellyReflex::new(
                config.name.clone(),
                shelly_config,
                app_ctx,
            )))
        }
        "tasmota" => {
            let tasmota_config: TasmotaReflexConfig =
                serde_yaml::from_value(config.action.config.clone())?;
            Ok(Box::new(TasmotaReflex::new(
                config.name.clone(),
                tasmota_config,
                app_ctx,
            )))
        }
        "esphome" => {
            let esphome_config: EsphomeReflexConfig =
                serde_yaml::from_value(config.action.config.clone())?;
            Ok(Box::new(EsphomeReflex::new(
                config.name.clone(),
                esphome_config,
                app_ctx,
            )))
        }
        "fhem" => {
            let fhem_config: FhemReflexConfig =
                serde_yaml::from_value(config.action.config.clone())?;
            Ok(Box::new(FhemReflex::new(
                config.name.clone(),
                fhem_config,
                app_ctx,
            )))
        }
        "openhab" => {
            let oh_config: OpenhabReflexConfig =
                serde_yaml::from_value(config.action.config.clone())?;
            Ok(Box::new(OpenhabReflex::new(
                config.name.clone(),
                oh_config,
                app_ctx,
            )))
        }
        "pushover" => {
            let pushover_config: PushoverReflexConfig =
                serde_yaml::from_value(config.action.config.clone())?;
            Ok(Box::new(PushoverReflex::new(
                config.name.clone(),
                pushover_config,
                app_ctx,
            )))
        }
        "bark" => {
            let bark_config: BarkReflexConfig =
                serde_yaml::from_value(config.action.config.clone())?;
            Ok(Box::new(BarkReflex::new(
                config.name.clone(),
                bark_config,
                app_ctx,
            )))
        }
        "custom_webhook" => {
            let cw_config: CustomWebhookReflexConfig =
                serde_yaml::from_value(config.action.config.clone())?;
            Ok(Box::new(CustomWebhookReflex::new(
                config.name.clone(),
                cw_config,
                app_ctx,
            )))
        }
        "sound_alert" => {
            let sa_config: SoundAlertReflexConfig =
                serde_yaml::from_value(config.action.config.clone())?;
            Ok(Box::new(SoundAlertReflex::new(
                config.name.clone(),
                sa_config,
            )))
        }
        "led_display" => {
            let led_config: LedDisplayReflexConfig =
                serde_yaml::from_value(config.action.config.clone())?;
            Ok(Box::new(LedDisplayReflex::new(
                config.name.clone(),
                led_config,
                app_ctx,
            )))
        }
        "elasticsearch" => {
            let es_config: ElasticsearchReflexConfig =
                serde_yaml::from_value(config.action.config.clone())?;
            Ok(Box::new(ElasticsearchReflex::new(
                config.name.clone(),
                es_config,
                app_ctx,
            )))
        }
        "influxdb" => {
            let influx_config: InfluxdbReflexConfig =
                serde_yaml::from_value(config.action.config.clone())?;
            Ok(Box::new(InfluxdbReflex::new(
                config.name.clone(),
                influx_config,
                app_ctx,
            )))
        }
        "prometheus" => {
            let prom_config: PrometheusReflexConfig =
                serde_yaml::from_value(config.action.config.clone())?;
            Ok(Box::new(PrometheusReflex::new(
                config.name.clone(),
                prom_config,
                app_ctx,
            )))
        }
        "s3_minio" => {
            let s3_config: S3MinioReflexConfig =
                serde_yaml::from_value(config.action.config.clone())?;
            Ok(Box::new(S3MinioReflex::new(
                config.name.clone(),
                s3_config,
                app_ctx,
            )))
        }
        "ftp_sftp_upload" => {
            let ftp_config: FtpSftpUploadReflexConfig =
                serde_yaml::from_value(config.action.config.clone())?;
            Ok(Box::new(FtpSftpUploadReflex::new(
                config.name.clone(),
                ftp_config,
                app_ctx,
            )))
        }
        "fail2ban" => {
            let f2b_config: Fail2BanReflexConfig =
                serde_yaml::from_value(config.action.config.clone())?;
            Ok(Box::new(Fail2BanReflex::new(
                config.name.clone(),
                f2b_config,
            )))
        }
        "grafana" => {
            let grafana_config: GrafanaReflexConfig =
                serde_yaml::from_value(config.action.config.clone())?;
            Ok(Box::new(GrafanaReflex::new(
                config.name.clone(),
                grafana_config,
                app_ctx,
            )))
        }
        "thehive" => {
            let hive_config: TheHiveReflexConfig =
                serde_yaml::from_value(config.action.config.clone())?;
            Ok(Box::new(TheHiveReflex::new(
                config.name.clone(),
                hive_config,
                app_ctx,
            )))
        }
        "shodan" => {
            let shodan_config: ShodanReflexConfig =
                serde_yaml::from_value(config.action.config.clone())?;
            Ok(Box::new(ShodanReflex::new(
                config.name.clone(),
                shodan_config,
                app_ctx,
            )))
        }
        "n8n" => {
            let n8n_config: N8nReflexConfig = serde_yaml::from_value(config.action.config.clone())?;
            Ok(Box::new(N8nReflex::new(
                config.name.clone(),
                n8n_config,
                app_ctx,
            )))
        }
        "make" => {
            let make_config: MakeReflexConfig =
                serde_yaml::from_value(config.action.config.clone())?;
            Ok(Box::new(MakeReflex::new(config.name.clone(), make_config)))
        }
        "zapier" => {
            let zapier_config: ZapierReflexConfig =
                serde_yaml::from_value(config.action.config.clone())?;
            Ok(Box::new(ZapierReflex::new(
                config.name.clone(),
                zapier_config,
                app_ctx,
            )))
        }
        "temporal" => {
            let temporal_config: TemporalReflexConfig =
                serde_yaml::from_value(config.action.config.clone())?;
            Ok(Box::new(TemporalReflex::new(
                config.name.clone(),
                temporal_config,
                app_ctx,
            )))
        }
        "tts" => {
            let tts_config: TtsReflexConfig = serde_yaml::from_value(config.action.config.clone())?;
            Ok(Box::new(TtsReflex::new(config.name.clone(), tts_config)))
        }
        "printer" => {
            let printer_config: PrinterReflexConfig =
                serde_yaml::from_value(config.action.config.clone())?;
            Ok(Box::new(PrinterReflex::new(
                config.name.clone(),
                printer_config,
                app_ctx,
            )))
        }
        "printer_3d" => {
            let p3d_config: Printer3dReflexConfig =
                serde_yaml::from_value(config.action.config.clone())?;
            Ok(Box::new(Printer3dReflex::new(
                config.name.clone(),
                p3d_config,
                app_ctx,
            )))
        }
        _ => anyhow::bail!("Unknown reflex type: {}", config.action.r#type),
    }
}
