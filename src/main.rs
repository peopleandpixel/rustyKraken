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
use reflexes::{CommandReflex, CommandReflexConfig, FileReflex, FileReflexConfig, Reflex};
use tentacles::{
    DiscordTentacle, DiscordTentacleConfig, FileWatcherTentacle, FileWatcherTentacleConfig,
    HttpTentacle, HttpTentacleConfig, ImapTentacle, ImapTentacleConfig, MqttTentacle,
    MqttTentacleConfig, NtfyTentacle, NtfyTentacleConfig, Pop3Tentacle, Pop3TentacleConfig,
    SmtpTentacle, SmtpTentacleConfig, TelegramTentacle, TelegramTentacleConfig, Tentacle,
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
    info!("Loaded {} tentacle(s)", tentacle_configs.len());

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
    info!("Loaded {} reflex(es)", reflex_configs.len());

    // Für jeden Reflex einen eigenen Listener starten
    for reflex_config in reflex_configs {
        let reflex = create_reflex_handler(&reflex_config)?;
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
        _ => anyhow::bail!("Unknown tentacle type: {}", config.r#type),
    }
}

fn create_reflex_handler(config: &ReflexConfig) -> Result<Box<dyn Reflex>> {
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
        _ => anyhow::bail!("Unknown reflex type: {}", config.action.r#type),
    }
}
