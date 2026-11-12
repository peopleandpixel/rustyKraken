use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tracing::{debug, error, info, warn};

use super::Tentacle;
use crate::eventbus::EventBus;
use crate::model::Ink;

/// Konfiguration für den IRC Tentacle
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IrcTentacleConfig {
    /// IRC Server-Adresse
    #[serde(default = "default_server")]
    pub server: String,
    /// Server-Port
    #[serde(default = "default_port")]
    pub port: u16,
    /// Nickname
    pub nickname: String,
    /// Channels zum Beitreten
    pub channels: Vec<String>,
    /// Passwort (optional)
    #[serde(default)]
    pub password: Option<String>,
    /// Topic für Events
    #[serde(default = "default_topic")]
    pub topic: String,
    /// Reconnect-Verzögerung in Sekunden
    #[serde(default = "default_reconnect_delay")]
    pub reconnect_delay_secs: u64,
    /// TLS aktivieren
    #[serde(default)]
    pub tls_enabled: bool,
    /// Realname
    #[serde(default = "default_realname")]
    pub realname: String,
    /// Username
    #[serde(default = "default_username")]
    pub username: String,
}

fn default_server() -> String {
    "irc.libera.chat".to_string()
}

fn default_port() -> u16 {
    6667
}

fn default_topic() -> String {
    "/irc/message".to_string()
}

fn default_reconnect_delay() -> u64 {
    5
}

fn default_realname() -> String {
    "rustyKraken".to_string()
}

fn default_username() -> String {
    "rustykraken".to_string()
}

/// IRC-Nachricht
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IrcMessage {
    pub channel: String,
    pub nick: String,
    pub message: String,
    pub r#type: String,
}

pub struct IrcTentacle {
    name: String,
    config: IrcTentacleConfig,
}

impl IrcTentacle {
    pub fn new(name: String, config: IrcTentacleConfig) -> Self {
        Self { name, config }
    }

    #[allow(dead_code)]
    fn create_message_ink(&self, message: &IrcMessage, tentacle_name: &str) -> Ink {
        let mut meta_map = serde_json::Map::new();
        meta_map.insert("channel".into(), serde_json::json!(message.channel));
        meta_map.insert("nick".into(), serde_json::json!(message.nick));
        meta_map.insert("message_type".into(), serde_json::json!(message.r#type));

        Ink::new(
            &self.config.topic,
            "irc",
            tentacle_name,
            format!(
                "[{}] {}: {}",
                message.channel, message.nick, message.message
            ),
        )
        .with_metadata(serde_json::Value::Object(meta_map))
    }
}

#[async_trait]
impl Tentacle for IrcTentacle {
    fn name(&self) -> &str {
        &self.name
    }

    fn r#type(&self) -> &str {
        "irc"
    }

    async fn start(&self, event_bus: Arc<EventBus>) -> Result<()> {
        info!(
            "IRC Tentacle '{}' connecting to {}:{} for {} channels",
            self.name,
            self.config.server,
            self.config.port,
            self.config.channels.len()
        );

        let config = self.config.clone();
        let tentacle_name = self.name.clone();
        let event_bus_clone = Arc::clone(&event_bus);

        tokio::spawn(async move {
            loop {
                match connect_irc(&config, &tentacle_name, &event_bus_clone).await {
                    Ok(_) => {}
                    Err(e) => {
                        error!("IRC Tentacle '{}': connection failed: {}", tentacle_name, e);
                    }
                }

                warn!(
                    "IRC Tentacle '{}': reconnecting in {} seconds...",
                    tentacle_name, config.reconnect_delay_secs
                );
                tokio::time::sleep(Duration::from_secs(config.reconnect_delay_secs)).await;
            }
        });

        loop {
            tokio::time::sleep(Duration::from_secs(60)).await;
        }
    }
}

async fn connect_irc(
    config: &IrcTentacleConfig,
    tentacle_name: &str,
    event_bus: &Arc<EventBus>,
) -> Result<()> {
    info!(
        "IRC Tentacle '{}': connecting to {}:{}...",
        tentacle_name, config.server, config.port
    );

    let stream = TcpStream::connect(format!("{}:{}", config.server, config.port)).await?;
    let (reader, writer) = stream.into_split();
    let mut reader = BufReader::new(reader).lines();
    let writer = Arc::new(tokio::sync::Mutex::new(writer));

    // Nickname registrieren
    let w = Arc::clone(&writer);
    {
        let mut w = w.lock().await;
        w.write_all(format!("NICK {}\r\n", config.nickname).as_bytes())
            .await?;
        w.write_all(
            format!(
                "USER {} {} {} :{}\r\n",
                config.username, config.server, config.realname, config.nickname
            )
            .as_bytes(),
        )
        .await?;
    }

    // Passwort
    if let Some(ref password) = config.password {
        let w = Arc::clone(&writer);
        {
            let mut w = w.lock().await;
            w.write_all(format!("PASS {}\r\n", password).as_bytes())
                .await?;
        }
    }

    // Channels beitreten
    for channel in &config.channels {
        let w = Arc::clone(&writer);
        {
            let mut w = w.lock().await;
            w.write_all(format!("JOIN {}\r\n", channel).as_bytes())
                .await?;
        }
        info!(
            "IRC Tentacle '{}': joined channel {}",
            tentacle_name, channel
        );
    }

    // PING/PONG Handler
    let ping_writer = Arc::clone(&writer);
    let ping_task = tokio::spawn(async move {
        loop {
            tokio::time::sleep(Duration::from_secs(30)).await;
            let mut w = ping_writer.lock().await;
            if w.write_all(b"PING :rustyKraken\r\n").await.is_err() {
                break;
            }
        }
    });

    // Nachrichten lesen
    while let Ok(Some(line)) = reader.next_line().await {
        let line = line.trim().to_string();
        debug!("IRC Tentacle '{}': received: {}", tentacle_name, line);

        if line.starts_with("PING") {
            let w = Arc::clone(&writer);
            if let Ok(mut w) = w.try_lock() {
                let _ = w
                    .write_all(format!("PONG :{}\r\n", &line[5..]).as_bytes())
                    .await;
            }
            continue;
        }

        // PRIVMSG parsen
        if line.contains("PRIVMSG")
            && let Some(message) = parse_privmsg(&line, &config.nickname)
        {
            let ink = create_irc_ink(config, tentacle_name, &message);
            event_bus.publish(ink);
        }
    }

    ping_task.abort();
    Ok(())
}

fn parse_privmsg(line: &str, own_nick: &str) -> Option<IrcMessage> {
    // :nick!user@host PRIVMSG #channel :message
    let parts: Vec<&str> = line.splitn(4, ' ').collect();
    if parts.len() < 4 {
        return None;
    }

    let sender = parts[0].trim_start_matches(':');
    let nick = sender.split('!').next().unwrap_or(sender);

    // Ignoriere eigene Nachrichten
    if nick == own_nick {
        return None;
    }

    let channel = parts[2].to_string();
    let message = parts[3].trim_start_matches(':').to_string();

    Some(IrcMessage {
        channel,
        nick: nick.to_string(),
        message,
        r#type: "PRIVMSG".to_string(),
    })
}

fn create_irc_ink(config: &IrcTentacleConfig, tentacle_name: &str, message: &IrcMessage) -> Ink {
    let mut meta_map = serde_json::Map::new();
    meta_map.insert("channel".into(), serde_json::json!(message.channel));
    meta_map.insert("nick".into(), serde_json::json!(message.nick));
    meta_map.insert("message_type".into(), serde_json::json!(message.r#type));

    Ink::new(
        &config.topic,
        "irc",
        tentacle_name,
        format!(
            "[{}] {}: {}",
            message.channel, message.nick, message.message
        ),
    )
    .with_metadata(serde_json::Value::Object(meta_map))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_defaults() {
        let config: IrcTentacleConfig =
            serde_json::from_str(r#"{"nickname": "testbot", "channels": ["\u0023test"]}"#).unwrap();

        assert_eq!(config.server, "irc.libera.chat");
        assert_eq!(config.port, 6667);
        assert_eq!(config.topic, "/irc/message");
        assert_eq!(config.reconnect_delay_secs, 5);
        assert!(!config.tls_enabled);
        assert_eq!(config.realname, "rustyKraken");
        assert_eq!(config.username, "rustykraken");
    }

    #[test]
    fn test_parse_privmsg() {
        let line = ":alice!alice@host PRIVMSG #channel :Hello, World!";
        let msg = parse_privmsg(line, "testbot").unwrap();

        assert_eq!(msg.nick, "alice");
        assert_eq!(msg.channel, "#channel");
        assert_eq!(msg.message, "Hello, World!");
        assert_eq!(msg.r#type, "PRIVMSG");
    }

    #[test]
    fn test_parse_privmsg_own_message() {
        let line = ":testbot!testbot@host PRIVMSG #channel :Echo";
        assert!(parse_privmsg(line, "testbot").is_none());
    }

    #[test]
    fn test_parse_privmsg_invalid() {
        assert!(parse_privmsg("invalid line", "bot").is_none());
    }

    #[test]
    fn test_create_message_ink() {
        let tentacle = IrcTentacle::new(
            "test".to_string(),
            IrcTentacleConfig {
                server: "irc.libera.chat".to_string(),
                port: 6667,
                nickname: "testbot".to_string(),
                channels: vec!["#test".to_string()],
                password: None,
                topic: "/irc/test".to_string(),
                reconnect_delay_secs: 5,
                tls_enabled: false,
                realname: "rustyKraken".to_string(),
                username: "rustykraken".to_string(),
            },
        );

        let message = IrcMessage {
            channel: "#test".to_string(),
            nick: "alice".to_string(),
            message: "Hello!".to_string(),
            r#type: "PRIVMSG".to_string(),
        };

        let ink = tentacle.create_message_ink(&message, "test");

        assert_eq!(ink.topic, "/irc/test");
        assert!(ink.body.contains("Hello!"));
        assert!(ink.body.contains("alice"));
    }

    #[test]
    fn test_tls_config() {
        let config: IrcTentacleConfig = serde_json::from_str(
            r#"{"nickname": "testbot", "channels": [], "tls_enabled": true, "port": 6697}"#,
        )
        .unwrap();

        assert!(config.tls_enabled);
        assert_eq!(config.port, 6697);
    }

    #[test]
    fn test_password_config() {
        let config: IrcTentacleConfig = serde_json::from_str(
            r#"{"nickname": "testbot", "channels": [], "password": "secret"}"#,
        )
        .unwrap();

        assert_eq!(config.password, Some("secret".to_string()));
    }

    #[test]
    fn test_multiple_channels() {
        let config: IrcTentacleConfig = serde_json::from_str(
            r#"{"nickname": "testbot", "channels": ["\u0023chan1", "\u0023chan2", "\u0023chan3"]}"#,
        )
        .unwrap();

        assert_eq!(config.channels.len(), 3);
    }

    #[test]
    fn test_create_irc_ink() {
        let config = IrcTentacleConfig {
            server: "irc.libera.chat".to_string(),
            port: 6667,
            nickname: "testbot".to_string(),
            channels: vec![],
            password: None,
            topic: "/irc/test".to_string(),
            reconnect_delay_secs: 5,
            tls_enabled: false,
            realname: "rustyKraken".to_string(),
            username: "rustykraken".to_string(),
        };

        let message = IrcMessage {
            channel: "#test".to_string(),
            nick: "bob".to_string(),
            message: "Test".to_string(),
            r#type: "PRIVMSG".to_string(),
        };

        let ink = create_irc_ink(&config, "test", &message);
        assert_eq!(ink.topic, "/irc/test");
        assert!(ink.body.contains("Test"));
    }

    #[test]
    fn test_custom_server() {
        let config: IrcTentacleConfig = serde_json::from_str(
            r#"{"server": "irc.oftc.net", "port": 6667, "nickname": "bot", "channels": []}"#,
        )
        .unwrap();

        assert_eq!(config.server, "irc.oftc.net");
    }
}
