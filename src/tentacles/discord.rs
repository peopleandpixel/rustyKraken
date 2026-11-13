use anyhow::Result;
use async_trait::async_trait;
use axum::{Router, extract::State, http::StatusCode, response::Json, routing::post};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_tungstenite::tungstenite::protocol::{CloseFrame, Message as WsMessage};
use tracing::{debug, info, warn};

use super::Tentacle;
use crate::appcontext::AppContext;
use crate::eventbus::EventBus;
use crate::model::Ink;

/// Configuration for the Discord tentacle
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiscordTentacleConfig {
    /// Connection type: "bot" (Gateway WebSocket) or "webhook" (HTTP endpoint)
    #[serde(default = "default_connection_type")]
    pub connection_type: String,

    // Bot mode settings
    /// Discord Bot token (required for connection_type: "bot")
    #[serde(default)]
    pub bot_token: Option<String>,
    /// Gateway intents as a bitmask (default: GUILD_MESSAGES | MESSAGE_CONTENT = 131072 | 16384 = 147456)
    #[serde(default = "default_intents")]
    pub intents: u64,
    /// Shard ID for sharded connections (default: 0)
    #[serde(default)]
    pub shard_id: u64,
    /// Total number of shards (default: 1)
    #[serde(default = "default_total_shards")]
    pub total_shards: u64,

    // Webhook mode settings
    /// Webhook host (for connection_type: "webhook")
    #[serde(default = "default_webhook_host")]
    pub webhook_host: String,
    /// Webhook port (for connection_type: "webhook")
    #[serde(default = "default_webhook_port")]
    pub webhook_port: u16,
    /// Webhook path (for connection_type: "webhook")
    #[serde(default = "default_webhook_path")]
    pub webhook_path: String,
    /// Optional: Secret token for webhook validation
    #[serde(default)]
    pub webhook_secret_token: Option<String>,

    // Topic resolution
    /// Topic detection mode: "channel" (channel-based), "content" (message prefix), or "all" (combined)
    #[serde(default = "default_topic_mode")]
    pub topic_mode: String,
    /// Fallback topic when no topic is detected
    #[serde(default = "default_fallback_topic")]
    pub fallback_topic: String,
    /// Optional: mapping of channel IDs to topics
    #[serde(default)]
    pub channel_topic_mapping: Option<HashMap<String, String>>,
    /// Optional: mapping of guild IDs to topics
    #[serde(default)]
    pub guild_topic_mapping: Option<HashMap<String, String>>,

    // Filtering
    /// Optional: only accept messages from specific channel IDs (empty = all)
    #[serde(default)]
    pub allowed_channels: Option<Vec<String>>,
    /// Optional: only accept messages from specific guild IDs (empty = all)
    #[serde(default)]
    pub allowed_guilds: Option<Vec<String>>,
    /// Optional: only accept messages from specific user IDs (empty = all)
    #[serde(default)]
    pub allowed_users: Option<Vec<String>>,

    // Reconnection
    /// Maximum number of reconnection attempts (0 = unlimited)
    #[serde(default)]
    pub max_retries: u32,
    /// Delay between reconnection attempts in seconds
    #[serde(default = "default_reconnect_delay")]
    pub reconnect_delay: u64,
}

fn default_connection_type() -> String {
    "bot".to_string()
}

fn default_intents() -> u64 {
    // GUILD_MESSAGES (1 << 17) | MESSAGE_CONTENT (1 << 15)
    (1 << 17) | (1 << 15)
}

fn default_total_shards() -> u64 {
    1
}

fn default_webhook_host() -> String {
    "127.0.0.1".to_string()
}

fn default_webhook_port() -> u16 {
    8444
}

fn default_webhook_path() -> String {
    "/discord".to_string()
}

fn default_topic_mode() -> String {
    "all".to_string()
}

fn default_fallback_topic() -> String {
    "/discord/default".to_string()
}

fn default_reconnect_delay() -> u64 {
    5
}

// ---------------------------------------------------------------------------
// Discord Gateway API Types
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DiscordGatewayPayload {
    #[serde(default)]
    pub op: u32,
    #[serde(default)]
    pub s: Option<u64>,
    #[serde(default)]
    pub t: Option<String>,
    #[serde(default)]
    pub d: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DiscordMessage {
    #[serde(default)]
    pub id: String,
    #[serde(default)]
    pub channel_id: String,
    #[serde(default)]
    pub guild_id: Option<String>,
    #[serde(default)]
    pub content: String,
    #[serde(default)]
    pub timestamp: String,
    #[serde(default)]
    pub author: Option<DiscordUser>,
    #[serde(default)]
    pub member: Option<DiscordMember>,
    #[serde(default)]
    pub attachments: Vec<DiscordAttachment>,
    #[serde(default)]
    pub embeds: Vec<DiscordEmbed>,
    #[serde(default)]
    pub mentions: Vec<DiscordUser>,
    #[serde(default)]
    pub mention_roles: Vec<String>,
    #[serde(default)]
    pub mention_channels: Vec<DiscordChannelMention>,
    #[serde(default)]
    pub reactions: Vec<DiscordReaction>,
    #[serde(default)]
    pub nonce: Option<String>,
    #[serde(default)]
    pub tts: bool,
    #[serde(default)]
    pub mention_everyone: bool,
    #[serde(default)]
    pub message_reference: Option<DiscordMessageReference>,
    #[serde(default)]
    pub flags: u32,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct DiscordUser {
    #[serde(default)]
    pub id: String,
    #[serde(default)]
    pub username: String,
    #[serde(default)]
    pub discriminator: String,
    #[serde(default)]
    pub global_name: Option<String>,
    #[serde(default)]
    pub avatar: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct DiscordMember {
    #[serde(default)]
    pub user: Option<DiscordUser>,
    #[serde(default)]
    pub nick: Option<String>,
    #[serde(default)]
    pub roles: Vec<String>,
    #[serde(default)]
    pub joined_at: String,
    #[serde(default)]
    pub premium_since: Option<String>,
    #[serde(default)]
    pub deaf: bool,
    #[serde(default)]
    pub mute: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct DiscordAttachment {
    #[serde(default)]
    pub id: String,
    #[serde(default)]
    pub filename: String,
    #[serde(default)]
    pub content_type: Option<String>,
    #[serde(default)]
    pub width: Option<u32>,
    #[serde(default)]
    pub height: Option<u32>,
    #[serde(default)]
    pub size: u64,
    #[serde(default)]
    pub url: String,
    #[serde(default)]
    pub proxy_url: String,
    #[serde(default)]
    pub ephemeral_url: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct DiscordEmbed {
    #[serde(default)]
    pub title: Option<String>,
    #[serde(default)]
    pub description: Option<String>,
    #[serde(default)]
    pub url: Option<String>,
    #[serde(default)]
    pub r#type: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct DiscordChannelMention {
    #[serde(default)]
    pub id: String,
    #[serde(default)]
    pub guild_id: String,
    #[serde(default)]
    pub name: String,
    #[serde(default)]
    pub r#type: u8,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct DiscordReaction {
    #[serde(default)]
    pub emoji: Option<DiscordEmoji>,
    #[serde(default)]
    pub count: u32,
    #[serde(default)]
    pub me: bool,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct DiscordEmoji {
    #[serde(default)]
    pub id: Option<String>,
    #[serde(default)]
    pub name: String,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct DiscordMessageReference {
    #[serde(default)]
    pub message_id: Option<String>,
    #[serde(default)]
    pub channel_id: String,
    #[serde(default)]
    pub guild_id: Option<String>,
}

// ---------------------------------------------------------------------------
// DiscordTentacle
// ---------------------------------------------------------------------------

pub struct DiscordTentacle {
    name: String,
    config: DiscordTentacleConfig,
    sequence: Arc<Mutex<Option<u64>>>,
    #[allow(dead_code)]
    client: Arc<reqwest::Client>,
}

impl DiscordTentacle {
    pub fn new(name: String, config: DiscordTentacleConfig, app_ctx: &AppContext) -> Self {
        Self {
            name,
            config,
            sequence: Arc::new(Mutex::new(None)),
            client: Arc::clone(app_ctx.http_client()),
        }
    }

    /// Determine the rustyKraken topic from a Discord message
    fn resolve_topic(&self, msg: &DiscordMessage) -> String {
        match self.config.topic_mode.as_str() {
            "channel" => self.resolve_topic_from_channel(msg),
            "content" => self.resolve_topic_from_content(msg),
            "all" => {
                // Try content first, then channel, then fallback
                if let Some(topic) = self.try_resolve_content(msg) {
                    return topic;
                }
                self.resolve_topic_from_channel(msg)
            }
            _ => self.config.fallback_topic.clone(),
        }
    }

    /// Resolve topic from channel/guild mappings
    fn resolve_topic_from_channel(&self, msg: &DiscordMessage) -> String {
        // Check channel mapping first
        if let Some(ref mapping) = self.config.channel_topic_mapping
            && let Some(topic) = mapping.get(&msg.channel_id)
        {
            return topic.clone();
        }

        // Check guild mapping
        if let Some(ref mapping) = self.config.guild_topic_mapping
            && let Some(ref guild_id) = msg.guild_id
            && let Some(topic) = mapping.get(guild_id)
        {
            return topic.clone();
        }

        // Fallback: use channel ID
        if let Some(ref guild_id) = msg.guild_id {
            format!("/discord/guild/{}/channel/{}", guild_id, msg.channel_id)
        } else {
            format!("/discord/dm/{}", msg.channel_id)
        }
    }

    /// Try to resolve topic from message content (prefix like /topic)
    fn try_resolve_content(&self, msg: &DiscordMessage) -> Option<String> {
        let trimmed = msg.content.trim();
        if trimmed.starts_with('/') {
            let topic_part = trimmed.split_whitespace().next()?.to_string();
            if topic_part.len() > 1 {
                return Some(topic_part);
            }
        }
        None
    }

    /// Resolve topic from message content
    fn resolve_topic_from_content(&self, msg: &DiscordMessage) -> String {
        if let Some(topic) = self.try_resolve_content(msg) {
            return topic;
        }
        self.config.fallback_topic.clone()
    }

    /// Check if a message is allowed based on filtering rules
    fn is_allowed(&self, msg: &DiscordMessage) -> bool {
        // Check allowed channels
        if let Some(ref allowed_channels) = self.config.allowed_channels
            && !allowed_channels.is_empty()
            && !allowed_channels.contains(&msg.channel_id)
        {
            return false;
        }

        // Check allowed guilds
        if let Some(ref allowed_guilds) = self.config.allowed_guilds
            && !allowed_guilds.is_empty()
        {
            match &msg.guild_id {
                Some(guild_id) => {
                    if !allowed_guilds.contains(guild_id) {
                        return false;
                    }
                }
                None => return false,
            }
        }

        // Check allowed users
        if let Some(ref allowed_users) = self.config.allowed_users
            && !allowed_users.is_empty()
            && let Some(ref author) = msg.author
            && !allowed_users.contains(&author.id)
        {
            return false;
        }

        true
    }

    /// Create an Ink message from a Discord message
    fn create_ink(&self, msg: &DiscordMessage) -> Result<Ink> {
        let topic = self.resolve_topic(msg);

        // Build metadata
        let mut metadata = serde_json::Map::new();

        // Channel info
        metadata.insert(
            "channel_id".into(),
            serde_json::Value::String(msg.channel_id.clone()),
        );

        // Guild info
        if let Some(ref guild_id) = msg.guild_id {
            metadata.insert(
                "guild_id".into(),
                serde_json::Value::String(guild_id.clone()),
            );
        }

        // Author info
        if let Some(ref author) = msg.author {
            metadata.insert(
                "author_id".into(),
                serde_json::Value::String(author.id.clone()),
            );
            metadata.insert(
                "author_username".into(),
                serde_json::Value::String(author.username.clone()),
            );
            if let Some(ref global_name) = author.global_name {
                metadata.insert(
                    "author_global_name".into(),
                    serde_json::Value::String(global_name.clone()),
                );
            }
        }

        // Member info (nickname, roles)
        if let Some(ref member) = msg.member {
            if let Some(ref nick) = member.nick {
                metadata.insert(
                    "member_nickname".into(),
                    serde_json::Value::String(nick.clone()),
                );
            }
            if !member.roles.is_empty() {
                metadata.insert(
                    "member_roles".into(),
                    serde_json::Value::Array(
                        member
                            .roles
                            .iter()
                            .map(|r| serde_json::Value::String(r.clone()))
                            .collect(),
                    ),
                );
            }
        }

        // Message info
        metadata.insert(
            "message_id".into(),
            serde_json::Value::String(msg.id.clone()),
        );
        metadata.insert(
            "timestamp".into(),
            serde_json::Value::String(msg.timestamp.clone()),
        );

        // Message type detection
        let msg_type = self.get_message_type(msg);
        metadata.insert("message_type".into(), serde_json::Value::String(msg_type));

        // Attachments
        if !msg.attachments.is_empty() {
            let attachments: Vec<serde_json::Value> = msg
                .attachments
                .iter()
                .map(|a| {
                    serde_json::json!({
                        "id": a.id,
                        "filename": a.filename,
                        "content_type": a.content_type,
                        "size": a.size,
                        "url": a.url
                    })
                })
                .collect();
            metadata.insert("attachments".into(), serde_json::Value::Array(attachments));
        }

        // Embeds
        if !msg.embeds.is_empty() {
            let embeds: Vec<serde_json::Value> = msg
                .embeds
                .iter()
                .map(|e| {
                    serde_json::json!({
                        "title": e.title,
                        "description": e.description,
                        "url": e.url,
                        "type": e.r#type
                    })
                })
                .collect();
            metadata.insert("embeds".into(), serde_json::Value::Array(embeds));
        }

        // Mentions
        if !msg.mentions.is_empty() {
            let mentions: Vec<serde_json::Value> = msg
                .mentions
                .iter()
                .map(|m| {
                    serde_json::json!({
                        "id": m.id,
                        "username": m.username
                    })
                })
                .collect();
            metadata.insert("mentions".into(), serde_json::Value::Array(mentions));
        }

        // Reference (reply)
        if let Some(ref reference) = msg.message_reference {
            metadata.insert(
                "reply_to_message_id".into(),
                serde_json::Value::String(reference.message_id.clone().unwrap_or_default()),
            );
        }

        let body = if !msg.content.is_empty() {
            msg.content.clone()
        } else if !msg.attachments.is_empty() {
            let filename = &msg.attachments[0].filename;
            format!(
                "[Attachment: {}]",
                if filename.is_empty() {
                    "unknown"
                } else {
                    filename
                }
            )
        } else if !msg.embeds.is_empty() {
            format!(
                "[Embed: {}]",
                msg.embeds[0]
                    .title
                    .clone()
                    .unwrap_or_else(|| "Untitled".to_string())
            )
        } else {
            "[Empty message]".to_string()
        };

        let ink = Ink::new(topic, "discord", &self.name, body)
            .with_metadata(serde_json::Value::Object(metadata));

        Ok(ink)
    }

    /// Determine the message type based on content
    fn get_message_type(&self, msg: &DiscordMessage) -> String {
        if !msg.content.is_empty() && !msg.attachments.is_empty() {
            "text_with_attachment".to_string()
        } else if !msg.content.is_empty() {
            "text".to_string()
        } else if !msg.attachments.is_empty() {
            let first = &msg.attachments[0];
            if let Some(ref ct) = first.content_type {
                if ct.starts_with("image/") {
                    "image".to_string()
                } else if ct.starts_with("video/") {
                    "video".to_string()
                } else if ct.starts_with("audio/") {
                    "audio".to_string()
                } else {
                    "attachment".to_string()
                }
            } else {
                "attachment".to_string()
            }
        } else if !msg.embeds.is_empty() {
            "embed".to_string()
        } else if msg.message_reference.is_some() {
            "reply".to_string()
        } else {
            "empty".to_string()
        }
    }

    /// Process a MESSAGE_CREATE event
    async fn process_message(&self, msg: &DiscordMessage, event_bus: &EventBus) {
        // Check if the message is allowed
        if !self.is_allowed(msg) {
            debug!(
                "Discord Tentacle '{}': Message from channel {} not allowed",
                self.name, msg.channel_id
            );
            return;
        }

        // Create and publish the Ink message
        match self.create_ink(msg) {
            Ok(ink) => {
                info!(
                    "Discord Tentacle '{}': Received message on topic: {} (channel: {}, author: {:?})",
                    self.name,
                    ink.topic,
                    msg.channel_id,
                    msg.author
                        .as_ref()
                        .map(|a| &a.username)
                        .unwrap_or(&"unknown".to_string())
                );
                event_bus.publish(ink);
            }
            Err(e) => {
                warn!(
                    "Discord Tentacle '{}': Failed to create ink from message: {}",
                    self.name, e
                );
            }
        }
    }

    // -----------------------------------------------------------------------
    // Bot Mode (Gateway WebSocket)
    // -----------------------------------------------------------------------

    /// Start the bot mode using the Discord Gateway
    async fn start_bot(&self, event_bus: Arc<EventBus>) -> Result<()> {
        info!(
            "Discord Tentacle '{}': Starting in bot mode (shard {}/{})",
            self.name, self.config.shard_id, self.config.total_shards
        );

        let bot_token = self
            .config
            .bot_token
            .clone()
            .ok_or_else(|| anyhow::anyhow!("Bot token is required for bot mode"))?;

        let mut retries = 0u32;

        loop {
            // Get the gateway URL
            let gateway_url = match self.get_gateway_url(&bot_token).await {
                Ok(url) => url,
                Err(e) => {
                    warn!(
                        "Discord Tentacle '{}': Failed to get gateway URL: {}",
                        self.name, e
                    );
                    self.handle_retry(&mut retries).await?;
                    continue;
                }
            };

            info!(
                "Discord Tentacle '{}': Connecting to gateway: {}",
                self.name, gateway_url
            );

            match self
                .connect_gateway(&gateway_url, &bot_token, &Arc::clone(&event_bus))
                .await
            {
                Ok(()) => {
                    // Connection closed cleanly or by remote
                    info!(
                        "Discord Tentacle '{}': Gateway connection closed",
                        self.name
                    );
                }
                Err(e) => {
                    warn!(
                        "Discord Tentacle '{}': Gateway connection error: {}",
                        self.name, e
                    );
                }
            }

            // Handle reconnection
            self.handle_retry(&mut retries).await?;
        }
    }

    async fn handle_retry(&self, retries: &mut u32) -> Result<()> {
        *retries += 1;

        if self.config.max_retries > 0 && *retries >= self.config.max_retries {
            anyhow::bail!(
                "Discord Tentacle '{}': Max retries ({}) reached",
                self.name,
                self.config.max_retries
            );
        }

        info!(
            "Discord Tentacle '{}': Reconnecting in {} seconds (attempt {})...",
            self.name, self.config.reconnect_delay, retries
        );
        tokio::time::sleep(std::time::Duration::from_secs(self.config.reconnect_delay)).await;
        Ok(())
    }

    /// Fetch the Gateway URL from Discord API
    async fn get_gateway_url(&self, bot_token: &str) -> Result<String> {
        let client = reqwest::Client::new();
        let resp = client
            .get("https://discord.com/api/v10/gateway/bot")
            .header("Authorization", format!("Bot {}", bot_token))
            .send()
            .await?;

        if !resp.status().is_success() {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            anyhow::bail!("Discord API error {}: {}", status, body);
        }

        let json: serde_json::Value = resp.json().await?;
        let url = json["url"]
            .as_str()
            .ok_or_else(|| anyhow::anyhow!("No URL in gateway response"))?
            .to_string();

        Ok(format!("{}?v=10&encoding=json", url))
    }

    /// Connect to the Discord Gateway and handle the event loop
    async fn connect_gateway(
        &self,
        gateway_url: &str,
        bot_token: &str,
        event_bus: &Arc<EventBus>,
    ) -> Result<()> {
        // Connect WebSocket
        let (ws_stream, _) = tokio_tungstenite::connect_async(gateway_url).await?;
        info!(
            "Discord Tentacle '{}': WebSocket connected to gateway",
            self.name
        );

        let (mut write, mut read) = ws_stream.split();

        // Wait for HELLO event (opcode 10) to get the heartbeat interval
        let heartbeat_interval = {
            let msg = read.next().await.ok_or_else(|| {
                anyhow::anyhow!("Discord Tentacle '{}': No HELLO event received", self.name)
            })??;

            match msg {
                WsMessage::Text(text) => {
                    let payload: DiscordGatewayPayload =
                        serde_json::from_str(&text).map_err(|e| {
                            anyhow::anyhow!(
                                "Discord Tentacle '{}': Failed to parse HELLO: {}",
                                self.name,
                                e
                            )
                        })?;

                    if payload.op != 10 {
                        anyhow::bail!(
                            "Discord Tentacle '{}': Expected HELLO (op 10), got op {}",
                            self.name,
                            payload.op
                        );
                    }

                    let interval_ms = payload
                        .d
                        .as_ref()
                        .and_then(|d| d.get("heartbeat_interval"))
                        .and_then(|v| v.as_u64())
                        .ok_or_else(|| {
                            anyhow::anyhow!(
                                "Discord Tentacle '{}': No heartbeat_interval in HELLO",
                                self.name
                            )
                        })?;

                    std::time::Duration::from_millis(interval_ms)
                }
                _ => {
                    anyhow::bail!(
                        "Discord Tentacle '{}': Expected text HELLO event",
                        self.name
                    )
                }
            }
        };

        info!(
            "Discord Tentacle '{}': Heartbeat interval: {:?}",
            self.name, heartbeat_interval
        );

        // Send IDENTIFY (opcode 2)
        let identify = serde_json::json!({
            "op": 2,
            "d": {
                "token": bot_token,
                "intents": self.config.intents,
                "properties": {
                    "$os": "linux",
                    "$browser": "rustyKraken",
                    "$device": "rustyKraken"
                },
                "shard": [self.config.shard_id, self.config.total_shards]
            }
        });

        write
            .send(WsMessage::Text(identify.to_string().into()))
            .await?;
        info!("Discord Tentacle '{}': Sent IDENTIFY", self.name);

        // Main event loop with heartbeat using tokio::select!
        let mut heartbeat_interval_timer = tokio::time::interval(heartbeat_interval);
        heartbeat_interval_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        // Pin the read stream for use in select!
        tokio::pin!(read);

        loop {
            tokio::select! {
                // Send heartbeat
                _ = heartbeat_interval_timer.tick() => {
                    let seq = {
                        let seq_guard = self.sequence.lock().await;
                        *seq_guard
                    };

                    let heartbeat = serde_json::json!({
                        "op": 1,
                        "d": seq
                    });

                    if let Err(e) = write.send(WsMessage::Text(heartbeat.to_string().into())).await {
                        warn!(
                            "Discord Tentacle '{}': Failed to send heartbeat: {}",
                            self.name, e
                        );
                        break;
                    }
                }

                // Read message
                msg = read.next() => {
                    let msg = match msg {
                        Some(Ok(m)) => m,
                        Some(Err(e)) => {
                            warn!(
                                "Discord Tentacle '{}': WebSocket error: {}",
                                self.name, e
                            );
                            break;
                        }
                        None => {
                            info!(
                                "Discord Tentacle '{}': WebSocket connection closed",
                                self.name
                            );
                            break;
                        }
                    };

                    match msg {
                        WsMessage::Text(text) => {
                            let payload: DiscordGatewayPayload = match serde_json::from_str(&text) {
                                Ok(p) => p,
                                Err(e) => {
                                    warn!(
                                        "Discord Tentacle '{}': Failed to parse gateway message: {}",
                                        self.name, e
                                    );
                                    continue;
                                }
                            };

                            // Update sequence number
                            if let Some(s) = payload.s {
                                let mut seq = self.sequence.lock().await;
                                *seq = Some(s);
                            }

                            match payload.op {
                                // DISPATCH event
                                0 => {
                                    if let Some(ref event_type) = payload.t
                                        && event_type == "MESSAGE_CREATE"
                                            && let Some(ref data) = payload.d {
                                                let msg: DiscordMessage =
                                                    match serde_json::from_value(
                                                        data.clone(),
                                                    ) {
                                                        Ok(m) => m,
                                                        Err(e) => {
                                                            warn!(
                                                                "Discord Tentacle '{}': Failed to parse message: {}",
                                                                self.name, e
                                                            );
                                                            continue;
                                                        }
                                                    };

                                                // Skip own messages
                                                if let Some(ref author) = msg.author
                                                    && author.username == "rustyKraken" {
                                                        continue;
                                                    }

                                                self.process_message(&msg, event_bus)
                                                    .await;
                                            }
                                }
                                // HEARTBEAT ACK
                                11 => {
                                    debug!(
                                        "Discord Tentacle '{}': Heartbeat ACK received",
                                        self.name
                                    );
                                }
                                // RECONNECT
                                7 => {
                                    info!(
                                        "Discord Tentacle '{}': Discord requested reconnect",
                                        self.name
                                    );
                                    break;
                                }
                                // INVALID SESSION
                                9 => {
                                    warn!(
                                        "Discord Tentacle '{}': Invalid session",
                                        self.name
                                    );
                                    break;
                                }
                                // HELLO (shouldn't happen after initial HELLO, but handle it)
                                10 => {
                                    debug!(
                                        "Discord Tentacle '{}': Received additional HELLO",
                                        self.name
                                    );
                                }
                                _ => {
                                    debug!(
                                        "Discord Tentacle '{}': Unknown opcode: {}",
                                        self.name, payload.op
                                    );
                                }
                            }
                        }
                        WsMessage::Close(frame) => {
                            if let Some(CloseFrame { code, ref reason }) = frame {
                                warn!(
                                    "Discord Tentacle '{}': Closed with code {}: {}",
                                    self.name, code, reason
                                );
                            }
                            break;
                        }
                        WsMessage::Ping(payload) => {
                            debug!("Discord Tentacle '{}': Received ping", self.name);
                            let _ = write.send(WsMessage::Pong(payload)).await;
                        }
                        _ => {}
                    }
                }
            }
        }

        Ok(())
    }

    // -----------------------------------------------------------------------
    // Webhook Mode (HTTP endpoint)
    // -----------------------------------------------------------------------

    /// Start the webhook mode using an HTTP server
    async fn start_webhook(&self, event_bus: Arc<EventBus>) -> Result<()> {
        info!(
            "Discord Tentacle '{}': Starting in webhook mode on {}:{}",
            self.name, self.config.webhook_host, self.config.webhook_port
        );

        let path = if self.config.webhook_path.is_empty() {
            "/discord".to_string()
        } else {
            self.config.webhook_path.clone()
        };

        let tentacle_name = self.name.clone();
        let event_bus_clone = Arc::clone(&event_bus);
        let secret_token = self.config.webhook_secret_token.clone();

        let app = Router::new()
            .route(&path, post(webhook_handler))
            .with_state((tentacle_name, event_bus_clone, secret_token));

        let addr = format!("{}:{}", self.config.webhook_host, self.config.webhook_port);
        let listener = tokio::net::TcpListener::bind(&addr).await?;

        info!("Discord Tentacle '{}' listening on {}", self.name, addr);
        axum::serve(listener, app).await?;

        Ok(())
    }
}

#[async_trait]
impl Tentacle for DiscordTentacle {
    fn name(&self) -> &str {
        &self.name
    }

    fn r#type(&self) -> &str {
        "discord"
    }

    async fn start(&self, event_bus: Arc<EventBus>) -> Result<()> {
        match self.config.connection_type.as_str() {
            "bot" => self.start_bot(event_bus).await,
            "webhook" => self.start_webhook(event_bus).await,
            _ => self.start_bot(event_bus).await,
        }
    }
}

/// Webhook handler for receiving Discord-like payloads via HTTP
async fn webhook_handler(
    State((tentacle_name, event_bus, secret_token)): State<(String, Arc<EventBus>, Option<String>)>,
    headers: axum::http::HeaderMap,
    Json(payload): Json<serde_json::Value>,
) -> StatusCode {
    // Validate secret token if configured
    if let Some(ref secret) = secret_token {
        let auth_header = headers
            .get("Authorization")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");

        if auth_header != secret {
            warn!("Discord Tentacle '{}': Invalid secret token", tentacle_name);
            return StatusCode::UNAUTHORIZED;
        }
    }

    // Try to parse as a Discord message
    let msg: DiscordMessage = match serde_json::from_value(payload.clone()) {
        Ok(m) => m,
        Err(_) => {
            // Try to extract from a nested structure (e.g., {"data": {...}})
            if let Some(data) = payload.get("data") {
                match serde_json::from_value(data.clone()) {
                    Ok(m) => m,
                    Err(_) => {
                        warn!(
                            "Discord Tentacle '{}': Failed to parse webhook payload",
                            tentacle_name
                        );
                        return StatusCode::BAD_REQUEST;
                    }
                }
            } else {
                warn!(
                    "Discord Tentacle '{}': Failed to parse webhook payload",
                    tentacle_name
                );
                return StatusCode::BAD_REQUEST;
            }
        }
    };

    // Create a temporary tentacle instance for processing
    let config = DiscordTentacleConfig {
        connection_type: "webhook".to_string(),
        bot_token: None,
        intents: default_intents(),
        shard_id: 0,
        total_shards: 1,
        webhook_host: "127.0.0.1".to_string(),
        webhook_port: 8444,
        webhook_path: "/discord".to_string(),
        webhook_secret_token: None,
        topic_mode: "all".to_string(),
        fallback_topic: "/discord/default".to_string(),
        channel_topic_mapping: None,
        guild_topic_mapping: None,
        allowed_channels: None,
        allowed_guilds: None,
        allowed_users: None,
        max_retries: 0,
        reconnect_delay: 5,
    };

    let app_ctx = crate::appcontext::AppContext::new();
    let tentacle = DiscordTentacle::new(tentacle_name.clone(), config, &app_ctx);
    tentacle.process_message(&msg, &event_bus).await;

    StatusCode::OK
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_message(channel_id: &str, content: &str) -> DiscordMessage {
        DiscordMessage {
            id: "123456789".to_string(),
            channel_id: channel_id.to_string(),
            guild_id: Some("987654321".to_string()),
            content: content.to_string(),
            timestamp: "2024-01-01T00:00:00.000000+00:00".to_string(),
            author: Some(DiscordUser {
                id: "user123".to_string(),
                username: "testuser".to_string(),
                discriminator: "0001".to_string(),
                global_name: Some("Test User".to_string()),
                avatar: None,
            }),
            member: Some(DiscordMember {
                user: None,
                nick: Some("TestNick".to_string()),
                roles: vec!["role1".to_string(), "role2".to_string()],
                joined_at: "2024-01-01T00:00:00.000000+00:00".to_string(),
                premium_since: None,
                deaf: false,
                mute: false,
            }),
            attachments: vec![],
            embeds: vec![],
            mentions: vec![],
            mention_roles: vec![],
            mention_channels: vec![],
            reactions: vec![],
            nonce: None,
            tts: false,
            mention_everyone: false,
            message_reference: None,
            flags: 0,
        }
    }

    fn make_config() -> DiscordTentacleConfig {
        DiscordTentacleConfig {
            connection_type: "bot".to_string(),
            bot_token: Some("fake_token".to_string()),
            intents: default_intents(),
            shard_id: 0,
            total_shards: 1,
            webhook_host: "127.0.0.1".to_string(),
            webhook_port: 8444,
            webhook_path: "/discord".to_string(),
            webhook_secret_token: None,
            topic_mode: "all".to_string(),
            fallback_topic: "/discord/default".to_string(),
            channel_topic_mapping: None,
            guild_topic_mapping: None,
            allowed_channels: None,
            allowed_guilds: None,
            allowed_users: None,
            max_retries: 0,
            reconnect_delay: 5,
        }
    }

    #[test]
    fn test_resolve_topic_from_content() {
        let config = make_config();
        let tentacle = DiscordTentacle::new(
            "test".to_string(),
            config,
            &crate::appcontext::AppContext::new(),
        );

        let msg = make_message("ch1", "/alerts Hello world");
        assert_eq!(tentacle.resolve_topic(&msg), "/alerts");
    }

    #[test]
    fn test_resolve_topic_from_channel_mapping() {
        let mut config = make_config();
        config.topic_mode = "channel".to_string();
        let mut mapping = HashMap::new();
        mapping.insert("ch1".to_string(), "/mapped/topic".to_string());
        config.channel_topic_mapping = Some(mapping);

        let tentacle = DiscordTentacle::new(
            "test".to_string(),
            config,
            &crate::appcontext::AppContext::new(),
        );

        let msg = make_message("ch1", "Hello");
        assert_eq!(tentacle.resolve_topic(&msg), "/mapped/topic");
    }

    #[test]
    fn test_resolve_topic_from_guild_mapping() {
        let mut config = make_config();
        config.topic_mode = "channel".to_string();
        let mut mapping = HashMap::new();
        mapping.insert("987654321".to_string(), "/guild/topic".to_string());
        config.guild_topic_mapping = Some(mapping);

        let tentacle = DiscordTentacle::new(
            "test".to_string(),
            config,
            &crate::appcontext::AppContext::new(),
        );

        let msg = make_message("ch1", "Hello");
        assert_eq!(tentacle.resolve_topic(&msg), "/guild/topic");
    }

    #[test]
    fn test_resolve_topic_fallback() {
        let mut config = make_config();
        config.topic_mode = "channel".to_string();

        let tentacle = DiscordTentacle::new(
            "test".to_string(),
            config,
            &crate::appcontext::AppContext::new(),
        );

        let msg = make_message("ch1", "Hello");
        assert_eq!(
            tentacle.resolve_topic(&msg),
            "/discord/guild/987654321/channel/ch1"
        );
    }

    #[test]
    fn test_resolve_topic_dm_fallback() {
        let mut config = make_config();
        config.topic_mode = "channel".to_string();

        let tentacle = DiscordTentacle::new(
            "test".to_string(),
            config,
            &crate::appcontext::AppContext::new(),
        );

        let mut msg = make_message("ch1", "Hello");
        msg.guild_id = None;
        assert_eq!(tentacle.resolve_topic(&msg), "/discord/dm/ch1");
    }

    #[test]
    fn test_is_allowed_channels() {
        let mut config = make_config();
        config.allowed_channels = Some(vec!["ch1".to_string()]);

        let tentacle = DiscordTentacle::new(
            "test".to_string(),
            config,
            &crate::appcontext::AppContext::new(),
        );

        let msg_allowed = make_message("ch1", "Hello");
        let msg_not_allowed = make_message("ch2", "Hello");

        assert!(tentacle.is_allowed(&msg_allowed));
        assert!(!tentacle.is_allowed(&msg_not_allowed));
    }

    #[test]
    fn test_is_allowed_guilds() {
        let mut config = make_config();
        config.allowed_guilds = Some(vec!["987654321".to_string()]);

        let tentacle = DiscordTentacle::new(
            "test".to_string(),
            config,
            &crate::appcontext::AppContext::new(),
        );

        let msg = make_message("ch1", "Hello");
        assert!(tentacle.is_allowed(&msg));
    }

    #[test]
    fn test_is_allowed_users() {
        let mut config = make_config();
        config.allowed_users = Some(vec!["user123".to_string()]);

        let tentacle = DiscordTentacle::new(
            "test".to_string(),
            config,
            &crate::appcontext::AppContext::new(),
        );

        let msg = make_message("ch1", "Hello");
        assert!(tentacle.is_allowed(&msg));
    }

    #[test]
    fn test_create_ink() {
        let config = make_config();
        let tentacle = DiscordTentacle::new(
            "test".to_string(),
            config,
            &crate::appcontext::AppContext::new(),
        );

        let msg = make_message("ch1", "/test Hello world");
        let ink = tentacle.create_ink(&msg).unwrap();

        assert_eq!(ink.topic, "/test");
        assert_eq!(ink.source.r#type, "discord");
        assert_eq!(ink.source.name, "test");
        assert_eq!(ink.body, "/test Hello world");
        assert!(ink.metadata.is_some());
    }

    #[test]
    fn test_get_message_type_text() {
        let config = make_config();
        let tentacle = DiscordTentacle::new(
            "test".to_string(),
            config,
            &crate::appcontext::AppContext::new(),
        );

        let msg = make_message("ch1", "Hello");
        assert_eq!(tentacle.get_message_type(&msg), "text");
    }

    #[test]
    fn test_get_message_type_attachment() {
        let config = make_config();
        let tentacle = DiscordTentacle::new(
            "test".to_string(),
            config,
            &crate::appcontext::AppContext::new(),
        );

        let mut msg = make_message("ch1", "");
        msg.attachments = vec![DiscordAttachment {
            id: "att1".to_string(),
            filename: "image.png".to_string(),
            content_type: Some("image/png".to_string()),
            width: Some(800),
            height: Some(600),
            size: 1024,
            url: "https://example.com/image.png".to_string(),
            proxy_url: "https://cdn.example.com/image.png".to_string(),
            ephemeral_url: None,
        }];
        assert_eq!(tentacle.get_message_type(&msg), "image");
    }

    #[test]
    fn test_get_message_type_embed() {
        let config = make_config();
        let tentacle = DiscordTentacle::new(
            "test".to_string(),
            config,
            &crate::appcontext::AppContext::new(),
        );

        let mut msg = make_message("ch1", "");
        msg.embeds = vec![DiscordEmbed {
            title: Some("Test Embed".to_string()),
            description: Some("Description".to_string()),
            url: Some("https://example.com".to_string()),
            r#type: Some("rich".to_string()),
        }];
        assert_eq!(tentacle.get_message_type(&msg), "embed");
    }

    #[test]
    fn test_all_mode_topic_priority() {
        let mut config = make_config();
        config.topic_mode = "all".to_string();
        let mut mapping = HashMap::new();
        mapping.insert("ch1".to_string(), "/channel/topic".to_string());
        config.channel_topic_mapping = Some(mapping);

        let tentacle = DiscordTentacle::new(
            "test".to_string(),
            config,
            &crate::appcontext::AppContext::new(),
        );

        // Content prefix should take priority in "all" mode
        let msg = make_message("ch1", "/content Hello");
        assert_eq!(tentacle.resolve_topic(&msg), "/content");

        // Without content prefix, should fall back to channel mapping
        let msg2 = make_message("ch1", "Hello");
        assert_eq!(tentacle.resolve_topic(&msg2), "/channel/topic");
    }
}
