use anyhow::Result;
use async_trait::async_trait;
use axum::{Router, extract::State, http::StatusCode, response::Json, routing::post};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use tracing::{debug, info, warn};

use super::Tentacle;
use crate::eventbus::EventBus;
use crate::model::Ink;

/// Konfiguration für die Telegram-Verbindung
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TelegramTentacleConfig {
    /// Telegram Bot Token (vom BotFather erhalten)
    pub bot_token: String,
    /// Verbindungsart: "webhook" oder "polling" (Standard: "polling")
    #[serde(default = "default_connection_type")]
    pub connection_type: String,
    /// Webhook-Host (nur bei connection_type: "webhook")
    #[serde(default = "default_webhook_host")]
    pub webhook_host: String,
    /// Webhook-Port (nur bei connection_type: "webhook")
    #[serde(default = "default_webhook_port")]
    pub webhook_port: u16,
    /// Webhook-Pfad (nur bei connection_type: "webhook", Standard: "/telegram")
    #[serde(default = "default_webhook_path")]
    pub webhook_path: String,
    /// Optional: Secret Token für Webhook-Validierung
    #[serde(default)]
    pub webhook_secret_token: Option<String>,
    /// Polling-Intervall in Sekunden (nur bei connection_type: "polling", Standard: 3)
    #[serde(default = "default_polling_interval")]
    pub polling_interval: u64,
    /// Timeout für Polling-Requests in Sekunden (Standard: 25)
    #[serde(default = "default_polling_timeout")]
    pub polling_timeout: u64,
    /// Topic-Erkennungsmodus: "command" (Befehle wie /kraken_topic), "content" (Topic aus Betreff/Inhalt), "chat" (pro Chat festes Topic) oder "all" (alle Modi kombinieren)
    #[serde(default = "default_topic_mode")]
    pub topic_mode: String,
    /// Fallback-Topic, wenn kein Topic erkannt wird
    #[serde(default = "default_fallback_topic")]
    pub fallback_topic: String,
    /// Optional: Mapping von Chat-IDs auf Topics (nur bei topic_mode: "chat")
    #[serde(default)]
    pub chat_topic_mapping: Option<HashMap<String, String>>,
    /// Optional: Mapping von Telegram-Benutzernamen auf Topics
    #[serde(default)]
    pub username_topic_mapping: Option<HashMap<String, String>>,
    /// Optional: Nur Nachrichten von bestimmten Chats akzeptieren (leer = alle)
    #[serde(default)]
    pub allowed_chats: Option<Vec<i64>>,
    /// Optional: Nur Nachrichten von bestimmten Benutzern akzeptieren (leer = alle)
    #[serde(default)]
    pub allowed_users: Option<Vec<i64>>,
    /// Optional: Webhook-Server-URL (für die Registrierung beim Telegram-Server)
    #[serde(default)]
    pub webhook_url: Option<String>,
    /// Maximale Anzahl von Updates pro Polling-Request (Standard: 100)
    #[serde(default = "default_max_retries")]
    pub max_retries: u32,
    /// Verzögerung bei Reconnection in Sekunden (Standard: 5)
    #[serde(default = "default_reconnect_delay")]
    pub reconnect_delay: u64,
}

fn default_connection_type() -> String {
    "polling".to_string()
}

fn default_webhook_host() -> String {
    "127.0.0.1".to_string()
}

fn default_webhook_port() -> u16 {
    8443
}

fn default_webhook_path() -> String {
    "/telegram".to_string()
}

fn default_polling_interval() -> u64 {
    3
}

fn default_polling_timeout() -> u64 {
    25
}

fn default_topic_mode() -> String {
    "all".to_string()
}

fn default_fallback_topic() -> String {
    "/telegram/default".to_string()
}

fn default_max_retries() -> u32 {
    5
}

fn default_reconnect_delay() -> u64 {
    5
}

// ---------------------------------------------------------------------------
// Telegram Bot API Types (vereinfachte Darstellung der benötigten Felder)
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TelegramUpdate {
    pub update_id: i64,
    #[serde(default)]
    pub message: Option<TelegramMessage>,
    #[serde(default)]
    pub edited_message: Option<TelegramMessage>,
    #[serde(default)]
    pub channel_post: Option<TelegramMessage>,
    #[serde(default)]
    pub edited_channel_post: Option<TelegramMessage>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TelegramMessage {
    pub message_id: i64,
    #[serde(default)]
    pub date: i64,
    #[serde(default)]
    pub chat: TelegramChat,
    #[serde(default)]
    pub from: Option<TelegramUser>,
    #[serde(default)]
    pub text: Option<String>,
    #[serde(default)]
    pub entities: Option<Vec<TelegramEntity>>,
    #[serde(default)]
    pub caption: Option<String>,
    #[serde(default)]
    pub photo: Option<Vec<TelegramPhotoSize>>,
    #[serde(default)]
    pub document: Option<TelegramDocument>,
    #[serde(default)]
    pub voice: Option<TelegramVoice>,
    #[serde(default)]
    pub audio: Option<TelegramAudio>,
    #[serde(default)]
    pub video: Option<TelegramVideo>,
    #[serde(default)]
    pub forward_from: Option<TelegramUser>,
    #[serde(default)]
    pub forward_from_chat: Option<TelegramChat>,
    #[serde(default)]
    pub reply_to_message: Option<Box<TelegramMessage>>,
    #[serde(default)]
    pub via_bot: Option<TelegramUser>,
    #[serde(default)]
    pub location: Option<TelegramLocation>,
    #[serde(default)]
    pub contact: Option<TelegramContact>,
}

#[derive(Debug, Clone, Deserialize, Serialize, Default)]
pub struct TelegramChat {
    pub id: i64,
    pub r#type: String, // "private", "group", "supergroup", "channel"
    #[serde(default)]
    pub title: Option<String>,
    #[serde(default)]
    pub username: Option<String>,
    #[serde(default)]
    pub first_name: Option<String>,
    #[serde(default)]
    pub last_name: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TelegramUser {
    pub id: i64,
    #[serde(default)]
    pub is_bot: bool,
    #[serde(default)]
    pub first_name: Option<String>,
    #[serde(default)]
    pub last_name: Option<String>,
    #[serde(default)]
    pub username: Option<String>,
    #[serde(default)]
    pub language_code: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TelegramEntity {
    #[serde(default)]
    pub r#type: String, // "mention", "hashtag", "bot_command", "url", etc.
    #[serde(default)]
    pub offset: i64,
    #[serde(default)]
    pub length: i64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TelegramPhotoSize {
    #[serde(default)]
    pub file_id: String,
    #[serde(default)]
    pub file_unique_id: String,
    #[serde(default)]
    pub width: i32,
    #[serde(default)]
    pub height: i32,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TelegramDocument {
    #[serde(default)]
    pub file_name: Option<String>,
    #[serde(default)]
    pub mime_type: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TelegramVoice {
    #[serde(default)]
    pub mime_type: Option<String>,
    #[serde(default)]
    pub duration: Option<i32>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TelegramAudio {
    #[serde(default)]
    pub file_name: Option<String>,
    #[serde(default)]
    pub mime_type: Option<String>,
    #[serde(default)]
    pub duration: Option<i32>,
    #[serde(default)]
    pub title: Option<String>,
    #[serde(default)]
    pub performer: Option<String>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TelegramVideo {
    #[serde(default)]
    pub mime_type: Option<String>,
    #[serde(default)]
    pub duration: Option<i32>,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TelegramLocation {
    #[serde(default)]
    pub longitude: f64,
    #[serde(default)]
    pub latitude: f64,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TelegramContact {
    #[serde(default)]
    pub phone_number: String,
    #[serde(default)]
    pub first_name: Option<String>,
    #[serde(default)]
    pub last_name: Option<String>,
}

// Telegram Bot API Antwort-Struktur
pub struct TelegramApiResult<T> {
    pub ok: bool,
    pub result: Option<T>,
    pub description: Option<String>,
}

impl<'de, T: serde::de::DeserializeOwned> serde::de::Deserialize<'de> for TelegramApiResult<T> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct Helper {
            ok: bool,
            #[serde(default)]
            result: Option<serde_json::Value>,
            #[serde(default)]
            description: Option<String>,
        }
        let helper = Helper::deserialize(deserializer)?;
        let result = helper
            .result
            .map(|v| serde_json::from_value(v.clone()).map_err(serde::de::Error::custom))
            .transpose()?;
        Ok(TelegramApiResult {
            ok: helper.ok,
            result,
            description: helper.description,
        })
    }
}

// ---------------------------------------------------------------------------
// TelegramTentacle
// ---------------------------------------------------------------------------

pub struct TelegramTentacle {
    name: String,
    config: TelegramTentacleConfig,
}

impl TelegramTentacle {
    pub fn new(name: String, config: TelegramTentacleConfig) -> Self {
        Self { name, config }
    }

    /// Bestimmt den rustyKraken-Topic aus einer Telegram-Nachricht
    fn resolve_topic(&self, msg: &TelegramMessage) -> String {
        match self.config.topic_mode.as_str() {
            "command" => self.resolve_topic_from_command(msg),
            "content" => self.resolve_topic_from_content(msg),
            "chat" => self.resolve_topic_from_chat(msg),
            "all" => {
                // Versuche zuerst Command, dann Content, dann Chat, dann Fallback
                if let Some(topic) = self.try_resolve_content(msg) {
                    return topic;
                }
                if let Some(topic) = self.try_resolve_chat(msg) {
                    return topic;
                }
                self.config.fallback_topic.clone()
            }
            _ => self.config.fallback_topic.clone(),
        }
    }

    /// Topic aus Bot-Befehl extrahieren (z.B. "/kraken_topic Nachricht")
    fn resolve_topic_from_command(&self, msg: &TelegramMessage) -> String {
        if let Some(ref text) = msg.text
            && let Some(topic) = self.extract_command_topic(text)
        {
            return topic;
        }
        self.config.fallback_topic.clone()
    }

    /// Versuche Topic aus Bot-Befehl zu extrahieren
    fn extract_command_topic(&self, text: &str) -> Option<String> {
        // Befehle beginnen mit /
        if !text.starts_with('/') {
            return None;
        }

        // Extrahiere den Befehl (bis zum ersten Leerzeichen oder Ende)
        let command_part = text.split_whitespace().next()?;
        let command = command_part.trim_start_matches('/');

        if command.is_empty() {
            return None;
        }

        // Befehl als Topic verwenden (mit führendem /)
        Some(format!("/{}", command))
    }

    /// Topic aus Nachrichteninhalt extrahieren
    fn resolve_topic_from_content(&self, msg: &TelegramMessage) -> String {
        if let Some(topic) = self.try_resolve_content(msg) {
            return topic;
        }
        self.config.fallback_topic.clone()
    }

    /// Versuche Topic aus Nachrichteninhalt zu extrahieren
    fn try_resolve_content(&self, msg: &TelegramMessage) -> Option<String> {
        // Prüfe, ob der Text mit "/" beginnt (Topic-Angabe)
        if let Some(ref text) = msg.text {
            let trimmed = text.trim();
            if trimmed.starts_with('/') {
                // Extrahiere das Topic (bis zum ersten Leerzeichen)
                let topic_part = trimmed.split_whitespace().next()?.to_string();
                if topic_part.len() > 1 {
                    return Some(topic_part);
                }
            }
        }
        None
    }

    /// Topic aus Chat-Informationen extrahieren
    fn resolve_topic_from_chat(&self, msg: &TelegramMessage) -> String {
        if let Some(topic) = self.try_resolve_chat(msg) {
            return topic;
        }
        self.config.fallback_topic.clone()
    }

    /// Versuche Topic aus Chat-Informationen zu extrahieren
    fn try_resolve_chat(&self, msg: &TelegramMessage) -> Option<String> {
        // Prüfe Chat-ID Mapping
        if let Some(ref mapping) = self.config.chat_topic_mapping {
            let chat_id = msg.chat.id.to_string();
            if let Some(topic) = mapping.get(&chat_id) {
                return Some(topic.clone());
            }
        }

        // Prüfe Username Mapping
        if let Some(ref mapping) = self.config.username_topic_mapping
            && let Some(ref from) = msg.from
            && let Some(ref username) = from.username
            && let Some(topic) = mapping.get(username)
        {
            return Some(topic.clone());
        }

        // Fallback: Chat-Typ und -ID als Topic
        Some(format!("/telegram/chat/{}", msg.chat.id))
    }

    /// Erstellt eine Ink-Nachricht aus einer Telegram-Nachricht
    fn create_ink(&self, msg: &TelegramMessage) -> Result<Ink> {
        let topic = self.resolve_topic(msg);

        // Bestimme den Nachrichtentext
        let body = if let Some(ref text) = msg.text {
            text.clone()
        } else if let Some(ref caption) = msg.caption {
            caption.clone()
        } else {
            self.get_message_type_description(msg)
        };

        // Metadaten erstellen
        let mut metadata = serde_json::Map::new();

        // Chat-Informationen
        metadata.insert(
            "chat_id".into(),
            serde_json::Value::Number(msg.chat.id.into()),
        );
        metadata.insert(
            "chat_type".into(),
            serde_json::Value::String(msg.chat.r#type.clone()),
        );
        if let Some(ref title) = msg.chat.title {
            metadata.insert(
                "chat_title".into(),
                serde_json::Value::String(title.clone()),
            );
        }
        if let Some(ref username) = msg.chat.username {
            metadata.insert(
                "chat_username".into(),
                serde_json::Value::String(username.clone()),
            );
        }

        // Absender-Informationen
        if let Some(ref from) = msg.from {
            metadata.insert("from_id".into(), serde_json::Value::Number(from.id.into()));
            if let Some(ref first_name) = from.first_name {
                metadata.insert(
                    "from_first_name".into(),
                    serde_json::Value::String(first_name.clone()),
                );
            }
            if let Some(ref username) = from.username {
                metadata.insert(
                    "from_username".into(),
                    serde_json::Value::String(username.clone()),
                );
            }
            metadata.insert("from_is_bot".into(), serde_json::Value::Bool(from.is_bot));
        }

        // Nachricht-Informationen
        metadata.insert(
            "message_id".into(),
            serde_json::Value::Number(msg.message_id.into()),
        );
        metadata.insert("date".into(), serde_json::Value::Number(msg.date.into()));

        // Nachrichtentyp
        let msg_type = self.get_message_type(msg);
        metadata.insert("message_type".into(), serde_json::Value::String(msg_type));

        // Forward-Informationen
        if let Some(ref forward_from) = msg.forward_from {
            metadata.insert(
                "forwarded_from".into(),
                serde_json::Value::String(forward_from.first_name.clone().unwrap_or_default()),
            );
        }

        // Location
        if let Some(ref location) = msg.location {
            metadata.insert(
                "location".into(),
                serde_json::json!({
                    "latitude": location.latitude,
                    "longitude": location.longitude
                }),
            );
        }

        // Contact
        if let Some(ref contact) = msg.contact {
            metadata.insert(
                "contact".into(),
                serde_json::json!({
                    "phone_number": contact.phone_number,
                    "first_name": contact.first_name
                }),
            );
        }

        let ink = Ink::new(topic, "telegram", &self.name, body)
            .with_metadata(serde_json::Value::Object(metadata));

        Ok(ink)
    }

    /// Bestimmt den Typ der Nachricht
    fn get_message_type(&self, msg: &TelegramMessage) -> String {
        if msg.text.is_some() {
            "text".to_string()
        } else if msg.photo.is_some() {
            "photo".to_string()
        } else if msg.document.is_some() {
            "document".to_string()
        } else if msg.voice.is_some() {
            "voice".to_string()
        } else if msg.audio.is_some() {
            "audio".to_string()
        } else if msg.video.is_some() {
            "video".to_string()
        } else if msg.location.is_some() {
            "location".to_string()
        } else if msg.contact.is_some() {
            "contact".to_string()
        } else if msg.caption.is_some() {
            "caption".to_string()
        } else {
            "unknown".to_string()
        }
    }

    /// Generiert eine Beschreibung für Nachrichten ohne Text
    fn get_message_type_description(&self, msg: &TelegramMessage) -> String {
        if msg.photo.is_some() {
            "[Foto]".to_string()
        } else if let Some(ref doc) = msg.document {
            format!(
                "[Dokument: {}]",
                doc.file_name
                    .clone()
                    .unwrap_or_else(|| "unbekannt".to_string())
            )
        } else if msg.voice.is_some() {
            "[Sprachnachricht]".to_string()
        } else if let Some(ref audio) = msg.audio {
            format!(
                "[Audio: {}]",
                audio
                    .title
                    .clone()
                    .unwrap_or_else(|| "unbekannt".to_string())
            )
        } else if msg.video.is_some() {
            "[Video]".to_string()
        } else if msg.location.is_some() {
            "[Standort]".to_string()
        } else if msg.contact.is_some() {
            "[Kontakt]".to_string()
        } else if let Some(ref caption) = msg.caption {
            caption.clone()
        } else {
            "[Unbekannter Nachrichtentyp]".to_string()
        }
    }

    /// Prüft, ob eine Nachricht von einem erlaubten Chat/Benutzer stammt
    fn is_allowed(&self, msg: &TelegramMessage) -> bool {
        // Prüfe erlaubte Chats
        if let Some(ref allowed_chats) = self.config.allowed_chats
            && !allowed_chats.is_empty()
            && !allowed_chats.contains(&msg.chat.id)
        {
            return false;
        }

        // Prüfe erlaubte Benutzer
        if let Some(ref allowed_users) = self.config.allowed_users
            && !allowed_users.is_empty()
            && let Some(ref from) = msg.from
            && !allowed_users.contains(&from.id)
        {
            return false;
        }

        true
    }

    /// Verarbeitet ein Telegram-Update
    async fn process_update(&self, update: TelegramUpdate, event_bus: &EventBus) {
        // Extrahiere die Nachricht aus dem Update
        let msg = if let Some(msg) = update.message {
            msg
        } else if let Some(msg) = update.edited_message {
            msg
        } else if let Some(msg) = update.channel_post {
            msg
        } else if let Some(msg) = update.edited_channel_post {
            msg
        } else {
            debug!(
                "Telegram Tentacle '{}': Ignoring update without message (id: {})",
                self.name, update.update_id
            );
            return;
        };

        // Prüfe, ob die Nachricht erlaubt ist
        if !self.is_allowed(&msg) {
            debug!(
                "Telegram Tentacle '{}': Message from chat {} not allowed",
                self.name, msg.chat.id
            );
            return;
        }

        // Erstelle und publishen die Ink-Nachricht
        match self.create_ink(&msg) {
            Ok(ink) => {
                info!(
                    "Telegram Tentacle '{}': Received message on topic: {} (chat: {}, user: {:?})",
                    self.name,
                    ink.topic,
                    msg.chat.id,
                    msg.from
                        .as_ref()
                        .and_then(|u| u.username.as_ref())
                        .unwrap_or(&"unknown".to_string())
                );
                event_bus.publish(ink);
            }
            Err(e) => {
                warn!(
                    "Telegram Tentacle '{}': Failed to create ink from message: {}",
                    self.name, e
                );
            }
        }
    }

    // -----------------------------------------------------------------------
    // Polling Mode
    // -----------------------------------------------------------------------

    /// Startet den Polling-Modus
    async fn start_polling(&self, event_bus: Arc<EventBus>) -> Result<()> {
        info!(
            "Telegram Tentacle '{}': Starting in polling mode",
            self.name
        );

        let client = reqwest::Client::new();
        let base_url = "https://api.telegram.org/bot{}/".to_string();
        let bot_token = self.config.bot_token.clone();
        let full_base_url = format!("{}{}", base_url, bot_token);

        let mut offset: i64 = 0;
        let mut retries = 0u32;
        let max_retries = self.config.max_retries;
        let reconnect_delay = self.config.reconnect_delay;

        loop {
            let url = format!(
                "{}/getUpdates?offset={}&timeout={}&limit=100",
                full_base_url, offset, self.config.polling_timeout
            );

            match client.get(&url).send().await {
                Ok(resp) => {
                    if !resp.status().is_success() {
                        let status = resp.status();
                        let body = resp.text().await.unwrap_or_default();
                        warn!(
                            "Telegram Tentacle '{}': API error {}: {}",
                            self.name, status, body
                        );

                        // Bei 401/404 ist der Token wahrscheinlich falsch
                        if status.as_u16() == 401 || status.as_u16() == 404 {
                            anyhow::bail!(
                                "Telegram Tentacle '{}': Invalid bot token (HTTP {})",
                                self.name,
                                status
                            );
                        }

                        retries += 1;
                        if retries >= max_retries {
                            anyhow::bail!(
                                "Telegram Tentacle '{}': Max retries ({}) reached",
                                self.name,
                                max_retries
                            );
                        }

                        info!(
                            "Telegram Tentacle '{}': Retrying in {} seconds...",
                            self.name, reconnect_delay
                        );
                        tokio::time::sleep(std::time::Duration::from_secs(reconnect_delay)).await;
                        continue;
                    }

                    retries = 0; // Reset retries on success

                    let result: TelegramApiResult<Vec<TelegramUpdate>> = match resp.json().await {
                        Ok(r) => r,
                        Err(e) => {
                            warn!(
                                "Telegram Tentacle '{}': Failed to parse API response: {}",
                                self.name, e
                            );
                            continue;
                        }
                    };

                    if !result.ok {
                        warn!(
                            "Telegram Tentacle '{}': API error: {}",
                            self.name,
                            result.description.unwrap_or_default()
                        );
                        continue;
                    }

                    if let Some(updates) = result.result {
                        if updates.is_empty() {
                            tokio::time::sleep(std::time::Duration::from_secs(
                                self.config.polling_interval,
                            ))
                            .await;
                            continue;
                        }

                        for update in updates {
                            // Offset auf das höchste Update-ID + 1 setzen
                            if update.update_id + 1 > offset {
                                offset = update.update_id + 1;
                            }

                            self.process_update(update, &event_bus).await;
                        }
                    }
                }
                Err(e) => {
                    warn!("Telegram Tentacle '{}': Polling error: {}", self.name, e);
                    retries += 1;
                    if retries >= max_retries {
                        anyhow::bail!(
                            "Telegram Tentacle '{}': Max retries ({}) reached",
                            self.name,
                            max_retries
                        );
                    }
                    info!(
                        "Telegram Tentacle '{}': Retrying in {} seconds...",
                        self.name, reconnect_delay
                    );
                    tokio::time::sleep(std::time::Duration::from_secs(reconnect_delay)).await;
                }
            }
        }
    }

    // -----------------------------------------------------------------------
    // Webhook Mode
    // -----------------------------------------------------------------------

    /// Startet den Webhook-Modus
    async fn start_webhook(&self, event_bus: Arc<EventBus>) -> Result<()> {
        info!(
            "Telegram Tentacle '{}': Starting in webhook mode",
            self.name
        );

        // Webhook beim Telegram-Server registrieren
        if let Some(ref webhook_url) = self.config.webhook_url {
            self.register_webhook(webhook_url).await?;
        }

        let tentacle_name = self.name.clone();
        let event_bus_clone = Arc::clone(&event_bus);
        let secret_token = self.config.webhook_secret_token.clone();

        let path = if self.config.webhook_path.is_empty() {
            "/telegram".to_string()
        } else {
            self.config.webhook_path.clone()
        };

        // Axum-App erstellen
        let app = Router::new()
            .route(&path, post(webhook_handler))
            .with_state((tentacle_name, event_bus_clone, secret_token));

        let addr = format!("{}:{}", self.config.webhook_host, self.config.webhook_port);
        let listener = tokio::net::TcpListener::bind(&addr).await?;

        info!(
            "Telegram Tentacle '{}' listening on {}{}",
            self.name, addr, path
        );
        axum::serve(listener, app).await?;

        Ok(())
    }

    /// Registriert den Webhook beim Telegram-Server
    async fn register_webhook(&self, url: &str) -> Result<()> {
        let bot_token = self.config.bot_token.clone();
        let api_url = format!(
            "https://api.telegram.org/bot{}/setWebhook?url={}",
            bot_token, url
        );

        let client = reqwest::Client::new();
        let result: TelegramApiResult<bool> = client.get(&api_url).send().await?.json().await?;

        if result.ok {
            info!(
                "Telegram Tentacle '{}': Webhook registered at {}",
                self.name, url
            );
        } else {
            warn!(
                "Telegram Tentacle '{}': Failed to register webhook: {}",
                self.name,
                result.description.unwrap_or_default()
            );
        }

        Ok(())
    }
}

// ---------------------------------------------------------------------------
// Webhook Handler
// ---------------------------------------------------------------------------

async fn webhook_handler(
    State((tentacle_name, event_bus, secret_token)): State<(String, Arc<EventBus>, Option<String>)>,
    headers: axum::http::HeaderMap,
    Json(update): Json<TelegramUpdate>,
) -> StatusCode {
    // Secret Token validieren, falls konfiguriert
    if let Some(ref secret) = secret_token {
        let x_telegram = headers
            .get("X-Telegram-Bot-Api-Secret-Token")
            .and_then(|v| v.to_str().ok());

        if x_telegram != Some(secret.as_str()) {
            warn!(
                "Telegram Tentacle '{}': Invalid secret token",
                tentacle_name
            );
            return StatusCode::FORBIDDEN;
        }
    }

    // Extrahiere die Nachricht aus dem Update
    let msg = if let Some(msg) = update.message {
        msg
    } else if let Some(msg) = update.edited_message {
        msg
    } else if let Some(msg) = update.channel_post {
        msg
    } else if let Some(msg) = update.edited_channel_post {
        msg
    } else {
        debug!(
            "Telegram Tentacle '{}': Ignoring update without message (id: {})",
            tentacle_name, update.update_id
        );
        return StatusCode::OK;
    };

    // Bestimme den Topic
    let topic = resolve_topic_for_webhook(&msg);

    // Bestimme den Nachrichtentext
    let body = if let Some(ref text) = msg.text {
        text.clone()
    } else if let Some(ref caption) = msg.caption {
        caption.clone()
    } else {
        get_message_type_description_for_webhook(&msg)
    };

    // Metadaten erstellen
    let mut metadata = serde_json::Map::new();
    metadata.insert(
        "chat_id".into(),
        serde_json::Value::Number(msg.chat.id.into()),
    );
    metadata.insert(
        "chat_type".into(),
        serde_json::Value::String(msg.chat.r#type.clone()),
    );
    if let Some(ref from) = msg.from {
        metadata.insert("from_id".into(), serde_json::Value::Number(from.id.into()));
        if let Some(ref username) = from.username {
            metadata.insert(
                "from_username".into(),
                serde_json::Value::String(username.clone()),
            );
        }
    }
    metadata.insert(
        "message_id".into(),
        serde_json::Value::Number(msg.message_id.into()),
    );

    let ink = Ink::new(topic, "telegram", &tentacle_name, body)
        .with_metadata(serde_json::Value::Object(metadata));

    info!(
        "Telegram Tentacle '{}': Received webhook message on topic: {}",
        tentacle_name, ink.topic
    );
    event_bus.publish(ink);

    StatusCode::OK
}

/// Hilfsfunktion für Webhook: Topic bestimmen (vereinfachte Version)
fn resolve_topic_for_webhook(msg: &TelegramMessage) -> String {
    if let Some(ref text) = msg.text {
        let trimmed = text.trim();
        if trimmed.starts_with('/')
            && let Some(topic_part) = trimmed.split_whitespace().next()
            && topic_part.len() > 1
        {
            return topic_part.to_string();
        }
    }
    format!("/telegram/chat/{}", msg.chat.id)
}

/// Hilfsfunktion für Webhook: Nachrichtentyp-Beschreibung
fn get_message_type_description_for_webhook(msg: &TelegramMessage) -> String {
    if msg.photo.is_some() {
        "[Foto]".to_string()
    } else if let Some(ref doc) = msg.document {
        format!(
            "[Dokument: {}]",
            doc.file_name
                .clone()
                .unwrap_or_else(|| "unbekannt".to_string())
        )
    } else if msg.voice.is_some() {
        "[Sprachnachricht]".to_string()
    } else if msg.video.is_some() {
        "[Video]".to_string()
    } else if msg.location.is_some() {
        "[Standort]".to_string()
    } else if let Some(ref caption) = msg.caption {
        caption.clone()
    } else {
        "[Unbekannter Nachrichtentyp]".to_string()
    }
}

// ---------------------------------------------------------------------------
// Tentacle Trait Implementation
// ---------------------------------------------------------------------------

#[allow(dead_code)]
#[async_trait]
impl Tentacle for TelegramTentacle {
    fn name(&self) -> &str {
        &self.name
    }

    fn r#type(&self) -> &str {
        "telegram"
    }

    async fn start(&self, event_bus: Arc<EventBus>) -> Result<()> {
        info!(
            "Telegram Tentacle '{}': Starting (mode: {})",
            self.name, self.config.connection_type
        );

        match self.config.connection_type.as_str() {
            "webhook" => self.start_webhook(event_bus).await,
            "polling" | "long_polling" => self.start_polling(event_bus).await,
            _ => self.start_polling(event_bus).await,
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn make_private_chat(id: i64) -> TelegramChat {
        TelegramChat {
            id,
            r#type: "private".to_string(),
            title: None,
            username: None,
            first_name: Some("Test".to_string()),
            last_name: None,
        }
    }

    fn make_group_chat(id: i64) -> TelegramChat {
        TelegramChat {
            id,
            r#type: "group".to_string(),
            title: Some("Test Group".to_string()),
            username: Some("testgroup".to_string()),
            first_name: None,
            last_name: None,
        }
    }

    fn make_user(id: i64, username: Option<&str>) -> TelegramUser {
        TelegramUser {
            id,
            is_bot: false,
            first_name: Some("User".to_string()),
            last_name: None,
            username: username.map(|s| s.to_string()),
            language_code: None,
        }
    }

    fn make_message(text: &str, chat: TelegramChat) -> TelegramMessage {
        TelegramMessage {
            message_id: 1,
            date: 1700000000,
            chat,
            from: Some(make_user(123, Some("testuser"))),
            text: Some(text.to_string()),
            entities: None,
            caption: None,
            photo: None,
            document: None,
            voice: None,
            audio: None,
            video: None,
            forward_from: None,
            forward_from_chat: None,
            reply_to_message: None,
            via_bot: None,
            location: None,
            contact: None,
        }
    }

    #[test]
    fn test_extract_command_topic() {
        let config = TelegramTentacleConfig {
            bot_token: "test".to_string(),
            connection_type: "polling".to_string(),
            webhook_host: "127.0.0.1".to_string(),
            webhook_port: 8443,
            webhook_path: "/telegram".to_string(),
            webhook_secret_token: None,
            polling_interval: 3,
            polling_timeout: 25,
            topic_mode: "command".to_string(),
            fallback_topic: "/telegram/default".to_string(),
            chat_topic_mapping: None,
            username_topic_mapping: None,
            allowed_chats: None,
            allowed_users: None,
            webhook_url: None,
            max_retries: 5,
            reconnect_delay: 5,
        };
        let tentacle = TelegramTentacle::new("test".to_string(), config);

        assert_eq!(
            tentacle.extract_command_topic("/kraken/warning"),
            Some("/kraken/warning".to_string())
        );
        assert_eq!(
            tentacle.extract_command_topic("/alert Temperatur zu hoch"),
            Some("/alert".to_string())
        );
        assert_eq!(tentacle.extract_command_topic("Kein Befehl"), None);
        assert_eq!(tentacle.extract_command_topic("/"), None);
    }

    #[test]
    fn test_resolve_topic_command_mode() {
        let config = TelegramTentacleConfig {
            bot_token: "test".to_string(),
            connection_type: "polling".to_string(),
            webhook_host: "127.0.0.1".to_string(),
            webhook_port: 8443,
            webhook_path: "/telegram".to_string(),
            webhook_secret_token: None,
            polling_interval: 3,
            polling_timeout: 25,
            topic_mode: "command".to_string(),
            fallback_topic: "/telegram/default".to_string(),
            chat_topic_mapping: None,
            username_topic_mapping: None,
            allowed_chats: None,
            allowed_users: None,
            webhook_url: None,
            max_retries: 5,
            reconnect_delay: 5,
        };
        let tentacle = TelegramTentacle::new("test".to_string(), config);

        let msg = make_message("/kraken/warning Temperatur zu hoch", make_private_chat(100));
        assert_eq!(tentacle.resolve_topic(&msg), "/kraken/warning");

        let msg = make_message("Kein Befehl hier", make_private_chat(100));
        assert_eq!(tentacle.resolve_topic(&msg), "/telegram/default");
    }

    #[test]
    fn test_resolve_topic_chat_mode_with_mapping() {
        let mut chat_mapping = HashMap::new();
        chat_mapping.insert("100".to_string(), "/kraken/alerts".to_string());
        chat_mapping.insert("200".to_string(), "/kraken/status".to_string());

        let config = TelegramTentacleConfig {
            bot_token: "test".to_string(),
            connection_type: "polling".to_string(),
            webhook_host: "127.0.0.1".to_string(),
            webhook_port: 8443,
            webhook_path: "/telegram".to_string(),
            webhook_secret_token: None,
            polling_interval: 3,
            polling_timeout: 25,
            topic_mode: "chat".to_string(),
            fallback_topic: "/telegram/default".to_string(),
            chat_topic_mapping: Some(chat_mapping),
            username_topic_mapping: None,
            allowed_chats: None,
            allowed_users: None,
            webhook_url: None,
            max_retries: 5,
            reconnect_delay: 5,
        };
        let tentacle = TelegramTentacle::new("test".to_string(), config);

        let msg = make_message("Nachricht", make_group_chat(100));
        assert_eq!(tentacle.resolve_topic(&msg), "/kraken/alerts");

        let msg = make_message("Nachricht", make_group_chat(200));
        assert_eq!(tentacle.resolve_topic(&msg), "/kraken/status");

        // Kein Mapping -> Fallback auf Chat-ID
        let msg = make_message("Nachricht", make_group_chat(999));
        assert_eq!(tentacle.resolve_topic(&msg), "/telegram/chat/999");
    }

    #[test]
    fn test_resolve_topic_all_mode() {
        let config = TelegramTentacleConfig {
            bot_token: "test".to_string(),
            connection_type: "polling".to_string(),
            webhook_host: "127.0.0.1".to_string(),
            webhook_port: 8443,
            webhook_path: "/telegram".to_string(),
            webhook_secret_token: None,
            polling_interval: 3,
            polling_timeout: 25,
            topic_mode: "all".to_string(),
            fallback_topic: "/telegram/default".to_string(),
            chat_topic_mapping: None,
            username_topic_mapping: None,
            allowed_chats: None,
            allowed_users: None,
            webhook_url: None,
            max_retries: 5,
            reconnect_delay: 5,
        };
        let tentacle = TelegramTentacle::new("test".to_string(), config);

        // Command hat Vorrang
        let msg = make_message("/alert Wichtig!", make_private_chat(100));
        assert_eq!(tentacle.resolve_topic(&msg), "/alert");

        // Content mit /-Präfix
        let msg = make_message("/status OK", make_private_chat(100));
        assert_eq!(tentacle.resolve_topic(&msg), "/status");

        // Kein Command/Content -> Chat-Fallback
        let msg = make_message("Einfache Nachricht", make_private_chat(100));
        assert_eq!(tentacle.resolve_topic(&msg), "/telegram/chat/100");
    }

    #[test]
    fn test_create_ink() {
        let config = TelegramTentacleConfig {
            bot_token: "test".to_string(),
            connection_type: "polling".to_string(),
            webhook_host: "127.0.0.1".to_string(),
            webhook_port: 8443,
            webhook_path: "/telegram".to_string(),
            webhook_secret_token: None,
            polling_interval: 3,
            polling_timeout: 25,
            topic_mode: "all".to_string(),
            fallback_topic: "/telegram/default".to_string(),
            chat_topic_mapping: None,
            username_topic_mapping: None,
            allowed_chats: None,
            allowed_users: None,
            webhook_url: None,
            max_retries: 5,
            reconnect_delay: 5,
        };
        let tentacle = TelegramTentacle::new("test".to_string(), config);

        let msg = make_message("/kraken/test Testnachricht", make_private_chat(42));
        let ink = tentacle.create_ink(&msg).unwrap();

        assert_eq!(ink.topic, "/kraken/test");
        assert_eq!(ink.source.r#type, "telegram");
        assert_eq!(ink.source.name, "test");
        assert!(ink.body.contains("/kraken/test"));
        assert!(ink.metadata.is_some());
    }

    #[test]
    fn test_is_allowed() {
        // Mit allowed_chats
        let config = TelegramTentacleConfig {
            bot_token: "test".to_string(),
            connection_type: "polling".to_string(),
            webhook_host: "127.0.0.1".to_string(),
            webhook_port: 8443,
            webhook_path: "/telegram".to_string(),
            webhook_secret_token: None,
            polling_interval: 3,
            polling_timeout: 25,
            topic_mode: "all".to_string(),
            fallback_topic: "/telegram/default".to_string(),
            chat_topic_mapping: None,
            username_topic_mapping: None,
            allowed_chats: Some(vec![100, 200]),
            allowed_users: None,
            webhook_url: None,
            max_retries: 5,
            reconnect_delay: 5,
        };
        let tentacle = TelegramTentacle::new("test".to_string(), config);

        let msg = make_message("Hallo", make_private_chat(100));
        assert!(tentacle.is_allowed(&msg));

        let msg = make_message("Hallo", make_private_chat(999));
        assert!(!tentacle.is_allowed(&msg));

        // Ohne Filter
        let config = TelegramTentacleConfig {
            allowed_chats: None,
            allowed_users: None,
            ..TelegramTentacleConfig {
                bot_token: "test".to_string(),
                connection_type: "polling".to_string(),
                webhook_host: "127.0.0.1".to_string(),
                webhook_port: 8443,
                webhook_path: "/telegram".to_string(),
                webhook_secret_token: None,
                polling_interval: 3,
                polling_timeout: 25,
                topic_mode: "all".to_string(),
                fallback_topic: "/telegram/default".to_string(),
                chat_topic_mapping: None,
                username_topic_mapping: None,
                allowed_chats: None,
                allowed_users: None,
                webhook_url: None,
                max_retries: 5,
                reconnect_delay: 5,
            }
        };
        let tentacle = TelegramTentacle::new("test".to_string(), config);

        let msg = make_message("Hallo", make_private_chat(999));
        assert!(tentacle.is_allowed(&msg));
    }

    #[test]
    fn test_message_type_detection() {
        let config = TelegramTentacleConfig {
            bot_token: "test".to_string(),
            connection_type: "polling".to_string(),
            webhook_host: "127.0.0.1".to_string(),
            webhook_port: 8443,
            webhook_path: "/telegram".to_string(),
            webhook_secret_token: None,
            polling_interval: 3,
            polling_timeout: 25,
            topic_mode: "all".to_string(),
            fallback_topic: "/telegram/default".to_string(),
            chat_topic_mapping: None,
            username_topic_mapping: None,
            allowed_chats: None,
            allowed_users: None,
            webhook_url: None,
            max_retries: 5,
            reconnect_delay: 5,
        };
        let tentacle = TelegramTentacle::new("test".to_string(), config);

        // Text message
        let msg = make_message("Hallo", make_private_chat(100));
        assert_eq!(tentacle.get_message_type(&msg), "text");

        // Photo message
        let mut msg = TelegramMessage {
            message_id: 1,
            date: 1700000000,
            chat: make_private_chat(100),
            from: Some(make_user(123, Some("testuser"))),
            text: None,
            entities: None,
            caption: Some("Ein Foto".to_string()),
            photo: Some(vec![]),
            document: None,
            voice: None,
            audio: None,
            video: None,
            forward_from: None,
            forward_from_chat: None,
            reply_to_message: None,
            via_bot: None,
            location: None,
            contact: None,
        };
        assert_eq!(tentacle.get_message_type(&msg), "photo");
        assert_eq!(tentacle.get_message_type_description(&msg), "[Foto]");

        // Voice message
        msg = TelegramMessage {
            voice: Some(TelegramVoice {
                mime_type: None,
                duration: None,
            }),
            ..msg
        };
        msg.photo = None;
        assert_eq!(tentacle.get_message_type(&msg), "voice");
        assert_eq!(
            tentacle.get_message_type_description(&msg),
            "[Sprachnachricht]"
        );
    }

    #[test]
    fn test_webhook_topic_resolution() {
        let msg = make_message("/kraken/warning Temperatur", make_private_chat(100));
        assert_eq!(resolve_topic_for_webhook(&msg), "/kraken/warning");

        let msg = make_message("Kein Befehl", make_private_chat(200));
        assert_eq!(resolve_topic_for_webhook(&msg), "/telegram/chat/200");
    }
}
