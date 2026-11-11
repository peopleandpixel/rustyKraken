use anyhow::{Context, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info, warn};

use async_imap::Client;
use futures_util::StreamExt;
use mailparse::MailHeaderMap;
use tokio::net::TcpStream;
use tokio_rustls::{TlsConnector, rustls::pki_types::ServerName};

use super::Tentacle;
use crate::eventbus::EventBus;
use crate::model::Ink;

// Type alias für IMAP-Sessions – unterstützt sowohl TCP als auch TLS
type ImapStream = tokio_rustls::client::TlsStream<TcpStream>;

/// Konfiguration für das Topic-Matching bei E-Mails
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "mode")]
pub enum ImapTopicMode {
    /// Verwende den Betreff als Topic (Betreff muss mit "/" beginnen)
    #[serde(rename = "subject")]
    Subject,

    /// Reagiere auf alle E-Mails von einem bestimmten Absender
    #[serde(rename = "sender")]
    Sender {
        /// Absender-Adresse oder Muster (unterstützt Platzhalter wie "*@example.com")
        sender_pattern: String,
        /// Topic, das für E-Mails von diesem Absender verwendet wird
        topic: String,
    },

    /// Reagiere auf alle E-Mails in einem Postfach
    #[serde(rename = "all")]
    All {
        /// Topic, das für alle E-Mails verwendet wird
        topic: String,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ImapTentacleConfig {
    /// IMAP-Server-Host
    pub host: String,
    /// IMAP-Port (Standard: 993 für SSL/TLS, 143 für STARTTLS)
    #[serde(default = "default_port")]
    pub port: u16,
    /// Benutzername
    pub username: String,
    /// Passwort (oder Umgebungsvariable-Referenz wie "${IMAP_PASSWORD}")
    pub password: String,
    /// Postfach, das überwacht werden soll (Standard: "INBOX")
    #[serde(default = "default_mailbox")]
    pub mailbox: String,
    /// Topic-Matching-Modus
    pub topic_mode: ImapTopicMode,
    /// Optional: Zusätzliche Topic-Mappings basierend auf Absender
    #[serde(default)]
    pub sender_mappings: Option<HashMap<String, String>>,
    /// Intervall in Sekunden zwischen den Polls (Standard: 30)
    #[serde(default = "default_poll_interval")]
    pub poll_interval: u64,
    /// Optional: E-Mails nach Verarbeitung als gelesen markieren (Standard: true)
    #[serde(default = "default_mark_read")]
    pub mark_read: bool,
    /// Optional: E-Mails nach Verarbeitung verschieben (z.B. "Processed")
    #[serde(default)]
    pub move_to: Option<String>,
    /// Optional: IDLE-Modus verwenden für Echtzeit-Benachrichtigungen (Standard: false)
    /// Funktioniert nur, wenn der Server IDLE unterstützt
    #[serde(default)]
    pub use_idle: bool,
    /// Optional: STARTTLS verwenden (Port 143) statt implizite TLS (Port 993)
    /// Standard: false (implizite TLS für Port 993)
    #[serde(default)]
    pub starttls: bool,
    /// Optional: Pfad zur Datei für persistente UID-Speicherung
    /// Standard: "data/imap_<name>_uids.json"
    #[serde(default)]
    pub uid_storage_path: Option<String>,
}

fn default_port() -> u16 {
    993
}

fn default_mailbox() -> String {
    "INBOX".to_string()
}

fn default_poll_interval() -> u64 {
    30
}

fn default_mark_read() -> bool {
    true
}

impl ImapTentacleConfig {
    /// Ersetzt Umgebungsvariable-Referenzen im Passwort
    fn resolve_password(&self) -> String {
        let password = self.password.trim_start_matches("${").trim_end_matches('}');
        if self.password.starts_with("${") && self.password.ends_with('}') {
            std::env::var(password).unwrap_or_else(|_| {
                warn!(
                    "Environment variable '{}' not found, using literal value",
                    password
                );
                self.password.clone()
            })
        } else {
            self.password.clone()
        }
    }

    /// Gibt den Pfad zur UID-Speicherdatei zurück
    fn get_uid_storage_path(&self, tentacle_name: &str) -> PathBuf {
        if let Some(ref path) = self.uid_storage_path {
            PathBuf::from(path)
        } else {
            PathBuf::from(format!("data/imap_{}_uids.json", tentacle_name))
        }
    }
}

/// Persistente UID-Speicherung
#[derive(Debug, Clone, Serialize, Deserialize)]
struct UidStorage {
    /// Letzter verarbeiteter UIDVALIDITY-Wert (zur Erkennung von mailbox resets)
    uid_validity: u32,
    /// Menge der bereits verarbeiteten UIDs
    processed_uids: HashSet<u32>,
}

impl UidStorage {
    /// Lädt die UID-Speicherung aus einer Datei
    async fn load(path: &PathBuf) -> Result<Self> {
        if !path.exists() {
            return Ok(Self {
                uid_validity: 0,
                processed_uids: HashSet::new(),
            });
        }

        let content = tokio::fs::read_to_string(path)
            .await
            .with_context(|| format!("Failed to read UID storage file: {}", path.display()))?;

        serde_json::from_str(&content)
            .with_context(|| format!("Failed to parse UID storage file: {}", path.display()))
    }

    /// Speichert die UID-Speicherung in einer Datei
    async fn save(&self, path: &PathBuf) -> Result<()> {
        // Elternverzeichnis erstellen, falls nötig
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent)
                .await
                .with_context(|| format!("Failed to create directory: {}", parent.display()))?;
        }

        let content = serde_json::to_string_pretty(self)
            .with_context(|| "Failed to serialize UID storage")?;

        tokio::fs::write(path, content)
            .await
            .with_context(|| format!("Failed to write UID storage file: {}", path.display()))?;

        Ok(())
    }

    /// Prüft, ob eine UID bereits verarbeitet wurde
    fn is_processed(&self, uid: u32) -> bool {
        self.processed_uids.contains(&uid)
    }

    /// Markiert eine UID als verarbeitet
    fn mark_processed(&mut self, uid: u32) {
        self.processed_uids.insert(uid);
    }

    /// Markiert mehrere UIDs als verarbeitet
    #[allow(dead_code)]
    fn mark_processed_batch(&mut self, uids: &[u32]) {
        for uid in uids {
            self.processed_uids.insert(*uid);
        }
    }

    /// Zurücksetzen bei geänderter UIDVALIDITY
    fn reset(&mut self, new_validity: u32) {
        warn!(
            "UIDVALIDITY changed from {} to {}, resetting processed UIDs",
            self.uid_validity, new_validity
        );
        self.uid_validity = new_validity;
        self.processed_uids.clear();
    }
}

pub struct ImapTentacle {
    name: String,
    config: ImapTentacleConfig,
}

impl ImapTentacle {
    pub fn new(name: String, config: ImapTentacleConfig) -> Self {
        Self { name, config }
    }

    /// Bestimmt das Topic für eine E-Mail basierend auf der Konfiguration
    fn determine_topic(&self, subject: &str, from: &str) -> String {
        match &self.config.topic_mode {
            ImapTopicMode::Subject => {
                // Betreff als Topic verwenden, falls er mit "/" beginnt
                if subject.starts_with('/') {
                    subject.to_string()
                } else {
                    // Fallback: /email/default
                    "/email/default".to_string()
                }
            }
            ImapTopicMode::Sender {
                sender_pattern,
                topic,
            } => {
                // Falls die sender_pattern matcht, verwende das Topic
                if matches_email_pattern(from, sender_pattern) {
                    topic.clone()
                } else {
                    "/email/default".to_string()
                }
            }
            ImapTopicMode::All { topic } => topic.clone(),
        }
    }

    /// Prüft Absender-Mappings für zusätzliche Topic-Regeln
    fn check_sender_mapping(&self, from: &str) -> Option<String> {
        if let Some(mappings) = &self.config.sender_mappings {
            // Exaktes Matching zuerst
            if let Some(topic) = mappings.get(from) {
                return Some(topic.clone());
            }

            // Dann Muster-Matching
            for (pattern, topic) in mappings {
                if matches_email_pattern(from, pattern) {
                    return Some(topic.clone());
                }
            }
        }
        None
    }
}

/// Prüft, ob eine E-Mail-Adresse zu einem Muster passt
/// Unterstützt "*" als Platzhalter
fn matches_email_pattern(email: &str, pattern: &str) -> bool {
    if pattern == email {
        return true;
    }

    // Einfaches "*" Matching
    if pattern.contains('*') {
        let parts: Vec<&str> = pattern.splitn(2, '*').collect();
        let prefix = parts[0];
        let suffix = if parts.len() > 1 { parts[1] } else { "" };

        email.starts_with(prefix) && email.ends_with(suffix)
    } else {
        false
    }
}

#[async_trait]
impl Tentacle for ImapTentacle {
    fn name(&self) -> &str {
        &self.name
    }

    fn r#type(&self) -> &str {
        "imap"
    }

    async fn start(&self, event_bus: Arc<EventBus>) -> Result<()> {
        info!(
            "Starting IMAP tentacle '{}' connecting to {}:{}",
            self.name, self.config.host, self.config.port
        );

        let password = self.config.resolve_password();
        let mailbox = self.config.mailbox.clone();
        let poll_interval = Duration::from_secs(self.config.poll_interval);
        let host = self.config.host.clone();
        let port = self.config.port;
        let username = self.config.username.clone();
        let use_idle = self.config.use_idle;
        let starttls = self.config.starttls;

        // UID-Speicherung laden
        let uid_storage_path = self.config.get_uid_storage_path(&self.name);
        let mut uid_storage = UidStorage::load(&uid_storage_path)
            .await
            .with_context(|| "Failed to load UID storage")?;

        info!(
            "IMAP tentacle '{}': loaded {} processed UIDs from storage",
            self.name,
            uid_storage.processed_uids.len()
        );

        loop {
            // Verbindung zum IMAP-Server aufbauen
            let mut session = match connect_imap(&host, port, &username, &password, starttls).await
            {
                Ok(s) => s,
                Err(e) => {
                    error!("IMAP tentacle '{}' failed to connect: {}", self.name, e);
                    tokio::time::sleep(poll_interval).await;
                    continue;
                }
            };

            // Postfach öffnen (read-write für select, da wir UIDs verwenden)
            let mailbox_info = match session.select(&mailbox).await {
                Ok(info) => info,
                Err(e) => {
                    error!(
                        "IMAP tentacle '{}' failed to select mailbox '{}': {}",
                        self.name, mailbox, e
                    );
                    tokio::time::sleep(poll_interval).await;
                    continue;
                }
            };

            debug!(
                "IMAP tentacle '{}' opened mailbox '{}' with {} unseen messages",
                self.name,
                mailbox,
                mailbox_info.unseen.unwrap_or(0)
            );

            // UIDVALIDITY prüfen
            if let Some(uid_validity) = mailbox_info.uid_validity {
                if uid_storage.uid_validity != 0 && uid_storage.uid_validity != uid_validity {
                    uid_storage.reset(uid_validity);
                    uid_storage.save(&uid_storage_path).await.ok();
                } else if uid_storage.uid_validity == 0 {
                    uid_storage.uid_validity = uid_validity;
                    uid_storage.save(&uid_storage_path).await.ok();
                }
            }

            if use_idle {
                // IDLE-Modus: Echtzeit-Benachrichtigungen
                match run_idle_mode(
                    &self.name,
                    session,
                    &mailbox,
                    &event_bus,
                    &mut uid_storage,
                    &uid_storage_path,
                    &poll_interval,
                    self,
                )
                .await
                {
                    Ok(_) => {
                        // IDLE-Loop beendet, neu verbinden
                        break;
                    }
                    Err(e) => {
                        warn!(
                            "IMAP tentacle '{}' IDLE mode failed: {}, falling back to polling",
                            self.name, e
                        );
                        break;
                    }
                }
            } else {
                // Polling-Modus
                if let Err(e) = fetch_and_process_new_emails(
                    &self.name,
                    session,
                    &mailbox,
                    &event_bus,
                    &mut uid_storage,
                    &uid_storage_path,
                    self,
                )
                .await
                {
                    error!("IMAP tentacle '{}' polling mode failed: {}", self.name, e);
                }
                break;
            }
        }

        Ok(())
    }
}

/// IDLE-Modus: Echtzeit-Benachrichtigungen vom Server
#[allow(clippy::too_many_arguments)]
async fn run_idle_mode(
    tentacle_name: &str,
    session: async_imap::Session<ImapStream>,
    mailbox: &str,
    event_bus: &EventBus,
    uid_storage: &mut UidStorage,
    uid_storage_path: &PathBuf,
    poll_interval: &Duration,
    tentacle: &ImapTentacle,
) -> Result<()> {
    use async_imap::extensions::idle::IdleResponse;

    info!("IMAP tentacle '{}': entering IDLE mode", tentacle_name);

    // IDLE-Handle erstellen (nimmt ownership der session)
    let mut idle_handle = session.idle();

    // IDLE initialisieren
    idle_handle
        .init()
        .await
        .with_context(|| format!("IMAP tentacle '{}' failed to init IDLE", tentacle_name))?;

    info!(
        "IMAP tentacle '{}': IDLE started, waiting for new messages",
        tentacle_name
    );

    // IDLE-Loop mit Timeout
    let idle_timeout = *poll_interval;

    loop {
        let (future, _stop_source) = idle_handle.wait_with_timeout(idle_timeout);

        match future.await {
            Ok(IdleResponse::NewData(response)) => {
                debug!(
                    "IMAP tentacle '{}': IDLE response: {:?}",
                    tentacle_name, response
                );

                // IDLE beenden und Session zurückholen
                let recovered_session = match idle_handle.done().await {
                    Ok(s) => s,
                    Err(e) => {
                        error!(
                            "IMAP tentacle '{}' failed to exit IDLE: {}",
                            tentacle_name, e
                        );
                        break;
                    }
                };

                // Neue Nachrichten suchen und verarbeiten
                let result = fetch_and_process_new_emails(
                    tentacle_name,
                    recovered_session,
                    mailbox,
                    event_bus,
                    uid_storage,
                    uid_storage_path,
                    tentacle,
                )
                .await;

                match result {
                    Ok(()) => {
                        // UID-Speicherung aktualisieren
                        uid_storage.save(uid_storage_path).await.ok();
                    }
                    Err(e) => {
                        error!(
                            "IMAP tentacle '{}' failed to process new emails: {}",
                            tentacle_name, e
                        );
                    }
                }

                // session ist jetzt consumed, IDLE neu starten
                // Da fetch_and_process_new_emails die session consumed hat,
                // müssen wir hier neu verbinden
                break;
            }
            Ok(IdleResponse::Timeout) => {
                debug!(
                    "IMAP tentacle '{}': IDLE timeout, keeping alive",
                    tentacle_name
                );
            }
            Ok(IdleResponse::ManualInterrupt) => {
                info!(
                    "IMAP tentacle '{}': IDLE manually interrupted",
                    tentacle_name
                );
                break;
            }
            Err(e) => {
                error!("IMAP tentacle '{}' IDLE error: {}", tentacle_name, e);
                break;
            }
        }
    }

    Ok(())
}

/// Sucht nach neuen E-Mails und verarbeitet sie
async fn fetch_and_process_new_emails(
    tentacle_name: &str,
    session: async_imap::Session<ImapStream>,
    _mailbox: &str,
    event_bus: &EventBus,
    uid_storage: &mut UidStorage,
    uid_storage_path: &PathBuf,
    tentacle: &ImapTentacle,
) -> Result<()> {
    // UID SEARCH für ungelesene Nachrichten
    let mut session = session;
    let uid_result = session
        .uid_search("UNSEEN")
        .await
        .with_context(|| format!("IMAP tentacle '{}' UID SEARCH failed", tentacle_name))?;

    let new_uids: Vec<u32> = uid_result
        .into_iter()
        .filter(|&uid| !uid_storage.is_processed(uid))
        .collect();

    if new_uids.is_empty() {
        debug!("IMAP tentacle '{}': no new messages", tentacle_name);
        return Ok(());
    }

    info!(
        "IMAP tentacle '{}': found {} new message(s)",
        tentacle_name,
        new_uids.len()
    );

    // Jede neue E-Mail verarbeiten
    for uid in &new_uids {
        if let Err(e) =
            process_email_by_uid(&mut session, *uid, tentacle_name, event_bus, tentacle).await
        {
            error!(
                "IMAP tentacle '{}' failed to process message UID {}: {}",
                tentacle_name, uid, e
            );
        } else {
            uid_storage.mark_processed(*uid);
            debug!(
                "IMAP tentacle '{}': marked UID {} as processed",
                tentacle_name, uid
            );
        }
    }

    // UID-Speicherung aktualisieren
    uid_storage.save(uid_storage_path).await?;

    Ok(())
}

/// Stellt eine IMAP-Verbindung her und authentifiziert sich
/// Unterstützt implizite TLS (Port 993), STARTTLS (Port 143) und plain Verbindungen
async fn connect_imap(
    host: &str,
    port: u16,
    username: &str,
    password: &str,
    starttls: bool,
) -> Result<async_imap::Session<ImapStream>> {
    // TLS-Konfiguration vorbereiten
    let mut roots = tokio_rustls::rustls::RootCertStore::empty();
    for cert in rustls_native_certs::load_native_certs().certs {
        roots.add(cert).ok();
    }

    let tls_config = tokio_rustls::rustls::ClientConfig::builder()
        .with_root_certificates(roots)
        .with_no_client_auth();

    let server_name = ServerName::try_from(host)
        .map_err(|_| anyhow::anyhow!("Invalid hostname: {}", host))?
        .to_owned();

    if starttls {
        // STARTTLS-Modus: TCP-Verbindung aufbauen, STARTTLS-Befehl senden, dann TLS-Upgrade
        info!(
            "IMAP tentacle: Using STARTTLS to connect to {}:{}",
            host, port
        );
        let session =
            connect_starttls(host, port, username, password, &server_name, &tls_config).await?;
        Ok(session)
    } else {
        // Implizite TLS-Verbindung aufbauen
        let session =
            connect_implicit_tls(host, port, username, password, &server_name, &tls_config).await?;
        Ok(session)
    }
}

/// Stellt eine STARTTLS-Verbindung her (Port 143)
/// 1. TCP-Verbindung aufbauen
/// 2. CAPABILITY prüfen (optional)
/// 3. STARTTLS-Befehl senden
/// 4. TLS-Upgrade des Streams
/// 5. Login
async fn connect_starttls(
    host: &str,
    port: u16,
    username: &str,
    password: &str,
    server_name: &ServerName<'static>,
    tls_config: &tokio_rustls::rustls::ClientConfig,
) -> Result<async_imap::Session<ImapStream>> {
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

    let addr = format!("{}:{}", host, port);

    // 1. TCP-Verbindung aufbauen
    let tcp_stream = TcpStream::connect(&addr)
        .await
        .with_context(|| format!("Failed to connect to IMAP server at {}", addr))?;

    debug!("IMAP tentacle: TCP connection established to {}", addr);

    // BufReader für zeilenweises Lesen
    let mut reader = BufReader::new(tcp_stream);

    // 2. Welcome-Message lesen
    let mut welcome = String::new();
    reader.read_line(&mut welcome).await?;
    debug!("IMAP tentacle: Welcome message: {}", welcome.trim());

    // 3. CAPABILITY abfragen (optional, um STARTTLS zu prüfen)
    reader.get_mut().write_all(b"CAPABILITY\r\n").await.ok();
    reader.get_mut().flush().await.ok();

    // CAPABILITY-Antwort lesen (kann mehrere Zeilen sein)
    let mut has_starttls = false;
    loop {
        let mut line = String::new();
        match reader.read_line(&mut line).await {
            Ok(n) if n > 0 => {
                let trimmed = line.trim();
                debug!("IMAP tentacle: CAPABILITY response: {}", trimmed);
                if trimmed.to_uppercase().contains("STARTTLS") {
                    has_starttls = true;
                }
                // Antwort endet mit * OK oder Tag OK
                if trimmed.ends_with(" OK") {
                    break;
                }
            }
            _ => {
                debug!("IMAP tentacle: CAPABILITY reading ended");
                break;
            }
        }
    }

    if !has_starttls {
        warn!("IMAP tentacle: Server does not advertise STARTTLS capability, attempting anyway");
    }

    // 4. STARTTLS-Befehl senden
    reader
        .get_mut()
        .write_all(b"STARTTLS\r\n")
        .await
        .with_context(|| "Failed to send STARTTLS command")?;
    reader
        .get_mut()
        .flush()
        .await
        .with_context(|| "Failed to flush after STARTTLS")?;

    // Antwort auf STARTTLS lesen (sollte "OK" sein)
    let mut response = String::new();
    reader
        .read_line(&mut response)
        .await
        .with_context(|| "Failed to read STARTTLS response")?;

    if !response.to_uppercase().contains("OK") {
        anyhow::bail!(
            "IMAP tentacle: STARTTLS not accepted by server: {}",
            response.trim()
        );
    }

    debug!("IMAP tentacle: STARTTLS accepted, upgrading to TLS");

    // BufReader entpacken, um den TcpStream für TLS-Upgrade zu erhalten
    let tcp_stream = reader.into_inner();

    // 5. TLS-Upgrade des Streams
    let connector = TlsConnector::from(Arc::new(tls_config.clone()));
    let tls_stream = connector
        .connect(server_name.clone(), tcp_stream)
        .await
        .with_context(|| format!("TLS handshake failed after STARTTLS for {}", host))?;

    debug!(
        "IMAP tentacle: TLS connection established via STARTTLS to {}",
        host
    );

    // 6. Client mit TLS-Stream erstellen und Login
    let client = Client::new(tls_stream);

    let session = client
        .login(username, password)
        .await
        .map_err(|(e, _client)| anyhow::anyhow!("IMAP login failed for user {}: {}", username, e))
        .with_context(|| format!("IMAP login failed for user {}", username))?;

    info!(
        "IMAP tentacle: Successfully connected via STARTTLS to {}",
        host
    );
    Ok(session)
}

/// Stellt eine implizite TLS-Verbindung her
async fn connect_implicit_tls(
    host: &str,
    port: u16,
    username: &str,
    password: &str,
    server_name: &ServerName<'static>,
    tls_config: &tokio_rustls::rustls::ClientConfig,
) -> Result<async_imap::Session<ImapStream>> {
    let _addr = format!("{}:{}", host, port);

    // TCP-Verbindung aufbauen
    let tcp_stream = TcpStream::connect(&_addr)
        .await
        .with_context(|| format!("Failed to connect to IMAP server at {}", _addr))?;

    // TLS-Verbindung herstellen
    let connector = TlsConnector::from(Arc::new(tls_config.clone()));

    let tls_stream = connector
        .connect(server_name.clone(), tcp_stream)
        .await
        .with_context(|| format!("TLS handshake failed for {}", host))?;

    debug!("IMAP tentacle: TLS connection established to {}", host);

    let client = Client::new(tls_stream);

    // Login
    let session = client
        .login(username, password)
        .await
        .map_err(|(e, _client)| anyhow::anyhow!("IMAP login failed for user {}: {}", username, e))
        .with_context(|| format!("IMAP login failed for user {}", username))?;

    Ok(session)
}

/// Verarbeitet eine einzelne E-Mail basierend auf UID und publish't sie als Ink
async fn process_email_by_uid(
    session: &mut async_imap::Session<ImapStream>,
    uid: u32,
    tentacle_name: &str,
    event_bus: &EventBus,
    tentacle: &ImapTentacle,
) -> Result<()> {
    // E-Mail-Rohdaten via UID FETCH abrufen
    let body: Vec<u8> = {
        let mut body_buf = Vec::new();
        let mut fetch_stream = session
            .uid_fetch(uid.to_string(), "RFC822")
            .await
            .with_context(|| format!("Failed to fetch message UID {}", uid))?;

        while let Some(result) = fetch_stream.next().await {
            let fetch_data =
                result.with_context(|| format!("Failed to read message UID {}", uid))?;
            if let Some(body_data) = fetch_data.body() {
                body_buf.extend_from_slice(body_data);
            }
        }
        body_buf
    };
    // fetch_stream wird hier automatisch dropped, session ist wieder frei

    if body.is_empty() {
        warn!(
            "IMAP tentacle '{}': message UID {} has no body",
            tentacle_name, uid
        );
        return Ok(());
    }

    // E-Mail mit mailparse parsen
    let parsed = mailparse::parse_mail(&body)
        .with_context(|| format!("Failed to parse message UID {}", uid))?;

    // Betreff und Absender extrahieren
    let headers = parsed.get_headers();
    let subject = headers.get_first_value("Subject").unwrap_or_default();

    let from = headers.get_first_value("From").unwrap_or_default();

    // Message-ID für Metadaten
    let message_id = headers.get_first_value("Message-ID").unwrap_or_default();

    // Absender-Mappings prüfen (haben Priorität)
    let topic = tentacle
        .check_sender_mapping(&from)
        .unwrap_or_else(|| tentacle.determine_topic(&subject, &from));

    // E-Mail-Body als Text extrahieren
    let email_body = extract_email_body(&parsed);

    // Ink erstellen
    let ink = Ink::new(topic.clone(), "imap", tentacle_name, email_body).with_metadata(
        serde_json::json!({
            "subject": subject,
            "from": from,
            "message_id": message_id,
            "uid": uid,
        }),
    );

    debug!(
        "IMAP tentacle '{}': processing message UID {} with topic '{}', subject: '{}'",
        tentacle_name, uid, topic, subject
    );
    event_bus.publish(ink);

    // E-Mail als gelesen markieren, falls konfiguriert (UID-basiert)
    if tentacle.config.mark_read
        && let Err(e) = session.uid_store(uid.to_string(), r"+FLAGS \\Seen").await
    {
        warn!(
            "IMAP tentacle '{}': failed to mark message UID {} as read: {}",
            tentacle_name, uid, e
        );
    }

    // E-Mail verschieben, falls konfiguriert (UID-basiert)
    if let Some(ref move_to) = tentacle.config.move_to
        && let Err(e) = session.uid_mv(uid.to_string(), move_to).await
    {
        warn!(
            "IMAP tentacle '{}': failed to move message UID {} to '{}': {}",
            tentacle_name, uid, move_to, e
        );
    }

    Ok(())
}

/// Extrahiert den Text-Body einer E-Mail
/// Bevorzugt text/plain, dann text/html, dann den ersten verfügbaren Text-Part
fn extract_email_body(parsed: &mailparse::ParsedMail) -> String {
    // Zuerst nach text/plain suchen
    let plain_body = extract_text_part(parsed, "text/plain");
    if !plain_body.is_empty() {
        return plain_body;
    }

    // Dann nach text/html suchen
    let html_body = extract_text_part(parsed, "text/html");
    if !html_body.is_empty() {
        return html_body;
    }

    // Fallback: gesamten Body verwenden
    if let Ok(body) = parsed.get_body()
        && !body.is_empty()
    {
        return body;
    }

    String::new()
}

/// Extrahiert einen Text-Part eines bestimmten MIME-Typs
fn extract_text_part(parsed: &mailparse::ParsedMail, target_mime: &str) -> String {
    // Rekursive Suche in den Parts
    for part in parsed.parts() {
        let mimetype = part.ctype.mimetype.to_lowercase();

        if mimetype == target_mime
            && let Ok(body) = part.get_body()
            && !body.is_empty()
        {
            return body;
        }

        // In verschachtelten Parts suchen (z.B. multipart/alternative)
        if !mimetype.starts_with("multipart/") {
            continue;
        }

        if let Ok(sub_body) = part.get_body()
            && !sub_body.is_empty()
        {
            // Versuche, den Sub-Part zu parsen
            if let Ok(sub_parsed) = mailparse::parse_mail(sub_body.as_bytes()) {
                let result = extract_text_part(&sub_parsed, target_mime);
                if !result.is_empty() {
                    return result;
                }
            }
        }
    }

    String::new()
}
