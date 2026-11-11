use anyhow::{Context, Result};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info, warn};

use mailparse::{MailHeaderMap, ParsedMail};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWrite, AsyncWriteExt, BufReader};
use tokio::net::TcpStream;
use tokio_rustls::{TlsConnector, rustls::pki_types::ServerName};

use super::Tentacle;
use crate::eventbus::EventBus;
use crate::model::Ink;

/// Persistente Speicherung bereits verarbeiteter Message-IDs
#[derive(Debug, Clone, Serialize, Deserialize)]
struct ProcessedIdsStorage {
    /// Menge der bereits verarbeiteten Message-IDs
    processed_ids: HashSet<String>,
    /// Anzahl der gespeicherten IDs (für Logging)
    count: u64,
}

impl ProcessedIdsStorage {
    /// Lädt die Speicherung aus einer Datei
    async fn load(path: &PathBuf) -> Result<Self> {
        if !path.exists() {
            return Ok(Self {
                processed_ids: HashSet::new(),
                count: 0,
            });
        }

        let content = tokio::fs::read_to_string(path).await.with_context(|| {
            format!(
                "Fehler beim Lesen der Message-ID-Speicherdatei: {}",
                path.display()
            )
        })?;

        serde_json::from_str(&content).with_context(|| {
            format!(
                "Fehler beim Parsen der Message-ID-Speicherdatei: {}",
                path.display()
            )
        })
    }

    /// Speichert die Speicherung in einer Datei
    async fn save(&mut self, path: &PathBuf) -> Result<()> {
        // Elternverzeichnis erstellen, falls nötig
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await.with_context(|| {
                format!(
                    "Fehler beim Erstellen des Verzeichnisses: {}",
                    parent.display()
                )
            })?;
        }

        // Begrenze die gespeicherten IDs auf die letzten 1000, um die Dateigröße zu kontrollieren
        if self.processed_ids.len() > 1000 {
            // Behalte nur die letzten 1000 IDs (nach alphabetischer Sortierung)
            let mut ids: Vec<String> = self.processed_ids.drain().collect();
            ids.sort();
            ids.truncate(1000);
            self.processed_ids = ids.into_iter().collect();
        }

        self.count = self.processed_ids.len() as u64;

        let content = serde_json::to_string_pretty(self)
            .with_context(|| "Fehler beim Serialisieren der Message-ID-Speicherung")?;

        tokio::fs::write(path, content).await.with_context(|| {
            format!(
                "Fehler beim Schreiben der Message-ID-Speicherdatei: {}",
                path.display()
            )
        })?;

        Ok(())
    }

    /// Prüft, ob eine Message-ID bereits verarbeitet wurde
    fn is_processed(&self, message_id: &str) -> bool {
        self.processed_ids.contains(message_id)
    }

    /// Markiert eine Message-ID als verarbeitet
    fn mark_processed(&mut self, message_id: String) {
        self.processed_ids.insert(message_id);
    }
}

/// Einfacher POP3-Client, der sowohl TLS als auch plain Verbindungen unterstützt
struct Pop3Client<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    reader: BufReader<tokio::io::ReadHalf<S>>,
    writer: tokio::io::WriteHalf<S>,
}

impl<S: AsyncRead + AsyncWrite + Unpin> Pop3Client<S> {
    /// Login-Prozess für POP3 (liest Welcome-Message)
    async fn login(&mut self, host: &str, username: &str, password: &str) -> Result<()> {
        let welcome = self.read_line().await?;
        if !welcome.starts_with("+OK") {
            anyhow::bail!("POP3-Server {} antwortete nicht mit +OK: {}", host, welcome);
        }

        self.login_no_welcome(host, username, password).await
    }

    /// Login ohne Welcome-Message (für STARTTLS, wo Welcome bereits gelesen wurde)
    async fn login_no_welcome(&mut self, host: &str, username: &str, password: &str) -> Result<()> {
        // Login
        self.send_command(&format!("USER {}", username)).await?;
        let _response = self.read_line().await?;

        self.send_command(&format!("PASS {}", password)).await?;
        let response = self.read_line().await?;
        if !response.starts_with("+OK") {
            anyhow::bail!("POP3-Login bei {} fehlgeschlagen: {}", host, response);
        }

        info!("POP3-Verbindung zu {} hergestellt", host);
        Ok(())
    }

    /// Sendet einen POP3-Befehl
    async fn send_command(&mut self, command: &str) -> Result<()> {
        debug!("POP3 CMD: {}", command);
        self.writer
            .write_all(format!("{}\r\n", command).as_bytes())
            .await?;
        self.writer.flush().await?;
        Ok(())
    }

    /// Liest eine Antwortzeile vom Server
    async fn read_line(&mut self) -> Result<String> {
        let mut line = String::new();
        self.reader.read_line(&mut line).await?;
        // Entferne \r\n
        line.truncate(line.len().saturating_sub(2));
        debug!("POP3 RESP: {}", line);
        Ok(line)
    }

    /// Führt STAT aus und gibt die Anzahl der Nachrichten zurück
    async fn stat(&mut self) -> Result<(u64, u64)> {
        self.send_command("STAT").await?;
        let response = self.read_line().await?;

        if !response.starts_with("+OK") {
            anyhow::bail!("STAT fehlgeschlagen: {}", response);
        }

        // Format: +OK <count> <octets>
        let parts: Vec<&str> = response[4..].split_whitespace().collect();
        if parts.len() < 2 {
            anyhow::bail!("Ungültige STAT-Antwort: {}", response);
        }

        let count: u64 = parts[0].parse().context("Count parsen fehlgeschlagen")?;
        let octets: u64 = parts[1].parse().context("Octets parsen fehlgeschlagen")?;

        Ok((count, octets))
    }

    /// Ruft eine Nachricht mit RETR ab
    async fn retr(&mut self, msg_num: u64) -> Result<Vec<u8>> {
        self.send_command(&format!("RETR {}", msg_num)).await?;
        let response = self.read_line().await?;

        if !response.starts_with("+OK") {
            anyhow::bail!("RETR {} fehlgeschlagen: {}", msg_num, response);
        }

        // Lese den Nachrichteninhalt bis zur dot-terminating sequence
        let mut raw = Vec::new();
        loop {
            let mut line = String::new();
            self.reader.read_line(&mut line).await?;

            // Dot-terminating sequence: einzelne Zeile mit nur "."
            if line == ".\r\n" {
                break;
            }

            // Unfolding: Entferne \r\n, aber füge den Rest hinzu
            let line_bytes = if line.ends_with("\r\n") {
                &line.as_bytes()[..line.len() - 2]
            } else {
                line.as_bytes()
            };
            raw.extend_from_slice(line_bytes);
            raw.push(b'\n');
        }

        Ok(raw)
    }

    /// Markiert eine Nachricht zum Löschen
    async fn dele(&mut self, msg_num: u64) -> Result<()> {
        self.send_command(&format!("DELE {}", msg_num)).await?;
        let response = self.read_line().await?;
        if !response.starts_with("+OK") {
            anyhow::bail!("DELE {} fehlgeschlagen: {}", msg_num, response);
        }
        Ok(())
    }

    /// Beendet die Session mit QUIT
    async fn quit(&mut self) -> Result<()> {
        self.send_command("QUIT").await?;
        let _response = self.read_line().await?;
        Ok(())
    }
}

/// Erstellt eine POP3-Verbindung mit TLS (implizit, Port 995)
async fn connect_pop3_tls(
    host: &str,
    port: u16,
    username: &str,
    password: &str,
) -> Result<Pop3Client<tokio_rustls::client::TlsStream<TcpStream>>> {
    let server_name = ServerName::try_from(host.to_string())
        .context("Ungültiger Hostname für TLS")?
        .to_owned();

    let tcp_stream = TcpStream::connect(format!("{}:{}", host, port))
        .await
        .context(format!("Verbindung zu {}:{} fehlgeschlagen", host, port))?;

    let mut roots = tokio_rustls::rustls::RootCertStore::empty();
    for cert in rustls_native_certs::load_native_certs().certs {
        roots.add(cert).ok();
    }
    let tls_connector = TlsConnector::from(Arc::new(
        tokio_rustls::rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth(),
    ));

    let tls_stream = tls_connector
        .connect(server_name, tcp_stream)
        .await
        .context("TLS-Handshake fehlgeschlagen")?;

    let (read_half, write_half) = tokio::io::split(tls_stream);
    let mut client = Pop3Client {
        reader: BufReader::new(read_half),
        writer: write_half,
    };

    client.login(host, username, password).await?;
    Ok(client)
}

/// Erstellt eine POP3-Verbindung mit STARTTLS (Port 110)
/// 1. TCP-Verbindung aufbauen
/// 2. Welcome-Message lesen
/// 3. STLS-Befehl senden
/// 4. TLS-Upgrade des Streams
/// 5. Login
async fn connect_pop3_starttls(
    host: &str,
    port: u16,
    username: &str,
    password: &str,
) -> Result<Pop3Client<tokio_rustls::client::TlsStream<TcpStream>>> {
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader as TokioBufReader};

    let addr = format!("{}:{}", host, port);

    // 1. TCP-Verbindung aufbauen
    let tcp_stream = TcpStream::connect(&addr)
        .await
        .context(format!("Verbindung zu {} fehlgeschlagen", addr))?;

    // BufReader für zeilenweises Lesen
    let mut reader = TokioBufReader::new(tcp_stream);

    // 2. Welcome-Message lesen
    let mut welcome = String::new();
    reader.read_line(&mut welcome).await?;
    let welcome_trimmed = welcome.trim();
    debug!("POP3 Welcome: {}", welcome_trimmed);

    if !welcome_trimmed.starts_with("+OK") {
        anyhow::bail!(
            "POP3-Server {} antwortete nicht mit +OK: {}",
            host,
            welcome_trimmed
        );
    }

    // 3. STLS-Befehl senden
    reader
        .get_mut()
        .write_all(b"STLS\r\n")
        .await
        .context("Fehler beim Senden von STLS")?;
    reader
        .get_mut()
        .flush()
        .await
        .context("Fehler beim Flush nach STLS")?;

    // STLS-Antwort lesen
    let mut stls_response = String::new();
    reader.read_line(&mut stls_response).await?;
    let stls_trimmed = stls_response.trim();
    debug!("POP3 STLS response: {}", stls_trimmed);

    if !stls_trimmed.starts_with("+OK") {
        anyhow::bail!("POP3-Server akzeptiert STARTTLS nicht: {}", stls_trimmed);
    }

    // BufReader entpacken, um den TcpStream für TLS-Upgrade zu erhalten
    let tcp_stream = reader.into_inner();

    // 4. TLS-Upgrade des kompletten Streams
    let server_name = ServerName::try_from(host.to_string())
        .context("Ungültiger Hostname für TLS")?
        .to_owned();

    let mut roots = tokio_rustls::rustls::RootCertStore::empty();
    for cert in rustls_native_certs::load_native_certs().certs {
        roots.add(cert).ok();
    }
    let tls_connector = TlsConnector::from(Arc::new(
        tokio_rustls::rustls::ClientConfig::builder()
            .with_root_certificates(roots)
            .with_no_client_auth(),
    ));

    // TLS-Handshake mit dem kompletten Stream
    let tls_stream = tls_connector
        .connect(server_name, tcp_stream)
        .await
        .context("TLS-Handshake nach STARTTLS fehlgeschlagen")?;

    info!("POP3-STARTTLS Verbindung zu {} hergestellt", host);

    // 5. Login mit TLS-Stream
    let (tls_read_half, tls_write_half) = tokio::io::split(tls_stream);
    let mut client = Pop3Client {
        reader: BufReader::new(tls_read_half),
        writer: tls_write_half,
    };

    // Login ohne Welcome-Reading (bereits gelesen)
    client.login_no_welcome(host, username, password).await?;
    Ok(client)
}

/// Erstellt eine POP3-Verbindung ohne TLS (plain, Port 110)
#[allow(dead_code)]
async fn connect_pop3_plain(
    host: &str,
    port: u16,
    username: &str,
    password: &str,
) -> Result<Pop3Client<TcpStream>> {
    let tcp_stream = TcpStream::connect(format!("{}:{}", host, port))
        .await
        .context(format!("Verbindung zu {}:{} fehlgeschlagen", host, port))?;

    let (read_half, write_half) = tokio::io::split(tcp_stream);
    let mut client = Pop3Client {
        reader: BufReader::new(read_half),
        writer: write_half,
    };

    client.login(host, username, password).await?;
    Ok(client)
}

/// Konfiguration für das Topic-Matching bei E-Mails
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "mode")]
pub enum Pop3TopicMode {
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
pub struct Pop3TentacleConfig {
    /// POP3-Server-Host
    pub host: String,
    /// POP3-Port (Standard: 995 für SSL/TLS, 110 für plain)
    #[serde(default = "default_port")]
    pub port: u16,
    /// Benutzername
    pub username: String,
    /// Passwort (oder Umgebungsvariable-Referenz wie "${POP3_PASSWORD}")
    pub password: String,
    /// Topic-Matching-Modus
    pub topic_mode: Pop3TopicMode,
    /// Optional: Zusätzliche Topic-Mappings basierend auf Absender
    #[serde(default)]
    pub sender_mappings: Option<HashMap<String, String>>,
    /// Intervall in Sekunden zwischen den Polls (Standard: 30)
    #[serde(default = "default_poll_interval")]
    pub poll_interval: u64,
    /// Optional: E-Mails nach Verarbeitung löschen (Standard: false)
    /// POP3 unterstützt kein "als gelesen markieren", daher ist Löschen die einzige Option
    #[serde(default)]
    pub delete_after_processing: bool,
    /// Optional: STARTTLS verwenden (Port 110) statt implizite TLS (Port 995)
    /// Standard: false (implizite TLS für Port 995)
    #[serde(default)]
    pub starttls: bool,
    /// Optional: Pfad zur Datei für persistente Message-ID-Speicherung
    /// Standard: "data/pop3_<name>_ids.json"
    #[serde(default)]
    pub id_storage_path: Option<String>,
    /// Optional: Fallback-Topic, wenn Betreff nicht mit "/" beginnt (nur im Subject-Modus)
    #[serde(default = "default_fallback_topic")]
    pub fallback_topic: String,
}

fn default_port() -> u16 {
    995
}

fn default_poll_interval() -> u64 {
    30
}

fn default_fallback_topic() -> String {
    "/email/default".to_string()
}

impl Pop3TentacleConfig {
    /// Ersetzt Umgebungsvariable-Referenzen im Passwort
    fn resolve_password(&self) -> String {
        if self.password.starts_with("${") && self.password.ends_with('}') {
            let var_name = self.password.trim_start_matches("${").trim_end_matches('}');
            std::env::var(var_name).unwrap_or_else(|_| {
                warn!(
                    "Umgebungsvariable '{}' nicht gefunden, verwende literalen Wert",
                    var_name
                );
                self.password.clone()
            })
        } else {
            self.password.clone()
        }
    }

    /// Gibt den Pfad zur Message-ID-Speicherdatei zurück
    fn get_id_storage_path(&self, tentacle_name: &str) -> PathBuf {
        if let Some(ref path) = self.id_storage_path {
            PathBuf::from(path)
        } else {
            PathBuf::from(format!("data/pop3_{}_ids.json", tentacle_name))
        }
    }
}

pub struct Pop3Tentacle {
    name: String,
    config: Pop3TentacleConfig,
}

impl Pop3Tentacle {
    pub fn new(name: String, config: Pop3TentacleConfig) -> Self {
        Self { name, config }
    }

    /// Extrahiert den E-Mail-Body aus den rohen Bytes
    fn extract_body(&self, parsed: &ParsedMail) -> String {
        // Zuerst nach text/plain suchen
        let plain_body = Self::extract_text_part(parsed, "text/plain");
        if !plain_body.is_empty() {
            return plain_body;
        }

        // Dann nach text/html suchen
        let html_body = Self::extract_text_part(parsed, "text/html");
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
    fn extract_text_part(parsed: &ParsedMail, target_mime: &str) -> String {
        for part in parsed.parts() {
            let mimetype = part.ctype.mimetype.to_lowercase();

            if mimetype == target_mime
                && let Ok(body) = part.get_body()
                && !body.is_empty()
            {
                return body;
            }

            // In verschachtelten Parts suchen
            if !mimetype.starts_with("multipart/") {
                continue;
            }

            if let Ok(sub_body) = part.get_body()
                && !sub_body.is_empty()
                && let Ok(sub_parsed) = mailparse::parse_mail(sub_body.as_bytes())
            {
                let result = Self::extract_text_part(&sub_parsed, target_mime);
                if !result.is_empty() {
                    return result;
                }
            }
        }

        String::new()
    }

    /// Bestimmt das Topic basierend auf der Konfiguration und den E-Mail-Headern
    fn determine_topic(&self, parsed: &ParsedMail) -> String {
        let headers = parsed.get_headers();

        match &self.config.topic_mode {
            Pop3TopicMode::Subject => {
                let subject = headers.get_first_value("Subject").unwrap_or_default();

                if subject.starts_with('/') {
                    subject.trim_end_matches('/').to_string()
                } else {
                    // Fallback-Topic verwenden
                    debug!(
                        "[{}] Betreff beginnt nicht mit '/', verwende Fallback-Topic: {}",
                        self.name, self.config.fallback_topic
                    );
                    self.config.fallback_topic.clone()
                }
            }
            Pop3TopicMode::Sender {
                sender_pattern,
                topic,
            } => {
                let from = headers.get_first_value("From").unwrap_or_default();

                // Einfacher Platzhalter-Vergleich
                if Self::matches_pattern(&from, sender_pattern) {
                    topic.clone()
                } else {
                    // Fallback-Topic verwenden
                    debug!(
                        "[{}] Absender '{}' passt nicht zu '{}', verwende Fallback-Topic",
                        self.name, from, sender_pattern
                    );
                    self.config.fallback_topic.clone()
                }
            }
            Pop3TopicMode::All { topic } => topic.clone(),
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
                if Self::matches_pattern(from, pattern) {
                    return Some(topic.clone());
                }
            }
        }
        None
    }

    /// Prüft, ob ein String einem Muster mit Platzhaltern entspricht
    fn matches_pattern(text: &str, pattern: &str) -> bool {
        // Einfacher Platzhalter-Vergleich: * entspricht beliebig vielen Zeichen
        if pattern.contains('*') {
            let parts: Vec<&str> = pattern.splitn(2, '*').collect();
            let prefix = parts[0];
            let suffix = if parts.len() > 1 { parts[1] } else { "" };
            text.starts_with(prefix) && text.ends_with(suffix)
        } else {
            text == pattern
        }
    }

    /// Extrahiert die Message-ID aus den E-Mail-Headern
    fn extract_message_id(parsed: &ParsedMail) -> String {
        let headers = parsed.get_headers();
        // Versuche, die Message-ID zu extrahieren
        if let Some(message_id) = headers.get_first_value("Message-ID")
            && !message_id.is_empty()
        {
            return message_id;
        }
        // Fallback: erstelle eine ID aus Subject, From, und Date
        let subject = headers.get_first_value("Subject").unwrap_or_default();
        let from = headers.get_first_value("From").unwrap_or_default();
        let date = headers.get_first_value("Date").unwrap_or_default();
        format!("fallback:{}:{}:{}", subject, from, date)
    }

    /// Verarbeitet neue E-Mails via POP3
    async fn fetch_and_process_emails(
        &self,
        event_bus: Arc<EventBus>,
        uid_storage: &mut ProcessedIdsStorage,
        uid_storage_path: &PathBuf,
    ) -> Result<()> {
        info!("[{}] E-Mails via POP3 abrufen...", self.name);

        let password = self.config.resolve_password();

        // POP3-Session erstellen (entweder TLS, STARTTLS oder plain)
        let (count, processed_count) = if self.config.starttls {
            // STARTTLS-Modus: TCP-Verbindung, STLS-Befehl, TLS-Upgrade
            info!("[{}] Verwende STARTTLS für POP3-Verbindung", self.name);
            let mut client = connect_pop3_starttls(
                &self.config.host,
                self.config.port,
                &self.config.username,
                &password,
            )
            .await
            .context("POP3-STARTTLS-Verbindung fehlgeschlagen")?;

            let (count, processed) = self
                .process_emails_with_client(&mut client, &event_bus, uid_storage)
                .await?;
            client.quit().await?;
            (count, processed)
        } else {
            // TLS-Verbindung (Standard)
            let mut client = connect_pop3_tls(
                &self.config.host,
                self.config.port,
                &self.config.username,
                &password,
            )
            .await
            .context("POP3-Verbindung fehlgeschlagen")?;

            let (count, processed) = self
                .process_emails_with_client(&mut client, &event_bus, uid_storage)
                .await?;
            client.quit().await?;
            (count, processed)
        };

        // UID-Speicherung aktualisieren
        if !processed_count.is_empty() {
            uid_storage.save(uid_storage_path).await?;
            let processed_len = processed_count.len() as u64;
            info!(
                "[{}] {} neue E-Mails verarbeitet, {} bereits verarbeitet",
                self.name,
                processed_len,
                count.saturating_sub(processed_len)
            );
        } else {
            info!("[{}] Keine neuen E-Mails", self.name);
        }

        info!("[{}] POP3-Sitzung beendet", self.name);
        Ok(())
    }

    /// Verarbeitet E-Mails mit einem gegebenen Client
    async fn process_emails_with_client<S: AsyncRead + AsyncWrite + Unpin>(
        &self,
        client: &mut Pop3Client<S>,
        event_bus: &EventBus,
        uid_storage: &mut ProcessedIdsStorage,
    ) -> Result<(u64, Vec<String>)> {
        // Anzahl der Nachrichten abrufen
        let (count, _octets) = client.stat().await.context("STAT-Befehl fehlgeschlagen")?;

        if count == 0 {
            info!("[{}] Keine neuen E-Mails", self.name);
            return Ok((0, Vec::new()));
        }

        info!("[{}] {} E-Mails gefunden", self.name, count);
        let mut processed_ids = Vec::new();

        // Jede Nachricht verarbeiten
        for i in 1..=count {
            debug!("[{}] Verarbeite Nachricht {}/{}", self.name, i, count);

            // Nachricht abrufen
            let raw = match client.retr(i).await {
                Ok(r) => r,
                Err(e) => {
                    warn!(
                        "[{}] Nachricht {} abrufen fehlgeschlagen: {}",
                        self.name, i, e
                    );
                    continue;
                }
            };

            // E-Mail parsen
            let parsed = match mailparse::parse_mail(&raw) {
                Ok(p) => p,
                Err(e) => {
                    warn!(
                        "[{}] E-Mail-Parsing für Nachricht {} fehlgeschlagen: {}",
                        self.name, i, e
                    );
                    continue;
                }
            };

            // Message-ID extrahieren
            let message_id = Self::extract_message_id(&parsed);

            // Prüfen, ob bereits verarbeitet
            if uid_storage.is_processed(&message_id) {
                debug!(
                    "[{}] Nachricht {} bereits verarbeitet (Message-ID: {})",
                    self.name, i, message_id
                );
                continue;
            }

            // Absender-Mappings prüfen (haben Priorität)
            let headers = parsed.get_headers();
            let from = headers.get_first_value("From").unwrap_or_default();

            let topic = self
                .check_sender_mapping(&from)
                .unwrap_or_else(|| self.determine_topic(&parsed));

            // Body extrahieren
            let body = self.extract_body(&parsed);

            // Metadaten extrahieren
            let subject = headers.get_first_value("Subject").unwrap_or_default();
            let date = headers.get_first_value("Date").unwrap_or_default();

            // Ink erstellen
            let ink = Ink::new(topic.clone(), "pop3", format!("pop3_{}", self.name), body)
                .with_metadata(serde_json::json!({
                    "from": from,
                    "subject": subject,
                    "date": date,
                    "message_id": message_id,
                }));

            // An Event Bus senden
            event_bus.publish(ink);
            uid_storage.mark_processed(message_id.clone());
            processed_ids.push(message_id);

            info!(
                "[{}] Nachricht {} verarbeitet: Topic '{}'",
                self.name, i, topic
            );
        }

        // Optional: Verarbeitete Nachrichten löschen
        if self.config.delete_after_processing {
            warn!("[{}] Lösche verarbeitete Nachrichten...", self.name);
            for i in 1..=count {
                if let Err(e) = client.dele(i).await {
                    error!(
                        "[{}] Nachricht {} löschen fehlgeschlagen: {}",
                        self.name, i, e
                    );
                }
            }
        }

        Ok((count, processed_ids))
    }
}

#[async_trait]
impl Tentacle for Pop3Tentacle {
    fn name(&self) -> &str {
        &self.name
    }

    fn r#type(&self) -> &str {
        "pop3"
    }

    async fn start(&self, event_bus: Arc<EventBus>) -> Result<()> {
        info!("[{}] POP3-Tentacle gestartet", self.name);

        // Message-ID-Speicherung laden
        let id_storage_path = self.config.get_id_storage_path(&self.name);
        let mut id_storage = ProcessedIdsStorage::load(&id_storage_path)
            .await
            .with_context(|| "Fehler beim Laden der Message-ID-Speicherung")?;

        info!(
            "[{}] {} bereits verarbeitete Message-IDs geladen",
            self.name, id_storage.count
        );

        loop {
            if let Err(e) = self
                .fetch_and_process_emails(event_bus.clone(), &mut id_storage, &id_storage_path)
                .await
            {
                error!("[{}] Fehler beim Abrufen der E-Mails: {}", self.name, e);
            }

            tokio::time::sleep(Duration::from_secs(self.config.poll_interval)).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_matches_pattern_exact() {
        assert!(Pop3Tentacle::matches_pattern(
            "user@example.com",
            "user@example.com"
        ));
        assert!(!Pop3Tentacle::matches_pattern(
            "other@example.com",
            "user@example.com"
        ));
    }

    #[test]
    fn test_matches_pattern_wildcard() {
        assert!(Pop3Tentacle::matches_pattern(
            "user@example.com",
            "*@example.com"
        ));
        assert!(Pop3Tentacle::matches_pattern(
            "admin@example.com",
            "*@example.com"
        ));
        assert!(!Pop3Tentacle::matches_pattern(
            "user@other.com",
            "*@example.com"
        ));
    }

    #[test]
    fn test_matches_pattern_prefix() {
        assert!(Pop3Tentacle::matches_pattern("user@example.com", "user*"));
        assert!(Pop3Tentacle::matches_pattern(
            "user123@example.com",
            "user*"
        ));
        assert!(!Pop3Tentacle::matches_pattern("admin@example.com", "user*"));
    }

    #[test]
    fn test_processed_ids_storage() {
        // Teste die ProcessedIdsStorage-Struktur
        let mut storage = ProcessedIdsStorage {
            processed_ids: HashSet::new(),
            count: 0,
        };

        assert!(!storage.is_processed("msg-1"));

        storage.mark_processed("msg-1".to_string());
        assert!(storage.is_processed("msg-1"));
        assert!(!storage.is_processed("msg-2"));
    }
}
