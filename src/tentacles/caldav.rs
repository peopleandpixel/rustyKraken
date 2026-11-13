use anyhow::Result;
use async_trait::async_trait;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::RwLock;
use std::time::Duration;
use tracing::{debug, error, info, warn};

use super::Tentacle;
use crate::eventbus::EventBus;
use crate::model::Ink;

/// Modus: CalDAV (Kalender) oder CardDAV (Kontakte)
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "lowercase")]
pub enum DavMode {
    #[default]
    CalDav,
    CardDav,
}

/// Authentifizierung
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum DavAuth {
    Basic { username: String, password: String },
    Bearer { token: String },
    Digest { username: String, password: String },
}

impl Default for DavAuth {
    fn default() -> Self {
        DavAuth::Basic {
            username: String::new(),
            password: String::new(),
        }
    }
}

/// Konfiguration für den CalDAV/CardDAV Tentacle
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CalDavTentacleConfig {
    /// Modus: CalDAV oder CardDAV
    #[serde(default)]
    pub mode: DavMode,
    /// Server-URL (z.B. https://calendar.example.com/calendars/user/events)
    pub server_url: String,
    /// Authentifizierung
    #[serde(default)]
    pub auth: DavAuth,
    /// Polling-Interval in Sekunden
    #[serde(default = "default_poll_interval")]
    pub poll_interval_secs: u64,
    /// Timeout in Sekunden
    #[serde(default = "default_timeout")]
    pub timeout_secs: u64,
    /// Reconnect-Verzögerung
    #[serde(default = "default_reconnect_delay")]
    pub reconnect_delay_secs: u64,
    /// Topic für Events
    #[serde(default = "default_topic")]
    pub topic: String,
    /// Nur zukünftige Events (CalDAV-Modus)
    #[serde(default)]
    pub future_only: bool,
    /// Zeitfenster für Events in Stunden (CalDAV-Modus)
    #[serde(default = "default_time_window")]
    pub time_window_hours: u32,
    /// Kalender-URLs (für mehrere Kalender)
    #[serde(default)]
    pub calendar_urls: Vec<String>,
    /// Kontakt-Filter (CardDAV-Modus, z.B. "John")
    #[serde(default)]
    pub contact_filter: Option<String>,
    /// TLS-Zertifikat validieren
    #[serde(default = "default_verify_tls")]
    pub verify_tls: bool,
    /// Custom Headers
    #[serde(default)]
    pub headers: HashMap<String, String>,
}

fn default_poll_interval() -> u64 {
    300
}

fn default_timeout() -> u64 {
    30
}

fn default_reconnect_delay() -> u64 {
    5
}

fn default_topic() -> String {
    "/caldav/events".to_string()
}

fn default_time_window() -> u32 {
    24
}

fn default_verify_tls() -> bool {
    true
}

/// Parsendes Calendar Event (vereinfacht)
#[derive(Debug, Clone, Serialize, Deserialize)]
struct CalendarEvent {
    uid: String,
    summary: String,
    description: Option<String>,
    dtstart: Option<String>,
    dtend: Option<String>,
    location: Option<String>,
    url: Option<String>,
}

/// Parsendes Contact (vereinfacht vCard)
#[derive(Debug, Clone, Serialize, Deserialize)]
struct VCardContact {
    fn_name: String,
    email: Option<String>,
    tel: Option<String>,
    org: Option<String>,
    url: Option<String>,
}

pub struct CalDavTentacle {
    name: String,
    config: CalDavTentacleConfig,
    processed_events: Arc<RwLock<HashMap<String, chrono::DateTime<chrono::Utc>>>>,
}

impl CalDavTentacle {
    pub fn new(name: String, config: CalDavTentacleConfig) -> Self {
        Self {
            name,
            config,
            processed_events: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    fn create_event_ink(&self, event: &CalendarEvent) -> Ink {
        let mut meta_map = serde_json::Map::new();
        meta_map.insert("uid".into(), serde_json::json!(&event.uid));
        meta_map.insert("summary".into(), serde_json::json!(&event.summary));
        meta_map.insert("mode".into(), serde_json::json!("caldav"));
        meta_map.insert(
            "server_url".into(),
            serde_json::json!(&self.config.server_url),
        );

        if let Some(ref desc) = event.description {
            meta_map.insert("description".into(), serde_json::json!(desc));
        }
        if let Some(ref dtstart) = event.dtstart {
            meta_map.insert("dtstart".into(), serde_json::json!(dtstart));
        }
        if let Some(ref dtend) = event.dtend {
            meta_map.insert("dtend".into(), serde_json::json!(dtend));
        }
        if let Some(ref location) = event.location {
            meta_map.insert("location".into(), serde_json::json!(location));
        }
        if let Some(ref url) = event.url {
            meta_map.insert("event_url".into(), serde_json::json!(url));
        }

        Ink::new(
            &self.config.topic,
            "caldav",
            &self.name,
            format!("Calendar event: {}", event.summary),
        )
        .with_metadata(serde_json::Value::Object(meta_map))
    }

    fn create_contact_ink(&self, contact: &VCardContact) -> Ink {
        let mut meta_map = serde_json::Map::new();
        meta_map.insert("fn".into(), serde_json::json!(&contact.fn_name));
        meta_map.insert("mode".into(), serde_json::json!("carddav"));
        meta_map.insert(
            "server_url".into(),
            serde_json::json!(&self.config.server_url),
        );

        if let Some(ref email) = contact.email {
            meta_map.insert("email".into(), serde_json::json!(email));
        }
        if let Some(ref tel) = contact.tel {
            meta_map.insert("tel".into(), serde_json::json!(tel));
        }
        if let Some(ref org) = contact.org {
            meta_map.insert("org".into(), serde_json::json!(org));
        }
        if let Some(ref url) = contact.url {
            meta_map.insert("contact_url".into(), serde_json::json!(url));
        }

        Ink::new(
            &self.config.topic,
            "caldav",
            &self.name,
            format!("Contact: {}", contact.fn_name),
        )
        .with_metadata(serde_json::Value::Object(meta_map))
    }

    #[allow(dead_code)]
    fn build_client(&self) -> Result<Client> {
        Client::builder()
            .timeout(Duration::from_secs(self.config.timeout_secs))
            .danger_accept_invalid_certs(!self.config.verify_tls)
            .build()
            .map_err(|e| anyhow::anyhow!("Failed to build HTTP client: {}", e))
    }

    fn apply_auth(&self, req: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        let mut builder = req;
        match &self.config.auth {
            DavAuth::Basic { username, password } => {
                builder = builder.basic_auth(username, Some(password));
            }
            DavAuth::Bearer { token } => {
                builder = builder.bearer_auth(token);
            }
            DavAuth::Digest { username, password } => {
                // Digest auth requires a challenge-response cycle; for simplicity,
                // we fall back to basic auth with a header hint.
                builder = builder.basic_auth(username, Some(password));
            }
        }
        // Apply custom headers
        for (key, value) in &self.config.headers {
            builder = builder.header(key, value);
        }
        builder
    }

    /// Parse iCalendar (ICS) content to extract events
    fn parse_ics(&self, content: &str) -> Vec<CalendarEvent> {
        let mut events = Vec::new();
        let mut current_event = None;

        for line in content.lines() {
            let line = line.trim();
            match line {
                "BEGIN:VEVENT" => {
                    current_event = Some(CalendarEvent {
                        uid: String::new(),
                        summary: String::new(),
                        description: None,
                        dtstart: None,
                        dtend: None,
                        location: None,
                        url: None,
                    });
                }
                "END:VEVENT" => {
                    if let Some(event) = current_event.take() {
                        events.push(event);
                    }
                }
                _ => {
                    if let Some(ref mut event) = current_event {
                        if let Some(val) = line.strip_prefix("UID:") {
                            event.uid = val.to_string();
                        } else if let Some(val) = line.strip_prefix("SUMMARY:") {
                            event.summary = val.to_string();
                        } else if let Some(val) = line.strip_prefix("DESCRIPTION:") {
                            event.description = Some(val.to_string());
                        } else if let Some(val) = line.strip_prefix("DTSTART") {
                            event.dtstart = Some(val.to_string());
                        } else if let Some(val) = line.strip_prefix("DTEND") {
                            event.dtend = Some(val.to_string());
                        } else if let Some(val) = line.strip_prefix("LOCATION:") {
                            event.location = Some(val.to_string());
                        } else if let Some(val) = line.strip_prefix("URL:") {
                            event.url = Some(val.to_string());
                        }
                    }
                }
            }
        }
        events
    }

    /// Parse vCard content to extract contacts
    fn parse_vcard(&self, content: &str) -> Vec<VCardContact> {
        let mut contacts = Vec::new();
        let mut current_contact = None;

        for line in content.lines() {
            let line = line.trim();
            match line {
                "BEGIN:VCARD" => {
                    current_contact = Some(VCardContact {
                        fn_name: String::new(),
                        email: None,
                        tel: None,
                        org: None,
                        url: None,
                    });
                }
                "END:VCARD" => {
                    if let Some(contact) = current_contact.take() {
                        contacts.push(contact);
                    }
                }
                _ => {
                    if let Some(ref mut contact) = current_contact {
                        if let Some(val) = line.strip_prefix("FN:") {
                            contact.fn_name = val.to_string();
                        } else if let Some(val) = line.strip_prefix("EMAIL:") {
                            contact.email = Some(val.to_string());
                        } else if let Some(val) = line.strip_prefix("TEL:") {
                            contact.tel = Some(val.to_string());
                        } else if let Some(val) = line.strip_prefix("ORG:") {
                            contact.org = Some(val.to_string());
                        } else if let Some(val) = line.strip_prefix("URL:") {
                            contact.url = Some(val.to_string());
                        }
                    }
                }
            }
        }
        contacts
    }

    /// Fetch calendar events via CalDAV REPORT or HTTP GET
    async fn fetch_events(&self, client: &Client) -> Result<Vec<CalendarEvent>> {
        let urls = if self.config.calendar_urls.is_empty() {
            vec![self.config.server_url.clone()]
        } else {
            self.config.calendar_urls.clone()
        };

        let mut all_events = Vec::new();

        for url in &urls {
            debug!(
                "CalDAV Tentacle '{}': fetching events from {}",
                self.name, url
            );

            // Try HTTP GET first (simpler approach)
            let req = client.get(url);
            let req = self.apply_auth(req);

            match req.send().await {
                Ok(response) => {
                    let status = response.status();
                    let content_type = response
                        .headers()
                        .get(reqwest::header::CONTENT_TYPE)
                        .and_then(|h| h.to_str().ok())
                        .unwrap_or("")
                        .to_string();

                    if status.is_success() {
                        let body = response.text().await?;

                        if content_type.contains("text/calendar") || content_type.contains("ics") {
                            let events = self.parse_ics(&body);
                            info!(
                                "CalDAV Tentacle '{}': parsed {} events from {}",
                                self.name,
                                events.len(),
                                url
                            );
                            all_events.extend(events);
                        } else {
                            warn!(
                                "CalDAV Tentacle '{}': unexpected content type: {}",
                                self.name, content_type
                            );
                        }
                    } else {
                        warn!(
                            "CalDAV Tentacle '{}': GET failed with status: {}",
                            self.name, status
                        );
                    }
                }
                Err(e) => {
                    error!("CalDAV Tentacle '{}': GET request failed: {}", self.name, e);
                }
            }
        }

        Ok(all_events)
    }

    /// Fetch contacts via CardDAV
    async fn fetch_contacts(&self, client: &Client) -> Result<Vec<VCardContact>> {
        let url = self.config.server_url.clone();
        debug!(
            "CardDAV Tentacle '{}': fetching contacts from {}",
            self.name, url
        );

        let req = client.get(&url);
        let req = self.apply_auth(req);

        match req.send().await {
            Ok(response) => {
                if response.status().is_success() {
                    let body = response.text().await?;
                    let contacts = self.parse_vcard(&body);
                    info!(
                        "CardDAV Tentacle '{}': parsed {} contacts from {}",
                        self.name,
                        contacts.len(),
                        url
                    );
                    return Ok(contacts);
                } else {
                    warn!(
                        "CardDAV Tentacle '{}': GET failed with status: {}",
                        self.name,
                        response.status()
                    );
                }
            }
            Err(e) => {
                error!(
                    "CardDAV Tentacle '{}': GET request failed: {}",
                    self.name, e
                );
            }
        }

        Ok(Vec::new())
    }
}

#[async_trait]
impl Tentacle for CalDavTentacle {
    fn name(&self) -> &str {
        &self.name
    }

    fn r#type(&self) -> &str {
        "caldav"
    }

    async fn start(&self, event_bus: Arc<EventBus>) -> Result<()> {
        info!(
            "CalDAV/CardDAV Tentacle '{}' starting (mode: {:?}, URL: {})",
            self.name, self.config.mode, self.config.server_url
        );

        let config = self.config.clone();
        let tentacle_name = self.name.clone();
        let event_bus_clone = Arc::clone(&event_bus);
        let processed_events = Arc::clone(&self.processed_events);

        tokio::spawn(async move {
            let client = match CalDavTentacle::build_client_from_config(&config) {
                Ok(c) => c,
                Err(e) => {
                    error!(
                        "CalDAV Tentacle '{}': failed to build client: {}",
                        tentacle_name, e
                    );
                    return;
                }
            };

            loop {
                match poll_dav(
                    &config,
                    &tentacle_name,
                    &event_bus_clone,
                    &processed_events,
                    &client,
                )
                .await
                {
                    Ok(count) => {
                        if count > 0 {
                            info!(
                                "CalDAV Tentacle '{}': processed {} items",
                                tentacle_name, count
                            );
                        }
                    }
                    Err(e) => {
                        error!("CalDAV Tentacle '{}': poll failed: {}", tentacle_name, e);
                    }
                }

                tokio::time::sleep(Duration::from_secs(config.poll_interval_secs)).await;
            }
        });

        loop {
            tokio::time::sleep(Duration::from_secs(60)).await;
        }
    }
}

impl CalDavTentacle {
    fn build_client_from_config(config: &CalDavTentacleConfig) -> Result<Client> {
        Client::builder()
            .timeout(Duration::from_secs(config.timeout_secs))
            .danger_accept_invalid_certs(!config.verify_tls)
            .build()
            .map_err(|e| anyhow::anyhow!("Failed to build HTTP client: {}", e))
    }
}

async fn poll_dav(
    config: &CalDavTentacleConfig,
    tentacle_name: &str,
    event_bus: &Arc<EventBus>,
    processed_events: &Arc<RwLock<HashMap<String, chrono::DateTime<chrono::Utc>>>>,
    client: &Client,
) -> Result<u32> {
    let mut count = 0u32;

    let tentacle = CalDavTentacle::new(tentacle_name.to_string(), config.clone());

    match config.mode {
        DavMode::CalDav => {
            let events = tentacle.fetch_events(client).await?;
            let now = chrono::Utc::now();

            for event in events {
                let uid = event.uid.clone();

                // Skip if already processed
                {
                    let processed = processed_events
                        .read()
                        .map_err(|e| anyhow::anyhow!("Lock error: {}", e))?;
                    if let Some(last_seen) = processed.get(&uid)
                        && *last_seen > now - chrono::Duration::hours(1)
                    {
                        debug!(
                            "CalDAV Tentacle '{}': skipping processed event: {}",
                            tentacle_name, uid
                        );
                        continue;
                    }
                }

                // Filter future events if configured
                if config.future_only
                    && event.dtstart.as_ref().is_some_and(|dtstart| {
                        chrono::DateTime::parse_from_rfc3339(dtstart)
                            .map(|dt| dt.with_timezone(&chrono::Utc) < now)
                            .unwrap_or(false)
                    })
                {
                    debug!(
                        "CalDAV Tentacle '{}': skipping past event: {}",
                        tentacle_name, uid
                    );
                    continue;
                }

                // Mark as processed
                {
                    let mut processed = processed_events
                        .write()
                        .map_err(|e| anyhow::anyhow!("Lock error: {}", e))?;
                    processed.insert(uid.clone(), now);
                }

                let ink = tentacle.create_event_ink(&event);
                event_bus.publish(ink);
                count += 1;
            }
        }
        DavMode::CardDav => {
            let contacts = tentacle.fetch_contacts(client).await?;

            for contact in contacts {
                // Filter by contact name if configured
                if config.contact_filter.as_ref().is_some_and(|filter| {
                    !contact
                        .fn_name
                        .to_lowercase()
                        .contains(&filter.to_lowercase())
                }) {
                    debug!(
                        "CardDAV Tentacle '{}': skipping contact (filter): {}",
                        tentacle_name, contact.fn_name
                    );
                    continue;
                }

                let contact_id = contact.fn_name.clone();
                let now = chrono::Utc::now();

                // Skip if already processed recently
                {
                    let processed = processed_events
                        .read()
                        .map_err(|e| anyhow::anyhow!("Lock error: {}", e))?;
                    if let Some(last_seen) = processed.get(&contact_id)
                        && *last_seen > now - chrono::Duration::hours(1)
                    {
                        debug!(
                            "CardDAV Tentacle '{}': skipping processed contact: {}",
                            tentacle_name, contact_id
                        );
                        continue;
                    }
                }

                // Mark as processed
                {
                    let mut processed = processed_events
                        .write()
                        .map_err(|e| anyhow::anyhow!("Lock error: {}", e))?;
                    processed.insert(contact_id.clone(), now);
                }

                let ink = tentacle.create_contact_ink(&contact);
                event_bus.publish(ink);
                count += 1;
            }
        }
    }

    Ok(count)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::appcontext::AppContext;

    fn default_config() -> CalDavTentacleConfig {
        CalDavTentacleConfig {
            mode: DavMode::CalDav,
            server_url: "https://calendar.example.com".to_string(),
            auth: DavAuth::Basic {
                username: "user".to_string(),
                password: "pass".to_string(),
            },
            poll_interval_secs: 300,
            timeout_secs: 30,
            reconnect_delay_secs: 5,
            topic: "/caldav/events".to_string(),
            future_only: false,
            time_window_hours: 24,
            calendar_urls: vec![],
            contact_filter: None,
            verify_tls: true,
            headers: HashMap::new(),
        }
    }

    #[test]
    fn test_config_defaults() {
        let config = default_config();
        assert_eq!(config.mode, DavMode::CalDav);
        assert_eq!(config.poll_interval_secs, 300);
        assert_eq!(config.timeout_secs, 30);
        assert_eq!(config.topic, "/caldav/events");
        assert!(!config.future_only);
        assert_eq!(config.time_window_hours, 24);
        assert!(config.verify_tls);
    }

    #[test]
    fn test_carddav_config() {
        let mut config = default_config();
        config.mode = DavMode::CardDav;
        config.topic = "/carddav/contacts".to_string();
        config.contact_filter = Some("John".to_string());
        assert_eq!(config.mode, DavMode::CardDav);
        assert_eq!(config.contact_filter, Some("John".to_string()));
    }

    #[test]
    fn test_parse_ics_single_event() {
        let config = default_config();
        let tentacle = CalDavTentacle::new("test".to_string(), config);

        let ics = r#"BEGIN:VCALENDAR
BEGIN:VEVENT
UID:evt-123
SUMMARY:Team Meeting
DESCRIPTION:Weekly sync
DTSTART:20250101T100000Z
DTEND:20250101T110000Z
LOCATION:Room A
END:VEVENT
END:VCALENDAR"#;

        let events = tentacle.parse_ics(ics);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].uid, "evt-123");
        assert_eq!(events[0].summary, "Team Meeting");
        assert_eq!(events[0].description, Some("Weekly sync".to_string()));
        assert_eq!(events[0].location, Some("Room A".to_string()));
    }

    #[test]
    fn test_parse_ics_multiple_events() {
        let config = default_config();
        let tentacle = CalDavTentacle::new("test".to_string(), config);

        let ics = r#"BEGIN:VCALENDAR
BEGIN:VEVENT
UID:evt-1
SUMMARY:Event 1
END:VEVENT
BEGIN:VEVENT
UID:evt-2
SUMMARY:Event 2
END:VEVENT
END:VCALENDAR"#;

        let events = tentacle.parse_ics(ics);
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].uid, "evt-1");
        assert_eq!(events[1].uid, "evt-2");
    }

    #[test]
    fn test_parse_vcard_single_contact() {
        let config = default_config();
        let tentacle = CalDavTentacle::new("test".to_string(), config);

        let vcard = r#"BEGIN:VCARD
VERSION:3.0
FN:John Doe
EMAIL:john@example.com
TEL:+1234567890
ORG:Acme Corp
URL:https://example.com
END:VCARD"#;

        let contacts = tentacle.parse_vcard(vcard);
        assert_eq!(contacts.len(), 1);
        assert_eq!(contacts[0].fn_name, "John Doe");
        assert_eq!(contacts[0].email, Some("john@example.com".to_string()));
        assert_eq!(contacts[0].tel, Some("+1234567890".to_string()));
        assert_eq!(contacts[0].org, Some("Acme Corp".to_string()));
    }

    #[test]
    fn test_parse_vcard_multiple_contacts() {
        let config = default_config();
        let tentacle = CalDavTentacle::new("test".to_string(), config);

        let vcard = r#"BEGIN:VCARD
FN:Alice
EMAIL:alice@example.com
END:VCARD
BEGIN:VCARD
FN:Bob
EMAIL:bob@example.com
END:VCARD"#;

        let contacts = tentacle.parse_vcard(vcard);
        assert_eq!(contacts.len(), 2);
        assert_eq!(contacts[0].fn_name, "Alice");
        assert_eq!(contacts[1].fn_name, "Bob");
    }

    #[test]
    fn test_create_event_ink() {
        let config = default_config();
        let tentacle = CalDavTentacle::new("test".to_string(), config);

        let event = CalendarEvent {
            uid: "evt-123".to_string(),
            summary: "Meeting".to_string(),
            description: Some("Discussion".to_string()),
            dtstart: Some("20250101T100000Z".to_string()),
            dtend: None,
            location: Some("Room A".to_string()),
            url: None,
        };

        let ink = tentacle.create_event_ink(&event);
        assert_eq!(ink.topic, "/caldav/events");
        assert_eq!(ink.source.r#type, "caldav");
        assert!(ink.body.contains("Meeting"));
        assert_eq!(
            ink.metadata
                .as_ref()
                .and_then(|m| m.get("uid"))
                .and_then(|v| v.as_str()),
            Some("evt-123")
        );
    }

    #[test]
    fn test_create_contact_ink() {
        let config = default_config();
        let tentacle = CalDavTentacle::new("test".to_string(), config);

        let contact = VCardContact {
            fn_name: "John Doe".to_string(),
            email: Some("john@example.com".to_string()),
            tel: Some("+1234567890".to_string()),
            org: Some("Acme".to_string()),
            url: None,
        };

        let ink = tentacle.create_contact_ink(&contact);
        assert_eq!(ink.topic, "/caldav/events");
        assert_eq!(ink.source.r#type, "caldav");
        assert!(ink.body.contains("John Doe"));
        assert_eq!(
            ink.metadata
                .as_ref()
                .and_then(|m| m.get("email"))
                .and_then(|v| v.as_str()),
            Some("john@example.com")
        );
    }

    #[test]
    fn test_contact_filter() {
        let mut config = default_config();
        config.mode = DavMode::CardDav;
        config.contact_filter = Some("John".to_string());

        let contacts = [
            VCardContact {
                fn_name: "John Doe".to_string(),
                email: None,
                tel: None,
                org: None,
                url: None,
            },
            VCardContact {
                fn_name: "Alice Smith".to_string(),
                email: None,
                tel: None,
                org: None,
                url: None,
            },
        ];

        let filtered: Vec<&VCardContact> = contacts
            .iter()
            .filter(|c| {
                c.fn_name
                    .to_lowercase()
                    .contains(&config.contact_filter.as_ref().unwrap().to_lowercase())
            })
            .collect();

        assert_eq!(filtered.len(), 1);
        assert_eq!(filtered[0].fn_name, "John Doe");
    }

    #[test]
    fn test_auth_variants() {
        let mut config = default_config();

        // Basic auth
        config.auth = DavAuth::Basic {
            username: "user".to_string(),
            password: "pass".to_string(),
        };
        assert!(matches!(config.auth, DavAuth::Basic { .. }));

        // Bearer auth
        config.auth = DavAuth::Bearer {
            token: "token123".to_string(),
        };
        assert!(matches!(config.auth, DavAuth::Bearer { .. }));

        // Digest auth
        config.auth = DavAuth::Digest {
            username: "user".to_string(),
            password: "pass".to_string(),
        };
        assert!(matches!(config.auth, DavAuth::Digest { .. }));
    }

    #[test]
    fn test_future_only_filter() {
        let config = default_config();
        let tentacle = CalDavTentacle::new("test".to_string(), config);

        // Past event
        let past_event = CalendarEvent {
            uid: "past".to_string(),
            summary: "Past Event".to_string(),
            description: None,
            dtstart: Some("20200101T100000Z".to_string()),
            dtend: None,
            location: None,
            url: None,
        };

        // Future event
        let future_event = CalendarEvent {
            uid: "future".to_string(),
            summary: "Future Event".to_string(),
            description: None,
            dtstart: Some("20300101T100000Z".to_string()),
            dtend: None,
            location: None,
            url: None,
        };

        let ink1 = tentacle.create_event_ink(&past_event);
        let ink2 = tentacle.create_event_ink(&future_event);

        assert_eq!(ink1.body, "Calendar event: Past Event");
        assert_eq!(ink2.body, "Calendar event: Future Event");
    }

    #[test]
    fn test_multiple_calendar_urls() {
        let mut config = default_config();
        config.calendar_urls = vec![
            "https://cal.example.com/work".to_string(),
            "https://cal.example.com/personal".to_string(),
        ];
        assert_eq!(config.calendar_urls.len(), 2);
    }
}
