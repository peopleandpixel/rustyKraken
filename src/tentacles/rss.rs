use anyhow::Result;
use async_trait::async_trait;
use feed_parser::parsers::{Feed, atom, rss1, rss2};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, error, info, warn};

use super::Tentacle;
use crate::eventbus::EventBus;
use crate::model::Ink;

/// Topic-Erkennungsmodus für RSS/Atom-Feeds
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Default)]
#[serde(rename_all = "lowercase")]
pub enum RssTopicMode {
    /// Topic aus dem Feed-Titel (z.B. "/feed/Feed Title")
    #[default]
    FeedTitle,
    /// Topic aus dem Eintragstitel
    EntryTitle,
    /// Topic aus dem Creator/Author
    Creator,
    /// Alle Einträge erhalten das Fallback-Topic
    All,
}

/// Konfiguration für einen einzelnen Feed
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RssFeedConfig {
    /// Die URL des Feeds
    pub url: String,
    /// Optional: benutzerdefiniertes Topic für diesen Feed
    #[serde(default)]
    pub topic: Option<String>,
    /// Optional: Feed-Typ erzwingen ("rss", "atom", "auto")
    #[serde(default = "default_feed_type")]
    pub feed_type: String,
}

fn default_feed_type() -> String {
    "auto".to_string()
}

/// Konfiguration für den RSS/Atom Feed Tentacle
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RssTentacleConfig {
    /// Liste der Feeds, die abgefragt werden sollen
    pub feeds: Vec<RssFeedConfig>,
    /// Polling-Intervall in Sekunden (Standard: 300 = 5 Minuten)
    #[serde(default = "default_interval")]
    pub interval_seconds: u64,
    /// Timeout pro Anfrage in Sekunden (Standard: 30)
    #[serde(default = "default_timeout")]
    pub timeout_seconds: u64,
    /// Topic-Modus: "feed_title", "entry_title", "creator", "all"
    #[serde(default)]
    pub topic_mode: RssTopicMode,
    /// Fallback-Topic, wenn kein Topic erkannt wird
    #[serde(default = "default_fallback_topic")]
    pub fallback_topic: String,
    /// Maximale Anzahl von Wiederholungsversuchen bei Fehlern (Standard: 3)
    #[serde(default = "default_max_retries")]
    pub max_retries: u32,
    /// Verzögerung zwischen Wiederholungsversuchen in Sekunden (Standard: 10)
    #[serde(default = "default_retry_delay")]
    pub retry_delay_seconds: u64,
    /// Optional: HTTP-Header für Feed-Anfragen
    #[serde(default)]
    pub headers: Vec<(String, String)>,
}

fn default_interval() -> u64 {
    300
}

fn default_timeout() -> u64 {
    30
}

fn default_fallback_topic() -> String {
    "/rss/default".to_string()
}

fn default_max_retries() -> u32 {
    3
}

fn default_retry_delay() -> u64 {
    10
}

pub struct RssTentacle {
    name: String,
    config: RssTentacleConfig,
    /// Bereits gesehene Einträge (URL + Titel), um Duplikate zu vermeiden
    seen_entries: Arc<parking_lot::Mutex<HashSet<String>>>,
}

impl RssTentacle {
    pub fn new(name: String, config: RssTentacleConfig) -> Self {
        Self {
            name,
            config,
            seen_entries: Arc::new(parking_lot::Mutex::new(HashSet::new())),
        }
    }

    /// Erstellt einen reqwest-Client mit konfigurierter Timeout
    fn build_client(&self) -> reqwest::Client {
        reqwest::Client::builder()
            .timeout(Duration::from_secs(self.config.timeout_seconds))
            .user_agent("rustyKraken RSS/Atom Feed Reader/0.1")
            .build()
            .unwrap_or_else(|_| {
                warn!("Failed to build reqwest client with custom settings, using default");
                reqwest::Client::new()
            })
    }

    /// Lädt den Feed-Inhalt von einer URL
    async fn fetch_feed(&self, url: &str) -> Result<String> {
        let client = self.build_client();

        let mut request = client.get(url);

        // Custom-Header hinzufügen
        for (key, value) in &self.config.headers {
            request = request.header(key, value);
        }

        let response = request.send().await?;

        if !response.status().is_success() {
            return Err(anyhow::anyhow!(
                "HTTP error: {} when fetching feed: {}",
                response.status(),
                url
            ));
        }

        let body = response.text().await?;
        Ok(body)
    }

    /// Bestimmt den Feed-Typ basierend auf dem Inhalt
    fn detect_feed_type(&self, content: &str) -> &'static str {
        let lower = content.to_lowercase();
        if lower.contains("<feed") || lower.contains("<?xml") && lower.contains("atom") {
            "atom"
        } else if lower.contains("<rss") {
            "rss"
        } else {
            "auto"
        }
    }

    /// Parst den Feed-Inhalt und gibt die Einträge zurück
    fn parse_feed_content(&self, content: &str, feed_type: &str) -> Result<Vec<Feed>> {
        match feed_type {
            "rss" | "auto" => {
                // Versuche zuerst RSS2, dann RSS1
                if let Ok(feeds) = rss2::parse(content)
                    && !feeds.is_empty()
                {
                    return Ok(feeds);
                }
                if let Ok(feeds) = rss1::parse(content) {
                    return Ok(feeds);
                }
                // Falls RSS nicht funktioniert, versuche Atom
                if let Ok(feeds) = atom::parse(content) {
                    return Ok(feeds);
                }
                anyhow::bail!("Could not parse feed content as RSS or Atom");
            }
            "atom" => atom::parse(content)
                .map_err(|e| anyhow::anyhow!("Failed to parse Atom feed: {}", e)),
            _ => anyhow::bail!("Unknown feed type: {}", feed_type),
        }
    }

    /// Erstellt einen eindeutigen Schlüssel für einen Feed-Eintrag
    fn entry_key(feed_url: &str, feed: &Feed) -> String {
        // Verwende den Link als primären Schlüssel, falls vorhanden
        if !feed.link.is_empty() {
            format!("{}:{}", feed_url, feed.link)
        } else {
            // Fallback: Titel + Datum
            format!(
                "{}:{}:{}",
                feed_url,
                feed.title,
                feed.publish_date.as_deref().unwrap_or("")
            )
        }
    }

    /// Bestimmt das Topic für einen Feed-Eintrag
    fn determine_topic(&self, feed_config: &RssFeedConfig, feed: &Feed) -> String {
        // Wenn ein benutzerdefiniertes Topic konfiguriert ist, verwende es
        if let Some(ref topic) = feed_config.topic {
            return topic.clone();
        }

        match self.config.topic_mode {
            RssTopicMode::FeedTitle => {
                // Sanitisiere den Feed-Titel für das Topic
                let sanitized = Self::sanitize_topic(&feed.title);
                format!("/feed/{}", sanitized)
            }
            RssTopicMode::EntryTitle => {
                let sanitized = Self::sanitize_topic(&feed.title);
                format!("/feed/{}", sanitized)
            }
            RssTopicMode::Creator => {
                let creator = feed.creator.as_deref().unwrap_or("unknown");
                let sanitized = Self::sanitize_topic(creator);
                format!("/feed/{}", sanitized)
            }
            RssTopicMode::All => self.config.fallback_topic.clone(),
        }
    }

    /// Sanitisiert einen String für die Verwendung als Topic
    fn sanitize_topic(input: &str) -> String {
        input
            .chars()
            .map(|c| {
                if c.is_alphanumeric() || c == '-' || c == '_' {
                    c
                } else {
                    '_'
                }
            })
            .take(50)
            .collect()
    }

    /// Erstellt den Body für einen Feed-Eintrag
    fn create_body(feed: &Feed) -> String {
        let mut body = format!("Title: {}\n", feed.title);

        if let Some(ref desc) = feed.description {
            body.push_str(&format!("Description: {}\n", desc));
        }

        if let Some(ref summary) = feed.summary {
            body.push_str(&format!("Summary: {}\n", summary));
        }

        if !feed.link.is_empty() {
            body.push_str(&format!("Link: {}\n", feed.link));
        }

        if let Some(ref creator) = feed.creator {
            body.push_str(&format!("Creator: {}\n", creator));
        }

        if let Some(ref date) = feed.publish_date {
            body.push_str(&format!("Published: {}\n", date));
        }

        if let Some(ref other) = feed.other {
            body.push_str(&format!("Content: {}\n", other));
        }

        body.trim_end().to_string()
    }

    /// Fragt einen einzelnen Feed ab und publish't neue Einträge
    async fn poll_feed(
        &self,
        feed_config: &RssFeedConfig,
        event_bus: &Arc<EventBus>,
    ) -> Result<()> {
        info!(
            "RSS tentacle '{}' polling feed: {}",
            self.name, feed_config.url
        );

        let content = self.fetch_feed(&feed_config.url).await?;

        let feed_type = if feed_config.feed_type != "auto" {
            &feed_config.feed_type
        } else {
            self.detect_feed_type(&content)
        };

        let feeds = self.parse_feed_content(&content, feed_type)?;

        info!(
            "RSS tentacle '{}' received {} entries from {}",
            self.name,
            feeds.len(),
            feed_config.url
        );

        let mut new_count = 0;
        for feed in feeds {
            let key = Self::entry_key(&feed_config.url, &feed);

            // Prüfe, ob dieser Eintrag bereits gesehen wurde
            {
                let mut seen = self.seen_entries.lock();
                if seen.contains(&key) {
                    debug!(
                        "RSS tentacle '{}' skipping duplicate entry: {}",
                        self.name, feed.title
                    );
                    continue;
                }
                seen.insert(key);
            }

            let topic = self.determine_topic(feed_config, &feed);
            let body = Self::create_body(&feed);

            let ink = Ink::new(topic.clone(), "rss", &self.name, body);
            debug!(
                "RSS tentacle '{}' publishing new entry on topic: {}",
                self.name, topic
            );
            event_bus.publish(ink);
            new_count += 1;
        }

        if new_count > 0 {
            info!(
                "RSS tentacle '{}' found {} new entries from {}",
                self.name, new_count, feed_config.url
            );
        }

        Ok(())
    }

    /// Fragt alle Feeds ab mit Retry-Logik
    async fn poll_all_feeds(&self, event_bus: &Arc<EventBus>) {
        for feed_config in &self.config.feeds {
            let mut last_error = None;

            for attempt in 0..=self.config.max_retries {
                match self.poll_feed(feed_config, event_bus).await {
                    Ok(()) => {
                        last_error = None;
                        break;
                    }
                    Err(e) => {
                        let err_msg = e.to_string();
                        last_error = Some(e);
                        if attempt < self.config.max_retries {
                            warn!(
                                "RSS tentacle '{}' failed to poll {} (attempt {}/{}): {}",
                                self.name,
                                feed_config.url,
                                attempt + 1,
                                self.config.max_retries + 1,
                                err_msg
                            );
                            tokio::time::sleep(Duration::from_secs(
                                self.config.retry_delay_seconds,
                            ))
                            .await;
                        }
                    }
                }
            }

            if let Some(e) = last_error {
                error!(
                    "RSS tentacle '{}' failed to poll {} after {} retries: {}",
                    self.name, feed_config.url, self.config.max_retries, e
                );
            }
        }
    }
}

#[async_trait]
impl Tentacle for RssTentacle {
    fn name(&self) -> &str {
        &self.name
    }

    fn r#type(&self) -> &str {
        "rss"
    }

    async fn start(&self, event_bus: Arc<EventBus>) -> Result<()> {
        info!(
            "RSS/Atom feed tentacle '{}' started with {} feeds, interval: {}s",
            self.name,
            self.config.feeds.len(),
            self.config.interval_seconds
        );

        let interval = Duration::from_secs(self.config.interval_seconds);

        // Initialer Poll
        self.poll_all_feeds(&event_bus).await;

        loop {
            tokio::time::sleep(interval).await;
            self.poll_all_feeds(&event_bus).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_tentacle() -> RssTentacle {
        RssTentacle::new(
            "test_rss".to_string(),
            RssTentacleConfig {
                feeds: vec![RssFeedConfig {
                    url: "https://example.com/feed.xml".to_string(),
                    topic: None,
                    feed_type: "auto".to_string(),
                }],
                interval_seconds: 300,
                timeout_seconds: 30,
                topic_mode: RssTopicMode::FeedTitle,
                fallback_topic: "/rss/default".to_string(),
                max_retries: 3,
                retry_delay_seconds: 10,
                headers: vec![],
            },
        )
    }

    #[test]
    fn test_sanitize_topic() {
        assert_eq!(RssTentacle::sanitize_topic("Hello World"), "Hello_World");
        assert_eq!(
            RssTentacle::sanitize_topic("test@example.com"),
            "test_example_com"
        );
        assert_eq!(
            RssTentacle::sanitize_topic("normal-text_123"),
            "normal-text_123"
        );
    }

    #[test]
    fn test_sanitize_topic_long() {
        let long = "a".repeat(100);
        assert_eq!(RssTentacle::sanitize_topic(&long), "a".repeat(50));
    }

    #[test]
    fn test_entry_key_with_link() {
        let feed = Feed {
            title: "Test".to_string(),
            link: "https://example.com/post/1".to_string(),
            description: None,
            summary: None,
            updated: None,
            publish_date: None,
            creator: None,
            date: None,
            other: None,
        };
        let key = RssTentacle::entry_key("https://feed.example.com", &feed);
        assert_eq!(key, "https://feed.example.com:https://example.com/post/1");
    }

    #[test]
    fn test_entry_key_without_link() {
        let feed = Feed {
            title: "Test Post".to_string(),
            link: "".to_string(),
            description: None,
            summary: None,
            updated: None,
            publish_date: Some("2024-01-01".to_string()),
            creator: None,
            date: None,
            other: None,
        };
        let key = RssTentacle::entry_key("https://feed.example.com", &feed);
        assert_eq!(key, "https://feed.example.com:Test Post:2024-01-01");
    }

    #[test]
    fn test_create_body() {
        let feed = Feed {
            title: "Test Post".to_string(),
            link: "https://example.com/post".to_string(),
            description: Some("A test post".to_string()),
            summary: None,
            updated: None,
            publish_date: Some("2024-01-01".to_string()),
            creator: Some("Author".to_string()),
            date: None,
            other: None,
        };
        let body = RssTentacle::create_body(&feed);
        assert!(body.contains("Title: Test Post"));
        assert!(body.contains("Description: A test post"));
        assert!(body.contains("Link: https://example.com/post"));
        assert!(body.contains("Creator: Author"));
        assert!(body.contains("Published: 2024-01-01"));
    }

    #[test]
    fn test_determine_topic_custom() {
        let tentacle = create_tentacle();
        let feed_config = RssFeedConfig {
            url: "https://example.com/feed.xml".to_string(),
            topic: Some("/custom/topic".to_string()),
            feed_type: "auto".to_string(),
        };
        let feed = Feed {
            title: "Test".to_string(),
            link: "".to_string(),
            description: None,
            summary: None,
            updated: None,
            publish_date: None,
            creator: None,
            date: None,
            other: None,
        };
        assert_eq!(
            tentacle.determine_topic(&feed_config, &feed),
            "/custom/topic"
        );
    }

    #[test]
    fn test_determine_topic_feed_title() {
        let tentacle = create_tentacle();
        let feed_config = RssFeedConfig {
            url: "https://example.com/feed.xml".to_string(),
            topic: None,
            feed_type: "auto".to_string(),
        };
        let feed = Feed {
            title: "My Feed Post".to_string(),
            link: "".to_string(),
            description: None,
            summary: None,
            updated: None,
            publish_date: None,
            creator: None,
            date: None,
            other: None,
        };
        assert_eq!(
            tentacle.determine_topic(&feed_config, &feed),
            "/feed/My_Feed_Post"
        );
    }

    #[test]
    fn test_determine_topic_creator() {
        let mut tentacle = create_tentacle();
        tentacle.config.topic_mode = RssTopicMode::Creator;
        let feed_config = RssFeedConfig {
            url: "https://example.com/feed.xml".to_string(),
            topic: None,
            feed_type: "auto".to_string(),
        };
        let feed = Feed {
            title: "Test".to_string(),
            link: "".to_string(),
            description: None,
            summary: None,
            updated: None,
            publish_date: None,
            creator: Some("John Doe".to_string()),
            date: None,
            other: None,
        };
        assert_eq!(
            tentacle.determine_topic(&feed_config, &feed),
            "/feed/John_Doe"
        );
    }

    #[test]
    fn test_determine_topic_all_mode() {
        let mut tentacle = create_tentacle();
        tentacle.config.topic_mode = RssTopicMode::All;
        let feed_config = RssFeedConfig {
            url: "https://example.com/feed.xml".to_string(),
            topic: None,
            feed_type: "auto".to_string(),
        };
        let feed = Feed {
            title: "Test".to_string(),
            link: "".to_string(),
            description: None,
            summary: None,
            updated: None,
            publish_date: None,
            creator: None,
            date: None,
            other: None,
        };
        assert_eq!(
            tentacle.determine_topic(&feed_config, &feed),
            "/rss/default"
        );
    }

    #[test]
    fn test_detect_feed_type_rss() {
        let tentacle = create_tentacle();
        assert_eq!(
            tentacle.detect_feed_type("<?xml version=\"1.0\"?><rss><channel></channel></rss>"),
            "rss"
        );
    }

    #[test]
    fn test_detect_feed_type_atom() {
        let tentacle = create_tentacle();
        assert_eq!(
            tentacle.detect_feed_type(
                "<?xml version=\"1.0\"?><feed xmlns=\"http://www.w3.org/2005/Atom\"></feed>"
            ),
            "atom"
        );
    }

    #[test]
    fn test_config_deserialization() {
        let yaml = r#"
            feeds:
              - url: "https://example.com/feed.xml"
                topic: "/feed/custom"
                feed_type: "rss"
              - url: "https://example.com/atom.xml"
                feed_type: "atom"
            interval_seconds: 60
            topic_mode: "entrytitle"
            fallback_topic: "/rss/fallback"
        "#;
        let config: RssTentacleConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.feeds.len(), 2);
        assert_eq!(config.feeds[0].topic, Some("/feed/custom".to_string()));
        assert_eq!(config.feeds[1].feed_type, "atom");
        assert_eq!(config.interval_seconds, 60);
        assert_eq!(config.topic_mode, RssTopicMode::EntryTitle);
    }

    #[test]
    fn test_config_defaults() {
        let yaml = r#"
            feeds:
              - url: "https://example.com/feed.xml"
        "#;
        let config: RssTentacleConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.feeds[0].feed_type, "auto");
        assert_eq!(config.interval_seconds, 300);
        assert_eq!(config.topic_mode, RssTopicMode::FeedTitle);
        assert_eq!(config.fallback_topic, "/rss/default");
    }

    #[test]
    fn test_parse_rss2_feed() {
        let rss = r#"<?xml version="1.0" encoding="UTF-8"?>
<rss version="2.0">
  <channel>
    <title>Test Feed</title>
    <item>
      <title>Test Post</title>
      <link>https://example.com/post/1</link>
      <description>A test post</description>
    </item>
  </channel>
</rss>"#;

        let feeds = rss2::parse(rss).unwrap();
        assert_eq!(feeds.len(), 1);
        assert_eq!(feeds[0].title, "Test Post");
        assert_eq!(feeds[0].link, "https://example.com/post/1");
        assert_eq!(feeds[0].description, Some("A test post".to_string()));
    }

    #[test]
    fn test_parse_atom_feed() {
        let atom = r#"<?xml version="1.0" encoding="UTF-8"?>
<feed xmlns="http://www.w3.org/2005/Atom">
  <title>Test Feed</title>
  <entry>
    <title>Test Entry</title>
    <link href="https://example.com/entry/1" rel="alternate" type="text/html"/>
    <summary>A test entry</summary>
    <author><name>Author</name></author>
    <published>2024-01-01T00:00:00Z</published>
  </entry>
</feed>"#;

        let feeds = atom::parse(atom).unwrap();
        assert_eq!(feeds.len(), 1);
        assert_eq!(feeds[0].title, "Test Entry");
        assert_eq!(feeds[0].link, "https://example.com/entry/1");
    }
}
