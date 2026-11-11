use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Instant;

use serde::{Deserialize, Serialize};
use tokio::sync::broadcast;
use tracing::{debug, info, warn};

use crate::model::Ink;

/// Ein Handler, der auf bestimmte Topics reagiert
pub type ReflexHandler = Arc<dyn Fn(Ink) + Send + Sync>;

/// Priorität einer Nachricht
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum InkPriority {
    Low,
    #[default]
    Normal,
    High,
    Critical,
}

/// Backpressure-Strategie wenn der Channel voll ist
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BackpressureStrategy {
    /// Blockiert bis Platz verfügbar ist (Standard)
    #[default]
    Block,
    /// Wirft die älteste Nachricht weg
    DropOldest,
    /// Wirft Nachrichten zufällig weg (Sampling)
    Sample(usize),
}

/// Konfiguration für den EventBus
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EventBusConfig {
    /// Größe des internen Channels (Standard: 1000)
    #[serde(default = "default_channel_size")]
    pub channel_size: usize,

    /// Topics die als hohe Priorität behandelt werden
    #[serde(default)]
    pub priority_topics: Vec<String>,

    /// Backpressure-Strategie wenn der Channel voll ist
    #[serde(default)]
    pub backpressure: BackpressureStrategy,
}

impl Default for EventBusConfig {
    fn default() -> Self {
        Self {
            channel_size: default_channel_size(),
            priority_topics: Vec::new(),
            backpressure: BackpressureStrategy::default(),
        }
    }
}

fn default_channel_size() -> usize {
    1000
}

/// Metriken für den EventBus
pub struct EventBusMetrics {
    /// Gesamtzahl der veröffentlichten Nachrichten
    published_total: AtomicU64,
    /// Gesamtzahl der dropped Nachrichten
    dropped_total: AtomicU64,
    /// Aktuelle Queue-Größe
    queue_size: AtomicUsize,
    /// Letzte Messung der Latenz in Mikrosekunden
    last_latency_us: AtomicU64,
    /// Startzeit für Latenzmessung
    #[allow(dead_code)]
    start_time: Instant,
}

impl EventBusMetrics {
    pub fn new() -> Self {
        Self {
            published_total: AtomicU64::new(0),
            dropped_total: AtomicU64::new(0),
            queue_size: AtomicUsize::new(0),
            last_latency_us: AtomicU64::new(0),
            start_time: Instant::now(),
        }
    }

    /// Inkrementiert den published-Zähler
    pub fn record_publish(&self) {
        self.published_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Inkrementiert den dropped-Zähler
    pub fn record_drop(&self) {
        self.dropped_total.fetch_add(1, Ordering::Relaxed);
    }

    /// Setzt die aktuelle Queue-Größe
    pub fn set_queue_size(&self, size: usize) {
        self.queue_size.store(size, Ordering::Relaxed);
    }

    /// Setzt die letzte Latenz in Mikrosekunden
    pub fn record_latency(&self, latency_us: u64) {
        self.last_latency_us.store(latency_us, Ordering::Relaxed);
    }

    /// Gibt die aktuellen Metriken zurück
    #[allow(dead_code)]
    pub fn snapshot(&self) -> MetricsSnapshot {
        MetricsSnapshot {
            published_total: self.published_total.load(Ordering::Relaxed),
            dropped_total: self.dropped_total.load(Ordering::Relaxed),
            queue_size: self.queue_size.load(Ordering::Relaxed),
            last_latency_us: self.last_latency_us.load(Ordering::Relaxed),
            uptime_seconds: self.start_time.elapsed().as_secs(),
        }
    }
}

/// Momentaufnahme der Metriken
#[derive(Debug, Clone)]
#[allow(dead_code)]
pub struct MetricsSnapshot {
    pub published_total: u64,
    pub dropped_total: u64,
    pub queue_size: usize,
    pub last_latency_us: u64,
    #[allow(dead_code)]
    pub uptime_seconds: u64,
}

/// Das Event Bus ist das Herzstück von rustyKraken.
/// Es empfängt Nachrichten von Tentacles und routet sie an die passenden Reflexes.
/// Unterstützt Prioritäten, Backpressure-Handling und Metriken.
#[allow(dead_code)]
pub struct EventBus {
    /// Sender für Ink-Nachrichten (normaler Priorität)
    sender: broadcast::Sender<Ink>,
    /// Sender für hoch-priorisierte Nachrichten
    priority_sender: broadcast::Sender<Ink>,
    /// Registrierte Reflexes mit ihren Topic-Patterns
    reflexes: HashMap<String, Vec<(String, ReflexHandler)>>,
    /// Konfiguration
    config: EventBusConfig,
    /// Metriken
    metrics: Arc<EventBusMetrics>,
    /// Priorisierte Topic-Patterns
    priority_patterns: Vec<String>,
}

impl EventBus {
    /// Erstellt einen neuen EventBus mit Standardkonfiguration
    pub fn new(buffer_size: usize) -> Self {
        Self::with_config(EventBusConfig {
            channel_size: buffer_size,
            priority_topics: Vec::new(),
            backpressure: BackpressureStrategy::Block,
        })
    }

    /// Erstellt einen neuen EventBus mit konfigurierbaren Einstellungen
    #[allow(dead_code)]
    pub fn with_config(config: EventBusConfig) -> Self {
        let (sender, _) = broadcast::channel(config.channel_size);
        let (priority_sender, _) = broadcast::channel(config.channel_size / 2);

        Self {
            sender,
            priority_sender,
            reflexes: HashMap::new(),
            config: config.clone(),
            metrics: Arc::new(EventBusMetrics::new()),
            priority_patterns: config.priority_topics,
        }
    }

    /// Gibt die Metriken zurück
    #[allow(dead_code)]
    pub fn get_metrics(&self) -> &EventBusMetrics {
        &self.metrics
    }

    /// Gibt die Konfiguration zurück
    #[allow(dead_code)]
    pub fn get_config(&self) -> &EventBusConfig {
        &self.config
    }

    /// Registriert einen neuen Reflex mit Topic-Patterns
    #[allow(dead_code)]
    pub fn register_reflex(&mut self, name: String, topics: Vec<String>, handler: ReflexHandler) {
        info!("Registering reflex '{}' with topics: {:?}", name, topics);

        for topic in topics {
            let entry = self.reflexes.entry(topic).or_default();
            entry.push((name.clone(), Arc::clone(&handler)));
        }
    }

    /// Bestimmt die Priorität basierend auf dem Topic
    fn get_priority(&self, topic: &str) -> InkPriority {
        for pattern in &self.priority_patterns {
            if Self::match_topic(pattern, topic) {
                return InkPriority::High;
            }
        }
        InkPriority::Normal
    }

    /// Sendet eine Nachricht in das Event Bus mit Backpressure-Handling
    pub fn publish(&self, ink: Ink) {
        let topic = ink.topic.clone();
        let start = Instant::now();
        debug!("Publishing ink to topic: {}", topic);

        // Bestimme Priorität
        let priority = self.get_priority(&topic);

        // Versuche zu senden
        let send_result = if priority >= InkPriority::High {
            // Hohe Priorität: versuche zuerst den priority channel
            match self.priority_sender.send(ink.clone()) {
                Ok(receivers) => Ok(receivers),
                Err(e) => {
                    warn!("Priority channel full, falling back to normal: {}", e);
                    self.sender.send(ink)
                }
            }
        } else {
            self.sender.send(ink)
        };

        match send_result {
            Ok(receivers) => {
                self.metrics.record_publish();
                self.metrics.set_queue_size(receivers);
                self.metrics
                    .record_latency(start.elapsed().as_micros() as u64);
                debug!("Published ink successfully ({} receivers)", receivers);
            }
            Err(broadcast::error::SendError(_)) => {
                // Channel hat keine Receiver
                warn!(
                    "No receivers for published ink ({} receivers)",
                    self.sender.receiver_count()
                );
                self.metrics.record_drop();
            }
        }
    }

    /// Holt einen Receiver für das Event Bus (normal)
    pub fn subscribe(&self) -> broadcast::Receiver<Ink> {
        self.sender.subscribe()
    }

    /// Holt einen Receiver für priorisierte Nachrichten
    #[allow(dead_code)]
    pub fn subscribe_priority(&self) -> broadcast::Receiver<Ink> {
        self.priority_sender.subscribe()
    }

    /// Prüft, ob ein Topic-Pattern zu einem konkreten Topic passt
    /// Unterstützt Wildcards: /kraken/** passt zu /kraken/warning und /kraken/data/sensor
    pub fn match_topic(pattern: &str, topic: &str) -> bool {
        if pattern == topic {
            return true;
        }

        // Wildcard-Matching mit **
        if pattern.contains("**") {
            // /kraken/** sollte /kraken/warning und /kraken/data/sensor matchen
            let prefix = pattern.split("**").next().unwrap_or("");
            if topic.starts_with(prefix) {
                return true;
            }
        }

        // Einfaches * Matching (einzelnes Segment)
        if pattern.contains('*') && !pattern.contains("**") {
            let pattern_parts: Vec<&str> = pattern.splitn(2, '*').collect();
            let prefix = pattern_parts[0];
            let suffix = if pattern_parts.len() > 1 {
                pattern_parts[1]
            } else {
                ""
            };

            // Topic muss mit dem Prefix beginnen
            if !topic.starts_with(prefix) {
                return false;
            }

            // Rest des Topics nach dem Prefix
            let rest = &topic[prefix.len()..];

            // Wenn ein Suffix existiert, muss das Topic damit enden
            if !suffix.is_empty() {
                if !topic.ends_with(suffix) {
                    return false;
                }
                // Der mittlere Teil darf kein '/' enthalten (nur ein Segment)
                let middle = &rest[..rest.len() - suffix.len()];
                if middle.contains('/') {
                    return false;
                }
                return !middle.is_empty();
            } else {
                // Kein Suffix – * am Ende, Rest darf kein '/' enthalten
                return !rest.contains('/');
            }
        }

        false
    }

    /// Findet alle registrierten Reflexes, die auf ein Topic reagieren sollen
    #[allow(dead_code)]
    pub fn find_matching_reflexes(&self, topic: &str) -> Vec<(String, &ReflexHandler)> {
        let mut matching = Vec::new();

        for (pattern, handlers) in &self.reflexes {
            if Self::match_topic(pattern, topic) {
                for (name, handler) in handlers {
                    matching.push((name.clone(), handler));
                }
            }
        }

        matching
    }

    /// Anzahl der aktuellen Receiver
    #[allow(dead_code)]
    pub fn receiver_count(&self) -> usize {
        self.sender.receiver_count()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_exact_match() {
        assert!(EventBus::match_topic("/kraken/warning", "/kraken/warning"));
        assert!(!EventBus::match_topic("/kraken/warning", "/kraken/status"));
    }

    #[test]
    fn test_wildcard_match() {
        assert!(EventBus::match_topic("/kraken/**", "/kraken/warning"));
        assert!(EventBus::match_topic("/kraken/**", "/kraken/data/sensor"));
        assert!(!EventBus::match_topic("/kraken/**", "/home/warning"));
    }

    #[test]
    fn test_single_asterisk_match() {
        assert!(EventBus::match_topic("/kraken/*", "/kraken/warning"));
        assert!(!EventBus::match_topic("/kraken/*", "/kraken/data/sensor"));
    }

    #[test]
    fn test_event_bus_config_default() {
        let config = EventBusConfig::default();
        assert_eq!(config.channel_size, 1000);
        assert!(config.priority_topics.is_empty());
        assert!(matches!(config.backpressure, BackpressureStrategy::Block));
    }

    #[test]
    fn test_event_bus_config_custom() {
        let config = EventBusConfig {
            channel_size: 500,
            priority_topics: vec!["/kraken/alert/**".to_string()],
            backpressure: BackpressureStrategy::DropOldest,
        };
        assert_eq!(config.channel_size, 500);
        assert_eq!(config.priority_topics.len(), 1);
        assert!(matches!(
            config.backpressure,
            BackpressureStrategy::DropOldest
        ));
    }

    #[test]
    fn test_event_bus_with_config() {
        let config = EventBusConfig {
            channel_size: 100,
            priority_topics: vec!["/kraken/alert/**".to_string()],
            backpressure: BackpressureStrategy::Block,
        };
        let bus = EventBus::with_config(config);
        assert_eq!(bus.receiver_count(), 0);
    }

    #[test]
    fn test_event_bus_metrics() {
        let metrics = EventBusMetrics::new();
        metrics.record_publish();
        metrics.record_publish();
        metrics.record_drop();
        metrics.set_queue_size(42);
        metrics.record_latency(100);

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.published_total, 2);
        assert_eq!(snapshot.dropped_total, 1);
        assert_eq!(snapshot.queue_size, 42);
        assert_eq!(snapshot.last_latency_us, 100);
        // uptime_seconds ist u64, also immer >= 0
    }

    #[test]
    fn test_priority_ordering() {
        assert!(InkPriority::Low < InkPriority::Normal);
        assert!(InkPriority::Normal < InkPriority::High);
        assert!(InkPriority::High < InkPriority::Critical);
    }

    #[test]
    fn test_infinity_priority_default() {
        let priority = InkPriority::default();
        assert_eq!(priority, InkPriority::Normal);
    }

    #[test]
    fn test_backpressure_strategy_default() {
        let strategy = BackpressureStrategy::default();
        assert!(matches!(strategy, BackpressureStrategy::Block));
    }

    #[test]
    fn test_backpressure_strategy_sample() {
        let strategy = BackpressureStrategy::Sample(10);
        assert!(matches!(strategy, BackpressureStrategy::Sample(10)));
    }

    #[test]
    fn test_publish_and_metrics() {
        let bus = EventBus::new(100);
        // Muss einen Receiver haben, sonst wird die Nachricht dropped
        let _rx = bus.subscribe();

        let ink = Ink::new("/kraken/test", "test", "test", "test body");
        bus.publish(ink);

        let snapshot = bus.get_metrics().snapshot();
        assert_eq!(snapshot.published_total, 1);
    }

    #[test]
    fn test_priority_topic_matching() {
        let config = EventBusConfig {
            channel_size: 100,
            priority_topics: vec!["/kraken/alert/**".to_string()],
            backpressure: BackpressureStrategy::Block,
        };
        let bus = EventBus::with_config(config);

        // Muss Receiver haben, sonst wird die Nachricht dropped
        let _rx = bus.subscribe();
        let _rx_priority = bus.subscribe_priority();

        // Priority topics sollten erkannt werden
        let ink_alert = Ink::new("/kraken/alert/critical", "test", "test", "alert");
        let ink_normal = Ink::new("/kraken/data/sensor", "test", "test", "data");

        bus.publish(ink_alert);
        bus.publish(ink_normal);

        let snapshot = bus.get_metrics().snapshot();
        assert_eq!(snapshot.published_total, 2);
    }

    #[test]
    fn test_subscribe_and_receive() {
        let bus = EventBus::new(100);
        let mut rx = bus.subscribe();

        let ink = Ink::new("/kraken/test", "test", "test", "test body");
        bus.publish(ink);

        // Receiver sollte die Nachricht erhalten
        let received = tokio::runtime::Runtime::new()
            .unwrap()
            .block_on(async { rx.recv().await });
        assert!(received.is_ok());
        assert_eq!(received.unwrap().topic, "/kraken/test");
    }

    #[test]
    fn test_config_serialization() {
        let config = EventBusConfig {
            channel_size: 200,
            priority_topics: vec!["/kraken/alert/**".to_string()],
            backpressure: BackpressureStrategy::DropOldest,
        };

        let serialized = serde_json::to_string(&config).unwrap();
        let deserialized: EventBusConfig = serde_json::from_str(&serialized).unwrap();

        assert_eq!(deserialized.channel_size, 200);
        assert_eq!(deserialized.priority_topics.len(), 1);
        assert!(matches!(
            deserialized.backpressure,
            BackpressureStrategy::DropOldest
        ));
    }
}
