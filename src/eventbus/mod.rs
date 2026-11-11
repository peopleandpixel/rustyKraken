use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::broadcast;
use tracing::{debug, info, warn};

use crate::model::Ink;

/// Ein Handler, der auf bestimmte Topics reagiert
pub type ReflexHandler = Arc<dyn Fn(Ink) + Send + Sync>;

/// Das Event Bus ist das Herzstück von rustyKraken.
/// Es empfängt Nachrichten von Tentacles und routet sie an die passenden Reflexes.
#[allow(dead_code)]
pub struct EventBus {
    /// Sender für Ink-Nachrichten
    sender: broadcast::Sender<Ink>,
    /// Registrierte Reflexes mit ihren Topic-Patterns
    reflexes: HashMap<String, Vec<(String, ReflexHandler)>>,
}

impl EventBus {
    pub fn new(buffer_size: usize) -> Self {
        let (sender, _receiver) = broadcast::channel(buffer_size);
        Self {
            sender,
            reflexes: HashMap::new(),
        }
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

    /// Sendet eine Nachricht in das Event Bus
    pub fn publish(&self, ink: Ink) {
        let topic = ink.topic.clone();
        debug!("Publishing ink to topic: {}", topic);

        if let Err(e) = self.sender.send(ink) {
            warn!(
                "Failed to publish ink: {} ({} receivers)",
                e,
                self.sender.receiver_count()
            );
        }
    }

    /// Holt einen Receiver für das Event Bus
    pub fn subscribe(&self) -> broadcast::Receiver<Ink> {
        self.sender.subscribe()
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
}
