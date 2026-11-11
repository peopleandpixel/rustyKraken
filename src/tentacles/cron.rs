use anyhow::Result;
use async_trait::async_trait;
use chrono::{DateTime, Datelike, Timelike, Utc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::time;
use tracing::{debug, error, info};

use super::Tentacle;
use crate::eventbus::EventBus;
use crate::model::Ink;

/// Ein Cron-Job innerhalb des Cron Tentacles
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CronJob {
    /// Cron-Ausdruck (z.B. "*/5 * * * * *")
    pub schedule: String,
    /// Topic für die generierten Events
    pub topic: String,
    /// Optional: statischer Body für die Events
    #[serde(default)]
    pub body: Option<String>,
    /// Optional: Metadaten die an jede Nachricht angehängt werden
    #[serde(default)]
    pub metadata: Option<HashMap<String, String>>,
    /// Optional: Name des Jobs (für Logging)
    #[serde(default)]
    pub name: Option<String>,
}

/// Konfiguration für den Cron/Scheduler Tentacle
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CronTentacleConfig {
    /// Liste von Cron-Jobs
    pub jobs: Vec<CronJob>,
    /// Optional: Verzögerung vor dem ersten Start in Sekunden
    #[serde(default)]
    pub startup_delay_secs: u64,
}

pub struct CronTentacle {
    name: String,
    config: CronTentacleConfig,
}

impl CronTentacle {
    pub fn new(name: String, config: CronTentacleConfig) -> Self {
        Self { name, config }
    }

    /// Parst einen Cron-Ausdruck und berechnet die nächste Ausführungszeit
    /// Unterstützt 6-Feld-Cron: minute hour day month weekday second
    /// Oder 5-Feld-Cron: minute hour day month weekday (Sekunde = 0)
    fn parse_cron_expression(expr: &str) -> Result<CronSchedule, String> {
        let parts: Vec<&str> = expr.split_whitespace().collect();
        let parts = match parts.len() {
            5 => {
                // 5-Feld-Format: minute hour day month weekday
                vec!["0", parts[0], parts[1], parts[2], parts[3], parts[4]]
            }
            6 => parts,
            _ => {
                return Err(format!(
                    "Invalid cron expression: expected 5 or 6 fields, got {}",
                    parts.len()
                ));
            }
        };

        let second = parse_cron_field(parts[0], 0, 59)?;
        let minute = parse_cron_field(parts[1], 0, 59)?;
        let hour = parse_cron_field(parts[2], 0, 23)?;
        let day = parse_cron_field(parts[3], 1, 31)?;
        let month = parse_cron_field(parts[4], 1, 12)?;
        let weekday = parse_cron_field(parts[5], 0, 6)?;

        Ok(CronSchedule {
            seconds: second,
            minutes: minute,
            hours: hour,
            days: day,
            months: month,
            weekdays: weekday,
        })
    }

    /// Berechnet die nächste Ausführungszeit ab `from`
    #[allow(dead_code)]
    fn next_occurrence(&self, schedule: &CronSchedule, from: DateTime<Utc>) -> DateTime<Utc> {
        // Einfache Brute-Force-Suche: gehe Sekunde für Sekunde vorwärts
        let mut candidate = from + chrono::Duration::seconds(1);

        // Maximale Suchgrenze: 1 Jahr
        let max_search = from + chrono::Duration::days(365);

        while candidate <= max_search {
            if Self::matches_schedule(&candidate, schedule) {
                return candidate;
            }
            candidate += chrono::Duration::seconds(1);
        }

        // Falls keine Übereinstimmung gefunden, return max_search
        max_search
    }

    /// Prüft ob ein DateTime einem CronSchedule entspricht
    fn matches_schedule(dt: &DateTime<Utc>, schedule: &CronSchedule) -> bool {
        let naive = dt.naive_utc();
        let second = naive.second();
        let minute = naive.minute();
        let hour = naive.hour();
        let day = naive.day();
        let month = naive.month();
        let weekday = match naive.weekday() {
            // chrono::Weekday: Mon=0..=Sun=6 → Cron: Sun=0, Mon=1.., Sat=6
            chrono::Weekday::Mon => 1,
            chrono::Weekday::Tue => 2,
            chrono::Weekday::Wed => 3,
            chrono::Weekday::Thu => 4,
            chrono::Weekday::Fri => 5,
            chrono::Weekday::Sat => 6,
            chrono::Weekday::Sun => 0,
        };

        schedule.seconds.contains(&second)
            && schedule.minutes.contains(&minute)
            && schedule.hours.contains(&hour)
            && schedule.days.contains(&day)
            && schedule.months.contains(&month)
            && schedule.weekdays.contains(&weekday)
    }
}

/// Geparster Cron-Ausdruck
#[derive(Debug, Clone)]
struct CronSchedule {
    seconds: Vec<u32>,
    minutes: Vec<u32>,
    hours: Vec<u32>,
    days: Vec<u32>,
    months: Vec<u32>,
    weekdays: Vec<u32>,
}

/// Parst ein einzelnes Cron-Feld (z.B. "*/5", "1-10", "1,3,5", "*", "0-59/2")
fn parse_cron_field(field: &str, min: u32, max: u32) -> Result<Vec<u32>, String> {
    let mut result = Vec::new();

    for part in field.split(',') {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }

        // Schrittweite extrahieren
        let (range, step) = if let Some(pos) = part.find('/') {
            let range_str = &part[..pos];
            let step_str = &part[pos + 1..];
            let step_val: u32 = step_str
                .parse()
                .map_err(|_| format!("Invalid step: {}", step_str))?;
            if step_val == 0 {
                return Err(format!("Step cannot be 0: {}", part));
            }
            (range_str, step_val)
        } else {
            (part, 1)
        };

        // Bereich extrahieren
        let (start, end) = if let Some(pos) = range.find('-') {
            let start_str = &range[..pos];
            let end_str = &range[pos + 1..];
            let start_val: u32 = start_str
                .parse()
                .map_err(|_| format!("Invalid range start: {}", start_str))?;
            let end_val: u32 = end_str
                .parse()
                .map_err(|_| format!("Invalid range end: {}", end_str))?;
            (start_val, end_val)
        } else if range == "*" {
            (min, max)
        } else {
            let val: u32 = range
                .parse()
                .map_err(|_| format!("Invalid value: {}", range))?;
            (val, val)
        };

        if start > max || end > max {
            return Err(format!("Value out of range ({}-{}): {}", min, max, part));
        }

        let start = start.max(min);
        let end = end.min(max);

        for val in (start..=end).step_by(step as usize) {
            if !result.contains(&val) {
                result.push(val);
            }
        }
    }

    result.sort();
    Ok(result)
}

#[async_trait]
impl Tentacle for CronTentacle {
    fn name(&self) -> &str {
        &self.name
    }

    fn r#type(&self) -> &str {
        "cron"
    }

    async fn start(&self, event_bus: Arc<EventBus>) -> Result<()> {
        info!(
            "Cron Tentacle '{}' starting with {} job(s)",
            self.name,
            self.config.jobs.len()
        );

        // Optionalen Startup-Delay warten
        if self.config.startup_delay_secs > 0 {
            info!(
                "Waiting {} seconds before starting cron jobs...",
                self.config.startup_delay_secs
            );
            time::sleep(Duration::from_secs(self.config.startup_delay_secs)).await;
        }

        let jobs = self.config.jobs.clone();
        let tentacle_name = self.name.clone();

        // Für jeden Job einen eigenen Task starten
        for job in &jobs {
            let job_name = job.name.clone().unwrap_or_else(|| job.schedule.clone());
            let event_bus = Arc::clone(&event_bus);
            let tname = tentacle_name.clone();

            // Cron-Ausdruck parsen
            let schedule = match Self::parse_cron_expression(&job.schedule) {
                Ok(s) => s,
                Err(e) => {
                    error!(
                        "Failed to parse cron expression for job '{}': {}",
                        job_name, e
                    );
                    continue;
                }
            };

            info!(
                "Started cron job '{}' with schedule '{}' → topic '{}'",
                job_name, job.schedule, job.topic
            );

            let job_topic = job.topic.clone();
            let job_body = job.body.clone();
            let job_metadata = job.metadata.clone();

            tokio::spawn(async move {
                let mut last_run = DateTime::<Utc>::MIN_UTC;

                loop {
                    let next = next_occurrence(&schedule, last_run);
                    let now = Utc::now();

                    if next > now {
                        // Bis zur nächsten Ausführungszeit warten
                        let wait_secs = (next - now).num_seconds().max(1);
                        debug!(
                            "Cron job '{}': next run at {}, waiting {} seconds",
                            job_name, next, wait_secs
                        );
                        time::sleep(Duration::from_secs(wait_secs as u64)).await;
                    }

                    // Nochmal prüfen ob es Zeit ist (nach dem Wachen)
                    if Utc::now() >= next {
                        last_run = next;

                        // Ink-Nachricht erstellen
                        let body = job_body
                            .clone()
                            .unwrap_or_else(|| format!("Cron trigger: {}", job_topic));

                        let mut ink = Ink::new(&job_topic, "cron", &tname, body);

                        // Metadaten hinzufügen
                        if let Some(ref metadata_map) = job_metadata {
                            let mut meta = serde_json::Map::new();
                            for (key, value) in metadata_map {
                                meta.insert(key.clone(), serde_json::Value::String(value.clone()));
                            }
                            ink = ink.with_metadata(serde_json::Value::Object(meta));
                        }

                        info!("Cron job '{}' triggered on topic: {}", job_name, job_topic);
                        event_bus.publish(ink);
                    }
                }
            });
        }

        // Blockiere den Task, damit er läuft
        loop {
            time::sleep(Duration::from_secs(60)).await;
        }
    }
}

/// Berechnet die nächste Ausführungszeit (pub für Tests)
fn next_occurrence(schedule: &CronSchedule, from: DateTime<Utc>) -> DateTime<Utc> {
    let mut candidate = from + chrono::Duration::seconds(1);
    let max_search = from + chrono::Duration::days(365);

    while candidate <= max_search {
        if CronTentacle::matches_schedule(&candidate, schedule) {
            return candidate;
        }
        candidate += chrono::Duration::seconds(1);
    }
    max_search
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_cron_field_wildcard() {
        let result = parse_cron_field("*", 0, 59).unwrap();
        assert_eq!(result.len(), 60);
        assert_eq!(result[0], 0);
        assert_eq!(result[59], 59);
    }

    #[test]
    fn test_parse_cron_field_step() {
        let result = parse_cron_field("*/15", 0, 59).unwrap();
        assert_eq!(result, vec![0, 15, 30, 45]);
    }

    #[test]
    fn test_parse_cron_field_range() {
        let result = parse_cron_field("1-5", 0, 59).unwrap();
        assert_eq!(result, vec![1, 2, 3, 4, 5]);
    }

    #[test]
    fn test_parse_cron_field_range_with_step() {
        let result = parse_cron_field("0-10/2", 0, 59).unwrap();
        assert_eq!(result, vec![0, 2, 4, 6, 8, 10]);
    }

    #[test]
    fn test_parse_cron_field_list() {
        let result = parse_cron_field("1,3,5,7", 0, 59).unwrap();
        assert_eq!(result, vec![1, 3, 5, 7]);
    }

    #[test]
    fn test_parse_cron_field_single_value() {
        let result = parse_cron_field("30", 0, 59).unwrap();
        assert_eq!(result, vec![30]);
    }

    #[test]
    fn test_parse_cron_expression_5_fields() {
        let schedule = CronTentacle::parse_cron_expression("*/5 * * * *").unwrap();
        assert_eq!(schedule.seconds, vec![0]);
        assert_eq!(
            schedule.minutes,
            vec![0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55]
        );
        assert_eq!(schedule.hours.len(), 24);
    }

    #[test]
    fn test_parse_cron_expression_6_fields() {
        // 6-Feld-Format: second minute hour day month weekday
        let schedule = CronTentacle::parse_cron_expression("0 */2 * * * *").unwrap();
        assert_eq!(schedule.seconds, vec![0]);
        // */2 bei minute = 0,2,4,...,58 = 30 Werte
        assert_eq!(schedule.minutes.len(), 30);
        assert_eq!(schedule.hours.len(), 24);
    }

    #[test]
    fn test_parse_cron_expression_invalid() {
        let result = CronTentacle::parse_cron_expression("*");
        assert!(result.is_err());
    }

    #[test]
    fn test_matches_schedule() {
        // Erstelle einen festen Zeitpunkt: 2026-01-15 10:30:00 UTC (Donnerstag)
        let dt = DateTime::parse_from_rfc3339("2026-01-15T10:30:00+00:00")
            .unwrap()
            .with_timezone(&Utc);

        // Schedule: Sekunde 0, Minute 30, Stunde 10, Tag 15, Monat 1, Wochentag 4 (Donnerstag)
        let schedule = CronSchedule {
            seconds: vec![0],
            minutes: vec![30],
            hours: vec![10],
            days: vec![15],
            months: vec![1],
            weekdays: vec![4],
        };

        assert!(CronTentacle::matches_schedule(&dt, &schedule));
    }

    #[test]
    fn test_matches_schedule_wildcard() {
        let dt = DateTime::parse_from_rfc3339("2026-06-15T14:22:30+00:00")
            .unwrap()
            .with_timezone(&Utc);

        // Alle Sekunden, alle Minuten, Stunde 14, alle Tage, Monat 6, alle Wochentage
        let schedule = CronSchedule {
            seconds: (0..=59).collect(),
            minutes: (0..=59).collect(),
            hours: vec![14],
            days: (1..=31).collect(),
            months: vec![6],
            weekdays: (0..=6).collect(),
        };

        assert!(CronTentacle::matches_schedule(&dt, &schedule));
    }

    #[test]
    fn test_next_occurrence_every_minute() {
        let from = DateTime::parse_from_rfc3339("2026-01-15T10:30:45+00:00")
            .unwrap()
            .with_timezone(&Utc);

        // Jede Minute: * * * * *
        let schedule = CronTentacle::parse_cron_expression("* * * * *").unwrap();
        let next = next_occurrence(&schedule, from);

        // Nächste Minute sollte 10:31:00 sein
        assert_eq!(next.minute(), 31);
        assert_eq!(next.second(), 0);
    }

    #[test]
    fn test_cron_job_config_deserialization() {
        let yaml = r#"
        jobs:
          - schedule: "*/5 * * * *"
            topic: "/cron/heartbeat"
            body: "Heartbeat check"
            name: "heartbeat"
            metadata:
              check_type: "heartbeat"
              interval: "5min"
          - schedule: "0 0 * * *"
            topic: "/cron/daily"
            name: "daily_report"
        "#;

        let config: CronTentacleConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(config.jobs.len(), 2);
        assert_eq!(config.jobs[0].name.as_deref(), Some("heartbeat"));
        assert_eq!(config.jobs[0].topic, "/cron/heartbeat");
        assert_eq!(config.jobs[0].body.as_deref(), Some("Heartbeat check"));
        assert!(config.jobs[0].metadata.is_some());
        assert_eq!(config.jobs[1].name.as_deref(), Some("daily_report"));
        assert_eq!(config.jobs[1].body, None);
    }

    #[test]
    fn test_cron_tentacle_name_and_type() {
        let config = CronTentacleConfig {
            jobs: vec![CronJob {
                schedule: "* * * * *".to_string(),
                topic: "/test".to_string(),
                body: None,
                metadata: None,
                name: Some("test".to_string()),
            }],
            startup_delay_secs: 0,
        };
        let tentacle = CronTentacle::new("test_cron".to_string(), config);
        assert_eq!(tentacle.name(), "test_cron");
        assert_eq!(tentacle.r#type(), "cron");
    }

    #[test]
    fn test_weekday_mapping() {
        // Sonntag: 2026-01-11
        let sun = DateTime::parse_from_rfc3339("2026-01-11T00:00:00+00:00")
            .unwrap()
            .with_timezone(&Utc);
        let sun_schedule = CronSchedule {
            seconds: vec![0],
            minutes: vec![0],
            hours: vec![0],
            days: (1..=31).collect(),
            months: (1..=12).collect(),
            weekdays: vec![0], // Sonntag = 0 in Cron
        };
        assert!(CronTentacle::matches_schedule(&sun, &sun_schedule));

        // Montag: 2026-01-12
        let mon = DateTime::parse_from_rfc3339("2026-01-12T00:00:00+00:00")
            .unwrap()
            .with_timezone(&Utc);
        let mon_schedule = CronSchedule {
            seconds: vec![0],
            minutes: vec![0],
            hours: vec![0],
            days: (1..=31).collect(),
            months: (1..=12).collect(),
            weekdays: vec![1], // Montag = 1 in Cron
        };
        assert!(CronTentacle::matches_schedule(&mon, &mon_schedule));
    }
}
