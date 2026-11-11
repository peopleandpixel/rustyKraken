# Changelog

Alle bemerkenswerten Änderungen an rustyKraken werden in dieser Datei dokumentiert.

Das Format basiert auf [Keep a Changelog](https://keepachangelog.com/de/1.1.0/).

## [Unreleased]

## [0.2.0] - 2025-11-11

### Hinzugefügt

#### Kernarchitektur
- **Event Bus**: Pub/Sub-System mit Topic-Matching und Wildcard-Unterstützung (`**` und `*`)
- **Ink-Modell**: Einheitliches Nachrichtenformat mit UUID, Topic, Quelle, Body, Metadaten und Status
- **Mantle**: SQLite-basierte Speicherung aller Nachrichten mit Indexierung nach Topic und Status
- **Config-System**: YAML-basierter Loader für `config/incoming/` (Tentacles) und `config/listener/` (Reflexes) mit Hot-Reload-Vorbereitung

#### Tentacles (Eingangsquellen)
- **HTTP/Webhook Tentacle**: HTTP-Server mit Axum, akzeptiert POST-Anfragen mit JSON-Body
- **MQTT Tentacle**: Vollständige MQTT-Client-Implementierung mit:
  - TLS-Unterstützung (rustls, custom CA, mTLS)
  - Last-Will-Messages
  - Clean Session Konfiguration
  - Automatische Reconnection mit Resubscription
  - Topic-Mapping mit MQTT-Wildcards (`+` und `#`)
- **IMAP Tentacle**: E-Mail-Empfang via IMAP mit:
  - Implizite TLS (Port 993)
  - STARTTLS (Port 143)
  - Drei Topic-Modi: Subject, Sender, All
  - Absender-Mappings mit Platzhaltern
  - UID-basierte Nachverfolgung mit persistenter Speicherung
  - IDLE-Modus für Echtzeit-Benachrichtigungen
  - E-Mail-Parsing mit mailparse (text/plain, text/html, multipart)
- **POP3 Tentacle**: E-Mail-Empfang via POP3 mit:
  - Implizite TLS (Port 995)
  - STARTTLS (Port 110)
  - Plain-Verbindung (Port 110)
  - Drei Topic-Modi: Subject, Sender, All
  - Absender-Mappings mit Platzhaltern
  - Message-ID-basierte Deduplizierung mit persistenter Speicherung
  - Eigenes POP3-Protokoll-Implementation (async-Crates waren kaputt)
- **SMTP Tentacle**: SMTP-Server zum Empfangen von E-Mails mit:
  - Implizite TLS (Port 465)
  - STARTTLS-Unterstützung
  - AUTH PLAIN Authentifizierung
  - Dot-stuffing Entcodierung
  - Drei Topic-Modi: Subject, Sender, All
  - Absender-Mappings mit Platzhaltern
  - Erlaubte Absender filtern
  - Maximale Nachrichtengröße konfigurierbar

#### Reflexes (Aktionen)
- **File Reflex**: JSON-Logging in Dateien mit Append-Modus
- **Command Reflex**: Shell-Befehle ausführen mit Umgebungsvariablen (INK_JSON, INK_TOPIC, INK_BODY, INK_ID)

### Geändert
- **Crates-Updates**: Alle externen Crates auf neueste Versionen aktualisiert
  - `serde_yaml` → `yaml_serde 0.10.7` (RUSTSEC-2025-0068)
  - `sqlx 0.8` → `0.9`
  - `rumqttc 0.24` → `0.25`
  - `reqwest 0.12` → `0.13`
- **Namensänderung**: Alle Vorkommen von „octopus" durch „kraken" ersetzt

### Behoben
- **Clippy-Warnings**: Alle 32 clippy-Warnings behoben (collapsible_if, single_match, unnecessary_map_or, manual_strip, needless_borrow, sliced_string_as_bytes, too_many_arguments)
- **Compiler-Warnings**: Alle 20 Compiler-Warnings behoben (unused imports, unused variables, private interface, dead code)

### Sicherheit
- TLS-Unterstützung für alle E-Mail-Protokolle (IMAP, POP3, SMTP)
- STARTTLS für alle drei Protokolle implementiert
- rustls-basierte TLS-Implementierung ohne OpenSSL-Abhängigkeit

[Unreleased]: https://github.com/peopleandpixel/rustyKraken/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/peopleandpixel/rustyKraken/releases/tag/v0.2.0
