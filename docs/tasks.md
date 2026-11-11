nun # 🦑 rustyKraken – Tasks

Abhakbare Aufgabenliste für eine vollständige rustyKraken-Software, gruppiert nach Themenbereichen.

---

## 1. Tentacles (Nachrichtenquellen)

### 1.1 Bereits implementiert

- [x] **HTTP/Webhook-Tentacle** – Empfängt Nachrichten via POST-Requests (Axum)
- [x] **MQTT-Tentacle** – Vollständige Implementierung mit rumqttc 0.25
  - [x] TLS-Verbindung mit rustls (System-Root-CAs, custom CA, mTLS)
  - [x] Last-Will-Messages
  - [x] Clean Session Konfiguration
  - [x] QoS 0/1/2 Unterstützung
  - [x] Authentifizierung (Username/Password)
  - [x] Topic-Mapping mit Wildcard-Substitution (+ und #)
  - [x] Automatische Reconnection mit konfigurierbarer Verzögerung
  - [x] Resubscription nach Reconnection via try_subscribe
  - [x] 7 Unit-Tests für Topic-Matching und Mapping
- [x] **IMAP-Tentacle** – Vollständige Implementierung mit async-imap (TLS, UID-Speicherung, IDLE-Modus, Subject/Sender/All-Modi)

### 1.2 Zu implementierende Tentacles

#### E-Mail & Messaging

- [x] **POP3-Tentacle** – Vollständige Implementierung mit persistenter Message-ID-Speicherung
  - [x] POP3-Verbindung mit TLS (Port 995)
  - [x] POP3-Verbindung ohne TLS (plain, Port 110)
  - [x] STARTTLS-Option (fällt auf plain zurück)
  - [x] Nachrichtendownload und Parsing mit mailparse
  - [x] Topic-Erkennung aus Betreff/Absender
  - [x] Drei Topic-Modi: subject, sender, all
  - [x] Platzhalter-Unterstützung für Absender-Muster
  - [x] Absender-Mappings für zusätzliche Topic-Regeln
  - [x] Optionales Löschen nach Verarbeitung
  - [x] Persistente Message-ID-Speicherung (Vermeidung von Duplikaten)
  - [x] Fallback-Topic, wenn Betreff nicht mit "/" beginnt
  - [x] Umgebungsvariable-Referenzen im Passwort (${VAR_NAME})
  - [x] Generischer Pop3Client<S> für TLS und plain Verbindungen
  - [x] 4 Unit-Tests (Pattern-Matching, Storage)
- [x] **SMTP-Tentacle** – E-Mails via SMTP empfangen (als SMTP-Server)
  - [x] Eigener SMTP-Server auf Basis von tokio
  - [x] TLS-Unterstützung (implizite TLS, Port 465)
  - [x] Topic-Erkennung aus Betreff (Subject-Modus)
  - [x] Topic-Erkennung aus Absender (Sender-Modus)
  - [x] Alle E-Mails an festes Topic (All-Modus)
  - [x] Absender-Mappings mit Platzhaltern
  - [x] AUTH PLAIN Authentifizierung
  - [x] Erlaubte Absender filtern
  - [x] Dot-stuffing Entfernunge
  - [x] Maximale Nachrichtengröße konfigurierbar
  - [x] 4 Unit-Tests (Pattern-Matching, MAIL FROM, RCPT TO)

#### Push-Benachrichtigungen

- [x] **ntfy-Tentacle** – Empfängt Nachrichten vom ntfy-Push-Benachrichtigungsserver
  - [x] JSON Stream-Verbindung (HTTP long-polling)
  - [x] WebSocket-Verbindung zum ntfy-Broker
  - [x] Topic-Mapping von ntfy-Themen zu Kraken-Topics
  - [x] Authentifizierung (Bearer-Token/Basic Auth)
  - [x] Reconnection mit konfigurierbarer Verzögerung
  - [x] ntfy-Metadaten (Title, Priority, Tags, Click, Icon) als Ink-Metadaten
  - [x] 5 Unit-Tests (Topic-Mapping, Ink-Erstellung, JSON-Parsing, WebSocket-Events)
- [x] **Telegram-Tentacle** – Empfängt Nachrichten via Telegram Bot API
  - [x] Webhook-Modus mit Axum-Server
  - [x] Polling-Modus mit long-polling (getUpdates)
  - [x] Topic-Erkennung aus Commands (/kraken_topic)
  - [x] Topic-Erkennung aus Nachrichteninhalt (/topic-Präfix)
  - [x] Topic-Erkennung aus Chat-ID (chat_topic_mapping)
  - [x] Username-basiertes Topic-Mapping
  - [x] Gruppennachrichten, private Chats und Kanäle unterstützen
  - [x] Nachrichtentyp-Erkennung (Text, Foto, Dokument, Voice, Audio, Video, Location, Contact)
  - [x] Secret-Token-Validierung für Webhooks
  - [x] Allowed-Chats/Allowed-Users-Filter
  - [x] Webhook-Registrierung beim Telegram-Server
  - [x] Reconnection mit konfigurierbarer Verzögerung
  - [x] 8 Unit-Tests (Command-Extraktion, Topic-Modi, Ink-Erstellung, Filter, Nachrichtentypen, Webhook)
- [x] **Discord-Tentacle** – Empfängt Nachrichten via Discord Bot Gateway oder Webhook
  - [x] Discord Bot mit Gateway-Verbindung (WebSocket)
  - [x] Heartbeat und Session-Management
  - [x] MESSAGE_CREATE Event-Verarbeitung
  - [x] Webhook-Modus mit Axum-Server
  - [x] Topic-Mapping aus Channel, Guild oder Nachrichteninhalt
  - [x] Topic-Erkennung aus Nachrichteninhalt (/topic-Präfix)
  - [x] Channel-basiertes Topic-Mapping
  - [x] Guild-basiertes Topic-Mapping
  - [x] Drei Topic-Modi: channel, content, all
  - [x] Nachrichtentyp-Erkennung (Text, Bild, Video, Audio, Anhang, Embed, Reply)
  - [x] Rich Metadata (Author, Member/Nickname/Roles, Channel, Guild, Attachments, Embeds, Mentions)
  - [x] Allowed-Channels/Allowed-Guilds/Allowed-Users-Filter
  - [x] Secret-Token-Validierung für Webhooks
  - [x] Reconnection mit konfigurierbarer Verzögerung
  - [x] Sharding-Unterstützung
  - [x] Gateway Intents Konfiguration
  - [x] 13 Unit-Tests (Topic-Modi, Filter, Ink-Erstellung, Nachrichtentypen)

#### Dateibasierte Quellen

- [x] **File Watcher Tentacle** – Überwacht Ordner auf neue Dateien
  - [x] `notify`-Crate für Dateisystem-Events
  - [x] Konfigurierbare Ordner mit Topic-Mapping
  - [x] Dateityp-Filter (z.B. nur .txt, .json, .csv)
  - [x] Nach Verarbeitung verschieben/löschen
  - [x] Topic-Erkennung: folder, filename, content, all (kombiniert)
  - [x] Filename-basiertes Topic-Mapping mit Wildcard-Support
  - [x] Fallback-Topic, wenn kein Topic erkannt wird
  - [x] Rekursive Ordnerüberwachung konfigurierbar
  - [x] Verarbeitungsverzögerung für vollständige Dateischreibung
  - [x] Maximale Dateigröße konfigurierbar
  - [x] Rich Metadata (file_path, file_name, file_size, folder)
  - [x] 11 Unit-Tests (Pattern-Matching, Topic-Modi, Filter, Deserialisierung, Fallback)
- [x] **Stdin Tentacle** – Liest Nachrichten von der Standardeingabe
  - [x] Zeilenbasiertes Lesen
  - [x] JSON oder plain-text Format
  - [x] Auto-Erkennung des Formats (JSON wenn Zeile mit { beginnt)
  - [x] Topic-Erkennung aus Inhalt (/topic-Präfix)
  - [x] Zwei Topic-Modi: content (aus Inhalt), all (Fallback-Topic)
  - [x] Fallback-Topic konfigurierbar
  - [x] JSON-Felder: "topic", "body", "message"
  - [x] EOF und Ctrl+C Handling
  - [x] 16 Unit-Tests (Topic-Extraktion, Body-Extraktion, JSON-Parsing, Format-Erkennung, Konfiguration)

#### API & Web Services

- [x] **REST API Poller Tentacle** – Periodisches Abfragen von REST-APIs
  - [x] Konfigurierbare URLs und Intervalle
  - [x] Authentifizierung (Bearer, Basic, API-Key)
  - [x] JSON-Parsing und Topic-Mapping (json_path, topic_field, body_field)
  - [x] Rate-Limiting und Fehlerbehandlung (Retry-Logik)
  - [x] HTTP-Methoden: GET, POST, PUT, DELETE, PATCH
  - [x] Custom-Header und Request-Body
  - [x] 12 Unit-Tests (JSON-Pfad, String-Feld, Auth, Konfiguration)
- [x] **WebSocket Tentacle** – Verbindet zu WebSocket-Servern
  - [x] Einweg-Verbindung (nur empfangen)
  - [x] Topic-Mapping aus Nachrichteninhalt (field, content, all)
  - [x] ws:// und wss:// Unterstützung
  - [x] Auto-Reconnection mit konfigurierbarer Verzögerung
  - [x] Custom-Header und Subprotocol-Unterstützung
  - [x] Binär-Nachrichten als Base64
  - [x] Maximale Nachrichtengröße konfigurierbar
  - [x] 15 Unit-Tests (Topic-Modi, Body-Extraktion, Konfiguration, Header)
- [x] **RSS/Atom Feed Tentacle** – Liest RSS/Atom-Feeds
  - [x] Periodisches Polling konfigurierbarer Feeds
  - [x] Neue Einträge erkennen und als Ink speichern (Duplicate Detection)
  - [x] Topic aus Feed-Titel, Eintragstitel, Creator oder Fallback
  - [x] RSS 2.0, RSS 1.0 und Atom Feed Unterstützung
  - [x] Auto-Erkennung des Feed-Typs
  - [x] Retry-Logik bei Fehlern
  - [x] 15 Unit-Tests (Topic-Modi, Body-Erstellung, Feed-Parsing, Konfiguration)

#### IoT & Hardware

- [x] **Serial Port Tentacle** – Liest von seriellen Schnittstellen
  - [x] Konfigurierbare COM-Ports und Baudraten
  - [x] Protokoll-Parser (plain, JSON, CSV, custom)
  - [x] Custom-Delimiter als Hex-String
  - [x] Port-basiertes Topic-Mapping
  - [x] Auto-Reconnection mit konfigurierbarer Verzögerung
  - [x] Buffer-Overflow-Schutz
  - [x] Rich Metadata (port, baud_rate, parser)
  - [x] 11 Unit-Tests (Topic-Modi, Parser, Konfiguration, Deserialisierung)
- [x] **SNMP Trap Tentacle** – Empfängt SNMP-Traps
  - [x] UDP-Trap-Listener auf konfigurierbarer Adresse
  - [x] SNMP v1/v2c/v3 Unterstützung
  - [x] OID-Mapping zu Topics (exact match und prefix match)
  - [x] Längstes-Prefix-Matching für OID-Mappings
  - [x] Varbinds als JSON-Body
  - [x] Rich Metadata (trap_oid, version, community, message_type)
  - [x] 13 Unit-Tests (Topic-Modi, SNMP-Werte, Konfiguration, Deserialisierung)

#### Cloud & Dienste

- [x] **AWS SQS Tentacle** – Liest von Amazon SQS-Queues
  - [x] Queue-Polling mit Visibility Timeout
  - [x] AWS Signature v4 Authentifizierung
  - [x] Topic aus Message-Attributen (attribute, body, all Modi)
  - [x] Queue-URL zu Topic Mapping
  - [x] Nachrichten nach Verarbeitung löschen
  - [x] XML-Response Parsing
  - [x] 7 Unit-Tests (Topic-Modi, XML-Parsing, Konfiguration)
- [x] **Google Pub/Sub Tentacle** – Empfängt von Google Cloud Pub/Sub
  - [x] Subscription-Pulling via REST API
  - [x] OAuth2 JWT Authentifizierung (Service Account)
  - [x] Topic-Mapping (attribute, data, all Modi)
  - [x] Base64-Daten Decodierung
  - [x] Auto-Acknowledge
  - [x] 6 Unit-Tests (Topic-Modi, Base64, Ink-Erstellung)
- [x] **Slack Tentacle** – Empfängt Nachrichten via Slack Events API
  - [x] Event Subscription API mit Axum-Server
  - [x] Signing Secret Verifizierung (HMAC-SHA256)
  - [x] URL Verification Challenge
  - [x] Topic aus Channel oder Nachricht (channel, content, all Modi)
  - [x] Channel und User Filtering
  - [x] Rich Metadata (User-Profile, Files, Thread)
  - [x] 8 Unit-Tests (Topic-Modi, Filtering, Ink-Erstellung)

---

## 2. Reflexes (Aktionen & Listener)

### 2.1 Bereits implementiert

- [x] **File-Reflex** – Schreibt Nachrichten in Dateien (JSON-Logging)
- [x] **Command-Reflex** – Führt Shell-Befehle aus mit Umgebungsvariablen (INK_JSON, INK_TOPIC, INK_BODY, INK_ID)

### 2.2 Zu implementierende Reflexes

#### Messaging & Benachrichtigungen

- [x] **WhatsApp-Reflex** – Sendet Nachrichten via WhatsApp
  - [x] Integration mit WhatsApp Business API (Graph API)
  - [x] Template-Engine für Nachrichteninhalt ({{topic}}, {{body}}, {{id}}, {{timestamp}}, {{metadata.<key>}})
  - [x] Konfigurierbare Empfängernummern
  - [x] Text- und Template-Nachrichten
  - [x] Topic-basiertes Recipient-Mapping
  - [x] Topic-basiertes Template-Mapping
  - [x] Drittanbieter-API-Unterstützung (custom api_base_url)
  - [x] Optionale Metadata-Einbindung in Nachrichten
  - [x] 16 Unit-Tests (Message-Type, Recipients, Templates, Body-Rendering, Config)
- [x] **Telegram-Reflex** – Sendet Nachrichten via Telegram Bot
  - [x] Bot-Token-Konfiguration
  - [x] Chat-ID-Konfiguration (pro Reflex oder dynamisch via topic_chat_mapping)
  - [x] Markdown- und HTML-Formatierung (parse_mode: Markdown, HTML, None)
  - [x] Topic-basiertes Chat-ID-Mapping
  - [x] Topic-basiertes Parse-Mode-Mapping
  - [x] Template-Engine für Nachrichteninhalt ({{topic}}, {{body}}, {{id}}, {{timestamp}}, {{metadata.<key>}})
  - [x] Nachrichtentrunkierung bei Telegram-Limit (4096 Zeichen)
  - [x] Optionale Metadata-Einbindung in Nachrichten
  - [x] Custom API Base URL für Proxies
  - [x] 10 Unit-Tests (Chat-ID-Mapping, Parse-Mode-Mapping, Template-Rendering, Trunkierung, Metadata, Deserialisierung)
- [x] **Discord-Reflex** – Sendet Nachrichten via Discord Webhooks oder Bot
  - [x] Webhook-URL-Konfiguration
  - [x] Embed-Nachrichten mit Farben und Feldern
  - [x] Avatar und Benutzernamen konfigurieren
  - [x] Template-Engine für Nachrichteninhalt ({{topic}}, {{body}}, {{id}}, {{timestamp}}, {{metadata.<key>}})
  - [x] Topic-basiertes Webhook-URL-Mapping
  - [x] Topic-basiertes Embed-Farben-Mapping
  - [x] Ink-Metadaten als Embed-Felder
  - [x] Embed-Titel, Beschreibung, Footer mit Templates
  - [x] 15 Unit-Tests (Webhook-Mapping, Farben, Templates, Embeds, Trunkierung, Deserialisierung)
- [x] **ntfy-Reflex** – Sendet Push-Benachrichtigungen via ntfy
  - [x] Broker-URL konfigurierbar
  - [x] Priorität, Tags, Klick-Links
  - [x] Authentifizierung (Basic Auth)
  - [x] Template-Engine für Titel und Nachricht ({{topic}}, {{body}}, {{id}}, {{timestamp}}, {{metadata.<key>}})
  - [x] Topic-basiertes ntfy-Topic-Mapping
  - [x] Topic-basiertes Prioritäten-Mapping
  - [x] Topic-basiertes Tags-Mapping
  - [x] Icon und Markdown-Unterstützung
  - [x] 12 Unit-Tests (Topic-Mapping, Prioritäten, Tags, Templates, Metadaten, Deserialisierung)
- [x] **E-Mail-Reflex** – Sendet E-Mails via SMTP
  - [x] `lettre`-Integration (bereits in Cargo.toml)
  - [x] Absender, Empfänger, Betreff konfigurierbar
  - [x] HTML- und Text-Body
  - [x] Anhänge unterstützen
  - [x] CC und BCC Empfänger
  - [x] Template-Engine für Betreff und Body ({{topic}}, {{body}}, {{id}}, {{timestamp}}, {{metadata.<key>}})
  - [x] Topic-basiertes Recipient-Mapping
  - [x] Topic-basiertes Subject-Mapping
  - [x] STARTTLS und implizite TLS Unterstützung
  - [x] 12 Unit-Tests (Recipient-Mapping, Subject-Mapping, Templates, Metadaten, Deserialisierung)

#### Web & API

- [ ] **HTTP/Webhook-Reflex** – Sendet HTTP-Requests an externe APIs
  - [ ] GET, POST, PUT, DELETE Methoden
  - [ ] Header-Konfiguration
  - [ ] JSON-Body mit Template-Engine
  - [ ] Authentifizierung (Bearer, Basic, API-Key)
  - [ ] Retry-Logik bei Fehlern
- [ ] **Matrix/Element-Reflex** – Sendet Nachrichten via Matrix
  - [ ] Matrix-Client mit `ruma`-Crate
  - [ ] Raum-ID und Benutzer-Konfiguration

#### Dateibasierte Aktionen

- [ ] **File Write Reflex** – Schreibt Nachrichten in Dateien (erweitert)
  - [ ] Verschiedene Formate (JSON, CSV, plain text, XML)
  - [ ] Template-Engine für Dateiinhalte
  - [ ] Rotation und Größenlimits
- [ ] **Database Reflex** – Speichert Nachrichten in einer Datenbank
  - [ ] PostgreSQL-Unterstützung
  - [ ] MySQL/MariaDB-Unterstützung
  - [ ] Konfigurierbare Tabellenschemata

#### System & Automatisierung

- [ ] **Script-Reflex** – Führt Skripte aus (erweitert)
  - [ ] Lua-Scripting mit `mlua`
  - [ ] JavaScript-Scripting mit `rquickjs`
  - [ ] Python-Scripting via Subprocess
  - [ ] Zugriff auf Ink-Daten im Skript
- [ ] **Home Assistant Reflex** – Sendet Events an Home Assistant
  - [ ] REST API Integration
  - [ ] Event-Fire und Service-Call
- [ ] **Syslog Reflex** – Sendet Nachrichten an Syslog
  - [ ] UDP/TLS Syslog
  - [ ] Konfigurierbare Priorität und Facility

#### Transformation & Routing

- [x] **Tentacle Reflex (Filter/Forward)** – Filtert und weiterleitet Nachrichten
  - [x] Bedingte Weiterleitung basierend auf Topic- und Body-Filtern (Regex)
  - [x] Topic-Umbenennung mit exaktem Mapping, Prefix-Mapping und Template-Engine
  - [x] Body-Transformation mit Template-Engine ({{topic}}, {{body}}, {{id}}, {{timestamp}}, {{metadata.<key>}})
  - [x] Metadata-Erhaltung oder -Filterung mit konfigurierbaren Schlüsseln
  - [x] Custom Metadata-Felder hinzufügen
  - [x] Original-Topic als __original_topic für Tracing
  - [x] 16 Unit-Tests (Topic-Mapping, Prefix-Mapping, Template-Rendering, Filter, Metadata, Deserialisierung)
- [ ] **Aggregator Reflex** – Sammelt und aggregiert Nachrichten
  - [ ] Zeitbasierte Aggregation (z.B. alle 5 Minuten)
  - [ ] Zählung, Summe, Durchschnitt
  - [ ] Zusammenfassung als einzelne Nachricht

---

## 3. Performanceoptimierungen

### 3.1 Event Bus

- [ ] **Channel-Größe optimieren** – Aktuelle feste Größe (1000) durch konfigurierbare Größe ersetzen
- [ ] **Prioritäts-Warteschlange** – Wichtige Nachrichten (z.B. Warnungen) priorisieren
- [ ] **Backpressure-Handling** – Verhaltun bei überlastetem EventBus definieren (drop oldest, block, sample)
- [ ] **Metriken** – Nachrichtenrate, Latenz, Queue-Größe messen und exponieren

### 3.2 Mantle (Speicherung)

- [ ] **Batch-Speicherung** – Mehrere Ink-Nachrichten in einem Transaction speichern
- [ ] **WAL-Modus** – SQLite Write-Ahead Logging aktivieren für bessere Schreibperformance
- [ ] **Vakuum und Optimierung** – Regelmäßige Datenbankwartung planen
- [ ] **Partitionierung** – Alte Nachrichten in Archiv-Tabellen verschieben
- [ ] **Full-Text-Suche** – FTS5-Index für Volltextsuche in Nachrichteninhalt
- [ ] **Alternative Datenbanken** – PostgreSQL/MySQL-Support als Option neben SQLite
- [ ] **Komprimierung** – Große Nachrichten komprimiert speichern (zlib/zstd)

### 3.3 Tentacles

- [ ] **Connection Pooling** – Wiederverwendung von Verbindungen (IMAP, HTTP)
- [ ] **Async I/O Optimierung** – Alle I/O-Operationen sind asynchron (prüfen und optimieren)
- [ ] **Rate Limiting** – Pro Tentacle konfigurierbare Rate-Limits
- [ ] **Circuit Breaker** – Bei wiederholten Fehlern temporär pausieren
- [ ] **Parallel Processing** – Mehrere Nachrichten parallel verarbeiten

### 3.4 Reflexes

- [ ] **Parallel Execution** – Mehrere Reflexes parallel ausführen
- [ ] **Retry-Logik** – Exponentielles Backoff bei fehlgeschlagenen Aktionen
- [ ] **Dead Letter Queue** – Fehlgeschlagene Nachrichten separat speichern
- [ ] **Throttling** – Maximal N Aktionen pro Zeitfenster

### 3.5 Allgemein

- [ ] **Memory Profiling** – Speicherverbrauch mit `dhat` oder `heaptrack` analysieren
- [ ] **CPU Profiling** – Hotspots mit `perf` oder `flamegraph` identifizieren
- [ ] **Tokio Console** – Integration für Runtime-Metriken und Debugging
- [ ] **Prometheus Metriken** – Exponiere Metriken via `/metrics` Endpoint
- [ ] **Graceful Shutdown** – Sauberes Beenden aller Tentacles und Reflexes mit Signal-Handling

---

## 4. Sonstige Optimierungen & Features

### 4.1 Konfiguration

- [ ] **Hot-Reload** – Config-Änderungen ohne Neustart erkennen und anwenden
  - [ ] `notify`-Crate für `config/incoming/` und `config/listener/` überwachen
  - [ ] Neue Tentacles/Reflexes dynamisch starten
  - [ ] Entfernte Tentacles/Reflexes sauber stoppen
- [ ] **Config-Validierung** – Schema-Validierung beim Laden mit Fehlermeldungen
- [ ] **Umgebungsvariablen** – `${VAR}`-Ersetzung in allen Config-Dateien
- [ ] **Secrets-Management** – Integration mit HashiCorp Vault oder similar
- [ ] **Config-Beispiele** – Vollständige Beispiel-Configs für alle Tentacle/Reflex-Typen

### 4.2 Template-Engine

- [ ] **Template-Sprache** – `{{message.body}}`, `{{message.topic}}`, `{{message.timestamp}}` etc.
- [ ] **Filter und Funktionen** – `{{message.body | uppercase}}`, `{{message.body | truncate: 100}}`
- [ ] **Bedingungen** – `{% if message.status == "new" %}...{% endif %}`
- [ ] **Crate-Auswahl** – `tera`, `askama`, oder `minijinja` evaluieren und integrieren

### 4.3 Web-UI & API

- [ ] **REST API** – HTTP-API für Abfragen und Steuerung
  - [ ] `GET /api/ink` – Liste der neuesten Nachrichten
  - [ ] `GET /api/ink/{id}` – Einzelne Nachricht
  - [ ] `GET /api/ink/topic/{topic}` – Nachrichten nach Topic
  - [ ] `POST /api/ink` – Manuelles Einspeisen von Nachrichten
  - [ ] `GET /api/health` – Health-Check
  - [ ] `GET /api/status` – Status aller Tentacles und Reflexes
- [ ] **Web-UI** – Einfache Weboberfläche
  - [ ] Nachrichtenübersicht mit Filter und Suche
  - [ ] Echtzeit-Updates via Server-Sent Events (SSE) oder WebSocket
  - [ ] Config-Editor für Tentacles und Reflexes
  - [ ] Dashboard mit Statistiken
- [ ] **Authentifizierung** – Schutz der API und Web-UI
  - [ ] Basic Auth oder Token-basiert
  - [ ] JWT-Unterstützung
  - [ ] Role-Based Access Control (RBAC)

### 4.4 Logging & Monitoring

- [ ] **Strukturiertes Logging** – JSON-Logs mit `tracing-json`
- [ ] **Log-Level pro Modul** – Fein granulare Log-Konfiguration
- [ ] **Health Checks** – Regelmäßige Prüfung aller Tentacles und Reflexes
- [ ] **Alerting** – Benachrichtigung bei kritischen Fehlern
- [ ] **Audit-Log** – Protokollierung von Config-Änderungen und Aktionen

### 4.5 Testing & Qualität

- [ ] **Integrationstests** – End-to-End-Tests mit echten Diensten (Docker-Container)
- [ ] **Unit-Tests** – Erhöhte Testabdeckung für alle Module
- [ ] **Fuzz-Testing** – `cargo-fuzz` für Eingabevalidierung
- [ ] **CI/CD Pipeline** – GitHub Actions oder GitLab CI
  - [ ] Build und Test bei jedem Commit
  - [ ] Clippy und rustfmt-Prüfung
  - [ ] Security-Audits mit `cargo-audit`
- [ ] **Dokumentation** – KDoc-Kommentare für alle öffentlichen APIs
- [ ] **Benchmark-Tests** – Performance-Tests mit `criterion`

### 4.6 Deployment & Betrieb

- [ ] **Systemd-Service** – `.service`-Datei für Linux-Daemon
- [ ] **Docker-Image** – Offizielles Docker-Image mit Multi-Stage-Build
- [ ] **Docker Compose** – Beispiel-Setup mit MQTT, ntfy, etc.
- [ ] **Systemd-Path** – Auto-Start bei Systemstart
- [ ] **Log-Rotation** – `logrotate`-Konfiguration
- [ ] **Update-Mechanismus** – Selbstupdate oder Package-Manager-Integration
- [ ] **Plattformen** – Releases für Linux, macOS, Windows

### 4.7 Sicherheit

- [ ] **Input-Validierung** – Alle externen Eingaben validieren und sanitieren
- [ ] **TLS-Zertifizierung** – Zertifikatsvalidierung bei allen externen Verbindungen
- [ ] **Command-Injection-Schutz** – Shell-Befehle im Command-Reflex sicher ausführen
- [ ] **Rate-Limiting** – Schutz vor DoS bei HTTP-Webhook
- [ ] **CORS-Konfiguration** – Bei Web-UI und API
- [ ] **Security-Audit** – Regelmäßige `cargo-audit`-Prüfung

### 4.8 Plugin-System

- [ ] **Dynamisches Laden** – Tentacles und Reflexes als separate Shared Libraries (.so/.dll)
- [ ] **Plugin-API** – Stabile, versionierte API für externe Plugins
- [ ] **Plugin-Registry** – Zentrale Liste verfügbarer Plugins
- [ ] **Sandboxing** – Isolation von Plugins für Sicherheit

### 4.9 Nachrichtenversionierung & Migration

- [ ] **Ink-Schema-Versionierung** – Versionsfeld im Ink-Format
- [ ] **Migration-Tools** – Hilfsprogramme für Datenbank- und Config-Migrationen
- [ ] **Abwärtskompatibilität** – Alte Config-Formate unterstützen

### 4.10 Dokumentation

- [ ] **Architektur-Dokument** – Detaillierte Beschreibung der Architektur
- [ ] **API-Dokumentation** – Auto-generiert via `cargo doc`
- [ ] **Tutorials** – Schritt-für-Schritt-Anleitungen für häufige Use-Cases
- [ ] **FAQ** – Häufig gestellte Fragen und Lösungen
- [ ] **Changelog** – Versionshistorie mit `cargo-release`

---

## 5. Known Issues & Technische Schulden

- [ ] **IMAP STARTTLS** – async-imap unterstützt STARTTLS nicht direkt; Config-Option warnt und fällt auf implizite TLS zurück
- [ ] **IMAP IDLE-Loop** – Beendet sich nach der ersten Nachricht und bricht zur Hauptschleife zurück; kontinuierlicher IDLE-Betrieb optimieren
- [ ] **Warnungen** – 19 Compiler-Warnungen (unused imports, dead code, etc.) beseitigen
- [ ] **serde_yaml deprecated** – Ersatz `yaml_serde` via Package-Renaming verwendet; auf stabile Lösung achten
- [ ] **imap-proto Warnings** – Future-Incompatibility-Warnungen in imap-proto v0.10.2; Upgrade-Pfad prüfen
- [ ] **Mantle Session Ownership** – Session wird in `fetch_and_process_new_emails` consumed; IDLE-Modus muss neu verbinden

---

## Priorisierung

### Phase 1 – MVP-Ergänzung (hohe Priorität)
- Template-Engine
- HTTP/Webhook-Reflex
- E-Mail-Reflex (lettre)
- Config Hot-Reload
- REST API (Basis)
- Warnungen beseitigen
- Integrationstests

### Phase 2 – Erweiterung (mittlere Priorität)
- Telegram-Reflex
- Discord-Reflex
- File Watcher Tentacle
- Web-UI (Basis)
- Batch-Speicherung im Mantle
- Prometheus Metriken

### Phase 3 – Polish & Skalierung (niedrigere Priorität)
- Plugin-System
- Alternative Datenbanken
- Authentifizierung & RBAC
- Docker-Image und Deployment
- Performance-Profiling und Optimierung
- Fuzz-Testing
