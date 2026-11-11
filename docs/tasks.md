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

- [ ] **ntfy-Tentacle** – Empfängt Nachrichten vom ntfy-Push-Benachrichtigungsserver
  - [ ] WebSocket-Verbindung zum ntfy-Broker
  - [ ] Topic-Mapping von ntfy-Themen zu Kraken-Topics
  - [ ] Authentifizierung (Bearer-Token/Basic Auth)
- [ ] **Telegram-Tentacle** – Empfängt Nachrichten via Telegram Bot API
  - [ ] Webhook- oder Polling-Modus
  - [ ] Topic-Erkennung aus Nachrichteninhalt oder Commands
  - [ ] Gruppennachrichten und private Chats unterstützen
- [ ] **Discord-Tentacle** – Empfängt Nachrichten via Discord Webhooks oder Bot
  - [ ] Discord Webhook-Endpoint
  - [ ] Discord Bot mit Gateway-Verbindung
  - [ ] Topic-Mapping aus Channel oder Nachricht

#### Dateibasierte Quellen

- [ ] **File Watcher Tentacle** – Überwacht Ordner auf neue Dateien
  - [ ] `notify`-Crate für Dateisystem-Events
  - [ ] Konfigurierbare Ordner mit Topic-Mapping
  - [ ] Dateityp-Filter (z.B. nur .txt, .json, .csv)
  - [ ] Nach Verarbeitung verschieben/löschen
- [ ] **Stdin Tentacle** – Liest Nachrichten von der Standardeingabe
  - [ ] Zeilenbasiertes Lesen
  - [ ] JSON oder plain-text Format
  - [ ] Sinnvoll für Pipes und Skripte

#### API & Web Services

- [ ] **REST API Poller Tentacle** – Periodisches Abfragen von REST-APIs
  - [ ] Konfigurierbare URLs und Intervalle
  - [ ] Authentifizierung (Bearer, Basic, API-Key)
  - [ ] JSON-Parsing und Topic-Mapping
  - [ ] Rate-Limiting und Fehlerbehandlung
- [ ] **WebSocket Tentacle** – Verbindet zu WebSocket-Servern
  - [ ] Einweg-Verbindung (nur empfangen)
  - [ ] Topic-Mapping aus Nachrichteninhalt
- [ ] **RSS/Atom Feed Tentacle** – Liest RSS/Atom-Feeds
  - [ ] Periodisches Polling konfigurierbarer Feeds
  - [ ] Neue Einträge erkennen und als Ink speichern
  - [ ] Topic aus Feed-Titel oder Kategorie

#### IoT & Hardware

- [ ] **Serial Port Tentacle** – Liest von seriellen Schnittstellen
  - [ ] Konfigurierbare COM-Ports und Baudraten
  - [ ] Protokoll-Parser (z.B. JSON, CSV, custom)
- [ ] **SNMP Trap Tentacle** – Empfängt SNMP-Traps
  - [ ] SNMP-Trap-Listener
  - [ ] OID-Mapping zu Topics

#### Cloud & Dienste

- [ ] **AWS SQS Tentacle** – Liest von Amazon SQS-Queues
  - [ ] Queue-Polling mit Visibility Timeout
  - [ ] Topic aus Message-Attributen
- [ ] **Google Pub/Sub Tentacle** – Empfängt von Google Cloud Pub/Sub
  - [ ] Subscription-Pulling
  - [ ] Topic-Mapping
- [ ] **Slack Tentacle** – Empfängt Nachrichten via Slack Events API
  - [ ] Event Subscription API
  - [ ] Topic aus Channel oder Nachricht

---

## 2. Reflexes (Aktionen & Listener)

### 2.1 Bereits implementiert

- [x] **File-Reflex** – Schreibt Nachrichten in Dateien (JSON-Logging)
- [x] **Command-Reflex** – Führt Shell-Befehle aus mit Umgebungsvariablen (INK_JSON, INK_TOPIC, INK_BODY, INK_ID)

### 2.2 Zu implementierende Reflexes

#### Messaging & Benachrichtigungen

- [ ] **WhatsApp-Reflex** – Sendet Nachrichten via WhatsApp
  - [ ] Integration mit WhatsApp Business API oder Third-Party-Dienst
  - [ ] Template-Engine für Nachrichteninhalt
  - [ ] Konfigurierbare Empfängernummern
- [ ] **Telegram-Reflex** – Sendet Nachrichten via Telegram Bot
  - [ ] Bot-Token-Konfiguration
  - [ ] Chat-ID-Konfiguration (pro Reflex oder dynamisch)
  - [ ] Markdown- und HTML-Formatierung
  - [ ] Rich-Formatting (Bold, Italic, Code)
- [ ] **Discord-Reflex** – Sendet Nachrichten via Discord Webhooks oder Bot
  - [ ] Webhook-URL-Konfiguration
  - [ ] Embed-Nachrichten mit Farben und Feldern
  - [ ] Avatar und Benutzernamen konfigurieren
- [ ] **ntfy-Reflex** – Sendet Push-Benachrichtigungen via ntfy
  - [ ] Broker-URL konfigurierbar
  - [ ] Priorität, Tags, Klick-Links
  - [ ] Authentifizierung
- [ ] **E-Mail-Reflex** – Sendet E-Mails via SMTP
  - [ ] `lettre`-Integration (bereits in Cargo.toml)
  - [ ] Absender, Empfänger, Betreff konfigurierbar
  - [ ] HTML- und Text-Body
  - [ ] Anhänge unterstützen

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

- [ ] **Filter Reflex** – Filtert und transformiert Nachrichten
  - [ ] Bedingte Weiterleitung basierend auf Inhalt
  - [ ] JSON-Patch Transformationen
  - [ ] Topic-Umbenennung
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
- ntfy-Tentacle
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
