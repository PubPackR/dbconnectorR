# Design-Spec: CRM-Task-Meetings in die Event-Classification einspeisen

- **Datum:** 2026-07-24
- **Autor:** Moritz Hemmann
- **Ticket:** [call-monitoring] VC-Tool-Strings (Teams/WebEx) aus Terminen ins Call-Monitoring uebernehmen — Asana `1216701790869556` (Sales)
- **Verwandt (Dublette, wird geschlossen):** [vc-tracking] Asana `1216494020683105` (Revenue Operation)
- **Status:** Draft — zur Review

---

## 1. Kontext & Problem

Der Vertrieb dokumentiert Termine als **CRM-Tasks** (Centralstation, `raw.crm_lead_tasks`). In diesen Tasks stecken Meeting-Informationen, die wir **nirgends sonst** abgreifen koennen:

- **Externe VC-Tools** (WebEx, Zoom, ...): Diese Meetings finden ausserhalb von Microsoft Teams statt und sind fuer die **MSGraph-Exporte unsichtbar**. Der Vertrieb notiert das Tool als Freitext im Task-Namen (z.B. `VC Webex ZR mit Frings`).
- **No-Show / Show-Up**: als **Kommentar** an der Task (`raw.crm_lead_task_comments`).
- **Storno**: ebenfalls als Kommentar.

Alle bestehenden Meeting-Dashboards (shiny-22, shiny-25; ~8 Module) speisen sich aus der MSGraph-Welt, konkret aus der kanonischen Faktentabelle `processed.msgraph_extern_event_classification`. Die CRM-Task-Meetings fehlen dort komplett — dadurch fehlen extern-getoolte Meetings in den Zahlen, und der WebEx-Fall wurde in der Vergangenheit sogar als Off-Teams-No-Show-False-Positive fehlinterpretiert.

**Wert:** Die CRM-Tasks **ergaenzen** die Meeting-Wahrheit (externe Tools, No-Show, Storno) — sie ersetzen sie nicht.

## 2. Ziel & Non-Goals

**Ziel:** CRM-Task-Meetings, die MSGraph nicht kennt, als vollwertige Zeilen in die kanonische Meeting-Faktentabelle `processed.msgraph_extern_event_classification` einspeisen, sodass **alle Leser dieser Tabelle** sie ohne Dashboard-Umbau sehen — inklusive Tool-Name und einem sauberen Meeting-Status (`show_up` / `no_show` / `storniert` / `unbekannt`).

**Non-Goals:**
- Keine Aenderung der MSGraph-No-Show-Ableitung fuer bestehende Zeilen.
- Kein Nachziehen der Direktleser-Module (`funnel_cohort`, `closer_performance`, `sales_pipeline`, `churn`) in diesem Ticket — sie profitieren automatisch, soweit sie die Tabelle bzw. den geteilten Helper lesen; gesonderte Betrachtung spaeter.
- Keine Migration der bestehenden Google-Sheet-Videocall-KPIs (`raw.gsheet_videocall_kpis`) — separate, parallele Quelle.

## 3. Gewaehlte Architektur (Y2 + sequentieller Full-Set-Upsert)

CRM-Meetings werden **physisch als Zeilen** in `processed.msgraph_extern_event_classification` geschrieben (eine kanonische Tabelle, alle Leser profitieren). Weil die Tabelle msgraph-spezifisch geschnitten ist, sind zwei bewusste Anpassungen noetig (Variante **Y2**):

1. `call_event_mapping_id` wird **NULLbar**; CRM-Zeilen tragen ihr Datum in einer neuen Spalte `crm_event_date`. Der Dashboard-Helper joint per **LEFT JOIN** auf `mapping.msgraph_call_event` und nimmt `coalesce(mapping.event_date, crm_event_date)`.
2. Vier neue Spalten: `source`, `meeting_tool`, `meeting_status`, `is_external_tool`.

**Schreibmechanik (Variante a, sequentiell):** Der bestehende MSGraph-Producer bleibt **unveraendert** (`delete_missing = TRUE`). Ein **neuer CRM-Schritt** laeuft als **letzter Schritt im selben `base-35/do/main.R`-Lauf**, also sequentiell nach dem MSGraph-Producer:

1. MSGraph-Producer schreibt wie heute nur MSGraph-Zeilen (raeumt CRM-Zeilen dabei kurz weg).
2. CRM-Schritt liest die Tabelle (= frische MSGraph-Zeilen), berechnet die CRM-Zeilen, und upsertet das **Gesamtset** (MSGraph + CRM) mit `delete_missing = TRUE`.

Weil beide Schritte im selben sequentiellen Prozess laufen, gibt es **kein Ordering-Rennen**, und das erprobte `delete_missing = TRUE` samt eingebautem >50%-Guard bleibt in Benutzung. Preis: der CRM-Schritt schreibt die MSGraph-Zeilen jedes Mal mit zurueck (Write-Churn, idempotent).

### 3.1 Verworfene Alternativen (Kurzbegruendung)

- **X / Spiegel-Tabelle (`meetings_unified` materialisiert):** Schema-Drift-Pflege; erreicht nur Helper-Module. Verworfen zugunsten *einer* Tabelle.
- **Y1 (synthetische `mapping.msgraph_call_event`-Zeilen):** vermeidet Helper-Aenderung, erfordert aber Schreiben in eine **zweite** Produktionstabelle mit synthetischen Zeilen. Verworfen zugunsten der einfacheren `crm_event_date`-Spalte (Y2).
- **b (scoped delete):** entkoppelt die Jobs, verlangt aber einen Eingriff in den MSGraph-Producer-Delete plus custom Lösch-Code inkl. selbst nachgebautem Guard. Verworfen, weil (a-sequentiell) den Kern-Producer **unangetastet** laesst.
- **c (View ueber zwei Tabellen):** loest das delete_missing-Dilemma am saubersten, verlangt aber das Nachziehen der Konsumenten auf die View — mehr bewegliche Teile. Bewusst verworfen (Risiko-Abwaegung des Requesters).

## 4. Datenquellen & Grain

| Quelle | Rolle | Schluesselspalten |
|---|---|---|
| `raw.crm_lead_tasks` | Termin-Identitaet + Tool im Namen | `crm_task_id`, `lead_id`, `user_id`/`assigned_to_user_id`, `precise_time`, `task_name`, `is_finished`, `is_deleted`, `comments_count` |
| `raw.crm_lead_task_comments` | No-Show/Show-Up/Storno | `crm_comment_id`, `task_id` (→ `crm_task_id`), `comment_name`, `comment_created_at`, `is_deleted` |
| `raw.crm_users` | CRM-User → Name/Login | `crm_user_id`, `user_login`, `user_name`, `user_first_name` |
| `mapping.vw_service_users` | CRM-User → Personio | `personio_user_id`, `connected_service`, `service_user_id` |
| `mapping.crm_lead_msgraph_contact` | Lead → msgraph_contact | `crm_lead_id`, `msgraph_contact_id`, `is_primary_crm` |
| `raw.personio_persons` | Personio → E-Mail/Name | `id`, `email`, `first_name`, `name` |

**Grain der neuen CRM-Zeilen:** eine Zeile pro **VC-Termin** (`crm_task_id`), Kontakt = aufgeloester Lead- bzw. SDR-Kontakt (analog zur bestehenden `contact_id`-Semantik).

## 5. Komponenten

### 5.1 Parser (dbconnectorR, neue Datei `R/crm_task_meeting_parser.R`)

Reine, unit-testbare Funktionen (kein DB-Zugriff), damit die Freitext-Logik isoliert getestet werden kann.

**VC-Termin-Erkennung:** Ein Task gilt als VC-Termin, wenn `task_name` den Marker `\bVC\b` (case-insensitive) enthaelt **oder** ein Tool-Keyword matcht. Badge `visit` ist Zusatzsignal, keine harte Bedingung.

**Tool-Extraktion** (priorisierter, case-insensitiver Match auf `task_name`):
`Webex` (inkl. `Web Ex`/`WEBEX`) › `Zoom` › `Google Meet` › `Teams` › `Skype` → sonst `unbekannt`.
Bewusst dokumentierte Grenze: Freitext ist nicht 100% praezise. Bekannter False-Positive aus echten Daten: `...findet Telefonate besser als geplante Teams Sitzungen...` (Praeferenz-Notiz, kein Tool). `Teams` ist daher niedrig priorisiert; der Fall ist Testfall.

**Status-Klassifikation** — **ausschliesslich aus `comment_name`** (nicht aus dem Task-Namen; dort stehen irrefuehrende Notizen wie `No-Show, muss neu terminieren`, die sich auf *vergangene* Termine beziehen):
`storniert` (abgesagt/verschoben-vorher) › `no_show` (nicht erschienen/kam nicht/no show) › `show_up` (erschienen/stattgefunden/gehalten) → sonst `unbekannt`.

### 5.2 Identitaetsaufloesung

- **SDR/Organizer:** `crm_lead_tasks.user_id` → `crm_users.crm_user_id` → Personio. Primaer ueber `mapping.vw_service_users` mit `connected_service = 'crm'` (falls vorhanden, siehe §12). Fallback: `crm_users.user_login` → `personio_persons.email` (lower, getrimmt). Scheitert beides → Policy §8.
- **Lead-Kontakt:** `crm_lead_tasks.lead_id` → `mapping.crm_lead_msgraph_contact.msgraph_contact_id` (bevorzugt `is_primary_crm = TRUE`) → fuellt `contact_id`.

### 5.3 Anti-Join (nur ergaenzen, was MSGraph nicht kennt)

Ein CRM-Termin wird **nur** aufgenommen, wenn er nicht bereits als MSGraph-Meeting existiert. Robuste, erklaerbare Regel:

- **Tool ist extern (WebEx/Zoom/Skype/Google Meet):** per Definition **nicht** in MSGraph → **aufnehmen**.
- **Tool ist Teams oder unbekannt:** nur aufnehmen, wenn **kein** MSGraph-Meeting auf `lead + Kalendertag` (Europe/Berlin) matcht (Match gegen die bestehende Klassifikation via `contact_id` + `event_date`). Sonst als Duplikat verwerfen.

### 5.4 Schema-Migration

Auf `processed.msgraph_extern_event_classification`:

- `call_event_mapping_id` → **NULLbar** (bestehende Zeilen unveraendert NOT NULL-Werte).
- `+ source text NOT NULL DEFAULT 'msgraph'`
- `+ meeting_tool text` (nullable; nur CRM-Zeilen)
- `+ meeting_status text` (nullable; `show_up`/`no_show`/`storniert`/`unbekannt`; nur CRM-Zeilen)
- `+ is_external_tool boolean` (nullable; TRUE bei WebEx/Zoom/... )
- `+ crm_event_date date` (nullable; Termin-Datum der CRM-Zeile)

Migration via `/create-db-schema`-Standard (idempotentes ALTER, Bestandszeilen bekommen `source='msgraph'`).

### 5.5 CRM-Producer-Schritt (dbconnectorR, `R/update_crm_task_meeting_classification.R`)

Neue Funktion, aufgerufen als letzter Schritt in `base-35-export_teams_history/do/main.R` (nach `update_extern_event_classification`):

1. `needed_tables` erweitern um `raw.crm_lead_tasks`, `raw.crm_lead_task_comments`, `raw.crm_users`, `mapping.vw_service_users`, `mapping.crm_lead_msgraph_contact`, `raw.personio_persons`, `processed.msgraph_extern_event_classification`.
2. VC-Termine + Kommentare laden (nur `is_deleted = FALSE`), parsen (§5.1), Identitaet aufloesen (§5.2), Anti-Join (§5.3).
3. CRM-Zeilen ins Klassifikations-Schema mappen: `call_event_mapping_id = NA`, `crm_event_date`, `contact_id`, `is_responsible = TRUE`, `is_organizer = TRUE` (bzw. `'(extern/unbekannt)'`-Policy), `is_no_show` aus `meeting_status`, `excluded` aus `meeting_status = 'storniert'`, `original_created_at = task_created_at`, `is_short_lived_event = FALSE`, `source = 'crm_task'`, `meeting_tool`, `meeting_status`, `is_external_tool`.
4. Bestehende Tabelle lesen (= frische MSGraph-Zeilen), `bind_rows(msgraph, crm)`, **ein** `Billomatics::postgres_upsert_data(..., delete_missing = TRUE)`.

**Fail-safe** (hier bewusst erlaubt — eine Quelle darf den Rest nicht stoppen): Wirft der CRM-Block einen Fehler, wird per `log4r` geloggt und der Lauf **ohne** CRM-Zeilen beendet; der Kern-Refresh crasht nie am Zusatz. `bit64`/integer64-Typen beim Round-Trip beachten (`library(bit64)` vor Casts).

### 5.6 Helper-Anpassung (shiny-99-modules)

`func/module_sales_kpi/external_events_helpers.R`, `get_responsible_event_pool`:

- Join auf `mapping.msgraph_call_event` von INNER → **LEFT JOIN**.
- `event_date = coalesce(mapping.event_date, crm_event_date)`.
- `meeting_tool`, `meeting_status`, `is_external_tool` mit durchreichen.

Damit erscheinen CRM-Zeilen automatisch in No-Show, Scheduling, SDR-Monitoring.

### 5.7 Display (shiny-99-modules)

Neuer Content-Sub-Tab `module_kpi_videocalls_vc_tool.R` im Container `module_kpi_videocalls.R` (5. Tab „VC-Tool & Show-Up"): Termin-Tabelle (Datum · Sales-Rep · Lead · Tool · Status) + Aggregat (Tool-Verteilung, Show-Up-Rate), Monats-Picker (Haus-Standard). Nutzt `is_external_tool`/`meeting_status` sichtbar.

## 6. Status-Vokabular & Mapping

| `meeting_status` | `is_no_show` | `excluded` | Bedeutung |
|---|---|---|---|
| `show_up` | FALSE | FALSE | fand statt, Gegenseite da |
| `no_show` | TRUE | FALSE | Slot da, Gegenseite kam nicht |
| `storniert` | FALSE | TRUE | fand nicht statt (aus Zaehlungen raus) |
| `unbekannt` | FALSE | FALSE | kein Kommentar → als stattgefunden behandelt (konservativ) |

## 7. Fallstricke → Aufloesung

| # | Fallstrick | Aufloesung |
|---|---|---|
| A1 | No-Show nicht aus `event_class` ableitbar; Storno hat kein Zuhause | `meeting_status` nativ; `is_no_show`/`excluded` direkt gesetzt (§6) |
| A2 | Anti-Join fragil (Duplikat vs. externes Meeting verlieren) | Tool-gestuetzte Regel + `lead + Tag`-Match (§5.3) |
| A3 | SDR nicht auf Personio aufloesbar → stiller Drop | Policy: aufnehmen als `'(extern/unbekannt)'`, nicht verwerfen (§8) |
| B1 | `delete_missing = TRUE` tabellenweit | sequentieller Full-Set-Upsert, MSGraph-Producer unveraendert (§3) |
| B2 | Backfill aendert historische Zahlen | volle Historie in Tabelle; Helper-Delta vor Live-Schaltung messen (§8, §11) |
| C1 | 8 Konsumenten, neuer Status-Wert | `storniert → excluded=TRUE` → faellt automatisch aus Zaehlungen |

## 8. Produktentscheidungen (Defaults — im Review bestaetigen)

1. **Storno:** in der Tabelle mit `meeting_status='storniert'`, `excluded=TRUE` (sichtbar, zaehlt nicht).
2. **Backfill:** volle Historie; Helper-Umlegung (LEFT-Join/Sichtbarkeit) erst nach Delta-Review.
3. **Nicht-aufloesbarer SDR:** aufnehmen mit `organizer='(extern/unbekannt)'`, nicht verwerfen.

## 9. Test-Strategie

- **Unit-Tests (dbconnectorR `tests/testthat/`):** Parser (Tool-Extraktion inkl. Teams-False-Positive, Case-Varianten `Webex/WebEx/WEBEX`, `Nur ueber Zoom VC UC`; Status-Klassifikation inkl. Storno) — gegen echte Beispielstrings.
- **Unit-Tests Anti-Join:** externes Tool → immer rein; Teams mit MSGraph-Match am selben Tag → raus; ohne Match → rein.
- **Integrations-Check:** `parse()`-Syntaxcheck; Trockenlauf des CRM-Schritts auf einer Kopie; Row-Count-Sanity (CRM-Zeilen << MSGraph-Zeilen).
- **Delta-Messung:** No-Show-Rate je Modul vor/nach Helper-Umlegung.

## 10. Rollback

- Helper-Join zuruecknehmen (CRM-Zeilen werden nicht mehr gelesen).
- CRM-Schritt aus `base-35/do/main.R` entfernen; CRM-Zeilen einmalig loeschen (`DELETE WHERE source='crm_task'`).
- Migration ist additiv/NULLbar → Bestand unberuehrt.

## 11. Phasen (je pruefbar)

- **PR 1 (dbconnectorR):** Parser + Identitaet + Anti-Join als reine Funktionen + Unit-Tests. Kein DB-Schreibpfad. Isoliert validierbar.
- **PR 2 (dbconnectorR + base-35 + Migration):** Schema-Migration; `update_crm_task_meeting_classification`; Einhaengen in `main.R`. Delta-Messung. **Einziger Moment, in dem sich Zahlen aendern.**
- **PR 3 (shiny-99-modules):** Helper-LEFT-Join + neuer Sub-Tab. Sichtbarmachung.

## 12. Offene Build-Zeit-Verifikationen

- Existiert `mapping.vw_service_users` mit `connected_service='crm'`? (sonst E-Mail-Fallback) — kleine Query vor PR 1.
- Exaktes Spaltenset/Nullbarkeit von `processed.msgraph_extern_event_classification` gegen die geplante Migration gegenpruefen.
- `bit64`-Round-Trip beim Full-Set-Upsert (integer64 → 0-Falle) absichern.
- Reihenfolge/Struktur von `base-35-export_teams_history/do/main.R` bestaetigen (CRM-Schritt strikt nach `update_extern_event_classification`).
- Anti-Join-Match-Fenster (`lead + Tag`) an echten Daten kalibrieren; Trefferquote gegen erwartete externe-Tool-Termine sanity-checken.
