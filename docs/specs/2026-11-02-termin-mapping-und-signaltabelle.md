# Termin-Mapping CRM ↔ MS Graph und Signaltabelle
Asana: https://app.asana.com/0/0/1220115877676176/f · ADR: `docs/adr/0003-persist-task-mapping-and-separate-signals.md`

## Problem

Welcher CRM-VC-Task zu welchem MS-Graph-Termin gehört, wird heute zweimal und unterschiedlich
berechnet und nirgends gespeichert:

- `update_crm_task_meeting_classification()` gleicht nur über Lead × Tag ab.
- `update_sales_meetings_unified()` nutzt zusätzlich Rep, Startzeit und die Ersatzregel.

`processed.sales_meetings_unified` enthält nur das Ergebnis einer Kette von Überschreibungen
(`is_no_show`, `excluded`). Was CRM, Outlook und Teams jeweils über einen Termin sagen, ist aus den
Daten nicht mehr ablesbar. Mehrdeutige Tasks fallen dort ganz weg (`sales_meetings_unified.R:275-276`),
obwohl ADR 0001 sie getrennt erhalten will.

## Lösung

Zwei neue Tabellen, neben den bestehenden, gebaut von zwei neuen Funktionen in dbconnectorR und
aufgerufen in `base-62-msgraph-scoped/do/main.R` direkt nach `update_sales_meetings_unified`.
Beide werden bei jedem Lauf voll neu geschrieben (`delete_missing = TRUE`).

### `mapping.crm_lead_task_msgraph_event`

Eine Zeile je CRM-VC-Task. Menge wie in `update_sales_meetings_unified()`:

- Task nicht gelöscht.
- `is_crm_vc_meeting()` trifft zu.
- Der Rep-Kontakt ist über `resolve_crm_user_contact()` auflösbar, mit `coalesce(assigned_to_user_id, user_id)`.

| Spalte | Typ | Inhalt |
|---|---|---|
| `crm_lead_task_id` | bigint, FK `raw.crm_lead_tasks.id`, unique | der Task |
| `msgraph_event_id` | bigint, FK `raw.msgraph_events.id`, nullable | gematchter Termin, NULL ohne Treffer |
| `match_rule` | text, not null | siehe unten |
| `candidate_count` | integer | Kandidaten der entscheidenden Regel; 0 bei `netto_neu`, NULL bei `externes_tool`/`ohne_lead` |
| `start_diff_minutes` | numeric | `abs(event_start - precise_time)` in Minuten, nur bei Treffer |
| `lead_source` | text | Herkunft des Leads der gematchten Termin-Zeile: `mapping`, `domain` oder `ersatzregel` |

`match_rule`, erste greifende Regel gewinnt; die Logik ist dieselbe wie in `assemble_unified_meetings()`:

| Wert | Regel |
|---|---|
| `externes_tool` | Tool webex, zoom, google_meet oder skype |
| `ohne_lead` | Task ohne `lead_id` |
| `lead_tag` | genau ein Termin mit gleichem Lead und gleichem Tag |
| `lead_tag_rep` | mehrere, genau einer mit gleichem Rep |
| `lead_tag_naechste_zeit` | mehrere mit gleichem Rep, eindeutig nächster Beginn |
| `mehrdeutig` | Rest-Mehrdeutigkeit, kein Treffer |
| `ersatzregel` | kein Kandidat; Termin ohne Lead, gleicher Rep, gleicher Tag, ≤ 15 min (`ersatzregel_paare()`) |
| `netto_neu` | nichts gefunden |

Die Matching-Schleife aus `assemble_unified_meetings()` wird dafür in eine reine Funktion
ausgelagert, die je Task Treffer, Regel und Belege liefert. `assemble_unified_meetings()` ruft sie
auf und verhält sich unverändert; die bestehenden Tests in `test-sales_meetings_unified.R`
sichern das ab.

### `processed.sales_meeting_signals`

Eine Zeile je Termin × Lead wie in `sales_meetings_unified`, also MS-Graph-Termine mit mindestens
einem externen Teilnehmer, expandiert über `meeting_leads_ableiten()`. Dazu kommt je CRM-VC-Task
ohne Treffer eine eigene Zeile, auch bei `mehrdeutig`. Ein Task gehört zu der Termin-Zeile mit
seinem `msgraph_event_id` und seinem Lead; per Ersatzregel übernimmt die Zeile den Lead des Tasks.

| Gruppe | Spalte | Inhalt |
|---|---|---|
| Schlüssel | `meeting_key` | `msgraph_<call_event_mapping_id>_<lead_id>` bzw. `crm_<crm_task_id>`, wie in unified |
| | `msgraph_event_id`, `call_event_mapping_id` | NULL bei CRM-only |
| | `crm_lead_task_id` | der Task, dessen Status gewinnt; NULL ohne Task |
| | `lead_id`, `lead_source` | `lead_source`: `mapping`, `domain`, `ersatzregel`, `crm_task` oder NULL (Termin ohne Lead) |
| | `contact_id` | verantwortlicher Rep; bei CRM-only der Rep des Tasks |
| | `organizer_contact_id` | aus `load_meeting_organizers()` |
| | `match_rule` | der gewinnende Task; NULL ohne Task |
| Zeit | `event_date`, `event_start`, `original_created_at` | wie in unified; bei CRM-only `precise_time` und `task_created_at` |
| Vorhanden | `has_crm_task`, `has_msgraph_event` | |
| CRM | `crm_status` | `show_up`, `no_show`, `storniert`, `ohne_ausgang`, `kein_task` |
| | `crm_status_conflict` | Tasks oder Kommentare widersprechen sich |
| | `crm_task_count`, `crm_comment_count` | Anzahl gemappter Tasks, Anzahl nicht gelöschter Kommentare des gewinnenden Tasks |
| | `meeting_tool`, `meeting_type`, `is_external_tool` | vom gewinnenden Task |
| Outlook | `outlook_status` | `aktiv`, `verschoben`, `abgesagt`, `kein_event` |
| | `msgraph_exclusion_reason` | unverändert aus `msgraph_extern_event_classification` |
| | `is_short_lived_event` | aus `msgraph_extern_event_classification` |
| | `is_booking` | Event hat einen Teilnehmer mit `source = 'booking'` |
| Teams | `call_status` | siehe unten |
| | `online_meeting_lookup` | Rohwert aus `raw.msgraph_events` |

**CRM-Status je Task:** Es zählt der späteste Kommentar mit erkanntem Status (`classify_meeting_status()`
≠ `unbekannt`). „Später“ heißt `comment_created_at`, ersatzweise `created_at`; bei Gleichstand
gewinnt die höhere `crm_comment_id`. Kommentare ohne erkannten Status sind kein Signal. Ohne
erkannten Kommentar steht `ohne_ausgang`.

**CRM-Status über mehrere Tasks:** `show_up` > `no_show` > `storniert` > `ohne_ausgang`. Gewinnt
mehr als ein Task, entscheidet die kleinere `crm_task_id`.

**`crm_status_conflict`:** TRUE, wenn die erkannten Status der Kommentare eines Tasks oder der
Tasks einer Zeile nicht alle gleich sind.

**`outlook_status`:**
- `verschoben`: `is_canceled` und es gibt ein aktives Event mit derselben `msgraph_ical_uid`.
- `abgesagt`: sonst `is_canceled`, einschließlich Events, die aus dem Kalender verschwunden sind.
- `aktiv`: sonst.
- `kein_event`: CRM-only-Zeilen.

**`call_status`**, in dieser Reihenfolge, ermittelt über `compute_observability_exclusions()` mit
`tenant_id` aus der Konfiguration von base-62 und `event_class` aus `mapping.msgraph_call_event`:
1. `in_zukunft`
2. `extern_call` bzw. `intern_call`
3. `alt_tenant`
4. `nicht_abrufbar`
5. `ohne_link` (`join_url_checked_at` gesetzt, `join_url` leer)
6. `no_call`
7. `kein_event` für CRM-only-Zeilen

Bewertungen (`is_no_show`, `excluded`, `exclusion_reason`, `no_show_source`) stehen nicht in der
Tabelle.

## Entscheidungen

- **Q1 Welche Signale gehören in die Mapping-Tabelle:** „q1 passt“, also IDs plus die Belege für den
  Abgleich (`match_rule`, Abstand in Minuten, Anzahl Kandidaten, `lead_source`), kein Terminstatus.
- **Q2 Bekommt auch jeder Task ohne Treffer eine Zeile im Mapping:** „q2 passt“: ja, mit
  `msgraph_event_id = NULL` und dem Grund in `match_rule`.
- **Q3 Was passiert mit mehrdeutigen Tasks:** „q3 passt, lass uns diese fälle später untersuchen“:
  eigene Zeile in der Signaltabelle mit `match_rule = 'mehrdeutig'`; die Untersuchung ist ein
  Folgeticket.
- **Q4 Welcher CRM-Status steht in der Signaltabelle, wenn zwei Tasks auf dasselbe Event gemappt
  sind:** „in dieser mapping tabelle sollen keine signale für den termin status sein. ich möchte die
  tasks mit den msgraph daten mappen“ und „Erstmal sollten wir eigentlich keine unterschiedlichen
  Signale bekommen. Und wenn wir von einem das Signal bekommen, dass der Termin stattgefunden hat, und
  der andere aber sagt, das wurde storniert oder gecancelt, dann zählt das stattgefunden. Wenn wir
  einen Status haben für No-Show, zählt der No-Show. Wichtig, wenn nur eins von beiden irgendwie
  beschrieben wurde, dann nehmen wir den Status, den wir bekommen können. […] Zeigen mir bitte gerne
  die Fälle, wo wir verschiedene Signale haben. […] Wenn wir gar keine Informationen haben, in dem
  einen Termin, in dem anderen Termin steht storniert oder abgesagt, dann haben wir dort ein Signal,
  das Signal zählt.“
- **Q5 Woher kommen die Outlook- und Teams-Signale:** „q5 passt für das nachziehen von a ein extra
  ticket“, also c: übernehmen, was `msgraph_extern_event_classification` und
  `compute_observability_exclusions()` schon berechnen, nur „verschoben“ selbst rechnen; das
  Auslagern (a) ist ein Folgeticket.
- **Q6 Gleiches Matching und gleiche Menge wie `sales_meetings_unified`:** „q6 passt“.
- **Q7 Gehört die Bewertungs-View dazu:** „q7 passt“: nein, Folgeticket; hier nur der Zahlenabgleich.
- **Q8 Gehört das Signal „in Outlook verschwunden“ dazu:** „q8 passt“: nein, Folgeticket; bis dahin
  `abgesagt`.
- **Q9 Bookings ohne Online-Meeting:** „q9 passt“: nur `is_booking` auf den vorhandenen Zeilen, die
  Booking-View bleibt.
- **Q10 Tabellennamen:** „q10 passt“: `mapping.crm_lead_task_msgraph_event` und
  `processed.sales_meeting_signals`.
- **Q11 Wo liegt der Code:** „q11 passt“: DDL und Funktionen in dbconnectorR, Aufruf in
  `base-62/do/main.R`, zwei PRs.
- **Q12 Schreibweise:** „q12 passt“: voller Neuaufbau mit `delete_missing = TRUE`.
- **Q13 Stattgefunden gegen No-Show:** „Wie schon gesagt, ein Fall, der eigentlich nie auftritt.
  Dementsprechend, wenn wir einmal sagen, es hat stattgefunden und jemand anderes sagt, der war nicht
  da, dann vertrauen wir dem, der geschrieben hat, es hat stattgefunden.“
- **Q14 Reihenfolge innerhalb eines Tasks:** „Der spätere Kommentar gewinnt.“
- **Q15 Sichtbarkeit der widersprüchlichen Fälle:** „q15 passt“: Spalte `crm_status_conflict` und
  eine SQL-Query in der Validierung.
- **Q16 Zählt ein späterer Kommentar ohne Status-Stichwort:** „q16 passt“: nein, er ist kein Signal.
- **Q17 Zeitpunkt für „später“:** „q17 passt“: `comment_created_at`, ersatzweise `created_at`, bei
  Gleichstand die höhere `crm_comment_id`.

## Betroffen

- dbconnectorR (dieser PR): `inst/sql/2026-11-02-crm-lead-task-msgraph-event.sql`,
  `inst/sql/2026-11-02-sales-meeting-signals.sql`, neue Funktionen in `R/`, Auslagerung der
  Matching-Schleife aus `R/sales_meetings_unified.R`, Tests, `CONTEXT.md`, ADR 0003.
- base-62-msgraph-scoped (zweiter PR): `do/main.R`, zwei neue Jobs nach `update_sales_meetings_unified`.
- Gelesen: `raw.crm_lead_tasks`, `raw.crm_lead_task_comments`, `raw.crm_users`, `raw.crm_lead_mail_address`,
  `raw.msgraph_events`, `raw.msgraph_event_participants`, `raw.msgraph_contacts`,
  `mapping.msgraph_call_event`, `mapping.crm_lead_msgraph_contact`,
  `processed.msgraph_extern_event_classification`.
- Unverändert: `processed.sales_meetings_unified`, `processed.msgraph_extern_event_classification`
  und alle ihre Leser.

## Out-of-Scope

- Bewertungs-View auf der Signaltabelle (Folgeticket https://app.asana.com/0/0/1220115896589056/f).
- Untersuchung der mehrdeutigen Fälle (https://app.asana.com/0/0/1220116740191794/f).
- Eigenes Signal „in Outlook verschwunden“ (https://app.asana.com/0/0/1220117253528426/f).
- Einzelflags statt eines übernommenen Grundes für die Outlook-Erkennung
  (https://app.asana.com/0/0/1220115896703637/f).
- Bookings ohne Online-Meeting.
- Umzug der Leser von `sales_meetings_unified`.

## Validierung

Nach Deploy und erstem Lauf von base-62:

1. Jeder CRM-VC-Task aus der Menge oben steht genau einmal in `mapping.crm_lead_task_msgraph_event`;
   die Verteilung über `match_rule` ist plausibel.
2. Die Zeilen der Signaltabelle mit `has_msgraph_event` entsprechen in Anzahl und `meeting_key` den
   `source = 'msgraph'`-Zeilen von `sales_meetings_unified`. Die CRM-only-Zeilen entsprechen den
   `source = 'crm_task'`-Zeilen plus den Tasks mit `match_rule = 'mehrdeutig'`.
3. Abweichungen beim CRM-Status gegenüber `meeting_status` in unified gehen allein auf die neue
   Reihenfolge zurück (Q4, Q13, Q14, Q16).
4. Die Query für `crm_status_conflict` listet die widersprüchlichen Termine mit allen Einzelstatus.

Die Queries liegen unter `docs/sql/2026-11-02-validierung-sales-meeting-signals.sql`.
