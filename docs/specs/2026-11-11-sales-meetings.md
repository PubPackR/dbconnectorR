# processed.sales_meetings und sales_meeting_leads: eine Zeile je Meeting record
Ticket: https://app.asana.com/1/734700742714256/project/1211924185203938/task/1220345592707180

## Problem

`processed.sales_meeting_signals` und `processed.sales_meetings_unified` haben eine Zeile je Termin und Lead. Ein einfaches `count(*)` liegt dort um 2,7 % zu hoch (gemessen am 09.11.2026: 957 von 39.963 Terminen stehen mehrfach). Beide Tabellen starten an `processed.msgraph_extern_event_classification` und sehen deshalb nur eine gefilterte Menge: Events ohne `meeting_id`, abgesagte Events ohne internen Call-Teilnehmer und alles außer `extern_planned` fehlen. RevOps braucht eine Tabelle mit einer Zeile je Meeting record, die direkt an `raw.msgraph_events` startet, nur Signale führt und keine Bewertung (ADR 0003), dazu eine zweite mit einer Zeile je Meeting record und Lead.

## Lösung

Ein Producer `update_sales_meetings(con, person_roles, tenant_id, now_utc)` in `R/sales_meetings.R` baut beide Tabellen jede Nacht vollständig neu. Die Logik steht in reinen Funktionen ohne Datenbankzugriff, der Producer lädt nur und schreibt. Er hängt nicht an der Klassifikation, sondern nutzt die Hilfsfunktionen aus `R/msgraph_event_detection.R` und `R/msgraph_calendar_state.R`.

Ablauf:

1. **Startmenge.** Jede Zeile von `raw.msgraph_events`, die laut `count_event_participants()` mindestens einen externen Teilnehmer hat (weder intern noch synthetisch), auch abgesagte, verschwundene, `booking:`-Einträge und Events ohne `meeting_id`. Dazu je CRM-VC-Task (`crm_task_id`), der keiner Kalenderzeile zugeordnet ist, eine CRM-Zeile. Schlüssel `msgraph_<event_id>` bzw. `crm_<crm_task_id>`.
2. **Kalenderzustand.** `derive_calendar_state()` läuft zweimal. Der erste Lauf ohne Ersatz-Paare liefert, welche Zeilen aktiv, veraltete Kopie, abgesagt oder verschwunden sind. Daraus bekommt `detect_replaced_events()` die Zeilen nach Q2, und der zweite Lauf mit diesen Paaren liefert `calendar_status`, `calendar_changed_at_utc` und den Nachfolger.
3. **Legedatum.** Wie heute `original_created_at` (Q4): `compute_original_created_at()` über alle Events, `inherit_moved_copy_created_at()` über alle Events und Teilnehmer, danach erbt ein Ersatztermin das frühere Datum seines abgesagten Vorgängers. `booking:`-Einträge bekommen den Berliner Tag ihres Beginns (Q10), CRM-Zeilen den Berliner Tag von `task_created_at`.
4. **Teams.** Je Event alle Zeilen von `mapping.msgraph_call_event`. Eine Sitzung mit `extern_call` macht den Termin zu `external_joined`, sonst eine mit `intern_call` zu `internal_only`. Ein Event ohne Mapping-Zeile hat keinen Call. Die Rangfolge mit Zukunft, Alt-Tenant, nicht abrufbar und ohne Link kommt unverändert aus `derive_call_status()` und wird ins Englische übersetzt (Q9). Beginn und Dauer kommen vom längsten Call, `external_speaker_count` ist das Maximum von `count_external_speakers()` über alle Transkripte der Calls des Events (Q16).
5. **Personen.** Die verantwortliche Person bestimmt die neue reine Funktion `derive_responsible_contact()` in `R/msgraph_event_detection.R`, die auch die Klassifikation aufruft (Q5). Je Event zählt dabei der Call, den die Klassifikation wählt: zuerst einer mit externem Teilnehmer, dann der längste. Der Aufrufer übergibt `person_roles`, eine Funktion, die Kontakte in Personen übersetzt (Q8). Eine Person-ID, die es in `raw.personio_persons` nicht mehr gibt, wird leer geschrieben, damit der Fremdschlüssel den Lauf nicht abbricht.
6. **Duplikate.** `detect_duplicate_events()` sieht nur Zeilen nach Q2.
7. **CRM-Zuordnung.** `match_crm_tasks()` auf der Startmenge (Q1). Kandidaten sind die Kalenderzeilen je Lead, ohne `booking:`-Einträge, Kalenderkopien und veraltete Kopien. Ein Task über die Ersatzregel gibt einer Zeile ohne Lead seinen Lead.
8. **Leads.** Je Kalenderzeile jeder Lead aus `meeting_leads_ableiten()` (Mapping, sonst eindeutige Domain), dazu der Lead aus der Ersatzregel; je CRM-Zeile die Leads ihrer Task-Zeilen. Je Zeile und Lead zählt der Task mit dem stärksten Ausgang nach `CRM_STATUS_RANG`, bei Gleichstand die kleinere `crm_task_id`.
9. **Schreiben**, in einer Transaktion: Upsert von `sales_meetings` über `meeting_key` mit `delete_missing`, danach Nachfolger und Ketten-ID per UPDATE über `meeting_key`, weil die IDs erst die Datenbank vergibt, zuletzt Upsert von `sales_meeting_leads` über `(meeting_id, lead_id)` mit `delete_missing`.

Spalten von `processed.sales_meetings`: `meeting_key`, `msgraph_event_id`, `meeting_date`, `starts_at_utc`, `booked_date`, `booked_date_source` (`outlook`, `bookings_start`, `crm_task`), `organizer_person_id`, `organizer_type`, `responsible_person_id`, `calendar_status` (`active`, `rescheduled`, `cancelled`, `vanished`, `stale_copy`, `not_in_calendar`), `calendar_changed_at_utc`, `teams_status` (`future`, `external_joined`, `internal_only`, `old_tenant`, `not_retrievable`, `no_link`, `no_call`, `not_in_calendar`), `booking_role` (`booking_entry`, `calendar_copy` oder leer), `is_short_lived`, `is_duplicate`, `internal_participant_count`, `external_participant_count`, `external_speaker_count`, `call_started_at_utc`, `call_duration_minutes`, `successor_meeting_id`, `meeting_chain_id`.

Spalten von `processed.sales_meeting_leads`: `meeting_id`, `lead_id`, `lead_match_source` (`mapping`, `domain`, `substitute_rule`, `crm_task`), `crm_lead_task_id`, `crm_outcome` (`held`, `no_show`, `cancelled`, `no_outcome`, `no_task`), `crm_outcome_conflict`, `meeting_tool`, `meeting_type`, `is_external_tool`.

`meeting_chain_id` ist die `id` der ersten Zeile der Kette. Eine Kette sind alle Zeilen, die über ihre Nachfolger bei derselben letzten Zeile ankommen. Hat eine Kette mehrere Zeilen ohne Vorgänger, ist die erste die mit der kleinsten `msgraph_event_id` (Q19).

`is_duplicate` ist TRUE für die Zeilen, die `detect_duplicate_events()` als Verlierer liefert, FALSE für jede andere Kalenderzeile, auch die nicht geprüften, und NULL nur für CRM-Zeilen (Q20).

Der Aufruf kommt in einem zweiten PR nach `studyflix-apps` `base-apps/msgraph-ingestion/do/main.R`, als eigener Job nach `update_sales_meeting_signals`. Er lädt `kpiR::msgraph_contact_to_person(con)` und `kpiR::load_sales_person_history(con)`. Im selben PR hebt er `images/r/kpir.ref` auf einen Commit ab kpiR #74, weil `meeting_person_roles()` erst dort exportiert ist (README von studyflix-apps: der Pin wandert mit dem Skript, das ihn braucht).

Reihenfolge beim Livegang: DDL `inst/sql/2026-11-11-sales-meetings.sql` in DBeaver, PR 1 mergen, die neue dbconnectorR-Version ins Jobs-Image bringen (der übliche Weg für Pakete), PR 2 mergen, „Deploy folder“ für `msgraph-ingestion`.

## Entscheidungen

Die Empfehlungen zu Q1 bis Q16 hat der Nutzer am 10.11.2026 mit „passt alles“ bzw. „passt“ bestätigt.

- **Q1 CRM-Tasks den Terminen zuordnen:** passt. b) Der Producer ordnet selbst zu, mit `match_crm_tasks()` auf der Startmenge. Kandidat ist jede Zeile außer `booking:`-Einträgen, Kalenderkopien und veralteten Kopien. `mapping.crm_lead_task_msgraph_event` bleibt unverändert.
- **Q2 Welche Zeilen die Duplikat- und die Ersatz-Erkennung sehen:** passt. `detect_duplicate_events()` sieht nur Zeilen mit `calendar_status = 'active'`, ohne `booking:`-Einträge und Kalenderkopien, ohne Schwelle für Interne; als No-Show-Klasse gilt `teams_status` ungleich `external_joined`. `detect_replaced_events()` vergleicht abgesagte und verschwundene Zeilen ohne aktive Zeile derselben `ical_uid` mit allen aktiven Zeilen außer veralteten Kopien. Bookings-Zeilen fehlen auf beiden Seiten.
- **Q3 `booking_role` ohne `own_invite`:** passt. Nur `booking_entry`, `calendar_copy` oder leer. Die eigene Einladung findet man über `sales_meeting_bookings.meeting_id` aus dem Ticket SDR-Signale (1220345979589881).
- **Q4 Wie `booked_date` entsteht:** passt. a) Alle drei Schritte wie heute: Ingest-Schranke je `ical_uid`, Erbe der Umzugskopie, Erbe des Ersatztermins.
- **Q5 Wer für einen Termin verantwortlich ist:** passt. Schritt 8 der Klassifikation wird eine reine Funktion in `R/msgraph_event_detection.R`, die Klassifikation ruft sie auf.
- **Q6 `mailbox_copy_status`:** passt. Kommt im Folgeticket 1220345738207738, nicht hier.
- **Q7 `sales_meeting_signals` und das Task-Mapping:** passt. Beide laufen weiter, auch `derive_outlook_status()` bleibt. Abgeschaltet wird im Ticket „Welle 1 abschalten“ (1220345979391112).
- **Q8 Personen ohne Paketabhängigkeit:** passt. Der Aufrufer lädt `kpiR::msgraph_contact_to_person(con)` und `kpiR::load_sales_person_history(con)` und übergibt `function(rows) kpiR::meeting_person_roles(rows, personen, sales)` an `dbconnectorR::update_sales_meetings()`.
- **Q9 `teams_status` und Call-Spalten:** passt. Rangfolge wie `derive_call_status()` auf Englisch: `future`, `external_joined`, `internal_only`, `old_tenant`, `not_retrievable`, `no_link`, `no_call`; CRM-Zeilen `not_in_calendar`. Ohne Eintrag in `mapping.msgraph_call_event` kein Call. `external_joined`, sobald eine Sitzung extern war. Beginn und Dauer vom längsten Call.
- **Q10 `booking:`-Einträge:** passt. `booked_date` ist der Berliner Tag des Beginns, `booked_date_source = 'bookings_start'`, `is_short_lived` bleibt leer.
- **Q11 `meeting_chain_id`:** passt. Die `sales_meetings.id` der ersten Zeile der Kette, in jeder Zeile gefüllt; ein einzelner Termin trägt seine eigene `id`.
- **Q12 Fremdschlüssel:** passt. `ON DELETE CASCADE` nach Hausstandard, nur `successor_meeting_id` mit `ON DELETE SET NULL`.
- **Q13 Ablage:** passt. dbconnectorR `R/sales_meetings.R`, DDL unter `inst/sql/`, Spec unter `docs/specs/`, Abgleich-SQL unter `docs/sql/`. Nach dem Umzug (Kommentar am Ticket vom 10.11.2026) kommt der Aufruf nach `studyflix-apps` `base-apps/msgraph-ingestion/do/main.R` statt nach base-62.
- **Q14 Neuer Begriff in `CONTEXT.md`:** passt. **Meeting record**, Text wie vorgeschlagen.
- **Q15 Aufruf erst nach dem Import:** passt. Bleibt in diesem Ticket: zuerst der PR in dbconnectorR, danach ein zweiter in studyflix-apps. Das Kriterium heißt „läuft im nächtlichen Lauf von msgraph-ingestion“.
- **Q16 Transkript-Signal in `sales_meetings`:** passt. Spalte `external_speaker_count` (integer): das Maximum von `count_external_speakers()` über alle Sitzungen des Termins, NULL ohne Transkript, 0 bei einem Transkript ohne externen Sprecher.
- **Q17 Wie die neue dbconnectorR-Version ins Image kommt:** vom Nutzer am 11.11.2026 aus dem Ticket genommen. Das ist der übliche Deploy-Weg für Pakete und gehört nicht zu diesem Ticket.
- **Q19 `meeting_chain_id` bei mehreren Anfängen:** passt (11.11.2026). Erste Zeile ist die zuerst gespeicherte, also die kleinste `msgraph_event_id`, nicht der früheste Beginn.
- **Q20 `is_duplicate` für nicht geprüfte Zeilen:** passt (11.11.2026). FALSE für alle Kalenderzeilen außerhalb der Prüfung, NULL nur für CRM-Zeilen.

## Betroffen

- dbconnectorR: `R/sales_meetings.R` (neu), `R/msgraph_event_detection.R` (`derive_responsible_contact()`), `R/msgraph_extern_event_classification.R` (ruft sie auf), `R/transcript_speakers.R` (Lader für die Sprecherzahl je Call), `inst/sql/2026-11-11-sales-meetings.sql`, `docs/sql/2026-11-11-abgleich-sales-meetings.sql`, `CONTEXT.md`, Tests.
- studyflix-apps: `base-apps/msgraph-ingestion/do/main.R`, `images/r/kpir.ref`.
- Neue Tabellen `processed.sales_meetings`, `processed.sales_meeting_leads`. Gelesen werden `raw.msgraph_events`, `raw.msgraph_event_participants`, `raw.msgraph_contacts`, `raw.msgraph_calls`, `raw.msgraph_call_participants`, `mapping.msgraph_call_event`, `processed.msgraph_call_transcripts`, `mapping.crm_lead_msgraph_contact`, `raw.crm_lead_mail_address`, `raw.crm_lead_tasks`, `raw.crm_lead_task_comments`, `raw.personio_persons`.

## Out-of-Scope

- Bewertungen (No-Show, Ausschluss, Terminierung): die baut RevOps in der Referenz-SQL (Ticket 1220115896589056).
- `own_invite` und `sales_meeting_bookings` (Ticket SDR-Signale 1220345979589881).
- `mailbox_copy_status` (Ticket 1220345738207738).
- Abschalten von `sales_meeting_signals`, `sales_meetings_unified` und dem Task-Mapping (Ticket 1220345979391112).
- Wie dbconnectorR ins Jobs-Image kommt (Q17).

## Validierung

Nach dem ersten Nachtlauf mit dem neuen Job, alle Abfragen in `docs/sql/2026-11-11-abgleich-sales-meetings.sql`:

- `meeting_key` ist eindeutig, je Kalenderzeile mit externem Teilnehmer steht genau eine Zeile in `sales_meetings`.
- `(meeting_id, lead_id)` ist eindeutig in `sales_meeting_leads`.
- Abgleich gegen `sales_meeting_signals` über die Event-ID: jede Event-ID dort steht auch in `sales_meetings`, und die Abweichungen bei Ausgang, Teams-Status und Legedatum sind nach Grund gezählt.
