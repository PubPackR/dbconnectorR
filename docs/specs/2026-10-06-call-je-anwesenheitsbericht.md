# Wiederverwendeter Teams-Link: ein Call je Anwesenheitsbericht
Asana: https://app.asana.com/1/734700742714256/project/1211924185203938/task/1218828738255412

## Problem

base-62 holt die Anwesenheit im neuen Tenant über die Anwesenheitsberichte eines Online-Meetings. `msgraph_scoped_update_calls_attendance()` speichert pro Online-Meeting genau eine Zeile in `raw.msgraph_calls` (`distinct(msgraph_call_id)`, `msgraph_call_id` = Online-Meeting-ID) mit dem Start des ersten Berichts. Ein Teams-Link, der für mehrere Termine benutzt wird (Serie, persönlicher Link), hat aber einen Bericht je Session. Nur die erste Session bekommt einen Call, alle weiteren werden in `msgraph_map_calls_events()` zu `no_call` und damit zum No-Show. Gemessen am 24.09.2026: 52 No-Shows ab dem 19.08.2026 mit Call zum selben Link an einem anderen Tag.

## Lösung

- `msgraph_scoped_update_calls_attendance()` schreibt eine Call-Zeile je Anwesenheitsbericht: `msgraph_call_id` = Bericht-ID, `call_start`/`call_end` aus dem Bericht, neue Spalte `msgraph_online_meeting_id` = Online-Meeting-ID, `meeting_id` wie bisher die Thread-ID aus der joinUrl. Die Teilnehmer hängen am Call ihres Berichts.
- Bestehende Zeilen mit `msgraph_call_id` = Online-Meeting-ID schlüsselt der Job vor dem Upsert um: sie bekommen die Bericht-ID der Session, deren Start ihrem `call_start` am nächsten liegt (im Normalfall gleich), und `msgraph_online_meeting_id`. Ihre Teilnehmer werden dabei geleert und vom Upsert mit den Teilnehmern dieser Session neu geschrieben, denn bisher standen dort die Teilnehmer aller Sessions. Die `id` bleibt gleich. Nach dem ersten Lauf ist der Schritt ein No-op.
- `msgraph_scoped_update_transcripts()` adressiert Graph über `coalesce(msgraph_online_meeting_id, msgraph_call_id)`, fragt jedes Online-Meeting einmal ab und hängt ein neues Transkript an die Session, in deren Zeitraum `createdDateTime` fällt. Liegt es in keinem, gewinnt die letzte Session, die vorher begonnen hat, sonst die früheste.
- `msgraph_map_calls_events()` bleibt unverändert, es paart schon über `(meeting_id, contact_id, Datum)`.
- base-62 bekommt ein One-off, das nur den Calls-Job mit einem Fenster ab dem 19.08.2026 laufen lässt. Mapping, Klassifikation und `processed.sales_meetings_unified` rechnet der nächste reguläre Lauf von `do/main.R` neu.

## Entscheidungen

- Q1, Schlüssel: `msgraph_call_id` = ID des Anwesenheitsberichts, neue nullable Spalte `msgraph_online_meeting_id` als Graph-Griff für die Transkripte. UNIQUE und `match_cols` auf `msgraph_call_id` bleiben. ADR: `docs/adr/0002-call-row-per-attendance-report.md`.
- Q2, Bestand: umschlüsseln statt löschen, im Job, idempotent. Transkripte, Teilnehmer und Mapping zeigen weiter auf dieselbe `id`.
- Q3, Neuklassifizierung: One-off in base-62 mit überschriebenem Fenster ab 19.08.2026, `config.yaml` bleibt. Nötig, weil `events_days_back: 50` den 19.08. ab dem 08.10. nicht mehr abdeckt.
- Q4, Transkripte: neue Transkripte gehen an die passende Session, bestehende bleiben, wo sie sind.
- Q5, Begriff: **Session** im `CONTEXT.md`.

## Betroffen

| Was | Wo |
|---|---|
| Ingest Calls | `dbconnectorR/R/msgraph_scoped_calls.R` |
| Ingest Transkripte | `dbconnectorR/R/msgraph_scoped_transcripts.R` |
| DDL | `dbconnectorR/inst/sql/2026-10-06-msgraph-calls-online-meeting-id.sql` |
| Tabellen | `raw.msgraph_calls` (neue Spalte), `raw.msgraph_call_participants`, `processed.msgraph_call_transcripts` |
| Nachgelagert, ohne Codeänderung | `mapping.msgraph_call_event`, `processed.msgraph_extern_event_classification`, `processed.sales_meetings_unified` |
| One-off | `base-62-msgraph-scoped/one-off/2026-10-06_backfill_attendance_sessions.R` |

Reihenfolge: DDL in DBeaver, dann Package-Installation auf dem Server (Festangestellte), dann base-62 deployen, dann One-off auf dem Server. Ohne die Spalte bricht der Upsert nicht ab, sondern verwirft sie still, deshalb prüft der Job ihr Vorhandensein selbst.

## Out-of-Scope

- Transkripte späterer Sessions, die heute schon am Call der ersten Session hängen (teils bereits ins CRM exportiert).
- Calls aus base-35 (callRecords, Alt-Tenant): ihr `msgraph_call_id` bleibt die callRecord-ID.

## Validierung

- Nach DDL: `information_schema.columns` zeigt `raw.msgraph_calls.msgraph_online_meeting_id`.
- Nach dem One-off: kein Call mehr mit `msgraph_online_meeting_id IS NULL` und `call_start >= '2026-08-19'` aus dem neuen Tenant; Online-Meetings mit mehreren Termintagen haben mehrere Calls.
- Nach dem nächsten regulären Lauf: die Query aus dem Ticket (No-Show mit Call zum selben Link an anderem Tag) liefert nur noch Termine, an deren Tag wirklich kein Bericht existiert.
