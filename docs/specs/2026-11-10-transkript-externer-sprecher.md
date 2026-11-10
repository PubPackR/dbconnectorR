# Transkript mit externem Sprecher macht einen Termin zum externen Call
Ticket: https://app.asana.com/1/734700742714256/project/1211924185203938/task/1220382115257832

## Problem

Teams führt manche externen Teilnehmer nicht im Anwesenheitsbericht, obwohl sie im Transkript
sprechen. `msgraph_map_calls_events()` stuft solche Calls als `intern_call` ein, die Klassifikation
zählt sie als No-Show (`R/msgraph_extern_event_classification.R`, `is_no_show`). Seit 01.09.2026
betrifft das 28 Termine, alle als No-Show gezählt (Analyse zu Anfrage #0157, Lead 17095,
Session 789903). Wiederkehrende Kunden fehlen jedes Mal im Bericht. Warum Microsoft sie weglässt
(Opt-out, Richtlinie), ist offen; die Analyse hat entschieden, das nicht weiter zu klären und das
Transkript als Beleg zu nehmen.

## Lösung

Neue Datei `R/transcript_speakers.R`:

| Funktion | Export | Ergebnis |
|---|---|---|
| `transcript_speakers(transcript_content)` | ja | je Transkript und Sprecher-Label die Zahl der Zeilen |
| `is_internal_speaker(label)` | ja | `TRUE`, wenn das Label mit „, Studyflix GmbH“ endet |
| `count_external_speakers(transcript_content, min_lines = 2L)` | ja | je Transkript die Zahl der externen Sprecher mit mindestens `min_lines` Zeilen |

`count_external_speakers()` ist die ganze Regel in einer Funktion, damit `processed.sales_meetings`
dieselbe Zählung als Signal führen kann.

`msgraph_map_calls_events()`: Für Calls, deren Gruppe (`meeting_id`, Tag) nur interne Teilnehmer
im Bericht hat, werden die Transkripte geladen. Hat ein Call der Gruppe einen externen Sprecher,
wird `event_class_call` zu `extern`. Die Einstufung steht in der reinen Funktion
`derive_event_class_call()`. Geladen werden nur Transkripte dieser Kandidaten, erst die IDs, dann
der Inhalt.

Es gibt keine neue Spalte und keine DDL. Klassifikation, `call_status` der Signale,
`processed.sales_meetings_unified`, `shiny.sales_kpi_termine_ereignisse` und crm-tagging folgen
über `event_class`, rückwirkend über den nächtlichen Komplettlauf (`delete_missing = TRUE`).

## Entscheidungen

Runde 1 und 2 am 10.11.2026, beantwortet mit „passt alles so“.

- **Q1 Woran ein Sprecher als intern gilt:** am Kürzel „, Studyflix GmbH“ im Label. `@1` und jedes
  andere Label ohne Kürzel gelten als extern. Grundlage Query K: alle 34 internen Labels tragen das
  Kürzel, kein Label ohne Kürzel passt zu einem internen Konto oder Kontakt.
- **Q2 Wo die Regel sitzt:** in `event_class_call` von `msgraph_map_calls_events()`.
- **Q3 Kennzeichnung der Herkunft:** keine Kennzeichnung in der bisherigen Kette. Das
  Transkript-Signal kommt in `processed.sales_meetings` (eigenes Ticket).
- **Q4 CRM-Automatik:** zieht mit. „Letzter Kontakt“ und Tags ändern sich rückwirkend für die
  betroffenen Leads.
- **Q5 Wiederverwendung:** Die Erkennung entsteht als exportierte Funktion in dbconnectorR.
- **Q6 Namen:** `transcript_speakers()` und `is_internal_speaker()`, Begriff *External speaker* in
  `CONTEXT.md`.
- **Q7 Mindestzahl Zeilen:** Ein externer Sprecher zählt ab zwei Zeilen.

## Betroffen

- `R/transcript_speakers.R` (neu), `R/msgraph_map_call_event.R`, `NAMESPACE`, `man/`,
  `DESCRIPTION` (0.0.0.9042), `CONTEXT.md`.
- `tests/testthat/test-transcript_speakers.R` (neu), `tests/testthat/test-msgraph_map_calls_events.R`
  (neu).
- Liest zusätzlich `processed.msgraph_call_transcripts`, schreibt weiter nur
  `mapping.msgraph_call_event`, ohne Schemaänderung.
- base-62 bleibt unverändert: `processed.msgraph_call_transcripts` steht schon in `NEEDED`, der
  Transkript-Job läuft vor `msgraph_map_calls_events()`.

## Out-of-Scope

- Die Ursache am rohen Anwesenheitsbericht klären, die Teams-Richtlinie im Tenant ändern.
- Eine Doku-Regel für CRM-Tasks.
- Eine Kennzeichnung, dass ein externer Call aus dem Transkript kommt (Q3).
- Das Signal in `processed.sales_meetings` (eigenes Ticket, nutzt `count_external_speakers()`).

## Validierung

- `devtools::test()` grün, darunter die Fälle: externer Sprecher mit zwei Zeilen macht
  `extern_call`, ein Label ohne Kürzel mit nur einer Zeile und ein Transkript nur mit internen
  Sprechern ändern nichts.
- Nach Paket-Install und dem nächsten base-62-Lauf, Query aus dem Ticket-Body: die 28 Fälle tragen
  `is_no_show = false`, Session 789903 steht als `extern_planned_extern_call` in
  `mapping.msgraph_call_event`, Session 756559 bleibt `extern_planned_intern_call`.
- Der Anteil `intern_call` (Messquery vom 28.09.2026 im Vorgänger-Ticket) sinkt für September bis
  November.
