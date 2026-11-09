# Signal „in Outlook verschwunden“ getrennt von Absagen speichern
Ticket: https://app.asana.com/1/734700742714256/project/1211924185203938/task/1220117253528426

## Problem

Seit #57 setzt der Ingest Events, die aus einem geladenen Kalender verschwunden sind, auf
`is_canceled = TRUE`. Sie sehen danach aus wie echte Absagen. Zugleich fehlt jeder Zeitpunkt: Der
Verschwunden-Pfad ändert `event_updated_at` nicht, und `updated_at` wird auch von der
Online-Meeting-Suche bewegt. Für `processed.sales_meetings` braucht es je Event einen Zustand,
einen Änderungszeitpunkt und einen Nachfolger.

## Lösung

- `raw.msgraph_events.vanished_at` (`timestamp`, UTC). `mark_vanished_events_canceled()` setzt die
  Spalte zusammen mit `is_canceled`. Der Kalender- und der Bookings-Upsert leeren sie, wenn die
  Zeile wieder im Download steht.
- `derive_calendar_state()` in `R/msgraph_calendar_state.R` ist eine reine Funktion. Sie bekommt
  die Events und die Ersatz-Paare aus Schritt 7 als Argument und liefert je Event Zustand,
  Änderungszeitpunkt und Nachfolger. Noch kein Job ruft sie auf.
- Ein Backfill-Skript für DBeaver trägt einen Näherungswert für alte Kalenderzeilen nach.

| Zeile | Bedingung | Zustand | Nachfolger |
|---|---|---|---|
| aktiv | Einzeltermin, und eine Zeile derselben `ical_uid` hat ein späteres `event_updated_at` | `stale_copy` | aktive Zeile mit spätestem `event_updated_at`, nie die eigene |
| aktiv | sonst | `active` | keiner |
| abgesagt | aktive Zeile derselben `ical_uid` oder Ersatz-Paar vorhanden | `rescheduled` | diese Zeile, das Ersatz-Paar nur als Rückfall |
| abgesagt | kein Nachfolger, `vanished_at` gesetzt | `vanished` | keiner |
| abgesagt | kein Nachfolger, `vanished_at` leer | `cancelled` | keiner |

Der Änderungszeitpunkt ist `coalesce(vanished_at, event_updated_at)`, bei Bookings ohne
`vanished_at` leer.

Reihenfolge beim Livegang: DDL, Backfill, dann die neue `dbconnectorR`-Version auf dem Server,
Backfill und Installation am selben Tag ohne Nachtlauf dazwischen. Das UPDATE im Ingest schlägt
ohne die Spalte fehl.

## Entscheidungen

- **Q1 Nachfolger aus Schritt 7:** „passt“. Die Erkennung bekommt die Paare als Argument, die
  Klassifikation bleibt in diesem PR unberührt.
- **Q2 Mehrere aktive Kandidaten:** „Beide passen“. Nachfolger ist die Zeile mit dem spätesten
  `event_updated_at`, bei Gleichstand dem späteren `event_start`. Schritt 7 zählt nur, wenn es
  keine aktive Zeile derselben `ical_uid` gibt.
- **Q3 „Erster Nachfolger“ im Backfill:** „Beide passen“. Es zählt die früheste Zeile derselben
  `ical_uid`, die nach der eigenen Zeile angelegt wurde, egal ob sie heute aktiv oder abgesagt ist.
  Findet sich keine, gilt `updated_at`. Schritt 7 entfällt, weil es ihn nur in R gibt.
- **Q4 Bookings im Backfill:** „passt“. Nur Kalenderzeilen. Alte Bookings bleiben `cancelled`, neue
  werden ab Rollout `vanished`.
- **Q5 Änderungszeitpunkt bei aktiven Zeilen:** „passt“. Für alle Zeilen
  `coalesce(vanished_at, event_updated_at)`.
- **Q6 Zustandswerte:** „passt“. Englisch: `active`, `rescheduled`, `cancelled`, `vanished`,
  `stale_copy`.
- **Q7 Spalte und DDL:** „passt“. `timestamp` in UTC, gesetzt mit `timezone('UTC', now())`, die DDL
  ergänzt auch `raw_scoped_test.msgraph_events`.
- **Q8 Ablage:** „passt“. `R/msgraph_calendar_state.R`, DDL und Backfill als zwei Dateien unter
  `inst/sql/`.
- **Q9 Änderungszeitpunkt bei Bookings:** „Beide passen“. Bei Bookings ohne `vanished_at` bleibt der
  Änderungszeitpunkt leer, weil `event_updated_at` dort der Terminstart ist. Schränkt Q5 für
  Bookings ein.
- **Q10 `Abgesagt:` mitten im Betreff:** „Beide passen“. Der Backfill schließt jeden Betreff aus, in
  dem `Canceled:`, `canceled:` oder `Abgesagt:` vorkommt.

## Betroffen

- `raw.msgraph_events`, `raw_scoped_test.msgraph_events`: neue Spalte `vanished_at`.
- `R/msgraph_scoped_cancellation.R`, `R/msgraph_scoped_events.R`, `R/msgraph_scoped_bookings.R`.
- `R/msgraph_calendar_state.R` (neu).
- `inst/sql/2026-11-09-msgraph-events-vanished-at.sql`,
  `inst/sql/2026-11-09-msgraph-events-vanished-at-backfill.sql`.

## Out-of-Scope

- Die Spalten `calendar_status`, `calendar_changed_at_utc` und `successor_meeting_id`: Ticket zu
  `processed.sales_meetings`.
- Das Auslagern von Schritt 7: Ticket zur Outlook-Erkennung.
- `derive_outlook_status()` und `processed.sales_meeting_signals` bleiben unverändert.
- Der alte Abruf `update_events()`: `MSGraph_Retrieve_Events_Calls` hängt in keiner Job-Kette mehr.

## Validierung

- `testthat`: Zustände, Nachfolger, Änderungszeitpunkt, Verschwinden, Wiederauftauchen und erneutes
  Verschwinden.
- Der Backfill zählt vorab lesend und nennt auffällige Betreff-Präfixe. Die Zahl der geänderten
  Zeilen steht danach im Ticket.
- Der Backfill zieht über `trigger_set_updated_at` das `updated_at` der betroffenen Zeilen mit. Er
  fasst deshalb nur Zeilen mit leerem `vanished_at` an und läuft nur einmal wirksam.
- Nach dem ersten Serverlauf: `vanished_at` ist bei frisch verschwundenen Zeilen gesetzt und bei
  wieder aufgetauchten leer. Die Tests laufen ohne echte Datenbank, den Beleg liefert erst dieser
  Lauf.
