# MSGraph Booking Appointments Ingest — Design

**Status:** Draft
**Date:** 2026-05-19
**Author:** Moritz Hemmann (mit Claude)

## Problem

Für interne User, deren Kundentermine über Microsoft Bookings-Pages laufen (z.B. die `StudyflixBeratungsgesprch`-Mailbox), liefert `/users/{id}/calendar/calendarView` Events mit nur einem internen Attendee — die Kunden (= externe Teilnehmer) stehen nicht in `attendees`, sondern in `customers` des Booking-Appointment-Objekts. Dadurch fehlen externe Teilnehmer in `raw.msgraph_event_participants` für diese Termine, und das Call-Event-Mapping in `mapping.msgraph_call_event` läuft mit unvollständigen Daten.

Die App hat seit Kurzem die Permission `BookingsAppointment.ReadWrite.All` und kann auf `/solutions/bookingBusinesses/.../appointments` zugreifen.

## Ziel

Booking-Appointments werden so behandelt, als wären sie reguläre Calendar-Events:

- Sie landen in `raw.msgraph_events` (gleiche Tabelle, gleiches Schema).
- Customers werden als `raw.msgraph_event_participants` mit ihren Kontakten in `raw.msgraph_contacts` geschrieben.
- Das bestehende Call-Event-Mapping (`mapping.msgraph_call_event`) verknüpft sie automatisch mit Telefonaten über die Teams-`meeting_id`.

## Nicht-Ziel

- Keine Schema-Änderungen in `raw.msgraph_events`, `raw.msgraph_event_participants`, `raw.msgraph_contacts` oder `raw.msgraph_users`.
- Kein neuer Flag in `raw.msgraph_users` für die Booking-Business-Mailboxen — sie bleiben in der Tabelle.
- Keine Änderungen an `msgraph_map_calls_events()`.

## Architektur

Eine neue Funktion **`msgraph_update_booking_appointments()`** in [R/msgraph_events.R](../../../R/msgraph_events.R) (gleiches File wie die bestehende Event-Pipeline). Sie wird im FlowForce-Cron nach `msgraph_update_events()` und vor `msgraph_update_calls()` aufgerufen.

```
do/main_msgraph_*.R
  ├── msgraph_update_users()                  (unverändert)
  ├── msgraph_update_events()                 (unverändert)
  ├── msgraph_update_booking_appointments()   ← NEU
  ├── msgraph_update_calls()                  (unverändert)
  └── msgraph_map_calls_events()              (unverändert)
```

Die Funktion ruft am Ende dieselben Subfunktionen wie `msgraph_update_events()` auf:

- `update_events(con, ...)` — schreibt/aktualisiert Event-Rows
- `update_contacts_from_events(con, ...)` — schreibt Customers als Kontakte (inkl. `#EXT#`-Cleanup)
- `update_event_participants(con, ...)` — schreibt Customer + Staff als Participants

Dadurch erbt die Booking-Pipeline automatisch alle bestehenden Eigenheiten (Upsert-Match-Cols, Email-Cleanup, Cancel-Logik).

## Datenfluss

```
msgraph_update_booking_appointments(con, access_token, startDate)
│
├── 1. GET /solutions/bookingBusinesses                  → biz-Liste
│
├── 2. Pro Business: GET /staffMembers                   → (biz_id, staff_id) → email
│
├── 3. Email-Lookup gegen raw.msgraph_users              → (biz_id, staff_id) → msgraph_user_id
│
├── 4. Pro Business: GET /appointments mit Paging        → roh-Appointments
│      (Server-Filter via $filter=start/dateTime ge '<startDate>')
│
├── 5. Appointments-Dataframe bauen
│      pro Appointment:
│        - meeting_id        = extract_meeting_id(joinWebUrl/onlineMeetingUrl)
│        - msgraph_ical_uid  = "booking:" + appointment$id   (Präfix verhindert Kollision)
│        - event_start/end   = startDateTime/endDateTime
│        - user_id           = msgraph_user_id des PRIMÄREN Staffs (staffMemberIds[1])
│        - is_canceled       = aus Status-Feld (genaues Feld bei Implementierung verifizieren)
│        - subject           = serviceName (nur als Fallback bei "Event existiert noch nicht")
│
├── 6. Match gegen bestehende raw.msgraph_events via meeting_id
│      a) Match gefunden  → bestehenden msgraph_ical_uid + event_start übernehmen,
│                           subject NICHT überschreiben (Original-Event-Subject bleibt)
│      b) Kein Match      → neue Event-Row mit serviceName als Subject
│
├── 7. Participants-Dataframe bauen
│      pro Customer:
│        - email = customer$emailAddress  (oder synthetic "<bookingId>-<idx>@external.guest")
│        - name  = customer$name
│        - is_organizer = FALSE
│      + Staff aus staffMemberIds:
│        - email = staff_email aus Lookup
│        - is_organizer = TRUE (analog zum organizer-Pfad in msgraph_update_events)
│
├── 8. update_events(con, df, startDate)
├── 9. update_contacts_from_events(con, df)
└── 10. update_event_participants(con, df)
```

### Neue Helper-Funktionen

- **`retrieve_booking_appointments(access_token, biz_id, startDate)`** — Paging-Loop für `/appointments`, analog zu `retrieve_calendar_events()`.
- **`retrieve_booking_staff(access_token, biz_id)`** — holt die Staff-Liste pro Business.
- **`build_staff_lookup(access_token, biz_ids, con)`** — baut den `staff_id → msgraph_user_id`-Mapping einmalig pro Lauf, mittels Email-Join gegen `raw.msgraph_users`.
- **`appointments_to_event_dataframes(appts, staff_lookup, existing_events)`** — Konvertierungslogik (Schritte 5-7); gibt zurück: `all_calendar_events_`-äquivalent + `msgraph_event_participants`-äquivalent.

## Match-Strategie zwischen Booking-Appointment und Calendar-Event

Microsoft Bookings legt für jeden gebuchten Slot zwei Spuren an:

1. Einen **Calendar-Event in der Booking-Mailbox** (sichtbar via `calendarView`, hat seinen eigenen `iCalUId`).
2. Ein **Booking-Appointment-Objekt** (eigene `id`, eigene Customer-Liste).

Beide enthalten die **gleiche Teams `meeting_id`** in `joinWebUrl`. Diese ist der Match-Anker:

- Wenn ein Calendar-Event mit dieser meeting_id existiert → Booking-Daten werden auf diese Event-Row **angewendet** (Subject bleibt, Participants kommen dazu).
- Wenn nicht (Event-Pipeline lief noch nicht oder Booking-Mailbox-Pull war leer) → neue Event-Row aus Appointment angelegt mit `msgraph_ical_uid = "booking:<id>"`.

## Edge Cases

| Case | Verhalten |
|---|---|
| Appointment ohne `joinWebUrl` (z.B. Vor-Ort) | Event-Row wird mit `msgraph_ical_uid = "booking:<id>"` angelegt, `meeting_id = NULL`. Kein Call-Match möglich, aber Event existiert. |
| Customer ohne Email | Synthetic `<bookingId>-<idx>@external.guest`, später potentiell auflösbar via `enrich_guest_participants()`. |
| Cancelled Appointment | Event-Row wird mit `is_canceled = TRUE` markiert (exaktes Status-Feld bei Impl verifizieren). |
| Appointment verschwindet aus API | Nicht angefasst — analog Cancel-Scope in `update_events()` ([msgraph_events.R:178-180](../../../R/msgraph_events.R#L178-L180)). |
| Mehrere Staff pro Appointment | Alle als Participants mit `is_organizer = TRUE`. `user_id` des Events = erster Staff. |
| Customer mit `#EXT#`-Form | Schon abgedeckt durch bestehenden Cleanup in [msgraph_events.R:228-232](../../../R/msgraph_events.R#L228-L232). |
| Mehrfacher Lauf | Idempotent über Upsert-Match-Cols `(msgraph_ical_uid, event_start)`. |

## Logging

Pro Business: Anzahl Appointments, Anzahl Customers (gesamt / mit Email / synthetic). Standard `print()`-Style wie im restlichen MSGraph-Code.

## Testing

- Keine formellen Unit-Tests (analog Rest der Codebase).
- Manueller Sanity-Check nach erstem Lauf: `raw.msgraph_event_participants` für die `3332258a-3784-4568-9cfb-6325a62de30e`-Mailbox sollte externe Customers enthalten.
- Gegenprobe gegen `mapping.msgraph_call_event`: für ein bekanntes Booking-Termin mit Telefonat sollte `event_id` + `call_id` gemappt sein.

## Out-of-Scope für diese Implementierung

- Cleanup der Booking-Mailbox-Einträge in `raw.msgraph_users` (`is_internal` bleibt unverändert).
- Backfill historischer Appointments — der erste Lauf zieht ab `startDate`.
- Anpassungen an Dashboards / Reports.
