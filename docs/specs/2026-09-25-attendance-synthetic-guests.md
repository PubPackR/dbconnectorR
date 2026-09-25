# Externe Gäste im Anwesenheitsbericht als Synthetic guest behalten
Asana: https://app.asana.com/1/734700742714256/project/1211924185203938/task/1218829097330794

## Problem

Seit dem Tenant-Wechsel (19.08.2026) landen extern geplante Termine gehäuft als
`extern_planned_intern_call` und zählen damit als No-Show (neuer Tenant 22–25 %, alter
Tenant 0–8 %). Externe Gäste stehen im Anwesenheitsbericht ohne `emailAddress`;
`msgraph_scoped_update_calls_attendance()` verwirft sie per `filter(!is.na(email))`, bevor
sie in `raw.msgraph_call_participants` kommen. Der alte callRecords-Pfad
(`msgraph_calls.R`) hat solche Teilnehmer als `guest_<identity_id>@external.guest`
behalten — das ging beim Umbau auf Attendance verloren.

Beleg (Probe 25.09.2026, base-62 `one-off/2026-09-25_probe_attendance_guests.R`): in 4 von
8 Terminen mit Bericht stehen 6 Teilnehmer ohne E-Mail, alle
`#microsoft.graph.communicationsUserIdentity` mit nur `id` und `displayName`, ohne
`tenantId`, Rolle Presenter, 136–1749 s im Call. `identity.id` ist eine GUID und gleich
der Record-id.

## Lösung

`parse_attendance_records()` bekommt `tenant_id` und macht einen Record ohne E-Mail zum
Synthetic guest, wenn seine `identity.tenantId` fehlt oder fremd ist:
`guest_<lower(identity.id)>@external.guest`, `ms_name = identity.displayName`.
`msgraph_scoped_update_calls_attendance()` reicht `cfg$tenant_id` durch. Downstream bleibt
unverändert: ein Synthetic guest ist kein interner Kontakt und macht den Call in
`msgraph_map_calls_events()` zum `extern_call`.

## Entscheidungen

- Modus: bei `/ticket` bleiben, Nachweis per Probe vor der Umsetzung.
- Probe als eigener PR in base-62 (Subtask 1), nicht als Wegwerfdatei.
- Bestehendes Schema `guest_<id>@external.guest` wiederverwenden, kein neues Präfix — der
  Glossarbegriff *Synthetic guest* und `is_synthetic_email()` greifen unverändert.
- Schlüssel `guest_<lower(identity.id)>@external.guest`. Die id ist eine GUID, das
  Kleinschreiben ist verlustfrei und kollidiert nicht mit `email_normalized = lower(email)`.
  Sie ist wahrscheinlich pro Termin neu: pro Gastteilnahme ein Kontakt, wie im alten Pfad.
- Gast wird nur, wer keine E-Mail hat **und** dessen `tenantId` fehlt oder fremd ist.
  Ohne E-Mail aus dem eigenen Tenant wird weiter verworfen — ein interner Account darf kein
  `extern_call` erzeugen. Ohne übergebenes `tenant_id` zählt nur die fehlende `tenantId`.
- `ms_name = displayName` speichern wie bisher.
- Guest-Enrichment (`enrich_guest_participants`) out of scope, Folgeticket.
- Kein Backfill-Skript: `events_days_back = 50` holt nach dem Deploy alles ab 19.08. neu.
  Deploy deshalb bis spätestens 07.10.2026.
- Akzeptanzkriterium 2 ohne Schwelle: kein `intern_call`-Termin mehr mit Teilnehmer ohne
  E-Mail; der Anteil wird gemessen und dokumentiert. Die verbleibenden `intern_call` sind
  echte No-Shows.

## Betroffen

- `R/msgraph_scoped_calls.R` — `parse_attendance_records()`, Aufruf in
  `msgraph_scoped_update_calls_attendance()`
- `tests/testthat/test-msgraph_scoped_parsers.R`
- Tabellen: `raw.msgraph_contacts`, `raw.msgraph_call_participants` (neue Zeilen),
  indirekt `mapping.msgraph_call_event.event_class`,
  `processed.msgraph_extern_event_classification.is_no_show`
- base-62-msgraph-scoped: `one-off/2026-09-25_probe_attendance_guests.R`

## Out-of-Scope

- Gäste auf echte E-Mail auflösen (Enrichment über Event-Teilnehmer)
- Organizer, deren Meetings die Policy mit 403 abweist
- Alt-Tenant-Termine

## Validierung

- Unit-Test: Record-Form aus der Probe (ohne E-Mail, ohne `tenantId`) → Synthetic guest;
  ohne E-Mail mit eigener `tenantId` → verworfen; mit E-Mail → unverändert.
- Nach Deploy und erstem Serverlauf: Probe erneut laufen lassen — kein
  `extern_planned_intern_call`-Termin mehr mit Teilnehmer ohne E-Mail.
- Anteil `intern_call` an den vergangenen Terminen mit Link im neuen Tenant messen und im
  Ticket dokumentieren (Erwartung aus der Stichprobe: etwa halbiert).
