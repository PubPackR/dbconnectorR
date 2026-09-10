# VC-Termin-Erkennung: Badge statt nur Titel

Asana: https://app.asana.com/1/734700742714256/task/1218210627259100

## Problem

`is_vc_task()` beantwortet, ob ein CRM-Task einen **Videocall** meint (Kanal), sagt aber
nichts darüber, ob er einen **Termin** meint (Art). Weil sie allein entscheidet, landen
Terminierungs-Aufgaben als eigene Zeilen in `processed.sales_meetings_unified`, z.B.
`crm_task_id 23628741` — "Terminierung VC NV Frau Benchenna", badge `important`.

Solche Zeilen tragen `exclusion_reason = 'crm_unbekannt'`. Das zählt in kpiR als
beobachtbar (`ist_beobachtbarer_termin()`, `nicht_messbar` enthält nur
`alt_tenant_join_url` und `crm_storniert`), sie stehen also im Nenner der Show-Up-Quote
und können wegen `is_no_show = NA` nie im Zähler landen.

Gemessen 09/2025–08/2026: 34–82 solcher Zeilen pro Monat, 3,1–6,7 % des Nenners.

## Lösung

Termin = Videocall **und** Termin-Badge:

```r
is_crm_vc_meeting(task_name, task_badge)  # is_vc_task(task_name) & task_badge == "visit"
```

Eine Funktion, beide Konsumenten rufen sie auf. `is_vc_task()` bleibt unverändert.

## Entscheidungen

- **Anker ist `task_badge = 'visit'`.** Über alle CRM-Tasks tragen 67,7 % der
  `visit`-Tasks einen belegten MSGraph-Kalendertermin, gegen 12,2 % (`important`),
  1,1 % (`task`) und 0,5 % (`call`). Faktor 5,5 zum nächstbesten Wert.
- **Kein Titel-Muster**, weder als Ausschluss noch als Einschluss. Von 41 `visit`-Tasks
  mit "Terminierung" im Namen haben 28 einen belegten Kalendertermin (68 %, über der
  Basisquote von 63 %). Ein zusätzlicher Titel-Ausschluss zerstört 28 belegte Termine,
  um 13 unsichere zu entfernen.
- **Positivliste, kein `NOT IN`.** `task_badge` ist nullable; NULL darf nicht durchrutschen.
- **Kein `is_finished`-Filter.** Misst Aufgaben-Erledigung, nicht Terminwahrnehmung.
  Geplante Termine bleiben drin.
- **`important` fliegt mit raus**, obwohl es der teuerste Ausschluss ist (21 der 41
  verlorenen belegten Termine). 8,2 % Kalenderquote im VC-Ausschnitt gegen 69 % bei `visit`.
- **Preis der Regel**: 41 von 5.235 belegten Terminen fallen weg, 0,8 %.
- **Der Informativ-Filter** in `crm_task_meeting_classification.R:45` bleibt unangetastet.
  Ihn nach `sales_meetings_unified` zu portieren würde 93 % der CRM-Zeilen löschen —
  eigenes Ticket.

## Betroffen

- `R/crm_task_meeting_parser.R` — neue Funktion `is_crm_vc_meeting()`
- `R/sales_meetings_unified.R` — `task_badge` selektieren, neue Funktion nutzen
- `R/crm_task_meeting_classification.R` — dito, `@param crm_tasks` erweitern
- `tests/testthat/test-crm_task_meeting_parser.R`, `test-crm_task_meeting_classification.R`
- Schreibt nach `processed.sales_meetings_unified` und
  `processed.msgraph_extern_event_classification` (beide werden je Lauf neu geschrieben,
  der Effekt gilt rückwirkend für alle Monate)

## Out-of-Scope

- `is_vc_task()` selbst — nie gemessen, eigener Verdacht ("Videocall"/"Videokonferenz"
  werden nicht erkannt), eigenes Ticket
- Zukunftstermine im Quotennenner — gehört zu Ticket 1218179753663016
- Der Informativ-Filter (siehe oben)

## Validierung

- Zahl der `crm_task`-Zeilen in `sales_meetings_unified` sinkt um ~796 (12 Monate)
- Zahl der `msgraph`-Zeilen bleibt **unverändert** — steigt sie, hat der Fix echte
  Termine mitgenommen
- Show-Up-Quote je Monat steigt um 1,4 bis 4,1 Prozentpunkte
