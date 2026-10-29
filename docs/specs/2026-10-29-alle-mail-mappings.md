# Lead-Ableitung nutzt alle CRM-Mail-Mappings statt nur is_primary_crm
Asana: https://app.asana.com/0/0/1219978400790427/f · Rahmen: https://app.asana.com/0/0/1219968320061827/f · Folgeticket: https://app.asana.com/0/0/1219978596056611/f (B2)

## Problem

Die Lead-Ableitung für Meetings filtert `mapping.crm_lead_msgraph_contact` auf `is_primary_crm`.
`is_primary` markiert je Lead nur die Adresse des höchsten Typs (office vor office_hq vor
other/private vor from_description, `R/crm_lead_mails_background.R`). Ein Teilnehmer, dessen
Adresse im CRM nur als nachrangige Adresse eines Leads steht, liefert deshalb keinen Lead.

Gemessen 28.10.2026 (MSGraph-VCs, 12 Monate, `package-02-kpiR/analytics/2026-10-28-is-primary-fanout.sql`):
ohne den Filter bekämen 1.191 VCs (12,2 %) neu genau einen Lead, 21 VCs (0,2 %) zwei Leads, 64 VCs
(0,7 %) mit Lead weitere dazu. Gemessen 29.10.2026 (B2): 692 Zusammenlegungen per Ersatzregel
waren über ein nicht-primäres Mapping beim Lead des CRM-Tasks bestätigt.

## Lösung

- `update_sales_meetings_unified()`: Lead je Meeting aus jedem Mapping. Mehrere Leads ergeben wie
  bisher eine Zeile je Lead.
- `update_crm_task_meeting_classification()`: der Abgleich „hat der Kalender diesen Termin schon“
  leitet den Lead eines Meetings aus den externen Teilnehmern ab, über jedes Mapping. Bisher kam er
  über die `contact_id` der Klassifikationszeilen, und das sind nur interne Kontakte.
- Beide nutzen dieselbe reine Funktion `meeting_leads_aus_teilnehmern()`.
- shiny-99-modules `func/external_lead_contact_mapping.R`: `external_lead_contact_mapping()` und
  `get_internal_mapped_contact_ids()` nutzen jedes Mapping (eigener PR).

## Entscheidungen

- **Q2 (Analyse) `is_primary_crm` als Filter:** fällt.
- **Q23 Filter auch in `crm_task_meeting_classification`:** „passt alles“: ja. Der Tab
  „VC-Tool & Show-Up“ zeigt dadurch weniger CRM-Termine, nämlich nur noch die, die der Kalender
  auch über nachrangige Adressen nicht sieht.
- **Q24 Subtask für die Klassifikation:** „passt alles“: eigener Subtask, selber dbconnectorR-PR.
- **Q25 `CONTEXT.md`:** „passt alles“: Halbsatz bei Leadless meeting, kein ADR.
- **Q26 Reihenfolge:** „passt alles“: zuerst dbconnectorR, dann shiny-99, beide einzeln mergebar.
- **Q27 Abgleich in `crm_task_meeting_classification`:** „b, Abgleich reparieren“: im selben PR
  auf die externen Teilnehmer umstellen, mit derselben Funktion wie die Termintabelle.

## Betroffen

- `R/sales_meetings_unified.R`, `R/crm_task_meeting_classification.R`, `CONTEXT.md`.
- Gelesen: `mapping.crm_lead_msgraph_contact`. Geschrieben: `processed.sales_meetings_unified`,
  `processed.msgraph_extern_event_classification` (Zeilen `source = 'crm_task'`); in base-62 daraus
  neu gebaut `shiny.sales_kpi_termine_ereignisse` und `shiny.sales_kpi_sdr_termintage`.
- shiny-99-modules: sieben Module über `external_lead_contact_mapping()` (closer_performance,
  external_employees, funnel_cohort, sales_kpi, videocalls_scheduling, videocalls_sdr_monitoring,
  sales_pipeline). Alle verarbeiten bereits mehrere Leads je Meeting.

## Out-of-Scope

- `base-02-asana_auswertung` nutzt `is_primary_crm` für die Anrede und behält den Filter.
- Domain-Fallback vor dem Abgleich und Ersatzregel mit Zeitfenster (B2).
- Wie das Mapping selbst `is_primary` vergibt, auch office_hq-Adressen fremder Personen.

## Validierung

- Nach Paket-Deploy und base-62-Lauf: `2026-10-28-is-primary-fanout.sql` zeigt die rund 1.190
  VCs nicht mehr als „nur nicht-primär gemappt“, sondern mit Lead.
- `SELECT count(*) FILTER (WHERE lead_id IS NULL) FROM processed.sales_meetings_unified WHERE
  source = 'msgraph'` sinkt gegenüber dem Stand vor dem Lauf.
- `package-02-kpiR/analytics/2026-10-29-vc-tool-antijoin.sql` vorher und nachher: die Gruppen
  „Kalender hat den Termin“ fallen nach dem Lauf weg, der Tab „VC-Tool & Show-Up“ zeigt nur noch
  Gruppe c.
- Tests: `meeting_leads_aus_teilnehmern()` liefert Leads aus jedem Mapping, keine aus internen
  oder synthetischen Teilnehmern, eine Zeile je Meeting und Lead, auch mit integer64-IDs.
