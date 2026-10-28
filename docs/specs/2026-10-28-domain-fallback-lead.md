# Meetings ohne gemappten Teilnehmer über eindeutige Domain einem Lead zuordnen
Asana: https://app.asana.com/0/0/1219978256263059/f · Rahmen: https://app.asana.com/0/0/1219968320061827/f

## Problem

`update_sales_meetings_unified()` leitet den Lead eines MSGraph-Meetings nur aus
`mapping.crm_lead_msgraph_contact` ab. Steht die Adresse des externen Teilnehmers nicht im CRM,
bleibt das Meeting ein Leadless meeting (`lead_id = NA`) und landet in kpiR im Kundensegment
`unbekannt`. Oft steht aber eine Kollegin oder ein Kollege derselben Firma im CRM.

Gemessen 28.10.2026 (12 Monate, VCs nur mit Sales-Beteiligung, Query
`package-02-kpiR/analytics/2026-10-28-fallback-wirkung.sql`): Nach B2 (Platzhalter-Match übernimmt
den Lead) und B3 (`is_primary_crm` entfällt) blieben 248 Termine (1,6 %) und 96 VCs (0,9 %) ohne
Lead, deren externe Domain im CRM genau einem Lead gehört.

## Lösung

Nach `assemble_unified_meetings()` bekommt jede MSGraph-Platzhalter-Zeile, die nicht über einen
CRM-Task zugeordnet wurde und bei der kein externer Teilnehmer irgendein Mail-Mapping hat, je
externer Domain den Lead, dem diese Domain im CRM eindeutig gehört (Domain-matched lead).

## Entscheidungen

- **Q3 Domain-Fallback bei genau einem Lead:** passt so, wird gebaut.
- **Q9 Regel für Domains mit mehreren Leads:** gehen wir noch nicht an.
- **Q10 Wann greift der Fallback:** nach dem CRM-Task-Abgleich, nur für Zeilen ohne Lead und ohne
  `no_show_source = 'crm_override'`. Vorher gesetzt fände ein CRM-Task mit dem echten Lead keinen
  Platzhalter mehr und würde als netto-neue Zeile doppelt zählen.
- **Q11 Was heißt „ohne gemappten Teilnehmer“:** kein externer Teilnehmer hat irgendein Mapping,
  auch kein nicht-primäres. Damit ist der Fallback unabhängig davon, ob B3 vorher oder nachher
  gebaut wird.
- **Q12 Kennzeichnung:** keine neue Spalte, kein DDL.
- **Q13 Freemail-Liste und Domain-Basis:** Konstante `FREEMAIL_DOMAINS` neben
  `INTERNAL_EMAIL_PATTERNS`; Domain-Basis sind alle nicht gelöschten Einträge in
  `raw.crm_lead_mail_address`, wie in der Messung.
- **Q14 `meeting_key` und Begriff:** Fallback-Zeilen tragen `msgraph_<id>_<lead>` wie gemappte.
  Begriff Domain-matched lead ins `CONTEXT.md`, Leadless meeting ergänzt.
- **Q15 Mehrere eindeutige Domains in einem Meeting:** eine Zeile je Lead, wie beim Mapping.

## Betroffen

- `R/sales_meetings_unified.R`: `update_sales_meetings_unified()` lädt Domain-Leads und alle
  Mappings, neue Funktionen `domain_leads_aus_crm_mails()` und `apply_domain_fallback()`.
- `R/utils_msgraph_func.R`: Konstante `FREEMAIL_DOMAINS`.
- Tabellen gelesen: `raw.crm_lead_mail_address`, `mapping.crm_lead_msgraph_contact` (ohne Filter
  auf `is_primary_crm` für die Vorrang-Prüfung). Geschrieben: `processed.sales_meetings_unified`.
- Folgewirkung in kpiR (kein Code): mehr Zeilen mit Lead verschieben die Übernahme durch
  SDR-Termintage (ADR 0019) und die erste VC je Lead für Closes.

## Out-of-Scope

- Domains mit mehreren Leads (Q9), Freemail und unbekannte Domains (Rahmenticket Q7).
- B2 und B3, eigene Tickets.
- Lead-Ableitung für `crm_task_meeting_classification` und shiny-99.

## Validierung

- Nach Deploy und Lauf: `fallback-wirkung.sql` zeigt Stand „5 Fallback: Domain bei 1 Lead“ nicht
  mehr als offenen Rest, sondern unter „1 Lead heute“; die Zeilen in Stand 3, 6, 7 und 8 bleiben
  in der Größenordnung gleich.
- `SELECT count(*) FROM processed.sales_meetings_unified WHERE source = 'crm_task'` ist vor und
  nach dem Lauf gleich (kein CRM-Task wird durch den Fallback netto-neu).
