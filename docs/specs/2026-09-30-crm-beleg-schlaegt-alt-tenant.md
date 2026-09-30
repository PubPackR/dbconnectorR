# CRM-Beleg schlägt Alt-Tenant-Ausschluss bei VC-Terminen

Asana: https://app.asana.com/1/734700742714256/project/1211924185203938/task/1218788153926894

## Problem

Seit dem Tenant-Wechsel (19.08.2026) schließt `compute_observability_exclusions()` Meetings mit
einem Teams-Link aus dem Alt-Tenant als nicht beobachtbar aus (`exclusion_reason =
'alt_tenant_join_url'`). Trifft ein CRM-VC-Task auf so ein Meeting, setzt der `crm_override`-Zweig
in `assemble_unified_meetings()` zwar `is_no_show` nach dem CRM-Status, lässt aber `excluded =
TRUE` und den Grund stehen. Der Termin zählt dadurch in `kpiR::kpi_sales_vcs_stattgefunden()` nie
als VC und fehlt im Nenner der Angebotsquote (September 2026: 124 %), in Show-Up, No-Show und
Produktivität.

Ohne Kalender-Match wäre derselbe CRM-Task eine netto-neue `crm_only`-Zeile und zählte schon heute
als VC. Erst der Match auf das ausgeschlossene Meeting löscht den Beleg.

Gemessen am 23.09.2026 (`is_responsible`, vergangene Termine, Alt-Tenant mit CRM-Task): August 105
`unbekannt`, 5 `show_up`, 3 `no_show`; September 227 `unbekannt`, 7 `show_up`, 8 `no_show`.

## Lösung

Im `crm_override`-Zweig gilt die Anwesenheit eines Meetings als **nicht messbar**, wenn es
nachweislich keinen Teams-Link trägt (ADR 0015) **oder** aus dem Alt-Tenant stammt. Für ein
Alt-Tenant-Meeting mit CRM-Task heißt das:

| CRM-Status | `excluded` | `exclusion_reason` | `is_no_show` |
|---|---|---|---|
| `show_up` | FALSE | NA | FALSE |
| `unbekannt` | FALSE | NA | FALSE |
| `no_show` | FALSE | NA | TRUE |
| `storniert` | TRUE | `crm_storniert` | unverändert |

## Entscheidungen

- **Q1, Ort der Regel**: im `crm_override`-Zweig von `assemble_unified_meetings()`, nicht in
  `compute_observability_exclusions()`. Nur dort ist bekannt, welcher CRM-Task zu welchem Meeting
  gehört. `processed.msgraph_extern_event_classification` bleibt unverändert.
- **Q2, Grund nach dem Override**: `excluded = FALSE`, `exclusion_reason = NA`. Der Termin ist
  danach ein normaler, bewertbarer Termin und zählt in VCs, Show-Up und No-Show. Nachvollziehbar
  bleibt er über `no_show_source = 'crm_override'` und `meeting_status`, die Klassifikationstabelle
  führt den Grund weiter. Bestätigt am 30.09.2026 im Wissen um den Preis: `unbekannt` stellt im
  September 227 von 242 geretteten Terminen, die Show-Up-Quote September steigt dadurch grob von
  45 auf 67 %, und undokumentierte Termine sind gemessen zu 25 % No-Shows. Deshalb weist die
  Nachmessung die Show-Up-Quote mit und ohne die geretteten `unbekannt` aus.
- **Q3, Formulierung**: eine Bedingung „Anwesenheit nicht messbar“ = kein Link **oder**
  Alt-Tenant, nicht ein zweiter Zweig daneben. Aufgehoben wird **nur** der Grund
  `alt_tenant_join_url`; ein gematchtes `duplikat_event`, `verschoben` oder
  `termin_in_zukunft` bleibt ausgeschlossen. Die Grenze 19.08.2026 steckt schon im Grund.
  Treffen zwei CRM-Tasks dieselbe Zeile, bleibt ein Storno in jeder Reihenfolge stehen
  (Befund aus dem Code-Review).
- **Q4, ADR-Nachtrag**: ADR 0015 und `CONTEXT.md` in `package-02-kpiR`, eigener Doku-PR mit
  eigenem Subtask. Merge erst nach der Installation dieses Pakets.
- **Q5, mehrere Leads**: der Override trifft wie bisher nur die Zeile des gematchten Leads. Die
  übrigen Lead-Zeilen desselben Alt-Tenant-Meetings bleiben ausgeschlossen. Die VC-Zählung ist
  davon unberührt, `kpiR::entdopple_auf_meeting()` kann aber die ausgeschlossene Zeile behalten;
  dann fehlt das Meeting in der Show-Up-Quote. Umfang wird gemessen (Validierung, Abfrage 3),
  bei Relevanz eigenes Ticket.
- **Q6, Abnahme**: kein Zielwert für die Angebotsquote. Vorher/Nachher um den ersten
  Producer-Lauf, die VC-Differenz muss der Zahl der geretteten Meetings entsprechen. Die
  Show-Up-Nachmessung im Nachbarticket (Asana 1218397071350702) läuft erst nach diesem Deploy.

## Betroffen

- `R/sales_meetings_unified.R`: `assemble_unified_meetings()`, `crm_override`-Zweig
- `tests/testthat/test-sales_meetings_unified.R`
- `CONTEXT.md`: **CRM override**, neu **Unmeasurable attendance**
- Tabelle `processed.sales_meetings_unified`, voll neu aufgebaut von
  `update_sales_meetings_unified()` im Nachtlauf von `base-62-msgraph-scoped/do/main.R`
- Konsumenten: `package-02-kpiR` (`kpi_sales_vcs_stattgefunden()`, `termin_flags()`,
  `sdr_termine()`), danach `shiny.sales_kpi_*`

## Out-of-Scope

- Alt-Tenant-Termine **ohne** CRM-Task (September 558): die schließt nur der Umzug der Termine
  durch Sales.
- `processed.msgraph_extern_event_classification` und die alten Oberflächen, die sie lesen
  (No-Show-Sub-Tab in shiny-22, trägt bereits einen Divergenz-Marker).
- `entdopple_auf_meeting()` in kpiR (Q5).
- Die Definition **No-show rate** in `CONTEXT.md` nennt noch den Ausschluss unbekannter
  CRM-Ausgänge und ist seit dem 05.09.2026 überholt.

## Validierung

Reihenfolge: PR mergen → Paket installieren (Festangestellte) → Abfragen 1 und 2 als
**Vorher** → Nachtlauf base-62 → kpiR-Materialisierung → Abfragen 1 bis 4 als **Nachher**.

**Abfrage 1: gerettete Meetings je Monat und Status.** Läuft vorher und nachher gleich. Vorher
steht in `noch_ausgeschlossen` jede Zeile, nachher nur noch `storniert`.

```sql
WITH alt AS (
  SELECT DISTINCT call_event_mapping_id
  FROM processed.msgraph_extern_event_classification
  WHERE source = 'msgraph' AND exclusion_reason = 'alt_tenant_join_url'
), u AS (
  SELECT u.*, split_part(u.meeting_key, '_', 2)::bigint AS cem_id
  FROM processed.sales_meetings_unified u
  WHERE u.source = 'msgraph' AND u.no_show_source = 'crm_override'
    AND u.is_responsible AND NOT u.is_short_lived_event
    AND u.event_date >= DATE '2026-08-01' AND u.event_date < CURRENT_DATE
)
SELECT date_trunc('month', u.event_date)::date AS monat,
       u.meeting_status,
       count(*)                                                  AS zeilen,
       count(DISTINCT u.event_id)                                AS meetings,
       count(*) FILTER (WHERE u.excluded)                        AS noch_ausgeschlossen,
       -- je (Rep, Meeting) wie kpiR zaehlt, nicht je Lead-Zeile
       count(DISTINCT (u.contact_id, u.event_id))
         FILTER (WHERE NOT u.excluded AND NOT u.is_no_show)      AS zaehlt_als_vc,
       count(*) FILTER (WHERE NOT u.excluded AND u.is_no_show)   AS no_show
FROM u JOIN alt ON alt.call_event_mapping_id = u.cem_id
GROUP BY ROLLUP (date_trunc('month', u.event_date)::date, u.meeting_status)
ORDER BY 1, 2;
```

**Abfrage 2: Team-VCs und Angebotsquote je Monat.** Die Differenz nachher minus vorher in
`n_vcs` muss je Monat `zaehlt_als_vc` aus Abfrage 1 (nachher) entsprechen. Abweichungen um
wenige Termine erklären neue oder umgezogene Termine zwischen den Läufen.

```sql
SELECT monat, sum(n_vcs) AS n_vcs, sum(n_angebote) AS n_angebote,
       round(100.0 * sum(n_angebote) / NULLIF(sum(n_vcs), 0), 1) AS angebotsquote_team
FROM shiny.sales_kpi_zusammengesetzt
WHERE monat >= DATE '2026-05-01'
GROUP BY monat ORDER BY monat;
```

**Abfrage 3: Meetings mit mehreren Leads (Q5).**

```sql
WITH alt AS (
  SELECT DISTINCT call_event_mapping_id
  FROM processed.msgraph_extern_event_classification
  WHERE source = 'msgraph' AND exclusion_reason = 'alt_tenant_join_url'
), ov AS (
  SELECT u.event_id, u.lead_id
  FROM processed.sales_meetings_unified u
  JOIN alt ON alt.call_event_mapping_id = split_part(u.meeting_key, '_', 2)::bigint
  WHERE u.source = 'msgraph' AND u.no_show_source = 'crm_override' AND u.is_responsible
), m AS (
  SELECT event_id, count(*) AS n_zeilen, min(lead_id) AS kleinste_lead_id
  FROM processed.sales_meetings_unified
  WHERE source = 'msgraph' AND is_responsible
  GROUP BY event_id
)
SELECT count(DISTINCT ov.event_id)                                          AS gerettete_meetings,
       count(DISTINCT ov.event_id) FILTER (WHERE m.n_zeilen > 1)            AS mit_mehreren_leads,
       count(DISTINCT ov.event_id) FILTER (WHERE m.n_zeilen > 1
         AND ov.lead_id IS DISTINCT FROM m.kleinste_lead_id)                AS entdopplung_behaelt_andere_zeile
FROM ov JOIN m USING (event_id);
```

**Abfrage 4: Show-Up-Quote mit und ohne gerettete `unbekannt`** (nachher, Organizer in Sales).

```sql
WITH alt AS (
  SELECT DISTINCT call_event_mapping_id
  FROM processed.msgraph_extern_event_classification
  WHERE source = 'msgraph' AND exclusion_reason = 'alt_tenant_join_url'
), gerettet_unbekannt AS (
  SELECT u.meeting_key
  FROM processed.sales_meetings_unified u
  JOIN alt ON alt.call_event_mapping_id = split_part(u.meeting_key, '_', 2)::bigint
  WHERE u.source = 'msgraph' AND u.no_show_source = 'crm_override'
    AND u.meeting_status = 'unbekannt'
)
SELECT e.monat_gelegt_in AS monat,
       round(100.0 * count(*) FILTER (WHERE e.bewertbar AND e.stattgefunden)
             / NULLIF(count(*) FILTER (WHERE e.bewertbar), 0), 1)          AS showup_mit,
       round(100.0 * count(*) FILTER (WHERE e.bewertbar AND e.stattgefunden AND g.meeting_key IS NULL)
             / NULLIF(count(*) FILTER (WHERE e.bewertbar AND g.meeting_key IS NULL), 0), 1) AS showup_ohne,
       count(*) FILTER (WHERE e.bewertbar AND g.meeting_key IS NOT NULL)   AS gerettete_unbekannt
FROM shiny.sales_kpi_termine_ereignisse e
LEFT JOIN gerettet_unbekannt g USING (meeting_key)
WHERE e.organizer_status = 'sales' AND e.monat_gelegt_in >= DATE '2026-06-01'
GROUP BY e.monat_gelegt_in ORDER BY 1;
```
