# crm_unbekannt als stattgefundenen Termin zaehlen

Asana: https://app.asana.com/1/734700742714256/task/1218192760112154

## Problem

Ein CRM-Termin ohne dokumentierten Ausgang (`meeting_status = 'unbekannt'`) wird
in `processed.sales_meetings_unified` als nicht stattgefunden gefuehrt und wirkt
damit rechnerisch wie ein No-Show: `crm_status_flags()` setzt `excluded = TRUE`
und `is_no_show = NA`, die Zeile steht danach im Nenner der Show-Up-Quote
(`bewertbar`), aber nie im Zaehler (`stattgefunden`).

Fehlende Dokumentation ist kein Beleg fuer einen geplatzten Termin. Gemessen an
MSGraph-verifizierbaren Terminen sind undokumentierte Termine sogar etwas
seltener No-Shows (25,3 %) als der Durchschnittstermin (29,0 %), weil ein
geplatzter Termin Nacharbeit erzeugt und damit Doku.

Dazu kommt eine Divergenz innerhalb von dbconnectorR: dieselbe CRM-Zeile traegt
in `processed.msgraph_extern_event_classification` bereits
`is_no_show = FALSE, excluded = FALSE` (`assemble_crm_classification_rows()`),
wird beim Bau der Fakttabelle aber von `crm_status_flags()` neu als
ausgeschlossen geflaggt. Zwei Tabellen, zwei Lesarten derselben Zeile.

## Loesung

`crm_status_flags()` gibt fuer `meeting_status = 'unbekannt'` kuenftig
`is_no_show = FALSE` und `excluded = FALSE`. Damit faellt der Ausschlussgrund
`crm_unbekannt` weg, die Zeile zaehlt als stattgefundener Termin, und die
Fakttabelle stimmt mit der Klassifikationstabelle ueberein.

Die Beobachtung selbst geht nicht verloren: `meeting_status = 'unbekannt'` und
`no_show_source = 'crm_only'` bleiben unveraendert auf der Zeile.

## Entscheidungen

- **Producer, nicht Konsument.** Der Fix sitzt in `crm_status_flags()`, nicht in
  `kpiR::termin_flags()`. Grund: die Divergenz existiert bereits zwischen zwei
  dbconnectorR-Funktionen; ein Konsumenten-Fix stellt eine dritte Lesart daneben
  und muesste an vier kpiR-Stellen einzeln nachgebaut werden, zwei davon in SQL.
- **Alle vier abhaengigen Kennzahlen aendern sich mit**, nicht nur die
  gemeldete. Gemessen 09/2025 bis 08/2026 trifft es aber andere als gedacht:
  - `kpi_sales_vcs_stattgefunden()` (Grundgroesse 8): **+7,4 bis +16,0 % je
    Monat**, ueber den Zeitraum 8.833 auf 9.929 VCs (+12,4 %). Der Weg dorthin
    ist `contact_id`, also der Rep-Kontakt der CRM-Zeile.
  - `kpi_sales_termine_showup()`: **null**, in allen zwoelf Monaten. Ein
    CRM-Task kennt keinen Organisator, jede crm_task-Zeile traegt deshalb
    `restmenge = organizer_crm_task` und faellt aus `shiny.sales_kpi_termine`
    (`restmenge IS NULL`) heraus. Sie stand nie im Nenner.
  - SDR-Anker und `erster_vc_je_lead()` (Closing-Time, `n_closes_ohne_vc`):
    **ungemessen**. Letzterer braucht gar keine Person, dort wirkt es voll.
- **shiny-99-modules braucht keine Codeaenderung.** Alle Aufrufer von
  `filter_by_exclusion_scope()` lesen `msgraph_extern_event_classification`, und
  `get_responsible_event_pool()` verwirft crm_task-Zeilen ueber
  `filter(!is.na(event_date))`. Der `crm_unbekannt`-Absatz in
  `observability_exclusion_reasons()` ist Doku und wird nachgezogen.
- **Wirkung vor dem Merge ausgezaehlt**, Skript unter `one-off/`. Der
  Badge-Fix aus PR 43 ist dabei entgegen der Annahme bereits installiert und
  gelaufen: alle 1.873 crm_task-Zeilen tragen `task_badge = 'visit'`.

## Betroffen

- `R/sales_meetings_unified.R` — `crm_status_flags()`, Doku
- `tests/testthat/test-sales_meetings_unified.R`
- `inst/sql/2026-09-05-exclusion-reason-ohne-crm-unbekannt.sql` — Spaltenkommentar,
  der `crm_unbekannt` noch als moeglichen Wert fuehrt
- Tabelle `processed.sales_meetings_unified` (Rebuild noetig, voll rueckwirkend)
- Nachgezogene Doku: `kpiR/R/support_termine_helpers.R`,
  `shiny-99-modules/func/module_sales_kpi/external_events_helpers.R`

## Out-of-Scope

- Zusaetzliche Statusquellen fuer CRM-Termine (geprueft und verworfen, PR 44)
- Aenderungen an `crm_storniert`, das bleibt ein Ausschluss
- Die Ansage an Sales; sie ist bei +12,4 % auf Grundgroesse 8 noetig, den Text
  macht Moritz
- **Dass CRM-Termine gar keine Sales-Person tragen.** Der Befund faellt hier an
  und ist groesser als dieses Ticket: er betrifft alle crm_task-Zeilen, nicht
  nur die undokumentierten. Eigenes Ticket.

## Validierung

- `SELECT exclusion_reason, count(*) FROM processed.sales_meetings_unified
  GROUP BY 1` nach dem naechsten Lauf: `crm_unbekannt` faellt auf 0, die
  Gesamtzahl der Zeilen bleibt unveraendert.
- Videocalls-Tab: die stattgefundenen VCs steigen je Monat um 7 bis 16 %.
- Terminierung-Tab und Show-Up-Quote: duerfen sich **nicht** aendern.
