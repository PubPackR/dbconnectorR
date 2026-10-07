# Nicht abrufbares Online-Meeting zählt nicht als No-Show
Asana: https://app.asana.com/0/0/1218828831136393/f

## Problem

Der Calls-Job (`msgraph_scoped_update_calls_attendance`) erzeugt einen Call nur, wenn Microsoft
zu einem Teams-Link einen Anwesenheitsbericht liefert. Scheitert die Abfrage, wird das gezählt und
geloggt, der Termin bleibt ohne Call und zählt als `no_call`, also als No-Show. Drei Fälle sind
dabei gar nicht messbar:

- **403**: Die `CsApplicationAccessPolicy` deckt den Organisator nicht. Der Job sperrt ihn für den
  Rest des Laufs.
- **nicht gefunden**: Die Suche über `JoinWebUrl` liefert 200 ohne Treffer.
- **Organisator unbekannt**: Der Organisator steht nicht in `raw.msgraph_users` oder trägt
  `is_deleted`. Die Discovery fragt seine Meetings nie ab.

Gemessen 06./07.10.2026 (Queries in `package-02-kpiR/analytics/2026-10-06-online-meeting-nicht-abrufbar.sql`,
Probe `base-62-msgraph-scoped/one-off/2026-10-06_probe_online_meeting_ausgang.R`):
439 externe No-Shows seit 19.08. ohne jeden Call am Link. In der Probe (40 Links ohne Call) sind
29 gefunden ohne Bericht, 11 `policy_403`, 0 nicht gefunden, 0 Abruffehler. 24 Organisatoren mit
153 Links seit 19.08. werden nie abgefragt (Bertelsmann-Konten, Schwesterfirmen, eine
Alt-Tenant-Adresse).

## Lösung

Der Calls-Job speichert je Link den **Ausgang der Online-Meeting-Suche** auf `raw.msgraph_events`.
Die Klassifikation leitet daraus einen neuen Ausschlussgrund ab, der wie `alt_tenant_join_url`
aus Zähler und Nenner jeder Anwesenheitsquote fällt.

## Entscheidungen

- **Ablage**: drei Spalten auf `raw.msgraph_events`, gesetzt für alle Events mit derselben
  `join_url` (Vorbild `join_url_checked_at`). 1085 von 1374 Links haben genau ein Event.
  - `online_meeting_lookup` text: `gefunden_mit_bericht` · `gefunden_ohne_bericht` ·
    `nicht_gefunden` · `policy_403` · `organisator_unbekannt` · `abruf_fehler`
  - `online_meeting_lookup_http_status` integer: nur bei `abruf_fehler`
  - `online_meeting_lookup_at` timestamp ohne tz, UTC (wie `join_url_checked_at`): seit wann der
    aktuelle Ausgang gilt. Geschrieben wird eine Zeile nur, wenn sich Ausgang oder HTTP-Status
    ändern, sonst zöge `trigger_set_updated_at` jede Nacht `updated_at` des ganzen
    50-Tage-Fensters mit. Der Zeitpunkt springt nur beim Ausgang, nicht beim Status.
- **Ausgänge im Job**:
  - Suche 403 → `policy_403`, auch für die danach übersprungenen Links desselben Organisators.
  - Suche 200 ohne Treffer → `nicht_gefunden`.
  - Organisator in Graph unbekannt (404 auf das Konto, oder per E-Mail nicht auflösbar) →
    `organisator_unbekannt`.
  - Suche oder Berichtsabruf mit anderem Status als 200/403 → `abruf_fehler` mit HTTP-Status.
  - Berichtsabruf 200 ohne Bericht, oder nur Berichte ohne Teilnehmende → `gefunden_ohne_bericht`.
  - Mindestens eine Session geschrieben → `gefunden_mit_bericht`.
- **Discovery fragt jeden Organisator ab**: auch mit `is_deleted`. Fehlt der Organisator in
  `raw.msgraph_users`, löst der Job sein Konto per E-Mail über Graph auf (`User.ReadBasic.All`,
  wie der Users-Job). Gelingt das nicht → `organisator_unbekannt`.
- **Veraltete oid** (Code-Review 07.10.): Antwortet die Suche mit 403 oder 404 auf eine oid aus
  `raw.msgraph_users`, löst der Job das Konto einmal per E-Mail auf. Liefert Graph eine andere
  oid, sucht er damit neu. Grund: rund 290 interne Konten stammen aus dem Directory-Load von
  base-35 und tragen noch die oid des alten Tenants. Je Kontakt nimmt die Discovery genau eine
  Zeile aus `raw.msgraph_users`: keine `merged-%`-Platzhalter, nicht gelöschte und interne zuerst.
- **Ein Befund wird nie verschlechtert**: Steht schon `gefunden_mit_bericht` oder
  `gefunden_ohne_bericht`, überschreibt ein späteres `policy_403`, `organisator_unbekannt` oder
  `abruf_fehler` ihn nicht. Grund: Nach dem Ausscheiden ist das Konto bei Microsoft weg, und der
  Befund aus der Zeit davor ist der einzige.
- **Fehlerquote** (Abbruch über 50 %): `policy_403` und `organisator_unbekannt` zählen nicht
  hinein, `nicht_gefunden` und `abruf_fehler` schon. Ist die Suche systematisch kaputt, bricht der
  Lauf weiter laut ab.
- **Ausschlussgrund** `online_meeting_nicht_abrufbar` für `nicht_gefunden`, `policy_403` und
  `organisator_unbekannt`. Ein gefundener Call sticht ihn (wie beim Alt-Tenant).
  `termin_in_zukunft` hat Vorrang. **Keine Datumsschranke**: Der Ausgang entsteht nur im Fenster
  von base-62, vor dem 19.08. lägen darin 3 Termine.
- **`abruf_fehler` zählt vorerst als No-Show** (Status quo). Die Probe fand keinen einzigen; die
  gespeicherte HTTP-Status-Spalte zeigt, ob sich das ändert.
- **Gefunden ohne Bericht bleibt ein No-Show.** Je Organisator verteilt sich das über alle, die
  überhaupt Berichte haben (Query 5); Organisatoren mit abgeschaltetem Bericht sind nicht zu sehen.
- **CRM-Beleg hebt den Grund auf** in `assemble_unified_meetings`, wie bei `alt_tenant_join_url`
  (Entscheidung 30.09., Asana 1218788153926894): `ms_nicht_messbar` umfasst ihn, `show_up` /
  `no_show` / `unbekannt` heben den Ausschluss auf, `storniert` wird `crm_storniert`.
- **Konsumenten**: kpiR `ist_beobachtbarer_termin()` führt ihn als nicht messbar,
  `termine_stattgefunden()` behält ihn als SDR-Anker wie `alt_tenant_join_url`. shiny-99-modules
  `observability_exclusion_reasons()` führt ihn, sonst fallen die Termine aus "Termine gelegt".
- **Backfill**: kein eigener Code im Paket, sondern ein One-off in base-62, das den Calls-Job
  einmal mit einem Fenster bis zum 19.08.2026 laufen lässt.
- **Deploy-Reihenfolge**: shiny-99-modules und kpiR → DDL → dbconnectorR → base-62 samt One-off.

## Betroffen

| Repo | Datei / Tabelle | Änderung |
|---|---|---|
| dbconnectorR | `R/msgraph_scoped_calls.R` | Ausgang je Link, Discovery ohne `is_deleted`-Filter, Auflösung per E-Mail, Schreiben der Spalten, Fehlerquote |
| dbconnectorR | `R/msgraph_extern_event_classification.R` | `compute_observability_exclusions()` und Ergebnisblock kennen den neuen Grund |
| dbconnectorR | `R/sales_meetings_unified.R` | CRM-Beleg hebt den neuen Grund auf |
| dbconnectorR | `inst/sql/2026-10-07-msgraph-events-online-meeting-lookup.sql` | DDL der drei Spalten, Kommentar `exclusion_reason` |
| DB | `raw.msgraph_events`, `processed.msgraph_extern_event_classification`, `processed.sales_meetings_unified` | drei neue Spalten bzw. ein neuer Wert in `exclusion_reason` |
| package-02-kpiR | `R/support_termine_helpers.R`, `R/sdr_termine.R`, `CONTEXT.md` | Grund als nicht messbar und als Anker |
| shiny-99-modules | `func/module_sales_kpi/external_events_helpers.R` | Grund in der Beobachtbarkeitsliste |
| base-62-msgraph-scoped | `one-off/2026-10-07_backfill_online_meeting_lookup.R`, Probe | Backfill ab 19.08. |

## Out-of-Scope

- Organisatoren mit `policy_403` in die `CsApplicationAccessPolicy` aufnehmen. Das ist IT-Arbeit
  (Louis). Die Liste kommt nach dem ersten Lauf aus der neuen Spalte.
- `abruf_fehler` als nicht bewertbar werten: erst, wenn die Statusverteilung Fälle zeigt.
- Meetings mit einzeln abgeschaltetem Anwesenheitsbericht erkennen: Graph führt dafür kein Feld
  (v1.0 und beta geprüft 07.10.2026). Bleibt als Risiko, die Rate ist für sie zu hoch.
- `is_deleted` in `raw.msgraph_users` wieder pflegen (der Directory-Sync von base-35 läuft nicht mehr).

## Validierung

- Nach dem Backfill trägt jedes Event ab 19.08. mit Link des neuen Tenants einen Ausgang
  (`online_meeting_lookup IS NOT NULL`), Verteilung je Wert dokumentiert.
- Kein Event mit Call trägt `online_meeting_nicht_abrufbar`.
- Kein Event mit `gefunden_ohne_bericht` trägt einen Ausschlussgrund, es bleibt No-Show.
- Die Zahl der Termine mit `online_meeting_nicht_abrufbar` passt zur Zahl der Links mit
  `nicht_gefunden`, `policy_403` oder `organisator_unbekannt` ohne Call.
- "Termine gelegt" im Sales-KPI-Dashboard verliert durch den neuen Grund keine Termine.
