# Design: processed.sales_meetings_unified (Phase 4 — CRM-Meetings in die No-Show-Auswertung vereinheitlichen)

Datum: 2026-08-11
Autor: Moritz Hemmann
Ticket-Kontext: Folgestufe zu [call-monitoring] VC-Tool-Strings (Asana 1216701790869556)

## Kontext & Ziel

Phase 1-3 (deployed) schreiben CRM-Task-VC-Termine als `source='crm_task'`-Zeilen in
`processed.msgraph_extern_event_classification` und zeigen sie in einem **separaten** Tab.
Ein **Guard** (`filter(!is.na(event_date))`) hält sie aus den bestehenden Auswertungen raus.

**Ziel Phase 4:** Die CRM-Meetings **wie echte Meetings behandeln** — Lücken füllen (MSGraph-unsichtbare
externe Termine) **und** bestehende MSGraph-Termine mit dem manuell gepflegten No-Show-Status **anreichern**.
Ergebnis ist eine **feste, abgeleitete Tabelle** `processed.sales_meetings_unified`, in der alle Meetings
(MSGraph + CRM) mit einem **finalen** No-Show-Status an einer Stelle liegen.

## Getroffene Entscheidungen (Brainstorming 2026-08-11)

1. **Anreichern + Lücken füllen** (nicht nur Netto-Add): Ein CRM-Termin, der zu einem bestehenden
   MSGraph-Termin gehört, wird gematcht und ergänzt ihn; ein CRM-Termin ohne MSGraph-Gegenstück kommt
   als neuer Termin dazu.
2. **CRM gewinnt beim No-Show:** Bei Widerspruch überschreibt der manuell gepflegte
   No-Show/Show-Up-Status (`meeting_status` aus dem CRM-Kommentar) die MSGraph-Ableitung.
3. **Voll rückwirkend:** Die Vereinheitlichung wird über die gesamte Historie gerechnet (kompletter
   Rebuild pro Lauf). Bereits berichtete No-Show-Raten verschieben sich entsprechend — bewusst gewollt.
4. **Scope = No-Show-Ecke (+ Tool):** Nur die No-Show-Auswertung bezieht die vereinheitlichten Meetings ein.
   Terminierung, SDR-Monitoring und Monatstabelle bleiben unverändert.
5. **Feste Tabelle statt View:** Bewusste Entscheidung für eine materialisierte
   `processed.sales_meetings_unified` (nicht View, nicht read-time-R-Merge) — eine abfragbare, stabile
   Quelle, die auch andere Consumer/Analysen nutzen können.

## Nicht-Ziele

- **Keine Mutation der MSGraph-Rohzeilen.** `processed.msgraph_extern_event_classification` bleibt
  unangetastet (MSGraph-Wahrheit + CRM-Zeilen nebeneinander). `sales_meetings_unified` ist ein
  **abgeleitetes** Produkt daneben.
- Keine Änderung an Terminierung/SDR/Monatstabelle (lesen weiter den bestehenden Guard-Pool).
- Kein inkrementeller Merge — voller Rebuild ist ausreichend (Datenmenge klein).

## Architektur-Überblick

```
processed.msgraph_extern_event_classification (source='msgraph')  ─┐  (MSGraph-Meetings + is_no_show)
raw.crm_lead_tasks / _comments  ─(frisch geparst, OHNE Anti-Join)─┤  (alle CRM-VC-Termine)
                                                                   ▼
                                     update_sales_meetings_unified(con)   [NEU, Rebuild]
                                     - Match CRM↔MSGraph (lead_id + Tag [+ Rep])
                                     - nicht-extern eindeutig → Override is_no_show
                                     - nicht-extern mehrdeutig → verwerfen
                                     - extern / kein Match → Netto-neu
                                                                   ▼
                                     processed.sales_meetings_unified   [NEU]
                                                                        │
                                              ┌─────────────────────────┴───────────────┐
                                     No-Show-Modul (shiny-99)                  bestehender VC-Tool-Tab
                                     liest daraus (NEU)                        (optional Umstellung)

Terminierung / SDR / Monatstabelle ── lesen weiter get_responsible_event_pool (Guard) ── UNVERÄNDERT
```

## 1. Tabelle `processed.sales_meetings_unified`

Grain: **eine Zeile pro Meeting** (MSGraph-Termin oder CRM-Termin). Erstellung über `/create-db-schema`
(Haus-Standard: `id bigint GENERATED ALWAYS AS IDENTITY`, `created_at`/`updated_at`, `trigger_set_updated_at`,
`pk_`/`uq_`/`idx_`-Namen).

Fachliche Spalten:

| Spalte | Typ | Bedeutung |
|---|---|---|
| `meeting_key` | text | Stabile Identität/Dedup-Schlüssel: `call_event_mapping_id::text` für MSGraph-Termine, `'crm_' \|\| crm_task_id` für Netto-neue CRM-Termine. **UNIQUE.** |
| `source` | text | `msgraph` oder `crm_task` (Herkunft der Basiszeile). |
| `event_date` | date | `coalesce(mapping.event_date, crm_event_date)`. |
| `contact_id` | bigint | Verantwortlicher Rep-Kontakt (für Rep-/Team-Aggregation). |
| `lead_id` | integer | Zugehöriger Lead (für Match + Analysen). |
| `is_no_show` | boolean | **Finaler** No-Show-Status nach Override-Regel. |
| `no_show_source` | text | Woher der finale Status kommt: `msgraph`, `crm_override`, `crm_only`. (Transparenz/Debugging.) |
| `meeting_status` | text | CRM-Status, falls vorhanden (`no_show`/`show_up`/`storniert`/`unbekannt`), sonst NULL. |
| `meeting_tool` | text | VC-Tool (webex/zoom/teams/…/unbekannt), falls bekannt. |
| `is_external_tool` | boolean | Externes (MSGraph-unsichtbares) Tool. |
| `excluded` | boolean | Ausschluss-Flag (aus Klassifikation übernommen). |
| `is_short_lived_event` | boolean | Kurzlebig-Flag (aus Klassifikation). |
| `is_responsible` | boolean | Verantwortlichkeit (aus Klassifikation). |

Das No-Show-Modul wendet seine bestehenden Filter (`is_responsible`, `!excluded`, `!is_short_lived_event`)
auf diese Tabelle an — die Tabelle ist ein **faithful Superset**, kein vorgefiltertes Aggregat.

## 2. Producer `update_sales_meetings_unified(con)`

Neue Funktion in `dbconnectorR`. Voller **Rebuild** pro Lauf (kein Delta). Reine Logik so weit wie möglich
in eine testbare Hilfsfunktion `assemble_unified_meetings()` extrahiert (kein DB-Zugriff), DB-I/O in der
Wrapper-Funktion (`tbl(con, I(...))` + `postgres_upsert_data(..., match_cols=c("meeting_key"), delete_missing=TRUE)`).

**WICHTIG — Datenquelle der CRM-Meetings:** NICHT die `source='crm_task'`-Zeilen aus der Klassifikation.
Die sind bereits **anti-gejoint** (Phase 1-3 verwirft Teams-/unbekannt-Termine, die MSGraph schon kennt) —
und genau diese verworfenen Termine sind die **Override-Kandidaten** fürs Anreichern. Der Phase-4-Producer
leitet die CRM-Meetings deshalb **frisch aus den Rohdaten** ab (Wiederverwendung der reinen Parser aus PR #22:
`is_vc_task`, `extract_meeting_tool`, `classify_meeting_status`, plus `resolve_crm_user_contact`) — ohne den
Anti-Join. Der Anti-Join wird in Phase 4 durch die Match-Entscheidung ersetzt (Match → anreichern statt
verwerfen; kein Match → als Netto-neu aufnehmen).

Schritte:

1. **MSGraph-Meetings** laden: `source='msgraph'`-Zeilen aus der Klassifikation + `event_date` via
   `mapping.msgraph_call_event`. Tragen `is_no_show` (MSGraph-Ableitung), `contact_id`, `lead_id` (via
   `mapping.crm_lead_msgraph_contact`, `is_primary_crm = TRUE`).
2. **CRM-Meetings** frisch aus `raw.crm_lead_tasks` + `raw.crm_lead_task_comments` ableiten (Parser aus §
   „Datenquelle" oben): pro VC-Task → `lead_id`, `event_date` (aus `precise_time`), Rep-`contact_id`,
   `meeting_tool`, `meeting_status`. **Alle** VC-Termine, auch die, die MSGraph schon kennt.
3. **Match** CRM↔MSGraph auf `lead_id` + `event_date` (Tag; optional zusätzlich Rep `contact_id`).
4. **Override / Merge** — Verzweigung nach Tool-Typ (siehe §3):
   - **Externes Tool (webex/zoom/skype/google_meet):** **immer Netto-neu** (`no_show_source='crm_only'`,
     `meeting_key='crm_'||crm_task_id`). Nie Override — MSGraph sieht externe Termine nicht, ein
     gleicher-Tag-MSGraph-Termin ist ein *anderer* Termin.
   - **Nicht-extern (teams/unbekannt):**
     - **eindeutiger** MSGraph-Match → **Override** der MSGraph-Zeile: `is_no_show` = CRM-Wert (falls CRM
       definitiven Status hat), `no_show_source='crm_override'`, `meeting_tool`/`meeting_status` ergänzen.
     - **mehrdeutiger** Match → **verwerfen** (kein Override, NICHT als Netto-neu) — MSGraph hat den Termin
       sehr wahrscheinlich schon; das vermeidet Doppelzählung.
     - **kein** Match → Netto-neu (`no_show_source='crm_only'`).
   - MSGraph ohne CRM-Match → unverändert (`no_show_source='msgraph'`).
5. Ergebnis nach `processed.sales_meetings_unified` schreiben (Rebuild via `delete_missing=TRUE` auf
   `meeting_key`).

## 3. Match-Sicherung (zentral, wegen rückwirkend + Override)

Ein Fehlmatch würde rückwirkend einen falschen Termin überschreiben. Daher **konservative Regeln**:

- **Externe Tools überschreiben nie** — sie sind für MSGraph unsichtbar, ein Match auf `lead_id`+Tag wäre
  ein Zufallstreffer auf einen *anderen* Termin. Externe CRM-Termine sind immer Netto-neu.
- Override (nur nicht-extern) greift **nur bei eindeutigem Match** — genau *eine* MSGraph-Zeile für
  (`lead_id`, `event_date` [, Rep]). Mehrere Kandidaten → **verwerfen** (kein Override, kein Netto-neu),
  weil MSGraph den Termin dann sehr wahrscheinlich schon führt (Doppelzählung vermeiden).
- Bei mehreren CRM-Status auf dasselbe Meeting gilt die Parser-Priorität (`storniert` > `no_show` >
  `show_up`).
- `no_show_source` (msgraph / crm_override / crm_only) macht jede Zeile nachvollziehbar.

Leitsatz: **Lieber eine Lücke nicht füllen als eine falsche Zahl schreiben.**

## 4. Pipeline-Einbindung

`update_sales_meetings_unified(con)` läuft in `base-35/do/main.R` **nach** dem CRM-Producer
(`update_crm_task_meeting_classification`) — es braucht dessen Zeilen als Input. `needed_tables` um
`processed.sales_meetings_unified` erweitern. Ein neuer `run_data_job`-Block, analog zu den bestehenden.

## 5. Konsum

- **No-Show-Modul (shiny-99, `module_kpi_no_show`)** liest künftig aus `processed.sales_meetings_unified`
  statt aus `get_responsible_event_pool`. Damit greifen Override + Lückenfüllung genau dort.
- **Terminierung / SDR / Monatstabelle:** unverändert am alten Pool → Scope „nur No-Show" gewahrt. Der
  Guard in `get_responsible_event_pool` bleibt bestehen (schützt diese Tabs weiter).
- **Bestehender „VC-Tool & Show-Up"-Tab:** kann auf `sales_meetings_unified` umgestellt werden (dann eine
  gemeinsame Quelle) oder bleibt wie er ist. **Offener Detailpunkt für den Plan** — Default-Empfehlung:
  umstellen, damit es eine einzige Quelle gibt.

## 6. Tests

- **Unit-Tests** auf `assemble_unified_meetings()` (rein, Fixtures):
  - nicht-extern, eindeutiger Match + CRM-No-Show → MSGraph-Zeile `is_no_show=TRUE`,
    `no_show_source='crm_override'`.
  - nicht-extern, CRM `show_up` widerspricht MSGraph `is_no_show=TRUE` → `FALSE` (CRM gewinnt).
  - nicht-extern, mehrdeutiger Match → CRM-Zeile **verworfen** (weder Override noch Netto-neu).
  - nicht-extern, kein Match → CRM als Netto-neu (`crm_only`).
  - **externes Tool** mit gleicher-Tag-MSGraph-Zeile → CRM **immer Netto-neu**, MSGraph-Zeile unverändert
    (kein Override).
  - Dedup: `meeting_key` eindeutig, keine Doppelzählung.
- **Integrations-Check** auf `studyflix_local`: Vorher/Nachher-Zeilenzahl + wie viele MSGraph-Zeilen durch
  Override kippen (Größenordnung des Effekts sichtbar machen).

## 7. Reversibilität

- Rein additiv auf DB-Ebene: neue Tabelle, kein Überschreiben bestehender Daten.
- Zurückdrehen: `DROP TABLE processed.sales_meetings_unified;` + No-Show-Modul zurück auf den Pool +
  den `run_data_job`-Block entfernen. Keine Datenverluste an den Quellen.

## Offene Detailpunkte (für den Implementierungsplan)

- Match-Schlüssel final: `lead_id + Tag` vs. zusätzlich Rep — im Plan an echten Daten kalibrieren
  (Rate eindeutiger vs. mehrdeutiger Matches auf `studyflix_local` messen).
- VC-Tool-Tab: umstellen auf `sales_meetings_unified` (empfohlen) oder belassen.
- Genaue Spaltenliste final beim `/create-db-schema`-Schritt.
