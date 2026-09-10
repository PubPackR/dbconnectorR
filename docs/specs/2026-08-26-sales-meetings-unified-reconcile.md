# sales_meetings_unified – Reconcile an das re-derivte Design

Asana: https://app.asana.com/1/734700742714256/project/1211291490559148/task/1217870726538111
Ursprung (Phasen 1–3, geschlossen): Asana 1216701790869556

## Problem
Der bestehende Producer `update_sales_meetings_unified` (Branch `feat/sales-meetings-unified`)
führt MSGraph- und CRM-Task-Meetings bereits zusammen, weicht aber vom in der Grill-Session
re-derivten Design ab: interne Teilnehmer zählen als Lead, kein `is_primary_crm`, Grain = ein
Event-Row mit `min(lead_id)`, kein NULL-Platzhalter, kein Rep-Zeit-Tiebreak.

## Lösung
`assemble_unified_meetings` + `update_sales_meetings_unified` + DDL ans Design anpassen.
Die Struktur bleibt; nur Lead-Ableitung, Grain, Match und `meeting_key` ändern sich.

## Entscheidungen
- Scope: NUR Producer (`packages/dbconnectorR`). Dashboard-Tab = eigenes Folge-Ticket.
- Reconcile in-place auf `feat/sales-meetings-unified`, vorher auf `main` rebasen.
- Lead extern-only: `is_internal_email` + `is_synthetic_email` raus, nur `is_primary_crm == TRUE`.
- Grain: eine Zeile pro (Meeting × externer Lead). Gruppen-Meeting = N Zeilen.
  Extern-aber-nicht-gemappt = eine Zeile mit `lead_id = NULL`. Internal-only Meeting = raus.
- Match: `(lead × Berlin-Tag)`-Bucket; Tiebreak = gleicher Rep + nächste `event_start` zur
  `precise_time`; echte Rest-Mehrdeutigkeit (gleicher Rep, gleicher Zeitpunkt) verwerfen.
- CRM gewinnt bei definitivem Status (`no_show`/`show_up`/`storniert`); `unbekannt` lässt den
  MSGraph-Wert unangetastet.
- `meeting_key` um `lead_id` erweitern (`msgraph_<call_event_mapping_id>_<lead_id>` bzw.
  `crm_<crm_task_id>`), `uq_` entsprechend.
- Rate `no_shows/all_scheduled` rechnet das Dashboard; der Producer liefert nur Zeilen + Flags
  (`is_no_show`, `excluded`, `no_show_source`).

## Betroffen
- `packages/dbconnectorR`: `R/sales_meetings_unified.R`, `inst/sql/2026-08-11-sales-meetings-unified.sql`
- `base-35`: `do/main*.R` (`needed_tables`: `raw.msgraph_event_participants`, `raw.msgraph_contacts`, …)
- Tabelle: `processed.sales_meetings_unified` (DDL: `meeting_key`-uq inkl. lead, `lead_id` nullable)

## Out-of-Scope
- Dashboard-Tab (Folge-Ticket).
- Rep-Zeit-Toleranzschwelle (Match ganz verwerfen wenn zu weit) – erst nach Alignment-Messung.
- Deploy: `packages/` → Moritz deployt selbst.

## Validierung
- Producer läuft clean auf `studyflix_local`, Zeilenzahl plausibel.
- Keine internen/synthetischen Leads in der Tabelle (Stichprobe `is_internal_email`).
- Gruppen-Meeting erzeugt N Lead-Zeilen; leadless-extern hat `lead_id = NULL`; internal-only fehlt.
- `meeting_key` eindeutig pro (Event × Lead).
- Override: CRM-`no_show` überschreibt MSGraph nur bei eindeutigem Match.
