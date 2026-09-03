-- Migration: Organisator auf der vereinheitlichten Meeting-Tabelle (T4).
-- Idempotent. Vor dem naechsten Lauf von update_sales_meetings_unified ausfuehren,
-- sonst verwirft der Upsert die beiden Spalten still und der Job laeuft gruen durch.
--
-- Die Tabelle fuehrte bisher nur die verantwortliche Person. Wer den Termin
-- ANGELEGT hat, stand allein in processed.msgraph_extern_event_classification
-- (is_organizer). Genau diese Rollenteilung ist die SDR-Definition: SDR = der
-- Organisator, wenn Organisator und Verantwortliche:r verschiedene Personen sind.
-- Die Regel selbst wohnt in package-02-kpiR; hier liegt nur das rohe Merkmal.
-- Siehe kpiR docs/specs/2026-09-02-t4-sdr-kette-termin-angebot-close.md.
--
-- Kein Backfill noetig: update_sales_meetings_unified schreibt die Tabelle bei
-- jedem Lauf vollstaendig neu (delete_missing = TRUE).

ALTER TABLE processed.sales_meetings_unified
  ADD COLUMN IF NOT EXISTS organizer_contact_id bigint;

ALTER TABLE processed.sales_meetings_unified
  ADD COLUMN IF NOT EXISTS organizer_source text;

-- Der Grund des Ausschlusses, bisher nur in der Klassifikationstabelle. Ohne
-- ihn ist `excluded` nicht lesbar: "crm_storniert" ist eine fachliche Aussage,
-- "termin_in_zukunft" und "alt_tenant_join_url" betreffen allein die
-- Beobachtbarkeit. Genau diese Verwechslung hat im August 2026 die
-- Terminierung um bis zu 69 Prozent zu niedrig ausgewiesen.
ALTER TABLE processed.sales_meetings_unified
  ADD COLUMN IF NOT EXISTS exclusion_reason text;

COMMENT ON COLUMN processed.sales_meetings_unified.organizer_contact_id IS
  'Kontakt (raw.msgraph_contacts), der den Termin angelegt hat, aus processed.msgraph_extern_event_classification (is_organizer). NULL, wo kein Organisator bekannt ist; organizer_source sagt warum. Rohes Merkmal ohne Kennzahlenlogik: ob daraus ein SDR wird, entscheidet package-02-kpiR.';

COMMENT ON COLUMN processed.sales_meetings_unified.organizer_source IS
  'Herkunft der Organisator-Angabe, drei Faelle, die als blosses NULL ununterscheidbar waeren. msgraph = Organisator bekannt. unbekannt = Kalendertermin ohne klassifizierte Organisator-Zeile (der Paarungs-Vorbehalt aus T1/C4: "nur Organizer" und "nur Verantwortlicher" sind in keinem Monat gleich). crm_task = netto-neuer CRM-Termin, der grundsaetzlich keinen Organisator tragen kann.';

COMMENT ON COLUMN processed.sales_meetings_unified.exclusion_reason IS
  'Warum excluded gesetzt ist. Aus processed.msgraph_extern_event_classification durchgereicht (rescheduled_without_meeting_id, verschoben, zu_viele_interne, duplikat_event, termin_in_zukunft, alt_tenant_join_url); bei CRM-Zeilen crm_storniert beziehungsweise crm_unbekannt. Die letzten beiden MSGraph-Gruende betreffen nur die Messbarkeit der Anwesenheit, nicht die Existenz des Termins: fuer eine No-Show-Quote gehoeren sie heraus, fuer gelegte Termine und als Anker einer SDR-Zurechnung nicht.';

CREATE INDEX IF NOT EXISTS idx_sales_meetings_unified_organizer_contact_id
  ON processed.sales_meetings_unified (organizer_contact_id);
