-- Migration: Spaltenkommentar exclusion_reason nachziehen.
-- Idempotent, reiner COMMENT, keine Datenaenderung. Zusammen mit der
-- Installation des Pakets ausfuehren.
--
-- crm_status_flags() setzt fuer meeting_status = 'unbekannt' seit 05.09.2026
-- excluded = FALSE (Asana 1218192760112154). Der Wert 'crm_unbekannt' kann in
-- dieser Spalte damit nicht mehr entstehen. Der bisherige Kommentar aus
-- 2026-09-02-sales-meetings-unified-organizer.sql fuehrt ihn weiter auf und
-- erklaert ihn als Beobachtbarkeitsgrund. Wer sich beim Bau einer Auswertung
-- darauf verlaesst, filtert auf einen Wert mit null Zeilen und haelt das
-- Ergebnis fuer einen Befund.
--
-- Nebenbei praezisiert: der alte Text sagte "die letzten beiden MSGraph-Gruende"
-- und meinte damit termin_in_zukunft und alt_tenant_join_url, zaehlte aber
-- unmittelbar davor die CRM-Gruende auf. Die Aufzaehlung ist jetzt getrennt.

COMMENT ON COLUMN processed.sales_meetings_unified.exclusion_reason IS
  'Warum excluded gesetzt ist. Aus processed.msgraph_extern_event_classification durchgereicht: rescheduled_without_meeting_id, verschoben, zu_viele_interne, duplikat_event (die Zeile ist kein eigener gelegter Termin) sowie termin_in_zukunft und alt_tenant_join_url (der Termin existiert, nur seine Anwesenheit ist nicht messbar: fuer eine No-Show-Quote gehoeren sie aus Zaehler und Nenner heraus, fuer gelegte Termine und als Anker einer SDR-Zurechnung nicht). Bei CRM-Zeilen nur crm_storniert: rechtzeitig abgesagt, also kein No-Show, aber auch kein stattgefundener Termin. Ein CRM-Termin ohne dokumentierten Ausgang traegt hier seit 05.09.2026 NULL und zaehlt als stattgefunden; dass nichts dokumentiert ist, steht in meeting_status.';
