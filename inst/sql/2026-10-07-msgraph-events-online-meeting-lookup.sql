-- Migration: raw.msgraph_events.online_meeting_lookup(_http_status, _at)
-- VOR der Installation des Pakets in DBeaver gegen Production ausfuehren.
-- Ein einziger, idempotenter DO-Block: vorhandene Spalten bleiben unberuehrt,
-- die Kommentare werden (gleichlautend) neu gesetzt.
--
-- Der gescopte Calls-Job (base-62, msgraph_scoped_update_calls_attendance)
-- speichert je Teams-Link den Ausgang der Online-Meeting-Suche auf allen Events
-- mit dieser join_url. Die Klassifikation leitet daraus den Ausschlussgrund
-- online_meeting_nicht_abrufbar ab. Bisher blieb ein nicht abrufbares Meeting
-- ohne Call und zaehlte als No-Show.
-- Asana: https://app.asana.com/0/0/1218828831136393/f
-- Spec: docs/specs/2026-10-07-online-meeting-nicht-abrufbar.md

DO $$
BEGIN
  ALTER TABLE raw.msgraph_events ADD COLUMN IF NOT EXISTS online_meeting_lookup text;
  ALTER TABLE raw.msgraph_events ADD COLUMN IF NOT EXISTS online_meeting_lookup_http_status integer;
  ALTER TABLE raw.msgraph_events ADD COLUMN IF NOT EXISTS online_meeting_lookup_at timestamp;

  COMMENT ON COLUMN raw.msgraph_events.online_meeting_lookup IS
    'Ausgang der Online-Meeting-Suche des Calls-Jobs (base-62) fuer die join_url, gleich auf allen Events mit diesem Link. gefunden_mit_bericht: Meeting gefunden, mindestens eine Session mit Teilnehmenden. gefunden_ohne_bericht: Meeting gefunden, kein Anwesenheitsbericht mit Teilnehmenden (No-Show). nicht_gefunden: Suche ueber JoinWebUrl lieferte keinen Treffer. policy_403: die CsApplicationAccessPolicy deckt den Organisator nicht. organisator_unbekannt: Konto des Organisators bei Microsoft nicht (mehr) vorhanden (404 oder per E-Mail nicht aufloesbar). abruf_fehler: anderer HTTP-Status oder Ausnahme bei Suche oder Berichtsabruf, Status in online_meeting_lookup_http_status. Ein gefunden_-Wert wird nie durch einen schlechteren ersetzt. NULL: Link nie abgefragt (etwa ausserhalb des Fensters, Alt-Tenant oder Organisator nicht intern).';
  COMMENT ON COLUMN raw.msgraph_events.online_meeting_lookup_http_status IS
    'HTTP-Status des gescheiterten Graph-Abrufs, nur bei online_meeting_lookup = abruf_fehler. NULL bei jedem anderen Ausgang und bei einer Ausnahme ohne Antwort.';
  COMMENT ON COLUMN raw.msgraph_events.online_meeting_lookup_at IS
    'Zeitpunkt (UTC), seit dem der aktuelle online_meeting_lookup gilt. Aendert sich nur, wenn sich der Ausgang aendert, nicht bei jedem Lauf.';
END $$;

-- Ausschlussgrund online_meeting_nicht_abrufbar im Spaltenkommentar nachziehen
-- (Basis: 2026-09-05-exclusion-reason-ohne-crm-unbekannt.sql). Reiner COMMENT.
COMMENT ON COLUMN processed.sales_meetings_unified.exclusion_reason IS
  'Warum excluded gesetzt ist. Aus processed.msgraph_extern_event_classification durchgereicht: rescheduled_without_meeting_id, verschoben, zu_viele_interne, duplikat_event (die Zeile ist kein eigener gelegter Termin) sowie termin_in_zukunft, alt_tenant_join_url und online_meeting_nicht_abrufbar (der Termin existiert, nur seine Anwesenheit ist nicht messbar: fuer eine No-Show-Quote gehoeren sie aus Zaehler und Nenner heraus, fuer gelegte Termine und als Anker einer SDR-Zurechnung nicht). online_meeting_nicht_abrufbar heisst: Teams-Meeting bei Microsoft nicht abrufbar, also nicht gefunden, 403 der Zugriffsregel oder Organisator unbekannt. Bei CRM-Zeilen nur crm_storniert: rechtzeitig abgesagt, also kein No-Show, aber auch kein stattgefundener Termin. Ein CRM-Termin ohne dokumentierten Ausgang traegt hier seit 05.09.2026 NULL und zaehlt als stattgefunden; dass nichts dokumentiert ist, steht in meeting_status.';

-- Gegenprobe: muss genau drei Zeilen liefern.
SELECT table_schema, table_name, column_name, data_type, is_nullable
  FROM information_schema.columns
 WHERE table_schema = 'raw' AND table_name = 'msgraph_events'
   AND column_name IN ('online_meeting_lookup', 'online_meeting_lookup_http_status',
                       'online_meeting_lookup_at')
 ORDER BY column_name;
