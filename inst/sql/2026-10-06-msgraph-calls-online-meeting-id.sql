-- Migration: raw.msgraph_calls.msgraph_online_meeting_id
-- VOR der Installation des Pakets in DBeaver gegen Production ausfuehren.
-- Ein einziger, idempotenter DO-Block: ist die Spalte schon da, tut er nichts.
--
-- Seit dbconnectorR 0.0.0.9033 schreibt der gescopte Ingest (base-62) einen Call
-- je Anwesenheitsbericht. msgraph_call_id ist dort die Bericht-ID; die
-- onlineMeeting-id, die der Transkript-Job fuer die Graph-URL braucht, steht in
-- dieser neuen Spalte. Calls aus base-35 (callRecords) bleiben NULL.
-- Asana: https://app.asana.com/0/0/1218828738255412/f
-- ADR: docs/adr/0002-call-row-per-attendance-report.md

DO $$
BEGIN
  IF EXISTS (
    SELECT 1 FROM information_schema.columns
     WHERE table_schema = 'raw' AND table_name = 'msgraph_calls'
       AND column_name = 'msgraph_online_meeting_id'
  ) THEN
    RAISE NOTICE 'raw.msgraph_calls.msgraph_online_meeting_id existiert schon, nichts zu tun.';
    RETURN;
  END IF;

  ALTER TABLE raw.msgraph_calls ADD COLUMN msgraph_online_meeting_id text;
  COMMENT ON COLUMN raw.msgraph_calls.msgraph_online_meeting_id IS
    'onlineMeeting-id aus Graph (gescopter Ingest, base-62). Mehrere Calls (Sessions eines wiederverwendeten Teams-Links) teilen sie; msgraph_call_id ist dort die ID des Anwesenheitsberichts. NULL bei Calls aus base-35 (callRecords).';
END $$;

-- Gegenprobe: muss genau eine Zeile liefern.
SELECT table_schema, table_name, column_name, data_type, is_nullable
  FROM information_schema.columns
 WHERE table_schema = 'raw' AND table_name = 'msgraph_calls'
   AND column_name = 'msgraph_online_meeting_id';
