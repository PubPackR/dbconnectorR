-- raw.msgraph_events.vanished_at: wann eine Zeile aus einem geladenen Kalender verschwunden ist.
-- VOR dem Backfill und VOR der Installation des Pakets in DBeaver gegen Production ausfuehren.
-- Ohne die Spalte schlaegt das UPDATE in mark_vanished_events_canceled() fehl.
-- Das Test-Schema gehoert dazu: base-62 nennt den Rollback auf *_scoped_test als Betriebsart.
-- Asana: https://app.asana.com/0/0/1220117253528426/f
-- Spec: docs/specs/2026-11-09-vanished-at-und-kalenderzustand.md

ALTER TABLE raw.msgraph_events ADD COLUMN IF NOT EXISTS vanished_at timestamp without time zone;
ALTER TABLE raw_scoped_test.msgraph_events ADD COLUMN IF NOT EXISTS vanished_at timestamp without time zone;

COMMENT ON COLUMN raw.msgraph_events.vanished_at IS
  'When the nightly ingest found this row missing from a calendar it had loaded (UTC, accurate to the nightly run), set together with is_canceled and cleared when the row is downloaded again. NULL on a cancelled row means Outlook itself reported the cancellation. Rows with event_start before May 2025 and bookings cancelled before the rollout were not backfilled; backfilled values are approximations, see inst/sql/2026-11-09-msgraph-events-vanished-at-backfill.sql.';
COMMENT ON COLUMN raw_scoped_test.msgraph_events.vanished_at IS
  'Test schema copy of raw.msgraph_events.vanished_at.';
