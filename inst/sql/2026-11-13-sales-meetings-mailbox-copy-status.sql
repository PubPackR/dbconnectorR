-- processed.sales_meetings.mailbox_copy_status: Kopien aus dem Postfach-Umzug als Signal.
-- VOR der Installation der dbconnectorR-Version mit dieser Spalte in DBeaver gegen
-- Production ausfuehren. Ohne die Spalte bricht update_sales_meetings() ab
-- (pruefe_zielspalten), der Nachtlauf von msgraph-ingestion wird rot.
-- Asana: https://app.asana.com/0/0/1220345738207738/f
-- Spec: docs/specs/2026-11-13-mailbox-copy-status.md (Q4, Q14, Q16, Q17)

ALTER TABLE processed.sales_meetings ADD COLUMN IF NOT EXISTS mailbox_copy_status text;

COMMENT ON COLUMN processed.sales_meetings.mailbox_copy_status IS
  'Whether this calendar row is suspected to be a copy the mailbox move re-created: created 2026-08-01 to 2026-10-31 and part of a move session or created after the meeting day. has_earlier_original: an earlier row of the same start shares an external participant; booked_date carries its creation date. evidenced: no such row, but a CRM task assigned to this row was created before it; booked_date is the day of the earliest such task, unless the row''s iCal UID is older. suspected: neither; booked_date is the copy day. NULL: not suspected, a Bookings entry or a CRM-only row.';

COMMENT ON COLUMN processed.sales_meetings.booked_date_source IS
  'Where booked_date comes from: outlook, bookings_start (Bookings entry, which carries no real creation time), crm_task (CRM-only row, or an evidenced mailbox-move copy: day of the earliest CRM task created before the copy; a replacement row inherits it together with the date).';
