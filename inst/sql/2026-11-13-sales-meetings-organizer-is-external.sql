-- processed.sales_meetings.organizer_is_external: ob ein Kunde den Termin organisiert.
-- VOR der Installation der dbconnectorR-Version mit dieser Spalte in DBeaver gegen
-- Production ausfuehren. Ohne die Spalte verwirft der Upsert in update_sales_meetings()
-- sie still, und der Lauf bleibt gruen.
-- Asana: https://app.asana.com/0/0/1220115896589056/f
-- Spec: docs/specs/2026-11-11-sales-meetings.md (Spalten); Entscheidung Q14 in der Spec
-- 2026-11-12-referenz-termin-bewertung.md

ALTER TABLE processed.sales_meetings ADD COLUMN IF NOT EXISTS organizer_is_external boolean;

COMMENT ON COLUMN processed.sales_meetings.organizer_is_external IS
  'TRUE when the organizer of the calendar row has an external address: a customer invites to a meeting in their own Teams tenant, whose attendance report never reaches us. FALSE when an internal person organizes (an internal organizer wins over an external one). NULL for CRM-only rows and calendar rows without an organizer.';
