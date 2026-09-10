-- Migration: Termin-Typ auf der vereinheitlichten Meeting-Tabelle (T2).
-- Idempotent. Vor dem naechsten Lauf von update_sales_meetings_unified ausfuehren.
-- Der Task-Name nennt fast nie das Tool, aber fast immer den Termin-Typ; diese
-- Spalte haelt ihn fest. Siehe kpiR docs/specs/2026-08-28-t2-vcs-toolunabhaengig.md.
ALTER TABLE processed.sales_meetings_unified
  ADD COLUMN IF NOT EXISTS meeting_type text;

COMMENT ON COLUMN processed.sales_meetings_unified.meeting_type IS 'Termin-Typ aus dem CRM-Task-Namen (nv/uc/fu/rep/er/zr/planung/unbekannt). Die Abkuerzung des Vertriebs bleibt der Token; belegt sind uc = Updatecall, fu = Follow-up, rep = Reporting, unbestaetigt sind nv, er, zr.';
