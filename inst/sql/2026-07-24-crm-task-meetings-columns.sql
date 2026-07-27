-- Migration: CRM-Task-Meetings-Spalten auf der kanonischen Klassifikations-Tabelle.
-- Idempotent (IF NOT EXISTS). Vor dem ersten Lauf von update_crm_task_meeting_classification ausfuehren.
ALTER TABLE processed.msgraph_extern_event_classification
  ALTER COLUMN call_event_mapping_id DROP NOT NULL,
  ADD COLUMN IF NOT EXISTS source           text NOT NULL DEFAULT 'msgraph',
  ADD COLUMN IF NOT EXISTS meeting_tool     text,
  ADD COLUMN IF NOT EXISTS meeting_status   text,
  ADD COLUMN IF NOT EXISTS is_external_tool boolean,
  ADD COLUMN IF NOT EXISTS crm_event_date   date,
  ADD COLUMN IF NOT EXISTS crm_task_id      bigint;

-- Eindeutigkeit der CRM-Zeilen (partial unique) fuer den scoped Upsert.
CREATE UNIQUE INDEX IF NOT EXISTS uq_extern_event_classification_crm_task
  ON processed.msgraph_extern_event_classification (crm_task_id, contact_id)
  WHERE source = 'crm_task';

-- Schneller Filter fuer den scoped DELETE.
CREATE INDEX IF NOT EXISTS idx_extern_event_classification_source
  ON processed.msgraph_extern_event_classification (source);
