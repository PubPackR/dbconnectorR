-- processed.sales_meetings_unified: alle Meetings (MSGraph + CRM) mit finalem No-Show-Status.
-- Abgeleitete Tabelle, voller Rebuild durch dbconnectorR::update_sales_meetings_unified().
CREATE TABLE IF NOT EXISTS processed.sales_meetings_unified (
  id                   bigint GENERATED ALWAYS AS IDENTITY,
  meeting_key          text        NOT NULL,
  source               text        NOT NULL,
  event_date           date,
  contact_id           bigint,
  lead_id              integer,
  is_no_show           boolean,
  no_show_source       text,
  meeting_status       text,
  meeting_tool         text,
  is_external_tool     boolean,
  excluded             boolean     NOT NULL DEFAULT false,
  is_short_lived_event boolean     NOT NULL DEFAULT false,
  is_responsible       boolean     NOT NULL DEFAULT true,
  original_created_at  timestamp,
  event_id             text,
  created_at           timestamp   NOT NULL DEFAULT now(),
  updated_at           timestamp   NOT NULL DEFAULT now(),
  CONSTRAINT pk_sales_meetings_unified PRIMARY KEY (id),
  CONSTRAINT uq_sales_meetings_unified_meeting_key UNIQUE (meeting_key)
);

CREATE INDEX IF NOT EXISTS idx_sales_meetings_unified_event_date ON processed.sales_meetings_unified (event_date);
CREATE INDEX IF NOT EXISTS idx_sales_meetings_unified_contact_id ON processed.sales_meetings_unified (contact_id);

CREATE TRIGGER trigger_set_updated_at
  BEFORE UPDATE ON processed.sales_meetings_unified
  FOR EACH ROW EXECUTE FUNCTION trigger_set_updated_at();
