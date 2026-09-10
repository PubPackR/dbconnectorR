-- processed.sales_meetings_unified: alle Meetings (MSGraph + CRM) mit finalem No-Show-Status.
-- Abgeleitete Tabelle, voller Rebuild durch dbconnectorR::update_sales_meetings_unified().
CREATE TABLE IF NOT EXISTS processed.sales_meetings_unified (
    id                   bigint NOT NULL GENERATED ALWAYS AS IDENTITY ( INCREMENT 1 START 1 MINVALUE 1 MAXVALUE 9223372036854775807 CACHE 1 ),
    created_at           timestamp without time zone NOT NULL DEFAULT now(),
    updated_at           timestamp without time zone NOT NULL DEFAULT now(),

    meeting_key          text        NOT NULL,
    source               text        NOT NULL,
    event_date           date,
    contact_id           bigint,
    lead_id              bigint,
    is_no_show           boolean,
    no_show_source       text,
    meeting_status       text,
    meeting_tool         text,
    meeting_type         text,
    is_external_tool     boolean,
    excluded             boolean     NOT NULL DEFAULT false,
    is_short_lived_event boolean     NOT NULL DEFAULT false,
    is_responsible       boolean     NOT NULL DEFAULT true,
    original_created_at  timestamp without time zone,
    event_id             text,

    CONSTRAINT pk_sales_meetings_unified PRIMARY KEY (id),
    CONSTRAINT fk_sales_meetings_unified_msgraph_contacts FOREIGN KEY (contact_id)
        REFERENCES raw.msgraph_contacts (id) ON DELETE CASCADE,
    CONSTRAINT fk_sales_meetings_unified_crm_leads FOREIGN KEY (lead_id)
        REFERENCES raw.crm_leads (id) ON DELETE CASCADE,
    CONSTRAINT uq_sales_meetings_unified_meeting_key UNIQUE (meeting_key)
);

CREATE INDEX IF NOT EXISTS idx_sales_meetings_unified_contact_id ON processed.sales_meetings_unified (contact_id);
CREATE INDEX IF NOT EXISTS idx_sales_meetings_unified_lead_id    ON processed.sales_meetings_unified (lead_id);
CREATE INDEX IF NOT EXISTS idx_sales_meetings_unified_event_date ON processed.sales_meetings_unified (event_date);

COMMENT ON TABLE processed.sales_meetings_unified IS 'Unified sales meetings (MSGraph + CRM-task-derived), one row per meeting, carrying the final no-show status after CRM override. Derived table, fully rebuilt by dbconnectorR::update_sales_meetings_unified(). Source of the No-Show analysis.';
COMMENT ON COLUMN processed.sales_meetings_unified.meeting_key IS 'Stable per-(meeting x external lead) identity / dedup key: ''msgraph_<call_event_mapping_id>_<lead_id>'' (lead_id NULL = external-but-unmapped placeholder) for MSGraph meetings, ''crm_<crm_task_id>'' for CRM-only meetings.';
COMMENT ON COLUMN processed.sales_meetings_unified.source IS 'Origin of the base row: ''msgraph'' or ''crm_task''.';
COMMENT ON COLUMN processed.sales_meetings_unified.event_date IS 'Date the meeting takes place; coalesce of the MSGraph mapping event_date and the CRM task precise_time.';
COMMENT ON COLUMN processed.sales_meetings_unified.contact_id IS 'Responsible sales-rep contact (references raw.msgraph_contacts).';
COMMENT ON COLUMN processed.sales_meetings_unified.lead_id IS 'External CRM lead the meeting belongs to (surrogate raw.crm_leads.id); NULL = meeting had an external participant but none mapped to a lead (placeholder row, counts in the overall rate, not in per-lead breakdowns).';
COMMENT ON COLUMN processed.sales_meetings_unified.is_no_show IS 'Final no-show flag after CRM override; NULL when the outcome is unknown.';
COMMENT ON COLUMN processed.sales_meetings_unified.no_show_source IS 'Provenance of the final no-show value: ''msgraph'', ''crm_override'', or ''crm_only''.';
COMMENT ON COLUMN processed.sales_meetings_unified.meeting_status IS 'CRM-comment-derived status (no_show/show_up/storniert/unbekannt), where available.';
COMMENT ON COLUMN processed.sales_meetings_unified.meeting_tool IS 'Video-call tool parsed from the CRM task name (webex/zoom/teams/...), where known.';
COMMENT ON COLUMN processed.sales_meetings_unified.meeting_type IS 'Termin-Typ aus dem CRM-Task-Namen (nv/uc/fu/rep/er/zr/planung/unbekannt). Die Abkuerzung des Vertriebs bleibt der Token; belegt sind uc = Updatecall, fu = Follow-up, rep = Reporting, unbestaetigt sind nv, er, zr.';
COMMENT ON COLUMN processed.sales_meetings_unified.is_external_tool IS 'TRUE if the tool is an external (MSGraph-invisible) VC tool.';
COMMENT ON COLUMN processed.sales_meetings_unified.excluded IS 'Excluded from the no-show analysis (cancelled / unknown-outcome CRM meetings, or MSGraph exclusions).';
COMMENT ON COLUMN processed.sales_meetings_unified.is_short_lived_event IS 'MSGraph flag: event cancelled less than 24h after creation.';
COMMENT ON COLUMN processed.sales_meetings_unified.is_responsible IS 'Row represents a responsible-contact assignment for the meeting.';
COMMENT ON COLUMN processed.sales_meetings_unified.original_created_at IS 'Original creation timestamp of the meeting/task (for lead-time analysis).';
COMMENT ON COLUMN processed.sales_meetings_unified.event_id IS 'MSGraph event id (NULL for CRM-only meetings).';

CREATE OR REPLACE TRIGGER trigger_set_updated_at
    BEFORE UPDATE
    ON processed.sales_meetings_unified
    FOR EACH ROW
    EXECUTE FUNCTION public.update_timestamp();
