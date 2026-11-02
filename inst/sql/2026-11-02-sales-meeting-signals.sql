-- processed.sales_meeting_signals: was CRM, Outlook und Teams je Termin sagen, ohne Bewertung.
-- VOR der Installation des Pakets in DBeaver gegen Production ausfuehren, NACH
-- 2026-11-02-crm-lead-task-msgraph-event.sql.
-- Abgeleitete Tabelle, voller Rebuild durch dbconnectorR::update_sales_meeting_signals().
-- Asana: https://app.asana.com/0/0/1220115877676176/f
-- Spec: docs/specs/2026-11-02-termin-mapping-und-signaltabelle.md, ADR 0003

CREATE TABLE IF NOT EXISTS processed.sales_meeting_signals (
    id                       bigint NOT NULL GENERATED ALWAYS AS IDENTITY ( INCREMENT 1 START 1 MINVALUE 1 MAXVALUE 9223372036854775807 CACHE 1 ),
    created_at               timestamp without time zone NOT NULL DEFAULT now(),
    updated_at               timestamp without time zone NOT NULL DEFAULT now(),

    meeting_key              text    NOT NULL,
    msgraph_event_id         bigint,
    call_event_mapping_id    bigint,
    crm_lead_task_id         bigint,
    lead_id                  bigint,
    lead_source              text,
    contact_id               bigint,
    organizer_contact_id     bigint,
    match_rule               text,

    event_date               date,
    event_start              timestamp without time zone,
    original_created_at      timestamp without time zone,

    has_crm_task             boolean NOT NULL,
    has_msgraph_event        boolean NOT NULL,

    crm_status               text    NOT NULL,
    crm_status_conflict      boolean NOT NULL DEFAULT false,
    crm_task_count           integer NOT NULL DEFAULT 0,
    crm_comment_count        integer,
    meeting_tool             text,
    meeting_type             text,
    is_external_tool         boolean,

    outlook_status           text    NOT NULL,
    msgraph_exclusion_reason text,
    is_short_lived_event     boolean,
    is_booking               boolean,

    call_status              text    NOT NULL,
    online_meeting_lookup    text,

    CONSTRAINT pk_sales_meeting_signals PRIMARY KEY (id),
    CONSTRAINT fk_sales_meeting_signals_msgraph_events FOREIGN KEY (msgraph_event_id)
        REFERENCES raw.msgraph_events (id) ON DELETE CASCADE,
    CONSTRAINT fk_sales_meeting_signals_msgraph_call_event FOREIGN KEY (call_event_mapping_id)
        REFERENCES mapping.msgraph_call_event (id) ON DELETE CASCADE,
    CONSTRAINT fk_sales_meeting_signals_crm_lead_tasks FOREIGN KEY (crm_lead_task_id)
        REFERENCES raw.crm_lead_tasks (id) ON DELETE CASCADE,
    CONSTRAINT fk_sales_meeting_signals_crm_leads FOREIGN KEY (lead_id)
        REFERENCES raw.crm_leads (id) ON DELETE CASCADE,
    CONSTRAINT fk_sales_meeting_signals_msgraph_contacts FOREIGN KEY (contact_id)
        REFERENCES raw.msgraph_contacts (id) ON DELETE CASCADE,
    CONSTRAINT fk_sales_meeting_signals_msgraph_contacts_organizer FOREIGN KEY (organizer_contact_id)
        REFERENCES raw.msgraph_contacts (id) ON DELETE CASCADE,
    CONSTRAINT uq_sales_meeting_signals_meeting_key UNIQUE (meeting_key)
);

CREATE INDEX IF NOT EXISTS idx_sales_meeting_signals_msgraph_event_id      ON processed.sales_meeting_signals (msgraph_event_id);
CREATE INDEX IF NOT EXISTS idx_sales_meeting_signals_call_event_mapping_id ON processed.sales_meeting_signals (call_event_mapping_id);
CREATE INDEX IF NOT EXISTS idx_sales_meeting_signals_crm_lead_task_id      ON processed.sales_meeting_signals (crm_lead_task_id);
CREATE INDEX IF NOT EXISTS idx_sales_meeting_signals_lead_id               ON processed.sales_meeting_signals (lead_id);
CREATE INDEX IF NOT EXISTS idx_sales_meeting_signals_contact_id            ON processed.sales_meeting_signals (contact_id);
CREATE INDEX IF NOT EXISTS idx_sales_meeting_signals_organizer_contact_id  ON processed.sales_meeting_signals (organizer_contact_id);
CREATE INDEX IF NOT EXISTS idx_sales_meeting_signals_event_date            ON processed.sales_meeting_signals (event_date);

COMMENT ON TABLE processed.sales_meeting_signals IS 'What each source system says about a sales meeting, without judging it: one row per (MSGraph meeting x external lead) as in processed.sales_meetings_unified, plus one row per CRM VC-task that matched no meeting (also ambiguous ones). No-show and exclusion are evaluations and are not stored here. Derived table, fully rebuilt by dbconnectorR::update_sales_meeting_signals().';
COMMENT ON COLUMN processed.sales_meeting_signals.meeting_key IS 'Same identity as processed.sales_meetings_unified.meeting_key: msgraph_<call_event_mapping_id>_<lead_id> for MSGraph meetings (lead_id NA = leadless), crm_<crm_task_id> for CRM tasks without a meeting.';
COMMENT ON COLUMN processed.sales_meeting_signals.msgraph_event_id IS 'MSGraph event (raw.msgraph_events.id); NULL for CRM-only rows.';
COMMENT ON COLUMN processed.sales_meeting_signals.call_event_mapping_id IS 'Call-event mapping row of the meeting (mapping.msgraph_call_event.id); NULL for CRM-only rows.';
COMMENT ON COLUMN processed.sales_meeting_signals.crm_lead_task_id IS 'The CRM task whose outcome won (raw.crm_lead_tasks.id); NULL when no task belongs to the meeting. All tasks of a meeting are in mapping.crm_lead_task_msgraph_event.';
COMMENT ON COLUMN processed.sales_meeting_signals.lead_id IS 'External CRM lead of the row (raw.crm_leads.id); NULL for a leadless meeting.';
COMMENT ON COLUMN processed.sales_meeting_signals.lead_source IS 'Where lead_id comes from: mapping, domain, ersatzregel (leadless meeting took the task''s lead), crm_task (CRM-only row); NULL for a leadless meeting.';
COMMENT ON COLUMN processed.sales_meeting_signals.contact_id IS 'Responsible rep (raw.msgraph_contacts.id); for CRM-only rows the rep of the task.';
COMMENT ON COLUMN processed.sales_meeting_signals.organizer_contact_id IS 'Organizer of the MSGraph meeting (raw.msgraph_contacts.id); NULL when unknown and for CRM-only rows.';
COMMENT ON COLUMN processed.sales_meeting_signals.match_rule IS 'match_rule of the winning task from mapping.crm_lead_task_msgraph_event; NULL when no task belongs to the meeting.';
COMMENT ON COLUMN processed.sales_meeting_signals.event_date IS 'Date of the meeting (Europe/Berlin); for CRM-only rows the date of the task precise_time.';
COMMENT ON COLUMN processed.sales_meeting_signals.event_start IS 'Start of the meeting in UTC; for CRM-only rows the task precise_time.';
COMMENT ON COLUMN processed.sales_meeting_signals.original_created_at IS 'When the meeting was first scheduled: original_created_at of the MSGraph classification, for CRM-only rows the task creation time.';
COMMENT ON COLUMN processed.sales_meeting_signals.has_crm_task IS 'At least one CRM VC-task belongs to the row.';
COMMENT ON COLUMN processed.sales_meeting_signals.has_msgraph_event IS 'The row is an MSGraph calendar meeting.';
COMMENT ON COLUMN processed.sales_meeting_signals.crm_status IS 'CRM outcome: show_up, no_show, storniert, ohne_ausgang (task without a recognised outcome note), kein_task. Within a task the latest note with a recognised outcome counts (comment_created_at, then created_at, then crm_comment_id); across tasks show_up > no_show > storniert > ohne_ausgang.';
COMMENT ON COLUMN processed.sales_meeting_signals.crm_status_conflict IS 'TRUE when the recognised outcomes of the notes of a task, or of the tasks of the row, are not all the same.';
COMMENT ON COLUMN processed.sales_meeting_signals.crm_task_count IS 'Number of CRM VC-tasks that belong to the row.';
COMMENT ON COLUMN processed.sales_meeting_signals.crm_comment_count IS 'Number of not deleted notes on the winning task; NULL without a task.';
COMMENT ON COLUMN processed.sales_meeting_signals.meeting_tool IS 'Video-call tool parsed from the winning task name (webex/zoom/google_meet/skype/teams/unbekannt).';
COMMENT ON COLUMN processed.sales_meeting_signals.meeting_type IS 'Meeting type parsed from the winning task name (nv/uc/fu/rep/er/zr/planung/unbekannt).';
COMMENT ON COLUMN processed.sales_meeting_signals.is_external_tool IS 'TRUE if the winning task names an external (MSGraph-invisible) video-call tool.';
COMMENT ON COLUMN processed.sales_meeting_signals.outlook_status IS 'Calendar state of the MSGraph event: aktiv, verschoben (cancelled while an active event with the same iCal UID exists), abgesagt (cancelled otherwise, including events that vanished from a loaded calendar), kein_event (CRM-only row).';
COMMENT ON COLUMN processed.sales_meeting_signals.msgraph_exclusion_reason IS 'exclusion_reason of the responsible row in processed.msgraph_extern_event_classification, taken over unchanged; one reason per meeting by that table''s precedence.';
COMMENT ON COLUMN processed.sales_meeting_signals.is_short_lived_event IS 'Event was cancelled less than 24 hours after it was created (from processed.msgraph_extern_event_classification); NULL for CRM-only rows.';
COMMENT ON COLUMN processed.sales_meeting_signals.is_booking IS 'Event comes from the Bookings tool (a participant with source = booking); NULL for CRM-only rows.';
COMMENT ON COLUMN processed.sales_meeting_signals.call_status IS 'Teams attendance, first applicable wins: in_zukunft (meeting not yet started), extern_call (call with an external participant), intern_call (only internal participants joined), alt_tenant (link of the previous tenant, no call), nicht_abrufbar (Microsoft does not return the online meeting, no call), ohne_link (checked, the event has no Teams link), no_call (no call found), kein_event (CRM-only row).';
COMMENT ON COLUMN processed.sales_meeting_signals.online_meeting_lookup IS 'Raw outcome of the online meeting search from raw.msgraph_events.online_meeting_lookup.';

CREATE OR REPLACE TRIGGER trigger_set_updated_at
    BEFORE UPDATE
    ON processed.sales_meeting_signals
    FOR EACH ROW
    EXECUTE FUNCTION public.update_timestamp();
