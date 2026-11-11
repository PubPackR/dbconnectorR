-- processed.sales_meetings und processed.sales_meeting_leads: eine Zeile je Meeting record
-- bzw. je Meeting record und Lead, nur Signale, keine Bewertung.
-- VOR dem ersten Lauf von dbconnectorR::update_sales_meetings() in DBeaver gegen
-- Production ausfuehren. Abgeleitete Tabellen, voller Rebuild jede Nacht.
-- Asana: https://app.asana.com/0/0/1220345592707180/f
-- Spec: docs/specs/2026-11-11-sales-meetings.md

CREATE TABLE IF NOT EXISTS processed.sales_meetings (
    id                          bigint NOT NULL GENERATED ALWAYS AS IDENTITY ( INCREMENT 1 START 1 MINVALUE 1 MAXVALUE 9223372036854775807 CACHE 1 ),
    created_at                  timestamp without time zone NOT NULL DEFAULT now(),
    updated_at                  timestamp without time zone NOT NULL DEFAULT now(),

    meeting_key                 text    NOT NULL,
    msgraph_event_id            bigint,

    meeting_date                date,
    starts_at_utc               timestamp without time zone,
    booked_date                 date,
    booked_date_source          text,

    organizer_person_id         bigint,
    organizer_type              text,
    responsible_person_id       bigint,

    calendar_status             text    NOT NULL,
    calendar_changed_at_utc     timestamp without time zone,
    teams_status                text    NOT NULL,
    booking_role                text,
    is_short_lived              boolean,
    is_duplicate                boolean,

    internal_participant_count  integer,
    external_participant_count  integer,
    external_speaker_count      integer,
    call_started_at_utc         timestamp without time zone,
    -- ASSUMPTION: numeric(14,4), a call can last longer than 999.99 minutes.
    call_duration_minutes       numeric(14,4),

    successor_meeting_id        bigint,
    meeting_chain_id            bigint,

    CONSTRAINT pk_sales_meetings PRIMARY KEY (id),
    CONSTRAINT fk_sales_meetings_msgraph_events FOREIGN KEY (msgraph_event_id)
        REFERENCES raw.msgraph_events (id) ON DELETE CASCADE,
    CONSTRAINT fk_sales_meetings_personio_persons_organizer FOREIGN KEY (organizer_person_id)
        REFERENCES raw.personio_persons (id) ON DELETE CASCADE,
    CONSTRAINT fk_sales_meetings_personio_persons_responsible FOREIGN KEY (responsible_person_id)
        REFERENCES raw.personio_persons (id) ON DELETE CASCADE,
    -- SET NULL statt CASCADE: Billomatics::postgres_upsert_data loescht fehlende Zeilen
    -- nach dem Upsert; ein kaskadierender Selbstbezug nahme gueltige Vorgaenger mit (Q12).
    CONSTRAINT fk_sales_meetings_sales_meetings_successor FOREIGN KEY (successor_meeting_id)
        REFERENCES processed.sales_meetings (id) ON DELETE SET NULL,
    CONSTRAINT uq_sales_meetings_meeting_key UNIQUE (meeting_key)
);

CREATE INDEX IF NOT EXISTS idx_sales_meetings_msgraph_event_id      ON processed.sales_meetings (msgraph_event_id);
CREATE INDEX IF NOT EXISTS idx_sales_meetings_organizer_person_id   ON processed.sales_meetings (organizer_person_id);
CREATE INDEX IF NOT EXISTS idx_sales_meetings_responsible_person_id ON processed.sales_meetings (responsible_person_id);
CREATE INDEX IF NOT EXISTS idx_sales_meetings_successor_meeting_id  ON processed.sales_meetings (successor_meeting_id);
CREATE INDEX IF NOT EXISTS idx_sales_meetings_meeting_chain_id      ON processed.sales_meetings (meeting_chain_id);
CREATE INDEX IF NOT EXISTS idx_sales_meetings_meeting_date          ON processed.sales_meetings (meeting_date);

COMMENT ON TABLE processed.sales_meetings IS 'One row per meeting record: every calendar row of raw.msgraph_events with at least one external participant (also cancelled, vanished, Bookings entries and rows without meeting_id), plus one row per CRM VC-task that belongs to no calendar row. A meeting can have several records (Bookings entry, calendar copy, own invite, each step of a reschedule chain). Signals only, no evaluation. Derived table, fully rebuilt by dbconnectorR::update_sales_meetings().';
COMMENT ON COLUMN processed.sales_meetings.meeting_key IS 'Identity of the record: msgraph_<raw.msgraph_events.id> for calendar rows, crm_<crm_task_id> for CRM-only rows.';
COMMENT ON COLUMN processed.sales_meetings.msgraph_event_id IS 'Calendar row (raw.msgraph_events.id); NULL for CRM-only rows.';
COMMENT ON COLUMN processed.sales_meetings.meeting_date IS 'Day of the start in Europe/Berlin.';
COMMENT ON COLUMN processed.sales_meetings.starts_at_utc IS 'Start in UTC: event_start of the calendar row, precise_time of the task for CRM-only rows.';
COMMENT ON COLUMN processed.sales_meetings.booked_date IS 'Day the meeting was scheduled, in Europe/Berlin. Calendar rows: the earliest creation per iCal UID, capped by our own first ingest, inherited from an older mailbox-move copy of the same slot and from a cancelled predecessor that was replaced by another calendar item. Bookings entries: the day of the start. CRM-only rows: the day the task was created.';
COMMENT ON COLUMN processed.sales_meetings.booked_date_source IS 'Where booked_date comes from: outlook, bookings_start (Bookings entry, which carries no real creation time), crm_task.';
COMMENT ON COLUMN processed.sales_meetings.organizer_person_id IS 'Person behind the internal organizer of the calendar row (raw.personio_persons.id); NULL when not resolvable and for CRM-only rows.';
COMMENT ON COLUMN processed.sales_meetings.organizer_type IS 'sales (organizer in Sales on the booked day), outside_sales, crm_task (CRM-only row, has no organizer), unknown (organizer missing or not resolvable). From kpiR::meeting_person_roles().';
COMMENT ON COLUMN processed.sales_meetings.responsible_person_id IS 'Person who holds the meeting (raw.personio_persons.id): the organizer if there was no call or the organizer joined it, otherwise the first internal call participant; the rep of the task for CRM-only rows. NULL when not resolvable.';
COMMENT ON COLUMN processed.sales_meetings.calendar_status IS 'active, stale_copy (active single instance with a later version of the same iCal UID), rescheduled (cancelled with a successor), vanished (cancelled without successor, disappeared from a loaded calendar), cancelled (cancelled without successor), not_in_calendar (CRM-only row).';
COMMENT ON COLUMN processed.sales_meetings.calendar_changed_at_utc IS 'When the calendar state last changed in UTC: vanished_at, otherwise event_updated_at; NULL for Bookings entries without vanished_at and for CRM-only rows.';
COMMENT ON COLUMN processed.sales_meetings.teams_status IS 'Teams attendance, first applicable wins: future (not started yet), external_joined (a session with an external participant in the attendance report or an external speaker in the transcript), internal_only (only internal participants joined), old_tenant (link of the previous tenant, no call), not_retrievable (Microsoft does not return the online meeting, no call), no_link (checked, the row has no Teams link), no_call, not_in_calendar (CRM-only row).';
COMMENT ON COLUMN processed.sales_meetings.booking_role IS 'booking_entry (Bookings appointment, iCal UID booking:...), calendar_copy (calendar row with the Bookings mailbox as participant); NULL otherwise. The own invite is found through the SDR Bookings table, not here.';
COMMENT ON COLUMN processed.sales_meetings.is_short_lived IS 'Cancelled less than 24 hours after booked: cancellation time is vanished_at, otherwise event_updated_at. NULL for Bookings entries and CRM-only rows.';
COMMENT ON COLUMN processed.sales_meetings.is_duplicate IS 'TRUE when another active row invites the same external person within 15 minutes and this row loses the pair (the no-show when only it is one, otherwise the later). Only active rows that are neither a Bookings entry nor a calendar copy are checked; all other calendar rows are FALSE. NULL for CRM-only rows.';
COMMENT ON COLUMN processed.sales_meetings.internal_participant_count IS 'Invited internal participants of the calendar row; NULL for CRM-only rows.';
COMMENT ON COLUMN processed.sales_meetings.external_participant_count IS 'Invited participants that are neither internal nor synthetic guests; at least 1 for calendar rows, NULL for CRM-only rows.';
COMMENT ON COLUMN processed.sales_meetings.external_speaker_count IS 'Highest number of external speakers (label without the company suffix, at least two lines) in a transcript of any session of the row; 0 with a transcript without external speaker, NULL without transcript.';
COMMENT ON COLUMN processed.sales_meetings.call_started_at_utc IS 'Start of the longest Teams session of the row in UTC; NULL without call.';
COMMENT ON COLUMN processed.sales_meetings.call_duration_minutes IS 'Duration of the longest Teams session of the row in minutes; NULL without call.';
COMMENT ON COLUMN processed.sales_meetings.successor_meeting_id IS 'Record that replaces this one (processed.sales_meetings.id): the newest active row of the same iCal UID, else the replacing calendar item. NULL when there is none or it is no meeting record.';
COMMENT ON COLUMN processed.sales_meetings.meeting_chain_id IS 'id of the first record of the reschedule chain this record belongs to (the one without predecessor, the smallest msgraph_event_id when there are several). A record without chain carries its own id. Group by it to count a rescheduled meeting once.';

CREATE OR REPLACE TRIGGER trigger_set_updated_at
    BEFORE UPDATE
    ON processed.sales_meetings
    FOR EACH ROW
    EXECUTE FUNCTION public.update_timestamp();


CREATE TABLE IF NOT EXISTS processed.sales_meeting_leads (
    id                          bigint NOT NULL GENERATED ALWAYS AS IDENTITY ( INCREMENT 1 START 1 MINVALUE 1 MAXVALUE 9223372036854775807 CACHE 1 ),
    created_at                  timestamp without time zone NOT NULL DEFAULT now(),
    updated_at                  timestamp without time zone NOT NULL DEFAULT now(),

    meeting_id                  bigint  NOT NULL,
    lead_id                     bigint  NOT NULL,
    lead_match_source           text    NOT NULL,
    crm_lead_task_id            bigint,
    crm_outcome                 text    NOT NULL,
    crm_outcome_conflict        boolean NOT NULL DEFAULT false,
    meeting_tool                text,
    meeting_type                text,
    is_external_tool            boolean,

    CONSTRAINT pk_sales_meeting_leads PRIMARY KEY (id),
    CONSTRAINT fk_sales_meeting_leads_sales_meetings FOREIGN KEY (meeting_id)
        REFERENCES processed.sales_meetings (id) ON DELETE CASCADE,
    CONSTRAINT fk_sales_meeting_leads_crm_leads FOREIGN KEY (lead_id)
        REFERENCES raw.crm_leads (id) ON DELETE CASCADE,
    CONSTRAINT fk_sales_meeting_leads_crm_lead_tasks FOREIGN KEY (crm_lead_task_id)
        REFERENCES raw.crm_lead_tasks (id) ON DELETE CASCADE,
    CONSTRAINT uq_sales_meeting_leads_meeting_id_lead_id UNIQUE (meeting_id, lead_id)
);

CREATE INDEX IF NOT EXISTS idx_sales_meeting_leads_meeting_id       ON processed.sales_meeting_leads (meeting_id);
CREATE INDEX IF NOT EXISTS idx_sales_meeting_leads_lead_id          ON processed.sales_meeting_leads (lead_id);
CREATE INDEX IF NOT EXISTS idx_sales_meeting_leads_crm_lead_task_id ON processed.sales_meeting_leads (crm_lead_task_id);

COMMENT ON TABLE processed.sales_meeting_leads IS 'One row per meeting record and external CRM lead, with the CRM task that speaks for the lead. Records without a derivable lead have no row here. Derived table, fully rebuilt by dbconnectorR::update_sales_meetings().';
COMMENT ON COLUMN processed.sales_meeting_leads.meeting_id IS 'Meeting record (processed.sales_meetings.id).';
COMMENT ON COLUMN processed.sales_meeting_leads.lead_id IS 'External CRM lead (raw.crm_leads.id).';
COMMENT ON COLUMN processed.sales_meeting_leads.lead_match_source IS 'How the lead was found: mapping (a participant address is mapped to the lead), domain (the unique domain of a participant), substitute_rule (a leadless calendar row took the lead of a CRM task of the same rep within 15 minutes), crm_task (CRM-only row).';
COMMENT ON COLUMN processed.sales_meeting_leads.crm_lead_task_id IS 'The CRM VC-task whose outcome counts for this lead (raw.crm_lead_tasks.id): across tasks held > no_show > cancelled > no_outcome, on a tie the smaller crm_task_id. NULL when no task belongs to the row.';
COMMENT ON COLUMN processed.sales_meeting_leads.crm_outcome IS 'Outcome documented in the CRM: held, no_show, cancelled, no_outcome (task without a recognised outcome note), no_task. Within a task the latest note with a recognised outcome counts.';
COMMENT ON COLUMN processed.sales_meeting_leads.crm_outcome_conflict IS 'TRUE when the recognised outcomes of the notes of a task, or of the tasks of the row, are not all the same.';
COMMENT ON COLUMN processed.sales_meeting_leads.meeting_tool IS 'Video-call tool parsed from the task name (webex/zoom/google_meet/skype/teams/unbekannt); NULL without task.';
COMMENT ON COLUMN processed.sales_meeting_leads.meeting_type IS 'Meeting type parsed from the task name (nv/uc/fu/rep/er/zr/planung/unbekannt); NULL without task.';
COMMENT ON COLUMN processed.sales_meeting_leads.is_external_tool IS 'TRUE when the task names a video-call tool that MSGraph cannot see; NULL without task.';

CREATE OR REPLACE TRIGGER trigger_set_updated_at
    BEFORE UPDATE
    ON processed.sales_meeting_leads
    FOR EACH ROW
    EXECUTE FUNCTION public.update_timestamp();
