-- mapping.crm_lead_task_msgraph_event: welcher CRM-VC-Task zu welchem MSGraph-Termin gehoert.
-- VOR der Installation des Pakets in DBeaver gegen Production ausfuehren.
-- Abgeleitete Tabelle, voller Rebuild durch dbconnectorR::update_crm_lead_task_msgraph_event().
-- Asana: https://app.asana.com/0/0/1220115877676176/f
-- Spec: docs/specs/2026-11-02-termin-mapping-und-signaltabelle.md, ADR 0003

CREATE TABLE IF NOT EXISTS mapping.crm_lead_task_msgraph_event (
    id                 bigint NOT NULL GENERATED ALWAYS AS IDENTITY ( INCREMENT 1 START 1 MINVALUE 1 MAXVALUE 9223372036854775807 CACHE 1 ),
    created_at         timestamp without time zone NOT NULL DEFAULT now(),
    updated_at         timestamp without time zone NOT NULL DEFAULT now(),

    crm_lead_task_id   bigint        NOT NULL,
    msgraph_event_id   bigint,
    match_rule         text          NOT NULL,
    candidate_count    integer,
    start_diff_minutes numeric(14,4),
    lead_source        text,

    CONSTRAINT pk_crm_lead_task_msgraph_event PRIMARY KEY (id),
    CONSTRAINT fk_crm_lead_task_msgraph_event_crm_lead_tasks FOREIGN KEY (crm_lead_task_id)
        REFERENCES raw.crm_lead_tasks (id) ON DELETE CASCADE,
    CONSTRAINT fk_crm_lead_task_msgraph_event_msgraph_events FOREIGN KEY (msgraph_event_id)
        REFERENCES raw.msgraph_events (id) ON DELETE CASCADE,
    CONSTRAINT uq_crm_lead_task_msgraph_event_crm_lead_task_id UNIQUE (crm_lead_task_id)
);

CREATE INDEX IF NOT EXISTS idx_crm_lead_task_msgraph_event_crm_lead_task_id
    ON mapping.crm_lead_task_msgraph_event (crm_lead_task_id);
CREATE INDEX IF NOT EXISTS idx_crm_lead_task_msgraph_event_msgraph_event_id
    ON mapping.crm_lead_task_msgraph_event (msgraph_event_id);

COMMENT ON TABLE mapping.crm_lead_task_msgraph_event IS 'Which MSGraph calendar event a CRM video-call task belongs to, with the evidence for the match. One row per CRM VC-task (not deleted, VC marker plus visit badge, resolvable rep), also when no event was found. Carries no meeting outcome; that lives in processed.sales_meeting_signals. Derived table, fully rebuilt by dbconnectorR::update_crm_lead_task_msgraph_event().';
COMMENT ON COLUMN mapping.crm_lead_task_msgraph_event.crm_lead_task_id IS 'The CRM task (surrogate raw.crm_lead_tasks.id).';
COMMENT ON COLUMN mapping.crm_lead_task_msgraph_event.msgraph_event_id IS 'The matched MSGraph event (raw.msgraph_events.id); NULL when the task matched no event, see match_rule.';
COMMENT ON COLUMN mapping.crm_lead_task_msgraph_event.match_rule IS 'Rule that decided the match, first applicable wins: externes_tool (WebEx/Zoom/Google Meet/Skype, never in MSGraph), ohne_lead (task has no lead), lead_tag (exactly one meeting of the lead that day), lead_tag_rep (several, exactly one of the same rep), lead_tag_naechste_zeit (several of the same rep, unique nearest start), mehrdeutig (residual ambiguity, not merged), ersatzregel (no candidate; leadless meeting of the same rep that day starting within 15 minutes), netto_neu (nothing found).';
COMMENT ON COLUMN mapping.crm_lead_task_msgraph_event.candidate_count IS 'Number of candidate meetings the deciding rule looked at: meetings of the lead that day for the lead_tag rules and mehrdeutig, leadless meetings within 15 minutes for ersatzregel, 0 for netto_neu. NULL for externes_tool and ohne_lead, which look at no candidates.';
COMMENT ON COLUMN mapping.crm_lead_task_msgraph_event.start_diff_minutes IS 'Absolute distance in minutes between the matched event start and the task precise_time. NULL without a match or without precise_time.';
COMMENT ON COLUMN mapping.crm_lead_task_msgraph_event.lead_source IS 'Where the lead of the matched meeting row comes from: mapping (an attendee address is mapped to the lead in mapping.crm_lead_msgraph_contact), domain (an attendee domain belongs to exactly this lead), ersatzregel (the meeting had no lead and took the task''s lead). NULL without a match.';

CREATE OR REPLACE TRIGGER trigger_set_updated_at
    BEFORE UPDATE
    ON mapping.crm_lead_task_msgraph_event
    FOR EACH ROW
    EXECUTE FUNCTION public.update_timestamp();
