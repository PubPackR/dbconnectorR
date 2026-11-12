-- Referenz-SQL Termin-Bewertung: die heutigen Regeln auf den Signalen.
-- Asana: https://app.asana.com/0/0/1220115896589056/f
-- Spec:  dbconnectorR docs/specs/2026-11-12-referenz-termin-bewertung.md
--
-- Diese Datei ist der reviewte Ausgangsstand. Sobald das Metabase-Modell
-- "Termin-Bewertung" in der RevOps-Sammlung angelegt ist, ist das Modell massgeblich.
-- Aenderungen macht RevOps dann dort, nicht hier.
--
-- Eine Zeile je Meeting record und Lead. Ein Termin ohne Lead steht als eine Zeile mit
-- leerem lead_id. Termine zaehlt count(DISTINCT meeting_id), nicht count(*).
--
-- Spalten
--   meeting_id, lead_id, meeting_key  meeting_key ist msgraph_<event_id> oder crm_<crm_task_id>
--   meeting_date, booked_date         Termintag und Legedatum, Berliner Tage
--   is_no_show                        kein Externer im Teams-Call oder CRM-Ausgang no_show.
--                                     Wie heute auch fuer Termine in der Zukunft wahr, sie
--                                     fallen ueber excluded heraus.
--   excluded, exclusion_reason        Ausschluss und Grund, siehe unten
--   stattgefunden                     vergangen, nicht ausgeschlossen, kein No-Show
--   bewertbar                         vergangen und Anwesenheit messbar
--   eigener_termin                    zaehlt als gelegter Termin
--
-- exclusion_reason                   Bedeutung                                       eigener_termin  bewertbar
--   verschoben                       abgesagte Haelfte einer Verlegung                nein            ja
--   abgesagt                         abgesagt oder verschwunden, ohne Teams-Call,     nein            ja
--                                    ohne CRM-Task
--   ohne_teams_meeting               aktiv, ohne Teams-Meeting, ohne CRM-Task         nein            ja
--   zu_viele_interne                 mindestens 7 interne Teilnehmende                nein            ja
--   duplikat_event                   derselbe Termin ein zweites Mal                  nein            ja
--   termin_in_zukunft                liegt in der Zukunft                             ja              nein (nicht vergangen)
--   alt_tenant_join_url              Teams-Link aus dem Alt-Tenant                    ja              nein
--   online_meeting_nicht_abrufbar    Microsoft gibt das Online-Meeting nicht heraus   ja              nein
--   crm_storniert                    im CRM storniert, ueberschreibt jeden Grund      ja              nein

WITH heute AS (
  SELECT (now() AT TIME ZONE 'Europe/Berlin')::date AS tag
),

zeile AS (
  SELECT
    m.id AS meeting_id,
    l.lead_id,
    m.meeting_key,
    m.meeting_date,
    m.booked_date,
    m.calendar_status,
    m.teams_status,
    m.is_duplicate,
    m.internal_participant_count,
    -- CRM-Ausgang je Lead, Rangfolge vom 02.11.2026: held > no_show > cancelled > no_outcome.
    -- Eine CRM-Zeile ohne Lead hat keine Lead-Zeile und gilt als no_outcome.
    CASE
      WHEN l.crm_outcome IS NOT NULL             THEN l.crm_outcome
      WHEN m.calendar_status = 'not_in_calendar' THEN 'no_outcome'
      ELSE 'no_task'
    END AS crm_outcome,
    m.teams_status IN ('external_joined', 'internal_only') AS mit_call,
    -- Termin ohne Teams-Meeting: das Event traegt keine meeting_id.
    (m.msgraph_event_id IS NOT NULL AND coalesce(e.meeting_id, '') = '') AS ohne_teams_meeting,
    -- Echter No-Show trotz Verlegung: interner Call hoechstens 30 Minuten um den Slot,
    -- geaendert erst nach dem Slot.
    (m.calendar_status = 'rescheduled'
     AND m.teams_status IN ('internal_only', 'no_call')
     AND m.call_started_at_utc IS NOT NULL
     AND abs(extract(epoch FROM m.call_started_at_utc - m.starts_at_utc)) <= 30 * 60
     AND m.calendar_changed_at_utc > m.starts_at_utc) AS echter_no_show
  FROM processed.sales_meetings m
  LEFT JOIN processed.sales_meeting_leads l ON l.meeting_id = m.id
  LEFT JOIN raw.msgraph_events e ON e.id = m.msgraph_event_id
),

-- Welche Regel greift. Die Kalenderzeilen der Arten verschoben, abgesagt und
-- ohne_teams_meeting bewertet heute niemand: Mit CRM-Task zaehlen sie allein ueber den
-- CRM-Ausgang wie eine CRM-Zeile, ohne Task tragen sie die Art als Grund.
art AS (
  SELECT z.*,
    CASE
      WHEN z.calendar_status = 'not_in_calendar'                                  THEN 'crm'
      WHEN z.calendar_status = 'rescheduled' AND NOT z.mit_call                   THEN 'verschoben'
      WHEN z.calendar_status IN ('cancelled', 'vanished') AND NOT z.mit_call      THEN 'abgesagt'
      WHEN z.calendar_status IN ('active', 'stale_copy') AND z.ohne_teams_meeting THEN 'ohne_teams_meeting'
      ELSE 'kalender'
    END AS art
  FROM zeile z
),

-- Kalenderzeilen: Grund und No-Show aus Outlook und Teams, vor dem CRM-Abgleich.
-- Der erste zutreffende Grund gewinnt.
basis AS (
  SELECT a.*,
    a.teams_status <> 'external_joined' AS basis_no_show,
    CASE
      WHEN a.calendar_status = 'rescheduled' AND NOT a.echter_no_show THEN 'verschoben'
      WHEN a.internal_participant_count >= 7                         THEN 'zu_viele_interne'
      WHEN a.is_duplicate                                             THEN 'duplikat_event'
      WHEN a.teams_status = 'future'                                  THEN 'termin_in_zukunft'
      WHEN a.teams_status = 'old_tenant'                              THEN 'alt_tenant_join_url'
      WHEN a.teams_status = 'not_retrievable'                         THEN 'online_meeting_nicht_abrufbar'
    END AS basis_grund
  FROM art a
),

bewertung AS (
  SELECT b.*,
    CASE
      -- CRM-Zeilen und Zeilen ohne heutige Bewertung mit CRM-Task: allein der CRM-Ausgang.
      WHEN b.art = 'crm' OR (b.art <> 'kalender' AND b.crm_outcome <> 'no_task') THEN
        CASE b.crm_outcome WHEN 'no_show' THEN true WHEN 'cancelled' THEN NULL ELSE false END
      WHEN b.art <> 'kalender' THEN b.basis_no_show
      -- Kalenderzeilen: Ein dokumentierter Ausgang gewinnt. Ohne Ausgang entscheidet Teams,
      -- ausser die Anwesenheit ist nicht messbar; dann gilt der Termin als stattgefunden.
      WHEN b.crm_outcome = 'held'    THEN false
      WHEN b.crm_outcome = 'no_show' THEN true
      WHEN b.crm_outcome = 'no_outcome'
           AND (b.teams_status = 'no_link'
                OR b.basis_grund IN ('alt_tenant_join_url', 'online_meeting_nicht_abrufbar')) THEN false
      ELSE b.basis_no_show
    END AS is_no_show,
    CASE
      WHEN b.crm_outcome = 'cancelled' THEN 'crm_storniert'
      WHEN b.art = 'crm' OR (b.art <> 'kalender' AND b.crm_outcome <> 'no_task') THEN NULL
      WHEN b.art <> 'kalender' THEN b.art
      -- Ein CRM-Ausgang hebt nur die Gruende auf, bei denen Teams nichts messen konnte.
      WHEN b.basis_grund IN ('alt_tenant_join_url', 'online_meeting_nicht_abrufbar')
           AND b.crm_outcome IN ('held', 'no_show', 'no_outcome') THEN NULL
      ELSE b.basis_grund
    END AS exclusion_reason
  FROM basis b
)

SELECT
  w.meeting_id,
  w.lead_id,
  w.meeting_key,
  w.meeting_date,
  w.booked_date,
  w.is_no_show,
  w.exclusion_reason IS NOT NULL AS excluded,
  w.exclusion_reason,
  (coalesce(w.meeting_date < h.tag, false)
   AND w.exclusion_reason IS NULL
   AND w.is_no_show IS FALSE) AS stattgefunden,
  (coalesce(w.meeting_date < h.tag, false)
   AND (w.exclusion_reason IS NULL
        OR w.exclusion_reason NOT IN ('alt_tenant_join_url', 'online_meeting_nicht_abrufbar',
                                      'crm_storniert'))) AS bewertbar,
  (w.exclusion_reason IS NULL
   OR w.exclusion_reason NOT IN ('verschoben', 'abgesagt', 'ohne_teams_meeting',
                                 'zu_viele_interne', 'duplikat_event')) AS eigener_termin
FROM bewertung w
CROSS JOIN heute h
