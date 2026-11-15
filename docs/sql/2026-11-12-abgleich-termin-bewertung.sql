-- Abgleich der Referenz-SQL Termin-Bewertung gegen processed.sales_meetings_unified.
-- Lesend, in DBeaver gegen Production, nach dem Nachtlauf von msgraph-ingestion.
-- Spec:  docs/specs/2026-11-12-referenz-termin-bewertung.md (Validierung)
-- Asana: https://app.asana.com/0/0/1220115896589056/f
--
-- Der Block "ref" ist eine unveraenderte Kopie von
-- docs/sql/2026-11-12-referenz-termin-bewertung.sql ab "WITH heute". Nach einer Aenderung
-- dort neu kopieren.
--
-- Ein Ergebnis mit drei Sichten:
--   A  je befund: Zeilen und Kennzahlen beider Seiten nebeneinander. Jede Zeile der
--      Referenz-SQL wird ihrem Gegenstueck in unified zugeordnet: Kalenderzeilen ueber
--      Event und Lead, CRM-Zeilen von heute ueber ihren Task.
--   B  je Monat (Termintag): dieselben Kennzahlen, ohne Zuordnung.
--   C  Grain: Die Referenz-SQL hat je (meeting_id, lead_id) genau eine Zeile.
-- Kennzahlen je Zeile (Meeting record x Lead, auf beiden Seiten dieselbe Ebene):
--   gelegt         eigener_termin
--   bewertbar      bewertbar und eigener_termin
--   stattgefunden  stattgefunden
--   no_show        bewertbar, eigener_termin und nicht stattgefunden
-- unified wird mit denselben Formeln abgeleitet wie kpiR termin_flags() und
-- ist_eigener_termin(); rescheduled_without_meeting_id zaehlt als verschoben (Q8).

WITH ref AS (
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
    m.organizer_is_external,
    -- Echter No-Show trotz Verlegung: interner Call hoechstens 30 Minuten um den Slot,
    -- geaendert erst nach dem Slot. Wie heute zaehlt event_updated_at aus Outlook, nicht
    -- calendar_changed_at_utc: bei verschwundenen Zeilen traegt das Signal vanished_at und
    -- liegt fast immer nach dem Slot (13.11.2026: 835 Zeilen Unterschied).
    (m.calendar_status = 'rescheduled'
     AND m.teams_status IN ('internal_only', 'no_call')
     AND m.call_started_at_utc IS NOT NULL
     AND abs(extract(epoch FROM m.call_started_at_utc - m.starts_at_utc)) <= 30 * 60
     AND e.event_updated_at > e.event_start) AS echter_no_show
  FROM processed.sales_meetings m
  LEFT JOIN processed.sales_meeting_leads l ON l.meeting_id = m.id
  LEFT JOIN raw.msgraph_events e ON e.id = m.msgraph_event_id
),

-- Welche Regel greift. Die Kalenderzeilen der Arten verschoben, abgesagt,
-- ohne_teams_meeting und externer_organisator bewertet heute niemand: Mit CRM-Task zaehlen
-- sie allein ueber den CRM-Ausgang wie eine CRM-Zeile, ohne Task tragen sie die Art als
-- Grund. externer_organisator: Der Kunde laedt in sein eigenes Teams-Meeting ein, einen
-- Anwesenheitsbericht dazu bekommen wir nie.
art AS (
  SELECT z.*,
    CASE
      WHEN z.calendar_status = 'not_in_calendar'                                  THEN 'crm'
      WHEN z.calendar_status = 'rescheduled' AND NOT z.mit_call                   THEN 'verschoben'
      WHEN z.calendar_status IN ('cancelled', 'vanished') AND NOT z.mit_call      THEN 'abgesagt'
      WHEN z.calendar_status IN ('active', 'stale_copy') AND z.ohne_teams_meeting THEN 'ohne_teams_meeting'
      WHEN z.calendar_status IN ('active', 'stale_copy') AND z.organizer_is_external
           AND NOT z.mit_call                                                    THEN 'externer_organisator'
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
                                 'externer_organisator', 'zu_viele_interne',
                                 'duplikat_event')) AS eigener_termin
FROM bewertung w
CROSS JOIN heute h
),

heute AS (
  SELECT (now() AT TIME ZONE 'Europe/Berlin')::date AS tag
),

-- Referenz-Zeilen mit den Signalen, die der Abgleich zum Einordnen braucht.
r AS (
  SELECT ref.*,
         m.msgraph_event_id,
         m.calendar_status,
         m.teams_status,
         m.teams_status IN ('external_joined', 'internal_only') AS mit_call,
         (m.msgraph_event_id IS NOT NULL AND coalesce(e.meeting_id, '') = '') AS ohne_teams_meeting,
         -- Regel wie im Block "art" der Referenz-SQL.
         CASE
           WHEN m.calendar_status = 'not_in_calendar' THEN 'crm'
           WHEN m.calendar_status = 'rescheduled'
                AND m.teams_status NOT IN ('external_joined', 'internal_only') THEN 'verschoben'
           WHEN m.calendar_status IN ('cancelled', 'vanished')
                AND m.teams_status NOT IN ('external_joined', 'internal_only') THEN 'abgesagt'
           WHEN m.calendar_status IN ('active', 'stale_copy')
                AND m.msgraph_event_id IS NOT NULL AND coalesce(e.meeting_id, '') = '' THEN 'ohne_teams_meeting'
           WHEN m.calendar_status IN ('active', 'stale_copy') AND m.organizer_is_external
                AND m.teams_status NOT IN ('external_joined', 'internal_only') THEN 'externer_organisator'
           ELSE 'kalender'
         END AS art,
         l.crm_lead_task_id,
         CASE
           WHEN l.crm_outcome IS NOT NULL             THEN l.crm_outcome
           WHEN m.calendar_status = 'not_in_calendar' THEN 'no_outcome'
           ELSE 'no_task'
         END AS crm_outcome,
         ref.eigener_termin                                          AS k_gelegt,
         ref.bewertbar AND ref.eigener_termin                        AS k_bewertbar,
         ref.stattgefunden                                           AS k_stattgefunden,
         ref.bewertbar AND ref.eigener_termin AND NOT ref.stattgefunden AS k_no_show
  FROM ref
  JOIN processed.sales_meetings m ON m.id = ref.meeting_id
  LEFT JOIN processed.sales_meeting_leads l
         ON l.meeting_id = ref.meeting_id AND l.lead_id = ref.lead_id
  LEFT JOIN raw.msgraph_events e ON e.id = m.msgraph_event_id
),

-- unified mit denselben Ableitungen.
u0 AS (
  SELECT u.id AS unified_id,
         u.source,
         u.meeting_key,
         u.event_id,
         u.lead_id,
         u.event_date,
         u.is_no_show,
         u.excluded,
         u.exclusion_reason AS exclusion_reason_roh,
         CASE WHEN u.exclusion_reason = 'rescheduled_without_meeting_id' THEN 'verschoben'
              ELSE u.exclusion_reason END AS exclusion_reason,
         CASE u.meeting_status
           WHEN 'show_up'   THEN 'held'
           WHEN 'no_show'   THEN 'no_show'
           WHEN 'storniert' THEN 'cancelled'
           WHEN 'unbekannt' THEN 'no_outcome'
           ELSE 'no_task'
         END AS crm_outcome,
         CASE WHEN u.source = 'crm_task' THEN substring(u.meeting_key FROM 5) END AS crm_task_id_text
  FROM processed.sales_meetings_unified u
),
u1 AS (
  SELECT u0.*,
         coalesce(u0.event_date < h.tag, false) AS vergangen,
         (u0.exclusion_reason IS NULL
          OR u0.exclusion_reason NOT IN ('verschoben', 'zu_viele_interne', 'duplikat_event')) AS eigener_termin,
         (coalesce(u0.event_date < h.tag, false)
          AND (u0.exclusion_reason IS NULL
               OR u0.exclusion_reason NOT IN ('alt_tenant_join_url', 'online_meeting_nicht_abrufbar',
                                              'crm_storniert'))) AS bewertbar,
         (coalesce(u0.event_date < h.tag, false)
          AND u0.excluded = false
          AND u0.is_no_show IS FALSE) AS stattgefunden
  FROM u0
  CROSS JOIN heute h
),

-- Task-ID aus der CRM-ID, fuer die CRM-Zeilen von heute.
task AS (
  SELECT DISTINCT ON (crm_task_id) crm_task_id::text AS crm_task_id_text, id
  FROM raw.crm_lead_tasks
  ORDER BY crm_task_id, is_deleted, id DESC
),

-- Jede unified-Zeile bekommt ihr Ziel in der Referenz-SQL.
alt AS (
  SELECT u1.*,
         u1.eigener_termin                                              AS k_gelegt,
         u1.bewertbar AND u1.eigener_termin                             AS k_bewertbar,
         u1.stattgefunden                                               AS k_stattgefunden,
         u1.bewertbar AND u1.eigener_termin AND NOT u1.stattgefunden    AS k_no_show,
         coalesce(rk.meeting_id, rc.meeting_id, rz.meeting_id) AS ziel_meeting_id,
         CASE WHEN rk.meeting_id IS NOT NULL THEN rk.lead_id
              WHEN rc.meeting_id IS NOT NULL THEN rc.lead_id
              ELSE rz.lead_id END AS ziel_lead_id,
         t.id IS NOT NULL AS task_gefunden,
         ev.msgraph_event_id IS NOT NULL AS event_in_sales_meetings
  FROM u1
  LEFT JOIN r rk
         ON u1.source = 'msgraph'
        AND rk.msgraph_event_id::text = u1.event_id
        AND coalesce(rk.lead_id, -1) = coalesce(u1.lead_id, -1)
  LEFT JOIN task t ON u1.source = 'crm_task' AND t.crm_task_id_text = u1.crm_task_id_text
  LEFT JOIN r rc ON rc.crm_lead_task_id = t.id
  LEFT JOIN r rz
         ON u1.source = 'crm_task' AND rc.meeting_id IS NULL AND rz.meeting_key = u1.meeting_key
  LEFT JOIN (SELECT msgraph_event_id FROM processed.sales_meetings
             WHERE msgraph_event_id IS NOT NULL) ev
         ON u1.source = 'msgraph' AND ev.msgraph_event_id::text = u1.event_id
),

-- Events, die heute in unified stehen, unabhaengig vom Lead.
alt_events AS (
  SELECT DISTINCT event_id FROM u0 WHERE source = 'msgraph'
),

paar AS (
  SELECT r.meeting_id AS r_meeting_id, r.lead_id AS r_lead_id, r.meeting_date,
         r.calendar_status, r.teams_status, r.mit_call, r.art AS r_art,
         r.crm_outcome AS r_crm_outcome, r.is_no_show AS r_is_no_show,
         r.exclusion_reason AS r_grund, r.stattgefunden AS r_stattgefunden,
         r.bewertbar AS r_bewertbar, r.eigener_termin AS r_eigener_termin,
         r.k_gelegt AS r_gelegt, r.k_bewertbar AS r_k_bewertbar,
         r.k_stattgefunden AS r_k_stattgefunden, r.k_no_show AS r_no_show,
         (ae.event_id IS NOT NULL) AS event_in_unified,
         a.unified_id, a.source, a.event_date, a.crm_outcome AS a_crm_outcome,
         a.is_no_show AS a_is_no_show, a.exclusion_reason AS a_grund,
         a.exclusion_reason_roh, a.stattgefunden AS a_stattgefunden,
         a.bewertbar AS a_bewertbar, a.eigener_termin AS a_eigener_termin,
         a.k_gelegt AS a_gelegt, a.k_bewertbar AS a_k_bewertbar,
         a.k_stattgefunden AS a_k_stattgefunden, a.k_no_show AS a_no_show,
         a.task_gefunden, a.event_in_sales_meetings,
         count(a.unified_id) OVER (PARTITION BY r.meeting_id, r.lead_id) AS n_alt,
         row_number() OVER (PARTITION BY r.meeting_id, r.lead_id ORDER BY a.unified_id) AS rn
  FROM r
  FULL JOIN alt a
         ON a.ziel_meeting_id = r.meeting_id
        AND coalesce(a.ziel_lead_id, -1) = coalesce(r.lead_id, -1)
  LEFT JOIN alt_events ae ON ae.event_id = r.msgraph_event_id::text
),

bef AS (
  SELECT p.*,
    CASE
      WHEN p.unified_id IS NULL THEN 'neu: ' ||
        CASE
          WHEN p.calendar_status = 'not_in_calendar' THEN 'CRM-Zeile, heute verworfen oder anders zugeordnet'
          WHEN p.event_in_unified THEN 'Lead nur in der Referenz-SQL'
          WHEN p.r_art = 'verschoben' THEN 'verschoben ohne Call (Q1)'
          WHEN p.r_art = 'abgesagt' THEN 'abgesagt ohne Call (Q1)'
          WHEN p.r_art = 'ohne_teams_meeting' THEN 'ohne Teams-Meeting (Q9)'
          WHEN p.r_art = 'externer_organisator' THEN 'externer Organisator ohne Call (Q14)'
          ELSE 'Filter auf die verantwortliche Person (Rest Q1)'
        END ||
        CASE WHEN p.r_art NOT IN ('crm', 'kalender') AND p.r_crm_outcome <> 'no_task'
               THEN ', mit CRM-Task (Q10)'
             WHEN p.r_art = 'kalender' AND p.r_crm_outcome <> 'no_task'
               THEN ', mit CRM-Task'
             ELSE '' END
      WHEN p.r_meeting_id IS NULL THEN 'fehlt: ' ||
        CASE
          WHEN p.source = 'msgraph' AND NOT p.event_in_sales_meetings THEN 'Event nicht in sales_meetings'
          WHEN p.source = 'msgraph' THEN 'Lead nur in unified'
          WHEN p.task_gefunden THEN 'CRM-Task heute eigene Zeile, jetzt kein Sieger'
          ELSE 'CRM-Task nicht in raw.crm_lead_tasks'
        END
      WHEN p.n_alt > 1 THEN 'heute mehrfach: ' || p.source
      WHEN p.r_is_no_show IS NOT DISTINCT FROM p.a_is_no_show
       AND p.r_grund IS NOT DISTINCT FROM p.a_grund
       AND p.r_stattgefunden = p.a_stattgefunden
       AND p.r_bewertbar = p.a_bewertbar
       AND p.r_eigener_termin = p.a_eigener_termin THEN
        CASE WHEN p.exclusion_reason_roh = 'rescheduled_without_meeting_id'
             THEN 'gleich, Umbenennung (Q8)' ELSE 'gleich' END
      ELSE 'abweichend: ' ||
        CASE
          WHEN p.source = 'crm_task' AND p.calendar_status <> 'not_in_calendar'
            THEN 'CRM-Task jetzt an einer Kalenderzeile'
          WHEN p.r_crm_outcome <> p.a_crm_outcome
            THEN 'CRM-Ausgang anders (Rangfolge 02.11. oder Zuordnung)'
          -- Grund heute -> Grund in der Referenz-SQL, "kein" fuer keinen Grund.
          WHEN p.r_grund IS DISTINCT FROM p.a_grund
            THEN 'Grund ' || coalesce(p.a_grund, 'kein') || ' -> ' || coalesce(p.r_grund, 'kein')
          WHEN p.r_is_no_show IS DISTINCT FROM p.a_is_no_show
            THEN 'Teams-Messung anders'
          WHEN p.meeting_date IS DISTINCT FROM p.event_date
            THEN 'Termintag anders'
          ELSE 'sonstige'
        END
    END AS befund
  FROM paar p
),

sicht_a AS (
  SELECT 'A' AS sicht,
         befund AS schluessel,
         count(*) FILTER (WHERE r_meeting_id IS NOT NULL AND rn = 1)               AS zeilen_ref,
         count(unified_id)                                                        AS zeilen_alt,
         count(*) FILTER (WHERE r_gelegt AND rn = 1)                              AS gelegt_ref,
         count(*) FILTER (WHERE a_gelegt)                                         AS gelegt_alt,
         count(*) FILTER (WHERE r_k_bewertbar AND rn = 1)                         AS bewertbar_ref,
         count(*) FILTER (WHERE a_k_bewertbar)                                    AS bewertbar_alt,
         count(*) FILTER (WHERE r_k_stattgefunden AND rn = 1)                     AS stattgefunden_ref,
         count(*) FILTER (WHERE a_k_stattgefunden)                                AS stattgefunden_alt,
         count(*) FILTER (WHERE r_no_show AND rn = 1)                             AS no_show_ref,
         count(*) FILTER (WHERE a_no_show)                                        AS no_show_alt
  FROM bef
  GROUP BY befund
),

monat AS (
  SELECT to_char(meeting_date, 'YYYY-MM') AS m, 1 AS ref, 0 AS alt,
         k_gelegt AS gelegt, k_bewertbar AS bewertbar, k_stattgefunden AS stattgefunden,
         k_no_show AS no_show
  FROM r
  UNION ALL
  SELECT to_char(event_date, 'YYYY-MM'), 0, 1,
         k_gelegt, k_bewertbar, k_stattgefunden, k_no_show
  FROM alt
),

sicht_b AS (
  SELECT 'B' AS sicht,
         coalesce(m, 'ohne Termintag') AS schluessel,
         sum(ref)                                                  AS zeilen_ref,
         sum(alt)                                                  AS zeilen_alt,
         count(*) FILTER (WHERE ref = 1 AND gelegt)                AS gelegt_ref,
         count(*) FILTER (WHERE alt = 1 AND gelegt)                AS gelegt_alt,
         count(*) FILTER (WHERE ref = 1 AND bewertbar)             AS bewertbar_ref,
         count(*) FILTER (WHERE alt = 1 AND bewertbar)             AS bewertbar_alt,
         count(*) FILTER (WHERE ref = 1 AND stattgefunden)         AS stattgefunden_ref,
         count(*) FILTER (WHERE alt = 1 AND stattgefunden)         AS stattgefunden_alt,
         count(*) FILTER (WHERE ref = 1 AND no_show)               AS no_show_ref,
         count(*) FILTER (WHERE alt = 1 AND no_show)               AS no_show_alt
  FROM monat
  GROUP BY 2
),

-- C: Grain der Referenz-SQL. Erwartet: zeilen_ref = zeilen_alt.
sicht_c AS (
  SELECT 'C' AS sicht,
         'Grain: zeilen_ref = Zeilen, zeilen_alt = Paare (meeting_id, lead_id)' AS schluessel,
         count(*) AS zeilen_ref,
         count(DISTINCT (meeting_id, coalesce(lead_id, -1))) AS zeilen_alt,
         NULL::bigint, NULL::bigint, NULL::bigint, NULL::bigint,
         NULL::bigint, NULL::bigint, NULL::bigint, NULL::bigint
  FROM ref
)

SELECT *
FROM (SELECT * FROM sicht_a UNION ALL SELECT * FROM sicht_b UNION ALL SELECT * FROM sicht_c) x
ORDER BY sicht,
         CASE WHEN sicht = 'A' THEN -(zeilen_ref + zeilen_alt) END,
         CASE WHEN sicht = 'B' THEN schluessel END DESC;
