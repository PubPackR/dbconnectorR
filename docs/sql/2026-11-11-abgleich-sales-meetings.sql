-- Abgleich processed.sales_meetings / sales_meeting_leads nach dem ersten Nachtlauf.
-- Lesend, in DBeaver gegen Production. Spec: docs/specs/2026-11-11-sales-meetings.md (Validierung).
-- Asana: https://app.asana.com/0/0/1220345592707180/f
-- Zeitstempel ohne Zeitzone tragen UTC: der Berliner Tag braucht
-- AT TIME ZONE 'UTC' AT TIME ZONE 'Europe/Berlin'.


-- A: Grain. records = keys, und je Kalenderzeile hoechstens ein Record.
SELECT count(*)                                              AS records,
       count(DISTINCT meeting_key)                           AS keys,
       count(DISTINCT msgraph_event_id)                      AS kalenderzeilen,
       count(*) FILTER (WHERE msgraph_event_id IS NOT NULL)  AS kalender_records,
       count(*) FILTER (WHERE msgraph_event_id IS NULL)      AS crm_records
FROM processed.sales_meetings;


-- B: Startmenge. Kalenderzeilen mit mindestens einem externen Teilnehmer (Adresse mit @,
-- nicht intern laut INTERNAL_EMAIL_PATTERNS, kein synthetischer Gast) gegen die Records.
-- Erwartet: fehlt = 0 und zusaetzlich = 0.
WITH extern AS (
  SELECT DISTINCT p.event_id
  FROM raw.msgraph_event_participants p
  JOIN raw.msgraph_contacts c ON c.id = p.contact_id
  WHERE c.email LIKE '%@%'
    AND c.email !~* '(@studyflix\.de|/o=exchangelabs|@bertelsmann\.|@ausbildung\.de|@vocanto\.de|@ad-alliance\.de|@arvato|@rtl\.de|@rtl\.com|@rtl2\.de|@embrace\.)'
    AND c.email !~* '@external\.(guest|msgraph)$'
)
SELECT CASE WHEN m.msgraph_event_id IS NULL THEN 'fehlt in sales_meetings'
            WHEN e.event_id IS NULL         THEN 'zusaetzlich in sales_meetings'
            ELSE 'gleich' END AS befund,
       count(*) AS kalenderzeilen
FROM extern e
FULL JOIN (SELECT msgraph_event_id FROM processed.sales_meetings
           WHERE msgraph_event_id IS NOT NULL) m ON m.msgraph_event_id = e.event_id
GROUP BY 1
ORDER BY 2 DESC;


-- C: Leads-Grain. Erwartet: zeilen = paare.
SELECT count(*) AS zeilen, count(DISTINCT (meeting_id, lead_id)) AS paare
FROM processed.sales_meeting_leads;


-- D: Abgleich je Event gegen processed.sales_meeting_signals. Jede Event-ID dort muss in
-- sales_meetings stehen; Abweichungen bei Teams-Status und Legedatum nach Befund gezaehlt.
-- Erwartbare Gruende: vanished_at als Absagezeitpunkt, Ersatz-Erkennung ohne Schwelle (Q2),
-- Bookings-Zeilen nicht mehr als Kandidaten (Q1).
WITH sig AS (
  SELECT DISTINCT ON (msgraph_event_id)
         msgraph_event_id, call_status, original_created_at
  FROM processed.sales_meeting_signals
  WHERE msgraph_event_id IS NOT NULL
  ORDER BY msgraph_event_id, id
), j AS (
  SELECT s.msgraph_event_id,
         m.id AS meeting_id,
         CASE s.call_status
              WHEN 'in_zukunft'     THEN 'future'
              WHEN 'extern_call'    THEN 'external_joined'
              WHEN 'intern_call'    THEN 'internal_only'
              WHEN 'alt_tenant'     THEN 'old_tenant'
              WHEN 'nicht_abrufbar' THEN 'not_retrievable'
              WHEN 'ohne_link'      THEN 'no_link'
              ELSE s.call_status END                                               AS teams_alt,
         m.teams_status                                                             AS teams_neu,
         (s.original_created_at AT TIME ZONE 'UTC' AT TIME ZONE 'Europe/Berlin')::date AS gelegt_alt,
         m.booked_date                                                              AS gelegt_neu
  FROM sig s
  LEFT JOIN processed.sales_meetings m ON m.msgraph_event_id = s.msgraph_event_id
)
SELECT CASE WHEN meeting_id IS NULL                       THEN 'fehlt in sales_meetings'
            WHEN teams_alt IS DISTINCT FROM teams_neu
             AND gelegt_alt IS DISTINCT FROM gelegt_neu   THEN 'teams_status und booked_date weichen ab'
            WHEN teams_alt IS DISTINCT FROM teams_neu     THEN 'teams_status weicht ab'
            WHEN gelegt_alt IS DISTINCT FROM gelegt_neu   THEN 'booked_date weicht ab'
            ELSE 'gleich' END AS befund,
       count(*) AS events
FROM j
GROUP BY 1
ORDER BY 2 DESC;


-- D2: welche Teams-Werte sich unterscheiden.
WITH sig AS (
  SELECT DISTINCT ON (msgraph_event_id) msgraph_event_id, call_status
  FROM processed.sales_meeting_signals
  WHERE msgraph_event_id IS NOT NULL
  ORDER BY msgraph_event_id, id
)
SELECT s.call_status AS call_status_alt, m.teams_status AS teams_status_neu, count(*) AS events
FROM sig s
JOIN processed.sales_meetings m ON m.msgraph_event_id = s.msgraph_event_id
GROUP BY 1, 2
ORDER BY 3 DESC;


-- E: CRM-Ausgang je Event und Lead gegen sales_meeting_signals.
WITH alt AS (
  SELECT msgraph_event_id, lead_id,
         CASE crm_status WHEN 'show_up'      THEN 'held'
                         WHEN 'storniert'    THEN 'cancelled'
                         WHEN 'ohne_ausgang' THEN 'no_outcome'
                         WHEN 'kein_task'    THEN 'no_task'
                         ELSE crm_status END AS crm_alt
  FROM processed.sales_meeting_signals
  WHERE msgraph_event_id IS NOT NULL AND lead_id IS NOT NULL
), neu AS (
  SELECT m.msgraph_event_id, l.lead_id, l.crm_outcome AS crm_neu
  FROM processed.sales_meeting_leads l
  JOIN processed.sales_meetings m ON m.id = l.meeting_id
  WHERE m.msgraph_event_id IS NOT NULL
)
SELECT alt.crm_alt, coalesce(neu.crm_neu, '(keine Lead-Zeile)') AS crm_neu, count(*) AS paare
FROM alt
LEFT JOIN neu ON neu.msgraph_event_id = alt.msgraph_event_id AND neu.lead_id = alt.lead_id
GROUP BY 1, 2
ORDER BY 3 DESC;


-- F: CRM-only-Zeilen. "nur alt" sind Tasks, die jetzt eine Kalenderzeile finden, weil die
-- Startmenge groesser ist (Q1). "nur neu" waren im Handlauf vom 12.11.2026 Tasks an
-- veralteten Kopien (Q1) und Tasks, die mit einer frueheren Fassung desselben Termins
-- konkurrierten (Q21). Gruende je Zeile: G und H.
SELECT CASE WHEN s.meeting_key IS NULL THEN 'nur neu'
            WHEN m.meeting_key IS NULL THEN 'nur alt'
            ELSE 'beide' END AS befund,
       count(*) AS crm_zeilen
FROM (SELECT DISTINCT meeting_key FROM processed.sales_meeting_signals
      WHERE meeting_key LIKE 'crm\_%') s
FULL JOIN (SELECT meeting_key FROM processed.sales_meetings
           WHERE meeting_key LIKE 'crm\_%') m ON m.meeting_key = s.meeting_key
GROUP BY 1
ORDER BY 2 DESC;


-- G: Gruende der Abweichungen aus D, E und F, ein Ergebnis: abfrage | befund | anzahl.
-- Erwartbar: D "hat Vorgaenger | neu frueher" (Q2, Q4) und "bookings_start" (Q10);
-- E und F "Task ... jetzt an anderem Event" oder "jetzt CRM-Zeile" an veralteten Kopien (Q1).
WITH sig_ev AS (
  SELECT DISTINCT ON (msgraph_event_id) msgraph_event_id, original_created_at
  FROM processed.sales_meeting_signals
  WHERE msgraph_event_id IS NOT NULL
  ORDER BY msgraph_event_id, id
), vorgaenger AS (
  SELECT DISTINCT successor_meeting_id AS id
  FROM processed.sales_meetings
  WHERE successor_meeting_id IS NOT NULL
), d AS (
  SELECT 'D booked_date'::text AS abfrage,
         concat_ws(' | ', m.booked_date_source,
                   coalesce(m.booking_role, 'ohne booking_role'),
                   m.calendar_status,
                   CASE WHEN v.id IS NOT NULL THEN 'hat Vorgaenger' ELSE 'ohne Vorgaenger' END,
                   CASE WHEN m.booked_date IS NULL THEN 'neu leer'
                        WHEN s.original_created_at IS NULL THEN 'alt leer'
                        WHEN m.booked_date < (s.original_created_at AT TIME ZONE 'UTC' AT TIME ZONE 'Europe/Berlin')::date
                          THEN 'neu frueher' ELSE 'neu spaeter' END) AS befund
  FROM sig_ev s
  JOIN processed.sales_meetings m ON m.msgraph_event_id = s.msgraph_event_id
  LEFT JOIN vorgaenger v ON v.id = m.id
  WHERE (s.original_created_at AT TIME ZONE 'UTC' AT TIME ZONE 'Europe/Berlin')::date
        IS DISTINCT FROM m.booked_date
), neu_paar AS (
  SELECT m.msgraph_event_id, l.lead_id, l.crm_outcome, l.crm_lead_task_id
  FROM processed.sales_meeting_leads l
  JOIN processed.sales_meetings m ON m.id = l.meeting_id
  WHERE m.msgraph_event_id IS NOT NULL
), e_abw AS (
  SELECT a.msgraph_event_id, a.crm_lead_task_id AS task_alt,
         CASE a.crm_status WHEN 'show_up' THEN 'held' WHEN 'storniert' THEN 'cancelled'
                           WHEN 'ohne_ausgang' THEN 'no_outcome' WHEN 'kein_task' THEN 'no_task'
                           ELSE a.crm_status END AS crm_alt,
         n.crm_outcome AS crm_neu, n.crm_lead_task_id AS task_neu
  FROM processed.sales_meeting_signals a
  LEFT JOIN neu_paar n ON n.msgraph_event_id = a.msgraph_event_id AND n.lead_id = a.lead_id
  WHERE a.msgraph_event_id IS NOT NULL AND a.lead_id IS NOT NULL
), e AS (
  SELECT 'E crm_outcome'::text AS abfrage,
         concat_ws(' | ', x.crm_alt || ' -> ' || coalesce(x.crm_neu, 'keine Lead-Zeile'),
                   coalesce(me.booking_role, 'ohne booking_role'),
                   me.calendar_status,
                   CASE
                     WHEN x.task_alt IS NOT NULL AND x.task_alt IS DISTINCT FROM x.task_neu THEN
                       CASE WHEN EXISTS (SELECT 1 FROM raw.crm_lead_tasks t
                                         JOIN processed.sales_meetings c ON c.meeting_key = 'crm_' || t.crm_task_id
                                         WHERE t.id = x.task_alt)
                              THEN 'Task alt jetzt CRM-Zeile'
                            WHEN EXISTS (SELECT 1 FROM neu_paar n2
                                         WHERE n2.crm_lead_task_id = x.task_alt
                                           AND n2.msgraph_event_id <> x.msgraph_event_id)
                              THEN 'Task alt jetzt an anderem Event'
                            ELSE 'Task alt nirgends Sieger' END
                     WHEN x.task_alt IS NULL AND x.task_neu IS NOT NULL THEN
                       CASE WHEN EXISTS (SELECT 1 FROM processed.sales_meeting_signals s2
                                         WHERE s2.crm_lead_task_id = x.task_neu AND s2.msgraph_event_id IS NULL)
                              THEN 'Task neu war alt CRM-Zeile'
                            WHEN EXISTS (SELECT 1 FROM processed.sales_meeting_signals s2
                                         WHERE s2.crm_lead_task_id = x.task_neu
                                           AND s2.msgraph_event_id <> x.msgraph_event_id)
                              THEN 'Task neu war alt an anderem Event'
                            ELSE 'Task neu war alt nirgends' END
                     ELSE 'gleicher Task' END) AS befund
  FROM e_abw x
  JOIN processed.sales_meetings me ON me.msgraph_event_id = x.msgraph_event_id
  WHERE x.crm_alt IS DISTINCT FROM x.crm_neu
), f_alt AS (
  SELECT DISTINCT meeting_key FROM processed.sales_meeting_signals WHERE meeting_key LIKE 'crm\_%'
), f_neu AS (
  SELECT meeting_key FROM processed.sales_meetings WHERE meeting_key LIKE 'crm\_%'
), f_alt_ziel AS (
  SELECT a.meeting_key,
         bool_or(t.id IS NULL)   AS task_fehlt,
         bool_and(t.is_deleted)  AS task_geloescht,
         min(m.calendar_status)  AS status_neu
  FROM f_alt a
  LEFT JOIN f_neu n USING (meeting_key)
  LEFT JOIN raw.crm_lead_tasks t ON t.crm_task_id::text = substr(a.meeting_key, 5)
  LEFT JOIN processed.sales_meeting_leads l ON l.crm_lead_task_id = t.id
  LEFT JOIN processed.sales_meetings m ON m.id = l.meeting_id AND m.msgraph_event_id IS NOT NULL
  WHERE n.meeting_key IS NULL
  GROUP BY a.meeting_key
), f AS (
  SELECT 'F nur alt'::text AS abfrage,
         CASE WHEN status_neu IS NOT NULL THEN 'Task jetzt an Kalenderzeile, ' || status_neu
              WHEN task_fehlt THEN 'Task fehlt in raw.crm_lead_tasks'
              WHEN task_geloescht THEN 'Task geloescht'
              ELSE 'Task nirgends Sieger' END AS befund
  FROM f_alt_ziel
  UNION ALL
  SELECT 'F nur neu'::text,
         CASE WHEN min(m2.calendar_status) IS NOT NULL
                THEN 'Task alt an Kalenderzeile, ' || coalesce(min(m2.booking_role), 'ohne booking_role')
                     || ', ' || min(m2.calendar_status)
              ELSE 'Task alt nirgends' END
  FROM f_neu n
  LEFT JOIN f_alt a USING (meeting_key)
  JOIN raw.crm_lead_tasks t ON t.crm_task_id::text = substr(n.meeting_key, 5)
  LEFT JOIN processed.sales_meeting_signals s ON s.crm_lead_task_id = t.id
  LEFT JOIN processed.sales_meetings m2 ON m2.msgraph_event_id = s.msgraph_event_id
  WHERE a.meeting_key IS NULL
  GROUP BY n.meeting_key
)
SELECT abfrage, befund, count(*) AS anzahl FROM d GROUP BY 1, 2
UNION ALL
SELECT abfrage, befund, count(*) FROM e GROUP BY 1, 2
UNION ALL
SELECT abfrage, befund, count(*) FROM f GROUP BY 1, 2
ORDER BY abfrage, anzahl DESC;


-- H: Tasks, die in sales_meeting_signals an einem Event hingen und jetzt woanders stehen,
-- mit den Kandidaten, die match_crm_tasks() fuer sie sieht (gleicher Lead, gleicher
-- Berliner Tag, ohne Bookings-Zeilen und veraltete Kopien). Erwartet nach Q21: keine Zeile
-- mit status_alt_event = 'active', aktive_kandidaten = 1 und wo 'jetzt CRM-Zeile' oder
-- 'jetzt an anderem Event'.
WITH betroffen AS (
  SELECT DISTINCT s.crm_lead_task_id AS task_id, s.msgraph_event_id AS event_alt
  FROM processed.sales_meeting_signals s
  WHERE s.msgraph_event_id IS NOT NULL
    AND s.crm_lead_task_id IS NOT NULL
    AND NOT EXISTS (SELECT 1
                    FROM processed.sales_meeting_leads l
                    JOIN processed.sales_meetings m ON m.id = l.meeting_id
                    WHERE l.crm_lead_task_id = s.crm_lead_task_id
                      AND m.msgraph_event_id = s.msgraph_event_id)
), task AS (
  SELECT b.task_id, b.event_alt, t.crm_task_id, t.lead_id,
         (t.precise_time AT TIME ZONE 'UTC' AT TIME ZONE 'Europe/Berlin')::date AS task_tag
  FROM betroffen b
  JOIN raw.crm_lead_tasks t ON t.id = b.task_id
), kand AS (
  SELECT k.task_id,
         count(DISTINCT m.id)                                              AS kandidaten,
         count(DISTINCT m.id) FILTER (WHERE m.calendar_status = 'active')  AS aktive_kandidaten,
         string_agg(DISTINCT m.calendar_status, ',')                       AS stati,
         count(DISTINCT m.starts_at_utc)                                   AS startzeiten,
         bool_or(m.msgraph_event_id = k.event_alt)                         AS alt_event_kandidat
  FROM task k
  LEFT JOIN processed.sales_meeting_leads l
         ON l.lead_id = k.lead_id AND l.lead_match_source IN ('mapping', 'domain')
  LEFT JOIN processed.sales_meetings m
         ON m.id = l.meeting_id
        AND m.msgraph_event_id IS NOT NULL
        AND m.meeting_date = k.task_tag
        AND m.booking_role IS NULL
        AND m.calendar_status <> 'stale_copy'
  GROUP BY k.task_id
), verbleib AS (
  SELECT k.task_id,
         CASE
           WHEN EXISTS (SELECT 1 FROM processed.sales_meetings c
                        WHERE c.meeting_key = 'crm_' || k.crm_task_id)
             THEN 'jetzt CRM-Zeile'
           WHEN EXISTS (SELECT 1 FROM processed.sales_meeting_leads l2
                        JOIN processed.sales_meetings m2 ON m2.id = l2.meeting_id
                        WHERE l2.crm_lead_task_id = k.task_id AND m2.msgraph_event_id IS NOT NULL)
             THEN 'jetzt an anderem Event'
           ELSE 'kein Sieger'
         END AS wo
  FROM task k
)
SELECT v.wo,
       me.calendar_status                     AS status_alt_event,
       ka.kandidaten,
       ka.aktive_kandidaten,
       ka.stati,
       ka.startzeiten,
       coalesce(ka.alt_event_kandidat, false) AS alt_event_kandidat,
       count(*)                               AS tasks
FROM task k
JOIN verbleib v ON v.task_id = k.task_id
JOIN kand ka ON ka.task_id = k.task_id
JOIN processed.sales_meetings me ON me.msgraph_event_id = k.event_alt
GROUP BY 1, 2, 3, 4, 5, 6, 7
ORDER BY tasks DESC;
