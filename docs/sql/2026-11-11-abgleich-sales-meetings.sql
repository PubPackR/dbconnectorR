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


-- F: CRM-only-Zeilen. "nur neu" sind vor allem Tasks, deren Kalenderzeile Bookings-Eintrag
-- oder Kalenderkopie ist (Q1); "nur alt" Tasks, die jetzt eine Kalenderzeile finden, weil die
-- Startmenge groesser ist.
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
