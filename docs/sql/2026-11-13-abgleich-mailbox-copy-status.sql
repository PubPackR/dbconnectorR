-- Abgleich processed.sales_meetings.mailbox_copy_status nach dem ersten Nachtlauf mit der
-- dbconnectorR-Version, die die Spalte schreibt.
-- Lesend, in DBeaver gegen Production. Spec: docs/specs/2026-11-13-mailbox-copy-status.md (Validierung).
-- Asana: https://app.asana.com/0/0/1220345738207738/f
-- Zeitstempel ohne Zeitzone tragen UTC: der Berliner Tag braucht
-- AT TIME ZONE 'UTC' AT TIME ZONE 'Europe/Berlin'. meeting_date und booked_date sind schon
-- Berliner Tage.


-- A: Verteilung je Wert und Quelle des Legedatums. Erwartet: nur Kalenderzeilen haben einen
-- Wert, has_earlier_original und suspected tragen outlook, evidenced meist crm_task (outlook
-- nur, wenn die ical_uid aelter ist als der Task, Q17).
SELECT coalesce(m.mailbox_copy_status, '(leer)')                     AS mailbox_copy_status,
       m.booked_date_source,
       count(*)                                                      AS zeilen,
       count(*) FILTER (WHERE m.msgraph_event_id IS NULL)            AS crm_zeilen,
       min(m.meeting_date)                                           AS erster_termin,
       max(m.meeting_date)                                           AS letzter_termin
FROM processed.sales_meetings m
GROUP BY 1, 2
ORDER BY 1, 2;


-- B: Ueberschneidung mit der veralteten Kopie (Subtask 2). Zeilenweise: wie viele
-- stale_copy-Zeilen tragen einen Umzugsstatus. Je ical_uid: wie viele ical_uids haben eine
-- stale_copy-Zeile und eine Zeile mit Umzugsstatus.
SELECT coalesce(mailbox_copy_status, '(leer)') AS mailbox_copy_status,
       count(*)                                AS stale_copy_zeilen
FROM processed.sales_meetings
WHERE calendar_status = 'stale_copy'
GROUP BY 1
ORDER BY 2 DESC;

SELECT count(*) AS uids_mit_beidem
FROM (
  SELECT e.msgraph_ical_uid
  FROM processed.sales_meetings m
  JOIN raw.msgraph_events e ON e.id = m.msgraph_event_id
  GROUP BY e.msgraph_ical_uid
  HAVING bool_or(m.calendar_status = 'stale_copy') AND bool_or(m.mailbox_copy_status IS NOT NULL)
) u;


-- C: Abgleich mit der festen Liste der Sales-Challenge, je Kalendertermin.
-- Erwartete Entsprechung (die Liste rechnet mit dem Fenster ab 01.09., das Signal ohne):
--   original_vor_fenster -> has_earlier_original, oder leer ohne Verdacht (Q2)
--   gerettet             -> has_earlier_original (Original ab 01.09.) oder evidenced
--   verdacht             -> suspected, oder evidenced bei einem Task vor dem 01.09.
-- Abweichungen darueber hinaus kommen aus der eigenen CRM-Zuordnung des Producers statt
-- mapping.crm_lead_task_msgraph_event (Q5) oder aus Kopien, die nach dem 31.10. angelegt
-- wurden (Q3).
SELECT l.status                                                       AS liste,
       CASE WHEN m.meeting_key IS NULL THEN '(nicht in sales_meetings)'
            ELSE coalesce(m.mailbox_copy_status, '(leer)') END        AS signal,
       count(*)                                                       AS termine
FROM processed.sales_challenge_umzugskopie l
LEFT JOIN processed.sales_meetings m ON m.msgraph_event_id = l.msgraph_event_id
GROUP BY 1, 2
ORDER BY 1, 3 DESC;

-- C2: Umgekehrt: Signal im Fenster der Challenge (Kopie ab 01.09. angelegt, Termin bis
-- 31.12.), das nicht in der Liste steht. Die Liste kennt nur Kalendertermine der
-- Termin-Ereignisse aus shiny.sales_kpi_termine_ereignisse.
SELECT m.mailbox_copy_status,
       count(*) AS termine
FROM processed.sales_meetings m
JOIN raw.msgraph_events e ON e.id = m.msgraph_event_id
LEFT JOIN processed.sales_challenge_umzugskopie l ON l.msgraph_event_id = m.msgraph_event_id
WHERE m.mailbox_copy_status IS NOT NULL
  AND l.msgraph_event_id IS NULL
  AND (e.event_created_at AT TIME ZONE 'UTC' AT TIME ZONE 'Europe/Berlin')::date >= DATE '2026-09-01'
  AND m.meeting_date <= DATE '2026-12-31'
GROUP BY 1
ORDER BY 2 DESC;


-- D: Ausserhalb des Challenge-Fensters (Subtask 3). Erwartet: angelegt_vor_0109 > 0 (der
-- Umzug begann Ende August) und Termine vor dem 01.09. bzw. nach dem 31.12. vorhanden.
SELECT m.mailbox_copy_status,
       count(*)                                                                    AS zeilen,
       count(*) FILTER (WHERE (e.event_created_at AT TIME ZONE 'UTC'
                               AT TIME ZONE 'Europe/Berlin')::date < DATE '2026-09-01') AS angelegt_vor_0109,
       count(*) FILTER (WHERE m.meeting_date < DATE '2026-09-01')                  AS termin_vor_0109,
       count(*) FILTER (WHERE m.meeting_date > DATE '2026-12-31')                  AS termin_nach_3112
FROM processed.sales_meetings m
JOIN raw.msgraph_events e ON e.id = m.msgraph_event_id
WHERE m.mailbox_copy_status IS NOT NULL
GROUP BY 1
ORDER BY 1;


-- E: Legedatum belegter Kopien (Subtask 4). Erwartet: ueber_kopie = 0, denn der Task und
-- eine aeltere ical_uid liegen beide vor der Kopie. ersatz_mit_crm zaehlt Ersatztermine,
-- die das CRM-Datum ihres Vorgaengers geerbt haben (Q16).
SELECT count(*) FILTER (WHERE m.mailbox_copy_status = 'evidenced')            AS belegt,
       count(*) FILTER (WHERE m.mailbox_copy_status = 'evidenced'
                          AND m.booked_date_source = 'crm_task')              AS belegt_crm_datum,
       count(*) FILTER (WHERE m.mailbox_copy_status = 'evidenced'
                          AND m.booked_date > (e.event_created_at AT TIME ZONE 'UTC'
                                               AT TIME ZONE 'Europe/Berlin')::date) AS ueber_kopie,
       count(*) FILTER (WHERE m.mailbox_copy_status IS NULL
                          AND m.booked_date_source = 'crm_task'
                          AND m.msgraph_event_id IS NOT NULL)                 AS ersatz_mit_crm
FROM processed.sales_meetings m
LEFT JOIN raw.msgraph_events e ON e.id = m.msgraph_event_id;
