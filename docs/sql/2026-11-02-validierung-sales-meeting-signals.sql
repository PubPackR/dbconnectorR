-- Validierung mapping.crm_lead_task_msgraph_event und processed.sales_meeting_signals
-- gegen processed.sales_meetings_unified. Nach dem ersten base-62-Lauf in DBeaver ausfuehren.
-- Spec: docs/specs/2026-11-02-termin-mapping-und-signaltabelle.md, Abschnitt Validierung.

-- 1. Jeder CRM-VC-Task genau einmal im Mapping, Verteilung ueber match_rule.
SELECT match_rule,
       count(*)                                   AS tasks,
       count(msgraph_event_id)                    AS mit_event,
       round(avg(start_diff_minutes), 1)          AS abstand_min_avg,
       max(candidate_count)                       AS kandidaten_max
FROM mapping.crm_lead_task_msgraph_event
GROUP BY match_rule
ORDER BY tasks DESC;

-- 2. meeting_keys: was steht nur in einer der beiden Tabellen?
--    Erwartet: nur_signale = genau die mehrdeutigen Tasks (crm_...), nur_unified = 0.
SELECT 'nur_signale' AS seite, s.match_rule, count(*) AS zeilen
FROM processed.sales_meeting_signals s
LEFT JOIN processed.sales_meetings_unified u USING (meeting_key)
WHERE u.meeting_key IS NULL
GROUP BY s.match_rule
UNION ALL
SELECT 'nur_unified', u.source, count(*)
FROM processed.sales_meetings_unified u
LEFT JOIN processed.sales_meeting_signals s USING (meeting_key)
WHERE s.meeting_key IS NULL
GROUP BY u.source;

-- 3. CRM-Status gegen meeting_status in unified, auf gemeinsamen Zeilen.
--    Abweichungen gehen allein auf die neue Rangfolge zurueck (Q4, Q13, Q14, Q16):
--    unified liest 'storniert' vor allem anderen, die Signaltabelle den spaetesten
--    erkannten Kommentar und ueber Tasks stattgefunden vor No-Show vor storniert.
SELECT u.meeting_status            AS unified,
       s.crm_status                AS signale,
       s.crm_status_conflict,
       count(*)                    AS zeilen
FROM processed.sales_meeting_signals s
JOIN processed.sales_meetings_unified u USING (meeting_key)
WHERE s.has_crm_task
GROUP BY 1, 2, 3
ORDER BY zeilen DESC;

-- 4. Termine mit widerspruechlichen CRM-Signalen, je Task alle Kommentare (Q15).
SELECT s.meeting_key,
       s.event_date,
       s.crm_status                AS gewaehlter_status,
       s.crm_task_count,
       t.crm_task_id,
       t.task_name,
       c.comment_created_at,
       c.comment_name
FROM processed.sales_meeting_signals s
JOIN mapping.crm_lead_task_msgraph_event m
  ON m.msgraph_event_id = s.msgraph_event_id
JOIN raw.crm_lead_tasks t
  ON t.id = m.crm_lead_task_id
 AND t.lead_id = s.lead_id
LEFT JOIN raw.crm_lead_task_comments c
  ON c.task_id = t.id AND NOT c.is_deleted
WHERE s.crm_status_conflict AND s.has_msgraph_event
UNION ALL
SELECT s.meeting_key, s.event_date, s.crm_status, s.crm_task_count,
       t.crm_task_id, t.task_name, c.comment_created_at, c.comment_name
FROM processed.sales_meeting_signals s
JOIN raw.crm_lead_tasks t
  ON t.id = s.crm_lead_task_id
LEFT JOIN raw.crm_lead_task_comments c
  ON c.task_id = t.id AND NOT c.is_deleted
WHERE s.crm_status_conflict AND NOT s.has_msgraph_event
ORDER BY 2 DESC, 1, 5, 7;

-- 5. Ueberblick je System: wie oft sagt welches System was?
SELECT crm_status, outlook_status, call_status, count(*) AS zeilen
FROM processed.sales_meeting_signals
GROUP BY 1, 2, 3
ORDER BY zeilen DESC;
