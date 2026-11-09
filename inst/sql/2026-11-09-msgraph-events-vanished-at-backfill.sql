-- Backfill raw.msgraph_events.vanished_at fuer abgesagte Kalenderzeilen.
-- NACH 2026-11-09-msgraph-events-vanished-at.sql in DBeaver ausfuehren, Block fuer Block.
-- Backfill und Paket-Installation am selben Tag, ohne Nachtlauf (02:48) dazwischen: Sonst
-- bekommen Zeilen, die in dieser Nacht verschwinden, nie ein vanished_at.
-- Der Kandidaten-Filter steht dreimal (Block 1 zweimal, Block 2). Eine Aenderung gilt fuer alle.
-- Asana: https://app.asana.com/0/0/1220117253528426/f
-- Spec: docs/specs/2026-11-09-vanished-at-und-kalenderzustand.md (Q3, Q4)
--
-- DER WERT IST EINE NAEHERUNG. Den echten Zeitpunkt hat niemand gespeichert.
--   1. created_at der fruehesten spaeter angelegten Zeile derselben ical_uid, egal ob
--      sie heute aktiv oder abgesagt ist. Das ist der Nachtlauf nach der Verschiebung.
--   2. Sonst updated_at der Zeile. Das kann spaeter liegen als das Verschwinden: Die
--      Korrektur vom 29.10.2026 und die Online-Meeting-Suche haben updated_at bewegt.
--
-- Bookings bleiben aussen vor (Q4). Eine Graph-Absage ohne Betreff-Praefix laesst sich
-- rueckwirkend nicht erkennen und bekommt ebenfalls einen Wert.
--
-- created_at und updated_at sind timestamptz, vanished_at ist timestamp in UTC:
-- timezone('UTC', ...) macht daraus die UTC-Uhrzeit. event_start ist UTC ohne Zeitzone,
-- der Stichtag 01.05.2025 gilt als Berliner Tag.
--
-- Das UPDATE zieht ueber trigger_set_updated_at das updated_at der Zeilen auf den Lauf.
-- Es fasst deshalb nur Zeilen mit leerem vanished_at an: Ein zweiter Lauf aendert nichts.

-- ----- Block 1: Vorschau, nur lesend -----
-- Erwartung: Zeilen mit und ohne Nachfolger ergeben zusammen die Kandidaten.
WITH kandidaten AS (
    SELECT e.id, e.msgraph_ical_uid, e.created_at
    FROM raw.msgraph_events e
    WHERE e.is_canceled
      AND e.vanished_at IS NULL
      AND e.msgraph_ical_uid NOT LIKE 'booking:%'
      AND (e.event_start AT TIME ZONE 'UTC' AT TIME ZONE 'Europe/Berlin') >= TIMESTAMP '2025-05-01 00:00:00'
      AND coalesce(e.subject, '') !~ '^(Canceled:|Abgesagt:)'
),
nachfolger AS (
    SELECT k.id, min(n.created_at) AS erster_nachfolger
    FROM kandidaten k
    JOIN raw.msgraph_events n
      ON n.msgraph_ical_uid = k.msgraph_ical_uid
     AND n.id <> k.id
     AND n.created_at > k.created_at
    GROUP BY k.id
)
SELECT count(*)                                                  AS kandidaten,
       count(n.id)                                               AS mit_nachfolger,
       count(*) - count(n.id)                                    AS ohne_nachfolger
FROM kandidaten k
LEFT JOIN nachfolger n ON n.id = k.id;

-- Betreffe, die wie ein weiteres Absage-Praefix aussehen (etwa "Cancelled:"). Die
-- Eintraege bitte ansehen, bevor Block 2 laeuft: Ein echtes Praefix gehoert in den Filter.
SELECT substring(e.subject FROM '^[^:]{1,20}:') AS praefix,
       count(*)                                  AS n
FROM raw.msgraph_events e
WHERE e.is_canceled
  AND e.vanished_at IS NULL
  AND e.msgraph_ical_uid NOT LIKE 'booking:%'
  AND (e.event_start AT TIME ZONE 'UTC' AT TIME ZONE 'Europe/Berlin') >= TIMESTAMP '2025-05-01 00:00:00'
  AND coalesce(e.subject, '') !~ '^(Canceled:|Abgesagt:)'
GROUP BY 1
ORDER BY 2 DESC
LIMIT 30;

-- ----- Block 2: Backfill, schreibt -----
-- Je Kandidat genau eine Zeile: id ist eindeutig, der Nachfolger ist je id aggregiert.
-- Die von DBeaver gemeldete Zeilenzahl muss gleich "kandidaten" aus Block 1 sein. Sie
-- kommt ins Ticket.
WITH kandidaten AS (
    SELECT e.id, e.msgraph_ical_uid, e.created_at, e.updated_at
    FROM raw.msgraph_events e
    WHERE e.is_canceled
      AND e.vanished_at IS NULL
      AND e.msgraph_ical_uid NOT LIKE 'booking:%'
      AND (e.event_start AT TIME ZONE 'UTC' AT TIME ZONE 'Europe/Berlin') >= TIMESTAMP '2025-05-01 00:00:00'
      AND coalesce(e.subject, '') !~ '^(Canceled:|Abgesagt:)'
),
nachfolger AS (
    SELECT k.id, min(n.created_at) AS erster_nachfolger
    FROM kandidaten k
    JOIN raw.msgraph_events n
      ON n.msgraph_ical_uid = k.msgraph_ical_uid
     AND n.id <> k.id
     AND n.created_at > k.created_at
    GROUP BY k.id
)
UPDATE raw.msgraph_events e
SET vanished_at = timezone('UTC', coalesce(n.erster_nachfolger, k.updated_at))
FROM kandidaten k
LEFT JOIN nachfolger n ON n.id = k.id
WHERE e.id = k.id;

-- ----- Block 3: Kontrolle, nur lesend -----
-- Erwartung: mit_wert ist gleich der gemeldeten Zeilenzahl aus Block 2 (plus den
-- Zeilen, die der Ingest seit dem Rollout gesetzt hat), vor_anlage ist 0.
SELECT count(*) FILTER (WHERE e.vanished_at IS NOT NULL)                                    AS mit_wert,
       count(*) FILTER (WHERE e.vanished_at < timezone('UTC', e.created_at))                AS vor_anlage
FROM raw.msgraph_events e;
