# Umzugskopien als Signal: processed.sales_meetings.mailbox_copy_status
Ticket: https://app.asana.com/1/734700742714256/project/1211924185203938/task/1220345738207738 · Vorgänger: [2026-11-11-sales-meetings.md](2026-11-11-sales-meetings.md) · Vorbild: shiny-99-modules `docs/specs/2026-11-03-sales-challenge-umzugskopien.md`

## Problem

Der Postfach-Umzug in den neuen Tenant hat Kalendertermine unter neuer iCal UID neu angelegt (*Mailbox-move copy*, `CONTEXT.md`). Eine Kopie trägt den Umzugstag als Anlage, obwohl der Termin oft lange vorher vereinbart wurde. `inherit_moved_copy_created_at()` (#62) gibt einer Kopie das Datum ihres Originals, findet ein Original aber nur über einen gemeinsamen externen Teilnehmer. Für die Sales-Challenge gibt es deshalb die feste Liste `processed.sales_challenge_umzugskopie`: einmal befüllt, nur für das Fenster der Challenge, ohne Producer. RevOps braucht die Erkennung als normales Signal an jedem Meeting record (Konzept „RevOps-Datenumzug“: die Liste geht in `mailbox_copy_status` auf).

## Lösung

`update_sales_meetings()` schreibt eine neue Spalte `processed.sales_meetings.mailbox_copy_status` mit. Die Regeln stehen in zwei reinen Funktionen in `R/msgraph_event_detection.R`, `build_sales_meetings()` ruft sie auf (Q1).

**Verdacht.** Geprüft wird jede Kalenderzeile außer `booking:`-Einträgen, auch abgesagte, verschwundene, `stale_copy` und Kalenderkopien; CRM-Zeilen bleiben leer (Q9). Eine Zeile steht unter Verdacht, wenn ihre Graph-Anlage (`event_created_at`) im Umzugszeitraum liegt, ab 01.08.2026 00:00 bis vor 01.11.2026 00:00 UTC (Q3, Q15, Konstanten `MAILBOX_MOVE_FROM` und `MAILBOX_MOVE_TO`), und wenn sie

- aus einer Umzugs-Sitzung stammt (`detect_move_sessions()`), oder
- bei ihrer Anlage schon vorbei war: der Berliner Termintag liegt vor dem Berliner Tag der eigenen Graph-Anlage (Q10).

Umzugs-Sitzung, wie im Befüllungsskript der Challenge (Q10): Anlagen derselben Person im Umzugszeitraum, über alle `raw.msgraph_events` ohne `booking:`, interne Termine eingeschlossen. Die Person ist der Teilnehmer mit `is_organizer` und der kleinsten `contact_id`, gleich welche Adresse. Ein Abstand über 30 Minuten beginnt eine neue Sitzung. Eine Sitzung zählt, wenn mindestens zwei ihrer Termine über einen Tag vor ihrer Anlage begonnen hatten. Dann stehen alle ihre Zeilen unter Verdacht.

**Wert.** Von einer Zeile unter Verdacht gilt der erste zutreffende Wert (Q2, Q5):

| Wert | Wann |
|---|---|
| `has_earlier_original` | Die ical_uid hat ein früheres Original: gleicher Start, gemeinsamer externer Kontakt, andere ical_uid, früher angelegt (Paarung von `inherit_moved_copy_created_at()`). |
| `evidenced` | Der früheste CRM-Task, den der Producer dieser Zeile zuordnet (`match_crm_tasks()`, gelöschte Tasks lädt `load_crm_vc_tasks()` nicht), wurde vor der Kopie angelegt: `task_created_at < event_created_at` (Q15). Ein Task an einer anderen Zeile derselben Kette zählt nicht. |
| `suspected` | sonst |

Ohne Verdacht bleibt die Spalte leer, auch bei einem Original.

**Legedatum.** Eine Zeile mit `evidenced` bekommt als `booked_date` den Berliner Tag ihres frühesten Tasks und `booked_date_source = 'crm_task'` (Q4). Ist die ical_uid älter als der Task, bleibt wie bei jeder Erbregel das frühere Datum mit der Quelle `outlook` (Q17). Das neue Datum gilt schon für `is_short_lived`, die Personenrollen zum Legezeitpunkt und das Erbe an einen Ersatztermin (Q12). Ein Ersatztermin, der ein CRM-Datum erbt, trägt ebenfalls `crm_task` (Q16). Dafür rechnet `build_sales_meetings()` das Legedatum jetzt nach der CRM-Zuordnung (Schritt 7b statt 3); die Schritte 4 bis 7 brauchen es nicht.

Daraus folgt (Q5, Q9): `stale_copy`-Zeilen und Bookings-Kalenderkopien sind keine Kandidaten von `match_crm_tasks()` und werden deshalb nie `evidenced`.

**Livegang.** DDL `inst/sql/2026-11-13-sales-meetings-mailbox-copy-status.sql` in DBeaver, danach dbconnectorR installieren und die Image-Kette wie bei 0.0.0.9044: Pin in r-runtime (Shiny-0-studyflix-infrastructure), r-corenlp in studyflix-apps, „Deploy folder“ für `msgraph-ingestion` (Q6). Ohne die Spalte bricht `update_sales_meetings()` über `pruefe_zielspalten()` ab.

## Entscheidungen

Die Empfehlungen zu Q1 bis Q11 hat der Nutzer am 13.11.2026 mit „passt alles“ bestätigt, Q12 bis Q16 und das Zielbild ebenfalls mit „passt alles“, Q17 mit „passt“.

- **Q1 Wo entsteht das Signal?:** passt alles. (a) Eine reine Funktion `detect_mailbox_copies()` in `R/msgraph_event_detection.R`, aufgerufen in `build_sales_meetings()`; `update_sales_meetings()` schreibt die Spalte mit, wie `organizer_is_external` in #69. Kein eigener Job.
- **Q2 Wann trägt eine Zeile `has_earlier_original`?:** passt alles. (b) Nur wenn sie außerdem unter Verdacht steht. Die drei Werte sind die drei Ausgänge eines Verdachts.
- **Q3 Zeitraum des Umzugs:** passt alles. (a) Beide Verdachtsgründe gelten nur für Zeilen mit Anlage vom 01.08. bis 31.10.2026, als feste Konstante.
- **Q4 Legedatum einer belegten Kopie:** passt alles. (b) `booked_date` wird der Berliner Tag des frühesten Tasks, `booked_date_source = 'crm_task'`.
- **Q5 Was belegt eine Kopie?:** passt alles. Ein CRM-Task, den der Producer selbst eindeutig dieser Zeile zuordnet, nicht gelöscht, angelegt vor dem eigenen Graph-`createdDateTime` der Zeile. Keine Fenster-Bedingung. Ein Task an einer anderen Zeile derselben Kette zählt nicht.
- **Q6 Image-Kette im Ticket?:** passt alles. Ja: DDL, PR dbconnectorR, Pin in r-runtime, Digest in r-corenlp, „Deploy folder“, je als Subtask.
- **Q7 Messung und Abgleich:** passt alles. `docs/sql/2026-11-13-abgleich-mailbox-copy-status.sql` mit Verteilung, Überschneidung mit `stale_copy`, Abgleich gegen die Liste und Werten außerhalb des Challenge-Fensters.
- **Q8 Die feste Liste:** passt alles. Liste, Tab und n8n-Abfrage bleiben unverändert; ihr Abschalten gehört nicht in dieses Ticket.
- **Q9 Geprüfte Zeilen:** passt alles. Alle Kalenderzeilen außer `booking:`-Einträgen, auch abgesagte, verschwundene, `stale_copy` und Kalenderkopien; CRM-Zeilen leer; Sitzungen über alle `raw.msgraph_events`.
- **Q10 Regeln wie im Befüllungsskript:** passt alles. Abstände bis 30 Minuten, mindestens zwei Termine über einen Tag vor ihrer Anlage, Organisator ist jede Adresse mit `is_organizer`; „bei Anlage schon vorbei“ heißt Berliner Termintag vor dem Berliner Tag der eigenen Graph-Anlage.
- **Q11 Ablage:** passt alles. dbconnectorR: `R/msgraph_event_detection.R`, `R/sales_meetings.R`, DDL `inst/sql/2026-11-13-sales-meetings-mailbox-copy-status.sql` (`ADD COLUMN IF NOT EXISTS`, englischer Kommentar, kein CHECK), diese Spec, `CONTEXT.md` mit *Mailbox-move copy* und *Move session*.
- **Q12 Wie weit reicht das CRM-Datum?:** passt alles. (a) Die belegte Kopie bekommt das CRM-Datum, bevor `is_short_lived`, die Personenrollen und das Erbe an den Ersatztermin berechnet werden.
- **Q13 Glossar:** passt alles. Die beiden Einträge in `CONTEXT.md` wie vorgeschlagen.
- **Q14 Spaltenkommentare:** passt alles. Wie vorgeschlagen; mit Q16 und Q17 ergänzt.
- **Q15 Zeitgrenzen genau:** passt alles. Graph-Anlage ab 01.08.2026 00:00 bis vor 01.11.2026 00:00; „vor der Kopie“ heißt `task_created_at < event_created_at`, beide mit UTC-Ziffern.
- **Q16 Quelle beim Ersatztermin:** passt alles. `crm_task`: die Quelle folgt dem Datum.
- **Q17 Belegte Kopie, aber die ical_uid war schon früher da:** passt. (a) Das frühere Datum bleibt, mit der Quelle `outlook`.

## Betroffen

| Was | Wo |
|---|---|
| Erkennung | `R/msgraph_event_detection.R`: `MAILBOX_MOVE_FROM`, `MAILBOX_MOVE_TO`, `detect_move_sessions()`, `detect_mailbox_copies()` |
| Producer | `R/sales_meetings.R`: `build_sales_meetings()` (Schritt 7b), `SALES_MEETINGS_SPALTEN` |
| DDL | `inst/sql/2026-11-13-sales-meetings-mailbox-copy-status.sql`: neue Spalte, Kommentar von `booked_date_source` |
| Abgleich | `docs/sql/2026-11-13-abgleich-mailbox-copy-status.sql` |
| Glossar | `CONTEXT.md` |
| Tests | `tests/testthat/test-msgraph_event_detection.R`, `tests/testthat/test-sales_meetings.R` |
| Tabelle | `processed.sales_meetings` (neue Spalte; `booked_date`, `booked_date_source`, `is_short_lived` und die Personenrollen ändern sich für belegte Kopien und ihre Ersatztermine) |

Neue Lesetabellen braucht der Producer nicht.

## Out-of-Scope

- `processed.sales_challenge_umzugskopie`, der Tab der Sales-Challenge und die n8n-Abfrage bleiben bis zum 31.12.2026 unverändert; ihr Abschalten gehört nicht hierher (Q8).
- Termine, die nach dem 31.10.2026 nachträglich eingetragen werden und dabei schon vorbei sind: keine Umzugskopie (Q3). Braucht RevOps sie, ist das ein eigenes Signal.
- Die Bewertung („gestrichen“, „in Prüfung“, Fenster ab 01.09.): RevOps in der Referenz-SQL (Ticket „Bewertungsregeln vom 03.11. in die Referenz-SQL übernehmen“).
- Die Doku für RevOps (Ticket „Process-Doku und Übergabe an Elias“).

## Validierung

Nach dem ersten Nachtlauf mit der neuen Version, alle Abfragen in `docs/sql/2026-11-13-abgleich-mailbox-copy-status.sql`:

1. A: Nur Kalenderzeilen tragen einen Wert. `has_earlier_original` und `suspected` haben die Quelle `outlook`, `evidenced` meist `crm_task`.
2. B: Die Überschneidung mit `stale_copy` ist gezählt, je Zeile und je ical_uid (Subtask 2).
3. C: Die Liste der Challenge findet sich je Kalendertermin im Signal wieder, nach der Entsprechung im Kopf von C. Abweichungen darüber hinaus sind nach Grund benannt.
4. D: Es gibt Werte außerhalb des Challenge-Fensters, also Kopien mit Anlage vor dem 01.09. oder mit Termin vor dem 01.09. bzw. nach dem 31.12. (Subtask 3).
5. E: Keine belegte Kopie hat ein Legedatum nach dem Tag ihrer Kopie (Subtask 4).

Die Abfrage D aus `docs/sql/2026-11-11-abgleich-sales-meetings.sql` (Legedatum gegen `sales_meeting_signals`) zeigt danach zusätzlich die belegten Kopien und ihre Ersatztermine als Abweichung. Das ist gewollt (Q4, Q12).
