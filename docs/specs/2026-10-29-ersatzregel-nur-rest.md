# Ersatzregel beim CRM-Termin-Abgleich nur noch für Termine ohne ableitbaren Lead
Asana: https://app.asana.com/0/0/1219978596056611/f · Rahmen: https://app.asana.com/0/0/1219968320061827/f · setzt voraus: https://app.asana.com/0/0/1219978400790427/f (B3)

## Problem

`assemble_unified_meetings()` legt einen CRM-VC-Termin mit einer MSGraph-Platzhalter-Zeile
(Meeting ohne Lead) zusammen, wenn derselbe Rep am selben Tag beides hat und der Abgleich über
Lead × Tag nichts fand. Die Zeile übernimmt dabei den Lead des Tasks nicht, die Uhrzeit zählt nur
als Tiebreak, und welcher Task bei Konkurrenz gewinnt, hängt von der Ladereihenfolge ab.

Gemessen 29.10.2026 (12 Monate, 1.337 Zusammenlegungen per Ersatzregel, Queries
`package-02-kpiR/analytics/2026-10-29-platzhalter-match-sicherheit.sql` und
`2026-10-29-platzhalter-match-vertiefung.sql`):

- 895 bestätigt (Mail, Mapping oder Domain eines Teilnehmers gehören zum Task-Lead), 413
  widersprechen (Teilnehmer gehört einem anderen Lead), 29 neutral.
- Abstand Task zu Meetingbeginn bis 15 min: 36 von 928 widersprechen. 16 bis 30 min: 68 von 76.
  Über 30 min: 309 von 333. Vor und nach dem Umstieg am 26.08.2026 gleich verteilt.
- Von den 895 bestätigten bekämen 849 den Lead auch ohne Ersatzregel: 702 über ein nicht-primäres
  Mapping (B3), 147 über die eindeutige Domain (Domain-Fallback). 46 hängen an der Regel.
- An 218 Rep-Tagen gab es mehr Tasks als Platzhalter. Bei genau einem Platzhalter gewann 99-mal
  der zeitlich fernere Task; „frühester zuerst“ wählte 94-mal anders als „nächster zuerst“.

## Lösung

Reihenfolge in `update_sales_meetings_unified()`:

1. Lead je MSGraph-Meeting aus allen Mail-Mappings (liefert B3).
2. Domain-Fallback (`apply_domain_fallback()`) auf die verbleibenden Platzhalter, **vor**
   `assemble_unified_meetings()`.
3. Abgleich Lead × Tag unverändert.
4. Ersatzregel nur noch für den Rest: Platzhalter-Zeile, gleicher Rep, gleicher Tag, Abstand
   `|event_start - precise_time|` höchstens 15 Minuten. Die Paare eines Rep-Tags werden nach
   Abstand vergeben, das nächste zuerst, bei Gleichstand nach `crm_task_id`. Das Ergebnis hängt
   nicht mehr von der Reihenfolge der Tasks ab.
5. Eine per Ersatzregel gematchte Zeile übernimmt `lead_id` des Tasks und den Schlüssel
   `msgraph_<id>_<lead>`.

## Entscheidungen

- **Q16 Wer gewinnt, wenn zwei Tasks um denselben Platzhalter konkurrieren:** „q16 passt“, also b:
  der zeitlich nächste Task gewinnt, Vergabe aller Paare eines Rep-Tags nach Abstand.
- **Q17 Wie gehen wir mit der Ersatzregel um:** „Weg c“: der Lead wird zuerst aus den Teilnehmern
  abgeleitet (alle Mappings über B3, eindeutige Domain), danach läuft der Abgleich, die
  Ersatzregel bleibt nur für den Rest. Hebt Q10 der Spec `2026-10-28-domain-fallback-lead.md`
  auf: der Domain-Fallback läuft jetzt vor dem Abgleich, die Ausnahme für `crm_override` entfällt.
- **Q18 `meeting_key` einer per Ersatzregel gematchten Zeile:** „q18 passt“: `msgraph_<id>_<task-lead>`.
- **Q19 Dokumentation:** „q19 passt“: ein Satz in `CONTEXT.md` unter Meeting row, kein ADR.
- **Q21 Was passiert mit dem Rest ohne ableitbaren Lead:** „okay, passt“, also a: die Ersatzregel
  bleibt für den Rest, aber nur bis 15 Minuten Abstand; die Zeile übernimmt den Lead des Tasks.
- **Q22 Schnitt B2 und B3:** „okay, passt“: B3 kommt zuerst als eigener PR, B2 baut darauf auf und
  trägt B3 in Asana als Abhängigkeit.

## Betroffen

- `R/sales_meetings_unified.R`: `assemble_unified_meetings()` (Ersatzregel mit Zeitfenster,
  Vergabe nach Abstand, Lead-Übernahme), `update_sales_meetings_unified()` (Domain-Fallback vor
  den Abgleich), `apply_domain_fallback()` (Ausnahme `crm_override` entfällt).
- `CONTEXT.md`: Satz unter Meeting row.
- `tests/testthat/test-sales_meetings_unified.R`.
- Geschrieben: `processed.sales_meetings_unified`; in base-62 daraus neu gebaut
  `shiny.sales_kpi_termine_ereignisse` und `shiny.sales_kpi_sdr_termintage`.
- Folgewirkung in kpiR (kein Code): Termin- und VC-Zahlen verschieben sich rückwirkend, ebenso
  die Übernahme durch SDR-Termintage (ADR 0019) und die erste VC je Lead für Closes.

## Out-of-Scope

- Der Wegfall von `is_primary_crm` selbst (B3, eigenes Ticket, Voraussetzung).
- Domains mit mehreren Leads (Rahmenticket Q9), Freemail und unbekannte Domains (Q7).
- Der Abgleich Lead × Tag: dort überschreibt bei mehreren Tasks weiter der letzte Status, Tool
  und Typ. Nicht gemessen, nicht Teil dieser Änderung.
- Löschen von Meetings ohne Vertriebsbezug oder vor dem 26.08.2026 (Rahmenticket B11).

## Validierung

- Vor und nach Deploy und Lauf: `platzhalter-match-sicherheit.sql` Teil 1 zeigt keine Paare über
  15 Minuten mehr, und `SELECT count(*) FROM processed.sales_meetings_unified WHERE lead_id IS NULL
  AND no_show_source = 'crm_override'` ist 0.
- `SELECT source, count(*), count(*) FILTER (WHERE lead_id IS NULL) FROM
  processed.sales_meetings_unified GROUP BY 1` vorher und nachher: `crm_task` steigt um die
  getrennten Paare (Größenordnung einige hundert), `msgraph` ohne Lead sinkt.
- Tests: gleiche Eingabe mit gemischter Task-Reihenfolge liefert identisches Ergebnis; ein Task
  mit 16 Minuten Abstand bleibt netto-neu; ein Platzhalter mit eindeutiger Domain wird vor dem
  Abgleich zugeordnet und dann über Lead × Tag gefunden.
