# Lead-Protokolle als zweite Statusquelle: geprüft und verworfen

Asana: https://app.asana.com/1/734700742714256/task/1218210627259100
Vorgänger: https://app.asana.com/1/734700742714256/task/1218179753663016

## Frage

`classify_meeting_status()` liest genau eine Textquelle, `raw.crm_lead_task_comments`.
Der Vertrieb dokumentiert Termine aber oft im Protokoll-Feed des Leads
(`raw.crm_lead_protocols`). Sollte diese Quelle mitgelesen werden, um
`meeting_status = 'unbekannt'` aufzulösen?

Der Auslöser: Ticket 1218179753663016 hat am 04.09.2026 entschieden, dass
`crm_unbekannt` im Nenner der Show-Up-Quote bleibt und als No-Show gilt —
ausdrücklich unter der Voraussetzung, dass vorher alle Quellen ausgewertet werden.

## Antwort: nein, nicht bauen

Gemessen an belegten MSGraph-Terminen (Kalendertermin am selben Tag beim selben Lead
beweist, dass der Termin existierte und wie er ausging), 09/2025 bis 08/2026.

## Belege

**1. Die Abdeckung reicht nicht.** Auf die Zielmenge angewandt — CRM-Termine mit
`task_badge = 'visit'` und `meeting_status = 'unbekannt'`, Fenster −1 bis +3 Tage,
`protocol_type = 'note'` und `is_user_generated = true` — lösen sich **147 von 1.599
Zeilen (9,2 %)** auf. Der Trend fällt: 11 bis 13 % in 09–11/2025, **3 bis 7 % in
04–09/2026**, zuletzt 3 bis 6 Zeilen im Monat.

**2. Der Gewinn bewegt die Kennzahl um null.** Eine Zeile von `crm_unbekannt` auf
`crm_no_show` zu heben ändert nichts: beide sind `bewertbar` (stehen im Nenner) und
keine ist `stattgefunden` (keine im Zähler). Siehe `termin_flags()` und
`ist_beobachtbarer_termin()` in package-02-kpiR.

**3. „Hat stattgefunden" ist nicht per Stichwort erkennbar.** Wortfrequenzanalyse über
die Notizen, getrennt nach tatsächlichem Ausgang (Basis 31,0 % No-Show-Anteil):

- No-Show-Sprache ist scharf: `absagen` 93,0 %, `taucht` 90,7 %, `absage` 90,0 %,
  `abgesagt` 88,8 %, `show` 85,6 %, `erreichbar` 75,0 %, `leider` 74,7 %.
- Für „stattgefunden" gibt es **kein Vokabular**. Am unteren Ende stehen
  Gesprächsinhalte: `report` 10,1 %, `zahlen` 13,9 %, `tracking` 14,3 %,
  `klicks` 15,0 %, `zwischenreport` 14,1 %. Bei einem gelaufenen Termin dokumentiert
  der Vertrieb *worüber* gesprochen wurde, nicht *dass* gesprochen wurde. Das ist über
  hunderte Wörter verteilt und mit einer Stichwortregel nicht einzusammeln.

**4. Die Existenz einer Notiz taugt nicht als Ersatzsignal — sie zeigt in die falsche
Richtung.** Eine Notiz am Termintag gibt es bei **29,4 %** der No-Shows, aber nur bei
**19,0 %** der stattgefundenen Termine. Ein geplatzter Termin erzeugt Nacharbeit
(nachfassen, neu terminieren, Grund festhalten), ein gelaufener oft nicht.

**5. Die bestehende Kategorie-Ebene ist leer.** `processed.crm_lead_events` /
`processed.crm_event_category` führen 21 Kategorien, darunter `sales_video_call` und
`anrufdoku`, aber **null Zeilen** und kein `letztes_event`. Als Quelle nicht verfügbar.
Der Produzent ist in keinem lokal ausgecheckten Repo auffindbar.

## Was damit belegt ist

Die Voraussetzung aus Ticket 1218179753663016 ist eingelöst: die Protokolle wurden
ausgewertet und schließen die Lücke nicht. `crm_unbekannt` heißt damit belegbar
„nirgends dokumentiert" statt „nicht nachgesehen".

**Und der undokumentierte Fall hat überwiegend stattgefunden.** Punkt 4 wird oft falsch
herum gelesen: dass Notizen bei No-Shows häufiger sind, heißt für die *undokumentierten*
Termine das Gegenteil. Aus denselben Zahlen (Fenster 0 bis +3):

| | Termine | mit Notiz | ohne Notiz |
|---|---|---|---|
| stattgefunden | 7.355 | 2.093 | 5.262 |
| No-Show | 3.011 | 1.232 | 1.779 |

Unter den Terminen **ohne** Dokumentation sind **25,3 % No-Shows**, gegen eine
Basisquote von 29,0 % über alle Termine. Ein undokumentierter Termin hat also mit rund
drei Vierteln Wahrscheinlichkeit stattgefunden und ist sogar etwas seltener ein No-Show
als der Durchschnittstermin. Mit den Tag-0-Zahlen gerechnet: 27,7 %, dieselbe Aussage.

Ticket 1218179753663016 hatte `crm_unbekannt` am 04.09.2026 als **No-Show** gelesen.
Diese Messung stützt das **nicht**, sie spricht dagegen. Die Lesart wurde am 05.09.2026
entsprechend gedreht: `crm_unbekannt` ist ein stattgefundener Termin ohne weitere
Dokumentation.

Einschränkung: gerechnet auf MSGraph-verifizierbaren Terminen. `crm_unbekannt`-Zeilen
sind gerade die **ohne** MSGraph-Partner. Es ist eine Übertragung, kein Beweis.

## Grenzen dieser Aussage

- Nur `protocol_type = 'note'` mit `is_user_generated = true`. `email` blieb draußen:
  3.242 Zeichen im Schnitt (ganze Mailverläufe) und ein Verhältnis von 9:1 zugunsten
  `storniert` — das Keyword-Verfahren scheitert dort strukturell.
- Fenster −1 bis +3 Tage. Das Histogramm zeigt den Dokumentations-Peak auf Tag 0
  (Faktor 12 über Grundpegel) mit Ausläufer auf Tag +1; größere Fenster sammeln
  Lead-Aktivität statt Termindokumentation.
- Ein Klassifikator statt Stichwörtern ist **ungeprüft**. Punkt 3 zeigt, dass dort
  Signal liegt — es ist nur mit Regex nicht zu heben.
- Warum die Auflösungsquote über die Zeit fällt, ist nicht gemessen.

## Falls das Thema wiederkommt

Der billigste Hebel wäre nicht eine neue Quelle, sondern die vorhandene Wortliste:
`absage|absagen|abgesagt|taucht` in den No-Show-Zweig von `classify_meeting_status()`
verdoppelt den Recall (8,0 → 16,7 %) und verbessert dabei die Präzision
(72,3 → 81,0 %). Weichere Wörter (`erreichbar`, `mehrmals`, `krank`, `leider`) kosten
mehr Präzision als sie Recall bringen. Bewegt die Kennzahl trotzdem um null, siehe
Punkt 2.
