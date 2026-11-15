# Referenz-SQL Termin-Bewertung
Ticket: https://app.asana.com/1/734700742714256/project/1211924185203938/task/1220115896589056

## Problem

Heute wird ein Termin dreimal hintereinander bewertet: in `R/msgraph_extern_event_classification.R` (Ausschlussgründe, Schritt 8b), in `R/sales_meetings_unified.R` (`crm_status_flags()`, CRM-Override) und in kpiR `termin_flags()` (`stattgefunden`, `bewertbar`). Mit dem RevOps-Datenumzug (kpiR ADR 0021) gehört die Bewertung zu RevOps und liegt als Referenz-SQL in Metabase. Die Signale dafür stehen seit dem 12.11.2026 in `processed.sales_meetings` und `processed.sales_meeting_leads` (Spec `2026-11-11-sales-meetings.md`).

Deren Startmenge ist größer als die heutige. Auf dem Handlauf vom 12.11.2026 stehen 50.119 Kalenderzeilen in `sales_meetings`, 34.471 davon auch in `sales_meetings_unified`. Die übrigen 15.648 bewertet heute niemand:

| Gruppe (Signale) | Zeilen | davon mit CRM-Task | warum heute nicht bewertet |
|---|---|---|---|
| `rescheduled` ohne Teams-Call | 8.291 | 345 | Call-Mapping verwirft abgesagte Events ohne internen Call-Teilnehmer (`R/msgraph_map_call_event.R:55`) |
| `cancelled`/`vanished` ohne Teams-Call | 4.179 | 674 | derselbe Filter |
| aktiv, ohne Teams-Meeting (`meeting_id` leer) | 1.853 | 393 | Call-Mapping braucht eine `meeting_id` (`R/msgraph_map_call_event.R:29`) |
| aktiv mit Teams-Meeting, abgesagt oder verschoben mit Call | 1.325 | 127 | 941 fehlen in der Klassifikation, 360 in `unified`, 24 im Call-Mapping |

Die 1.325 Zeilen der letzten Gruppe hängen laut Code an zwei Filtern auf die verantwortliche Person: Schritt 9 der Klassifikation behält nur Termine mit internem Organisator oder verantwortlicher Person, und `load_unified_msgraph_meetings()` liest nur Zeilen mit `is_responsible = TRUE` und einem externen Teilnehmer, der nicht Organisator ist. An den Daten belegt (Abfrage D3 vom 13.11.2026): 875 davon haben einen externen Organisator und keinen Teams-Call, und solche Termine stehen in `unified` nie (0 Zeilen). Es sind Teams-Meetings des Kunden, zu denen wir keinen Anwesenheitsbericht bekommen. Die übrigen 450 haben einen Call oder einen internen Organisator; gleichartige Zeilen stehen auch in `unified`. Kein vorhandenes Signal trennt sie: `organizer_type = 'unknown'` ohne Call trifft 888 fehlende, aber auch 808 vorhandene Zeilen (D4). Der Teams-Link trennt sie auch nicht, 1.095 der 1.325 haben keinen (D2).

## Lösung

Eine Referenz-SQL `docs/sql/2026-11-12-referenz-termin-bewertung.sql` liest nur `processed.sales_meetings`, `processed.sales_meeting_leads` und für die `meeting_id` `raw.msgraph_events`. Sie liefert eine Zeile je Meeting record und Lead, ein Termin ohne Lead steht als eine Zeile mit leerem `lead_id`. Spalten: `meeting_id`, `lead_id`, `meeting_key`, `meeting_date`, `booked_date`, `is_no_show`, `excluded`, `exclusion_reason`, `stattgefunden`, `bewertbar`, `eigener_termin`.

Die Regeln sind die heutigen, übersetzt auf die Signale:

1. **Zeilen ohne heutiges Gegenstück** (Q1, Q9, Q10, Q14). Eine verschobene Zeile ohne Teams-Call, eine abgesagte oder verschwundene Zeile ohne Teams-Call, eine aktive Zeile ohne Teams-Meeting und eine aktive Zeile mit externem Organisator ohne Teams-Call bewertet heute niemand. Hängt an ihr ein CRM-Task (`crm_outcome` ungleich `no_task`), zählt sie wie heute ein Task ohne Kalendertreffer allein über den Ausgang: `held` und `no_outcome` stattgefunden, `no_show` No-Show, `cancelled` `crm_storniert`. Sonst trägt sie `verschoben`, `abgesagt`, `ohne_teams_meeting` oder `externer_organisator` und zählt nirgends. Den externen Organisator liefert das neue Signal `processed.sales_meetings.organizer_is_external` (Q14).
2. **CRM-Zeilen** (`calendar_status = 'not_in_calendar'`) bewertet allein der CRM-Ausgang, wie `crm_status_flags()`. Ohne Zeile in `sales_meeting_leads` gilt `no_outcome` (Q13).
3. **Kalenderzeilen mit heutigem Gegenstück:**
   - `is_no_show` ist wahr, wenn `teams_status` nicht `external_joined` ist. Wie heute gilt das auch für Termine in der Zukunft; sie fallen über `excluded` heraus.
   - Ausschlussgründe, der erste zutreffende gewinnt (Q12, Reihenfolge aus `R/msgraph_extern_event_classification.R:473`): `verschoben` (`calendar_status = 'rescheduled'`, auch für das frühere `rescheduled_without_meeting_id`, Q8), `zu_viele_interne` (ab 7 internen Teilnehmenden), `duplikat_event` (`is_duplicate`), `termin_in_zukunft`, `alt_tenant_join_url`, `online_meeting_nicht_abrufbar`.
   - Schritt 8b: Eine verschobene Zeile mit internem Call höchstens 30 Minuten um den Slot und einer Änderung nach dem Slot ist ein echter No-Show und trägt keinen Grund (`identify_real_no_show_reschedules()`). Die Änderung liest die SQL wie heute aus `raw.msgraph_events.event_updated_at`, nicht aus `calendar_changed_at_utc`. Das Signal trägt bei verschwundenen Zeilen `vanished_at`, das fast immer nach dem Slot liegt: Mit ihm würden 835 heute verschobene Zeilen zu No-Shows (Abfrage D5 vom 13.11.2026). Genau diese Regel bringt erst das Folgeticket mit den Regeln vom 03.11. (Ticket A dort).
   - Eine veraltete Kopie zählt wie eine aktive Zeile, ein verschwundener Termin wie ein abgesagter. Beides ist heute so, die Regeln vom 03.11.2026 ändern es im Folgeticket.
   - CRM-Override mit `crm_outcome` aus `sales_meeting_leads`, also mit der Rangfolge vom 02.11.2026: `held` macht `is_no_show` falsch, `no_show` wahr, `no_outcome` falsch, wenn die Anwesenheit nicht messbar ist (`no_link`, Alt-Tenant, nicht abrufbar). Ein Ausgang außer `cancelled` hebt `alt_tenant_join_url` und `online_meeting_nicht_abrufbar` auf. `cancelled` setzt `crm_storniert` und überschreibt jeden anderen Grund.
4. **Ableitungen** wie kpiR `termin_flags()`, mit dem Berliner Tag als heute: `stattgefunden` heißt vergangen, nicht ausgeschlossen und kein No-Show. `bewertbar` heißt vergangen und kein Grund aus `alt_tenant_join_url`, `online_meeting_nicht_abrufbar`, `crm_storniert`. `eigener_termin` heißt kein Grund aus `verschoben`, `duplikat_event`, `zu_viele_interne`, `abgesagt`, `ohne_teams_meeting`, `externer_organisator` (kpiR `ist_eigener_termin()`, um die neuen Gründe ergänzt, Q11).

Der Abgleich `docs/sql/2026-11-12-abgleich-termin-bewertung.sql` stellt jede Zeile der Referenz-SQL ihrem Gegenstück in `sales_meetings_unified` gegenüber: Kalenderzeilen über Event und Lead, CRM-Zeilen von heute über ihren Task. Abfrage A zählt je `befund` die Zeilen und die Differenzen bei `stattgefunden`, `bewertbar` und No-Shows. Abfrage B vergleicht dieselben Größen je Monat.

Nach dem Merge legt Moritz oder Elias aus der Datei ein Metabase-Modell an (Q3, Q6). Ab dann ist die Karte maßgeblich, der Dateikopf sagt das.

## Entscheidungen

Die Empfehlungen zu Q1 bis Q8 hat der Nutzer am 12.11.2026 mit „passt alles“ bestätigt, Q9 bis Q12 mit „passen alle“, Q13 und das Zielbild mit „Passt“.

- **Q1 Zeilen ohne heutiges Gegenstück:** passt alles. b) Sie bekommen einen eigenen `exclusion_reason` aus den Signalen und zählen wie heute nirgends; was sich nicht trennen lässt, steht im Abgleich als Rest.
- **Q2 Zeilenebene:** passt alles. Eine Zeile je Meeting record × Lead, ein Termin ohne Lead als eine Zeile mit leerem `lead_id`.
- **Q3 Form in Metabase:** passt alles. Ein Metabase-Modell, auf dem spätere Kennzahlkarten aufsetzen.
- **Q4 Ablage im Repo:** passt alles. dbconnectorR, `docs/sql/2026-11-12-referenz-termin-bewertung.sql` und `docs/sql/2026-11-12-abgleich-termin-bewertung.sql`, Spec unter `docs/specs/`. Nach dem Anlegen der Karte gilt Metabase.
- **Q5 Abgleich auf Production:** Die Empfehlung, die Abfrage selbst über Metabase im eingebauten Browser auszuführen, hat der Nutzer zurückgewiesen: „Niemand würde dir Zugriff auf die Daten geben.“ Claude schreibt die Abfrage, der Nutzer führt sie in DBeaver aus und gibt das Ergebnis zurück.
- **Q6 Referenzkarte anlegen:** passt alles. Abschluss-Subtask „Die Referenzkarte Termin-Bewertung liegt als Modell in der RevOps-Sammlung in Metabase“; anlegen tut Moritz oder Elias aus der gemergten Datei, die Sammlung bestimmt Elias.
- **Q7 Ausgabespalten:** passt alles. Heutige Namen und deutsche Werte: `meeting_id`, `lead_id`, `meeting_key`, `meeting_date`, `booked_date`, `is_no_show`, `excluded`, `exclusion_reason`, `stattgefunden`, `bewertbar`.
- **Q8 Verschiebungen:** passt alles. Ein Wert `verschoben` für `verschoben` und `rescheduled_without_meeting_id`.
- **Q9 Termine ohne Teams-Meeting:** passen alle. b) Die Referenz-SQL liest `raw.msgraph_events.meeting_id`.
- **Q10 Zeilen ohne heutiges Gegenstück mit CRM-Task:** passen alle. Sie zählen allein über ihren CRM-Ausgang wie heute ein Task als eigene CRM-Zeile; ohne Task tragen sie ihren Grund und zählen nirgends.
- **Q11 Spalte `eigener_termin`:** passen alle. Ja, die Verbotsliste aus kpiR `ist_eigener_termin()` um die neuen Gründe ergänzt.
- **Q12 Gründe und Reihenfolge:** passen alle. `verschoben` (außer echter No-Show nach Schritt 8b), `abgesagt` und `ohne_teams_meeting` (je ohne CRM-Task), `zu_viele_interne`, `duplikat_event`, `termin_in_zukunft`, `alt_tenant_join_url` / `online_meeting_nicht_abrufbar` (aufhebbar durch einen CRM-Ausgang); `crm_storniert` überschreibt alles.
- **Q13 CRM-Zeilen ohne Lead:** Passt. Sie zählen als `no_outcome`; ein untergegangener No-Show oder Storno wird im Abgleich als eigene Klasse gezählt. `raw.crm_lead_tasks.lead_id` ist `NOT NULL`, die Klasse ist deshalb voraussichtlich leer.
- **Q14 Externer Organisator ohne Call:** „c“ (13.11.2026, nach dem ersten Abgleich). Data liefert ein Signal in `sales_meetings`, `organizer_is_external`, mit eigenem Subtask, PR in dbconnectorR und Image-Kette. Die Referenz-SQL gibt aktiven Zeilen mit externem Organisator ohne Teams-Call den Grund `externer_organisator`; sie zählen nirgends, mit CRM-Task gilt Q10. Anlass: Solche Termine stehen heute nie in `unified` (D3: 0 Zeilen), im Rest waren es 875 von 1.325 und der größte Teil der 664 zusätzlichen No-Shows. Kein vorhandenes Signal trennt sie (D4: `organizer_type = 'unknown'` ohne Call 888 im Rest, 808 in `unified`).

## Betroffen

- dbconnectorR: `docs/sql/2026-11-12-referenz-termin-bewertung.sql`, `docs/sql/2026-11-12-abgleich-termin-bewertung.sql`, diese Spec, `CONTEXT.md` (Begriffe **Meeting without Teams meeting** und **Own meeting**). Kein R-Code, keine neue Paketversion.
- Gelesen: `processed.sales_meetings`, `processed.sales_meeting_leads`, `raw.msgraph_events`; im Abgleich zusätzlich `processed.sales_meetings_unified` und `raw.crm_lead_tasks`.
- Neue Spalte `processed.sales_meetings.organizer_is_external` (Q14) in einem eigenen PR in dbconnectorR (`R/sales_meetings.R`, DDL unter `inst/sql/`), danach r-runtime, r-corenlp und „Deploy folder“ für msgraph-ingestion. Die Referenz-SQL läuft erst, wenn die Spalte in Production steht.
- Metabase: ein neues Modell in der RevOps-Sammlung.

## Out-of-Scope

- Die Regeln vom 03.11.2026, `bewertungsgrund`, Termin-Kette und `zaehlt_als_gelegt` (Ticket 1220346269594123).
- Weitere Signale, etwa „hat ein Teams-Meeting“ statt des Joins auf `raw.msgraph_events.meeting_id` (Q9) oder eines für den Rest von 450 Zeilen hinter dem Filter auf die verantwortliche Person.
- Die CRM-Automatik: base-11 liest weiter `processed.msgraph_extern_event_classification`.
- Abschalten von `sales_meetings_unified` und der kpiR-Tabs (Ticket 1220345979391112).

## Validierung

- Die Referenz-SQL läuft in DBeaver gegen Production ohne Fehler, und `(meeting_id, lead_id)` ist in ihrem Ergebnis eindeutig.
- Abfrage A des Abgleichs nach dem ersten Nachtlauf mit `organizer_is_external`: Jede Klasse von `befund` ist hier mit Zahl und Ursache erklärt. Erwartet sind die Gruppen aus *Problem*, Abweichungen im CRM-Ausgang (Rangfolge vom 02.11., Task-Zuordnung nach Q1, Q21, Q25 der Sales-Meetings-Spec), in der Duplikat-Menge (Q2 der Sales-Meetings-Spec) und Zeilen, die heute als Kalender- und als CRM-Zeile doppelt stehen.
- Abfrage B: No-Shows, stattgefundene und bewertbare Termine je Monat beider Seiten nebeneinander, die Differenz je Monat erklärt sich aus Abfrage A.
- Moritz oder Elias hat das Modell in Metabase angelegt.

### Ergebnis vom 15.11.2026

Gelaufen nach dem Nachtlauf vom 15.11.2026, dem ersten mit `organizer_is_external`. Das Signal ist gefüllt: 4.140 Kalenderzeilen mit externem Organisator, 46.085 mit internem, 2 ohne Organisator unter den Teilnehmenden; alle 4.196 CRM-Zeilen sind leer, wie vorgesehen.

Sicht C: 55.871 Zeilen und 55.871 Paare `(meeting_id, lead_id)`, der Grain hält.

Sicht A über alle Monate, Referenz-SQL gegen `unified`: gelegt 39.660 zu 39.495 (+165), bewertbar 36.413 zu 36.255 (+158), stattgefunden 21.666 zu 21.629 (+37), No-Shows 14.747 zu 14.626 (+121). Die Differenzen je Klasse (Referenz minus heute):

| Befund | Zeilen Referenz / heute | gelegt | stattgefunden | No-Shows | Ursache |
|---|---|---|---|---|---|
| gleich | 41.153 / 41.153 | 0 | 0 | 0 | gleich bewertet |
| gleich, Umbenennung (Q8) | 8 / 8 | 0 | 0 | 0 | `rescheduled_without_meeting_id` heißt jetzt `verschoben` |
| neu: verschoben ohne Call (Q1) | 8.197 / 0 | 0 | 0 | 0 | Gruppe aus *Problem*, Grund `verschoben`, zählt nirgends |
| neu: abgesagt ohne Call (Q1) | 3.591 / 0 | 0 | 0 | 0 | Gruppe aus *Problem*, Grund `abgesagt` |
| neu: ohne Teams-Meeting (Q9) | 1.501 / 0 | 0 | 0 | 0 | Gruppe aus *Problem*, Grund `ohne_teams_meeting` |
| neu: externer Organisator ohne Call (Q14) | 825 / 0 | 0 | 0 | 0 | Grund `externer_organisator`, das neue Signal greift |
| neu: dieselben Gruppen mit CRM-Task (Q10) | 3 / 0 | +3 | +3 | 0 | zählen allein über den CRM-Ausgang; die übrigen Zeilen dieser Gruppen mit Task stehen unter „gleich“, weil ihr Task heute als CRM-Zeile genauso zählt |
| neu: Filter auf die verantwortliche Person (Rest Q1), auch mit CRM-Task | 406 / 0 | +163 | +47 | +112 | der Rest hinter den beiden Filtern auf die verantwortliche Person (*Problem*); kein Signal trennt ihn (Q1, Out-of-Scope). Größte verbleibende Abweichung |
| abweichend: Grund kein -> duplikat_event / duplikat_event -> kein | 92 / 92 | −14 | +1 | −15 | `is_duplicate` markiert im Paar teils den anderen Partner als heute; kein Fehler im Signal (D1 vom 13.11.2026) |
| abweichend: Wechsel zwischen duplikat_event und termin_in_zukunft oder alt_tenant_join_url | 5 / 5 | +1 | 0 | 0 | derselbe Partnertausch, der andere Partner trägt einen anderen Grund |
| abweichend: CRM-Task jetzt an einer Kalenderzeile | 27 / 27 | −1 | −27 | +26 | Keiner der 27 Tasks hat einen dokumentierten Ausgang, heute `unbekannt`, jetzt `no_outcome` (D6 vom 15.11.2026); der Ausgang hat sich also nicht geändert. Heute steht der Task als eigene CRM-Zeile und zählt als stattgefunden. Jetzt hängt er an einer Kalenderzeile, und dort entscheidet ohne Ausgang wie heute Teams. Die 26 No-Shows sind 24 aktive Termine mit externem Organisator, deren Teams-Call nur Interne hatte, und 2 aktive mit internem Organisator ohne Call. Der 27. ist verlegt und hatte einen internen Call, er trägt `verschoben` |
| neu: CRM-Zeile, heute verworfen oder anders zugeordnet | 41 / 0 | +41 | +36 | 0 | Tasks, die `unified` heute verwirft oder einem anderen Termin zuordnet; neue Zuordnung nach Q21 und Q25 der Sales-Meetings-Spec |
| fehlt: CRM-Task heute eigene Zeile, jetzt kein Sieger | 0 / 34 | −34 | −31 | 0 | an keinem Paar Sieger; **Annahme** aus der Sales-Meetings-Spec (Befund E und F): Sie verlieren gegen einen Task mit stärkerem Ausgang am selben Termin und Lead und zählen heute zusätzlich |
| abweichend: CRM-Ausgang anders | 11 / 11 | +2 | +4 | −2 | Rangfolge vom 02.11.2026 oder andere Task-Zuordnung |
| abweichend: Grund verschoben -> kein / kein -> verschoben | 6 / 6 | +4 | +4 | 0 | **Annahme:** Die Verlegung erkennt `calendar_status` anders als die heutige Klassifikation |
| Lead nur in der Referenz-SQL / nur in unified | 5 / 5 | 0 | 0 | 0 | derselbe Termin, ein anderer Lead zugeordnet |

Sicht B, die letzten zwölf vollen Monate (11.2025 bis 10.2026): Je Monat liegt die Referenz-SQL bei gelegt 1 bis 15 Termine über `unified`, bei stattgefunden zwischen 5 darunter und 16 darüber, bei No-Shows zwischen 1 darunter und 10 darüber. Beispiel 10.2026: 326 No-Shows zu 319, im ersten Abgleich vom 13.11.2026 waren es noch 392. Die Monatsdifferenzen setzen sich aus den Klassen oben zusammen, vor allem aus dem Rest hinter dem Filter auf die verantwortliche Person.
