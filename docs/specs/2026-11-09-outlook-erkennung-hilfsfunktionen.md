# Outlook-Erkennung aus der Klassifikation in reine Hilfsfunktionen auslagern
Ticket: https://app.asana.com/1/734700742714256/project/1211924185203938/task/1220115896703637

## Problem

Die Erkennungsschritte für kurzlebig, verschoben, zu viele Interne, Duplikat und verschoben ohne
`meeting_id` stehen mitten in `update_extern_event_classification()`. `processed.sales_meetings`
soll dieselben Regeln nutzen, ohne an der Klassifikationstabelle zu hängen, die je Termin nur einen
Grund führt. Für den Hauptablauf gab es keinen Test.

## Lösung

Neue Datei `R/msgraph_event_detection.R` mit vier reinen, internen Funktionen:

| Funktion | Eingabe | Ergebnis |
|---|---|---|
| `detect_short_lived_events()` | `is_canceled`, Absagezeitpunkt, Anlagezeitpunkt | logischer Vektor |
| `count_event_participants()` | je Teilnehmer `event_id`, `email` | je Event `participant_count`, `internal_count`, `external_count` |
| `detect_duplicate_events()` | je Event und externer Person `event_id`, `email_lower`, `event_start`, `is_no_show_class` | IDs der Verlierer |
| `detect_replaced_events()` | abgesagte und aktive Events je externer Person | Paare `canceled_event_id`, `replacement_event_id` |

Jede Funktion wendet nur ihre Regel auf die übergebenen Zeilen an. Die Klassifikation ruft sie auf
und filtert selbst vor, ihr Ergebnis bleibt gleich. Die Paare haben genau die Spalten, die
`derive_calendar_state(replacements = )` erwartet.

## Entscheidungen

Alle Antworten aus der Runde vom 09.11.2026: „passt alles genau so“.

- **Q1 Was liefert dieses Ticket?:** Dieses Ticket liefert die reinen Hilfsfunktionen und stellt die
  Klassifikation darauf um. Die einzelnen Signale und die Lösung von der Klassifikationstabelle
  kommen im sales_meetings-Ticket.
- **Q2 Wo liegt das Vorfiltern?:** Jeder Helfer wendet nur seine Regel auf die übergebenen Zeilen
  an, das Vorfiltern bleibt beim Aufrufer. Welche Menge sales_meetings übergibt, entscheidet dessen
  Ticket.
- **Q3 Womit belegen wir, dass sich nichts ändert?:** Beides: ein End-to-End-Test mit synthetischen
  Eingaben und gemockten DB-Zugriffen, der erst gegen den alten und dann unverändert gegen den neuen
  Code läuft, und ein Snapshot der Tabelle vor der Paketinstallation mit einem Diff je
  `(call_event_mapping_id, contact_id)` nach dem nächsten base-62-Lauf.
- **Q4 Kommt Schritt 4 mit?:** Ja. Die Klassifikation ruft dafür `derive_outlook_status()` auf, die
  Funktion selbst bleibt unverändert.
- **Q5 Schritt 7 und die parallele Session:** Der Helfer liefert Paare (abgesagtes Event, Ersatz-Event)
  statt nur IDs.
- **Q6 Stellen wir `sales_meeting_signals` um?:** Nein. Die Tabelle wird durch die neuen ersetzt.
- **Q7 Kurzlebig bei verschwundenen Terminen:** Nicht in diesem Umbau. Der Helfer bekommt den
  Absagezeitpunkt als Parameter, die Klassifikation übergibt weiter `event_updated_at`.
- **Q8 Ablage und Namen:** `R/msgraph_event_detection.R` mit `detect_short_lived_events()`,
  `count_event_participants()`, `detect_duplicate_events()` und `detect_replaced_events()`.
- **Q9 Teilnehmerzähler:** Der Helfer liefert `internal_count` und `external_count`. Die Schwelle von
  sieben bleibt in der Klassifikation, extern zählt ohne synthetische Gäste.

## Betroffen

- `R/msgraph_event_detection.R` (neu), `R/msgraph_extern_event_classification.R`.
- `tests/testthat/test-msgraph_event_detection.R`, `tests/testthat/test-update_extern_event_classification.R`.
- Geschrieben wird weiter nur `processed.msgraph_extern_event_classification`, ohne Schemaänderung.

## Out-of-Scope

- `processed.sales_meetings`, die einzelnen Signale dort und die Lösung von der Klassifikationstabelle.
- `processed.sales_meeting_signals` und `derive_outlook_status()` bleiben unverändert.
- Die Korrektur von kurzlebig bei verschwundenen Terminen über `vanished_at`.
- Das Aufbereiten der externen Adressen je Event für die Helfer bleibt beim Aufrufer.

## Validierung

- `testthat`: Unit-Tests der vier Funktionen und der End-to-End-Test, dessen Commit vor dem Umbau liegt.
- Nach der Paketinstallation und dem nächsten Lauf von `MSGraph_New_Tenant`: Der Diff gegen den
  Snapshot zeigt nur Abweichungen aus neuen Daten. Erst dieser Lauf belegt das Ergebnis auf echten Daten.
