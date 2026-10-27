# Transkripte über den Meeting-Besitzer abrufen
Ticket: https://app.asana.com/1/734700742714256/project/1211291490559148/task/1219921253486764

## Problem

Seit #55 sucht der Calls-Job Online-Meetings unter der oid aus der joinUrl, also unter dem
Meeting-Besitzer. Dadurch entstehen Calls zu Links, die einer anderen Person gehören als der,
in deren Kalender der Termin steht. Der Transkript-Endpoint liefert nur unter der oid des
Besitzers Transkripte. `resolve_transcript_source` probiert aber nur die internen Teilnehmer
des Calls aus `raw.msgraph_users`. Hat der Besitzer nicht teilgenommen, bekommt der Call kein
Transkript und kein CRM-Protokoll.

Beispiel aus #0135: Der Link eines Kollegen steht nur im Kalender einer Vertrieblerin, sie
führt das Meeting allein. Das Transkript liegt beim Kollegen.

## Lösung

- Die oid des Besitzers steckt in der Online-Meeting-ID des Calls
  (`msgraph_online_meeting_id`): base64 von `1*<oid>*0**<thread-id>`, Format belegt durch
  `build_online_meeting_id` in `R/msgraph_transcripts_load.R`.
- Je Online-Meeting steht diese oid als erster Kandidat vor den internen Teilnehmern.
- Lässt sich keine oid ablesen (base-35-Altbestand, geändertes Format), bleibt die Liste wie
  bisher.
- Der CRM-Export bleibt unverändert: Autor ist weiter die Kalender-Organisatorin, wenn sie
  teilgenommen hat, sonst die erste interne Person (`R/msgraph_transcripts_crm_export.R`).

## Entscheidungen

- **Q5 Woher der Besitzer kommt:** aus der Online-Meeting-ID des Calls, nicht über einen Join
  zum Event und dessen joinUrl.
- **Q6 Reihenfolge:** Besitzer zuerst, danach wie bisher die internen Teilnehmer.
- **Q7 Installation:** #55 und dieser PR gehen in einem Ansible-Lauf auf den Server; kein
  Nachhol-Skript.
- **Q8 Begriff:** „Meeting-Besitzer“ (Meeting owner) steht im Glossar `CONTEXT.md`,
  abgegrenzt von Responsible rep und Kalender-Organisator.

## Betroffen

- `R/msgraph_scoped_transcripts.R` (`msgraph_scoped_update_transcripts`,
  Kandidatenliste vor `resolve_transcript_source`)
- `tests/testthat/` (Transkript-Kandidaten)
- Tabellen nur lesend: `raw.msgraph_calls`, `raw.msgraph_call_participants`,
  `raw.msgraph_contacts`, `raw.msgraph_users`; schreibend wie bisher
  `processed.msgraph_call_transcripts`

## Out-of-Scope

- CRM-Export und Autor-Zuordnung.
- Veraltete oids (Alt-Tenant) in `raw.msgraph_users` bei den Teilnehmer-Kandidaten.
- Der stillgelegte base-35-Loader (`R/msgraph_transcripts_load.R`).

## Validierung

- Ein Call, dessen Link-Besitzer nicht unter den Teilnehmern steht, hat nach dem ersten
  Nachtlauf nach der Installation eine Zeile in `processed.msgraph_call_transcripts`.
- Das CRM-Protokoll dazu trägt die Kalender-Organisatorin als Autorin.
- Transkripte von Calls mit eigenem Link kommen weiter wie bisher.
