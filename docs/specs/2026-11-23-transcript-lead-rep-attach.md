# Transcript protocol goes to the lead whose lead rep was in the call
Ticket: https://app.asana.com/1/734700742714256/project/1211291490555541/task/1220781937989601

## Problem

`msgraph_export_transcripts_to_crm()` attaches each Teams summary to exactly one CRM lead. It
reaches leads only through external participant addresses (`mapping.crm_lead_msgraph_contact`)
and picked one per call by address tier alone (`office` > `office_hq` > `other` > `private` >
`from_description`), keeping the first row on a tie.

Case of 12.11.2026, call 818860: Simone Kuhn exists twice in the CRM. Her new address sits only
on the Recruiting lead (people/21400532, lead rep Julia Schuster) as `office`; the E-Learning lead
(people/47174012, lead rep Peter Egger) is reached only through a colleague's `office_hq` address.
Only Peter was in the call, yet the summary landed on Julia's lead.

Measured on 23.11.2026 over about 5,800 exported calls since 05.2025: 7 calls where the picked
lead's lead rep was not in the call while another candidate's lead rep was, plus 98 top-tier ties
with a lead rep in the call.

## Solution

Still one protocol per call, on exactly one lead. The ranking of the candidate leads is turned
around: a candidate whose **lead rep** (`raw.crm_leads.responsible_user_id` → `raw.crm_users.id`)
was an internal participant of the call wins first; the address tier decides among the rest and
among several such leads. A tie in both keeps the first row, as before.

- Candidate leads are the leads the export already finds: any participant address, any tier,
  internal and synthetic addresses excluded as before.
- "In the call" uses the same internal participant set as the protocol author: internal addresses
  of actual call participants (event invitees who did not join are left out), matched to
  `raw.crm_users.user_login`.
- Deleted leads (`raw.crm_leads.is_deleted`) never win through their lead rep.
- The rule lives in the pure helper `pick_transcript_lead()`, tested without a database.

PubPackR/dbconnectorR #74 first attached the protocol to the picked lead and to the lead rep's
lead (`person_ids` with several entries). It was merged but never installed; this change replaces
it (Q8).

## Decisions

- **Q1 Which leads qualify:** only the existing candidate leads (reached through any participant address, any tier), no company-wide lookup.
- **Q2 Who counts as "in the call":** every internal participant whose address equals a CRM login, not only the organizer.
- **Q3 Several qualifying leads:** answered for the earlier "attach to all" design; superseded by Q8, the address tier decides among them.
- **Q4 Backfill of already exported calls:** none.
- **Q5 Glossary term:** **Lead rep** in `CONTEXT.md`, distinct from the meeting's Responsible rep.
- **Q6 What stays the same:** title and author unchanged.
- **Q7 Manual single-transcript export:** not changed.
- **Q8 One lead or several:** "Ich würde grundsätzlich keine Sachen ändern, die schon drin sind, wo sich keiner gemeldet hat, dass sie falsch sind. Das bedeutet, wir lassen das so. Und dann würde ich einfach nur die Regel umdrehen und nicht ein Protokoll an mehrere Personen hängen."

## Affected

- `R/msgraph_transcripts_crm_export.R`: `map_transcripts_to_crm_entities()`, new
  `pick_transcript_lead()`; `export_transcripts_to_crm()` and `export_single_protocol_to_crm()`
  back to their state before #74
- `tests/testthat/test-transcript_lead_rep_pick.R`
- `CONTEXT.md` (term **Lead rep**)
- Tables read: `mapping.crm_lead_msgraph_contact`, `raw.msgraph_contacts`, `raw.crm_leads`,
  `raw.crm_users` (all already read by the export)
- Consumer: studyflix-apps `base-apps/msgraph-ingestion/do/main_process_transcripts.R`, unchanged;
  it picks the change up with the package installation on FlowForce.

## Out of scope

- Re-exporting or updating protocols already in the CRM (Q4, Q8).
- `msgraph_export_single_transcript_to_crm()` with a fixed `lead_id` (Q7).
- Unmappable transcripts sent to the debug lead.

## Validation

- Tests: lead rep in the call wins over a better tier; no lead rep in the call keeps today's pick;
  several lead reps → tier; deleted lead never wins through its rep; integer64 call ids match.
- After the first productive run: a new call like 818860 lands on the lead rep's lead only, and
  the FlowForce log shows "Lead rep in call: n of m calls go to the lead of a rep in the call".
