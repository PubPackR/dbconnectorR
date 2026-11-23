# Transcript protocol also on the lead whose lead rep was in the call
Ticket: https://app.asana.com/1/734700742714256/project/1211291490555541/task/1220781937989601

## Problem

`msgraph_export_transcripts_to_crm()` attaches each Teams summary to exactly one CRM lead. It
reaches leads only through external participant addresses (`mapping.crm_lead_msgraph_contact`)
and picks one per call by address tier (`office` > `office_hq` > `other` > `private` >
`from_description`), at random on a tie.

Case of 12.11.2026, call 818860: Simone Kuhn exists twice in the CRM. Her new address sits only
on the Recruiting lead (people/21400532, lead rep Julia Schuster) as `office`; the E-Learning lead
(people/47174012, lead rep Peter Egger) is reached only through a colleague's `office_hq` address.
Only Peter was in the call, yet the summary landed on Julia's lead.

Measured on 23.11.2026 over about 5,800 exported calls since 05.2025: 7 calls where the picked
lead's lead rep was not in the call while another candidate's lead rep was, plus 98 top-tier ties
with a lead rep in the call. In 6 of the 7 the picked lead's person did attend, so whether the
protocol belongs to the attendee or to the lead rep's lead is a business question. The answer is:
both.

## Solution

The picked lead stays the protocol's `person_id`, exactly as today. On top, every other
**candidate lead** of the call whose **lead rep** (`raw.crm_leads.responsible_user_id` →
`raw.crm_users.id`) was an internal participant of the call is added to `person_ids`.

- Candidate leads are the leads the export already finds: any participant address, any tier,
  internal and synthetic addresses excluded as before.
- "In the call" uses the same internal participant set as the protocol author: internal addresses
  of actual call participants (event invitees who did not join are left out), matched to
  `raw.crm_users.user_login`.
- Deleted leads (`raw.crm_leads.is_deleted`) are never added: the CRM cannot attach a protocol to
  a deleted person, and one invalid id would fail the whole protocol.
- With `use_test_account = TRUE` every protocol keeps going to the test person only.

The rule lives in a pure helper, `find_lead_rep_additional_leads()`, so it is tested without a
database.

## Decisions

- **Q1 Which leads qualify as additional lead:** only the existing candidate leads (reached through any participant address, any tier), no company-wide lookup.
- **Q2 Who counts as "in the call":** every internal participant whose address equals a CRM login, not only the organizer.
- **Q3 Several qualifying leads:** all of them are attached.
- **Q4 Backfill of already exported calls:** none.
- **Q5 Glossary term:** **Lead rep** in `CONTEXT.md`, distinct from the meeting's Responsible rep.
- **Q6 What stays the same:** the picked lead stays `person_id`; additional leads only go into `person_ids`; title and author unchanged.
- **Q7 Manual single-transcript export:** not changed.

## Affected

- `R/msgraph_transcripts_crm_export.R`: `map_transcripts_to_crm_entities()`,
  `export_transcripts_to_crm()`, `export_single_protocol_to_crm()`, new
  `find_lead_rep_additional_leads()`
- `tests/testthat/test-transcript_lead_rep_attach.R`
- `CONTEXT.md` (term **Lead rep**)
- Tables read: `mapping.crm_lead_msgraph_contact`, `raw.msgraph_contacts`, `raw.crm_leads`,
  `raw.crm_users` (all already read by the export)
- Consumer: studyflix-apps `base-apps/msgraph-ingestion/do/main_process_transcripts.R`, unchanged;
  it picks the change up with the package installation on FlowForce.

## Out of scope

- Re-exporting or updating protocols already in the CRM (Q4).
- `msgraph_export_single_transcript_to_crm()` with a fixed `lead_id` (Q7).
- The address-tier ranking itself and the random pick on ties.
- Unmappable transcripts sent to the debug lead.

## Validation

- Tests: call with a lead rep in the call on a second candidate → protocol carries both ids;
  no lead rep in the call, or the lead rep owns the picked lead → `person_ids` holds the picked
  lead alone, identical to today.
- CRM API: one protocol posted with two `person_ids` (test person 33560961 and test lead
  43689733) appears on both persons in CentralStation.
- After the first productive run: a new call like 818860 shows up on both leads; SQL check over
  `raw.crm_lead_protocol_relations` that protocols with `Teams Meeting Summary` in the name and
  two relations exist only where a lead rep was in the call.
