# Sales Meetings (No-Show Analysis)

Shared language for unifying two independent records of the same sales meeting —
Microsoft Graph calendar events and CRM video-call tasks — into one dataset for
no-show analysis.

## Language

### Meetings & sources

**Meeting**:
One scheduled sales appointment between a responsible rep and one or more
external leads.
_Avoid_: Call, appointment, event, Termin (each names only one source's view).

**MSGraph meeting**:
A meeting as recorded by a Microsoft Teams calendar event (Microsoft Graph),
carrying auto-detected attendance.
_Avoid_: Teams event, calendar event.

**CRM VC-task**:
A meeting as recorded by a CRM lead task whose name mentions a video-call tool
(Teams / WebEx / Zoom) as freetext, carrying the rep's manual outcome note. Tied
to exactly one lead.
_Avoid_: CRM appointment, task.

**Meeting row**:
One external lead's expected participation in one meeting — the grain of the
unified dataset. A meeting with N mapped external leads yields N rows; a meeting
with external attendees but no identifiable lead yields one placeholder row.

**Leadless meeting**:
A meeting with external (customer-side) attendees but none that map to a known
CRM lead — a mapping gap, not an internal meeting. Kept in the analysis with an
unknown lead.
_Avoid_: Orphan meeting, unmatched meeting.

### People

**External lead** (or **Lead**):
The customer or prospective customer a meeting is with. Derived only from
external meeting participants, never from internal staff.
_Avoid_: Customer, contact, account, participant.

**Responsible rep**:
The internal salesperson accountable for a meeting. A rep cannot be in two
meetings at the same instant — the basis for time-based disambiguation.
_Avoid_: Owner, organizer, agent, Mitarbeiter.

**Internal contact**:
A Studyflix or partner-group person (identified by email pattern). Never counts
as a lead, even when present as a participant.
_Avoid_: Employee, colleague.

**Synthetic guest**:
A placeholder participant Graph creates for an unresolved external attendee
(`@external.guest` / `@external.msgraph`). Not an identifiable lead.

### Outcomes & matching

**No-show**:
A meeting where the external lead did not attend. The unit of the analysis.
_Avoid_: Absence, miss, cancellation (a no-show is not a cancellation).

**No-show rate**:
No-shows ÷ all scheduled in-scope meetings (cancelled, excluded, and
unknown-outcome CRM meetings removed first), reported per responsible rep per
month. Leadless meetings count in the overall rate but not in per-lead breakdowns.

**Match key**:
The rule deciding that an MSGraph meeting and a CRM VC-task are the same
meeting — shared (external lead × calendar day), disambiguated by
(responsible rep × start-time) when a lead has more than one meeting that day.
No shared unique meeting id exists between the two sources.
_Avoid_: Join key, meeting id.

**CRM override**:
On an unambiguous match, the rep's manual CRM outcome wins over Graph's
auto-detected attendance — but only when the CRM status is definite
(no-show / show-up / cancelled). An unknown CRM status leaves the Graph outcome
untouched.
