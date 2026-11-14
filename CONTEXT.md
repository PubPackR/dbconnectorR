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
with external attendees but no identifiable lead yields one placeholder row. A
CRM VC-task of the same rep starting within 15 minutes of a placeholder row
claims it, and the row takes that task's lead.

**Meeting record**:
One calendar row with at least one external participant, or one CRM VC-task
that belongs to no calendar row. A meeting can have several records: the
Bookings entry, its calendar copy and the own invite, or each step of a
reschedule chain.
_Avoid_: Meeting, Termin, event (each names the whole appointment or one source).

**Leadless meeting**:
A meeting with external (customer-side) attendees but none that map to a known
CRM lead, neither by any of the lead's CRM addresses nor by unique domain — a mapping gap, not an
internal meeting. Kept in the analysis with an unknown lead.
_Avoid_: Orphan meeting, unmatched meeting.

**Domain-matched lead**:
A lead assigned to a meeting no attendee address maps to, because an external
attendee's email domain belongs to exactly one CRM lead. Often a colleague of
the attendee rather than the attendee. Never a freemail domain, never when any
attendee address maps.
_Avoid_: Fallback lead, guessed lead.

**Session**:
One occurrence of a Teams online meeting, evidenced by its own attendance report. A reused Teams link (series, personal meeting link) has many sessions, each on its own day with its own attendees; attendance is measured per session, never per link.
_Avoid_: Occurrence, call, online meeting (that is the link all sessions share).

### People

**External lead** (or **Lead**):
The customer or prospective customer a meeting is with. Derived only from
external meeting participants, never from internal staff.
_Avoid_: Customer, contact, account, participant.

**Responsible rep**:
The internal salesperson accountable for a meeting. A rep cannot be in two
meetings at the same instant — the basis for time-based disambiguation.
_Avoid_: Owner, organizer, agent, Mitarbeiter.

**Meeting owner** (Meeting-Besitzer):
The account that created the Teams link of a meeting. Microsoft keeps the
online meeting, its attendance reports and its transcripts under this account.
Not necessarily the person whose calendar holds the appointment, nor the
responsible rep: a rep may hold a link a colleague created and lead the meeting
alone.
_Avoid_: Organizer, calendar owner, host.

**Internal contact**:
A Studyflix or partner-group person (identified by email pattern). Never counts
as a lead, even when present as a participant.
_Avoid_: Employee, colleague.

**Synthetic guest**:
A placeholder participant Graph creates for an unresolved external attendee
(`@external.guest` / `@external.msgraph`). Not an identifiable lead.

**External speaker**:
A speaker in a session's transcript whose name does not carry the Studyflix
company suffix, and who speaks at least twice. Evidence that an external person
attended the session, even when its attendance report lists only internal
contacts. An unnamed speaker (`@1`) counts.
_Avoid_: External participant (that is the attendance report's view), guest.

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
auto-detected attendance when the CRM status is definite
(no-show / show-up / cancelled). An unknown CRM status leaves the Graph outcome
untouched — unless the meeting's attendance is unmeasurable; then the meeting
counts as held. On an old-tenant meeting the override also lifts the
old-tenant exclusion.

**Unmeasurable attendance**:
A meeting for which Graph could never have produced an attendance report: it
provably has no Teams link, or its link belongs to the previous tenant. Its
"no call found" is an observation limit, not a no-show.
_Avoid_: Unknown outcome (that is the CRM status), unobservable meeting.

**Task mapping**:
The stored decision which MSGraph meeting a CRM VC-task belongs to, together
with the evidence for it (which match rule applied, how many candidates, how far
apart in time). Every CRM VC-task has one, also when no meeting was found — then
the evidence says why. Carries nothing about the meeting's outcome.
_Avoid_: Classification, meeting status.

**Signal**:
What one source system says about a meeting, kept as the source says it: the
CRM outcome note, the calendar state (active, rescheduled, cancelled), the Teams
attendance (external call, internal call, no call, unmeasurable). Signals of
different systems may contradict each other; that is information, not an error.
_Avoid_: Outcome, evaluation, no-show (those are judgements on signals).

**Vanished meeting**:
A calendar row that disappeared from a loaded calendar without Outlook reporting
a cancellation, and has no successor. A signal that the meeting is gone, not
proof that it was called off.
_Avoid_: Deleted meeting, cancelled meeting.

**Cancelled meeting**:
A calendar row Outlook itself reports as cancelled, with no successor.
_Avoid_: Vanished meeting, no-show.

**Rescheduled meeting**:
A cancelled or vanished calendar row that has a successor.
_Avoid_: Moved meeting.

**Stale copy**:
A one-off calendar row still active although a newer version of the same
calendar item exists, typically held by another invitee's calendar.
_Avoid_: Mailbox-move copy (a different item carrying the same meeting).

**Mailbox-move copy**:
A calendar row that the move of the mailboxes to the new tenant
(August to October 2026) re-created under a new iCal UID. It carries
the move day as its creation date, although the meeting was often
arranged long before. Whether a row is one can only be suspected or
evidenced, never read off.
_Avoid_: Stale copy (same iCal UID, older version), duplicate (the copy
is often the meeting's only row).

**Move session**:
A run of calendar creations by the same organizer in quick succession,
among which at least two meetings had already taken place before they
were created. That is what copying a calendar looks like; no person
creates several past meetings within minutes. A row from a move session
is a suspected mailbox-move copy.
_Avoid_: Move day (there was no fixed day per person).

**Successor**:
The calendar row that replaces another one: the active row of the same calendar
item, or else the replacement found by same external contact and nearby start.
_Avoid_: Follow-up meeting, next meeting.

**Evaluation**:
A judgement derived from the signals of a meeting: no-show, excluded, and why.
Always downstream of the signals, never stored next to them.
_Avoid_: Signal, status.

**CRM outcome precedence**:
How one CRM outcome is read from several notes. Within one task the latest note
with a recognised outcome counts; a note without one is no signal. Across
several tasks on the same meeting a written show-up beats no-show, no-show beats
cancelled, and any recognised outcome beats none. Where tasks or notes disagree,
the meeting is marked as conflicting.
_Avoid_: Strongest status (the older reading, where cancelled won).
