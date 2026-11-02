---
status: accepted
---

# Persist the CRM task mapping and keep signals apart from evaluation

Until now the decision which MSGraph meeting a CRM VC-task belongs to was recomputed twice, with different rules (`update_crm_task_meeting_classification()` on lead × day only, `update_sales_meetings_unified()` with rep, start time and the substitute rule), and never stored. `processed.sales_meetings_unified` then held only the result of a chain of overrides (`is_no_show`, `excluded`), so the reason a meeting counted one way could not be read back from the data. We now store the decision in `mapping.crm_lead_task_msgraph_event`, one row per CRM VC-task with the rule that applied, and keep what each source system says in `processed.sales_meeting_signals` (CRM outcome, calendar state, Teams attendance) without judging it. No-show and exclusion are evaluations and belong in a view on top of the signals. Both tables are built next to the existing ones; nothing that reads `sales_meetings_unified` or `msgraph_extern_event_classification` changes.

## Considered Options

- **Signals in the mapping table**: rejected. The mapping answers "which meeting is this task", the signal table answers "what does each system say about the meeting". Keeping the outcome in both would let them drift apart.
- **Mapping table with matched pairs only**: rejected. A row per task, also without a match, shows why a task was not merged (ambiguous, external tool, no candidate), which is the transparency the table is for.
- **Recompute the calendar signals from scratch**: rejected for now. It would put the duplicate, reschedule and internal-meeting rules into the code a second time. The signal table takes them from `msgraph_extern_event_classification` instead, where each meeting carries only one reason by fixed precedence.

## Consequences

- The signal table depends on `msgraph_extern_event_classification` until its detection steps are split into shared helpers (follow-up ticket). Until then a meeting shows one calendar reason, not every one that applies.
- The CRM outcome follows a new precedence (show-up > no-show > cancelled > none, latest recognised note within a task). `sales_meetings_unified` keeps the old reading (cancelled wins), so the two tables can disagree on the CRM outcome of the same meeting.
- Ambiguous tasks, dropped entirely by `sales_meetings_unified`, appear in the signal table as rows of their own with `match_rule = 'mehrdeutig'`. This restores what ADR 0001 describes ("left unmerged").
- A meeting that vanished from a loaded calendar is stored as cancelled by the ingest and cannot be told apart from a real cancellation yet (follow-up ticket).
