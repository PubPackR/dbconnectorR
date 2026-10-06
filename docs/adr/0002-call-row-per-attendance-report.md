---
status: accepted
---

# One call row per attendance report, keyed by the report id

On the scoped (attendance-report) path, `raw.msgraph_calls.msgraph_call_id` is the **attendance report id**, not the online meeting id. A reused Teams link has one online meeting but many sessions; keying calls on the online meeting kept only the first session, so every later session counted as a no-show. The online meeting id, which the transcript endpoints need, moves to its own column `msgraph_online_meeting_id`. Rows written before the change are re-keyed in place (same `id`) onto the report of their first session, so transcripts, participants and the call-event mapping keep pointing at them.

## Considered Options

- **Composite key `<onlineMeetingId>/<reportId>` in `msgraph_call_id`**: needs no DDL, but the transcript job would have to split the key apart again to address Graph.
- **Delete the old rows and re-ingest**: rejected, because transcripts (partly exported to the CRM already) hang on them via `call_id`.

## Consequences

- `msgraph_call_id` means different things by origin: callRecord id (base-35, old tenant) or attendance report id (base-62). Anything that needs the Graph online meeting reads `msgraph_online_meeting_id` and falls back to `msgraph_call_id` for rows that have none.
