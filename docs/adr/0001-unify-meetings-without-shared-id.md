---
status: accepted
---

# Unify MSGraph and CRM meetings on (external-lead × day), not a shared id

MSGraph Teams events and CRM video-call tasks are two independent records of the
same sales meetings, but there is **no shared unique meeting identifier**: the CRM
stores only the video-call tool as freetext in the task name (never a join URL or
meeting id), and external-tool meetings (WebEx / Zoom) never produce a Graph event
at all. We therefore match the two sources heuristically on
**(external lead × Europe/Berlin calendar day)**, disambiguated by
**(responsible rep × start-time)**, and unify at the grain of **one row per
(meeting × external lead)**. The lead of a meeting is derived **strictly from
external participants** (internal and synthetic emails excluded); a meeting with
external attendees but no mapped lead is kept with a `NULL` lead, and an
internal-only meeting is excluded.

## Considered Options

- **Exact join on a shared meeting id / join URL** — rejected: no such column
  exists on the CRM side, and external-tool meetings have no Graph event to join
  against.
- **`(lead × day)` alone** — rejected: measured 30% ambiguous, but that was almost
  entirely internal colleagues mis-counted as leads. After external-only lead
  derivation, genuine ambiguity falls to ~2%, resolved by rep + start-time.

## Consequences

- The match can never be perfect; a small residual (~2% of multi-meeting
  lead-days) is deliberately left **unmerged** rather than mis-attributed.
- External-only lead derivation fixes a latent bug in the shipped no-show analysis
  (internal colleagues were assigned as the customer). It is a **hard requirement**,
  not a tuning option.
- ~25% of meetings have an external customer we cannot map to a CRM lead. These are
  retained with a `NULL` lead, so the overall denominator stays whole but per-lead
  breakdowns exclude them. Widening `mapping.crm_lead_msgraph_contact` coverage is
  the lever to shrink that gap.
