#' Detect Short-Lived Events
#'
#' A short-lived event was cancelled soon after it was created, which usually
#' means a mistaken or immediately withdrawn invitation.
#'
#' @param is_canceled Logical vector.
#' @param canceled_at POSIXct vector: when the event was cancelled. Must use the
#'   same timezone representation as `created_at`, see
#'   [compute_original_created_at()].
#' @param created_at POSIXct vector: when the event was created.
#' @param max_hours Numeric. An event cancelled within fewer hours than this
#'   counts as short-lived. Default 24.
#' @return Logical vector of the same length. `NA` for a cancelled event with a
#'   missing timestamp.
#' @keywords internal
detect_short_lived_events <- function(is_canceled, canceled_at, created_at, max_hours = 24) {
  # ---- start ---- #
  hours_to_cancellation <- as.numeric(difftime(canceled_at, created_at, units = "hours"))
  is_canceled & hours_to_cancellation < max_hours
}

#' Count the Participants of Each Event
#'
#' @param participants Data frame with one row per participant and the columns
#'   `event_id` and `email`.
#' @return Data frame with one row per event: `event_id`, `participant_count`
#'   (all rows), `internal_count` (see [is_internal_email()]) and
#'   `external_count` (neither internal nor synthetic, see
#'   [is_synthetic_email()]). Participants without an email count only in
#'   `participant_count`.
#' @keywords internal
count_event_participants <- function(participants) {
  # ---- start ---- #
  participants %>%
    dplyr::mutate(
      is_internal = is_internal_email(email),
      is_synthetic = is_synthetic_email(email)
    ) %>%
    dplyr::group_by(event_id) %>%
    dplyr::summarise(
      participant_count = dplyr::n(),
      internal_count = sum(is_internal, na.rm = TRUE),
      external_count = sum(!is_internal & !is_synthetic, na.rm = TRUE),
      .groups = "drop"
    )
}

#' Detect Duplicate Events
#'
#' Two events are duplicates when the same external person is invited to both
#' and they start at most 15 minutes apart. Of each neighbouring pair one event
#' loses: the no-show when only the first of the two is one, otherwise the later
#' event.
#'
#' Applies this rule to the given rows only. Which events take part is up to the
#' caller. Rows without an email are ignored.
#'
#' @param event_contacts Data frame with one row per event and external person:
#'   `event_id`, `email_lower`, `event_start` (POSIXct) and `is_no_show_class`
#'   (logical).
#' @param max_minutes Numeric. Largest distance between the starts. Default 15.
#' @return Integer vector of the `event_id`s that lose, without duplicates.
#' @keywords internal
detect_duplicate_events <- function(event_contacts, max_minutes = 15) {
  # ---- start ---- #
  duplicate_ids <- integer(0)
  sorted <- event_contacts %>%
    dplyr::filter(!is.na(email_lower)) %>%
    dplyr::arrange(email_lower, event_start)
  if (nrow(sorted) < 2) {
    return(duplicate_ids)
  }

  for (i in seq_len(nrow(sorted) - 1)) {
    curr <- sorted[i, ]
    next_row <- sorted[i + 1, ]
    if (curr$email_lower == next_row$email_lower &&
        !is.na(curr$event_start) && !is.na(next_row$event_start) &&
        abs(as.numeric(difftime(curr$event_start, next_row$event_start, units = "mins"))) <= max_minutes) {
      if (curr$is_no_show_class && !next_row$is_no_show_class) {
        duplicate_ids <- c(duplicate_ids, as.integer(curr$event_id))
      } else {
        duplicate_ids <- c(duplicate_ids, as.integer(next_row$event_id))
      }
    }
  }
  unique(duplicate_ids)
}

#' Derive the Responsible Contact of Each Event
#'
#' The responsible contact is the internal person who holds the meeting:
#'
#' - without a call, the organizer;
#' - when the organizer joined the call, the organizer;
#' - otherwise the first internal call participant;
#' - without any internal call participant, the organizer again.
#'
#' Applies the rule to the given events only. Which call counts for an event
#' is up to the caller ([update_extern_event_classification()] takes the call
#' with an external participant first, then the longest).
#'
#' @param event_calls Data frame with one row per event: `event_id` and
#'   `call_id` (`NA` without call).
#' @param organizers Data frame with at most one row per event: `event_id` and
#'   `organizer_contact_id` (the internal organizer).
#' @param call_participants Data frame with one row per call participant:
#'   `call_id`, `contact_id` and `is_internal` (logical). The first internal
#'   row of a call, in the given order, is its fallback.
#' @return Data frame with one row per row of `event_calls`: `event_id` and
#'   `responsible_contact_id` (`NA` when neither organizer nor internal
#'   participant is known).
#' @keywords internal
derive_responsible_contact <- function(event_calls, organizers, call_participants) {
  # ---- start ---- #
  org_in_call_events <- event_calls %>%
    dplyr::filter(!is.na(call_id)) %>%
    dplyr::select(event_id, call_id) %>%
    dplyr::inner_join(organizers, by = "event_id") %>%
    dplyr::semi_join(
      call_participants,
      by = c("call_id" = "call_id", "organizer_contact_id" = "contact_id")
    ) %>%
    dplyr::distinct(event_id) %>%
    dplyr::pull(event_id)

  internal_per_call <- call_participants %>%
    dplyr::filter(is_internal) %>%
    dplyr::group_by(call_id) %>%
    dplyr::slice(1) %>%
    dplyr::ungroup() %>%
    dplyr::select(call_id, fallback_contact_id = contact_id)

  event_calls %>%
    dplyr::select(event_id, call_id) %>%
    dplyr::left_join(organizers, by = "event_id") %>%
    dplyr::left_join(internal_per_call, by = "call_id") %>%
    dplyr::mutate(
      has_call = !is.na(call_id),
      org_was_in_call = event_id %in% org_in_call_events,
      responsible_contact_id = dplyr::case_when(
        !has_call ~ organizer_contact_id,
        org_was_in_call ~ organizer_contact_id,
        !is.na(fallback_contact_id) ~ fallback_contact_id,
        TRUE ~ organizer_contact_id
      )
    ) %>%
    dplyr::select(event_id, responsible_contact_id) %>%
    as.data.frame()
}

#' Detect Cancelled Events Replaced by Another Calendar Item
#'
#' A rescheduled meeting sometimes comes back as a new calendar item with a new
#' `msgraph_ical_uid`, so the usual link over the shared uid fails. This rule
#' pairs a cancelled event with an active event of another uid that invites the
#' same external person and starts less than two days away. When several qualify
#' the nearest wins.
#'
#' Applies this rule to the given rows only. Which events take part is up to the
#' caller. The result fits the `replacements` argument of
#' [derive_calendar_state()].
#'
#' @param canceled_events,active_events Data frames with one row per event and
#'   external person: `event_id`, `email_lower`, `event_start` (POSIXct) and
#'   `msgraph_ical_uid`.
#' @param max_days Numeric. The starts must be fewer days apart than this.
#'   Default 2.
#' @return Data frame with at most one row per cancelled event:
#'   `canceled_event_id` and `replacement_event_id`.
#' @keywords internal
detect_replaced_events <- function(canceled_events, active_events, max_days = 2) {
  # ---- start ---- #
  if (nrow(canceled_events) == 0 || nrow(active_events) == 0) {
    return(data.frame(canceled_event_id = integer(0), replacement_event_id = integer(0),
                      stringsAsFactors = FALSE))
  }

  canceled_events %>%
    dplyr::select(canceled_event_id = event_id, email_lower,
                  canceled_start = event_start, canceled_ical_uid = msgraph_ical_uid) %>%
    dplyr::inner_join(
      active_events %>%
        dplyr::select(replacement_event_id = event_id, email_lower,
                      replacement_start = event_start, replacement_ical_uid = msgraph_ical_uid),
      by = "email_lower",
      relationship = "many-to-many"
    ) %>%
    dplyr::filter(
      canceled_ical_uid != replacement_ical_uid,
      !is.na(canceled_start), !is.na(replacement_start)
    ) %>%
    dplyr::mutate(
      time_diff_days = abs(as.numeric(difftime(replacement_start, canceled_start, units = "days")))
    ) %>%
    dplyr::filter(time_diff_days < max_days) %>%
    dplyr::group_by(canceled_event_id) %>%
    dplyr::slice_min(time_diff_days, n = 1, with_ties = FALSE) %>%
    dplyr::ungroup() %>%
    dplyr::select(canceled_event_id, replacement_event_id) %>%
    as.data.frame()
}

#' Period of the Mailbox Move
#'
#' Calendar rows whose Graph `createdDateTime` lies in `[MAILBOX_MOVE_FROM,
#' MAILBOX_MOVE_TO)` can be mailbox-move copies (`CONTEXT.md`). The mailboxes
#' moved to the new tenant from the end of August to September 2026, a
#' different day per person; the period adds a month on each side, like the
#' Sales-Challenge list (spec `docs/specs/2026-11-13-mailbox-copy-status.md`,
#' Q3 and Q15). UTC, like `raw.msgraph_events.event_created_at`.
#'
#' @keywords internal
MAILBOX_MOVE_FROM <- as.POSIXct("2026-08-01 00:00:00", tz = "UTC")

#' @rdname MAILBOX_MOVE_FROM
#' @keywords internal
MAILBOX_MOVE_TO <- as.POSIXct("2026-11-01 00:00:00", tz = "UTC")

#' Detect Calendar Rows Created in a Move Session
#'
#' A move session is a run of calendar creations by the same organizer in quick
#' succession, among which at least two meetings had already taken place before
#' they were created (`CONTEXT.md`). Rule as in shiny-99
#' `one-off/2026-11-03_sales_challenge_umzugskopie_befuellen.sql`: only rows
#' created in `[move_from, move_to)` and not a Bookings entry (`booking:`), the
#' organizer is the participant flagged `is_organizer` with the smallest
#' `contact_id` (any address), a gap of more than `gap_minutes` starts a new
#' session, and a meeting counts as past when it started more than a day before
#' its creation.
#'
#' @param events Data frame with one row per calendar row: `event_id`,
#'   `msgraph_ical_uid`, `event_created_at` and `event_start` (POSIXct, UTC) and
#'   `organizer_contact_id` (`NA` without organizer; such rows never belong to a
#'   session).
#' @param move_from,move_to POSIXct. Period of the move, `move_to` excluded.
#' @param gap_minutes Numeric. Largest gap between two creations of a session.
#'   Default 30.
#' @param min_past Integer. Past meetings a session needs. Default 2.
#' @return The `event_id`s of all rows in a qualifying session, without
#'   duplicates, in the type of `events$event_id`.
#' @keywords internal
detect_move_sessions <- function(events, move_from = MAILBOX_MOVE_FROM,
                                 move_to = MAILBOX_MOVE_TO, gap_minutes = 30, min_past = 2) {
  # ---- start ---- #
  ev <- as.data.frame(events)
  im_zeitraum <- (ev$event_created_at >= move_from & ev$event_created_at < move_to) %in% TRUE
  ev <- ev[im_zeitraum & !(startsWith(ev$msgraph_ical_uid, "booking:") %in% TRUE) &
             !is.na(ev$organizer_contact_id), , drop = FALSE]
  if (nrow(ev) == 0) return(events$event_id[0])

  ev$vorbei <- (ev$event_start < ev$event_created_at - 86400) %in% TRUE
  ev <- ev[order(as.character(ev$organizer_contact_id), ev$event_created_at,
                 as.numeric(ev$event_id)), , drop = FALSE]
  gleicher_org <- c(FALSE, ev$organizer_contact_id[-1] == ev$organizer_contact_id[-nrow(ev)])
  abstand <- c(Inf, as.numeric(difftime(ev$event_created_at[-1], ev$event_created_at[-nrow(ev)],
                                        units = "mins")))
  neu <- !gleicher_org | abstand > gap_minutes
  sitzung <- cumsum(neu)
  vorbei_je_sitzung <- tapply(ev$vorbei, sitzung, sum)
  treffer <- sitzung %in% as.integer(names(vorbei_je_sitzung)[vorbei_je_sitzung >= min_past])
  unique(ev$event_id[treffer])
}

#' Detect Mailbox-Move Copies
#'
#' A calendar row is suspected to be a mailbox-move copy (`CONTEXT.md`) when it
#' was created in the move period and either belongs to a move session
#' ([detect_move_sessions()]) or was created after its meeting day (Berlin
#' days). Of a suspected row the first that applies wins:
#'
#' - `has_earlier_original`: an earlier calendar item of the same start shares
#'   an external participant (the pairing of [inherit_moved_copy_created_at()]);
#' - `evidenced`: a CRM task assigned to this row was created before the row;
#' - `suspected`: neither.
#'
#' Applies the rule to the given rows only. Which rows take part is up to the
#' caller (spec `docs/specs/2026-11-13-mailbox-copy-status.md`).
#'
#' @param rows Data frame with one row per calendar row: `event_created_at` and
#'   `event_start` (POSIXct, UTC), `in_move_session` and `has_earlier_original`
#'   (logical) and `first_crm_task_at` (POSIXct, UTC, `NA` without task: the
#'   creation of the earliest CRM task assigned to the row).
#' @param move_from,move_to POSIXct. Period of the move, `move_to` excluded.
#' @return Character vector of the same length: `has_earlier_original`,
#'   `evidenced`, `suspected` or `NA` when the row is not suspected.
#' @keywords internal
detect_mailbox_copies <- function(rows, move_from = MAILBOX_MOVE_FROM, move_to = MAILBOX_MOVE_TO) {
  # ---- start ---- #
  angelegt <- rows$event_created_at
  im_zeitraum <- (angelegt >= move_from & angelegt < move_to) %in% TRUE
  vorbei <- (as.Date(rows$event_start, tz = "Europe/Berlin") <
               as.Date(angelegt, tz = "Europe/Berlin")) %in% TRUE
  verdacht <- im_zeitraum & ((rows$in_move_session %in% TRUE) | vorbei)

  belegt <- (rows$first_crm_task_at < angelegt) %in% TRUE
  status <- rep(NA_character_, nrow(rows))
  status[verdacht] <- "suspected"
  status[verdacht & belegt] <- "evidenced"
  status[verdacht & rows$has_earlier_original %in% TRUE] <- "has_earlier_original"
  status
}
