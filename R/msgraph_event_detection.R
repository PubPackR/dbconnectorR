#' Detect Short-Lived Events
#'
#' A short-lived event was cancelled soon after it was created, which usually
#' means a mistaken or immediately withdrawn invitation.
#'
#' @param is_canceled Logical vector.
#' @param canceled_at POSIXct vector: when the event was cancelled.
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
#' caller.
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
  if (nrow(event_contacts) < 2) {
    return(duplicate_ids)
  }

  sorted <- event_contacts %>% dplyr::arrange(email_lower, event_start)
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
