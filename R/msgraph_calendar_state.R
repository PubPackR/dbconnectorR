#' Kalenderzustand, Aenderungszeitpunkt und Nachfolger je Event (rein, ohne DB)
#'
#' Eine Zeile von `raw.msgraph_events` ist ein Termin zu einer Startzeit. Wird er
#' verschoben, entsteht eine neue Zeile derselben `msgraph_ical_uid`. Diese
#' Funktion liest daraus, was mit einer Zeile passiert ist:
#'
#' - `stale_copy`: aktiver Einzeltermin, zu dessen `msgraph_ical_uid` es eine
#'   Zeile mit spaeterem `event_updated_at` gibt, auch eine abgesagte.
#' - `active`: sonst aktiv.
#' - `rescheduled`: abgesagt mit Nachfolger.
#' - `vanished`: abgesagt ohne Nachfolger, `vanished_at` gesetzt.
#' - `cancelled`: abgesagt ohne Nachfolger, `vanished_at` leer.
#'
#' Nachfolger ist die aktive Zeile derselben `msgraph_ical_uid` mit dem
#' spaetesten `event_updated_at`, bei Gleichstand dem spaeteren `event_start`.
#' Bei einer abgesagten Zeile ohne aktive Zeile gilt das Ersatz-Paar aus Schritt
#' 7 von [update_extern_event_classification()].
#'
#' @param events
#'   data.frame mit `id`, `msgraph_ical_uid`, `is_canceled`, `is_single_instance`,
#'   `event_start`, `event_updated_at`, `vanished_at`.
#' @param replacements
#'   NULL oder data.frame mit `canceled_event_id`, `replacement_event_id`.
#'
#' @return
#'   data.frame je Event mit `event_id` (character), `calendar_status`,
#'   `calendar_changed_at_utc` (`vanished_at`, sonst `event_updated_at`) und
#'   `successor_event_id` (character).
#'
#' @keywords internal
derive_calendar_state <- function(events, replacements = NULL) {
  # ---- start ---- #
  n <- nrow(events)
  event_id <- as.character(events$id)
  uid <- as.character(events$msgraph_ical_uid)
  aktiv <- !(events$is_canceled %in% TRUE)
  aenderung <- as.POSIXct(events$event_updated_at, tz = "UTC")
  start <- as.POSIXct(events$event_start, tz = "UTC")
  vanished <- as.POSIXct(events$vanished_at, tz = "UTC")

  if (n == 0) {
    return(data.frame(event_id = character(0), calendar_status = character(0),
                      calendar_changed_at_utc = vanished, successor_event_id = character(0),
                      stringsAsFactors = FALSE))
  }

  # Spaetester Stand je Termin; abgesagte Zeilen zaehlen mit.
  stand <- as.numeric(aenderung)
  spaetester_stand <- stats::ave(stand, uid, FUN = function(x) {
    if (all(is.na(x))) NA_real_ else max(x, na.rm = TRUE)
  })
  stale <- aktiv & (events$is_single_instance %in% TRUE) &
    !is.na(stand) & stand < spaetester_stand

  # Aktive Zeilen, neueste zuerst: match() trifft so je Termin die beste Zeile.
  sortiert <- order(stand, as.numeric(start), decreasing = TRUE)
  aktive_sortiert <- sortiert[aktiv[sortiert]]
  beste_id <- event_id[aktive_sortiert[match(uid, uid[aktive_sortiert])]]

  nachfolger <- rep(NA_character_, n)
  nachfolger[!aktiv] <- beste_id[!aktiv]
  # Eine veraltete Kopie kann selbst die neueste aktive Zeile sein, wenn nur eine
  # abgesagte Zeile spaeter ist. Dann hat sie keinen Nachfolger.
  anderer <- stale & beste_id != event_id
  nachfolger[anderer] <- beste_id[anderer]

  if (!is.null(replacements) && nrow(replacements) > 0) {
    ersatz <- as.character(replacements$replacement_event_id)[
      match(event_id, as.character(replacements$canceled_event_id))]
    ohne_nachfolger <- !aktiv & is.na(nachfolger)
    nachfolger[ohne_nachfolger] <- ersatz[ohne_nachfolger]
  }

  status <- ifelse(
    aktiv,
    ifelse(stale, "stale_copy", "active"),
    ifelse(!is.na(nachfolger), "rescheduled",
           ifelse(!is.na(vanished), "vanished", "cancelled")))

  data.frame(event_id = event_id,
             calendar_status = status,
             calendar_changed_at_utc = dplyr::coalesce(vanished, aenderung),
             successor_event_id = nachfolger,
             stringsAsFactors = FALSE)
}
