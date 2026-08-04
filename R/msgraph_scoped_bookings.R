#' Bookings-Appointments -> Event-Zielschema (rein)
#'
#' @param appointments_value
#'   Liste von Appointment-Objekten (calendarView $value).
#' @param staff_map
#'   named chr: staff_id -> email (lowercase).
#'
#' @return
#'   list(events, participants)
#'
#' @export
parse_scoped_bookings <- function(appointments_value, staff_map) {
  # ---- start ---- #
  ev_rows <- list(); pt_rows <- list()
  for (a in appointments_value) {
    aid <- a$id %||% NA_character_
    astart <- lubridate::ymd_hms(a$start$dateTime %||% NA_character_, quiet = TRUE)
    if (is.na(aid) || is.na(astart)) next
    ju <- a$joinWebUrl %||% a$onlineMeetingUrl %||% NA_character_
    ical <- paste0("booking:", aid)
    ev_rows[[length(ev_rows) + 1]] <- tibble::tibble(
      msgraph_ical_uid = ical,
      event_created_at = astart, event_updated_at = astart,
      subject = a$serviceName %||% NA_character_,
      event_start = astart,
      event_end = lubridate::ymd_hms(a$end$dateTime %||% NA_character_, quiet = TRUE),
      meeting_id = if (!is.na(ju)) extract_meeting_id_safe(ju) else NA_character_,
      is_single_instance = TRUE,
      is_online_meeting = !is.na(ju),
      is_canceled = (a$status %||% "") %in% c("cancelled", "noShow"))
    # Staff = Organizer (primaerer Staff)
    sids <- a$staffMemberIds %||% list()
    if (length(sids) > 0) {
      semail <- unname(staff_map[as.character(sids[[1]])])
      if (!is.na(semail) && length(semail) == 1) {
        pt_rows[[length(pt_rows) + 1]] <- tibble::tibble(
          msgraph_ical_uid = ical, event_start = astart,
          email = tolower(normalize_external_email(semail)), ms_name = NA_character_,
          is_organizer = TRUE, source = "booking")
      }
    }
    # Kunden = Attendees (synthetische Email fuer Gaeste ohne Mail)
    custs <- a$customers %||% list()
    for (idx in seq_along(custs)) {
      cu <- custs[[idx]]
      addr <- cu$emailAddress %||% NA_character_
      email <- if (is.na(addr) || !nzchar(addr)) paste0(aid, "-", idx, "@external.guest")
               else tolower(normalize_external_email(addr))
      pt_rows[[length(pt_rows) + 1]] <- tibble::tibble(
        msgraph_ical_uid = ical, event_start = astart,
        email = email, ms_name = cu$name %||% NA_character_,
        is_organizer = FALSE, source = "booking")
    }
  }
  list(
    events = if (length(ev_rows)) dplyr::distinct(dplyr::bind_rows(ev_rows)) else tibble::tibble(),
    participants = if (length(pt_rows)) {
      dplyr::bind_rows(pt_rows) %>%
        dplyr::arrange(dplyr::desc(is_organizer)) %>%
        dplyr::distinct(msgraph_ical_uid, event_start, email, .keep_all = TRUE)
    } else tibble::tibble())
}

#' Bookings-Termine gescopt aktualisieren (app-only Bookings.Read.All)
#'
#' @param con
#'   DB-Pool.
#' @param app_token
#'   app-only Provider.
#' @param cfg
#'   load_scoped_config(); `raw_schema`/`processed_schema` steuern das Ziel-Schema.
#'
#' @return
#'   invisible(Anzahl Events).
#'
#' @export
msgraph_scoped_update_bookings <- function(con, app_token, cfg) {
  # ---- start ---- #
  rs <- cfg$raw_schema %||% "raw"
  ps <- cfg$processed_schema %||% "processed"
  start_dt <- format(Sys.Date() - cfg$events_days_back, "%Y-%m-%dT00:00:00Z")
  end_dt   <- format(Sys.Date() + cfg$events_days_forward, "%Y-%m-%dT23:59:59Z")
  biz <- graph_collect("https://graph.microsoft.com/v1.0/solutions/bookingBusinesses", app_token)
  if (biz$status != 200) stop("bookingBusinesses HTTP ", biz$status)

  all_events <- tibble::tibble(); all_part <- tibble::tibble()
  for (b in biz$value) {
    bid <- b$id %||% next
    staff <- graph_collect(paste0("https://graph.microsoft.com/v1.0/solutions/bookingBusinesses/",
                                  utils::URLencode(bid, reserved = TRUE), "/staffMembers"), app_token)
    staff_map <- if (staff$status == 200 && length(staff$value) > 0)
      stats::setNames(tolower(vapply(staff$value, function(s) s$emailAddress %||% NA_character_, character(1))),
                      vapply(staff$value, function(s) s$id %||% NA_character_, character(1))) else character(0)
    appts <- graph_collect(paste0("https://graph.microsoft.com/v1.0/solutions/bookingBusinesses/",
                                  utils::URLencode(bid, reserved = TRUE), "/calendarView"),
                           app_token, query = list(start = start_dt, end = end_dt, `$top` = 400))
    if (appts$status != 200) next
    parsed <- parse_scoped_bookings(appts$value, staff_map)
    all_events <- dplyr::bind_rows(all_events, parsed$events)
    all_part   <- dplyr::bind_rows(all_part, parsed$participants)
  }
  if (nrow(all_events) == 0) { message("Keine Bookings."); return(invisible(0L)) }

  Billomatics::postgres_upsert_data(con, rs, "msgraph_events", all_events,
                                    match_cols = c("msgraph_ical_uid", "event_start"))

  if (nrow(all_part) == 0) return(invisible(nrow(all_events)))

  contacts <- all_part %>% dplyr::transmute(email, ms_name) %>% dplyr::distinct(email, .keep_all = TRUE)
  Billomatics::postgres_upsert_data(con, rs, "msgraph_contacts", contacts, match_cols = "email")
  ev_ids <- dplyr::tbl(con, I(paste0(rs, ".msgraph_events"))) %>%
    dplyr::select(id, msgraph_ical_uid, event_start) %>% dplyr::collect()
  ct_ids <- dplyr::tbl(con, I(paste0(rs, ".msgraph_contacts"))) %>%
    dplyr::select(id, email) %>% dplyr::collect() %>% dplyr::rename(contact_id = id)
  part <- all_part %>%
    dplyr::left_join(ev_ids, by = c("msgraph_ical_uid", "event_start")) %>% dplyr::rename(event_id = id) %>%
    dplyr::left_join(ct_ids, by = "email") %>%
    dplyr::filter(!is.na(event_id), !is.na(contact_id)) %>%
    dplyr::transmute(event_id, contact_id, is_organizer, source)
  Billomatics::postgres_upsert_data(con, rs, "msgraph_event_participants", part,
                                    match_cols = c("event_id", "contact_id"))
  invisible(nrow(all_events))
}
