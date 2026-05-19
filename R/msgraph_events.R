#' Retrieve and Update Calendar Events from MSGraph
#'
#' Retrieves calendar events for users from MSGraph since a given start date, processes them, and updates relevant database tables.
#'
#' @param con A PostgreSQL database connection object.
#' @param access_token MSGraph API access token.
#' @param startDate Date from which to retrieve calendar events.
#' @param user_id Optional. Internal user ID(s) to filter calendar events. Accepts a
#'   single value or a vector. If `NULL` (default), all internal, non-deleted users
#'   from `raw.msgraph_users` are processed.
#'
#' @return No return value. Updates database tables with new calendar events and participants.
#'
#' @export
#'
#' @examples
#' msgraph_update_events(con, access_token, startDate)
#' msgraph_update_events(con, access_token, startDate, user_id = 23)
#' msgraph_update_events(con, access_token, startDate, user_id = c(23, 47, 89))
msgraph_update_events <- function(con, access_token, startDate, user_id = NULL) {

  all_users <- dplyr::tbl(con, I("raw.msgraph_users")) %>%
    dplyr::filter(is_internal & !is_deleted)
  if (!is.null(user_id)) {
    all_users <- all_users %>% dplyr::filter(id %in% user_id)
  }
  all_users <- dplyr::collect(all_users %>% dplyr::mutate(id = msgraph_user_id))

  all_calendar_events <- c()
  pb <- progress::progress_bar$new(format = "  downloading [:bar] :current/:total (:percent) eta: :eta", total = nrow(all_users), clear = FALSE, width = 60)
  pb$tick(0)
  for(i in all_users$id) {
    all_records <- retrieve_calendar_events(access_token, i, startDate)
    calendar_events <- as.data.frame(t(sapply(all_records, function(x) if(is.null(x)) NA else x)), stringsAsFactors = FALSE) %>%
      dplyr::mutate(user_id = i)
    all_calendar_events <- dplyr::bind_rows(all_calendar_events, calendar_events)
    pb$tick()
  }

  if(ncol(all_calendar_events) <= 1) {
    print("No new Calendar Events found!")
    return(NULL)
  }

  all_calendar_events_ <- all_calendar_events %>%
    dplyr::filter(id != "NULL" & iCalUId != "NULL") %>%
    dplyr::mutate(createdDateTime = lubridate::ymd_hms(createdDateTime)) %>%
    dplyr::mutate(isCancelled = as.character(isCancelled)) %>%
    dplyr::mutate(isCancelled = as.logical(isCancelled)) %>%
    dplyr::mutate(isOnlineMeeting = as.character(isOnlineMeeting)) %>%
    dplyr::mutate(isOnlineMeeting = as.logical(isOnlineMeeting)) %>%
    dplyr::mutate(isOrganizer = as.character(isOrganizer)) %>%
    dplyr::mutate(isOrganizer = as.logical(isOrganizer)) %>%
    tidyr::unnest_wider(start, names_sep = "_") %>%
    tidyr::unnest_wider(end, names_sep = "_") %>%
    tidyr::unnest_wider(onlineMeeting, names_sep = "_") %>%
    dplyr::mutate(subject = as.character(subject)) %>%
    dplyr::mutate(is_canceled = grepl("canceled:", subject) | grepl("Abgesagt:", subject) | isCancelled)

  msgraph_event_attendees <- all_calendar_events_ %>%
    dplyr::select(start_dateTime, attendees, iCalUId) %>%
    dplyr::mutate(event_start = lubridate::ymd_hms(start_dateTime)) %>%
    tidyr::unnest(attendees, names_sep = "_") %>%
    tidyr::unnest_wider(attendees, names_sep = "_") %>%
    dplyr::select(-attendees_type, -attendees_status) %>%
    tidyr::unnest_wider(attendees_emailAddress, names_sep = "_") %>%
    dplyr::distinct(id = iCalUId, attendees_emailAddress_name, attendees_emailAddress_address, event_start) %>%
    dplyr::mutate(is_organizer = FALSE) %>%
    dplyr::mutate(event_id = as.character(id),
           attendees_emailAddress_name = as.character(attendees_emailAddress_name),
           attendees_emailAddress_address = tolower(as.character(attendees_emailAddress_address))) %>%
    dplyr::select(-id)

  msgraph_event_organizers <- all_calendar_events_ %>%
    dplyr::mutate(event_start = lubridate::ymd_hms(start_dateTime)) %>%
    tidyr::unnest_wider(organizer, names_sep = "_") %>%
    tidyr::unnest_wider(organizer_emailAddress, names_sep = "_") %>%
    dplyr::distinct(id = iCalUId, organizer_emailAddress_name, organizer_emailAddress_address, event_start) %>%
    dplyr::mutate(is_organizer = TRUE) %>%
    dplyr::mutate(event_id = as.character(id),
           attendees_emailAddress_name = as.character(organizer_emailAddress_name),
           attendees_emailAddress_address = tolower(as.character(organizer_emailAddress_address))) %>%
    dplyr::select(-organizer_emailAddress_name, -organizer_emailAddress_address, -id)

  msgraph_event_participants <- dplyr::bind_rows(msgraph_event_attendees, msgraph_event_organizers) %>%
    dplyr::distinct() %>%
    dplyr::group_by(attendees_emailAddress_address, attendees_emailAddress_name, event_id, event_start) %>%
    dplyr::summarise(is_organizer = any(is_organizer)) %>%
    dplyr::ungroup()

  update_events(con, all_calendar_events_, startDate)
  update_contacts_from_events(con, msgraph_event_participants)
  update_event_participants(con, msgraph_event_participants)
}

update_events <- function(con, all_calendar_events_, startDate) {

  # Process new calendar events - KEEP user_id for now to track which events came from which calendars
  msgraph_events_new <- all_calendar_events_ %>%
    dplyr::select(-attendees, -organizer) %>%
    dplyr::mutate(date = as.Date(start_dateTime)) %>%
    dplyr::mutate(id = as.character(id)) %>%
    dplyr::distinct() %>%
    dplyr::mutate(iCalUId = as.character(iCalUId)) %>%
    dplyr::mutate(online_meeting_join_url = as.character(onlineMeeting_joinUrl)) %>%
    dplyr::mutate(is_single_instance = type == "singleInstance") %>%
    dplyr::select(msgraph_ical_uid = iCalUId, event_created_at = createdDateTime, event_updated_at = lastModifiedDateTime, subject, is_single_instance,
           is_online_meeting = isOnlineMeeting, online_meeting_join_url, event_start = start_dateTime, event_end = end_dateTime, is_canceled, user_id) %>%
    dplyr::mutate(msgraph_ical_uid = as.character(msgraph_ical_uid),
           event_created_at = lubridate::ymd_hms(event_created_at),
           event_updated_at = lubridate::ymd_hms(event_updated_at),
           event_start = lubridate::ymd_hms(event_start),
           event_end = lubridate::ymd_hms(event_end),
           subject = as.character(subject),
           user_id = as.character(user_id)) %>%
    dplyr::distinct() %>%
    dplyr::ungroup() %>%
    dplyr::mutate(meeting_id = extract_meeting_id(online_meeting_join_url)) %>%
    dplyr::select(-online_meeting_join_url) %>%
    dplyr::group_by(msgraph_ical_uid) %>%
    dplyr::mutate(event_created_at = min(event_created_at),
           event_updated_at = max(event_updated_at)) %>%
    dplyr::ungroup() %>%
    dplyr::distinct() %>%
    dplyr::arrange(msgraph_ical_uid, event_start, is_canceled) %>%  # if is_canceled is sometimes FALSE, take this value
    dplyr::distinct(msgraph_ical_uid, event_start, .keep_all = TRUE)

  user_ids <- msgraph_events_new %>% distinct(user_id) %>% pull(user_id)

  # Calculate endDate (same logic as in retrieve_calendar_events)
  endDate <- lubridate::today() + 365

  # Load existing events from DB
  existing_events <- dplyr::tbl(con, I("raw.msgraph_events")) %>%
    dplyr::collect()

  # Get internal users' email addresses for the specified user_ids
  internal_users <- dplyr::tbl(con, I("raw.msgraph_users")) %>%
    dplyr::filter(is_internal & !is_deleted) %>%
    dplyr::collect()

  # Filter to specific user_ids
  if (!is.null(user_ids) && length(user_ids) > 0) {
    internal_users <- internal_users %>%
      dplyr::filter(msgraph_user_id %in% user_ids)
  }

  internal_emails <- tolower(internal_users$email)
  contacts <- dplyr::tbl(con, I("raw.msgraph_contacts")) %>% dplyr::collect()

  # Find events that belong to the specified users (where they are participants)
  # and are in the date range we're updating
  event_participants <- dplyr::tbl(con, I("raw.msgraph_event_participants")) %>%
    dplyr::collect() %>%
    dplyr::left_join(
      contacts %>% dplyr::select(id, email),
      by = c("contact_id" = "id")
    ) %>%
    dplyr::filter(tolower(email) %in% internal_emails) %>%
    dplyr::left_join(
      existing_events %>% dplyr::select(id, msgraph_ical_uid, event_start),
      by = c("event_id" = "id")
    ) %>%
    dplyr::filter(
      event_start >= lubridate::ymd(startDate) &
      event_start <= lubridate::ymd(endDate)
    )

  # Events in scope: events that belong to the specified users and are in the date range
  events_in_scope <- event_participants %>%
    dplyr::distinct(msgraph_ical_uid, event_start)

  # New event identifiers (what we just downloaded)
  new_event_identifiers <- msgraph_events_new %>%
    dplyr::distinct(msgraph_ical_uid, event_start)

  # Events to mark as cancelled: in scope but NOT in the new download
  events_to_cancel <- events_in_scope %>%
    dplyr::anti_join(new_event_identifiers, by = c("msgraph_ical_uid", "event_start"))

  # Mark events as cancelled
  existing_events_updated <- existing_events %>%
    dplyr::left_join(
      events_to_cancel %>% dplyr::mutate(should_cancel = TRUE),
      by = c("msgraph_ical_uid", "event_start")
    ) %>%
    dplyr::mutate(is_canceled = ifelse(!is.na(should_cancel), TRUE, is_canceled)) %>%
    dplyr::select(-should_cancel)

  # Remove events that are in the new download from existing events (we'll add the new versions)
  events_to_keep <- existing_events_updated %>%
    dplyr::anti_join(new_event_identifiers, by = c("msgraph_ical_uid", "event_start"))

  # Combine: kept events + new events
  msgraph_events_final <- dplyr::bind_rows(
    events_to_keep,
    msgraph_events_new %>% dplyr::select(-user_id)
  ) %>%
    select(-id, -created_at, -updated_at) %>%  # Remove internal DB IDs and timestamps to let upsert handle them
    dplyr::distinct() %>% 
    group_by(msgraph_ical_uid, event_start) %>%
    slice_max(event_updated_at, with_ties = FALSE)

  # Write back to DB with delete_missing = FALSE (we're keeping all events)
  Billomatics::postgres_upsert_data(
    con = con,
    schema = "raw",
    table = "msgraph_events",
    data = msgraph_events_final,
    match_cols = c("msgraph_ical_uid", "event_start"),
    delete_missing = FALSE # all data should be updated, no missing
  )

}

update_contacts_from_events <- function(con, msgraph_event_participants) {

  contacts <- dplyr::tbl(con, I("raw.msgraph_contacts")) %>% dplyr::collect()

  contacts_from_events <- msgraph_event_participants %>%
    dplyr::ungroup() %>%
    dplyr::distinct(attendees_emailAddress_name, attendees_emailAddress_address) %>%
    dplyr::select(ms_name = attendees_emailAddress_name, email = attendees_emailAddress_address) %>%
    dplyr::filter(!is.na(email) & email != "") %>%
    dplyr::mutate(email = tolower(email)) %>%
    dplyr::mutate(email = ifelse(
      grepl("#ext#", email, ignore.case = TRUE),
      # For those: cut off from '#EXT#' onward, then replace first underscore with '@'
      sub("_", "@", sub("(?i)#ext#.*", "", email), fixed = TRUE),
      # Else: leave untouched
      email)) %>%
    dplyr::distinct()

  Billomatics::postgres_upsert_data(
    con = con,
    schema = "raw",
    table = "msgraph_contacts",
    data = contacts_from_events %>% dplyr::distinct(email, .keep_all = TRUE),
    match_cols = c("email")
  )

}

update_event_participants <- function(con, msgraph_event_participants) {

  events <- dplyr::tbl(con, I("raw.msgraph_events")) %>% dplyr::collect()
  contacts <- dplyr::tbl(con, I("raw.msgraph_contacts")) %>% dplyr::collect()

  # Process new event participants
  msgraph_event_participants_new <- msgraph_event_participants %>%
      dplyr::ungroup() %>%
      dplyr::distinct(event_id, attendees_emailAddress_address, is_organizer, event_start) %>%
      dplyr::left_join(contacts %>% dplyr::select(email, id), by = c("attendees_emailAddress_address" = "email")) %>% dplyr::mutate(contact_id = id) %>% dplyr::select(-id, -attendees_emailAddress_address) %>%
      dplyr::left_join(events %>% dplyr::select(msgraph_ical_uid, event_start, id), by = c("event_id" = "msgraph_ical_uid", "event_start")) %>% dplyr::mutate(event_id = id) %>% dplyr::select(-id) %>%
      dplyr::filter(!is.na(contact_id)) %>%
      dplyr::filter(!is.na(event_id)) %>%
      dplyr::distinct(contact_id, event_id, .keep_all = TRUE) %>%
      dplyr::select(-event_start)

  # Load existing event participants from DB
  existing_participants <- dplyr::tbl(con, I("raw.msgraph_event_participants")) %>%
    dplyr::collect()

  # Get event_ids for the newly downloaded events only
  # We only want to replace participants for events we just downloaded, not for cancelled events
  new_event_ids <- msgraph_event_participants_new %>%
    dplyr::distinct(event_id)

  # Keep all participants EXCEPT those for events we just downloaded (we'll replace those)
  participants_to_keep <- existing_participants %>%
    dplyr::anti_join(new_event_ids, by = "event_id")

  # Combine: kept participants + new participants
  msgraph_event_participants_final <- dplyr::bind_rows(
    participants_to_keep,
    msgraph_event_participants_new
  ) %>%
    dplyr::distinct() %>% 
    select(-id, -updated_at, -created_at)

  # Write back to DB with delete_missing = FALSE
  Billomatics::postgres_upsert_data(
    con = con,
    schema = "raw",
    table = "msgraph_event_participants",
    data = msgraph_event_participants_final,
    match_cols = c("contact_id", "event_id"),
    delete_missing = TRUE
  )

}

retrieve_calendar_events <- function(access_token, user, startDate) {
  endDate <- lubridate::today() + 365

  call_records_url <- paste0("https://graph.microsoft.com/v1.0/users/", user,
                             "/calendar/calendarView?startDateTime=", startDate,
                             "T00:00:00-08:00&endDateTime=", endDate,
                             "T23:59:59.000Z&$top=10000")

  all_call_records <- list()  # Initialize an empty list to store all results

  repeat {

    call_records <- fetch_with_retry(call_records_url, access_token, max_retries = 3, delay = 2)

    # Append the current page of results to the list
    if (!is.null(call_records$value)) {
      all_call_records <- c(all_call_records, call_records$value)
    }

    # Check if there's a next page
    if (!is.null(call_records$`@odata.nextLink`)) {
      call_records_url <- call_records$`@odata.nextLink`
    } else {
      break  # Exit the loop if there are no more pages
    }
  }

  return(all_call_records)
}

#' Retrieve Booking Appointments for a Business
#'
#' Fetches all appointments of a single Microsoft Bookings business from a given
#' start date until today + 365 days, following @odata.nextLink pagination.
#'
#' @param access_token MSGraph API access token.
#' @param biz_id Booking business identifier (e.g. "Studyflix@studyf.onmicrosoft.com").
#' @param startDate Date object — earliest appointment start date to include.
#' @return List of raw appointment objects from the API.
#' @keywords internal
retrieve_booking_appointments <- function(access_token, biz_id, startDate) {
  # ---- start ---- #
  endDate <- lubridate::today() + 365

  url <- paste0(
    "https://graph.microsoft.com/v1.0/solutions/bookingBusinesses/",
    utils::URLencode(biz_id, reserved = TRUE),
    "/calendarView"
  )

  query_params <- list(
    start  = paste0(startDate, "T00:00:00Z"),
    end    = paste0(endDate,   "T23:59:59Z"),
    `$top` = 400
  )

  all_appointments <- list()
  first_page <- TRUE
  repeat {
    page <- fetch_with_retry(
      url, access_token,
      query = if (first_page) query_params else c(),
      max_retries = 3, delay = 2
    )
    if (!is.null(page$value)) {
      all_appointments <- c(all_appointments, page$value)
    }
    if (!is.null(page$`@odata.nextLink`)) {
      url <- page$`@odata.nextLink`
      first_page <- FALSE
    } else {
      break
    }
  }

  return(all_appointments)
}


#' Retrieve Staff Members of a Booking Business
#'
#' Fetches the staffMembers collection of a Microsoft Bookings business.
#'
#' @param access_token MSGraph API access token.
#' @param biz_id Booking business identifier.
#' @return Data frame with columns `biz_id`, `staff_id`, `staff_email`, `staff_name`.
#' @keywords internal
retrieve_booking_staff <- function(access_token, biz_id) {
  # ---- start ---- #
  url <- paste0(
    "https://graph.microsoft.com/v1.0/solutions/bookingBusinesses/",
    utils::URLencode(biz_id, reserved = TRUE), "/staffMembers"
  )
  resp <- fetch_with_retry(url, access_token, max_retries = 3, delay = 2)
  if (is.null(resp$value) || length(resp$value) == 0) {
    return(data.frame(
      biz_id      = character(0),
      staff_id    = character(0),
      staff_email = character(0),
      staff_name  = character(0),
      stringsAsFactors = FALSE
    ))
  }
  data.frame(
    biz_id      = biz_id,
    staff_id    = vapply(resp$value, function(s) s$id %||% NA_character_, character(1)),
    staff_email = tolower(vapply(resp$value, function(s) s$emailAddress %||% NA_character_, character(1))),
    staff_name  = vapply(resp$value, function(s) s$displayName %||% NA_character_, character(1)),
    stringsAsFactors = FALSE
  )
}

#' Build a Lookup From Booking Staff to Internal User IDs
#'
#' For all given booking businesses, fetches their staff members and joins them
#' against `raw.msgraph_users` by email to resolve each `staff_id` to the
#' canonical `msgraph_user_id` of an internal user. Booking-businesses whose
#' Staff lists overlap (a user is staff in multiple businesses) are kept as
#' separate rows.
#'
#' @param con A PostgreSQL database connection object.
#' @param access_token MSGraph API access token.
#' @param biz_ids Character vector of booking business identifiers.
#' @return Data frame with columns `biz_id`, `staff_id`, `staff_email`, `staff_name`, `msgraph_user_id`.
#' @keywords internal
build_staff_lookup <- function(con, access_token, biz_ids) {
  # ---- start ---- #
  staff_all <- do.call(
    rbind,
    lapply(biz_ids, function(b) retrieve_booking_staff(access_token, b))
  )

  if (nrow(staff_all) == 0) {
    return(cbind(staff_all, msgraph_user_id = character(0)))
  }

  users <- dplyr::tbl(con, I("raw.msgraph_users")) %>%
    dplyr::select(msgraph_user_id, email) %>%
    dplyr::collect() %>%
    dplyr::mutate(email = tolower(email))

  dplyr::left_join(staff_all, users, by = c("staff_email" = "email"))
}

#' Convert Booking Appointments to Event + Participant Data Frames
#'
#' Maps raw booking appointments to the same dataframe shape that
#' `msgraph_update_events()` produces, so that the downstream
#' `update_events()` / `update_contacts_from_events()` /
#' `update_event_participants()` functions can be reused as-is.
#'
#' Matching of an appointment to an existing calendar-event row happens via the
#' Teams meeting_id extracted from `joinWebUrl`. If a match is found the
#' existing `msgraph_ical_uid` + `event_start` are reused and the appointment
#' only enriches the participants. If no match exists a new event row is
#' created with `msgraph_ical_uid = paste0("booking:", appointment$id)`.
#'
#' Customers without an email address get a synthetic address
#' `<bookingId>-<idx>@external.guest`, analog to the guest-handling in
#' `msgraph_calls.R`.
#'
#' `update_events()` calls `dplyr::select(-attendees, -organizer)` on its
#' input, so placeholder `NA` columns are added to keep the shape compatible.
#'
#' @param appointments List of raw appointment objects (from `retrieve_booking_appointments`).
#' @param staff_lookup Data frame produced by `build_staff_lookup()`.
#' @param existing_events Data frame from `dplyr::collect(dplyr::tbl(con, I("raw.msgraph_events")))`.
#' @return Named list with two data frames: `events` (shape of `all_calendar_events_`) and `participants` (shape of `msgraph_event_participants`).
#' @keywords internal
appointments_to_event_dataframes <- function(appointments, staff_lookup, existing_events) {
  # ---- start ---- #
  if (length(appointments) == 0) {
    return(list(events = NULL, participants = NULL))
  }

  # ---- one-row-per-appointment frame with everything we need ----
  appt_df <- do.call(rbind, lapply(seq_along(appointments), function(i) {
    a <- appointments[[i]]
    join_url <- a$joinWebUrl %||% a$onlineMeetingUrl %||% NA_character_
    meeting_id <- if (!is.na(join_url)) extract_meeting_id(join_url) else NA_character_
    data.frame(
      appt_idx       = i,
      appt_id        = a$id %||% NA_character_,
      service_name   = a$serviceName %||% NA_character_,
      start_dt       = a$start$dateTime %||% NA_character_,
      end_dt         = a$end$dateTime %||% NA_character_,
      join_url       = join_url,
      meeting_id     = meeting_id,
      status         = a$status %||% NA_character_,
      stringsAsFactors = FALSE
    )
  }))

  # ---- match against existing events via meeting_id ----
  # Carry subject/created/updated through the join so matched events keep
  # their original calendarView-derived subject (which is descriptive, e.g.
  # "Booking: Studyflix Beratungsgespräch - Foo Bar") instead of being
  # overwritten with the generic Bookings serviceName ("Beratungsgespräch").
  existing_lookup <- existing_events %>%
    dplyr::filter(!is.na(meeting_id) & meeting_id != "") %>%
    dplyr::distinct(meeting_id, msgraph_ical_uid, event_start,
                    subject, event_created_at, event_updated_at) %>%
    dplyr::group_by(meeting_id) %>%
    dplyr::slice_max(event_start, n = 1, with_ties = FALSE) %>%
    dplyr::ungroup()

  appt_df <- appt_df %>%
    dplyr::left_join(existing_lookup, by = "meeting_id") %>%
    dplyr::mutate(
      msgraph_ical_uid = ifelse(
        is.na(msgraph_ical_uid),
        paste0("booking:", appt_id),
        msgraph_ical_uid
      ),
      event_start_dt = ifelse(
        is.na(event_start),
        start_dt,
        as.character(event_start)
      ),
      # Prefer existing event metadata when matched; fall back to Bookings data
      subject_final = ifelse(is.na(subject), service_name, subject),
      created_final = ifelse(is.na(event_created_at), start_dt, as.character(event_created_at)),
      updated_final = ifelse(is.na(event_updated_at), start_dt, as.character(event_updated_at))
    )

  # ---- events dataframe (shape of all_calendar_events_) ----
  # Placeholder columns attendees/organizer/id are required because
  # update_events() calls select(-attendees, -organizer) and mutate(id = as.character(id)).
  events_df <- data.frame(
    id                   = NA_character_,          # placeholder; update_events drops this via select()
    iCalUId              = appt_df$msgraph_ical_uid,
    createdDateTime      = appt_df$created_final,  # matched: existing event_created_at, fallback: start
    lastModifiedDateTime = appt_df$updated_final,  # matched: existing event_updated_at, fallback: start
    subject              = appt_df$subject_final,  # matched: existing subject, fallback: serviceName
    type                 = "singleInstance",
    isOnlineMeeting      = !is.na(appt_df$meeting_id),
    onlineMeeting_joinUrl = appt_df$join_url,
    start_dateTime       = appt_df$event_start_dt,
    end_dateTime         = appt_df$end_dt,
    isCancelled          = vapply(appt_df$status, function(s) isTRUE(s %in% c("cancelled", "noShow")), logical(1)),
    is_canceled          = vapply(appt_df$status, function(s) isTRUE(s %in% c("cancelled", "noShow")), logical(1)),
    isOrganizer          = TRUE,
    attendees            = NA,                     # placeholder; dropped by update_events()
    organizer            = NA,                     # placeholder; dropped by update_events()
    user_id              = NA_character_,          # filled below
    stringsAsFactors = FALSE
  )

  # primary staff -> user_id
  primary_user <- vapply(seq_along(appointments), function(i) {
    a <- appointments[[i]]
    sids <- a$staffMemberIds %||% character(0)
    if (length(sids) == 0) return(NA_character_)
    hit <- staff_lookup %>% dplyr::filter(staff_id == sids[[1]])
    if (nrow(hit) == 0 || is.na(hit$msgraph_user_id[1])) NA_character_ else hit$msgraph_user_id[1]
  }, character(1))
  events_df$user_id <- primary_user

  # ---- participants dataframe (shape of msgraph_event_participants) ----
  parts <- do.call(rbind, lapply(seq_along(appointments), function(i) {
    a <- appointments[[i]]
    ical <- appt_df$msgraph_ical_uid[i]
    ev_start <- appt_df$event_start_dt[i]

    # Customers
    custs <- a$customers %||% list()
    cust_rows <- if (length(custs) == 0) NULL else do.call(rbind, lapply(seq_along(custs), function(j) {
      cust <- custs[[j]]
      email_raw <- cust$emailAddress %||% NA_character_
      email <- if (is.null(email_raw) || is.na(email_raw) || email_raw == "") {
        paste0(a$id, "-", j, "@external.guest")
      } else {
        tolower(email_raw)
      }
      data.frame(
        event_id                       = ical,
        event_start                    = ev_start,
        attendees_emailAddress_name    = cust$name %||% NA_character_,
        attendees_emailAddress_address = email,
        is_organizer                   = FALSE,
        stringsAsFactors = FALSE
      )
    }))

    # Staff (all as organizers, analog to msgraph_event_organizers in msgraph_update_events)
    sids <- a$staffMemberIds %||% character(0)
    staff_rows <- if (length(sids) == 0) NULL else do.call(rbind, lapply(sids, function(sid) {
      hit <- staff_lookup %>% dplyr::filter(staff_id == sid)
      if (nrow(hit) == 0 || is.na(hit$staff_email[1])) return(NULL)
      data.frame(
        event_id                       = ical,
        event_start                    = ev_start,
        attendees_emailAddress_name    = hit$staff_name[1] %||% NA_character_,
        attendees_emailAddress_address = tolower(hit$staff_email[1]),
        is_organizer                   = TRUE,
        stringsAsFactors = FALSE
      )
    }))

    rbind(cust_rows, staff_rows)
  }))

  # Convert event_start to POSIXct for downstream consistency
  parts$event_start <- lubridate::ymd_hms(parts$event_start)
  events_df$start_dateTime <- as.character(events_df$start_dateTime)

  list(events = events_df, participants = parts)
}

#' Retrieve and Update Booking Appointments From MSGraph
#'
#' Retrieves Microsoft Bookings appointments for all booking businesses in the
#' tenant, converts them to the same shape as regular calendar events, and
#' writes them through the existing `update_events()`,
#' `update_contacts_from_events()`, and `update_event_participants()` pipeline.
#'
#' Externe Kunden (`customers` in der Bookings-API) erscheinen damit in
#' `raw.msgraph_event_participants` — was über die `calendarView`-API nicht
#' möglich ist, weil Kunden dort nicht im `attendees`-Feld stehen.
#'
#' Match zu bestehenden Calendar-Events erfolgt über die Teams-`meeting_id`;
#' wenn kein Match → neue Event-Row mit `msgraph_ical_uid = "booking:<id>"`.
#'
#' @param con A PostgreSQL database connection object.
#' @param access_token MSGraph API access token.
#' @param startDate Date from which to retrieve appointments.
#'
#' @return No return value. Updates database tables.
#' @export
#'
#' @examples
#' msgraph_update_booking_appointments(con, access_token, startDate)
msgraph_update_booking_appointments <- function(con, access_token, startDate) {
  # ---- start ---- #

  # 1. List businesses
  biz_resp <- fetch_with_retry(
    "https://graph.microsoft.com/v1.0/solutions/bookingBusinesses",
    access_token, max_retries = 3, delay = 2
  )
  biz_ids <- vapply(biz_resp$value %||% list(), function(b) b$id, character(1))
  if (length(biz_ids) == 0) {
    print("No booking businesses accessible.")
    return(invisible(NULL))
  }
  print(paste0("Booking businesses: ", paste(biz_ids, collapse = ", ")))

  # 2. Staff lookup
  staff_lookup <- build_staff_lookup(con, access_token, biz_ids)
  print(paste0("Staff lookup rows: ", nrow(staff_lookup),
               " (", sum(is.na(staff_lookup$msgraph_user_id)), " ohne user-match)"))

  # 3. Existing events (für meeting_id-Match)
  existing_events <- dplyr::tbl(con, I("raw.msgraph_events")) %>% dplyr::collect()

  # 4. Pro Business Appointments holen und konvertieren
  all_events_df <- NULL
  all_parts_df  <- NULL
  for (b in biz_ids) {
    appts <- retrieve_booking_appointments(access_token, b, startDate)
    print(paste0("  ", b, ": ", length(appts), " appointments"))
    if (length(appts) == 0) next
    out <- appointments_to_event_dataframes(appts, staff_lookup, existing_events)
    all_events_df <- dplyr::bind_rows(all_events_df, out$events)
    all_parts_df  <- dplyr::bind_rows(all_parts_df,  out$participants)
  }

  if (is.null(all_events_df) || nrow(all_events_df) == 0) {
    print("No booking appointments to process.")
    return(invisible(NULL))
  }

  update_events(con, all_events_df, startDate)

  if (!is.null(all_parts_df) && nrow(all_parts_df) > 0) {
    update_contacts_from_events(con, all_parts_df)
    update_event_participants(con, all_parts_df)
  }

  invisible(NULL)
}

