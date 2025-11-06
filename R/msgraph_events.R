#' Retrieve and Update Calendar Events from MSGraph
#'
#' Retrieves calendar events for users from MSGraph since a given start date, processes them, and updates relevant database tables.
#'
#' @param con A PostgreSQL database connection object.
#' @param access_token MSGraph API access token.
#' @param startDate Date from which to retrieve calendar events.
#' @param user_id Optional. Specific user ID (intern ID) to filter calendar events.
#'
#' @return No return value. Updates database tables with new calendar events and participants.
#'
#' @export
#'
#' @examples
#' msgraph_update_events(con, access_token, startDate)
#' msgraph_update_events(con, access_token, startDate, user_id = 23)
msgraph_update_events <- function(con, access_token, startDate, user_id = NULL) {

  all_users <- dplyr::tbl(con, I("raw.msgraph_users")) %>%
    dplyr::filter(is_internal & !is_deleted)
  if (!is.null(user_id)) {
    all_users <- all_users %>% dplyr::filter(id == user_id)
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

  update_events(con, all_calendar_events_)
  update_contacts_from_events(con, msgraph_event_participants)
  update_event_participants(con, msgraph_event_participants)
}

update_events <- function(con, all_calendar_events_) {

  msgraph_events <- all_calendar_events_ %>%
    dplyr::select(-attendees, -organizer) %>%
    dplyr::mutate(date = as.Date(start_dateTime)) %>%
    dplyr::mutate(id = as.character(id)) %>%
    dplyr::distinct() %>%
    dplyr::mutate(iCalUId = as.character(iCalUId)) %>%
    dplyr::mutate(online_meeting_join_url = as.character(onlineMeeting_joinUrl)) %>%
    dplyr::mutate(is_single_instance = type == "singleInstance") %>%
    dplyr::select(msgraph_ical_uid = iCalUId, event_created_at = createdDateTime, event_updated_at = lastModifiedDateTime, subject, is_single_instance,
           is_online_meeting = isOnlineMeeting, online_meeting_join_url, event_start = start_dateTime, event_end = end_dateTime, is_canceled) %>%
    dplyr::mutate(msgraph_ical_uid = as.character(msgraph_ical_uid),
           event_created_at = lubridate::ymd_hms(event_created_at),
           event_updated_at = lubridate::ymd_hms(event_updated_at),
           event_start = lubridate::ymd_hms(event_start),
           event_end = lubridate::ymd_hms(event_end),
           subject = as.character(subject)) %>%
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

  Billomatics::postgres_upsert_data(
    con = con,
    schema = "raw",
    table = "msgraph_events",
    data = msgraph_events,
    match_cols = c("msgraph_ical_uid", "event_start")
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

  msgraph_event_participants_ <- msgraph_event_participants %>%
      dplyr::ungroup() %>%
      dplyr::distinct(event_id, attendees_emailAddress_address, is_organizer, event_start) %>%
      dplyr::left_join(contacts %>% dplyr::select(email, id), by = c("attendees_emailAddress_address" = "email")) %>% dplyr::mutate(contact_id = id) %>% dplyr::select(-id, -attendees_emailAddress_address) %>%
      dplyr::left_join(events %>% dplyr::select(msgraph_ical_uid, event_start, id), by = c("event_id" = "msgraph_ical_uid", "event_start")) %>% dplyr::mutate(event_id = id) %>% dplyr::select(-id) %>%
      dplyr::filter(!is.na(contact_id) & contact_id != "") %>%
      dplyr::filter(!is.na(event_id) & event_id != "") %>%
      dplyr::distinct(contact_id, event_id, .keep_all = TRUE) %>%
      dplyr::select(-event_start)

  Billomatics::postgres_upsert_data(
    con = con,
    schema = "raw",
    table = "msgraph_event_participants",
    data = msgraph_event_participants_,
    match_cols = c("contact_id", "event_id")
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