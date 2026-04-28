#' Retrieve Call Records from MSGraph and Update Database
#'
#' Retrieves new call records from MSGraph since the last update, processes them, and updates relevant database tables.
#'
#' @param con A PostgreSQL database connection object.
#' @param access_token MSGraph API access token.
#' @param days_back Number of days to look back from today. If NULL (default),
#'   uses the last update timestamp from the database.
#' @param user_id Optional. Integer vector of internal user IDs
#'   (raw.msgraph_users.id) to filter which calls are written to the database.
#'   Calls are retrieved tenant-wide from the API and then reduced to those
#'   where at least one participant matches one of the given users. If NULL
#'   (default), all retrieved calls are written.
#'
#' @return No return value. Updates database tables with new call records and participants.
#'
#' @export
#'
#' @examples
#' msgraph_update_calls(con, access_token)
#' msgraph_update_calls(con, access_token, days_back = 30)
#' msgraph_update_calls(con, access_token, user_id = c(23, 47, 89))
msgraph_update_calls <- function(con, access_token, days_back = NULL, user_id = NULL) {

  old_calls_file <- dplyr::tbl(con, I("raw.msgraph_calls")) %>% dplyr::collect()

  if (!is.null(days_back)) {
    startDate <- Sys.Date() - days_back
  } else {
    startDate <- as.Date(max(old_calls_file$updated_at)) - 1
  }
  all_records <- retrieve_all_call_records(access_token, startDate)
  organizer_data <- extract_organizer_data(all_records)
  call_records_df <- as.data.frame(t(sapply(all_records, function(x) if(is.null(x)) NA else x)), stringsAsFactors = FALSE)
  new_call_found <- nrow(call_records_df) >= 5

  if (new_call_found) {

    call_records_df <- call_records_df %>%
      dplyr::select(id, type, call_start = startDateTime, call_end = endDateTime, joinWebUrl) %>%
      dplyr::mutate(id = as.character(id))

    # When days_back is set, reprocess all records (overwrite); otherwise only new ones
    if (is.null(days_back)) {
      call_records_df <- call_records_df %>%
        dplyr::filter(!(id %in% old_calls_file$msgraph_call_id))
    }

    if(nrow(call_records_df) == 0) {
      print("All Call Records already in database.")
    } else {

    print(paste0(nrow(call_records_df), " Call Records to process (", if (!is.null(days_back)) "overwrite mode" else "new only", ")"))

    # ----- Get all participants of one Call -----
    call_meeting_record <- update_call_records_with_caller_data(access_token, call_records_df, organizer_data)

    # Optional filter: keep only calls with at least one participant in user_id.
    # user_id is an internal raw.msgraph_users.id vector; translate to the
    # msgraph_user_id values used in call_identity_id.
    if (!is.null(user_id)) {
      target_msgraph_ids <- dplyr::tbl(con, I("raw.msgraph_users")) %>%
        dplyr::filter(id %in% !!user_id) %>%
        dplyr::pull(msgraph_user_id)

      matching_call_ids <- call_meeting_record %>%
        dplyr::filter(call_identity_id %in% target_msgraph_ids) %>%
        dplyr::pull(callId) %>%
        unique()

      call_meeting_record <- call_meeting_record %>%
        dplyr::filter(callId %in% matching_call_ids)

      print(paste0(length(matching_call_ids), " calls kept after user_id filter"))
    }

    if (nrow(call_meeting_record) == 0) {
      print("No calls to upsert after filtering.")
    } else {
      update_calls(con, call_meeting_record)
      update_contacts_from_calls(con, call_meeting_record)
      update_call_participants(con, call_meeting_record)
    }

    }
  } else {

    print("No new Call Records found!")

  }

  # Enrich guest participants with real identity via event matching
  enrich_guest_participants(con)

}

retrieve_all_call_records <- function(access_token, startDate) {
  call_records_url <- "https://graph.microsoft.com/v1.0/communications/callRecords"

  query <- list(
  `$filter` = paste0("startDateTime ge ", startDate, "T00:00:00Z")
  )

  all_call_records <- list()
  retry_count <- 0

  repeat {
    repeat {
      # Send the GET request for the current page
      response <- httr::GET(call_records_url, query = query,
                      httr::add_headers(Authorization = paste("Bearer", access_token)))

      if (httr::status_code(response) == 401) {
        print("Access Token is not valid")
      }

      if (httr::status_code(response) == 429) {
        retry_count <- retry_count + 1

        # Extract the Retry-After header
        retry_after <- as.numeric(httr::headers(response)$`retry-after`)

        if (!is.na(retry_after)) {
          message(paste(
            "Throttled. Retrying after",
            retry_after,
            "seconds..."
          ))
          Sys.sleep(retry_after)
        } else {
          # If Retry-After is missing, use exponential backoff
          backoff_time <- min(2 ^ retry_count, 60)
          message(
            paste(
              "Retry-After header missing. Using exponential backoff:",
              backoff_time,
              "seconds..."
            )
          )
          Sys.sleep(backoff_time)
        }

        # Check if max retries reached
        if (retry_count >= 5) {
          stop("Max retries reached.")
        }

      } else {
        break
      }
    }

    # Parse the response
    call_records <- httr::content(response, as = "parsed", type = "application/json")

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

extract_organizer_data <- function(all_records) {
  organizer_list <- lapply(all_records, function(record) {
    org <- record$organizer$user
    data.frame(
      call_id = record$id %||% NA_character_,
      organizer_id = org$id %||% NA_character_,
      organizer_name = org$displayName %||% NA_character_,
      organizer_tenant_id = org$tenantId %||% NA_character_,
      stringsAsFactors = FALSE
    )
  })
  dplyr::bind_rows(organizer_list)
}

get_call_sessions <- function(access_token, call_id) {
  url <- paste0(
    "https://graph.microsoft.com/v1.0/communications/callRecords/",
    call_id,
    "/sessions"
  )
  response <- fetch_with_retry(url, access_token)

  if (is.null(response) || is.null(response$value) || length(response$value) == 0) {
    return(data.frame(startDateTime = character(0), endDateTime = character(0), stringsAsFactors = FALSE))
  }

  sessions <- lapply(response$value, function(s) {
    data.frame(
      startDateTime = s$startDateTime %||% NA_character_,
      endDateTime = s$endDateTime %||% NA_character_,
      stringsAsFactors = FALSE
    )
  })
  dplyr::bind_rows(sessions)
}

check_organizer_participation <- function(access_token, call_id, call_start, call_end) {
  sessions <- get_call_sessions(access_token, call_id)

  if (nrow(sessions) == 0) return(FALSE)

  session_starts <- lubridate::ymd_hms(sessions$startDateTime, quiet = TRUE)
  session_ends <- lubridate::ymd_hms(sessions$endDateTime, quiet = TRUE)

  call_start_time <- lubridate::ymd_hms(as.character(call_start), quiet = TRUE)
  call_end_time <- lubridate::ymd_hms(as.character(call_end), quiet = TRUE)

  if (is.na(call_start_time) || is.na(call_end_time)) return(FALSE)

  min_session_start <- min(session_starts, na.rm = TRUE)
  max_session_end <- max(session_ends, na.rm = TRUE)

  if (is.infinite(min_session_start) || is.infinite(max_session_end)) return(FALSE)

  diff_start <- as.numeric(difftime(min_session_start, call_start_time, units = "secs"))
  diff_end <- as.numeric(difftime(call_end_time, max_session_end, units = "secs"))

  return(diff_start > 0 || diff_end > 0)
}

add_external_organizer_participants <- function(all_participants_, organizer_data,
                                                internal_tenant_id, access_token,
                                                call_records_df) {
  if (is.null(organizer_data) || nrow(organizer_data) == 0) return(all_participants_)

  external_orgs <- organizer_data %>%
    dplyr::filter(
      !is.na(organizer_tenant_id) &
        organizer_tenant_id != internal_tenant_id &
        !is.na(organizer_id)
    )

  if (nrow(external_orgs) == 0) return(all_participants_)

  orgs_to_check <- external_orgs %>%
    dplyr::anti_join(
      all_participants_ %>% dplyr::select(call_id, identity_id),
      by = c("call_id" = "call_id", "organizer_id" = "identity_id")
    )

  if (nrow(orgs_to_check) == 0) return(all_participants_)

  print(paste0(nrow(orgs_to_check), " calls with external organizer not in participants - checking sessions..."))

  # Pre-join call times to avoid repeated filtering inside loop
  orgs_with_times <- orgs_to_check %>%
    dplyr::left_join(
      call_records_df %>% dplyr::select(id, call_start, call_end),
      by = c("call_id" = "id")
    ) %>%
    dplyr::filter(!is.na(call_start))

  organizer_rows <- list()

  for (j in seq_len(nrow(orgs_with_times))) {
    org <- orgs_with_times[j, ]

    participated <- tryCatch(
      check_organizer_participation(access_token, org$call_id, org$call_start, org$call_end),
      error = function(e) {
        message("Session check failed for call ", org$call_id, ": ", e$message)
        FALSE
      }
    )

    if (participated) {
      organizer_rows[[length(organizer_rows) + 1]] <- data.frame(
        call_id = org$call_id,
        identity_id = org$organizer_id,
        identity_displayName = org$organizer_name,
        identity_userPrincipalName = paste0(org$organizer_id, "@external.msgraph"),
        intern_call = FALSE,
        stringsAsFactors = FALSE
      )
    }
  }

  if (length(organizer_rows) > 0) {
    organizer_df <- dplyr::bind_rows(organizer_rows)
    all_participants_ <- dplyr::bind_rows(all_participants_, organizer_df)
    print(paste0(nrow(organizer_df), " external organizers confirmed and added as participants"))
  } else {
    print("No external organizer participation confirmed via sessions")
  }

  return(all_participants_)
}

update_call_records_with_caller_data <- function(access_token, call_records_df, organizer_data = NULL) {
  all_participants <- c()
  counter <- 0

  for (i in call_records_df$id) {
    call_records_url <- paste0(
      "https://graph.microsoft.com/v1.0/communications/callRecords/",
      i,
      "/participants_v2"
    )
    response <- httr::GET(call_records_url, httr::add_headers(Authorization = paste("Bearer", access_token)))
    participants <- httr::content(response, as = "parsed", type = "application/json")
    participants_df <- as.data.frame(t(sapply(participants$value, function(x)
      if (is.null(x))
        NA
      else
        x)), stringsAsFactors = FALSE) %>%
      dplyr::mutate(call_id = i)

    all_participants <- dplyr::bind_rows(all_participants, participants_df)

    counter <- counter + 1
    if (counter %% 100 == 0) {
      print(counter)
    }
  }

  # Extract internal tenant ID from the access token (JWT tid claim)
  internal_tenant_id <- extract_tenant_from_token(access_token)

  all_participants_ <- all_participants %>%
    dplyr::mutate(id = as.character(id), call_id = as.character(call_id)) %>%
    tidyr::unnest(identity, names_sep = "_") %>%
    dplyr::filter(identity != "NULL") %>%
    tidyr::unnest_wider(identity, names_sep = "_") %>%
    # Synthetic email for Guest Users and anonymized external users without email
    dplyr::mutate(
      identity_userPrincipalName = dplyr::case_when(
        # 1. Real email present -> use it
        !is.na(identity_userPrincipalName) & identity_userPrincipalName != "" ~
          identity_userPrincipalName,
        # 2. Guest identity without email -> synthetic email
        grepl("Guest", `identity_@odata.type`, ignore.case = TRUE) ~
          paste0("guest_", identity_id, "@external.guest"),
        # 3. External user without email (anonymized tenant) -> synthetic email
        (is.na(identity_tenantId) | identity_tenantId != internal_tenant_id) &
          (is.na(identity_userPrincipalName) | identity_userPrincipalName == "") ~
          paste0("guest_", identity_id, "@external.guest"),
        # 4. Everything else (e.g. internal user without email) -> NA
        TRUE ~ identity_userPrincipalName
      )
    ) %>%
    dplyr::mutate(
      intern_call = !(
        is.na(identity_tenantId) |
          identity_tenantId != internal_tenant_id
      )
    ) %>%
    dplyr::select(-tidyselect::any_of(
      c(
        "id",
        "identity_@odata.type",
        "identity_tenantId",
        "identity_email"
      )
    ))

  # ----- Add external organizers as participants (Sessions check) -----
  all_participants_ <- add_external_organizer_participants(
    all_participants_, organizer_data, internal_tenant_id, access_token, call_records_df
  )

  call_meeting_record <- call_records_df %>%
    dplyr::left_join(all_participants_, by = c("id" = "call_id")) %>%
    dplyr::select(-type) %>%
    dplyr::mutate(joinWebUrl = as.character(joinWebUrl)) %>%
    dplyr::rename(callId = id) %>%
    dplyr::mutate(call_start = lubridate::ymd_hms(call_start)) %>%
    dplyr::mutate(date = as.Date(call_start)) %>%
    dplyr::rename(
      call_identity_id = identity_id,
      call_identity_name = identity_displayName,
      call_identity_mail = identity_userPrincipalName
    )

  call_meeting_record <- dplyr::bind_rows(call_meeting_record) %>%
    dplyr::group_by(callId, call_identity_id, date) %>%
    dplyr::slice(1)

  return(call_meeting_record)
}

update_calls <- function(con, call_meeting_record) {

  Billomatics::postgres_upsert_data(
    con = con,
    schema = "raw",
    table = "msgraph_calls",
    data = temp <- call_meeting_record %>%
      dplyr::ungroup() %>%
      dplyr::mutate(alt_meeting_id = ifelse(grepl("^https://teams.microsoft.com", joinWebUrl), NA, joinWebUrl)) %>%
      dplyr::mutate(joinWebUrl = ifelse(grepl("^https://teams.microsoft.com", joinWebUrl), joinWebUrl, NA)) %>%
      dplyr::mutate(meeting_id = extract_meeting_id(joinWebUrl)) %>%
      dplyr::mutate(meeting_id = dplyr::coalesce(meeting_id, alt_meeting_id)) %>%
      dplyr::mutate(call_end = lubridate::ymd_hms(call_end)) %>%
      dplyr::select(msgraph_call_id = callId, call_start, call_end, meeting_id) %>%
      dplyr::distinct(msgraph_call_id, .keep_all = TRUE),
    match_cols = c("msgraph_call_id")
  )

}

update_contacts_from_calls <- function(con, call_meeting_record) {

  contacts <- dplyr::tbl(con, I("raw.msgraph_contacts")) %>% dplyr::collect()

  contacts_from_calls <- call_meeting_record %>%
    dplyr::ungroup() %>%
    dplyr::distinct(call_identity_name, call_identity_mail) %>%
    dplyr::select(ms_name = call_identity_name, email = call_identity_mail) %>%
    dplyr::filter(!is.na(email) & email != "") %>%
    dplyr::mutate(email = tolower(email))  %>%
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
    data = contacts_from_calls %>% dplyr::distinct(email, .keep_all = TRUE),
    match_cols = c("email")
  )

}

update_call_participants <- function(con, call_meeting_record) {

  calls <- dplyr::tbl(con, I("raw.msgraph_calls")) %>% dplyr::collect()
  contacts <- dplyr::tbl(con, I("raw.msgraph_contacts")) %>% dplyr::collect()

  Billomatics::postgres_upsert_data(
    con = con,
    schema = "raw",
    table = "msgraph_call_participants",
    data = temp <- call_meeting_record %>%
      dplyr::ungroup() %>%
      dplyr::distinct(callId, call_identity_mail) %>%
      dplyr::left_join(contacts %>% dplyr::select(email, id), by = c("call_identity_mail" = "email")) %>% dplyr::mutate(contact_id = id) %>% dplyr::select(-id, -call_identity_mail) %>%
      dplyr::left_join(calls %>% dplyr::select(msgraph_call_id, id), by = c("callId" = "msgraph_call_id")) %>% dplyr::mutate(call_id = id) %>% dplyr::select(-id, -callId) %>%
      dplyr::filter(!is.na(contact_id)) %>%
      dplyr::filter(!is.na(call_id)) %>%
      dplyr::distinct(contact_id, call_id, .keep_all = TRUE),
    match_cols = c("contact_id", "call_id")
  )

}

#' Enrich Guest Participants with Real Identity via Event Matching
#'
#' Resolves synthetic guest emails (@external.guest) to real identities
#' by matching unresolved guests against event attendees.
#' Only resolves when exactly 1 guest and 1 unmatched event attendee exist per call.
#'
#' @param con A PostgreSQL database connection object.
#' @return No return value. Updates contacts in database.
#' @keywords internal
enrich_guest_participants <- function(con) {

  # 1. Find contacts with synthetic guest email

  guest_contacts <- dplyr::tbl(con, I("raw.msgraph_contacts")) %>%
    dplyr::collect() %>%
    dplyr::filter(grepl("@external\\.guest$", email, ignore.case = TRUE))

  if (nrow(guest_contacts) == 0) return(invisible(NULL))

  # 2. Get their call participations
  guest_participations <- dplyr::tbl(con, I("raw.msgraph_call_participants")) %>%
    dplyr::filter(contact_id %in% !!guest_contacts$id) %>%
    dplyr::collect() %>%
    dplyr::left_join(guest_contacts %>% dplyr::select(id, email), by = c("contact_id" = "id"))

  if (nrow(guest_participations) == 0) return(invisible(NULL))

  # 3. Find linked events via mapping table
  call_events <- dplyr::tbl(con, I("mapping.msgraph_call_event")) %>%
    dplyr::filter(call_id %in% !!guest_participations$call_id) %>%
    dplyr::select(call_id, event_id) %>%
    dplyr::collect()

  if (nrow(call_events) == 0) {
    message("No events found for calls with guest participants")
    return(invisible(NULL))
  }

  # 4. Load event attendees with emails
  event_attendees <- dplyr::tbl(con, I("raw.msgraph_event_participants")) %>%
    dplyr::filter(event_id %in% !!call_events$event_id) %>%
    dplyr::left_join(
      dplyr::tbl(con, I("raw.msgraph_contacts")) %>% dplyr::select(id, email, ms_name),
      by = c("contact_id" = "id")
    ) %>%
    dplyr::collect() %>%
    dplyr::left_join(call_events, by = "event_id")

  # 5. Load all real (non-guest) call participants per call
  all_call_participants <- dplyr::tbl(con, I("raw.msgraph_call_participants")) %>%
    dplyr::filter(call_id %in% !!call_events$call_id) %>%
    dplyr::left_join(
      dplyr::tbl(con, I("raw.msgraph_contacts")) %>% dplyr::select(id, email),
      by = c("contact_id" = "id")
    ) %>%
    dplyr::collect() %>%
    dplyr::filter(!is_synthetic_email(email))

  # 6. Pre-load all existing contacts for potential email matches (avoids N+1 queries)
  all_attendee_emails <- event_attendees %>%
    dplyr::filter(!is.na(email) & !is_synthetic_email(email)) %>%
    dplyr::filter(!is_internal_email(email)) %>%
    dplyr::pull(email) %>%
    unique() %>%
    tolower()

  existing_contacts_lookup <- dplyr::tbl(con, I("raw.msgraph_contacts")) %>%
    dplyr::filter(email %in% !!all_attendee_emails) %>%
    dplyr::collect()

  # 7. Per call: match guests to unmatched external event attendees
  resolved <- 0
  unresolved <- 0
  errors <- 0

  for (cid in unique(guest_participations$call_id)) {

    # Guests on this call
    call_guests <- guest_participations %>%
      dplyr::filter(call_id == cid)

    # Real participants on this call (emails)
    real_participant_emails <- all_call_participants %>%
      dplyr::filter(call_id == cid) %>%
      dplyr::pull(email) %>%
      tolower()

    # Event attendees not already in call participants (external only)
    unmatched_attendees <- event_attendees %>%
      dplyr::filter(call_id == cid) %>%
      dplyr::filter(!is.na(email) & !is_synthetic_email(email)) %>%
      dplyr::filter(!is_internal_email(email)) %>%
      dplyr::filter(!(tolower(email) %in% real_participant_emails)) %>%
      dplyr::distinct(email, .keep_all = TRUE)

    # Only resolve if exactly 1 guest and 1 unmatched attendee
    if (nrow(call_guests) == 1 && nrow(unmatched_attendees) == 1) {
      guest_contact_id <- call_guests$contact_id[1]
      real_email <- unmatched_attendees$email[1]
      real_name <- unmatched_attendees$ms_name[1]

      success <- tryCatch({
        # Check if a contact with this real email already exists (from pre-loaded lookup)
        existing_contact <- existing_contacts_lookup %>%
          dplyr::filter(email == tolower(real_email))

        if (nrow(existing_contact) > 0) {
          # Contact exists: update call_participant to point to existing contact
          DBI::dbExecute(con,
            "UPDATE raw.msgraph_call_participants SET contact_id = $1 WHERE contact_id = $2 AND call_id = $3",
            params = list(existing_contact$id[1], guest_contact_id, cid)
          )
        } else {
          # Update guest contact with real email and name
          DBI::dbExecute(con,
            "UPDATE raw.msgraph_contacts SET email = $1, ms_name = $2 WHERE id = $3",
            params = list(tolower(real_email), real_name, guest_contact_id)
          )
        }
        TRUE
      }, error = function(e) {
        message("Failed to enrich guest contact ", guest_contact_id, " for call ", cid, ": ", e$message)
        FALSE
      })

      if (success) {
        resolved <- resolved + 1
      } else {
        errors <- errors + 1
      }
    } else {
      unresolved <- unresolved + nrow(call_guests)
    }
  }

  message(paste0("Guest enrichment: ", resolved, " resolved, ", unresolved, " unresolved, ", errors, " errors"))
}