#' Retrieve Call Records from MSGraph and Update Database
#'
#' Retrieves new call records from MSGraph since the last update, processes them, and updates relevant database tables.
#'
#' @param con A PostgreSQL database connection object.
#' @param access_token MSGraph API access token.
#'
#' @return No return value. Updates database tables with new call records and participants.
#'
#' @export
#'
#' @examples
#' msgraph_update_calls(con, access_token)
msgraph_update_calls <- function(con, access_token) {

  old_calls_file <- dplyr::tbl(con, I("raw.msgraph_calls")) %>% dplyr::collect()

  startDate <- as.Date(max(old_calls_file$updated_at)) - 1
  all_records <- retrieve_all_call_records(access_token, startDate)
  call_records_df <- as.data.frame(t(sapply(all_records, function(x) if(is.null(x)) NA else x)), stringsAsFactors = FALSE)
  new_call_found <- nrow(call_records_df) >= 5

  if (new_call_found) {

    call_records_df <- call_records_df %>%
      dplyr::select(id, type, call_start = startDateTime, call_end = endDateTime, joinWebUrl) %>%
      dplyr::mutate(id = as.character(id)) %>%
      dplyr::filter(!(id %in% old_calls_file$msgraph_call_id))

    if(nrow(call_records_df) == 0) {
      print("No new Call Records found!")
      break
    }

    print(paste0(nrow(call_records_df), " new Call Records found!"))

    # ----- Get all participants of one Call -----
    call_meeting_record <- update_call_records_with_caller_data(access_token, call_records_df)

    update_calls(con, call_meeting_record)
    update_contacts_from_calls(con, call_meeting_record)
    update_call_participants(con, call_meeting_record)

  } else {

    print("No new Call Records found!")

  }

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

update_call_records_with_caller_data <- function(access_token, call_records_df) {
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

  all_participants_ <- all_participants %>%
    dplyr::mutate(id = as.character(id), call_id = as.character(call_id)) %>%
    tidyr::unnest(identity, names_sep = "_") %>%
    dplyr::filter(identity != "NULL") %>%
    tidyr::unnest_wider(identity, names_sep = "_") %>%
    dplyr::mutate(
      intern_call = !(
        is.na(identity_tenantId) |
          identity_tenantId != "f34ad13c-3f44-4e15-b70c-cadfbdb6bfb8"
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
      dplyr::filter(!is.na(contact_id) & contact_id != "") %>%
      dplyr::filter(!is.na(call_id) & call_id != "") %>%
      dplyr::distinct(contact_id, call_id, .keep_all = TRUE),
    match_cols = c("contact_id", "call_id")
  )

}