################################################################################
# MS Graph Transcript Loading Module
#
# Functions for retrieving call transcripts from Microsoft Graph API
#
# This module contains all logic for transcript retrieval, including:
# - MS Graph API authentication and calls
# - Transcript metadata retrieval
# - Transcript content download
# - Database operations
################################################################################

#' Retrieve and Save Transcripts
#'
#' Main function to retrieve transcripts from MS Graph API and save to database
#'
#' @param con Database connection
#' @param msgraph_keys List containing MS Graph API credentials with elements:
#'   - tenant_id: Azure AD tenant ID
#'   - client_id: Azure AD client/application ID
#'   - client_secret: Azure AD client secret
#' @param start_date Start date for retrieval (NULL = from last transcript in DB)
#' @param end_date End date for retrieval
#' @param logger Logger function
#' @return Invisible NULL
#' @keywords internal
retrieve_and_save_transcripts <- function(con,
                                          msgraph_keys,
                                          start_date = NULL,
                                          end_date = Sys.Date() + 1,
                                          logger = function(msg, level = "INFO") cat(msg, "\n")) {

  # Load required data
  users <- dplyr::tbl(con, I("raw.msgraph_users")) %>%
    dplyr::filter(is_internal & !is_deleted) %>%
    dplyr::mutate(id = msgraph_user_id) %>%
    dplyr::collect()

  calls <- dplyr::tbl(con, I("raw.msgraph_calls")) %>%
    dplyr::collect()

  call_event_mapping <- dplyr::tbl(con, I("mapping.msgraph_call_event")) %>%
    dplyr::collect()

  logger(sprintf("Loaded %d users, %d calls", nrow(users), nrow(calls)), "DEBUG")

  # Prepare authentication
  authentication_msgraph <- list(
    tenant_id = msgraph_keys$tenant_id,
    client_id = msgraph_keys$client_id,
    access_token = msgraph_keys$client_secret
  )

  # Call main retrieval function
  get_and_save_transcript_data(
    con = con,
    authentication_msgraph = authentication_msgraph,
    users = users,
    call_event_mapping = call_event_mapping,
    calls = calls,
    start_date = start_date,
    end_date = end_date
  )

  logger("Transcript loading completed", "INFO")
  invisible(NULL)
}


################################################################################
# Core Transcript Retrieval Functions
################################################################################

#' Get and Save Transcript Data
#'
#' Main orchestrator for transcript retrieval process
#' @keywords internal
get_and_save_transcript_data <- function(con,
                                        authentication_msgraph,
                                        users,
                                        call_event_mapping,
                                        calls,
                                        start_date = NULL,
                                        end_date = Sys.Date() + 1) {

  # Get latest known date in DB
  latest_in_db <- get_latest_transcript_timestamp(con)
  latest_in_db <- as.POSIXct(latest_in_db, format = "%Y-%m-%dT%H:%M:%SZ", tz = "UTC")

  # Parse incoming start_date
  if (!is.null(start_date)) {
    start_date <- as.POSIXct(start_date, tz = "UTC")
  }

  # Determine final start_date
  start_date <- if (is.null(start_date)) {
    latest_in_db
  } else {
    as.POSIXct(start_date, tz = "UTC") + lubridate::hours(2)
  }

  # Parse end_date
  end_date <- as.POSIXct(end_date, tz = "UTC") + lubridate::hours(2)

  # Validate date order
  if (end_date < start_date) {
    stop("Aborted: end_date (", format(end_date, "%Y-%m-%dT%H:%M:%SZ"),
         ") is earlier than start_date (", format(start_date, "%Y-%m-%dT%H:%M:%SZ"), ").")
  }

  # Get existing transcript IDs (for deduplication)
  existing_transcript_ids <- tryCatch({
    dplyr::tbl(con, I("processed.msgraph_call_transcripts")) %>%
      dplyr::filter(
        transcript_created_at >= !!start_date,
        !is.na(call_id)
      ) %>%
      dplyr::select(transcript_id) %>%
      dplyr::collect() %>%
      dplyr::pull(transcript_id)
  }, error = function(e) {
    message("Could not load existing transcript IDs: ", e$message)
    character(0)
  })

  message("Found ", length(existing_transcript_ids), " existing transcript(s) with call_id in DB")

  # Format and URL-encode timestamps for API
  start_encoded <- utils::URLencode(format(start_date, "%Y-%m-%dT%H:%M:%SZ"), reserved = TRUE)
  end_encoded   <- utils::URLencode(format(end_date, "%Y-%m-%dT%H:%M:%SZ"), reserved = TRUE)

  message("Fetching transcripts from: ", format(start_date, "%Y-%m-%dT%H:%M:%SZ"),
          " to ", format(end_date, "%Y-%m-%dT%H:%M:%SZ"))

  # Fetch transcripts
  new_transcripts <- fetch_transcripts_for_all_users(
    users = users,
    all_events_categorised_df = call_event_mapping,
    calls = calls,
    authentication_msgraph = authentication_msgraph,
    start_date = start_encoded,
    end_date = end_encoded,
    existing_transcript_ids = existing_transcript_ids
  )

  # Save to database
  if (nrow(new_transcripts) > 0) {
    Billomatics::postgres_upsert_data(
      con = con,
      schema = "processed",
      table = "msgraph_call_transcripts",
      data = new_transcripts %>%
        dplyr::mutate(transcript_created_at = lubridate::ymd_hms(transcript_created_at)),
      match_cols = c("transcript_id")
    )

    message("New transcript data written to database.")
  } else {
    message("No new transcripts found in the specified date range.")
  }

  invisible(NULL)
}


#' Fetch Transcripts for All Users
#'
#' Iterates through all users and retrieves their transcripts
#' @keywords internal
fetch_transcripts_for_all_users <- function(users,
                                            all_events_categorised_df,
                                            calls,
                                            authentication_msgraph,
                                            start_date,
                                            end_date,
                                            existing_transcript_ids = character(0)) {
  transcript_rows <- list()

  # Generate access token
  access_token <- MSGraph::authorize_graph(
    authentication_msgraph$tenant_id,
    authentication_msgraph$client_id,
    authentication_msgraph$access_token
  )

  for (user_id in users$msgraph_user_id) {
    # Get transcript metadata
    metadata <- retrieve_transcript_metadata(access_token, user_id, start_date, end_date)
    if (is.null(metadata$value) || length(metadata$value) == 0) next

    for (entry in metadata$value) {
      # Refresh token (might expire during long runs)
      access_token <- MSGraph::authorize_graph(
        authentication_msgraph$tenant_id,
        authentication_msgraph$client_id,
        authentication_msgraph$access_token
      )

      # Get meeting metadata
      meeting_metadata <- tryCatch({
        get_meeting_metadata(access_token, user_id, entry$meetingId)
      }, error = function(e) {
        warning("Failed to retrieve meeting metadata: ", e$message)
        NULL
      })

      if (is.null(meeting_metadata)) {
        warning("No meeting metadata for user ", user_id, " and meeting ", entry$meetingId)
        next
      }

      # Download transcript content
      transcript <- get_transcript_row(
        entry,
        meeting_metadata,
        all_events_categorised_df,
        calls,
        access_token,
        user_id,
        existing_transcript_ids
      )

      if (!is.null(transcript)) {
        transcript_rows[[length(transcript_rows) + 1]] <- transcript
      }
    }
  }

  # Combine all transcripts
  transcripts_df <- if (length(transcript_rows) > 0) {
    dplyr::bind_rows(transcript_rows)
  } else {
    tibble::tibble()
  }

  return(transcripts_df)
}


#' Get Transcript Row
#'
#' Processes a single transcript entry and returns formatted data
#' @keywords internal
get_transcript_row <- function(entry,
                               meeting_metadata,
                               all_events_categorised_df,
                               calls,
                               access_token,
                               user_id,
                               existing_transcript_ids = character(0)) {

  meeting_id <- meeting_metadata$chatInfo$threadId
  creation_datetime_transcript <- lubridate::ymd_hms(entry$createdDateTime)
  transcript_id <- as.character(entry$id)

  # Extract organizer ID
  organizer_id <- tryCatch({
    as.character(meeting_metadata$participants$organizer$identity$user$id)
  }, error = function(e) {
    NA_character_
  })

  # Match to call
  call_id <- get_call_id(meeting_id, creation_datetime_transcript, calls)

  # Find interesting (external) calls
  interesting_calls <- all_events_categorised_df %>%
    dplyr::filter(grepl("extern", event_class)) %>%
    dplyr::distinct(call_id)

  # Check if already exists
  if (transcript_id %in% existing_transcript_ids) {
    message("Skipping transcript ", transcript_id, " - already in database")
    return(NULL)
  }

  # Download content if it's an interesting call
  if(!is.na(call_id) & (call_id %in% interesting_calls$call_id)) {

    transcript_content <- tryCatch({
      get_content_transcript_url(access_token, entry$transcriptContentUrl)
    }, error = function(e) {
      warning("Failed to retrieve transcript content: ", e$message)
      NA
    })

    return(tibble::tibble(
      transcript_id = transcript_id,
      call_id = as.integer(call_id),
      organizer_id = organizer_id,
      transcript_url = as.character(entry$transcriptContentUrl),
      transcript_created_at = lubridate::ymd_hms(entry$createdDateTime),
      transcript_content = as.character(transcript_content),
      transcript_content_anonymized = NA_character_,
      transcript_summary = NA_character_,
      transcript_summary_anonymized = NA_character_
    ))

  } else {

    warning("Not interested in call ", call_id)

    return(tibble::tibble(
      transcript_id = transcript_id,
      call_id = as.integer(call_id),
      organizer_id = organizer_id,
      transcript_url = as.character(entry$transcriptContentUrl),
      transcript_created_at = lubridate::ymd_hms(creation_datetime_transcript),
      transcript_content = NA_character_,
      transcript_content_anonymized = NA_character_,
      transcript_summary = NA_character_,
      transcript_summary_anonymized = NA_character_
    ))
  }
}


################################################################################
# MS Graph API Functions
################################################################################

#' Retrieve Transcript Metadata
#'
#' Gets transcript metadata from MS Graph API
#' @keywords internal
retrieve_transcript_metadata <- function(access_token, user_id, start_date, end_date) {

  start_date <- utils::URLencode(start_date, reserved = TRUE)
  end_date   <- utils::URLencode(end_date, reserved = TRUE)

  url <- paste0(
    "https://graph.microsoft.com/beta/users/", user_id,
    "/onlineMeetings/getAllTranscripts(meetingOrganizerUserId='", user_id,
    "',startDateTime=", start_date, ",endDateTime=", end_date, ")"
  )

  response <- httr::GET(url, httr::add_headers(Authorization = paste("Bearer", access_token)))

  status <- httr::http_status(response)

  if (status$category != "Success") {
    return(NULL)
  }

  parsed_response <- httr::content(response, as = "parsed", type = "application/json")

  if (!is.list(parsed_response) || is.null(parsed_response$value) ||
      length(parsed_response$value) == 0) {
    return(NULL)
  }

  return(parsed_response)
}


#' Get Meeting Metadata
#'
#' Gets full meeting metadata from MS Graph API
#' @keywords internal
get_meeting_metadata <- function(access_token, user_id, meeting_id) {

  url <- paste0(
    "https://graph.microsoft.com/beta/users/", user_id,
    "/onlineMeetings/", meeting_id
  )

  response <- httr::GET(
    url,
    httr::add_headers(Authorization = paste("Bearer", access_token))
  )

  if (httr::http_status(response)$category != "Success") {
    warning("Failed to fetch meeting metadata: ", httr::http_status(response)$message)
    return(NULL)
  }

  parsed_response <- httr::content(response, as = "parsed", type = "application/json")

  return(parsed_response)
}


#' Get Content from Transcript URL
#'
#' Downloads transcript content (VTT format)
#' @keywords internal
get_content_transcript_url <- function(access_token, content_url) {
  response <- httr::GET(
    content_url,
    httr::add_headers(
      Authorization = paste("Bearer", access_token),
      Accept = "text/vtt"
    )
  )

  if (httr::http_status(response)$category != "Success") {
    stop("Failed to fetch transcript content.")
  }

  content_text <- httr::content(response, as = "text", encoding = "UTF-8")
  return(content_text)
}


################################################################################
# Helper Functions
################################################################################

#' Get Call ID from Meeting ID
#'
#' Matches transcript to call record
#' @keywords internal
get_call_id <- function(metadata_meeting_id, creation_datetime_transcript, calls) {
  matched_call <- calls %>%
    dplyr::filter(
      meeting_id == metadata_meeting_id,
      call_start < creation_datetime_transcript,
      call_end > creation_datetime_transcript
    ) %>%
    dplyr::slice(1)

  call_id <- matched_call$id

  if (length(call_id) == 0) {
     return(NA)
  }
  return(call_id)
}


#' Get Latest Transcript Timestamp
#'
#' Returns the latest transcript date from database
#' @keywords internal
get_latest_transcript_timestamp <- function(con) {

  if (!DBI::dbExistsTable(con, DBI::Id(schema = "processed", table = "msgraph_call_transcripts"))) {
    return("2022-01-01T00:00:00Z")
  }

  query <- "SELECT MAX(transcript_created_at) AS latest FROM processed.msgraph_call_transcripts;"
  latest <- DBI::dbGetQuery(con, query)$latest

  if (is.na(latest) || is.null(latest)) {
    return("2025-05-25T00:00:00Z")
  }

  # Return beginning of the day
  latest_date <- as.Date(as.POSIXct(latest, tz = "UTC"))
  format(as.POSIXct(paste0(latest_date, " 00:00:00"), tz = "UTC"), "%Y-%m-%dT%H:%M:%SZ")
}
