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

#' Retrieve Transcripts from MS Graph
#'
#' Main function to retrieve transcripts from MS Graph API and save to database.
#' This is step 1 of the transcript processing pipeline.
#'
#' @param con Database connection
#' @param msgraph_keys List containing MS Graph API credentials with elements:
#'   - tenant_id: Azure AD tenant ID
#'   - client_id: Azure AD client/application ID
#'   - client_secret: Azure AD client secret
#' @param start_date Start date for retrieval (NULL = from last transcript in DB)
#' @param end_date End date for retrieval (defaults to tomorrow)
#' @param logger Logger function for output messages
#'
#' @return Invisible NULL (updates database)
#'
#' @examples
#' \dontrun{
#' msgraph_retrieve_transcripts(
#'   con = con,
#'   msgraph_keys = list(
#'     tenant_id = "your-tenant-id",
#'     client_id = "your-client-id",
#'     client_secret = keys$msgraph
#'   )
#' )
#' }
#'
#' @export
msgraph_retrieve_transcripts <- function(con,
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

  # Get existing transcript IDs that already have content (for deduplication).
  # Placeholder rows with NULL content are intentionally excluded so they get
  # re-evaluated against an updated mapping on the next run (self-heals the
  # race condition where a call was not yet classified as extern at first load).
  existing_transcript_ids <- tryCatch({
    dplyr::tbl(con, I("processed.msgraph_call_transcripts")) %>%
      dplyr::filter(
        transcript_created_at >= !!start_date,
        !is.na(call_id),
        !is.na(transcript_content)
      ) %>%
      dplyr::select(transcript_id) %>%
      dplyr::collect() %>%
      dplyr::pull(transcript_id)
  }, error = function(e) {
    message("Could not load existing transcript IDs: ", e$message)
    character(0)
  })

  message("Found ", length(existing_transcript_ids), " existing transcript(s) with content in DB")

  # Format and URL-encode timestamps for API
  start_encoded <- utils::URLencode(format(start_date, "%Y-%m-%dT%H:%M:%SZ"), reserved = TRUE)
  end_encoded   <- utils::URLencode(format(end_date, "%Y-%m-%dT%H:%M:%SZ"), reserved = TRUE)

  message("Fetching transcripts from: ", format(start_date, "%Y-%m-%dT%H:%M:%SZ"),
          " to ", format(end_date, "%Y-%m-%dT%H:%M:%SZ"))

  # Pass 1: organizer-scoped enumeration via getAllTranscripts.
  new_transcripts <- fetch_transcripts_for_all_users(
    users = users,
    all_events_categorised_df = call_event_mapping,
    calls = calls,
    authentication_msgraph = authentication_msgraph,
    start_date = start_encoded,
    end_date = end_encoded,
    existing_transcript_ids = existing_transcript_ids,
    con = con
  )

  # Pass 2: close the gaps pass 1 cannot see.
  #
  # getAllTranscripts enumerates over the organizer's Exchange calendar — the
  # IDs it returns carry a calendar anchor ("2##1<oid><tenant><GlobalObjectId>##
  # <guid>"). Meetings whose anchor is missing or stale are simply absent from
  # that result, without any error. The meeting-scoped endpoint enumerates over
  # the call instead and still knows them; verified on call_id 666374, where
  # getAllTranscripts returned nothing and the meeting endpoint returned an
  # intact transcript of 82716 characters.
  #
  # This pass is deliberately additive: pass 1 covers the vast majority and
  # stays untouched. Once getAllTranscripts is dropped for the tenant migration
  # (see the endpoint overview, which already lists it as replaced by #6), the
  # call to pass 1 above is the only thing that needs to go.
  completion_transcripts <- fetch_transcripts_for_open_calls(
    con                     = con,
    authentication_msgraph  = authentication_msgraph,
    users                   = users,
    calls                   = calls,
    call_event_mapping      = call_event_mapping,
    start_date              = start_date,
    end_date                = end_date,
    handled_call_ids        = if (nrow(new_transcripts) > 0) new_transcripts$call_id else integer(0)
  )

  new_transcripts <- dplyr::bind_rows(new_transcripts, completion_transcripts)

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
                                            existing_transcript_ids = character(0),
                                            con = NULL) {
  transcript_rows <- list()
  processed_transcript_ids <- character(0)  # Track IDs within this batch

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
        message("[WARNING] get_meeting_metadata failed for user ", user_id, ": ", e$message)
        NULL
      })

      # Retry with organizer + call participants if primary user failed
      if (is.null(meeting_metadata) && !is.null(con)) {
        transcript_time <- lubridate::ymd_hms(entry$createdDateTime)
        meeting_metadata <- retry_meeting_metadata_with_participants(
          access_token = access_token,
          primary_user_id = user_id,
          meeting_id = entry$meetingId,
          transcript_created_at = transcript_time,
          con = con,
          calls = calls,
          call_event_mapping = all_events_categorised_df,
          users = users
        )
      }

      if (is.null(meeting_metadata)) {
        message("[WARNING] No meeting metadata for user ", user_id,
                " and meeting ", entry$meetingId, " (all retries exhausted)")
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
        tid <- transcript$transcript_id[1]
        if (tid %in% processed_transcript_ids) {
          message("[DEBUG] SKIP: transcript ", tid, " already processed in this batch")
          next
        }
        processed_transcript_ids <- c(processed_transcript_ids, tid)
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

  message("[DEBUG] === Processing transcript ", transcript_id, " ===")
  message("[DEBUG] meeting_id: ", meeting_id)
  message("[DEBUG] created: ", creation_datetime_transcript)
  message("[DEBUG] transcriptContentUrl present: ", !is.null(entry$transcriptContentUrl))

  # Extract organizer ID
  organizer_id <- tryCatch({
    as.character(meeting_metadata$participants$organizer$identity$user$id)
  }, error = function(e) {
    NA_character_
  })
  message("[DEBUG] organizer_id: ", organizer_id)

  # Match to call
  call_id <- get_call_id(meeting_id, creation_datetime_transcript, calls)
  message("[DEBUG] matched call_id: ", call_id, " (is.na: ", is.na(call_id), ")")

  if (is.na(call_id)) {
    # Debug: show what calls exist for this meeting_id
    matching_calls <- calls %>%
      dplyr::filter(meeting_id == !!meeting_id)
    message("[DEBUG] calls with same meeting_id: ", nrow(matching_calls))
    if (nrow(matching_calls) > 0) {
      for (i in seq_len(nrow(matching_calls))) {
        message("[DEBUG]   call ", matching_calls$id[i],
                " | start: ", matching_calls$call_start[i],
                " | end: ", matching_calls$call_end[i])
      }
      message("[DEBUG]   transcript created at: ", creation_datetime_transcript,
              " (must be between call_start and call_end)")
    }
  }

  # Find interesting (external) calls
  interesting_calls <- all_events_categorised_df %>%
    dplyr::filter(grepl("extern", event_class)) %>%
    dplyr::distinct(call_id)

  message("[DEBUG] total interesting (extern) calls: ", nrow(interesting_calls))

  if (!is.na(call_id)) {
    is_interesting <- call_id %in% interesting_calls$call_id
    message("[DEBUG] call_id ", call_id, " is interesting (extern): ", is_interesting)

    if (!is_interesting) {
      # Show what event_class this call actually has
      call_event_class <- all_events_categorised_df %>%
        dplyr::filter(call_id == !!call_id) %>%
        dplyr::select(dplyr::any_of(c("call_id", "event_class")))
      if (nrow(call_event_class) > 0) {
        message("[DEBUG] actual event_class for call_id ", call_id, ": ",
                paste(call_event_class$event_class, collapse = ", "))
      } else {
        message("[DEBUG] call_id ", call_id, " NOT FOUND in all_events_categorised_df at all!")
      }
    }
  }

  # Check if already exists
  if (transcript_id %in% existing_transcript_ids) {
    message("[DEBUG] SKIP: transcript ", transcript_id, " already in database")
    return(NULL)
  }

  # Download content if it's an interesting call
  if(!is.na(call_id) & (call_id %in% interesting_calls$call_id)) {
    message("[DEBUG] DOWNLOADING transcript content...")

    transcript_content <- tryCatch({
      content <- get_content_transcript_url(access_token, entry$transcriptContentUrl)
      message("[DEBUG] Download SUCCESS, content length: ", nchar(content), " chars")
      content
    }, error = function(e) {
      message("[DEBUG] Download FAILED: ", e$message)
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

    if (is.na(call_id)) {
      message("[DEBUG] SKIP DOWNLOAD: no matching call_id found (call_id is NA)")
    } else {
      message("[DEBUG] SKIP DOWNLOAD: call_id ", call_id, " is not classified as extern")
    }
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
# Meeting-scoped Completion Pass
#
# getAllTranscripts enumerates over the organizer's Exchange calendar and
# silently omits meetings whose calendar anchor is missing or stale. The
# functions below enumerate over the call instead, which the meeting-scoped
# endpoint /onlineMeetings/{id}/transcripts still knows about.
################################################################################

#' Build the Graph Online-Meeting ID
#'
#' The ID is deterministic and needs no lookup call:
#'   base64("1*<organizer-oid>*0**<threadId>")
#' Verified byte-for-byte against a stored transcript_url.
#'
#' jsonlite::base64_enc wraps long output — the line breaks have to go, they
#' would corrupt the URL.
#'
#' @param organizer_oid msgraph_user_id of the organising person
#' @param thread_id Teams thread id, as stored in raw.msgraph_calls.meeting_id
#'
#' @return base64-encoded meeting id
#' @keywords internal
# ---- start ---- #
build_online_meeting_id <- function(organizer_oid, thread_id) {
  gsub("[\r\n]", "",
       jsonlite::base64_enc(charToRaw(paste0("1*", organizer_oid, "*0**", thread_id))))
}


#' Does a transcript fall into a call's time window?
#'
#' One Teams meeting can produce several call records, so a transcript has to be
#' attributed to exactly one of them. The transcript id cannot be used for that:
#' unlike the getAllTranscripts ids (base64 of "2##1<oid><tenant><GOID>##<guid>")
#' the meeting-scoped ids are opaque and carry no call reference.
#'
#' The time window is what production already uses in get_call_id(): same
#' meeting, and the transcript created strictly between call start and end.
#' Checked against both known cases — each transcript falls into its own call's
#' window and into no other.
#'
#' @param created_date_time createdDateTime from the Graph response
#' @param call_start Start of the call (POSIXct)
#' @param call_end End of the call (POSIXct)
#'
#' @return TRUE when the transcript was created during the call
#' @keywords internal
# ---- start ---- #
transcript_within_call_window <- function(created_date_time, call_start, call_end) {
  created <- suppressWarnings(
    lubridate::ymd_hms(as.character(created_date_time), quiet = TRUE)
  )
  if (length(created) == 0 || is.na(created[1])) return(FALSE)

  isTRUE(created[1] > call_start && created[1] < call_end)
}


#' Is this transcript entry a dead stub?
#'
#' The meeting-scoped endpoint also returns stubs that never carry content:
#' end date on the zero date and no contentCorrelationId. They answer every
#' content request with 404.
#'
#' @param entry One entry of the Graph response
#'
#' @return TRUE for a stub
#' @keywords internal
# ---- start ---- #
is_dead_transcript_stub <- function(entry) {
  correlation_id <- entry$contentCorrelationId
  end_date_time  <- entry$endDateTime

  no_correlation <- is.null(correlation_id) || length(correlation_id) == 0 ||
    is.na(correlation_id[1]) || !nzchar(as.character(correlation_id[1]))
  zero_date <- !is.null(end_date_time) && length(end_date_time) > 0 &&
    !is.na(end_date_time[1]) && grepl("^0001-01-01", as.character(end_date_time[1]))

  no_correlation || zero_date
}


#' List all transcripts of one meeting
#'
#' @param access_token MS Graph access token
#' @param user_id msgraph_user_id used as the calling identity
#' @param meeting_id base64 meeting id from build_online_meeting_id()
#'
#' @return List of entries, empty when nothing is available
#' @keywords internal
# ---- start ---- #
list_meeting_transcripts <- function(access_token, user_id, meeting_id) {
  url <- paste0("https://graph.microsoft.com/beta/users/", user_id,
                "/onlineMeetings/", meeting_id, "/transcripts")

  parsed <- fetch_with_retry(url, access_token)

  if (!is.list(parsed) || is.null(parsed$value) || length(parsed$value) == 0) {
    return(list())
  }

  parsed$value
}


#' Internal users that can be used to query a call's meeting
#'
#' Priority: the organiser of the calendar event first, then every internal
#' participant of the call. Only the organiser's identity resolves the meeting
#' id, so the order matters.
#'
#' @param con Database connection
#' @param call_ids Vector of call ids
#' @param call_event_mapping Data frame of call-event mappings (already loaded)
#' @param users Data frame of internal users (already loaded)
#'
#' @return Data frame with call_id, msgraph_user_id and priority
#' @keywords internal
# ---- start ---- #
get_internal_candidates_for_calls <- function(con, call_ids, call_event_mapping, users) {
  empty <- tibble::tibble(call_id = integer(0), msgraph_user_id = character(0),
                          priority = integer(0))
  if (length(call_ids) == 0) return(empty)

  user_lookup <- users %>%
    dplyr::select(msgraph_user_id, email) %>%
    dplyr::filter(!is.na(email))

  # 1. organiser of the calendar event
  event_map <- call_event_mapping %>%
    dplyr::filter(call_id %in% !!call_ids, !is.na(event_id), event_id > 0) %>%
    dplyr::select(call_id, event_id)

  organizers <- empty
  if (nrow(event_map) > 0) {
    organizers <- tryCatch({
      dplyr::tbl(con, I("raw.msgraph_event_participants")) %>%
        dplyr::filter(event_id %in% !!unique(event_map$event_id), is_organizer == TRUE) %>%
        dplyr::inner_join(
          dplyr::tbl(con, I("raw.msgraph_contacts")) %>% dplyr::select(id, email),
          by = c("contact_id" = "id")
        ) %>%
        dplyr::select(event_id, email) %>%
        dplyr::collect() %>%
        dplyr::inner_join(event_map, by = "event_id") %>%
        dplyr::inner_join(user_lookup, by = "email") %>%
        dplyr::transmute(call_id, msgraph_user_id, priority = 1L)
    }, error = function(e) {
      message("[WARNING] Could not resolve event organisers: ", e$message)
      empty
    })
  }

  # 2. internal participants of the call
  participants <- tryCatch({
    dplyr::tbl(con, I("raw.msgraph_call_participants")) %>%
      dplyr::filter(call_id %in% !!call_ids) %>%
      dplyr::inner_join(
        dplyr::tbl(con, I("raw.msgraph_contacts")) %>% dplyr::select(id, email),
        by = c("contact_id" = "id")
      ) %>%
      dplyr::select(call_id, email) %>%
      dplyr::collect() %>%
      dplyr::inner_join(user_lookup, by = "email") %>%
      dplyr::transmute(call_id, msgraph_user_id, priority = 2L)
  }, error = function(e) {
    message("[WARNING] Could not resolve call participants: ", e$message)
    empty
  })

  dplyr::bind_rows(organizers, participants) %>%
    dplyr::arrange(call_id, priority) %>%
    dplyr::distinct(call_id, msgraph_user_id, .keep_all = TRUE)
}


#' Fetch transcripts for calls that pass 1 left without content
#'
#' Walks the external calls of the load window that still have no transcript
#' content, builds the meeting id from organiser and thread id, and reads the
#' transcript from the meeting-scoped endpoint.
#'
#' Attribution is by time window — same meeting, transcript created strictly
#' between call start and end, exactly as get_call_id() does it. One Teams
#' meeting can produce several call records; the window is what separates them.
#' Anything ambiguous is left alone rather than guessed.
#'
#' @param con Database connection
#' @param authentication_msgraph List with tenant_id, client_id, access_token
#' @param users Data frame of internal users (already loaded)
#' @param calls Data frame of calls (already loaded)
#' @param call_event_mapping Data frame of call-event mappings (already loaded)
#' @param start_date Start of the load window (POSIXct)
#' @param end_date End of the load window (POSIXct)
#' @param handled_call_ids Call ids that pass 1 already produced a row for
#' @param max_calls Upper bound per run; a truncation is logged, never silent
#'
#' @return Tibble in the same shape as get_transcript_row(), possibly empty
#' @keywords internal
# ---- start ---- #
fetch_transcripts_for_open_calls <- function(con,
                                             authentication_msgraph,
                                             users,
                                             calls,
                                             call_event_mapping,
                                             start_date,
                                             end_date,
                                             handled_call_ids = integer(0),
                                             max_calls = 150) {

  empty_result <- tibble::tibble()

  extern_call_ids <- call_event_mapping %>%
    dplyr::filter(grepl("extern", event_class), !is.na(call_id)) %>%
    dplyr::pull(call_id) %>%
    unique()

  if (length(extern_call_ids) == 0) return(empty_result)

  open_calls <- calls %>%
    dplyr::filter(
      id %in% extern_call_ids,
      !is.na(meeting_id), nzchar(meeting_id),
      call_start >= start_date,
      call_start <= end_date
    )

  # Never touch a call that pass 1 already handled — the two endpoints use
  # different transcript id spaces, so a second row would not be deduplicated
  # by match_cols = transcript_id and the call would end up with two rows.
  if (length(handled_call_ids) > 0) {
    open_calls <- open_calls %>% dplyr::filter(!id %in% handled_call_ids)
  }

  calls_with_content <- tryCatch({
    dplyr::tbl(con, I("processed.msgraph_call_transcripts")) %>%
      dplyr::filter(!is.na(transcript_content), !is.na(call_id)) %>%
      dplyr::distinct(call_id) %>%
      dplyr::collect() %>%
      dplyr::pull(call_id)
  }, error = function(e) {
    message("[WARNING] Could not read existing transcript content: ", e$message)
    integer(0)
  })

  if (length(calls_with_content) > 0) {
    open_calls <- open_calls %>% dplyr::filter(!id %in% calls_with_content)
  }

  if (nrow(open_calls) == 0) {
    message("[completion] No external calls without transcript content in the window.")
    return(empty_result)
  }

  open_calls <- open_calls %>% dplyr::arrange(dplyr::desc(call_start))

  if (nrow(open_calls) > max_calls) {
    message("[completion] ", nrow(open_calls), " open calls, processing the ",
            max_calls, " most recent — ", nrow(open_calls) - max_calls,
            " NOT processed in this run (max_calls).")
    open_calls <- open_calls[seq_len(max_calls), ]
  } else {
    message("[completion] ", nrow(open_calls), " external call(s) without content to check.")
  }

  candidates <- get_internal_candidates_for_calls(
    con, open_calls$id, call_event_mapping, users
  )

  access_token <- MSGraph::authorize_graph(
    authentication_msgraph$tenant_id,
    authentication_msgraph$client_id,
    authentication_msgraph$access_token
  )

  rows       <- list()
  recovered  <- 0L
  not_found  <- 0L

  for (i in seq_len(nrow(open_calls))) {
    call_row <- open_calls[i, ]

    uids <- candidates %>%
      dplyr::filter(call_id == call_row$id[1]) %>%
      dplyr::pull(msgraph_user_id)

    if (length(uids) == 0) {
      message("[completion] call ", call_row$id[1], ": no internal candidate")
      not_found <- not_found + 1L
      next
    }

    entries <- list()
    for (uid in uids) {
      entries <- list_meeting_transcripts(
        access_token, uid, build_online_meeting_id(uid, call_row$meeting_id[1])
      )
      if (length(entries) > 0) break
    }

    if (length(entries) == 0) {
      not_found <- not_found + 1L
      next
    }

    alive <- Filter(function(e) !is_dead_transcript_stub(e), entries)
    if (length(alive) == 0) {
      message("[completion] call ", call_row$id[1], ": only dead stub(s) available")
      not_found <- not_found + 1L
      next
    }

    matched <- Filter(
      function(e) transcript_within_call_window(
        e$createdDateTime, call_row$call_start[1], call_row$call_end[1]
      ),
      alive
    )

    # Exactly one hit or nothing. Two transcripts inside the same call window
    # would be ambiguous, and guessing is worse than leaving the call open —
    # it stays a candidate for the next run.
    if (length(matched) != 1) {
      message("[completion] call ", call_row$id[1], ": ", length(matched),
              " transcript(s) inside the call window out of ", length(alive),
              " live entr(y/ies) - skipped")
      not_found <- not_found + 1L
      next
    }

    entry <- matched[[1]]

    content_url <- as.character(entry$transcriptContentUrl)
    if (length(content_url) == 0 || is.na(content_url) || !nzchar(content_url)) {
      message("[completion] call ", call_row$id[1], ": entry without transcriptContentUrl")
      not_found <- not_found + 1L
      next
    }

    transcript_content <- tryCatch(
      get_content_transcript_url(access_token, content_url),
      error = function(e) {
        message("[completion] call ", call_row$id[1], ": download failed - ", e$message)
        NA_character_
      }
    )

    # No placeholder row on failure: the call keeps no content and is retried on
    # the next run, which is the self-healing behaviour we want here.
    if (is.na(transcript_content) || !nzchar(transcript_content)) {
      not_found <- not_found + 1L
      next
    }

    # raw.msgraph_calls.id is a bigint and arrives as integer64. Without bit64
    # loaded, as.integer() on it silently yields 0 instead of NA — that would
    # attach the transcript to a non-existent call. Fail loudly instead.
    call_id_value <- suppressWarnings(as.integer(call_row$id[1]))
    if (is.na(call_id_value) || call_id_value == 0L) {
      message("[completion] call ", call_row$id[1],
              ": call_id conversion failed (integer64 without bit64?) - skipped")
      not_found <- not_found + 1L
      next
    }

    rows[[length(rows) + 1]] <- tibble::tibble(
      transcript_id                 = as.character(entry$id),
      call_id                       = call_id_value,
      transcript_url                = content_url,
      transcript_created_at         = lubridate::ymd_hms(as.character(entry$createdDateTime)),
      transcript_content            = as.character(transcript_content),
      transcript_content_anonymized = NA_character_,
      transcript_summary            = NA_character_,
      transcript_summary_anonymized = NA_character_
    )
    recovered <- recovered + 1L
  }

  message("[completion] recovered ", recovered, " transcript(s), ",
          not_found, " call(s) without a usable transcript.")

  if (length(rows) == 0) return(empty_result)

  dplyr::bind_rows(rows)
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

  # fetch_with_retry handles 401 refresh, 429 throttling and 5xx retries;
  # returns NULL when all retries are exhausted.
  parsed_response <- fetch_with_retry(url, access_token)

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

  # fetch_with_retry handles 401 refresh, 429 throttling and 5xx retries.
  # Returns NULL on a 404 (meeting not visible to this user) or after exhausting
  # retries — both cases trigger the participant-based retry in the caller.
  parsed_response <- fetch_with_retry(url, access_token)

  if (is.null(parsed_response) || length(parsed_response) == 0) {
    message("[DEBUG] get_meeting_metadata failed (empty/404/retries exhausted) ",
            "for user ", user_id, ", meeting ", meeting_id,
            " — see fetch_with_retry messages")
    return(NULL)
  }

  return(parsed_response)
}


#' Retry get_meeting_metadata with Call/Event Participants
#'
#' When get_meeting_metadata fails for the primary user, finds the organizer
#' and other internal participants via DB lookup and retries with them.
#' Priority: event organizer first, then other event/call participants.
#'
#' @param access_token MS Graph API access token
#' @param primary_user_id The user ID that already failed
#' @param meeting_id The online meeting ID to query
#' @param transcript_created_at POSIXct timestamp of the transcript
#' @param con Database connection
#' @param calls Data frame of calls (already loaded)
#' @param call_event_mapping Data frame of call-event mappings (already loaded)
#' @param users Data frame of internal users (already loaded)
#'
#' @return Meeting metadata list or NULL if all retries fail
#' @keywords internal
retry_meeting_metadata_with_participants <- function(access_token,
                                                     primary_user_id,
                                                     meeting_id,
                                                     transcript_created_at,
                                                     con,
                                                     calls,
                                                     call_event_mapping,
                                                     users) {

  # Find candidate calls: prefer exact meeting_id match, then time-window, then same-day

  # 1. Exact match by meeting_id (most reliable)
  candidate_calls <- calls %>%
    dplyr::filter(!is.na(meeting_id) & meeting_id == !!meeting_id)

  # 2. Fallback: time-window match
  if (nrow(candidate_calls) == 0) {
    candidate_calls <- calls %>%
      dplyr::filter(
        call_start < transcript_created_at,
        call_end > transcript_created_at
      )
  }

  # 3. Broader fallback: same day
  if (nrow(candidate_calls) == 0) {
    transcript_date <- as.Date(transcript_created_at)
    candidate_calls <- calls %>%
      dplyr::filter(as.Date(call_start) == transcript_date)
    if (nrow(candidate_calls) > 0) {
      message("[WARNING] Using same-day fallback for retry: ", nrow(candidate_calls),
              " candidate calls on ", transcript_date)
    }
  }

  if (nrow(candidate_calls) == 0) {
    message("[WARNING] No candidate calls found for meeting metadata retry")
    return(NULL)
  }

  candidate_call_ids <- candidate_calls$id

  # Get event_ids from already-loaded call_event_mapping
  candidate_event_ids <- call_event_mapping %>%
    dplyr::filter(call_id %in% candidate_call_ids, !is.na(event_id), event_id > 0) %>%
    dplyr::pull(event_id) %>%
    unique()

  retry_uids <- character(0)

  # 1. Event organizer (highest priority)
  if (length(candidate_event_ids) > 0) {
    organizer_uids <- tryCatch({
      dplyr::tbl(con, I("raw.msgraph_event_participants")) %>%
        dplyr::filter(event_id %in% !!candidate_event_ids, is_organizer == TRUE) %>%
        dplyr::inner_join(
          dplyr::tbl(con, I("raw.msgraph_contacts")) %>% dplyr::select(id, email),
          by = c("contact_id" = "id")
        ) %>%
        dplyr::inner_join(
          dplyr::tbl(con, I("raw.msgraph_users")) %>%
            dplyr::filter(is_internal, !is_deleted) %>%
            dplyr::select(msgraph_user_id, email),
          by = "email"
        ) %>%
        dplyr::pull(msgraph_user_id) %>%
        unique()
    }, error = function(e) {
      message("[WARNING] Failed to look up event organizer: ", e$message)
      character(0)
    })
    retry_uids <- c(retry_uids, organizer_uids)
  }

  # 2. Other event participants
  if (length(candidate_event_ids) > 0) {
    event_participant_uids <- tryCatch({
      dplyr::tbl(con, I("raw.msgraph_event_participants")) %>%
        dplyr::filter(event_id %in% !!candidate_event_ids) %>%
        dplyr::inner_join(
          dplyr::tbl(con, I("raw.msgraph_contacts")) %>% dplyr::select(id, email),
          by = c("contact_id" = "id")
        ) %>%
        dplyr::inner_join(
          dplyr::tbl(con, I("raw.msgraph_users")) %>%
            dplyr::filter(is_internal, !is_deleted) %>%
            dplyr::select(msgraph_user_id, email),
          by = "email"
        ) %>%
        dplyr::pull(msgraph_user_id) %>%
        unique()
    }, error = function(e) {
      message("[WARNING] Failed to look up event participants: ", e$message)
      character(0)
    })
    retry_uids <- c(retry_uids, event_participant_uids)
  }

  # 3. Call participants
  call_participant_uids <- tryCatch({
    dplyr::tbl(con, I("raw.msgraph_call_participants")) %>%
      dplyr::filter(call_id %in% !!candidate_call_ids) %>%
      dplyr::inner_join(
        dplyr::tbl(con, I("raw.msgraph_contacts")) %>% dplyr::select(id, email),
        by = c("contact_id" = "id")
      ) %>%
      dplyr::inner_join(
        dplyr::tbl(con, I("raw.msgraph_users")) %>%
          dplyr::filter(is_internal, !is_deleted) %>%
          dplyr::select(msgraph_user_id, email),
        by = "email"
      ) %>%
      dplyr::pull(msgraph_user_id) %>%
      unique()
  }, error = function(e) {
    message("[WARNING] Failed to look up call participants: ", e$message)
    character(0)
  })
  retry_uids <- c(retry_uids, call_participant_uids)

  # Deduplicate and remove the primary user (already tried)
  retry_uids <- unique(retry_uids)
  retry_uids <- setdiff(retry_uids, primary_user_id)

  if (length(retry_uids) == 0) {
    message("[WARNING] No alternative users found for meeting metadata retry")
    return(NULL)
  }

  message("[INFO] Retrying get_meeting_metadata with ", length(retry_uids),
          " alternative user(s) (organizer + participants)")

  for (retry_uid in retry_uids) {
    metadata <- tryCatch({
      get_meeting_metadata(access_token, retry_uid, meeting_id)
    }, error = function(e) NULL)

    if (!is.null(metadata)) {
      retry_user_name <- users$display_name[users$msgraph_user_id == retry_uid]
      if (length(retry_user_name) == 0) retry_user_name <- retry_uid
      message("[INFO] get_meeting_metadata succeeded with user ", retry_user_name[1])
      return(metadata)
    }
  }

  message("[WARNING] get_meeting_metadata failed for all ",
          length(retry_uids), " alternative users")
  return(NULL)
}


#' Get Content from Transcript URL
#'
#' Downloads transcript content (VTT format)
#' @keywords internal
get_content_transcript_url <- function(access_token, content_url) {
  # Transcript content is VTT text, not JSON. fetch_with_retry retries 5xx/429
  # and refreshes on 401. error_on_failure = TRUE makes it raise with the last
  # HTTP status and response body on definitive failure (exhausted retries or a
  # 404), so the caller's tryCatch captures that detail in e$message, logs it and
  # stores a NULL-content placeholder row which the next run re-attempts.
  content_text <- fetch_with_retry(
    url              = content_url,
    access_token     = access_token,
    accept           = "text/vtt",
    parse            = "text",
    error_on_failure = TRUE
  )

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
    message("[DEBUG get_call_id] NO match for meeting_id=", metadata_meeting_id,
            " at time=", creation_datetime_transcript,
            " (total calls in DB: ", nrow(calls), ")")
     return(NA)
  }
  message("[DEBUG get_call_id] MATCHED call_id=", call_id,
          " for meeting_id=", metadata_meeting_id)
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

  # Return beginning of the previous day so the load window spans the last two
  # days. This gives placeholder rows from yesterday a chance to be re-evaluated
  # against the now-up-to-date mapping. Re-scanning is cheap because every
  # downstream step is idempotent (skips anything already processed/exported).
  latest_date <- as.Date(as.POSIXct(latest, tz = "UTC")) - 1
  format(as.POSIXct(paste0(latest_date, " 00:00:00"), tz = "UTC"), "%Y-%m-%dT%H:%M:%SZ")
}
