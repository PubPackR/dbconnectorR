fetch_with_retry <- function(url, access_token, query = c(), max_retries = 5, delay = 2) {
  attempt <- 1
  success <- FALSE
  response_content <- NULL

  while (attempt <= max_retries && !success) {
    if (length(query) == 0) {
      response <- httr::GET(url, httr::add_headers(Authorization = paste("Bearer", access_token)))
    } else {
      response <- httr::GET(url, query = query, httr::add_headers(Authorization = paste("Bearer", access_token)))
    }

    # Check if the response is valid JSON
    if (response$status_code > 500) {
      message(paste("\nAttempt", attempt, "failed"))
      success <- FALSE
      attempt <- attempt + 1
      Sys.sleep(2)
    } else if (response$status_code == 404) {
      response_content <- c()
      break
    } else {
      # Parse the content
      response_content <- httr::content(response, as = "parsed", type = "application/json")
      success <- TRUE
    }
  }

  # if (!success) {
  #   stop("Failed to fetch content after", max_retries, "attempts.")
  # }

  return(response_content)
}

re_authentication <- function(tenant_id, client_id, client_secret) {
  MSGraph::authorize_graph(tenant_id, client_id, client_secret)
}

# Funktion zur Extraktion der Meeting-ID
extract_meeting_id <- function(url) {
    meeting_id <- stringr::str_split(url, "/", simplify = TRUE)[, 6]
    decoded_meeting_id <- URLdecode(meeting_id)
    return(decoded_meeting_id)
}

################################################################################
# Transcript Processing Utility Functions
#
# Helper functions used across the transcript processing pipeline
################################################################################

#' Get Combined Call Participants
#'
#' Retrieves all participants for calls, combining actual participants
#' with event invitees
#'
#' @param con Database connection
#' @param call_ids Vector of call IDs
#' @param include_event_attendees Logical, include event invitees
#' @return Data frame with participant information
#' @keywords internal
get_call_participants_combined <- function(con, call_ids, include_event_attendees = TRUE) {

  # Get actual call participants
  actual_participants <- dplyr::tbl(con, I("raw.msgraph_call_participants")) %>%
    dplyr::filter(call_id %in% !!call_ids) %>%
    dplyr::left_join(
      dplyr::tbl(con, I("raw.msgraph_contacts")) %>%
        dplyr::select(id, ms_name, email),
      by = c("contact_id" = "id")
    ) %>%
    dplyr::select(call_id, contact_id, ms_name, email) %>%
    dplyr::collect() %>%
    dplyr::mutate(
      participant_source = "actual_call",
      participation_status = "joined",
      email_domain = stringr::str_extract(email, "@(.+)$"),
      is_studyflix = stringr::str_detect(tolower(email), "@studyflix\\.de$"),
      is_external = !is_studyflix
    )

  if (!include_event_attendees) {
    return(actual_participants %>% dplyr::arrange(call_id, participant_source, email))
  }

  # Get event invitees
  event_mappings <- dplyr::tbl(con, I("mapping.msgraph_call_event")) %>%
    dplyr::filter(call_id %in% !!call_ids) %>%
    dplyr::select(call_id, event_id) %>%
    dplyr::collect()

  if (nrow(event_mappings) == 0) {
    return(actual_participants %>% dplyr::arrange(call_id, participant_source, email))
  }

  # Get event attendees
  event_attendees <- dplyr::tbl(con, I("raw.msgraph_event_participants")) %>%
    dplyr::filter(event_id %in% !!event_mappings$event_id) %>%
    dplyr::left_join(
      dplyr::tbl(con, I("raw.msgraph_contacts")) %>%
        dplyr::select(id, ms_name, email),
      by = c("contact_id" = "id")
    ) %>%
    dplyr::select(event_id, contact_id, ms_name, email, is_organizer) %>%
    dplyr::collect() %>%
    dplyr::left_join(event_mappings, by = "event_id") %>%
    dplyr::select(call_id, contact_id, ms_name, email, is_organizer) %>%
    dplyr::mutate(
      participant_source = "event_invite",
      participation_status = as.character(ifelse(is_organizer, "organizer", "accepted"))
    )

  # Combine both sources
  combined_participants <- dplyr::bind_rows(
    actual_participants,
    event_attendees
  ) %>%
    # Remove duplicates (prefer actual_call)
    dplyr::group_by(call_id, email) %>%
    dplyr::arrange(participant_source) %>%
    dplyr::slice(1) %>%
    dplyr::ungroup() %>%
    dplyr::mutate(
      email_domain = stringr::str_extract(email, "@(.+)$"),
      is_studyflix = stringr::str_detect(tolower(email), "@studyflix\\.de$"),
      is_external = !is_studyflix
    ) %>%
    dplyr::arrange(call_id, participant_source, email)

  return(combined_participants)
}


#' Load Preserved Company Names
#'
#' Loads company/brand names to preserve during anonymization
#'
#' @param file_path Path to file with company names
#' @return Character vector of company names
#' @keywords internal
load_preserved_names <- function(file_path = "../../base-data/msgraph/text_for_summary_prompt/wettbewerber.txt") {
  if (!file.exists(file_path)) {
    warning("Preserved names file not found: ", file_path)
    return(character(0))
  }

  content <- paste(readLines(file_path, warn = FALSE), collapse = " ")
  names <- trimws(unlist(strsplit(content, ",")))
  names <- names[names != ""]

  return(names)
}
