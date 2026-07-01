################################################################################
# Internal Email Pattern Configuration
#
# Central definition of internal email patterns for classification
################################################################################

#' Internal Email Patterns
#'
#' Vector of email patterns considered internal (company emails).
#' Patterns are matched anywhere in the email address.
#' Dots are automatically escaped for regex matching.
#'
#' @examples
#' # Domain patterns
#' INTERNAL_EMAIL_PATTERNS <- c("@studyflix.de", "@tenhil.de")
#'
#' # Exchange system addresses
#' INTERNAL_EMAIL_PATTERNS <- c("@studyflix.de", "/o=exchangelabs")
#'
#' @export
INTERNAL_EMAIL_PATTERNS <- c("@studyflix.de", "/o=exchangelabs", "@bertelsmann.", "@ausbildung.de", "@vocanto.de", "@ad-alliance.de", "@arvato", "@rtl.de", "@rtl.com", "@rtl2.de", "@embrace.")

#' Check if Email is Internal
#'
#' Checks whether an email address matches any internal pattern.
#' Emails without @ are always considered internal (system addresses).
#'
#' @param email Character vector of email addresses
#' @return Logical vector indicating internal emails
#' @export
is_internal_email <- function(email) {
  # Emails without @ are internal (system addresses)
  no_at <- !stringr::str_detect(email, "@")
  # Or matches any internal pattern
  matches_pattern <- stringr::str_detect(tolower(email), get_internal_email_pattern())
  no_at | matches_pattern
}

#' Get Internal Email Regex Pattern
#'
#' Returns the regex pattern for matching internal emails.
#' Useful for grepl() calls that don't support vectorized functions.
#'
#' @return Character string with regex pattern
#' @export
get_internal_email_pattern <- function() {
  # Escape dots in patterns for regex
  escaped_patterns <- gsub("\\.", "\\\\.", INTERNAL_EMAIL_PATTERNS)
  paste0("(", paste(escaped_patterns, collapse = "|"), ")")
}

#' Check if Email is Synthetic
#'
#' Checks whether an email address is a synthetic placeholder
#' (e.g. from guest users or external organizers without real email).
#'
#' @param email Character vector of email addresses
#' @return Logical vector
#' @export
is_synthetic_email <- function(email) {
  grepl("@external\\.(guest|msgraph)$", email, ignore.case = TRUE)
}

################################################################################

#' Create a Self-Refreshing MS Graph Token Provider
#'
#' Returns a closure that lazily fetches an MS Graph access token and caches it
#' until shortly before its JWT `exp` claim, then transparently refreshes it.
#' The returned closure can be passed wherever the package expects an
#' `access_token` argument — `fetch_with_retry()` and the direct `httr::GET`
#' calls in the `msgraph_update_*` functions resolve it on demand and also
#' force-refresh on a 401 response.
#'
#' @param tenant_id Azure AD tenant ID.
#' @param client_id Application (client) ID.
#' @param client_secret App registration client secret.
#' @param refresh_buffer_seconds Refresh the cached token when fewer than this
#'   many seconds remain until expiry. Default 300 (5 minutes).
#'
#' @return A function `function(force_refresh = FALSE)` returning a valid access
#'   token string. Call with `force_refresh = TRUE` to bypass the cache.
#'
#' @examples
#' \dontrun{
#' token_provider <- msgraph_make_token_provider(
#'   tenant_id     = "f34ad13c-...",
#'   client_id     = "128a9a89-...",
#'   client_secret = keys$msgraph
#' )
#' msgraph_update_events(con, token_provider, startDate = Sys.Date() - 50)
#' }
#' @export
msgraph_make_token_provider <- function(tenant_id, client_id, client_secret,
                                        refresh_buffer_seconds = 300) {
  cache <- new.env(parent = emptyenv())
  cache$token <- NULL
  cache$exp <- as.POSIXct(NA)

  function(force_refresh = FALSE) {
    now <- Sys.time()
    needs_refresh <- force_refresh ||
      is.null(cache$token) ||
      is.na(cache$exp) ||
      as.numeric(difftime(cache$exp, now, units = "secs")) < refresh_buffer_seconds

    if (needs_refresh) {
      cache$token <- MSGraph::authorize_graph(tenant_id, client_id, client_secret)
      cache$exp <- tryCatch(
        extract_token_exp(cache$token),
        error = function(e) {
          # If exp cannot be decoded, assume a conservative 50 minute lifetime
          Sys.time() + 50 * 60
        }
      )
    }
    cache$token
  }
}

#' Resolve a Token Argument to a Bearer String
#'
#' Internal helper. `access_token` may be either a raw token string (legacy
#' usage) or a provider closure produced by [msgraph_make_token_provider()].
#' This function returns the current token string in either case, optionally
#' forcing a refresh when the provider supports it.
#'
#' @param access_token Either a character string or a function.
#' @param force_refresh If TRUE and `access_token` is a provider closure,
#'   forces re-authentication.
#' @return Character scalar with the active access token.
#' @keywords internal
resolve_token <- function(access_token, force_refresh = FALSE) {
  if (is.function(access_token)) {
    fmls <- names(formals(access_token))
    if ("force_refresh" %in% fmls) {
      access_token(force_refresh = force_refresh)
    } else {
      access_token()
    }
  } else {
    access_token
  }
}

fetch_with_retry <- function(url, access_token, query = c(), max_retries = 5, delay = 2,
                             accept = "application/json", parse = c("json", "text", "raw"),
                             error_on_failure = FALSE) {
  parse <- match.arg(parse)
  attempt <- 1
  success <- FALSE
  response_content <- NULL
  auth_refresh_done <- FALSE
  response <- NULL  # holds the most recent response for failure diagnostics

  while (attempt <= max_retries && !success) {
    bearer <- resolve_token(access_token)
    req_headers <- httr::add_headers(
      Authorization = paste("Bearer", bearer),
      Accept = accept
    )
    if (length(query) == 0) {
      response <- httr::GET(url, req_headers)
    } else {
      response <- httr::GET(url, query = query, req_headers)
    }

    if (response$status_code == 401 && !auth_refresh_done && is.function(access_token)) {
      message("Got 401 — forcing token refresh and retrying once")
      resolve_token(access_token, force_refresh = TRUE)
      auth_refresh_done <- TRUE
      # Don't count this against max_retries
      next
    } else if (response$status_code == 429) {
      # Rate limited: use Retry-After header or exponential backoff
      retry_after <- as.numeric(httr::headers(response)$`retry-after`)
      if (!is.na(retry_after)) {
        message(paste("Throttled. Retrying after", retry_after, "seconds..."))
        Sys.sleep(retry_after)
      } else {
        backoff_time <- min(delay * 2 ^ (attempt - 1), 60)
        message(paste("Throttled. Using exponential backoff:", backoff_time, "seconds..."))
        Sys.sleep(backoff_time)
      }
      attempt <- attempt + 1
    } else if (response$status_code >= 500) {
      message(paste("\nAttempt", attempt, "failed with status", response$status_code))
      attempt <- attempt + 1
      Sys.sleep(delay)
    } else if (response$status_code == 404) {
      response_content <- c()
      break
    } else {
      if (response$status_code == 401) {
        message("401 Unauthorized — token appears expired. ",
                "Use msgraph_make_token_provider() instead of a raw token for auto-refresh.")
      }
      # Parse the content according to the requested format
      response_content <- switch(
        parse,
        json = httr::content(response, as = "parsed", type = "application/json"),
        text = httr::content(response, as = "text", encoding = "UTF-8"),
        raw  = httr::content(response, as = "raw")
      )
      success <- TRUE
    }
  }

  if (!success) {
    last_status <- if (is.null(response)) NA_integer_ else response$status_code
    last_body <- tryCatch(
      substr(httr::content(response, as = "text", encoding = "UTF-8"), 1, 300),
      error = function(e) "<no body>"
    )
    message(sprintf("Failed to fetch content (last HTTP %s): %s", last_status, url))
    if (error_on_failure) {
      stop(sprintf("Failed to fetch content (HTTP %s) from %s: %s",
                   last_status, url, last_body))
    }
  }

  return(response_content)
}

re_authentication <- function(tenant_id, client_id, client_secret) {
  MSGraph::authorize_graph(tenant_id, client_id, client_secret)
}

#' Decode JWT Payload Claims from an MS Graph Access Token
#'
#' Internal helper that base64url-decodes the JWT payload and returns the
#' parsed claims list.
#'
#' @param access_token A valid MS Graph API access token (JWT format).
#' @return Named list of JWT claims.
#' @keywords internal
decode_jwt_claims <- function(access_token) {
  parts <- strsplit(access_token, "\\.")[[1]]
  if (length(parts) < 2) stop("Invalid JWT structure")
  payload_b64url <- parts[2]
  payload_b64 <- chartr("-_", "+/", payload_b64url)
  mod <- nchar(payload_b64) %% 4
  if (mod > 0) payload_b64 <- paste0(payload_b64, strrep("=", 4 - mod))
  jsonlite::fromJSON(rawToChar(jsonlite::base64_dec(payload_b64)))
}

#' Extract Tenant ID from MS Graph Access Token
#'
#' Decodes the JWT payload of an MS Graph access token and returns the tenant ID (tid claim).
#'
#' @param access_token A valid MS Graph API access token (JWT format).
#' @return Character string with the Azure AD tenant ID.
#' @keywords internal
extract_tenant_from_token <- function(access_token) {
  tryCatch({
    claims <- decode_jwt_claims(access_token)
    if (is.null(claims$tid)) stop("Missing tid claim in token")
    claims$tid
  }, error = function(e) {
    stop("Failed to extract tenant ID from access token: ", e$message)
  })
}

#' Extract Expiry Timestamp from MS Graph Access Token
#'
#' Decodes the JWT `exp` claim (seconds since epoch) into a POSIXct.
#'
#' @param access_token A valid MS Graph API access token (JWT format).
#' @return POSIXct expiry timestamp (UTC).
#' @keywords internal
extract_token_exp <- function(access_token) {
  claims <- decode_jwt_claims(access_token)
  if (is.null(claims$exp)) stop("Missing exp claim in token")
  as.POSIXct(as.numeric(claims$exp), origin = "1970-01-01", tz = "UTC")
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
      is_internal = is_internal_email(email),
      is_external = !is_internal
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
      is_internal = is_internal_email(email),
      is_external = !is_internal
    ) %>%
    dplyr::arrange(call_id, participant_source, email)

  return(combined_participants)
}


#' Load Preserved Company Names
#'
#' Loads company/brand names to preserve during anonymization
#' from a text file containing comma-separated values.
#'
#' @return Character vector of company names. Always includes studyflix variations.
#' @keywords internal
load_preserved_names <- function() {
  file_path <- "../../base-data/msgraph/text_for_summary_prompt/wettbewerber.txt"

  if (!file.exists(file_path)) {
    warning("Preserved names file not found: ", file_path,
            ". Using default values only.")
    return(c("studyflix", "study flix", "studi flix", "studiflix"))
  }

  content <- paste(readLines(file_path, warn = FALSE), collapse = " ")
  names <- trimws(unlist(strsplit(content, ",")))
  names <- names[names != ""]

  # Always include standard studyflix variations
  names <- unique(c(names, "studyflix", "study flix", "studi flix", "studiflix"))

  return(names)
}
