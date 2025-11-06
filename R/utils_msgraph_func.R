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