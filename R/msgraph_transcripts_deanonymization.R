################################################################################
# MS Graph Transcript Deanonymization Module
#
# Functions for deanonymizing transcript summaries
#
# This module contains all logic for reversing anonymization, including:
# - Placeholder lookup from database
# - Token extraction and matching
# - Text replacement logic
################################################################################

################################################################################
# Main Wrapper Function
################################################################################

#' Deanonymize Transcript Summaries
#'
#' Replaces anonymized placeholders with original names in all summaries.
#' This is step 4 of the transcript processing pipeline.
#'
#' @param con Database connection
#' @param logger Logger function for output messages
#'
#' @return Invisible NULL (updates database)
#'
#' @examples
#' \dontrun{
#' msgraph_deanonymize_transcripts(con = con)
#' }
#'
#' @export
msgraph_deanonymize_transcripts <- function(con,
                                                 logger = function(msg, level = "INFO") cat(msg, "\n")) {

  # Call the main deanonymization function
  deanonymize_transcript_summaries(con)

  logger("Deanonymization completed", "INFO")
  invisible(NULL)
}


################################################################################
# Core Deanonymization Functions
################################################################################

#' Adds deanonymized summaries to the transcript table
#'
#' @param con Database connection
#' @return NULL (updates DB in place)
#' @keywords internal
deanonymize_transcript_summaries <- function(con) {

  # Load transcripts that need deanonymization (have anonymized summary but no real one)
  to_process <- get_transcripts_that_need_deanonymisation(con)

  if (nrow(to_process) == 0) {
    message("No transcript summaries to deanonymize.")
    return(invisible(NULL))
  }

  for (i in seq_len(nrow(to_process))) {
    row <- to_process[i, ]

    # Ensure ID is properly handled as character
    transcript_id <- as.character(row$id)
    anonymized_text <- row$transcript_summary_anonymized

    # Replace narrow non-breaking spaces with regular spaces
    strange_spaces <- "[\u00A0\u202F\u2009\u200A\u200B]"
    anonymized_text <- stringr::str_replace_all(anonymized_text, strange_spaces, " ")

    cat("Deanonymizing transcript", i, "of", nrow(to_process), "\n")

    tryCatch({
      deanonymized <- deanonymize_text(
        con,
        anonymized_text = anonymized_text,
        transcript_id = transcript_id
      )

      if (is.null(deanonymized)) deanonymized <- ""

      DBI::dbExecute(con, "
        UPDATE processed.msgraph_call_transcripts
        SET transcript_summary = $1
        WHERE id = $2
      ", params = list(deanonymized, transcript_id))

    }, error = function(e) {
      cat("❌ Failed to deanonymize transcript ID:", transcript_id, "\n")
      cat("   Error:", e$message, "\n")
      cat("   Continuing with next transcript...\n\n")
    })
  }

  message("Deanonymized summaries successfully updated.")
  return(invisible(NULL))
}


#' Deanonymizes text by replacing anonymized IDs with original names
#'
#' @param con Database connection
#' @param anonymized_text A character vector with anonymized text
#' @param transcript_id_var Transcript ID to look up placeholders for
#' @return A character vector with deanonymized text
#' @keywords internal
deanonymize_text <- function(con, anonymized_text, transcript_id_var) {

  anonymized_text <- as.character(anonymized_text)[1]

  # Check if anonymized_text is NA or empty
  if(is.na(anonymized_text) || nchar(anonymized_text) == 0) {
    return("")
  }

  placeholders <- dplyr::tbl(con, I("processed.msgraph_call_transcript_placeholders")) %>%
    dplyr::filter(transcript_id == transcript_id_var) %>%
    dplyr::select(placeholder, actual_value) %>%
    dplyr::collect()

  if(nrow(placeholders) == 0) {
    return(anonymized_text)
  }

  # Extract tokens: "< Lead_1 >" (with spaces) or "<Lead_1>" (without spaces)
  # Pattern: < optional-spaces WORD_NUMBER optional-spaces >
  tokens_raw <- stringr::str_extract_all(anonymized_text, "< (?:Speaker: )?[A-Za-z0-9_-]+ >")[[1]]

  if(length(tokens_raw) == 0) {
    return(anonymized_text)
  }

  tokens <- tokens_raw %>%
    unique() %>%
    stringr::str_replace_all("-", "_")  # Replace hyphens with underscores

  # Build data frame
  token_clean_vec <- stringr::str_trim(stringr::str_remove_all(tokens, "[<>]"))

  # Remove "Speaker: " prefix if present (it's sometimes in the summary but not in placeholders)
  token_clean_vec <- stringr::str_remove(token_clean_vec, "^Speaker:\\s*")

  df <- tibble::tibble(
    token_full  = tokens,
    token_clean = token_clean_vec,
    actual_value = NA_character_
  )

  if(nrow(df) == 0) {
    return(anonymized_text)
  }

  # Match tokens with placeholders
  for(i in 1:nrow(df)) {
    token_clean <- df$token_clean[i]
    token_full <- df$token_full[i]

    # Step 1: Try exact match first (handles Speaker: prefix correctly)
    # Build expected placeholder patterns for exact matching
    exact_patterns <- c(
      paste0("< ", token_clean, " >"),
      paste0("< Speaker: ", token_clean, " >")
    )
    exact_match <- placeholders %>%
      dplyr::filter(placeholder %in% exact_patterns)

    if (nrow(exact_match) > 0) {
      # Exact match found - use it
      df$actual_value[i] <- exact_match$actual_value[1]
    } else {
      # Step 2: Fallback to grepl for partial matches (e.g., City, Street placeholders)
      partial_match <- placeholders %>%
        dplyr::filter(grepl(token_clean, placeholder, fixed = TRUE))

      if (nrow(partial_match) > 0) {
        partial_match <- partial_match %>%
          dplyr::filter(nchar(placeholder) == min(nchar(placeholder)))
        df$actual_value[i] <- partial_match$actual_value[1]
      } else {
        df$actual_value[i] <- NA
      }
    }
  }

  df <- df %>% dplyr::filter(!is.na(actual_value))

  if(nrow(df) == 0) {
    return(anonymized_text)
  }

  deanonymized_text <- stringi::stri_replace_all_fixed(
    anonymized_text,
    pattern = df$token_full,
    replacement = df$actual_value,
    vectorize_all = FALSE
  )

  return(deanonymized_text)

}


################################################################################
# Helper Functions
################################################################################

#' Gets transcripts that have an anonymized summary but no deanonymized summary
#'
#' @param con Database connection
#' @return A dataframe of transcripts to process
#' @keywords internal
get_transcripts_that_need_deanonymisation <- function(con) {
  query <- "
    SELECT *
    FROM processed.msgraph_call_transcripts
    WHERE transcript_summary IS NULL
      AND transcript_summary_anonymized IS NOT NULL
      AND transcript_content_anonymized IS NOT NULL;
  "
  DBI::dbGetQuery(con, query)
}
