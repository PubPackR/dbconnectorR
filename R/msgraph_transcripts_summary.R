################################################################################
# MS Graph Transcript Summary Module
#
# Functions for generating AI summaries of transcripts via OpenRouter API
#
# This module contains all logic for summarizing transcripts, including:
# - OpenRouter API integration
# - Multi-step conversation-based summarization
# - Category-based prompt selection
# - Retry logic for API failures
# - Database operations for summary storage
################################################################################

################################################################################
# Main Wrapper Function
################################################################################

#' Summarize Transcripts
#'
#' Generates AI summaries for all anonymized transcripts that don't have summaries yet.
#' This is step 3 of the transcript processing pipeline.
#'
#' @param con Database connection
#' @param openrouter_model AI model to use. Default: "openai/gpt-5-chat"
#' @param openrouter_keys OpenRouter API key for AI summary generation
#' @param prompt_base_path Base path for prompt template files.
#'   Default: "../../base-data/msgraph/text_for_summary_prompt/"
#' @param logger Logger function for output messages
#'
#' @return Invisible NULL (updates database)
#'
#' @examples
#' \dontrun{
#' msgraph_summarize_transcripts(
#'   con = con,
#'   openrouter_keys = keys$openrouter,
#'   openrouter_model = "openai/gpt-5-chat"
#' )
#' }
#'
#' @export
msgraph_summarize_transcripts <- function(con,
                                              openrouter_model = "openai/gpt-5-chat",
                                              openrouter_keys,
                                              prompt_base_path = "../../base-data/msgraph/text_for_summary_prompt/",
                                              logger = function(msg, level = "INFO") cat(msg, "\n")) {

  logger(sprintf("Using model: %s", openrouter_model), "DEBUG")

  # Call the main summary generation function
  generate_and_save_transcript_summaries(
    con,
    openrouter_model,
    openrouter_keys,
    prompt_base_path
  )

  logger("Summary generation completed", "INFO")
  invisible(NULL)
}


################################################################################
# Core Summary Generation Functions
################################################################################

#' Generates and saves transcript summaries for multiple transcripts (DB version)
#'
#' @param con A database connection object
#' @param openrouter_model The OpenRouter model to use
#' @param openrouter_key The OpenRouter API key
#' @param prompt_base_path Base path for prompt template files
#' @keywords internal
generate_and_save_transcript_summaries <- function(con, openrouter_model, openrouter_key,
                                                    prompt_base_path = "../../base-data/msgraph/text_for_summary_prompt/") {

  # Load prompt paths
  paths <- list(
    "einfuehrung" = file.path(prompt_base_path, "einfuehrung.txt"),
    "aufgaben_kategorisierung" = file.path(prompt_base_path, "aufgaben_kategorisierung.txt"),
    "aufgaben_kategorisierung_refinement" = file.path(prompt_base_path, "aufgaben_kategorisierung_refinement.txt"),
    "aufgaben_neuvorstellung" = file.path(prompt_base_path, "aufgaben_neuvorstellung.txt"),
    "aufgaben_followup" = file.path(prompt_base_path, "aufgaben_followup.txt"),
    "aufgaben_reporting" = file.path(prompt_base_path, "aufgaben_reporting.txt"),
    "pruefung_wettbewerber" = file.path(prompt_base_path, "pruefung_wettbewerber.txt"),
    "pruefung_sprecher" = file.path(prompt_base_path, "pruefung_sprecherzuordnung.txt"),
    "pruefung_datum" = file.path(prompt_base_path, "pruefung_datum.txt"),
    "layout" = file.path(prompt_base_path, "layout.txt")
  )

  transcripts_to_process <- get_transcripts_without_summary(con)
  transcripts_with_date <- dplyr::tbl(con, I("raw.msgraph_calls")) %>%
    dplyr::select(id, call_start) %>% dplyr::collect() %>% dplyr::right_join(transcripts_to_process, by=c("id"="call_id"))

  if (nrow(transcripts_to_process) == 0) {
    message("No new anonymized transcripts to summarize.")
    return(invisible(NULL))
  }

  for (i in seq_len(nrow(transcripts_with_date))) {
    row <- transcripts_with_date[i, ]

    cat("Summarizing transcript", i, "of", nrow(transcripts_with_date), "...\n")

    tryCatch({
      # Actually generate the summary
      row$transcript_content_anonymized <- paste0("Gespräch vom ", row$call_start, "\r\n ", row$transcript_content_anonymized)
      summary_text <- summarize_ts(row$transcript_content_anonymized, paths, openrouter_model, openrouter_key)

      strange_spaces <- "[\u00A0\u202F\u2009\u200A\u200B]"
      summary_text <- stringr::str_replace_all(summary_text, strange_spaces, " ")

      if (is.null(summary_text)) summary_text <- ""

      DBI::dbExecute(
        con,
        "UPDATE processed.msgraph_call_transcripts
         SET transcript_summary_anonymized = $1
         WHERE transcript_id = $2",
        params = list(summary_text, row$transcript_id)
      )

    }, error = function(e) {
      cat("❌ Failed to summarize transcript ID:", row$transcript_id, "\n")
      cat("   Error:", e$message, "\n")
      cat("   Continuing with next transcript...\n\n")
    })

    Sys.sleep(1.2)  # Respect API limits
  }

  message("Transcript summaries successfully saved to the database.")
}


#' Categorizes and summarizes a call transcript using OpenRouter API
#'
#' @param transcript_anonymized A string containing the anonymized call transcript to be categorized and summarized.
#' @param paths A list containing file paths to the different prompt template files.
#' @param openrouter_model A string specifying the OpenRouter model to use for processing.
#' @param openrouter_key A string containing the OpenRouter API key for authentication.
#' @return A string containing the category codes and summary text, separated by comma and space.
#' @keywords internal
summarize_ts <- function(transcript_anonymized, paths, openrouter_model, openrouter_key){

  # Define the possible category codes
  possible_categories <- c("NV", #Neuvorstellung
                          "FU", #Follow Up
                          "RP", #Reporting
                          "MD", #Mediadaten
                          "SW", #Similar Web
                          "AG" #Agentur
                          )

  # Initialize Chat
  task_einfuehrung <- paste(readLines(paths$einfuehrung, warn = FALSE), collapse = "\n")
  chat <- ellmer::chat_openrouter(
    system_prompt = paste(task_einfuehrung, transcript_anonymized),
    model = openrouter_model,
    api_key = openrouter_key,
    echo = "none"
  )

  # Categorize Chat
  task_categorization <- paste(readLines(paths$aufgaben_kategorisierung, warn = FALSE), collapse = "\n")
  transcript_categories <- retry_llm(task_categorization, chat)
  categories_vector <- character(0)
  # Check for each category in the response and add to vector in order of appearance
  for (category in possible_categories) {
    if (grepl(category, transcript_categories, fixed = TRUE)) {
      categories_vector <- c(categories_vector, category)
    }
  }
  # Retry if could not be categorized
  if (length(categories_vector) == 0 || !(categories_vector[1] %in% c("NV", "FU", "RP"))) {
    task_retry <- paste(readLines(paths$aufgaben_kategorisierung_refinement, warn = FALSE), collapse = "\n")
    transcript_categories <- retry_llm(task_retry, chat)
    categories_vector <- character(0)
    # Check for each category in the response and add to vector in order of appearance
    for (category in possible_categories) {
      if (grepl(category, transcript_categories, fixed = TRUE)) {
        categories_vector <- c(categories_vector, category)
      }
    }
  }
  # Set default if still could not be categorized
  if (length(categories_vector) == 0 || !(categories_vector[1] %in% c("NV", "FU", "RP"))) {
    categories_vector <- "NV"
  }

  # Summarize Chat with category sensitive prompt
  if (categories_vector[1] == "NV") {
    prompt <- paste(readLines(paths$aufgaben_neuvorstellung, warn = FALSE), collapse = "\n")
    summary <- retry_llm(prompt, chat)
  } else if (categories_vector[1] == "FU") {
    prompt <- paste(readLines(paths$aufgaben_followup, warn = FALSE), collapse = "\n")
    summary <- retry_llm(prompt, chat)
  } else if (categories_vector[1] == "RP") {
    prompt <- paste(readLines(paths$aufgaben_reporting, warn = FALSE), collapse = "\n")
    summary <- retry_llm(prompt, chat)
  } else {
    summary <- "ICH KONNTE DAS TRANSKRIPT NICHT KATEGORISIEREN, VERSUCHE ES ERNEUT"
  }

  prompt <- paste(readLines(paths$pruefung_wettbewerber, warn = FALSE), collapse = "\n")
  summary <- retry_llm(prompt, chat)

  prompt <- paste(readLines(paths$pruefung_sprecher, warn = FALSE), collapse = "\n")
  summary <- retry_llm(prompt, chat)

  prompt <- paste(readLines(paths$pruefung_datum, warn = FALSE), collapse = "\n")
  summary <- retry_llm(prompt, chat)

  prompt <- paste(readLines(paths$layout, warn = FALSE), collapse = "\n")
  summary <- retry_llm(prompt, chat)

  #return(paste(paste(categories_vector, collapse = ", "), summary))
  return(paste(summary))
}


################################################################################
# Helper Functions
################################################################################

#' Retries the llm call in case of transient errors
#'
#' @param prompt A string containing the call transcript to be summarized.
#' @param chat An ellmer chat object
#' @param max_attempts The maximum number of retry attempts.
#' @param wait_base The base wait time for exponential backoff.
#' @return summary of the text
#' @keywords internal
retry_llm <- function(prompt, chat, max_attempts = 4, wait_base = 2) {
  for (i in seq_len(max_attempts)) {
    result <- tryCatch({
    chat$chat(prompt)
    }, error = function(e) {
      # Only retry on 503 / 429 or general transient failures
      if (grepl("503|429", e$message)) {
        wait_time <- wait_base ^ i
        message(sprintf("Attempt %d failed with %s. Retrying in %d seconds...", i, e$message, wait_time))
        Sys.sleep(wait_time)
        return(NULL)
      } else {
        warning("Non-retriable error: ", e$message)
        return(NA)
      }
    })
    if (!is.null(result)) return(result)
  }

  warning("Max retries exceeded for this transcript.")
  return(NA)
}


#' Gets all transcripts without a summary from the database
#'
#' @param con A database connection object.
#' @return A data frame with transcripts that do not have a summary
#' @keywords internal
get_transcripts_without_summary <- function(con) {
  query <- "
    SELECT id, transcript_id, call_id, transcript_url, transcript_created_at,
           transcript_content, transcript_content_anonymized
    FROM processed.msgraph_call_transcripts
    WHERE transcript_summary_anonymized IS NULL
      AND transcript_content_anonymized IS NOT NULL
      AND TRIM(transcript_content_anonymized) != ''
      AND transcript_created_at >= '2025-09-17'
    ORDER BY transcript_created_at ASC;
  "

  df <- DBI::dbGetQuery(con, query)
  return(df)
}
