################################################################################
# MS Graph Transcript Processing - Main Orchestrator
#
# Main function for orchestrating the complete transcript processing pipeline
################################################################################

#' Update and Process Transcripts from MS Graph API
#'
#' Complete pipeline for Microsoft Teams transcript processing. This function:
#' - Retrieves transcripts from MS Graph API for all internal users
#' - Anonymizes transcript content using CoreNLP NER
#' - Generates AI summaries via OpenRouter API
#' - Deanonymizes summaries for CRM export
#' - Exports formatted summaries to CRM system as protocols
#'
#' The pipeline tracks state and can be resumed from any step if interrupted.
#' Each step only processes records that haven't been processed yet.
#'
#' @param con Database connection object (pool or DBI connection).
#'   Must have access to required tables (see Details)
#' @param msgraph_keys List containing MS Graph API credentials with elements:
#'   - tenant_id: Azure AD tenant ID
#'   - client_id: Azure AD client/application ID
#'   - client_secret: Azure AD client secret
#'   Note: This is NOT keys$msgraph from authentication_process(). You need to
#'   construct this list manually with all three elements.
#' @param openrouter_keys Character string with OpenRouter API key for AI summaries
#' @param crm_keys Character string with CRM API key for protocol export
#' @param openrouter_model Character string specifying which AI model to use.
#'   Default: "openai/gpt-5-chat"
#' @param steps Character vector of pipeline steps to execute.
#'   Options: c("load", "anonymize", "summarize", "deanonymize", "export_to_crm")
#'   Default: all steps
#' @param start_date Optional. Date to start transcript retrieval from.
#'   If NULL, automatically determined from last transcript in database.
#' @param end_date Optional. Date to end transcript retrieval.
#'   Default: tomorrow (to include today's transcripts)
#' @param preserved_names Character vector of company/brand names to preserve
#'   during anonymization (e.g., c("studyflix", "competitor_name"))
#' @param use_test_account Logical. If TRUE, exports to test CRM account.
#'   Default: FALSE
#' @param resume Logical. If TRUE, resumes from last successful step.
#'   Default: FALSE
#' @param log_level Character string specifying logging verbosity.
#'   Options: "DEBUG", "INFO", "WARNING", "ERROR". Default: "INFO"
#' @param log_file Optional. File path for logging output.
#'
#' @return Invisible NULL. Function is called for side effects (database updates).
#'
#' @details
#' Required database tables:
#' - raw.msgraph_users, raw.msgraph_contacts, raw.msgraph_calls
#' - raw.msgraph_call_participants, raw.msgraph_event_participants
#' - raw.crm_users, raw.crm_leads, raw.crm_lead_mail_address
#' - raw.crm_company_lead_positions
#' - mapping.msgraph_call_event, mapping.crm_lead_msgraph_contact
#' - processed.msgraph_call_transcripts
#' - processed.msgraph_call_transcript_placeholders
#'
#' @examples
#' \dontrun{
#' # Basic usage - run complete pipeline
#' msgraph_update_transcripts(
#'   con = con,
#'   msgraph_keys = list(
#'     tenant_id = "your-tenant-id",
#'     client_id = "your-client-id",
#'     client_secret = keys$msgraph
#'   ),
#'   openrouter_keys = keys$openrouter,
#'   crm_keys = keys$crm
#' )
#'
#' # Run specific steps only
#' msgraph_update_transcripts(
#'   con = con,
#'   msgraph_keys = list(
#'     tenant_id = "your-tenant-id",
#'     client_id = "your-client-id",
#'     client_secret = keys$msgraph
#'   ),
#'   openrouter_keys = keys$openrouter,
#'   crm_keys = keys$crm,
#'   steps = c("summarize", "deanonymize", "export_to_crm")
#' )
#'
#' # Resume from last successful step
#' msgraph_update_transcripts(
#'   con = con,
#'   msgraph_keys = list(
#'     tenant_id = "your-tenant-id",
#'     client_id = "your-client-id",
#'     client_secret = keys$msgraph
#'   ),
#'   openrouter_keys = keys$openrouter,
#'   crm_keys = keys$crm,
#'   resume = TRUE
#' )
#' }
#'
#' @export
msgraph_update_transcripts <- function(con,
                                       msgraph_keys,
                                       openrouter_keys,
                                       crm_keys,
                                       openrouter_model = "openai/gpt-5-chat",
                                       steps = c("load", "anonymize", "summarize", "deanonymize", "export_to_crm"),
                                       start_date = NULL,
                                       end_date = Sys.Date() + 1,
                                       preserved_names = c("studyflix", "study flix", "studi flix", "studiflix"),
                                       use_test_account = FALSE,
                                       resume = FALSE,
                                       log_level = "INFO",
                                       log_file = NULL) {

  # Initialize logging
  logger <- create_transcript_logger(log_level, log_file)
  logger("=== MS Graph Transcript Processing Pipeline ===", "INFO")

  # Create configuration object
  config <- list(
    msgraph_keys = msgraph_keys,
    openrouter_keys = openrouter_keys,
    crm_keys = crm_keys,
    openrouter_model = openrouter_model,
    preserved_names = preserved_names,
    use_test_account = use_test_account,
    start_date = start_date,
    end_date = end_date,
    logger = logger,
    log_level = log_level
  )

  # Determine which steps to run
  if (resume) {
    steps <- determine_transcript_resume_steps(con, logger)
    if (length(steps) == 0) {
      logger("All pipeline steps already completed", "INFO")
      return(invisible(NULL))
    }
    logger(sprintf("Resuming from step: %s", steps[1]), "INFO")
  }

  logger(sprintf("Steps to execute: %s", paste(steps, collapse = ", ")), "INFO")

  # Map step names to handler functions
  step_handlers <- list(
    "load" = execute_transcript_load,
    "anonymize" = execute_transcript_anonymization,
    "summarize" = execute_transcript_summarization,
    "deanonymize" = execute_transcript_deanonymization,
    "export_to_crm" = execute_transcript_crm_export
  )

  # Execute pipeline steps
  for (step in steps) {
    if (!step %in% names(step_handlers)) {
      logger(sprintf("Unknown step: %s", step), "WARNING")
      next
    }

    logger(sprintf("\n--- Executing step: %s ---", step), "INFO")

    tryCatch({
      step_handlers[[step]](con, config)
      logger(sprintf("Step '%s' completed successfully", step), "INFO")
    }, error = function(e) {
      logger(sprintf("Step '%s' failed: %s", step, e$message), "ERROR")
      logger(paste("Traceback:", paste(capture.output(traceback()), collapse = "\n")), "DEBUG")
      stop(sprintf("Pipeline failed at step '%s': %s", step, e$message))
    })
  }

  logger("\n=== Pipeline Execution Complete ===", "INFO")
  invisible(NULL)
}


################################################################################
# Step Execution Functions (delegate to specialized modules)
################################################################################

#' Execute Transcript Load Step
#' @keywords internal
execute_transcript_load <- function(con, config) {
  config$logger("Loading transcripts from MS Graph API", "INFO")

  # Delegate to transcript retrieval module
  msgraph_retrieve_transcripts(
    con = con,
    msgraph_keys = config$msgraph_keys,
    start_date = config$start_date,
    end_date = config$end_date,
    logger = config$logger
  )
}


#' Execute Transcript Anonymization Step
#' @keywords internal
execute_transcript_anonymization <- function(con, config) {
  config$logger("Anonymizing transcripts", "INFO")

  # Delegate to anonymization module
  msgraph_anonymize_transcripts(
    con = con,
    preserved_names = config$preserved_names,
    logger = config$logger
  )
}


#' Execute Transcript Summarization Step
#' @keywords internal
execute_transcript_summarization <- function(con, config) {
  config$logger("Generating AI summaries", "INFO")

  # Delegate to summary module
  msgraph_summarize_transcripts(
    con = con,
    openrouter_model = config$openrouter_model,
    openrouter_keys = config$openrouter_keys,
    logger = config$logger
  )
}


#' Execute Transcript Deanonymization Step
#' @keywords internal
execute_transcript_deanonymization <- function(con, config) {
  config$logger("Deanonymizing summaries", "INFO")

  # Delegate to deanonymization module
  msgraph_deanonymize_transcripts(
    con = con,
    logger = config$logger
  )
}


#' Execute CRM Export Step
#' @keywords internal
execute_transcript_crm_export <- function(con, config) {
  config$logger("Exporting to CRM", "INFO")

  # Delegate to CRM export module
  msgraph_export_transcripts_to_crm(
    con = con,
    crm_keys = config$crm_keys,
    use_test_account = config$use_test_account,
    logger = config$logger
  )
}


################################################################################
# Helper Functions
################################################################################

#' Create Logger Function
#' @keywords internal
create_transcript_logger <- function(level = "INFO", file = NULL) {
  levels <- c("DEBUG" = 0, "INFO" = 1, "WARNING" = 2, "ERROR" = 3)
  current_level <- levels[level]

  function(message, level = "INFO", ...) {
    if (levels[level] >= current_level) {
      timestamp <- format(Sys.time(), "%Y-%m-%d %H:%M:%S")
      log_msg <- sprintf("[%s] %s: %s", timestamp, level, message)

      if (!is.null(file)) {
        cat(log_msg, "\n", file = file, append = TRUE)
      }
      if (level == "ERROR") {
        message(log_msg)
      } else {
        cat(log_msg, "\n")
      }
    }
  }
}


#' Determine Which Steps to Resume
#' @keywords internal
determine_transcript_resume_steps <- function(con, logger) {
  all_steps <- c("load", "anonymize", "summarize", "deanonymize", "export_to_crm")

  # Check database state for each step
  needs_anonymization <- dplyr::tbl(con, I("processed.msgraph_call_transcripts")) %>%
    dplyr::filter(is.na(transcript_content_anonymized) & !is.na(transcript_content)) %>%
    dplyr::count() %>%
    dplyr::pull(n) > 0

  needs_summary <- dplyr::tbl(con, I("processed.msgraph_call_transcripts")) %>%
    dplyr::filter(is.na(transcript_summary_anonymized) & !is.na(transcript_content_anonymized)) %>%
    dplyr::count() %>%
    dplyr::pull(n) > 0

  needs_deanonymization <- dplyr::tbl(con, I("processed.msgraph_call_transcripts")) %>%
    dplyr::filter(is.na(transcript_summary) & !is.na(transcript_summary_anonymized)) %>%
    dplyr::count() %>%
    dplyr::pull(n) > 0

  needs_export <- dplyr::tbl(con, I("processed.msgraph_call_transcripts")) %>%
    dplyr::filter((is.na(exported_to_crm) | exported_to_crm == FALSE) & !is.na(transcript_summary)) %>%
    dplyr::filter(is.na(not_matchable_with_crm) | not_matchable_with_crm == FALSE) %>%
    dplyr::count() %>%
    dplyr::pull(n) > 0

  # Determine starting point
  if (needs_anonymization) {
    return(c("anonymize", "summarize", "deanonymize", "export_to_crm"))
  } else if (needs_summary) {
    return(c("summarize", "deanonymize", "export_to_crm"))
  } else if (needs_deanonymization) {
    return(c("deanonymize", "export_to_crm"))
  } else if (needs_export) {
    return(c("export_to_crm"))
  }

  return(character(0))
}
