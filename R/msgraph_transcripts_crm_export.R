################################################################################
# MS Graph Transcript CRM Export Module
#
# Functions for exporting deanonymized summaries to CRM system
#
# This module contains all logic for CRM export, including:
# - Mapping transcripts to CRM entities (people/companies)
# - Formatting summaries for CRM markdown
# - API calls to create protocols
# - Export tracking and error handling
################################################################################

# Constants
# Default CRM user ID for protocol author when organizer mapping fails
# This is typically the user ID of the technical/admin account
CRM_DEFAULT_AUTHOR_USER_ID <- 199146L

################################################################################
# Main Wrapper Function
################################################################################

#' Export Transcripts to CRM
#'
#' Exports all deanonymized transcript summaries to CRM as protocols.
#' This is step 5 of the transcript processing pipeline.
#'
#' @param con Database connection
#' @param crm_keys CRM API key for protocol creation
#' @param use_test_account Logical, whether to use test CRM account.
#'   Default: FALSE
#' @param logger Logger function for output messages
#'
#' @return Invisible NULL (updates database and creates CRM protocols)
#'
#' @examples
#' \dontrun{
#' msgraph_export_transcripts_to_crm(
#'   con = con,
#'   crm_keys = keys$crm,
#'   use_test_account = FALSE
#' )
#' }
#'
#' @export
msgraph_export_transcripts_to_crm <- function(con,
                                          crm_keys,
                                          use_test_account = FALSE,
                                          logger = function(msg, level = "INFO") cat(msg, "\n")) {

  # Get transcripts ready for export
  ready_transcripts <- get_transcripts_ready_for_export(con)

  if (nrow(ready_transcripts) == 0) {
    logger("No transcript summaries ready for CRM export", "INFO")
    return(invisible(NULL))
  }

  logger(sprintf("Found %d transcript summaries ready for export", nrow(ready_transcripts)), "INFO")

  # Map transcripts to CRM entities
  transcript_mappings <- map_transcripts_to_crm_entities(con, transcript_summaries = ready_transcripts)

  # Update mapping status
  mapped_ids <- if (nrow(transcript_mappings) > 0) transcript_mappings$id else integer(0)

  # Mark successfully mapped transcripts
  if (length(mapped_ids) > 0) {
    mapped_ids_string <- paste(mapped_ids, collapse = ",")
    sql_success <- sprintf("
      UPDATE processed.msgraph_call_transcripts
      SET not_matchable_with_crm = FALSE
      WHERE id IN (%s)
    ", mapped_ids_string)

    success_count <- DBI::dbExecute(con, sql_success)
    logger(sprintf("Marked %d transcripts as matchable", success_count), "DEBUG")
  }

  # Mark non-mappable transcripts
  non_mappable_ids <- setdiff(as.character(ready_transcripts$id), as.character(mapped_ids))

  if (length(non_mappable_ids) > 0) {
    ids_string <- paste(non_mappable_ids, collapse = ",")
    sql <- sprintf("
      UPDATE processed.msgraph_call_transcripts
      SET not_matchable_with_crm = TRUE
      WHERE id IN (%s)
    ", ids_string)

    updated_count <- DBI::dbExecute(con, sql)
    logger(sprintf("Marked %d transcripts as not matchable", updated_count), "INFO")
  }

  if (nrow(transcript_mappings) == 0) {
    logger("No transcript summaries could be mapped to CRM entities", "WARNING")
    return(invisible(NULL))
  }

  logger(sprintf("Mapped %d transcripts to CRM entities", nrow(transcript_mappings)), "INFO")

  # Export to CRM
  export_results <- export_transcripts_to_crm(
    con = con,
    crm_api_key = crm_keys,
    transcript_mappings = transcript_mappings,
    use_test_account = use_test_account,
    batch_size = 50,
    max_retries = 3
  )

  # Log results
  logger(sprintf("CRM export completed: %d successful, %d failed",
                export_results$exported_count, export_results$failed_count), "INFO")

  if (length(export_results$errors) > 0) {
    logger("Export errors:", "WARNING")
    for (error in export_results$errors) {
      logger(sprintf("  %s", error), "WARNING")
    }
  }

  invisible(NULL)
}


################################################################################
# Core Export Functions
################################################################################

#' Map Transcript Summaries to CRM Entities
#'
#' Maps transcript participants to CRM people/companies for protocol attachment
#' @param con Database connection
#' @param transcript_summaries Dataframe with transcript summary data
#' @return Dataframe with CRM entity mappings added
#' @keywords internal
map_transcripts_to_crm_entities <- function(con, transcript_summaries) {

  # Get all unique call IDs from transcript summaries
  call_ids <- unique(transcript_summaries$call_id)

  # Get combined participants (actual + invited) for all relevant calls
  call_participants_full <- get_call_participants_combined(con, call_ids, include_event_attendees = TRUE)

  message(sprintf("Mapping %d transcript summaries using %d participant records (actual + invited)",
                  nrow(transcript_summaries),
                  nrow(call_participants_full)))

  # Convert to format compatible with existing logic
  call_participants <- call_participants_full %>%
    dplyr::select(call_id, contact_id) %>%
    dplyr::filter(!is.na(contact_id)) %>%
    dplyr::distinct()

  # Load CRM mapping tables - optimized to only collect relevant data
  # Only collect relevant CRM lead contact mappings for the call participants
  crm_lead_contact_mapping <- dplyr::tbl(con, I("mapping.crm_lead_msgraph_contact")) %>%
    dplyr::filter(msgraph_contact_id %in% local(call_participants$contact_id)) %>%
    dplyr::collect()

  # Only collect relevant msgraph contacts for call participants
  relevant_msgraph_ids <- unique(call_participants$contact_id)
  relevant_msgraph_ids <- relevant_msgraph_ids[!is.na(relevant_msgraph_ids)]  # Remove NAs

  msgraph_contacts <- dplyr::tbl(con, I("raw.msgraph_contacts")) %>%
    dplyr::filter(id %in% !!relevant_msgraph_ids) %>%
    dplyr::select(id, email) %>%
    dplyr::collect()

  # Get event organizers for calls that have event mappings
  event_organizers <- dplyr::tbl(con, I("mapping.msgraph_call_event")) %>%
    dplyr::filter(call_id %in% !!call_ids) %>%
    dplyr::left_join(
      dplyr::tbl(con, I("raw.msgraph_event_participants")) %>%
        dplyr::filter(is_organizer == TRUE) %>%
        dplyr::select(event_id, organizer_contact_id = contact_id),
      by = "event_id"
    ) %>%
    dplyr::select(call_id, organizer_contact_id) %>%
    dplyr::filter(!is.na(organizer_contact_id)) %>%
    dplyr::collect()

  # Check which event organizers also participated in the call
  event_organizers_in_call <- event_organizers %>%
    dplyr::inner_join(
      call_participants %>% dplyr::select(call_id, contact_id),
      by = c("call_id" = "call_id", "organizer_contact_id" = "contact_id")
    ) %>%
    dplyr::left_join(
      msgraph_contacts %>% dplyr::select(id, organizer_email = email),
      by = c("organizer_contact_id" = "id")
    ) %>%
    dplyr::select(call_id, organizer_email)

  # Get ALL internal Studyflix participants who actually joined the call
  # Exclude event invitees who didn't participate
  all_internal_participants <- call_participants_full %>%
    dplyr::filter(!is.na(email), grepl("@studyflix\\.de$", email, ignore.case = TRUE)) %>%
    dplyr::filter(participant_source != "event_invite") %>%  # Only actual call participants
    dplyr::arrange(call_id, email) %>%
    dplyr::distinct(call_id, email) %>%
    dplyr::select(call_id, internal_email = email)

  # Build prioritized list of internal participants per call
  # Priority: Organizer first (if participated), then all other internal participants
  internal_participants_list <- transcript_summaries %>%
    dplyr::select(id, call_id) %>%
    # Get organizer email
    dplyr::left_join(event_organizers_in_call, by = "call_id") %>%
    # Get all internal participant emails
    dplyr::left_join(
      all_internal_participants %>%
        dplyr::group_by(call_id) %>%
        dplyr::summarise(all_internal_emails = list(internal_email), .groups = "drop"),
      by = "call_id"
    ) %>%
    dplyr::mutate(organizer_in_call = organizer_email %in% unlist(all_internal_emails)) %>%
    # Build prioritized email list: organizer first, then others
    dplyr::rowwise() %>%
    dplyr::mutate(
      sales_user_emails = list({
        emails <- c()
        # Add organizer first if exists
        if (!is.na(organizer_email) & organizer_in_call) {
          emails <- c(emails, organizer_email)
        }
        # Add all other internal participants (excluding organizer to avoid duplicates)
        if (!is.null(all_internal_emails)) {
          other_emails <- setdiff(unlist(all_internal_emails), organizer_email)
          emails <- c(emails, other_emails)
        }
        unique(emails)  # Remove any duplicates
      })
    ) %>%
    dplyr::ungroup() %>%
    dplyr::select(id, call_id, sales_user_emails)

  # Get all unique emails for CRM user lookup
  all_sales_emails <- internal_participants_list %>%
    dplyr::pull(sales_user_emails) %>%
    unlist() %>%
    unique() %>%
    na.omit()

  # Map emails to CRM user info (ID and names)
  crm_users <- dplyr::tbl(con, I("raw.crm_users")) %>%
    dplyr::filter(user_login %in% !!all_sales_emails) %>%
    dplyr::select(id, user_login, user_first_name, user_name) %>%
    dplyr::collect()

  # Create mapping with comma-separated names
  sales_users_mapping <- internal_participants_list %>%
    dplyr::rowwise() %>%
    dplyr::mutate(
      sales_user_names = {
        if (length(sales_user_emails) == 0) {
          NA_character_
        } else {
          # Map each email to full name
          names_list <- sapply(sales_user_emails, function(email) {
            user_info <- crm_users %>% dplyr::filter(user_login == email)
            if (nrow(user_info) > 0 && !is.na(user_info$user_first_name) && !is.na(user_info$user_name)) {
              paste(user_info$user_first_name, user_info$user_name)
            } else {
              NA_character_
            }
          })
          # Remove NAs and combine with comma
          valid_names <- names_list[!is.na(names_list)]
          if (length(valid_names) > 0) {
            paste(valid_names, collapse = ", ")
          } else {
            NA_character_
          }
        }
      },
      # Also keep first user's ID for backward compatibility (if needed)
      author_user_id = {
        if (length(sales_user_emails) > 0) {
          first_user <- crm_users %>% dplyr::filter(user_login == sales_user_emails[1])
          if (nrow(first_user) > 0) {
            as.integer(first_user$id)
          } else {
            CRM_DEFAULT_AUTHOR_USER_ID
          }
        } else {
          CRM_DEFAULT_AUTHOR_USER_ID
        }
      }
    ) %>%
    dplyr::ungroup() %>%
    dplyr::select(id, author_user_id, sales_user_names)

  # Map transcripts to CRM leads through the correct join path:
  # transcripts -> calls -> participants -> contacts -> crm_leads
  transcript_mappings <- transcript_summaries %>%
    # Join with call participants to get msgraph contact IDs
    dplyr::left_join(
      call_participants %>%
        dplyr::select(call_id, msgraph_contact_id = contact_id),
      by = "call_id"
    ) %>%
    # Map msgraph contacts to CRM leads
    dplyr::left_join(
      crm_lead_contact_mapping %>%
        dplyr::select(msgraph_contact_id, crm_lead_id),
      by = "msgraph_contact_id"
    ) %>%
    dplyr::left_join(
      msgraph_contacts %>% dplyr::select(id, email),
      by = c("msgraph_contact_id" = "id")
    ) %>%
    # Filter out Studyflix emails (keep only external contacts or missing emails)
    dplyr::filter(is.na(email) | !grepl("@studyflix\\.de$", email, ignore.case = TRUE)) %>%
    # Set attachable type and ID (always people/leads)
    dplyr::mutate(
      crm_lead_id = as.integer(crm_lead_id),
      attachable_type = ifelse(!is.na(crm_lead_id), "people", NA_character_),
      attachable_id = crm_lead_id
    ) %>%
    dplyr::filter(!is.na(attachable_id)) %>%
    # Remove duplicates (one protocol per call)
    dplyr::group_by(call_id) %>%
    dplyr::slice_head(n = 1) %>%
    dplyr::ungroup() %>%
    # Add sales users mapping (author_user_id and sales_user_names)
    dplyr::left_join(
      sales_users_mapping,
      by = "id"
    ) %>%
    dplyr::mutate(
      author_user_id = ifelse(is.na(author_user_id), CRM_DEFAULT_AUTHOR_USER_ID, author_user_id)
    )

  # Filter to only mappable transcripts
  mappable_transcripts <- transcript_mappings %>%
    dplyr::filter(!is.na(attachable_id), !is.na(attachable_type)) %>% 
    dplyr::distinct()

  unmapped_count <- nrow(transcript_summaries) - nrow(mappable_transcripts)
  if (unmapped_count > 0) {
    message(sprintf("Warning: %d transcript summaries could not be mapped to CRM entities", unmapped_count))
  }

  # Log organizer mapping statistics
  organizer_mapped_count <- sum(!is.na(mappable_transcripts$author_user_id) & mappable_transcripts$author_user_id != CRM_DEFAULT_AUTHOR_USER_ID, na.rm = TRUE)
  organizer_fallback_count <- sum(mappable_transcripts$author_user_id == CRM_DEFAULT_AUTHOR_USER_ID, na.rm = TRUE)
  message(sprintf("Organizer mapping: %d mapped to CRM users, %d using fallback (user_id=%d)",
                  organizer_mapped_count, organizer_fallback_count, CRM_DEFAULT_AUTHOR_USER_ID))

  return(mappable_transcripts)
}


#' Export Transcript Summaries to CRM
#'
#' Creates protocols in CRM system for mapped transcript summaries
#' @param con Database connection
#' @param crm_api_key CRM API authentication key
#' @param transcript_mappings Dataframe with transcript-to-CRM mappings
#' @param use_test_account Boolean flag to use test account instead of production
#' @param batch_size Number of protocols to process in each batch
#' @param max_retries Maximum number of retry attempts for failed requests
#' @return List with export results and statistics
#' @keywords internal
export_transcripts_to_crm <- function(con, crm_api_key, transcript_mappings,
                                     use_test_account = FALSE, batch_size = 50, max_retries = 3) {

  if (nrow(transcript_mappings) == 0) {
    return(list(
      success = TRUE,
      exported_count = 0,
      failed_count = 0,
      errors = character(0)
    ))
  }

  # Set up API headers
  headers <- c(
    "content-type" = "application/json",
    "X-apikey" = crm_api_key,
    "Accept" = "*/*"
  )

  # Prepare dataframe for CRM API function
  crm_protocols <- transcript_mappings %>%
    dplyr::mutate(
      transcript_id = id,  # Preserve transcript ID for tracking
      protocol_header = paste("Teams Meeting Summary -",
                            format(as.Date(transcript_created_at), "%Y-%m-%d")),
      content = format_transcript_for_crm(transcript_summary, sales_user_names),
      field_type = "protocols",
      action = "add",
      badge = "meeting-summary"
    ) %>%
    dplyr::select(transcript_id, protocol_header, content, attachable_id, attachable_type, author_user_id, field_type, action, badge, sales_user_names) %>%
    dplyr::filter(!is.na(content), content != "") %>%
    dplyr::mutate(attachable_id = as.integer(attachable_id)) %>%
    # Map internal CRM lead IDs to external IDs
    dplyr::left_join(
      dplyr::tbl(con, I("raw.crm_leads")) %>%
        dplyr::select(id, crm_lead_id) %>%
        dplyr::collect(),
      by = c("attachable_id" = "id")
    ) %>%
    dplyr::mutate(attachable_id = crm_lead_id) %>%
    dplyr::select(-crm_lead_id) %>%
    dplyr::filter(!is.na(attachable_id)) %>%
    dplyr::select(transcript_id, protocol_header, content, attachable_id, attachable_type, author_user_id, field_type, action, badge, sales_user_names)

  # Export statistics tracking
  exported_count <- 0
  failed_count <- 0
  errors <- character(0)
  successful_transcript_ids <- character(0)
  failed_transcript_ids <- character(0)

  # Process in batches
  total_batches <- ceiling(nrow(crm_protocols) / batch_size)

  for (batch_num in seq_len(total_batches)) {
    start_row <- (batch_num - 1) * batch_size + 1
    end_row <- min(batch_num * batch_size, nrow(crm_protocols))
    batch_data <- crm_protocols[start_row:end_row, ]

    message(sprintf("Processing batch %d/%d (%d protocols)",
                   batch_num, total_batches, nrow(batch_data)))

    # Export each protocol in the batch
    for (i in seq_len(nrow(batch_data))) {
      row <- batch_data[i, ]
      transcript_id <- row$transcript_id
      success <- FALSE

      # Retry logic for transient failures
      for (attempt in seq_len(max_retries)) {
        tryCatch({
          result <- export_single_protocol_to_crm(con, headers, protocol_data = row, use_test_account)
          if (result$success) {
            exported_count <- exported_count + 1
            successful_transcript_ids <- c(successful_transcript_ids, transcript_id)
            success <- TRUE
            cat("✓ Successfully exported transcript ID:", transcript_id, "\n")

            # Mark this transcript as exported immediately
            mark_transcripts_as_exported(con, transcript_id)
            break
          } else {
            if (attempt == max_retries) {
              failed_count <- failed_count + 1
              failed_transcript_ids <- c(failed_transcript_ids, transcript_id)
              errors <- c(errors, sprintf("Transcript %s: %s", transcript_id, result$error))
              cat("❌ Failed to export transcript ID:", transcript_id, "after", max_retries, "attempts\n")
              cat("   Final error:", result$error, "\n")
              cat("   Continuing with next transcript...\n\n")
            } else {
              cat("⚠️  Attempt", attempt, "failed for transcript ID:", transcript_id, "- retrying...\n")
              Sys.sleep(2^attempt)  # Exponential backoff
            }
          }
        }, error = function(e) {
          if (attempt == max_retries) {
            failed_count <- failed_count + 1
            failed_transcript_ids <- c(failed_transcript_ids, transcript_id)
            errors <- c(errors, sprintf("Transcript %s: %s", transcript_id, e$message))
            cat("❌ Failed to export transcript ID:", transcript_id, "after", max_retries, "attempts\n")
            cat("   Final error:", e$message, "\n")
            cat("   Continuing with next transcript...\n\n")
          } else {
            cat("⚠️  Attempt", attempt, "failed for transcript ID:", transcript_id, "- retrying...\n")
            Sys.sleep(2^attempt)  # Exponential backoff
          }
        })
      }
    }

    # Brief pause between batches to respect rate limits
    if (batch_num < total_batches) {
      Sys.sleep(1)
    }
  }

  return(list(
    success = failed_count == 0,
    exported_count = exported_count,
    failed_count = failed_count,
    errors = errors,
    successful_transcript_ids = successful_transcript_ids,
    failed_transcript_ids = failed_transcript_ids
  ))
}


#' Export Single Protocol to CRM
#'
#' Internal helper function to export a single protocol to CRM
#' @param con Database connection
#' @param headers HTTP headers with authentication
#' @param protocol_data Single row of protocol data
#' @param use_test_account Boolean flag to use test account instead of production
#' @return List with success status and any error message
#' @keywords internal
export_single_protocol_to_crm <- function(con, headers, protocol_data, use_test_account = FALSE) {

  # Convert attachable_type for API compatibility
  attachable_type_mapped <- ifelse(
    protocol_data$attachable_type == "companies",
    "Company",
    ifelse(protocol_data$attachable_type == "people", "Person", protocol_data$attachable_type)
  )

  # Set account details based on test account flag
  if (use_test_account) {
    person_id <- 33560961  # Test account person ID
  } else {
    person_id <- protocol_data$attachable_id  # Production person ID
  }

  # print(protocol_data$attachable_id)
  # print(protocol_data$protocol_header)
  # print(paste0("https://studyflix-gmbh.centralstationcrm.net/people/", protocol_data$attachable_id))
  # browser()

  formatted_content <- protocol_data$content

  # Always use default user ID for CRM protocol creation
  # (Sales user information will be stored separately)

  # Build protocol name with sales user names appended (comma-separated list)
  # protocol_data$sales_user_names already contains the formatted string
  protocol_name <- if (!is.na(protocol_data$sales_user_names)) {
    paste0(protocol_data$protocol_header, " - ", protocol_data$sales_user_names)
  } else {
    protocol_data$protocol_header  # Fallback: use original name if no sales users found
  }

  # Create JSON payload
  json_data <- list(
    protocol = list(
      user_id = CRM_DEFAULT_AUTHOR_USER_ID,
      name = protocol_name,
      confidential = FALSE,
      content = formatted_content,
      updated_by_user_id = NULL,
      account_id = 2582,
      type = "ProtocolObjectNote",
      badge = "meeting",
      person_id = person_id,
      person_ids = list(person_id),
      format = "markdown"
    )
  )

  # Convert to JSON string
  body_string <- jsonlite::toJSON(json_data, auto_unbox = TRUE)

  print(body_string)  # For debugging

  # Execute POST request
  response <- httr::POST(
    "https://api.centralstationcrm.net/api/protocols?only_object_logging=true",
    httr::add_headers(headers),
    body = body_string,
    encode = "json"
  )

  status_code <- httr::status_code(response)

  if (status_code == 201) {
    return(list(success = TRUE, error = NULL))
  } else {
    response_text <- tryCatch({
      httr::content(response, "text")
    }, error = function(e) {
      "Unable to read response content"
    })
    error_msg <- sprintf("HTTP %d: %s", status_code, response_text)
    return(list(success = FALSE, error = error_msg))
  }
}


################################################################################
# Helper Functions
################################################################################

#' Format Transcript Summary for CRM Export
#'
#' Applies markdown formatting transformations to prepare transcript summaries for CRM export.
#' The function normalizes line endings, standardizes list bullets, removes trailing whitespace,
#' fixes bullet point formatting, adds section breaks after bold headers, and replaces underscores
#' with dashes to meet CRM style requirements.
#'
#' @param content Character vector of transcript summary content.
#'
#' @return Character vector with applied formatting transformations.
#'
#' @examples
#' format_transcript_for_crm(c("**Header**\\n- Item 1\\n- Item 2"))
#'
#' @export
format_transcript_for_crm <- function(content, sales_user_names = NULL, wrap_length = 100) {
  if (length(content) == 0 || all(is.na(content))) {
    return(content)
  }

  # Apply each formatting transformation sequentially
  formatted_content <- content

  # Replace narrow non-breaking spaces with regular spaces
  strange_spaces <- "[\u00A0\u202F\u2009\u200A\u200B]"
  formatted_content <- stringr::str_replace_all(formatted_content, strange_spaces, " ")

  # 1. Line ending normalization (convert escaped newlines to proper line endings)
  formatted_content <- gsub("\\\\n", "\r\n", formatted_content)

  # 2. Standardize list bullets (convert dashes to asterisks)
  formatted_content <- gsub("\r\n-", "\r\n*", formatted_content)
  formatted_content <- gsub("\r\n -", "\r\n*", formatted_content)

  # 3. Remove trailing whitespace before line endings
  formatted_content <- gsub("[ \t]+(\r?\n)", "\\1", formatted_content)

  # 4. Fix bullet point formatting (ensure space after asterisk)
  formatted_content <- gsub("\n\\*(?![ *])", "\n* ", formatted_content, perl = TRUE)

  # 5. Add section breaks after bold headers
  formatted_content <- gsub("\\*\\*\r\n\\*", "**\r\n\r\n*", formatted_content)

  # 6. Replace underscores with dashes (per CRM style requirements)
  formatted_content <- gsub("_", "-", formatted_content)

  # 7. Weitere Formatierungen aus export_single_protocol_to_crm
  formatted_content <- formatted_content %>%
    sub("\n\n", "\r\n\r\n---\r\n\r\n", .) %>%
    gsub("\n\\*\\*", "\n* **", .) %>%
    gsub("\n- ", "\r\n  * ", .) %>%
    gsub("\n  - ", "\r\n      * ", .) %>%
    gsub("\n    - ", "\r\n          * ", .) %>%
    gsub("\n      - ", "\r\n              * ", .)

  # 8. Append sales user names to first line of content (optional)
  if (!is.null(sales_user_names) && !is.na(sales_user_names)) {
    content_lines <- strsplit(formatted_content, "\r\n")[[1]]
    if (length(content_lines) > 0) {
      content_lines[1] <- paste0(content_lines[1], " - ", sales_user_names)
    }
    formatted_content <- paste(content_lines, collapse = "\r\n")
  }

  return(formatted_content)
}

#' Get Transcript Summaries Ready for CRM Export
#'
#' Retrieves deanonymized transcript summaries that haven't been exported yet
#' @param con Database connection
#' @return Dataframe with transcript summaries ready for export
#' @keywords internal
get_transcripts_ready_for_export <- function(con) {

  # Get transcripts with deanonymized summaries that haven't been exported
  # For non-matchable transcripts: retry for 30 days after creation
  # After 30 days, permanently exclude them (allows time for CRM data corrections)
  ready_transcripts <- dplyr::tbl(con, I("processed.msgraph_call_transcripts")) %>%
    dplyr::filter(
      !is.na(transcript_summary),
      transcript_summary != "",
      is.na(exported_to_crm) | exported_to_crm == FALSE,
      # Include if not marked as non-matchable OR if non-matchable but less than 30 days old
      not_matchable_with_crm == FALSE |
        (not_matchable_with_crm == TRUE &
         transcript_created_at > dbplyr::sql("CURRENT_TIMESTAMP - INTERVAL '30 days'"))
    ) %>%
    dplyr::collect()

  return(ready_transcripts)
}


#' Mark Transcripts as Exported to CRM
#'
#' Updates the database to mark successfully exported transcripts
#' @param con Database connection
#' @param transcript_ids Vector of transcript IDs that were successfully exported
#' @return Number of records updated
#' @keywords internal
mark_transcripts_as_exported <- function(con, transcript_ids) {

  if (length(transcript_ids) == 0) {
    return(0)
  }

  # Prepare SQL for batch update
  ids_string <- paste(transcript_ids, collapse = ",")

  sql <- sprintf("
    UPDATE processed.msgraph_call_transcripts
    SET exported_to_crm = TRUE
    WHERE id IN (%s)
  ", ids_string)

  updated_count <- DBI::dbExecute(con, sql)

  return(updated_count)
}
