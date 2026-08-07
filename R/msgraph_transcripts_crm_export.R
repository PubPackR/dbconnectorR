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


#' Propagate occurred_at to the CRM activity via a partial PUT
#'
#' CentralStation stores occurred_at on the protocol when it is sent with the
#' POST, but the feed is driven by a separate activity object that the POST does
#' not touch. Per CRM support (2026-04-27) the API mirrors the app's own
#' behaviour only on UPDATE: "Wenn du beim Update eines protocols occurred_at
#' uebergibst, wird das auch auf die activity durchgeschliffen."
#'
#' A protocol created via POST therefore shows the creation time in the UI until
#' a PUT follows. The payload is deliberately minimal so the content stays
#' untouched — same shape as the proven implementation in
#' base-11-CRM_Tagging/func/set_template_protocols.R (update_protocol_occurred_at).
#'
#' @param headers Named vector with request headers (content-type, X-apikey, Accept)
#' @param crm_protocol_id CRM-facing protocol ID from the POST response
#' @param occurred_at Timestamp as "YYYY-MM-DDTHH:MM:SS" (local wall clock)
#'
#' @return List with success (logical), status_code and error (character or NULL)
#' @keywords internal
# ---- start ---- #
crm_push_occurred_at_to_activity <- function(headers, crm_protocol_id, occurred_at) {

  if (is.null(crm_protocol_id) || is.na(crm_protocol_id) ||
      is.null(occurred_at) || is.na(occurred_at) || !nzchar(as.character(occurred_at))) {
    return(list(success = FALSE, status_code = NA_integer_,
                error = "protocol id or occurred_at missing"))
  }

  body_string <- jsonlite::toJSON(
    list(protocol = list(
      updated_by_user_id = CRM_DEFAULT_AUTHOR_USER_ID,
      occurred_at        = as.character(occurred_at)
    )),
    auto_unbox = TRUE
  )

  response <- Billomatics::crm_PUT(
    paste0("https://api.centralstationcrm.net/api/protocols/", crm_protocol_id),
    headers,
    body   = body_string,
    encode = "json"
  )

  status_code <- httr::status_code(response)

  if (status_code == 200) {
    list(success = TRUE, status_code = status_code, error = NULL)
  } else {
    list(success = FALSE, status_code = status_code,
         error = sprintf("PUT occurred_at failed with HTTP %s", status_code))
  }
}


#' Read the protocol ID out of a CRM create response
#'
#' @param response httr response object of the POST
#' @return Protocol ID or NULL
#' @keywords internal
# ---- start ---- #
crm_extract_protocol_id <- function(response) {
  body <- tryCatch(httr::content(response, as = "text", encoding = "UTF-8"),
                   error = function(e) "")
  if (length(body) == 0 || is.na(body[1]) || !nzchar(body[1])) return(NULL)

  parsed <- tryCatch(jsonlite::fromJSON(body[1], simplifyVector = FALSE),
                     error = function(e) NULL)
  if (is.null(parsed)) return(NULL)

  id <- parsed$protocol$id
  if (is.null(id)) id <- parsed$id
  if (is.null(id) || length(id) == 0) return(NULL)

  id[[1]]
}

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
#' @param debug_lead_id Optional integer. CRM lead ID (external) where unmappable
#'   transcripts are uploaded with debug info prepended. They remain marked as
#'   not_matchable_with_crm and are NOT marked as exported. Default: NULL (disabled)
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
                                          debug_lead_id = NULL,
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

  # Export unmappable transcripts to debug lead if configured
  if (!is.null(debug_lead_id) && length(non_mappable_ids) > 0) {
    logger(sprintf("Exporting %d unmappable transcripts to debug lead %s",
                   length(non_mappable_ids), debug_lead_id), "INFO")

    export_unmappable_to_debug_lead(
      con = con,
      crm_api_key = crm_keys,
      unmappable_transcripts = ready_transcripts %>%
        dplyr::filter(as.character(id) %in% non_mappable_ids),
      debug_lead_id = debug_lead_id,
      logger = logger
    )
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

  # Get ALL internal participants who actually joined the call
  # Exclude event invitees who didn't participate
  all_internal_participants <- call_participants_full %>%
    dplyr::filter(!is.na(email), grepl(get_internal_email_pattern(), email, ignore.case = TRUE)) %>%
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
    # Map msgraph contacts to CRM leads (include email_source_crm for prioritization)
    dplyr::left_join(
      crm_lead_contact_mapping %>%
        dplyr::select(msgraph_contact_id, crm_lead_id, email_source_crm),
      by = "msgraph_contact_id"
    ) %>%
    dplyr::left_join(
      msgraph_contacts %>% dplyr::select(id, email),
      by = c("msgraph_contact_id" = "id")
    ) %>%
    # Filter out internal emails (keep only external contacts or missing emails)
    dplyr::filter(is.na(email) | !grepl(get_internal_email_pattern(), email, ignore.case = TRUE)) %>%
    # Filter out synthetic emails (guest users, external organizers without real email)
    dplyr::filter(is.na(email) | !is_synthetic_email(email)) %>%
    # Set attachable type and ID (always people/leads)
    dplyr::mutate(
      crm_lead_id = as.integer(crm_lead_id),
      attachable_type = ifelse(!is.na(crm_lead_id), "people", NA_character_),
      attachable_id = crm_lead_id
    ) %>%
    dplyr::filter(!is.na(attachable_id)) %>%
    # Remove duplicates (one protocol per call), prioritizing by email source type
    # Priority: office > office_hq > other / private > from_description
    dplyr::group_by(call_id) %>%
    dplyr::arrange(
      factor(
        email_source_crm,
        levels = c("office", "office_hq", "other", "private", "from_description")
      ),
      .by_group = TRUE
    ) %>%
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

    # Debug: show why each unmapped transcript failed
    mapped_ids <- unique(mappable_transcripts$id)
    unmapped <- transcript_summaries %>% dplyr::filter(!id %in% mapped_ids)
    for (i in seq_len(nrow(unmapped))) {
      uid_debug <- unmapped$id[i]
      cid_debug <- unmapped$call_id[i]
      message(sprintf("  [DEBUG] Unmapped transcript id=%s, call_id=%s:", uid_debug, cid_debug))

      # Show all participants for this call
      call_parts_debug <- call_participants %>% dplyr::filter(call_id == cid_debug)
      contacts_debug <- msgraph_contacts %>% dplyr::filter(id %in% call_parts_debug$contact_id)
      crm_mapped_debug <- crm_lead_contact_mapping %>%
        dplyr::filter(msgraph_contact_id %in% call_parts_debug$contact_id)

      for (j in seq_len(nrow(contacts_debug))) {
        em <- contacts_debug$email[j]
        cid_contact <- contacts_debug$id[j]
        is_internal <- grepl(get_internal_email_pattern(), em, ignore.case = TRUE)
        is_synthetic <- is_synthetic_email(em)
        has_crm <- cid_contact %in% crm_mapped_debug$msgraph_contact_id
        crm_id <- if (has_crm) {
          crm_mapped_debug$crm_lead_id[crm_mapped_debug$msgraph_contact_id == cid_contact][1]
        } else NA
        reason <- if (is_internal) "INTERNAL" else if (is_synthetic) "SYNTHETIC" else if (!has_crm) "NO_CRM_MAPPING" else "OK"
        message(sprintf("    contact_id=%s | %s | %s | crm_lead_id=%s",
                        cid_contact, em, reason, ifelse(is.na(crm_id), "NA", crm_id)))
      }
      if (nrow(contacts_debug) == 0) {
        message("    No contacts found for this call")
      }
    }
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
      # transcript_created_at is UTC. as.Date() on a POSIXct defaults to UTC and
      # would shift late-afternoon meetings to the previous day, so the timezone
      # is passed explicitly. Same value feeds the header and occurred_at.
      meeting_date = as.Date(transcript_created_at, tz = "Europe/Berlin"),
      protocol_header = paste("Teams Meeting Summary -",
                            format(meeting_date, "%Y-%m-%d")),
      # CRM protocols carry occurred_at separately from their creation time.
      # Without it CentralStation stamps "now", which puts backfilled or
      # delayed summaries at the wrong place in the lead's feed.
      # Full local timestamp, not just the date: CentralStation reads a naive
      # value as Europe/Berlin wall clock, so the protocol lands at the actual
      # meeting time instead of midnight.
      occurred_at = format(transcript_created_at, "%Y-%m-%dT%H:%M:%S",
                           tz = "Europe/Berlin"),
      content = format_transcript_for_crm(transcript_summary, sales_user_names),
      field_type = "protocols",
      action = "add",
      badge = "meeting-summary"
    ) %>%
    dplyr::select(transcript_id, protocol_header, occurred_at, content, attachable_id, attachable_type, author_user_id, field_type, action, badge, sales_user_names) %>%
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
    dplyr::select(transcript_id, protocol_header, occurred_at, content, attachable_id, attachable_type, author_user_id, field_type, action, badge, sales_user_names)

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

  # Date of the meeting, not of the API call. The field name is confirmed by
  # analytics_04_sales/move_duplicate_template_protocols.R, which set it via
  # PUT /api/protocols/{id} with {"protocol":{"occurred_at":"YYYY-MM-DD"}}.
  # Only sent when known, so a missing date keeps the previous behaviour
  # (CentralStation stamps the creation time) instead of writing "NA".
  occurred_at <- protocol_data$occurred_at
  if (!is.null(occurred_at) && length(occurred_at) == 1 && !is.na(occurred_at) &&
      nzchar(occurred_at)) {
    json_data$protocol$occurred_at <- as.character(occurred_at)
  }

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
    # The POST stores occurred_at on the protocol but does not move the activity
    # that drives the CRM feed. A partial PUT does — see
    # crm_push_occurred_at_to_activity(). A failure here leaves the protocol in
    # place with the wrong feed position, so it is reported as a warning rather
    # than turning the whole export into an error.
    if (!is.null(json_data$protocol$occurred_at)) {
      crm_protocol_id <- crm_extract_protocol_id(response)

      if (is.null(crm_protocol_id)) {
        warning("Protocol created but its ID could not be read from the response ",
                "— occurred_at was not propagated to the CRM activity.")
      } else {
        put_result <- crm_push_occurred_at_to_activity(
          headers, crm_protocol_id, json_data$protocol$occurred_at
        )
        if (!isTRUE(put_result$success)) {
          warning(sprintf("Protocol %s created, but propagating occurred_at failed: %s",
                          crm_protocol_id, put_result$error))
        }
      }
    }

    return(list(success = TRUE, error = NULL))
  } else {
    response_text <- tryCatch({
      suppressWarnings(httr::content(response, "text"))
    }, error = function(e) {
      "Unable to read response content"
    })
    error_msg <- sprintf("HTTP %d: %s", status_code, response_text)
    return(list(success = FALSE, error = error_msg))
  }
}


#' Export a Single Transcript to CRM
#'
#' Manually exports a single transcript summary to CRM as a protocol.
#' Content can be provided directly as a string or read from the database via transcript_id.
#' Supports both creating new protocols (POST) and updating existing ones (PUT).
#'
#' @param con Database connection
#' @param crm_api_key CRM API authentication key
#' @param lead_id CRM lead ID (external person ID)
#' @param transcript_id Optional transcript ID to read transcript_summary from DB.
#'   Either transcript_id or content must be provided.
#' @param content Optional unformatted summary string. Will be formatted via
#'   format_transcript_for_crm(). Either transcript_id or content must be provided.
#' @param protocol_id Optional CRM protocol ID for updating an existing protocol (PUT).
#'   If NULL, a new protocol is created (POST).
#' @param sales_user_names Optional sales user names to append to content header and protocol name.
#' @param protocol_name Optional custom protocol name. If NULL, auto-generated as
#'   "Teams Meeting Summary - {date}".
#' @param occurred_at Optional date of the meeting as "YYYY-MM-DD". Determines
#'   where the protocol appears in the lead's feed. If NULL and a transcript_id
#'   is given, it is derived from transcript_created_at in Europe/Berlin. If it
#'   stays NULL, the field is omitted and CentralStation stamps the creation time.
#' @param use_test_account Logical, whether to use test CRM account. Default: FALSE
#'
#' @return List with success (logical) and error (character or NULL)
#'
#' @examples
#' \dontrun{
#' # Export from DB by transcript_id
#' msgraph_export_single_transcript_to_crm(
#'   con = con,
#'   crm_api_key = keys$crm,
#'   lead_id = 27552674,
#'   transcript_id = 12345
#' )
#'
#' # Export with direct content string
#' msgraph_export_single_transcript_to_crm(
#'   con = con,
#'   crm_api_key = keys$crm,
#'   lead_id = 27552674,
#'   content = "### Meeting Summary\n- Punkt 1\n- Punkt 2",
#'   sales_user_names = "Max Mustermann"
#' )
#'
#' # Update existing protocol
#' msgraph_export_single_transcript_to_crm(
#'   con = con,
#'   crm_api_key = keys$crm,
#'   lead_id = 27552674,
#'   content = "### Updated Summary\n- Neuer Punkt",
#'   protocol_id = 98765
#' )
#' }
#'
#' @export
msgraph_export_single_transcript_to_crm <- function(con,
                                                     crm_api_key,
                                                     lead_id,
                                                     transcript_id = NULL,
                                                     content = NULL,
                                                     protocol_id = NULL,
                                                     sales_user_names = NULL,
                                                     protocol_name = NULL,
                                                     occurred_at = NULL,
                                                     use_test_account = FALSE) {

  # Validate input: either transcript_id or content must be provided
  if (is.null(transcript_id) && is.null(content)) {
    stop("Either 'transcript_id' or 'content' must be provided.")
  }

  # Determine content source
  if (!is.null(transcript_id)) {
    transcript_data <- dplyr::tbl(con, I("processed.msgraph_call_transcripts")) %>%
      dplyr::filter(id == !!transcript_id | transcript_id == !!transcript_id) %>%
      dplyr::collect()

    if (nrow(transcript_data) == 0) {
      stop(sprintf("No transcript found with id/transcript_id: %s", transcript_id))
    }

    content <- transcript_data$transcript_summary[1]
    if (is.na(content) || content == "" || content == "NA") {
      stop(sprintf("Transcript %s has no valid summary (content is NA or empty).", transcript_id))
    }

    # Auto-generate protocol name from transcript date if not provided.
    # transcript_created_at is UTC — as.Date() without tz would move late
    # meetings to the previous day.
    if (!is.null(transcript_data$transcript_created_at)) {
      meeting_date <- as.Date(transcript_data$transcript_created_at[1],
                              tz = "Europe/Berlin")

      if (is.null(protocol_name)) {
        protocol_name <- paste("Teams Meeting Summary -",
                               format(meeting_date, "%Y-%m-%d"))
      }
      # Fall back to the meeting timestamp so the protocol lands at the right
      # spot in the lead's feed instead of being stamped with the API call time.
      # Local wall clock — CentralStation reads a naive value as Europe/Berlin.
      if (is.null(occurred_at)) {
        occurred_at <- format(transcript_data$transcript_created_at[1],
                              "%Y-%m-%dT%H:%M:%S", tz = "Europe/Berlin")
      }
    }
  }

  # Format content for CRM
  formatted_content <- format_transcript_for_crm(content, sales_user_names)

  # Build protocol name
  if (is.null(protocol_name)) {
    protocol_name <- paste("Teams Meeting Summary -", format(Sys.Date(), "%Y-%m-%d"))
  }
  if (!is.null(sales_user_names) && !is.na(sales_user_names)) {
    protocol_name <- paste0(protocol_name, " - ", sales_user_names)
  }

  # Set person_id based on test account flag
  person_id <- if (use_test_account) 33560961L else as.integer(lead_id)

  # Build JSON payload
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

  # Only send occurred_at when known. Passing it inside list() would keep a NULL
  # entry that toJSON serialises as {} — an empty object rather than no field.
  if (!is.null(occurred_at) && length(occurred_at) == 1 && !is.na(occurred_at) &&
      nzchar(as.character(occurred_at))) {
    json_data$protocol$occurred_at <- as.character(occurred_at)
  }

  body_string <- jsonlite::toJSON(json_data, auto_unbox = TRUE)

  # Set up API headers
  headers <- c(
    "content-type" = "application/json",
    "X-apikey" = crm_api_key,
    "Accept" = "*/*"
  )

  # Execute API call: POST (create) or PUT (update)
  if (is.null(protocol_id)) {
    # Create new protocol
    response <- httr::POST(
      "https://api.centralstationcrm.net/api/protocols?only_object_logging=true",
      httr::add_headers(headers),
      body = body_string,
      encode = "json"
    )
    expected_status <- 201
  } else {
    # Update existing protocol
    url <- sprintf("https://api.centralstationcrm.net/api/protocols/%s", protocol_id)
    response <- httr::PUT(
      url,
      httr::add_headers(headers),
      body = body_string,
      encode = "json"
    )
    expected_status <- 200
  }

  status_code <- httr::status_code(response)

  if (status_code == expected_status) {
    action <- if (is.null(protocol_id)) "created" else "updated"
    message(sprintf("Successfully %s protocol for lead %s", action, lead_id))

    # Only the create path needs the follow-up PUT: an update already carries
    # occurred_at through to the activity that drives the CRM feed.
    if (is.null(protocol_id) && !is.null(json_data$protocol$occurred_at)) {
      crm_protocol_id <- crm_extract_protocol_id(response)

      if (is.null(crm_protocol_id)) {
        warning("Protocol created but its ID could not be read from the response ",
                "— occurred_at was not propagated to the CRM activity.")
      } else {
        put_result <- crm_push_occurred_at_to_activity(
          headers, crm_protocol_id, json_data$protocol$occurred_at
        )
        if (isTRUE(put_result$success)) {
          message(sprintf("occurred_at propagated to activity (protocol %s)",
                          crm_protocol_id))
        } else {
          warning(sprintf("Protocol %s created, but propagating occurred_at failed: %s",
                          crm_protocol_id, put_result$error))
        }
      }
    }

    return(list(success = TRUE, error = NULL))
  } else {
    response_text <- tryCatch({
      suppressWarnings(httr::content(response, "text"))
    }, error = function(e) {
      "Unable to read response content"
    })
    error_msg <- sprintf("HTTP %d: %s", status_code, response_text)
    message(sprintf("Failed to export protocol: %s", error_msg))
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
  if (!is.null(sales_user_names) && !all(is.na(sales_user_names))) {
    # Process each transcript individually
    formatted_content <- mapply(function(content_item, names_item) {
      if (is.na(names_item)) {
        return(content_item)
      }
      content_lines <- strsplit(content_item, "\r\n")[[1]]
      if (length(content_lines) > 0) {
        content_lines[1] <- paste0(content_lines[1], " - ", names_item)
      }
      paste(content_lines, collapse = "\r\n")
    }, formatted_content, sales_user_names, SIMPLIFY = TRUE, USE.NAMES = FALSE)
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
      transcript_summary != "NA",
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


#' Export Unmappable Transcripts to Debug CRM Lead
#'
#' Uploads transcripts that could not be matched to CRM entities to a debug lead.
#' The debug info (participant details, mapping failure reasons) is prepended to
#' the transcript content. These transcripts are NOT marked as exported in the DB.
#'
#' @param con Database connection
#' @param crm_api_key CRM API key
#' @param unmappable_transcripts Dataframe of transcript records that couldn't be mapped
#' @param debug_lead_id CRM lead ID (external) for the debug lead
#' @param logger Logger function
#' @return Invisible NULL
#' @keywords internal
export_unmappable_to_debug_lead <- function(con, crm_api_key, unmappable_transcripts,
                                            debug_lead_id, logger) {

  if (nrow(unmappable_transcripts) == 0) return(invisible(NULL))

  # Load participant data needed for debug info
  call_ids <- unique(unmappable_transcripts$call_id)
  call_participants <- get_call_participants_combined(con, call_ids, include_event_attendees = TRUE)

  relevant_contact_ids <- unique(call_participants$contact_id)
  relevant_contact_ids <- relevant_contact_ids[!is.na(relevant_contact_ids)]

  msgraph_contacts <- dplyr::tbl(con, I("raw.msgraph_contacts")) %>%
    dplyr::filter(id %in% !!relevant_contact_ids) %>%
    dplyr::select(id, email, ms_name) %>%
    dplyr::collect()

  # Set up API headers
  headers <- c(
    "content-type" = "application/json",
    "X-apikey" = crm_api_key,
    "Accept" = "*/*"
  )

  exported_count <- 0

  for (i in seq_len(nrow(unmappable_transcripts))) {
    transcript <- unmappable_transcripts[i, ]

    # Build debug header with participant info
    debug_lines <- c(
      sprintf("Unmapped transcript id=%s, call id=%s", transcript$id, transcript$call_id),
      ""
    )

    # Get participants for this call
    parts <- call_participants %>%
      dplyr::filter(call_id == transcript$call_id) %>%
      dplyr::distinct(contact_id)

    contacts_for_call <- msgraph_contacts %>%
      dplyr::filter(id %in% parts$contact_id)

    if (nrow(contacts_for_call) > 0) {
      for (j in seq_len(nrow(contacts_for_call))) {
        em <- contacts_for_call$email[j]
        is_internal <- !is.na(em) && grepl(get_internal_email_pattern(), em, ignore.case = TRUE)
        label <- if (is_internal) "INTERNAL" else "EXTERN"
        debug_lines <- c(debug_lines, sprintf("%s | %s", label, em))
      }
    } else {
      debug_lines <- c(debug_lines, "No contacts found for this call")
    }

    debug_lines <- c(debug_lines, "", "---", "")

    # Build content: debug header + formatted transcript summary
    debug_header <- paste(debug_lines, collapse = "\r\n")
    formatted_summary <- format_transcript_for_crm(transcript$transcript_summary)
    full_content <- paste0(debug_header, formatted_summary)

    # Build protocol name. transcript_created_at is UTC — pass the timezone so
    # late meetings do not end up dated on the previous day.
    meeting_date <- as.Date(transcript$transcript_created_at, tz = "Europe/Berlin")
    protocol_name <- paste("Teams Meeting Summary -",
                           format(meeting_date, "%Y-%m-%d"),
                           "- UNMAPPED")

    # Create JSON payload — always goes to debug_lead_id
    json_data <- list(
      protocol = list(
        user_id = CRM_DEFAULT_AUTHOR_USER_ID,
        name = protocol_name,
        confidential = FALSE,
        content = full_content,
        updated_by_user_id = NULL,
        account_id = 2582,
        type = "ProtocolObjectNote",
        badge = "meeting",
        person_id = as.integer(debug_lead_id),
        person_ids = list(as.integer(debug_lead_id)),
        format = "markdown"
      )
    )

    if (!is.na(meeting_date)) {
      json_data$protocol$occurred_at <- format(meeting_date, "%Y-%m-%d")
    }

    body_string <- jsonlite::toJSON(json_data, auto_unbox = TRUE)

    tryCatch({
      response <- httr::POST(
        "https://api.centralstationcrm.net/api/protocols?only_object_logging=true",
        httr::add_headers(headers),
        body = body_string,
        encode = "json"
      )

      status_code <- httr::status_code(response)
      if (status_code == 201) {
        exported_count <- exported_count + 1
        logger(sprintf("  Uploaded unmapped transcript id=%s to debug lead %s",
                        transcript$id, debug_lead_id), "DEBUG")
      } else {
        response_text <- tryCatch(suppressWarnings(httr::content(response, "text")), error = function(e) "")
        logger(sprintf("  Failed to upload transcript id=%s: HTTP %d %s",
                        transcript$id, status_code, response_text), "WARNING")
      }
    }, error = function(e) {
      logger(sprintf("  Error uploading transcript id=%s: %s", transcript$id, e$message), "WARNING")
    })

    # Brief pause to respect rate limits
    Sys.sleep(0.5)
  }

  logger(sprintf("Uploaded %d/%d unmappable transcripts to debug lead %s",
                  exported_count, nrow(unmappable_transcripts), debug_lead_id), "INFO")
  invisible(NULL)
}
