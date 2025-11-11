################################################################################
# MS Graph Transcript Anonymization Module
#
# Complete anonymization logic using CoreNLP NER
# Handles speaker tags, names, companies, and personal data
################################################################################

################################################################################
# Main Wrapper Function
################################################################################

#' Anonymize Transcripts
#'
#' Anonymizes all transcripts that haven't been anonymized yet using CoreNLP NER.
#' This is step 2 of the transcript processing pipeline.
#'
#' @param con Database connection
#' @param preserved_names Character vector of names to preserve (e.g., company names).
#'   Default: c("studyflix", "study flix", "studi flix", "studiflix")
#' @param logger Logger function for output messages
#'
#' @return Invisible NULL (updates database)
#'
#' @examples
#' \dontrun{
#' msgraph_anonymize_transcripts(
#'   con = con,
#'   preserved_names = c("studyflix", "competitor_name")
#' )
#' }
#'
#' @export
msgraph_anonymize_transcripts <- function(con,
                                      preserved_names = c("studyflix"),
                                      logger = function(msg, level = "INFO") cat(msg, "\n")) {

  # Initialize CoreNLP
  path_to_core_nlp <- get_latest_corenlp_path()
  coreNLP::initCoreNLP(path_to_core_nlp, type = "german")
  logger("CoreNLP initialized", "DEBUG")

  # Get call IDs from transcripts that need anonymization
  transcript_call_ids <- dplyr::tbl(con, I("processed.msgraph_call_transcripts")) %>%
    dplyr::filter(is.na(transcript_content_anonymized) & !is.na(transcript_content)) %>%
    dplyr::select(call_id) %>%
    dplyr::distinct() %>%
    dplyr::pull()

  if (length(transcript_call_ids) == 0) {
    logger("No transcripts to anonymize", "INFO")
    return(invisible(NULL))
  }

  logger(sprintf("Found %d transcripts to anonymize", length(transcript_call_ids)), "INFO")

  # Get combined participants (actual + invited) using helper function
  call_participants_full <- get_call_participants_combined(
    con,
    transcript_call_ids,
    include_event_attendees = TRUE
  )

  logger(sprintf("Loaded %d participant records for %d calls",
                nrow(call_participants_full),
                length(unique(call_participants_full$call_id))), "INFO")

  # Convert to format expected by anonymize_transcripts
  call_participants <- call_participants_full %>%
    dplyr::select(call_id, contact_id) %>%
    dplyr::filter(!is.na(contact_id)) %>%
    dplyr::distinct()

  # Load additional required data
  contacts <- dplyr::tbl(con, I("raw.msgraph_contacts")) %>%
    dplyr::collect()

  lead_mails <- dplyr::tbl(con, I("raw.crm_lead_mail_address")) %>%
    dplyr::collect()

  positions <- dplyr::tbl(con, I("raw.crm_company_lead_positions")) %>%
    dplyr::collect()

  logger("Starting anonymization process", "INFO")

  # Call the main anonymization function
  anonymize_transcripts(
    con,
    call_participants,
    contacts,
    preserved_names,
    positions,
    lead_mails
  )

  logger("Transcript anonymization completed", "INFO")
  invisible(NULL)
}


#' Get Latest CoreNLP Path
#'
#' Finds the most recent CoreNLP installation
#'
#' @return Character string with path to CoreNLP directory
#' @keywords internal
get_latest_corenlp_path <- function() {
  # Base path depends on interactivity
  base_path <- if (interactive()) "opt" else "/opt"

  # List folders in that path
  folders <- list.dirs(base_path, full.names = FALSE, recursive = FALSE)
  folders <- folders[grepl("^stanford-corenlp-", folders)]

  if (length(folders) == 0) {
    stop("No Stanford CoreNLP folders found in ", base_path)
  }

  # Extract versions
  versions <- sub("^stanford-corenlp-", "", folders)

  # Pick the highest version
  latest_idx <- order(package_version(versions), decreasing = TRUE)[1]

  file.path(base_path, folders[latest_idx])
}

################################################################################
# Core Anonymization Functions
################################################################################

#' Anonymizes contents of the transcript table
#'
#' @param con Database connection
#' @param call_participants A data frame of participants (raw.msgraph_call_participants)
#' @param contacts A data frame of contacts (raw.msgraph_contacts)
#' @param preserved_names A vector of names to preserve
#' @param positions A data frame of CRM positions
#' @param lead_mails A data frame of CRM lead mail addresses
#' @param complete_overwrite Logical, whether to overwrite existing anonymizations
#' @return Invisible NULL
#' @keywords internal
anonymize_transcripts <- function(con,
                                  call_participants,
                                  contacts,
                                  preserved_names,
                                  positions,
                                  lead_mails,
                                  complete_overwrite = FALSE) {

  transcripts <- dplyr::tbl(con, I("processed.msgraph_call_transcripts")) %>%
    dplyr::filter(!is.na(transcript_content) & (is.na(transcript_content_anonymized) | complete_overwrite)) %>%
    dplyr::collect()

  if (nrow(transcripts) == 0) {
    message("No transcripts to anonymize.")
    return(invisible(NULL))
  }

  for (i in seq_len(nrow(transcripts))) {
    cat(sprintf("Anonymizing transcript %d of %d\n", i, nrow(transcripts)))

    transcript_id_out <- transcripts$transcript_id[i]
    call_id_out <- transcripts$call_id[i]
    content <- transcripts$transcript_content[i]

    # Get participants for this call
    participant_ids <- call_participants$contact_id[call_participants$call_id == call_id_out]

    if (length(participant_ids) == 0) {
      warning("No participants found for call_id: ", call_id_out)
      next
    }

    participant_info <- contacts[contacts$id %in% participant_ids, ]
    mails <- participant_info$email
    ids <- participant_info$id

    all_participants <- dplyr::tbl(con, I("raw.msgraph_contacts")) %>%
      dplyr::filter(id %in% ids) %>%
      dplyr::select(id, email) %>%
      dplyr::left_join(dplyr::tbl(con, I("raw.msgraph_users")) %>% dplyr::select(email, name), by = "email") %>%
      dplyr::collect()

    # Data for customers
    person_data_crm <- lead_mails %>%
      dplyr::select(mail_address_name, lead_id) %>%
      dplyr::left_join(all_participants, by = c("mail_address_name" = "email"), relationship = "many-to-many") %>%
      dplyr::filter(!is.na(id)) %>%
      dplyr::select(-id)

    tryCatch({
      anonymized_content <- anonymize_content(
        con,
        content = content,
        positions = positions,
        contacts = contacts,
        crm_lead_ids = person_data_crm,
        all_participants = all_participants,
        preserved_names = preserved_names,
        scope = "call_transcripts",
        content_id = transcripts$id[i]
      )

      # Update in database
      if (is.na(anonymized_content)) {
        # Internal call detected - set both content fields to NULL
        sql_query <- glue::glue_sql(
          "
          UPDATE processed.msgraph_call_transcripts
          SET transcript_content_anonymized = NULL,
              transcript_content = NULL
          WHERE id = {transcripts$id[i]}
          ",
          .con = con
        )
      } else {
        # External call - store anonymized content
        sql_query <- glue::glue_sql(
          "
          UPDATE processed.msgraph_call_transcripts
          SET transcript_content_anonymized = {anonymized_content}
          WHERE id = {transcripts$id[i]}
          ",
          .con = con
        )
      }

      DBI::dbExecute(con, sql_query)

    }, error = function(e) {
      cat("Failed to anonymize transcript ID:", transcript_id_out, "- Call ID:", call_id_out, "\n")
      cat("   Error:", e$message, "\n")
      cat("   Continuing with next transcript...\n\n")
    })
  }

  message("All transcripts anonymized and saved to the database.")
  return(invisible(NULL))
}


#' Anonymizes personal data in a text
#'
#' @param con Database connection
#' @param content The text to be anonymized
#' @param positions CRM positions data
#' @param contacts Contacts data
#' @param crm_lead_ids CRM lead IDs
#' @param all_participants All participants data
#' @param preserved_names A vector of names to be preserved
#' @param scope Scope of anonymization ("call_transcripts" or "crm_protocols")
#' @param content_id ID of the content being anonymized
#' @return Text with anonymized personal data
#' @keywords internal
anonymize_content <- function(con, content, positions, contacts, crm_lead_ids, all_participants, preserved_names, scope, content_id) {

  if (is.na(content) || content == "") {
    warning("Transcript content is NA or empty. Skipping anonymization.")
    return(NA)
  }

  # get token of the transcript and cleans them
  tokens <- coreNLP::annotateString(content)$token
  tokens <- clean_token(tokens, preserved_names)

  known_information <- c()

  if(scope == "call_transcripts") {
    # anonymize all names in <v ... >
    information_speaker <- anonymize_speaker(con, tokens) # replace with speaker_xxxxxx
    known_information <- dplyr::bind_rows(known_information, information_speaker)

    # anonymize employees and customers
    information_lead_msgraph <- anonymize_names((all_participants %>% dplyr::filter(is.na(name)))$id, contacts, tokens) %>% dplyr::mutate(person_type = "Lead")
    known_information <- dplyr::bind_rows(known_information, information_lead_msgraph)

    # anonymize studyflix sales employees
    information_sales_msgraph <- anonymize_names((all_participants %>% dplyr::filter(!is.na(name)))$id, contacts, tokens) %>% dplyr::mutate(person_type = "Salesperson")
    known_information <- dplyr::bind_rows(known_information, information_sales_msgraph)
  }

  if(nrow(crm_lead_ids) > 0) {
    information_crm_data <- anonymize_with_crm_data(con, crm_lead_ids, positions, tokens)
    known_information <- dplyr::bind_rows(known_information, information_crm_data)
  }

  # replace PERSON NER with a specific id
  information_ner <- id_anonymize_persons(con, tokens)
  known_information <- dplyr::bind_rows(known_information, information_ner)

  if(scope == "call_transcripts") {
    tokens <- process_transcript_placeholders(con, known_information, tokens, content_id, scope)
  } else {
    stop(paste("Unknown scope:", scope, ". Only 'call_transcripts' is supported in this module."))
  }

  # Check if process_transcript_placeholders returned NULL (internal call detected)
  if (is.null(tokens)) {
    return(NA_character_)
  }

  # choose tokens that should be replace
  tokens$anonymized <- ifelse(
    !(tokens$NER %in% c("O", "MISC", "LOCATION", "ORGANIZATION", "PERSON")),
    tokens$NER,
    tokens$token
  )

  # put tokens back into a text
  anonymized_text <- paste(tokens$anonymized, collapse = " ")

  return(anonymized_text)
}


#' Anonymizes speaker tags like <v Max Müller> to SPEAKER/00001 in the NER column
#'
#' @param con Database connection
#' @param tokens A data frame with a column `token` and `NER`
#' @return Updated tokens with speaker names anonymized in the NER column
#' @keywords internal
anonymize_speaker <- function(con, tokens) {
  # Match speaker tags like <v Max Müller>
  speaker_pattern <- "^<v\\s.+\\s?>$"
  speaker_tokens <- unique(na.omit(tokens$token[grepl(
    speaker_pattern,
    tokens$token
  )]))

  if (length(speaker_tokens) == 0) {
    return(tokens)
  }

  # Step 1: Fetch existing speaker mappings from DB
  tokens_safe <- na.omit(speaker_tokens)
  tokens_safe <- tokens_safe[tokens_safe != ""]
  tokens_df <- data.frame(token = tokens_safe, stringsAsFactors = FALSE)
  tokens_only_contact <- tokens_df %>%
    dplyr::mutate(system = "msgraph") %>%
    dplyr::mutate(name_clean = gsub(" ", "", gsub("^<v\\s+|>$", "", token)))

  tokens_with_id <- dplyr::tbl(con, I("raw.msgraph_contacts")) %>%
    dplyr::select(id, ms_name) %>%
    dplyr::rename(name = ms_name) %>%
    dplyr::mutate(name_clean = dbplyr::sql("REPLACE(name, ' ', '')")) %>%
    dplyr::filter(name_clean %in% tokens_only_contact$name_clean) %>%
    dplyr::collect() %>%
    dplyr::full_join(tokens_only_contact, by = "name_clean") %>%
    dplyr::select(-name_clean) %>%
    dplyr::mutate(type = "SPEAKER") %>%
    dplyr::mutate(
      only_name = dplyr::if_else(grepl("^<v\\s+[^>]+>$", token), gsub("^<v\\s+|>$", "", token), NA_character_),
      formatted_name = dplyr::if_else(
        stringr::str_detect(token, ","),
        stringr::str_replace(only_name, "^([^,]+),\\s*(.*)$", "\\2 \\1"),
        only_name),
      formatted_name = stringr::str_trim(formatted_name)
    ) %>%
    dplyr::mutate(name = dplyr::coalesce(name, formatted_name)) %>%
    dplyr::select(-formatted_name, -only_name)

  return(tokens_with_id)
}


#' Anonymizes names in a token dataframe based on contact IDs
#'
#' @param participant_ids A vector of contact IDs of participants
#' @param contacts A dataframe of contacts (id, ms_name, email)
#' @param tokens A dataframe with tokens
#' @param prefix Prefix for anonymization (unused parameter, kept for compatibility)
#' @return A dataframe with anonymized tokens
#' @keywords internal
anonymize_names <- function(participant_ids, contacts, tokens, prefix) {

  if(length(participant_ids) == 0) {
    return(tibble::tibble())
  }

  names_long <- contacts %>%
    dplyr::filter(id %in% participant_ids) %>%
    dplyr::select(id, ms_name) %>%
    dplyr::mutate(
      name = trimws(ms_name),
      # handle "Nachname, Vorname" by swapping
      ms_name_clean = dplyr::if_else(stringr::str_detect(name, ","),
                              stringr::str_trim(stringr::str_c(stringr::str_trim(stringr::str_extract(name, "(?<=,).*")), " ", stringr::str_trim(stringr::str_extract(name, "^[^,]+")))),
                              name),
      split_name = stringr::str_split(ms_name_clean, "\\s+"),
      FIRST_NAME = sapply(split_name, function(x) if(length(x) > 1) paste(head(x, -1), collapse = " ") else ""),
      NAME = sapply(split_name, function(x) tail(x, 1)),
      FULL_NAME = ms_name_clean
    ) %>%
    dplyr::select(id, name, FIRST_NAME, NAME, FULL_NAME) %>%
    tidyr::pivot_longer(cols = c(FIRST_NAME, NAME, FULL_NAME),
                names_to = "type",
                values_to = "token") %>%
    dplyr::filter(token != "") %>%  # optional, falls FIRST_NAME leer war
    dplyr::mutate(system = "msgraph")

  return(names_long)
}


#' Processes transcript placeholders
#'
#' @param con Database connection
#' @param known_information Known information about participants
#' @param tokens Token dataframe
#' @param content_id Content ID
#' @param scope Scope of processing
#' @return Updated tokens dataframe or NULL if internal call
#' @keywords internal
process_transcript_placeholders <- function(con, known_information, tokens, content_id, scope) {

  # clean known information
  known_information_cleaned <- known_information %>%
    dplyr::distinct() %>%
    dplyr::mutate(nullable_id = dplyr::if_else(is.na(id) & system != "ner", TRUE, FALSE)) %>%
    dplyr::mutate(id = as.integer(dplyr::if_else(is.na(id) & system != "ner", dplyr::row_number(), id))) %>%
    dplyr::group_by(id) %>%
    dplyr::mutate(person_type = dplyr::last(na.omit(person_type))) %>%
    dplyr::ungroup() %>%
    dplyr::mutate(
      formatted_name = dplyr::if_else(
        !is.na(name) & stringr::str_detect(name, ",") & !grepl("COMPANY", type),
        stringr::str_replace(name, "^([^,]+),\\s*(.*)$", "\\2 \\1"),
        ifelse(grepl("COMPANY", type), NA, dplyr::if_else(is.na(name), token, name))
      )
    ) %>%
    dplyr::mutate(name = trimws(gsub("  ", " ", name))) %>%
    dplyr::mutate(formatted_name = trimws(gsub("  ", " ", formatted_name))) %>%
    dplyr::group_by(formatted_name) %>%
    dplyr::mutate(person_type = dplyr::last(na.omit(person_type))) %>%
    dplyr::ungroup() %>%
    dplyr::mutate(person_type = dplyr::coalesce(person_type, "Lead")) %>%
    dplyr::filter(!is.na(person_type)) %>%
    dplyr::group_by(id) %>%
    dplyr::mutate(formatted_name = ifelse(!is.na(id), dplyr::last(na.omit(formatted_name)), formatted_name)) %>%
    dplyr::ungroup() %>%
    dplyr::group_by(token) %>%
    dplyr::filter(!(dplyr::n() > 1 & system == "ner")) %>%
    dplyr::ungroup() %>%
    dplyr::filter(!(person_type == "Unknown" & token %in% c(
      "Social", "Media", "Marketing", "Branding", "Employer",
      "E-Learning", "Learning", "Plattform", "Klicktechnisch"
    ))) %>%
    dplyr::mutate(id = ifelse(nullable_id, 0, id)) %>%
    dplyr::select(-nullable_id)

  # build placeholders
  placeholders <- known_information_cleaned %>%
    dplyr::filter(person_type %in% c("Lead", "Salesperson", "Unknown") & !grepl("COMPANY", type)) %>%
    dplyr::distinct(person_type, formatted_name) %>%
    dplyr::arrange(person_type, formatted_name) %>%
    dplyr::group_by(person_type) %>%
    dplyr::mutate(placeholder = paste0(person_type, "_", dplyr::row_number()))

  replacements_and_placeholders <- known_information_cleaned %>%
    dplyr::left_join(placeholders, by = c("person_type", "formatted_name")) %>%
    dplyr::mutate(type = stringr::str_to_title(stringr::str_to_lower(gsub("COMPANY_", "", type)))) %>%
    dplyr::group_by(formatted_name, type) %>%
    dplyr::mutate(additional_number = dplyr::row_number()) %>%
    dplyr::mutate(placeholder = ifelse(
      type %in% c("City", "Street", "Zip", "Company"),
      paste0(placeholder, "_", type, "_", additional_number),
      placeholder
    )) %>%
    dplyr::mutate(placeholder = ifelse(
      grepl("<v ", token),
      paste0("< Speaker: ", placeholder, " >"),
      paste0("< ", placeholder, " >")
    )) %>%
    dplyr::arrange(person_type, dplyr::desc(nchar(token)))

  # replace tokens in text
  for (i in seq_len(nrow(replacements_and_placeholders))) {
    tokens <- find_and_replace_personal_data(
      tokens,
      replacements_and_placeholders$token[i],
      replacements_and_placeholders$placeholder[i]
    )
  }

  # track which replacements were used
  used_replacement_tokens <- tokens %>%
    dplyr::distinct(NER) %>%
    dplyr::filter(!(NER %in% c("O", "MISC", "LOCATION", "ORGANIZATION"))) %>%
    dplyr::left_join(replacements_and_placeholders, by = c("NER" = "placeholder")) %>%
    dplyr::filter(!is.na(token)) %>%
    dplyr::mutate(actual_value = ifelse(
      grepl("name", type, ignore.case = TRUE) | type == "Speaker",
      formatted_name,
      token
    )) %>%
    dplyr::mutate(
      crm_id = ifelse(system == "crm", id, 0),
      ms_id = ifelse(system == "msgraph", id, 0),
      crm_user_id = ifelse(system == "crm_user", id, 0)
    ) %>%
    dplyr::distinct(NER, actual_value, person_type, crm_id, ms_id, crm_user_id) %>%
    dplyr::group_by(NER) %>%
    dplyr::summarise(
      actual_value = dplyr::first(actual_value),
      person_type  = dplyr::first(person_type),
      crm_id       = max(crm_id),
      ms_id        = max(ms_id),
      crm_user_id  = max(crm_user_id),
      .groups = "drop"
    ) %>%
    dplyr::mutate(
      crm_id = ifelse(crm_id == 0, NA_integer_, crm_id),
      ms_id = ifelse(ms_id == 0, NA_integer_, ms_id),
      crm_user_id = ifelse(crm_user_id == 0, NA_integer_, crm_user_id)
    ) %>%
    dplyr::rename(placeholder = NER) %>%
    dplyr::mutate(transcript_id = content_id)

    # Check if any Lead_ placeholders exist - if not, this is an internal call
    has_lead_placeholders <- any(grepl("Lead_", used_replacement_tokens$placeholder, fixed = TRUE))

    if (!has_lead_placeholders) {
      # Internal call detected - don't populate placeholder table and return NULL to indicate early exit
      return(NULL)
    }

    # upsert into DB only if external participants (leads) were found
    Billomatics::postgres_upsert_data(
      con,
      "processed",
      "msgraph_call_transcript_placeholders",
      used_replacement_tokens,
      match_cols = c("transcript_id", "placeholder"),
      delete_missing = FALSE
    )

  # return updated tokens
  return(tokens)
}


#' Gives anonymized IDs to PERSON tokens using PERSON/xxxxx format
#'
#' @param con Database connection
#' @param tokens A dataframe with a `token` column and `NER` column
#' @return The updated tokens dataframe with NER values anonymized
#' @keywords internal
id_anonymize_persons <- function(con, tokens) {

  # Step 1: Get unique PERSON tokens
  person_tokens <- unique(na.omit(tokens$token[tokens$NER == "PERSON"]))

  if (length(person_tokens) == 0) {
    return(tibble::tibble())
  }

  # Step 2: Fetch existing mappings

  tokens_safe <- na.omit(person_tokens)
  tokens_safe <- tokens_safe[tokens_safe != ""]

  tokens_df <- data.frame(token = tokens_safe, stringsAsFactors = FALSE)
  tokens_only_contact <- tokens_df %>%
    dplyr::mutate(system = "ner") %>%
    dplyr::mutate(type = "PERSON") %>%
    dplyr::mutate(person_type = "Unknown")

  return(tokens_only_contact)
}


#' Cleans tokens, filters out tokens in PERSON that are too short
#' Finds and replaces emails, phone numbers and websites
#'
#' @param tokens A dataframe with tokens
#' @param preserved_names A vector of names to be preserved
#' @return A dataframe with cleaned tokens
#' @keywords internal
clean_token <- function(tokens, preserved_names) {

  tokens <- merge_v_tags(tokens)

  # Clean up raw tokens
  tokens$token <- clean_phone_numbers(tokens$token)

  # Replace NER values with tags
  tokens <- replace_phone_numbers(tokens)
  tokens <- replace_emails(tokens)
  tokens <- replace_websites(tokens)

  # Update NER for short PERSONs and preserved names
  tokens <- tokens %>%
    dplyr::mutate(token_lower_case = stringr::str_to_lower(token)) %>%
    dplyr::mutate(NER = dplyr::if_else((NER == "PERSON" & nchar(token) <= 2) | token_lower_case %in% preserved_names, "O", NER)) %>%
    dplyr::select(-token_lower_case)

  return(tokens)
}

################################################################################
# Replacement of websites, emails, phone numbers
################################################################################

#' Finds phone numbers in a text and removes spaces
#'
#' @param text_vector A character vector with text
#' @return A character vector with cleaned phone numbers
#' @keywords internal
clean_phone_numbers <- function(text_vector) {

  # regularexpression for phone numbers: digits, possibly separated by spaces
  pattern <- "(\\d[\\d\\s]*\\d)"

  # removes spaces in the found phone number
  replace_function <- function(match) {
    gsub(" ", "", match)
  }

  cleaned_text <- stringr::str_replace_all(text_vector, pattern, replace_function)

  return(cleaned_text)
}


#' Replaces phone numbers in the NER column with <TEL>
#'
#' @param tokenized_df A dataframe with tokens
#' @return A dataframe with replaced phone numbers
#' @keywords internal
replace_phone_numbers <- function(tokenized_df) {

  # Regex pattern for phone numbers
  phone_pattern <- "^\\+?\\d{1,4}[\\s\\-]?\\(?\\d{2,}\\)?[\\s\\-]?\\d{3,}[\\s\\-]?\\d{3,}$"

  # check if token is a phone number and replace with <TEL>
  tokenized_df$NER <- ifelse(grepl(phone_pattern, tokenized_df$token), "<TEL>", tokenized_df$NER)

  return(tokenized_df)
}


#' Replaces websites in the NER column with <WEBSITE>
#'
#' @param tokens_df A dataframe with tokens
#' @return A dataframe with replaced websites
#' @keywords internal
replace_websites <- function(tokens_df) {
  web_pattern <- "(https?://(?:www\\.)?[a-zA-Z0-9-]+\\.[a-zA-Z]{2,6}(/[^\\s]*)?|www\\.[a-zA-Z0-9-]+\\.[a-zA-Z]{2,6}(/[^\\s]*)?)"

  tokens_df <- tokens_df %>%
    dplyr::mutate(NER = dplyr::if_else(
      grepl(web_pattern, token),
      "<WEBSITE>",
      NER
    ))

  return(tokens_df)
}


#' Replaces emails in the NER column with <EMAIL>
#'
#' @param tokens_df A dataframe with tokens
#' @return A dataframe with replaced emails
#' @keywords internal
replace_emails <- function(tokens_df) {
  email_pattern <- "[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}"

  tokens_df <- tokens_df %>%
    dplyr::mutate(NER = dplyr::if_else(
      grepl(email_pattern, token),
      "<EMAIL>",
      NER
    ))

  return(tokens_df)
}

################################################################################
# CRM Data Functions
################################################################################

#' Anonymizes customers in a token dataframe
#'
#' @param con Database connection
#' @param crm_lead_ids A dataframe with CRM lead IDs
#' @param positions A dataframe with CRM positions
#' @param tokens A dataframe with tokens
#' @return A dataframe with anonymized tokens for customers
#' @keywords internal
anonymize_with_crm_data <- function(con, crm_lead_ids, positions, tokens) {

  known_information <- c()

  # iterate over all persons in the call and anonymize the data
  for (id_var in 1:nrow(crm_lead_ids)) {
    lead_id_var <- crm_lead_ids$lead_id[id_var]

    if (is.na(crm_lead_ids$name[id_var])) {
      # CRM Protocol: Get both lead data AND salesperson data
      positions_ <- positions %>%
        dplyr::filter(lead_id == lead_id_var)

      company_id <- positions_ %>%
        dplyr::filter(lead_id == lead_id_var) %>%
        dplyr::pull(company_id)

      # Get lead/customer data (names, company, addresses)
      known_information <- dplyr::bind_rows(known_information, filter_person_data_all(con, tokens_df = tokens, person_ids = as.integer(lead_id_var), company_ids = company_id))

      # Also get salesperson data (responsible user, protocol writers)
      known_information <- dplyr::bind_rows(known_information, filter_salesperson_crm_data(con, tokens_df = tokens, person_ids = as.integer(lead_id_var)))
    } else {
      # MS Teams Transcript: Only anonymize salesperson data (internal call)
      known_information <- dplyr::bind_rows(known_information, filter_salesperson_crm_data(con, tokens, as.integer(lead_id_var)))
    }
  }

  return(known_information)
}


#' Finds personal data in a text and replaces it with a placeholder
#'
#' @param con Database connection
#' @param tokens_df A dataframe with tokens
#' @param person_ids The ids of the persons
#' @param company_ids The ids of the companies
#' @return A dataframe with replaced personal data
#' @keywords internal
filter_person_data_all <- function(con, tokens_df, person_ids, company_ids) {

  leads <- dplyr::tbl(con, I("raw.crm_leads")) %>% dplyr::filter(id %in% person_ids) %>% dplyr::collect()
  lead_address <- dplyr::tbl(con, I("processed.crm_lead_address_with_fallback")) %>%
    dplyr::mutate(lead_id = as.integer(lead_id)) %>%
    dplyr::filter(lead_id %in% person_ids) %>%
    dplyr::collect()
  companies <- dplyr::tbl(con, I("raw.crm_companies")) %>% dplyr::filter(id %in% company_ids) %>% dplyr::collect()
  company_address <- dplyr::tbl(con, I("raw.crm_company_address")) %>% dplyr::filter(company_id %in% company_ids) %>% dplyr::collect()

  name <- safe_extract(leads, "lead_first_name")
  surname <- safe_extract(leads, "lead_name")
  person_id <- safe_extract(leads, "id")

  # --- Address: Lead ---
  city_person   <- safe_extract(lead_address, "city") %>% gsub("[0-9]", "", .) %>% trimws()
  street_person <- safe_extract(lead_address, "street") %>% gsub("[0-9]", "", .) %>% trimws()
  zip_person    <- safe_extract(lead_address, "zip")

  # --- Company Info ---
  company_name <- compress_column(safe_extract(companies, "company_name"))
  company_id   <- safe_extract(companies, "crm_company_id")

  # Break company name into tokens
  company_name_parts <- if (!is.na(company_name)) unlist(strsplit(company_name, split = " ")) else character(0)

  # --- Address: Company ---
  city_company   <- safe_extract(company_address, "city") %>% gsub("[0-9]", "", .) %>% trimws()
  street_company <- safe_extract(company_address, "street") %>% gsub("[0-9]", "", .) %>% trimws()
  zip_company    <- safe_extract(company_address, "zip")

  # --- Leads: Hole historische Namen ---
  lead_names_historical <- dplyr::tbl(con, I("raw.crm_lead_names")) %>%
    dplyr::filter(!is.na(name_removed_at)) %>%
    dplyr::filter(lead_id %in% person_ids) %>%
    dplyr::collect()

  # Erstelle Tokens für aktuellen Namen (Priority 1 = zuerst gesucht)
  current_name_tokens <- tibble::tibble(
    token = c(surname, name, paste(name, surname)),
    type = c("SURNAME", "FIRST_NAME", "FULL_NAME"),
    priority = 1,
    name_for_token = paste(name, surname)  # Vollständiger aktueller Name
  ) %>% dplyr::filter(!is.na(token) & token != "")

  # Erstelle Tokens für alle historischen Namen (Priority 2 = danach gesucht)
  if (nrow(lead_names_historical) > 0) {
    historical_name_tokens <- lead_names_historical %>%
      dplyr::filter(!is.na(first_name_new) | !is.na(name_new)) %>%
      dplyr::rowwise() %>%
      dplyr::mutate(
        full_name = paste(first_name_new, name_new),
        name_for_token = paste(first_name_new, name_new)  # Historischer vollständiger Name
      ) %>%
      dplyr::ungroup() %>%
      dplyr::select(first_name_new, name_new, full_name, name_for_token) %>%
      tidyr::pivot_longer(cols = c(first_name_new, name_new, full_name),
                   names_to = "type_name",
                   values_to = "token") %>%
      dplyr::filter(!is.na(token) & token != "NA" & token != "") %>%
      dplyr::mutate(
        type = dplyr::case_when(
          type_name == "first_name_new" ~ "FIRST_NAME",
          type_name == "name_new" ~ "SURNAME",
          type_name == "full_name" ~ "FULL_NAME"
        ),
        priority = 2
      ) %>%
      dplyr::select(token, type, priority, name_for_token)
  } else {
    historical_name_tokens <- tibble::tibble(
      token = character(),
      type = character(),
      priority = integer(),
      name_for_token = character()
    )
  }

  # Kombiniere: Aktuelle Namen ZUERST, dann historische (bei Duplikaten: erste behalten)
  all_name_tokens <- dplyr::bind_rows(current_name_tokens, historical_name_tokens) %>%
    dplyr::arrange(priority, dplyr::desc(nchar(token))) %>%
    dplyr::distinct(token, .keep_all = TRUE)

  # Erstelle lead_tokens mit allen Namen
  # WICHTIG: Jeder Token behält seinen eigenen Namen (aktuell ODER historisch)
  lead_tokens <- all_name_tokens %>%
    dplyr::mutate(
      id = person_id,
      name = name_for_token,  # Verwende den Namen des jeweiligen Tokens (aktuell oder historisch)
      system = "crm",
      person_type = "Lead"
    ) %>%
    dplyr::select(-priority, -name_for_token)

  if(!is.na(company_id)) {
      # --- Company ---
    company_tokens <- tibble::tibble(
      id         = person_id,
      additional_id = company_id,
      name       = company_name,
      token      = company_name_parts,
      system     = "crm",
      type       = "COMPANY",
      person_type= "Lead"
    )
  } else {
    company_tokens <- tibble::tibble()
  }


  # --- Lead Address ---
  lead_address_tokens <- tibble::tibble(
    id         = person_id,
    name    = paste(name, surname),
    token      = c(city_person, street_person, zip_person),
    system     = "crm",
    type       = c("CITY", "STREET", "ZIP"),
    person_type= "Lead"
  )

  if(!is.na(company_id)) {
    # --- Company Address ---
    company_address_tokens <- tibble::tibble(
      id         = person_id,
      additional_id = company_id,
      name    = company_name,
      token      = c(city_company, street_company, zip_company),
      system     = "crm",
      type       = c("COMPANY_CITY", "COMPANY_STREET", "COMPANY_ZIP"),
      person_type= "Lead"
    )
  } else {
    company_address_tokens <- tibble::tibble()
  }

  # --- Alles zusammen ---
  all_tokens <- dplyr::bind_rows(
    lead_tokens,
    company_tokens,
    lead_address_tokens,
    company_address_tokens
  ) %>%
    dplyr::filter(!is.na(token) & token != "")

  return(all_tokens)
}


#' Finds salesperson data in CRM and returns tokens
#'
#' @param con Database connection
#' @param tokens_df A dataframe with tokens
#' @param person_ids The ids of the persons
#' @return A dataframe with salesperson tokens
#' @keywords internal
filter_salesperson_crm_data <- function(con, tokens_df, person_ids) {

  responsible_users <- dplyr::tbl(con, I("raw.crm_leads")) %>%
    dplyr::filter(id %in% person_ids) %>%
    dplyr::distinct(responsible_user_id) %>%
    dplyr::rename(user_id = responsible_user_id) %>%
    dplyr::collect()

  protocol_writers <- dplyr::tbl(con, I("raw.crm_lead_protocols")) %>%
    dplyr::left_join(dplyr::tbl(con, I("raw.crm_lead_protocol_relations")) %>% dplyr::select(protocol_id, lead_id), by = c("id" = "protocol_id")) %>%
    dplyr::filter(lead_id %in% person_ids) %>%
    dplyr::distinct(user_id) %>%
    dplyr::collect()

  # get user names
  user_names <- dplyr::tbl(con, I("raw.crm_users")) %>%
    dplyr::filter(id %in% responsible_users$user_id | id %in% protocol_writers$user_id) %>%
    dplyr::select(id, user_first_name, user_name) %>%
    dplyr::filter(!is.na(user_first_name) & !is.na(user_name)) %>%
    dplyr::filter(user_first_name != "Leadmanagement") %>%
    dplyr::collect()

  # --- Apply Replacements ---

  salesperson_tokens <- c()

  if(nrow(user_names) == 0) {
    return(tibble::tibble())
  }

  for(i in 1:nrow(user_names)) {
    salesperson <- tibble::tibble(
      id          = user_names$id[i],
      name        = paste(user_names$user_first_name[i], user_names$user_name[i]),
      token       = c(user_names$user_name[i], user_names$user_first_name[i], paste(user_names$user_first_name[i], user_names$user_name[i])),
      system      = "crm_user",
      type        = c("SURNAME", "FIRST_NAME", "FULL_NAME"),
      person_type = "Salesperson"
    ) %>%
      dplyr::filter(!is.na(token), token != "")
    salesperson_tokens <- dplyr::bind_rows(salesperson_tokens, salesperson)
  }

  return(salesperson_tokens)
}

################################################################################
# Utility Functions
################################################################################

#' Merges speaker tags (< v Name >) into single tokens
#'
#' @param df A dataframe with tokens
#' @param token_col The name of the token column
#' @return A dataframe with merged speaker tags
#' @keywords internal
merge_v_tags <- function(df, token_col = "token") {
  tokens <- df[[token_col]]

  out_tokens <- character()
  out_index  <- integer()

  inside <- FALSE
  buffer <- character()

  for (i in seq_along(tokens)) {
    tok <- tokens[i]

    if (!inside && tok == "<" && i < length(tokens) && tokens[i+1] == "v") {
      # Start einer zu mergenden Sequenz
      inside <- TRUE
      buffer <- c(buffer, tok)
    } else if (inside) {
      buffer <- c(buffer, tok)
      if (tok == ">") {
        # Alles zusammenfügen, aber mit Leerzeichen
        merged <- paste(buffer, collapse = " ")

        merged <- gsub("^< v", "<v", merged)
        merged <- gsub(" >", ">", merged)

        out_tokens <- c(out_tokens, merged)
        out_index  <- c(out_index, i)
        inside <- FALSE
        buffer <- character()
      }
    } else {
      out_tokens <- c(out_tokens, tok)
      out_index  <- c(out_index, i)
    }
  }

  df[out_index, token_col] <- out_tokens
  df[out_index, , drop = FALSE]
}


#' Safely extracts a column from a dataframe
#'
#' @param df A dataframe
#' @param col_name The name of the column to extract
#' @return The value of the column or NA if not found
#' @keywords internal
safe_extract <- function(df, col_name) {
  if (!is.null(df) && col_name %in% names(df) && nrow(df) > 0) {
    return(df[[col_name]][[1]])
  } else {
    return(NA_character_)
  }
}


#' Compresses and normalizes company names
#'
#' @param column A character vector with company names
#' @return A character vector with compressed company names
#' @keywords internal
compress_column <- function(column) {
  column <- gsub("\u00A0", " ", column, fixed = TRUE)
  column <- gsub("\u00AD", "", column, fixed = TRUE)
  column <- gsub(" \u2013 ", " ", column)
  column <- gsub(" \u2013", " ", column)
  column <- gsub("\u2013", " ", column)
  column <- gsub(" - ", " ", column, fixed = TRUE)
  column <- gsub(" -", " ", column, fixed = TRUE)
  column <- gsub("-", " ", column, fixed = TRUE)
  column <- gsub("®", "", column, fixed = TRUE)
  column <- gsub("\\*", "", column)
  column <- tolower(column)
  column <- stringr::str_remove_all(column, "[`]|[']|[']| [`]| [']| [']|[,]|[(]|[)]| [/]|[/]")
  column <- stringr::str_remove_all(column, " ag\\b| se\\b| gmbh|\\bohg\\b|\\bco\\b| \\+ co\\.| co\\.| kg\\b| deutschland| österreich| und\\b| für\\b| holding\\b| mbh\\b| eg\\b| ggmbh\\b| group\\b| ev\\b| der\\b| germany\\b| unilive| gesellschaft\\b| schweiz| gruppe| bv\\b")
  column <- gsub(" & ", " ", column, fixed = TRUE)
  column <- gsub(" &", " ", column, fixed = TRUE)
  column <- gsub("&", " ", column, fixed = TRUE)
  column <- gsub(" + ", "plussign", column, fixed = TRUE)
  column <- gsub(" +", "plussign", column, fixed = TRUE)
  column <- gsub("+", "plussign", column, fixed = TRUE)
  column <- gsub("  ", " ", column, fixed = TRUE)
  column <- gsub(" \\bgruppe\\b ", " ", column)
  column <- gsub(" \\bfirmengruppe\\b ", " ", column)
  column <- gsub(" \\bunternehmensgruppe\\b ", " ", column)
  column <- gsub("^gruppe ", "", column)
  column <- gsub("^firmengruppe ", "", column)
  column <- gsub("^unternehmensgruppe ", "", column)
  column <- gsub("stadtverwaltung", "stadt", column)
  column <- stringr::str_remove_all(column, " [.]|[.]")
  column <- trimws(column)
  column %>% stringr::str_replace_all("[äÄ]", "ae") %>%
    stringr::str_replace_all("[öÖ]", "oe") %>% stringr::str_replace_all("[üÜ]",
                                                                        "ue")
}


#' Finds a token and replaces it with a placeholder in the NER column
#'
#' @param tokens_df A dataframe with tokens
#' @param personal_data A character vector with personal data
#' @param replacement A character vector with replacements
#' @return A dataframe with replaced personal data
#' @keywords internal
find_and_replace_personal_data <- function(tokens_df, personal_data, replacement) {

  if (is.null(personal_data) || length(personal_data) == 0 || all(is.na(personal_data))) {
    return(tokens_df)
  }

  personal_data <- as.character(na.omit(personal_data))
  if (length(personal_data) == 0) return(tokens_df)

  # min 3 chars
  filtered_terms <- personal_data[nchar(personal_data) >= 3]
  if (length(filtered_terms) == 0) return(tokens_df)

  # fuzzy search with variable max_dist
  tokens <- purrr::map_dfr(filtered_terms, function(term) {

    # adaptive distance: here 30% of length, but at least 1, at most 3
    max_dist <- max(1, min(3, floor(nchar(term) * 0.3)))

    tokens_df %>%
      dplyr::filter(stringdist::stringdist(tolower(token), tolower(term), method = "lv") <= max_dist)
  }) %>%
    dplyr::distinct()

  tokens_df <- tokens_df %>%
    dplyr::mutate(
      NER = ifelse(
        CharacterOffsetBegin %in% tokens$CharacterOffsetBegin & !grepl("< ", NER),
        replacement,
        NER
      )
    )

  return(tokens_df)
}

################################################################################
# Unused Legacy Functions (kept for reference)
################################################################################

#' Gives anonymized IDs to LOCATION tokens using NER_LOCATION_%06d format
#'
#' @param con Database connection
#' @param tokens A dataframe with a `token` column and `NER` column
#' @return The updated tokens dataframe with NER values anonymized
#' @keywords internal
id_anonymize_locations <- function(con, tokens) {

  # Step 1: Get unique LOCATION tokens
  person_tokens <- unique(na.omit(tokens$token[tokens$NER == "LOCATION"]))

  if (length(person_tokens) == 0) {
    return(tokens)
  }

  # Step 2: Fetch existing mappings

  tokens_safe <- na.omit(person_tokens)
  tokens_safe <- tokens_safe[tokens_safe != ""]

  query <- glue::glue_sql("
  SELECT original, anonymized
  FROM processed.text_anonymizations
  WHERE original = ANY (ARRAY[{tokens_safe*}])
  ", tokens_safe = tokens_safe, .con = con)

  existing <- DBI::dbGetQuery(con, query)

  #get only number rows of table
  existing_rows <- dplyr::tbl(con, I("processed.text_anonymizations")) %>%
    dplyr::filter(grepl("LOCATION", anonymized)) %>%
    dplyr::summarise(n = dplyr::n()) %>%
    dplyr::pull(n) %>%
    as.integer()

  existing_tokens <- existing$original
  new_tokens <- setdiff(person_tokens, existing_tokens)

  # Step 3: Insert new tokens if needed
  if (length(new_tokens) > 0) {

    insert_data <- data.frame(
      original = new_tokens,
      anonymized = sprintf("NER_LOCATION_%06d", seq_len(length(new_tokens)) + existing_rows),
      stringsAsFactors = FALSE
    )

    DBI::dbWriteTable(
      con,
      DBI::Id(schema = "processed", table = "text_anonymizations"),
      insert_data,
      append = TRUE,
      row.names = FALSE
    )

    existing <- rbind(existing, insert_data)
  }


  # Step 4: Build lookup and update NER column
  person_lookup <- setNames(existing$anonymized, existing$original)

  tokens <- tokens %>%
    dplyr::mutate(NER = dplyr::if_else(NER == "LOCATION" & token %in% names(person_lookup), person_lookup[token], NER))

  return(tokens)
}


#' Gives anonymized IDs to ORGANIZATION tokens using NER_ORGANIZATION_%06d format
#'
#' @param con Database connection
#' @param tokens A dataframe with a `token` column and `NER` column
#' @return The updated tokens dataframe with NER values anonymized
#' @keywords internal
id_anonymize_organizations <- function(con, tokens) {

  # Step 1: Get unique ORGANIZATION tokens
  person_tokens <- unique(na.omit(tokens$token[tokens$NER == "ORGANIZATION"]))

  if (length(person_tokens) == 0) {
    return(tokens)
  }

  # Step 2: Fetch existing mappings

  tokens_safe <- na.omit(person_tokens)
  tokens_safe <- tokens_safe[tokens_safe != ""]

  query <- glue::glue_sql("
  SELECT original, anonymized
  FROM processed.text_anonymizations
  WHERE original = ANY (ARRAY[{tokens_safe*}])
  ", tokens_safe = tokens_safe, .con = con)

  existing <- DBI::dbGetQuery(con, query)

  #get only number rows of table
  existing_rows <- dplyr::tbl(con, I("processed.text_anonymizations")) %>%
    dplyr::filter(grepl("ORGANIZATION", anonymized)) %>%
    dplyr::summarise(n = dplyr::n()) %>%
    dplyr::pull(n) %>%
    as.integer()

  existing_tokens <- existing$original
  new_tokens <- setdiff(person_tokens, existing_tokens)

  # Step 3: Insert new tokens if needed
  if (length(new_tokens) > 0) {

    insert_data <- data.frame(
      original = new_tokens,
      anonymized = sprintf("NER_ORGANIZATION_%06d", seq_len(length(new_tokens)) + existing_rows),
      stringsAsFactors = FALSE
    )

    DBI::dbWriteTable(
      con,
      DBI::Id(schema = "processed", table = "text_anonymizations"),
      insert_data,
      append = TRUE,
      row.names = FALSE
    )

    existing <- rbind(existing, insert_data)
  }


  # Step 4: Build lookup and update NER column
  person_lookup <- setNames(existing$anonymized, existing$original)

  tokens <- tokens %>%
    dplyr::mutate(NER = dplyr::if_else(NER == "ORGANIZATION" & token %in% names(person_lookup), person_lookup[token], NER))

  return(tokens)
}
