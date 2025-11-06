################################################################################
# MS Graph Mail Update - Complete Mail Synchronization
#
# Functions for synchronizing mail data from MS Graph API to PostgreSQL
################################################################################

################################################################################
# Main Function
################################################################################

#' Update Mails from MS Graph API
#'
#' Main function to retrieve and process mail data from Microsoft Graph API
#' and store it in PostgreSQL database. Handles mail content, contacts,
#' senders, and recipients.
#'
#' This function:
#' - Retrieves mails from MS Graph for all internal users
#' - Converts HTML mail bodies to plaintext
#' - Updates mail, contact, sender, and recipient tables in database
#' - Implements retry logic for users with empty results
#'
#' @param con Database connection object (pool or DBI connection).
#'   Must have access to tables: raw.msgraph_users, raw.msgraph_mails,
#'   raw.msgraph_contacts, raw.msgraph_mail_recipients, raw.msgraph_mail_senders
#' @param msgraph_keys List containing MS Graph API credentials with elements:
#'   - tenant_id: Azure AD tenant ID
#'   - client_id: Azure AD client/application ID
#'   - client_secret: Azure AD client secret
#' @param start_date Optional. Date to start mail retrieval from.
#'   If NULL, automatically determined from last update in database.
#'   Format: Date object or character string in format "YYYY-MM-DD"
#' @param user_filter Optional. Character vector of user IDs to process.
#'   If NULL, all internal non-deleted users are processed.
#' @param max_retries Integer. Maximum number of retry attempts for users
#'   with empty results. Default: 3. Set to 1 to disable retries.
#'
#' @return Invisible NULL. Function is called for side effects (database updates).
#'
#' @examples
#' \dontrun{
#' # Basic usage with automatic start date
#' msgraph_update_mails(con, keys)
#'
#' # Specify custom start date
#' msgraph_update_mails(con, keys, start_date = "2025-01-01")
#'
#' # Process specific users only
#' msgraph_update_mails(con, keys, user_filter = c("user-id-1", "user-id-2"))
#'
#' # Disable retry logic
#' msgraph_update_mails(con, keys, max_retries = 1)
#' }
#'
#' @export
msgraph_update_mails <- function(con,
                                  msgraph_keys,
                                  start_date = NULL,
                                  user_filter = NULL,
                                  max_retries = 3) {

  # Load required helper functions
  source('func/support_functions.R')

  # 1. Get processable users
  message("Loading user list...")
  all_users <- get_processable_users(con)
  all_users_processable <- all_users

  # Apply user filter if provided
  if (!is.null(user_filter)) {
    all_users_processable <- all_users_processable %>%
      dplyr::filter(msgraph_user_id %in% user_filter)
    message(sprintf("Filtered to %d users", nrow(all_users_processable)))
  }

  # 2. Determine start date
  if (is.null(start_date)) {
    start_date <- get_mail_sync_start_date(con)
    message(sprintf("Start date determined from database: %s", start_date))
  } else {
    start_date <- as.Date(start_date)
    message(sprintf("Using provided start date: %s", start_date))
  }

  # 3. Retrieve mails with retry logic
  message(sprintf("Starting mail retrieval for %d users...", nrow(all_users_processable)))

  all_mail_of_all_accounts <- c()
  empty_mail_accounts <- 0
  all_downloaded <- FALSE
  retry_count <- 0

  while (!all_downloaded && retry_count < max_retries) {
    retry_count <- retry_count + 1

    if (retry_count > 1) {
      message(sprintf("Retry attempt %d/%d for users with empty results...",
                      retry_count - 1, max_retries - 1))
    }

    pb <- progress::progress_bar$new(
      format = "  downloading since :elapsedfull [:bar] :current/:total (:percent) eta: :eta",
      total = nrow(all_users_processable),
      clear = FALSE,
      width = 60
    )
    pb$tick(0)

    for(i in all_users_processable$msgraph_user_id) {

      # Retrieve mails for user
      all_records <- retrieve_all_mails_short(i, start_date, msgraph_keys = msgraph_keys)
      access_token <- all_records[[2]]

      # Convert to data frame
      all_mails <- convert_mail_list_to_dataframe(all_records[[1]], i)

      # Bind to overall collection
      all_mail_of_all_accounts <- dplyr::bind_rows(all_mail_of_all_accounts, all_mails)

      pb$tick()
    }

    message(sprintf("Retrieved %d mail records", nrow(all_mail_of_all_accounts)))

    # Filter out records without ID
    all_mail_of_all_accounts <- all_mail_of_all_accounts %>%
      dplyr::filter(!is.na(id))

    # Check for users with empty results
    temp <- as.data.frame(table(all_mail_of_all_accounts$user_id))

    user_temp <- all_users_processable %>%
      dplyr::select(display_name, user_principal_name, msgraph_user_id) %>%
      dplyr::mutate(msgraph_user_id = as.character(msgraph_user_id)) %>%
      dplyr::left_join(temp %>% dplyr::mutate(msgraph_user_id = as.character(Var1)), by = "msgraph_user_id") %>%
      dplyr::filter(is.na(Freq) | Freq == 0)

    # Check if we should stop or continue
    if(nrow(user_temp) == empty_mail_accounts) {
      all_downloaded <- TRUE
      if (nrow(user_temp) > 0) {
        message(sprintf("%d users still have no results after retries", nrow(user_temp)))
      }
    } else {
      empty_mail_accounts <- nrow(user_temp)

      # Prepare next iteration with users that had results + empty users
      all_users_processable <- all_users_processable %>%
        dplyr::select(display_name, user_principal_name, msgraph_user_id) %>%
        dplyr::mutate(msgraph_user_id = as.character(msgraph_user_id)) %>%
        dplyr::left_join(temp %>% dplyr::mutate(msgraph_user_id = as.character(Var1)), by = "msgraph_user_id") %>%
        dplyr::filter(Freq != 0) %>%
        dplyr::slice_tail(n = 1) %>%
        dplyr::bind_rows(user_temp)

      message(sprintf("%d users to retry in next iteration", nrow(user_temp)))
    }
  }

  # 4. Check if we have mails to process
  if(length(all_mail_of_all_accounts) <= 1) {
    message("No mails to process. Exiting.")
    return(invisible(NULL))
  }

  # 5. Process and export mails
  message("Processing mail content and exporting to database...")
  new_mails <- upsert_mails(con, all_mail_of_all_accounts, all_users)
  message(sprintf("Upserted %d mail records", nrow(new_mails)))

  # 6. Parse senders
  message("Processing sender information...")
  mails_from_export <- all_mail_of_all_accounts %>%
    dplyr::distinct() %>%
    dplyr::mutate(from = convert_to_list(from)) %>%
    tidyr::unnest_wider(from, names_sep = "_") %>%
    tidyr::unnest_wider(from_emailAddress, names_sep = "_") %>%
    dplyr::mutate(from_emailAddress_address = tolower(from_emailAddress_address)) %>%
    dplyr::distinct(msgraph_id = id, email = from_emailAddress_address, name = from_emailAddress_name)

  # 7. Parse recipients
  message("Processing recipient information...")
  mails_to_export <- all_mail_of_all_accounts %>%
    dplyr::distinct(id, toRecipients, ccRecipients, bccRecipients) %>%
    dplyr::mutate(toRecipients = convert_to_list(toRecipients)) %>%
    dplyr::mutate(ccRecipients = convert_to_list(ccRecipients)) %>%
    dplyr::mutate(bccRecipients = convert_to_list(bccRecipients))

  # Parse all recipient types
  recipients_to <- Map(parse_mail_recipients,
                       recipient_list = mails_to_export$toRecipients,
                       mail_id = mails_to_export$id,
                       MoreArgs = list(recipient_type = "to"))
  recipients_to_df <- do.call(rbind, recipients_to) %>%
    dplyr::filter(!is.na(mail_recipient)) %>%
    dplyr::filter(grepl("\\@", mail_recipient))

  recipients_cc <- Map(parse_mail_recipients,
                       recipient_list = mails_to_export$ccRecipients,
                       mail_id = mails_to_export$id,
                       MoreArgs = list(recipient_type = "cc"))
  recipients_cc_df <- do.call(rbind, recipients_cc) %>%
    dplyr::filter(!is.na(mail_recipient)) %>%
    dplyr::filter(grepl("\\@", mail_recipient))

  recipients_bcc <- Map(parse_mail_recipients,
                        recipient_list = mails_to_export$bccRecipients,
                        mail_id = mails_to_export$id,
                        MoreArgs = list(recipient_type = "bcc"))
  recipients_bcc_df <- do.call(rbind, recipients_bcc) %>%
    dplyr::filter(!is.na(mail_recipient)) %>%
    dplyr::filter(grepl("\\@", mail_recipient))

  # Combine all recipients
  mails_to_export <- dplyr::bind_rows(recipients_to_df, recipients_cc_df, recipients_bcc_df) %>%
    dplyr::select(msgraph_id = id, email = mail_recipient, name = name_recipient, recipient_type) %>%
    dplyr::distinct(msgraph_id, email, name, recipient_type)

  # 8. Upsert contacts
  message("Updating contacts...")
  upsert_mail_contacts(con, mails_from_export, mails_to_export)

  # 9. Upsert recipients
  message("Updating recipient relationships...")
  upsert_mail_recipients(con, mails_to_export, new_mails)

  # 10. Upsert senders
  message("Updating sender relationships...")
  upsert_mail_senders(con, mails_from_export, new_mails)

  message("Mail synchronization completed successfully!")

  invisible(NULL)
}

################################################################################
# Helper Functions - Data Transformation
################################################################################

#' Convert Character to List
#'
#' Helper function to convert character strings to lists using eval/parse.
#' Used for parsing nested list structures in API responses.
#'
#' @param char_column Character vector to convert
#'
#' @return List
#' @keywords internal
convert_to_list <- function(char_column) {
  lapply(char_column, function(x) eval(parse(text = x)))
}


#' Remove Bidirectional Unicode Characters
#'
#' Returns pattern for problematic bidirectional Unicode characters.
#'
#' @return Regular expression pattern for bidirectional characters
#' @keywords internal
get_bidi_pattern <- function() {
  "[\u202a-\u202e\u2066-\u2069]"
}


#' Convert Mail List to DataFrame
#'
#' Converts the mail list response from MS Graph API into a normalized data frame.
#' Handles both single and multiple mail records with proper column structure.
#'
#' @param mail_records List of mail records from API response
#' @param user_id Character string with the user's MS Graph ID
#'
#' @return Data frame with normalized mail data
convert_mail_list_to_dataframe <- function(mail_records, user_id) {
  # Convert list to matrix
  all_mails <- t(sapply(mail_records, function(x) if(is.null(x)) NA else x))

  # Handle different shapes of data
  if(nrow(all_mails) > 1 | length(mail_records) <= 1) {
    all_mails <- all_mails %>%
      as.data.frame() %>%
      dplyr::mutate_all(as.character) %>%
      dplyr::mutate(user_id = user_id) %>%
      dplyr::select(-tidyselect::any_of(c("@odata.etag")))
  } else {
    all_mails <- all_mails %>%
      t() %>%
      as.data.frame() %>%
      tidyr::unnest_wider(V1) %>%
      dplyr::mutate_all(as.character) %>%
      dplyr::mutate(user_id = user_id) %>%
      dplyr::select(-tidyselect::any_of(c("@odata.etag")))
  }

  return(all_mails)
}


#' Extract Plaintext from HTML
#'
#' Converts HTML email body to plaintext while preserving basic formatting.
#' Removes HTML tags, converts common entities, and handles line breaks.
#'
#' @param body Character string containing HTML email body
#'
#' @return Character string with plaintext content
extract_plaintext_from_html <- function(body) {
  body %>%
    # Remove escape characters
    stringr::str_replace_all('\\\\r|\\\\n', '') %>%
    stringr::str_replace_all('\\\\\"', '"') %>%

    # Extract content between <body> and </body>
    stringr::str_extract("(?i)<body[^>]*>(.*?)</body>") %>%

    # Replace <br> and <p> with line breaks
    stringr::str_replace_all("(?i)<br\\s*/?>", "\n") %>%
    stringr::str_replace_all("(?i)<p[^>]*>", "\n\n") %>%
    stringr::str_replace_all("(?i)</p>", "\n\n") %>%
    stringr::str_replace_all("(?i)<div[^>]*>", "\n\n") %>%
    stringr::str_replace_all("(?i)</div>", "\n\n") %>%

    # Remove all other HTML tags
    stringr::str_replace_all("<[^>]+>", "") %>%

    # Replace HTML entities
    stringr::str_replace_all("&nbsp;", " ") %>%
    stringr::str_replace_all("&amp;", "&") %>%
    stringr::str_replace_all("&uuml;", "ü") %>%
    stringr::str_replace_all("&auml;", "ä") %>%
    stringr::str_replace_all("&ouml;", "ö") %>%
    stringr::str_replace_all("&szlig;", "ß") %>%

    # Replace multiple spaces with single line break
    stringr::str_replace_all("(?<=\\S)\\s{2,}", "\n") %>%

    # Trim leading and trailing whitespace
    stringr::str_trim()
}


#' Parse Mail Recipients
#'
#' Extracts recipient information (email and name) from the recipient list structure.
#' Handles toRecipients, ccRecipients, and bccRecipients fields.
#'
#' @param recipient_list List containing recipient data from API response
#' @param recipient_type Character string: "to", "cc", or "bcc"
#' @param mail_id Character string with the mail's MS Graph ID
#'
#' @return Data frame with columns: id, mail_recipient, name_recipient, recipient_type
parse_mail_recipients <- function(recipient_list, recipient_type, mail_id) {
  if (is.null(recipient_list) || length(recipient_list) == 0) {
    return(data.frame(
      id = as.character(mail_id),
      mail_recipient = NA_character_,
      name_recipient = NA_character_,
      recipient_type = as.character(recipient_type),
      stringsAsFactors = FALSE
    ))
  }

  recipients <- lapply(recipient_list, function(x) {
    # Check: is x a list and does it contain emailAddress?
    if (!is.list(x) || is.null(x$emailAddress)) return(NULL)

    email <- tryCatch(x$emailAddress$address, error = function(e) NA_character_)
    name <- tryCatch(x$emailAddress$name, error = function(e) NA_character_)

    if (length(email) == 0 && length(name) == 0) return(NULL)
    if (length(email) == 0) email <- name
    if (length(name) == 0) name <- email

    data.frame(
      id = as.character(mail_id),
      mail_recipient = as.character(email),
      name_recipient = as.character(name),
      recipient_type = as.character(recipient_type),
      stringsAsFactors = FALSE
    )
  })

  result <- do.call(rbind, recipients)
  if (is.null(result)) {
    return(data.frame(
      id = as.character(mail_id),
      mail_recipient = NA_character_,
      name_recipient = NA_character_,
      recipient_type = as.character(recipient_type),
      stringsAsFactors = FALSE
    ))
  }

  return(result)
}


#' Normalize External Email Address
#'
#' Converts external email addresses with #EXT# notation to standard format.
#' Example: john_doe_gmail.com#EXT#@tenant.onmicrosoft.com -> john.doe@gmail.com
#'
#' @param email Character string with email address
#'
#' @return Character string with normalized email address
normalize_external_email <- function(email) {
  if (grepl("#ext#", email, ignore.case = TRUE)) {
    # Cut off from '#EXT#' onward, then replace first underscore with '@'
    email <- sub("(?i)#ext#.*", "", email)
    email <- sub("_", "@", email, fixed = TRUE)
  }
  return(email)
}


################################################################################
# Database Operations
################################################################################

#' Get Mail Sync Start Date
#'
#' Determines the start date for mail synchronization by finding the most recent
#' update date in the database. Falls back to a default date if table is empty.
#'
#' @param con Database connection object (pool or DBI connection)
#' @param default_date Date to use if database table is empty (default: 2025-01-01)
#'
#' @return Date object representing the start date for sync
get_mail_sync_start_date <- function(con, default_date = lubridate::ymd("2025-01-01")) {
  start_date <- dplyr::tbl(con, I("raw.msgraph_mails")) %>%
    dplyr::summarise(max_date = max(updated_at, na.rm = TRUE)) %>%
    dplyr::collect() %>%
    dplyr::pull(max_date)

  # If db-table is empty, use default date
  start_date <- dplyr::coalesce(as.Date(start_date - lubridate::days(1)), default_date)

  return(start_date)
}


#' Get Processable Users
#'
#' Retrieves all internal, non-deleted users from the database.
#'
#' @param con Database connection object (pool or DBI connection)
#'
#' @return Data frame with user information
get_processable_users <- function(con) {
  users <- dplyr::tbl(con, I("raw.msgraph_users")) %>%
    dplyr::filter(is_internal) %>%
    dplyr::filter(!is_deleted) %>%
    dplyr::select(-is_internal, -is_deleted) %>%
    dplyr::collect()

  return(users)
}


#' Upsert Mails to Database
#'
#' Inserts or updates mail records in the database.
#' Converts HTML body to plaintext and formats data for database schema.
#'
#' @param con Database connection object (pool or DBI connection)
#' @param mail_data Data frame with mail data from API
#' @param all_users Data frame with user information for ID mapping
#'
#' @return Data frame with new mail IDs (columns: id, msgraph_id)
upsert_mails <- function(con, mail_data, all_users) {
  # Remove bidirectional unicode characters
  bidi_chars <- get_bidi_pattern()

  # Prepare export data
  msgraph_mails_export <- mail_data %>%
    dplyr::mutate(
      body = stringr::str_remove_all(body, bidi_chars),
      content = purrr::map_chr(body, extract_plaintext_from_html),
      content_type = "plaintext"
    ) %>%
    dplyr::mutate(sentDateTime = lubridate::ymd_hms(sentDateTime)) %>%
    dplyr::select(msgraph_id = id, user_id_matching = user_id, sent_datetime = sentDateTime, subject, content, content_type) %>%
    dplyr::left_join(all_users %>% dplyr::select(user_id = id, msgraph_user_id), by = c("user_id_matching" = "msgraph_user_id")) %>%
    dplyr::select(-user_id_matching) %>%
    tidyr::drop_na(sent_datetime) %>%
    dplyr::distinct()

  # Upsert to database
  new_mails <- Billomatics::postgres_upsert_data(
    con = con,
    schema = "raw",
    table = "msgraph_mails",
    data = msgraph_mails_export,
    match_cols = c("msgraph_id"),
    delete_missing = FALSE,
    returning_cols = c("id", "msgraph_id")
  )

  return(new_mails)
}


#' Upsert Mail Contacts to Database
#'
#' Inserts or updates contact records (email addresses) in the database.
#' Handles both senders and recipients, normalizes external email addresses.
#'
#' @param con Database connection object (pool or DBI connection)
#' @param senders_data Data frame with sender information (columns: email, name)
#' @param recipients_data Data frame with recipient information (columns: email, name)
#'
#' @return Invisible NULL (function called for side effects)
upsert_mail_contacts <- function(con, senders_data, recipients_data) {
  # Get existing contacts
  contacts <- dplyr::tbl(con, I("raw.msgraph_contacts")) %>%
    dplyr::select(id, ms_name, email) %>%
    dplyr::collect()

  # Combine senders and recipients
  mail_contacts_export <- dplyr::bind_rows(
    recipients_data %>% dplyr::distinct(email, name),
    senders_data %>% dplyr::distinct(email, name)
  ) %>%
    dplyr::mutate(email = tolower(email)) %>%
    dplyr::filter(!(email %in% contacts$email)) %>%
    dplyr::select(email) %>%
    tidyr::drop_na(email) %>%
    dplyr::distinct(email) %>%
    dplyr::select(-tidyselect::any_of("id")) %>%
    dplyr::mutate(email = normalize_external_email(email)) %>%
    dplyr::distinct()

  # Only upsert if there are new contacts
  if(nrow(mail_contacts_export) > 0) {
    Billomatics::postgres_upsert_data(
      con = con,
      schema = "raw",
      table = "msgraph_contacts",
      data = mail_contacts_export,
      match_cols = c("email"),
      delete_missing = FALSE
    )
  }

  invisible(NULL)
}


#' Upsert Mail Recipients to Database
#'
#' Inserts or updates recipient relationships (mail-to-contact mappings) in the database.
#' Includes recipient type (to, cc, bcc).
#'
#' @param con Database connection object (pool or DBI connection)
#' @param recipients_data Data frame with recipient information
#' @param new_mails Data frame with mail IDs from database
#'
#' @return Invisible NULL (function called for side effects)
upsert_mail_recipients <- function(con, recipients_data, new_mails) {
  # Get updated contact list with IDs
  new_contacts <- dplyr::tbl(con, I("raw.msgraph_contacts")) %>%
    dplyr::select(id, email) %>%
    dplyr::collect()

  # Prepare recipient export
  mails_to_export <- recipients_data %>%
    dplyr::distinct() %>%
    dplyr::left_join(new_contacts, by = c("email" = "email")) %>%
    dplyr::select(msgraph_id, contact_id = id, recipient_type) %>%
    dplyr::mutate(contact_id = as.integer(contact_id)) %>%
    dplyr::distinct() %>%
    dplyr::left_join(new_mails, by = c("msgraph_id" = "msgraph_id")) %>%
    dplyr::rename(email_id = id) %>%
    dplyr::distinct(email_id, contact_id, recipient_type) %>%
    # Secure that we only add an entry if we have a contact_id and an email_id
    tidyr::drop_na(email_id) %>%
    tidyr::drop_na(contact_id) %>%
    tidyr::drop_na(recipient_type)

  # Upsert to database
  Billomatics::postgres_upsert_data(
    con = con,
    schema = "raw",
    table = "msgraph_mail_recipients",
    data = mails_to_export,
    match_cols = c("email_id", "contact_id", "recipient_type"),
    delete_missing = FALSE
  )

  invisible(NULL)
}


#' Upsert Mail Senders to Database
#'
#' Inserts or updates sender relationships (mail-to-contact mappings) in the database.
#'
#' @param con Database connection object (pool or DBI connection)
#' @param senders_data Data frame with sender information
#' @param new_mails Data frame with mail IDs from database
#'
#' @return Invisible NULL (function called for side effects)
upsert_mail_senders <- function(con, senders_data, new_mails) {
  # Get updated contact list with IDs
  new_contacts <- dplyr::tbl(con, I("raw.msgraph_contacts")) %>%
    dplyr::select(id, email) %>%
    dplyr::collect()

  # Prepare sender export
  mails_from_export <- senders_data %>%
    dplyr::distinct() %>%
    dplyr::left_join(new_contacts, by = c("email" = "email")) %>%
    dplyr::select(msgraph_id, contact_id = id) %>%
    dplyr::mutate(contact_id = as.integer(contact_id)) %>%
    dplyr::distinct() %>%
    dplyr::left_join(new_mails, by = c("msgraph_id" = "msgraph_id")) %>%
    dplyr::rename(email_id = id) %>%
    dplyr::distinct(email_id, contact_id) %>%
    # Secure that we only add an entry if we have a contact_id and an email_id
    tidyr::drop_na(email_id) %>%
    tidyr::drop_na(contact_id)

  # Upsert to database
  Billomatics::postgres_upsert_data(
    con = con,
    schema = "raw",
    table = "msgraph_mail_senders",
    data = mails_from_export,
    match_cols = c("email_id", "contact_id"),
    delete_missing = FALSE
  )

  invisible(NULL)
}

retrieve_all_mails_short <- function(user, startDate, msgraph_keys) {

  access_token <- re_authentication(tenant_id = msgraph_keys$tenant_id,
                                    client_id = msgraph_keys$client_id,
                                    client_secret = msgraph_keys$client_secret)

  endDate <- lubridate::today() + 1

  call_records_url <- paste0("https://graph.microsoft.com/v1.0/users/", user, "/messages")

  query <- list(
  `$filter` = paste0("receivedDateTime ge ", startDate, "T00:00:00Z", " and ", "receivedDateTime le ", endDate, "T00:00:00Z"),
  `$select` = "id,sentDateTime,sender,from,toRecipients,ccRecipients,bccRecipients,subject,body",
  `$top` = 100
  )

  all_call_records <- list()  # Initialize an empty list to store all results

  repeat {

    call_records <- fetch_with_retry(call_records_url, access_token, query)

    # Append the current page of results to the list
    if (!is.null(call_records$value)) {
      all_call_records <- c(all_call_records, call_records$value)
    }

    # Check if there's a next page
    if (!is.null(call_records$`@odata.nextLink`)) {
      access_token <- re_authentication(tenant_id = msgraph_keys$tenant_id,
                                    client_id = msgraph_keys$client_id,
                                    client_secret = msgraph_keys$client_secret)
      call_records_url <- call_records$`@odata.nextLink`
    } else {
      break  # Exit the loop if there are no more pages
    }
  }

  return(list(all_call_records, access_token))
}