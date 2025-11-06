################################################################################-
# ----- Description -------------------------------------------------------------
#
# This script defines the main function for updating CRM lead emails in the background.
# It extracts, cleans, and matches CRM lead email addresses with MS Graph contacts,
# and updates the relevant tables in the database.
#
################################################################################-
# ----- Start -------------------------------------------------------------------

# Function to extract email addresses from a text block
extract_emails <- function(text) {
    # Regex to extract valid crm_lead_id email addresses, including those with " - kein NL gewünscht"
    emails <- stringr::str_extract_all(text, "[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+[.][a-zA-Z]{2,}( - kein NL gewünscht)?") %>% unlist()
}

#' Update CRM Lead Mails in Background
#'
#' This function processes CRM lead email addresses, extracting, cleaning, and matching them with MS Graph contacts.
#'
#' @param con A database connection object.
#'
#' @returns No return value. Updates relevant tables in the database.
#'
#' @export
#' @examples
#' crm_update_lead_mails_background(con)
crm_update_lead_mails_background <- function(con) {
  
  persons <- dplyr::tbl(con, I("raw.crm_leads")) %>% 
    dplyr::filter(!is_deleted) %>%
    dplyr::select(id, crm_lead_id, salutation, lead_first_name, lead_name, lead_background) %>%
    dplyr::left_join(
      dplyr::tbl(con, I("raw.crm_lead_mail_address")) %>%
      dplyr::filter(!is_deleted) %>% 
      dplyr::select(lead_id, mail_address_type, mail_address_name) %>%
      dplyr::filter(mail_address_type %in% c("office", "office_hq")), 
      by = c("id" = "lead_id")
    ) %>%
    dplyr::select(-id) %>%
    dplyr::distinct() %>%
    dplyr::collect()
  
  ################################################################################-
  # ----- Start -------------------------------------------------------------------

  ## ----- get all Mails and contact data -----

  mails_from_description <- persons %>% 
    dplyr::select(crm_lead_id, lead_background) %>% 
    dplyr::mutate(emails = sapply(lead_background, extract_emails)) %>% 
    dplyr::distinct() %>% 
    dplyr::filter(!grepl("kein NL gewünscht", emails)) %>% 
    dplyr::mutate(nr_emails = sapply(emails, length)) %>% 
    dplyr::filter(nr_emails != 0) %>% 
    tidyr::drop_na() %>% 
    tidyr::unnest(emails) %>% 
    dplyr::select(crm_lead_id, mail_address_name = emails)

  mails_leads <- persons %>%
    dplyr::select(-lead_background) %>% 
    dplyr::mutate(
      lead_first_name = if_else(mail_address_type == "office", lead_first_name, NA_character_),
      lead_first_name = if_else(lead_first_name == "", NA_character_, lead_first_name),
      lead_name = if_else(mail_address_type == "office", lead_name, NA_character_),
      lead_name = if_else(lead_name == "", NA_character_, lead_name)
    )

  personal_mails_structured <- mails_leads %>% 
    dplyr::filter(!(is.na(lead_first_name) | is.na(lead_name)))

  keywords <- c("^hr\\.", "\\.hr$", "personal", "bewerb", "job", 
        "bildung", "corporate", "recruiting", "verwaltung", 
        "communication", "account", "abteilung", "info", 
        "germany", "austria", "human", "\\.at$", "\\.de$", 
        "^at\\.", "^de\\.", "[0-9]", "administration", "anfragen", 
        "application", "aquilinavictoria", "^aramis\\.", "^arcus\\.", 
        "^bad\\.", "bau", "besime", "betuel", "^bfs\\.", "campus", "campusteam", 
        "contact", "kontakt", "culture", "dajen", "de-fdcd", "de-flcn", "sekret", "^dial\\.", "direktion", "regensburg",
        "arbeit", "career", "presse", "branding", "employer", "nachwuchs", "office", "recruit", "gmbH", "deutschland", 
        "dialog", "talent", "berlin", "nagold", "service", "talent", "dach", "medien", "vorname", "empfang", "redaktion", 
        "^hrm\\.", "leipzig", "extern", "proteam", "karriere", "chance")

  pattern <- paste(keywords, collapse = "|")

  persons_new_match <- mails_leads %>% 
    dplyr::filter(is.na(lead_first_name) | is.na(lead_name)) %>% 
    dplyr::bind_rows(mails_from_description) %>% 
    dplyr::mutate(mail_address_name_compressed = gsub("@\\S+", "", mail_address_name)) %>%
    dplyr::mutate(mail_address_name_compressed = tolower(mail_address_name_compressed)) %>% 
    dplyr::distinct(mail_address_name, mail_address_name_compressed, crm_lead_id, mail_address_type, salutation, lead_first_name, lead_name)

  not_matchable_mails <- persons_new_match %>% 
    dplyr::filter(!grepl("\\.", mail_address_name_compressed) | grepl(pattern, mail_address_name_compressed)) %>%
    dplyr::mutate(personal_mail = if_else(mail_address_type == "office", TRUE, NA)) %>% 
    dplyr::mutate(personal_mail = if_else(is.na(personal_mail), FALSE, personal_mail)) %>%
    dplyr::mutate(salutation = if_else(mail_address_type == "office", salutation, NA_character_)) %>% 
    dplyr::select(-mail_address_name_compressed)

  new_personal_mails <- persons_new_match %>% 
    dplyr::select(-lead_first_name, -lead_name, -salutation) %>% 
    dplyr::filter(grepl("\\.", mail_address_name_compressed)) %>% 
    dplyr::filter(!grepl(pattern, mail_address_name_compressed)) %>% 
    tidyr::separate(mail_address_name_compressed, c("lead_first_name", "lead_name"), "\\.") %>%
    dplyr::mutate(lead_first_name = if_else(nchar(lead_first_name) < 2, toupper(lead_first_name), tools::toTitleCase(lead_first_name))) %>% 
    dplyr::mutate(lead_name = if_else(nchar(lead_name) < 2, toupper(lead_name), tools::toTitleCase(lead_name)))

  all_personal_mails <- new_personal_mails %>% 
    dplyr::bind_rows(personal_mails_structured) %>% 
    dplyr::mutate(lead_first_name_abbreviated = nchar(lead_first_name) < 3) %>% 
    dplyr::mutate(name_abbreviated = nchar(lead_name) < 3) %>% 
    dplyr::mutate(
      lead_first_name = trimws(gsub("\\([^)]*\\)", "", lead_first_name), which = "right"),
      lead_name = trimws(gsub("\\([^)]*\\)", "", lead_name), which = "right")
    ) %>%
    dplyr::mutate(personal_mail = TRUE)

  all_mails <- all_personal_mails %>%
    dplyr::bind_rows(not_matchable_mails) %>%
    dplyr::mutate(mail_address_type = dplyr::coalesce(mail_address_type, "from_description")) %>%
    dplyr::rename(email_source_crm = mail_address_type) %>%
    dplyr::distinct() %>%
    dplyr::group_by(crm_lead_id) %>%
    dplyr::mutate(
      lead_has_office = any(email_source_crm %in% c("office")),
      lead_has_office_hq = any(email_source_crm %in% c("office_hq"))
    ) %>% 
    dplyr::ungroup() %>% 
    dplyr::mutate(is_primary = dplyr::case_when(
      email_source_crm == "office" ~ TRUE,
      email_source_crm == "office_hq" & !lead_has_office ~ TRUE,
      email_source_crm == "from_description" & !lead_has_office & !lead_has_office_hq ~ TRUE,
      TRUE ~ FALSE
    ))

  data <- all_mails %>%
    Billomatics::replace_external_ids_with_internal(
      "crm_lead_id", 
      dplyr::tbl(con, I("raw.crm_leads")) %>% dplyr::select("id", "crm_lead_id") %>% dplyr::collect(), 
      "crm_lead_id", 
      "lead_id"
    ) %>% 
    tidyr::drop_na(mail_address_name) %>%
    dplyr::select(
      lead_id, 
      mail_address_type = email_source_crm, 
      mail_address_name, 
      associated_person_first_name = lead_first_name, 
      associated_person_name = lead_name, 
      first_name_abbreviated = lead_first_name_abbreviated, 
      name_abbreviated, 
      is_primary
    ) %>%
    dplyr::distinct(lead_id, mail_address_name, .keep_all = TRUE)
    Billomatics::postgres_upsert_data(con, "raw", "crm_lead_mail_address", data, match_cols = c("lead_id", "mail_address_name"), delete_missing = TRUE)

  ### Match with msgraph_contacts ----

  msgraph_contacts <- dplyr::tbl(con, I("raw.msgraph_contacts")) %>% 
    dplyr::collect() %>% 
    dplyr::select(msgraph_contact_id = id, email_normalized) %>%
    dplyr::mutate(
      compact_mail = email_normalized %>% tolower() %>%
      sub("\\+.*(?=@)", "", ., perl = TRUE) %>%
      gsub("@([a-z0-9\\-]+)\\.[a-z]{2,}$", "@\\1", ., perl = TRUE) %>%
      gsub("\\+", "", .) %>%
      gsub("\\.", "", .) %>%
      gsub("-", "", .) %>%
      gsub("_", "", .) %>%
      gsub("@onmicrosoft\\.com$", "@", .) %>%
      gsub("@$", "@microsoft.com", .) %>%
      stringi::stri_trans_general(., "Latin-ASCII")
    ) %>% 
    dplyr::select(msgraph_contact_id, compact_mail)

  all_mails_compact <- all_mails %>% 
    dplyr::mutate(mail_address_name = tolower(mail_address_name)) %>%
    tidyr::drop_na(mail_address_name) %>%
    dplyr::mutate(compact_mail = mail_address_name %>% tolower() %>% 
      sub("\\+.*(?=@)", "", ., perl = TRUE) %>%
      gsub("@([a-z0-9\\-]+)\\.[a-z]{2,}$", "@\\1", ., perl = TRUE) %>%
      gsub("\\+", "", .) %>%
      gsub("\\.", "", .) %>%
      gsub("-", "", .) %>%
      gsub("_", "", .) %>%
      gsub("@onmicrosoft\\.com$", "@", .) %>%
      gsub("@$", "@microsoft.com", .) %>%
      stringi::stri_trans_general(., "Latin-ASCII")) %>%
    dplyr::group_by(mail_address_name) %>%
    dplyr::mutate(
      has_office_full_mail = any(email_source_crm %in% c("office")),
      has_office_hq_full_mail = any(email_source_crm %in% c("office_hq"))
    ) %>% 
    dplyr::ungroup() %>% 
    dplyr::filter(!(has_office_full_mail & email_source_crm != "office")) %>%
    dplyr::filter(!(has_office_hq_full_mail & email_source_crm == "from_description"))

  all_mails_office <- all_mails_compact %>%
    dplyr::filter(email_source_crm == "office") %>%
    dplyr::mutate(has_office = TRUE)

  all_mails_office_hq <- all_mails_compact %>%
    dplyr::filter(email_source_crm == "office_hq") %>%
    dplyr::left_join(
      all_mails_office %>% dplyr::select(has_office, compact_mail, lead_id_overwrite = crm_lead_id), 
      by = "compact_mail"
    ) %>% 
    dplyr::mutate(has_office = dplyr::coalesce(has_office, FALSE)) %>%
    dplyr::mutate(crm_lead_id = if_else(has_office, lead_id_overwrite, crm_lead_id)) %>%
    dplyr::mutate(email_source_crm = if_else(has_office, "office", email_source_crm)) %>% 
    dplyr::mutate(has_office_hq = if_else(has_office, FALSE, TRUE)) %>% 
    dplyr::mutate(matched_through_compact_mail = has_office)

  all_mails_description <- all_mails_compact %>%
    dplyr::filter(email_source_crm == "from_description") %>%
    dplyr::left_join(
      all_mails_office %>% dplyr::select(has_office, compact_mail, lead_id_overwrite = crm_lead_id), 
      by = "compact_mail"
    ) %>% 
    dplyr::mutate(has_office = dplyr::coalesce(has_office, FALSE)) %>%
    dplyr::left_join(
      all_mails_office_hq %>% dplyr::filter(has_office_hq) %>% dplyr::select(has_office_hq, compact_mail, lead_id_overwrite_hq = crm_lead_id), 
      by = "compact_mail"
    ) %>% 
    dplyr::mutate(has_office_hq = dplyr::coalesce(has_office_hq, FALSE)) %>%
    dplyr::mutate(crm_lead_id = if_else(has_office, lead_id_overwrite, crm_lead_id)) %>%
    dplyr::mutate(email_source_crm = if_else(has_office, "office", email_source_crm)) %>% 
    dplyr::mutate(crm_lead_id = if_else(is.na(has_office) & has_office_hq, lead_id_overwrite_hq, crm_lead_id)) %>%
    dplyr::mutate(email_source_crm = if_else(is.na(has_office) & has_office_hq, "office_hq", email_source_crm)) %>% 
    dplyr::mutate(matched_through_compact_mail = has_office | has_office_hq)

  all_mails <- all_mails_office %>%
    dplyr::bind_rows(all_mails_office_hq) %>%
    dplyr::bind_rows(all_mails_description) %>% 
    dplyr::mutate(matched_through_compact_mail = dplyr::coalesce(matched_through_compact_mail, FALSE)) %>% 
    dplyr::select(-lead_id_overwrite, -lead_id_overwrite_hq, -has_office, -has_office_hq, -has_office_full_mail, -has_office_hq_full_mail, -lead_first_name, -lead_name, -salutation)

  matched_with_msgraph <- all_mails %>% 
    dplyr::left_join(msgraph_contacts, by = "compact_mail", relationship = "many-to-many") %>% 
    tidyr::drop_na(msgraph_contact_id) %>% 
    Billomatics::replace_external_ids_with_internal(
      "crm_lead_id", 
      dplyr::tbl(con, I("raw.crm_leads")) %>% dplyr::select("id", "crm_lead_id") %>% dplyr::collect(), 
      "crm_lead_id"
    ) %>% 
    dplyr::select(msgraph_contact_id, crm_lead_id, email_source_crm, is_primary_crm = is_primary) %>% 
    dplyr::distinct(msgraph_contact_id, crm_lead_id, .keep_all = TRUE)

  Billomatics::postgres_upsert_data(con, "mapping", "crm_lead_msgraph_contact", matched_with_msgraph, match_cols = c("msgraph_contact_id", "crm_lead_id"), delete_missing = TRUE)

}