#' Upsert CRM Companies and Related Data
#'
#' Downloads and upserts company data and related entities from Central Station CRM.
#'
#' @param con A DBI database connection.
#' @param crm_key Authentication key for Central Station CRM.
#' @param all_users Data frame of all CRM users.
#' @param all_leads Data frame of all CRM leads.
#' @param positions Data frame of lead positions.
#'
#' @return Invisibly returns TRUE after successful upsert.
#' @export
crm_update_companies <- function(
  con,
  crm_key,
  all_users,
  all_leads,
  positions
) {
  companies <- download_and_enrich_companies(crm_key)

  # Upsert Companies
  data <- companies$main_table %>%
    resolve_user_ids(
      all_users,
      cols = c(
        "created_by_user_id",
        "updated_by_user_id",
        "responsible_user_id"
      )
    )
  upsert_delete_missing(
    con,
    "raw.crm_companies",
    data,
    match_cols = c("crm_company_id")
  )
  all_companies <- dplyr::tbl(con, I("raw.crm_companies")) %>%
    dplyr::filter(!is_deleted) %>%
    dplyr::select("id", "crm_company_id") %>%
    dplyr::collect()

  # Upsert Company Custom Fields
  data <- companies$custom_fields %>% resolve_company_ids(all_companies)
  upsert_delete_missing(
    con,
    "raw.crm_company_custom_fields",
    data,
    match_cols = c("company_id", "field_type_id")
  )

  # Upsert Company Mail Addresses
  data <- companies$mail_addrs %>% resolve_company_ids(all_companies)
  upsert_delete_missing(
    con,
    "raw.crm_company_mail_address",
    data,
    match_cols = c("company_id", "mail_address_name")
  )

  # Upsert Company Address
  data <- companies$addrs %>% resolve_company_ids(all_companies)
  upsert_delete_missing(
    con,
    "raw.crm_company_address",
    data,
    match_cols = c("crm_company_address_id")
  )

  # Upsert Company Tels
  data <- companies$tels %>% resolve_company_ids(all_companies)
  upsert_delete_missing(
    con,
    "raw.crm_company_tels",
    data,
    match_cols = c("company_id", "tel_name")
  )

  # Upsert Company Lead Positions
  data <- positions %>%
    resolve_company_ids(all_companies) %>%
    resolve_lead_id(all_leads)
  upsert_delete_missing(
    con,
    "raw.crm_company_lead_positions",
    data,
    match_cols = c("company_id", "lead_id")
  )

  invisible(TRUE)
}

download_and_enrich_companies <- function(crm_key) {
  companies <- Billomatics::get_central_station_companies(
    crm_key,
    positions = FALSE
  )

  custom_fields <- companies %>%
    dplyr::select(custom_fields) %>%
    tidyr::unnest("custom_fields") %>%
    dplyr::select(-attachable_type) %>%
    dplyr::rename(company_id = attachable_id) %>%
    dplyr::rename(
      custom_field_type_id = custom_fields_type_id,
      custom_field_type_name = custom_fields_type_name
    ) %>%
    dplyr::mutate(
      created_at = lubridate::ymd_hms(created_at, tz = "CET"),
      updated_at = lubridate::ymd_hms(updated_at, tz = "CET")
    ) %>%
    dplyr::rename(is_api_input = api_input) %>%
    dplyr::mutate(is_api_input = dplyr::coalesce(is_api_input, FALSE)) %>%
    dplyr::select(
      company_id,
      field_id = id,
      field_type_id = custom_field_type_id,
      field_type_name = custom_field_type_name,
      field_content = name,
      field_created_at = created_at,
      field_updated_at = updated_at,
      is_api_input
    )

  mail_addrs <- companies %>%
    dplyr::select(emails) %>%
    dplyr::filter(lengths(emails) > 0) %>%
    tidyr::unnest("emails") %>%
    dplyr::select(-type, -attachable_type, -name_clean) %>%
    dplyr::rename(
      company_id = attachable_id,
      is_primary = primary,
      is_api_input = api_input
    ) %>%
    dplyr::mutate(
      created_at = lubridate::ymd_hms(created_at, tz = "CET"),
      updated_at = lubridate::ymd_hms(updated_at, tz = "CET")
    ) %>%
    dplyr::mutate(is_primary = dplyr::coalesce(is_primary, TRUE)) %>%
    dplyr::mutate(is_api_input = dplyr::coalesce(is_api_input, FALSE)) %>%
    dplyr::select(
      company_id,
      mail_address_type = atype,
      mail_address_name = name,
      is_primary,
      mail_created_at = created_at,
      mail_updated_at = updated_at,
      is_api_input
    )

  tels <- companies %>%
    dplyr::select(tels) %>%
    dplyr::filter(lengths(tels) > 0) %>%
    tidyr::unnest("tels") %>%
    dplyr::select(-type, -attachable_type) %>%
    dplyr::rename(
      company_id = attachable_id,
      is_primary = primary,
      is_api_input = api_input
    ) %>%
    dplyr::mutate(
      created_at = lubridate::ymd_hms(created_at, tz = "CET"),
      updated_at = lubridate::ymd_hms(updated_at, tz = "CET")
    ) %>%
    dplyr::mutate(is_primary = dplyr::coalesce(is_primary, TRUE)) %>%
    dplyr::mutate(is_api_input = dplyr::coalesce(is_api_input, FALSE)) %>%
    dplyr::select(
      company_id,
      tel_address_type = atype,
      tel_name = name,
      tel_name_clean = name_clean,
      tel_created_at = created_at,
      tel_updated_at = updated_at,
      is_api_input,
      is_primary
    )

  addrs <- companies %>%
    dplyr::select(addrs) %>%
    dplyr::filter(lengths(addrs) > 0) %>%
    tidyr::unnest("addrs") %>%
    dplyr::select(-attachable_type) %>%
    dplyr::rename(
      company_id = attachable_id,
      is_primary = primary,
      is_api_input = api_input
    ) %>%
    dplyr::mutate(
      created_at = lubridate::ymd_hms(created_at, tz = "CET"),
      updated_at = lubridate::ymd_hms(updated_at, tz = "CET")
    ) %>%
    dplyr::mutate(is_primary = dplyr::coalesce(is_primary, TRUE)) %>%
    dplyr::mutate(is_api_input = dplyr::coalesce(is_api_input, FALSE)) %>%
    dplyr::select(
      crm_company_address_id = id,
      company_id,
      address_type = atype,
      street,
      zip,
      city,
      state_code,
      is_primary,
      country_code,
      country_name,
      address_created_at = created_at,
      address_updated_at = updated_at,
      is_api_input
    )

  main_table <- companies %>%
    dplyr::select(
      -account_id,
      -group_id,
      -custom_fields,
      -addrs,
      -tels,
      -emails
    ) %>%
    dplyr::mutate(
      created_at = lubridate::ymd_hms(created_at, tz = "CET"),
      updated_at = lubridate::ymd_hms(updated_at, tz = "CET")
    ) %>%
    dplyr::select(
      crm_company_id = id,
      responsible_user_id = user_id,
      created_by_user_id,
      updated_by_user_id,
      company_name = name,
      company_created_at = created_at,
      company_updated_at = updated_at,
      company_background = background
    )

  new_tables <- list(
    main_table = main_table,
    custom_fields = custom_fields,
    mail_addrs = mail_addrs,
    tels = tels,
    addrs = addrs
  )

  return(new_tables)
}