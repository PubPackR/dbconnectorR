#' Update CRM Lead Protocols
#'
#' Downloads and upserts CRM lead protocols, comments, and attachments.
#'
#' @param con A DBI database connection.
#' @param keys Authentication keys list containing crm key.
#' @param is_daily Logical. Whether this is a daily update.
#' @param override_last_update POSIXct or Date. If provided, overrides both
#'   \code{last_update_protocols} and \code{last_update_attachments} instead of
#'   deriving them from the database. Useful for backfilling or manual reruns.
#' @param crm_lead_ids Optional integer/character vector of external CRM lead IDs
#'   (\code{crm_lead_id}). When provided, the date-based filter is skipped and
#'   only these leads are downloaded. Intended for targeted one-off reruns (e.g.
#'   leads whose protocol count was truncated at 250 due to a pagination bug).
#' @return Invisibly returns NULL after successful update.
#' @export
crm_update_protocols <- function(con, keys, is_daily, override_last_update = NA, crm_lead_ids = NULL) {

  if (!is.null(override_last_update) & !is.na(override_last_update)) {
    last_update_protocols   <- lubridate::as_datetime(override_last_update)
    last_update_attachments <- lubridate::as_datetime(override_last_update)
  } else {
    if (is_daily) {
      last_update_protocols <- dplyr::tbl(con, I("raw.crm_lead_protocols")) %>%
        dplyr::summarise(max_updated_at = max(updated_at, na.rm = TRUE)) %>%
        dplyr::collect() %>%
        dplyr::pull(max_updated_at)
      last_update_protocols <- last_update_protocols - lubridate::hours(1) - lubridate::days(1)

      last_update_attachments <- dplyr::tbl(con, I("raw.crm_lead_protocol_attachments")) %>%
        dplyr::summarise(max_updated_at = max(updated_at, na.rm = TRUE)) %>%
        dplyr::collect() %>%
        dplyr::pull(max_updated_at)
      last_update_attachments <- last_update_attachments - lubridate::hours(1) - lubridate::days(1)
    } else {
      last_update_protocols   <- lubridate::as_datetime(as.Date("2000-01-01"))
      last_update_attachments <- lubridate::as_datetime(as.Date("2000-01-01"))
    }
  }

  # override_last_update überschreibt is_daily vollständig (NULL und NA gelten beide als "kein Override")
  effective_daily <- is_daily || (!is.null(override_last_update) & !is.na(override_last_update))

  leads_to_update <- dplyr::tbl(con, I("raw.crm_leads")) %>%
    dplyr::filter(!is_deleted) %>%
    dplyr::collect()
  if (!is.null(crm_lead_ids)) {
    leads_to_update <- leads_to_update %>%
      dplyr::filter(crm_lead_id %in% crm_lead_ids)
  } else if (effective_daily) {
    leads_to_update <- leads_to_update %>%
      dplyr::filter(lubridate::as_datetime(lead_updated_at) >= as.Date(last_update_protocols))
  }

  protocols <- download_and_enrich_protocols(con, crm_key = keys$crm, last_update_attachments, leads_to_update, daily_download = effective_daily)

  all_users <- dplyr::tbl(con, I("raw.crm_users")) %>% dplyr::select("id", "crm_user_id") %>% dplyr::collect()
  all_leads <- dplyr::tbl(con, I("raw.crm_leads")) %>% dplyr::select("id", "crm_lead_id") %>% dplyr::collect()
  deleted_leads <- dplyr::tbl(con, I("raw.crm_leads")) %>% dplyr::filter(is_deleted) %>% dplyr::select("id", "crm_lead_id") %>% dplyr::collect()
  main_table <- protocols$main_table
  attachments <- protocols$attachments
  comments <- protocols$comments
  comment_attachments <- protocols$comment_attachments
  protocol_relations <- dplyr::tbl(con, I("raw.crm_lead_protocol_relations")) %>% dplyr::select("protocol_id", "lead_id") %>% replace_internal_ids_with_external("lead_id", dplyr::tbl(con, I("raw.crm_leads")), "crm_lead_id")
  protocols_old <- dplyr::tbl(con, I("raw.crm_lead_protocols")) %>%
    dplyr::left_join(protocol_relations, by = c("id" = "protocol_id")) %>%
    dplyr::filter(lead_id %in% !!leads_to_update$crm_lead_id) %>%
    dplyr::filter(!(crm_protocol_id %in% !!main_table$crm_protocol_id)) %>%
    dplyr::filter(lead_id %in% !!deleted_leads$crm_lead_id | lead_id %in% !!main_table$lead_id) %>%
    dplyr::select(-id, -updated_at, -created_at) %>%
    dplyr::mutate(is_deleted = TRUE) %>%
    dplyr::collect()

  print(paste0("Number of deleted protocols: ", nrow(protocols_old)))

  data <- main_table %>%
    tidyr::drop_na(crm_protocol_id) %>%
    dplyr::bind_rows(protocols_old) %>%
    resolve_user_ids(all_users, "user_id") %>%
    tidyr::drop_na(user_id) %>%
    dplyr::filter(protocol_type != "comment") %>%
    dplyr::filter(protocol_type != "protocol_attachment") %>%
    dplyr::select(-lead_id) %>%
    dplyr::distinct()

  if(data %>% nrow() > 0) {

    batch_upsert(
      con,
      "raw",
      "crm_lead_protocols",
      data,
      match_cols = c("crm_protocol_id"),
      batch_size = 10000
    )
    print("protocols uploaded")

  }

  all_protocols <- dplyr::tbl(con, I("raw.crm_lead_protocols")) %>%
    dplyr::select(crm_protocol_id, id) %>%
    dplyr::collect()

  data <- main_table %>%
    dplyr::filter(protocol_type != "comment") %>%
    dplyr::filter(protocol_type != "protocol_attachment") %>%
    dplyr::filter(protocol_type != "user_notes") %>%
    replace_external_ids_with_internal("lead_id", all_leads, "crm_lead_id") %>%
    replace_external_ids_with_internal("crm_protocol_id", all_protocols, "crm_protocol_id", "protocol_id") %>%
    tidyr::drop_na(protocol_id, lead_id)

  leads_to_update_internal_ids <- all_leads %>%
    dplyr::filter(crm_lead_id %in% !!leads_to_update$crm_lead_id) %>%
    dplyr::pull(id)

  old_in_scope_relations <- dplyr::tbl(con, I("raw.crm_lead_protocol_relations")) %>%
    dplyr::select("protocol_id", "lead_id") %>%
    dplyr::filter(lead_id %in% !!leads_to_update_internal_ids) %>%
    dplyr::collect()

  protocol_relations_filtered <- dplyr::tbl(con, I("raw.crm_lead_protocol_relations")) %>%
    dplyr::select("protocol_id", "lead_id") %>%
    dplyr::filter(!(lead_id %in% !!deleted_leads$id | lead_id %in% !!leads_to_update_internal_ids)) %>%
    dplyr::collect()

  full_data <- dplyr::bind_rows(
    data %>% dplyr::select(protocol_id, lead_id),
    protocol_relations_filtered
  ) %>% dplyr::distinct(protocol_id, lead_id) %>%
    tidyr::drop_na(protocol_id, lead_id)

  old_full_relations <- dplyr::bind_rows(old_in_scope_relations, protocol_relations_filtered)
  new_relations <- dplyr::anti_join(
    full_data,
    old_full_relations,
    by = c("protocol_id", "lead_id")
  )
  removed_relations <- dplyr::anti_join(
    old_full_relations,
    full_data,
    by = c("protocol_id", "lead_id")
  )
  message(sprintf(
    "[relations] scope: %d leads | +%d added, -%d removed | total: %d → %d",
    length(leads_to_update_internal_ids),
    nrow(new_relations),
    nrow(removed_relations),
    nrow(old_in_scope_relations) + nrow(protocol_relations_filtered),
    nrow(full_data)
  ))

  pool::poolWithTransaction(con, function(con) {
    DBI::dbExecute(con, "TRUNCATE raw.crm_lead_protocol_relations")
    Billomatics::postgres_upsert_data(con, "raw", "crm_lead_protocol_relations", full_data, match_cols = c("protocol_id", "lead_id"))
  })
  print("protocols mapping uploaded")

  # Heruntergeladene Comments aufbereiten
  downloaded_comments <- comments %>%
    tidyr::drop_na(crm_comment_id) %>%
    resolve_user_ids(all_users, "user_id") %>%
    resolve_protocol_id(all_protocols, "protocol_id") %>%
    tidyr::drop_na(protocol_id) %>%
    dplyr::distinct(crm_comment_id, .keep_all = TRUE) %>%
    tidyr::drop_na(user_id)

  # Comments aus DB die zu heruntergeladenen oder gelöschten Protokollen gehören,
  # aber nicht im Download sind → als gelöscht markieren
  downloaded_protocol_db_ids <- all_protocols %>%
    dplyr::filter(crm_protocol_id %in% main_table$crm_protocol_id) %>%
    dplyr::pull(id)
  deleted_protocol_db_ids <- all_protocols %>%
    dplyr::filter(crm_protocol_id %in% protocols_old$crm_protocol_id) %>%
    dplyr::pull(id)
  affected_protocol_ids <- unique(c(downloaded_protocol_db_ids, deleted_protocol_db_ids))

  comments_old <- dplyr::tbl(con, I("raw.crm_lead_protocol_comments")) %>%
    dplyr::filter(protocol_id %in% !!affected_protocol_ids) %>%
    dplyr::filter(!(crm_comment_id %in% !!downloaded_comments$crm_comment_id)) %>%
    dplyr::select(-id, -updated_at, -created_at) %>%
    dplyr::mutate(is_deleted = TRUE) %>%
    dplyr::collect()

  print(paste0("Number of deleted comments: ", nrow(comments_old)))

  data <- downloaded_comments %>%
    dplyr::bind_rows(comments_old) %>%
    tidyr::drop_na(crm_comment_id)

  Billomatics::postgres_upsert_data(con, "raw", "crm_lead_protocol_comments", data, match_cols = c("crm_comment_id"), returning_cols = c("id", "crm_comment_id"), delete_missing = FALSE)
  print("comments uploaded")

  all_comments <- dplyr::tbl(con, I("raw.crm_lead_protocol_comments")) %>%
    dplyr::select(crm_comment_id, id) %>%
    dplyr::collect()

  # Heruntergeladene Attachments aufbereiten
  downloaded_attachments <- attachments %>%
    tidyr::drop_na(crm_attachment_id) %>%
    resolve_user_ids(all_users, "user_id") %>%
    resolve_protocol_id(all_protocols, "protocol_id") %>%
    dplyr::distinct(crm_attachment_id, .keep_all = TRUE) %>%
    tidyr::drop_na(user_id) %>%
    drop_attachments_without_file_metadata("protocol")

  # Attachments aus DB die zu heruntergeladenen oder gelöschten Protokollen gehören,
  # aber nicht im Download sind → als gelöscht markieren
  attachments_old <- dplyr::tbl(con, I("raw.crm_lead_protocol_attachments")) %>%
    dplyr::collect() %>%
    dplyr::left_join(all_protocols %>% dplyr::select(id, crm_protocol_id), by = c("protocol_id" = "id")) %>%
    dplyr::filter(crm_protocol_id %in% protocols$attachment_for_protocols$id) %>% 
    dplyr::filter(!(crm_attachment_id %in% !!downloaded_attachments$crm_attachment_id)) %>%
    dplyr::select(-id, -updated_at, -created_at, -crm_protocol_id) %>%
    dplyr::mutate(is_deleted = TRUE)

  print(paste0("Number of deleted attachments: ", nrow(attachments_old)))

  data <- downloaded_attachments %>%
    dplyr::bind_rows(attachments_old) %>%
    tidyr::drop_na(crm_attachment_id)

  Billomatics::postgres_upsert_data(con, "raw", "crm_lead_protocol_attachments", data, match_cols = c("crm_attachment_id"))
  print("attachments uploaded")

  # Heruntergeladene Comment-Attachments aufbereiten
  downloaded_comment_attachments <- comment_attachments %>%
    tidyr::drop_na(crm_attachment_id) %>%
    resolve_user_ids(all_users, "user_id") %>%
    resolve_comment_ids(all_comments, "comment_id") %>%
    tidyr::drop_na(comment_id) %>%
    dplyr::distinct(crm_attachment_id, .keep_all = TRUE) %>%
    tidyr::drop_na(user_id) %>%
    drop_attachments_without_file_metadata("comment")

  # Comment-Attachments aus DB deren Prefix gesucht wurde,
  # aber nicht im Download sind → als gelöscht markieren
  searched_prefixes <- protocols$searched_prefixes
  if (length(searched_prefixes) > 0) {
    comment_attachments_old <- dplyr::tbl(con, I("raw.crm_lead_protocol_comment_attachments")) %>%
      dplyr::collect() %>%
      dplyr::filter(grepl_chunked(filename, searched_prefixes)) %>%
      dplyr::filter(!(crm_attachment_id %in% downloaded_comment_attachments$crm_attachment_id)) %>%
      dplyr::select(-id, -updated_at, -created_at) %>%
      dplyr::mutate(is_deleted = TRUE)

    print(paste0("Number of deleted comment attachments: ", nrow(comment_attachments_old)))

    data <- downloaded_comment_attachments %>%
      dplyr::bind_rows(comment_attachments_old) %>%
      tidyr::drop_na(crm_attachment_id)
  } else {
    data <- downloaded_comment_attachments
  }

  Billomatics::postgres_upsert_data(con, "raw", "crm_lead_protocol_comment_attachments", data, match_cols = c("crm_attachment_id"))
  print("comment attachments uploaded")
}

download_and_enrich_protocols <- function(con, crm_key, last_update_attachments = NA, leads_to_update, daily_download = TRUE) {

  offers_billomat <- dplyr::tbl(con, I("raw.billomat_offers")) %>% dplyr::collect()
  confirmations_billomat <- dplyr::tbl(con, I("raw.billomat_confirmations")) %>% dplyr::collect()

  new_protocols <- import_new_protocols(
      crm_key,
      leads_to_update
    ) %>%
    dplyr::mutate(is_user_generated = is.na(user_id) | is.na(name) | (user_id != "199146" & !grepl("Vorlage Note API", name)))

  protocols_new <- simplify_protocols(protocols = new_protocols) %>%
    dplyr::mutate(is_user_generated = is.na(user_id) | is.na(name) | (user_id != "199146" & !grepl("Vorlage Note API", name)))
  
  attachments <- expand_protocol_attachments(protocols_simplified = protocols_new, crm_api_key = crm_key, last_update_attachments = last_update_attachments, daily_download = daily_download)
    
  comment_attachments_result <- expand_protocol_comment_attachments(crm_api_key = crm_key, offers_billomat, confirmations_billomat, last_update_attachments)

  ################################-

  comments <- protocols_new %>%
    dplyr::filter(type == "comment")

  if(nrow(comments)) {

    comments <- comments %>%
      dplyr::mutate(comment_updated_at = lubridate::ymd_hms(updated_at, tz = "CET")) %>%
      dplyr::mutate(is_deleted = FALSE) %>%
      dplyr::select(crm_comment_id = id,
             user_id,
             protocol_id = attachable_id,
             comment_name = name,
             comment_content = content,
             comment_updated_at,
             is_user_generated,
             is_deleted)

  } else {

    comments <- tibble::tibble(
      crm_comment_id = character(),
      user_id = integer(),
      protocol_id = character(),
      comment_name = character(),
      comment_content = character(),
      comment_updated_at = as.POSIXct(character()),
      is_user_generated = logical(),
      is_deleted = logical()
    )

  }

  ################################-

  protocols_new <- protocols_new %>%
    dplyr::filter(type != "comment")

  if(nrow(protocols_new) > 0) {

    main_table <- protocols_new %>%
      dplyr::mutate(protocol_created_at  = lubridate::ymd_hms(created_at,  tz = "CET"),
             protocol_updated_at  = lubridate::ymd_hms(updated_at,  tz = "CET"),
             protocol_occurred_at = dplyr::coalesce(
               lubridate::ymd_hms(occurred_at, tz = "CET"),
               protocol_created_at
             )) %>%
      dplyr::mutate(is_deleted = FALSE) %>%
      dplyr::select(
        crm_protocol_id = id,
        user_id,
        lead_id = person_ids,
        protocol_name = name,
        protocol_content = content,
        protocol_updated_at,
        protocol_created_at,
        protocol_occurred_at,
        attachments_count,
        protocol_type = type,
        is_user_generated,
        is_deleted
      )

  } else {

    main_table <- tibble::tibble(
      crm_protocol_id = character(),
      user_id = integer(),
      lead_id = integer(),
      protocol_name = character(),
      protocol_content = character(),
      protocol_updated_at = as.POSIXct(character()),
      protocol_created_at = as.POSIXct(character()),
      protocol_occurred_at = as.POSIXct(character()),
      attachments_count = integer(),
      protocol_type = character(),
      is_user_generated = logical(),
      is_deleted = logical()
    )

  }

  ################################-

  attachments$attachments <- attachments$attachments %>%
    dplyr::filter(protocol_id %in% main_table$crm_protocol_id) %>%
    dplyr::select(crm_attachment_id = id, user_id, protocol_id, attachment_updated_at = updated_at, attachment_created_at = created_at, filename, content_type, file_size = size, is_deleted)

  comment_attachments <- comment_attachments_result$attachments %>%
    dplyr::select(comment_id, user_id, crm_attachment_id = id, attachment_created_at = created_at, attachment_updated_at = updated_at, filename, content_type, file_size = size, is_deleted)

  ################################-

  protocols <- list(main_table = main_table,
                     comments = comments,
                     attachments = attachments$attachments,
                     attachment_for_protocols = attachments$protocols_to_update,
                     comment_attachments = comment_attachments,
                     searched_prefixes = comment_attachments_result$searched_prefixes)
  
  return(protocols)
  
}

expand_protocol_attachments <- function(protocols_simplified, crm_api_key, last_update_attachments = NA, daily_download = TRUE){

  protocols_to_update <- protocols_simplified %>%
    dplyr::filter(attachments_count > 0) %>%
    dplyr::filter(!type %in% c("comment", "protocol_attachment")) %>%
    dplyr::distinct(id, attachments_count, updated_at) %>%
    dplyr::filter(is.na(last_update_attachments) | as.Date(updated_at) >= as.Date(last_update_attachments))

  print(paste0("Update Attachments for ", nrow(protocols_to_update), " Protocols"))

  if (nrow(protocols_to_update) > 0) {

    fetch_attachments <- function(ids) {
      result <- Billomatics::get_central_station_attachments(crm_api_key, as.vector(ids)) %>%
        conditional_unnest("attachment", "_")
      colnames(result) <- gsub("attachment_", "", names(result))
      result
    }

    # Initial download
    attachments_new <- fetch_attachments(protocols_to_update$id)

    # Retry loop: up to 2 retries for protocols where downloaded count != attachments_count
    for (retry_num in 1:2) {
      if (!"attachable_id" %in% names(attachments_new)) break
      downloaded_counts <- attachments_new %>%
        dplyr::mutate(attachable_id = as.character(attachable_id)) %>%
        dplyr::count(attachable_id, name = "downloaded") %>%
        dplyr::right_join(
          protocols_to_update %>%
            dplyr::select(id, attachments_count) %>%
            dplyr::mutate(id = as.character(id)),
          by = c("attachable_id" = "id")
        ) %>%
        dplyr::mutate(downloaded = tidyr::replace_na(downloaded, 0L))

      mismatch_ids <- downloaded_counts %>%
        dplyr::filter(downloaded != attachments_count) %>%
        dplyr::pull(attachable_id)

      if (length(mismatch_ids) == 0) break

      warning(sprintf(
        "Attachment count mismatch (retry %d/2): %d protocols — %s",
        retry_num, length(mismatch_ids), paste(mismatch_ids, collapse = ", ")
      ))

      retry_result <- fetch_attachments(mismatch_ids)
      if ("attachable_id" %in% names(retry_result) && nrow(retry_result) > 0) {
        attachments_new <- attachments_new %>%
          dplyr::filter(!(as.character(attachable_id) %in% mismatch_ids)) %>%
          dplyr::bind_rows(retry_result)
      }
    }

    # After retries: if no attachable_id column, no data came back at all
    if (!"attachable_id" %in% names(attachments_new)) {
      warning(sprintf(
        "Excluding all %d protocols from deletion scope: no attachments downloaded at all",
        nrow(protocols_to_update)
      ))
      protocols_to_update <- protocols_to_update[0, ]
    } else {
      final_counts <- attachments_new %>%
        dplyr::mutate(attachable_id = as.character(attachable_id)) %>%
        dplyr::count(attachable_id, name = "downloaded") %>%
        dplyr::right_join(
          protocols_to_update %>%
            dplyr::select(id, attachments_count) %>%
            dplyr::mutate(id = as.character(id)),
          by = c("attachable_id" = "id")
        ) %>%
        dplyr::mutate(downloaded = tidyr::replace_na(downloaded, 0L))

      still_mismatching <- final_counts %>%
        dplyr::filter(downloaded != attachments_count) %>%
        dplyr::pull(attachable_id)

      if (length(still_mismatching) > 0) {
        warning(sprintf(
          "Excluding %d protocols from deletion scope after 2 retries (API count mismatch): %s",
          length(still_mismatching), paste(still_mismatching, collapse = ", ")
        ))
        protocols_to_update <- protocols_to_update %>%
          dplyr::filter(!as.character(id) %in% still_mismatching)
      }
    }

    attachments <- if ("attachable_id" %in% names(attachments_new)) {
      attachments_new %>%
        dplyr::select(-account_id,
               -attachable_type,
               -category_id) %>%
        dplyr::rename(protocol_id = attachable_id) %>%
        dplyr::mutate(created_at = lubridate::ymd_hms(created_at, tz = "CET"),
               updated_at = lubridate::ymd_hms(updated_at, tz = "CET"),
               protocol_id = as.character(protocol_id)) %>%
        dplyr::mutate(is_deleted = FALSE)
    } else {
      tibble::tibble(
        id = character(), user_id = integer(), protocol_id = character(),
        updated_at = as.POSIXct(character()), created_at = as.POSIXct(character()),
        filename = character(), content_type = character(), size = integer(),
        is_deleted = logical()
      )
    }

  } else {

    attachments <- tibble::tibble(
      id = character(),
      user_id = integer(),
      protocol_id = character(),
      updated_at = as.POSIXct(character()),
      created_at = as.POSIXct(character()),
      filename = character(),
      content_type = character(),
      size = integer(),
      is_deleted = logical()
    )

  }

  return(list(attachments = attachments, protocols_to_update = protocols_to_update))

}

expand_protocol_comment_attachments <- function(crm_api_key, offers_billomat, confirmations_billomat, last_update_attachments = NA){

  next_year <- as.character(as.integer(format(Sys.Date(), "%Y")) + 1)

  if(!is.na(last_update_attachments)) {
    last_update_attachments_comments <- last_update_attachments
  } else {
    last_update_attachments_comments <- as.Date("2000-01-01") # arbitrary old date to include all attachments if no last update is provided
  }

  last_update_attachments_comments <- pmin(last_update_attachments_comments, Sys.Date() - lubridate::days(3)) # Ensure minimum 3-day lookback window (pmin picks older date)

  offer_numbers <- offers_billomat %>%
    dplyr::group_by(offer_number) %>%
    dplyr::summarise(last_update = max(offer_updated_at, na.rm = TRUE)) %>%
    dplyr::ungroup() %>%
    dplyr::mutate(in_time_frame = as.Date(last_update) >= as.Date(last_update_attachments_comments)) %>%
    dplyr::filter(grepl("AN20", offer_number)) %>%
    dplyr::mutate(is_highest_offer_numbers = rank(offer_number, ties.method = "first") > (n() - 3)) %>%
    dplyr::arrange(offer_number) %>%
    dplyr::filter(in_time_frame | is_highest_offer_numbers) %>%
    dplyr::mutate(offer_number = ifelse(is_highest_offer_numbers, substring(offer_number, 1, nchar(offer_number) - 2), offer_number)) %>%
    dplyr::distinct(offer_number)

  # Next hundred-prefix: highest current number + 1 hundred-range
  an_next_prefix <- NULL
  if (nrow(offer_numbers) > 0) {
    highest_an <- dplyr::last(offer_numbers$offer_number)
    an_num <- as.integer(gsub(".*SF", "", highest_an))
    an_year <- gsub("SF.*", "", gsub("AN", "", highest_an))
    an_next_prefix <- paste0("AN", an_year, "SF", formatC(an_num + 1, width = 2, flag = "0"))
  }

  if (!is.null(an_next_prefix)) {
    offer_numbers <- offer_numbers %>%
      dplyr::filter(!grepl(highest_an, offer_number, fixed = TRUE))
  }

  # Add next prefix and next year's first prefix
  offer_numbers <- c(
    offer_numbers$offer_number,
    if (!is.null(an_next_prefix)) highest_an,
    an_next_prefix,
    paste0("AN", next_year, "SF00")
  ) %>% unique()

  confirmation_numbers <- confirmations_billomat %>%
    dplyr::group_by(confirmation_number) %>%
    dplyr::summarise(last_update = max(confirmation_updated_at, na.rm = TRUE)) %>%
    dplyr::ungroup() %>%
    dplyr::mutate(in_time_frame = as.Date(last_update) >= as.Date(last_update_attachments_comments)) %>%
    dplyr::filter(grepl("AB20", confirmation_number)) %>%
    dplyr::mutate(is_highest_confirmation_numbers = rank(confirmation_number, ties.method = "first") > (n() - 3)) %>%
    dplyr::arrange(confirmation_number) %>%
    dplyr::filter(in_time_frame | is_highest_confirmation_numbers) %>%
    dplyr::mutate(confirmation_number = ifelse(is_highest_confirmation_numbers, substring(confirmation_number, 1, nchar(confirmation_number) - 2), confirmation_number)) %>%
    dplyr::distinct(confirmation_number)

  # Next hundred-prefix for ABs
  ab_next_prefix <- NULL
  if (nrow(confirmation_numbers) > 0) {
    highest_ab <- dplyr::last(confirmation_numbers$confirmation_number)
    ab_num <- as.integer(gsub(".*SF", "", highest_ab))
    ab_year <- gsub("SF.*", "", gsub("AB", "", highest_ab))
    ab_next_prefix <- paste0("AB", ab_year, "SF", formatC(ab_num + 1, width = 2, flag = "0"))
  }

  if (!is.null(ab_next_prefix)) {
    confirmation_numbers <- confirmation_numbers %>%
      dplyr::filter(!grepl(highest_ab, confirmation_number, fixed = TRUE))
  }

  # Add next prefix and next year's first prefix
  confirmation_numbers <- c(
    confirmation_numbers$confirmation_number,
    if (!is.null(ab_next_prefix)) highest_ab,
    ab_next_prefix,
    paste0("AB", next_year, "SF00")
  ) %>% unique()

  an_attachments <- get_searched_attachments(crm_api_key, offer_numbers)
  ab_attachments <- get_searched_attachments(crm_api_key, confirmation_numbers)

  comment_attachments_new <- dplyr::bind_rows(an_attachments, ab_attachments)

  if (nrow(comment_attachments_new) == 0) {
    message("No new attachments found for searched prefixes")
    comment_attachments_new <- tibble::tibble(
      id = character(), comment_id = character(), user_id = character(),
      name = character(), created_at = as.POSIXct(character()), updated_at = as.POSIXct(character()),
      is_deleted = logical()
    )
    return(list(attachments = comment_attachments_new, searched_prefixes = c(offer_numbers, confirmation_numbers)))
  }

  comment_attachments_new <- comment_attachments_new %>%
    tidyr::unnest()

  comment_attachments_new <- comment_attachments_new %>%
    dplyr::select(-tidyselect::any_of(c("account_id", "attachment_category_id"))) %>%
    dplyr::rename(comment_id = attachable_id) %>%
    dplyr::mutate(
      created_at = lubridate::ymd_hms(created_at, tz = "CET"),
      updated_at = lubridate::ymd_hms(updated_at, tz = "CET"),
      comment_id = as.character(comment_id)
    ) %>%
    dplyr::mutate(is_deleted = FALSE)

  comment_attachments_new <- comment_attachments_new %>%
    dplyr::select(-tidyselect::any_of(c("attachable_type")))

  return(list(
    attachments = comment_attachments_new,
    searched_prefixes = c(offer_numbers, confirmation_numbers)
  ))

}

import_new_protocols <- function(crm_key, leads_to_update) {
  
  column_names <- c(
    "id", "user_id", "attachments_count", "comments_count", "name", "confidential", "format", "content", "updated_by_user_id", "created_at", "updated_at", "occurred_at", "data_hash",
    "account_id", "type", "badge", "person_ids", "company_ids", "deal_ids", "project_ids", "person_id", "company_id", "comments", "protocol_attachment_id",
    "protocol_attachment_user_id", "protocol_attachment_attachments_count", "protocol_attachment_comments_count", "protocol_attachment_name",
    "protocol_attachment_confidential", "protocol_attachment_format", "protocol_attachment_content", "protocol_attachment_updated_by_user_id",
    "protocol_attachment_created_at", "protocol_attachment_updated_at", "protocol_attachment_occurred_at", "protocol_attachment_data_hash", "protocol_attachment_account_id",
    "protocol_attachment_type", "protocol_attachment_badge", "protocol_attachment_person_ids", "protocol_attachment_company_ids",
    "protocol_attachment_deal_ids", "protocol_attachment_project_ids", "protocol_attachment_person_id", "protocol_attachment_company_id",
    "protocol_attachment_comments", "protocol_email_id", "protocol_email_user_id", "protocol_email_attachments_count", "protocol_email_comments_count",
    "protocol_email_name", "protocol_email_confidential", "protocol_email_format", "protocol_email_content", "protocol_email_updated_by_user_id",
    "protocol_email_created_at", "protocol_email_updated_at", "protocol_email_occurred_at", "protocol_email_data_hash_from", "protocol_email_data_hash_to",
    "protocol_email_data_hash_cc", "protocol_email_data_hash_bcc", "protocol_email_data_hash_subject", "protocol_email_data_hash_time",
    "protocol_email_data_hash_inbound_email_id", "protocol_email_data_hash_mail_part", "protocol_email_account_id", "protocol_email_type", "protocol_email_badge",
    "protocol_email_person_ids", "protocol_email_company_ids", "protocol_email_deal_ids", "protocol_email_project_ids", "protocol_email_person_id", "protocol_email_company_id",
    "protocol_email_comments", "protocol_user_note_id", "protocol_user_note_user_id", "protocol_user_note_attachments_count", "protocol_user_note_comments_count",
    "protocol_user_note_name", "protocol_user_note_confidential", "protocol_user_note_format", "protocol_user_note_content",
    "protocol_user_note_updated_by_user_id", "protocol_user_note_created_at", "protocol_user_note_updated_at", "protocol_user_note_occurred_at", "protocol_user_note_data_hash",
    "protocol_user_note_account_id", "protocol_user_note_type", "protocol_user_note_badge", "protocol_user_note_person_ids",
    "protocol_user_note_company_ids", "protocol_user_note_deal_ids", "protocol_user_note_project_ids", "protocol_user_note_person_id",
    "protocol_user_note_company_id", "protocol_user_note_comments", "protocol_user_note", "user_generated"
  )
  
  # Print the number of persons for whom protocols are being downloaded
  print(paste0("Downloading Protocols for ", nrow(leads_to_update), " Persons"))

  protocols_all <- c()

  # Check if there are persons to update
  if (nrow(leads_to_update) > 0) {

    # IDs in Batches von 100 aufteilen
    id_batches <- split(leads_to_update$crm_lead_id, ceiling(seq_along(leads_to_update$crm_lead_id) / 100))

    # Funktion zur Verarbeitung eines Batches
    process_protocols_batch <- function(id_batch) {
      protocols_new <- Billomatics::get_central_station_protocols(crm_key, "person_id", id_batch)

      if (nrow(protocols_new) > 0) {
        cols <- c("id" = NA, "protocol_email_id" = NA, "protocol_attachment_id" = NA, "protocol_user_note_id" = NA)

        protocols_new <- protocols_new %>%
          conditional_unnest("protocol_object_note", NA) %>%
          conditional_unnest("protocol_email", "_") %>%
          conditional_unnest("protocol_email_data_hash", "_") %>%
          conditional_unnest("protocol_attachment", "_") %>%
          conditional_unnest("protocol_user_note", "_") %>%
          dplyr::mutate(
            updated_at = lubridate::as_datetime(updated_at, tz = "CET"),
            created_at = lubridate::as_datetime(created_at, tz = "CET")
          ) %>%
          tibble::add_column(!!!cols[!names(cols) %in% names(.)]) %>%
          dplyr::mutate(id = dplyr::coalesce(id, protocol_email_id, protocol_attachment_id, protocol_user_note_id)) %>%
          dplyr::filter(!is.na(id))

        return(protocols_new)
      } else {
        return(NULL) # Falls kein neuer Eintrag vorhanden ist
      }
    }

    # Alle Batches durchlaufen und Ergebnisse sammeln
    n_batches <- length(id_batches)
    protocols_new_list <- vector("list", n_batches)
    batch_start_time <- Sys.time()

    for (i in seq_along(id_batches)) {
      protocols_new_list[[i]] <- process_protocols_batch(id_batches[[i]])

      elapsed <- as.numeric(difftime(Sys.time(), batch_start_time, units = "secs"))
      avg_per_batch <- elapsed / i
      remaining <- avg_per_batch * (n_batches - i)
      eta_min <- round(remaining / 60, 1)
      cat(sprintf("Batch %d/%d done | ETA: %.1f min\n", i, n_batches, eta_min))
      flush.console()
    }

    # NULL-Werte entfernen und zusammenführen
    protocols_new <- dplyr::bind_rows(purrr::compact(protocols_new_list))

    # Falls Daten vorhanden sind, zu protocols_all hinzufügen
    if (nrow(protocols_new) > 0) {
      protocols_all <- dplyr::bind_rows(protocols_all, protocols_new)
    }
  } else {}

  protocols_all <- dplyr::tibble(!!!ensure_columns(column_names, protocols_all)) %>%
    tidyr::drop_na(id) %>%
    dplyr::mutate(id = dplyr::coalesce(
      id,
      protocol_email_id,
      protocol_attachment_id,
      protocol_user_note_id
    ))

  return(protocols_all)
}

ensure_columns <- function(old_column_names, new_df) {
  missing_cols <- setdiff(old_column_names, names(new_df))
  for (col in missing_cols) {
    new_df[[col]] <- NA
  }
  new_df <- new_df[old_column_names]  # Reorder to match old_df
  return(new_df)
}

get_searched_attachments <- function(api_key, prefixes) {

  # define header
  headers <- c(
    "content-type" = "application/json",
    "X-apikey" = api_key,
    "Accept" = "*/*"
  )

  # create an data.frame for attachments
  attachments <- tibble::tibble()

  for (i in prefixes) {
    tryCatch({
      response <-
        httr::GET(
          paste0(
            "https://api.centralstationcrm.net/api/attachments/search?filename=", i
          ),
          httr::add_headers(headers)
        )

      status_code <- httr::status_code(response)

      # Rate limit handling: wait and retry
      if (status_code == 429) {
        retry_after <- as.numeric(httr::headers(response)[["Retry-After"]])
        wait_time <- ifelse(is.na(retry_after), 60, retry_after)
        warning(sprintf("Rate limited on prefix '%s', waiting %d seconds...", i, wait_time))
        Sys.sleep(wait_time)

        # Retry once after waiting
        response <- httr::GET(
          paste0("https://api.centralstationcrm.net/api/attachments/search?filename=", i),
          httr::add_headers(headers)
        )
        status_code <- httr::status_code(response)
      }

      if (status_code != 200) {
        warning(sprintf("API error for prefix '%s': HTTP %d", i, status_code))
        next
      }

      data <- jsonlite::fromJSON(suppressWarnings(httr::content(response, "text"))) %>%
        dplyr::select(-data)
      attachments <- dplyr::bind_rows(attachments, data)
    },
    error = function(cond) {
      warning(sprintf("Error fetching attachments for prefix '%s': %s", i, cond$message))
    })
    print(i)
    Sys.sleep(0.5)  # Brief pause between calls to avoid rate limits
  }

  return(attachments)
}

simplify_protocols <- function(protocols) {
  
  notes <- protocols %>%
    dplyr::mutate(person_ids = purrr::map(person_ids, ~ as.integer(unlist(.x)))) %>%
    tidyr::unnest(person_ids) %>%
    dplyr::select(id, user_id, person_ids, name, content, updated_at, attachments_count, created_at, occurred_at) %>%
    dplyr::mutate(id = as.character(id),
           created_at = as.character(created_at),
           updated_at = as.character(updated_at),
           occurred_at = as.character(occurred_at)) %>%
    dplyr::mutate(type = "note")
  
  protocol_attachments <- protocols %>%
    dplyr::mutate(protocol_attachment_person_ids = purrr::map(protocol_attachment_person_ids, ~ as.integer(unlist(.x)))) %>%
    tidyr::unnest(protocol_attachment_person_ids) %>%
    dplyr::select(protocol_attachment_id, protocol_attachment_user_id, protocol_attachment_person_ids, protocol_attachment_content, protocol_attachment_name, protocol_attachment_updated_at, protocol_attachment_attachments_count, protocol_attachment_created_at, protocol_attachment_occurred_at) %>%
    dplyr::rename(id = protocol_attachment_id,
           user_id = protocol_attachment_user_id,
           person_ids = protocol_attachment_person_ids,
           name = protocol_attachment_name,
           content = protocol_attachment_content,
           created_at = protocol_attachment_created_at,
           updated_at = protocol_attachment_updated_at,
           occurred_at = protocol_attachment_occurred_at,
           attachments_count = protocol_attachment_attachments_count) %>%
    dplyr::mutate(type = "protocol_attachment",
                  id = as.character(id),
                  name = as.character(name))
  
  user_notes <- protocols %>%
    dplyr::mutate(protocol_user_note_person_ids = purrr::map(protocol_user_note_person_ids, ~ as.integer(unlist(.x)))) %>%
    tidyr::unnest(protocol_user_note_person_ids) %>%
    dplyr::select(protocol_user_note_id, protocol_user_note_user_id, protocol_user_note_person_ids, protocol_user_note_content, protocol_user_note_name, protocol_user_note_updated_at, protocol_user_note_attachments_count, protocol_user_note_created_at, protocol_user_note_occurred_at) %>%
    dplyr::rename(id = protocol_user_note_id,
           user_id = protocol_user_note_user_id,
           person_ids = protocol_user_note_person_ids,
           name = protocol_user_note_name,
           content = protocol_user_note_content,
           created_at = protocol_user_note_created_at,
           updated_at = protocol_user_note_updated_at,
           occurred_at = protocol_user_note_occurred_at,
           attachments_count = protocol_user_note_attachments_count) %>%
    dplyr::mutate(type = "user_notes",
                  id = as.character(id))
    
  mails <- protocols %>%
    dplyr::mutate(protocol_email_person_ids = purrr::map(protocol_email_person_ids, ~ as.integer(unlist(.x)))) %>%
    tidyr::unnest(protocol_email_person_ids) %>%
    dplyr::select(protocol_email_id, protocol_email_user_id, protocol_email_person_ids, protocol_email_name, protocol_email_content, protocol_email_updated_at, protocol_email_attachments_count, protocol_email_created_at, protocol_email_occurred_at) %>%
    dplyr::rename(id = protocol_email_id,
           user_id = protocol_email_user_id,
           person_ids = protocol_email_person_ids,
           name = protocol_email_name,
           content = protocol_email_content,
           created_at = protocol_email_created_at,
           updated_at = protocol_email_updated_at,
           occurred_at = protocol_email_occurred_at,
           attachments_count = protocol_email_attachments_count) %>%
    dplyr::mutate(id = as.character(id)) %>%
    dplyr::mutate(type = "email")
  
  comments_notes <- protocols %>%
    dplyr::mutate(comments = sapply(comments, as.list)) %>%
    dplyr::select(comments) %>%
    dplyr::filter(comments != "NA") %>%
    tidyr::drop_na(comments) %>% 
    tidyr::unnest_wider(comments) 
  
  if(nrow(comments_notes) > 0 && ncol(comments_notes) > 0) {
    comments_notes <- comments_notes %>%
      dplyr::mutate(dplyr::across(dplyr::any_of("person_ids"), ~ purrr::map(.x, ~ as.integer(unlist(.x))))) %>%
      tidyr::unnest() %>%
      dplyr::mutate(id = as.character(id), content = name) %>%
      dplyr::left_join(notes %>% dplyr::select(id, person_ids), by = c("attachable_id" = "id"), relationship = "many-to-many")
  }

  protocol_email_comments <- protocols %>%
    dplyr::mutate(comments = sapply(protocol_email_comments, as.list)) %>%
    dplyr::select(comments) %>%
    tidyr::drop_na(comments) %>% 
    dplyr::filter(comments != "NA") %>%
    tidyr::unnest_wider(comments) %>%
    dplyr::mutate(dplyr::across(dplyr::any_of("person_ids"), ~ purrr::map(.x, ~ as.integer(unlist(.x))))) %>%
    tidyr::unnest()

  if(ncol(protocol_email_comments) > 1 && nrow(protocol_email_comments) > 0) {
    protocol_email_comments <- protocol_email_comments %>% 
      dplyr::mutate(id = as.character(id), content = name) %>%
      dplyr::left_join(mails %>% dplyr::select(id, person_ids), by = c("attachable_id" = "id"), relationship = "many-to-many")
    
    comments <- rbind(comments_notes, protocol_email_comments)
  } else {
    comments <- comments_notes
  }
  
  if(nrow(comments) > 0 && ncol(comments) > 0) {
    comments <- comments %>%
      dplyr::mutate(type = "comment") %>%
      dplyr::select(id, user_id, person_ids, name, content, updated_at, type, attachable_id)
    }
    
    dfs <- Filter(function(df) nrow(df) > 0, list(notes, mails, comments, user_notes, protocol_attachments))
    
    all_protocols <- if (length(dfs) > 0) {
      dplyr::bind_rows(!!!dfs)
  } else {
    # define an empty df with desired column structure, e.g. from notes
    all_protocols <- tibble::tibble(
      id = character(),
      user_id = integer(),
      person_ids = integer(),
      name = character(),
      content = character(),
      updated_at = character(),
      # Falls Datumsangabe als Text kommt
      attachments_count = integer(),
      created_at = character(),
      # Ggf. auch hier: Datumsangabe prüfen
      type = character(),
      attachable_id = character(),
      is_user_generated = logical()
    )
  }
  
  return(all_protocols)

}

#' Drop attachments without file metadata
#'
#' CentralStation occasionally returns attachment objects with no file behind
#' them (empty or failed uploads): \code{content_type} and \code{file_size}
#' come back as \code{NA}. Both columns are \code{NOT NULL} in the raw schema,
#' so an unfiltered upsert aborts the entire ingestion job. This helper removes
#' such rows before the upsert and warns, listing the affected
#' \code{crm_attachment_id}s so the anomaly stays visible.
#'
#' @param attachments A data frame of prepared attachments with at least the
#'   columns \code{crm_attachment_id}, \code{content_type} and \code{file_size}.
#' @param attachment_kind Character label used in the warning message, e.g.
#'   \code{"protocol"} or \code{"comment"}.
#' @return The input data frame with metadata-less rows removed.
#' @noRd
drop_attachments_without_file_metadata <- function(attachments, attachment_kind = "protocol") {

  # ---- start ---- #

  if (nrow(attachments) == 0) return(attachments)

  missing_metadata <- is.na(attachments$content_type) | is.na(attachments$file_size)

  if (any(missing_metadata)) {
    dropped_ids <- attachments$crm_attachment_id[missing_metadata]
    warning(sprintf(
      "Dropping %d %s attachment(s) without file metadata (content_type/file_size NA): %s",
      length(dropped_ids), attachment_kind, paste(dropped_ids, collapse = ", ")
    ))
  }

  attachments[!missing_metadata, , drop = FALSE]
}

# grepl with a single huge alternation pattern can exceed R's regex memory limit.
# This helper splits the patterns into chunks and ORs the results together.
grepl_chunked <- function(x, patterns, chunk_size = 200) {
  result <- rep(FALSE, length(x))
  for (i in seq(1, length(patterns), by = chunk_size)) {
    chunk <- patterns[i:min(i + chunk_size - 1, length(patterns))]
    pattern <- paste(chunk, collapse = "|")
    result <- result | grepl(pattern, x)
  }
  result
}

