#' Update CRM Lead Protocols
#'
#' Downloads and upserts CRM lead protocols, comments, and attachments.
#'
#' @param con A DBI database connection.
#' @param keys Authentication keys list containing crm key.
#' @param is_daily Logical. Whether this is a daily update.
#'
#' @return Invisibly returns NULL after successful update.
#' @export
crm_update_protocols <- function(con, keys, is_daily) {

  last_update_protocols <- dplyr::tbl(con, I("raw.crm_lead_protocols")) %>%
    dplyr::summarise(max_updated_at = max(updated_at, na.rm = TRUE)) %>%
    dplyr::collect() %>%
    dplyr::pull(max_updated_at)
  last_update_protocols <- last_update_protocols - lubridate::hours(1)

  last_update_attachments <- dplyr::tbl(con, I("raw.crm_lead_protocol_attachments")) %>%
    dplyr::summarise(max_updated_at = max(updated_at, na.rm = TRUE)) %>%
    dplyr::collect() %>%
    dplyr::pull(max_updated_at)
  last_update_attachments <- last_update_attachments - lubridate::hours(1)

  protocols <- download_and_enrich_protocols(con, keys$crm, last_update_attachments, last_update_protocols, daily_download = is_daily)

  all_users <- dplyr::tbl(con, I("raw.crm_users")) %>% dplyr::select("id", "crm_user_id") %>% dplyr::collect()
  all_leads <- dplyr::tbl(con, I("raw.crm_leads")) %>% dplyr::select("id", "crm_lead_id") %>% dplyr::collect()
  protocols_old <- dplyr::tbl(con, I("raw.crm_lead_protocols")) %>% dplyr::select(crm_protocol_id, old_updated_at = protocol_updated_at) %>% dplyr::collect()

  data <- protocols$main_table %>%
    dplyr::left_join(protocols_old, by = "crm_protocol_id") %>%
    dplyr::filter(is.na(old_updated_at) | protocol_updated_at > old_updated_at) %>%
    dplyr::select(-lead_id, -old_updated_at) %>%
    tidyr::drop_na(crm_protocol_id) %>%
    resolve_user_ids(all_users, "user_id") %>%
    dplyr::distinct(crm_protocol_id, .keep_all = TRUE)

  if(!is_daily) {
    protocols_old <- dplyr::tbl(con, I("raw.crm_lead_protocols")) %>%
      dplyr::filter(!crm_protocol_id %in% protocols$main_table$crm_protocol_id) %>%
      dplyr::filter(!is_deleted) %>%
      dplyr::collect()

    id_string <- paste(sprintf("'%s'", protocols_old$crm_protocol_id), collapse = ", ")

    query <- sprintf("UPDATE raw.crm_lead_protocols SET is_deleted = TRUE, updated_at = NOW() WHERE crm_protocol_id IN (%s)", id_string)

    # Ausführen
    DBI::dbExecute(con, query)
  }

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

    all_protocols <- dplyr::tbl(con, I("raw.crm_lead_protocols")) %>%
      dplyr::select(crm_protocol_id, id) %>%
      dplyr::collect()

    data <- protocols$main_table %>%
      tidyr::drop_na(crm_protocol_id) %>%
      resolve_lead_id(all_leads, "lead_id") %>%
      left_join(all_protocols, by = c("crm_protocol_id")) %>% mutate(protocol_id = id) %>% select(-id) %>%
      drop_na(lead_id) %>% 
      drop_na(protocol_id) %>%
      dplyr::distinct(protocol_id, lead_id)

    upsert_no_delete(con, "raw.crm_lead_protocol_relations", data, match_cols = c("protocol_id", "lead_id"))
    print("protocols mapping uploaded")

  }

  data <- protocols$comments %>%
    tidyr::drop_na(crm_comment_id) %>%
    resolve_user_ids(all_users, "user_id") %>%
    resolve_protocol_id(all_protocols, "protocol_id") %>%
    tidyr::drop_na(protocol_id) %>%
    dplyr::distinct(crm_comment_id, .keep_all = TRUE) %>%
    tidyr::drop_na(user_id)

  new_comments <- Billomatics::postgres_upsert_data(con, "raw", "crm_lead_protocol_comments", data, match_cols = c("crm_comment_id"), returning_cols = c("id", "crm_comment_id"), delete_missing = !is_daily)
  print("comments uploaded")

  data <- protocols$attachments %>%
    tidyr::drop_na(crm_attachment_id) %>%
    resolve_user_ids(all_users, "user_id") %>%
    resolve_protocol_id(all_protocols, "protocol_id") %>%
    dplyr::distinct(crm_attachment_id, .keep_all = TRUE) %>%
    tidyr::drop_na(user_id)

  upsert_delete_variable(con, "raw.crm_lead_protocol_attachments", data, match_cols = c("crm_attachment_id"), is_daily = is_daily)
  print("attachments uploaded")

  if(new_comments %>% nrow() > 0) {
    data <- protocols$comment_attachments %>%
      tidyr::drop_na(crm_attachment_id) %>%
      resolve_user_ids(all_users, "user_id") %>%
      resolve_comment_ids(new_comments, "comment_id") %>%
      tidyr::drop_na(comment_id) %>%
      dplyr::distinct(crm_attachment_id, .keep_all = TRUE) %>%
      tidyr::drop_na(user_id)

    upsert_delete_variable(con, "raw.crm_lead_protocol_comment_attachments", data, match_cols = c("crm_attachment_id"), is_daily = is_daily)
    print("comment attachments uploaded")
  }
}

download_and_enrich_protocols <- function(con, crm_key, last_update_attachments = NA, last_update_protocols = NA, daily_download = TRUE) {

  offers_billomat <- dplyr::tbl(con, I("raw.billomat_offers")) %>% dplyr::collect()
  confirmations_billomat <- dplyr::tbl(con, I("raw.billomat_confirmations")) %>% dplyr::collect()

  leads_to_update <- dplyr::tbl(con, I("raw.crm_leads")) %>% dplyr::collect()

  # We only want to download all Protocols from time to time, on a daily basis we only export the persons with changes
  if(daily_download) {
    leads_to_update <- leads_to_update %>%
      dplyr::filter(lubridate::as_datetime(lead_updated_at) >= as.Date(last_update_protocols))
  }

  new_protocols <- import_new_protocols(
      crm_key,
      leads_to_update
    ) %>%
    dplyr::mutate(is_user_generated = is.na(user_id) | is.na(name) | (user_id != "199146" & !grepl("Vorlage Note API", name)))

  protocols_new <- simplify_protocols(new_protocols) %>%
    dplyr::mutate(is_user_generated = is.na(user_id) | is.na(name) | (user_id != "199146" & !grepl("Vorlage Note API", name)))
  
  attachments <- expand_protocol_attachments(protocols_new, last_update_attachments, crm_key, daily_download = daily_download)
    
  comment_attachments <- expand_protocol_comment_attachments(crm_key, offers_billomat, confirmations_billomat, daily_download = daily_download)  
  
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
      dplyr::mutate(protocol_created_at = lubridate::ymd_hms(created_at, tz = "CET"),
             protocol_updated_at = lubridate::ymd_hms(updated_at, tz = "CET")) %>%
      dplyr::mutate(is_deleted = FALSE) %>%
      dplyr::select(
        crm_protocol_id = id,
        user_id,
        lead_id = person_ids,
        protocol_name = name,
        protocol_content = content,
        protocol_updated_at,
        protocol_created_at,
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
      attachments_count = integer(),
      protocol_type = character(),
      is_user_generated = logical(),
      is_deleted = logical()
    )

  }

  ################################-

  attachments <- attachments$attachments %>%
    dplyr::filter(protocol_id %in% main_table$crm_protocol_id) %>%
    dplyr::select(crm_attachment_id = id, user_id, protocol_id, attachment_updated_at = updated_at, attachment_created_at = created_at, filename, content_type, file_size = size, is_deleted)

  comment_attachments <- comment_attachments %>%
    dplyr::filter(comment_id %in% comments$crm_comment_id) %>%
    dplyr::select(comment_id, user_id, crm_attachment_id = id, attachment_created_at = created_at, attachment_updated_at = updated_at, filename, content_type, file_size = size, is_deleted)
  
  ################################-

  new_tables <- list(main_table = main_table,
                     comments = comments,
                     attachments = attachments,
                     comment_attachments = comment_attachments)
  
  return(new_tables)
  
}

expand_protocol_attachments <- function(protocols_simplified, last_update_attachments, crm_api_key, daily_download = TRUE){

  protocols_to_update <- protocols_simplified %>%
    dplyr::filter(attachments_count > 0)

  if (daily_download) {
    protocols_to_update <- protocols_to_update %>%
      dplyr::filter(lubridate::as_datetime(updated_at, tz = "CET") >= last_update_attachments)
  }

  print(paste0("Update Protocols for ", nrow(protocols_to_update), " Protocols"))

  if (nrow(protocols_to_update) > 0) {

    # download missing attachments
    attachments_new <- Billomatics::get_central_station_attachments(crm_api_key, as.vector(protocols_to_update$id)) %>%
      conditional_unnest("attachment", "_")

    colnames(attachments_new) <- gsub("attachment_", "", names(attachments_new))

    attachments <- attachments_new %>%
      dplyr::select(-account_id,
             -attachable_type,
             -category_id) %>%
      dplyr::rename(protocol_id = attachable_id) %>%
      dplyr::mutate(created_at = lubridate::ymd_hms(created_at, tz = "CET"),
             updated_at = lubridate::ymd_hms(updated_at, tz = "CET"),
             protocol_id = as.character(protocol_id)) %>%
      dplyr::mutate(is_deleted = FALSE)

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

expand_protocol_comment_attachments <- function(crm_api_key, offers_billomat, confirmations_billomat, daily_download = TRUE){

  offer_numbers <- offers_billomat %>%
    dplyr::distinct(offer_number) %>%
    dplyr::ungroup() %>%
    dplyr::select(offer_number) %>%
    dplyr::filter(grepl("AN20", offer_number)) %>%
    dplyr::mutate(offer_number = substring(offer_number, 1, nchar(offer_number) - 2)) %>%
    dplyr::distinct(offer_number) %>%
    dplyr::arrange(offer_number)

  if (daily_download) {
    offer_numbers <- offer_numbers %>%
        tail(3)
  }

  confirmation_numbers <- confirmations_billomat %>%
    dplyr::distinct(confirmation_number) %>%
    dplyr::ungroup() %>%
    dplyr::select(confirmation_number) %>%
    dplyr::filter(grepl("AB20", confirmation_number)) %>%
    dplyr::mutate(confirmation_number = substring(confirmation_number, 1, nchar(confirmation_number) - 2)) %>%
    dplyr::distinct(confirmation_number) %>%
    dplyr::arrange(confirmation_number)

  if (daily_download) {
    confirmation_numbers <- confirmation_numbers %>%
        tail(3)
  }

  repeat {
    an_attachments <- get_searched_attachments(crm_api_key, offer_numbers$offer_number)
    ab_attachments <- get_searched_attachments(crm_api_key, confirmation_numbers$confirmation_number)

    comment_attachments_new <- dplyr::bind_rows(an_attachments, ab_attachments) %>%
      tidyr::unnest()

    if ("attachable_id" %in% names(comment_attachments_new)) {
      break
    } else {
      warning("⚠ attachable_id missing, retrying in 5 minutes...")
      Sys.sleep(5 * 60) # wait 5 minutes before retrying
    }
  }

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

  return(comment_attachments_new)

}

import_new_protocols <- function(crm_key, leads_to_update) {
  
  column_names <- c(
    "id", "user_id", "attachments_count", "comments_count", "name", "confidential", "format", "content", "updated_by_user_id", "created_at", "updated_at", "data_hash",
    "account_id", "type", "badge", "person_ids", "company_ids", "deal_ids", "project_ids", "person_id", "company_id", "comments", "protocol_attachment_id",
    "protocol_attachment_user_id", "protocol_attachment_attachments_count", "protocol_attachment_comments_count", "protocol_attachment_name",
    "protocol_attachment_confidential", "protocol_attachment_format", "protocol_attachment_content", "protocol_attachment_updated_by_user_id",
    "protocol_attachment_created_at", "protocol_attachment_updated_at", "protocol_attachment_data_hash", "protocol_attachment_account_id",
    "protocol_attachment_type", "protocol_attachment_badge", "protocol_attachment_person_ids", "protocol_attachment_company_ids",
    "protocol_attachment_deal_ids", "protocol_attachment_project_ids", "protocol_attachment_person_id", "protocol_attachment_company_id",
    "protocol_attachment_comments", "protocol_email_id", "protocol_email_user_id", "protocol_email_attachments_count", "protocol_email_comments_count",
    "protocol_email_name", "protocol_email_confidential", "protocol_email_format", "protocol_email_content", "protocol_email_updated_by_user_id",
    "protocol_email_created_at", "protocol_email_updated_at", "protocol_email_data_hash_from", "protocol_email_data_hash_to",
    "protocol_email_data_hash_cc", "protocol_email_data_hash_bcc", "protocol_email_data_hash_subject", "protocol_email_data_hash_time",
    "protocol_email_data_hash_inbound_email_id", "protocol_email_data_hash_mail_part", "protocol_email_account_id", "protocol_email_type", "protocol_email_badge",
    "protocol_email_person_ids", "protocol_email_company_ids", "protocol_email_deal_ids", "protocol_email_project_ids", "protocol_email_person_id", "protocol_email_company_id",
    "protocol_email_comments", "protocol_user_note_id", "protocol_user_note_user_id", "protocol_user_note_attachments_count", "protocol_user_note_comments_count",
    "protocol_user_note_name", "protocol_user_note_confidential", "protocol_user_note_format", "protocol_user_note_content",
    "protocol_user_note_updated_by_user_id", "protocol_user_note_created_at", "protocol_user_note_updated_at", "protocol_user_note_data_hash",
    "protocol_user_note_account_id", "protocol_user_note_type", "protocol_user_note_badge", "protocol_user_note_person_ids",
    "protocol_user_note_company_ids", "protocol_user_note_deal_ids", "protocol_user_note_project_ids", "protocol_user_note_person_id",
    "protocol_user_note_company_id", "protocol_user_note_comments", "protocol_user_note", "user_generated"
  )
  
  # Print the number of persons for whom protocols are being downloaded
  print(paste0("Downloading Protocols for ", nrow(leads_to_update), " Persons"))

  protocols_all <- c()

  # Check if there are persons to update
  if (nrow(leads_to_update) > 0) {

    # IDs in Batches von 1000 aufteilen
    id_batches <- split(leads_to_update$crm_lead_id, ceiling(seq_along(leads_to_update$crm_lead_id) / 1000))

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
    protocols_new_list <- purrr::map(id_batches, process_protocols_batch)

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
    # create an response with httr and the GET function. In the function you paste
    # url and your endpoint and you also need the headers
    tryCatch({
      start_time <- Sys.time()
      response <-
        httr::GET(
          # set url with "page=" so you can add endpoints and defined filter
          paste0(
            "https://api.centralstationcrm.net/api/attachments/search?filename=", i
          ),
          httr::add_headers(headers)
        )

      data <- jsonlite::fromJSON(httr::content(response, "text")) %>%
        dplyr::select(-data)
      end_time <- Sys.time()
      end_time - start_time
      attachments <- dplyr::bind_rows(attachments, data)
    },
    error = function(cond) {
      # Choose a return value in case of error
      NA
    })
    print(i)
  }

  return(attachments)
}

simplify_protocols <- function(protocols) {
  
  notes <- protocols %>%
    tidyr::unnest(person_ids) %>% 
    dplyr::select(id, user_id, person_ids, name, content, updated_at, attachments_count, created_at) %>% 
    dplyr::mutate(id = as.character(id),
           created_at = as.character(created_at),
           updated_at = as.character(updated_at)) %>% 
    dplyr::mutate(type = "note")
  
  protocol_attachments <- protocols %>%
    tidyr::unnest(protocol_attachment_person_ids) %>% 
    dplyr::select(protocol_attachment_id, protocol_attachment_user_id, protocol_attachment_person_ids, protocol_attachment_content, protocol_attachment_name, protocol_attachment_updated_at, protocol_attachment_attachments_count, protocol_attachment_created_at) %>% 
    dplyr::rename(id = protocol_attachment_id,
           user_id = protocol_attachment_user_id,
           person_ids = protocol_attachment_person_ids,
           name = protocol_attachment_name,
           content = protocol_attachment_content,
           created_at = protocol_attachment_created_at,
           updated_at = protocol_attachment_updated_at,
           attachments_count = protocol_attachment_attachments_count) %>%
    dplyr::mutate(type = "protocol_attachment",
                  id = as.character(id),
                  name = as.character(name))
  
  user_notes <- protocols %>%
    tidyr::unnest(protocol_user_note_person_ids) %>% 
    dplyr::select(protocol_user_note_id, protocol_user_note_user_id, protocol_user_note_person_ids, protocol_user_note_content, protocol_user_note_name, protocol_user_note_updated_at, protocol_user_note_attachments_count, protocol_user_note_created_at) %>% 
    dplyr::rename(id = protocol_user_note_id,
           user_id = protocol_user_note_user_id,
           person_ids = protocol_user_note_person_ids,
           name = protocol_user_note_name,
           content = protocol_user_note_content,
           created_at = protocol_user_note_created_at,
           updated_at = protocol_user_note_updated_at,
           attachments_count = protocol_user_note_attachments_count) %>%
    dplyr::mutate(type = "user_notes",
                  id = as.character(id))
    
  mails <- protocols %>%
    tidyr::unnest(protocol_email_person_ids) %>% 
    dplyr::select(protocol_email_id, protocol_email_user_id, protocol_email_person_ids, protocol_email_name, protocol_email_content, protocol_email_updated_at, protocol_email_attachments_count, protocol_email_created_at) %>% 
    dplyr::rename(id = protocol_email_id,
           user_id = protocol_email_user_id,
           person_ids = protocol_email_person_ids,
           name = protocol_email_name,
           content = protocol_email_content,
           created_at = protocol_email_created_at,
           updated_at = protocol_email_updated_at,
           attachments_count = protocol_email_attachments_count) %>% 
    dplyr::mutate(id = as.character(id)) %>%
    dplyr::mutate(type = "email")
  
  comments_notes <- protocols %>%
    dplyr::mutate(comments = sapply(comments, as.list)) %>%
    dplyr::select(comments) %>%
    dplyr::filter(comments != "NA") %>%
    tidyr::drop_na(comments) %>% 
    tidyr::unnest_wider(comments) 
  
  if(nrow(comments_notes) > 0) {
    comments_notes <- comments_notes %>% 
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
    tidyr::unnest()
  
  if(ncol(protocol_email_comments) > 1) {
    protocol_email_comments <- protocol_email_comments %>% 
      dplyr::mutate(id = as.character(id), content = name) %>%
      dplyr::left_join(mails %>% dplyr::select(id, person_ids), by = c("attachable_id" = "id"), relationship = "many-to-many")
    
    comments <- rbind(comments_notes, protocol_email_comments)
  } else {
    comments <- comments_notes
  }
  
  if(nrow(comments) > 0) {
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

