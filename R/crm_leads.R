#' Import and Upsert CRM Persons Data
#'
#' Downloads, processes, and upserts persons (leads) data from Central Station CRM into the database.
#'
#' @param con A DBI database connection.
#' @param crm_key Authentication key for Central Station CRM.
#' @param is_daily Logical. Whether this is a daily run.
#'
#' @return A list containing updated leads and tasks data.
#' @export
crm_update_leads <- function(con, crm_key, is_daily = TRUE) {

    last_update_tasks <- dplyr::tbl(con, I("raw.crm_lead_tasks")) %>%
      dplyr::summarise(max_updated_at = max(updated_at, na.rm = TRUE)) %>%
      dplyr::collect() %>%
      dplyr::pull(max_updated_at)
    last_update_tasks <- last_update_tasks - lubridate::hours(1)

    tags_old <- dplyr::tbl(con, I("raw.crm_lead_tags")) %>%
      dplyr::select(-updated_at) %>%
      dplyr::filter(is.na(tag_removed_at)) %>%
      dplyr::collect()

    leads_old <- dplyr::tbl(con, I("raw.crm_leads")) %>%
      dplyr::select(lead_id = id, crm_lead_id) %>%
      dplyr::collect()

    tags_old <- tags_old %>%
      dplyr::left_join(leads_old, by = c("lead_id" = "lead_id")) %>%
      dplyr::select(-lead_id) %>%
      dplyr::rename(lead_id = crm_lead_id) %>%
      dplyr::mutate(tag_added_at = lubridate::ymd_hms(tag_added_at))

    all_users <- dplyr::tbl(con, I("raw.crm_users")) %>%
      dplyr::select("id", "crm_user_id") %>%
      dplyr::collect()
    
    leads <- download_and_enrich_leads(last_update_tasks, tags_old, crm_key, daily_download = is_daily)
    
    # Upsert main table
    data <- leads$main_table %>%
      resolve_user_ids(all_users, c("responsible_user_id", "created_by_user_id", "updated_by_user_id"))
    upsert_delete_missing(con, "raw.crm_leads", data, match_cols = c("crm_lead_id"))
    all_leads <- dplyr::tbl(con, I("raw.crm_leads")) %>%
      dplyr::select("id", "crm_lead_id") %>%
      dplyr::collect()
    
    # Upsert Custom Fields
    data <- leads$custom_fields %>% resolve_lead_id(all_leads)
    upsert_delete_missing(con, "raw.crm_lead_custom_fields", data, match_cols = c("lead_id", "field_type_id"))
    
    # Upsert Tags
    data <- leads$tags %>%
      resolve_lead_id(all_leads) %>%
      dplyr::mutate(row_matching_id = paste0(gsub(" ", "_", tag), "_", format(lubridate::with_tz(tag_added_at, "UTC"), "%Y-%m-%d_%H:%M:%S"))) %>%
      tidyr::drop_na(lead_id)
    upsert_no_delete(con, "raw.crm_lead_tags", data, match_cols = c("lead_id", "row_matching_id"))
    
    # Upsert Mail Address
    old_mails <- dplyr::tbl(con, I("raw.crm_lead_mail_address")) %>%
      dplyr::select(lead_id, mail_address_type, mail_address_name, is_deleted) %>%
      dplyr::collect()
    data <- leads$mail_addrs %>%
      resolve_lead_id(all_leads) %>%
      dplyr::bind_rows(old_mails %>% dplyr::filter(mail_address_type == "from_description" & !is_deleted) %>% dplyr::select(-is_deleted)) %>%
      dplyr::mutate(is_primary = dplyr::coalesce(is_primary, FALSE)) %>%
      dplyr::mutate(is_api_input = dplyr::coalesce(is_api_input, FALSE))
    upsert_delete_missing(con, "raw.crm_lead_mail_address", data, match_cols = c("lead_id", "mail_address_name"))
    
    # Upsert Tels
    old_tels <- dplyr::tbl(con, I("raw.crm_lead_tels")) %>% dplyr::collect()
    data <- leads$tels %>%
      resolve_lead_id(all_leads) %>%
      dplyr::bind_rows(old_tels %>% dplyr::filter(tel_address_type == "from_description" & !is_deleted) %>% dplyr::select(-id, -is_deleted, -created_at, -updated_at))
    upsert_delete_missing(con, "raw.crm_lead_tels", data, match_cols = c("lead_id", "tel_name"))
    
    # Upsert Addresses
    data <- leads$addrs %>% resolve_lead_id(all_leads)
    upsert_delete_missing(con, "raw.crm_lead_address", data, match_cols = c("crm_lead_address_id"))
    
    # Upsert Homepages
    data <- leads$homepages %>% resolve_lead_id(all_leads)
    upsert_delete_missing(con, "raw.crm_lead_homepages", data, match_cols = c("lead_id", "page_name"))
    
    # Upsert Social Media Links
    data <- leads$social_media_links %>% resolve_lead_id(all_leads)
    upsert_delete_missing(con, "raw.crm_lead_social_media_links", data, match_cols = c("lead_id", "link_name"))
  
    # Upsert Connections
    data <- leads$connections %>%
      resolve_lead_id(all_leads, col = "lead_id_1") %>%
      resolve_lead_id(all_leads, col = "lead_id_2") %>%
      resolve_user_ids(all_users, c("created_by_user_id", "updated_by_user_id")) %>%
      dplyr::mutate(
        lead_id_1_new = if_else(lead_id_1 > lead_id_2, lead_id_2, lead_id_1),
        lead_id_2_new = if_else(lead_id_1 > lead_id_2, lead_id_1, lead_id_2)
      ) %>%
      dplyr::select(-lead_id_1, -lead_id_2) %>%
      dplyr::rename(lead_id_1 = lead_id_1_new, lead_id_2 = lead_id_2_new)
    upsert_delete_missing(con, "raw.crm_lead_connections", data, match_cols = c("lead_id_1", "lead_id_2"))
    
    # Upsert Tasks
    data <- leads$tasks %>%
      resolve_lead_id(all_leads) %>%
      resolve_user_ids(all_users, c("user_id", "created_by_user_id", "updated_by_user_id", "assigned_to_user_id")) %>%
      dplyr::select(-tidyselect::any_of(c("error", "description"))) %>%
      dplyr::filter(!is.na(precise_time))
    upsert_delete_missing(con, "raw.crm_lead_tasks", data, match_cols = c("crm_task_id", "lead_id"))
    all_tasks <- dplyr::tbl(con, I("raw.crm_lead_tasks")) %>%
      dplyr::select("id", "crm_task_id", "lead_id") %>%
      dplyr::collect()
    
    if (nrow(leads$task_comments) > 0) {
      # Upsert Task Comments
      data <- leads$task_comments %>%
        resolve_lead_id(all_leads) %>%
        dplyr::mutate(crm_task_id = as.integer(crm_task_id)) %>%
        dplyr::left_join(all_tasks, by = c("crm_task_id" = "crm_task_id", "lead_id")) %>%
        dplyr::mutate(task_id = id) %>%
        dplyr::select(-id, -crm_task_id, -lead_id) %>%
        dplyr::mutate(crm_user_id = as.integer(crm_user_id)) %>%
        resolve_user_ids(all_users) %>%
        dplyr::rename(user_id = crm_user_id) %>%
        tidyr::drop_na(task_id)
      upsert_delete_variable(con, "raw.crm_lead_task_comments", data, match_cols = c("crm_comment_id"), is_daily = is_daily)
    }
    
    return(leads)
}

download_and_enrich_leads <- function(last_update_tasks, tags_old, crm_key, daily_download = TRUE) {

  leads <- Billomatics::get_central_station_contacts(crm_key)

  if(daily_download) {
    last_update_tasks <- last_update_tasks
  } else {
    last_update_tasks <- lubridate::ymd_hms("2018-01-01 00:00:00")
  }

  tasks <- get_tasks_from_leads(leads)

  task_comments <- import_task_comments(tasks, last_update_tasks, crm_key)

  positions <- leads %>%
    dplyr::select(positions) %>%
    tidyr::unnest("positions") %>%
    dplyr::select(-account_id) %>%
    dplyr::rename(lead_id = person_id,
           crm_position_id = id,
           company_id = company_id,
           position_name = name,
           position_created_at = created_at,
           position_updated_at = updated_at) %>%
    dplyr::mutate(position_created_at = lubridate::ymd_hms(position_created_at, tz = "CET"),
           position_updated_at = lubridate::ymd_hms(position_updated_at, tz = "CET"),
           is_deleted = FALSE) %>%
    dplyr::rename(is_api_input = api_input,
           is_lead_primary_function = primary_function,
           is_former_position = former) %>%
    dplyr::distinct()
  
  custom_fields <- leads %>%
    dplyr::select(custom_fields) %>%
    tidyr::unnest("custom_fields") %>%
    dplyr::select(-attachable_type) %>%
    dplyr::rename(lead_id = attachable_id,
           field_created_at = created_at,
           field_updated_at = updated_at,
           field_content = name,
           field_type_name = custom_fields_type_name,
           field_type_id = custom_fields_type_id,
           field_id = id,
           is_api_input = api_input) %>%
    dplyr::mutate(field_created_at = lubridate::ymd_hms(field_created_at, tz = "CET"),
           field_updated_at = lubridate::ymd_hms(field_updated_at, tz = "CET"),
           is_deleted = FALSE) %>%
    dplyr::distinct()
  
  current_tags <- leads %>%
    dplyr::select(tags) %>%
    tidyr::unnest("tags") %>%
    dplyr::select(crm_tag_id = id, attachable_id, name, updated_at) %>%
    dplyr::rename(lead_id = attachable_id) %>%
    dplyr::mutate(tag_added_at = lubridate::ymd_hms(lubridate::floor_date(lubridate::ymd_hms(updated_at), unit = "second"))) %>%
    dplyr::distinct() %>%
    dplyr::rename(tag = name) %>%
    dplyr::mutate(tag = as.character(tag)) %>%
    dplyr::mutate(tag = if_else(tag == "NA" | is.na(tag), "NA_", tag))

  new_tags <- current_tags %>%
    dplyr::left_join(tags_old, by = c("lead_id", "tag"), suffix = c("", "_old")) %>%
    dplyr::mutate(tag_added_at = if_else(is.na(crm_tag_id_old) | crm_tag_id != crm_tag_id_old, dplyr::coalesce(tag_added_at_old, tag_added_at), tag_added_at)) %>%
    dplyr::mutate(tag_added_at_old = if_else(is.na(crm_tag_id_old) | crm_tag_id != crm_tag_id_old, NA, tag_added_at_old)) %>%
    dplyr::filter(is.na(tag_added_at_old)) %>%
    dplyr::mutate(tag_removed_at = NA_Date_) %>%
    dplyr::select(crm_tag_id, lead_id, tag, tag_added_at, tag_removed_at)

  removed_tags <- tags_old %>%
    dplyr::filter(is.na(tag_removed_at)) %>%
    dplyr::left_join(current_tags, by = c("lead_id", "tag"), suffix = c("", "_new")) %>%
    dplyr::filter(is.na(tag_added_at_new)) %>%
    dplyr::mutate(tag_removed_at = Sys.Date() - 1) %>%
    dplyr::select(crm_tag_id, lead_id, tag, tag_added_at, tag_removed_at)

  changes <- dplyr::bind_rows(new_tags, removed_tags) 
  
  mail_addrs <- leads %>%
    dplyr::select(emails) %>%
    tidyr::unnest("emails") %>%
    dplyr::select(-type,
           -attachable_type,
           -name_clean) %>%
    dplyr::rename(lead_id = attachable_id,
           is_primary = primary,
           is_api_input = api_input,
           mail_created_at = created_at,
           mail_updated_at = updated_at,
           mail_address_type = atype,
           mail_address_name = name,
           crm_mail_address_id = id) %>%
    dplyr::mutate(mail_created_at = lubridate::ymd_hms(mail_created_at, tz = "CET"),
           mail_updated_at = lubridate::ymd_hms(mail_updated_at, tz = "CET")) %>%
    dplyr::mutate(is_primary = dplyr::coalesce(is_primary, TRUE)) %>%
    dplyr::mutate(is_api_input = dplyr::coalesce(is_primary, FALSE)) %>%
    dplyr::distinct()
  
  tels <- leads %>%
    dplyr::select(tels) %>%
    tidyr::unnest("tels") %>%
    dplyr::select(-type,
           -attachable_type) %>%
    dplyr::rename(lead_id = attachable_id,
           is_primary = primary,
           is_api_input = api_input,
           tel_created_at = created_at,
           tel_updated_at = updated_at,
           tel_address_type = atype,
           tel_name = name,
           tel_name_clean = name_clean,
           crm_tel_id = id) %>%
    dplyr::mutate(tel_created_at = lubridate::ymd_hms(tel_created_at, tz = "CET"),
           tel_updated_at = lubridate::ymd_hms(tel_updated_at, tz = "CET")) %>%
      dplyr::mutate(is_primary = dplyr::coalesce(is_primary, TRUE)) %>%
    dplyr::mutate(is_api_input = dplyr::coalesce(is_primary, FALSE)) %>%
    dplyr::distinct()
  
  addrs <- leads %>%
    dplyr::select(addrs) %>%
    tidyr::unnest("addrs") %>%
    dplyr::select(-attachable_type) %>%
    dplyr::rename(lead_id = attachable_id,
           is_primary = primary,
           is_api_input = api_input,
           crm_lead_address_id = id,
           address_created_at = created_at,
           address_updated_at = updated_at,
           address_type = atype) %>%
    dplyr::mutate(address_created_at = lubridate::ymd_hms(address_created_at, tz = "CET"),
           address_updated_at = lubridate::ymd_hms(address_updated_at, tz = "CET")) %>%
    dplyr::mutate(is_primary = dplyr::coalesce(is_primary, TRUE)) %>%
    dplyr::mutate(is_api_input = dplyr::coalesce(is_primary, FALSE)) %>%
    dplyr::distinct()
       
  homepages <- leads %>%
    dplyr::select(homepages) %>%
    tidyr::unnest("homepages") %>%
    dplyr::select(-type,
           -attachable_type,
           -name_clean) %>%
    dplyr::rename(lead_id = attachable_id,
           is_primary = primary,
           is_api_input = api_input,
           page_created_at = created_at,
           page_updated_at = updated_at,
           page_type = atype,
           page_name = name,
           crm_page_id = id) %>%
    dplyr::mutate(page_created_at = lubridate::ymd_hms(page_created_at, tz = "CET"),
           page_updated_at = lubridate::ymd_hms(page_updated_at, tz = "CET")) %>%
    dplyr::mutate(is_primary = dplyr::coalesce(is_primary, TRUE)) %>%
    dplyr::mutate(is_api_input = dplyr::coalesce(is_primary, FALSE)) %>%
    dplyr::distinct()
  
  social_media_links <- leads %>%
    dplyr::select(sms) %>%
    tidyr::unnest("sms") %>%
    dplyr::select(-type,
           -attachable_type,
           -name_clean) %>%
    dplyr::rename(lead_id = attachable_id,
           is_primary = primary,
           is_api_input = api_input,
           link_created_at = created_at,
           link_updated_at = updated_at,
           link_type = atype,
           link_name = name,
           crm_link_id = id) %>%
    dplyr::mutate(link_created_at = lubridate::ymd_hms(link_created_at, tz = "CET"),
           link_updated_at = lubridate::ymd_hms(link_updated_at, tz = "CET")) %>%
    dplyr::mutate(is_primary = dplyr::coalesce(is_primary, TRUE)) %>%
    dplyr::mutate(is_api_input = dplyr::coalesce(is_primary, FALSE)) %>%
    dplyr::distinct()

  connections <- leads %>%
    dplyr::select(connections) %>%
    tidyr::unnest("connections") %>%
    dplyr::select(
      crm_connection_id = id,
      lead_id_1 = object_1_id,
      lead_id_2 = object_2_id,
      connection_quantity = quantity,
      connection_quality = quality,
      created_by_user_id,
      updated_by_user_id,
      connection_created_at = created_at,
      connection_updated_at = updated_at
    ) %>%
    dplyr::mutate(
      connection_created_at = lubridate::ymd_hms(connection_created_at, tz = "CET"),
      connection_updated_at = lubridate::ymd_hms(connection_updated_at, tz = "CET"),
      is_deleted = FALSE
    ) %>%
    dplyr::distinct()
  
  main_table <- leads %>%
    dplyr::select(-tidyselect::where(is.list)) %>%
    dplyr::select(
      -account_id,
      -group_id,
      -salutation_official,
      -salutation_formal,
      -salutation_informal,
      -backlink,
      -absolute_url,
      -absolute_avatar_url,
      -responsible_user_natural_name
    ) %>%
    dplyr::rename(crm_lead_id = id,
           responsible_user_id = user_id,
           lead_created_at = created_at,
           lead_updated_at = updated_at,
           lead_name = name,
           lead_first_name = first_name,
           lead_background = background) %>%
    dplyr::mutate(lead_created_at = lubridate::ymd_hms(lead_created_at, tz = "CET"),
           lead_updated_at = lubridate::ymd_hms(lead_updated_at, tz = "CET"),
           stream_updated_at = lubridate::ymd_hms(stream_updated_at, tz = "CET"),
           is_deleted = FALSE) %>%
    dplyr::distinct()
  
  new_tables <- list(main_table = main_table,
                     positions = positions,
                     custom_fields = custom_fields,
                     tags = changes, 
                     mail_addrs = mail_addrs,
                     tels = tels,
                     addrs = addrs,
                     homepages = homepages,
                     social_media_links = social_media_links,
                     connections = connections,
                     tasks = tasks,
                     task_comments = task_comments)
  
  return(new_tables)
  
}

get_tasks_from_leads <- function(leads) {

  persons_tasks_all <- leads %>%
    tidyr::unnest(tasks, names_sep = "_", keep_empty = TRUE) %>%
    dplyr::mutate(tasks_pending = purrr::map(tasks_pending, ~ if (length(.x) == 0 || !is.data.frame(.x)) NULL else .x)) %>%
    tidyr::unnest(tasks_pending, names_sep = "_", keep_empty = TRUE) %>%
    dplyr::mutate(tasks_updated_at = lubridate::as_datetime(tasks_updated_at, tz = "CET"),
           tasks_pending_updated_at = lubridate::as_datetime(tasks_pending_updated_at, tz = "CET"))

  tasks <- persons_tasks_all %>%
    dplyr::select(crm_task_id = tasks_id,
           user_id,
           lead_id = id,
           is_finished = tasks_finished,,
           task_badge = tasks_badge,
           comments_count = tasks_comments_count,
           task_name = tasks_name,
           precise_time = tasks_precise_time,
           created_by_user_id = tasks_created_by_user_id,
           updated_by_user_id = tasks_updated_by_user_id,
           assigned_to_user_id = tasks_pending_user_id,
           task_created_at = tasks_created_at,
           task_updated_at = tasks_updated_at)

  tasks_pending <- persons_tasks_all %>%
    dplyr::select(crm_task_id = tasks_pending_id,
           user_id,
           lead_id = id,
           is_finished = tasks_pending_finished,
           task_badge = tasks_pending_badge,
           comments_count = tasks_pending_comments_count,
           task_name = tasks_pending_name,
           precise_time = tasks_pending_precise_time,
           created_by_user_id = tasks_pending_created_by_user_id,
           updated_by_user_id = tasks_pending_updated_by_user_id,
           assigned_to_user_id = tasks_pending_user_id,
           task_created_at = tasks_pending_created_at,
           task_updated_at = tasks_pending_updated_at)

  tasks <- dplyr::bind_rows(tasks, tasks_pending) %>%
    tidyr::drop_na(crm_task_id) %>%
    dplyr::distinct() %>%
    dplyr::mutate(precise_time = as.character(precise_time)) %>%
    dplyr::mutate(precise_time = ifelse(
      nchar(precise_time) == 10,
      paste(precise_time, "00:00:00"),
      precise_time
    )) %>%
    dplyr::mutate(
      precise_time = lubridate::ymd_hms(precise_time, tz = "CET"),
      task_created_at = lubridate::ymd_hms(task_created_at, tz = "CET"),
      task_updated_at = lubridate::ymd_hms(task_updated_at, tz = "CET")
    ) %>%
    dplyr::mutate(is_deleted = FALSE) %>%
    dplyr::distinct(lead_id, crm_task_id, .keep_all = TRUE) %>%
    dplyr::mutate(created_by_user_id = ifelse(created_by_user_id == "character(0)", NA, created_by_user_id)) %>%
    dplyr::mutate(updated_by_user_id = ifelse(updated_by_user_id == "character(0)", NA, updated_by_user_id))

}

fetch_tasks_by_filter <- function(filter_vector, crm_key) {
  tasks_new <-  c()
  data <- c()
    
  headers <- c(
    "content-type" = "application/json",
    "X-apikey" = crm_key,
    "Accept" = "*/*"
  )

  for (i in seq_along(filter_vector)) {
    tryCatch({
      response <- httr::GET(
        url = paste0(
          "https://api.centralstationcrm.net/api/tasks/",
          filter_vector[i],
          "?includes=user%20comments&methods=responsible_user_natural_name"
        ),
        httr::add_headers(.headers = headers)
      )

      data <- jsonlite::fromJSON(httr::content(response, "text"))

      data_df <- as.data.frame(
        t(sapply(data, function(x) if (is.null(x)) NA else x)),
        stringsAsFactors = FALSE
      )

      tasks_new <- dplyr::bind_rows(tasks_new, data_df)
    },
    error = function(cond) {
      message(paste("Task", filter_vector[i], "does not exist or caused an error"))
      # fallback: continue gracefully
      NULL
    })

    if (i %% 100 == 0) {
      message(paste("Processed", i, "tasks"))
    }
  }

  return(tasks_new)
}

import_task_comments <- function(tasks, last_update_tasks, crm_key) {

    dummy_table <- tibble::tibble(
      comment_name = character(),
      lead_id = integer(),
      comment_created_at = as.POSIXct(character()),
      comment_updated_at = as.POSIXct(character()),
      crm_task_id = integer(),
      crm_comment_id = integer(),
      crm_user_id = integer(),
      is_deleted = logical()
    )

    #last_update <- max(lubridate::as_datetime(tasks_old$updated_at, tz = "CET"), na.rm = TRUE)
    last_update <- last_update_tasks

    person_tasks_update_comments <- tasks %>%
      dplyr::filter(comments_count > 0) %>%
      dplyr::filter(task_updated_at > last_update) %>%
      dplyr::select(crm_task_id, precise_time, task_updated_at) %>%
      dplyr::group_by(crm_task_id) %>%
      dplyr::summarise_all(last)

    filter_vector <- unique(person_tasks_update_comments$crm_task_id)

    print(length(filter_vector))

    # return when no tasks updated
    if(length(filter_vector) == 0) { return(dummy_table) }

    tasks_new <- fetch_tasks_by_filter(filter_vector, crm_key)

    # return when no comments could be fetched
    if(is.null(tasks_new)) { return(dummy_table) }

    tasks_new <- tasks_new %>%
      dplyr::mutate(comments_count = as.character(comments_count)) %>%
      dplyr::mutate(comments = purrr::map(comments, ~ if (is.list(.x)) as.data.frame(.x) else .x)) %>%
      tidyr::unnest(comments, names_sep = "_") %>%
      dplyr::select(-user)

    new_comments_available <- nrow(tasks_new) > 0

    # return when no new comments
    if (!new_comments_available) { return(dummy_table) }

    tasks_new <- as.data.frame(tasks_new)

    tasks_comments <- tasks_new %>%
      tibble::tibble() %>%
      dplyr::select(-tidyselect::any_of(c("error", "description", "task_version_id"))) %>%
      dplyr::mutate(id = as.character(id),
             comments_id = as.character(comments_id),
             comments_user_id = as.character(comments_user_id)) %>%
      dplyr::select(crm_task_id = id, crm_comment_id = comments_id, crm_user_id = comments_user_id, comment_name = comments_name,
             comments_created_at, comments_updated_at, attachable_id) %>%
      dplyr::filter(!is.na(crm_comment_id)) %>%
      dplyr::mutate(comment_created_at = lubridate::ymd_hms(comments_created_at, tz = "CET"),
             comment_updated_at = lubridate::ymd_hms(comments_updated_at, tz = "CET"),
             lead_id = as.integer(attachable_id),
             crm_task_id = as.integer(crm_task_id),
             crm_comment_id = as.integer(crm_comment_id),
             crm_user_id = as.integer(crm_user_id),
             is_deleted = FALSE) %>%
      dplyr::select(-attachable_id, -comments_created_at, -comments_updated_at) %>%
      dplyr::filter(crm_task_id %in% tasks$crm_task_id) %>%
      dplyr::distinct(crm_task_id, crm_comment_id, .keep_all = TRUE)

    if (length(tasks_comments) == 0) { return(dummy_table) } else { return(tasks_comments) }
}
