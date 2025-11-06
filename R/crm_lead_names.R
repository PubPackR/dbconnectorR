#' Update CRM Lead Names Table
#'
#' This function updates the `raw.crm_lead_names` table in the database by downloading new and updated lead names from Central Station CRM.
#'
#' @param con A DBI connection object to the PostgreSQL database.
#' @param crm_key Authentication key for Central Station CRM API.
#' @param is_daily Logical. If TRUE, performs a daily incremental update; if FALSE, performs a full update.
#'
#' @return Invisibly returns the result of the upsert operation.
#'
#' @export
#'
#' @examples
#' \dontrun{
#'   crm_update_lead_names(con, crm_key, TRUE)
#' }
crm_update_lead_names <- function(con, crm_key, is_daily) {
  last_update_lead_names <- dplyr::tbl(con, I("raw.crm_lead_names")) %>%
    dplyr::summarise(max_added_at = max(name_added_at, na.rm = TRUE)) %>%
    dplyr::collect() %>%
    dplyr::pull(max_added_at)

  lead_names <- download_and_enrich_lead_names(con, crm_key, last_update_lead_names, daily_download = is_daily)

  lead_names_clean <- lead_names %>%
    dplyr::group_by(lead_id, name_added_at) %>%
    dplyr::arrange(is.na(name_removed_at), name_removed_at, .by_group = TRUE) %>%
    dplyr::slice_tail(n = 1) %>%
    dplyr::ungroup()

  Billomatics::postgres_upsert_data(con, schema = "raw", table = "crm_lead_names", data = lead_names_clean, match_cols = c("lead_id", "name_added_at"))
}

download_and_enrich_lead_names <- function(con, crm_key, last_update_lead_names = NA, daily_download = TRUE) {
  
  ################################################################################-
  # ----- Load DB-Tables -----
  
  all_leads <- dplyr::tbl(con, I("raw.crm_leads")) %>%
    dplyr::select(id, crm_lead_id, lead_updated_at) %>%
    dplyr::collect()

  old_names <- dplyr::tbl(con, I("raw.crm_lead_names")) %>%
    dplyr::collect()

  leads_to_update <- dplyr::tbl(con, I("raw.crm_leads")) %>% dplyr::collect()
  
  ################################################################################-
  # ----- Download and Enrich Lead Names -----

  upper_time_boarder <- Sys.time()
  
  # We only want to download all Protocols from time to time, on a daily basis we only export the persons with changes
  if(daily_download) {
    leads_to_update <- leads_to_update %>%
      dplyr::filter(lubridate::as_datetime(lead_updated_at, tz = "UTC") >= lubridate::as_datetime(last_update_lead_names, tz = "UTC"))
  } else {
    last_update_lead_names <- lubridate::ymd_hms("2018-01-01 00:00:00")
  }
  
  if (nrow(leads_to_update) > 0) {
    activities <- expand_activities_new(crm_key, persons_to_update = leads_to_update$crm_lead_id, lubridate::as_datetime(last_update_lead_names, tz = "UTC"), upper_time_boarder)
  } else {
    activities <- NULL
  }
  
  if (!is.null(activities)) {
    
    lead_names <- create_changelog(activities, daily_download) %>%
      dplyr::select(lead_id, first_name_new, name_new = last_name_new, name_added_at = added_at, name_removed_at = removed_at)

    old_names <- old_names %>%
      dplyr::filter(is.na(name_removed_at)) %>%
      dplyr::left_join(all_leads %>% dplyr::select(id, crm_lead_id), by = c("lead_id" = "id")) %>%
      dplyr::filter(crm_lead_id %in% lead_names$lead_id) %>%
      dplyr::select(first_name_new, name_new, name_added_at, lead_id = crm_lead_id, test_id = lead_id)
    
    if (nrow(lead_names) < 1) {
      
      # hier leeres df einfügen
      lead_names <- tibble::tibble(
        lead_id = integer(),
        lead_name = character(),
        name_added_at = as.POSIXct(character()),
        name_removed_at = as.POSIXct(character())
      )

    } else {
    
      data <- lead_names %>%
        dplyr::mutate(first_name_new = as.character(first_name_new),
               name_new = as.character(name_new)) %>%
        dplyr::bind_rows(old_names) %>%
        dplyr::group_by(lead_id) %>%
        dplyr::mutate(name_added_at = dplyr::coalesce(name_added_at, min(name_added_at, na.rm = TRUE))) %>%
        dplyr::ungroup() %>%
        dplyr::group_by(lead_id, name_added_at) %>%
        dplyr::mutate(first_name_new = ifelse(is.na(first_name_new) | first_name_new == "", min(first_name_new, na.rm = TRUE), first_name_new),
               name_new = ifelse(is.na(name_new) | name_new == "", min(name_new, na.rm = TRUE), name_new)) %>%
        dplyr::ungroup() %>%
        dplyr::filter(is.na(test_id)) %>%
        dplyr::select(-test_id) %>%
        dplyr::left_join(all_leads, by = c("lead_id" = "crm_lead_id")) %>% dplyr::mutate(lead_id = id) %>% dplyr::select(-id) %>%
        dplyr::select(-lead_updated_at)
    
    }
    
  } else {
    # hier leeres df einfügen
    lead_names <- tibble::tibble(
      lead_id = integer(),
      lead_name = character(),
      name_added_at = as.POSIXct(character()),
      name_removed_at = as.POSIXct(character())
    )

  }
  
}

#' create_changelog
#'
#' Create a changelog for Ansprechpartner changes based on the change of the first_name
#' of the Ansprechpartner in CRM from CRM/changelogs/missing_activities data.
#' This function also creates a second table, that tracks changes in either only the
#' first oder the last name.
#' 
#' Afterwards the newly created tables are saved tp ../base-data/CRM/changelogs/ folder
#' as RDS files.
#'
#' @param missing_activities input_argument_description
#' @return return_value_description
create_changelog <- function(missing_activities, daily_download) {
  # ---- start ---- #

  placeholder_date <- lubridate::ymd_hms("2018-01-01 00:00:00")

  ### ----- dataframe of name changes of existing leads -----
  if ("activity_change_person" %in% names(missing_activities)) {
    
    missing_activities <- missing_activities %>%
      dplyr::ungroup() %>%
      dplyr::filter(activity_attachable_type == "Person") %>%
      tidyr::unnest(activity_change_person, sep = ".", keep_empty = TRUE) %>%
      dplyr::select(-tidyselect::any_of(c("activity_change_task", "activity_change_protocol")))
      
      if ("activity_change_person.change" %in% names(missing_activities)) {
        missing_activities <- missing_activities %>%
          tidyr::unnest(activity_change_person.change, names_sep = ".", keep_empty = TRUE) %>%
          dplyr::filter(activity_change_person.change.cause == "name")
      }

      if ("activity_change_person.changes" %in% names(missing_activities)) {
        missing_activities <- missing_activities %>%
          tidyr::unnest(activity_change_person.changes, names_sep = ".", keep_empty = TRUE) %>%
          tidyr::unnest_wider(activity_change_person.changes.first_name, names_sep = ".") %>%
          tidyr::unnest_wider(activity_change_person.changes.name, names_sep = ".")
      }
    
  }
  
  if ("changes" %in% names(missing_activities)) {
    missing_activities <- missing_activities %>%
      tidyr::unnest(changes, names_sep = ".", keep_empty = TRUE)

    if ("changes.first_name" %in% names(missing_activities)) {
      missing_activities <- missing_activities %>%
        tidyr::unnest_wider(changes.first_name, names_sep = ".")
    }

    if ("changes.name" %in% names(missing_activities)) {
      missing_activities <- missing_activities %>%
        tidyr::unnest_wider(changes.name, names_sep = ".")
    }

  }
  
  if ("activity_change_changes" %in% names(missing_activities)) {
    missing_activities <- missing_activities %>%
      tidyr::unnest(activity_change_changes, names_sep = ".", keep_empty = TRUE)

      if ("activity_change_changes.first_name" %in% names(missing_activities)) {
        missing_activities <- missing_activities %>%
          tidyr::unnest_wider(activity_change_changes.first_name, names_sep = ".")
      }

      if ("activity_change_changes.name" %in% names(missing_activities)) {
        missing_activities <- missing_activities %>%
          tidyr::unnest_wider(activity_change_changes.name, names_sep = ".")
      }

  }
  
  cols <- c("activity_change_person.changes.first_name.1" = NA, "activity_change_person.changes.first_name.2" = NA, 
            "activity_change_changes.first_name.1" = NA, "activity_change_changes.first_name.2" = NA, 
            "activity_change_person.changes.name.1" = NA, "activity_change_person.changes.name.2" = NA, 
            "activity_change_changes.name.2" = NA, "activity_change_changes.name.1" = NA,
            "changes.name.2" = NA, "changes.name.1" = NA,
            "changes.first_name.2" = NA, "changes.first_name.1" = NA)
  
  lead_name_changes <- missing_activities %>%
    tibble::add_column(!!!cols[!names(cols) %in% names(.)]) %>%
    dplyr::rename(person_id = activity_attachable_id,
           updated_at = activity_updated_at) %>%
    dplyr::mutate(first_name_new = dplyr::coalesce(changes.first_name.2,activity_change_person.changes.first_name.2,activity_change_changes.first_name.2),
           first_name_old = dplyr::coalesce(changes.first_name.1,activity_change_person.changes.first_name.1,activity_change_changes.first_name.1),
           last_name_new = dplyr::coalesce(changes.name.2,activity_change_person.changes.name.2,activity_change_changes.name.2),
           last_name_old = dplyr::coalesce(changes.name.1,activity_change_person.changes.name.1,activity_change_changes.name.1)) %>%
    dplyr::select(-tidyselect::all_of(c("activity_change_person.changes.first_name.2",
              "activity_change_changes.first_name.2",
              "activity_change_person.changes.first_name.1",
              "activity_change_changes.first_name.1",
              "activity_change_person.changes.name.2",
              "activity_change_changes.name.1",
              "activity_change_person.changes.name.1",
              "activity_change_changes.name.2",
              "activity_created_at", "changes.name.2", "changes.name.1", "changes.first_name.2", "changes.first_name.1"
    ))) %>%
    dplyr::mutate(updated_at = lubridate::as_datetime(updated_at, tz = "Europe/Berlin")) %>%
    dplyr::mutate(first_name_change = ifelse(first_name_new != first_name_old, TRUE, FALSE),
           last_name_change = ifelse(last_name_new != last_name_old, TRUE, FALSE)) %>%
    dplyr::distinct(.keep_all = TRUE) %>%
    # if only first or last name in table, than only keep this part of the name
    #unite(lead_natural_name, c(first_name_new, last_name_new), sep = " ", remove = FALSE, na.rm = TRUE) %>%
    dplyr::filter(((!is.na(first_name_new) & first_name_new != "") | (!is.na(last_name_new) & last_name_new != ""))) %>%
    dplyr::rowwise() %>%
    dplyr::mutate(first_name_new = ifelse(is.na(first_name_new), stringr::str_trim(gsub(last_name_new, "", activity_name, fixed = TRUE)), first_name_new)) %>%
    dplyr::mutate(last_name_new = ifelse(is.na(last_name_new), stringr::str_trim(gsub(first_name_new, "", activity_name, fixed = TRUE)), last_name_new)) %>%
    dplyr::ungroup()

  ### ----- dataframe of initial natural name per Lead -----
  if(daily_download) {
    
    first_activity_per_lead <- lead_name_changes %>%
      dplyr::group_by(person_id) %>%
      dplyr::filter(updated_at == min(updated_at)) %>%
      dplyr::ungroup() %>%
      dplyr::mutate(updated_at = placeholder_date) %>%
      dplyr::select(activity_id, person_id, first_name_new = first_name_old, last_name_new = last_name_old, updated_at)
    
  } else {
  
    first_activity_per_lead <- missing_activities %>%
      tidyr::drop_na(activity_name) %>%
      dplyr::mutate(first_name_new = stringr::str_replace(activity_name, "\\s+[^\\s]+$", ""),
             last_name_new = stringr::str_extract(activity_name, "[^\\s]+$")) %>%
      dplyr::rename(person_id = activity_attachable_id,
             updated_at = activity_updated_at) %>%
      dplyr::ungroup() %>%
      dplyr::mutate(updated_at = lubridate::as_datetime(updated_at, tz = "Europe/Berlin")) %>%
      dplyr::group_by(person_id) %>%
      dplyr::arrange(updated_at) %>%
      dplyr::slice_head(n = 1) %>%
      dplyr::ungroup() %>%
      dplyr::select(activity_id, person_id, first_name_new, last_name_new, updated_at)
  
  }
  
  ### ----- Combine both dataframes to create changelog -----
  asp_changelog <- lead_name_changes %>%
    #dplyr::filter(first_name_change) %>%
    dplyr::select(activity_id, person_id, first_name_new, last_name_new, updated_at) %>%
    dplyr::bind_rows(first_activity_per_lead) %>%
    dplyr::arrange(updated_at) %>%
    dplyr::group_by(activity_id, person_id, first_name_new, last_name_new, updated_at) %>%
    dplyr::distinct(activity_id, person_id, first_name_new, last_name_new, updated_at, .keep_all = TRUE) %>%
    dplyr::ungroup() %>%
    dplyr::rename(id = activity_id,
           lead_id = person_id) %>%
    dplyr::mutate(lead_id = as.integer(lead_id))

  asp_changelog <- asp_changelog %>%
    #dplyr::rename(lead_id = person_id) %>%
    dplyr::arrange(lead_id, updated_at) %>%
    dplyr::group_by(lead_id) %>%
    dplyr::mutate(removed_at = lead(updated_at)) %>%
    dplyr::ungroup() %>%
    dplyr::rename(added_at = updated_at) %>%
    dplyr::mutate(lead_id = as.integer(lead_id)) %>%
    dplyr::mutate(added_at = if_else(added_at == placeholder_date, NA_POSIXct_, lubridate::ymd_hms(added_at)))
  
  return(asp_changelog)

}

expand_activities_new <- function(api_key, persons_to_update, last_update_lead_names, upper_time_boarder) {
  # if the pages are set to "all" then all pages will be downloaded
  # if pages is set to a specific number, then this number of pages will be downloaded
  # define header
  
  last_update_lead_names_str <- as.Date(last_update_lead_names)
  upper_time_boarder <- upper_time_boarder

  headers <- c(
    "content-type" = "application/json",
    "X-apikey" = api_key,
    "Accept" = "*/*"
  )


  ################################################################################
  persons <- tibble::tibble()
  
  url_base <- "https://api.centralstationcrm.net/api/activities"
  counter <- 0
  
  for (j in persons_to_update) {
    counter <- counter + 1
    page <- 1
    all_data_person <- list()
    
    last_update_lead_names_str_temp <- last_update_lead_names_str
  
    repeat {
      url <- paste0(
        url_base,
        "?perpage=250&page=", page,
        "&filter%5Bcreated_at%5D[larger_than]=", last_update_lead_names_str_temp,
        "&person_id=", j
      )
      
      response <- httr::GET(url, httr::add_headers(headers))
      content_text <- httr::content(response, "text", encoding = "UTF-8")
      
      # Retry bei "Retry later"
      if (grepl("Retry later", content_text, ignore.case = TRUE)) {
        message("Rate Limit erreicht. Warte 10 Sekunden...")
        Sys.sleep(10)
  
        # Noch ein Versuch
        response <- httr::GET(url, httr::add_headers(headers))
        content_text <- httr::content(response, "text", encoding = "UTF-8")
  
        if (grepl("Retry later", content_text, ignore.case = TRUE)) {
          warning("Auch nach Retry kein Erfolg – überspringe person_id = ", j)
          break
        }
      }
  
      # Falls Statuscode kein 200, einfach weiter
      if (response$status_code != 200) {
        warning("Fehlerhafter Statuscode: ", response$status_code, " bei person_id = ", j)
        break
      }
  
      # Prüfen, ob überhaupt JSON
      if (!startsWith(content_text, "{") && !startsWith(content_text, "[")) {
        warning("Antwort ist kein JSON: ", substr(content_text, 1, 100), " – person_id = ", j)
        break
      }
  
      # TryCatch für JSON-Parsing
      data <- tryCatch({
        jsonlite::fromJSON(content_text)
      }, error = function(e) {
        warning("Fehler beim Parsen von JSON für person_id = ", j, ": ", e$message)
        return(NULL)
      })
  
      if (is.null(data) || length(data) == 0) break
  
      all_data_person[[page]] <- data
      page <- page + 1
  
      # Pagination-Ende prüfen
      rows_returned <- if (!is.null(data$activity)) nrow(data$activity) else 0
      if (rows_returned < 250) break
  
      # Timestamp-Update für nächsten Loop
      if (!is.null(data$activity$updated_at)) {
        last_update_lead_names_str_temp <- max(data$activity$updated_at, na.rm = TRUE)
      } else {
        break
      }
    }
  
    if (length(all_data_person) > 0) {
      combined_data <- dplyr::bind_rows(all_data_person)
      persons <- dplyr::bind_rows(persons, combined_data)
    }
  
    if (counter %% 100 == 0) {
      print(paste(counter, "of", length(persons_to_update)))
      print(Sys.time())
      print(nrow(persons))
    }
  }
  
  if(nrow(persons) > 0) {
    persons_compressed <- persons %>%
      tidyr::unnest(activity, names_sep = "_") %>%
      dplyr::filter(is.na(activity_attachable_id) | activity_attachable_type == "Person") %>%
      dplyr::select(-tidyselect::any_of(c("activity_attachable", "activity_user", "activity_activity_receivers"))) %>%
      tidyr::unnest(activity_change, names_sep = "_") %>%
      dplyr::filter(activity_updated_at < upper_time_boarder) %>%
      dplyr::filter(activity_updated_at > last_update_lead_names)
    
  } else {
    persons_compressed <- c()
  }
  
  return(persons_compressed)
}

