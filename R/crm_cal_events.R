  #' Download and upsert calendar events and participants from CRM
  #'
  #' Downloads calendar events for given leads, processes them, and upserts both
  #' events and participants into the database.
  #'
  #' @param con A DBI connection or pool object to the database.
  #' @param crm_key Authentication key for Central Station CRM.
  #' @param all_users Data frame of all CRM users.
  #' @param all_leads Data frame of all CRM leads.
  #'
  #' @return Invisibly returns TRUE if successful.
  #' @export
  crm_update_cal_events <- function(con, crm_key, all_users, all_leads) {
    cal_events <- download_and_enrich_cal_events(all_leads$crm_lead_id, crm_key)

    # Upsert Cal Events
    data <- cal_events$main_table %>%
      resolve_lead_id(all_leads) %>%
      resolve_user_ids(all_users, cols = "updated_by_user_id")
    upsert_no_delete(
      con,
      "raw.crm_lead_calendar_events",
      data,
      match_cols = c("crm_event_id")
    )
    all_cal_events <- dplyr::tbl(con, I("raw.crm_lead_calendar_events")) %>%
      dplyr::select("id", "crm_event_id") %>%
      dplyr::collect()

    # Upsert Cal Event Participants
    data <- cal_events$participants %>%
      dplyr::left_join(
        all_cal_events,
        by = c("calendar_event_id" = "crm_event_id")
      ) %>%
      dplyr::mutate(calendar_event_id = id) %>%
      dplyr::select(-id) %>%
      resolve_lead_id(all_leads)
    upsert_no_delete(
      con,
      "raw.crm_lead_calendar_event_participants",
      data,
      match_cols = c("calendar_event_id", "lead_id")
    )

    invisible(TRUE)
  }

  download_and_enrich_cal_events <- function(lead_ids, crm_key) {
    lead_cal_events <- Billomatics::get_central_station_cal_events(crm_key) %>%
      filter(attachable_id %in% lead_ids) %>%
      dplyr::rename(crm_event_id = id)

    participants <- lead_cal_events %>%
      dplyr::select(lead_id = people_id, calendar_event_id = crm_event_id)

    main_table <- lead_cal_events %>%
      dplyr::select(-people_id) %>%
      dplyr::mutate(
        event_created_at = ymd_hms(created_at, tz = "CET"),
        event_updated_at = ymd_hms(updated_at, tz = "CET"),
        event_starts_at = ymd_hms(starts_at, tz = "CET"),
        event_ends_at = ymd_hms(ends_at, tz = "CET")
      ) %>%
      dplyr::select(
        crm_event_id,
        lead_id = attachable_id,
        event_name = name,
        location,
        event_description = description,
        event_created_at,
        event_updated_at,
        event_starts_at,
        event_ends_at,
        updated_by_user_id
      ) %>%
      dplyr::distinct()

    new_tables <- list(main_table = main_table, participants = participants)

    return(new_tables)
  }