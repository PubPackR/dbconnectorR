#' Map calls with events and update mapping table.
#'
#' This function matches MSGraph call and event records, classifies events, and updates the mapping.msgraph_call_event table.
#'
#' A call counts as external when its attendance report lists an external
#' participant or when a transcript of the same meeting and day has an external
#' speaker ([count_external_speakers()]). Teams leaves some external attendees
#' out of the attendance report although they speak in the transcript.
#'
#' @param con A PostgreSQL connection object.
#'
#' @returns No return value. Updates the database table.
#'
#' @export
#' @examples
#' msgraph_map_calls_events(con)
msgraph_map_calls_events <- function(con) {
  events <- dplyr::tbl(con, I("raw.msgraph_events")) %>% dplyr::collect()
  calls <- dplyr::tbl(con, I("raw.msgraph_calls")) %>% dplyr::collect()
  event_participants <- dplyr::tbl(con, I("raw.msgraph_event_participants")) %>% dplyr::collect()
  call_participants <- dplyr::tbl(con, I("raw.msgraph_call_participants")) %>% dplyr::collect()
  intern_contacts <- dplyr::tbl(con, I("raw.msgraph_contacts")) %>%
    dplyr::collect() %>%
    dplyr::filter(is_internal_email(email)) %>%
    dplyr::distinct(id)

  calls_with_contacts <- calls %>%
    dplyr::left_join(call_participants %>% dplyr::select(call_id, contact_id), by = c("id" = "call_id")) %>%
    dplyr::filter(!is.na(meeting_id) & meeting_id != "")

  events_with_contacts <- events %>%
    dplyr::left_join(event_participants %>% dplyr::select(event_id, contact_id), by = c("id" = "event_id")) %>%
    dplyr::filter(!is.na(meeting_id) & meeting_id != "")

  call_event_mapping <-
    dplyr::full_join(
      calls_with_contacts %>% dplyr::mutate(date = as.Date(call_start)) %>% dplyr::select(meeting_id, contact_id, call_id = id, date, call_start),
      events_with_contacts %>% dplyr::mutate(date = as.Date(event_start)) %>% dplyr::select(meeting_id, contact_id, event_id = id, date, is_canceled),
      by = c("meeting_id", "contact_id", "date")
    ) %>%
    dplyr::filter(!is.na(meeting_id) & meeting_id != "") %>%
    dplyr::distinct() %>%
    dplyr::mutate(is_internal = contact_id %in% intern_contacts$id) %>%
    dplyr::mutate(intern_invited_users = is_internal & !is.na(event_id)) %>%
    dplyr::mutate(intern_participating_users = is_internal & !is.na(call_id)) %>%
    dplyr::group_by(meeting_id, date) %>%
    dplyr::mutate(participating_users = sum(!is.na(call_id))) %>%
    dplyr::mutate(invited_users = sum(!is.na(event_id))) %>%
    dplyr::mutate(intern_invited_users = sum(intern_invited_users, na.rm = TRUE)) %>%
    dplyr::mutate(intern_participating_users = sum(intern_participating_users, na.rm = TRUE)) %>%
    dplyr::ungroup() %>%
    dplyr::mutate(extern_invited_users = invited_users - intern_invited_users) %>%
    dplyr::mutate(extern_participating_users = participating_users - intern_participating_users) %>%
    dplyr::filter(!(is_canceled & is_internal & is.na(call_start))) %>%
    dplyr::filter(!(is_canceled & intern_participating_users == 0))

  # Externer Sprecher im Transkript zaehlt wie ein externer Teilnehmer im
  # Bericht: Teams fuehrt manche Externe nicht im Anwesenheitsbericht, obwohl sie
  # im Transkript sprechen (Asana 1220382115257832). Nur Gruppen ohne Externen im
  # Bericht brauchen das Transkript; die Markierung gilt je (meeting_id, Tag) wie
  # die Teilnehmerzaehlung.
  kandidaten <- call_event_mapping$call_id[
    call_event_mapping$participating_users > 0 &
      call_event_mapping$extern_participating_users <= 0]
  calls_mit_externem_sprecher <- load_external_speaker_call_ids(con, kandidaten)
  call_event_mapping <- call_event_mapping %>%
    dplyr::group_by(meeting_id, date) %>%
    dplyr::mutate(has_external_speaker =
                    any(as.character(call_id) %in% calls_mit_externem_sprecher)) %>%
    dplyr::ungroup()

  all_events <- call_event_mapping %>%
    dplyr::select(-contact_id) %>%
    dplyr::mutate(event_class_planed = dplyr::case_when(
      extern_invited_users > 0 ~ "extern",
      intern_invited_users > 0 ~ "intern",
      TRUE ~ "not"
    )) %>%
    dplyr::mutate(event_class_call = derive_event_class_call(
      extern_participating_users, participating_users, has_external_speaker
    )) %>%
    dplyr::mutate(event_class = paste0(event_class_planed, "_planned", "_", event_class_call, "_call")) %>%
    dplyr::filter(!is.na(event_class)) %>%
    dplyr::distinct(call_id, event_id, meeting_id, event_class, event_date = date, call_start) %>%
    dplyr::mutate(full_match = !is.na(call_id) & !is.na(event_id)) %>%
    dplyr::group_by(call_id) %>%
    dplyr::mutate(has_ever_full_match_call = max(full_match, na.rm = TRUE)) %>%
    dplyr::group_by(event_id) %>%
    dplyr::mutate(has_ever_full_match_event = max(full_match, na.rm = TRUE)) %>%
    dplyr::filter(has_ever_full_match_call == full_match) %>%
    dplyr::filter(has_ever_full_match_event == full_match) %>%
    dplyr::ungroup() %>%
    dplyr::select(-has_ever_full_match_call, -has_ever_full_match_event, -full_match) %>%
    dplyr::group_by(meeting_id, event_date) %>%
    dplyr::arrange(call_start, .by_group = TRUE) %>%
    dplyr::mutate(call_number_on_date = dplyr::row_number()) %>%
    dplyr::ungroup() %>%
    dplyr::select(-call_start) %>%
    dplyr::filter(!(call_number_on_date > 1 & is.na(call_id)))

  Billomatics::postgres_upsert_data(
    con = con,
    schema = "mapping",
    table = "msgraph_call_event",
    data = all_events,
    match_cols = c("meeting_id", "event_date", "call_number_on_date"),
    delete_missing = TRUE
  )
}