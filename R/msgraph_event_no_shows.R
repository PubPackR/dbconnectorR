#' Classify External Events as No-Show and Update Database
#'
#' Classifies external events from `mapping.msgraph_call_event` as no-show or attended,
#' determines the responsible employee, filters shifted/large/duplicate events,
#' and upserts the result into `processed.msgraph_event_no_shows`.
#'
#' @param con A PostgreSQL database connection object.
#' @param min_date Date. Only process events from this date onwards.
#'   Defaults to 90 days ago.
#'
#' @return No return value. Updates database table `processed.msgraph_event_no_shows`.
#'
#' @export
#' @examples
#' msgraph_update_event_no_shows(con)
#' msgraph_update_event_no_shows(con, min_date = as.Date("2025-05-01"))
msgraph_update_event_no_shows <- function(con, min_date = Sys.Date() - 90) {

  # === 1. EVENTS LADEN & MAPPING-DEDUP ========================================

  message("1. Events laden...")

  # Alle extern_planned Events im Zeitraum.
  # Deduplizierung: Bei mehreren Mapping-Eintraegen pro Event (mehrere Calls)
  # den besten behalten (erfolgreich > no-show). arrange() sortiert FALSE vor TRUE,
  # d.h. nicht-no-show vor no-show -> distinct() behaelt den erfolgreichen Eintrag.
  events_classified <- dplyr::tbl(con, I("mapping.msgraph_call_event")) %>%
    dplyr::filter(event_date >= !!min_date) %>%
    dplyr::collect() %>%
    dplyr::filter(grepl("extern_planned", event_class, ignore.case = TRUE)) %>%
    dplyr::mutate(
      is_no_show_class = grepl("no_call|intern_call", event_class, ignore.case = TRUE)
    ) %>%
    dplyr::arrange(event_id, is_no_show_class) %>%
    dplyr::distinct(event_id, .keep_all = TRUE) %>%
    dplyr::select(-is_no_show_class)

  message(paste0("  ", nrow(events_classified), " Events geladen (nach Mapping-Dedup)"))

  if (nrow(events_classified) == 0) {
    message("Keine Events gefunden. Abbruch.")
    return(invisible(NULL))
  }

  # === 2. VERSCHOBENE EVENTS ==================================================

  message("2. Verschobene Events erkennen...")

  # Alle Events aus DB laden fuer ical_uid Pruefung (Ersatz-Event kann ausserhalb
  # unseres Datums-Filters liegen)
  all_db_events <- dplyr::tbl(con, I("raw.msgraph_events")) %>%
    dplyr::select(id, msgraph_ical_uid, is_canceled) %>%
    dplyr::collect()

  uids_with_active_event <- all_db_events %>%
    dplyr::filter(!is_canceled) %>%
    dplyr::distinct(msgraph_ical_uid) %>%
    dplyr::pull(msgraph_ical_uid)

  events_all <- dplyr::tbl(con, I("raw.msgraph_events")) %>%
    dplyr::filter(id %in% !!unique(events_classified$event_id)) %>%
    dplyr::select(id, subject, event_start, event_end, is_canceled, is_online_meeting, msgraph_ical_uid) %>%
    dplyr::collect()

  # Verschoben = is_canceled UND es gibt ein aktives Event mit derselben ical_uid
  verschobene_ids <- events_all %>%
    dplyr::filter(is_canceled & msgraph_ical_uid %in% uids_with_active_event) %>%
    dplyr::pull(id)

  message(paste0("  ", length(verschobene_ids), " verschobene Events"))

  # Event-Details ohne verschobene (fuer Zeitvergleiche in Duplikat-Erkennung)
  event_details <- events_all %>%
    dplyr::filter(!id %in% verschobene_ids)

  # === 3. GROSSE MEETINGS =====================================================

  message("3. Grosse Meetings erkennen...")

  participant_counts <- dplyr::tbl(con, I("raw.msgraph_event_participants")) %>%
    dplyr::filter(event_id %in% !!unique(events_classified$event_id)) %>%
    dplyr::group_by(event_id) %>%
    dplyr::summarise(participant_count = dplyr::n(), .groups = "drop") %>%
    dplyr::collect()

  large_event_ids <- participant_counts %>%
    dplyr::filter(participant_count > 15) %>%
    dplyr::pull(event_id)

  message(paste0("  ", length(large_event_ids), " grosse Meetings (>15 Teilnehmer)"))

  # === 4. DUPLIKAT-EVENTS =====================================================

  message("4. Duplikat-Events erkennen...")

  # Nur aktive Events (nicht verschoben/gross) fuer Duplikat-Check
  active_event_ids <- events_classified$event_id[
    !events_classified$event_id %in% c(verschobene_ids, large_event_ids)
  ]

  event_participants <- dplyr::tbl(con, I("raw.msgraph_event_participants")) %>%
    dplyr::filter(event_id %in% !!unique(active_event_ids)) %>%
    dplyr::collect()

  contacts <- dplyr::tbl(con, I("raw.msgraph_contacts")) %>%
    dplyr::select(id, ms_name, email) %>%
    dplyr::collect()

  # Externe Teilnehmer pro Event
  externe_teilnehmer_pro_event <- event_participants %>%
    dplyr::left_join(contacts %>% dplyr::select(id, email), by = c("contact_id" = "id")) %>%
    dplyr::filter(!is.na(email)) %>%
    dplyr::filter(!is_internal_email(email)) %>%
    dplyr::filter(!is_synthetic_email(email)) %>%
    dplyr::mutate(email_lower = tolower(email)) %>%
    dplyr::select(event_id, email_lower)

  events_mit_zeit <- events_classified %>%
    dplyr::filter(event_id %in% active_event_ids) %>%
    dplyr::left_join(event_details %>% dplyr::select(id, event_start), by = c("event_id" = "id")) %>%
    dplyr::inner_join(externe_teilnehmer_pro_event, by = "event_id") %>%
    dplyr::mutate(is_no_show_class = grepl("no_call|intern_call", event_class, ignore.case = TRUE))

  # Paarweise Duplikate: gleiche externe Email, event_start <=15 Min
  duplikat_ids <- c()
  if (nrow(events_mit_zeit) > 1) {
    events_sorted <- events_mit_zeit %>% dplyr::arrange(email_lower, event_start)
    for (i in seq_len(nrow(events_sorted) - 1)) {
      curr <- events_sorted[i, ]
      next_row <- events_sorted[i + 1, ]
      if (curr$email_lower == next_row$email_lower &&
          !is.na(curr$event_start) && !is.na(next_row$event_start) &&
          abs(as.numeric(difftime(curr$event_start, next_row$event_start, units = "mins"))) <= 15) {
        if (curr$is_no_show_class && !next_row$is_no_show_class) {
          duplikat_ids <- c(duplikat_ids, as.integer(curr$event_id))
        } else {
          duplikat_ids <- c(duplikat_ids, as.integer(next_row$event_id))
        }
      }
    }
  }
  duplikat_ids <- unique(duplikat_ids)

  message(paste0("  ", length(duplikat_ids), " Duplikat-Events"))

  # === 5. VERANTWORTLICHEN BESTIMMEN ==========================================

  message("5. Verantwortliche bestimmen...")

  # Alle Event-Participants laden (fuer alle Events, inkl. excluded)
  all_event_participants <- dplyr::tbl(con, I("raw.msgraph_event_participants")) %>%
    dplyr::filter(event_id %in% !!unique(events_classified$event_id)) %>%
    dplyr::collect()

  # Interner Organisator pro Event (erster interner Org falls mehrere)
  organizer_per_event <- all_event_participants %>%
    dplyr::filter(is_organizer == TRUE) %>%
    dplyr::left_join(contacts %>% dplyr::select(id, email), by = c("contact_id" = "id")) %>%
    dplyr::filter(is_internal_email(email)) %>%
    dplyr::group_by(event_id) %>%
    dplyr::slice(1) %>%
    dplyr::ungroup() %>%
    dplyr::select(event_id, organizer_contact_id = contact_id)

  # Call-Teilnehmer
  alle_call_ids <- events_classified %>%
    dplyr::filter(!is.na(call_id)) %>%
    dplyr::pull(call_id) %>%
    unique()

  call_participants_df <- dplyr::tbl(con, I("raw.msgraph_call_participants")) %>%
    dplyr::filter(call_id %in% !!alle_call_ids) %>%
    dplyr::left_join(
      dplyr::tbl(con, I("raw.msgraph_contacts")) %>% dplyr::select(id, email),
      by = c("contact_id" = "id")
    ) %>%
    dplyr::collect() %>%
    dplyr::mutate(is_internal = is_internal_email(email))

  # Pro Event+Call: War der Organisator im Call?
  event_call_map <- events_classified %>%
    dplyr::filter(!is.na(call_id)) %>%
    dplyr::select(event_id, call_id)

  org_in_call_events <- event_call_map %>%
    dplyr::inner_join(organizer_per_event, by = "event_id") %>%
    dplyr::semi_join(
      call_participants_df,
      by = c("call_id" = "call_id", "organizer_contact_id" = "contact_id")
    ) %>%
    dplyr::distinct(event_id) %>%
    dplyr::pull(event_id)

  # Pro Call: Erster interner Teilnehmer (Fallback wenn Org nicht im Call)
  first_internal_per_call <- call_participants_df %>%
    dplyr::filter(is_internal) %>%
    dplyr::group_by(call_id) %>%
    dplyr::slice(1) %>%
    dplyr::ungroup() %>%
    dplyr::select(call_id, fallback_contact_id = contact_id)

  # Zusammenbauen: Verantwortlicher pro Event
  verantwortliche <- events_classified %>%
    dplyr::select(event_id, call_id) %>%
    dplyr::left_join(organizer_per_event, by = "event_id") %>%
    dplyr::left_join(first_internal_per_call, by = "call_id") %>%
    dplyr::mutate(
      has_call = !is.na(call_id),
      org_was_in_call = event_id %in% org_in_call_events,
      responsible_contact_id = dplyr::case_when(
        !has_call ~ organizer_contact_id,
        org_was_in_call ~ organizer_contact_id,
        !is.na(fallback_contact_id) ~ fallback_contact_id,
        TRUE ~ organizer_contact_id
      ),
      is_organizer = dplyr::case_when(
        !has_call ~ TRUE,
        org_was_in_call ~ TRUE,
        !is.na(fallback_contact_id) &
          !is.na(organizer_contact_id) &
          fallback_contact_id != organizer_contact_id ~ FALSE,
        TRUE ~ TRUE
      )
    ) %>%
    dplyr::select(event_id, responsible_contact_id, is_organizer)

  message(paste0("  ", sum(!is.na(verantwortliche$responsible_contact_id)),
                 " / ", nrow(verantwortliche), " Events mit Verantwortlichem"))

  # === 6. ERGEBNIS ZUSAMMENBAUEN & UPSERT =====================================

  message("6. Ergebnis zusammenbauen...")

  result <- events_classified %>%
    dplyr::select(mapping_id = id, event_id, event_class) %>%
    dplyr::left_join(verantwortliche, by = "event_id") %>%
    dplyr::mutate(
      is_no_show = grepl("no_call|intern_call", event_class, ignore.case = TRUE),
      excluded = event_id %in% c(verschobene_ids, large_event_ids, duplikat_ids),
      exclusion_reason = dplyr::case_when(
        event_id %in% verschobene_ids ~ "verschoben",
        event_id %in% large_event_ids ~ "grosses_meeting",
        event_id %in% duplikat_ids ~ "duplikat_event",
        TRUE ~ NA_character_
      ),
      is_organizer = ifelse(is.na(is_organizer), TRUE, is_organizer)
    ) %>%
    dplyr::select(mapping_id, is_no_show, responsible_contact_id, is_organizer,
                  excluded, exclusion_reason)

  message(paste0("  ", nrow(result), " Events total"))
  message(paste0("  ", sum(!result$excluded), " aktiv, ", sum(result$excluded), " excluded"))
  message(paste0("  ", sum(result$is_no_show & !result$excluded), " No-Shows (aktiv)"))

  message("7. Upsert in DB...")

  Billomatics::postgres_upsert_data(
    con,
    "processed",
    "msgraph_event_no_shows",
    result,
    match_cols = "mapping_id",
    delete_missing = FALSE
  )

  message(paste0("  ", nrow(result), " Zeilen upserted in processed.msgraph_event_no_shows"))
}
