#' Classify External Events with Original Creation Date
#'
#' Classifies external events from `mapping.msgraph_call_event` as no-show or attended,
#' determines responsible contacts (one row per event-contact combination),
#' filters shifted/large/duplicate events, and correctly calculates original_created_at
#' by taking the minimum event_created_at across all events with the same msgraph_ical_uid.
#'
#' Key features:
#' - Correct original_created_at: min(event_created_at) grouped by msgraph_ical_uid
#' - Multiple rows per event: one row per (event, contact) combination
#' - is_short_lived_event flag: events canceled < 24h after creation
#' - Rescheduled meeting detection: same lead, < 2 days apart, no meeting_id
#'
#' @param con A PostgreSQL database connection object.
#' @param min_date Date. Only process events from this date onwards.
#'   Defaults to 90 days ago. Only used when `use_date_filter = TRUE`.
#' @param use_date_filter Logical. If TRUE, restrict processing to events with
#'   `event_date >= min_date`. Default FALSE (full table).
#'
#' @return No return value. Updates database table `processed.msgraph_extern_event_classification`.
#'
#' @details
#' Before writing, the function builds a sync-set as the union of (a) the freshly
#' computed classifications for events in this run and (b) the existing active
#' rows for events outside the run's scope. It then calls
#' `Billomatics::postgres_upsert_data(..., delete_missing = TRUE)`, which
#' soft-deletes any active row not in the sync-set. The net effect: stale
#' `(call_event_mapping_id, contact_id)` rows from earlier runs — where the
#' dedup later picked a different winning mapping_id for the same event — are
#' marked as deleted, while classifications for events outside the date window
#' (`use_date_filter = TRUE`) are preserved untouched.
#'
#' @export
#' @examples
#' update_extern_event_classification(con)
#' update_extern_event_classification(con, min_date = as.Date("2025-05-01"))
update_extern_event_classification <- function(con, min_date = Sys.Date() - 90, use_date_filter = FALSE) {

  # === 1. EVENTS LADEN & MAPPING-DEDUP ========================================

  message("1. Events laden...")

  # Alle extern_planned Events im Zeitraum.
  # Deduplizierung: Bei mehreren Mapping-Eintraegen pro Event (mehrere Calls)
  # den besten behalten: 1) erfolgreich > no-show, 2) laengster Call.
  # arrange() sortiert FALSE vor TRUE (nicht-no-show zuerst), dann absteigend nach Dauer.
  call_durations <- dplyr::tbl(con, I("raw.msgraph_calls")) %>%
    dplyr::select(id, call_start, call_end) %>%
    dplyr::collect() %>%
    dplyr::mutate(call_duration_sec = as.numeric(difftime(call_end, call_start, units = "secs"))) %>%
    dplyr::select(call_id = id, call_duration_sec)

  events_classified <- dplyr::tbl(con, I("mapping.msgraph_call_event")) %>%
    {if (use_date_filter) dplyr::filter(., event_date >= !!min_date) else .} %>%
    dplyr::collect() %>%
    dplyr::filter(grepl("extern_planned", event_class, ignore.case = TRUE)) %>%
    dplyr::left_join(call_durations, by = "call_id") %>%
    dplyr::mutate(
      is_no_show_class = grepl("no_call|intern_call", event_class, ignore.case = TRUE),
      call_duration_sec = dplyr::if_else(is.na(call_duration_sec), 0, call_duration_sec)
    ) %>%
    dplyr::arrange(event_id, is_no_show_class, dplyr::desc(call_duration_sec)) %>%
    dplyr::distinct(event_id, .keep_all = TRUE) %>%
    dplyr::select(-is_no_show_class, -call_duration_sec)

  message(paste0("  ", nrow(events_classified), " Events geladen (nach Mapping-Dedup)"))

  if (nrow(events_classified) == 0) {
    message("Keine Events gefunden. Abbruch.")
    return(invisible(NULL))
  }
  # === 2. ORIGINAL_CREATED_AT BERECHNEN =======================================

  message("2. Original_created_at berechnen...")

  # Alle Events aus DB laden mit event_created_at und msgraph_ical_uid
  all_events <- dplyr::tbl(con, I("raw.msgraph_events")) %>%
    dplyr::select(id, msgraph_ical_uid, event_created_at, event_updated_at,
                  event_start, event_end, is_canceled, is_online_meeting, subject) %>%
    dplyr::collect()

  # Pro msgraph_ical_uid: Minimum event_created_at berechnen
  # Dies ist das ECHTE Erstelldatum, auch wenn Events mehrfach verschoben wurden
  original_created_lookup <- all_events %>%
    dplyr::group_by(msgraph_ical_uid) %>%
    dplyr::summarise(
      original_created_at = min(event_created_at, na.rm = TRUE),
      .groups = "drop"
    )

  # Events mit original_created_at anreichern
  events_all <- all_events %>%
    dplyr::filter(id %in% unique(events_classified$event_id)) %>%
    dplyr::left_join(original_created_lookup, by = "msgraph_ical_uid")

  message(paste0("  Original_created_at fuer ", nrow(events_all), " Events berechnet"))

  # === 3. IS_SHORT_LIVED_EVENT BERECHNEN ======================================

  message("3. Short-lived Events erkennen...")

  # Short-lived = gecancelt UND < 24h zwischen original_created_at und event_updated_at
  events_all <- events_all %>%
    dplyr::mutate(
      time_to_cancellation_hours = as.numeric(
        difftime(event_updated_at, original_created_at, units = "hours")
      ),
      is_short_lived_event = is_canceled & time_to_cancellation_hours < 24
    )

  short_lived_count <- sum(events_all$is_short_lived_event, na.rm = TRUE)
  message(paste0("  ", short_lived_count, " short-lived Events (<24h)"))

  # === 4. VERSCHOBENE EVENTS ==================================================

  message("4. Verschobene Events erkennen...")

  # all_events (aus Schritt 2) enthaelt alle Events inkl. ausserhalb des Datums-Filters
  uids_with_active_event <- all_events %>%
    dplyr::filter(!is_canceled) %>%
    dplyr::distinct(msgraph_ical_uid) %>%
    dplyr::pull(msgraph_ical_uid)

  # Verschoben = is_canceled UND es gibt ein aktives Event mit derselben ical_uid
  verschobene_ids <- events_all %>%
    dplyr::filter(is_canceled & msgraph_ical_uid %in% uids_with_active_event) %>%
    dplyr::pull(id)

  message(paste0("  ", length(verschobene_ids), " verschobene Events"))

  # Event-Details ohne verschobene (fuer Zeitvergleiche in Duplikat-Erkennung)
  event_details <- events_all %>%
    dplyr::filter(!id %in% verschobene_ids)

  # === 5. INTERNE MEETINGS (ZU VIELE INTERNE) =================================

  message("5. Interne Meetings erkennen...")

  participant_counts <- dplyr::tbl(con, I("raw.msgraph_event_participants")) %>%
    dplyr::filter(event_id %in% !!unique(events_classified$event_id)) %>%
    dplyr::left_join(
      dplyr::tbl(con, I("raw.msgraph_contacts")) %>% dplyr::select(id, email),
      by = c("contact_id" = "id")
    ) %>%
    dplyr::collect() %>%
    dplyr::mutate(is_internal = is_internal_email(email)) %>%
    dplyr::group_by(event_id) %>%
    dplyr::summarise(
      participant_count = dplyr::n(),
      internal_count = sum(is_internal, na.rm = TRUE),
      external_count = sum(!is_internal, na.rm = TRUE),
      .groups = "drop"
    )

  internal_meeting_ids <- participant_counts %>%
    dplyr::filter(internal_count >= 7) %>%
    dplyr::pull(event_id)

  message(paste0("  ", length(internal_meeting_ids), " interne Meetings (>=7 interne Teilnehmer)"))

  # === 6. DUPLIKAT-EVENTS =====================================================

  message("6. Duplikat-Events erkennen...")

  # Nur aktive Events (nicht verschoben/gross) fuer Duplikat-Check
  active_event_ids <- events_classified$event_id[
    !events_classified$event_id %in% c(verschobene_ids, internal_meeting_ids)
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

  # === 7. RESCHEDULED MEETINGS OHNE MEETING_ID ERKENNEN ======================

  message("7. Rescheduled Meetings ohne meeting_id erkennen...")

  # Heuristik: Gleicher Lead (externe Email), < 2 Tage Abstand, unterschiedliche ical_uid
  # Nur unter aktiven Events (keine bereits als verschoben markierten)
  # Zusaetzlich: original_created_at des gecancelten Events auf Ersatz-Event uebertragen
  rescheduled_without_mid_ids <- c()
  rescheduled_mapping <- data.frame(
    canceled_event_id = integer(0),
    replacement_event_id = integer(0),
    stringsAsFactors = FALSE
  )

  if (nrow(events_mit_zeit) > 1) {
    # Gecancelte Events mit Lead-Info
    canceled_events <- events_mit_zeit %>%
      dplyr::inner_join(
        events_all %>% dplyr::select(id, msgraph_ical_uid, is_canceled, event_start),
        by = c("event_id" = "id"),
        suffix = c("", "_evt")
      ) %>%
      dplyr::filter(is_canceled) %>%
      dplyr::select(canceled_id = event_id, canceled_email = email_lower,
                    canceled_start = event_start_evt, canceled_ical_uid = msgraph_ical_uid)

    # Aktive Events mit Lead-Info
    active_events <- events_all %>%
      dplyr::filter(!is_canceled, id %in% active_event_ids) %>%
      dplyr::select(replacement_id = id, replacement_start = event_start,
                    replacement_ical_uid = msgraph_ical_uid) %>%
      dplyr::inner_join(externe_teilnehmer_pro_event, by = c("replacement_id" = "event_id")) %>%
      dplyr::rename(replacement_email = email_lower)

    # Vektorisierter Join: gleicher Lead, andere ical_uid, < 2 Tage
    if (nrow(canceled_events) > 0 && nrow(active_events) > 0) {
      rescheduled_pairs <- canceled_events %>%
        dplyr::inner_join(active_events, by = c("canceled_email" = "replacement_email"),
                         relationship = "many-to-many") %>%
        dplyr::filter(
          canceled_ical_uid != replacement_ical_uid,
          !is.na(canceled_start), !is.na(replacement_start)
        ) %>%
        dplyr::mutate(
          time_diff_days = abs(as.numeric(difftime(replacement_start, canceled_start, units = "days")))
        ) %>%
        dplyr::filter(time_diff_days < 2) %>%
        dplyr::group_by(canceled_id) %>%
        dplyr::slice_min(time_diff_days, n = 1, with_ties = FALSE) %>%
        dplyr::ungroup()

      rescheduled_without_mid_ids <- unique(rescheduled_pairs$canceled_id)
      rescheduled_mapping <- rescheduled_pairs %>%
        dplyr::select(canceled_event_id = canceled_id, replacement_event_id = replacement_id) %>%
        as.data.frame()
    }
  }
  message(paste0("  ", length(rescheduled_without_mid_ids), " rescheduled ohne meeting_id"))

  # original_created_at des Ersatz-Events anpassen:
  # Erbt das frueheste original_created_at aus der Kette (gecanceltes Event oder eigenes)
  if (nrow(rescheduled_mapping) > 0) {
    canceled_created <- events_all %>%
      dplyr::select(id, original_created_at) %>%
      dplyr::inner_join(rescheduled_mapping, by = c("id" = "canceled_event_id")) %>%
      dplyr::select(replacement_event_id, canceled_original_created_at = original_created_at)

    # Pro Ersatz-Event: min aus eigenem und gecanceltem original_created_at
    events_all <- events_all %>%
      dplyr::left_join(canceled_created, by = c("id" = "replacement_event_id")) %>%
      dplyr::mutate(
        original_created_at = dplyr::if_else(
          !is.na(canceled_original_created_at),
          pmin(original_created_at, canceled_original_created_at, na.rm = TRUE),
          original_created_at
        )
      ) %>%
      dplyr::select(-canceled_original_created_at)
    message(paste0("  ", nrow(canceled_created),
                   " Ersatz-Events mit angepasstem original_created_at"))
  }

  # === 8. VERANTWORTLICHE & KONTAKTE BESTIMMEN ================================

  message("8. Verantwortliche und Kontakte bestimmen...")

  # Alle Event-Participants laden (fuer alle Events, inkl. excluded)
  all_event_participants <- dplyr::tbl(con, I("raw.msgraph_event_participants")) %>%
    dplyr::filter(event_id %in% !!unique(events_classified$event_id)) %>%
    dplyr::collect()

  # Interner Organisator pro Event
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

  # Pro Call: Alle internen Teilnehmer (fuer Fallback)
  internal_per_call <- call_participants_df %>%
    dplyr::filter(is_internal) %>%
    dplyr::group_by(call_id) %>%
    dplyr::slice(1) %>%
    dplyr::ungroup() %>%
    dplyr::select(call_id, fallback_contact_id = contact_id)

  # Pro Event: Alle internen Teilnehmer (Organizer + weitere interne)
  internal_contacts_per_event <- all_event_participants %>%
    dplyr::left_join(contacts %>% dplyr::select(id, email), by = c("contact_id" = "id")) %>%
    dplyr::filter(is_internal_email(email)) %>%
    dplyr::select(event_id, contact_id, is_organizer)

  # Zusammenbauen: Verantwortlicher pro Event (fuer is_responsible flag)
  verantwortliche_contact <- events_classified %>%
    dplyr::select(event_id, call_id) %>%
    dplyr::left_join(organizer_per_event, by = "event_id") %>%
    dplyr::left_join(internal_per_call, by = "call_id") %>%
    dplyr::mutate(
      has_call = !is.na(call_id),
      org_was_in_call = event_id %in% org_in_call_events,
      responsible_contact_id = dplyr::case_when(
        !has_call ~ organizer_contact_id,
        org_was_in_call ~ organizer_contact_id,
        !is.na(fallback_contact_id) ~ fallback_contact_id,
        TRUE ~ organizer_contact_id
      )
    ) %>%
    dplyr::select(event_id, responsible_contact_id)

  message(paste0("  ", sum(!is.na(verantwortliche_contact$responsible_contact_id)),
                 " / ", nrow(verantwortliche_contact), " Events mit Verantwortlichem"))

  # === 9. ERGEBNIS ZUSAMMENBAUEN (EINE ZEILE PRO EVENT-CONTACT) ==============

  message("9. Ergebnis zusammenbauen (eine Zeile pro Event-Contact)...")

  # Pro Event: Alle internen Contacts mit ihren Rollen
  event_contact_rows_all <- internal_contacts_per_event %>%
    dplyr::left_join(verantwortliche_contact, by = "event_id") %>%
    dplyr::mutate(
      is_responsible = !is.na(responsible_contact_id) & contact_id == responsible_contact_id
    )

  event_contact_rows <- event_contact_rows_all %>%
    dplyr::filter(is_responsible | is_organizer) %>%
    dplyr::select(event_id, contact_id, is_organizer, is_responsible)

  dropped_contacts <- nrow(event_contact_rows_all) - nrow(event_contact_rows)
  if (dropped_contacts > 0) {
    dropped_events <- length(setdiff(unique(event_contact_rows_all$event_id),
                                     unique(event_contact_rows$event_id)))
    message(paste0("  ", dropped_contacts, " Contacts gefiltert (weder responsible noch organizer), ",
                   dropped_events, " Events komplett entfernt"))
  }

  # Join mit Events-Classification und Exclusion-Regeln
  result <- events_classified %>%
    dplyr::select(mapping_id = id, event_id, event_class) %>%
    dplyr::inner_join(event_contact_rows, by = "event_id") %>%
    dplyr::left_join(
      events_all %>%
        dplyr::select(id, original_created_at, is_short_lived_event) %>%
        dplyr::distinct(id, .keep_all = TRUE),
      by = c("event_id" = "id")
    ) %>%
    dplyr::mutate(
      is_no_show = grepl("no_call|intern_call", event_class, ignore.case = TRUE),
      excluded = event_id %in% c(verschobene_ids, internal_meeting_ids, duplikat_ids, rescheduled_without_mid_ids),
      exclusion_reason = dplyr::case_when(
        event_id %in% rescheduled_without_mid_ids ~ "rescheduled_without_meeting_id",
        event_id %in% verschobene_ids ~ "verschoben",
        event_id %in% internal_meeting_ids ~ "zu_viele_interne",
        event_id %in% duplikat_ids ~ "duplikat_event",
        TRUE ~ NA_character_
      )
    ) %>%
    dplyr::select(
      call_event_mapping_id = mapping_id,
      contact_id,
      is_responsible,
      is_organizer,
      is_no_show,
      excluded,
      exclusion_reason,
      original_created_at,
      is_short_lived_event
    )

  message(paste0("  ", nrow(result), " Zeilen total (Event-Contact Kombinationen)"))
  message(paste0("  ", sum(!result$excluded), " aktiv, ", sum(result$excluded), " excluded"))
  message(paste0("  ", sum(result$is_no_show & !result$excluded), " No-Shows (aktiv)"))
  message(paste0("  ", length(unique(result$call_event_mapping_id)), " eindeutige Events"))

  # === 10. SYNC-SET BAUEN + UPSERT IN DB ======================================

  message("10. Sync-Set aus aktuellem result + Out-of-Scope-Bestand bauen...")

  # Sanity-Checks am result.
  if (nrow(result) == 0) {
    stop("update_extern_event_classification: result ist leer. Abbruch.")
  }
  if (anyDuplicated(result[c("call_event_mapping_id", "contact_id")])) {
    stop("update_extern_event_classification: result enthaelt duplizierte ",
         "(call_event_mapping_id, contact_id) Kombinationen. Bug in Pipeline.")
  }

  # Scope dieses Runs: alle Mapping-IDs zu Events, die wir gerade klassifiziert
  # haben. Bei use_date_filter = TRUE ist das nur das Date-Fenster.
  processed_event_ids <- unique(events_classified$event_id)
  processed_mapping_ids <- dplyr::tbl(con, I("mapping.msgraph_call_event")) %>%
    dplyr::filter(event_id %in% !!processed_event_ids) %>%
    dplyr::pull(id)

  # Aktiver Bestand der Klassifikations-Tabelle AUSSERHALB unseres Scopes
  # (= Events anderer Datumsfenster, die wir gerade nicht beruehren) wird 1:1
  # ins Sync-Set uebernommen, damit postgres_upsert_data mit delete_missing = TRUE
  # diese Zeilen nicht aus Versehen als geloescht markiert.
  out_of_scope <- dplyr::tbl(con, I("processed.msgraph_extern_event_classification")) %>%
    dplyr::filter(is.na(deleted_on)) %>%
    dplyr::filter(!call_event_mapping_id %in% !!processed_mapping_ids) %>%
    dplyr::select(
      call_event_mapping_id, contact_id, is_responsible, is_organizer,
      is_no_show, excluded, exclusion_reason, original_created_at,
      is_short_lived_event
    ) %>%
    dplyr::collect()

  # Wahrheits-Set = neue Klassifikationen fuer prozessierte Events + unangetasteter
  # Bestand fuer alles ausserhalb. Alle aktiven DB-Zeilen, die hier nicht
  # auftauchen, sind stale und werden vom Upsert soft-deleted.
  to_upsert <- dplyr::bind_rows(result, out_of_scope)

  message(paste0("  ", nrow(result), " neue Klassifikationen (Scope), ",
                 nrow(out_of_scope), " unveraendert (Out-of-Scope)"))

  Billomatics::postgres_upsert_data(
    con,
    "processed",
    "msgraph_extern_event_classification",
    to_upsert,
    match_cols = c("call_event_mapping_id", "contact_id"),
    delete_missing = TRUE
  )

  message(paste0("  ", nrow(to_upsert), " Zeilen aktiv nach Upsert"))
  message("Fertig!")
}
