#' Classify External Events with Original Creation Date
#'
#' Classifies external events from `mapping.msgraph_call_event` as no-show or attended,
#' determines responsible contacts (one row per event-contact combination),
#' filters shifted/large/duplicate events, and correctly calculates original_created_at
#' across all events with the same msgraph_ical_uid.
#'
#' Key features:
#' - Correct original_created_at: earliest of Graph's `event_created_at` and our
#'   own ingest stamp `created_at`, grouped by msgraph_ical_uid. Graph reports a
#'   *new* creation date for pre-existing meetings after a tenant migration, so
#'   it is only believed up to the point we first saw the row. See
#'   [compute_original_created_at()].
#' - Multiple rows per event: one row per (event, contact) combination
#' - is_short_lived_event flag: events canceled < 24h after original_created_at,
#'   and therefore subject to the same ingest bound
#' - Rescheduled meeting detection: same lead, < 2 days apart, no meeting_id
#'
#' @param con A PostgreSQL database connection object.
#' @param min_date Date. Only process events from this date onwards.
#'   Defaults to 90 days ago. Only used when `use_date_filter = TRUE`.
#' @param use_date_filter Logical. If TRUE, restrict processing to events with
#'   `event_date >= min_date`. Default FALSE (full table).
#' @param tenant_id Character or NULL. GUID of the own Microsoft tenant. Meetings
#'   whose `join_url` does not carry this GUID were created in the previous tenant;
#'   their attendance data is unreachable app-only, so they are excluded instead of
#'   counted as no-shows (see `compute_observability_exclusions`). Pass
#'   `cfg$tenant_id` from the calling base-app. Defaults to NULL, which skips that
#'   exclusion and emits a warning -- the pre-cutover behaviour.
#' @param now_utc POSIXct. Reference point for "is this meeting still in the
#'   future". Defaults to the current time. Only injectable for tests.
#'
#' @return No return value. Updates database table `processed.msgraph_extern_event_classification`.
#'
#' @details
#' Before writing, the function builds a sync-set as the union of (a) the freshly
#' computed classifications for events in this run and (b) the existing rows for
#' events outside the run's scope. It then calls
#' `Billomatics::postgres_upsert_data(..., delete_missing = TRUE)`, which deletes
#' any row not in the sync-set. Since this table has no `is_deleted` column (and
#' therefore no soft-delete trigger), the delete is a hard, physical delete. The
#' net effect: stale `(call_event_mapping_id, contact_id)` rows from earlier runs
#' — where the dedup later picked a different winning mapping_id for the same
#' event — are removed, while classifications for events outside the date window
#' (`use_date_filter = TRUE`) are preserved untouched.
#'
#' @export
#' @examples
#' update_extern_event_classification(con)
#' update_extern_event_classification(con, min_date = as.Date("2025-05-01"))
update_extern_event_classification <- function(con, min_date = Sys.Date() - 90, use_date_filter = FALSE,
                                               tenant_id = NULL, now_utc = Sys.time()) {

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
    dplyr::select(call_id = id, call_start, call_duration_sec)

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

  # Alle Events aus DB laden mit event_created_at und msgraph_ical_uid.
  # created_at ist unser eigener Ingest-Stempel und wird als untere Schranke
  # fuer das Anlagedatum gebraucht, siehe compute_original_created_at().
  all_events <- dplyr::tbl(con, I("raw.msgraph_events")) %>%
    dplyr::select(id, msgraph_ical_uid, event_created_at, event_updated_at,
                  event_start, event_end, is_canceled, is_online_meeting, subject,
                  join_url, created_at) %>%
    dplyr::collect()

  # Pro msgraph_ical_uid: fruehestes Anlagedatum, wobei Graph nur bis zu unserem
  # ersten Ingest geglaubt wird. Seit dem Tenant-Wechsel meldet Graph fuer
  # bestehende Termine spaetere createdDateTime-Werte; ohne diese Schranke
  # wandert ihre Terminierung rueckwirkend in den Migrationsmonat.
  original_created_lookup <- compute_original_created_at(all_events)

  # Events mit original_created_at anreichern
  events_all <- all_events %>%
    dplyr::filter(id %in% unique(events_classified$event_id)) %>%
    dplyr::left_join(original_created_lookup, by = "msgraph_ical_uid")

  message(paste0("  Original_created_at fuer ", nrow(events_all), " Events berechnet"))

  # === 3. IS_SHORT_LIVED_EVENT BERECHNEN ======================================

  message("3. Short-lived Events erkennen...")

  # Short-lived = gecancelt UND < 24h zwischen original_created_at und event_updated_at
  #
  # Haengt mit an der Ingest-Schranke aus compute_original_created_at(): zieht
  # sie original_created_at nach vorn, waechst dieser Abstand und Events
  # verlieren das Flag. Beabsichtigt, aber es ist eine zweite Mengenaenderung.
  # Beispiel aus dem August-Cutover: Graph-Anlagedatum 19.08. 09:00, Absage
  # 19.08. 15:00, bisher 6 h und damit short-lived und ueberall ausgefiltert.
  # Mit dem Ingest-Stempel vom 31.07. sind es 19 Tage, das Flag faellt weg und
  # der Termin zaehlt wieder mit. Fachlich richtig, denn er wurde im Juli
  # gelegt und im August abgesagt, war also kein kurzlebiger Fehleintrag.
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

  # === 8b. ECHTE NO-SHOWS UNTER RESCHEDULE-AUSSCHLUESSEN ======================

  message("8b. Echte No-Shows unter Reschedule-Ausschluessen erkennen...")

  # Ein Reschedule-Ausschluss (verschoben / rescheduled_without_meeting_id)
  # entfernt die gecancelte Haelfte eines verschobenen Meetings. Das ist falsch,
  # wenn der Lead no-showt und der Termin erst DANACH neu gelegt wurde -- so ein
  # No-Show zaehlt weiter. Diagnostik: interner Call am Slot (call_start ~
  # event_start), Absage NACH dem Slot (event_updated_at > event_start) und ein
  # externer Lead war eingeladen.
  event_call_times <- events_classified %>%
    dplyr::select(event_id, call_start) %>%
    dplyr::left_join(
      events_all %>% dplyr::distinct(id, event_start, event_updated_at),
      by = c("event_id" = "id")
    )

  event_participant_emails <- all_event_participants %>%
    dplyr::left_join(contacts %>% dplyr::select(id, email), by = c("contact_id" = "id")) %>%
    dplyr::select(event_id, email)

  # Nur No-Show-Events kommen fuer den Override in Frage (nicht stattgefundene
  # extern_call-Termine, die auch externen Lead + Call am Slot haben).
  no_show_event_ids <- events_classified %>%
    dplyr::filter(grepl("no_call|intern_call", event_class, ignore.case = TRUE)) %>%
    dplyr::pull(event_id)

  reschedule_no_show_ids <- intersect(
    c(verschobene_ids, rescheduled_without_mid_ids),
    no_show_event_ids
  )

  real_no_show_ids <- identify_real_no_show_reschedules(
    reschedule_event_ids     = reschedule_no_show_ids,
    event_call_times         = event_call_times,
    event_participant_emails = event_participant_emails
  )

  # Reschedule-Ausschluesse um die echten No-Shows bereinigen. zu_viele_interne
  # und duplikat_event bleiben unberuehrt.
  verschobene_final <- setdiff(verschobene_ids, real_no_show_ids)
  rescheduled_final <- setdiff(rescheduled_without_mid_ids, real_no_show_ids)

  message(paste0("  ", length(real_no_show_ids),
                 " Reschedule-Events sind echte No-Shows (bleiben gezaehlt)"))

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

  # Nicht beobachtbare Events: Zukunft und Alt-Tenant. Ohne diese Ausschluesse
  # zaehlt jedes Meeting, dessen Anwesenheit nie abrufbar ist, als No-Show.
  if (is.null(tenant_id)) {
    warning(paste0(
      "update_extern_event_classification: kein tenant_id uebergeben. ",
      "Meetings aus dem Alt-Tenant werden weiter als No-Show gezaehlt, ",
      "obwohl ihre Anwesenheitsdaten app-only unerreichbar sind."
    ))
  }
  # Events, zu denen ein Call gefunden wurde, sind beobachtet worden - egal aus
  # welchem Tenant sie stammen. Der gesamte Bestand vor der Migration faellt
  # darunter: base-35 hat tenantweit Calls geholt, diese Events sind korrekt
  # klassifiziert. Sie hier auszuschliessen wuerde die Historie loeschen, gegen
  # die validiert wird.
  ids_mit_call <- events_classified$event_id[
    !grepl("no_call", events_classified$event_class, ignore.case = TRUE)]

  observability <- compute_observability_exclusions(events_all, tenant_id = tenant_id,
                                                    now_utc = now_utc,
                                                    event_ids_mit_call = ids_mit_call)
  # Echte No-Shows bleiben gezaehlt, gleiche Regel wie bei verschobene_final und
  # rescheduled_final oben.
  future_ids     <- setdiff(observability$event_id[observability$reason == "termin_in_zukunft"],
                            real_no_show_ids)
  alt_tenant_ids <- setdiff(observability$event_id[observability$reason == "alt_tenant_join_url"],
                            real_no_show_ids)

  message(paste0("  ", length(future_ids), " Events in der Zukunft, ",
                 length(alt_tenant_ids), " Events aus dem Alt-Tenant -> excluded"))

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
      excluded = event_id %in% c(verschobene_final, internal_meeting_ids, duplikat_ids,
                                 rescheduled_final, future_ids, alt_tenant_ids),
      exclusion_reason = dplyr::case_when(
        event_id %in% rescheduled_final ~ "rescheduled_without_meeting_id",
        event_id %in% verschobene_final ~ "verschoben",
        event_id %in% internal_meeting_ids ~ "zu_viele_interne",
        event_id %in% duplikat_ids ~ "duplikat_event",
        event_id %in% future_ids ~ "termin_in_zukunft",
        event_id %in% alt_tenant_ids ~ "alt_tenant_join_url",
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

  # Spalten der Klassifikations-Tabelle, die wir in beiden Teilen des Sync-Sets
  # (result + out_of_scope) konsistent fuehren muessen. Einmal definiert, damit
  # spaetere Schema-Erweiterungen nicht stillschweigend in out_of_scope wegfallen
  # und durch den Upsert mit NA ueberschrieben werden.
  classification_cols <- c(
    "call_event_mapping_id", "contact_id", "is_responsible", "is_organizer",
    "is_no_show", "excluded", "exclusion_reason", "original_created_at",
    "is_short_lived_event"
  )

  # Scope dieses Runs: alle Mapping-IDs zu Events, die wir gerade klassifiziert
  # haben. Bei use_date_filter = TRUE ist das nur das Date-Fenster.
  processed_event_ids <- unique(events_classified$event_id)
  processed_mapping_ids <- dplyr::tbl(con, I("mapping.msgraph_call_event")) %>%
    dplyr::filter(event_id %in% !!processed_event_ids) %>%
    dplyr::distinct(id) %>%
    dplyr::pull(id)

  # Bestand der Klassifikations-Tabelle AUSSERHALB unseres Scopes (= Events
  # anderer Datumsfenster, die wir gerade nicht beruehren) wird 1:1 ins Sync-Set
  # uebernommen, damit postgres_upsert_data mit delete_missing = TRUE diese Zeilen
  # nicht physisch loescht. Die Tabelle hat keine is_deleted-Spalte und damit
  # keinen soft-delete-Trigger -> delete_missing wirkt als harter DELETE.
  out_of_scope <- dplyr::tbl(con, I("processed.msgraph_extern_event_classification")) %>%
    dplyr::filter(!call_event_mapping_id %in% !!processed_mapping_ids) %>%
    dplyr::select(dplyr::all_of(classification_cols)) %>%
    dplyr::collect()

  # Wahrheits-Set = neue Klassifikationen fuer prozessierte Events + unangetasteter
  # Bestand fuer alles ausserhalb. Alle DB-Zeilen, die hier nicht auftauchen,
  # sind stale und werden vom Upsert physisch geloescht.
  to_upsert <- dplyr::bind_rows(
    dplyr::select(result, dplyr::all_of(classification_cols)),
    out_of_scope
  )

  message(paste0("  ", nrow(result), " neue Klassifikationen (Scope), ",
                 nrow(out_of_scope), " unveraendert (Out-of-Scope)"))

  # Safety-Guard: delete_missing = TRUE loescht physisch (kein soft-delete-Netz).
  # Wenn das Sync-Set ploetzlich <50% des aktuellen Bestands haette, deutet das
  # auf einen Upstream-Bug hin -> abbrechen, statt halb die Tabelle zu loeschen.
  existing_n <- dplyr::tbl(con, I("processed.msgraph_extern_event_classification")) %>%
    dplyr::count() %>%
    dplyr::pull(n)
  if (existing_n > 0 && nrow(to_upsert) < 0.5 * existing_n) {
    stop(sprintf(
      paste0("update_extern_event_classification: Sync-Set hat nur %d Zeilen, ",
             "Bestand ist %d. delete_missing = TRUE wuerde >50%% physisch loeschen. ",
             "Abbruch (vermuteter Upstream-Bug)."),
      nrow(to_upsert), existing_n
    ))
  }

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

#' Determine the Original Creation Date per iCal UID
#'
#' `original_created_at` is the anchor of every "Termine gelegt" figure: it says
#' in which month a meeting was scheduled. It used to be `min(event_created_at)`
#' per `msgraph_ical_uid`, taking Graph's `createdDateTime` at face value.
#'
#' That broke in the August 2026 tenant migration. Graph started reporting a
#' **new** `createdDateTime` for meetings that already existed. No second event
#' appeared -- the same row, same id, same uid, simply got a later stamp.
#' Measured on 2026-09-01, 1975 of 2768 post-cutover meetings carried a Graph
#' date that fell on average 68 days *after* our own first ingest of that row.
#' Their scheduling work moved retroactively into August: the week of the 17th
#' showed 1713 meetings instead of 377, while June and July lost the same
#' amount.
#'
#' The fix needs no pairing and no heuristic, because the correct answer is
#' already in our own data. `raw.msgraph_events.created_at` is the moment we
#' first inserted the row, and `Billomatics::postgres_upsert_data` excludes
#' `created_at` from its update columns, so it never moves. A meeting we already
#' held on 31 July cannot have been created on 19 August. The ingest stamp is
#' therefore a hard upper bound, at most one nightly run away from the truth,
#' and the earlier of the two dates wins.
#'
#' Deliberately applied to all rows, not just the migration window: the same
#' pattern recurs with every further calendar share (see the note in base-62's
#' `config.yaml`), and a rule bound to fixed dates would not survive it. Series
#' occurrences were checked and are unaffected -- of the 484 meetings shifted by
#' more than 90 days, 416 are single instances with exactly one occurrence per
#' uid.
#'
#' `event_created_at` is the reference representation, not UTC: the column is a
#' `timestamp without time zone` holding UTC digits, which some drivers hand back
#' tagged with the session timezone. `created_at` is a genuine `timestamptz` and
#' is converted to those same digits before the comparison, so `pmin` compares
#' digits with digits.
#'
#' The result deliberately keeps `event_created_at`'s timezone attribute rather
#' than being tagged UTC. Callers keep working on raw driver values:
#' `is_short_lived_event` takes the difference to `event_updated_at`, and the
#' upsert writes into a column without a timezone. Tagging the result would shift
#' both by the local offset -- a meeting created 30 June 22:30 UTC would land as
#' 1 July 00:30 in the table and count in the wrong month.
#'
#' @param events Data frame with columns `msgraph_ical_uid`, `event_created_at`
#'   (Graph's `createdDateTime`) and `created_at` (our first insert).
#' @return Data frame with one row per `msgraph_ical_uid` and the column
#'   `original_created_at`.
#' @keywords internal
# ---- start ---- #
compute_original_created_at <- function(events) {
  # `event_created_at` ist die Referenz-Darstellung, nicht UTC: die Spalte ist
  # `timestamp without time zone` und traegt UTC-Ziffern, je nach Treiber aber
  # mit oder ohne tz-Attribut. Der Ingest-Stempel ist ein echtes `timestamptz`
  # und wird auf genau diese Darstellung gebracht, damit pmin Ziffern mit
  # Ziffern vergleicht.
  #
  # Bewusst NICHT das Ergebnis auf UTC umtaggen: die Aufrufer rechnen mit rohen
  # Treiber-Werten weiter. `is_short_lived_event` bildet die Differenz zu
  # `event_updated_at`, und der Upsert schreibt in eine Spalte ohne Zeitzone.
  # Ein hier gesetztes tz-Attribut verschoebe beides um den lokalen Offset:
  # ein am 30.06. 22:30 UTC angelegter Termin landete als 01.07. 00:30 in der
  # Tabelle und zaehlte im falschen Monat.
  graph_tz <- attr(events$event_created_at, "tzone")
  if (is.null(graph_tz)) graph_tz <- ""

  events %>%
    dplyr::mutate(
      .ingest_wie_graph = lubridate::force_tz(
        lubridate::with_tz(created_at, "UTC"), tzone = graph_tz
      ),
      .effektiv = pmin(event_created_at, .ingest_wie_graph, na.rm = TRUE)
    ) %>%
    dplyr::group_by(msgraph_ical_uid) %>%
    dplyr::summarise(
      original_created_at = min(.effektiv, na.rm = TRUE),
      .groups = "drop"
    )
}

#' Determine Which Events Are Not Observable At All
#'
#' `is_no_show` is not a measured state -- it is derived from the *absence* of a
#' matching call (`no_call` / `intern_call`). Every event whose attendance can
#' never be observed therefore looks like a no-show. This helper names those
#' events so they can be excluded from numerator *and* denominator instead.
#'
#' Two conditions, both permanent for the event in question:
#'
#' - **`termin_in_zukunft`** -- the meeting has not happened yet. There cannot be
#'   a call record for it, so it is not a no-show. Relevant because the scoped
#'   ingest pulls calendar events up to a year ahead (`events_days_forward`).
#' - **`alt_tenant_join_url`** -- the `join_url` does not carry the own tenant's
#'   GUID, so the meeting was created in the previous tenant. Its attendance
#'   report is unreachable app-only; `discover_meetings_from_events` filters those
#'   meetings out by the same rule, which is why no call ever arrives for them.
#'   Recurring series created before the tenant migration keep their original
#'   `join_url` indefinitely, so this does not age out on its own.
#'
#' Precedence when both apply: `termin_in_zukunft` wins while the meeting is still
#' ahead, `alt_tenant_join_url` takes over once it has passed. The future reason is
#' the one that changes, so reporting it first keeps "not due yet" separable from
#' "never observable".
#'
#' Events with a missing `join_url` are never excluded here. They are not online
#' meetings and were not counted differently before this fix; changing that is a
#' separate decision.
#'
#' **Two guards keep the tenant rule from eating the history.**
#'
#' - **Call evidence wins.** An event with a matching call was observed, whatever
#'   its `join_url` says, and is never excluded as `alt_tenant_join_url`. Without
#'   this the whole pre-migration series would disappear. It deliberately does
#'   *not* apply to `termin_in_zukunft`: a meeting that has not happened yet is
#'   no no-show even if some call row points at it -- that is a data
#'   contradiction, not an observation.
#' - **The rule has a start date (`alt_tenant_ab`).** Old-tenant meetings only
#'   became unreachable once base-62 was the sole supplier. Before that base-35
#'   fetched calls tenant-wide, so a missing call was a genuine no-show, not an
#'   observability gap. Measured on 2026-08-31, dropping this guard removed 86
#'   real no-shows from July alone and pushed its rate from 17.2 % to 11.5 %.
#'
#' @param events Data frame of events with columns `id`, `event_start` and
#'   `join_url`.
#' @param tenant_id Character or NULL. GUID of the own tenant, matched literally
#'   against `join_url` -- the same rule `discover_meetings_from_events` applies.
#'   NULL skips the tenant check entirely.
#' @param now_utc POSIXct. Reference point for the future check.
#' @param event_ids_mit_call Vector of event ids for which a call was found
#'   (`event_class` without `no_call`). These were observed by definition and are
#'   never returned as `alt_tenant_join_url`. Defaults to none.
#' @param alt_tenant_ab Date. `alt_tenant_join_url` is only applied to events
#'   starting on or after this date. Default 2026-08-19 -- the last successful
#'   run of base-35's `msgraph_update_calls` (per `processed.data_job_events`),
#'   and therefore the last day on which old-tenant calls could still be
#'   fetched. Events without an `event_start` are never excluded by the tenant
#'   rule, because the window cannot be decided for them.
#' @keywords internal
#'
#' @return Data frame with one row per excluded event: `event_id` and `reason`
#'   (`"termin_in_zukunft"` or `"alt_tenant_join_url"`). Zero rows when nothing
#'   is excluded.
#'
#' @details
#' `event_start` is a `timestamp without time zone` holding UTC. Depending on the
#' driver it may arrive tagged with the session timezone, which would shift the
#' comparison by the local offset. `force_tz(..., "UTC")` fixes that case and is a
#' no-op when the value is already tagged UTC.
# ---- start ---- #
compute_observability_exclusions <- function(events, tenant_id = NULL, now_utc = Sys.time(),
                                             event_ids_mit_call = NULL,
                                             alt_tenant_ab = as.Date("2026-08-19")) {

  empty <- data.frame(event_id = events$id[0], reason = character(0),
                      stringsAsFactors = FALSE)

  if (nrow(events) == 0) {
    return(empty)
  }

  event_start_utc <- lubridate::force_tz(events$event_start, "UTC")
  is_future <- !is.na(event_start_utc) & event_start_utc > now_utc

  if (is.null(tenant_id)) {
    is_alt_tenant <- rep(FALSE, nrow(events))
  } else {
    is_alt_tenant <- !is.na(events$join_url) &
      !grepl(tenant_id, events$join_url, fixed = TRUE)
  }

  # Ein gefundener Call ist der Beweis, dass das Meeting beobachtbar war. Er
  # sticht die Tenant-Regel, sonst faellt der komplette Vor-Migrations-Bestand
  # raus. Beim Zukunfts-Grund gilt das NICHT: ein Termin, der noch bevorsteht,
  # ist kein No-Show, auch wenn irgendwo ein Call daranhaengt - das waere ein
  # Datenwiderspruch und keine Beobachtung.
  hat_call <- events$id %in% (event_ids_mit_call %||% events$id[0])

  # Die Alt-Tenant-Unerreichbarkeit gilt erst, seit base-62 der einzige
  # Lieferant ist. Davor hat base-35 tenantweit Calls geholt, ein fehlender Call
  # war also ein echter No-Show und kein Beobachtungsproblem. Ohne diese Grenze
  # verschwinden ruecwirkend echte No-Shows: gemessen am 31.08.2026 waren es 86
  # allein im Juli, die Rate fiel dadurch von 17,2 auf 11,5 Prozent.
  im_unerreichbaren_fenster <- !is.na(event_start_utc) &
    as.Date(event_start_utc) >= alt_tenant_ab

  is_alt_tenant <- is_alt_tenant & !hat_call & im_unerreichbaren_fenster

  reason <- ifelse(is_future, "termin_in_zukunft",
                   ifelse(is_alt_tenant, "alt_tenant_join_url", NA_character_))

  out <- data.frame(event_id = events$id, reason = reason,
                    stringsAsFactors = FALSE)
  out[!is.na(out$reason), , drop = FALSE]
}

#' Identify Genuine No-Shows Among Reschedule-Excluded Events
#'
#' A reschedule exclusion (`verschoben` / `rescheduled_without_meeting_id`)
#' removes the canceled leg of a moved meeting. That is correct when a meeting
#' was moved *before* it happened, but wrong when the lead no-showed and the
#' meeting was only rebooked *afterwards* -- per definition such a no-show still
#' counts. This helper returns the subset of reschedule-excluded event_ids that
#' are genuine no-shows and must therefore NOT be excluded.
#'
#' An event qualifies when ALL hold:
#' - an external lead was invited: at least one participant email that is
#'   neither internal (`is_internal_email`) nor synthetic (`is_synthetic_email`)
#'   -- rules out internal meetings mis-tagged as `extern_planned`;
#' - an internal call actually took place at the scheduled slot: the event's
#'   (deduped) mapped `call_start` lies within `slot_window_minutes` of
#'   `event_start` -- positive evidence the meeting time arrived and someone
#'   internal joined; and
#' - the event was cancelled/last modified AFTER the slot
#'   (`event_updated_at > event_start`) -- the no-show happened before the
#'   reschedule. This rules out meetings moved *before* their time (proactive
#'   reschedule, no no-show), where a call may still map to the slot via a
#'   shared recurring meeting id. `event_updated_at` is the same
#'   cancellation-time proxy the pipeline already uses for `is_short_lived_event`.
#'
#' @param reschedule_event_ids Integer vector of event_ids currently flagged for
#'   reschedule-exclusion.
#' @param event_call_times Data frame with one row per event and columns
#'   `event_id`, `call_start`, `event_start`, `event_updated_at` (the deduped
#'   mapped call plus event timing).
#' @param event_participant_emails Data frame with columns `event_id`, `email`
#'   for all participants of the events in scope.
#' @param slot_window_minutes Numeric. Maximum absolute distance (minutes)
#'   between `call_start` and `event_start` to count as "call at the slot".
#'   Default 30.
#' @return Integer vector: the subset of `reschedule_event_ids` that are genuine
#'   no-shows and should stay counted (not excluded).
#' @keywords internal
identify_real_no_show_reschedules <- function(reschedule_event_ids,
                                              event_call_times,
                                              event_participant_emails,
                                              slot_window_minutes = 30) {
  # ---- start ---- #
  if (length(reschedule_event_ids) == 0) {
    return(reschedule_event_ids)
  }

  events_with_external_lead <- event_participant_emails %>%
    dplyr::filter(
      !is.na(email),
      !is_internal_email(email),
      !is_synthetic_email(email)
    ) %>%
    dplyr::distinct(event_id) %>%
    dplyr::pull(event_id)

  # Echter No-Show am Slot: interner Call am geplanten Termin UND Absage erst
  # NACH dem Slot (sonst proaktive Vorher-Verschiebung -> kein No-Show).
  events_real_no_show <- event_call_times %>%
    dplyr::filter(
      !is.na(call_start), !is.na(event_start),
      abs(as.numeric(difftime(call_start, event_start, units = "mins"))) <= slot_window_minutes,
      !is.na(event_updated_at), event_updated_at > event_start
    ) %>%
    dplyr::distinct(event_id) %>%
    dplyr::pull(event_id)

  reschedule_event_ids[
    reschedule_event_ids %in% events_with_external_lead &
      reschedule_event_ids %in% events_real_no_show
  ]
}
