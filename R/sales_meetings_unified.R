#' CRM-Status -> (is_no_show, excluded)
#' @param status character vector mit CRM-Meeting-Status (z.B. "no_show",
#'   "show_up", "storniert", "unbekannt").
#' @return list mit `is_no_show` (logical, NA falls nicht definitiv) und
#'   `excluded` (logical).
#' @keywords internal
crm_status_flags <- function(status) {
  is_no_show <- ifelse(status == "no_show", TRUE, ifelse(status == "show_up", FALSE, NA))
  excluded   <- status %in% c("storniert", "unbekannt")
  list(is_no_show = is_no_show, excluded = excluded)
}

#' Baut die vereinheitlichte Meeting-Menge (rein, kein DB-Zugriff)
#'
#' Grain: eine Zeile pro (Meeting x externer Lead). `msgraph_meetings` ist bereits
#' per-Lead expandiert (eine Zeile je (call_event_mapping_id, lead_id); `lead_id`
#' = NA = externer Teilnehmer ohne gemappten Lead -> Platzhalter). CRM-VC-Termine
#' werden auf (lead_id x event_date) gematcht; bei Mehrdeutigkeit ueber gleichen
#' Rep-Kontakt + naechste `event_start` disambiguiert, echte Rest-Mehrdeutigkeit
#' (gleicher Rep, gleicher Zeitpunkt) wird verworfen.
#'
#' `lead_id` und `contact_id` werden im CHARACTER-Raum gehalten (rbind-sicher
#' gegen die integer64-Falle); der Caller castet vor dem Upsert auf bigint.
#'
#' @param msgraph_meetings data.frame mit call_event_mapping_id, lead_id,
#'   event_date, event_start, contact_id (Rep), is_no_show, excluded,
#'   is_short_lived_event, is_responsible, original_created_at, event_id.
#' @param crm_meetings data.frame mit crm_task_id, lead_id, event_date,
#'   precise_time, contact_id (Rep), meeting_tool, meeting_status,
#'   is_external_tool, original_created_at.
#' @return data.frame im Schema von processed.sales_meetings_unified (+ intern
#'   genutzte, nicht geschriebene Spalten werden vom Caller entfernt).
#' @export
assemble_unified_meetings <- function(msgraph_meetings, crm_meetings) {
  ms_lead <- as.character(msgraph_meetings$lead_id)  # NA fuer Platzhalter
  base <- data.frame(
    meeting_key          = paste0("msgraph_", msgraph_meetings$call_event_mapping_id,
                                  "_", ms_lead),
    source               = "msgraph",
    event_date           = msgraph_meetings$event_date,
    contact_id           = as.character(msgraph_meetings$contact_id),
    lead_id              = ms_lead,
    is_no_show           = msgraph_meetings$is_no_show,
    no_show_source       = "msgraph",
    meeting_status       = NA_character_,
    meeting_tool         = NA_character_,
    is_external_tool     = NA,
    excluded             = msgraph_meetings$excluded,
    is_short_lived_event = msgraph_meetings$is_short_lived_event,
    is_responsible       = msgraph_meetings$is_responsible,
    original_created_at  = msgraph_meetings$original_created_at,
    event_id             = msgraph_meetings$event_id,
    stringsAsFactors     = FALSE
  )
  # Nur fuer den Tiebreak (nicht im DB-Schema): Rep-Kontakt + event_start je Zeile.
  base_rep   <- as.character(msgraph_meetings$contact_id)
  base_start <- msgraph_meetings$event_start

  new_rows <- list()
  for (i in seq_len(nrow(crm_meetings))) {
    cm <- crm_meetings[i, ]
    fl <- crm_status_flags(cm$meeting_status)
    cm_lead <- as.character(cm$lead_id)
    netto_neu <- function() data.frame(
      meeting_key = paste0("crm_", cm$crm_task_id), source = "crm_task",
      event_date = cm$event_date, contact_id = as.character(cm$contact_id), lead_id = cm_lead,
      is_no_show = fl$is_no_show, no_show_source = "crm_only",
      meeting_status = cm$meeting_status, meeting_tool = cm$meeting_tool,
      is_external_tool = cm$is_external_tool, excluded = fl$excluded,
      is_short_lived_event = FALSE, is_responsible = TRUE,
      original_created_at = cm$original_created_at, event_id = NA_character_,
      stringsAsFactors = FALSE)

    # Externes Tool (kein MSGraph-Pendant) und Task ohne Lead -> immer netto-neu.
    if (isTRUE(cm$is_external_tool)) { new_rows[[length(new_rows)+1]] <- netto_neu(); next }
    if (is.na(cm$lead_id))          { new_rows[[length(new_rows)+1]] <- netto_neu(); next }

    # Kandidaten gleicher (lead_id, event_date). Platzhalter (lead_id NA) matchen nie.
    cand <- which(!is.na(base$lead_id) & base$lead_id == cm_lead &
                    base$event_date == cm$event_date)
    if (length(cand) == 0) { new_rows[[length(new_rows)+1]] <- netto_neu(); next }
    if (length(cand) > 1) {
      # Tiebreak 1: gleicher Rep-Kontakt.
      same_rep <- cand[base_rep[cand] == as.character(cm$contact_id)]
      if (length(same_rep) == 1) {
        cand <- same_rep
      } else if (length(same_rep) > 1 && !is.na(cm$precise_time)) {
        # Tiebreak 2: naechste event_start zur precise_time (Sekunden-Distanz).
        d <- abs(as.numeric(base_start[same_rep]) - as.numeric(cm$precise_time))
        if (sum(d == min(d, na.rm = TRUE), na.rm = TRUE) == 1) {
          cand <- same_rep[which.min(d)]
        } else next  # echte Rest-Mehrdeutigkeit -> verwerfen
      } else next    # kein eindeutiger Rep -> verwerfen
    }

    # eindeutiger (bzw. aufgeloester) Match -> Override. Nur definitiver Status
    # setzt is_no_show; storniert -> excluded; "unbekannt" laesst MSGraph unangetastet.
    j <- cand[1]
    if (cm$meeting_status %in% c("no_show", "show_up")) base$is_no_show[j] <- fl$is_no_show
    if (cm$meeting_status == "storniert") base$excluded[j] <- TRUE
    base$no_show_source[j] <- "crm_override"
    base$meeting_tool[j]   <- cm$meeting_tool
    base$meeting_status[j] <- cm$meeting_status
  }

  if (length(new_rows) > 0) base <- rbind(base, do.call(rbind, new_rows))
  base
}

#' Rebuild processed.sales_meetings_unified (voll rueckwirkend)
#'
#' Laeuft nach update_crm_task_meeting_classification. MSGraph-Meetings (extern-only
#' Lead-Ableitung, per (Meeting x Lead) expandiert) + frisch aus Rohdaten
#' abgeleitete CRM-VC-Termine werden via assemble_unified_meetings() vereinheitlicht
#' und komplett neu geschrieben.
#' @param con Pool/DBI-Connection.
#' @return invisible(Anzahl geschriebener Zeilen).
#' @export
update_sales_meetings_unified <- function(con) {
  message("update_sales_meetings_unified: lade MSGraph-Meetings ...")
  msgraph_meetings <- dplyr::tbl(con, I("processed.msgraph_extern_event_classification")) %>%
    dplyr::filter(source == "msgraph", is_responsible == TRUE) %>%
    dplyr::select(call_event_mapping_id, contact_id, is_no_show, original_created_at,
                  excluded, is_short_lived_event, is_responsible) %>%
    dplyr::inner_join(
      dplyr::tbl(con, I("mapping.msgraph_call_event")) %>%
        dplyr::select(id, event_id, event_date),
      by = c("call_event_mapping_id" = "id")) %>%
    dplyr::left_join(
      dplyr::tbl(con, I("raw.msgraph_events")) %>%
        dplyr::select(id, event_start),
      by = c("event_id" = "id")) %>%
    dplyr::collect()
  msgraph_meetings$event_id    <- as.character(msgraph_meetings$event_id)
  msgraph_meetings$event_date  <- as.Date(msgraph_meetings$event_date, tz = "Europe/Berlin")
  msgraph_meetings$event_start <- as.POSIXct(msgraph_meetings$event_start, tz = "UTC")
  # Eine Zeile je Meeting (mehrere verantwortliche Kontakte -> ersten waehlen).
  msgraph_meetings <- dplyr::distinct(msgraph_meetings, call_event_mapping_id, .keep_all = TRUE)

  message("  leite externe Leads je Meeting ab (extern-only, is_primary_crm) ...")
  # Nicht-Organisator-Teilnehmer mit Email -> extern/intern klassifizieren.
  participants <- dplyr::tbl(con, I("mapping.msgraph_call_event")) %>%
    dplyr::select(call_event_mapping_id = id, event_id) %>%
    dplyr::inner_join(
      dplyr::tbl(con, I("raw.msgraph_event_participants")) %>%
        dplyr::filter(is_organizer == FALSE) %>%
        dplyr::select(event_id, contact_id),
      by = "event_id") %>%
    dplyr::inner_join(
      dplyr::tbl(con, I("raw.msgraph_contacts")) %>%
        dplyr::select(id, email),
      by = c("contact_id" = "id")) %>%
    dplyr::select(call_event_mapping_id, contact_id, email) %>%
    dplyr::collect()
  participants$is_external <- !is_internal_email(participants$email) &
                              !is_synthetic_email(participants$email)

  # Meetings mit >=1 externem Teilnehmer (sonst internal-only -> raus).
  has_ext <- unique(participants$call_event_mapping_id[participants$is_external])

  # Externe MAPPED Leads (is_primary_crm) je Meeting.
  crm_map <- dplyr::tbl(con, I("mapping.crm_lead_msgraph_contact")) %>%
    dplyr::filter(is_primary_crm == TRUE) %>%
    dplyr::select(msgraph_contact_id, crm_lead_id) %>%
    dplyr::collect()
  ext_part <- participants[participants$is_external, c("call_event_mapping_id", "contact_id")]
  meeting_lead <- merge(ext_part, crm_map, by.x = "contact_id", by.y = "msgraph_contact_id")
  meeting_lead <- unique(data.frame(
    call_event_mapping_id = meeting_lead$call_event_mapping_id,
    lead_id               = meeting_lead$crm_lead_id,
    stringsAsFactors      = FALSE))

  # internal-only Meetings raus, dann per (Meeting x externer Lead) expandieren.
  # all.x = TRUE -> Meetings mit externem Teilnehmer aber ohne gemappten Lead
  # behalten EINE Zeile mit lead_id = NA (Platzhalter).
  msgraph_meetings <- msgraph_meetings[msgraph_meetings$call_event_mapping_id %in% has_ext, , drop = FALSE]
  msgraph_meetings <- merge(msgraph_meetings, meeting_lead, by = "call_event_mapping_id", all.x = TRUE)

  message("  leite CRM-VC-Termine aus Rohdaten ab ...")
  tasks <- dplyr::tbl(con, I("raw.crm_lead_tasks")) %>%
    dplyr::filter(is_deleted == FALSE) %>%
    dplyr::select(id, crm_task_id, lead_id, user_id, assigned_to_user_id,
                  precise_time, task_created_at, task_name) %>%
    dplyr::collect()
  comments <- dplyr::tbl(con, I("raw.crm_lead_task_comments")) %>%
    dplyr::filter(is_deleted == FALSE) %>%
    dplyr::select(task_id, comment_name) %>%
    dplyr::collect()
  ruc <- resolve_crm_user_contact(con)

  vc <- tasks[is_vc_task(tasks$task_name), , drop = FALSE]
  vc$meeting_tool     <- extract_meeting_tool(vc$task_name)
  vc$is_external_tool <- is_external_tool(vc$meeting_tool)
  # staerkster Status je Task (Surrogat-id-Join, wie in Phase 2)
  rank <- c(storniert = 3L, no_show = 2L, show_up = 1L, unbekannt = 0L)
  comments$status <- classify_meeting_status(comments$comment_name)
  comments$rank   <- rank[comments$status]
  agg <- stats::aggregate(rank ~ task_id, data = comments, FUN = max)
  agg$meeting_status <- names(rank)[match(agg$rank, rank)]
  vc$meeting_status <- agg$meeting_status[match(as.character(vc$id), as.character(agg$task_id))]
  vc$meeting_status[is.na(vc$meeting_status)] <- "unbekannt"
  vc$precise_time <- as.POSIXct(vc$precise_time, tz = "UTC")
  vc$event_date   <- as.Date(vc$precise_time, tz = "Europe/Berlin")
  # Rep-Kontakt (coalesce assigned_to_user_id/user_id im Character-Raum -> integer64-sicher)
  uid <- ifelse(!is.na(vc$assigned_to_user_id), as.character(vc$assigned_to_user_id),
                as.character(vc$user_id))
  vc$contact_id <- ruc$contact_id[match(uid, as.character(ruc$user_id))]
  vc <- vc[!is.na(vc$contact_id), , drop = FALSE]

  crm_meetings <- data.frame(
    crm_task_id = vc$crm_task_id, lead_id = vc$lead_id, event_date = vc$event_date,
    precise_time = vc$precise_time, contact_id = vc$contact_id, meeting_tool = vc$meeting_tool,
    meeting_status = vc$meeting_status, is_external_tool = vc$is_external_tool,
    original_created_at = vc$task_created_at, stringsAsFactors = FALSE)

  rows <- assemble_unified_meetings(msgraph_meetings, crm_meetings)

  # ID-Spalten (im Assemble character) auf integer64 (=bigint) casten; NA -> NULL.
  rows$contact_id <- bit64::as.integer64(rows$contact_id)
  rows$lead_id    <- bit64::as.integer64(rows$lead_id)

  message(paste0("  ", nrow(rows), " Zeilen -> processed.sales_meetings_unified"))

  pool::poolWithTransaction(con, function(conn) {
    Billomatics::postgres_upsert_data(
      conn, "processed", "sales_meetings_unified",
      rows, match_cols = c("meeting_key"), delete_missing = TRUE)
  })
  message("  fertig.")
  invisible(nrow(rows))
}
