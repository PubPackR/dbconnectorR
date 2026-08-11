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
#' @param msgraph_meetings data.frame (siehe Plan/Interfaces).
#' @param crm_meetings data.frame (siehe Plan/Interfaces).
#' @return data.frame im Schema von processed.sales_meetings_unified.
#' @export
assemble_unified_meetings <- function(msgraph_meetings, crm_meetings) {
  # Basis: MSGraph-Zeilen
  base <- data.frame(
    meeting_key          = as.character(msgraph_meetings$call_event_mapping_id),
    source               = "msgraph",
    event_date           = msgraph_meetings$event_date,
    contact_id           = as.character(msgraph_meetings$contact_id),
    lead_id              = msgraph_meetings$lead_id,
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

  new_rows <- list()
  # Match-Index: wie viele MSGraph-Termine pro (lead_id, event_date)
  ms_key <- paste(msgraph_meetings$lead_id, msgraph_meetings$event_date)

  for (i in seq_len(nrow(crm_meetings))) {
    cm <- crm_meetings[i, ]
    fl <- crm_status_flags(cm$meeting_status)
    netto_neu <- function() data.frame(
      meeting_key = paste0("crm_", cm$crm_task_id), source = "crm_task",
      event_date = cm$event_date, contact_id = as.character(cm$contact_id), lead_id = cm$lead_id,
      is_no_show = fl$is_no_show, no_show_source = "crm_only",
      meeting_status = cm$meeting_status, meeting_tool = cm$meeting_tool,
      is_external_tool = cm$is_external_tool, excluded = fl$excluded,
      is_short_lived_event = FALSE, is_responsible = TRUE,
      original_created_at = cm$original_created_at, event_id = NA_character_,
      stringsAsFactors = FALSE)

    if (isTRUE(cm$is_external_tool)) { new_rows[[length(new_rows)+1]] <- netto_neu(); next }

    if (is.na(cm$lead_id)) { new_rows[[length(new_rows)+1]] <- netto_neu(); next }

    idx <- which(ms_key == paste(cm$lead_id, cm$event_date))
    if (length(idx) == 0) { new_rows[[length(new_rows)+1]] <- netto_neu(); next }
    if (length(idx) > 1) next  # mehrdeutig -> verwerfen

    # eindeutiger Match -> Override (nur definitiver Status setzt is_no_show).
    # Die netto-neu-Tabelle (is_no_show, excluded) aus crm_status_flags() gilt
    # NUR fuer crm_only-Zeilen. Im Override wirkt nur definitiver Status:
    # no_show/show_up -> is_no_show, storniert -> excluded. "unbekannt" laesst
    # die MSGraph-Zeile unveraendert, sonst wuerde ein echtes, per MSGraph
    # getracktes Meeting wegen eines nicht klassifizierbaren CRM-Kommentars
    # faelschlich aus dem No-Show-Nenner ausgeschlossen.
    j <- idx[1]
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
#' Laeuft nach update_crm_task_meeting_classification. MSGraph-Meetings +
#' frisch aus Rohdaten abgeleitete CRM-VC-Termine (ohne Anti-Join) werden via
#' assemble_unified_meetings() vereinheitlicht und komplett neu geschrieben.
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
      dplyr::tbl(con, I("mapping.crm_lead_msgraph_contact")) %>%
        dplyr::filter(is_primary_crm == TRUE) %>%
        dplyr::select(contact_id = msgraph_contact_id, lead_id = crm_lead_id),
      by = "contact_id") %>%
    dplyr::collect()
  msgraph_meetings$event_id   <- as.character(msgraph_meetings$event_id)
  msgraph_meetings$event_date <- as.Date(msgraph_meetings$event_date, tz = "Europe/Berlin")
  # Fan-out-Guard: der Left-Join auf mapping.crm_lead_msgraph_contact kann
  # pro call_event_mapping_id mehrere Zeilen erzeugen (analog Phase 2).
  # meeting_key = as.character(call_event_mapping_id) muss eindeutig sein.
  msgraph_meetings <- dplyr::distinct(msgraph_meetings, call_event_mapping_id, .keep_all = TRUE)

  message("  leite CRM-VC-Termine aus Rohdaten ab (ohne Anti-Join) ...")
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
  vc$event_date <- as.Date(vc$precise_time, tz = "Europe/Berlin")
  # Rep-Kontakt (coalesce assigned_to_user_id/user_id im Character-Raum -> integer64-sicher)
  uid <- ifelse(!is.na(vc$assigned_to_user_id), as.character(vc$assigned_to_user_id),
                as.character(vc$user_id))
  vc$contact_id <- ruc$contact_id[match(uid, as.character(ruc$user_id))]
  vc <- vc[!is.na(vc$contact_id), , drop = FALSE]

  crm_meetings <- data.frame(
    crm_task_id = vc$crm_task_id, lead_id = vc$lead_id, event_date = vc$event_date,
    contact_id = vc$contact_id, meeting_tool = vc$meeting_tool,
    meeting_status = vc$meeting_status, is_external_tool = vc$is_external_tool,
    original_created_at = vc$task_created_at, stringsAsFactors = FALSE)

  rows <- assemble_unified_meetings(msgraph_meetings, crm_meetings)
  message(paste0("  ", nrow(rows), " Zeilen -> processed.sales_meetings_unified"))

  pool::poolWithTransaction(con, function(conn) {
    Billomatics::postgres_upsert_data(
      conn, "processed", "sales_meetings_unified",
      rows, match_cols = c("meeting_key"), delete_missing = TRUE)
  })
  message("  fertig.")
  invisible(nrow(rows))
}
