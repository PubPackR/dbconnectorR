#' CRM-Status -> (is_no_show, excluded)
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
    contact_id           = msgraph_meetings$contact_id,
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
      event_date = cm$event_date, contact_id = cm$contact_id, lead_id = cm$lead_id,
      is_no_show = fl$is_no_show, no_show_source = "crm_only",
      meeting_status = cm$meeting_status, meeting_tool = cm$meeting_tool,
      is_external_tool = cm$is_external_tool, excluded = fl$excluded,
      is_short_lived_event = FALSE, is_responsible = TRUE,
      original_created_at = cm$original_created_at, event_id = NA_character_,
      stringsAsFactors = FALSE)

    if (isTRUE(cm$is_external_tool)) { new_rows[[length(new_rows)+1]] <- netto_neu(); next }

    idx <- which(ms_key == paste(cm$lead_id, cm$event_date))
    if (length(idx) == 0) { new_rows[[length(new_rows)+1]] <- netto_neu(); next }
    if (length(idx) > 1) next  # mehrdeutig -> verwerfen

    # eindeutiger Match -> Override (nur definitiver Status setzt is_no_show)
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
