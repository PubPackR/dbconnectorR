#' Baut CRM-Task-Meetings zu Zeilen im Klassifikations-Schema zusammen
#'
#' Rein (kein DB-Zugriff). Filtert VC-Termine, extrahiert Tool + Status,
#' wendet den Anti-Join gegen bestehende MSGraph-Meetings an und mappt auf das
#' Schema von processed.msgraph_extern_event_classification (+ CRM-Zusatzspalten).
#'
#' @param crm_tasks data.frame: crm_task_id, lead_id, user_id, precise_time, task_name.
#' @param crm_comments data.frame: task_id, comment_name.
#' @param crm_user_contact data.frame: user_id, contact_id (Sales-Rep-Kontakt).
#' @param lead_contact data.frame: lead_id, contact_id (Lead-Kontakt).
#' @param msgraph_meetings data.frame: lead_id, event_date.
#' @return data.frame im Klassifikations-Schema mit source='crm_task'.
#' @export
assemble_crm_classification_rows <- function(crm_tasks, crm_comments,
                                             crm_user_contact, lead_contact,
                                             msgraph_meetings) {
  # 1. nur VC-Termine
  vc <- crm_tasks[is_vc_task(crm_tasks$task_name), , drop = FALSE]
  if (nrow(vc) == 0) return(assemble_crm_empty_result())

  # 2. Tool
  vc$meeting_tool <- extract_meeting_tool(vc$task_name)
  vc$is_external_tool <- is_external_tool(vc$meeting_tool)

  # 3. Status: staerkste Kategorie je Task ueber alle Kommentare
  status_rank <- c(storniert = 3L, no_show = 2L, show_up = 1L, unbekannt = 0L)
  comment_status <- crm_comments
  comment_status$status <- classify_meeting_status(comment_status$comment_name)
  comment_status$rank <- status_rank[comment_status$status]
  agg <- stats::aggregate(rank ~ task_id, data = comment_status, FUN = max)
  agg$meeting_status <- names(status_rank)[match(agg$rank, status_rank)]
  vc$meeting_status <- agg$meeting_status[match(vc$crm_task_id, agg$task_id)]
  vc$meeting_status[is.na(vc$meeting_status)] <- "unbekannt"

  # 4. Datum (Europe/Berlin)
  vc$event_date <- as.Date(vc$precise_time, tz = "Europe/Berlin")

  # 5. Anti-Join
  vc <- filter_new_crm_meetings(vc, msgraph_meetings)
  if (nrow(vc) == 0) return(assemble_crm_empty_result())

  # 6. Lead-Kontakt (ohne Kontakt keine Zeile)
  vc$contact_id <- lead_contact$contact_id[match(vc$lead_id, lead_contact$lead_id)]
  vc <- vc[!is.na(vc$contact_id), , drop = FALSE]
  if (nrow(vc) == 0) return(assemble_crm_empty_result())

  # 7. Status -> Flags
  is_no_show <- vc$meeting_status == "no_show"
  excluded   <- vc$meeting_status == "storniert"

  data.frame(
    call_event_mapping_id = NA_integer_,
    contact_id            = vc$contact_id,
    is_responsible        = TRUE,
    is_organizer          = TRUE,
    is_no_show            = is_no_show,
    excluded              = excluded,
    exclusion_reason      = ifelse(excluded, "crm_storniert", NA_character_),
    original_created_at   = vc$precise_time,
    is_short_lived_event  = FALSE,
    source                = "crm_task",
    meeting_tool          = vc$meeting_tool,
    meeting_status        = vc$meeting_status,
    is_external_tool      = vc$is_external_tool,
    crm_event_date        = vc$event_date,
    crm_task_id           = vc$crm_task_id,
    stringsAsFactors      = FALSE
  )
}

#' Leeres Ergebnis im Klassifikations-Schema (interne Helferfunktion)
#' @return data.frame mit 0 Zeilen und den korrekten Spalten.
#' @keywords internal
assemble_crm_empty_result <- function() {
  data.frame(
    call_event_mapping_id = integer(0), contact_id = integer(0),
    is_responsible = logical(0), is_organizer = logical(0),
    is_no_show = logical(0), excluded = logical(0),
    exclusion_reason = character(0), original_created_at = as.POSIXct(character(0)),
    is_short_lived_event = logical(0), source = character(0),
    meeting_tool = character(0), meeting_status = character(0),
    is_external_tool = logical(0), crm_event_date = as.Date(character(0)),
    crm_task_id = integer(0), stringsAsFactors = FALSE
  )
}
