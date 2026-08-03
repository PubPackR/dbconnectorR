#' Attendance-Reports -> tidy Teilnehmer (rein)
#' @param reports_value Liste von attendanceReport-Objekten (mit attendanceRecords).
#' @param meeting_id onlineMeeting-id (wird als msgraph_call_id verwendet).
#' @return tibble(meeting_id, email, ms_name, role, total_seconds)
#' @export
parse_attendance_records <- function(reports_value, meeting_id) {
  # ---- start ---- #
  rows <- list()
  for (rep in reports_value) {
    for (r in rep$attendanceRecords %||% list()) {
      addr <- r$emailAddress %||% NA_character_
      rows[[length(rows) + 1]] <- tibble::tibble(
        meeting_id    = meeting_id,
        email         = if (is.na(addr)) NA_character_ else tolower(normalize_external_email(addr)),
        ms_name       = r$identity$displayName %||% NA_character_,
        role          = r$role %||% NA_character_,
        total_seconds = r$totalAttendanceInSeconds %||% NA_integer_)
    }
  }
  if (length(rows)) dplyr::bind_rows(rows) else
    tibble::tibble(meeting_id = character(), email = character(), ms_name = character(),
                   role = character(), total_seconds = numeric())
}

# --- interne Fetch-Helfer (portiert aus scope_01) ---
rep_online_meetings <- function(upn, app_token, start_dt, end_dt) {
  url <- paste0("https://graph.microsoft.com/v1.0/users/", utils::URLencode(upn, reserved = TRUE),
                "/calendar/calendarView")
  res <- graph_collect(url, app_token, query = list(
    startDateTime = start_dt, endDateTime = end_dt, `$top` = 1000,
    `$select` = "subject,start,organizer,isOnlineMeeting,onlineMeeting,isCancelled"))
  if (res$status != 200) return(character(0))
  cand <- Filter(function(e) isTRUE(e$isOnlineMeeting) && !isTRUE(e$isCancelled) &&
                   !is.null(e$onlineMeeting$joinUrl) &&
                   tolower(e$organizer$emailAddress$address %||% "") == tolower(upn), res$value)
  joins <- vapply(cand, function(e) e$onlineMeeting$joinUrl %||% NA_character_, character(1))
  unique(joins[!is.na(joins)])
}

resolve_meeting <- function(object_id, join_url, app_token) {
  res <- graph_get(paste0("https://graph.microsoft.com/v1.0/users/", object_id, "/onlineMeetings"),
                   app_token, query = list(`$filter` = paste0("JoinWebUrl eq '", join_url, "'")))
  list(status = res$status,
       id = if (!is.null(res$content$value) && length(res$content$value) > 0)
         res$content$value[[1]]$id %||% NA_character_ else NA_character_)
}

attendance_records <- function(object_id, meeting_id, app_token) {
  base <- paste0("https://graph.microsoft.com/v1.0/users/", object_id,
                 "/onlineMeetings/", meeting_id, "/attendanceReports")
  res <- graph_collect(base, app_token, query = list(`$expand` = "attendanceRecords"))
  if (res$status != 200) return(list(status = res$status, meeting_start = NA_character_,
                                     meeting_end = NA_character_, reports = list()))
  # per-Report-Fallback: $expand liefert oft keine Records -> nachladen
  for (i in seq_along(res$value)) {
    if (length(res$value[[i]]$attendanceRecords %||% list()) == 0 && !is.null(res$value[[i]]$id)) {
      rr <- graph_collect(paste0(base, "/", res$value[[i]]$id, "/attendanceRecords"), app_token)
      if (rr$status == 200) res$value[[i]]$attendanceRecords <- rr$value
    }
  }
  list(status = 200,
       meeting_start = res$value[[1]]$meetingStartDateTime %||% NA_character_,
       meeting_end = res$value[[1]]$meetingEndDateTime %||% NA_character_,
       reports = res$value)
}

#' Calls/Teilnehmer gescopt via Attendance aktualisieren (app-only, policy-gescoped)
#' @param con DB-Pool.
#' @param app_token app-only Provider.
#' @param cfg load_scoped_config().
#' @return invisible(Anzahl Calls).
#' @export
msgraph_scoped_update_calls_attendance <- function(con, app_token, cfg) {
  # ---- start ---- #
  start_dt <- format(Sys.Date() - cfg$events_days_back, "%Y-%m-%dT00:00:00Z")
  end_dt   <- format(Sys.Date(), "%Y-%m-%dT23:59:59Z")
  users <- dplyr::tbl(con, I("raw.msgraph_users")) %>%
    dplyr::filter(is_internal & !is_deleted) %>%
    dplyr::select(msgraph_user_id, user_principal_name) %>% dplyr::collect()

  calls <- list(); parts <- list()
  for (i in seq_len(nrow(users))) {
    upn <- users$user_principal_name[i]; oid <- users$msgraph_user_id[i]
    if (is.na(upn)) next
    joins <- tryCatch(rep_online_meetings(upn, app_token, start_dt, end_dt),
                      error = function(e) { message("meetings ", upn, ": ", e$message); character(0) })
    for (ju in joins) {
      mt <- tryCatch(resolve_meeting(oid, ju, app_token), error = function(e) list(status = NA, id = NA_character_))
      if (isTRUE(mt$status == 403)) break            # Policy-Block definitiv -> Rest sparen
      if (!isTRUE(mt$status == 200) || is.na(mt$id)) next
      at <- tryCatch(attendance_records(oid, mt$id, app_token),
                     error = function(e) list(status = NA, meeting_start = NA, meeting_end = NA, reports = list()))
      if (!isTRUE(at$status == 200) || length(at$reports) == 0) next
      df <- parse_attendance_records(at$reports, mt$id)
      if (nrow(df) == 0) next
      cs <- lubridate::ymd_hms(at$meeting_start, quiet = TRUE)
      ce <- lubridate::ymd_hms(at$meeting_end, quiet = TRUE)
      if (is.na(ce)) ce <- cs   # Fallback: NOT NULL column, use start when end missing
      calls[[length(calls) + 1]] <- tibble::tibble(
        msgraph_call_id = mt$id, call_start = cs, call_end = ce,
        meeting_id = mt$id)
      parts[[length(parts) + 1]] <- df
    }
  }
  if (length(calls) == 0) { message("Keine Calls/Attendance."); return(invisible(0L)) }
  calls_df <- dplyr::distinct(dplyr::bind_rows(calls), msgraph_call_id, .keep_all = TRUE)
  parts_df <- dplyr::bind_rows(parts) %>% dplyr::filter(!is.na(email)) %>% dplyr::distinct()

  # Kontakte upserten
  contacts <- parts_df %>% dplyr::transmute(email, ms_name) %>% dplyr::distinct(email, .keep_all = TRUE)
  Billomatics::postgres_upsert_data(con, "raw", "msgraph_contacts", contacts, match_cols = "email")
  # Calls upserten
  Billomatics::postgres_upsert_data(con, "raw", "msgraph_calls", calls_df, match_cols = "msgraph_call_id")
  # Teilnehmer verknuepfen
  call_ids <- dplyr::tbl(con, I("raw.msgraph_calls")) %>%
    dplyr::select(id, msgraph_call_id) %>% dplyr::collect() %>% dplyr::rename(call_id = id)
  ct_ids <- dplyr::tbl(con, I("raw.msgraph_contacts")) %>%
    dplyr::select(id, email) %>% dplyr::collect() %>% dplyr::rename(contact_id = id)
  cp <- parts_df %>%
    dplyr::left_join(call_ids, by = c("meeting_id" = "msgraph_call_id")) %>%
    dplyr::left_join(ct_ids, by = "email") %>%
    dplyr::filter(!is.na(call_id), !is.na(contact_id)) %>%
    dplyr::transmute(call_id, contact_id)
  Billomatics::postgres_upsert_data(con, "raw", "msgraph_call_participants", cp,
                                    match_cols = c("call_id", "contact_id"))
  invisible(nrow(calls_df))
}
