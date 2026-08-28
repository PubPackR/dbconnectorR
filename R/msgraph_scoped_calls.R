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

#' Meeting-Discovery aus den delegiert ingestierten Kalender-Events (DB, kein Graph)
#'
#' Ersetzt das fruehere app-only calendarView-Lesen in der Discovery
#' (403 — `Calendars.Read` als Application-Permission wird nie granted):
#' die delegiert ingestierten Events liefern `join_url`; der Organizer wird
#' ueber `is_organizer` -> msgraph_contacts -> msgraph_users (Email-Match)
#' auf seine object_id aufgeloest. Nur intern organisierte Meetings sind
#' aufloesbar — extern organisierte deckt die CsApplicationAccessPolicy
#' ohnehin nicht.
#'
#' @param con DB-Pool.
#' @param cfg load_scoped_config(); `raw_schema` steuert das Quell-Schema,
#'   `events_days_back` das Fenster (nur vergangene/laufende Meetings),
#'   `tenant_id` filtert auf Meetings des eigenen Tenants (Alt-Tenant-URLs
#'   sind app-only unerreichbar).
#' @return data.frame(join_url, organizer_oid), distinct.
#' @keywords internal
discover_meetings_from_events <- function(con, cfg) {
  # ---- start ---- #
  rs <- cfg$raw_schema %||% "raw"
  window_start <- format(Sys.Date() - cfg$events_days_back, "%Y-%m-%d")
  # rs kommt aus der Config (kein User-Input) -> sichere String-Interpolation;
  # event_start liegt als UTC-timestamp -> Vergleich gegen now() AT TIME ZONE 'UTC'.
  kandidaten <- DBI::dbGetQuery(con, sprintf("
    SELECT DISTINCT e.join_url, u.msgraph_user_id AS organizer_oid
    FROM %1$s.msgraph_events e
    JOIN %1$s.msgraph_event_participants p ON p.event_id = e.id AND p.is_organizer
    JOIN %1$s.msgraph_contacts ct          ON ct.id = p.contact_id
    JOIN %1$s.msgraph_users u              ON lower(u.email) = lower(ct.email)
    WHERE u.is_internal AND NOT u.is_deleted
      AND e.join_url IS NOT NULL
      AND NOT e.is_canceled
      AND e.event_start >= %2$s
      AND e.event_start <= (now() AT TIME ZONE 'UTC')",
    rs, DBI::dbQuoteLiteral(con, window_start)))

  # Nur Meetings des EIGENEN Tenants: die joinUrl traegt die Tenant-GUID im
  # context-Parameter. Meetings aus dem Alt-Tenant (vor der Migration) sind
  # app-only prinzipiell unerreichbar und wuerden per 403 faelschlich den
  # Organizer fuer seine gueltigen neuen Meetings blocken.
  #
  # Der Vergleich laeuft bewusst nicht mehr als WHERE-Klausel, sondern hier in R:
  # nur so laesst sich zaehlen, wie viele Meetings der Filter kostet. Genau diese
  # Zahl blieb beim Tenant-Wechsel unsichtbar, waehrend die No-Show-Rate davon
  # auf 52,6 Prozent hochlief.
  eigener_tenant <- grepl(cfg$tenant_id, kandidaten$join_url, fixed = TRUE)
  out <- kandidaten[eigener_tenant, c("join_url", "organizer_oid"), drop = FALSE]
  attr(out, "n_kandidaten") <- nrow(kandidaten)
  attr(out, "n_alt_tenant") <- sum(!eigener_tenant)
  out
}

# --- interne Fetch-Helfer (portiert aus scope_01) ---
# rep_online_meetings ist NICHT mehr Teil des Jobs (app-only calendarView = 403);
# bleibt nur als Diagnose-Helfer fuer one-off/probe_calls_attendance*.R erhalten.
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

#' Calls/Teilnehmer gescopt via Attendance aktualisieren
#'
#' Discovery aus den delegiert ingestierten Events (`discover_meetings_from_events`),
#' Meeting-Aufloesung + Attendance app-only (CsApplicationAccessPolicy-gescoped).
#'
#' @param con DB-Pool.
#' @param app_token app-only Provider (Meeting-Aufloesung + Attendance).
#' @param cfg load_scoped_config(); `raw_schema`/`processed_schema` steuern das Ziel-Schema.
#' @param suppression_pepper DSGVO-Pepper; wenn gesetzt, werden gesperrte PII (config.privacy_deletion_log) vor dem Upsert getombstoned.
#' @param dry_run Wenn TRUE: nur zaehlen/loggen, kein Upsert.
#' @return invisible(Anzahl Calls).
#' @export
msgraph_scoped_update_calls_attendance <- function(con, app_token, cfg, suppression_pepper = NULL, dry_run = FALSE) {
  # ---- start ---- #
  rs <- cfg$raw_schema %||% "raw"
  ps <- cfg$processed_schema %||% "processed"
  disc <- discover_meetings_from_events(con, cfg)
  message(sprintf("Discovery: %d Meetings im Fenster, %d davon aus dem Alt-Tenant verworfen (%d Kandidaten).",
                  nrow(disc), attr(disc, "n_alt_tenant") %||% 0L,
                  attr(disc, "n_kandidaten") %||% nrow(disc)))
  if (nrow(disc) == 0) { message("Keine Meetings im Fenster (Discovery aus Events)."); return(invisible(0L)) }

  calls <- list(); parts <- list()
  blocked_oids <- character(0)   # 403 = Policy deckt diesen Organizer nicht -> Rest sparen
  # Fehlerbuchhaltung: bisher fiel jeder Fehlschlag stumm durch 'next'. Ein
  # abgelaufener Token oder ein Graph-Ausfall sah dadurch aus wie "keine Calls" -
  # und weiter unten wie eine Welle von No-Shows.
  n_versucht <- 0L; n_resolve_fehler <- 0L; n_attendance_fehler <- 0L; n_policy_403 <- 0L
  for (i in seq_len(nrow(disc))) {
    ju <- disc$join_url[i]; oid <- disc$organizer_oid[i]
    if (oid %in% blocked_oids) next
    n_versucht <- n_versucht + 1L
    mt <- tryCatch(resolve_meeting(oid, ju, app_token), error = function(e) list(status = NA, id = NA_character_))
    if (isTRUE(mt$status == 403)) {
      # Policy-403 ist eine erwartete Abgrenzung, kein Fehlschlag - zaehlt
      # deshalb nicht in die Fehlerquote unten.
      blocked_oids <- c(blocked_oids, oid); n_policy_403 <- n_policy_403 + 1L; next
    }
    if (!isTRUE(mt$status == 200) || is.na(mt$id)) { n_resolve_fehler <- n_resolve_fehler + 1L; next }
    at <- tryCatch(attendance_records(oid, mt$id, app_token),
                   error = function(e) list(status = NA, meeting_start = NA, meeting_end = NA, reports = list()))
    if (!isTRUE(at$status == 200)) { n_attendance_fehler <- n_attendance_fehler + 1L; next }
    # Keine Reports ist KEIN Fehler: ein Meeting, an dem niemand teilgenommen
    # hat, liefert legitim nichts - das ist der echte No-Show.
    if (length(at$reports) == 0) next
    df <- parse_attendance_records(at$reports, mt$id)
    if (nrow(df) == 0) next
    cs <- lubridate::ymd_hms(at$meeting_start, quiet = TRUE)
    ce <- lubridate::ymd_hms(at$meeting_end, quiet = TRUE)
    if (is.na(ce)) ce <- cs   # Fallback: NOT NULL column, use start when end missing
    # meeting_id = thread-id aus der joinUrl, identische Ableitung wie in
    # parse_scoped_events und im alten base-35-Pfad (msgraph_calls.R:414). Nur so
    # paart msgraph_map_calls_events den Call mit seinem Event; ohne das bleibt
    # jedes Event ohne Call und wird als No-Show klassifiziert. Die
    # onlineMeeting-id bleibt als Graph-Griff in msgraph_call_id erhalten.
    mid_thread <- extract_meeting_id_safe(ju)
    if (is.na(mid_thread))
      message("meeting_id nicht aus joinUrl ableitbar, Call bleibt ohne Event-Zuordnung: ",
              substr(ju, 1, 90))
    calls[[length(calls) + 1]] <- tibble::tibble(
      msgraph_call_id = mt$id, call_start = cs, call_end = ce,
      meeting_id = mid_thread)
    parts[[length(parts) + 1]] <- df
  }
  if (length(blocked_oids) > 0)
    message("Policy-403 fuer ", length(blocked_oids), " Organizer-oid(s) — deren Meetings uebersprungen.")

  # Laut ausfallen statt still nichts zu schreiben. Beide Faelle bedeuten, dass
  # der Job zwar Meetings gefunden, aber keine belastbaren Daten geholt hat -
  # jedes betroffene Event wird downstream sonst zum No-Show.
  n_fehler <- n_resolve_fehler + n_attendance_fehler
  n_bewertbar <- n_versucht - n_policy_403   # 403 ist Abgrenzung, kein Fehlschlag
  if (n_bewertbar > 0 && n_fehler > 0.5 * n_bewertbar) {
    stop(sprintf(paste0(
      "msgraph_scoped_update_calls_attendance: %d von %d bewertbaren Meetings scheiterten ",
      "an Graph (%d resolve, %d attendance). Ueber der Haelfte - vermutlich Token oder ",
      "Graph-Ausfall. Abbruch, statt die fehlenden Calls als No-Shows wirken zu lassen."),
      n_fehler, n_bewertbar, n_resolve_fehler, n_attendance_fehler))
  }
  if (length(calls) == 0) {
    if (n_bewertbar > 0) {
      stop(sprintf(paste0(
        "msgraph_scoped_update_calls_attendance: %d bewertbare Meetings versucht, kein ",
        "einziger Attendance-Report verwertbar (%d resolve-, %d attendance-Fehler). Abbruch."),
        n_bewertbar, n_resolve_fehler, n_attendance_fehler))
    }
    message("Keine Calls/Attendance."); return(invisible(0L))
  }
  if (n_fehler > 0)
    message(sprintf("  %d von %d bewertbaren Meetings ohne Attendance (%d resolve, %d attendance).",
                    n_fehler, n_bewertbar, n_resolve_fehler, n_attendance_fehler))
  calls_df <- dplyr::distinct(dplyr::bind_rows(calls), msgraph_call_id, .keep_all = TRUE)
  parts_df <- dplyr::bind_rows(parts) %>% dplyr::filter(!is.na(email)) %>% dplyr::distinct()

  # DSGVO: PII gesperrter Personen tombstonen (email -> Tombstone, ms_name -> NA),
  # BEVOR Kontakte + Teilnehmer daraus abgeleitet werden -> beide Seiten nutzen
  # denselben Tombstone, der Email-Join bleibt konsistent (wie base-35 msgraph_update_calls).
  parts_df <- dsgvo_suppress_participants(parts_df, con, suppression_pepper)

  if (dry_run) {
    message(sprintf("[dry-run] %d Calls, %d Teilnehmer (kein Upsert).", nrow(calls_df), nrow(parts_df)))
    return(invisible(nrow(calls_df)))
  }

  # Kontakte upserten
  contacts <- parts_df %>% dplyr::transmute(email, ms_name) %>% dplyr::distinct(email, .keep_all = TRUE)
  Billomatics::postgres_upsert_data(con, rs, "msgraph_contacts", contacts, match_cols = "email")
  # Calls upserten
  Billomatics::postgres_upsert_data(con, rs, "msgraph_calls", calls_df, match_cols = "msgraph_call_id")
  # Teilnehmer verknuepfen
  call_ids <- dplyr::tbl(con, I(paste0(rs, ".msgraph_calls"))) %>%
    dplyr::select(id, msgraph_call_id) %>% dplyr::collect() %>% dplyr::rename(call_id = id)
  ct_ids <- dplyr::tbl(con, I(paste0(rs, ".msgraph_contacts"))) %>%
    dplyr::select(id, email) %>% dplyr::collect() %>% dplyr::rename(contact_id = id)
  cp <- parts_df %>%
    dplyr::left_join(call_ids, by = c("meeting_id" = "msgraph_call_id")) %>%
    dplyr::left_join(ct_ids, by = "email") %>%
    dplyr::filter(!is.na(call_id), !is.na(contact_id)) %>%
    dplyr::transmute(call_id, contact_id) %>%
    # dieselbe Person kann in mehreren Attendance-Reports eines Meetings stehen
    dplyr::distinct(call_id, contact_id)
  Billomatics::postgres_upsert_data(con, rs, "msgraph_call_participants", cp,
                                    match_cols = c("call_id", "contact_id"))
  invisible(nrow(calls_df))
}
