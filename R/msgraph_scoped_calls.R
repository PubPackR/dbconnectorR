#' Attendance-Reports -> tidy Teilnehmer (rein)
#'
#' Externe Gaeste stehen im Bericht ohne `emailAddress` und ohne
#' `identity.tenantId`. Sie werden - wie im alten callRecords-Pfad - als
#' Synthetic guest `guest_<identity.id>@external.guest` behalten, sonst fallen
#' sie im Ingest weg und der Call zaehlt als intern_call (No-Show).
#'
#' @param reports_value Liste von attendanceReport-Objekten (mit attendanceRecords).
#' @param meeting_id Schluessel des Calls, zu dem die Records gehoeren; im
#'   Ingest die Bericht-ID der Session (= msgraph_call_id).
#' @param tenant_id Eigene Tenant-GUID. Ein Record ohne E-Mail mit dieser
#'   `tenantId` ist ein interner Account und bleibt `NA`. Ohne `tenant_id`
#'   wird nur ein Record ohne `tenantId` zum Gast.
#' @return tibble(meeting_id, email, ms_name, role, total_seconds);
#'   `email` ist `NA`, wenn weder Adresse noch Gast-Schluessel ableitbar ist.
#' @export
parse_attendance_records <- function(reports_value, meeting_id, tenant_id = NULL) {
  # ---- start ---- #
  rows <- list()
  for (rep in reports_value) {
    for (r in rep$attendanceRecords %||% list()) {
      addr <- r$emailAddress %||% NA_character_
      email <- if (!is.na(addr) && nzchar(addr)) tolower(normalize_external_email(addr)) else
        synthetic_attendance_guest_email(r$identity, tenant_id)
      rows[[length(rows) + 1]] <- tibble::tibble(
        meeting_id    = meeting_id,
        email         = email,
        ms_name       = r$identity$displayName %||% NA_character_,
        role          = r$role %||% NA_character_,
        total_seconds = r$totalAttendanceInSeconds %||% NA_integer_)
    }
  }
  if (length(rows)) dplyr::bind_rows(rows) else
    tibble::tibble(meeting_id = character(), email = character(), ms_name = character(),
                   role = character(), total_seconds = numeric())
}

#' Gast-Adresse fuer einen Attendance-Record ohne E-Mail
#'
#' @param identity `identity`-Objekt des attendanceRecords (Liste).
#' @param tenant_id Eigene Tenant-GUID oder NULL.
#' @return `guest_<lower(identity.id)>@external.guest`, wenn die `tenantId`
#'   fehlt oder fremd ist und eine `id` vorliegt; sonst `NA_character_`.
#' @keywords internal
synthetic_attendance_guest_email <- function(identity, tenant_id = NULL) {
  # ---- start ---- #
  iid <- identity$id %||% ""
  tid <- identity$tenantId %||% ""
  if (!nzchar(iid)) return(NA_character_)
  ist_gast <- !nzchar(tid) || (!is.null(tenant_id) && !identical(tolower(tid), tolower(tenant_id)))
  if (!ist_gast) return(NA_character_)
  # identity.id ist eine GUID -> Kleinschreiben ist verlustfrei und passt zu
  # msgraph_contacts.email_normalized = lower(email)
  paste0("guest_", tolower(iid), "@external.guest")
}

#' Meeting-Discovery aus den delegiert ingestierten Kalender-Events (DB, kein Graph)
#'
#' Ersetzt das fruehere app-only calendarView-Lesen in der Discovery
#' (403 — `Calendars.Read` als Application-Permission wird nie granted):
#' die delegiert ingestierten Events liefern `join_url`; der Organizer wird
#' ueber `is_organizer` -> msgraph_contacts -> msgraph_users (Email-Match)
#' auf seine object_id aufgeloest.
#'
#' Jeder Organisator wird abgefragt, auch mit `is_deleted` (der Directory-Sync,
#' der das Feld pflegte, laeuft nicht mehr) und auch ohne Zeile in
#' msgraph_users: dann ist `organizer_oid` NA, und der Calls-Job loest das Konto
#' ueber `organizer_email` in Graph auf. Eine vorhandene Zeile, die nicht intern
#' ist, faellt weiter heraus - extern organisierte Meetings deckt die
#' CsApplicationAccessPolicy ohnehin nicht.
#'
#' @param con DB-Pool.
#' @param cfg load_scoped_config(); `raw_schema` steuert das Quell-Schema,
#'   `events_days_back` das Fenster (nur vergangene/laufende Meetings),
#'   `tenant_id` filtert auf Meetings des eigenen Tenants (Alt-Tenant-URLs
#'   sind app-only unerreichbar).
#' @return data.frame(join_url, organizer_oid, organizer_email), distinct;
#'   `organizer_email` kleingeschrieben, `organizer_oid` NA ohne msgraph_users-Zeile.
#' @keywords internal
discover_meetings_from_events <- function(con, cfg) {
  # ---- start ---- #
  rs <- cfg$raw_schema %||% "raw"
  window_start <- format(Sys.Date() - cfg$events_days_back, "%Y-%m-%d")
  # rs kommt aus der Config (kein User-Input) -> sichere String-Interpolation;
  # event_start liegt als UTC-timestamp -> Vergleich gegen now() AT TIME ZONE 'UTC'.
  # LEFT JOIN: u.email IS NULL heisst "keine Zeile in msgraph_users" (der Join
  # vergleicht auf u.email, eine gefundene Zeile hat sie also gesetzt).
  kandidaten <- DBI::dbGetQuery(con, sprintf("
    SELECT DISTINCT e.join_url, u.msgraph_user_id AS organizer_oid,
           lower(ct.email) AS organizer_email
    FROM %1$s.msgraph_events e
    JOIN %1$s.msgraph_event_participants p ON p.event_id = e.id AND p.is_organizer
    JOIN %1$s.msgraph_contacts ct          ON ct.id = p.contact_id
    LEFT JOIN %1$s.msgraph_users u         ON lower(u.email) = lower(ct.email)
    WHERE (u.email IS NULL OR u.is_internal)
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
  out <- kandidaten[eigener_tenant, c("join_url", "organizer_oid", "organizer_email"), drop = FALSE]
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
  # meeting_start/_end: Start/Ende des NEUESTEN Berichts (Graph listet die
  # hoechstens 50 juengsten Berichte, neueste zuerst). Der Ingest nimmt Start
  # und Ende je Bericht; die Felder bleiben fuer die base-62-Probe-Skripte.
  list(status = 200,
       meeting_start = res$value[[1]]$meetingStartDateTime %||% NA_character_,
       meeting_end = res$value[[1]]$meetingEndDateTime %||% NA_character_,
       reports = res$value)
}

#' Anwesenheitsberichte eines Online-Meetings in Sessions zerlegen (rein)
#'
#' Jeder Anwesenheitsbericht ist eine Session: ein Vorkommen des Online-Meetings
#' mit eigenem Start, eigenem Ende und eigenen Teilnehmern. Ein wiederverwendeter
#' Teams-Link (Serie, persoenlicher Link) hat viele davon. Frueher wurde daraus
#' ein einziger Call mit dem Start des zuerst gelisteten Berichts, jeder andere Termin
#' am selben Link zaehlte als No-Show.
#'
#' @param reports Liste von attendanceReport-Objekten (mit attendanceRecords).
#' @param online_meeting_id onlineMeeting-id, der Graph-Griff fuer Transkripte.
#' @param meeting_id thread-id aus der joinUrl (Paarung mit dem Event).
#' @param tenant_id Eigene Tenant-GUID, siehe `parse_attendance_records()`.
#' @return list(calls, parts). `calls` hat eine Zeile je Session mit
#'   `msgraph_call_id` = Bericht-ID; `parts` traegt dieselbe ID in `meeting_id`.
#'   Attribut `n_ohne_start`: Berichte ohne ID oder Startzeit, die wegfallen
#'   (die Zielspalten sind NOT NULL).
#' @keywords internal
sessions_from_reports <- function(reports, online_meeting_id, meeting_id, tenant_id = NULL) {
  # ---- start ---- #
  calls <- list(); parts <- list(); n_ohne_start <- 0L
  for (rep in reports) {
    rid <- rep$id %||% NA_character_
    cs  <- lubridate::ymd_hms(rep$meetingStartDateTime %||% NA_character_, quiet = TRUE)
    if (is.na(rid) || !nzchar(rid) || is.na(cs)) { n_ohne_start <- n_ohne_start + 1L; next }
    df <- parse_attendance_records(list(rep), rid, tenant_id = tenant_id)
    # Ein Bericht ohne Teilnehmer ist keine Session, an der jemand teilnahm
    if (nrow(df) == 0) next
    ce <- lubridate::ymd_hms(rep$meetingEndDateTime %||% NA_character_, quiet = TRUE)
    if (is.na(ce)) ce <- cs   # Fallback: NOT NULL column, use start when end missing
    calls[[length(calls) + 1]] <- tibble::tibble(
      msgraph_call_id = rid, call_start = cs, call_end = ce,
      meeting_id = meeting_id, msgraph_online_meeting_id = online_meeting_id)
    parts[[length(parts) + 1]] <- df
  }
  out <- list(
    calls = if (length(calls)) dplyr::bind_rows(calls) else tibble::tibble(
      msgraph_call_id = character(), call_start = as.POSIXct(character(), tz = "UTC"),
      call_end = as.POSIXct(character(), tz = "UTC"), meeting_id = character(),
      msgraph_online_meeting_id = character()),
    parts = if (length(parts)) dplyr::bind_rows(parts) else
      parse_attendance_records(list(), NA_character_))
  attr(out, "n_ohne_start") <- n_ohne_start
  out
}

#' Umschluesselung des Call-Bestands planen (rein)
#'
#' Calls, die vor dem Session-Schluessel geschrieben wurden, tragen in
#' `msgraph_call_id` noch die onlineMeeting-id und den Start des Berichts, den
#' Graph beim letzten Lauf zuerst lieferte - das ist der neueste, nicht der
#' erste. Jede solche Zeile bekommt die Session desselben Online-Meetings, deren
#' Start ihrem `call_start` am naechsten liegt, im Normalfall genau diese.
#' Hatte genau diese Session keinen verwertbaren Teilnehmer, ist sie nicht in
#' `calls_df`, und die naechstgelegene andere Session erbt die Zeile. Das ist in
#' Kauf genommen: die Alternative waere eine Dublette unter altem Schluessel.
#'
#' @param bestand data.frame(id, msgraph_call_id, call_start): vorhandene Zeilen,
#'   deren `msgraph_call_id` eine onlineMeeting-id aus `calls_df` ist.
#' @param calls_df Die Sessions dieses Laufs (`sessions_from_reports()$calls`).
#' @return tibble(id, new_call_id, online_meeting_id), eine Zeile je umzuschluesselnder Zeile.
#' @keywords internal
plan_call_rekey <- function(bestand, calls_df) {
  # ---- start ---- #
  if (nrow(bestand) == 0 || nrow(calls_df) == 0)
    return(tibble::tibble(id = bestand$id[0], new_call_id = character(), online_meeting_id = character()))
  bestand %>%
    dplyr::transmute(id, online_meeting_id = msgraph_call_id, alt_start = call_start) %>%
    dplyr::inner_join(
      calls_df %>% dplyr::transmute(new_call_id = msgraph_call_id,
                                    online_meeting_id = msgraph_online_meeting_id, call_start),
      by = "online_meeting_id") %>%
    dplyr::mutate(abstand = abs(as.numeric(difftime(call_start, alt_start, units = "secs")))) %>%
    dplyr::group_by(id) %>%
    dplyr::slice_min(abstand, n = 1, with_ties = FALSE) %>%
    dplyr::ungroup() %>%
    dplyr::select(id, new_call_id, online_meeting_id)
}

#' Call-Bestand auf den Session-Schluessel umschluesseln
#'
#' Setzt fuer Zeilen mit onlineMeeting-id in `msgraph_call_id` die Bericht-ID
#' ihrer Session und `msgraph_online_meeting_id`. Die `id` bleibt, damit
#' Transkripte, Mapping und Klassifikation weiter auf dieselbe Zeile zeigen.
#' Die Teilnehmer dieser Zeilen werden geleert: dort standen die Teilnehmer
#' aller Sessions des Links, der anschliessende Upsert schreibt die der einen
#' Session neu. Nach dem ersten Lauf findet die Abfrage nichts mehr (No-op).
#'
#' @param con Pool oder DBI-Verbindung.
#' @param rs Ziel-Schema (config-Schalter, i.d.R. "raw").
#' @param calls_df Die Sessions dieses Laufs (`sessions_from_reports()$calls`).
#' @return invisible(Anzahl umgeschluesselter Zeilen).
#' @keywords internal
rekey_meeting_calls <- function(con, rs, calls_df) {
  # ---- start ---- #
  omids <- unique(stats::na.omit(calls_df$msgraph_online_meeting_id))
  if (length(omids) == 0) return(invisible(0L))
  # rs kommt aus der Config (kein User-Input); die ids werden per dbQuoteLiteral gequotet.
  bestand <- DBI::dbGetQuery(con, sprintf(
    "SELECT id, msgraph_call_id, call_start FROM %s.msgraph_calls WHERE msgraph_call_id IN (%s)",
    rs, paste(DBI::dbQuoteLiteral(con, omids), collapse = ", ")))
  plan <- plan_call_rekey(bestand, calls_df)
  if (nrow(plan) == 0) return(invisible(0L))
  # id ist bigint und kommt als integer64 -> als Text in die Statements
  ids <- as.character(plan$id)
  values <- paste(sprintf("(%s::bigint, %s, %s)", ids,
                          DBI::dbQuoteLiteral(con, plan$new_call_id),
                          DBI::dbQuoteLiteral(con, plan$online_meeting_id)), collapse = ",\n")
  upd <- sprintf("
    UPDATE %1$s.msgraph_calls AS c
       SET msgraph_call_id = v.new_call_id, msgraph_online_meeting_id = v.online_meeting_id
      FROM (VALUES %2$s) AS v(id, new_call_id, online_meeting_id)
     WHERE c.id = v.id", rs, values)
  del <- sprintf("DELETE FROM %s.msgraph_call_participants WHERE call_id IN (%s)",
                 rs, paste(ids, collapse = ", "))
  # Beides in einer Transaktion: ein umgeschluesselter Call mit den Teilnehmern
  # aller Sessions waere genau der Zustand, den der Fix beseitigen soll.
  schreibe <- function(conn) { n <- DBI::dbExecute(conn, upd); DBI::dbExecute(conn, del); n }
  n <- if (inherits(con, "Pool")) pool::poolWithTransaction(con, schreibe) else
    DBI::dbWithTransaction(con, schreibe(con))
  message(sprintf("%d Call(s) vom onlineMeeting- auf den Session-Schluessel umgeschluesselt.", n))
  invisible(n)
}

#' Abbrechen, wenn msgraph_calls.msgraph_online_meeting_id fehlt
#'
#' @param con Pool oder DBI-Verbindung.
#' @param rs Ziel-Schema (config-Schalter, i.d.R. "raw").
#' @return invisible(TRUE), sonst Fehler mit Verweis auf die Migration.
#' @keywords internal
assert_online_meeting_id_column <- function(con, rs) {
  # ---- start ---- #
  hat_spalte <- DBI::dbGetQuery(con, "
    SELECT count(*) AS n FROM information_schema.columns
     WHERE table_schema = $1 AND table_name = 'msgraph_calls'
       AND column_name = 'msgraph_online_meeting_id'", params = list(rs))$n
  if (as.numeric(hat_spalte) == 0) {
    stop(sprintf(paste0(
      "Spalte %s.msgraph_calls.msgraph_online_meeting_id fehlt. Erst die Migration ",
      "ausfuehren: dbconnectorR/inst/sql/2026-10-06-msgraph-calls-online-meeting-id.sql ",
      "(sie aendert nur raw; fuer ein anderes Schema die ALTER-Zeile dort nachziehen)."), rs))
  }
  invisible(TRUE)
}

# --- Ausgang der Online-Meeting-Suche je Link ---

# Rangfolge der Ausgaenge der Online-Meeting-Suche, bester zuerst. Hat ein Link
# mehrere Discovery-Zeilen (mehrere Organisator-Zeilen), gilt der beste Ausgang
# ueber alle Versuche.
ONLINE_MEETING_LOOKUP_RANG <- c("gefunden_mit_bericht", "gefunden_ohne_bericht", "nicht_gefunden",
                                "abruf_fehler", "policy_403", "organisator_unbekannt")

#' Organisator-Konto per E-Mail in Graph aufloesen
#'
#' Gleiche Abfrage wie der Users-Job (`msgraph_scoped_update_users`), nur mit
#' `$select = id`. Fuer Organisatoren ohne Zeile in msgraph_users.
#'
#' @param email E-Mail-Adresse des Organisators.
#' @param app_token app-only Provider (User.ReadBasic.All).
#' @return list(status, id); `id` NA, wenn Graph keinen Treffer liefert.
#' @keywords internal
resolve_organizer_by_email <- function(email, app_token) {
  # ---- start ---- #
  # OData-Literal: ein Apostroph in der Adresse wird verdoppelt
  addr <- gsub("'", "''", email, fixed = TRUE)
  res <- graph_get("https://graph.microsoft.com/v1.0/users", app_token,
                   query = list(`$filter` = paste0("userPrincipalName eq '", addr, "' or mail eq '", addr, "'"),
                                `$select` = "id"))
  v <- res$content$value
  list(status = res$status,
       id = if (!is.null(v) && length(v) > 0) v[[1]]$id %||% NA_character_ else NA_character_)
}

#' Ausgang der Konto-Aufloesung per E-Mail (rein)
#'
#' @param status HTTP-Status der Abfrage (NA bei Exception).
#' @param id Gefundene object_id oder NA.
#' @return list(ausgang, http_status); `ausgang` NA, wenn das Konto aufgeloest ist
#'   und die Suche weiterlaufen kann.
#' @keywords internal
lookup_outcome_konto <- function(status, id) {
  # ---- start ---- #
  if (isTRUE(status == 200) && !is.na(id)) return(list(ausgang = NA_character_, http_status = NA_integer_))
  if (isTRUE(status == 200)) return(list(ausgang = "organisator_unbekannt", http_status = NA_integer_))
  list(ausgang = "abruf_fehler", http_status = as.integer(status))
}

#' Ausgang der Online-Meeting-Suche ueber JoinWebUrl (rein)
#'
#' 404 auf `/users/{oid}/onlineMeetings` heisst: das Konto gibt es bei Microsoft
#' nicht mehr.
#'
#' @param status HTTP-Status von `resolve_meeting()` (NA bei Exception).
#' @param id Gefundene onlineMeeting-id oder NA.
#' @return list(ausgang, http_status); `ausgang` NA, wenn das Meeting gefunden ist.
#' @keywords internal
lookup_outcome_suche <- function(status, id) {
  # ---- start ---- #
  ausgang <- if (isTRUE(status == 403)) "policy_403"
    else if (isTRUE(status == 404)) "organisator_unbekannt"
    else if (isTRUE(status == 200) && is.na(id)) "nicht_gefunden"
    else if (isTRUE(status == 200)) NA_character_
    else "abruf_fehler"
  list(ausgang = ausgang,
       http_status = if (identical(ausgang, "abruf_fehler")) as.integer(status) else NA_integer_)
}

#' Ausgang des Berichtsabrufs (rein)
#'
#' @param status HTTP-Status von `attendance_records()` (NA bei Exception).
#' @param n_sessions Anzahl Sessions aus `sessions_from_reports()` (0 ohne Berichte).
#' @return list(ausgang, http_status).
#' @keywords internal
lookup_outcome_bericht <- function(status, n_sessions) {
  # ---- start ---- #
  if (!isTRUE(status == 200)) return(list(ausgang = "abruf_fehler", http_status = as.integer(status)))
  list(ausgang = if (n_sessions > 0) "gefunden_mit_bericht" else "gefunden_ohne_bericht",
       http_status = NA_integer_)
}

#' Versuche zu einem Ausgang je Link verdichten (rein)
#'
#' @param versuche data.frame(join_url, ausgang, http_status), eine Zeile je
#'   Discovery-Zeile.
#' @return tibble(join_url, online_meeting_lookup, online_meeting_lookup_http_status),
#'   eine Zeile je join_url mit dem besten Ausgang nach `ONLINE_MEETING_LOOKUP_RANG`.
#'   Der HTTP-Status steht nur bei `abruf_fehler`.
#' @keywords internal
aggregate_online_meeting_lookups <- function(versuche) {
  # ---- start ---- #
  versuche %>%
    dplyr::mutate(rang = match(ausgang, ONLINE_MEETING_LOOKUP_RANG)) %>%
    dplyr::group_by(join_url) %>%
    dplyr::slice_min(rang, n = 1, with_ties = FALSE) %>%
    dplyr::ungroup() %>%
    dplyr::transmute(
      join_url,
      online_meeting_lookup = ausgang,
      online_meeting_lookup_http_status = dplyr::if_else(
        ausgang == "abruf_fehler", as.integer(http_status), NA_integer_))
}

#' UPDATE-Statement der Ausgaenge (rein)
#'
#' Eigene Funktion, damit die Bedingungen ohne Datenbank pruefbar sind:
#' 1. Nur Zeilen, deren Wert sich aendert (`IS DISTINCT FROM`). Sonst zoege
#'    `trigger_set_updated_at` jede Nacht `updated_at` des ganzen Fensters mit.
#' 2. `online_meeting_lookup_at` springt nur, wenn sich der Ausgang aendert: er
#'    ist der Zeitpunkt, seit dem der aktuelle Ausgang gilt. Ein anderer
#'    HTTP-Status bei weiter `abruf_fehler` laesst ihn stehen.
#' 3. Ein gefundenes Meeting wird nie verschlechtert: Nach dem Ausscheiden ist
#'    das Konto bei Microsoft weg, und der Befund aus der Zeit davor ist der
#'    einzige. Zwischen den beiden gefunden_-Werten gilt der neue.
#'
#' @param rs Ziel-Schema.
#' @param tmp Name der Temp-Tabelle (join_url, lookup, http_status).
#' @return SQL-Statement als character.
#' @keywords internal
online_meeting_lookup_sql <- function(rs, tmp) {
  # ---- start ---- #
  sprintf("
    UPDATE %s.msgraph_events e
       SET online_meeting_lookup             = t.lookup,
           online_meeting_lookup_http_status = t.http_status,
           online_meeting_lookup_at          = CASE
             WHEN e.online_meeting_lookup IS DISTINCT FROM t.lookup
             THEN timezone('UTC', now())
             ELSE e.online_meeting_lookup_at END
      FROM %s t
     WHERE e.join_url = t.join_url
       AND (e.online_meeting_lookup IS DISTINCT FROM t.lookup
            OR e.online_meeting_lookup_http_status IS DISTINCT FROM t.http_status)
       AND (e.online_meeting_lookup IS NULL
            OR e.online_meeting_lookup NOT IN ('gefunden_mit_bericht', 'gefunden_ohne_bericht')
            OR t.lookup IN ('gefunden_mit_bericht', 'gefunden_ohne_bericht'))", rs, tmp)
}

#' Abbrechen, wenn die Ausgangs-Spalten auf msgraph_events fehlen
#'
#' @param con Pool oder DBI-Verbindung.
#' @param rs Ziel-Schema (config-Schalter, i.d.R. "raw").
#' @return invisible(TRUE), sonst Fehler mit Verweis auf die Migration.
#' @keywords internal
assert_online_meeting_lookup_columns <- function(con, rs) {
  # ---- start ---- #
  n_spalten <- DBI::dbGetQuery(con, "
    SELECT count(*) AS n FROM information_schema.columns
     WHERE table_schema = $1 AND table_name = 'msgraph_events'
       AND column_name IN ('online_meeting_lookup', 'online_meeting_lookup_http_status',
                           'online_meeting_lookup_at')", params = list(rs))$n
  if (as.numeric(n_spalten) < 3) {
    stop(sprintf(paste0(
      "Spalten %s.msgraph_events.online_meeting_lookup* fehlen. Erst die Migration ",
      "ausfuehren: dbconnectorR/inst/sql/2026-10-07-msgraph-events-online-meeting-lookup.sql ",
      "(sie aendert nur raw; fuer ein anderes Schema die ALTER-Zeilen dort nachziehen)."), rs))
  }
  invisible(TRUE)
}

#' Ausgaenge der Online-Meeting-Suche auf msgraph_events schreiben
#'
#' Setzt den Ausgang auf ALLE Events mit derselben join_url (Vorbild
#' `mark_join_url_checked()`): Temp-Tabelle + UPDATE in einer Transaktion auf
#' einer Verbindung. Die Bedingungen des UPDATE stehen in
#' `online_meeting_lookup_sql()`.
#'
#' @param con Pool oder DBI-Verbindung.
#' @param rs Ziel-Schema (config-Schalter, i.d.R. "raw").
#' @param ausgaenge `aggregate_online_meeting_lookups()`.
#' @return invisible(Anzahl geaenderter Event-Zeilen).
#' @keywords internal
write_online_meeting_lookups <- function(con, rs, ausgaenge) {
  # ---- start ---- #
  if (nrow(ausgaenge) == 0) return(invisible(0L))
  werte <- data.frame(
    join_url    = as.character(ausgaenge$join_url),
    lookup      = as.character(ausgaenge$online_meeting_lookup),
    http_status = as.integer(ausgaenge$online_meeting_lookup_http_status),
    stringsAsFactors = FALSE)
  tmp <- "tmp_online_meeting_lookup"
  sql <- online_meeting_lookup_sql(rs, tmp)
  # Eine Transaktion auf EINER Verbindung: die Temp-Tabelle ueberlebt keinen
  # Pool-Checkout, ein zweiter Checkout saehe sie nicht mehr.
  schreibe <- function(conn) {
    DBI::dbWriteTable(conn, tmp, werte, temporary = TRUE, overwrite = TRUE)
    DBI::dbExecute(conn, sql)
  }
  n <- if (inherits(con, "Pool")) {
    pool::poolWithTransaction(con, schreibe)
  } else {
    DBI::dbBegin(con)
    res <- tryCatch(schreibe(con), error = function(e) { DBI::dbRollback(con); stop(e) })
    DBI::dbCommit(con)
    res
  }
  message(sprintf("Online-Meeting-Ausgang: %d Event-Zeile(n) geaendert.", n))
  invisible(n)
}

#' Calls/Teilnehmer gescopt via Attendance aktualisieren
#'
#' Discovery aus den delegiert ingestierten Events (`discover_meetings_from_events`),
#' Meeting-Aufloesung + Attendance app-only (CsApplicationAccessPolicy-gescoped).
#'
#' Je Link (join_url) entsteht ein Ausgang der Online-Meeting-Suche
#' (`gefunden_mit_bericht`, `gefunden_ohne_bericht`, `nicht_gefunden`,
#' `policy_403`, `organisator_unbekannt`, `abruf_fehler`). Er wird nach der
#' Schleife auf alle Events mit dieser join_url geschrieben, auch wenn kein
#' Call entstand - nicht bei `dry_run` und nicht, wenn der Lauf abbricht.
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
  konten <- list()               # E-Mail -> list(status, id); je E-Mail nur eine Graph-Abfrage pro Lauf
  # Ausgang je Discovery-Zeile. Er ersetzt die frueheren Zaehler: bisher fiel
  # jeder Fehlschlag stumm durch 'next', ein abgelaufener Token oder ein
  # Graph-Ausfall sah dadurch aus wie "keine Calls" - und weiter unten wie eine
  # Welle von No-Shows. `stufe` trennt Suche (konto/suche) und Berichtsabruf.
  versuche <- vector("list", nrow(disc))
  n_session_ohne_start <- 0L
  # Graph listet hoechstens die 50 juengsten Berichte eines Online-Meetings. Bei
  # einem viel genutzten persoenlichen Link fehlen aeltere Sessions dann still -
  # deshalb zaehlen, wie oft die Grenze erreicht ist.
  GRAPH_MAX_BERICHTE <- 50L; n_bericht_grenze <- 0L
  for (i in seq_len(nrow(disc))) {
    ju <- disc$join_url[i]; oid <- disc$organizer_oid[i]
    if (is.na(oid)) {
      # Organisator ohne Zeile in msgraph_users: Konto per E-Mail aufloesen
      email <- disc$organizer_email[i]
      if (is.na(email) || !nzchar(email)) {
        versuche[[i]] <- list(ausgang = "organisator_unbekannt", http_status = NA_integer_, stufe = "konto")
        next
      }
      if (is.null(konten[[email]]))
        konten[[email]] <- tryCatch(resolve_organizer_by_email(email, app_token),
                                    error = function(e) list(status = NA, id = NA_character_))
      ko <- lookup_outcome_konto(konten[[email]]$status, konten[[email]]$id)
      if (!is.na(ko$ausgang)) { versuche[[i]] <- c(ko, stufe = "konto"); next }
      oid <- konten[[email]]$id
    }
    if (oid %in% blocked_oids) {
      # Uebersprungen, weil die Policy diesen Organizer schon im selben Lauf ablehnte
      versuche[[i]] <- list(ausgang = "policy_403", http_status = NA_integer_, stufe = "suche")
      next
    }
    mt <- tryCatch(resolve_meeting(oid, ju, app_token), error = function(e) list(status = NA, id = NA_character_))
    su <- lookup_outcome_suche(mt$status, mt$id)
    if (!is.na(su$ausgang)) {
      # Jeder Ausgang der Suche ausser einem Treffer endet hier. Nur policy_403
      # sperrt zusaetzlich den Organizer fuer den Rest des Laufs. Fuer die
      # Fehlerquote unten sind policy_403 und organisator_unbekannt erwartete
      # Abgrenzung und zaehlen nicht hinein; nicht_gefunden und abruf_fehler
      # zaehlen als Fehlschlag.
      if (su$ausgang == "policy_403") blocked_oids <- c(blocked_oids, oid)
      versuche[[i]] <- c(su, stufe = "suche"); next
    }
    at <- tryCatch(attendance_records(oid, mt$id, app_token),
                   error = function(e) list(status = NA, meeting_start = NA, meeting_end = NA, reports = list()))
    # Keine Reports ist KEIN Fehler: ein Meeting, an dem niemand teilgenommen
    # hat, liefert legitim nichts - das ist der echte No-Show.
    if (!isTRUE(at$status == 200) || length(at$reports) == 0) {
      versuche[[i]] <- c(lookup_outcome_bericht(at$status, 0L), stufe = "bericht"); next
    }
    if (length(at$reports) >= GRAPH_MAX_BERICHTE) n_bericht_grenze <- n_bericht_grenze + 1L
    # meeting_id = thread-id aus der joinUrl, identische Ableitung wie in
    # parse_scoped_events und im alten base-35-Pfad (msgraph_calls.R:414). Nur so
    # paart msgraph_map_calls_events den Call mit seinem Event; ohne das bleibt
    # jedes Event ohne Call und wird als No-Show klassifiziert.
    mid_thread <- extract_meeting_id_safe(ju)
    if (is.na(mid_thread))
      message("meeting_id nicht aus joinUrl ableitbar, Call bleibt ohne Event-Zuordnung: ",
              substr(ju, 1, 90))
    # Ein Call je Session (= Anwesenheitsbericht), nicht je Online-Meeting: ein
    # wiederverwendeter Teams-Link hat viele Sessions an verschiedenen Tagen.
    sess <- sessions_from_reports(at$reports, online_meeting_id = mt$id,
                                  meeting_id = mid_thread, tenant_id = cfg$tenant_id)
    n_session_ohne_start <- n_session_ohne_start + attr(sess, "n_ohne_start")
    versuche[[i]] <- c(lookup_outcome_bericht(at$status, nrow(sess$calls)), stufe = "bericht")
    if (nrow(sess$calls) == 0) next
    calls[[length(calls) + 1]] <- sess$calls
    parts[[length(parts) + 1]] <- sess$parts
  }
  versuche_df <- tibble::tibble(
    join_url    = disc$join_url,
    ausgang     = vapply(versuche, function(v) v$ausgang, character(1)),
    http_status = vapply(versuche, function(v) as.integer(v$http_status), integer(1)),
    stufe       = vapply(versuche, function(v) v$stufe, character(1)))
  ausgaenge <- aggregate_online_meeting_lookups(versuche_df)
  n_je_ausgang <- table(factor(ausgaenge$online_meeting_lookup, levels = ONLINE_MEETING_LOOKUP_RANG))
  message("Online-Meeting-Ausgang je Link: ",
          paste(names(n_je_ausgang), as.integer(n_je_ausgang), sep = " ", collapse = ", "))
  if (n_session_ohne_start > 0)
    message(n_session_ohne_start, " Anwesenheitsbericht(e) ohne ID oder Startzeit uebersprungen.")
  if (n_bericht_grenze > 0)
    message(n_bericht_grenze, " Online-Meeting(s) mit ", GRAPH_MAX_BERICHTE,
            " Berichten (Graph-Grenze): aeltere Sessions dieser Links liefert Graph nicht mehr.")
  if (length(blocked_oids) > 0)
    message("Policy-403 fuer ", length(blocked_oids), " Organizer-oid(s) — deren Meetings uebersprungen.")

  # Laut ausfallen statt still nichts zu schreiben. Beide Faelle bedeuten, dass
  # der Job zwar Meetings gefunden, aber keine belastbaren Daten geholt hat -
  # jedes betroffene Event wird downstream sonst zum No-Show. Gezaehlt wird je
  # Discovery-Zeile. nicht_gefunden zaehlt weiter als Resolve-Fehler: ist die
  # Suche systematisch kaputt, soll der Lauf laut abbrechen.
  n_versucht <- nrow(versuche_df)
  n_policy_403 <- sum(versuche_df$ausgang == "policy_403")
  n_organisator_unbekannt <- sum(versuche_df$ausgang == "organisator_unbekannt")
  n_resolve_fehler <- sum(versuche_df$stufe %in% c("konto", "suche") &
                            versuche_df$ausgang %in% c("nicht_gefunden", "abruf_fehler"))
  n_attendance_fehler <- sum(versuche_df$stufe == "bericht" & versuche_df$ausgang == "abruf_fehler")
  n_fehler <- n_resolve_fehler + n_attendance_fehler
  # 403 und unbekannter Organisator sind Abgrenzung, kein Fehlschlag
  n_bewertbar <- n_versucht - n_policy_403 - n_organisator_unbekannt

  # Mindestmenge, bevor eine Quote ueberhaupt aussagekraeftig ist. Ohne sie
  # kippt ein einzelner transienter 500 an einem ruhigen Tag - Feiertag, Ferien,
  # Wochenende - den kompletten Lauf: run_data_job wirft den Fehler weiter und
  # reisst die uebrigen Jobs in do/main.R mit.
  MIN_BEWERTBAR_FUER_QUOTE <- 10L
  if (n_bewertbar >= MIN_BEWERTBAR_FUER_QUOTE && n_fehler > 0.5 * n_bewertbar) {
    stop(sprintf(paste0(
      "msgraph_scoped_update_calls_attendance: %d von %d bewertbaren Meetings scheiterten ",
      "an Graph (%d resolve, %d attendance). Ueber der Haelfte - vermutlich Token oder ",
      "Graph-Ausfall. Abbruch, statt die fehlenden Calls als No-Shows wirken zu lassen."),
      n_fehler, n_bewertbar, n_resolve_fehler, n_attendance_fehler))
  }
  # Null Calls: nur abbrechen, wenn tatsaechlich etwas fehlgeschlagen ist. Null
  # Calls bei null Fehlern ist der Normalfall an einem Tag, an dem die gefundenen
  # Meetings schlicht niemand besucht hat - das IST der echte No-Show und darf
  # den Lauf nicht abbrechen.
  if (length(calls) == 0 && n_fehler > 0) {
    stop(sprintf(paste0(
      "msgraph_scoped_update_calls_attendance: %d bewertbare Meetings versucht, kein ",
      "einziger Attendance-Report verwertbar (%d resolve-, %d attendance-Fehler). Abbruch."),
      n_bewertbar, n_resolve_fehler, n_attendance_fehler))
  }
  # Ausgaenge erst nach den Abbruch-Pruefungen schreiben: ein abgebrochener Lauf
  # schreibt nichts. Die Spalten vor jedem Schreiben pruefen, auch vor den Calls.
  if (dry_run) {
    message(sprintf("[dry-run] %d Link-Ausgaenge (nicht geschrieben).", nrow(ausgaenge)))
  } else {
    assert_online_meeting_lookup_columns(con, rs)
  }
  if (length(calls) == 0) {
    if (!dry_run) write_online_meeting_lookups(con, rs, ausgaenge)
    message(sprintf("Keine Calls/Attendance (%d bewertbare Meetings, keine Fehler).", n_bewertbar))
    return(invisible(0L))
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

  # Ohne die Spalte verwirft postgres_upsert_data sie still, und die Transkripte
  # verloeren ihren Graph-Griff. Deshalb vor jedem Schreiben pruefen.
  assert_online_meeting_id_column(con, rs)
  # Bestand aus der Zeit vor dem Session-Schluessel umschluesseln, BEVOR der
  # Upsert laeuft - sonst legte er fuer diese Session eine zweite Zeile an.
  rekey_meeting_calls(con, rs, calls_df)
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
    # zwei Records eines Berichts koennen auf denselben Kontakt fallen
    dplyr::distinct(call_id, contact_id)
  Billomatics::postgres_upsert_data(con, rs, "msgraph_call_participants", cp,
                                    match_cols = c("call_id", "contact_id"))
  # Nach den Calls: gefunden_mit_bericht steht erst, wenn die Sessions geschrieben sind
  write_online_meeting_lookups(con, rs, ausgaenge)
  invisible(nrow(calls_df))
}
