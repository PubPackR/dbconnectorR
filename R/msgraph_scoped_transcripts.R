#' VTT-Transkript in Plaintext (Sprecher + Text, ohne Zeitstempel)
#'
#' @param vtt VTT-String.
#'
#' @return Plaintext-String.
#'
#' @export
vtt_to_plaintext <- function(vtt) {
  # ---- start ---- #
  lines <- strsplit(vtt, "\r?\n")[[1]]
  keep <- lines[!grepl("^WEBVTT", lines) &
                  !grepl("-->", lines) &
                  !grepl("^\\s*$", lines) &
                  !grepl("^[0-9a-fA-F-]{8,}$", lines)]        # Cue-IDs
  # <v Speaker>Text</v> -> "Speaker: Text"
  keep <- gsub("<v ([^>]+)>(.*?)</v>", "\\1: \\2", keep)
  keep <- gsub("<[^>]+>", "", keep)                          # sonstige Tags
  paste(trimws(keep), collapse = "\n")
}

#' Transkript-Quelle je Meeting aufloesen (Organisator-oid durchprobieren)
#'
#' Das onlineMeeting ist organizer-scoped: nur die oid des Organisators liefert am
#' `/onlineMeetings/{id}/transcripts`-Endpoint HTTP 200 mit Transkripten;
#' nicht-Organisatoren geben 403/leer. Der Job stellt den Meeting-Besitzer aus der
#' Online-Meeting-ID an die erste Stelle, danach die internen Teilnehmer; der
#' erste mit 200 + Transkripten wird genommen.
#'
#' @param cands Character-Vektor kandidierender object_ids in Abfrage-Reihenfolge
#'   (Meeting-Besitzer, dann interne Teilnehmer).
#' @param mid onlineMeeting-id.
#' @param app_token app-only Provider.
#' @return list(oid, value) des ersten treffenden Kandidaten, oder NULL.
#' @keywords internal
resolve_transcript_source <- function(cands, mid, app_token) {
  # ---- start ---- #
  for (cand in cands) {
    resp <- tryCatch(graph_collect(sprintf(
      "https://graph.microsoft.com/v1.0/users/%s/onlineMeetings/%s/transcripts",
      cand, utils::URLencode(mid, reserved = TRUE)), app_token),
      error = function(e) list(status = NA, value = list()))
    if (isTRUE(resp$status == 200) && length(resp$value) > 0) return(list(oid = cand, value = resp$value))
  }
  NULL
}

#' Meeting-Besitzer aus einer Online-Meeting-ID (rein)
#'
#' Die Online-Meeting-ID aus Graph ist base64 von `1*<oid>*0**<thread-id>`
#' (siehe `build_online_meeting_id`). Die oid ist die des Meeting-Besitzers, der
#' den Teams-Link angelegt hat - nur unter ihr liefert der Transkript-Endpoint.
#'
#' @param online_meeting_id Online-Meeting-ID, mit oder ohne base64-Padding.
#' @return oid kleingeschrieben, oder `NA_character_`, wenn keine ablesbar ist
#'   (z.B. Bericht-ID aus dem base-35-Bestand).
#' @keywords internal
meeting_owner_oid_from_online_meeting_id <- function(online_meeting_id) {
  # ---- start ---- #
  if (length(online_meeting_id) != 1 || is.na(online_meeting_id) || !nzchar(online_meeting_id))
    return(NA_character_)
  # URL-sicheres Alphabet (-_) auf das Standard-Alphabet (+/) abbilden
  b64 <- chartr("-_", "+/", online_meeting_id)
  if (nchar(b64) %% 4 != 0) b64 <- paste0(b64, strrep("=", 4 - nchar(b64) %% 4))
  klartext <- tryCatch(rawToChar(jsonlite::base64_dec(b64)), error = function(e) NA_character_)
  if (is.na(klartext)) return(NA_character_)
  # useBytes: eine Bericht-ID (base-35-Bestand) dekodiert zu beliebigen Bytes,
  # die in der Locale ungueltig sind - ohne useBytes bricht regexec den Job ab.
  treffer <- regmatches(klartext, regexec("^1\\*([0-9a-fA-F-]{36})\\*", klartext, useBytes = TRUE))[[1]]
  if (length(treffer) < 2) return(NA_character_)
  tolower(treffer[2])
}

#' Calls im Transkript-Fenster auswaehlen
#'
#' Ein Call gehoert ins Fenster, wenn sein Termin ODER sein Eingang in der DB
#' (`created_at`) im Fenster liegt. Nur auf den Termin zu schauen, verliert jeden
#' Call, der spaeter als die Fenstergroesse nach dem Termin ankommt: der Calls-Job
#' findet Meetings ueber den Organisator, und den kennt er erst ab dessen
#' Kalenderfreigabe (neue Vertriebler, spaete Freigaben, Nachzug nach Ausfaellen).
#'
#' @param calls Lazy oder lokale Tabelle mit `call_start` und `created_at`.
#' @param window_start Date, erster Tag des Fensters.
#' @return `calls`, gefiltert.
#' @keywords internal
filter_transcript_window_calls <- function(calls, window_start) {
  # ---- start ---- #
  ws <- format(window_start, "%Y-%m-%d")
  dplyr::filter(calls, call_start >= !!ws | created_at >= !!ws)
}

#' Transkript der Session seines Online-Meetings zuordnen (rein)
#'
#' Die Sessions eines wiederverwendeten Teams-Links teilen sich die
#' Transkript-Liste in Graph. Ein Transkript gehoert zu der Session, waehrend der
#' es entstanden ist. Liegt `createdDateTime` in keiner (Uhrversatz, Ende fehlt),
#' gewinnt die letzte Session, die davor begonnen hat; ohne eine solche die
#' frueheste.
#'
#' @param sessions data.frame(call_db_id, call_start, call_end) der Sessions eines
#'   Online-Meetings (mindestens eine Zeile).
#' @param created POSIXct, `createdDateTime` des Transkripts (darf NA sein).
#' @return `call_db_id` der zugeordneten Session.
#' @keywords internal
assign_transcript_session <- function(sessions, created) {
  # ---- start ---- #
  sessions <- sessions[order(sessions$call_start), , drop = FALSE]
  if (is.na(created)) return(sessions$call_db_id[1])
  waehrend <- which(sessions$call_start <= created & sessions$call_end >= created)
  if (length(waehrend) > 0) return(sessions$call_db_id[max(waehrend)])
  davor <- which(sessions$call_start <= created)
  if (length(davor) > 0) return(sessions$call_db_id[max(davor)])
  sessions$call_db_id[1]
}

#' Transkripte gescopet aktualisieren (Sliding Window, policy-gescopte Meeting-Kette)
#'
#' @param con
#'   DB-Pool.
#'
#' @param app_token
#'   app-only Provider.
#'
#' @param cfg
#'   load_scoped_config(); `raw_schema`/`processed_schema` steuern das Ziel-Schema.
#'
#' @param dry_run
#'   Wenn TRUE: nur zaehlen/loggen, kein Upsert.
#'
#' @return
#'   invisible(Anzahl neu geholter Transkripte).
#'
#' @export
msgraph_scoped_update_transcripts <- function(con, app_token, cfg, dry_run = FALSE) {
  # ---- start ---- #
  rs <- cfg$raw_schema %||% "raw"
  ps <- cfg$processed_schema %||% "processed"
  # Calls im Sliding Window. KEIN Filter auf meeting_id: das Feld traegt seit dem
  # Mapping-Fix die thread-id des Events und sagt nichts darueber aus, ob der Call
  # in Graph adressierbar ist. Das tut die onlineMeeting-id, und genau die geht in
  # die Graph-URL. Ein Filter auf meeting_id wuerde Calls aussortieren, deren
  # joinUrl sich nicht parsen liess, obwohl ihr Transkript abrufbar waere.
  # Transkriptverlust waere schlimmer als die paar Fehlversuche auf Alt-Calls aus
  # dem base-35-Bestand, die ohnehin im Fenster liegen.
  #
  # graph_mid: seit dem Session-Schluessel (ADR 0002) steht die onlineMeeting-id
  # in msgraph_online_meeting_id, msgraph_call_id ist die Bericht-ID. Zeilen ohne
  # die Spalte (base-35, noch nicht umgeschluesselt) fallen auf msgraph_call_id
  # zurueck, wie vor dem Umbau.
  window_start <- Sys.Date() - cfg$transcripts_sliding_window_days
  calls_tbl <- dplyr::tbl(con, I(paste0(rs, ".msgraph_calls"))) %>%
    dplyr::mutate(graph_mid = dplyr::coalesce(msgraph_online_meeting_id, msgraph_call_id))
  mids <- calls_tbl %>%
    filter_transcript_window_calls(window_start) %>%
    dplyr::distinct(graph_mid) %>% dplyr::collect() %>% dplyr::pull(graph_mid)
  if (length(mids) == 0) { message("Keine Calls im Fenster."); return(invisible(0L)) }
  # Alle Sessions dieser Online-Meetings, auch die ausserhalb des Fensters: ein
  # Transkript muss an seine eigene Session, nicht an die zufaellig im Fenster.
  sessions <- calls_tbl %>%
    dplyr::filter(graph_mid %in% !!mids) %>%
    dplyr::select(call_db_id = id, graph_mid, call_start, call_end) %>% dplyr::collect()
  have <- dplyr::tbl(con, I(paste0(ps, ".msgraph_call_transcripts"))) %>%
    dplyr::select(transcript_id, call_id) %>% dplyr::collect()

  # Kandidaten-object_ids je Online-Meeting = alle INTERNEN Teilnehmer seiner
  # Sessions (Details zur Organizer-Scoping-Logik siehe resolve_transcript_source),
  # in der Schleife unten ergaenzt um den Meeting-Besitzer an erster Stelle.
  # rs kommt aus der Config (kein User-Input) -> sichere String-Interpolation;
  # die ids werden per dbQuoteLiteral sicher gequotet.
  quoted_ids <- paste(DBI::dbQuoteLiteral(con, mids), collapse = ", ")
  cand_lookup <- DBI::dbGetQuery(con, sprintf("
    SELECT DISTINCT COALESCE(c.msgraph_online_meeting_id, c.msgraph_call_id) AS graph_mid,
           u.msgraph_user_id AS object_id
    FROM %1$s.msgraph_calls c
    JOIN %1$s.msgraph_call_participants p ON p.call_id = c.id
    JOIN %1$s.msgraph_contacts ct          ON ct.id = p.contact_id
    JOIN %1$s.msgraph_users u              ON lower(u.email) = lower(ct.email)
    WHERE u.is_internal AND NOT u.is_deleted
      AND COALESCE(c.msgraph_online_meeting_id, c.msgraph_call_id) IN (%2$s)", rs, quoted_ids))
  cand_map <- split(cand_lookup$object_id, cand_lookup$graph_mid)

  # Je Online-Meeting genau eine Abfrage: mehrere Sessions teilen sich dieselben
  # Transkripte, und dieselbe transcript_id zweimal im Upsert liesse ON CONFLICT
  # scheitern.
  new_rows <- list()
  for (mid in mids) {
    # Der Meeting-Besitzer zuerst: nur unter seiner oid liefert Graph Transkripte.
    # Seit dem Calls-Job mit der oid aus der joinUrl entstehen Calls zu Links, die
    # nicht der Kalender-Person gehoeren - nimmt der Besitzer nicht teil, steht er
    # unter den internen Teilnehmern gar nicht drin.
    besitzer <- meeting_owner_oid_from_online_meeting_id(mid)
    cands <- c(besitzer[!is.na(besitzer)], cand_map[[mid]])
    cands <- cands[!duplicated(tolower(cands))]   # oids sind GUIDs, Schreibweise egal
    if (length(cands) == 0) next
    src <- resolve_transcript_source(cands, mid, app_token)
    if (is.null(src)) next
    oid <- src$oid
    for (t in src$value) {
      tid <- t$id %||% NA_character_
      if (is.na(tid) || tid %in% have$transcript_id) next
      # Graph liefert createdDateTime als ISO-String -> parsen, die Zielspalte
      # ist timestamp (Upsert scheitert sonst am Typ-Mismatch)
      created <- lubridate::ymd_hms(t$createdDateTime %||% NA_character_, quiet = TRUE)
      call_db_id <- assign_transcript_session(sessions[sessions$graph_mid == mid, ], created)
      url <- sprintf("https://graph.microsoft.com/v1.0/users/%s/onlineMeetings/%s/transcripts/%s/content",
                     oid, utils::URLencode(mid, reserved = TRUE), utils::URLencode(tid, reserved = TRUE))
      vtt <- tryCatch(fetch_with_retry(paste0(url, "?$format=text/vtt"), app_token,
                                       accept = "text/vtt", parse = "text"),
                      error = function(e) NULL)
      if (is.null(vtt)) next
      new_rows[[length(new_rows) + 1]] <- tibble::tibble(
        transcript_id = tid, call_id = call_db_id, transcript_url = url,
        transcript_created_at = created,
        transcript_content = vtt_to_plaintext(vtt))
    }
  }
  if (length(new_rows) == 0) { message("Keine neuen Transkripte."); return(invisible(0L)) }
  df <- dplyr::bind_rows(new_rows)
  if (dry_run) {
    message(sprintf("[dry-run] %d neue Transkripte (kein Upsert).", nrow(df)))
    return(invisible(nrow(df)))
  }
  Billomatics::postgres_upsert_data(con, ps, "msgraph_call_transcripts", df,
                                    match_cols = "transcript_id")
  invisible(nrow(df))
}
