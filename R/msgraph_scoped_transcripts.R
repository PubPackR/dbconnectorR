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
#' nicht-Organisatoren geben 403/leer. Der Organisator wird (noch) nicht separat
#' gespeichert, daher werden alle internen Teilnehmer-Kandidaten durchprobiert und
#' der erste mit 200 + Transkripten genommen.
#'
#' @param cands Character-Vektor kandidierender object_ids (interne Teilnehmer).
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
  # Calls im Sliding Window, die noch KEIN Transkript haben. Angesprochen wird
  # Graph ueber die onlineMeeting-id, die in msgraph_call_id steht - NICHT ueber
  # meeting_id, das seit dem Mapping-Fix die thread-id des Events traegt.
  window_start <- Sys.Date() - cfg$transcripts_sliding_window_days
  calls <- dplyr::tbl(con, I(paste0(rs, ".msgraph_calls"))) %>%
    dplyr::filter(!is.na(msgraph_call_id) & call_start >= !!format(window_start, "%Y-%m-%d")) %>%
    dplyr::select(call_db_id = id, msgraph_call_id) %>% dplyr::collect()
  if (nrow(calls) == 0) { message("Keine Calls im Fenster."); return(invisible(0L)) }
  have <- dplyr::tbl(con, I(paste0(ps, ".msgraph_call_transcripts"))) %>%
    dplyr::select(transcript_id, call_id) %>% dplyr::collect()

  # Kandidaten-object_ids je Meeting = alle INTERNEN Teilnehmer der FENSTER-Calls
  # (Details zur Organizer-Scoping-Logik siehe resolve_transcript_source). Auf die
  # Fenster-Calls beschraenkt, statt die ganze Calls-/Teilnehmer-Tabelle zu joinen.
  # rs kommt aus der Config (kein User-Input) -> sichere String-Interpolation;
  # die msgraph_call_ids werden per dbQuoteLiteral sicher gequotet.
  quoted_ids <- paste(DBI::dbQuoteLiteral(con, calls$msgraph_call_id), collapse = ", ")
  cand_lookup <- DBI::dbGetQuery(con, sprintf("
    SELECT DISTINCT c.msgraph_call_id, u.msgraph_user_id AS object_id
    FROM %1$s.msgraph_calls c
    JOIN %1$s.msgraph_call_participants p ON p.call_id = c.id
    JOIN %1$s.msgraph_contacts ct          ON ct.id = p.contact_id
    JOIN %1$s.msgraph_users u              ON lower(u.email) = lower(ct.email)
    WHERE u.is_internal AND NOT u.is_deleted AND c.msgraph_call_id IN (%2$s)", rs, quoted_ids))
  cand_map <- split(cand_lookup$object_id, cand_lookup$msgraph_call_id)

  new_rows <- list()
  for (i in seq_len(nrow(calls))) {
    mid <- calls$msgraph_call_id[i]; call_db_id <- calls$call_db_id[i]
    cands <- cand_map[[calls$msgraph_call_id[i]]]
    if (is.null(cands) || length(cands) == 0) next
    src <- resolve_transcript_source(cands, mid, app_token)
    if (is.null(src)) next
    oid <- src$oid
    for (t in src$value) {
      tid <- t$id %||% NA_character_
      if (is.na(tid) || tid %in% have$transcript_id) next
      url <- sprintf("https://graph.microsoft.com/v1.0/users/%s/onlineMeetings/%s/transcripts/%s/content",
                     oid, utils::URLencode(mid, reserved = TRUE), utils::URLencode(tid, reserved = TRUE))
      vtt <- tryCatch(fetch_with_retry(paste0(url, "?$format=text/vtt"), app_token,
                                       accept = "text/vtt", parse = "text"),
                      error = function(e) NULL)
      if (is.null(vtt)) next
      new_rows[[length(new_rows) + 1]] <- tibble::tibble(
        transcript_id = tid, call_id = call_db_id, transcript_url = url,
        # Graph liefert createdDateTime als ISO-String -> parsen, die Zielspalte
        # ist timestamp (Upsert scheitert sonst am Typ-Mismatch)
        transcript_created_at = lubridate::ymd_hms(t$createdDateTime %||% NA_character_, quiet = TRUE),
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
