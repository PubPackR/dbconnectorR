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

#' Transkripte gescopet aktualisieren (Sliding Window, policy-gescopte Meeting-Kette)
#'
#' @param con
#'   DB-Pool.
#'
#' @param app_token
#'   app-only Provider.
#'
#' @param cfg
#'   load_scoped_config().
#'
#' @return
#'   invisible(Anzahl neu geholter Transkripte).
#'
#' @export
msgraph_scoped_update_transcripts <- function(con, app_token, cfg) {
  # ---- start ---- #
  # Calls im Sliding Window, deren meeting_id noch KEIN Transkript hat
  window_start <- Sys.Date() - cfg$transcripts_sliding_window_days
  calls <- dplyr::tbl(con, I("raw.msgraph_calls")) %>%
    dplyr::filter(!is.na(meeting_id) & call_start >= !!format(window_start, "%Y-%m-%d")) %>%
    dplyr::select(call_db_id = id, msgraph_call_id, meeting_id) %>% dplyr::collect()
  have <- dplyr::tbl(con, I("processed.msgraph_call_transcripts")) %>%
    dplyr::select(transcript_id, call_id) %>% dplyr::collect()

  # object_id je Meeting: ueber den internen Organizer des zugehoerigen Events (mapping)
  # Vereinfachung: wir loesen den Organizer per erstem internen Teilnehmer des Calls auf.
  org_lookup <- DBI::dbGetQuery(con, "
    SELECT DISTINCT c.msgraph_call_id, u.msgraph_user_id AS object_id
    FROM raw.msgraph_calls c
    JOIN raw.msgraph_call_participants p ON p.call_id = c.id
    JOIN raw.msgraph_contacts ct ON ct.id = p.contact_id
    JOIN raw.msgraph_users u ON lower(u.email) = lower(ct.email)
    WHERE u.is_internal AND NOT u.is_deleted")
  org_map <- stats::setNames(org_lookup$object_id, org_lookup$msgraph_call_id)

  new_rows <- list()
  for (i in seq_len(nrow(calls))) {
    mid <- calls$meeting_id[i]; call_db_id <- calls$call_db_id[i]
    oid <- unname(org_map[calls$msgraph_call_id[i]])
    if (is.na(oid)) next
    tr <- tryCatch(graph_collect(sprintf(
      "https://graph.microsoft.com/v1.0/users/%s/onlineMeetings/%s/transcripts",
      oid, utils::URLencode(mid, reserved = TRUE)), app_token),
      error = function(e) list(status = NA, value = list()))
    if (!isTRUE(tr$status == 200) || length(tr$value) == 0) next
    for (t in tr$value) {
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
        transcript_created_at = t$createdDateTime %||% NA_character_,
        transcript_content = vtt_to_plaintext(vtt))
    }
  }
  if (length(new_rows) == 0) { message("Keine neuen Transkripte."); return(invisible(0L)) }
  df <- dplyr::bind_rows(new_rows)
  Billomatics::postgres_upsert_data(con, "processed", "msgraph_call_transcripts", df,
                                    match_cols = "transcript_id")
  invisible(nrow(df))
}
