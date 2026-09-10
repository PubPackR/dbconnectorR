#' Graph-Kalender-Events -> Zielschema (rein, ohne Netz)
#'
#' @param events_value
#'   Liste von Graph-Event-Objekten (aus calendarView $value).
#'
#' @return
#'   list(events, participants) als tibbles.
#'
#' @export
parse_scoped_events <- function(events_value) {
  # ---- start ---- #
  ev_rows <- list(); pt_rows <- list()
  for (e in events_value) {
    ical  <- e$iCalUId %||% NA_character_
    estart <- lubridate::ymd_hms(e$start$dateTime %||% NA_character_, quiet = TRUE)
    if (is.na(ical) || is.na(estart)) next
    ju <- e$onlineMeeting$joinUrl %||% NA_character_
    subj <- e$subject %||% NA_character_
    canceled <- isTRUE(e$isCancelled) ||
      (!is.na(subj) && grepl("^(Canceled:|Abgesagt:)", subj))
    ev_rows[[length(ev_rows) + 1]] <- tibble::tibble(
      msgraph_ical_uid   = ical,
      event_created_at   = lubridate::ymd_hms(e$createdDateTime %||% NA_character_, quiet = TRUE),
      event_updated_at   = lubridate::ymd_hms(e$lastModifiedDateTime %||% NA_character_, quiet = TRUE),
      subject            = subj,
      event_start        = estart,
      event_end          = lubridate::ymd_hms(e$end$dateTime %||% NA_character_, quiet = TRUE),
      meeting_id         = if (!is.na(ju)) extract_meeting_id_safe(ju) else NA_character_,
      join_url           = ju,
      is_single_instance = identical(e$type, "singleInstance"),
      is_online_meeting  = isTRUE(e$isOnlineMeeting),
      is_canceled        = canceled)
    # Teilnehmer: Organizer + attendees
    org <- e$organizer$emailAddress
    if (!is.null(org$address)) {
      pt_rows[[length(pt_rows) + 1]] <- tibble::tibble(
        msgraph_ical_uid = ical, event_start = estart,
        email = tolower(normalize_external_email(org$address)), ms_name = org$name %||% NA_character_,
        is_organizer = TRUE, source = "calendar")
    }
    for (a in e$attendees %||% list()) {
      addr <- a$emailAddress$address %||% NA_character_
      if (is.na(addr)) next
      pt_rows[[length(pt_rows) + 1]] <- tibble::tibble(
        msgraph_ical_uid = ical, event_start = estart,
        email = tolower(normalize_external_email(addr)), ms_name = a$emailAddress$name %||% NA_character_,
        is_organizer = FALSE, source = "calendar")
    }
  }
  list(
    # Dedup ueber den Upsert-Match-Key (nicht alle Spalten): dasselbe Meeting kann
    # aus mehreren freigegebenen Kalendern kommen und sich minimal unterscheiden ->
    # sonst "ON CONFLICT ... cannot affect row a second time".
    events = if (length(ev_rows)) {
      dplyr::distinct(dplyr::bind_rows(ev_rows), msgraph_ical_uid, event_start, .keep_all = TRUE)
    } else tibble::tibble(),
    participants = if (length(pt_rows)) {
      dplyr::bind_rows(pt_rows) %>%
        dplyr::arrange(dplyr::desc(is_organizer)) %>%
        dplyr::distinct(msgraph_ical_uid, event_start, email, .keep_all = TRUE)
    } else tibble::tibble())
}

#' Build a (msgraph_ical_uid, event_start, email) -> showAs Lookup (gescoped)
#'
#' Jede freigegebene Kalender-Kopie eines Events traegt ihr eigenes `showAs`.
#' `msgraph_scoped_update_events()` taggt jedes abgerufene Event vor dem
#' Zusammenfuehren mit der Owner-E-Mail des Kalenders, aus dem es kommt
#' (`X_cal_owner_email`, siehe dort). Diese Funktion baut daraus die
#' Zuordnung, die gebraucht wird, um `show_as` in `parse_scoped_events()`s
#' Teilnehmer-Output zu annotieren.
#'
#' @param events_value
#'   Liste von Graph-Event-Objekten, getaggt mit `X_cal_owner_email` (siehe
#'   `msgraph_scoped_update_events()`).
#'
#' @return
#'   Tibble mit `msgraph_ical_uid`, `event_start`, `email` (die getaggte
#'   Owner-E-Mail, lowercased), `show_as`. Events ohne Owner-Tag, ohne
#'   `iCalUId` oder ohne Start werden verworfen - kein show_as zuweisbar,
#'   erwarteter Zustand, kein Fehlerfall.
#'
#' @keywords internal
build_show_as_lookup_scoped <- function(events_value) {
  # ---- start ---- #
  # Vektoren statt einer Zeile pro tibble()/ymd_hms()-Aufruf: der eigentliche
  # Kostentreiber bei grossen events_value ist nicht (wie ein frueherer
  # Kommentar hier behauptete) der wachsende Liste-Anhang selbst - R-Listen
  # wachsen seit R 3.4 amortisiert linear, gemessen bei n=15.000: 0,00s fuer
  # den Listen-Anhang vs. 16,94s fuer per-row tibble() + 51,64s fuer per-row
  # ymd_hms() (Review-Fund, Issue 3). Deshalb hier ein Extraktions-Pass in
  # Basis-Vektoren, dann EIN tibble() und EIN vektorisiertes ymd_hms() statt
  # 15.000 einzelner Aufrufe.
  ical_v  <- vapply(events_value, function(e) e$iCalUId %||% NA_character_, character(1))
  start_v <- vapply(events_value, function(e) e$start$dateTime %||% NA_character_, character(1))
  owner_v <- vapply(events_value, function(e) e$X_cal_owner_email %||% NA_character_, character(1))
  showas_v <- vapply(events_value, function(e) e$showAs %||% NA_character_, character(1))

  estart_v <- lubridate::ymd_hms(start_v, quiet = TRUE)
  keep <- !is.na(ical_v) & !is.na(estart_v) & !is.na(owner_v) & nzchar(owner_v)

  if (!any(keep)) {
    return(tibble::tibble(
      msgraph_ical_uid = character(), event_start = as.POSIXct(character()),
      email = character(), show_as = character()))
  }

  tibble::tibble(
    msgraph_ical_uid = ical_v[keep],
    event_start       = estart_v[keep],
    email             = tolower(owner_v[keep]),
    show_as           = showas_v[keep]
  ) %>%
    dplyr::distinct(msgraph_ical_uid, event_start, email, .keep_all = TRUE)
}

#' Meeting-ID-Extraktion, NA-sicher
#'
#' @param url
#'   Teams/OnlineMeeting joinUrl.
#'
#' @return
#'   meeting_id als character, oder NA_character_ bei Fehler.
#'
#' @keywords internal
extract_meeting_id_safe <- function(url) {
  tryCatch(extract_meeting_id(url), error = function(e) NA_character_)
}

#' Kalender-Events der freigegebenen Kalender gescoped aktualisieren (delegiert)
#'
#' @param con
#'   DB-Pool.
#' @param del_token
#'   delegierter Token-Provider.
#' @param cfg
#'   load_scoped_config(); `raw_schema`/`processed_schema` steuern das Ziel-Schema.
#' @param suppression_pepper
#'   DSGVO-Pepper; wenn gesetzt, werden gesperrte PII (config.privacy_deletion_log) vor dem Upsert getombstoned.
#' @param dry_run
#'   Wenn TRUE: nur zaehlen/loggen, kein Upsert.
#'
#' @return
#'   invisible(Anzahl geschriebener Events).
#'
#' @export
msgraph_scoped_update_events <- function(con, del_token, cfg, suppression_pepper = NULL, dry_run = FALSE) {
  # ---- start ---- #
  rs <- cfg$raw_schema %||% "raw"
  ps <- cfg$processed_schema %||% "processed"
  start_dt <- format(Sys.Date() - cfg$events_days_back, "%Y-%m-%dT00:00:00Z")
  end_dt   <- format(Sys.Date() + cfg$events_days_forward, "%Y-%m-%dT23:59:59Z")
  # Freigegebene Kalender = solche, deren Owner NICHT der Service-Account ist.
  # (isSharedWithMe ist bei Graph unzuverlaessig -> Owner-Vergleich ist robust.)
  cals <- graph_collect("https://graph.microsoft.com/v1.0/me/calendars", del_token)
  if (cals$status != 200) stop("Kalenderliste HTTP ", cals$status)
  sa <- tolower(cfg$service_account_upn)
  shared <- Filter(function(x) {
    oa <- tolower(x$owner$address %||% "")
    nzchar(oa) && oa != sa
  }, cals$value)

  all_events <- list()
  for (cal in shared) {
    cid <- cal$id
    res <- graph_collect(
      paste0("https://graph.microsoft.com/v1.0/me/calendars/",
             utils::URLencode(cid, reserved = TRUE), "/calendarView"),
      del_token,
      query = list(startDateTime = start_dt, endDateTime = end_dt, `$top` = 100,
                   `$select` = paste0("iCalUId,type,createdDateTime,lastModifiedDateTime,subject,",
                                      "start,end,isCancelled,isOnlineMeeting,onlineMeeting,organizer,attendees,showAs")))
    if (res$status == 200) {
      # Owner-E-Mail pro Event mitfuehren, bevor sie beim Zusammenfuehren
      # verschiedener freigegebener Kalender verloren geht - wird von
      # build_show_as_lookup_scoped() gebraucht, um show_as der jeweiligen
      # Kalender-Kopie zuzuordnen.
      # normalize_external_email() auch hier, symmetrisch zu den
      # Teilnehmer-E-Mails in parse_scoped_events() (die dieselbe
      # Normalisierung anwenden) - beide Seiten des Join-Keys muessen
      # identisch normalisiert sein (Review-Fund).
      cal_owner_email <- tolower(normalize_external_email(cal$owner$address %||% ""))
      tagged <- lapply(res$value, function(ev) {
        ev$X_cal_owner_email <- cal_owner_email
        ev
      })
      all_events <- c(all_events, tagged)
    }
  }

  parsed <- parse_scoped_events(all_events)
  if (nrow(parsed$events) == 0) { message("Keine Events."); return(invisible(0L)) }

  # show_as pro Teilnehmer-Kopie annotieren, siehe build_show_as_lookup_scoped().
  # Der Lookup ist auf die Owner-E-Mail des jeweiligen Kalenders keyed - der
  # Owner muss dafuer selbst als Teilnehmer (Organizer oder Attendee) im
  # Event stehen, sonst matcht der left_join still nichts (Review-Fund,
  # Issue 2). Anti-Join macht diesen Miss sichtbar statt ihn unbeobachtet zu
  # lassen - genau der Fall, den D3 (base-41_personio) fuer Bot-organisierte
  # Personio-System-Events braucht, um erkennbar zu bleiben.
  show_as_lookup <- build_show_as_lookup_scoped(all_events)
  unmatched_show_as <- dplyr::anti_join(
    show_as_lookup, parsed$participants,
    by = c("msgraph_ical_uid", "event_start", "email")
  )
  if (nrow(unmatched_show_as) > 0) {
    message(sprintf(
      "show_as: %d/%d Kalender-Kopien ohne passende Teilnehmer-Zeile (Owner nicht als Teilnehmer im Event - kein show_as zuweisbar).",
      nrow(unmatched_show_as), nrow(show_as_lookup)
    ))
  }

  parsed$participants <- parsed$participants %>%
    dplyr::left_join(
      show_as_lookup,
      by = c("msgraph_ical_uid", "event_start", "email")
    )

  # DSGVO: PII gesperrter Personen in den Teilnehmern tombstonen (vor Kontakt-/Teilnehmer-Upsert)
  parsed$participants <- dsgvo_suppress_participants(parsed$participants, con, suppression_pepper)

  if (dry_run) {
    message(sprintf("[dry-run] %d Events, %d Teilnehmer (kein Upsert).",
                    nrow(parsed$events), nrow(parsed$participants)))
    return(invisible(nrow(parsed$events)))
  }

  # 2) Events upserten
  Billomatics::postgres_upsert_data(con, rs, "msgraph_events", parsed$events,
                                    match_cols = c("msgraph_ical_uid", "event_start"))

  if (nrow(parsed$participants) == 0) return(invisible(nrow(parsed$events)))

  # 1) Kontakte (email) upserten -> danach event_id/contact_id-Lookup
  contacts <- parsed$participants %>%
    dplyr::transmute(email, ms_name) %>% dplyr::distinct(email, .keep_all = TRUE)
  Billomatics::postgres_upsert_data(con, rs, "msgraph_contacts", contacts, match_cols = "email")

  # 3) Teilnehmer via Lookup auf DB-ids verknuepfen (nur source='calendar')
  ev_ids <- dplyr::tbl(con, I(paste0(rs, ".msgraph_events"))) %>%
    dplyr::select(id, msgraph_ical_uid, event_start) %>% dplyr::collect()
  ct_ids <- dplyr::tbl(con, I(paste0(rs, ".msgraph_contacts"))) %>%
    dplyr::select(id, email) %>% dplyr::collect() %>% dplyr::rename(contact_id = id)
  part <- parsed$participants %>%
    dplyr::left_join(ev_ids, by = c("msgraph_ical_uid", "event_start")) %>%
    dplyr::rename(event_id = id) %>%
    dplyr::left_join(ct_ids, by = "email") %>%
    dplyr::filter(!is.na(event_id), !is.na(contact_id)) %>%
    dplyr::transmute(event_id, contact_id, is_organizer, source, show_as)

  # W6-Fix (Review-Fund): siehe coalesce_show_as() (msgraph_events.R) -
  # schuetzt einen zuvor korrekt gesetzten show_as-Wert davor, durch einen
  # neuen NA ueberschrieben zu werden (Owner-Lookup fand diesmal keinen
  # Treffer, siehe Anti-Join-Logging oben).
  existing_show_as <- dplyr::tbl(con, I(paste0(rs, ".msgraph_event_participants"))) %>%
    dplyr::filter(source == "calendar") %>%
    dplyr::select(event_id, contact_id, show_as) %>%
    dplyr::collect()
  part <- part %>%
    coalesce_show_as(existing_show_as, by = c("event_id", "contact_id"))

  Billomatics::postgres_upsert_data(con, rs, "msgraph_event_participants", part,
                                    match_cols = c("event_id", "contact_id"))
  invisible(nrow(parsed$events))
}
