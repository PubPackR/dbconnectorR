#' Rangfolge der CRM-Status ueber mehrere Tasks eines Termins
#'
#' Ein geschriebenes "stattgefunden" schlaegt "No-Show", "No-Show" schlaegt
#' "storniert", jeder erkannte Status schlaegt keinen (Spec
#' 2026-11-02-termin-mapping-und-signaltabelle, Q4 und Q13). Bewusst nicht die
#' Rangfolge aus [update_sales_meetings_unified()], in der "storniert" gewinnt.
#'
#' @keywords internal
CRM_STATUS_RANG <- c(show_up = 3L, no_show = 2L, storniert = 1L, ohne_ausgang = 0L)

#' CRM-Status je Task aus den Kommentaren
#'
#' Rein (kein DB-Zugriff). Es zaehlt der spaeteste Kommentar mit erkanntem
#' Status ([classify_meeting_status()] ungleich `unbekannt`). "Spaeter" heisst
#' `comment_created_at`, ersatzweise `created_at`, bei Gleichstand die hoehere
#' `crm_comment_id`. Ein Kommentar ohne erkannten Status ist kein Signal (Q14,
#' Q16, Q17).
#'
#' @param task_ids Surrogat-IDs der Tasks (`raw.crm_lead_tasks.id`).
#' @param comments data.frame mit task_id, crm_comment_id, comment_name,
#'   comment_created_at, created_at (aus [load_crm_task_comments()]).
#' @return data.frame je Task: task_id (character), crm_status
#'   (`show_up`/`no_show`/`storniert`/`ohne_ausgang`), crm_status_conflict
#'   (erkannte Status der Kommentare nicht alle gleich) und crm_comment_count.
#' @keywords internal
# ---- start ---- #
crm_task_outcomes <- function(task_ids, comments) {
  ids <- as.character(task_ids)
  out <- data.frame(task_id = ids, crm_status = rep("ohne_ausgang", length(ids)),
                    crm_status_conflict = rep(FALSE, length(ids)),
                    crm_comment_count = rep(0L, length(ids)), stringsAsFactors = FALSE)
  if (nrow(comments) == 0 || length(ids) == 0) return(out)

  k <- data.frame(task_id = as.character(comments$task_id),
                  status  = classify_meeting_status(comments$comment_name),
                  zeit    = as.numeric(dplyr::coalesce(
                              as.POSIXct(comments$comment_created_at, tz = "UTC"),
                              as.POSIXct(comments$created_at, tz = "UTC"))),
                  cid     = as.numeric(as.character(comments$crm_comment_id)),
                  stringsAsFactors = FALSE)
  k <- k[k$task_id %in% ids, , drop = FALSE]

  anzahl <- table(k$task_id)
  out$crm_comment_count <- as.integer(ifelse(ids %in% names(anzahl), anzahl[ids], 0L))

  erkannt <- k[k$status != "unbekannt", , drop = FALSE]
  if (nrow(erkannt) == 0) return(out)
  erkannt <- erkannt[order(erkannt$task_id, -erkannt$zeit, -erkannt$cid), , drop = FALSE]
  spaetester <- erkannt[!duplicated(erkannt$task_id), , drop = FALSE]
  n_status <- tapply(erkannt$status, erkannt$task_id, function(s) length(unique(s)))

  m <- match(ids, spaetester$task_id)
  out$crm_status[!is.na(m)] <- spaetester$status[m[!is.na(m)]]
  out$crm_status_conflict <- ids %in% names(n_status)[n_status > 1]
  out
}

#' Zeilen des Task-Mappings zusammenbauen
#'
#' Rein (kein DB-Zugriff). Eine Zeile je CRM-VC-Task, mit dem MSGraph-Event der
#' per [match_crm_tasks()] zugeordneten Meeting-Zeile, sonst `NA`. Dieselbe
#' Zuordnung wie in [assemble_unified_meetings()]: beide sehen dieselben
#' Meeting-Zeilen und dieselbe Funktion.
#'
#' @param tasks data.frame aus [load_crm_vc_tasks()] (id, crm_task_id, lead_id,
#'   event_date, precise_time, contact_id, is_external_tool).
#' @param msgraph_meetings data.frame aus [load_unified_msgraph_meetings()]
#'   (event_id, lead_id, lead_source, event_date, event_start, contact_id).
#' @return data.frame im Schema von `mapping.crm_lead_task_msgraph_event`:
#'   crm_lead_task_id, msgraph_event_id, match_rule, candidate_count,
#'   start_diff_minutes, lead_source; IDs als character.
#' @keywords internal
# ---- start ---- #
assemble_task_mapping <- function(tasks, msgraph_meetings) {
  abgleich <- match_crm_tasks(tasks, msgraph_meetings$lead_id, msgraph_meetings$event_date,
                              msgraph_meetings$contact_id, msgraph_meetings$event_start)
  j <- abgleich$row
  lead_source <- as.character(msgraph_meetings$lead_source)[j]
  lead_source[abgleich$match_rule == "ersatzregel"] <- "ersatzregel"
  data.frame(
    crm_lead_task_id   = as.character(tasks$id),
    msgraph_event_id   = as.character(msgraph_meetings$event_id)[j],
    match_rule         = abgleich$match_rule,
    candidate_count    = abgleich$candidate_count,
    start_diff_minutes = abgleich$start_diff_minutes,
    lead_source        = lead_source,
    stringsAsFactors   = FALSE)
}

#' Teams-Anwesenheit je Event als call_status
#'
#' Rein (kein DB-Zugriff). Erste greifende Bedingung gewinnt: `in_zukunft`,
#' `extern_call`/`intern_call` (ein Call ist eine Messung), `alt_tenant`,
#' `nicht_abrufbar`, `ohne_link`, `no_call`. Zukunft, Alt-Tenant und nicht
#' abrufbar kommen aus [compute_observability_exclusions()], damit die Regeln
#' nur einmal im Code stehen.
#'
#' @param events data.frame je Event mit id, event_start, join_url,
#'   join_url_checked_at, online_meeting_lookup und event_class (aus
#'   `mapping.msgraph_call_event`).
#' @param tenant_id GUID des eigenen Tenants, siehe
#'   [update_extern_event_classification()].
#' @param now_utc POSIXct, Bezugspunkt fuer die Zukunft.
#' @return character je Zeile in `events`.
#' @keywords internal
# ---- start ---- #
derive_call_status <- function(events, tenant_id = NULL, now_utc = Sys.time()) {
  if (nrow(events) == 0) return(character(0))
  klasse <- as.character(events$event_class)
  mit_call <- events$id[!grepl("no_call", klasse, ignore.case = TRUE)]
  obs <- compute_observability_exclusions(events, tenant_id = tenant_id, now_utc = now_utc,
                                          event_ids_mit_call = mit_call)
  grund <- obs$reason[match(as.character(events$id), as.character(obs$event_id))]
  url <- as.character(events$join_url)
  ohne_link <- !is.na(events$join_url_checked_at) & (is.na(url) | !nzchar(trimws(url)))

  dplyr::case_when(
    grund %in% "termin_in_zukunft"                          ~ "in_zukunft",
    grepl("extern_call$", klasse, ignore.case = TRUE)       ~ "extern_call",
    grepl("intern_call$", klasse, ignore.case = TRUE)       ~ "intern_call",
    grund %in% "alt_tenant_join_url"                        ~ "alt_tenant",
    grund %in% "online_meeting_nicht_abrufbar"              ~ "nicht_abrufbar",
    ohne_link                                               ~ "ohne_link",
    TRUE                                                    ~ "no_call"
  )
}

#' Kalenderzustand je Event als outlook_status
#'
#' Rein (kein DB-Zugriff). `verschoben`: abgesagt, waehrend ein aktives Event
#' derselben `msgraph_ical_uid` existiert. `abgesagt`: sonst abgesagt, auch ein
#' aus dem Kalender verschwundenes Event, das der Ingest als abgesagt speichert
#' (Q8). `aktiv`: sonst.
#'
#' @param is_canceled logical je Event.
#' @param ical_uid character je Event.
#' @param aktive_uids character, alle `msgraph_ical_uid` mit einem nicht
#'   abgesagten Event.
#' @return character je Event.
#' @keywords internal
# ---- start ---- #
derive_outlook_status <- function(is_canceled, ical_uid, aktive_uids) {
  ifelse(!(is_canceled %in% TRUE), "aktiv",
         ifelse(ical_uid %in% aktive_uids, "verschoben", "abgesagt"))
}

#' Signaltabelle zusammenbauen
#'
#' Rein (kein DB-Zugriff). Eine Zeile je MSGraph-Meeting-Zeile (Meeting x Lead),
#' dazu je Task ohne Event eine CRM-Zeile. Ein Task gehoert zur Zeile mit seinem
#' Event und seinem Lead; per Ersatzregel uebernimmt die Zeile ohne Lead den
#' Lead des Tasks. Ueber mehrere Tasks gilt [CRM_STATUS_RANG], bei Gleichstand
#' die kleinere `crm_task_id`.
#'
#' @param msgraph_meetings data.frame aus [load_unified_msgraph_meetings()].
#' @param event_signals data.frame je Event (`event_id` character) mit
#'   outlook_status, call_status, is_booking, online_meeting_lookup.
#' @param tasks data.frame aus [load_crm_vc_tasks()].
#' @param mapping data.frame aus [assemble_task_mapping()].
#' @param outcomes data.frame aus [crm_task_outcomes()].
#' @return data.frame im Schema von `processed.sales_meeting_signals`, IDs als
#'   character.
#' @keywords internal
# ---- start ---- #
assemble_meeting_signals <- function(msgraph_meetings, event_signals, tasks, mapping, outcomes) {
  # Task-Ebene: Mapping + Task + CRM-Status, alles im character-Raum.
  t <- data.frame(
    crm_lead_task_id = as.character(tasks$id),
    crm_task_id      = as.character(tasks$crm_task_id),
    task_lead        = as.character(tasks$lead_id),
    contact_id       = as.character(tasks$contact_id),
    event_date       = tasks$event_date,
    precise_time     = tasks$precise_time,
    task_created_at  = tasks$task_created_at,
    meeting_tool     = tasks$meeting_tool,
    meeting_type     = tasks$meeting_type,
    is_external_tool = tasks$is_external_tool,
    stringsAsFactors = FALSE)
  mp <- mapping[match(t$crm_lead_task_id, as.character(mapping$crm_lead_task_id)), , drop = FALSE]
  t$msgraph_event_id <- as.character(mp$msgraph_event_id)
  t$match_rule       <- mp$match_rule
  oc <- outcomes[match(t$crm_lead_task_id, as.character(outcomes$task_id)), , drop = FALSE]
  t$crm_status          <- oc$crm_status
  t$crm_status_conflict <- oc$crm_status_conflict
  t$crm_comment_count   <- oc$crm_comment_count
  t$rang <- CRM_STATUS_RANG[t$crm_status]

  # MSGraph-Zeilen; per Ersatzregel uebernimmt die Zeile ohne Lead den Task-Lead.
  m <- msgraph_meetings
  m$event_id    <- as.character(m$event_id)
  m$lead_id     <- as.character(m$lead_id)
  m$lead_source <- as.character(m$lead_source)
  ers <- t[t$match_rule %in% "ersatzregel", , drop = FALSE]
  for (k in seq_len(nrow(ers))) {
    z <- which(m$event_id == ers$msgraph_event_id[k] & is.na(m$lead_id))
    m$lead_id[z]     <- ers$task_lead[k]
    m$lead_source[z] <- "ersatzregel"
  }

  # Tasks je Zeile: gleiches Event, gleicher Lead. Ein Task, dessen Zeile es in
  # diesem Lauf nicht gibt (Mapping und Meeting-Zeilen werden getrennt geladen),
  # geht nicht verloren, sondern wird unten eine CRM-Zeile.
  getroffen <- t[!is.na(t$msgraph_event_id), , drop = FALSE]
  zeile_key <- paste(m$event_id, m$lead_id)
  ohne_zeile <- !(paste(getroffen$msgraph_event_id, getroffen$task_lead) %in% zeile_key)
  verwaist  <- getroffen[ohne_zeile, , drop = FALSE]
  getroffen <- getroffen[!ohne_zeile, , drop = FALSE]
  task_key  <- paste(getroffen$msgraph_event_id, getroffen$task_lead)
  getroffen <- getroffen[order(task_key, -getroffen$rang,
                               as.numeric(getroffen$crm_task_id)), , drop = FALSE]
  task_key  <- paste(getroffen$msgraph_event_id, getroffen$task_lead)
  sieger    <- getroffen[!duplicated(task_key), , drop = FALSE]
  sieger_key <- task_key[!duplicated(task_key)]
  n_tasks   <- table(task_key)
  konflikt  <- tapply(seq_len(nrow(getroffen)), task_key, function(ix)
    length(unique(getroffen$crm_status[ix][getroffen$crm_status[ix] != "ohne_ausgang"])) > 1 ||
      any(getroffen$crm_status_conflict[ix]))

  s <- sieger[match(zeile_key, sieger_key), , drop = FALSE]
  hat_task <- !is.na(s$crm_lead_task_id)
  es <- event_signals[match(m$event_id, as.character(event_signals$event_id)), , drop = FALSE]

  ms_rows <- data.frame(
    meeting_key              = paste0("msgraph_", m$call_event_mapping_id, "_", m$lead_id, recycle0 = TRUE),
    msgraph_event_id         = m$event_id,
    call_event_mapping_id    = as.character(m$call_event_mapping_id),
    crm_lead_task_id         = s$crm_lead_task_id,
    lead_id                  = m$lead_id,
    lead_source              = m$lead_source,
    contact_id               = as.character(m$contact_id),
    organizer_contact_id     = as.character(m$organizer_contact_id),
    match_rule               = s$match_rule,
    event_date               = m$event_date,
    event_start              = m$event_start,
    original_created_at      = m$original_created_at,
    has_crm_task             = hat_task,
    has_msgraph_event        = TRUE,
    crm_status               = ifelse(hat_task, s$crm_status, "kein_task"),
    crm_status_conflict      = hat_task & as.logical(konflikt[zeile_key]) %in% TRUE,
    crm_task_count           = as.integer(ifelse(zeile_key %in% names(n_tasks),
                                                 n_tasks[zeile_key], 0L)),
    crm_comment_count        = s$crm_comment_count,
    meeting_tool             = s$meeting_tool,
    meeting_type             = s$meeting_type,
    is_external_tool         = s$is_external_tool,
    outlook_status           = es$outlook_status,
    msgraph_exclusion_reason = as.character(m$exclusion_reason),
    is_short_lived_event     = m$is_short_lived_event,
    is_booking               = es$is_booking,
    call_status              = es$call_status,
    online_meeting_lookup    = es$online_meeting_lookup,
    stringsAsFactors         = FALSE)

  # CRM-Zeilen: jeder Task ohne Event, auch mehrdeutige (Q3), und Tasks, deren
  # Meeting-Zeile in diesem Lauf fehlt.
  if (nrow(verwaist) > 0) {
    warning(nrow(verwaist), " Tasks zeigen auf eine Meeting-Zeile, die es nicht gibt; ",
            "sie werden als CRM-Zeile geschrieben.", call. = FALSE)
  }
  c0 <- rbind(t[is.na(t$msgraph_event_id), , drop = FALSE], verwaist)
  crm_rows <- data.frame(
    meeting_key              = paste0("crm_", c0$crm_task_id, recycle0 = TRUE),
    msgraph_event_id         = rep(NA_character_, nrow(c0)),
    call_event_mapping_id    = rep(NA_character_, nrow(c0)),
    crm_lead_task_id         = c0$crm_lead_task_id,
    lead_id                  = c0$task_lead,
    lead_source              = ifelse(is.na(c0$task_lead), NA_character_, "crm_task"),
    contact_id               = c0$contact_id,
    organizer_contact_id     = rep(NA_character_, nrow(c0)),
    match_rule               = c0$match_rule,
    event_date               = c0$event_date,
    event_start              = c0$precise_time,
    original_created_at      = c0$task_created_at,
    has_crm_task             = rep(TRUE, nrow(c0)),
    has_msgraph_event        = rep(FALSE, nrow(c0)),
    crm_status               = c0$crm_status,
    crm_status_conflict      = c0$crm_status_conflict,
    crm_task_count           = rep(1L, nrow(c0)),
    crm_comment_count        = c0$crm_comment_count,
    meeting_tool             = c0$meeting_tool,
    meeting_type             = c0$meeting_type,
    is_external_tool         = c0$is_external_tool,
    outlook_status           = rep("kein_event", nrow(c0)),
    msgraph_exclusion_reason = rep(NA_character_, nrow(c0)),
    is_short_lived_event     = rep(NA, nrow(c0)),
    is_booking               = rep(NA, nrow(c0)),
    call_status              = rep("kein_event", nrow(c0)),
    online_meeting_lookup    = rep(NA_character_, nrow(c0)),
    stringsAsFactors         = FALSE)

  out <- rbind(ms_rows, crm_rows)
  rownames(out) <- NULL
  out
}

#' Rebuild mapping.crm_lead_task_msgraph_event (voll rueckwirkend)
#'
#' Eine Zeile je CRM-VC-Task mit dem MSGraph-Event, dem er zugeordnet ist, und
#' den Belegen dafuer. Dieselben Eingaben und dieselbe Zuordnung wie
#' [update_sales_meetings_unified()]. Laeuft nach update_sales_meetings_unified,
#' vor [update_sales_meeting_signals()], das das hier geschriebene Mapping liest.
#'
#' @param con Pool/DBI-Connection.
#' @return invisible(Anzahl geschriebener Zeilen).
#' @export
update_crm_lead_task_msgraph_event <- function(con) {
  message("update_crm_lead_task_msgraph_event ...")
  msgraph_meetings <- load_unified_msgraph_meetings(con)
  tasks <- load_crm_vc_tasks(con)
  rows  <- assemble_task_mapping(tasks, msgraph_meetings)

  # delete_missing = TRUE leert die Tabelle bei einem leeren Ergebnis. Lieber
  # laut scheitern (Hausregel fuer do/main*.R).
  if (nrow(rows) == 0) stop("update_crm_lead_task_msgraph_event: keine CRM-VC-Tasks. Abbruch.")
  # Grain: eine Zeile je Task (raw.crm_lead_tasks.id).
  if (anyDuplicated(rows$crm_lead_task_id)) {
    stop("update_crm_lead_task_msgraph_event: crm_lead_task_id doppelt. Bug in der Pipeline.")
  }
  message(paste0("  ", nrow(rows), " Tasks, davon ", sum(!is.na(rows$msgraph_event_id)),
                 " mit Event -> mapping.crm_lead_task_msgraph_event"))

  rows$crm_lead_task_id <- bit64::as.integer64(rows$crm_lead_task_id)
  rows$msgraph_event_id <- bit64::as.integer64(rows$msgraph_event_id)
  pool::poolWithTransaction(con, function(conn) {
    Billomatics::postgres_upsert_data(
      conn, "mapping", "crm_lead_task_msgraph_event",
      rows, match_cols = c("crm_lead_task_id"), delete_missing = TRUE)
  })
  message("  fertig.")
  invisible(nrow(rows))
}

#' Rebuild processed.sales_meeting_signals (voll rueckwirkend)
#'
#' Was CRM, Outlook und Teams je Termin sagen, ohne Bewertung (ADR 0003). Liest
#' das Task-Mapping aus `mapping.crm_lead_task_msgraph_event`, laeuft also nach
#' [update_crm_lead_task_msgraph_event()]. Outlook-Gruende und Kurzlebigkeit
#' kommen unveraendert aus `processed.msgraph_extern_event_classification`.
#'
#' @param con Pool/DBI-Connection.
#' @param tenant_id GUID des eigenen Tenants (`cfg$tenant_id` der base-app).
#'   NULL laesst `alt_tenant` aus, wie in [update_extern_event_classification()].
#' @param now_utc POSIXct, Bezugspunkt fuer `in_zukunft`. Nur fuer Tests.
#' @return invisible(Anzahl geschriebener Zeilen).
#' @export
update_sales_meeting_signals <- function(con, tenant_id = NULL, now_utc = Sys.time()) {
  message("update_sales_meeting_signals ...")
  msgraph_meetings <- load_unified_msgraph_meetings(con)
  tasks    <- load_crm_vc_tasks(con)
  comments <- load_crm_task_comments(con)
  mapping  <- dplyr::tbl(con, I("mapping.crm_lead_task_msgraph_event")) %>%
    dplyr::select(crm_lead_task_id, msgraph_event_id, match_rule) %>%
    dplyr::collect()

  message("  lade Event-Signale ...")
  lookup_spalte <- online_meeting_lookup_select_cols(con)
  events <- dplyr::tbl(con, I("raw.msgraph_events")) %>%
    dplyr::select(id, msgraph_ical_uid, is_canceled, event_start, join_url,
                  join_url_checked_at, dplyr::all_of(lookup_spalte)) %>%
    dplyr::collect()
  if (length(lookup_spalte) == 0) events$online_meeting_lookup <- rep(NA_character_, nrow(events))
  aktive_uids <- unique(events$msgraph_ical_uid[!(events$is_canceled %in% TRUE)])
  booking_ids <- dplyr::tbl(con, I("raw.msgraph_event_participants")) %>%
    dplyr::filter(source == "booking") %>%
    dplyr::distinct(event_id) %>%
    dplyr::collect()
  klassen <- dplyr::tbl(con, I("mapping.msgraph_call_event")) %>%
    dplyr::select(id, event_class) %>%
    dplyr::collect()

  # Nur die Events der Meeting-Zeilen, je Event die Klasse seiner Mapping-Zeile.
  ev <- events[as.character(events$id) %in% as.character(msgraph_meetings$event_id), , drop = FALSE]
  zu_mapping <- msgraph_meetings$call_event_mapping_id[
    match(as.character(ev$id), as.character(msgraph_meetings$event_id))]
  ev$event_class <- klassen$event_class[match(as.character(zu_mapping), as.character(klassen$id))]
  event_signals <- data.frame(
    event_id              = as.character(ev$id),
    outlook_status        = derive_outlook_status(ev$is_canceled, ev$msgraph_ical_uid, aktive_uids),
    call_status           = derive_call_status(ev, tenant_id = tenant_id, now_utc = now_utc),
    is_booking            = as.character(ev$id) %in% as.character(booking_ids$event_id),
    online_meeting_lookup = as.character(ev$online_meeting_lookup),
    stringsAsFactors      = FALSE)

  outcomes <- crm_task_outcomes(tasks$id, comments)
  rows <- assemble_meeting_signals(msgraph_meetings, event_signals, tasks, mapping, outcomes)

  if (nrow(rows) == 0) stop("update_sales_meeting_signals: keine Termine. Abbruch.")
  # Grain: eine Zeile je (Meeting x Lead) bzw. je CRM-Task ohne Meeting. Eine
  # Dublette heisst Fan-out in der Lead-Expansion oder dieselbe crm_task_id an
  # zwei Leads; beides soll laut scheitern statt im Upsert zu verschwinden.
  dubletten <- unique(rows$meeting_key[duplicated(rows$meeting_key)])
  if (length(dubletten) > 0) {
    stop("update_sales_meeting_signals: ", length(dubletten), " meeting_keys doppelt, z.B. ",
         paste(utils::head(dubletten, 5), collapse = ", "), ". Abbruch.")
  }
  message(paste0("  ", nrow(rows), " Zeilen, davon ", sum(rows$crm_status_conflict),
                 " mit widerspruechlichem CRM-Status -> processed.sales_meeting_signals"))

  for (spalte in c("msgraph_event_id", "call_event_mapping_id", "crm_lead_task_id", "lead_id",
                   "contact_id", "organizer_contact_id")) {
    rows[[spalte]] <- bit64::as.integer64(rows[[spalte]])
  }
  pool::poolWithTransaction(con, function(conn) {
    Billomatics::postgres_upsert_data(
      conn, "processed", "sales_meeting_signals",
      rows, match_cols = c("meeting_key"), delete_missing = TRUE)
  })
  message("  fertig.")
  invisible(nrow(rows))
}
