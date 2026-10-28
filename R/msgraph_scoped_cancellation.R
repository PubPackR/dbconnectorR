#' Welche verschwundenen Events werden abgesagt? (rein, ohne Netz)
#'
#' Der delegierte Abruf laedt jeden freigegebenen Kalender (bzw. jedes
#' Booking-Business) einzeln. Ein Event, das im Bestand steht, an dem die
#' Besitzerin eines geladenen Kalenders teilnimmt und das im Download fehlt, ist
#' verschoben oder geloescht. Der alte app-only-Abruf (`update_events()`) hat
#' solche Events auf `is_canceled = TRUE` gesetzt, der delegierte hat das bis
#' Oktober 2026 nicht getan - die alte Haelfte eines verschobenen Termins blieb
#' aktiv und zaehlte als No-Show.
#'
#' Der Abdeckungsschutz (`cancellation_coverage_check()`) rechnet hier je Scope
#' statt aggregiert: Faellt ein einzelner Kalender still aus (HTTP 200 mit
#' unvollstaendiger Antwort), blockt das nur diesen Kalender, statt dessen
#' Events faelschlich abzusagen. Ein Event in mehreren Scopes wird abgesagt,
#' sobald einer davon den Schutz besteht - es fehlt dann im Download insgesamt,
#' also auch im vollstaendig geladenen Kalender.
#'
#' @param rows
#'   Ein Eintrag je (Event, Scope): `id`, `scope`, `fehlt` (TRUE, wenn das Event
#'   im Download dieses Laufs fehlt). Siehe `scoped_cancellation_rows_sql()`.
#' @param min_coverage
#'   Mindestanteil der Scope-Events, die der Download abdecken muss.
#'
#' @return
#'   list(ids = abzusagende Event-IDs, coverage = tibble je Scope mit
#'   `in_scope_n`, `to_cancel_n`, `coverage`, `skip`).
#'
#' @keywords internal
scoped_cancellation_plan <- function(rows, min_coverage = 0.5) {
  # ---- start ---- #
  if (nrow(rows) == 0) {
    return(list(ids = character(0),
                coverage = tibble::tibble(scope = character(0), in_scope_n = integer(0),
                                          to_cancel_n = integer(0), coverage = numeric(0),
                                          skip = logical(0))))
  }
  rows <- dplyr::distinct(rows, id, scope, fehlt)
  coverage <- rows %>%
    dplyr::group_by(scope) %>%
    dplyr::summarise(in_scope_n = dplyr::n(), to_cancel_n = sum(fehlt), .groups = "drop") %>%
    dplyr::rowwise() %>%
    dplyr::mutate(
      coverage = cancellation_coverage_check(in_scope_n, to_cancel_n, min_coverage)$coverage,
      skip     = cancellation_coverage_check(in_scope_n, to_cancel_n, min_coverage)$skip) %>%
    dplyr::ungroup()
  ids <- rows %>%
    dplyr::filter(fehlt) %>%
    dplyr::semi_join(dplyr::filter(coverage, !skip), by = "scope") %>%
    dplyr::distinct(id) %>%
    dplyr::pull(id)
  list(ids = as.character(ids), coverage = coverage)
}

#' SELECT der Bestands-Events im Scope eines Laufs
#'
#' Eigene Funktion, damit die tragenden Bedingungen ohne Datenbank pruefbar
#' sind: nur nicht abgesagte Zeilen, nur die Quelle des Laufs (Kalender-Zeilen
#' duerfen nie gegen den Bookings-Download geprueft werden und umgekehrt), nur
#' Teilnehmer aus geladenen Scopes.
#'
#' @param rs Ziel-Schema.
#' @param tmp_seen Temp-Tabelle mit den Schluesseln des Downloads (`msgraph_ical_uid`, `event_start` als Text in UTC).
#' @param tmp_scope Temp-Tabelle `email`, `scope` der geladenen Kalender bzw. Businesses.
#' @param source "calendar" oder "booking".
#'
#' @return SQL-Statement als character, Parameter $1 = Fensterbeginn, $2 = Fensterende (Text, UTC).
#'
#' @keywords internal
scoped_cancellation_rows_sql <- function(rs, tmp_seen, tmp_scope, source = c("calendar", "booking")) {
  # ---- start ---- #
  source <- match.arg(source)
  quelle <- if (source == "booking") "e.msgraph_ical_uid LIKE 'booking:%'"
            else "e.msgraph_ical_uid NOT LIKE 'booking:%'"
  sprintf("
    SELECT DISTINCT e.id::text AS id, s.scope,
           NOT EXISTS (SELECT 1 FROM %s t
                        WHERE t.msgraph_ical_uid = e.msgraph_ical_uid
                          AND t.event_start::timestamp = e.event_start) AS fehlt
      FROM %s.msgraph_events e
      JOIN %s.msgraph_event_participants p ON p.event_id = e.id
      JOIN %s.msgraph_contacts c ON c.id = p.contact_id
      JOIN %s s ON s.email = lower(c.email)
     WHERE e.event_start >= $1::timestamp
       AND e.event_start <= $2::timestamp
       AND NOT coalesce(e.is_canceled, FALSE)
       AND %s", tmp_seen, rs, rs, rs, tmp_scope, quelle)
}

#' Verschwundene Events eines delegierten Laufs als abgesagt markieren
#'
#' Laeuft NACH dem Upsert des Laufs. Liest die Bestands-Events im Scope, plant
#' mit `scoped_cancellation_plan()` und setzt `is_canceled = TRUE` fuer die
#' geplanten IDs. Temp-Tabellen und UPDATE laufen in EINER Transaktion auf einer
#' Verbindung - eine Temp-Tabelle ueberlebt keinen Pool-Checkout.
#'
#' Bewusst log-and-degrade statt stop(), wenn ein Scope den Abdeckungsschutz
#' nicht besteht: der Rest des Laufs soll weiterlaufen, der Vorfall steht im Log.
#'
#' @param con Pool oder DBI-Verbindung.
#' @param rs Ziel-Schema.
#' @param seen Schluessel des Downloads: `msgraph_ical_uid`, `event_start` (POSIXct, UTC).
#' @param scope_map `email` (lowercase, normalisiert), `scope` der in diesem Lauf geladenen Kalender bzw. Businesses.
#' @param window_start,window_end Abruffenster als POSIXct oder Date (UTC).
#' @param source "calendar" oder "booking".
#' @param min_coverage Mindestabdeckung je Scope, Default 0.5 wie im alten Abruf.
#'
#' @return invisible(Anzahl abgesagter Events).
#'
#' @keywords internal
mark_vanished_events_canceled <- function(con, rs, seen, scope_map, window_start, window_end,
                                          source = c("calendar", "booking"), min_coverage = 0.5) {
  # ---- start ---- #
  source <- match.arg(source)
  if (nrow(scope_map) == 0) return(invisible(0L))
  # Text in UTC statt POSIXct, siehe mark_join_url_checked()
  seen_keys <- data.frame(
    msgraph_ical_uid = as.character(seen$msgraph_ical_uid),
    event_start      = format(seen$event_start, "%Y-%m-%d %H:%M:%S", tz = "UTC"),
    stringsAsFactors = FALSE)
  scope_keys <- dplyr::distinct(data.frame(
    email = tolower(as.character(scope_map$email)),
    scope = as.character(scope_map$scope),
    stringsAsFactors = FALSE))
  fenster <- list(format(as.POSIXct(window_start, tz = "UTC"), "%Y-%m-%d %H:%M:%S", tz = "UTC"),
                  format(as.POSIXct(window_end, tz = "UTC"), "%Y-%m-%d %H:%M:%S", tz = "UTC"))
  sql <- scoped_cancellation_rows_sql(rs, "tmp_cancel_seen", "tmp_cancel_scope", source)

  absagen <- function(conn) {
    DBI::dbWriteTable(conn, "tmp_cancel_seen", seen_keys, temporary = TRUE, overwrite = TRUE)
    DBI::dbWriteTable(conn, "tmp_cancel_scope", scope_keys, temporary = TRUE, overwrite = TRUE)
    rows <- tibble::as_tibble(DBI::dbGetQuery(conn, sql, params = fenster))
    rows$fehlt <- as.logical(rows$fehlt)
    plan <- scoped_cancellation_plan(rows, min_coverage)
    for (i in which(plan$coverage$skip)) {
      cv <- plan$coverage[i, ]
      msg <- sprintf(paste0(
        "%s (%s): Download deckt nur %.1f%% der %d Bestands-Events ab, Schwelle %.0f%%. ",
        "Fuer diesen Scope wird in diesem Lauf NICHTS abgesagt (%d Events betroffen)."),
        source, cv$scope, 100 * cv$coverage, cv$in_scope_n, 100 * min_coverage, cv$to_cancel_n)
      warning(msg, call. = FALSE, immediate. = TRUE)
      message(msg)
    }
    if (length(plan$ids) == 0) return(0L)
    DBI::dbExecute(conn, sprintf(
      "UPDATE %s.msgraph_events SET is_canceled = TRUE WHERE id::text = ANY(string_to_array($1, ','))", rs),
      params = list(paste(plan$ids, collapse = ",")))
  }
  n <- if (inherits(con, "Pool")) {
    pool::poolWithTransaction(con, absagen)
  } else {
    DBI::dbBegin(con)
    res <- tryCatch(absagen(con), error = function(e) { DBI::dbRollback(con); stop(e) })
    DBI::dbCommit(con)
    res
  }
  message(sprintf("%s: %d verschwundene Events als abgesagt markiert.", source, n))
  invisible(as.integer(n))
}
