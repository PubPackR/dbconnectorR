# Ausgang der Online-Meeting-Suche je Link (msgraph_scoped_update_calls_attendance).
# Kein DB- und kein Graph-Zugriff: alles per mockery::stub bzw. local_mocked_bindings.
# Hinweis: mockery::stub() muss IM test_that-Block stehen (Scope), nicht in einem Helper.

lookup_cfg <- list(raw_schema = "raw", processed_schema = "processed")

bericht_mit_teilnehmer <- list(id = "REP1", meetingStartDateTime = "2026-08-10T10:00:00Z",
                               meetingEndDateTime = "2026-08-10T10:30:00Z", attendanceRecords = list(
  list(emailAddress = "rep.a@studyflix.de", identity = list(displayName = "Rep A"),
       role = "Organizer", totalAttendanceInSeconds = 1800)))
bericht_ohne_teilnehmer <- list(id = "REP2", meetingStartDateTime = "2026-08-11T10:00:00Z",
                                attendanceRecords = list())

# --- Ausgang je Fall im Job ----------------------------------------------------

test_that("calls_attendance: jeder Link bekommt seinen Ausgang, der beste ueber alle Versuche gilt", {
  disc <- data.frame(
    join_url = c("https://teams/a",   # zuerst ein Versuch ueber einen 403-Organisator ...
                 "https://teams/a",   # ... dann der erfolgreiche: bester Ausgang gewinnt
                 "https://teams/b", "https://teams/c", "https://teams/d",
                 "https://teams/e",   # gleiche oid wie die erste Zeile: uebersprungen
                 "https://teams/g", "https://teams/h", "https://teams/i",
                 "https://teams/j", "https://teams/k", "https://teams/l", "https://teams/m",
                 "https://teams/n", "https://teams/o"),
    organizer_oid = c("OID_403", "OID_OK", "OID_OK", "OID_OK", "OID_OK", "OID_403",
                      "OID_404", "OID_OK", "OID_OK",
                      NA, NA, NA, NA, NA, "OID_OK"),
    organizer_email = c("x@studyflix.de", "ok@studyflix.de", "ok@studyflix.de", "ok@studyflix.de",
                        "ok@studyflix.de", "x@studyflix.de", "weg@studyflix.de", "ok@studyflix.de",
                        "ok@studyflix.de",
                        "neu@bertelsmann.de", "unbekannt@bertelsmann.de", "unbekannt@bertelsmann.de",
                        "kaputt@bertelsmann.de", NA, "ok@studyflix.de"),
    stringsAsFactors = FALSE)
  resolve <- function(oid, ju, tok) {
    if (oid == "OID_403") return(list(status = 403, id = NA_character_))
    if (oid == "OID_404") return(list(status = 404, id = NA_character_))
    switch(sub("https://teams/", "", ju, fixed = TRUE),
           a = list(status = 200, id = "MID_A"), b = list(status = 200, id = "MID_B"),
           c = list(status = 200, id = "MID_C"), d = list(status = 200, id = NA_character_),
           h = list(status = 500, id = NA_character_), i = list(status = 200, id = "MID_I"),
           j = list(status = 200, id = "MID_J"), o = stop("Verbindung weg"))
  }
  attendance <- function(oid, mid, tok) {
    switch(mid,
           MID_A = list(status = 200, reports = list(bericht_mit_teilnehmer)),
           MID_B = list(status = 200, reports = list()),
           MID_C = list(status = 200, reports = list(bericht_ohne_teilnehmer)),
           MID_I = list(status = 502, reports = list()),
           MID_J = list(status = 200, reports = list(bericht_mit_teilnehmer)))
  }
  # Konto-Aufloesung per E-Mail, mit Zaehler je Adresse (Cache-Pruefung)
  abfragen <- new.env()
  konto <- function(email, tok) {
    assign(email, (abfragen[[email]] %||% 0L) + 1L, envir = abfragen)
    switch(email,
           "x@studyflix.de"           = list(status = 200, id = "OID_403"),   # gleiche oid: bleibt 403
           "weg@studyflix.de"         = list(status = 200, id = NA_character_),
           "neu@bertelsmann.de"       = list(status = 200, id = "OID_OK"),
           "unbekannt@bertelsmann.de" = list(status = 200, id = NA_character_),
           "kaputt@bertelsmann.de"    = list(status = 503, id = NA_character_))
  }
  write <- mockery::mock(1L)
  assert_cols <- mockery::mock(TRUE)
  mockery::stub(msgraph_scoped_update_calls_attendance, "discover_meetings_from_events",
                function(con, cfg) disc)
  mockery::stub(msgraph_scoped_update_calls_attendance, "resolve_meeting", resolve)
  mockery::stub(msgraph_scoped_update_calls_attendance, "attendance_records", attendance)
  mockery::stub(msgraph_scoped_update_calls_attendance, "resolve_organizer_by_email", konto)
  mockery::stub(msgraph_scoped_update_calls_attendance, "assert_online_meeting_lookup_columns", assert_cols)
  mockery::stub(msgraph_scoped_update_calls_attendance, "write_online_meeting_lookups", write)
  # Calls-Pfad ohne Datenbank
  mockery::stub(msgraph_scoped_update_calls_attendance, "assert_online_meeting_id_column", TRUE)
  mockery::stub(msgraph_scoped_update_calls_attendance, "rekey_meeting_calls", 0L)
  mockery::stub(msgraph_scoped_update_calls_attendance, "Billomatics::postgres_upsert_data", NULL)
  mockery::stub(msgraph_scoped_update_calls_attendance, "dplyr::tbl", function(con, tab) {
    if (grepl("msgraph_calls", tab)) data.frame(id = 1L, msgraph_call_id = "REP1")
    else data.frame(id = 1L, email = "rep.a@studyflix.de")
  })

  n <- suppressMessages(msgraph_scoped_update_calls_attendance(con = NULL, app_token = "t",
                                                               cfg = lookup_cfg, dry_run = FALSE))

  expect_equal(n, 1)   # REP1 an a und j ist dieselbe Session -> ein Call
  mockery::expect_called(write, 1)
  mockery::expect_called(assert_cols, 1)
  aus <- mockery::mock_args(write)[[1]][[3]]
  ist <- stats::setNames(aus$online_meeting_lookup, aus$join_url)
  expect_equal(ist[["https://teams/a"]], "gefunden_mit_bericht")
  expect_equal(ist[["https://teams/b"]], "gefunden_ohne_bericht")
  expect_equal(ist[["https://teams/c"]], "gefunden_ohne_bericht")   # Bericht ohne Teilnehmende
  expect_equal(ist[["https://teams/d"]], "nicht_gefunden")
  expect_equal(ist[["https://teams/e"]], "policy_403")              # nach 403 uebersprungen
  expect_equal(ist[["https://teams/g"]], "organisator_unbekannt")   # 404 auf das Konto
  expect_equal(ist[["https://teams/h"]], "abruf_fehler")
  expect_equal(ist[["https://teams/i"]], "abruf_fehler")
  expect_equal(ist[["https://teams/j"]], "gefunden_mit_bericht")    # NA-oid per E-Mail aufgeloest
  expect_equal(ist[["https://teams/k"]], "organisator_unbekannt")   # per E-Mail nicht aufloesbar
  expect_equal(ist[["https://teams/l"]], "organisator_unbekannt")
  expect_equal(ist[["https://teams/m"]], "abruf_fehler")
  expect_equal(ist[["https://teams/n"]], "organisator_unbekannt")   # ohne E-Mail
  expect_equal(ist[["https://teams/o"]], "abruf_fehler")            # Exception bei der Suche
  expect_equal(nrow(aus), 14)                                       # eine Zeile je Link
  status <- stats::setNames(aus$online_meeting_lookup_http_status, aus$join_url)
  expect_equal(status[["https://teams/h"]], 500L)
  expect_equal(status[["https://teams/i"]], 502L)
  expect_equal(status[["https://teams/m"]], 503L)
  expect_true(is.na(status[["https://teams/d"]]))
  expect_true(is.na(status[["https://teams/o"]]))
  # Je E-Mail nur eine Graph-Abfrage pro Lauf
  expect_equal(abfragen[["unbekannt@bertelsmann.de"]], 1L)
  expect_equal(abfragen[["x@studyflix.de"]], 1L)   # Rueckfall nach 403 nur einmal, dann gesperrt
})

# --- Veraltete oid aus msgraph_users: Rueckfall per E-Mail ------------------------

zaehle <- function(env, key) assign(key, (env[[key]] %||% 0L) + 1L, envir = env)

test_that("calls_attendance: 403/404 auf eine veraltete oid sucht mit der oid aus der E-Mail weiter", {
  disc <- data.frame(join_url = c("https://teams/a1", "https://teams/a2", "https://teams/b1", "https://teams/b2"),
                     organizer_oid = c("ALT1", "ALT1", "ALT2", "ALT2"),
                     organizer_email = c("eins@studyflix.de", "eins@studyflix.de",
                                         "zwei@studyflix.de", "zwei@studyflix.de"),
                     stringsAsFactors = FALSE)
  suchen <- new.env(); konten <- new.env()
  write <- mockery::mock(1L)
  mockery::stub(msgraph_scoped_update_calls_attendance, "discover_meetings_from_events",
                function(con, cfg) disc)
  mockery::stub(msgraph_scoped_update_calls_attendance, "resolve_meeting", function(oid, ju, tok) {
    zaehle(suchen, oid)
    switch(oid, ALT1 = list(status = 403, id = NA_character_), ALT2 = list(status = 404, id = NA_character_),
           list(status = 200, id = paste0("MID_", ju)))
  })
  mockery::stub(msgraph_scoped_update_calls_attendance, "resolve_organizer_by_email", function(email, tok) {
    zaehle(konten, email)
    list(status = 200, id = if (email == "eins@studyflix.de") "NEU1" else "NEU2")
  })
  mockery::stub(msgraph_scoped_update_calls_attendance, "attendance_records",
                function(oid, mid, tok) list(status = 200, reports = list()))
  mockery::stub(msgraph_scoped_update_calls_attendance, "assert_online_meeting_lookup_columns", TRUE)
  mockery::stub(msgraph_scoped_update_calls_attendance, "write_online_meeting_lookups", write)

  suppressMessages(msgraph_scoped_update_calls_attendance(con = NULL, app_token = "t",
                                                          cfg = lookup_cfg, dry_run = FALSE))

  aus <- mockery::mock_args(write)[[1]][[3]]
  expect_true(all(aus$online_meeting_lookup == "gefunden_ohne_bericht"))
  expect_equal(nrow(aus), 4)
  # Die alte oid wird je Organizer genau einmal gefragt, danach direkt die neue
  expect_equal(suchen[["ALT1"]], 1L); expect_equal(suchen[["ALT2"]], 1L)
  expect_equal(suchen[["NEU1"]], 2L); expect_equal(suchen[["NEU2"]], 2L)
  expect_equal(konten[["eins@studyflix.de"]], 1L); expect_equal(konten[["zwei@studyflix.de"]], 1L)
})

test_that("calls_attendance: Rueckfall ohne neue oid behaelt den Ausgang und sperrt die oid", {
  # S: E-Mail liefert dieselbe oid -> policy_403, gesperrt
  # U: E-Mail ohne Treffer -> organisator_unbekannt, gesperrt (kein weiterer 404-Aufruf)
  # E: E-Mail-Abfrage scheitert -> policy_403, aber NICHT gesperrt: der naechste Link fragt neu
  disc <- data.frame(join_url = paste0("https://teams/", c("s1", "s2", "u1", "u2", "e1", "e2")),
                     organizer_oid = c("OID_S", "OID_S", "OID_U", "OID_U", "OID_E", "OID_E"),
                     organizer_email = c("s@studyflix.de", "s@studyflix.de", "u@studyflix.de",
                                         "u@studyflix.de", "e@studyflix.de", "e@studyflix.de"),
                     stringsAsFactors = FALSE)
  suchen <- new.env(); konten <- new.env()
  write <- mockery::mock(1L)
  mockery::stub(msgraph_scoped_update_calls_attendance, "discover_meetings_from_events",
                function(con, cfg) disc)
  mockery::stub(msgraph_scoped_update_calls_attendance, "resolve_meeting", function(oid, ju, tok) {
    zaehle(suchen, oid)
    list(status = if (oid == "OID_U") 404 else 403, id = NA_character_)
  })
  mockery::stub(msgraph_scoped_update_calls_attendance, "resolve_organizer_by_email", function(email, tok) {
    zaehle(konten, email)
    switch(email, "s@studyflix.de" = list(status = 200, id = "OID_S"),
           "u@studyflix.de" = list(status = 200, id = NA_character_),
           "e@studyflix.de" = stop("Graph weg"))
  })
  mockery::stub(msgraph_scoped_update_calls_attendance, "assert_online_meeting_lookup_columns", TRUE)
  mockery::stub(msgraph_scoped_update_calls_attendance, "write_online_meeting_lookups", write)

  # Beide Links von E scheitern an der E-Mail-Abfrage -> eine Log-Zeile mit 2
  expect_message(
    n <- msgraph_scoped_update_calls_attendance(con = NULL, app_token = "t",
                                                cfg = lookup_cfg, dry_run = FALSE),
    "^2 E-Mail-Abfrage\\(n\\) im Rueckfall")
  expect_equal(n, 0)

  aus <- mockery::mock_args(write)[[1]][[3]]
  ist <- stats::setNames(aus$online_meeting_lookup, aus$join_url)
  expect_equal(unname(ist[paste0("https://teams/", c("s1", "s2", "u1", "u2", "e1", "e2"))]),
               c("policy_403", "policy_403", "organisator_unbekannt", "organisator_unbekannt",
                 "policy_403", "policy_403"))
  expect_equal(suchen[["OID_S"]], 1L); expect_equal(konten[["s@studyflix.de"]], 1L)
  expect_equal(suchen[["OID_U"]], 1L); expect_equal(konten[["u@studyflix.de"]], 1L)
  expect_equal(suchen[["OID_E"]], 2L); expect_equal(konten[["e@studyflix.de"]], 2L)
})

test_that("calls_attendance: gescheiterte Konto-Abfrage wird nicht gemerkt, der naechste Link fragt neu", {
  disc <- data.frame(join_url = c("https://teams/a", "https://teams/b", "https://teams/c"),
                     organizer_oid = c(NA, NA, "OID_OK"),
                     organizer_email = c("neu@bertelsmann.de", "neu@bertelsmann.de", "ok@studyflix.de"),
                     stringsAsFactors = FALSE)
  konten <- new.env(); suchen <- new.env()
  mockery::stub(msgraph_scoped_update_calls_attendance, "discover_meetings_from_events",
                function(con, cfg) disc)
  mockery::stub(msgraph_scoped_update_calls_attendance, "resolve_organizer_by_email", function(email, tok) {
    zaehle(konten, email)
    if (konten[[email]] == 1L) list(status = 503, id = NA_character_) else list(status = 200, id = "OID_NEU")
  })
  mockery::stub(msgraph_scoped_update_calls_attendance, "resolve_meeting", function(oid, ju, tok) {
    zaehle(suchen, oid); list(status = 200, id = "MID1")
  })
  mockery::stub(msgraph_scoped_update_calls_attendance, "attendance_records",
                function(oid, mid, tok) list(status = 200, reports = list(bericht_mit_teilnehmer)))

  expect_message(
    msgraph_scoped_update_calls_attendance(con = NULL, app_token = "t", cfg = lookup_cfg, dry_run = TRUE),
    "gefunden_mit_bericht 2, gefunden_ohne_bericht 0, nicht_gefunden 0, abruf_fehler 1", fixed = TRUE)
  expect_equal(konten[["neu@bertelsmann.de"]], 2L)   # 503 nicht gecacht, zweiter Versuch gelingt
  expect_equal(suchen[["OID_NEU"]], 1L)
})

# --- Schreiben: auch bei 0 Calls, nicht bei dry_run, nicht bei Abbruch -----------

test_that("calls_attendance schreibt die Ausgaenge auch, wenn kein Call entstand", {
  write <- mockery::mock(1L)
  assert_cols <- mockery::mock(TRUE)
  mockery::stub(msgraph_scoped_update_calls_attendance, "discover_meetings_from_events",
                function(con, cfg) data.frame(join_url = c("https://teams/a", "https://teams/b"),
                                              organizer_oid = c("OID1", "OID2")))
  mockery::stub(msgraph_scoped_update_calls_attendance, "resolve_meeting", function(oid, ju, tok) {
    if (oid == "OID1") list(status = 200, id = "MID1") else list(status = 403, id = NA_character_)
  })
  mockery::stub(msgraph_scoped_update_calls_attendance, "attendance_records",
                function(oid, mid, tok) list(status = 200, reports = list()))
  mockery::stub(msgraph_scoped_update_calls_attendance, "assert_online_meeting_lookup_columns", assert_cols)
  mockery::stub(msgraph_scoped_update_calls_attendance, "write_online_meeting_lookups", write)

  n <- suppressMessages(msgraph_scoped_update_calls_attendance(con = NULL, app_token = "t",
                                                               cfg = lookup_cfg, dry_run = FALSE))

  expect_equal(n, 0)
  mockery::expect_called(assert_cols, 1)
  mockery::expect_called(write, 1)
  aus <- mockery::mock_args(write)[[1]][[3]]
  expect_equal(aus$online_meeting_lookup[aus$join_url == "https://teams/a"], "gefunden_ohne_bericht")
  expect_equal(aus$online_meeting_lookup[aus$join_url == "https://teams/b"], "policy_403")
})

test_that("calls_attendance schreibt bei dry_run keine Ausgaenge und prueft keine Spalten", {
  write <- mockery::mock(1L)
  assert_cols <- mockery::mock(TRUE)
  mockery::stub(msgraph_scoped_update_calls_attendance, "discover_meetings_from_events",
                function(con, cfg) data.frame(join_url = "https://teams/a", organizer_oid = "OID1"))
  mockery::stub(msgraph_scoped_update_calls_attendance, "resolve_meeting",
                function(oid, ju, tok) list(status = 200, id = "MID1"))
  mockery::stub(msgraph_scoped_update_calls_attendance, "attendance_records",
                function(oid, mid, tok) list(status = 200, reports = list(bericht_mit_teilnehmer)))
  mockery::stub(msgraph_scoped_update_calls_attendance, "assert_online_meeting_lookup_columns", assert_cols)
  mockery::stub(msgraph_scoped_update_calls_attendance, "write_online_meeting_lookups", write)

  expect_message(
    n <- msgraph_scoped_update_calls_attendance(con = NULL, app_token = "t",
                                                cfg = lookup_cfg, dry_run = TRUE),
    "gefunden_mit_bericht 1")
  expect_equal(n, 1)
  mockery::expect_called(write, 0)
  mockery::expect_called(assert_cols, 0)
})

test_that("calls_attendance schreibt keine Ausgaenge, wenn der Lauf abbricht", {
  write <- mockery::mock(1L)
  mockery::stub(msgraph_scoped_update_calls_attendance, "discover_meetings_from_events",
                function(con, cfg) data.frame(join_url = "https://teams/a", organizer_oid = "OID1"))
  mockery::stub(msgraph_scoped_update_calls_attendance, "resolve_meeting",
                function(oid, ju, tok) list(status = 401, id = NA_character_))
  mockery::stub(msgraph_scoped_update_calls_attendance, "assert_online_meeting_lookup_columns", TRUE)
  mockery::stub(msgraph_scoped_update_calls_attendance, "write_online_meeting_lookups", write)

  expect_error(suppressMessages(msgraph_scoped_update_calls_attendance(
    con = NULL, app_token = "t", cfg = lookup_cfg, dry_run = FALSE)), "Abbruch")
  mockery::expect_called(write, 0)
})

# --- Fehlerquote --------------------------------------------------------------

test_that("Fehlerquote: viele policy_403 und organisator_unbekannt brechen nicht ab", {
  disc <- data.frame(join_url = paste0("https://teams/", 1:23),
                     organizer_oid = c("OK", paste0("P", 1:11), paste0("U", 1:11)))
  mockery::stub(msgraph_scoped_update_calls_attendance, "discover_meetings_from_events",
                function(con, cfg) disc)
  mockery::stub(msgraph_scoped_update_calls_attendance, "resolve_meeting", function(oid, ju, tok) {
    if (oid == "OK") list(status = 200, id = "MID1")
    else if (startsWith(oid, "P")) list(status = 403, id = NA_character_)
    else list(status = 404, id = NA_character_)
  })
  mockery::stub(msgraph_scoped_update_calls_attendance, "attendance_records",
                function(oid, mid, tok) list(status = 200, reports = list(bericht_mit_teilnehmer)))

  expect_equal(suppressMessages(msgraph_scoped_update_calls_attendance(
    con = NULL, app_token = "t", cfg = lookup_cfg, dry_run = TRUE)), 1)
})

test_that("Fehlerquote: viele nicht_gefunden brechen ab", {
  disc <- data.frame(join_url = paste0("https://teams/", 1:12),
                     organizer_oid = c("OK", paste0("N", 1:11)))
  mockery::stub(msgraph_scoped_update_calls_attendance, "discover_meetings_from_events",
                function(con, cfg) disc)
  mockery::stub(msgraph_scoped_update_calls_attendance, "resolve_meeting", function(oid, ju, tok) {
    if (oid == "OK") list(status = 200, id = "MID1") else list(status = 200, id = NA_character_)
  })
  mockery::stub(msgraph_scoped_update_calls_attendance, "attendance_records",
                function(oid, mid, tok) list(status = 200, reports = list(bericht_mit_teilnehmer)))

  expect_error(suppressMessages(msgraph_scoped_update_calls_attendance(
    con = NULL, app_token = "t", cfg = lookup_cfg, dry_run = TRUE)), "scheiterten")
})

# --- Reine Helfer -------------------------------------------------------------

test_that("lookup_outcome_*: Status -> Ausgang", {
  expect_equal(lookup_outcome_suche(403, NA_character_)$ausgang, "policy_403")
  expect_equal(lookup_outcome_suche(404, NA_character_)$ausgang, "organisator_unbekannt")
  expect_equal(lookup_outcome_suche(200, NA_character_)$ausgang, "nicht_gefunden")
  expect_true(is.na(lookup_outcome_suche(200, "MID")$ausgang))
  expect_equal(lookup_outcome_suche(500, NA_character_), list(ausgang = "abruf_fehler", http_status = 500L))
  expect_true(is.na(lookup_outcome_suche(403, NA_character_)$http_status))

  expect_true(is.na(lookup_outcome_konto(200, "OID")$ausgang))
  expect_equal(lookup_outcome_konto(200, NA_character_)$ausgang, "organisator_unbekannt")
  expect_equal(lookup_outcome_konto(429, NA_character_), list(ausgang = "abruf_fehler", http_status = 429L))

  expect_equal(lookup_outcome_bericht(200, 2L)$ausgang, "gefunden_mit_bericht")
  expect_equal(lookup_outcome_bericht(200, 0L)$ausgang, "gefunden_ohne_bericht")
  expect_equal(lookup_outcome_bericht(404, 0L), list(ausgang = "abruf_fehler", http_status = 404L))
  expect_equal(lookup_outcome_bericht(NA, 0L), list(ausgang = "abruf_fehler", http_status = NA_integer_))
})

test_that("aggregate_online_meeting_lookups nimmt den besten Ausgang je Link", {
  versuche <- tibble::tibble(
    join_url    = c("a", "a", "b", "b", "c", "c", "d"),
    ausgang     = c("organisator_unbekannt", "nicht_gefunden", "policy_403", "abruf_fehler",
                    "gefunden_ohne_bericht", "gefunden_mit_bericht", "abruf_fehler"),
    http_status = c(NA, NA, NA, 500L, NA, NA, 503L))
  aus <- aggregate_online_meeting_lookups(versuche)
  ist <- stats::setNames(aus$online_meeting_lookup, aus$join_url)
  expect_equal(unname(ist[c("a", "b", "c", "d")]),
               c("nicht_gefunden", "abruf_fehler", "gefunden_mit_bericht", "abruf_fehler"))
  status <- stats::setNames(aus$online_meeting_lookup_http_status, aus$join_url)
  expect_equal(unname(status[c("a", "b", "c", "d")]), c(NA, 500L, NA, 503L))
})

test_that("online_meeting_lookup_sql aendert nur Abweichungen und verschlechtert keinen Fund", {
  sql <- online_meeting_lookup_sql("raw", "tmp_x")
  squish <- function(x) gsub("\\s+", " ", x)
  s <- squish(sql)
  # 1. Nur Zeilen mit anderem Wert - sonst zoege der Trigger updated_at jede Nacht mit
  expect_match(s, "AND (e.online_meeting_lookup IS DISTINCT FROM t.lookup OR e.online_meeting_lookup_http_status IS DISTINCT FROM t.http_status)", fixed = TRUE)
  # 2. Zeitstempel springt nur bei neuem Ausgang
  expect_match(s, "online_meeting_lookup_at = CASE WHEN e.online_meeting_lookup IS DISTINCT FROM t.lookup THEN timezone('UTC', now()) ELSE e.online_meeting_lookup_at END", fixed = TRUE)
  # 3. Ein gefunden_-Wert wird nie durch einen schlechteren ersetzt
  expect_match(s, "AND (e.online_meeting_lookup IS NULL OR e.online_meeting_lookup NOT IN ('gefunden_mit_bericht', 'gefunden_ohne_bericht') OR t.lookup IN ('gefunden_mit_bericht', 'gefunden_ohne_bericht'))", fixed = TRUE)
  # Alle Events mit derselben join_url, Schema aus der config
  expect_match(s, "WHERE e.join_url = t.join_url", fixed = TRUE)
  expect_match(squish(online_meeting_lookup_sql("raw_scoped_test", "tmp_x")),
               "UPDATE raw_scoped_test.msgraph_events e", fixed = TRUE)
})

test_that("assert_online_meeting_lookup_columns bricht ab, wenn Spalten fehlen", {
  gesehen <- NULL
  testthat::local_mocked_bindings(
    dbGetQuery = function(conn, statement, ...) {
      gesehen <<- list(sql = statement, args = list(...))
      data.frame(n = 2L)
    },
    .package = "DBI")
  expect_error(assert_online_meeting_lookup_columns(NULL, "raw_scoped_test"),
               "2026-10-07-msgraph-events-online-meeting-lookup.sql", fixed = TRUE)
  expect_match(gesehen$sql, "table_schema = $1", fixed = TRUE)
  expect_equal(gesehen$args$params, list("raw_scoped_test"))
})

test_that("assert_online_meeting_lookup_columns laesst den Lauf mit allen drei Spalten weiter", {
  testthat::local_mocked_bindings(dbGetQuery = function(conn, statement, ...) data.frame(n = 3L),
                                  .package = "DBI")
  expect_true(assert_online_meeting_lookup_columns(NULL, "raw"))
})

test_that("write_online_meeting_lookups ruehrt die Verbindung bei leerem Ergebnis nicht an", {
  leer <- tibble::tibble(join_url = character(), online_meeting_lookup = character(),
                         online_meeting_lookup_http_status = integer())
  expect_equal(write_online_meeting_lookups(NULL, "raw", leer), 0L)
})

test_that("write_online_meeting_lookups schreibt Temp-Tabelle und UPDATE in einer Transaktion", {
  aus <- tibble::tibble(join_url = c("a", "b"),
                        online_meeting_lookup = c("abruf_fehler", "policy_403"),
                        online_meeting_lookup_http_status = c(500L, NA_integer_))
  gesehen <- NULL
  testthat::local_mocked_bindings(
    poolWithTransaction = function(pool, func) {
      gesehen <<- list(werte = environment(func)$werte, sql = environment(func)$sql); 2L
    },
    .package = "pool")
  n <- suppressMessages(write_online_meeting_lookups(structure(list(), class = "Pool"), "raw", aus))
  expect_equal(n, 2L)
  expect_equal(gesehen$werte$lookup, c("abruf_fehler", "policy_403"))
  expect_type(gesehen$werte$http_status, "integer")
  expect_match(gesehen$sql, "FROM tmp_online_meeting_lookup t", fixed = TRUE)
})

# --- Discovery ----------------------------------------------------------------

test_that("discover_meetings_from_events behaelt Organisatoren ohne msgraph_users-Zeile", {
  gesehen <- NULL
  testthat::local_mocked_bindings(
    dbQuoteLiteral = function(conn, x, ...) paste0("'", x, "'"),
    dbGetQuery = function(conn, statement, ...) {
      gesehen <<- statement
      data.frame(join_url = c("https://teams/TEN/a", "https://teams/TEN/b", "https://teams/ALT/c"),
                 organizer_oid = c("OID1", NA, "OID2"),
                 organizer_email = c("rep@studyflix.de", "neu@bertelsmann.de", "rep@studyflix.de"),
                 stringsAsFactors = FALSE)
    },
    .package = "DBI")
  out <- discover_meetings_from_events(NULL, list(raw_schema = "raw", events_days_back = 50,
                                                  tenant_id = "TEN"))
  s <- gsub("\\s+", " ", gesehen)
  # Hoechstens eine msgraph_users-Zeile je Kontakt, ohne Platzhalter, nicht
  # geloeschte und zuletzt aktualisierte zuerst
  expect_match(s, "LEFT JOIN LATERAL ( SELECT mu.msgraph_user_id, mu.is_internal FROM raw.msgraph_users mu", fixed = TRUE)
  expect_match(s, "AND mu.msgraph_user_id NOT LIKE 'merged-%'", fixed = TRUE)
  # interne vor externen, mu.id macht die Wahl bei Gleichstand deterministisch
  expect_match(s, paste0("ORDER BY (mu.is_deleted IS TRUE), (mu.is_internal IS NOT TRUE), ",
                         "mu.updated_at DESC NULLS LAST, mu.id DESC LIMIT 1 ) u ON TRUE"), fixed = TRUE)
  expect_match(s, "(u.msgraph_user_id IS NULL OR u.is_internal)", fixed = TRUE)
  expect_match(s, "lower(ct.email) AS organizer_email", fixed = TRUE)
  # is_deleted steuert nur die Auswahl, filtert aber keinen Organisator heraus
  expect_false(grepl("NOT (mu|u)\\.is_deleted", s))
  expect_equal(names(out), c("join_url", "organizer_oid", "organizer_email"))
  expect_equal(out$organizer_oid, c("OID1", NA))
  expect_equal(attr(out, "n_alt_tenant"), 1L)
  expect_equal(attr(out, "n_kandidaten"), 3L)
})

test_that("resolve_organizer_by_email fragt UPN oder mail ab und quotet Apostrophe", {
  gesehen <- NULL
  mockery::stub(resolve_organizer_by_email, "graph_get", function(url, token, query = NULL) {
    gesehen <<- list(url = url, query = query)
    list(status = 200, content = list(value = list(list(id = "OID9"))))
  })
  res <- resolve_organizer_by_email("o'neil@bertelsmann.de", "t")
  expect_equal(res, list(status = 200, id = "OID9"))
  expect_equal(gesehen$url, "https://graph.microsoft.com/v1.0/users")
  expect_equal(gesehen$query$`$filter`,
               "userPrincipalName eq 'o''neil@bertelsmann.de' or mail eq 'o''neil@bertelsmann.de'")
  expect_equal(gesehen$query$`$select`, "id")
})

test_that("calls_attendance: Graph 200 ohne Bericht wird durch das echte attendance_records gefunden_ohne_bericht", {
  # Die anderen Job-Tests stubben attendance_records selbst. Hier laeuft der
  # echte Helfer, nur graph_collect ist im Paket-Namespace ersetzt. Vor dem Fix
  # warf er "subscript out of bounds", und der No-Show landete als abruf_fehler.
  testthat::local_mocked_bindings(
    graph_collect = function(...) list(status = 200, error = NULL, value = list()))
  write <- mockery::mock(1L)
  mockery::stub(msgraph_scoped_update_calls_attendance, "discover_meetings_from_events",
                function(con, cfg) data.frame(join_url = "https://teams/a", organizer_oid = "OID1",
                                              organizer_email = "a@studyflix.de"))
  mockery::stub(msgraph_scoped_update_calls_attendance, "resolve_meeting",
                function(oid, ju, tok) list(status = 200, id = "MID1"))
  mockery::stub(msgraph_scoped_update_calls_attendance, "assert_online_meeting_lookup_columns", TRUE)
  mockery::stub(msgraph_scoped_update_calls_attendance, "write_online_meeting_lookups", write)

  suppressMessages(msgraph_scoped_update_calls_attendance(con = NULL, app_token = "t",
                                                          cfg = lookup_cfg, dry_run = FALSE))

  aus <- mockery::mock_args(write)[[1]][[3]]
  expect_equal(aus$online_meeting_lookup, "gefunden_ohne_bericht")
})

test_that("calls_attendance: ein R-Fehler im Graph-Helfer wird gezaehlt und mit Meldung geloggt", {
  write <- mockery::mock(1L)
  mockery::stub(msgraph_scoped_update_calls_attendance, "discover_meetings_from_events",
                function(con, cfg) data.frame(join_url = "https://teams/a", organizer_oid = "OID1",
                                              organizer_email = "a@studyflix.de"))
  mockery::stub(msgraph_scoped_update_calls_attendance, "resolve_meeting",
                function(oid, ju, tok) list(status = 200, id = "MID1"))
  mockery::stub(msgraph_scoped_update_calls_attendance, "attendance_records",
                function(oid, mid, tok) stop("kaputter Helfer"))
  mockery::stub(msgraph_scoped_update_calls_attendance, "assert_online_meeting_lookup_columns", TRUE)
  mockery::stub(msgraph_scoped_update_calls_attendance, "write_online_meeting_lookups", write)

  # Ein einziger Link, dessen Abruf scheitert: der Lauf bricht (wie gewollt) ab.
  # Die Meldung muss VOR dem Abbruch im Log stehen, sonst sieht man im
  # FlowForce-Log wieder nur die Fehlerquote.
  log <- new.env(); log$meldungen <- character(0)
  expect_error(withCallingHandlers(
    msgraph_scoped_update_calls_attendance(con = NULL, app_token = "t", cfg = lookup_cfg, dry_run = FALSE),
    message = function(m) {
      log$meldungen <- c(log$meldungen, conditionMessage(m))
      invokeRestart("muffleMessage")
    }), "kein einziger Attendance-Report")

  expect_true(any(grepl("1 R-Fehler in Graph-Abfragen.*kaputter Helfer", log$meldungen)))
  mockery::expect_called(write, 0)
})
