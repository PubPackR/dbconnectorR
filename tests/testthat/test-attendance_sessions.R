# Ein Call je Session (Anwesenheitsbericht), nicht je Online-Meeting (ADR 0002).
# Der Fall aus der Praxis: ein wiederverwendeter Teams-Link (Serie, persoenlicher
# Link) hat Berichte an mehreren Tagen. Bisher bekam nur der erste Tag einen Call,
# jeder weitere Termin am selben Link zaehlte als No-Show.

bericht <- function(id, start, ende, email) {
  list(id = id, meetingStartDateTime = start, meetingEndDateTime = ende,
       attendanceRecords = list(list(emailAddress = email,
                                     identity = list(id = "U", displayName = "X"),
                                     role = "Attendee", totalAttendanceInSeconds = 600)))
}

zwei_tage <- list(
  bericht("REP1", "2026-09-01T09:00:00Z", "2026-09-01T09:30:00Z", "kunde.a@firma.de"),
  bericht("REP2", "2026-09-08T09:00:00Z", "2026-09-08T09:30:00Z", "kunde.b@firma.de"))

test_that("Jeder Bericht wird ein eigener Call mit eigenem Datum und eigenen Teilnehmern", {
  s <- sessions_from_reports(zwei_tage, online_meeting_id = "OM1", meeting_id = "THREAD1")

  expect_equal(s$calls$msgraph_call_id, c("REP1", "REP2"))
  expect_equal(as.Date(s$calls$call_start), as.Date(c("2026-09-01", "2026-09-08")))
  expect_equal(unique(s$calls$msgraph_online_meeting_id), "OM1")
  expect_equal(unique(s$calls$meeting_id), "THREAD1")
  # Teilnehmer haengen am Bericht, nicht am Online-Meeting
  expect_equal(s$parts$email[s$parts$meeting_id == "REP1"], "kunde.a@firma.de")
  expect_equal(s$parts$email[s$parts$meeting_id == "REP2"], "kunde.b@firma.de")
})

test_that("Ein Bericht ohne ID oder Start faellt weg und wird gezaehlt", {
  kaputt <- list(bericht(NULL, "2026-09-01T09:00:00Z", NULL, "a@firma.de"),
                 bericht("REP3", NULL, NULL, "b@firma.de"))
  s <- sessions_from_reports(c(zwei_tage[1], kaputt), "OM1", "THREAD1")
  expect_equal(s$calls$msgraph_call_id, "REP1")
  expect_equal(attr(s, "n_ohne_start"), 2L)
})

test_that("Ein Bericht ohne Ende bekommt den Start als Ende (NOT NULL)", {
  s <- sessions_from_reports(list(bericht("REP1", "2026-09-01T09:00:00Z", NULL, "a@firma.de")),
                             "OM1", "THREAD1")
  expect_equal(s$calls$call_end, s$calls$call_start)
})

test_that("calls_attendance zaehlt Sessions, nicht Online-Meetings", {
  upsert <- mockery::mock()
  mockery::stub(msgraph_scoped_update_calls_attendance, "discover_meetings_from_events",
                function(con, cfg) data.frame(join_url = "https://teams/x", organizer_oid = "OID1"))
  mockery::stub(msgraph_scoped_update_calls_attendance, "resolve_meeting",
                function(oid, ju, tok) list(status = 200, id = "OM1"))
  mockery::stub(msgraph_scoped_update_calls_attendance, "attendance_records",
                function(oid, mid, tok) list(status = 200, meeting_start = NA, meeting_end = NA,
                                             reports = zwei_tage))
  mockery::stub(msgraph_scoped_update_calls_attendance, "Billomatics::postgres_upsert_data", upsert)

  n <- msgraph_scoped_update_calls_attendance(con = NULL, app_token = "t",
                                              cfg = list(raw_schema = "raw"), dry_run = TRUE)
  expect_equal(n, 2)
  mockery::expect_called(upsert, 0)
})

# --- Umschluesselung des Bestands --------------------------------------------

sessions_om1 <- tibble::tibble(
  msgraph_call_id = c("REP1", "REP2"),
  msgraph_online_meeting_id = "OM1",
  call_start = as.POSIXct(c("2026-09-01 09:00:00", "2026-09-08 09:00:00"), tz = "UTC"))

test_that("Eine Altzeile bekommt die Session mit ihrem call_start", {
  bestand <- data.frame(id = 42, msgraph_call_id = "OM1",
                        call_start = as.POSIXct("2026-09-01 09:00:00", tz = "UTC"))
  plan <- plan_call_rekey(bestand, sessions_om1)
  expect_equal(nrow(plan), 1)
  expect_equal(plan$id, 42)
  expect_equal(plan$new_call_id, "REP1")
  expect_equal(plan$online_meeting_id, "OM1")
})

test_that("Ohne exakten Start gewinnt die naechstgelegene Session", {
  bestand <- data.frame(id = 42, msgraph_call_id = "OM1",
                        call_start = as.POSIXct("2026-09-07 23:00:00", tz = "UTC"))
  expect_equal(plan_call_rekey(bestand, sessions_om1)$new_call_id, "REP2")
})

test_that("Ohne Altzeilen gibt es nichts umzuschluesseln", {
  leer <- data.frame(id = numeric(), msgraph_call_id = character(),
                     call_start = as.POSIXct(character(), tz = "UTC"))
  expect_equal(nrow(plan_call_rekey(leer, sessions_om1)), 0)
})

test_that("rekey_meeting_calls schluesselt um und leert die Teilnehmer in einer Transaktion", {
  gesehen <- new.env(); gesehen$exec <- character(0); gesehen$tx <- FALSE
  testthat::local_mocked_bindings(
    dbQuoteLiteral = function(conn, x, ...) paste0("'", x, "'"),
    dbGetQuery = function(conn, statement, ...) {
      gesehen$select <- statement
      data.frame(id = 42, msgraph_call_id = "OM1",
                 call_start = as.POSIXct("2026-09-01 09:00:00", tz = "UTC"))
    },
    dbExecute = function(conn, statement, ...) { gesehen$exec <- c(gesehen$exec, statement); 1L },
    dbWithTransaction = function(conn, code, ...) { gesehen$tx <- TRUE; code },
    .package = "DBI")

  n <- rekey_meeting_calls(con = "con", rs = "raw", calls_df = sessions_om1)

  expect_equal(n, 1L)
  expect_true(gesehen$tx)
  expect_match(gesehen$select, "msgraph_call_id IN ('OM1')", fixed = TRUE)
  expect_match(gesehen$exec[1], "UPDATE raw.msgraph_calls", fixed = TRUE)
  expect_match(gesehen$exec[1], "(42::bigint, 'REP1', 'OM1')", fixed = TRUE)
  expect_match(gesehen$exec[2], "DELETE FROM raw.msgraph_call_participants WHERE call_id IN (42)",
               fixed = TRUE)
})

test_that("rekey_meeting_calls fasst die Datenbank ohne Online-Meeting-IDs nicht an", {
  # NULL als Verbindung: jeder DBI-Aufruf wuerde scheitern
  calls <- sessions_om1; calls$msgraph_online_meeting_id <- NA_character_
  expect_equal(rekey_meeting_calls(NULL, "raw", calls), 0L)
})

test_that("assert_online_meeting_id_column bricht ab, wenn die Spalte fehlt", {
  testthat::local_mocked_bindings(
    dbGetQuery = function(conn, statement, ...) data.frame(n = 0L), .package = "DBI")
  expect_error(assert_online_meeting_id_column("con", "raw"),
               "2026-10-06-msgraph-calls-online-meeting-id.sql", fixed = TRUE)
})

# --- Transkript an seine Session ---------------------------------------------

sessions_tr <- tibble::tibble(
  call_db_id = c(1, 2),
  call_start = as.POSIXct(c("2026-09-01 09:00:00", "2026-09-08 09:00:00"), tz = "UTC"),
  call_end   = as.POSIXct(c("2026-09-01 09:30:00", "2026-09-08 09:30:00"), tz = "UTC"))

test_that("Ein Transkript haengt an der Session, waehrend der es entstand", {
  expect_equal(assign_transcript_session(sessions_tr,
    as.POSIXct("2026-09-08 09:05:00", tz = "UTC")), 2)
  expect_equal(assign_transcript_session(sessions_tr,
    as.POSIXct("2026-09-01 09:05:00", tz = "UTC")), 1)
})

test_that("Ausserhalb jeder Session gewinnt die letzte davor, sonst die frueheste", {
  expect_equal(assign_transcript_session(sessions_tr,
    as.POSIXct("2026-09-03 12:00:00", tz = "UTC")), 1)
  expect_equal(assign_transcript_session(sessions_tr,
    as.POSIXct("2026-08-30 12:00:00", tz = "UTC")), 1)
  expect_equal(assign_transcript_session(sessions_tr, as.POSIXct(NA)), 1)
})
