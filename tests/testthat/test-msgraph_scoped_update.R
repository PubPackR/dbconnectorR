# Contract-Tests fuer die scoped update_*-Funktionen (Go-Live-Blocker-Fixes):
# - dry_run darf NICHT schreiben (kein Upsert)
# - kein Massen-Soft-Delete mehr (kein DBI::dbExecute-UPDATE)
# Hinweis: mockery::stub() muss IM test_that-Block stehen (Scope), nicht in einem Helper.

fake_cfg <- list(service_account_upn = "sa@studyflix.de",
                 raw_schema = "raw", processed_schema = "processed")

fake_calendars <- function(url, token, query = NULL)
  list(status = 200, value = list(list(owner = list(address = "rep.a@studyflix.de"))))

fake_user_lookup <- function(url, token, query = NULL)
  list(content = list(value = list(list(id = "OID1", givenName = "Rep", surname = "A",
    userPrincipalName = "rep.a@studyflix.de", displayName = "Rep A", mail = "rep.a@studyflix.de"))))

test_that("msgraph_scoped_update_users: dry_run schreibt NICHT (kein Upsert)", {
  upsert <- mockery::mock()
  mockery::stub(msgraph_scoped_update_users, "graph_collect", fake_calendars)
  mockery::stub(msgraph_scoped_update_users, "graph_get", fake_user_lookup)
  mockery::stub(msgraph_scoped_update_users, "Billomatics::postgres_upsert_data", upsert)

  n <- msgraph_scoped_update_users(con = NULL, app_token = "t", del_token = "t",
                                   cfg = fake_cfg, dry_run = TRUE)

  expect_equal(n, 1)
  mockery::expect_called(upsert, 0)
})

test_that("msgraph_scoped_update_users: ohne dry_run upsertet und macht KEINEN Massen-Soft-Delete", {
  upsert <- mockery::mock()
  exec   <- mockery::mock()   # DBI::dbExecute (frueher der Soft-Delete) darf nie feuern
  mockery::stub(msgraph_scoped_update_users, "graph_collect", fake_calendars)
  mockery::stub(msgraph_scoped_update_users, "graph_get", fake_user_lookup)
  mockery::stub(msgraph_scoped_update_users, "Billomatics::postgres_upsert_data", upsert)
  mockery::stub(msgraph_scoped_update_users, "DBI::dbExecute", exec)

  n <- msgraph_scoped_update_users(con = NULL, app_token = "t", del_token = "t",
                                   cfg = fake_cfg, dry_run = FALSE)

  expect_equal(n, 1)
  mockery::expect_called(upsert, 1)
  mockery::expect_called(exec, 0)
})

# --- Calls: Discovery aus delegierten Events ----------------------------------

fake_attendance <- function(oid, mid, tok)
  list(status = 200, meeting_start = "2026-08-10T10:00:00Z", meeting_end = "2026-08-10T10:30:00Z",
       reports = list(list(attendanceRecords = list(
         list(emailAddress = "REP.A@studyflix.de", identity = list(displayName = "Rep A"),
              role = "Organizer", totalAttendanceInSeconds = 1800)))))

test_that("calls_attendance: Discovery kommt aus den Events, dry_run schreibt NICHT", {
  upsert <- mockery::mock()
  mockery::stub(msgraph_scoped_update_calls_attendance, "discover_meetings_from_events",
                function(con, cfg) data.frame(join_url = "https://teams/x", organizer_oid = "OID1"))
  mockery::stub(msgraph_scoped_update_calls_attendance, "resolve_meeting",
                function(oid, ju, tok) list(status = 200, id = "MID1"))
  mockery::stub(msgraph_scoped_update_calls_attendance, "attendance_records", fake_attendance)
  mockery::stub(msgraph_scoped_update_calls_attendance, "Billomatics::postgres_upsert_data", upsert)

  n <- msgraph_scoped_update_calls_attendance(con = NULL, app_token = "t",
                                              cfg = fake_cfg, dry_run = TRUE)

  expect_equal(n, 1)
  mockery::expect_called(upsert, 0)
})

test_that("calls_attendance: 403 blockt den Organizer, weitere Meetings derselben oid werden uebersprungen", {
  resolve <- mockery::mock(list(status = 403, id = NA_character_))
  mockery::stub(msgraph_scoped_update_calls_attendance, "discover_meetings_from_events",
                function(con, cfg) data.frame(join_url = c("https://teams/a", "https://teams/b"),
                                              organizer_oid = c("OID1", "OID1")))
  mockery::stub(msgraph_scoped_update_calls_attendance, "resolve_meeting", resolve)

  n <- msgraph_scoped_update_calls_attendance(con = NULL, app_token = "t",
                                              cfg = fake_cfg, dry_run = TRUE)

  expect_equal(n, 0)
  mockery::expect_called(resolve, 1)   # zweites Meeting derselben oid nicht mehr angefragt
})

# --- Organizer-Aufloesung (try-all) -------------------------------------------

test_that("resolve_transcript_source nimmt den ersten Kandidaten mit 200 + Transkripten", {
  mockery::stub(resolve_transcript_source, "graph_collect", function(url, token, query = NULL) {
    if (grepl("/users/ORG/", url, fixed = TRUE)) list(status = 200, value = list(list(id = "t1")))
    else list(status = 403, value = list())
  })
  src <- resolve_transcript_source(c("NOPE", "ORG"), mid = "MID1", app_token = "t")
  expect_equal(src$oid, "ORG")
  expect_equal(length(src$value), 1)
})

test_that("resolve_transcript_source gibt NULL, wenn kein Kandidat 200 liefert", {
  mockery::stub(resolve_transcript_source, "graph_collect",
                function(url, token, query = NULL) list(status = 403, value = list()))
  expect_null(resolve_transcript_source(c("A", "B"), mid = "MID1", app_token = "t"))
})

# --- DSGVO-Suppression --------------------------------------------------------

test_that("dsgvo_suppress_participants ist no-op ohne Pepper (kein DB-Zugriff)", {
  load_mock <- mockery::mock()
  mockery::stub(dsgvo_suppress_participants, "Billomatics::dsgvo_load_suppression", load_mock)
  df <- tibble::tibble(email = "a@x.de", ms_name = "A")
  out <- dsgvo_suppress_participants(df, con = NULL, suppression_pepper = NULL)
  expect_identical(out, df)
  mockery::expect_called(load_mock, 0)
})

test_that("dsgvo_suppress_participants tombstonet gesperrte Mail, laesst andere unveraendert", {
  del_hash <- Billomatics::dsgvo_hash_email("del@x.de", "pep")
  mockery::stub(dsgvo_suppress_participants, "Billomatics::dsgvo_load_suppression",
                function(con) list(email_hashes = del_hash, phone_hashes = character(0)))
  df <- tibble::tibble(email = c("del@x.de", "keep@x.de"), ms_name = c("Del", "Keep"))
  out <- dsgvo_suppress_participants(df, con = NULL, suppression_pepper = "pep")
  expect_true(grepl("^\\[geloescht\\]-", out$email[1]))
  expect_true(is.na(out$ms_name[1]))
  expect_equal(out$email[2], "keep@x.de")
  expect_equal(out$ms_name[2], "Keep")
})
