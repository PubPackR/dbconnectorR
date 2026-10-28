# Absage verschwundener Events im delegierten Abruf
# (Ticket 1218304809448672: verschobene Termine blieben als No-Show stehen)

rows_of <- function(...) {
  x <- tibble::tribble(~id, ~scope, ~fehlt, ...)
  x$id <- as.character(x$id)
  x
}

test_that("plan: fehlendes Event in vollstaendig geladenem Kalender wird abgesagt", {
  rows <- rows_of("1", "rep.a", TRUE,
                  "2", "rep.a", FALSE,
                  "3", "rep.a", FALSE)
  plan <- scoped_cancellation_plan(rows)
  expect_equal(plan$ids, "1")
  expect_false(plan$coverage$skip)
})

test_that("plan: Abdeckungsschutz greift je Kalender, nicht aggregiert", {
  # rep.b liefert fast nichts (stiller Teilausfall), rep.a ist vollstaendig.
  # Aggregiert laege die Abdeckung bei 7/10 und rep.b wuerde faelschlich abgesagt.
  rows <- rows_of("1", "rep.a", FALSE, "2", "rep.a", FALSE, "3", "rep.a", FALSE,
                  "4", "rep.a", FALSE, "5", "rep.a", FALSE, "6", "rep.a", FALSE,
                  "7", "rep.a", TRUE,
                  "8", "rep.b", TRUE, "9", "rep.b", TRUE, "10", "rep.b", FALSE)
  plan <- scoped_cancellation_plan(rows)
  expect_equal(plan$ids, "7")
  expect_true(plan$coverage$skip[plan$coverage$scope == "rep.b"])
  expect_false(plan$coverage$skip[plan$coverage$scope == "rep.a"])
})

test_that("plan: Event in zwei Kalendern wird abgesagt, wenn einer den Schutz besteht", {
  rows <- rows_of("1", "rep.a", TRUE, "2", "rep.a", FALSE, "3", "rep.a", FALSE,
                  "1", "rep.b", TRUE, "4", "rep.b", TRUE)
  plan <- scoped_cancellation_plan(rows)
  expect_equal(plan$ids, "1")
})

test_that("plan: leerer Bestand sagt nichts ab", {
  plan <- scoped_cancellation_plan(rows_of())
  expect_length(plan$ids, 0)
  expect_equal(nrow(plan$coverage), 0)
})

test_that("SQL: nur offene Zeilen, nur die Quelle des Laufs, nur Teilnehmer geladener Scopes", {
  cal <- scoped_cancellation_rows_sql("raw", "tmp_s", "tmp_c", "calendar")
  expect_match(cal, "NOT coalesce(e.is_canceled, FALSE)", fixed = TRUE)
  expect_match(cal, "e.msgraph_ical_uid NOT LIKE 'booking:%'", fixed = TRUE)
  expect_match(cal, "JOIN tmp_c s ON s.email = lower(c.email)", fixed = TRUE)
  expect_match(cal, "t.event_start::timestamp = e.event_start", fixed = TRUE)
  # nur Zeilen, die der delegierte Abruf schon gesehen hat (keine app-only-Altzeilen)
  expect_match(cal, "e.join_url_checked_at IS NOT NULL", fixed = TRUE)
  book <- scoped_cancellation_rows_sql("raw", "tmp_s", "tmp_c", "booking")
  expect_match(book, "e.msgraph_ical_uid LIKE 'booking:%'", fixed = TRUE)
  expect_false(grepl("NOT LIKE", book, fixed = TRUE))
  expect_false(grepl("join_url_checked_at", book, fixed = TRUE))
})

test_that("mark_vanished_events_canceled: ohne geladenen Scope keine DB-Beruehrung", {
  expect_equal(mark_vanished_events_canceled(NULL, "raw", tibble::tibble(),
                                             tibble::tibble(email = character(0), scope = character(0)),
                                             Sys.time(), Sys.time()), 0L)
})

test_that("mark_vanished_events_canceled: Download-Schluessel gehen als UTC-Text in die Temp-Tabelle", {
  written <- list(); executed <- NULL
  mockery::stub(mark_vanished_events_canceled, "pool::poolWithTransaction", function(con, f) f("conn"))
  mockery::stub(mark_vanished_events_canceled, "DBI::dbWriteTable",
                function(conn, name, value, ...) { written[[name]] <<- value; TRUE })
  mockery::stub(mark_vanished_events_canceled, "DBI::dbGetQuery",
                function(conn, sql, params) data.frame(id = c("11", "12", "13"), scope = "rep.a",
                                                       fehlt = c(TRUE, FALSE, FALSE)))
  mockery::stub(mark_vanished_events_canceled, "DBI::dbExecute",
                function(conn, sql, params) { executed <<- params[[1]]; 1L })
  seen <- tibble::tibble(msgraph_ical_uid = "U1",
                         event_start = as.POSIXct("2026-10-01 10:00:00", tz = "UTC"))
  withr::with_timezone("Europe/Berlin", {
    n <- mark_vanished_events_canceled(structure(list(), class = "Pool"), "raw", seen,
                                       tibble::tibble(email = "Rep.A@studyflix.de", scope = "rep.a"),
                                       as.POSIXct("2026-09-08", tz = "UTC"),
                                       as.POSIXct("2027-10-28 23:59:59", tz = "UTC"))
  })
  expect_equal(n, 1L)
  expect_equal(written$tmp_cancel_seen$event_start, "2026-10-01 10:00:00")
  expect_equal(written$tmp_cancel_scope$email, "rep.a@studyflix.de")
  expect_equal(executed, "11")
})

test_that("msgraph_scoped_update_events: nur vollstaendig geladene Owner kommen in den Absage-Scope", {
  # rep.b: Kalender schlaegt fehl. rep.c: ein Kalender laedt, ein zweiter nicht.
  cals <- list(status = 200, value = list(
    list(id = "C1", owner = list(address = "Rep.A@studyflix.de")),
    list(id = "C2", owner = list(address = "rep.b@studyflix.de")),
    list(id = "C3", owner = list(address = "rep.c@studyflix.de")),
    list(id = "C4", owner = list(address = "rep.c@studyflix.de"))))
  ev <- list(iCalUId = "U1", type = "singleInstance", subject = "Termin",
             start = list(dateTime = "2026-10-01T10:00:00.0000000"),
             end = list(dateTime = "2026-10-01T10:30:00.0000000"),
             organizer = list(emailAddress = list(address = "rep.a@studyflix.de", name = "Rep A")),
             attendees = list())
  fake_collect <- function(url, token, query = NULL) {
    if (grepl("/calendarView", url)) {
      if (grepl("C1|C3", url)) list(status = 200, value = list(ev)) else list(status = 403, value = list())
    } else cals
  }
  scope_seen <- NULL
  mockery::stub(msgraph_scoped_update_events, "graph_collect", fake_collect)
  mockery::stub(msgraph_scoped_update_events, "dsgvo_suppress_participants", function(p, con, pepper) p)
  mockery::stub(msgraph_scoped_update_events, "Billomatics::postgres_upsert_data", mockery::mock())
  mockery::stub(msgraph_scoped_update_events, "mark_join_url_checked", function(...) 0L)
  mockery::stub(msgraph_scoped_update_events, "mark_vanished_events_canceled",
                function(con, rs, seen, scope_map, ...) { scope_seen <<- scope_map; 0L })
  # Der Teilnehmer-Schritt nach der Absage liest die DB - dort gezielt anhalten
  mockery::stub(msgraph_scoped_update_events, "dplyr::tbl", function(...) stop("stop nach Absage"))
  cfg <- list(service_account_upn = "sa@studyflix.de", raw_schema = "raw",
              processed_schema = "processed", events_days_back = 50, events_days_forward = 365)
  expect_error(msgraph_scoped_update_events(con = NULL, del_token = "t", cfg = cfg),
               "stop nach Absage", fixed = TRUE)
  expect_equal(scope_seen$email, "rep.a@studyflix.de")
})

test_that("msgraph_scoped_update_bookings: Staff eines fehlgeschlagenen Business faellt aus dem Scope", {
  biz <- list(status = 200, value = list(list(id = "B1"), list(id = "B2")))
  staff <- function(email) list(status = 200, value = list(list(id = "S1", emailAddress = email)))
  appt <- list(id = "A1", start = list(dateTime = "2026-10-01T10:00:00.0000000"),
               end = list(dateTime = "2026-10-01T10:30:00.0000000"), staffMemberIds = list("S1"),
               customers = list())
  fake_collect <- function(url, token, query = NULL) {
    if (grepl("/staffMembers", url)) {
      if (grepl("B1", url)) list(status = 200, value = list(list(id = "S1", emailAddress = "x@studyflix.de"),
                                                            list(id = "S2", emailAddress = "y@studyflix.de")))
      else staff("x@studyflix.de")
    } else if (grepl("/calendarView", url)) {
      if (grepl("B1", url)) list(status = 200, value = list(appt)) else list(status = 403, value = list())
    } else biz
  }
  scope_seen <- NULL
  mockery::stub(msgraph_scoped_update_bookings, "graph_collect", fake_collect)
  mockery::stub(msgraph_scoped_update_bookings, "dsgvo_suppress_participants", function(p, con, pepper) p)
  mockery::stub(msgraph_scoped_update_bookings, "Billomatics::postgres_upsert_data", mockery::mock())
  mockery::stub(msgraph_scoped_update_bookings, "mark_vanished_events_canceled",
                function(con, rs, seen, scope_map, ...) { scope_seen <<- scope_map; 0L })
  mockery::stub(msgraph_scoped_update_bookings, "dplyr::tbl", function(...) stop("stop nach Absage"))
  cfg <- list(raw_schema = "raw", processed_schema = "processed",
              events_days_back = 50, events_days_forward = 365)
  expect_error(msgraph_scoped_update_bookings(con = NULL, del_token = "t", cfg = cfg),
               "stop nach Absage", fixed = TRUE)
  # x steht auch in B2, dessen calendarView fehlschlug -> raus; y bleibt
  expect_equal(scope_seen$email, "y@studyflix.de")
})

test_that("msgraph_scoped_update_bookings: unbekannter Staff eines fehlgeschlagenen Business sagt nichts ab", {
  biz <- list(status = 200, value = list(list(id = "B1"), list(id = "B2")))
  appt <- list(id = "A1", start = list(dateTime = "2026-10-01T10:00:00.0000000"),
               end = list(dateTime = "2026-10-01T10:30:00.0000000"), staffMemberIds = list("S1"),
               customers = list())
  fake_collect <- function(url, token, query = NULL) {
    if (grepl("B2", url)) list(status = 403, value = list())
    else if (grepl("/staffMembers", url)) list(status = 200, value = list(list(id = "S1", emailAddress = "x@studyflix.de")))
    else if (grepl("/calendarView", url)) list(status = 200, value = list(appt))
    else biz
  }
  scope_seen <- NULL
  mockery::stub(msgraph_scoped_update_bookings, "graph_collect", fake_collect)
  mockery::stub(msgraph_scoped_update_bookings, "dsgvo_suppress_participants", function(p, con, pepper) p)
  mockery::stub(msgraph_scoped_update_bookings, "Billomatics::postgres_upsert_data", mockery::mock())
  mockery::stub(msgraph_scoped_update_bookings, "mark_vanished_events_canceled",
                function(con, rs, seen, scope_map, ...) { scope_seen <<- scope_map; 0L })
  mockery::stub(msgraph_scoped_update_bookings, "dplyr::tbl", function(...) stop("stop nach Absage"))
  cfg <- list(raw_schema = "raw", processed_schema = "processed",
              events_days_back = 50, events_days_forward = 365)
  expect_error(msgraph_scoped_update_bookings(con = NULL, del_token = "t", cfg = cfg),
               "stop nach Absage", fixed = TRUE)
  expect_equal(nrow(scope_seen), 0)
})
