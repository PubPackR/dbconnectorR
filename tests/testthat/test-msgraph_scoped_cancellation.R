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
  book <- scoped_cancellation_rows_sql("raw", "tmp_s", "tmp_c", "booking")
  expect_match(book, "e.msgraph_ical_uid LIKE 'booking:%'", fixed = TRUE)
  expect_false(grepl("NOT LIKE", book, fixed = TRUE))
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

test_that("msgraph_scoped_update_events: nur geladene Kalender kommen in den Absage-Scope", {
  cals <- list(status = 200, value = list(
    list(id = "C1", owner = list(address = "Rep.A@studyflix.de")),
    list(id = "C2", owner = list(address = "rep.b@studyflix.de"))))
  ev <- list(iCalUId = "U1", type = "singleInstance", subject = "Termin",
             start = list(dateTime = "2026-10-01T10:00:00.0000000"),
             end = list(dateTime = "2026-10-01T10:30:00.0000000"),
             organizer = list(emailAddress = list(address = "rep.a@studyflix.de", name = "Rep A")),
             attendees = list())
  fake_collect <- function(url, token, query = NULL) {
    if (grepl("/calendarView", url)) {
      if (grepl("C1", url)) list(status = 200, value = list(ev)) else list(status = 403, value = list())
    } else cals
  }
  scope_seen <- NULL
  mockery::stub(msgraph_scoped_update_events, "graph_collect", fake_collect)
  mockery::stub(msgraph_scoped_update_events, "dsgvo_suppress_participants", function(p, con, pepper) p)
  mockery::stub(msgraph_scoped_update_events, "Billomatics::postgres_upsert_data", mockery::mock())
  mockery::stub(msgraph_scoped_update_events, "mark_join_url_checked", function(...) 0L)
  mockery::stub(msgraph_scoped_update_events, "mark_vanished_events_canceled",
                function(con, rs, seen, scope_map, ...) { scope_seen <<- scope_map; 0L })
  mockery::stub(msgraph_scoped_update_events, "dplyr::tbl", function(...) stop("stop nach Absage"))
  cfg <- list(service_account_upn = "sa@studyflix.de", raw_schema = "raw",
              processed_schema = "processed", events_days_back = 50, events_days_forward = 365)
  try(msgraph_scoped_update_events(con = NULL, del_token = "t", cfg = cfg), silent = TRUE)
  expect_equal(scope_seen$email, "rep.a@studyflix.de")
})
