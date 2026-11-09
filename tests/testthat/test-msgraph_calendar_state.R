# Kalenderzustand, Aenderungszeitpunkt und Nachfolger je Event
# (Ticket 1220117253528426, Spec docs/specs/2026-11-09-vanished-at-und-kalenderzustand.md)

ts <- function(x) as.POSIXct(x, tz = "UTC")

events_of <- function(...) {
  x <- tibble::tribble(~id, ~msgraph_ical_uid, ~is_canceled, ~is_single_instance,
                       ~event_start, ~event_updated_at, ~vanished_at, ...)
  x$event_start <- ts(x$event_start)
  x$event_updated_at <- ts(x$event_updated_at)
  x$vanished_at <- ts(x$vanished_at)
  x
}

state_of <- function(result, id) result[result$event_id == id, ]

test_that("aktiver Einzeltermin ohne neuere Zeile ist active und hat keinen Nachfolger", {
  res <- derive_calendar_state(events_of(
    1, "U1", FALSE, TRUE, "2026-10-05 10:00:00", "2026-10-01 08:00:00", NA_character_))
  expect_equal(res$calendar_status, "active")
  expect_true(is.na(res$successor_event_id))
})

test_that("Graph-Absage ohne Nachfolger ist cancelled", {
  res <- derive_calendar_state(events_of(
    1, "U1", TRUE, TRUE, "2026-10-05 10:00:00", "2026-10-01 08:00:00", NA_character_))
  expect_equal(res$calendar_status, "cancelled")
})

test_that("verschwunden ohne Nachfolger ist vanished", {
  res <- derive_calendar_state(events_of(
    1, "U1", TRUE, TRUE, "2026-10-05 10:00:00", "2026-10-01 08:00:00", "2026-10-03 02:50:00"))
  expect_equal(res$calendar_status, "vanished")
})

test_that("verschwundene Zeile mit aktiver Zeile derselben ical_uid ist rescheduled", {
  res <- derive_calendar_state(events_of(
    1, "U1", TRUE,  TRUE, "2026-10-05 10:00:00", "2026-10-01 08:00:00", "2026-10-03 02:50:00",
    2, "U1", FALSE, TRUE, "2026-10-06 14:00:00", "2026-10-02 09:00:00", NA_character_))
  expect_equal(state_of(res, "1")$calendar_status, "rescheduled")
  expect_equal(state_of(res, "1")$successor_event_id, "2")
  expect_equal(state_of(res, "2")$calendar_status, "active")
  expect_true(is.na(state_of(res, "2")$successor_event_id))
})

test_that("Q2: bei mehreren aktiven Zeilen zeigen alle veralteten auf die neueste", {
  res <- derive_calendar_state(events_of(
    1, "U1", FALSE, TRUE, "2026-10-05 10:00:00", "2026-10-01 08:00:00", NA_character_,
    2, "U1", FALSE, TRUE, "2026-10-06 14:00:00", "2026-10-05 08:00:00", NA_character_,
    3, "U1", FALSE, TRUE, "2026-10-07 09:00:00", "2026-10-07 08:00:00", NA_character_))
  expect_equal(res$calendar_status, c("stale_copy", "stale_copy", "active"))
  expect_equal(state_of(res, "1")$successor_event_id, "3")
  expect_equal(state_of(res, "2")$successor_event_id, "3")
  expect_true(is.na(state_of(res, "3")$successor_event_id))
})

test_that("Q2: bei gleichem Aenderungszeitpunkt gewinnt der spaetere Start", {
  res <- derive_calendar_state(events_of(
    1, "U1", TRUE,  TRUE, "2026-10-04 10:00:00", "2026-10-01 08:00:00", "2026-10-03 02:50:00",
    2, "U1", FALSE, TRUE, "2026-10-05 10:00:00", "2026-10-02 08:00:00", NA_character_,
    3, "U1", FALSE, TRUE, "2026-10-06 10:00:00", "2026-10-02 08:00:00", NA_character_))
  expect_equal(state_of(res, "1")$successor_event_id, "3")
})

test_that("Q1: Ersatz-Paar macht aus einer Absage ohne aktive Zeile rescheduled", {
  res <- derive_calendar_state(
    events_of(
      1, "U1", TRUE,  TRUE, "2026-10-05 10:00:00", "2026-10-01 08:00:00", "2026-10-03 02:50:00",
      9, "U9", FALSE, TRUE, "2026-10-06 10:00:00", "2026-10-02 08:00:00", NA_character_),
    replacements = tibble::tibble(canceled_event_id = 1, replacement_event_id = 9))
  expect_equal(state_of(res, "1")$calendar_status, "rescheduled")
  expect_equal(state_of(res, "1")$successor_event_id, "9")
})

test_that("Q2: Ersatz-Paar zaehlt nicht, wenn es eine aktive Zeile derselben ical_uid gibt", {
  res <- derive_calendar_state(
    events_of(
      1, "U1", TRUE,  TRUE, "2026-10-05 10:00:00", "2026-10-01 08:00:00", NA_character_,
      2, "U1", FALSE, TRUE, "2026-10-06 10:00:00", "2026-10-02 08:00:00", NA_character_,
      9, "U9", FALSE, TRUE, "2026-10-06 11:00:00", "2026-10-02 08:00:00", NA_character_),
    replacements = tibble::tibble(canceled_event_id = 1, replacement_event_id = 9))
  expect_equal(state_of(res, "1")$successor_event_id, "2")
})

test_that("Serien-Vorkommen werden nie zur stale_copy", {
  res <- derive_calendar_state(events_of(
    1, "U1", FALSE, FALSE, "2026-10-05 10:00:00", "2026-10-01 08:00:00", NA_character_,
    2, "U1", FALSE, FALSE, "2026-10-06 10:00:00", "2026-10-02 08:00:00", NA_character_))
  expect_equal(res$calendar_status, c("active", "active"))
  expect_true(all(is.na(res$successor_event_id)))
})

test_that("spaetere abgesagte Zeile macht aktive Zeile zur stale_copy ohne Nachfolger", {
  res <- derive_calendar_state(events_of(
    1, "U1", FALSE, TRUE, "2026-10-05 10:00:00", "2026-10-01 08:00:00", NA_character_,
    2, "U1", TRUE,  TRUE, "2026-10-06 10:00:00", "2026-10-04 08:00:00", NA_character_))
  expect_equal(state_of(res, "1")$calendar_status, "stale_copy")
  expect_true(is.na(state_of(res, "1")$successor_event_id))
})

test_that("Q5: Aenderungszeitpunkt ist vanished_at, sonst event_updated_at", {
  res <- derive_calendar_state(events_of(
    1, "U1", TRUE,  TRUE, "2026-10-05 10:00:00", "2026-10-01 08:00:00", "2026-10-03 02:50:00",
    2, "U2", TRUE,  TRUE, "2026-10-05 10:00:00", "2026-10-02 08:00:00", NA_character_,
    3, "U3", FALSE, TRUE, "2026-10-05 10:00:00", "2026-10-04 08:00:00", NA_character_))
  expect_equal(res$calendar_changed_at_utc,
               ts(c("2026-10-03 02:50:00", "2026-10-02 08:00:00", "2026-10-04 08:00:00")))
})

test_that("Q9: Bookings ohne vanished_at haben keinen Aenderungszeitpunkt", {
  res <- derive_calendar_state(events_of(
    1, "booking:A", TRUE,  TRUE, "2026-10-20 10:00:00", "2026-10-20 10:00:00", NA_character_,
    2, "booking:B", TRUE,  TRUE, "2026-10-21 10:00:00", "2026-10-21 10:00:00", "2026-10-03 02:50:00",
    3, "booking:C", FALSE, TRUE, "2026-10-22 10:00:00", "2026-10-22 10:00:00", NA_character_,
    4, "U4",        TRUE,  TRUE, "2026-10-23 10:00:00", "2026-10-02 08:00:00", NA_character_))
  expect_true(is.na(state_of(res, "1")$calendar_changed_at_utc))
  expect_equal(state_of(res, "2")$calendar_changed_at_utc, ts("2026-10-03 02:50:00"))
  expect_true(is.na(state_of(res, "3")$calendar_changed_at_utc))
  expect_equal(state_of(res, "4")$calendar_changed_at_utc, ts("2026-10-02 08:00:00"))
})

test_that("Spalte vanished_at ohne einen einzigen Wert bricht nichts", {
  ev <- events_of(1, "U1", TRUE, TRUE, "2026-10-05 10:00:00", "2026-10-01 08:00:00", NA_character_)
  ev$vanished_at <- NA
  res <- derive_calendar_state(ev)
  expect_equal(res$calendar_status, "cancelled")
  expect_s3_class(res$calendar_changed_at_utc, "POSIXct")
})

test_that("leere Eingabe liefert leeres Ergebnis mit allen Spalten", {
  res <- derive_calendar_state(events_of())
  expect_equal(nrow(res), 0)
  expect_named(res, c("event_id", "calendar_status", "calendar_changed_at_utc", "successor_event_id"))
})
