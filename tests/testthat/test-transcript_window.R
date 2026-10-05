# Fenster des Transkript-Jobs: welche Calls nach ihrem Transkript gefragt werden.

calls_fixture <- function() {
  tibble::tibble(
    id              = 1:3,
    msgraph_call_id = c("SPAET", "FRISCH", "ALT"),
    call_start      = as.POSIXct(c("2026-09-22 09:00:00", "2026-10-01 09:00:00", "2026-09-10 09:00:00"), tz = "UTC"),
    created_at      = as.POSIXct(c("2026-10-02 09:24:14", "2026-10-02 01:25:00", "2026-09-11 01:25:00"), tz = "UTC")
  )
}

test_that("Ein spaet angekommener Call mit altem Termin bleibt im Fenster", {
  out <- filter_transcript_window_calls(calls_fixture(), as.Date("2026-09-28"))
  expect_true("SPAET" %in% out$msgraph_call_id)
})

test_that("Ein frischer Termin bleibt im Fenster, ein alter mit altem Eingang faellt raus", {
  out <- filter_transcript_window_calls(calls_fixture(), as.Date("2026-09-28"))
  expect_true("FRISCH" %in% out$msgraph_call_id)
  expect_false("ALT" %in% out$msgraph_call_id)
})
