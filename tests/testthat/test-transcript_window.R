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

test_that("msgraph_scoped_update_transcripts fragt einen spaet angekommenen Call an", {
  heute <- Sys.Date()
  calls <- tibble::tibble(
    id              = 1:2,
    msgraph_call_id = c("SPAET", "ALT"),
    call_start      = as.POSIXct(c(heute - 13, heute - 30)),
    created_at      = as.POSIXct(c(heute - 3,  heute - 29))
  )
  rec <- new.env(); rec$mids <- character(0)
  mockery::stub(msgraph_scoped_update_transcripts, "dplyr::tbl", function(con, from) {
    if (grepl("msgraph_calls", from)) calls
    else tibble::tibble(transcript_id = character(0), call_id = integer(0))
  })
  mockery::stub(msgraph_scoped_update_transcripts, "DBI::dbQuoteLiteral",
                function(con, x) paste0("'", x, "'"))
  mockery::stub(msgraph_scoped_update_transcripts, "DBI::dbGetQuery",
                function(con, sql) data.frame(msgraph_call_id = calls$msgraph_call_id, object_id = "ORG"))
  mockery::stub(msgraph_scoped_update_transcripts, "resolve_transcript_source",
                function(cands, mid, app_token) { rec$mids <- c(rec$mids, mid); NULL })

  msgraph_scoped_update_transcripts(con = NULL, app_token = "t",
                                    cfg = list(transcripts_sliding_window_days = 7), dry_run = TRUE)
  expect_equal(rec$mids, "SPAET")
})
