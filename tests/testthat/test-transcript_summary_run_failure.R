# Zusammenfassungs-Schritt: einzelne Fehler werden uebersprungen, ein Lauf ganz
# ohne gespeicherte Zusammenfassung bricht ab, damit FlowForce ihn meldet.

test_that("Ein Lauf ohne eine einzige gespeicherte Zusammenfassung bricht ab", {
  expect_error(stop_if_no_summary_saved(3, 0), "No transcript summary could be saved")
})

test_that("Ein Lauf mit mindestens einer Zusammenfassung oder ohne Transkripte laeuft durch", {
  expect_null(stop_if_no_summary_saved(3, 1))
  expect_null(stop_if_no_summary_saved(0, 0))
})

fake_transcripts <- function() {
  tibble::tibble(id = 11L, transcript_id = "t1", call_id = 1L, transcript_url = "u",
                 transcript_created_at = as.POSIXct("2026-11-11 08:00:00", tz = "UTC"),
                 transcript_content = "roh", transcript_content_anonymized = "anonym")
}

fake_calls <- function(...) {
  tibble::tibble(id = 1L, call_start = as.POSIXct("2026-11-11 08:00:00", tz = "UTC"))
}

test_that("Scheitert jede Zusammenfassung, scheitert der Schritt", {
  local_mocked_bindings(
    get_transcripts_without_summary = function(con) fake_transcripts(),
    summarize_ts = function(...) NA
  )
  local_mocked_bindings(tbl = fake_calls, .package = "dplyr")

  expect_error(
    suppressMessages(capture.output(
      generate_and_save_transcript_summaries(NULL, "openai/gpt-5.6-luna", "key", tempdir())
    )),
    "No transcript summary could be saved in this run \\(1 attempted\\)"
  )
})

test_that("Eine gespeicherte Zusammenfassung laesst den Schritt durchlaufen", {
  seen <- new.env()
  local_mocked_bindings(
    get_transcripts_without_summary = function(con) fake_transcripts(),
    summarize_ts = function(...) "### 11.11.2026 VC NV"
  )
  local_mocked_bindings(tbl = fake_calls, .package = "dplyr")
  local_mocked_bindings(dbExecute = function(con, sql, params) {
    seen$params <- params
    1L
  }, .package = "DBI")

  expect_no_error(suppressMessages(capture.output(
    generate_and_save_transcript_summaries(NULL, "openai/gpt-5.6-luna", "key", tempdir())
  )))
  expect_identical(seen$params[[2]], "t1")
})
