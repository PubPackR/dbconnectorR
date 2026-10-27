# Transkript-Quelle: der Meeting-Besitzer steht vor den internen Teilnehmern.
# Spec: docs/specs/2026-10-27-transkript-ueber-meeting-besitzer.md
# Hinweis: mockery::stub() muss IM test_that-Block stehen (Scope), nicht in einem Helper.

oid_besitzer <- "22222222-aaaa-4bbb-8ccc-000000000002"
thread_id    <- "19:meeting_ABC@thread.v2"

test_that("meeting_owner_oid_from_online_meeting_id liest die oid aus der Online-Meeting-ID", {
  mid <- build_online_meeting_id(oid_besitzer, thread_id)
  expect_equal(meeting_owner_oid_from_online_meeting_id(mid), oid_besitzer)
  # Graph liefert die ID teils ohne Padding
  expect_equal(meeting_owner_oid_from_online_meeting_id(sub("=+$", "", mid)), oid_besitzer)
  # URL-sicheres Alphabet: die thread-id erzwingt '/' im Standard-base64
  mid_slash <- build_online_meeting_id(oid_besitzer, "19:meeting_???@thread.v2")
  expect_match(mid_slash, "/", fixed = TRUE)
  expect_equal(meeting_owner_oid_from_online_meeting_id(chartr("+/", "-_", mid_slash)), oid_besitzer)
  expect_true(is.na(meeting_owner_oid_from_online_meeting_id("REP1")))          # Bericht-ID, kein base64-Format
  expect_true(is.na(meeting_owner_oid_from_online_meeting_id("bm9wZQ==")))      # base64, aber nicht 1*<oid>*
  expect_true(is.na(meeting_owner_oid_from_online_meeting_id(NA_character_)))
  expect_true(is.na(meeting_owner_oid_from_online_meeting_id("")))
})

test_that("msgraph_scoped_update_transcripts fragt zuerst den Meeting-Besitzer, dann die Teilnehmer", {
  heute <- Sys.Date()
  mid_fremd <- build_online_meeting_id(oid_besitzer, thread_id)
  mid_ohne  <- build_online_meeting_id(oid_besitzer, "19:meeting_OHNE@thread.v2")
  calls <- tibble::tibble(
    id              = 1:3,
    msgraph_call_id = c("REP1", "REP2", "ALT"),
    msgraph_online_meeting_id = c(mid_fremd, mid_ohne, NA_character_),
    call_start      = as.POSIXct(rep(heute - 1, 3)),
    call_end        = as.POSIXct(rep(heute - 1, 3)),
    created_at      = as.POSIXct(rep(heute - 1, 3))
  )
  rec <- new.env(); rec$cands <- list()
  mockery::stub(msgraph_scoped_update_transcripts, "dplyr::tbl", function(con, from) {
    if (grepl("msgraph_calls", from)) calls
    else tibble::tibble(transcript_id = character(0), call_id = integer(0))
  })
  mockery::stub(msgraph_scoped_update_transcripts, "DBI::dbQuoteLiteral",
                function(con, x) paste0("'", x, "'"))
  # Interne Teilnehmer: beim fremden Link nur die Kalender-Person, beim zweiten
  # Call niemand Internes (Besitzer allein zaehlt), beim Altbestand die Kalender-Person
  mockery::stub(msgraph_scoped_update_transcripts, "DBI::dbGetQuery",
                function(con, sql) data.frame(graph_mid = c(mid_fremd, "ALT"),
                                              object_id = c("KALENDER", "KALENDER")))
  mockery::stub(msgraph_scoped_update_transcripts, "resolve_transcript_source",
                function(cands, mid, app_token) { rec$cands[[mid]] <- cands; NULL })

  suppressMessages(msgraph_scoped_update_transcripts(
    con = NULL, app_token = "t", cfg = list(transcripts_sliding_window_days = 7), dry_run = TRUE))

  expect_equal(rec$cands[[mid_fremd]], c(oid_besitzer, "KALENDER"))
  expect_equal(rec$cands[[mid_ohne]], oid_besitzer)       # frueher uebersprungen: kein interner Teilnehmer
  expect_equal(rec$cands[["ALT"]], "KALENDER")            # ID nicht lesbar: Teilnehmer wie bisher
})

test_that("Besitzer, der auch Teilnehmer ist, wird nur einmal gefragt", {
  heute <- Sys.Date()
  mid <- build_online_meeting_id(oid_besitzer, thread_id)
  calls <- tibble::tibble(id = 1L, msgraph_call_id = "REP1", msgraph_online_meeting_id = mid,
                          call_start = as.POSIXct(heute - 1), call_end = as.POSIXct(heute - 1),
                          created_at = as.POSIXct(heute - 1))
  rec <- new.env()
  mockery::stub(msgraph_scoped_update_transcripts, "dplyr::tbl", function(con, from) {
    if (grepl("msgraph_calls", from)) calls
    else tibble::tibble(transcript_id = character(0), call_id = integer(0))
  })
  mockery::stub(msgraph_scoped_update_transcripts, "DBI::dbQuoteLiteral",
                function(con, x) paste0("'", x, "'"))
  mockery::stub(msgraph_scoped_update_transcripts, "DBI::dbGetQuery",
                function(con, sql) data.frame(graph_mid = mid, object_id = c(toupper(oid_besitzer), "KALENDER")))
  mockery::stub(msgraph_scoped_update_transcripts, "resolve_transcript_source",
                function(cands, mid, app_token) { rec$cands <- cands; NULL })

  suppressMessages(msgraph_scoped_update_transcripts(
    con = NULL, app_token = "t", cfg = list(transcripts_sliding_window_days = 7), dry_run = TRUE))

  expect_equal(rec$cands, c(oid_besitzer, "KALENDER"))
})
