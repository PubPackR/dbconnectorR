mk_msgraph <- function() data.frame(
  call_event_mapping_id = c(10L, 11L, 12L),
  event_id             = c("e10", "e11", "e12"),
  event_date           = as.Date(c("2026-07-01", "2026-07-02", "2026-07-03")),
  contact_id           = c(500L, 500L, 600L),
  lead_id              = c(10L, 20L, 30L),
  is_no_show           = c(FALSE, FALSE, TRUE),
  original_created_at  = as.POSIXct(c("2026-06-01","2026-06-02","2026-06-03"), tz="UTC"),
  excluded             = FALSE, is_short_lived_event = FALSE, is_responsible = TRUE,
  stringsAsFactors = FALSE
)
mk_crm <- function(rows) do.call(rbind, rows)
crm_row <- function(id, lead, date, tool, status, ext) data.frame(
  crm_task_id=id, lead_id=lead, event_date=as.Date(date), contact_id=500L,
  meeting_tool=tool, meeting_status=status, is_external_tool=ext,
  original_created_at=as.POSIXct("2026-06-15", tz="UTC"), stringsAsFactors=FALSE)

test_that("nicht-extern eindeutiger Match: CRM-No-Show ueberschreibt MSGraph", {
  # lead 10 / 2026-07-01 -> genau ein MSGraph-Termin (key 10), war FALSE
  res <- assemble_unified_meetings(mk_msgraph(),
           mk_crm(list(crm_row(1, 10L, "2026-07-01", "teams", "no_show", FALSE))))
  r <- res[res$meeting_key == "10", ]
  expect_true(r$is_no_show)
  expect_equal(r$no_show_source, "crm_override")
  expect_equal(r$meeting_tool, "teams")
})

test_that("CRM show_up ueberschreibt MSGraph is_no_show=TRUE -> FALSE", {
  res <- assemble_unified_meetings(mk_msgraph(),
           mk_crm(list(crm_row(2, 30L, "2026-07-03", "teams", "show_up", FALSE))))
  r <- res[res$meeting_key == "12", ]
  expect_false(r$is_no_show)
  expect_equal(r$no_show_source, "crm_override")
})

test_that("nicht-extern mehrdeutig: CRM-Zeile wird verworfen", {
  ms <- mk_msgraph()
  ms <- rbind(ms, transform(ms[1,], call_event_mapping_id=99L, event_id="e99"))  # 2. Termin lead 10 / 2026-07-01
  res <- assemble_unified_meetings(ms,
           mk_crm(list(crm_row(3, 10L, "2026-07-01", "teams", "no_show", FALSE))))
  expect_false(any(res$no_show_source == "crm_override"))      # kein Override
  expect_false(any(res$meeting_key == "crm_3"))                # kein Netto-neu
})

test_that("nicht-extern kein Match: CRM als Netto-neu (crm_only)", {
  res <- assemble_unified_meetings(mk_msgraph(),
           mk_crm(list(crm_row(4, 77L, "2026-08-01", "teams", "no_show", FALSE))))
  r <- res[res$meeting_key == "crm_4", ]
  expect_equal(r$source, "crm_task")
  expect_true(r$is_no_show)
  expect_equal(r$no_show_source, "crm_only")
})

test_that("externes Tool mit gleicher-Tag-MSGraph-Zeile: immer Netto-neu, MSGraph unveraendert", {
  res <- assemble_unified_meetings(mk_msgraph(),
           mk_crm(list(crm_row(5, 10L, "2026-07-01", "webex", "no_show", TRUE))))
  expect_equal(res[res$meeting_key == "10", ]$no_show_source, "msgraph")  # nicht ueberschrieben
  expect_equal(res[res$meeting_key == "crm_5", ]$source, "crm_task")      # eigener Termin
})

test_that("CRM unbekannt/storniert: excluded=TRUE, kein Override", {
  res <- assemble_unified_meetings(mk_msgraph(),
           mk_crm(list(crm_row(6, 77L, "2026-08-02", "webex", "unbekannt", TRUE))))
  r <- res[res$meeting_key == "crm_6", ]
  expect_true(r$excluded)
  expect_true(is.na(r$is_no_show))
})

test_that("meeting_key eindeutig (keine Doppelzaehlung)", {
  res <- assemble_unified_meetings(mk_msgraph(),
           mk_crm(list(crm_row(7, 77L, "2026-08-03", "zoom", "show_up", TRUE))))
  expect_equal(anyDuplicated(res$meeting_key), 0L)
})

test_that("Override + storniert: MSGraph-Termin wird excluded", {
  # lead 20 / 2026-07-02 -> genau ein MSGraph-Termin (key 11)
  res <- assemble_unified_meetings(mk_msgraph(),
           mk_crm(list(crm_row(8, 20L, "2026-07-02", "teams", "storniert", FALSE))))
  r <- res[res$meeting_key == "11", ]
  expect_true(r$excluded)
  expect_equal(r$no_show_source, "crm_override")
})

test_that("Override + unbekannt: MSGraph is_no_show/excluded bleiben unveraendert", {
  # lead 30 / 2026-07-03 -> genau ein MSGraph-Termin (key 12), is_no_show=TRUE, excluded=FALSE
  res <- assemble_unified_meetings(mk_msgraph(),
           mk_crm(list(crm_row(9, 30L, "2026-07-03", "teams", "unbekannt", FALSE))))
  r <- res[res$meeting_key == "12", ]
  expect_true(r$is_no_show)
  expect_false(r$excluded)
  expect_equal(r$meeting_tool, "teams")
})
