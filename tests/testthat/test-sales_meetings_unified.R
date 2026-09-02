mk_msgraph <- function() data.frame(
  call_event_mapping_id = c(10L, 11L, 12L),
  event_id             = c("e10", "e11", "e12"),
  event_date           = as.Date(c("2026-07-01", "2026-07-02", "2026-07-03")),
  event_start          = as.POSIXct(c("2026-07-01 09:00","2026-07-02 10:00","2026-07-03 11:00"), tz="UTC"),
  contact_id           = c(500L, 500L, 600L),
  lead_id              = c(10L, 20L, 30L),
  is_no_show           = c(FALSE, FALSE, TRUE),
  original_created_at  = as.POSIXct(c("2026-06-01","2026-06-02","2026-06-03"), tz="UTC"),
  excluded             = FALSE, is_short_lived_event = FALSE, is_responsible = TRUE,
  stringsAsFactors = FALSE
)
mk_crm <- function(rows) do.call(rbind, rows)
crm_row <- function(id, lead, date, tool, status, ext, rep = 500L, ptime = NA_character_,
                    type = "unbekannt") data.frame(
  crm_task_id=id, lead_id=lead, event_date=as.Date(date),
  precise_time=as.POSIXct(ptime, tz="UTC"), contact_id=rep,
  meeting_tool=tool, meeting_type=type, meeting_status=status, is_external_tool=ext,
  original_created_at=as.POSIXct("2026-06-15", tz="UTC"), stringsAsFactors=FALSE)

test_that("nicht-extern eindeutiger Match: CRM-No-Show ueberschreibt MSGraph", {
  # lead 10 / 2026-07-01 -> genau ein MSGraph-Termin, war FALSE
  res <- assemble_unified_meetings(mk_msgraph(),
           mk_crm(list(crm_row(1, 10L, "2026-07-01", "teams", "no_show", FALSE))))
  r <- res[res$meeting_key == "msgraph_10_10", ]
  expect_true(r$is_no_show)
  expect_equal(r$no_show_source, "crm_override")
  expect_equal(r$meeting_tool, "teams")
})

test_that("CRM show_up ueberschreibt MSGraph is_no_show=TRUE -> FALSE", {
  res <- assemble_unified_meetings(mk_msgraph(),
           mk_crm(list(crm_row(2, 30L, "2026-07-03", "teams", "show_up", FALSE))))
  r <- res[res$meeting_key == "msgraph_12_30", ]
  expect_false(r$is_no_show)
  expect_equal(r$no_show_source, "crm_override")
})

test_that("nicht-extern mehrdeutig (gleicher Rep, keine Zeit): CRM-Zeile wird verworfen", {
  ms <- mk_msgraph()
  ms <- rbind(ms, transform(ms[1,], call_event_mapping_id=99L, event_id="e99"))  # 2. Termin lead 10 / 2026-07-01, Rep 500
  res <- assemble_unified_meetings(ms,
           mk_crm(list(crm_row(3, 10L, "2026-07-01", "teams", "no_show", FALSE))))
  expect_false(any(res$no_show_source == "crm_override"))      # kein Override
  expect_false(any(res$meeting_key == "crm_3"))                # kein Netto-neu
})

test_that("Tiebreak: 2 Meetings (Lead, Tag) -> CRM matcht den gleichen Rep", {
  ms <- mk_msgraph()
  ms <- rbind(ms, transform(ms[1,], call_event_mapping_id=98L, event_id="e98", contact_id=600L))  # anderer Rep
  res <- assemble_unified_meetings(ms,
           mk_crm(list(crm_row(20, 10L, "2026-07-01", "teams", "no_show", FALSE, rep=500L))))
  expect_equal(res[res$meeting_key=="msgraph_10_10", ]$no_show_source, "crm_override")  # Rep 500
  expect_equal(res[res$meeting_key=="msgraph_98_10", ]$no_show_source, "msgraph")       # Rep 600 unberuehrt
})

test_that("Tiebreak: gleicher Rep, 2 Zeiten -> naechste event_start gewinnt", {
  ms <- mk_msgraph()
  ms <- rbind(ms, transform(ms[1,], call_event_mapping_id=97L, event_id="e97",
                            event_start=as.POSIXct("2026-07-01 15:00", tz="UTC")))  # gleicher Rep, spaeter
  res <- assemble_unified_meetings(ms,
           mk_crm(list(crm_row(21, 10L, "2026-07-01", "teams", "no_show", FALSE,
                               rep=500L, ptime="2026-07-01 14:30"))))               # naeher an 15:00
  expect_equal(res[res$meeting_key=="msgraph_97_10", ]$no_show_source, "crm_override")
  expect_equal(res[res$meeting_key=="msgraph_10_10", ]$no_show_source, "msgraph")
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
  expect_equal(res[res$meeting_key == "msgraph_10_10", ]$no_show_source, "msgraph")  # nicht ueberschrieben
  expect_equal(res[res$meeting_key == "crm_5", ]$source, "crm_task")                 # eigener Termin
})

test_that("CRM unbekannt/storniert (extern): excluded=TRUE, kein Override", {
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
  # lead 20 / 2026-07-02 -> genau ein MSGraph-Termin
  res <- assemble_unified_meetings(mk_msgraph(),
           mk_crm(list(crm_row(8, 20L, "2026-07-02", "teams", "storniert", FALSE))))
  r <- res[res$meeting_key == "msgraph_11_20", ]
  expect_true(r$excluded)
  expect_equal(r$no_show_source, "crm_override")
})

test_that("Override + unbekannt: MSGraph is_no_show/excluded bleiben unveraendert", {
  # lead 30 / 2026-07-03 -> genau ein MSGraph-Termin, is_no_show=TRUE, excluded=FALSE
  res <- assemble_unified_meetings(mk_msgraph(),
           mk_crm(list(crm_row(9, 30L, "2026-07-03", "teams", "unbekannt", FALSE))))
  r <- res[res$meeting_key == "msgraph_12_30", ]
  expect_true(r$is_no_show)
  expect_false(r$excluded)
  expect_equal(r$meeting_tool, "teams")
})

test_that("Platzhalter-Guard: CRM-Zeile mit lead_id=NA matcht keine NA-lead-MSGraph-Zeile", {
  # msgraph-Fixture mit einer Platzhalter-Zeile (lead_id NA) am selben Datum wie die CRM-Zeile
  ms_na <- mk_msgraph()
  ms_na <- rbind(ms_na, transform(ms_na[1, ],
                                   call_event_mapping_id = 13L, event_id = "e13",
                                   lead_id = NA_integer_, event_date = as.Date("2026-08-04")))
  cm <- crm_row(10, NA_integer_, "2026-08-04", "teams", "no_show", FALSE)
  res <- assemble_unified_meetings(ms_na, mk_crm(list(cm)))

  crm_r <- res[res$meeting_key == "crm_10", ]
  expect_equal(crm_r$source, "crm_task")
  expect_equal(crm_r$no_show_source, "crm_only")

  ms_r <- res[res$meeting_key == "msgraph_13_NA", ]
  expect_equal(ms_r$no_show_source, "msgraph")  # Platzhalter nicht ueberschrieben
})

test_that("meeting_type wandert auf die netto-neue CRM-Zeile", {
  res <- assemble_unified_meetings(mk_msgraph(),
           mk_crm(list(crm_row(40, 99L, "2026-07-09", "webex", "show_up", TRUE, type = "nv"))))
  expect_equal(res[res$meeting_key == "crm_40", ]$meeting_type, "nv")
})

test_that("meeting_type wird beim Override auf die MSGraph-Zeile geschrieben", {
  res <- assemble_unified_meetings(mk_msgraph(),
           mk_crm(list(crm_row(41, 10L, "2026-07-01", "teams", "no_show", FALSE, type = "uc"))))
  r <- res[res$meeting_key == "msgraph_10_10", ]
  expect_equal(r$meeting_type, "uc")
  expect_equal(r$no_show_source, "crm_override")
})

test_that("MSGraph-Zeilen ohne CRM-Pendant behalten meeting_type NA", {
  leer <- crm_row(0, 1L, "2026-07-01", "teams", "unbekannt", FALSE)[0, ]
  res  <- assemble_unified_meetings(mk_msgraph(), leer)
  expect_true(all(is.na(res$meeting_type)))
})

# --- Platzhalter-Fallback: MSGraph-Meeting ohne gemappten Lead ------------------
ms_platzhalter <- function(rep = 500L, start = "2026-08-10 09:00") {
  ms <- mk_msgraph()
  rbind(ms, transform(ms[1, ], call_event_mapping_id = 50L, event_id = "e50",
                      lead_id = NA_integer_, contact_id = rep,
                      event_date = as.Date("2026-08-10"),
                      event_start = as.POSIXct(start, tz = "UTC")))
}

test_that("eindeutige Platzhalter-Zeile desselben Reps wird gematcht statt verdoppelt", {
  res <- assemble_unified_meetings(ms_platzhalter(),
           mk_crm(list(crm_row(60, 42L, "2026-08-10", "teams", "no_show", FALSE, rep = 500L))))
  expect_false(any(res$meeting_key == "crm_60"))                       # keine zweite Zeile
  r <- res[res$meeting_key == "msgraph_50_NA", ]
  expect_true(r$is_no_show)
  expect_equal(r$no_show_source, "crm_override")
})

test_that("Platzhalter eines anderen Reps wird nicht gematcht", {
  res <- assemble_unified_meetings(ms_platzhalter(rep = 600L),
           mk_crm(list(crm_row(61, 42L, "2026-08-10", "teams", "no_show", FALSE, rep = 500L))))
  expect_equal(res[res$meeting_key == "crm_61", ]$source, "crm_task")   # netto-neu
  expect_equal(res[res$meeting_key == "msgraph_50_NA", ]$no_show_source, "msgraph")
})

test_that("mehrere Platzhalter: naechste event_start zur precise_time gewinnt", {
  ms <- ms_platzhalter(start = "2026-08-10 09:00")
  ms <- rbind(ms, transform(ms[nrow(ms), ], call_event_mapping_id = 51L, event_id = "e51",
                            event_start = as.POSIXct("2026-08-10 15:00", tz = "UTC")))
  res <- assemble_unified_meetings(ms,
           mk_crm(list(crm_row(62, 42L, "2026-08-10", "teams", "no_show", FALSE,
                               rep = 500L, ptime = "2026-08-10 14:30"))))
  expect_equal(res[res$meeting_key == "msgraph_51_NA", ]$no_show_source, "crm_override")
  expect_equal(res[res$meeting_key == "msgraph_50_NA", ]$no_show_source, "msgraph")
  expect_false(any(res$meeting_key == "crm_62"))
})

test_that("mehrere Platzhalter ohne precise_time: CRM-Zeile bleibt netto-neu statt zu verschwinden", {
  ms <- ms_platzhalter()
  ms <- rbind(ms, transform(ms[nrow(ms), ], call_event_mapping_id = 51L, event_id = "e51"))
  res <- assemble_unified_meetings(ms,
           mk_crm(list(crm_row(63, 42L, "2026-08-10", "teams", "no_show", FALSE, rep = 500L))))
  expect_equal(res[res$meeting_key == "crm_63", ]$source, "crm_task")
})

test_that("ein Platzhalter nimmt hoechstens einen CRM-Termin auf", {
  # Zwei CRM-Termine desselben Reps am selben Tag, verschiedene Leads, aber nur
  # eine Platzhalter-Zeile. Ohne Buchfuehrung ueberschreiben sie einander und
  # keiner wird netto-neu -> aus zwei Meetings wird eines.
  res <- assemble_unified_meetings(ms_platzhalter(),
           mk_crm(list(
             crm_row(70, 42L, "2026-08-10", "teams", "no_show", FALSE, rep = 500L),
             crm_row(71, 43L, "2026-08-10", "teams", "no_show", FALSE, rep = 500L))))
  expect_equal(res[res$meeting_key == "msgraph_50_NA", ]$no_show_source, "crm_override")
  expect_equal(res[res$meeting_key == "crm_71", ]$source, "crm_task")   # zweiter bleibt erhalten
  expect_equal(sum(res$event_date == as.Date("2026-08-10")), 2L)        # zwei Meetings, zwei Zeilen
})

test_that("Organisator wird uebernommen und je Herkunft unterscheidbar gehalten", {
  ms <- mk_msgraph()
  # Meeting 10 hat einen Organisator (SDR-Fall: 700 legt fuer Rep 500),
  # Meeting 11 keine klassifizierte Organisator-Zeile.
  ms$organizer_contact_id <- c("700", NA_character_, "600")
  res <- assemble_unified_meetings(ms, mk_crm(list(
    # Externes Tool -> netto-neue CRM-Zeile, kann keinen Organisator tragen.
    crm_row(80, 99L, "2026-07-05", "zoom", "show_up", TRUE))))

  expect_equal(res[res$meeting_key == "msgraph_10_10", ]$organizer_contact_id, "700")
  expect_equal(res[res$meeting_key == "msgraph_10_10", ]$organizer_source, "msgraph")
  expect_true(is.na(res[res$meeting_key == "msgraph_11_20", ]$organizer_contact_id))
  expect_equal(res[res$meeting_key == "msgraph_11_20", ]$organizer_source, "unbekannt")
  expect_true(is.na(res[res$meeting_key == "crm_80", ]$organizer_contact_id))
  expect_equal(res[res$meeting_key == "crm_80", ]$organizer_source, "crm_task")
})

test_that("fehlende Organisator-Spalte bricht den Aufbau nicht", {
  # Ein Caller ohne die neue Spalte bekommt "unbekannt", nicht einen Fehler.
  res <- assemble_unified_meetings(mk_msgraph(), mk_crm(list(
    crm_row(81, 10L, "2026-07-01", "teams", "show_up", FALSE))))
  expect_true(all(res$organizer_source == "unbekannt"))
  expect_true(all(is.na(res$organizer_contact_id)))
})
