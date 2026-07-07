# Tests fuer identify_real_no_show_reschedules(): ein Reschedule-Ausschluss
# (verschoben / rescheduled_without_meeting_id) darf einen ECHTEN No-Show nicht
# entfernen. Echt = externer Lead eingeladen UND interner Call am Slot.

slot <- as.POSIXct("2026-06-30 15:00:00", tz = "UTC")

# event 1: externer Lead + Call +1 Min + Absage nach Slot -> echter No-Show (KEEP)
# event 2: nur intern + Call am Slot                       -> Fehl-Tag intern (raus)
# event 3: externer Lead + Call 2h vor Slot                -> kein No-Show am Slot (raus)
# event 4: externer Lead + gar kein Call                   -> kein Beleg am Slot (raus)
# (Absage nach Slot fuer alle, damit 2/3/4 nur an ihrer eigenen Bedingung scheitern)
make_call_times <- function() {
  data.frame(
    event_id         = c(1L, 2L, 3L, 4L),
    call_start       = c(slot + 60, slot + 30, slot - 2 * 3600, as.POSIXct(NA)),
    event_start      = c(slot, slot, slot, slot),
    event_updated_at = c(slot + 600, slot + 600, slot + 600, slot + 600)
  )
}

make_participants <- function() {
  data.frame(
    event_id = c(1L, 1L, 2L, 2L, 3L, 3L, 4L, 4L),
    email = c(
      "sdr@studyflix.de", "lead@kunde.de",      # 1: extern dabei
      "a@studyflix.de",   "b@studyflix.de",     # 2: rein intern
      "sdr@studyflix.de", "lead3@kunde.de",     # 3: extern dabei
      "sdr@studyflix.de", "lead4@kunde.de"      # 4: extern dabei
    ),
    stringsAsFactors = FALSE
  )
}

test_that("nur externer-Lead-plus-Call-am-Slot bleibt als echter No-Show erhalten", {
  keep <- identify_real_no_show_reschedules(
    reschedule_event_ids     = c(1L, 2L, 3L, 4L),
    event_call_times         = make_call_times(),
    event_participant_emails = make_participants()
  )
  expect_equal(sort(keep), 1L)
})

test_that("internes Meeting (kein externer Lead) wird NICHT als No-Show behalten", {
  keep <- identify_real_no_show_reschedules(
    reschedule_event_ids     = c(2L),
    event_call_times         = make_call_times(),
    event_participant_emails = make_participants()
  )
  expect_length(keep, 0)
})

test_that("Call ausserhalb des Slots zaehlt nicht (Fehl-Mapping / vorher verschoben)", {
  keep <- identify_real_no_show_reschedules(
    reschedule_event_ids     = c(3L, 4L),
    event_call_times         = make_call_times(),
    event_participant_emails = make_participants()
  )
  expect_length(keep, 0)
})

test_that("synthetische Gast-Mails zaehlen nicht als externer Lead", {
  parts <- data.frame(
    event_id = c(5L, 5L),
    email = c("sdr@studyflix.de", "guest@external.guest"),
    stringsAsFactors = FALSE
  )
  ct <- data.frame(event_id = 5L, call_start = slot + 60, event_start = slot,
                   event_updated_at = slot + 600)
  keep <- identify_real_no_show_reschedules(
    reschedule_event_ids     = c(5L),
    event_call_times         = ct,
    event_participant_emails = parts
  )
  expect_length(keep, 0)
})

test_that("vor dem Slot storniert (event_updated_at <= event_start) zaehlt nicht", {
  # Call am Slot + externer Lead, aber Absage 1h VOR dem Slot -> proaktive
  # Verschiebung, kein No-Show. Call ist ueber geteilte Meeting-ID fehl-gemappt.
  ct <- data.frame(event_id = 7L, call_start = slot + 60, event_start = slot,
                   event_updated_at = slot - 3600)
  parts <- data.frame(event_id = c(7L, 7L),
                      email = c("sdr@studyflix.de", "lead@kunde.de"),
                      stringsAsFactors = FALSE)
  keep <- identify_real_no_show_reschedules(c(7L), ct, parts)
  expect_length(keep, 0)
})

test_that("nur Events aus der Reschedule-Menge koennen ueberhaupt zurueckkommen", {
  # event 9 qualifiziert (extern + Call am Slot), ist aber nicht in der Menge
  ct <- data.frame(event_id = 9L, call_start = slot, event_start = slot,
                   event_updated_at = slot + 600)
  parts <- data.frame(event_id = c(9L, 9L),
                      email = c("sdr@studyflix.de", "lead@kunde.de"),
                      stringsAsFactors = FALSE)
  keep <- identify_real_no_show_reschedules(
    reschedule_event_ids     = c(1L, 2L),
    event_call_times         = ct,
    event_participant_emails = parts
  )
  expect_length(keep, 0)
})

test_that("leere Reschedule-Menge liefert leeren Vektor", {
  keep <- identify_real_no_show_reschedules(
    reschedule_event_ids     = integer(0),
    event_call_times         = make_call_times(),
    event_participant_emails = make_participants()
  )
  expect_length(keep, 0)
})

test_that("Slot-Fenster ist konfigurierbar", {
  ct <- data.frame(event_id = 1L, call_start = slot + 40 * 60, event_start = slot,  # +40 Min
                   event_updated_at = slot + 3600)
  parts <- data.frame(event_id = c(1L, 1L),
                      email = c("sdr@studyflix.de", "lead@kunde.de"),
                      stringsAsFactors = FALSE)
  # Default 30 Min -> raus
  expect_length(
    identify_real_no_show_reschedules(c(1L), ct, parts), 0
  )
  # 60 Min -> drin
  expect_equal(
    identify_real_no_show_reschedules(c(1L), ct, parts, slot_window_minutes = 60), 1L
  )
})
