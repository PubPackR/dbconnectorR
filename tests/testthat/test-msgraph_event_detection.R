# Reine Hilfsfunktionen der Outlook-Erkennung, kein DB-Zugriff.

utc <- function(x) as.POSIXct(x, tz = "UTC")
start <- utc("2026-09-01 10:00:00")

# ---- detect_short_lived_events ----

test_that("detect_short_lived_events: nur abgesagt und unter der Grenze", {
  angelegt <- start
  expect_equal(
    detect_short_lived_events(
      is_canceled = c(TRUE, TRUE, TRUE, FALSE),
      canceled_at = start + 3600 * c(23.9, 24, 30, 1),
      created_at = angelegt
    ),
    c(TRUE, FALSE, FALSE, FALSE)
  )
})

test_that("detect_short_lived_events: ohne Absagezeitpunkt ist ein abgesagtes Event offen", {
  expect_equal(
    detect_short_lived_events(c(TRUE, FALSE), utc(c(NA, NA)), start),
    c(NA, FALSE)
  )
})

test_that("detect_short_lived_events: der Absagezeitpunkt kommt vom Aufrufer", {
  # Ein spaet verschwundener Termin ist nur kurzlebig, wenn der Aufrufer den
  # alten Aenderungsstempel uebergibt.
  expect_true(detect_short_lived_events(TRUE, start + 3600, start))
  expect_false(detect_short_lived_events(TRUE, start + 10 * 86400, start))
})

test_that("detect_short_lived_events: Grenze ist einstellbar", {
  expect_true(detect_short_lived_events(TRUE, start + 3600, start, max_hours = 2))
  expect_false(detect_short_lived_events(TRUE, start + 3600, start, max_hours = 1))
})

# ---- count_event_participants ----

test_that("count_event_participants: zaehlt je Event intern und extern ohne synthetische Gaeste", {
  ergebnis <- count_event_participants(tibble::tribble(
    ~event_id, ~email,
    1L,        "a@studyflix.de",
    1L,        "b@studyflix.de",
    1L,        "lead@kunde.de",
    1L,        "gast@external.guest",
    2L,        "x@external.msgraph",
    2L,        NA_character_
  ))

  expect_equal(as.data.frame(ergebnis), data.frame(
    event_id = c(1L, 2L), participant_count = c(4L, 2L),
    internal_count = c(2L, 0L), external_count = c(1L, 0L)
  ))
})

test_that("count_event_participants: leere Eingabe liefert keine Zeile", {
  ergebnis <- count_event_participants(tibble::tibble(event_id = integer(0), email = character(0)))
  expect_equal(nrow(ergebnis), 0)
  expect_named(ergebnis, c("event_id", "participant_count", "internal_count", "external_count"))
})

# ---- detect_duplicate_events ----

zeile <- function(event_id, email, minuten, no_show = FALSE) {
  tibble::tibble(event_id = as.integer(event_id), email_lower = email,
                 event_start = start + 60 * minuten, is_no_show_class = no_show)
}

test_that("detect_duplicate_events: der spaetere verliert, wenn nicht nur der erste No-Show ist", {
  expect_equal(
    detect_duplicate_events(dplyr::bind_rows(zeile(1, "a", 0), zeile(2, "a", 15))),
    2L
  )
  expect_equal(
    detect_duplicate_events(dplyr::bind_rows(zeile(1, "a", 0, TRUE), zeile(2, "a", 5, TRUE))),
    2L
  )
  expect_equal(
    detect_duplicate_events(dplyr::bind_rows(zeile(1, "a", 0), zeile(2, "a", 5, TRUE))),
    2L
  )
})

test_that("detect_duplicate_events: der No-Show verliert gegen den stattgefundenen Termin", {
  expect_equal(
    detect_duplicate_events(dplyr::bind_rows(zeile(1, "a", 0, TRUE), zeile(2, "a", 5))),
    1L
  )
})

test_that("detect_duplicate_events: andere Person oder groesserer Abstand ist kein Duplikat", {
  expect_equal(
    detect_duplicate_events(dplyr::bind_rows(zeile(1, "a", 0), zeile(2, "b", 5))),
    integer(0)
  )
  expect_equal(
    detect_duplicate_events(dplyr::bind_rows(zeile(1, "a", 0), zeile(2, "a", 16))),
    integer(0)
  )
})

test_that("detect_duplicate_events: fehlende Startzeit vergleicht nicht", {
  ohne_start <- zeile(2, "a", 0)
  ohne_start$event_start <- as.POSIXct(NA, tz = "UTC")
  expect_equal(detect_duplicate_events(dplyr::bind_rows(zeile(1, "a", 0), ohne_start)), integer(0))
})

test_that("detect_duplicate_events: Zeilen ohne E-Mail brechen nichts und zaehlen nicht", {
  eingabe <- dplyr::bind_rows(zeile(1, "a", 0), zeile(2, NA_character_, 1), zeile(3, NA_character_, 2))
  expect_equal(detect_duplicate_events(eingabe), integer(0))
  expect_equal(detect_duplicate_events(zeile(1, NA_character_, 0)), integer(0))
})

test_that("detect_duplicate_events: ein Event mit zwei Personen zaehlt einmal", {
  eingabe <- dplyr::bind_rows(zeile(1, "a", 0), zeile(1, "b", 0), zeile(2, "a", 5), zeile(2, "b", 5))
  expect_equal(detect_duplicate_events(eingabe), 2L)
})

test_that("detect_duplicate_events: weniger als zwei Zeilen liefern nichts", {
  expect_equal(detect_duplicate_events(zeile(1, "a", 0)), integer(0))
  expect_equal(detect_duplicate_events(zeile(1, "a", 0)[0, ]), integer(0))
})

# ---- detect_replaced_events ----

termin <- function(event_id, email, stunden, uid) {
  tibble::tibble(event_id = as.integer(event_id), email_lower = email,
                 event_start = start + 3600 * stunden, msgraph_ical_uid = uid)
}

leeres_paar <- data.frame(canceled_event_id = integer(0), replacement_event_id = integer(0))

test_that("detect_replaced_events: gleiche Person, andere ical_uid, unter zwei Tagen", {
  expect_equal(
    detect_replaced_events(termin(1, "a", 0, "U1"), termin(2, "a", 24, "U2")),
    data.frame(canceled_event_id = 1L, replacement_event_id = 2L)
  )
})

test_that("detect_replaced_events: der naechste Kandidat gewinnt, auch rueckwirkend", {
  expect_equal(
    detect_replaced_events(
      termin(1, "a", 0, "U1"),
      dplyr::bind_rows(termin(2, "a", 30, "U2"), termin(3, "a", -5, "U3"))
    ),
    data.frame(canceled_event_id = 1L, replacement_event_id = 3L)
  )
})

test_that("detect_replaced_events: gleiche ical_uid, andere Person oder zwei Tage passen nicht", {
  expect_equal(detect_replaced_events(termin(1, "a", 0, "U1"), termin(2, "a", 24, "U1")), leeres_paar)
  expect_equal(detect_replaced_events(termin(1, "a", 0, "U1"), termin(2, "b", 24, "U2")), leeres_paar)
  expect_equal(detect_replaced_events(termin(1, "a", 0, "U1"), termin(2, "a", 48, "U2")), leeres_paar)
})

test_that("detect_replaced_events: mehrere Personen ergeben ein Paar je abgesagtem Event", {
  ergebnis <- detect_replaced_events(
    dplyr::bind_rows(termin(1, "a", 0, "U1"), termin(1, "b", 0, "U1")),
    dplyr::bind_rows(termin(2, "a", 24, "U2"), termin(2, "b", 24, "U2"))
  )
  expect_equal(ergebnis, data.frame(canceled_event_id = 1L, replacement_event_id = 2L))
})

test_that("detect_replaced_events: fehlende Startzeit und leere Eingaben", {
  ohne_start <- termin(2, "a", 0, "U2")
  ohne_start$event_start <- as.POSIXct(NA, tz = "UTC")
  expect_equal(detect_replaced_events(termin(1, "a", 0, "U1"), ohne_start), leeres_paar)
  expect_equal(detect_replaced_events(termin(1, "a", 0, "U1")[0, ], termin(2, "a", 24, "U2")), leeres_paar)
  expect_equal(detect_replaced_events(termin(1, "a", 0, "U1"), termin(2, "a", 24, "U2")[0, ]), leeres_paar)
})

test_that("detect_replaced_events: das Ergebnis passt zu derive_calendar_state", {
  paare <- detect_replaced_events(termin(1, "a", 0, "U1"), termin(2, "a", 24, "U2"))
  events <- tibble::tibble(
    id = c(1L, 2L), msgraph_ical_uid = c("U1", "U2"), is_canceled = c(TRUE, FALSE),
    is_single_instance = TRUE, event_start = start + c(0, 86400),
    event_updated_at = start, vanished_at = utc(c(NA, NA))
  )

  ergebnis <- derive_calendar_state(events, replacements = paare)

  expect_equal(ergebnis$calendar_status, c("rescheduled", "active"))
  expect_equal(ergebnis$successor_event_id, c("2", NA))
})
