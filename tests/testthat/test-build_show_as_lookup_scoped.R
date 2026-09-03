test_that("build_show_as_lookup_scoped matches a tagged event to its owner", {
  events <- list(list(
    iCalUId = "ABC123",
    start = list(dateTime = "2026-06-01T09:00:00"),
    showAs = "oof",
    X_cal_owner_email = "Tim.Roensch@studyflix.de"
  ))

  result <- build_show_as_lookup_scoped(events)

  expect_equal(nrow(result), 1)
  expect_equal(result$msgraph_ical_uid, "ABC123")
  expect_equal(result$email, "tim.roensch@studyflix.de")
  expect_equal(result$show_as, "oof")
})

test_that("build_show_as_lookup_scoped drops events without an owner tag", {
  # z.B. ein Fetch-Pfad ohne Tagging - sollte in der Praxis nicht vorkommen,
  # aber die Funktion darf dabei nicht raten.
  events <- list(list(
    iCalUId = "ABC123",
    start = list(dateTime = "2026-06-01T09:00:00"),
    showAs = "busy"
  ))

  result <- build_show_as_lookup_scoped(events)

  expect_equal(nrow(result), 0)
})

test_that("build_show_as_lookup_scoped keeps different calendar copies of the same event separate", {
  events <- list(
    list(iCalUId = "EVT1", start = list(dateTime = "2026-06-01T00:00:00"),
         showAs = "oof", X_cal_owner_email = "tim.roensch@studyflix.de"),
    list(iCalUId = "EVT1", start = list(dateTime = "2026-06-01T00:00:00"),
         showAs = "free", X_cal_owner_email = "ali.yildirim@studyflix.de")
  )

  result <- build_show_as_lookup_scoped(events)

  expect_equal(nrow(result), 2)
  tim_row <- result[result$email == "tim.roensch@studyflix.de", ]
  ali_row <- result[result$email == "ali.yildirim@studyflix.de", ]
  expect_equal(tim_row$show_as, "oof")
  expect_equal(ali_row$show_as, "free")
})

test_that("build_show_as_lookup_scoped passes through NA showAs", {
  events <- list(list(
    iCalUId = "ABC123",
    start = list(dateTime = "2026-06-01T09:00:00"),
    showAs = NULL,
    X_cal_owner_email = "tim.roensch@studyflix.de"
  ))

  result <- build_show_as_lookup_scoped(events)

  expect_equal(nrow(result), 1)
  expect_true(is.na(result$show_as))
})

test_that("build_show_as_lookup_scoped returns an empty typed tibble for no input", {
  result <- build_show_as_lookup_scoped(list())

  expect_equal(nrow(result), 0)
  expect_setequal(names(result), c("msgraph_ical_uid", "event_start", "email", "show_as"))
})

test_that("build_show_as_lookup_scoped dedups exact-duplicate tagged events", {
  events <- list(
    list(iCalUId = "ABC123", start = list(dateTime = "2026-06-01T09:00:00"),
         showAs = "oof", X_cal_owner_email = "tim.roensch@studyflix.de"),
    list(iCalUId = "ABC123", start = list(dateTime = "2026-06-01T09:00:00"),
         showAs = "oof", X_cal_owner_email = "tim.roensch@studyflix.de")
  )

  result <- build_show_as_lookup_scoped(events)

  expect_equal(nrow(result), 1)
})

test_that("build_show_as_lookup_scoped keeps two instances sharing an iCalUId but different event_start apart", {
  # Wiederkehrende Serie - dieselbe iCalUId, zwei verschiedene Instanzen.
  # Der Key (msgraph_ical_uid, event_start, email), auf den der Backfill
  # spaeter gegen raw.msgraph_events joint, muss die beiden trennen.
  events <- list(
    list(iCalUId = "SERIES1", start = list(dateTime = "2026-06-01T09:00:00"),
         showAs = "oof", X_cal_owner_email = "tim.roensch@studyflix.de"),
    list(iCalUId = "SERIES1", start = list(dateTime = "2026-06-08T09:00:00"),
         showAs = "busy", X_cal_owner_email = "tim.roensch@studyflix.de")
  )

  result <- build_show_as_lookup_scoped(events)

  expect_equal(nrow(result), 2)
  expect_setequal(result$show_as, c("oof", "busy"))
})

test_that("build_show_as_lookup_scoped dedup does not mask a real conflict between duplicate rows", {
  # Bewusst unterschiedliche showAs-Werte statt wie im Test oben identische -
  # der bestehende Dedup-Test kann Nichtdeterminismus beim Survivor nicht
  # erkennen. Hier nur erzwungen, dass GENAU EINER der beiden gueltigen
  # Werte ueberlebt.
  events <- list(
    list(iCalUId = "ABC123", start = list(dateTime = "2026-06-01T09:00:00"),
         showAs = "oof", X_cal_owner_email = "tim.roensch@studyflix.de"),
    list(iCalUId = "ABC123", start = list(dateTime = "2026-06-01T09:00:00"),
         showAs = "busy", X_cal_owner_email = "tim.roensch@studyflix.de")
  )

  result <- build_show_as_lookup_scoped(events)

  expect_equal(nrow(result), 1)
  expect_true(result$show_as %in% c("oof", "busy"))
})

test_that("build_show_as_lookup_scoped: no fan-out across multiple owners of the same event", {
  # Zwei verschiedene Kalender-Kopien desselben Events (gleiche iCalUId +
  # event_start, unterschiedliche Owner) muessen zwei getrennte Zeilen
  # bleiben, nicht eine mehrdeutige.
  events <- list(
    list(iCalUId = "EVT1", start = list(dateTime = "2026-06-01T00:00:00"),
         showAs = "oof", X_cal_owner_email = "tim.roensch@studyflix.de"),
    list(iCalUId = "EVT1", start = list(dateTime = "2026-06-01T00:00:00"),
         showAs = "free", X_cal_owner_email = "ali.yildirim@studyflix.de"),
    list(iCalUId = "EVT2", start = list(dateTime = "2026-06-02T00:00:00"),
         showAs = "busy", X_cal_owner_email = "tim.roensch@studyflix.de")
  )

  result <- build_show_as_lookup_scoped(events)

  expect_equal(nrow(result), 3)
  expect_equal(nrow(dplyr::distinct(result, msgraph_ical_uid, event_start, email)), 3)
})
