test_that("build_show_as_lookup matches a copy to its owner's email", {
  events <- data.frame(
    iCalUId = "ABC123",
    start_dateTime = "2026-06-01T09:00:00",
    user_id = "u1",
    showAs = "oof",
    stringsAsFactors = FALSE
  )
  users <- data.frame(id = "u1", email = "Tim.Roensch@studyflix.de", stringsAsFactors = FALSE)

  result <- build_show_as_lookup(events, users)

  expect_equal(nrow(result), 1)
  expect_equal(result$event_id, "ABC123")
  expect_equal(result$attendees_emailAddress_address, "tim.roensch@studyflix.de")
  expect_equal(result$show_as, "oof")
})

test_that("build_show_as_lookup drops copies without a resolvable internal user", {
  # z.B. keine Kalenderfreigabe / ausgeschiedene Person - erwarteter,
  # dauerhafter Zustand, kein Fehlerfall.
  events <- data.frame(
    iCalUId = "ABC123",
    start_dateTime = "2026-06-01T09:00:00",
    user_id = "unknown_user",
    showAs = "busy",
    stringsAsFactors = FALSE
  )
  users <- data.frame(id = "u1", email = "tim.roensch@studyflix.de", stringsAsFactors = FALSE)

  result <- build_show_as_lookup(events, users)

  expect_equal(nrow(result), 0)
})

test_that("build_show_as_lookup keeps different users' copies of the same event separate", {
  # Tim + Ali, gleiches Event, unterschiedliche eigene showAs-Werte.
  events <- data.frame(
    iCalUId = c("EVT1", "EVT1"),
    start_dateTime = c("2026-06-01T00:00:00", "2026-06-01T00:00:00"),
    user_id = c("u_tim", "u_ali"),
    showAs = c("oof", "free"),
    stringsAsFactors = FALSE
  )
  users <- data.frame(
    id = c("u_tim", "u_ali"),
    email = c("tim.roensch@studyflix.de", "ali.yildirim@studyflix.de"),
    stringsAsFactors = FALSE
  )

  result <- build_show_as_lookup(events, users)

  expect_equal(nrow(result), 2)
  tim_row <- result[result$attendees_emailAddress_address == "tim.roensch@studyflix.de", ]
  ali_row <- result[result$attendees_emailAddress_address == "ali.yildirim@studyflix.de", ]
  expect_equal(tim_row$show_as, "oof")
  expect_equal(ali_row$show_as, "free")
})

test_that("build_show_as_lookup dedups exact-duplicate (event, copy) rows", {
  events <- data.frame(
    iCalUId = c("ABC123", "ABC123"),
    start_dateTime = c("2026-06-01T09:00:00", "2026-06-01T09:00:00"),
    user_id = c("u1", "u1"),
    showAs = c("oof", "oof"),
    stringsAsFactors = FALSE
  )
  users <- data.frame(id = "u1", email = "tim.roensch@studyflix.de", stringsAsFactors = FALSE)

  result <- build_show_as_lookup(events, users)

  expect_equal(nrow(result), 1)
})

test_that("build_show_as_lookup returns NA show_as when the API field itself is missing/NA", {
  events <- data.frame(
    iCalUId = "ABC123",
    start_dateTime = "2026-06-01T09:00:00",
    user_id = "u1",
    showAs = NA_character_,
    stringsAsFactors = FALSE
  )
  users <- data.frame(id = "u1", email = "tim.roensch@studyflix.de", stringsAsFactors = FALSE)

  result <- build_show_as_lookup(events, users)

  expect_equal(nrow(result), 1)
  expect_true(is.na(result$show_as))
})

test_that("build_show_as_lookup handles the real production shape: showAs as a list column", {
  # all_calendar_events_ entsteht produktiv via
  # as.data.frame(t(sapply(all_records, ...))), was fuer JEDES Feld eine
  # List-Column liefert - nicht nur fuer showAs. Ohne den as.character()-Cast
  # in build_show_as_lookup() blieb show_as eine List-Column und crashte
  # bind_rows() gegen die aus der DB gelesenen character-Werte (Review-Fund,
  # Issue 1). Reproduziert den echten Input-Shape statt eines atomaren data
  # frame wie in den Tests oben.
  events <- data.frame(iCalUId = "ABC123", start_dateTime = "2026-06-01T09:00:00",
                        user_id = "u1", stringsAsFactors = FALSE)
  events$showAs <- list("oof")
  users <- data.frame(id = "u1", email = "tim.roensch@studyflix.de", stringsAsFactors = FALSE)

  result <- build_show_as_lookup(events, users)

  expect_false(is.list(result$show_as))
  expect_equal(result$show_as, "oof")
})

test_that("build_show_as_lookup keeps two events sharing an iCalUId but different event_start apart", {
  # Wiederkehrende Serie: dieselbe iCalUId, zwei verschiedene Instanzen. Der
  # Key (event_id, event_start, email) muss die beiden trennen, nicht zu
  # einem Event zusammenfassen - genau die Kernannahme, auf der der spaetere
  # Join mit raw.msgraph_events (msgraph_ical_uid, event_start) beruht.
  events <- data.frame(
    iCalUId = c("SERIES1", "SERIES1"),
    start_dateTime = c("2026-06-01T09:00:00", "2026-06-08T09:00:00"),
    user_id = c("u1", "u1"),
    showAs = c("oof", "busy"),
    stringsAsFactors = FALSE
  )
  users <- data.frame(id = "u1", email = "tim.roensch@studyflix.de", stringsAsFactors = FALSE)

  result <- build_show_as_lookup(events, users)

  expect_equal(nrow(result), 2)
  expect_setequal(result$show_as, c("oof", "busy"))
})

test_that("build_show_as_lookup dedup does not mask a real conflict between duplicate rows", {
  # Die bestehenden Dedup-Tests nutzen identische showAs-Werte in beiden
  # Duplikaten und koennen daher nie erkennen, WELCHES Duplikat ueberlebt.
  # Hier bewusst unterschiedliche Werte - der Test erzwingt nur, dass genau
  # einer der beiden gueltigen Werte survived (nicht welcher), statt
  # stillschweigend anzunehmen, dass Reihenfolge irrelevant ist.
  events <- data.frame(
    iCalUId = c("ABC123", "ABC123"),
    start_dateTime = c("2026-06-01T09:00:00", "2026-06-01T09:00:00"),
    user_id = c("u1", "u1"),
    showAs = c("oof", "busy"),
    stringsAsFactors = FALSE
  )
  users <- data.frame(id = "u1", email = "tim.roensch@studyflix.de", stringsAsFactors = FALSE)

  result <- build_show_as_lookup(events, users)

  expect_equal(nrow(result), 1)
  expect_true(result$show_as %in% c("oof", "busy"))
})

test_that("build_show_as_lookup's left_join cannot fan out and has a clean no-match path", {
  # Zwei User teilen sich denselben (event_id, event_start)-Schluessel (zwei
  # Kalender-Kopien desselben Events), plus ein User ohne Match in `users`.
  # Der Join darf pro (event, copy) nur genau eine Zeile erzeugen.
  events <- data.frame(
    iCalUId = c("EVT1", "EVT1", "EVT2"),
    start_dateTime = c("2026-06-01T00:00:00", "2026-06-01T00:00:00", "2026-06-02T00:00:00"),
    user_id = c("u_tim", "u_ali", "u_unknown"),
    showAs = c("oof", "free", "busy"),
    stringsAsFactors = FALSE
  )
  users <- data.frame(
    id = c("u_tim", "u_ali"),
    email = c("tim.roensch@studyflix.de", "ali.yildirim@studyflix.de"),
    stringsAsFactors = FALSE
  )

  result <- build_show_as_lookup(events, users)

  expect_equal(nrow(result), 2)
  expect_equal(nrow(dplyr::distinct(result, event_id, event_start, attendees_emailAddress_address)), 2)
})
