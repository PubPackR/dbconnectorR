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
