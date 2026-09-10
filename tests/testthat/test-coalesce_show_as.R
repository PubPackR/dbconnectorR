test_that("coalesce_show_as keeps the existing value when the new fetch is NA", {
  new_participants <- tibble::tibble(contact_id = 1L, event_id = 10L, show_as = NA_character_)
  existing <- tibble::tibble(contact_id = 1L, event_id = 10L, show_as = "oof")

  result <- coalesce_show_as(new_participants, existing, by = c("contact_id", "event_id"))

  expect_equal(result$show_as, "oof")
})

test_that("coalesce_show_as lets a real new value overwrite an existing value", {
  new_participants <- tibble::tibble(contact_id = 1L, event_id = 10L, show_as = "busy")
  existing <- tibble::tibble(contact_id = 1L, event_id = 10L, show_as = "oof")

  result <- coalesce_show_as(new_participants, existing, by = c("contact_id", "event_id"))

  expect_equal(result$show_as, "busy")
})

test_that("coalesce_show_as leaves NA as NA when there is no existing value either", {
  new_participants <- tibble::tibble(contact_id = 1L, event_id = 10L, show_as = NA_character_)
  existing <- tibble::tibble(contact_id = integer(), event_id = integer(), show_as = character())

  result <- coalesce_show_as(new_participants, existing, by = c("contact_id", "event_id"))

  expect_true(is.na(result$show_as))
})

test_that("coalesce_show_as does not touch rows without a matching key in existing", {
  new_participants <- tibble::tibble(
    contact_id = c(1L, 2L),
    event_id   = c(10L, 20L),
    show_as    = c(NA_character_, "free")
  )
  existing <- tibble::tibble(contact_id = 1L, event_id = 10L, show_as = "oof")

  result <- coalesce_show_as(new_participants, existing, by = c("contact_id", "event_id"))

  expect_equal(result$show_as, c("oof", "free"))
})

test_that("coalesce_show_as does not fan out on a duplicate key in existing", {
  # existing sollte immer schon auf (contact_id, event_id) dedupliziert
  # ankommen (siehe Call-Sites), aber die Funktion selbst darf bei einem
  # Verstoss keine zusaetzlichen Zeilen erzeugen.
  new_participants <- tibble::tibble(contact_id = 1L, event_id = 10L, show_as = NA_character_)
  existing <- tibble::tibble(contact_id = c(1L, 1L), event_id = c(10L, 10L), show_as = c("oof", "oof"))

  result <- coalesce_show_as(new_participants, existing, by = c("contact_id", "event_id"))

  expect_equal(nrow(result), 1)
  expect_equal(result$show_as, "oof")
})
