# Zusammenfassungs-Kette: Ein gescheiterter Schritt beendet die Kette, und
# voruebergehende Fehler des Endpunkts werden erneut versucht. Beides zaehlt, seit
# das Routing ohne Fallback auf einen Endpunkt festgelegt sein kann.

http_error <- function(status, message) {
  structure(class = c(paste0("httr2_http_", status), "httr2_http", "error", "condition"),
            list(message = message, call = NULL))
}

# Chat-Attrappe: jede Antwort ist ein Text oder eine Fehlerbedingung.
fake_chat <- function(answers) {
  state <- new.env()
  state$calls <- 0L
  list(
    state = state,
    chat = function(prompt) {
      state$calls <- state$calls + 1L
      answer <- answers[[min(state$calls, length(answers))]]
      if (inherits(answer, "condition")) stop(answer)
      answer
    }
  )
}

prompt_paths <- function() {
  names <- c("einfuehrung", "aufgaben_kategorisierung", "aufgaben_kategorisierung_refinement",
             "aufgaben_neuvorstellung", "aufgaben_followup", "aufgaben_reporting",
             "pruefung_wettbewerber", "pruefung_sprecher", "pruefung_datum", "layout")
  dir <- withr::local_tempdir(.local_envir = parent.frame())
  paths <- lapply(names, function(n) {
    path <- file.path(dir, paste0(n, ".txt"))
    writeLines(n, path)
    path
  })
  stats::setNames(paths, names)
}

test_that("Ein voruebergehender Fehler des Endpunkts wird erneut versucht", {
  for (status in c(408, 429, 500, 502, 503, 504, 529)) {
    chat <- fake_chat(list(http_error(status, sprintf("HTTP %d.", status)), "Antwort"))
    expect_identical(suppressMessages(retry_llm("p", chat, wait_base = 0)), "Antwort")
    expect_identical(chat$state$calls, 2L)
  }
  chat <- fake_chat(list(
    structure(class = c("httr2_failure", "error", "condition"),
              list(message = "Failed to perform HTTP request.", call = NULL)),
    "Antwort"
  ))
  expect_identical(suppressMessages(retry_llm("p", chat, wait_base = 0)), "Antwort")
})

test_that("Ein endgueltiger Fehler wird nicht erneut versucht", {
  chat <- fake_chat(list(http_error(404, "No endpoints found matching your data policy"), "Antwort"))
  expect_true(is.na(suppressMessages(retry_llm("p", chat, wait_base = 0))))
  expect_identical(chat$state$calls, 1L)

  chat <- fake_chat(list(http_error(400, "HTTP 400 Bad Request."), "Antwort"))
  expect_true(is.na(suppressMessages(retry_llm("p", chat, wait_base = 0))))
  expect_identical(chat$state$calls, 1L)
})

test_that("Nach dem letzten Versuch gibt retry_llm NA zurueck", {
  chat <- fake_chat(list(http_error(502, "HTTP 502 Bad Gateway.")))
  expect_true(is.na(suppressMessages(retry_llm("p", chat, max_attempts = 3, wait_base = 0))))
  expect_identical(chat$state$calls, 3L)
})

test_that("Scheitert die Zusammenfassung, laufen die Pruefschritte nicht mehr", {
  # Antwort 1 ist die Kategorie, Antwort 2 die Zusammenfassung; ohne den Abbruch
  # wuerden die vier Folgeschritte "Pruefung" liefern und als Summary gespeichert.
  chat <- fake_chat(list("NV", http_error(400, "HTTP 400 Bad Request."), "Pruefung"))
  local_mocked_bindings(chat_openrouter = function(...) chat, .package = "ellmer")

  result <- suppressMessages(summarize_ts("Transkript", prompt_paths(), "openai/gpt-6-luna", "key"))
  expect_true(is.na(result))
  expect_identical(chat$state$calls, 2L)
})

test_that("Scheitert die Kategorisierung, wird nicht als NV zusammengefasst", {
  chat <- fake_chat(list(http_error(400, "HTTP 400 Bad Request."), "Zusammenfassung"))
  local_mocked_bindings(chat_openrouter = function(...) chat, .package = "ellmer")

  result <- suppressMessages(summarize_ts("Transkript", prompt_paths(), "openai/gpt-6-luna", "key"))
  expect_true(is.na(result))
  expect_identical(chat$state$calls, 1L)
})

test_that("Scheitert ein Pruefschritt, wird nichts gespeichert", {
  chat <- fake_chat(list("FU", "Zusammenfassung", "Pruefung 1",
                         http_error(400, "HTTP 400 Bad Request."), "Pruefung 3"))
  local_mocked_bindings(chat_openrouter = function(...) chat, .package = "ellmer")

  result <- suppressMessages(summarize_ts("Transkript", prompt_paths(), "openai/gpt-6-luna", "key"))
  expect_true(is.na(result))
  expect_identical(chat$state$calls, 4L)
})

test_that("Eine Kette ohne Fehler liefert die Antwort des letzten Schritts", {
  chat <- fake_chat(list("RP", "Zusammenfassung", "Pruefung 1", "Pruefung 2", "Pruefung 3", "Layout"))
  local_mocked_bindings(chat_openrouter = function(...) chat, .package = "ellmer")

  expect_identical(summarize_ts("Transkript", prompt_paths(), "openai/gpt-6-luna", "key"), "Layout")
  expect_identical(chat$state$calls, 6L)
})

test_that("Eine Antwort ohne Kategorie fuehrt weiter zur Nachfrage und zu NV", {
  # Bisheriges Verhalten bleibt: keine Kategorie erkannt ist kein Fehler.
  chat <- fake_chat(list("unklar", "weiter unklar", "Zusammenfassung", "P1", "P2", "P3", "Layout"))
  local_mocked_bindings(chat_openrouter = function(...) chat, .package = "ellmer")

  expect_identical(summarize_ts("Transkript", prompt_paths(), "openai/gpt-6-luna", "key"), "Layout")
  expect_identical(chat$state$calls, 7L)
})
