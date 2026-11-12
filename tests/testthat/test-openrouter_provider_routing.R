# Provider-Routing fuer OpenRouter: welche Anbieter eine Zusammenfassung sehen duerfen.

test_that("Ohne Routing bleiben die api_args leer", {
  expect_identical(build_openrouter_api_args(NULL), list())
})

test_that("Ein einzelner Endpunkt geht als JSON-Array raus, nicht als String", {
  args <- build_openrouter_api_args(list(
    zdr = TRUE, data_collection = "deny", only = "azure/eu", allow_fallbacks = FALSE
  ))
  json <- as.character(jsonlite::toJSON(args, auto_unbox = TRUE))
  expect_match(json, '"only":["azure/eu"]', fixed = TRUE)
  expect_match(json, '"zdr":true', fixed = TRUE)
  expect_match(json, '"data_collection":"deny"', fixed = TRUE)
  expect_match(json, '"allow_fallbacks":false', fixed = TRUE)
})

test_that("Mehrere Endpunkte bleiben in ihrer Reihenfolge erhalten", {
  args <- build_openrouter_api_args(list(only = c("google-vertex/europe", "amazon-bedrock/eu-west-1")))
  expect_identical(args$provider$only, list("google-vertex/europe", "amazon-bedrock/eu-west-1"))
})

test_that("Ein Routing ohne Namen bricht laut ab", {
  expect_error(build_openrouter_api_args("azure/eu"), "named list")
  expect_error(build_openrouter_api_args(list("azure/eu")), "named list")
  expect_error(build_openrouter_api_args(list()), "named list")
})

test_that("summarize_ts reicht das Routing an ellmer::chat_openrouter durch", {
  einfuehrung <- withr::local_tempfile(lines = "Einfuehrung")
  seen <- new.env()
  local_mocked_bindings(
    chat_openrouter = function(...) {
      seen$args <- list(...)
      stop("nach dem Mitschnitt angehalten")
    },
    .package = "ellmer"
  )

  expect_error(
    summarize_ts("Transkript", list(einfuehrung = einfuehrung), "openai/gpt-5.6-luna", "key",
                 openrouter_provider = list(zdr = TRUE, only = "azure/eu")),
    "nach dem Mitschnitt angehalten"
  )
  expect_identical(seen$args$api_args, list(provider = list(zdr = TRUE, only = list("azure/eu"))))
  expect_identical(seen$args$model, "openai/gpt-5.6-luna")
})

test_that("summarize_ts ohne Routing ruft ellmer wie bisher ohne Provider-Vorgabe auf", {
  einfuehrung <- withr::local_tempfile(lines = "Einfuehrung")
  seen <- new.env()
  local_mocked_bindings(
    chat_openrouter = function(...) {
      seen$args <- list(...)
      stop("nach dem Mitschnitt angehalten")
    },
    .package = "ellmer"
  )

  expect_error(
    summarize_ts("Transkript", list(einfuehrung = einfuehrung), "openai/gpt-5.6-luna", "key"),
    "nach dem Mitschnitt angehalten"
  )
  expect_identical(seen$args$api_args, list())
})
