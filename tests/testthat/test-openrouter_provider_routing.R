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

test_that("Eine leere Endpunktliste oder falsche Typen brechen laut ab", {
  expect_error(build_openrouter_api_args(list(zdr = TRUE, only = character(0))), "at least one")
  expect_error(build_openrouter_api_args(list(zdr = TRUE, only = list())), "at least one")
  expect_error(build_openrouter_api_args(list(zdr = TRUE, only = "")), "at least one")
  expect_error(build_openrouter_api_args(list(zdr = "true")), "TRUE or FALSE")
  expect_error(build_openrouter_api_args(list(allow_fallbacks = NA)), "TRUE or FALSE")
  expect_error(build_openrouter_api_args(list(data_collection = "nein")), "deny")
})

test_that("msgraph_update_transcripts reicht das Routing bis zum Zusammenfassen durch", {
  seen <- new.env()
  routing <- list(zdr = TRUE, only = "azure/eu")
  local_mocked_bindings(
    msgraph_summarize_transcripts = function(...) {
      seen$args <- list(...)
      invisible(NULL)
    }
  )

  msgraph_update_transcripts(con = NULL, msgraph_keys = NULL, openrouter_keys = "key",
                             crm_keys = "crm", openrouter_model = "openai/gpt-5.6-luna",
                             steps = "summarize", log_level = "ERROR",
                             openrouter_provider = routing)
  expect_identical(seen$args$openrouter_provider, routing)
  expect_identical(seen$args$openrouter_model, "openai/gpt-5.6-luna")
})

test_that("msgraph_summarize_transcripts reicht das Routing an die Schleife durch", {
  seen <- new.env()
  routing <- list(zdr = TRUE, only = "azure/eu")
  local_mocked_bindings(
    generate_and_save_transcript_summaries = function(...) {
      seen$args <- list(...)
      invisible(NULL)
    }
  )

  msgraph_summarize_transcripts(con = NULL, openrouter_model = "openai/gpt-5.6-luna",
                                openrouter_keys = "key", logger = function(...) NULL,
                                openrouter_provider = routing)
  expect_identical(seen$args$openrouter_provider, routing)
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
