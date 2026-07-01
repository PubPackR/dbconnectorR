# Unit tests for fetch_with_retry(): retry behaviour, parse modes, error surfacing.
# httr is mocked, so these run offline with no credentials or network.

# --- fakes -------------------------------------------------------------------
# A sequence of responses is supplied; each GET pops the next one.
make_get <- function(responses) {
  i <- 0
  function(url, ...) {
    i <<- i + 1
    responses[[min(i, length(responses))]]
  }
}
# content() dispatches on `as`: "text" -> body string, "raw" -> raw, else parsed.
fake_content <- function(x, as = "parsed", type = NULL, encoding = NULL) {
  if (identical(as, "text")) return(if (is.null(x$body)) "<no body>" else x$body)
  if (identical(as, "raw"))  return(if (is.null(x$raw)) as.raw(0) else x$raw)
  x$payload
}
fake_headers <- function(x) if (is.null(x$headers)) list() else x$headers

run <- function(responses, ...) {
  testthat::with_mocked_bindings(
    fetch_with_retry(url = "https://x", access_token = "TOKEN", delay = 0, ...),
    GET = make_get(responses), content = fake_content, headers = fake_headers,
    .package = "httr"
  )
}

# --- tests -------------------------------------------------------------------

test_that("200 + parse=json returns the parsed body", {
  out <- run(list(list(status_code = 200, payload = list(value = "ok"))))
  expect_equal(out$value, "ok")
})

test_that("200 + parse=text returns raw text, not JSON-parsed", {
  out <- run(list(list(status_code = 200, body = "WEBVTT\n00:00 hi")), parse = "text")
  expect_true(is.character(out))
  expect_match(out, "WEBVTT")
})

test_that("HTTP 500 is retried, not mis-parsed as success (>= 500 fix)", {
  out <- run(rep(list(list(status_code = 500, payload = list(error = "boom"))), 3),
             max_retries = 3)
  expect_null(out)
})

test_that("a transient 5xx followed by 200 recovers within the same call", {
  out <- run(list(list(status_code = 502, body = "bad gateway"),
                  list(status_code = 200, payload = list(value = "recovered"))),
             max_retries = 5)
  expect_equal(out$value, "recovered")
})

test_that("error_on_failure = TRUE raises with the last HTTP status and body", {
  expect_error(
    run(rep(list(list(status_code = 502, body = "BadGateway detail")), 2),
        max_retries = 2, error_on_failure = TRUE),
    regexp = "502.*BadGateway detail"
  )
})

test_that("error_on_failure = FALSE (default) returns NULL on failure", {
  out <- run(rep(list(list(status_code = 503, body = "x")), 2), max_retries = 2)
  expect_null(out)
})

test_that("404 returns NULL/empty so the caller can fall back", {
  out <- run(list(list(status_code = 404, body = "not found")))
  expect_true(is.null(out) || length(out) == 0)
})
