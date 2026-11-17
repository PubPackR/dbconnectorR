# msgraph_export_single_transcript_to_crm() and the CRM rate limit.
# CentralStation answers a throttled call with HTTP 429 and the body "Retry
# later". httr is mocked, so these run offline with no credentials or network.

# --- fakes -------------------------------------------------------------------
# A sequence of responses is supplied; each call pops the next one and the last
# one repeats. calls() tells how often the endpoint was hit.
make_endpoint <- function(responses) {
  i <- 0
  list(
    call  = function(url, ...) {
      i <<- i + 1
      responses[[min(i, length(responses))]]
    },
    calls = function() i
  )
}

# Class "response" so httr::status_code() dispatches as on a real answer. The
# short retry-after keeps the wait of the retry at a hundredth of a second.
fake_response <- function(status_code, body = "") {
  structure(
    list(status_code = status_code, body = body,
         headers = list(`retry-after` = "0.01")),
    class = "response"
  )
}
fake_content <- function(x, as = "parsed", type = NULL, encoding = NULL) x$body
fake_headers <- function(x) x$headers

throttled <- fake_response(429L, "Retry later")

export_with <- function(post = make_endpoint(list()), put = make_endpoint(list()), ...) {
  testthat::with_mocked_bindings(
    suppressMessages(msgraph_export_single_transcript_to_crm(
      con         = NULL,
      crm_api_key = "KEY",
      lead_id     = 1L,
      content     = "### Zusammenfassung\n- Punkt 1",
      ...
    )),
    POST = post$call, PUT = put$call, content = fake_content, headers = fake_headers,
    .package = "httr"
  )
}

# --- tests -------------------------------------------------------------------

test_that("a throttled create call is repeated and the protocol is created", {
  post <- make_endpoint(list(throttled, fake_response(201L, '{"protocol":{"id":123}}')))

  result <- export_with(post = post)

  expect_true(result$success)
  expect_null(result$error)
  expect_equal(post$calls(), 2)
})

test_that("a throttled update call is repeated and the protocol is updated", {
  put <- make_endpoint(list(throttled, fake_response(200L, '{"protocol":{"id":123}}')))

  result <- export_with(put = put, protocol_id = 123)

  expect_true(result$success)
  expect_equal(put$calls(), 2)
})

test_that("a rate limit that never clears ends as a failed export naming the status", {
  post <- make_endpoint(list(throttled))

  expect_warning(result <- export_with(post = post), "429")

  expect_false(result$success)
  expect_match(result$error, "HTTP 429: Retry later", fixed = TRUE)
  expect_gt(post$calls(), 1)
})

test_that("another error status is not repeated", {
  post <- make_endpoint(list(fake_response(422L, "Unprocessable")))

  result <- export_with(post = post)

  expect_false(result$success)
  expect_match(result$error, "HTTP 422: Unprocessable", fixed = TRUE)
  expect_equal(post$calls(), 1)
})
