# A transcript protocol also goes to every candidate lead whose lead rep was in
# the call. find_lead_rep_additional_leads() decides which leads; the payload
# tests run export_single_protocol_to_crm() with httr mocked, offline.

# --- find_lead_rep_additional_leads() ----------------------------------------

# Call 818860 as it happened: Simone Kuhn twice in the CRM. Julia's lead (internal
# 1, person 21400532) is picked through Simone's office address; Peter's lead
# (internal 2, person 47174012) is reached only through a colleague's office_hq
# address. Peter (CRM user 144) was in the call, Julia (64) was not.
candidates <- tibble::tibble(
  call_id             = c("818860", "818860"),
  crm_lead_id         = c(1L, 2L),
  external_lead_id    = c(21400532L, 47174012L),
  responsible_user_id = c(64L, 144L)
)
chosen <- tibble::tibble(call_id = "818860", crm_lead_id = 1L)

test_that("a candidate whose lead rep was in the call is added", {
  result <- find_lead_rep_additional_leads(
    candidates, chosen,
    call_crm_users = tibble::tibble(call_id = "818860", crm_user_id = 144L)
  )

  expect_equal(result$call_id, "818860")
  expect_equal(result$additional_person_ids, list(47174012L))
})

test_that("no lead rep in the call adds nothing", {
  result <- find_lead_rep_additional_leads(
    candidates, chosen,
    call_crm_users = tibble::tibble(call_id = "818860", crm_user_id = 999L)
  )

  expect_equal(nrow(result), 0)
  expect_named(result, c("call_id", "additional_person_ids"))
})

test_that("a lead rep who owns the picked lead adds nothing", {
  result <- find_lead_rep_additional_leads(
    candidates, chosen,
    call_crm_users = tibble::tibble(call_id = "818860", crm_user_id = 64L)
  )

  expect_equal(nrow(result), 0)
})

test_that("several qualifying leads are all added, once each", {
  more <- dplyr::bind_rows(
    candidates,
    tibble::tibble(call_id = "818860", crm_lead_id = 3L,
                   external_lead_id = 50000001L, responsible_user_id = 200L),
    # the same lead reached through a second address
    tibble::tibble(call_id = "818860", crm_lead_id = 2L,
                   external_lead_id = 47174012L, responsible_user_id = 144L)
  )

  result <- find_lead_rep_additional_leads(
    more, chosen,
    call_crm_users = tibble::tibble(call_id = c("818860", "818860"),
                                    crm_user_id = c(144L, 200L))
  )

  expect_equal(result$additional_person_ids, list(c(47174012L, 50000001L)))
})

test_that("a lead rep only counts in the call they were in", {
  two_calls <- dplyr::bind_rows(
    candidates,
    tibble::tibble(call_id = "900000", crm_lead_id = 2L,
                   external_lead_id = 47174012L, responsible_user_id = 144L)
  )

  result <- find_lead_rep_additional_leads(
    two_calls,
    chosen_leads = dplyr::bind_rows(chosen, tibble::tibble(call_id = "900000", crm_lead_id = 9L)),
    call_crm_users = tibble::tibble(call_id = "900000", crm_user_id = 144L)
  )

  expect_equal(result$call_id, "900000")
})

test_that("a lead without lead rep never qualifies", {
  no_rep <- dplyr::mutate(candidates, responsible_user_id = NA_integer_)

  result <- find_lead_rep_additional_leads(
    no_rep, chosen,
    call_crm_users = tibble::tibble(call_id = "818860", crm_user_id = 144L)
  )

  expect_equal(nrow(result), 0)
})

# --- payload of export_single_protocol_to_crm() -------------------------------

posted_body <- function(protocol_data, use_test_account = FALSE) {
  captured <- NULL
  testthat::with_mocked_bindings(
    suppressMessages(capture.output(
      export_single_protocol_to_crm(NULL, c("X-apikey" = "KEY"), protocol_data,
                                    use_test_account)
    )),
    POST = function(url, ..., body) {
      captured <<- body
      structure(list(status_code = 201L), class = "response")
    },
    .package = "httr"
  )
  jsonlite::fromJSON(captured, simplifyVector = FALSE)$protocol
}

protocol_row <- function(additional) {
  tibble::tibble(
    protocol_header       = "Teams Meeting Summary - 2026-11-12",
    occurred_at           = NA_character_,
    content               = "### Zusammenfassung",
    attachable_id         = 21400532L,
    attachable_type       = "people",
    sales_user_names      = "Peter Egger",
    additional_person_ids = list(additional)
  )
}

test_that("the protocol carries the picked lead and the additional leads", {
  body <- posted_body(protocol_row(47174012L))

  expect_equal(body$person_id, 21400532L)
  expect_equal(unlist(body$person_ids), c(21400532L, 47174012L))
})

test_that("without additional leads the payload is the same as before", {
  body <- posted_body(protocol_row(integer(0)))

  expect_equal(body$person_id, 21400532L)
  expect_equal(unlist(body$person_ids), 21400532L)
})

test_that("a rejected multi-person protocol is sent to the picked lead alone", {
  bodies <- list()
  result <- testthat::with_mocked_bindings(
    suppressWarnings(suppressMessages({
      out <- NULL
      capture.output(out <- export_single_protocol_to_crm(
        NULL, c("X-apikey" = "KEY"), protocol_row(47174012L), FALSE))
      out
    })),
    POST = function(url, ..., body) {
      bodies[[length(bodies) + 1]] <<- jsonlite::fromJSON(body, simplifyVector = FALSE)$protocol
      status <- if (length(bodies) == 1) 422L else 201L
      structure(list(status_code = status), class = "response")
    },
    content = function(x, ...) "Unprocessable",
    .package = "httr"
  )

  expect_true(result$success)
  expect_length(bodies, 2)
  expect_equal(unlist(bodies[[1]]$person_ids), c(21400532L, 47174012L))
  expect_equal(unlist(bodies[[2]]$person_ids), 21400532L)
})

test_that("a rate-limited multi-person protocol is not narrowed", {
  calls <- 0
  result <- testthat::with_mocked_bindings(
    suppressWarnings(suppressMessages({
      out <- NULL
      capture.output(out <- export_single_protocol_to_crm(
        NULL, c("X-apikey" = "KEY"), protocol_row(47174012L), FALSE))
      out
    })),
    POST = function(url, ..., body) {
      calls <<- calls + 1
      structure(list(status_code = 429L), class = "response")
    },
    content = function(x, ...) "Retry later",
    .package = "httr"
  )

  expect_false(result$success)
  expect_equal(calls, 1)
})

test_that("the test account keeps the protocol on the test person only", {
  body <- posted_body(protocol_row(47174012L), use_test_account = TRUE)

  expect_equal(unlist(body$person_ids), 33560961L)
})
