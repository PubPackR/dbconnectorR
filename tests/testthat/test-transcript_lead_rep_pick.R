# pick_transcript_lead(): one protocol per call on exactly one lead. A lead
# whose lead rep was in the call wins over the address tier; otherwise the tier
# decides as before.

# Call 818860 as it happened: Simone Kuhn twice in the CRM. Julia's lead
# (internal 1) is reached through Simone's office address, Peter's lead
# (internal 2) only through a colleague's office_hq address. Peter (CRM user
# 144) was in the call, Julia (64) was not.
candidates <- tibble::tibble(
  id               = c(11928L, 11928L),
  call_id          = c(818860L, 818860L),
  crm_lead_id      = c(1L, 2L),
  email_source_crm = c("office", "office_hq")
)
lead_reps <- tibble::tibble(crm_lead_id = c(1L, 2L), responsible_user_id = c(64L, 144L))

pick <- function(candidates, lead_reps, users) {
  pick_transcript_lead(candidates, lead_reps,
                       tibble::tibble(call_id = users$call_id, crm_user_id = users$crm_user_id))
}

test_that("the lead of a rep in the call wins over a better address tier", {
  result <- pick(candidates, lead_reps, list(call_id = 818860L, crm_user_id = 144L))

  expect_equal(nrow(result), 1)
  expect_equal(result$crm_lead_id, 2L)
  expect_true(result$lead_rep_in_call)
})

test_that("without a lead rep in the call the address tier decides as before", {
  result <- pick(candidates, lead_reps, list(call_id = 818860L, crm_user_id = 999L))

  expect_equal(result$crm_lead_id, 1L)
  expect_false(result$lead_rep_in_call)
})

test_that("a call without internal CRM users keeps the address tier", {
  result <- pick(candidates, lead_reps, list(call_id = integer(0), crm_user_id = integer(0)))

  expect_equal(result$crm_lead_id, 1L)
})

test_that("among several leads of reps in the call the address tier decides", {
  three <- dplyr::bind_rows(
    candidates,
    tibble::tibble(id = 11928L, call_id = 818860L, crm_lead_id = 3L,
                   email_source_crm = "from_description")
  )
  reps <- dplyr::bind_rows(lead_reps,
                           tibble::tibble(crm_lead_id = 3L, responsible_user_id = 200L))

  result <- pick(three, reps, list(call_id = c(818860L, 818860L), crm_user_id = c(144L, 200L)))

  expect_equal(result$crm_lead_id, 2L)
})

test_that("a lead rep only counts in the call they were in", {
  two_calls <- dplyr::bind_rows(
    candidates,
    tibble::tibble(id = c(500L, 500L), call_id = c(900000L, 900000L),
                   crm_lead_id = c(1L, 2L), email_source_crm = c("office", "office_hq"))
  )

  result <- pick(two_calls, lead_reps, list(call_id = 900000L, crm_user_id = 144L))

  expect_equal(result$crm_lead_id[result$call_id == 818860L], 1L)
  expect_equal(result$crm_lead_id[result$call_id == 900000L], 2L)
})

test_that("a lead missing from lead_reps (deleted) never wins through its rep", {
  result <- pick(candidates, lead_reps[1, ], list(call_id = 818860L, crm_user_id = 144L))

  expect_equal(result$crm_lead_id, 1L)
})

test_that("integer64 and integer call ids still match", {
  result <- pick(candidates, lead_reps,
                 list(call_id = bit64::as.integer64(818860), crm_user_id = 144L))

  expect_equal(result$crm_lead_id, 2L)
})

test_that("a tie in both keeps the first row of the call, as before", {
  tie <- tibble::tibble(id = c(1L, 1L), call_id = c(7L, 7L), crm_lead_id = c(5L, 6L),
                        email_source_crm = c("office", "office"))

  result <- pick(tie, tibble::tibble(crm_lead_id = integer(0), responsible_user_id = integer(0)),
                 list(call_id = integer(0), crm_user_id = integer(0)))

  expect_equal(result$crm_lead_id, 5L)
  expect_named(result, c("id", "call_id", "crm_lead_id", "email_source_crm", "lead_rep_in_call"))
})
