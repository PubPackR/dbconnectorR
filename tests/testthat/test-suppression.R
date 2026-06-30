test_that("dsgvo_suppress_msgraph_record maps both column shapes to the stable tombstone", {
  suppressMessages(library(Billomatics))
  blocked <- dsgvo_hash_email("t@x.de", "p")
  calls <- data.frame(call_identity_mail = "t@x.de", call_identity_name = "T", stringsAsFactors = FALSE)
  ev <- data.frame(attendees_emailAddress_address = "t@x.de", attendees_emailAddress_name = "T", stringsAsFactors = FALSE)
  oc <- dsgvo_suppress_msgraph_record(calls, blocked, "p")
  oe <- dsgvo_suppress_msgraph_record(ev, blocked, "p",
          mail_col = "attendees_emailAddress_address", name_col = "attendees_emailAddress_name")
  ts <- dsgvo_email_tombstone(blocked)
  expect_equal(oc$call_identity_mail, ts)
  expect_equal(oe$attendees_emailAddress_address, ts)
})

test_that("no-op when there is nothing to suppress (mirrors the suppression_pepper = NULL path)", {
  suppressMessages(library(Billomatics))
  calls <- data.frame(call_identity_mail = "t@x.de", call_identity_name = "T", stringsAsFactors = FALSE)
  # leere Sperrliste -> Record unveraendert (entspricht dem uebersprungenen Hook bei pepper = NULL)
  out <- dsgvo_suppress_msgraph_record(calls, character(0), "p")
  expect_equal(out$call_identity_mail, "t@x.de")
  expect_equal(out$call_identity_name, "T")
})
