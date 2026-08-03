test_that("graph_collect folgt @odata.nextLink und aggregiert value", {
  mockery::stub(graph_collect, "graph_get", function(url, token, query = NULL) {
    if (identical(url, "url2")) list(status = 200, content = list(value = list(list(id = "b"))))
    else list(status = 200, content = list(value = list(list(id = "a")), `@odata.nextLink` = "url2"))
  })
  res <- graph_collect("url1", token = "tok")
  expect_equal(res$status, 200)
  expect_equal(length(res$value), 2)
  expect_equal(res$value[[2]]$id, "b")
})

test_that("parse_scoped_user mappt Graph-User auf raw.msgraph_users-Schema (email/upn lowercase)", {
  u <- list(id = "OID123", givenName = "Tim", surname = "Roensch",
            userPrincipalName = "Tim.Roensch@studyflix.de", displayName = "Tim Roensch",
            mail = "Tim.Roensch@studyflix.de")
  row <- parse_scoped_user(u)
  expect_equal(nrow(row), 1)
  expect_equal(row$msgraph_user_id, "OID123")
  expect_equal(row$first_name, "Tim")
  expect_equal(row$name, "Roensch")
  expect_equal(row$email, "tim.roensch@studyflix.de")
  expect_equal(row$user_principal_name, "tim.roensch@studyflix.de")
  expect_true(row$is_internal)
  expect_false(row$is_deleted)
})

test_that("parse_scoped_events mappt Felder und extrahiert Teilnehmer inkl. Organizer", {
  ev <- list(list(
    iCalUId = "ICAL1", type = "singleInstance",
    createdDateTime = "2026-07-01T08:00:00Z", lastModifiedDateTime = "2026-07-02T09:00:00Z",
    subject = "Kundencall", start = list(dateTime = "2026-07-10T10:00:00.0000000"),
    end = list(dateTime = "2026-07-10T10:30:00.0000000"),
    isCancelled = FALSE, isOnlineMeeting = TRUE,
    onlineMeeting = list(joinUrl = "https://teams.microsoft.com/l/meetup-join/19%3ameeting_ABC%40thread.v2/0"),
    organizer = list(emailAddress = list(name = "Rep A", address = "rep.a@studyflix.de")),
    attendees = list(list(emailAddress = list(name = "Kunde X", address = "x@kunde.com")))
  ))
  out <- parse_scoped_events(ev)
  expect_equal(nrow(out$events), 1)
  expect_equal(out$events$msgraph_ical_uid, "ICAL1")
  expect_true(out$events$is_single_instance)
  expect_equal(out$events$event_start, "2026-07-10T10:00:00.0000000")
  # Organizer + 1 Attendee = 2 Teilnehmerzeilen, Emails lowercase
  expect_equal(nrow(out$participants), 2)
  expect_setequal(out$participants$email, c("rep.a@studyflix.de", "x@kunde.com"))
  expect_true(out$participants$is_organizer[out$participants$email == "rep.a@studyflix.de"])
  expect_true(all(out$participants$source == "calendar"))
})
