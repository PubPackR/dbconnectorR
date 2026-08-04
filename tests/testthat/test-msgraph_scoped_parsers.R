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
    attendees = list(list(emailAddress = list(name = "Kunde X", address = "x@kunde.com")),
                     list(emailAddress = list(name = "Kunde EXT",
                                               address = "john_doe_gmail.com#EXT#@tenant.onmicrosoft.com")))
  ))
  out <- parse_scoped_events(ev)
  expect_equal(nrow(out$events), 1)
  expect_equal(out$events$msgraph_ical_uid, "ICAL1")
  expect_true(out$events$is_single_instance)
  # event_start / event_end sind POSIXct (Join-Key muss zum DB-Reread passen)
  expect_s3_class(out$events$event_start, "POSIXct")
  expect_s3_class(out$participants$event_start, "POSIXct")
  # Organizer + 2 Attendees (davon 1 #EXT#) = 3 Teilnehmerzeilen, Emails lowercase
  expect_equal(nrow(out$participants), 3)
  known <- c("rep.a@studyflix.de", "x@kunde.com")
  expect_true(all(known %in% out$participants$email))
  expect_true(out$participants$is_organizer[out$participants$email == "rep.a@studyflix.de"])
  expect_true(all(out$participants$source == "calendar"))
  # #EXT#-Adresse muss zurueckuebersetzt + lowercase sein (exakter Output liegt bei normalize_external_email)
  ext_email <- out$participants$email[!out$participants$email %in% known]
  expect_length(ext_email, 1)
  expect_false(grepl("#ext#", ext_email, ignore.case = TRUE))
  expect_equal(ext_email, tolower(ext_email))
})

test_that("parse_scoped_bookings praefixt booking: und synthetisiert Gast-Email", {
  appts <- list(list(
    id = "APT1", serviceName = "Beratung", status = "confirmed",
    start = list(dateTime = "2026-07-11T09:00:00Z"), end = list(dateTime = "2026-07-11T09:30:00Z"),
    joinWebUrl = "https://teams.microsoft.com/l/meetup-join/19%3ameeting_XYZ%40thread.v2/0",
    staffMemberIds = list("S1"),
    customers = list(list(name = "Kunde Ohne Mail", emailAddress = NULL),
                     list(name = "Kunde Y", emailAddress = "y@kunde.com"))))
  out <- parse_scoped_bookings(appts, staff_map = c(S1 = "rep.b@studyflix.de"))
  expect_equal(out$events$msgraph_ical_uid, "booking:APT1")
  expect_equal(out$events$subject, "Beratung")
  # Staff-Organizer + 2 Kunden (einer synthetisch)
  expect_equal(nrow(out$participants), 3)
  expect_true(any(grepl("@external.guest$", out$participants$email)))
  expect_true(out$participants$is_organizer[out$participants$email == "rep.b@studyflix.de"])
  expect_true(all(out$participants$source == "booking"))
})

test_that("parse_attendance_records extrahiert Teilnehmer mit lowercase email", {
  reports <- list(list(
    id = "R1", meetingStartDateTime = "2026-07-10T10:00:00Z", totalParticipantCount = 2,
    attendanceRecords = list(
      list(identity = list(displayName = "Rep A"), emailAddress = "REP.A@studyflix.de",
           role = "Organizer", totalAttendanceInSeconds = 1800),
      list(identity = list(displayName = "Kunde X"), emailAddress = "X@kunde.com",
           role = "Attendee", totalAttendanceInSeconds = 1700),
      list(identity = list(displayName = "Kunde EXT"),
           emailAddress = "john_doe_gmail.com#EXT#@tenant.onmicrosoft.com",
           role = "Attendee", totalAttendanceInSeconds = 1600))))
  df <- parse_attendance_records(reports, meeting_id = "MID1")
  expect_equal(nrow(df), 3)
  expect_equal(df$meeting_id[1], "MID1")
  expect_true(all(df$email == tolower(df$email)))
  known <- c("rep.a@studyflix.de", "x@kunde.com")
  expect_true(all(known %in% df$email))
  ext_email <- df$email[!df$email %in% known]
  expect_length(ext_email, 1)
  expect_false(grepl("#ext#", ext_email, ignore.case = TRUE))
  expect_equal(ext_email, tolower(ext_email))
})

test_that("vtt_to_plaintext entfernt Zeitstempel und WEBVTT-Header", {
  vtt <- "WEBVTT\n\n00:00:01.000 --> 00:00:03.000\n<v Rep A>Hallo Herr X</v>\n\n00:00:04.000 --> 00:00:05.000\n<v Kunde X>Guten Tag</v>\n"
  out <- vtt_to_plaintext(vtt)
  expect_false(grepl("-->", out))
  expect_false(grepl("WEBVTT", out))
  expect_true(grepl("Hallo Herr X", out))
  expect_true(grepl("Guten Tag", out))
})

test_that("parse_scoped_events dedupt Events per (ical, event_start) fuer den Upsert", {
  mk <- function(subj) list(
    iCalUId = "ICALDUP", type = "singleInstance",
    createdDateTime = "2026-07-01T08:00:00Z", lastModifiedDateTime = "2026-07-02T09:00:00Z",
    subject = subj, start = list(dateTime = "2026-07-10T10:00:00.0000000"),
    end = list(dateTime = "2026-07-10T10:30:00.0000000"), isCancelled = FALSE,
    isOnlineMeeting = FALSE,
    organizer = list(emailAddress = list(name = "A", address = "a@studyflix.de")),
    attendees = list())
  # dasselbe Meeting aus zwei Kalendern, minimal unterschiedlich (Subject) -> nur EINE Event-Zeile
  out <- parse_scoped_events(list(mk("Titel A"), mk("Titel B")))
  expect_equal(nrow(out$events), 1)
})

test_that("parse_scoped_bookings dedupt Events per (ical, event_start)", {
  a <- function(svc) list(id = "APTDUP", serviceName = svc, status = "confirmed",
    start = list(dateTime = "2026-07-11T09:00:00Z"), end = list(dateTime = "2026-07-11T09:30:00Z"),
    staffMemberIds = list("S1"), customers = list(list(name = "K", emailAddress = "k@kunde.com")))
  out <- parse_scoped_bookings(list(a("Svc A"), a("Svc B")), staff_map = c(S1 = "rep@studyflix.de"))
  expect_equal(nrow(out$events), 1)
})
