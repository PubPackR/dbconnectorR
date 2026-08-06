# Tests fuer assemble_crm_classification_rows() (Spec 2026-07-24, Phase 2).

make_tasks <- function() {
  data.frame(
    id          = c(1001L, 1002L, 1003L, 1004L),  # Surrogat-PK; Kommentare referenzieren DIESE Spalte
    crm_task_id = c(1L, 2L, 3L, 4L),              # CRM-Business-ID (Output-Spalte)
    lead_id     = c(10L, 20L, 30L, 40L),
    user_id     = c(100L, 100L, 200L, 200L),
    precise_time = as.POSIXct(
      c("2026-07-20 10:00:00", "2026-07-20 09:00:00",
        "2026-07-21 08:00:00", "2026-07-22 14:00:00"), tz = "UTC"),
    task_name = c("VC Webex ZR mit Frings",   # extern webex
                  "VC Teams Update",          # teams
                  "VC Zoom NV",               # extern zoom
                  "er machen"),               # KEIN VC-Termin
    stringsAsFactors = FALSE
  )
}
make_comments <- function() {
  data.frame(
    task_id = c(1001L, 1002L),  # -> crm_lead_tasks.id (Surrogat), NICHT crm_task_id
    comment_name = c("Kunde ist nicht erschienen", "Termin wurde abgesagt"),
    stringsAsFactors = FALSE
  )
}
make_user_contact <- function() {
  data.frame(user_id = c(100L, 200L), contact_id = c(5100L, 5200L))
}
make_msgraph <- function() {
  # Teams-Task 2 (lead 20, 2026-07-20) hat einen MSGraph-Match -> raus.
  data.frame(lead_id = 20L, event_date = as.Date("2026-07-20"))
}

test_that("assemble bildet nur VC-Termine, wendet Anti-Join und Status korrekt an", {
  res <- assemble_crm_classification_rows(
    crm_tasks = make_tasks(),
    crm_comments = make_comments(),
    crm_user_contact = make_user_contact(),
    msgraph_meetings = make_msgraph()
  )
  # task 4 (kein VC) raus; task 2 (teams + MSGraph-Match) raus -> bleiben 1 und 3
  expect_setequal(res$crm_task_id, c(1L, 3L))

  r1 <- res[res$crm_task_id == 1L, ]
  expect_equal(r1$meeting_tool, "webex")
  expect_true(r1$is_external_tool)
  expect_equal(r1$meeting_status, "no_show")
  expect_true(r1$is_no_show)
  expect_false(r1$excluded)
  expect_equal(r1$contact_id, 5100L)
  expect_equal(as.character(r1$crm_event_date), "2026-07-20")
  expect_equal(r1$source, "crm_task")
  expect_true(is.na(r1$call_event_mapping_id))
  expect_true(r1$is_responsible)

  r3 <- res[res$crm_task_id == 3L, ]
  expect_equal(r3$meeting_tool, "zoom")
  expect_equal(r3$meeting_status, "unbekannt")  # kein Kommentar
  expect_false(r3$is_no_show)
  expect_equal(r3$contact_id, 5200L)
})

test_that("storniertes Meeting wird excluded=TRUE, is_no_show=FALSE", {
  tasks <- make_tasks()[1, ]
  comments <- data.frame(task_id = 1001L, comment_name = "verschoben auf naechste Woche")
  res <- assemble_crm_classification_rows(
    tasks, comments, make_user_contact(),
    make_msgraph()[0, ]
  )
  expect_equal(res$meeting_status, "storniert")
  expect_true(res$excluded)
  expect_false(res$is_no_show)
})

test_that("Filter: nur Zeilen mit erkanntem Tool ODER erkanntem Status bleiben", {
  tasks <- data.frame(
    id           = c(101L, 105L, 106L),  # Surrogat-PK
    crm_task_id  = c(1L, 5L, 6L),
    lead_id      = c(10L, 50L, 60L),
    user_id      = c(100L, 100L, 100L),
    precise_time = as.POSIXct(
      c("2026-07-20 10:00:00", "2026-07-23 11:00:00", "2026-07-24 09:00:00"),
      tz = "UTC"),
    task_name    = c("VC Webex ZR",   # Tool erkannt -> bleibt
                     "VC Rueckruf",   # kein Tool, kein Status -> raus (Rauschen)
                     "VC Rueckruf"),  # kein Tool, aber Status-Kommentar -> bleibt
    stringsAsFactors = FALSE
  )
  comments <- data.frame(task_id = 106L, comment_name = "Kunde nicht erschienen",
                         stringsAsFactors = FALSE)  # -> tasks.id 106 (crm_task_id 6)
  res <- assemble_crm_classification_rows(
    crm_tasks = tasks, crm_comments = comments,
    crm_user_contact = make_user_contact(),
    msgraph_meetings = make_msgraph()[0, ]
  )
  expect_setequal(res$crm_task_id, c(1L, 6L))
  expect_equal(res$meeting_status[res$crm_task_id == 6L], "no_show")
})

test_that("Kommentar-Join greift auch mit integer64-id (bigint-Surrogat aus der DB)", {
  skip_if_not_installed("bit64")
  tasks <- make_tasks()[1, ]          # id 1001, crm_task_id 1, VC Webex
  tasks$id <- bit64::as.integer64(tasks$id)   # in der DB ist crm_lead_tasks.id bigint
  comments <- data.frame(task_id = 1001L, comment_name = "Kunde nicht erschienen",
                         stringsAsFactors = FALSE)
  res <- assemble_crm_classification_rows(
    crm_tasks = tasks, crm_comments = comments,
    crm_user_contact = make_user_contact(),
    msgraph_meetings = make_msgraph()[0, ]
  )
  # integer64-id darf den Match nicht zerstoeren -> Status greift
  expect_equal(res$meeting_status, "no_show")
  expect_true(res$is_no_show)
})

test_that("Zeile wird verworfen, wenn der Rep-Kontakt nicht auflösbar ist", {
  tasks <- make_tasks()[1, ]
  tasks$user_id <- 999L  # nicht in crm_user_contact enthalten
  res <- assemble_crm_classification_rows(
    crm_tasks = tasks,
    crm_comments = make_comments(),
    crm_user_contact = make_user_contact(),
    msgraph_meetings = make_msgraph()[0, ]
  )
  expect_equal(nrow(res), 0)
})
