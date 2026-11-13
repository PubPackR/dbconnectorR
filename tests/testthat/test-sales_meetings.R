# ---- Fixtures ------------------------------------------------------------------
#
# Eine kleine Welt (Spec 2026-11-11-sales-meetings):
#   1  aktiv, Kunde a@kunde.de (Lead 10), zwei Calls: 30 min extern, 10 min intern
#   2  abgesagt ohne meeting_id, b@firma.de (Lead 20), 2 h nach Anlage abgesagt
#   3  aktiv, neue ical_uid, b@firma.de, einen Tag spaeter: Ersatz fuer 2
#   4  nur Interne: gehoert nicht zur Startmenge
#   5  Bookings-Eintrag (booking:), c@kunde.de (Lead 30)
#   6  Kalenderkopie (Bookings-Postfach als Teilnehmer), c@kunde.de
#   7  aktiv, a@kunde.de 10 min nach 1, ohne Call: Duplikat von 1

ts <- function(x) as.POSIXct(x, tz = "UTC")

mk_world <- function() {
  events <- data.frame(
    id                 = c(1, 2, 3, 4, 5, 6, 7),
    msgraph_ical_uid   = c("u1", "u2", "u3", "u4", "booking:abc", "u6", "u7"),
    event_created_at   = ts(c("2026-06-01 08:00", "2026-06-02 08:00", "2026-06-10 08:00",
                              "2026-06-01 08:00", "2026-07-05 09:00", "2026-06-20 08:00",
                              "2026-06-01 08:00")),
    event_updated_at   = ts(c("2026-06-01 08:00", "2026-06-02 10:00", "2026-06-10 08:00",
                              "2026-06-01 08:00", "2026-07-05 09:00", "2026-06-20 08:00",
                              "2026-06-01 08:00")),
    event_start        = ts(c("2026-07-01 09:00", "2026-07-02 09:00", "2026-07-03 09:00",
                              "2026-07-01 12:00", "2026-07-05 09:00", "2026-07-05 09:00",
                              "2026-07-01 09:10")),
    is_canceled        = c(FALSE, TRUE, FALSE, FALSE, FALSE, FALSE, FALSE),
    is_single_instance = TRUE,
    join_url           = NA_character_,
    join_url_checked_at = ts(NA),
    vanished_at        = ts(NA),
    created_at         = ts(c("2026-06-01 09:00", "2026-06-02 09:00", "2026-06-10 09:00",
                              "2026-06-01 09:00", "2026-07-05 09:00", "2026-06-20 09:00",
                              "2026-06-01 09:00")),
    online_meeting_lookup = NA_character_,
    stringsAsFactors = FALSE)

  p <- function(event, contact, email, org = FALSE) data.frame(
    event_id = event, contact_id = contact, is_organizer = org, email = email,
    stringsAsFactors = FALSE)
  participants <- rbind(
    p(1, 500, "rep@studyflix.de", TRUE), p(1, 900, "a@kunde.de"),
    p(2, 500, "rep@studyflix.de", TRUE), p(2, 901, "b@firma.de"),
    p(3, 500, "rep@studyflix.de", TRUE), p(3, 901, "b@firma.de"),
    p(4, 500, "rep@studyflix.de", TRUE), p(4, 501, "kollegin@studyflix.de"),
    p(5, 500, "rep@studyflix.de", TRUE), p(5, 902, "c@kunde.de"),
    p(6, 999, BOOKINGS_MAILBOX, TRUE), p(6, 500, "rep@studyflix.de"), p(6, 902, "c@kunde.de"),
    p(7, 500, "rep@studyflix.de", TRUE), p(7, 900, "a@kunde.de"))

  call_event <- data.frame(call_id = c(50, 51), event_id = c(1, 1),
                           event_class = c("extern_planned_extern_call", "extern_planned_intern_call"),
                           stringsAsFactors = FALSE)
  calls <- data.frame(id = c(50, 51),
                      call_start = ts(c("2026-07-01 09:02", "2026-07-01 08:40")),
                      call_end   = ts(c("2026-07-01 09:32", "2026-07-01 08:50")))
  call_participants <- data.frame(call_id = c(50, 50, 51), contact_id = c(500, 900, 500),
                                  email = c("rep@studyflix.de", "a@kunde.de", "rep@studyflix.de"),
                                  stringsAsFactors = FALSE)
  speaker_counts <- data.frame(call_id = c("50", "51"), external_speaker_count = c(2L, 1L),
                               stringsAsFactors = FALSE)
  lead_mappings <- data.frame(msgraph_contact_id = c(900, 901, 902), crm_lead_id = c(10, 20, 30))
  crm_mails <- data.frame(lead_id = numeric(0), mail_address_name = character(0),
                          is_deleted = logical(0))

  t <- function(id, crm, lead, ptime) data.frame(
    id = id, crm_task_id = crm, lead_id = lead, precise_time = ts(ptime),
    task_created_at = ts("2026-06-15 08:00"), task_name = "Teams NV",
    meeting_tool = "teams", is_external_tool = FALSE, meeting_type = "nv",
    event_date = as.Date(ts(ptime), tz = "Europe/Berlin"), contact_id = 500,
    stringsAsFactors = FALSE)
  tasks <- rbind(t(1001, 7001, 10, "2026-07-01 09:00"),   # trifft Event 1
                 t(1002, 7002, 40, "2026-07-08 09:00"),   # kein Event: CRM-Zeile
                 t(1003, 7003, 30, "2026-07-05 09:00"))   # nur Bookings-Zeilen: CRM-Zeile (Q1)
  comments <- data.frame(task_id = 1001, crm_comment_id = 1, comment_name = "hat stattgefunden",
                         comment_created_at = ts("2026-07-01 10:00"),
                         created_at = ts("2026-07-01 10:00"), stringsAsFactors = FALSE)

  list(events = events, participants = participants, call_event = call_event, calls = calls,
       call_participants = call_participants, speaker_counts = speaker_counts,
       lead_mappings = lead_mappings, crm_mails = crm_mails, tasks = tasks, comments = comments)
}

build_world <- function(w = mk_world()) {
  build_sales_meetings(w$events, w$participants, w$call_event, w$calls, w$call_participants,
                       w$speaker_counts, w$lead_mappings, w$crm_mails, w$tasks, w$comments,
                       tenant_id = NULL, now_utc = ts("2026-08-01 00:00"))
}

row_of <- function(df, key) df[df$meeting_key == key, , drop = FALSE]

# ---- build_sales_meetings: Startmenge und Grain ----------------------------------

test_that("Startmenge: jede Kalenderzeile mit Externem, auch abgesagt und ohne meeting_id, plus CRM-only", {
  m <- build_world()$meetings
  expect_setequal(m$meeting_key, c("msgraph_1", "msgraph_2", "msgraph_3", "msgraph_5",
                                   "msgraph_6", "msgraph_7", "crm_7002", "crm_7003"))
  expect_false(anyDuplicated(m$meeting_key) > 0)
})

test_that("ein Task, der eine Kalenderzeile trifft, erzeugt keine CRM-Zeile (Q1)", {
  m <- build_world()$meetings
  expect_false("crm_7001" %in% m$meeting_key)
})

test_that("Bookings-Eintrag und Kalenderkopie sind keine Kandidaten: der Task bleibt CRM-Zeile (Q1, Q3)", {
  m <- build_world()$meetings
  expect_equal(row_of(m, "msgraph_5")$booking_role, "booking_entry")
  expect_equal(row_of(m, "msgraph_6")$booking_role, "calendar_copy")
  expect_true(is.na(row_of(m, "msgraph_1")$booking_role))
  expect_true("crm_7003" %in% m$meeting_key)
  # Das Bookings-Postfach ist intern und zaehlt nicht als Externer.
  expect_equal(row_of(m, "msgraph_6")$external_participant_count, 1L)
})

# ---- Kalenderzustand, Kette, Legedatum -----------------------------------------------

test_that("abgesagte Zeile mit Ersatz-Termin ist verlegt, der Ersatz ihr Nachfolger (Q2)", {
  m <- build_world()$meetings
  expect_equal(row_of(m, "msgraph_2")$calendar_status, "rescheduled")
  expect_equal(row_of(m, "msgraph_2")$successor_key, "msgraph_3")
  expect_equal(row_of(m, "msgraph_3")$calendar_status, "active")
  expect_equal(row_of(m, "msgraph_2")$chain_root_key, "msgraph_2")
  expect_equal(row_of(m, "msgraph_3")$chain_root_key, "msgraph_2")
  expect_equal(row_of(m, "msgraph_1")$chain_root_key, "msgraph_1")
})

test_that("Ersatztermin erbt das fruehere Legedatum, Bookings-Eintrag den Beginn (Q4, Q10)", {
  m <- build_world()$meetings
  expect_equal(row_of(m, "msgraph_3")$booked_date, as.Date("2026-06-02"))
  expect_equal(row_of(m, "msgraph_3")$booked_date_source, "outlook")
  expect_equal(row_of(m, "msgraph_5")$booked_date, as.Date("2026-07-05"))
  expect_equal(row_of(m, "msgraph_5")$booked_date_source, "bookings_start")
  expect_equal(row_of(m, "crm_7002")$booked_date_source, "crm_task")
})

test_that("kurzlebig: abgesagt kurz nach Anlage; Bookings-Eintrag und CRM leer (Q10)", {
  m <- build_world()$meetings
  expect_true(row_of(m, "msgraph_2")$is_short_lived)
  expect_false(row_of(m, "msgraph_1")$is_short_lived)
  expect_true(is.na(row_of(m, "msgraph_5")$is_short_lived))
  expect_true(is.na(row_of(m, "crm_7002")$is_short_lived))
})

test_that("kurzlebig misst bis vanished_at, wenn gesetzt", {
  w <- mk_world()
  w$events$vanished_at[2] <- ts("2026-06-05 08:00")
  m <- build_world(w)$meetings
  expect_false(row_of(m, "msgraph_2")$is_short_lived)
})

# ---- Teams --------------------------------------------------------------------------

test_that("eine externe Sitzung reicht; Call-Spalten vom laengsten Call (Q9, Q16)", {
  m <- build_world()$meetings
  r <- row_of(m, "msgraph_1")
  expect_equal(r$teams_status, "external_joined")
  expect_equal(r$call_duration_minutes, 30)
  expect_equal(r$call_started_at_utc, ts("2026-07-01 09:02"))
  expect_equal(r$external_speaker_count, 2L)
  expect_equal(row_of(m, "msgraph_3")$teams_status, "no_call")
  expect_true(is.na(row_of(m, "msgraph_3")$external_speaker_count))
  expect_equal(row_of(m, "crm_7002")$teams_status, "not_in_calendar")
  expect_equal(row_of(m, "crm_7002")$calendar_status, "not_in_calendar")
})

test_that("Termin in der Zukunft ist future", {
  w <- mk_world()
  m <- build_sales_meetings(w$events, w$participants, w$call_event, w$calls, w$call_participants,
                            w$speaker_counts, w$lead_mappings, w$crm_mails, w$tasks, w$comments,
                            now_utc = ts("2026-07-02 00:00"))$meetings
  expect_equal(row_of(m, "msgraph_3")$teams_status, "future")
})

# ---- Duplikate ----------------------------------------------------------------------

test_that("Duplikat: der Termin ohne Call verliert; nicht gepruefte Zeilen sind FALSE (Q2, Q20)", {
  m <- build_world()$meetings
  expect_true(row_of(m, "msgraph_7")$is_duplicate)
  expect_false(row_of(m, "msgraph_1")$is_duplicate)
  expect_false(row_of(m, "msgraph_2")$is_duplicate)
  expect_true(is.na(row_of(m, "crm_7002")$is_duplicate))
})

# ---- Personen und Leads -------------------------------------------------------------

test_that("verantwortlich ist der Organisator, der im Call war", {
  m <- build_world()$meetings
  expect_equal(row_of(m, "msgraph_1")$responsible_contact_id, "500")
  expect_equal(row_of(m, "msgraph_1")$organizer_source, "msgraph")
  expect_equal(row_of(m, "crm_7002")$responsible_contact_id, "500")
  expect_equal(row_of(m, "crm_7002")$organizer_source, "crm_task")
})

test_that("organizer_is_external: externer Organisator TRUE, interner gewinnt, CRM-Zeile NA (Q14)", {
  w <- mk_world()
  # Event 1: der Kunde organisiert. Event 3: zusaetzlich ein externer Organisator,
  # der interne gewinnt.
  w$participants$is_organizer[w$participants$event_id == 1] <-
    w$participants$email[w$participants$event_id == 1] == "a@kunde.de"
  w$participants <- rbind(w$participants, data.frame(
    event_id = 3, contact_id = 903, is_organizer = TRUE, email = "x@firma.de",
    stringsAsFactors = FALSE))
  m <- build_world(w)$meetings
  expect_true(row_of(m, "msgraph_1")$organizer_is_external)
  expect_false(row_of(m, "msgraph_3")$organizer_is_external)
  expect_false(row_of(m, "msgraph_6")$organizer_is_external)  # Bookings-Postfach ist intern
  expect_true(is.na(row_of(m, "crm_7002")$organizer_is_external))
})

test_that("der Writer schreibt organizer_is_external, und eine fehlende Spalte bricht ab", {
  expect_true("organizer_is_external" %in% SALES_MEETINGS_SPALTEN)
  expect_true(all(SALES_MEETINGS_SPALTEN %in% names(build_world()$meetings) |
                    SALES_MEETINGS_SPALTEN %in% c("organizer_person_id", "organizer_type",
                                                  "responsible_person_id")))
  expect_error(pruefe_zielspalten(setdiff(SALES_MEETINGS_SPALTEN, "organizer_is_external"),
                                  SALES_MEETINGS_SPALTEN, "processed.sales_meetings"),
               "organizer_is_external")
  expect_true(pruefe_zielspalten(SALES_MEETINGS_SPALTEN, SALES_MEETINGS_SPALTEN, "x"))
})

test_that("Leads: je Record und Lead eine Zeile mit dem Task, der zaehlt", {
  l <- build_world()$leads
  key <- paste(l$meeting_key, l$lead_id)
  expect_setequal(key, c("msgraph_1 10", "msgraph_7 10", "msgraph_2 20", "msgraph_3 20",
                         "msgraph_5 30", "msgraph_6 30", "crm_7002 40", "crm_7003 30"))
  r1 <- l[key == "msgraph_1 10", ]
  expect_equal(r1$crm_lead_task_id, "1001")
  expect_equal(r1$crm_outcome, "held")
  expect_equal(r1$lead_match_source, "mapping")
  expect_equal(l$crm_outcome[key == "msgraph_3 20"], "no_task")
  expect_equal(l$crm_outcome[key == "crm_7002 40"], "no_outcome")
  expect_equal(l$lead_match_source[key == "crm_7002 40"], "crm_task")
})

test_that("Ersatzregel: Zeile ohne Lead bekommt den Lead des Tasks", {
  w <- mk_world()
  w$lead_mappings <- w$lead_mappings[w$lead_mappings$msgraph_contact_id != 901, ]
  w$tasks <- rbind(w$tasks, transform(w$tasks[1, ], id = 1004, crm_task_id = 7004, lead_id = 20,
                                      precise_time = ts("2026-07-03 09:05"),
                                      event_date = as.Date("2026-07-03")))
  r <- build_world(w)
  l <- r$leads
  expect_equal(l$lead_match_source[l$meeting_key == "msgraph_3"], "substitute_rule")
  expect_equal(l$crm_lead_task_id[l$meeting_key == "msgraph_3"], "1004")
  expect_false("crm_7004" %in% r$meetings$meeting_key)
})

# ---- CRM-Zuordnung: aktive Zeile in der Kette, sonst statt Mehrdeutigkeit (Q21, Q25) ----

mk_kandidaten_task <- function(ptime) data.frame(
  crm_task_id = 1, lead_id = 10, event_date = as.Date("2026-07-02"),
  precise_time = ts(ptime), contact_id = 500, is_external_tool = FALSE,
  stringsAsFactors = FALSE)

# Zwei Kandidaten mit gleichem Lead, Tag und Rep: Zeile 1 um 09:00, Zeile 2 um 14:00.
kand_lead  <- c(10, 10)
kand_tag   <- as.Date(c("2026-07-02", "2026-07-02"))
kand_rep   <- c(500, 500)
kand_start <- ts(c("2026-07-02 09:00", "2026-07-02 14:00"))

test_that("match_crm_tasks: ohne aktiv gilt das alte Verhalten", {
  r <- match_crm_tasks(mk_kandidaten_task("2026-07-02 09:00"), kand_lead, kand_tag, kand_rep,
                       kand_start)
  expect_equal(r$row, 1L)
  expect_equal(r$match_rule, "lead_tag_naechste_zeit")
})

test_that("match_crm_tasks: in einer Kette gewinnt die aktive Zeile vor Rep und Startzeit (Q21)", {
  r <- match_crm_tasks(mk_kandidaten_task("2026-07-02 09:00"), kand_lead, kand_tag, kand_rep,
                       kand_start, aktiv = c(FALSE, TRUE), kette = c("k1", "k1"))
  expect_equal(r$row, 2L)
  expect_equal(r$match_rule, "lead_tag_aktiv")
  expect_equal(r$candidate_count, 2L)
})

test_that("match_crm_tasks: ueber Ketten hinweg entscheiden erst Rep und Startzeit (Q25)", {
  task <- mk_kandidaten_task("2026-07-02 09:00")
  r <- match_crm_tasks(task, kand_lead, kand_tag, kand_rep, kand_start,
                       aktiv = c(FALSE, TRUE), kette = c("k1", "k2"))
  expect_equal(r$row, 1L)
  expect_equal(r$match_rule, "lead_tag_naechste_zeit")
  # Ohne kette gilt jede Zeile als eigener Termin, ebenso mit NA.
  expect_equal(match_crm_tasks(task, kand_lead, kand_tag, kand_rep, kand_start,
                               aktiv = c(FALSE, TRUE))$row, 1L)
  expect_equal(match_crm_tasks(task, kand_lead, kand_tag, kand_rep, kand_start,
                               aktiv = c(FALSE, TRUE), kette = c(NA, NA))$row, 1L)
})

test_that("match_crm_tasks: bleibt es mehrdeutig, gewinnt die einzige aktive Zeile statt keiner (Q25)", {
  gleich <- ts(c("2026-07-02 09:00", "2026-07-02 09:00"))
  task <- mk_kandidaten_task("2026-07-02 09:00")
  expect_equal(match_crm_tasks(task, kand_lead, kand_tag, kand_rep, gleich)$match_rule,
               "mehrdeutig")
  r <- match_crm_tasks(task, kand_lead, kand_tag, kand_rep, gleich,
                       aktiv = c(TRUE, FALSE), kette = c("k1", "k2"))
  expect_equal(r$row, 1L)
  expect_equal(r$match_rule, "mehrdeutig_aktiv")
  # Kein Rep passt: ebenfalls die aktive Zeile.
  r2 <- match_crm_tasks(task, kand_lead, kand_tag, c(600, 700), kand_start,
                        aktiv = c(FALSE, TRUE), kette = c("k1", "k2"))
  expect_equal(r2$row, 2L)
  expect_equal(r2$match_rule, "mehrdeutig_aktiv")
})

test_that("match_crm_tasks: zwei aktive Kandidaten fallen auf Rep und Startzeit zurueck", {
  r <- match_crm_tasks(mk_kandidaten_task("2026-07-02 13:30"), kand_lead, kand_tag, kand_rep,
                       kand_start, aktiv = c(TRUE, TRUE), kette = c("k1", "k1"))
  expect_equal(r$row, 2L)
  expect_equal(r$match_rule, "lead_tag_naechste_zeit")
  gleich <- ts(c("2026-07-02 09:00", "2026-07-02 09:00"))
  expect_equal(match_crm_tasks(mk_kandidaten_task("2026-07-02 09:00"), kand_lead, kand_tag,
                               kand_rep, gleich, aktiv = c(TRUE, TRUE))$match_rule, "mehrdeutig")
})

test_that("match_crm_tasks: ein einzelner Kandidat bleibt lead_tag, auch wenn er nicht aktiv ist", {
  r <- match_crm_tasks(mk_kandidaten_task("2026-07-02 09:00"), 10, as.Date("2026-07-02"), 500,
                       ts("2026-07-02 09:00"), aktiv = FALSE, kette = "k1")
  expect_equal(r$row, 1L)
  expect_equal(r$match_rule, "lead_tag")
})

test_that("match_crm_tasks: aktiv oder kette in falscher Laenge bricht ab", {
  task <- mk_kandidaten_task("2026-07-02 09:00")
  expect_error(match_crm_tasks(task, kand_lead, kand_tag, kand_rep, kand_start, aktiv = TRUE),
               "aktiv")
  expect_error(match_crm_tasks(task, kand_lead, kand_tag, kand_rep, kand_start,
                               aktiv = c(TRUE, FALSE), kette = "k1"),
               "kette")
})

test_that("Task zur alten Uhrzeit eines am selben Tag verlegten Termins landet an der aktiven Zeile (Q21)", {
  w <- mk_world()
  # Ersatz 3 liegt jetzt am selben Tag wie die abgesagte Zeile 2, fuenf Stunden spaeter.
  w$events$event_start[3] <- ts("2026-07-02 14:00")
  w$tasks <- rbind(w$tasks, transform(w$tasks[1, ], id = 1005, crm_task_id = 7005, lead_id = 20,
                                      precise_time = ts("2026-07-02 09:00"),
                                      event_date = as.Date("2026-07-02")))
  r <- build_world(w)
  m <- r$meetings
  expect_equal(row_of(m, "msgraph_2")$calendar_status, "rescheduled")
  expect_equal(row_of(m, "msgraph_3")$calendar_status, "active")
  expect_false("crm_7005" %in% m$meeting_key)
  l <- r$leads
  key <- paste(l$meeting_key, l$lead_id)
  expect_equal(l$crm_lead_task_id[key == "msgraph_3 20"], "1005")
  expect_equal(l$crm_outcome[key == "msgraph_2 20"], "no_task")
})

# ---- derive_meeting_chains ------------------------------------------------------------

test_that("Kette mit zwei Anfaengen: erste Zeile ist die kleinste msgraph_event_id (Q19)", {
  root <- derive_meeting_chains(
    meeting_key   = c("msgraph_10", "msgraph_5", "msgraph_20", "msgraph_30", "crm_1"),
    successor_key = c("msgraph_20", "msgraph_20", NA, NA, NA),
    order_value   = c(10, 5, 20, 30, NA))
  expect_equal(root, c("msgraph_5", "msgraph_5", "msgraph_5", "msgraph_30", "crm_1"))
})

test_that("Kette ueber zwei Schritte landet bei derselben ersten Zeile", {
  root <- derive_meeting_chains(c("a", "b", "c"), c("b", "c", NA), c(1, 2, 3))
  expect_equal(root, c("a", "a", "a"))
})

# ---- attach_meeting_persons -----------------------------------------------------------

test_that("Personen: unbekannte Person-ID wird NA, Zeilenzahl muss stimmen", {
  m <- build_world()$meetings
  rollen <- function(rows) transform(rows, responsible_person_id = 1L, organizer_person_id = 99L,
                                     organizer_type = "sales")
  r <- attach_meeting_persons(m, rollen, person_ids = c(1, 2))
  expect_true(all(r$responsible_person_id == "1"))
  expect_true(all(is.na(r$organizer_person_id)))
  expect_error(attach_meeting_persons(m, function(rows) rows[1, ], person_ids = 1),
               "person_roles")
})

# ---- Umzugskopien und Legedatum (Spec 2026-11-13-mailbox-copy-status) -----------------
#
# Zusaetzlich zur kleinen Welt, alle mit Organisator rep@studyflix.de:
#   21  angelegt 10.09. 08:00, Termin 02.09.: vorbei, ohne Original und Task
#   22  angelegt 10.09. 08:10, Termin 05.10., Original 23 (01.07.) mit demselben Kunden
#   24  angelegt 10.09. 08:20, Termin 08.09., zwei Stunden spaeter abgesagt, Task vom 20.08.
#       21, 22 und 24 bilden eine Umzugs-Sitzung (21 und 24 lagen vorher)
#   25  angelegt 08.09., Termin 09.09.: Ersatz fuer 24, selbst kein Verdacht
#   26  angelegt 25.08., dieselbe ical_uid wie 27, aeltere Fassung (stale_copy)
#   27  angelegt 15.09., Termin 05.09.: vorbei, Task vom 01.09. liegt nach 26 (Q17)

mk_umzug_world <- function() {
  w <- mk_world()
  e <- function(id, uid, angelegt, geaendert, beginn, abgesagt = FALSE) data.frame(
    id = id, msgraph_ical_uid = uid, event_created_at = ts(angelegt),
    event_updated_at = ts(geaendert), event_start = ts(beginn), is_canceled = abgesagt,
    is_single_instance = TRUE, join_url = NA_character_, join_url_checked_at = ts(NA),
    vanished_at = ts(NA), created_at = ts(angelegt) + 3600, online_meeting_lookup = NA_character_,
    stringsAsFactors = FALSE)
  w$events <- rbind(w$events,
    e(21, "u21", "2026-09-10 08:00", "2026-09-10 08:00", "2026-09-02 09:00"),
    e(22, "u22", "2026-09-10 08:10", "2026-09-10 08:10", "2026-10-05 09:00"),
    e(23, "u23", "2026-07-01 08:00", "2026-07-01 08:00", "2026-10-05 09:00"),
    e(24, "u24", "2026-09-10 08:20", "2026-09-10 10:20", "2026-09-08 09:00", abgesagt = TRUE),
    e(25, "u25", "2026-09-08 07:00", "2026-09-08 07:00", "2026-09-09 09:00"),
    e(26, "u26", "2026-08-25 08:00", "2026-08-25 08:00", "2026-09-05 09:00"),
    e(27, "u26", "2026-09-15 08:00", "2026-09-15 08:00", "2026-09-05 09:00"))
  p <- function(event, contact, email, org = FALSE) data.frame(
    event_id = event, contact_id = contact, is_organizer = org, email = email,
    stringsAsFactors = FALSE)
  rep_org <- function(event) p(event, 500, "rep@studyflix.de", TRUE)
  w$participants <- rbind(w$participants,
    rep_org(21), p(21, 921, "k21@kunde.de"),
    rep_org(22), p(22, 922, "k22@kunde.de"),
    rep_org(23), p(23, 922, "k22@kunde.de"),
    rep_org(24), p(24, 924, "k24@kunde.de"),
    rep_org(25), p(25, 924, "k24@kunde.de"),
    rep_org(26), p(26, 926, "k26@kunde.de"),
    rep_org(27), p(27, 926, "k26@kunde.de"))
  w$lead_mappings <- rbind(w$lead_mappings,
    data.frame(msgraph_contact_id = c(921, 922, 924, 926), crm_lead_id = c(121, 122, 124, 126)))
  # Argumentnamen anders als die Spalten: transform() saehe sonst die Spalte.
  task <- function(task_id, crm, lead, ptime, angelegt) transform(
    w$tasks[1, ], id = task_id, crm_task_id = crm, lead_id = lead, precise_time = ts(ptime),
    event_date = as.Date(ts(ptime), tz = "Europe/Berlin"), task_created_at = ts(angelegt))
  w$tasks <- rbind(w$tasks,
    task(1024, 7024, 124, "2026-09-08 09:00", "2026-08-20 08:00"),
    task(1026, 7026, 126, "2026-09-05 09:00", "2026-09-01 08:00"))
  w
}

test_that("Umzugskopien: Status je Kalenderzeile, CRM-Zeilen und booking:-Eintraege leer (Q2, Q5, Q9)", {
  m <- build_world(mk_umzug_world())$meetings
  status <- function(key) row_of(m, key)$mailbox_copy_status
  expect_equal(status("msgraph_21"), "suspected")
  expect_equal(status("msgraph_22"), "has_earlier_original")
  expect_equal(status("msgraph_24"), "evidenced")
  expect_equal(status("msgraph_27"), "evidenced")
  for (key in c("msgraph_1", "msgraph_5", "msgraph_23", "msgraph_25", "msgraph_26", "crm_7002")) {
    expect_true(is.na(status(key)), info = key)
  }
  expect_equal(row_of(m, "msgraph_26")$calendar_status, "stale_copy")
})

test_that("Legedatum: Original erbt, belegte Kopie nimmt den Task, die aeltere ical_uid gewinnt (Q4, Q17)", {
  m <- build_world(mk_umzug_world())$meetings
  expect_equal(row_of(m, "msgraph_21")$booked_date, as.Date("2026-09-10"))
  expect_equal(row_of(m, "msgraph_21")$booked_date_source, "outlook")
  expect_equal(row_of(m, "msgraph_22")$booked_date, as.Date("2026-07-01"))
  expect_equal(row_of(m, "msgraph_22")$booked_date_source, "outlook")
  expect_equal(row_of(m, "msgraph_24")$booked_date, as.Date("2026-08-20"))
  expect_equal(row_of(m, "msgraph_24")$booked_date_source, "crm_task")
  expect_equal(row_of(m, "msgraph_27")$booked_date, as.Date("2026-08-25"))
  expect_equal(row_of(m, "msgraph_27")$booked_date_source, "outlook")
})

test_that("Das CRM-Datum gilt vor Kurzlebigkeit und Ersatz-Erbe (Q12, Q16)", {
  m <- build_world(mk_umzug_world())$meetings
  expect_equal(row_of(m, "msgraph_24")$calendar_status, "rescheduled")
  expect_equal(row_of(m, "msgraph_24")$successor_key, "msgraph_25")
  expect_false(row_of(m, "msgraph_24")$is_short_lived)
  expect_equal(row_of(m, "msgraph_25")$booked_date, as.Date("2026-08-20"))
  expect_equal(row_of(m, "msgraph_25")$booked_date_source, "crm_task")

  # Gegenprobe ohne Task: 24 bleibt Verdacht, zwei Stunden nach dem Umzugstag abgesagt.
  w <- mk_umzug_world()
  w$tasks <- w$tasks[w$tasks$id != 1024, ]
  m <- build_world(w)$meetings
  expect_equal(row_of(m, "msgraph_24")$mailbox_copy_status, "suspected")
  expect_true(row_of(m, "msgraph_24")$is_short_lived)
  expect_equal(row_of(m, "msgraph_25")$booked_date_source, "outlook")
})

test_that("der Writer schreibt mailbox_copy_status", {
  expect_true("mailbox_copy_status" %in% SALES_MEETINGS_SPALTEN)
  expect_true("mailbox_copy_status" %in% names(build_world()$meetings))
})
