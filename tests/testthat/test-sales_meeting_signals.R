# ---- Fixtures ------------------------------------------------------------------

mk_meetings <- function() data.frame(
  call_event_mapping_id = c(10L, 11L, 12L, 13L),
  event_id              = c("100", "110", "120", "130"),
  event_date            = as.Date(c("2026-07-01", "2026-07-02", "2026-07-03", "2026-07-04")),
  event_start           = as.POSIXct(c("2026-07-01 09:00", "2026-07-02 10:00",
                                       "2026-07-03 11:00", "2026-07-04 12:00"), tz = "UTC"),
  contact_id            = c(500L, 500L, 600L, 500L),
  lead_id               = c("10", "20", "30", NA),
  lead_source           = c("mapping", "domain", "mapping", NA),
  is_no_show            = c(FALSE, FALSE, TRUE, TRUE),
  original_created_at   = as.POSIXct(c("2026-06-01", "2026-06-02", "2026-06-03", "2026-06-04"), tz = "UTC"),
  excluded              = c(FALSE, FALSE, FALSE, TRUE),
  exclusion_reason      = c(NA, NA, NA, "duplikat_event"),
  is_short_lived_event  = FALSE, is_responsible = TRUE,
  organizer_contact_id  = c("500", NA, "600", "500"),
  stringsAsFactors = FALSE
)

mk_task <- function(id, crm_id, lead, date, rep = 500L, ptime = NA_character_,
                    tool = "teams", ext = FALSE) data.frame(
  id = id, crm_task_id = crm_id, lead_id = lead, event_date = as.Date(date),
  precise_time = as.POSIXct(ptime, tz = "UTC"), contact_id = rep,
  task_created_at = as.POSIXct("2026-06-15", tz = "UTC"),
  meeting_tool = tool, meeting_type = "nv", is_external_tool = ext,
  stringsAsFactors = FALSE)

mk_comment <- function(task_id, cid, text, at = NA_character_, created = "2026-07-10 08:00") data.frame(
  task_id = task_id, crm_comment_id = cid, comment_name = text,
  comment_created_at = as.POSIXct(at, tz = "UTC"),
  created_at = as.POSIXct(created, tz = "UTC"), stringsAsFactors = FALSE)

mk_event_signals <- function(ids = c("100", "110", "120", "130")) data.frame(
  event_id = ids, outlook_status = "aktiv", call_status = "extern_call",
  is_booking = FALSE, online_meeting_lookup = NA_character_, stringsAsFactors = FALSE)

# ---- crm_task_outcomes -----------------------------------------------------------

test_that("spaeterer Kommentar mit erkanntem Status gewinnt (Q14)", {
  k <- rbind(mk_comment(1, 1, "verschoben", "2026-07-01 08:00"),
             mk_comment(1, 2, "hat stattgefunden", "2026-07-02 08:00"))
  r <- crm_task_outcomes(1, k)
  expect_equal(r$crm_status, "show_up")
  expect_true(r$crm_status_conflict)
  expect_equal(r$crm_comment_count, 2L)
})

test_that("spaeterer Kommentar ohne Stichwort ist kein Signal (Q16)", {
  k <- rbind(mk_comment(1, 1, "storniert", "2026-07-01 08:00"),
             mk_comment(1, 2, "Angebot schicken", "2026-07-02 08:00"))
  r <- crm_task_outcomes(1, k)
  expect_equal(r$crm_status, "storniert")
  expect_false(r$crm_status_conflict)
})

test_that("ohne comment_created_at zaehlt created_at, bei Gleichstand die hoehere crm_comment_id (Q17)", {
  k <- rbind(mk_comment(1, 5, "nicht erschienen", NA, "2026-07-03 08:00"),
             mk_comment(1, 4, "hat stattgefunden", "2026-07-02 08:00"))
  expect_equal(crm_task_outcomes(1, k)$crm_status, "no_show")

  gleich <- rbind(mk_comment(2, 7, "nicht erschienen", "2026-07-02 08:00"),
                  mk_comment(2, 8, "hat stattgefunden", "2026-07-02 08:00"))
  expect_equal(crm_task_outcomes(2, gleich)$crm_status, "show_up")
})

test_that("Task ohne Kommentar: ohne_ausgang, 0 Kommentare", {
  r <- crm_task_outcomes(c(1, 2), mk_comment(1, 1, "hat stattgefunden", "2026-07-02 08:00"))
  expect_equal(r$crm_status, c("show_up", "ohne_ausgang"))
  expect_equal(r$crm_comment_count, c(1L, 0L))
})

# ---- match_crm_tasks / assemble_task_mapping -----------------------------------------

test_that("Mapping fuehrt Regel, Kandidaten und Abstand je Task", {
  m <- mk_meetings()
  m <- rbind(m, transform(m[1, ], call_event_mapping_id = 14L, event_id = "140",
                          event_start = as.POSIXct("2026-07-01 15:00", tz = "UTC")))
  tasks <- rbind(
    mk_task(1, 901, 20L, "2026-07-02"),                                  # lead_tag
    mk_task(2, 902, 10L, "2026-07-01", ptime = "2026-07-01 14:30"),      # naechste_zeit -> 140
    mk_task(3, 903, 10L, "2026-07-01"),                                  # mehrdeutig (keine Zeit)
    mk_task(4, 904, 77L, "2026-07-04", ptime = "2026-07-04 12:10"),      # ersatzregel -> 130
    mk_task(5, 905, 88L, "2026-08-01"),                                  # netto_neu
    mk_task(6, 906, 10L, "2026-07-01", tool = "zoom", ext = TRUE),       # externes_tool
    mk_task(7, 907, NA,  "2026-07-01"))                                  # ohne_lead
  r <- assemble_task_mapping(tasks, m)

  expect_equal(r$match_rule, c("lead_tag", "lead_tag_naechste_zeit", "mehrdeutig",
                               "ersatzregel", "netto_neu", "externes_tool", "ohne_lead"))
  expect_equal(r$msgraph_event_id, c("110", "140", NA, "130", NA, NA, NA))
  expect_equal(r$candidate_count, c(1L, 2L, 2L, 1L, 0L, NA, NA))
  expect_equal(r$start_diff_minutes[c(2, 4)], c(30, 10))
  expect_equal(r$lead_source[c(1, 4)], c("domain", "ersatzregel"))
  expect_true(all(is.na(r$start_diff_minutes[c(1, 3, 5, 6, 7)])))
})

test_that("Zuordnung entspricht assemble_unified_meetings", {
  m <- mk_meetings()
  tasks <- rbind(mk_task(1, 901, 10L, "2026-07-01"),
                 mk_task(4, 904, 77L, "2026-07-04", ptime = "2026-07-04 12:10"))
  crm <- transform(tasks, meeting_status = "show_up", original_created_at = task_created_at)
  uni <- assemble_unified_meetings(m, crm)
  map <- assemble_task_mapping(tasks, m)
  expect_equal(uni$no_show_source[uni$event_id %in% map$msgraph_event_id],
               c("crm_override", "crm_override"))
})

# ---- derive_outlook_status / derive_call_status -------------------------------------

test_that("outlook_status: verschoben nur bei aktiver gleicher ical_uid", {
  expect_equal(derive_outlook_status(c(FALSE, TRUE, TRUE), c("a", "b", "c"), c("a", "b")),
               c("aktiv", "verschoben", "abgesagt"))
})

test_that("call_status folgt der Rangfolge, ein Call sticht Alt-Tenant", {
  now <- as.POSIXct("2026-10-01 12:00", tz = "UTC")
  ev <- data.frame(
    id = 1:7,
    event_start = as.POSIXct(c("2026-11-01 10:00", rep("2026-09-01 10:00", 6)), tz = "UTC"),
    join_url = c("x/tenantA", "x/tenantB", "x/tenantB", "x/tenantA", NA, "x/tenantA", "x/tenantA"),
    join_url_checked_at = as.POSIXct("2026-09-02", tz = "UTC"),
    online_meeting_lookup = c(NA, NA, NA, "policy_403", NA, NA, NA),
    event_class = c("extern_planned_extern_call", "extern_planned_extern_call",
                    "extern_planned_no_call", "extern_planned_no_call", "extern_planned_no_call",
                    "extern_planned_no_call", "extern_planned_intern_call"),
    stringsAsFactors = FALSE)
  expect_equal(derive_call_status(ev, tenant_id = "tenantA", now_utc = now),
               c("in_zukunft", "extern_call", "alt_tenant", "nicht_abrufbar", "ohne_link",
                 "no_call", "intern_call"))
})

# ---- assemble_meeting_signals ---------------------------------------------------------

signals_fuer <- function(tasks, comments, m = mk_meetings()) {
  map <- assemble_task_mapping(tasks, m)
  assemble_meeting_signals(m, mk_event_signals(), tasks, map, crm_task_outcomes(tasks$id, comments))
}

test_that("zwei Tasks: geschriebenes stattgefunden schlaegt storniert, Widerspruch markiert (Q4)", {
  tasks <- rbind(mk_task(1, 901, 10L, "2026-07-01"), mk_task(2, 902, 10L, "2026-07-01", rep = 600L))
  k <- rbind(mk_comment(1, 1, "storniert", "2026-07-01 08:00"),
             mk_comment(2, 2, "hat stattgefunden", "2026-07-01 10:00"))
  # Task 2 ist mehrdeutig? Nein: genau ein Termin (Lead 10, 01.07.) -> beide lead_tag.
  s <- signals_fuer(tasks, k)
  r <- s[s$meeting_key == "msgraph_10_10", ]
  expect_equal(r$crm_status, "show_up")
  expect_equal(r$crm_lead_task_id, "2")
  expect_equal(r$crm_task_count, 2L)
  expect_true(r$crm_status_conflict)
})

test_that("zwei Tasks: No-Show schlaegt storniert, stattgefunden schlaegt No-Show (Q4, Q13)", {
  tasks <- rbind(mk_task(1, 901, 10L, "2026-07-01"), mk_task(2, 902, 10L, "2026-07-01"))
  k1 <- rbind(mk_comment(1, 1, "storniert"), mk_comment(2, 2, "nicht erschienen"))
  expect_equal(signals_fuer(tasks, k1)$crm_status[1], "no_show")
  k2 <- rbind(mk_comment(1, 1, "hat stattgefunden"), mk_comment(2, 2, "nicht erschienen"))
  expect_equal(signals_fuer(tasks, k2)$crm_status[1], "show_up")
})

test_that("ein Task ohne Ausgang, einer storniert: storniert zaehlt, kein Widerspruch", {
  tasks <- rbind(mk_task(1, 901, 10L, "2026-07-01"), mk_task(2, 902, 10L, "2026-07-01"))
  s <- signals_fuer(tasks, mk_comment(2, 2, "abgesagt"))
  r <- s[s$meeting_key == "msgraph_10_10", ]
  expect_equal(r$crm_status, "storniert")
  expect_false(r$crm_status_conflict)
})

test_that("Termin ohne Task: kein_task; Ersatzregel uebernimmt den Task-Lead", {
  tasks <- mk_task(4, 904, 77L, "2026-07-04", ptime = "2026-07-04 12:10")
  s <- signals_fuer(tasks, mk_comment(4, 1, "hat stattgefunden"))
  expect_equal(s$crm_status[s$meeting_key == "msgraph_11_20"], "kein_task")
  r <- s[s$meeting_key == "msgraph_13_77", ]
  expect_equal(r$lead_source, "ersatzregel")
  expect_equal(r$match_rule, "ersatzregel")
  expect_equal(r$msgraph_exclusion_reason, "duplikat_event")
  expect_false(any(s$meeting_key == "msgraph_13_NA"))
})

test_that("mehrdeutiger Task bekommt eine eigene CRM-Zeile (Q3)", {
  m <- mk_meetings()
  m <- rbind(m, transform(m[1, ], call_event_mapping_id = 99L, event_id = "990"))
  tasks <- mk_task(3, 903, 10L, "2026-07-01")
  s <- signals_fuer(tasks, mk_comment(3, 1, "nicht erschienen"), m)
  r <- s[s$meeting_key == "crm_903", ]
  expect_equal(r$match_rule, "mehrdeutig")
  expect_equal(r$crm_status, "no_show")
  expect_equal(r$outlook_status, "kein_event")
  expect_equal(r$call_status, "kein_event")
  expect_false(r$has_msgraph_event)
})

test_that("meeting_keys stimmen mit assemble_unified_meetings ueberein, plus mehrdeutige Tasks", {
  m <- mk_meetings()
  m <- rbind(m, transform(m[1, ], call_event_mapping_id = 99L, event_id = "990"))
  tasks <- rbind(mk_task(1, 901, 20L, "2026-07-02"),
                 mk_task(3, 903, 10L, "2026-07-01"),
                 mk_task(4, 904, 77L, "2026-07-04", ptime = "2026-07-04 12:10"),
                 mk_task(5, 905, 88L, "2026-08-01"),
                 mk_task(6, 906, 30L, "2026-07-03", tool = "zoom", ext = TRUE))
  crm <- transform(tasks, meeting_status = "unbekannt", original_created_at = task_created_at)
  uni <- assemble_unified_meetings(m, crm)
  s <- signals_fuer(tasks, mk_comment(1, 1, "x")[0, ], m)
  expect_setequal(s$meeting_key, c(uni$meeting_key, "crm_903"))
  expect_equal(nrow(s), length(unique(s$meeting_key)))
})

test_that("Task, dessen Meeting-Zeile fehlt, wird CRM-Zeile statt zu verschwinden", {
  tasks <- mk_task(1, 901, 10L, "2026-07-01")
  map <- data.frame(crm_lead_task_id = "1", msgraph_event_id = "999", match_rule = "lead_tag",
                    stringsAsFactors = FALSE)
  expect_warning(
    s <- assemble_meeting_signals(mk_meetings(), mk_event_signals(), tasks, map,
                                  crm_task_outcomes(1, mk_comment(1, 1, "hat stattgefunden"))),
    "Meeting-Zeile, die es nicht gibt")
  r <- s[s$meeting_key == "crm_901", ]
  expect_equal(r$crm_status, "show_up")
  expect_false(r$has_msgraph_event)
})
