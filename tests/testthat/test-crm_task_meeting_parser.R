# Tests fuer den reinen CRM-Task-Meeting-Parser (Spec 2026-07-24, §5.1/§5.3).
# Beispielstrings stammen aus der echten Discovery auf raw.crm_lead_tasks.

test_that("is_vc_task erkennt VC-Marker und externe Tools, aber nicht blosses 'Teams'", {
  expect_true(is_vc_task("VC Webex ZR mit Frings und Knechtges"))
  expect_true(is_vc_task("Nur ueber Zoom VC UC"))
  expect_true(is_vc_task("VC UC (WEBEX)"))
  expect_true(is_vc_task("WEBEX EINLADUNG senden!!!"))          # externes Tool ohne VC-Marker
  # bewusst FALSE: 'Teams' als Praeferenz-Notiz, kein VC-Termin
  expect_false(is_vc_task("Telefonisch melden - findet Telefonate besser als geplante Teams Sitzungen"))
  expect_false(is_vc_task("er machen"))
  expect_false(is_vc_task(NA_character_))
})

test_that("extract_meeting_tool priorisiert korrekt und ist case-insensitiv", {
  expect_equal(extract_meeting_tool("VC Webex ZR"), "webex")
  expect_equal(extract_meeting_tool("VC UC (WEBEX)"), "webex")
  expect_equal(extract_meeting_tool("Nur ueber Zoom VC UC"), "zoom")
  expect_equal(extract_meeting_tool("VC Teams Update"), "teams")
  expect_equal(extract_meeting_tool("VC ZR mit Bronder"), "unbekannt")
  expect_equal(extract_meeting_tool(NA_character_), "unbekannt")
})

test_that("is_external_tool trennt externe Tools von Teams/unbekannt", {
  expect_true(is_external_tool("webex"))
  expect_true(is_external_tool("zoom"))
  expect_true(is_external_tool("google_meet"))
  expect_true(is_external_tool("skype"))
  expect_false(is_external_tool("teams"))
  expect_false(is_external_tool("unbekannt"))
})

test_that("classify_meeting_status priorisiert Storno vor No-Show vor Show-Up", {
  expect_equal(classify_meeting_status("Termin wurde abgesagt"), "storniert")
  expect_equal(classify_meeting_status("verschoben auf naechste Woche"), "storniert")
  expect_equal(classify_meeting_status("Kunde ist nicht erschienen"), "no_show")
  expect_equal(classify_meeting_status("No Show, muss neu terminieren"), "no_show")
  expect_equal(classify_meeting_status("hat stattgefunden, lief gut"), "show_up")
  expect_equal(classify_meeting_status("Kunde war da, Show-Up"), "show_up")
  expect_equal(classify_meeting_status("No-Show"), "no_show")
  expect_equal(classify_meeting_status("Show-Up"), "show_up")
  expect_equal(classify_meeting_status(""), "unbekannt")
  expect_equal(classify_meeting_status(NA_character_), "unbekannt")
})

test_that("classify_meeting_status erkennt NE-Kurzform + AB (Anrufbeantworter) als No-Show", {
  # NE = 'nicht erschienen', die haeufigste Sales-Kurzform (Discovery 2026-08)
  expect_equal(classify_meeting_status("n.e."), "no_show")
  expect_equal(classify_meeting_status("N.E"), "no_show")
  expect_equal(classify_meeting_status("NE"), "no_show")
  expect_equal(classify_meeting_status("12.12.22 NE"), "no_show")
  expect_equal(classify_meeting_status("14.12.22 NE / Direkt Mailbox"), "no_show")
  # AB = Anrufbeantworter zaehlt als No-Show (Domaenen-Entscheidung Moritz)
  expect_equal(classify_meeting_status("AB"), "no_show")
  # verschieben (Infinitiv) muss wie verschoben Storno sein
  expect_equal(classify_meeting_status("verschieben den Termin"), "storniert")
  # Wortgrenzen + Case: KEIN False-Positive bei umgangssprachlichem 'ne'/Praeposition 'ab'
  expect_equal(classify_meeting_status("hab ne Mail geschrieben"), "unbekannt")
  expect_equal(classify_meeting_status("ruft ab naechster Woche zurueck"), "unbekannt")
})

test_that("filter_new_crm_meetings behaelt externe Tools immer und Teams nur ohne MSGraph-Match", {
  crm <- data.frame(
    crm_task_id     = 1:4,
    lead_id         = c(10L, 20L, 30L, 40L),
    event_date      = as.Date(c("2026-07-20", "2026-07-20", "2026-07-21", "2026-07-22")),
    is_external_tool = c(TRUE, FALSE, FALSE, TRUE),
    stringsAsFactors = FALSE
  )
  msgraph <- data.frame(
    lead_id    = c(10L, 20L, 99L),
    event_date = as.Date(c("2026-07-20", "2026-07-20", "2026-07-20")),
    stringsAsFactors = FALSE
  )
  # task 1: extern + MSGraph-Match am selben Tag -> trotzdem BEHALTEN (extern immer)
  # task 2: teams + MSGraph-Match -> VERWERFEN
  # task 3: teams + kein Match     -> BEHALTEN
  # task 4: extern + kein Match    -> BEHALTEN
  res <- filter_new_crm_meetings(crm, msgraph)
  expect_equal(sort(res$crm_task_id), c(1L, 3L, 4L))
  # keine Hilfsspalte durchgereicht
  expect_false(".in_msgraph" %in% names(res))
})

test_that("extract_meeting_type kanonisiert die im Bestand haeufigen Termin-Typen", {
  expect_equal(extract_meeting_type("VC NV"), "nv")
  expect_equal(extract_meeting_type("NV VC "), "nv")
  expect_equal(extract_meeting_type("VC UC"), "uc")
  expect_equal(extract_meeting_type("VC: Updatecall "), "uc")
  expect_equal(extract_meeting_type("VC Update-Call"), "uc")
  expect_equal(extract_meeting_type("VC Updates "), "uc")
  expect_equal(extract_meeting_type("VC FU"), "fu")
  expect_equal(extract_meeting_type("VC: FUP"), "fu")
  expect_equal(extract_meeting_type("VC Follow Up "), "fu")
  expect_equal(extract_meeting_type("VC: Reporting"), "rep")
  expect_equal(extract_meeting_type("VC REP "), "rep")
  expect_equal(extract_meeting_type("VC ZR"), "zr")
  expect_equal(extract_meeting_type("VC Planungstermin"), "planung")
  expect_equal(extract_meeting_type("VC Kampagnenplanung "), "planung")
})

test_that("extract_meeting_type: nv gewinnt bei doppelt genanntem Typ", {
  expect_equal(extract_meeting_type("VC NV/UC"), "nv")
})

test_that("extract_meeting_type matcht ER case-sensitiv, damit das Pronomen 'er' nicht trifft", {
  expect_equal(extract_meeting_type("VC ER"), "er")
  expect_equal(extract_meeting_type("er machen"), "unbekannt")
  expect_equal(extract_meeting_type("Kunde will, dass er sich meldet"), "unbekannt")
})

test_that("extract_meeting_type liefert unbekannt statt einer Fehlklassifikation", {
  expect_equal(extract_meeting_type("VC: Albatros "), "unbekannt")
  expect_equal(extract_meeting_type("VC"), "unbekannt")
  expect_equal(extract_meeting_type(NA_character_), "unbekannt")
})
