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
