# Rueckuebersetzung der Platzhalter: Das Modell schreibt sie nicht immer mit den
# Leerzeichen in den spitzen Klammern. Ohne Treffer bliebe der Platzhalter im CRM stehen.

test_that("Platzhalter werden mit und ohne Leerzeichen gefunden", {
  text <- "Gespraech mit < Speaker: Lead_1 > in <Lead_2_City_1>, Kollege < Unknown_6>, Sales <Speaker:Salesperson_3 >"

  expect_equal(
    extract_placeholder_tokens(text),
    c("< Speaker: Lead_1 >", "<Lead_2_City_1>", "< Unknown_6>", "<Speaker:Salesperson_3 >")
  )
})

test_that("Anderer Text in spitzen Klammern ist kein Platzhalter", {
  expect_length(extract_placeholder_tokens("<b>fett</b>, <i>kursiv</i>, Umkreis <20 km, a < b > c"), 1)
  expect_equal(extract_placeholder_tokens("a < b > c"), "< b >")
  expect_length(extract_placeholder_tokens("kein Platzhalter"), 0)
})

fake_placeholders <- function(...) {
  tibble::tibble(
    transcript_id = "t1",
    placeholder   = c("< Speaker: Lead_1 >", "< Lead_2_City_1 >", "< Unknown_6 >"),
    actual_value  = c("Erika Muster", "Musterstadt", "Max Beispiel")
  )
}

test_that("Platzhalter ohne Leerzeichen werden zurueckuebersetzt", {
  local_mocked_bindings(tbl = fake_placeholders, .package = "dplyr")

  expect_equal(
    deanonymize_text(NULL, "Standort <Lead_2_City_1>, Einkauf < Unknown_6>", "t1"),
    "Standort Musterstadt, Einkauf Max Beispiel"
  )
})

test_that("Platzhalter mit Leerzeichen werden wie bisher zurueckuebersetzt", {
  local_mocked_bindings(tbl = fake_placeholders, .package = "dplyr")

  expect_equal(
    deanonymize_text(NULL, "Gespraech mit < Speaker: Lead_1 > in < Lead_2_City_1 >", "t1"),
    "Gespraech mit Erika Muster in Musterstadt"
  )
})

test_that("Unbekannte Platzhalter bleiben stehen", {
  local_mocked_bindings(tbl = fake_placeholders, .package = "dplyr")

  expect_equal(
    deanonymize_text(NULL, "Kollegin <Unknown_9> in < Lead_2_City_1 >", "t1"),
    "Kollegin <Unknown_9> in Musterstadt"
  )
})
