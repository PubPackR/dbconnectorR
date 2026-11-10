# Hauptablauf von msgraph_map_calls_events() mit Transkript-Sprechern
# (Asana 1220382115257832). Kein DB-Zugriff: dplyr::tbl liefert die Tabellen
# unten, der Upsert wird abgefangen.

utc <- function(x) as.POSIXct(x, tz = "UTC")
klartext <- function(...) paste(c(...), collapse = "\n")

# Je Szenario ein eigenes Meeting: Event n, Call 10 + n, intern Kontakt 1,
# extern Kontakt 100 + n.
szenario <- function(n, call = TRUE, extern_im_call = FALSE) {
  start <- utc("2026-10-26 11:00:00") + n * 86400
  list(
    events = tibble::tibble(id = as.integer(n), meeting_id = paste0("M", n),
                            event_start = start, is_canceled = FALSE),
    event_participants = tibble::tibble(event_id = as.integer(n),
                                        contact_id = c(1L, 100L + as.integer(n))),
    calls = if (call) tibble::tibble(id = 10L + as.integer(n), meeting_id = paste0("M", n),
                                     call_start = start + 120) else NULL,
    call_participants = if (call) tibble::tibble(
      call_id = 10L + as.integer(n),
      contact_id = if (extern_im_call) c(1L, 100L + as.integer(n)) else 1L) else NULL,
    contacts = tibble::tibble(id = 100L + as.integer(n),
                              email = paste0("kunde", n, "@extern.example"))
  )
}

zuordnungs_tabellen <- function() {
  s <- list(
    szenario(1),                         # externe Sprecherin im Transkript
    szenario(2),                         # Transkript nur mit Internen
    szenario(3),                         # Label ohne Kuerzel nur einmal
    szenario(4),                         # kein Transkript
    szenario(5, extern_im_call = TRUE),  # Externer im Bericht
    szenario(6, call = FALSE),           # kein Call
    szenario(7)                          # Transkript ohne Inhalt
  )
  alle <- function(teil) dplyr::bind_rows(lapply(s, `[[`, teil))
  list(
    "raw.msgraph_events" = alle("events"),
    "raw.msgraph_calls" = alle("calls"),
    "raw.msgraph_event_participants" = alle("event_participants"),
    "raw.msgraph_call_participants" = alle("call_participants"),
    "raw.msgraph_contacts" = dplyr::bind_rows(
      tibble::tibble(id = 1L, email = "tanja.mick@studyflix.de"), alle("contacts")),
    "processed.msgraph_call_transcripts" = tibble::tibble(
      call_id = c(11L, 12L, 13L, 15L, 17L, NA),
      transcript_content = c(
        klartext("Krampitz, Svea: Hallo", "Mick, Tanja, Studyflix GmbH: Hallo",
                 "Krampitz, Svea: Danke"),
        klartext("Mick, Tanja, Studyflix GmbH: Ist jemand da?",
                 "Mick, Tanja, Studyflix GmbH: Niemand da"),
        klartext("Mick, Tanja, Studyflix GmbH: Wir warten", "Wichtig ist: die Agenda"),
        klartext("Kunde, Fünf: Hallo", "Kunde, Fünf: Tschüss"),
        NA_character_,
        klartext("Ohne, Call: Hallo", "Ohne, Call: Tschüss")))
  )
}

zuordnung_ausfuehren <- function(tabellen, env = parent.frame()) {
  gefangen <- new.env()
  testthat::local_mocked_bindings(
    tbl = function(src, from, ...) tabellen[[as.character(from)]],
    .package = "dplyr", .env = env)
  testthat::local_mocked_bindings(
    postgres_upsert_data = function(con, schema, table, data, ...) {
      gefangen$table <- paste0(schema, ".", table)
      gefangen$data <- data
      gefangen$args <- list(...)
      invisible(NULL)
    },
    .package = "Billomatics", .env = env)
  suppressMessages(msgraph_map_calls_events(NULL))
  gefangen
}

test_that("Transkript mit externem Sprecher macht aus intern_call einen extern_call", {
  ergebnis <- zuordnung_ausfuehren(zuordnungs_tabellen())
  expect_equal(ergebnis$table, "mapping.msgraph_call_event")
  klasse <- ergebnis$data %>%
    dplyr::arrange(event_id) %>%
    dplyr::select(event_id, call_id, event_class)

  erwartet <- tibble::tribble(
    ~event_id, ~call_id, ~event_class,
    1L,        11L,      "extern_planned_extern_call",
    2L,        12L,      "extern_planned_intern_call",
    3L,        13L,      "extern_planned_intern_call",
    4L,        14L,      "extern_planned_intern_call",
    5L,        15L,      "extern_planned_extern_call",
    6L,        NA,       "extern_planned_no_call",
    7L,        17L,      "extern_planned_intern_call"
  )
  expect_equal(klasse, erwartet, ignore_attr = TRUE)
})

test_that("die Transkript-Spalte landet nicht in der Zieltabelle", {
  ergebnis <- zuordnung_ausfuehren(zuordnungs_tabellen())
  expect_setequal(names(ergebnis$data),
                  c("call_id", "event_id", "meeting_id", "event_class", "event_date",
                    "call_number_on_date"))
  expect_true(ergebnis$args$delete_missing)
})

test_that("derive_event_class_call: Transkript macht nur aus einem internen Call einen externen", {
  expect_equal(
    derive_event_class_call(
      extern_participating_users = c(1, 0, 0, 0, 0),
      participating_users        = c(2, 1, 1, 0, 1),
      has_external_speaker       = c(FALSE, TRUE, FALSE, FALSE, NA)),
    c("extern", "extern", "intern", "no", "intern"))
})

test_that("bigint-IDs als integer64 (wie aus RPostgres) treffen das Transkript", {
  tabellen <- zuordnungs_tabellen()
  tabellen[["raw.msgraph_calls"]]$id <- bit64::as.integer64(tabellen[["raw.msgraph_calls"]]$id)
  tabellen[["raw.msgraph_call_participants"]]$call_id <-
    bit64::as.integer64(tabellen[["raw.msgraph_call_participants"]]$call_id)
  tabellen[["processed.msgraph_call_transcripts"]]$call_id <-
    bit64::as.integer64(tabellen[["processed.msgraph_call_transcripts"]]$call_id)
  klasse <- zuordnung_ausfuehren(tabellen)$data
  expect_equal(klasse$event_class[klasse$event_id == 1L], "extern_planned_extern_call")
  expect_equal(klasse$event_class[klasse$event_id == 2L], "extern_planned_intern_call")
})

test_that("ohne Transkripte bleibt die Zuordnung wie bisher", {
  tabellen <- zuordnungs_tabellen()
  tabellen[["processed.msgraph_call_transcripts"]] <-
    tabellen[["processed.msgraph_call_transcripts"]][0, ]
  klasse <- zuordnung_ausfuehren(tabellen)$data %>% dplyr::arrange(event_id)
  expect_equal(klasse$event_class[klasse$event_id == 1L], "extern_planned_intern_call")
  expect_equal(klasse$event_class[klasse$event_id == 5L], "extern_planned_extern_call")
})
