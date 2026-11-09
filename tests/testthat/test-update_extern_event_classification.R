# Charakterisierung des Hauptablaufs von update_extern_event_classification().
# Kein DB-Zugriff: dplyr::tbl liefert die Tabellen unten, der Upsert wird
# abgefangen. Die Erwartungen beschreiben das Verhalten vor dem Auslagern der
# Erkennungsschritte und duerfen sich durch das Auslagern nicht aendern.

utc <- function(x) as.POSIXct(x, tz = "UTC")

JETZT <- utc("2026-10-20 12:00:00")
TENANT <- "own-tenant"
LINK <- "https://teams.example/own-tenant/x"
ALT_LINK <- "https://teams.example/old-tenant/x"

# Jedes Szenario hat seinen eigenen Tag und seinen eigenen Lead, damit sich
# Szenarien nicht ueber Duplikat- oder Ersatz-Regeln gegenseitig treffen.
tag <- function(n) utc("2026-09-01 10:00:00") + n * 86400

ereignis <- function(id, uid, start, canceled = FALSE, stunden_bis_aenderung = 72,
                     join_url = LINK, lookup = NA_character_,
                     angelegt = start - 10 * 86400) {
  tibble::tibble(
    id = as.integer(id), msgraph_ical_uid = uid,
    event_created_at = angelegt, created_at = angelegt,
    event_updated_at = angelegt + stunden_bis_aenderung * 3600,
    event_start = start, event_end = start + 1800,
    is_canceled = canceled, is_online_meeting = TRUE, subject = "Termin",
    join_url = join_url, online_meeting_lookup = lookup
  )
}

KLASSE_CALL <- "extern_planned_extern_call"
KLASSE_NO_CALL <- "extern_planned_no_call"
KLASSE_INTERN <- "extern_planned_intern_call"

mapping <- function(event_id, klasse, call_id = NA_integer_, id = 100L + as.integer(event_id)) {
  tibble::tibble(id = as.integer(id), event_id = as.integer(event_id),
                 call_id = as.integer(call_id), event_class = klasse,
                 event_date = as.Date("2026-09-01"))
}

teilnehmer <- function(event_id, lead_id, weitere_interne = integer(0)) {
  tibble::tibble(
    event_id = as.integer(event_id),
    contact_id = as.integer(c(1L, lead_id, weitere_interne)),
    is_organizer = c(TRUE, rep(FALSE, 1L + length(weitere_interne)))
  )
}

klassifikations_tabellen <- function() {
  sechs_kollegen <- 2:7
  fuenf_kollegen <- 2:6

  events <- dplyr::bind_rows(
    # Normalfaelle
    ereignis(1, "U1", tag(1)),
    ereignis(2, "U2", tag(2)),
    # Schritt 4: abgesagt, aktive Zeile derselben ical_uid
    ereignis(3, "U3", tag(3), canceled = TRUE),
    ereignis(4, "U3", tag(4)),
    # Schritt 5: Grenze bei sieben Internen. Event 6 liegt 5 Minuten neben
    # Event 5 beim selben Lead, ist aber kein Duplikat, weil Schritt 6 Event 5
    # nicht sieht.
    ereignis(5, "U5", tag(5)),
    ereignis(6, "U6", tag(5) + 300),
    # Schritt 6: No-Show verliert gegen stattgefundenen Termin
    ereignis(7, "U7", tag(7)),
    ereignis(8, "U8", tag(7) + 600),
    # Schritt 6: Grenze 15 Minuten, sonst verliert der spaetere
    ereignis(9, "U9", tag(9)),
    ereignis(10, "U10", tag(9) + 900),
    ereignis(11, "U11", tag(11)),
    ereignis(12, "U12", tag(11) + 960),
    # Schritt 7: Ersatz mit anderer ical_uid, der naehere gewinnt und erbt das
    # fruehere original_created_at. Der fernere bleibt unberuehrt.
    ereignis(13, "U13", tag(13), canceled = TRUE, angelegt = tag(13) - 30 * 86400),
    ereignis(14, "U14", tag(14)),
    ereignis(15, "U15", tag(14) + 43200),
    # Schritt 7: zwei Tage Abstand reichen nicht mehr. Event 16 ist kurzlebig.
    ereignis(16, "U16", tag(16), canceled = TRUE, stunden_bis_aenderung = 5),
    ereignis(17, "U17", tag(18)),
    # Schritt 3: Absage nach 30 Stunden ist nicht kurzlebig
    ereignis(18, "U18", tag(20), canceled = TRUE, stunden_bis_aenderung = 30),
    # Schritt 9: Beobachtbarkeit
    ereignis(19, "U19", JETZT + 86400),
    ereignis(20, "U20", tag(21), join_url = ALT_LINK),
    ereignis(21, "U21", tag(22), lookup = "policy_403"),
    # Schritt 8b: interner Call am Slot, Absage danach, bleibt gezaehlt
    ereignis(22, "U22", tag(23), canceled = TRUE, stunden_bis_aenderung = 300),
    ereignis(23, "U22", tag(24)),
    # Rangfolge: verschoben sticht zu viele Interne
    ereignis(24, "U24", tag(25), canceled = TRUE),
    ereignis(25, "U24", tag(26)),
    # Synthetische Gaeste machen keine Duplikate
    ereignis(26, "U26", tag(27)),
    ereignis(27, "U27", tag(27) + 300),
    # Schritt 6 sieht keine verschobenen Events: Event 30 liegt 5 Minuten neben
    # dem verschobenen Event 28 und ist trotzdem kein Duplikat.
    ereignis(28, "U28", tag(28), canceled = TRUE),
    ereignis(29, "U28", tag(29)),
    ereignis(30, "U30", tag(28) + 300)
  )

  list(
    "raw.msgraph_calls" = tibble::tibble(
      id = c(1L, 2L, 3L, 4L, 5L),
      call_start = c(tag(1), tag(1), tag(7) + 600, tag(9), tag(23)),
      call_end = c(tag(1) + 60, tag(1) + 1800, tag(7) + 2400, tag(9) + 1800, tag(23) + 600)
    ),
    "mapping.msgraph_call_event" = dplyr::bind_rows(
      # Event 1 hat zwei Mappings, der laengste erfolgreiche Call gewinnt
      mapping(1, KLASSE_CALL, 1L, id = 1001L),
      mapping(1, KLASSE_CALL, 2L, id = 1002L),
      mapping(2, KLASSE_NO_CALL),
      mapping(3, KLASSE_NO_CALL),
      mapping(4, KLASSE_NO_CALL),
      mapping(5, KLASSE_NO_CALL),
      mapping(6, KLASSE_NO_CALL),
      mapping(7, KLASSE_NO_CALL),
      mapping(8, KLASSE_CALL, 3L),
      mapping(9, KLASSE_CALL, 4L),
      mapping(10, KLASSE_NO_CALL),
      mapping(11, KLASSE_NO_CALL),
      mapping(12, KLASSE_NO_CALL),
      mapping(13, KLASSE_NO_CALL),
      mapping(14, KLASSE_NO_CALL),
      mapping(15, KLASSE_NO_CALL),
      mapping(16, KLASSE_NO_CALL),
      mapping(17, KLASSE_NO_CALL),
      mapping(18, KLASSE_NO_CALL),
      mapping(19, KLASSE_NO_CALL),
      mapping(20, KLASSE_NO_CALL),
      mapping(21, KLASSE_NO_CALL),
      mapping(22, KLASSE_INTERN, 5L),
      mapping(23, KLASSE_NO_CALL),
      mapping(24, KLASSE_NO_CALL),
      mapping(25, KLASSE_NO_CALL),
      mapping(26, KLASSE_NO_CALL),
      mapping(27, KLASSE_NO_CALL),
      mapping(28, KLASSE_NO_CALL),
      mapping(29, KLASSE_NO_CALL),
      mapping(30, KLASSE_NO_CALL)
    ),
    "raw.msgraph_events" = events,
    "raw.msgraph_event_participants" = dplyr::bind_rows(
      teilnehmer(1, 101),
      teilnehmer(2, 102),
      teilnehmer(3, 103),
      teilnehmer(4, 103),
      # Organisator plus sechs Kollegen: genau sieben Interne
      teilnehmer(5, 105, sechs_kollegen),
      # Organisator plus fuenf Kollegen: sechs Interne, kein Ausschluss
      teilnehmer(6, 105, fuenf_kollegen),
      teilnehmer(7, 107),
      teilnehmer(8, 107),
      teilnehmer(9, 109),
      teilnehmer(10, 109),
      teilnehmer(11, 111),
      teilnehmer(12, 111),
      teilnehmer(13, 113),
      teilnehmer(14, 113),
      teilnehmer(15, 113),
      teilnehmer(16, 116),
      teilnehmer(17, 116),
      teilnehmer(18, 118),
      teilnehmer(19, 119),
      teilnehmer(20, 120),
      teilnehmer(21, 121),
      teilnehmer(22, 122),
      teilnehmer(23, 122),
      teilnehmer(24, 124, sechs_kollegen),
      teilnehmer(25, 124),
      teilnehmer(26, 126),
      teilnehmer(27, 126),
      teilnehmer(28, 128),
      teilnehmer(29, 128),
      teilnehmer(30, 128)
    ),
    "raw.msgraph_contacts" = tibble::tibble(
      id = c(1:7, 101L, 102L, 103L, 105L, 107L, 109L, 111L, 113L, 116L, 118L, 119L,
             120L, 121L, 122L, 124L, 128L, 126L),
      ms_name = "Name",
      email = c(paste0("kollege", 1:7, "@studyflix.de"),
                paste0("lead", c(101, 102, 103, 105, 107, 109, 111, 113, 116, 118, 119,
                                 120, 121, 122, 124, 128), "@kunde.de"),
                "gast@external.guest")
    ),
    "raw.msgraph_call_participants" = tibble::tibble(
      call_id = c(1L, 2L, 3L, 4L, 5L), contact_id = 1L
    ),
    "processed.msgraph_extern_event_classification" = tibble::tibble(
      call_event_mapping_id = 9999L, contact_id = 1L, is_responsible = TRUE,
      is_organizer = TRUE, is_no_show = FALSE, excluded = TRUE,
      exclusion_reason = "alter_grund", original_created_at = utc("2025-01-01 00:00:00"),
      is_short_lived_event = FALSE
    )
  )
}

klassifikation_ausfuehren <- function(tabellen, env = parent.frame()) {
  gefangen <- new.env()
  testthat::local_mocked_bindings(
    tbl = function(src, from, ...) tabellen[[as.character(from)]],
    .package = "dplyr", .env = env)
  testthat::local_mocked_bindings(
    dbGetQuery = function(conn, statement, ...) data.frame(n = 1L),
    .package = "DBI", .env = env)
  testthat::local_mocked_bindings(
    postgres_upsert_data = function(con, schema, table, data, ...) {
      gefangen$schema <- schema
      gefangen$table <- table
      gefangen$data <- data
      gefangen$args <- list(...)
      invisible(NULL)
    },
    .package = "Billomatics", .env = env)
  suppressMessages(
    update_extern_event_classification(NULL, tenant_id = TENANT, now_utc = JETZT)
  )
  gefangen
}

test_that("Hauptablauf: jedes Event bekommt den erwarteten Ausschlussgrund", {
  ergebnis <- klassifikation_ausfuehren(klassifikations_tabellen())$data %>%
    dplyr::filter(call_event_mapping_id != 9999L) %>%
    dplyr::arrange(call_event_mapping_id) %>%
    dplyr::select(call_event_mapping_id, exclusion_reason, excluded,
                  is_no_show, is_short_lived_event)

  erwartet <- tibble::tribble(
    ~call_event_mapping_id, ~exclusion_reason,                ~excluded, ~is_no_show, ~is_short_lived_event,
    102L,                   NA,                               FALSE,     TRUE,        FALSE,
    103L,                   "verschoben",                     TRUE,      TRUE,        FALSE,
    104L,                   NA,                               FALSE,     TRUE,        FALSE,
    105L,                   "zu_viele_interne",               TRUE,      TRUE,        FALSE,
    106L,                   NA,                               FALSE,     TRUE,        FALSE,
    107L,                   "duplikat_event",                 TRUE,      TRUE,        FALSE,
    108L,                   NA,                               FALSE,     FALSE,       FALSE,
    109L,                   NA,                               FALSE,     FALSE,       FALSE,
    110L,                   "duplikat_event",                 TRUE,      TRUE,        FALSE,
    111L,                   NA,                               FALSE,     TRUE,        FALSE,
    112L,                   NA,                               FALSE,     TRUE,        FALSE,
    113L,                   "rescheduled_without_meeting_id", TRUE,      TRUE,        FALSE,
    114L,                   NA,                               FALSE,     TRUE,        FALSE,
    115L,                   NA,                               FALSE,     TRUE,        FALSE,
    116L,                   NA,                               FALSE,     TRUE,        TRUE,
    117L,                   NA,                               FALSE,     TRUE,        FALSE,
    118L,                   NA,                               FALSE,     TRUE,        FALSE,
    119L,                   "termin_in_zukunft",              TRUE,      TRUE,        FALSE,
    120L,                   "alt_tenant_join_url",            TRUE,      TRUE,        FALSE,
    121L,                   "online_meeting_nicht_abrufbar",  TRUE,      TRUE,        FALSE,
    122L,                   NA,                               FALSE,     TRUE,        FALSE,
    123L,                   NA,                               FALSE,     TRUE,        FALSE,
    124L,                   "verschoben",                     TRUE,      TRUE,        FALSE,
    125L,                   NA,                               FALSE,     TRUE,        FALSE,
    126L,                   NA,                               FALSE,     TRUE,        FALSE,
    127L,                   NA,                               FALSE,     TRUE,        FALSE,
    128L,                   "verschoben",                     TRUE,      TRUE,        FALSE,
    129L,                   NA,                               FALSE,     TRUE,        FALSE,
    130L,                   NA,                               FALSE,     TRUE,        FALSE,
    1002L,                 NA,                               FALSE,     FALSE,       FALSE
  ) %>%
    dplyr::mutate(exclusion_reason = as.character(exclusion_reason))

  expect_equal(as.data.frame(ergebnis), as.data.frame(erwartet))
})

test_that("Hauptablauf: das Ersatz-Event erbt das fruehere Anlagedatum, das fernere nicht", {
  ergebnis <- klassifikation_ausfuehren(klassifikations_tabellen())$data
  angelegt <- function(mapping_id) {
    ergebnis$original_created_at[ergebnis$call_event_mapping_id == mapping_id]
  }

  expect_equal(angelegt(114L), tag(13) - 30 * 86400)
  expect_equal(angelegt(115L), tag(14) + 43200 - 10 * 86400)
})

test_that("Hauptablauf: Bestand ausserhalb des Laufs bleibt im Sync-Set", {
  gefangen <- klassifikation_ausfuehren(klassifikations_tabellen())

  expect_equal(gefangen$schema, "processed")
  expect_equal(gefangen$table, "msgraph_extern_event_classification")
  expect_true(gefangen$args$delete_missing)
  expect_equal(gefangen$args$match_cols, c("call_event_mapping_id", "contact_id"))
  bestand <- gefangen$data[gefangen$data$call_event_mapping_id == 9999L, ]
  expect_equal(bestand$exclusion_reason, "alter_grund")
})
