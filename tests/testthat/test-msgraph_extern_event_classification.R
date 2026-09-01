# Tests fuer compute_observability_exclusions().
#
# Hintergrund: is_no_show ist das FEHLEN einer Call-Zuordnung. Jedes Meeting,
# dessen Anwesenheit nie abrufbar ist, sieht damit aus wie ein No-Show. Diese
# Tests decken die beiden Faelle ab, in denen genau das passiert ist:
# Zukunftstermine und Meetings aus dem Alt-Tenant.

TENANT <- "1ca8bd94-3c97-4fc6-8955-bad266b43f0b"

join_url_fuer <- function(tenant) {
  paste0("https://teams.microsoft.com/l/meetup-join/19%3ameeting_abc%40thread.v2/0",
         "?context=%7b%22Tid%22%3a%22", tenant, "%22%7d")
}

make_events <- function() {
  data.frame(
    id = 1:5,
    event_start = as.POSIXct(
      c("2026-08-20 10:00:00",  # 1 vergangen, eigener Tenant  -> zaehlt
        "2026-08-20 11:00:00",  # 2 vergangen, Alt-Tenant      -> alt_tenant
        "2026-09-15 09:00:00",  # 3 Zukunft,   eigener Tenant  -> zukunft
        "2026-09-15 09:00:00",  # 4 Zukunft,   Alt-Tenant      -> zukunft (Vorrang)
        "2026-08-20 12:00:00"), # 5 vergangen, ohne join_url   -> zaehlt
      tz = "UTC"),
    join_url = c(join_url_fuer(TENANT),
                 join_url_fuer("99999999-0000-0000-0000-000000000000"),
                 join_url_fuer(TENANT),
                 join_url_fuer("99999999-0000-0000-0000-000000000000"),
                 NA_character_),
    stringsAsFactors = FALSE
  )
}

JETZT <- as.POSIXct("2026-08-28 12:00:00", tz = "UTC")

test_that("vergangenes Meeting im eigenen Tenant wird nicht ausgeschlossen", {
  res <- compute_observability_exclusions(make_events(), TENANT, JETZT)
  expect_false(1L %in% res$event_id)
})

test_that("vergangenes Meeting aus dem Alt-Tenant wird ausgeschlossen statt als No-Show gezaehlt", {
  res <- compute_observability_exclusions(make_events(), TENANT, JETZT)
  expect_equal(res$reason[res$event_id == 2L], "alt_tenant_join_url")
})

test_that("Termin in der Zukunft wird ausgeschlossen", {
  res <- compute_observability_exclusions(make_events(), TENANT, JETZT)
  expect_equal(res$reason[res$event_id == 3L], "termin_in_zukunft")
})

test_that("Zukunft hat Vorrang vor Alt-Tenant", {
  res <- compute_observability_exclusions(make_events(), TENANT, JETZT)
  expect_equal(res$reason[res$event_id == 4L], "termin_in_zukunft")
})

test_that("fehlende join_url fuehrt nicht zum Ausschluss", {
  res <- compute_observability_exclusions(make_events(), TENANT, JETZT)
  expect_false(5L %in% res$event_id)
})

test_that("ohne tenant_id greift nur der Zukunfts-Ausschluss", {
  res <- compute_observability_exclusions(make_events(), NULL, JETZT)
  expect_setequal(res$event_id, c(3L, 4L))
  expect_true(all(res$reason == "termin_in_zukunft"))
})

test_that("ein gefundener Call schuetzt das Event vor dem Alt-Tenant-Ausschluss", {
  # Der gesamte Bestand vor der Migration traegt die alte Tenant-GUID, hat aber
  # gueltige Calls aus dem tenantweiten base-35-Ingest. Wuerde die Regel auch
  # dort greifen, verschwaende die historische No-Show-Reihe.
  res <- compute_observability_exclusions(make_events(), TENANT, JETZT,
                                          event_ids_mit_call = 2L)
  expect_false(2L %in% res$event_id)
})

test_that("ein Call schuetzt NICHT vor dem Zukunfts-Ausschluss", {
  # Ein Termin, der noch bevorsteht, ist kein No-Show - auch wenn ein Call an
  # ihm haengt. Das waere ein Datenwiderspruch und keine Beobachtung.
  res <- compute_observability_exclusions(make_events(), TENANT, JETZT,
                                          event_ids_mit_call = c(3L, 4L))
  expect_equal(res$reason[res$event_id == 3L], "termin_in_zukunft")
  expect_equal(res$reason[res$event_id == 4L], "termin_in_zukunft")
})

test_that("Alt-Tenant vor dem Stichtag ist ein echter No-Show, kein Ausschluss", {
  # Solange base-35 tenantweit Calls holte, war ein fehlender Call ein echter
  # No-Show. Ohne diese Grenze verschwanden im Juli 86 davon.
  vorher <- data.frame(
    id = 10L,
    event_start = as.POSIXct("2026-07-15 10:00:00", tz = "UTC"),
    join_url = join_url_fuer("99999999-0000-0000-0000-000000000000"),
    stringsAsFactors = FALSE
  )
  res <- compute_observability_exclusions(vorher, TENANT, JETZT)
  expect_equal(nrow(res), 0L)
})

test_that("Alt-Tenant ab dem Stichtag wird ausgeschlossen", {
  nachher <- data.frame(
    id = 11L,
    event_start = as.POSIXct("2026-08-20 10:00:00", tz = "UTC"),
    join_url = join_url_fuer("99999999-0000-0000-0000-000000000000"),
    stringsAsFactors = FALSE
  )
  res <- compute_observability_exclusions(nachher, TENANT, JETZT)
  expect_equal(res$reason, "alt_tenant_join_url")
})

test_that("der Stichtag ist verschiebbar", {
  ev <- data.frame(
    id = 12L,
    event_start = as.POSIXct("2026-07-15 10:00:00", tz = "UTC"),
    join_url = join_url_fuer("99999999-0000-0000-0000-000000000000"),
    stringsAsFactors = FALSE
  )
  res <- compute_observability_exclusions(ev, TENANT, JETZT,
                                          alt_tenant_ab = as.Date("2026-07-01"))
  expect_equal(res$reason, "alt_tenant_join_url")
})

test_that("ohne event_ids_mit_call bleibt das Verhalten unveraendert", {
  res <- compute_observability_exclusions(make_events(), TENANT, JETZT)
  expect_setequal(res$event_id, c(2L, 3L, 4L))
})

test_that("leere Eingabe liefert null Zeilen statt eines Fehlers", {
  leer <- make_events()[0, , drop = FALSE]
  res <- compute_observability_exclusions(leer, TENANT, JETZT)
  expect_equal(nrow(res), 0L)
  expect_named(res, c("event_id", "reason"))
})

test_that("event_start wird als UTC gelesen, nicht in der Session-Zeitzone", {
  # Der Treiber kann den timestamp-without-time-zone mit der Session-Zone
  # taggen. 01:00 Berlin waere 23:00 UTC am Vortag und damit faelschlich
  # Vergangenheit. force_tz(\"UTC\") macht daraus 01:00 UTC = Zukunft.
  ev <- data.frame(
    id = 99L,
    event_start = as.POSIXct("2026-08-29 01:00:00", tz = "Europe/Berlin"),
    join_url = join_url_fuer(TENANT),
    stringsAsFactors = FALSE
  )
  res <- compute_observability_exclusions(ev, TENANT,
                                          as.POSIXct("2026-08-29 00:00:00", tz = "UTC"))
  expect_equal(res$reason, "termin_in_zukunft")
})

# Tests fuer compute_original_created_at().
#
# Hintergrund: Beim Tenant-Wechsel im August 2026 hat Graph fuer bestehende
# Termine ein NEUES createdDateTime geliefert. Es entstand kein zweites Event,
# dieselbe Zeile bekam nur einen spaeteren Stempel. Gemessen am 01.09.2026
# trugen 1975 von 2768 Cutover-Terminen ein Graph-Anlagedatum, das im Schnitt
# 68 Tage NACH unserem ersten Ingest lag. Ihre Terminierung wanderte dadurch
# rueckwirkend in den August: die Woche vom 17.08. zeigte 1713 statt 377
# gelegte Termine.
#
# raw.msgraph_events.created_at ist der Zeitpunkt unseres ersten Inserts.
# Billomatics::postgres_upsert_data nimmt created_at aus update_cols heraus,
# der Wert bleibt also stabil. Er ist damit eine harte obere Schranke fuer das
# echte Anlagedatum.

utc <- function(x) as.POSIXct(x, tz = "UTC")

test_that("ohne Widerspruch bleibt das Graph-Datum unveraendert", {
  events <- data.frame(
    msgraph_ical_uid  = c("uid-a", "uid-b"),
    event_created_at  = utc(c("2026-06-10 08:00:00", "2026-07-02 09:30:00")),
    created_at        = utc(c("2026-06-11 02:00:00", "2026-07-03 02:00:00")),
    stringsAsFactors  = FALSE
  )

  result <- compute_original_created_at(events)

  expect_equal(result$original_created_at[result$msgraph_ical_uid == "uid-a"],
               utc("2026-06-10 08:00:00"))
  expect_equal(result$original_created_at[result$msgraph_ical_uid == "uid-b"],
               utc("2026-07-02 09:30:00"))
})

test_that("liegt das Graph-Datum nach dem ersten Ingest, gewinnt der Ingest", {
  # Realfall aus der Diagnose: Termin am 31.07. eingelesen, Graph meldet nach
  # der Migration den 19.08. als Anlagedatum.
  events <- data.frame(
    msgraph_ical_uid  = "uid-migriert",
    event_created_at  = utc("2026-08-19 09:00:21"),
    created_at        = utc("2026-07-31 02:00:31"),
    stringsAsFactors  = FALSE
  )

  result <- compute_original_created_at(events)

  expect_equal(result$original_created_at, utc("2026-07-31 02:00:31"))
})

test_that("mehrere Instanzen einer uid liefern das frueheste Datum", {
  # Verschobener Termin: zwei Events unter derselben ical_uid. Die Klammer
  # bleibt das Minimum ueber die ganze uid, wie bisher.
  events <- data.frame(
    msgraph_ical_uid  = c("uid-serie", "uid-serie"),
    event_created_at  = utc(c("2026-06-01 10:00:00", "2026-08-19 08:00:00")),
    created_at        = utc(c("2026-06-02 02:00:00", "2026-06-02 02:00:00")),
    stringsAsFactors  = FALSE
  )

  result <- compute_original_created_at(events)

  expect_equal(nrow(result), 1)
  expect_equal(result$original_created_at, utc("2026-06-01 10:00:00"))
})

test_that("fehlendes Graph-Datum faellt auf den Ingest zurueck", {
  events <- data.frame(
    msgraph_ical_uid  = "uid-ohne",
    event_created_at  = as.POSIXct(NA, tz = "UTC"),
    created_at        = utc("2026-07-15 02:00:00"),
    stringsAsFactors  = FALSE
  )

  result <- compute_original_created_at(events)

  expect_equal(result$original_created_at, utc("2026-07-15 02:00:00"))
})

test_that("fehlender Ingest-Stempel laesst das Graph-Datum stehen", {
  events <- data.frame(
    msgraph_ical_uid  = "uid-alt",
    event_created_at  = utc("2026-06-10 08:00:00"),
    created_at        = as.POSIXct(NA, tz = "UTC"),
    stringsAsFactors  = FALSE
  )

  result <- compute_original_created_at(events)

  expect_equal(result$original_created_at, utc("2026-06-10 08:00:00"))
})
