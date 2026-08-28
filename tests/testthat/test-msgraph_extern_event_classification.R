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
