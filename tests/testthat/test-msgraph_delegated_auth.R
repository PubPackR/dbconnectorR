# Unit tests fuer den delegierten MSGraph-Token-Store und -Provider.
# Kein Netzwerk, keine Credentials: httr wird gemockt, Dateien liegen in tempdir().

make_store <- function(refresh_token = "RT-1",
                       obtained_at = "2026-07-01T09:00:00Z") {
  list(
    refresh_token     = refresh_token,
    obtained_at       = obtained_at,
    last_refreshed_at = obtained_at,
    tenant_id         = "tenant-x",
    client_id         = "client-y",
    scopes            = c("https://graph.microsoft.com/Calendars.Read.Shared",
                          "offline_access")
  )
}

test_that("Store ueberlebt Schreiben und Lesen unveraendert", {
  path <- file.path(tempdir(), "store_roundtrip.txt")
  on.exit(unlink(c(path, paste0(path, ".bak"))), add = TRUE)

  msgraph_delegated_store_write(path, "geheim", make_store())
  back <- msgraph_delegated_store_read(path, "geheim")

  expect_equal(back$refresh_token, "RT-1")
  expect_equal(back$tenant_id, "tenant-x")
  expect_equal(back$scopes, make_store()$scopes)
})

test_that("Chiffrat steht auf genau einer Zeile", {
  path <- file.path(tempdir(), "store_oneline.txt")
  on.exit(unlink(c(path, paste0(path, ".bak"))), add = TRUE)

  # Grosser Token, damit ein etwaiger base64-Zeilenumbruch sicher auftraete
  msgraph_delegated_store_write(path, "geheim",
                                make_store(refresh_token = strrep("A", 4000)))

  expect_length(readLines(path, warn = FALSE), 1)
  expect_equal(nchar(msgraph_delegated_store_read(path, "geheim")$refresh_token), 4000)
})

test_that("Ueberschreiben legt ein .bak des Vorgaengers an", {
  path <- file.path(tempdir(), "store_backup.txt")
  bak  <- paste0(path, ".bak")
  on.exit(unlink(c(path, bak)), add = TRUE)

  msgraph_delegated_store_write(path, "geheim", make_store("RT-alt"))
  msgraph_delegated_store_write(path, "geheim", make_store("RT-neu"))

  expect_equal(msgraph_delegated_store_read(path, "geheim")$refresh_token, "RT-neu")
  expect_true(file.exists(bak))
  expect_equal(msgraph_delegated_store_read(bak, "geheim")$refresh_token, "RT-alt")
})

test_that("Kein Temp-Rest nach dem Schreiben", {
  path <- file.path(tempdir(), "store_notmp.txt")
  on.exit(unlink(c(path, paste0(path, ".bak"))), add = TRUE)

  msgraph_delegated_store_write(path, "geheim", make_store())

  expect_false(file.exists(paste0(path, ".tmp")))
})

test_that("Fehlender Store nennt den Pfad und den Bootstrap-Hinweis", {
  expect_error(
    msgraph_delegated_store_read(file.path(tempdir(), "gibtsnicht.txt"), "geheim"),
    "Bootstrap"
  )
})

test_that("Alter wird in Tagen aus obtained_at berechnet", {
  store <- make_store(obtained_at = "2026-07-01T00:00:00Z")
  now   <- as.POSIXct("2026-07-28T00:00:00", tz = "UTC")

  expect_equal(msgraph_delegated_store_age_days(store, now), 27)
})

# --- Fakes fuer httr ----------------------------------------------------------
# Jeder POST liefert die naechste vorbereitete Antwort. Zustand im Environment,
# nicht via <<-.

fake_post_factory <- function(responses) {
  state <- new.env(parent = emptyenv())
  state$i <- 0
  list(
    post = function(url, ...) {
      state$i <- state$i + 1
      responses[[min(state$i, length(responses))]]
    },
    calls = function() state$i
  )
}

fake_content <- function(x, as = "parsed", type = NULL, encoding = NULL) x$payload

ok_response <- function(access = "AT-1", refresh = "RT-2", expires = 3600) {
  list(status_code = 200,
       payload = list(access_token = access, refresh_token = refresh,
                      expires_in = expires))
}

err_response <- function(error = "invalid_grant", desc = "AADSTS700082: expired") {
  list(status_code = 400,
       payload = list(error = error, error_description = desc))
}

with_fake_http <- function(responses, code) {
  fake <- fake_post_factory(responses)
  result <- testthat::with_mocked_bindings(
    code(),
    POST = fake$post, content = fake_content,
    .package = "httr"
  )
  list(result = result, calls = fake$calls())
}

# --- Tests --------------------------------------------------------------------

test_that("Refresh gibt Access-Token und rotiertes Refresh-Token zurueck", {
  out <- with_fake_http(list(ok_response()), function() {
    msgraph_delegated_refresh("t", "c", "s", "RT-1", "offline_access")
  })

  expect_equal(out$result$access_token, "AT-1")
  expect_equal(out$result$refresh_token, "RT-2")
})

test_that("Fehlgeschlagener Refresh nennt AADSTS-Code und Bootstrap-Hinweis", {
  expect_error(
    with_fake_http(list(err_response()), function() {
      msgraph_delegated_refresh("t", "c", "s", "RT-1", "offline_access")
    }),
    "AADSTS700082"
  )
  expect_error(
    with_fake_http(list(err_response()), function() {
      msgraph_delegated_refresh("t", "c", "s", "RT-1", "offline_access")
    }),
    "Bootstrap"
  )
})

test_that("Provider schreibt das rotierte Refresh-Token in den Store", {
  path <- file.path(tempdir(), "store_rotate.txt")
  on.exit(unlink(c(path, paste0(path, ".bak"))), add = TRUE)
  msgraph_delegated_store_write(path, "geheim", make_store("RT-1"))

  out <- with_fake_http(list(ok_response(refresh = "RT-2")), function() {
    provider <- msgraph_make_delegated_token_provider(
      "t", "c", "s", path, "geheim", warn_after_days = 100000)
    provider()
  })

  expect_equal(out$result, "AT-1")
  expect_equal(msgraph_delegated_store_read(path, "geheim")$refresh_token, "RT-2")
})

test_that("Unveraendertes Refresh-Token loest keinen Schreibvorgang aus", {
  path <- file.path(tempdir(), "store_nowrite.txt")
  bak  <- paste0(path, ".bak")
  on.exit(unlink(c(path, bak)), add = TRUE)
  msgraph_delegated_store_write(path, "geheim", make_store("RT-1"))
  unlink(bak)  # ein spaeteres .bak beweist dann einen Schreibvorgang

  with_fake_http(list(ok_response(refresh = "RT-1")), function() {
    provider <- msgraph_make_delegated_token_provider(
      "t", "c", "s", path, "geheim", warn_after_days = 100000)
    provider()
  })

  expect_false(file.exists(bak))
})

test_that("Gueltiges Access-Token wird gecacht, kein zweiter HTTP-Aufruf", {
  path <- file.path(tempdir(), "store_cache.txt")
  on.exit(unlink(c(path, paste0(path, ".bak"))), add = TRUE)
  msgraph_delegated_store_write(path, "geheim", make_store("RT-1"))

  out <- with_fake_http(list(ok_response()), function() {
    provider <- msgraph_make_delegated_token_provider(
      "t", "c", "s", path, "geheim", warn_after_days = 100000)
    provider(); provider(); provider()
  })

  expect_equal(out$calls, 1)
})

test_that("force_refresh erzwingt einen neuen HTTP-Aufruf", {
  path <- file.path(tempdir(), "store_force.txt")
  on.exit(unlink(c(path, paste0(path, ".bak"))), add = TRUE)
  msgraph_delegated_store_write(path, "geheim", make_store("RT-1"))

  out <- with_fake_http(list(ok_response(), ok_response(access = "AT-2")), function() {
    provider <- msgraph_make_delegated_token_provider(
      "t", "c", "s", path, "geheim", warn_after_days = 100000)
    provider(); provider(force_refresh = TRUE)
  })

  expect_equal(out$result, "AT-2")
  expect_equal(out$calls, 2)
})

test_that("Alter Store loest eine Warnung aus", {
  path <- file.path(tempdir(), "store_warn.txt")
  on.exit(unlink(c(path, paste0(path, ".bak"))), add = TRUE)
  msgraph_delegated_store_write(path, "geheim", make_store("RT-1"))

  expect_warning(
    with_fake_http(list(ok_response()), function() {
      provider <- msgraph_make_delegated_token_provider(
        "t", "c", "s", path, "geheim", warn_after_days = 0)
      provider()
    }),
    "Bootstrap"
  )
})
