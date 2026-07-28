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
