# Tests fuer drop_attachments_without_file_metadata(): CentralStation liefert
# gelegentlich Attachment-Objekte ohne Datei dahinter (leere / fehlgeschlagene
# Uploads) mit content_type / file_size = NA. Diese Spalten sind im raw-Schema
# NOT NULL, ein ungefilterter Upsert bricht den ganzen Ingestion-Job ab. Der
# Helper muss solche Zeilen entfernen und warnen.

make_attachments <- function() {
  data.frame(
    crm_attachment_id = c("aaaa-1111", "f7eda81c-empty", "bbbb-2222"),
    user_id           = c(64L, 64L, 65L),
    protocol_id       = c(10L, 11L, 12L),
    filename          = c("a.pdf", NA, "b.pdf"),
    content_type      = c("application/pdf", NA, "image/png"),
    file_size         = c(1234L, NA, 5678L),
    is_deleted        = c(FALSE, FALSE, FALSE),
    stringsAsFactors  = FALSE
  )
}

test_that("Zeile mit NA content_type wird entfernt, valide bleiben", {
  out <- suppressWarnings(drop_attachments_without_file_metadata(make_attachments()))
  expect_equal(nrow(out), 2L)
  expect_equal(sort(out$crm_attachment_id), c("aaaa-1111", "bbbb-2222"))
})

test_that("Zeile mit NA file_size (aber gesetztem content_type) wird entfernt", {
  att <- make_attachments()
  att$content_type[2] <- "application/pdf"  # nur file_size fehlt
  out <- suppressWarnings(drop_attachments_without_file_metadata(att))
  expect_equal(nrow(out), 2L)
  expect_false("f7eda81c-empty" %in% out$crm_attachment_id)
})

test_that("Warnung nennt die betroffene crm_attachment_id", {
  expect_warning(
    drop_attachments_without_file_metadata(make_attachments()),
    "f7eda81c-empty"
  )
})

test_that("vollstaendige Attachments bleiben unveraendert, keine Warnung", {
  att <- make_attachments()[c(1, 3), ]
  expect_warning(out <- drop_attachments_without_file_metadata(att), NA)
  expect_equal(nrow(out), 2L)
  expect_identical(out$crm_attachment_id, att$crm_attachment_id)
})

test_that("leerer Input liefert leeren Output ohne Fehler", {
  att <- make_attachments()[0, ]
  expect_warning(out <- drop_attachments_without_file_metadata(att), NA)
  expect_equal(nrow(out), 0L)
})

test_that("attachment_kind erscheint im Warntext", {
  expect_warning(
    drop_attachments_without_file_metadata(make_attachments(), attachment_kind = "comment"),
    "comment"
  )
})

# Tests for keep_downloaded_columns(): crm_update_protocols() re-reads the stored
# rows of attachments missing from the download, sets is_deleted and upserts them
# together with the downloaded rows. A column that only the stored rows carry
# (storage_cleanup_marked_at, set by hand before a Speicherbereinigung) must not
# reach the upsert: bind_rows() would fill it with NA for every downloaded row and
# the upsert would clear it in the database.

make_stored_attachments <- function() {
  data.frame(
    crm_attachment_id         = c("cccc-3333", "dddd-4444"),
    user_id                   = c(64L, 65L),
    protocol_id               = c(10L, 12L),
    filename                  = c("Angebot.pdf", "AB2001.pdf"),
    content_type              = c("application/pdf", "application/pdf"),
    file_size                 = c(111L, 222L),
    is_deleted                = c(TRUE, TRUE),
    storage_cleanup_marked_at = as.POSIXct(c("2026-11-17 11:30:48", NA), tz = "UTC"),
    stringsAsFactors          = FALSE
  )
}

test_that("a column only the stored rows carry is dropped", {
  out <- keep_downloaded_columns(make_stored_attachments(), make_attachments())
  expect_false("storage_cleanup_marked_at" %in% names(out))
  expect_setequal(names(out), names(make_attachments()))
})

test_that("rows and values of the shared columns stay unchanged", {
  stored <- make_stored_attachments()
  out <- keep_downloaded_columns(stored, make_attachments())
  expect_equal(nrow(out), 2L)
  expect_identical(out$crm_attachment_id, stored$crm_attachment_id)
  expect_identical(out$is_deleted, stored$is_deleted)
})

test_that("the upsert data built like crm_update_protocols() has no mark column", {
  downloaded <- make_attachments()[c(1, 3), ]
  data <- dplyr::bind_rows(
    downloaded,
    keep_downloaded_columns(make_stored_attachments(), downloaded)
  )
  expect_false("storage_cleanup_marked_at" %in% names(data))
  expect_equal(nrow(data), 4L)
})

test_that("an empty download still restricts the stored rows to its columns", {
  out <- keep_downloaded_columns(make_stored_attachments(), make_attachments()[0, ])
  expect_false("storage_cleanup_marked_at" %in% names(out))
  expect_equal(nrow(out), 2L)
})

test_that("no stored rows give an empty result without error", {
  out <- keep_downloaded_columns(make_stored_attachments()[0, ], make_attachments())
  expect_equal(nrow(out), 0L)
  expect_false("storage_cleanup_marked_at" %in% names(out))
})
