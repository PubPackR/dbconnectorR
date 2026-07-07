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
