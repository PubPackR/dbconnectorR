# Sprecher-Erkennung im Transkript-Klartext (Asana 1220382115257832).
# Der Klartext kommt aus vtt_to_plaintext(): eine Zeile je Sprechbeitrag,
# Form "Label: Text".

klartext <- function(...) paste(c(...), collapse = "\n")

test_that("transcript_speakers zaehlt Zeilen je Transkript und Label", {
  tr <- c(
    klartext("Krampitz, Svea: Hallo", "Mick, Tanja, Studyflix GmbH: Hallo zusammen",
             "Krampitz, Svea: Gerne"),
    klartext("Mick, Tanja, Studyflix GmbH: Ist jemand da?")
  )
  sp <- transcript_speakers(tr)
  expect_equal(nrow(sp), 3)
  expect_equal(sp$n_lines[sp$transcript == 1 & sp$label == "Krampitz, Svea"], 2L)
  expect_equal(sp$n_lines[sp$transcript == 1 & sp$label == "Mick, Tanja, Studyflix GmbH"], 1L)
  expect_equal(sp$label[sp$transcript == 2], "Mick, Tanja, Studyflix GmbH")
  expect_type(sp$n_lines, "integer")
})

test_that("transcript_speakers verkraftet NA, leere Transkripte und Zeilen ohne Sprecher", {
  sp <- transcript_speakers(c(NA_character_, "", klartext("ohne Sprecher", "10:30 Uhr passt")))
  expect_equal(nrow(sp), 0)
  expect_named(sp, c("transcript", "label", "n_lines"))
  expect_equal(nrow(transcript_speakers(character(0))), 0)
})

test_that("transcript_speakers nimmt auch eine Zeile ohne Text nach dem Label", {
  # vtt_to_plaintext() trimmt die Zeile, ein leerer Beitrag endet auf ":"
  sp <- transcript_speakers(klartext("@1:", "@1: Hallo"))
  expect_equal(sp$label, "@1")
  expect_equal(sp$n_lines, 2L)
})

test_that("is_internal_speaker erkennt das Studyflix-Kuerzel, sonst extern", {
  expect_equal(
    is_internal_speaker(c("Mick, Tanja, Studyflix GmbH", "mick, tanja, studyflix gmbh ",
                          "Krampitz, Svea", "Petry, Kathrin (EBS)", "@1",
                          "Michael Schmitz | Ruhe Agrar GmbH")),
    c(TRUE, TRUE, FALSE, FALSE, FALSE, FALSE))
  expect_true(is.na(is_internal_speaker(NA_character_)))
})

test_that("count_external_speakers zaehlt externe Sprecher ab min_lines Zeilen", {
  tr <- c(
    # Fall 789903: externe Sprecherin mit mehreren Zeilen
    klartext("Krampitz, Svea: Hallo", "Mick, Tanja, Studyflix GmbH: Hallo",
             "Krampitz, Svea: Danke"),
    # Fall 756559: nur interne Sprecher
    klartext("Casablanca, Marco, Studyflix GmbH: Hallo",
             "Weißenberger, Lisa, Studyflix GmbH: Hallo", "Casablanca, Marco, Studyflix GmbH: Tschüss"),
    # eine einzelne Zeile ohne Sprecher-Tag mit Doppelpunkt
    klartext("Mick, Tanja, Studyflix GmbH: Wir warten", "Wichtig ist: die Agenda",
             "Mick, Tanja, Studyflix GmbH: Niemand da"),
    # unbenannter Sprecher zaehlt als extern
    klartext("@1: Hallo", "@1: Ja", "Egger, Peter, Studyflix GmbH: Hallo"),
    NA_character_
  )
  expect_equal(count_external_speakers(tr), c(1L, 0L, 0L, 1L, 0L))
  expect_equal(count_external_speakers(tr, min_lines = 1L), c(1L, 0L, 1L, 1L, 0L))
  expect_equal(count_external_speakers(character(0)), integer(0))
})
