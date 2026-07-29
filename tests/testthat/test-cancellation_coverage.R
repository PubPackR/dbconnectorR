test_that("volle Abdeckung storniert normal", {
  res <- cancellation_coverage_check(in_scope_n = 100, to_cancel_n = 0)
  expect_equal(res$coverage, 1)
  expect_false(res$skip)
})

test_that("einzelne echte Absagen bleiben erlaubt", {
  # Realfall: 8 Absagen bei rund 100 Booking-Events im Zeitfenster
  res <- cancellation_coverage_check(in_scope_n = 104, to_cancel_n = 8)
  expect_gt(res$coverage, 0.9)
  expect_false(res$skip)
})

test_that("unvollstaendiger Download wird geblockt", {
  # API liefert nur ein Viertel zurueck -> der Rest waere faelschlich abgesagt
  res <- cancellation_coverage_check(in_scope_n = 100, to_cancel_n = 75)
  expect_equal(res$coverage, 0.25)
  expect_true(res$skip)
})

test_that("Totalausfall wird geblockt", {
  res <- cancellation_coverage_check(in_scope_n = 100, to_cancel_n = 100)
  expect_equal(res$coverage, 0)
  expect_true(res$skip)
})

test_that("leerer Scope storniert nicht und blockt nicht", {
  # Nichts im Bestand: es gibt nichts zu stornieren, also auch keinen Alarm
  res <- cancellation_coverage_check(in_scope_n = 0, to_cancel_n = 0)
  expect_equal(res$coverage, 1)
  expect_false(res$skip)
})

test_that("Schwelle ist genau und konfigurierbar", {
  # exakt auf der Schwelle wird noch storniert, knapp darunter nicht mehr
  expect_false(cancellation_coverage_check(100, 50, min_coverage = 0.5)$skip)
  expect_true(cancellation_coverage_check(100, 51, min_coverage = 0.5)$skip)

  # strengere Schwelle blockt frueher
  expect_true(cancellation_coverage_check(100, 5, min_coverage = 0.99)$skip)
  expect_false(cancellation_coverage_check(100, 5, min_coverage = 0.90)$skip)
})
