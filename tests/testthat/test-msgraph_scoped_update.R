# Contract-Tests fuer die scoped update_*-Funktionen (Go-Live-Blocker-Fixes):
# - dry_run darf NICHT schreiben (kein Upsert)
# - kein Massen-Soft-Delete mehr (kein DBI::dbExecute-UPDATE)
# Hinweis: mockery::stub() muss IM test_that-Block stehen (Scope), nicht in einem Helper.

fake_cfg <- list(service_account_upn = "sa@studyflix.de",
                 raw_schema = "raw", processed_schema = "processed")

fake_calendars <- function(url, token, query = NULL)
  list(status = 200, value = list(list(owner = list(address = "rep.a@studyflix.de"))))

fake_user_lookup <- function(url, token, query = NULL)
  list(content = list(value = list(list(id = "OID1", givenName = "Rep", surname = "A",
    userPrincipalName = "rep.a@studyflix.de", displayName = "Rep A", mail = "rep.a@studyflix.de"))))

test_that("msgraph_scoped_update_users: dry_run schreibt NICHT (kein Upsert)", {
  upsert <- mockery::mock()
  mockery::stub(msgraph_scoped_update_users, "graph_collect", fake_calendars)
  mockery::stub(msgraph_scoped_update_users, "graph_get", fake_user_lookup)
  mockery::stub(msgraph_scoped_update_users, "Billomatics::postgres_upsert_data", upsert)

  n <- msgraph_scoped_update_users(con = NULL, app_token = "t", del_token = "t",
                                   cfg = fake_cfg, dry_run = TRUE)

  expect_equal(n, 1)
  mockery::expect_called(upsert, 0)
})

test_that("msgraph_scoped_update_users: ohne dry_run upsertet und macht KEINEN Massen-Soft-Delete", {
  upsert <- mockery::mock()
  exec   <- mockery::mock()   # DBI::dbExecute (frueher der Soft-Delete) darf nie feuern
  mockery::stub(msgraph_scoped_update_users, "graph_collect", fake_calendars)
  mockery::stub(msgraph_scoped_update_users, "graph_get", fake_user_lookup)
  mockery::stub(msgraph_scoped_update_users, "Billomatics::postgres_upsert_data", upsert)
  mockery::stub(msgraph_scoped_update_users, "DBI::dbExecute", exec)

  n <- msgraph_scoped_update_users(con = NULL, app_token = "t", del_token = "t",
                                   cfg = fake_cfg, dry_run = FALSE)

  expect_equal(n, 1)
  mockery::expect_called(upsert, 1)
  mockery::expect_called(exec, 0)
})
