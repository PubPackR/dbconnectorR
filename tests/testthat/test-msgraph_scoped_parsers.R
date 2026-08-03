test_that("graph_collect folgt @odata.nextLink und aggregiert value", {
  mockery::stub(graph_collect, "graph_get", function(url, token, query = NULL) {
    if (identical(url, "url2")) list(status = 200, content = list(value = list(list(id = "b"))))
    else list(status = 200, content = list(value = list(list(id = "a")), `@odata.nextLink` = "url2"))
  })
  res <- graph_collect("url1", token = "tok")
  expect_equal(res$status, 200)
  expect_equal(length(res$value), 2)
  expect_equal(res$value[[2]]$id, "b")
})

test_that("parse_scoped_user mappt Graph-User auf raw.msgraph_users-Schema (email/upn lowercase)", {
  u <- list(id = "OID123", givenName = "Tim", surname = "Roensch",
            userPrincipalName = "Tim.Roensch@studyflix.de", displayName = "Tim Roensch",
            mail = "Tim.Roensch@studyflix.de")
  row <- parse_scoped_user(u)
  expect_equal(nrow(row), 1)
  expect_equal(row$msgraph_user_id, "OID123")
  expect_equal(row$first_name, "Tim")
  expect_equal(row$name, "Roensch")
  expect_equal(row$email, "tim.roensch@studyflix.de")
  expect_equal(row$user_principal_name, "tim.roensch@studyflix.de")
  expect_true(row$is_internal)
  expect_false(row$is_deleted)
})
