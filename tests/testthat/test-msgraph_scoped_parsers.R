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
