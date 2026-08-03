test_that("graph_collect folgt @odata.nextLink und aggregiert value", {
  page1 <- list(status_code = 200)
  calls <- 0
  fake_GET <- function(url, ...) { calls <<- calls + 1; structure(list(), class = "response") }
  fake_content <- function(resp, ...) {
    if (calls == 1) list(value = list(list(id = "a")), `@odata.nextLink` = "url2")
    else list(value = list(list(id = "b")))
  }
  mockery::stub(graph_collect, "graph_get", function(url, token, query = NULL) {
    if (identical(url, "url2")) list(status = 200, content = list(value = list(list(id = "b"))))
    else list(status = 200, content = list(value = list(list(id = "a")), `@odata.nextLink` = "url2"))
  })
  res <- graph_collect("url1", token = "tok")
  expect_equal(res$status, 200)
  expect_equal(length(res$value), 2)
  expect_equal(res$value[[2]]$id, "b")
})
