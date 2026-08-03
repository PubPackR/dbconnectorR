`%||%` <- function(a, b) if (is.null(a)) b else a

#' Einzelner Graph-GET
#' @param url Voll-URL. @param token Provider-Closure oder String. @param query optionale Query-Liste.
#' @return list(status, content)
#' @export
graph_get <- function(url, token, query = NULL) {
  # ---- start ---- #
  bearer <- if (is.function(token)) token() else token
  resp <- if (is.null(query)) httr::GET(url, httr::add_headers(Authorization = paste("Bearer", bearer)))
          else httr::GET(url, query = query, httr::add_headers(Authorization = paste("Bearer", bearer)))
  list(status = httr::status_code(resp),
       content = httr::content(resp, as = "parsed", type = "application/json"))
}

#' Graph-GET mit Paging (@odata.nextLink)
#' @inheritParams graph_get
#' @return list(status, error, value)
#' @export
graph_collect <- function(url, token, query = NULL) {
  # ---- start ---- #
  values <- list(); status_first <- NA_integer_
  repeat {
    res <- graph_get(url, token, query)
    if (is.na(status_first)) status_first <- res$status
    if (res$status != 200) return(list(status = res$status, error = res$content$error, value = list()))
    if (!is.null(res$content$value)) values <- c(values, res$content$value)
    nxt <- res$content$`@odata.nextLink`
    if (is.null(nxt)) break
    url <- nxt; query <- NULL
  }
  list(status = status_first, error = NULL, value = values)
}

#' UPN -> ObjectId (List-Endpoint mit $filter)
#' @param upn userPrincipalName. @param token Provider/String.
#' @return list(status, id)
#' @export
resolve_user_id <- function(upn, token) {
  # ---- start ---- #
  res <- graph_get("https://graph.microsoft.com/v1.0/users", token,
                   query = list(`$filter` = paste0("userPrincipalName eq '", upn, "'"), `$select` = "id"))
  v <- res$content$value
  list(status = res$status,
       id = if (!is.null(v) && length(v) > 0) v[[1]]$id %||% NA_character_ else NA_character_)
}
