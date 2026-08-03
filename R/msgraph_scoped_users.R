#' Graph-User -> raw.msgraph_users-Zeile (rein)
#'
#' @param user_obj Graph-User-Objekt (Felder id, givenName, surname, userPrincipalName, displayName, mail).
#' @return tibble mit einer Zeile im raw.msgraph_users-Schema.
#' @export
parse_scoped_user <- function(user_obj) {
  # ---- start ---- #
  tibble::tibble(
    msgraph_user_id     = user_obj$id %||% NA_character_,
    first_name          = user_obj$givenName %||% NA_character_,
    name                = user_obj$surname %||% NA_character_,
    email               = tolower(user_obj$mail %||% NA_character_),
    display_name        = user_obj$displayName %||% NA_character_,
    user_principal_name = tolower(user_obj$userPrincipalName %||% NA_character_),
    is_internal         = TRUE,
    is_deleted          = FALSE)
}

#' Interne User aus den freigegebenen Kalendern pflegen (delegiert + app-only)
#'
#' Owner der dem Service-Account freigegebenen Kalender (del_token) werden per
#' app-only User.ReadBasic.All aufgeloest und in raw.msgraph_users upsertet.
#' Interne User, die nicht mehr auftauchen, werden soft-deleted (is_deleted = TRUE).
#'
#' @param con DB-Pool.
#' @param app_token app-only Token-Provider (User.ReadBasic.All).
#' @param del_token delegierter Token-Provider (Calendars.Read.Shared).
#' @param cfg load_scoped_config() (aktuell ungenutzt; fuer einheitliche Aufrufsignatur der scoped-Funktionen).
#' @return invisible(Anzahl aktiver interner User).
#' @export
msgraph_scoped_update_users <- function(con, app_token, del_token, cfg) {
  # ---- start ---- #
  cals <- graph_collect("https://graph.microsoft.com/v1.0/me/calendars", del_token)
  if (cals$status != 200) stop("Kalenderliste HTTP ", cals$status)
  owners <- unique(tolower(unlist(lapply(
    Filter(function(x) isTRUE(x$isSharedWithMe), cals$value),
    function(x) x$owner$address %||% NA_character_))))
  owners <- owners[!is.na(owners) & nzchar(owners)]
  if (length(owners) == 0) { message("Keine freigegebenen Kalender/Owner."); return(invisible(0L)) }

  rows <- list()
  for (addr in owners) {
    res <- graph_get("https://graph.microsoft.com/v1.0/users", app_token,
                     query = list(`$filter` = paste0("userPrincipalName eq '", addr, "' or mail eq '", addr, "'"),
                                  `$select` = "id,givenName,surname,userPrincipalName,displayName,mail"))
    v <- res$content$value
    if (!is.null(v) && length(v) > 0) rows[[length(rows) + 1]] <- parse_scoped_user(v[[1]])
  }
  if (length(rows) == 0) { message("Keine User aufloesbar."); return(invisible(0L)) }
  users_df <- dplyr::distinct(dplyr::bind_rows(rows), msgraph_user_id, .keep_all = TRUE)

  Billomatics::postgres_upsert_data(con, "raw", "msgraph_users", users_df,
                                    match_cols = "msgraph_user_id")

  # Soft-Delete: interne User, die diesmal NICHT geliefert wurden.
  # active_ids stammen aus der Graph-API -> sicher quoten (kein rohes paste0 in SQL).
  active_ids <- users_df$msgraph_user_id
  quoted_ids <- paste(DBI::dbQuoteLiteral(con, active_ids), collapse = ", ")
  DBI::dbExecute(con, paste0(
    "UPDATE raw.msgraph_users SET is_deleted = TRUE ",
    "WHERE is_internal AND NOT is_deleted AND msgraph_user_id NOT IN (", quoted_ids, ")"))

  invisible(nrow(users_df))
}
