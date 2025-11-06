#' Retrieve and upsert all users from MS Graph to PostgreSQL
#'
#' @param con PostgreSQL connection object
#' @param access_token MS Graph access token
#'
#' @return NULL. Upserts user data into the database.
#'
#' @export
#' @examples
#' msgraph_update_users(access_token, con)
msgraph_update_users <- function(con, access_token) {

  all_users <- retrieve_all_users(access_token)

  # Convert inner list to data frame
  all_users_df <- lapply(all_users, function(user_list) {
    user_list_clean <- lapply(user_list, function(x) if(length(x) == 0 || is.null(x)) NA else x)
    as.data.frame(user_list_clean)
  })

  df <- dplyr::bind_rows(all_users_df) %>%
    tidyr::drop_na(userPrincipalName) %>%
    dplyr::select(msgraph_user_id = id,
           first_name = givenName,
           name = surname,
           user_principal_name = userPrincipalName,
           display_name = displayName,
           email = mail) %>%
    dplyr::mutate(is_deleted = FALSE) %>%
    dplyr::mutate(is_internal = TRUE) %>%
    dplyr::bind_rows(dplyr::tbl(con, I("raw.msgraph_users")) %>% dplyr::filter(is_internal == FALSE) %>% dplyr::select(-id, -created_at, -updated_at) %>% dplyr::collect())

  Billomatics::postgres_upsert_data(
    con = con,
    schema = "raw",
    table = "msgraph_users",
    data = df,
    match_cols = c("msgraph_user_id"),
    delete_missing = TRUE
  )
}

retrieve_all_users <- function(access_token) {
  user_records_url <- "https://graph.microsoft.com/v1.0/users"
  all_users <- list()  # Initialize an empty list to store all results

  repeat {

    # Send the GET request for the current page
    response <- httr::GET(user_records_url, httr::add_headers(Authorization = paste("Bearer", access_token)))

    # Parse the response
    user_records <- httr::content(response, as = "parsed", type = "application/json")

    # Append the current page of results to the list
    if (!is.null(user_records$value)) {
      all_users <- c(all_users, user_records$value)
    }

    # Check if there's a next page
    if (!is.null(user_records$`@odata.nextLink`)) {
      user_records_url <- user_records$`@odata.nextLink`
    } else {
      break  # Exit the loop if there are no more pages
    }
  }

  return(all_users)
}