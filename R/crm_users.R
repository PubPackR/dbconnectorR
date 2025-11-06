  #' Download and upsert CRM users
  #'
  #' Downloads all users from Central Station CRM and upserts them into the database.
  #'
  #' @param con A DBI database connection.
  #' @param crm_key Authentication key for Central Station CRM.
  #' @return Invisibly returns the upserted users data.frame.
  #' @export
  crm_update_users <- function(con, crm_key) {
    users <- get_all_users_crm(crm_key)
    upsert_delete_missing(
      con,
      "raw.crm_users",
      users,
      match_cols = c("crm_user_id")
    )
    invisible(users)
  }

  get_all_users_crm <- function(crm_key) {
    # Initial URL
    url_base <- "https://api.centralstationcrm.net/api/users?perpage=250&page="
    headers <- c(
      "content-type" = "application/json",
      "X-apikey" = crm_key,
      "Accept" = "*/*"
    )

    # Create an empty data frame to store all the data
    all_data <- data.frame()

    # Start with page 1
    page <- 1

    # Loop to download all pages
    repeat {
      # Construct the URL for the current page
      url <- paste0(url_base, page)

      # Send GET request
      response <- httr::GET(url, httr::add_headers(headers))

      # Parse the response
      data <- jsonlite::fromJSON(httr::content(response, "text")) %>%
        tidyr::unnest(user)

      # Append to the all_data data frame
      all_data <- dplyr::bind_rows(all_data, data)

      # Check if the number of entries is less than 250 (indicating the last page)
      if (nrow(data) < 250) {
        break
      }

      # Move to the next page
      page <- page + 1
    }

    all_data <- all_data %>%
      dplyr::rename(
        user_first_name = first,
        user_name = name,
        user_login = login,
        crm_user_id = id
      ) %>%
      dplyr::mutate(is_deleted = FALSE)

    # Create an empty data frame to store all active users
    active_users <- data.frame()

    # Start with page 1
    page <- 1

    # Loop to download all pages
    repeat {
      # Construct the URL for the current page
      url <- paste0(url_base, page, "&active=active")

      # Send GET request
      response <- httr::GET(url, httr::add_headers(headers))

      # Parse the response
      data <- jsonlite::fromJSON(httr::content(response, "text")) %>%
        tidyr::unnest(user)

      # Append to the all_data data frame
      active_users <- dplyr::bind_rows(active_users, data)

      # Check if the number of entries is less than 250 (indicating the last page)
      if (nrow(data) < 250) {
        break
      }

      # Move to the next page
      page <- page + 1
    }

    all_data <- all_data %>%
      dplyr::mutate(is_active = ifelse(crm_user_id %in% active_users$id, TRUE, FALSE))

    return(all_data)
  }