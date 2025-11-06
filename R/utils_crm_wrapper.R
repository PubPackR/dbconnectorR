  
# Wrapper-Funktionen für die Upsert-Operationen in PostgreSQL
parse_table_name <- function(full_table_name) {
  parts <- strsplit(full_table_name, "\\.")[[1]]
  list(schema = parts[1], table = parts[2])
}

upsert_table <- function(
  con,
  full_table_name,
  data,
  match_cols,
  delete_missing = FALSE
) {
  tbl <- parse_table_name(full_table_name)
  data <- data %>% dplyr::distinct(across(all_of(match_cols)), .keep_all = TRUE)
  Billomatics::postgres_upsert_data(
    con,
    tbl$schema,
    tbl$table,
    data,
    match_cols = match_cols,
    delete_missing = delete_missing
  )
}

# Drei einfache Wrapper
upsert_delete_missing <- function(con, full_table_name, data, match_cols) {
  upsert_table(con, full_table_name, data, match_cols, delete_missing = TRUE)
}

upsert_no_delete <- function(con, full_table_name, data, match_cols) {
  upsert_table(con, full_table_name, data, match_cols, delete_missing = FALSE)
}

upsert_delete_variable <- function(
  con,
  full_table_name,
  data,
  match_cols,
  is_daily
) {
  upsert_table(
    con,
    full_table_name,
    data,
    match_cols,
    delete_missing = !is_daily
  )
}

# Funktion zum Ersetzen von IDs in einem DataFrame basierend auf einem Referenz-DataFrame
replace_ids <- function(
  df,
  reference_df,
  cols_df,
  cols_ref = rep("id", length(cols_df)),
  join_cols_ref = rep("id", length(cols_df))
) {
  stopifnot(
    length(cols_df) == length(join_cols_ref),
    length(cols_ref) == length(cols_df)
  )

  for (i in seq_along(cols_df)) {
    df_col <- cols_df[i]
    ref_col <- join_cols_ref[i]
    new_id_col <- cols_ref[i]

    df <- df %>%
      dplyr::left_join(reference_df, by = setNames(ref_col, df_col)) %>%
      dplyr::mutate(!!sym(df_col) := .data[[new_id_col]]) %>%
      dplyr::select(-all_of(new_id_col))
  }

  df
}

# Wrapper-Funktionen zum Ersetzen von IDs in DataFrames
resolve_company_ids <- function(df, companies_df, col = "company_id") {
  replace_ids(df, companies_df, col, join_cols_ref = "crm_company_id")
}

resolve_user_ids <- function(df, user_df, cols = c("crm_user_id")) {
  replace_ids(
    df,
    user_df,
    cols,
    join_cols_ref = rep("crm_user_id", length(cols))
  )
}

resolve_lead_id <- function(df, leads_df, col = "lead_id") {
  replace_ids(df, leads_df, cols_df = col, join_cols_ref = "crm_lead_id")
}

resolve_protocol_id <- function(df, protocols_df, col = "protocol_id") {
  replace_ids(
    df,
    protocols_df,
    cols_df = col,
    join_cols_ref = "crm_protocol_id"
  )
}

resolve_comment_ids <- function(df, comments_df, col = "comment_id") {
  replace_ids(
    df,
    comments_df,
    cols_df = col,
    join_cols_ref = "crm_comment_id"
  )
}

batch_upsert <- function(
  con,
  schema,
  table,
  data,
  match_cols,
  batch_size = 10000
) {
  results <- list()
  n <- nrow(data)

  for (start in seq(1, n, by = batch_size)) {
    batch <- data[start:min(start + batch_size - 1, n), ]

    Billomatics::postgres_upsert_data(
      con = con,
      schema = schema,
      table = table,
      data = batch,
      match_cols = match_cols,
      delete_missing = FALSE
    )

    print("New batch uploaded")
  }
}