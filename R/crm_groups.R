#' Ensure all given CRM group_ids exist in raw.crm_groups and return the mapping
#'
#' For every group_id that is not yet registered in raw.crm_groups a stub row
#' is inserted with placeholder name "UNKNOWN_<group_id>" and is_stub = TRUE.
#' Existing entries are never modified - group names are maintained by hand,
#' because CentralStation CRM has no groups API endpoint.
#'
#' Returns the mapping `(id, group_id)` for all groups so that callers can
#' resolve the CRM-side group_id to the internal surrogate id via
#' `resolve_group_id()` before upserting raw.crm_leads / raw.crm_companies.
#'
#' @param con A DBI database connection.
#' @param group_ids Integer vector of CRM group_ids (NAs are dropped), or a
#'   data.frame with a group_id column.
#' @return A tibble with columns `id` and `group_id` covering all existing
#'   entries in raw.crm_groups (including newly inserted stubs).
#' @export
crm_update_groups <- function(con, group_ids) {
  if (is.data.frame(group_ids)) {
    group_ids <- group_ids$group_id
  }
  group_ids <- unique(group_ids[!is.na(group_ids)])

  existing <- dplyr::tbl(con, I("raw.crm_groups")) %>%
    dplyr::select(group_id) %>%
    dplyr::collect() %>%
    dplyr::pull(group_id)

  new_ids <- setdiff(group_ids, existing)

  if (length(new_ids) > 0) {
    stubs <- tibble::tibble(
      group_id = as.integer(new_ids),
      name = paste0("UNKNOWN_", new_ids),
      is_stub = TRUE
    )

    warning(sprintf(
      "Found %d new CRM group_id(s) without name: %s. Inserted as stubs into raw.crm_groups - please maintain names manually.",
      nrow(stubs),
      paste(new_ids, collapse = ", ")
    ))

    upsert_no_delete(con, "raw.crm_groups", stubs, match_cols = c("group_id"))
  }

  dplyr::tbl(con, I("raw.crm_groups")) %>%
    dplyr::select("id", "group_id") %>%
    dplyr::collect()
}
