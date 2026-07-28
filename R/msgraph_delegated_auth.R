################################################################################-
# ----- Description -------------------------------------------------------------
#
# Delegierte MSGraph-Authentifizierung ohne Device Code.
# Authorization Code Flow mit Confidential Client: einmaliger interaktiver Login
# (Bootstrap-Skript), danach unbeaufsichtigte Refreshs aus einem verschluesselten
# Token-Store.
#
# Ergaenzt msgraph_make_token_provider() (app-only), ersetzt sie NICHT.
#
# ------------------------------------------------------------------ #
# Authors@R: Moritz Hemmann
# Date: 2026/07
#

#' Verschluesselten Token-Store lesen
#'
#' @param path Pfad zur verschluesselten Store-Datei.
#' @param key Entschluesselungsschluessel.
#' @return Liste mit refresh_token, obtained_at, last_refreshed_at, tenant_id,
#'   client_id, scopes.
#' @export
msgraph_delegated_store_read <- function(path, key) {
  # ---- start ---- #
  if (!file.exists(path)) {
    stop("Token-Store nicht gefunden: ", path,
         "\nBootstrap ausfuehren, um ihn anzulegen.", call. = FALSE)
  }

  cipher <- paste(readLines(path, warn = FALSE), collapse = "")
  json <- safer::decrypt_string(cipher, key = key)
  jsonlite::fromJSON(json, simplifyVector = TRUE)
}

#' Verschluesselten Token-Store schreiben
#'
#' Schreibt ueber eine Temp-Datei und legt den Vorgaenger als .bak ab.
#'
#' @param path Zielpfad.
#' @param key Verschluesselungsschluessel.
#' @param store Liste wie von msgraph_delegated_store_read() geliefert.
#' @return invisible(path)
#' @export
msgraph_delegated_store_write <- function(path, key, store) {
  # ---- start ---- #
  json <- as.character(jsonlite::toJSON(store, auto_unbox = TRUE))
  cipher <- safer::encrypt_string(json, key = key)
  # Zeilenumbrueche im Chiffrat wuerden das einzeilige Wiedereinlesen zerstoeren
  cipher <- gsub("[\r\n]", "", cipher)

  tmp <- paste0(path, ".tmp")
  writeLines(cipher, tmp)

  if (file.exists(path)) {
    file.copy(path, paste0(path, ".bak"), overwrite = TRUE)
    # file.rename() scheitert unter Windows, wenn das Ziel existiert
    unlink(path)
  }

  if (!file.rename(tmp, path)) {
    unlink(tmp)
    stop("Token-Store konnte nicht ersetzt werden: ", path, call. = FALSE)
  }

  invisible(path)
}

#' Alter des Refresh-Tokens in Tagen
#'
#' Gemessen ab dem letzten interaktiven Login (obtained_at), nicht ab dem
#' letzten Refresh — massgeblich ist das Alter der Anmeldung.
#'
#' @param store Liste aus msgraph_delegated_store_read().
#' @param now Referenzzeitpunkt.
#' @return Alter in Tagen (numeric).
#' @export
msgraph_delegated_store_age_days <- function(store, now = Sys.time()) {
  # ---- start ---- #
  obtained <- as.POSIXct(store$obtained_at, format = "%Y-%m-%dT%H:%M:%SZ", tz = "UTC")
  as.numeric(difftime(now, obtained, units = "days"))
}
