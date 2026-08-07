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

#' Access-Token ueber ein Refresh-Token erneuern
#'
#' Confidential Client: client_secret geht mit. Wirft bei HTTP != 200 mit dem
#' AADSTS-Code, damit im Log sofort die Ursache steht.
#'
#' @param tenant_id Tenant-ID.
#' @param client_id Client-ID der App-Registrierung.
#' @param client_secret Client-Secret.
#' @param refresh_token Aktuelles Refresh-Token.
#' @param scopes Character-Vektor der Scopes.
#' @return Geparste Entra-Antwort (access_token, expires_in, meist refresh_token).
#' @export
msgraph_delegated_refresh <- function(tenant_id, client_id, client_secret,
                                      refresh_token, scopes) {
  # ---- start ---- #
  uri <- paste0("https://login.microsoftonline.com/", tenant_id, "/oauth2/v2.0/token")

  response <- httr::POST(uri, encode = "form", body = list(
    grant_type    = "refresh_token",
    client_id     = client_id,
    client_secret = client_secret,
    refresh_token = refresh_token,
    scope         = paste(scopes, collapse = " ")
  ))

  parsed <- httr::content(response, as = "parsed", type = "application/json")
  # Direktzugriff statt httr::status_code() - so machen es die uebrigen
  # MSGraph-Funktionen im Paket, und der Test braucht einen Mock weniger.
  status <- response$status_code

  if (status != 200) {
    error_code <- if (is.null(parsed$error)) "unbekannt" else parsed$error
    error_desc <- if (is.null(parsed$error_description)) {
      "keine Beschreibung"
    } else {
      parsed$error_description
    }
    stop(sprintf(paste0(
      "MSGraph-Token-Refresh fehlgeschlagen (HTTP %s): %s\n%s\n",
      "Bootstrap erneut ausfuehren, um ein neues Refresh-Token zu holen."),
      status, error_code, error_desc), call. = FALSE)
  }

  parsed
}

#' Provider fuer delegierte MSGraph-Tokens
#'
#' Liefert dieselbe Closure-Signatur wie msgraph_make_token_provider(), ist also
#' bei allen Konsumenten ein reiner Token-Tausch. Haelt das Access-Token fuer die
#' Prozesslaufzeit im Speicher und schreibt den Store nur, wenn Entra tatsaechlich
#' ein neues Refresh-Token liefert.
#'
#' @param tenant_id Tenant-ID.
#' @param client_id Client-ID der App-Registrierung.
#' @param client_secret Client-Secret.
#' @param token_store_path Pfad zum verschluesselten Token-Store.
#' @param store_key Schluessel fuer den Store.
#' @param scopes Character-Vektor der Scopes.
#' @param refresh_buffer_seconds Vorlauf, ab dem vorsorglich erneuert wird.
#' @param warn_after_days Alter des Logins, ab dem gewarnt wird.
#' @return function(force_refresh = FALSE), liefert das Access-Token als String.
#' @export
msgraph_make_delegated_token_provider <- function(
    tenant_id, client_id, client_secret,
    token_store_path, store_key,
    scopes = c("https://graph.microsoft.com/Calendars.Read.Shared",
               "https://graph.microsoft.com/User.Read",
               "offline_access"),
    refresh_buffer_seconds = 300,
    warn_after_days = 75) {
  # ---- start ---- #
  cache <- new.env(parent = emptyenv())
  cache$token <- NULL
  cache$exp <- as.POSIXct(NA)

  function(force_refresh = FALSE) {
    now <- Sys.time()
    needs_refresh <- force_refresh ||
      is.null(cache$token) ||
      is.na(cache$exp) ||
      as.numeric(difftime(cache$exp, now, units = "secs")) < refresh_buffer_seconds

    if (!needs_refresh) {
      return(cache$token)
    }

    store <- msgraph_delegated_store_read(token_store_path, store_key)

    age_days <- msgraph_delegated_store_age_days(store, now)
    if (age_days > warn_after_days) {
      warning(sprintf(paste0(
        "MSGraph-Refresh-Token ist %.0f Tage alt (Warnschwelle %s). ",
        "Bootstrap demnaechst erneut ausfuehren."),
        age_days, warn_after_days), call. = FALSE)
    }

    credentials <- msgraph_delegated_refresh(tenant_id, client_id, client_secret,
                                             store$refresh_token, scopes)

    rotated <- !is.null(credentials$refresh_token) &&
      !identical(credentials$refresh_token, store$refresh_token)
    if (rotated) {
      store$refresh_token <- credentials$refresh_token
      store$last_refreshed_at <- format(now, "%Y-%m-%dT%H:%M:%SZ", tz = "UTC")
      msgraph_delegated_store_write(token_store_path, store_key, store)
    }

    cache$token <- credentials$access_token
    cache$exp <- now + as.numeric(credentials$expires_in)
    cache$token
  }
}
