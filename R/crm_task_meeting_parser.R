#' Erkennt, ob ein CRM-Task ein Video-Call-Termin ist
#'
#' TRUE, wenn der Task-Name den Marker `VC` (als eigenes Wort) enthaelt oder ein
#' externes VC-Tool nennt. Ein blosses "Teams" ohne VC-Marker gilt NICHT als
#' VC-Termin, weil "Teams" haeufig als Praeferenz-Notiz vorkommt.
#'
#' @param task_name Character(-Vektor) mit dem CRM-Task-Namen.
#' @return Logical(-Vektor).
#' @export
is_vc_task <- function(task_name) {
  x <- tolower(ifelse(is.na(task_name), "", task_name))
  stringr::str_detect(x, "\\bvc\\b") |
    stringr::str_detect(x, "web ?ex|zoom|skype|google ?meet|g ?meet")
}

#' Extrahiert das VC-Tool aus dem CRM-Task-Namen
#'
#' Priorisierter, case-insensitiver Keyword-Match. Gibt einen kanonischen
#' Lowercase-Token zurueck.
#'
#' @param task_name Character(-Vektor) mit dem CRM-Task-Namen.
#' @return Character(-Vektor): webex/zoom/google_meet/skype/teams/unbekannt.
#' @export
extract_meeting_tool <- function(task_name) {
  x <- tolower(ifelse(is.na(task_name), "", task_name))
  dplyr::case_when(
    stringr::str_detect(x, "web ?ex")              ~ "webex",
    stringr::str_detect(x, "zoom")                 ~ "zoom",
    stringr::str_detect(x, "google ?meet|g ?meet") ~ "google_meet",
    stringr::str_detect(x, "skype")                ~ "skype",
    stringr::str_detect(x, "teams")                ~ "teams",
    TRUE                                           ~ "unbekannt"
  )
}

#' Ist das Tool ein externes (nicht-Teams) VC-Tool?
#'
#' @param tool Character(-Vektor) aus [extract_meeting_tool()].
#' @return Logical(-Vektor).
#' @export
is_external_tool <- function(tool) {
  tool %in% c("webex", "zoom", "google_meet", "skype")
}

#' Klassifiziert den Meeting-Status aus einem CRM-Task-Kommentar
#'
#' Ausschliesslich auf Kommentar-Text anzuwenden (nicht auf Task-Namen, dort
#' stehen irrefuehrende Notizen zu vergangenen Terminen). Priorisiert:
#' storniert > no_show > show_up > unbekannt.
#'
#' @param comment_text Character(-Vektor) mit dem Kommentar-Text.
#' @return Character(-Vektor): storniert/no_show/show_up/unbekannt.
#' @export
classify_meeting_status <- function(comment_text) {
  x <- tolower(ifelse(is.na(comment_text), "", comment_text))
  dplyr::case_when(
    stringr::str_detect(x, "storn|abgesagt|verschoben|cancel")                      ~ "storniert",
    stringr::str_detect(x, "no ?show|nicht erschienen|nicht da|kam nicht|nicht aufgetaucht") ~ "no_show",
    stringr::str_detect(x, "show ?up|erschienen|stattgefunden|gehalten|durchgef|war da")     ~ "show_up",
    TRUE                                                                            ~ "unbekannt"
  )
}
