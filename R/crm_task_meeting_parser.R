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

#' Extrahiert den Termin-Typ aus dem CRM-Task-Namen
#'
#' Der Task-Name nennt fast nie das Tool, aber fast immer den Termin-Typ
#' (`VC NV`, `VC UC`, `VC: Updatecall`, `VC FU`, ...). Diese Funktion
#' kanonisiert ihn.
#'
#' Die Abkuerzungen (`NV`, `UC`, `FU`, `FUP`, `ER`, `ZR`) werden CASE-SENSITIV
#' auf dem Rohtext gematcht, die ausgeschriebenen Formen (Updatecall, Follow-up,
#' Report) case-insensitiv. Grund: Task-Namen tragen regelmaessig Personen- und
#' Firmennamen, und die stehen in Titlecase. `\bFU\b` trifft "VC FU", aber nicht
#' "VC mit Frau Fu"; `\bER\b` trifft nicht das Pronomen `er`. Dieselbe Schranke
#' zieht [classify_meeting_status()] fuer `NE` und `AB`. Der Preis ist bewusst:
#' ein klein geschriebenes "vc fu" landet auf `unbekannt` statt auf einer
#' Fehlklassifikation.
#'
#' Belegte Bedeutungen, weil Langform und Abkuerzung im Bestand in denselben
#' Task-Namen vorkommen: `uc` = Updatecall, `fu` = Follow-up, `rep` =
#' Reporting. NICHT belegt und in T1 mit dem Vertrieb zu bestaetigen: `nv`,
#' `er`, `zr`. Darum bleiben die Tokens die Abkuerzung selbst, eine
#' ausgeschriebene Bedeutung waere geraten.
#'
#' Nennt ein Name zwei Typen (`VC NV/UC`), gewinnt `nv`.
#'
#' @param task_name Character(-Vektor) mit dem CRM-Task-Namen.
#' @return Character(-Vektor): nv/uc/fu/rep/er/zr/planung/unbekannt.
#' @export
extract_meeting_type <- function(task_name) {
  raw <- ifelse(is.na(task_name), "", task_name)
  x   <- tolower(raw)
  dplyr::case_when(
    stringr::str_detect(raw, "\\bNV\\b")                                        ~ "nv",
    stringr::str_detect(x, "updatecall|update[ -]?call|\\bupdates?\\b") |
      stringr::str_detect(raw, "\\bUC\\b")                                      ~ "uc",
    stringr::str_detect(x, "follow[ -]?up") |
      stringr::str_detect(raw, "\\bFUP?\\b")                                    ~ "fu",
    stringr::str_detect(x, "report|\\brep\\b")                                  ~ "rep",
    stringr::str_detect(raw, "\\bER\\b")                                        ~ "er",
    stringr::str_detect(raw, "\\bZR\\b")                                        ~ "zr",
    stringr::str_detect(x, "kampagnenplanung|planungstermin")                   ~ "planung",
    TRUE                                                                        ~ "unbekannt"
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
  raw <- ifelse(is.na(comment_text), "", comment_text)
  x   <- tolower(raw)
  # NE ('nicht erschienen') und AB ('Anrufbeantworter') sind die haeufigsten
  # Sales-Kurzformen fuer No-Show. Sie werden CASE-SENSITIV auf dem Rohtext
  # gematcht (\bNE\b/\bAB\b), damit umgangssprachliches 'ne' (= 'eine') und die
  # Praeposition 'ab' keine Fehltreffer erzeugen. Die punktierte Form n.e. ist
  # eindeutig und wird case-insensitiv erkannt.
  dplyr::case_when(
    stringr::str_detect(x, "storn|abgesagt|verschob|verschieb|cancel")              ~ "storniert",
    stringr::str_detect(x, "no[ -]?show|nicht erschienen|nicht da|kam nicht|nicht aufgetaucht|n\\.e\\.?") |
      stringr::str_detect(raw, "\\bNE\\b|\\bAB\\b")                                  ~ "no_show",
    stringr::str_detect(x, "show[ -]?up|erschienen|stattgefunden|gehalten|durchgef|war da")     ~ "show_up",
    TRUE                                                                            ~ "unbekannt"
  )
}

#' Behaelt nur CRM-Meetings, die MSGraph noch nicht kennt
#'
#' Externe Tools (is_external_tool == TRUE) werden immer behalten (per Definition
#' nicht in MSGraph). Teams/unbekannt werden nur behalten, wenn kein
#' MSGraph-Meeting mit gleichem lead_id + event_date existiert.
#'
#' @param crm_meetings data.frame mit Spalten lead_id, event_date, is_external_tool (+ beliebige weitere).
#' @param msgraph_meetings data.frame mit Spalten lead_id, event_date.
#' @return data.frame — gefilterte Teilmenge von crm_meetings, ohne Hilfsspalten.
#' @export
filter_new_crm_meetings <- function(crm_meetings, msgraph_meetings) {
  ms_keys <- msgraph_meetings %>%
    dplyr::distinct(lead_id, event_date) %>%
    dplyr::mutate(.in_msgraph = TRUE)

  crm_meetings %>%
    dplyr::left_join(ms_keys, by = c("lead_id", "event_date")) %>%
    dplyr::filter(is_external_tool | is.na(.in_msgraph)) %>%
    dplyr::select(-.in_msgraph)
}
