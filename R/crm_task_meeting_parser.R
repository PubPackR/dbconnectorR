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

#' Erkennt, ob ein CRM-Task ein Videocall-TERMIN ist
#'
#' Zwei unabhaengige Fragen an denselben Task, beide muessen mit ja beantwortet
#' sein: [is_vc_task()] klaert den **Kanal** (Videocall, nicht Telefon, nicht vor
#' Ort), `task_badge == "visit"` klaert die **Art** (Termin, nicht Aufgabe).
#'
#' Ohne die zweite Bedingung zaehlen Terminierungs-Aufgaben als Termine: ein Task
#' "Terminierung VC NV Frau Benchenna" nennt einen Videocall, ist aber die
#' Aufgabe, ihn erst zu vereinbaren.
#'
#' `visit` ist als Termin-Marker belegt, nicht angenommen. Ueber alle CRM-Tasks
#' (09/2025-09/2026) tragen 67,7 Prozent der `visit`-Tasks einen MSGraph-
#' Kalendertermin am selben Tag beim selben Lead, gegen 12,2 (`important`), 5,5
#' (`preparation`), 4,3 (`email`), 1,1 (`task`) und 0,5 Prozent (`call`).
#'
#' BEWUSST KEIN Titel-Muster fuer Terminierungs-Aufgaben. Von 41 `visit`-Tasks
#' mit "Terminierung" im Namen haben 28 einen belegten Kalendertermin — es sind
#' echte Termine, deren Task beim Zustandekommen nicht umbenannt wurde. Ein
#' Titel-Ausschluss zerstoert dort 28 belegte Termine, um 13 unsichere zu
#' entfernen. Der Badge erledigt die Terminierungs-Aufgaben ohnehin: von 519
#' Tasks mit diesem Titelmuster tragen nur 41 den `visit`-Badge.
#'
#' Positivliste, nicht Verbotsliste: `task_badge` ist nullable, ein NA-Badge
#' darf nicht durchrutschen.
#'
#' @param task_name Character(-Vektor) mit dem CRM-Task-Namen.
#' @param task_badge Character(-Vektor) aus `raw.crm_lead_tasks.task_badge`.
#' @return Logical(-Vektor).
#' @export
is_crm_vc_meeting <- function(task_name, task_badge) {
  # Ohne die Spalte waere task_badge NULL, das Ergebnis logical(0) und die
  # Trefferliste lautlos leer — siehe stop_if_badge_unbrauchbar().
  if (is.null(task_badge)) {
    stop("is_crm_vc_meeting(): task_badge fehlt. Ohne die Spalte gilt kein Task ",
         "als Termin und die CRM-Zeilen wuerden beim naechsten Lauf geloescht.",
         call. = FALSE)
  }
  is_vc_task(task_name) & !is.na(task_badge) & task_badge == "visit"
}

#' Bricht ab, wenn die Badge-Spalte unbrauchbar ist (interne Helferfunktion)
#'
#' Beide Schreib-Jobs ersetzen ihre CRM-Zeilen vollstaendig
#' (`delete_missing = TRUE` bzw. scoped delete). Eine leere Trefferliste ist
#' deshalb nicht "keine Termine", sondern "alle bestehenden CRM-Termine
#' loeschen". `raw.crm_lead_tasks.task_badge` ist nullable: faellt die Befuellung
#' aus, waere jeder Badge NA, jeder Task kein Termin und der naechste
#' FlowForce-Lauf wuerde beide Zieltabellen still leerraeumen. Lieber laut
#' scheitern (Hausregel fuer `do/main*.R`).
#'
#' @param task_badge Character(-Vektor) aus `raw.crm_lead_tasks.task_badge`.
#' @return `invisible(NULL)`, oder ein Fehler.
#' @keywords internal
# ---- start ---- #
stop_if_badge_unbrauchbar <- function(task_badge) {
  if (is.null(task_badge)) {
    stop("raw.crm_lead_tasks.task_badge fehlt in der geladenen Task-Menge.",
         call. = FALSE)
  }
  if (length(task_badge) > 0 && all(is.na(task_badge))) {
    stop("raw.crm_lead_tasks.task_badge ist durchgehend NA (", length(task_badge),
         " Tasks). Das wuerde jeden Task als Nicht-Termin einstufen und die ",
         "bestehenden CRM-Termine beim Schreiben loeschen.", call. = FALSE)
  }
  invisible(NULL)
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
#' KOMMENTARE SIND BEWUSST DIE EINZIGE QUELLE. Die Lead-Protokolle
#' (`raw.crm_lead_protocols`) als zweite Textquelle wurden am 05.09.2026 geprueft
#' und verworfen: sie loesen nur 9,2 Prozent der `unbekannt`-Faelle auf (zuletzt 3
#' bis 6 im Monat), der Gewinn bewegt die Show-Up-Quote um null, und "hat
#' stattgefunden" ist dort per Stichwort nicht erkennbar. Vollstaendige Messung mit
#' Zahlen und Grenzen: `docs/specs/2026-09-05-protokolle-als-statusquelle-verworfen.md`.
#' Wer die Idee erneut aufgreift, findet dort auch den billigeren Hebel (die
#' Absage-Familie in den no_show-Zweig).
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
