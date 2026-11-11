# Sprecher im Transkript als Beleg fuer externe Teilnahme
# (Asana 1220382115257832, Spec docs/specs/2026-11-10-transkript-externer-sprecher.md).
#
# Teams fuehrt manche externen Teilnehmer nicht im Anwesenheitsbericht, obwohl
# sie im Transkript sprechen. Diese Funktionen lesen die Sprecher aus dem
# Klartext, den vtt_to_plaintext() in processed.msgraph_call_transcripts
# schreibt, und zaehlen die externen. msgraph_map_calls_events() nutzt die
# Zaehlung, processed.sales_meetings soll sie als Signal fuehren.

# Label-Endung, an der ein interner Sprecher erkannt wird. Teams haengt die
# Firma aus Entra an den Namen; alle 34 internen Labels seit 09/2026 tragen sie,
# kein Label ohne sie passt zu einem internen Konto (Query K, 10.11.2026).
INTERNAL_SPEAKER_SUFFIX <- ", studyflix gmbh"

#' Sprecher eines Transkripts mit Zeilenzahl
#'
#' Liest die Sprecher-Labels aus dem Klartext, den [vtt_to_plaintext()] schreibt:
#' eine Zeile je Sprechbeitrag in der Form `Label: Text`. Eine Zeile ohne dieses
#' Muster zaehlt fuer keinen Sprecher.
#'
#' @param transcript_content Character-Vektor, ein Element je Transkript
#'   (`processed.msgraph_call_transcripts.transcript_content`). `NA` und leere
#'   Elemente liefern keine Zeilen.
#' @return data.frame mit `transcript` (Position in `transcript_content`),
#'   `label` und `n_lines` (integer), eine Zeile je Transkript und Label.
#' @export
transcript_speakers <- function(transcript_content) {
  # ---- start ---- #
  leer <- data.frame(transcript = integer(0), label = character(0),
                     n_lines = integer(0), stringsAsFactors = FALSE)
  if (length(transcript_content) == 0) return(leer)

  text <- ifelse(is.na(transcript_content), "", as.character(transcript_content))
  zeilen <- strsplit(text, "\r?\n")
  nr <- rep(seq_along(zeilen), lengths(zeilen))
  zeilen <- unlist(zeilen, use.names = FALSE)
  if (length(zeilen) == 0) return(leer)

  # vtt_to_plaintext() schreibt "Label: Text" und trimmt die Zeile; ein leerer
  # Beitrag endet deshalb auf ":". Das Leerzeichen danach haelt Uhrzeiten wie
  # "10:30" aus dem Label heraus.
  treffer <- regmatches(zeilen, regexec("^([^:]{1,80}):(\\s|$)", zeilen))
  label <- vapply(treffer, function(t) if (length(t) >= 2) trimws(t[2]) else NA_character_,
                  character(1))
  ok <- !is.na(label) & nzchar(label)
  if (!any(ok)) return(leer)

  sp <- data.frame(transcript = nr[ok], label = label[ok], stringsAsFactors = FALSE)
  sp <- stats::aggregate(list(n_lines = rep(1L, nrow(sp))),
                         by = list(transcript = sp$transcript, label = sp$label),
                         FUN = length)
  sp$n_lines <- as.integer(sp$n_lines)
  sp <- sp[order(sp$transcript, sp$label), c("transcript", "label", "n_lines")]
  rownames(sp) <- NULL
  sp
}

#' Ist ein Transkript-Sprecher intern?
#'
#' Intern ist ein Sprecher, dessen Label mit dem Firmenkuerzel
#' `, Studyflix GmbH` endet (Gross-/Kleinschreibung egal). Jedes andere Label
#' gilt als extern, auch der Teams-Platzhalter `@1` fuer einen unbenannten
#' Sprecher.
#'
#' @param label Character-Vektor mit Sprecher-Labels aus [transcript_speakers()].
#' @return Logischer Vektor, `NA` fuer ein fehlendes Label.
#' @export
is_internal_speaker <- function(label) {
  # ---- start ---- #
  x <- tolower(trimws(as.character(label)))
  ifelse(is.na(x), NA, endsWith(x, INTERNAL_SPEAKER_SUFFIX))
}

#' Zahl der externen Sprecher je Transkript
#'
#' Ein externer Sprecher ist ein Label ohne Firmenkuerzel (siehe
#' [is_internal_speaker()]) mit mindestens `min_lines` Zeilen. Die Schwelle
#' haelt eine einzelne Zeile ohne Sprecher-Tag, die zufaellig einen Doppelpunkt
#' enthaelt, davon ab, einen internen Termin zum externen zu machen.
#'
#' @param transcript_content Character-Vektor, ein Element je Transkript.
#' @param min_lines Mindestzahl Zeilen, ab der ein externer Sprecher zaehlt.
#' @return Integer-Vektor in der Laenge von `transcript_content`.
#' @export
count_external_speakers <- function(transcript_content, min_lines = 2L) {
  # ---- start ---- #
  n <- integer(length(transcript_content))
  sp <- transcript_speakers(transcript_content)
  sp <- sp[!is_internal_speaker(sp$label) & sp$n_lines >= min_lines, , drop = FALSE]
  if (nrow(sp) == 0) return(n)
  je_transkript <- table(sp$transcript)
  n[as.integer(names(je_transkript))] <- as.integer(je_transkript)
  n
}

#' Teams-Seite eines Termins aus Bericht und Transkript (rein)
#'
#' `extern`, wenn der Anwesenheitsbericht einen Externen fuehrt oder ein
#' Transkript der Session einen externen Sprecher hat; sonst `intern`, wenn
#' jemand teilnahm, sonst `no`.
#'
#' @param extern_participating_users Zahl externer Teilnehmer im Bericht.
#' @param participating_users Zahl aller Teilnehmer im Bericht.
#' @param has_external_speaker Logisch, `NA` zaehlt wie `FALSE`.
#' @return Character-Vektor `extern` / `intern` / `no`.
#' @keywords internal
derive_event_class_call <- function(extern_participating_users, participating_users,
                                    has_external_speaker) {
  # ---- start ---- #
  dplyr::case_when(
    extern_participating_users > 0 ~ "extern",
    has_external_speaker %in% TRUE ~ "extern",
    participating_users > 0 ~ "intern",
    TRUE ~ "no"
  )
}

#' Calls mit externem Sprecher im Transkript laden
#'
#' Laedt erst die IDs der Calls, zu denen ein Transkript mit Inhalt existiert,
#' und nur fuer die Schnittmenge mit `call_ids` den Inhalt. So bleibt der
#' naechtliche Lauf klein, obwohl die Tabelle alle Transkripte seit 2025 haelt.
#'
#' @param con Pool/DBI-Connection.
#' @param call_ids IDs aus `raw.msgraph_calls`, fuer die das Transkript zaehlt.
#' @param min_lines Siehe [count_external_speakers()].
#' @return Character-Vektor der Call-IDs mit externem Sprecher
#'   (integer64-sicher als Zeichen).
#' @keywords internal
load_external_speaker_call_ids <- function(con, call_ids, min_lines = 2L) {
  # ---- start ---- #
  call_ids <- unique(as.character(call_ids[!is.na(call_ids)]))
  if (length(call_ids) == 0) return(character(0))

  mit_transkript <- dplyr::tbl(con, I("processed.msgraph_call_transcripts")) %>%
    dplyr::filter(!is.na(call_id), !is.na(transcript_content)) %>%
    dplyr::distinct(call_id) %>%
    dplyr::collect()
  ids <- intersect(call_ids, as.character(mit_transkript$call_id))
  if (length(ids) == 0) return(character(0))

  transkripte <- dplyr::tbl(con, I("processed.msgraph_call_transcripts")) %>%
    dplyr::filter(call_id %in% !!ids) %>%
    dplyr::select(call_id, transcript_content) %>%
    dplyr::collect()
  n <- count_external_speakers(transkripte$transcript_content, min_lines = min_lines)
  unique(as.character(transkripte$call_id[n > 0]))
}

#' Zahl der externen Sprecher je Call laden
#'
#' Wie [load_external_speaker_call_ids()], liefert aber die Zahl: je Call das
#' Maximum von [count_external_speakers()] ueber seine Transkripte mit Inhalt.
#' Ein Call ohne solches Transkript fehlt im Ergebnis, einer mit Transkript ohne
#' externen Sprecher hat 0. Signal fuer `processed.sales_meetings`
#' (`external_speaker_count`).
#'
#' @param con Pool/DBI-Connection.
#' @param call_ids IDs aus `raw.msgraph_calls`, fuer die gezaehlt wird.
#' @param min_lines Siehe [count_external_speakers()].
#' @return data.frame mit call_id (character) und external_speaker_count
#'   (integer), eine Zeile je Call mit Transkript.
#' @keywords internal
load_external_speaker_counts <- function(con, call_ids, min_lines = 2L) {
  # ---- start ---- #
  leer <- data.frame(call_id = character(0), external_speaker_count = integer(0),
                     stringsAsFactors = FALSE)
  call_ids <- unique(as.character(call_ids[!is.na(call_ids)]))
  if (length(call_ids) == 0) return(leer)

  mit_transkript <- dplyr::tbl(con, I("processed.msgraph_call_transcripts")) %>%
    dplyr::filter(!is.na(call_id), !is.na(transcript_content)) %>%
    dplyr::distinct(call_id) %>%
    dplyr::collect()
  ids <- intersect(call_ids, as.character(mit_transkript$call_id))
  if (length(ids) == 0) return(leer)

  transkripte <- dplyr::tbl(con, I("processed.msgraph_call_transcripts")) %>%
    dplyr::filter(call_id %in% !!ids, !is.na(transcript_content)) %>%
    dplyr::select(call_id, transcript_content) %>%
    dplyr::collect()
  n <- count_external_speakers(transkripte$transcript_content, min_lines = min_lines)
  je_call <- tapply(n, as.character(transkripte$call_id), max)
  data.frame(call_id = names(je_call), external_speaker_count = as.integer(je_call),
             stringsAsFactors = FALSE)
}
