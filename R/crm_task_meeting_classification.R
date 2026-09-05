#' Baut CRM-Task-Meetings zu Zeilen im Klassifikations-Schema zusammen
#'
#' Rein (kein DB-Zugriff). Filtert VC-Termine, extrahiert Tool + Status,
#' wendet den Anti-Join gegen bestehende MSGraph-Meetings an und mappt auf das
#' Schema von processed.msgraph_extern_event_classification (+ CRM-Zusatzspalten).
#'
#' @param crm_tasks data.frame: id (Surrogat-PK, fuer Kommentar-Join), crm_task_id,
#'   lead_id, user_id, precise_time, task_name, task_badge.
#' @param crm_comments data.frame: task_id, comment_name.
#' @param crm_user_contact data.frame: user_id, contact_id (Sales-Rep-Kontakt).
#' @param msgraph_meetings data.frame: lead_id, event_date.
#' @return data.frame im Klassifikations-Schema mit source='crm_task'.
#' @export
assemble_crm_classification_rows <- function(crm_tasks, crm_comments,
                                             crm_user_contact,
                                             msgraph_meetings) {
  # 1. nur VC-Termine (Kanal per Titel, Art per Badge — siehe is_crm_vc_meeting)
  vc <- crm_tasks[is_crm_vc_meeting(crm_tasks$task_name, crm_tasks$task_badge),
                  , drop = FALSE]
  if (nrow(vc) == 0) return(assemble_crm_empty_result())

  # 2. Tool
  vc$meeting_tool <- extract_meeting_tool(vc$task_name)
  vc$is_external_tool <- is_external_tool(vc$meeting_tool)

  # 3. Status: staerkste Kategorie je Task ueber alle Kommentare
  status_rank <- c(storniert = 3L, no_show = 2L, show_up = 1L, unbekannt = 0L)
  comment_status <- crm_comments
  comment_status$status <- classify_meeting_status(comment_status$comment_name)
  comment_status$rank <- status_rank[comment_status$status]
  agg <- stats::aggregate(rank ~ task_id, data = comment_status, FUN = max)
  agg$meeting_status <- names(status_rank)[match(agg$rank, status_rank)]
  # Kommentar-Join auf die Surrogat-PK: crm_lead_task_comments.task_id
  # referenziert crm_lead_tasks.id (DB-verifiziert 2026-08: 253195 vs 113
  # Treffer), NICHT crm_task_id.
  vc$meeting_status <- agg$meeting_status[match(as.character(vc$id),
                                                 as.character(agg$task_id))]
  vc$meeting_status[is.na(vc$meeting_status)] <- "unbekannt"

  # 3b. Nur informative Zeilen: erkanntes Tool ODER erkannter Status. VC-Tasks
  # ohne Tool-String und ohne Status-Kommentar tragen keine Information (die
  # sichtbaren Teams-/internen Meetings erfasst MSGraph ohnehin) und wuerden die
  # Fakttabelle + den Tab mit Rauschen fluten. Frueh filtern spart Anti-Join/
  # Rep-Match. Bewusster Tradeoff: ein echtes externes Meeting mit unerkanntem
  # Freitext-Tool UND ohne Kommentar faellt hier raus (seltener Randfall).
  vc <- vc[vc$meeting_tool != "unbekannt" | vc$meeting_status != "unbekannt",
           , drop = FALSE]
  if (nrow(vc) == 0) return(assemble_crm_empty_result())

  # 4. Datum (Europe/Berlin)
  vc$event_date <- as.Date(vc$precise_time, tz = "Europe/Berlin")

  # 5. Anti-Join
  vc <- filter_new_crm_meetings(vc, msgraph_meetings)
  if (nrow(vc) == 0) return(assemble_crm_empty_result())

  # 6. Rep-Kontakt (ohne Kontakt keine Zeile)
  vc$contact_id <- crm_user_contact$contact_id[match(as.character(vc$user_id),
                                                       as.character(crm_user_contact$user_id))]
  vc <- vc[!is.na(vc$contact_id), , drop = FALSE]
  if (nrow(vc) == 0) return(assemble_crm_empty_result())

  # 7. Status -> Flags
  is_no_show <- vc$meeting_status == "no_show"
  excluded   <- vc$meeting_status == "storniert"

  data.frame(
    call_event_mapping_id = NA_integer_,
    contact_id            = vc$contact_id,
    is_responsible        = TRUE,
    is_organizer          = TRUE,
    is_no_show            = is_no_show,
    excluded              = excluded,
    exclusion_reason      = ifelse(excluded, "crm_storniert", NA_character_),
    original_created_at   = vc$precise_time,
    is_short_lived_event  = FALSE,
    source                = "crm_task",
    meeting_tool          = vc$meeting_tool,
    meeting_status        = vc$meeting_status,
    is_external_tool      = vc$is_external_tool,
    crm_event_date        = vc$event_date,
    crm_task_id           = vc$crm_task_id,
    stringsAsFactors      = FALSE
  )
}

#' Leeres Ergebnis im Klassifikations-Schema (interne Helferfunktion)
#' @return data.frame mit 0 Zeilen und den korrekten Spalten.
#' @keywords internal
assemble_crm_empty_result <- function() {
  data.frame(
    call_event_mapping_id = integer(0), contact_id = integer(0),
    is_responsible = logical(0), is_organizer = logical(0),
    is_no_show = logical(0), excluded = logical(0),
    exclusion_reason = character(0), original_created_at = as.POSIXct(character(0)),
    is_short_lived_event = logical(0), source = character(0),
    meeting_tool = character(0), meeting_status = character(0),
    is_external_tool = logical(0), crm_event_date = as.Date(character(0)),
    crm_task_id = integer(0), stringsAsFactors = FALSE
  )
}

#' Aktualisiert die CRM-Task-Meeting-Zeilen in der Klassifikations-Tabelle
#'
#' Laeuft als letzter Schritt nach update_extern_event_classification. Schreibt
#' NUR source='crm_task'-Zeilen (scoped): loescht die bestehenden CRM-Zeilen und
#' schreibt die frisch berechneten. MSGraph-Zeilen werden nicht beruehrt.
#'
#' @param con Pool/DBI-Connection.
#' @return invisible(Anzahl geschriebener CRM-Zeilen).
#' @export
update_crm_task_meeting_classification <- function(con) {
  message("update_crm_task_meeting_classification: lade CRM-Tasks ...")

  crm_tasks <- dplyr::tbl(con, I("raw.crm_lead_tasks")) %>%
    dplyr::filter(is_deleted == FALSE) %>%
    dplyr::select(id, crm_task_id, lead_id, user_id, assigned_to_user_id,
                  precise_time, task_name, task_badge) %>%
    dplyr::collect()

  crm_comments <- dplyr::tbl(con, I("raw.crm_lead_task_comments")) %>%
    dplyr::filter(is_deleted == FALSE) %>%
    dplyr::select(task_id, comment_name) %>%
    dplyr::collect()

  stop_if_badge_unbrauchbar(crm_tasks$task_badge)

  # Rep-Aufloesung: crm_users.user_login -> raw.msgraph_contacts.email (best
  # effort, kein Personio-Zwischenschritt). Liefert den Sales-Rep-Kontakt.
  # ACHTUNG: assemble_crm_classification_rows verwirft Zeilen ohne aufloesbaren
  # Rep -> die Auflösung ist de facto Pflicht (leeres Ergebnis => keine CRM-Zeilen).
  crm_user_contact <- resolve_crm_user_contact(con)

  # MSGraph-Meetings fuer Anti-Join: lead_id + event_date aus bestehender
  # Klassifikation (nur msgraph-Zeilen), Datum via mapping.
  msgraph_meetings <- dplyr::tbl(con, I("processed.msgraph_extern_event_classification")) %>%
    dplyr::filter(source == "msgraph") %>%
    dplyr::inner_join(
      dplyr::tbl(con, I("mapping.msgraph_call_event")) %>%
        dplyr::select(call_event_mapping_id = id, event_date),
      by = "call_event_mapping_id") %>%
    dplyr::inner_join(
      dplyr::tbl(con, I("mapping.crm_lead_msgraph_contact")) %>%
        dplyr::filter(is_primary_crm == TRUE) %>%
        dplyr::select(contact_id = msgraph_contact_id, lead_id = crm_lead_id),
      by = "contact_id") %>%
    dplyr::distinct(lead_id, event_date) %>%
    dplyr::collect()

  # assigned_to_user_id bevorzugen, sonst user_id. Coalesce im CHARACTER-Raum:
  # assigned_to_user_id ist bigint/integer64; ifelse() strippt die integer64-Klasse
  # und liefert Garbage-Doubles -> Match schlaegt still fehl (integer64-Falle).
  crm_tasks$user_id <- ifelse(!is.na(crm_tasks$assigned_to_user_id),
                              as.character(crm_tasks$assigned_to_user_id),
                              as.character(crm_tasks$user_id))

  rows <- assemble_crm_classification_rows(
    crm_tasks = crm_tasks, crm_comments = crm_comments,
    crm_user_contact = crm_user_contact,
    msgraph_meetings = msgraph_meetings)

  message(paste0("  ", nrow(rows), " CRM-Meeting-Zeilen zu schreiben"))

  # scoped: bestehende CRM-Zeilen loeschen, dann frische schreiben (atomar)
  pool::poolWithTransaction(con, function(conn) {
    DBI::dbExecute(conn,
      "DELETE FROM processed.msgraph_extern_event_classification WHERE source = 'crm_task'")
    if (nrow(rows) > 0) {
      Billomatics::postgres_upsert_data(
        conn, "processed", "msgraph_extern_event_classification",
        rows, match_cols = c("crm_task_id", "contact_id"), delete_missing = FALSE)
    }
  })
  message("  fertig.")
  invisible(nrow(rows))
}

#' Loest CRM-User auf msgraph-Kontakte auf (best effort)
#'
#' Best-effort E-Mail-Join crm_users.user_login -> raw.msgraph_contacts-E-Mail-
#' Spalte; gibt user_id + contact_id zurueck. Der zurueckgegebene `user_id` ist
#' die Surrogat-Spalte `crm_users.id` — DAS ist die Spalte, die
#' `raw.crm_lead_tasks.user_id` / `assigned_to_user_id` referenzieren (NICHT
#' `crm_user_id`; DB-verifiziert 2026-07-24: 186547 vs 0 Join-Treffer).
#'
#' @param con Pool/DBI-Connection.
#' @return data.frame: user_id (= crm_users.id), contact_id.
#' @keywords internal
resolve_crm_user_contact <- function(con) {
  crm_users <- dplyr::tbl(con, I("raw.crm_users")) %>%
    dplyr::filter(is_deleted == FALSE) %>%
    dplyr::select(user_id = id, user_login) %>%
    dplyr::collect()
  contacts <- dplyr::tbl(con, I("raw.msgraph_contacts")) %>%
    dplyr::collect()
  # E-Mail-Join crm_users.user_login -> msgraph_contacts (best effort).
  # Wenn keine Kontakt-E-Mail-Spalte existiert, bleibt das Ergebnis leer.
  email_col <- intersect(c("email", "mail", "address"), names(contacts))
  if (length(email_col) == 0) {
    return(data.frame(user_id = integer(0), contact_id = integer(0)))
  }
  contacts$.email <- tolower(contacts[[email_col[1]]])
  crm_users$.email <- tolower(crm_users$user_login)
  merged <- merge(crm_users, contacts, by = ".email")
  id_col <- intersect(c("contact_id", "msgraph_contact_id", "id"), names(contacts))
  data.frame(user_id = merged$user_id,
             contact_id = merged[[id_col[1]]])
}
