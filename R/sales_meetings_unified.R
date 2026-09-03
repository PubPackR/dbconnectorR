#' Organisator je MSGraph-Meeting laden
#'
#' Genau eine Zeile je `call_event_mapping_id`. Die Klassifikationstabelle
#' fuehrt Organisator und Verantwortliche:n als getrennte Zeilen desselben
#' Meetings; hier interessiert allein die Organisator-Zeile.
#'
#' Traegt ein Meeting mehrere verschiedene Organisatoren, ist das ein
#' Datenfehler und kein fachlicher Fall. Statt still einen davon zu nehmen,
#' wird gewarnt und der kleinste Kontakt deterministisch gewaehlt, damit zwei
#' Laeufe nicht unterschiedliche Ergebnisse schreiben.
#'
#' @param con Pool/DBI-Connection.
#' @return data.frame mit `call_event_mapping_id` und `organizer_contact_id`
#'   (character, integer64-sicher).
#' @keywords internal
load_meeting_organizers <- function(con) {
  org <- dplyr::tbl(con, I("processed.msgraph_extern_event_classification")) %>%
    dplyr::filter(source == "msgraph", is_organizer == TRUE) %>%
    dplyr::select(call_event_mapping_id, contact_id) %>%
    dplyr::collect()

  org <- org[!is.na(org$call_event_mapping_id), , drop = FALSE]
  org$organizer_contact_id <- as.character(org$contact_id)
  org <- unique(org[, c("call_event_mapping_id", "organizer_contact_id")])

  mehrfach <- unique(org$call_event_mapping_id[duplicated(org$call_event_mapping_id)])
  if (length(mehrfach) > 0) {
    warning(length(mehrfach), " Meetings tragen mehrere Organisatoren; ",
            "je Meeting wird der kleinste Kontakt verwendet.", call. = FALSE)
    org <- org[order(org$call_event_mapping_id, as.numeric(org$organizer_contact_id)), ]
    org <- org[!duplicated(org$call_event_mapping_id), , drop = FALSE]
  }

  org
}

#' CRM-Status -> (is_no_show, excluded)
#' @param status character vector mit CRM-Meeting-Status (z.B. "no_show",
#'   "show_up", "storniert", "unbekannt").
#' @return list mit `is_no_show` (logical, NA falls nicht definitiv) und
#'   `excluded` (logical).
#' @keywords internal
crm_status_flags <- function(status) {
  is_no_show <- ifelse(status == "no_show", TRUE, ifelse(status == "show_up", FALSE, NA))
  excluded   <- status %in% c("storniert", "unbekannt")
  list(is_no_show = is_no_show, excluded = excluded)
}

#' Baut die vereinheitlichte Meeting-Menge (rein, kein DB-Zugriff)
#'
#' Grain: eine Zeile pro (Meeting x externer Lead). `msgraph_meetings` ist bereits
#' per-Lead expandiert (eine Zeile je (call_event_mapping_id, lead_id); `lead_id`
#' = NA = externer Teilnehmer ohne gemappten Lead -> Platzhalter). CRM-VC-Termine
#' werden auf (lead_id x event_date) gematcht; bei Mehrdeutigkeit ueber gleichen
#' Rep-Kontakt + naechste `event_start` disambiguiert, echte Rest-Mehrdeutigkeit
#' (gleicher Rep, gleicher Zeitpunkt) wird verworfen.
#'
#' Findet der Match keinen Kandidaten, greift der **Platzhalter-Fallback**: eine
#' MSGraph-Zeile mit `lead_id = NA` (externer Teilnehmer ohne gemappten Lead)
#' desselben Reps am selben Tag ist mit hoher Wahrscheinlichkeit derselbe Termin.
#' Ohne ihn zaehlt der CRM-Task ein zweites Mal (Juni 2026: 76 von 178
#' netto-neuen CRM-Terminen). Mehrere Platzhalter werden ueber `precise_time`
#' aufgeloest; bleibt es mehrdeutig, wird die CRM-Zeile netto-neu geschrieben und
#' nicht verworfen.
#'
#' `lead_id` und `contact_id` werden im CHARACTER-Raum gehalten (rbind-sicher
#' gegen die integer64-Falle); der Caller castet vor dem Upsert auf bigint.
#'
#' **Organisator.** `organizer_contact_id` haelt fest, wer den Termin angelegt
#' hat, `organizer_source` woher diese Angabe stammt. Beides ist ein rohes
#' Merkmal des Termins ohne jede Kennzahlenlogik: ob daraus ein SDR wird,
#' entscheidet `package-02-kpiR` (Organizer ungleich Verantwortliche:r, siehe
#' dessen `CONTEXT.md`). `organizer_source` unterscheidet die drei Faelle, die
#' sonst zu einem `NA` verschmelzen wuerden: `"msgraph"` = Organisator bekannt,
#' `"unbekannt"` = MSGraph-Termin ohne klassifizierte Organisator-Zeile,
#' `"crm_task"` = netto-neuer CRM-Termin, der grundsaetzlich keinen tragen kann.
#'
#' @param msgraph_meetings data.frame mit call_event_mapping_id, lead_id,
#'   event_date, event_start, contact_id (Rep), is_no_show, excluded,
#'   is_short_lived_event, is_responsible, original_created_at, event_id und
#'   optional organizer_contact_id (fehlt sie, gilt der Organisator als
#'   unbekannt).
#' @param crm_meetings data.frame mit crm_task_id, lead_id, event_date,
#'   precise_time, contact_id (Rep), meeting_tool, meeting_type, meeting_status,
#'   is_external_tool, original_created_at.
#' @return data.frame im Schema von processed.sales_meetings_unified (+ intern
#'   genutzte, nicht geschriebene Spalten werden vom Caller entfernt).
#' @export
assemble_unified_meetings <- function(msgraph_meetings, crm_meetings) {
  ms_lead <- as.character(msgraph_meetings$lead_id)  # NA fuer Platzhalter
  # Optional, damit ein Caller ohne Organisator-Spalte nicht bricht: dann ist
  # der Organisator schlicht unbekannt, nicht "es gibt keinen".
  ms_org <- if (is.null(msgraph_meetings$organizer_contact_id)) {
    rep(NA_character_, nrow(msgraph_meetings))
  } else {
    as.character(msgraph_meetings$organizer_contact_id)
  }
  ms_reason <- if (is.null(msgraph_meetings$exclusion_reason)) {
    rep(NA_character_, nrow(msgraph_meetings))
  } else {
    as.character(msgraph_meetings$exclusion_reason)
  }
  base <- data.frame(
    meeting_key          = paste0("msgraph_", msgraph_meetings$call_event_mapping_id,
                                  "_", ms_lead),
    source               = "msgraph",
    event_date           = msgraph_meetings$event_date,
    contact_id           = as.character(msgraph_meetings$contact_id),
    lead_id              = ms_lead,
    is_no_show           = msgraph_meetings$is_no_show,
    no_show_source       = "msgraph",
    meeting_status       = NA_character_,
    meeting_tool         = NA_character_,
    meeting_type         = NA_character_,
    is_external_tool     = NA,
    excluded             = msgraph_meetings$excluded,
    exclusion_reason     = ms_reason,
    is_short_lived_event = msgraph_meetings$is_short_lived_event,
    is_responsible       = msgraph_meetings$is_responsible,
    original_created_at  = msgraph_meetings$original_created_at,
    event_id             = msgraph_meetings$event_id,
    organizer_contact_id = ms_org,
    organizer_source     = ifelse(is.na(ms_org), "unbekannt", "msgraph"),
    stringsAsFactors     = FALSE
  )
  # Nur fuer den Tiebreak (nicht im DB-Schema): Rep-Kontakt + event_start je Zeile.
  base_rep   <- as.character(msgraph_meetings$contact_id)
  base_start <- msgraph_meetings$event_start
  # Buchfuehrung fuer den Platzhalter-Fallback: jede Platzhalter-Zeile darf
  # hoechstens einen CRM-Termin aufnehmen.
  ph_used    <- rep(FALSE, nrow(base))

  new_rows <- list()
  for (i in seq_len(nrow(crm_meetings))) {
    cm <- crm_meetings[i, ]
    fl <- crm_status_flags(cm$meeting_status)
    cm_lead <- as.character(cm$lead_id)
    netto_neu <- function() data.frame(
      meeting_key = paste0("crm_", cm$crm_task_id), source = "crm_task",
      event_date = cm$event_date, contact_id = as.character(cm$contact_id), lead_id = cm_lead,
      is_no_show = fl$is_no_show, no_show_source = "crm_only",
      meeting_status = cm$meeting_status, meeting_tool = cm$meeting_tool, meeting_type = cm$meeting_type,
      is_external_tool = cm$is_external_tool, excluded = fl$excluded,
      exclusion_reason = if (fl$excluded) paste0("crm_", cm$meeting_status) else NA_character_,
      is_short_lived_event = FALSE, is_responsible = TRUE,
      original_created_at = cm$original_created_at, event_id = NA_character_,
      # Ein CRM-Task kennt keinen Organisator. Das ist etwas anderes als ein
      # Kalendertermin, dessen Organisator-Zeile fehlt, und bleibt deshalb
      # unterscheidbar.
      organizer_contact_id = NA_character_, organizer_source = "crm_task",
      stringsAsFactors = FALSE)

    # Externes Tool (kein MSGraph-Pendant) und Task ohne Lead -> immer netto-neu.
    if (isTRUE(cm$is_external_tool)) { new_rows[[length(new_rows)+1]] <- netto_neu(); next }
    if (is.na(cm$lead_id))          { new_rows[[length(new_rows)+1]] <- netto_neu(); next }

    # Kandidaten gleicher (lead_id, event_date).
    cand <- which(!is.na(base$lead_id) & base$lead_id == cm_lead &
                    base$event_date == cm$event_date)
    if (length(cand) == 0) {
      # Platzhalter-Fallback. Ein MSGraph-Meeting, dessen externer Teilnehmer
      # keinem Lead zugeordnet ist, traegt lead_id = NA und kann per (Lead x Tag)
      # nie gefunden werden. Der CRM-Task desselben Reps am selben Tag ist dann
      # mit hoher Wahrscheinlichkeit derselbe Termin, und ohne diesen Zweig
      # zaehlt er ein zweites Mal. Gemessen im Juni 2026: 76 von 178 netto-neuen
      # CRM-Terminen, rund 5 Prozent zu hohe VC-Zahl.
      # Bewusst NICHT "gleicher Rep, gleicher Tag" ohne die Platzhalter-Bedingung:
      # das wuerde zwei erkennbar verschiedene Meetings verschmelzen, sobald ein
      # Rep an einem Tag mehrere Termine hat.
      # `ph_used` schliesst bereits verbrauchte Platzhalter aus. Ohne das wuerden
      # zwei CRM-Termine desselben Reps am selben Tag denselben Platzhalter
      # matchen, einander ueberschreiben und beide nicht netto-neu geschrieben:
      # aus zwei Meetings wuerde eines. Die lead-basierte Zuordnung oben ist
      # ueber lead_id verschluesselt und deshalb strukturell kollisionsfrei;
      # dieser Zweig gibt den Schluessel auf und braucht die Buchfuehrung.
      ph <- which(is.na(base$lead_id) & base$event_date == cm$event_date &
                    base_rep == as.character(cm$contact_id) & !ph_used)
      if (length(ph) == 1) {
        cand <- ph
      } else if (length(ph) > 1 && !is.na(cm$precise_time)) {
        d <- abs(as.numeric(base_start[ph]) - as.numeric(cm$precise_time))
        if (sum(d == min(d, na.rm = TRUE), na.rm = TRUE) == 1) {
          cand <- ph[which.min(d)]
        } else { new_rows[[length(new_rows)+1]] <- netto_neu(); next }
      } else { new_rows[[length(new_rows)+1]] <- netto_neu(); next }
      ph_used[cand] <- TRUE
    }
    if (length(cand) > 1) {
      # Tiebreak 1: gleicher Rep-Kontakt.
      same_rep <- cand[base_rep[cand] == as.character(cm$contact_id)]
      if (length(same_rep) == 1) {
        cand <- same_rep
      } else if (length(same_rep) > 1 && !is.na(cm$precise_time)) {
        # Tiebreak 2: naechste event_start zur precise_time (Sekunden-Distanz).
        d <- abs(as.numeric(base_start[same_rep]) - as.numeric(cm$precise_time))
        if (sum(d == min(d, na.rm = TRUE), na.rm = TRUE) == 1) {
          cand <- same_rep[which.min(d)]
        } else next  # echte Rest-Mehrdeutigkeit -> verwerfen
      } else next    # kein eindeutiger Rep -> verwerfen
    }

    # eindeutiger (bzw. aufgeloester) Match -> Override. Nur definitiver Status
    # setzt is_no_show; storniert -> excluded; "unbekannt" laesst MSGraph unangetastet.
    j <- cand[1]
    if (cm$meeting_status %in% c("no_show", "show_up")) base$is_no_show[j] <- fl$is_no_show
    if (cm$meeting_status == "storniert") {
      base$excluded[j] <- TRUE
      # Ohne den Grund waere nach dem Override nicht mehr erkennbar, ob der
      # Ausschluss fachlich ist (Storno) oder nur die Beobachtbarkeit betrifft.
      base$exclusion_reason[j] <- "crm_storniert"
    }
    base$no_show_source[j] <- "crm_override"
    base$meeting_tool[j]   <- cm$meeting_tool
    base$meeting_type[j]   <- cm$meeting_type
    base$meeting_status[j] <- cm$meeting_status
  }

  if (length(new_rows) > 0) base <- rbind(base, do.call(rbind, new_rows))
  base
}

#' Rebuild processed.sales_meetings_unified (voll rueckwirkend)
#'
#' Laeuft nach update_crm_task_meeting_classification. MSGraph-Meetings (extern-only
#' Lead-Ableitung, per (Meeting x Lead) expandiert) + frisch aus Rohdaten
#' abgeleitete CRM-VC-Termine werden via assemble_unified_meetings() vereinheitlicht
#' und komplett neu geschrieben.
#' @param con Pool/DBI-Connection.
#' @return invisible(Anzahl geschriebener Zeilen).
#' @export
update_sales_meetings_unified <- function(con) {
  message("update_sales_meetings_unified: lade MSGraph-Meetings ...")
  msgraph_meetings <- dplyr::tbl(con, I("processed.msgraph_extern_event_classification")) %>%
    dplyr::filter(source == "msgraph", is_responsible == TRUE) %>%
    dplyr::select(call_event_mapping_id, contact_id, is_no_show, original_created_at,
                  excluded, exclusion_reason, is_short_lived_event, is_responsible) %>%
    dplyr::inner_join(
      dplyr::tbl(con, I("mapping.msgraph_call_event")) %>%
        dplyr::select(id, event_id, event_date),
      by = c("call_event_mapping_id" = "id")) %>%
    dplyr::left_join(
      dplyr::tbl(con, I("raw.msgraph_events")) %>%
        dplyr::select(id, event_start),
      by = c("event_id" = "id")) %>%
    dplyr::collect()
  msgraph_meetings$event_id    <- as.character(msgraph_meetings$event_id)
  msgraph_meetings$event_date  <- as.Date(msgraph_meetings$event_date, tz = "Europe/Berlin")
  msgraph_meetings$event_start <- as.POSIXct(msgraph_meetings$event_start, tz = "UTC")
  # Eine Zeile je Meeting (mehrere verantwortliche Kontakte -> ersten waehlen).
  msgraph_meetings <- dplyr::distinct(msgraph_meetings, call_event_mapping_id, .keep_all = TRUE)

  message("  lade Organisator je Meeting ...")
  msgraph_meetings <- merge(
    msgraph_meetings, load_meeting_organizers(con),
    by = "call_event_mapping_id", all.x = TRUE)

  message("  leite externe Leads je Meeting ab (extern-only, is_primary_crm) ...")
  # Nicht-Organisator-Teilnehmer mit Email -> extern/intern klassifizieren.
  participants <- dplyr::tbl(con, I("mapping.msgraph_call_event")) %>%
    dplyr::select(call_event_mapping_id = id, event_id) %>%
    dplyr::inner_join(
      dplyr::tbl(con, I("raw.msgraph_event_participants")) %>%
        dplyr::filter(is_organizer == FALSE) %>%
        dplyr::select(event_id, contact_id),
      by = "event_id") %>%
    dplyr::inner_join(
      dplyr::tbl(con, I("raw.msgraph_contacts")) %>%
        dplyr::select(id, email),
      by = c("contact_id" = "id")) %>%
    dplyr::select(call_event_mapping_id, contact_id, email) %>%
    dplyr::collect()
  participants$is_external <- !is_internal_email(participants$email) &
                              !is_synthetic_email(participants$email)

  # Meetings mit >=1 externem Teilnehmer (sonst internal-only -> raus).
  has_ext <- unique(participants$call_event_mapping_id[participants$is_external])

  # Externe MAPPED Leads (is_primary_crm) je Meeting.
  crm_map <- dplyr::tbl(con, I("mapping.crm_lead_msgraph_contact")) %>%
    dplyr::filter(is_primary_crm == TRUE) %>%
    dplyr::select(msgraph_contact_id, crm_lead_id) %>%
    dplyr::collect()
  ext_part <- participants[participants$is_external, c("call_event_mapping_id", "contact_id")]
  meeting_lead <- merge(ext_part, crm_map, by.x = "contact_id", by.y = "msgraph_contact_id")
  meeting_lead <- unique(data.frame(
    call_event_mapping_id = meeting_lead$call_event_mapping_id,
    lead_id               = meeting_lead$crm_lead_id,
    stringsAsFactors      = FALSE))

  # internal-only Meetings raus, dann per (Meeting x externer Lead) expandieren.
  # all.x = TRUE -> Meetings mit externem Teilnehmer aber ohne gemappten Lead
  # behalten EINE Zeile mit lead_id = NA (Platzhalter).
  msgraph_meetings <- msgraph_meetings[msgraph_meetings$call_event_mapping_id %in% has_ext, , drop = FALSE]
  msgraph_meetings <- merge(msgraph_meetings, meeting_lead, by = "call_event_mapping_id", all.x = TRUE)

  message("  leite CRM-VC-Termine aus Rohdaten ab ...")
  tasks <- dplyr::tbl(con, I("raw.crm_lead_tasks")) %>%
    dplyr::filter(is_deleted == FALSE) %>%
    dplyr::select(id, crm_task_id, lead_id, user_id, assigned_to_user_id,
                  precise_time, task_created_at, task_name) %>%
    dplyr::collect()
  comments <- dplyr::tbl(con, I("raw.crm_lead_task_comments")) %>%
    dplyr::filter(is_deleted == FALSE) %>%
    dplyr::select(task_id, comment_name) %>%
    dplyr::collect()
  ruc <- resolve_crm_user_contact(con)

  vc <- tasks[is_vc_task(tasks$task_name), , drop = FALSE]
  vc$meeting_tool     <- extract_meeting_tool(vc$task_name)
  vc$is_external_tool <- is_external_tool(vc$meeting_tool)
  vc$meeting_type     <- extract_meeting_type(vc$task_name)
  # staerkster Status je Task (Surrogat-id-Join, wie in Phase 2)
  rank <- c(storniert = 3L, no_show = 2L, show_up = 1L, unbekannt = 0L)
  comments$status <- classify_meeting_status(comments$comment_name)
  comments$rank   <- rank[comments$status]
  agg <- stats::aggregate(rank ~ task_id, data = comments, FUN = max)
  agg$meeting_status <- names(rank)[match(agg$rank, rank)]
  vc$meeting_status <- agg$meeting_status[match(as.character(vc$id), as.character(agg$task_id))]
  vc$meeting_status[is.na(vc$meeting_status)] <- "unbekannt"
  vc$precise_time <- as.POSIXct(vc$precise_time, tz = "UTC")
  vc$event_date   <- as.Date(vc$precise_time, tz = "Europe/Berlin")
  # Rep-Kontakt (coalesce assigned_to_user_id/user_id im Character-Raum -> integer64-sicher)
  uid <- ifelse(!is.na(vc$assigned_to_user_id), as.character(vc$assigned_to_user_id),
                as.character(vc$user_id))
  vc$contact_id <- ruc$contact_id[match(uid, as.character(ruc$user_id))]
  vc <- vc[!is.na(vc$contact_id), , drop = FALSE]

  crm_meetings <- data.frame(
    crm_task_id = vc$crm_task_id, lead_id = vc$lead_id, event_date = vc$event_date,
    precise_time = vc$precise_time, contact_id = vc$contact_id, meeting_tool = vc$meeting_tool,
    meeting_type = vc$meeting_type,
    meeting_status = vc$meeting_status, is_external_tool = vc$is_external_tool,
    original_created_at = vc$task_created_at, stringsAsFactors = FALSE)

  rows <- assemble_unified_meetings(msgraph_meetings, crm_meetings)

  # ID-Spalten (im Assemble character) auf integer64 (=bigint) casten; NA -> NULL.
  rows$contact_id           <- bit64::as.integer64(rows$contact_id)
  rows$lead_id              <- bit64::as.integer64(rows$lead_id)
  rows$organizer_contact_id <- bit64::as.integer64(rows$organizer_contact_id)

  message(paste0("  ", nrow(rows), " Zeilen -> processed.sales_meetings_unified"))

  pool::poolWithTransaction(con, function(conn) {
    Billomatics::postgres_upsert_data(
      conn, "processed", "sales_meetings_unified",
      rows, match_cols = c("meeting_key"), delete_missing = TRUE)
  })
  message("  fertig.")
  invisible(nrow(rows))
}
