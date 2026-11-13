#' Das Bookings-Postfach
#'
#' Das Postfach, von dem Bookings die Kalenderkopie in den Kalender des AE legt.
#' Eine Kalenderzeile mit dieser Adresse als Teilnehmer ist eine Kalenderkopie.
#' Dieselbe Adresse wie `kpiR:::sdr_bookings_postfach()`; hier eigens gefuehrt,
#' weil dbconnectorR nicht von kpiR abhaengt.
#'
#' @keywords internal
BOOKINGS_MAILBOX <- "studyflixgmbhsales@bertelsmann.onmicrosoft.com"

#' teams_status auf Englisch
#'
#' Uebersetzt die Werte von [derive_call_status()] in die Werte von
#' `processed.sales_meetings.teams_status` (Spec 2026-11-11-sales-meetings, Q9).
#'
#' @keywords internal
TEAMS_STATUS_EN <- c(in_zukunft = "future", extern_call = "external_joined",
                     intern_call = "internal_only", alt_tenant = "old_tenant",
                     nicht_abrufbar = "not_retrievable", ohne_link = "no_link",
                     no_call = "no_call")

#' crm_outcome auf Englisch
#'
#' Uebersetzt die Werte von [crm_task_outcomes()] in die Werte von
#' `processed.sales_meeting_leads.crm_outcome`. Ohne Task gilt `no_task`.
#'
#' @keywords internal
CRM_OUTCOME_EN <- c(show_up = "held", no_show = "no_show", storniert = "cancelled",
                    ohne_ausgang = "no_outcome")

#' Zeitstempel ohne Zeitzone als UTC-Zeitpunkt
#'
#' Spalten `timestamp without time zone` tragen UTC-Ziffern, kommen je nach
#' Treiber aber mit einem anderen tz-Attribut. `force_tz` behaelt die Ziffern und
#' setzt UTC, wie in [compute_observability_exclusions()].
#'
#' @param x POSIXct (oder NA) aus einer Spalte ohne Zeitzone.
#' @return POSIXct in UTC.
#' @keywords internal
# ---- start ---- #
sm_utc <- function(x) {
  # Nicht as.POSIXct(x, tz = "UTC") auf POSIXct: das setzt nur das Attribut um
  # und verschiebt damit die Ziffern.
  if (!inherits(x, "POSIXct")) x <- as.POSIXct(x, tz = "UTC")
  lubridate::force_tz(x, "UTC")
}

#' Ketten-ID je Meeting record
#'
#' Rein. Eine Kette sind alle Zeilen, die ueber ihre Nachfolger bei derselben
#' letzten Zeile ankommen. Ihre erste Zeile ist eine ohne Vorgaenger; hat die
#' Kette mehrere, die mit dem kleinsten `order_value` (Spec, Q11 und Q19: die
#' kleinste `msgraph_event_id`), bei Gleichstand der kleinere Schluessel. Eine
#' Zeile ohne Kette ist ihre eigene erste Zeile.
#'
#' @param meeting_key character, eindeutig.
#' @param successor_key character je Zeile, NA ohne Nachfolger.
#' @param order_value numeric je Zeile, kleiner heisst frueher.
#' @return character je Zeile: der `meeting_key` der ersten Zeile ihrer Kette.
#' @keywords internal
# ---- start ---- #
derive_meeting_chains <- function(meeting_key, successor_key, order_value) {
  n <- length(meeting_key)
  if (n == 0) return(character(0))
  nachfolger <- match(successor_key, meeting_key)
  ende <- seq_len(n)
  for (schritt in seq_len(100)) {
    weiter <- !is.na(nachfolger[ende])
    if (!any(weiter)) break
    ende[weiter] <- nachfolger[ende[weiter]]
  }
  if (any(!is.na(nachfolger[ende]))) {
    stop("derive_meeting_chains: Nachfolger bilden einen Kreis. Bug in derive_calendar_state().")
  }

  anfaenge <- which(!(seq_len(n) %in% nachfolger))
  o <- order(ende[anfaenge], order_value[anfaenge], meeting_key[anfaenge], na.last = TRUE)
  sortiert <- anfaenge[o]
  erster <- sortiert[!duplicated(ende[sortiert])]
  meeting_key[erster[match(ende, ende[erster])]]
}

#' Meeting records und ihre Leads zusammenbauen
#'
#' Rein (kein DB-Zugriff). Baut aus den Rohdaten je Meeting record eine Zeile
#' fuer `processed.sales_meetings` und je Record und Lead eine Zeile fuer
#' `processed.sales_meeting_leads`, ohne Personen und ohne Datenbank-IDs. Den
#' Ablauf beschreibt die Spec `docs/specs/2026-11-11-sales-meetings.md`,
#' Abschnitt *Loesung*.
#'
#' @param events data.frame aus `raw.msgraph_events`, alle Zeilen: id,
#'   msgraph_ical_uid, event_created_at, event_updated_at, event_start,
#'   is_canceled, is_single_instance, join_url, join_url_checked_at,
#'   vanished_at, created_at, optional online_meeting_lookup.
#' @param participants data.frame aus `raw.msgraph_event_participants` mit der
#'   Adresse aus `raw.msgraph_contacts`: event_id, contact_id, is_organizer,
#'   email.
#' @param call_event data.frame aus `mapping.msgraph_call_event`: call_id,
#'   event_id, event_class.
#' @param calls data.frame aus `raw.msgraph_calls`: id, call_start, call_end.
#' @param call_participants data.frame aus `raw.msgraph_call_participants` mit
#'   Adresse: call_id, contact_id, email.
#' @param speaker_counts data.frame aus [load_external_speaker_counts()]:
#'   call_id, external_speaker_count.
#' @param lead_mappings data.frame aus `mapping.crm_lead_msgraph_contact`:
#'   msgraph_contact_id, crm_lead_id.
#' @param crm_mails data.frame aus `raw.crm_lead_mail_address`: lead_id,
#'   mail_address_name, is_deleted.
#' @param tasks data.frame aus [load_crm_vc_tasks()].
#' @param comments data.frame aus [load_crm_task_comments()].
#' @param tenant_id GUID des eigenen Tenants, siehe [derive_call_status()].
#' @param now_utc POSIXct, Bezugspunkt fuer `future`.
#' @return list mit `meetings` (eine Zeile je Record, dazu die Hilfsspalten
#'   successor_key, chain_root_key, responsible_contact_id,
#'   organizer_contact_id, organizer_source und booked_at_utc) und `leads`
#'   (meeting_key, lead_id und die Spalten von `sales_meeting_leads` ohne
#'   meeting_id). IDs als character.
#' @keywords internal
# ---- start ---- #
build_sales_meetings <- function(events, participants, call_event, calls, call_participants,
                                 speaker_counts, lead_mappings, crm_mails, tasks, comments,
                                 tenant_id = NULL, now_utc = Sys.time()) {

  # ---- Eingaben normalisieren: IDs als character (integer64-sicher) ---- #
  ev <- as.data.frame(events)
  ev$event_id  <- as.character(ev$id)
  ev$uid       <- as.character(ev$msgraph_ical_uid)
  ev$start_utc <- sm_utc(ev$event_start)
  if (!"online_meeting_lookup" %in% names(ev)) {
    ev$online_meeting_lookup <- rep(NA_character_, nrow(ev))
  }

  pa <- as.data.frame(participants)
  pa$event_id   <- as.character(pa$event_id)
  pa$contact_id <- as.character(pa$contact_id)
  pa$is_external <- !is.na(pa$email) & !(is_internal_email(pa$email) %in% TRUE) &
    !is_synthetic_email(pa$email)

  # ---- 1. Startmenge: Kalenderzeilen mit mindestens einem Externen ---- #
  zaehlung <- as.data.frame(count_event_participants(pa[, c("event_id", "email")]))
  zaehlung$event_id <- as.character(zaehlung$event_id)
  rec <- ev[ev$event_id %in% zaehlung$event_id[zaehlung$external_count >= 1], , drop = FALSE]
  z <- match(rec$event_id, zaehlung$event_id)
  rec$internal_participant_count <- as.integer(zaehlung$internal_count[z])
  rec$external_participant_count <- as.integer(zaehlung$external_count[z])

  # Bookings: der Eintrag selbst und die Kalenderkopie (Q3).
  ist_eintrag <- startsWith(rec$uid, "booking:") %in% TRUE
  kopie_ids <- unique(pa$event_id[tolower(pa$email) %in% BOOKINGS_MAILBOX])
  ist_kopie <- !ist_eintrag & rec$event_id %in% kopie_ids
  rec$booking_role <- ifelse(ist_eintrag, "booking_entry",
                             ifelse(ist_kopie, "calendar_copy", NA_character_))
  rec$ist_bookings <- ist_eintrag | ist_kopie

  extern <- pa[pa$is_external, c("event_id", "email"), drop = FALSE]
  extern$email_lower <- tolower(extern$email)
  extern <- unique(extern[, c("event_id", "email_lower")])
  mit_zeit <- function(ids) {
    x <- extern[extern$event_id %in% ids, , drop = FALSE]
    m <- match(x$event_id, rec$event_id)
    x$event_start <- rec$start_utc[m]
    x$msgraph_ical_uid <- rec$uid[m]
    x
  }

  # ---- 2. Kalenderzustand, zweistufig (Q2) ---- #
  # Ueber alle Kalenderzeilen, damit jede Version einer ical_uid mitzaehlt.
  zustand_ein <- data.frame(
    id = ev$event_id, msgraph_ical_uid = ev$uid, is_canceled = ev$is_canceled,
    is_single_instance = ev$is_single_instance, event_start = ev$start_utc,
    event_updated_at = sm_utc(ev$event_updated_at), vanished_at = sm_utc(ev$vanished_at),
    stringsAsFactors = FALSE)
  stufe1 <- derive_calendar_state(zustand_ein)
  s1 <- stufe1$calendar_status[match(rec$event_id, stufe1$event_id)]
  abgesagt <- rec$event_id[s1 %in% c("cancelled", "vanished") & !rec$ist_bookings]
  aktiv    <- rec$event_id[s1 %in% "active" & !rec$ist_bookings]
  ersatz <- detect_replaced_events(mit_zeit(abgesagt), mit_zeit(aktiv))
  zustand <- derive_calendar_state(zustand_ein, ersatz)

  m <- match(rec$event_id, zustand$event_id)
  rec$calendar_status <- zustand$calendar_status[m]
  rec$calendar_changed_at_utc <- zustand$calendar_changed_at_utc[m]
  nachfolger <- zustand$successor_event_id[m]
  rec$successor_key <- ifelse(!is.na(nachfolger) & nachfolger %in% rec$event_id,
                              paste0("msgraph_", nachfolger), NA_character_)

  # ---- 3. Legedatum: erst in Schritt 7b ---- #
  # Eine belegte Umzugskopie bekommt das Datum ihres CRM-Tasks, und das gilt schon
  # fuer Kurzlebigkeit, Personenrollen und das Erbe an den Ersatztermin (Spec
  # 2026-11-13-mailbox-copy-status, Q12). Die Schritte 4 bis 7 brauchen kein
  # Legedatum.

  # ---- 4. Teams: alle Sitzungen des Events (Q9, Q16) ---- #
  ce <- as.data.frame(call_event)
  ce$event_id <- as.character(ce$event_id)
  ce$call_id  <- as.character(ce$call_id)
  ce <- ce[ce$event_id %in% rec$event_id, , drop = FALSE]
  cl <- as.data.frame(calls)
  cl_id <- as.character(cl$id)
  ce$call_start <- sm_utc(cl$call_start[match(ce$call_id, cl_id)])
  ce$call_end   <- sm_utc(cl$call_end[match(ce$call_id, cl_id)])
  ce$dauer <- as.numeric(difftime(ce$call_end, ce$call_start, units = "secs"))
  klasse <- as.character(ce$event_class)

  mit_extern <- unique(ce$event_id[grepl("extern_call$", klasse, ignore.case = TRUE)])
  mit_intern <- unique(ce$event_id[grepl("intern_call$", klasse, ignore.case = TRUE)])
  rec$event_class <- ifelse(rec$event_id %in% mit_extern, "extern_call",
                            ifelse(rec$event_id %in% mit_intern, "intern_call", "no_call"))
  status_de <- derive_call_status(
    data.frame(id = rec$event_id, event_start = rec$start_utc, join_url = rec$join_url,
               join_url_checked_at = rec$join_url_checked_at,
               online_meeting_lookup = rec$online_meeting_lookup,
               event_class = rec$event_class, stringsAsFactors = FALSE),
    tenant_id = tenant_id, now_utc = now_utc)
  rec$teams_status <- unname(TEAMS_STATUS_EN[status_de])

  # Laengster Call: Beginn und Dauer.
  lc <- ce[!is.na(ce$call_id) & !is.na(ce$dauer), , drop = FALSE]
  lc <- lc[order(lc$event_id, -lc$dauer, as.numeric(lc$call_start), lc$call_id), , drop = FALSE]
  lc <- lc[!duplicated(lc$event_id), , drop = FALSE]
  m <- match(rec$event_id, lc$event_id)
  rec$call_started_at_utc   <- lc$call_start[m]
  rec$call_duration_minutes <- lc$dauer[m] / 60

  # Externe Sprecher: Maximum ueber alle Transkripte der Calls des Events.
  sc <- as.data.frame(speaker_counts)
  sc$call_id <- as.character(sc$call_id)
  es <- ce[ce$call_id %in% sc$call_id, c("event_id", "call_id"), drop = FALSE]
  es$n <- sc$external_speaker_count[match(es$call_id, sc$call_id)]
  hoechste <- if (nrow(es) > 0) tapply(es$n, es$event_id, max) else integer(0)
  rec$external_speaker_count <- as.integer(hoechste[rec$event_id])

  # ---- 5. Verantwortliche Person (Q5) ---- #
  # Je Event der Call, den die Klassifikation waehlt: extern vor No-Show-Klasse,
  # dann der laengste.
  ce$no_show_klasse <- grepl("no_call|intern_call", klasse, ignore.case = TRUE)
  ce$dauer0 <- ifelse(is.na(ce$dauer), 0, ce$dauer)
  hc <- ce[order(ce$event_id, ce$no_show_klasse, -ce$dauer0), c("event_id", "call_id"), drop = FALSE]
  hc <- hc[!duplicated(hc$event_id), , drop = FALSE]
  event_calls <- data.frame(event_id = rec$event_id,
                            call_id = hc$call_id[match(rec$event_id, hc$event_id)],
                            stringsAsFactors = FALSE)
  org <- pa[pa$is_organizer %in% TRUE & is_internal_email(pa$email) %in% TRUE,
            c("event_id", "contact_id"), drop = FALSE]
  org <- org[!duplicated(org$event_id), , drop = FALSE]
  names(org) <- c("event_id", "organizer_contact_id")
  cp <- as.data.frame(call_participants)
  cp <- data.frame(call_id = as.character(cp$call_id), contact_id = as.character(cp$contact_id),
                   is_internal = is_internal_email(cp$email), stringsAsFactors = FALSE)
  verantwortlich <- derive_responsible_contact(event_calls, org, cp)
  rec$responsible_contact_id <- as.character(verantwortlich$responsible_contact_id[
    match(rec$event_id, verantwortlich$event_id)])
  rec$organizer_contact_id <- org$organizer_contact_id[match(rec$event_id, org$event_id)]

  # Organisator mit externer Adresse: Der Kunde laedt in sein eigenes Teams-Meeting
  # ein, dessen Anwesenheitsbericht nie bei uns ankommt (Spec
  # 2026-11-12-referenz-termin-bewertung, Q14). Ein interner Organisator gewinnt;
  # ohne Organisator-Zeile bleibt es NA.
  org_alle <- pa[pa$is_organizer %in% TRUE, , drop = FALSE]
  org_intern <- unique(org_alle$event_id[is_internal_email(org_alle$email) %in% TRUE])
  org_extern <- unique(org_alle$event_id[org_alle$is_external])
  rec$organizer_is_external <- ifelse(rec$event_id %in% org_intern, FALSE,
                                      ifelse(rec$event_id %in% org_extern, TRUE, NA))

  # ---- 6. Duplikate (Q2, Q20) ---- #
  geprueft <- rec$event_id[rec$calendar_status %in% "active" & !rec$ist_bookings]
  dk <- mit_zeit(geprueft)
  dk$is_no_show_class <- rec$teams_status[match(dk$event_id, rec$event_id)] != "external_joined"
  rec$is_duplicate <- rec$event_id %in% as.character(detect_duplicate_events(dk))

  # ---- 7. Leads je Event und CRM-Zuordnung (Q1) ---- #
  lp <- pa[!(pa$is_organizer %in% TRUE) & !is.na(pa$email) & pa$event_id %in% rec$event_id,
           c("event_id", "contact_id", "email"), drop = FALSE]
  names(lp)[1] <- "call_event_mapping_id"
  lm <- as.data.frame(lead_mappings)
  lm <- data.frame(msgraph_contact_id = as.character(lm$msgraph_contact_id),
                   crm_lead_id = as.character(lm$crm_lead_id), stringsAsFactors = FALSE)
  ev_leads <- meeting_leads_ableiten(lp, lm, as.data.frame(crm_mails))
  ev_leads <- data.frame(event_id = as.character(ev_leads$call_event_mapping_id),
                         lead_id = as.character(ev_leads$lead_id),
                         quelle = ev_leads$quelle, stringsAsFactors = FALSE)

  kandidat_ev <- rec$event_id[!rec$ist_bookings & !(rec$calendar_status %in% "stale_copy")]
  kz <- merge(data.frame(event_id = kandidat_ev, stringsAsFactors = FALSE),
              ev_leads[, c("event_id", "lead_id")], by = "event_id", all.x = TRUE, sort = TRUE)
  km <- match(kz$event_id, rec$event_id)
  tk <- as.data.frame(tasks)
  # Unter mehreren Kandidaten gewinnt die einzige aktive Zeile: sofort, wenn alle
  # Kandidaten in einer Kette liegen, sonst nur statt einer Mehrdeutigkeit (Q21, Q25).
  # Die Kette steht schon hier fest, sie haengt nur an den Nachfolgern aus Schritt 2.
  kal_kette <- derive_meeting_chains(paste0("msgraph_", rec$event_id), rec$successor_key,
                                     as.numeric(rec$event_id))
  abgleich <- match_crm_tasks(tk, kz$lead_id, as.Date(rec$start_utc[km], tz = "Europe/Berlin"),
                              rec$responsible_contact_id[km], rec$start_utc[km],
                              aktiv = rec$calendar_status[km] %in% "active",
                              kette = kal_kette[km])
  tk$task_id     <- as.character(tk$id)
  tk$crm_task    <- as.character(tk$crm_task_id)
  tk$lead_chr    <- as.character(tk$lead_id)
  tk$event_hit   <- kz$event_id[abgleich$row]
  tk$match_rule  <- abgleich$match_rule

  # ---- 7b. Umzugskopien und Legedatum (Q4, Q10; Spec 2026-11-13-mailbox-copy-status) ---- #
  # Legedatum wie original_created_at: je ical_uid die frueheste Anlage, dann das
  # Erbe der Umzugskopie (#62). Wo das Erbe greift, hat die ical_uid ein frueheres
  # Original.
  eigen <- compute_original_created_at(data.frame(
    msgraph_ical_uid = ev$uid, event_created_at = ev$event_created_at,
    created_at = ev$created_at, stringsAsFactors = FALSE))
  angelegt <- inherit_moved_copy_created_at(
    eigen,
    data.frame(id = ev$event_id, msgraph_ical_uid = ev$uid, event_start = ev$event_start,
               stringsAsFactors = FALSE),
    pa[, c("event_id", "contact_id", "email")])
  gelegt <- sm_utc(angelegt$original_created_at[match(rec$uid, angelegt$msgraph_ical_uid)])
  gelegt_eigen <- sm_utc(eigen$original_created_at[match(rec$uid, eigen$msgraph_ical_uid)])
  mit_original <- (gelegt < gelegt_eigen) %in% TRUE

  # Umzugs-Sitzungen ueber alle Kalenderzeilen, auch interne Termine (Q9). Der
  # Organisator ist hier jede Adresse, die kleinste contact_id (Q10).
  org_jede <- pa[pa$is_organizer %in% TRUE & !is.na(pa$contact_id), c("event_id", "contact_id"),
                 drop = FALSE]
  org_jede <- org_jede[order(org_jede$event_id, as.numeric(org_jede$contact_id)), , drop = FALSE]
  org_jede <- org_jede[!duplicated(org_jede$event_id), , drop = FALSE]
  sitzung_ids <- detect_move_sessions(data.frame(
    event_id = ev$event_id, msgraph_ical_uid = ev$uid,
    event_created_at = sm_utc(ev$event_created_at), event_start = ev$start_utc,
    organizer_contact_id = org_jede$contact_id[match(ev$event_id, org_jede$event_id)],
    stringsAsFactors = FALSE))

  # Beleg: der frueheste eigene CRM-Task der Zeile (Q5). Geloeschte Tasks laedt
  # load_crm_vc_tasks() gar nicht erst.
  getroffen <- tk[!is.na(tk$event_hit), , drop = FALSE]
  erster_task <- if (nrow(getroffen) > 0) {
    tapply(as.numeric(sm_utc(getroffen$task_created_at)), getroffen$event_hit, min)
  } else numeric(0)
  rec$first_crm_task_at <- .POSIXct(unname(erster_task[rec$event_id]), tz = "UTC")

  # Geprueft wird jede Kalenderzeile ausser booking:-Eintraegen (Q9).
  rec$mailbox_copy_status <- NA_character_
  pruefen <- !ist_eintrag
  rec$mailbox_copy_status[pruefen] <- detect_mailbox_copies(data.frame(
    event_created_at = sm_utc(rec$event_created_at[pruefen]),
    event_start = rec$start_utc[pruefen],
    in_move_session = rec$event_id[pruefen] %in% sitzung_ids,
    has_earlier_original = mit_original[pruefen],
    first_crm_task_at = rec$first_crm_task_at[pruefen]))

  # Eine belegte Kopie traegt den Tag ihres fruehesten Tasks (Q4), ausser die
  # ical_uid ist noch aelter: dann gewinnt wie ueberall das fruehere Datum (Q17).
  quelle <- rep("outlook", nrow(rec))
  belegt <- rec$mailbox_copy_status %in% "evidenced" &
    (is.na(gelegt) | rec$first_crm_task_at < gelegt) %in% TRUE
  gelegt[belegt] <- rec$first_crm_task_at[belegt]
  quelle[belegt] <- "crm_task"

  # Ein Ersatztermin erbt das fruehere Datum seines abgesagten Vorgaengers, samt
  # dessen Quelle (Q16).
  if (nrow(ersatz) > 0) {
    alt_i <- match(as.character(ersatz$canceled_event_id), rec$event_id)
    neu <- match(as.character(ersatz$replacement_event_id), rec$event_id)
    for (k in seq_along(neu)) {
      alt <- gelegt[alt_i[k]]
      if (!is.na(neu[k]) && !is.na(alt) && (is.na(gelegt[neu[k]]) || alt < gelegt[neu[k]])) {
        gelegt[neu[k]] <- alt
        quelle[neu[k]] <- quelle[alt_i[k]]
      }
    }
  }
  # booking:-Eintraege tragen keine echte Anlagezeit (msgraph_scoped_bookings.R).
  gelegt[ist_eintrag] <- rec$start_utc[ist_eintrag]
  rec$booked_at_utc <- gelegt
  rec$booked_date <- as.Date(rec$booked_at_utc, tz = "Europe/Berlin")
  rec$booked_date_source <- ifelse(ist_eintrag, "bookings_start", quelle)

  # Kurzlebig: Absagezeitpunkt vanished_at, sonst event_updated_at.
  abgesagt_am <- dplyr::coalesce(sm_utc(rec$vanished_at), sm_utc(rec$event_updated_at))
  rec$is_short_lived <- detect_short_lived_events(rec$is_canceled %in% TRUE, abgesagt_am,
                                                  rec$booked_at_utc)
  rec$is_short_lived[ist_eintrag] <- NA

  ersatz_leads <- tk[tk$match_rule %in% "ersatzregel", , drop = FALSE]
  ersatz_leads <- data.frame(event_id = ersatz_leads$event_hit, lead_id = ersatz_leads$lead_chr,
                             quelle = rep("substitute_rule", nrow(ersatz_leads)),
                             stringsAsFactors = FALSE)
  kal_leads <- rbind(ev_leads[ev_leads$event_id %in% rec$event_id, , drop = FALSE], ersatz_leads)
  kal_leads <- kal_leads[!is.na(kal_leads$lead_id), , drop = FALSE]
  kal_leads <- data.frame(meeting_key = paste0("msgraph_", kal_leads$event_id, recycle0 = TRUE),
                          lead_id = kal_leads$lead_id, lead_match_source = kal_leads$quelle,
                          stringsAsFactors = FALSE)

  # ---- 8. CRM-Zeilen: Tasks ohne Kalenderzeile, eine Zeile je crm_task_id ---- #
  crm <- tk[is.na(tk$event_hit), , drop = FALSE]
  crm <- crm[order(as.numeric(crm$task_id)), , drop = FALSE]
  c1 <- crm[!duplicated(crm$crm_task), , drop = FALSE]
  crm_gelegt <- sm_utc(c1$task_created_at)
  crm_rows <- data.frame(
    meeting_key = paste0("crm_", c1$crm_task, recycle0 = TRUE),
    msgraph_event_id = rep(NA_character_, nrow(c1)),
    meeting_date = as.Date(c1$precise_time, tz = "Europe/Berlin"),
    starts_at_utc = as.POSIXct(c1$precise_time, tz = "UTC"),
    booked_date = as.Date(crm_gelegt, tz = "Europe/Berlin"),
    booked_date_source = rep("crm_task", nrow(c1)),
    calendar_status = rep("not_in_calendar", nrow(c1)),
    calendar_changed_at_utc = .POSIXct(rep(NA_real_, nrow(c1)), tz = "UTC"),
    teams_status = rep("not_in_calendar", nrow(c1)),
    booking_role = rep(NA_character_, nrow(c1)),
    is_short_lived = rep(NA, nrow(c1)),
    is_duplicate = rep(NA, nrow(c1)),
    mailbox_copy_status = rep(NA_character_, nrow(c1)),
    internal_participant_count = rep(NA_integer_, nrow(c1)),
    external_participant_count = rep(NA_integer_, nrow(c1)),
    external_speaker_count = rep(NA_integer_, nrow(c1)),
    call_started_at_utc = .POSIXct(rep(NA_real_, nrow(c1)), tz = "UTC"),
    call_duration_minutes = rep(NA_real_, nrow(c1)),
    successor_key = rep(NA_character_, nrow(c1)),
    responsible_contact_id = as.character(c1$contact_id),
    organizer_contact_id = rep(NA_character_, nrow(c1)),
    organizer_is_external = rep(NA, nrow(c1)),
    organizer_source = rep("crm_task", nrow(c1)),
    booked_at_utc = crm_gelegt,
    stringsAsFactors = FALSE)
  crm_leads <- crm[!is.na(crm$lead_chr), , drop = FALSE]
  crm_leads <- data.frame(meeting_key = paste0("crm_", crm_leads$crm_task, recycle0 = TRUE),
                          lead_id = crm_leads$lead_chr,
                          lead_match_source = rep("crm_task", nrow(crm_leads)),
                          stringsAsFactors = FALSE)

  kal_rows <- data.frame(
    meeting_key = paste0("msgraph_", rec$event_id, recycle0 = TRUE),
    msgraph_event_id = rec$event_id,
    meeting_date = as.Date(rec$start_utc, tz = "Europe/Berlin"),
    starts_at_utc = rec$start_utc,
    booked_date = rec$booked_date,
    booked_date_source = rec$booked_date_source,
    calendar_status = rec$calendar_status,
    calendar_changed_at_utc = as.POSIXct(rec$calendar_changed_at_utc, tz = "UTC"),
    teams_status = rec$teams_status,
    booking_role = rec$booking_role,
    is_short_lived = rec$is_short_lived,
    is_duplicate = rec$is_duplicate,
    mailbox_copy_status = rec$mailbox_copy_status,
    internal_participant_count = rec$internal_participant_count,
    external_participant_count = rec$external_participant_count,
    external_speaker_count = rec$external_speaker_count,
    call_started_at_utc = as.POSIXct(rec$call_started_at_utc, tz = "UTC"),
    call_duration_minutes = rec$call_duration_minutes,
    successor_key = rec$successor_key,
    responsible_contact_id = rec$responsible_contact_id,
    organizer_contact_id = rec$organizer_contact_id,
    organizer_is_external = rec$organizer_is_external,
    organizer_source = rep("msgraph", nrow(rec)),
    booked_at_utc = rec$booked_at_utc,
    stringsAsFactors = FALSE)

  meetings <- rbind(kal_rows, crm_rows)
  rownames(meetings) <- NULL
  meetings$chain_root_key <- derive_meeting_chains(
    meetings$meeting_key, meetings$successor_key, as.numeric(meetings$msgraph_event_id))

  # ---- 9. Leads mit dem Task, der fuer sie zaehlt ---- #
  leads <- rbind(kal_leads, crm_leads)
  leads <- leads[!duplicated(paste(leads$meeting_key, leads$lead_id)), , drop = FALSE]

  ausgang <- crm_task_outcomes(tk$id, comments)
  ao <- match(tk$task_id, as.character(ausgang$task_id))
  tk$crm_status          <- ausgang$crm_status[ao]
  tk$crm_status_conflict <- ausgang$crm_status_conflict[ao]
  tk$rang <- CRM_STATUS_RANG[tk$crm_status]
  tk$meeting_key <- ifelse(is.na(tk$event_hit), paste0("crm_", tk$crm_task),
                           paste0("msgraph_", tk$event_hit))
  zugeordnet <- tk[!is.na(tk$lead_chr), , drop = FALSE]
  paar <- paste(zugeordnet$meeting_key, zugeordnet$lead_chr)
  zugeordnet <- zugeordnet[order(paar, -zugeordnet$rang, as.numeric(zugeordnet$crm_task)), , drop = FALSE]
  paar <- paste(zugeordnet$meeting_key, zugeordnet$lead_chr)
  sieger <- zugeordnet[!duplicated(paar), , drop = FALSE]
  sieger_paar <- paar[!duplicated(paar)]
  konflikt <- if (nrow(zugeordnet) > 0) {
    tapply(seq_len(nrow(zugeordnet)), paar, function(ix) {
      erkannt <- zugeordnet$crm_status[ix][zugeordnet$crm_status[ix] != "ohne_ausgang"]
      length(unique(erkannt)) > 1 || any(zugeordnet$crm_status_conflict[ix] %in% TRUE)
    })
  } else logical(0)

  lp_paar <- paste(leads$meeting_key, leads$lead_id)
  s <- sieger[match(lp_paar, sieger_paar), , drop = FALSE]
  mit_task <- !is.na(s$task_id)
  leads$crm_lead_task_id     <- s$task_id
  leads$crm_outcome          <- ifelse(mit_task, unname(CRM_OUTCOME_EN[s$crm_status]), "no_task")
  leads$crm_outcome_conflict <- mit_task & as.logical(konflikt[lp_paar]) %in% TRUE
  leads$meeting_tool         <- s$meeting_tool
  leads$meeting_type         <- s$meeting_type
  leads$is_external_tool     <- s$is_external_tool
  rownames(leads) <- NULL

  list(meetings = meetings, leads = leads)
}

#' Personen an die Meeting records haengen
#'
#' Rein. Ruft `person_roles` auf die Zeilen und uebernimmt
#' responsible_person_id, organizer_person_id und organizer_type. Eine
#' Person-ID, die es in `raw.personio_persons` nicht gibt, wird NA: die Person
#' ist dann ohnehin nicht mehr aufloesbar, und der Fremdschluessel brache den
#' Lauf ab.
#'
#' @param meetings `meetings` aus [build_sales_meetings()].
#' @param person_roles Funktion, die ein data.frame mit contact_id,
#'   organizer_contact_id, organizer_source, original_created_at und
#'   event_date nimmt und es um responsible_person_id, organizer_person_id und
#'   organizer_type ergaenzt, in derselben Zeilenfolge
#'   (`kpiR::meeting_person_roles()` mit gebundenen Eingaben).
#' @param person_ids IDs aus `raw.personio_persons.id`.
#' @return `meetings` mit den drei Spalten.
#' @keywords internal
# ---- start ---- #
attach_meeting_persons <- function(meetings, person_roles, person_ids) {
  eingabe <- data.frame(
    contact_id = meetings$responsible_contact_id,
    organizer_contact_id = meetings$organizer_contact_id,
    organizer_source = meetings$organizer_source,
    original_created_at = meetings$booked_at_utc,
    event_date = meetings$meeting_date,
    stringsAsFactors = FALSE)
  rollen <- person_roles(eingabe)
  if (nrow(rollen) != nrow(meetings)) {
    stop("attach_meeting_persons: person_roles liefert ", nrow(rollen), " statt ",
         nrow(meetings), " Zeilen.")
  }
  bekannt <- as.character(person_ids)
  pruefen <- function(x) {
    x <- as.character(x)
    x[!(x %in% bekannt)] <- NA_character_
    x
  }
  meetings$responsible_person_id <- pruefen(rollen$responsible_person_id)
  meetings$organizer_person_id   <- pruefen(rollen$organizer_person_id)
  meetings$organizer_type        <- as.character(rollen$organizer_type)
  meetings
}

#' Spalten, die update_sales_meetings() nach processed.sales_meetings schreibt
#'
#' Ohne `id`, `created_at`, `updated_at` (vergibt die Datenbank) und ohne
#' `successor_meeting_id`, `meeting_chain_id` (setzt ein UPDATE nach dem Upsert).
#'
#' @keywords internal
SALES_MEETINGS_SPALTEN <- c(
  "meeting_key", "msgraph_event_id", "meeting_date", "starts_at_utc", "booked_date",
  "booked_date_source", "organizer_person_id", "organizer_type", "organizer_is_external",
  "responsible_person_id", "calendar_status", "calendar_changed_at_utc", "teams_status",
  "booking_role", "is_short_lived", "is_duplicate", "mailbox_copy_status",
  "internal_participant_count",
  "external_participant_count", "external_speaker_count", "call_started_at_utc",
  "call_duration_minutes")

#' Bricht ab, wenn der Zieltabelle eine Spalte fehlt
#'
#' Rein. `Billomatics::postgres_upsert_data()` verwirft Spalten, die die Tabelle
#' nicht hat, ohne Fehler; ohne diese Pruefung bliebe eine neue Spalte nach einem
#' Deploy vor ihrer DDL still leer.
#'
#' @param vorhanden Spaltennamen der Tabelle, etwa aus `DBI::dbListFields()`.
#' @param benoetigt Spalten, die geschrieben werden sollen.
#' @param tabelle Name fuer die Meldung.
#' @return `invisible(TRUE)`; sonst `stop()`.
#' @keywords internal
# ---- start ---- #
pruefe_zielspalten <- function(vorhanden, benoetigt, tabelle) {
  fehlt <- setdiff(benoetigt, vorhanden)
  if (length(fehlt) > 0) {
    stop(tabelle, ": Spalte(n) ", paste(fehlt, collapse = ", "),
         " fehlen. Erst die DDL aus inst/sql/ ausfuehren. Abbruch.")
  }
  invisible(TRUE)
}

#' Rebuild processed.sales_meetings und processed.sales_meeting_leads
#'
#' Eine Zeile je Meeting record und eine je Record und Lead, nur Signale, keine
#' Bewertung (ADR 0003, Spec `docs/specs/2026-11-11-sales-meetings.md`). Startet
#' an `raw.msgraph_events`, haengt nicht an
#' `processed.msgraph_extern_event_classification`. Laeuft nach
#' `msgraph_map_calls_events()`, weil es `mapping.msgraph_call_event` liest.
#'
#' Geschrieben wird in einer Transaktion: erst die Records ueber `meeting_key`,
#' dann Nachfolger und Ketten-ID per UPDATE (die IDs vergibt die Datenbank),
#' zuletzt die Leads ueber `(meeting_id, lead_id)`.
#'
#' @param con Pool/DBI-Connection.
#' @param person_roles Funktion fuer die Personen, siehe
#'   [attach_meeting_persons()]. Im Nachtlauf
#'   `function(rows) kpiR::meeting_person_roles(rows, personen, sales)`.
#' @param tenant_id GUID des eigenen Tenants (`cfg$tenant_id`). NULL laesst
#'   `old_tenant` aus.
#' @param now_utc POSIXct, Bezugspunkt fuer `future`. Nur fuer Tests.
#' @return invisible(Anzahl geschriebener Records).
#' @export
update_sales_meetings <- function(con, person_roles, tenant_id = NULL, now_utc = Sys.time()) {
  # ---- start ---- #
  message("update_sales_meetings ...")
  if (!is.function(person_roles)) stop("update_sales_meetings: person_roles muss eine Funktion sein.")

  message("  lade Kalenderzeilen und Teilnehmer ...")
  lookup_spalte <- online_meeting_lookup_select_cols(con)
  events <- dplyr::tbl(con, I("raw.msgraph_events")) %>%
    dplyr::select(id, msgraph_ical_uid, event_created_at, event_updated_at, event_start,
                  is_canceled, is_single_instance, join_url, join_url_checked_at,
                  vanished_at, created_at, dplyr::all_of(lookup_spalte)) %>%
    dplyr::collect()
  participants <- dplyr::tbl(con, I("raw.msgraph_event_participants")) %>%
    dplyr::select(event_id, contact_id, is_organizer) %>%
    dplyr::left_join(dplyr::tbl(con, I("raw.msgraph_contacts")) %>% dplyr::select(id, email),
                     by = c("contact_id" = "id")) %>%
    dplyr::collect()

  message("  lade Calls und Transkripte ...")
  call_event <- dplyr::tbl(con, I("mapping.msgraph_call_event")) %>%
    dplyr::select(call_id, event_id, event_class) %>%
    dplyr::collect()
  calls <- dplyr::tbl(con, I("raw.msgraph_calls")) %>%
    dplyr::select(id, call_start, call_end) %>%
    dplyr::collect()
  call_participants <- dplyr::tbl(con, I("raw.msgraph_call_participants")) %>%
    dplyr::select(call_id, contact_id) %>%
    dplyr::left_join(dplyr::tbl(con, I("raw.msgraph_contacts")) %>% dplyr::select(id, email),
                     by = c("contact_id" = "id")) %>%
    dplyr::collect()
  # Transkripte nur fuer Calls von Events mit Externem, nicht fuer interne Meetings.
  mit_extern <- unique(as.character(participants$event_id[
    !is.na(participants$email) & !(is_internal_email(participants$email) %in% TRUE) &
      !is_synthetic_email(participants$email)]))
  speaker_counts <- load_external_speaker_counts(
    con, call_event$call_id[as.character(call_event$event_id) %in% mit_extern])

  message("  lade Lead-Zuordnung und CRM-Tasks ...")
  lead_mappings <- dplyr::tbl(con, I("mapping.crm_lead_msgraph_contact")) %>%
    dplyr::select(msgraph_contact_id, crm_lead_id) %>%
    dplyr::collect()
  crm_mails <- dplyr::tbl(con, I("raw.crm_lead_mail_address")) %>%
    dplyr::select(lead_id, mail_address_name, is_deleted) %>%
    dplyr::collect()
  tasks    <- load_crm_vc_tasks(con)
  comments <- load_crm_task_comments(con)
  person_ids <- dplyr::tbl(con, I("raw.personio_persons")) %>%
    dplyr::select(id) %>%
    dplyr::collect()

  message("  baue Meeting records ...")
  gebaut <- build_sales_meetings(events, participants, call_event, calls, call_participants,
                                 speaker_counts, lead_mappings, crm_mails, tasks, comments,
                                 tenant_id = tenant_id, now_utc = now_utc)
  meetings <- attach_meeting_persons(gebaut$meetings, person_roles, person_ids$id)
  leads <- gebaut$leads

  # delete_missing = TRUE leert die Tabellen bei einem leeren Ergebnis. Lieber
  # laut scheitern (Hausregel fuer do/main*.R).
  if (nrow(meetings) == 0) stop("update_sales_meetings: keine Meeting records. Abbruch.")
  # delete_missing loescht hart. Schrumpft das Ergebnis auf unter die Haelfte des
  # Bestands, deutet das auf einen Fehler beim Laden hin (wie in
  # update_extern_event_classification()).
  bestand <- dplyr::tbl(con, I("processed.sales_meetings")) %>% dplyr::count() %>% dplyr::pull(n)
  if (as.numeric(bestand) > 0 && nrow(meetings) < 0.5 * as.numeric(bestand)) {
    stop(sprintf(paste0("update_sales_meetings: %d Records, Bestand ist %.0f. delete_missing ",
                        "wuerde mehr als die Haelfte loeschen. Abbruch."),
                 nrow(meetings), as.numeric(bestand)))
  }
  dubletten <- unique(meetings$meeting_key[duplicated(meetings$meeting_key)])
  if (length(dubletten) > 0) {
    stop("update_sales_meetings: ", length(dubletten), " meeting_keys doppelt, z.B. ",
         paste(utils::head(dubletten, 5), collapse = ", "), ". Abbruch.")
  }
  if (anyDuplicated(paste(leads$meeting_key, leads$lead_id))) {
    stop("update_sales_meetings: (meeting_key, lead_id) doppelt. Bug in build_sales_meetings().")
  }
  message(paste0("  ", nrow(meetings), " Meeting records (", sum(is.na(meetings$msgraph_event_id)),
                 " nur im CRM), ", nrow(leads), " Lead-Zeilen"))

  spalten_meetings <- SALES_MEETINGS_SPALTEN
  # Der Upsert verwirft Spalten, die die Tabelle nicht hat, ohne Fehler. Fehlt die
  # DDL einer neuen Spalte, soll der Lauf brechen statt sie still leer zu lassen.
  pruefe_zielspalten(DBI::dbListFields(con, DBI::Id(schema = "processed", table = "sales_meetings")),
                     spalten_meetings, "processed.sales_meetings")
  schreiben <- meetings[, spalten_meetings]
  for (spalte in c("msgraph_event_id", "organizer_person_id", "responsible_person_id")) {
    schreiben[[spalte]] <- bit64::as.integer64(schreiben[[spalte]])
  }
  kette <- data.frame(meeting_key = meetings$meeting_key, successor_key = meetings$successor_key,
                      chain_root_key = meetings$chain_root_key, stringsAsFactors = FALSE)

  pool::poolWithTransaction(con, function(conn) {
    Billomatics::postgres_upsert_data(
      conn, "processed", "sales_meetings",
      schreiben, match_cols = c("meeting_key"), delete_missing = TRUE)

    # Nachfolger und Ketten-ID erst jetzt: die IDs vergibt die Datenbank.
    temp <- paste0("temp_sales_meeting_chain_", format(Sys.time(), "%Y%m%d%H%M%S"), "_",
                   sample.int(999999, 1))
    DBI::dbWriteTable(conn, temp, kette, temporary = TRUE, overwrite = TRUE)
    DBI::dbExecute(conn, paste0("
      UPDATE processed.sales_meetings AS t
         SET successor_meeting_id = s.id,
             meeting_chain_id     = r.id
        FROM ", DBI::dbQuoteIdentifier(conn, temp), " AS x
        LEFT JOIN processed.sales_meetings AS s ON s.meeting_key = x.successor_key
        JOIN processed.sales_meetings AS r ON r.meeting_key = x.chain_root_key
       WHERE t.meeting_key = x.meeting_key
         AND (t.successor_meeting_id IS DISTINCT FROM s.id
              OR t.meeting_chain_id IS DISTINCT FROM r.id)"))
    DBI::dbRemoveTable(conn, temp)

    ids <- DBI::dbGetQuery(conn, "SELECT id, meeting_key FROM processed.sales_meetings")
    lead_rows <- data.frame(
      meeting_id = ids$id[match(leads$meeting_key, ids$meeting_key)],
      lead_id = bit64::as.integer64(leads$lead_id),
      lead_match_source = leads$lead_match_source,
      crm_lead_task_id = bit64::as.integer64(leads$crm_lead_task_id),
      crm_outcome = leads$crm_outcome,
      crm_outcome_conflict = leads$crm_outcome_conflict,
      meeting_tool = leads$meeting_tool,
      meeting_type = leads$meeting_type,
      is_external_tool = leads$is_external_tool,
      stringsAsFactors = FALSE)
    Billomatics::postgres_upsert_data(
      conn, "processed", "sales_meeting_leads",
      lead_rows, match_cols = c("meeting_id", "lead_id"), delete_missing = TRUE)
  })
  message("  fertig.")
  invisible(nrow(meetings))
}
