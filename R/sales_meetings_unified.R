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
#'
#' @description
#' **`unbekannt` gilt als stattgefunden, nicht als No-Show** (entschieden am
#' 05.09.2026, Asana `1218192760112154`). Ein CRM-Termin ohne dokumentierten
#' Ausgang ist ein stattgefundener Termin ohne weitere Doku; fehlende
#' Dokumentation ist kein Beleg fuer einen geplatzten Termin und geht nicht zu
#' Lasten des Vertrieblers. Gemessen an MSGraph-verifizierbaren Terminen sind
#' undokumentierte Termine sogar etwas seltener No-Shows (25,3 %) als der
#' Durchschnittstermin (29,0 %): ein geplatzter Termin erzeugt Nacharbeit und
#' damit Doku.
#'
#' Das bringt diese Funktion mit `assemble_crm_classification_rows()` in
#' Deckung, die dieselbe Zeile fuer `processed.msgraph_extern_event_classification`
#' schon immer als `is_no_show = FALSE, excluded = FALSE` gefuehrt hat. Vorher
#' trug dieselbe CRM-Zeile in den beiden Tabellen zwei verschiedene Lesarten.
#'
#' **Die Beobachtung geht dabei nicht verloren.** Dass der Ausgang nicht
#' dokumentiert ist, steht weiterhin in `meeting_status` und
#' `no_show_source = "crm_only"` auf der Zeile. Was wegfaellt, ist allein der
#' Ausschlussgrund `crm_unbekannt`, und der ist nach dieser Lesart keiner mehr.
#'
#' `storniert` bleibt ausgeschlossen: eine rechtzeitige Absage ist kein
#' No-Show, sondern ein nicht stattgefundener Termin (T1, Punkt C3).
#'
#' @param status character vector mit CRM-Meeting-Status (z.B. "no_show",
#'   "show_up", "storniert", "unbekannt").
#' @return list mit `is_no_show` (logical, `NA` nur bei unbekanntem Statuswert)
#'   und `excluded` (logical).
#' @keywords internal
crm_status_flags <- function(status) {
  is_no_show <- ifelse(status == "no_show", TRUE,
                       ifelse(status %in% c("show_up", "unbekannt"), FALSE, NA))
  excluded   <- status %in% "storniert"
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
#' Findet der Match keinen Kandidaten, greift die **Ersatzregel**: eine
#' MSGraph-Zeile mit `lead_id = NA` (kein Lead ableitbar, weder per Mapping noch
#' per Domain) desselben Reps am selben Tag, deren Beginn hoechstens
#' [ERSATZREGEL_MAX_MINUTEN] von `precise_time` abweicht, ist derselbe Termin.
#' Die Zeile uebernimmt den Lead des Tasks (`meeting_key = msgraph_<id>_<lead>`).
#' Die Paare eines Rep-Tags werden nach Abstand vergeben, das naechste zuerst,
#' bei Gleichstand die kleinere `crm_task_id`; das Ergebnis haengt nicht von der
#' Reihenfolge der Tasks ab. Ohne Treffer wird die CRM-Zeile netto-neu
#' geschrieben und nicht verworfen.
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
#' **Alt-Tenant und nicht abrufbares Online-Meeting.** Traegt die gematchte
#' MSGraph-Zeile den Grund `alt_tenant_join_url` oder
#' `online_meeting_nicht_abrufbar` (Microsoft gibt das Online-Meeting zum Link
#' nicht heraus: nicht gefunden, 403 der Zugriffsregel, Organisator unbekannt),
#' ist ihre Anwesenheit nicht messbar, und der CRM-Status entscheidet: `show_up`
#' und `unbekannt` gelten als stattgefunden, `no_show` als No-Show, der
#' Ausschluss faellt (`excluded = FALSE`, `exclusion_reason = NA`). `storniert`
#' bleibt als `crm_storniert` ausgeschlossen. Andere Lead-Zeilen desselben
#' Meetings bleiben unberuehrt.
#'
#' @param msgraph_meetings data.frame mit call_event_mapping_id, lead_id,
#'   event_date, event_start, contact_id (Rep), is_no_show, excluded,
#'   is_short_lived_event, is_responsible, original_created_at, event_id und
#'   optional organizer_contact_id (fehlt sie, gilt der Organisator als
#'   unbekannt) sowie optional join_url und join_url_checked_at (fehlt eine
#'   davon, greift der `unbekannt`-Override gar nicht und der Datenbestand
#'   bleibt wie ohne diese Regel).
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
  # Traegt das Meeting einen Teams-Beitrittslink, konnte ueberhaupt ein
  # Anwesenheitsbericht entstehen. Fehlt er, ist "kein Call gefunden" keine
  # Messung. Siehe den Override-Block weiter unten.
  #
  # `ms_ohne_link` sagt **nicht** "join_url ist NA", sondern "wir wissen, dass
  # es keinen Link gab". Vier Faelle laufen sonst zusammen:
  #   * kein Link              -> TRUE, hier gibt es nichts zu messen
  #   * leerer String als Link -> TRUE, ein "" ist kein Beitrittslink
  #   * keine Event-Zeile      -> FALSE, wir wissen es schlicht nicht, und aus
  #                               Nichtwissen darf keine Anwesenheitsaussage
  #                               werden. Erkennbar an `event_start`, das in
  #                               raw.msgraph_events NOT NULL ist: fehlt es,
  #                               hat der left_join nichts gefunden.
  #   * nie gegen Graph        -> FALSE, siehe `join_url_checked_at` unten.
  #     geprueft
  #
  # `join_url_checked_at` ist die Geprueft-Marke auf raw.msgraph_events: gesetzt
  # heisst "wir haben Graph nach dem Beitrittslink gefragt, und was in join_url
  # steht ist die Antwort" - auch wenn dort nichts steht. NULL heisst "nie
  # gefragt", und dann ist ein leeres join_url keine Aussage, sondern eine
  # Beobachtungsluecke.
  #
  # Vorher stand hier ein Stichtag (`updated_at >= JOIN_URL_ROLLOUT`). Der hat
  # nicht getragen: der Trigger `trigger_set_updated_at` feuert nur
  # `IF NEW IS DISTINCT FROM OLD`, und ein echter Nicht-Teams-Termin aendert sich
  # nie. Er behielt sein altes `updated_at` und blieb dauerhaft unentscheidbar,
  # obwohl der naechtliche Ingest ihn im 50-Tage-Fenster jede Nacht sah.
  # `updated_at` beantwortet "hat sich geaendert", nicht "wurde geprueft".
  #
  # Fehlt eine der beiden Spalten (Caller mit selbst gebautem data.frame),
  # bleibt alles FALSE. Der Fallback ist damit der neutrale: kein Override, kein
  # veraenderter Datenbestand, so wie es ohne diese Regel war.
  ms_ohne_link <- if (is.null(msgraph_meetings$join_url) ||
                      is.null(msgraph_meetings$join_url_checked_at)) {
    rep(FALSE, nrow(msgraph_meetings))
  } else {
    url <- as.character(msgraph_meetings$join_url)
    # event_start bleibt in der Bedingung, obwohl eine fehlende Event-Zeile auch
    # join_url_checked_at auf NA laesst: der Fall "left_join hat nichts
    # gefunden" ist ein anderer als "Zeile existiert, wurde aber nie geprueft",
    # und beide sollen hier sichtbar bleiben statt sich zu ueberdecken.
    link_bekannt <- !is.na(msgraph_meetings$event_start) &
      !is.na(msgraph_meetings$join_url_checked_at)
    link_bekannt & (is.na(url) | !nzchar(trimws(url)))
  }
  # Alt-Tenant-Meetings tragen einen Link, ihr Anwesenheitsbericht ist app-only
  # aber unerreichbar. Bei online_meeting_nicht_abrufbar gibt Microsoft das
  # Online-Meeting zum Link nicht heraus (nicht gefunden, 403, Organisator
  # unbekannt). Beide Gruende stammen aus `compute_observability_exclusions()`;
  # gemessen wird dort ebenso wenig wie ohne Link, und ein CRM-Beleg hebt den
  # Ausschluss auf (Override-Block unten). Einmal vor der Schleife, weil der
  # Override den Grund auf der Zeile loescht: ein zweiter CRM-Task auf dieselbe
  # Zeile saehe sonst ein anderes Meeting als der erste.
  aufhebbare_gruende <- c("alt_tenant_join_url", "online_meeting_nicht_abrufbar")
  ms_grund_nicht_messbar <- ms_reason %in% aufhebbare_gruende
  ms_nicht_messbar <- ms_ohne_link | ms_grund_nicht_messbar
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
  # Nur fuer Tiebreak und Ersatzregel (nicht im DB-Schema): Rep-Kontakt +
  # event_start je Zeile.
  base_rep   <- as.character(msgraph_meetings$contact_id)
  base_start <- msgraph_meetings$event_start

  netto_neu <- function(cm) {
    fl <- crm_status_flags(cm$meeting_status)
    data.frame(
      meeting_key = paste0("crm_", cm$crm_task_id), source = "crm_task",
      event_date = cm$event_date, contact_id = as.character(cm$contact_id),
      lead_id = as.character(cm$lead_id),
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
  }

  # Erster Durchgang: Abgleich Lead x Tag. Tasks ohne Kandidat sammelt `offen`
  # fuer die Ersatzregel. Erst wenn alle Tasks durch sind, steht fest, welche
  # Tasks um welchen Platzhalter konkurrieren; vorher gewann, wer zuerst geladen
  # wurde.
  # treffer[i] = Meeting-Zeile, die Task i ueberschreibt (NA = keine).
  new_rows <- list()
  treffer  <- rep(NA_integer_, nrow(crm_meetings))
  offen    <- integer(0)
  for (i in seq_len(nrow(crm_meetings))) {
    cm <- crm_meetings[i, ]
    cm_lead <- as.character(cm$lead_id)

    # Externes Tool (kein MSGraph-Pendant) und Task ohne Lead -> immer netto-neu.
    if (isTRUE(cm$is_external_tool)) { new_rows[[length(new_rows)+1]] <- netto_neu(cm); next }
    if (is.na(cm$lead_id))          { new_rows[[length(new_rows)+1]] <- netto_neu(cm); next }

    # Kandidaten gleicher (lead_id, event_date).
    cand <- which(!is.na(base$lead_id) & base$lead_id == cm_lead &
                    base$event_date == cm$event_date)
    if (length(cand) == 0) { offen <- c(offen, i); next }
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
    treffer[i] <- cand[1]
  }

  # Zweiter Durchgang: Ersatzregel fuer die Tasks ohne Kandidat. Ein
  # MSGraph-Meeting ohne ableitbaren Lead traegt lead_id = NA und kann per
  # (Lead x Tag) nie gefunden werden; ohne Ersatzregel zaehlte der Task ein
  # zweites Mal. Nur bis ERSATZREGEL_MAX_MINUTEN Abstand: gemessen 29.10.2026
  # widersprechen bis 15 Minuten 36 von 928 Paaren dem Task-Lead, darueber 377
  # von 409 (Spec 2026-10-29-ersatzregel-nur-rest).
  treffer[offen] <- ersatzregel_paare(crm_meetings[offen, , drop = FALSE],
                                      is.na(base$lead_id), base$event_date, base_rep, base_start)
  for (i in offen[is.na(treffer[offen])]) new_rows[[length(new_rows)+1]] <- netto_neu(crm_meetings[i, ])

  # In Task-Reihenfolge, damit mehrere Tasks auf derselben Zeile wie bisher
  # nacheinander wirken (siehe Storno-Regel unten).
  for (i in which(!is.na(treffer))) {
    cm <- crm_meetings[i, ]
    fl <- crm_status_flags(cm$meeting_status)
    j  <- treffer[i]

    # Ersatzregel-Treffer: die Zeile uebernimmt den Lead des Tasks. Nur sie
    # tragen hier noch lead_id = NA, ein Lead x Tag-Treffer hat immer einen Lead.
    if (is.na(base$lead_id[j])) {
      base$lead_id[j]     <- as.character(cm$lead_id)
      base$meeting_key[j] <- paste0("msgraph_", msgraph_meetings$call_event_mapping_id[j],
                                    "_", base$lead_id[j])
    }

    # eindeutiger (bzw. aufgeloester) Match -> Override. Nur ein am CRM-Task
    # dokumentierter Ausgang setzt is_no_show; storniert -> excluded.
    #
    # **"unbekannt" haengt am Teams-Beitrittslink** (ADR 0015, 07.09.2026).
    # Bis dahin liess es MSGraph immer unangetastet, mit der Begruendung, hier
    # laege eine echte Anwesenheitsmessung aus dem Kalender vor, und die faende
    # ein fehlender CRM-Kommentar nicht besser. Das Argument stimmt — aber nur,
    # wo es einen Anwesenheitsbericht geben konnte. Der setzt einen
    # Beitrittslink voraus (ADR 0004 in kpiR: 5.607 von 29.201 Terminen tragen
    # einen). Ohne Link ist "kein Call gefunden" dieselbe Beobachtungsgrenze wie
    # beim Alt-Tenant und keine Messung, und dann traegt der CRM-Termin ohne
    # dokumentierten Ausgang: stattgefunden ohne Doku.
    #
    # Wer die Bedingung auf Meetings **mit** Link ausweitet, ueberschreibt eine
    # Messung mit einer Auslegung. Genau davor stand hier die Warnung, und sie
    # gilt weiter.
    #
    # **Alt-Tenant ist der zweite Fall von "nicht messbar"** (30.09.2026, Asana
    # 1218788153926894). Der Link existiert, der Bericht dazu ist seit dem
    # 19.08.2026 app-only unerreichbar. Das Meeting steht deshalb als
    # `alt_tenant_join_url` ausgeschlossen, und ohne diesen Zweig loeschte der
    # Kalender-Match den CRM-Beleg: haette der Task kein Meeting gefunden, zaehlte
    # er als netto-neue crm_only-Zeile ohnehin als VC. Mit CRM-Task entscheidet
    # deshalb der CRM-Status, und der Ausschluss faellt.
    #
    # **Nicht abrufbares Online-Meeting ist der dritte Fall** (07.10.2026, Spec
    # 2026-10-07-online-meeting-nicht-abrufbar). Der Link ist aus dem eigenen
    # Tenant, Microsoft gibt das Meeting aber nicht heraus (nicht gefunden, 403
    # der Zugriffsregel, Organisator unbekannt). Gleiche Lage wie beim
    # Alt-Tenant, gleiche Regel. Nur diese beiden Gruende (`aufhebbare_gruende`):
    # Identitaetsgruende (duplikat_event, verschoben, ...) und die Zukunft bleiben
    # stehen, ein Beleg macht aus einem Duplikat keinen eigenen Termin.
    #
    # Aufgehoben wird nur, solange die Zeile den Grund **noch** traegt. Treffen
    # zwei CRM-Tasks dieselbe Zeile und der erste ist storniert, steht dort schon
    # crm_storniert, und ein spaeterer show_up darf die Absage nicht wieder
    # aufheben. Vor diesem Zweig blieb der Storno in jeder Reihenfolge stehen.
    if (cm$meeting_status %in% c("no_show", "show_up")) base$is_no_show[j] <- fl$is_no_show
    if (cm$meeting_status == "unbekannt" && ms_nicht_messbar[j]) {
      base$is_no_show[j] <- FALSE
    }
    if (base$exclusion_reason[j] %in% aufhebbare_gruende &&
        cm$meeting_status %in% c("no_show", "show_up", "unbekannt")) {
      base$excluded[j] <- FALSE
      base$exclusion_reason[j] <- NA_character_
    }
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

#' Hoechster Abstand der Ersatzregel in Minuten
#'
#' Ein CRM-VC-Termin wird einem Meeting ohne ableitbaren Lead nur zugeordnet,
#' wenn `precise_time` und Meetingbeginn hoechstens so weit auseinanderliegen.
#' Gemessen 29.10.2026 ueber 12 Monate: bis 15 Minuten widersprechen 36 von 928
#' Paaren dem Task-Lead, von 16 bis 30 Minuten schon 68 von 76.
#'
#' @export
ERSATZREGEL_MAX_MINUTEN <- 15

#' Ersatzregel: CRM-Tasks auf Meetings ohne Lead verteilen
#'
#' Rein (kein DB-Zugriff). Ein Paar entsteht aus einem Task und einer Zeile ohne
#' Lead desselben Reps am selben Tag, deren Beginn hoechstens
#' [ERSATZREGEL_MAX_MINUTEN] von `precise_time` abweicht. Vergeben wird nach
#' Abstand, das naechste Paar zuerst, bei Gleichstand die kleinere
#' `crm_task_id`; jeder Task und jede Zeile hoechstens einmal. Damit haengt das
#' Ergebnis nicht von der Reihenfolge der Tasks ab.
#'
#' Hat ein Task zwei freie Zeilen im selben Abstand, bekommt er keine: zwei
#' Meetings desselben Reps zur selben Zeit sind nicht unterscheidbar, und der
#' Task bleibt besser netto-neu als falsch zugeordnet.
#'
#' @param tasks data.frame mit crm_task_id, event_date, precise_time, contact_id.
#' @param ohne_lead logical je Meeting-Zeile, TRUE = kein Lead.
#' @param event_date,rep_kontakt,start Datum, Rep-Kontakt (character) und Beginn je
#'   Meeting-Zeile, parallel zu `ohne_lead`.
#' @return integer je Zeile in `tasks`: die zugeordnete Meeting-Zeile, NA = keine.
#' @keywords internal
# ---- start ---- #
ersatzregel_paare <- function(tasks, ohne_lead, event_date, rep_kontakt, start) {
  zeile <- rep(NA_integer_, nrow(tasks))
  max_sek <- ERSATZREGEL_MAX_MINUTEN * 60
  kand <- list()
  for (t in seq_len(nrow(tasks))) {
    if (is.na(tasks$precise_time[t])) next
    p <- which(ohne_lead & event_date == tasks$event_date[t] &
                 rep_kontakt == as.character(tasks$contact_id[t]) & !is.na(start))
    d <- abs(as.numeric(start[p]) - as.numeric(tasks$precise_time[t]))
    if (any(d <= max_sek)) kand[[length(kand)+1]] <- data.frame(t = t, j = p[d <= max_sek], d = d[d <= max_sek])
  }
  if (length(kand) == 0) return(zeile)
  kand <- do.call(rbind, kand)
  # crm_task_id kommt aus der DB als integer64; order() rechnet darauf falsch.
  task_id <- as.numeric(as.character(tasks$crm_task_id))
  kand <- kand[order(kand$d, task_id[kand$t], kand$j), ]

  task_fertig <- rep(FALSE, nrow(tasks))
  zeile_belegt <- rep(FALSE, length(ohne_lead))
  for (k in seq_len(nrow(kand))) {
    t <- kand$t[k]; j <- kand$j[k]
    if (task_fertig[t] || zeile_belegt[j]) next
    task_fertig[t] <- TRUE
    gleich_nah <- kand$t == t & kand$d == kand$d[k] & kand$j != j & !zeile_belegt[kand$j]
    if (any(gleich_nah)) next
    zeile_belegt[j] <- TRUE
    zeile[t] <- j
  }
  zeile
}

#' Eindeutige Domain-Leads aus den CRM-Mailadressen
#'
#' Je Mail-Domain der Lead, dem sie im CRM gehoert, aber nur wenn es genau einer
#' ist. Eine Domain mehrerer Leads sagt nicht, welcher gemeint ist (Spec
#' 2026-10-28-domain-fallback-lead, Q9 zurueckgestellt). Freemail-Domains
#' ([FREEMAIL_DOMAINS]) und geloeschte Adressen zaehlen nicht.
#'
#' @param mails data.frame mit lead_id, mail_address_name, is_deleted
#'   (Spalten aus `raw.crm_lead_mail_address`).
#' @return data.frame mit domain (lower case) und lead_id (character), eine
#'   Zeile je Domain.
#' @export
# ---- start ---- #
domain_leads_aus_crm_mails <- function(mails) {
  aktiv <- mails[!(mails$is_deleted %in% TRUE) & grepl("@", mails$mail_address_name), , drop = FALSE]
  paare <- unique(data.frame(
    domain  = email_domain(aktiv$mail_address_name),
    lead_id = as.character(aktiv$lead_id),
    stringsAsFactors = FALSE))
  paare <- paare[nzchar(paare$domain) & !(paare$domain %in% FREEMAIL_DOMAINS), , drop = FALSE]
  n_leads <- table(paare$domain)
  eindeutig <- paare[paare$domain %in% names(n_leads)[n_leads == 1], , drop = FALSE]
  rownames(eindeutig) <- NULL
  eindeutig
}

#' Leads je MSGraph-Meeting: Mapping, sonst eindeutige Domain
#'
#' Rein (kein DB-Zugriff). Je Meeting jeder Lead, den
#' [meeting_leads_aus_teilnehmern()] ueber ein Mail-Mapping liefert. Hat kein
#' externer Teilnehmer ein Mapping, der Domain-matched lead: je externer
#' Teilnehmer-Domain der Lead, dem sie im CRM eindeutig gehoert
#' ([domain_leads_aus_crm_mails()]). Mehrere Leads ergeben eine Zeile je Lead.
#'
#' **Laeuft vor [assemble_unified_meetings()]** (Spec
#' 2026-10-29-ersatzregel-nur-rest, Q17). So findet ein CRM-Task sein Meeting
#' ueber Lead x Tag, und die Ersatzregel bleibt fuer Meetings ohne ableitbaren
#' Lead. Bis dahin lief der Domain-Fallback danach und liess Zeilen aus, die die
#' Ersatzregel schon einem Task gegeben hatte.
#'
#' **Jedes Mail-Mapping hat Vorrang**, auch ein nicht-primaeres: hat irgendein
#' externer Teilnehmer eine Zeile in `mapping.crm_lead_msgraph_contact`, greift
#' die Domain fuer das ganze Meeting nicht. So ueberdeckt sie nie einen Lead,
#' den das Mapping liefern kann.
#'
#' @param participants data.frame mit call_event_mapping_id, contact_id, email
#'   (Nicht-Organisator-Teilnehmer; interne und synthetische filtert die Funktion).
#' @param mappings data.frame mit msgraph_contact_id, crm_lead_id.
#' @param crm_mails data.frame mit lead_id, mail_address_name, is_deleted
#'   (Spalten aus `raw.crm_lead_mail_address`).
#' @return data.frame mit call_event_mapping_id, lead_id (character) und quelle
#'   (`"mapping"` oder `"domain"`); eine Zeile je Meeting und Lead.
#' @export
# ---- start ---- #
meeting_leads_ableiten <- function(participants, mappings, crm_mails) {
  per_mapping <- meeting_leads_aus_teilnehmern(participants, mappings)
  per_mapping$lead_id <- as.character(per_mapping$lead_id)
  per_mapping$quelle  <- rep("mapping", nrow(per_mapping))

  extern <- participants[!is_internal_email(participants$email) &
                           !is_synthetic_email(participants$email), , drop = FALSE]
  gemappt <- unique(extern$call_event_mapping_id[
    as.character(extern$contact_id) %in% as.character(mappings$msgraph_contact_id)])
  kandidaten <- extern[!(extern$call_event_mapping_id %in% gemappt), , drop = FALSE]
  kandidaten$domain <- email_domain(kandidaten$email)
  kandidaten <- kandidaten[!(kandidaten$domain %in% FREEMAIL_DOMAINS), , drop = FALSE]

  per_domain <- merge(kandidaten[, c("call_event_mapping_id", "domain")],
                      domain_leads_aus_crm_mails(crm_mails), by = "domain")
  per_domain <- unique(per_domain[, c("call_event_mapping_id", "lead_id")])
  per_domain$lead_id <- as.character(per_domain$lead_id)
  per_domain$quelle  <- rep("domain", nrow(per_domain))

  ergebnis <- rbind(per_mapping, per_domain)
  rownames(ergebnis) <- NULL
  ergebnis
}

#' Leads je MSGraph-Meeting aus den externen Teilnehmern
#'
#' Rein (kein DB-Zugriff). Ein Meeting bekommt jeden Lead, auf den einer seiner
#' externen, nicht-synthetischen Teilnehmer in `mapping.crm_lead_msgraph_contact`
#' gemappt ist.
#'
#' **Jedes Mapping, nicht nur das primaere** (Asana 1219978400790427,
#' 29.10.2026). `is_primary_crm` markiert je Lead nur die Adresse des hoechsten
#' Typs (office vor office_hq vor other/private vor from_description); eine
#' nachrangige Adresse ist trotzdem eine Adresse dieses Leads. Gemessen ueber 12
#' Monate: ohne den Filter bekommen 1.191 VCs neu einen Lead, nur 21 VCs bekommen
#' zwei. Mehrere Leads ergeben eine Zeile je Lead.
#'
#' Gemeinsame Quelle fuer [update_sales_meetings_unified()] und
#' [update_crm_task_meeting_classification()], damit Termintabelle und
#' CRM-Abgleich denselben Lead sehen.
#'
#' @param participants data.frame mit call_event_mapping_id, contact_id, email
#'   (Nicht-Organisator-Teilnehmer; interne und synthetische filtert die Funktion).
#' @param mappings data.frame mit msgraph_contact_id, crm_lead_id.
#' @return data.frame mit call_event_mapping_id, lead_id; eine Zeile je Meeting
#'   und Lead.
#' @keywords internal
# ---- start ---- #
meeting_leads_aus_teilnehmern <- function(participants, mappings) {
  extern <- participants[!is_internal_email(participants$email) &
                           !is_synthetic_email(participants$email),
                         c("call_event_mapping_id", "contact_id"), drop = FALSE]
  ml <- merge(extern, mappings, by.x = "contact_id", by.y = "msgraph_contact_id")
  unique(data.frame(
    call_event_mapping_id = ml$call_event_mapping_id,
    lead_id               = ml$crm_lead_id,
    stringsAsFactors      = FALSE))
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
        dplyr::select(id, event_start, join_url, join_url_checked_at),
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

  message("  leite externe Leads je Meeting ab (extern-only, alle Mail-Mappings) ...")
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

  # Externe Leads je Meeting, aus JEDEM Mail-Mapping (Begruendung an
  # meeting_leads_aus_teilnehmern), sonst ueber die eindeutige Domain. Beides vor
  # dem CRM-Abgleich, damit die Ersatzregel nur den Rest sieht. Das Mapping
  # allein nutzt auch der CRM-Abgleich in update_crm_task_meeting_classification().
  alle_mappings <- dplyr::tbl(con, I("mapping.crm_lead_msgraph_contact")) %>%
    dplyr::select(msgraph_contact_id, crm_lead_id) %>%
    dplyr::collect()
  crm_mails <- dplyr::tbl(con, I("raw.crm_lead_mail_address")) %>%
    dplyr::select(lead_id, mail_address_name, is_deleted) %>%
    dplyr::collect()
  meeting_lead <- meeting_leads_ableiten(participants, alle_mappings, crm_mails)
  # character statt integer64: setdiff() rechnet auf integer64 falsch.
  ml_id <- as.character(meeting_lead$call_event_mapping_id)
  ohne_mapping <- setdiff(as.character(has_ext), ml_id[meeting_lead$quelle == "mapping"])
  message(paste0("  ", length(unique(ml_id[meeting_lead$quelle == "domain"])), " von ",
                 length(ohne_mapping), " Meetings ohne Mapping per Domain zugeordnet"))

  # internal-only Meetings raus, dann per (Meeting x externer Lead) expandieren.
  # all.x = TRUE -> Meetings mit externem Teilnehmer aber ohne ableitbaren Lead
  # behalten EINE Zeile mit lead_id = NA (Platzhalter).
  msgraph_meetings <- msgraph_meetings[msgraph_meetings$call_event_mapping_id %in% has_ext, , drop = FALSE]
  msgraph_meetings <- merge(msgraph_meetings, meeting_lead[, c("call_event_mapping_id", "lead_id")],
                            by = "call_event_mapping_id", all.x = TRUE)

  message("  leite CRM-VC-Termine aus Rohdaten ab ...")
  tasks <- dplyr::tbl(con, I("raw.crm_lead_tasks")) %>%
    dplyr::filter(is_deleted == FALSE) %>%
    dplyr::select(id, crm_task_id, lead_id, user_id, assigned_to_user_id,
                  precise_time, task_created_at, task_name, task_badge) %>%
    dplyr::collect()
  comments <- dplyr::tbl(con, I("raw.crm_lead_task_comments")) %>%
    dplyr::filter(is_deleted == FALSE) %>%
    dplyr::select(task_id, comment_name) %>%
    dplyr::collect()
  ruc <- resolve_crm_user_contact(con)

  stop_if_badge_unbrauchbar(tasks$task_badge)
  vc <- tasks[is_crm_vc_meeting(tasks$task_name, tasks$task_badge), , drop = FALSE]
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
