# MSGraph Booking Appointments Ingest — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Booking-Appointments aus Microsoft Bookings (4 BookingBusinesses im Tenant) als reguläre Events in `raw.msgraph_events` schreiben, sodass Customers in `raw.msgraph_event_participants` landen und das bestehende Call-Event-Mapping greift.

**Architecture:** Eine neue Orchestrator-Funktion `msgraph_update_booking_appointments()` plus 3 Helper in `R/msgraph_events.R`. Appointments werden in das bestehende Event-Schema gemappt; die finalen Schreibvorgänge laufen über die bereits vorhandenen Funktionen `update_events()`, `update_contacts_from_events()`, `update_event_participants()`. Match zwischen Appointment und bestehender Event-Row über die Teams-`meeting_id`.

**Tech Stack:** R, dplyr, tidyr, httr, lubridate, MSGraph v1.0 API. **Keine Tests** (Codebase nutzt keine), Verifikation per interaktivem R-Skript.

**Spec:** [`docs/superpowers/specs/2026-05-19-msgraph-booking-appointments-design.md`](../specs/2026-05-19-msgraph-booking-appointments-design.md)

**Issue / Task:** [PubPackR/dbconnectorR#14](https://github.com/PubPackR/dbconnectorR/issues/14) · [Asana](https://app.asana.com/1/734700742714256/project/1211291490559148/task/1214938752023241)

**Anmerkungen vor dem Start**
- Vor jedem Lauf der Verify-Snippets sicherstellen, dass `con`, `keys` und `access_token` im REPL gesetzt sind (siehe globale CLAUDE.md, Abschnitt "Database access").
- Alle Helper sind `@keywords internal` — sie werden nur vom Orchestrator gerufen, nicht exportiert.
- Funktionen werden ans Ende von [`R/msgraph_events.R`](../../../R/msgraph_events.R) angehängt, **nach** `retrieve_calendar_events()` (aktuelle letzte Funktion).

---

### Task 0: Hardcoded-Debug-Filter aus `msgraph_update_events()` entfernen

Vor allem Neubau einmal den Debug-Filter rausnehmen, der aktuell jeden Lauf auf den einen User reduziert (sonst überschreibt der nächste Production-Run die Events nur dieses einen Users).

**Files:**
- Modify: `R/msgraph_events.R:20-28`

- [ ] **Step 1: Aktuellen Block lesen**

Run: `Read R/msgraph_events.R offset=20 limit=10`
Erwartung: Zeilen 20-28 enthalten den Filter mit der hartkodierten msgraph_user_id.

- [ ] **Step 2: Filter entfernen**

Ersetze in `R/msgraph_events.R:22-28`:

```r
  all_users <- dplyr::tbl(con, I("raw.msgraph_users")) %>%
    dplyr::filter(is_internal & !is_deleted)
  if (!is.null(user_id)) {
    all_users <- all_users %>% dplyr::filter(id %in% user_id)
  }
  all_users <- dplyr::collect(all_users %>% dplyr::mutate(id = msgraph_user_id)) %>% 
    filter(msgraph_user_id == "3332258a-3784-4568-9cfb-6325a62de30e")
```

durch:

```r
  all_users <- dplyr::tbl(con, I("raw.msgraph_users")) %>%
    dplyr::filter(is_internal & !is_deleted)
  if (!is.null(user_id)) {
    all_users <- all_users %>% dplyr::filter(id %in% user_id)
  }
  all_users <- dplyr::collect(all_users %>% dplyr::mutate(id = msgraph_user_id))
```

- [ ] **Step 3: Verifikation**

Run im REPL:

```r
devtools::load_all()
formals(msgraph_update_events)
```

Erwartung: `function(con, access_token, startDate, user_id = NULL)`. Keine Fehler beim load_all.

- [ ] **Step 4: Commit**

```bash
git add R/msgraph_events.R
git commit -m "fix(msgraph): remove leftover debug filter in msgraph_update_events"
```

---

### Task 1: Helper `retrieve_booking_appointments()` mit Paging

**Files:**
- Modify: `R/msgraph_events.R` (append am Ende)

- [ ] **Step 1: Funktion ans Ende von `R/msgraph_events.R` anhängen**

```r
#' Retrieve Booking Appointments for a Business
#'
#' Fetches all appointments of a single Microsoft Bookings business from a given
#' start date until today + 365 days, following @odata.nextLink pagination.
#'
#' @param access_token MSGraph API access token.
#' @param biz_id Booking business identifier (e.g. "Studyflix@studyf.onmicrosoft.com").
#' @param startDate Date object — earliest appointment start date to include.
#' @return List of raw appointment objects from the API.
#' @keywords internal
retrieve_booking_appointments <- function(access_token, biz_id, startDate) {
  # ---- start ---- #
  endDate <- lubridate::today() + 365

  url <- paste0(
    "https://graph.microsoft.com/v1.0/solutions/bookingBusinesses/",
    utils::URLencode(biz_id, reserved = TRUE),
    "/appointments",
    "?$filter=start/dateTime ge '", startDate, "T00:00:00Z'",
    " and start/dateTime le '", endDate, "T23:59:59Z'",
    "&$top=400"
  )

  all_appointments <- list()
  repeat {
    page <- fetch_with_retry(url, access_token, max_retries = 3, delay = 2)
    if (!is.null(page$value)) {
      all_appointments <- c(all_appointments, page$value)
    }
    if (!is.null(page$`@odata.nextLink`)) {
      url <- page$`@odata.nextLink`
    } else {
      break
    }
  }

  return(all_appointments)
}
```

- [ ] **Step 2: Verifikation im REPL**

```r
devtools::load_all()
appts <- dbconnectorR:::retrieve_booking_appointments(
  access_token,
  biz_id   = "StudyflixBeratungsgesprch@studyf.onmicrosoft.com",
  startDate = lubridate::today() - 30
)
length(appts)
names(appts[[1]])
```

Erwartung: `length(appts) > 0`, Namen enthalten `id`, `serviceName`, `customers`, `staffMemberIds`, `joinWebUrl`, `start`, `end`.

- [ ] **Step 3: Commit**

```bash
git add R/msgraph_events.R
git commit -m "feat(msgraph): add retrieve_booking_appointments helper"
```

---

### Task 2: Helper `retrieve_booking_staff()`

**Files:**
- Modify: `R/msgraph_events.R` (append)

- [ ] **Step 1: Funktion anhängen**

```r
#' Retrieve Staff Members of a Booking Business
#'
#' Fetches the staffMembers collection of a Microsoft Bookings business.
#'
#' @param access_token MSGraph API access token.
#' @param biz_id Booking business identifier.
#' @return Data frame with columns `biz_id`, `staff_id`, `staff_email`, `staff_name`.
#' @keywords internal
retrieve_booking_staff <- function(access_token, biz_id) {
  # ---- start ---- #
  url <- paste0(
    "https://graph.microsoft.com/v1.0/solutions/bookingBusinesses/",
    utils::URLencode(biz_id, reserved = TRUE), "/staffMembers"
  )
  resp <- fetch_with_retry(url, access_token, max_retries = 3, delay = 2)
  if (is.null(resp$value) || length(resp$value) == 0) {
    return(data.frame(
      biz_id      = character(0),
      staff_id    = character(0),
      staff_email = character(0),
      staff_name  = character(0),
      stringsAsFactors = FALSE
    ))
  }
  data.frame(
    biz_id      = biz_id,
    staff_id    = vapply(resp$value, function(s) s$id %||% NA_character_, character(1)),
    staff_email = tolower(vapply(resp$value, function(s) s$emailAddress %||% NA_character_, character(1))),
    staff_name  = vapply(resp$value, function(s) s$displayName %||% NA_character_, character(1)),
    stringsAsFactors = FALSE
  )
}
```

- [ ] **Step 2: Verifikation**

```r
devtools::load_all()
staff <- dbconnectorR:::retrieve_booking_staff(
  access_token,
  "StudyflixBeratungsgesprch@studyf.onmicrosoft.com"
)
head(staff)
```

Erwartung: Data frame mit Zeilen pro Staff, alle 4 Spalten gefüllt.

- [ ] **Step 3: Commit**

```bash
git add R/msgraph_events.R
git commit -m "feat(msgraph): add retrieve_booking_staff helper"
```

---

### Task 3: Helper `build_staff_lookup()`

**Files:**
- Modify: `R/msgraph_events.R` (append)

- [ ] **Step 1: Funktion anhängen**

```r
#' Build a Lookup From Booking Staff to Internal User IDs
#'
#' For all given booking businesses, fetches their staff members and joins them
#' against `raw.msgraph_users` by email to resolve each `staff_id` to the
#' canonical `msgraph_user_id` of an internal user. Booking-businesses whose
#' Staff lists overlap (a user is staff in multiple businesses) are kept as
#' separate rows.
#'
#' @param con A PostgreSQL database connection object.
#' @param access_token MSGraph API access token.
#' @param biz_ids Character vector of booking business identifiers.
#' @return Data frame with columns `biz_id`, `staff_id`, `staff_email`, `staff_name`, `msgraph_user_id`.
#' @keywords internal
build_staff_lookup <- function(con, access_token, biz_ids) {
  # ---- start ---- #
  staff_all <- do.call(
    rbind,
    lapply(biz_ids, function(b) retrieve_booking_staff(access_token, b))
  )

  if (nrow(staff_all) == 0) {
    return(cbind(staff_all, msgraph_user_id = character(0)))
  }

  users <- dplyr::tbl(con, I("raw.msgraph_users")) %>%
    dplyr::select(msgraph_user_id, email) %>%
    dplyr::collect() %>%
    dplyr::mutate(email = tolower(email))

  dplyr::left_join(staff_all, users, by = c("staff_email" = "email"))
}
```

- [ ] **Step 2: Verifikation**

```r
devtools::load_all()
biz_ids <- c(
  "Studyflix@studyf.onmicrosoft.com",
  "AustauschStudyflix@studyf.onmicrosoft.com",
  "TestBuchungsseite@studyf.onmicrosoft.com",
  "StudyflixBeratungsgesprch@studyf.onmicrosoft.com"
)
lookup <- dbconnectorR:::build_staff_lookup(con, access_token, biz_ids)
nrow(lookup)
sum(is.na(lookup$msgraph_user_id))
head(lookup)
```

Erwartung: `nrow(lookup) > 0`. Anzahl `NA` in `msgraph_user_id` ist der Anteil Staff ohne Match in `raw.msgraph_users` (sollte klein sein; bei vielen NAs Patterns in `INTERNAL_EMAIL_PATTERNS` checken oder neue User syncen).

- [ ] **Step 3: Commit**

```bash
git add R/msgraph_events.R
git commit -m "feat(msgraph): add build_staff_lookup helper"
```

---

### Task 4: Helper `appointments_to_event_dataframes()` — Kern-Konvertierung

Dieser Helper ist der inhaltliche Schwerpunkt. Er bekommt Roh-Appointments + Staff-Lookup + bestehende Events und liefert zwei Dataframes, die exakt dieselbe Struktur haben wie `all_calendar_events_` und `msgraph_event_participants` in `msgraph_update_events()` (sodass `update_events()`, `update_contacts_from_events()`, `update_event_participants()` die Daten direkt schlucken).

**Files:**
- Modify: `R/msgraph_events.R` (append)

- [ ] **Step 1: Funktion anhängen**

```r
#' Convert Booking Appointments to Event + Participant Data Frames
#'
#' Maps raw booking appointments to the same dataframe shape that
#' `msgraph_update_events()` produces, so that the downstream
#' `update_events()` / `update_contacts_from_events()` /
#' `update_event_participants()` functions can be reused as-is.
#'
#' Matching of an appointment to an existing calendar-event row happens via the
#' Teams meeting_id extracted from `joinWebUrl`. If a match is found the
#' existing `msgraph_ical_uid` + `event_start` are reused and the appointment
#' only enriches the participants. If no match exists a new event row is
#' created with `msgraph_ical_uid = paste0("booking:", appointment$id)`.
#'
#' Customers without an email address get a synthetic address
#' `<bookingId>-<idx>@external.guest`, analog to the guest-handling in
#' `msgraph_calls.R`.
#'
#' @param appointments List of raw appointment objects (from `retrieve_booking_appointments`).
#' @param staff_lookup Data frame produced by `build_staff_lookup()`.
#' @param existing_events Data frame from `dplyr::collect(dplyr::tbl(con, I("raw.msgraph_events")))`.
#' @return Named list with two data frames: `events` (shape of `all_calendar_events_`) and `participants` (shape of `msgraph_event_participants`).
#' @keywords internal
appointments_to_event_dataframes <- function(appointments, staff_lookup, existing_events) {
  # ---- start ---- #
  if (length(appointments) == 0) {
    return(list(events = NULL, participants = NULL))
  }

  # ---- one-row-per-appointment frame with everything we need ----
  appt_df <- do.call(rbind, lapply(seq_along(appointments), function(i) {
    a <- appointments[[i]]
    join_url <- a$joinWebUrl %||% a$onlineMeetingUrl %||% NA_character_
    meeting_id <- if (!is.na(join_url)) extract_meeting_id(join_url) else NA_character_
    data.frame(
      appt_idx       = i,
      appt_id        = a$id %||% NA_character_,
      service_name   = a$serviceName %||% NA_character_,
      start_dt       = a$start$dateTime %||% NA_character_,
      end_dt         = a$end$dateTime %||% NA_character_,
      join_url       = join_url,
      meeting_id     = meeting_id,
      stringsAsFactors = FALSE
    )
  }))

  # ---- match against existing events via meeting_id ----
  existing_lookup <- existing_events %>%
    dplyr::filter(!is.na(meeting_id) & meeting_id != "") %>%
    dplyr::distinct(meeting_id, msgraph_ical_uid, event_start)

  appt_df <- appt_df %>%
    dplyr::left_join(existing_lookup, by = "meeting_id") %>%
    dplyr::mutate(
      msgraph_ical_uid = ifelse(
        is.na(msgraph_ical_uid),
        paste0("booking:", appt_id),
        msgraph_ical_uid
      ),
      event_start_dt = ifelse(
        is.na(event_start),
        start_dt,
        as.character(event_start)
      )
    )

  # ---- events dataframe (shape of all_calendar_events_) ----
  events_df <- data.frame(
    iCalUId            = appt_df$msgraph_ical_uid,
    createdDateTime    = appt_df$start_dt,   # Bookings has no createdDateTime; reuse start
    lastModifiedDateTime = appt_df$start_dt,
    subject            = appt_df$service_name,  # fallback subject; existing event subjects are preserved in update_events via slice_max on updated_at
    type               = "singleInstance",
    isOnlineMeeting    = !is.na(appt_df$meeting_id),
    onlineMeeting_joinUrl = appt_df$join_url,
    start_dateTime     = appt_df$event_start_dt,
    end_dateTime       = appt_df$end_dt,
    isCancelled        = FALSE,
    is_canceled        = FALSE,
    isOrganizer        = TRUE,
    user_id            = NA_character_,        # filled below
    stringsAsFactors = FALSE
  )

  # primary staff -> user_id
  primary_user <- vapply(seq_along(appointments), function(i) {
    a <- appointments[[i]]
    sids <- a$staffMemberIds %||% character(0)
    if (length(sids) == 0) return(NA_character_)
    hit <- staff_lookup %>% dplyr::filter(staff_id == sids[[1]])
    if (nrow(hit) == 0 || is.na(hit$msgraph_user_id[1])) NA_character_ else hit$msgraph_user_id[1]
  }, character(1))
  events_df$user_id <- primary_user

  # ---- participants dataframe (shape of msgraph_event_participants) ----
  parts <- do.call(rbind, lapply(seq_along(appointments), function(i) {
    a <- appointments[[i]]
    ical <- appt_df$msgraph_ical_uid[i]
    ev_start <- appt_df$event_start_dt[i]

    # Customers
    custs <- a$customers %||% list()
    cust_rows <- if (length(custs) == 0) NULL else do.call(rbind, lapply(seq_along(custs), function(j) {
      cust <- custs[[j]]
      email_raw <- cust$emailAddress %||% NA_character_
      email <- if (is.null(email_raw) || is.na(email_raw) || email_raw == "") {
        paste0(a$id, "-", j, "@external.guest")
      } else {
        tolower(email_raw)
      }
      data.frame(
        event_id                      = ical,
        event_start                   = ev_start,
        attendees_emailAddress_name   = cust$name %||% NA_character_,
        attendees_emailAddress_address = email,
        is_organizer                  = FALSE,
        stringsAsFactors = FALSE
      )
    }))

    # Staff (all as organizers, analog to msgraph_event_organizers in msgraph_update_events)
    sids <- a$staffMemberIds %||% character(0)
    staff_rows <- if (length(sids) == 0) NULL else do.call(rbind, lapply(sids, function(sid) {
      hit <- staff_lookup %>% dplyr::filter(staff_id == sid)
      if (nrow(hit) == 0 || is.na(hit$staff_email[1])) return(NULL)
      data.frame(
        event_id                      = ical,
        event_start                   = ev_start,
        attendees_emailAddress_name   = hit$staff_name[1] %||% NA_character_,
        attendees_emailAddress_address = tolower(hit$staff_email[1]),
        is_organizer                  = TRUE,
        stringsAsFactors = FALSE
      )
    }))

    rbind(cust_rows, staff_rows)
  }))

  # Convert event_start to POSIXct for downstream consistency
  parts$event_start <- lubridate::ymd_hms(parts$event_start)
  events_df$start_dateTime <- as.character(events_df$start_dateTime)

  list(events = events_df, participants = parts)
}
```

- [ ] **Step 2: Sanity-Check im REPL**

```r
devtools::load_all()
existing_events <- dplyr::tbl(con, I("raw.msgraph_events")) %>% dplyr::collect()
biz_ids <- c(
  "Studyflix@studyf.onmicrosoft.com",
  "AustauschStudyflix@studyf.onmicrosoft.com",
  "TestBuchungsseite@studyf.onmicrosoft.com",
  "StudyflixBeratungsgesprch@studyf.onmicrosoft.com"
)
lookup <- dbconnectorR:::build_staff_lookup(con, access_token, biz_ids)
appts <- dbconnectorR:::retrieve_booking_appointments(
  access_token,
  "StudyflixBeratungsgesprch@studyf.onmicrosoft.com",
  lubridate::today() - 30
)
out <- dbconnectorR:::appointments_to_event_dataframes(appts, lookup, existing_events)

nrow(out$events)
nrow(out$participants)
table(out$participants$is_organizer)              # TRUE = Staff, FALSE = Customer
sum(grepl("@external\\.guest$", out$participants$attendees_emailAddress_address))  # synthetic
sum(startsWith(out$events$iCalUId, "booking:"))   # appointments ohne Match
```

Erwartung:
- `nrow(out$events)` ≈ `length(appts)`
- Mehr `FALSE` (Customers) als `TRUE` (Staff) in `is_organizer`
- `synthetic > 0` möglich, abhängig von wie viele Customers ohne Email
- `startsWith(... "booking:")` zeigt, wieviele Appointments noch keine bestehende Event-Row hatten

- [ ] **Step 3: Commit**

```bash
git add R/msgraph_events.R
git commit -m "feat(msgraph): add appointments_to_event_dataframes converter"
```

---

### Task 5: Orchestrator `msgraph_update_booking_appointments()`

**Files:**
- Modify: `R/msgraph_events.R` (append)

- [ ] **Step 1: Funktion anhängen**

```r
#' Retrieve and Update Booking Appointments From MSGraph
#'
#' Retrieves Microsoft Bookings appointments for all booking businesses in the
#' tenant, converts them to the same shape as regular calendar events, and
#' writes them through the existing `update_events()`,
#' `update_contacts_from_events()`, and `update_event_participants()` pipeline.
#'
#' Externe Kunden (`customers` in der Bookings-API) erscheinen damit in
#' `raw.msgraph_event_participants` — was über die `calendarView`-API nicht
#' möglich ist, weil Kunden dort nicht im `attendees`-Feld stehen.
#'
#' Match zu bestehenden Calendar-Events erfolgt über die Teams-`meeting_id`;
#' wenn kein Match → neue Event-Row mit `msgraph_ical_uid = "booking:<id>"`.
#'
#' @param con A PostgreSQL database connection object.
#' @param access_token MSGraph API access token.
#' @param startDate Date from which to retrieve appointments.
#'
#' @return No return value. Updates database tables.
#' @export
#'
#' @examples
#' msgraph_update_booking_appointments(con, access_token, startDate)
msgraph_update_booking_appointments <- function(con, access_token, startDate) {
  # ---- start ---- #

  # 1. List businesses
  biz_resp <- fetch_with_retry(
    "https://graph.microsoft.com/v1.0/solutions/bookingBusinesses",
    access_token, max_retries = 3, delay = 2
  )
  biz_ids <- vapply(biz_resp$value %||% list(), function(b) b$id, character(1))
  if (length(biz_ids) == 0) {
    print("No booking businesses accessible.")
    return(invisible(NULL))
  }
  print(paste0("Booking businesses: ", paste(biz_ids, collapse = ", ")))

  # 2. Staff lookup
  staff_lookup <- build_staff_lookup(con, access_token, biz_ids)
  print(paste0("Staff lookup rows: ", nrow(staff_lookup),
               " (", sum(is.na(staff_lookup$msgraph_user_id)), " ohne user-match)"))

  # 3. Existing events (für meeting_id-Match)
  existing_events <- dplyr::tbl(con, I("raw.msgraph_events")) %>% dplyr::collect()

  # 4. Pro Business Appointments holen und konvertieren
  all_events_df <- NULL
  all_parts_df  <- NULL
  for (b in biz_ids) {
    appts <- retrieve_booking_appointments(access_token, b, startDate)
    print(paste0("  ", b, ": ", length(appts), " appointments"))
    if (length(appts) == 0) next
    out <- appointments_to_event_dataframes(appts, staff_lookup, existing_events)
    all_events_df <- dplyr::bind_rows(all_events_df, out$events)
    all_parts_df  <- dplyr::bind_rows(all_parts_df,  out$participants)
  }

  if (is.null(all_events_df) || nrow(all_events_df) == 0) {
    print("No booking appointments to process.")
    return(invisible(NULL))
  }

  # 5. all_events_df an die Schema-Erwartungen von update_events() angleichen.
  # update_events() ruft tidyr::unnest_wider auf onlineMeeting/start/end nicht mehr —
  # wir liefern die Felder bereits flach. Die nicht benötigten Spalten attendees/organizer
  # ergänzen wir als list() damit dplyr::select(-attendees, -organizer) in update_events
  # keinen Fehler wirft.
  all_events_df$attendees <- replicate(nrow(all_events_df), list(), simplify = FALSE)
  all_events_df$organizer <- replicate(nrow(all_events_df), list(), simplify = FALSE)

  update_events(con, all_events_df, startDate)
  update_contacts_from_events(con, all_parts_df)
  update_event_participants(con, all_parts_df)

  invisible(NULL)
}
```

- [ ] **Step 2: Schema-Kompatibilität gegen `update_events()` prüfen**

`update_events()` erwartet in `all_calendar_events_` die Spalten, die in `msgraph_events.R:46-59` erzeugt werden. Unsere Konvertierung muss diese alle bereitstellen. Schnellcheck im REPL:

```r
devtools::load_all()
existing_events <- dplyr::tbl(con, I("raw.msgraph_events")) %>% dplyr::collect()
lookup <- dbconnectorR:::build_staff_lookup(con, access_token, c(
  "StudyflixBeratungsgesprch@studyf.onmicrosoft.com"
))
appts <- dbconnectorR:::retrieve_booking_appointments(
  access_token, "StudyflixBeratungsgesprch@studyf.onmicrosoft.com",
  lubridate::today() - 30
)
out <- dbconnectorR:::appointments_to_event_dataframes(appts, lookup, existing_events)

# Spalten, die update_events() via select() referenziert:
required_cols <- c("iCalUId","createdDateTime","lastModifiedDateTime","subject",
                   "type","isOnlineMeeting","onlineMeeting_joinUrl",
                   "start_dateTime","end_dateTime","isCancelled","is_canceled",
                   "isOrganizer","user_id")
setdiff(required_cols, names(out$events))
```

Erwartung: leerer Vektor (alle Spalten vorhanden).

- [ ] **Step 3: End-to-End Trockenlauf gegen DB**

```r
devtools::load_all()
msgraph_update_booking_appointments(con, access_token, lubridate::today() - 30)
```

Erwartung: print-Output zeigt Anzahl Businesses + Appointments pro Business; keine Errors.

- [ ] **Step 4: DB-Verifikation**

```r
# Customers für die Beratungsgespräch-Mailbox sichtbar?
beratung_users <- dplyr::tbl(con, I("raw.msgraph_users")) %>%
  dplyr::filter(email == "StudyflixBeratungsgesprch@studyf.onmicrosoft.com") %>%
  dplyr::pull(msgraph_user_id)

dplyr::tbl(con, I("raw.msgraph_events")) %>%
  dplyr::filter(user_id %in% !!beratung_users | startsWith(msgraph_ical_uid, "booking:")) %>%
  dplyr::collect() %>%
  nrow()

# externe Customers im Participants-Topf?
dplyr::tbl(con, I("raw.msgraph_event_participants")) %>%
  dplyr::left_join(dplyr::tbl(con, I("raw.msgraph_contacts")) %>% dplyr::select(id, email),
                   by = c("contact_id" = "id")) %>%
  dplyr::filter(!is_internal_email(email)) %>%
  dplyr::collect() %>%
  dplyr::semi_join(
    dplyr::tbl(con, I("raw.msgraph_events")) %>%
      dplyr::filter(startsWith(msgraph_ical_uid, "booking:")) %>%
      dplyr::select(id) %>% dplyr::collect(),
    by = c("event_id" = "id")
  ) %>%
  nrow()
```

Erwartung: beide > 0. Wenn 0 → siehe Task 7 (Troubleshooting).

- [ ] **Step 5: Commit**

```bash
git add R/msgraph_events.R
git commit -m "feat(msgraph): add msgraph_update_booking_appointments orchestrator"
```

---

### Task 6: Idempotenz-Test (zweiter Lauf darf nichts kaputt machen)

**Files:** keine Änderungen, nur Verifikation.

- [ ] **Step 1: Zählungen vor zweitem Lauf festhalten**

```r
before_events <- dplyr::tbl(con, I("raw.msgraph_events")) %>%
  dplyr::filter(startsWith(msgraph_ical_uid, "booking:")) %>%
  dplyr::collect() %>% nrow()
before_parts <- dplyr::tbl(con, I("raw.msgraph_event_participants")) %>%
  dplyr::collect() %>% nrow()
```

- [ ] **Step 2: Zweiten Lauf ausführen**

```r
msgraph_update_booking_appointments(con, access_token, lubridate::today() - 30)
```

- [ ] **Step 3: Vergleich**

```r
after_events <- dplyr::tbl(con, I("raw.msgraph_events")) %>%
  dplyr::filter(startsWith(msgraph_ical_uid, "booking:")) %>%
  dplyr::collect() %>% nrow()
after_parts <- dplyr::tbl(con, I("raw.msgraph_event_participants")) %>%
  dplyr::collect() %>% nrow()

before_events == after_events  # erwartet: TRUE
abs(before_parts - after_parts) <= 5  # kleine Schwankung durch parallelen Sync OK
```

Erwartung: keine neuen booking-Events, Participants-Count stabil. Falls FALSE → match-cols der Upserts prüfen.

---

### Task 7: Call-Event-Mapping läuft auch automatisch?

**Files:** keine Änderungen, nur Verifikation gegen `mapping.msgraph_call_event`.

- [ ] **Step 1: msgraph_map_calls_events ausführen**

```r
msgraph_map_calls_events(con)
```

- [ ] **Step 2: Booking-Events mit Call-Match zählen**

```r
booking_event_ids <- dplyr::tbl(con, I("raw.msgraph_events")) %>%
  dplyr::filter(startsWith(msgraph_ical_uid, "booking:")) %>%
  dplyr::pull(id) %>% as.integer()

dplyr::tbl(con, I("mapping.msgraph_call_event")) %>%
  dplyr::filter(event_id %in% !!booking_event_ids & !is.na(call_id)) %>%
  dplyr::collect() %>% nrow()
```

Erwartung: > 0, wenn es Booking-Termine mit zugehörigen Teams-Calls im Zeitfenster gibt.

Falls 0: prüfen ob `meeting_id` auf den Booking-Events korrekt gefüllt ist:

```r
dplyr::tbl(con, I("raw.msgraph_events")) %>%
  dplyr::filter(startsWith(msgraph_ical_uid, "booking:")) %>%
  dplyr::select(msgraph_ical_uid, meeting_id) %>%
  dplyr::collect() %>%
  dplyr::count(empty = is.na(meeting_id) | meeting_id == "")
```

Wenn meeting_id leer: `update_events()` ruft `extract_meeting_id(online_meeting_join_url)` auf — also muss unsere Konvertierung `onlineMeeting_joinUrl` korrekt befüllen. Im REPL Inhalt verifizieren.

---

### Task 8: Aufruf in den consuming-Apps verankern (Out-of-Repo)

`dbconnectorR` ist nur das Paket — die FlowForce-Scripts liegen in den jeweiligen `base-apps/` und `shiny-apps/`-Repos. Dort muss der neue Funktionsaufruf hinzugefügt werden, nachdem dieses PR gemergt und im consuming-Repo `renv::update("dbconnectorR")` (oder die im Repo übliche Pinning-Methode) gelaufen ist.

**Out-of-scope für diesen PR — Hinweis für den Mergenden:**

```r
# Direkt nach msgraph_update_events(...) ergänzen:
msgraph_update_booking_appointments(con, access_token, startDate)
```

Betroffene Repos identifizieren:

```bash
cd c:/Users/HEMM036/Github
grep -rln "msgraph_update_events" base-apps shiny-apps
```

- [ ] **Step 1: Liste an betroffenen do-Scripts dokumentieren** (Issue-Kommentar an [PubPackR/dbconnectorR#14](https://github.com/PubPackR/dbconnectorR/issues/14) mit den gefundenen Pfaden, damit die Integration nicht vergessen wird).

---

## Self-Review

**Spec coverage:**
- "Eine neue Funktion `msgraph_update_booking_appointments()` in `R/msgraph_events.R`" → Task 5 ✓
- "Pulls appointments aus allen 4 Booking-Businesses" → Task 5, Schritt 1 ✓
- "Customers landen in `raw.msgraph_contacts` und `raw.msgraph_event_participants`" → Task 5, durch reuse von `update_contacts_from_events` und `update_event_participants` ✓
- "Match via Teams `meeting_id`; sonst neue Row mit `msgraph_ical_uid = "booking:<id>"`" → Task 4 ✓
- "Customers ohne Email → synthetic `<bookingId>-<idx>@external.guest`" → Task 4 ✓
- "Helper `retrieve_booking_staff`, `retrieve_booking_appointments`, `build_staff_lookup`, `appointments_to_event_dataframes`" → Tasks 1-4 ✓
- "Subject NICHT überschreiben (Original bleibt)" → Task 4 schreibt subject nur als fallback; existing rows behalten ihren subject weil `update_events()` an Zeile 200-203 via `slice_max(event_updated_at)` priorisiert. Wir setzen `lastModifiedDateTime = start_dt` für Booking-only-Rows; bestehende calendarView-Rows haben das echte `lastModifiedDateTime` und gewinnen den slice_max. ✓
- "Idempotenz" → Task 6 ✓
- "Cancelled Appointments" → laut MS-Graph-Docs verschwinden cancelled Bookings aus dem List-Response. Spec sagt: "Appointments, die völlig aus dem Response fallen, fassen wir nicht an" → automatisch erfüllt, da `update_events` mit `delete_missing = FALSE` aufruft. Kein eigener Task nötig.
- "Logging" → print-Statements in Task 5 ✓

**Placeholder scan:** Keine TBD/TODO/`[file paths]`. Alle Code-Blocks sind komplett.

**Type consistency:** `msgraph_ical_uid` (Spalte in DB) vs `iCalUId` (im events-df für Kompatibilität mit `update_events`-Ablauf, der per `dplyr::select(msgraph_ical_uid = iCalUId, ...)` umbenennt) — bewusst gewählt für Wiederverwendung von `update_events()`. ✓

`extract_meeting_id` wird in Task 4 verwendet — ist bereits definiert in [`R/utils_msgraph_func.R:139`](../../../R/utils_msgraph_func.R#L139). ✓

`fetch_with_retry` wird in Tasks 1, 2, 5 verwendet — definiert in [`R/utils_msgraph_func.R:66`](../../../R/utils_msgraph_func.R#L66). ✓

`%||%` (null-coalescing) wird mehrfach verwendet. Verifiziert: in [`R/msgraph_calls.R`](../../../R/msgraph_calls.R) bereits in Benutzung, und seit R 4.4.0 base — daher ohne weiteren Import nutzbar. ✓

