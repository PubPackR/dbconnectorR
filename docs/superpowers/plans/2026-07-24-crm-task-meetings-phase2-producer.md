# CRM-Task-Meetings — Phase 2 (Migration + Producer) Implementation Plan

> **For agentic workers:** implement task-by-task, TDD. Steps use checkbox (`- [ ]`) syntax.

**Goal:** CRM-Task-Meetings als Zeilen (`source='crm_task'`) in `processed.msgraph_extern_event_classification` schreiben — via einer reinen Assembly-Funktion (unit-getestet) + einem duennen DB-Producer, der als letzter Schritt in base-35 laeuft.

**Architecture (Y2 + scoped DELETE+INSERT):** Der MSGraph-Producer `update_extern_event_classification` bleibt **unveraendert**; sein `delete_missing=TRUE` loescht die CRM-Zeilen transient mit (deren `call_event_mapping_id` ist NULL → faellt aus `out_of_scope`). Der **neue CRM-Schritt laeuft danach** und schreibt die CRM-Zeilen frisch: `DELETE ... WHERE source='crm_task'` + Insert/Upsert (`delete_missing=FALSE`, Match `(crm_task_id, contact_id)`). Sequentiell im selben `base-35/do/main.R`-Lauf → kein Rennen. CRM-Zeilen tragen `call_event_mapping_id=NULL` + eigenes `crm_event_date`; der Dashboard-Helper joint spaeter LEFT + coalesce (Phase 3).

**Tech Stack:** R, dplyr, stringr, testthat, Billomatics::postgres_upsert_data, DBI.

**Spec:** `docs/superpowers/specs/2026-07-24-crm-task-meetings-in-event-classification-design.md`.
**Baut auf Phase 1:** `is_vc_task`, `extract_meeting_tool`, `is_external_tool`, `classify_meeting_status`, `filter_new_crm_meetings` in `R/crm_task_meeting_parser.R`.

## Global Constraints

- Branch `feat/crm-task-meetings-classification` (Phase 1 schon drauf).
- Zielschema `processed.msgraph_extern_event_classification` Bestandsspalten: `id, created_at, updated_at, call_event_mapping_id, contact_id, is_responsible, is_organizer, is_no_show, excluded, exclusion_reason, original_created_at, is_short_lived_event`. Unique/Match heute: `(call_event_mapping_id, contact_id)`.
- Neue Spalten (Migration): `source text NOT NULL DEFAULT 'msgraph'`, `meeting_tool text`, `meeting_status text`, `is_external_tool boolean`, `crm_event_date date`, `crm_task_id bigint`. `call_event_mapping_id` → NULLbar.
- Status-Kanon: `show_up`/`no_show`/`storniert`/`unbekannt`. `storniert → excluded=TRUE`, `is_no_show=FALSE`. `no_show → is_no_show=TRUE`. Sonst `is_no_show=FALSE, excluded=FALSE`.
- Alle DB-Timestamps UTC; `crm_event_date = as.Date(precise_time, tz='Europe/Berlin')`.
- `library(bit64)` vor integer64-Casts (crm_task_id/lead_id sind bigint/integer).
- Kein `<<-`; roxygen2 auf exportierten Funktionen; `needed_tables` pflegen.

## File Structure

- Create: `R/crm_task_meeting_classification.R` — `assemble_crm_classification_rows()` (rein) + `update_crm_task_meeting_classification()` (DB-Wrapper).
- Create: `tests/testthat/test-crm_task_meeting_classification.R` — Unit-Tests fuer die reine Assembly.
- Create: `inst/sql/2026-07-24-crm-task-meetings-columns.sql` — Migrations-ALTER.
- (Separater base-35-PR: `do/main.R` um den CRM-Schritt + `needed_tables` erweitern — NICHT in diesem Repo-PR.)

---

### Task 1: Reine Assembly-Funktion `assemble_crm_classification_rows()`

**Files:**
- Create: `R/crm_task_meeting_classification.R`
- Test: `tests/testthat/test-crm_task_meeting_classification.R`

**Interfaces:**
- Consumes (Phase 1): `is_vc_task`, `extract_meeting_tool`, `is_external_tool`, `classify_meeting_status`, `filter_new_crm_meetings`.
- Produces:
  `assemble_crm_classification_rows(crm_tasks, crm_comments, crm_user_contact, lead_contact, msgraph_meetings) -> data.frame`
  - `crm_tasks`: Spalten `crm_task_id, lead_id, user_id, precise_time, task_name` (nur is_deleted=FALSE; VC-Filter passiert INNEN).
  - `crm_comments`: Spalten `task_id, comment_name` (nur is_deleted=FALSE).
  - `crm_user_contact`: Spalten `user_id, contact_id` (aufgeloester Sales-Rep-msgraph-Kontakt; kann fehlen → linke Zeile ohne Match).
  - `lead_contact`: Spalten `lead_id, contact_id` (Lead-msgraph-Kontakt).
  - `msgraph_meetings`: Spalten `lead_id, event_date` (fuer Anti-Join).
  - Rueckgabe-Spalten: `call_event_mapping_id (NA integer64/na), contact_id, is_responsible (TRUE), is_organizer (TRUE), is_no_show, excluded, exclusion_reason, original_created_at, is_short_lived_event (FALSE), source ('crm_task'), meeting_tool, meeting_status, is_external_tool, crm_event_date, crm_task_id`.

**Logik:** (1) VC-Tasks filtern via `is_vc_task(task_name)`. (2) `meeting_tool=extract_meeting_tool`, `is_external_tool=is_external_tool(tool)`. (3) Status: pro Task die staerkste Kategorie ueber alle Kommentare (Praezedenz storniert>no_show>show_up>unbekannt). (4) `crm_event_date=as.Date(precise_time, tz='Europe/Berlin')`. (5) Anti-Join via `filter_new_crm_meetings` (braucht `is_external_tool`, `lead_id`, `event_date`). (6) `contact_id` = Lead-Kontakt (`lead_contact`); Zeilen ohne Lead-Kontakt fallen raus (kein contact_id moeglich). (7) is_no_show/excluded aus Status.

- [ ] **Step 1: Write the failing test**

Create `tests/testthat/test-crm_task_meeting_classification.R`:

```r
# Tests fuer assemble_crm_classification_rows() (Spec 2026-07-24, Phase 2).

make_tasks <- function() {
  data.frame(
    crm_task_id = c(1L, 2L, 3L, 4L),
    lead_id     = c(10L, 20L, 30L, 40L),
    user_id     = c(100L, 100L, 200L, 200L),
    precise_time = as.POSIXct(
      c("2026-07-20 10:00:00", "2026-07-20 09:00:00",
        "2026-07-21 08:00:00", "2026-07-22 14:00:00"), tz = "UTC"),
    task_name = c("VC Webex ZR mit Frings",   # extern webex
                  "VC Teams Update",          # teams
                  "VC Zoom NV",               # extern zoom
                  "er machen"),               # KEIN VC-Termin
    stringsAsFactors = FALSE
  )
}
make_comments <- function() {
  data.frame(
    task_id = c(1L, 2L),
    comment_name = c("Kunde ist nicht erschienen", "Termin wurde abgesagt"),
    stringsAsFactors = FALSE
  )
}
make_lead_contact <- function() {
  data.frame(lead_id = c(10L, 20L, 30L, 40L),
             contact_id = c(1010L, 2020L, 3030L, 4040L))
}
make_user_contact <- function() {
  data.frame(user_id = c(100L, 200L), contact_id = c(5100L, 5200L))
}
make_msgraph <- function() {
  # Teams-Task 2 (lead 20, 2026-07-20) hat einen MSGraph-Match -> raus.
  data.frame(lead_id = 20L, event_date = as.Date("2026-07-20"))
}

test_that("assemble bildet nur VC-Termine, wendet Anti-Join und Status korrekt an", {
  res <- assemble_crm_classification_rows(
    crm_tasks = make_tasks(),
    crm_comments = make_comments(),
    crm_user_contact = make_user_contact(),
    lead_contact = make_lead_contact(),
    msgraph_meetings = make_msgraph()
  )
  # task 4 (kein VC) raus; task 2 (teams + MSGraph-Match) raus -> bleiben 1 und 3
  expect_setequal(res$crm_task_id, c(1L, 3L))

  r1 <- res[res$crm_task_id == 1L, ]
  expect_equal(r1$meeting_tool, "webex")
  expect_true(r1$is_external_tool)
  expect_equal(r1$meeting_status, "no_show")
  expect_true(r1$is_no_show)
  expect_false(r1$excluded)
  expect_equal(r1$contact_id, 1010L)
  expect_equal(as.character(r1$crm_event_date), "2026-07-20")
  expect_equal(r1$source, "crm_task")
  expect_true(is.na(r1$call_event_mapping_id))
  expect_true(r1$is_responsible)

  r3 <- res[res$crm_task_id == 3L, ]
  expect_equal(r3$meeting_tool, "zoom")
  expect_equal(r3$meeting_status, "unbekannt")  # kein Kommentar
  expect_false(r3$is_no_show)
})

test_that("storniertes Meeting wird excluded=TRUE, is_no_show=FALSE", {
  tasks <- make_tasks()[1, ]
  comments <- data.frame(task_id = 1L, comment_name = "verschoben auf naechste Woche")
  res <- assemble_crm_classification_rows(
    tasks, comments, make_user_contact(), make_lead_contact(),
    make_msgraph()[0, ]
  )
  expect_equal(res$meeting_status, "storniert")
  expect_true(res$excluded)
  expect_false(res$is_no_show)
})
```

- [ ] **Step 2: Run test to verify it fails**

Run (PowerShell):
```powershell
Rscript -e "devtools::load_all('.'); testthat::test_file('tests/testthat/test-crm_task_meeting_classification.R')"
```
Expected: FAIL — `could not find function "assemble_crm_classification_rows"`.

- [ ] **Step 3: Write minimal implementation**

Create `R/crm_task_meeting_classification.R`:

```r
#' Baut CRM-Task-Meetings zu Zeilen im Klassifikations-Schema zusammen
#'
#' Rein (kein DB-Zugriff). Filtert VC-Termine, extrahiert Tool + Status,
#' wendet den Anti-Join gegen bestehende MSGraph-Meetings an und mappt auf das
#' Schema von processed.msgraph_extern_event_classification (+ CRM-Zusatzspalten).
#'
#' @param crm_tasks data.frame: crm_task_id, lead_id, user_id, precise_time, task_name.
#' @param crm_comments data.frame: task_id, comment_name.
#' @param crm_user_contact data.frame: user_id, contact_id (Sales-Rep-Kontakt).
#' @param lead_contact data.frame: lead_id, contact_id (Lead-Kontakt).
#' @param msgraph_meetings data.frame: lead_id, event_date.
#' @return data.frame im Klassifikations-Schema mit source='crm_task'.
#' @export
assemble_crm_classification_rows <- function(crm_tasks, crm_comments,
                                             crm_user_contact, lead_contact,
                                             msgraph_meetings) {
  # 1. nur VC-Termine
  vc <- crm_tasks[is_vc_task(crm_tasks$task_name), , drop = FALSE]
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
  vc$meeting_status <- agg$meeting_status[match(vc$crm_task_id, agg$task_id)]
  vc$meeting_status[is.na(vc$meeting_status)] <- "unbekannt"

  # 4. Datum (Europe/Berlin)
  vc$event_date <- as.Date(vc$precise_time, tz = "Europe/Berlin")

  # 5. Anti-Join
  vc <- filter_new_crm_meetings(vc, msgraph_meetings)
  if (nrow(vc) == 0) return(assemble_crm_empty_result())

  # 6. Lead-Kontakt (ohne Kontakt keine Zeile)
  vc$contact_id <- lead_contact$contact_id[match(vc$lead_id, lead_contact$lead_id)]
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
```

- [ ] **Step 4: Run test to verify it passes**

Run (PowerShell):
```powershell
Rscript -e "devtools::load_all('.'); testthat::test_file('tests/testthat/test-crm_task_meeting_classification.R')"
```
Expected: PASS (2 tests).

- [ ] **Step 5: Commit**

```bash
git add R/crm_task_meeting_classification.R tests/testthat/test-crm_task_meeting_classification.R
git commit -m "feat: assemble_crm_classification_rows (reine Assembly CRM-Meetings)"
```

---

### Task 2: Migrations-SQL (neue Spalten + FK nullbar)

**Files:**
- Create: `inst/sql/2026-07-24-crm-task-meetings-columns.sql`

- [ ] **Step 1: Write the migration**

Create `inst/sql/2026-07-24-crm-task-meetings-columns.sql`:

```sql
-- Migration: CRM-Task-Meetings-Spalten auf der kanonischen Klassifikations-Tabelle.
-- Idempotent (IF NOT EXISTS). Vor dem ersten Lauf von update_crm_task_meeting_classification ausfuehren.
ALTER TABLE processed.msgraph_extern_event_classification
  ALTER COLUMN call_event_mapping_id DROP NOT NULL,
  ADD COLUMN IF NOT EXISTS source           text NOT NULL DEFAULT 'msgraph',
  ADD COLUMN IF NOT EXISTS meeting_tool     text,
  ADD COLUMN IF NOT EXISTS meeting_status   text,
  ADD COLUMN IF NOT EXISTS is_external_tool boolean,
  ADD COLUMN IF NOT EXISTS crm_event_date   date,
  ADD COLUMN IF NOT EXISTS crm_task_id      bigint;

-- Eindeutigkeit der CRM-Zeilen (partial unique) fuer den scoped Upsert.
CREATE UNIQUE INDEX IF NOT EXISTS uq_extern_event_classification_crm_task
  ON processed.msgraph_extern_event_classification (crm_task_id, contact_id)
  WHERE source = 'crm_task';

-- Schneller Filter fuer den scoped DELETE.
CREATE INDEX IF NOT EXISTS idx_extern_event_classification_source
  ON processed.msgraph_extern_event_classification (source);
```

- [ ] **Step 2: Commit**

```bash
git add inst/sql/2026-07-24-crm-task-meetings-columns.sql
git commit -m "feat: Migration CRM-Meeting-Spalten + partial unique/index"
```

---

### Task 3: DB-Producer `update_crm_task_meeting_classification()`

**Files:**
- Modify: `R/crm_task_meeting_classification.R`

**Interfaces:**
- Produces: `update_crm_task_meeting_classification(con) -> invisible(n)` — laedt CRM-Tasks/Kommentare/Identitaets-Lookups + MSGraph-Meetings, ruft `assemble_crm_classification_rows`, schreibt scoped (DELETE source='crm_task' + Upsert delete_missing=FALSE, match `(crm_task_id, contact_id)`). Fail-safe fuers Log.

- [ ] **Step 1: Write implementation**

Append to `R/crm_task_meeting_classification.R`:

```r
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
    dplyr::select(crm_task_id, lead_id, user_id, assigned_to_user_id,
                  precise_time, task_name) %>%
    dplyr::collect()

  crm_comments <- dplyr::tbl(con, I("raw.crm_lead_task_comments")) %>%
    dplyr::filter(is_deleted == FALSE) %>%
    dplyr::select(task_id, comment_name) %>%
    dplyr::collect()

  # Lead -> msgraph-Kontakt (Primary bevorzugt)
  lead_contact <- dplyr::tbl(con, I("mapping.crm_lead_msgraph_contact")) %>%
    dplyr::select(lead_id = crm_lead_id, contact_id = msgraph_contact_id,
                  is_primary_crm) %>%
    dplyr::collect() %>%
    dplyr::arrange(dplyr::desc(is_primary_crm)) %>%
    dplyr::distinct(lead_id, .keep_all = TRUE) %>%
    dplyr::select(lead_id, contact_id)

  # crm_user -> Personio -> (E-Mail) -> msgraph-Kontakt. Best effort; wenn leer,
  # bleibt crm_user_contact leer (Rep-Aufloesung optional fuer diese Phase).
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
        dplyr::select(contact_id = msgraph_contact_id, lead_id = crm_lead_id),
      by = "contact_id") %>%
    dplyr::distinct(lead_id, event_date) %>%
    dplyr::collect()

  # assigned_to_user_id bevorzugen, sonst user_id
  crm_tasks$user_id <- ifelse(!is.na(crm_tasks$assigned_to_user_id),
                              crm_tasks$assigned_to_user_id, crm_tasks$user_id)

  rows <- assemble_crm_classification_rows(
    crm_tasks = crm_tasks, crm_comments = crm_comments,
    crm_user_contact = crm_user_contact, lead_contact = lead_contact,
    msgraph_meetings = msgraph_meetings)

  message(paste0("  ", nrow(rows), " CRM-Meeting-Zeilen zu schreiben"))

  # scoped: bestehende CRM-Zeilen loeschen, dann frische schreiben
  DBI::dbExecute(con,
    "DELETE FROM processed.msgraph_extern_event_classification WHERE source = 'crm_task'")

  if (nrow(rows) > 0) {
    Billomatics::postgres_upsert_data(
      con, "processed", "msgraph_extern_event_classification",
      rows, match_cols = c("crm_task_id", "contact_id"), delete_missing = FALSE)
  }
  message("  fertig.")
  invisible(nrow(rows))
}

#' Loest CRM-User auf msgraph-Kontakte auf (best effort)
#'
#' Primaer via mapping.vw_service_users (connected_service='crm'); Fallback ueber
#' crm_users.user_login == personio_persons.email == msgraph_contacts (E-Mail).
#' Gibt bei fehlender Aufloesung eine leere/teilweise Tabelle zurueck.
#'
#' @param con Pool/DBI-Connection.
#' @return data.frame: user_id, contact_id.
#' @keywords internal
resolve_crm_user_contact <- function(con) {
  # Diese Aufloesung ist DB-abhaengig und wird von Moritz gegen die echte DB
  # verifiziert (vw_service_users-crm-Zweig vorhanden?). Vorerst E-Mail-Fallback.
  crm_users <- dplyr::tbl(con, I("raw.crm_users")) %>%
    dplyr::filter(is_deleted == FALSE) %>%
    dplyr::select(user_id = crm_user_id, user_login) %>%
    dplyr::collect()
  personio <- dplyr::tbl(con, I("raw.personio_persons")) %>%
    dplyr::select(email) %>% dplyr::collect()
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
```

- [ ] **Step 2: Parse-Check**

Run (PowerShell):
```powershell
Rscript -e "parse('R/crm_task_meeting_classification.R'); cat('parse OK\n')"
```
Expected: `parse OK`.

- [ ] **Step 3: Roxygen + Testlauf (Regression)**

Run (PowerShell):
```powershell
Rscript -e "devtools::document(); devtools::test()"
```
Expected: `NAMESPACE`/`man` aktualisiert; alle Tests gruen.

- [ ] **Step 4: Commit**

```bash
git add R/crm_task_meeting_classification.R NAMESPACE man/
git commit -m "feat: update_crm_task_meeting_classification DB-Producer (scoped)"
```

---

## Self-Review

**Spec coverage:** Migration (§5.4) → Task 2; Assembly + Status/Anti-Join/Regeln (§5.1/§5.3/§6) → Task 1; Producer scoped write (§3/§5.5) → Task 3; Identitaet (§5.2) → `resolve_crm_user_contact` + `lead_contact` (best effort, DB-verifiziert). base-35-Einbindung → separater Repo-PR (nicht hier).

**Bewusst offen (DB-verifiziert durch Moritz):** `resolve_crm_user_contact` haengt an der echten `raw.msgraph_contacts`-Spaltenstruktur (E-Mail/ID-Spaltennamen defensiv erkannt) und am `vw_service_users`-crm-Zweig; `msgraph_meetings`-Anti-Join-Query gegen echte Daten sanity-checken. Der Producer ist DB-gebunden und wird nicht in CI ausgefuehrt.

**Placeholder scan:** kein TBD; reale Assembly + Tests + Migration + Producer.

## base-35-Wiring (separater PR, nach diesem)

In `base-apps/base-35-export_teams_history/do/main.R`: `needed_tables` um `raw.crm_lead_tasks`, `raw.crm_lead_task_comments`, `raw.crm_users`, `raw.msgraph_contacts`, `mapping.vw_service_users` ergaenzen; nach dem `update_extern_event_classification`-Block einen neuen `run_data_job`-Block mit `dbconnectorR::update_crm_task_meeting_classification(con)` einfuegen.
