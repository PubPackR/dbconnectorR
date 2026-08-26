# sales_meetings_unified (Phase 4 — No-Show-Vereinheitlichung) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Eine materialisierte Tabelle `processed.sales_meetings_unified` bauen, die alle Meetings (MSGraph + CRM) mit finalem No-Show-Status enthält, und das No-Show-Modul darauf umstellen.

**Architecture:** Neue reine Assembly-Funktion (Match/Override/Gap-Fill) + DB-Producer in `dbconnectorR`, eingehängt in `base-35/do/main.R` (voller Rebuild). Das shiny-99 No-Show-Modul liest über eine parallele Pool-Funktion aus der neuen Tabelle. MSGraph-Rohzeilen und alle anderen Tabs bleiben unangetastet.

**Tech Stack:** R (dbplyr, dplyr, pool, testthat), PostgreSQL, Billomatics-Helper, shiny.

## Global Constraints

- DB-Zugriff: `Billomatics::authentication_process(c("postgresql"), args)` + `Billomatics::postgres_connect(needed_tables=..., postgres_keys=keys$postgresql)`; Tabellen immer `tbl(con, I("schema.table"))`; `on.exit(pool::poolClose(con), add = TRUE)`.
- Schreiben: `Billomatics::postgres_upsert_data(conn, schema, table, data, match_cols, delete_missing)`; DDL-Änderungen via `/create-db-schema`-Standard (`id bigint GENERATED ALWAYS AS IDENTITY`, `created_at`/`updated_at`, `trigger_set_updated_at`, `pk_`/`uq_`/`idx_`-Namen, FK-Indizes).
- integer64/bigint: niemals via `ifelse()` coalescen (strippt die Klasse); Vergleiche über `as.character()`.
- Alle DB-Timestamps sind UTC; `as.Date()` auf POSIXct immer mit `tz="Europe/Berlin"`.
- stringr `str_detect`/`str_starts` NICHT in dbplyr-Pipelines (übersetzt nicht nach SQL) — erst `collect()`, dann stringr.
- Neue Funktionen: roxygen2-Doku (`#' @param`, `#' @return`, `@export` bzw. `@keywords internal`).
- Reihenfolge in `do/main.R`: der Unified-Producer läuft NACH `update_crm_task_meeting_classification`.

---

## File Structure

- **Create** `dbconnectorR/inst/sql/2026-08-11-sales-meetings-unified.sql` — DDL für `processed.sales_meetings_unified`.
- **Create** `dbconnectorR/R/sales_meetings_unified.R` — reine Funktion `assemble_unified_meetings()` + Producer `update_sales_meetings_unified()`.
- **Create** `dbconnectorR/tests/testthat/test-sales_meetings_unified.R` — Unit-Tests für die reine Funktion.
- **Modify** `base-35-export_teams_history/do/main.R` — `needed_tables` + neuer `run_data_job`-Block.
- **Modify** `shiny-99-modules/func/module_sales_kpi/external_events_helpers.R` — neue Funktion `get_responsible_event_pool_unified()`.
- **Modify** `shiny-99-modules/func/module_sales_kpi/module_kpi_no_show.R:845` — „verantwortlicher"-Zweig auf die Unified-Pool-Funktion umstellen.

---

### Task 1: DB-Schema `processed.sales_meetings_unified`

**Files:**
- Create: `dbconnectorR/inst/sql/2026-08-11-sales-meetings-unified.sql`

**Interfaces:**
- Produces: Tabelle `processed.sales_meetings_unified` mit Spalten (fachlich): `meeting_key text UNIQUE NOT NULL`, `source text NOT NULL`, `event_date date`, `contact_id bigint`, `lead_id integer`, `is_no_show boolean`, `no_show_source text`, `meeting_status text`, `meeting_tool text`, `is_external_tool boolean`, `excluded boolean NOT NULL DEFAULT false`, `is_short_lived_event boolean NOT NULL DEFAULT false`, `is_responsible boolean NOT NULL DEFAULT true`, `original_created_at timestamp`, `event_id text` — plus Haus-Standard `id`/`created_at`/`updated_at`/Trigger.

- [ ] **Step 1: DDL schreiben** (`/create-db-schema`-Konventionen; die Datei ist die Quelle der Wahrheit)

```sql
-- processed.sales_meetings_unified: alle Meetings (MSGraph + CRM) mit finalem No-Show-Status.
-- Abgeleitete Tabelle, voller Rebuild durch dbconnectorR::update_sales_meetings_unified().
CREATE TABLE IF NOT EXISTS processed.sales_meetings_unified (
  id                   bigint GENERATED ALWAYS AS IDENTITY,
  meeting_key          text        NOT NULL,
  source               text        NOT NULL,
  event_date           date,
  contact_id           bigint,
  lead_id              integer,
  is_no_show           boolean,
  no_show_source       text,
  meeting_status       text,
  meeting_tool         text,
  is_external_tool     boolean,
  excluded             boolean     NOT NULL DEFAULT false,
  is_short_lived_event boolean     NOT NULL DEFAULT false,
  is_responsible       boolean     NOT NULL DEFAULT true,
  original_created_at  timestamp,
  event_id             text,
  created_at           timestamp   NOT NULL DEFAULT now(),
  updated_at           timestamp   NOT NULL DEFAULT now(),
  CONSTRAINT pk_sales_meetings_unified PRIMARY KEY (id),
  CONSTRAINT uq_sales_meetings_unified_meeting_key UNIQUE (meeting_key)
);

CREATE INDEX IF NOT EXISTS idx_sales_meetings_unified_event_date ON processed.sales_meetings_unified (event_date);
CREATE INDEX IF NOT EXISTS idx_sales_meetings_unified_contact_id ON processed.sales_meetings_unified (contact_id);

CREATE TRIGGER trigger_set_updated_at
  BEFORE UPDATE ON processed.sales_meetings_unified
  FOR EACH ROW EXECUTE FUNCTION trigger_set_updated_at();
```

- [ ] **Step 2: Gegen studyflix_local ausführen und Spalten prüfen**

Run (Positron/psql):
```sql
\i inst/sql/2026-08-11-sales-meetings-unified.sql
SELECT column_name, data_type FROM information_schema.columns
WHERE table_schema='processed' AND table_name='sales_meetings_unified' ORDER BY ordinal_position;
```
Expected: 18 Spalten wie oben, `uq_..._meeting_key` vorhanden.

- [ ] **Step 3: Commit**

```bash
git add inst/sql/2026-08-11-sales-meetings-unified.sql
git commit -m "feat(sql): processed.sales_meetings_unified DDL (Phase 4)"
```

---

### Task 2: Reine Assembly-Funktion `assemble_unified_meetings()`

Das Herzstück (Match/Override/Gap-Fill), rein und voll TDD. Kein DB-Zugriff.

**Files:**
- Create: `dbconnectorR/R/sales_meetings_unified.R`
- Test: `dbconnectorR/tests/testthat/test-sales_meetings_unified.R`

**Interfaces:**
- Produces: `assemble_unified_meetings(msgraph_meetings, crm_meetings) -> data.frame`
  - `msgraph_meetings`: data.frame mit `call_event_mapping_id` (int), `event_id` (chr), `event_date` (Date), `contact_id` (int/int64), `lead_id` (int), `is_no_show` (lgl), `original_created_at` (POSIXct), `excluded` (lgl), `is_short_lived_event` (lgl), `is_responsible` (lgl).
  - `crm_meetings`: data.frame mit `crm_task_id` (int), `lead_id` (int), `event_date` (Date), `contact_id` (int/int64), `meeting_tool` (chr), `meeting_status` (chr), `is_external_tool` (lgl), `original_created_at` (POSIXct).
  - Rückgabe: data.frame mit den fachlichen Tabellenspalten aus Task 1 (`meeting_key`, `source`, `event_date`, `contact_id`, `lead_id`, `is_no_show`, `no_show_source`, `meeting_status`, `meeting_tool`, `is_external_tool`, `excluded`, `is_short_lived_event`, `is_responsible`, `original_created_at`, `event_id`).

**Statusabbildung CRM → (is_no_show, excluded):** `no_show`→(TRUE, FALSE); `show_up`→(FALSE, FALSE); `storniert`→(NA, TRUE); `unbekannt`→(NA, TRUE). „Definitiv" = Status in {`no_show`,`show_up`} (nur diese überschreiben/setzen `is_no_show`).

**Match-Regeln:** extern → immer Netto-neu. nicht-extern: eindeutiger `lead_id`+`event_date`-Match → Override; mehrdeutig → verwerfen; kein Match → Netto-neu.

- [ ] **Step 1: Failing test schreiben**

```r
# tests/testthat/test-sales_meetings_unified.R
mk_msgraph <- function() data.frame(
  call_event_mapping_id = c(10L, 11L, 12L),
  event_id             = c("e10", "e11", "e12"),
  event_date           = as.Date(c("2026-07-01", "2026-07-02", "2026-07-03")),
  contact_id           = c(500L, 500L, 600L),
  lead_id              = c(10L, 20L, 30L),
  is_no_show           = c(FALSE, FALSE, TRUE),
  original_created_at  = as.POSIXct(c("2026-06-01","2026-06-02","2026-06-03"), tz="UTC"),
  excluded             = FALSE, is_short_lived_event = FALSE, is_responsible = TRUE,
  stringsAsFactors = FALSE
)
mk_crm <- function(rows) do.call(rbind, rows)
crm_row <- function(id, lead, date, tool, status, ext) data.frame(
  crm_task_id=id, lead_id=lead, event_date=as.Date(date), contact_id=500L,
  meeting_tool=tool, meeting_status=status, is_external_tool=ext,
  original_created_at=as.POSIXct("2026-06-15", tz="UTC"), stringsAsFactors=FALSE)

test_that("nicht-extern eindeutiger Match: CRM-No-Show ueberschreibt MSGraph", {
  # lead 10 / 2026-07-01 -> genau ein MSGraph-Termin (key 10), war FALSE
  res <- assemble_unified_meetings(mk_msgraph(),
           mk_crm(list(crm_row(1, 10L, "2026-07-01", "teams", "no_show", FALSE))))
  r <- res[res$meeting_key == "10", ]
  expect_true(r$is_no_show)
  expect_equal(r$no_show_source, "crm_override")
  expect_equal(r$meeting_tool, "teams")
})

test_that("CRM show_up ueberschreibt MSGraph is_no_show=TRUE -> FALSE", {
  res <- assemble_unified_meetings(mk_msgraph(),
           mk_crm(list(crm_row(2, 30L, "2026-07-03", "teams", "show_up", FALSE))))
  r <- res[res$meeting_key == "12", ]
  expect_false(r$is_no_show)
  expect_equal(r$no_show_source, "crm_override")
})

test_that("nicht-extern mehrdeutig: CRM-Zeile wird verworfen", {
  ms <- mk_msgraph()
  ms <- rbind(ms, transform(ms[1,], call_event_mapping_id=99L, event_id="e99"))  # 2. Termin lead 10 / 2026-07-01
  res <- assemble_unified_meetings(ms,
           mk_crm(list(crm_row(3, 10L, "2026-07-01", "teams", "no_show", FALSE))))
  expect_false(any(res$no_show_source == "crm_override"))      # kein Override
  expect_false(any(res$meeting_key == "crm_3"))                # kein Netto-neu
})

test_that("nicht-extern kein Match: CRM als Netto-neu (crm_only)", {
  res <- assemble_unified_meetings(mk_msgraph(),
           mk_crm(list(crm_row(4, 77L, "2026-08-01", "teams", "no_show", FALSE))))
  r <- res[res$meeting_key == "crm_4", ]
  expect_equal(r$source, "crm_task")
  expect_true(r$is_no_show)
  expect_equal(r$no_show_source, "crm_only")
})

test_that("externes Tool mit gleicher-Tag-MSGraph-Zeile: immer Netto-neu, MSGraph unveraendert", {
  res <- assemble_unified_meetings(mk_msgraph(),
           mk_crm(list(crm_row(5, 10L, "2026-07-01", "webex", "no_show", TRUE))))
  expect_equal(res[res$meeting_key == "10", ]$no_show_source, "msgraph")  # nicht ueberschrieben
  expect_equal(res[res$meeting_key == "crm_5", ]$source, "crm_task")      # eigener Termin
})

test_that("CRM unbekannt/storniert: excluded=TRUE, kein Override", {
  res <- assemble_unified_meetings(mk_msgraph(),
           mk_crm(list(crm_row(6, 77L, "2026-08-02", "webex", "unbekannt", TRUE))))
  r <- res[res$meeting_key == "crm_6", ]
  expect_true(r$excluded)
  expect_true(is.na(r$is_no_show))
})

test_that("meeting_key eindeutig (keine Doppelzaehlung)", {
  res <- assemble_unified_meetings(mk_msgraph(),
           mk_crm(list(crm_row(7, 77L, "2026-08-03", "zoom", "show_up", TRUE))))
  expect_equal(anyDuplicated(res$meeting_key), 0L)
})
```

- [ ] **Step 2: Test laufen lassen, Fehlschlag prüfen**

Run: `Rscript -e 'pkgload::load_all("."); testthat::test_file("tests/testthat/test-sales_meetings_unified.R")'`
Expected: FAIL — `assemble_unified_meetings` nicht gefunden.

- [ ] **Step 3: Implementierung schreiben**

```r
# R/sales_meetings_unified.R
#' CRM-Status -> (is_no_show, excluded)
#' @keywords internal
crm_status_flags <- function(status) {
  is_no_show <- ifelse(status == "no_show", TRUE, ifelse(status == "show_up", FALSE, NA))
  excluded   <- status %in% c("storniert", "unbekannt")
  list(is_no_show = is_no_show, excluded = excluded)
}

#' Baut die vereinheitlichte Meeting-Menge (rein, kein DB-Zugriff)
#'
#' @param msgraph_meetings data.frame (siehe Plan/Interfaces).
#' @param crm_meetings data.frame (siehe Plan/Interfaces).
#' @return data.frame im Schema von processed.sales_meetings_unified.
#' @export
assemble_unified_meetings <- function(msgraph_meetings, crm_meetings) {
  # Basis: MSGraph-Zeilen
  base <- data.frame(
    meeting_key          = as.character(msgraph_meetings$call_event_mapping_id),
    source               = "msgraph",
    event_date           = msgraph_meetings$event_date,
    contact_id           = msgraph_meetings$contact_id,
    lead_id              = msgraph_meetings$lead_id,
    is_no_show           = msgraph_meetings$is_no_show,
    no_show_source       = "msgraph",
    meeting_status       = NA_character_,
    meeting_tool         = NA_character_,
    is_external_tool     = NA,
    excluded             = msgraph_meetings$excluded,
    is_short_lived_event = msgraph_meetings$is_short_lived_event,
    is_responsible       = msgraph_meetings$is_responsible,
    original_created_at  = msgraph_meetings$original_created_at,
    event_id             = msgraph_meetings$event_id,
    stringsAsFactors     = FALSE
  )

  new_rows <- list()
  # Match-Index: wie viele MSGraph-Termine pro (lead_id, event_date)
  ms_key <- paste(msgraph_meetings$lead_id, msgraph_meetings$event_date)

  for (i in seq_len(nrow(crm_meetings))) {
    cm <- crm_meetings[i, ]
    fl <- crm_status_flags(cm$meeting_status)
    netto_neu <- function() data.frame(
      meeting_key = paste0("crm_", cm$crm_task_id), source = "crm_task",
      event_date = cm$event_date, contact_id = cm$contact_id, lead_id = cm$lead_id,
      is_no_show = fl$is_no_show, no_show_source = "crm_only",
      meeting_status = cm$meeting_status, meeting_tool = cm$meeting_tool,
      is_external_tool = cm$is_external_tool, excluded = fl$excluded,
      is_short_lived_event = FALSE, is_responsible = TRUE,
      original_created_at = cm$original_created_at, event_id = NA_character_,
      stringsAsFactors = FALSE)

    if (isTRUE(cm$is_external_tool)) { new_rows[[length(new_rows)+1]] <- netto_neu(); next }

    idx <- which(ms_key == paste(cm$lead_id, cm$event_date))
    if (length(idx) == 0) { new_rows[[length(new_rows)+1]] <- netto_neu(); next }
    if (length(idx) > 1) next  # mehrdeutig -> verwerfen

    # eindeutiger Match -> Override (nur definitiver Status setzt is_no_show)
    j <- idx[1]
    if (cm$meeting_status %in% c("no_show", "show_up")) base$is_no_show[j] <- fl$is_no_show
    if (cm$meeting_status == "storniert") base$excluded[j] <- TRUE
    base$no_show_source[j] <- "crm_override"
    base$meeting_tool[j]   <- cm$meeting_tool
    base$meeting_status[j] <- cm$meeting_status
  }

  if (length(new_rows) > 0) base <- rbind(base, do.call(rbind, new_rows))
  base
}
```

- [ ] **Step 4: Tests laufen lassen, grün prüfen**

Run: `Rscript -e 'pkgload::load_all("."); testthat::test_file("tests/testthat/test-sales_meetings_unified.R")'`
Expected: PASS (7/7).

- [ ] **Step 5: Doku + Commit**

Run: `Rscript -e 'devtools::document(roclets="namespace")'` (fügt `export(assemble_unified_meetings)` hinzu).
```bash
git add R/sales_meetings_unified.R tests/testthat/test-sales_meetings_unified.R NAMESPACE man/assemble_unified_meetings.Rd
git commit -m "feat(crm-meetings): reine assemble_unified_meetings (Match/Override/Gap-Fill)"
```

---

### Task 3: Producer `update_sales_meetings_unified(con)`

DB-I/O drumherum: MSGraph-Meetings laden, CRM-Meetings frisch aus Rohdaten ableiten (Parser wiederverwenden, OHNE Anti-Join), `assemble_unified_meetings()` rufen, in die Tabelle upserten.

**Files:**
- Modify: `dbconnectorR/R/sales_meetings_unified.R` (Funktion anhängen)

**Interfaces:**
- Consumes: `assemble_unified_meetings()` (Task 2); Parser `is_vc_task`, `extract_meeting_tool`, `is_external_tool`, `classify_meeting_status`, `resolve_crm_user_contact` (PR #22/#23, bereits im Package).
- Produces: `update_sales_meetings_unified(con) -> invisible(int)` (Anzahl geschriebener Zeilen).

- [ ] **Step 1: Implementierung schreiben** (Muster: `R/crm_task_meeting_classification.R:97-157`)

```r
#' Rebuild processed.sales_meetings_unified (voll rueckwirkend)
#'
#' Laeuft nach update_crm_task_meeting_classification. MSGraph-Meetings +
#' frisch aus Rohdaten abgeleitete CRM-VC-Termine (ohne Anti-Join) werden via
#' assemble_unified_meetings() vereinheitlicht und komplett neu geschrieben.
#' @param con Pool/DBI-Connection.
#' @return invisible(Anzahl geschriebener Zeilen).
#' @export
update_sales_meetings_unified <- function(con) {
  message("update_sales_meetings_unified: lade MSGraph-Meetings ...")
  msgraph_meetings <- dplyr::tbl(con, I("processed.msgraph_extern_event_classification")) %>%
    dplyr::filter(source == "msgraph", is_responsible == TRUE) %>%
    dplyr::select(call_event_mapping_id, contact_id, is_no_show, original_created_at,
                  excluded, is_short_lived_event, is_responsible) %>%
    dplyr::inner_join(
      dplyr::tbl(con, I("mapping.msgraph_call_event")) %>%
        dplyr::select(id, event_id, event_date),
      by = c("call_event_mapping_id" = "id")) %>%
    dplyr::left_join(
      dplyr::tbl(con, I("mapping.crm_lead_msgraph_contact")) %>%
        dplyr::filter(is_primary_crm == TRUE) %>%
        dplyr::select(contact_id = msgraph_contact_id, lead_id = crm_lead_id),
      by = "contact_id") %>%
    dplyr::collect()
  msgraph_meetings$event_id   <- as.character(msgraph_meetings$event_id)
  msgraph_meetings$event_date <- as.Date(msgraph_meetings$event_date, tz = "Europe/Berlin")

  message("  leite CRM-VC-Termine aus Rohdaten ab (ohne Anti-Join) ...")
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
  # staerkster Status je Task (Surrogat-id-Join, wie in Phase 2)
  rank <- c(storniert = 3L, no_show = 2L, show_up = 1L, unbekannt = 0L)
  comments$status <- classify_meeting_status(comments$comment_name)
  comments$rank   <- rank[comments$status]
  agg <- stats::aggregate(rank ~ task_id, data = comments, FUN = max)
  agg$meeting_status <- names(rank)[match(agg$rank, rank)]
  vc$meeting_status <- agg$meeting_status[match(as.character(vc$id), as.character(agg$task_id))]
  vc$meeting_status[is.na(vc$meeting_status)] <- "unbekannt"
  vc$event_date <- as.Date(vc$precise_time, tz = "Europe/Berlin")
  # Rep-Kontakt (coalesce assigned_to_user_id/user_id im Character-Raum -> integer64-sicher)
  uid <- ifelse(!is.na(vc$assigned_to_user_id), as.character(vc$assigned_to_user_id),
                as.character(vc$user_id))
  vc$contact_id <- ruc$contact_id[match(uid, as.character(ruc$user_id))]
  vc <- vc[!is.na(vc$contact_id), , drop = FALSE]

  crm_meetings <- data.frame(
    crm_task_id = vc$crm_task_id, lead_id = vc$lead_id, event_date = vc$event_date,
    contact_id = vc$contact_id, meeting_tool = vc$meeting_tool,
    meeting_status = vc$meeting_status, is_external_tool = vc$is_external_tool,
    original_created_at = vc$task_created_at, stringsAsFactors = FALSE)

  rows <- assemble_unified_meetings(msgraph_meetings, crm_meetings)
  message(paste0("  ", nrow(rows), " Zeilen -> processed.sales_meetings_unified"))

  pool::poolWithTransaction(con, function(conn) {
    Billomatics::postgres_upsert_data(
      conn, "processed", "sales_meetings_unified",
      rows, match_cols = c("meeting_key"), delete_missing = TRUE)
  })
  message("  fertig.")
  invisible(nrow(rows))
}
```

- [ ] **Step 2: Parse- + Doku-Check**

Run: `Rscript -e 'pkgload::load_all("."); devtools::document(roclets="namespace"); cat("ok\n")'`
Expected: `ok`, `export(update_sales_meetings_unified)` in NAMESPACE.

- [ ] **Step 3: Commit**

```bash
git add R/sales_meetings_unified.R NAMESPACE man/update_sales_meetings_unified.Rd
git commit -m "feat(crm-meetings): update_sales_meetings_unified Producer (Rebuild)"
```

---

### Task 4: Einbindung in base-35 `do/main.R`

**Files:**
- Modify: `base-35-export_teams_history/do/main.R` (`needed_tables` + neuer `run_data_job`-Block, NACH `update_crm_task_meeting_classification`)

**Interfaces:**
- Consumes: `dbconnectorR::update_sales_meetings_unified(con)` (Task 3).

- [ ] **Step 1: `needed_tables` erweitern**

In der `postgres_connect(needed_tables = c(...))`-Liste ergänzen:
```r
"processed.sales_meetings_unified",
```

- [ ] **Step 2: Job-Block anhängen** (nach dem `update_crm_task_meeting_classification`-Block)

```r
################################################################################-

# Vereinheitlichte Meeting-Tabelle (MSGraph + CRM, finaler No-Show-Status) neu
# aufbauen. MUSS nach update_crm_task_meeting_classification laufen.
Billomatics::run_data_job(
  con,
  job_function = function () {
    dbconnectorR::update_sales_meetings_unified(con)
  },
  job_name = "update_sales_meetings_unified",
  timeout = 180
)
```

- [ ] **Step 3: Parse-Check + Commit**

Run: `Rscript -e 'invisible(parse("do/main.R")); cat("parse OK\n")'`
```bash
git add do/main.R
git commit -m "feat: update_sales_meetings_unified in Pipeline einhaengen"
```

---

### Task 5: No-Show-Modul auf die Unified-Tabelle umstellen (shiny-99)

**Files:**
- Modify: `shiny-99-modules/func/module_sales_kpi/external_events_helpers.R` (neue Funktion in der `module_external_events_helpers`-Liste)
- Modify: `shiny-99-modules/func/module_sales_kpi/module_kpi_no_show.R:845`

**Interfaces:**
- Produces: `module_external_events_helpers$get_responsible_event_pool_unified(con)` — identisches Output-Schema wie `get_responsible_event_pool`: `call_event_mapping_id, is_no_show, original_created_at, contact_id, event_id, event_date, email` (1 Zeile pro Meeting/Rep).

- [ ] **Step 1: Unified-Pool-Funktion hinzufügen** (in `external_events_helpers.R`, neben `get_responsible_event_pool`)

```r
  #' Get Responsible Event Pool (unified — inkl. CRM-Meetings)
  #'
  #' Wie get_responsible_event_pool(), aber aus processed.sales_meetings_unified:
  #' MSGraph-Meetings mit CRM-No-Show-Override + ungematchte CRM-Termine. Nur fuer
  #' die No-Show-Auswertung. `call_event_mapping_id` traegt hier den `meeting_key`
  #' (Dedup-Schluessel; text, fuer CRM-Zeilen 'crm_<id>').
  #' @param con Database connection
  #' @return Tibble: call_event_mapping_id, is_no_show, original_created_at,
  #'   contact_id, event_id, event_date, email (lower). 1 Zeile pro Meeting/Rep.
  get_responsible_event_pool_unified = function(con) {
    tbl(con, I("processed.sales_meetings_unified")) %>%
      filter(excluded == FALSE, is_short_lived_event == FALSE, is_responsible == TRUE) %>%
      select(call_event_mapping_id = meeting_key, is_no_show, original_created_at,
             contact_id, event_id, event_date) %>%
      left_join(
        tbl(con, I("raw.msgraph_contacts")) %>% select(id, email),
        by = c("contact_id" = "id")
      ) %>%
      collect() %>%
      mutate(email = tolower(email))
  },
```

- [ ] **Step 2: No-Show-Modul umstellen** (`module_kpi_no_show.R:845`)

Ersetze in `get_no_show_data()`, „verantwortlicher"-Zweig:
```r
      no_show_data <- module_external_events_helpers$get_responsible_event_pool(con) %>%
```
durch:
```r
      no_show_data <- module_external_events_helpers$get_responsible_event_pool_unified(con) %>%
```
(Die `select(...)`-Zeile darunter bleibt unverändert — dasselbe Schema.)

- [ ] **Step 3: Parse-Check + Commit**

Run: `Rscript -e 'invisible(parse("func/module_sales_kpi/external_events_helpers.R")); invisible(parse("func/module_sales_kpi/module_kpi_no_show.R")); cat("parse OK\n")'`
```bash
git add func/module_sales_kpi/external_events_helpers.R func/module_sales_kpi/module_kpi_no_show.R
git commit -m "feat(no-show): verantwortlicher-Ansicht liest aus sales_meetings_unified"
```

---

### Task 6: Integrations-Verifikation + Match-Kalibrierung (studyflix_local)

**Files:** keine (Verifikation).

- [ ] **Step 1: Producer lokal ausführen** (Positron, dbconnectorR via `load_all` vom Task-Branch)

```r
pkgload::load_all("C:/Users/HEMM036/Github/packages/dbconnectorR-wt-sales-unified")
library(Billomatics); library(dplyr)
keys <- Billomatics::authentication_process(c("postgresql"), commandArgs(trailingOnly=TRUE))
con <- Billomatics::postgres_connect(
  needed_tables = c("processed.msgraph_extern_event_classification","mapping.msgraph_call_event",
    "mapping.crm_lead_msgraph_contact","raw.crm_lead_tasks","raw.crm_lead_task_comments",
    "raw.crm_users","raw.msgraph_contacts","processed.sales_meetings_unified"),
  postgres_keys = keys$postgresql, update_local_tables = FALSE)
on.exit(pool::poolClose(con), add = TRUE)
n <- dbconnectorR::update_sales_meetings_unified(con); cat("Zeilen:", n, "\n")
```
Expected: `n` > 0.

- [ ] **Step 2: Match-Kalibrierung (Eindeutigkeitsrate) + Effekt messen** (SQL)

```sql
-- Wie viele Zeilen kommen aus Override / crm_only / msgraph?
SELECT no_show_source, count(*) FROM processed.sales_meetings_unified GROUP BY no_show_source;
-- Wie viele No-Shows insgesamt (unified) vs. nur MSGraph?
SELECT
  count(*) FILTER (WHERE is_no_show)                          AS no_shows_unified,
  count(*) FILTER (WHERE is_no_show AND source='msgraph')     AS no_shows_msgraph_basis,
  count(*) FILTER (WHERE no_show_source='crm_override')       AS ueberschrieben
FROM processed.sales_meetings_unified WHERE excluded = false;
```
Erwartung/Entscheidung: Ist die Override-Zahl plausibel klein und crm_only sinnvoll groß? Falls `crm_override` auffällig hoch oder Mehrdeutigkeit stört → im Match zusätzlich Rep (`contact_id`) aufnehmen (in `assemble_unified_meetings` den `ms_key`/Match um `contact_id` erweitern) und Task-2-Tests ergänzen.

- [ ] **Step 3: App-Smoke-Test** (No-Show-Tab aus dem #199-Standalone-Muster, gegen studyflix_local): No-Show-Rate rendert, Zahl liegt plausibel über dem alten MSGraph-only-Stand; keine Errors.

- [ ] **Step 4: Ergebnis dokumentieren** (kurzer Kommentar am Asana-Ticket mit den Zahlen aus Step 2).

---

## Self-Review

- **Spec-Abdeckung:** Tabelle (Task 1), Producer + Match/Override/Gap-Fill (Task 2/3), Rohdaten-Quelle ohne Anti-Join (Task 3), Pipeline (Task 4), No-Show-Konsum (Task 5), voll rückwirkend = Rebuild (Task 3), Scope nur No-Show = nur „verantwortlicher"-Zweig umgestellt (Task 5), Reversibilität (DROP TABLE + Zeile zurück). Externe-Tool-Regel + Mehrdeutigkeit in Task 2 getestet. ✅
- **Offene Detailpunkte aus der Spec:** Match-Schlüssel wird in Task 6/Step 2 an echten Daten kalibriert (Default lead+Tag, ggf. + Rep). VC-Tool-Tab bleibt vorerst unverändert (liest weiter `source='crm_task'`, existiert weiter) — bewusst nicht im Scope dieses Plans.
- **Typkonsistenz:** `assemble_unified_meetings(msgraph_meetings, crm_meetings)` — Spaltennamen in Task 2/3 identisch; Rückgabe-Schema = Tabellenspalten Task 1; Unified-Pool (Task 5) liefert exakt das `get_responsible_event_pool`-Schema.
