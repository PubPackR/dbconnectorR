# CRM-Task-Meetings — Phase 1 (Parser + Anti-Join) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reine, unit-getestete R-Funktionen in dbconnectorR, die aus CRM-Task-Freitext das VC-Tool und den Meeting-Status extrahieren und CRM-Meetings gegen bestehende MSGraph-Meetings anti-joinen — ohne jeden DB-Zugriff.

**Architecture:** Alle Funktionen sind pur (Input = Strings / data.frames, Output = Werte / gefilterte data.frames), damit die fehleranfällige Freitext-Logik isoliert testbar ist. Die DB-Anbindung, Identitätsauflösung und der Schreibpfad kommen erst in Phase 2 (eigener Plan nach diesem Checkpoint).

**Tech Stack:** R, dplyr, stringr, testthat (Package dbconnectorR).

**Spec:** `docs/superpowers/specs/2026-07-24-crm-task-meetings-in-event-classification-design.md`, §5.1 (Parser) und §5.3 (Anti-Join).

## Global Constraints

- Package-Root: `C:\Users\HEMM036\Github\packages\dbconnectorR`, Branch `feat/crm-task-meetings-classification`.
- Neue Funktionen: roxygen2-Doku (`#' @param`, `#' @return`, `#' @export` nur wo öffentlich) im Stil bestehender `R/*.R`.
- Kein `<<-`, kein Überschreiben über den globalen Scope.
- `stringr` nur auf lokalen Vektoren (kein dbplyr) — hier unkritisch, alles ist in-memory.
- Tool-Kanon (lowercase Tokens): `webex`, `zoom`, `google_meet`, `skype`, `teams`, `unbekannt`.
- Status-Kanon: `show_up`, `no_show`, `storniert`, `unbekannt`.
- Externe Tools (für `is_external_tool`): `webex`, `zoom`, `google_meet`, `skype` (NICHT `teams`).
- Tests laufen via PowerShell + Rscript (R-Stack in git-bash instabil).

## File Structure

- Create: `R/crm_task_meeting_parser.R` — alle reinen Funktionen dieser Phase: `is_vc_task()`, `extract_meeting_tool()`, `is_external_tool()`, `classify_meeting_status()`, `filter_new_crm_meetings()`.
- Create: `tests/testthat/test-crm_task_meeting_parser.R` — Unit-Tests gegen echte Beispielstrings aus der Discovery.

---

### Task 1: VC-Erkennung + Tool-Extraktion

**Files:**
- Create: `R/crm_task_meeting_parser.R`
- Test: `tests/testthat/test-crm_task_meeting_parser.R`

**Interfaces:**
- Produces:
  - `is_vc_task(task_name: character) -> logical` — TRUE, wenn Marker `\bVC\b` (case-insensitive) ODER ein **externes** Tool-Keyword (webex/zoom/skype/google meet) vorkommt. Ein blosses „teams" ohne VC-Marker ist KEIN VC-Termin (killt den „geplante Teams Sitzungen"-False-Positive).
  - `extract_meeting_tool(task_name: character) -> character` — priorisiert `webex` > `zoom` > `google_meet` > `skype` > `teams` > `unbekannt`.
  - `is_external_tool(tool: character) -> logical` — TRUE für webex/zoom/google_meet/skype.

- [ ] **Step 1: Write the failing test**

Create `tests/testthat/test-crm_task_meeting_parser.R`:

```r
# Tests fuer den reinen CRM-Task-Meeting-Parser (Spec 2026-07-24, §5.1/§5.3).
# Beispielstrings stammen aus der echten Discovery auf raw.crm_lead_tasks.

test_that("is_vc_task erkennt VC-Marker und externe Tools, aber nicht blosses 'Teams'", {
  expect_true(is_vc_task("VC Webex ZR mit Frings und Knechtges"))
  expect_true(is_vc_task("Nur ueber Zoom VC UC"))
  expect_true(is_vc_task("VC UC (WEBEX)"))
  expect_true(is_vc_task("WEBEX EINLADUNG senden!!!"))          # externes Tool ohne VC-Marker
  # bewusst FALSE: 'Teams' als Praeferenz-Notiz, kein VC-Termin
  expect_false(is_vc_task("Telefonisch melden - findet Telefonate besser als geplante Teams Sitzungen"))
  expect_false(is_vc_task("er machen"))
  expect_false(is_vc_task(NA_character_))
})

test_that("extract_meeting_tool priorisiert korrekt und ist case-insensitiv", {
  expect_equal(extract_meeting_tool("VC Webex ZR"), "webex")
  expect_equal(extract_meeting_tool("VC UC (WEBEX)"), "webex")
  expect_equal(extract_meeting_tool("Nur ueber Zoom VC UC"), "zoom")
  expect_equal(extract_meeting_tool("VC Teams Update"), "teams")
  expect_equal(extract_meeting_tool("VC ZR mit Bronder"), "unbekannt")
  expect_equal(extract_meeting_tool(NA_character_), "unbekannt")
})

test_that("is_external_tool trennt externe Tools von Teams/unbekannt", {
  expect_true(is_external_tool("webex"))
  expect_true(is_external_tool("zoom"))
  expect_true(is_external_tool("google_meet"))
  expect_true(is_external_tool("skype"))
  expect_false(is_external_tool("teams"))
  expect_false(is_external_tool("unbekannt"))
})
```

- [ ] **Step 2: Run test to verify it fails**

Run (PowerShell):
```powershell
Rscript -e "devtools::load_all('.'); testthat::test_file('tests/testthat/test-crm_task_meeting_parser.R')"
```
Expected: FAIL — `could not find function "is_vc_task"`.

- [ ] **Step 3: Write minimal implementation**

Create `R/crm_task_meeting_parser.R`:

```r
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

#' Ist das Tool ein externes (nicht-Teams) VC-Tool?
#'
#' @param tool Character(-Vektor) aus [extract_meeting_tool()].
#' @return Logical(-Vektor).
#' @export
is_external_tool <- function(tool) {
  tool %in% c("webex", "zoom", "google_meet", "skype")
}
```

- [ ] **Step 4: Run test to verify it passes**

Run (PowerShell):
```powershell
Rscript -e "devtools::load_all('.'); testthat::test_file('tests/testthat/test-crm_task_meeting_parser.R')"
```
Expected: PASS (3 tests).

- [ ] **Step 5: Commit**

```bash
git add R/crm_task_meeting_parser.R tests/testthat/test-crm_task_meeting_parser.R
git commit -m "feat: VC-Erkennung + Tool-Extraktion fuer CRM-Task-Meetings"
```

---

### Task 2: Status-Klassifikation aus Kommentaren

**Files:**
- Modify: `R/crm_task_meeting_parser.R`
- Test: `tests/testthat/test-crm_task_meeting_parser.R`

**Interfaces:**
- Consumes: nichts aus Task 1.
- Produces:
  - `classify_meeting_status(comment_text: character) -> character` — priorisiert `storniert` > `no_show` > `show_up` > `unbekannt`. Arbeitet auf Kommentar-Text (NICHT Task-Namen).

- [ ] **Step 1: Write the failing test**

Append to `tests/testthat/test-crm_task_meeting_parser.R`:

```r
test_that("classify_meeting_status priorisiert Storno vor No-Show vor Show-Up", {
  expect_equal(classify_meeting_status("Termin wurde abgesagt"), "storniert")
  expect_equal(classify_meeting_status("verschoben auf naechste Woche"), "storniert")
  expect_equal(classify_meeting_status("Kunde ist nicht erschienen"), "no_show")
  expect_equal(classify_meeting_status("No Show, muss neu terminieren"), "no_show")
  expect_equal(classify_meeting_status("hat stattgefunden, lief gut"), "show_up")
  expect_equal(classify_meeting_status("Kunde war da, Show-Up"), "show_up")
  expect_equal(classify_meeting_status(""), "unbekannt")
  expect_equal(classify_meeting_status(NA_character_), "unbekannt")
})
```

- [ ] **Step 2: Run test to verify it fails**

Run (PowerShell):
```powershell
Rscript -e "devtools::load_all('.'); testthat::test_file('tests/testthat/test-crm_task_meeting_parser.R')"
```
Expected: FAIL — `could not find function "classify_meeting_status"`.

- [ ] **Step 3: Write minimal implementation**

Append to `R/crm_task_meeting_parser.R`:

```r
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
  x <- tolower(ifelse(is.na(comment_text), "", comment_text))
  dplyr::case_when(
    stringr::str_detect(x, "storn|abgesagt|verschoben|cancel")                      ~ "storniert",
    stringr::str_detect(x, "no ?show|nicht erschienen|nicht da|kam nicht|nicht aufgetaucht") ~ "no_show",
    stringr::str_detect(x, "show ?up|erschienen|stattgefunden|gehalten|durchgef|war da")     ~ "show_up",
    TRUE                                                                            ~ "unbekannt"
  )
}
```

- [ ] **Step 4: Run test to verify it passes**

Run (PowerShell):
```powershell
Rscript -e "devtools::load_all('.'); testthat::test_file('tests/testthat/test-crm_task_meeting_parser.R')"
```
Expected: PASS (4 tests).

- [ ] **Step 5: Commit**

```bash
git add R/crm_task_meeting_parser.R tests/testthat/test-crm_task_meeting_parser.R
git commit -m "feat: Status-Klassifikation (show_up/no_show/storniert) aus CRM-Kommentaren"
```

---

### Task 3: Anti-Join gegen bestehende MSGraph-Meetings

**Files:**
- Modify: `R/crm_task_meeting_parser.R`
- Test: `tests/testthat/test-crm_task_meeting_parser.R`

**Interfaces:**
- Consumes: `is_external_tool` (Spalte `is_external_tool` auf dem Input).
- Produces:
  - `filter_new_crm_meetings(crm_meetings: data.frame, msgraph_meetings: data.frame) -> data.frame` — behaelt eine CRM-Meeting-Zeile, wenn `is_external_tool == TRUE` ODER kein MSGraph-Meeting mit gleichem `lead_id` + `event_date` existiert.
  - `crm_meetings` muss die Spalten `lead_id`, `event_date`, `is_external_tool` enthalten; `msgraph_meetings` die Spalten `lead_id`, `event_date`.

- [ ] **Step 1: Write the failing test**

Append to `tests/testthat/test-crm_task_meeting_parser.R`:

```r
test_that("filter_new_crm_meetings behaelt externe Tools immer und Teams nur ohne MSGraph-Match", {
  crm <- data.frame(
    crm_task_id     = 1:4,
    lead_id         = c(10L, 20L, 30L, 40L),
    event_date      = as.Date(c("2026-07-20", "2026-07-20", "2026-07-21", "2026-07-22")),
    is_external_tool = c(TRUE, FALSE, FALSE, TRUE),
    stringsAsFactors = FALSE
  )
  msgraph <- data.frame(
    lead_id    = c(10L, 20L, 99L),
    event_date = as.Date(c("2026-07-20", "2026-07-20", "2026-07-20")),
    stringsAsFactors = FALSE
  )
  # task 1: extern + MSGraph-Match am selben Tag -> trotzdem BEHALTEN (extern immer)
  # task 2: teams + MSGraph-Match -> VERWERFEN
  # task 3: teams + kein Match     -> BEHALTEN
  # task 4: extern + kein Match    -> BEHALTEN
  res <- filter_new_crm_meetings(crm, msgraph)
  expect_equal(sort(res$crm_task_id), c(1L, 3L, 4L))
  # keine Hilfsspalte durchgereicht
  expect_false(".in_msgraph" %in% names(res))
})
```

- [ ] **Step 2: Run test to verify it fails**

Run (PowerShell):
```powershell
Rscript -e "devtools::load_all('.'); testthat::test_file('tests/testthat/test-crm_task_meeting_parser.R')"
```
Expected: FAIL — `could not find function "filter_new_crm_meetings"`.

- [ ] **Step 3: Write minimal implementation**

Append to `R/crm_task_meeting_parser.R`:

```r
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
```

- [ ] **Step 4: Run test to verify it passes**

Run (PowerShell):
```powershell
Rscript -e "devtools::load_all('.'); testthat::test_file('tests/testthat/test-crm_task_meeting_parser.R')"
```
Expected: PASS (5 tests).

- [ ] **Step 5: Commit**

```bash
git add R/crm_task_meeting_parser.R tests/testthat/test-crm_task_meeting_parser.R
git commit -m "feat: Anti-Join CRM-Meetings gegen bestehende MSGraph-Meetings"
```

---

### Task 4: Roxygen-Doku generieren + Full-Package-Testlauf

**Files:**
- Modify: `NAMESPACE`, `man/*.Rd` (generiert)

- [ ] **Step 1: Roxygenise**

Run (PowerShell):
```powershell
Rscript -e "devtools::document()"
```
Expected: neue `man/is_vc_task.Rd`, `man/extract_meeting_tool.Rd`, `man/is_external_tool.Rd`, `man/classify_meeting_status.Rd`, `man/filter_new_crm_meetings.Rd`; `NAMESPACE` um die fünf `export()` ergänzt.

- [ ] **Step 2: Voller Package-Testlauf (keine Regression)**

Run (PowerShell):
```powershell
Rscript -e "devtools::test()"
```
Expected: alle Tests PASS, inkl. der neuen Datei; keine bestehenden Tests gebrochen.

- [ ] **Step 3: Commit**

```bash
git add NAMESPACE man/
git commit -m "docs: roxygen fuer CRM-Task-Meeting-Parser-Funktionen"
```

---

## Self-Review

**Spec coverage (§5.1/§5.3):**
- Tool-Extraktion + Priorität → Task 1 ✅
- VC-Erkennung inkl. Teams-False-Positive-Mitigation → Task 1 ✅
- `is_external_tool` → Task 1 ✅
- Status-Klassifikation aus Kommentaren (Storno/No-Show/Show-Up-Präzedenz) → Task 2 ✅
- Anti-Join (extern immer, Teams nur ohne lead+Tag-Match) → Task 3 ✅
- Identitätsauflösung, DB-Laden, Schreibpfad → **bewusst NICHT hier** (Phase 2, eigener Plan).

**Placeholder scan:** keine TBD/TODO; jeder Code-Step enthält vollständigen Code.

**Type consistency:** Tool-Tokens (webex/zoom/google_meet/skype/teams/unbekannt) einheitlich in `extract_meeting_tool`, `is_external_tool`, Tests; Status-Tokens (show_up/no_show/storniert/unbekannt) einheitlich; `filter_new_crm_meetings` konsumiert `is_external_tool`-Spalte, die aus `is_external_tool(extract_meeting_tool(...))` entsteht (Assembly in Phase 2).

## Phase-2-Vorschau (eigener Plan nach diesem Checkpoint)

Build-Zeit-Verifikation (`vw_service_users` crm-Zweig), Schema-Migration, `update_crm_task_meeting_classification()` (Identitätsauflösung + Assembly + Full-Set-Upsert), Einhängen in `base-35/do/main.R`, Delta-Messung. Phase 3: Helper-LEFT-Join + neuer Sub-Tab.
