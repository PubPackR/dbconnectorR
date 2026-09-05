# ============================================================================ #
# Wirkung: crm_unbekannt als stattgefundenen Termin zaehlen
# ============================================================================ #
#
# Description
#
#   Misst, um wie viel die Show-Up-Quote je Monat steigt, wenn ein CRM-Termin
#   ohne dokumentierten Ausgang (`exclusion_reason = 'crm_unbekannt'`) als
#   stattgefunden statt als No-Show gezaehlt wird.
#   Asana 1218192760112154, Spec docs/specs/2026-09-05-crm-unbekannt-als-stattgefunden.md
#
#   Rechnet NICHT die Quotenkette nach, sondern nimmt die Produktionsfunktion
#   `kpiR::kpi_sales_termine_showup()` und laesst sie zweimal ueber dieselbe
#   Ereignismenge laufen: einmal unveraendert, einmal mit umgestelltem
#   `stattgefunden`. Der Nenner (`bewertbar`) aendert sich dabei nicht, weil
#   `crm_unbekannt` nie in `ist_beobachtbarer_termin()` stand.
#
#   BADGE-VORGRIFF. PR 43 (task_badge == 'visit') ist auf main, aber noch nicht
#   installiert; die Produktionstabelle enthaelt also noch CRM-Zeilen, die gar
#   keine Termine sind. Das Skript nimmt den Filter vorweg, damit die Zahl die
#   ist, die nach der Installation im Dashboard steht.
#
#   Nicht vorweggenommen wird der Badge-Effekt auf MSGraph-Zeilen
#   (crm_override): 22 Zeilen in 15 Monaten, gemessen in PR 43.
#
#   Aufruf: in Positron im Repo dbconnectorR sourcen.

# ---------------------------------------------------------------------------- #
# Settings ----
# ---------------------------------------------------------------------------- #

## ----- libraries -----
library(dplyr)
library(kpiR)

## ----- constants -----
START_MONAT <- as.Date("2025-09-01")
END_MONAT   <- as.Date("2026-08-01")

## ----- data -----
args <- commandArgs(trailingOnly = TRUE)
keys <- Billomatics::authentication_process(c("postgresql"), args)

con <- Billomatics::postgres_connect(
  needed_tables = c(
    "processed.sales_meetings_unified",
    "raw.crm_lead_tasks",
    "raw.crm_lead_tags",
    "raw.msgraph_contacts",
    "raw.msgraph_users",
    "raw.personio_persons",
    "raw.personio_person_position_history",
    "mapping.vw_service_users",
    "mapping.user_aliases"
  ),
  postgres_keys = keys$postgresql,
  update_local_tables = TRUE
)
on.exit(pool::poolClose(con), add = TRUE)

# ---------------------------------------------------------------------------- #
# Start ----
# ---------------------------------------------------------------------------- #

## ----- Ereignisse einmal laden ----- ##

ereignisse <- kpiR:::lade_termin_ereignisse(con, START_MONAT, END_MONAT)

## ----- Lookup: Grund und Herkunft je Meeting ----- ##
# `lade_termin_ereignisse()` selektiert `exclusion_reason` weg, `meeting_key`
# bleibt. Der Lookup holt ihn zurueck. Fan-out waere hier ein stiller Fehler,
# deshalb die Eindeutigkeitspruefung statt eines blinden left_join.

lookup <- tbl(con, I("processed.sales_meetings_unified")) %>%
  select(meeting_key, exclusion_reason, meeting_status, source) %>%
  distinct() %>%
  collect()

stopifnot(!anyDuplicated(lookup$meeting_key))

ereignisse <- left_join(ereignisse, lookup, by = "meeting_key")

## ----- Badge-Vorgriff: Nicht-visit-CRM-Zeilen entfernen ----- ##
# meeting_key der CRM-Zeilen ist "crm_<crm_task_id>" (sales_meetings_unified.R).

visit_ids <- tbl(con, I("raw.crm_lead_tasks")) %>%
  filter(is_deleted == FALSE, task_badge == "visit") %>%
  distinct(crm_task_id) %>%
  collect() %>%
  pull(crm_task_id) %>%
  as.character()

ist_crm <- ereignisse$source %in% "crm_task"
crm_id  <- sub("^crm_", "", ereignisse$meeting_key)

vorher_n <- sum(ist_crm)
ereignisse <- ereignisse[!ist_crm | crm_id %in% visit_ids, , drop = FALSE]
message("Badge-Vorgriff: ", vorher_n - sum(ereignisse$source %in% "crm_task"),
        " von ", vorher_n, " crm_task-Zeilen entfernt.")

## ----- Alte und neue Lesart ----- ##

ereignisse_neu <- ereignisse
ereignisse_neu$stattgefunden <- ereignisse_neu$stattgefunden |
  (ereignisse_neu$exclusion_reason %in% "crm_unbekannt" & ereignisse_neu$bewertbar)

zusammenziehen <- function(ereignisse, name) {
  kpiR::kpi_sales_termine_showup(con, START_MONAT, END_MONAT,
                                 ereignisse = ereignisse) %>%
    group_by(monat) %>%
    summarise(nenner = sum(n_termine_gelegt_bewertbar),
              !!name := sum(n_termine_gelegt_stattgefunden),
              .groups = "drop")
}

alt <- zusammenziehen(ereignisse, "zaehler_alt")
neu <- zusammenziehen(ereignisse_neu, "zaehler_neu")

## ----- Ergebnis ----- ##

ergebnis <- alt %>%
  inner_join(select(neu, monat, zaehler_neu), by = "monat") %>%
  mutate(
    quote_alt = round(zaehler_alt / nenner * 100, 1),
    quote_neu = round(zaehler_neu / nenner * 100, 1),
    delta_pp  = round(quote_neu - quote_alt, 1),
    Monat     = format(monat, "%m.%Y")
  ) %>%
  arrange(monat) %>%
  select(Monat, nenner, zaehler_alt, zaehler_neu, quote_alt, quote_neu, delta_pp)

print(as.data.frame(ergebnis), row.names = FALSE)

# Der Nenner darf sich zwischen den beiden Laeufen NICHT unterscheiden. Tut er
# es doch, wurde die Lesart nicht nur im Zaehler geaendert.
stopifnot(identical(alt$nenner, neu$nenner))

cat("\nGesamt ueber den Zeitraum:\n")
cat("  Nenner      ", sum(ergebnis$nenner), "\n")
cat("  Zaehler alt ", sum(ergebnis$zaehler_alt),
    sprintf(" (%.1f %%)", sum(ergebnis$zaehler_alt) / sum(ergebnis$nenner) * 100), "\n")
cat("  Zaehler neu ", sum(ergebnis$zaehler_neu),
    sprintf(" (%.1f %%)", sum(ergebnis$zaehler_neu) / sum(ergebnis$nenner) * 100), "\n")
cat("  Spanne der Monatsdifferenz: ",
    min(ergebnis$delta_pp), " bis ", max(ergebnis$delta_pp), " Prozentpunkte\n")
