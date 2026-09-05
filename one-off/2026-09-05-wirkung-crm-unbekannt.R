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
#   RESTMENGEN BLEIBEN DRAUSSEN. Gezaehlt wird nur `restmenge IS NULL`, also
#   dieselbe Menge, die auch in `shiny.sales_kpi_termine` landet
#   (`kpi_sales_zusammengesetzt.R`). Ueber alle Restmengen zu summieren wuerde
#   die Wirkung ueberschaetzen: CRM-only-Zeilen tragen keinen Organizer und
#   landen ueberdurchschnittlich oft in einer Restmenge, stehen also gar nicht
#   in der Quote, die Sales im Dashboard sieht.
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

## ----- Lookup: Grund, Herkunft und Badge je Meeting ----- ##
# `lade_termin_ereignisse()` selektiert `exclusion_reason` weg, `meeting_key`
# bleibt. Der Lookup holt ihn zurueck und bringt den `task_badge` gleich mit.
#
# Der Badge-Join laeuft bewusst in SQL ueber `'crm_' || t.crm_task_id` und
# nicht in R: `crm_task_id` ist bigint, und `as.character()` auf einem als
# double zurueckgekommenen bigint liefert "2.3628741e+07" statt "23628741".
# Der Match schluege dann auf ALLEN Zeilen fehl, der Badge-Vorgriff wuerde
# saemtliche crm_task-Zeilen entfernen und die Wirkung als null ausweisen,
# ohne dass ein Fehler auftritt. Postgres castet bigint nach text korrekt.
# Dieselbe integer64-Falle ist in crm_task_meeting_classification.R vermerkt.

lookup <- DBI::dbGetQuery(con, "
  SELECT DISTINCT
         smu.meeting_key,
         smu.exclusion_reason,
         smu.meeting_status,
         smu.source,
         t.task_badge
    FROM processed.sales_meetings_unified smu
    LEFT JOIN raw.crm_lead_tasks t
      ON t.is_deleted = FALSE
     AND smu.meeting_key = 'crm_' || t.crm_task_id
")

# Fan-out waere hier ein stiller Fehler: traegt eine crm_task_id zwei Zeilen mit
# verschiedenem Badge, stuende dasselbe Meeting doppelt in der Quote.
stopifnot(!anyDuplicated(lookup$meeting_key))

ereignisse <- left_join(ereignisse, lookup, by = "meeting_key")

## ----- Badge-Vorgriff: Nicht-visit-CRM-Zeilen entfernen ----- ##

ist_crm  <- ereignisse$source %in% "crm_task"
vorher_n <- sum(ist_crm)

ereignisse <- ereignisse[!ist_crm | ereignisse$task_badge %in% "visit", , drop = FALSE]

entfernt <- vorher_n - sum(ereignisse$source %in% "crm_task")
message("Badge-Vorgriff: ", entfernt, " von ", vorher_n,
        " crm_task-Zeilen entfernt.")
# Alles oder nichts entfernt heisst: der Join hat nicht gegriffen, nicht dass
# der Badge so sauber trennt.
stopifnot(entfernt > 0, entfernt < vorher_n)

## ----- Alte und neue Lesart ----- ##

ereignisse_neu <- ereignisse
ereignisse_neu$stattgefunden <- ereignisse_neu$stattgefunden |
  (ereignisse_neu$exclusion_reason %in% "crm_unbekannt" & ereignisse_neu$bewertbar)

zusammenziehen <- function(ereignisse, name) {
  kpiR::kpi_sales_termine_showup(con, START_MONAT, END_MONAT,
                                 ereignisse = ereignisse) %>%
    # Dieselbe Menge wie shiny.sales_kpi_termine, siehe Kopf.
    filter(is.na(restmenge)) %>%
    group_by(monat) %>%
    summarise(nenner = sum(n_termine_gelegt_bewertbar),
              !!name := sum(n_termine_gelegt_stattgefunden),
              .groups = "drop")
}

alt <- zusammenziehen(ereignisse, "zaehler_alt")
neu <- zusammenziehen(ereignisse_neu, "zaehler_neu")

# Der Nenner darf sich zwischen den beiden Laeufen NICHT unterscheiden. Tut er
# es doch, wurde die Lesart nicht nur im Zaehler geaendert und keine Zeile der
# Tabelle unten ist brauchbar. Deshalb VOR der Ausgabe.
stopifnot(identical(alt$monat, neu$monat), identical(alt$nenner, neu$nenner))

## ----- Ergebnis ----- ##

ergebnis <- alt %>%
  inner_join(select(neu, monat, zaehler_neu), by = "monat") %>%
  mutate(
    quote_alt = zaehler_alt / nenner * 100,
    quote_neu = zaehler_neu / nenner * 100,
    # Erst die Differenz, dann runden. Aus gerundeten Quoten gerechnet weicht
    # sie um bis zu 0,1 Prozentpunkte ab, und genau diese Spanne wird berichtet.
    delta_pp  = round(quote_neu - quote_alt, 1),
    quote_alt = round(quote_alt, 1),
    quote_neu = round(quote_neu, 1),
    Monat     = format(monat, "%m.%Y")
  ) %>%
  arrange(monat) %>%
  select(Monat, nenner, zaehler_alt, zaehler_neu, quote_alt, quote_neu, delta_pp)

print(as.data.frame(ergebnis), row.names = FALSE)

cat("\nGesamt ueber den Zeitraum:\n")
cat("  Nenner      ", sum(ergebnis$nenner), "\n")
cat("  Zaehler alt ", sum(ergebnis$zaehler_alt),
    sprintf(" (%.1f %%)", sum(ergebnis$zaehler_alt) / sum(ergebnis$nenner) * 100), "\n")
cat("  Zaehler neu ", sum(ergebnis$zaehler_neu),
    sprintf(" (%.1f %%)", sum(ergebnis$zaehler_neu) / sum(ergebnis$nenner) * 100), "\n")
cat("  Spanne der Monatsdifferenz: ",
    min(ergebnis$delta_pp), " bis ", max(ergebnis$delta_pp), " Prozentpunkte\n")
