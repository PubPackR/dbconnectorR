mk_msgraph <- function() data.frame(
  call_event_mapping_id = c(10L, 11L, 12L),
  event_id             = c("e10", "e11", "e12"),
  event_date           = as.Date(c("2026-07-01", "2026-07-02", "2026-07-03")),
  event_start          = as.POSIXct(c("2026-07-01 09:00","2026-07-02 10:00","2026-07-03 11:00"), tz="UTC"),
  contact_id           = c(500L, 500L, 600L),
  lead_id              = c(10L, 20L, 30L),
  is_no_show           = c(FALSE, FALSE, TRUE),
  original_created_at  = as.POSIXct(c("2026-06-01","2026-06-02","2026-06-03"), tz="UTC"),
  excluded             = FALSE, is_short_lived_event = FALSE, is_responsible = TRUE,
  stringsAsFactors = FALSE
)
mk_crm <- function(rows) do.call(rbind, rows)
crm_row <- function(id, lead, date, tool, status, ext, rep = 500L, ptime = NA_character_,
                    type = "unbekannt") data.frame(
  crm_task_id=id, lead_id=lead, event_date=as.Date(date),
  precise_time=as.POSIXct(ptime, tz="UTC"), contact_id=rep,
  meeting_tool=tool, meeting_type=type, meeting_status=status, is_external_tool=ext,
  original_created_at=as.POSIXct("2026-06-15", tz="UTC"), stringsAsFactors=FALSE)

test_that("nicht-extern eindeutiger Match: CRM-No-Show ueberschreibt MSGraph", {
  # lead 10 / 2026-07-01 -> genau ein MSGraph-Termin, war FALSE
  res <- assemble_unified_meetings(mk_msgraph(),
           mk_crm(list(crm_row(1, 10L, "2026-07-01", "teams", "no_show", FALSE))))
  r <- res[res$meeting_key == "msgraph_10_10", ]
  expect_true(r$is_no_show)
  expect_equal(r$no_show_source, "crm_override")
  expect_equal(r$meeting_tool, "teams")
})

test_that("CRM show_up ueberschreibt MSGraph is_no_show=TRUE -> FALSE", {
  res <- assemble_unified_meetings(mk_msgraph(),
           mk_crm(list(crm_row(2, 30L, "2026-07-03", "teams", "show_up", FALSE))))
  r <- res[res$meeting_key == "msgraph_12_30", ]
  expect_false(r$is_no_show)
  expect_equal(r$no_show_source, "crm_override")
})

test_that("nicht-extern mehrdeutig (gleicher Rep, keine Zeit): CRM-Zeile wird verworfen", {
  ms <- mk_msgraph()
  ms <- rbind(ms, transform(ms[1,], call_event_mapping_id=99L, event_id="e99"))  # 2. Termin lead 10 / 2026-07-01, Rep 500
  res <- assemble_unified_meetings(ms,
           mk_crm(list(crm_row(3, 10L, "2026-07-01", "teams", "no_show", FALSE))))
  expect_false(any(res$no_show_source == "crm_override"))      # kein Override
  expect_false(any(res$meeting_key == "crm_3"))                # kein Netto-neu
})

test_that("Tiebreak: 2 Meetings (Lead, Tag) -> CRM matcht den gleichen Rep", {
  ms <- mk_msgraph()
  ms <- rbind(ms, transform(ms[1,], call_event_mapping_id=98L, event_id="e98", contact_id=600L))  # anderer Rep
  res <- assemble_unified_meetings(ms,
           mk_crm(list(crm_row(20, 10L, "2026-07-01", "teams", "no_show", FALSE, rep=500L))))
  expect_equal(res[res$meeting_key=="msgraph_10_10", ]$no_show_source, "crm_override")  # Rep 500
  expect_equal(res[res$meeting_key=="msgraph_98_10", ]$no_show_source, "msgraph")       # Rep 600 unberuehrt
})

test_that("Tiebreak: gleicher Rep, 2 Zeiten -> naechste event_start gewinnt", {
  ms <- mk_msgraph()
  ms <- rbind(ms, transform(ms[1,], call_event_mapping_id=97L, event_id="e97",
                            event_start=as.POSIXct("2026-07-01 15:00", tz="UTC")))  # gleicher Rep, spaeter
  res <- assemble_unified_meetings(ms,
           mk_crm(list(crm_row(21, 10L, "2026-07-01", "teams", "no_show", FALSE,
                               rep=500L, ptime="2026-07-01 14:30"))))               # naeher an 15:00
  expect_equal(res[res$meeting_key=="msgraph_97_10", ]$no_show_source, "crm_override")
  expect_equal(res[res$meeting_key=="msgraph_10_10", ]$no_show_source, "msgraph")
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
  expect_equal(res[res$meeting_key == "msgraph_10_10", ]$no_show_source, "msgraph")  # nicht ueberschrieben
  expect_equal(res[res$meeting_key == "crm_5", ]$source, "crm_task")                 # eigener Termin
})

test_that("CRM unbekannt (extern): zaehlt als stattgefunden, kein Ausschluss", {
  # Entscheidung vom 05.09.2026: ein CRM-Termin ohne dokumentierten Ausgang ist
  # ein stattgefundener Termin ohne Doku, kein No-Show. Vorher trug die Zeile
  # excluded=TRUE / is_no_show=NA und wirkte damit rechnerisch wie ein No-Show:
  # im Nenner der Show-Up-Quote, nie im Zaehler.
  res <- assemble_unified_meetings(mk_msgraph(),
           mk_crm(list(crm_row(6, 77L, "2026-08-02", "webex", "unbekannt", TRUE))))
  r <- res[res$meeting_key == "crm_6", ]
  expect_false(r$excluded)
  expect_false(r$is_no_show)
  expect_true(is.na(r$exclusion_reason))
  # Die Beobachtung bleibt lesbar: dass nichts dokumentiert ist, steht weiter
  # auf der Zeile. Nur ihre Auslegung hat sich geaendert.
  expect_equal(r$meeting_status, "unbekannt")
  expect_equal(r$no_show_source, "crm_only")
})

test_that("CRM storniert (extern): bleibt ausgeschlossen", {
  # Gegenprobe zur Zeile darueber: die Lockerung gilt nur fuer "unbekannt".
  # Eine rechtzeitige Absage ist kein No-Show, aber auch kein stattgefundener
  # Termin, und faellt deshalb weiter aus Zaehler UND Nenner.
  res <- assemble_unified_meetings(mk_msgraph(),
           mk_crm(list(crm_row(6, 77L, "2026-08-02", "webex", "storniert", TRUE))))
  r <- res[res$meeting_key == "crm_6", ]
  expect_true(r$excluded)
  expect_equal(r$exclusion_reason, "crm_storniert")
  # Kein Ausgang gemessen: eine Absage sagt nichts ueber Anwesenheit.
  expect_true(is.na(r$is_no_show))
})

test_that("meeting_key eindeutig (keine Doppelzaehlung)", {
  res <- assemble_unified_meetings(mk_msgraph(),
           mk_crm(list(crm_row(7, 77L, "2026-08-03", "zoom", "show_up", TRUE))))
  expect_equal(anyDuplicated(res$meeting_key), 0L)
})

test_that("Override + storniert: MSGraph-Termin wird excluded", {
  # lead 20 / 2026-07-02 -> genau ein MSGraph-Termin
  res <- assemble_unified_meetings(mk_msgraph(),
           mk_crm(list(crm_row(8, 20L, "2026-07-02", "teams", "storniert", FALSE))))
  r <- res[res$meeting_key == "msgraph_11_20", ]
  expect_true(r$excluded)
  expect_equal(r$no_show_source, "crm_override")
})

# Fixture fuer die join_url-Regel: Zeile wurde gegen Graph geprueft, sonst sagt
# ein leeres Feld ohnehin nichts.
mk_msgraph_link <- function(url = NA_character_,
                            checked = as.POSIXct("2026-09-09 12:00:00", tz = "UTC")) {
  ms <- mk_msgraph()
  ms$join_url <- url
  ms$join_url_checked_at <- checked
  ms
}
# Die eine CRM-Zeile, die alle Faelle hier ausloest: lead 30 / 2026-07-03 ->
# genau ein MSGraph-Termin (msgraph_12_30), is_no_show=TRUE, excluded=FALSE.
crm_unbekannt <- function() mk_crm(list(
  crm_row(9, 30L, "2026-07-03", "teams", "unbekannt", FALSE)))

test_that("Override + unbekannt MIT Teams-Link: MSGraph-Messung bleibt stehen", {
  # Mit Beitrittslink konnte ein Anwesenheitsbericht entstehen; ihn ein fehlender
  # CRM-Kommentar ueberschreiben zu lassen, hiesse Messung durch Auslegung ersetzen.
  res <- assemble_unified_meetings(
    mk_msgraph_link("https://teams.microsoft.com/l/meetup-join/x"), crm_unbekannt())
  r <- res[res$meeting_key == "msgraph_12_30", ]
  expect_true(r$is_no_show)
  expect_false(r$excluded)
  expect_equal(r$meeting_tool, "teams")
})

test_that("Override + unbekannt OHNE Teams-Link: kein No-Show aus fehlendem Call", {
  # Ohne Beitrittslink konnte es keinen Anwesenheitsbericht geben. is_no_show
  # traegt dort nur "kein Call gefunden" — eine Beobachtungsgrenze, keine
  # Messung. Dann gilt der CRM-Termin ohne Doku als stattgefunden (ADR 0015).
  res <- assemble_unified_meetings(mk_msgraph_link(NA_character_), crm_unbekannt())
  r <- res[res$meeting_key == "msgraph_12_30", ]
  expect_false(r$is_no_show)
  expect_false(r$excluded)
  expect_equal(r$no_show_source, "crm_override")
  expect_equal(r$meeting_status, "unbekannt")
})

test_that("Nie gegen Graph geprueft: kein Override aus einem leeren Feld", {
  # join_url_checked_at NULL heisst "nie danach gefragt". Ein leeres join_url
  # ist dann keine Aussage, sondern eine Beobachtungsluecke. Ohne diese Schranke
  # wuerde jede Zeile aus der Zeit vor dem Backfill faelschlich drehen.
  res <- assemble_unified_meetings(
    mk_msgraph_link(NA_character_, as.POSIXct(NA, tz = "UTC")), crm_unbekannt())
  expect_true(res[res$meeting_key == "msgraph_12_30", ]$is_no_show)
})

test_that("Geprueft und trotzdem ohne Link: genau hier greift die Regel", {
  # Die Gegenprobe zum Test darueber - identische Zeile, nur mit gesetzter
  # Marke. Dass allein das Setzen von join_url_checked_at das Ergebnis dreht,
  # ist der ganze Punkt der Umstellung.
  res <- assemble_unified_meetings(
    mk_msgraph_link(NA_character_, as.POSIXct("2026-09-09 12:00:00", tz = "UTC")),
    crm_unbekannt())
  expect_false(res[res$meeting_key == "msgraph_12_30", ]$is_no_show)
})

test_that("Caller ohne die neuen Spalten: Override greift nicht, Bestand unveraendert", {
  # Neutraler Fallback. Ein Caller, der die Spalten nicht kennt, darf keine
  # Anwesenheitsdaten drehen — auch nicht still.
  res <- assemble_unified_meetings(mk_msgraph(), crm_unbekannt())
  expect_true(res[res$meeting_key == "msgraph_12_30", ]$is_no_show)
})

test_that("Leerer String als join_url zaehlt als kein Link", {
  res <- assemble_unified_meetings(mk_msgraph_link("  "), crm_unbekannt())
  expect_false(res[res$meeting_key == "msgraph_12_30", ]$is_no_show)
})

test_that("Keine Event-Zeile (event_start NA): kein Override aus Nichtwissen", {
  # join_url ist hier NA, weil der left_join auf raw.msgraph_events nichts fand,
  # nicht weil es keinen Link gab. Daraus darf keine Anwesenheitsaussage werden.
  ms <- mk_msgraph_link(NA_character_)
  ms$event_start[3] <- as.POSIXct(NA, tz = "UTC")
  res <- assemble_unified_meetings(ms, crm_unbekannt())
  expect_true(res[res$meeting_key == "msgraph_12_30", ]$is_no_show)
})

test_that("Platzhalter-Guard: CRM-Zeile mit lead_id=NA matcht keine NA-lead-MSGraph-Zeile", {
  # msgraph-Fixture mit einer Platzhalter-Zeile (lead_id NA) am selben Datum wie die CRM-Zeile
  ms_na <- mk_msgraph()
  ms_na <- rbind(ms_na, transform(ms_na[1, ],
                                   call_event_mapping_id = 13L, event_id = "e13",
                                   lead_id = NA_integer_, event_date = as.Date("2026-08-04")))
  cm <- crm_row(10, NA_integer_, "2026-08-04", "teams", "no_show", FALSE)
  res <- assemble_unified_meetings(ms_na, mk_crm(list(cm)))

  crm_r <- res[res$meeting_key == "crm_10", ]
  expect_equal(crm_r$source, "crm_task")
  expect_equal(crm_r$no_show_source, "crm_only")

  ms_r <- res[res$meeting_key == "msgraph_13_NA", ]
  expect_equal(ms_r$no_show_source, "msgraph")  # Platzhalter nicht ueberschrieben
})

test_that("meeting_type wandert auf die netto-neue CRM-Zeile", {
  res <- assemble_unified_meetings(mk_msgraph(),
           mk_crm(list(crm_row(40, 99L, "2026-07-09", "webex", "show_up", TRUE, type = "nv"))))
  expect_equal(res[res$meeting_key == "crm_40", ]$meeting_type, "nv")
})

test_that("meeting_type wird beim Override auf die MSGraph-Zeile geschrieben", {
  res <- assemble_unified_meetings(mk_msgraph(),
           mk_crm(list(crm_row(41, 10L, "2026-07-01", "teams", "no_show", FALSE, type = "uc"))))
  r <- res[res$meeting_key == "msgraph_10_10", ]
  expect_equal(r$meeting_type, "uc")
  expect_equal(r$no_show_source, "crm_override")
})

test_that("MSGraph-Zeilen ohne CRM-Pendant behalten meeting_type NA", {
  leer <- crm_row(0, 1L, "2026-07-01", "teams", "unbekannt", FALSE)[0, ]
  res  <- assemble_unified_meetings(mk_msgraph(), leer)
  expect_true(all(is.na(res$meeting_type)))
})

# --- Ersatzregel: MSGraph-Meeting ohne ableitbaren Lead --------------------------
# Spec docs/specs/2026-10-29-ersatzregel-nur-rest.md: nur bis 15 Minuten Abstand,
# Vergabe nach Abstand, die Zeile uebernimmt den Lead des Tasks.
ms_platzhalter <- function(rep = 500L, start = "2026-08-10 09:00") {
  ms <- mk_msgraph()
  rbind(ms, transform(ms[1, ], call_event_mapping_id = 50L, event_id = "e50",
                      lead_id = NA_integer_, contact_id = rep,
                      event_date = as.Date("2026-08-10"),
                      event_start = as.POSIXct(start, tz = "UTC")))
}
ph_task <- function(id, lead, ptime, rep = 500L, status = "no_show")
  crm_row(id, lead, "2026-08-10", "teams", status, FALSE, rep = rep, ptime = ptime)
sortiert <- function(res) {
  res <- res[order(res$meeting_key), ]
  rownames(res) <- NULL
  res
}

test_that("Platzhalter desselben Reps bis 15 Minuten wird gematcht und uebernimmt den Task-Lead", {
  res <- assemble_unified_meetings(ms_platzhalter(),
           mk_crm(list(ph_task(60, 42L, "2026-08-10 09:10"))))
  expect_false(any(res$meeting_key == "crm_60"))                       # keine zweite Zeile
  expect_false(any(res$meeting_key == "msgraph_50_NA"))
  r <- res[res$meeting_key == "msgraph_50_42", ]
  expect_equal(nrow(r), 1)
  expect_equal(r$lead_id, "42")
  expect_true(r$is_no_show)
  expect_equal(r$no_show_source, "crm_override")
  expect_equal(r$event_id, "e50")
})

test_that("genau 15 Minuten Abstand matcht noch", {
  res <- assemble_unified_meetings(ms_platzhalter(),
           mk_crm(list(ph_task(64, 42L, "2026-08-10 08:45"))))
  expect_true(any(res$meeting_key == "msgraph_50_42"))
  expect_false(any(res$meeting_key == "crm_64"))
})

test_that("Task mit 16 Minuten Abstand bleibt netto-neu, der Platzhalter unberuehrt", {
  res <- assemble_unified_meetings(ms_platzhalter(),
           mk_crm(list(ph_task(65, 42L, "2026-08-10 09:16"))))
  expect_equal(res[res$meeting_key == "crm_65", ]$source, "crm_task")
  expect_equal(res[res$meeting_key == "msgraph_50_NA", ]$no_show_source, "msgraph")
})

test_that("ohne precise_time greift die Ersatzregel nicht, auch bei nur einem Platzhalter", {
  res <- assemble_unified_meetings(ms_platzhalter(),
           mk_crm(list(ph_task(66, 42L, NA_character_))))
  expect_equal(res[res$meeting_key == "crm_66", ]$source, "crm_task")
  expect_equal(res[res$meeting_key == "msgraph_50_NA", ]$no_show_source, "msgraph")
})

test_that("Platzhalter eines anderen Reps wird nicht gematcht", {
  res <- assemble_unified_meetings(ms_platzhalter(rep = 600L),
           mk_crm(list(ph_task(61, 42L, "2026-08-10 09:00"))))
  expect_equal(res[res$meeting_key == "crm_61", ]$source, "crm_task")   # netto-neu
  expect_equal(res[res$meeting_key == "msgraph_50_NA", ]$no_show_source, "msgraph")
})

test_that("mehrere Platzhalter: naechste event_start zur precise_time gewinnt", {
  ms <- ms_platzhalter(start = "2026-08-10 09:00")
  ms <- rbind(ms, transform(ms[nrow(ms), ], call_event_mapping_id = 51L, event_id = "e51",
                            event_start = as.POSIXct("2026-08-10 09:20", tz = "UTC")))
  res <- assemble_unified_meetings(ms, mk_crm(list(ph_task(62, 42L, "2026-08-10 09:12"))))
  expect_equal(res[res$meeting_key == "msgraph_51_42", ]$no_show_source, "crm_override")
  expect_equal(res[res$meeting_key == "msgraph_50_NA", ]$no_show_source, "msgraph")
  expect_false(any(res$meeting_key == "crm_62"))
})

test_that("zwei Platzhalter im gleichen Abstand: CRM-Zeile bleibt netto-neu statt zu raten", {
  ms <- ms_platzhalter()
  ms <- rbind(ms, transform(ms[nrow(ms), ], call_event_mapping_id = 51L, event_id = "e51"))
  res <- assemble_unified_meetings(ms, mk_crm(list(ph_task(63, 42L, "2026-08-10 09:05"))))
  expect_equal(res[res$meeting_key == "crm_63", ]$source, "crm_task")
  expect_true(all(c("msgraph_50_NA", "msgraph_51_NA") %in% res$meeting_key))
})

test_that("ein Platzhalter nimmt hoechstens einen CRM-Termin auf, der naechste gewinnt", {
  # Zwei CRM-Termine desselben Reps am selben Tag, verschiedene Leads, aber nur
  # eine Platzhalter-Zeile: aus zwei Meetings darf nicht eines werden.
  res <- assemble_unified_meetings(ms_platzhalter(),
           mk_crm(list(ph_task(70, 42L, "2026-08-10 09:10"),
                       ph_task(71, 43L, "2026-08-10 09:03"))))
  expect_equal(res[res$meeting_key == "msgraph_50_43", ]$no_show_source, "crm_override")
  expect_equal(res[res$meeting_key == "crm_70", ]$source, "crm_task")   # fernerer bleibt erhalten
  expect_equal(sum(res$event_date == as.Date("2026-08-10")), 2L)        # zwei Meetings, zwei Zeilen
})

test_that("die Reihenfolge der Tasks aendert das Ergebnis nicht", {
  # P1 09:00, P2 09:20. Task A 09:08 (8 / 12 min), Task B 09:05 (5 / 15 min).
  # Das naechste Paar ist B-P1, danach bleibt A-P2. Wer A zuerst abarbeitet und
  # ihm den naechsten Platzhalter gibt, schiebt B auf P2 und vertauscht die Leads.
  ms <- ms_platzhalter(start = "2026-08-10 09:00")
  ms <- rbind(ms, transform(ms[nrow(ms), ], call_event_mapping_id = 51L, event_id = "e51",
                            event_start = as.POSIXct("2026-08-10 09:20", tz = "UTC")))
  a <- ph_task(72, 42L, "2026-08-10 09:08")
  b <- ph_task(73, 43L, "2026-08-10 09:05")
  vor  <- assemble_unified_meetings(ms, mk_crm(list(a, b)))
  nach <- assemble_unified_meetings(ms, mk_crm(list(b, a)))
  expect_equal(sortiert(vor), sortiert(nach))
  expect_true(all(c("msgraph_50_43", "msgraph_51_42") %in% vor$meeting_key))
  expect_false(any(vor$source == "crm_task"))
})

test_that("gleicher Abstand zweier Tasks: die kleinere crm_task_id gewinnt, in jeder Reihenfolge", {
  a <- ph_task(75, 42L, "2026-08-10 09:05")
  b <- ph_task(74, 43L, "2026-08-10 08:55")
  vor  <- assemble_unified_meetings(ms_platzhalter(), mk_crm(list(a, b)))
  nach <- assemble_unified_meetings(ms_platzhalter(), mk_crm(list(b, a)))
  expect_equal(sortiert(vor), sortiert(nach))
  expect_true("msgraph_50_43" %in% vor$meeting_key)
  expect_equal(vor[vor$meeting_key == "crm_75", ]$source, "crm_task")
})

test_that("ein Task mit Lead x Tag-Treffer verbraucht keinen Platzhalter", {
  # Task 76 findet sein Meeting ueber den Lead; der Platzhalter bleibt fuer Task 77.
  ms <- rbind(ms_platzhalter(),
              transform(mk_msgraph()[1, ], call_event_mapping_id = 52L, event_id = "e52",
                        lead_id = 44L, event_date = as.Date("2026-08-10"),
                        event_start = as.POSIXct("2026-08-10 09:00", tz = "UTC")))
  res <- assemble_unified_meetings(ms, mk_crm(list(ph_task(76, 44L, "2026-08-10 09:00"),
                                                   ph_task(77, 45L, "2026-08-10 09:01"))))
  expect_equal(res[res$meeting_key == "msgraph_52_44", ]$no_show_source, "crm_override")
  expect_equal(res[res$meeting_key == "msgraph_50_45", ]$no_show_source, "crm_override")
  expect_false(any(res$source == "crm_task"))
})

test_that("Ersatzregel traegt integer64-IDs aus der DB", {
  ms <- ms_platzhalter()
  ms$call_event_mapping_id <- bit64::as.integer64(c(10, 11, 12, 4000000001))
  ms$contact_id <- bit64::as.integer64(ms$contact_id)
  cm <- mk_crm(list(ph_task(60, 42L, "2026-08-10 09:10"), ph_task(61, 43L, "2026-08-10 09:12")))
  cm$crm_task_id <- bit64::as.integer64(c(4000000061, 4000000060))
  cm$lead_id     <- bit64::as.integer64(c(4000000042, 43))
  cm$contact_id  <- bit64::as.integer64(cm$contact_id)
  res <- assemble_unified_meetings(ms, cm)
  expect_true("msgraph_4000000001_4000000042" %in% res$meeting_key)
  expect_equal(res[res$meeting_key == "crm_4000000060", ]$source, "crm_task")
})

test_that("Organisator wird uebernommen und je Herkunft unterscheidbar gehalten", {
  ms <- mk_msgraph()
  # Meeting 10 hat einen Organisator (SDR-Fall: 700 legt fuer Rep 500),
  # Meeting 11 keine klassifizierte Organisator-Zeile.
  ms$organizer_contact_id <- c("700", NA_character_, "600")
  res <- assemble_unified_meetings(ms, mk_crm(list(
    # Externes Tool -> netto-neue CRM-Zeile, kann keinen Organisator tragen.
    crm_row(80, 99L, "2026-07-05", "zoom", "show_up", TRUE))))

  expect_equal(res[res$meeting_key == "msgraph_10_10", ]$organizer_contact_id, "700")
  expect_equal(res[res$meeting_key == "msgraph_10_10", ]$organizer_source, "msgraph")
  expect_true(is.na(res[res$meeting_key == "msgraph_11_20", ]$organizer_contact_id))
  expect_equal(res[res$meeting_key == "msgraph_11_20", ]$organizer_source, "unbekannt")
  expect_true(is.na(res[res$meeting_key == "crm_80", ]$organizer_contact_id))
  expect_equal(res[res$meeting_key == "crm_80", ]$organizer_source, "crm_task")
})

test_that("fehlende Organisator-Spalte bricht den Aufbau nicht", {
  # Ein Caller ohne die neue Spalte bekommt "unbekannt", nicht einen Fehler.
  res <- assemble_unified_meetings(mk_msgraph(), mk_crm(list(
    crm_row(81, 10L, "2026-07-01", "teams", "show_up", FALSE))))
  expect_true(all(res$organizer_source == "unbekannt"))
  expect_true(all(is.na(res$organizer_contact_id)))
})

test_that("Ausschlussgrund wird durchgereicht und beim CRM-Storno gesetzt", {
  ms <- mk_msgraph()
  ms$exclusion_reason <- c("alt_tenant_join_url", NA_character_, NA_character_)
  res <- assemble_unified_meetings(ms, mk_crm(list(
    crm_row(90, 20L, "2026-07-02", "teams", "storniert", FALSE))))

  # Beobachtbarkeitsgrund bleibt lesbar, statt hinter excluded zu verschwinden.
  expect_equal(res[res$meeting_key == "msgraph_10_10", ]$exclusion_reason,
               "alt_tenant_join_url")
  # Storno-Override setzt Grund und Flag gemeinsam.
  ueberschrieben <- res[res$meeting_key == "msgraph_11_20", ]
  expect_true(ueberschrieben$excluded)
  expect_equal(ueberschrieben$exclusion_reason, "crm_storniert")
})

# Fixture fuer den Alt-Tenant-Fall: msgraph_12_30 stammt aus dem Alt-Tenant. Wie
# in echt traegt es einen Teams-Link und is_no_show = TRUE, weil nie ein Call
# ankommen konnte.
mk_msgraph_alt_tenant <- function(reason = "alt_tenant_join_url") {
  ms <- mk_msgraph_link("https://teams.microsoft.com/l/meetup-join/alt")
  ms$excluded[3] <- TRUE
  ms$exclusion_reason <- c(NA_character_, NA_character_, reason)
  ms
}
alt_tenant_mit_crm <- function(status) {
  res <- assemble_unified_meetings(mk_msgraph_alt_tenant(), mk_crm(list(
    crm_row(95, 30L, "2026-07-03", "teams", status, FALSE, rep = 600L))))
  res[res$meeting_key == "msgraph_12_30", ]
}

test_that("Alt-Tenant + CRM show_up: Termin ist nicht mehr ausgeschlossen", {
  # Ohne den Kalender-Match waere derselbe Task eine crm_only-Zeile und zaehlte
  # als VC. Der Match darf den Beleg nicht loeschen.
  r <- alt_tenant_mit_crm("show_up")
  expect_false(r$excluded)
  expect_true(is.na(r$exclusion_reason))
  expect_false(r$is_no_show)
  expect_equal(r$no_show_source, "crm_override")
})

test_that("Alt-Tenant + CRM unbekannt: stattgefunden, obwohl ein Link existiert", {
  # Der Link stammt aus dem Alt-Tenant, ein Anwesenheitsbericht ist app-only
  # nicht abrufbar. Das ist dieselbe Beobachtungsgrenze wie ein fehlender Link
  # (ADR 0015), und dann traegt der CRM-Termin ohne Doku.
  r <- alt_tenant_mit_crm("unbekannt")
  expect_false(r$excluded)
  expect_true(is.na(r$exclusion_reason))
  expect_false(r$is_no_show)
  expect_equal(r$meeting_status, "unbekannt")
})

test_that("Alt-Tenant + CRM no_show: zaehlt als No-Show statt ausgeschlossen", {
  r <- alt_tenant_mit_crm("no_show")
  expect_false(r$excluded)
  expect_true(is.na(r$exclusion_reason))
  expect_true(r$is_no_show)
})

test_that("Alt-Tenant + CRM storniert: bleibt ausgeschlossen, Grund wird crm_storniert", {
  r <- alt_tenant_mit_crm("storniert")
  expect_true(r$excluded)
  expect_equal(r$exclusion_reason, "crm_storniert")
})

test_that("Nur der Alt-Tenant-Grund wird aufgehoben, Identitaetsgruende bleiben", {
  # Ein Duplikat oder die abgesagte Haelfte eines verlegten Termins ist kein
  # eigener Termin. Ein CRM-Beleg aendert daran nichts.
  for (grund in c("duplikat_event", "verschoben", "termin_in_zukunft")) {
    res <- assemble_unified_meetings(mk_msgraph_alt_tenant(grund), mk_crm(list(
      crm_row(96, 30L, "2026-07-03", "teams", "show_up", FALSE, rep = 600L))))
    r <- res[res$meeting_key == "msgraph_12_30", ]
    expect_true(r$excluded, info = grund)
    expect_equal(r$exclusion_reason, grund, info = grund)
  }
})

test_that("Alt-Tenant + zwei CRM-Tasks: ein Storno bleibt in jeder Reihenfolge stehen", {
  # Beide Tasks landen auf derselben Zeile (gleicher Lead, gleicher Tag, gleicher
  # Rep). Ein spaeterer show_up darf die Absage nicht wieder aufheben.
  storno  <- crm_row(98, 30L, "2026-07-03", "teams", "storniert", FALSE, rep = 600L)
  show_up <- crm_row(99, 30L, "2026-07-03", "teams", "show_up",   FALSE, rep = 600L)
  for (reihenfolge in list(list(storno, show_up), list(show_up, storno))) {
    res <- assemble_unified_meetings(mk_msgraph_alt_tenant(), mk_crm(reihenfolge))
    r <- res[res$meeting_key == "msgraph_12_30", ]
    expect_true(r$excluded)
    expect_equal(r$exclusion_reason, "crm_storniert")
  }
})

test_that("Alt-Tenant mit zwei Leads: nur die Zeile des gematchten Leads wird gerettet", {
  ms <- mk_msgraph_alt_tenant()
  ms <- rbind(ms, transform(ms[3, ], lead_id = 31L))  # zweiter Lead desselben Meetings
  res <- assemble_unified_meetings(ms, mk_crm(list(
    crm_row(97, 30L, "2026-07-03", "teams", "show_up", FALSE, rep = 600L))))
  expect_false(res[res$meeting_key == "msgraph_12_30", ]$excluded)
  andere <- res[res$meeting_key == "msgraph_12_31", ]
  expect_true(andere$excluded)
  expect_equal(andere$exclusion_reason, "alt_tenant_join_url")
  expect_equal(andere$no_show_source, "msgraph")
})

# online_meeting_nicht_abrufbar: Link aus dem eigenen Tenant, Microsoft gibt das
# Online-Meeting aber nicht heraus (nicht gefunden, 403, Organisator unbekannt).
# Gleiche Lage wie beim Alt-Tenant, gleiche Regel.
nicht_abrufbar_mit_crm <- function(status) {
  res <- assemble_unified_meetings(mk_msgraph_alt_tenant("online_meeting_nicht_abrufbar"),
                                   mk_crm(list(crm_row(95, 30L, "2026-07-03", "teams", status,
                                                       FALSE, rep = 600L))))
  res[res$meeting_key == "msgraph_12_30", ]
}

test_that("Nicht abrufbar + CRM show_up: Termin ist nicht mehr ausgeschlossen", {
  r <- nicht_abrufbar_mit_crm("show_up")
  expect_false(r$excluded)
  expect_true(is.na(r$exclusion_reason))
  expect_false(r$is_no_show)
  expect_equal(r$no_show_source, "crm_override")
})

test_that("Nicht abrufbar + CRM unbekannt: stattgefunden, obwohl ein Link existiert", {
  r <- nicht_abrufbar_mit_crm("unbekannt")
  expect_false(r$excluded)
  expect_true(is.na(r$exclusion_reason))
  expect_false(r$is_no_show)
  expect_equal(r$meeting_status, "unbekannt")
})

test_that("Nicht abrufbar + CRM no_show: zaehlt als No-Show statt ausgeschlossen", {
  r <- nicht_abrufbar_mit_crm("no_show")
  expect_false(r$excluded)
  expect_true(is.na(r$exclusion_reason))
  expect_true(r$is_no_show)
})

test_that("Nicht abrufbar + CRM storniert: bleibt ausgeschlossen, Grund wird crm_storniert", {
  r <- nicht_abrufbar_mit_crm("storniert")
  expect_true(r$excluded)
  expect_equal(r$exclusion_reason, "crm_storniert")
})

test_that("Nicht abrufbar + zwei CRM-Tasks: ein Storno bleibt in jeder Reihenfolge stehen", {
  storno  <- crm_row(98, 30L, "2026-07-03", "teams", "storniert", FALSE, rep = 600L)
  show_up <- crm_row(99, 30L, "2026-07-03", "teams", "show_up",   FALSE, rep = 600L)
  for (reihenfolge in list(list(storno, show_up), list(show_up, storno))) {
    res <- assemble_unified_meetings(mk_msgraph_alt_tenant("online_meeting_nicht_abrufbar"),
                                     mk_crm(reihenfolge))
    r <- res[res$meeting_key == "msgraph_12_30", ]
    expect_true(r$excluded)
    expect_equal(r$exclusion_reason, "crm_storniert")
  }
})

test_that("Nicht abrufbar ohne CRM-Task: Ausschluss und Grund bleiben stehen", {
  res <- assemble_unified_meetings(mk_msgraph_alt_tenant("online_meeting_nicht_abrufbar"),
                                   mk_crm(list(crm_row(97, 10L, "2026-07-01", "teams",
                                                       "show_up", FALSE))))
  r <- res[res$meeting_key == "msgraph_12_30", ]
  expect_true(r$excluded)
  expect_equal(r$exclusion_reason, "online_meeting_nicht_abrufbar")
  expect_equal(r$no_show_source, "msgraph")
})

test_that("Netto-neue CRM-Zeile traegt den Grund nur wenn sie ausgeschlossen ist", {
  res <- assemble_unified_meetings(mk_msgraph(), mk_crm(list(
    crm_row(91, 99L, "2026-07-05", "zoom", "storniert", TRUE),
    crm_row(92, 98L, "2026-07-05", "zoom", "show_up", TRUE))))

  expect_equal(res[res$meeting_key == "crm_91", ]$exclusion_reason, "crm_storniert")
  expect_true(is.na(res[res$meeting_key == "crm_92", ]$exclusion_reason))
})

# --- Domain-Fallback: Leadless meeting ueber eindeutige Domain -----------------
# Spec docs/specs/2026-10-28-domain-fallback-lead.md, Reihenfolge seit
# docs/specs/2026-10-29-ersatzregel-nur-rest.md: der Domain-Lead entsteht mit der
# Lead-Ableitung, also vor dem CRM-Task-Abgleich.
mk_tn <- function(cemid, contact, email) data.frame(
  call_event_mapping_id = cemid, contact_id = contact, email = email,
  stringsAsFactors = FALSE)
keine_maps <- function() data.frame(msgraph_contact_id = numeric(0), crm_lead_id = numeric(0))
mk_mails <- function() data.frame(
  lead_id           = c(77L, 88L, 89L, 90L, 91L),
  mail_address_name = c(" Anna@Firma-A.de ", "x@firma-b.de", "y@firma-b.de",
                        "kunde@gmail.com", "z@firma-c.de"),
  is_deleted        = c(FALSE, FALSE, FALSE, FALSE, TRUE),
  stringsAsFactors  = FALSE)
paare <- function(res) paste(res$call_event_mapping_id, res$lead_id, res$quelle)

test_that("Domain-Leads: nur Domains mit genau einem Lead, ohne Freemail und Geloeschte", {
  dl <- domain_leads_aus_crm_mails(mk_mails())
  expect_equal(dl$domain, "firma-a.de")   # firma-b: zwei Leads, gmail: Freemail, firma-c: geloescht
  expect_equal(as.character(dl$lead_id), "77")
})

test_that("Meeting ohne Mapping bekommt den Lead seiner eindeutigen Domain", {
  res <- meeting_leads_ableiten(mk_tn(40, 1, "max@FIRMA-A.de"), keine_maps(), mk_mails())
  expect_equal(paare(res), "40 77 domain")
})

test_that("Freemail, Domain mit mehreren Leads und interne Teilnehmer loesen nichts aus", {
  tn <- rbind(mk_tn(40, 1, "max@gmail.com"), mk_tn(41, 2, "q@firma-b.de"),
              mk_tn(42, 3, "chef@studyflix.de"))
  res <- meeting_leads_ableiten(tn, keine_maps(), mk_mails())
  expect_equal(nrow(res), 0)
})

test_that("ein Mapping eines Teilnehmers hat Vorrang, auch ein nicht-primaeres", {
  tn <- rbind(mk_tn(40, 1, "max@firma-a.de"), mk_tn(40, 2, "bekannt@irgendwo.de"))
  maps <- data.frame(msgraph_contact_id = 2, crm_lead_id = 500)
  res <- meeting_leads_ableiten(tn, maps, mk_mails())
  expect_equal(paare(res), "40 500 mapping")
})

test_that("zwei eindeutige Domains in einem Meeting ergeben einen Lead je Domain", {
  mails <- rbind(mk_mails(), data.frame(lead_id = 95L, mail_address_name = "k@firma-d.de",
                                        is_deleted = FALSE, stringsAsFactors = FALSE))
  tn <- rbind(mk_tn(40, 1, "max@firma-a.de"), mk_tn(40, 3, "eva@firma-d.de"))
  res <- meeting_leads_ableiten(tn, keine_maps(), mails)
  expect_setequal(paare(res), c("40 77 domain", "40 95 domain"))
})

test_that("Domain-Lead vor dem Abgleich: der CRM-Task findet das Meeting ueber Lead x Tag", {
  # Der Task des Domain-Leads matcht direkt, ohne Ersatzregel und auch ueber
  # 15 Minuten Abstand hinweg. Vorher lief der Domain-Fallback erst nach dem
  # Abgleich, und der Task war netto-neu.
  ms <- transform(mk_msgraph()[1, ], call_event_mapping_id = 40L, event_id = "e40",
                  event_date = as.Date("2026-08-10"),
                  event_start = as.POSIXct("2026-08-10 09:00", tz = "UTC"))
  ms$lead_id <- NULL
  leads <- meeting_leads_ableiten(mk_tn(40L, 1, "max@firma-a.de"), keine_maps(), mk_mails())
  ms <- merge(ms, leads[, c("call_event_mapping_id", "lead_id")],
              by = "call_event_mapping_id", all.x = TRUE)
  cm <- mk_crm(list(crm_row(60, 77L, "2026-08-10", "teams", "show_up", FALSE,
                            ptime = "2026-08-10 11:00")))
  res <- assemble_unified_meetings(ms, cm)
  expect_equal(res$meeting_key, "msgraph_40_77")
  expect_equal(res$no_show_source, "crm_override")
})

test_that("Lead-Ableitung mit Domain traegt integer64-IDs aus der DB und Teilnehmer ohne Mail", {
  tn <- data.frame(call_event_mapping_id = bit64::as.integer64(c(100000, 4000000001, 7)),
                   contact_id = bit64::as.integer64(c(7, 8, 9)),
                   email = c("max@firma-a.de", NA, "bekannt@irgendwo.de"),
                   stringsAsFactors = FALSE)
  maps  <- data.frame(msgraph_contact_id = bit64::as.integer64(9),
                      crm_lead_id = bit64::as.integer64(4000000042))
  mails <- transform(mk_mails(), lead_id = bit64::as.integer64(lead_id))
  res <- meeting_leads_ableiten(tn, maps, mails)
  expect_setequal(paare(res), c("100000 77 domain", "7 4000000042 mapping"))
  expect_type(res$lead_id, "character")
})

# ---- Leads je Meeting aus externen Teilnehmern (alle Mail-Mappings) ----------

mk_teilnehmer <- function() data.frame(
  call_event_mapping_id = c(1, 1, 2, 3, 4),
  contact_id            = c(10, 11, 20, 30, 40),
  email = c("anna@kunde.de", "ben@kunde.de", "chef@studyflix.de",
            "gast@external.guest", "cara@andere.de"),
  stringsAsFactors = FALSE)

test_that("jedes Mapping liefert einen Lead, die Primaer-Markierung spielt keine Rolle", {
  # 11 steht im CRM nur als nachrangige Adresse von Lead 501; die Funktion sieht
  # keine is_primary_crm-Spalte und darf sie auch nicht brauchen.
  maps <- data.frame(msgraph_contact_id = c(10, 11), crm_lead_id = c(500, 501))
  res <- meeting_leads_aus_teilnehmern(mk_teilnehmer(), maps)
  expect_setequal(paste(res$call_event_mapping_id, res$lead_id), c("1 500", "1 501"))
})

test_that("interne und synthetische Teilnehmer liefern keinen Lead", {
  maps <- data.frame(msgraph_contact_id = c(20, 30), crm_lead_id = c(600, 700))
  res <- meeting_leads_aus_teilnehmern(mk_teilnehmer(), maps)
  expect_equal(nrow(res), 0)
})

test_that("zwei Teilnehmer desselben Leads ergeben eine Zeile", {
  maps <- data.frame(msgraph_contact_id = c(10, 11, 40), crm_lead_id = c(500, 500, 800))
  res <- meeting_leads_aus_teilnehmern(mk_teilnehmer(), maps)
  expect_equal(nrow(res), 2)
  expect_setequal(paste(res$call_event_mapping_id, res$lead_id), c("1 500", "4 800"))
})

test_that("Lead-Ableitung traegt integer64-IDs aus der DB", {
  tn <- mk_teilnehmer()
  tn$call_event_mapping_id <- bit64::as.integer64(tn$call_event_mapping_id)
  tn$contact_id <- bit64::as.integer64(tn$contact_id)
  maps <- data.frame(msgraph_contact_id = bit64::as.integer64(c(10, 40)),
                     crm_lead_id = bit64::as.integer64(c(4000000001, 800)))
  res <- meeting_leads_aus_teilnehmern(tn, maps)
  expect_setequal(paste(res$call_event_mapping_id, res$lead_id), c("1 4000000001", "4 800"))
})
