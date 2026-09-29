# Tests fuer get_tasks_from_leads(): die Personen-Zuordnung muss aus der Task
# selbst kommen, nicht vom Lead oder von einer anderen Task desselben Leads.
# Fall: ein Lead wird an einen neuen SDR uebergeben, danach tauchten seine
# alten Tasks bei diesem auf. IDs sind frei gewaehlt.

mk_task_df <- function(id, user_id, finished) {
  data.frame(
    id                 = id,
    user_id            = user_id,
    finished           = finished,
    badge              = "visit",
    comments_count     = 0L,
    name               = paste("Task", id),
    precise_time       = "2025-09-24T15:00:00.000+02:00",
    created_by_user_id = user_id,
    updated_by_user_id = user_id,
    created_at         = "2025-09-20T10:00:00.000+02:00",
    updated_at         = "2025-09-24T16:00:00.000+02:00",
    stringsAsFactors   = FALSE
  )
}

mk_leads <- function() {
  tibble::tibble(
    id      = c(10L, 20L),
    user_id = c(900L, 900L),   # heutiger Lead-Owner (neuer SDR)
    tasks   = list(
      mk_task_df(c(1001L, 1002L, 1003L), c(101L, 900L, 102L), c(TRUE, FALSE, FALSE)),
      mk_task_df(c(2001L, 2002L), c(103L, 104L), c(TRUE, TRUE))
    ),
    tasks_pending = list(
      mk_task_df(c(1002L, 1003L), c(900L, 102L), c(FALSE, FALSE)),
      list()                    # Lead ohne offene Task
    )
  )
}

assignee_of <- function(tasks, lead, task) {
  tasks$assigned_to_user_id[tasks$lead_id == lead & tasks$crm_task_id == task]
}

test_that("erledigte Tasks behalten ihre eigene Person, auch wenn der Lead eine offene Task hat", {
  tasks <- get_tasks_from_leads(mk_leads())

  expect_equal(assignee_of(tasks, 10L, 1001L), 101L)
})

test_that("erledigte Tasks eines Leads ohne offene Task sind nicht NA", {
  tasks <- get_tasks_from_leads(mk_leads())

  expect_equal(assignee_of(tasks, 20L, 2001L), 103L)
  expect_equal(assignee_of(tasks, 20L, 2002L), 104L)
})

test_that("offene Tasks behalten ihre Person", {
  tasks <- get_tasks_from_leads(mk_leads())

  expect_equal(assignee_of(tasks, 10L, 1002L), 900L)
  expect_equal(assignee_of(tasks, 10L, 1003L), 102L)
})

test_that("jede Task steht genau einmal im Ergebnis", {
  tasks <- get_tasks_from_leads(mk_leads())

  expect_equal(nrow(tasks), 5L)
  expect_false(any(duplicated(tasks[, c("lead_id", "crm_task_id")])))
})

test_that("user_id bleibt der Lead-Owner (Scope dieses Fixes)", {
  tasks <- get_tasks_from_leads(mk_leads())

  expect_true(all(tasks$user_id == 900L))
})
