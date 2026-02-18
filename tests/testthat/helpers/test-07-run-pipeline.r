test_that("run_pipeline sources scripts in sequence", {
  local_options(fao.run_pipeline.auto = FALSE)
  source(here::here("R/run_pipeline.R"), local = TRUE)

  source_calls <- character()

  mock_source <- function(file, echo = FALSE) {
    source_calls <<- c(source_calls, file)
    invisible(NULL)
  }

  with_mocked_bindings(
    source = mock_source,
    .env = environment(run_pipeline),
    {
      result <- run_pipeline(show_view = FALSE, pipeline_root = "R")
    }
  )

  expect_true(isTRUE(result))
  expect_equal(
    basename(source_calls),
    c("run_general_pipeline.R", "run_import_pipeline.R", "run_export_pipeline.R")
  )
})

test_that("run_pipeline validates pipeline_root existence", {
  local_options(fao.run_pipeline.auto = FALSE)
  source(here::here("R/run_pipeline.R"), local = TRUE)

  expect_error(
    run_pipeline(show_view = FALSE, pipeline_root = "missing-folder"),
    "pipeline root does not exist"
  )
})

test_that("run_pipeline keeps backward-compatible defaults", {
  local_options(fao.run_pipeline.auto = FALSE)
  source(here::here("R/run_pipeline.R"), local = TRUE)

  run_pipeline_formals <- formals(run_pipeline)

  expect_true(identical(run_pipeline_formals$show_view, quote(interactive())))
  expect_true(identical(run_pipeline_formals$pipeline_root, quote(here::here("R"))))
})
