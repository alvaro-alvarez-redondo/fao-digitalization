test_that("run_pipeline sources scripts and executes stage runners in sequence", {
  local_options(fao.run_pipeline.auto = FALSE)
  source(here::here("R/run_pipeline.R"), local = TRUE)

  source_calls <- character()

  mock_source <- function(file, echo = FALSE) {
    source_calls <<- c(source_calls, file)
    invisible(NULL)
  }

  stage_calls <- character()

  with_mocked_bindings(
    source = mock_source,
    run_general_pipeline = function(dataset_name = "fao_data_raw") {
      stage_calls <<- c(stage_calls, "general")
      list(paths = list(data = list(imports = list(raw = tempdir()))))
    },
    run_import_pipeline = function(config) {
      stage_calls <<- c(stage_calls, "import")
      list(data = data.frame(country = "argentina"), diagnostics = list())
    },
    run_export_pipeline = function(fao_data_raw, config, overwrite = TRUE) {
      stage_calls <<- c(stage_calls, "export")
      list(processed_path = "processed.xlsx", lists_path = "lists.xlsx")
    },
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
  expect_identical(stage_calls, c("general", "import", "export"))
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


test_that("run_pipeline aborts when sourced runner functions are missing", {
  local_options(fao.run_pipeline.auto = FALSE)
  source(here::here("R/run_pipeline.R"), local = TRUE)

  with_mocked_bindings(
    source = function(file, echo = FALSE) invisible(NULL),
    exists = function(x, where = -1, mode = "any", inherits = FALSE) FALSE,
    .env = environment(run_pipeline),
    {
      expect_error(
        run_pipeline(show_view = FALSE, pipeline_root = "R"),
        "required pipeline runner functions are unavailable"
      )
    }
  )
})


test_that("run_pipeline assigns legacy global objects", {
  local_options(fao.run_pipeline.auto = FALSE)
  source(here::here("R/run_pipeline.R"), local = TRUE)

  with_mocked_bindings(
    source = function(file, echo = FALSE) invisible(NULL),
    run_general_pipeline = function(dataset_name = "fao_data_raw") {
      list(paths = list(data = list(imports = list(raw = tempdir()))))
    },
    run_import_pipeline = function(config) {
      list(
        data = data.frame(country = "argentina"),
        wide_raw = data.frame(country = "argentina"),
        diagnostics = list(
          reading_errors = character(0),
          validation_errors = character(0),
          warnings = character(0)
        )
      )
    },
    run_export_pipeline = function(fao_data_raw, config, overwrite = TRUE) {
      list(processed_path = "processed.xlsx", lists_path = "lists.xlsx")
    },
    .env = environment(run_pipeline),
    {
      run_pipeline(show_view = FALSE, pipeline_root = "R")
    }
  )

  expect_true(exists("config", envir = .GlobalEnv, inherits = FALSE))
  expect_true(exists("import_pipeline_result", envir = .GlobalEnv, inherits = FALSE))
  expect_true(exists("fao_data_raw", envir = .GlobalEnv, inherits = FALSE))
  expect_true(exists("fao_data_wide_raw", envir = .GlobalEnv, inherits = FALSE))
  expect_true(exists("collected_reading_errors", envir = .GlobalEnv, inherits = FALSE))
  expect_true(exists("collected_errors", envir = .GlobalEnv, inherits = FALSE))
  expect_true(exists("collected_warnings", envir = .GlobalEnv, inherits = FALSE))
  expect_true(exists("export_paths", envir = .GlobalEnv, inherits = FALSE))

  rm(
    "config",
    "import_pipeline_result",
    "fao_data_raw",
    "fao_data_wide_raw",
    "collected_reading_errors",
    "collected_errors",
    "collected_warnings",
    "export_paths",
    envir = .GlobalEnv
  )
})
