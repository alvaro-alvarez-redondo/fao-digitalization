testthat::test_that("source_import_scripts returns sourced paths", {
  source(here::here("R/1-import_pipeline/run_import_pipeline.R"), echo = FALSE)

  mock_source <- function(file, echo = FALSE) {
    invisible(file)
  }
  mock_assert_file_exists <- function(file, access = "r", extension = NULL) {
    invisible(TRUE)
  }

  sourced_paths <- testthat::with_mocked_bindings(
    source = mock_source,
    `checkmate::assert_file_exists` = mock_assert_file_exists,
    .env = environment(source_import_scripts),
    {
      source_import_scripts(c("10-file_io.R", "11-reading.R"))
    }
  )

  testthat::expect_type(sourced_paths, "character")
  testthat::expect_identical(
    basename(sourced_paths),
    c("10-file_io.R", "11-reading.R")
  )
})

testthat::test_that("source_export_scripts returns sourced paths", {
  source(here::here("R/3-export_pipeline/run_export_pipeline.R"), echo = FALSE)

  mock_source <- function(file, echo = FALSE) {
    invisible(file)
  }
  mock_assert_file_exists <- function(file, access = "r", extension = NULL) {
    invisible(TRUE)
  }

  sourced_paths <- testthat::with_mocked_bindings(
    source = mock_source,
    `checkmate::assert_file_exists` = mock_assert_file_exists,
    .env = environment(source_export_scripts),
    {
      source_export_scripts(c("30-data_audit.R", "31-export_data.R"))
    }
  )

  testthat::expect_type(sourced_paths, "character")
  testthat::expect_identical(
    basename(sourced_paths),
    c("30-data_audit.R", "31-export_data.R")
  )
})

testthat::test_that("run_import_pipeline happy path returns expected structure", {
  source(here::here("R/1-import_pipeline/run_import_pipeline.R"), echo = FALSE)

  config <- list(paths = list(data = list(imports = list(raw = tempdir()))))

  file_list_dt <- data.table::data.table(document = "doc.xlsx", file_path = "doc.xlsx")
  transformed_long <- data.table::data.table(
    document = "doc.xlsx",
    product = "rice",
    variable = "production",
    year = "2020",
    value = "1"
  )

  result <- testthat::with_mocked_bindings(
    source_import_scripts = function(script_names) invisible(script_names),
    discover_files = function(import_folder) file_list_dt,
    read_pipeline_files = function(file_list_dt, config) {
      list(read_data_list = list(data.frame(x = 1L)), errors = character(0))
    },
    transform_files_list = function(file_list_dt, read_data_list, config) {
      list(wide_raw = data.table::data.table(a = 1L), long_raw = transformed_long)
    },
    validate_long_dt = function(long_dt, config) {
      list(data = long_dt, errors = character(0))
    },
    consolidate_audited_dt = function(dt_list, config) {
      list(data = data.table::rbindlist(dt_list), warnings = character(0))
    },
    .env = environment(run_import_pipeline),
    {
      run_import_pipeline(config)
    }
  )

  testthat::expect_true(data.table::is.data.table(result$data))
  testthat::expect_true(data.table::is.data.table(result$wide_raw))
  testthat::expect_named(result$diagnostics, c("reading_errors", "validation_errors", "warnings"))
})



testthat::test_that("run_import_pipeline normalizes diagnostics to character vectors", {
  source(here::here("R/1-import_pipeline/run_import_pipeline.R"), echo = FALSE)

  config <- list(paths = list(data = list(imports = list(raw = tempdir()))))

  file_list_dt <- data.table::data.table(document = "doc.xlsx", file_path = "doc.xlsx")
  transformed_long <- data.table::data.table(
    document = "doc.xlsx",
    product = "rice",
    variable = "production",
    year = "2020",
    value = "1"
  )

  result <- testthat::with_mocked_bindings(
    source_import_scripts = function(script_names) invisible(script_names),
    discover_files = function(import_folder) file_list_dt,
    read_pipeline_files = function(file_list_dt, config) {
      list(read_data_list = list(data.frame(x = 1L)), errors = NULL)
    },
    transform_files_list = function(file_list_dt, read_data_list, config) {
      list(wide_raw = data.table::data.table(a = 1L), long_raw = transformed_long)
    },
    validate_long_dt = function(long_dt, config) {
      list(data = long_dt, errors = NULL)
    },
    consolidate_audited_dt = function(dt_list, config) {
      list(data = data.table::rbindlist(dt_list), warnings = NULL)
    },
    .env = environment(run_import_pipeline),
    {
      run_import_pipeline(config)
    }
  )

  testthat::expect_type(result$diagnostics$reading_errors, "character")
  testthat::expect_type(result$diagnostics$validation_errors, "character")
  testthat::expect_type(result$diagnostics$warnings, "character")
})

testthat::test_that("run_import_pipeline errors when no files are discovered", {
  source(here::here("R/1-import_pipeline/run_import_pipeline.R"), echo = FALSE)

  config <- list(paths = list(data = list(imports = list(raw = tempdir()))))

  testthat::with_mocked_bindings(
    source_import_scripts = function(script_names) invisible(script_names),
    discover_files = function(import_folder) data.table::data.table(),
    .env = environment(run_import_pipeline),
    {
      testthat::expect_error(
        run_import_pipeline(config),
        class = "rlang_error"
      )
    }
  )
})

testthat::test_that("run_import_pipeline keeps backward-compatible signature", {
  source(here::here("R/1-import_pipeline/run_import_pipeline.R"), echo = FALSE)

  testthat::expect_identical(names(formals(run_import_pipeline)), "config")
})

testthat::test_that("run_export_pipeline happy path returns output paths", {
  source(here::here("R/3-export_pipeline/run_export_pipeline.R"), echo = FALSE)

  input_dt <- data.frame(country = "argentina")
  config <- list(dummy = TRUE)

  result <- testthat::with_mocked_bindings(
    source_export_scripts = function(script_names) invisible(script_names),
    ensure_data_table = function(df) data.table::as.data.table(df),
    audit_data_output = function(dataset_dt, config) dataset_dt,
    export_processed_data = function(fao_data_raw, config, base_name, overwrite = TRUE) {
      "processed.xlsx"
    },
    export_selected_unique_lists = function(df, config, overwrite = TRUE) {
      "lists.xlsx"
    },
    .env = environment(run_export_pipeline),
    {
      run_export_pipeline(input_dt, config, overwrite = TRUE)
    }
  )

  testthat::expect_identical(result$processed_path, "processed.xlsx")
  testthat::expect_identical(result$lists_path, "lists.xlsx")
})

testthat::test_that("run_export_pipeline validates overwrite flag", {
  source(here::here("R/3-export_pipeline/run_export_pipeline.R"), echo = FALSE)

  testthat::expect_error(
    run_export_pipeline(data.frame(country = "x"), list(dummy = TRUE), overwrite = "yes")
  )
})

testthat::test_that("run_export_pipeline keeps backward-compatible defaults", {
  source(here::here("R/3-export_pipeline/run_export_pipeline.R"), echo = FALSE)

  export_formals <- formals(run_export_pipeline)
  testthat::expect_true(identical(export_formals$overwrite, TRUE))
})

testthat::test_that("legacy_source_run_general_pipeline assigns global config", {
  source(here::here("R/0-general_pipeline/run_general_pipeline.R"), echo = FALSE)

  testthat::with_mocked_bindings(
    run_general_pipeline = function(dataset_name = "fao_data_raw") {
      list(dataset_name = dataset_name)
    },
    .env = environment(legacy_source_run_general_pipeline),
    {
      output <- legacy_source_run_general_pipeline("fao_data_raw")
      testthat::expect_identical(output$dataset_name, "fao_data_raw")
      testthat::expect_true(exists("config", envir = .GlobalEnv, inherits = FALSE))
      testthat::expect_identical(get("config", envir = .GlobalEnv)$dataset_name, "fao_data_raw")
      rm("config", envir = .GlobalEnv)
    }
  )
})

testthat::test_that("legacy_source_run_import_pipeline resolves global config", {
  source(here::here("R/1-import_pipeline/run_import_pipeline.R"), echo = FALSE)

  assign("config", list(paths = list(data = list(imports = list(raw = tempdir())))), envir = .GlobalEnv)

  testthat::with_mocked_bindings(
    run_import_pipeline = function(config) {
      list(
        data = data.table::data.table(x = 1L),
        wide_raw = data.table::data.table(y = 1L),
        diagnostics = list(
          reading_errors = character(0),
          validation_errors = character(0),
          warnings = character(0)
        )
      )
    },
    .env = environment(legacy_source_run_import_pipeline),
    {
      output <- legacy_source_run_import_pipeline()
      testthat::expect_true(data.table::is.data.table(output$data))
      testthat::expect_true(exists("fao_data_raw", envir = .GlobalEnv, inherits = FALSE))
      testthat::expect_true(exists("fao_data_wide_raw", envir = .GlobalEnv, inherits = FALSE))
      rm("config", "fao_data_raw", "fao_data_wide_raw", "import_pipeline_result", "collected_reading_errors", "collected_errors", "collected_warnings", envir = .GlobalEnv)
    }
  )
})

testthat::test_that("legacy_source_run_export_pipeline resolves globals and assigns export_paths", {
  source(here::here("R/3-export_pipeline/run_export_pipeline.R"), echo = FALSE)

  assign("fao_data_raw", data.frame(country = "argentina"), envir = .GlobalEnv)
  assign("config", list(dummy = TRUE), envir = .GlobalEnv)

  testthat::with_mocked_bindings(
    run_export_pipeline = function(fao_data_raw, config, overwrite = TRUE) {
      list(processed_path = "processed.xlsx", lists_path = "lists.xlsx")
    },
    .env = environment(legacy_source_run_export_pipeline),
    {
      output <- legacy_source_run_export_pipeline(overwrite = TRUE)
      testthat::expect_identical(output$processed_path, "processed.xlsx")
      testthat::expect_true(exists("export_paths", envir = .GlobalEnv, inherits = FALSE))
      rm("fao_data_raw", "config", "export_paths", envir = .GlobalEnv)
    }
  )
})
